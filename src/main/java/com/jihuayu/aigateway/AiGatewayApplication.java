package com.jihuayu.aigateway;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.jihuayu.aigateway.config.GatewayConfig;
import com.jihuayu.aigateway.config.GatewayConfig.JdbcParams;
import com.jihuayu.aigateway.db.DatabaseMigrator;
import com.jihuayu.aigateway.hot.HotBilling;
import com.jihuayu.aigateway.hot.HotContract.HotKeys;
import com.jihuayu.aigateway.hot.HotProjector;
import com.jihuayu.aigateway.hot.HotRuntime;
import com.jihuayu.aigateway.netty.DataPlaneExecutor;
import com.jihuayu.aigateway.netty.NettyGatewayServer;
import com.jihuayu.aigateway.worker.EventWorker;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.lettuce.core.ClientOptions;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import jakarta.annotation.PreDestroy;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import javax.sql.DataSource;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;

@SpringBootApplication
@EnableConfigurationProperties(GatewayConfig.class)
public class AiGatewayApplication {

    public static void main(String[] args) {
        SpringApplication.run(AiGatewayApplication.class, args);
    }

    @Bean
    public ObjectMapper objectMapper() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule());
        return mapper;
    }

    @Bean(destroyMethod = "close")
    @Primary
    public HikariDataSource primaryDataSource(GatewayConfig config) {
        config.validate("production".equalsIgnoreCase(System.getenv("NODE_ENV"))
            || "production".equalsIgnoreCase(System.getenv("SPRING_PROFILES_ACTIVE")));
        JdbcParams jdbc = config.parseJdbcParams();
        HikariConfig hc = new HikariConfig();
        hc.setJdbcUrl(jdbc.jdbcUrl());
        if (jdbc.username() != null) hc.setUsername(jdbc.username());
        if (jdbc.password() != null) hc.setPassword(jdbc.password());
        hc.setMaximumPoolSize(12);
        hc.setConnectionTimeout(5000);
        hc.addDataSourceProperty("options", "-c statement_timeout=10000");
        HikariDataSource ds = new HikariDataSource(hc);
        DatabaseMigrator.migrate(ds);
        return ds;
    }

    @Bean(name = "backgroundDataSource", destroyMethod = "close")
    public HikariDataSource backgroundDataSource(GatewayConfig config, DataSource primaryDataSource) {
        JdbcParams jdbc = config.parseJdbcParams();
        HikariConfig hc = new HikariConfig();
        hc.setJdbcUrl(jdbc.jdbcUrl());
        if (jdbc.username() != null) hc.setUsername(jdbc.username());
        if (jdbc.password() != null) hc.setPassword(jdbc.password());
        hc.setMaximumPoolSize(4);
        hc.setConnectionTimeout(2000);
        hc.addDataSourceProperty("options", "-c statement_timeout=10000");
        return new HikariDataSource(hc);
    }

    @Bean(destroyMethod = "shutdown")
    public RedisClient redisClient(GatewayConfig config) {
        RedisURI uri = RedisURI.create(config.getRedisUrl());
        uri.setTimeout(Duration.ofSeconds(5));
        RedisClient client = RedisClient.create(uri);
        client.setOptions(ClientOptions.builder()
            .disconnectedBehavior(ClientOptions.DisconnectedBehavior.REJECT_COMMANDS)
            .build());
        return client;
    }

    @Bean(destroyMethod = "close")
    public StatefulRedisConnection<String, String> redisConnection(RedisClient redisClient) {
        return redisClient.connect();
    }

    @Bean
    public HotKeys hotKeys(GatewayConfig config) {
        return HotKeys.of(config.getDataPlaneNamespace());
    }

    @Bean
    public HotRuntime hotRuntime(
        @Qualifier("backgroundDataSource") HikariDataSource backgroundPool,
        StatefulRedisConnection<String, String> redisConn,
        HotKeys hotKeys,
        GatewayConfig config,
        ObjectMapper mapper
    ) {
        HotRuntime rt = new HotRuntime(
            backgroundPool,
            redisConn,
            hotKeys,
            config.getDataPlaneNamespace(),
            config.getHotDurability(),
            mapper
        );
        rt.initialize();
        return rt;
    }

    @Bean(destroyMethod = "close")
    public HotBilling hotBilling(
        StatefulRedisConnection<String, String> redisConn,
        HotKeys hotKeys,
        HotRuntime hotRuntime,
        GatewayConfig config,
        ObjectMapper mapper
    ) {
        HotBilling billing = new HotBilling(
            redisConn,
            hotKeys,
            hotRuntime.getEpoch(),
            config.getHotJournalCapacity(),
            45_000L,
            mapper
        );
        billing.initialize();
        return billing;
    }

    @Bean(destroyMethod = "close")
    public HotProjector hotProjector(
        @Qualifier("backgroundDataSource") HikariDataSource backgroundPool,
        StatefulRedisConnection<String, String> redisConn,
        HotKeys hotKeys,
        HotRuntime hotRuntime,
        ObjectMapper mapper
    ) {
        return new HotProjector(backgroundPool, redisConn, hotKeys, hotRuntime.getEpoch(), mapper);
    }

    @Bean(destroyMethod = "close")
    public EventWorker eventWorker(
        @Qualifier("backgroundDataSource") HikariDataSource backgroundPool,
        GatewayConfig config,
        ObjectMapper mapper
    ) {
        List<String> brokers = Arrays.stream(config.getKafkaBrokers().split(","))
            .map(String::trim)
            .filter(s -> !s.isEmpty())
            .toList();
        EventWorker worker = new EventWorker(backgroundPool, brokers, mapper);
        worker.start();
        return worker;
    }

    @Bean(destroyMethod = "close")
    public DataPlaneExecutor dataPlaneExecutor(
        HotRuntime hotRuntime,
        HotBilling hotBilling,
        GatewayConfig config,
        ObjectMapper mapper
    ) {
        return new DataPlaneExecutor(
            hotRuntime,
            hotBilling,
            config.getEncryptionKey(),
            config.isAllowPrivateUpstream(),
            mapper
        );
    }

    @Bean(destroyMethod = "stop")
    public NettyGatewayServer nettyGatewayServer(
        GatewayConfig config,
        DataPlaneExecutor dataPlaneExecutor,
        HotBilling hotBilling
    ) throws InterruptedException {
        NettyGatewayServer server = new NettyGatewayServer(
            config.getHost(),
            config.getNettyPort(),
            dataPlaneExecutor,
            hotBilling
        );
        if (config.getNettyPort() >= 0) {
            server.start();
        }
        return server;
    }

    @Bean(destroyMethod = "close")
    public BackgroundCoordinator backgroundCoordinator(
        GatewayConfig config,
        @Qualifier("backgroundDataSource") HikariDataSource backgroundPool,
        StatefulRedisConnection<String, String> redisConn,
        HotKeys hotKeys,
        HotRuntime hotRuntime,
        HotBilling hotBilling,
        HotProjector hotProjector,
        EventWorker eventWorker
    ) {
        BackgroundCoordinator coordinator = new BackgroundCoordinator(
            config.isBackgroundWorkers(),
            backgroundPool,
            redisConn,
            hotKeys,
            hotRuntime,
            hotBilling,
            hotProjector,
            eventWorker
        );
        coordinator.start();
        return coordinator;
    }

    public static class BackgroundCoordinator {
        private final boolean enabled;
        private final HikariDataSource backgroundPool;
        private final StatefulRedisConnection<String, String> redisConn;
        private final HotKeys hotKeys;
        private final HotRuntime hotRuntime;
        private final HotBilling hotBilling;
        private final HotProjector hotProjector;
        private final EventWorker eventWorker;
        private final ScheduledExecutorService executor = Executors.newScheduledThreadPool(2, Thread.ofVirtual().factory());

        public BackgroundCoordinator(
            boolean enabled,
            HikariDataSource backgroundPool,
            StatefulRedisConnection<String, String> redisConn,
            HotKeys hotKeys,
            HotRuntime hotRuntime,
            HotBilling hotBilling,
            HotProjector hotProjector,
            EventWorker eventWorker
        ) {
            this.enabled = enabled;
            this.backgroundPool = backgroundPool;
            this.redisConn = redisConn;
            this.hotKeys = hotKeys;
            this.hotRuntime = hotRuntime;
            this.hotBilling = hotBilling;
            this.hotProjector = hotProjector;
            this.eventWorker = eventWorker;
        }

        public void start() {
            if (!enabled) return;
            executor.scheduleWithFixedDelay(this::runProjectionCycle, 100, 100, TimeUnit.MILLISECONDS);
            executor.scheduleWithFixedDelay(this::runPublishCycle, 10_000, 10_000, TimeUnit.MILLISECONDS);
        }

        public void runProjectionCycle() {
            try {
                boolean full = false;
                try (Connection conn = backgroundPool.getConnection();
                     PreparedStatement ps = conn.prepareStatement("/*background:outbox-capacity*/ SELECT 1 FROM outbox WHERE status != 'published' OFFSET 9999 LIMIT 1");
                     ResultSet rs = ps.executeQuery()) {
                    full = rs.next();
                }
                redisConn.sync().hset(hotKeys.runtime(), "projection_paused", full ? "1" : "0");
                if (!full) {
                    hotProjector.tick();
                }
                eventWorker.tick();
                hotBilling.reap();
            } catch (Exception ignored) {
                // Background projection failed; uncommitted journal retained
            }
        }

        public void runPublishCycle() {
            try {
                hotRuntime.publish();
            } catch (Exception ignored) {}
        }

        @PreDestroy
        public void close() {
            executor.shutdown();
            try {
                executor.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
