package com.jihuayu.aigateway.netty;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.jihuayu.aigateway.hot.HotBilling;
import com.jihuayu.aigateway.hot.HotContract.HotKeys;
import com.jihuayu.aigateway.hot.HotRuntime;
import io.lettuce.core.ClientOptions;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.regex.Pattern;

/**
 * Standalone, zero-Spring, zero-SQL entrypoint for the pure Netty Data-Plane Gateway.
 * Designed for instant GraalVM Native Image compilation and sub-10ms cold start.
 */
public final class NettyGatewayMain {
    private static final Pattern HEX_64 = Pattern.compile("^[a-fA-F0-9]{64}$");
    private static final Pattern NAMESPACE_RE = Pattern.compile("^[A-Za-z0-9_-]{1,80}$");

    private NettyGatewayMain() {}

    public record StandaloneGatewayHandle(
        NettyGatewayServer server,
        DataPlaneExecutor executor,
        HotBilling billing,
        HotRuntime runtime,
        StatefulRedisConnection<String, String> redisConn,
        RedisClient redisClient
    ) implements AutoCloseable {
        @Override
        public void close() {
            try { server.stop(); } catch (Exception ignored) {}
            try { executor.close(); } catch (Exception ignored) {}
            try { billing.close(); } catch (Exception ignored) {}
            try { redisConn.close(); } catch (Exception ignored) {}
            try { redisClient.shutdown(); } catch (Exception ignored) {}
        }
    }

    public static StandaloneGatewayHandle startStandalone(
        String redisUrl,
        String encryptionKey,
        String host,
        int port,
        boolean allowPrivateUpstream,
        String namespace,
        String hotDurability,
        int hotJournalCapacity
    ) throws InterruptedException {
        if (redisUrl == null || (!redisUrl.startsWith("redis://") && !redisUrl.startsWith("rediss://"))) {
            throw new IllegalArgumentException("Invalid REDIS_URL: must start with redis:// or rediss://");
        }
        if (encryptionKey == null || !HEX_64.matcher(encryptionKey).matches()) {
            throw new IllegalArgumentException("Invalid ENCRYPTION_KEY: must be 64 hex characters (32 bytes)");
        }
        if (namespace == null || !NAMESPACE_RE.matcher(namespace).matches()) {
            throw new IllegalArgumentException("Invalid DATA_PLANE_NAMESPACE");
        }
        if (!"aof".equals(hotDurability) && !"memory".equals(hotDurability)) {
            throw new IllegalArgumentException("Invalid HOT_DURABILITY: must be aof or memory");
        }
        if (hotJournalCapacity < 7 || hotJournalCapacity > 1_000_000) {
            throw new IllegalArgumentException("Invalid HOT_JOURNAL_CAPACITY");
        }

        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule());

        RedisURI uri = RedisURI.create(redisUrl);
        uri.setTimeout(Duration.ofSeconds(5));
        RedisClient redisClient = RedisClient.create(uri);
        redisClient.setOptions(ClientOptions.builder()
            .disconnectedBehavior(ClientOptions.DisconnectedBehavior.REJECT_COMMANDS)
            .build());
        StatefulRedisConnection<String, String> redisConn = redisClient.connect();

        try {
            HotKeys keys = HotKeys.of(namespace);
            HotRuntime runtime = new HotRuntime(null, redisConn, keys, namespace, hotDurability, mapper);
            runtime.initializeFromRedisOnly();

            HotBilling billing = new HotBilling(redisConn, keys, runtime.getEpoch(), hotJournalCapacity, 45_000L, mapper);
            billing.initialize();

            DataPlaneExecutor executor = new DataPlaneExecutor(runtime, billing, encryptionKey, allowPrivateUpstream, mapper);
            NettyGatewayServer server = new NettyGatewayServer(host, port, executor, billing);
            server.start();
            return new StandaloneGatewayHandle(server, executor, billing, runtime, redisConn, redisClient);
        } catch (Exception e) {
            try { redisConn.close(); } catch (Exception ignored) {}
            try { redisClient.shutdown(); } catch (Exception ignored) {}
            throw e;
        }
    }

    public static void main(String[] args) throws Exception {
        String redisUrl = env("REDIS_URL", "redis://127.0.0.1:6379");
        String encryptionKey = env("ENCRYPTION_KEY", "0000000000000000000000000000000000000000000000000000000000000001");
        String host = env("HOST", "0.0.0.0");
        int port = Integer.parseInt(env("GATEWAY_PORT", env("PORT", "3001")));
        boolean allowPrivateUpstream = "true".equalsIgnoreCase(env("ALLOW_PRIVATE_UPSTREAM", "false"));
        String namespace = env("DATA_PLANE_NAMESPACE", "gateway");
        String durability = env("HOT_DURABILITY", "aof");
        int capacity = Integer.parseInt(env("HOT_JOURNAL_CAPACITY", "10000"));

        long t0 = System.nanoTime();
        StandaloneGatewayHandle handle = startStandalone(
            redisUrl, encryptionKey, host, port, allowPrivateUpstream, namespace, durability, capacity
        );
        double bootMs = (System.nanoTime() - t0) / 1_000_000.0;
        System.out.printf("AI Gateway Native Netty Data Plane listening on http://%s:%d (epoch=%s, started in %.2f ms)%n",
            host, handle.server().getBoundPort(), handle.runtime().getEpoch(), bootMs);

        CountDownLatch latch = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            handle.close();
            latch.countDown();
        }, "gateway-shutdown"));
        latch.await();
    }

    private static String env(String key, String defaultVal) {
        String v = System.getenv(key);
        return (v != null && !v.isBlank()) ? v.trim() : defaultVal;
    }
}
