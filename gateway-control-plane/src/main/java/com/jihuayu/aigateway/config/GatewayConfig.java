package com.jihuayu.aigateway.config;

import java.net.URI;
import java.util.regex.Pattern;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "gateway")
public class GatewayConfig {
    private static final Pattern HEX_64 = Pattern.compile("^[a-fA-F0-9]{64}$");
    private static final Pattern NAMESPACE_RE = Pattern.compile("^[A-Za-z0-9_-]{1,80}$");

    private String databaseUrl = "postgresql://gateway:local-development-only@127.0.0.1:5432/gateway";
    private String redisUrl = "redis://127.0.0.1:6379";
    private String encryptionKey = "0000000000000000000000000000000000000000000000000000000000000001";
    private String host = "127.0.0.1";
    private int adminPort = 3000;
    private int nettyPort = 3001;
    private boolean allowPrivateUpstream = false;
    private String kafkaBrokers = "";
    private String dataPlaneNamespace = "gateway";
    private String hotDurability = "aof";
    private int hotJournalCapacity = 65536;
    private boolean backgroundWorkers = true;

    public void validate(boolean production) {
        if (databaseUrl == null || (!databaseUrl.startsWith("postgres://") && !databaseUrl.startsWith("postgresql://") && !databaseUrl.startsWith("jdbc:postgresql://"))) {
            throw new IllegalArgumentException("Invalid configuration: DATABASE_URL: PostgreSQL URL required");
        }
        if (redisUrl == null || (!redisUrl.startsWith("redis://") && !redisUrl.startsWith("rediss://"))) {
            throw new IllegalArgumentException("Invalid configuration: REDIS_URL: Redis URL required");
        }
        if (encryptionKey == null || !HEX_64.matcher(encryptionKey).matches()) {
            throw new IllegalArgumentException("Invalid configuration: ENCRYPTION_KEY: ENCRYPTION_KEY must be 32 bytes encoded as hex");
        }
        if (dataPlaneNamespace == null || !NAMESPACE_RE.matcher(dataPlaneNamespace).matches()) {
            throw new IllegalArgumentException("Invalid configuration: DATA_PLANE_NAMESPACE: Invalid data-plane namespace");
        }
        if (!"aof".equals(hotDurability) && !"memory".equals(hotDurability)) {
            throw new IllegalArgumentException("Invalid configuration: HOT_DURABILITY: must be aof or memory");
        }
        if (production && "memory".equals(hotDurability)) {
            throw new IllegalArgumentException("Memory-only Redis durability is restricted to development/test");
        }
        if (hotJournalCapacity < 7 || hotJournalCapacity > 1_000_000) {
            throw new IllegalArgumentException("Invalid configuration: HOT_JOURNAL_CAPACITY: must be between 7 and 1000000");
        }
    }

    public record JdbcParams(String jdbcUrl, String username, String password) {}

    public JdbcParams parseJdbcParams() {
        String raw = databaseUrl;
        if (raw.startsWith("jdbc:postgresql://")) {
            return new JdbcParams(raw, null, null);
        }
        String normalized = raw.startsWith("postgres://") ? "postgresql://" + raw.substring("postgres://".length()) : raw;
        URI uri = URI.create(normalized);
        String userInfo = uri.getUserInfo();
        String user = null;
        String pass = null;
        if (userInfo != null) {
            int colon = userInfo.indexOf(':');
            if (colon >= 0) {
                user = userInfo.substring(0, colon);
                pass = userInfo.substring(colon + 1);
            } else {
                user = userInfo;
            }
        }
        int port = uri.getPort() > 0 ? uri.getPort() : 5432;
        String path = uri.getPath() != null ? uri.getPath() : "/gateway";
        String query = uri.getQuery() != null && !uri.getQuery().isBlank() ? "?" + uri.getQuery() : "";
        String jdbc = "jdbc:postgresql://" + uri.getHost() + ":" + port + path + query;
        return new JdbcParams(jdbc, user, pass);
    }

    public String getDatabaseUrl() { return databaseUrl; }
    public void setDatabaseUrl(String databaseUrl) { this.databaseUrl = databaseUrl; }
    public String getRedisUrl() { return redisUrl; }
    public void setRedisUrl(String redisUrl) { this.redisUrl = redisUrl; }
    public String getEncryptionKey() { return encryptionKey; }
    public void setEncryptionKey(String encryptionKey) { this.encryptionKey = encryptionKey; }
    public String getHost() { return host; }
    public void setHost(String host) { this.host = host; }
    public int getAdminPort() { return adminPort; }
    public void setAdminPort(int adminPort) { this.adminPort = adminPort; }
    public int getNettyPort() { return nettyPort; }
    public void setNettyPort(int nettyPort) { this.nettyPort = nettyPort; }
    public boolean isAllowPrivateUpstream() { return allowPrivateUpstream; }
    public void setAllowPrivateUpstream(boolean allowPrivateUpstream) { this.allowPrivateUpstream = allowPrivateUpstream; }
    public String getKafkaBrokers() { return kafkaBrokers; }
    public void setKafkaBrokers(String kafkaBrokers) { this.kafkaBrokers = kafkaBrokers; }
    public String getDataPlaneNamespace() { return dataPlaneNamespace; }
    public void setDataPlaneNamespace(String dataPlaneNamespace) { this.dataPlaneNamespace = dataPlaneNamespace; }
    public String getHotDurability() { return hotDurability; }
    public void setHotDurability(String hotDurability) { this.hotDurability = hotDurability; }
    public int getHotJournalCapacity() { return hotJournalCapacity; }
    public void setHotJournalCapacity(int hotJournalCapacity) { this.hotJournalCapacity = hotJournalCapacity; }
    public boolean isBackgroundWorkers() { return backgroundWorkers; }
    public void setBackgroundWorkers(boolean backgroundWorkers) { this.backgroundWorkers = backgroundWorkers; }
}
