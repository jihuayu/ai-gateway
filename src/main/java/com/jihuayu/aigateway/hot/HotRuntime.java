package com.jihuayu.aigateway.hot;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jihuayu.aigateway.hot.HotContract.HotKeys;
import com.jihuayu.aigateway.hot.HotContract.KeyContext;
import com.jihuayu.aigateway.hot.HotContract.RouteSnapshot;
import com.jihuayu.aigateway.hot.HotContract.RouteTarget;
import com.jihuayu.aigateway.hot.HotContract.Snapshot;
import com.jihuayu.aigateway.security.SecurityUtils;
import com.zaxxer.hikari.HikariDataSource;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class HotRuntime {
    private static final Pattern RUN_ID_PATTERN = Pattern.compile("run_id:([^\\r\\n]+)");
    private static final int DEFAULT_PROJECT_RPM = 600;
    private static final int DEFAULT_KEY_RPM = 600;
    private static final int DEFAULT_CONCURRENCY = 32;

    private static final String INIT_LUA = """
        if redis.call('EXISTS',KEYS[1])==1 or redis.call('EXISTS',KEYS[2])==1 or redis.call('EXISTS',KEYS[3])==1 then return 0 end
        redis.call('HSET',KEYS[1],'epoch',ARGV[1],'run_id',ARGV[2],'journal_tail','0-0')
        redis.call('SET',KEYS[2],0)
        redis.call('XGROUP','CREATE',KEYS[3],'pg-hot-v1','0','MKSTREAM')
        return 1
        """;

    private static final String PUBLISH_LUA = """
        local run=string.match(redis.call('INFO','server'),'run_id:([%w]+)')
        if redis.call('HGET',KEYS[1],'epoch')~=ARGV[1] or redis.call('HGET',KEYS[1],'run_id')~=run then return redis.error_reply('HOT_FENCED') end
        local seeds=cjson.decode(ARGV[4])
        for id,data in pairs(seeds) do
          local key=ARGV[5]..'project:'..id
          if redis.call('SISMEMBER',KEYS[4],id)==1 then
            if redis.call('EXISTS',key)==0 then return redis.error_reply('HOT_PROJECT_LOST') end
          elseif redis.call('EXISTS',key)==1 then return redis.error_reply('HOT_ORPHAN_PROJECT') end
        end
        for id,data in pairs(seeds) do
          if redis.call('SISMEMBER',KEYS[4],id)==0 then redis.call('SET',ARGV[5]..'project:'..id,data);redis.call('SADD',KEYS[4],id) end
        end
        redis.call('SET',KEYS[3],ARGV[3],'EX',86400)
        if redis.call('HGET',KEYS[2],'revision')==ARGV[2] then return 0 end
        redis.call('DEL',KEYS[2])
        redis.call('HSET',KEYS[2],'revision',ARGV[2])
        local entries=cjson.decode(ARGV[6])
        for key,value in pairs(entries) do redis.call('HSET',KEYS[2],'key:'..key,cjson.encode(value)) end
        return 1
        """;

    private static final String AUTH_LUA = """
        local run=string.match(redis.call('INFO','server'),'run_id:([%w]+)')
        if redis.call('HGET',KEYS[1],'epoch')~=ARGV[1] or redis.call('HGET',KEYS[1],'run_id')~=run then return {false,false} end
        return redis.call('HMGET',KEYS[2],'revision','key:'..ARGV[2])
        """;

    private final HikariDataSource pool;
    private final StatefulRedisConnection<String, String> redisConn;
    private final HotKeys keys;
    private final String namespace;
    private final String durability;
    private final ObjectMapper mapper;
    private final Map<String, Snapshot> cache = Collections.synchronizedMap(new LinkedHashMap<>(8, 0.75f, true) {
        @Override
        protected boolean removeEldestEntry(Map.Entry<String, Snapshot> eldest) {
            return size() > 4;
        }
    });

    private volatile String epoch = "";

    public HotRuntime(
        HikariDataSource pool,
        StatefulRedisConnection<String, String> redisConn,
        HotKeys keys,
        String namespace,
        String durability,
        ObjectMapper mapper
    ) {
        this.pool = pool;
        this.redisConn = redisConn;
        this.keys = keys;
        this.namespace = namespace;
        this.durability = durability;
        this.mapper = mapper;
    }

    public String getEpoch() {
        return epoch;
    }

    public HotKeys getKeys() {
        return keys;
    }

    
    /**
     * Zero-SQL data-plane initialization for standalone / GraalVM Native Netty Gateway nodes.
     * Verifies Redis AOF durability and attaches to the active Redis financial epoch & run_id
     * published by the Control Plane without requiring any PostgreSQL connection.
     */
    public void initializeFromRedisOnly() {
        RedisCommands<String, String> redis = redisConn.sync();
        if ("aof".equals(durability)) {
            Map<String, String> cfg = redis.configGet("appendonly", "appendfsync", "maxmemory-policy");
            if (!"yes".equals(cfg.get("appendonly"))
                || !"always".equals(cfg.get("appendfsync"))
                || !"noeviction".equals(cfg.get("maxmemory-policy"))) {
                throw new IllegalStateException("Redis hot ledger requires appendonly=yes, appendfsync=always, maxmemory-policy=noeviction");
            }
        }
        String existingEpoch = redis.hget(keys.runtime(), "epoch");
        String existingRunId = redis.hget(keys.runtime(), "run_id");
        String actualRunId = extractRunId(redis.info("server"));
        if (actualRunId == null || actualRunId.isBlank()) {
            throw new IllegalStateException("Redis identity unavailable");
        }
        if (existingEpoch == null || existingEpoch.isBlank()) {
            throw new IllegalStateException("HOT_STATE_NOT_INITIALIZED: Redis runtime epoch not found; start Control Plane first to publish initial snapshot");
        }
        if (!actualRunId.equals(existingRunId)) {
            throw new IllegalStateException("HOT_FENCED: Redis run_id mismatch; verify durable journal before attaching data-plane gateway");
        }
        this.epoch = existingEpoch;
    }

    public void initialize() {
        RedisCommands<String, String> redis = redisConn.sync();
        if ("aof".equals(durability)) {
            Map<String, String> cfg = redis.configGet("appendonly", "appendfsync", "maxmemory-policy");
            if (!"yes".equals(cfg.get("appendonly"))
                || !"always".equals(cfg.get("appendfsync"))
                || !"noeviction".equals(cfg.get("maxmemory-policy"))) {
                throw new IllegalStateException("Redis hot ledger requires appendonly=yes, appendfsync=always, maxmemory-policy=noeviction");
            }
        }
        try (Connection conn = pool.getConnection()) {
            boolean oldAuto = conn.getAutoCommit();
            conn.setAutoCommit(false);
            try {
                try (PreparedStatement lockPs = conn.prepareStatement("SELECT pg_advisory_xact_lock(hashtextextended(?,0))")) {
                    lockPs.setString(1, "gateway:hot-config:" + namespace);
                    lockPs.execute();
                }
                String storedEpoch = null;
                try (PreparedStatement ps = conn.prepareStatement("SELECT epoch::text FROM hot_runtime_manifests WHERE namespace = ?")) {
                    ps.setString(1, namespace);
                    try (ResultSet rs = ps.executeQuery()) {
                        if (rs.next()) {
                            storedEpoch = rs.getString(1);
                        }
                    }
                }
                String existingEpoch = redis.hget(keys.runtime(), "epoch");
                String runId = extractRunId(redis.info("server"));
                if (runId == null || runId.isBlank()) {
                    throw new IllegalStateException("Redis identity unavailable");
                }
                if (storedEpoch != null) {
                    if (!storedEpoch.equals(existingEpoch)) {
                        throw new IllegalStateException("HOT_STATE_LOST: restore complete Redis journal; PG may lag, automatic reset refused");
                    }
                    String existingRun = redis.hget(keys.runtime(), "run_id");
                    if (!runId.equals(existingRun)) {
                        throw new IllegalStateException("HOT_FENCED: Redis identity changed; verify durable journal and isolate old primary before recovery");
                    }
                    this.epoch = storedEpoch;
                } else {
                    try (PreparedStatement ps = conn.prepareStatement("SELECT 1 FROM hot_runtime_manifests LIMIT 1");
                         ResultSet rs = ps.executeQuery()) {
                        if (rs.next()) {
                            throw new IllegalStateException("HOT_NAMESPACE_CHANGE_REFUSED: this PG schema already owns another financial namespace");
                        }
                    }
                    if (existingEpoch != null) {
                        throw new IllegalStateException("HOT_ORPHAN_STATE: Redis namespace belongs to another manifest");
                    }
                    try (PreparedStatement ps = conn.prepareStatement("SELECT 1 FROM requests WHERE status IN('preparing','reserved','dispatch_intent','dispatched') LIMIT 1");
                         ResultSet rs = ps.executeQuery()) {
                        if (rs.next()) {
                            throw new IllegalStateException("Drain legacy in-flight requests before enabling the Redis data plane");
                        }
                    }
                    this.epoch = UUID.randomUUID().toString();
                    try (PreparedStatement ps = conn.prepareStatement("INSERT INTO hot_runtime_manifests(namespace, epoch) VALUES (?, ?::uuid)")) {
                        ps.setString(1, namespace);
                        ps.setString(2, this.epoch);
                        ps.executeUpdate();
                    }
                    Long created = redis.eval(
                        INIT_LUA,
                        ScriptOutputType.INTEGER,
                        new String[]{keys.runtime(), keys.slots(), keys.journal()},
                        this.epoch, runId
                    );
                    if (created == null || created != 1L) {
                        throw new IllegalStateException("Concurrent Redis namespace initialization");
                    }
                }
                publishInside(conn, redis);
                conn.commit();
            } catch (Exception e) {
                conn.rollback();
                throw e;
            } finally {
                conn.setAutoCommit(oldAuto);
            }
        } catch (RuntimeException re) {
            throw re;
        } catch (Exception e) {
            throw new IllegalStateException("HotRuntime initialize failed: " + e.getMessage(), e);
        }
    }

    public void publish() {
        RedisCommands<String, String> redis = redisConn.sync();
        Connection conn = null;
        boolean unlockFailed = false;
        try {
            conn = pool.getConnection();
            try (PreparedStatement lockPs = conn.prepareStatement("SELECT pg_advisory_lock(hashtextextended(?,0))")) {
                lockPs.setString(1, "gateway:hot-config:" + namespace);
                lockPs.execute();
            }
            boolean oldAuto = conn.getAutoCommit();
            int oldIso = conn.getTransactionIsolation();
            conn.setAutoCommit(false);
            conn.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
            try {
                publishInside(conn, redis);
                conn.commit();
            } catch (Exception e) {
                try { conn.rollback(); } catch (Exception ignored) {}
                throw e;
            } finally {
                try {
                    conn.setTransactionIsolation(oldIso);
                    conn.setAutoCommit(oldAuto);
                } catch (Exception ignored) {}
            }
        } catch (RuntimeException re) {
            throw re;
        } catch (Exception e) {
            throw new IllegalStateException("HotRuntime publish failed: " + e.getMessage(), e);
        } finally {
            if (conn != null) {
                try (PreparedStatement unlockPs = conn.prepareStatement("SELECT pg_advisory_unlock(hashtextextended(?,0))")) {
                    unlockPs.setString(1, "gateway:hot-config:" + namespace);
                    unlockPs.execute();
                } catch (Exception e) {
                    unlockFailed = true;
                    try { pool.evictConnection(conn); } catch (Exception ignored) {}
                }
                try { conn.close(); } catch (Exception ignored) {}
                if (unlockFailed) {
                    throw new IllegalStateException("Publisher unlock failed; locked session evicted");
                }
            }
        }
    }

    private void publishInside(Connection conn, RedisCommands<String, String> redis) throws Exception {
        List<Map<String, String>> projects = new ArrayList<>();
        try (PreparedStatement ps = conn.prepareStatement("SELECT id::text AS id, workspace_id::text AS workspace_id FROM projects ORDER BY id");
             ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                projects.add(Map.of("id", rs.getString("id"), "workspace_id", rs.getString("workspace_id")));
            }
        }
        Map<String, Object> seeds = new LinkedHashMap<>();
        for (Map<String, String> proj : projects) {
            String pid = proj.get("id");
            boolean known = Boolean.TRUE.equals(redis.sismember(keys.projects(), pid));
            if (known) {
                seeds.put(pid, "");
                continue;
            }
            Map<String, Map<String, Long>> budgets = new LinkedHashMap<>();
            try (PreparedStatement ps = conn.prepareStatement("SELECT period, COALESCE(SUM(amount_micros),0)::bigint AS spent FROM ledger WHERE project_id = ?::uuid GROUP BY period")) {
                ps.setString(1, pid);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        long spent = rs.getLong("spent");
                        if (spent < 0 || spent > HotContract.MAX_SAFE_INTEGER) {
                            throw new IllegalStateException("Invalid ledger bootstrap total");
                        }
                        budgets.put(rs.getString("period"), Map.of("spent", spent, "reserved", 0L));
                    }
                }
            }
            Map<String, Object> projectSeed = new LinkedHashMap<>();
            projectSeed.put("epoch", this.epoch);
            projectSeed.put("budgets", budgets);
            projectSeed.put("leases", Map.of());
            seeds.put(pid, mapper.writeValueAsString(projectSeed));
        }

        Map<String, Map<String, Object>> entries = new LinkedHashMap<>();
        String readKeysSql = """
            SELECT k.key_hash, k.id::text AS key_id, k.prefix, k.allowed_models::text AS allowed_models,
                   k.expires_at, k.revoked_at, k.key_rpm,
                   p.id::text AS project_id, p.workspace_id::text AS workspace_id,
                   p.rpm AS project_rpm, p.concurrency, p.budget_micros::text AS budget_micros, p.max_output_tokens
              FROM api_keys k JOIN projects p ON p.id = k.project_id
             ORDER BY k.id
            """;
        try (PreparedStatement ps = conn.prepareStatement(readKeysSql);
             ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                String keyHash = rs.getString("key_hash");
                Map<String, Object> row = new LinkedHashMap<>();
                row.put("key_id", rs.getString("key_id"));
                row.put("prefix", rs.getString("prefix"));
                String allowedJson = rs.getString("allowed_models");
                List<String> allowed = allowedJson != null ? mapper.readValue(allowedJson, new TypeReference<List<String>>() {}) : List.of();
                row.put("allowed_models", allowed);
                Timestamp expiresAt = rs.getTimestamp("expires_at");
                Timestamp revokedAt = rs.getTimestamp("revoked_at");
                row.put("expires_at", expiresAt != null ? expiresAt.toInstant().toString() : null);
                row.put("revoked_at", revokedAt != null ? revokedAt.toInstant().toString() : null);
                int keyRpm = rs.getInt("key_rpm");
                row.put("key_rpm", rs.wasNull() ? null : keyRpm);
                row.put("project_id", rs.getString("project_id"));
                row.put("workspace_id", rs.getString("workspace_id"));
                int projRpm = rs.getInt("project_rpm");
                row.put("project_rpm", rs.wasNull() ? null : projRpm);
                int conc = rs.getInt("concurrency");
                row.put("concurrency", rs.wasNull() ? null : conc);
                row.put("budget_micros", rs.getString("budget_micros"));
                int maxOut = rs.getInt("max_output_tokens");
                row.put("max_output_tokens", rs.wasNull() ? null : maxOut);
                entries.put(keyHash, row);
            }
        }

        Map<String, RouteSnapshot> routes = new LinkedHashMap<>();
        Map<String, List<Map<String, Object>>> models = new LinkedHashMap<>();
        for (Map<String, String> proj : projects) {
            String pid = proj.get("id");
            String wid = proj.get("workspace_id");
            List<Map<String, Object>> aliases = new ArrayList<>();
            try (PreparedStatement ps = conn.prepareStatement("""
                SELECT a.id::text AS id, a.name, a.created_at
                  FROM aliases a JOIN route_versions rv ON rv.id = a.current_version_id
                 WHERE a.project_id = ?::uuid AND rv.status = 'published'
                 ORDER BY a.name
                """)) {
                ps.setString(1, pid);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        Map<String, Object> aliasMap = new LinkedHashMap<>();
                        String aliasId = rs.getString("id");
                        String aliasName = rs.getString("name");
                        Timestamp createdAt = rs.getTimestamp("created_at");
                        aliasMap.put("id", aliasId);
                        aliasMap.put("name", aliasName);
                        aliasMap.put("created_at", createdAt != null ? createdAt.toInstant().toString() : Instant.now().toString());
                        aliases.add(aliasMap);
                        RouteSnapshot route = loadRouteFromDb(conn, pid, wid, aliasName);
                        if (route != null) {
                            routes.put(pid + ":" + aliasName, route);
                        }
                    }
                }
            }
            models.put(pid, aliases);
        }

        Snapshot snapshot = new Snapshot(this.epoch, routes, models);
        String snapshotJson = mapper.writeValueAsString(snapshot);
        String revision = SecurityUtils.hashKey(mapper.writeValueAsString(List.of(snapshot, entries, projects)));

        redis.eval(
            PUBLISH_LUA,
            ScriptOutputType.INTEGER,
            new String[]{keys.runtime(), keys.config(), keys.snapshot(revision), keys.projects()},
            this.epoch,
            revision,
            snapshotJson,
            mapper.writeValueAsString(seeds),
            keys.prefix(),
            mapper.writeValueAsString(entries)
        );
        cache.put(revision, snapshot);
    }

    private RouteSnapshot loadRouteFromDb(Connection conn, String projectId, String workspaceId, String aliasName) throws Exception {
        String aliasId;
        String routeVersionId;
        int version;
        List<String> targetIds;
        List<String> capabilities;
        try (PreparedStatement ps = conn.prepareStatement("""
            SELECT a.id::text AS alias_id, a.name AS alias_name, rv.id::text AS route_version_id,
                   rv.version, rv.targets::text AS targets, rv.capabilities::text AS capabilities
              FROM aliases a JOIN route_versions rv ON rv.id = a.current_version_id
             WHERE a.project_id = ?::uuid AND a.name = ? AND rv.status = 'published'
            """)) {
            ps.setString(1, projectId);
            ps.setString(2, aliasName);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) return null;
                aliasId = rs.getString("alias_id");
                routeVersionId = rs.getString("route_version_id");
                version = rs.getInt("version");
                targetIds = parseStringListOrObjects(rs.getString("targets"));
                if (targetIds.size() > 2) {
                    targetIds = targetIds.subList(0, 2);
                }
                capabilities = parseStringList(rs.getString("capabilities"));
            }
        }
        if (targetIds.isEmpty()) {
            return new RouteSnapshot(aliasId, aliasName, routeVersionId, version, capabilities, List.of());
        }
        Map<String, RouteTarget> byId = new LinkedHashMap<>();
        try (PreparedStatement ps = conn.prepareStatement("""
            SELECT t.id::text AS id, t.upstream_model, t.input_price, t.output_price,
                   t.capabilities::text AS capabilities, t.enabled AS target_enabled,
                   p.base_url, p.encrypted_secret, p.enabled AS provider_enabled
              FROM model_targets t JOIN providers p ON p.id = t.provider_id
             WHERE t.id = ANY(?::uuid[]) AND p.workspace_id = ?::uuid
            """)) {
            ps.setArray(1, conn.createArrayOf("uuid", targetIds.toArray()));
            ps.setString(2, workspaceId);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String tid = rs.getString("id");
                    byId.put(tid, new RouteTarget(
                        tid,
                        rs.getString("upstream_model"),
                        rs.getLong("input_price"),
                        rs.getLong("output_price"),
                        rs.getString("base_url"),
                        rs.getString("encrypted_secret"),
                        rs.getBoolean("target_enabled"),
                        rs.getBoolean("provider_enabled"),
                        parseStringList(rs.getString("capabilities"))
                    ));
                }
            }
        }
        List<RouteTarget> ordered = new ArrayList<>();
        for (String tid : targetIds) {
            RouteTarget rt = byId.get(tid);
            if (rt != null) ordered.add(rt);
        }
        return new RouteSnapshot(aliasId, aliasName, routeVersionId, version, capabilities, ordered);
    }

    private List<String> parseStringList(String json) {
        if (json == null || json.isBlank()) return List.of();
        try {
            List<?> raw = mapper.readValue(json, List.class);
            List<String> out = new ArrayList<>();
            for (Object o : raw) {
                if (o instanceof String s && !s.isBlank()) out.add(s);
            }
            return out;
        } catch (Exception e) {
            return List.of();
        }
    }

    private List<String> parseStringListOrObjects(String json) {
        if (json == null || json.isBlank()) return List.of();
        try {
            List<?> raw = mapper.readValue(json, List.class);
            List<String> out = new ArrayList<>();
            for (Object o : raw) {
                if (o instanceof String s && !s.isBlank()) {
                    out.add(s);
                } else if (o instanceof Map<?, ?> map) {
                    for (String k : List.of("target_id", "targetId", "id", "model_target_id")) {
                        Object val = map.get(k);
                        if (val instanceof String s && !s.isBlank()) {
                            out.add(s);
                            break;
                        }
                    }
                }
            }
            return out;
        } catch (Exception e) {
            return List.of();
        }
    }

    /** Zero-SQL data-plane authentication directly against Redis config hash */
    @SuppressWarnings("unchecked")
    public KeyContext authenticateBearerHeader(String authorizationHeader) {
        String token = extractBearerToken(authorizationHeader);
        String keyHash = SecurityUtils.hashKey(token);
        List<Object> fields;
        try {
            fields = redisConn.sync().eval(
                AUTH_LUA,
                ScriptOutputType.MULTI,
                new String[]{keys.runtime(), keys.config()},
                this.epoch,
                keyHash
            );
        } catch (Exception e) {
            throw new GatewayException(503, "configuration_unavailable", "运行配置暂不可用。");
        }
        if (fields == null || fields.isEmpty() || !(fields.get(0) instanceof String revision) || revision.isBlank()) {
            throw new GatewayException(503, "configuration_unavailable", "运行配置未就绪。");
        }
        String json = fields.size() > 1 && fields.get(1) instanceof String s ? s : null;
        if (json == null || json.isBlank()) {
            throw new GatewayException(401, "invalid_api_key", "API Key 无效或已失效。");
        }
        Map<String, Object> row;
        try {
            row = mapper.readValue(json, new TypeReference<Map<String, Object>>() {});
        } catch (Exception e) {
            throw new GatewayException(503, "configuration_unavailable", "运行配置暂不可用。");
        }
        if (row.get("revoked_at") != null) {
            throw new GatewayException(401, "invalid_api_key", "API Key 无效或已失效。");
        }
        Long expiresAtMs = null;
        Object expObj = row.get("expires_at");
        if (expObj instanceof String expStr && !expStr.isBlank()) {
            expiresAtMs = Instant.parse(expStr).toEpochMilli();
            if (expiresAtMs <= System.currentTimeMillis()) {
                throw new GatewayException(401, "invalid_api_key", "API Key 无效或已失效。");
            }
        }
        List<String> allowedModels = new ArrayList<>();
        if (row.get("allowed_models") instanceof List<?> list) {
            for (Object item : list) {
                if (item instanceof String s && !s.isBlank()) allowedModels.add(s);
            }
        }
        int projectRpm = row.get("project_rpm") instanceof Number n ? n.intValue() : DEFAULT_PROJECT_RPM;
        int keyRpm = row.get("key_rpm") instanceof Number n ? n.intValue() : DEFAULT_KEY_RPM;
        int concurrency = row.get("concurrency") instanceof Number n ? n.intValue() : DEFAULT_CONCURRENCY;
        long budgetMicros = -1L;
        Object bObj = row.get("budget_micros");
        if (bObj != null && !String.valueOf(bObj).isBlank() && !"null".equals(String.valueOf(bObj))) {
            budgetMicros = Long.parseLong(String.valueOf(bObj));
            if (budgetMicros < 0 || budgetMicros > HotContract.MAX_SAFE_INTEGER) {
                throw new GatewayException(500, "invalid_configuration", "budget_micros 配置无效。");
            }
        }
        Integer maxOutputTokens = row.get("max_output_tokens") instanceof Number n ? n.intValue() : null;

        return new KeyContext(
            revision,
            keyHash,
            expiresAtMs,
            String.valueOf(row.get("key_id")),
            String.valueOf(row.get("project_id")),
            String.valueOf(row.get("workspace_id")),
            allowedModels,
            projectRpm,
            keyRpm,
            concurrency,
            budgetMicros,
            maxOutputTokens
        );
    }

    public static String extractBearerToken(String authorizationHeader) {
        if (authorizationHeader == null || !authorizationHeader.regionMatches(true, 0, "Bearer ", 0, 7)) {
            throw new GatewayException(401, "invalid_api_key", "缺少有效的 API Key。");
        }
        String token = authorizationHeader.substring(7).trim();
        if (token.length() < 8 || token.length() > 512) {
            throw new GatewayException(401, "invalid_api_key", "缺少有效的 API Key。");
        }
        return token;
    }

    private Snapshot resolveSnapshot(KeyContext key) {
        String revision = key.runtimeRevision();
        if (revision == null || revision.isBlank()) {
            throw new GatewayException(503, "configuration_unavailable", "运行配置未就绪。");
        }
        Snapshot snap = cache.get(revision);
        if (snap == null) {
            String json;
            try {
                json = redisConn.sync().get(keys.snapshot(revision));
            } catch (Exception e) {
                throw new GatewayException(503, "configuration_unavailable", "运行配置暂不可用。");
            }
            if (json == null || json.isBlank()) {
                throw new GatewayException(503, "configuration_unavailable", "运行配置快照缺失。");
            }
            try {
                snap = mapper.readValue(json, Snapshot.class);
            } catch (Exception e) {
                throw new GatewayException(503, "configuration_unavailable", "运行配置快照损坏。");
            }
            if (!this.epoch.equals(snap.epoch()) || snap.routes() == null || snap.models() == null) {
                throw new GatewayException(503, "configuration_unavailable", "运行配置代次不匹配。");
            }
            cache.put(revision, snap);
        }
        return snap;
    }

    public RouteSnapshot loadRoute(KeyContext key, String model) {
        Snapshot snap = resolveSnapshot(key);
        return snap.routes().get(key.projectId() + ":" + model);
    }

    public List<Map<String, Object>> models(KeyContext key) {
        Snapshot snap = resolveSnapshot(key);
        return snap.models().getOrDefault(key.projectId(), List.of());
    }

    public void clearLocalCache() {
        cache.clear();
    }

    public static String extractRunId(String infoServer) {
        if (infoServer == null) return null;
        Matcher m = RUN_ID_PATTERN.matcher(infoServer);
        return m.find() ? m.group(1).trim() : null;
    }
}
