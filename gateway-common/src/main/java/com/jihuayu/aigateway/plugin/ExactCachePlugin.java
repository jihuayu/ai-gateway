package com.jihuayu.aigateway.plugin;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jihuayu.aigateway.security.SecurityUtils;
import io.lettuce.core.api.StatefulRedisConnection;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Built-in Exact Match Response Cache Plugin (#37).
 *
 * <p>Operates at:
 * <ul>
 *   <li><b>Stage 3 ({@code onPostAdmitPreDispatch})</b>: Checks Redis + L1 cache by deterministic SHA-256 key
 *       over {@code (projectId, endpoint, modelAlias, canonicalRequest)}. On cache HIT, returns
 *       {@link PluginResult.ShortCircuit} with {@code x-gateway-cache: HIT}, causing {@code DataPlaneExecutor}
 *       to settle immediately at {@code 0} micro-USD and refund 100% of reserved budget and TPM quota.</li>
 *   <li><b>Stage 4b ({@code onResponseContent})</b>: Stores HTTP 200 non-streaming upstream responses in Redis + L1
 *       with configurable TTL ({@code ttlSeconds}, default 300s).</li>
 * </ul>
 */
public class ExactCachePlugin implements GatewayPlugin {

    public static final String ID = "builtin:exact-cache";
    private static final String ATTR_CACHE_KEY = "exact_cache.key";

    private record CacheEntry(String body, long expiresAtMillis) {}

    private final StatefulRedisConnection<String, String> redisConn;
    private final String keyPrefix;
    private final ObjectMapper mapper;
    private final Map<String, CacheEntry> l1Cache = new ConcurrentHashMap<>();

    public ExactCachePlugin(
        StatefulRedisConnection<String, String> redisConn,
        String keyPrefix,
        ObjectMapper mapper
    ) {
        this.redisConn = redisConn;
        this.keyPrefix = keyPrefix != null ? keyPrefix : "gw:";
        this.mapper = mapper != null ? mapper : new ObjectMapper();
    }

    @Override
    public String id() {
        return ID;
    }

    @Override
    public String name() {
        return "exact-cache";
    }

    @Override
    public String type() {
        return "BUILTIN";
    }

    @Override
    public int priority() {
        return 20;
    }

    @Override
    public PluginResult onPostAdmitPreDispatch(PluginExecutionContext ctx) {
        if (ctx.stream() && Boolean.FALSE.equals(ctx.pluginConfig().get("cacheStreaming"))) {
            return PluginResult.Continue.INSTANCE;
        }
        String noCacheHdr = ctx.requestHeaders().getOrDefault("cache-control", "");
        if (noCacheHdr.contains("no-cache") || noCacheHdr.contains("no-store")) {
            return PluginResult.Continue.INSTANCE;
        }
        String cacheKey = computeCacheKey(ctx);
        ctx.attributes().put(ATTR_CACHE_KEY, cacheKey);

        String cachedBody = lookupCached(cacheKey);
        if (cachedBody != null && !cachedBody.isBlank()) {
            return PluginResult.shortCircuit(
                200,
                cachedBody,
                Map.of(
                    "x-gateway-cache", "HIT",
                    "x-gateway-cache-key", cacheKey
                )
            );
        }
        ctx.setResponseHeader("x-gateway-cache", "MISS");
        ctx.setResponseHeader("x-gateway-cache-key", cacheKey);
        return PluginResult.Continue.INSTANCE;
    }

    @Override
    public PluginResult onResponseContent(PluginExecutionContext ctx) {
        if (ctx.responseStatus() != 200 || (ctx.stream() && Boolean.FALSE.equals(ctx.pluginConfig().get("cacheStreaming")))) {
            return PluginResult.Continue.INSTANCE;
        }
        String body = ctx.responseBody();
        if (body == null || body.isBlank()) {
            return PluginResult.Continue.INSTANCE;
        }
        Object keyObj = ctx.attributes().get(ATTR_CACHE_KEY);
        String cacheKey = keyObj instanceof String s ? s : computeCacheKey(ctx);
        long ttlSeconds = 300L;
        Object ttlCfg = ctx.pluginConfig().get("ttlSeconds");
        if (ttlCfg instanceof Number n && n.longValue() > 0) {
            ttlSeconds = n.longValue();
        }
        storeCached(cacheKey, body, ttlSeconds);
        return PluginResult.Continue.INSTANCE;
    }

    private String computeCacheKey(PluginExecutionContext ctx) {
        try {
            Map<String, Object> canonical = new TreeMap<>();
            canonical.put("projectId", ctx.projectId());
            canonical.put("endpoint", ctx.endpoint());
            canonical.put("model", ctx.modelAlias());
            canonical.put("messages", ctx.requestJson().get("messages"));
            canonical.put("system", ctx.requestJson().get("system"));
            canonical.put("input", ctx.requestJson().get("input"));
            canonical.put("temperature", ctx.requestJson().get("temperature"));
            canonical.put("top_p", ctx.requestJson().get("top_p"));
            canonical.put("tools", ctx.requestJson().get("tools"));
            canonical.put("response_format", ctx.requestJson().get("response_format"));
            return SecurityUtils.hashKey(mapper.writeValueAsString(canonical));
        } catch (Exception e) {
            return SecurityUtils.hashKey(ctx.projectId() + ":" + ctx.modelAlias() + ":" + ctx.extractPromptText());
        }
    }

    private String lookupCached(String cacheKey) {
        long now = System.currentTimeMillis();
        CacheEntry l1 = l1Cache.get(cacheKey);
        if (l1 != null) {
            if (l1.expiresAtMillis() > now) {
                return l1.body();
            }
            l1Cache.remove(cacheKey);
        }
        if (redisConn != null) {
            try {
                String redisVal = redisConn.sync().get(keyPrefix + "cache:exact:" + cacheKey);
                if (redisVal != null && !redisVal.isBlank()) {
                    l1Cache.put(cacheKey, new CacheEntry(redisVal, now + 60_000L));
                    return redisVal;
                }
            } catch (Exception ignored) {}
        }
        return null;
    }

    private void storeCached(String cacheKey, String body, long ttlSeconds) {
        long now = System.currentTimeMillis();
        l1Cache.put(cacheKey, new CacheEntry(body, now + ttlSeconds * 1000L));
        if (redisConn != null) {
            try {
                redisConn.sync().setex(keyPrefix + "cache:exact:" + cacheKey, ttlSeconds, body);
            } catch (Exception ignored) {}
        }
    }

    public void clearCache() {
        l1Cache.clear();
    }
}
