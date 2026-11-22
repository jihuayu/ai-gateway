package com.jihuayu.aigateway.plugin;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.jihuayu.aigateway.security.SecurityUtils;
import io.lettuce.core.api.StatefulRedisConnection;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Built-in Exact Match Response Cache Plugin (#37) upgraded with Two-Tiered Caffeine W-TinyLFU L1 + Redis L2
 * and SingleFlight request coalescing.
 */
public class ExactCachePlugin implements GatewayPlugin {

    public static final String ID = "builtin:exact-cache";
    private static final String ATTR_CACHE_KEY = "exact_cache.key";

    private record CacheEntry(String body, long expiresAtMillis) {}

    private final StatefulRedisConnection<String, String> redisConn;
    private final String keyPrefix;
    private final ObjectMapper mapper;
    private final Cache<String, CacheEntry> caffeineL1;
    private final Map<String, CompletableFuture<String>> singleFlightL2Loaders = new ConcurrentHashMap<>();
    private final AtomicLong singleFlightCoalescedHits = new AtomicLong(0);

    public ExactCachePlugin(
        StatefulRedisConnection<String, String> redisConn,
        String keyPrefix,
        ObjectMapper mapper
    ) {
        this.redisConn = redisConn;
        this.keyPrefix = keyPrefix != null ? keyPrefix : "gw:";
        this.mapper = mapper != null ? mapper : new ObjectMapper();
        this.caffeineL1 = Caffeine.newBuilder()
            .maximumWeight(128L * 1024L * 1024L) // 128 MB W-TinyLFU window
            .weigher((String k, CacheEntry v) -> k.length() + (v != null && v.body() != null ? v.body().getBytes(StandardCharsets.UTF_8).length : 0))
            .expireAfterWrite(Duration.ofMinutes(15))
            .recordStats()
            .build();
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
            if (!Boolean.FALSE.equals(ctx.pluginConfig().get("skipLedgerOnCacheHit"))) {
                ctx.attributes().put("cache.skip_ledger_on_hit", Boolean.TRUE);
            }
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
        CacheEntry l1 = caffeineL1.getIfPresent(cacheKey);
        if (l1 != null) {
            if (l1.expiresAtMillis() > now) {
                return l1.body();
            }
            caffeineL1.invalidate(cacheKey);
        }
        if (redisConn != null) {
            CompletableFuture<String> myFuture = new CompletableFuture<>();
            CompletableFuture<String> prior = singleFlightL2Loaders.putIfAbsent(cacheKey, myFuture);
            if (prior != null) {
                singleFlightCoalescedHits.incrementAndGet();
                try {
                    return prior.join();
                } catch (Exception ignored) {
                    return null;
                }
            }
            try {
                String redisVal = redisConn.sync().get(keyPrefix + "cache:exact:" + cacheKey);
                if (redisVal != null && !redisVal.isBlank()) {
                    caffeineL1.put(cacheKey, new CacheEntry(redisVal, now + 60_000L));
                    myFuture.complete(redisVal);
                    return redisVal;
                }
                myFuture.complete(null);
            } catch (Exception e) {
                myFuture.complete(null);
            } finally {
                singleFlightL2Loaders.remove(cacheKey, myFuture);
            }
        }
        return null;
    }

    private void storeCached(String cacheKey, String body, long ttlSeconds) {
        long now = System.currentTimeMillis();
        caffeineL1.put(cacheKey, new CacheEntry(body, now + ttlSeconds * 1000L));
        if (redisConn != null) {
            try {
                redisConn.async().setex(keyPrefix + "cache:exact:" + cacheKey, ttlSeconds, body);
            } catch (Exception ignored) {}
        }
    }

    public com.github.benmanes.caffeine.cache.stats.CacheStats cacheStats() {
        return caffeineL1.stats();
    }

    public long singleFlightCoalescedHits() {
        return singleFlightCoalescedHits.get();
    }

    public void clearCache() {
        caffeineL1.invalidateAll();
    }
}
