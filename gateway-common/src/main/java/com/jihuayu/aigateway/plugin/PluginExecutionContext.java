package com.jihuayu.aigateway.plugin;

import com.jihuayu.aigateway.hot.GatewayTelemetry;
import com.jihuayu.aigateway.hot.HotContract.KeyContext;
import com.jihuayu.aigateway.hot.HotContract.RouteTarget;
import com.jihuayu.aigateway.security.SecurityUtils;
import io.lettuce.core.SetArgs;
import io.lettuce.core.api.StatefulRedisConnection;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;
import java.util.function.Function;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

/**
 * Isomorphic Execution Context ({@code ctx}) shared across all 8 microkernel lifecycle stages
 * by BOTH Java plugins ({@link GatewayPlugin}) and JavaScript plugins ({@link JsPluginSandbox}).
 *
 * <p>Exposes first-class host capabilities:
 * <ul>
 *   <li>{@link #kv()} ({@link PluginKvStore}): Namespace-isolated, quota-governed Redis + L1 KV store</li>
 *   <li>{@link #crypto()} ({@link PluginCrypto}): SHA-256, HMAC-SHA256, Base64 encode/decode, constant-time compare</li>
 *   <li>{@link #bindAuthenticatedKey(String)} &amp; {@link #bindKeyContext(KeyContext)}: Custom authentication binding</li>
 *   <li>{@link #candidateTargets()} &amp; {@link #setCandidateTargets(List)}: Pluggable route target ordering</li>
 *   <li>{@link #streamChunk()} &amp; {@link #setStreamChunk(String)}: Streaming SSE chunk transformation</li>
 *   <li>{@link #attributionTags()} &amp; {@link #settledBillingEvent()}: FinOps cost attribution &amp; billing sink payload</li>
 *   <li>{@link #jevDecide(Object, Map)}: Built-in TypeSafe AI Jev System One decision primitive</li>
 * </ul>
 */
public class PluginExecutionContext {

    public record PluginTraceEntry(
        String pluginId,
        String pluginName,
        String pluginType,
        String stage,
        String outcome,
        long durationMicros,
        long executedStatements,
        String detail
    ) {}

    public record SettledBillingEvent(
        String requestId,
        String projectId,
        String workspaceId,
        String keyId,
        String endUserId,
        String endpoint,
        String modelAlias,
        String upstreamModel,
        String status,
        long reserveMicros,
        long actualMicros,
        long savedCacheMicros,
        long inputTokens,
        long outputTokens,
        long cachedInputTokens,
        long reasoningTokens,
        Long ttftMs,
        long latencyMs,
        Map<String, String> attributionTags,
        long timestampEpochMs
    ) {
        public SettledBillingEvent(
            String requestId,
            String workspaceId,
            String projectId,
            String keyId,
            String endpoint,
            String modelAlias,
            String upstreamModel,
            String status,
            long actualMicros,
            long inputTokens,
            long outputTokens,
            long cachedInputTokens,
            long reasoningTokens,
            Long ttftMs,
            Map<String, String> attributionTags,
            String timestampIso
        ) {
            this(
                requestId,
                projectId,
                workspaceId,
                keyId,
                attributionTags != null ? attributionTags.getOrDefault("end_user_id", "") : "",
                endpoint,
                modelAlias,
                upstreamModel,
                status,
                actualMicros,
                actualMicros,
                0L,
                inputTokens,
                outputTokens,
                cachedInputTokens,
                reasoningTokens,
                ttftMs,
                ttftMs != null ? ttftMs : 0L,
                attributionTags != null ? attributionTags : Map.of(),
                System.currentTimeMillis()
            );
        }
    }

    /**
     * Namespace-isolated, quota-governed KV store accessible via {@code ctx.kv()} in Java and {@code ctx.kv} in JS.
     */
    public static final class PluginKvStore {
        private record L1Val(String value, long expiresAtMs) {}

        private static final Map<String, L1Val> SHARED_L1 = new ConcurrentHashMap<>();
        private static final int MAX_OPS_PER_REQUEST = 32;
        private static final int MAX_KEY_LEN = 160;
        private static final int MAX_VAL_LEN = 8192;

        private final StatefulRedisConnection<String, String> redisConn;
        private final String keyPrefix;
        private String activePluginNamespace = "global";
        private int opsCount = 0;

        public PluginKvStore(StatefulRedisConnection<String, String> redisConn, String keyPrefix) {
            this.redisConn = redisConn;
            this.keyPrefix = keyPrefix != null ? keyPrefix : "gw:";
        }

        public void setActivePluginNamespace(String pluginId) {
            this.activePluginNamespace = (pluginId != null && !pluginId.isBlank())
                ? pluginId.replaceAll("[^A-Za-z0-9_:-]", "_")
                : "global";
        }

        private String scopedKey(String rawKey) {
            if (rawKey == null || rawKey.isBlank() || rawKey.length() > MAX_KEY_LEN) {
                throw new IllegalArgumentException("Invalid plugin KV key length");
            }
            if (++opsCount > MAX_OPS_PER_REQUEST) {
                throw new IllegalStateException("Plugin exceeded per-request KV operation quota (" + MAX_OPS_PER_REQUEST + ")");
            }
            return keyPrefix + "plugin:kv:" + activePluginNamespace + ":" + rawKey;
        }

        public String get(String key) {
            String fullKey = scopedKey(key);
            long now = System.currentTimeMillis();
            L1Val l1 = SHARED_L1.get(fullKey);
            if (l1 != null) {
                if (l1.expiresAtMs() > now) {
                    return l1.value();
                }
                SHARED_L1.remove(fullKey);
            }
            if (redisConn != null) {
                try {
                    return redisConn.sync().get(fullKey);
                } catch (Exception ignored) {}
            }
            return null;
        }

        public void set(String key, String value, long ttlSeconds) {
            String fullKey = scopedKey(key);
            String val = value != null ? value : "";
            if (val.length() > MAX_VAL_LEN) {
                throw new IllegalArgumentException("Plugin KV value exceeds max length (" + MAX_VAL_LEN + ")");
            }
            long ttl = Math.max(1L, Math.min(86_400L, ttlSeconds));
            SHARED_L1.put(fullKey, new L1Val(val, System.currentTimeMillis() + ttl * 1000L));
            if (redisConn != null) {
                try {
                    redisConn.sync().setex(fullKey, ttl, val);
                } catch (Exception ignored) {}
            }
        }

        public boolean setNx(String key, String value, long ttlSeconds) {
            String fullKey = scopedKey(key);
            String val = value != null ? value : "1";
            long ttl = Math.max(1L, Math.min(86_400L, ttlSeconds));
            long now = System.currentTimeMillis();
            if (redisConn != null) {
                try {
                    String res = redisConn.sync().set(fullKey, val, SetArgs.Builder.nx().ex(ttl));
                    boolean ok = "OK".equalsIgnoreCase(res);
                    if (ok) {
                        SHARED_L1.put(fullKey, new L1Val(val, now + ttl * 1000L));
                    }
                    return ok;
                } catch (Exception ignored) {}
            }
            L1Val existing = SHARED_L1.get(fullKey);
            if (existing != null && existing.expiresAtMs() > now) {
                return false;
            }
            SHARED_L1.put(fullKey, new L1Val(val, now + ttl * 1000L));
            return true;
        }

        public long incr(String key, long delta, long ttlSeconds) {
            String fullKey = scopedKey(key);
            long ttl = Math.max(1L, Math.min(86_400L, ttlSeconds));
            long now = System.currentTimeMillis();
            if (redisConn != null) {
                try {
                    Long next = redisConn.sync().incrby(fullKey, delta);
                    if (next != null) {
                        if (next == delta) {
                            redisConn.sync().expire(fullKey, ttl);
                        }
                        SHARED_L1.put(fullKey, new L1Val(String.valueOf(next), now + ttl * 1000L));
                        return next;
                    }
                } catch (Exception ignored) {}
            }
            L1Val cur = SHARED_L1.get(fullKey);
            long base = 0L;
            long exp = now + ttl * 1000L;
            if (cur != null && cur.expiresAtMs() > now) {
                try { base = Long.parseLong(cur.value()); } catch (Exception ignored) {}
                exp = cur.expiresAtMs();
            }
            long next = base + delta;
            SHARED_L1.put(fullKey, new L1Val(String.valueOf(next), exp));
            return next;
        }

        public void del(String key) {
            String fullKey = scopedKey(key);
            SHARED_L1.remove(fullKey);
            if (redisConn != null) {
                try {
                    redisConn.sync().del(fullKey);
                } catch (Exception ignored) {}
            }
        }
    }

    /**
     * Cryptographic and encoding utility accessible via {@code ctx.crypto()} in Java and {@code ctx.crypto} / {@code Crypto} in JS.
     */
    public static final class PluginCrypto {
        public static final PluginCrypto INSTANCE = new PluginCrypto();

        private PluginCrypto() {}

        public String sha256(String input) {
            return SecurityUtils.hashKey(input != null ? input : "");
        }

        public String hmacSha256(String secret, String message) {
            try {
                Mac mac = Mac.getInstance("HmacSHA256");
                byte[] keyBytes = (secret != null ? secret : "").getBytes(StandardCharsets.UTF_8);
                mac.init(new SecretKeySpec(keyBytes, "HmacSHA256"));
                byte[] raw = mac.doFinal((message != null ? message : "").getBytes(StandardCharsets.UTF_8));
                StringBuilder hex = new StringBuilder(raw.length * 2);
                for (byte b : raw) {
                    hex.append(String.format("%02x", b & 0xff));
                }
                return hex.toString();
            } catch (Exception e) {
                throw new IllegalStateException("HMAC-SHA256 computation failed", e);
            }
        }

        public String base64Encode(String input) {
            return Base64.getEncoder().encodeToString((input != null ? input : "").getBytes(StandardCharsets.UTF_8));
        }

        public String base64Decode(String base64) {
            if (base64 == null || base64.isBlank()) return "";
            return new String(Base64.getDecoder().decode(base64.trim()), StandardCharsets.UTF_8);
        }

        public String base64UrlDecode(String base64Url) {
            if (base64Url == null || base64Url.isBlank()) return "";
            return new String(Base64.getUrlDecoder().decode(base64Url.trim()), StandardCharsets.UTF_8);
        }

        public boolean constantTimeEquals(String a, String b) {
            if (a == null || b == null) return false;
            return MessageDigest.isEqual(
                a.getBytes(StandardCharsets.UTF_8),
                b.getBytes(StandardCharsets.UTF_8)
            );
        }
    }

    /**
     * Isomorphic Telemetry &amp; Distributed Tracing handle accessible via {@code ctx.telemetry()} in Java
     * and {@code ctx.telemetry} in JS, protected by per-request operation quota (64 ops) and series cardinality caps.
     */
    public static final class PluginTelemetry {
        private static final int MAX_OPS_PER_REQUEST = 64;

        private final String traceId;
        private final String spanId;
        private final String parentSpanId;
        private final Map<String, Object> customSpanAttrs = new LinkedHashMap<>();
        private final List<GatewayTelemetry.SpanEvent> customSpanEvents = new ArrayList<>();
        private int opsCount = 0;

        public PluginTelemetry(String traceparentHeader) {
            String[] parts = GatewayTelemetry.parseOrGenerateTraceparent(traceparentHeader);
            this.traceId = parts[0];
            this.spanId = parts[1];
            this.parentSpanId = parts[2];
        }

        private void checkQuota() {
            if (++opsCount > MAX_OPS_PER_REQUEST) {
                throw new IllegalStateException("Plugin exceeded per-request telemetry operation quota (" + MAX_OPS_PER_REQUEST + ")");
            }
        }

        public void incr(String metricName) {
            incr(metricName, 1L, Map.of());
        }

        public void incr(String metricName, long delta, Map<String, String> labels) {
            checkQuota();
            GatewayTelemetry.global().recordCustomCounter(metricName, delta, labels);
        }

        public void gauge(String metricName, double value, Map<String, String> labels) {
            checkQuota();
            GatewayTelemetry.global().recordCustomGauge(metricName, value, labels);
        }

        public void observe(String metricName, double value, Map<String, String> labels) {
            checkQuota();
            GatewayTelemetry.global().recordCustomHistogram(metricName, value, labels);
        }

        public void setSpanAttr(String key, Object value) {
            checkQuota();
            if (key != null && !key.isBlank() && value != null && customSpanAttrs.size() < 32) {
                customSpanAttrs.put(key.trim(), value);
            }
        }

        public void addSpanEvent(String eventName, Map<String, Object> attributes) {
            checkQuota();
            if (eventName != null && !eventName.isBlank() && customSpanEvents.size() < 16) {
                Map<String, Object> safeAttrs = attributes != null ? new LinkedHashMap<>(attributes) : Map.of();
                customSpanEvents.add(new GatewayTelemetry.SpanEvent(eventName.trim(), System.currentTimeMillis(), safeAttrs));
            }
        }

        public String traceId() { return traceId; }
        public String spanId() { return spanId; }
        public String parentSpanId() { return parentSpanId; }
        public String traceparent() { return "00-" + traceId + "-" + spanId + "-01"; }

        public Map<String, Object> customSpanAttrs() { return Collections.unmodifiableMap(customSpanAttrs); }
        public List<GatewayTelemetry.SpanEvent> customSpanEvents() { return Collections.unmodifiableList(customSpanEvents); }
    }

    private final String requestId;
    private String projectId;
    private String workspaceId;
    private String keyId;
    private final String endpoint;
    private String rawQuery = "";
    private final Map<String, String> queryParams = new LinkedHashMap<>();
    private String modelAlias;
    private final boolean stream;
    private final Map<String, String> requestHeaders;
    private final Map<String, Object> requestJson;
    private final Map<String, String> responseHeaders = new LinkedHashMap<>();
    private final Map<String, Object> attributes = new LinkedHashMap<>();
    private final Map<String, String> attributionTags = new LinkedHashMap<>();
    private final List<PluginTraceEntry> traceLog = new ArrayList<>();
    private Map<String, Object> currentPluginConfig = Map.of();

    private final PluginKvStore kvStore;
    private final PluginTelemetry telemetryHandle;
    private KeyContext authenticatedKey;
    private String boundApiKeyPlain;
    private String endUserId;
    private List<RouteTarget> candidateTargets = new ArrayList<>();
    private boolean candidateTargetsMutated = false;

    private String streamChunk;
    private boolean streamChunkMutated = false;

    private int responseStatus = 200;
    private String responseBody;
    private Map<String, Object> responseJson;
    private boolean requestBodyMutated = false;
    private boolean responseBodyMutated = false;

    private long settledActualMicros = 0L;
    private long settledInputTokens = 0L;
    private long settledOutputTokens = 0L;
    private long settledSavedCacheMicros = 0L;
    private String settledStatus = "succeeded";
    private SettledBillingEvent settledBillingEvent;
    private BiFunction<Object, Map<String, Object>, Map<String, Object>> jevEvaluator;

    public PluginExecutionContext(
        String requestId,
        String projectId,
        String workspaceId,
        String keyId,
        String endpoint,
        String modelAlias,
        boolean stream,
        Map<String, String> requestHeaders,
        Map<String, Object> requestJson
    ) {
        this(
            requestId, projectId, workspaceId, keyId, endpoint, modelAlias, stream,
            requestHeaders, requestJson, new PluginKvStore(null, "gw:")
        );
    }

    public PluginExecutionContext(
        String requestId,
        String projectId,
        String workspaceId,
        String keyId,
        String endpoint,
        String rawQuery,
        String modelAlias,
        boolean stream,
        Map<String, String> requestHeaders,
        Map<String, Object> requestJson,
        PluginKvStore kvStore
    ) {
        this(requestId, projectId, workspaceId, keyId, endpoint, modelAlias, stream, requestHeaders, requestJson, kvStore);
        setRawQuery(rawQuery);
    }

    public PluginExecutionContext(
        String requestId,
        String projectId,
        String workspaceId,
        String keyId,
        String endpoint,
        String modelAlias,
        boolean stream,
        Map<String, String> requestHeaders,
        Map<String, Object> requestJson,
        PluginKvStore kvStore
    ) {
        this.requestId = requestId != null ? requestId : "";
        this.projectId = projectId != null ? projectId : "";
        this.workspaceId = workspaceId != null ? workspaceId : "";
        this.keyId = keyId != null ? keyId : "";
        this.endpoint = endpoint != null ? endpoint : "/v1/chat/completions";
        this.modelAlias = modelAlias != null ? modelAlias : "";
        this.stream = stream;
        this.requestHeaders = requestHeaders != null ? new LinkedHashMap<>(requestHeaders) : new LinkedHashMap<>();
        this.requestJson = requestJson != null ? new LinkedHashMap<>(requestJson) : new LinkedHashMap<>();
        this.kvStore = kvStore != null ? kvStore : new PluginKvStore(null, "gw:");
        this.telemetryHandle = new PluginTelemetry(this.requestHeaders.get("traceparent"));
    }

    public void setIdentityContext(String projectId, String workspaceId, String keyId) {
        this.projectId = projectId != null ? projectId : "";
        this.workspaceId = workspaceId != null ? workspaceId : "";
        this.keyId = keyId != null ? keyId : "";
    }

    public void clearRequestBodyMutated() {
        this.requestBodyMutated = false;
    }

    public PluginKvStore kv() { return kvStore; }
    public PluginCrypto crypto() { return PluginCrypto.INSTANCE; }
    public PluginTelemetry telemetry() { return telemetryHandle; }

    public String requestId() { return requestId; }
    public String projectId() { return projectId; }
    public String workspaceId() { return workspaceId; }
    public String keyId() { return keyId; }
    public String endpoint() { return endpoint; }

    public String rawQuery() { return rawQuery; }
    public Map<String, String> queryParams() { return queryParams; }
    public void setRawQuery(String rawQuery) {
        this.rawQuery = rawQuery != null ? rawQuery : "";
        this.queryParams.clear();
        if (!this.rawQuery.isBlank()) {
            for (String pair : this.rawQuery.split("&")) {
                int eq = pair.indexOf('=');
                if (eq > 0) {
                    queryParams.put(pair.substring(0, eq).trim(), pair.substring(eq + 1).trim());
                } else if (!pair.isBlank()) {
                    queryParams.put(pair.trim(), "");
                }
            }
        }
    }

    public KeyContext authenticatedKey() { return authenticatedKey; }
    public void bindKeyContext(KeyContext key) {
        this.authenticatedKey = key;
        if (key != null) {
            this.projectId = key.projectId();
            this.workspaceId = key.workspaceId();
            this.keyId = key.keyId();
        }
    }

    public String boundApiKeyPlain() { return boundApiKeyPlain; }
    public void bindAuthenticatedKey(String apiKeyPlain) {
        if (apiKeyPlain != null && !apiKeyPlain.isBlank()) {
            this.boundApiKeyPlain = apiKeyPlain.trim();
        }
    }

    public String endUserId() {
        if (endUserId != null && !endUserId.isBlank()) return endUserId;
        if (attributes.get("auth.end_user_id") instanceof String s && !s.isBlank()) return s;
        String hdr = requestHeaders.get("x-end-user-id");
        if (hdr != null && !hdr.isBlank()) return hdr.trim();
        hdr = requestHeaders.get("x-tenant-id");
        if (hdr != null && !hdr.isBlank()) return hdr.trim();
        if (requestJson.get("user") instanceof String u && !u.isBlank()) return u.trim();
        hdr = requestHeaders.get("x-forwarded-for");
        if (hdr != null && !hdr.isBlank()) return hdr.split(",")[0].trim();
        return null;
    }

    public void setEndUserId(String endUserId) {
        this.endUserId = endUserId;
        if (endUserId != null) {
            this.attributes.put("auth.end_user_id", endUserId);
            this.attributionTags.put("end_user_id", endUserId);
        }
    }

    public String modelAlias() { return modelAlias; }
    public void setModelAlias(String modelAlias) {
        this.modelAlias = modelAlias;
        this.requestJson.put("model", modelAlias);
        this.requestBodyMutated = true;
    }
    public boolean stream() { return stream; }

    public List<RouteTarget> candidateTargets() {
        return Collections.unmodifiableList(candidateTargets);
    }

    public void initCandidateTargets(List<RouteTarget> targets) {
        this.candidateTargets = targets != null ? new ArrayList<>(targets) : new ArrayList<>();
        this.candidateTargetsMutated = false;
    }

    public void setCandidateTargets(List<RouteTarget> targets) {
        if (targets != null && !targets.isEmpty()) {
            this.candidateTargets = new ArrayList<>(targets);
            this.candidateTargetsMutated = true;
        }
    }

    public void reorderTargetsByIds(List<String> orderedIds) {
        if (orderedIds == null || orderedIds.isEmpty() || candidateTargets.size() <= 1) return;
        Map<String, RouteTarget> byId = new LinkedHashMap<>();
        for (RouteTarget rt : candidateTargets) {
            byId.put(rt.targetId(), rt);
            byId.put(rt.upstreamModel(), rt);
        }
        List<RouteTarget> reordered = new ArrayList<>(candidateTargets.size());
        for (String id : orderedIds) {
            RouteTarget match = byId.get(id);
            if (match != null && !reordered.contains(match)) {
                reordered.add(match);
            }
        }
        for (RouteTarget rt : candidateTargets) {
            if (!reordered.contains(rt)) {
                reordered.add(rt);
            }
        }
        this.candidateTargets = reordered;
        this.candidateTargetsMutated = true;
    }

    public boolean isCandidateTargetsMutated() { return candidateTargetsMutated; }

    public String streamChunk() { return streamChunk; }
    public void initStreamChunk(String chunk) {
        this.streamChunk = chunk;
        this.streamChunkMutated = false;
    }
    public void setStreamChunk(String chunk) {
        this.streamChunk = chunk;
        this.streamChunkMutated = true;
    }
    public boolean isStreamChunkMutated() { return streamChunkMutated; }

    public Map<String, String> attributionTags() { return attributionTags; }
    public void putAttributionTag(String key, String value) {
        if (key != null && !key.isBlank() && value != null) {
            attributionTags.put(key.trim(), value.trim());
        }
    }

    public SettledBillingEvent settledBillingEvent() { return settledBillingEvent; }
    public void setSettledBillingEvent(SettledBillingEvent settledBillingEvent) {
        this.settledBillingEvent = settledBillingEvent;
    }

    public long settledSavedCacheMicros() { return settledSavedCacheMicros; }
    public void setSettledSavedCacheMicros(long savedMicros) {
        this.settledSavedCacheMicros = Math.max(0L, savedMicros);
    }

    public void setJevEvaluator(BiFunction<Object, Map<String, Object>, Map<String, Object>> evaluator) {
        this.jevEvaluator = evaluator;
    }

    /**
     * Evaluates TypeSafe AI Jev System One typed questions ({@code noul}, {@code choice}, {@code score})
     * against the provided state (or the current prompt text if state is null).
     * Accessible from both Java plugins ({@code ctx.jevDecide(...)}) and JS plugins ({@code ctx.jevDecide(...)}).
     */
    @SuppressWarnings("unchecked")
    public Map<String, Object> jevDecide(Object state, Map<String, Object> questions) {
        Object effectiveState = (state != null && !String.valueOf(state).isBlank()) ? state : extractPromptText();
        if (jevEvaluator != null) {
            return jevEvaluator.apply(effectiveState, questions);
        }
        // Deterministic local System One RLCD heuristic calibration fallback when no external Jev target is specified
        String text = String.valueOf(effectiveState).toLowerCase(Locale.ROOT);
        Map<String, Object> answers = new LinkedHashMap<>();
        if (questions != null) {
            for (Map.Entry<String, Object> e : questions.entrySet()) {
                String qName = e.getKey();
                Map<String, Object> qSpec = e.getValue() instanceof Map<?, ?> m
                    ? (Map<String, Object>) m
                    : Map.of("type", "noul");
                String qType = String.valueOf(qSpec.getOrDefault("type", "noul")).toLowerCase(Locale.ROOT);
                String instr = String.valueOf(qSpec.getOrDefault("instructions", "")).toLowerCase(Locale.ROOT);
                if ("noul".equals(qType)) {
                    double prob = evaluateHeuristicNoul(text, qName.toLowerCase(Locale.ROOT), instr);
                    answers.put(qName, Map.of("type", "noul", "noul", prob));
                } else if ("choice".equals(qType)) {
                    List<String> opts = new ArrayList<>();
                    Object rawOpts = qSpec.get("options") != null ? qSpec.get("options") : qSpec.get("choices");
                    if (rawOpts instanceof List<?> list) {
                        for (Object o : list) if (o != null) opts.add(String.valueOf(o));
                    }
                    if (opts.isEmpty()) opts = List.of("simple", "complex");
                    String chosen = evaluateHeuristicChoice(text, opts);
                    Map<String, Double> dist = new LinkedHashMap<>();
                    for (String o : opts) {
                        dist.put(o, o.equals(chosen) ? 0.92 : Math.round((0.08 / Math.max(1, opts.size() - 1)) * 1000.0) / 1000.0);
                    }
                    answers.put(qName, Map.of("type", "choice", "choice", chosen, "probabilities", dist, "confidence", 0.92));
                } else if ("score".equals(qType)) {
                    double score = (text.length() > 180 || text.contains("proof") || text.contains("architecture") || text.contains("algorithm")) ? 2.5 : 0.5;
                    answers.put(qName, Map.of("type", "score", "score", score, "confidence", 0.90));
                }
            }
        }
        return Map.of("model", "jev-latest", "answers", answers);
    }

    private static double evaluateHeuristicNoul(String text, String qName, String instr) {
        if (qName.contains("unsafe") || qName.contains("jailbreak") || qName.contains("injection")
            || instr.contains("jailbreak") || instr.contains("unsafe") || instr.contains("injection")) {
            if (text.contains("ignore previous") || text.contains("ignore all previous")
                || text.contains("system prompt") || text.contains("jailbreak")
                || text.contains("bypass safety") || text.contains("drop table")
                || text.contains("/etc/passwd")) {
                return 0.97;
            }
            return 0.02;
        }
        if (qName.contains("complex") || instr.contains("complex") || instr.contains("reasoning")) {
            if (text.length() > 160 || text.contains("prove") || text.contains("theorem")
                || text.contains("implement") || text.contains("distributed") || text.contains("optimize")) {
                return 0.94;
            }
            return 0.12;
        }
        return 0.50;
    }

    private static String evaluateHeuristicChoice(String text, List<String> opts) {
        boolean isComplex = text.length() > 140 || text.contains("code") || text.contains("implement")
            || text.contains("prove") || text.contains("theorem") || text.contains("design")
            || text.contains("algorithm") || text.contains("sql") || text.contains("debug");
        for (String o : opts) {
            String lo = o.toLowerCase(Locale.ROOT);
            if (isComplex && (lo.contains("complex") || lo.contains("pro") || lo.contains("reason") || lo.contains("code") || lo.contains("high"))) {
                return o;
            }
            if (!isComplex && (lo.contains("simple") || lo.contains("fast") || lo.contains("mini") || lo.contains("flash") || lo.contains("low"))) {
                return o;
            }
        }
        return isComplex ? opts.getLast() : opts.getFirst();
    }

    public Map<String, String> requestHeaders() { return requestHeaders; }
    public void setRequestHeader(String name, String value) {
        if (name != null && value != null) {
            requestHeaders.put(name.toLowerCase(), value);
        }
    }

    public Map<String, Object> requestJson() { return requestJson; }
    public boolean isRequestBodyMutated() { return requestBodyMutated; }
    public void markRequestBodyMutated() { this.requestBodyMutated = true; }

    public Map<String, String> responseHeaders() { return responseHeaders; }
    public void setResponseHeader(String name, String value) {
        if (name != null && value != null) {
            responseHeaders.put(name, value);
        }
    }

    public int responseStatus() { return responseStatus; }
    public void setResponseStatus(int responseStatus) { this.responseStatus = responseStatus; }

    public String responseBody() { return responseBody; }
    public void setResponseBody(String responseBody) {
        this.responseBody = responseBody;
        this.responseBodyMutated = true;
    }

    public Map<String, Object> responseJson() { return responseJson; }
    public void setResponseJson(Map<String, Object> responseJson) {
        this.responseJson = responseJson;
        this.responseBodyMutated = true;
    }

    public boolean isResponseBodyMutated() { return responseBodyMutated; }
    public void clearResponseBodyMutated() { this.responseBodyMutated = false; }

    public Map<String, Object> attributes() { return attributes; }

    public Map<String, Object> pluginConfig() { return currentPluginConfig; }
    public void setPluginConfig(Map<String, Object> config) {
        this.currentPluginConfig = config != null ? config : Map.of();
    }

    public long settledActualMicros() { return settledActualMicros; }
    public long settledInputTokens() { return settledInputTokens; }
    public long settledOutputTokens() { return settledOutputTokens; }
    public String settledStatus() { return settledStatus; }

    public void setSettlementMetrics(long actualMicros, long inputTokens, long outputTokens, String status) {
        this.settledActualMicros = actualMicros;
        this.settledInputTokens = inputTokens;
        this.settledOutputTokens = outputTokens;
        this.settledStatus = status != null ? status : "succeeded";
    }

    public List<PluginTraceEntry> traceLog() { return traceLog; }
    public void recordTrace(
        String pluginId,
        String pluginName,
        String pluginType,
        PluginStage stage,
        String outcome,
        long durationMicros,
        long executedStatements,
        String detail
    ) {
        traceLog.add(new PluginTraceEntry(
            pluginId,
            pluginName,
            pluginType,
            stage.name(),
            outcome,
            durationMicros,
            executedStatements,
            detail
        ));
    }

    @SuppressWarnings("unchecked")
    public String extractPromptText() {
        StringBuilder sb = new StringBuilder();
        Object sys = requestJson.get("system");
        if (sys instanceof String s && !s.isBlank()) {
            sb.append(s).append('\n');
        }
        Object msgs = requestJson.get("messages");
        if (msgs instanceof List<?> list) {
            for (Object item : list) {
                if (item instanceof Map<?, ?> m) {
                    Object content = m.get("content");
                    if (content instanceof String s) {
                        if (!sb.isEmpty()) sb.append('\n');
                        sb.append(s);
                    } else if (content instanceof List<?> parts) {
                        for (Object p : parts) {
                            if (p instanceof Map<?, ?> pm && pm.get("text") instanceof String ts) {
                                if (!sb.isEmpty()) sb.append('\n');
                                sb.append(ts);
                            }
                        }
                    }
                }
            }
        }
        Object input = requestJson.get("input");
        if (input instanceof String s) {
            if (!sb.isEmpty()) sb.append('\n');
            sb.append(s);
        } else if (input instanceof List<?> list) {
            for (Object item : list) {
                if (item instanceof String s) {
                    if (!sb.isEmpty()) sb.append('\n');
                    sb.append(s);
                } else if (item instanceof Map<?, ?> m && m.get("content") instanceof String s) {
                    if (!sb.isEmpty()) sb.append('\n');
                    sb.append(s);
                }
            }
        }
        Object stateObj = requestJson.get("state");
        if (stateObj instanceof String s && !s.isBlank()) {
            if (!sb.isEmpty()) sb.append('\n');
            sb.append(s);
        } else if (stateObj != null) {
            if (!sb.isEmpty()) sb.append('\n');
            sb.append(String.valueOf(stateObj));
        }
        Object promptObj = requestJson.get("prompt");
        if (promptObj instanceof String s && !s.isBlank()) {
            if (!sb.isEmpty()) sb.append('\n');
            sb.append(s);
        }
        Object queryObj = requestJson.get("query");
        if (queryObj instanceof String s && !s.isBlank()) {
            if (!sb.isEmpty()) sb.append('\n');
            sb.append(s);
        }
        return sb.toString();
    }

    @SuppressWarnings("unchecked")
    public boolean transformPromptText(Function<String, String> transformer) {
        boolean changed = false;
        Object sys = requestJson.get("system");
        if (sys instanceof String s) {
            String next = transformer.apply(s);
            if (!s.equals(next)) {
                requestJson.put("system", next);
                changed = true;
            }
        }
        Object msgs = requestJson.get("messages");
        if (msgs instanceof List<?> rawList) {
            List<Object> newList = new ArrayList<>(rawList.size());
            for (Object item : rawList) {
                if (item instanceof Map<?, ?> rawMap) {
                    Map<String, Object> m = new LinkedHashMap<>((Map<String, Object>) rawMap);
                    Object content = m.get("content");
                    if (content instanceof String s) {
                        String next = transformer.apply(s);
                        if (!s.equals(next)) {
                            m.put("content", next);
                            changed = true;
                        }
                    }
                    newList.add(m);
                } else {
                    newList.add(item);
                }
            }
            if (changed) {
                requestJson.put("messages", newList);
            }
        }
        Object input = requestJson.get("input");
        if (input instanceof String s) {
            String next = transformer.apply(s);
            if (!s.equals(next)) {
                requestJson.put("input", next);
                changed = true;
            }
        }
        Object stateObj = requestJson.get("state");
        if (stateObj instanceof String s) {
            String next = transformer.apply(s);
            if (!s.equals(next)) {
                requestJson.put("state", next);
                changed = true;
            }
        }
        Object promptObj = requestJson.get("prompt");
        if (promptObj instanceof String s) {
            String next = transformer.apply(s);
            if (!s.equals(next)) {
                requestJson.put("prompt", next);
                changed = true;
            }
        }
        Object queryObj = requestJson.get("query");
        if (queryObj instanceof String s) {
            String next = transformer.apply(s);
            if (!s.equals(next)) {
                requestJson.put("query", next);
                changed = true;
            }
        }
        if (changed) {
            this.requestBodyMutated = true;
        }
        return changed;
    }
}
