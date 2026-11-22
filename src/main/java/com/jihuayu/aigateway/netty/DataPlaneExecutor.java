package com.jihuayu.aigateway.netty;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jihuayu.aigateway.hot.GatewayException;
import com.jihuayu.aigateway.hot.HotBilling;
import com.jihuayu.aigateway.hot.HotContract;
import com.jihuayu.aigateway.hot.HotContract.AdmitInput;
import com.jihuayu.aigateway.hot.HotContract.AttemptRecord;
import com.jihuayu.aigateway.hot.HotContract.KeyContext;
import com.jihuayu.aigateway.hot.HotContract.RouteSnapshot;
import com.jihuayu.aigateway.hot.HotContract.RouteTarget;
import com.jihuayu.aigateway.hot.HotContract.SettleInput;
import com.jihuayu.aigateway.hot.HotRuntime;
import com.jihuayu.aigateway.security.SecurityUtils;
import java.io.InputStream;
import java.io.OutputStream;
import java.math.BigInteger;
import java.net.HttpURLConnection;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.net.ssl.SSLException;

public class DataPlaneExecutor {
    public static final int MAX_BODY_BYTES = 1024 * 1024;
    public static final int MAX_MESSAGES = 200;
    public static final int MAX_MESSAGE_CHARS = 200_000;
    public static final int MAX_TOTAL_CHARS = 400_000;
    public static final int DEFAULT_MAX_OUTPUT_TOKENS = 4_096;
    public static final int MAX_MAX_OUTPUT_TOKENS = 32_768;
    public static final int MAX_UPSTREAM_JSON_BYTES = 8 * 1024 * 1024;
    public static final int MAX_SSE_EVENT_BYTES = 1024 * 1024;
    public static final int MAX_ATTEMPTS = 2;
    public static final int MAX_TOOLS = 64;
    public static final int MAX_EMBEDDING_INPUTS = 128;
    public static final int MAX_VISION_DATA_URL_BYTES = 512 * 1024;

    public static final Set<String> KNOWN_CAPABILITIES = Set.of(
        "chat", "stream", "stream_usage", "tools", "json_object", "json_schema",
        "embeddings", "vision", "anthropic_messages"
    );

    private static final Set<String> ALLOWED_CHAT_KEYS = Set.of(
        "model", "messages", "stream", "temperature", "max_tokens",
        "tools", "tool_choice", "response_format"
    );
    private static final Set<String> ALLOWED_MSG_KEYS = Set.of(
        "role", "content", "name", "tool_calls", "tool_call_id"
    );
    private static final Set<String> ALLOWED_ROLES = Set.of("system", "user", "assistant", "tool");
    private static final Set<String> ALLOWED_EMBED_KEYS = Set.of("model", "input", "encoding_format", "dimensions");
    private static final Set<String> ALLOWED_ANTHROPIC_KEYS = Set.of(
        "model", "messages", "max_tokens", "system", "temperature", "stream", "tools"
    );
    private static final Pattern DATA_IMAGE_RE = Pattern.compile("^data:(image/(?:png|jpeg|webp|gif));base64,([A-Za-z0-9+/=]+)$");

    public record Timings(int connectMs, int firstByteMs, int idleMs, int totalMs, int heartbeatMs) {
        public static final Timings DEFAULT = new Timings(5_000, 30_000, 30_000, 120_000, 10_000);
    }

    public record ChatMessage(
        String role,
        Object content,
        String textSummary,
        boolean hasVision,
        List<String> visionUrls,
        List<Map<String, Object>> toolCalls,
        String toolCallId
    ) {}

    public record ChatRequest(
        String model,
        List<ChatMessage> messages,
        boolean stream,
        Double temperature,
        Integer maxTokens,
        List<Map<String, Object>> tools,
        Object toolChoice,
        Map<String, Object> responseFormat,
        Set<String> requiredCapabilities
    ) {}

    public record EmbeddingRequest(
        String model,
        List<String> inputs,
        String encodingFormat,
        Integer dimensions
    ) {}

    public record PriceEntry(
        int attempt_no,
        String target_id,
        String upstream_model,
        long input_micros_per_million,
        long output_micros_per_million,
        int max_output_tokens,
        int estimated_input_tokens,
        long reserved_micros
    ) {}

    public record PreparedRequest(
        KeyContext key,
        ChatRequest body,
        RouteSnapshot route,
        List<RouteTarget> targets,
        int maxOutputTokens,
        Map<String, Object> priceSnapshot,
        List<PriceEntry> priceEntries,
        long totalReservedMicros,
        String period,
        Map<String, Object> routeVersion
    ) {}

    public record TokenUsage(long input, long output) {}

    public record SseEvent(String event, String data) {}

    private static final Map<String, Map.Entry<Integer, String>> ERROR_TABLE = Map.ofEntries(
        Map.entry("upstream_connect_failed", Map.entry(502, "无法连接上游服务。")),
        Map.entry("upstream_tls_error", Map.entry(502, "上游 TLS 校验失败。")),
        Map.entry("upstream_connect_timeout", Map.entry(504, "连接上游超时。")),
        Map.entry("upstream_first_byte_timeout", Map.entry(504, "等待上游响应超时。")),
        Map.entry("upstream_idle_timeout", Map.entry(504, "上游响应中断超时。")),
        Map.entry("upstream_timeout", Map.entry(504, "上游请求超时。")),
        Map.entry("request_timeout", Map.entry(504, "请求超过整体时间上限。")),
        Map.entry("upstream_invalid_request", Map.entry(400, "上游拒绝了请求参数。")),
        Map.entry("upstream_auth_failed", Map.entry(502, "上游鉴权失败。")),
        Map.entry("upstream_model_unavailable", Map.entry(502, "上游模型不可用。")),
        Map.entry("upstream_rate_limited", Map.entry(429, "上游限流，请稍后重试。")),
        Map.entry("upstream_unavailable", Map.entry(503, "上游服务暂时不可用。")),
        Map.entry("upstream_error", Map.entry(502, "上游服务返回错误。")),
        Map.entry("upstream_redirect", Map.entry(502, "上游返回了重定向。")),
        Map.entry("upstream_protocol_error", Map.entry(502, "上游响应格式不符合协议。")),
        Map.entry("upstream_empty_stream", Map.entry(502, "上游返回了空的流式响应。")),
        Map.entry("upstream_stream_error", Map.entry(502, "上游在流式响应中返回错误。")),
        Map.entry("upstream_invalid_response", Map.entry(502, "上游返回了无法识别的响应。")),
        Map.entry("upstream_response_too_large", Map.entry(502, "上游响应体过大。")),
        Map.entry("upstream_connection_error", Map.entry(502, "上游连接中断。")),
        Map.entry("upstream_address_rejected", Map.entry(502, "上游地址未通过安全校验。")),
        Map.entry("upstream_credentials_invalid", Map.entry(502, "上游凭据不可用。")),
        Map.entry("invalid_provider_url", Map.entry(502, "上游服务地址无效。")),
        Map.entry("target_disabled", Map.entry(503, "模型当前没有可用目标。")),
        Map.entry("lease_lost", Map.entry(503, "请求租约失效。")),
        Map.entry("client_cancelled", Map.entry(499, "客户端已取消请求。")),
        Map.entry("client_write_timeout", Map.entry(504, "向客户端写入响应超时。")),
        Map.entry("upstream_incomplete_stream", Map.entry(502, "上游流式响应未正常结束。"))
    );

    private final HotRuntime runtime;
    private final HotBilling billing;
    private final String encryptionKey;
    private final boolean allowPrivateUpstream;
    private final ObjectMapper mapper;
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2, Thread.ofVirtual().factory());
    private volatile Timings timings = Timings.DEFAULT;

    public DataPlaneExecutor(
        HotRuntime runtime,
        HotBilling billing,
        String encryptionKey,
        boolean allowPrivateUpstream,
        ObjectMapper mapper
    ) {
        this.runtime = runtime;
        this.billing = billing;
        this.encryptionKey = encryptionKey;
        this.allowPrivateUpstream = allowPrivateUpstream;
        this.mapper = mapper;
    }

    public void setTimings(Timings timings) {
        this.timings = timings != null ? timings : Timings.DEFAULT;
    }

    public Timings getTimings() {
        return timings;
    }

    public void close() {
        scheduler.shutdownNow();
    }

    public record HttpResponsePayload(int status, Map<String, String> headers, String jsonBody) {}

    public HttpResponsePayload handleModels(String authorizationHeader, String requestId) {
        try {
            KeyContext key = runtime.authenticateBearerHeader(authorizationHeader);
            List<Map<String, Object>> rows = runtime.models(key);
            long nowSec = Instant.now().getEpochSecond();
            List<Map<String, Object>> data = new ArrayList<>();
            for (Map<String, Object> row : rows) {
                String id = String.valueOf(row.get("id"));
                String name = String.valueOf(row.get("name"));
                if (!key.canUseModel(id, name)) continue;
                long created = nowSec;
                Object createdAt = row.get("created_at");
                if (createdAt instanceof String s && !s.isBlank()) {
                    try { created = Instant.parse(s).getEpochSecond(); } catch (Exception ignored) {}
                }
                Map<String, Object> entry = new LinkedHashMap<>();
                entry.put("id", name);
                entry.put("object", "model");
                entry.put("created", created);
                entry.put("owned_by", "ai-gateway");
                data.add(entry);
            }
            Map<String, Object> body = new LinkedHashMap<>();
            body.put("object", "list");
            body.put("data", data);
            body.put("items", data);
            return new HttpResponsePayload(200, Map.of("x-request-id", requestId), mapper.writeValueAsString(body));
        } catch (Exception e) {
            return buildErrorPayload(requestId, e, null);
        }
    }

    public interface ResponseSender {
        boolean isClientClosed();
        void sendJson(int status, String requestId, String jsonBody, Integer retryAfterSec);
        void startSse(String requestId);
        boolean isSseStarted();
        void writeSseFrame(String frame, long deadlineAt);
        void finishSse();
    }

    public void handleChat(String authorizationHeader, byte[] rawBody, String requestId, ResponseSender sender) {
        Timings t = this.timings;
        long deadlineAt = System.currentTimeMillis() + t.totalMs();
        List<AttemptRecord> attempts = new ArrayList<>();
        Set<Integer> persistedAttempts = new HashSet<>();
        AtomicReference<HttpURLConnection> activeConn = new AtomicReference<>();
        AtomicReference<String> abortReason = new AtomicReference<>();
        PreparedRequest prepared = null;
        boolean admitted = false;
        boolean dispatched = false;
        ScheduledFuture<?> heartbeatTask = null;
        String finalStatus = "failed";

        try {
            if (rawBody != null && rawBody.length > MAX_BODY_BYTES) {
                throw new GatewayException(400, "invalid_request", "请求体超过大小上限。");
            }
            KeyContext key = runtime.authenticateBearerHeader(authorizationHeader);
            Object parsedBody;
            try {
                if (rawBody == null || rawBody.length == 0) {
                    throw new GatewayException(400, "invalid_request", "请求体必须是 JSON 对象。");
                }
                parsedBody = mapper.readValue(rawBody, Object.class);
            } catch (GatewayException ge) {
                throw ge;
            } catch (Exception e) {
                throw new GatewayException(400, "invalid_request", "请求体必须是合法 JSON。");
            }
            ChatRequest chatReq = validateChatBody(parsedBody, allowPrivateUpstream);
            if (!key.canUseModel("", chatReq.model())) {
                throw new GatewayException(403, "model_not_permitted", "API Key 无权使用该模型。");
            }
            RouteSnapshot route = runtime.loadRoute(key, chatReq.model());
            if (route == null) {
                throw new GatewayException(404, "model_not_found", "模型不存在或未发布。");
            }
            if (!key.canUseModel(route.aliasId(), route.aliasName())) {
                throw new GatewayException(403, "model_not_permitted", "API Key 无权使用该模型。");
            }
            verifyCapabilities(route, chatReq.requiredCapabilities());
            prepared = prepareRequest(key, chatReq, route);
            admitRequest(requestId, prepared);
            admitted = true;
            billing.dispatch(requestId);
            dispatched = true;
        } catch (Exception e) {
            finishRequest(requestId, admitted, dispatched, prepared, "failed", attempts, persistedAttempts, heartbeatTask);
            if (!sender.isSseStarted() && !sender.isClientClosed()) {
                HttpResponsePayload errPayload = buildErrorPayload(requestId, e, retryAfterOf(e));
                String retryHeader = errPayload.headers().get("retry-after");
                sender.sendJson(errPayload.status(), requestId, errPayload.jsonBody(), retryHeader != null ? Integer.parseInt(retryHeader) : null);
            }
            return;
        }

        heartbeatTask = scheduler.scheduleAtFixedRate(() -> {
            try {
                billing.heartbeat(requestId);
            } catch (Exception e) {
                abortReason.compareAndSet(null, "lease_lost");
                HttpURLConnection conn = activeConn.get();
                if (conn != null) {
                    try { conn.disconnect(); } catch (Exception ignored) {}
                }
            }
        }, t.heartbeatMs(), t.heartbeatMs(), TimeUnit.MILLISECONDS);

        final PreparedRequest prep = prepared;
        try {
            if (prep.body().stream()) {
                finalStatus = executeStreamFlow(requestId, prep, t, deadlineAt, attempts, persistedAttempts, activeConn, abortReason, sender);
            } else {
                finalStatus = executeBufferedFlow(requestId, prep, t, deadlineAt, attempts, persistedAttempts, activeConn, abortReason, sender);
            }
        } catch (Exception e) {
            finalStatus = "failed";
            if (!sender.isSseStarted() && !sender.isClientClosed()) {
                HttpResponsePayload errPayload = buildErrorPayload(requestId, e, null);
                sender.sendJson(errPayload.status(), requestId, errPayload.jsonBody(), null);
            }
        } finally {
            finishRequest(requestId, admitted, dispatched, prep, finalStatus, attempts, persistedAttempts, heartbeatTask);
        }
    }

    /** P1-D: POST /v1/embeddings independent endpoint with zero-SQL Redis admission & settlement */
    @SuppressWarnings("unchecked")
    public void handleEmbeddings(String authorizationHeader, byte[] rawBody, String requestId, ResponseSender sender) {
        Timings t = this.timings;
        long deadlineAt = System.currentTimeMillis() + t.totalMs();
        List<AttemptRecord> attempts = new ArrayList<>();
        Set<Integer> persistedAttempts = new HashSet<>();
        PreparedRequest prepared = null;
        boolean admitted = false;
        boolean dispatched = false;
        String finalStatus = "failed";

        try {
            if (rawBody != null && rawBody.length > MAX_BODY_BYTES) {
                throw new GatewayException(400, "invalid_request", "请求体超过大小上限。");
            }
            KeyContext key = runtime.authenticateBearerHeader(authorizationHeader);
            Object parsedBody;
            try {
                if (rawBody == null || rawBody.length == 0) {
                    throw new GatewayException(400, "invalid_request", "请求体必须是 JSON 对象。");
                }
                parsedBody = mapper.readValue(rawBody, Object.class);
            } catch (GatewayException ge) {
                throw ge;
            } catch (Exception e) {
                throw new GatewayException(400, "invalid_request", "请求体必须是合法 JSON。");
            }
            EmbeddingRequest embReq = validateEmbeddingBody(parsedBody);
            if (!key.canUseModel("", embReq.model())) {
                throw new GatewayException(403, "model_not_permitted", "API Key 无权使用该模型。");
            }
            RouteSnapshot route = runtime.loadRoute(key, embReq.model());
            if (route == null) {
                throw new GatewayException(404, "model_not_found", "模型不存在或未发布。");
            }
            if (!key.canUseModel(route.aliasId(), route.aliasName())) {
                throw new GatewayException(403, "model_not_permitted", "API Key 无权使用该模型。");
            }
            verifyCapabilities(route, Set.of("embeddings"));

            List<RouteTarget> targets = route.targets().stream()
                .filter(rt -> rt.targetEnabled() && rt.providerEnabled())
                .limit(MAX_ATTEMPTS)
                .toList();
            if (targets.isEmpty()) {
                throw new GatewayException(503, "model_unavailable", "模型当前没有可用目标。");
            }
            long chars = embReq.inputs().stream().mapToLong(String::length).sum();
            int estInputTokens = (int) Math.max(1L, (chars + 2) / 3 + (long) embReq.inputs().size() * 2);
            List<PriceEntry> entries = new ArrayList<>(targets.size());
            long totalReserved = 0;
            for (int i = 0; i < targets.size(); i++) {
                RouteTarget target = targets.get(i);
                long res = tokensToMicros(estInputTokens, target.inputPrice());
                totalReserved += res;
                entries.add(new PriceEntry(i + 1, target.targetId(), target.upstreamModel(), target.inputPrice(), 0L, 0, estInputTokens, res));
            }
            String period = utcPeriod();
            Map<String, Object> priceSnap = new LinkedHashMap<>();
            priceSnap.put("version", 1);
            priceSnap.put("currency", "USD");
            priceSnap.put("unit", "tokens");
            priceSnap.put("period", period);
            priceSnap.put("max_attempts", entries.size());
            priceSnap.put("estimated_input_tokens", estInputTokens);
            priceSnap.put("max_output_tokens", 0);
            priceSnap.put("total_reserved_micros", totalReserved);
            priceSnap.put("attempts", entries);

            Map<String, Object> routeVer = new LinkedHashMap<>();
            routeVer.put("alias_id", route.aliasId());
            routeVer.put("alias_name", route.aliasName());
            routeVer.put("version_id", route.routeVersionId());
            routeVer.put("runtime_revision", key.runtimeRevision());
            routeVer.put("runtime_key_hash", key.runtimeKeyHash());
            routeVer.put("runtime_expires_at", key.runtimeExpiresAt());
            routeVer.put("version", route.version());
            routeVer.put("targets", entries.stream().map(e -> Map.of("target_id", e.target_id(), "upstream_model", e.upstream_model())).toList());

            ChatRequest syntheticChat = new ChatRequest(embReq.model(), List.of(), false, null, null, null, null, null, Set.of("embeddings"));
            prepared = new PreparedRequest(key, syntheticChat, route, targets, 0, priceSnap, entries, totalReserved, period, routeVer);

            admitRequest(requestId, prepared);
            admitted = true;
            billing.dispatch(requestId);
            dispatched = true;

            GatewayException lastErr = null;
            for (int i = 0; i < targets.size(); i++) {
                RouteTarget target = targets.get(i);
                PriceEntry pe = entries.get(i);
                long started = System.currentTimeMillis();
                HttpURLConnection conn = null;
                try {
                    String token = providerToken(target.encryptedSecret(), encryptionKey);
                    URI endpoint = embeddingsEndpoint(target.baseUrl());
                    SecurityUtils.validateProviderUrl(endpoint.toString(), allowPrivateUpstream);
                    Map<String, Object> upPayload = new LinkedHashMap<>();
                    upPayload.put("model", target.upstreamModel());
                    upPayload.put("input", embReq.inputs().size() == 1 ? embReq.inputs().get(0) : embReq.inputs());
                    if (embReq.encodingFormat() != null) upPayload.put("encoding_format", embReq.encodingFormat());
                    if (embReq.dimensions() != null) upPayload.put("dimensions", embReq.dimensions());
                    byte[] outBytes = mapper.writeValueAsBytes(upPayload);

                    conn = (HttpURLConnection) endpoint.toURL().openConnection();
                    conn.setInstanceFollowRedirects(false);
                    conn.setRequestMethod("POST");
                    conn.setDoOutput(true);
                    conn.setConnectTimeout(t.connectMs());
                    conn.setReadTimeout(t.firstByteMs());
                    conn.setRequestProperty("Content-Type", "application/json");
                    conn.setRequestProperty("Accept", "application/json");
                    conn.setRequestProperty("Authorization", "Bearer " + token);
                    conn.setRequestProperty("X-Request-Id", requestId);
                    conn.setFixedLengthStreamingMode(outBytes.length);
                    conn.connect();
                    try (OutputStream os = conn.getOutputStream()) {
                        os.write(outBytes);
                        os.flush();
                    }
                    int code = conn.getResponseCode();
                    long lat = Math.max(0L, System.currentTimeMillis() - started);
                    if (code < 200 || code >= 300) {
                        UpstreamHttpErr info = upstreamHttpError(code);
                        AttemptRecord ar = new AttemptRecord(
                            pe.attempt_no(), target.targetId(), target.upstreamModel(), "error", code, info.code(),
                            code != 429, code != 429, code == 429, code == 429 ? 0L : null, code == 429 ? 0L : null, code == 429 ? 0L : null,
                            pe.reserved_micros(), info.retrySafe(), lat
                        );
                        attempts.add(ar);
                        lastErr = errorFromRecord(ar);
                        if (!canFallback(ar, i, targets.size())) break;
                        continue;
                    }
                    byte[] rawResp = readBoundedStream(conn.getInputStream(), MAX_UPSTREAM_JSON_BYTES, new AtomicReference<>());
                    Map<String, Object> parsed = new LinkedHashMap<>(mapper.readValue(rawResp, Map.class));
                    if (!(parsed.get("data") instanceof List<?> dataList) || dataList.isEmpty()) {
                        throw new GatewayException(502, "upstream_invalid_response", "上游嵌入响应无效。");
                    }
                    parsed.put("model", route.aliasName());
                    Long pTokens = null;
                    if (parsed.get("usage") instanceof Map<?, ?> uMap && uMap.get("prompt_tokens") instanceof Number n && n.longValue() >= 0) {
                        pTokens = n.longValue();
                    }
                    Long cost = pTokens != null ? tokensToMicros(pTokens, target.inputPrice()) : null;
                    AttemptRecord okRec = new AttemptRecord(
                        pe.attempt_no(), target.targetId(), target.upstreamModel(), "ok", code, null,
                        true, false, pTokens != null, pTokens, pTokens != null ? 0L : null, cost,
                        pe.reserved_micros(), false, lat
                    );
                    attempts.add(okRec);
                    sender.sendJson(200, requestId, mapper.writeValueAsString(parsed), null);
                    finalStatus = "completed";
                    return;
                } catch (GatewayException ge) {
                    long lat = Math.max(0L, System.currentTimeMillis() - started);
                    AttemptRecord ar = new AttemptRecord(
                        pe.attempt_no(), target.targetId(), target.upstreamModel(), "error", null, ge.getCode(),
                        true, true, false, null, null, null, pe.reserved_micros(), false, lat
                    );
                    attempts.add(ar);
                    lastErr = ge;
                    break;
                } catch (Exception ex) {
                    long lat = Math.max(0L, System.currentTimeMillis() - started);
                    AttemptRecord ar = new AttemptRecord(
                        pe.attempt_no(), target.targetId(), target.upstreamModel(), "error", null, "upstream_connect_failed",
                        false, false, true, 0L, 0L, 0L, pe.reserved_micros(), true, lat
                    );
                    attempts.add(ar);
                    lastErr = errorFromRecord(ar);
                    if (!canFallback(ar, i, targets.size())) break;
                } finally {
                    if (conn != null) try { conn.disconnect(); } catch (Exception ignored) {}
                }
            }
            HttpResponsePayload errPayload = buildErrorPayload(requestId, lastErr != null ? lastErr : new GatewayException(502, "upstream_error", "上游调用失败。"), null);
            sender.sendJson(errPayload.status(), requestId, errPayload.jsonBody(), null);
        } catch (Exception e) {
            if (!sender.isClientClosed()) {
                HttpResponsePayload errPayload = buildErrorPayload(requestId, e, retryAfterOf(e));
                sender.sendJson(errPayload.status(), requestId, errPayload.jsonBody(), null);
            }
        } finally {
            finishRequest(requestId, admitted, dispatched, prepared, finalStatus, attempts, persistedAttempts, null);
        }
    }

    /** P1-F: Native Anthropic Messages Protocol Adapter (POST /v1/messages) */
    @SuppressWarnings("unchecked")
    public void handleAnthropicMessages(String authHeader, String xApiKeyHeader, byte[] rawBody, String requestId, ResponseSender sender) {
        String effectiveAuth = (authHeader != null && !authHeader.isBlank()) ? authHeader
            : (xApiKeyHeader != null && !xApiKeyHeader.isBlank() ? "Bearer " + xApiKeyHeader.trim() : null);
        try {
            if (rawBody == null || rawBody.length == 0 || rawBody.length > MAX_BODY_BYTES) {
                throw new GatewayException(400, "invalid_request", "请求体无效或超过大小限制。");
            }
            Object parsedObj = mapper.readValue(rawBody, Object.class);
            if (!(parsedObj instanceof Map<?, ?> rawMap)) {
                throw new GatewayException(400, "invalid_request", "请求体必须是 JSON 对象。");
            }
            Map<String, Object> anthroReq = (Map<String, Object>) rawMap;
            for (String k : anthroReq.keySet()) {
                if (!ALLOWED_ANTHROPIC_KEYS.contains(k)) {
                    throw new GatewayException(400, "unsupported_parameter", "不支持的 Anthropic 参数：" + k);
                }
            }
            if (!(anthroReq.get("max_tokens") instanceof Number maxTokNum) || maxTokNum.intValue() < 1) {
                throw new GatewayException(400, "invalid_request", "max_tokens 是必填正整数。");
            }
            List<Map<String, Object>> openaiMsgs = new ArrayList<>();
            if (anthroReq.get("system") instanceof String sys && !sys.isBlank()) {
                openaiMsgs.add(Map.of("role", "system", "content", sys));
            }
            if (!(anthroReq.get("messages") instanceof List<?> mList) || mList.isEmpty()) {
                throw new GatewayException(400, "invalid_request", "messages 不能为空。");
            }
            for (Object item : mList) {
                if (!(item instanceof Map<?, ?> m)) throw new GatewayException(400, "invalid_request", "message 格式错误");
                String role = String.valueOf(m.get("role"));
                if (!"user".equals(role) && !"assistant".equals(role)) {
                    throw new GatewayException(400, "unsupported_role", "Anthropic messages 仅支持 user 与 assistant 角色。");
                }
                Object contentObj = m.get("content");
                String textContent;
                if (contentObj instanceof String s) {
                    textContent = s;
                } else if (contentObj instanceof List<?> blocks) {
                    StringBuilder sb = new StringBuilder();
                    for (Object b : blocks) {
                        if (b instanceof Map<?, ?> bm && "text".equals(bm.get("type")) && bm.get("text") instanceof String ts) {
                            sb.append(ts);
                        }
                    }
                    textContent = sb.toString();
                } else {
                    throw new GatewayException(400, "invalid_request", "content 格式无效");
                }
                openaiMsgs.add(Map.of("role", role, "content", textContent));
            }
            Map<String, Object> translated = new LinkedHashMap<>();
            translated.put("model", anthroReq.get("model"));
            translated.put("messages", openaiMsgs);
            translated.put("max_tokens", maxTokNum.intValue());
            if (anthroReq.get("temperature") != null) translated.put("temperature", anthroReq.get("temperature"));
            boolean stream = Boolean.TRUE.equals(anthroReq.get("stream"));
            translated.put("stream", false); // Normalized buffered execution, translated back to Anthropic format

            AtomicReference<Integer> statusRef = new AtomicReference<>(200);
            AtomicReference<String> bodyRef = new AtomicReference<>("{}");
            AtomicReference<Integer> retryRef = new AtomicReference<>();

            ResponseSender capture = new ResponseSender() {
                @Override public boolean isClientClosed() { return sender.isClientClosed(); }
                @Override public void sendJson(int status, String reqId, String jsonBody, Integer retryAfterSec) {
                    statusRef.set(status);
                    bodyRef.set(jsonBody);
                    retryRef.set(retryAfterSec);
                }
                @Override public void startSse(String reqId) {}
                @Override public boolean isSseStarted() { return false; }
                @Override public void writeSseFrame(String frame, long deadlineAt) {}
                @Override public void finishSse() {}
            };

            handleChat(effectiveAuth, mapper.writeValueAsBytes(translated), requestId, capture);

            if (statusRef.get() != 200) {
                sender.sendJson(statusRef.get(), requestId, bodyRef.get(), retryRef.get());
                return;
            }
            Map<?, ?> openaiResp = mapper.readValue(bodyRef.get(), Map.class);
            List<?> choices = (List<?>) openaiResp.get("choices");
            Map<?, ?> firstChoice = (choices != null && !choices.isEmpty()) ? (Map<?, ?>) choices.get(0) : Map.of();
            Map<?, ?> message = firstChoice.get("message") instanceof Map<?, ?> mm ? mm : Map.of();
            String text = message.get("content") instanceof String s ? s : "";
            Map<?, ?> usage = openaiResp.get("usage") instanceof Map<?, ?> um ? um : null;

            Map<String, Object> anthroOut = new LinkedHashMap<>();
            anthroOut.put("id", "msg_" + requestId);
            anthroOut.put("type", "message");
            anthroOut.put("role", "assistant");
            anthroOut.put("model", openaiResp.get("model"));
            anthroOut.put("content", List.of(Map.of("type", "text", "text", text)));
            anthroOut.put("stop_reason", "end_turn");
            if (usage != null) {
                anthroOut.put("usage", Map.of(
                    "input_tokens", usage.get("prompt_tokens"),
                    "output_tokens", usage.get("completion_tokens")
                ));
            } else {
                anthroOut.put("usage", null);
            }

            if (stream) {
                long deadline = System.currentTimeMillis() + 30_000L;
                sender.startSse(requestId);
                sender.writeSseFrame("event: message_start\ndata: " + mapper.writeValueAsString(Map.of("type", "message_start", "message", anthroOut)) + "\n\n", deadline);
                sender.writeSseFrame("event: content_block_delta\ndata: " + mapper.writeValueAsString(Map.of("type", "content_block_delta", "index", 0, "delta", Map.of("type", "text_delta", "text", text))) + "\n\n", deadline);
                sender.writeSseFrame("event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n", deadline);
                sender.finishSse();
            } else {
                sender.sendJson(200, requestId, mapper.writeValueAsString(anthroOut), null);
            }
        } catch (Exception e) {
            HttpResponsePayload errPayload = buildErrorPayload(requestId, e, retryAfterOf(e));
            sender.sendJson(errPayload.status(), requestId, errPayload.jsonBody(), null);
        }
    }

    /** P1-A: Capability Registry Enforcement against route snapshot and active targets */
    public static void verifyCapabilities(RouteSnapshot route, Set<String> required) {
        if (required == null || required.isEmpty()) return;
        for (String cap : required) {
            if (!KNOWN_CAPABILITIES.contains(cap)) {
                throw new GatewayException(400, "capability_not_supported", "未知或不支持的能力：" + cap);
            }
            // If the route alias explicitly declared capabilities, it must cover the required P1 capability
            if (route.capabilities() != null && !route.capabilities().isEmpty() && !route.capabilities().contains(cap)) {
                if (!"chat".equals(cap) && !"stream".equals(cap)) {
                    throw new GatewayException(400, "capability_not_supported", "当前模型别名未声明支持能力：" + cap);
                }
            }
            for (RouteTarget target : route.targets()) {
                if (!target.targetEnabled() || !target.providerEnabled()) continue;
                if (!"chat".equals(cap) && !"stream".equals(cap)) {
                    if (target.capabilities() == null || !target.capabilities().contains(cap)) {
                        throw new GatewayException(400, "capability_not_supported", "模型目标 " + target.upstreamModel() + " 不支持能力：" + cap);
                    }
                }
            }
        }
    }

    private void finishRequest(
        String requestId,
        boolean admitted,
        boolean dispatched,
        PreparedRequest prepared,
        String status,
        List<AttemptRecord> attempts,
        Set<Integer> persistedAttempts,
        ScheduledFuture<?> heartbeatTask
    ) {
        if (heartbeatTask != null) {
            heartbeatTask.cancel(false);
        }
        if (!admitted) return;
        if (!dispatched) {
            try { billing.cancelBeforeDispatch(requestId); } catch (Exception ignored) {}
            return;
        }
        if (prepared != null) {
            try {
                billing.persistMetadata(requestId, prepared.route().aliasName(), status, attempts);
            } catch (Exception ignored) {}
        }
        AggregatedTotals totals = aggregateSettlement(attempts);
        try {
            billing.settle(requestId, new SettleInput(
                totals.actualMicros(),
                totals.inputTokens(),
                totals.outputTokens(),
                status,
                attempts
            ));
        } catch (Exception ignored) {}
    }

    private String executeBufferedFlow(
        String requestId,
        PreparedRequest prepared,
        Timings t,
        long deadlineAt,
        List<AttemptRecord> attempts,
        Set<Integer> persistedAttempts,
        AtomicReference<HttpURLConnection> activeConn,
        AtomicReference<String> abortReason,
        ResponseSender sender
    ) {
        GatewayException lastError = null;
        Integer retryAfter = null;
        boolean timedOut = false;

        for (int index = 0; index < prepared.targets().size(); index++) {
            if (sender.isClientClosed()) {
                lastError = new GatewayException(499, "client_cancelled", "客户端已取消请求。");
                break;
            }
            if (System.currentTimeMillis() >= deadlineAt) {
                timedOut = true;
                lastError = new GatewayException(504, "request_timeout", "请求超过整体时间上限。");
                break;
            }
            BufferedAttemptResult result = runBufferedAttempt(
                requestId, prepared.route().aliasName(), prepared.body(), prepared.maxOutputTokens(),
                prepared.targets().get(index), prepared.priceEntries().get(index),
                t, deadlineAt, activeConn, abortReason, sender
            );
            attempts.add(result.record());
            try {
                billing.persistMetadata(requestId, prepared.route().aliasName(), "ok".equals(result.record().status()) ? "completed" : "failed", List.of(result.record()));
                persistedAttempts.add(result.record().attempt_no());
            } catch (Exception ignored) {}
            if (result.retryAfter() != null) retryAfter = result.retryAfter();
            if ("ok".equals(result.record().status()) && result.completionJson() != null) {
                sender.sendJson(200, requestId, result.completionJson(), null);
                return "completed";
            }
            lastError = errorFromRecord(result.record());
            String ec = result.record().error_code() != null ? result.record().error_code() : "";
            timedOut = timedOut || Set.of("request_timeout", "upstream_connect_timeout", "upstream_first_byte_timeout").contains(ec);
            if (!canFallback(result.record(), index, prepared.targets().size())) break;
        }
        if (sender.isClientClosed()) {
            return "cancelled";
        }
        GatewayException err = lastError != null ? lastError : new GatewayException(502, "upstream_error", "上游调用失败。");
        HttpResponsePayload payload = buildErrorPayload(requestId, err, retryAfter);
        sender.sendJson(payload.status(), requestId, payload.jsonBody(), retryAfter);
        return timedOut ? "timeout" : "failed";
    }

    private String executeStreamFlow(
        String requestId,
        PreparedRequest prepared,
        Timings t,
        long deadlineAt,
        List<AttemptRecord> attempts,
        Set<Integer> persistedAttempts,
        AtomicReference<HttpURLConnection> activeConn,
        AtomicReference<String> abortReason,
        ResponseSender sender
    ) {
        GatewayException lastError = null;
        Integer retryAfter = null;
        boolean timedOut = false;

        for (int index = 0; index < prepared.targets().size(); index++) {
            if (sender.isClientClosed()) return "cancelled";
            if (System.currentTimeMillis() >= deadlineAt) {
                timedOut = true;
                lastError = new GatewayException(504, "request_timeout", "请求超过整体时间上限。");
                break;
            }
            StreamAttemptResult result = runStreamAttempt(
                requestId, prepared.route().aliasName(), prepared.body(), prepared.maxOutputTokens(),
                prepared.targets().get(index), prepared.priceEntries().get(index),
                t, deadlineAt, activeConn, abortReason, sender
            );
            attempts.add(result.record());
            try {
                billing.persistMetadata(requestId, prepared.route().aliasName(), "ok".equals(result.record().status()) ? "completed" : "failed", List.of(result.record()));
                persistedAttempts.add(result.record().attempt_no());
            } catch (Exception ignored) {}
            if (result.retryAfter() != null) retryAfter = result.retryAfter();
            if ("ok".equals(result.record().status())) {
                sender.finishSse();
                return "completed";
            }
            if (sender.isSseStarted() || result.wroteAny()) {
                GatewayException failure = errorFromRecord(result.record());
                try {
                    Map<String, Object> errFrame = Map.of(
                        "error", Map.of("code", failure.getCode(), "message", failure.getMessage(), "type", "gateway_error"),
                        "request_id", requestId
                    );
                    sender.writeSseFrame(formatSseEvent(new SseEvent("error", mapper.writeValueAsString(errFrame))), deadlineAt);
                    sender.finishSse();
                } catch (Exception ignored) {}
                return "client_cancelled".equals(result.record().error_code()) ? "cancelled" : "partial";
            }
            lastError = errorFromRecord(result.record());
            String ec = result.record().error_code() != null ? result.record().error_code() : "";
            timedOut = timedOut || Set.of("request_timeout", "upstream_connect_timeout", "upstream_first_byte_timeout").contains(ec);
            if (!canFallback(result.record(), index, prepared.targets().size()) || sender.isClientClosed()) break;
        }
        if (sender.isClientClosed()) {
            return "cancelled";
        }
        if (!sender.isSseStarted()) {
            GatewayException err = lastError != null ? lastError : new GatewayException(502, "upstream_error", "上游调用失败。");
            HttpResponsePayload payload = buildErrorPayload(requestId, err, retryAfter);
            sender.sendJson(payload.status(), requestId, payload.jsonBody(), retryAfter);
        }
        return timedOut ? "timeout" : "failed";
    }

    private record BufferedAttemptResult(AttemptRecord record, String completionJson, Integer retryAfter) {}
    private record StreamAttemptResult(AttemptRecord record, boolean wroteAny, Integer retryAfter) {}

    private BufferedAttemptResult runBufferedAttempt(
        String requestId,
        String aliasName,
        ChatRequest body,
        int maxOutputTokens,
        RouteTarget target,
        PriceEntry entry,
        Timings timings,
        long deadlineAt,
        AtomicReference<HttpURLConnection> activeConn,
        AtomicReference<String> abortReason,
        ResponseSender sender
    ) {
        long started = System.currentTimeMillis();
        int attemptNo = entry.attempt_no();
        String targetId = target.targetId();
        String upstreamModel = target.upstreamModel();
        long upperBound = entry.reserved_micros();

        String status = "error";
        Integer httpStatus = null;
        String errorCode = null;
        boolean requestSent = false;
        boolean executionUnknown = false;
        boolean usageKnown = false;
        Long inputTokens = null;
        Long outputTokens = null;
        Long costMicros = null;
        boolean retrySafe = false;
        Integer retryAfter = null;
        String completionJson = null;

        HttpURLConnection conn = null;
        ScheduledFuture<?> exitWatch = null;
        try {
            if (!target.targetEnabled() || !target.providerEnabled()) {
                errorCode = "target_disabled";
                usageKnown = true;
                inputTokens = 0L;
                outputTokens = 0L;
                costMicros = 0L;
                long lat = Math.max(0L, System.currentTimeMillis() - started);
                return new BufferedAttemptResult(new AttemptRecord(
                    attemptNo, targetId, upstreamModel, status, httpStatus, errorCode,
                    false, false, usageKnown, inputTokens, outputTokens, costMicros, upperBound, false, lat
                ), null, null);
            }
            String token = providerToken(target.encryptedSecret(), encryptionKey);
            String payload = upstreamPayload(target, body, maxOutputTokens);
            URI endpoint = chatEndpoint(target.baseUrl());
            try {
                SecurityUtils.validateProviderUrl(endpoint.toString(), allowPrivateUpstream);
            } catch (Exception e) {
                throw new UpstreamFailureException("upstream_address_rejected", 502, "上游地址未通过安全校验。", false, false, false);
            }

            long remaining = Math.max(1L, deadlineAt - System.currentTimeMillis());
            conn = (HttpURLConnection) endpoint.toURL().openConnection();
            conn.setInstanceFollowRedirects(false);
            conn.setRequestMethod("POST");
            conn.setDoOutput(true);
            conn.setConnectTimeout((int) Math.min(timings.connectMs(), remaining));
            conn.setReadTimeout((int) Math.min(timings.firstByteMs(), remaining));
            conn.setRequestProperty("Content-Type", "application/json");
            conn.setRequestProperty("Accept", "application/json");
            conn.setRequestProperty("Authorization", "Bearer " + token);
            conn.setRequestProperty("User-Agent", "ai-gateway/0.1");
            conn.setRequestProperty("X-Request-Id", requestId);

            HttpURLConnection cRef = conn;
            activeConn.set(cRef);
            abortReason.set(null);
            exitWatch = scheduler.schedule(() -> {
                abortReason.compareAndSet(null, "request_timeout");
                try { cRef.disconnect(); } catch (Exception ignored) {}
            }, remaining, TimeUnit.MILLISECONDS);

            byte[] outBytes = payload.getBytes(StandardCharsets.UTF_8);
            conn.setFixedLengthStreamingMode(outBytes.length);
            try {
                conn.connect();
            } catch (SocketTimeoutException ste) {
                throw new UpstreamFailureException("upstream_connect_timeout", 504, "连接上游超时。", false, false, true);
            } catch (SSLException ssle) {
                throw new UpstreamFailureException("upstream_tls_error", 502, "上游 TLS 校验失败。", false, false, true);
            } catch (Exception ce) {
                throw classifyConnectException(ce, abortReason.get());
            }

            requestSent = true;
            try (OutputStream os = conn.getOutputStream()) {
                os.write(outBytes);
                os.flush();
            }
            int code;
            try {
                code = conn.getResponseCode();
            } catch (SocketTimeoutException ste) {
                String ab = abortReason.get();
                if ("request_timeout".equals(ab)) {
                    throw new UpstreamFailureException("request_timeout", 504, "请求超过整体时间上限。", true, true, false);
                }
                throw new UpstreamFailureException("upstream_first_byte_timeout", 504, "等待上游响应超时。", true, true, false);
            }
            httpStatus = code;
            retryAfter = parseRetryAfterHeader(conn.getHeaderField("Retry-After"));
            if (code < 200 || code >= 300) {
                UpstreamHttpErr info = upstreamHttpError(code);
                errorCode = info.code();
                retrySafe = info.retrySafe();
                executionUnknown = code != 429;
                if (code == 429) {
                    requestSent = false;
                    executionUnknown = false;
                    usageKnown = true;
                    inputTokens = 0L;
                    outputTokens = 0L;
                    costMicros = 0L;
                }
                long lat = Math.max(0L, System.currentTimeMillis() - started);
                return new BufferedAttemptResult(new AttemptRecord(
                    attemptNo, targetId, upstreamModel, status, httpStatus, errorCode,
                    requestSent, executionUnknown, usageKnown, inputTokens, outputTokens, costMicros, upperBound, retrySafe, lat
                ), null, retryAfter);
            }

            conn.setReadTimeout((int) Math.min(timings.idleMs(), Math.max(1L, deadlineAt - System.currentTimeMillis())));
            byte[] rawResponse = readBoundedStream(conn.getInputStream(), MAX_UPSTREAM_JSON_BYTES, abortReason);
            Object parsed;
            try {
                parsed = mapper.readValue(rawResponse, Object.class);
            } catch (Exception e) {
                errorCode = "upstream_invalid_response";
                executionUnknown = true;
                long lat = Math.max(0L, System.currentTimeMillis() - started);
                return new BufferedAttemptResult(new AttemptRecord(
                    attemptNo, targetId, upstreamModel, status, httpStatus, errorCode,
                    true, true, false, null, null, null, upperBound, false, lat
                ), null, null);
            }
            TokenUsage usage = parseUsage(parsed instanceof Map<?, ?> m ? m.get("usage") : null);
            if (usage != null) {
                usageKnown = true;
                inputTokens = usage.input();
                outputTokens = usage.output();
                costMicros = usageMicros(usage.input(), usage.output(), target.inputPrice(), target.outputPrice());
            }
            try {
                Map<String, Object> completion = normalizeCompletion(parsed, aliasName, requestId, usage, body.responseFormat() != null, mapper);
                completionJson = mapper.writeValueAsString(completion);
            } catch (Exception e) {
                errorCode = "upstream_invalid_response";
                executionUnknown = true;
                long lat = Math.max(0L, System.currentTimeMillis() - started);
                return new BufferedAttemptResult(new AttemptRecord(
                    attemptNo, targetId, upstreamModel, status, httpStatus, errorCode,
                    true, true, usageKnown, inputTokens, outputTokens, costMicros, upperBound, false, lat
                ), null, null);
            }
            status = "ok";
            errorCode = null;
        } catch (UpstreamFailureException ufe) {
            errorCode = ufe.code;
            requestSent = ufe.requestSent;
            executionUnknown = ufe.executionUnknown;
            retrySafe = ufe.retrySafe;
            if (!requestSent) {
                usageKnown = true;
                inputTokens = 0L;
                outputTokens = 0L;
                costMicros = 0L;
            }
        } catch (Exception e) {
            UpstreamFailureException ufe = classifyTransportException(e, abortReason.get(), requestSent);
            errorCode = ufe.code;
            requestSent = ufe.requestSent;
            executionUnknown = ufe.executionUnknown;
            retrySafe = ufe.retrySafe;
            if (!requestSent) {
                usageKnown = true;
                inputTokens = 0L;
                outputTokens = 0L;
                costMicros = 0L;
            }
        } finally {
            if (exitWatch != null) exitWatch.cancel(false);
            activeConn.set(null);
            if (conn != null) {
                try { conn.disconnect(); } catch (Exception ignored) {}
            }
        }
        long lat = Math.max(0L, System.currentTimeMillis() - started);
        return new BufferedAttemptResult(new AttemptRecord(
            attemptNo, targetId, upstreamModel, status, httpStatus, errorCode,
            requestSent, executionUnknown, usageKnown, inputTokens, outputTokens, costMicros, upperBound, retrySafe, lat
        ), completionJson, retryAfter);
    }

    @SuppressWarnings("unchecked")
    private StreamAttemptResult runStreamAttempt(
        String requestId,
        String aliasName,
        ChatRequest body,
        int maxOutputTokens,
        RouteTarget target,
        PriceEntry entry,
        Timings timings,
        long deadlineAt,
        AtomicReference<HttpURLConnection> activeConn,
        AtomicReference<String> abortReason,
        ResponseSender sender
    ) {
        long started = System.currentTimeMillis();
        int attemptNo = entry.attempt_no();
        String targetId = target.targetId();
        String upstreamModel = target.upstreamModel();
        long upperBound = entry.reserved_micros();

        String status = "error";
        Integer httpStatus = null;
        String errorCode = null;
        boolean requestSent = false;
        boolean executionUnknown = false;
        boolean usageKnown = false;
        Long inputTokens = null;
        Long outputTokens = null;
        Long costMicros = null;
        boolean retrySafe = false;
        Integer retryAfter = null;
        boolean wroteAny = false;
        boolean sawDone = false;

        HttpURLConnection conn = null;
        ScheduledFuture<?> exitWatch = null;
        try {
            if (!target.targetEnabled() || !target.providerEnabled()) {
                errorCode = "target_disabled";
                usageKnown = true;
                inputTokens = 0L;
                outputTokens = 0L;
                costMicros = 0L;
                long lat = Math.max(0L, System.currentTimeMillis() - started);
                return new StreamAttemptResult(new AttemptRecord(
                    attemptNo, targetId, upstreamModel, status, httpStatus, errorCode,
                    false, false, usageKnown, inputTokens, outputTokens, costMicros, upperBound, false, lat
                ), false, null);
            }
            String token = providerToken(target.encryptedSecret(), encryptionKey);
            String payload = upstreamPayload(target, body, maxOutputTokens);
            URI endpoint = chatEndpoint(target.baseUrl());
            try {
                SecurityUtils.validateProviderUrl(endpoint.toString(), allowPrivateUpstream);
            } catch (Exception e) {
                throw new UpstreamFailureException("upstream_address_rejected", 502, "上游地址未通过安全校验。", false, false, false);
            }

            long remaining = Math.max(1L, deadlineAt - System.currentTimeMillis());
            conn = (HttpURLConnection) endpoint.toURL().openConnection();
            conn.setInstanceFollowRedirects(false);
            conn.setRequestMethod("POST");
            conn.setDoOutput(true);
            conn.setConnectTimeout((int) Math.min(timings.connectMs(), remaining));
            conn.setReadTimeout((int) Math.min(timings.firstByteMs(), remaining));
            conn.setRequestProperty("Content-Type", "application/json");
            conn.setRequestProperty("Accept", "text/event-stream");
            conn.setRequestProperty("Authorization", "Bearer " + token);
            conn.setRequestProperty("User-Agent", "ai-gateway/0.1");
            conn.setRequestProperty("X-Request-Id", requestId);

            HttpURLConnection cRef = conn;
            activeConn.set(cRef);
            abortReason.set(null);
            exitWatch = scheduler.schedule(() -> {
                abortReason.compareAndSet(null, "request_timeout");
                try { cRef.disconnect(); } catch (Exception ignored) {}
            }, remaining, TimeUnit.MILLISECONDS);

            byte[] outBytes = payload.getBytes(StandardCharsets.UTF_8);
            conn.setFixedLengthStreamingMode(outBytes.length);
            try {
                conn.connect();
            } catch (SocketTimeoutException ste) {
                throw new UpstreamFailureException("upstream_connect_timeout", 504, "连接上游超时。", false, false, true);
            } catch (SSLException ssle) {
                throw new UpstreamFailureException("upstream_tls_error", 502, "上游 TLS 校验失败。", false, false, true);
            } catch (Exception ce) {
                throw classifyConnectException(ce, abortReason.get());
            }

            requestSent = true;
            try (OutputStream os = conn.getOutputStream()) {
                os.write(outBytes);
                os.flush();
            }
            int code;
            try {
                code = conn.getResponseCode();
            } catch (SocketTimeoutException ste) {
                String ab = abortReason.get();
                if ("request_timeout".equals(ab)) {
                    throw new UpstreamFailureException("request_timeout", 504, "请求超过整体时间上限。", true, true, false);
                }
                throw new UpstreamFailureException("upstream_first_byte_timeout", 504, "等待上游响应超时。", true, true, false);
            }
            httpStatus = code;
            retryAfter = parseRetryAfterHeader(conn.getHeaderField("Retry-After"));
            if (code < 200 || code >= 300) {
                UpstreamHttpErr info = upstreamHttpError(code);
                errorCode = info.code();
                retrySafe = info.retrySafe();
                executionUnknown = code != 429;
                if (code == 429) {
                    requestSent = false;
                    executionUnknown = false;
                    usageKnown = true;
                    inputTokens = 0L;
                    outputTokens = 0L;
                    costMicros = 0L;
                }
                long lat = Math.max(0L, System.currentTimeMillis() - started);
                return new StreamAttemptResult(new AttemptRecord(
                    attemptNo, targetId, upstreamModel, status, httpStatus, errorCode,
                    requestSent, executionUnknown, usageKnown, inputTokens, outputTokens, costMicros, upperBound, retrySafe, lat
                ), false, retryAfter);
            }

            String contentType = conn.getHeaderField("Content-Type");
            if (contentType == null || !contentType.toLowerCase().contains("text/event-stream")) {
                throw new UpstreamFailureException("upstream_protocol_error", 502, "上游未返回流式响应。", true, true, false);
            }

            conn.setReadTimeout((int) Math.min(timings.idleMs(), Math.max(1L, deadlineAt - System.currentTimeMillis())));
            SseParser parser = new SseParser();
            try (InputStream in = conn.getInputStream()) {
                byte[] buf = new byte[4096];
                int n;
                while ((n = in.read(buf)) != -1) {
                    if (sender.isClientClosed()) {
                        throw new GatewayException(499, "client_closed", "客户端已断开连接。");
                    }
                    conn.setReadTimeout((int) Math.min(timings.idleMs(), Math.max(1L, deadlineAt - System.currentTimeMillis())));
                    String chunkStr = new String(buf, 0, n, StandardCharsets.UTF_8);
                    for (SseEvent ev : parser.push(chunkStr)) {
                        if ("[DONE]".equals(ev.data())) {
                            sender.startSse(requestId);
                            sender.writeSseFrame("data: [DONE]\n\n", deadlineAt);
                            wroteAny = true;
                            sawDone = true;
                            break;
                        }
                        Map<String, Object> parsedObj = null;
                        try {
                            Object rawObj = mapper.readValue(ev.data(), Object.class);
                            if (rawObj instanceof Map<?, ?> m) {
                                parsedObj = new LinkedHashMap<>((Map<String, Object>) m);
                            }
                        } catch (Exception ignored) {}
                        if (parsedObj != null) {
                            if (parsedObj.containsKey("model")) {
                                parsedObj.put("model", aliasName);
                            }
                            TokenUsage u = parseUsage(parsedObj.get("usage"));
                            if (u != null) {
                                usageKnown = true;
                                inputTokens = u.input();
                                outputTokens = u.output();
                                costMicros = usageMicros(u.input(), u.output(), target.inputPrice(), target.outputPrice());
                            }
                            if (parsedObj.get("error") != null) {
                                throw new UpstreamFailureException("upstream_stream_error", 502, "上游在流式响应中返回错误。", true, true, false);
                            }
                            sender.startSse(requestId);
                            sender.writeSseFrame(formatSseEvent(new SseEvent(ev.event(), mapper.writeValueAsString(parsedObj))), deadlineAt);
                        } else {
                            sender.startSse(requestId);
                            sender.writeSseFrame(formatSseEvent(ev), deadlineAt);
                        }
                        wroteAny = true;
                    }
                    if (sawDone) break;
                }
                if (!sawDone) {
                    for (SseEvent ev : parser.flush()) {
                        if ("[DONE]".equals(ev.data())) {
                            sender.startSse(requestId);
                            sender.writeSseFrame("data: [DONE]\n\n", deadlineAt);
                            wroteAny = true;
                            sawDone = true;
                            break;
                        }
                    }
                }
            }
            if (!sawDone) {
                throw new UpstreamFailureException("upstream_incomplete_stream", 502, "上游流式响应未正常结束。", true, true, false);
            }
            status = "ok";
            errorCode = null;
        } catch (GatewayException ge) {
            if ("client_closed".equals(ge.getCode()) || "client_cancelled".equals(abortReason.get())) {
                errorCode = "client_cancelled";
                requestSent = true;
                executionUnknown = true;
            } else if ("client_write_timeout".equals(ge.getCode())) {
                errorCode = "client_write_timeout";
                requestSent = true;
                executionUnknown = true;
                retrySafe = false;
            } else {
                errorCode = ge.getCode();
                requestSent = true;
                executionUnknown = true;
            }
        } catch (UpstreamFailureException ufe) {
            errorCode = ufe.code;
            requestSent = ufe.requestSent;
            executionUnknown = ufe.executionUnknown;
            retrySafe = ufe.retrySafe;
            if (!requestSent) {
                usageKnown = true;
                inputTokens = 0L;
                outputTokens = 0L;
                costMicros = 0L;
            }
        } catch (Exception e) {
            if (sender.isClientClosed()) {
                errorCode = "client_cancelled";
                requestSent = true;
                executionUnknown = true;
            } else {
                UpstreamFailureException ufe = classifyTransportException(e, abortReason.get(), true);
                errorCode = ufe.code;
                requestSent = ufe.requestSent;
                executionUnknown = ufe.executionUnknown;
                retrySafe = ufe.retrySafe;
            }
        } finally {
            if (exitWatch != null) exitWatch.cancel(false);
            activeConn.set(null);
            if (conn != null) {
                try { conn.disconnect(); } catch (Exception ignored) {}
            }
        }
        long lat = Math.max(0L, System.currentTimeMillis() - started);
        return new StreamAttemptResult(new AttemptRecord(
            attemptNo, targetId, upstreamModel, status, httpStatus, errorCode,
            requestSent, executionUnknown, usageKnown, inputTokens, outputTokens, costMicros, upperBound, retrySafe, lat
        ), wroteAny, retryAfter);
    }

    private byte[] readBoundedStream(InputStream in, int maxBytes, AtomicReference<String> abortReason) throws Exception {
        try (in) {
            java.io.ByteArrayOutputStream bos = new java.io.ByteArrayOutputStream();
            byte[] buf = new byte[4096];
            int n;
            while ((n = in.read(buf)) != -1) {
                if (bos.size() + n > maxBytes) {
                    throw new UpstreamFailureException("upstream_response_too_large", 502, "上游响应体过大。", true, true, false);
                }
                bos.write(buf, 0, n);
            }
            return bos.toByteArray();
        } catch (SocketTimeoutException ste) {
            if ("request_timeout".equals(abortReason.get())) {
                throw new UpstreamFailureException("request_timeout", 504, "请求超过整体时间上限。", true, true, false);
            }
            throw new UpstreamFailureException("upstream_idle_timeout", 504, "上游响应中断超时。", true, true, false);
        }
    }

    public static class UpstreamFailureException extends RuntimeException {
        final String code;
        final int statusCode;
        final boolean requestSent;
        final boolean executionUnknown;
        final boolean retrySafe;

        public UpstreamFailureException(String code, int statusCode, String message, boolean requestSent, boolean executionUnknown, boolean retrySafe) {
            super(message);
            this.code = code;
            this.statusCode = statusCode;
            this.requestSent = requestSent;
            this.executionUnknown = executionUnknown;
            this.retrySafe = retrySafe;
        }
    }

    private UpstreamFailureException classifyConnectException(Exception e, String abortCode) {
        if ("lease_lost".equals(abortCode)) {
            return new UpstreamFailureException("lease_lost", 503, "请求租约失效。", true, true, false);
        }
        if ("request_timeout".equals(abortCode)) {
            return new UpstreamFailureException("request_timeout", 504, "请求超过整体时间上限。", true, true, false);
        }
        if (e instanceof SSLException) {
            return new UpstreamFailureException("upstream_tls_error", 502, "上游 TLS 校验失败。", false, false, true);
        }
        if (e instanceof SocketTimeoutException) {
            return new UpstreamFailureException("upstream_connect_timeout", 504, "连接上游超时。", false, false, true);
        }
        return new UpstreamFailureException("upstream_connect_failed", 502, "无法连接上游服务。", false, false, true);
    }

    private UpstreamFailureException classifyTransportException(Exception e, String abortCode, boolean alreadyConnected) {
        if ("lease_lost".equals(abortCode)) {
            return new UpstreamFailureException("lease_lost", 503, "请求租约失效。", true, true, false);
        }
        if ("request_timeout".equals(abortCode)) {
            return new UpstreamFailureException("request_timeout", 504, "请求超过整体时间上限。", true, true, false);
        }
        if (e instanceof SocketTimeoutException) {
            return new UpstreamFailureException("upstream_idle_timeout", 504, "上游响应中断超时。", true, true, false);
        }
        if (!alreadyConnected) {
            return classifyConnectException(e, abortCode);
        }
        return new UpstreamFailureException("upstream_connection_error", 502, "上游连接中断。", true, true, false);
    }

    public static ChatRequest validateChatBody(Object bodyObj) {
        return validateChatBody(bodyObj, false);
    }

    @SuppressWarnings("unchecked")
    public static ChatRequest validateChatBody(Object bodyObj, boolean allowPrivateVisionUrl) {
        if (!(bodyObj instanceof Map<?, ?> rawMap)) {
            throw new GatewayException(400, "invalid_request", "请求体必须是 JSON 对象。");
        }
        Map<String, Object> map = (Map<String, Object>) rawMap;
        List<String> unknown = new ArrayList<>();
        for (String k : map.keySet()) {
            if (!ALLOWED_CHAT_KEYS.contains(k)) unknown.add(k);
        }
        if (!unknown.isEmpty()) {
            throw new GatewayException(400, "unsupported_parameter", "不支持的参数：" + String.join(", ", unknown));
        }
        Object modelObj = map.get("model");
        if (!(modelObj instanceof String model) || model.isEmpty() || model.length() > 128) {
            throw new GatewayException(400, "invalid_request", "model: Invalid model name");
        }
        Object msgsObj = map.get("messages");
        if (!(msgsObj instanceof List<?> msgList) || msgList.isEmpty() || msgList.size() > MAX_MESSAGES) {
            throw new GatewayException(400, "invalid_request", "messages: Must be a non-empty array up to 200 messages");
        }
        Set<String> requiredCaps = new LinkedHashSet<>();
        requiredCaps.add("chat");

        List<ChatMessage> messages = new ArrayList<>(msgList.size());
        long totalChars = 0;
        for (Object item : msgList) {
            if (!(item instanceof Map<?, ?> mRaw)) {
                throw new GatewayException(400, "invalid_request", "messages: Each message must be an object");
            }
            Map<String, Object> m = (Map<String, Object>) mRaw;
            for (String k : m.keySet()) {
                if (!ALLOWED_MSG_KEYS.contains(k)) {
                    throw new GatewayException(400, "unsupported_parameter", "不支持的参数：" + k);
                }
            }
            Object roleObj = m.get("role");
            if (!(roleObj instanceof String role) || !ALLOWED_ROLES.contains(role)) {
                throw new GatewayException(400, "unsupported_role", "仅支持 system、user、assistant、tool 角色。");
            }
            if ("tool".equals(role)) {
                requiredCaps.add("tools");
                if (!(m.get("tool_call_id") instanceof String tcid) || tcid.isBlank()) {
                    throw new GatewayException(400, "invalid_request", "tool 消息必须包含 tool_call_id");
                }
            }
            List<Map<String, Object>> msgToolCalls = null;
            if (m.get("tool_calls") != null) {
                requiredCaps.add("tools");
                if (!(m.get("tool_calls") instanceof List<?> tcList) || tcList.isEmpty()) {
                    throw new GatewayException(400, "invalid_request", "tool_calls 必须是非空数组");
                }
                msgToolCalls = (List<Map<String, Object>>) tcList;
            }

            Object contentObj = m.get("content");
            String textSummary = "";
            boolean hasVision = false;
            List<String> visionUrls = new ArrayList<>();

            if (contentObj == null && msgToolCalls != null) {
                textSummary = "";
            } else if (contentObj instanceof String content) {
                if (content.length() > MAX_MESSAGE_CHARS) {
                    throw new GatewayException(400, "invalid_request", "messages.content: Must be a string within length limit");
                }
                textSummary = content;
            } else if (contentObj instanceof List<?> parts) {
                if (parts.isEmpty()) throw new GatewayException(400, "invalid_request", "messages.content 数组不能为空");
                StringBuilder sb = new StringBuilder();
                for (Object pObj : parts) {
                    if (!(pObj instanceof Map<?, ?> pMap)) {
                        throw new GatewayException(400, "invalid_request", "content 分片必须是对象");
                    }
                    String type = String.valueOf(pMap.get("type"));
                    if ("text".equals(type)) {
                        if (!(pMap.get("text") instanceof String txt) || txt.length() > MAX_MESSAGE_CHARS) {
                            throw new GatewayException(400, "invalid_request", "text 分片无效");
                        }
                        sb.append(txt);
                    } else if ("image_url".equals(type)) {
                        hasVision = true;
                        requiredCaps.add("vision");
                        if (!(pMap.get("image_url") instanceof Map<?, ?> imgMap) || !(imgMap.get("url") instanceof String imgUrl) || imgUrl.isBlank()) {
                            throw new GatewayException(400, "invalid_request", "image_url.url 必填");
                        }
                        validateVisionUrl(imgUrl, allowPrivateVisionUrl);
                        visionUrls.add(imgUrl);
                    } else {
                        throw new GatewayException(400, "unsupported_parameter", "不支持的内容分片类型：" + type);
                    }
                }
                textSummary = sb.toString();
            } else {
                throw new GatewayException(400, "invalid_request", "messages.content 类型无效");
            }
            totalChars += textSummary.length();
            messages.add(new ChatMessage(role, contentObj, textSummary, hasVision, visionUrls, msgToolCalls, m.get("tool_call_id") instanceof String s ? s : null));
        }
        if (totalChars > MAX_TOTAL_CHARS) {
            throw new GatewayException(400, "invalid_messages", "消息内容超过长度上限。");
        }
        boolean stream = false;
        if (map.containsKey("stream") && map.get("stream") != null) {
            if (!(map.get("stream") instanceof Boolean b)) {
                throw new GatewayException(400, "invalid_request", "stream: Must be boolean");
            }
            stream = b;
            if (stream) requiredCaps.add("stream");
        }
        Double temperature = null;
        if (map.containsKey("temperature") && map.get("temperature") != null) {
            if (!(map.get("temperature") instanceof Number n)) {
                throw new GatewayException(400, "invalid_request", "temperature: Must be a number");
            }
            double d = n.doubleValue();
            if (!Double.isFinite(d) || d < 0.0 || d > 2.0) {
                throw new GatewayException(400, "invalid_request", "temperature: Must be between 0 and 2");
            }
            temperature = d;
        }
        Integer maxTokens = null;
        if (map.containsKey("max_tokens") && map.get("max_tokens") != null) {
            if (!(map.get("max_tokens") instanceof Number n) || n.doubleValue() != Math.floor(n.doubleValue())) {
                throw new GatewayException(400, "invalid_request", "max_tokens: Must be an integer");
            }
            int mt = n.intValue();
            if (mt < 1 || mt > MAX_MAX_OUTPUT_TOKENS) {
                throw new GatewayException(400, "invalid_request", "max_tokens: Must be between 1 and " + MAX_MAX_OUTPUT_TOKENS);
            }
            maxTokens = mt;
        }

        // P1-B: Tools validation
        List<Map<String, Object>> tools = null;
        if (map.containsKey("tools") && map.get("tools") != null) {
            if (!(map.get("tools") instanceof List<?> tList) || tList.isEmpty() || tList.size() > MAX_TOOLS) {
                throw new GatewayException(400, "invalid_request", "tools: Must be a non-empty array up to " + MAX_TOOLS);
            }
            requiredCaps.add("tools");
            tools = new ArrayList<>(tList.size());
            for (Object tObj : tList) {
                if (!(tObj instanceof Map<?, ?> tm) || !"function".equals(tm.get("type")) || !(tm.get("function") instanceof Map<?, ?> fn)) {
                    throw new GatewayException(400, "invalid_request", "tools: Each tool must be of type 'function'");
                }
                if (!(fn.get("name") instanceof String fnName) || fnName.isBlank() || fnName.length() > 128) {
                    throw new GatewayException(400, "invalid_request", "tools.function.name is required");
                }
                tools.add((Map<String, Object>) tm);
            }
        }
        Object toolChoice = map.get("tool_choice");
        if (toolChoice != null) {
            requiredCaps.add("tools");
        }

        // P1-C: Structured Output validation
        Map<String, Object> responseFormat = null;
        if (map.containsKey("response_format") && map.get("response_format") != null) {
            if (!(map.get("response_format") instanceof Map<?, ?> rfMap)) {
                throw new GatewayException(400, "invalid_request", "response_format must be an object");
            }
            String rfType = String.valueOf(rfMap.get("type"));
            if ("json_object".equals(rfType)) {
                requiredCaps.add("json_object");
            } else if ("json_schema".equals(rfType)) {
                requiredCaps.add("json_schema");
                if (!(rfMap.get("json_schema") instanceof Map<?, ?> jsMap)
                    || !(jsMap.get("name") instanceof String jsName) || jsName.isBlank()
                    || !(jsMap.get("schema") instanceof Map<?, ?>)) {
                    throw new GatewayException(400, "invalid_request", "response_format.json_schema requires name and schema object");
                }
            } else {
                throw new GatewayException(400, "unsupported_parameter", "不支持的 response_format.type：" + rfType);
            }
            responseFormat = (Map<String, Object>) rfMap;
        }

        return new ChatRequest(model, messages, stream, temperature, maxTokens, tools, toolChoice, responseFormat, requiredCaps);
    }

    /** P1-E: Vision Input URL / Data-URI security & size validation */
    public static void validateVisionUrl(String url, boolean allowPrivate) {
        if (url.startsWith("data:")) {
            Matcher m = DATA_IMAGE_RE.matcher(url);
            if (!m.matches()) {
                throw new GatewayException(400, "invalid_vision_input", "仅支持 png、jpeg、webp、gif 的 base64 data URI。");
            }
            String b64 = m.group(2);
            byte[] decoded;
            try {
                decoded = Base64.getDecoder().decode(b64);
            } catch (Exception e) {
                throw new GatewayException(400, "invalid_vision_input", "图像 base64 编码无效。");
            }
            if (decoded.length == 0 || decoded.length > MAX_VISION_DATA_URL_BYTES) {
                throw new GatewayException(400, "invalid_vision_input", "内联图像大小超出 512KB 上限。");
            }
            return;
        }
        try {
            SecurityUtils.validateProviderUrl(url, allowPrivate);
        } catch (Exception e) {
            throw new GatewayException(400, "invalid_vision_input", "图像 URL 未通过安全与 SSRF 校验。");
        }
    }

    /** P1-D: Validate Embeddings request body */
    @SuppressWarnings("unchecked")
    public static EmbeddingRequest validateEmbeddingBody(Object bodyObj) {
        if (!(bodyObj instanceof Map<?, ?> rawMap)) {
            throw new GatewayException(400, "invalid_request", "请求体必须是 JSON 对象。");
        }
        Map<String, Object> map = (Map<String, Object>) rawMap;
        for (String k : map.keySet()) {
            if (!ALLOWED_EMBED_KEYS.contains(k)) {
                throw new GatewayException(400, "unsupported_parameter", "不支持的参数：" + k);
            }
        }
        if (!(map.get("model") instanceof String model) || model.isBlank() || model.length() > 128) {
            throw new GatewayException(400, "invalid_request", "model 必填");
        }
        Object inputObj = map.get("input");
        List<String> inputs = new ArrayList<>();
        if (inputObj instanceof String s) {
            if (s.isEmpty() || s.length() > MAX_MESSAGE_CHARS) {
                throw new GatewayException(400, "invalid_request", "input 不能为空或超过长度限制");
            }
            inputs.add(s);
        } else if (inputObj instanceof List<?> list) {
            if (list.isEmpty() || list.size() > MAX_EMBEDDING_INPUTS) {
                throw new GatewayException(400, "invalid_request", "input 批量大小需在 1–" + MAX_EMBEDDING_INPUTS + " 之间");
            }
            long total = 0;
            for (Object item : list) {
                if (!(item instanceof String s) || s.isEmpty() || s.length() > MAX_MESSAGE_CHARS) {
                    throw new GatewayException(400, "invalid_request", "input 数组元素必须是非空字符串");
                }
                total += s.length();
                inputs.add(s);
            }
            if (total > MAX_TOTAL_CHARS) {
                throw new GatewayException(400, "invalid_request", "input 总字符数超出上限");
            }
        } else {
            throw new GatewayException(400, "invalid_request", "input 必须是字符串或字符串数组");
        }
        String encFmt = null;
        if (map.get("encoding_format") != null) {
            if (!(map.get("encoding_format") instanceof String ef) || (!"float".equals(ef) && !"base64".equals(ef))) {
                throw new GatewayException(400, "invalid_request", "encoding_format 仅支持 float 或 base64");
            }
            encFmt = ef;
        }
        Integer dims = null;
        if (map.get("dimensions") != null) {
            if (!(map.get("dimensions") instanceof Number n) || n.intValue() < 1 || n.intValue() > 16384) {
                throw new GatewayException(400, "invalid_request", "dimensions 需为 1–16384 的整数");
            }
            dims = n.intValue();
        }
        return new EmbeddingRequest(model, inputs, encFmt, dims);
    }

    public static int estimateInputTokens(List<ChatMessage> messages) {
        long chars = 0;
        int visionTokens = 0;
        for (ChatMessage m : messages) {
            String summary = m.textSummary() != null ? m.textSummary() : "";
            chars += summary.length() + m.role().length() + 4;
            if (m.hasVision()) {
                visionTokens += 1024 * Math.max(1, m.visionUrls().size());
            }
        }
        return (int) Math.max(1L, (chars + 2) / 3 + (long) messages.size() * 4 + 8 + visionTokens);
    }

    public static long tokensToMicros(long tokens, long microsPerMillion) {
        if (tokens < 0 || microsPerMillion < 0) {
            throw new GatewayException(500, "invalid_configuration", "价格配置无效。");
        }
        BigInteger t = BigInteger.valueOf(tokens);
        BigInteger p = BigInteger.valueOf(microsPerMillion);
        BigInteger micros = t.multiply(p).add(BigInteger.valueOf(999_999L)).divide(BigInteger.valueOf(1_000_000L));
        if (micros.compareTo(BigInteger.valueOf(HotContract.MAX_SAFE_INTEGER)) > 0) {
            throw new GatewayException(500, "price_overflow", "价格计算超出安全范围。");
        }
        return micros.longValueExact();
    }

    public static long estimateAttemptMicros(int inputTokens, int maxOutputTokens, long inputPrice, long outputPrice) {
        return tokensToMicros(inputTokens, inputPrice) + tokensToMicros(maxOutputTokens, outputPrice);
    }

    public static long usageMicros(long inputTokens, long outputTokens, long inputPrice, long outputPrice) {
        return tokensToMicros(inputTokens, inputPrice) + tokensToMicros(outputTokens, outputPrice);
    }

    public static String utcPeriod() {
        ZonedDateTime now = ZonedDateTime.now(ZoneOffset.UTC);
        return String.format("%04d-%02d", now.getYear(), now.getMonthValue());
    }

    public static PreparedRequest prepareRequest(KeyContext key, ChatRequest body, RouteSnapshot route) {
        if (body.maxTokens() != null && key.maxOutputTokens() != null && body.maxTokens() > key.maxOutputTokens()) {
            throw new GatewayException(400, "max_tokens_exceeds_project_limit", "max_tokens 不能超过项目上限 " + key.maxOutputTokens() + "。");
        }
        int maxOutputTokens = body.maxTokens() != null ? body.maxTokens()
            : (key.maxOutputTokens() != null ? key.maxOutputTokens() : DEFAULT_MAX_OUTPUT_TOKENS);
        if (maxOutputTokens < 1 || maxOutputTokens > MAX_MAX_OUTPUT_TOKENS) {
            throw new GatewayException(500, "invalid_configuration", "输出上限配置无效。");
        }
        List<RouteTarget> targets = route.targets().stream()
            .filter(t -> t.targetEnabled() && t.providerEnabled())
            .limit(MAX_ATTEMPTS)
            .toList();
        if (targets.isEmpty()) {
            throw new GatewayException(503, "model_unavailable", "模型当前没有可用目标。");
        }
        int estimatedInputTokens = estimateInputTokens(body.messages());
        List<PriceEntry> entries = new ArrayList<>(targets.size());
        long total = 0;
        for (int i = 0; i < targets.size(); i++) {
            RouteTarget target = targets.get(i);
            long reserved = estimateAttemptMicros(estimatedInputTokens, maxOutputTokens, target.inputPrice(), target.outputPrice());
            total += reserved;
            if (total < 0 || total > HotContract.MAX_SAFE_INTEGER) {
                throw new GatewayException(500, "price_overflow", "价格计算超出安全范围。");
            }
            entries.add(new PriceEntry(
                i + 1,
                target.targetId(),
                target.upstreamModel(),
                target.inputPrice(),
                target.outputPrice(),
                maxOutputTokens,
                estimatedInputTokens,
                reserved
            ));
        }
        String period = utcPeriod();
        Map<String, Object> priceSnapshot = new LinkedHashMap<>();
        priceSnapshot.put("version", 1);
        priceSnapshot.put("currency", "USD");
        priceSnapshot.put("unit", "tokens");
        priceSnapshot.put("period", period);
        priceSnapshot.put("max_attempts", entries.size());
        priceSnapshot.put("estimated_input_tokens", estimatedInputTokens);
        priceSnapshot.put("max_output_tokens", maxOutputTokens);
        priceSnapshot.put("total_reserved_micros", total);
        priceSnapshot.put("attempts", entries);

        Map<String, Object> routeVersion = new LinkedHashMap<>();
        routeVersion.put("alias_id", route.aliasId());
        routeVersion.put("alias_name", route.aliasName());
        routeVersion.put("version_id", route.routeVersionId());
        routeVersion.put("runtime_revision", key.runtimeRevision());
        routeVersion.put("runtime_key_hash", key.runtimeKeyHash());
        routeVersion.put("runtime_expires_at", key.runtimeExpiresAt());
        routeVersion.put("version", route.version());
        routeVersion.put("targets", entries.stream().map(e -> Map.of("target_id", e.target_id(), "upstream_model", e.upstream_model())).toList());

        return new PreparedRequest(key, body, route, targets, maxOutputTokens, priceSnapshot, entries, total, period, routeVersion);
    }

    private void admitRequest(String requestId, PreparedRequest prepared) {
        billing.admit(new AdmitInput(
            requestId,
            prepared.key().projectId(),
            prepared.key().keyId(),
            prepared.period(),
            prepared.totalReservedMicros(),
            prepared.key().projectRpm(),
            prepared.key().keyRpm(),
            prepared.key().concurrency(),
            prepared.key().budgetMicros(),
            prepared.routeVersion(),
            prepared.priceSnapshot()
        ));
    }

    public String providerToken(String encryptedSecret, String hexKey) {
        String plain = SecurityUtils.decrypt(encryptedSecret, hexKey).trim();
        if (plain.startsWith("{")) {
            try {
                Map<?, ?> parsed = mapper.readValue(plain, Map.class);
                for (String k : List.of("apiKey", "api_key", "token", "secret", "key")) {
                    Object val = parsed.get(k);
                    if (val instanceof String s && !s.isEmpty()) return s;
                }
            } catch (Exception ignored) {}
        }
        if (plain.isEmpty()) {
            throw new GatewayException(502, "upstream_credentials_invalid", "上游凭据不可用。");
        }
        return plain;
    }

    public static URI chatEndpoint(String baseUrl) {
        try {
            URI u = new URI(baseUrl.trim());
            String path = u.getPath() != null ? u.getPath() : "";
            if (!path.endsWith("/")) path += "/";
            path += "chat/completions";
            return new URI(u.getScheme(), u.getUserInfo(), u.getHost(), u.getPort(), path, null, null);
        } catch (Exception e) {
            throw new GatewayException(502, "invalid_provider_url", "上游服务地址无效。");
        }
    }

    public static URI embeddingsEndpoint(String baseUrl) {
        try {
            URI u = new URI(baseUrl.trim());
            String path = u.getPath() != null ? u.getPath() : "";
            if (!path.endsWith("/")) path += "/";
            path += "embeddings";
            return new URI(u.getScheme(), u.getUserInfo(), u.getHost(), u.getPort(), path, null, null);
        } catch (Exception e) {
            throw new GatewayException(502, "invalid_provider_url", "上游服务地址无效。");
        }
    }

    public String upstreamPayload(RouteTarget target, ChatRequest body, int maxOutputTokens) throws Exception {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("model", target.upstreamModel());
        List<Map<String, Object>> msgs = new ArrayList<>(body.messages().size());
        for (ChatMessage m : body.messages()) {
            Map<String, Object> item = new LinkedHashMap<>();
            item.put("role", m.role());
            item.put("content", m.content());
            if (m.toolCalls() != null) item.put("tool_calls", m.toolCalls());
            if (m.toolCallId() != null) item.put("tool_call_id", m.toolCallId());
            msgs.add(item);
        }
        payload.put("messages", msgs);
        payload.put("max_tokens", body.maxTokens() != null ? body.maxTokens() : maxOutputTokens);
        if (body.temperature() != null) {
            payload.put("temperature", body.temperature());
        }
        if (body.tools() != null) {
            payload.put("tools", body.tools());
        }
        if (body.toolChoice() != null) {
            payload.put("tool_choice", body.toolChoice());
        }
        if (body.responseFormat() != null) {
            payload.put("response_format", body.responseFormat());
        }
        if (body.stream()) {
            payload.put("stream", true);
            if (target.capabilities() != null && target.capabilities().contains("stream_usage")) {
                payload.put("stream_options", Map.of("include_usage", true));
            }
        }
        return mapper.writeValueAsString(payload);
    }

    public static TokenUsage parseUsage(Object value) {
        if (!(value instanceof Map<?, ?> usage)) return null;
        Object inObj = usage.get("prompt_tokens");
        Object outObj = usage.get("completion_tokens");
        if (!(inObj instanceof Number inNum) || inNum.doubleValue() != Math.floor(inNum.doubleValue()) || inNum.longValue() < 0) {
            return null;
        }
        if (!(outObj instanceof Number outNum) || outNum.doubleValue() != Math.floor(outNum.doubleValue()) || outNum.longValue() < 0) {
            return null;
        }
        return new TokenUsage(inNum.longValue(), outNum.longValue());
    }

    public static Map<String, Object> normalizeCompletion(Object upstream, String aliasName, String requestId, TokenUsage usage) {
        return normalizeCompletion(upstream, aliasName, requestId, usage, false, null);
    }

    @SuppressWarnings("unchecked")
    public static Map<String, Object> normalizeCompletion(
        Object upstream,
        String aliasName,
        String requestId,
        TokenUsage usage,
        boolean verifyJsonOutput,
        ObjectMapper mapper
    ) {
        if (!(upstream instanceof Map<?, ?> body)) {
            throw new GatewayException(502, "upstream_invalid_response", "上游返回了无法识别的响应。");
        }
        Object choicesObj = body.get("choices");
        if (!(choicesObj instanceof List<?> rawChoices) || rawChoices.isEmpty()) {
            throw new GatewayException(502, "upstream_invalid_response", "上游返回了无法识别的响应。");
        }
        List<Map<String, Object>> choices = new ArrayList<>(rawChoices.size());
        for (int i = 0; i < rawChoices.size(); i++) {
            Object rc = rawChoices.get(i);
            Map<?, ?> choice = rc instanceof Map<?, ?> m ? m : Map.of();
            Map<?, ?> msg = choice.get("message") instanceof Map<?, ?> m ? m : Map.of();
            String content = msg.get("content") instanceof String s ? s : (msg.get("tool_calls") != null ? null : "");
            if (verifyJsonOutput && mapper != null && content != null) {
                try {
                    Object parsedJson = mapper.readValue(content, Object.class);
                    if (!(parsedJson instanceof Map<?, ?>) && !(parsedJson instanceof List<?>)) {
                        throw new GatewayException(502, "upstream_invalid_response", "结构化输出不是合法 JSON 对象。");
                    }
                } catch (Exception e) {
                    throw new GatewayException(502, "upstream_invalid_response", "结构化输出不是合法 JSON。");
                }
            }
            String finish = choice.get("finish_reason") instanceof String s ? s : null;
            int idx = choice.get("index") instanceof Number n ? n.intValue() : i;
            Map<String, Object> msgOut = new LinkedHashMap<>();
            msgOut.put("role", "assistant");
            msgOut.put("content", content);
            if (msg.get("tool_calls") instanceof List<?> tcList && !tcList.isEmpty()) {
                for (Object tcObj : tcList) {
                    if (!(tcObj instanceof Map<?, ?> tcMap)
                        || !(tcMap.get("id") instanceof String tcId) || tcId.isBlank()
                        || !(tcMap.get("function") instanceof Map<?, ?> fnMap)
                        || !(fnMap.get("name") instanceof String fnName) || fnName.isBlank()
                        || !(fnMap.get("arguments") instanceof String fnArgs)) {
                        throw new GatewayException(502, "upstream_invalid_response", "上游返回的 tool_calls 格式无效。");
                    }
                    if (mapper != null) {
                        try {
                            mapper.readValue(fnArgs, Object.class);
                        } catch (Exception e) {
                            throw new GatewayException(502, "upstream_invalid_response", "上游返回的 tool_calls arguments 不是合法 JSON。");
                        }
                    }
                }
                msgOut.put("tool_calls", tcList);
            }
            Map<String, Object> item = new LinkedHashMap<>();
            item.put("index", idx);
            item.put("message", msgOut);
            item.put("finish_reason", finish);
            choices.add(item);
        }
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("id", "chatcmpl_" + requestId);
        out.put("object", "chat.completion");
        out.put("created", Instant.now().getEpochSecond());
        out.put("model", aliasName);
        out.put("choices", choices);
        if (usage != null) {
            Map<String, Object> uMap = new LinkedHashMap<>();
            uMap.put("prompt_tokens", usage.input());
            uMap.put("completion_tokens", usage.output());
            uMap.put("total_tokens", usage.input() + usage.output());
            out.put("usage", uMap);
        } else {
            out.put("usage", null);
        }
        return out;
    }

    public record UpstreamHttpErr(String code, int statusCode, String message, boolean retrySafe) {}

    public static UpstreamHttpErr upstreamHttpError(int status) {
        if (status >= 300 && status < 400) return new UpstreamHttpErr("upstream_redirect", 502, "上游返回了重定向。", false);
        if (status == 400 || status == 422) return new UpstreamHttpErr("upstream_invalid_request", 400, "上游拒绝了请求参数。", false);
        if (status == 401 || status == 403) return new UpstreamHttpErr("upstream_auth_failed", 502, "上游鉴权失败。", false);
        if (status == 404) return new UpstreamHttpErr("upstream_model_unavailable", 502, "上游模型不可用。", false);
        if (status == 408) return new UpstreamHttpErr("upstream_timeout", 504, "上游请求超时。", false);
        if (status == 429) return new UpstreamHttpErr("upstream_rate_limited", 429, "上游限流，请稍后重试。", true);
        if (status == 503) return new UpstreamHttpErr("upstream_unavailable", 503, "上游服务暂时不可用。", false);
        return new UpstreamHttpErr("upstream_error", 502, "上游服务返回错误。", false);
    }

    public static Integer parseRetryAfterHeader(String raw) {
        if (raw == null || !raw.matches("^\\d{1,5}$")) return null;
        int sec = Integer.parseInt(raw);
        return (sec > 0 && sec <= 3600) ? sec : null;
    }

    public static boolean canFallback(AttemptRecord record, int attemptIndex, int totalTargets) {
        if (attemptIndex + 1 >= totalTargets) return false;
        if ("ok".equals(record.status())) return false;
        return record.retry_safe();
    }

    public record AggregatedTotals(Long actualMicros, Long inputTokens, Long outputTokens) {}

    public static AggregatedTotals aggregateSettlement(List<AttemptRecord> attempts) {
        long actual = 0;
        long input = 0;
        long output = 0;
        for (AttemptRecord a : attempts) {
            if (!a.usage_known()) {
                return new AggregatedTotals(null, null, null);
            }
            actual += a.cost_micros() != null ? a.cost_micros() : 0L;
            input += a.input_tokens() != null ? a.input_tokens() : 0L;
            output += a.output_tokens() != null ? a.output_tokens() : 0L;
            if (actual < 0 || actual > HotContract.MAX_SAFE_INTEGER
                || input < 0 || input > HotContract.MAX_SAFE_INTEGER
                || output < 0 || output > HotContract.MAX_SAFE_INTEGER) {
                return new AggregatedTotals(null, null, null);
            }
        }
        return new AggregatedTotals(actual, input, output);
    }

    public static GatewayException errorFromRecord(AttemptRecord record) {
        String code = record.error_code() != null ? record.error_code() : "upstream_error";
        Map.Entry<Integer, String> entry = ERROR_TABLE.getOrDefault(code, Map.entry(502, "上游调用失败。"));
        return new GatewayException(entry.getKey(), code, entry.getValue());
    }

    public static Integer retryAfterOf(Exception e) {
        if (e instanceof GatewayException ge && ge.getDetails() != null) {
            Object msObj = ge.getDetails().get("retryAfterMs");
            if (msObj instanceof Number n && n.longValue() > 0) {
                return (int) Math.max(1L, (n.longValue() + 999L) / 1000L);
            }
        }
        return null;
    }

    public HttpResponsePayload buildErrorPayload(String requestId, Exception err, Integer retryAfterSec) {
        int status = 503;
        String code = "service_unavailable";
        String message = "服务暂时不可用，请稍后重试。";
        if (err instanceof GatewayException ge) {
            status = ge.getStatusCode();
            code = ge.getCode();
            message = ge.getMessage();
        }
        Map<String, String> headers = new LinkedHashMap<>();
        headers.put("x-request-id", requestId);
        if (retryAfterSec != null && retryAfterSec > 0) {
            headers.put("retry-after", String.valueOf(retryAfterSec));
        }
        try {
            String json = mapper.writeValueAsString(Map.of(
                "error", Map.of("code", code, "message", message, "type", "gateway_error"),
                "request_id", requestId
            ));
            return new HttpResponsePayload(status, headers, json);
        } catch (Exception e) {
            return new HttpResponsePayload(status, headers, "{\"error\":{\"code\":\"" + code + "\",\"message\":\"" + message + "\",\"type\":\"gateway_error\"},\"request_id\":\"" + requestId + "\"}");
        }
    }

    public static String formatSseEvent(SseEvent event) {
        String prefix = (event.event() != null && !event.event().isEmpty()) ? "event: " + event.event() + "\n" : "";
        return prefix + "data: " + event.data() + "\n\n";
    }

    public static class SseParser {
        private static final Pattern FRAME_DELIM = Pattern.compile("\\r?\\n\\r?\\n");
        private String buffer = "";

        public List<SseEvent> push(String chunk) {
            buffer += chunk;
            if (buffer.length() > MAX_SSE_EVENT_BYTES) {
                throw new UpstreamFailureException("upstream_protocol_error", 502, "上游流式响应格式无效。", true, true, false);
            }
            List<SseEvent> events = new ArrayList<>();
            while (true) {
                Matcher m = FRAME_DELIM.matcher(buffer);
                if (!m.find()) break;
                String raw = buffer.substring(0, m.start());
                buffer = buffer.substring(m.end());
                SseEvent ev = parseFrame(raw);
                if (ev != null) events.add(ev);
            }
            return events;
        }

        public List<SseEvent> flush() {
            if (buffer.isBlank()) return List.of();
            String raw = buffer;
            buffer = "";
            SseEvent ev = parseFrame(raw);
            return ev != null ? List.of(ev) : List.of();
        }

        private static SseEvent parseFrame(String raw) {
            String event = null;
            List<String> data = new ArrayList<>();
            for (String line : raw.split("\\r?\\n", -1)) {
                if (line.isEmpty() || line.startsWith(":")) continue;
                int colon = line.indexOf(':');
                String field = colon == -1 ? line : line.substring(0, colon);
                String val = colon == -1 ? "" : line.substring(colon + 1);
                if (val.startsWith(" ")) val = val.substring(1);
                if ("event".equals(field)) event = val;
                else if ("data".equals(field)) data.add(val);
            }
            if (data.isEmpty() && event == null) return null;
            return new SseEvent(event, String.join("\n", data));
        }
    }
}
