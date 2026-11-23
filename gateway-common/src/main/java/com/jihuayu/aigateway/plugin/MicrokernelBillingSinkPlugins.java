package com.jihuayu.aigateway.plugin;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jihuayu.aigateway.plugin.PluginExecutionContext.SettledBillingEvent;
import com.jihuayu.aigateway.security.SecurityUtils;
import io.lettuce.core.api.StatefulRedisConnection;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedDeque;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

/**
 * Built-in FinOps Cost Attribution & Billing Sink Plugins (#54).
 *
 * <ul>
 *   <li>{@link CostAttributionTagPlugin} ({@code builtin:billing-cost-attribution}): Extracts cost_center, department,
 *       agent_id, workflow_id, and environment tags from headers/metadata into {@code ctx.attributionTags()}.</li>
 *   <li>{@link WebhookBillingSinkPlugin} ({@code builtin:billing-webhook-sink}): Exports settled micro-USD billing events
 *       to external financial/ERP/Lago/Stripe webhook endpoints with {@code HMAC-SHA256} signature and DLQ fallback.</li>
 *   <li>{@link RedisStreamBillingSinkPlugin} ({@code builtin:billing-redis-stream-sink}): Publishes settled FinOps
 *       billing events to a dedicated Redis Stream for real-time data warehouse / ClickHouse ingestion.</li>
 * </ul>
 */
public final class MicrokernelBillingSinkPlugins {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private MicrokernelBillingSinkPlugins() {}

    /**
     * 1. CostAttributionTagPlugin (builtin:billing-cost-attribution):
     * Extracts FinOps cost attribution tags at ON_PRE_ADMIT and attaches cost summary headers.
     */
    public static final class CostAttributionTagPlugin implements GatewayPlugin {
        public static final String ID = "builtin:billing-cost-attribution";

        private static final Map<String, String> DEFAULT_HEADER_TAG_MAP = Map.of(
            "x-cost-center", "cost_center",
            "x-department", "department",
            "x-agent-id", "agent_id",
            "x-workflow-id", "workflow_id",
            "x-environment", "environment"
        );

        @Override public String id() { return ID; }
        @Override public String name() { return "billing-cost-attribution"; }
        @Override public String type() { return "BUILTIN"; }
        @Override public int priority() { return 11; }

        @Override
        public PluginResult onPreAdmit(PluginExecutionContext ctx) {
            for (Map.Entry<String, String> e : DEFAULT_HEADER_TAG_MAP.entrySet()) {
                String val = ctx.requestHeaders().get(e.getKey());
                if (val != null && !val.isBlank()) {
                    ctx.putAttributionTag(e.getValue(), val.trim());
                }
            }
            if (ctx.endUserId() != null && !ctx.endUserId().isBlank()) {
                ctx.putAttributionTag("end_user_id", ctx.endUserId());
            }
            if (!ctx.attributionTags().isEmpty()) {
                String cc = ctx.attributionTags().get("cost_center");
                if (cc != null) {
                    ctx.setResponseHeader("x-gateway-cost-center", cc);
                }
            }
            return PluginResult.Continue.INSTANCE;
        }
    }

    /**
     * 2. WebhookBillingSinkPlugin (builtin:billing-webhook-sink):
     * Sends settled micro-USD billing events with HMAC-SHA256 signature to an external FinOps webhook at ON_POST_SETTLE.
     */
    public static final class WebhookBillingSinkPlugin implements GatewayPlugin {
        public static final String ID = "builtin:billing-webhook-sink";

        private final HttpClient httpClient;
        private final ConcurrentLinkedDeque<Map<String, Object>> exportedEvents = new ConcurrentLinkedDeque<>();
        private final ConcurrentLinkedDeque<Map<String, Object>> dlqEvents = new ConcurrentLinkedDeque<>();
        private volatile String lastWebhookUrl = "";
        private volatile String lastSigningSecret = "default-billing-secret";

        public WebhookBillingSinkPlugin(HttpClient httpClient) {
            this.httpClient = httpClient != null
                ? httpClient
                : HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build();
        }

        @Override public String id() { return ID; }
        @Override public String name() { return "billing-webhook-sink"; }
        @Override public String type() { return "BUILTIN"; }
        @Override public int priority() { return 50; }

        public List<Map<String, Object>> recentExportedEvents() {
            return Collections.unmodifiableList(new ArrayList<>(exportedEvents));
        }

        public List<Map<String, Object>> dlqEvents() {
            return Collections.unmodifiableList(new ArrayList<>(dlqEvents));
        }

        public int clearDlqEvents() {
            int count = dlqEvents.size();
            dlqEvents.clear();
            return count;
        }

        public Map<String, Object> replayDlqEvents(String overrideWebhookUrl, String overrideSecret) {
            List<Map<String, Object>> snapshot = new ArrayList<>(dlqEvents);
            dlqEvents.clear();
            int attempted = snapshot.size();
            int succeeded = 0;
            int failed = 0;
            for (Map<String, Object> item : snapshot) {
                Map<String, Object> cleanPayload = new LinkedHashMap<>(item);
                String itemUrl = cleanPayload.remove("dlqWebhookUrl") instanceof String s ? s : lastWebhookUrl;
                cleanPayload.remove("dlqReason");
                String targetUrl = (overrideWebhookUrl != null && !overrideWebhookUrl.isBlank()) ? overrideWebhookUrl.trim() : itemUrl;
                String secret = (overrideSecret != null && !overrideSecret.isBlank()) ? overrideSecret.trim() : lastSigningSecret;
                if (targetUrl == null || targetUrl.isBlank()) {
                    Map<String, Object> retryFail = new LinkedHashMap<>(cleanPayload);
                    retryFail.put("dlqReason", "missing_webhook_url");
                    dlqEvents.addLast(retryFail);
                    failed++;
                    continue;
                }
                try {
                    String jsonBody = MAPPER.writeValueAsString(cleanPayload);
                    String sig = computeHmacSha256(secret, jsonBody);
                    HttpRequest req = HttpRequest.newBuilder()
                        .uri(URI.create(targetUrl))
                        .timeout(Duration.ofMillis(2000L))
                        .header("Content-Type", "application/json")
                        .header("X-Gateway-Billing-Signature", "sha256=" + sig)
                        .header("X-Gateway-Billing-Replay", "true")
                        .header("User-Agent", "Mozilla/5.0 (compatible; Google-Agent; +https://developers.google.com/crawling/docs/crawlers-fetchers/google-agent)")
                        .POST(HttpRequest.BodyPublishers.ofString(jsonBody))
                        .build();
                    HttpResponse<String> resp = httpClient.send(req, HttpResponse.BodyHandlers.ofString());
                    if (resp.statusCode() >= 400) {
                        Map<String, Object> retryFail = new LinkedHashMap<>(cleanPayload);
                        retryFail.put("dlqWebhookUrl", targetUrl);
                        retryFail.put("dlqReason", "HTTP " + resp.statusCode());
                        dlqEvents.addLast(retryFail);
                        failed++;
                    } else {
                        exportedEvents.addFirst(cleanPayload);
                        while (exportedEvents.size() > 256) exportedEvents.pollLast();
                        succeeded++;
                    }
                } catch (Exception e) {
                    Map<String, Object> retryFail = new LinkedHashMap<>(cleanPayload);
                    retryFail.put("dlqWebhookUrl", targetUrl);
                    retryFail.put("dlqReason", e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName());
                    dlqEvents.addLast(retryFail);
                    failed++;
                }
            }
            Map<String, Object> out = new LinkedHashMap<>();
            out.put("attempted", attempted);
            out.put("succeeded", succeeded);
            out.put("failed", failed);
            out.put("remainingDlqCount", dlqEvents.size());
            return out;
        }

        private static String computeHmacSha256(String secret, String message) {
            try {
                Mac mac = Mac.getInstance("HmacSHA256");
                mac.init(new SecretKeySpec(
                    (secret != null ? secret : "").getBytes(StandardCharsets.UTF_8),
                    "HmacSHA256"
                ));
                byte[] digest = mac.doFinal((message != null ? message : "").getBytes(StandardCharsets.UTF_8));
                return HexFormat.of().formatHex(digest);
            } catch (Exception e) {
                return SecurityUtils.hashKey((secret != null ? secret : "") + ":" + (message != null ? message : ""));
            }
        }

        @Override
        public void onPostSettle(PluginExecutionContext ctx) {
            Map<String, Object> payload = buildBillingEventMap(ctx);
            exportedEvents.addFirst(payload);
            while (exportedEvents.size() > 256) exportedEvents.pollLast();

            Map<String, Object> cfg = ctx.pluginConfig();
            String webhookUrl = cfg.get("webhookUrl") instanceof String s ? s.trim() : "";
            if (webhookUrl.isEmpty()) {
                return;
            }
            String secret = cfg.get("signingSecret") instanceof String s ? s : "default-billing-secret";
            long timeoutMs = cfg.get("timeoutMs") instanceof Number n ? Math.max(100L, n.longValue()) : 1500L;
            this.lastWebhookUrl = webhookUrl;
            this.lastSigningSecret = secret;

            try {
                String jsonBody = MAPPER.writeValueAsString(payload);
                String sig = ctx.crypto().hmacSha256(secret, jsonBody);
                HttpRequest req = HttpRequest.newBuilder()
                    .uri(URI.create(webhookUrl))
                    .timeout(Duration.ofMillis(timeoutMs))
                    .header("Content-Type", "application/json")
                    .header("X-Gateway-Billing-Signature", "sha256=" + sig)
                    .header("User-Agent", "Mozilla/5.0 (compatible; Google-Agent; +https://developers.google.com/crawling/docs/crawlers-fetchers/google-agent)")
                    .POST(HttpRequest.BodyPublishers.ofString(jsonBody))
                    .build();
                HttpResponse<String> resp = httpClient.send(req, HttpResponse.BodyHandlers.ofString());
                if (resp.statusCode() >= 400) {
                    recordDlq(ctx, payload, webhookUrl, "HTTP " + resp.statusCode());
                }
            } catch (Exception e) {
                recordDlq(ctx, payload, webhookUrl, e.getMessage());
            }
        }

        private void recordDlq(PluginExecutionContext ctx, Map<String, Object> payload, String webhookUrl, String error) {
            Map<String, Object> dlqEntry = new LinkedHashMap<>(payload);
            dlqEntry.put("dlqWebhookUrl", webhookUrl);
            dlqEntry.put("dlqReason", error);
            dlqEvents.addFirst(dlqEntry);
            while (dlqEvents.size() > 128) dlqEvents.pollLast();
            try {
                ctx.kv().set("dlq:" + ctx.requestId(), MAPPER.writeValueAsString(dlqEntry), 86_400L);
            } catch (Exception ignored) {}
        }
    }

    /**
     * 3. RedisStreamBillingSinkPlugin (builtin:billing-redis-stream-sink):
     * Publishes settled micro-USD billing events with FinOps attribution tags to a Redis Stream at ON_POST_SETTLE.
     */
    public static final class RedisStreamBillingSinkPlugin implements GatewayPlugin {
        public static final String ID = "builtin:billing-redis-stream-sink";

        private final StatefulRedisConnection<String, String> redisConn;
        private final String defaultStreamKey;

        public RedisStreamBillingSinkPlugin(StatefulRedisConnection<String, String> redisConn, String keyPrefix) {
            this.redisConn = redisConn;
            this.defaultStreamKey = (keyPrefix != null ? keyPrefix : "gw:") + "finops:billing_stream";
        }

        @Override public String id() { return ID; }
        @Override public String name() { return "billing-redis-stream-sink"; }
        @Override public String type() { return "BUILTIN"; }
        @Override public int priority() { return 60; }

        @Override
        public void onPostSettle(PluginExecutionContext ctx) {
            Map<String, Object> payload = buildBillingEventMap(ctx);
            String streamKey = ctx.pluginConfig().get("streamKey") instanceof String s && !s.isBlank()
                ? s.trim()
                : defaultStreamKey;
            try {
                String json = MAPPER.writeValueAsString(payload);
                if (redisConn != null) {
                    redisConn.sync().xadd(streamKey, Map.of("request_id", ctx.requestId(), "data", json));
                }
                ctx.kv().set("last_stream_event:" + ctx.requestId(), json, 3600L);
            } catch (Exception ignored) {}
        }
    }

    public static Map<String, Object> buildBillingEventMap(PluginExecutionContext ctx) {
        SettledBillingEvent ev = ctx.settledBillingEvent();
        Map<String, Object> map = new LinkedHashMap<>();
        if (ev != null) {
            map.put("requestId", ev.requestId());
            map.put("projectId", ev.projectId());
            map.put("workspaceId", ev.workspaceId());
            map.put("keyId", ev.keyId());
            map.put("endUserId", ev.endUserId());
            map.put("endpoint", ev.endpoint());
            map.put("modelAlias", ev.modelAlias());
            map.put("upstreamModel", ev.upstreamModel());
            map.put("status", ev.status());
            map.put("reserveMicros", ev.reserveMicros());
            map.put("actualMicros", ev.actualMicros());
            map.put("savedCacheMicros", ev.savedCacheMicros());
            map.put("inputTokens", ev.inputTokens());
            map.put("outputTokens", ev.outputTokens());
            map.put("cachedInputTokens", ev.cachedInputTokens());
            map.put("reasoningTokens", ev.reasoningTokens());
            map.put("ttftMs", ev.ttftMs());
            map.put("latencyMs", ev.latencyMs());
            map.put("attributionTags", ev.attributionTags());
            map.put("timestampEpochMs", ev.timestampEpochMs());
        } else {
            map.put("requestId", ctx.requestId());
            map.put("projectId", ctx.projectId());
            map.put("workspaceId", ctx.workspaceId());
            map.put("keyId", ctx.keyId());
            map.put("endUserId", ctx.endUserId());
            map.put("endpoint", ctx.endpoint());
            map.put("modelAlias", ctx.modelAlias());
            map.put("status", ctx.settledStatus());
            map.put("actualMicros", ctx.settledActualMicros());
            map.put("savedCacheMicros", ctx.settledSavedCacheMicros());
            map.put("inputTokens", ctx.settledInputTokens());
            map.put("outputTokens", ctx.settledOutputTokens());
            map.put("attributionTags", ctx.attributionTags());
            map.put("timestampEpochMs", System.currentTimeMillis());
        }
        return map;
    }
}
