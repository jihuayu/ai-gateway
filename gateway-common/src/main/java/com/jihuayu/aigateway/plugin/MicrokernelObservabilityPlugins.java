package com.jihuayu.aigateway.plugin;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jihuayu.aigateway.hot.GatewayTelemetry;
import com.jihuayu.aigateway.hot.GatewayTelemetry.GenAiSpan;
import com.jihuayu.aigateway.hot.GatewayTelemetry.SpanEvent;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Built-in Pluggable Microkernel Observability Suite:
 * <ol>
 *   <li>{@link PrometheusMetricsPlugin} ({@code builtin:obs-prometheus-metrics}): Multi-dimensional attribution &amp; custom bucket latency/TTFT histograms.</li>
 *   <li>{@link OpenTelemetryGenAiTracingPlugin} ({@code builtin:obs-otel-genai}): W3C {@code traceparent} propagation, OpenTelemetry {@code gen_ai.*} spans merged with {@code ctx.telemetry()} attributes/events, and asynchronous OTLP HTTP JSON exporter.</li>
 *   <li>{@link LangfuseLlmOpsSinkPlugin} ({@code builtin:obs-langfuse-sink}): Asynchronous non-blocking LLMOps ingestion sink (Langfuse {@code trace-create} + {@code generation-create}).</li>
 *   <li>{@link StructuredAccessLogPlugin} ({@code builtin:obs-access-log}): Configurable slow-query &amp; error-filtered structured JSON access log ring &amp; HTTP shipper.</li>
 * </ol>
 */
public final class MicrokernelObservabilityPlugins {

    private MicrokernelObservabilityPlugins() {}

    /**
     * 1. PrometheusMetricsPlugin (builtin:obs-prometheus-metrics):
     * Operates at POST_SETTLE to project attribution-tagged request/cost/latency metrics into GatewayTelemetry.
     */
    public static final class PrometheusMetricsPlugin implements GatewayPlugin {
        public static final String ID = "builtin:obs-prometheus-metrics";

        @Override public String id() { return ID; }
        @Override public String name() { return "obs-prometheus-metrics"; }
        @Override public String type() { return "BUILTIN"; }
        @Override public int priority() { return 40; }

        @Override
        @SuppressWarnings("unchecked")
        public void onPostSettle(PluginExecutionContext ctx) {
            PluginExecutionContext.SettledBillingEvent ev = ctx.settledBillingEvent();
            if (ev == null) {
                return;
            }
            Map<String, Object> cfg = ctx.pluginConfig();
            boolean includeProj = !Boolean.FALSE.equals(cfg.get("includeProjectLabel"));

            Map<String, String> labels = new LinkedHashMap<>();
            labels.put("model", ev.modelAlias());
            labels.put("status", ev.status());
            if (includeProj && ev.projectId() != null && !ev.projectId().isBlank()) {
                labels.put("project_id", ev.projectId());
            }
            if (cfg.get("includeAttributionLabels") instanceof List<?> attrKeys && ev.attributionTags() != null) {
                for (Object kObj : attrKeys) {
                    if (labels.size() >= 4) break;
                    if (kObj instanceof String k && ev.attributionTags().containsKey(k)) {
                        labels.put(k, ev.attributionTags().get(k));
                    }
                }
            }

            GatewayTelemetry.global().recordCustomCounter("obs_requests", 1L, labels);
            if (ev.actualMicros() > 0) {
                GatewayTelemetry.global().recordCustomCounter("obs_cost_micros", ev.actualMicros(), labels);
            }
            GatewayTelemetry.global().recordCustomHistogram("obs_latency_ms", (double) ev.latencyMs(), Map.of("model", ev.modelAlias()));
            if (ev.ttftMs() != null) {
                GatewayTelemetry.global().recordCustomHistogram("obs_ttft_ms", (double) ev.ttftMs(), Map.of("model", ev.modelAlias()));
            }
            return;
        }
    }

    /**
     * 2. OpenTelemetryGenAiTracingPlugin (builtin:obs-otel-genai):
     * Propagates W3C traceparent at REQUEST_RECEIVED, constructs OpenTelemetry gen_ai.* semantic convention spans
     * merged with ctx.telemetry() custom attributes/events at POST_SETTLE, and exports OTLP HTTP JSON asynchronously.
     */
    public static final class OpenTelemetryGenAiTracingPlugin implements GatewayPlugin {
        public static final String ID = "builtin:obs-otel-genai";
        private final HttpClient httpClient;
        private final ObjectMapper mapper;

        public OpenTelemetryGenAiTracingPlugin(HttpClient httpClient, ObjectMapper mapper) {
            this.httpClient = httpClient;
            this.mapper = mapper != null ? mapper : new ObjectMapper();
        }

        @Override public String id() { return ID; }
        @Override public String name() { return "obs-otel-genai"; }
        @Override public String type() { return "BUILTIN"; }
        @Override public int priority() { return 42; }

        @Override
        public PluginResult onRequestReceived(PluginExecutionContext ctx) {
            ctx.setResponseHeader("x-trace-id", ctx.telemetry().traceId());
            ctx.setResponseHeader("traceparent", ctx.telemetry().traceparent());
            return PluginResult.Continue.INSTANCE;
        }

        @Override
        @SuppressWarnings("unchecked")
        public void onPostSettle(PluginExecutionContext ctx) {
            Map<String, Object> cfg = ctx.pluginConfig();
            PluginExecutionContext.SettledBillingEvent ev = ctx.settledBillingEvent();
            String serviceName = cfg.get("serviceName") instanceof String s && !s.isBlank() ? s.trim() : "ai-gateway";
            boolean capturePrompt = Boolean.TRUE.equals(cfg.get("capturePrompt"));
            boolean captureCompletion = Boolean.TRUE.equals(cfg.get("captureCompletion"));
            int maxLen = cfg.get("maxAttrLength") instanceof Number n ? Math.max(64, n.intValue()) : 1024;

            long durationMs = ev != null ? Math.max(0L, ev.latencyMs()) : 0L;
            Long ttftMs = ev != null ? ev.ttftMs() : null;
            long endMs = System.currentTimeMillis();
            long startMs = endMs - durationMs;

            Map<String, Object> attrs = new LinkedHashMap<>();
            attrs.put("service.name", serviceName);
            attrs.put("gen_ai.system", "ai-gateway");
            attrs.put("gen_ai.operation.name", ctx.endpoint());
            attrs.put("gen_ai.request.model", ctx.modelAlias());
            if (ev != null) {
                attrs.put("gen_ai.response.model", ev.upstreamModel() != null ? ev.upstreamModel() : ctx.modelAlias());
                attrs.put("gen_ai.usage.input_tokens", ev.inputTokens());
                attrs.put("gen_ai.usage.output_tokens", ev.outputTokens());
                attrs.put("gen_ai.usage.cached_input_tokens", ev.cachedInputTokens());
                attrs.put("gen_ai.usage.reasoning_tokens", ev.reasoningTokens());
                attrs.put("gen_ai.cost.micros", ev.actualMicros());
                attrs.put("gen_ai.response.status", ev.status());
                if (ttftMs != null) {
                    attrs.put("gen_ai.server.time_to_first_token_ms", ttftMs);
                }
                if (ev.attributionTags() != null) {
                    for (Map.Entry<String, String> tag : ev.attributionTags().entrySet()) {
                        attrs.put("gen_ai.attribution." + tag.getKey(), tag.getValue());
                    }
                }
            }
            if (capturePrompt) {
                attrs.put("gen_ai.prompt", truncate(ctx.extractPromptText(), maxLen));
            }
            if (captureCompletion && ctx.responseBody() != null) {
                attrs.put("gen_ai.completion", truncate(ctx.responseBody(), maxLen));
            }
            // Merge custom attributes added by Java/JS plugins via ctx.telemetry().setSpanAttr(...)
            attrs.putAll(ctx.telemetry().customSpanAttrs());

            List<SpanEvent> events = ctx.telemetry().customSpanEvents();
            GenAiSpan span = new GenAiSpan(
                ctx.telemetry().traceId(),
                ctx.telemetry().spanId(),
                ctx.telemetry().parentSpanId(),
                ctx.requestId(),
                "gen_ai " + ctx.endpoint() + " " + ctx.modelAlias(),
                startMs,
                durationMs,
                ttftMs,
                attrs,
                events
            );
            GatewayTelemetry.global().recordSpan(span);

            String otlpEndpoint = cfg.get("otlpEndpoint") instanceof String s ? s.trim() : "";
            if (!otlpEndpoint.isEmpty() && httpClient != null) {
                Map<String, String> otlpHeaders = new LinkedHashMap<>();
                if (cfg.get("otlpHeaders") instanceof Map<?, ?> hm) {
                    for (Map.Entry<?, ?> e : hm.entrySet()) {
                        if (e.getKey() != null && e.getValue() != null) {
                            otlpHeaders.put(String.valueOf(e.getKey()), String.valueOf(e.getValue()));
                        }
                    }
                }
                exportOtlpHttpJsonAsync(otlpEndpoint, otlpHeaders, serviceName, span);
            }
            return;
        }

        private void exportOtlpHttpJsonAsync(String endpoint, Map<String, String> headers, String serviceName, GenAiSpan span) {
            try {
                List<Map<String, Object>> otlpAttrs = new ArrayList<>();
                for (Map.Entry<String, Object> e : span.attributes().entrySet()) {
                    otlpAttrs.add(toOtlpKeyValue(e.getKey(), e.getValue()));
                }
                List<Map<String, Object>> otlpEvents = new ArrayList<>();
                for (SpanEvent ev : span.events()) {
                    List<Map<String, Object>> evKvs = new ArrayList<>();
                    for (Map.Entry<String, Object> a : ev.attributes().entrySet()) {
                        evKvs.add(toOtlpKeyValue(a.getKey(), a.getValue()));
                    }
                    otlpEvents.add(Map.of(
                        "timeUnixNano", String.valueOf(ev.timestampEpochMs() * 1_000_000L),
                        "name", ev.name(),
                        "attributes", evKvs
                    ));
                }
                Map<String, Object> otlpSpan = new LinkedHashMap<>();
                otlpSpan.put("traceId", span.traceId());
                otlpSpan.put("spanId", span.spanId());
                if (span.parentSpanId() != null && !span.parentSpanId().isEmpty()) {
                    otlpSpan.put("parentSpanId", span.parentSpanId());
                }
                otlpSpan.put("name", span.operationName());
                otlpSpan.put("kind", 2); // SPAN_KIND_SERVER
                otlpSpan.put("startTimeUnixNano", String.valueOf(span.startEpochMs() * 1_000_000L));
                otlpSpan.put("endTimeUnixNano", String.valueOf((span.startEpochMs() + span.durationMs()) * 1_000_000L));
                otlpSpan.put("attributes", otlpAttrs);
                otlpSpan.put("events", otlpEvents);

                Map<String, Object> payload = Map.of(
                    "resourceSpans", List.of(Map.of(
                        "resource", Map.of("attributes", List.of(toOtlpKeyValue("service.name", serviceName))),
                        "scopeSpans", List.of(Map.of(
                            "scope", Map.of("name", "com.jihuayu.aigateway.otel", "version", "0.4.2"),
                            "spans", List.of(otlpSpan)
                        ))
                    ))
                );

                byte[] jsonBytes = mapper.writeValueAsBytes(payload);
                HttpRequest.Builder rb = HttpRequest.newBuilder()
                    .uri(URI.create(endpoint))
                    .timeout(Duration.ofSeconds(3))
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofByteArray(jsonBytes));
                for (Map.Entry<String, String> h : headers.entrySet()) {
                    rb.header(h.getKey(), h.getValue());
                }
                httpClient.sendAsync(rb.build(), HttpResponse.BodyHandlers.discarding());
            } catch (Exception ignored) {}
        }

        private static Map<String, Object> toOtlpKeyValue(String key, Object val) {
            if (val instanceof Boolean b) {
                return Map.of("key", key, "value", Map.of("boolValue", b));
            }
            if (val instanceof Integer || val instanceof Long) {
                return Map.of("key", key, "value", Map.of("intValue", String.valueOf(((Number) val).longValue())));
            }
            if (val instanceof Number n) {
                return Map.of("key", key, "value", Map.of("doubleValue", n.doubleValue()));
            }
            return Map.of("key", key, "value", Map.of("stringValue", String.valueOf(val)));
        }
    }

    /**
     * 3. LangfuseLlmOpsSinkPlugin (builtin:obs-langfuse-sink):
     * Exports LLM traces and generations to Langfuse / compatible LLMOps ingestion APIs asynchronously at POST_SETTLE.
     */
    public static final class LangfuseLlmOpsSinkPlugin implements GatewayPlugin {
        public static final String ID = "builtin:obs-langfuse-sink";
        private final HttpClient httpClient;
        private final ObjectMapper mapper;

        public LangfuseLlmOpsSinkPlugin(HttpClient httpClient, ObjectMapper mapper) {
            this.httpClient = httpClient;
            this.mapper = mapper != null ? mapper : new ObjectMapper();
        }

        @Override public String id() { return ID; }
        @Override public String name() { return "obs-langfuse-sink"; }
        @Override public String type() { return "BUILTIN"; }
        @Override public int priority() { return 45; }

        @Override
        public void onPostSettle(PluginExecutionContext ctx) {
            Map<String, Object> cfg = ctx.pluginConfig();
            String endpointUrl = cfg.get("endpointUrl") instanceof String s ? s.trim() : "";
            if (endpointUrl.isEmpty() || httpClient == null) {
                return;
            }
            String pubKey = cfg.get("publicKey") instanceof String s ? s.trim() : "";
            String secKey = cfg.get("secretKey") instanceof String s ? s.trim() : "";
            boolean captureIo = Boolean.TRUE.equals(cfg.get("captureInputOutput"));
            String env = cfg.get("environment") instanceof String s && !s.isBlank() ? s.trim() : "production";
            int timeoutMs = cfg.get("timeoutMs") instanceof Number n ? Math.max(200, n.intValue()) : 3_000;

            PluginExecutionContext.SettledBillingEvent ev = ctx.settledBillingEvent();
            String nowIso = Instant.now().toString();
            Map<String, Object> traceBody = new LinkedHashMap<>();
            traceBody.put("id", ctx.telemetry().traceId());
            traceBody.put("name", ctx.endpoint());
            traceBody.put("userId", ctx.endUserId() != null ? ctx.endUserId() : "");
            traceBody.put("environment", env);
            traceBody.put("metadata", ev != null ? ev.attributionTags() : ctx.attributionTags());
            if (captureIo) {
                traceBody.put("input", truncate(ctx.extractPromptText(), 2048));
                traceBody.put("output", truncate(ctx.responseBody(), 2048));
            }

            Map<String, Object> genBody = new LinkedHashMap<>();
            genBody.put("id", ctx.telemetry().spanId());
            genBody.put("traceId", ctx.telemetry().traceId());
            genBody.put("name", "llm-generation");
            genBody.put("model", ev != null && ev.upstreamModel() != null && !ev.upstreamModel().isEmpty() ? ev.upstreamModel() : ctx.modelAlias());
            if (ev != null) {
                genBody.put("usage", Map.of(
                    "input", ev.inputTokens(),
                    "output", ev.outputTokens(),
                    "total", ev.inputTokens() + ev.outputTokens(),
                    "unit", "TOKENS"
                ));
                genBody.put("costDetails", Map.of("total", ev.actualMicros() / 1_000_000.0));
            }

            Map<String, Object> batchPayload = Map.of(
                "batch", List.of(
                    Map.of("id", UUID.randomUUID().toString(), "timestamp", nowIso, "type", "trace-create", "body", traceBody),
                    Map.of("id", UUID.randomUUID().toString(), "timestamp", nowIso, "type", "generation-create", "body", genBody)
                )
            );

            try {
                byte[] bytes = mapper.writeValueAsBytes(batchPayload);
                HttpRequest.Builder rb = HttpRequest.newBuilder()
                    .uri(URI.create(endpointUrl))
                    .timeout(Duration.ofMillis(timeoutMs))
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofByteArray(bytes));
                if (!pubKey.isEmpty() || !secKey.isEmpty()) {
                    String basic = Base64.getEncoder().encodeToString((pubKey + ":" + secKey).getBytes(StandardCharsets.UTF_8));
                    rb.header("Authorization", "Basic " + basic);
                }
                httpClient.sendAsync(rb.build(), HttpResponse.BodyHandlers.discarding());
            } catch (Exception ignored) {}
            return;
        }
    }

    /**
     * 4. StructuredAccessLogPlugin (builtin:obs-access-log):
     * Emits structured JSON access/audit logs with slow-query threshold &amp; error-only filtering at POST_SETTLE.
     */
    public static final class StructuredAccessLogPlugin implements GatewayPlugin {
        public static final String ID = "builtin:obs-access-log";
        private final HttpClient httpClient;
        private final ObjectMapper mapper;

        public StructuredAccessLogPlugin(HttpClient httpClient, ObjectMapper mapper) {
            this.httpClient = httpClient;
            this.mapper = mapper != null ? mapper : new ObjectMapper();
        }

        @Override public String id() { return ID; }
        @Override public String name() { return "obs-access-log"; }
        @Override public String type() { return "BUILTIN"; }
        @Override public int priority() { return 48; }

        @Override
        public void onPostSettle(PluginExecutionContext ctx) {
            Map<String, Object> cfg = ctx.pluginConfig();
            PluginExecutionContext.SettledBillingEvent ev = ctx.settledBillingEvent();
            long latencyMs = ev != null ? ev.latencyMs() : 0L;
            String status = ev != null ? ev.status() : "completed";

            long slowThresholdMs = cfg.get("slowThresholdMs") instanceof Number n ? n.longValue() : 0L;
            boolean errorsOnly = Boolean.TRUE.equals(cfg.get("errorsOnly"));

            if (errorsOnly && "completed".equalsIgnoreCase(status)) {
                return;
            }
            if (slowThresholdMs > 0 && latencyMs < slowThresholdMs) {
                return;
            }

            Map<String, Object> logEntry = new LinkedHashMap<>();
            logEntry.put("timestamp", Instant.now().toString());
            logEntry.put("requestId", ctx.requestId());
            logEntry.put("traceId", ctx.telemetry().traceId());
            logEntry.put("projectId", ctx.projectId());
            logEntry.put("keyId", ctx.keyId());
            logEntry.put("endpoint", ctx.endpoint());
            logEntry.put("modelAlias", ctx.modelAlias());
            logEntry.put("upstreamModel", ev != null ? ev.upstreamModel() : "");
            logEntry.put("status", status);
            logEntry.put("latencyMs", latencyMs);
            logEntry.put("ttftMs", ev != null ? ev.ttftMs() : null);
            logEntry.put("actualMicros", ev != null ? ev.actualMicros() : 0L);
            logEntry.put("inputTokens", ev != null ? ev.inputTokens() : 0L);
            logEntry.put("outputTokens", ev != null ? ev.outputTokens() : 0L);
            logEntry.put("attributionTags", ev != null ? ev.attributionTags() : ctx.attributionTags());

            if (cfg.get("includeHeaders") instanceof List<?> hdrList) {
                Map<String, String> capturedHeaders = new LinkedHashMap<>();
                for (Object hObj : hdrList) {
                    if (hObj instanceof String hName) {
                        String val = ctx.requestHeaders().get(hName.toLowerCase());
                        if (val != null) {
                            capturedHeaders.put(hName.toLowerCase(), val);
                        }
                    }
                }
                if (!capturedHeaders.isEmpty()) {
                    logEntry.put("headers", capturedHeaders);
                }
            }

            GatewayTelemetry.global().recordAccessLog(logEntry);

            String httpSinkUrl = cfg.get("httpSinkUrl") instanceof String s ? s.trim() : "";
            if (!httpSinkUrl.isEmpty() && httpClient != null) {
                try {
                    byte[] bytes = mapper.writeValueAsBytes(logEntry);
                    HttpRequest req = HttpRequest.newBuilder()
                        .uri(URI.create(httpSinkUrl))
                        .timeout(Duration.ofSeconds(2))
                        .header("Content-Type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofByteArray(bytes))
                        .build();
                    httpClient.sendAsync(req, HttpResponse.BodyHandlers.discarding());
                } catch (Exception ignored) {}
            }
            return;
        }
    }

    private static String truncate(String s, int maxLen) {
        if (s == null) return "";
        return s.length() <= maxLen ? s : s.substring(0, maxLen) + "...[truncated]";
    }
}
