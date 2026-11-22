package com.jihuayu.aigateway.hot;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * Zero-Spring, GraalVM Native Image compatible Prometheus OpenMetrics & OpenTelemetry {@code gen_ai.*}
 * Telemetry Registry (#39).
 */
public final class GatewayTelemetry {

    public record GenAiSpan(
        String traceId,
        String spanId,
        String parentSpanId,
        String requestId,
        String operationName,
        long startEpochMs,
        long durationMs,
        Long ttftMs,
        Map<String, Object> attributes
    ) {}

    private static final GatewayTelemetry GLOBAL = new GatewayTelemetry();

    private final Map<String, LongAdder> requestCounters = new ConcurrentHashMap<>();
    private final Map<String, LongAdder> tokenCounters = new ConcurrentHashMap<>();
    private final Map<String, LongAdder> costMicrosCounters = new ConcurrentHashMap<>();
    private final Map<String, LongAdder> mcpCallCounters = new ConcurrentHashMap<>();
    private final Map<String, LongAdder> pluginExecCounters = new ConcurrentHashMap<>();

    private final LongAdder ttftCount = new LongAdder();
    private final LongAdder ttftSumMs = new LongAdder();
    private final AtomicLong lastTtftMs = new AtomicLong(0);

    private final LongAdder latencyCount = new LongAdder();
    private final LongAdder latencySumMs = new LongAdder();

    private final ConcurrentLinkedDeque<GenAiSpan> recentSpans = new ConcurrentLinkedDeque<>();
    private static final int MAX_RECENT_SPANS = 256;

    public static GatewayTelemetry global() {
        return GLOBAL;
    }

    public void recordRequest(
        String requestId,
        String traceparentHeader,
        String endpoint,
        String projectId,
        String model,
        String status,
        long latencyMs,
        Long ttftMs,
        long inputTokens,
        long outputTokens,
        long cachedInputTokens,
        long reasoningTokens,
        long costMicros
    ) {
        String ep = sanitizeLabel(endpoint != null ? endpoint : "/v1/chat/completions");
        String mdl = sanitizeLabel(model != null ? model : "unknown");
        String st = sanitizeLabel(status != null ? status : "completed");
        String proj = sanitizeLabel(projectId != null ? projectId : "unknown");

        requestCounters.computeIfAbsent(ep + "|" + mdl + "|" + st, _ -> new LongAdder()).increment();
        latencyCount.increment();
        latencySumMs.add(Math.max(0L, latencyMs));

        if (ttftMs != null && ttftMs >= 0) {
            ttftCount.increment();
            ttftSumMs.add(ttftMs);
            lastTtftMs.set(ttftMs);
        }

        if (inputTokens > 0) {
            tokenCounters.computeIfAbsent(mdl + "|input", _ -> new LongAdder()).add(inputTokens);
        }
        if (outputTokens > 0) {
            tokenCounters.computeIfAbsent(mdl + "|output", _ -> new LongAdder()).add(outputTokens);
        }
        if (cachedInputTokens > 0) {
            tokenCounters.computeIfAbsent(mdl + "|cached_input", _ -> new LongAdder()).add(cachedInputTokens);
        }
        if (reasoningTokens > 0) {
            tokenCounters.computeIfAbsent(mdl + "|reasoning", _ -> new LongAdder()).add(reasoningTokens);
        }
        if (costMicros > 0) {
            costMicrosCounters.computeIfAbsent(proj + "|" + mdl, _ -> new LongAdder()).add(costMicros);
        }

        // Record OpenTelemetry gen_ai.* semantic convention span
        String[] traceParts = parseOrGenerateTraceparent(traceparentHeader);
        Map<String, Object> attrs = new LinkedHashMap<>();
        attrs.put("gen_ai.system", "ai-gateway");
        attrs.put("gen_ai.operation.name", ep);
        attrs.put("gen_ai.request.model", mdl);
        attrs.put("gen_ai.usage.input_tokens", inputTokens);
        attrs.put("gen_ai.usage.output_tokens", outputTokens);
        attrs.put("gen_ai.usage.cached_input_tokens", cachedInputTokens);
        attrs.put("gen_ai.usage.reasoning_tokens", reasoningTokens);
        attrs.put("gen_ai.cost.micros", costMicros);
        attrs.put("gen_ai.response.status", st);
        if (ttftMs != null) {
            attrs.put("gen_ai.server.time_to_first_token_ms", ttftMs);
        }
        recordSpan(new GenAiSpan(
            traceParts[0],
            traceParts[1],
            traceParts[2],
            requestId != null ? requestId : "",
            "gen_ai " + ep + " " + mdl,
            System.currentTimeMillis() - Math.max(0L, latencyMs),
            Math.max(0L, latencyMs),
            ttftMs,
            attrs
        ));
    }

    public void recordMcpCall(String serverAlias, String method, String status) {
        String srv = sanitizeLabel(serverAlias != null ? serverAlias : "unknown");
        String m = sanitizeLabel(method != null ? method : "unknown");
        String st = sanitizeLabel(status != null ? status : "ok");
        mcpCallCounters.computeIfAbsent(srv + "|" + m + "|" + st, _ -> new LongAdder()).increment();
    }

    public void recordPluginExecution(String pluginName, String stage, String outcome) {
        String p = sanitizeLabel(pluginName != null ? pluginName : "unknown");
        String s = sanitizeLabel(stage != null ? stage : "PRE_ADMIT");
        String o = sanitizeLabel(outcome != null ? outcome : "CONTINUE");
        pluginExecCounters.computeIfAbsent(p + "|" + s + "|" + o, _ -> new LongAdder()).increment();
    }

    public void recordSpan(GenAiSpan span) {
        if (span == null) return;
        recentSpans.addFirst(span);
        while (recentSpans.size() > MAX_RECENT_SPANS) {
            recentSpans.pollLast();
        }
    }

    public List<GenAiSpan> recentSpans() {
        return Collections.unmodifiableList(new ArrayList<>(recentSpans));
    }

    public static String[] parseOrGenerateTraceparent(String traceparent) {
        if (traceparent != null && traceparent.startsWith("00-")) {
            String[] parts = traceparent.trim().split("-");
            if (parts.length >= 4 && parts[1].length() == 32 && parts[2].length() == 16) {
                String newSpanId = UUID.randomUUID().toString().replace("-", "").substring(0, 16);
                return new String[]{parts[1], newSpanId, parts[2]};
            }
        }
        String traceId = UUID.randomUUID().toString().replace("-", "");
        String spanId = UUID.randomUUID().toString().replace("-", "").substring(0, 16);
        return new String[]{traceId, spanId, ""};
    }

    /**
     * Renders Prometheus OpenMetrics text exposition format (version 0.0.4).
     */
    public String scrapePrometheus(long pooledUpstreamDispatched, int openCircuitBreakers) {
        StringBuilder sb = new StringBuilder(2048);

        sb.append("# HELP ai_gateway_requests_total Total number of AI Gateway data-plane requests.\n");
        sb.append("# TYPE ai_gateway_requests_total counter\n");
        for (Map.Entry<String, LongAdder> e : requestCounters.entrySet()) {
            String[] parts = e.getKey().split("\\|", 3);
            if (parts.length == 3) {
                sb.append("ai_gateway_requests_total{endpoint=\"")
                    .append(parts[0]).append("\",model=\"")
                    .append(parts[1]).append("\",status=\"")
                    .append(parts[2]).append("\"} ")
                    .append(e.getValue().sum()).append('\n');
            }
        }

        sb.append("# HELP ai_gateway_tokens_total Total tokens processed by model and token type.\n");
        sb.append("# TYPE ai_gateway_tokens_total counter\n");
        for (Map.Entry<String, LongAdder> e : tokenCounters.entrySet()) {
            String[] parts = e.getKey().split("\\|", 2);
            if (parts.length == 2) {
                sb.append("ai_gateway_tokens_total{model=\"")
                    .append(parts[0]).append("\",type=\"")
                    .append(parts[1]).append("\"} ")
                    .append(e.getValue().sum()).append('\n');
            }
        }

        sb.append("# HELP ai_gateway_cost_micros_total Total settled cost in micro-USD (10^-6 USD).\n");
        sb.append("# TYPE ai_gateway_cost_micros_total counter\n");
        for (Map.Entry<String, LongAdder> e : costMicrosCounters.entrySet()) {
            String[] parts = e.getKey().split("\\|", 2);
            if (parts.length == 2) {
                sb.append("ai_gateway_cost_micros_total{project_id=\"")
                    .append(parts[0]).append("\",model=\"")
                    .append(parts[1]).append("\"} ")
                    .append(e.getValue().sum()).append('\n');
            }
        }

        sb.append("# HELP ai_gateway_ttft_ms_sum Sum of Time To First Token (TTFT) in milliseconds.\n");
        sb.append("# TYPE ai_gateway_ttft_ms_sum counter\n");
        sb.append("ai_gateway_ttft_ms_sum ").append(ttftSumMs.sum()).append('\n');
        sb.append("# HELP ai_gateway_ttft_ms_count Total streaming requests with recorded TTFT.\n");
        sb.append("# TYPE ai_gateway_ttft_ms_count counter\n");
        sb.append("ai_gateway_ttft_ms_count ").append(ttftCount.sum()).append('\n');
        sb.append("# HELP ai_gateway_ttft_ms_last Most recent Time To First Token (TTFT) in milliseconds.\n");
        sb.append("# TYPE ai_gateway_ttft_ms_last gauge\n");
        sb.append("ai_gateway_ttft_ms_last ").append(lastTtftMs.get()).append('\n');

        sb.append("# HELP ai_gateway_request_latency_ms_sum Sum of end-to-end request latency in milliseconds.\n");
        sb.append("# TYPE ai_gateway_request_latency_ms_sum counter\n");
        sb.append("ai_gateway_request_latency_ms_sum ").append(latencySumMs.sum()).append('\n');
        sb.append("# HELP ai_gateway_request_latency_ms_count Total requests with recorded latency.\n");
        sb.append("# TYPE ai_gateway_request_latency_ms_count counter\n");
        sb.append("ai_gateway_request_latency_ms_count ").append(latencyCount.sum()).append('\n');

        sb.append("# HELP ai_gateway_pooled_upstream_requests_total Total upstream HTTP requests sent via connection pool.\n");
        sb.append("# TYPE ai_gateway_pooled_upstream_requests_total counter\n");
        sb.append("ai_gateway_pooled_upstream_requests_total ").append(pooledUpstreamDispatched).append('\n');

        sb.append("# HELP ai_gateway_circuit_breakers_open Current number of open upstream target circuit breakers.\n");
        sb.append("# TYPE ai_gateway_circuit_breakers_open gauge\n");
        sb.append("ai_gateway_circuit_breakers_open ").append(openCircuitBreakers).append('\n');

        sb.append("# HELP ai_gateway_mcp_calls_total Total MCP JSON-RPC proxy requests by server, method, and status.\n");
        sb.append("# TYPE ai_gateway_mcp_calls_total counter\n");
        for (Map.Entry<String, LongAdder> e : mcpCallCounters.entrySet()) {
            String[] parts = e.getKey().split("\\|", 3);
            if (parts.length == 3) {
                sb.append("ai_gateway_mcp_calls_total{server=\"")
                    .append(parts[0]).append("\",method=\"")
                    .append(parts[1]).append("\",status=\"")
                    .append(parts[2]).append("\"} ")
                    .append(e.getValue().sum()).append('\n');
            }
        }

        return sb.toString();
    }

    private static String sanitizeLabel(String raw) {
        if (raw == null || raw.isBlank()) return "unknown";
        return raw.replace("\\", "").replace("\"", "").replace("\n", "");
    }
}
