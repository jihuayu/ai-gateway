package com.jihuayu.aigateway.hot;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.DoubleAdder;
import java.util.concurrent.atomic.LongAdder;

/**
 * Zero-Spring, GraalVM Native Image compatible Microkernel Telemetry Registry &amp; Kernel Engine Probes (#39, P4).
 *
 * <p>Architecture:
 * <ul>
 *   <li><b>Layer 1 (Kernel Intrinsic Probes)</b>: Zero-allocation counters/gauges for Caffeine L1/L2 near-cache,
 *       SingleFlight coalescing, MPSC batch channel, HA state &amp; local WAL depth, circuit breakers,
 *       Peak-EWMA hedging, and plugin execution telemetry, plus cardinality-guarded custom plugin metrics
 *       emitted via {@code ctx.telemetry()}.</li>
 *   <li><b>Layer 2 (Pluggable Observability Suite)</b>: Latency/TTFT Histograms, OpenTelemetry {@code gen_ai.*}
 *       spans &amp; OTLP HTTP export, Langfuse LLMOps ingestion, and structured slow/error access logs
 *       operated by {@code MicrokernelObservabilityPlugins}.</li>
 * </ul>
 */
public final class GatewayTelemetry {

    public record SpanEvent(
        String name,
        long timestampEpochMs,
        Map<String, Object> attributes
    ) {}

    public record GenAiSpan(
        String traceId,
        String spanId,
        String parentSpanId,
        String requestId,
        String operationName,
        long startEpochMs,
        long durationMs,
        Long ttftMs,
        Map<String, Object> attributes,
        List<SpanEvent> events
    ) {
        public GenAiSpan(
            String traceId,
            String spanId,
            String parentSpanId,
            String requestId,
            String operationName,
            long startEpochMs,
            long durationMs,
            Long ttftMs,
            Map<String, Object> attributes
        ) {
            this(traceId, spanId, parentSpanId, requestId, operationName, startEpochMs, durationMs, ttftMs, attributes, List.of());
        }
    }

    public record KernelEngineSnapshot(
        long pooledUpstreamDispatched,
        int openCircuitBreakers,
        long caffeineAuthHits,
        long caffeineAuthMisses,
        long caffeineAuthEvictions,
        long caffeineResponseHits,
        long caffeineResponseMisses,
        long caffeineResponseEvictions,
        long singleFlightCoalesced,
        long caffeineStaleFallbacks,
        long mpscCommandsSubmitted,
        long mpscBatchesFlushed,
        long mpscMaxBatchSize,
        int haStateCode, // 0 = NORMAL, 1 = DEGRADED_LOCAL_WAL, 2 = FENCED_SAFE_MODE
        int haWalPendingRecords,
        long haWalReplayedRecords,
        long hedgeTriggers,
        long hedgeSecondaryWins
    ) {}

    public static final class HistogramSeries {
        private final double[] bounds;
        private final LongAdder[] bucketCounts;
        private final LongAdder infCount = new LongAdder();
        private final DoubleAdder sum = new DoubleAdder();
        private final LongAdder count = new LongAdder();

        public HistogramSeries(double[] sortedBounds) {
            this.bounds = sortedBounds.clone();
            this.bucketCounts = new LongAdder[this.bounds.length];
            for (int i = 0; i < this.bounds.length; i++) {
                this.bucketCounts[i] = new LongAdder();
            }
        }

        public void observe(double value) {
            count.increment();
            sum.add(value);
            // Cumulative Prometheus histogram buckets: increment every bucket where value <= le
            for (int i = 0; i < bounds.length; i++) {
                if (value <= bounds[i]) {
                    bucketCounts[i].increment();
                }
            }
            infCount.increment();
        }

        public double[] bounds() { return bounds; }
        public long bucketCount(int idx) { return bucketCounts[idx].sum(); }
        public long infCount() { return infCount.sum(); }
        public double sum() { return sum.sum(); }
        public long count() { return count.sum(); }
    }

    public static final double[] DEFAULT_LATENCY_BUCKETS_MS = new double[]{
        5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1000.0, 2500.0, 5000.0
    };
    public static final double[] DEFAULT_PLUGIN_HISTOGRAM_BUCKETS = new double[]{
        0.1, 0.25, 0.5, 0.75, 0.9, 1.0, 5.0, 25.0, 100.0, 500.0, 2500.0
    };

    private static final int MAX_CUSTOM_SERIES = 4_096;
    private static final int MAX_LABELS_PER_SERIES = 4;
    private static final int MAX_RECENT_SPANS = 256;
    private static final int MAX_RECENT_ACCESS_LOGS = 256;

    private static final GatewayTelemetry GLOBAL = new GatewayTelemetry();

    private final Map<String, LongAdder> requestCounters = new ConcurrentHashMap<>();
    private final Map<String, LongAdder> tokenCounters = new ConcurrentHashMap<>();
    private final Map<String, LongAdder> costMicrosCounters = new ConcurrentHashMap<>();
    private final Map<String, LongAdder> mcpCallCounters = new ConcurrentHashMap<>();
    private final Map<String, LongAdder> pluginExecCounters = new ConcurrentHashMap<>();

    private final HistogramSeries requestLatencyHistogram = new HistogramSeries(DEFAULT_LATENCY_BUCKETS_MS);
    private final HistogramSeries ttftHistogram = new HistogramSeries(DEFAULT_LATENCY_BUCKETS_MS);
    private final AtomicLong lastTtftMs = new AtomicLong(0);

    // Custom plugin metrics via ctx.telemetry() with cardinality cap (MAX_CUSTOM_SERIES = 4096)
    private final AtomicInteger customSeriesCount = new AtomicInteger(0);
    private final Map<String, LongAdder> customCounters = new ConcurrentHashMap<>();
    private final Map<String, Double> customGauges = new ConcurrentHashMap<>();
    private final Map<String, HistogramSeries> customHistograms = new ConcurrentHashMap<>();

    private final ConcurrentLinkedDeque<GenAiSpan> recentSpans = new ConcurrentLinkedDeque<>();
    private final ConcurrentLinkedDeque<Map<String, Object>> recentAccessLogs = new ConcurrentLinkedDeque<>();

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
        recordRequestMetrics(endpoint, projectId, model, status, latencyMs, ttftMs, inputTokens, outputTokens, cachedInputTokens, reasoningTokens, costMicros, DEFAULT_LATENCY_BUCKETS_MS);
        String[] traceParts = parseOrGenerateTraceparent(traceparentHeader);
        Map<String, Object> attrs = new LinkedHashMap<>();
        attrs.put("gen_ai.system", "ai-gateway");
        attrs.put("gen_ai.operation.name", endpoint != null ? endpoint : "/v1/chat/completions");
        attrs.put("gen_ai.request.model", model != null ? model : "unknown");
        attrs.put("gen_ai.usage.input_tokens", inputTokens);
        attrs.put("gen_ai.usage.output_tokens", outputTokens);
        attrs.put("gen_ai.usage.cached_input_tokens", cachedInputTokens);
        attrs.put("gen_ai.usage.reasoning_tokens", reasoningTokens);
        attrs.put("gen_ai.cost.micros", costMicros);
        attrs.put("gen_ai.response.status", status != null ? status : "completed");
        if (ttftMs != null) {
            attrs.put("gen_ai.server.time_to_first_token_ms", ttftMs);
        }
        recordSpan(new GenAiSpan(
            traceParts[0],
            traceParts[1],
            traceParts[2],
            requestId != null ? requestId : "",
            "gen_ai " + (endpoint != null ? endpoint : "/v1/chat/completions") + " " + (model != null ? model : "unknown"),
            System.currentTimeMillis() - Math.max(0L, latencyMs),
            Math.max(0L, latencyMs),
            ttftMs,
            attrs,
            List.of()
        ));
    }

    public void recordRequestMetrics(
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
        long costMicros,
        double[] customBucketsMs
    ) {
        String ep = sanitizeLabel(endpoint != null ? endpoint : "/v1/chat/completions");
        String mdl = sanitizeLabel(model != null ? model : "unknown");
        String st = sanitizeLabel(status != null ? status : "completed");
        String proj = sanitizeLabel(projectId != null ? projectId : "unknown");

        requestCounters.computeIfAbsent(ep + "|" + mdl + "|" + st, _ -> new LongAdder()).increment();
        requestLatencyHistogram.observe(Math.max(0.0, (double) latencyMs));

        if (ttftMs != null && ttftMs >= 0) {
            ttftHistogram.observe((double) ttftMs);
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

    public void recordCustomCounter(String rawName, long delta, Map<String, String> labels) {
        String seriesKey = buildSeriesKey(rawName, labels);
        LongAdder existing = customCounters.get(seriesKey);
        if (existing != null) {
            existing.add(delta);
            return;
        }
        if (customSeriesCount.get() >= MAX_CUSTOM_SERIES) {
            seriesKey = sanitizeMetricName(rawName) + "{_overflow=\"true\"}";
        } else {
            customSeriesCount.incrementAndGet();
        }
        customCounters.computeIfAbsent(seriesKey, _ -> new LongAdder()).add(delta);
    }

    public void recordCustomGauge(String rawName, double value, Map<String, String> labels) {
        String seriesKey = buildSeriesKey(rawName, labels);
        if (!customGauges.containsKey(seriesKey)) {
            if (customSeriesCount.get() >= MAX_CUSTOM_SERIES) {
                seriesKey = sanitizeMetricName(rawName) + "{_overflow=\"true\"}";
            } else {
                customSeriesCount.incrementAndGet();
            }
        }
        customGauges.put(seriesKey, value);
    }

    public void recordCustomHistogram(String rawName, double value, Map<String, String> labels) {
        String seriesKey = buildSeriesKey(rawName, labels);
        HistogramSeries existing = customHistograms.get(seriesKey);
        if (existing != null) {
            existing.observe(value);
            return;
        }
        if (customSeriesCount.get() >= MAX_CUSTOM_SERIES) {
            seriesKey = sanitizeMetricName(rawName) + "|_overflow=\"true\"";
        } else {
            customSeriesCount.incrementAndGet();
        }
        customHistograms.computeIfAbsent(seriesKey, _ -> new HistogramSeries(DEFAULT_PLUGIN_HISTOGRAM_BUCKETS)).observe(value);
    }

    private static String buildSeriesKey(String rawName, Map<String, String> labels) {
        String metric = sanitizeMetricName(rawName);
        if (labels == null || labels.isEmpty()) {
            return metric + "|";
        }
        Map<String, String> sorted = new TreeMap<>();
        int count = 0;
        for (Map.Entry<String, String> e : labels.entrySet()) {
            if (e.getKey() == null || e.getKey().isBlank() || e.getValue() == null) continue;
            sorted.put(sanitizeMetricName(e.getKey()), sanitizeLabel(e.getValue()));
            if (++count >= MAX_LABELS_PER_SERIES) break;
        }
        StringBuilder sb = new StringBuilder(metric).append('|');
        boolean first = true;
        for (Map.Entry<String, String> e : sorted.entrySet()) {
            if (!first) sb.append(',');
            first = false;
            sb.append(e.getKey()).append("=\"").append(e.getValue()).append('"');
        }
        return sb.toString();
    }

    public void recordSpan(GenAiSpan span) {
        if (span == null) return;
        recentSpans.removeIf(existing -> !span.requestId().isEmpty() && span.requestId().equals(existing.requestId()));
        recentSpans.addFirst(span);
        while (recentSpans.size() > MAX_RECENT_SPANS) {
            recentSpans.pollLast();
        }
    }

    public List<GenAiSpan> recentSpans() {
        return Collections.unmodifiableList(new ArrayList<>(recentSpans));
    }

    public void recordAccessLog(Map<String, Object> entry) {
        if (entry == null) return;
        recentAccessLogs.addFirst(entry);
        while (recentAccessLogs.size() > MAX_RECENT_ACCESS_LOGS) {
            recentAccessLogs.pollLast();
        }
    }

    public List<Map<String, Object>> recentAccessLogs() {
        return Collections.unmodifiableList(new ArrayList<>(recentAccessLogs));
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

    public String scrapePrometheus(long pooledUpstreamDispatched, int openCircuitBreakers) {
        return scrapePrometheus(new KernelEngineSnapshot(
            pooledUpstreamDispatched, openCircuitBreakers,
            0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0
        ));
    }

    /**
     * Renders Prometheus OpenMetrics text exposition format (version 0.0.4), combining
     * Layer 1 Kernel Intrinsic Probes + Layer 2 Business Histograms + ctx.telemetry() Custom Plugin Metrics.
     */
    public String scrapePrometheus(KernelEngineSnapshot snap) {
        StringBuilder sb = new StringBuilder(4096);

        // 1. Business Request / Token / Cost Counters
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

        // 2. Latency & TTFT Prometheus Histograms (enabling P50 / P95 / P99 histogram_quantile in Grafana)
        appendHistogram(sb, "ai_gateway_request_latency_ms", "End-to-end request latency histogram in milliseconds.", requestLatencyHistogram, "");
        appendHistogram(sb, "ai_gateway_ttft_ms", "Streaming Time To First Token (TTFT) histogram in milliseconds.", ttftHistogram, "");
        sb.append("# HELP ai_gateway_ttft_ms_last Most recent Time To First Token (TTFT) in milliseconds.\n");
        sb.append("# TYPE ai_gateway_ttft_ms_last gauge\n");
        sb.append("ai_gateway_ttft_ms_last ").append(lastTtftMs.get()).append('\n');

        // 3. Layer 1 Kernel Intrinsic Probes (Caffeine L1, SingleFlight, MPSC, HA State & WAL, Peak-EWMA Hedging, Plugins)
        sb.append("# HELP ai_gateway_pooled_upstream_requests_total Total upstream HTTP requests sent via connection pool.\n");
        sb.append("# TYPE ai_gateway_pooled_upstream_requests_total counter\n");
        sb.append("ai_gateway_pooled_upstream_requests_total ").append(snap.pooledUpstreamDispatched()).append('\n');

        sb.append("# HELP ai_gateway_circuit_breakers_open Current number of open upstream target circuit breakers.\n");
        sb.append("# TYPE ai_gateway_circuit_breakers_open gauge\n");
        sb.append("ai_gateway_circuit_breakers_open ").append(snap.openCircuitBreakers()).append('\n');

        sb.append("# HELP ai_gateway_caffeine_hits_total Total Caffeine W-TinyLFU L1 cache hits.\n");
        sb.append("# TYPE ai_gateway_caffeine_hits_total counter\n");
        sb.append("ai_gateway_caffeine_hits_total{cache=\"auth\"} ").append(snap.caffeineAuthHits()).append('\n');
        sb.append("ai_gateway_caffeine_hits_total{cache=\"response\"} ").append(snap.caffeineResponseHits()).append('\n');

        sb.append("# HELP ai_gateway_caffeine_misses_total Total Caffeine W-TinyLFU L1 cache misses.\n");
        sb.append("# TYPE ai_gateway_caffeine_misses_total counter\n");
        sb.append("ai_gateway_caffeine_misses_total{cache=\"auth\"} ").append(snap.caffeineAuthMisses()).append('\n');
        sb.append("ai_gateway_caffeine_misses_total{cache=\"response\"} ").append(snap.caffeineResponseMisses()).append('\n');

        sb.append("# HELP ai_gateway_caffeine_evictions_total Total Caffeine W-TinyLFU L1 cache evictions.\n");
        sb.append("# TYPE ai_gateway_caffeine_evictions_total counter\n");
        sb.append("ai_gateway_caffeine_evictions_total{cache=\"auth\"} ").append(snap.caffeineAuthEvictions()).append('\n');
        sb.append("ai_gateway_caffeine_evictions_total{cache=\"response\"} ").append(snap.caffeineResponseEvictions()).append('\n');

        sb.append("# HELP ai_gateway_singleflight_coalesced_total Concurrent identical cache misses coalesced by SingleFlight.\n");
        sb.append("# TYPE ai_gateway_singleflight_coalesced_total counter\n");
        sb.append("ai_gateway_singleflight_coalesced_total ").append(snap.singleFlightCoalesced()).append('\n');

        sb.append("# HELP ai_gateway_caffeine_stale_fallbacks_total Stale-While-Revalidate auth fallbacks served during Redis failover.\n");
        sb.append("# TYPE ai_gateway_caffeine_stale_fallbacks_total counter\n");
        sb.append("ai_gateway_caffeine_stale_fallbacks_total ").append(snap.caffeineStaleFallbacks()).append('\n');

        sb.append("# HELP ai_gateway_mpsc_commands_total Total Redis Lua commands submitted through MPSC group-commit channel.\n");
        sb.append("# TYPE ai_gateway_mpsc_commands_total counter\n");
        sb.append("ai_gateway_mpsc_commands_total ").append(snap.mpscCommandsSubmitted()).append('\n');

        sb.append("# HELP ai_gateway_mpsc_batches_total Total pipelined TCP batches flushed by MPSC channel.\n");
        sb.append("# TYPE ai_gateway_mpsc_batches_total counter\n");
        sb.append("ai_gateway_mpsc_batches_total ").append(snap.mpscBatchesFlushed()).append('\n');

        sb.append("# HELP ai_gateway_mpsc_max_batch_size Maximum observed micro-batch size in MPSC channel.\n");
        sb.append("# TYPE ai_gateway_mpsc_max_batch_size gauge\n");
        sb.append("ai_gateway_mpsc_max_batch_size ").append(snap.mpscMaxBatchSize()).append('\n');

        sb.append("# HELP ai_gateway_ha_state Current Distributed HA state (0=NORMAL, 1=DEGRADED_LOCAL_WAL, 2=FENCED_SAFE_MODE).\n");
        sb.append("# TYPE ai_gateway_ha_state gauge\n");
        sb.append("ai_gateway_ha_state ").append(snap.haStateCode()).append('\n');

        sb.append("# HELP ai_gateway_ha_wal_pending_records Pending settlement records buffered in local WAL ring buffer.\n");
        sb.append("# TYPE ai_gateway_ha_wal_pending_records gauge\n");
        sb.append("ai_gateway_ha_wal_pending_records ").append(snap.haWalPendingRecords()).append('\n');

        sb.append("# HELP ai_gateway_ha_wal_replayed_total Total local WAL records reconciled after Redis recovery.\n");
        sb.append("# TYPE ai_gateway_ha_wal_replayed_total counter\n");
        sb.append("ai_gateway_ha_wal_replayed_total ").append(snap.haWalReplayedRecords()).append('\n');

        sb.append("# HELP ai_gateway_hedge_triggers_total Total Peak-EWMA adaptive tail-latency hedged requests triggered.\n");
        sb.append("# TYPE ai_gateway_hedge_triggers_total counter\n");
        sb.append("ai_gateway_hedge_triggers_total ").append(snap.hedgeTriggers()).append('\n');

        sb.append("# HELP ai_gateway_hedge_secondary_wins_total Total hedged requests where secondary target beat slow primary.\n");
        sb.append("# TYPE ai_gateway_hedge_secondary_wins_total counter\n");
        sb.append("ai_gateway_hedge_secondary_wins_total ").append(snap.hedgeSecondaryWins()).append('\n');

        sb.append("# HELP ai_gateway_plugin_executions_total Total microkernel plugin executions by plugin, stage, and outcome.\n");
        sb.append("# TYPE ai_gateway_plugin_executions_total counter\n");
        for (Map.Entry<String, LongAdder> e : pluginExecCounters.entrySet()) {
            String[] parts = e.getKey().split("\\|", 3);
            if (parts.length == 3) {
                sb.append("ai_gateway_plugin_executions_total{plugin=\"")
                    .append(parts[0]).append("\",stage=\"")
                    .append(parts[1]).append("\",outcome=\"")
                    .append(parts[2]).append("\"} ")
                    .append(e.getValue().sum()).append('\n');
            }
        }

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

        // 4. Custom Plugin Metrics emitted via Java/JS ctx.telemetry()
        for (Map.Entry<String, LongAdder> e : customCounters.entrySet()) {
            String[] parts = e.getKey().split("\\|", 2);
            String mName = "ai_gateway_plugin_metric_" + parts[0] + "_total";
            String lbls = parts.length > 1 && !parts[1].isEmpty() ? "{" + parts[1] + "}" : "";
            sb.append(mName).append(lbls).append(' ').append(e.getValue().sum()).append('\n');
        }
        for (Map.Entry<String, Double> e : customGauges.entrySet()) {
            String[] parts = e.getKey().split("\\|", 2);
            String mName = "ai_gateway_plugin_metric_" + parts[0];
            String lbls = parts.length > 1 && !parts[1].isEmpty() ? "{" + parts[1] + "}" : "";
            sb.append(mName).append(lbls).append(' ').append(formatDouble(e.getValue())).append('\n');
        }
        for (Map.Entry<String, HistogramSeries> e : customHistograms.entrySet()) {
            String[] parts = e.getKey().split("\\|", 2);
            String mName = "ai_gateway_plugin_metric_" + parts[0];
            String extraLabels = parts.length > 1 ? parts[1] : "";
            appendHistogram(sb, mName, "Custom plugin histogram " + parts[0], e.getValue(), extraLabels);
        }

        return sb.toString();
    }

    private static void appendHistogram(StringBuilder sb, String name, String help, HistogramSeries h, String extraLabels) {
        sb.append("# HELP ").append(name).append(' ').append(help).append('\n');
        sb.append("# TYPE ").append(name).append(" histogram\n");
        String prefixLabels = (extraLabels != null && !extraLabels.isEmpty()) ? extraLabels + "," : "";
        String plainLabels = (extraLabels != null && !extraLabels.isEmpty()) ? "{" + extraLabels + "}" : "";
        double[] bounds = h.bounds();
        for (int i = 0; i < bounds.length; i++) {
            sb.append(name).append("_bucket{").append(prefixLabels).append("le=\"")
                .append(formatDouble(bounds[i])).append("\"} ").append(h.bucketCount(i)).append('\n');
        }
        sb.append(name).append("_bucket{").append(prefixLabels).append("le=\"+Inf\"} ").append(h.infCount()).append('\n');
        sb.append(name).append("_sum").append(plainLabels).append(' ').append(formatDouble(h.sum())).append('\n');
        sb.append(name).append("_count").append(plainLabels).append(' ').append(h.count()).append('\n');
    }

    private static String formatDouble(double d) {
        if (d == (long) d) {
            return String.format(Locale.ROOT, "%d", (long) d);
        }
        return String.format(Locale.ROOT, "%.4f", d).replaceAll("0+$", "").replaceAll("\\.$", "");
    }

    public static String sanitizeMetricName(String s) {
        if (s == null || s.isBlank()) return "unnamed";
        String cleaned = s.trim().toLowerCase(Locale.ROOT).replaceAll("[^a-z0-9_]", "_");
        if (Character.isDigit(cleaned.charAt(0))) {
            cleaned = "_" + cleaned;
        }
        return cleaned.length() > 64 ? cleaned.substring(0, 64) : cleaned;
    }

    private static String sanitizeLabel(String s) {
        if (s == null) return "";
        return s.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", " ");
    }
}
