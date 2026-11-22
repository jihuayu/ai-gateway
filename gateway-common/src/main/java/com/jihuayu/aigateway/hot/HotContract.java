package com.jihuayu.aigateway.hot;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

public final class HotContract {
    private static final Pattern NAMESPACE_RE = Pattern.compile("^[A-Za-z0-9_-]{1,80}$");
    public static final long MAX_SAFE_INTEGER = 9_007_199_254_740_991L; // 2^53 - 1

    private HotContract() {}

    public record HotKeys(
        String namespace,
        String prefix,
        String runtime,
        String config,
        String journal,
        String slots,
        String projects,
        String active
    ) {
        public static HotKeys of(String namespace) {
            if (namespace == null || !NAMESPACE_RE.matcher(namespace).matches()) {
                throw new IllegalArgumentException("Invalid data-plane namespace");
            }
            String prefix = "gateway:{" + namespace + "}:hot:v1:";
            return new HotKeys(
                namespace,
                prefix,
                prefix + "runtime",
                prefix + "config",
                prefix + "journal",
                prefix + "slots",
                prefix + "projects",
                prefix + "active"
            );
        }

        public String request(String id) {
            return prefix + "request:" + id;
        }

        public String project(String id) {
            return prefix + "project:" + id;
        }

        public String snapshot(String revision) {
            return prefix + "snapshot:" + revision;
        }

        public String projectRate(String projectId) {
            return prefix + "rate:project:" + projectId;
        }

        public String keyRate(String keyId) {
            return prefix + "rate:key:" + keyId;
        }

        public String configChannel() {
            return prefix + "config:notify";
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record AttemptRecord(
        int attempt_no,
        String target_id,
        String upstream_model,
        String status,
        Integer http_status,
        String error_code,
        boolean request_sent,
        boolean execution_unknown,
        boolean usage_known,
        Long input_tokens,
        Long output_tokens,
        Long cost_micros,
        long upper_bound_micros,
        boolean retry_safe,
        long latency_ms,
        Long cached_input_tokens,
        Long reasoning_tokens
    ) {
        public AttemptRecord(
            int attempt_no,
            String target_id,
            String upstream_model,
            String status,
            Integer http_status,
            String error_code,
            boolean request_sent,
            boolean execution_unknown,
            boolean usage_known,
            Long input_tokens,
            Long output_tokens,
            Long cost_micros,
            long upper_bound_micros,
            boolean retry_safe,
            long latency_ms
        ) {
            this(
                attempt_no, target_id, upstream_model, status, http_status, error_code,
                request_sent, execution_unknown, usage_known, input_tokens, output_tokens,
                cost_micros, upper_bound_micros, retry_safe, latency_ms, null, null
            );
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record HotRequest(
        int version,
        String epoch,
        long revision,
        String requestId,
        String projectId,
        String keyId,
        String period,
        long reserveMicros,
        Object routeVersion,
        Object priceSnapshot,
        String state,
        String status,
        Long actualMicros,
        Long chargedMicros,
        Long inputTokens,
        Long outputTokens,
        Long cachedInputTokens,
        Long reasoningTokens,
        String aliasName,
        String errorCode,
        List<AttemptRecord> attempts,
        String createdAt,
        String updatedAt,
        long leaseUntil,
        String owner,
        int remainingEvents
    ) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record RouteTarget(
        String targetId,
        String upstreamModel,
        long inputPrice,
        long outputPrice,
        Long cachedInputPrice,
        Long cacheWritePrice,
        Long reasoningPrice,
        String protocol,
        int weight,
        String baseUrl,
        String encryptedSecret,
        boolean targetEnabled,
        boolean providerEnabled,
        List<String> capabilities
    ) {
        public RouteTarget(
            String targetId,
            String upstreamModel,
            long inputPrice,
            long outputPrice,
            String baseUrl,
            String encryptedSecret,
            boolean targetEnabled,
            boolean providerEnabled,
            List<String> capabilities
        ) {
            this(
                targetId, upstreamModel, inputPrice, outputPrice,
                null, null, null, "openai", 100,
                baseUrl, encryptedSecret, targetEnabled, providerEnabled, capabilities
            );
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record RouteSnapshot(
        String aliasId,
        String aliasName,
        String routeVersionId,
        int version,
        List<String> capabilities,
        List<RouteTarget> targets
    ) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record PluginSnapshot(
        String id,
        String workspaceId,
        String name,
        String type,
        int version,
        int priority,
        List<String> stages,
        String sourceCode,
        String codeSha256,
        String jarPath,
        String className,
        Map<String, Object> config,
        String failMode,
        int timeoutMs,
        int maxStatements,
        boolean enabled
    ) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record PluginBindingSnapshot(
        String id,
        String pluginId,
        String scopeType,
        String scopeId,
        int priority,
        Map<String, Object> configOverride,
        boolean enabled
    ) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Snapshot(
        String epoch,
        Map<String, RouteSnapshot> routes,
        Map<String, List<Map<String, Object>>> models,
        List<PluginSnapshot> plugins,
        List<PluginBindingSnapshot> pluginBindings
    ) {
        public Snapshot(
            String epoch,
            Map<String, RouteSnapshot> routes,
            Map<String, List<Map<String, Object>>> models
        ) {
            this(epoch, routes, models, List.of(), List.of());
        }
    }

    public record KeyContext(
        String runtimeRevision,
        String runtimeKeyHash,
        Long runtimeExpiresAt,
        String keyId,
        String projectId,
        String workspaceId,
        List<String> allowedModels,
        int projectRpm,
        int keyRpm,
        Integer projectTpm,
        Integer keyTpm,
        int concurrency,
        long budgetMicros,
        long keyBudgetMicros,
        Integer maxOutputTokens
    ) {
        public KeyContext(
            String runtimeRevision,
            String runtimeKeyHash,
            Long runtimeExpiresAt,
            String keyId,
            String projectId,
            String workspaceId,
            List<String> allowedModels,
            int projectRpm,
            int keyRpm,
            int concurrency,
            long budgetMicros,
            Integer maxOutputTokens
        ) {
            this(
                runtimeRevision, runtimeKeyHash, runtimeExpiresAt, keyId, projectId, workspaceId,
                allowedModels, projectRpm, keyRpm, null, null, concurrency, budgetMicros, -1L, maxOutputTokens
            );
        }

        public boolean canUseModel(String aliasId, String aliasName) {
            if (allowedModels == null || allowedModels.isEmpty()) return true;
            return (aliasName != null && allowedModels.contains(aliasName))
                || (aliasId != null && !aliasId.isEmpty() && allowedModels.contains(aliasId));
        }
    }

    public record AdmitInput(
        String requestId,
        String projectId,
        String keyId,
        String period,
        long reserveMicros,
        int rpm,
        int keyRpm,
        Integer projectTpm,
        Integer keyTpm,
        long reserveTokens,
        int concurrency,
        long budgetMicros,
        long keyBudgetMicros,
        Map<String, Object> routeVersion,
        Map<String, Object> priceSnapshot
    ) {
        public AdmitInput(
            String requestId,
            String projectId,
            String keyId,
            String period,
            long reserveMicros,
            int rpm,
            int keyRpm,
            int concurrency,
            long budgetMicros,
            Map<String, Object> routeVersion,
            Map<String, Object> priceSnapshot
        ) {
            this(
                requestId, projectId, keyId, period, reserveMicros,
                rpm, keyRpm, null, null, 0L, concurrency, budgetMicros, -1L,
                routeVersion, priceSnapshot
            );
        }
    }

    public record SettleInput(
        Long actualMicros,
        Long inputTokens,
        Long outputTokens,
        Long cachedInputTokens,
        Long reasoningTokens,
        String status,
        List<AttemptRecord> attempts
    ) {
        public SettleInput(
            Long actualMicros,
            Long inputTokens,
            Long outputTokens,
            String status,
            List<AttemptRecord> attempts
        ) {
            this(actualMicros, inputTokens, outputTokens, null, null, status, attempts);
        }
    }
}
