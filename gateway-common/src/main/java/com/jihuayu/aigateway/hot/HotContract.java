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

        public String projectTpm(String projectId) {
            return prefix + "tpm:project:" + projectId;
        }

        public String keyTpm(String keyId) {
            return prefix + "tpm:key:" + keyId;
        }

        /** #39: Project-scoped Redis Cluster hash-tag key set ensuring all per-project Lua keys map to the same CRC16 slot */
        public HotKeys forClusterProject(String projectId) {
            String clusterPrefix = "gateway:{proj:" + projectId + "}:hot:v1:";
            return new HotKeys(
                namespace,
                clusterPrefix,
                clusterPrefix + "runtime",
                clusterPrefix + "config",
                clusterPrefix + "journal",
                clusterPrefix + "slots",
                clusterPrefix + "projects",
                clusterPrefix + "active"
            );
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
        Long reasoning_tokens,
        Long ttft_ms
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
            long latency_ms,
            Long cached_input_tokens,
            Long reasoning_tokens
        ) {
            this(
                attempt_no, target_id, upstream_model, status, http_status, error_code,
                request_sent, execution_unknown, usage_known, input_tokens, output_tokens,
                cost_micros, upper_bound_micros, retry_safe, latency_ms,
                cached_input_tokens, reasoning_tokens, null
            );
        }

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
                cost_micros, upper_bound_micros, retry_safe, latency_ms, null, null, null
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
        Long ttftMs,
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
        boolean enabled,
        Double sampleRate
    ) {
        public PluginBindingSnapshot(
            String id,
            String pluginId,
            String scopeType,
            String scopeId,
            int priority,
            Map<String, Object> configOverride,
            boolean enabled
        ) {
            this(id, pluginId, scopeType, scopeId, priority, configOverride, enabled, 1.0);
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record McpServerSnapshot(
        String id,
        String workspaceId,
        String alias,
        String transport,
        String endpointUrl,
        String encryptedSecret,
        List<String> allowedTools,
        int timeoutMs,
        boolean enabled
    ) {
        public boolean isToolAllowedByServer(String toolName) {
            if (allowedTools == null || allowedTools.isEmpty()) return true;
            return allowedTools.contains("*") || (toolName != null && allowedTools.contains(toolName));
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Snapshot(
        String epoch,
        Map<String, RouteSnapshot> routes,
        Map<String, List<Map<String, Object>>> models,
        List<PluginSnapshot> plugins,
        List<PluginBindingSnapshot> pluginBindings,
        List<McpServerSnapshot> mcpServers
    ) {
        public Snapshot(
            String epoch,
            Map<String, RouteSnapshot> routes,
            Map<String, List<Map<String, Object>>> models,
            List<PluginSnapshot> plugins,
            List<PluginBindingSnapshot> pluginBindings
        ) {
            this(epoch, routes, models, plugins, pluginBindings, List.of());
        }

        public Snapshot(
            String epoch,
            Map<String, RouteSnapshot> routes,
            Map<String, List<Map<String, Object>>> models
        ) {
            this(epoch, routes, models, List.of(), List.of(), List.of());
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
        List<String> allowedMcpTools,
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
            Integer projectTpm,
            Integer keyTpm,
            int concurrency,
            long budgetMicros,
            long keyBudgetMicros,
            Integer maxOutputTokens
        ) {
            this(
                runtimeRevision, runtimeKeyHash, runtimeExpiresAt, keyId, projectId, workspaceId,
                allowedModels, List.of(), projectRpm, keyRpm, projectTpm, keyTpm,
                concurrency, budgetMicros, keyBudgetMicros, maxOutputTokens
            );
        }

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
                allowedModels, List.of(), projectRpm, keyRpm, null, null, concurrency, budgetMicros, -1L, maxOutputTokens
            );
        }

        public boolean canUseModel(String aliasId, String aliasName) {
            if (allowedModels == null || allowedModels.isEmpty()) return true;
            return (aliasName != null && allowedModels.contains(aliasName))
                || (aliasId != null && !aliasId.isEmpty() && allowedModels.contains(aliasId));
        }

        public boolean canUseMcpTool(String serverAlias, String toolName) {
            if (allowedMcpTools == null || allowedMcpTools.isEmpty()) return true;
            if (allowedMcpTools.contains("*")) return true;
            if (serverAlias != null && allowedMcpTools.contains(serverAlias + ":*")) return true;
            if (toolName != null && allowedMcpTools.contains(toolName)) return true;
            return serverAlias != null && toolName != null && allowedMcpTools.contains(serverAlias + ":" + toolName);
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
        Long ttftMs,
        String status,
        List<AttemptRecord> attempts
    ) {
        public SettleInput(
            Long actualMicros,
            Long inputTokens,
            Long outputTokens,
            Long cachedInputTokens,
            Long reasoningTokens,
            String status,
            List<AttemptRecord> attempts
        ) {
            this(actualMicros, inputTokens, outputTokens, cachedInputTokens, reasoningTokens, null, status, attempts);
        }

        public SettleInput(
            Long actualMicros,
            Long inputTokens,
            Long outputTokens,
            String status,
            List<AttemptRecord> attempts
        ) {
            this(actualMicros, inputTokens, outputTokens, null, null, null, status, attempts);
        }
    }
}
