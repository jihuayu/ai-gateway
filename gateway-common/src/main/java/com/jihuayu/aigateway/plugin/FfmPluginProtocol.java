package com.jihuayu.aigateway.plugin;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jihuayu.aigateway.hot.HotContract.KeyContext;
import com.jihuayu.aigateway.hot.HotContract.RouteTarget;
import com.jihuayu.aigateway.plugin.PluginExecutionContext.PluginKvStore;
import com.jihuayu.aigateway.plugin.PluginExecutionContext.SettledBillingEvent;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

/**
 * Cross-Isolate / C-ABI JSON Wire Protocol for GraalVM {@code --shared} JAR Plugins &amp; Native FFM Plugins (#36).
 */
public final class FfmPluginProtocol {

    public static final ObjectMapper MAPPER = new ObjectMapper();

    private FfmPluginProtocol() {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record ManifestEntry(
        String id,
        String name,
        String type,
        int priority,
        String className
    ) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record ManifestPayload(
        List<ManifestEntry> plugins
    ) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record ContextRequestEnvelope(
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
        Map<String, String> responseHeaders,
        int responseStatus,
        String responseBody,
        Map<String, Object> responseJson,
        String streamChunk,
        List<RouteTarget> candidateTargets,
        Map<String, Object> pluginConfig,
        Map<String, Object> attributes,
        Map<String, String> attributionTags,
        KeyContext authenticatedKey,
        String boundApiKeyPlain,
        String endUserId,
        long settledActualMicros,
        long settledInputTokens,
        long settledOutputTokens,
        long settledSavedCacheMicros,
        String settledStatus,
        SettledBillingEvent settledBillingEvent
    ) {
        public static ContextRequestEnvelope fromContext(PluginExecutionContext ctx) {
            return new ContextRequestEnvelope(
                ctx.requestId(),
                ctx.projectId(),
                ctx.workspaceId(),
                ctx.keyId(),
                ctx.endpoint(),
                ctx.rawQuery(),
                ctx.modelAlias(),
                ctx.stream(),
                new LinkedHashMap<>(ctx.requestHeaders()),
                new LinkedHashMap<>(ctx.requestJson()),
                new LinkedHashMap<>(ctx.responseHeaders()),
                ctx.responseStatus(),
                ctx.responseBody(),
                ctx.responseJson() != null ? new LinkedHashMap<>(ctx.responseJson()) : null,
                ctx.streamChunk(),
                new ArrayList<>(ctx.candidateTargets()),
                new LinkedHashMap<>(ctx.pluginConfig()),
                toSerializableAttributes(ctx.attributes()),
                new LinkedHashMap<>(ctx.attributionTags()),
                ctx.authenticatedKey(),
                ctx.boundApiKeyPlain(),
                ctx.endUserId(),
                ctx.settledActualMicros(),
                ctx.settledInputTokens(),
                ctx.settledOutputTokens(),
                ctx.settledSavedCacheMicros(),
                ctx.settledStatus(),
                ctx.settledBillingEvent()
            );
        }

        public PluginExecutionContext toExecutionContext(
            PluginKvStore kvStore,
            BiFunction<Object, Map<String, Object>, Map<String, Object>> jevEvaluator
        ) {
            PluginExecutionContext ctx = new PluginExecutionContext(
                requestId,
                projectId,
                workspaceId,
                keyId,
                endpoint,
                rawQuery,
                modelAlias,
                stream,
                requestHeaders != null ? new LinkedHashMap<>(requestHeaders) : new LinkedHashMap<>(),
                requestJson != null ? new LinkedHashMap<>(requestJson) : new LinkedHashMap<>(),
                kvStore
            );
            if (responseHeaders != null) {
                responseHeaders.forEach(ctx::setResponseHeader);
            }
            ctx.setResponseStatus(responseStatus > 0 ? responseStatus : 200);
            if (responseBody != null) {
                ctx.setResponseBody(responseBody);
                ctx.clearResponseBodyMutated();
            }
            if (responseJson != null) {
                ctx.setResponseJson(new LinkedHashMap<>(responseJson));
                ctx.clearResponseBodyMutated();
            }
            if (streamChunk != null) {
                ctx.initStreamChunk(streamChunk);
            }
            if (candidateTargets != null && !candidateTargets.isEmpty()) {
                ctx.initCandidateTargets(candidateTargets);
            }
            if (pluginConfig != null) {
                ctx.setPluginConfig(pluginConfig);
            }
            if (attributes != null) {
                ctx.attributes().putAll(attributes);
            }
            if (attributionTags != null) {
                attributionTags.forEach(ctx::putAttributionTag);
            }
            if (authenticatedKey != null) {
                ctx.bindKeyContext(authenticatedKey);
            }
            if (boundApiKeyPlain != null && !boundApiKeyPlain.isBlank()) {
                ctx.bindAuthenticatedKey(boundApiKeyPlain);
            }
            if (endUserId != null && !endUserId.isBlank()) {
                ctx.setEndUserId(endUserId);
            }
            ctx.setSettlementMetrics(settledActualMicros, settledInputTokens, settledOutputTokens, settledStatus);
            ctx.setSettledSavedCacheMicros(settledSavedCacheMicros);
            if (settledBillingEvent != null) {
                ctx.setSettledBillingEvent(settledBillingEvent);
            }
            if (jevEvaluator != null) {
                ctx.setJevEvaluator(jevEvaluator);
            }
            ctx.clearRequestBodyMutated();
            return ctx;
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record ContextResponseEnvelope(
        String action,
        int httpStatus,
        String errorCode,
        String message,
        String shortCircuitContentType,
        String shortCircuitBody,
        Map<String, String> resultHeaders,
        Map<String, String> requestHeaders,
        Map<String, String> responseHeaders,
        String modelAlias,
        KeyContext authenticatedKey,
        String boundApiKeyPlain,
        String endUserId,
        boolean requestBodyMutated,
        Map<String, Object> requestJson,
        boolean responseBodyMutated,
        int responseStatus,
        String responseBody,
        Map<String, Object> responseJson,
        boolean streamChunkMutated,
        String streamChunk,
        boolean candidateTargetsMutated,
        List<RouteTarget> candidateTargets,
        Map<String, String> attributionTags,
        Map<String, Object> attributes,
        long settledSavedCacheMicros,
        String pluginError
    ) {
        public static ContextResponseEnvelope fromExecution(
            PluginExecutionContext beforeState,
            PluginExecutionContext ctx,
            PluginResult result
        ) {
            String act = "CONTINUE";
            int status = 200;
            String errCode = null;
            String msg = null;
            String scContentType = null;
            String scBody = null;
            Map<String, String> resHeaders = Map.of();

            if (result instanceof PluginResult.Reject rej) {
                act = "REJECT";
                status = rej.httpStatus();
                errCode = rej.errorCode();
                msg = rej.message();
                resHeaders = rej.headers() != null ? rej.headers() : Map.of();
            } else if (result instanceof PluginResult.ShortCircuit sc) {
                act = "SHORT_CIRCUIT";
                status = sc.httpStatus();
                scContentType = sc.contentType();
                scBody = sc.responseBody();
                resHeaders = sc.headers() != null ? sc.headers() : Map.of();
            }

            boolean reqBodyChanged = ctx.isRequestBodyMutated()
                || (beforeState != null && !ctx.requestJson().equals(beforeState.requestJson()));

            return new ContextResponseEnvelope(
                act,
                status,
                errCode,
                msg,
                scContentType,
                scBody,
                resHeaders,
                new LinkedHashMap<>(ctx.requestHeaders()),
                new LinkedHashMap<>(ctx.responseHeaders()),
                ctx.modelAlias(),
                ctx.authenticatedKey(),
                ctx.boundApiKeyPlain(),
                ctx.endUserId(),
                reqBodyChanged,
                reqBodyChanged ? new LinkedHashMap<>(ctx.requestJson()) : null,
                ctx.isResponseBodyMutated(),
                ctx.responseStatus(),
                ctx.isResponseBodyMutated() ? ctx.responseBody() : null,
                ctx.isResponseBodyMutated() && ctx.responseJson() != null ? new LinkedHashMap<>(ctx.responseJson()) : null,
                ctx.isStreamChunkMutated(),
                ctx.isStreamChunkMutated() ? ctx.streamChunk() : null,
                ctx.isCandidateTargetsMutated(),
                ctx.isCandidateTargetsMutated() ? new ArrayList<>(ctx.candidateTargets()) : null,
                new LinkedHashMap<>(ctx.attributionTags()),
                toSerializableAttributes(ctx.attributes()),
                ctx.settledSavedCacheMicros(),
                null
            );
        }

        public static ContextResponseEnvelope error(String errorMessage) {
            return new ContextResponseEnvelope(
                "ERROR", 500, "native_plugin_error", errorMessage, null, null, Map.of(),
                null, null, null, null, null, null, false, null, false, 200, null, null,
                false, null, false, null, null, null, 0L, errorMessage
            );
        }

        public PluginResult applyToAndBuildResult(PluginExecutionContext ctx) {
            if (pluginError != null && !pluginError.isBlank()) {
                throw new IllegalStateException(pluginError);
            }
            if (requestHeaders != null) {
                ctx.requestHeaders().clear();
                requestHeaders.forEach(ctx::setRequestHeader);
            }
            if (responseHeaders != null) {
                responseHeaders.forEach(ctx::setResponseHeader);
            }
            if (modelAlias != null && !modelAlias.isBlank() && !modelAlias.equals(ctx.modelAlias())) {
                ctx.setModelAlias(modelAlias);
            }
            if (authenticatedKey != null) {
                ctx.bindKeyContext(authenticatedKey);
            }
            if (boundApiKeyPlain != null && !boundApiKeyPlain.isBlank()) {
                ctx.bindAuthenticatedKey(boundApiKeyPlain);
            }
            if (endUserId != null && !endUserId.isBlank() && !endUserId.equals(ctx.endUserId())) {
                ctx.setEndUserId(endUserId);
            }
            if (requestBodyMutated && requestJson != null) {
                ctx.requestJson().clear();
                ctx.requestJson().putAll(requestJson);
                ctx.markRequestBodyMutated();
            }
            if (responseStatus > 0 && responseStatus != ctx.responseStatus()) {
                ctx.setResponseStatus(responseStatus);
            }
            if (responseBodyMutated) {
                if (responseBody != null) {
                    ctx.setResponseBody(responseBody);
                }
                if (responseJson != null) {
                    ctx.setResponseJson(responseJson);
                }
            }
            if (streamChunkMutated) {
                ctx.setStreamChunk(streamChunk);
            }
            if (candidateTargetsMutated && candidateTargets != null && !candidateTargets.isEmpty()) {
                ctx.setCandidateTargets(candidateTargets);
            }
            if (attributionTags != null) {
                attributionTags.forEach(ctx::putAttributionTag);
            }
            if (attributes != null) {
                ctx.attributes().putAll(attributes);
            }
            if (settledSavedCacheMicros > ctx.settledSavedCacheMicros()) {
                ctx.setSettledSavedCacheMicros(settledSavedCacheMicros);
            }

            String act = action != null ? action.trim().toUpperCase() : "CONTINUE";
            return switch (act) {
                case "REJECT" -> new PluginResult.Reject(
                    httpStatus > 0 ? httpStatus : 403,
                    errorCode != null ? errorCode : "native_plugin_rejected",
                    message != null ? message : "Rejected by native plugin",
                    resultHeaders != null ? resultHeaders : Map.of()
                );
                case "SHORT_CIRCUIT", "SHORTCIRCUIT" -> new PluginResult.ShortCircuit(
                    httpStatus > 0 ? httpStatus : 200,
                    shortCircuitContentType != null ? shortCircuitContentType : "application/json",
                    shortCircuitBody != null ? shortCircuitBody : "{}",
                    resultHeaders != null ? resultHeaders : Map.of()
                );
                default -> PluginResult.Continue.INSTANCE;
            };
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record HostUpcallRequest(
        String op,
        String key,
        String value,
        long delta,
        long ttlSeconds,
        Object jevState,
        Map<String, Object> jevQuestions
    ) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record HostUpcallResponse(
        boolean ok,
        String strVal,
        long longVal,
        boolean boolVal,
        Map<String, Object> mapVal,
        String error
    ) {}

    private static Map<String, Object> toSerializableAttributes(Map<String, Object> raw) {
        Map<String, Object> clean = new LinkedHashMap<>();
        if (raw == null) return clean;
        for (Map.Entry<String, Object> e : raw.entrySet()) {
            Object v = e.getValue();
            if (v == null || v instanceof String || v instanceof Number || v instanceof Boolean
                || v instanceof Map<?, ?> || v instanceof List<?>) {
                clean.put(e.getKey(), v);
            } else {
                clean.put(e.getKey(), String.valueOf(v));
            }
        }
        return clean;
    }
}
