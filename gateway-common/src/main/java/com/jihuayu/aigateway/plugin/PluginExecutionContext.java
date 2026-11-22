package com.jihuayu.aigateway.plugin;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * Mutable execution context shared across the 5 plugin lifecycle stages of a gateway request.
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

    private final String requestId;
    private final String projectId;
    private final String workspaceId;
    private final String keyId;
    private final String endpoint;
    private String modelAlias;
    private final boolean stream;
    private final Map<String, String> requestHeaders;
    private final Map<String, Object> requestJson;
    private final Map<String, String> responseHeaders = new LinkedHashMap<>();
    private final Map<String, Object> attributes = new LinkedHashMap<>();
    private final List<PluginTraceEntry> traceLog = new ArrayList<>();
    private Map<String, Object> currentPluginConfig = Map.of();

    private int responseStatus = 200;
    private String responseBody;
    private Map<String, Object> responseJson;
    private boolean requestBodyMutated = false;
    private boolean responseBodyMutated = false;

    private long settledActualMicros = 0L;
    private long settledInputTokens = 0L;
    private long settledOutputTokens = 0L;
    private String settledStatus = "succeeded";

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
        this.requestId = requestId != null ? requestId : "";
        this.projectId = projectId != null ? projectId : "";
        this.workspaceId = workspaceId != null ? workspaceId : "";
        this.keyId = keyId != null ? keyId : "";
        this.endpoint = endpoint != null ? endpoint : "/v1/chat/completions";
        this.modelAlias = modelAlias != null ? modelAlias : "";
        this.stream = stream;
        this.requestHeaders = requestHeaders != null ? new LinkedHashMap<>(requestHeaders) : new LinkedHashMap<>();
        this.requestJson = requestJson != null ? new LinkedHashMap<>(requestJson) : new LinkedHashMap<>();
    }

    public String requestId() { return requestId; }
    public String projectId() { return projectId; }
    public String workspaceId() { return workspaceId; }
    public String keyId() { return keyId; }
    public String endpoint() { return endpoint; }
    public String modelAlias() { return modelAlias; }
    public void setModelAlias(String modelAlias) {
        this.modelAlias = modelAlias;
        this.requestJson.put("model", modelAlias);
        this.requestBodyMutated = true;
    }
    public boolean stream() { return stream; }

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

    /**
     * Extracts concatenated text from request messages, prompt, or input fields.
     */
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

    /**
     * Applies a string transformation function across all prompt/message text fields in requestJson.
     */
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
