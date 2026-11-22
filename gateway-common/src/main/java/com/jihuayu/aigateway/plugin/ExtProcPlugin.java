package com.jihuayu.aigateway.plugin;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * External Processor Webhook Plugin ({@code ext_proc} / {@code WEBHOOK}) (#42).
 *
 * <p>Sends a structured JSON callout to a configured external HTTP endpoint at any plugin stage
 * with strict timeout enforcement ({@code timeoutMs}) and configurable {@code FAIL_OPEN} / {@code FAIL_CLOSED}
 * resilience.
 */
public class ExtProcPlugin implements GatewayPlugin {

    public static final String ID = "builtin:ext-proc";

    private final String id;
    private final String name;
    private final int priority;
    private final HttpClient httpClient;
    private final ObjectMapper mapper;

    public ExtProcPlugin(String id, String name, int priority, HttpClient httpClient, ObjectMapper mapper) {
        this.id = id != null ? id : ID;
        this.name = name != null ? name : "ext-proc";
        this.priority = priority;
        this.httpClient = httpClient != null
            ? httpClient
            : HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build();
        this.mapper = mapper != null ? mapper : new ObjectMapper();
    }

    @Override
    public String id() { return id; }

    @Override
    public String name() { return name; }

    @Override
    public String type() { return "WEBHOOK"; }

    @Override
    public int priority() { return priority; }

    @Override
    public PluginResult executeStage(PluginStage stage, PluginExecutionContext ctx) {
        Map<String, Object> cfg = ctx.pluginConfig();
        Object urlObj = cfg.get("webhookUrl");
        if (urlObj == null) urlObj = cfg.get("url");
        if (urlObj == null || String.valueOf(urlObj).isBlank()) {
            return PluginResult.Continue.INSTANCE;
        }
        String webhookUrl = String.valueOf(urlObj).trim();
        long timeoutMs = 500L;
        if (cfg.get("timeoutMs") instanceof Number n && n.longValue() > 0) {
            timeoutMs = n.longValue();
        }
        String failMode = cfg.get("failMode") != null ? String.valueOf(cfg.get("failMode")).toUpperCase() : "FAIL_OPEN";

        try {
            Map<String, Object> payload = new LinkedHashMap<>();
            payload.put("stage", stage.name());
            payload.put("requestId", ctx.requestId());
            payload.put("projectId", ctx.projectId());
            payload.put("keyId", ctx.keyId());
            payload.put("endpoint", ctx.endpoint());
            payload.put("model", ctx.modelAlias());
            payload.put("headers", ctx.requestHeaders());
            payload.put("body", ctx.requestJson());
            if (ctx.responseBody() != null) {
                payload.put("responseBody", ctx.responseBody());
                payload.put("responseStatus", ctx.responseStatus());
            }

            HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create(webhookUrl))
                .timeout(Duration.ofMillis(timeoutMs))
                .header("Content-Type", "application/json")
                .header("User-Agent", "Mozilla/5.0 (compatible; Google-Agent; +https://developers.google.com/crawling/docs/crawlers-fetchers/google-agent)")
                .POST(HttpRequest.BodyPublishers.ofString(mapper.writeValueAsString(payload)))
                .build();

            HttpResponse<String> resp = httpClient.send(req, HttpResponse.BodyHandlers.ofString());
            if (resp.statusCode() >= 500) {
                return handleFailure(ctx, failMode, "ext_proc returned HTTP " + resp.statusCode());
            }
            String respBody = resp.body();
            if (respBody == null || respBody.isBlank()) {
                return PluginResult.Continue.INSTANCE;
            }
            Map<String, Object> decision = mapper.readValue(respBody, new TypeReference<Map<String, Object>>() {});
            applyMutations(ctx, decision);

            String action = decision.get("action") != null ? String.valueOf(decision.get("action")).toUpperCase() : "CONTINUE";
            if ("REJECT".equals(action)) {
                int status = decision.get("status") instanceof Number n ? n.intValue() : 403;
                String code = decision.get("code") != null ? String.valueOf(decision.get("code")) : "ext_proc_rejected";
                String msg = decision.get("message") != null ? String.valueOf(decision.get("message")) : "Rejected by external processor";
                return PluginResult.reject(status, code, msg);
            }
            if ("SHORT_CIRCUIT".equals(action) || "SHORTCIRCUIT".equals(action)) {
                int status = decision.get("status") instanceof Number n ? n.intValue() : 200;
                Object scBody = decision.get("body");
                String scBodyStr = (scBody instanceof String s) ? s : mapper.writeValueAsString(scBody != null ? scBody : Map.of());
                return PluginResult.shortCircuit(status, scBodyStr, Map.of("x-gateway-extproc", "SHORT_CIRCUIT"));
            }
            return PluginResult.Continue.INSTANCE;
        } catch (Exception e) {
            return handleFailure(ctx, failMode, e.getMessage());
        }
    }

    @SuppressWarnings("unchecked")
    private void applyMutations(PluginExecutionContext ctx, Map<String, Object> decision) {
        if (decision.get("requestHeaders") instanceof Map<?, ?> rh) {
            for (Map.Entry<?, ?> e : rh.entrySet()) {
                if (e.getKey() != null && e.getValue() != null) {
                    ctx.setRequestHeader(String.valueOf(e.getKey()), String.valueOf(e.getValue()));
                }
            }
        }
        if (decision.get("responseHeaders") instanceof Map<?, ?> respH) {
            for (Map.Entry<?, ?> e : respH.entrySet()) {
                if (e.getKey() != null && e.getValue() != null) {
                    ctx.setResponseHeader(String.valueOf(e.getKey()), String.valueOf(e.getValue()));
                }
            }
        }
        if (decision.get("mutatedBody") instanceof Map<?, ?> mb) {
            ctx.requestJson().clear();
            ctx.requestJson().putAll((Map<String, Object>) mb);
            ctx.markRequestBodyMutated();
        }
        if (decision.get("mutatedPrompt") instanceof String mp) {
            ctx.transformPromptText(_ -> mp);
        }
    }

    private PluginResult handleFailure(PluginExecutionContext ctx, String failMode, String reason) {
        if ("FAIL_CLOSED".equals(failMode)) {
            return PluginResult.reject(502, "ext_proc_failed", "External processor webhook failed: " + reason);
        }
        ctx.setResponseHeader("x-gateway-extproc-fallback", "fail_open");
        return PluginResult.Continue.INSTANCE;
    }
}
