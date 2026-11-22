package com.jihuayu.aigateway.plugin;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Built-in Content Safety Guardrails & PII Redaction Plugin (#37).
 *
 * <p>Operates at:
 * <ul>
 *   <li><b>Stage 2 ({@code onPreAdmit})</b>: Inspects/masks prompt messages BEFORE Redis Lua ADMIT.
 *       If {@code mode = "BLOCK"} and a violation is detected, rejects immediately with {@code 400 guardrail_blocked}
 *       consuming 0 financial budget, concurrency lease, or TPM quota.</li>
 *   <li><b>Stage 4b ({@code onResponseContent})</b>: Inspects/masks upstream LLM response content before returning to caller.</li>
 * </ul>
 */
public class PiiGuardrailPlugin implements GatewayPlugin {

    public static final String ID = "builtin:pii-guardrail";

    private static final Pattern EMAIL_PATTERN =
        Pattern.compile("\\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}\\b");
    private static final Pattern CN_PHONE_PATTERN =
        Pattern.compile("(?<!\\d)(?:\\+?86[-\\s]?)?1[3-9]\\d{9}(?!\\d)");
    private static final Pattern SSN_PATTERN =
        Pattern.compile("\\b\\d{3}-\\d{2}-\\d{4}\\b");
    private static final Pattern CN_ID_CARD_PATTERN =
        Pattern.compile("(?<!\\d)[1-9]\\d{5}(?:19|20)\\d{2}(?:0[1-9]|1[0-2])(?:0[1-9]|[12]\\d|3[01])\\d{3}[0-9Xx](?!\\d)");
    private static final Pattern SECRET_KEY_PATTERN =
        Pattern.compile("\\b(?:sk-[A-Za-z0-9_-]{16,}|gh[pousr]_[A-Za-z0-9]{16,}|AKIA[0-9A-Z]{16})\\b");
    private static final Pattern PROMPT_INJECTION_PATTERN =
        Pattern.compile("(?i)(?:ignore\\s+all\\s+previous\\s+instructions|ignore\\s+previous\\s+instructions|disregard\\s+system\\s+prompt|jailbreak\\s+mode\\s+enabled)");

    @Override
    public String id() {
        return ID;
    }

    @Override
    public String name() {
        return "pii-guardrail";
    }

    @Override
    public String type() {
        return "BUILTIN";
    }

    @Override
    public int priority() {
        return 10;
    }

    @Override
    public PluginResult onPreAdmit(PluginExecutionContext ctx) {
        Map<String, Object> cfg = ctx.pluginConfig();
        String mode = cfg.get("mode") != null ? String.valueOf(cfg.get("mode")).toUpperCase() : "MASK";
        boolean blockPromptInjection = !Boolean.FALSE.equals(cfg.get("blockPromptInjection"));
        String promptText = ctx.extractPromptText();
        if (promptText == null || promptText.isEmpty()) {
            return PluginResult.Continue.INSTANCE;
        }

        List<String> findings = detectFindings(promptText, cfg, blockPromptInjection);
        if (findings.isEmpty()) {
            return PluginResult.Continue.INSTANCE;
        }

        ctx.attributes().put("guardrail.input.findings", findings);
        if (findings.contains("PROMPT_INJECTION") || "BLOCK".equals(mode)) {
            return PluginResult.reject(
                400,
                "guardrail_blocked",
                "Request blocked by content safety guardrail: " + String.join(", ", findings),
                Map.of("x-gateway-guardrail", "BLOCKED", "x-gateway-guardrail-findings", String.join(",", findings))
            );
        }

        if ("MASK".equals(mode)) {
            ctx.transformPromptText(text -> redactText(text, cfg));
            ctx.setResponseHeader("x-gateway-guardrail", "MASKED");
            ctx.setResponseHeader("x-gateway-guardrail-findings", String.join(",", findings));
        } else {
            // AUDIT_ONLY
            ctx.setResponseHeader("x-gateway-guardrail", "FLAGGED");
            ctx.setResponseHeader("x-gateway-guardrail-findings", String.join(",", findings));
        }
        return PluginResult.Continue.INSTANCE;
    }

    @Override
    public PluginResult onStreamChunk(PluginExecutionContext ctx) {
        String chunk = ctx.streamChunk();
        if (chunk == null || chunk.isEmpty()) {
            return PluginResult.Continue.INSTANCE;
        }
        Map<String, Object> cfg = ctx.pluginConfig();
        String mode = cfg.get("mode") != null ? String.valueOf(cfg.get("mode")).toUpperCase() : "MASK";
        List<String> findings = detectFindings(chunk, cfg, false);
        if (findings.isEmpty()) {
            return PluginResult.Continue.INSTANCE;
        }
        if ("BLOCK".equals(mode) && Boolean.TRUE.equals(cfg.get("blockOnOutputPii"))) {
            return PluginResult.reject(
                400,
                "guardrail_stream_blocked",
                "Streaming output blocked by guardrail: " + String.join(", ", findings)
            );
        }
        if ("MASK".equals(mode)) {
            String redacted = redactText(chunk, cfg);
            if (!redacted.equals(chunk)) {
                ctx.setStreamChunk(redacted);
                ctx.setResponseHeader("x-gateway-guardrail", "MASKED");
            }
        }
        return PluginResult.Continue.INSTANCE;
    }

    @Override
    public PluginResult onResponseContent(PluginExecutionContext ctx) {
        String body = ctx.responseBody();
        if (body == null || body.isEmpty()) {
            return PluginResult.Continue.INSTANCE;
        }
        Map<String, Object> cfg = ctx.pluginConfig();
        if (Boolean.FALSE.equals(cfg.get("inspectResponse"))) {
            return PluginResult.Continue.INSTANCE;
        }
        String mode = cfg.get("mode") != null ? String.valueOf(cfg.get("mode")).toUpperCase() : "MASK";
        List<String> findings = detectFindings(body, cfg, false);
        if (findings.isEmpty()) {
            return PluginResult.Continue.INSTANCE;
        }
        ctx.attributes().put("guardrail.output.findings", findings);
        if ("BLOCK".equals(mode) && Boolean.TRUE.equals(cfg.get("blockOnOutputPii"))) {
            return PluginResult.reject(
                400,
                "guardrail_output_blocked",
                "Response blocked by output content safety guardrail: " + String.join(", ", findings),
                Map.of("x-gateway-guardrail", "OUTPUT_BLOCKED")
            );
        }
        if ("MASK".equals(mode)) {
            String masked = redactText(body, cfg);
            if (!masked.equals(body)) {
                ctx.setResponseBody(masked);
                ctx.setResponseHeader("x-gateway-guardrail", "MASKED");
            }
        } else {
            ctx.setResponseHeader("x-gateway-guardrail", "FLAGGED");
        }
        return PluginResult.Continue.INSTANCE;
    }

    private static List<String> detectFindings(String text, Map<String, Object> cfg, boolean checkInjection) {
        List<String> findings = new ArrayList<>();
        if (checkInjection && PROMPT_INJECTION_PATTERN.matcher(text).find()) {
            findings.add("PROMPT_INJECTION");
        }
        if (SECRET_KEY_PATTERN.matcher(text).find()) {
            findings.add("SECRET_KEY");
        }
        if (EMAIL_PATTERN.matcher(text).find()) {
            findings.add("EMAIL");
        }
        if (CN_PHONE_PATTERN.matcher(text).find()) {
            findings.add("PHONE");
        }
        if (SSN_PATTERN.matcher(text).find() || CN_ID_CARD_PATTERN.matcher(text).find()) {
            findings.add("ID_NUMBER");
        }
        Object blockedWords = cfg.get("blockedKeywords");
        if (blockedWords instanceof List<?> words) {
            String lower = text.toLowerCase();
            for (Object w : words) {
                if (w != null && !String.valueOf(w).isBlank() && lower.contains(String.valueOf(w).toLowerCase())) {
                    findings.add("BLOCKED_KEYWORD:" + w);
                }
            }
        }
        return findings;
    }

    public static String redactText(String text, Map<String, Object> cfg) {
        if (text == null || text.isEmpty()) return text;
        String out = SECRET_KEY_PATTERN.matcher(text).replaceAll("[REDACTED_SECRET]");
        out = EMAIL_PATTERN.matcher(out).replaceAll("[REDACTED_EMAIL]");
        out = CN_ID_CARD_PATTERN.matcher(out).replaceAll("[REDACTED_ID]");
        out = SSN_PATTERN.matcher(out).replaceAll("[REDACTED_SSN]");
        out = CN_PHONE_PATTERN.matcher(out).replaceAll("[REDACTED_PHONE]");
        Object blockedWords = cfg != null ? cfg.get("blockedKeywords") : null;
        if (blockedWords instanceof List<?> words) {
            for (Object w : words) {
                if (w != null && !String.valueOf(w).isBlank()) {
                    out = out.replace(String.valueOf(w), "[REDACTED_KEYWORD]");
                }
            }
        }
        return out;
    }
}
