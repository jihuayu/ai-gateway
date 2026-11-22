package com.jihuayu.aigateway.plugin;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jihuayu.aigateway.hot.HotContract.PluginBindingSnapshot;
import com.jihuayu.aigateway.hot.HotContract.PluginSnapshot;
import com.jihuayu.aigateway.hot.HotContract.Snapshot;
import com.jihuayu.aigateway.plugin.JsPluginSandbox.CompiledJsPlugin;
import com.jihuayu.aigateway.plugin.JsPluginSandbox.ExecutionOutcome;
import com.jihuayu.aigateway.plugin.JsPluginSandbox.JsSandboxFuelExhaustedException;
import com.jihuayu.aigateway.plugin.MicrokernelAuthAndRateLimitPlugins.CustomExtractorAuthPlugin;
import com.jihuayu.aigateway.plugin.MicrokernelAuthAndRateLimitPlugins.EndUserRateLimitPlugin;
import com.jihuayu.aigateway.plugin.MicrokernelAuthAndRateLimitPlugins.HmacSignatureAuthPlugin;
import com.jihuayu.aigateway.plugin.MicrokernelAuthAndRateLimitPlugins.JwtOidcAuthPlugin;
import com.jihuayu.aigateway.plugin.MicrokernelAuthAndRateLimitPlugins.SpendVelocityCircuitPlugin;
import com.jihuayu.aigateway.plugin.MicrokernelBillingSinkPlugins.CostAttributionTagPlugin;
import com.jihuayu.aigateway.plugin.MicrokernelBillingSinkPlugins.RedisStreamBillingSinkPlugin;
import com.jihuayu.aigateway.hot.GatewayTelemetry;
import com.jihuayu.aigateway.plugin.MicrokernelBillingSinkPlugins.WebhookBillingSinkPlugin;
import com.jihuayu.aigateway.plugin.MicrokernelObservabilityPlugins.LangfuseLlmOpsSinkPlugin;
import com.jihuayu.aigateway.plugin.MicrokernelObservabilityPlugins.OpenTelemetryGenAiTracingPlugin;
import com.jihuayu.aigateway.plugin.MicrokernelObservabilityPlugins.PrometheusMetricsPlugin;
import com.jihuayu.aigateway.plugin.MicrokernelObservabilityPlugins.StructuredAccessLogPlugin;
import com.jihuayu.aigateway.plugin.MicrokernelRoutingAndPolicyPlugins.CanaryHeaderRouterPlugin;
import com.jihuayu.aigateway.plugin.MicrokernelRoutingAndPolicyPlugins.JevCalibratedGuardrailPlugin;
import com.jihuayu.aigateway.plugin.MicrokernelRoutingAndPolicyPlugins.JevSemanticRouterPlugin;
import com.jihuayu.aigateway.plugin.MicrokernelRoutingAndPolicyPlugins.LeastTtftRouterPlugin;
import com.jihuayu.aigateway.plugin.MicrokernelRoutingAndPolicyPlugins.McpToolArgumentPolicyPlugin;
import com.jihuayu.aigateway.plugin.MicrokernelRoutingAndPolicyPlugins.ModelCapabilityPolicyPlugin;
import com.jihuayu.aigateway.plugin.MicrokernelRoutingAndPolicyPlugins.PrefixCacheAffinityRouterPlugin;
import com.jihuayu.aigateway.plugin.MicrokernelRoutingAndPolicyPlugins.PromptDecoratorTrimmerPlugin;
import com.jihuayu.aigateway.plugin.MicrokernelRoutingAndPolicyPlugins.WeightedCircuitBreakerRouterPlugin;
import io.lettuce.core.api.StatefulRedisConnection;
import java.net.http.HttpClient;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 8-Stage Microkernel AI Gateway Plugin Pipeline & Online Dry-Run Debugger (#36, #41, #42, #37, #51, #52, #53, #54).
 */
public class PluginPipeline implements AutoCloseable {

    public record BoundPluginInvocation(
        GatewayPlugin plugin,
        PluginSnapshot metadata,
        Map<String, Object> mergedConfig,
        String failMode,
        int effectivePriority,
        double sampleRate
    ) {}

    private final StatefulRedisConnection<String, String> redisConn;
    private final String keyPrefix;
    private final DynamicJarPluginLoader jarLoader;
    private final DynamicJsPluginManager jsManager;

    private final PiiGuardrailPlugin piiGuardrailPlugin;
    private final ExactCachePlugin exactCachePlugin;
    private final SemanticCachePlugin semanticCachePlugin;
    private final ExtProcPlugin defaultExtProcPlugin;

    private final CustomExtractorAuthPlugin customExtractorAuthPlugin;
    private final HmacSignatureAuthPlugin hmacSignatureAuthPlugin;
    private final JwtOidcAuthPlugin jwtOidcAuthPlugin;
    private final EndUserRateLimitPlugin endUserRateLimitPlugin;
    private final SpendVelocityCircuitPlugin spendVelocityCircuitPlugin;

    private final WeightedCircuitBreakerRouterPlugin weightedCircuitBreakerRouterPlugin;
    private final LeastTtftRouterPlugin leastTtftRouterPlugin;
    private final PrefixCacheAffinityRouterPlugin prefixCacheAffinityRouterPlugin;
    private final CanaryHeaderRouterPlugin canaryHeaderRouterPlugin;
    private final JevSemanticRouterPlugin jevSemanticRouterPlugin;
    private final JevCalibratedGuardrailPlugin jevCalibratedGuardrailPlugin;
    private final ModelCapabilityPolicyPlugin modelCapabilityPolicyPlugin;
    private final PromptDecoratorTrimmerPlugin promptDecoratorTrimmerPlugin;
    private final McpToolArgumentPolicyPlugin mcpToolArgumentPolicyPlugin;

    private final CostAttributionTagPlugin costAttributionTagPlugin;
    private final WebhookBillingSinkPlugin webhookBillingSinkPlugin;
    private final RedisStreamBillingSinkPlugin redisStreamBillingSinkPlugin;
    private final PrometheusMetricsPlugin prometheusMetricsPlugin;
    private final OpenTelemetryGenAiTracingPlugin openTelemetryGenAiTracingPlugin;
    private final LangfuseLlmOpsSinkPlugin langfuseLlmOpsSinkPlugin;
    private final StructuredAccessLogPlugin structuredAccessLogPlugin;

    private final HttpClient httpClient;
    private final ObjectMapper mapper;

    public PluginPipeline(
        StatefulRedisConnection<String, String> redisConn,
        String keyPrefix,
        HttpClient httpClient,
        ObjectMapper mapper
    ) {
        this.redisConn = redisConn;
        this.keyPrefix = keyPrefix != null ? keyPrefix : "gw:";
        this.httpClient = httpClient;
        this.mapper = mapper != null ? mapper : new ObjectMapper();
        this.jarLoader = new DynamicJarPluginLoader();
        this.jsManager = new DynamicJsPluginManager(this.jarLoader);

        this.piiGuardrailPlugin = new PiiGuardrailPlugin();
        this.exactCachePlugin = new ExactCachePlugin(redisConn, this.keyPrefix, this.mapper);
        this.semanticCachePlugin = new SemanticCachePlugin();
        this.defaultExtProcPlugin = new ExtProcPlugin(ExtProcPlugin.ID, "ext-proc", 30, httpClient, this.mapper);

        this.customExtractorAuthPlugin = new CustomExtractorAuthPlugin();
        this.hmacSignatureAuthPlugin = new HmacSignatureAuthPlugin();
        this.jwtOidcAuthPlugin = new JwtOidcAuthPlugin();
        this.endUserRateLimitPlugin = new EndUserRateLimitPlugin();
        this.spendVelocityCircuitPlugin = new SpendVelocityCircuitPlugin();

        this.weightedCircuitBreakerRouterPlugin = new WeightedCircuitBreakerRouterPlugin();
        this.leastTtftRouterPlugin = new LeastTtftRouterPlugin();
        this.prefixCacheAffinityRouterPlugin = new PrefixCacheAffinityRouterPlugin();
        this.canaryHeaderRouterPlugin = new CanaryHeaderRouterPlugin();
        this.jevSemanticRouterPlugin = new JevSemanticRouterPlugin();
        this.jevCalibratedGuardrailPlugin = new JevCalibratedGuardrailPlugin();
        this.modelCapabilityPolicyPlugin = new ModelCapabilityPolicyPlugin();
        this.promptDecoratorTrimmerPlugin = new PromptDecoratorTrimmerPlugin();
        this.mcpToolArgumentPolicyPlugin = new McpToolArgumentPolicyPlugin();

        this.costAttributionTagPlugin = new CostAttributionTagPlugin();
        this.webhookBillingSinkPlugin = new WebhookBillingSinkPlugin(httpClient);
        this.redisStreamBillingSinkPlugin = new RedisStreamBillingSinkPlugin(redisConn, this.keyPrefix);
        this.prometheusMetricsPlugin = new PrometheusMetricsPlugin();
        this.openTelemetryGenAiTracingPlugin = new OpenTelemetryGenAiTracingPlugin(httpClient, this.mapper);
        this.langfuseLlmOpsSinkPlugin = new LangfuseLlmOpsSinkPlugin(httpClient, this.mapper);
        this.structuredAccessLogPlugin = new StructuredAccessLogPlugin(httpClient, this.mapper);

        for (GatewayPlugin builtIn : List.of(
            piiGuardrailPlugin, exactCachePlugin, semanticCachePlugin, defaultExtProcPlugin,
            customExtractorAuthPlugin, hmacSignatureAuthPlugin, jwtOidcAuthPlugin,
            endUserRateLimitPlugin, spendVelocityCircuitPlugin,
            weightedCircuitBreakerRouterPlugin, leastTtftRouterPlugin, prefixCacheAffinityRouterPlugin,
            canaryHeaderRouterPlugin, jevSemanticRouterPlugin, jevCalibratedGuardrailPlugin,
            modelCapabilityPolicyPlugin, promptDecoratorTrimmerPlugin, mcpToolArgumentPolicyPlugin,
            costAttributionTagPlugin, webhookBillingSinkPlugin, redisStreamBillingSinkPlugin,
            prometheusMetricsPlugin, openTelemetryGenAiTracingPlugin, langfuseLlmOpsSinkPlugin, structuredAccessLogPlugin
        )) {
            this.jarLoader.registerPlugin(builtIn);
        }
        this.jarLoader.registerAlias("builtin:router-peak-ewma-hedge", leastTtftRouterPlugin);
        this.jarLoader.registerAlias("router-peak-ewma-hedge", leastTtftRouterPlugin);

        String pluginsDirEnv = System.getenv("PLUGINS_DIR");
        if (pluginsDirEnv != null && !pluginsDirEnv.isBlank()) {
            this.jsManager.startDirectoryWatcher(Path.of(pluginsDirEnv.trim()), 150L);
        }
    }

    public PluginExecutionContext.PluginKvStore createKvStore() {
        return new PluginExecutionContext.PluginKvStore(redisConn, keyPrefix);
    }

    public DynamicJarPluginLoader jarLoader() { return jarLoader; }
    public DynamicJsPluginManager jsManager() { return jsManager; }
    public PiiGuardrailPlugin piiGuardrailPlugin() { return piiGuardrailPlugin; }
    public ExactCachePlugin exactCachePlugin() { return exactCachePlugin; }
    public SemanticCachePlugin semanticCachePlugin() { return semanticCachePlugin; }
    public LeastTtftRouterPlugin leastTtftRouterPlugin() { return leastTtftRouterPlugin; }
    public WebhookBillingSinkPlugin webhookBillingSinkPlugin() { return webhookBillingSinkPlugin; }

    public List<BoundPluginInvocation> resolveStagePlugins(
        Snapshot snapshot,
        PluginStage stage,
        String workspaceId,
        String projectId,
        String aliasName,
        String keyId
    ) {
        List<BoundPluginInvocation> invocations = new ArrayList<>();

        String hookName = switch (stage) {
            case AUTHENTICATE -> "onAuthenticate";
            case REQUEST_RECEIVED -> "onRequestReceived";
            case RATE_LIMIT -> "onRateLimit";
            case PRE_ADMIT -> "onPreAdmit";
            case POST_ADMIT_PRE_DISPATCH -> "onPostAdmitPreDispatch";
            case ROUTE_SELECT -> "onRouteSelect";
            case PRE_UPSTREAM -> "onPreUpstream";
            case STREAM_CHUNK -> "onStreamChunk";
            case RESPONSE_CONTENT -> "onResponseContent";
            case POST_SETTLE -> "onPostSettle";
        };
        for (CompiledJsPlugin dirJs : jsManager.activeDirectoryJsPlugins().values()) {
            if (dirJs.hasHook(hookName)) {
                invocations.add(new BoundPluginInvocation(dirJs, null, Map.of(), "FAIL_CLOSED", dirJs.priority(), 1.0));
            }
        }

        if (snapshot != null && snapshot.plugins() != null && !snapshot.plugins().isEmpty()) {
            Map<String, PluginSnapshot> pluginsById = new LinkedHashMap<>();
            for (PluginSnapshot ps : snapshot.plugins()) {
                if (ps != null && ps.enabled()) {
                    pluginsById.put(ps.id(), ps);
                }
            }

            List<PluginBindingSnapshot> bindings = snapshot.pluginBindings() != null ? snapshot.pluginBindings() : List.of();
            for (PluginBindingSnapshot b : bindings) {
                if (b == null || !b.enabled()) continue;
                PluginSnapshot ps = pluginsById.get(b.pluginId());
                if (ps == null || !ps.enabled()) continue;
                if (!matchesStage(ps.stages(), stage)) continue;
                if (!matchesScope(b.scopeType(), b.scopeId(), workspaceId, projectId, aliasName, keyId)) continue;

                GatewayPlugin instance = resolvePluginInstance(ps);
                if (instance == null) continue;

                Map<String, Object> mergedCfg = new LinkedHashMap<>();
                if (ps.config() != null) mergedCfg.putAll(ps.config());
                if (b.configOverride() != null) mergedCfg.putAll(b.configOverride());

                int prio = b.priority() > 0 ? b.priority() : ps.priority();
                String failMode = ps.failMode() != null ? ps.failMode() : "FAIL_CLOSED";
                double sampleRate = b.sampleRate() != null ? b.sampleRate() : 1.0;
                invocations.add(new BoundPluginInvocation(instance, ps, mergedCfg, failMode, prio, sampleRate));
            }
        }

        invocations.sort(Comparator.comparingInt(BoundPluginInvocation::effectivePriority));
        return invocations;
    }

    private boolean matchesStage(List<String> configuredStages, PluginStage stage) {
        if (configuredStages == null || configuredStages.isEmpty()) {
            return true;
        }
        for (String s : configuredStages) {
            if ("*".equals(s) || "ALL".equalsIgnoreCase(s)) return true;
            try {
                if (PluginStage.fromString(s) == stage) return true;
            } catch (Exception ignored) {}
        }
        return false;
    }

    private boolean matchesScope(
        String scopeType,
        String scopeId,
        String workspaceId,
        String projectId,
        String aliasName,
        String keyId
    ) {
        if (scopeType == null || "GLOBAL".equalsIgnoreCase(scopeType) || "*".equals(scopeId)) {
            return true;
        }
        return switch (scopeType.toUpperCase()) {
            case "WORKSPACE" -> workspaceId == null || workspaceId.isBlank() || workspaceId.equals(scopeId);
            case "PROJECT" -> projectId != null && projectId.equals(scopeId);
            case "ALIAS" -> aliasName != null && aliasName.equals(scopeId);
            case "API_KEY" -> keyId != null && keyId.equals(scopeId);
            default -> false;
        };
    }

    private GatewayPlugin resolvePluginInstance(PluginSnapshot ps) {
        if (ps == null) return null;
        String type = ps.type() != null ? ps.type().toUpperCase() : "BUILTIN";
        return switch (type) {
            case "JS" -> jsManager.getOrCompileSnapshotPlugin(ps);
            case "WEBHOOK" -> new ExtProcPlugin(ps.id(), ps.name(), ps.priority(), httpClient, mapper);
            case "BUILTIN", "JAVA_SPI" -> {
                if (ps.className() != null && !ps.className().isBlank()) {
                    GatewayPlugin byClass = jarLoader.getPlugin(ps.className());
                    if (byClass != null) yield byClass;
                }
                GatewayPlugin byName = jarLoader.getPlugin(ps.name());
                if (byName != null) yield byName;
                yield jarLoader.getPlugin(ps.id());
            }
            default -> jarLoader.getPlugin(ps.name());
        };
    }

    public PluginResult executeStage(
        Snapshot snapshot,
        PluginStage stage,
        PluginExecutionContext ctx
    ) {
        List<BoundPluginInvocation> bound = resolveStagePlugins(
            snapshot,
            stage,
            ctx.workspaceId(),
            ctx.projectId(),
            ctx.modelAlias(),
            ctx.keyId()
        );
        if (bound.isEmpty()) {
            return PluginResult.Continue.INSTANCE;
        }

        for (BoundPluginInvocation inv : bound) {
            if (inv.sampleRate() <= 0.0) continue;
            if (inv.sampleRate() < 1.0) {
                int bucket = Math.floorMod((ctx.requestId() + ":" + inv.plugin().id()).hashCode(), 10_000);
                if (bucket >= (int) Math.round(inv.sampleRate() * 10_000)) {
                    continue;
                }
            }
            PluginResult res = invokeSingle(inv.plugin(), stage, ctx, inv.mergedConfig(), inv.failMode());
            if (!(res instanceof PluginResult.Continue)) {
                return res;
            }
        }
        return PluginResult.Continue.INSTANCE;
    }

    public PluginResult invokeSingle(
        GatewayPlugin plugin,
        PluginStage stage,
        PluginExecutionContext ctx,
        Map<String, Object> config,
        String failMode
    ) {
        long startNanos = System.nanoTime();
        long statements = 0L;
        ctx.setPluginConfig(config);
        ctx.kv().setActivePluginNamespace(plugin.id());
        try {
            PluginResult result;
            if (plugin instanceof CompiledJsPlugin jsPlugin) {
                ExecutionOutcome outcome = jsPlugin.executeWithMetrics(stage, ctx);
                result = outcome.result();
                statements = outcome.executedStatements();
            } else {
                result = plugin.executeStage(stage, ctx);
            }
            if (result == null) {
                result = PluginResult.Continue.INSTANCE;
            }
            long durationMicros = Math.max(1L, (System.nanoTime() - startNanos) / 1000L);
            String outcomeName = switch (result) {
                case PluginResult.Continue _ -> "CONTINUE";
                case PluginResult.Reject r -> "REJECT:" + r.httpStatus() + ":" + r.errorCode();
                case PluginResult.ShortCircuit sc -> "SHORT_CIRCUIT:" + sc.httpStatus();
            };
            ctx.recordTrace(plugin.id(), plugin.name(), plugin.type(), stage, outcomeName, durationMicros, statements, "OK");
            GatewayTelemetry.global().recordPluginExecution(plugin.name(), stage.name(), outcomeName);
            return result;
        } catch (JsSandboxFuelExhaustedException fuelEx) {
            long durationMicros = Math.max(1L, (System.nanoTime() - startNanos) / 1000L);
            ctx.recordTrace(
                plugin.id(),
                plugin.name(),
                plugin.type(),
                stage,
                "FUEL_EXHAUSTED",
                durationMicros,
                fuelEx.executedStatements(),
                fuelEx.getMessage()
            );
            if ("FAIL_OPEN".equalsIgnoreCase(failMode)) {
                ctx.setResponseHeader("x-gateway-plugin-fallback", "fail_open");
                return PluginResult.Continue.INSTANCE;
            }
            return PluginResult.reject(400, "js_plugin_fuel_exhausted", fuelEx.getMessage());
        } catch (Exception ex) {
            long durationMicros = Math.max(1L, (System.nanoTime() - startNanos) / 1000L);
            ctx.recordTrace(plugin.id(), plugin.name(), plugin.type(), stage, "ERROR", durationMicros, statements, ex.getMessage());
            if ("FAIL_OPEN".equalsIgnoreCase(failMode)) {
                ctx.setResponseHeader("x-gateway-plugin-fallback", "fail_open");
                return PluginResult.Continue.INSTANCE;
            }
            return PluginResult.reject(500, "plugin_execution_failed", "Plugin '" + plugin.name() + "' failed: " + ex.getMessage());
        }
    }

    @Override
    public void close() {
        jsManager.close();
    }
}
