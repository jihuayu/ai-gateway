package com.jihuayu.aigateway.plugin;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jihuayu.aigateway.hot.HotContract.PluginBindingSnapshot;
import com.jihuayu.aigateway.hot.HotContract.PluginSnapshot;
import com.jihuayu.aigateway.hot.HotContract.Snapshot;
import com.jihuayu.aigateway.plugin.JsPluginSandbox.CompiledJsPlugin;
import com.jihuayu.aigateway.plugin.JsPluginSandbox.ExecutionOutcome;
import com.jihuayu.aigateway.plugin.JsPluginSandbox.JsSandboxFuelExhaustedException;
import io.lettuce.core.api.StatefulRedisConnection;
import java.net.http.HttpClient;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 5-Stage Unified AI Gateway Plugin Pipeline & Online Dry-Run Debugger (#36, #41, #42, #37).
 *
 * <p>Coordinates:
 * <ul>
 *   <li>Built-in native plugins ({@link PiiGuardrailPlugin}, {@link ExactCachePlugin}, {@link SemanticCachePlugin}, {@link ExtProcPlugin})</li>
 *   <li>Compile-time {@link java.util.ServiceLoader} &amp; JVM-mode dynamic {@code .jar} plugins ({@link DynamicJarPluginLoader})</li>
 *   <li>Runtime dynamic {@code .js} plugins via Control Plane Redis Snapshot &amp; local {@code PLUGINS_DIR/*.js} hot watcher ({@link DynamicJsPluginManager})</li>
 * </ul>
 */
public class PluginPipeline implements AutoCloseable {

    public record BoundPluginInvocation(
        GatewayPlugin plugin,
        PluginSnapshot metadata,
        Map<String, Object> mergedConfig,
        String failMode,
        int effectivePriority
    ) {}

    private final DynamicJarPluginLoader jarLoader;
    private final DynamicJsPluginManager jsManager;
    private final PiiGuardrailPlugin piiGuardrailPlugin;
    private final ExactCachePlugin exactCachePlugin;
    private final SemanticCachePlugin semanticCachePlugin;
    private final ExtProcPlugin defaultExtProcPlugin;
    private final HttpClient httpClient;
    private final ObjectMapper mapper;

    public PluginPipeline(
        StatefulRedisConnection<String, String> redisConn,
        String keyPrefix,
        HttpClient httpClient,
        ObjectMapper mapper
    ) {
        this.httpClient = httpClient;
        this.mapper = mapper != null ? mapper : new ObjectMapper();
        this.jarLoader = new DynamicJarPluginLoader();
        this.jsManager = new DynamicJsPluginManager(this.jarLoader);

        this.piiGuardrailPlugin = new PiiGuardrailPlugin();
        this.exactCachePlugin = new ExactCachePlugin(redisConn, keyPrefix, this.mapper);
        this.semanticCachePlugin = new SemanticCachePlugin();
        this.defaultExtProcPlugin = new ExtProcPlugin(ExtProcPlugin.ID, "ext-proc", 30, httpClient, this.mapper);

        this.jarLoader.registerPlugin(piiGuardrailPlugin);
        this.jarLoader.registerPlugin(exactCachePlugin);
        this.jarLoader.registerPlugin(semanticCachePlugin);
        this.jarLoader.registerPlugin(defaultExtProcPlugin);

        String pluginsDirEnv = System.getenv("PLUGINS_DIR");
        if (pluginsDirEnv != null && !pluginsDirEnv.isBlank()) {
            this.jsManager.startDirectoryWatcher(Path.of(pluginsDirEnv.trim()), 150L);
        }
    }

    public DynamicJarPluginLoader jarLoader() { return jarLoader; }
    public DynamicJsPluginManager jsManager() { return jsManager; }
    public PiiGuardrailPlugin piiGuardrailPlugin() { return piiGuardrailPlugin; }
    public ExactCachePlugin exactCachePlugin() { return exactCachePlugin; }
    public SemanticCachePlugin semanticCachePlugin() { return semanticCachePlugin; }

    /**
     * Resolves all active bound plugins for a request in a specific lifecycle stage, sorted by priority.
     */
    public List<BoundPluginInvocation> resolveStagePlugins(
        Snapshot snapshot,
        PluginStage stage,
        String workspaceId,
        String projectId,
        String aliasName,
        String keyId
    ) {
        List<BoundPluginInvocation> invocations = new ArrayList<>();

        // 1. Always include any hot-loaded local directory .js plugins (Channel B) that implement this stage
        String hookName = switch (stage) {
            case REQUEST_RECEIVED -> "onRequestReceived";
            case PRE_ADMIT -> "onPreAdmit";
            case POST_ADMIT_PRE_DISPATCH -> "onPostAdmitPreDispatch";
            case PRE_UPSTREAM -> "onPreUpstream";
            case RESPONSE_CONTENT -> "onResponseContent";
            case POST_SETTLE -> "onPostSettle";
        };
        for (CompiledJsPlugin dirJs : jsManager.activeDirectoryJsPlugins().values()) {
            if (dirJs.hasHook(hookName)) {
                invocations.add(new BoundPluginInvocation(dirJs, null, Map.of(), "FAIL_CLOSED", dirJs.priority()));
            }
        }

        // 2. Include Control Plane / Redis Snapshot bound plugins (Channel A)
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
                invocations.add(new BoundPluginInvocation(instance, ps, mergedCfg, failMode, prio));
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
            case "WORKSPACE" -> workspaceId != null && workspaceId.equals(scopeId);
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

    /**
     * Executes all matching plugins for the given lifecycle stage in priority order.
     */
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
