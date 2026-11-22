package com.jihuayu.aigateway.plugin;

/**
 * Isomorphic 5-Stage AI Gateway Plugin SPI (#36).
 *
 * <p>Implementations of this interface are 100% source- and binary-isomorphic across:
 * <ul>
 *   <li>Compile-time native plugins inlined into Oracle GraalVM 25 Native Image binaries</li>
 *   <li>Runtime dynamic {@code .jar} plugins hot-loaded in JVM mode via {@link DynamicJarPluginLoader}</li>
 *   <li>Runtime dynamic {@code .js} plugins hot-loaded via {@link DynamicJsPluginManager}</li>
 * </ul>
 */
public interface GatewayPlugin {

    /**
     * Globally unique identifier for this plugin implementation (e.g. "builtin:pii-guardrail" or UUID).
     */
    String id();

    /**
     * Human-readable plugin name.
     */
    default String name() {
        return id();
    }

    /**
     * Plugin tier type: BUILTIN, JAVA_SPI, JS, or WEBHOOK.
     */
    default String type() {
        return "JAVA_SPI";
    }

    /**
     * Default execution priority (lower numbers run earlier within the same stage).
     */
    default int priority() {
        return 100;
    }

    /**
     * Stage 1: Triggered immediately upon request reception before JSON body parsing or admission.
     */
    default PluginResult onRequestReceived(PluginExecutionContext ctx) {
        return PluginResult.Continue.INSTANCE;
    }

    /**
     * Stage 2: Triggered after JSON parsing and BEFORE the Redis Lua ADMIT script.
     * Rejections at this stage consume 0 financial budget, concurrency leases, or TPM quota.
     */
    default PluginResult onPreAdmit(PluginExecutionContext ctx) {
        return PluginResult.Continue.INSTANCE;
    }

    /**
     * Stage 3: Triggered AFTER Redis Lua ADMIT and BEFORE upstream DISPATCH.
     * Ideal for Exact and Semantic Response Caches: returning {@link PluginResult.ShortCircuit}
     * immediately settles the request at 0 micro-USD and refunds reserved budget and TPM tokens.
     */
    default PluginResult onPostAdmitPreDispatch(PluginExecutionContext ctx) {
        return PluginResult.Continue.INSTANCE;
    }

    /**
     * Stage 4a: Triggered right before sending the HTTP request to a selected upstream target.
     */
    default PluginResult onPreUpstream(PluginExecutionContext ctx) {
        return PluginResult.Continue.INSTANCE;
    }

    /**
     * Stage 4b: Triggered after receiving the upstream response body (output guardrails, PII masking, cache store).
     */
    default PluginResult onResponseContent(PluginExecutionContext ctx) {
        return PluginResult.Continue.INSTANCE;
    }

    /**
     * Stage 5: Triggered asynchronously/non-blockingly after Redis Lua SETTLE completes.
     */
    default void onPostSettle(PluginExecutionContext ctx) {
        // default no-op
    }

    default PluginResult executeStage(PluginStage stage, PluginExecutionContext ctx) {
        return switch (stage) {
            case REQUEST_RECEIVED -> onRequestReceived(ctx);
            case PRE_ADMIT -> onPreAdmit(ctx);
            case POST_ADMIT_PRE_DISPATCH -> onPostAdmitPreDispatch(ctx);
            case PRE_UPSTREAM -> onPreUpstream(ctx);
            case RESPONSE_CONTENT -> onResponseContent(ctx);
            case POST_SETTLE -> {
                onPostSettle(ctx);
                yield PluginResult.Continue.INSTANCE;
            }
        };
    }
}
