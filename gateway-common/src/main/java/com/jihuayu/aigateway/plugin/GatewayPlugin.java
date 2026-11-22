package com.jihuayu.aigateway.plugin;

/**
 * Isomorphic 8-Stage Microkernel AI Gateway Plugin SPI (#36, #51, #52).
 *
 * <p>Implementations of this interface are 100% source- and binary-isomorphic across:
 * <ul>
 *   <li>Compile-time native plugins inlined into Oracle GraalVM 25 Native Image binaries</li>
 *   <li>Runtime dynamic {@code .jar} plugins hot-loaded in JVM mode via {@link DynamicJarPluginLoader}</li>
 *   <li>Runtime dynamic {@code .js} plugins hot-loaded via {@link DynamicJsPluginManager}</li>
 * </ul>
 * Both Java and JavaScript plugins receive the exact same {@link PluginExecutionContext} ({@code ctx})
 * with first-class {@code ctx.kv()}, {@code ctx.crypto()}, {@code ctx.bindAuthenticatedKey(...)},
 * {@code ctx.candidateTargets()} / {@code ctx.setCandidateTargets(...)}, {@code ctx.streamChunk()},
 * {@code ctx.attributionTags()}, and {@code ctx.jevDecide(...)}.
 */
public interface GatewayPlugin {

    String id();

    default String name() {
        return id();
    }

    default String type() {
        return "JAVA_SPI";
    }

    default int priority() {
        return 100;
    }

    /**
     * Stage 1a: Custom authentication & credential extraction (Bearer, Custom Header, Query, Cookie, HMAC, JWT).
     * Plugins can call {@code ctx.bindAuthenticatedKey(apiKeyPlain)} or {@code ctx.bindKeyContext(keyContext)}.
     */
    default PluginResult onAuthenticate(PluginExecutionContext ctx) {
        return PluginResult.Continue.INSTANCE;
    }

    /**
     * Stage 1b: Triggered immediately upon request reception after authentication.
     */
    default PluginResult onRequestReceived(PluginExecutionContext ctx) {
        return PluginResult.Continue.INSTANCE;
    }

    /**
     * Stage 2: Multi-dimensional rate limiting (end-user, tenant, IP) & spend-velocity circuit breaking.
     * Rejections at this stage consume 0 Redis Lua financial budget or concurrency leases.
     */
    default PluginResult onRateLimit(PluginExecutionContext ctx) {
        return PluginResult.Continue.INSTANCE;
    }

    /**
     * Stage 3: Pre-admission system policies (model allowlists, capabilities, parameter bounds),
     * Prompt decoration, sliding-window context trimming, PII guardrails, and semantic model alias routing.
     */
    default PluginResult onPreAdmit(PluginExecutionContext ctx) {
        return PluginResult.Continue.INSTANCE;
    }

    /**
     * Stage 4: Post-admit, pre-dispatch cache lookup (Exact & Semantic Cache, supporting both buffered JSON
     * and streaming SSE replay). Returning {@link PluginResult.ShortCircuit} settles at 0 micro-USD.
     */
    default PluginResult onPostAdmitPreDispatch(PluginExecutionContext ctx) {
        return PluginResult.Continue.INSTANCE;
    }

    /**
     * Stage 5: Pluggable route target selection & load balancing (Weighted, Circuit Breaker, Least-TTFT,
     * Prompt Cache Prefix Affinity). Plugins inspect {@code ctx.candidateTargets()} and reorder via
     * {@code ctx.setCandidateTargets(...)}.
     */
    default PluginResult onRouteSelect(PluginExecutionContext ctx) {
        return PluginResult.Continue.INSTANCE;
    }

    /**
     * Stage 6: Right before dispatching HTTP request to the selected upstream target.
     */
    default PluginResult onPreUpstream(PluginExecutionContext ctx) {
        return PluginResult.Continue.INSTANCE;
    }

    /**
     * Stage 7a: Real-time streaming SSE chunk inspection & transformation ({@code ctx.streamChunk()} /
     * {@code ctx.setStreamChunk(...)}).
     */
    default PluginResult onStreamChunk(PluginExecutionContext ctx) {
        return PluginResult.Continue.INSTANCE;
    }

    /**
     * Stage 7b: Complete response content inspection (for both buffered responses and reassembled streams).
     */
    default PluginResult onResponseContent(PluginExecutionContext ctx) {
        return PluginResult.Continue.INSTANCE;
    }

    /**
     * Stage 8: Post-settlement FinOps cost attribution & asynchronous Billing Sink fan-out.
     */
    default void onPostSettle(PluginExecutionContext ctx) {
        // default no-op
    }

    default PluginResult executeStage(PluginStage stage, PluginExecutionContext ctx) {
        return switch (stage) {
            case AUTHENTICATE -> onAuthenticate(ctx);
            case REQUEST_RECEIVED -> onRequestReceived(ctx);
            case RATE_LIMIT -> onRateLimit(ctx);
            case PRE_ADMIT -> onPreAdmit(ctx);
            case POST_ADMIT_PRE_DISPATCH -> onPostAdmitPreDispatch(ctx);
            case ROUTE_SELECT -> onRouteSelect(ctx);
            case PRE_UPSTREAM -> onPreUpstream(ctx);
            case STREAM_CHUNK -> onStreamChunk(ctx);
            case RESPONSE_CONTENT -> onResponseContent(ctx);
            case POST_SETTLE -> {
                onPostSettle(ctx);
                yield PluginResult.Continue.INSTANCE;
            }
        };
    }
}
