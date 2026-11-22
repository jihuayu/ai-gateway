package com.jihuayu.aigateway.hot;

import com.jihuayu.aigateway.hot.HotContract.AdmitInput;
import com.jihuayu.aigateway.hot.HotContract.SettleInput;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.StructuredTaskScope;
import org.springframework.ai.chat.metadata.DefaultUsage;
import org.springframework.ai.chat.metadata.Usage;
import org.springframework.ai.chat.prompt.ChatOptions;

/**
 * Java 25 ScopedValue request context, Sealed Interface algebraic state machines,
 * Structured Concurrency (StructuredTaskScope) helper, and official Spring AI metadata bridge.
 */
public record RequestContext(
    String requestId,
    String projectId,
    String keyId,
    String nodeId,
    long admittedAtNanos
) {
    /**
     * Immutable Java 25 ScopedValue replacing ThreadLocal across virtual threads and StructuredTaskScope.
     */
    public static final ScopedValue<RequestContext> CURRENT = ScopedValue.newInstance();

    public static RequestContext currentOrAnonymous(String fallbackRequestId, String fallbackNodeId) {
        if (CURRENT.isBound()) {
            return CURRENT.get();
        }
        return new RequestContext(fallbackRequestId, "anon", "anon", fallbackNodeId, System.nanoTime());
    }

    public RequestContext withIdentity(String resolvedProjectId, String resolvedKeyId) {
        return new RequestContext(requestId, resolvedProjectId, resolvedKeyId, nodeId, admittedAtNanos);
    }

    /**
     * Sealed Interface representing all possible outcomes of the 3-phase hot-ledger admission stage.
     * Enforces compiler exhaustiveness checking in Java 25 pattern-matching switch expressions.
     */
    public sealed interface AdmitOutcome
        permits AdmitOutcome.Admitted,
                AdmitOutcome.RateLimited,
                AdmitOutcome.ConcurrencyExhausted,
                AdmitOutcome.BudgetExhausted,
                AdmitOutcome.Rejected {

        record Admitted(String requestId, String projectId, String keyId, long reservedMicros) implements AdmitOutcome {}

        record RateLimited(int retryAfterSeconds, String code, String message) implements AdmitOutcome {}

        record ConcurrencyExhausted(String code, String message) implements AdmitOutcome {}

        record BudgetExhausted(long availableMicros, String code, String message) implements AdmitOutcome {}

        record Rejected(int httpStatus, String code, String message) implements AdmitOutcome {}
    }

    /**
     * Sealed Interface representing the outcome of request settlement or pre-dispatch cancellation.
     */
    public sealed interface SettleOutcome
        permits SettleOutcome.Settled,
                SettleOutcome.Released,
                SettleOutcome.Failed {

        record Settled(
            String requestId,
            long actualMicros,
            Usage springAiUsage,
            ChatOptions springAiChatOptions
        ) implements SettleOutcome {}

        record Released(String requestId, String reason) implements SettleOutcome {}

        record Failed(int httpStatus, String code, String message) implements SettleOutcome {}
    }

    /**
     * Builds an official Spring AI {@link Usage} metadata instance from settled token counts.
     */
    public static Usage toSpringAiUsage(Long promptTokens, Long completionTokens) {
        int inTok = promptTokens != null ? Math.max(0, promptTokens.intValue()) : 0;
        int outTok = completionTokens != null ? Math.max(0, completionTokens.intValue()) : 0;
        return new DefaultUsage(inTok, outTok, inTok + outTok);
    }

    /**
     * Builds an official Spring AI {@link ChatOptions} metadata instance from request parameters.
     */
    public static ChatOptions toSpringAiChatOptions(String model, Integer maxTokens, Double temperature, Double topP) {
        ChatOptions.Builder builder = ChatOptions.builder().model(model);
        if (maxTokens != null) {
            builder.maxTokens(maxTokens);
        }
        if (temperature != null) {
            builder.temperature(temperature);
        }
        if (topP != null) {
            builder.topP(topP);
        }
        return builder.build();
    }

    /**
     * Executes parallel tasks inside a Java 25 {@link StructuredTaskScope}, automatically
     * propagating {@link RequestContext#CURRENT} ScopedValue to all forked virtual threads.
     */
    public static <T> List<T> executeStructuredParallel(List<Callable<T>> tasks) throws Exception {
        try (var scope = StructuredTaskScope.open()) {
            List<StructuredTaskScope.Subtask<T>> subtasks = new ArrayList<>(tasks.size());
            for (Callable<T> task : tasks) {
                subtasks.add(scope.fork(task));
            }
            scope.join();
            List<T> results = new ArrayList<>(subtasks.size());
            for (StructuredTaskScope.Subtask<T> st : subtasks) {
                if (st.state() == StructuredTaskScope.Subtask.State.SUCCESS) {
                    results.add(st.get());
                } else if (st.exception() instanceof Exception ex) {
                    throw ex;
                } else {
                    throw new RuntimeException(st.exception());
                }
            }
            return results;
        }
    }

    /**
     * Evaluates admission through HotBilling and maps the outcome into the exhaustive {@link AdmitOutcome} sealed hierarchy.
     */
    public static AdmitOutcome evaluateAdmit(HotBilling billing, AdmitInput input) {
        try {
            billing.admit(input);
            return new AdmitOutcome.Admitted(input.requestId(), input.projectId(), input.keyId(), input.reserveMicros());
        } catch (GatewayException ge) {
            return switch (ge.getCode()) {
                case "rate_limit_exceeded" -> {
                    int retrySec = 1;
                    Map<String, Object> details = ge.getDetails();
                    if (details != null && details.get("retryAfterMs") instanceof Number n) {
                        retrySec = Math.max(1, (int) Math.ceil(n.longValue() / 1000.0));
                    }
                    yield new AdmitOutcome.RateLimited(retrySec, ge.getCode(), ge.getMessage());
                }
                case "concurrency_limit_exceeded" ->
                    new AdmitOutcome.ConcurrencyExhausted(ge.getCode(), ge.getMessage());
                case "budget_exceeded" -> {
                    long avail = 0L;
                    Map<String, Object> details = ge.getDetails();
                    if (details != null && details.get("availableMicros") instanceof Number n) {
                        avail = n.longValue();
                    }
                    yield new AdmitOutcome.BudgetExhausted(avail, ge.getCode(), ge.getMessage());
                }
                default -> new AdmitOutcome.Rejected(ge.getStatusCode(), ge.getCode(), ge.getMessage());
            };
        }
    }

    /**
     * Evaluates settlement through HotBilling and returns an exhaustive {@link SettleOutcome} sealed result
     * enriched with official Spring AI {@link Usage} and {@link ChatOptions} metadata.
     */
    public static SettleOutcome evaluateSettle(
        HotBilling billing,
        String requestId,
        SettleInput input,
        String modelName,
        Integer maxTokens
    ) {
        try {
            billing.settle(requestId, input);
            Usage usage = toSpringAiUsage(input.inputTokens(), input.outputTokens());
            ChatOptions options = toSpringAiChatOptions(modelName, maxTokens, null, null);
            long actual = input.actualMicros() != null ? input.actualMicros() : 0L;
            return new SettleOutcome.Settled(requestId, actual, usage, options);
        } catch (GatewayException ge) {
            return new SettleOutcome.Failed(ge.getStatusCode(), ge.getCode(), ge.getMessage());
        }
    }
}
