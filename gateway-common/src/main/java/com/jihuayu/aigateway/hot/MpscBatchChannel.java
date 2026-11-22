package com.jihuayu.aigateway.hot;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

/**
 * Bounded MPSC Actor Group-Commit Pipeline
 * for Redis Lua execution.
 *
 * <p>When dozens of virtual threads concurrently invoke {@code admit}, {@code dispatch}, or {@code settle},
 * instead of contending on synchronous socket round-trips one by one, each caller pushes a command envelope
 * with a {@link CompletableFuture} into a bounded
 * MPSC queue ({@code capacity = 4096}). A dedicated Flusher Virtual Thread drains up to 128 ready commands
 * in a single micro-batch, disables Lettuce auto-flush, writes all {@code EVALSHA} commands into a single
 * TCP pipeline frame, flushes once, and completes each caller's oneshot future as responses arrive.
 */
public final class MpscBatchChannel implements AutoCloseable {

    public record EvalCommand(
        String sha,
        String fallbackLua,
        String[] keys,
        String[] argv,
        CompletableFuture<List<Object>> oneshot
    ) {}

    private static final int DEFAULT_CHANNEL_CAPACITY = 4_096;
    private static final int MAX_BATCH_SIZE = 128;
    private static final long MICRO_BATCH_PARK_NANOS = 60_000L; // 60 microseconds coalescing window under load

    private final StatefulRedisConnection<String, String> redisConn;
    private final ArrayBlockingQueue<EvalCommand> mpscQueue;
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final Thread flusherThread;

    private final AtomicLong totalCommandsSubmitted = new AtomicLong(0);
    private final AtomicLong totalBatchesFlushed = new AtomicLong(0);
    private final AtomicLong maxObservedBatchSize = new AtomicLong(0);

    public MpscBatchChannel(StatefulRedisConnection<String, String> redisConn) {
        this(redisConn, DEFAULT_CHANNEL_CAPACITY);
    }

    public MpscBatchChannel(StatefulRedisConnection<String, String> redisConn, int capacity) {
        this.redisConn = redisConn;
        this.mpscQueue = new ArrayBlockingQueue<>(Math.max(64, capacity));
        this.flusherThread = Thread.ofVirtual()
            .name("mpsc-batch-redis-flusher")
            .start(this::actorFlusherLoop);
    }

    /**
     * Submits a Lua command to the bounded MPSC channel and awaits the oneshot reply.
     */
    public List<Object> submitAndAwait(
        String sha,
        String fallbackLua,
        String[] keys,
        String[] argv,
        long timeoutMs
    ) {
        if (!running.get()) {
            return executeDirectFallback(sha, fallbackLua, keys, argv);
        }
        CompletableFuture<List<Object>> oneshot = new CompletableFuture<>();
        EvalCommand cmd = new EvalCommand(sha, fallbackLua, keys, argv, oneshot);
        if (!mpscQueue.offer(cmd)) {
            throw new GatewayException(503, "event_backlog", "MPSC Batch channel capacity exceeded (backpressure)");
        }
        totalCommandsSubmitted.incrementAndGet();
        try {
            return oneshot.get(Math.max(500L, timeoutMs), TimeUnit.MILLISECONDS);
        } catch (TimeoutException te) {
            throw new GatewayException(503, "billing_unavailable", "Redis group-commit pipeline timed out");
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new GatewayException(503, "billing_unavailable", "Redis operation interrupted");
        } catch (ExecutionException ee) {
            Throwable cause = ee.getCause();
            if (cause instanceof GatewayException ge) {
                throw ge;
            }
            if (cause instanceof RuntimeException re) {
                throw re;
            }
            throw new GatewayException(503, "billing_unavailable", "Redis billing unavailable");
        }
    }

    @SuppressWarnings("unchecked")
    private void actorFlusherLoop() {
        List<EvalCommand> batch = new ArrayList<>(MAX_BATCH_SIZE);
        while (running.get() || !mpscQueue.isEmpty()) {
            try {
                EvalCommand first = mpscQueue.poll(50, TimeUnit.MILLISECONDS);
                if (first == null) {
                    continue;
                }
                batch.add(first);
                // If no other items immediately visible, park briefly (60 us) to allow concurrent virtual threads to coalesce
                if (mpscQueue.isEmpty()) {
                    LockSupport.parkNanos(MICRO_BATCH_PARK_NANOS);
                }
                mpscQueue.drainTo(batch, MAX_BATCH_SIZE - 1);

                int size = batch.size();
                totalBatchesFlushed.incrementAndGet();
                maxObservedBatchSize.accumulateAndGet(size, Math::max);

                if (size == 1) {
                    EvalCommand single = batch.get(0);
                    try {
                        List<Object> res = executeDirectFallback(single.sha(), single.fallbackLua(), single.keys(), single.argv());
                        single.oneshot().complete(res);
                    } catch (Throwable t) {
                        single.oneshot().completeExceptionally(t);
                    }
                } else {
                    flushPipelinedBatch(batch);
                }
            } catch (InterruptedException ie) {
                if (!running.get()) {
                    break;
                }
            } catch (Throwable unexpected) {
                for (EvalCommand cmd : batch) {
                    if (!cmd.oneshot().isDone()) {
                        cmd.oneshot().completeExceptionally(unexpected);
                    }
                }
            } finally {
                batch.clear();
            }
        }
    }

    @SuppressWarnings("unchecked")
    private void flushPipelinedBatch(List<EvalCommand> batch) {
        RedisAsyncCommands<String, String> async = redisConn.async();
        List<RedisFuture<List<Object>>> futures = new ArrayList<>(batch.size());
        synchronized (redisConn) {
            redisConn.setAutoFlushCommands(false);
            try {
                for (EvalCommand cmd : batch) {
                    RedisFuture<List<Object>> f = async.evalsha(
                        cmd.sha(),
                        ScriptOutputType.MULTI,
                        cmd.keys(),
                        cmd.argv()
                    );
                    futures.add(f);
                }
                redisConn.flushCommands();
            } finally {
                redisConn.setAutoFlushCommands(true);
            }
        }

        for (int i = 0; i < batch.size(); i++) {
            EvalCommand cmd = batch.get(i);
            RedisFuture<List<Object>> f = futures.get(i);
            try {
                List<Object> res = f.get(5, TimeUnit.SECONDS);
                cmd.oneshot().complete(res);
            } catch (ExecutionException ee) {
                Throwable cause = ee.getCause();
                if (cause != null && cause.getMessage() != null && cause.getMessage().contains("NOSCRIPT")) {
                    try {
                        List<Object> fallbackRes = redisConn.sync().eval(
                            cmd.fallbackLua(),
                            ScriptOutputType.MULTI,
                            cmd.keys(),
                            cmd.argv()
                        );
                        cmd.oneshot().complete(fallbackRes);
                    } catch (Throwable t2) {
                        cmd.oneshot().completeExceptionally(t2);
                    }
                } else {
                    cmd.oneshot().completeExceptionally(cause != null ? cause : ee);
                }
            } catch (Throwable t) {
                cmd.oneshot().completeExceptionally(t);
            }
        }
    }

    private List<Object> executeDirectFallback(String sha, String fallbackLua, String[] keys, String[] argv) {
        try {
            return redisConn.sync().evalsha(sha, ScriptOutputType.MULTI, keys, argv);
        } catch (Exception e) {
            if (e.getMessage() != null && e.getMessage().contains("NOSCRIPT")) {
                return redisConn.sync().eval(fallbackLua, ScriptOutputType.MULTI, keys, argv);
            }
            throw e;
        }
    }

    public long totalCommandsSubmitted() {
        return totalCommandsSubmitted.get();
    }

    public long totalBatchesFlushed() {
        return totalBatchesFlushed.get();
    }

    public long maxObservedBatchSize() {
        return maxObservedBatchSize.get();
    }

    @Override
    public void close() {
        running.set(false);
        flusherThread.interrupt();
    }
}
