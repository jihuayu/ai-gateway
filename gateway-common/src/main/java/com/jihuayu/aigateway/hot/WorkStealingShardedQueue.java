package com.jihuayu.aigateway.hot;

import com.jihuayu.aigateway.hot.HotStream.StreamEntry;
import io.lettuce.core.api.StatefulRedisConnection;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.CRC32;

/**
 * Bounded Distributed Sharded Work-Stealing Queue built over partitioned Redis Streams.
 *
 * <p>Partitions events across {@code N} deterministic shard streams ({@code journal:shard-0 .. N-1})
 * using {@code crc32(projectId) % N} so that all lifecycle events for a given project remain strictly
 * ordered within its home shard while eliminating single-stream lock contention.
 *
 * <p>Workers consume from their assigned primary shard first (fast-path cache locality). Whenever a worker's
 * primary shard is empty, it executes a randomized Work-Stealing probe across sibling shards to steal
 * backlogged or orphaned entries, guaranteeing lock-free horizontal scaling across distributed nodes.
 */
public final class WorkStealingShardedQueue {

    public static final int DEFAULT_SHARDS = 16;

    private final StatefulRedisConnection<String, String> redisConn;
    private final String baseJournalKey;
    private final int shardCount;
    private final HotStream[] shardStreams;
    private final AtomicLong primaryConsumedCount = new AtomicLong(0);
    private final AtomicLong workStolenCount = new AtomicLong(0);

    public WorkStealingShardedQueue(StatefulRedisConnection<String, String> redisConn, String baseJournalKey) {
        this(redisConn, baseJournalKey, DEFAULT_SHARDS);
    }

    public WorkStealingShardedQueue(
        StatefulRedisConnection<String, String> redisConn,
        String baseJournalKey,
        int shardCount
    ) {
        this.redisConn = redisConn;
        this.baseJournalKey = baseJournalKey;
        this.shardCount = Math.max(1, shardCount);
        this.shardStreams = new HotStream[this.shardCount];
        for (int i = 0; i < this.shardCount; i++) {
            this.shardStreams[i] = new HotStream(redisConn, shardKey(i));
        }
    }

    public String shardKey(int shardIndex) {
        return baseJournalKey + ":shard-" + shardIndex;
    }

    public int shardForProject(String projectId) {
        if (projectId == null || projectId.isEmpty()) {
            return 0;
        }
        CRC32 crc = new CRC32();
        crc.update(projectId.getBytes(StandardCharsets.UTF_8));
        return (int) (crc.getValue() % shardCount);
    }

    public String appendToProjectShard(String projectId, String jsonPayload) {
        int shard = shardForProject(projectId);
        return redisConn.sync().xadd(shardKey(shard), "data", jsonPayload);
    }

    public record ShardedBatch(int shardIndex, List<StreamEntry> entries, boolean stolen) {}

    /**
     * Polls from {@code primaryShard} first; if empty, steals work from the most backlogged sibling shard.
     */
    public ShardedBatch pollOrSteal(int primaryShard, int limit) {
        int home = Math.floorMod(primaryShard, shardCount);
        List<StreamEntry> local = shardStreams[home].read(limit);
        if (!local.isEmpty()) {
            primaryConsumedCount.addAndGet(local.size());
            return new ShardedBatch(home, local, false);
        }

        // Work-Stealing: probe all sibling shards starting from a random offset
        int startOffset = ThreadLocalRandom.current().nextInt(shardCount);
        for (int i = 0; i < shardCount; i++) {
            int victim = (home + startOffset + i) % shardCount;
            if (victim == home) continue;
            List<StreamEntry> stolen = shardStreams[victim].read(limit);
            if (!stolen.isEmpty()) {
                workStolenCount.addAndGet(stolen.size());
                return new ShardedBatch(victim, stolen, true);
            }
        }
        return new ShardedBatch(home, List.of(), false);
    }

    public void ack(int shardIndex, List<String> ids) {
        if (ids == null || ids.isEmpty()) return;
        int idx = Math.floorMod(shardIndex, shardCount);
        shardStreams[idx].ack(ids);
    }

    public long primaryConsumedCount() {
        return primaryConsumedCount.get();
    }

    public long workStolenCount() {
        return workStolenCount.get();
    }

    public int shardCount() {
        return shardCount;
    }
}
