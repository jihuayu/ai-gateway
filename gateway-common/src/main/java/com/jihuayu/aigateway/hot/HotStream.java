package com.jihuayu.aigateway.hot;

import io.lettuce.core.Consumer;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.XAutoClaimArgs;
import io.lettuce.core.XGroupCreateArgs;
import io.lettuce.core.XReadArgs;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.models.stream.ClaimedMessages;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

public class HotStream {
    private static final String ACK_DELETE_LUA = """
        local group = ARGV[1]
        local deleted = 0
        for i = 2, #ARGV do
          local id = ARGV[i]
          if redis.call('XACK', KEYS[1], group, id) == 1 then
            if redis.call('XDEL', KEYS[1], id) == 1 then deleted = deleted + 1 end
          end
        end
        return deleted
        """;

    public record StreamEntry(String id, String data) {}

    private final StatefulRedisConnection<String, String> redisConn;
    private final String key;
    private final String group;
    private final long claimIdleMs;
    private final String consumer;
    private volatile boolean initialized = false;
    private volatile String cursor = "0-0";

    public HotStream(StatefulRedisConnection<String, String> redisConn, String key, String group, long claimIdleMs) {
        this.redisConn = redisConn;
        this.key = key;
        this.group = group;
        this.claimIdleMs = claimIdleMs;
        this.consumer = "pg-" + UUID.randomUUID();
    }

    public HotStream(StatefulRedisConnection<String, String> redisConn, String key) {
        this(redisConn, key, "pg-hot-v1", 30_000L);
    }

    private void ensureGroup(RedisCommands<String, String> redis) {
        if (initialized) return;
        try {
            redis.xgroupCreate(XReadArgs.StreamOffset.from(key, "0"), group, XGroupCreateArgs.Builder.mkstream(true));
        } catch (Exception e) {
            if (e.getMessage() == null || !e.getMessage().contains("BUSYGROUP")) {
                throw e;
            }
        }
        initialized = true;
    }

    public List<StreamEntry> read(int limit) {
        int bound = Math.max(1, Math.min(500, limit));
        RedisCommands<String, String> redis = redisConn.sync();
        ensureGroup(redis);
        List<StreamEntry> out = new ArrayList<>();
        Set<String> seen = new LinkedHashSet<>();

        try {
            // 1. Reclaim messages abandoned by a crashed consumer via XAUTOCLAIM
            ClaimedMessages<String, String> claimed = redis.xautoclaim(
                key,
                XAutoClaimArgs.Builder.xautoclaim(Consumer.from(group, consumer), claimIdleMs, cursor).count(bound)
            );
            if (claimed != null) {
                if (claimed.getId() != null) {
                    this.cursor = claimed.getId();
                }
                drain(claimed.getMessages(), out, seen, bound);
            }
            // 2. Our own pending entries
            if (out.size() < bound) {
                List<StreamMessage<String, String>> pending = redis.xreadgroup(
                    Consumer.from(group, consumer),
                    XReadArgs.Builder.count(bound - out.size()),
                    XReadArgs.StreamOffset.from(key, "0")
                );
                drain(pending, out, seen, bound);
            }
            // 3. Never-delivered entries
            if (out.size() < bound) {
                List<StreamMessage<String, String>> fresh = redis.xreadgroup(
                    Consumer.from(group, consumer),
                    XReadArgs.Builder.count(bound - out.size()),
                    XReadArgs.StreamOffset.lastConsumed(key)
                );
                drain(fresh, out, seen, bound);
            }
        } catch (Exception e) {
            if (e.getMessage() != null && e.getMessage().contains("NOGROUP")) {
                this.initialized = false;
            }
            throw e;
        }
        return out;
    }

    private void drain(List<StreamMessage<String, String>> msgs, List<StreamEntry> out, Set<String> seen, int bound) {
        if (msgs == null) return;
        for (StreamMessage<String, String> msg : msgs) {
            if (out.size() >= bound) return;
            if (msg == null || msg.getId() == null) {
                throw new IllegalStateException("hot stream entry is malformed");
            }
            Map<String, String> body = msg.getBody();
            if (body == null || body.size() != 1 || !body.containsKey("data") || body.get("data") == null) {
                throw new IllegalStateException("hot stream entry must contain exactly one data field");
            }
            if (seen.add(msg.getId())) {
                out.add(new StreamEntry(msg.getId(), body.get("data")));
            }
        }
    }

    public void ack(List<String> ids) {
        if (ids == null || ids.isEmpty()) return;
        String[] argv = new String[ids.size() + 1];
        argv[0] = group;
        for (int i = 0; i < ids.size(); i++) {
            argv[i + 1] = ids.get(i);
        }
        try {
            redisConn.sync().eval(ACK_DELETE_LUA, ScriptOutputType.INTEGER, new String[]{key}, argv);
        } catch (Exception e) {
            if (e.getMessage() != null && e.getMessage().contains("NOGROUP")) {
                this.initialized = false;
            }
            throw e;
        }
    }
}
