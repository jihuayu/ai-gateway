package com.jihuayu.aigateway.worker;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jihuayu.aigateway.control.ControlAuthService;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import javax.sql.DataSource;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class EventWorker {
    public record Envelope(
        String event_id,
        int schema_version,
        String occurred_at,
        String project_id,
        String event_type,
        Object payload
    ) {}

    private record OutboxRow(
        String id,
        String eventId,
        String eventType,
        String projectId,
        Object payload,
        int attempts,
        Instant createdAt
    ) {}

    private final DataSource pool;
    private final List<String> brokers;
    private final ObjectMapper mapper;
    private KafkaProducer<String, String> producer;
    private volatile boolean stopped = false;

    public EventWorker(DataSource pool, List<String> brokers, ObjectMapper mapper) {
        this.pool = pool;
        this.brokers = brokers != null ? brokers.stream().filter(s -> s != null && !s.isBlank()).toList() : List.of();
        this.mapper = mapper;
    }

    public void start() {
        setupEvents(pool);
        if (!brokers.isEmpty()) {
            Properties props = new Properties();
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, String.join(",", brokers));
            props.put(ProducerConfig.CLIENT_ID_CONFIG, "ai-gateway");
            props.put(ProducerConfig.ACKS_CONFIG, "all");
            props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
            props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "1");
            props.put(ProducerConfig.RETRIES_CONFIG, "3");
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            this.producer = new KafkaProducer<>(props);
        }
    }

    public static void setupEvents(DataSource pool) {
        try (Connection conn = pool.getConnection(); Statement st = conn.createStatement()) {
            st.execute("""
                CREATE TABLE IF NOT EXISTS usage_hourly (
                  project_id uuid NOT NULL REFERENCES projects(id),
                  bucket timestamptz NOT NULL,
                  actual_micros bigint NOT NULL DEFAULT 0,
                  estimated_micros bigint NOT NULL DEFAULT 0,
                  input_tokens bigint NOT NULL DEFAULT 0,
                  output_tokens bigint NOT NULL DEFAULT 0,
                  event_count bigint NOT NULL DEFAULT 0,
                  updated_at timestamptz NOT NULL DEFAULT now(),
                  PRIMARY KEY(project_id, bucket)
                );
                CREATE TABLE IF NOT EXISTS usage_request_state (
                  request_id uuid PRIMARY KEY,
                  project_id uuid NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
                  bucket timestamptz NOT NULL,
                  actual_micros bigint NOT NULL DEFAULT 0,
                  estimated_micros bigint NOT NULL DEFAULT 0,
                  input_tokens bigint NOT NULL DEFAULT 0,
                  output_tokens bigint NOT NULL DEFAULT 0,
                  counted boolean NOT NULL DEFAULT false,
                  updated_at timestamptz NOT NULL DEFAULT now()
                );
                CREATE TABLE IF NOT EXISTS dead_letters (
                  event_id text PRIMARY KEY,
                  envelope jsonb NOT NULL,
                  reason text NOT NULL,
                  created_at timestamptz DEFAULT now()
                );
                """);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to setup events tables", e);
        }
    }

    /**
     * #21 O(1) Request-Level Incremental Usage Projection:
     * Claims events in consumer_inbox, extracts the affected (project_id, request_id) pairs,
     * and computes the exact delta between the request's current net ledger state and its
     * previously projected state in usage_request_state. Never scans the full historical ledger.
     */
    public static void consumeEvents(DataSource pool, List<Envelope> events, ObjectMapper mapper) {
        if (events == null || events.isEmpty()) return;
        Map<String, Envelope> deduped = new LinkedHashMap<>();
        List<Map<String, String>> inboxRows = new ArrayList<>();
        for (Envelope ev : events) {
            if (ev == null || ev.schema_version() != 1 || ev.event_id() == null || ev.event_id().isBlank()) {
                throw new IllegalArgumentException("Unsupported event schema");
            }
            if (!deduped.containsKey(ev.event_id())) {
                deduped.put(ev.event_id(), ev);
                Map<String, String> row = new LinkedHashMap<>();
                row.put("event_id", ev.event_id());
                row.put("project_id", ev.project_id());
                inboxRows.add(row);
            }
        }
        inboxRows.sort((a, b) -> a.get("event_id").compareTo(b.get("event_id")));

        try (Connection conn = pool.getConnection()) {
            boolean oldAuto = conn.getAutoCommit();
            conn.setAutoCommit(false);
            try {
                Set<String> newlyInsertedEventIds = new LinkedHashSet<>();
                try (PreparedStatement ps = conn.prepareStatement("""
                    INSERT INTO consumer_inbox(consumer_name, event_id, project_id)
                    SELECT 'usage-v1', event_id, project_id
                      FROM jsonb_to_recordset(?::jsonb) AS e(event_id text, project_id uuid)
                     ORDER BY event_id
                    ON CONFLICT DO NOTHING
                    RETURNING event_id
                    """)) {
                    ps.setString(1, mapper.writeValueAsString(inboxRows));
                    try (ResultSet rs = ps.executeQuery()) {
                        while (rs.next()) {
                            newlyInsertedEventIds.add(rs.getString(1));
                        }
                    }
                }
                if (newlyInsertedEventIds.isEmpty()) {
                    conn.commit();
                    return;
                }

                // Group affected requestIds by projectId
                Map<String, Set<String>> requestsByProject = new TreeMap<>();
                Set<String> fallbackFullProjects = new TreeSet<>();
                for (String evId : newlyInsertedEventIds) {
                    Envelope ev = deduped.get(evId);
                    if (ev == null || ev.project_id() == null || ev.project_id().isBlank()) continue;
                    String reqId = extractRequestId(ev);
                    if (reqId != null && ControlAuthService.isUuid(reqId)) {
                        requestsByProject.computeIfAbsent(ev.project_id(), k -> new TreeSet<>()).add(reqId);
                    } else {
                        fallbackFullProjects.add(ev.project_id());
                    }
                }

                Set<String> allProjects = new TreeSet<>(requestsByProject.keySet());
                allProjects.addAll(fallbackFullProjects);

                for (String project : allProjects) {
                    try (PreparedStatement lockPs = conn.prepareStatement("SELECT pg_advisory_xact_lock(hashtextextended(?, 0))")) {
                        lockPs.setString(1, project);
                        lockPs.execute();
                    }
                    if (fallbackFullProjects.contains(project)) {
                        recomputeProjectUsageFull(conn, project);
                    } else {
                        Set<String> reqIds = requestsByProject.get(project);
                        if (reqIds != null && !reqIds.isEmpty()) {
                            incrementallyProjectRequests(conn, project, reqIds);
                        }
                    }
                }
                conn.commit();
            } catch (Exception e) {
                conn.rollback();
                throw e;
            } finally {
                conn.setAutoCommit(oldAuto);
            }
        } catch (RuntimeException re) {
            throw re;
        } catch (Exception e) {
            throw new IllegalStateException("Consume events failed: " + e.getMessage(), e);
        }
    }

    private static String extractRequestId(Envelope ev) {
        if (ev.payload() instanceof Map<?, ?> map) {
            Object r = map.get("requestId") != null ? map.get("requestId") : map.get("request_id");
            if (r instanceof String s && ControlAuthService.isUuid(s)) {
                return s;
            }
        }
        String eventId = ev.event_id();
        String[] parts = eventId.split(":");
        if (parts.length >= 2 && ControlAuthService.isUuid(parts[1])) {
            return parts[1];
        }
        return null;
    }

    /**
     * Incrementally updates usage_hourly for only the specific request IDs touched in this batch.
     * Complexity is O(|reqIds|) using the indexed `ledger (request_id)` lookup, independent of total historical ledger size.
     */
    private static void incrementallyProjectRequests(Connection conn, String projectId, Set<String> reqIds) throws Exception {
        for (String reqId : reqIds) {
            Timestamp newBucket = null;
            long netAmount = 0L;
            boolean latestEstimated = false;
            long latestInTokens = 0L;
            long latestOutTokens = 0L;
            boolean hasLedgerEntry = false;

            try (PreparedStatement ps = conn.prepareStatement("""
                SELECT date_trunc('hour', min(created_at)) AS bucket,
                       COALESCE(sum(amount_micros), 0)::bigint AS amount,
                       (array_agg(estimated ORDER BY created_at DESC, event_id DESC))[1] AS estimated,
                       COALESCE((array_agg(input_tokens ORDER BY created_at DESC, event_id DESC))[1], 0)::bigint AS input_tokens,
                       COALESCE((array_agg(output_tokens ORDER BY created_at DESC, event_id DESC))[1], 0)::bigint AS output_tokens,
                       count(*)::int AS cnt
                  FROM ledger
                 WHERE project_id = ?::uuid AND request_id = ?::uuid
                """)) {
                ps.setString(1, projectId);
                ps.setString(2, reqId);
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next() && rs.getInt("cnt") > 0) {
                        hasLedgerEntry = true;
                        newBucket = rs.getTimestamp("bucket");
                        netAmount = rs.getLong("amount");
                        latestEstimated = rs.getBoolean("estimated");
                        latestInTokens = rs.getLong("input_tokens");
                        latestOutTokens = rs.getLong("output_tokens");
                    }
                }
            }
            if (!hasLedgerEntry || newBucket == null) {
                continue;
            }

            long newActual = latestEstimated ? 0L : netAmount;
            long newEstimated = latestEstimated ? netAmount : 0L;

            Timestamp oldBucket = null;
            long oldActual = 0L;
            long oldEstimated = 0L;
            long oldIn = 0L;
            long oldOut = 0L;
            boolean oldCounted = false;

            try (PreparedStatement ps = conn.prepareStatement("""
                SELECT bucket, actual_micros, estimated_micros, input_tokens, output_tokens, counted
                  FROM usage_request_state
                 WHERE request_id = ?::uuid
                   FOR UPDATE
                """)) {
                ps.setString(1, reqId);
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) {
                        oldBucket = rs.getTimestamp(1);
                        oldActual = rs.getLong(2);
                        oldEstimated = rs.getLong(3);
                        oldIn = rs.getLong(4);
                        oldOut = rs.getLong(5);
                        oldCounted = rs.getBoolean(6);
                    }
                }
            }

            Timestamp targetBucket = oldBucket != null ? oldBucket : newBucket;
            long deltaActual = newActual - oldActual;
            long deltaEstimated = newEstimated - oldEstimated;
            long deltaIn = latestInTokens - oldIn;
            long deltaOut = latestOutTokens - oldOut;
            long deltaCount = oldCounted ? 0L : 1L;

            if (deltaActual == 0 && deltaEstimated == 0 && deltaIn == 0 && deltaOut == 0 && deltaCount == 0) {
                continue;
            }

            try (PreparedStatement ps = conn.prepareStatement("""
                INSERT INTO usage_request_state(request_id, project_id, bucket, actual_micros, estimated_micros, input_tokens, output_tokens, counted, updated_at)
                VALUES(?::uuid, ?::uuid, ?, ?, ?, ?, ?, true, now())
                ON CONFLICT(request_id) DO UPDATE
                   SET actual_micros = excluded.actual_micros,
                       estimated_micros = excluded.estimated_micros,
                       input_tokens = excluded.input_tokens,
                       output_tokens = excluded.output_tokens,
                       counted = true,
                       updated_at = now()
                """)) {
                ps.setString(1, reqId);
                ps.setString(2, projectId);
                ps.setTimestamp(3, targetBucket);
                ps.setLong(4, newActual);
                ps.setLong(5, newEstimated);
                ps.setLong(6, latestInTokens);
                ps.setLong(7, latestOutTokens);
                ps.executeUpdate();
            }

            try (PreparedStatement ps = conn.prepareStatement("""
                INSERT INTO usage_hourly(project_id, bucket, actual_micros, estimated_micros, input_tokens, output_tokens, event_count, updated_at)
                VALUES(?::uuid, ?, ?, ?, ?, ?, ?, now())
                ON CONFLICT(project_id, bucket) DO UPDATE
                   SET actual_micros = usage_hourly.actual_micros + excluded.actual_micros,
                       estimated_micros = usage_hourly.estimated_micros + excluded.estimated_micros,
                       input_tokens = usage_hourly.input_tokens + excluded.input_tokens,
                       output_tokens = usage_hourly.output_tokens + excluded.output_tokens,
                       event_count = usage_hourly.event_count + excluded.event_count,
                       updated_at = now()
                """)) {
                ps.setString(1, projectId);
                ps.setTimestamp(2, targetBucket);
                ps.setLong(3, deltaActual);
                ps.setLong(4, deltaEstimated);
                ps.setLong(5, deltaIn);
                ps.setLong(6, deltaOut);
                ps.setLong(7, deltaCount);
                ps.executeUpdate();
            }
        }
    }

    private static void recomputeProjectUsageFull(Connection conn, String project) throws Exception {
        try (PreparedStatement aggPs = conn.prepareStatement("""
            INSERT INTO usage_hourly(project_id, bucket, actual_micros, estimated_micros, input_tokens, output_tokens, event_count)
            SELECT project_id, date_trunc('hour', first_at),
                   COALESCE(sum(amount) FILTER(WHERE NOT estimated), 0),
                   COALESCE(sum(amount) FILTER(WHERE estimated), 0),
                   COALESCE(sum(input_tokens), 0),
                   COALESCE(sum(output_tokens), 0),
                   count(*)
              FROM (
                SELECT project_id,
                       COALESCE(request_id::text, NULLIF(split_part(event_id, ':', 2), ''), event_id) AS identity,
                       min(created_at) AS first_at,
                       sum(amount_micros) AS amount,
                       (array_agg(estimated ORDER BY created_at DESC, event_id DESC))[1] AS estimated,
                       (array_agg(input_tokens ORDER BY created_at DESC, event_id DESC))[1] AS input_tokens,
                       (array_agg(output_tokens ORDER BY created_at DESC, event_id DESC))[1] AS output_tokens
                  FROM ledger
                 WHERE project_id = ?::uuid
                 GROUP BY project_id, identity
              ) entries
             GROUP BY project_id, date_trunc('hour', first_at)
            ON CONFLICT(project_id, bucket) DO UPDATE
               SET actual_micros = excluded.actual_micros,
                   estimated_micros = excluded.estimated_micros,
                   input_tokens = excluded.input_tokens,
                   output_tokens = excluded.output_tokens,
                   event_count = excluded.event_count,
                   updated_at = now()
            """)) {
            aggPs.setString(1, project);
            aggPs.executeUpdate();
        }
    }

    public static void consumeEvent(DataSource pool, Envelope event, ObjectMapper mapper) {
        consumeEvents(pool, List.of(event), mapper);
    }

    public synchronized int tick() {
        if (stopped) return 0;
        List<OutboxRow> rows = new ArrayList<>();
        try (Connection conn = pool.getConnection()) {
            boolean oldAuto = conn.getAutoCommit();
            conn.setAutoCommit(false);
            try (PreparedStatement ps = conn.prepareStatement("""
                WITH candidate AS (
                  SELECT id FROM outbox
                   WHERE status IN ('pending', 'leased') AND available_at <= now()
                   ORDER BY created_at
                   FOR UPDATE SKIP LOCKED
                   LIMIT 200
                )
                UPDATE outbox o
                   SET status = 'leased', available_at = now() + interval '30 seconds'
                  FROM candidate c
                 WHERE o.id = c.id
                RETURNING o.id::text, o.event_id, o.event_type, o.project_id::text, o.payload::text, o.attempts, o.created_at
                """);
                 ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String payloadJson = rs.getString(5);
                    Object payloadObj = payloadJson != null ? mapper.readValue(payloadJson, Object.class) : Map.of();
                    Timestamp ts = rs.getTimestamp(7);
                    rows.add(new OutboxRow(
                        rs.getString(1),
                        rs.getString(2),
                        rs.getString(3),
                        rs.getString(4),
                        payloadObj,
                        rs.getInt(6),
                        ts != null ? ts.toInstant() : Instant.now()
                    ));
                }
                conn.commit();
            } catch (Exception e) {
                conn.rollback();
                throw e;
            } finally {
                conn.setAutoCommit(oldAuto);
            }
        } catch (Exception e) {
            throw new IllegalStateException("Failed to claim outbox events", e);
        }

        if (rows.isEmpty()) return 0;

        if (producer == null) {
            try {
                List<Envelope> batch = rows.stream()
                    .map(r -> new Envelope(r.eventId(), 1, r.createdAt().toString(), r.projectId(), r.eventType(), r.payload()))
                    .toList();
                consumeEvents(pool, batch, mapper);
                try (Connection conn = pool.getConnection();
                     PreparedStatement ps = conn.prepareStatement("UPDATE outbox SET status = 'published', published_at = now() WHERE id = ANY(?::uuid[])")) {
                    ps.setArray(1, conn.createArrayOf("uuid", rows.stream().map(OutboxRow::id).toArray()));
                    ps.executeUpdate();
                }
                return rows.size();
            } catch (Exception ignored) {
                // Fall back to individual retry/DLQ handling
            }
        }

        for (OutboxRow row : rows) {
            Envelope env = new Envelope(row.eventId(), 1, row.createdAt().toString(), row.projectId(), row.eventType(), row.payload());
            try {
                if (producer != null) {
                    String kind = row.eventType().contains("audit") ? "audit"
                        : (row.eventType().contains("usage") || row.eventType().contains("settle") || row.eventType().contains("billing")) ? "usage"
                        : "request";
                    String key = row.projectId() != null ? row.projectId() : row.eventId();
                    producer.send(new ProducerRecord<>("gateway." + kind + ".v1", key, mapper.writeValueAsString(env))).get();
                    consumeEvent(pool, env, mapper);
                } else {
                    consumeEvent(pool, env, mapper);
                }
                try (Connection conn = pool.getConnection();
                     PreparedStatement ps = conn.prepareStatement("UPDATE outbox SET status = 'published', published_at = now() WHERE id = ?::uuid")) {
                    ps.setString(1, row.id());
                    ps.executeUpdate();
                }
            } catch (Exception e) {
                int attempt = row.attempts() + 1;
                try (Connection conn = pool.getConnection()) {
                    boolean oldAuto = conn.getAutoCommit();
                    conn.setAutoCommit(false);
                    try {
                        if (attempt >= 5) {
                            try (PreparedStatement ps = conn.prepareStatement("""
                                INSERT INTO dead_letters(event_id, envelope, reason)
                                VALUES(?, ?::jsonb, 'delivery_failed')
                                ON CONFLICT DO NOTHING
                                """)) {
                                ps.setString(1, row.eventId());
                                ps.setString(2, mapper.writeValueAsString(env));
                                ps.executeUpdate();
                            }
                        }
                        int backoffSec = Math.min(300, 1 << attempt);
                        try (PreparedStatement ps = conn.prepareStatement("""
                            UPDATE outbox
                               SET status = ?, attempts = ?, available_at = now() + (? * interval '1 second')
                             WHERE id = ?::uuid
                            """)) {
                            ps.setString(1, attempt >= 5 ? "failed" : "pending");
                            ps.setInt(2, attempt);
                            ps.setInt(3, backoffSec);
                            ps.setString(4, row.id());
                            ps.executeUpdate();
                        }
                        conn.commit();
                    } catch (Exception ex) {
                        conn.rollback();
                        throw ex;
                    } finally {
                        conn.setAutoCommit(oldAuto);
                    }
                } catch (Exception ignored) {}
            }
        }
        return rows.size();
    }

    public void replay(String eventId) {
        try (Connection conn = pool.getConnection();
             PreparedStatement ps = conn.prepareStatement("""
                 UPDATE outbox
                    SET status = 'pending', attempts = 0, available_at = now()
                  WHERE event_id = ? AND status = 'failed'
                 """)) {
            ps.setString(1, eventId);
            ps.executeUpdate();
        } catch (Exception e) {
            throw new IllegalStateException("Replay failed", e);
        }
    }

    public void close() {
        this.stopped = true;
        if (producer != null) {
            try { producer.close(); } catch (Exception ignored) {}
        }
    }
}
