package com.jihuayu.aigateway.worker;

import com.fasterxml.jackson.databind.ObjectMapper;
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

    public static void consumeEvents(DataSource pool, List<Envelope> events, ObjectMapper mapper) {
        if (events == null || events.isEmpty()) return;
        Map<String, Map<String, String>> deduped = new LinkedHashMap<>();
        for (Envelope ev : events) {
            if (ev == null || ev.schema_version() != 1 || ev.event_id() == null || ev.event_id().isBlank()) {
                throw new IllegalArgumentException("Unsupported event schema");
            }
            Map<String, String> item = new LinkedHashMap<>();
            item.put("event_id", ev.event_id());
            item.put("project_id", ev.project_id());
            deduped.put(ev.event_id(), item);
        }
        List<Map<String, String>> entries = new ArrayList<>(deduped.values());
        entries.sort((a, b) -> a.get("event_id").compareTo(b.get("event_id")));

        try (Connection conn = pool.getConnection()) {
            boolean oldAuto = conn.getAutoCommit();
            conn.setAutoCommit(false);
            try {
                Set<String> projects = new TreeSet<>();
                try (PreparedStatement ps = conn.prepareStatement("""
                    INSERT INTO consumer_inbox(consumer_name, event_id, project_id)
                    SELECT 'usage-v1', event_id, project_id
                      FROM jsonb_to_recordset(?::jsonb) AS e(event_id text, project_id uuid)
                     ORDER BY event_id
                    ON CONFLICT DO NOTHING
                    RETURNING project_id::text
                    """)) {
                    ps.setString(1, mapper.writeValueAsString(entries));
                    try (ResultSet rs = ps.executeQuery()) {
                        while (rs.next()) {
                            String pid = rs.getString(1);
                            if (pid != null && !pid.isBlank()) {
                                projects.add(pid);
                            }
                        }
                    }
                }
                for (String project : projects) {
                    try (PreparedStatement lockPs = conn.prepareStatement("SELECT pg_advisory_xact_lock(hashtextextended(?, 0))")) {
                        lockPs.setString(1, project);
                        lockPs.execute();
                    }
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
            throw new IllegalStateException("Consume events failed", e);
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
                   LIMIT 50
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
