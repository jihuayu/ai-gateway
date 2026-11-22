package com.jihuayu.aigateway.hot;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jihuayu.aigateway.hot.HotContract.AttemptRecord;
import com.jihuayu.aigateway.hot.HotContract.HotKeys;
import com.jihuayu.aigateway.hot.HotContract.HotRequest;
import com.jihuayu.aigateway.hot.HotStream.StreamEntry;
import io.lettuce.core.api.StatefulRedisConnection;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import javax.sql.DataSource;

public class HotProjector {
    private static final int DEFAULT_BATCH_LIMIT = 200;

    private final DataSource pool;
    private final StatefulRedisConnection<String, String> redisConn;
    private final HotKeys keys;
    private volatile String expectedEpoch;
    private final ObjectMapper mapper;
    private final HotStream stream;
    private volatile boolean stopped = false;

    public HotProjector(
        DataSource pool,
        StatefulRedisConnection<String, String> redisConn,
        HotKeys keys,
        String expectedEpoch,
        ObjectMapper mapper
    ) {
        this.pool = pool;
        this.redisConn = redisConn;
        this.keys = keys;
        this.expectedEpoch = expectedEpoch;
        this.mapper = mapper;
        this.stream = new HotStream(redisConn, keys.journal());
    }

    private record CoalescedEntry(HotRequest event, List<String> ids) {}

    public synchronized int tick() {
        if (stopped) return 0;
        List<StreamEntry> entries = stream.read(DEFAULT_BATCH_LIMIT);
        if (entries.isEmpty()) return 0;

        Map<String, CoalescedEntry> latest = new LinkedHashMap<>();
        for (StreamEntry entry : entries) {
            HotRequest event;
            try {
                event = mapper.readValue(entry.data(), HotRequest.class);
            } catch (Exception e) {
                throw new IllegalStateException("Malformed HotRequest JSON in stream", e);
            }
            validateEvent(event);
            if (expectedEpoch != null && !expectedEpoch.isBlank() && !expectedEpoch.equals(event.epoch())) {
                throw new IllegalStateException("Unexpected financial epoch");
            }
            CoalescedEntry prev = latest.get(event.requestId());
            if (prev != null && !prev.event().epoch().equals(event.epoch())) {
                throw new IllegalStateException("Projection epoch conflict");
            }
            List<String> ids = new ArrayList<>();
            if (prev != null) ids.addAll(prev.ids());
            ids.add(entry.id());
            HotRequest chosen = (prev != null && prev.event().revision() > event.revision()) ? prev.event() : event;
            latest.put(event.requestId(), new CoalescedEntry(chosen, ids));
        }

        // Group coalesced requests by projectId in deterministic order so a single PG transaction
        // projects all requests for that project with one advisory lock and one COMMIT.
        Map<String, List<CoalescedEntry>> byProject = new TreeMap<>();
        for (CoalescedEntry item : latest.values()) {
            byProject.computeIfAbsent(item.event().projectId(), k -> new ArrayList<>()).add(item);
        }

        for (Map.Entry<String, List<CoalescedEntry>> group : byProject.entrySet()) {
            List<CoalescedEntry> projectBatch = group.getValue();
            if (projectBatch.size() == 1) {
                CoalescedEntry single = projectBatch.get(0);
                projectHotRequest(pool, single.event(), mapper);
                stream.ack(single.ids());
                continue;
            }
            try {
                projectHotRequestBatch(pool, group.getKey(), projectBatch.stream().map(CoalescedEntry::event).toList(), mapper);
                List<String> batchAckIds = new ArrayList<>();
                for (CoalescedEntry ce : projectBatch) {
                    batchAckIds.addAll(ce.ids());
                }
                stream.ack(batchAckIds);
            } catch (Exception batchErr) {
                // Fallback to per-request transactions so valid peers commit and any poison record stays unacked
                for (CoalescedEntry item : projectBatch) {
                    projectHotRequest(pool, item.event(), mapper);
                    stream.ack(item.ids());
                }
            }
        }
        return entries.size();
    }

    public static void projectHotRequestBatch(DataSource pool, String projectId, List<HotRequest> batch, ObjectMapper mapper) {
        if (batch == null || batch.isEmpty()) return;
        for (HotRequest e : batch) {
            validateEventAndFinancialConsistency(e);
        }
        try (Connection conn = pool.getConnection()) {
            boolean oldAuto = conn.getAutoCommit();
            conn.setAutoCommit(false);
            try {
                try (PreparedStatement ps = conn.prepareStatement("SELECT pg_advisory_xact_lock(hashtextextended(?,0))")) {
                    ps.setString(1, "gateway:billing:" + projectId);
                    ps.execute();
                }
                for (HotRequest e : batch) {
                    projectSingleInsideLockedTx(conn, e, mapper);
                }
                conn.commit();
            } catch (Exception ex) {
                conn.rollback();
                throw ex;
            } finally {
                conn.setAutoCommit(oldAuto);
            }
        } catch (RuntimeException re) {
            throw re;
        } catch (Exception ex) {
            throw new IllegalStateException("Project hot request batch failed: " + ex.getMessage(), ex);
        }
    }

    public void drain(long timeoutMs) {
        long until = System.currentTimeMillis() + timeoutMs;
        while (true) {
            Long xlen = redisConn.sync().xlen(keys.journal());
            if (xlen == null || xlen == 0L) break;
            if (stopped || System.currentTimeMillis() >= until) {
                throw new IllegalStateException("PG projection drain timed out; uncommitted events retained");
            }
            tick();
            try {
                Thread.sleep(2);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("PG projection drain interrupted", ie);
            }
        }
    }

    public void setExpectedEpoch(String expectedEpoch) {
        this.expectedEpoch = expectedEpoch;
    }

    public void close() {
        this.stopped = true;
    }

    public static void projectHotRequest(DataSource pool, HotRequest e, ObjectMapper mapper) {
        validateEventAndFinancialConsistency(e);
        try (Connection conn = pool.getConnection()) {
            boolean oldAuto = conn.getAutoCommit();
            conn.setAutoCommit(false);
            try {
                try (PreparedStatement ps = conn.prepareStatement("SELECT pg_advisory_xact_lock(hashtextextended(?,0))")) {
                    ps.setString(1, "gateway:billing:" + e.projectId());
                    ps.execute();
                }
                projectSingleInsideLockedTx(conn, e, mapper);
                conn.commit();
            } catch (Exception ex) {
                conn.rollback();
                throw ex;
            } finally {
                conn.setAutoCommit(oldAuto);
            }
        } catch (RuntimeException re) {
            throw re;
        } catch (Exception ex) {
            throw new IllegalStateException("Project hot request failed: " + ex.getMessage(), ex);
        }
    }

    private static void validateEventAndFinancialConsistency(HotRequest e) {
        validateEvent(e);
        boolean terminal = "settled".equals(e.state()) || "unknown".equals(e.state()) || "cancelled".equals(e.state());
        if (("settled".equals(e.state()) && (e.actualMicros() == null || !e.actualMicros().equals(e.chargedMicros())))
            || ("unknown".equals(e.state()) && (e.actualMicros() != null || e.chargedMicros() == null || e.chargedMicros() != e.reserveMicros()))
            || ("cancelled".equals(e.state()) && (e.actualMicros() != null || e.chargedMicros() == null || e.chargedMicros() != 0L))
            || (!terminal && (e.actualMicros() != null || e.chargedMicros() != null))) {
            throw new IllegalStateException("Inconsistent financial snapshot");
        }
    }

    private static void projectSingleInsideLockedTx(Connection conn, HotRequest e, ObjectMapper mapper) throws Exception {
        boolean terminal = "settled".equals(e.state()) || "unknown".equals(e.state()) || "cancelled".equals(e.state());
        Instant createdAt = Instant.parse(e.createdAt());
        Instant updatedAt = Instant.parse(e.updatedAt());

        try (PreparedStatement ps = conn.prepareStatement("SELECT epoch::text, revision FROM hot_projection_state WHERE request_id = ?::uuid")) {
            ps.setString(1, e.requestId());
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    String revEpoch = rs.getString(1);
                    long rev = rs.getLong(2);
                    if (!e.epoch().equals(revEpoch)) {
                        throw new IllegalStateException("Projection epoch conflict");
                    }
                    if (rev >= e.revision()) {
                        return;
                    }
                }
            }
        }

        try (PreparedStatement ps = conn.prepareStatement("""
            INSERT INTO requests(request_id, project_id, key_id, period, reserve_micros, route_version, price_snapshot, status, created_at, updated_at)
            VALUES(?::uuid, ?::uuid, (SELECT id FROM api_keys WHERE id = ?::uuid), ?, ?, ?::jsonb, ?::jsonb, 'reserved', ?, ?)
            ON CONFLICT(request_id) DO NOTHING
            """)) {
            ps.setString(1, e.requestId());
            ps.setString(2, e.projectId());
            ps.setString(3, e.keyId());
            ps.setString(4, e.period());
            ps.setLong(5, e.reserveMicros());
            ps.setString(6, mapper.writeValueAsString(e.routeVersion()));
            ps.setString(7, mapper.writeValueAsString(e.priceSnapshot()));
            ps.setTimestamp(8, Timestamp.from(createdAt));
            ps.setTimestamp(9, Timestamp.from(updatedAt));
            ps.executeUpdate();
        }

        String prevProjectId;
        String prevKeyId;
        String prevPeriod;
        long prevReserve;
        Long prevActual;
        try (PreparedStatement ps = conn.prepareStatement("""
            SELECT project_id::text, key_id::text, period, reserve_micros, actual_micros, status
              FROM requests WHERE request_id = ?::uuid FOR UPDATE
            """)) {
            ps.setString(1, e.requestId());
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) throw new IllegalStateException("Request row missing");
                prevProjectId = rs.getString(1);
                prevKeyId = rs.getString(2);
                prevPeriod = rs.getString(3);
                prevReserve = rs.getLong(4);
                long act = rs.getLong(5);
                prevActual = rs.wasNull() ? null : act;
            }
        }
        if (!e.projectId().equals(prevProjectId)
            || (prevKeyId != null && !prevKeyId.equals(e.keyId()))
            || !e.period().equals(prevPeriod)
            || prevReserve != e.reserveMicros()) {
            throw new IllegalStateException("Projection request collision");
        }

        String prevReservationStatus = null;
        try (PreparedStatement ps = conn.prepareStatement("SELECT status FROM reservations WHERE request_id = ?::uuid")) {
            ps.setString(1, e.requestId());
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) prevReservationStatus = rs.getString(1);
            }
        }
        if ("cancelled".equals(prevReservationStatus) && !"cancelled".equals(e.state())) {
            throw new IllegalStateException("Cannot reopen cancelled request");
        }
        if (prevActual != null && (e.actualMicros() == null || !prevActual.equals(e.actualMicros()))) {
            throw new IllegalStateException("Actual settlement is immutable");
        }

        Long baselineAmount = null;
        Boolean baselineEstimated = null;
        Timestamp baselineCreatedAt = null;
        try (PreparedStatement ps = conn.prepareStatement("""
            SELECT amount_micros, estimated, created_at
              FROM ledger
             WHERE event_id IN (?, ?)
             ORDER BY event_id DESC
            """)) {
            ps.setString(1, "settle:" + e.requestId());
            ps.setString(2, "estimate:" + e.requestId());
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    baselineAmount = rs.getLong(1);
                    baselineEstimated = rs.getBoolean(2);
                    baselineCreatedAt = rs.getTimestamp(3);
                }
            }
        }
        if (baselineAmount != null && !terminal) {
            throw new IllegalStateException("Cannot regress a charged request");
        }

        String eventType = null;
        String eventId = "";
        if ("cancelled".equals(e.state())) {
            if (baselineAmount != null) {
                throw new IllegalStateException("Cannot cancel charged request");
            }
            try (PreparedStatement ps = conn.prepareStatement("""
                INSERT INTO ledger(event_id, request_id, project_id, period, amount_micros, kind, estimated, created_at)
                VALUES(?, ?::uuid, ?::uuid, ?, 0, 'release', false, ?)
                ON CONFLICT(event_id) DO NOTHING
                """)) {
                ps.setString(1, "release:" + e.requestId());
                ps.setString(2, e.requestId());
                ps.setString(3, e.projectId());
                ps.setString(4, e.period());
                ps.setTimestamp(5, Timestamp.from(createdAt));
                ps.executeUpdate();
            }
            eventType = "request.cancelled";
            eventId = "request.cancelled:" + e.requestId();
        } else if (terminal) {
            if (baselineAmount == null) {
                try (PreparedStatement ps = conn.prepareStatement("""
                    INSERT INTO ledger(event_id, request_id, project_id, period, amount_micros, kind, input_tokens, output_tokens, estimated, created_at)
                    VALUES(?, ?::uuid, ?::uuid, ?, ?, ?, ?, ?, ?, ?)
                    """)) {
                    ps.setString(1, "settle:" + e.requestId());
                    ps.setString(2, e.requestId());
                    ps.setString(3, e.projectId());
                    ps.setString(4, e.period());
                    ps.setLong(5, e.chargedMicros());
                    ps.setString(6, "unknown".equals(e.state()) ? "estimate" : "settle");
                    setNullableLong(ps, 7, e.inputTokens());
                    setNullableLong(ps, 8, e.outputTokens());
                    ps.setBoolean(9, "unknown".equals(e.state()));
                    ps.setTimestamp(10, Timestamp.from(createdAt));
                    ps.executeUpdate();
                }
                eventType = "request.settled";
                eventId = "request.settled:" + e.requestId();
            } else if (Boolean.TRUE.equals(baselineEstimated) && e.actualMicros() != null && prevActual == null) {
                long diff = e.actualMicros() - baselineAmount;
                try (PreparedStatement ps = conn.prepareStatement("""
                    INSERT INTO ledger(event_id, request_id, project_id, period, amount_micros, kind, input_tokens, output_tokens, estimated, created_at)
                    VALUES(?, ?::uuid, ?::uuid, ?, ?, 'adjust', ?, ?, false, GREATEST(?::timestamptz, ?::timestamptz + interval '1 microsecond'))
                    ON CONFLICT(event_id) DO NOTHING
                    """)) {
                    ps.setString(1, "adjust:" + e.requestId() + ":" + e.actualMicros());
                    ps.setString(2, e.requestId());
                    ps.setString(3, e.projectId());
                    ps.setString(4, e.period());
                    ps.setLong(5, diff);
                    setNullableLong(ps, 6, e.inputTokens());
                    setNullableLong(ps, 7, e.outputTokens());
                    ps.setTimestamp(8, Timestamp.from(updatedAt));
                    ps.setTimestamp(9, baselineCreatedAt);
                    ps.executeUpdate();
                }
                eventType = "request.adjusted";
                eventId = "request.adjusted:" + e.requestId() + ":" + e.actualMicros();
            } else if (Boolean.FALSE.equals(baselineEstimated) && !baselineAmount.equals(e.chargedMicros())) {
                throw new IllegalStateException("Conflicting ledger baseline");
            }
        }

        try (PreparedStatement ps = conn.prepareStatement("""
            UPDATE requests
               SET status = ?, actual_micros = ?, input_tokens = ?, output_tokens = ?,
                   alias_name = ?, error_code = ?, updated_at = ?
             WHERE request_id = ?::uuid
            """)) {
            ps.setString(1, e.status());
            setNullableLong(ps, 2, e.actualMicros());
            setNullableLong(ps, 3, e.inputTokens());
            setNullableLong(ps, 4, e.outputTokens());
            ps.setString(5, e.aliasName());
            ps.setString(6, e.errorCode());
            ps.setTimestamp(7, Timestamp.from(updatedAt));
            ps.setString(8, e.requestId());
            ps.executeUpdate();
        }

        try (PreparedStatement ps = conn.prepareStatement("""
            INSERT INTO reservations(request_id, project_id, period, reserved_micros, actual_micros, status, lease_expires_at, created_at, updated_at)
            VALUES(?::uuid, ?::uuid, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(request_id) DO UPDATE
               SET actual_micros = excluded.actual_micros,
                   status = excluded.status,
                   lease_expires_at = excluded.lease_expires_at,
                   updated_at = excluded.updated_at
            """)) {
            ps.setString(1, e.requestId());
            ps.setString(2, e.projectId());
            ps.setString(3, e.period());
            ps.setLong(4, e.reserveMicros());
            setNullableLong(ps, 5, e.actualMicros());
            ps.setString(6, e.state());
            if (terminal) {
                ps.setNull(7, Types.TIMESTAMP_WITH_TIMEZONE);
            } else {
                ps.setTimestamp(7, Timestamp.from(Instant.ofEpochMilli(e.leaseUntil())));
            }
            ps.setTimestamp(8, Timestamp.from(createdAt));
            ps.setTimestamp(9, Timestamp.from(updatedAt));
            ps.executeUpdate();
        }

        if (e.attempts() != null) {
            for (AttemptRecord a : e.attempts()) {
                Instant finished = updatedAt;
                Instant started = finished.minusMillis(Math.max(0L, a.latency_ms()));
                try (PreparedStatement ps = conn.prepareStatement("""
                    INSERT INTO attempts(id, request_id, attempt_no, target_id, status, error_code, started_at, finished_at, input_tokens, output_tokens, cost_micros)
                    VALUES(?::uuid, ?::uuid, ?, (SELECT id FROM model_targets WHERE id = ?::uuid), ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(request_id, attempt_no) DO UPDATE
                       SET status = excluded.status,
                           error_code = excluded.error_code,
                           finished_at = excluded.finished_at,
                           input_tokens = excluded.input_tokens,
                           output_tokens = excluded.output_tokens,
                           cost_micros = excluded.cost_micros
                    """)) {
                    ps.setString(1, UUID.randomUUID().toString());
                    ps.setString(2, e.requestId());
                    ps.setInt(3, a.attempt_no());
                    ps.setString(4, a.target_id());
                    ps.setString(5, a.status());
                    ps.setString(6, a.error_code());
                    ps.setTimestamp(7, Timestamp.from(started));
                    ps.setTimestamp(8, Timestamp.from(finished));
                    setNullableLong(ps, 9, a.input_tokens());
                    setNullableLong(ps, 10, a.output_tokens());
                    setNullableLong(ps, 11, a.cost_micros());
                    ps.executeUpdate();
                }
            }
        }

        if (eventType != null) {
            Map<String, Object> outboxPayload = new LinkedHashMap<>();
            outboxPayload.put("requestId", e.requestId());
            outboxPayload.put("projectId", e.projectId());
            outboxPayload.put("period", e.period());
            outboxPayload.put("status", e.status());
            outboxPayload.put("actualMicros", e.actualMicros());
            outboxPayload.put("chargedMicros", e.chargedMicros());
            outboxPayload.put("estimated", "unknown".equals(e.state()));
            outboxPayload.put("inputTokens", e.inputTokens());
            outboxPayload.put("outputTokens", e.outputTokens());
            outboxPayload.put("attempts", e.attempts());
            try (PreparedStatement ps = conn.prepareStatement("""
                INSERT INTO outbox(id, event_id, event_type, project_id, payload, status, created_at)
                VALUES(?::uuid, ?, ?, ?::uuid, ?::jsonb, 'pending', ?)
                ON CONFLICT(event_id) DO NOTHING
                """)) {
                ps.setString(1, UUID.randomUUID().toString());
                ps.setString(2, eventId);
                ps.setString(3, eventType);
                ps.setString(4, e.projectId());
                ps.setString(5, mapper.writeValueAsString(outboxPayload));
                ps.setTimestamp(6, Timestamp.from(updatedAt));
                ps.executeUpdate();
            }
        }

        try (PreparedStatement ps = conn.prepareStatement("""
            UPDATE api_keys
               SET last_used_at = GREATEST(COALESCE(last_used_at, ?::timestamptz), ?::timestamptz)
             WHERE id = ?::uuid
            """)) {
            Timestamp ts = Timestamp.from(updatedAt);
            ps.setTimestamp(1, ts);
            ps.setTimestamp(2, ts);
            ps.setString(3, e.keyId());
            ps.executeUpdate();
        }

        try (PreparedStatement ps = conn.prepareStatement("""
            INSERT INTO hot_projection_state(request_id, epoch, revision)
            VALUES(?::uuid, ?::uuid, ?)
            ON CONFLICT(request_id) DO UPDATE
               SET revision = excluded.revision,
                   updated_at = now()
            """)) {
            ps.setString(1, e.requestId());
            ps.setString(2, e.epoch());
            ps.setLong(3, e.revision());
            ps.executeUpdate();
        }
    }

    private static void setNullableLong(PreparedStatement ps, int index, Long val) throws Exception {
        if (val == null) {
            ps.setNull(index, Types.BIGINT);
        } else {
            ps.setLong(index, val);
        }
    }

    private static void validateEvent(HotRequest e) {
        if (e == null || e.version() != 1) throw new IllegalStateException("Invalid event version");
        HotBilling.requireUuid(e.epoch());
        HotBilling.requireUuid(e.requestId());
        HotBilling.requireUuid(e.projectId());
        HotBilling.requireUuid(e.keyId());
        HotBilling.requireUuid(e.owner());
        if (e.revision() < 1) throw new IllegalStateException("Invalid revision");
        if (e.reserveMicros() < 0 || e.reserveMicros() > HotContract.MAX_SAFE_INTEGER) {
            throw new IllegalStateException("Invalid reserveMicros");
        }
        if (e.attempts() != null) {
            if (e.attempts().size() > 2) throw new IllegalStateException("Too many attempts");
            Set<Integer> seen = new HashSet<>();
            for (AttemptRecord a : e.attempts()) {
                if (!seen.add(a.attempt_no())) {
                    throw new IllegalStateException("Duplicate attempt number");
                }
            }
        }
    }
}
