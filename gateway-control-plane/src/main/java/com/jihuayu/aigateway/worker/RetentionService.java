package com.jihuayu.aigateway.worker;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.time.Instant;
import javax.sql.DataSource;

public final class RetentionService {
    private RetentionService() {}

    public record RetentionResult(int requestsDeleted, int attemptsDeleted, int sessionsDeleted, int outboxDeleted) {}

    public static RetentionResult cleanExpiredData(DataSource pool, int retentionDays) {
        if (retentionDays < 1 || retentionDays > 3650) {
            throw new IllegalArgumentException("retentionDays must be between 1 and 3650");
        }
        Instant cutoff = Instant.now().minusSeconds((long) retentionDays * 86_400L);
        Timestamp cutoffTs = Timestamp.from(cutoff);

        try (Connection conn = pool.getConnection()) {
            boolean oldAuto = conn.getAutoCommit();
            conn.setAutoCommit(false);
            try {
                int attemptsDeleted;
                try (PreparedStatement ps = conn.prepareStatement("""
                    DELETE FROM attempts
                     WHERE request_id IN (
                       SELECT request_id FROM requests
                        WHERE created_at < ?
                          AND status NOT IN ('preparing', 'reserved', 'dispatch_intent', 'dispatched', 'unknown')
                     )
                    """)) {
                    ps.setTimestamp(1, cutoffTs);
                    attemptsDeleted = ps.executeUpdate();
                }

                int requestsDeleted;
                try (PreparedStatement ps = conn.prepareStatement("""
                    DELETE FROM requests
                     WHERE created_at < ?
                       AND status NOT IN ('preparing', 'reserved', 'dispatch_intent', 'dispatched', 'unknown')
                    """)) {
                    ps.setTimestamp(1, cutoffTs);
                    requestsDeleted = ps.executeUpdate();
                }

                int sessionsDeleted;
                try (PreparedStatement ps = conn.prepareStatement("""
                    DELETE FROM sessions
                     WHERE expires_at < ? OR (revoked_at IS NOT NULL AND revoked_at < ?)
                    """)) {
                    ps.setTimestamp(1, cutoffTs);
                    ps.setTimestamp(2, cutoffTs);
                    sessionsDeleted = ps.executeUpdate();
                }

                int outboxDeleted;
                try (PreparedStatement ps = conn.prepareStatement("""
                    DELETE FROM outbox
                     WHERE status = 'published' AND published_at < ?
                    """)) {
                    ps.setTimestamp(1, cutoffTs);
                    outboxDeleted = ps.executeUpdate();
                }

                conn.commit();
                return new RetentionResult(requestsDeleted, attemptsDeleted, sessionsDeleted, outboxDeleted);
            } catch (Exception e) {
                conn.rollback();
                throw e;
            } finally {
                conn.setAutoCommit(oldAuto);
            }
        } catch (Exception e) {
            throw new IllegalStateException("Retention cleanup failed", e);
        }
    }
}
