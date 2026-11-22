package com.jihuayu.aigateway.db;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;
import javax.sql.DataSource;

public final class DatabaseMigrator {
    private static final List<String> MIGRATION_FILES = List.of(
        "001.sql",
        "002_integrity.sql",
        "003_billing_state.sql",
        "004_hot_projection.sql",
        "005_single_hot_owner.sql",
        "006_incremental_usage.sql"
    );

    private DatabaseMigrator() {}

    public static void migrate(DataSource dataSource) {
        try (Connection conn = dataSource.getConnection()) {
            boolean oldAutoCommit = conn.getAutoCommit();
            conn.setAutoCommit(false);
            try {
                try (Statement st = conn.createStatement()) {
                    st.execute("SELECT pg_advisory_xact_lock(2147483001)");
                    st.execute("""
                        CREATE TABLE IF NOT EXISTS schema_migrations (
                          version text PRIMARY KEY,
                          applied_at timestamptz NOT NULL DEFAULT now()
                        )
                        """);
                }
                for (String file : MIGRATION_FILES) {
                    boolean applied;
                    try (PreparedStatement ps = conn.prepareStatement("SELECT 1 FROM schema_migrations WHERE version = ?")) {
                        ps.setString(1, file);
                        try (ResultSet rs = ps.executeQuery()) {
                            applied = rs.next();
                        }
                    }
                    if (!applied) {
                        String sql = readMigrationSql(file);
                        try (Statement st = conn.createStatement()) {
                            st.execute(sql);
                        }
                        try (PreparedStatement ps = conn.prepareStatement("INSERT INTO schema_migrations (version) VALUES (?)")) {
                            ps.setString(1, file);
                            ps.executeUpdate();
                        }
                    }
                }
                conn.commit();
            } catch (Exception e) {
                conn.rollback();
                throw e;
            } finally {
                conn.setAutoCommit(oldAutoCommit);
            }
        } catch (Exception e) {
            throw new IllegalStateException("Database migration failed: " + e.getMessage(), e);
        }
    }

    private static String readMigrationSql(String file) {
        String resourcePath = "/migrations/" + file;
        try (InputStream in = DatabaseMigrator.class.getResourceAsStream(resourcePath)) {
            if (in == null) {
                throw new IllegalStateException("Missing migration resource: " + resourcePath);
            }
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to read migration " + file, e);
        }
    }
}
