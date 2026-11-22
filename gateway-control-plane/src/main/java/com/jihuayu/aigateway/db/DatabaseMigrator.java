package com.jihuayu.aigateway.db;

import java.sql.Connection;
import java.sql.Statement;
import javax.sql.DataSource;
import org.flywaydb.core.Flyway;

/**
 * Schema migration coordinator powered by Flyway (classpath:db/migration/V1..V6).
 */
public final class DatabaseMigrator {

    private DatabaseMigrator() {}

    public static void migrate(DataSource dataSource) {
        try (Connection conn = dataSource.getConnection();
             Statement st = conn.createStatement()) {
            st.execute("SELECT pg_advisory_lock(2147483001)");
            try {
                Flyway.configure()
                    .dataSource(dataSource)
                    .locations("classpath:db/migration")
                    .baselineOnMigrate(true)
                    .baselineVersion("0")
                    .load()
                    .migrate();
            } finally {
                st.execute("SELECT pg_advisory_unlock(2147483001)");
            }
        } catch (Exception e) {
            throw new IllegalStateException("Flyway database migration failed: " + e.getMessage(), e);
        }
    }
}
