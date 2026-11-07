import { readFile, readdir } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import type { Pool } from 'pg';

const MIGRATION_LOCK_ID = 0x61696777; // "aigw"

async function migrationDir(): Promise<URL> {
  return new URL('../migrations/', import.meta.url);
}

/**
 * Apply pending SQL migrations. Safe under concurrent startup: a session-level
 * advisory lock serialises migrators, and schema_migrations records versions.
 */
export async function migrate(pool: Pool): Promise<void> {
  const client = await pool.connect();
  let locked = false;
  try {
    await client.query('SELECT pg_advisory_lock($1)', [MIGRATION_LOCK_ID]);
    locked = true;
    await client.query(`
      CREATE TABLE IF NOT EXISTS schema_migrations (
        version text PRIMARY KEY,
        applied_at timestamptz NOT NULL DEFAULT now()
      )`);
    const directory=await migrationDir();
    const files=(await readdir(fileURLToPath(directory))).filter(f=>/^\d+.*\.sql$/.test(f)).sort();
    for(const file of files) {
      const version=file.replace(/\.sql$/,'');
      const applied=await client.query('SELECT 1 FROM schema_migrations WHERE version=$1',[version]);
      if(applied.rowCount)continue;
      const sql=await readFile(new URL(file,directory),'utf8');
      await client.query('BEGIN');
      try {
        await client.query(sql);
        await client.query('INSERT INTO schema_migrations(version) VALUES($1)',[version]);
        await client.query('COMMIT');
      }catch(err){await client.query('ROLLBACK');throw err;}
    }
  } finally {
    if (locked) {
      try {
        await client.query('SELECT pg_advisory_unlock($1)', [MIGRATION_LOCK_ID]);
      } catch {
        // connection close releases the lock anyway
      }
    }
    client.release();
  }
}
