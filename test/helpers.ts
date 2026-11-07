import { randomUUID } from 'node:crypto';
import { Pool } from 'pg';
import { migrate } from '../src/db.js';
import { loadConfig } from '../src/config.js';
export const hasServices=Boolean(process.env.DATABASE_URL&&process.env.REDIS_URL);
export async function sandbox() {
  const root=new Pool({connectionString:process.env.DATABASE_URL,max:2});
  const schema='test_'+randomUUID().replaceAll('-','');
  await root.query(`CREATE SCHEMA ${schema}`);
  const url=new URL(process.env.DATABASE_URL!);url.searchParams.set('options',`-c search_path=${schema}`);
  const pool=new Pool({connectionString:url.toString(),max:6});
  await migrate(pool);
  return {pool,config:loadConfig({DATABASE_URL:url.toString(),REDIS_URL:process.env.REDIS_URL,ENCRYPTION_KEY:'1'.repeat(64),ALLOW_PRIVATE_UPSTREAM:'true',DATA_PLANE_NAMESPACE:schema,HOT_DURABILITY:'memory'}),cleanup:async()=>{await pool.end();await root.query(`DROP SCHEMA ${schema} CASCADE`);await root.end();}};
}
