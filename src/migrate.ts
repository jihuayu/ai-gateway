import { Pool } from 'pg';
import { migrate } from './db.js';
if(!process.env.DATABASE_URL) throw Error('DATABASE_URL is required');
const pool=new Pool({connectionString:process.env.DATABASE_URL,max:1});
try {await migrate(pool);console.log('Migrations applied');} finally {await pool.end();}
