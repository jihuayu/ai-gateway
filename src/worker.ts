import { Pool } from 'pg';
import { loadConfig } from './config.js';
import { migrate } from './db.js';
import { EventWorker } from './events.js';
const cfg=loadConfig();
const pool=new Pool({connectionString:cfg.DATABASE_URL,max:4});
await migrate(pool);
const worker=new EventWorker(pool,cfg.KAFKA_BROKERS.split(',').filter(Boolean));
await worker.start();
for(const s of ['SIGINT','SIGTERM'])process.once(s,()=>{void worker.close();});
try{await worker.run();}finally{await worker.close();await pool.end();}
