import { Pool } from 'pg';
import { loadConfig } from './config.js';
import { migrate } from './db.js';
import { EventWorker } from './events.js';
import { Redis } from 'ioredis';
import { HotRuntime } from './hot-runtime.js';
import { HotBilling } from './hot-billing.js';
import { HotProjector } from './hot-projector.js';
import { hotKeys } from './hot-contract.js';
const cfg=loadConfig();
const pool=new Pool({connectionString:cfg.DATABASE_URL,max:4,connectionTimeoutMillis:2000,statement_timeout:10000});
pool.on('error',()=>console.error('Idle PostgreSQL connection unavailable; journal retained'));
const redis=new Redis(cfg.REDIS_URL,{maxRetriesPerRequest:1,enableOfflineQueue:false});redis.on('error',()=>{});
if(redis.status!=='ready')await new Promise<void>((resolve,reject)=>{redis.once('ready',resolve);redis.once('error',reject);});
await migrate(pool);
const keys=hotKeys(cfg.DATA_PLANE_NAMESPACE),runtime=new HotRuntime(pool,redis,keys,cfg.DATA_PLANE_NAMESPACE,cfg.HOT_DURABILITY);
await runtime.initialize();
const billing=new HotBilling(redis,keys,runtime.epoch,cfg.HOT_JOURNAL_CAPACITY);await billing.initialize();
const projector=new HotProjector(pool,redis,keys);
const worker=new EventWorker(pool,cfg.KAFKA_BROKERS.split(',').filter(Boolean));
await worker.start();
let stopped=false,lastPublish=Date.now();
for(const s of ['SIGINT','SIGTERM'])process.once(s,()=>{stopped=true;});
try{
  while(!stopped){
    try{
      await billing.reap();
      const full=await pool.query("SELECT 1 FROM outbox WHERE status!='published' OFFSET 9999 LIMIT 1");
      await redis.hset(keys.runtime,'projection_paused',full.rowCount?'1':'0');
      if(!full.rowCount)await projector.tick();
      await worker.tick();
      if(Date.now()-lastPublish>=10000){await runtime.publish();lastPublish=Date.now();}
    }catch{console.error('Background projection failed; uncommitted journal retained');}
    await new Promise(r=>setTimeout(r,100));
  }
}finally{await billing.close();await projector.close();await worker.close();await redis.quit().catch(()=>redis.disconnect());await pool.end();}
