/** Offline drain only. This command NEVER rebinds Redis identity or reopens admission. */
import { Pool } from 'pg';
import { Redis } from 'ioredis';
import { loadConfig } from '../src/config.js';
import { migrate } from '../src/db.js';
import { hotKeys } from '../src/hot-contract.js';
import { HotProjector } from '../src/hot-projector.js';
const cfg=loadConfig(),keys=hotKeys(cfg.DATA_PLANE_NAMESPACE);
const pool=new Pool({connectionString:cfg.DATABASE_URL,max:2,connectionTimeoutMillis:2000,statement_timeout:10000});
pool.on('error',()=>{});
const redis=new Redis(cfg.REDIS_URL,{maxRetriesPerRequest:1,commandTimeout:5000});redis.on('error',()=>{});
let projector:HotProjector|undefined;
try{
  await migrate(pool);
  const manifest=(await pool.query('SELECT epoch::text FROM hot_runtime_manifests WHERE namespace=$1',[cfg.DATA_PLANE_NAMESPACE])).rows[0];
  if(!manifest||await redis.hget(keys.runtime,'epoch')!==manifest.epoch)throw Error('Financial manifest missing/mismatched; restore the complete source first');
  const continuous=await redis.eval(`if redis.call('TYPE',KEYS[2]).ok~='stream' then return 0 end;
    local info=redis.call('XINFO','STREAM',KEYS[2]);for i=1,#info,2 do if info[i]=='last-generated-id' and info[i+1]==redis.call('HGET',KEYS[1],'journal_tail') then return 1 end end;return 0`,2,keys.runtime,keys.journal);
  if(Number(continuous)!==1)throw Error('Journal continuity check failed; refusing to certify an empty/missing queue');
  projector=new HotProjector(pool,redis,keys,manifest.epoch);await projector.drain(60000);
  console.log(JSON.stringify({drained:true,admissionIdentityChanged:false,namespace:cfg.DATA_PLANE_NAMESPACE,
    scope:'known retained journal only; verify complete AOF/replication source separately before any recovery authorization'}));
}finally{await projector?.close();await redis.quit().catch(()=>redis.disconnect());await pool.end();}
