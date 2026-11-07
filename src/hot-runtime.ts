import { createHash,randomUUID } from 'node:crypto';
import type { Pool,PoolClient } from 'pg';
import type { Redis } from 'ioredis';
import type { FastifyRequest } from 'fastify';
import { bearerToken,keyContextFromRow,loadRoute,GatewayError,type KeyRow,type KeyContext,type RouteSnapshot } from './gateway.js';
import { hashKey } from './security.js';
import type { HotKeys } from './hot-contract.js';

interface Snapshot {
  epoch:string;
  routes:Record<string,RouteSnapshot>;
  models:Record<string,Array<Record<string,unknown>>>;
}
const READ_KEYS=`SELECT k.key_hash,k.id AS key_id,k.prefix,k.allowed_models,k.expires_at,k.revoked_at,k.key_rpm,
  p.id AS project_id,p.workspace_id,p.rpm AS project_rpm,p.concurrency,p.budget_micros::text,p.max_output_tokens
  FROM api_keys k JOIN projects p ON p.id=k.project_id ORDER BY k.id`;
const PUBLISH=`
local run=string.match(redis.call('INFO','server'),'run_id:([%w]+)')
if redis.call('HGET',KEYS[1],'epoch')~=ARGV[1] or redis.call('HGET',KEYS[1],'run_id')~=run then return redis.error_reply('HOT_FENCED') end
local seeds=cjson.decode(ARGV[4])
for id,data in pairs(seeds) do
  local key=ARGV[5]..'project:'..id
  if redis.call('SISMEMBER',KEYS[4],id)==1 then
    if redis.call('EXISTS',key)==0 then return redis.error_reply('HOT_PROJECT_LOST') end
  elseif redis.call('EXISTS',key)==1 then return redis.error_reply('HOT_ORPHAN_PROJECT') end
end
for id,data in pairs(seeds) do
  if redis.call('SISMEMBER',KEYS[4],id)==0 then redis.call('SET',ARGV[5]..'project:'..id,data);redis.call('SADD',KEYS[4],id) end
end
redis.call('SET',KEYS[3],ARGV[3],'EX',86400)
if redis.call('HGET',KEYS[2],'revision')==ARGV[2] then return 0 end
redis.call('DEL',KEYS[2])
redis.call('HSET',KEYS[2],'revision',ARGV[2])
local entries=cjson.decode(ARGV[6])
for key,value in pairs(entries) do redis.call('HSET',KEYS[2],'key:'..key,cjson.encode(value)) end
return 1`;

/** PG is used only during startup/config publication. Request methods below own no PG fallback. */
export class HotRuntime {
  private cache=new Map<string,Snapshot>();
  epoch='';
  constructor(private pool:Pool,private redis:Redis,readonly keys:HotKeys,private namespace:string,
    private durability:'aof'|'memory'='aof'){}

  async initialize():Promise<void>{
    if(this.durability==='aof'){
      const values=await this.redis.config('GET','appendonly','appendfsync','maxmemory-policy') as string[];
      const cfg=Object.fromEntries(Array.from({length:values.length/2},(_,i)=>[values[i*2],values[i*2+1]]));
      if(cfg.appendonly!=='yes'||cfg.appendfsync!=='always'||cfg['maxmemory-policy']!=='noeviction')
        throw Error('Redis hot ledger requires appendonly=yes, appendfsync=always, maxmemory-policy=noeviction');
    }
    const c=await this.pool.connect();
    try{
      await c.query('BEGIN');
      await c.query('SELECT pg_advisory_xact_lock(hashtextextended($1,0))',[`gateway:hot-config:${this.namespace}`]);
      const stored=(await c.query('SELECT epoch::text FROM hot_runtime_manifests WHERE namespace=$1',[this.namespace])).rows[0];
      const existing=await this.redis.hget(this.keys.runtime,'epoch');
      const run=(await this.redis.info('server')).match(/run_id:([^\r\n]+)/)?.[1];
      if(!run)throw Error('Redis identity unavailable');
      if(stored){
        if(existing!==stored.epoch)throw Error('HOT_STATE_LOST: restore complete Redis journal; PG may lag, automatic reset refused');
        if(await this.redis.hget(this.keys.runtime,'run_id')!==run)throw Error('HOT_FENCED: Redis identity changed; verify durable journal and isolate old primary before recovery');
        this.epoch=stored.epoch;
      }else{
        if((await c.query('SELECT 1 FROM hot_runtime_manifests LIMIT 1')).rowCount)
          throw Error('HOT_NAMESPACE_CHANGE_REFUSED: this PG schema already owns another financial namespace');
        if(existing)throw Error('HOT_ORPHAN_STATE: Redis namespace belongs to another manifest');
        const live=await c.query("SELECT 1 FROM requests WHERE status IN('preparing','reserved','dispatch_intent','dispatched') LIMIT 1");
        if(live.rowCount)throw Error('Drain legacy in-flight requests before enabling the Redis data plane');
        this.epoch=randomUUID();
        await c.query('INSERT INTO hot_runtime_manifests(namespace,epoch) VALUES($1,$2)',[this.namespace,this.epoch]);
        const created=await this.redis.eval("if redis.call('EXISTS',KEYS[1])==1 or redis.call('EXISTS',KEYS[2])==1 or redis.call('EXISTS',KEYS[3])==1 then return 0 end;redis.call('HSET',KEYS[1],'epoch',ARGV[1],'run_id',ARGV[2],'journal_tail','0-0');redis.call('SET',KEYS[2],0);redis.call('XGROUP','CREATE',KEYS[3],'pg-hot-v1','0','MKSTREAM');return 1",3,this.keys.runtime,this.keys.slots,this.keys.journal,this.epoch,run);
        if(Number(created)!==1)throw Error('Concurrent Redis namespace initialization');
      }
      await this.publishInside(c);
      await c.query('COMMIT');
    }catch(e){await c.query('ROLLBACK').catch(()=>{});throw e;}finally{c.release();}
  }

  async publish():Promise<void>{
    const c=await this.pool.connect();
    try{
      // Lock first, then establish a fresh repeatable snapshot. Other publishers
      // cannot overwrite a newer publication with an older DB snapshot.
      await c.query('SELECT pg_advisory_lock(hashtextextended($1,0))',[`gateway:hot-config:${this.namespace}`]);
      await c.query('BEGIN ISOLATION LEVEL REPEATABLE READ');
      await this.publishInside(c);
      await c.query('COMMIT');
    }catch(e){await c.query('ROLLBACK').catch(()=>{});throw e;}finally{
      let unlockError:Error|undefined;
      try{await c.query('SELECT pg_advisory_unlock(hashtextextended($1,0))',[`gateway:hot-config:${this.namespace}`]);}
      catch(e){unlockError=e instanceof Error?e:new Error('Publisher unlock failed');}
      // A possibly locked session must never return to the idle pool.
      c.release(unlockError);
    }
  }

  private async publishInside(c:PoolClient){
    const projects=(await c.query('SELECT id,workspace_id FROM projects ORDER BY id')).rows;
    const seeds:Record<string,unknown>={};
    for(const project of projects){
      const known=await this.redis.sismember(this.keys.projects,project.id);
      if(known){seeds[project.id]='';continue;}
      const totals=(await c.query('SELECT period,sum(amount_micros)::text AS spent FROM ledger WHERE project_id=$1 GROUP BY period',[project.id])).rows;
      const budgets:Record<string,{spent:number;reserved:number}>={};
      for(const row of totals){const spent=Number(row.spent);if(!Number.isSafeInteger(spent)||spent<0)throw Error('Invalid ledger bootstrap total');budgets[row.period]={spent,reserved:0};}
      seeds[project.id]=JSON.stringify({epoch:this.epoch,budgets,leases:{}});
    }
    const entries:Record<string,KeyRow>={};
    for(const {key_hash,...row} of (await c.query(READ_KEYS)).rows)entries[key_hash]=row;
    const snapshot:Snapshot={epoch:this.epoch,routes:{},models:{}};
    for(const project of projects){
      const aliases=(await c.query(`SELECT a.id,a.name,a.created_at FROM aliases a JOIN route_versions rv ON rv.id=a.current_version_id
        WHERE a.project_id=$1 AND rv.status='published' ORDER BY a.name`,[project.id])).rows;
      snapshot.models[project.id]=aliases;
      for(const alias of aliases){const route=await loadRoute(c,project.id,project.workspace_id,alias.name);if(route)snapshot.routes[project.id+':'+alias.name]=route;}
    }
    const json=JSON.stringify(snapshot);
    const revision=createHash('sha256').update(JSON.stringify([snapshot,entries,projects])).digest('hex');
    await this.redis.eval(PUBLISH,4,this.keys.runtime,this.keys.config,this.keys.snapshot(revision),this.keys.projects,
      this.epoch,revision,json,JSON.stringify(seeds),this.keys.prefix,JSON.stringify(entries));
    this.remember(revision,snapshot);
  }

  private remember(revision:string,snapshot:Snapshot){
    this.cache.set(revision,snapshot);
    if(this.cache.size>4)this.cache.delete(this.cache.keys().next().value!);
  }
  private async snapshot(key:KeyContext):Promise<Snapshot>{
    const revision=key.runtimeRevision;
    if(!revision)throw new GatewayError(503,'configuration_unavailable','运行配置未就绪。');
    let value=this.cache.get(revision);
    if(!value){
      let json:string|null;
      try{json=await this.redis.get(this.keys.snapshot(revision));}catch{throw new GatewayError(503,'configuration_unavailable','运行配置暂不可用。');}
      if(!json)throw new GatewayError(503,'configuration_unavailable','运行配置快照缺失。');
      try{value=JSON.parse(json) as Snapshot;}catch{throw new GatewayError(503,'configuration_unavailable','运行配置快照损坏。');}
      if(value.epoch!==this.epoch||!value.routes||!value.models)throw new GatewayError(503,'configuration_unavailable','运行配置代次不匹配。');
      this.remember(revision,value);
    }
    return value;
  }
  async authenticateKey(req:FastifyRequest):Promise<KeyContext>{
    const keyHash=hashKey(bearerToken(req));
    let fields:Array<string|null>;
    try{fields=await this.redis.eval(`local run=string.match(redis.call('INFO','server'),'run_id:([%w]+)');
      if redis.call('HGET',KEYS[1],'epoch')~=ARGV[1] or redis.call('HGET',KEYS[1],'run_id')~=run then return {false,false} end;
      return redis.call('HMGET',KEYS[2],'revision','key:'..ARGV[2])`,2,this.keys.runtime,this.keys.config,this.epoch,keyHash) as Array<string|null>;
    }catch{throw new GatewayError(503,'configuration_unavailable','运行配置暂不可用。');}
    const [revision,json]=fields;
    if(!revision)throw new GatewayError(503,'configuration_unavailable','运行配置未就绪。');
    const row:KeyRow|undefined=json?JSON.parse(json):undefined;
    return {...keyContextFromRow(row),runtimeRevision:revision,runtimeKeyHash:keyHash,
      runtimeExpiresAt:row?.expires_at?+new Date(row.expires_at):null};
  }
  async loadRoute(key:KeyContext,model:string):Promise<RouteSnapshot|null>{
    const route=(await this.snapshot(key)).routes[key.projectId+':'+model];return route?structuredClone(route):null;
  }
  async models(key:KeyContext){return structuredClone((await this.snapshot(key)).models[key.projectId]||[]);}
}
