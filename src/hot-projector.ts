import { randomUUID } from 'node:crypto';
import type { Pool } from 'pg';
import type { Redis } from 'ioredis';
import { z } from 'zod';
import { HotStream } from './hot-stream.js';
import type { HotKeys,HotRequest } from './hot-contract.js';

const int=z.number().int().min(0).max(Number.MAX_SAFE_INTEGER);
const token=z.string().regex(/^[a-z][a-z0-9_]{0,99}$/);
const attempt=z.strictObject({attempt_no:int.min(1).max(2),target_id:z.uuid(),upstream_model:z.string().max(256),
  status:z.enum(['ok','error']),http_status:int.min(100).max(599).nullable(),error_code:token.nullable(),request_sent:z.boolean(),
  execution_unknown:z.boolean(),usage_known:z.boolean(),input_tokens:int.nullable(),output_tokens:int.nullable(),cost_micros:int.nullable(),
  upper_bound_micros:int,retry_safe:z.boolean(),latency_ms:z.number().min(0).max(86400000)});
const eventSchema=z.strictObject({version:z.literal(1),epoch:z.uuid(),revision:int.min(1),requestId:z.uuid(),projectId:z.uuid(),keyId:z.uuid(),
  period:z.string().regex(/^\d{4}-(0[1-9]|1[0-2])$/),reserveMicros:int,routeVersion:z.unknown(),priceSnapshot:z.unknown(),
  state:z.enum(['reserved','dispatched','settled','unknown','cancelled']),status:token.max(32),actualMicros:int.nullable(),chargedMicros:int.nullable(),
  inputTokens:int.nullable(),outputTokens:int.nullable(),aliasName:z.string().max(128).nullable(),errorCode:token.nullable(),
  attempts:z.array(attempt).max(2),createdAt:z.iso.datetime(),updatedAt:z.iso.datetime(),leaseUntil:int,owner:z.uuid(),remainingEvents:int.max(6)});

export async function projectHotRequest(pool:Pool,input:HotRequest):Promise<void>{
  const e=eventSchema.parse(input);
  if(new Set(e.attempts.map(a=>a.attempt_no)).size!==e.attempts.length)throw Error('Duplicate attempt number');
  const terminal=['settled','unknown','cancelled'].includes(e.state);
  if(e.state==='settled'&&(e.actualMicros===null||e.chargedMicros!==e.actualMicros)
    ||e.state==='unknown'&&(e.actualMicros!==null||e.chargedMicros!==e.reserveMicros)
    ||e.state==='cancelled'&&(e.actualMicros!==null||e.chargedMicros!==0)
    ||!terminal&&(e.actualMicros!==null||e.chargedMicros!==null))throw Error('Inconsistent financial snapshot');
  const c=await pool.connect();
  try{
    await c.query('BEGIN');
    await c.query('SELECT pg_advisory_xact_lock(hashtextextended($1,0))',[`gateway:billing:${e.projectId}`]);
    const revision=(await c.query('SELECT epoch::text,revision::text FROM hot_projection_state WHERE request_id=$1',[e.requestId])).rows[0];
    if(revision){
      if(revision.epoch!==e.epoch)throw Error('Projection epoch conflict');
      if(Number(revision.revision)>=e.revision){await c.query('COMMIT');return;}
    }
    await c.query(`INSERT INTO requests(request_id,project_id,key_id,period,reserve_micros,route_version,price_snapshot,status,created_at,updated_at)
      VALUES($1,$2,(SELECT id FROM api_keys WHERE id=$3),$4,$5,$6::jsonb,$7::jsonb,'reserved',$8,$9) ON CONFLICT(request_id) DO NOTHING`,
      [e.requestId,e.projectId,e.keyId,e.period,e.reserveMicros,JSON.stringify(e.routeVersion),JSON.stringify(e.priceSnapshot),e.createdAt,e.updatedAt]);
    const previous=(await c.query('SELECT project_id::text,key_id::text,period,reserve_micros::text,actual_micros::text,status FROM requests WHERE request_id=$1 FOR UPDATE',[e.requestId])).rows[0];
    if(previous.project_id!==e.projectId||previous.key_id&&previous.key_id!==e.keyId||previous.period!==e.period||Number(previous.reserve_micros)!==e.reserveMicros)throw Error('Projection request collision');
    const previousReservation=(await c.query('SELECT status FROM reservations WHERE request_id=$1',[e.requestId])).rows[0];
    if(previousReservation?.status==='cancelled'&&e.state!=='cancelled')throw Error('Cannot reopen cancelled request');
    if(previous.actual_micros!==null&&(e.actualMicros===null||Number(previous.actual_micros)!==e.actualMicros))throw Error('Actual settlement is immutable');
    const baseline=(await c.query('SELECT amount_micros::text,estimated,created_at FROM ledger WHERE event_id=ANY($1::text[]) ORDER BY event_id DESC',
      [[`settle:${e.requestId}`,`estimate:${e.requestId}`]])).rows[0];
    if(baseline&&!terminal)throw Error('Cannot regress a charged request');
    let eventType:string|null=null,eventId='';
    if(e.state==='cancelled'){
      if(baseline)throw Error('Cannot cancel charged request');
      await c.query(`INSERT INTO ledger(event_id,request_id,project_id,period,amount_micros,kind,estimated,created_at)
        VALUES($1,$2,$3,$4,0,'release',false,$5) ON CONFLICT(event_id) DO NOTHING`,[`release:${e.requestId}`,e.requestId,e.projectId,e.period,e.createdAt]);
      eventType='request.cancelled';eventId=`request.cancelled:${e.requestId}`;
    }else if(terminal){
      if(!baseline){
        await c.query(`INSERT INTO ledger(event_id,request_id,project_id,period,amount_micros,kind,input_tokens,output_tokens,estimated,created_at)
          VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`,[`settle:${e.requestId}`,e.requestId,e.projectId,e.period,e.chargedMicros,
            e.state==='unknown'?'estimate':'settle',e.inputTokens,e.outputTokens,e.state==='unknown',e.createdAt]);
        eventType='request.settled';eventId=`request.settled:${e.requestId}`;
      }else if(baseline.estimated&&e.actualMicros!==null&&previous.actual_micros===null){
        await c.query(`INSERT INTO ledger(event_id,request_id,project_id,period,amount_micros,kind,input_tokens,output_tokens,estimated,created_at)
          VALUES($1,$2,$3,$4,$5,'adjust',$6,$7,false,GREATEST($8::timestamptz,$9::timestamptz+interval '1 microsecond')) ON CONFLICT(event_id) DO NOTHING`,
          [`adjust:${e.requestId}:${e.actualMicros}`,e.requestId,e.projectId,e.period,e.actualMicros-Number(baseline.amount_micros),e.inputTokens,e.outputTokens,e.updatedAt,baseline.created_at]);
        eventType='request.adjusted';eventId=`request.adjusted:${e.requestId}:${e.actualMicros}`;
      }else if(!baseline.estimated&&Number(baseline.amount_micros)!==e.chargedMicros)throw Error('Conflicting ledger baseline');
    }
    await c.query(`UPDATE requests SET status=$2,actual_micros=$3,input_tokens=$4,output_tokens=$5,alias_name=$6,error_code=$7,updated_at=$8 WHERE request_id=$1`,
      [e.requestId,e.status,e.actualMicros,e.inputTokens,e.outputTokens,e.aliasName,e.errorCode,e.updatedAt]);
    await c.query(`INSERT INTO reservations(request_id,project_id,period,reserved_micros,actual_micros,status,lease_expires_at,created_at,updated_at)
      VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9) ON CONFLICT(request_id) DO UPDATE SET actual_micros=excluded.actual_micros,status=excluded.status,
      lease_expires_at=excluded.lease_expires_at,updated_at=excluded.updated_at`,
      [e.requestId,e.projectId,e.period,e.reserveMicros,e.actualMicros,e.state,terminal?null:new Date(e.leaseUntil),e.createdAt,e.updatedAt]);
    for(const a of e.attempts){
      const finished=new Date(e.updatedAt),started=new Date(+finished-a.latency_ms);
      await c.query(`INSERT INTO attempts(id,request_id,attempt_no,target_id,status,error_code,started_at,finished_at,input_tokens,output_tokens,cost_micros)
        VALUES($1,$2,$3,(SELECT id FROM model_targets WHERE id=$4),$5,$6,$7,$8,$9,$10,$11)
        ON CONFLICT(request_id,attempt_no) DO UPDATE SET status=excluded.status,error_code=excluded.error_code,finished_at=excluded.finished_at,
        input_tokens=excluded.input_tokens,output_tokens=excluded.output_tokens,cost_micros=excluded.cost_micros`,
        [randomUUID(),e.requestId,a.attempt_no,a.target_id,a.status,a.error_code,started,finished,a.input_tokens,a.output_tokens,a.cost_micros]);
    }
    if(eventType)await c.query(`INSERT INTO outbox(id,event_id,event_type,project_id,payload,status,created_at)
      VALUES($1,$2,$3,$4,$5::jsonb,'pending',$6) ON CONFLICT(event_id) DO NOTHING`,[randomUUID(),eventId,eventType,e.projectId,
        JSON.stringify({requestId:e.requestId,projectId:e.projectId,period:e.period,status:e.status,actualMicros:e.actualMicros,
          chargedMicros:e.chargedMicros,estimated:e.state==='unknown',inputTokens:e.inputTokens,outputTokens:e.outputTokens,attempts:e.attempts}),e.updatedAt]);
    await c.query('UPDATE api_keys SET last_used_at=GREATEST(COALESCE(last_used_at,$2::timestamptz),$2::timestamptz) WHERE id=$1',[e.keyId,e.updatedAt]);
    await c.query(`INSERT INTO hot_projection_state(request_id,epoch,revision) VALUES($1,$2,$3)
      ON CONFLICT(request_id) DO UPDATE SET revision=excluded.revision,updated_at=now()`,[e.requestId,e.epoch,e.revision]);
    await c.query('COMMIT');
  }catch(error){await c.query('ROLLBACK').catch(()=>{});throw error;}finally{c.release();}
}

export class HotProjector {
  private stream:HotStream;
  private running:Promise<number>|null=null;
  private stopped=false;
  constructor(private pool:Pool,private redis:Redis,private keys:HotKeys){this.stream=new HotStream(redis,keys.journal);}
  tick():Promise<number>{
    if(this.running)return this.running;
    if(this.stopped)return Promise.resolve(0);
    const run=(async()=>{
      const entries=await this.stream.read(50);
      const latest=new Map<string,{event:HotRequest;ids:string[]}>();
      for(const entry of entries){
        const event=eventSchema.parse(JSON.parse(entry.data));
        const previous=latest.get(event.requestId);
        if(previous&&previous.event.epoch!==event.epoch)throw Error('Projection epoch conflict');
        const ids=[...(previous?.ids||[]),entry.id];
        latest.set(event.requestId,{event:previous&&previous.event.revision>event.revision?previous.event:event,ids});
      }
      for(const {event,ids} of latest.values()){await projectHotRequest(this.pool,event);await this.stream.ack(ids);}
      return entries.length;
    })();
    this.running=run;return run.finally(()=>{this.running=null;});
  }
  async drain(timeoutMs=10000){
    const until=Date.now()+timeoutMs;
    while(Number(await this.redis.xlen(this.keys.journal))){
      if(this.stopped||Date.now()>=until)throw Error('PG projection drain timed out; uncommitted events retained');
      await this.tick();await new Promise(r=>setTimeout(r,5));
    }
  }
  async close(){this.stopped=true;await this.running?.catch(()=>{});}
}
