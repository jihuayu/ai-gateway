import { test } from 'node:test';
import assert from 'node:assert/strict';
import { Redis } from 'ioredis';
import { buildApp } from '../src/app.js';
import { hasServices,sandbox } from './helpers.js';
import { mockUpstream } from './mock-upstream.js';
import { randomUUID } from 'node:crypto';
import { HotRuntime } from '../src/hot-runtime.js';
import { HotBilling } from '../src/hot-billing.js';
import { HotStream } from '../src/hot-stream.js';
import { projectHotRequest } from '../src/hot-projector.js';
import { prepareRequest,validateChatBody,admitRequest } from '../src/gateway.js';
import { consumeEvent } from '../src/events.js';
import type { FastifyRequest } from 'fastify';
import { createServer,connect,type Socket } from 'node:net';

async function fixture(capacity=10000,connection?:(url:string)=>string){
  const db=await sandbox(),upstream=await mockUpstream();
  const server=await buildApp({...db.config,...(connection?{DATABASE_URL:connection(db.config.DATABASE_URL)}:{}),HOT_JOURNAL_CAPACITY:capacity},false,{backgroundWorkers:false});
  const {app,pool,backgroundPool,redis,billing,runtime,projector}=server;
  const address=await app.listen({host:'127.0.0.1',port:0});
  const first=await app.inject({method:'POST',url:'/api/bootstrap',payload:{email:'hot@example.com',password:'hot-test-password',projectName:'hot'}});
  assert.equal(first.statusCode,200,first.body);const project=first.json().project.id;
  const headers={cookie:first.cookies.map(c=>`${c.name}=${c.value}`).join('; '),'x-csrf-token':first.json().csrf};
  const post=async(url:string,payload:Record<string,unknown>)=>{const response=await app.inject({method:'POST',url,headers,payload});assert.equal(response.statusCode,200,response.body);return response.json();};
  const provider=await post('/api/providers',{name:'hot',base_url:upstream.url,secret:'mock-secret'});
  const targets:Record<string,string>={};
  for(const model of ['mock-model','partial','limited'])targets[model]=(await post(`/api/providers/${provider.id}/targets`,{upstream_model:model,input_price:1000000,output_price:2000000,capabilities:['chat','stream']})).id;
  for(const [name,models]of Object.entries({chat:['mock-model'],fallback:['limited','mock-model'],partial:['partial']})){
    const alias=await post('/api/aliases',{project_id:project,name,targets:models.map(m=>targets[m]),capabilities:['chat','stream']});
    await post(`/api/aliases/${alias.id}/publish`,{expectedVersion:1});
  }
  const key=await post('/api/keys',{project_id:project,name:'hot',allowed_models:['chat','fallback','partial']});
  const auth={authorization:`Bearer ${key.key}`};
  const send=async(model='chat',stream=false)=>{
    const response=await fetch(address+'/v1/chat/completions',{method:'POST',headers:{...auth,'content-type':'application/json'},
      body:JSON.stringify({model,messages:[{role:'user',content:'hot-private-prompt-marker'}],max_tokens:16,stream}),signal:AbortSignal.timeout(10000)});
    const body=await response.text(),id=response.headers.get('x-request-id')!;
    return {response,body,id};
  };
  const finished=async(id:string)=>{
    for(let i=0;i<200;i++){const raw=await redis.get(runtime.keys.request(id));if(raw){const row=JSON.parse(raw);if(['settled','unknown','cancelled'].includes(row.state))return row;}await new Promise(r=>setTimeout(r,5));}
    throw Error('Redis finalization did not finish');
  };
  const close=async()=>{
    await app.close();await upstream.close();
    const clean=new Redis(process.env.REDIS_URL!);let cursor='0';
    do{const [next,keys]=await clean.scan(cursor,'MATCH',runtime.keys.prefix+'*','COUNT',100);cursor=next;if(keys.length)await clean.del(...keys);}while(cursor!=='0');
    await clean.quit();await db.cleanup();
  };
  return {...server,db,upstream,address,project,headers,auth,key,provider,targets,post,send,finished,close};
}
async function prepared(f:Awaited<ReturnType<typeof fixture>>){
  const key=await f.runtime.authenticateKey({headers:f.auth} as FastifyRequest),route=await f.runtime.loadRoute(key,'chat');assert.ok(route);
  return prepareRequest(key,validateChatBody({model:'chat',messages:[{role:'user',content:'not stored'}],max_tokens:16}),route);
}

function forbidSql(pools:unknown[]){
  let calls=0;const originals:Array<[any,any,any]>=[];
  for(const raw of pools){const pool=raw as any;originals.push([pool,pool.query,pool.connect]);
    pool.query=()=>{calls++;throw Error('PG unavailable: hot path must not call query');};
    pool.connect=()=>{calls++;throw Error('PG unavailable: hot path must not connect');};
  }
  return {get calls(){return calls;},restore(){for(const [pool,query,connect]of originals){pool.query=query;pool.connect=connect;}}};
}

test('default data plane serves buffered/SSE/fallback/partial/models with zero SQL while PG is unavailable',{skip:!hasServices,timeout:60000},async()=>{
  const f=await fixture();let guard:ReturnType<typeof forbidSql>|undefined;
  try{
    guard=forbidSql([f.pool,f.backgroundPool]);
    const models=await f.app.inject({url:'/v1/models',headers:f.auth});assert.equal(models.statusCode,200,models.body);assert.equal(models.json().data.length,3);
    assert.equal((await f.app.inject({url:'/v1/models'})).statusCode,401);
    assert.equal((await f.app.inject({method:'POST',url:'/v1/chat/completions',headers:f.auth,payload:{model:'chat',messages:[]}})).statusCode,400);
    for(const [model,stream]of [['chat',false],['chat',true],['fallback',false],['partial',true]] as const){
      const r=await f.send(model,stream);assert.equal(r.response.status,200,r.body);const saved=await f.finished(r.id);
      if(model==='partial'){assert.equal(saved.state,'unknown');assert.ok(saved.errorCode);assert.doesNotMatch(r.body,/\[DONE\]/);}
      else assert.equal(saved.actualMicros,20);
    }
    assert.equal((await f.app.inject({url:'/health/ready'})).statusCode,200);
    assert.equal(guard.calls,0);guard.restore();guard=undefined;
    assert.equal((await f.db.pool.query('SELECT count(*)::int AS n FROM requests')).rows[0].n,0,'HTTP completion does not wait for PG');
    assert.ok(await f.redis.xlen(f.runtime.keys.journal));
    const usage=await f.app.inject({url:`/api/usage?project_id=${f.project}`,headers:f.headers});assert.equal(usage.statusCode,200,usage.body);
    assert.ok(Number(usage.json().budget.spent_micros)>0,'budget display reads live Redis state while PG projection is pending');
    assert.equal(usage.json().stale,true);assert.ok(usage.json().ingestion_pending_events>0);
    await f.projector.drain();
    assert.equal((await f.db.pool.query('SELECT count(*)::int AS n FROM requests')).rows[0].n,4);
    assert.equal((await f.db.pool.query('SELECT count(*)::int AS n FROM ledger')).rows[0].n,4);
    const rows=(await f.db.pool.query('SELECT * FROM requests')).rows;
    assert.ok(!JSON.stringify(rows).includes('hot-private-prompt-marker'));
    assert.ok(rows.find(r=>r.alias_name==='partial').error_code);
  }finally{guard?.restore();await f.close();}
});

test('bounded journal rejects new work but preserves accepted request finalization, then resumes after PG projection',{skip:!hasServices,timeout:60000},async()=>{
  const f=await fixture(7);let guard:ReturnType<typeof forbidSql>|undefined;
  try{
    guard=forbidSql([f.pool,f.backgroundPool]);
    const first=await f.send();assert.equal(first.response.status,200,first.body);await f.finished(first.id);
    const rejected=await f.send();assert.equal(rejected.response.status,503,rejected.body);assert.match(rejected.body,/event_backlog/);
    assert.equal(f.upstream.calls,1);assert.equal(guard.calls,0);assert.equal(await f.redis.get(f.runtime.keys.slots),'0');
    assert.ok(await f.redis.xlen(f.runtime.keys.journal)<=7);
    guard.restore();guard=undefined;await f.projector.drain();
    const next=await f.send();assert.equal(next.response.status,200,next.body);await f.finished(next.id);await f.projector.drain();
    assert.equal((await f.db.pool.query('SELECT sum(amount_micros)::text AS n FROM ledger')).rows[0].n,'40');
  }finally{guard?.restore();await f.close();}
});

test('a second instance loads new snapshots from Redis without SQL and immediately observes provider disable and key revocation',{skip:!hasServices,timeout:60000},async()=>{
  const f=await fixture();const second=await buildApp(f.db.config,false,{backgroundWorkers:false});let guard:ReturnType<typeof forbidSql>|undefined;
  try{
    assert.equal((await second.app.inject({url:'/v1/models',headers:f.auth})).statusCode,200);
    const disabled=await f.app.inject({method:'PATCH',url:`/api/providers/${f.provider.id}`,headers:f.headers,payload:{enabled:false}});assert.equal(disabled.statusCode,200,disabled.body);
    guard=forbidSql([f.pool,f.backgroundPool,second.pool,second.backgroundPool]);
    const body={model:'chat',messages:[{role:'user',content:'x'}],max_tokens:16};
    const unavailable=await second.app.inject({method:'POST',url:'/v1/chat/completions',headers:f.auth,payload:body});
    assert.equal(unavailable.statusCode,503,unavailable.body);assert.match(unavailable.body,/model_unavailable/);assert.equal(f.upstream.calls,0);
    assert.equal(guard.calls,0);guard.restore();guard=undefined;
    await f.post(`/api/keys/${f.key.id}/revoke`,{});
    guard=forbidSql([f.pool,f.backgroundPool,second.pool,second.backgroundPool]);
    for(const url of ['/v1/models','/v1/chat/completions']){
      const r=await second.app.inject({method:url.endsWith('/models')?'GET':'POST',url,headers:f.auth,...(url.endsWith('/models')?{}:{payload:body})});
      assert.equal(r.statusCode,401,r.body);
    }
    assert.equal(guard.calls,0);
  }finally{guard?.restore();await second.app.close();await f.close();}
});

test('PG transaction failure retains the journal and retries atomically without duplicate ledger rows',{skip:!hasServices,timeout:60000},async()=>{
  const f=await fixture();
  try{
    const r=await f.send();assert.equal(r.response.status,200);await f.finished(r.id);
    await f.db.pool.query("ALTER TABLE outbox ADD CONSTRAINT injected_projection_failure CHECK(event_type!='request.settled')");
    await assert.rejects(()=>f.projector.tick());
    assert.equal((await f.db.pool.query('SELECT count(*)::int AS n FROM ledger')).rows[0].n,0);
    assert.equal((await f.db.pool.query('SELECT count(*)::int AS n FROM hot_projection_state')).rows[0].n,0);
    assert.equal((await f.db.pool.query('SELECT count(*)::int AS n FROM requests')).rows[0].n,0);
    assert.ok(await f.redis.xlen(f.runtime.keys.journal));
    await f.db.pool.query('ALTER TABLE outbox DROP CONSTRAINT injected_projection_failure');
    await f.projector.drain();
    const terminal=JSON.parse((await f.redis.get(f.runtime.keys.request(r.id)))!);
    await projectHotRequest(f.db.pool,terminal);await projectHotRequest(f.db.pool,{...terminal,revision:1,state:'reserved',status:'reserved',actualMicros:null,chargedMicros:null});
    assert.equal((await f.db.pool.query('SELECT count(*)::int AS n,sum(amount_micros)::text AS amount FROM ledger')).rows[0].amount,'20');
    assert.equal((await f.db.pool.query('SELECT count(*)::int AS n FROM ledger')).rows[0].n,1);
    assert.equal((await f.db.pool.query('SELECT status FROM requests')).rows[0].status,'completed');
    await f.db.pool.query('DELETE FROM requests WHERE request_id=$1',[r.id]);
    await projectHotRequest(f.db.pool,terminal);
    assert.equal((await f.db.pool.query('SELECT count(*)::int AS n FROM requests')).rows[0].n,0,'revision tombstone prevents replay resurrection after retention');
  }finally{await f.close();}
});

test('missing state and changed Redis identity fence admission instead of falling back to stale PG',{skip:!hasServices,timeout:60000},async()=>{
  const f=await fixture();let guard:ReturnType<typeof forbidSql>|undefined;
  try{
    const saved=await f.redis.get(f.runtime.keys.project(f.project));assert.ok(saved);
    await f.redis.del(f.runtime.keys.project(f.project));guard=forbidSql([f.pool,f.backgroundPool]);
    const missing=await f.send();assert.equal(missing.response.status,503);assert.equal(f.upstream.calls,0);assert.equal(guard.calls,0);
    await f.redis.set(f.runtime.keys.project(f.project),saved);
    await f.redis.hset(f.runtime.keys.runtime,'run_id','changed-primary');
    const fenced=await f.send();assert.equal(fenced.response.status,503);assert.equal(f.upstream.calls,0);assert.equal(guard.calls,0);
    guard.restore();guard=undefined;
    const other=new HotRuntime(f.backgroundPool,f.redis,f.runtime.keys,f.db.config.DATA_PLANE_NAMESPACE,'memory');
    await assert.rejects(()=>other.initialize(),/HOT_FENCED/);
    await f.redis.del(f.runtime.keys.runtime);
    await assert.rejects(()=>other.initialize(),/HOT_STATE_LOST/);
  }finally{guard?.restore();await f.close();}
});

test('unknown correction is immutable, projection is exact and safe-integer money survives Redis JSON encoding',{skip:!hasServices,timeout:60000},async()=>{
  const f=await fixture();
  try{
    const p=await prepared(f),id=randomUUID();await admitRequest(f.billing,id,p);await f.billing.heartbeat(id);await f.billing.dispatch(id);
    await f.billing.settle(id,{actualMicros:null,inputTokens:null,outputTokens:null,status:'unknown',attempts:[]});await f.projector.drain();
    await f.billing.settle(id,{actualMicros:4,inputTokens:2,outputTokens:1,status:'completed',attempts:[]});
    await f.billing.settle(id,{actualMicros:4,inputTokens:2,outputTokens:1,status:'completed',attempts:[]});
    await assert.rejects(()=>f.billing.settle(id,{actualMicros:8,inputTokens:2,outputTokens:1,status:'completed',attempts:[]}),e=>(e as any).code==='settlement_conflict');
    await f.projector.drain();
    await consumeEvent(f.db.pool,{event_id:'verify-correction',schema_version:1,occurred_at:new Date().toISOString(),project_id:f.project,event_type:'request.adjusted',payload:{}});
    const usage=(await f.db.pool.query('SELECT actual_micros::text,estimated_micros::text,input_tokens::text FROM usage_hourly')).rows[0];
    assert.deepEqual(usage,{actual_micros:'4',estimated_micros:'0',input_tokens:'2'});
    const large=Number.MAX_SAFE_INTEGER-4,bigId=randomUUID();
    await f.billing.admit({requestId:bigId,projectId:f.project,keyId:f.key.id,period:p.price.period,reserveMicros:large,rpm:600,keyRpm:600,concurrency:32,budgetMicros:-1,routeVersion:p.routeVersion,priceSnapshot:p.price});
    await f.billing.dispatch(bigId);await f.billing.settle(bigId,{actualMicros:large-1,inputTokens:1,outputTokens:1,status:'completed',attempts:[]});
    const data=JSON.parse((await f.redis.get(f.runtime.keys.request(bigId)))!);assert.equal(data.reserveMicros,large);assert.equal(data.actualMicros,large-1);
    await f.projector.drain();assert.equal((await f.db.pool.query('SELECT sum(amount_micros)::text AS n FROM ledger')).rows[0].n,String(Number.MAX_SAFE_INTEGER-1));
    assert.equal(await f.redis.get(f.runtime.keys.slots),'0');
  }finally{await f.close();}
});

test('expired reserved/dispatched leases release or conservatively charge through Redis and the async journal',{skip:!hasServices,timeout:60000},async()=>{
  const f=await fixture(),fast=new HotBilling(f.redis,f.runtime.keys,f.runtime.epoch,10000,2000);await fast.initialize();
  try{
    const p=await prepared(f),cancel=randomUUID(),unknown=randomUUID();
    await admitRequest(fast,unknown,p);await fast.dispatch(unknown);await admitRequest(fast,cancel,p);
    await new Promise(r=>setTimeout(r,2100));
    await assert.rejects(()=>fast.heartbeat(unknown),e=>(e as any).code==='lease_lost');
    await fast.reap();await f.projector.drain();
    const states=(await f.db.pool.query('SELECT request_id,status FROM requests')).rows;
    assert.equal(states.find(r=>r.request_id===cancel).status,'cancelled');assert.equal(states.find(r=>r.request_id===unknown).status,'unknown');
    assert.equal((await f.db.pool.query('SELECT sum(amount_micros)::text AS amount FROM ledger')).rows[0].amount,String(p.price.total_reserved_micros));
    const state=JSON.parse((await f.redis.get(f.runtime.keys.project(f.project)))!);assert.equal(state.budgets[p.price.period].reserved,0);
    assert.equal(await f.redis.get(f.runtime.keys.slots),'1','unknown charge keeps capacity for one actual correction');
  }finally{await fast.close();await f.close();}
});

test('loss of an unprojected journal is detected even when a consumer recreates its group',{skip:!hasServices,timeout:60000},async()=>{
  const f=await fixture();
  try{
    const r=await f.send();assert.equal(r.response.status,200);await f.finished(r.id);await f.redis.del(f.runtime.keys.journal);
    await new HotStream(f.redis,f.runtime.keys.journal).read();
    const refused=await f.send();assert.equal(refused.response.status,503);assert.match(refused.body,/billing_state_lost/);
    assert.equal((await f.app.inject({url:'/health/ready'})).statusCode,503);
    const state=JSON.parse((await f.redis.get(f.runtime.keys.project(f.project)))!);assert.equal(Object.values(state.budgets).map((b:any)=>b.spent)[0],20);
  }finally{await f.close();}
});

test('real PostgreSQL TCP outage leaves the warm gateway serving and replays its retained journal after reconnect',{skip:!hasServices,timeout:60000},async()=>{
  const destination=new URL(process.env.DATABASE_URL!),sockets=new Set<Socket>();let blocked=false;
  const proxy=createServer(client=>{
    if(blocked){client.destroy();return;}
    const upstream=connect(Number(destination.port||5432),destination.hostname);sockets.add(client);sockets.add(upstream);
    client.pipe(upstream);upstream.pipe(client);client.on('error',()=>upstream.destroy());upstream.on('error',()=>client.destroy());
    client.on('close',()=>{sockets.delete(client);upstream.destroy();});upstream.on('close',()=>{sockets.delete(upstream);client.destroy();});
  });
  await new Promise<void>(r=>proxy.listen(0,'127.0.0.1',r));const endpoint=proxy.address();if(!endpoint||typeof endpoint==='string')throw Error('No proxy address');
  const f=await fixture(10000,connection=>{const url=new URL(connection);url.hostname='127.0.0.1';url.port=String(endpoint.port);return url.toString();});
  try{
    blocked=true;for(const socket of sockets)socket.destroy();
    await new Promise(r=>setTimeout(r,20));
    const r=await f.send();assert.equal(r.response.status,200,r.body);await f.finished(r.id);
    await assert.rejects(()=>f.projector.tick());
    assert.ok(await f.redis.xlen(f.runtime.keys.journal));
    assert.equal((await f.app.inject({url:'/health/ready'})).statusCode,200);
    assert.equal((await f.db.pool.query('SELECT count(*)::int AS n FROM requests')).rows[0].n,0);
    blocked=false;await f.projector.drain();
    assert.equal((await f.db.pool.query('SELECT sum(amount_micros)::text AS amount FROM ledger')).rows[0].amount,'20');
  }finally{blocked=false;await f.close();for(const socket of sockets)socket.destroy();await new Promise<void>(r=>proxy.close(()=>r()));}
});
