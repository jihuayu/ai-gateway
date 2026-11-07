import { test } from 'node:test';
import assert from 'node:assert/strict';
import Fastify from 'fastify';
import { Redis } from 'ioredis';
import { registerControl } from '../src/control.js';
import { registerProviders } from '../src/providers.js';
import { registerCatalog } from '../src/catalog.js';
import { registerGateway } from '../src/gateway.js';
import { registerQueries } from '../src/queries.js';
import { Billing } from '../src/billing.js';
import { mockUpstream } from './mock-upstream.js';
import { hasServices,sandbox } from './helpers.js';

test('full gateway: setup, request, SSE, fallback, unknown usage, revocation and metadata',{skip:!hasServices,timeout:60000},async()=>{
  const db=await sandbox(),redis=new Redis(process.env.REDIS_URL!);redis.on('error',()=>{});
  const upstream=await mockUpstream(),app=Fastify();
  const billing=new Billing(db.pool,redis);await billing.initialize();
  const deps={pool:db.pool,billing,encryptionKey:'1'.repeat(64),allowPrivateUpstream:true};
  await registerControl(app,deps);await registerProviders(app,deps);await registerCatalog(app,deps);await registerGateway(app,deps);await registerQueries(app,deps);
  const address=await app.listen({host:'127.0.0.1',port:0});
  try{
    const first=await app.inject({method:'POST',url:'/api/bootstrap',payload:{email:'admin@example.com',password:'secure-test-password',projectName:'Integration'}});
    assert.equal(first.statusCode,200,first.body);
    const project=first.json().project.id;
    const headers={cookie:first.cookies.map(c=>`${c.name}=${c.value}`).join('; '),'x-csrf-token':first.json().csrf};
    const call=async(url:string,payload:any)=>{const r=await app.inject({method:'POST',url,headers,payload});assert.equal(r.statusCode,200,r.body);return r.json();};
    const provider=await call('/api/providers',{name:'test',base_url:upstream.url,secret:'do-not-persist-plaintext'});
    const aliases:Record<string,string>={};
    for(const [name,models]of Object.entries({chat:['mock-model'],fallback:['limited','mock-model'],unknown:['unknown'],partial:['partial']})){
      const targets=[];
      for(const model of models){
        const existing=(await db.pool.query('SELECT id FROM model_targets WHERE provider_id=$1 AND upstream_model=$2',[provider.id,model])).rows[0];
        targets.push(existing?.id||(await call(`/api/providers/${provider.id}/targets`,{upstream_model:model,input_price:1000000,output_price:2000000,capabilities:['chat','stream']})).id);
      }
      const alias=await call('/api/aliases',{project_id:project,name,targets,capabilities:['chat','stream']});
      aliases[name]=alias.id;await call(`/api/aliases/${alias.id}/publish`,{expectedVersion:1});
    }
    const key=await call('/api/keys',{project_id:project,name:'test',allowed_models:['chat','fallback','unknown','partial']});
    const dataHeaders={authorization:`Bearer ${key.key}`};
    const send=async(model:string,stream=false)=>{
      const r=await fetch(address+'/v1/chat/completions',{method:'POST',headers:{...dataHeaders,'content-type':'application/json'},body:JSON.stringify({model,messages:[{role:'user',content:'private-prompt-marker'}],max_tokens:16,stream}),signal:AbortSignal.timeout(10000)});
      const body=await r.text();return {statusCode:r.status,body,headers:Object.fromEntries(r.headers),json:()=>JSON.parse(body)};
    };
    assert.equal((await app.inject({url:'/v1/models',headers:dataHeaders})).json().data.length,4);
    const originalQuery=db.pool.query;let metadataFailureInjected=false;
    (db.pool as any).query=function(...args:any[]){
      if(!metadataFailureInjected&&typeof args[0]==='string'&&args[0].includes('/*gw:request-attempts*/')){
        metadataFailureInjected=true;return Promise.reject(Error('injected metadata write failure'));
      }
      return (originalQuery as any).apply(this,args);
    };
    console.log('integration: buffered');const result=await send('chat');assert.equal(result.statusCode,200,result.body);assert.ok(result.json().choices[0].message.content);
    const id=result.headers['x-request-id'];assert.ok(id);
    let saved:any;
    for(let i=0;i<100;i++){saved=(await db.pool.query('SELECT * FROM requests WHERE request_id=$1',[id])).rows[0];if(saved.actual_micros!==null)break;await new Promise(r=>setTimeout(r,20));}
    assert.equal(saved.actual_micros,'20',JSON.stringify(saved));
    db.pool.query=originalQuery;assert.equal(metadataFailureInjected,true);
    assert.equal((await db.pool.query('SELECT * FROM attempts WHERE request_id=$1',[id])).rowCount,1,'finalization retries the failed incremental metadata write');
    console.log('integration: stream');const stream=await send('chat',true);assert.equal(stream.statusCode,200,stream.body);assert.match(stream.body,/data: \[DONE\]/);
    console.log('integration: fallback');const fallback=await send('fallback');assert.equal(fallback.statusCode,200,fallback.body);
    let attemptCount=0;for(let i=0;i<100;i++){attemptCount=(await db.pool.query('SELECT * FROM attempts WHERE request_id=$1',[fallback.headers['x-request-id']])).rowCount||0;if(attemptCount===2)break;await new Promise(r=>setTimeout(r,20));}assert.equal(attemptCount,2);
    assert.equal((await db.pool.query('SELECT error_code FROM requests WHERE request_id=$1',[fallback.headers['x-request-id']])).rows[0].error_code,null,'successful fallback clears the earlier failure code');
    console.log('integration: unknown');const unknown=await send('unknown');assert.equal(unknown.statusCode,200,unknown.body);
    assert.equal((await db.pool.query('SELECT actual_micros FROM requests WHERE request_id=$1',[unknown.headers['x-request-id']])).rows[0].actual_micros,null);
    const partial=await send('partial',true);assert.match(partial.body,/error/);assert.ok(!partial.body.includes('[DONE]'));
    let partialRow:any;
    for(let i=0;i<100;i++){partialRow=(await db.pool.query('SELECT status,error_code FROM requests WHERE request_id=$1',[partial.headers['x-request-id']])).rows[0];if(partialRow.status==='partial')break;await new Promise(r=>setTimeout(r,20));}
    assert.equal(partialRow.status,'partial');
    const partialAttempts=(await db.pool.query('SELECT error_code FROM attempts WHERE request_id=$1',[partial.headers['x-request-id']])).rows;
    assert.equal(partialAttempts.length,1);assert.ok(partialAttempts[0].error_code);
    assert.equal(partialRow.error_code,partialAttempts[0].error_code,'finalization must retain the incrementally persisted failure code');
    const detail=await app.inject({url:`/api/requests/${id}`,headers});assert.equal(detail.statusCode,200,detail.body);assert.ok(detail.json().attempts.length);
    assert.ok(!JSON.stringify((await db.pool.query('SELECT * FROM requests')).rows).includes('private-prompt-marker'));
    const primaryTarget=(await db.pool.query('SELECT targets FROM route_versions WHERE alias_id=$1 AND version=1',[aliases.chat])).rows[0].targets;
    await call(`/api/aliases/${aliases.chat}/versions`,{targets:primaryTarget,capabilities:['chat']});
    const stale=await app.inject({method:'POST',url:`/api/aliases/${aliases.chat}/publish`,headers,payload:{expectedVersion:1}});
    assert.equal(stale.statusCode,409,'stale published version cannot select a newer draft');
    const filtered=await app.inject({url:`/api/requests?project_id=${project}&model=chat`,headers});
    assert.equal(filtered.statusCode,200);assert.equal(filtered.json().items.length,2);
    assert.ok(filtered.json().items.every((r:any)=>r.alias_name==='chat'));
    const conflict=await app.inject({method:'POST',url:`/api/aliases/${aliases.chat}/publish`,headers,payload:{expectedVersion:0}});assert.equal(conflict.statusCode,409);
    await call(`/api/keys/${key.id}/revoke`,{});
    const before=upstream.calls;assert.equal((await send('chat')).statusCode,401);assert.equal(upstream.calls,before);
    assert.equal((await app.inject({url:`/api/usage?project_id=${project}`,headers})).statusCode,200);
  }finally{await app.close();await redis.quit();await upstream.close();await db.cleanup();}
});
