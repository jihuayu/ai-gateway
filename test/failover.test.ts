import { test } from 'node:test';
import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { createServer,connect,type Socket } from 'node:net';
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import { Redis } from 'ioredis';
import { Billing,BillingError } from '../src/billing.js';
import { hasServices,sandbox } from './helpers.js';
const exec=promisify(execFile);
test('promotion to a new Redis primary fences the old generation and preserves spent budget',{
  skip:!hasServices||!process.env.FAILOVER_REPLICA_URL||!process.env.FAILOVER_PRIMARY_CONTAINER,timeout:60000
},async()=>{
  let target=6381;const sockets=new Set<Socket>();
  const proxy=createServer(client=>{const upstream=connect(target,'127.0.0.1');sockets.add(client);sockets.add(upstream);
    client.pipe(upstream);upstream.pipe(client);
    client.on('error',()=>upstream.destroy());upstream.on('error',()=>client.destroy());
    client.on('close',()=>{sockets.delete(client);upstream.destroy();});upstream.on('close',()=>{sockets.delete(upstream);client.destroy();});
  });
  await new Promise<void>(r=>proxy.listen(0,'127.0.0.1',r));const address=proxy.address();if(!address||typeof address==='string')throw Error('No proxy');
  const redis=new Redis(`redis://127.0.0.1:${address.port}`,{maxRetriesPerRequest:1});redis.on('error',()=>{});
  const replica=new Redis(process.env.FAILOVER_REPLICA_URL!);replica.on('error',()=>{});
  const db=await sandbox(),billing=new Billing(db.pool,redis);
  try{
    const workspace=randomUUID(),project=randomUUID(),key=randomUUID();
    await db.pool.query('INSERT INTO workspaces(id,name) VALUES($1,$2)',[workspace,'failover']);
    await db.pool.query("INSERT INTO projects(id,workspace_id,name,budget_micros) VALUES($1,$2,'failover',1000)",[project,workspace]);
    await db.pool.query("INSERT INTO api_keys(id,project_id,name,key_hash,prefix) VALUES($1,$2,'failover',$3,'failover')",[key,project,randomUUID()]);
    await billing.initialize();const params=(reserveMicros:number)=>({requestId:randomUUID(),projectId:project,keyId:key,period:'2026-11',reserveMicros,rpm:1000,keyRpm:1000,concurrency:10,budgetMicros:1000,routeVersion:{},priceSnapshot:{}});
    const first=params(800);await billing.admit(first);await billing.dispatch(first.requestId);
    await billing.settle(first.requestId,{actualMicros:800,inputTokens:1,outputTokens:1,status:'completed',attempts:[]});
    assert.equal(await redis.wait(1,5000),1,'replica acknowledged baseline');
    await replica.replicaof('NO','ONE');
    target=6382;for(const socket of sockets)socket.destroy();
    await exec('docker',['stop',process.env.FAILOVER_PRIMARY_CONTAINER!]);
    let connected=false;for(let i=0;i<100;i++){try{await redis.ping();connected=true;break;}catch{}await new Promise(r=>setTimeout(r,100));}assert.ok(connected);
    await assert.rejects(()=>billing.admit(params(300)),e=>(e as BillingError).statusCode===503);
    await billing.reconcile();
    assert.equal(await redis.hget(`billing:{${project}}:budget:2026-11`,'spent'),'800');
    await assert.rejects(()=>billing.admit(params(300)),e=>(e as BillingError).code==='budget_exceeded');
  }finally{await billing.close();await redis.quit();await replica.quit();for(const socket of sockets)socket.destroy();await new Promise<void>(r=>proxy.close(()=>r()));await db.cleanup();}
});
