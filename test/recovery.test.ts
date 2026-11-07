import { test } from 'node:test';
import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { promisify } from 'node:util';
import { execFile } from 'node:child_process';
import { Redis } from 'ioredis';
import { Billing,BillingError } from '../src/billing.js';
import { sandbox,hasServices } from './helpers.js';
const exec=promisify(execFile);
test('real Redis restart fences admission and rebuilds PG ledger without resetting spend',{
  skip:!hasServices||!process.env.RECOVERY_REDIS_URL||!process.env.RECOVERY_CONTAINER,timeout:60000
},async()=>{
  const db=await sandbox(),redis=new Redis(process.env.RECOVERY_REDIS_URL!,{maxRetriesPerRequest:1,enableOfflineQueue:false});redis.on('error',()=>{});
  if(redis.status!=='ready')await new Promise<void>(r=>redis.once('ready',r));
  const billing=new Billing(db.pool,redis);
  try{
    const workspace=randomUUID(),project=randomUUID(),key=randomUUID();
    await db.pool.query('INSERT INTO workspaces(id,name) VALUES($1,$2)',[workspace,'recovery']);
    await db.pool.query('INSERT INTO projects(id,workspace_id,name,budget_micros) VALUES($1,$2,$3,1000)',[project,workspace,'recovery']);
    await db.pool.query("INSERT INTO api_keys(id,project_id,name,key_hash,prefix) VALUES($1,$2,'recovery',$3,'recovery')",[key,project,randomUUID()]);
    await billing.initialize();
    const params=(reserveMicros:number)=>({requestId:randomUUID(),projectId:project,keyId:key,period:'2026-11',reserveMicros,rpm:1000,keyRpm:1000,concurrency:10,budgetMicros:1000,routeVersion:{},priceSnapshot:{}});
    const first=params(800);await billing.admit(first);await billing.dispatch(first.requestId);
    await billing.settle(first.requestId,{actualMicros:800,inputTokens:1,outputTokens:1,status:'completed',attempts:[]});
    const before=(await redis.info('server')).match(/run_id:(\w+)/)?.[1];
    await exec('docker',['restart',process.env.RECOVERY_CONTAINER!]);
    let restarted=false;
    for(let i=0;i<100;i++){try{if((await redis.info('server')).match(/run_id:(\w+)/)?.[1]!==before){restarted=true;break;}}catch{}await new Promise(r=>setTimeout(r,100));}
    assert.ok(restarted);
    await assert.rejects(()=>billing.admit(params(300)),e=>[429,503].includes((e as BillingError).statusCode));
    await billing.reconcile();
    assert.equal(await redis.hget(`billing:{${project}}:budget:2026-11`,'spent'),'800');
    await assert.rejects(()=>billing.admit(params(300)),e=>(e as BillingError).code==='budget_exceeded');
    assert.equal((await db.pool.query('SELECT sum(amount_micros)::text AS total FROM ledger')).rows[0].total,'800');
  }finally{await billing.close();await redis.quit();await db.cleanup();}
});
