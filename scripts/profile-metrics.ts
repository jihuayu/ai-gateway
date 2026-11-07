import { performance } from 'node:perf_hooks';
import { AsyncLocalStorage } from 'node:async_hooks';
import type { FastifyInstance } from 'fastify';
import { Client, type Pool } from 'pg';
import type { Redis } from 'ioredis';
import type { Billing } from '../src/billing.js';

export function distribution(values:number[]) {
  const sorted=[...values].sort((a,b)=>a-b);
  const at=(p:number)=>sorted[Math.max(0,Math.ceil(sorted.length*p)-1)]??0;
  return {count:sorted.length,p50Ms:at(.5),p95Ms:at(.95),meanMs:values.length?values.reduce((a,b)=>a+b,0)/values.length:0};
}
/** Benchmark-process only: never records SQL values, credentials, prompts or results. */
export function profileMetrics(app:FastifyInstance,pool:Pool,redis:Redis,billing:Billing) {
  const contexts=new AsyncLocalStorage<'request'|undefined>();
  app.addHook('onRequest',(_req,_reply,done)=>contexts.run('request',done));
  let active=false,completedSettlements=0,maxWaiting=0;
  let requestSqlCalls=0,backgroundSqlCalls=0;
  let sql=new Map<string,number[]>(),commands:Record<string,number>={},phases:Record<string,number[]>={},waits:number[]=[];
  const clientQuery=Client.prototype.query,poolConnect=pool.connect,sendCommand=redis.sendCommand;
  const record=(query:unknown,start:number,request:boolean)=>{
    const text=typeof query==='string'?query:(query as any)?.text||'';
    if(!active||text.startsWith('/*profile:'))return;
    // The app's capacity guard runs before the benchmark hook but belongs to this request.
    request=request||text.trim()==="SELECT 1 FROM outbox WHERE status!='published' OFFSET 9999 LIMIT 1";
    if(request)requestSqlCalls++;else backgroundSqlCalls++;
    const fingerprint=(request?'[request] ':'[background] ')+text.replace(/\s+/g,' ').trim().slice(0,160);
    const values=sql.get(fingerprint)||[];values.push(performance.now()-start);sql.set(fingerprint,values);
  };
  (Client.prototype as any).query=function(...args:any[]) {
    const start=performance.now(),query=args[0],callback=args.at(-1);
    const enabled=active,request=contexts.getStore()==='request';
    if(typeof callback==='function') args[args.length-1]=function(this:unknown,...values:any[]){if(enabled)record(query,start,request);return callback.apply(this,values);};
    const result=(clientQuery as any).apply(this,args);
    if(enabled&&typeof callback!=='function'&&result?.then)result.then(()=>record(query,start,request),()=>record(query,start,request));
    return result;
  };
  (pool as any).connect=function(callback?:Function) {
    const at=performance.now(),enabled=active,context=contexts.getStore();
    if(callback)return (poolConnect as any).call(this,(...args:any[])=>{if(enabled)waits.push(performance.now()-at);contexts.run(context,()=>callback(...args));});
    return (poolConnect as any).call(this).then((client:any)=>{if(enabled)waits.push(performance.now()-at);return client;});
  };
  (redis as any).sendCommand=function(command:any,...args:any[]) {
    if(active)commands[command.name]=(commands[command.name]||0)+1;
    return (sendCommand as any).call(this,command,...args);
  };
  const originals=new Map<string,Function>();
  for(const name of ['admit','dispatch','settle'] as const) {
    const original=billing[name];originals.set(name,original);
    (billing as any)[name]=async function(...args:any[]) {
      const at=performance.now(),enabled=active;
      try{return await (original as any).apply(this,args);}
      finally{if(enabled){(phases[name]??=[]).push(performance.now()-at);if(name==='settle')completedSettlements++;}}
    };
  }
  const timer=setInterval(()=>{if(active)maxWaiting=Math.max(maxWaiting,pool.waitingCount);},5);timer.unref();
  return {
    begin(){sql=new Map();commands={};phases={};waits=[];maxWaiting=0;completedSettlements=0;requestSqlCalls=0;backgroundSqlCalls=0;active=true;},
    get completed(){return completedSettlements;},
    end(requests:number){
      active=false;
      const queries=[...sql.entries()].map(([fingerprint,times])=>({fingerprint,...distribution(times)})).sort((a,b)=>b.count-a.count);
      const count=queries.reduce((n,q)=>n+q.count,0);
      return {sqlCalls:count,sqlCallsPerRequest:count/requests,requestSqlCalls,requestSqlCallsPerRequest:requestSqlCalls/requests,backgroundSqlCalls,queries,redisCommands:commands,redisCommandsPerRequest:Object.values(commands).reduce((a,b)=>a+b,0)/requests,pool:{maxWaiting,acquisition:distribution(waits)},phases:Object.fromEntries(Object.entries(phases).map(([n,v])=>[n,distribution(v)]))};
    },
    restore(){active=false;clearInterval(timer);Client.prototype.query=clientQuery;pool.connect=poolConnect;redis.sendCommand=sendCommand;for(const [n,fn]of originals)(billing as any)[n]=fn;},
  };
}
