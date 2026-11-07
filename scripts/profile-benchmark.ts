import { performance } from 'node:perf_hooks';
import { cpus,totalmem } from 'node:os';
import { mkdir,writeFile } from 'node:fs/promises';
import { buildApp } from '../src/app.js';
import { sandbox } from '../test/helpers.js';
import { mockUpstream } from '../test/mock-upstream.js';
import { profileMetrics,distribution } from './profile-metrics.js';

if(!process.env.DATABASE_URL||!process.env.REDIS_URL)throw Error('Test DATABASE_URL and REDIS_URL required');
const levels=(process.env.PERF_CONCURRENCY||'1,10,50,100').split(',').map(Number);
const repeats=Number(process.env.PERF_REPEATS||3);
if(levels.some(n=>!Number.isSafeInteger(n)||n<1||n>500)||!Number.isSafeInteger(repeats)||repeats<1||repeats>10)throw Error('Invalid benchmark configuration');
const db=await sandbox(),mock=await mockUpstream();
const {app,pool,redis,billing}=await buildApp(db.config,false);
const metrics=profileMetrics(app,pool,redis,billing);
const address=await app.listen({host:'127.0.0.1',port:0});
try{
  const first=await app.inject({method:'POST',url:'/api/bootstrap',payload:{email:'profile@example.com',password:'profiling-test-password',projectName:'profile'}});
  if(first.statusCode!==200)throw Error('Bootstrap failed');
  const project=first.json().project.id,headers={cookie:first.cookies.map(c=>`${c.name}=${c.value}`).join('; '),'x-csrf-token':first.json().csrf};
  const post=async(url:string,payload:any)=>{const r=await app.inject({method:'POST',url,headers,payload});if(r.statusCode!==200)throw Error(`Setup failed: ${url} (${r.statusCode})`);return r.json();};
  const config=await app.inject({method:'PATCH',url:`/api/projects/${project}`,headers,payload:{rpm:100000,concurrency:500,budget_micros:10000000}});if(config.statusCode!==200)throw Error('Limits setup failed');
  const provider=await post('/api/providers',{name:'profile',base_url:mock.url,secret:'mock'});
  const target=await post(`/api/providers/${provider.id}/targets`,{upstream_model:'mock-model',input_price:1000000,output_price:2000000,capabilities:['chat']});
  const alias=await post('/api/aliases',{project_id:project,name:'profile-chat',targets:[target.id]});await post(`/api/aliases/${alias.id}/publish`,{expectedVersion:1});
  const key=await post('/api/keys',{project_id:project,name:'profile',allowed_models:['profile-chat'],key_rpm:100000});
  let expected=0;
  const results:any[]=[];
  const run=async(concurrency:number,upstream:boolean)=>{
    const durations:number[]=[],statuses:Record<string,number>={};const at=performance.now();
    await Promise.all(Array.from({length:concurrency},async()=>{
      const started=performance.now();const response=await fetch((upstream?mock.url:address+'/v1')+'/chat/completions',{
        method:'POST',headers:{'content-type':'application/json',...(upstream?{}:{authorization:`Bearer ${key.key}`})},
        body:JSON.stringify({model:upstream?'mock-model':'profile-chat',messages:[{role:'user',content:'performance'}],max_tokens:16}),signal:AbortSignal.timeout(30000),
      });await response.arrayBuffer();durations.push(performance.now()-started);statuses[response.status]=(statuses[response.status]||0)+1;
    }));
    return {statuses,elapsedMs:performance.now()-at,response:distribution(durations)};
  };
  // Cold single request remains separate from warmed-up batches.
  metrics.begin();const coldStart=performance.now(),cold=await run(1,false);
  while(metrics.completed<1){if(performance.now()-coldStart>30000)throw Error('Cold settlement timed out');await new Promise(r=>setTimeout(r,5));}
  const coldResult={...cold,settlementElapsedMs:performance.now()-coldStart,metrics:metrics.end(1)};expected++;
  for(const concurrency of levels)for(let repetition=1;repetition<=repeats;repetition++){
    const baseline=await run(concurrency,true);
    metrics.begin();const at=performance.now(),gateway=await run(concurrency,false);
    while(metrics.completed<concurrency){if(performance.now()-at>30000)throw Error('Settlement timed out');await new Promise(r=>setTimeout(r,5));}
    const settlementElapsedMs=performance.now()-at,profile=metrics.end(concurrency);expected+=concurrency;
    const ledger=(await pool.query('SELECT count(*)::int AS count,sum(amount_micros)::text AS micros FROM ledger')).rows[0];
    if(gateway.statuses['200']!==concurrency||ledger.count!==expected||Number(ledger.micros)!==expected*20)throw Error('Response or accounting invariant failed');
    results.push({concurrency,repetition,baseline,gateway,settlementElapsedMs,metrics:profile});
  }
  const report={environment:{logicalCpus:cpus().length,totalMemoryBytes:totalmem(),node:process.version},cold:coldResult,results,accounting:{requests:expected,micros:expected*20},scope:'synthetic local upstream; real PG/Redis; SQL values and credentials excluded; method percentiles overlap and are not additive'};
  const path=process.env.PERF_REPORT||'artifacts/performance/profile.json';await mkdir('artifacts/performance',{recursive:true});await writeFile(path,JSON.stringify(report,null,2)+'\n');
  console.log(JSON.stringify({path,accounting:report.accounting,results:results.map(r=>({concurrency:r.concurrency,repetition:r.repetition,p95Ms:r.gateway.response.p95Ms,settlementElapsedMs:r.settlementElapsedMs,sqlCallsPerRequest:r.metrics.sqlCallsPerRequest,requestSqlCallsPerRequest:r.metrics.requestSqlCallsPerRequest,redisCommandsPerRequest:r.metrics.redisCommandsPerRequest,pool:r.metrics.pool,phases:r.metrics.phases}))}));
}finally{metrics.restore();await app.close();await mock.close();await db.cleanup();}
