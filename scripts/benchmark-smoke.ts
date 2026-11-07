import { performance } from 'node:perf_hooks';
import { cpus,totalmem } from 'node:os';
import { writeFile,mkdir } from 'node:fs/promises';
import { buildApp } from '../src/app.js';
import { sandbox } from '../test/helpers.js';
import { mockUpstream } from '../test/mock-upstream.js';
const db=await sandbox(),mock=await mockUpstream();
const {app,pool}=await buildApp(db.config,false);
const address=await app.listen({host:'127.0.0.1',port:0});
try{
  const first=await app.inject({method:'POST',url:'/api/bootstrap',payload:{email:'perf@example.com',password:'performance-test-password',projectName:'performance'}});
  if(first.statusCode!==200)throw Error(first.body);
  const project=first.json().project.id;
  const headers={cookie:first.cookies.map(c=>`${c.name}=${c.value}`).join('; '),'x-csrf-token':first.json().csrf};
  const create=async(url:string,payload:any)=>{const r=await app.inject({method:'POST',url,headers,payload});if(r.statusCode!==200)throw Error(r.body);return r.json();};
  await app.inject({method:'PATCH',url:`/api/projects/${project}`,headers,payload:{rpm:10000,concurrency:200,budget_micros:1000000}});
  const provider=await create('/api/providers',{name:'perf',base_url:mock.url,secret:'mock'});
  const target=await create(`/api/providers/${provider.id}/targets`,{upstream_model:'mock-model',input_price:1000000,output_price:2000000,capabilities:['chat']});
  const alias=await create('/api/aliases',{project_id:project,name:'perf-chat',targets:[target.id]});
  await create(`/api/aliases/${alias.id}/publish`,{expectedVersion:1});
  const key=await create('/api/keys',{project_id:project,name:'perf',allowed_models:['perf-chat']});
  const run=async(url:string,model:string,authorization?:string)=>{
    const latencies:number[]=[];const statuses:Record<string,number>={};const at=performance.now();
    await Promise.all(Array.from({length:100},async()=>{const start=performance.now();
      const r=await fetch(url,{method:'POST',headers:{'content-type':'application/json',...(authorization?{authorization}: {})},body:JSON.stringify({model,messages:[{role:'user',content:'performance'}],max_tokens:16})});
      await r.text();statuses[r.status]=(statuses[r.status]||0)+1;latencies.push(performance.now()-start);
    }));latencies.sort((a,b)=>a-b);return {statuses,elapsedMs:performance.now()-at,p50Ms:latencies[50],p95Ms:latencies[95]};
  };
  const baseline=await run(mock.url+'/chat/completions','mock-model');
  const gateway=await run(address+'/v1/chat/completions','perf-chat',`Bearer ${key.key}`);
  for(let i=0;i<100;i++){if(Number((await pool.query('SELECT count(*) FROM ledger')).rows[0].count)===100)break;await new Promise(r=>setTimeout(r,50));}
  const ledger=(await pool.query('SELECT count(*)::int AS requests,sum(amount_micros)::text AS micros FROM ledger')).rows[0];
  const report={concurrency:100,environment:{logicalCpus:cpus().length,totalMemoryBytes:totalmem(),node:process.version},baseline,gateway,p95DifferenceMs:gateway.p95Ms-baseline.p95Ms,ledger,scope:'local mock upstream, real PostgreSQL/Redis; total response latency, not isolated gateway overhead'};
  await mkdir('artifacts',{recursive:true});await writeFile('artifacts/benchmark.json',JSON.stringify(report,null,2)+'\n');console.log(JSON.stringify(report));
  if(gateway.statuses['200']!==100||ledger.requests!==100||ledger.micros!=='2000')throw Error('100-request accounting benchmark failed');
}finally{await app.close();await mock.close();await db.cleanup();}
