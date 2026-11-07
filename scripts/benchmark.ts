import { performance } from 'node:perf_hooks';
const endpoint=process.env.BENCH_URL||'http://127.0.0.1:3000';
const key=process.env.BENCH_API_KEY;
if(!key)throw Error('BENCH_API_KEY is required; use a dedicated test project/key');
const model=process.env.BENCH_MODEL||'support-chat';
const concurrent=Number(process.env.BENCH_CONCURRENCY||100);
if(!Number.isSafeInteger(concurrent)||concurrent<1||concurrent>1000)throw Error('Invalid concurrency');
const latencies:number[]=[];const statuses:Record<string,number>={};
const start=performance.now();
await Promise.all(Array.from({length:concurrent},async()=>{
  const at=performance.now();
  try{
    const response=await fetch(endpoint+'/v1/chat/completions',{method:'POST',headers:{authorization:`Bearer ${key}`,'content-type':'application/json'},body:JSON.stringify({model,messages:[{role:'user',content:'ping'}],max_tokens:16}),signal:AbortSignal.timeout(130000)});
    await response.arrayBuffer();statuses[response.status]=(statuses[response.status]||0)+1;
  }catch{statuses.network_error=(statuses.network_error||0)+1;}
  latencies.push(performance.now()-at);
}));
latencies.sort((a,b)=>a-b);
console.log(JSON.stringify({concurrent,elapsedMs:performance.now()-start,statuses,p50Ms:latencies[Math.floor(latencies.length*.5)],p95Ms:latencies[Math.min(latencies.length-1,Math.floor(latencies.length*.95))]},null,2));
