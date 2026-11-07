import { randomUUID } from 'node:crypto';
import { readFile } from 'node:fs/promises';
import type { Redis } from 'ioredis';
import { BillingError,isUuid,type AdmitInput,type SettleInput } from './billing.js';
import type { AttemptRecord,SettlementStatus } from './gateway.js';
import type { HotKeys,HotRequest } from './hot-contract.js';

const codes:Record<number,[number,string]>={1:[503,'billing_generation_mismatch'],2:[503,'configuration_changed'],
  3:[503,'billing_state_lost'],4:[429,'rate_limit_exceeded'],5:[429,'concurrency_limit_exceeded'],6:[429,'budget_exceeded'],
  7:[409,'request_already_final'],8:[400,'invalid_request'],9:[404,'request_not_found'],10:[503,'event_backlog'],
  11:[409,'settlement_conflict'],12:[409,'not_admitted'],13:[410,'lease_lost'],14:[409,'already_dispatched'],15:[401,'invalid_api_key']};
function integer(n:unknown,min=0,max=Number.MAX_SAFE_INTEGER):number{
  if(typeof n!=='number'||!Number.isSafeInteger(n)||n<min||n>max)throw new BillingError(400,'invalid_request','Invalid integer');return n;
}
function nullable(n:unknown){return n===null?null:integer(n);}
function uuid(n:unknown):string{if(!isUuid(n))throw new BillingError(400,'invalid_request','Invalid UUID');return n;}
function attempts(input:unknown[]):AttemptRecord[]{
  if(!Array.isArray(input)||input.length>2)throw new BillingError(400,'invalid_request','Invalid attempts');
  return input.map((raw:any)=>({attempt_no:integer(raw.attempt_no,1,2),target_id:uuid(raw.target_id),
    upstream_model:String(raw.upstream_model).slice(0,256),status:raw.status==='ok'?'ok':'error',
    http_status:raw.http_status===null?null:integer(raw.http_status,100,599),
    error_code:typeof raw.error_code==='string'?raw.error_code.slice(0,100):null,
    request_sent:raw.request_sent===true,execution_unknown:raw.execution_unknown===true,usage_known:raw.usage_known===true,
    input_tokens:nullable(raw.input_tokens),output_tokens:nullable(raw.output_tokens),cost_micros:nullable(raw.cost_micros),
    upper_bound_micros:integer(raw.upper_bound_micros),retry_safe:raw.retry_safe===true,
    latency_ms:typeof raw.latency_ms==='number'&&Number.isFinite(raw.latency_ms)&&raw.latency_ms>=0?raw.latency_ms:0}));
}

/** Synchronous hot state and journal use Redis only. PG is exclusively a projector concern. */
export class HotBilling {
  private lua='';
  private sha='';
  private initialized=false;
  private pending=new Set<Promise<unknown>>();
  private cache=new Map<string,{projectId:string;keyId:string;owner:string}>();
  constructor(private redis:Redis,readonly keys:HotKeys,private epoch:string,private maxBacklog=10000,private leaseMs=45000){}
  async initialize(){this.lua=await readFile(new URL('./lua/hot.lua',import.meta.url),'utf8');this.sha=await this.redis.script('LOAD',this.lua) as string;this.initialized=true;}
  private async meta(id:string){
    const cached=this.cache.get(id);if(cached)return cached;
    const json=await this.redis.get(this.keys.request(id));if(!json)throw new BillingError(404,'request_not_found','Request not found');
    const data=JSON.parse(json) as HotRequest;const value={projectId:data.projectId,keyId:data.keyId,owner:data.owner};
    this.remember(id,value);return value;
  }
  private remember(id:string,value:{projectId:string;keyId:string;owner:string}){
    if(this.cache.size>=10000)this.cache.clear();this.cache.set(id,value);
  }
  private operation(op:string,id:string,input:Record<string,unknown>,meta?:{projectId:string;keyId:string;owner:string}):Promise<unknown[]>{
    if(!this.initialized)return Promise.reject(new BillingError(503,'billing_not_initialized','Billing not initialized'));
    const run=(async()=>{
      const scope=meta||await this.meta(id);
      const keys=[this.keys.runtime,this.keys.config,this.keys.journal,this.keys.slots,this.keys.active,this.keys.project(scope.projectId),
        this.keys.request(id),this.keys.prefix+'rate:project:'+scope.projectId,this.keys.prefix+'rate:key:'+scope.keyId];
      const payload=JSON.stringify({...input,owner:scope.owner,at:new Date().toISOString()});
      if(Buffer.byteLength(payload)>65536)throw new BillingError(400,'invalid_request','Metadata too large');
      let result:unknown;
      try{
        const argv=[op,this.epoch,id,payload,String(this.maxBacklog),String(this.leaseMs)];
        try{result=await this.redis.evalsha(this.sha,keys.length,...keys,...argv);}
        catch(e){if(!(e instanceof Error)||!e.message.includes('NOSCRIPT'))throw e;result=await this.redis.eval(this.lua,keys.length,...keys,...argv);}
      }catch(e){throw e instanceof BillingError?e:new BillingError(503,'billing_unavailable','Redis billing unavailable');}
      const values=result as unknown[],code=Number(values[0]);
      if(code){const [status,name]=codes[code]||[503,'billing_unavailable'];throw new BillingError(status,name,name,
        code===4?{retryAfterMs:Number(values[1])}:code===6?{availableMicros:Number(values[1])}:undefined);}
      return values;
    })();
    this.pending.add(run);return run.finally(()=>{this.pending.delete(run);});
  }
  async admit(input:AdmitInput){
    uuid(input.requestId);uuid(input.projectId);uuid(input.keyId);
    if(!/^\d{4}-(0[1-9]|1[0-2])$/.test(input.period))throw new BillingError(400,'invalid_period','Invalid period');
    integer(input.reserveMicros);integer(input.budgetMicros,-1);integer(input.rpm,1,1000000);integer(input.keyRpm,1,1000000);integer(input.concurrency,1,1000000);
    const meta={projectId:input.projectId,keyId:input.keyId,owner:randomUUID()};
    const values=await this.operation('admit',input.requestId,input as unknown as Record<string,unknown>,meta);
    this.remember(input.requestId,{...meta,owner:typeof values[1]==='string'?values[1]:meta.owner});
  }
  async dispatch(id:string){await this.operation('dispatch',uuid(id),{});}
  async heartbeat(id:string){await this.operation('heartbeat',uuid(id),{});}
  async cancelBeforeDispatch(id:string){await this.operation('cancel',uuid(id),{});}
  async settle(id:string,input:SettleInput){
    if(!/^[a-z][a-z0-9_]{0,31}$/.test(input.status))throw new BillingError(400,'invalid_request','Invalid status');
    const rows=attempts(input.attempts);
    const failure=[...rows].reverse().find(a=>a.status!=='ok');
    const errorCode=input.status==='completed'?null:failure?.error_code??(input.status==='cancelled'?'client_cancelled':null);
    await this.operation('settle',uuid(id),{actualMicros:nullable(input.actualMicros),inputTokens:nullable(input.inputTokens),
      outputTokens:nullable(input.outputTokens),status:input.status,attempts:rows,errorCode});
  }
  async persistMetadata(id:string,aliasName:string|null,status:SettlementStatus,records:AttemptRecord[]){
    const rows=attempts(records);const failure=[...rows].reverse().find(a=>a.status!=='ok');
    const errorCode=status==='completed'?null:failure?.error_code??(status==='cancelled'?'client_cancelled':null);
    await this.operation('metadata',uuid(id),{aliasName,errorCode,attempts:rows});
  }
  async reap(){
    const expired=await this.redis.zrangebyscore(this.keys.active,'-inf',Date.now(),'LIMIT',0,50);
    for(const id of expired)await this.operation('expire',id,{});
    return expired.length;
  }
  async ready(){
    try{return Number(await this.redis.eval(`local r=string.match(redis.call('INFO','server'),'run_id:([%w]+)');
      if redis.call('HGET',KEYS[1],'epoch')~=ARGV[1] or redis.call('HGET',KEYS[1],'run_id')~=r or redis.call('HEXISTS',KEYS[2],'revision')~=1 or redis.call('TYPE',KEYS[4]).ok~='stream' then return 0 end;
      local slots=tonumber(redis.call('GET',KEYS[3]));if not slots or redis.call('HGET',KEYS[1],'projection_paused')=='1' or redis.call('XLEN',KEYS[4])+slots+7>tonumber(ARGV[2]) then return 0 end;
      local info=redis.call('XINFO','STREAM',KEYS[4]);for i=1,#info,2 do if info[i]=='last-generated-id' and info[i+1]==redis.call('HGET',KEYS[1],'journal_tail') then return 1 end end;return 0`,
      4,this.keys.runtime,this.keys.config,this.keys.slots,this.keys.journal,this.epoch,String(this.maxBacklog)))===1;}catch{return false;}
  }
  async close(){this.initialized=false;await Promise.allSettled([...this.pending]);}
}
