import { test } from 'node:test';
import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { Redis } from 'ioredis';
import { HotStream } from '../src/hot-stream.js';
const enabled=Boolean(process.env.REDIS_URL);
async function fixture(){
  const redis=new Redis(process.env.REDIS_URL!),key=`test:{${randomUUID()}}:hot-stream`;
  return {redis,key,close:async()=>{await redis.del(key);await redis.quit();}};
}
test('stream retries its pending messages and never deletes an unacknowledged delivery',{skip:!enabled},async()=>{
  const f=await fixture();try{
    const one=(await f.redis.xadd(f.key,'*','data','one'))!,two=(await f.redis.xadd(f.key,'*','data','two'))!;
    const stream=new HotStream(f.redis,f.key);
    assert.deepEqual((await stream.read()).map(e=>e.id),[one,two]);
    assert.deepEqual((await stream.read()).map(e=>e.id),[one,two]);
    await stream.ack([one]);
    const three=(await f.redis.xadd(f.key,'*','data','three'))!;
    await stream.ack([three]);assert.equal(await f.redis.xlen(f.key),2,'undelivered message cannot be acknowledged/deleted');
    assert.deepEqual((await stream.read()).map(e=>e.id),[two,three]);await stream.ack([two,three]);assert.equal(await f.redis.xlen(f.key),0);
  }finally{await f.close();}
});
test('a replacement stream consumer reclaims an abandoned delivery without losing its payload',{skip:!enabled},async()=>{
  const f=await fixture();try{
    await f.redis.xadd(f.key,'*','data','retained');const old=new HotStream(f.redis,f.key,'pg-hot-v1',0);
    const first=await old.read(),replacement=new HotStream(f.redis,f.key,'pg-hot-v1',0),again=await replacement.read();
    assert.deepEqual(again,first);assert.equal(again[0].data,'retained');await replacement.ack(again.map(e=>e.id));assert.equal(await f.redis.xlen(f.key),0);
  }finally{await f.close();}
});
test('malformed financial stream records are retained and group reset is surfaced before recreation',{skip:!enabled},async()=>{
  const f=await fixture();try{
    const reader=new HotStream(f.redis,f.key);await reader.read();await f.redis.call('XGROUP','DESTROY',f.key,'pg-hot-v1');
    await assert.rejects(()=>reader.read(),/NOGROUP/);assert.deepEqual(await reader.read(),[]);
    await f.redis.xadd(f.key,'*','wrong','payload');await assert.rejects(()=>reader.read(),/data field/);
    assert.equal(await f.redis.xlen(f.key),1);const pending=await f.redis.xpending(f.key,'pg-hot-v1') as unknown[];assert.equal(Number(pending[0]),1);
  }finally{await f.close();}
});
