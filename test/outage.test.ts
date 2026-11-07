import { test } from 'node:test';
import assert from 'node:assert/strict';
import Fastify from 'fastify';
import { Pool } from 'pg';
import { registerGateway } from '../src/gateway.js';
test('PostgreSQL connection failure never reaches admission or upstream',async()=>{
  const pool=new Pool({connectionString:'postgresql://unavailable@127.0.0.1:1/unavailable',connectionTimeoutMillis:100,max:1});
  const app=Fastify();let admitted=false;
  const billing={admit:async()=>{admitted=true;throw Error('must not admit');}} as any;
  await registerGateway(app,{pool,billing,encryptionKey:'1'.repeat(64),allowPrivateUpstream:false});
  try{
    const response=await app.inject({method:'POST',url:'/v1/chat/completions',headers:{authorization:'Bearer invalid-test-key'},payload:{model:'test',messages:[{role:'user',content:'test'}]}});
    assert.ok(response.statusCode>=500,response.body);assert.equal(admitted,false);
    assert.ok(!response.body.includes('postgresql://'));
  }finally{await app.close();await pool.end();}
});
