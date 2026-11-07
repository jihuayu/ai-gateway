import { test } from 'node:test';
import assert from 'node:assert/strict';
import Fastify from 'fastify';
import { registerControl } from '../src/control.js';
import { registerProviders } from '../src/providers.js';
import { hasServices,sandbox } from './helpers.js';
import { mockUpstream } from './mock-upstream.js';
test('supplier test connects, hides credentials and validates model prices',{skip:!hasServices},async()=>{
  const db=await sandbox();const upstream=await mockUpstream();const app=Fastify();
  const deps={pool:db.pool,encryptionKey:'1'.repeat(64),allowPrivateUpstream:true};
  await registerControl(app,deps);await registerProviders(app,deps);
  try{
    const first=await app.inject({method:'POST',url:'/api/bootstrap',payload:{email:'admin@example.com',password:'secure-test-password',projectName:'test'}});
    const headers={cookie:first.cookies.map(c=>`${c.name}=${c.value}`).join('; '),'x-csrf-token':first.json().csrf};
    const created=await app.inject({method:'POST',url:'/api/providers',headers,payload:{name:'Mock',base_url:upstream.url,secret:'sensitive-test-secret'}});
    assert.equal(created.statusCode,200,created.body);assert.ok(!created.body.includes('sensitive-test-secret'));
    const id=created.json().id;
    const probe=await app.inject({method:'POST',url:`/api/providers/${id}/test`,headers});assert.equal(probe.statusCode,200,probe.body);assert.equal(probe.json().status,'ok');
    const target=await app.inject({method:'POST',url:`/api/providers/${id}/targets`,headers,payload:{upstream_model:'mock-model',input_price:1000000,output_price:2000000,capabilities:['chat','stream']}});
    assert.equal(target.statusCode,200,target.body);
    const bad=await app.inject({method:'POST',url:`/api/providers/${id}/targets`,headers,payload:{upstream_model:'bad',input_price:-1,output_price:0}});assert.equal(bad.statusCode,400,bad.body);
    const audit=(await db.pool.query('SELECT changes FROM audit_events')).rows;
    assert.ok(!JSON.stringify(audit).includes('sensitive-test-secret'));
  }finally{await app.close();await upstream.close();await db.cleanup();}
});
