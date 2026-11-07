import { test } from 'node:test';
import assert from 'node:assert/strict';
import Fastify from 'fastify';
import { registerControl } from '../src/control.js';
import { hasServices,sandbox } from './helpers.js';
test('bootstrap is one-time; login, CSRF and revoked sessions enforce real PG state',{skip:!hasServices},async()=>{
  const db=await sandbox();const app=Fastify();
  await registerControl(app,{pool:db.pool,encryptionKey:'1'.repeat(64),allowPrivateUpstream:true});
  try{
    const credentials={email:'admin@example.com',password:'secure-test-password',projectName:'Control test'};
    const first=await app.inject({method:'POST',url:'/api/bootstrap',payload:credentials});
    assert.equal(first.statusCode,200,first.body);
    const cookie=first.cookies.map(c=>`${c.name}=${c.value}`).join('; ');
    const csrf=first.json().csrf;
    assert.equal((await app.inject({method:'GET',url:'/api/session',headers:{cookie}})).json().user.role,'admin');
    assert.equal((await app.inject({method:'POST',url:'/api/bootstrap',payload:credentials})).statusCode,409);
    assert.equal((await app.inject({method:'POST',url:'/api/logout',headers:{cookie}})).statusCode,403);
    assert.equal((await app.inject({method:'POST',url:'/api/logout',headers:{cookie,'x-csrf-token':csrf}})).statusCode,200);
    assert.equal((await app.inject({method:'GET',url:'/api/session',headers:{cookie}})).statusCode,401);
    const login=await app.inject({method:'POST',url:'/api/login',payload:{email:credentials.email,password:credentials.password}});
    assert.equal(login.statusCode,200,login.body);
    const loginCookie=login.cookies.map(c=>`${c.name}=${c.value}`).join('; ');
    assert.equal((await app.inject({method:'GET',url:'/api/projects',headers:{cookie:loginCookie}})).json().items.length,1);
    assert.equal((await app.inject({method:'POST',url:'/api/login',payload:{email:credentials.email,password:'wrong-password'}})).statusCode,401);
  }finally{await app.close();await db.cleanup();}
});
