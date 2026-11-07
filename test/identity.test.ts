import { test } from 'node:test';
import assert from 'node:assert/strict';
import Fastify from 'fastify';
import { registerControl } from '../src/control.js';
import { registerIdentity } from '../src/identity.js';
import { registerCatalog } from '../src/catalog.js';
import { hasServices,sandbox } from './helpers.js';
test('invitations, scoped roles, admin protection and session invalidation',{skip:!hasServices},async()=>{
  const db=await sandbox(),app=Fastify();const deps={pool:db.pool,encryptionKey:'1'.repeat(64),allowPrivateUpstream:true};
  await registerControl(app,deps);await registerIdentity(app,deps);await registerCatalog(app,deps);
  try{
    const first=await app.inject({method:'POST',url:'/api/bootstrap',payload:{email:'admin@example.com',password:'secure-test-password',projectName:'one'}});
    const headers={cookie:first.cookies.map(c=>`${c.name}=${c.value}`).join('; '),'x-csrf-token':first.json().csrf};
    const project=first.json().project.id,adminId=first.json().session.user.id;
    const second=await app.inject({method:'POST',url:'/api/projects',headers,payload:{name:'two'}});assert.equal(second.statusCode,200,second.body);
    const invitation=await app.inject({method:'POST',url:'/api/invitations',headers,payload:{email:'viewer@example.com',role:'viewer',project_ids:[project]}});assert.equal(invitation.statusCode,200,invitation.body);
    const accepted=await app.inject({method:'POST',url:'/api/invitations/accept',payload:{token:invitation.json().token,password:'viewer-test-password'}});assert.equal(accepted.statusCode,200,accepted.body);
    const viewer={cookie:accepted.cookies.map(c=>`${c.name}=${c.value}`).join('; '),'x-csrf-token':accepted.json().csrf||accepted.json().session.csrf};
    assert.equal((await app.inject({url:'/api/projects',headers:viewer})).json().items.length,1);
    assert.equal((await app.inject({url:`/api/projects/${second.json().id}`,headers:viewer})).statusCode,404);
    assert.equal((await app.inject({method:'PATCH',url:`/api/projects/${project}`,headers:viewer,payload:{name:'bad'}})).statusCode,403);
    assert.equal((await app.inject({method:'POST',url:'/api/keys',headers:viewer,payload:{project_id:project,name:'bad'}})).statusCode,403);
    const again=await app.inject({method:'POST',url:'/api/invitations/accept',payload:{token:invitation.json().token,password:'viewer-test-password'}});assert.ok(again.statusCode>=400);
    const last=await app.inject({method:'PATCH',url:`/api/members/${adminId}`,headers,payload:{role:'viewer'}});assert.equal(last.statusCode,409,last.body);
    const viewerId=accepted.json().session.user.id;
    assert.equal((await app.inject({method:'PATCH',url:`/api/members/${viewerId}`,headers,payload:{disabled:true}})).statusCode,200);
    assert.equal((await app.inject({url:'/api/session',headers:viewer})).statusCode,401);
  }finally{await app.close();await db.cleanup();}
});
