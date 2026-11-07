import { test } from 'node:test';
import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { EventWorker, consumeEvent, consumeEvents } from '../src/events.js';
import { hasServices,sandbox } from './helpers.js';
test('PG outbox and inbox deduplicate deliveries and recompute usage', {skip:!hasServices}, async()=>{
  const db=await sandbox();
  try {
    const workspace=randomUUID(),project=randomUUID();
    await db.pool.query('INSERT INTO workspaces(id,name) VALUES($1,$2)',[workspace,'events']);
    await db.pool.query('INSERT INTO projects(id,workspace_id,name) VALUES($1,$2,$3)',[project,workspace,'events']);
    await db.pool.query(`INSERT INTO ledger(event_id,project_id,period,amount_micros,kind,input_tokens,output_tokens) VALUES('ledger1',$1,'2026-11',123,'settle',10,5)`,[project]);
    await db.pool.query(`INSERT INTO outbox(id,event_id,event_type,project_id,payload) VALUES($1,'evt1','usage.settled',$2,'{}')`,[randomUUID(),project]);
    const worker=new EventWorker(db.pool);await worker.start();
    assert.equal(await worker.tick(),1);assert.equal(await worker.tick(),0);
    const event={event_id:'evt1',schema_version:1,occurred_at:new Date().toISOString(),project_id:project,event_type:'usage.settled',payload:{}};
    await consumeEvent(db.pool,event);
    const rows=await db.pool.query('SELECT * FROM usage_hourly');
    assert.equal(rows.rows[0].actual_micros,'123');assert.equal(rows.rows[0].input_tokens,'10');
    assert.equal((await db.pool.query('SELECT * FROM consumer_inbox')).rowCount,1);
    await db.pool.query(`INSERT INTO ledger(event_id,project_id,period,amount_micros,kind,input_tokens,output_tokens,estimated,created_at)
      VALUES('estimate:charge',$1,'2026-11',1000,'estimate',10,5,true,now()-interval '2 seconds'),
      ('adjust:charge:600',$1,'2026-11',-400,'adjustment',10,5,false,now())`,[project]);
    await consumeEvent(db.pool,{...event,event_id:'adjusted'});
    const corrected=(await db.pool.query('SELECT * FROM usage_hourly')).rows[0];
    assert.equal(corrected.actual_micros,'723');assert.equal(corrected.estimated_micros,'0');
    assert.equal(corrected.input_tokens,'20','correction replaces usage instead of counting it twice');
    await assert.rejects(()=>consumeEvent(db.pool,{...event,schema_version:99}),/Unsupported/);
    await worker.close();
  }finally{await db.cleanup();}
});

test('batched replay handles multiple projects, duplicates and corrections without double projection',{skip:!hasServices},async()=>{
  const db=await sandbox();try{
    const workspace=randomUUID(),a=randomUUID(),b=randomUUID();
    await db.pool.query("INSERT INTO workspaces(id,name) VALUES($1,'batch')",[workspace]);
    await db.pool.query("INSERT INTO projects(id,workspace_id,name) VALUES($1,$3,'a'),($2,$3,'b')",[a,b,workspace]);
    const worker=new EventWorker(db.pool);await worker.start();
    await db.pool.query(`INSERT INTO ledger(event_id,project_id,period,amount_micros,kind,input_tokens,estimated,created_at) VALUES
      ('estimate:batch',$1,'2026-11',1000,'estimate',null,true,now()-interval '2 seconds'),
      ('adjust:batch:400',$1,'2026-11',-600,'adjust',10,false,now()),
      ('settle:other',$2,'2026-11',50,'settle',2,false,now())`,[a,b]);
    const event=(id:string,project:string)=>({event_id:id,schema_version:1,occurred_at:new Date().toISOString(),project_id:project,event_type:'request.settled',payload:{}});
    const first=event('batch-first',a),second=event('batch-second',a),other=event('batch-other',b);
    await consumeEvents(db.pool,[second,first,other,first]);
    await consumeEvents(db.pool,[other,second,first]);
    assert.equal((await db.pool.query('SELECT * FROM consumer_inbox')).rowCount,3);
    const rows=(await db.pool.query('SELECT * FROM usage_hourly')).rows;
    assert.equal(rows.find(r=>r.project_id===a).actual_micros,'400');
    assert.equal(rows.find(r=>r.project_id===a).estimated_micros,'0');
    assert.equal(rows.find(r=>r.project_id===a).input_tokens,'10');
    assert.equal(rows.find(r=>r.project_id===b).actual_micros,'50');
    await worker.close();
  }finally{await db.cleanup();}
});

test('a poison event rolls back its batch, peers still publish, and the dead letter can replay',{skip:!hasServices},async()=>{
  const db=await sandbox();const worker=new EventWorker(db.pool);await worker.start();
  try{
    await db.pool.query("ALTER TABLE consumer_inbox ADD CONSTRAINT reject_test_event CHECK(event_id!='bad') NOT VALID");
    for(const event of ['good-a','bad','good-b'])await db.pool.query("INSERT INTO outbox(id,event_id,event_type,payload) VALUES($1,$2,'request.test','{}')",[randomUUID(),event]);
    assert.equal(await worker.tick(),3);
    const initial=(await db.pool.query('SELECT event_id,status FROM outbox')).rows;
    assert.equal(initial.find(r=>r.event_id==='good-a').status,'published');
    assert.equal(initial.find(r=>r.event_id==='good-b').status,'published');
    assert.equal(initial.find(r=>r.event_id==='bad').status,'pending');
    assert.equal((await db.pool.query('SELECT * FROM consumer_inbox')).rowCount,2);
    for(let i=0;i<4;i++){await db.pool.query("UPDATE outbox SET available_at=now() WHERE event_id='bad'");await worker.tick();}
    assert.equal((await db.pool.query("SELECT status FROM outbox WHERE event_id='bad'")).rows[0].status,'failed');
    assert.equal((await db.pool.query("SELECT * FROM dead_letters WHERE event_id='bad'")).rowCount,1);
    await db.pool.query('ALTER TABLE consumer_inbox DROP CONSTRAINT reject_test_event');
    await worker.replay('bad');await worker.tick();
    assert.equal((await db.pool.query('SELECT * FROM consumer_inbox')).rowCount,3);
    assert.equal((await db.pool.query("SELECT status FROM outbox WHERE event_id='bad'")).rows[0].status,'published');
  }finally{await worker.close();await db.cleanup();}
});
