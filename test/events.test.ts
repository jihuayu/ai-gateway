import { test } from 'node:test';
import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { EventWorker, consumeEvent } from '../src/events.js';
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
