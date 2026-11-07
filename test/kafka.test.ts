import { test } from 'node:test';
import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { EventWorker } from '../src/events.js';
import { hasServices,sandbox } from './helpers.js';
test('optional Kafka delivers real outbox events to transactional inbox',{skip:!hasServices||!process.env.KAFKA_BROKERS,timeout:90000},async()=>{
  const db=await sandbox();const worker=new EventWorker(db.pool,process.env.KAFKA_BROKERS!.split(','));
  try{
    const eventId=randomUUID();
    await worker.start();
    await db.pool.query(`INSERT INTO outbox(id,event_id,event_type,payload) VALUES($1,$2,'request.test','{}')`,[randomUUID(),eventId]);
    assert.equal(await worker.tick(),1);
    const until=Date.now()+45000;let delivered=false;
    while(Date.now()<until){
      if((await db.pool.query('SELECT 1 FROM consumer_inbox WHERE event_id=$1',[eventId])).rowCount){delivered=true;break;}
      await new Promise(r=>setTimeout(r,200));
    }
    assert.ok(delivered,'Kafka consumer persisted inbox record');
    assert.equal((await db.pool.query('SELECT status FROM outbox WHERE event_id=$1',[eventId])).rows[0].status,'published');
  }finally{await worker.close();await db.cleanup();}
});
