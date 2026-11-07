import { test } from 'node:test';
import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import { hasServices,sandbox } from './helpers.js';
const exec=promisify(execFile);
test('workspace retention removes completed metadata while preserving ledger and unknown executions',{skip:!hasServices},async()=>{
  const db=await sandbox();try{
    const workspace=randomUUID(),project=randomUUID(),complete=randomUUID(),unknown=randomUUID();
    await db.pool.query("INSERT INTO workspaces(id,name) VALUES($1,'retention')",[workspace]);
    await db.pool.query("INSERT INTO projects(id,workspace_id,name) VALUES($1,$2,'retention')",[project,workspace]);
    await db.pool.query("INSERT INTO settings(workspace_id,key,value) VALUES($1,'request_retention_days','5')",[workspace]);
    await db.pool.query(`INSERT INTO requests(request_id,project_id,period,reserve_micros,price_snapshot,status,actual_micros,created_at)
      VALUES($1,$3,'2026-11',100,'{}','completed',50,now()-interval '10 days'),($2,$3,'2026-11',100,'{}','unknown',null,now()-interval '40 days')`,[complete,unknown,project]);
    await db.pool.query("INSERT INTO ledger(event_id,request_id,project_id,period,amount_micros,kind) VALUES($1,$2,$3,'2026-11',50,'settle')",['settle:'+complete,complete,project]);
    await exec(process.execPath,['--import','tsx','scripts/retention.ts'],{env:{...process.env,DATABASE_URL:db.config.DATABASE_URL}});
    assert.equal((await db.pool.query('SELECT request_id FROM requests')).rows[0].request_id,unknown);
    const ledger=(await db.pool.query('SELECT * FROM ledger')).rows[0];assert.equal(ledger.amount_micros,'50');assert.equal(ledger.request_id,null);
  }finally{await db.cleanup();}
});
