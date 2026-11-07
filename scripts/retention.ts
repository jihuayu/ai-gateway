import { Pool } from 'pg';
if(!process.env.DATABASE_URL)throw Error('DATABASE_URL required');
const days=Number(process.env.REQUEST_RETENTION_DAYS||30);
if(!Number.isInteger(days)||days<1)throw Error('Retention days must be a positive integer');
const pool=new Pool({connectionString:process.env.DATABASE_URL,max:1});
try{
  const deleted=await pool.query(`DELETE FROM requests q USING projects p WHERE q.project_id=p.id
    AND q.created_at<now()-(COALESCE((SELECT (s.value #>> '{}')::int FROM settings s WHERE s.workspace_id=p.workspace_id AND s.key='request_retention_days'),$1)*interval '1 day')
    AND lower(q.status) IN('success','completed','succeeded','cancelled','failed')
    AND NOT EXISTS(SELECT 1 FROM reservations r WHERE r.request_id=q.request_id AND r.actual_micros IS NULL AND r.status!='cancelled')`,[days]);
  // Ledger is deliberately never deleted by request retention. Unknown execution stays available for reconciliation.
  await pool.query(`DELETE FROM outbox WHERE status='published' AND published_at<now()-interval '30 days'`);
  await pool.query(`DELETE FROM consumer_inbox WHERE processed_at<now()-interval '31 days'`);
  console.log(JSON.stringify({deletedRequests:deleted.rowCount,ledgerRetained:true}));
}finally{await pool.end();}
