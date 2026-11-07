import type { FastifyInstance, FastifyRequest } from 'fastify';
import type { Pool } from 'pg';
import type { Redis } from 'ioredis';
import type { HotKeys } from './hot-contract.js';
import { requireProject, authenticateSession } from './control.js';
import { setupEvents } from './events.js';

function bad(message:string,statusCode=400):never {throw Object.assign(new Error(message),{statusCode});}
function projectId(req:FastifyRequest) {
  const q=req.query as Record<string,string>;
  const id=q.project_id||q.projectId;
  if(!id||!/^[0-9a-f-]{36}$/i.test(id))bad('请选择项目。');
  return id;
}
function range(q:Record<string,string>) {
  const to=new Date(q.to||Date.now());const from=new Date(q.from||to.getTime()-86400000);
  if(!Number.isFinite(+to)||!Number.isFinite(+from)||+from>=+to||+to-+from>366*86400000)bad('时间范围无效。');
  return {from,to};
}
export async function registerQueries(app:FastifyInstance,{pool,hotState}: {pool:Pool;hotState?:{redis:Redis;keys:HotKeys}}) {
  await setupEvents(pool);
  app.get('/api/requests',async req=>{
    const id=projectId(req);await requireProject(req,pool,id);
    const q=req.query as Record<string,string>;const {from,to}=range(q);
    const limit=Math.min(100,Math.max(1,Math.floor(Number(q.limit)||50)));
    const params:any[]=[id,from,to];let filter='';
    if(q.status){params.push(q.status);filter+=` AND status=$${params.length}`;}
    if(q.model){params.push(q.model);filter+=` AND alias_name=$${params.length}`;}
    if(q.cursor){let cursor;try{cursor=JSON.parse(Buffer.from(q.cursor,'base64url').toString());}catch{bad('无效游标。');}
      if(!Array.isArray(cursor)||cursor.length!==2||!Number.isFinite(+new Date(cursor[0]))||!/^[a-f0-9-]{36}$/i.test(cursor[1]))bad('无效游标。');
      params.push(cursor[0],cursor[1]);filter+=` AND (created_at,request_id)<($${params.length-1},$${params.length})`;}
    params.push(limit+1);
    const rows=(await pool.query(`SELECT * FROM requests WHERE project_id=$1 AND created_at>=$2 AND created_at<$3 ${filter} ORDER BY created_at DESC,request_id DESC LIMIT $${params.length}`,params)).rows;
    const items=rows.slice(0,limit);const last=items.at(-1);
    return {items,next_cursor:rows.length>limit&&last?Buffer.from(JSON.stringify([last.created_at,last.request_id])).toString('base64url'):null};
  });
  app.get('/api/requests/:id',async req=>{
    const id=(req.params as any).id;if(!/^[a-f0-9-]{36}$/i.test(id))bad('无效请求 ID。');
    await authenticateSession(req,pool);
    const row=(await pool.query('SELECT * FROM requests WHERE request_id=$1',[id])).rows[0];
    if(!row)bad('请求不存在。',404);await requireProject(req,pool,row.project_id);
    return {...row,attempts:(await pool.query('SELECT * FROM attempts WHERE request_id=$1 ORDER BY attempt_no',[id])).rows};
  });
  const usage=async (req:FastifyRequest)=>{
    const id=projectId(req);await requireProject(req,pool,id);
    const q=req.query as Record<string,string>;const {from,to}=range(q);
    const totals=(await pool.query(`SELECT count(*)::int AS requests,
      count(*) FILTER(WHERE lower(status) IN('completed','success','succeeded'))::int AS successful_requests,
      count(*) FILTER(WHERE actual_micros IS NULL AND lower(status) NOT IN('preparing','reserved','rejected','cancelled'))::int AS unknown_requests,
      COALESCE(sum(input_tokens),0)::text AS input_tokens,COALESCE(sum(output_tokens),0)::text AS output_tokens,
      COALESCE(sum(actual_micros),0)::text AS actual_micros,
      COALESCE(sum(reserve_micros) FILTER(WHERE actual_micros IS NULL),0)::text AS estimated_micros
      FROM requests WHERE project_id=$1 AND created_at>=$2 AND created_at<$3`,[id,from,to])).rows[0];
    const series=(await pool.query('SELECT * FROM usage_hourly WHERE project_id=$1 AND bucket>=$2 AND bucket<$3 ORDER BY bucket',[id,from,to])).rows;
    const project=(await pool.query('SELECT id,name,budget_micros FROM projects WHERE id=$1',[id])).rows[0];
    const outstanding=(await pool.query(`SELECT COALESCE(sum(reserved_micros),0)::text AS reserved_micros FROM reservations WHERE project_id=$1 AND actual_micros IS NULL AND status NOT IN('CANCELLED','cancelled','REJECTED','rejected')`,[id])).rows[0];
    const backlog=(await pool.query(`SELECT min(created_at) AS oldest FROM outbox WHERE project_id=$1 AND status!='published'`,[id])).rows[0];
    const period=new Date().toISOString().slice(0,7);
    const monthly=(await pool.query(`SELECT COALESCE(sum(amount_micros),0)::text AS spent_micros FROM ledger WHERE project_id=$1 AND period=$2`,[id,period])).rows[0];
    const monthlyReserved=(await pool.query(`SELECT COALESCE(sum(reserved_micros),0)::text AS reserved_micros FROM reservations WHERE project_id=$1 AND period=$2 AND status IN('preparing','reserved','dispatch_intent','dispatched')`,[id,period])).rows[0];
    const budget={period,...monthly,...monthlyReserved,limit_micros:project.budget_micros};
    let pendingEvents=0;
    if(hotState){
      const raw=await hotState.redis.get(hotState.keys.project(id));
      if(!raw)bad('实时预算暂不可用。',503);
      const state=JSON.parse(raw),current=state.budgets?.[period]||{spent:0,reserved:0};
      if(!state.budgets||!Number.isSafeInteger(current.spent)||!Number.isSafeInteger(current.reserved))bad('实时预算暂不可用。',503);
      budget.spent_micros=String(current.spent);budget.reserved_micros=String(current.reserved);
      pendingEvents=await hotState.redis.xlen(hotState.keys.journal);
    }
    const updated_at=series.at(-1)?.updated_at||null;
    return {...totals,...outstanding,budget,summary:totals,totals,project,series,items:series,updated_at,
      ingestion_pending_events:pendingEvents,stale:pendingEvents>0||(backlog.oldest?Date.now()-+new Date(backlog.oldest)>60000:false),from,to};
  };
  app.get('/api/usage',usage);app.get('/api/usage/summary',usage);
  app.get('/api/audit',async req=>{
    const user=await authenticateSession(req,pool);
    if(user.role!=='admin')bad('仅管理员可查看审计记录。',403);
    return {items:(await pool.query('SELECT * FROM audit_events WHERE workspace_id=$1 ORDER BY created_at DESC LIMIT 100',[user.workspaceId])).rows};
  });
}
