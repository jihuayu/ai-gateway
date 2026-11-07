import test, { after, before, beforeEach, type TestContext } from 'node:test';
import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { readFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import { Pool } from 'pg';
import { Redis } from 'ioredis';
import { Billing, BillingError, type AdmitInput } from '../src/billing.js';

const PG_URL = process.env.DATABASE_URL ?? 'postgresql://gateway:local-development-only@127.0.0.1:5432/gateway';
const REDIS_URL = process.env.REDIS_URL ?? 'redis://127.0.0.1:6379';

type Project = { id: string; keyId: string; budgetMicros: number | null; rpm: number; keyRpm: number; concurrency: number };
let admin: Pool | undefined;
let pool: Pool | undefined;
let redis: Redis | undefined;
let billing: Billing | undefined;
let schema = '';
let workspaceId = '';
let userId = '';
let skipReason: string | null = null;
const projects: Project[] = [];

function guard(t: TestContext): boolean {
  if (skipReason) {
    t.skip(skipReason);
    return false;
  }
  return true;
}

function db(): Pool { if (!pool) throw new Error('pool unavailable'); return pool; }
function cache(): Redis { if (!redis) throw new Error('redis unavailable'); return redis; }
function bill(): Billing { if (!billing) throw new Error('billing unavailable'); return billing; }

async function newProject(opts: { budgetMicros?: number | null; rpm?: number; keyRpm?: number; concurrency?: number } = {}): Promise<Project> {
  const id = randomUUID();
  const keyId = randomUUID();
  const budget = opts.budgetMicros === undefined ? null : opts.budgetMicros;
  const rpm = opts.rpm ?? 1000;
  const keyRpm = opts.keyRpm ?? 1000;
  const concurrency = opts.concurrency ?? 10;
  await db().query(
    `INSERT INTO projects (id, workspace_id, name, budget_micros, rpm, concurrency)
     VALUES ($1, $2, $3, $4, $5, $6)`, [id, workspaceId, `project-${id.slice(0, 8)}`, budget, rpm, concurrency]);
  await db().query(
    `INSERT INTO api_keys (id, project_id, name, key_hash, prefix, key_rpm)
     VALUES ($1, $2, 'test', $3, 'sk-test', $4)`, [keyId, id, randomUUID(), keyRpm]);
  const project = { id, keyId, budgetMicros: budget, rpm, keyRpm, concurrency };
  projects.push(project);
  return project;
}

function params(project: Project, overrides: Partial<AdmitInput> = {}): AdmitInput {
  return {
    requestId: randomUUID(),
    projectId: project.id,
    keyId: project.keyId,
    period: '2026-11',
    reserveMicros: 1000,
    rpm: project.rpm,
    keyRpm: project.keyRpm,
    concurrency: project.concurrency,
    budgetMicros: project.budgetMicros ?? -1,
    routeVersion: { id: randomUUID(), version: 1 },
    priceSnapshot: { currency: 'USD', inputPerMillion: 1000, outputPerMillion: 2000 },
    ...overrides,
  };
}

async function removeProjectKeys(projectId: string): Promise<void> {
  if (!redis) return;
  const pattern = `billing:{${projectId}}:*`;
  let cursor = '0';
  do {
    const [next, keys] = await redis.scan(cursor, 'MATCH', pattern, 'COUNT', 100);
    cursor = next;
    if (keys.length) await redis.del(...keys);
  } while (cursor !== '0');
}

before(async () => {
  try {
    admin = new Pool({ connectionString: PG_URL, max: 2 });
    await admin.query('SELECT 1');
    redis = new Redis(REDIS_URL, { maxRetriesPerRequest: 1, connectTimeout: 3000 });
    await redis.ping();
    schema = `billing_test_${process.pid}_${Date.now().toString(36)}`;
    await admin.query(`CREATE SCHEMA "${schema}"`);
    pool = new Pool({ connectionString: PG_URL, max: 8, options: `-c search_path=${schema}` });
    const migration = await readFile(fileURLToPath(new URL('../migrations/001.sql', import.meta.url)), 'utf8');
    await pool.query(migration);
    workspaceId = randomUUID();
    userId = randomUUID();
    await pool.query('INSERT INTO workspaces (id, name) VALUES ($1, $2)', [workspaceId, 'test workspace']);
    await pool.query(
      `INSERT INTO users (id, workspace_id, email, password_hash, role) VALUES ($1, $2, $3, 'x', 'admin')`,
      [userId, workspaceId, `billing-${userId.slice(0, 8)}@example.test`]);
    billing = new Billing(pool, redis);
    await billing.initialize();
  } catch (err) {
    skipReason = `PostgreSQL/Redis unavailable: ${err instanceof Error ? err.message : String(err)}`;
  }
});

beforeEach(async () => {
  if (skipReason || !billing) return;
  // Fault cases schedule automatic recovery. Drain and restart that worker so
  // the next case cannot inherit a pending rebuild of a newly admitted project.
  await billing.close();
  await billing.initialize();
});

after(async () => {
  await billing?.close().catch(() => undefined);
  for (const project of projects) await removeProjectKeys(project.id).catch(() => undefined);
  await pool?.end().catch(() => undefined);
  await redis?.quit().catch(() => undefined);
  if (admin && schema) await admin.query(`DROP SCHEMA IF EXISTS "${schema}" CASCADE`).catch(() => undefined);
  await admin?.end().catch(() => undefined);
});


test('admit -> dispatch -> settle is durable and idempotent', async (t) => {
  if (!guard(t)) return;
  const b = bill();
  const project = await newProject({ budgetMicros: 1_000_000 });
  const input = params(project, { reserveMicros: 1000 });
  await b.admit(input);
  const reserved = await db().query('SELECT status FROM requests WHERE request_id = $1', [input.requestId]);
  assert.equal(reserved.rows[0].status, 'reserved');
  await b.dispatch(input.requestId);
  await b.heartbeat(input.requestId);
  await b.settle(input.requestId, { actualMicros: 700, inputTokens: 10, outputTokens: 20, status: 'success', attempts: [{ attempt: 1, status: 'success' }] });
  const row = (await db().query(
    'SELECT status, actual_micros::text AS actual, input_tokens::text AS input_tokens FROM requests WHERE request_id = $1',
    [input.requestId])).rows[0];
  assert.equal(row.status, 'success');
  assert.equal(Number(row.actual), 700);
  assert.equal(Number(row.input_tokens), 10);
  const ledger = await db().query(
    'SELECT event_id, amount_micros::text AS amount, kind, estimated FROM ledger WHERE request_id = $1', [input.requestId]);
  assert.equal(ledger.rowCount, 1);
  assert.equal(ledger.rows[0].event_id, `settle:${input.requestId}`);
  assert.equal(Number(ledger.rows[0].amount), 700);
  assert.equal(ledger.rows[0].kind, 'settle');
  assert.equal(ledger.rows[0].estimated, false);
  assert.equal((await db().query('SELECT 1 FROM outbox WHERE event_id = $1', [`request.settled:${input.requestId}`])).rowCount, 1);
  const budget = await cache().hmget(`billing:{${project.id}}:budget:2026-11`, 'spent', 'reserved');
  assert.equal(Number(budget[0]), 700);
  assert.equal(Number(budget[1]), 0);
  await b.settle(input.requestId, { actualMicros: 700, inputTokens: 10, outputTokens: 20, status: 'success', attempts: [] });
  assert.equal((await db().query('SELECT count(*)::int AS n FROM ledger WHERE request_id = $1', [input.requestId])).rows[0].n, 1);
  assert.equal(await b.ready(), true);
});

test('project token bucket rejects excess requests and does not refund rate tokens', async (t) => {
  if (!guard(t)) return;
  const b = bill();
  const project = await newProject({ budgetMicros: null, rpm: 1, keyRpm: 1, concurrency: 10 });
  const first = params(project);
  await b.admit(first);
  await assert.rejects(
    () => b.admit(params(project)),
    (err: unknown) => err instanceof BillingError && err.statusCode === 429 && err.code === 'rate_limit_exceeded');
  await b.cancelBeforeDispatch(first.requestId);
  await assert.rejects(
    () => b.admit(params(project)),
    (err: unknown) => err instanceof BillingError && err.statusCode === 429 && err.code === 'rate_limit_exceeded');
  const failed = await db().query("SELECT count(*)::int AS n FROM requests WHERE project_id = $1 AND status = 'failed'", [project.id]);
  assert.equal(failed.rows[0].n, 2);
  assert.equal(await b.ready(), true);
});

test('concurrency lease blocks, cancel releases, settle ends heartbeat', async (t) => {
  if (!guard(t)) return;
  const b = bill();
  const project = await newProject({ budgetMicros: 1_000_000, rpm: 1000, keyRpm: 1000, concurrency: 1 });
  const first = params(project);
  await b.admit(first);
  await assert.rejects(
    () => b.admit(params(project)),
    (err: unknown) => err instanceof BillingError && err.statusCode === 429 && err.code === 'concurrency_limit_exceeded');
  await b.heartbeat(first.requestId);
  await b.cancelBeforeDispatch(first.requestId);
  await b.cancelBeforeDispatch(first.requestId);
  const second = params(project);
  await b.admit(second);
  await b.heartbeat(second.requestId);
  await b.settle(second.requestId, { actualMicros: 0, inputTokens: null, outputTokens: null, status: 'cancelled', attempts: [] });
  await assert.rejects(
    () => b.heartbeat(second.requestId),
    (err: unknown) => err instanceof BillingError && err.statusCode === 410 && err.code === 'lease_lost');
  assert.equal(await b.ready(), true);
});

test('atomic budget reservation does not oversubscribe under concurrency', async (t) => {
  if (!guard(t)) return;
  const b = bill();
  const project = await newProject({ budgetMicros: 1000, rpm: 100000, keyRpm: 100000, concurrency: 100 });
  const inputs = Array.from({ length: 10 }, () => params(project, { reserveMicros: 600 }));
  const results = await Promise.allSettled(inputs.map((input) => b.admit(input)));
  const accepted = results.flatMap((result, index) => result.status === 'fulfilled' ? [inputs[index]] : []);
  const rejected = results.filter((result): result is PromiseRejectedResult => result.status === 'rejected');
  assert.ok(accepted.length <= 1, `accepted ${accepted.length} reservations over budget`);
  assert.ok(rejected.every((result) => result.reason instanceof BillingError && result.reason.code === 'budget_exceeded'), rejected.map((r) => String((r.reason as Error).message) + ':' + String((r.reason as { code?: string }).code)).join(','));
  const budget = await cache().hmget(`billing:{${project.id}}:budget:2026-11`, 'spent', 'reserved');
  assert.ok(Number(budget[1]) <= 1000);
  for (const input of accepted) await b.cancelBeforeDispatch(input.requestId);
  const live = await db().query(
    "SELECT count(*)::int AS n FROM requests WHERE project_id = $1 AND status IN ('preparing','reserved','dispatched')", [project.id]);
  assert.equal(live.rows[0].n, 0);
  assert.equal(await b.ready(), true);
});


test('missing usage is charged conservatively and can be adjusted', async (t) => {
  if (!guard(t)) return;
  const b = bill();
  const project = await newProject({ budgetMicros: 1_000_000 });
  const input = params(project, { reserveMicros: 1000 });
  await b.admit(input);
  await b.dispatch(input.requestId);
  await b.settle(input.requestId, { actualMicros: null, inputTokens: null, outputTokens: null, status: 'success', attempts: [] });
  const unknown = (await db().query(
    'SELECT status, actual_micros::text AS actual FROM requests WHERE request_id = $1', [input.requestId])).rows[0];
  assert.equal(unknown.actual, null);
  const estimate = (await db().query(
    'SELECT amount_micros::text AS amount, estimated, kind FROM ledger WHERE event_id = $1', [`settle:${input.requestId}`])).rows[0];
  assert.equal(Number(estimate.amount), 1000);
  assert.equal(estimate.estimated, true);
  assert.equal(estimate.kind, 'estimate');
  assert.equal(Number((await cache().hmget(`billing:{${project.id}}:budget:2026-11`, 'spent'))[0]), 1000);

  await b.settle(input.requestId, { actualMicros: 400, inputTokens: 5, outputTokens: 6, status: 'success', attempts: [] });
  const state = (await db().query('SELECT dirty FROM billing_state WHERE project_id = $1', [project.id])).rows[0];
  assert.equal(state.dirty, true);
  const adjustment = (await db().query(
    'SELECT amount_micros::text AS amount FROM ledger WHERE event_id = $1', [`adjust:${input.requestId}:400`])).rows[0];
  assert.equal(Number(adjustment.amount), -600);
  await b.reconcile();
  const budget = await cache().hmget(`billing:{${project.id}}:budget:2026-11`, 'spent', 'reserved');
  assert.equal(Number(budget[0]), 400);
  assert.equal(Number(budget[1]), 0);
  assert.equal((await db().query('SELECT dirty FROM billing_state WHERE project_id = $1', [project.id])).rows[0].dirty, false);
  assert.equal(await b.ready(), true);
});

test('redis loss rebuilds live reservations from PostgreSQL with a new generation', async (t) => {
  if (!guard(t)) return;
  const b = bill();
  const project = await newProject({ budgetMicros: 1_000_000 });
  const input = params(project, { reserveMicros: 1000 });
  await b.admit(input);
  const oldGeneration = await cache().get(`billing:{${project.id}}:gen`);
  assert.ok(oldGeneration);
  await removeProjectKeys(project.id);
  assert.equal(await cache().exists(`billing:{${project.id}}:gen`), 0);
  await b.reconcile();
  const newGeneration = await cache().get(`billing:{${project.id}}:gen`);
  assert.ok(newGeneration);
  assert.notEqual(newGeneration, oldGeneration);
  assert.equal(await cache().hget(`billing:{${project.id}}:req:${input.requestId}`, 'state'), 'reserved');
  const budget = await cache().hmget(`billing:{${project.id}}:budget:2026-11`, 'spent', 'reserved');
  assert.equal(Number(budget[0]), 0);
  assert.equal(Number(budget[1]), 1000);
  await b.cancelBeforeDispatch(input.requestId);
  assert.equal((await cache().hmget(`billing:{${project.id}}:budget:2026-11`, 'reserved'))[0], '0');
  assert.equal(await b.ready(), true);
});

test('dispatched requests with no outcome become conservative unknown charges', async (t) => {
  if (!guard(t)) return;
  const b = bill();
  const project = await newProject({ budgetMicros: 1_000_000 });
  const input = params(project, { reserveMicros: 1000 });
  await b.admit(input);
  await b.dispatch(input.requestId);
  await db().query("UPDATE requests SET updated_at = now() - interval '11 minutes' WHERE request_id = $1", [input.requestId]);
  await b.reconcile();
  const request = (await db().query(
    'SELECT status, actual_micros::text AS actual FROM requests WHERE request_id = $1', [input.requestId])).rows[0];
  assert.equal(request.status, 'unknown');
  assert.equal(request.actual, null);
  const ledger = (await db().query(
    'SELECT amount_micros::text AS amount, kind, estimated FROM ledger WHERE event_id = $1', [`estimate:${input.requestId}`])).rows[0];
  assert.equal(Number(ledger.amount), 1000);
  assert.equal(ledger.kind, 'estimate');
  assert.equal(ledger.estimated, true);
  const budget = await cache().hmget(`billing:{${project.id}}:budget:2026-11`, 'spent', 'reserved');
  assert.equal(Number(budget[0]), 1000);
  assert.equal(Number(budget[1]), 0);
  assert.equal(await b.ready(), true);
});

test('input validation rejects unsafe numbers and non-uuid ids', async (t) => {
  if (!guard(t)) return;
  const b = bill();
  const project = await newProject({ budgetMicros: 1_000_000 });
  const bad = (overrides: Partial<AdmitInput>) => () => b.admit({ ...params(project), ...overrides });
  const expect400 = (code = 'invalid_request') => (err: unknown) => err instanceof BillingError && err.statusCode === 400 && err.code === code;
  await assert.rejects(bad({ requestId: 'not-a-uuid' }), expect400());
  await assert.rejects(bad({ reserveMicros: Number.MAX_SAFE_INTEGER + 1 }), expect400());
  await assert.rejects(bad({ reserveMicros: -1 }), expect400());
  await assert.rejects(bad({ budgetMicros: -2 }), expect400());
  await assert.rejects(bad({ rpm: 0 }), expect400());
  await assert.rejects(bad({ period: '2026-13' }), expect400('invalid_period'));
  const requestId = randomUUID();
  await assert.rejects(
    () => b.settle(requestId, { actualMicros: -1, inputTokens: null, outputTokens: null, status: 'error', attempts: [] }),
    expect400());
});


test('concurrent reconciliation is serialized and rebuilds every live reservation', async (t) => {
  if (!guard(t)) return;
  const b = bill();
  const project = await newProject({ budgetMicros: 1_000_000 });
  const first = params(project, { reserveMicros: 1200 });
  const second = params(project, { reserveMicros: 800 });
  await b.admit(first);
  await b.admit(second);
  await b.dispatch(second.requestId);
  await removeProjectKeys(project.id);
  await Promise.all(Array.from({ length: 5 }, () => b.reconcile()));
  const generation = await cache().get(`billing:{${project.id}}:gen`);
  assert.ok(generation);
  assert.equal(await cache().hget(`billing:{${project.id}}:req:${first.requestId}`, 'state'), 'reserved');
  assert.equal(await cache().hget(`billing:{${project.id}}:req:${second.requestId}`, 'state'), 'dispatched');
  const budget = await cache().hmget(`billing:{${project.id}}:budget:2026-11`, 'spent', 'reserved');
  assert.equal(Number(budget[0]), 0);
  assert.equal(Number(budget[1]), 2000);
  const state = (await db().query('SELECT paused, dirty FROM billing_state WHERE project_id = $1', [project.id])).rows[0];
  assert.equal(state.paused, false);
  assert.equal(state.dirty, false);
  assert.equal(await b.ready(), true);
});


test('late actual usage corrects a conservative unknown charge without double billing', async (t) => {
  if (!guard(t)) return;
  const b = bill();
  const project = await newProject({ budgetMicros: 1_000_000 });
  const input = params(project, { reserveMicros: 1000 });
  await b.admit(input);
  await b.dispatch(input.requestId);
  await db().query("UPDATE requests SET updated_at = now() - interval '11 minutes' WHERE request_id = $1", [input.requestId]);
  await b.reconcile();
  await b.settle(input.requestId, { actualMicros: 800, inputTokens: 7, outputTokens: 8, status: 'success', attempts: [] });
  await b.reconcile();
  const total = (await db().query(
    'SELECT COALESCE(SUM(amount_micros), 0)::text AS total FROM ledger WHERE request_id = $1', [input.requestId])).rows[0];
  assert.equal(Number(total.total), 800);
  const request = (await db().query(
    'SELECT status, actual_micros::text AS actual FROM requests WHERE request_id = $1', [input.requestId])).rows[0];
  assert.equal(request.status, 'success');
  assert.equal(Number(request.actual), 800);
  assert.equal(Number((await cache().hmget(`billing:{${project.id}}:budget:2026-11`, 'spent'))[0]), 800);
  assert.equal(await b.ready(), true);
});


test('redis key loss cannot reset ledger spend for an idle project', async (t) => {
  if (!guard(t)) return;
  const b = bill();
  const project = await newProject({ budgetMicros: 1000 });
  const settled = params(project, { reserveMicros: 700 });
  await b.admit(settled);
  await b.settle(settled.requestId, { actualMicros: 700, inputTokens: 1, outputTokens: 1, status: 'success', attempts: [] });
  await removeProjectKeys(project.id);
  assert.equal(await b.ready(), false);
  const retry = params(project, { reserveMicros: 100 });
  await assert.rejects(
    () => b.admit(retry),
    (err: unknown) => err instanceof BillingError && err.statusCode === 503 && (err.code === 'billing_recovery_required' || err.code === 'billing_recovering'));
  let recovered = false;
  for (let i = 0; i < 60; i += 1) {
    await new Promise((resolve) => setTimeout(resolve, 100));
    const row = (await db().query('SELECT dirty FROM billing_state WHERE project_id = $1', [project.id])).rows[0];
    if (row?.dirty === false && (await cache().exists(`billing:{${project.id}}:gen`)) === 1) { recovered = true; break; }
  }
  assert.equal(recovered, true);
  assert.equal(await b.ready(), true);
  const spent = (await cache().hmget(`billing:{${project.id}}:budget:2026-11`, 'spent'))[0];
  assert.equal(spent, '700');
  await b.admit(retry);
  const identity = (await db().query('SELECT redis_run_id FROM billing_state WHERE project_id = $1', [project.id])).rows[0];
  assert.ok(identity.redis_run_id);
  const budget = await cache().hmget(`billing:{${project.id}}:budget:2026-11`, 'spent', 'reserved');
  assert.equal(Number(budget[0]), 700);
  assert.equal(Number(budget[1]), 100);
  await b.cancelBeforeDispatch(retry.requestId);
  assert.equal(await b.ready(), true);
});

test('actual settlement is immutable after the first unknown-to-actual correction',async t=>{
  if(!guard(t))return;
  const p=await newProject({budgetMicros:10000});const input=params(p,{reserveMicros:1000});
  await bill().admit(input);await bill().dispatch(input.requestId);
  await bill().settle(input.requestId,{actualMicros:null,inputTokens:null,outputTokens:null,status:'unknown',attempts:[]});
  await bill().settle(input.requestId,{actualMicros:400,inputTokens:1,outputTokens:1,status:'success',attempts:[]});
  await bill().settle(input.requestId,{actualMicros:400,inputTokens:1,outputTokens:1,status:'success',attempts:[]});
  await assert.rejects(()=>bill().settle(input.requestId,{actualMicros:800,inputTokens:2,outputTokens:2,status:'success',attempts:[]}),e=>(e as BillingError).code==='settlement_conflict');
  assert.equal((await db().query('SELECT sum(amount_micros)::text AS amount FROM ledger WHERE request_id=$1',[input.requestId])).rows[0].amount,'400');
});

test('known-period budget hash loss fences admission without resetting spend',async t=>{
  if(!guard(t))return;
  const p=await newProject({budgetMicros:1000});const input=params(p,{reserveMicros:800});
  await bill().admit(input);await bill().dispatch(input.requestId);
  await bill().settle(input.requestId,{actualMicros:800,inputTokens:1,outputTokens:1,status:'success',attempts:[]});
  await cache().del(`billing:{${p.id}}:budget:2026-11`);
  await assert.rejects(()=>bill().admit(params(p,{reserveMicros:300})),e=>(e as BillingError).statusCode===503);
  await bill().reconcile();
  assert.equal(await cache().hget(`billing:{${p.id}}:budget:2026-11`,'spent'),'800');
});

test('per-key rate limit and request-period accounting remain independent',async t=>{
  if(!guard(t))return;
  const p=await newProject({keyRpm:1,rpm:1000});const one=params(p,{reserveMicros:100});
  await bill().admit(one);await bill().cancelBeforeDispatch(one.requestId);
  await assert.rejects(()=>bill().admit(params(p)),e=>(e as BillingError).code==='rate_limit_exceeded');
  const months=await newProject({budgetMicros:1000});
  const november=params(months,{period:'2026-11',reserveMicros:800,priceSnapshot:{version:'old'}});
  const december=params(months,{period:'2026-12',reserveMicros:800,priceSnapshot:{version:'new'}});
  await bill().admit(november);await bill().dispatch(november.requestId);await bill().admit(december);
  await bill().settle(november.requestId,{actualMicros:20,inputTokens:10,outputTokens:5,status:'completed',attempts:[]});
  assert.equal(await cache().hget(`billing:{${months.id}}:budget:2026-11`,'spent'),'20');
  assert.equal(await cache().hget(`billing:{${months.id}}:budget:2026-12`,'reserved'),'800');
  assert.deepEqual((await db().query('SELECT price_snapshot FROM requests WHERE request_id=$1',[november.requestId])).rows[0].price_snapshot,{version:'old'});
  await bill().cancelBeforeDispatch(december.requestId);
});

test('intent and reservation creation roll back together on a reservation constraint failure',async t=>{
  if(!guard(t))return;
  const p=await newProject();const input=params(p,{reserveMicros:1000});
  await db().query('ALTER TABLE reservations ADD CONSTRAINT injected_reservation_failure CHECK(reserved_micros<500) NOT VALID');
  try{
    await assert.rejects(()=>bill().admit(input));
    assert.equal((await db().query('SELECT 1 FROM requests WHERE request_id=$1',[input.requestId])).rowCount,0);
    assert.equal((await db().query('SELECT 1 FROM reservations WHERE request_id=$1',[input.requestId])).rowCount,0);
  }finally{await db().query('ALTER TABLE reservations DROP CONSTRAINT injected_reservation_failure');}
});

test('close waits for accepted financial work even before its project lookup finishes',async t=>{
  if(!guard(t))return;
  const p=await newProject();const input=params(p,{reserveMicros:100});
  await bill().admit(input);await bill().dispatch(input.requestId);
  const cold=new Billing(db(),cache());await cold.initialize();
  const pool=db(),original=pool.query;
  let release!:()=>void,entered!:()=>void;
  const gate=new Promise<void>(r=>release=r),started=new Promise<void>(r=>entered=r);
  (pool as any).query=function(...args:any[]){
    if(typeof args[0]==='string'&&args[0].startsWith('SELECT project_id::text AS project_id FROM requests')&&args[1]?.[0]===input.requestId){entered();return gate.then(()=>(original as any).apply(pool,args));}
    return (original as any).apply(pool,args);
  };
  try{
    const settled=cold.settle(input.requestId,{actualMicros:20,inputTokens:10,outputTokens:5,status:'completed',attempts:[]});
    await started;let closed=false;const closing=cold.close().then(()=>{closed=true;});
    await new Promise(r=>setImmediate(r));assert.equal(closed,false);
    release();await Promise.all([settled,closing]);
    assert.equal((await pool.query('SELECT actual_micros FROM requests WHERE request_id=$1',[input.requestId])).rows[0].actual_micros,'20');
  }finally{release();pool.query=original;await cold.close();}
});

test('settlement outbox failure rolls back all durable writes and preserves the Redis reservation',async t=>{
  if(!guard(t))return;
  const p=await newProject();const input=params(p,{reserveMicros:100});
  await bill().admit(input);await bill().dispatch(input.requestId);
  await db().query(`ALTER TABLE outbox ADD CONSTRAINT injected_settlement_failure CHECK(event_id <> 'request.settled:${input.requestId}') NOT VALID`);
  const settlement={actualMicros:20,inputTokens:10,outputTokens:5,status:'completed',attempts:[]};
  try{
    await assert.rejects(()=>bill().settle(input.requestId,settlement));
    assert.equal((await db().query('SELECT * FROM ledger WHERE request_id=$1',[input.requestId])).rowCount,0);
    assert.equal((await db().query('SELECT actual_micros FROM requests WHERE request_id=$1',[input.requestId])).rows[0].actual_micros,null);
    assert.deepEqual(await cache().hmget(`billing:{${p.id}}:budget:2026-11`,'spent','reserved'),['0','100']);
  }finally{await db().query('ALTER TABLE outbox DROP CONSTRAINT injected_settlement_failure');}
  await bill().settle(input.requestId,settlement);
  assert.equal((await db().query('SELECT actual_micros FROM requests WHERE request_id=$1',[input.requestId])).rows[0].actual_micros,'20');
});
