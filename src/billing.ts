import { randomUUID } from 'node:crypto';
import { readFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import type { Pool, PoolClient } from 'pg';
import type { Redis } from 'ioredis';
import { KeyedQueue } from './keyed-queue.js';

export interface AdmitInput {
  requestId: string;
  projectId: string;
  keyId: string;
  period: string;
  reserveMicros: number;
  rpm: number;
  keyRpm: number;
  concurrency: number;
  budgetMicros: number;
  routeVersion: unknown;
  priceSnapshot: unknown;
}

export interface SettleInput {
  actualMicros: number | null;
  inputTokens: number | null;
  outputTokens: number | null;
  status: string;
  attempts: unknown[];
}

/** Validated settle data carried across the in-process settlement queue. */
interface PreparedSettle {
  actual: number | null;
  inputTokens: number | null;
  outputTokens: number | null;
  status: string;
  attempts: unknown[];
  attemptsJson: string;
  usageState: 'actual' | 'unknown';
}

export class BillingError extends Error {
  readonly statusCode: number;
  readonly code: string;
  readonly details?: Record<string, unknown>;
  constructor(statusCode: number, code: string, message: string, details?: Record<string, unknown>) {
    super(message);
    this.name = 'BillingError';
    this.statusCode = statusCode;
    this.code = code;
    this.details = details;
  }
}

export type ScriptName = 'admit' | 'dispatch' | 'settle' | 'cancel' | 'heartbeat';
const SCRIPT_FILES: Record<ScriptName, string> = {
  admit: 'admit.lua',
  dispatch: 'dispatch.lua',
  settle: 'settle.lua',
  cancel: 'cancel.lua',
  heartbeat: 'heartbeat.lua',
};
const MAX_SAFE = Number.MAX_SAFE_INTEGER;
const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
const PERIOD_RE = /^\d{4}-(0[1-9]|1[0-2])$/;

const BILLING_STATE_DDL = `
CREATE TABLE IF NOT EXISTS billing_state (
  project_id uuid PRIMARY KEY REFERENCES projects(id) ON DELETE CASCADE,
  generation bigint NOT NULL DEFAULT 1 CHECK (generation > 0),
  paused boolean NOT NULL DEFAULT false,
  dirty boolean NOT NULL DEFAULT false,
  redis_run_id text,
  last_reconciled_at timestamptz,
  updated_at timestamptz NOT NULL DEFAULT now()
)`;

export function isUuid(value: unknown): value is string {
  return typeof value === 'string' && UUID_RE.test(value);
}

function requireUuid(value: unknown, field: string): string {
  if (!isUuid(value)) throw new BillingError(400, 'invalid_request', `${field} must be a UUID`);
  return value;
}

function requireSafeInt(value: unknown, field: string, min: number, max = MAX_SAFE): number {
  if (typeof value !== 'number' || !Number.isSafeInteger(value) || value < min || value > max) {
    throw new BillingError(400, 'invalid_request', `${field} must be a safe integer >= ${min}`);
  }
  return value;
}

function requirePeriod(value: unknown): string {
  if (typeof value !== 'string' || !PERIOD_RE.test(value)) {
    throw new BillingError(400, 'invalid_period', 'period must be YYYY-MM');
  }
  return value;
}

function requireJson(value: unknown, field: string): string {
  try {
    const encoded = JSON.stringify(value ?? null);
    if (encoded.length > 65536) throw new Error('too large');
    return encoded;
  } catch {
    throw new BillingError(400, 'invalid_request', `${field} must be JSON-serializable`);
  }
}

const keyFor = (projectId: string) => {
  const tag = `{${projectId}}`;
  return {
    gen: `billing:${tag}:gen`,
    paused: `billing:${tag}:paused`,
    projectBucket: `billing:${tag}:rate:project`,
    keyBucket: (keyId: string) => `billing:${tag}:rate:key:${keyId}`,
    concurrency: `billing:${tag}:conc`,
    reservations: `billing:${tag}:resv`,
    budget: (period: string) => `billing:${tag}:budget:${period}`,
    request: (requestId: string) => `billing:${tag}:req:${requestId}`,
    requests: `billing:${tag}:reqs`,
    periods: `billing:${tag}:periods`,
  };
};

export class Billing {
  private readonly pool: Pool;
  private readonly redis: Redis;
  private readonly lua = new Map<ScriptName, string>();
  private readonly shas = new Map<ScriptName, string>();
  private readonly leaseMs = 45_000;
  private readonly requestCache = new Map<string, { projectId: string; owner: string }>();
  private initialized = false;
  private reconcileTimer: NodeJS.Timeout | null = null;
  private reconcileSoonTimer: NodeJS.Timeout | null = null;
  private currentRunId = '';
  private runIdCheckedAt = 0;
  private expectedRunId = '';
  private recovering: Promise<void> | null = null;
  private readonly settlementQueue = new KeyedQueue();
  private readonly financialOperations = new Set<Promise<void>>();

  constructor(pool: Pool, redis: Redis) {
    this.pool = pool;
    this.redis = redis;
    redis.on('ready',()=>{this.currentRunId='';this.runIdCheckedAt=0;});
  }

  async initialize(): Promise<void> {
    await this.pool.query(BILLING_STATE_DDL);
    await this.pool.query('ALTER TABLE billing_state ADD COLUMN IF NOT EXISTS redis_run_id text');
    await this.pool.query(
      `SELECT request_id, project_id, period, reserve_micros, price_snapshot, status,
              actual_micros, input_tokens, output_tokens FROM requests LIMIT 0`,
    );
    await this.pool.query(
      `SELECT request_id, project_id, period, reserved_micros, actual_micros, status,
              lease_expires_at FROM reservations LIMIT 0`,
    );
    await this.pool.query(`SELECT event_id, request_id, project_id, period, amount_micros, kind, estimated FROM ledger LIMIT 0`);
    await this.redis.ping();
    for (const name of Object.keys(SCRIPT_FILES) as ScriptName[]) {
      const url = new URL(`./lua/${SCRIPT_FILES[name]}`, import.meta.url);
      const source = await readFile(fileURLToPath(url), 'utf8');
      this.lua.set(name, source);
      const sha = (await this.redis.script('LOAD', source)) as unknown as string;
      this.shas.set(name, sha);
    }
    this.initialized = true;
    this.expectedRunId = await this.redisRunId(true);
    await this.reconcile();
    if (!this.reconcileTimer) {
      this.reconcileTimer = setInterval(() => { void this.reconcile().catch(() => undefined); }, 30000);
      this.reconcileTimer.unref();
    }
  }

  async ready(): Promise<boolean> {
    try {
      await this.pool.query('SELECT 1');
      const redis = await this.redis.ping();
      if (redis !== 'PONG') return false;
      const runId = await this.redisRunId(true);
      if (runId !== this.expectedRunId) return false;
      // A recovering project is fenced by generation() without removing healthy projects from service.
      const projects = await this.pool.query('SELECT project_id::text AS project_id FROM billing_state');
      for (const row of projects.rows) {
        if ((await this.redis.exists(keyFor(row.project_id).gen)) === 0) {
          await this.pool.query('UPDATE billing_state SET dirty = true, updated_at = now() WHERE project_id = $1', [row.project_id]).catch(() => undefined);
          this.scheduleReconcileSoon();
          return false;
        }
      }
      return true;
    } catch {
      return false;
    }
  }

  private async redisRunId(force = false): Promise<string> {
    if (!force && this.currentRunId && Date.now() - this.runIdCheckedAt < 2000) return this.currentRunId;
    const info = await this.redis.info('server');
    const match = /^run_id:(.+)$/m.exec(info);
    if (!match?.[1]) throw new BillingError(503, 'billing_unavailable', 'redis identity unavailable');
    this.currentRunId = match[1].trim();
    this.runIdCheckedAt = Date.now();
    return this.currentRunId;
  }

  private async runScript(name: ScriptName, keys: string[], args: Array<string | number>): Promise<unknown> {
    const source = this.lua.get(name);
    if (!source) throw new BillingError(503, 'billing_not_initialized', 'billing is not initialized');
    const sha = this.shas.get(name);
    const argv = args.map(String);
    if (sha) {
      try {
        return await this.redis.evalsha(sha, keys.length, ...keys, ...argv);
      } catch (err) {
        if (!(err instanceof Error) || !err.message.includes('NOSCRIPT')) throw err;
      }
    }
    const result = await this.redis.eval(source, keys.length, ...keys, ...argv);
    this.shas.set(name, (await this.redis.script('LOAD', source)) as unknown as string);
    return result;
  }

  async admit(input: AdmitInput): Promise<void> {
    this.ensureInitialized();
    const requestId = requireUuid(input.requestId, 'requestId');
    const projectId = requireUuid(input.projectId, 'projectId');
    const keyId = requireUuid(input.keyId, 'keyId');
    const period = requirePeriod(input.period);
    const reserve = requireSafeInt(input.reserveMicros, 'reserveMicros', 0);
    const rpm = requireSafeInt(input.rpm, 'rpm', 1, 1_000_000);
    const keyRpm = requireSafeInt(input.keyRpm, 'keyRpm', 1, 1_000_000);
    const concurrency = requireSafeInt(input.concurrency, 'concurrency', 1, 1_000_000);
    const budget = requireSafeInt(input.budgetMicros, 'budgetMicros', -1);
    const routeVersion = requireJson(input.routeVersion, 'routeVersion');
    const priceSnapshot = requireJson(input.priceSnapshot, 'priceSnapshot');
    const owner = randomUUID();
    const runId = await this.redisRunId();
    if (runId !== this.expectedRunId) {
      await this.markDirty(projectId);
      throw new BillingError(503, 'billing_recovery_required', 'redis identity changed; admission is fenced');
    }

    // One committed statement creates both durable intent rows. The conflict
    // path stays read-only; a concurrent uncommitted winner may require a fresh snapshot.
    const intent = await this.pool.query(`WITH inserted AS (
      INSERT INTO requests(request_id,project_id,key_id,period,reserve_micros,route_version,price_snapshot,status)
      VALUES($1,$2,$3,$4,$5,$6::jsonb,$7::jsonb,'preparing') ON CONFLICT(request_id) DO NOTHING
      RETURNING request_id,project_id,period,reserve_micros,status
    ), prepared AS (
      INSERT INTO reservations(request_id,project_id,period,reserved_micros,status,lease_expires_at)
      SELECT request_id,project_id,period,reserve_micros,'preparing',now()+($8*interval '1 millisecond') FROM inserted
      RETURNING request_id
    ) SELECT project_id::text,period,reserve_micros::text,status,true AS created FROM inserted
      UNION ALL SELECT project_id::text,period,reserve_micros::text,status,false AS created FROM requests
      WHERE request_id=$1 AND NOT EXISTS(SELECT 1 FROM inserted)`,
      [requestId,projectId,keyId,period,reserve,routeVersion,priceSnapshot,this.leaseMs]);
    const row = intent.rows[0] ?? (await this.pool.query(
      `SELECT project_id::text,period,reserve_micros::text,status,false AS created FROM requests WHERE request_id=$1`,[requestId])).rows[0];
    if(!row)throw new BillingError(503,'billing_unavailable','request intent could not be read');
    if(row.project_id!==projectId||row.period!==period||Number(row.reserve_micros)!==reserve){
      throw new BillingError(409,'request_id_conflict','request id already used with different billing parameters');
    }
    if(!row.created){
      if(row.status==='settled'||row.status==='unknown')return;
      if(row.status==='cancelled'||row.status==='failed')throw new BillingError(409,'request_already_final','request is already finalized');
      if(row.status!=='preparing'){
        if(!await this.redis.exists(keyFor(projectId).request(requestId))){
          await this.markDirty(projectId);throw new BillingError(503,'billing_recovery_required','billing state must be reconciled');
        }
        return;
      }
      // Compatibility with an intent interrupted before its reservation was persisted.
      await this.pool.query(`INSERT INTO reservations(request_id,project_id,period,reserved_micros,status,lease_expires_at)
        VALUES($1,$2,$3,$4,'preparing',now()+($5*interval '1 millisecond'))
        ON CONFLICT(request_id) DO UPDATE SET updated_at=now() WHERE reservations.status='preparing'`,
        [requestId,projectId,period,reserve,this.leaseMs]);
    }

    const state = await this.generation(projectId);
    if (state.redis_run_id && state.redis_run_id !== runId) {
      await this.markDirty(projectId);
      throw new BillingError(503, 'billing_recovery_required', 'project billing state belongs to a different redis instance');
    }
    if (!state.redis_run_id) {
      await this.pool.query('UPDATE billing_state SET redis_run_id = $2, updated_at = now() WHERE project_id = $1 AND redis_run_id IS NULL', [projectId, runId]);
    }
    const k = keyFor(projectId);
    let res: unknown[];
    try {
      const currentGeneration = await this.redis.get(k.gen);
      if (currentGeneration === null) {
        const seen = await this.pool.query(
          `SELECT (EXISTS(SELECT 1 FROM ledger WHERE project_id = $1) OR EXISTS(SELECT 1 FROM reservations WHERE project_id = $1 AND request_id <> $2 AND (status IN ('reserved','dispatched','settled','unknown') OR (status = 'preparing' AND created_at < now() - interval '10 seconds')))) AS seen`,
          [projectId, requestId]);
        if (seen.rows[0]?.seen && (await this.redis.get(k.gen)) === null) {
          await this.markDirty(projectId);
          throw new BillingError(503, 'billing_recovery_required', 'billing state must be reconciled');
        }
        await this.redis.set(k.gen, state.generation, 'NX');
      }
      const generation = await this.redis.get(k.gen);
      if (generation !== state.generation) {
        await this.markDirty(projectId);
        throw new BillingError(503, 'billing_recovery_required', 'billing generation is being rebuilt');
      }
      res = await this.runScript('admit', [k.gen, k.paused, k.projectBucket, k.keyBucket(keyId), k.concurrency,
        k.reservations, k.budget(period), k.request(requestId), k.requests, k.periods],
        [state.generation, requestId, keyId, owner, reserve, rpm, keyRpm, concurrency, budget, this.leaseMs, period, runId]) as unknown[];
    } catch (err) {
      if (err instanceof BillingError) throw err;
      throw new BillingError(503, 'billing_unavailable', 'billing store unavailable');
    }
    const code = Number(res[0]);
    const detail = Number(res[1]);
    if (code === 0 || code === 1) {
      this.requestCache.set(requestId, { projectId, owner });
      try {
        await this.pool.query(
          `WITH r AS (
             UPDATE requests SET status = 'reserved', updated_at = now()
             WHERE request_id = $1 AND status = 'preparing' RETURNING request_id
           )
           UPDATE reservations SET status = 'reserved', updated_at = now(),
             lease_expires_at = now() + ($2 * interval '1 millisecond')
           WHERE request_id = $1 AND status = 'preparing'`, [requestId, this.leaseMs]);
      } catch (err) {
        await this.runScript('cancel', [k.gen, k.request(requestId), k.concurrency, k.reservations,
          k.budget(period), k.requests], [state.generation, requestId]).catch(() => undefined);
        await this.markDirty(projectId);
        throw err instanceof BillingError ? err : new BillingError(503, 'billing_unavailable', 'billing reservation could not be persisted');
      }
      return;
    }
    await this.redisFailure(projectId, code);
    if (code === 4 || code === 5 || code === 6 || code === 7 || code === 8) await this.abortIntent(requestId);
    if (code === 2) throw new BillingError(503, 'billing_generation_mismatch', 'billing generation changed; retry after reconciliation');
    if (code === 3) throw new BillingError(503, 'billing_recovering', 'billing is recovering');
    if (code === 4) throw new BillingError(429, 'rate_limit_exceeded', 'request rate limit reached', { retryAfterMs: detail });
    if (code === 5) throw new BillingError(429, 'concurrency_limit_exceeded', 'concurrency limit reached');
    if (code === 6) throw new BillingError(429, 'budget_exceeded', 'project budget would be exceeded', { availableMicros: detail });
    if (code === 7) throw new BillingError(409, 'request_already_final', 'request is already finalized');
    throw new BillingError(503, 'billing_unavailable', 'billing admission failed');
  }

  async dispatch(requestIdInput: string): Promise<void> {
    this.ensureInitialized();
    const requestId = requireUuid(requestIdInput, 'requestId');
    const row = await this.lookupRequest(requestId);
    if (!row) throw new BillingError(404, 'request_not_found', 'request not found');
    if (row.status === 'preparing') throw new BillingError(409, 'not_admitted', 'request has no reservation');
    if (row.status === 'settled' || row.status === 'unknown' || row.status === 'cancelled' || row.status === 'failed') {
      throw new BillingError(409, 'request_already_final', 'request is already finalized');
    }
    if (row.status === 'reserved') {
      const updated = await this.pool.query(
        `UPDATE requests SET status = 'dispatch_intent', updated_at = now()
         WHERE request_id = $1 AND status = 'reserved' RETURNING request_id`, [requestId]);
      if (updated.rowCount === 0) {
        const again = await this.lookupRequest(requestId);
        if (!again || (again.status !== 'dispatch_intent' && again.status !== 'dispatched')) {
          throw new BillingError(409, 'not_admitted', 'request is not dispatchable');
        }
      }
    }
    const state = await this.generation(row.project_id);
    const k = keyFor(row.project_id);
    let res: unknown[];
    try {
      res = await this.runScript('dispatch', [k.gen, k.paused, k.request(requestId), k.concurrency],
        [state.generation, requestId, this.leaseMs]) as unknown[];
    } catch (err) {
      await this.markDirty(row.project_id);
      throw err instanceof BillingError ? err : new BillingError(503, 'billing_unavailable', 'billing store unavailable');
    }
    const code = Number(res[0]);
    if (code !== 0) {
      if (code === 1 || code === 4) await this.markDirty(row.project_id);
      if (code === 2 || code === 3) await this.resetToReserved(requestId);
      if (code === 1) throw new BillingError(503, 'billing_generation_mismatch', 'billing generation changed');
      if (code === 2) throw new BillingError(503, 'billing_recovering', 'billing is recovering');
      if (code === 3) throw new BillingError(409, 'not_admitted', 'request reservation is not dispatchable');
      throw new BillingError(503, 'billing_recovery_required', 'billing reservation is missing');
    }
    await this.pool.query(
      `UPDATE requests SET status = 'dispatched', updated_at = now()
       WHERE request_id = $1 AND status = 'dispatch_intent'`, [requestId]).catch(() => undefined);
  }

  private ensureInitialized(): void {
    if (!this.initialized) throw new BillingError(503, 'billing_not_initialized', 'billing is not initialized');
  }

  /**
   * Queue key for settlement/cancel: the project id when already known locally,
   * otherwise one indexed lookup by request id (no Redis ownership lookup for
   * finalized requests). A missing row falls back to a request-scoped key so the
   * transaction below still reports the authoritative 404/validation error.
   */
  private runFinancial(requestId:string, work:()=>Promise<void|(()=>Promise<void>)>):Promise<void> {
    const operation=this.settlementKey(requestId).then(key=>this.settlementQueue.run(key,work)).then(async afterCommit=>{
      // The process-local slot protects only the durable transaction. Redis finalization
      // may overlap the next transaction, as it did under the authoritative PG lock.
      if(afterCommit)await afterCommit();
    });
    this.financialOperations.add(operation);
    return operation.finally(()=>{this.financialOperations.delete(operation);});
  }

  private async settlementKey(requestId: string): Promise<string> {
    const cached = this.requestCache.get(requestId);
    if (cached) return `project:${cached.projectId}`;
    const r = await this.pool.query('SELECT project_id::text AS project_id FROM requests WHERE request_id = $1', [requestId]);
    const projectId = r.rows[0]?.project_id;
    if (typeof projectId === 'string' && isUuid(projectId)) return `project:${projectId}`;
    return `request:${requestId}`;
  }

  private async generation(projectId: string): Promise<{ generation: string; paused: boolean; dirty: boolean; redis_run_id: string | null }> {
    const read = () => this.pool.query(
      'SELECT generation::text AS generation, paused, dirty, redis_run_id FROM billing_state WHERE project_id = $1',[projectId]);
    let r=await read();
    if(!r.rows[0]){
      await this.pool.query('INSERT INTO billing_state(project_id) VALUES($1) ON CONFLICT(project_id) DO NOTHING',[projectId]);
      r=await read();
    }
    if (!r.rows[0]) throw new BillingError(503, 'billing_unavailable', 'billing state missing');
    const state = r.rows[0];
    if (state.paused || state.dirty) throw new BillingError(503, 'billing_recovering', 'billing is reconciling');
    return state;
  }

  private async lookupRequest(requestId: string): Promise<{ project_id: string; status: string; period: string } | null> {
    const r = await this.pool.query(
      'SELECT project_id::text AS project_id, status, period FROM requests WHERE request_id = $1', [requestId]);
    return r.rows[0] ?? null;
  }

  private async resetToReserved(requestId: string): Promise<void> {
    await this.pool.query(
      `UPDATE requests SET status = 'reserved', updated_at = now()
       WHERE request_id = $1 AND status = 'dispatch_intent'`, [requestId]).catch(() => undefined);
  }

  private async markDirty(projectId: string): Promise<void> {
    await this.pool.query('INSERT INTO billing_state (project_id) VALUES ($1) ON CONFLICT (project_id) DO NOTHING', [projectId]).catch(() => undefined);
    await this.pool.query('UPDATE billing_state SET dirty = true, updated_at = now() WHERE project_id = $1', [projectId]).catch(() => undefined);
    this.scheduleReconcileSoon();
  }

  private scheduleReconcileSoon(): void {
    if (this.reconcileSoonTimer || !this.initialized) return;
    this.reconcileSoonTimer = setTimeout(() => {
      this.reconcileSoonTimer = null;
      void this.reconcile().catch(() => undefined);
    }, 500);
    this.reconcileSoonTimer.unref();
  }

  private async redisFailure(projectId: string, code: number): Promise<void> {
    if (code === 1 || code === 2) await this.markDirty(projectId);
  }

  async settle(requestIdInput: string, input: SettleInput): Promise<void> {
    this.ensureInitialized();
    const requestId = requireUuid(requestIdInput, 'requestId');
    const actual = input.actualMicros === null ? null : requireSafeInt(input.actualMicros, 'actualMicros', 0);
    const inputTokens = input.inputTokens === null ? null : requireSafeInt(input.inputTokens, 'inputTokens', 0);
    const outputTokens = input.outputTokens === null ? null : requireSafeInt(input.outputTokens, 'outputTokens', 0);
    if (typeof input.status !== 'string' || !/^[a-z][a-z0-9_]{0,31}$/.test(input.status)) {
      throw new BillingError(400, 'invalid_request', 'status must be a short lowercase token');
    }
    if (!Array.isArray(input.attempts)) throw new BillingError(400, 'invalid_request', 'attempts must be an array');
    const attemptsJson = requireJson(input.attempts, 'attempts');
    const status = input.status;
    const usageState: 'actual' | 'unknown' = actual === null ? 'unknown' : 'actual';
    // Serialize finalization for the same project in-process BEFORE taking a pool
    // connection: same-project transactions used to queue up inside PostgreSQL
    // holding a client each, starving the pool. The PG advisory xact lock is
    // still taken below and remains the cross-process authority.
    await this.runFinancial(requestId, () => this.performSettle(requestId, {
      actual, inputTokens, outputTokens, status, attempts: input.attempts, attemptsJson, usageState,
    }));
  }

  private async performSettle(requestId: string, prepared: PreparedSettle): Promise<void|(()=>Promise<void>)> {
    const { actual, inputTokens, outputTokens, status, attemptsJson, usageState } = prepared;
    const input = { attempts: prepared.attempts };
    const client = await this.pool.connect();
    let committed = false;
    let released = false;
    const releaseClient = () => {if(!released){released=true;client.release();}};
    let projectId = '';
    let period = '';
    let charge = 0;
    try {
      await client.query('BEGIN');
      const scope=await client.query('SELECT project_id::text FROM requests WHERE request_id=$1',[requestId]);
      if(!scope.rows[0])throw new BillingError(404,'request_not_found','request not found');
      await client.query('SELECT pg_advisory_xact_lock(hashtextextended($1,0))',[`gateway:billing:${scope.rows[0].project_id}`]);
      const found = await client.query(
        `SELECT project_id::text AS project_id, period, reserve_micros::text AS reserve_micros, status,
                actual_micros::text AS actual_micros
           FROM requests WHERE request_id = $1 FOR UPDATE`, [requestId]);
      if (!found.rows[0]) throw new BillingError(404, 'request_not_found', 'request not found');
      const row = found.rows[0];
      projectId = row.project_id;
      period = row.period;
      if (row.status === 'cancelled') throw new BillingError(409, 'request_already_final', 'request was cancelled');
      if (row.status === 'failed') throw new BillingError(409, 'request_already_final', 'request admission failed');
      if (row.status === 'preparing') throw new BillingError(409, 'not_admitted', 'request has no reservation');
      charge = actual ?? Number(row.reserve_micros);
      const existing = await client.query(
        `SELECT event_id, amount_micros::text AS amount_micros, estimated
           FROM ledger WHERE event_id = ANY($1::text[])`,
        [[`settle:${requestId}`, `estimate:${requestId}`]]);
      const baseline = existing.rows.find((r: { event_id: string }) => r.event_id === `settle:${requestId}`) ?? existing.rows[0];
      if (baseline) {
        // Actual usage is final. Repeated delivery is idempotent; a different final
        // amount requires an explicit future administrative correction protocol.
        if(row.actual_micros !== null) {
          if(actual === null || actual === Number(row.actual_micros)) {
            await client.query('COMMIT');committed=true;releaseClient();return;
          }
          throw new BillingError(409,'settlement_conflict','Actual usage was already finalized');
        }
        const prior = Number(baseline.amount_micros);
        if (actual !== null && baseline.estimated) {
          const adjustment = charge - prior;
          await this.insertAdjustment(client, requestId, projectId, period, adjustment, actual, inputTokens, outputTokens, status, attemptsJson);
          await client.query(
            `UPDATE requests SET status = $2, actual_micros = $3, input_tokens = $4, output_tokens = $5, updated_at = now()
             WHERE request_id = $1`, [requestId, status, actual, inputTokens, outputTokens]);
          await client.query(
            `UPDATE reservations SET status = 'settled', actual_micros = $2, lease_expires_at = NULL, updated_at = now()
             WHERE request_id = $1`, [requestId, actual]);
          await client.query('COMMIT');
          committed = true;
          releaseClient();
          return () => this.markDirty(projectId);
        }
        if (actual === null || prior === charge) {
          await client.query('COMMIT');
          committed = true;
          releaseClient();
          if (actual !== null) return () => this.applySettlement(projectId, period, requestId, charge, 'actual');
          return;
        }
        throw new BillingError(409, 'settlement_conflict', 'request was already settled with a different amount');
      }
      const estimated = actual === null;
      const payload = {requestId,projectId,period,status,actualMicros:actual,chargedMicros:charge,estimated,
        inputTokens,outputTokens,attempts:input.attempts};
      // Preserve the existing transaction + row/project locks while committing
      // the ledger, request, reservation and event writes in one database round trip.
      await client.query(`WITH entry AS (
        INSERT INTO ledger(event_id,request_id,project_id,period,amount_micros,kind,input_tokens,output_tokens,estimated)
        VALUES($10,$1,$2,$3,$4,$15,$6,$7,$9) ON CONFLICT(event_id) DO NOTHING
      ), request_update AS (
        UPDATE requests SET status=$8,actual_micros=$5,input_tokens=$6,output_tokens=$7,updated_at=now() WHERE request_id=$1
      ), reservation_update AS (
        UPDATE reservations SET status=$14,actual_micros=$5,lease_expires_at=NULL,updated_at=now() WHERE request_id=$1
      ) INSERT INTO outbox(id,event_id,event_type,project_id,payload,status)
        VALUES($11,$12,'request.settled',$2,$13::jsonb,'pending') ON CONFLICT(event_id) DO NOTHING`,
        [requestId,projectId,period,charge,actual,inputTokens,outputTokens,status,estimated,`settle:${requestId}`,
          randomUUID(),`request.settled:${requestId}`,JSON.stringify(payload),estimated?'unknown':'settled',estimated?'estimate':'settle']);
      await client.query('COMMIT');
      committed = true;
          releaseClient();
      return () => this.applySettlement(projectId, period, requestId, charge, usageState);
    } catch (err) {
      if (!committed) await client.query('ROLLBACK').catch(() => undefined);
      throw err;
    } finally {
      releaseClient();
    }
  }

  async cancelBeforeDispatch(requestIdInput: string): Promise<void> {
    this.ensureInitialized();
    const requestId = requireUuid(requestIdInput, 'requestId');
    // Same-project finalization must not hold pool connections while parked.
    await this.runFinancial(requestId, () => this.performCancel(requestId));
  }

  private async performCancel(requestId: string): Promise<void> {
    const client = await this.pool.connect();
    let committed = false;
    let projectId = '';
    let period = '';
    try {
      await client.query('BEGIN');
      const scope=await client.query('SELECT project_id::text FROM requests WHERE request_id=$1',[requestId]);
      if(!scope.rows[0])throw new BillingError(404,'request_not_found','request not found');
      await client.query('SELECT pg_advisory_xact_lock(hashtextextended($1,0))',[`gateway:billing:${scope.rows[0].project_id}`]);
      const found = await client.query(
        `SELECT project_id::text AS project_id, period, status FROM requests WHERE request_id = $1 FOR UPDATE`, [requestId]);
      if (!found.rows[0]) throw new BillingError(404, 'request_not_found', 'request not found');
      const row = found.rows[0];
      projectId = row.project_id;
      period = row.period;
      if (row.status === 'cancelled') {
        await client.query('COMMIT');
        committed = true;
        return;
      }
      if (row.status === 'settled' || row.status === 'unknown' || row.status === 'failed') {
        throw new BillingError(409, 'request_already_final', 'request is already finalized');
      }
      if (row.status === 'dispatch_intent' || row.status === 'dispatched') {
        throw new BillingError(409, 'already_dispatched', 'request may have been sent upstream');
      }
      await client.query(`UPDATE requests SET status = 'cancelled', updated_at = now() WHERE request_id = $1`, [requestId]);
      await client.query(
        `UPDATE reservations SET status = 'cancelled', lease_expires_at = NULL, updated_at = now() WHERE request_id = $1`, [requestId]);
      await client.query(
        `INSERT INTO ledger (event_id, request_id, project_id, period, amount_micros, kind, estimated)
         VALUES ($1, $2, $3, $4, 0, 'release', false) ON CONFLICT (event_id) DO NOTHING`,
        [`release:${requestId}`, requestId, projectId, period]);
      await client.query(
        `INSERT INTO outbox (id, event_id, event_type, project_id, payload, status)
         VALUES ($1, $2, 'request.cancelled', $3, $4::jsonb, 'pending') ON CONFLICT (event_id) DO NOTHING`,
        [randomUUID(), `request.cancelled:${requestId}`, projectId, JSON.stringify({ requestId, projectId, period })]);
      await client.query('COMMIT');
      committed = true;
    } catch (err) {
      if (!committed) await client.query('ROLLBACK').catch(() => undefined);
      throw err;
    } finally {
      client.release();
    }

    let generation: string | undefined;
    try {
      const r = await this.pool.query('SELECT generation::text AS generation FROM billing_state WHERE project_id = $1', [projectId]);
      generation = r.rows[0]?.generation;
    } catch { /* reconciliation will retry */ }
    if (!generation) { await this.markDirty(projectId); return; }
    const k = keyFor(projectId);
    try {
      const res = await this.runScript('cancel', [k.gen, k.request(requestId), k.concurrency, k.reservations,
        k.budget(period), k.requests], [generation, requestId]) as unknown[];
      const code = Number(res[0]);
      if (code === 3) {
        await this.markDirty(projectId);
        throw new BillingError(409, 'already_dispatched', 'request may have been sent upstream');
      }
      if (code === 2) await this.markDirty(projectId);
      if (code !== 0 && code !== 2 && code !== 4) await this.markDirty(projectId);
    } catch (err) {
      if (err instanceof BillingError) throw err;
      await this.markDirty(projectId);
    }
  }

  private async insertAdjustment(client: { query: (sql: string, params: unknown[]) => Promise<unknown> }, requestId: string, projectId: string, period: string, adjustment: number, actual: number, inputTokens: number | null, outputTokens: number | null, status: string, attemptsJson: string): Promise<void> {
    await client.query(
      `INSERT INTO ledger (event_id, request_id, project_id, period, amount_micros, kind, input_tokens, output_tokens, estimated)
       VALUES ($1, $2, $3, $4, $5, 'adjust', $6, $7, false) ON CONFLICT (event_id) DO NOTHING`,
      [`adjust:${requestId}:${actual}`, requestId, projectId, period, adjustment, inputTokens, outputTokens]);
    await client.query(
      `INSERT INTO outbox (id, event_id, event_type, project_id, payload, status)
       VALUES ($1, $2, 'request.adjusted', $3, $4::jsonb, 'pending') ON CONFLICT (event_id) DO NOTHING`,
      [randomUUID(), `request.adjusted:${requestId}:${actual}`, projectId,
        JSON.stringify({ requestId, projectId, period, status, actualMicros: actual, adjustmentMicros: adjustment,
          inputTokens, outputTokens, attempts: JSON.parse(attemptsJson) })]);
  }

  private async applySettlement(projectId: string, period: string, requestId: string, charge: number, usageState: 'actual' | 'unknown'): Promise<void> {
    try {
      const r = await this.pool.query('SELECT generation::text AS generation FROM billing_state WHERE project_id = $1', [projectId]);
      const generation = r.rows[0]?.generation;
      if (!generation) { await this.markDirty(projectId); return; }
      const k = keyFor(projectId);
      const res = await this.runScript('settle', [k.gen, k.request(requestId), k.concurrency, k.reservations,
        k.budget(period), k.requests], [generation, requestId, charge, usageState]) as unknown[];
      const code = Number(res[0]);
      if (code === 0 || code === 2 || code === 4) return;
      await this.markDirty(projectId);
    } catch {
      await this.markDirty(projectId);
    }
  }

  async heartbeat(requestIdInput: string): Promise<void> {
    this.ensureInitialized();
    const requestId = requireUuid(requestIdInput, 'requestId');
    const meta = await this.requestMeta(requestId);
    const r = await this.pool.query('SELECT generation::text AS generation FROM billing_state WHERE project_id = $1', [meta.projectId]);
    const generation = r.rows[0]?.generation;
    if (!generation) throw new BillingError(410, 'lease_lost', 'billing state for request is gone');
    const k = keyFor(meta.projectId);
    let code: number;
    try {
      const res = await this.runScript('heartbeat', [k.gen, k.request(requestId), k.concurrency],
        [generation, requestId, meta.owner, this.leaseMs, 0]) as unknown[];
      code = Number(res[0]);
    } catch {
      await this.markDirty(meta.projectId);
      throw new BillingError(503, 'billing_unavailable', 'heartbeat could not be recorded');
    }
    if (code === 0) {
      await this.pool.query(
        `WITH r AS (
           UPDATE requests SET updated_at = now()
           WHERE request_id = $1 AND status IN ('preparing', 'reserved', 'dispatch_intent', 'dispatched')
           RETURNING request_id
         )
         UPDATE reservations SET lease_expires_at = now() + ($2 * interval '1 millisecond'), updated_at = now()
         WHERE request_id = $1 AND status IN ('preparing', 'reserved', 'dispatch_intent', 'dispatched')`,
        [requestId, this.leaseMs]).catch(() => undefined);
      return;
    }
    if (code === 1) {
      await this.markDirty(meta.projectId);
      throw new BillingError(410, 'lease_lost', 'billing generation changed; upstream should be cancelled');
    }
    if (code === 5) throw new BillingError(409, 'lease_owner_mismatch', 'lease owner does not match');
    throw new BillingError(410, 'lease_lost', 'concurrency lease is no longer held');
  }

  async close(): Promise<void> {
    this.initialized=false;
    if(this.reconcileTimer)clearInterval(this.reconcileTimer);
    if(this.reconcileSoonTimer)clearTimeout(this.reconcileSoonTimer);
    this.reconcileTimer=null;this.reconcileSoonTimer=null;
    // Financial settlement/cancel already accepted must finish before the pool closes.
    await Promise.allSettled([...this.financialOperations]);
    await this.settlementQueue.drain();
    await this.recovering?.catch(()=>undefined);
  }

  async reconcile(): Promise<void> {
    while(this.recovering)await this.recovering;
    const run=this.doReconcile();this.recovering=run;
    try{await run;}finally{this.recovering=null;}
  }

  private async doReconcile(): Promise<void> {
    this.ensureInitialized();
    const bootstrapKey = 'billing:meta:bootstrapped';
    const bootstrapped = (await this.redis.exists(bootstrapKey)) === 1;
    const query = bootstrapped
      ? `SELECT project_id::text AS project_id FROM billing_state WHERE dirty OR paused
         UNION
         SELECT DISTINCT project_id::text FROM reservations
          WHERE status IN ('preparing','reserved','dispatch_intent','dispatched')
         UNION
         SELECT DISTINCT project_id::text FROM requests
          WHERE status IN ('preparing','reserved','dispatch_intent','dispatched')`
      : `SELECT id::text AS project_id FROM projects`;
    const candidates = await this.pool.query(query);
    let allOk = true;
    for (const row of candidates.rows) {
      try {
        await this.reconcileProject(row.project_id, !bootstrapped);
      } catch {
        allOk = false;
      }
    }
    if (!bootstrapped) await this.redis.set(bootstrapKey, '1');
    this.expectedRunId = await this.redisRunId(true);
  }

  private async requestMeta(requestId: string): Promise<{ projectId: string; owner: string }> {
    const cached = this.requestCache.get(requestId);
    if (cached) return cached;
    const r = await this.pool.query('SELECT project_id::text AS project_id FROM requests WHERE request_id = $1', [requestId]);
    if (!r.rows[0]) throw new BillingError(404, 'request_not_found', 'request not found');
    const projectId = r.rows[0].project_id as string;
    const owner = await this.redis.hget(keyFor(projectId).request(requestId), 'owner');
    if (!owner) {
      await this.markDirty(projectId);
      throw new BillingError(410, 'lease_lost', 'billing reservation is no longer present');
    }
    const meta = { projectId, owner };
    if (this.requestCache.size > 10000) this.requestCache.clear();
    this.requestCache.set(requestId, meta);
    return meta;
  }

  private async reconcileProject(projectId: string, force: boolean): Promise<void> {
    if (!isUuid(projectId)) return;
    const client = await this.pool.connect();
    let locked = false;
    try {
      const lock = await client.query('SELECT pg_try_advisory_lock(hashtextextended($1, 0)) AS locked',
        [`gateway:billing:${projectId}`]);
      if (!lock.rows[0]?.locked) return;
      locked = true;
      await client.query('INSERT INTO billing_state (project_id) VALUES ($1) ON CONFLICT (project_id) DO NOTHING', [projectId]);
      const state = (await client.query(
        'SELECT generation::text AS generation, paused, dirty FROM billing_state WHERE project_id = $1', [projectId])).rows[0];
      if (!state) return;
      const k = keyFor(projectId);
      const redisGen = await this.redis.get(k.gen);
      const redisPaused = (await this.redis.exists(k.paused)) === 1;
      let rebuild = force || !redisGen || redisGen !== state.generation || Boolean(state.paused) || Boolean(state.dirty) || redisPaused;
      if (await this.expireStale(projectId, client)) rebuild = true;
      if (rebuild) await this.rebuildProject(client, projectId);
    } finally {
      if (locked) {
        await client.query('SELECT pg_advisory_unlock(hashtextextended($1, 0))',
          [`gateway:billing:${projectId}`]).catch(() => undefined);
      }
      client.release();
    }
  }

  private async expireStale(projectId: string, client: PoolClient): Promise<boolean> {
    let changed = false;
    const undispatched = await client.query(
      `SELECT r.request_id::text AS request_id, r.period, r.reserved_micros::text AS reserved_micros
         FROM reservations r JOIN requests q ON q.request_id = r.request_id
        WHERE r.project_id = $1 AND q.status IN ('preparing', 'reserved')
          AND r.status IN ('preparing', 'reserved')
          AND (r.lease_expires_at IS NULL OR r.lease_expires_at < now() - interval '5 seconds')`, [projectId]);
    for (const row of undispatched.rows) {
      const id = row.request_id as string;
      const period = row.period as string;
      await client.query(`BEGIN`);
      try {
        const updated = await client.query(
          `UPDATE requests SET status = 'cancelled', updated_at = now()
           WHERE request_id = $1 AND status IN ('preparing', 'reserved')`, [id]);
        if (updated.rowCount === 0) { await client.query('ROLLBACK'); continue; }
        await client.query(
          `UPDATE reservations SET status = 'cancelled', lease_expires_at = NULL, updated_at = now()
           WHERE request_id = $1 AND status IN ('preparing', 'reserved')`, [id]);
        await client.query(
          `INSERT INTO ledger (event_id, request_id, project_id, period, amount_micros, kind, estimated)
           VALUES ($1, $2, $3, $4, 0, 'release', false) ON CONFLICT (event_id) DO NOTHING`,
          [`release:${id}`, id, projectId, period]);
        await client.query(
          `INSERT INTO outbox (id, event_id, event_type, project_id, payload, status)
           VALUES ($1, $2, 'request.cancelled', $3, $4::jsonb, 'pending') ON CONFLICT (event_id) DO NOTHING`,
          [randomUUID(), `request.cancelled:${id}`, projectId, JSON.stringify({ requestId: id, projectId, period, reason: 'expired_undispatched' })]);
        await client.query('COMMIT');
        changed = true;
      } catch (err) {
        await client.query('ROLLBACK').catch(() => undefined);
        throw err;
      }
    }
    const dispatched = await client.query(
      `SELECT r.request_id::text AS request_id, r.period, r.reserved_micros::text AS reserved_micros
         FROM reservations r JOIN requests q ON q.request_id = r.request_id
        WHERE r.project_id = $1 AND q.status IN ('dispatch_intent', 'dispatched')
          AND (q.updated_at < now() - interval '10 minutes' OR r.lease_expires_at < now() - interval '5 seconds')`, [projectId]);
    for (const row of dispatched.rows) {
      const id = row.request_id as string;
      const period = row.period as string;
      const reserved = Number(row.reserved_micros);
      await client.query('BEGIN');
      try {
        const updated = await client.query(
          `UPDATE requests SET status = 'unknown', actual_micros = NULL, updated_at = now()
           WHERE request_id = $1 AND status IN ('dispatch_intent', 'dispatched')`, [id]);
        if (updated.rowCount === 0) { await client.query('ROLLBACK'); continue; }
        await client.query(
          `UPDATE reservations SET status = 'unknown', actual_micros = NULL, lease_expires_at = NULL, updated_at = now()
           WHERE request_id = $1`, [id]);
        await client.query(
          `INSERT INTO ledger (event_id, request_id, project_id, period, amount_micros, kind, estimated)
           VALUES ($1, $2, $3, $4, $5, 'estimate', true) ON CONFLICT (event_id) DO NOTHING`,
          [`estimate:${id}`, id, projectId, period, reserved]);
        await client.query(
          `INSERT INTO outbox (id, event_id, event_type, project_id, payload, status)
           VALUES ($1, $2, 'request.settled', $3, $4::jsonb, 'pending') ON CONFLICT (event_id) DO NOTHING`,
          [randomUUID(), `request.settled:${id}`, projectId,
            JSON.stringify({ requestId: id, projectId, period, status: 'unknown', actualMicros: null, chargedMicros: reserved, estimated: true, reason: 'dispatch_outcome_unknown' })]);
        await client.query('COMMIT');
        changed = true;
      } catch (err) {
        await client.query('ROLLBACK').catch(() => undefined);
        throw err;
      }
    }
    return changed;
  }

  private async rebuildProject(client: PoolClient, projectId: string): Promise<void> {
    const bumped = await client.query(
      `UPDATE billing_state SET generation = generation + 1, paused = true, dirty = true, updated_at = now()
       WHERE project_id = $1 RETURNING generation::text AS generation`, [projectId]);
    const newGen = bumped.rows[0]?.generation as string | undefined;
    if (!newGen) throw new BillingError(503, 'billing_rebuild_failed', 'billing state missing during rebuild');
    const snapshot = (await client.query('SELECT clock_timestamp()::text AS at')).rows[0].at as string;
    const k = keyFor(projectId);
    const runId = await this.redisRunId(true);
    await this.redis.set(k.paused, '1');
    await this.redis.set(k.gen, newGen);

    const ledger = await client.query(
      `SELECT period, COALESCE(SUM(amount_micros), 0)::text AS spent
         FROM ledger WHERE project_id = $1 GROUP BY period`, [projectId]);
    const live = await client.query(
      `SELECT r.request_id::text AS request_id, r.period, r.reserved_micros::text AS reserved_micros,
              q.status AS request_status, q.key_id::text AS key_id
         FROM reservations r JOIN requests q ON q.request_id = r.request_id
        WHERE r.project_id = $1 AND r.status IN ('preparing', 'reserved')
          AND q.status IN ('preparing', 'reserved', 'dispatch_intent', 'dispatched')`, [projectId]);
    const project = await client.query(
      'SELECT concurrency, budget_micros::text AS budget_micros FROM projects WHERE id = $1', [projectId]);
    const oldRequests = await this.redis.smembers(k.requests);
    const oldPeriods = await this.redis.smembers(k.periods);

    const spent = new Map<string, number>();
    for (const row of ledger.rows) {
      const value = Number(row.spent);
      if (!Number.isSafeInteger(value) || value < 0) throw new BillingError(503, 'billing_rebuild_failed', 'ledger total is outside the safe integer range');
      spent.set(row.period as string, value);
    }
    const reserved = new Map<string, number>();
    for (const row of live.rows) {
      const period = row.period as string;
      const value = Number(row.reserved_micros);
      const total = (reserved.get(period) ?? 0) + value;
      if (!Number.isSafeInteger(value) || !Number.isSafeInteger(total)) throw new BillingError(503, 'billing_rebuild_failed', 'reservation total is outside the safe integer range');
      reserved.set(period, total);
    }
    const limit = project.rows[0]?.budget_micros == null ? -1 : Number(project.rows[0].budget_micros);
    const projectConcurrency = project.rows[0]?.concurrency == null ? 1_000_000 : Number(project.rows[0].concurrency);
    const periods = new Set<string>([...spent.keys(), ...reserved.keys(), ...oldPeriods]);
    const t = await this.redis.time();
    const nowMs = Number(t[0]) * 1000 + Math.floor(Number(t[1]) / 1000);

    const pipe = this.redis.pipeline();
    pipe.del(k.concurrency);
    pipe.del(k.reservations);
    for (const id of oldRequests) pipe.del(k.request(id));
    pipe.del(k.requests);
    pipe.del(k.periods);
    for (const period of periods) pipe.del(k.budget(period));
    for (const period of new Set([...spent.keys(), ...reserved.keys()])) {
      const s = spent.get(period) ?? 0;
      const r = reserved.get(period) ?? 0;
      pipe.hset(k.budget(period), 'spent', String(s), 'reserved', String(r), 'limit', String(limit));
      if (limit >= 0 && s > limit) pipe.hset(k.budget(period), 'over_limit', '1');
      pipe.sadd(k.periods, period);
    }
    for (const row of live.rows) {
      const id = row.request_id as string;
      const period = row.period as string;
      const amount = Number(row.reserved_micros);
      const dispatched = row.request_status === 'dispatch_intent' || row.request_status === 'dispatched';
      pipe.hset(k.reservations, id, String(amount));
      pipe.hset(k.request(id), 'state', dispatched ? 'dispatched' : 'reserved', 'amount', String(amount),
        'period', period, 'key', (row.key_id as string | null) ?? '', 'owner', randomUUID(),
        'generation', newGen, 'dispatched', dispatched ? '1' : '0', 'concurrency', String(projectConcurrency));
      pipe.zadd(k.concurrency, nowMs + (dispatched ? 600_000 : this.leaseMs), id);
      pipe.sadd(k.requests, id);
    }
    const results = await pipe.exec();
    if (!results) throw new BillingError(503, 'billing_rebuild_failed', 'redis rebuild failed');
    for (const [err] of results) {
      if (err) throw new BillingError(503, 'billing_rebuild_failed', `redis rebuild failed: ${err.message}`);
    }

    const changed = await client.query(
      'SELECT 1 FROM ledger WHERE project_id = $1 AND created_at > $2 LIMIT 1', [projectId, snapshot]);
    if ((changed.rowCount ?? 0) > 0) return;
    await this.redis.del(k.paused);
    const updated = await client.query(
      `UPDATE billing_state SET paused = false, dirty = false, last_reconciled_at = now(), redis_run_id = $3, updated_at = now()
       WHERE project_id = $1 AND generation = $2`, [projectId, newGen, runId]);
    if ((updated.rowCount ?? 0) === 0) return;
  }
  private async abortIntent(requestId: string): Promise<void> {
    await this.pool.query(
      "UPDATE requests SET status = 'failed', updated_at = now() WHERE request_id = $1 AND status = 'preparing'",
      [requestId]).catch(() => undefined);
    await this.pool.query(
      "UPDATE reservations SET status = 'cancelled', lease_expires_at = NULL, updated_at = now() WHERE request_id = $1 AND status = 'preparing'",
      [requestId]).catch(() => undefined);
  }
}
