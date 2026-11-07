import { createServer, type IncomingMessage, type ServerResponse } from 'node:http';
import { describe, test } from 'node:test';
import assert from 'node:assert/strict';
import Fastify, { type FastifyInstance, type FastifyReply } from 'fastify';
import { EventEmitter } from 'node:events';
import type { Pool } from 'pg';
import type { Billing } from '../src/billing.js';
import { encrypt } from '../src/security.js';
import {
  registerGateway, validateChatBody, tokensToMicros, estimateAttemptMicros, SseParser, ReplyStreamSink,
  extractTargetIds, aggregateSettlement, type Timings, type AttemptRecord, type GatewayDeps,
} from '../src/gateway.js';

const ENCRYPTION_KEY = 'ab'.repeat(32);
const PROVIDER_TOKEN = 'sk-upstream-secret-token';
const KEY_ID = '11111111-1111-4111-8111-111111111111';
const PROJECT_ID = '22222222-2222-4222-8222-222222222222';
const WORKSPACE_ID = '33333333-3333-4333-8333-333333333333';
const ALIAS_ID = '44444444-4444-4444-8444-444444444444';
const ROUTE_VERSION_ID = '55555555-5555-4555-8555-555555555555';
const TARGET_ID = '66666666-6666-4666-8666-666666666666';
const FALLBACK_ID = '77777777-7777-4777-8777-777777777777';

type Row = Record<string, unknown>;

interface PoolConfig {
  key?: Row | null;
  route?: Row | null;
  targets?: Row[];
  models?: Row[];
}

type QueryLog = Array<{ sql: string; params: unknown[] }>;
type PoolStub = Pool & { queries: QueryLog };

function poolStub(config: PoolConfig): PoolStub {
  const queries: QueryLog = [];
  const query = async (sql: string, params: unknown[] = []): Promise<{ rows: Row[]; rowCount: number }> => {
    queries.push({ sql, params });
    if (sql.includes('/*gw:key*/')) return { rows: config.key ? [config.key] : [], rowCount: config.key ? 1 : 0 };
    if (sql.includes('/*gw:touch*/')) return { rows: [], rowCount: 1 };
    if (sql.includes('/*gw:route*/')) return { rows: config.route ? [config.route] : [], rowCount: config.route ? 1 : 0 };
    if (sql.includes('/*gw:targets*/')) return { rows: config.targets ?? [], rowCount: (config.targets ?? []).length };
    if (sql.includes('/*gw:models*/')) return { rows: config.models ?? [], rowCount: (config.models ?? []).length };
    if (sql.includes('/*gw:request-meta*/')) return { rows: [], rowCount: 1 };
    if (sql.includes('/*gw:attempt*/')) return { rows: [], rowCount: 1 };
    throw new Error(`unexpected query in gateway test: ${sql.slice(0, 80)}`);
  };
  return Object.assign({ query } as unknown as Pool, { queries }) as PoolStub;
}

class FakeBilling {
  admitted: Array<Record<string, any>> = [];
  dispatched: string[] = [];
  settled: Array<{ id: string; input: Record<string, any> }> = [];
  cancelled: string[] = [];
  heartbeats = 0;
  heartbeatError: Error | null = null;
  async admit(input: Record<string, any>): Promise<void> { this.admitted.push(input); }
  async dispatch(id: string): Promise<void> { this.dispatched.push(id); }
  async settle(id: string, input: Record<string, any>): Promise<void> { this.settled.push({ id, input }); }
  async cancelBeforeDispatch(id: string): Promise<void> { this.cancelled.push(id); }
  async heartbeat(): Promise<void> {
    this.heartbeats += 1;
    if (this.heartbeatError) throw this.heartbeatError;
  }
  async ready(): Promise<boolean> { return true; }
}

interface UpstreamCall { model: string; stream: boolean; raw: string }

async function upstream(handler: (call: UpstreamCall, res: ServerResponse) => void | Promise<void>) {
  const calls: UpstreamCall[] = [];
  const server = createServer(async (req: IncomingMessage, res: ServerResponse) => {
    let raw = '';
    for await (const chunk of req) raw += chunk;
    const body = JSON.parse(raw || '{}') as Record<string, any>;
    const call: UpstreamCall = { model: String(body.model ?? ''), stream: body.stream === true, raw };
    calls.push(call);
    await handler(call, res);
  });
  await new Promise<void>(resolve => server.listen(0, '127.0.0.1', resolve));
  const address = server.address();
  if (!address || typeof address === 'string') throw new Error('bad address');
  return {
    baseUrl: `http://127.0.0.1:${address.port}/v1`,
    calls,
    close: () => new Promise<void>((resolve, reject) => server.close(e => (e ? reject(e) : resolve()))),
  };
}

function keyRow(overrides: Partial<Row> = {}): Row {
  return {
    key_id: KEY_ID, prefix: 'sk-gw-test', allowed_models: [], expires_at: null, revoked_at: null,
    key_rpm: null, project_id: PROJECT_ID, workspace_id: WORKSPACE_ID, project_rpm: null,
    concurrency: null, budget_micros: null, max_output_tokens: null, ...overrides,
  };
}

function targetRow(id: string, baseUrl: string, upstreamModel: string, overrides: Partial<Row> = {}): Row {
  return {
    id, upstream_model: upstreamModel, input_price: '1000000', output_price: '2000000',
    target_enabled: true, provider_enabled: true, base_url: baseUrl, capabilities: [],
    encrypted_secret: encrypt(PROVIDER_TOKEN, ENCRYPTION_KEY), ...overrides,
  };
}

function routeRow(targetIds: string[], aliasName = 'support-chat'): Row {
  return {
    alias_id: ALIAS_ID, alias_name: aliasName, route_version_id: ROUTE_VERSION_ID, version: 3,
    targets: targetIds, capabilities: ['chat'],
  };
}

interface HarnessOptions {
  pool: PoolConfig;
  allowPrivate?: boolean;
  timings?: Partial<Timings>;
  billing?: FakeBilling;
}

async function buildHarness(options: HarnessOptions) {
  const billing = options.billing ?? new FakeBilling();
  const pool = poolStub(options.pool);
  const deps = {
    pool, billing: billing as unknown as Billing,
    encryptionKey: ENCRYPTION_KEY, allowPrivateUpstream: options.allowPrivate ?? true, timings: options.timings,
  } satisfies GatewayDeps;
  const app: FastifyInstance = Fastify({ logger: false, bodyLimit: 1024 * 1024 });
  await registerGateway(app, deps);
  await app.ready();
  return { app, billing, pool };
}

async function waitFor(check: () => boolean, timeoutMs = 3_000): Promise<void> {
  const started = Date.now();
  while (!check()) {
    if (Date.now() - started > timeoutMs) throw new Error('timed out waiting for condition');
    await new Promise(resolve => setTimeout(resolve, 10));
  }
}

/** Real socket HTTP client: Fastify.inject does not deliver hijacked SSE responses reliably. */
async function postReal(app: FastifyInstance, payload: unknown, token = 'sk-gateway-key-123456') {
  if (!app.server.listening) await app.listen({ port: 0, host: '127.0.0.1' });
  const address = app.server.address();
  if (!address || typeof address === 'string') throw new Error('app is not listening');
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), 10_000);
  try {
    const response = await fetch(`http://127.0.0.1:${address.port}/v1/chat/completions`, {
      method: 'POST', signal: controller.signal,
      headers: { authorization: `Bearer ${token}`, 'content-type': 'application/json' },
      body: JSON.stringify(payload),
    });
    const body = await response.text();
    return { statusCode: response.status, headers: Object.fromEntries(response.headers), body, json: () => JSON.parse(body) };
  } finally {
    clearTimeout(timer);
  }
}

function post(app: FastifyInstance, payload: unknown, token = 'sk-gateway-key-123456') {
  return app.inject({
    method: 'POST', url: '/v1/chat/completions',
    headers: { authorization: `Bearer ${token}`, 'content-type': 'application/json' },
    payload: JSON.stringify(payload),
  });
}

describe('request validation', () => {
  test('accepts the supported subset', () => {
    const body = validateChatBody({ model: 'support-chat', messages: [{ role: 'user', content: 'hi' }], stream: true, temperature: 0.5, max_tokens: 16 });
    assert.equal(body.stream, true);
    assert.equal(body.messages.length, 1);
  });
  test('rejects unsupported parameters instead of ignoring them', () => {
    assert.throws(() => validateChatBody({ model: 'm', messages: [{ role: 'user', content: 'hi' }], tools: [] }),
      (err: any) => err.statusCode === 400 && err.code === 'unsupported_parameter');
  });
  test('rejects unsupported roles and malformed content', () => {
    assert.throws(() => validateChatBody({ model: 'm', messages: [{ role: 'tool', content: 'x' }] }),
      (err: any) => err.statusCode === 400 && err.code === 'unsupported_role');
    assert.throws(() => validateChatBody({ model: 'm', messages: [{ role: 'user', content: 42 }] }),
      (err: any) => err.statusCode === 400);
    assert.throws(() => validateChatBody({ model: 'm', messages: [] }), (err: any) => err.statusCode === 400);
  });
  test('rejects out-of-range sampling parameters', () => {
    assert.throws(() => validateChatBody({ model: 'm', messages: [{ role: 'user', content: 'x' }], temperature: 9 }), (err: any) => err.statusCode === 400);
    assert.throws(() => validateChatBody({ model: 'm', messages: [{ role: 'user', content: 'x' }], max_tokens: 0 }), (err: any) => err.statusCode === 400);
  });
});

describe('pricing math', () => {
  test('converts tokens to micro-USD with ceiling and integer math', () => {
    assert.equal(tokensToMicros(0, 5_000_000), 0);
    assert.equal(tokensToMicros(1, 1_000_000), 1);
    assert.equal(tokensToMicros(1, 1), 1);
    assert.equal(tokensToMicros(1_000_000, 250_000), 250_000);
    assert.equal(tokensToMicros(3, 333_333), 1);
  });
  test('rejects invalid prices instead of silently zeroing them', () => {
    assert.throws(() => tokensToMicros(10, -1), (err: any) => err.statusCode === 500);
    assert.throws(() => tokensToMicros(Number.MAX_SAFE_INTEGER, 2_000_000), (err: any) => err.code === 'price_overflow');
  });
  test('reservation is the sum of both attempt upper bounds', () => {
    const perAttempt = estimateAttemptMicros(1000, 500, 1_000_000, 2_000_000);
    assert.equal(perAttempt, 1000 + 1000);
    assert.ok(estimateAttemptMicros(1000, 500, 1_000_000, 2_000_000) * 2 > perAttempt);
  });
});

describe('SSE decoding', () => {
  test('reassembles frames split across chunks including CRLF', () => {
    const parser = new SseParser();
    assert.deepEqual(parser.push('data: {"a":'), []);
    const events = parser.push('1}\r\n\r\ndata: [DONE]\n\n');
    assert.deepEqual(events, [{ event: null, data: '{"a":1}' }, { event: null, data: '[DONE]' }]);
  });
  test('captures event names, multiple data lines and comments', () => {
    const parser = new SseParser();
    const events = parser.push(': keepalive\nevent: error\ndata: a\ndata: b\n\n');
    assert.deepEqual(events, [{ event: 'error', data: 'a\nb' }]);
  });
  test('flushes a trailing frame', () => {
    const parser = new SseParser();
    assert.deepEqual(parser.push('data: [DONE]'), []);
    assert.deepEqual(parser.flush(), [{ event: null, data: '[DONE]' }]);
  });
});

describe('route target parsing and settlement aggregation', () => {
  test('accepts id arrays and legacy object entries', () => {
    assert.deepEqual(extractTargetIds([TARGET_ID, { target_id: FALLBACK_ID }]), [TARGET_ID, FALLBACK_ID]);
    assert.deepEqual(extractTargetIds({ primary: TARGET_ID }), []);
  });
  test('unknown usage keeps settlement unknown instead of zero', () => {
    const base: AttemptRecord = { attempt_no: 1, target_id: TARGET_ID, upstream_model: 'm', status: 'ok', http_status: 200,
      error_code: null, request_sent: true, execution_unknown: false, usage_known: true, input_tokens: 10, output_tokens: 5,
      cost_micros: 25, upper_bound_micros: 100, retry_safe: false, latency_ms: 1 };
    assert.deepEqual(aggregateSettlement([base]), { actualMicros: 25, inputTokens: 10, outputTokens: 5 });
    assert.deepEqual(aggregateSettlement([{ ...base, usage_known: false, cost_micros: null }]),
      { actualMicros: null, inputTokens: null, outputTokens: null });
  });
});

describe('POST /v1/chat/completions', () => {
  test('completes a buffered request with frozen pricing and settlement', async () => {
    const mock = await upstream((call, res) => {
      res.writeHead(200, { 'content-type': 'application/json' });
      res.end(JSON.stringify({
        id: 'upstream-id', model: call.model, choices: [{ index: 0, message: { role: 'assistant', content: 'hello' }, finish_reason: 'stop' }],
        usage: { prompt_tokens: 10, completion_tokens: 5, total_tokens: 15 },
      }));
    });
    const { app, billing, pool } = await buildHarness({
      pool: { key: keyRow(), route: routeRow([TARGET_ID]), targets: [targetRow(TARGET_ID, mock.baseUrl, 'upstream-model')] },
    });
    try {
      const res = await post(app, { model: 'support-chat', messages: [{ role: 'user', content: 'secret prompt' }] });
      assert.equal(res.statusCode, 200);
      const body = res.json();
      assert.equal(body.model, 'support-chat');
      assert.equal(body.choices[0].message.content, 'hello');
      const admitted = billing.admitted[0];
      assert.equal(admitted.projectId, PROJECT_ID);
      assert.equal(admitted.keyId, KEY_ID);
      assert.equal(admitted.reserveMicros, admitted.priceSnapshot.total_reserved_micros);
      assert.equal(admitted.priceSnapshot.attempts.length, 1);
      assert.equal(billing.dispatched[0], admitted.requestId);
      assert.equal(res.headers['x-request-id'], admitted.requestId);
      assert.equal(body.id, `chatcmpl_${admitted.requestId}`);
      assert.equal(mock.calls.length, 1);
      const forwarded = JSON.parse(mock.calls[0].raw);
      assert.equal(forwarded.model, 'upstream-model');
      assert.equal(forwarded.max_tokens, 4096);
      const settled = billing.settled[0];
      assert.equal(settled.input.status, 'completed');
      assert.equal(settled.input.actualMicros, 20);
      assert.equal(settled.input.inputTokens, 10);
      assert.equal(settled.input.outputTokens, 5);
      assert.ok(!JSON.stringify(settled.input).includes('secret prompt'));
      assert.ok(!JSON.stringify(settled.input).includes('hello'));
      const meta = pool.queries.filter(q => q.sql.includes('/*gw:request-meta*/')).at(-1);
      assert.ok(meta);
      assert.deepEqual(meta!.params, [admitted.requestId, 'support-chat', null]);
      const attemptWrites = pool.queries.filter(q => q.sql.includes('/*gw:attempt*/'));
      assert.ok(attemptWrites.length >= 1);
      const attempt = attemptWrites.at(-1)!.params;
      assert.equal(attempt[1], admitted.requestId);
      assert.equal(attempt[2], 1);
      assert.equal(attempt[3], TARGET_ID);
      assert.equal(attempt[4], 'ok');
      assert.equal(attempt[5], null);
      assert.equal(attempt[8], 10);
      assert.equal(attempt[9], 5);
      assert.equal(attempt[10], 20);
      assert.ok(!JSON.stringify(pool.queries).includes('secret prompt'));
      assert.ok(!JSON.stringify(pool.queries).includes('hello'));
    } finally {
      await app.close();
      await mock.close();
    }
  });

  test('rejects missing or unknown keys without calling upstream', async () => {
    const mock = await upstream((_call, res) => { res.writeHead(200, { 'content-type': 'application/json' }); res.end('{}'); });
    const { app, billing } = await buildHarness({
      pool: { key: null, route: routeRow([TARGET_ID]), targets: [targetRow(TARGET_ID, mock.baseUrl, 'upstream-model')] },
    });
    try {
      const res = await post(app, { model: 'support-chat', messages: [{ role: 'user', content: 'x' }] });
      assert.equal(res.statusCode, 401);
      assert.equal(res.json().error.code, 'invalid_api_key');
      assert.ok(res.headers['x-request-id']);
      assert.equal(mock.calls.length, 0);
      assert.equal(billing.admitted.length, 0);
    } finally {
      await app.close();
      await mock.close();
    }
  });

  test('returns 404 for an unpublished alias and 403 for a key out of scope', async () => {
    const mock = await upstream((_call, res) => { res.writeHead(200, { 'content-type': 'application/json' }); res.end('{}'); });
    const missing = await buildHarness({ pool: { key: keyRow(), route: null } });
    const forbidden = await buildHarness({
      pool: { key: keyRow({ allowed_models: ['other-model'] }), route: routeRow([TARGET_ID]), targets: [targetRow(TARGET_ID, mock.baseUrl, 'm')] },
    });
    try {
      const notFound = await post(missing.app, { model: 'support-chat', messages: [{ role: 'user', content: 'x' }] });
      assert.equal(notFound.statusCode, 404);
      assert.equal(notFound.json().error.code, 'model_not_found');
      const denied = await post(forbidden.app, { model: 'support-chat', messages: [{ role: 'user', content: 'x' }] });
      assert.equal(denied.statusCode, 403);
      assert.equal(denied.json().error.code, 'model_not_permitted');
      assert.equal(forbidden.billing.admitted.length, 0);
      assert.equal(mock.calls.length, 0);
    } finally {
      await missing.app.close();
      await forbidden.app.close();
      await mock.close();
    }
  });

  test('leaves cost unknown (never zero) when upstream omits usage', async () => {
    const mock = await upstream((_call, res) => {
      res.writeHead(200, { 'content-type': 'application/json' });
      res.end(JSON.stringify({ choices: [{ index: 0, message: { role: 'assistant', content: 'ok' }, finish_reason: 'stop' }] }));
    });
    const { app, billing } = await buildHarness({
      pool: { key: keyRow(), route: routeRow([TARGET_ID]), targets: [targetRow(TARGET_ID, mock.baseUrl, 'm')] },
    });
    try {
      const res = await post(app, { model: 'support-chat', messages: [{ role: 'user', content: 'x' }] });
      assert.equal(res.statusCode, 200);
      await waitFor(() => billing.settled.length > 0);
      assert.equal(billing.settled[0].input.status, 'completed');
      assert.equal(billing.settled[0].input.actualMicros, null);
      assert.equal(billing.settled[0].input.inputTokens, null);
    } finally {
      await app.close();
      await mock.close();
    }
  });
});

describe('routing, retries and failures', () => {
  test('uses one safe fallback after an explicit upstream 429 and bills both attempts', async () => {
    const mock = await upstream((call, res) => {
      if (call.model === 'primary-model') {
        res.writeHead(429, { 'content-type': 'application/json', 'retry-after': '2' });
        res.end('{"error":{"message":"limited"}}');
        return;
      }
      res.writeHead(200, { 'content-type': 'application/json' });
      res.end(JSON.stringify({ choices: [{ index: 0, message: { role: 'assistant', content: 'fallback' }, finish_reason: 'stop' }],
        usage: { prompt_tokens: 4, completion_tokens: 2, total_tokens: 6 } }));
    });
    const { app, billing, pool } = await buildHarness({
      pool: {
        key: keyRow(), route: routeRow([TARGET_ID, FALLBACK_ID]),
        targets: [targetRow(TARGET_ID, mock.baseUrl, 'primary-model'), targetRow(FALLBACK_ID, mock.baseUrl, 'fallback-model', { input_price: '500000', output_price: '500000' })],
      },
    });
    try {
      const res = await post(app, { model: 'support-chat', messages: [{ role: 'user', content: 'x' }] });
      assert.equal(res.statusCode, 200);
      assert.equal(res.json().choices[0].message.content, 'fallback');
      assert.equal(mock.calls.length, 2);
      assert.equal(billing.admitted[0].priceSnapshot.attempts.length, 2);
      assert.ok(billing.admitted[0].reserveMicros >= billing.admitted[0].priceSnapshot.attempts[1].reserved_micros);
      await waitFor(() => billing.settled.length > 0);
      const settled = billing.settled[0].input;
      assert.equal(settled.status, 'completed');
      assert.equal(settled.attempts.length, 2);
      assert.equal(settled.attempts[0].error_code, 'upstream_rate_limited');
      assert.equal(settled.attempts[0].cost_micros, 0);
      assert.equal(settled.attempts[1].cost_micros, 2 + 1);
      assert.equal(settled.actualMicros, 3);
      const attemptWrites = pool.queries.filter(q => q.sql.includes('/*gw:attempt*/'));
      assert.ok(attemptWrites.length >= 2);
      assert.deepEqual([...new Set(attemptWrites.map(q => q.params[2]))], [1, 2]);
      assert.equal(attemptWrites.find(q => q.params[2] === 1)!.params[3], TARGET_ID);
      assert.equal(attemptWrites.find(q => q.params[2] === 2)!.params[3], FALLBACK_ID);
      const meta = pool.queries.filter(q => q.sql.includes('/*gw:request-meta*/')).at(-1)!;
      assert.deepEqual(meta.params.slice(0, 3), [billing.admitted[0].requestId, 'support-chat', null]);
    } finally {
      await app.close();
      await mock.close();
    }
  });

  test('does not fall back after an upstream 5xx with unknown execution', async () => {
    const mock = await upstream((_call, res) => { res.writeHead(500, { 'content-type': 'text/plain' }); res.end('boom'); });
    const { app, billing } = await buildHarness({
      pool: { key: keyRow(), route: routeRow([TARGET_ID, FALLBACK_ID]), targets: [targetRow(TARGET_ID, mock.baseUrl, 'primary'), targetRow(FALLBACK_ID, mock.baseUrl, 'fallback')] },
    });
    try {
      const res = await post(app, { model: 'support-chat', messages: [{ role: 'user', content: 'x' }] });
      assert.equal(res.statusCode, 502);
      assert.equal(res.json().error.code, 'upstream_error');
      assert.equal(mock.calls.length, 1);
      await waitFor(() => billing.settled.length > 0);
      assert.equal(billing.settled[0].input.status, 'failed');
      assert.equal(billing.settled[0].input.actualMicros, null);
    } finally {
      await app.close();
      await mock.close();
    }
  });

  test('returns 429 with Retry-After when upstream rate limits without a fallback', async () => {
    const mock = await upstream((_call, res) => { res.writeHead(429, { 'retry-after': '7' }); res.end('{}'); });
    const { app } = await buildHarness({ pool: { key: keyRow(), route: routeRow([TARGET_ID]), targets: [targetRow(TARGET_ID, mock.baseUrl, 'm')] } });
    try {
      const res = await post(app, { model: 'support-chat', messages: [{ role: 'user', content: 'x' }] });
      assert.equal(res.statusCode, 429);
      assert.equal(res.json().error.code, 'upstream_rate_limited');
      assert.equal(res.headers['retry-after'], '7');
      assert.equal(mock.calls.length, 1);
    } finally {
      await app.close();
      await mock.close();
    }
  });

  test('enforces the first-byte deadline and reports a gateway timeout', async () => {
    const mock = await upstream(async (_call, res) => {
      await new Promise(resolve => setTimeout(resolve, 600));
      try { res.writeHead(200, { 'content-type': 'application/json' }); res.end('{}'); } catch { /* aborted */ }
    });
    const { app, billing } = await buildHarness({
      pool: { key: keyRow(), route: routeRow([TARGET_ID]), targets: [targetRow(TARGET_ID, mock.baseUrl, 'm')] },
      timings: { connectMs: 100, firstByteMs: 80, idleMs: 100, totalMs: 2000, heartbeatMs: 100000 },
    });
    try {
      const res = await post(app, { model: 'support-chat', messages: [{ role: 'user', content: 'x' }] });
      assert.equal(res.statusCode, 504);
      assert.equal(res.json().error.code, 'upstream_first_byte_timeout');
      await waitFor(() => billing.settled.length > 0);
      assert.equal(billing.settled[0].input.status, 'timeout');
    } finally {
      await app.close();
      await mock.close();
    }
  });

  test('rejects private/http upstreams unless the deployment opts in', async () => {
    const mock = await upstream((_call, res) => { res.writeHead(200, { 'content-type': 'application/json' }); res.end('{}'); });
    const { app, billing } = await buildHarness({
      allowPrivate: false, pool: { key: keyRow(), route: routeRow([TARGET_ID]), targets: [targetRow(TARGET_ID, mock.baseUrl, 'm')] },
    });
    try {
      const res = await post(app, { model: 'support-chat', messages: [{ role: 'user', content: 'x' }] });
      assert.equal(res.statusCode, 502);
      assert.equal(res.json().error.code, 'upstream_address_rejected');
      assert.equal(mock.calls.length, 0);
      await waitFor(() => billing.settled.length > 0);
      assert.equal(billing.settled[0].input.actualMicros, 0);
    } finally {
      await app.close();
      await mock.close();
    }
  });

  test('aborts the upstream call when the billing lease heartbeat fails', async () => {
    const mock = await upstream(async (_call, res) => {
      await new Promise(resolve => setTimeout(resolve, 200));
      try { res.writeHead(200, { 'content-type': 'application/json' }); res.end('{}'); } catch { /* aborted */ }
    });
    const billing = new FakeBilling();
    billing.heartbeatError = new Error('lease lost');
    const { app } = await buildHarness({
      billing, pool: { key: keyRow(), route: routeRow([TARGET_ID]), targets: [targetRow(TARGET_ID, mock.baseUrl, 'm')] },
      timings: { connectMs: 200, firstByteMs: 500, idleMs: 500, totalMs: 2000, heartbeatMs: 20 },
    });
    try {
      const res = await post(app, { model: 'support-chat', messages: [{ role: 'user', content: 'x' }] });
      assert.equal(res.statusCode, 503);
      assert.equal(res.json().error.code, 'lease_lost');
      assert.ok(billing.heartbeats >= 1);
      await waitFor(() => billing.settled.length > 0);
      assert.equal(billing.settled[0].input.attempts[0].error_code, 'lease_lost');
    } finally {
      await app.close();
      await mock.close();
    }
  });
});

describe('streaming and model listing', () => {
  test('streams SSE, rewrites the alias and settles upstream usage', async () => {
    const mock = await upstream((_call, res) => {
      res.writeHead(200, { 'content-type': 'text/event-stream' });
      res.write(`data: ${JSON.stringify({ id: 'up', model: 'upstream-model', choices: [{ index: 0, delta: { role: 'assistant', content: '你好' }, finish_reason: null }] })}\n\n`);
      res.write(`data: ${JSON.stringify({ id: 'up', model: 'upstream-model', choices: [{ index: 0, delta: {}, finish_reason: 'stop' }], usage: { prompt_tokens: 7, completion_tokens: 3 } })}\n\n`);
      res.end('data: [DONE]\n\n');
    });
    const { app, billing } = await buildHarness({
      pool: { key: keyRow(), route: routeRow([TARGET_ID]), targets: [targetRow(TARGET_ID, mock.baseUrl, 'upstream-model')] },
    });
    try {
      const res = await postReal(app, { model: 'support-chat', stream: true, messages: [{ role: 'user', content: 'hi' }] });
      assert.equal(res.statusCode, 200);
      assert.match(String(res.headers['content-type']), /text\/event-stream/);
      assert.match(res.body, /你好/);
      assert.match(res.body, /"model":"support-chat"/);
      assert.doesNotMatch(res.body, /"model":"upstream-model"/);
      assert.match(res.body, /data: \[DONE\]/);
      assert.equal(JSON.parse(mock.calls[0].raw).stream_options, undefined);
      await waitFor(() => billing.settled.length > 0);
      const settled = billing.settled[0].input;
      assert.equal(settled.status, 'completed');
      assert.equal(settled.actualMicros, 7 + 6);
      assert.equal(settled.inputTokens, 7);
      assert.equal(settled.outputTokens, 3);
    } finally {
      await app.close();
      await mock.close();
    }
  });

  test('reports a partial stream with an SSE error event and never retries after content', async () => {
    const mock = await upstream((_call, res) => {
      res.writeHead(200, { 'content-type': 'text/event-stream' });
      res.write(`data: ${JSON.stringify({ model: 'upstream-model', choices: [{ index: 0, delta: { content: '你好' }, finish_reason: null }] })}\n\n`);
      setTimeout(() => res.destroy(), 10);
    });
    const { app, billing } = await buildHarness({
      pool: { key: keyRow(), route: routeRow([TARGET_ID, FALLBACK_ID]), targets: [targetRow(TARGET_ID, mock.baseUrl, 'primary'), targetRow(FALLBACK_ID, mock.baseUrl, 'fallback')] },
    });
    try {
      const res = await postReal(app, { model: 'support-chat', stream: true, messages: [{ role: 'user', content: 'hi' }] });
      assert.equal(res.statusCode, 200);
      assert.match(res.body, /你好/);
      assert.match(res.body, /event: error/);
      assert.match(res.body, /request_id/);
      assert.equal(mock.calls.length, 1);
      await waitFor(() => billing.settled.length > 0);
      const settled = billing.settled[0].input;
      assert.equal(settled.status, 'partial');
      assert.equal(settled.actualMicros, null);
      assert.equal(settled.attempts.length, 1);
    } finally {
      await app.close();
      await mock.close();
    }
  });

  test('GET /v1/models lists only published aliases visible to the key', async () => {
    const { app } = await buildHarness({
      pool: {
        key: keyRow({ allowed_models: ['support-chat'] }),
        models: [
          { id: ALIAS_ID, name: 'support-chat', created_at: new Date() },
          { id: '88888888-8888-4888-8888-888888888888', name: 'private-alias', created_at: new Date() },
        ],
      },
    });
    try {
      const res = await app.inject({ method: 'GET', url: '/v1/models', headers: { authorization: 'Bearer sk-gateway-key-123456' } });
      assert.equal(res.statusCode, 200);
      const body = res.json();
      assert.equal(body.object, 'list');
      assert.deepEqual(body.data.map((m: any) => m.id), ['support-chat']);
      assert.deepEqual(body.items, body.data);
      assert.ok(res.headers['x-request-id']);
      const unauthenticated = await app.inject({ method: 'GET', url: '/v1/models' });
      assert.equal(unauthenticated.statusCode, 401);
      assert.equal(unauthenticated.json().error.code, 'invalid_api_key');
    } finally {
      await app.close();
    }
  });

  test('treats clean EOF without [DONE] as an incomplete partial stream', async () => {
    const mock = await upstream((_call, res) => {
      res.writeHead(200, { 'content-type': 'text/event-stream' });
      res.write(`data: ${JSON.stringify({ model: 'upstream-model', choices: [{ index: 0, delta: { content: 'truncated' }, finish_reason: null }] })}\n\n`);
      res.end();
    });
    const { app, billing } = await buildHarness({
      pool: { key: keyRow(), route: routeRow([TARGET_ID]), targets: [targetRow(TARGET_ID, mock.baseUrl, 'upstream-model')] },
    });
    try {
      const res = await postReal(app, { model: 'support-chat', stream: true, messages: [{ role: 'user', content: 'hi' }] });
      assert.equal(res.statusCode, 200);
      assert.match(res.body, /truncated/);
      assert.match(res.body, /event: error/);
      assert.match(res.body, /upstream_incomplete_stream/);
      assert.doesNotMatch(res.body, /\[DONE\]/);
      await waitFor(() => billing.settled.length > 0);
      const settled = billing.settled[0].input;
      assert.equal(settled.status, 'partial');
      assert.equal(settled.actualMicros, null);
      assert.equal(settled.attempts[0].error_code, 'upstream_incomplete_stream');
    } finally {
      await app.close();
      await mock.close();
    }
  });

  test('requests stream usage only when the target declares the stream_usage capability', async () => {
    const mock = await upstream((_call, res) => {
      res.writeHead(200, { 'content-type': 'text/event-stream' });
      res.end('data: [DONE]\n\n');
    });
    const { app } = await buildHarness({
      pool: { key: keyRow(), route: routeRow([TARGET_ID]), targets: [targetRow(TARGET_ID, mock.baseUrl, 'upstream-model', { capabilities: ['chat', 'stream_usage'] })] },
    });
    try {
      const res = await postReal(app, { model: 'support-chat', stream: true, messages: [{ role: 'user', content: 'hi' }] });
      assert.equal(res.statusCode, 200);
      assert.deepEqual(JSON.parse(mock.calls[0].raw).stream_options, { include_usage: true });
    } finally {
      await app.close();
      await mock.close();
    }
  });

  test('sink write unblocks at the downstream deadline when a slow client never drains', async () => {
    const raw = new EventEmitter() as any;
    raw.write = () => false;
    raw.writeHead = () => raw;
    raw.destroyed = false;
    raw.writableEnded = false;
    raw.destroy = () => { raw.destroyed = true; };
    const issue = Date.now();
    const sink = new ReplyStreamSink({ raw, hijack() {} } as unknown as FastifyReply, 'req-sink', issue + 80);
    assert.equal(sink.started, false);
    await assert.rejects(() => sink.write('data: x\n\n'),
      (err: any) => err.statusCode === 504 && err.code === 'client_write_timeout');
    assert.ok(Date.now() - issue < 1000);
    assert.equal(raw.destroyed, true);
  });

  test('sink write resolves once the client drains', async () => {
    const raw = new EventEmitter() as any;
    raw.write = () => false;
    raw.writeHead = () => raw;
    raw.destroyed = false;
    raw.writableEnded = false;
    const sink = new ReplyStreamSink({ raw, hijack() {} } as unknown as FastifyReply, 'req-sink-2', Date.now() + 2000);
    const pending = sink.write('data: x\n\n');
    setTimeout(() => raw.emit('drain'), 10);
    await pending;
    assert.equal(sink.started, true);
  });
});
