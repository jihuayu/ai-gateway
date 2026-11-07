/**
 * Data plane for the AI gateway: GET /v1/models and POST /v1/chat/completions.
 *
 * Schema assumptions (see migrations/001.sql, read at integration time):
 *  - api_keys.allowed_models jsonb: [] means "all models", otherwise entries match alias name or alias id.
 *  - route_versions.targets jsonb: array of model_targets.id strings, order = primary first, optional fallback second.
 *  - model_targets.input_price/output_price: integer micro-USD per million tokens.
 *  - projects.rpm / concurrency / budget_micros / max_output_tokens: nullable project policy, defaults applied here.
 *  - route_versions.price_snapshot is written by the control plane; this module freezes its own snapshot per request
 *    from the immutable route version ids plus the prices read at admission time.
 *
 * Prompts and completions are never persisted: only counts, costs and error codes leave this module.
 */
import { randomUUID } from 'node:crypto';
import { lookup as dnsLookup } from 'node:dns';
import { isIP } from 'node:net';
import { request as httpRequest, type IncomingMessage } from 'node:http';
import { request as httpsRequest } from 'node:https';
import type { FastifyInstance, FastifyReply, FastifyRequest } from 'fastify';
import type { Pool } from 'pg';
import { z } from 'zod';
import type { Billing } from './billing.js';
import { decrypt, hashKey, isBlockedAddress, validateProviderUrl } from './security.js';

export const DEFAULT_TIMINGS = {
  connectMs: 5_000,
  firstByteMs: 30_000,
  idleMs: 30_000,
  totalMs: 120_000,
  heartbeatMs: 10_000,
} as const;
export type Timings = { -readonly [K in keyof typeof DEFAULT_TIMINGS]: number };

export const LIMITS = {
  maxBodyBytes: 1024 * 1024,
  maxMessages: 200,
  maxMessageChars: 200_000,
  maxTotalChars: 400_000,
  defaultMaxOutputTokens: 4_096,
  maxMaxOutputTokens: 32_768,
  maxUpstreamJsonBytes: 8 * 1024 * 1024,
  maxSseEventBytes: 1024 * 1024,
  maxAttempts: 2,
  defaultProjectRpm: 600,
  defaultKeyRpm: 600,
  defaultConcurrency: 32,
} as const;

export class GatewayError extends Error {
  constructor(readonly statusCode: number, readonly code: string, message: string) {
    super(message);
    this.name = 'GatewayError';
  }
}

export type GatewayBilling = Pick<Billing,'admit'|'dispatch'|'heartbeat'|'settle'|'cancelBeforeDispatch'>;
export interface GatewayDataPlane {
  authenticateKey(req:FastifyRequest):Promise<KeyContext>;
  loadRoute(key:KeyContext,model:string):Promise<RouteSnapshot|null>;
  models(key:KeyContext):Promise<Array<Record<string,unknown>>>;
  persistMetadata(requestId:string,aliasName:string|null,status:SettlementStatus,attempts:AttemptRecord[],attemptsToPersist?:AttemptRecord[]):Promise<void>;
}
export interface GatewayDeps {
  billing: GatewayBilling;
  dataPlane: GatewayDataPlane;
  encryptionKey: string;
  allowPrivateUpstream: boolean;
  /** Test/deployment overrides; the contract fields above are sufficient in production. */
  timings?: Partial<Timings>;
}

export interface KeyRow {
  key_id: string;
  prefix: string;
  allowed_models: unknown;
  expires_at: Date | string | null;
  revoked_at: Date | string | null;
  key_rpm: number | null;
  project_id: string;
  workspace_id: string;
  project_rpm: number | null;
  concurrency: number | null;
  budget_micros: string | number | null;
  max_output_tokens: number | null;
}

export interface KeyContext {
  runtimeRevision?: string;
  runtimeKeyHash?: string;
  runtimeExpiresAt?: number | null;
  keyId: string;
  projectId: string;
  workspaceId: string;
  allowedModels: string[];
  projectRpm: number;
  keyRpm: number;
  concurrency: number;
  budgetMicros: number;
  maxOutputTokens: number | null;
}

const KEY_SQL = `/*gw:key*/
SELECT k.id AS key_id, k.prefix, k.allowed_models, k.expires_at, k.revoked_at, k.key_rpm,
       p.id AS project_id, p.workspace_id, p.rpm AS project_rpm, p.concurrency,
       p.budget_micros, p.max_output_tokens
  FROM api_keys k JOIN projects p ON p.id = k.project_id
 WHERE k.key_hash = $1`;

export const TOUCH_KEY_SQL = `/*gw:touch*/ UPDATE api_keys SET last_used_at = now() WHERE id = $1`;

export function bearerToken(req: FastifyRequest): string {
  const raw = req.headers.authorization;
  if (typeof raw !== 'string' || !/^Bearer[ ]+/i.test(raw)) {
    throw new GatewayError(401, 'invalid_api_key', '缺少有效的 API Key。');
  }
  const token = raw.replace(/^Bearer[ ]+/i, '').trim();
  if (token.length < 8 || token.length > 512) throw new GatewayError(401, 'invalid_api_key', '缺少有效的 API Key。');
  return token;
}

/** Non-negative safe integer from pg bigint (string) or number. */
export function safeNonNegative(value: unknown, field: string): number {
  const n = typeof value === 'number' ? value : Number(String(value));
  if (!Number.isSafeInteger(n) || n < 0) throw new GatewayError(500, 'invalid_configuration', `${field} 配置无效。`);
  return n;
}

function optionalSafeNonNegative(value: unknown, field: string): number | null {
  if (value === null || value === undefined) return null;
  return safeNonNegative(value, field);
}

function allowedModelsFrom(value: unknown): string[] {
  if (!Array.isArray(value)) return [];
  return value.filter((v): v is string => typeof v === 'string' && v.length > 0).slice(0, 500);
}

export async function authenticateKey(req: FastifyRequest, pool: Pool): Promise<KeyContext> {
  const token = bearerToken(req);
  const result = await pool.query(KEY_SQL, [hashKey(token)]);
  const row = result.rows[0] as KeyRow | undefined;
  const context=keyContextFromRow(row);
  void pool.query(TOUCH_KEY_SQL, [row!.key_id]).catch(() => undefined);
  return context;
}

export function keyContextFromRow(row:KeyRow|undefined):KeyContext {
  if (!row) throw new GatewayError(401, 'invalid_api_key', 'API Key 无效或已失效。');
  if (row.revoked_at) throw new GatewayError(401, 'invalid_api_key', 'API Key 无效或已失效。');
  if (row.expires_at && new Date(row.expires_at).getTime() <= Date.now()) {
    throw new GatewayError(401, 'invalid_api_key', 'API Key 无效或已失效。');
  }
  const budget = row.budget_micros === null || row.budget_micros === undefined ? -1 : safeNonNegative(row.budget_micros, 'budget_micros');
  return {
    keyId: row.key_id,
    projectId: row.project_id,
    workspaceId: row.workspace_id,
    allowedModels: allowedModelsFrom(row.allowed_models),
    projectRpm: row.project_rpm ?? LIMITS.defaultProjectRpm,
    keyRpm: row.key_rpm ?? LIMITS.defaultKeyRpm,
    concurrency: row.concurrency ?? LIMITS.defaultConcurrency,
    budgetMicros: budget,
    maxOutputTokens: row.max_output_tokens ?? null,
  };
}

/** A key scoped to specific models may only see/use those aliases; [] means unrestricted. */
export function keyCanUseModel(key: KeyContext, aliasId: string, aliasName: string): boolean {
  if (key.allowedModels.length === 0) return true;
  return key.allowedModels.includes(aliasName) || key.allowedModels.includes(aliasId);
}

const messageSchema = z.strictObject({
  role: z.enum(['system', 'user', 'assistant']),
  content: z.string().max(LIMITS.maxMessageChars),
});

const chatSchema = z.strictObject({
  model: z.string().min(1).max(128),
  messages: z.array(messageSchema).min(1).max(LIMITS.maxMessages),
  stream: z.boolean().optional(),
  temperature: z.number().min(0).max(2).optional(),
  max_tokens: z.number().int().min(1).max(LIMITS.maxMaxOutputTokens).optional(),
});

export interface ChatMessage { role: 'system' | 'user' | 'assistant'; content: string }
export interface ChatRequest {
  model: string;
  messages: ChatMessage[];
  stream: boolean;
  temperature?: number;
  max_tokens?: number;
}

function zodToGatewayError(err: z.ZodError): GatewayError {
  const issue = err.issues[0];
  if (!issue) return new GatewayError(400, 'invalid_request', '请求体无效。');
  if (issue.code === 'unrecognized_keys') {
    const keys = (issue as unknown as { keys: string[] }).keys;
    return new GatewayError(400, 'unsupported_parameter', `不支持的参数：${keys.join(', ')}`);
  }
  if (issue.code === 'invalid_value' && issue.path[0] === 'messages') {
    return new GatewayError(400, 'unsupported_role', '仅支持 system、user、assistant 角色。');
  }
  const path = issue.path.length ? issue.path.join('.') : 'body';
  return new GatewayError(400, 'invalid_request', `${path}: ${issue.message}`);
}

/** Strict request subset: unknown fields and roles are rejected instead of silently ignored. */
export function validateChatBody(body: unknown): ChatRequest {
  if (body === null || typeof body !== 'object' || Array.isArray(body)) {
    throw new GatewayError(400, 'invalid_request', '请求体必须是 JSON 对象。');
  }
  const parsed = chatSchema.safeParse(body);
  if (!parsed.success) throw zodToGatewayError(parsed.error);
  const totalChars = parsed.data.messages.reduce((sum, m) => sum + m.content.length, 0);
  if (totalChars > LIMITS.maxTotalChars) {
    throw new GatewayError(400, 'invalid_messages', '消息内容超过长度上限。');
  }
  return { ...parsed.data, stream: parsed.data.stream === true };
}

/** Deliberately conservative input estimate (chars/3 + per-message overhead) used for reservation only. */
export function estimateInputTokens(messages: ChatMessage[]): number {
  let chars = 0;
  for (const message of messages) chars += message.content.length + message.role.length + 4;
  return Math.max(1, Math.ceil(chars / 3) + messages.length * 4 + 8);
}

/** Integer micro-USD for a token count; ceil, bigint math, rejects unsafe results. */
export function tokensToMicros(tokens: number, microsPerMillion: number): number {
  const t = BigInt(tokens);
  const p = BigInt(microsPerMillion);
  if (t < 0n || p < 0n) throw new GatewayError(500, 'invalid_configuration', '价格配置无效。');
  const micros = (t * p + 999_999n) / 1_000_000n;
  if (micros > BigInt(Number.MAX_SAFE_INTEGER)) throw new GatewayError(500, 'price_overflow', '价格计算超出安全范围。');
  return Number(micros);
}

export interface PriceEntry {
  attempt_no: number;
  target_id: string;
  upstream_model: string;
  input_micros_per_million: number;
  output_micros_per_million: number;
  max_output_tokens: number;
  estimated_input_tokens: number;
  reserved_micros: number;
}

export interface PriceSnapshot {
  version: 1;
  currency: 'USD';
  unit: 'tokens';
  period: string;
  max_attempts: number;
  estimated_input_tokens: number;
  max_output_tokens: number;
  total_reserved_micros: number;
  attempts: PriceEntry[];
}

export function estimateAttemptMicros(inputTokens: number, maxOutputTokens: number, inputPrice: number, outputPrice: number): number {
  return tokensToMicros(inputTokens, inputPrice) + tokensToMicros(maxOutputTokens, outputPrice);
}

export function usageMicros(inputTokens: number, outputTokens: number, inputPrice: number, outputPrice: number): number {
  return tokensToMicros(inputTokens, inputPrice) + tokensToMicros(outputTokens, outputPrice);
}

export function utcPeriod(now = new Date()): string {
  return `${now.getUTCFullYear()}-${String(now.getUTCMonth() + 1).padStart(2, '0')}`;
}

export interface RouteTarget {
  targetId: string;
  upstreamModel: string;
  inputPrice: number;
  outputPrice: number;
  baseUrl: string;
  encryptedSecret: string;
  targetEnabled: boolean;
  providerEnabled: boolean;
  /** Declared model_targets.capabilities; only an explicit 'stream_usage' enables stream_options. */
  capabilities: string[];
}

export interface RouteSnapshot {
  aliasId: string;
  aliasName: string;
  routeVersionId: string;
  version: number;
  capabilities: string[];
  targets: RouteTarget[];
}

const ROUTE_SQL = `/*gw:route*/
SELECT a.id AS alias_id, a.name AS alias_name, rv.id AS route_version_id, rv.version, rv.targets, rv.capabilities
  FROM aliases a JOIN route_versions rv ON rv.id = a.current_version_id
 WHERE a.project_id = $1 AND a.name = $2 AND rv.status = 'published'`;

const TARGETS_SQL = `/*gw:targets*/
SELECT t.id, t.upstream_model, t.input_price::text AS input_price, t.output_price::text AS output_price,
       t.capabilities, t.enabled AS target_enabled, p.id AS provider_id, p.base_url, p.encrypted_secret,
       p.enabled AS provider_enabled
  FROM model_targets t JOIN providers p ON p.id = t.provider_id
 WHERE t.id = ANY($1::uuid[]) AND p.workspace_id = $2`;

export const MODELS_SQL = `/*gw:models*/
SELECT a.id, a.name, a.created_at
  FROM aliases a JOIN route_versions rv ON rv.id = a.current_version_id
 WHERE a.project_id = $1 AND rv.status = 'published'
 ORDER BY a.name`;

/** route_versions.targets is a jsonb array of model_targets ids; tolerate legacy object entries. */
export function extractTargetIds(value: unknown): string[] {
  if (!Array.isArray(value)) return [];
  const out: string[] = [];
  for (const entry of value) {
    if (typeof entry === 'string' && entry) out.push(entry);
    else if (entry && typeof entry === 'object') {
      const obj = entry as Record<string, unknown>;
      for (const key of ['target_id', 'targetId', 'id', 'model_target_id']) {
        const id = obj[key];
        if (typeof id === 'string' && id) { out.push(id); break; }
      }
    }
  }
  return out;
}

/** Load the immutable published route for an alias; null when the alias is absent or unpublished. */
export async function loadRoute(pool: Pick<Pool,'query'>, projectId: string, workspaceId: string, aliasName: string): Promise<RouteSnapshot | null> {
  const routeResult = await pool.query(ROUTE_SQL, [projectId, aliasName]);
  const row = routeResult.rows[0] as Record<string, unknown> | undefined;
  if (!row) return null;
  const targetIds = extractTargetIds(row.targets).slice(0, LIMITS.maxAttempts);
  if (targetIds.length === 0) {
    return {
      aliasId: String(row.alias_id), aliasName: String(row.alias_name), routeVersionId: String(row.route_version_id),
      version: Number(row.version), capabilities: stringArray(row.capabilities), targets: [],
    };
  }
  const targetRows = (await pool.query(TARGETS_SQL, [targetIds, workspaceId])).rows as Array<Record<string, unknown>>;
  const byId = new Map(targetRows.map(r => [String(r.id), r]));
  const targets: RouteTarget[] = [];
  for (const id of targetIds) {
    const target = byId.get(id);
    if (!target) continue;
    targets.push({
      targetId: id,
      upstreamModel: String(target.upstream_model),
      inputPrice: safeNonNegative(target.input_price, 'input_price'),
      outputPrice: safeNonNegative(target.output_price, 'output_price'),
      baseUrl: String(target.base_url),
      encryptedSecret: String(target.encrypted_secret),
      targetEnabled: target.target_enabled === true,
      providerEnabled: target.provider_enabled === true,
      capabilities: stringArray(target.capabilities),
    });
  }
  return {
    aliasId: String(row.alias_id), aliasName: String(row.alias_name), routeVersionId: String(row.route_version_id),
    version: Number(row.version), capabilities: stringArray(row.capabilities), targets,
  };
}

function stringArray(value: unknown): string[] {
  return Array.isArray(value) ? value.filter((v): v is string => typeof v === 'string') : [];
}

/** Providers may store a raw token or a small JSON credential blob. */
export function providerToken(encryptedSecret: string, encryptionKey: string): string {
  const plain = decrypt(encryptedSecret, encryptionKey).trim();
  if (plain.startsWith('{')) {
    try {
      const parsed = JSON.parse(plain) as Record<string, unknown>;
      for (const key of ['apiKey', 'api_key', 'token', 'secret', 'key']) {
        const value = parsed[key];
        if (typeof value === 'string' && value) return value;
      }
    } catch {
      // fall through to raw credential
    }
  }
  if (!plain) throw new GatewayError(502, 'upstream_credentials_invalid', '上游凭据不可用。');
  return plain;
}

export function chatEndpoint(baseUrl: string): URL {
  let url: URL;
  try {
    url = new URL(baseUrl);
  } catch {
    throw new GatewayError(502, 'invalid_provider_url', '上游服务地址无效。');
  }
  url.search = '';
  url.hash = '';
  if (!url.pathname.endsWith('/')) url.pathname += '/';
  return new URL('chat/completions', url);
}

export class UpstreamFailure extends GatewayError {
  constructor(code: string, statusCode: number, message: string,
    readonly requestSent: boolean, readonly executionUnknown: boolean, readonly retrySafe: boolean) {
    super(statusCode, code, message);
    this.name = 'UpstreamFailure';
  }
}

const TLS_ERRORS = new Set([
  'DEPTH_ZERO_SELF_SIGNED_CERT', 'SELF_SIGNED_CERT_IN_CHAIN', 'UNABLE_TO_VERIFY_LEAF_SIGNATURE',
  'UNABLE_TO_GET_ISSUER_CERT_LOCALLY', 'CERT_HAS_EXPIRED', 'ERR_TLS_CERT_ALTNAME_INVALID',
]);
const NOT_SENT_ERRORS = new Set([
  'ECONNREFUSED', 'ENOTFOUND', 'EAI_AGAIN', 'EHOSTUNREACH', 'ENETUNREACH', 'EADDRNOTAVAIL', 'ERR_INVALID_IP_ADDRESS',
]);

function isTlsError(code: string): boolean {
  return TLS_ERRORS.has(code) || code.startsWith('ERR_TLS_') || code.startsWith('ERR_SSL_');
}

/** Classify a transport failure. Only failures that provably sent nothing may be retried on a fallback. */
export function classifyTransportError(err: unknown, abortCode: string | null): UpstreamFailure {
  if (abortCode === 'lease_lost') return new UpstreamFailure('lease_lost', 503, '请求租约失效。', true, true, false);
  if (abortCode === 'upstream_connect_timeout') {
    return new UpstreamFailure('upstream_connect_timeout', 504, '连接上游超时。', false, false, true);
  }
  if (abortCode === 'request_timeout') return new UpstreamFailure('request_timeout', 504, '请求超过整体时间上限。', true, true, false);
  if (abortCode === 'upstream_first_byte_timeout') {
    return new UpstreamFailure('upstream_first_byte_timeout', 504, '等待上游响应超时。', true, true, false);
  }
  if (abortCode === 'upstream_idle_timeout') {
    return new UpstreamFailure('upstream_idle_timeout', 504, '上游响应中断超时。', true, true, false);
  }
  const rawCode = (err as { code?: unknown } | null)?.code;
  const code = typeof rawCode === 'string' ? rawCode : 'upstream_connection_error';
  if (code === 'ERR_BLOCKED_ADDRESS') {
    return new UpstreamFailure('upstream_address_rejected', 502, '上游地址未通过安全校验。', false, false, false);
  }
  if (NOT_SENT_ERRORS.has(code)) {
    return new UpstreamFailure('upstream_connect_failed', 502, '无法连接上游服务。', false, false, true);
  }
  if (isTlsError(code)) return new UpstreamFailure('upstream_tls_error', 502, '上游 TLS 校验失败。', false, false, true);
  return new UpstreamFailure('upstream_connection_error', 502, '上游连接中断。', true, true, false);
}

export interface UpstreamHandle {
  res: IncomingMessage;
  status: number;
  headers: NodeJS.Dict<string | string[]>;
  touch(): void;
  abort(code: string): void;
  cleanup(): void;
  abortCode(): string | null;
}

function blockedAddressError(): NodeJS.ErrnoException {
  const err = new Error('upstream address is blocked') as NodeJS.ErrnoException;
  err.code = 'ERR_BLOCKED_ADDRESS';
  return err;
}

/** Connect-time DNS guard: every resolved address is checked and one vetted address is pinned for the socket. */
export function guardedLookup(allowPrivate: boolean) {
  return (hostname: string, options: Record<string, unknown>, callback: (...args: unknown[]) => void): void => {
    const host = hostname.replace(/^\[|\]$/g, '');
    if (isIP(host)) {
      if (!allowPrivate && isBlockedAddress(host)) return callback(blockedAddressError());
      return callback(null, host, isIP(host));
    }
    dnsLookup(host, { all: true, verbatim: true }, (err, addresses) => {
      if (err) return callback(err);
      const usable = addresses.filter(a => allowPrivate || !isBlockedAddress(a.address));
      if (usable.length === 0) return callback(blockedAddressError());
      if (options.all === true) return callback(null, usable);
      return callback(null, usable[0].address, usable[0].family);
    });
  };
}

export interface StartUpstreamOptions {
  endpoint: URL;
  token: string;
  body: string;
  requestId: string;
  accept: string;
  allowPrivate: boolean;
  timings: Timings;
  deadlineAt: number;
  onAbort?: (code: string) => void;
  onAbortReady?: (abort: (code: string) => void) => void;
}

/** Open the upstream request, enforcing URL validation, connect-time IP checks and phase timers. */
export async function startUpstream(opts: StartUpstreamOptions): Promise<UpstreamHandle> {
  const { endpoint, timings, deadlineAt } = opts;
  try {
    await validateProviderUrl(endpoint.toString(), opts.allowPrivate);
  } catch {
    throw new UpstreamFailure('upstream_address_rejected', 502, '上游地址未通过安全校验。', false, false, false);
  }
  const controller = new AbortController();
  let aborted: string | null = null;
  let idleTimer: NodeJS.Timeout | undefined;
  let headersTimer: NodeJS.Timeout | undefined;
  let connectTimer: NodeJS.Timeout | undefined;
  let exitTimer: NodeJS.Timeout | undefined;
  const clear = () => { for (const t of [idleTimer, headersTimer, connectTimer, exitTimer]) if (t) clearTimeout(t); };
  const abort = (code: string) => {
    if (aborted) return;
    aborted = code;
    clear();
    opts.onAbort?.(code);
    controller.abort(new Error(code));
  };
  opts.onAbortReady?.(abort);
  const remaining = Math.max(1, deadlineAt - Date.now());
  exitTimer = setTimeout(() => abort('request_timeout'), remaining);
  connectTimer = setTimeout(() => abort('upstream_connect_timeout'), Math.min(timings.connectMs, remaining));
  headersTimer = setTimeout(() => abort('upstream_first_byte_timeout'), Math.min(timings.firstByteMs, remaining));
  const transport = endpoint.protocol === 'https:' ? httpsRequest : httpRequest;
  const req = transport({
    protocol: endpoint.protocol,
    hostname: endpoint.hostname,
    port: endpoint.port || undefined,
    path: `${endpoint.pathname}${endpoint.search}`,
    method: 'POST',
    agent: false,
    signal: controller.signal,
    lookup: guardedLookup(opts.allowPrivate) as never,
    headers: {
      'content-type': 'application/json',
      accept: opts.accept,
      authorization: `Bearer ${opts.token}`,
      'user-agent': 'ai-gateway/0.1',
      'x-request-id': opts.requestId,
      'content-length': Buffer.byteLength(opts.body),
    },
  });
  req.on('socket', socket => {
    if (!socket.connecting) { if (connectTimer) clearTimeout(connectTimer); return; }
    socket.once(endpoint.protocol === 'https:' ? 'secureConnect' : 'connect', () => { if (connectTimer) clearTimeout(connectTimer); });
  });
  const res = await new Promise<IncomingMessage>((resolve, reject) => {
    req.once('response', resolve);
    req.once('error', (err: unknown) => reject(classifyTransportError(err, aborted)));
    req.end(opts.body);
  });
  if (headersTimer) clearTimeout(headersTimer);
  const armIdle = () => {
    if (aborted) return;
    if (idleTimer) clearTimeout(idleTimer);
    idleTimer = setTimeout(() => abort('upstream_idle_timeout'), Math.min(timings.idleMs, Math.max(1, deadlineAt - Date.now())));
  };
  armIdle();
  return {
    res,
    status: res.statusCode ?? 0,
    headers: res.headers,
    touch: armIdle,
    abort,
    cleanup: () => { clear(); },
    abortCode: () => aborted,
  };
}

export interface SseEvent { event: string | null; data: string }

/** Incremental SSE frame decoder; bounded so a hostile upstream cannot grow memory without limit. */
export class SseParser {
  private buffer = '';
  push(chunk: string): SseEvent[] {
    this.buffer += chunk;
    if (this.buffer.length > LIMITS.maxSseEventBytes) {
      throw new UpstreamFailure('upstream_protocol_error', 502, '上游流式响应格式无效。', true, true, false);
    }
    const events: SseEvent[] = [];
    for (;;) {
      const match = /\r?\n\r?\n/.exec(this.buffer);
      if (!match) break;
      const raw = this.buffer.slice(0, match.index);
      this.buffer = this.buffer.slice(match.index + match[0].length);
      const event = parseSseFrame(raw);
      if (event) events.push(event);
    }
    return events;
  }
  flush(): SseEvent[] {
    if (!this.buffer.trim()) return [];
    const raw = this.buffer;
    this.buffer = '';
    const event = parseSseFrame(raw);
    return event ? [event] : [];
  }
}

function parseSseFrame(raw: string): SseEvent | null {
  let event: string | null = null;
  const data: string[] = [];
  for (const line of raw.split(/\r?\n/)) {
    if (!line || line.startsWith(':')) continue;
    const colon = line.indexOf(':');
    const field = colon === -1 ? line : line.slice(0, colon);
    let value = colon === -1 ? '' : line.slice(colon + 1);
    if (value.startsWith(' ')) value = value.slice(1);
    if (field === 'event') event = value;
    else if (field === 'data') data.push(value);
  }
  if (data.length === 0 && event === null) return null;
  return { event, data: data.join('\n') };
}

export function formatSseEvent(event: SseEvent): string {
  const prefix = event.event ? `event: ${event.event}\n` : '';
  return `${prefix}data: ${event.data}\n\n`;
}

export interface TokenUsage { input: number; output: number }

/** Upstream usage is optional; anything missing or malformed stays unknown (null), never zero. */
export function parseUsage(value: unknown): TokenUsage | null {
  if (!value || typeof value !== 'object' || Array.isArray(value)) return null;
  const usage = value as Record<string, unknown>;
  const input = usage.prompt_tokens;
  const output = usage.completion_tokens;
  if (!Number.isInteger(input) || (input as number) < 0) return null;
  if (!Number.isInteger(output) || (output as number) < 0) return null;
  return { input: input as number, output: output as number };
}

export async function readBuffered(handle: UpstreamHandle): Promise<Buffer> {
  const chunks: Buffer[] = [];
  let size = 0;
  try {
    for await (const chunk of handle.res) {
      handle.touch();
      const buf = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk as string);
      size += buf.length;
      if (size > LIMITS.maxUpstreamJsonBytes) {
        handle.abort('upstream_response_too_large');
        throw new UpstreamFailure('upstream_response_too_large', 502, '上游响应体过大。', true, true, false);
      }
      chunks.push(buf);
    }
  } catch (err) {
    if (err instanceof UpstreamFailure) throw err;
    throw classifyTransportError(err, handle.abortCode());
  } finally {
    handle.cleanup();
  }
  return Buffer.concat(chunks);
}

export function upstreamPayload(target: RouteTarget, body: ChatRequest, maxOutputTokens: number): string {
  const payload: Record<string, unknown> = {
    model: target.upstreamModel,
    messages: body.messages.map(m => ({ role: m.role, content: m.content })),
    max_tokens: body.max_tokens ?? maxOutputTokens,
  };
  if (body.temperature !== undefined) payload.temperature = body.temperature;
  if (body.stream) {
    payload.stream = true;
    if (target.capabilities.includes('stream_usage')) payload.stream_options = { include_usage: true };
  }
  return JSON.stringify(payload);
}

/** Map an upstream completion onto the supported response subset; upstream ids are not exposed. */
export function normalizeCompletion(upstream: unknown, aliasName: string, requestId: string, usage: TokenUsage | null): Record<string, unknown> {
  if (!upstream || typeof upstream !== 'object' || Array.isArray(upstream)) {
    throw new GatewayError(502, 'upstream_invalid_response', '上游返回了无法识别的响应。');
  }
  const body = upstream as Record<string, unknown>;
  if (!Array.isArray(body.choices) || body.choices.length === 0) {
    throw new GatewayError(502, 'upstream_invalid_response', '上游返回了无法识别的响应。');
  }
  const choices = body.choices.map((raw, index) => {
    const choice = (raw ?? {}) as Record<string, unknown>;
    const message = (choice.message ?? {}) as Record<string, unknown>;
    const content = typeof message.content === 'string' ? message.content : '';
    const finish = typeof choice.finish_reason === 'string' ? choice.finish_reason : null;
    return { index: typeof choice.index === 'number' ? choice.index : index, message: { role: 'assistant', content }, finish_reason: finish };
  });
  return {
    id: `chatcmpl_${requestId}`,
    object: 'chat.completion',
    created: Math.floor(Date.now() / 1000),
    model: aliasName,
    choices,
    usage: usage ? { prompt_tokens: usage.input, completion_tokens: usage.output, total_tokens: usage.input + usage.output } : null,
  };
}

export type SettlementStatus = 'completed' | 'failed' | 'partial' | 'cancelled' | 'timeout';

export interface AttemptRecord {
  attempt_no: number;
  target_id: string;
  upstream_model: string;
  status: 'ok' | 'error';
  http_status: number | null;
  error_code: string | null;
  request_sent: boolean;
  execution_unknown: boolean;
  usage_known: boolean;
  input_tokens: number | null;
  output_tokens: number | null;
  cost_micros: number | null;
  upper_bound_micros: number;
  retry_safe: boolean;
  latency_ms: number;
}

export interface GatewayRuntime {
  requestId: string;
  aliasName: string;
  body: ChatRequest;
  maxOutputTokens: number;
  timings: Timings;
  allowPrivate: boolean;
  encryptionKey: string;
  deadlineAt: number;
  setActiveAbort(abort: ((code: string) => void) | null): void;
  clientAborted(): boolean;
}

export function newRecord(target: RouteTarget, attemptNo: number, upperBoundMicros: number): AttemptRecord {
  return {
    attempt_no: attemptNo, target_id: target.targetId, upstream_model: target.upstreamModel, status: 'error',
    http_status: null, error_code: null, request_sent: false, execution_unknown: false, usage_known: false,
    input_tokens: null, output_tokens: null, cost_micros: null, upper_bound_micros: upperBoundMicros,
    retry_safe: false, latency_ms: 0,
  };
}

/** No trickle of raw upstream error text: only a stable code and a safe message. */
export function upstreamHttpError(status: number): { code: string; statusCode: number; message: string; retrySafe: boolean } {
  if (status >= 300 && status < 400) return { code: 'upstream_redirect', statusCode: 502, message: '上游返回了重定向。', retrySafe: false };
  if (status === 400 || status === 422) return { code: 'upstream_invalid_request', statusCode: 400, message: '上游拒绝了请求参数。', retrySafe: false };
  if (status === 401 || status === 403) return { code: 'upstream_auth_failed', statusCode: 502, message: '上游鉴权失败。', retrySafe: false };
  if (status === 404) return { code: 'upstream_model_unavailable', statusCode: 502, message: '上游模型不可用。', retrySafe: false };
  if (status === 408) return { code: 'upstream_timeout', statusCode: 504, message: '上游请求超时。', retrySafe: false };
  if (status === 429) return { code: 'upstream_rate_limited', statusCode: 429, message: '上游限流，请稍后重试。', retrySafe: true };
  if (status === 503) return { code: 'upstream_unavailable', statusCode: 503, message: '上游服务暂时不可用。', retrySafe: false };
  return { code: 'upstream_error', statusCode: 502, message: '上游服务返回错误。', retrySafe: false };
}

export function retryAfterSeconds(headers: NodeJS.Dict<string | string[]>): number | null {
  const raw = headers['retry-after'];
  const value = Array.isArray(raw) ? raw[0] : raw;
  if (!value || !/^\d{1,5}$/.test(value)) return null;
  const seconds = Number(value);
  return seconds > 0 && seconds <= 3600 ? seconds : null;
}

export function canFallback(record: AttemptRecord, attemptIndex: number, totalTargets: number): boolean {
  if (attemptIndex + 1 >= totalTargets) return false;
  if (record.status === 'ok') return false;
  return record.retry_safe;
}

/**
 * Aggregate per-attempt usage. Any attempt that may have executed without usage keeps the whole
 * settlement unknown (actualMicros null) so billing retains the conservative reservation.
 */
export function aggregateSettlement(attempts: AttemptRecord[]): { actualMicros: number | null; inputTokens: number | null; outputTokens: number | null } {
  let actual = 0;
  let input = 0;
  let output = 0;
  for (const attempt of attempts) {
    if (!attempt.usage_known) return { actualMicros: null, inputTokens: null, outputTokens: null };
    actual += attempt.cost_micros ?? 0;
    input += attempt.input_tokens ?? 0;
    output += attempt.output_tokens ?? 0;
    if (!Number.isSafeInteger(actual) || !Number.isSafeInteger(input) || !Number.isSafeInteger(output)) {
      return { actualMicros: null, inputTokens: null, outputTokens: null };
    }
  }
  return { actualMicros: actual, inputTokens: input, outputTokens: output };
}

export function publicAttempts(attempts: AttemptRecord[]): unknown[] {
  return attempts.map(a => ({ ...a }));
}

function markNotExecuted(record: AttemptRecord): void {
  record.request_sent = false;
  record.execution_unknown = false;
  record.usage_known = true;
  record.input_tokens = 0;
  record.output_tokens = 0;
  record.cost_micros = 0;
}

function applyUsage(record: AttemptRecord, usage: TokenUsage | null, target: RouteTarget): void {
  if (!usage) return;
  record.usage_known = true;
  record.input_tokens = usage.input;
  record.output_tokens = usage.output;
  record.cost_micros = usageMicros(usage.input, usage.output, target.inputPrice, target.outputPrice);
}

export interface BufferedAttemptResult {
  record: AttemptRecord;
  completion?: Record<string, unknown>;
  retryAfter: number | null;
}

/** One buffered (non-stream) attempt against a single route target. */
export async function runBufferedAttempt(rt: GatewayRuntime, target: RouteTarget, entry: PriceEntry): Promise<BufferedAttemptResult> {
  const record = newRecord(target, entry.attempt_no, entry.reserved_micros);
  const started = Date.now();
  try {
    if (!target.targetEnabled || !target.providerEnabled) {
      record.error_code = 'target_disabled';
      markNotExecuted(record);
      return { record, retryAfter: null };
    }
    const token = providerToken(target.encryptedSecret, rt.encryptionKey);
    const payload = upstreamPayload(target, rt.body, rt.maxOutputTokens);
    const handle = await startUpstream({
      endpoint: chatEndpoint(target.baseUrl), token, body: payload, requestId: rt.requestId,
      accept: 'application/json', allowPrivate: rt.allowPrivate, timings: rt.timings, deadlineAt: rt.deadlineAt,
      onAbortReady: abort => rt.setActiveAbort(abort),
    });
    rt.setActiveAbort(handle.abort);
    record.request_sent = true;
    record.http_status = handle.status;
    if (handle.status < 200 || handle.status >= 300) {
      const info = upstreamHttpError(handle.status);
      record.error_code = info.code;
      record.retry_safe = info.retrySafe;
      record.execution_unknown = handle.status !== 429;
      if (handle.status === 429) markNotExecuted(record);
      const retryAfter = retryAfterSeconds(handle.headers);
      handle.res.destroy();
      handle.cleanup();
      return { record, retryAfter };
    }
    const raw = await readBuffered(handle);
    let parsed: unknown;
    try {
      parsed = JSON.parse(raw.toString('utf8'));
    } catch {
      record.error_code = 'upstream_invalid_response';
      record.execution_unknown = true;
      return { record, retryAfter: null };
    }
    const usage = parseUsage((parsed as Record<string, unknown> | null)?.usage);
    applyUsage(record, usage, target);
    let completion: Record<string, unknown>;
    try {
      completion = normalizeCompletion(parsed, rt.aliasName, rt.requestId, usage);
    } catch {
      record.error_code = 'upstream_invalid_response';
      record.execution_unknown = true;
      return { record, retryAfter: null };
    }
    record.status = 'ok';
    record.error_code = null;
    return { record, completion, retryAfter: null };
  } catch (err) {
    const failure = err instanceof UpstreamFailure
      ? err
      : new UpstreamFailure('upstream_error', 502, '上游调用失败。', true, true, false);
    record.error_code = failure.code;
    record.request_sent = failure.requestSent;
    record.execution_unknown = failure.executionUnknown;
    record.retry_safe = failure.retrySafe;
    record.http_status = record.http_status ?? null;
    if (!failure.requestSent) markNotExecuted(record);
    return { record, retryAfter: null };
  } finally {
    record.latency_ms = Math.max(0, Date.now() - started);
    rt.setActiveAbort(null);
  }
}

export interface StreamSink {
  readonly started: boolean;
  start(): void;
  write(text: string): Promise<void>;
  end(): Promise<void>;
}

const SSE_HEADERS = {
  'content-type': 'text/event-stream; charset=utf-8',
  'cache-control': 'no-store',
  connection: 'keep-alive',
  'x-accel-buffering': 'no',
  'x-content-type-options': 'nosniff',
};

/** Lazily writes SSE headers so a failure before the first event can still return a JSON error. */
export class ReplyStreamSink implements StreamSink {
  private startedAt = false;
  private closed = false;
  constructor(private readonly reply: FastifyReply, private readonly requestId: string,
    private readonly deadlineAt: number = Number.POSITIVE_INFINITY) {
    reply.raw.on('close', () => { this.closed = true; });
  }
  get started(): boolean { return this.startedAt; }
  get clientClosed(): boolean { return this.closed; }
  start(): void {
    if (this.startedAt) return;
    this.startedAt = true;
    this.reply.hijack();
    this.reply.raw.writeHead(200, { ...SSE_HEADERS, 'x-request-id': this.requestId });
  }
  async write(text: string): Promise<void> {
    this.start();
    const raw = this.reply.raw;
    if (this.closed || raw.destroyed || raw.writableEnded) {
      throw new GatewayError(499, 'client_closed', '客户端已断开连接。');
    }
    if (!raw.write(text, 'utf8')) await this.waitDrain();
  }
  async end(): Promise<void> {
    const raw = this.reply.raw;
    if (!this.startedAt || raw.writableEnded || raw.destroyed) return;
    await new Promise<void>(resolve => {
      let timer: NodeJS.Timeout | undefined;
      const done = () => { if (timer) clearTimeout(timer); raw.off('finish', done); raw.off('close', done); resolve(); };
      timer = setTimeout(done, 5_000);
      raw.once('finish', done);
      raw.once('close', done);
      raw.end();
    });
  }
  private waitDrain(): Promise<void> {
    const raw = this.reply.raw;
    return new Promise((resolve, reject) => {
      const remaining = Math.max(1, this.deadlineAt - Date.now());
      const timer = setTimeout(() => {
        cleanup();
        try { raw.destroy(); } catch { /* socket already gone */ }
        reject(new GatewayError(504, 'client_write_timeout', '向客户端写入响应超时。'));
      }, Math.min(remaining, 120_000));
      const onDrain = () => { cleanup(); resolve(); };
      const onClose = () => { cleanup(); reject(new GatewayError(499, 'client_closed', '客户端已断开连接。')); };
      const cleanup = () => { clearTimeout(timer); raw.off('drain', onDrain); raw.off('close', onClose); };
      raw.once('drain', onDrain);
      raw.once('close', onClose);
    });
  }
}

export interface StreamAttemptResult {
  record: AttemptRecord;
  wroteAny: boolean;
  retryAfter: number | null;
}

/** One SSE attempt: rewrites model/usage, forwards frames, never falls back after the first byte reaches the client. */
export async function runStreamAttempt(rt: GatewayRuntime, target: RouteTarget, entry: PriceEntry, sink: StreamSink): Promise<StreamAttemptResult> {
  const record = newRecord(target, entry.attempt_no, entry.reserved_micros);
  const started = Date.now();
  let wroteAny = false;
  let sawDone = false;
  let handle: UpstreamHandle | undefined;
  try {
    if (!target.targetEnabled || !target.providerEnabled) {
      record.error_code = 'target_disabled';
      markNotExecuted(record);
      return { record, wroteAny, retryAfter: null };
    }
    const token = providerToken(target.encryptedSecret, rt.encryptionKey);
    const payload = upstreamPayload(target, rt.body, rt.maxOutputTokens);
    handle = await startUpstream({
      endpoint: chatEndpoint(target.baseUrl), token, body: payload, requestId: rt.requestId,
      accept: 'text/event-stream', allowPrivate: rt.allowPrivate, timings: rt.timings, deadlineAt: rt.deadlineAt,
      onAbortReady: abort => rt.setActiveAbort(abort),
    });
    rt.setActiveAbort(handle.abort);
    record.request_sent = true;
    record.http_status = handle.status;
    if (handle.status < 200 || handle.status >= 300) {
      const info = upstreamHttpError(handle.status);
      record.error_code = info.code;
      record.retry_safe = info.retrySafe;
      record.execution_unknown = handle.status !== 429;
      if (handle.status === 429) markNotExecuted(record);
      const retryAfter = retryAfterSeconds(handle.headers);
      handle.res.destroy();
      handle.cleanup();
      return { record, wroteAny, retryAfter };
    }
    if (!String(handle.headers['content-type'] ?? '').includes('text/event-stream')) {
      handle.abort('upstream_protocol_error');
      throw new UpstreamFailure('upstream_protocol_error', 502, '上游未返回流式响应。', true, true, false);
    }
    const decoder = new TextDecoder();
    const parser = new SseParser();
    const applyEvent = async (event: SseEvent): Promise<boolean> => {
      if (event.data === '[DONE]') {
        await sink.write('data: [DONE]\n\n');
        wroteAny = true;
        return true;
      }
      let parsed: unknown = null;
      try { parsed = JSON.parse(event.data); } catch { parsed = null; }
      if (parsed && typeof parsed === 'object' && !Array.isArray(parsed)) {
        const obj = parsed as Record<string, unknown>;
        if ('model' in obj) obj.model = rt.aliasName;
        applyUsage(record, parseUsage(obj.usage), target);
        if (obj.error) throw new UpstreamFailure('upstream_stream_error', 502, '上游在流式响应中返回错误。', true, true, false);
        await sink.write(formatSseEvent({ event: event.event, data: JSON.stringify(obj) }));
      } else {
        await sink.write(formatSseEvent(event));
      }
      wroteAny = true;
      return false;
    };
    for await (const chunk of handle.res) {
      handle.touch();
      const buf = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk as string);
      for (const event of parser.push(decoder.decode(buf, { stream: true }))) {
        if (await applyEvent(event)) { sawDone = true; break; }
      }
      if (sawDone) break;
    }
    if (!sawDone) {
      for (const event of parser.flush()) {
        if (await applyEvent(event)) { sawDone = true; break; }
      }
    }
    if (!sawDone) {
      // Clean EOF without [DONE] is a truncated stream, even if content was forwarded: never synthesize DONE.
      throw new UpstreamFailure('upstream_incomplete_stream', 502, '上游流式响应未正常结束。', true, true, false);
    }
    handle.res.destroy();
    record.status = 'ok';
    record.error_code = null;
    return { record, wroteAny, retryAfter: null };
  } catch (err) {
    const abortCode = handle?.abortCode() ?? null;
    if (err instanceof GatewayError && err.code === 'client_closed' || abortCode === 'client_cancelled') {
      record.error_code = 'client_cancelled';
      record.request_sent = true;
      record.execution_unknown = true;
      return { record, wroteAny, retryAfter: null };
    }
    if (err instanceof GatewayError && err.code === 'client_write_timeout') {
      record.error_code = 'client_write_timeout';
      record.request_sent = true;
      record.execution_unknown = true;
      record.retry_safe = false;
      return { record, wroteAny, retryAfter: null };
    }
    const failure = err instanceof UpstreamFailure
      ? err
      : new UpstreamFailure('upstream_error', 502, '上游调用失败。', true, true, false);
    record.error_code = failure.code;
    record.request_sent = failure.requestSent;
    record.execution_unknown = failure.executionUnknown;
    record.retry_safe = failure.retrySafe;
    if (!failure.requestSent) markNotExecuted(record);
    return { record, wroteAny, retryAfter: retryAfterSeconds(handle?.headers ?? {}) };
  } finally {
    record.latency_ms = Math.max(0, Date.now() - started);
    rt.setActiveAbort(null);
    handle?.cleanup();
  }
}

const requestIds = new WeakMap<object, string>();

function requestIdFor(req: FastifyRequest): string {
  const existing = requestIds.get(req);
  if (existing) return existing;
  const created = randomUUID();
  requestIds.set(req, created);
  return created;
}

function toGatewayError(err: unknown): GatewayError {
  if (err instanceof GatewayError) return err;
  const raw = err as { statusCode?: unknown; code?: unknown; message?: unknown } | null;
  const status = Number(raw?.statusCode);
  if (Number.isInteger(status) && status >= 400 && status <= 599 && typeof raw?.code === 'string' && raw.code) {
    const message = typeof raw.message === 'string' && raw.message ? raw.message : '请求失败。';
    return new GatewayError(status, raw.code, message);
  }
  return new GatewayError(503, 'service_unavailable', '服务暂时不可用，请稍后重试。');
}

function sendError(app: FastifyInstance, reply: FastifyReply, requestId: string, err: unknown, retryAfter: number | null = null): void {
  const failure = toGatewayError(err);
  if (failure.statusCode >= 500) {
    app.log.error({ request_id: requestId, code: failure.code }, 'gateway request failed');
  }
  if (reply.raw.headersSent) {
    if (!reply.raw.writableEnded) reply.raw.end();
    return;
  }
  if (retryAfter) reply.header('retry-after', String(retryAfter));
  reply.code(failure.statusCode).header('x-request-id', requestId).type('application/json; charset=utf-8')
    .send({ error: { code: failure.code, message: failure.message, type: 'gateway_error' }, request_id: requestId });
}

export interface ModelListEntry { id: string; object: 'model'; created: number; owned_by: string }

function modelList(rows: Array<Record<string, unknown>>, key: KeyContext): ModelListEntry[] {
  const created = Math.floor(Date.now() / 1000);
  const out: ModelListEntry[] = [];
  for (const row of rows) {
    const id = String(row.id);
    const name = String(row.name);
    if (!keyCanUseModel(key, id, name)) continue;
    const at = row.created_at ? Math.floor(new Date(row.created_at as string).getTime() / 1000) : created;
    out.push({ id: name, object: 'model', created: Number.isFinite(at) ? at : created, owned_by: 'ai-gateway' });
  }
  return out;
}

async function handleModels(app: FastifyInstance, deps: GatewayDeps, req: FastifyRequest, reply: FastifyReply): Promise<void> {
  const requestId = requestIdFor(req);
  try {
    const key = await deps.dataPlane.authenticateKey(req);
    const rows = await deps.dataPlane.models(key);
    const data = modelList(rows, key);
    reply.header('x-request-id', requestId).send({ object: 'list', data, items: data });
  } catch (err) {
    sendError(app, reply, requestId, err);
  }
}

export async function registerGateway(app: FastifyInstance, deps: GatewayDeps): Promise<void> {
  const timings: Timings = { ...DEFAULT_TIMINGS, ...deps.timings };
  const assignRequestId = async (req: FastifyRequest, reply: FastifyReply): Promise<void> => {
    reply.header('x-request-id', requestIdFor(req));
  };
  app.get('/v1/models', { onRequest: assignRequestId }, async (req, reply) => {
    await handleModels(app, deps, req, reply);
  });
  app.post('/v1/chat/completions', { onRequest: assignRequestId, bodyLimit: LIMITS.maxBodyBytes }, async (req, reply) => {
    await handleChat(app, deps, timings, req, reply, requestIdFor(req));
  });
}

export interface PreparedRequest {
  key: KeyContext;
  body: ChatRequest;
  route: RouteSnapshot;
  targets: RouteTarget[];
  maxOutputTokens: number;
  price: PriceSnapshot;
  routeVersion: Record<string, unknown>;
}

/** Freeze prices/limits at request start; the reservation covers every permitted attempt. */
export function prepareRequest(key: KeyContext, body: ChatRequest, route: RouteSnapshot): PreparedRequest {
  if (body.max_tokens !== undefined && key.maxOutputTokens !== null && body.max_tokens > key.maxOutputTokens) {
    throw new GatewayError(400, 'max_tokens_exceeds_project_limit', `max_tokens 不能超过项目上限 ${key.maxOutputTokens}。`);
  }
  const maxOutputTokens = body.max_tokens ?? key.maxOutputTokens ?? LIMITS.defaultMaxOutputTokens;
  if (!Number.isSafeInteger(maxOutputTokens) || maxOutputTokens < 1 || maxOutputTokens > LIMITS.maxMaxOutputTokens) {
    throw new GatewayError(500, 'invalid_configuration', '输出上限配置无效。');
  }
  const targets = route.targets.filter(t => t.targetEnabled && t.providerEnabled).slice(0, LIMITS.maxAttempts);
  if (targets.length === 0) throw new GatewayError(503, 'model_unavailable', '模型当前没有可用目标。');
  const estimatedInputTokens = estimateInputTokens(body.messages);
  const entries: PriceEntry[] = targets.map((target, index) => ({
    attempt_no: index + 1,
    target_id: target.targetId,
    upstream_model: target.upstreamModel,
    input_micros_per_million: target.inputPrice,
    output_micros_per_million: target.outputPrice,
    max_output_tokens: maxOutputTokens,
    estimated_input_tokens: estimatedInputTokens,
    reserved_micros: estimateAttemptMicros(estimatedInputTokens, maxOutputTokens, target.inputPrice, target.outputPrice),
  }));
  let total = 0;
  for (const entry of entries) {
    total += entry.reserved_micros;
    if (!Number.isSafeInteger(total)) throw new GatewayError(500, 'price_overflow', '价格计算超出安全范围。');
  }
  const price: PriceSnapshot = {
    version: 1, currency: 'USD', unit: 'tokens', period: utcPeriod(), max_attempts: entries.length,
    estimated_input_tokens: estimatedInputTokens, max_output_tokens: maxOutputTokens,
    total_reserved_micros: total, attempts: entries,
  };
  return {
    key, body, route, targets, maxOutputTokens, price,
    routeVersion: {
      alias_id: route.aliasId, alias_name: route.aliasName, version_id: route.routeVersionId,
      runtime_revision:key.runtimeRevision,
      runtime_key_hash:key.runtimeKeyHash,
      runtime_expires_at:key.runtimeExpiresAt,
      version: route.version, targets: entries.map(e => ({ target_id: e.target_id, upstream_model: e.upstream_model })),
    },
  };
}

export interface Admission { requestId: string; period: string }

export async function admitRequest(billing: GatewayBilling, requestId: string, prepared: PreparedRequest): Promise<Admission> {
  const period = prepared.price.period;
  await billing.admit({
    requestId, projectId: prepared.key.projectId, keyId: prepared.key.keyId, period,
    reserveMicros: prepared.price.total_reserved_micros,
    rpm: prepared.key.projectRpm, keyRpm: prepared.key.keyRpm, concurrency: prepared.key.concurrency,
    budgetMicros: prepared.key.budgetMicros, routeVersion: prepared.routeVersion, priceSnapshot: prepared.price,
  });
  return { requestId, period };
}

const ERROR_TABLE: Record<string, [number, string]> = {
  upstream_connect_failed: [502, '无法连接上游服务。'],
  upstream_tls_error: [502, '上游 TLS 校验失败。'],
  upstream_connect_timeout: [504, '连接上游超时。'],
  upstream_first_byte_timeout: [504, '等待上游响应超时。'],
  upstream_idle_timeout: [504, '上游响应中断超时。'],
  upstream_timeout: [504, '上游请求超时。'],
  request_timeout: [504, '请求超过整体时间上限。'],
  upstream_invalid_request: [400, '上游拒绝了请求参数。'],
  upstream_auth_failed: [502, '上游鉴权失败。'],
  upstream_model_unavailable: [502, '上游模型不可用。'],
  upstream_rate_limited: [429, '上游限流，请稍后重试。'],
  upstream_unavailable: [503, '上游服务暂时不可用。'],
  upstream_error: [502, '上游服务返回错误。'],
  upstream_redirect: [502, '上游返回了重定向。'],
  upstream_protocol_error: [502, '上游响应格式不符合协议。'],
  upstream_empty_stream: [502, '上游返回了空的流式响应。'],
  upstream_stream_error: [502, '上游在流式响应中返回错误。'],
  upstream_invalid_response: [502, '上游返回了无法识别的响应。'],
  upstream_response_too_large: [502, '上游响应体过大。'],
  upstream_connection_error: [502, '上游连接中断。'],
  upstream_address_rejected: [502, '上游地址未通过安全校验。'],
  upstream_credentials_invalid: [502, '上游凭据不可用。'],
  invalid_provider_url: [502, '上游服务地址无效。'],
  target_disabled: [503, '模型当前没有可用目标。'],
  lease_lost: [503, '请求租约失效。'],
  client_cancelled: [499, '客户端已取消请求。'],
  client_write_timeout: [504, '向客户端写入响应超时。'],
  upstream_incomplete_stream: [502, '上游流式响应未正常结束。'],
};

export function errorFromRecord(record: AttemptRecord): GatewayError {
  const code = record.error_code ?? 'upstream_error';
  const [status, message] = ERROR_TABLE[code] ?? [502, '上游调用失败。'];
  return new GatewayError(status, code, message);
}

async function bufferedFlow(app: FastifyInstance, deps: GatewayDeps, runtime: GatewayRuntime,
  prepared: PreparedRequest, attempts: AttemptRecord[], reply: FastifyReply, requestId: string,
  onAttempt: AttemptCallback): Promise<SettlementStatus> {
  let lastError: GatewayError | null = null;
  let retryAfter: number | null = null;
  let timedOut = false;
  for (let index = 0; index < prepared.targets.length; index += 1) {
    if (runtime.clientAborted()) {
      lastError = new GatewayError(499, 'client_cancelled', '客户端已取消请求。');
      break;
    }
    if (Date.now() >= runtime.deadlineAt) {
      timedOut = true;
      lastError = new GatewayError(504, 'request_timeout', '请求超过整体时间上限。');
      break;
    }
    const result = await runBufferedAttempt(runtime, prepared.targets[index], prepared.price.attempts[index]);
    attempts.push(result.record);
    await onAttempt(result.record);
    retryAfter = result.retryAfter ?? retryAfter;
    if (result.record.status === 'ok' && result.completion) {
      reply.code(200).header('x-request-id', requestId).send(result.completion);
      return 'completed';
    }
    lastError = errorFromRecord(result.record);
    timedOut = timedOut || ['request_timeout', 'upstream_connect_timeout', 'upstream_first_byte_timeout'].includes(result.record.error_code ?? '');
    if (!canFallback(result.record, index, prepared.targets.length)) break;
  }
  if (runtime.clientAborted()) {
    if (!reply.raw.destroyed) sendError(app, reply, requestId, new GatewayError(499, 'client_cancelled', '客户端已取消请求。'));
    return 'cancelled';
  }
  sendError(app, reply, requestId, lastError ?? new GatewayError(502, 'upstream_error', '上游调用失败。'), retryAfter);
  return timedOut ? 'timeout' : 'failed';
}

async function streamFlow(app: FastifyInstance, deps: GatewayDeps, runtime: GatewayRuntime,
  prepared: PreparedRequest, attempts: AttemptRecord[], reply: FastifyReply, requestId: string,
  onAttempt: AttemptCallback): Promise<SettlementStatus> {
  const sink = new ReplyStreamSink(reply, requestId, runtime.deadlineAt);
  let lastError: GatewayError | null = null;
  let retryAfter: number | null = null;
  let timedOut = false;
  for (let index = 0; index < prepared.targets.length; index += 1) {
    if (runtime.clientAborted()) return 'cancelled';
    if (Date.now() >= runtime.deadlineAt) {
      timedOut = true;
      lastError = new GatewayError(504, 'request_timeout', '请求超过整体时间上限。');
      break;
    }
    const result = await runStreamAttempt(runtime, prepared.targets[index], prepared.price.attempts[index], sink);
    attempts.push(result.record);
    await onAttempt(result.record);
    retryAfter = result.retryAfter ?? retryAfter;
    if (result.record.status === 'ok') {
      await sink.end();
      return 'completed';
    }
    if (sink.started || result.wroteAny) {
      const failure = errorFromRecord(result.record);
      try {
        await sink.write(formatSseEvent({ event: 'error', data: JSON.stringify({
          error: { code: failure.code, message: failure.message, type: 'gateway_error' }, request_id: requestId,
        }) }));
        await sink.end();
      } catch {
        // client already gone
      }
      return result.record.error_code === 'client_cancelled' ? 'cancelled' : 'partial';
    }
    lastError = errorFromRecord(result.record);
    timedOut = timedOut || ['request_timeout', 'upstream_connect_timeout', 'upstream_first_byte_timeout'].includes(result.record.error_code ?? '');
    if (!canFallback(result.record, index, prepared.targets.length) || runtime.clientAborted()) break;
  }
  if (runtime.clientAborted()) {
    if (!sink.started && !reply.raw.headersSent && !reply.raw.destroyed) {
      sendError(app, reply, requestId, new GatewayError(499, 'client_cancelled', '客户端已取消请求。'));
    }
    return 'cancelled';
  }
  if (!sink.started && !reply.raw.headersSent) {
    sendError(app, reply, requestId, lastError ?? new GatewayError(502, 'upstream_error', '上游调用失败。'), retryAfter);
  }
  return timedOut ? 'timeout' : 'failed';
}

export const REQUEST_META_SQL = `/*gw:request-meta*/
UPDATE requests SET alias_name = $2, error_code = $3, updated_at = now() WHERE request_id = $1`;

export const ATTEMPT_UPSERT_SQL = `/*gw:attempt*/
INSERT INTO attempts (id, request_id, attempt_no, target_id, status, error_code, started_at, finished_at, input_tokens, output_tokens, cost_micros)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
ON CONFLICT (request_id, attempt_no) DO UPDATE SET
  status = EXCLUDED.status, error_code = EXCLUDED.error_code, finished_at = EXCLUDED.finished_at,
  input_tokens = EXCLUDED.input_tokens, output_tokens = EXCLUDED.output_tokens, cost_micros = EXCLUDED.cost_micros`;

const REQUEST_ATTEMPTS_SQL = `/*gw:request-attempts*/
WITH meta AS (
  UPDATE requests SET alias_name=$2,error_code=$3,updated_at=now() WHERE request_id=$1 RETURNING request_id
)
INSERT INTO attempts(id,request_id,attempt_no,target_id,status,error_code,started_at,finished_at,input_tokens,output_tokens,cost_micros)
SELECT a.id,meta.request_id,a.attempt_no,a.target_id,a.status,a.error_code,a.started_at,a.finished_at,a.input_tokens,a.output_tokens,a.cost_micros
FROM meta CROSS JOIN jsonb_to_recordset($4::jsonb) AS a(
  id uuid,attempt_no integer,target_id uuid,status text,error_code text,started_at timestamptz,finished_at timestamptz,input_tokens bigint,output_tokens bigint,cost_micros bigint)
ON CONFLICT(request_id,attempt_no) DO UPDATE SET status=excluded.status,error_code=excluded.error_code,
  finished_at=excluded.finished_at,input_tokens=excluded.input_tokens,output_tokens=excluded.output_tokens,cost_micros=excluded.cost_micros`;

/** Persist request outcome metadata and per-attempt rows. Bodies and credentials never reach these tables. */
export async function persistRequestMetadata(pool: Pool, requestId: string, aliasName: string | null,
  status: SettlementStatus, attempts: AttemptRecord[], attemptsToPersist: AttemptRecord[] = attempts): Promise<void> {
  const failure = [...attempts].reverse().find(a => a.status !== 'ok');
  const errorCode = status === 'completed' ? null
    : (failure?.error_code ?? (status === 'cancelled' ? 'client_cancelled' : null));
  if(!attemptsToPersist.length){await pool.query(REQUEST_META_SQL,[requestId,aliasName,errorCode]);return;}
  const finishedAt=Date.now();
  const rows=attemptsToPersist.map(a=>({id:randomUUID(),attempt_no:a.attempt_no,target_id:a.target_id||null,status:a.status,error_code:a.error_code,
    started_at:new Date(finishedAt-Math.max(0,a.latency_ms)).toISOString(),finished_at:new Date(finishedAt).toISOString(),
    input_tokens:a.input_tokens,output_tokens:a.output_tokens,cost_micros:a.cost_micros}));
  await pool.query(REQUEST_ATTEMPTS_SQL,[requestId,aliasName,errorCode,JSON.stringify(rows)]);
}

export async function persistAttemptRows(pool: Pool, requestId: string, attempts: AttemptRecord[]): Promise<void> {
  const finishedAt = Date.now();
  for (const attempt of attempts) {
    const finished = new Date(finishedAt);
    const started = new Date(finishedAt - Math.max(0, attempt.latency_ms));
    await pool.query(ATTEMPT_UPSERT_SQL, [randomUUID(), requestId, attempt.attempt_no,
      attempt.target_id || null, attempt.status, attempt.error_code, started, finished,
      attempt.input_tokens, attempt.output_tokens, attempt.cost_micros]);
  }
}

export type AttemptCallback = (record: AttemptRecord) => Promise<void>;

function retryAfterOf(err: unknown): number | null {
  const details = (err as { details?: { retryAfterMs?: unknown } } | null)?.details;
  const ms = Number(details?.retryAfterMs);
  return Number.isFinite(ms) && ms > 0 ? Math.max(1, Math.ceil(ms / 1000)) : null;
}

export async function handleChat(app: FastifyInstance, deps: GatewayDeps, timings: Timings,
  req: FastifyRequest, reply: FastifyReply, requestId: string): Promise<void> {
  const deadlineAt = Date.now() + timings.totalMs;
  const attempts: AttemptRecord[] = [];
  const persistedAttempts = new Set<number>();
  let activeAbort: ((code: string) => void) | null = null;
  let clientClosed = false;
  reply.raw.on('close', () => { clientClosed = true; activeAbort?.('client_cancelled'); });
  let prepared: PreparedRequest | null = null;
  let admitted = false;
  let dispatched = false;
  let heartbeat: NodeJS.Timeout | undefined;
  let finalStatus: SettlementStatus = 'failed';
  const finish = async (status: SettlementStatus): Promise<void> => {
    if (heartbeat) { clearInterval(heartbeat); heartbeat = undefined; }
    if (!admitted) return;
    if (!dispatched) {
      await deps.billing.cancelBeforeDispatch(requestId).catch(() => {
        app.log.error({ request_id: requestId, code: 'cancel_before_dispatch_failed' }, 'billing cancel failed');
      });
      return;
    }
    if (prepared) {
      await deps.dataPlane.persistMetadata(requestId,prepared.route.aliasName,status,attempts,
        attempts.filter(a=>!persistedAttempts.has(a.attempt_no))).catch(() => {
        app.log.error({ request_id: requestId, code: 'request_metadata_failed' }, 'request metadata persistence failed');
      });
    }
    const totals = aggregateSettlement(attempts);
    try {
      await deps.billing.settle(requestId, {
        actualMicros: totals.actualMicros, inputTokens: totals.inputTokens, outputTokens: totals.outputTokens,
        status, attempts: publicAttempts(attempts),
      });
    } catch {
      app.log.error({ request_id: requestId, code: 'settlement_failed' }, 'billing settlement failed; reservation retained for reconciliation');
    }
  };
  try {
    const key = await deps.dataPlane.authenticateKey(req);
    const body = validateChatBody(req.body);
    if (!keyCanUseModel(key, '', body.model)) throw new GatewayError(403, 'model_not_permitted', 'API Key 无权使用该模型。');
    const route = await deps.dataPlane.loadRoute(key,body.model);
    if (!route) throw new GatewayError(404, 'model_not_found', '模型不存在或未发布。');
    if (!keyCanUseModel(key, route.aliasId, route.aliasName)) throw new GatewayError(403, 'model_not_permitted', 'API Key 无权使用该模型。');
    prepared = prepareRequest(key, body, route);
    await admitRequest(deps.billing, requestId, prepared);
    admitted = true;
    await deps.billing.dispatch(requestId);
    dispatched = true;
  } catch (err) {
    await finish('failed');
    if (!reply.raw.headersSent && !reply.raw.destroyed) sendError(app, reply, requestId, err, retryAfterOf(err));
    return;
  }
  heartbeat = setInterval(() => {
    void deps.billing.heartbeat(requestId).catch(() => {
      app.log.warn({ request_id: requestId, code: 'lease_heartbeat_failed' }, 'billing lease heartbeat failed');
      activeAbort?.('lease_lost');
    });
  }, timings.heartbeatMs);
  heartbeat.unref();
  const runtime: GatewayRuntime = {
    requestId, aliasName: prepared.route.aliasName, body: prepared.body, maxOutputTokens: prepared.maxOutputTokens,
    timings, allowPrivate: deps.allowPrivateUpstream, encryptionKey: deps.encryptionKey, deadlineAt,
    setActiveAbort: abort => { activeAbort = abort; },
    clientAborted: () => clientClosed,
  };
  const aliasName = prepared.route.aliasName;
  const onAttempt: AttemptCallback = async record => {
    try {
      await deps.dataPlane.persistMetadata(requestId,aliasName,record.status==='ok'?'completed':'failed',[record]);
      persistedAttempts.add(record.attempt_no);
    } catch {
      app.log.error({ request_id: requestId, code: 'attempt_metadata_failed' }, 'attempt metadata persistence failed');
    }
  };
  try {
    finalStatus = prepared.body.stream
      ? await streamFlow(app, deps, runtime, prepared, attempts, reply, requestId, onAttempt)
      : await bufferedFlow(app, deps, runtime, prepared, attempts, reply, requestId, onAttempt);
  } catch (err) {
    finalStatus = 'failed';
    if (!reply.raw.headersSent && !reply.raw.destroyed) sendError(app, reply, requestId, err);
  } finally {
    await finish(finalStatus);
  }
}
