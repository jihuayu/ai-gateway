import { randomUUID } from 'node:crypto';
import { lookup } from 'node:dns/promises';
import { request as httpRequest } from 'node:http';
import { request as httpsRequest } from 'node:https';
import { isIP } from 'node:net';
import type { FastifyInstance, FastifyRequest } from 'fastify';
import type { Pool } from 'pg';
import { z } from 'zod';
import {
  authenticateSession, HttpError, type ControlDeps, type SessionInfo,
} from './control.js';
import { decrypt, encrypt, validateProviderUrl, ProviderUrlError, isBlockedAddress } from './security.js';

const uuid = z.uuid();
const price = z.number().int().min(0).max(Number.MAX_SAFE_INTEGER);
const capabilities = z.array(z.string().trim().min(1).max(64)).max(20);

const providerCreate = z.object({
  name: z.string().trim().min(1).max(100),
  base_url: z.string().url().max(2000),
  secret: z.string().min(1).max(8192),
  protocol: z.literal('openai').default('openai'),
  enabled: z.boolean().default(true),
});
const providerUpdate = z.object({
  name: z.string().trim().min(1).max(100).optional(),
  base_url: z.string().url().max(2000).optional(),
  secret: z.string().min(1).max(8192).optional(),
  enabled: z.boolean().optional(),
}).refine(v => Object.keys(v).length > 0, 'No fields to update');
const targetCreate = z.object({
  upstream_model: z.string().trim().min(1).max(200),
  input_price: price,
  output_price: price,
  capabilities: capabilities.default([]),
  enabled: z.boolean().default(true),
});
const targetUpdate = z.object({
  upstream_model: z.string().trim().min(1).max(200).optional(),
  input_price: price.optional(),
  output_price: price.optional(),
  capabilities: capabilities.optional(),
  enabled: z.boolean().optional(),
}).refine(v => Object.keys(v).length > 0, 'No fields to update');

type ProviderRow = {
  id: string; workspace_id: string; name: string; protocol: string; base_url: string;
  encrypted_secret: string; enabled: boolean; last_test_status: string | null;
  last_test_at: Date | null; last_test_error: string | null; created_at: Date; updated_at: Date;
};

function parse<T>(schema: z.ZodType<T>, value: unknown): T {
  const result = schema.safeParse(value ?? {});
  if (!result.success) {
    throw new HttpError(400, 'validation_error', result.error.issues.map(i => `${i.path.join('.')}: ${i.message}`).join('; '));
  }
  return result.data;
}

function idOf(req: FastifyRequest): string {
  const id = (req.params as { id?: string }).id ?? '';
  if (!uuid.safeParse(id).success) throw new HttpError(404, 'not_found', 'Resource not found');
  return id;
}

async function requireWorkspaceRole(req: FastifyRequest, pool: Pool, roles: string[]): Promise<SessionInfo> {
  const auth = await authenticateSession(req, pool);
  if (!roles.includes(auth.role)) throw new HttpError(403, 'forbidden', 'Insufficient role');
  return auth;
}

async function audit(pool: Pool, auth: SessionInfo, action: string, entityType: string, entityId: string, changes: unknown): Promise<void> {
  await pool.query(
    `INSERT INTO audit_events (id, workspace_id, actor_user_id, action, entity_type, entity_id, changes)
     VALUES ($1,$2,$3,$4,$5,$6,$7::jsonb)`,
    [randomUUID(), auth.workspaceId, auth.userId, action, entityType, entityId, JSON.stringify(changes ?? {})],
  );
}

function publicProvider(row: ProviderRow) {
  return {
    id: row.id, name: row.name, protocol: row.protocol, base_url: row.base_url,
    enabled: row.enabled, last_test_status: row.last_test_status, last_test_at: row.last_test_at,
    last_test_error: row.last_test_error, created_at: row.created_at, updated_at: row.updated_at,
  };
}

async function getProvider(pool: Pool, auth: SessionInfo, id: string): Promise<ProviderRow> {
  const { rows } = await pool.query('SELECT * FROM providers WHERE id = $1 AND workspace_id = $2', [id, auth.workspaceId]);
  if (!rows[0]) throw new HttpError(404, 'provider_not_found', 'Provider not found');
  return rows[0] as ProviderRow;
}

async function checkUrl(url: string, allowPrivate: boolean): Promise<void> {
  try {
    await validateProviderUrl(url, allowPrivate);
  } catch (err) {
    if (err instanceof ProviderUrlError) throw new HttpError(400, err.code, err.message);
    throw err;
  }
}

type TestStatus = 'ok' | 'auth_failed' | 'endpoint_not_found' | 'rate_limited' | 'redirect_rejected' | 'upstream_error' | 'timeout' | 'network_error' | 'failed';

/** Single request to a validated, DNS-pinned address. Redirects are never followed and bodies are discarded. */
async function testUpstream(baseUrl: string, secret: string, allowPrivate: boolean): Promise<{ status: TestStatus; latencyMs: number }> {
  await checkUrl(baseUrl, allowPrivate);
  const base = new URL(baseUrl);
  const endpoint = new URL('models', base.href.endsWith('/') ? base.href : `${base.href}/`);
  const host = endpoint.hostname.replace(/^\[|\]$/g, '');
  const family = isIP(host);
  let address = host;
  let pinnedFamily = family;
  if (!family) {
    const answers = await lookup(host, { all: true, verbatim: true });
    if (!allowPrivate && answers.some(a => isBlockedAddress(a.address))) throw new HttpError(400, 'invalid_provider_url', 'Provider host resolves to a blocked address');
    if (!answers[0]) throw new HttpError(400, 'invalid_provider_url', 'Provider host could not be resolved');
    address = answers[0].address;
    pinnedFamily = answers[0].family;
  }
  const started = Date.now();
  const requestImpl = endpoint.protocol === 'https:' ? httpsRequest : httpRequest;
  return await new Promise((resolve, reject) => {
    const req = requestImpl({
      protocol: endpoint.protocol,
      hostname: host,
      port: endpoint.port || (endpoint.protocol === 'https:' ? 443 : 80),
      path: `${endpoint.pathname}${endpoint.search}`,
      method: 'GET',
      headers: { authorization: `Bearer ${secret}`, accept: 'application/json', 'user-agent': 'ai-gateway-connection-test/1' },
      timeout: 8000,
      servername: endpoint.protocol === 'https:' && !isIP(host) ? host : undefined,
      lookup: ((_hostname: string, _options: unknown, cb: (err: Error | null, addr: string, fam: number) => void) => {
        cb(null, address, pinnedFamily);
      }) as never,
    }, res => {
      const code = res.statusCode ?? 0;
      res.resume();
      const latencyMs = Date.now() - started;
      let status: TestStatus = 'failed';
      if (code >= 200 && code < 300) status = 'ok';
      else if (code === 401 || code === 403) status = 'auth_failed';
      else if (code === 404) status = 'endpoint_not_found';
      else if (code === 429) status = 'rate_limited';
      else if (code >= 300 && code < 400) status = 'redirect_rejected';
      else if (code >= 500) status = 'upstream_error';
      resolve({ status, latencyMs });
    });
    req.setTimeout(8000, () => req.destroy(new Error('timeout')));
    req.on('error', err => {
      const code = (err as NodeJS.ErrnoException).code;
      if (code === 'ETIMEDOUT' || err.message === 'timeout') resolve({ status: 'timeout', latencyMs: Date.now() - started });
      else if (code === 'ENOTFOUND' || code === 'ECONNREFUSED' || code === 'ECONNRESET' || code?.startsWith('ERR_TLS')) {
        resolve({ status: 'network_error', latencyMs: Date.now() - started });
      } else reject(err);
    });
    req.end();
  });
}

export async function registerProviders(app: FastifyInstance, deps: ControlDeps): Promise<void> {
  const { pool, encryptionKey, allowPrivateUpstream } = deps;

  app.get('/api/providers', async req => {
    const auth = await requireWorkspaceRole(req, pool, ['admin', 'developer']);
    const { rows } = await pool.query(
      `SELECT id, name, protocol, base_url, enabled, last_test_status, last_test_at, last_test_error, created_at, updated_at
         FROM providers WHERE workspace_id = $1 ORDER BY name`,
      [auth.workspaceId],
    );
    return { items: rows };
  });

  app.post('/api/providers', async req => {
    const auth = await requireWorkspaceRole(req, pool, ['admin']);
    const body = parse(providerCreate, req.body);
    await checkUrl(body.base_url, allowPrivateUpstream);
    const id = randomUUID();
    try {
      await pool.query(
        `INSERT INTO providers (id, workspace_id, name, protocol, base_url, encrypted_secret, enabled)
         VALUES ($1,$2,$3,$4,$5,$6,$7)`,
        [id, auth.workspaceId, body.name, body.protocol, body.base_url, encrypt(body.secret, encryptionKey), body.enabled],
      );
    } catch (err) {
      if ((err as { code?: string }).code === '23505') throw new HttpError(409, 'provider_exists', 'Provider name already exists');
      throw err;
    }
    await audit(pool, auth, 'provider.create', 'provider', id, { name: body.name, base_url: body.base_url, enabled: body.enabled });
    const { rows } = await pool.query('SELECT * FROM providers WHERE id = $1', [id]);
    return publicProvider(rows[0]);
  });

  app.patch('/api/providers/:id', async req => {
    const auth = await requireWorkspaceRole(req, pool, ['admin']);
    const id = idOf(req);
    const body = parse(providerUpdate, req.body);
    const existing = await getProvider(pool, auth, id);
    if (body.base_url) await checkUrl(body.base_url, allowPrivateUpstream);
    const sets: string[] = [];
    const values: unknown[] = [];
    const changes: Record<string, unknown> = {};
    for (const field of ['name', 'base_url', 'enabled'] as const) {
      if (body[field] !== undefined) { values.push(body[field]); sets.push(`${field} = $${values.length}`); changes[field] = body[field]; }
    }
    if (body.secret !== undefined) {
      values.push(encrypt(body.secret, encryptionKey));
      sets.push(`encrypted_secret = $${values.length}`);
      changes.secret = 'replaced';
    }
    values.push(id);
    try {
      await pool.query(`UPDATE providers SET ${sets.join(', ')}, updated_at = now() WHERE id = $${values.length}`, values);
    } catch (err) {
      if ((err as { code?: string }).code === '23505') throw new HttpError(409, 'provider_exists', 'Provider name already exists');
      throw err;
    }
    await audit(pool, auth, 'provider.update', 'provider', id, changes);
    const { rows } = await pool.query('SELECT * FROM providers WHERE id = $1', [id]);
    return publicProvider(rows[0]);
  });

  app.delete('/api/providers/:id', async req => {
    const auth = await requireWorkspaceRole(req, pool, ['admin']);
    const id = idOf(req);
    await getProvider(pool, auth, id);
    const used = await pool.query(
      `SELECT 1 FROM route_versions rv
        JOIN model_targets mt ON mt.provider_id = $1
       WHERE EXISTS (SELECT 1 FROM jsonb_array_elements_text(rv.targets) t WHERE t = mt.id::text)
       LIMIT 1`,
      [id],
    );
    if (used.rowCount) throw new HttpError(409, 'provider_in_use', 'Provider targets are referenced by a route version');
    await pool.query('DELETE FROM providers WHERE id = $1', [id]);
    await audit(pool, auth, 'provider.delete', 'provider', id, {});
    return { ok: true };
  });

  app.post('/api/providers/:id/test', async req => {
    const auth = await requireWorkspaceRole(req, pool, ['admin']);
    const id = idOf(req);
    const provider = await getProvider(pool, auth, id);
    let secret: string;
    try {
      secret = decrypt(provider.encrypted_secret, encryptionKey);
    } catch {
      throw new HttpError(500, 'credential_error', 'Provider credential could not be decrypted');
    }
    let result: { status: TestStatus; latencyMs: number };
    try {
      result = await testUpstream(provider.base_url, secret, allowPrivateUpstream);
    } catch (err) {
      if (err instanceof HttpError) {
        await pool.query("UPDATE providers SET last_test_status = 'invalid_url', last_test_at = now(), last_test_error = $2 WHERE id = $1", [id, err.message]);
        throw err;
      }
      throw err;
    }
    const message: Record<TestStatus, string | null> = {
      ok: null, auth_failed: 'Upstream rejected credentials', endpoint_not_found: 'Models endpoint not found',
      rate_limited: 'Upstream rate limited the test', redirect_rejected: 'Upstream returned a redirect; redirects are not followed',
      upstream_error: 'Upstream returned a server error', timeout: 'Connection timed out',
      network_error: 'Network or TLS connection failed', failed: 'Connection test failed',
    };
    await pool.query(
      'UPDATE providers SET last_test_status = $2, last_test_at = now(), last_test_error = $3 WHERE id = $1',
      [id, result.status, message[result.status]],
    );
    await audit(pool, auth, 'provider.test', 'provider', id, { status: result.status, latencyMs: result.latencyMs });
    return { status: result.status, latency_ms: result.latencyMs, message: message[result.status], tested_at: new Date().toISOString() };
  });

  app.get('/api/providers/:id/targets', async req => {
    const auth = await requireWorkspaceRole(req, pool, ['admin', 'developer']);
    const id = idOf(req);
    await getProvider(pool, auth, id);
    const { rows } = await pool.query(
      `SELECT id, provider_id, upstream_model, input_price, output_price, capabilities, enabled, created_at, updated_at
         FROM model_targets WHERE provider_id = $1 ORDER BY upstream_model`,
      [id],
    );
    return { items: rows };
  });

  app.post('/api/providers/:id/targets', async req => {
    const auth = await requireWorkspaceRole(req, pool, ['admin']);
    const id = idOf(req);
    await getProvider(pool, auth, id);
    const body = parse(targetCreate, req.body);
    const targetId = randomUUID();
    try {
      await pool.query(
        `INSERT INTO model_targets (id, provider_id, upstream_model, input_price, output_price, capabilities, enabled)
         VALUES ($1,$2,$3,$4,$5,$6::jsonb,$7)`,
        [targetId, id, body.upstream_model, body.input_price, body.output_price, JSON.stringify(body.capabilities), body.enabled],
      );
    } catch (err) {
      if ((err as { code?: string }).code === '23505') throw new HttpError(409, 'target_exists', 'Upstream model already configured for this provider');
      throw err;
    }
    await audit(pool, auth, 'target.create', 'model_target', targetId, { provider_id: id, ...body });
    const { rows } = await pool.query('SELECT * FROM model_targets WHERE id = $1', [targetId]);
    return rows[0];
  });

  app.patch('/api/targets/:id', async req => {
    const auth = await requireWorkspaceRole(req, pool, ['admin']);
    const id = idOf(req);
    const body = parse(targetUpdate, req.body);
    const found = await pool.query(
      `SELECT mt.* FROM model_targets mt JOIN providers p ON p.id = mt.provider_id
        WHERE mt.id = $1 AND p.workspace_id = $2`,
      [id, auth.workspaceId],
    );
    if (!found.rows[0]) throw new HttpError(404, 'target_not_found', 'Model target not found');
    const sets: string[] = [];
    const values: unknown[] = [];
    for (const field of ['upstream_model', 'input_price', 'output_price', 'enabled'] as const) {
      if (body[field] !== undefined) { values.push(body[field]); sets.push(`${field} = $${values.length}`); }
    }
    if (body.capabilities !== undefined) {
      values.push(JSON.stringify(body.capabilities));
      sets.push(`capabilities = $${values.length}::jsonb`);
    }
    values.push(id);
    try {
      await pool.query(`UPDATE model_targets SET ${sets.join(', ')}, updated_at = now() WHERE id = $${values.length}`, values);
    } catch (err) {
      if ((err as { code?: string }).code === '23505') throw new HttpError(409, 'target_exists', 'Upstream model already configured for this provider');
      throw err;
    }
    await audit(pool, auth, 'target.update', 'model_target', id, body);
    const { rows } = await pool.query('SELECT * FROM model_targets WHERE id = $1', [id]);
    return rows[0];
  });
}
