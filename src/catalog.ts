import { randomBytes, randomUUID } from 'node:crypto';
import type { FastifyInstance, FastifyRequest } from 'fastify';
import type { Pool, PoolClient } from 'pg';
import { z } from 'zod';
import { requireProject, HttpError, type ControlDeps, type SessionInfo } from './control.js';
import { hashKey } from './security.js';

const uuid = z.uuid();
const capabilities = z.array(z.string().trim().min(1).max(64)).max(20);
const targetIds = z.array(z.string().uuid()).min(1).max(2);
const aliasInput = z.object({ name: z.string().trim().min(1).max(100), targets: targetIds, capabilities: capabilities.default([]) });
const versionInput = z.object({ targets: targetIds, capabilities: capabilities.default([]) });
const publishInput = z.object({
  expectedVersion: z.number().int().min(0).nullable().optional(),
  expected_version: z.number().int().min(0).nullable().optional(),
  current_version_id: z.string().uuid().nullable().optional(),
}).refine(v => (v.expectedVersion !== undefined && v.expectedVersion !== null) || (v.expected_version !== undefined && v.expected_version !== null), 'expectedVersion is required');
const keyInput = z.object({
  name: z.string().trim().min(1).max(100),
  allowed_models: z.array(z.string().trim().min(1).max(100)).max(100).default([]),
  expires_at: z.string().datetime({ offset: true }).optional(),
  key_rpm: z.number().int().positive().max(1000000).nullable().optional(),
});

export type AliasRow = {
  id: string; project_id: string; name: string; current_version_id: string | null;
  current_version: number | null; current_targets: unknown; current_capabilities: unknown;
  current_price_snapshot: unknown; draft_id: string | null; draft_version: number | null;
  draft_targets: unknown; draft_capabilities: unknown; draft_price_snapshot: unknown;
  draft_created_at: Date | null; published_at: Date | null;
};
type TargetRow = {
  id: string; provider_id: string; upstream_model: string; input_price: string | number;
  output_price: string | number; capabilities: unknown; provider_name: string;
};

function parse<T>(schema: z.ZodType<T>, value: unknown): T {
  const result = schema.safeParse(value ?? {});
  if (!result.success) {
    throw new HttpError(400, 'validation_error', result.error.issues.map(i => `${i.path.join('.')}: ${i.message}`).join('; '));
  }
  return result.data;
}

function idParam(req: FastifyRequest, key = 'id'): string {
  const id = (req.params as Record<string, string | undefined>)[key] ?? '';
  if (!uuid.safeParse(id).success) throw new HttpError(404, 'not_found', 'Resource not found');
  return id;
}

function queryProjectId(req: FastifyRequest): string {
  const id = (req.query as { project_id?: string } | undefined)?.project_id ?? '';
  if (!uuid.safeParse(id).success) throw new HttpError(400, 'project_id_required', 'A valid project_id query parameter is required');
  return id;
}

async function audit(pool: Pool | PoolClient, auth: SessionInfo, projectId: string, action: string, entityType: string, entityId: string, changes: unknown): Promise<void> {
  await pool.query(
    `INSERT INTO audit_events (id, workspace_id, project_id, actor_user_id, action, entity_type, entity_id, changes)
     VALUES ($1,$2,$3,$4,$5,$6,$7,$8::jsonb)`,
    [randomUUID(), auth.workspaceId, projectId, auth.userId, action, entityType, entityId, JSON.stringify(changes ?? {})],
  );
}

function idsOf(value: unknown): string[] {
  return Array.isArray(value) ? value.filter((v): v is string => typeof v === 'string') : [];
}

async function loadTargets(pool: Pool | PoolClient, workspaceId: string, ids: string[]): Promise<TargetRow[]> {
  const unique = [...new Set(ids)];
  if (unique.length !== ids.length) throw new HttpError(400, 'duplicate_target', 'Target IDs must be unique');
  const { rows } = await pool.query(
    `SELECT mt.id, mt.provider_id, mt.upstream_model, mt.input_price, mt.output_price,
            mt.capabilities, p.name AS provider_name
       FROM model_targets mt JOIN providers p ON p.id = mt.provider_id
      WHERE mt.id = ANY($1::uuid[]) AND p.workspace_id = $2`,
    [unique, workspaceId],
  );
  if (rows.length !== unique.length) throw new HttpError(400, 'target_not_found', 'One or more targets do not belong to this workspace');
  return rows as TargetRow[];
}

function requireCoverage(targets: TargetRow[], required: string[]): void {
  for (const target of targets) {
    const offered = Array.isArray(target.capabilities) ? target.capabilities as string[] : [];
    const missing = required.filter(c => !offered.includes(c));
    if (missing.length) throw new HttpError(400, 'capability_not_covered', `Target ${target.upstream_model} lacks capabilities: ${missing.join(', ')}`);
  }
}

function priceSnapshot(targets: TargetRow[]): Record<string, unknown> {
  const prices: Record<string, unknown> = {};
  for (const t of targets) {
    prices[t.id] = {
      upstream_model: t.upstream_model,
      input_micros_per_million: String(t.input_price),
      output_micros_per_million: String(t.output_price),
    };
  }
  return { carrier: 'USD', unit: 'micros_per_million_tokens', targets: prices };
}

async function targetDetails(pool: Pool, rows: AliasRow[]): Promise<Map<string, TargetRow>> {
  const ids = new Set<string>();
  for (const row of rows) {
    for (const id of [...idsOf(row.current_targets), ...idsOf(row.draft_targets)]) ids.add(id);
  }
  if (!ids.size) return new Map();
  const { rows: targets } = await pool.query(
    `SELECT mt.id, mt.provider_id, mt.upstream_model, mt.input_price, mt.output_price,
            mt.capabilities, mt.enabled, p.name AS provider_name
       FROM model_targets mt JOIN providers p ON p.id = mt.provider_id
      WHERE mt.id = ANY($1::uuid[])`,
    [[...ids]],
  );
  return new Map((targets as TargetRow[]).map(t => [t.id, t]));
}

function aliasPublic(row: AliasRow, details: Map<string, TargetRow>) {
  const draft = row.draft_id !== null;
  const targets = idsOf(draft ? row.draft_targets : row.current_targets);
  const version = draft ? row.draft_version : row.current_version;
  const status = draft ? 'draft' : row.current_version !== null ? 'published' : 'empty';
  const cap = (draft ? row.draft_capabilities : row.current_capabilities) ?? [];
  const snapshot = draft ? row.draft_price_snapshot : row.current_price_snapshot;
  const versionObject = (id: string | null, versionNo: number | null, statusValue: string, t: unknown, c: unknown, p: unknown) => id
    ? { id, version: versionNo, status: statusValue, targets: idsOf(t), capabilities: c ?? [], price_snapshot: p ?? {} }
    : null;
  return {
    id: row.id, project_id: row.project_id, name: row.name, current_version_id: row.current_version_id,
    version, status, targets, capabilities: cap, price_snapshot: snapshot ?? {},
    target_details: targets.map(id => details.get(id)).filter(Boolean),
    current_version: versionObject(row.current_version_id, row.current_version, 'published', row.current_targets, row.current_capabilities, row.current_price_snapshot),
    draft_version: versionObject(row.draft_id, row.draft_version, 'draft', row.draft_targets, row.draft_capabilities, row.draft_price_snapshot),
  };
}

const ALIAS_QUERY = `
  SELECT a.id, a.project_id, a.name, a.current_version_id,
         cv.version AS current_version, cv.targets AS current_targets, cv.capabilities AS current_capabilities,
         cv.price_snapshot AS current_price_snapshot, cv.published_at,
         dv.id AS draft_id, dv.version AS draft_version, dv.targets AS draft_targets,
         dv.capabilities AS draft_capabilities, dv.price_snapshot AS draft_price_snapshot, dv.created_at AS draft_created_at
    FROM aliases a
    LEFT JOIN route_versions cv ON cv.id = a.current_version_id
    LEFT JOIN LATERAL (SELECT * FROM route_versions WHERE alias_id = a.id AND status = 'draft'
                       ORDER BY version DESC LIMIT 1) dv ON true`;


export async function registerCatalog(app: FastifyInstance, deps: ControlDeps): Promise<void> {
  const { pool } = deps;

  async function listAliases(req: FastifyRequest, projectId: string) {
    await requireProject(req, pool, projectId, false);
    const { rows } = await pool.query(`${ALIAS_QUERY} WHERE a.project_id = $1 ORDER BY a.name`, [projectId]);
    const details = await targetDetails(pool, rows as AliasRow[]);
    return { items: (rows as AliasRow[]).map(row => aliasPublic(row, details)) };
  }

  async function createAlias(req: FastifyRequest, projectId: string) {
    const auth = await requireProject(req, pool, projectId, true);
    if (auth.role !== 'admin') throw new HttpError(403, 'forbidden', 'Only admins may change model routes');
    const body = parse(aliasInput, req.body);
    const targets = await loadTargets(pool, auth.workspaceId, body.targets);
    requireCoverage(targets, body.capabilities);
    const snapshot = priceSnapshot(targets);
    const aliasId = randomUUID();
    const versionId = randomUUID();
    const client = await pool.connect();
    try {
      await client.query('BEGIN');
      await client.query('INSERT INTO aliases (id, project_id, name) VALUES ($1,$2,$3)', [aliasId, projectId, body.name]);
      await client.query(
        `INSERT INTO route_versions (id, alias_id, version, targets, capabilities, price_snapshot, status, created_by)
         VALUES ($1,$2,1,$3::jsonb,$4::jsonb,$5::jsonb,'draft',$6)`,
        [versionId, aliasId, JSON.stringify(body.targets), JSON.stringify(body.capabilities), JSON.stringify(snapshot), auth.userId],
      );
      await client.query('COMMIT');
    } catch (err) {
      await client.query('ROLLBACK');
      if ((err as { code?: string }).code === '23505') throw new HttpError(409, 'alias_exists', 'Alias name already exists in this project');
      throw err;
    } finally {
      client.release();
    }
    await audit(pool, auth, projectId, 'alias.create', 'alias', aliasId, { name: body.name, targets: body.targets, capabilities: body.capabilities });
    const row: AliasRow = {
      id: aliasId, project_id: projectId, name: body.name, current_version_id: null,
      current_version: null, current_targets: [], current_capabilities: [], current_price_snapshot: {},
      draft_id: versionId, draft_version: 1, draft_targets: body.targets,
      draft_capabilities: body.capabilities, draft_price_snapshot: snapshot, draft_created_at: new Date(), published_at: null,
    };
    return aliasPublic(row, new Map(targets.map(t => [t.id, t])));
  }

  app.get('/api/aliases', async req => listAliases(req, queryProjectId(req)));
  app.get('/api/projects/:id/aliases', async req => listAliases(req, idParam(req)));

  app.post('/api/aliases', async req => {
    const body = parse(z.object({ project_id: z.string().uuid() }).passthrough(), req.body);
    return createAlias(req, body.project_id);
  });
  app.post('/api/projects/:id/aliases', async req => {
    return createAlias(req, idParam(req));
  });

  async function aliasForAccess(req: FastifyRequest, aliasId: string, write: boolean) {
    const { rows } = await pool.query(
      `SELECT a.*, p.workspace_id FROM aliases a JOIN projects p ON p.id = a.project_id WHERE a.id = $1`,
      [aliasId],
    );
    const alias = rows[0] as (AliasRow & { workspace_id: string }) | undefined;
    if (!alias) throw new HttpError(404, 'alias_not_found', 'Alias not found');
    const auth = await requireProject(req, pool, alias.project_id, write);
    if (write && auth.role !== 'admin') throw new HttpError(403, 'forbidden', 'Only admins may change model routes');
    return { alias, auth };
  }

  app.post('/api/aliases/:id/versions', async req => {
    const aliasId = idParam(req);
    const { alias, auth } = await aliasForAccess(req, aliasId, true);
    const body = parse(versionInput, req.body);
    const targets = await loadTargets(pool, alias.workspace_id, body.targets);
    requireCoverage(targets, body.capabilities);
    const snapshot = priceSnapshot(targets);
    const versionId = randomUUID();
    let version = 0;
    const client = await pool.connect();
    try {
      await client.query('BEGIN');
      await client.query('SELECT id FROM aliases WHERE id = $1 FOR UPDATE', [aliasId]);
      const next = await client.query('SELECT COALESCE(MAX(version),0) + 1 AS next FROM route_versions WHERE alias_id = $1', [aliasId]);
      version = Number(next.rows[0].next);
      await client.query(
        `INSERT INTO route_versions (id, alias_id, version, targets, capabilities, price_snapshot, status, created_by)
         VALUES ($1,$2,$3,$4::jsonb,$5::jsonb,$6::jsonb,'draft',$7)`,
        [versionId, aliasId, version, JSON.stringify(body.targets), JSON.stringify(body.capabilities), JSON.stringify(snapshot), auth.userId],
      );
      await client.query('COMMIT');
    } catch (err) {
      await client.query('ROLLBACK');
      throw err;
    } finally {
      client.release();
    }
    await audit(pool, auth, alias.project_id, 'alias.version.create', 'route_version', versionId, { alias_id: aliasId, version, targets: body.targets });
    return {
      id: versionId, alias_id: aliasId, version, status: 'draft', targets: body.targets,
      capabilities: body.capabilities, price_snapshot: snapshot,
      target_details: targets.map(t => ({ ...t })),
    };
  });

  app.post('/api/aliases/:id/publish', async req => {
    const aliasId = idParam(req);
    const body = parse(publishInput, req.body);
    const { alias, auth } = await aliasForAccess(req, aliasId, true);
    const client = await pool.connect();
    let published: { id: string; version: number; targets: unknown; capabilities: unknown; price_snapshot: unknown } | null = null;
    let targets: TargetRow[] = [];
    try {
      await client.query('BEGIN');
      const lockedResult = await client.query('SELECT * FROM aliases WHERE id = $1 FOR UPDATE', [aliasId]);
      const locked = lockedResult.rows[0];
      let currentVersion = 0;
      if (locked.current_version_id) {
        const current = await client.query('SELECT version FROM route_versions WHERE id = $1', [locked.current_version_id]);
        currentVersion = Number(current.rows[0]?.version ?? 0);
      }
      const latestResult = await client.query('SELECT COALESCE(MAX(version),0) AS latest FROM route_versions WHERE alias_id = $1', [aliasId]);
      const latest = Number(latestResult.rows[0].latest);
      if (body.current_version_id !== undefined && (body.current_version_id ?? null) !== locked.current_version_id) {
        throw new HttpError(409, 'version_conflict', 'Alias was modified; reload before publishing');
      }
      const hasExpected = body.expectedVersion !== undefined || body.expected_version !== undefined;
      const expected = body.expectedVersion !== undefined ? body.expectedVersion : body.expected_version;
      if (hasExpected) {
        if (expected === null) {
          if (locked.current_version_id !== null) throw new HttpError(409, 'version_conflict', 'Alias already has a published version');
        } else if (expected !== latest) {
          throw new HttpError(409, 'version_conflict', 'Alias was modified; reload before publishing');
        }
      }
      const draftResult = await client.query(
        "SELECT * FROM route_versions WHERE alias_id = $1 AND status = 'draft' ORDER BY version DESC LIMIT 1",
        [aliasId],
      );
      const draft = draftResult.rows[0];
      if (!draft) throw new HttpError(409, 'no_draft', 'There is no draft version to publish');
      const draftTargets = idsOf(draft.targets);
      if (draftTargets.length < 1 || draftTargets.length > 2) throw new HttpError(409, 'invalid_route', 'A route requires one or two targets');
      targets = await loadTargets(client, alias.workspace_id, draftTargets);
      requireCoverage(targets, Array.isArray(draft.capabilities) ? draft.capabilities as string[] : []);
      const updated = await client.query(
        "UPDATE route_versions SET status = 'published', published_at = now() WHERE id = $1 AND status = 'draft' RETURNING id, version, targets, capabilities, price_snapshot",
        [draft.id],
      );
      if (!updated.rowCount) throw new HttpError(409, 'version_conflict', 'Draft was already published');
      await client.query('UPDATE aliases SET current_version_id = $2, updated_at = now() WHERE id = $1', [aliasId, draft.id]);
      await client.query('COMMIT');
      published = updated.rows[0];
    } catch (err) {
      await client.query('ROLLBACK');
      throw err;
    } finally {
      client.release();
    }
    await audit(pool, auth, alias.project_id, 'alias.publish', 'alias', aliasId, { version_id: published!.id, version: published!.version });
    const row: AliasRow = {
      id: aliasId, project_id: alias.project_id, name: alias.name, current_version_id: published!.id,
      current_version: published!.version, current_targets: published!.targets,
      current_capabilities: published!.capabilities, current_price_snapshot: published!.price_snapshot,
      draft_id: null, draft_version: null, draft_targets: [], draft_capabilities: [],
      draft_price_snapshot: {}, draft_created_at: null, published_at: new Date(),
    };
    return aliasPublic(row, new Map(targets.map(t => [t.id, t])));
  });

  async function listKeys(req: FastifyRequest, projectId: string) {
    await requireProject(req, pool, projectId, false);
    const { rows } = await pool.query(
      `SELECT id, project_id, name, prefix, created_by, allowed_models, expires_at, revoked_at, key_rpm, created_at, last_used_at
         FROM api_keys WHERE project_id = $1 ORDER BY created_at DESC`,
      [projectId],
    );
    return { items: rows };
  }

  async function createKey(req: FastifyRequest, projectId: string) {
    const auth = await requireProject(req, pool, projectId, true);
    const body = parse(keyInput, req.body);
    const allowedModels = [...new Set(body.allowed_models)];
    if (allowedModels.length) {
      const found = await pool.query('SELECT name FROM aliases WHERE project_id = $1 AND name = ANY($2::text[])', [projectId, allowedModels]);
      const names = new Set(found.rows.map(r => r.name as string));
      const missing = allowedModels.filter(name => !names.has(name));
      if (missing.length) throw new HttpError(400, 'invalid_allowed_models', `Unknown model aliases: ${missing.join(', ')}`);
    }
    const expiresAt = body.expires_at ? new Date(body.expires_at) : new Date(Date.now() + 30 * 24 * 60 * 60 * 1000);
    if (Number.isNaN(expiresAt.getTime()) || expiresAt.getTime() <= Date.now()) {
      throw new HttpError(400, 'invalid_expiry', 'expires_at must be in the future');
    }
    const plaintext = `sk-${randomBytes(24).toString('base64url')}`;
    const prefix = plaintext.slice(0, 12);
    const id = randomUUID();
    const { rows } = await pool.query(
      `INSERT INTO api_keys (id, project_id, name, key_hash, prefix, created_by, allowed_models, expires_at, key_rpm)
       VALUES ($1,$2,$3,$4,$5,$6,$7::jsonb,$8,$9)
       RETURNING id, project_id, name, prefix, created_by, allowed_models, expires_at, key_rpm, created_at`,
      [id, projectId, body.name, hashKey(plaintext), prefix, auth.userId, JSON.stringify(allowedModels), expiresAt, body.key_rpm ?? null],
    );
    await audit(pool, auth, projectId, 'key.create', 'api_key', id, { name: body.name, allowed_models: allowedModels, expires_at: expiresAt.toISOString() });
    return { ...rows[0], key: plaintext };
  }

  app.get('/api/keys', async req => listKeys(req, queryProjectId(req)));
  app.get('/api/projects/:id/keys', async req => listKeys(req, idParam(req)));
  app.post('/api/keys', async req => {
    const body = parse(z.object({ project_id: z.string().uuid() }).passthrough(), req.body);
    return createKey(req, body.project_id);
  });
  app.post('/api/projects/:id/keys', async req => createKey(req, idParam(req)));

  app.post('/api/keys/:id/revoke', async req => {
    const keyId = idParam(req);
    const { rows } = await pool.query('SELECT * FROM api_keys WHERE id = $1', [keyId]);
    const key = rows[0];
    if (!key) throw new HttpError(404, 'key_not_found', 'API key not found');
    const auth = await requireProject(req, pool, key.project_id, true);
    if (auth.role === 'developer' && key.created_by !== auth.userId) {
      throw new HttpError(403, 'forbidden', 'Developers may only revoke their own keys');
    }
    const updated = await pool.query(
      'UPDATE api_keys SET revoked_at = COALESCE(revoked_at, now()) WHERE id = $1 RETURNING id, project_id, prefix, revoked_at',
      [keyId],
    );
    await audit(pool, auth, key.project_id, 'key.revoke', 'api_key', keyId, { prefix: key.prefix });
    return updated.rows[0];
  });
}
