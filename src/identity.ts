import { randomBytes, randomUUID } from 'node:crypto';
import type { FastifyInstance, FastifyRequest } from 'fastify';
import type { Pool, PoolClient } from 'pg';
import { z } from 'zod';
import {
  authenticateSession, createSession, hashPassword, hashToken, HttpError, requireProject,
  type ControlDeps, type SessionInfo,
} from './control.js';

const uuid = z.uuid();
const budget = z.number().int().min(0).max(Number.MAX_SAFE_INTEGER).nullable().optional();
const rpm = z.number().int().min(1).max(1_000_000).nullable().optional();
const concurrency = z.number().int().min(1).max(10_000).nullable().optional();
const maxOutput = z.number().int().min(1).max(1_000_000).nullable().optional();
const projectCreate = z.object({
  name: z.string().trim().min(1).max(100),
  budget_micros: budget, rpm, concurrency, max_output_tokens: maxOutput,
});
const projectUpdate = z.object({
  name: z.string().trim().min(1).max(100).optional(),
  budget_micros: budget, rpm, concurrency, max_output_tokens: maxOutput,
}).refine(v => Object.keys(v).length > 0, 'No fields to update');
const memberUpdate = z.object({
  role: z.enum(['admin', 'developer', 'viewer']).optional(),
  disabled: z.boolean().optional(),
  project_ids: z.array(z.string().uuid()).max(1000).optional(),
}).refine(v => Object.keys(v).length > 0, 'No fields to update');
const inviteCreate = z.object({
  email: z.email().max(320).transform(v => v.trim().toLowerCase()),
  role: z.enum(['admin', 'developer', 'viewer']),
  project_ids: z.array(z.string().uuid()).max(1000).default([]),
});
const inviteAccept = z.object({
  token: z.string().min(20).max(300),
  password: z.string().min(10).max(200),
});
const settingsUpdate = z.object({
  request_retention_days: z.number().int().min(1).max(3650).nullable(),
});

function parse<T>(schema: z.ZodType<T>, value: unknown): T {
  const result = schema.safeParse(value ?? {});
  if (!result.success) {
    throw new HttpError(400, 'validation_error', result.error.issues.map(i => `${i.path.join('.')}: ${i.message}`).join('; '));
  }
  return result.data;
}

function idParam(req: FastifyRequest): string {
  const id = (req.params as { id?: string }).id ?? '';
  if (!uuid.safeParse(id).success) throw new HttpError(404, 'not_found', 'Resource not found');
  return id;
}

async function requireAdmin(req: FastifyRequest, pool: Pool): Promise<SessionInfo> {
  const auth = await authenticateSession(req, pool);
  if (auth.role !== 'admin') throw new HttpError(403, 'forbidden', 'Administrator role required');
  return auth;
}

async function audit(
  db: Pool | PoolClient, auth: SessionInfo, projectId: string | null,
  action: string, entityType: string, entityId: string, changes: unknown,
): Promise<void> {
  await db.query(
    `INSERT INTO audit_events (id, workspace_id, project_id, actor_user_id, action, entity_type, entity_id, changes)
     VALUES ($1,$2,$3,$4,$5,$6,$7,$8::jsonb)`,
    [randomUUID(), auth.workspaceId, projectId, auth.userId, action, entityType, entityId, JSON.stringify(changes ?? {})],
  );
}

async function validateProjectIds(db: Pool | PoolClient, workspaceId: string, ids: string[]): Promise<string[]> {
  const unique = [...new Set(ids)];
  if (!unique.length) return [];
  const { rows } = await db.query('SELECT id FROM projects WHERE workspace_id = $1 AND id = ANY($2::uuid[])', [workspaceId, unique]);
  if (rows.length !== unique.length) throw new HttpError(400, 'invalid_project_ids', 'One or more projects do not belong to this workspace');
  return unique;
}

async function memberView(db: Pool | PoolClient, workspaceId: string, userId: string) {
  const { rows } = await db.query(
    `SELECT u.id, u.email, u.role, u.disabled_at, u.created_at,
            COALESCE(json_agg(p.id) FILTER (WHERE p.id IS NOT NULL), '[]'::json) AS project_ids
       FROM users u
       LEFT JOIN project_members pm ON pm.user_id = u.id
       LEFT JOIN projects p ON p.id = pm.project_id AND p.workspace_id = u.workspace_id
      WHERE u.id = $1 AND u.workspace_id = $2
      GROUP BY u.id`,
    [userId, workspaceId],
  );
  return rows[0];
}

export async function registerIdentity(app: FastifyInstance, deps: ControlDeps): Promise<void> {
  const { pool } = deps;

  app.post('/api/projects', async req => {
    const auth = await requireAdmin(req, pool);
    const body = parse(projectCreate, req.body);
    const id = randomUUID();
    const client = await pool.connect();
    try {
      await client.query('BEGIN');
      const { rows } = await client.query(
        `INSERT INTO projects (id, workspace_id, name, budget_micros, rpm, concurrency, max_output_tokens)
         VALUES ($1,$2,$3,$4,$5,$6,$7)
         RETURNING id, workspace_id, name, budget_micros, rpm, concurrency, max_output_tokens, created_at, updated_at`,
        [id, auth.workspaceId, body.name, body.budget_micros ?? null, body.rpm ?? null, body.concurrency ?? null, body.max_output_tokens ?? null],
      );
      await client.query('INSERT INTO project_members (project_id, user_id) VALUES ($1,$2) ON CONFLICT DO NOTHING', [id, auth.userId]);
      await audit(client, auth, id, 'project.create', 'project', id, {
        name: body.name, budget_micros: body.budget_micros ?? null, rpm: body.rpm ?? null,
        concurrency: body.concurrency ?? null, max_output_tokens: body.max_output_tokens ?? null,
      });
      await client.query('COMMIT');
      return rows[0];
    } catch (err) {
      await client.query('ROLLBACK');
      if ((err as { code?: string }).code === '23505') throw new HttpError(409, 'project_exists', 'Project name already exists');
      throw err;
    } finally {
      client.release();
    }
  });

  app.get('/api/projects/:id', async req => {
    const id = idParam(req);
    const { project } = await requireProject(req, pool, id, false);
    return project;
  });

  app.patch('/api/projects/:id', async req => {
    const id = idParam(req);
    const auth = await requireProject(req, pool, id, true);
    if (auth.role !== 'admin') throw new HttpError(403, 'forbidden', 'Only administrators may change project settings');
    const body = parse(projectUpdate, req.body);
    const sets: string[] = [];
    const values: unknown[] = [];
    for (const field of ['name', 'budget_micros', 'rpm', 'concurrency', 'max_output_tokens'] as const) {
      if (body[field] !== undefined) {
        values.push(body[field]);
        sets.push(`${field} = $${values.length}`);
      }
    }
    values.push(id);
    try {
      const { rows } = await pool.query(
        `UPDATE projects SET ${sets.join(', ')}, updated_at = now() WHERE id = $${values.length}
         RETURNING id, workspace_id, name, budget_micros, rpm, concurrency, max_output_tokens, created_at, updated_at`,
        values,
      );
      await audit(pool, auth, id, 'project.update', 'project', id, body);
      return rows[0];
    } catch (err) {
      if ((err as { code?: string }).code === '23505') throw new HttpError(409, 'project_exists', 'Project name already exists');
      throw err;
    }
  });

  app.get('/api/members', async req => {
    const auth = await requireAdmin(req, pool);
    const { rows } = await pool.query(
      `SELECT u.id, u.email, u.role, u.disabled_at, u.created_at,
              COALESCE(json_agg(p.id) FILTER (WHERE p.id IS NOT NULL), '[]'::json) AS project_ids
         FROM users u
         LEFT JOIN project_members pm ON pm.user_id = u.id
         LEFT JOIN projects p ON p.id = pm.project_id AND p.workspace_id = u.workspace_id
        WHERE u.workspace_id = $1
        GROUP BY u.id ORDER BY u.created_at, u.email`,
      [auth.workspaceId],
    );
    return { items: rows };
  });

  app.patch('/api/members/:id', async req => {
    const auth = await requireAdmin(req, pool);
    const userId = idParam(req);
    const body = parse(memberUpdate, req.body);
    if (userId === auth.userId && body.disabled === true) {
      throw new HttpError(400, 'self_disable', 'Administrators cannot disable their own account');
    }
    const client = await pool.connect();
    try {
      await client.query('BEGIN');
      const activeAdmins = await client.query(
        "SELECT id FROM users WHERE workspace_id = $1 AND role = 'admin' AND disabled_at IS NULL FOR UPDATE",
        [auth.workspaceId],
      );
      const targetResult = await client.query('SELECT * FROM users WHERE id = $1 AND workspace_id = $2 FOR UPDATE', [userId, auth.workspaceId]);
      const target = targetResult.rows[0];
      if (!target) throw new HttpError(404, 'member_not_found', 'Workspace member not found');
      const newRole = body.role ?? target.role;
      const newDisabled = body.disabled ?? target.disabled_at !== null;
      const wasActiveAdmin = target.role === 'admin' && target.disabled_at === null;
      const willBeActiveAdmin = newRole === 'admin' && !newDisabled;
      if (wasActiveAdmin && !willBeActiveAdmin) {
        const otherAdmins = activeAdmins.rows.filter(row => row.id !== userId).length;
        if (otherAdmins === 0) throw new HttpError(409, 'last_admin', 'At least one active administrator must remain');
      }
      const sets: string[] = [];
      const values: unknown[] = [];
      if (body.role !== undefined) { values.push(body.role); sets.push(`role = $${values.length}`); }
      if (body.disabled !== undefined) {
        values.push(body.disabled ? new Date() : null);
        sets.push(`disabled_at = $${values.length}`);
      }
      if (sets.length) {
        values.push(userId);
        await client.query(`UPDATE users SET ${sets.join(', ')} WHERE id = $${values.length}`, values);
      }
      if (body.project_ids !== undefined) {
        const projectIds = await validateProjectIds(client, auth.workspaceId, body.project_ids);
        await client.query('DELETE FROM project_members WHERE user_id = $1', [userId]);
        for (const projectId of projectIds) {
          await client.query('INSERT INTO project_members (project_id, user_id) VALUES ($1,$2) ON CONFLICT DO NOTHING', [projectId, userId]);
        }
      }
      await client.query('UPDATE sessions SET revoked_at = now() WHERE user_id = $1 AND revoked_at IS NULL', [userId]);
      await audit(client, auth, null, 'member.update', 'user', userId, {
        role: body.role, disabled: body.disabled, project_ids: body.project_ids,
      });
      await client.query('COMMIT');
    } catch (err) {
      await client.query('ROLLBACK');
      throw err;
    } finally {
      client.release();
    }
    return memberView(pool, auth.workspaceId, userId);
  });

  app.post('/api/invitations', async req => {
    const auth = await requireAdmin(req, pool);
    const body = parse(inviteCreate, req.body);
    const projectIds = await validateProjectIds(pool, auth.workspaceId, body.project_ids);
    const existing = await pool.query('SELECT id FROM users WHERE lower(email) = $1', [body.email]);
    if (existing.rowCount) throw new HttpError(409, 'email_registered', 'A user with this email already exists');
    const token = randomBytes(32).toString('base64url');
    const expiresAt = new Date(Date.now() + 7 * 24 * 60 * 60 * 1000);
    const id = randomUUID();
    await pool.query(
      `INSERT INTO invitations (id, workspace_id, email, role, token_hash, project_ids, created_by, expires_at)
       VALUES ($1,$2,$3,$4,$5,$6::jsonb,$7,$8)`,
      [id, auth.workspaceId, body.email, body.role, hashToken(token), JSON.stringify(projectIds), auth.userId, expiresAt],
    );
    await audit(pool, auth, null, 'invitation.create', 'invitation', id, {
      email: body.email, role: body.role, project_ids: projectIds, expires_at: expiresAt.toISOString(),
    });
    return { id, email: body.email, role: body.role, project_ids: projectIds, expires_at: expiresAt, token };
  });

  app.post('/api/invitations/accept', async (req, reply) => {
    const body = parse(inviteAccept, req.body);
    const passwordHash = await hashPassword(body.password);
    const client = await pool.connect();
    let accepted: { userId: string; email: string; role: string; workspaceId: string; projectIds: string[] } | null = null;
    try {
      await client.query('BEGIN');
      const invitationResult = await client.query(
        'SELECT * FROM invitations WHERE token_hash = $1 FOR UPDATE',
        [hashToken(body.token)],
      );
      const invitation = invitationResult.rows[0];
      if (!invitation || invitation.accepted_at || new Date(invitation.expires_at) <= new Date()) {
        throw new HttpError(400, 'invalid_invitation', 'Invitation is invalid or has expired');
      }
      const existing = await client.query('SELECT id FROM users WHERE lower(email) = lower($1)', [invitation.email]);
      if (existing.rowCount) throw new HttpError(409, 'email_registered', 'A user with this email already exists');
      const userId = randomUUID();
      await client.query(
        'INSERT INTO users (id, workspace_id, email, password_hash, role) VALUES ($1,$2,$3,$4,$5)',
        [userId, invitation.workspace_id, invitation.email, passwordHash, invitation.role],
      );
      const requested = Array.isArray(invitation.project_ids) ? invitation.project_ids.filter((v: unknown): v is string => typeof v === 'string') : [];
      let projectIds: string[] = [];
      if (requested.length) {
        const projects = await client.query(
          'SELECT id FROM projects WHERE workspace_id = $1 AND id = ANY($2::uuid[])',
          [invitation.workspace_id, requested],
        );
        projectIds = projects.rows.map(row => row.id as string);
        for (const projectId of projectIds) {
          await client.query('INSERT INTO project_members (project_id, user_id) VALUES ($1,$2) ON CONFLICT DO NOTHING', [projectId, userId]);
        }
      }
      await client.query('UPDATE invitations SET accepted_at = now() WHERE id = $1', [invitation.id]);
      const actor: SessionInfo = {
        userId, workspaceId: invitation.workspace_id, role: invitation.role,
        projectIds, csrfHash: '',
      };
      await audit(client, actor, null, 'invitation.accept', 'user', userId, {
        invitation_id: invitation.id, role: invitation.role, project_ids: projectIds,
      });
      await client.query('COMMIT');
      accepted = { userId, email: invitation.email, role: invitation.role, workspaceId: invitation.workspace_id, projectIds };
    } catch (err) {
      await client.query('ROLLBACK');
      throw err;
    } finally {
      client.release();
    }
    const session = await createSession(pool, reply, accepted.userId, accepted.workspaceId);
    const projects = await pool.query('SELECT id, name FROM projects WHERE workspace_id = $1 AND id = ANY($2::uuid[]) ORDER BY name', [accepted.workspaceId, accepted.projectIds]);
    return {
      session: {
        user: { id: accepted.userId, email: accepted.email, role: accepted.role, workspaceId: accepted.workspaceId },
        projects: projects.rows,
        csrf: session.csrf,
      },
      csrf: session.csrf,
    };
  });

  async function getSettings(auth: SessionInfo) {
    const { rows } = await pool.query("SELECT value FROM settings WHERE workspace_id = $1 AND key = 'request_retention_days'", [auth.workspaceId]);
    const value = rows[0]?.value;
    return { request_retention_days: typeof value === 'number' ? value : null };
  }

  app.get('/api/settings', async req => getSettings(await requireAdmin(req, pool)));

  app.patch('/api/settings', async req => {
    const auth = await requireAdmin(req, pool);
    const body = parse(settingsUpdate, req.body);
    await pool.query(
      `INSERT INTO settings (workspace_id, key, value) VALUES ($1, 'request_retention_days', $2::jsonb)
       ON CONFLICT (workspace_id, key) DO UPDATE SET value = EXCLUDED.value, updated_at = now()`,
      [auth.workspaceId, JSON.stringify(body.request_retention_days)],
    );
    await audit(pool, auth, null, 'settings.update', 'workspace_settings', auth.workspaceId, body);
    return getSettings(auth);
  });
}
