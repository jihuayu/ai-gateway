import { randomBytes, scrypt as scryptCb, timingSafeEqual, randomUUID } from 'node:crypto';
import { promisify } from 'node:util';
import type { FastifyInstance, FastifyReply, FastifyRequest } from 'fastify';
import type { Pool, PoolClient } from 'pg';
import { z } from 'zod';
import { hashKey } from './security.js';

const scrypt = promisify(scryptCb);
const SESSION_COOKIE = 'gw_session';
const CSRF_COOKIE = 'gw_csrf';
const SESSION_TTL_MS = 7 * 24 * 60 * 60 * 1000;

export class HttpError extends Error {
  constructor(readonly statusCode: number, readonly code: string, message: string) {
    super(message);
  }
}

export interface SessionInfo {
  userId: string;
  workspaceId: string;
  role: string;
  projectIds: string[];
  csrfHash: string;
}

export interface ProjectRow {
  id: string;
  workspace_id: string;
  name: string;
  budget_micros: string | null;
  rpm: number | null;
  concurrency: number | null;
  max_output_tokens: number | null;
}

export interface ControlDeps {
  pool: Pool;
  encryptionKey: string;
  allowPrivateUpstream: boolean;
}

function parseCookies(req: FastifyRequest): Record<string, string> {
  const out: Record<string, string> = {};
  const raw = req.headers.cookie;
  if (!raw) return out;
  for (const part of raw.split(';')) {
    const i = part.indexOf('=');
    if (i < 0) continue;
    const k = part.slice(0, i).trim();
    const v = part.slice(i + 1).trim();
    if (k) { try { out[k] = decodeURIComponent(v); } catch { out[k] = v; } }
  }
  return out;
}

export async function hashPassword(password: string): Promise<string> {
  const salt = randomBytes(16);
  const derived = (await scrypt(password, salt, 64)) as Buffer;
  return `scrypt:${salt.toString('hex')}:${derived.toString('hex')}`;
}

export async function verifyPassword(password: string, stored: string): Promise<boolean> {
  const [scheme, saltHex, hashHex] = stored.split(':');
  if (scheme !== 'scrypt' || !saltHex || !hashHex) return false;
  const expected = Buffer.from(hashHex, 'hex');
  const derived = (await scrypt(password, Buffer.from(saltHex, 'hex'), expected.length)) as Buffer;
  return expected.length === derived.length && timingSafeEqual(expected, derived);
}

export function hashToken(token: string): string {
  return hashKey(token);
}

function makeSessionSecrets(): { token: string; csrf: string } {
  return { token: randomBytes(32).toString('base64url'), csrf: randomBytes(32).toString('base64url') };
}

export async function createSession(
  pool: Pool | PoolClient, reply: FastifyReply, userId: string, workspaceId: string,
): Promise<{ token: string; csrf: string; expiresAt: Date }> {
  const { token, csrf } = makeSessionSecrets();
  const expiresAt = new Date(Date.now() + SESSION_TTL_MS);
  await pool.query(
    `INSERT INTO sessions (id, token_hash, csrf_hash, user_id, workspace_id, expires_at)
     VALUES ($1,$2,$3,$4,$5,$6)`,
    [randomUUID(), hashToken(token), hashToken(csrf), userId, workspaceId, expiresAt],
  );
  setSessionCookies(reply, token, csrf, Math.floor(SESSION_TTL_MS / 1000));
  return { token, csrf, expiresAt };
}

export function setSessionCookies(reply: FastifyReply, token: string, csrf: string, maxAge: number): void {
  const secure = process.env.NODE_ENV === 'production' ? '; Secure' : '';
  reply.header('set-cookie', [
    `${SESSION_COOKIE}=${encodeURIComponent(token)}; Path=/; HttpOnly; SameSite=Lax; Max-Age=${maxAge}${secure}`,
    `${CSRF_COOKIE}=${encodeURIComponent(csrf)}; Path=/; SameSite=Lax; Max-Age=${maxAge}${secure}`,
  ]);
}

export function clearSessionCookies(reply: FastifyReply): void {
  reply.header('set-cookie', [
    `${SESSION_COOKIE}=; Path=/; HttpOnly; SameSite=Lax; Max-Age=0`,
    `${CSRF_COOKIE}=; Path=/; SameSite=Lax; Max-Age=0`,
  ]);
}

/** Resolve the current session from the cookie; throws 401 when absent/invalid. */
export async function authenticateSession(req: FastifyRequest, pool: Pool): Promise<SessionInfo> {
  const cached = (req as unknown as { auth?: SessionInfo }).auth;
  if (cached) return cached;
  const token = parseCookies(req)[SESSION_COOKIE];
  if (!token) throw new HttpError(401, 'unauthenticated', 'Session required');
  const { rows } = await pool.query(
    `SELECT s.csrf_hash, s.user_id, s.workspace_id, s.expires_at, s.revoked_at, u.role, u.disabled_at
       FROM sessions s JOIN users u ON u.id = s.user_id
      WHERE s.token_hash = $1`,
    [hashToken(token)],
  );
  const row = rows[0];
  if (!row || row.revoked_at || row.disabled_at || new Date(row.expires_at) <= new Date()) {
    throw new HttpError(401, 'unauthenticated', 'Session required');
  }
  const memberships = await pool.query('SELECT project_id FROM project_members WHERE user_id = $1', [row.user_id]);
  const info: SessionInfo = {
    userId: row.user_id, workspaceId: row.workspace_id, role: row.role,
    projectIds: memberships.rows.map(r => r.project_id), csrfHash: row.csrf_hash,
  };
  (req as unknown as { auth?: SessionInfo }).auth = info;
  return info;
}

export async function requireCsrf(req: FastifyRequest): Promise<void> {
  const auth = (req as unknown as { auth?: SessionInfo }).auth;
  const token = req.headers['x-csrf-token'];
  if (!auth || typeof token !== 'string' || !token) throw new HttpError(403, 'csrf_failed', 'CSRF token required');
  const provided = Buffer.from(hashToken(token));
  const expected = Buffer.from(auth.csrfHash);
  if (provided.length !== expected.length || !timingSafeEqual(provided, expected)) {
    throw new HttpError(403, 'csrf_failed', 'CSRF token invalid');
  }
}

/** Require access to a project; admins access all workspace projects. */
export async function requireProject(
  req: FastifyRequest, pool: Pool, projectId: string, write = false,
): Promise<SessionInfo & { project: ProjectRow }> {
  if (!z.uuid().safeParse(projectId).success) throw new HttpError(404, 'project_not_found', 'Project not found');
  const auth = await authenticateSession(req, pool);
  if (write && !['admin', 'developer'].includes(auth.role)) {
    throw new HttpError(403, 'forbidden', 'Write access required');
  }
  const { rows } = await pool.query(
    `SELECT p.* FROM projects p
      WHERE p.id = $1 AND p.workspace_id = $2
        AND ($3 = 'admin' OR EXISTS (SELECT 1 FROM project_members m WHERE m.project_id = p.id AND m.user_id = $4))`,
    [projectId, auth.workspaceId, auth.role, auth.userId],
  );
  if (!rows[0]) throw new HttpError(404, 'project_not_found', 'Project not found');
  return { ...auth, project: rows[0] as ProjectRow };
}

async function sessionPayload(pool: Pool, auth: SessionInfo, csrf: string) {
  const user = await pool.query('SELECT id, email, role, workspace_id FROM users WHERE id = $1', [auth.userId]);
  const projects = await pool.query(
    `SELECT id, name FROM projects WHERE workspace_id = $1
       AND ($2 = 'admin' OR id = ANY($3::uuid[])) ORDER BY name`,
    [auth.workspaceId, auth.role, auth.projectIds],
  );
  return {
    user: { id: user.rows[0].id, email: user.rows[0].email, role: user.rows[0].role, workspaceId: auth.workspaceId },
    projects: projects.rows.map(p => ({ id: p.id, name: p.name })),
    csrf,
  };
}

const credentialsSchema = z.object({
  email: z.email().max(320).transform(v => v.trim().toLowerCase()),
  password: z.string().min(10).max(200),
});

function parseBody<T>(schema: z.ZodType<T>, body: unknown): T {
  const parsed = schema.safeParse(body ?? {});
  if (!parsed.success) {
    throw new HttpError(400, 'validation_error', parsed.error.issues.map(i => `${i.path.join('.')}: ${i.message}`).join('; '));
  }
  return parsed.data;
}

export async function registerControl(app: FastifyInstance, deps: ControlDeps): Promise<void> {
  const { pool } = deps;

  app.addHook('onRequest', async (req: FastifyRequest) => {
    const method = req.method.toUpperCase();
    if (!['POST', 'PUT', 'PATCH', 'DELETE'].includes(method)) return;
    if (!req.url.startsWith('/api')) return;
    const pathname = req.url.split('?')[0];
    if (pathname === '/api/invitations/accept') return;
    if (req.url === '/api/bootstrap' || req.url === '/api/login' || req.url.startsWith('/api/login?')) return;
    await authenticateSession(req, pool);
    await requireCsrf(req);
  });

  app.post('/api/bootstrap', async (req, reply) => {
    const body = parseBody(z.object({
      email: z.email().max(320).transform(v => v.trim().toLowerCase()),
      password: z.string().min(10).max(200),
      projectName: z.string().trim().min(1).max(100),
    }), req.body);
    const passwordHash = await hashPassword(body.password);
    const client = await pool.connect();
    try {
      await client.query('BEGIN');
      await client.query('SELECT pg_advisory_xact_lock($1)', [0x626f6f74]);
      const count = await client.query('SELECT count(*)::int AS n FROM users');
      if (count.rows[0].n > 0) throw new HttpError(409, 'bootstrap_closed', 'Bootstrap is only available on an empty installation');
      const workspaceId = randomUUID();
      const userId = randomUUID();
      const projectId = randomUUID();
      await client.query('INSERT INTO workspaces (id, name) VALUES ($1, $2)', [workspaceId, 'Default workspace']);
      await client.query(
        'INSERT INTO users (id, workspace_id, email, password_hash, role) VALUES ($1,$2,$3,$4,$5)',
        [userId, workspaceId, body.email, passwordHash, 'admin'],
      );
      const project = await client.query(
        'INSERT INTO projects (id, workspace_id, name) VALUES ($1,$2,$3) RETURNING id, name, budget_micros, rpm, concurrency, max_output_tokens',
        [projectId, workspaceId, body.projectName],
      );
      await client.query('INSERT INTO project_members (project_id, user_id) VALUES ($1,$2)', [projectId, userId]);
      await client.query(
        `INSERT INTO audit_events (id, workspace_id, project_id, actor_user_id, action, entity_type, entity_id, changes)
         VALUES ($1,$2,$3,$4,'bootstrap','workspace',$5,$6::jsonb)`,
        [randomUUID(), workspaceId, projectId, userId, workspaceId, JSON.stringify({ projectName: body.projectName })],
      );
      const session = await createSession(client, reply, userId, workspaceId);
      await client.query('COMMIT');
      return {
        session: {
          user: { id: userId, email: body.email, role: 'admin', workspaceId },
          projects: [{ id: projectId, name: body.projectName }],
          csrf: session.csrf,
        },
        csrf: session.csrf,
        project: project.rows[0],
      };
    } catch (err) {
      await client.query('ROLLBACK');
      throw err;
    } finally {
      client.release();
    }
  });

  app.post('/api/login', async (req, reply) => {
    const body = parseBody(credentialsSchema, req.body);
    const { rows } = await pool.query(
      'SELECT id, workspace_id, password_hash, role, disabled_at FROM users WHERE email = $1',
      [body.email],
    );
    const user = rows[0];
    const ok = user ? await verifyPassword(body.password, user.password_hash) : false;
    if (!user || !ok || user.disabled_at) throw new HttpError(401, 'invalid_credentials', 'Email or password is incorrect');
    const session = await createSession(pool, reply, user.id, user.workspace_id);
    const memberships = await pool.query('SELECT project_id FROM project_members WHERE user_id = $1', [user.id]);
    const auth: SessionInfo = {
      userId: user.id, workspaceId: user.workspace_id, role: user.role,
      projectIds: memberships.rows.map(r => r.project_id), csrfHash: hashToken(session.csrf),
    };
    return { session: await sessionPayload(pool, auth, session.csrf), csrf: session.csrf };
  });

  app.post('/api/logout', async (req, reply) => {
    const token = parseCookies(req)[SESSION_COOKIE];
    if (token) await pool.query('UPDATE sessions SET revoked_at = now() WHERE token_hash = $1 AND revoked_at IS NULL', [hashToken(token)]);
    clearSessionCookies(reply);
    return { ok: true };
  });

  app.get('/api/session', async (req, reply) => {
    const auth = await authenticateSession(req, pool);
    const csrf = parseCookies(req)[CSRF_COOKIE];
    if (!csrf) throw new HttpError(401, 'unauthenticated', 'Session required');
    return sessionPayload(pool, auth, csrf);
  });

  app.get('/api/projects', async (req) => {
    const auth = await authenticateSession(req, pool);
    const { rows } = await pool.query(
      `SELECT id, name, budget_micros, rpm, concurrency, max_output_tokens, created_at
         FROM projects WHERE workspace_id = $1
          AND ($2 = 'admin' OR id = ANY($3::uuid[]))
        ORDER BY name`,
      [auth.workspaceId, auth.role, auth.projectIds],
    );
    return { items: rows };
  });

}
