// AI Gateway API client, CSRF/Session state & micro-USD formatters.

export const API_BASE = ((import.meta as any).env?.VITE_API_BASE_URL || '').replace(/\/$/, '');

const numFmt = new Intl.NumberFormat('zh-CN');
const dateFmt = new Intl.DateTimeFormat('zh-CN', {
  month: '2-digit',
  day: '2-digit',
  hour: '2-digit',
  minute: '2-digit',
  second: '2-digit',
  hour12: false,
});

export const fmtInt = (v: unknown): string =>
  v === null || v === undefined || v === '' || !Number.isFinite(Number(v))
    ? '—'
    : numFmt.format(Number(v));

export const fmtMoney = (v: unknown): string => {
  if (v === null || v === undefined || v === '') return '—';
  const n = Number(v);
  if (!Number.isFinite(n)) return '—';
  return '$' + (n / 1e6).toFixed(n >= 1e6 ? 2 : 6);
};

export const fmtPct = (v: unknown): string =>
  v === null || v === undefined || v === '' || !Number.isFinite(Number(v))
    ? '—'
    : (Number(v) * 100).toFixed(1) + '%';

export const fmtDate = (v: unknown): string => {
  if (!v) return '—';
  const d = new Date(String(v));
  return Number.isFinite(+d) ? dateFmt.format(d) : '—';
};

export const fmtDuration = (ms: unknown): string =>
  ms === null || ms === undefined || !Number.isFinite(Number(ms))
    ? '—'
    : Number(ms) < 1000
      ? Math.round(Number(ms)) + ' ms'
      : (Number(ms) / 1000).toFixed(2) + ' s';

export const usdToMicros = (s: string | number | null | undefined): number | null => {
  if (s === null || s === undefined || String(s).trim() === '') return null;
  const n = Number(s);
  return !Number.isFinite(n) || n < 0 ? null : Math.round(n * 1e6);
};

export const microsToUsd = (m: unknown): string =>
  m === null || m === undefined || m === ''
    ? ''
    : (Number(m) / 1e6).toFixed(6).replace(/0+$/, '').replace(/\.$/, '');

export const arr = <T = any>(v: unknown): T[] => (Array.isArray(v) ? (v as T[]) : []);

export const items = <T = any>(d: any): T[] =>
  Array.isArray(d) ? d : arr<T>(d?.items ?? d?.data ?? d?.rows);

export const statusText = (s: unknown): string => {
  const key = String(s ?? '').toLowerCase();
  const map: Record<string, string> = {
    completed: '已完成',
    success: '成功',
    succeeded: '成功',
    published: '已发布',
    active: '启用',
    enabled: '启用',
    ok: '正常',
    healthy: '正常',
    failed: '失败',
    error: '错误',
    partial: '部分响应',
    rejected: '已拒绝',
    revoked: '已吊销',
    disabled: '已禁用',
    expired: '已过期',
    draft: '草稿',
    preparing: '准备中',
    reserved: '已预留',
    dispatched: '执行中',
    cancelled: '已取消',
    canceled: '已取消',
    unknown: '未知',
    pending: '待处理',
    auth_failed: '鉴权失败',
    endpoint_not_found: '接口不存在',
    rate_limited: '上游限流',
    redirect_rejected: '拒绝重定向',
    upstream_error: '上游错误',
    timeout: '连接超时',
    network_error: '网络错误',
    invalid_url: '地址无效',
  };
  return map[key] || (s ? String(s) : '—');
};

export type BadgeTone = 'success' | 'danger' | 'warning' | 'neutral' | 'info';

export const badgeTone = (s: unknown): BadgeTone => {
  const v = String(s ?? '').toLowerCase();
  if (['completed', 'success', 'succeeded', 'published', 'active', 'enabled', 'ok', 'healthy'].includes(v)) {
    return 'success';
  }
  if (
    [
      'failed',
      'error',
      'rejected',
      'revoked',
      'disabled',
      'expired',
      'auth_failed',
      'endpoint_not_found',
      'redirect_rejected',
      'invalid_url',
      'network_error',
      'upstream_error',
      'timeout',
    ].includes(v)
  ) {
    return 'danger';
  }
  if (
    ['rate_limited', 'draft', 'preparing', 'reserved', 'dispatched', 'partial', 'unknown', 'pending', 'cancelled', 'canceled', 'expiring'].includes(
      v,
    )
  ) {
    return 'warning';
  }
  return 'neutral';
};

export const roleName = (r?: string): string =>
  ({ admin: '管理员', developer: '开发者', viewer: '只读' }[r || ''] || r || '只读');

export class ApiError extends Error {
  status: number;
  code: string;
  details?: any;
  constructor(status: number, code: string, message: string, details?: any) {
    super(message);
    this.name = 'ApiError';
    this.status = status;
    this.code = code;
    this.details = details;
  }
}

let csrfToken = '';
let lastRequestId = '';
let onUnauthorized: ((msg: string) => void) | null = null;

export function setUnauthorizedHandler(fn: ((msg: string) => void) | null) {
  onUnauthorized = fn;
}

export function setCsrfToken(token: string) {
  if (token) csrfToken = token;
}

export function getLastRequestId(): string {
  return lastRequestId;
}

export async function api<T = any>(
  path: string,
  opt: {
    method?: string;
    body?: any;
    headers?: Record<string, string>;
    silentAuth?: boolean;
  } = {},
): Promise<T> {
  const method = (opt.method || 'GET').toUpperCase();
  const headers: Record<string, string> = {
    Accept: 'application/json',
    ...(opt.headers || {}),
  };
  if (opt.body !== undefined) {
    headers['Content-Type'] = 'application/json';
  }
  if (!['GET', 'HEAD'].includes(method) && csrfToken) {
    headers['X-CSRF-Token'] = csrfToken;
  }

  let res: Response;
  try {
    res = await fetch(API_BASE + path, {
      method,
      credentials: 'same-origin',
      headers,
      body: opt.body === undefined ? undefined : JSON.stringify(opt.body),
    });
  } catch {
    throw new ApiError(0, 'network_error', '无法连接服务器，请检查网络或服务状态后重试。');
  }

  const token = res.headers.get('x-csrf-token');
  if (token) csrfToken = token;
  lastRequestId = res.headers.get('x-request-id') || lastRequestId;

  const text = await res.text();
  let data: any = null;
  if (text) {
    try {
      data = JSON.parse(text);
    } catch {
      data = { raw: text };
    }
  }

  if (!res.ok) {
    const err = data?.error;
    const message =
      err?.message ||
      (res.status === 401
        ? '会话已失效，请重新登录。'
        : res.status === 403
          ? '当前角色没有执行此操作的权限。'
          : res.status >= 500
            ? '服务暂时不可用，请稍后重试。'
            : `请求失败（HTTP ${res.status}）。`);
    if (res.status === 401 && !opt.silentAuth && onUnauthorized) {
      onUnauthorized(message);
    }
    throw new ApiError(res.status, err?.code || 'request_failed', message, err?.details);
  }
  return data as T;
}

export async function getSafe<T = any>(
  path: string,
): Promise<{ ok: true; data: T } | { ok: false; error: ApiError }> {
  try {
    return { ok: true, data: await api<T>(path) };
  } catch (e: any) {
    return { ok: false, error: e };
  }
}

export function rangeBounds(key: string): { from: string; to: string } {
  const to = new Date();
  const days = key === '7d' ? 7 : key === '30d' ? 30 : 1;
  return {
    from: new Date(to.getTime() - days * 86400000).toISOString(),
    to: to.toISOString(),
  };
}

export const isPublished = (a: any): boolean =>
  !!(
    a?.current_version_id ||
    a?.current_version?.status === 'published' ||
    a?.status === 'published' ||
    a?.published === true ||
    a?.published_at
  );

export const isKeyActive = (k: any): boolean =>
  !k?.revoked_at &&
  !k?.revokedAt &&
  (!(k?.expires_at || k?.expiresAt) || new Date(k.expires_at || k.expiresAt) > new Date());

export const targetIdsOf = (a: any): string[] => {
  const t = a?.targets || a?.current_version?.targets || a?.currentVersion?.targets || [];
  return arr(t)
    .map((x: any) => (typeof x === 'string' ? x : x?.id || x?.targetId || x?.target_id))
    .filter(Boolean);
};

export const targetLabel = (t: any, ts: any[]): string => {
  const id = t?.id || t;
  const f = arr(ts).find((x: any) => (x?.id || x) === id);
  return f ? f.upstream_model || f.upstreamModel || id : id;
};
