// AI Gateway console — native ES module, no build step.
const $ = (s, r = document) => r.querySelector(s);
const $$ = (s, r = document) => [...r.querySelectorAll(s)];
const esc = v => String(v ?? '').replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
const numFmt = new Intl.NumberFormat('zh-CN');
const dateFmt = new Intl.DateTimeFormat('zh-CN', { month: '2-digit', day: '2-digit', hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: false });
const fmtInt = v => v === null || v === undefined || v === '' ? '—' : numFmt.format(Number(v));
const fmtMoney = v => {
  if (v === null || v === undefined || v === '') return '—';
  const n = Number(v);
  if (!Number.isFinite(n)) return '—';
  return '$' + (n / 1e6).toFixed(n >= 1e6 ? 2 : 6);
};
const fmtPct = v => v === null || v === undefined || v === '' || !Number.isFinite(Number(v)) ? '—' : (Number(v) * 100).toFixed(1) + '%';
const fmtDate = v => { if (!v) return '—'; const d = new Date(v); return Number.isFinite(+d) ? dateFmt.format(d) : '—'; };
const fmtDuration = ms => ms === null || ms === undefined || !Number.isFinite(Number(ms)) ? '—' : Number(ms) < 1000 ? Math.round(ms) + ' ms' : (Number(ms) / 1000).toFixed(2) + ' s';
const barClass = pct => 'h-' + Math.min(100, Math.max(5, Math.round(Number(pct) / 5) * 5));
const usdToMicros = s => { const n = Number(s); return s === '' || !Number.isFinite(n) || n < 0 ? null : Math.round(n * 1e6); };
const microsToUsd = m => m === null || m === undefined ? '' : (Number(m) / 1e6).toFixed(6).replace(/0+$/, '').replace(/\.$/, '');
const arr = v => Array.isArray(v) ? v : [];
const items = d => Array.isArray(d) ? d : arr(d?.items ?? d?.data ?? d?.rows);
const statusText = s => ({
  completed: '已完成', success: '成功', succeeded: '成功', published: '已发布', active: '启用', enabled: '启用', ok: '正常', healthy: '正常',
  failed: '失败', error: '错误', partial: '部分响应', rejected: '已拒绝', revoked: '已吊销', disabled: '已禁用', expired: '已过期',
  draft: '草稿', preparing: '准备中', reserved: '已预留', dispatched: '执行中', cancelled: '已取消', canceled: '已取消', unknown: '未知', pending: '待处理',
  auth_failed: '鉴权失败', endpoint_not_found: '接口不存在', rate_limited: '上游限流', redirect_rejected: '拒绝重定向', upstream_error: '上游错误', timeout: '连接超时', network_error: '网络错误', invalid_url: '地址无效'
}[String(s ?? '').toLowerCase()] || (s ? String(s) : '—'));
const badgeClass = s => {
  const v = String(s ?? '').toLowerCase();
  if (['completed', 'success', 'succeeded', 'published', 'active', 'enabled', 'ok', 'healthy'].includes(v)) return 'badge--success';
  if (['failed', 'error', 'rejected', 'revoked', 'disabled', 'expired', 'auth_failed', 'endpoint_not_found', 'redirect_rejected', 'invalid_url', 'network_error', 'upstream_error', 'timeout'].includes(v)) return 'badge--danger';
  if (v === 'rate_limited') return 'badge--warning';
  if (['draft', 'preparing', 'reserved', 'dispatched', 'partial', 'unknown', 'pending', 'cancelled', 'canceled'].includes(v)) return 'badge--warning';
  return 'badge--neutral';
};
const badge = (s, label) => `<span class="badge ${badgeClass(s)}">${esc(label ?? statusText(s))}</span>`;
const errMsg = e => e?.message || '请求失败，请稍后重试。';
const escAttr = esc;

const state = { user: null, projects: [], csrf: '', projectId: null, route: null, cache: {}, toastSeq: 0, modalResolve: null };
const VIEWS = {
  overview: { title: '概览', eyebrow: '项目', lead: '' },
  providers: { title: '供应商', eyebrow: '工作空间' },
  aliases: { title: '模型别名', eyebrow: '项目' },
  keys: { title: 'API Keys', eyebrow: '项目' },
  requests: { title: '请求日志', eyebrow: '项目' },
  usage: { title: '用量', eyebrow: '项目' },
  guide: { title: '接入指南', eyebrow: '项目' },
  settings: { title: '设置', eyebrow: '项目' }
};

class ApiError extends Error {
  constructor(status, code, message, details) { super(message); this.name = 'ApiError'; this.status = status; this.code = code; this.details = details; }
}
async function api(path, opt = {}) {
  const method = (opt.method || 'GET').toUpperCase();
  const headers = { Accept: 'application/json', ...(opt.headers || {}) };
  if (opt.body !== undefined) headers['Content-Type'] = 'application/json';
  if (!['GET', 'HEAD'].includes(method) && state.csrf) headers['X-CSRF-Token'] = state.csrf;
  let res;
  try {
    res = await fetch(path, { method, credentials: 'same-origin', headers, body: opt.body === undefined ? undefined : JSON.stringify(opt.body) });
  } catch {
    throw new ApiError(0, 'network_error', '无法连接服务器，请检查网络或服务状态后重试。');
  }
  const token = res.headers.get('x-csrf-token');
  if (token) state.csrf = token;
  state.lastRequestId = res.headers.get('x-request-id') || state.lastRequestId;
  const text = await res.text();
  let data = null;
  if (text) { try { data = JSON.parse(text); } catch { data = { raw: text }; } }
  if (!res.ok) {
    const err = data?.error;
    const message = err?.message || (res.status === 401 ? '会话已失效，请重新登录。' : res.status === 403 ? '当前角色没有执行此操作的权限。' : res.status >= 500 ? '服务暂时不可用，请稍后重试。' : `请求失败（HTTP ${res.status}）。`);
    if (res.status === 401 && !opt.silentAuth) lostSession(message);
    throw new ApiError(res.status, err?.code || 'request_failed', message, err?.details);
  }
  return data;
}

function applySession(raw) {
  const s = raw?.session && typeof raw.session === 'object' ? raw.session : (raw?.data && typeof raw.data === 'object' ? raw.data : raw) || {};
  const user = s.user || raw?.user || {};
  state.user = { ...user };
  const list = s.projects || raw?.projects || user.projects || [];
  const projectIds = user.projectIds || user.project_ids || s.projectIds || s.project_ids || [];
  state.projects = arr(list).map(p => typeof p === 'string' ? { id: p, name: p, role: user.role } : p)
    .map(p => ({ id: p.id || p.projectId || p.project_id, name: p.name || p.projectName || p.project_name || '未命名项目', role: p.role || p.projectRole || user.role || 'viewer' }))
    .filter(p => p.id);
  if (!state.projects.length && arr(projectIds).length) state.projects = projectIds.map(id => ({ id, name: id, role: user.role }));
  state.csrf = s.csrf || s.csrfToken || s.csrf_token || raw?.csrf || raw?.csrfToken || raw?.csrf_token || state.csrf;
  const saved = localStorage.getItem('ag.project');
  const preferred = s.project?.id || raw?.project?.id || saved;
  state.projectId = state.projects.some(p => p.id === preferred) ? preferred : state.projects[0]?.id || null;
  if (state.projectId) localStorage.setItem('ag.project', state.projectId);
}
function currentProject() { return state.projects.find(p => p.id === state.projectId) || state.projects[0] || null; }
function role() { return currentProject()?.role || state.user?.role || 'viewer'; }
function canWrite() { return ['admin', 'developer'].includes(role()); }
function isAdmin() { return role() === 'admin'; }
function lostSession(message) {
  state.user = null; state.projects = []; state.csrf = '';
  $('#app-shell').hidden = true; $('#auth-view').hidden = false; $('#boot-screen').hidden = true;
  showAuth('login'); authError('login', message || '会话已失效，请重新登录。');
}

function authError(which, message) {
  const el = $('#' + which + '-error'); if (!el) return;
  el.textContent = message || ''; el.hidden = !message;
}
function showAuth(mode = 'bootstrap') {
  $('#boot-screen').hidden = true; $('#app-shell').hidden = true; $('#auth-view').hidden = false;
  setAuthTab(mode);
  if (mode === 'accept') { const t = new URLSearchParams(location.hash.split('?')[1] || '').get('token'); if (t) $('#accept-token').value = t; }
  const focus = mode === 'bootstrap' ? '#bootstrap-email' : mode === 'accept' ? ($('#accept-token').value ? '#accept-password' : '#accept-token') : '#login-email';
  setTimeout(() => $(focus)?.focus(), 30);
}
function setAuthTab(mode) {
  const modes = ['bootstrap', 'login', 'accept'];
  modes.forEach(m => { const on = m === mode; $('#tab-' + m).classList.toggle('is-active', on); $('#tab-' + m).setAttribute('aria-selected', String(on)); $('#' + m + '-panel').hidden = !on; });
}
function showBootError(message) {
  const card = $('#boot-screen .boot-card');
  if (!card) return showAuth('login');
  card.innerHTML = `<div class="brand-mark brand-mark--large" aria-hidden="true">AG</div>
    <p class="boot-title">无法连接控制台</p><p class="boot-note">${esc(message)}</p>
    <button type="button" class="btn btn-primary mt-16" data-action="retry-boot">重试</button>`;
}
async function loadSession(silent = false) {
  const data = await api('/api/session', { silentAuth: true });
  applySession(data);
  if (!state.projects.length && !state.user?.id) throw new Error('会话响应缺少用户信息。');
  return data;
}

const q = p => encodeURIComponent(p);
const projectQuery = extra => new URLSearchParams({ project_id: state.projectId || '', ...extra }).toString();
function routeNow() {
  const raw = location.hash.replace(/^#\/?/, '');
  const [name, qs = ''] = raw.split('?');
  const params = new URLSearchParams(qs);
  return { name: VIEWS[name] ? name : 'overview', params };
}
function setRoute(name, params = {}) {
  const sp = new URLSearchParams(params);
  if (!sp.has('project_id') && state.projectId) sp.set('project_id', state.projectId);
  location.hash = '#/' + (VIEWS[name] ? name : 'overview') + '?' + sp.toString();
}
function keepRouteParams() { return new URLSearchParams(state.route?.params || ''); }
function updateHashParams(patch) {
  const sp = keepRouteParams(); Object.entries(patch).forEach(([k, v]) => v === null || v === undefined || v === '' ? sp.delete(k) : sp.set(k, v));
  location.hash = '#/' + routeNow().name + '?' + sp.toString();
}
// UI primitives and shell routing.
const loading = (label = '正在读取数据…') => `<div class="loading-state" role="status"><div class="skeleton"></div><div class="skeleton skeleton--row"></div><div class="skeleton skeleton--row"></div><p class="loading-text"><span class="spinner" aria-hidden="true"></span>${esc(label)}</p></div>`;
const errorBox = (message, retry = true) => `<div class="error-state" role="alert"><h3>数据读取失败</h3><p>${esc(message)}</p>${retry ? '<button type="button" class="btn btn-secondary btn-small" data-action="retry-view">重新加载</button>' : ''}</div>`;
const emptyBox = (title, text, action = '') => `<div class="empty-state"><div class="empty-icon" aria-hidden="true">◇</div><h3>${esc(title)}</h3><p>${esc(text)}</p>${action}</div>`;
const pageHeader = (title, text, actions = '') => `<div class="page-header"><div><h2>${esc(title)}</h2>${text ? `<p>${esc(text)}</p>` : ''}</div>${actions ? `<div class="page-actions">${actions}</div>` : ''}</div>`;
const metric = (label, value, unit = '', note = '') => `<div class="metric-card"><div class="metric-label">${esc(label)}</div><div class="metric-value">${value}${unit ? `<span class="metric-unit">${esc(unit)}</span>` : ''}</div>${note ? `<div class="metric-note">${esc(note)}</div>` : ''}</div>`;
const roleName = r => ({ admin: '管理员', developer: '开发者', viewer: '只读' }[r] || r || '只读');
function setStale(message) {
  const b = $('#stale-banner'); if (!b) return;
  b.innerHTML = `<span>${esc(message)} 当前显示的是上一次成功读取的数据。</span><button type="button" data-action="retry-view">重试</button>`;
  b.hidden = false;
}
function clearStale() { const b = $('#stale-banner'); if (b) { b.hidden = true; b.textContent = ''; } }
function toast(message, kind = 'info') {
  const id = 'toast-' + (++state.toastSeq);
  const t = document.createElement('div');
  t.className = 'toast is-' + kind; t.id = id; t.setAttribute('role', kind === 'error' ? 'alert' : 'status');
  t.innerHTML = `<span>${esc(message)}</span><button type="button" aria-label="关闭通知" data-action="dismiss-toast" data-target="${id}">✕</button>`;
  $('#toast-region').appendChild(t);
  setTimeout(() => t.remove(), kind === 'error' ? 8000 : 4500);
}
function setBusy(btn, busy, label) {
  if (!btn) return;
  if (busy) { btn.dataset.original = btn.innerHTML; btn.disabled = true; btn.innerHTML = `<span class="spinner-inline" aria-hidden="true"></span>${esc(label || '处理中…')}`; }
  else { btn.disabled = false; if (btn.dataset.original) btn.innerHTML = btn.dataset.original; }
}
function fieldError(form, name, message) {
  const el = form.querySelector(`[data-error-for="${name}"]`);
  if (el) { el.textContent = message || ''; el.hidden = !message; }
  const input = form.elements.namedItem(name);
  if (input) input.setAttribute('aria-invalid', message ? 'true' : 'false');
}
function formValues(form) { return Object.fromEntries(new FormData(form).entries()); }
function openModal({ title, body, footer = '', wide = false }) {
  const modal = $('#modal'); if (!modal) return;
  state.modalResolve = null;
  modal.classList.toggle('modal--wide', !!wide);
  $('#modal-title').textContent = title;
  $('#modal-body').innerHTML = body;
  $('#modal-footer').innerHTML = footer;
  $('#modal-backdrop').hidden = false;
  document.body.classList.add('modal-open');
  state.lastFocus = document.activeElement;
  setTimeout(() => { const target = modal.querySelector('input:not([type=hidden]), select, textarea, button'); (target || modal).focus(); }, 30);
}
function closeModal(result = false) {
  if ($('#modal-backdrop').hidden) return;
  $('#modal-backdrop').hidden = true;
  $('#modal-body').replaceChildren();
  $('#modal-footer').replaceChildren();
  document.body.classList.remove('modal-open');
  const resolve = state.modalResolve; state.modalResolve = null;
  if (typeof resolve === 'function') resolve(result);
  if (state.lastFocus?.focus) state.lastFocus.focus();
}
function confirmModal({ title, message, confirmLabel = '确认', danger = false }) {
  return new Promise(resolve => {
    openModal({
      title, body: `<p class="page-lead">${esc(message)}</p>`,
      footer: `<button type="button" class="btn btn-secondary" data-action="modal-cancel">取消</button><button type="button" class="btn ${danger ? 'btn-danger' : 'btn-primary'}" data-action="modal-confirm">${esc(confirmLabel)}</button>`
    });
    state.modalResolve = resolve;
  });
}
async function copyText(text, btn) {
  try {
    if (navigator.clipboard?.writeText) await navigator.clipboard.writeText(text);
    else {
      const ta = document.createElement('textarea'); ta.value = text; ta.className = 'visually-hidden';
      document.body.appendChild(ta); ta.select(); document.execCommand('copy'); ta.remove();
    }
    toast('已复制到剪贴板。', 'success'); if (btn) { const old = btn.textContent; btn.textContent = '已复制'; setTimeout(() => btn.textContent = old, 1600); }
  } catch { toast('复制失败，请手动选择文本后复制。', 'error'); }
}

function renderShell() {
  const u = state.user || {};
  const email = u.email || '未知用户';
  $('#user-email').textContent = email;
  $('#user-role').textContent = `${roleName(role())} · ${currentProject() ? '项目成员' : '无项目'}`;
  $('#user-avatar').textContent = email.slice(0, 1);
  const sel = $('#project-select'); sel.innerHTML = '';
  if (!state.projects.length) sel.innerHTML = '<option value="">无可用项目</option>';
  else for (const p of state.projects) {
    const o = document.createElement('option'); o.value = p.id; o.textContent = p.name; o.selected = p.id === state.projectId; sel.appendChild(o);
  }
  sel.disabled = !state.projects.length;
  $('#footer-scope').textContent = '项目范围：' + (currentProject()?.name || '无');
  $('#workspace-name').textContent = u.workspaceName || u.workspace_name || '当前部署';
}
function startApp() {
  $('#boot-screen').hidden = true; $('#auth-view').hidden = true; $('#app-shell').hidden = false;
  renderShell();
  if (!state.route) state.route = routeNow();
  router();
}
async function router() {
  const r = routeNow(); state.route = r;
  const valid = r.params.get('project_id');
  if (valid && state.projects.some(p => p.id === valid)) {
    if (valid !== state.projectId) { state.projectId = valid; localStorage.setItem('ag.project', valid); renderShell(); }
  } else if (state.projectId) {
    r.params.set('project_id', state.projectId);
    history.replaceState(null, '', '#/' + r.name + '?' + r.params.toString());
  }
  const meta = VIEWS[r.name];
  $('#page-title').textContent = meta.title; $('#page-eyebrow').textContent = meta.eyebrow;
  $$('.nav-item').forEach(a => { const on = a.dataset.nav === r.name; a.classList.toggle('is-active', on); if (on) a.setAttribute('aria-current', 'page'); else a.removeAttribute('aria-current'); });
  $$('[data-view-panel]').forEach(p => p.hidden = p.dataset.viewPanel !== r.name);
  closeSidebar();
  await loadCurrent();
}
async function loadCurrent(force = false) {
  const { name } = state.route || { name: 'overview' };
  const panel = $(`[data-view-panel="${name}"]`);
  if (!panel) return;
  if (!state.projectId) { panel.innerHTML = emptyBox('没有可访问的项目', '当前账号尚未加入任何项目。请让管理员在服务端完成成员与项目授权。'); panel.dataset.loaded = ''; return; }
  if (!panel.dataset.loaded) panel.innerHTML = loading();
  const token = ++state.loadToken;
  try {
    await RENDER[name](panel, state.route.params, force);
    if (token !== state.loadToken) return;
    panel.dataset.loaded = '1'; clearStale();
  } catch (e) {
    if (e.status === 401) return;
    if (token !== state.loadToken) return;
    if (panel.dataset.loaded) setStale(errMsg(e));
    else panel.innerHTML = errorBox(errMsg(e));
  }
}
function closeSidebar() {
  $('#sidebar').classList.remove('is-open'); $('#sidebar-backdrop').hidden = true;
  $('#menu-button').setAttribute('aria-expanded', 'false');
}
function openSidebar() {
  $('#sidebar').classList.add('is-open'); $('#sidebar-backdrop').hidden = false;
  $('#menu-button').setAttribute('aria-expanded', 'true');
  setTimeout(() => $('.nav-item', $('#sidebar'))?.focus(), 30);
}
const RENDER = {};
const actions = {};

async function doLogout() {
  try { await api('/api/logout', { method: 'POST' }); } catch { /* clear locally even if the server session already expired */ }
  localStorage.removeItem('ag.project');
  state.user = null; state.projects = []; state.csrf = ''; state.projectId = null; state.route = null;
  $$('[data-view-panel]').forEach(p => { p.dataset.loaded = ''; p.innerHTML = ''; });
  $('#app-shell').hidden = true; $('#auth-view').hidden = false; $('#boot-screen').hidden = true;
  setAuthTab('login'); toast('已退出登录。');
}
async function submitAuth(form, kind) {
  const data = formValues(form);
  const submit = form.querySelector('[type=submit]');
  if (kind === 'bootstrap') {
    const emailOk = /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(data.email || '');
    if (!emailOk) { fieldError(form, 'email', '请输入有效邮箱地址。'); form.elements.email.focus(); return; }
    if ((data.password || '').length < 10) { fieldError(form, 'password', '密码至少 10 个字符。'); form.elements.password.focus(); return; }
    if (!String(data.projectName || '').trim()) { fieldError(form, 'projectName', '请输入项目名称。'); form.elements.projectName.focus(); return; }
  } else {
    if (!(data.email || '').trim()) { fieldError(form, 'email', '请输入邮箱地址。'); return; }
    if (!(data.password || '')) { fieldError(form, 'password', '请输入密码。'); return; }
  }
  authError(kind, '');
  setBusy(submit, true, kind === 'bootstrap' ? '正在创建…' : '正在登录…');
  try {
    const path = kind === 'bootstrap' ? '/api/bootstrap' : '/api/login';
    const body = kind === 'bootstrap' ? { email: data.email.trim(), password: data.password, projectName: data.projectName.trim() } : { email: data.email.trim(), password: data.password };
    const res = await api(path, { method: 'POST', body, silentAuth: true });
    applySession(res);
    if (!state.projects.length && res?.project?.id) { state.projects = [{ id: res.project.id, name: res.project.name, role: res.project.role || state.user?.role }]; state.projectId = res.project.id; }
    if (!state.projects.length) throw new Error('登录成功，但当前账号没有可访问的项目。请联系管理员。');
    form.reset(); startApp(); toast(kind === 'bootstrap' ? '管理员与首个项目已创建。' : '登录成功。', 'success');
  } catch (e) {
    if (kind === 'login' && e.status === 401) {
      try { await loadSession(true); form.reset(); startApp(); toast('登录成功。', 'success'); return; } catch { /* credentials were actually rejected */ }
    }
    authError(kind, errMsg(e));
    if (e.status === 409) setAuthTab('login');
  } finally { setBusy(submit, false); }
}

async function submitAccept(form) {
  const v = formValues(form); const submit = form.querySelector('[type=submit]');
  if (!String(v.token || '').trim()) return fieldError(form, 'token', '请粘贴邀请令牌。');
  if ((v.password || '').length < 10) return fieldError(form, 'password', '密码至少 10 个字符。');
  if (v.password !== v.confirm) return fieldError(form, 'confirm', '两次输入的密码不一致。');
  authError('accept', ''); setBusy(submit, true, '正在接受…');
  try {
    const res = await api('/api/invitations/accept', { method: 'POST', body: { token: v.token.trim(), password: v.password }, silentAuth: true });
    applySession(res); form.reset(); history.replaceState(null, '', location.pathname); startApp(); toast('邀请已接受，已登录工作空间。', 'success');
  } catch (e) { authError('accept', errMsg(e)); } finally { setBusy(submit, false); }
}

async function init() {
  $('#tab-bootstrap').addEventListener('click', () => setAuthTab('bootstrap'));
  $('#tab-login').addEventListener('click', () => setAuthTab('login'));
  $('#tab-accept').addEventListener('click', () => showAuth('accept'));
  $('#bootstrap-form').addEventListener('submit', ev => { ev.preventDefault(); submitAuth(ev.currentTarget, 'bootstrap'); });
  $('#login-form').addEventListener('submit', ev => { ev.preventDefault(); submitAuth(ev.currentTarget, 'login'); });
  $('#accept-form').addEventListener('submit', ev => { ev.preventDefault(); submitAccept(ev.currentTarget); });
  $('#project-select').addEventListener('change', ev => {
    state.projectId = ev.target.value || null;
    if (state.projectId) localStorage.setItem('ag.project', state.projectId);
    $$('[data-view-panel]').forEach(p => { p.dataset.loaded = ''; p.innerHTML = ''; });
    const r = routeNow(); const sp = new URLSearchParams(r.params); sp.set('project_id', state.projectId || ''); location.hash = '#/' + r.name + '?' + sp.toString();
    renderShell();
  });
  $('#refresh-button').addEventListener('click', () => loadCurrent(true));
  $('#menu-button').addEventListener('click', () => $('#sidebar').classList.contains('is-open') ? closeSidebar() : openSidebar());
  $('#sidebar-backdrop').addEventListener('click', closeSidebar);
  $('#logout-button').addEventListener('click', async () => { if (await confirmModal({ title: '退出登录', message: '将结束当前浏览器会话。未提交的表单内容会丢失。', confirmLabel: '退出登录' })) doLogout(); });
  $('#modal-close').addEventListener('click', () => closeModal(false));
  $('#modal-backdrop').addEventListener('click', ev => { if (ev.target === $('#modal-backdrop')) closeModal(false); });
  window.addEventListener('hashchange', router);
  document.addEventListener('submit', ev => {
    const form = ev.target;
    if (form.dataset.form && typeof form.dataset.form === 'string') { ev.preventDefault(); actions['submit:' + form.dataset.form]?.(form); }
  });
  document.addEventListener('click', async ev => {
    const btn = ev.target.closest('[data-action]'); if (!btn) return;
    const action = btn.dataset.action;
    if (action === 'retry-boot') { ev.preventDefault(); $('#boot-screen').hidden = false; $('#auth-view').hidden = true; boot(); return; }
    if (action === 'dismiss-toast') { $('#' + btn.dataset.target)?.remove(); return; }
    if (action === 'retry-view') { ev.preventDefault(); loadCurrent(true); return; }
    if (action === 'modal-cancel') return closeModal(false);
    if (action === 'modal-confirm') return closeModal(true);
    if (actions[action]) { ev.preventDefault(); await actions[action](btn, ev); }
  });
  document.addEventListener('keydown', ev => {
    if (ev.key === 'Escape') { if (!$('#modal-backdrop').hidden) closeModal(false); else closeSidebar(); }
    if (ev.key === 'Tab' && !$('#modal-backdrop').hidden) {
      const f = $$('button, [href], input:not([type=hidden]), select, textarea, [tabindex]:not([tabindex="-1"])', $('#modal')).filter(x => !x.disabled && x.offsetParent !== null);
      if (!f.length) return;
      const first = f[0], last = f[f.length - 1];
      if (ev.shiftKey && document.activeElement === first) { ev.preventDefault(); last.focus(); }
      else if (!ev.shiftKey && document.activeElement === last) { ev.preventDefault(); first.focus(); }
    }
  });
  await boot();
}
async function boot() {
  try {
    await loadSession(true);
    if (location.hash.startsWith('#/accept')) { showAuth('accept'); return; }
    if (!state.projects.length) { if (state.user?.id) { startApp(); return; } showAuth('bootstrap'); return; }
    startApp();
  } catch (e) {
    if (e.status === 401) showAuth(location.hash.startsWith('#/accept') ? 'accept' : 'bootstrap');
    else if (e.status) showBootError(errMsg(e));
    else showBootError(errMsg(e));
  }
}
init();

// Shared loaders.
async function getSafe(path) { try { return { ok: true, data: await api(path) }; } catch (e) { return { ok: false, error: e }; } }
async function apiFirst(paths, opt) {
  let last;
  for (const p of paths) { try { return await api(p, opt); } catch (e) { last = e; if (e.status !== 404 && e.status !== 405) throw e; } }
  throw last;
}
function rangeBounds(key) {
  const to = new Date(); const days = key === '7d' ? 7 : key === '30d' ? 30 : 1;
  return { from: new Date(to.getTime() - days * 86400000).toISOString(), to: to.toISOString() };
}
const rangeChips = current => `<div class="inline-actions" role="group" aria-label="时间范围">${[['24h', '最近 24 小时'], ['7d', '最近 7 天'], ['30d', '最近 30 天']].map(([k, l]) => `<button type="button" class="btn btn-small ${current === k ? 'btn-primary' : 'btn-secondary'}" data-action="set-range" data-range="${k}">${l}</button>`).join('')}</div>`;
const isPublished = a => !!(a.current_version_id || a.current_version?.status === 'published' || a.status === 'published' || a.published === true || a.published_at);
const isKeyActive = k => !k.revoked_at && !k.revokedAt && (!(k.expires_at || k.expiresAt) || new Date(k.expires_at || k.expiresAt) > new Date());
const targetIdsOf = a => { const t = a.targets || a.current_version?.targets || a.currentVersion?.targets || []; return arr(t).map(x => typeof x === 'string' ? x : x.id || x.targetId || x.target_id).filter(Boolean); };
const targetLabel = (t, ts) => { const id = t.id || t; const f = ts.find(x => (x.id || x) === id); return f ? (f.upstream_model || f.upstreamModel || id) : id; };
const requestRow = r => {
  const dur = r.duration_ms ?? r.durationMs ?? (r.updated_at && r.created_at ? new Date(r.updated_at) - new Date(r.created_at) : null);
  return `<tr><td class="nowrap">${fmtDate(r.created_at || r.createdAt)}</td><td class="mono-cell"><span class="truncate" title="${escAttr(r.request_id || r.id || '')}">${esc(r.request_id || r.id || '—')}</span></td><td>${esc(r.alias_name || r.aliasName || r.model || '—')}</td><td>${badge(r.status, statusText(r.status))}</td><td class="num">${fmtDuration(dur)}</td><td class="num">${fmtInt(r.input_tokens ?? r.inputTokens)} / ${fmtInt(r.output_tokens ?? r.outputTokens)}</td><td class="num">${r.actual_micros !== null && r.actual_micros !== undefined ? fmtMoney(r.actual_micros) : r.reserve_micros !== null && r.reserve_micros !== undefined ? '预估 ' + fmtMoney(r.reserve_micros) : '—'}</td><td class="row-actions"><button type="button" class="btn btn-small btn-secondary" data-action="request-detail" data-id="${escAttr(r.request_id || r.id || '')}">详情</button></td></tr>`;
};

RENDER.overview = async panel => {
  const range = state.route.params.get('range') || '24h'; const b = rangeBounds(range); const qs = projectQuery({ from: b.from, to: b.to });
  const [u, rq, pr, al, ky] = await Promise.all([getSafe('/api/usage/summary?' + qs), getSafe('/api/requests?' + projectQuery({ from: b.from, to: b.to, limit: 5 })), getSafe('/api/providers'), getSafe('/api/aliases?' + projectQuery()), getSafe('/api/keys?' + projectQuery())]);
  if (!u.ok && !rq.ok && !pr.ok && !al.ok && !ky.ok) throw u.error;
  const usage = u.data || {}; const recent = items(rq.data); const providers = items(pr.data); const aliases = items(al.data); const keys = items(ky.data);
  state.cache.providers = providers; state.cache.targets = []; state.cache.aliases = aliases; state.cache.keys = keys;
  const reqs = Number(usage.requests || 0), success = Number(usage.successful_requests ?? usage.successfulRequests ?? NaN);
  const rate = Number.isFinite(success) && reqs > 0 ? success / reqs : null;
  const actual = usage.actual_micros ?? usage.actualMicros; const estimated = usage.estimated_micros ?? usage.estimatedMicros; const reserved = usage.budget?.reserved_micros ?? usage.reserved_micros ?? usage.reservedMicros;
  const unknown = usage.unknown_requests ?? usage.unknownRequests;
  const project = usage.project || {}; const budget = project.budget_micros ?? project.budgetMicros ?? currentProject()?.budget_micros ?? currentProject()?.budgetMicros;
  const spent = Number(usage.budget?.spent_micros ?? (Number(actual || 0) + Number(estimated || 0))); const pct = budget ? Math.min(100, spent / Number(budget) * 100) : 0;
  const steps = [
    { done: providers.length > 0, title: '连接供应商', note: providers.length ? `已配置 ${providers.length} 个连接` : '添加经过验证的 OpenAI 兼容服务', href: '#/providers', cta: '添加供应商' },
    { done: aliases.some(isPublished), title: '发布模型别名', note: aliases.some(isPublished) ? `${aliases.filter(isPublished).length} 个别名已发布` : '为目标模型定义稳定的项目别名', href: '#/aliases', cta: '配置别名' },
    { done: keys.some(isKeyActive), title: '创建密钥并调用', note: keys.some(isKeyActive) ? `${keys.filter(isKeyActive).length} 个密钥有效` : '创建项目密钥，再从接入指南发送测试', href: '#/keys', cta: '创建密钥' }
  ];
  const complete = steps.every(s => s.done);
  const setup = complete ? `<div class="callout">接入闭环已完成。新的调用会出现在请求日志与用量页；费用未知的请求会单独标记。</div>` : `<section class="setup-card"><div class="setup-head"><div><h2>完成首次接入</h2><p>按顺序连接供应商、发布模型别名、创建项目密钥。任何一步完成后可离开并稍后继续。</p></div><span class="badge badge--info badge--plain">${steps.filter(s => s.done).length}/3 已完成</span></div><div class="setup-steps">${steps.map((s, i) => `<div class="setup-step ${s.done ? 'is-done' : ''}"><span class="setup-index" aria-hidden="true">${s.done ? '✓' : i + 1}</span><div><h3>${esc(s.title)}</h3><p>${esc(s.note)}</p><a href="${s.href}">${esc(s.done ? '查看配置' : s.cta)}</a></div></div>`).join('')}</div></section>`;
  const series = arr(usage.series || usage.items); const maxReq = Math.max(1, ...series.map(s => Number(s.event_count ?? s.requests ?? 0)));
  const chart = series.length ? `<div class="bar-chart" role="img" aria-label="请求量趋势，附下方数据摘要">${series.slice(-48).map(s => `<div class="bar-col"><div class="bar-track"><div class="bar-fill ${barClass(Number(s.event_count ?? s.requests ?? 0) / maxReq * 100)}"></div></div><span class="bar-label">${fmtDate(s.bucket || s.hour || s.created_at).slice(0, 5)}</span></div>`).join('')}</div><p class="chart-caption">按小时聚合，共 ${series.length} 个时间桶；悬停标签受空间限制，完整数据可在用量页查看。</p>` : `<div class="empty-state"><div class="empty-icon" aria-hidden="true">▤</div><h3>尚无调用趋势</h3><p>当前时间范围内没有已记录请求。发送一次测试请求后，真实数据会出现在这里。</p><a class="btn btn-secondary btn-small" href="#/guide">前往接入指南</a></div>`;
  const warnings = [u, rq, pr, al, ky].filter(x => !x.ok).map(x => x.error.message);
  panel.innerHTML = `${warnings.length ? `<div class="callout callout--warning" role="status">部分数据读取失败：${esc(warnings[0])}。其余卡片仅展示已成功读取的数据。</div>` : ''}
    <div class="page-header"><div><h2>概览</h2><p>项目 ${esc(currentProject()?.name || '')} 的真实请求、费用与接入状态。</p></div><div class="page-actions">${rangeChips(range)}</div></div>${setup}
    <div class="metric-grid">${metric('请求数', fmtInt(reqs), '', `${range === '24h' ? '最近 24 小时' : range === '7d' ? '最近 7 天' : '最近 30 天'} · 已进入执行阶段`)}${metric('成功率', rate === null ? '—' : fmtPct(rate), '', rate === null ? '暂无已鉴权请求' : `${fmtInt(success)} 次完整成功 / ${fmtInt(reqs)} 次请求`)}${metric('未知用量请求', fmtInt(unknown), '次', 'usage 缺失，未按 0 费用计算')}${metric('估算费用', fmtMoney(actual), '', actual === null || actual === undefined ? '暂无已结算费用' : `另有预估 ${fmtMoney(estimated)}`)}</div>
    <div class="grid-2"><section class="card"><div class="card-header"><div><h3>请求趋势</h3><p>数据时间：${usage.updated_at || usage.updatedAt ? fmtDate(usage.updated_at || usage.updatedAt) : '尚未产生汇总'}</p></div>${usage.stale ? '<span class="badge badge--warning">数据更新延迟</span>' : ''}</div><div class="card-body">${chart}</div></section>
    <section class="card"><div class="card-header"><h3>预算使用</h3><a class="btn btn-small btn-secondary" href="#/usage">查看用量</a></div><div class="card-body"><div class="budget-block">${budget === null || budget === undefined ? `<p class="muted-panel">尚未设置项目月度预算。预算控制关闭时，请求仍受频率与并发限制。</p>` : `<div class="budget-top"><span class="budget-amount">${fmtMoney(spent)}</span><span class="budget-limit">/ ${fmtMoney(budget)}</span></div><progress class="progress ${pct >= 90 ? 'is-danger' : pct >= 70 ? 'is-warning' : ''}" max="100" value="${pct}" aria-label="预算使用率"></progress><p class="small muted">${pct.toFixed(1)}% · 已结算 ${fmtMoney(actual)} · 待核对预估 ${fmtMoney(estimated)}</p>`}<div class="legend"><span><i></i>已结算</span><span><i class="is-estimated"></i>预估/待核对</span><span><i class="is-unknown"></i>未知用量 ${fmtInt(unknown)}</span></div>${reserved !== null && reserved !== undefined ? `<p class="small muted">当前未结算预留：${fmtMoney(reserved)}</p>` : ''}</div></div></section></div>
    <section class="card"><div class="card-header"><div><h3>最近请求</h3><p>仅元数据，不包含提示词或回答。</p></div><a class="btn btn-small btn-secondary" href="#/requests">全部请求</a></div><div class="card-body card-body--flush">${recent.length ? `<div class="table-wrap"><table><thead><tr><th>时间</th><th>请求 ID</th><th>模型</th><th>状态</th><th class="num">耗时</th><th class="num">输入 / 输出 token</th><th class="num">费用</th><th></th></tr></thead><tbody>${recent.map(requestRow).join('')}</tbody></table></div>` : emptyBox('尚无调用记录', '发送一次真实测试请求后，这里会显示请求 ID、状态、耗时与计量结果。', '<a class="btn btn-secondary btn-small" href="#/guide">发送测试请求</a>')}</div>${rq.ok && recent.length ? `<div class="card-footer"><span class="small muted">显示最近 ${recent.length} 条；费用未知时显示预估或破折号。</span></div>` : ''}</section>`;
};

// Providers and model targets.
async function loadTargets(providers = null) {
  const ps = providers || items(await api('/api/providers'));
  const groups = await Promise.all(ps.map(async p => {
    try { const d = await api('/api/providers/' + q(p.id) + '/targets'); return items(d).map(t => ({ ...t, provider_id: t.provider_id || p.id, upstream_model: t.upstream_model || t.upstreamModel })); }
    catch (e) { if (e.status === 404 || e.status === 403) return arr(p.targets).map(t => ({ ...t, provider_id: p.id })); throw e; }
  }));
  return groups.flat();
}
function providerById(id) { return arr(state.cache.providers).find(p => p.id === id); }
function providerStatus(p) { return p.enabled === false ? 'disabled' : (p.last_test_status || 'pending'); }
RENDER.providers = async panel => {
  const pd = await getSafe('/api/providers');
  if (!pd.ok && pd.error.status !== 403) throw pd.error;
  const providers = items(pd.data); state.cache.providers = providers;
  const targets = pd.ok ? await loadTargets(providers).catch(() => []) : [];
  state.cache.targets = targets;
  const actionsHtml = isAdmin() ? `<button type="button" class="btn btn-primary" data-action="provider-new">添加供应商</button>` : `<span class="role-note">仅管理员可管理供应商与模型目标</span>`;
  const rows = providers.map(p => `<tr><td><span class="cell-primary">${esc(p.name)}</span><span class="cell-sub">${esc(p.protocol || 'openai')}</span></td><td class="mono-cell break-all">${esc(p.base_url || p.baseUrl || '—')}</td><td>${p.enabled === false ? badge('disabled', '已禁用') : badge(providerStatus(p), p.last_test_status ? statusText(p.last_test_status) : '未测试')}${p.last_test_at || p.lastTestAt ? `<span class="cell-sub">${fmtDate(p.last_test_at || p.lastTestAt)}</span>` : ''}</td><td class="num">${fmtInt(targets.filter(t => (t.provider_id || t.providerId) === p.id).length)}</td><td class="row-actions">${isAdmin() ? `<button type="button" class="btn btn-small btn-secondary" data-action="provider-test" data-id="${escAttr(p.id)}">测试</button><button type="button" class="btn btn-small btn-secondary" data-action="target-new" data-id="${escAttr(p.id)}">添加模型</button><button type="button" class="btn btn-small btn-secondary" data-action="provider-edit" data-id="${escAttr(p.id)}">编辑</button>` : '<span class="muted small">只读</span>'}</td></tr>`).join('');
  const targetRows = targets.map(t => { const p = providers.find(x => x.id === (t.provider_id || t.providerId)); return `<tr><td><span class="cell-primary">${esc(t.upstream_model || t.upstreamModel || t.model || '—')}</span><span class="cell-sub mono-cell">${esc(t.id || '')}</span></td><td>${esc(p?.name || t.provider_name || t.providerName || '—')}</td><td class="num">${fmtMoney(t.input_price ?? t.inputPrice)}</td><td class="num">${fmtMoney(t.output_price ?? t.outputPrice)}</td><td>${arr(t.capabilities).length ? arr(t.capabilities).map(c => `<span class="badge badge--neutral badge--plain">${esc(c)}</span>`).join(' ') : '<span class="muted">未声明</span>'}</td><td>${t.enabled === false ? badge('disabled') : badge('enabled')}</td></tr>`; }).join('');
  panel.innerHTML = `${pageHeader('供应商', '工作空间级连接配置。凭据加密保存且不会回显；最近测试通过不代表持续健康。', actionsHtml)}
    ${!pd.ok ? emptyBox('没有查看供应商的权限', '供应商与模型目标仅对管理员和开发者开放。当前角色可查看项目级别名、密钥与请求数据。') : ''}
    <section class="card"><div class="card-header"><div><h3>连接</h3><p>协议目前仅支持 OpenAI Chat Completions 兼容子集。自定义地址由服务端做 SSRF 校验。</p></div><span class="badge badge--neutral badge--plain">${providers.length} 个连接</span></div><div class="card-body card-body--flush">${providers.length ? `<div class="table-wrap"><table><thead><tr><th>名称 / 协议</th><th>服务地址</th><th>状态</th><th class="num">模型目标</th><th></th></tr></thead><tbody>${rows}</tbody></table></div>` : emptyBox('尚未连接供应商', '添加一个经过验证的上游服务后，才能创建模型目标与项目别名。', isAdmin() ? '<button type="button" class="btn btn-primary btn-small" data-action="provider-new">添加第一个供应商</button>' : '')}</div></section>
    <section class="card"><div class="card-header"><div><h3>模型目标</h3><p>价格按每百万 token 的微美元记录。未配置价格的模型不能启用预算控制。</p></div>${isAdmin() && providers.length ? '<button type="button" class="btn btn-small btn-secondary" data-action="target-new">添加模型目标</button>' : ''}</div><div class="card-body card-body--flush">${targets.length ? `<div class="table-wrap"><table><thead><tr><th>上游模型</th><th>供应商</th><th class="num">输入价格 / 1M</th><th class="num">输出价格 / 1M</th><th>能力</th><th>状态</th></tr></thead><tbody>${targetRows}</tbody></table></div>` : emptyBox('尚无模型目标', '先在供应商下登记实际模型标识与整数微美元价格，再创建路由别名。', isAdmin() && providers.length ? '<button type="button" class="btn btn-secondary btn-small" data-action="target-new">添加模型目标</button>' : '')}</div></section>
    <div class="callout callout--warning">连接测试可能产生上游费用。测试只返回阶段、时间与安全错误摘要，不展示上游原始敏感响应。</div>`;
};
function providerForm(p = {}) {
  return `<form data-form="provider" data-id="${escAttr(p.id || '')}" class="form-grid"><div class="field"><label for="provider-name">连接名称</label><input id="provider-name" name="name" required maxlength="100" value="${escAttr(p.name || '')}" placeholder="例如：主服务商"></div><div class="form-row"><div class="field"><label for="provider-protocol">协议</label><select id="provider-protocol" name="protocol"><option value="openai" selected>OpenAI Chat Completions</option></select><p class="field-hint">首版只验证该协议子集，不宣称全部参数兼容。</p></div><div class="field"><label for="provider-enabled">状态</label><select id="provider-enabled" name="enabled"><option value="true" ${p.enabled !== false ? 'selected' : ''}>启用</option><option value="false" ${p.enabled === false ? 'selected' : ''}>禁用</option></select></div></div><div class="field"><label for="provider-url">服务地址</label><input id="provider-url" name="baseUrl" type="url" required value="${escAttr(p.base_url || p.baseUrl || '')}" placeholder="https://api.example.com/v1"><p class="field-hint">默认要求 HTTPS；环回、私网与云元数据地址会被服务端拒绝。</p></div><div class="field"><label for="provider-secret">${p.id ? '替换 API 密钥（留空不修改）' : 'API 密钥'}</label><input id="provider-secret" name="apiKey" type="password" autocomplete="new-password" ${p.id ? '' : 'required'} placeholder="只写入，保存后不可读回"><p class="field-hint">浏览器不会持久化该值；提交后仅由服务端加密保存。</p></div><p class="form-error" data-error-for="form" hidden></p><div class="form-actions"><button type="button" class="btn btn-secondary" data-action="modal-cancel">取消</button><button type="submit" class="btn btn-primary">${p.id ? '保存更改' : '保存供应商'}</button></div></form>`;
}
function targetForm(providerId = '') {
  const opts = arr(state.cache.providers).map(p => `<option value="${escAttr(p.id)}" ${p.id === providerId ? 'selected' : ''}>${esc(p.name)}</option>`).join('');
  return `<form data-form="target" class="form-grid"><div class="field"><label for="target-provider">供应商</label><select id="target-provider" name="providerId" required>${opts}</select></div><div class="field"><label for="target-model">上游模型标识</label><input id="target-model" name="upstreamModel" required placeholder="例如：gpt-4.1-mini"><p class="field-hint">必须与供应商实际接受的 model 字段一致。</p></div><div class="form-row"><div class="field"><label for="target-input">输入价格（USD / 1M token）</label><input id="target-input" name="inputPrice" type="number" min="0" step="0.000001" required placeholder="0.40"></div><div class="field"><label for="target-output">输出价格（USD / 1M token）</label><input id="target-output" name="outputPrice" type="number" min="0" step="0.000001" required placeholder="1.60"></div></div><div class="field"><label for="target-caps">能力声明（可选，逗号分隔）</label><input id="target-caps" name="capabilities" placeholder="chat,stream"><p class="field-hint">只声明已验证能力；不会自动继承供应商的所有参数。</p></div><p class="form-error" data-error-for="form" hidden></p><div class="form-actions"><button type="button" class="btn btn-secondary" data-action="modal-cancel">取消</button><button type="submit" class="btn btn-primary">保存模型目标</button></div></form>`;
}
actions['provider-new'] = () => openModal({ title: '添加供应商连接', body: providerForm(), wide: true });
actions['provider-edit'] = btn => { const p = providerById(btn.dataset.id); if (p) openModal({ title: '编辑供应商连接', body: providerForm(p), wide: true }); };
actions['target-new'] = btn => { if (!arr(state.cache.providers).length) return toast('请先添加供应商。', 'error'); openModal({ title: '添加模型目标', body: targetForm(btn.dataset.id || ''), wide: true }); };
actions['submit:provider'] = async form => {
  const v = formValues(form); const submit = form.querySelector('[type=submit]'); const id = form.dataset.id;
  if (!v.name.trim() || !v.baseUrl.trim() || (!id && !v.apiKey)) return fieldError(form, 'form', '请填写名称、服务地址和 API 密钥。');
  setBusy(submit, true, '保存中…');
  try {
    const body = { name: v.name.trim(), protocol: v.protocol, base_url: v.baseUrl.trim(), enabled: v.enabled === 'true' };
    if (v.apiKey) body.secret = v.apiKey;
    if (id) await api('/api/providers/' + q(id), { method: 'PATCH', body }); else await api('/api/providers', { method: 'POST', body });
    closeModal(); toast(id ? '供应商已更新。' : '供应商已添加。', 'success'); await loadCurrent(true);
  } catch (e) { fieldError(form, 'form', errMsg(e)); } finally { setBusy(submit, false); }
};
actions['submit:target'] = async form => {
  const v = formValues(form); const submit = form.querySelector('[type=submit]');
  const input = usdToMicros(v.inputPrice), output = usdToMicros(v.outputPrice);
  if (!v.providerId || !v.upstreamModel.trim() || input === null || output === null) return fieldError(form, 'form', '请填写供应商、模型标识和非负价格。');
  setBusy(submit, true, '保存中…');
  try {
    const body = { upstream_model: v.upstreamModel.trim(), input_price: input, output_price: output, capabilities: v.capabilities.split(',').map(x => x.trim()).filter(Boolean), enabled: true };
    await apiFirst(['/api/providers/' + q(v.providerId) + '/targets', '/api/targets', '/api/model-targets'], { method: 'POST', body });
    closeModal(); toast('模型目标已添加。', 'success'); await loadCurrent(true);
  } catch (e) { fieldError(form, 'form', errMsg(e)); } finally { setBusy(submit, false); }
};
actions['provider-test'] = async btn => {
  const p = providerById(btn.dataset.id); if (!p) return;
  if (!await confirmModal({ title: '测试连接', message: `将向“${p.name}”的服务地址发起一次真实连通性测试，可能产生上游费用。继续吗？`, confirmLabel: '开始测试' })) return;
  setBusy(btn, true, '测试中…');
  try {
    const r = await api('/api/providers/' + q(p.id) + '/test', { method: 'POST' });
    const ok = r.ok !== false && !['failed', 'error'].includes(String(r.status || '').toLowerCase());
    openModal({ title: '连接测试结果', body: `<p>${badge(ok ? 'success' : 'failed', ok ? '测试通过' : '测试失败')}</p><dl class="detail-grid"><div class="detail-item"><dt>时间</dt><dd>${fmtDate(r.testedAt || r.tested_at || new Date())}</dd></div><div class="detail-item"><dt>状态</dt><dd>${esc(r.message || statusText(r.status) || (ok ? '连接成功' : '连接失败'))}</dd></div>${r.latency_ms != null ? `<div class="detail-item"><dt>延迟</dt><dd>${fmtDuration(r.latency_ms)}</dd></div>` : ''}${r.stage ? `<div class="detail-item"><dt>阶段</dt><dd>${esc(r.stage)}</dd></div>` : ''}</dl><p class="help-text">最近测试通过只代表该时刻连通，不代表持续健康。</p>`, footer: '<button type="button" class="btn btn-primary" data-action="modal-cancel">关闭</button>' });
    await loadCurrent(true);
  } catch (e) { toast(errMsg(e), 'error'); } finally { setBusy(btn, false); }
};

// Project model aliases.
async function loadAliases() {
  return items(await apiFirst(['/api/aliases?' + projectQuery(), '/api/model-aliases?' + projectQuery()]));
}
RENDER.aliases = async panel => {
  const ad = await getSafe('/api/aliases?' + projectQuery());
  let aliases = ad.ok ? items(ad.data) : [];
  if (!ad.ok && (ad.error.status === 404 || ad.error.status === 405)) { const alt = await getSafe('/api/model-aliases?' + projectQuery()); if (alt.ok) { aliases = items(alt.data); ad.ok = true; } }
  if (!ad.ok) throw ad.error;
  let targets = await loadTargets().catch(() => []);
  const embedded = aliases.flatMap(a => arr(a.target_details || a.targetDetails)); if (embedded.length) targets = [...targets, ...embedded].filter((t, i, all) => t?.id && all.findIndex(x => x.id === t.id) === i);
  state.cache.aliases = aliases; state.cache.targets = targets;
  const rows = aliases.map(a => {
    const ids = targetIdsOf(a), hasDraft = !!a.draft_version, hasCurrent = !!a.current_version_id;
    const version = a.draft_version?.version ?? a.current_version?.version ?? a.current_version ?? a.version ?? '—';
    const status = hasDraft ? 'draft' : hasCurrent ? 'published' : 'empty';
    const statusLabel = hasDraft && hasCurrent ? '已发布 · 有草稿' : hasDraft ? '草稿' : hasCurrent ? '已发布' : '未发布';
    const actions = isAdmin() ? `<button type="button" class="btn btn-small btn-secondary" data-action="alias-version" data-id="${escAttr(a.id)}">${hasDraft ? '编辑草稿' : '新版本'}</button>${hasDraft ? `<button type="button" class="btn btn-small btn-primary" data-action="alias-publish" data-id="${escAttr(a.id)}">发布草稿</button>` : ''}` : '<span class="muted small">只读</span>';
    return `<tr><td><span class="cell-primary">${esc(a.name)}</span><span class="cell-sub mono-cell">${esc(a.id)}</span></td><td>${ids.length ? ids.map(id => `<span class="badge badge--neutral badge--plain">${esc(targetLabel(id, targets))}</span>`).join(' ') : '<span class="muted">无目标</span>'}</td><td class="num">${esc(String(version))}</td><td>${badge(status, statusLabel)}${hasDraft ? '<span class="cell-sub">待发布</span>' : ''}</td><td class="row-actions">${actions}</td></tr>`;
  }).join('');
  panel.innerHTML = `${pageHeader('模型别名', '项目内稳定的模型名，映射到一个主目标。发布后的配置版本不可变；编辑会创建新草稿版本，已有请求继续使用原版本。', isAdmin() ? '<button type="button" class="btn btn-primary" data-action="alias-new">新建别名</button>' : '<span class="role-note">只有管理员可发布路由配置</span>')}
    ${!targets.length ? `<div class="callout callout--warning">尚未配置模型目标。请先在“供应商”页添加上游模型，才能发布别名。<a href="#/providers">前往供应商</a></div>` : ''}
    <section class="card"><div class="card-header"><div><h3>项目别名</h3><p>别名只对当前项目生效；API 请求中的 model 使用该名称。每个版本最多两个目标。</p></div><span class="badge badge--neutral badge--plain">${aliases.length} 个</span></div><div class="card-body card-body--flush">${aliases.length ? `<div class="table-wrap"><table><thead><tr><th>别名</th><th>目标模型</th><th class="num">版本</th><th>状态</th><th></th></tr></thead><tbody>${rows}</tbody></table></div>` : emptyBox('尚未创建模型别名', '从一个或多个已验证模型目标创建别名，发布后即可用项目密钥调用。', isAdmin() && targets.length ? '<button type="button" class="btn btn-primary btn-small" data-action="alias-new">新建别名</button>' : '')}</div></section>
    <div class="callout">主备切换只在请求尚未发送或上游明确拒绝执行的限流场景发生；首个内容片段发出后不会切换。目标顺序按列表顺序提交，最多两个。</div>`;
};
function aliasForm(alias = null) {
  const selected = new Set(alias ? targetIdsOf(alias) : []);
  const selectedCaps = new Set(arr(alias?.capabilities));
  const capUnion = [...new Set([...selectedCaps, ...state.cache.targets.flatMap(t => arr(t.capabilities))])];
  const options = state.cache.targets.map(t => { const p = providerById(t.provider_id || t.providerId); return `<label class="checkbox-field"><input type="checkbox" name="targetIds" value="${escAttr(t.id)}" ${selected.has(t.id) ? 'checked' : ''}><span><strong>${esc(t.upstream_model || t.upstreamModel || t.model)}</strong><span class="cell-sub">${esc(p?.name || '未知供应商')} · 输入 ${fmtMoney(t.input_price ?? t.inputPrice)} / 输出 ${fmtMoney(t.output_price ?? t.outputPrice)}</span></span></label>`; }).join('');
  const caps = capUnion.length ? capUnion.map(c => `<label class="checkbox-field"><input type="checkbox" name="capabilities" value="${escAttr(c)}" ${selectedCaps.has(c) ? 'checked' : ''}><span>${esc(c)}</span></label>`).join('') : '<p class="muted">目标尚未声明能力；可先发布基础文本聊天。</p>';
  const editing = !!alias;
  return `<form data-form="alias" data-mode="${editing ? 'version' : 'create'}" data-id="${escAttr(alias?.id || '')}" class="form-grid"><div class="field"><label for="alias-name">别名</label><input id="alias-name" name="name" required maxlength="80" pattern="[A-Za-z0-9._-]+" value="${escAttr(alias?.name || '')}" ${editing ? 'readonly' : ''} placeholder="例如：support-chat"><p class="field-hint">${editing ? '版本编辑不修改别名；保存后生成新的草稿版本。' : '项目内唯一，建议使用小写字母、数字、点、下划线或连字符。'}</p></div><fieldset class="field"><legend>目标模型（最多 2 个）</legend><div class="stack-sm">${options || '<p class="muted">没有可用模型目标。</p>'}</div><p class="field-hint">所有已选目标必须覆盖下方声明的能力；未声明能力不会被自动支持。</p></fieldset><fieldset class="field"><legend>声明能力（可选）</legend><div class="stack-sm">${caps}</div></fieldset><p class="form-error" data-error-for="form" hidden></p><div class="form-actions"><button type="button" class="btn btn-secondary" data-action="modal-cancel">取消</button><button type="submit" class="btn btn-primary" ${options ? '' : 'disabled'}>${editing ? '保存为新草稿' : '创建草稿'}</button></div></form>`;
}
actions['alias-new'] = () => openModal({ title: '新建模型别名', body: aliasForm(), wide: true });
actions['alias-version'] = btn => { const a = arr(state.cache.aliases).find(x => x.id === btn.dataset.id); if (a) openModal({ title: `编辑草稿：${a.name}`, body: aliasForm(a), wide: true }); };
actions['submit:alias'] = async form => {
  const v = formValues(form); const ids = new FormData(form).getAll('targetIds').filter(Boolean); const caps = new FormData(form).getAll('capabilities').filter(Boolean); const submit = form.querySelector('[type=submit]');
  if (!v.name.trim() || ids.length < 1) return fieldError(form, 'form', '请填写别名并至少选择一个模型目标。');
  if (ids.length > 2) return fieldError(form, 'form', '每个路由版本最多选择两个目标。');
  const mode = form.dataset.mode || 'create'; const aliasId = form.dataset.id;
  setBusy(submit, true, '保存中…');
  try {
    if (mode === 'version') await api('/api/aliases/' + q(aliasId) + '/versions', { method: 'POST', body: { targets: ids, capabilities: caps } });
    else await api('/api/aliases', { method: 'POST', body: { project_id: state.projectId, name: v.name.trim(), targets: ids, capabilities: caps } });
    closeModal(); toast(mode === 'version' ? '新草稿版本已创建，检查后发布。' : '别名草稿已创建。发布后才能用于调用。', 'success'); await loadCurrent(true);
  } catch (e) { fieldError(form, 'form', errMsg(e)); } finally { setBusy(submit, false); }
};
actions['alias-publish'] = async btn => {
  const a = arr(state.cache.aliases).find(x => x.id === btn.dataset.id); if (!a) return;
  if (!await confirmModal({ title: '发布别名', message: `发布“${a.name}”的最新草稿？发布后新请求使用该版本，已有请求保持原配置。`, confirmLabel: '发布' })) return;
  setBusy(btn, true, '发布中…');
  try {
    await api('/api/aliases/' + q(a.id) + '/publish', { method: 'POST', body: { expectedVersion: a.draft_version?.version ?? a.current_version?.version ?? a.version ?? null } });
    toast('别名已发布。', 'success'); await loadCurrent(true);
  } catch (e) { toast(e.status === 409 ? '发布冲突：配置已被其他会话更新，请刷新后重新检查。' : errMsg(e), 'error'); if (e.status === 409) await loadCurrent(true); } finally { setBusy(btn, false); }
};

// API keys.
RENDER.keys = async panel => {
  const [kd, ad] = await Promise.all([getSafe('/api/keys?' + projectQuery()), getSafe('/api/aliases?' + projectQuery())]);
  if (!kd.ok) throw kd.error;
  const keys = items(kd.data); const aliases = ad.ok ? items(ad.data) : []; state.cache.keys = keys; state.cache.aliases = aliases;
  const active = keys.filter(isKeyActive).length;
  const rows = keys.map(k => {
    const status = !isKeyActive(k) ? (k.revoked_at || k.revokedAt ? 'revoked' : 'expired') : (k.expires_at || k.expiresAt) && new Date(k.expires_at || k.expiresAt) - Date.now() < 7 * 86400000 ? 'expiring' : 'active';
    return `<tr><td><span class="cell-primary">${esc(k.name)}</span><span class="cell-sub mono-cell">${esc(k.prefix || '—')}…</span></td><td>${arr(k.allowed_models ?? k.allowedModels).length ? arr(k.allowed_models ?? k.allowedModels).map(m => `<span class="badge badge--neutral badge--plain">${esc(m)}</span>`).join(' ') : '<span class="muted">未限制 / 默认</span>'}</td><td>${status === 'expiring' ? badge('unknown', '即将到期') : badge(status === 'active' ? 'enabled' : status)}</td><td class="nowrap">${k.expires_at || k.expiresAt ? fmtDate(k.expires_at || k.expiresAt) : '不过期'}</td><td class="nowrap">${fmtDate(k.last_used_at || k.lastUsedAt)}</td><td class="row-actions">${isKeyActive(k) && canWrite() ? `<button type="button" class="btn btn-small btn-secondary" data-action="key-revoke" data-id="${escAttr(k.id)}">吊销</button>` : '<span class="muted small">只读</span>'}</td></tr>`;
  }).join('');
  panel.innerHTML = `${pageHeader('API Keys', '项目密钥用于数据面鉴权。完整密钥只展示一次，列表仅保存不可逆摘要与公开前缀。', canWrite() ? '<button type="button" class="btn btn-primary" data-action="key-new">创建密钥</button>' : '<span class="role-note">只读角色不能创建或吊销密钥</span>')}
    <div class="metric-grid">${metric('有效密钥', fmtInt(active), '个', `当前项目共 ${keys.length} 个`)}${metric('最近使用', fmtDate(keys.map(k => k.last_used_at || k.lastUsedAt).filter(Boolean).sort().at(-1)), '', '按最近一次成功鉴权记录')}${metric('设置到期', fmtInt(keys.filter(k => k.expires_at || k.expiresAt).length), '个', '含过期时间的密钥')}${metric('吊销', fmtInt(keys.filter(k => k.revoked_at || k.revokedAt).length), '个', '吊销后立即阻止新请求')}</div>
    <section class="card"><div class="card-header"><div><h3>项目密钥</h3><p>密钥状态以服务端权威存储为准；禁用后不依赖浏览器缓存。</p></div></div><div class="card-body card-body--flush">${keys.length ? `<div class="table-wrap"><table><thead><tr><th>名称 / 前缀</th><th>允许模型</th><th>状态</th><th>到期时间</th><th>最近使用</th><th></th></tr></thead><tbody>${rows}</tbody></table></div>` : emptyBox('尚未创建项目密钥', '创建后请立即保存完整密钥；控制台无法再次读取。建议为每个应用使用独立密钥。', canWrite() ? '<button type="button" class="btn btn-primary btn-small" data-action="key-new">创建第一个密钥</button>' : '')}</div></section>
    <div class="callout callout--warning">轮换顺序：创建替代密钥 → 更新应用配置 → 确认新密钥已有调用 → 吊销旧密钥。吊销会阻止后续请求，但不保证终止已发出的请求。</div>`;
};
function keyForm() {
  const models = state.cache.aliases.filter(isPublished).map(a => `<label class="checkbox-field"><input type="checkbox" name="allowedModels" value="${escAttr(a.name)}"><span>${esc(a.name)}</span></label>`).join('');
  return `<form data-form="key" class="form-grid"><div class="field"><label for="key-name">密钥名称</label><input id="key-name" name="name" required maxlength="80" placeholder="例如：support-api-prod"><p class="field-hint">用于区分应用或环境，不会影响鉴权。</p></div><div class="field"><label for="key-expiry">有效期（天）</label><input id="key-expiry" name="expiresInDays" type="number" min="1" max="365" value="30" required><p class="field-hint">默认 30 天；到期后服务端拒绝新的鉴权请求。</p></div><fieldset class="field"><legend>允许模型（可不选）</legend><div class="stack-sm">${models || '<p class="muted">尚无已发布别名。可先创建密钥，受服务端默认范围约束。</p>'}</div></fieldset><p class="form-error" data-error-for="form" hidden></p><div class="form-actions"><button type="button" class="btn btn-secondary" data-action="modal-cancel">取消</button><button type="submit" class="btn btn-primary">创建密钥</button></div></form>`;
}
actions['key-new'] = () => openModal({ title: '创建项目密钥', body: keyForm(), wide: true });
actions['submit:key'] = async form => {
  const v = formValues(form); const models = new FormData(form).getAll('allowedModels'); const submit = form.querySelector('[type=submit]');
  if (!v.name.trim()) return fieldError(form, 'form', '请输入密钥名称。');
  const days = Number(v.expiresInDays); if (!Number.isInteger(days) || days < 1 || days > 365) return fieldError(form, 'form', '有效期需为 1–365 天。');
  setBusy(submit, true, '创建中…');
  try {
    const body = { projectId: state.projectId, project_id: state.projectId, name: v.name.trim(), allowedModels: models, allowed_models: models, expiresInDays: days, expires_at: new Date(Date.now() + days * 86400000).toISOString() };
    const r = await api('/api/keys', { method: 'POST', body }); const secret = r.key || r.secret || r.apiKey;
    closeModal();
    openModal({ title: '密钥已创建', body: `<p class="callout callout--warning">完整密钥只显示这一次。关闭后无法再次读取，请立即保存到安全的密钥管理系统。</p><div class="copy-row"><code class="secret-key" id="new-key-value">${esc(secret || '服务端未返回完整密钥')}</code>${secret ? '<button type="button" class="btn btn-secondary" data-action="copy-new-key">复制</button>' : ''}</div><dl class="detail-grid"><div class="detail-item"><dt>名称</dt><dd>${esc(v.name.trim())}</dd></div><div class="detail-item"><dt>前缀</dt><dd class="mono">${esc(r.prefix || '—')}</dd></div><div class="detail-item"><dt>到期时间</dt><dd>${fmtDate(r.expires_at || r.expiresAt || new Date(Date.now() + days * 86400000))}</dd></div></dl>`, footer: '<button type="button" class="btn btn-primary" data-action="modal-cancel">我已保存，关闭</button>' });
    await loadCurrent(true);
  } catch (e) { fieldError(form, 'form', errMsg(e)); } finally { setBusy(submit, false); }
};
actions['copy-new-key'] = btn => copyText($('#new-key-value')?.textContent || '', btn);
actions['key-revoke'] = async btn => {
  const k = arr(state.cache.keys).find(x => x.id === btn.dataset.id); if (!k) return;
  if (!await confirmModal({ title: '吊销 API Key', message: `吊销“${k.name}”（${k.prefix || ''}…）？后续请求会被拒绝，已发出的请求不会因此停止。`, confirmLabel: '吊销密钥', danger: true })) return;
  setBusy(btn, true, '吊销中…');
  try { try { await api('/api/keys/' + q(k.id) + '/revoke', { method: 'POST' }); } catch (e) { if (e.status !== 404 && e.status !== 405) throw e; await api('/api/keys/' + q(k.id), { method: 'DELETE' }); } toast('密钥已吊销。', 'success'); await loadCurrent(true); }
  catch (e) { toast(errMsg(e), 'error'); } finally { setBusy(btn, false); }
};

// Request log and detail timeline.
function requestFilters() {
  const p = state.route.params; const range = p.get('range') || '24h'; const status = p.get('status') || ''; const model = p.get('model') || '';
  const b = rangeBounds(range); return { range, status, model, from: b.from, to: b.to };
}
RENDER.requests = async panel => {
  const f = requestFilters(); const qs = projectQuery({ from: f.from, to: f.to, limit: 50, status: f.status, model: f.model });
  const r = await api('/api/requests?' + qs); state.cache.reqQuery = qs; state.cache.reqNext = r.next_cursor || null;
  const rows = items(r); const hasFilter = !!(f.status || f.model);
  const html = rows.length ? `<div class="table-wrap"><table><thead><tr><th>时间</th><th>请求 ID</th><th>模型</th><th>状态</th><th class="num">耗时</th><th class="num">输入 / 输出 token</th><th class="num">费用</th><th></th></tr></thead><tbody id="request-rows">${rows.map(requestRow).join('')}</tbody></table></div>` : emptyBox(hasFilter ? '没有符合条件的请求' : '尚无请求记录', hasFilter ? '调整筛选条件，或选择更长的时间范围。' : '从接入指南发送一次测试请求，即可在这里看到真实元数据。', !hasFilter ? '<a class="btn btn-secondary btn-small" href="#/guide">发送测试请求</a>' : '');
  panel.innerHTML = `${pageHeader('请求日志', '按项目查看请求元数据。时间范围与筛选保存在 URL；不保存提示词、回答或鉴权头。', rangeChips(f.range))}
    <section class="card"><div class="card-header"><div><h3>筛选</h3><p>服务端按半开时间区间查询，默认最近 24 小时。</p></div></div><div class="card-body"><form data-form="request-filter" class="form-row"><div class="field"><label for="req-status">状态</label><select id="req-status" name="status"><option value="">全部状态</option>${['completed', 'failed', 'rejected', 'partial', 'cancelled', 'unknown'].map(s => `<option value="${s}" ${f.status === s ? 'selected' : ''}>${statusText(s)}</option>`).join('')}</select></div><div class="field"><label for="req-model">模型别名</label><input id="req-model" name="model" value="${escAttr(f.model)}" placeholder="例如：support-chat"></div><div class="field"><label for="req-range">时间范围</label><select id="req-range" name="range">${[['24h', '最近 24 小时'], ['7d', '最近 7 天'], ['30d', '最近 30 天']].map(([k, l]) => `<option value="${k}" ${f.range === k ? 'selected' : ''}>${l}</option>`).join('')}</select></div><div class="field field--end"><button type="submit" class="btn btn-primary">应用筛选</button></div></form></div></section>
    <section class="card"><div class="card-header"><div><h3>请求</h3><p>${rows.length} 条${state.cache.reqNext ? '，可继续加载' : '，已到末页'}</p></div></div><div class="card-body card-body--flush">${html}</div>${state.cache.reqNext ? `<div class="card-footer"><span class="small muted">游标分页，按创建时间倒序。</span><button type="button" class="btn btn-secondary btn-small" data-action="requests-more" data-cursor="${escAttr(state.cache.reqNext)}">加载更多</button></div>` : ''}</section>
    <div class="callout">请求详情按“尝试”展示时间线。流式请求在首个内容片段后中断会标记为“部分响应”，不会被计为完整成功。</div>`;
};
actions['submit:request-filter'] = form => {
  const v = formValues(form); updateHashParams({ range: v.range || '24h', status: v.status || null, model: v.model.trim() || null });
};
actions['requests-more'] = async btn => {
  const cursor = btn.dataset.cursor; if (!cursor || !state.cache.reqQuery) return;
  setBusy(btn, true, '加载中…');
  try {
    const r = await api('/api/requests?' + state.cache.reqQuery + '&cursor=' + q(cursor)); const rows = items(r);
    $('#request-rows')?.insertAdjacentHTML('beforeend', rows.map(requestRow).join(''));
    state.cache.reqNext = r.next_cursor || null;
    btn.closest('.card-footer')?.remove();
    if (state.cache.reqNext) { const foot = document.createElement('div'); foot.className = 'card-footer'; foot.innerHTML = `<span class="small muted">已加载 ${$('#request-rows')?.children.length || 0} 条。</span><button type="button" class="btn btn-secondary btn-small" data-action="requests-more" data-cursor="${escAttr(state.cache.reqNext)}">加载更多</button>`; $('#view-requests .card:last-of-type')?.appendChild(foot); }
    toast(rows.length ? `已加载 ${rows.length} 条请求。` : '没有更多请求。', 'info');
  } catch (e) { toast(errMsg(e), 'error'); } finally { setBusy(btn, false); }
};
actions['request-detail'] = async btn => {
  const id = btn.dataset.id; if (!id) return;
  openModal({ title: '请求详情', body: loading('正在读取请求与尝试记录…'), wide: true });
  try {
    const r = await api('/api/requests/' + q(id)); const attempts = arr(r.attempts);
    const dur = r.duration_ms ?? r.durationMs ?? (r.updated_at && r.created_at ? new Date(r.updated_at) - new Date(r.created_at) : null);
    const targetName = t => arr(state.cache.targets).find(x => x.id === t)?.upstream_model || t || '—';
    const timeline = attempts.length ? `<ol class="timeline">${attempts.map(a => {
      const s = String(a.status || '').toLowerCase(); const ok = ['completed', 'success', 'succeeded', 'ok'].includes(s); const dot = ok ? 'is-ok' : ['failed', 'error', 'rejected'].includes(s) ? 'is-failed' : '';
      return `<li class="timeline-item"><span class="timeline-dot ${dot}" aria-hidden="true"></span><div class="timeline-head"><strong>尝试 ${fmtInt(a.attempt_no)}</strong>${badge(a.status, statusText(a.status))}${a.error_code ? `<span class="badge badge--danger badge--plain">${esc(a.error_code)}</span>` : ''}</div><div class="timeline-meta"><span>目标：${esc(targetName(a.target_id))}</span><span>开始：${fmtDate(a.started_at)}</span><span>耗时：${a.finished_at ? fmtDuration(new Date(a.finished_at) - new Date(a.started_at)) : '—'}</span><span>token：${fmtInt(a.input_tokens)} / ${fmtInt(a.output_tokens)}</span><span>费用：${fmtMoney(a.cost_micros)}</span></div>${a.error_code ? `<p class="timeline-note">错误码：${esc(a.error_code)}。请结合请求 ID 与服务端日志排查，不展示上游原始响应。</p>` : ''}</li>`;
    }).join('')}</ol>` : `<p class="muted-panel">尚无上游尝试记录。该请求可能在准入或鉴权阶段结束，未发送到上游。</p>`;
    const unknown = (r.actual_micros === null || r.actual_micros === undefined) && !['preparing', 'reserved', 'rejected', 'cancelled'].includes(String(r.status).toLowerCase());
    $('#modal-body').innerHTML = `<div class="inline-actions">${badge(r.status, statusText(r.status))}${r.error_code ? `<span class="badge badge--danger badge--plain">${esc(r.error_code)}</span>` : ''}${unknown ? '<span class="badge badge--warning">用量未知</span>' : ''}</div><dl class="detail-grid"><div class="detail-item detail-item--full"><dt>请求 ID</dt><dd class="mono">${esc(r.request_id || id)}</dd></div><div class="detail-item"><dt>模型别名</dt><dd>${esc(r.alias_name || '—')}</dd></div><div class="detail-item"><dt>状态</dt><dd>${esc(statusText(r.status))}</dd></div><div class="detail-item"><dt>创建时间</dt><dd>${fmtDate(r.created_at)}</dd></div><div class="detail-item"><dt>更新时间</dt><dd>${fmtDate(r.updated_at)}</dd></div><div class="detail-item"><dt>耗时</dt><dd>${fmtDuration(dur)}</dd></div><div class="detail-item"><dt>输入 / 输出 token</dt><dd>${fmtInt(r.input_tokens)} / ${fmtInt(r.output_tokens)}</dd></div><div class="detail-item"><dt>实际费用 / 预留</dt><dd>${r.actual_micros !== null && r.actual_micros !== undefined ? fmtMoney(r.actual_micros) : '未知（不按 0 计）'} · ${fmtMoney(r.reserve_micros)}</dd></div><div class="detail-item"><dt>密钥 ID</dt><dd class="mono">${esc(r.key_id || '—')}</dd></div><div class="detail-item"><dt>路由版本</dt><dd class="mono break-all">${esc(typeof r.route_version === 'object' ? JSON.stringify(r.route_version) : r.route_version || '—')}</dd></div></dl><h3 class="mt-16">尝试时间线</h3>${timeline}<p class="help-text mt-16">此详情只包含请求元数据、尝试与计量结果；网关不存储提示词或模型回答。</p>`;
    $('#modal-title').textContent = '请求详情';
    $('#modal-footer').innerHTML = '<button type="button" class="btn btn-secondary" data-action="modal-cancel">关闭</button>';
  } catch (e) { $('#modal-body').innerHTML = errorBox(errMsg(e), false); }
};

// Usage, budget and reconciliation state.
RENDER.usage = async panel => {
  const range = state.route.params.get('range') || '24h'; const b = rangeBounds(range);
  const u = await api('/api/usage?' + projectQuery({ from: b.from, to: b.to }));
  const project = u.project || currentProject() || {}; const budget = project.budget_micros ?? project.budgetMicros ?? null;
  const actual = u.actual_micros ?? u.actualMicros ?? u.summary?.actual_micros; const estimated = u.estimated_micros ?? u.estimatedMicros ?? u.summary?.estimated_micros;
  const reserved = u.budget?.reserved_micros ?? u.reserved_micros ?? u.reservedMicros; const unknown = u.unknown_requests ?? u.unknownRequests; const reqs = u.requests ?? u.summary?.requests; const success = u.successful_requests ?? u.successfulRequests ?? u.summary?.successful_requests;
  const rate = Number(success) >= 0 && Number(reqs) > 0 ? Number(success) / Number(reqs) : null; const spent = Number(u.budget?.spent_micros ?? (Number(actual || 0) + Number(estimated || 0))); const pct = budget ? Math.min(100, spent / Number(budget) * 100) : 0;
  const series = arr(u.series || u.items); const maxEvents = Math.max(1, ...series.map(x => Number(x.event_count ?? x.requests ?? 0)));
  const chart = series.length ? `<div class="bar-chart" role="img" aria-label="按小时请求量趋势">${series.slice(-48).map(x => `<div class="bar-col"><div class="bar-track"><div class="bar-fill ${barClass(Number(x.event_count ?? x.requests ?? 0) / maxEvents * 100)}"></div></div><span class="bar-label">${fmtDate(x.bucket).slice(0, 5)}</span></div>`).join('')}</div><p class="chart-caption">最近 ${Math.min(48, series.length)} 个时间桶，按事件数显示；费用与 token 汇总见下方。</p>` : `<div class="empty-state"><div class="empty-icon" aria-hidden="true">▤</div><h3>暂无聚合数据</h3><p>当前范围内没有账本事件。用量不是实时账单，最终以供应商账单为准。</p></div>`;
  const rows = series.slice().reverse().map(x => `<tr><td>${fmtDate(x.bucket)}</td><td class="num">${fmtInt(x.event_count)}</td><td class="num">${fmtMoney(x.actual_micros)}</td><td class="num">${fmtMoney(x.estimated_micros)}</td><td class="num">${fmtInt(x.input_tokens)}</td><td class="num">${fmtInt(x.output_tokens)}</td></tr>`).join('');
  panel.innerHTML = `${pageHeader('用量', '已结算、估算与未知用量分开呈现。预算按 UTC 月度统计；费用为网关估算口径，不是财务结算依据。', rangeChips(range))}
    ${u.stale ? '<div class="callout callout--warning">统计正在后台同步，数据可能短暂延迟；预算限制按实时使用量执行。</div>' : ''}
    <div class="metric-grid">${metric('已结算费用', fmtMoney(actual), '', '来自实际 usage 的账本事件')}${metric('预估 / 待核对', fmtMoney(estimated), '', `${fmtInt(unknown)} 次请求用量未知`)}${metric('请求数', fmtInt(reqs), '次', `完整成功 ${fmtInt(success)} 次`)}${metric('成功率', rate === null ? '—' : fmtPct(rate), '', rate === null ? '暂无已鉴权请求' : '完整响应才算成功')}</div>
    <div class="grid-2"><section class="card"><div class="card-header"><div><h3>预算</h3><p>数据时间：${u.updated_at || u.updatedAt ? fmtDate(u.updated_at || u.updatedAt) : '尚无聚合更新'}</p></div></div><div class="card-body"><div class="budget-block">${budget === null || budget === undefined ? '<p class="muted-panel">项目尚未设置月度预算。预算控制关闭时，仍执行频率与并发限制。</p>' : `<div class="budget-top"><span class="budget-amount">${fmtMoney(spent)}</span><span class="budget-limit">/ ${fmtMoney(budget)}</span></div><progress class="progress ${pct >= 90 ? 'is-danger' : pct >= 70 ? 'is-warning' : ''}" max="100" value="${pct}" aria-label="预算使用率"></progress><p class="small muted">${pct.toFixed(1)}% · UTC 本月累计（含估算）${fmtMoney(spent)}</p>`}<p class="small muted">未结算预留：${fmtMoney(reserved)}。硬上限只约束网关估算口径，不保证与供应商账单一致。</p>${isAdmin() ? `<form data-form="budget" class="inline-actions"><div class="field field--grow"><label for="budget-usd">月度预算（USD）</label><input id="budget-usd" name="budget" type="number" min="0" step="0.000001" value="${escAttr(microsToUsd(budget))}" placeholder="留空关闭预算控制"></div><button type="submit" class="btn btn-primary">保存预算</button></form><p class="help-text">留空表示不设置预算。变更需要管理员权限。</p>` : '<p class="role-note">只有管理员可修改预算。</p>'}</div></div></section>
    <section class="card"><div class="card-header"><h3>费用构成</h3></div><div class="card-body"><dl class="detail-grid"><div class="detail-item"><dt>实际 / 已结算</dt><dd>${fmtMoney(actual)}</dd></div><div class="detail-item"><dt>估算 / 待核对</dt><dd>${fmtMoney(estimated)}</dd></div><div class="detail-item"><dt>未知用量请求</dt><dd>${fmtInt(unknown)} 次</dd></div><div class="detail-item"><dt>输入 token</dt><dd>${fmtInt(u.input_tokens ?? u.inputTokens)}</dd></div><div class="detail-item"><dt>输出 token</dt><dd>${fmtInt(u.output_tokens ?? u.outputTokens)}</dd></div><div class="detail-item"><dt>未结算预留</dt><dd>${fmtMoney(reserved)}</dd></div></dl><div class="legend mt-16"><span><i></i>已结算</span><span><i class="is-estimated"></i>估算/待核对</span><span><i class="is-unknown"></i>未知</span></div></div></section></div>
    <section class="card"><div class="card-header"><div><h3>请求趋势</h3><p>用量汇总按事件管道更新；积压时显示延迟提示。</p></div></div><div class="card-body">${chart}</div></section>
    <section class="card"><div class="card-header"><h3>按小时明细</h3></div><div class="card-body card-body--flush">${series.length ? `<div class="table-wrap"><table><thead><tr><th>时间桶</th><th class="num">事件数</th><th class="num">已结算</th><th class="num">预估</th><th class="num">输入 token</th><th class="num">输出 token</th></tr></thead><tbody>${rows}</tbody></table></div>` : emptyBox('没有可展示的聚合明细', '产生调用并完成结算后，按小时汇总会显示在这里。')}</div></section>`;
};
actions['set-range'] = btn => updateHashParams({ range: btn.dataset.range || '24h' });
actions['submit:budget'] = async form => {
  const raw = formValues(form).budget; const micros = raw === '' ? null : usdToMicros(raw); if (micros !== null && micros < 0) return toast('预算不能为负数。', 'error');
  const submit = form.querySelector('[type=submit]'); setBusy(submit, true, '保存中…');
  try { await api('/api/projects/' + q(state.projectId), { method: 'PATCH', body: { budgetMicros: micros, budget_micros: micros } }); toast(micros === null ? '预算控制已关闭。' : '预算已保存。', 'success'); await loadCurrent(true); }
  catch (e) { toast(errMsg(e), 'error'); } finally { setBusy(submit, false); }
};

// Guide: real test call and copyable integration examples.
RENDER.guide = async panel => {
  const ad = await apiFirst(['/api/aliases?' + projectQuery(), '/api/model-aliases?' + projectQuery()]); const aliases = items(ad).filter(isPublished); state.cache.aliases = items(ad);
  const base = location.origin; const model = aliases[0]?.name || 'support-chat';
  const basic = ['curl "' + base + '/v1/chat/completions" \\', '  -H "Authorization: Bearer $AI_GATEWAY_API_KEY" \\', '  -H "Content-Type: application/json" \\', '  -d \'{"model":"' + model + '","messages":[{"role":"user","content":"你好"}],"max_tokens":128}\''];
  const stream = ['curl -N "' + base + '/v1/chat/completions" \\', '  -H "Authorization: Bearer $AI_GATEWAY_API_KEY" \\', '  -H "Content-Type: application/json" \\', '  -d \'{"model":"' + model + '","messages":[{"role":"user","content":"你好"}],"stream":true,"max_tokens":128}\''];
  const modelOptions = aliases.map(a => `<option value="${escAttr(a.name)}">${esc(a.name)}</option>`).join('');
  panel.innerHTML = `${pageHeader('接入指南', '使用统一地址与项目密钥调用已发布的模型别名。完整密钥只应放在服务端环境变量中，不要写入浏览器或 URL。')}
    <div class="grid-2"><section class="card"><div class="card-header"><h3>网关地址</h3></div><div class="card-body"><div class="callout"><span class="inline-code">${esc(base)}/v1</span><p class="small mt-16">数据面接口：<span class="inline-code">GET /v1/models</span> 与 <span class="inline-code">POST /v1/chat/completions</span>。使用 <span class="inline-code">Authorization: Bearer &lt;项目密钥&gt;</span>。</p></div><dl class="detail-grid mt-16"><div class="detail-item"><dt>请求关联 ID</dt><dd class="mono">响应头 x-request-id</dd></div><div class="detail-item"><dt>流式协议</dt><dd>SSE；以可解析的 error 事件表示失败</dd></div><div class="detail-item"><dt>用量缺失</dt><dd>返回可用结果，但标记未知，不按 0 计费</dd></div><div class="detail-item"><dt>重试语义</dt><dd>首个内容片段之后不自动切换目标</dd></div></dl></div></section>
    <section class="card"><div class="card-header"><h3>可用模型</h3><a class="btn btn-small btn-secondary" href="#/aliases">管理别名</a></div><div class="card-body">${aliases.length ? `<div class="stack-sm">${aliases.map(a => `<div class="inline-actions"><code class="inline-code">${esc(a.name)}</code>${badge('published', '已发布')}</div>`).join('')}</div><p class="help-text mt-16">来自当前项目已发布别名。数据面 <span class="inline-code">/v1/models</span> 使用项目密钥鉴权，不能用控制台会话替代。</p>` : emptyBox('没有已发布模型', '先创建并发布项目别名，才能生成可调用的模型名。', isAdmin() ? '<a class="btn btn-secondary btn-small" href="#/aliases">前往模型别名</a>' : '')}</div></section></div>
    <div class="grid-2-equal"><section class="card"><div class="card-header"><h3>非流式请求</h3><button type="button" class="btn btn-small btn-secondary" data-action="copy-sample" data-target="curl-basic">复制</button></div><div class="card-body"><pre class="code-block" id="curl-basic"><code>${esc(basic.join('\n'))}</code></pre></div></section><section class="card"><div class="card-header"><h3>流式请求</h3><button type="button" class="btn btn-small btn-secondary" data-action="copy-sample" data-target="curl-stream">复制</button></div><div class="card-body"><pre class="code-block" id="curl-stream"><code>${esc(stream.join('\n'))}</code></pre></div></section></div>
    <section class="card"><div class="card-header"><div><h3>受控测试请求</h3><p>通过控制台会话授权，走与生产调用一致的鉴权、限流和计费链；不保存提示词或回答。</p></div></div><div class="card-body">${!canWrite() ? '<p class="role-note">只读角色不能发送测试请求。</p>' : aliases.length ? `<form data-form="test" class="form-grid"><div class="form-row"><div class="field"><label for="test-model">模型别名</label><select id="test-model" name="model" required>${modelOptions}</select></div><div class="field"><label for="test-max">最大输出 token</label><input id="test-max" name="maxTokens" type="number" min="1" max="4096" value="128" required></div></div><div class="field"><label for="test-prompt">测试消息</label><textarea id="test-prompt" name="prompt" required maxlength="4000" placeholder="例如：用一句话解释什么是 API 网关。"></textarea><p class="field-hint">仅用于本次请求；不要输入真实敏感数据。</p></div><div class="inline-actions"><button type="submit" class="btn btn-primary">发送测试请求</button><span class="help-text">可能产生上游费用。</span></div></form><div id="test-result" class="test-result" aria-live="polite"><p class="muted">发送后可在此查看状态、耗时、用量与响应摘要。</p></div>` : emptyBox('没有可用测试模型', '发布一个模型别名后即可发送真实测试请求。', isAdmin() ? '<a class="btn btn-secondary btn-small" href="#/aliases">配置别名</a>' : '')}</div></section>
    <div class="callout callout--warning">SSE 客户端必须等待完成事件；连接关闭不等于成功。若已收到内容后断流，网关会发送 error 事件并标记部分响应，不会切换目标续写。</div>`;
};
actions['copy-sample'] = btn => copyText($('#' + btn.dataset.target)?.textContent || '', btn);
actions['submit:test'] = async form => {
  const v = formValues(form); const submit = form.querySelector('[type=submit]'); const out = $('#test-result'); const model = v.model;
  if (!model || !v.prompt.trim()) return;
  setBusy(submit, true, '请求中…'); out.innerHTML = `<div class="loading-text"><span class="spinner" aria-hidden="true"></span>正在等待上游与网关完成…</div>`;
  const started = performance.now();
  try {
    const r = await api('/api/test', { method: 'POST', body: { project_id: state.projectId, projectId: state.projectId, model, messages: [{ role: 'user', content: v.prompt }], max_tokens: Number(v.maxTokens), maxTokens: Number(v.maxTokens) } });
    const elapsed = performance.now() - started; const msg = r?.choices?.[0]?.message?.content ?? r?.response ?? r?.content ?? '';
    const usage = r?.usage || {}; const requestId = r?.request_id || r?.requestId || state.lastRequestId || '—';
    out.innerHTML = `<div class="inline-actions">${badge('success', r?.status ? statusText(r.status) : '请求完成')}<span class="help-text">耗时 ${fmtDuration(elapsed)}</span><span class="help-text">请求 ID <span class="mono">${esc(requestId)}</span></span></div>${msg ? `<div class="response-box mt-16">${esc(typeof msg === 'string' ? msg : JSON.stringify(msg))}</div>` : '<p class="muted mt-16">网关未返回文本内容；这不等同于免费或成功，请查看请求日志。</p>'}<dl class="detail-grid mt-16"><div class="detail-item"><dt>输入 token</dt><dd>${fmtInt(usage.prompt_tokens ?? usage.input_tokens)}</dd></div><div class="detail-item"><dt>输出 token</dt><dd>${fmtInt(usage.completion_tokens ?? usage.output_tokens)}</dd></div><div class="detail-item"><dt>费用</dt><dd>${r?.actual_micros != null ? fmtMoney(r.actual_micros) : '未知 / 以后端结算为准'}</dd></div><div class="detail-item"><dt>响应模型</dt><dd>${esc(r?.model || model)}</dd></div></dl>`;
    toast('测试请求完成。', 'success');
  } catch (e) { out.innerHTML = errorBox(errMsg(e) + (e.status === 404 ? ' 控制台测试接口尚未提供，可使用上方 curl 示例测试。' : ''), false); }
  finally { setBusy(submit, false); }
};

// Settings, members, retention and audit.
function projectChoices(projects, selected = []) {
  return projects.map(p => `<label class="checkbox-field"><input type="checkbox" name="projectIds" value="${escAttr(p.id)}" ${selected.includes(p.id) ? 'checked' : ''}><span>${esc(p.name)}</span></label>`).join('');
}
function memberForm(m, projects) {
  return `<form data-form="member" data-id="${escAttr(m.id)}" class="form-grid"><div class="field"><label>成员</label><p class="inline-code">${esc(m.email)}</p></div><div class="form-row"><div class="field"><label for="member-role">工作空间角色</label><select id="member-role" name="role"><option value="admin" ${m.role === 'admin' ? 'selected' : ''}>管理员</option><option value="developer" ${m.role === 'developer' ? 'selected' : ''}>开发者</option><option value="viewer" ${m.role === 'viewer' ? 'selected' : ''}>只读</option></select></div><div class="field"><label for="member-disabled">账号状态</label><select id="member-disabled" name="disabled"><option value="false" ${m.disabled_at ? '' : 'selected'}>启用</option><option value="true" ${m.disabled_at ? 'selected' : ''}>禁用</option></select></div></div><fieldset class="field"><legend>可访问项目</legend><div class="stack-sm">${projectChoices(projects, arr(m.project_ids)) || '<p class="muted">没有项目。</p>'}</div><p class="field-hint">管理员可访问工作空间全部项目；其他角色只访问勾选项目。修改权限会吊销该成员现有会话。</p></fieldset><p class="form-error" data-error-for="form" hidden></p><div class="form-actions"><button type="button" class="btn btn-secondary" data-action="modal-cancel">取消</button><button type="submit" class="btn btn-primary">保存成员</button></div></form>`;
}
function inviteForm(projects) {
  return `<form data-form="invite" class="form-grid"><div class="field"><label for="invite-email">邀请邮箱</label><input id="invite-email" name="email" type="email" required maxlength="320" placeholder="developer@example.com"></div><div class="field"><label for="invite-role">角色</label><select id="invite-role" name="role"><option value="developer">开发者</option><option value="viewer">只读</option><option value="admin">管理员</option></select></div><fieldset class="field"><legend>初始项目访问</legend><div class="stack-sm">${projectChoices(projects) || '<p class="muted">暂无项目。</p>'}</div></fieldset><p class="callout">邀请链接包含一次性令牌；请通过安全渠道发送。接受后由用户设置至少 10 位密码。</p><p class="form-error" data-error-for="form" hidden></p><div class="form-actions"><button type="button" class="btn btn-secondary" data-action="modal-cancel">取消</button><button type="submit" class="btn btn-primary">创建邀请</button></div></form>`;
}
RENDER.settings = async panel => {
  const admin = isAdmin();
  const [pd, ad, md, sd] = await Promise.all([getSafe('/api/projects'), admin ? getSafe('/api/audit') : Promise.resolve({ ok: false }), admin ? getSafe('/api/members') : Promise.resolve({ ok: false }), admin ? getSafe('/api/settings') : Promise.resolve({ ok: false })]);
  const projects = pd.ok ? items(pd.data) : state.projects; const p = projects.find(x => x.id === state.projectId) || currentProject() || {};
  const members = md.ok ? items(md.data) : []; const audit = ad.ok ? items(ad.data) : []; const retention = sd.ok ? (sd.data?.request_retention_days ?? null) : null; state.cache.members = members; state.cache.settingsProjects = projects;
  const budget = p.budget_micros ?? p.budgetMicros ?? null; const name = p.name || currentProject()?.name || '';
  const projectForm = admin ? `<form data-form="settings-project" class="form-grid"><div class="form-row"><div class="field"><label for="settings-name">项目名称</label><input id="settings-name" name="name" required maxlength="80" value="${escAttr(name)}"></div><div class="field"><label for="settings-budget">月度预算（USD）</label><input id="settings-budget" name="budget" type="number" min="0" step="0.000001" value="${escAttr(microsToUsd(budget))}" placeholder="留空表示不设置"><p class="field-hint">按 UTC 月度估算；不是财务结算。</p></div></div><div class="form-row"><div class="field"><label for="settings-rpm">项目 RPM 上限</label><input id="settings-rpm" name="rpm" type="number" min="1" step="1" value="${escAttr(p.rpm ?? '')}" placeholder="留空使用服务端默认"></div><div class="field"><label for="settings-concurrency">并发上限</label><input id="settings-concurrency" name="concurrency" type="number" min="1" step="1" value="${escAttr(p.concurrency ?? '')}" placeholder="留空使用服务端默认"></div></div><div class="field field--max"><label for="settings-max-output">默认最大输出 token</label><input id="settings-max-output" name="maxOutputTokens" type="number" min="1" step="1" value="${escAttr(p.max_output_tokens ?? p.maxOutputTokens ?? '')}" placeholder="留空使用服务端默认"><p class="field-hint">客户端未指定输出上限时使用；服务端有最终约束。</p></div><p class="form-error" data-error-for="form" hidden></p><div class="form-actions"><button type="submit" class="btn btn-primary">保存项目设置</button></div></form>` : `<div class="muted-panel">当前角色为${esc(roleName(role()))}，项目设置只读。预算与限额修改需要管理员权限。</div>`;
  const memberRows = members.map(m => { const projs = arr(m.project_ids).map(id => projects.find(x => x.id === id)?.name || id); return `<tr><td><span class="cell-primary">${esc(m.email)}</span>${m.id === state.user?.id ? '<span class="cell-sub">当前账号</span>' : ''}</td><td>${badge(m.role === 'admin' ? 'published' : m.role === 'developer' ? 'enabled' : 'neutral', roleName(m.role))}</td><td>${m.disabled_at ? badge('disabled', '已禁用') : badge('enabled', '启用')}</td><td class="wrap">${m.role === 'admin' ? '全部项目' : projs.length ? projs.map(x => `<span class="badge badge--neutral badge--plain">${esc(x)}</span>`).join(' ') : '<span class="muted">无项目</span>'}</td><td class="row-actions"><button type="button" class="btn btn-small btn-secondary" data-action="member-manage" data-id="${escAttr(m.id)}">管理</button></td></tr>`; }).join('');
  const membersCard = admin ? `<section class="card"><div class="card-header"><div><h3>成员与权限</h3><p>管理员可邀请成员、调整角色、项目访问和账号状态。变更会吊销该成员现有会话。</p></div><button type="button" class="btn btn-primary btn-small" data-action="member-invite">邀请成员</button></div><div class="card-body card-body--flush">${!md.ok ? `<div class="card-body">${errorBox(md.error?.message || '成员读取失败。', false)}</div>` : members.length ? `<div class="table-wrap"><table><thead><tr><th>邮箱</th><th>角色</th><th>状态</th><th>项目访问</th><th></th></tr></thead><tbody>${memberRows}</tbody></table></div>` : emptyBox('暂无成员', '邀请开发者或只读成员加入当前工作空间。')}</div></section>` : `<section class="card"><div class="card-header"><h3>成员与权限</h3></div><div class="card-body"><p class="role-note">成员管理仅管理员可用。当前角色为${esc(roleName(role()))}。</p></div></section>`;
  const retentionCard = admin ? `<section class="card"><div class="card-header"><div><h3>数据保留</h3><p>请求元数据保留期由服务端设置控制，账本与去重记录不随日志清理删除。</p></div></div><div class="card-body"><form data-form="retention" class="inline-actions"><div class="field field--grow"><label for="retention-days">请求元数据保留天数</label><input id="retention-days" name="days" type="number" min="1" max="3650" value="${escAttr(retention ?? '')}" placeholder="留空表示使用服务端默认"></div><button type="submit" class="btn btn-primary">保存保留期</button></form><p class="help-text">允许 1–3650 天。清理任务与备份保留仍需在部署层验证；此设置不会删除账本。</p></div></section>` : `<section class="card"><div class="card-header"><h3>数据保留</h3></div><div class="card-body"><p class="role-note">保留期设置仅管理员可修改。</p></div></section>`;
  const auditRows = audit.map(a => `<tr><td class="nowrap">${fmtDate(a.created_at || a.createdAt)}</td><td>${esc(a.action)}</td><td>${esc(a.entity_type || a.entityType || '—')}</td><td class="mono-cell break-all">${esc(a.entity_id || a.entityId || '—')}</td><td class="mono-cell break-all">${esc(JSON.stringify(a.changes || {}).slice(0, 240))}</td></tr>`).join('');
  panel.innerHTML = `${pageHeader('设置', '项目范围设置、成员与权限、数据保留和审计。所有写操作在服务端校验角色与 CSRF；控制台不缓存供应商凭据。')}
    <div class="grid-2"><section class="card"><div class="card-header"><div><h3>项目设置</h3><p>修改会影响后续请求的准入与默认参数。</p></div></div><div class="card-body">${projectForm}</div></section>
    <section class="card"><div class="card-header"><h3>当前身份</h3></div><div class="card-body"><dl class="detail-grid"><div class="detail-item detail-item--full"><dt>邮箱</dt><dd>${esc(state.user?.email || '—')}</dd></div><div class="detail-item"><dt>工作空间角色</dt><dd>${esc(roleName(state.user?.role || role()))}</dd></div><div class="detail-item"><dt>当前项目角色</dt><dd>${esc(roleName(role()))}</dd></div><div class="detail-item"><dt>工作空间 ID</dt><dd class="mono break-all">${esc(state.user?.workspaceId || state.user?.workspace_id || '—')}</dd></div><div class="detail-item"><dt>项目 ID</dt><dd class="mono break-all">${esc(state.projectId || '—')}</dd></div><div class="detail-item"><dt>项目名称</dt><dd>${esc(name || '—')}</dd></div></dl><div class="form-actions"><button type="button" class="btn btn-secondary" data-action="settings-logout">退出登录</button></div></div></section></div>
    ${membersCard}
    <div class="grid-2-equal">${retentionCard}<section class="card"><div class="card-header"><h3>安全边界</h3></div><div class="card-body stack-sm"><p class="small muted">供应商密钥只写入并由服务端加密保存；浏览器不回显、不写入本地存储。自定义地址由服务端做 DNS/重定向安全检查。</p><p class="small muted">会话 Cookie 为 HttpOnly + SameSite；所有变更 /api 请求带 X-CSRF-Token。禁用成员或调整权限会吊销该成员会话。</p><p class="small muted">控制台与网关请求元数据不保存提示词、回答或鉴权头。部署级 SSRF 允许名单仍需通过环境配置管理。</p></div></section></div>
    <section class="card"><div class="card-header"><div><h3>审计记录</h3><p>仅管理员可查看，最近 100 条；敏感字段由服务端脱敏。</p></div></div><div class="card-body card-body--flush">${!admin ? '<div class="empty-state"><h3>需要管理员权限</h3><p>审计记录只对管理员开放。</p></div>' : !ad.ok ? `<div class="card-body">${errorBox(ad.error?.message || '审计读取失败。', false)}</div>` : audit.length ? `<div class="table-wrap"><table><thead><tr><th>时间</th><th>操作</th><th>对象类型</th><th>对象 ID</th><th>变更摘要</th></tr></thead><tbody>${auditRows}</tbody></table></div>` : emptyBox('暂无审计记录', '管理员或开发者完成配置写入后，审计事件会显示在这里。')}</div></section>
    <div class="callout">费用展示为估算与预算控制，不是收款或财务结算。成员和保留期写入会记录审计；删除账本或绕过保留策略不在控制台范围内。</div>`;
};
actions['member-manage'] = btn => { const m = arr(state.cache.members).find(x => x.id === btn.dataset.id); if (m) openModal({ title: '管理成员', body: memberForm(m, arr(state.cache.settingsProjects)), wide: true }); };
actions['member-invite'] = () => openModal({ title: '邀请成员', body: inviteForm(arr(state.cache.settingsProjects)), wide: true });
actions['submit:member'] = async form => {
  const v = formValues(form); const ids = new FormData(form).getAll('projectIds').filter(Boolean); const id = form.dataset.id; const submit = form.querySelector('[type=submit]');
  setBusy(submit, true, '保存中…');
  try {
    await api('/api/members/' + q(id), { method: 'PATCH', body: { role: v.role, disabled: v.disabled === 'true', project_ids: ids } });
    closeModal(); toast('成员权限已更新；该成员现有会话已被服务端吊销。', 'success');
    if (id === state.user?.id) await boot(); else await loadCurrent(true);
  } catch (e) { fieldError(form, 'form', errMsg(e)); } finally { setBusy(submit, false); }
};
actions['submit:invite'] = async form => {
  const v = formValues(form); const ids = new FormData(form).getAll('projectIds').filter(Boolean); const submit = form.querySelector('[type=submit]');
  setBusy(submit, true, '创建中…');
  try {
    const r = await api('/api/invitations', { method: 'POST', body: { email: v.email.trim(), role: v.role, project_ids: ids } });
    const token = r.token || ''; const link = location.origin + '/#/accept?token=' + q(token);
    openModal({ title: '邀请已创建', body: `<p class="callout callout--warning">邀请令牌只显示一次。请通过安全渠道发送给 ${esc(v.email.trim())}；令牌 7 天有效，接受时设置至少 10 位密码。</p><div class="copy-row"><code class="secret-key" id="invite-token">${esc(token)}</code><button type="button" class="btn btn-secondary" data-action="copy-invite">复制令牌</button></div><div class="field mt-16"><label>接受链接</label><div class="copy-row"><code class="secret-key" id="invite-link">${esc(link)}</code><button type="button" class="btn btn-secondary" data-action="copy-invite" data-target="invite-link">复制链接</button></div></div><p class="help-text">接受页面会创建成员账号并签发新会话；管理员当前会话不受影响。</p>`, footer: '<button type="button" class="btn btn-primary" data-action="modal-cancel">已保存，关闭</button>' });
    await loadCurrent(true);
  } catch (e) { fieldError(form, 'form', errMsg(e)); } finally { setBusy(submit, false); }
};
actions['copy-invite'] = btn => copyText($('#' + (btn.dataset.target || 'invite-token'))?.textContent || '', btn);
actions['submit:retention'] = async form => {
  const raw = formValues(form).days; const days = raw === '' ? null : Number(raw); const submit = form.querySelector('[type=submit]');
  if (days !== null && (!Number.isInteger(days) || days < 1 || days > 3650)) return toast('保留期需为 1–3650 天。', 'error');
  setBusy(submit, true, '保存中…');
  try { await api('/api/settings', { method: 'PATCH', body: { request_retention_days: days } }); toast('请求元数据保留期已保存。', 'success'); await loadCurrent(true); }
  catch (e) { toast(errMsg(e), 'error'); } finally { setBusy(submit, false); }
};
actions['settings-logout'] = async () => { if (await confirmModal({ title: '退出登录', message: '将结束当前浏览器会话。未提交的表单内容会丢失。', confirmLabel: '退出登录' })) doLogout(); };
actions['submit:settings-project'] = async form => {
  const v = formValues(form); const name = v.name.trim(); if (!name) return fieldError(form, 'form', '项目名称不能为空。');
  const budget = v.budget === '' ? null : usdToMicros(v.budget); if (budget !== null && budget < 0) return fieldError(form, 'form', '预算不能为负数。');
  const intOrNull = x => x === '' ? null : Number(x); const rpm = intOrNull(v.rpm), concurrency = intOrNull(v.concurrency), maxOutputTokens = intOrNull(v.maxOutputTokens);
  const submit = form.querySelector('[type=submit]'); setBusy(submit, true, '保存中…');
  try {
    await api('/api/projects/' + q(state.projectId), { method: 'PATCH', body: { name, budgetMicros: budget, budget_micros: budget, rpm, concurrency, maxOutputTokens, max_output_tokens: maxOutputTokens } });
    const cp = currentProject(); if (cp) cp.name = name; renderShell(); toast('项目设置已保存。', 'success'); await loadCurrent(true);
  } catch (e) { fieldError(form, 'form', errMsg(e)); } finally { setBusy(submit, false); }
};

document.addEventListener('change', ev => {
  const box = ev.target;
  if (!box.matches || !box.matches('input[name="targetIds"]') || !box.form) return;
  const boxes = $$('input[name="targetIds"]', box.form);
  let checked = boxes.filter(b => b.checked);
  if (checked.length > 2) { box.checked = false; checked = boxes.filter(b => b.checked); toast('每个路由版本最多选择两个目标。', 'error'); }
  boxes.forEach(b => { if (!b.checked) b.disabled = checked.length >= 2; });
});
