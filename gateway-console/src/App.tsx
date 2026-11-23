import React, { useEffect, useState } from 'react';
import * as stylex from '@stylexjs/stylex';
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@ui-sdk/react/select';
import { Field, FieldLabel } from '@ui-sdk/react/field';
import { Input } from '@ui-sdk/react/input';
import { ui } from './styles/ui';
import {
  api,
  arr,
  roleName,
  setCsrfToken,
  setUnauthorizedHandler,
  usdToMicros,
} from './api/client';
import {
  UiBadge,
  UiButton,
  UiConfirmDialog,
  UiDialog,
  UiToastRegion,
  notify,
} from './components/Primitives';
import { AuthView } from './views/AuthView';
import { AliasesView, KeysView, OverviewView, ProvidersView } from './views/CatalogViews';
import { McpView } from './views/McpViews';
import { PluginsView, TelemetryView } from './views/MicrokernelViews';
import { GuideView, RequestsView, SettingsView, UsageView } from './views/OperationsViews';

type ViewName =
  | 'overview'
  | 'aliases'
  | 'keys'
  | 'guide'
  | 'requests'
  | 'usage'
  | 'settings'
  | 'providers'
  | 'mcp'
  | 'plugins'
  | 'telemetry';

interface NavItem {
  key: ViewName;
  label: string;
  icon: string;
  eyebrow: string;
  scope: 'project' | 'workspace';
}

const NAV_ITEMS: NavItem[] = [
  // 1. Project Scope Workbench
  { key: 'overview', label: '项目指挥舱', icon: '▦', eyebrow: '项目工作台', scope: 'project' },
  { key: 'aliases', label: '智能路由与别名', icon: '◇', eyebrow: '项目工作台', scope: 'project' },
  { key: 'keys', label: 'API Keys 与 ACL', icon: '⌘', eyebrow: '项目工作台', scope: 'project' },
  { key: 'guide', label: '多模态联调台', icon: '→', eyebrow: '项目工作台', scope: 'project' },
  { key: 'requests', label: '请求追踪日志', icon: '≡', eyebrow: '项目工作台', scope: 'project' },
  { key: 'usage', label: 'FinOps 账本', icon: '◫', eyebrow: '项目工作台', scope: 'project' },
  { key: 'settings', label: '项目配额与审计', icon: '⚙', eyebrow: '项目工作台', scope: 'project' },
  // 2. Workspace & Cluster Infrastructure Scope
  { key: 'providers', label: '多模态供应商', icon: '⇄', eyebrow: '平台基础设施', scope: 'workspace' },
  { key: 'mcp', label: 'MCP 工具网关', icon: '⊞', eyebrow: '平台基础设施', scope: 'workspace' },
  { key: 'plugins', label: '微内核插件中心', icon: '❖', eyebrow: '平台基础设施', scope: 'workspace' },
  { key: 'telemetry', label: '内核健康与遥测', icon: '◉', eyebrow: '平台基础设施', scope: 'workspace' },
];

function parseHashRoute(): {
  view: ViewName;
  projectId: string | null;
  range: string;
  inviteToken: string;
  isAccept: boolean;
} {
  const raw = (typeof window !== 'undefined' ? window.location.hash : '').replace(/^#\/?/, '');
  const [pathPart, qs = ''] = raw.split('?');
  const params = new URLSearchParams(qs);
  const isAccept = pathPart === 'accept';
  const validView = NAV_ITEMS.some((n) => n.key === pathPart)
    ? (pathPart as ViewName)
    : 'overview';
  return {
    view: validView,
    projectId: params.get('project_id'),
    range: params.get('range') || '24h',
    inviteToken: params.get('token') || '',
    isAccept,
  };
}

export function App() {
  const [booting, setBooting] = useState(true);
  const [bootErr, setBootErr] = useState('');
  const [authed, setAuthed] = useState(false);
  const [authMode, setAuthMode] = useState<'bootstrap' | 'login' | 'accept'>('bootstrap');
  const [authError, setAuthError] = useState('');

  const [user, setUser] = useState<any | null>(null);
  const [projects, setProjects] = useState<any[]>([]);
  const [projectId, setProjectId] = useState<string | null>(null);

  const initialRoute = parseHashRoute();
  const [view, setView] = useState<ViewName>(initialRoute.view);
  const [range, setRange] = useState<string>(initialRoute.range);
  const [inviteToken, setInviteToken] = useState<string>(initialRoute.inviteToken);
  const [refreshTick, setRefreshTick] = useState(0);
  const [logoutConfirmOpen, setLogoutConfirmOpen] = useState(false);

  // Create New Project Dialog (Phase P5-A)
  const [newProjOpen, setNewProjOpen] = useState(false);
  const [npName, setNpName] = useState('');
  const [npBudgetUsd, setNpBudgetUsd] = useState('');
  const [npRpm, setNpRpm] = useState('600');
  const [npTpm, setNpTpm] = useState('500000');
  const [npConcurrency, setNpConcurrency] = useState('50');
  const [npErr, setNpErr] = useState('');

  function applySessionPayload(raw: any) {
    const s =
      raw?.session && typeof raw.session === 'object'
        ? raw.session
        : raw?.data && typeof raw.data === 'object'
          ? raw.data
          : raw || {};
    const u = s.user || raw?.user || {};
    setUser(u);

    const list = s.projects || raw?.projects || u.projects || [];
    const pIds = u.projectIds || u.project_ids || s.projectIds || s.project_ids || [];
    let normalized: { id: string; name: string; role: string; budget_micros?: any }[] = arr(list)
      .map((p: any) => (typeof p === 'string' ? { id: p, name: p, role: u.role } : p))
      .map((p: any) => ({
        id: p.id || p.projectId || p.project_id,
        name: p.name || p.projectName || p.project_name || '未命名项目',
        role: p.role || p.projectRole || u.role || 'viewer',
        budget_micros: p.budget_micros ?? p.budgetMicros,
      }))
      .filter((p: any) => p.id);

    if (!normalized.length && arr(pIds).length) {
      normalized = arr(pIds).map((id: string) => ({ id, name: id, role: u.role }));
    }
    if (!normalized.length && raw?.project?.id) {
      normalized = [
        {
          id: raw.project.id,
          name: raw.project.name || '默认项目',
          role: raw.project.role || u.role || 'admin',
        },
      ];
    }

    const token =
      s.csrf || s.csrfToken || s.csrf_token || raw?.csrf || raw?.csrfToken || raw?.csrf_token;
    if (token) setCsrfToken(token);

    setProjects(normalized);
    const saved = localStorage.getItem('ag.project');
    const urlPid = parseHashRoute().projectId;
    const preferred =
      (urlPid && normalized.some((p) => p.id === urlPid) ? urlPid : null) ||
      s.project?.id ||
      raw?.project?.id ||
      saved;
    const chosen = normalized.some((p) => p.id === preferred)
      ? preferred
      : normalized[0]?.id || null;
    setProjectId(chosen);
    if (chosen) localStorage.setItem('ag.project', chosen);
    setAuthed(true);
  }

  async function boot() {
    setBooting(true);
    setBootErr('');
    try {
      const data = await api('/api/session', { silentAuth: true });
      const parsed = parseHashRoute();
      if (parsed.isAccept) {
        setInviteToken(parsed.inviteToken);
        setAuthMode('accept');
        setAuthed(false);
        setBooting(false);
        return;
      }
      applySessionPayload(data);
      setBooting(false);
    } catch (e: any) {
      const parsed = parseHashRoute();
      if (e?.status === 401) {
        setAuthMode(parsed.isAccept ? 'accept' : 'bootstrap');
        setInviteToken(parsed.inviteToken);
        setAuthed(false);
      } else {
        setBootErr(e?.message || '无法连接 AI Gateway 控制面服务');
      }
      setBooting(false);
    }
  }

  useEffect(() => {
    setUnauthorizedHandler((msg) => {
      setAuthed(false);
      setUser(null);
      setProjects([]);
      setAuthMode('login');
      setAuthError(msg || '会话已失效，请重新登录。');
    });
    boot();
    return () => setUnauthorizedHandler(null);
  }, []);

  useEffect(() => {
    function onHashChange() {
      const r = parseHashRoute();
      if (r.isAccept) {
        setInviteToken(r.inviteToken);
        setAuthMode('accept');
        setAuthed(false);
        return;
      }
      setView(r.view);
      setRange(r.range);
      if (r.projectId && projects.some((p) => p.id === r.projectId)) {
        setProjectId(r.projectId);
        localStorage.setItem('ag.project', r.projectId);
      }
    }
    window.addEventListener('hashchange', onHashChange);
    return () => window.removeEventListener('hashchange', onHashChange);
  }, [projects]);

  function syncUrl(nextView: ViewName, nextPid: string | null, nextRange: string) {
    const sp = new URLSearchParams();
    if (nextPid) sp.set('project_id', nextPid);
    if (nextRange && nextRange !== '24h') sp.set('range', nextRange);
    const qs = sp.toString();
    window.history.replaceState(null, '', `#/${nextView}${qs ? '?' + qs : ''}`);
  }

  function handleNavigate(next: string) {
    const v = (NAV_ITEMS.some((n) => n.key === next) ? next : 'overview') as ViewName;
    setView(v);
    syncUrl(v, projectId, range);
  }

  function handleRangeChange(nextRange: string) {
    setRange(nextRange);
    syncUrl(view, projectId, nextRange);
  }

  function handleProjectChange(nextPid: string) {
    setProjectId(nextPid);
    localStorage.setItem('ag.project', nextPid);
    syncUrl(view, nextPid, range);
  }

  async function handleCreateProject() {
    setNpErr('');
    if (!npName.trim()) {
      setNpErr('请输入新项目名称。');
      return;
    }
    const bMicros = npBudgetUsd.trim() ? usdToMicros(npBudgetUsd) : null;
    const intOrNull = (s: string) => (s.trim() ? Number(s) : null);
    try {
      const created = await api('/api/projects', {
        method: 'POST',
        body: {
          name: npName.trim(),
          budget_micros: bMicros,
          rpm: intOrNull(npRpm),
          tpm: intOrNull(npTpm),
          concurrency: intOrNull(npConcurrency),
        },
      });
      const nextProj = {
        id: created.id,
        name: created.name,
        role: 'admin',
        budget_micros: created.budget_micros,
      };
      setProjects((prev) => [...prev, nextProj]);
      handleProjectChange(created.id);
      setNewProjOpen(false);
      notify(`新项目“${created.name}”已创建并切换为当前项目`, 'success');
    } catch (e: any) {
      setNpErr(e?.message || '创建项目失败');
    }
  }

  async function doLogout() {
    try {
      await api('/api/logout', { method: 'POST' });
    } catch {
      /* ignore expired session */
    }
    localStorage.removeItem('ag.project');
    setAuthed(false);
    setUser(null);
    setProjects([]);
    setAuthMode('login');
    notify('已退出登录', 'info');
  }

  const currentProject = projects.find((p) => p.id === projectId) || projects[0] || null;
  const currentRole = currentProject?.role || user?.role || 'viewer';
  const isAdmin = currentRole === 'admin';
  const canWrite = ['admin', 'developer'].includes(currentRole);
  const currentNav = NAV_ITEMS.find((n) => n.key === view) || NAV_ITEMS[0];
  const projectNavs = NAV_ITEMS.filter((n) => n.scope === 'project');
  const workspaceNavs = NAV_ITEMS.filter((n) => n.scope === 'workspace');

  if (booting) {
    return (
      <div {...stylex.props(ui.bootScreen)} role="status" aria-live="polite">
        <div {...stylex.props(ui.bootCard)}>
          <div
            {...stylex.props(ui.brandMark)}
            style={{ width: '40px', height: '40px', fontSize: '15px', marginBottom: '12px' }}
          >
            AG
          </div>
          <h2 style={{ margin: '0 0 6px 0', fontSize: '16px' }}>正在连接 AI Gateway</h2>
          <p {...stylex.props(ui.mutedText)} style={{ margin: 0 }}>
            校验会话与工作空间权限…
          </p>
        </div>
      </div>
    );
  }

  if (bootErr) {
    return (
      <div {...stylex.props(ui.bootScreen)} role="alert">
        <div {...stylex.props(ui.bootCard)}>
          <div
            {...stylex.props(ui.brandMark)}
            style={{ width: '40px', height: '40px', fontSize: '15px', marginBottom: '12px' }}
          >
            AG
          </div>
          <h2 style={{ margin: '0 0 6px 0', fontSize: '16px' }}>无法连接控制台</h2>
          <p {...stylex.props(ui.mutedText)} style={{ margin: '0 0 16px 0' }}>
            {bootErr}
          </p>
          <UiButton variant="primary" onClick={boot}>
            重试连接
          </UiButton>
        </div>
      </div>
    );
  }

  if (!authed) {
    return (
      <>
        <AuthView
          initialMode={authMode}
          initialInviteToken={inviteToken}
          externalError={authError}
          onSuccess={(payload, msg) => {
            applySessionPayload(payload);
            notify(msg, 'success');
          }}
        />
        <UiToastRegion />
      </>
    );
  }

  return (
    <>
      <div {...stylex.props(ui.appShell)}>
        <aside {...stylex.props(ui.sidebar)} aria-label="控制台主导航">
          <div {...stylex.props(ui.brandRow)}>
            <span {...stylex.props(ui.brandMark)} aria-hidden="true">
              AG
            </span>
            <span {...stylex.props(ui.brandText)}>AI Gateway</span>
          </div>

          <div {...stylex.props(ui.workspaceBox)}>
            <span {...stylex.props(ui.workspaceLabel)}>工作空间</span>
            <span {...stylex.props(ui.workspaceName)}>
              {user?.workspaceName || user?.workspace_name || '当前企业部署'}
            </span>
          </div>

          <nav {...stylex.props(ui.navGroup)} aria-label="双层作用域导航">
            <div
              style={{
                fontSize: '11px',
                fontWeight: 600,
                color: 'var(--fg-muted)',
                padding: '6px 10px 4px',
                letterSpacing: '0.04em',
              }}
            >
              项目工作台 · PROJECT
            </div>
            {projectNavs.map((item) => {
              const active = item.key === view;
              return (
                <button
                  key={item.key}
                  type="button"
                  onClick={() => handleNavigate(item.key)}
                  aria-current={active ? 'page' : undefined}
                  {...stylex.props(ui.navButton, active && ui.navButtonActive)}
                >
                  <span {...stylex.props(ui.navIcon)} aria-hidden="true">
                    {item.icon}
                  </span>
                  <span>{item.label}</span>
                </button>
              );
            })}

            <div
              style={{
                fontSize: '11px',
                fontWeight: 600,
                color: 'var(--fg-muted)',
                padding: '14px 10px 4px',
                letterSpacing: '0.04em',
              }}
            >
              平台基础设施 · WORKSPACE
            </div>
            {workspaceNavs.map((item) => {
              const active = item.key === view;
              return (
                <button
                  key={item.key}
                  type="button"
                  onClick={() => handleNavigate(item.key)}
                  aria-current={active ? 'page' : undefined}
                  {...stylex.props(ui.navButton, active && ui.navButtonActive)}
                >
                  <span {...stylex.props(ui.navIcon)} aria-hidden="true">
                    {item.icon}
                  </span>
                  <span>{item.label}</span>
                </button>
              );
            })}
          </nav>

          <div {...stylex.props(ui.sidebarFooter)}>
            <div {...stylex.props(ui.userCard)}>
              <span {...stylex.props(ui.avatar)} aria-hidden="true">
                {(user?.email || '?').slice(0, 1)}
              </span>
              <div {...stylex.props(ui.userMeta)}>
                <span {...stylex.props(ui.userEmail)}>{user?.email || '未知用户'}</span>
                <span {...stylex.props(ui.userRole)}>
                  {roleName(currentRole)} · 项目成员
                </span>
              </div>
            </div>
            <UiButton
              size="sm"
              variant="outline"
              block
              onClick={() => setLogoutConfirmOpen(true)}
            >
              退出登录
            </UiButton>
          </div>
        </aside>

        <div {...stylex.props(ui.mainShell)}>
          <header {...stylex.props(ui.topbar)}>
            <div {...stylex.props(ui.topbarLeft)}>
              <UiBadge
                tone={currentNav.scope === 'workspace' ? 'info' : 'neutral'}
                label={currentNav.eyebrow}
              />
              <h1 {...stylex.props(ui.topbarTitle)}>{currentNav.label}</h1>
            </div>

            <div {...stylex.props(ui.topbarRight)}>
              {projects.length > 0 && (
                <div {...stylex.props(ui.inlineRow)}>
                  <span {...stylex.props(ui.mutedText)}>当前项目</span>
                  <Select
                    aria-label="切换当前项目"
                    value={currentProject?.id || ''}
                    onValueChange={(val) => {
                      if (val) handleProjectChange(String(val));
                    }}
                  >
                    <SelectTrigger {...stylex.props(ui.selectTrigger)}>
                      <SelectValue placeholder="选择项目">
                        {currentProject?.name || '选择项目'}
                      </SelectValue>
                      <span aria-hidden="true" style={{ fontSize: '10px', opacity: 0.6 }}>
                        ▾
                      </span>
                    </SelectTrigger>
                    <SelectContent>
                      {projects.map((p) => (
                        <SelectItem
                          key={p.id}
                          value={p.id}
                          className={(state) =>
                            stylex.props(
                              ui.selectItem,
                              state.selected && ui.selectItemActive,
                            ).className
                          }
                        >
                          <span>{p.name}</span>
                          {p.id === currentProject?.id && <span>✓</span>}
                        </SelectItem>
                      ))}
                    </SelectContent>
                  </Select>
                  {isAdmin && (
                    <UiButton
                      size="sm"
                      variant="outline"
                      onClick={() => {
                        setNpName('');
                        setNpBudgetUsd('');
                        setNpErr('');
                        setNewProjOpen(true);
                      }}
                    >
                      + 新建项目
                    </UiButton>
                  )}
                </div>
              )}
              <UiButton
                size="sm"
                variant="secondary"
                onClick={() => setRefreshTick((t) => t + 1)}
              >
                刷新
              </UiButton>
            </div>
          </header>

          <main {...stylex.props(ui.content)}>
            {view === 'overview' && (
              <OverviewView
                project={currentProject}
                range={range}
                onRangeChange={handleRangeChange}
                onNavigate={handleNavigate}
                refreshTick={refreshTick}
              />
            )}
            {view === 'providers' && (
              <ProvidersView isAdmin={isAdmin} refreshTick={refreshTick} />
            )}
            {view === 'mcp' && (
              <McpView canWrite={canWrite} refreshTick={refreshTick} />
            )}
            {view === 'aliases' && (
              <AliasesView
                project={currentProject}
                isAdmin={isAdmin}
                onNavigate={handleNavigate}
                refreshTick={refreshTick}
              />
            )}
            {view === 'keys' && (
              <KeysView
                project={currentProject}
                canWrite={canWrite}
                refreshTick={refreshTick}
              />
            )}
            {view === 'plugins' && (
              <PluginsView
                project={currentProject}
                canWrite={canWrite}
                refreshTick={refreshTick}
              />
            )}
            {view === 'telemetry' && (
              <TelemetryView
                canWrite={canWrite}
                onNavigate={handleNavigate}
                refreshTick={refreshTick}
              />
            )}
            {view === 'requests' && (
              <RequestsView
                project={currentProject}
                range={range}
                onRangeChange={handleRangeChange}
                onNavigate={handleNavigate}
                refreshTick={refreshTick}
              />
            )}
            {view === 'usage' && (
              <UsageView
                project={currentProject}
                isAdmin={isAdmin}
                range={range}
                onRangeChange={handleRangeChange}
                refreshTick={refreshTick}
              />
            )}
            {view === 'guide' && (
              <GuideView
                project={currentProject}
                canWrite={canWrite}
                onNavigate={handleNavigate}
                refreshTick={refreshTick}
              />
            )}
            {view === 'settings' && (
              <SettingsView
                project={currentProject}
                user={user}
                isAdmin={isAdmin}
                onProjectRenamed={(newName) => {
                  setProjects((prev) =>
                    prev.map((p) => (p.id === currentProject?.id ? { ...p, name: newName } : p)),
                  );
                }}
                onLogout={() => setLogoutConfirmOpen(true)}
                refreshTick={refreshTick}
              />
            )}
          </main>

          <footer {...stylex.props(ui.footer)}>
            <span>AI Gateway 企业级微内核管理平台 v0.5.0 · React 19 + @ui-sdk/react + StyleX</span>
            <span>
              作用域：{currentNav.eyebrow} · 当前项目：{currentProject?.name || '无'}
            </span>
          </footer>
        </div>
      </div>

      {/* Create New Project Dialog */}
      <UiDialog
        open={newProjOpen}
        onOpenChange={setNewProjOpen}
        title="新建业务项目（Project）"
        description="每个项目拥有独立的模型别名命名空间、API Keys、微美元月度预算及 RPM / TPM / 并发租约配额。"
        wide
        footer={
          <>
            <UiButton variant="outline" onClick={() => setNewProjOpen(false)}>
              取消
            </UiButton>
            <UiButton variant="primary" onClick={handleCreateProject}>
              创建项目
            </UiButton>
          </>
        }
      >
        <div {...stylex.props(ui.formGrid)}>
          <div {...stylex.props(ui.formRow)}>
            <Field name="npName" {...stylex.props(ui.field)}>
              <FieldLabel {...stylex.props(ui.fieldLabel)}>项目名称</FieldLabel>
              <Input
                value={npName}
                onValueChange={setNpName}
                placeholder="例如：客服智能体生产环境"
                {...stylex.props(ui.input)}
              />
            </Field>
            <Field name="npBudget" {...stylex.props(ui.field)}>
              <FieldLabel {...stylex.props(ui.fieldLabel)}>
                UTC 月度预算上限（USD，可选）
              </FieldLabel>
              <Input
                type="number"
                min="0"
                step="0.01"
                value={npBudgetUsd}
                onValueChange={setNpBudgetUsd}
                placeholder="留空不设硬上限"
                {...stylex.props(ui.input)}
              />
            </Field>
          </div>
          <div {...stylex.props(ui.formRow)}>
            <Field name="npRpm" {...stylex.props(ui.field)}>
              <FieldLabel {...stylex.props(ui.fieldLabel)}>项目 RPM 上限</FieldLabel>
              <Input
                type="number"
                min="1"
                value={npRpm}
                onValueChange={setNpRpm}
                {...stylex.props(ui.input)}
              />
            </Field>
            <Field name="npTpm" {...stylex.props(ui.field)}>
              <FieldLabel {...stylex.props(ui.fieldLabel)}>项目 TPM 上限 (Tokens/min)</FieldLabel>
              <Input
                type="number"
                min="1"
                value={npTpm}
                onValueChange={setNpTpm}
                {...stylex.props(ui.input)}
              />
            </Field>
            <Field name="npConcurrency" {...stylex.props(ui.field)}>
              <FieldLabel {...stylex.props(ui.fieldLabel)}>最大并发请求租约</FieldLabel>
              <Input
                type="number"
                min="1"
                value={npConcurrency}
                onValueChange={setNpConcurrency}
                {...stylex.props(ui.input)}
              />
            </Field>
          </div>
          {npErr && <p {...stylex.props(ui.fieldError)}>{npErr}</p>}
        </div>
      </UiDialog>

      <UiConfirmDialog
        open={logoutConfirmOpen}
        onOpenChange={setLogoutConfirmOpen}
        title="退出登录"
        description="将结束当前浏览器会话，未提交的表单内容会丢失。确认退出？"
        confirmLabel="退出登录"
        onConfirm={doLogout}
      />

      <UiToastRegion />
    </>
  );
}
