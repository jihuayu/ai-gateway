import React, { useEffect, useState } from 'react';
import * as stylex from '@stylexjs/stylex';
import { Field, FieldDescription, FieldLabel } from '@ui-sdk/react/field';
import { Input } from '@ui-sdk/react/input';
import { ui } from '../styles/ui';
import {
  api,
  arr,
  fmtDate,
  fmtDuration,
  fmtInt,
  fmtMoney,
  fmtPct,
  getSafe,
  isKeyActive,
  isPublished,
  items,
  microsToUsd,
  rangeBounds,
  statusText,
  targetIdsOf,
  targetLabel,
  usdToMicros,
} from '../api/client';
import {
  EmptyBox,
  MetricCard,
  PageHeader,
  UiAttemptTimeline,
  UiRequestWaterfall,
  UiBadge,
  UiButton,
  UiCheckboxRow,
  UiConfirmDialog,
  UiDialog,
  UiQuotaMeter,
  UiRangeSegmented,
  UiSwitch,
  copyToClipboard,
  notify,
} from '../components/Primitives';

const KNOWN_CAPABILITIES = ['chat', 'stream', 'stream_usage', 'tools', 'vision', 'json_schema', 'json_object', 'anthropic_messages', 'reasoning', 'embeddings', 'rerank', 'image_generation', 'system_one', 'decisions'];

async function loadAllTargets(providersList?: any[]): Promise<any[]> {
  const ps = providersList || items(await api('/api/providers'));
  const groups = await Promise.all(
    ps.map(async (p: any) => {
      try {
        const d = await api(`/api/providers/${encodeURIComponent(p.id)}/targets`);
        return items(d).map((t: any) => ({
          ...t,
          provider_id: t.provider_id || p.id,
          provider_name: p.name,
          upstream_model: t.upstream_model || t.upstreamModel,
        }));
      } catch (e: any) {
        if (e?.status === 404 || e?.status === 403) {
          return arr(p.targets).map((t: any) => ({ ...t, provider_id: p.id, provider_name: p.name }));
        }
        throw e;
      }
    }),
  );
  return groups.flat();
}

// ============================================================================
// 1. OVERVIEW VIEW
// ============================================================================
export function OverviewView({
  project,
  range,
  onRangeChange,
  onNavigate,
  refreshTick,
}: {
  project: any;
  range: string;
  onRangeChange: (r: string) => void;
  onNavigate: (view: string) => void;
  refreshTick: number;
}) {
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');
  const [usage, setUsage] = useState<any>({});
  const [recent, setRecent] = useState<any[]>([]);
  const [providers, setProviders] = useState<any[]>([]);
  const [aliases, setAliases] = useState<any[]>([]);
  const [keys, setKeys] = useState<any[]>([]);
  const [detailReq, setDetailReq] = useState<any | null>(null);
  const [targets, setTargets] = useState<any[]>([]);

  useEffect(() => {
    let active = true;
    async function load() {
      if (!project?.id) return;
      setLoading(true);
      setError('');
      const b = rangeBounds(range);
      const pid = encodeURIComponent(project.id);
      const qs = `project_id=${pid}&from=${encodeURIComponent(b.from)}&to=${encodeURIComponent(b.to)}`;
      const [u, rq, pr, al, ky] = await Promise.all([
        getSafe(`/api/usage/summary?${qs}`),
        getSafe(`/api/requests?${qs}&limit=5`),
        getSafe('/api/providers'),
        getSafe(`/api/aliases?project_id=${pid}`),
        getSafe(`/api/keys?project_id=${pid}`),
      ]);
      if (!active) return;
      if (!u.ok && !rq.ok && !pr.ok && !al.ok && !ky.ok) {
        setError(u.error?.message || '读取概览数据失败');
        setLoading(false);
        return;
      }
      setUsage(u.ok ? u.data : {});
      setRecent(rq.ok ? items(rq.data) : []);
      const pList = pr.ok ? items(pr.data) : [];
      setProviders(pList);
      setAliases(al.ok ? items(al.data) : []);
      setKeys(ky.ok ? items(ky.data) : []);
      loadAllTargets(pList)
        .then((ts) => active && setTargets(ts))
        .catch(() => {});
      setLoading(false);
    }
    load();
    return () => {
      active = false;
    };
  }, [project?.id, range, refreshTick]);

  async function openRequestDetail(reqId: string) {
    try {
      const r = await api(`/api/requests/${encodeURIComponent(reqId)}`);
      setDetailReq(r);
    } catch (e: any) {
      notify(e?.message || '加载请求详情失败', 'error');
    }
  }

  if (error) {
    return <div {...stylex.props(ui.callout, ui.calloutDanger)}>{error}</div>;
  }

  const reqs = Number(usage?.requests || 0);
  const success = Number(usage?.successful_requests ?? usage?.successfulRequests ?? NaN);
  const rate = Number.isFinite(success) && reqs > 0 ? success / reqs : null;
  const actual = usage?.actual_micros ?? usage?.actualMicros;
  const estimated = usage?.estimated_micros ?? usage?.estimatedMicros;
  const reserved = usage?.budget?.reserved_micros ?? usage?.reserved_micros;
  const unknown = usage?.unknown_requests ?? usage?.unknownRequests ?? 0;
  const budget =
    usage?.project?.budget_micros ??
    usage?.project?.budgetMicros ??
    project?.budget_micros ??
    project?.budgetMicros;
  const spent = Number(usage?.budget?.spent_micros ?? Number(actual || 0) + Number(estimated || 0));

  const steps = [
    {
      done: providers.length > 0,
      title: '1. 连接供应商',
      note: providers.length ? `已配置 ${providers.length} 个上游连接` : '添加 OpenAI / Anthropic 兼容上游',
      view: 'providers',
      cta: '管理供应商',
    },
    {
      done: aliases.some(isPublished),
      title: '2. 发布模型别名',
      note: aliases.some(isPublished)
        ? `已发布 ${aliases.filter(isPublished).length} 个项目别名`
        : '为上游模型定义稳定的不可变路由版本',
      view: 'aliases',
      cta: '配置别名',
    },
    {
      done: keys.some(isKeyActive),
      title: '3. 创建密钥并调用',
      note: keys.some(isKeyActive)
        ? `${keys.filter(isKeyActive).length} 个有效项目密钥`
        : '签发项目 API Key 并从接入指南发起调用',
      view: 'keys',
      cta: '创建密钥',
    },
  ];
  const allDone = steps.every((s) => s.done);
  const series = arr(usage?.series || usage?.items);
  const maxReq = Math.max(1, ...series.map((s: any) => Number(s.event_count ?? s.requests ?? 0)));

  return (
    <>
      <PageHeader
        title="概览"
        description={`项目 ${project?.name || ''} 的实时请求、微美元账本预算与接入状态。`}
        actions={<UiRangeSegmented value={range} onChange={onRangeChange} />}
      />

      {!allDone ? (
        <section {...stylex.props(ui.card)}>
          <div {...stylex.props(ui.cardHeader)}>
            <div>
              <h3 {...stylex.props(ui.cardTitle)}>完成首次接入闭环</h3>
              <p {...stylex.props(ui.cardSubtitle)}>
                按顺序完成供应商连接、模型别名发布与项目密钥签发。
              </p>
            </div>
            <UiBadge
              tone="info"
              label={`${steps.filter((s) => s.done).length} / 3 已就绪`}
            />
          </div>
          <div {...stylex.props(ui.cardBody, ui.grid3)}>
            {steps.map((s) => (
              <div key={s.title} {...stylex.props(ui.detailItem)}>
                <div {...stylex.props(ui.inlineRow)} style={{ justifyContent: 'space-between' }}>
                  <strong style={{ fontSize: '13px' }}>{s.title}</strong>
                  <UiBadge
                    tone={s.done ? 'success' : 'neutral'}
                    label={s.done ? '已完成' : '待配置'}
                  />
                </div>
                <p style={{ fontSize: '12px', color: 'var(--fg-muted)', margin: '8px 0 12px 0' }}>
                  {s.note}
                </p>
                <UiButton size="sm" variant="secondary" onClick={() => onNavigate(s.view)}>
                  {s.cta}
                </UiButton>
              </div>
            ))}
          </div>
        </section>
      ) : (
        <div {...stylex.props(ui.callout)}>
          接入闭环已就绪。新的调用会实时进入 Redis Lua 热路径账本并投影至请求日志与用量页。
        </div>
      )}

      <div {...stylex.props(ui.metricGrid)}>
        <MetricCard
          label="请求总数"
          value={loading ? '…' : fmtInt(reqs)}
          note={range === '24h' ? '最近 24 小时' : range === '7d' ? '最近 7 天' : '最近 30 天'}
        />
        <MetricCard
          label="成功率"
          value={loading ? '…' : rate === null ? '—' : fmtPct(rate)}
          note={rate === null ? '暂无已鉴权请求' : `${fmtInt(success)} 次完整成功`}
        />
        <MetricCard
          label="未知用量请求"
          value={loading ? '…' : fmtInt(unknown)}
          unit="次"
          note="usage 缺失单独标记，不按 0 结算"
        />
        <MetricCard
          label="已结算费用"
          value={loading ? '…' : fmtMoney(actual)}
          note={estimated ? `另有待核对预估 ${fmtMoney(estimated)}` : '整数微美元精度'}
        />
      </div>

      <div {...stylex.props(ui.grid2)}>
        <section {...stylex.props(ui.card)}>
          <div {...stylex.props(ui.cardHeader)}>
            <div>
              <h3 {...stylex.props(ui.cardTitle)}>按小时请求趋势</h3>
              <p {...stylex.props(ui.cardSubtitle)}>
                最后同步：{usage?.updated_at ? fmtDate(usage.updated_at) : '尚未产生汇总'}
              </p>
            </div>
            {usage?.stale && <UiBadge tone="warning" label="投影同步中" />}
          </div>
          <div {...stylex.props(ui.cardBody)}>
            {series.length ? (
              <>
                <div {...stylex.props(ui.barChart)} role="img" aria-label="按小时请求量柱状趋势">
                  {series.slice(-36).map((s: any, idx: number) => {
                    const count = Number(s.event_count ?? s.requests ?? 0);
                    const hPct = Math.min(100, Math.max(6, Math.round((count / maxReq) * 100)));
                    return (
                      <div
                        key={s.bucket || idx}
                        {...stylex.props(ui.barCol)}
                        title={`${fmtDate(s.bucket)}: ${count} 次事件`}
                      >
                        <div {...stylex.props(ui.barTrack)}>
                          <div {...stylex.props(ui.barFill)} style={{ height: `${hPct}%` }} />
                        </div>
                        <span {...stylex.props(ui.barLabel)}>
                          {fmtDate(s.bucket).slice(0, 5)}
                        </span>
                      </div>
                    );
                  })}
                </div>
                <p {...stylex.props(ui.mutedText)} style={{ marginTop: '8px', marginBottom: 0 }}>
                  展示最近 {Math.min(36, series.length)} 个小时级聚合桶；完整账单与导出请前往用量页。
                </p>
              </>
            ) : (
              <EmptyBox
                title="尚无调用趋势"
                description="当前时间范围内没有已记录请求。发送一次测试请求后即可查看小时级趋势。"
                action={
                  <UiButton size="sm" variant="secondary" onClick={() => onNavigate('guide')}>
                    前往接入指南
                  </UiButton>
                }
              />
            )}
          </div>
        </section>

        <section {...stylex.props(ui.card)}>
          <div {...stylex.props(ui.cardHeader)}>
            <div>
              <h3 {...stylex.props(ui.cardTitle)}>UTC 月度预算水位</h3>
              <p {...stylex.props(ui.cardSubtitle)}>由 @ui-sdk/react/meter 实时渲染</p>
            </div>
            <UiButton size="sm" variant="secondary" onClick={() => onNavigate('usage')}>
              用量与导出
            </UiButton>
          </div>
          <div {...stylex.props(ui.cardBody, ui.stackSm)}>
            {budget !== null && budget !== undefined ? (
              <UiQuotaMeter
                label="本月已用（含预估）"
                spentMicros={spent}
                budgetMicros={Number(budget)}
              />
            ) : (
              <div {...stylex.props(ui.callout)}>
                尚未设置项目月度预算上限。预算控制关闭时，请求仍受 RPM 与并发租约保护。
              </div>
            )}
            <dl {...stylex.props(ui.detailGrid)} style={{ marginTop: '8px' }}>
              <div {...stylex.props(ui.detailItem)}>
                <dt {...stylex.props(ui.detailDt)}>已结算实际费用</dt>
                <dd {...stylex.props(ui.detailDd)}>{fmtMoney(actual)}</dd>
              </div>
              <div {...stylex.props(ui.detailItem)}>
                <dt {...stylex.props(ui.detailDt)}>预估 / 待核对</dt>
                <dd {...stylex.props(ui.detailDd)}>{fmtMoney(estimated)}</dd>
              </div>
              <div {...stylex.props(ui.detailItem)}>
                <dt {...stylex.props(ui.detailDt)}>当前在途预留</dt>
                <dd {...stylex.props(ui.detailDd)}>{fmtMoney(reserved)}</dd>
              </div>
            </dl>
          </div>
        </section>
      </div>

      <section {...stylex.props(ui.card)}>
        <div {...stylex.props(ui.cardHeader)}>
          <div>
            <h3 {...stylex.props(ui.cardTitle)}>最近请求</h3>
            <p {...stylex.props(ui.cardSubtitle)}>仅记录调用元数据，不存储提示词或回答正文。</p>
          </div>
          <UiButton size="sm" variant="secondary" onClick={() => onNavigate('requests')}>
            查看全部请求
          </UiButton>
        </div>
        <div {...stylex.props(ui.cardBodyFlush)}>
          {recent.length ? (
            <div {...stylex.props(ui.tableWrap)}>
              <table {...stylex.props(ui.table)}>
                <thead>
                  <tr>
                    <th {...stylex.props(ui.th)}>时间</th>
                    <th {...stylex.props(ui.th)}>请求 ID</th>
                    <th {...stylex.props(ui.th)}>模型别名</th>
                    <th {...stylex.props(ui.th)}>状态</th>
                    <th {...stylex.props(ui.th, ui.thNum)}>耗时</th>
                    <th {...stylex.props(ui.th, ui.thNum)}>输入 / 输出 Token</th>
                    <th {...stylex.props(ui.th, ui.thNum)}>费用</th>
                    <th {...stylex.props(ui.th)} />
                  </tr>
                </thead>
                <tbody>
                  {recent.map((r: any) => {
                    const id = r.request_id || r.id;
                    const dur =
                      r.duration_ms ??
                      (r.updated_at && r.created_at
                        ? new Date(r.updated_at).getTime() - new Date(r.created_at).getTime()
                        : null);
                    return (
                      <tr key={id}>
                        <td {...stylex.props(ui.td)}>{fmtDate(r.created_at)}</td>
                        <td {...stylex.props(ui.td, ui.tdMono)}>{id}</td>
                        <td {...stylex.props(ui.td)}>{r.alias_name || r.model || '—'}</td>
                        <td {...stylex.props(ui.td)}>
                          <UiBadge status={r.status} />
                        </td>
                        <td {...stylex.props(ui.td, ui.tdNum)}>{fmtDuration(dur)}</td>
                        <td {...stylex.props(ui.td, ui.tdNum)}>
                          {fmtInt(r.input_tokens)} / {fmtInt(r.output_tokens)}
                        </td>
                        <td {...stylex.props(ui.td, ui.tdNum)}>
                          {r.actual_micros != null
                            ? fmtMoney(r.actual_micros)
                            : r.reserve_micros != null
                              ? `预留 ${fmtMoney(r.reserve_micros)}`
                              : '—'}
                        </td>
                        <td {...stylex.props(ui.td, ui.tdActions)}>
                          <UiButton
                            size="sm"
                            variant="outline"
                            onClick={() => openRequestDetail(id)}
                          >
                            详情
                          </UiButton>
                        </td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          ) : (
            <EmptyBox
              title="尚无调用记录"
              description="从接入指南发送一次真实测试请求后，这里会显示请求 ID、耗时与计量明细。"
            />
          )}
        </div>
      </section>

      <UiDialog
        open={!!detailReq}
        onOpenChange={(o) => !o && setDetailReq(null)}
        title="请求详情与上游尝试时间线"
        description={detailReq ? `请求 ID: ${detailReq.request_id || detailReq.id}` : ''}
        wide
        footer={
          <UiButton variant="secondary" onClick={() => setDetailReq(null)}>
            关闭
          </UiButton>
        }
      >
        {detailReq && (
          <div {...stylex.props(ui.stackSm)}>
            <div {...stylex.props(ui.inlineRow)}>
              <UiBadge status={detailReq.status} />
              {detailReq.error_code && (
                <UiBadge tone="danger" label={String(detailReq.error_code)} />
              )}
            </div>
            <dl {...stylex.props(ui.detailGrid)}>
              <div {...stylex.props(ui.detailItem)}>
                <dt {...stylex.props(ui.detailDt)}>模型别名</dt>
                <dd {...stylex.props(ui.detailDd)}>{detailReq.alias_name || '—'}</dd>
              </div>
              <div {...stylex.props(ui.detailItem)}>
                <dt {...stylex.props(ui.detailDt)}>输入 / 输出 Token</dt>
                <dd {...stylex.props(ui.detailDd)}>
                  {fmtInt(detailReq.input_tokens)} / {fmtInt(detailReq.output_tokens)}
                </dd>
              </div>
              <div {...stylex.props(ui.detailItem)}>
                <dt {...stylex.props(ui.detailDt)}>实际结算 / 准入预留</dt>
                <dd {...stylex.props(ui.detailDd)}>
                  {detailReq.actual_micros != null ? fmtMoney(detailReq.actual_micros) : '未知'} ·{' '}
                  {fmtMoney(detailReq.reserve_micros)}
                </dd>
              </div>
            </dl>
            <h4 style={{ margin: '12px 0 4px 0', fontSize: '14px' }}>上游尝试时间线</h4>
            <UiRequestWaterfall detail={detailReq} />
            <UiAttemptTimeline attempts={arr(detailReq.attempts)} targets={targets} />
          </div>
        )}
      </UiDialog>
    </>
  );
}

// ============================================================================
// 2. PROVIDERS & MODEL TARGETS VIEW
// ============================================================================
export function ProvidersView({
  isAdmin,
  refreshTick,
}: {
  isAdmin: boolean;
  refreshTick: number;
}) {
  const [providers, setProviders] = useState<any[]>([]);
  const [targets, setTargets] = useState<any[]>([]);
  const [forbidden, setForbidden] = useState(false);

  // Provider dialog
  const [providerModalOpen, setProviderModalOpen] = useState(false);
  const [editingProvider, setEditingProvider] = useState<any | null>(null);
  const [pName, setPName] = useState('');
  const [pProtocol, setPProtocol] = useState('openai');
  const [pBaseUrl, setPBaseUrl] = useState('');
  const [pSecret, setPSecret] = useState('');
  const [pEnabled, setPEnabled] = useState(true);
  const [pErr, setPErr] = useState('');

  const [deletingProvider, setDeletingProvider] = useState<any | null>(null);

  // Target dialog (Create & Edit with 5-Dimensional Pricing)
  const [targetModalOpen, setTargetModalOpen] = useState(false);
  const [editingTarget, setEditingTarget] = useState<any | null>(null);
  const [tProviderId, setTProviderId] = useState('');
  const [tModel, setTModel] = useState('');
  const [tInputPrice, setTInputPrice] = useState('0.40');
  const [tOutputPrice, setTOutputPrice] = useState('1.60');
  const [tCachedInputPrice, setTCachedInputPrice] = useState('');
  const [tCacheWritePrice, setTCacheWritePrice] = useState('');
  const [tReasoningPrice, setTReasoningPrice] = useState('');
  const [tEnabled, setTEnabled] = useState(true);
  const [tCaps, setTCaps] = useState<string[]>(['chat', 'stream']);
  const [tErr, setTErr] = useState('');

  // Connectivity test confirm & result
  const [testingProvider, setTestingProvider] = useState<any | null>(null);
  const [testResult, setTestResult] = useState<any | null>(null);

  async function reload() {
    const pd = await getSafe('/api/providers');
    if (!pd.ok) {
      if (pd.error.status === 403) {
        setForbidden(true);
        return;
      }
      notify(pd.error.message, 'error');
      return;
    }
    setForbidden(false);
    const pList = items(pd.data);
    setProviders(pList);
    const tList = await loadAllTargets(pList).catch(() => []);
    setTargets(tList);
  }

  useEffect(() => {
    reload();
  }, [refreshTick]);

  function openNewProvider() {
    setEditingProvider(null);
    setPName('');
    setPProtocol('openai');
    setPBaseUrl('');
    setPSecret('');
    setPEnabled(true);
    setPErr('');
    setProviderModalOpen(true);
  }

  function openEditProvider(p: any) {
    setEditingProvider(p);
    setPName(p.name || '');
    setPProtocol(p.protocol || 'openai');
    setPBaseUrl(p.base_url || p.baseUrl || '');
    setPSecret('');
    setPEnabled(p.enabled !== false);
    setPErr('');
    setProviderModalOpen(true);
  }

  async function saveProvider() {
    setPErr('');
    if (!pName.trim() || !pBaseUrl.trim() || (!editingProvider && !pSecret.trim())) {
      setPErr('请填写供应商名称、服务地址与 API 密钥。');
      return;
    }
    try {
      const body: Record<string, any> = {
        name: pName.trim(),
        protocol: pProtocol,
        base_url: pBaseUrl.trim(),
        enabled: pEnabled,
      };
      if (pSecret.trim()) {
        const keyPool = pSecret
          .split(/[\n,]+/)
          .map((k) => k.trim())
          .filter(Boolean);
        if (keyPool.length > 1) {
          body.secrets = keyPool;
        } else if (keyPool.length === 1) {
          body.secret = keyPool[0];
        }
      }
      if (editingProvider?.id) {
        await api(`/api/providers/${encodeURIComponent(editingProvider.id)}`, {
          method: 'PATCH',
          body,
        });
        notify('供应商连接已更新', 'success');
      } else {
        await api('/api/providers', { method: 'POST', body });
        notify('供应商连接已添加', 'success');
      }
      setProviderModalOpen(false);
      await reload();
    } catch (e: any) {
      setPErr(e?.message || '保存供应商失败');
    }
  }

  function openNewTarget(defaultPid?: string) {
    if (!providers.length) {
      notify('请先添加供应商连接', 'error');
      return;
    }
    setEditingTarget(null);
    setTProviderId(defaultPid || providers[0]?.id || '');
    setTModel('');
    setTInputPrice('0.40');
    setTOutputPrice('1.60');
    setTCachedInputPrice('');
    setTCacheWritePrice('');
    setTReasoningPrice('');
    setTEnabled(true);
    setTCaps(['chat', 'stream']);
    setTErr('');
    setTargetModalOpen(true);
  }

  function openEditTarget(t: any) {
    setEditingTarget(t);
    setTProviderId(t.provider_id || t.providerId || providers[0]?.id || '');
    setTModel(t.upstream_model || t.upstreamModel || '');
    setTInputPrice(microsToUsd(t.input_price ?? t.inputPrice) || '0');
    setTOutputPrice(microsToUsd(t.output_price ?? t.outputPrice) || '0');
    setTCachedInputPrice(microsToUsd(t.cached_input_price ?? t.cachedInputPrice));
    setTCacheWritePrice(microsToUsd(t.cache_write_price ?? t.cacheWritePrice));
    setTReasoningPrice(microsToUsd(t.reasoning_price ?? t.reasoningPrice));
    setTEnabled(t.enabled !== false);
    setTCaps(arr(t.capabilities));
    setTErr('');
    setTargetModalOpen(true);
  }

  async function saveTarget() {
    setTErr('');
    const inMicros = usdToMicros(tInputPrice);
    const outMicros = usdToMicros(tOutputPrice);
    const cachedInMicros = tCachedInputPrice.trim() ? usdToMicros(tCachedInputPrice) : null;
    const cacheWriteMicros = tCacheWritePrice.trim() ? usdToMicros(tCacheWritePrice) : null;
    const reasoningMicros = tReasoningPrice.trim() ? usdToMicros(tReasoningPrice) : null;
    if (!tProviderId || !tModel.trim() || inMicros === null || outMicros === null) {
      setTErr('请填写供应商、上游模型标识与非负输入/输出价格。');
      return;
    }
    try {
      const payload: Record<string, any> = {
        upstream_model: tModel.trim(),
        input_price: inMicros,
        output_price: outMicros,
        cached_input_price: cachedInMicros,
        cache_write_price: cacheWriteMicros,
        reasoning_price: reasoningMicros,
        capabilities: tCaps,
        enabled: tEnabled,
      };
      if (editingTarget?.id) {
        await api(`/api/targets/${encodeURIComponent(editingTarget.id)}`, {
          method: 'PATCH',
          body: payload,
        });
        notify('模型目标与多维单价已更新', 'success');
      } else {
        await api(`/api/providers/${encodeURIComponent(tProviderId)}/targets`, {
          method: 'POST',
          body: payload,
        });
        notify('模型目标已添加', 'success');
      }
      setTargetModalOpen(false);
      await reload();
    } catch (e: any) {
      setTErr(e?.message || '保存模型目标失败');
    }
  }

  async function handleDeleteProvider(p: any) {
    try {
      await api(`/api/providers/${encodeURIComponent(p.id)}`, { method: 'DELETE' });
      notify(`供应商“${p.name}”已删除`, 'success');
      await reload();
    } catch (e: any) {
      notify(e?.message || '删除供应商失败（请先解绑引用该供应商模型的路由版本）', 'error');
    }
  }

  async function runProviderTest(p: any) {
    try {
      const r = await api(`/api/providers/${encodeURIComponent(p.id)}/test`, {
        method: 'POST',
      });
      setTestResult({ provider: p, result: r });
      await reload();
    } catch (e: any) {
      notify(e?.message || '连接测试失败', 'error');
    }
  }

  return (
    <>
      <PageHeader
        title="供应商与模型目标"
        description="工作空间全局连接配置。凭据经 AES-GCM 加密存储且不会回显；价格按每百万 Token 的微美元记录。"
        actions={
          isAdmin ? (
            <>
              {providers.length > 0 && (
                <UiButton variant="secondary" onClick={() => openNewTarget()}>
                  添加模型目标
                </UiButton>
              )}
              <UiButton variant="primary" onClick={openNewProvider}>
                添加供应商
              </UiButton>
            </>
          ) : (
            <UiBadge tone="neutral" label="仅管理员可修改供应商" />
          )
        }
      />

      {forbidden ? (
        <EmptyBox
          title="没有查看供应商的权限"
          description="供应商与模型目标仅对管理员和开发者开放。"
        />
      ) : (
        <>
          <section {...stylex.props(ui.card)}>
            <div {...stylex.props(ui.cardHeader)}>
              <div>
                <h3 {...stylex.props(ui.cardTitle)}>供应商连接</h3>
                <p {...stylex.props(ui.cardSubtitle)}>
                  自定义上游地址由服务端执行 DNS 与重定向 SSRF 安全校验。
                </p>
              </div>
              <UiBadge tone="neutral" label={`${providers.length} 个连接`} />
            </div>
            <div {...stylex.props(ui.cardBodyFlush)}>
              {providers.length ? (
                <div {...stylex.props(ui.tableWrap)}>
                  <table {...stylex.props(ui.table)}>
                    <thead>
                      <tr>
                        <th {...stylex.props(ui.th)}>名称 / 协议</th>
                        <th {...stylex.props(ui.th)}>服务地址</th>
                        <th {...stylex.props(ui.th)}>状态 / 最近测试</th>
                        <th {...stylex.props(ui.th, ui.thNum)}>模型目标数</th>
                        <th {...stylex.props(ui.th)} />
                      </tr>
                    </thead>
                    <tbody>
                      {providers.map((p) => {
                        const count = targets.filter(
                          (t) => (t.provider_id || t.providerId) === p.id,
                        ).length;
                        return (
                          <tr key={p.id}>
                            <td {...stylex.props(ui.td)}>
                              <span {...stylex.props(ui.cellPrimary)}>{p.name}</span>
                              <span {...stylex.props(ui.cellSub)}>{p.protocol || 'openai'}</span>
                            </td>
                            <td {...stylex.props(ui.td, ui.tdMono)}>
                              {p.base_url || p.baseUrl || '—'}
                            </td>
                            <td {...stylex.props(ui.td)}>
                              {p.enabled === false ? (
                                <UiBadge status="disabled" label="已禁用" />
                              ) : (
                                <UiBadge
                                  status={p.last_test_status || 'pending'}
                                  label={
                                    p.last_test_status
                                      ? statusText(p.last_test_status)
                                      : '未测试'
                                  }
                                />
                              )}
                              {(p.last_test_at || p.lastTestAt) && (
                                <span {...stylex.props(ui.cellSub)}>
                                  {fmtDate(p.last_test_at || p.lastTestAt)}
                                </span>
                              )}
                            </td>
                            <td {...stylex.props(ui.td, ui.tdNum)}>{fmtInt(count)}</td>
                            <td {...stylex.props(ui.td, ui.tdActions)}>
                              {isAdmin ? (
                                <div
                                  {...stylex.props(ui.inlineRow)}
                                  style={{ justifyContent: 'flex-end' }}
                                >
                                  <UiButton
                                    size="sm"
                                    variant="outline"
                                    onClick={() => setTestingProvider(p)}
                                  >
                                    连通测试
                                  </UiButton>
                                  <UiButton
                                    size="sm"
                                    variant="secondary"
                                    onClick={() => openNewTarget(p.id)}
                                  >
                                    添加模型
                                  </UiButton>
                                  <UiButton
                                    size="sm"
                                    variant="secondary"
                                    onClick={() => openEditProvider(p)}
                                  >
                                    编辑
                                  </UiButton>
                                  <UiButton
                                    size="sm"
                                    variant="outline"
                                    onClick={() => setDeletingProvider(p)}
                                  >
                                    删除
                                  </UiButton>
                                </div>
                              ) : (
                                <span {...stylex.props(ui.mutedText)}>只读</span>
                              )}
                            </td>
                          </tr>
                        );
                      })}
                    </tbody>
                  </table>
                </div>
              ) : (
                <EmptyBox
                  title="尚未连接供应商"
                  description="添加一个经过验证的上游服务后，才能登记模型目标与发布项目别名。"
                  action={
                    isAdmin && (
                      <UiButton size="sm" variant="primary" onClick={openNewProvider}>
                        添加第一个供应商
                      </UiButton>
                    )
                  }
                />
              )}
            </div>
          </section>

          <section {...stylex.props(ui.card)}>
            <div {...stylex.props(ui.cardHeader)}>
              <div>
                <h3 {...stylex.props(ui.cardTitle)}>模型目标（Model Targets）</h3>
                <p {...stylex.props(ui.cardSubtitle)}>
                  登记上游实际模型名、输入/输出单价（微美元 / 1M Token）以及显式协议能力。
                </p>
              </div>
              <UiBadge tone="neutral" label={`${targets.length} 个模型目标`} />
            </div>
            <div {...stylex.props(ui.cardBodyFlush)}>
              {targets.length ? (
                <div {...stylex.props(ui.tableWrap)}>
                  <table {...stylex.props(ui.table)}>
                    <thead>
                      <tr>
                        <th {...stylex.props(ui.th)}>上游模型标识</th>
                        <th {...stylex.props(ui.th)}>所属供应商</th>
                        <th {...stylex.props(ui.th, ui.thNum)}>输入 / 输出单价 (1M)</th>
                        <th {...stylex.props(ui.th, ui.thNum)}>缓存读 / 写 / 推理单价</th>
                        <th {...stylex.props(ui.th)}>声明能力</th>
                        <th {...stylex.props(ui.th)}>状态</th>
                        <th {...stylex.props(ui.th)} />
                      </tr>
                    </thead>
                    <tbody>
                      {targets.map((t) => {
                        const p = providers.find((x) => x.id === (t.provider_id || t.providerId));
                        return (
                          <tr key={t.id}>
                            <td {...stylex.props(ui.td)}>
                              <span {...stylex.props(ui.cellPrimary)}>
                                {t.upstream_model || t.upstreamModel}
                              </span>
                              <span {...stylex.props(ui.cellSub, ui.tdMono)}>{t.id}</span>
                            </td>
                            <td {...stylex.props(ui.td)}>
                              {p?.name || t.provider_name || '—'}
                            </td>
                            <td {...stylex.props(ui.td, ui.tdNum)}>
                              {fmtMoney(t.input_price ?? t.inputPrice)} /{' '}
                              {fmtMoney(t.output_price ?? t.outputPrice)}
                            </td>
                            <td {...stylex.props(ui.td, ui.tdNum)}>
                              <span {...stylex.props(ui.cellPrimary)}>
                                读 {fmtMoney(t.cached_input_price)} · 写 {fmtMoney(t.cache_write_price)}
                              </span>
                              <span {...stylex.props(ui.cellSub)}>
                                推理: {fmtMoney(t.reasoning_price)}
                              </span>
                            </td>
                            <td {...stylex.props(ui.td)}>
                              <div {...stylex.props(ui.inlineRow)}>
                                {arr(t.capabilities).length ? (
                                  arr(t.capabilities).map((c: string) => (
                                    <UiBadge key={c} tone="neutral" label={c} />
                                  ))
                                ) : (
                                  <span {...stylex.props(ui.mutedText)}>未声明</span>
                                )}
                              </div>
                            </td>
                            <td {...stylex.props(ui.td)}>
                              <UiBadge status={t.enabled === false ? 'disabled' : 'enabled'} />
                            </td>
                            <td {...stylex.props(ui.td, ui.tdActions)}>
                              {isAdmin && (
                                <UiButton
                                  size="sm"
                                  variant="secondary"
                                  onClick={() => openEditTarget(t)}
                                >
                                  编辑单价
                                </UiButton>
                              )}
                            </td>
                          </tr>
                        );
                      })}
                    </tbody>
                  </table>
                </div>
              ) : (
                <EmptyBox
                  title="尚无模型目标"
                  description="在已连接的供应商下注册上游模型标识及微美元单价。"
                />
              )}
            </div>
          </section>
        </>
      )}

      {/* Provider Add/Edit Dialog */}
      <UiDialog
        open={providerModalOpen}
        onOpenChange={setProviderModalOpen}
        title={editingProvider ? '编辑供应商连接' : '添加供应商连接'}
        description="API 密钥仅单向加密写入，保存后不可在浏览器读回。"
        wide
        footer={
          <>
            <UiButton variant="outline" onClick={() => setProviderModalOpen(false)}>
              取消
            </UiButton>
            <UiButton variant="primary" onClick={saveProvider}>
              {editingProvider ? '保存更改' : '保存供应商'}
            </UiButton>
          </>
        }
      >
        <div {...stylex.props(ui.formGrid)}>
          <Field name="providerName" {...stylex.props(ui.field)}>
            <FieldLabel {...stylex.props(ui.fieldLabel)}>连接名称</FieldLabel>
            <Input
              value={pName}
              onValueChange={setPName}
              placeholder="例如：OpenAI 主连接"
              {...stylex.props(ui.input)}
            />
          </Field>
          <div {...stylex.props(ui.field)}>
            <label {...stylex.props(ui.fieldLabel)}>上游协议（Protocol）</label>
            <select
              value={pProtocol}
              onChange={(e) => setPProtocol(e.target.value)}
              {...stylex.props(ui.input)}
            >
              <option value="openai">OpenAI 兼容 (openai)</option>
              <option value="anthropic">Anthropic Messages (anthropic)</option>
              <option value="gemini">Google Gemini / Imagen (gemini)</option>
              <option value="cohere">Cohere Embed / Rerank (cohere)</option>
              <option value="jina">Jina Reranker (jina)</option>
              <option value="typesafe">TypeSafe Jev System One (typesafe)</option>
            </select>
          </div>
          <Field name="baseUrl" {...stylex.props(ui.field)}>
            <FieldLabel {...stylex.props(ui.fieldLabel)}>服务地址（Base URL）</FieldLabel>
            <Input
              type="url"
              value={pBaseUrl}
              onValueChange={setPBaseUrl}
              placeholder="https://api.openai.com/v1"
              {...stylex.props(ui.input)}
            />
            <FieldDescription {...stylex.props(ui.fieldHint)}>
              默认要求 HTTPS；私网、环回与云元数据地址会被服务端 SSRF 策略拦截。
            </FieldDescription>
          </Field>
          <Field name="apiKey" {...stylex.props(ui.field)}>
            <FieldLabel {...stylex.props(ui.fieldLabel)}>
              {editingProvider
                ? '替换 API 密钥或多 Key 轮询池（多个密钥用逗号分隔，留空保持原凭据）'
                : '上游 API 密钥或多 Key 轮询池（多个密钥用逗号分隔自动启用负载均衡池）'}
            </FieldLabel>
            <Input
              type="password"
              autoComplete="new-password"
              value={pSecret}
              onValueChange={setPSecret}
              placeholder="sk-key1, sk-key2 (支持单密钥或多 Key 池)"
              {...stylex.props(ui.input)}
            />
          </Field>
          <UiSwitch
            checked={pEnabled}
            onChange={setPEnabled}
            label={pEnabled ? '连接状态：已启用' : '连接状态：已禁用'}
          />
          {pErr && <p {...stylex.props(ui.fieldError)}>{pErr}</p>}
        </div>
      </UiDialog>

      {/* Model Target Add / Edit Dialog with 5D Pricing & Capabilities */}
      <UiDialog
        open={targetModalOpen}
        onOpenChange={setTargetModalOpen}
        title={editingTarget ? `编辑模型目标：${editingTarget.upstream_model}` : '添加模型目标'}
        description="支持输入、输出、缓存读取（Cached Input）、缓存写入（Cache Write）及推理思考（Reasoning）五维微美元计价。"
        wide
        footer={
          <>
            <UiButton variant="outline" onClick={() => setTargetModalOpen(false)}>
              取消
            </UiButton>
            <UiButton variant="primary" onClick={saveTarget}>
              {editingTarget ? '保存单价与能力' : '保存模型目标'}
            </UiButton>
          </>
        }
      >
        <div {...stylex.props(ui.formGrid)}>
          <div {...stylex.props(ui.formRow)}>
            <div {...stylex.props(ui.field)}>
              <label {...stylex.props(ui.fieldLabel)}>所属供应商</label>
              <select
                value={tProviderId}
                disabled={!!editingTarget}
                onChange={(e) => setTProviderId(e.target.value)}
                {...stylex.props(ui.input)}
              >
                {providers.map((p) => (
                  <option key={p.id} value={p.id}>
                    {p.name}
                  </option>
                ))}
              </select>
            </div>
            <Field name="upstreamModel" {...stylex.props(ui.field)}>
              <FieldLabel {...stylex.props(ui.fieldLabel)}>上游模型标识</FieldLabel>
              <Input
                value={tModel}
                onValueChange={setTModel}
                placeholder="例如：gpt-4.1-mini 或 claude-3-7-sonnet"
                {...stylex.props(ui.input)}
              />
            </Field>
          </div>
          <div {...stylex.props(ui.formRow)}>
            <Field name="inputPrice" {...stylex.props(ui.field)}>
              <FieldLabel {...stylex.props(ui.fieldLabel)}>
                输入价格（USD / 1M Token）
              </FieldLabel>
              <Input
                type="number"
                step="0.000001"
                min="0"
                value={tInputPrice}
                onValueChange={setTInputPrice}
                {...stylex.props(ui.input)}
              />
            </Field>
            <Field name="outputPrice" {...stylex.props(ui.field)}>
              <FieldLabel {...stylex.props(ui.fieldLabel)}>
                输出价格（USD / 1M Token）
              </FieldLabel>
              <Input
                type="number"
                step="0.000001"
                min="0"
                value={tOutputPrice}
                onValueChange={setTOutputPrice}
                {...stylex.props(ui.input)}
              />
            </Field>
          </div>
          <div {...stylex.props(ui.formRow)}>
            <Field name="cachedInputPrice" {...stylex.props(ui.field)}>
              <FieldLabel {...stylex.props(ui.fieldLabel)}>
                缓存命中读取单价（Cached Input，可选）
              </FieldLabel>
              <Input
                type="number"
                step="0.000001"
                min="0"
                placeholder="例如：0.10"
                value={tCachedInputPrice}
                onValueChange={setTCachedInputPrice}
                {...stylex.props(ui.input)}
              />
            </Field>
            <Field name="cacheWritePrice" {...stylex.props(ui.field)}>
              <FieldLabel {...stylex.props(ui.fieldLabel)}>
                缓存写入单价（Cache Write，可选）
              </FieldLabel>
              <Input
                type="number"
                step="0.000001"
                min="0"
                placeholder="例如：0.50"
                value={tCacheWritePrice}
                onValueChange={setTCacheWritePrice}
                {...stylex.props(ui.input)}
              />
            </Field>
            <Field name="reasoningPrice" {...stylex.props(ui.field)}>
              <FieldLabel {...stylex.props(ui.fieldLabel)}>
                推理 Token 单价（Reasoning，可选）
              </FieldLabel>
              <Input
                type="number"
                step="0.000001"
                min="0"
                placeholder="例如：2.00"
                value={tReasoningPrice}
                onValueChange={setTReasoningPrice}
                {...stylex.props(ui.input)}
              />
            </Field>
          </div>
          <UiSwitch
            checked={tEnabled}
            onChange={setTEnabled}
            label={tEnabled ? '目标状态：已启用' : '目标状态：已禁用'}
          />
          <div {...stylex.props(ui.field)}>
            <span {...stylex.props(ui.fieldLabel)}>声明模型能力（P0 / P1 协议校验）</span>
            <div {...stylex.props(ui.grid3)}>
              {KNOWN_CAPABILITIES.map((cap) => (
                <UiCheckboxRow
                  key={cap}
                  checked={tCaps.includes(cap)}
                  onChange={(on) =>
                    setTCaps((prev) =>
                      on ? [...new Set([...prev, cap])] : prev.filter((x) => x !== cap),
                    )
                  }
                  title={cap}
                />
              ))}
            </div>
          </div>
          {tErr && <p {...stylex.props(ui.fieldError)}>{tErr}</p>}
        </div>
      </UiDialog>

      {/* Delete Provider Confirm */}
      <UiConfirmDialog
        open={!!deletingProvider}
        onOpenChange={(o) => !o && setDeletingProvider(null)}
        title="删除供应商连接"
        description={
          deletingProvider
            ? `确认删除供应商“${deletingProvider.name}”？若其模型目标正被路由版本引用，服务端将安全拦截删除。`
            : ''
        }
        confirmLabel="确认删除"
        danger
        onConfirm={async () => {
          if (deletingProvider) await handleDeleteProvider(deletingProvider);
        }}
      />

            {/* Connectivity Test Confirm */}
      <UiConfirmDialog
        open={!!testingProvider}
        onOpenChange={(o) => !o && setTestingProvider(null)}
        title="测试上游供应商连通性"
        description={
          testingProvider
            ? `将向“${testingProvider.name}”发起一次真实连通性探测，可能产生微小的上游调用费用。确认继续？`
            : ''
        }
        confirmLabel="开始测试"
        onConfirm={async () => {
          if (testingProvider) await runProviderTest(testingProvider);
        }}
      />

      {/* Connectivity Test Result */}
      <UiDialog
        open={!!testResult}
        onOpenChange={(o) => !o && setTestResult(null)}
        title="连接测试结果"
        footer={
          <UiButton variant="primary" onClick={() => setTestResult(null)}>
            关闭
          </UiButton>
        }
      >
        {testResult && (
          <div {...stylex.props(ui.stackSm)}>
            <div>
              <UiBadge
                status={
                  testResult.result?.ok !== false &&
                  !['failed', 'error'].includes(String(testResult.result?.status || '').toLowerCase())
                    ? 'success'
                    : 'failed'
                }
              />
            </div>
            <dl {...stylex.props(ui.detailGrid)}>
              <div {...stylex.props(ui.detailItem)}>
                <dt {...stylex.props(ui.detailDt)}>测试时间</dt>
                <dd {...stylex.props(ui.detailDd)}>
                  {fmtDate(testResult.result?.tested_at || new Date())}
                </dd>
              </div>
              <div {...stylex.props(ui.detailItem)}>
                <dt {...stylex.props(ui.detailDt)}>延迟</dt>
                <dd {...stylex.props(ui.detailDd)}>
                  {fmtDuration(testResult.result?.latency_ms)}
                </dd>
              </div>
              <div {...stylex.props(ui.detailItem)}>
                <dt {...stylex.props(ui.detailDt)}>状态摘要</dt>
                <dd {...stylex.props(ui.detailDd)}>
                  {testResult.result?.message || statusText(testResult.result?.status)}
                </dd>
              </div>
            </dl>
          </div>
        )}
      </UiDialog>
    </>
  );
}

// ============================================================================
// 3. MODEL ALIASES VIEW (with P1 One-Click Version Rollback!)
// ============================================================================
export function AliasesView({
  project,
  isAdmin,
  onNavigate,
  refreshTick,
}: {
  project: any;
  isAdmin: boolean;
  onNavigate: (view: string) => void;
  refreshTick: number;
}) {
  const [aliases, setAliases] = useState<any[]>([]);
  const [targets, setTargets] = useState<any[]>([]);

  // Create / Draft version modal
  const [modalOpen, setModalOpen] = useState(false);
  const [editingAlias, setEditingAlias] = useState<any | null>(null);
  const [aliasName, setAliasName] = useState('');
  const [selectedTargets, setSelectedTargets] = useState<string[]>([]);
  const [targetWeights, setTargetWeights] = useState<Record<string, number>>({});
  const [selectedCaps, setSelectedCaps] = useState<string[]>([]);
  const [formErr, setFormErr] = useState('');

  // Publish confirm
  const [publishingAlias, setPublishingAlias] = useState<any | null>(null);

  // P5 Version History & Diff Rollback Modal
  const [rollbackAlias, setRollbackAlias] = useState<any | null>(null);
  const [versionHistory, setVersionHistory] = useState<any[]>([]);
  const [rollbackVer, setRollbackVer] = useState('1');
  const [rollbackErr, setRollbackErr] = useState('');

  async function reload() {
    if (!project?.id) return;
    const pid = encodeURIComponent(project.id);
    const ad = await getSafe(`/api/aliases?project_id=${pid}`);
    const aList = ad.ok ? items(ad.data) : [];
    setAliases(aList);
    let tList = await loadAllTargets().catch(() => []);
    const embedded = aList.flatMap((a: any) => arr(a.target_details || a.targetDetails));
    if (embedded.length) {
      const merged = [...tList, ...embedded];
      tList = merged.filter((t, i, all) => t?.id && all.findIndex((x) => x.id === t.id) === i);
    }
    setTargets(tList);
  }

  useEffect(() => {
    reload();
  }, [project?.id, refreshTick]);

  function openCreateAlias() {
    setEditingAlias(null);
    setAliasName('');
    setSelectedTargets([]);
    setTargetWeights({});
    setSelectedCaps(['chat', 'stream']);
    setFormErr('');
    setModalOpen(true);
  }

  function openEditVersion(a: any) {
    setEditingAlias(a);
    setAliasName(a.name || '');
    const tids = targetIdsOf(a);
    setSelectedTargets(tids);
    const snapWeights = a.price_snapshot?.weights || a.current_version?.price_snapshot?.weights || {};
    const nextWeights: Record<string, number> = {};
    tids.forEach((id) => {
      nextWeights[id] = Number(snapWeights[id] ?? 100);
    });
    setTargetWeights(nextWeights);
    setSelectedCaps(arr(a.draft_version?.capabilities ?? a.capabilities));
    setFormErr('');
    setModalOpen(true);
  }

  function toggleTarget(id: string, checked: boolean) {
    if (checked) {
      if (selectedTargets.length >= 8) {
        notify('每个路由版本最多选择 8 个目标节点', 'error');
        return;
      }
      setSelectedTargets([...selectedTargets, id]);
      setTargetWeights((prev) => ({ ...prev, [id]: prev[id] ?? 100 }));
    } else {
      setSelectedTargets(selectedTargets.filter((x) => x !== id));
    }
  }

  async function saveAliasDraft() {
    setFormErr('');
    if (!aliasName.trim() || selectedTargets.length < 1) {
      setFormErr('请填写别名并至少选择 1 个模型目标。');
      return;
    }
    if (selectedTargets.length > 8) {
      setFormErr('每个路由版本最多选择 8 个目标。');
      return;
    }
    const weightsPayload: Record<string, number> = {};
    selectedTargets.forEach((id) => {
      weightsPayload[id] = Math.max(0, Math.min(10000, Number(targetWeights[id] ?? 100)));
    });
    try {
      if (editingAlias) {
        await api(`/api/aliases/${encodeURIComponent(editingAlias.id)}/versions`, {
          method: 'POST',
          body: {
            targets: selectedTargets,
            weights: weightsPayload,
            capabilities: selectedCaps,
          },
        });
        notify('新草稿版本（含目标权重）已保存，检查无误后即可发布', 'success');
      } else {
        await api('/api/aliases', {
          method: 'POST',
          body: {
            project_id: project.id,
            name: aliasName.trim(),
            targets: selectedTargets,
            weights: weightsPayload,
            capabilities: selectedCaps,
          },
        });
        notify('别名草稿已创建，发布后即可用于调用', 'success');
      }
      setModalOpen(false);
      await reload();
    } catch (e: any) {
      setFormErr(e?.message || '保存别名草稿失败');
    }
  }

  async function publishDraft(a: any) {
    try {
      await api(`/api/aliases/${encodeURIComponent(a.id)}/publish`, {
        method: 'POST',
        body: {
          expectedVersion:
            a.draft_version?.version ?? a.current_version?.version ?? a.current_version ?? a.version ?? null,
        },
      });
      notify(`别名“${a.name}”已发布生效`, 'success');
      await reload();
    } catch (e: any) {
      notify(e?.message || '发布失败', 'error');
      await reload();
    }
  }

  async function openRollbackModal(a: any) {
    const curVer = Number(a.current_version?.version ?? a.current_version ?? a.version ?? 1);
    setRollbackAlias(a);
    setRollbackVer(String(Math.max(1, curVer - 1)));
    setRollbackErr('');
    setVersionHistory([]);
    const res = await getSafe(`/api/aliases/${encodeURIComponent(a.id)}/versions`);
    if (res.ok) {
      setVersionHistory(items(res.data));
    }
  }

  async function executeRollback() {
    if (!rollbackAlias) return;
    setRollbackErr('');
    const ver = Number(rollbackVer);
    if (!Number.isInteger(ver) || ver < 1) {
      setRollbackErr('请输入有效的历史已发布版本号（正整数）。');
      return;
    }
    try {
      await api(`/api/aliases/${encodeURIComponent(rollbackAlias.id)}/rollback`, {
        method: 'POST',
        body: { target_version: ver },
      });
      notify(`已将“${rollbackAlias.name}”回滚至版本 v${ver} 并生成新发布版本`, 'success');
      setRollbackAlias(null);
      await reload();
    } catch (e: any) {
      setRollbackErr(e?.message || '回滚版本失败');
    }
  }

  const allCaps = [
    ...new Set([...KNOWN_CAPABILITIES, ...targets.flatMap((t) => arr(t.capabilities))]),
  ];

  return (
    <>
      <PageHeader
        title="模型别名与不可变路由"
        description="将稳定的项目级模型别名映射到 1 主 + 1 备目标。发布后的版本不可变，支持乐观锁发布与一键历史版本回滚（P1-F）。"
        actions={
          isAdmin ? (
            <UiButton variant="primary" onClick={openCreateAlias}>
              新建模型别名
            </UiButton>
          ) : (
            <UiBadge tone="neutral" label="仅管理员可变更路由" />
          )
        }
      />

      {!targets.length && (
        <div {...stylex.props(ui.callout, ui.calloutWarning)}>
          当前工作空间尚未登记模型目标。请先在“供应商”页添加上游模型目标后再发布别名。{' '}
          <UiButton size="sm" variant="secondary" onClick={() => onNavigate('providers')}>
            前往供应商
          </UiButton>
        </div>
      )}

      <section {...stylex.props(ui.card)}>
        <div {...stylex.props(ui.cardHeader)}>
          <div>
            <h3 {...stylex.props(ui.cardTitle)}>项目模型别名</h3>
            <p {...stylex.props(ui.cardSubtitle)}>
              数据面 `/v1/chat/completions`、`/v1/messages` 与 `/v1/embeddings` 中的 `model` 参数使用该别名。
            </p>
          </div>
          <UiBadge tone="neutral" label={`${aliases.length} 个别名`} />
        </div>
        <div {...stylex.props(ui.cardBodyFlush)}>
          {aliases.length ? (
            <div {...stylex.props(ui.tableWrap)}>
              <table {...stylex.props(ui.table)}>
                <thead>
                  <tr>
                    <th {...stylex.props(ui.th)}>别名</th>
                    <th {...stylex.props(ui.th)}>路由目标（主 / 备）</th>
                    <th {...stylex.props(ui.th)}>声明能力</th>
                    <th {...stylex.props(ui.th, ui.thNum)}>当前版本</th>
                    <th {...stylex.props(ui.th)}>状态</th>
                    <th {...stylex.props(ui.th)} />
                  </tr>
                </thead>
                <tbody>
                  {aliases.map((a) => {
                    const ids = targetIdsOf(a);
                    const hasDraft = !!a.draft_version;
                    const hasCurrent = !!a.current_version_id;
                    const curVer =
                      a.current_version?.version ?? a.current_version ?? a.version ?? null;
                    const draftVer = a.draft_version?.version ?? null;
                    const caps = arr(a.capabilities);
                    return (
                      <tr key={a.id}>
                        <td {...stylex.props(ui.td)}>
                          <span {...stylex.props(ui.cellPrimary)}>{a.name}</span>
                          <span {...stylex.props(ui.cellSub, ui.tdMono)}>{a.id}</span>
                        </td>
                        <td {...stylex.props(ui.td)}>
                          <div {...stylex.props(ui.inlineRow)}>
                            {ids.length ? (
                              ids.map((id, i) => (
                                <UiBadge
                                  key={id}
                                  tone={i === 0 ? 'info' : 'neutral'}
                                  label={`${i === 0 ? '主: ' : '备: '}${targetLabel(id, targets)}`}
                                />
                              ))
                            ) : (
                              <span {...stylex.props(ui.mutedText)}>无目标</span>
                            )}
                          </div>
                        </td>
                        <td {...stylex.props(ui.td)}>
                          <div {...stylex.props(ui.inlineRow)}>
                            {caps.length ? (
                              caps.map((c: string) => (
                                <UiBadge key={c} tone="neutral" label={c} />
                              ))
                            ) : (
                              <span {...stylex.props(ui.mutedText)}>基础对话</span>
                            )}
                          </div>
                        </td>
                        <td {...stylex.props(ui.td, ui.tdNum)}>
                          {curVer ? `v${curVer}` : '—'}
                          {draftVer && (
                            <span {...stylex.props(ui.cellSub)}>草稿 v{draftVer}</span>
                          )}
                        </td>
                        <td {...stylex.props(ui.td)}>
                          <UiBadge
                            status={hasDraft ? 'draft' : hasCurrent ? 'published' : 'pending'}
                            label={
                              hasDraft && hasCurrent
                                ? '已发布 · 有草稿'
                                : hasDraft
                                  ? '草稿待发布'
                                  : hasCurrent
                                    ? '已发布'
                                    : '未发布'
                            }
                          />
                        </td>
                        <td {...stylex.props(ui.td, ui.tdActions)}>
                          {isAdmin ? (
                            <div
                              {...stylex.props(ui.inlineRow)}
                              style={{ justifyContent: 'flex-end' }}
                            >
                              <UiButton
                                size="sm"
                                variant="secondary"
                                onClick={() => openEditVersion(a)}
                              >
                                {hasDraft ? '编辑草稿' : '新建版本'}
                              </UiButton>
                              {hasDraft && (
                                <UiButton
                                  size="sm"
                                  variant="primary"
                                  onClick={() => setPublishingAlias(a)}
                                >
                                  发布草稿
                                </UiButton>
                              )}
                              {hasCurrent && (
                                <UiButton
                                  size="sm"
                                  variant="outline"
                                  onClick={() => openRollbackModal(a)}
                                >
                                  版本历史与回滚
                                </UiButton>
                              )}
                            </div>
                          ) : (
                            <span {...stylex.props(ui.mutedText)}>只读</span>
                          )}
                        </td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          ) : (
            <EmptyBox
              title="尚未创建模型别名"
              description="创建一个项目内稳定的模型别名并绑定主备上游目标。"
              action={
                isAdmin &&
                targets.length > 0 && (
                  <UiButton size="sm" variant="primary" onClick={openCreateAlias}>
                    新建模型别名
                  </UiButton>
                )
              }
            />
          )}
        </div>
      </section>

      {/* Create / Edit Version Dialog */}
      <UiDialog
        open={modalOpen}
        onOpenChange={setModalOpen}
        title={editingAlias ? `编辑新草稿版本：${editingAlias.name}` : '新建模型别名'}
        description="按勾选顺序决定主目标（第 1 个）与备用目标（第 2 个）。所有选中目标必须覆盖所声明的能力。"
        wide
        footer={
          <>
            <UiButton variant="outline" onClick={() => setModalOpen(false)}>
              取消
            </UiButton>
            <UiButton variant="primary" onClick={saveAliasDraft}>
              {editingAlias ? '保存为新草稿' : '创建别名草稿'}
            </UiButton>
          </>
        }
      >
        <div {...stylex.props(ui.formGrid)}>
          <Field name="aliasName" {...stylex.props(ui.field)}>
            <FieldLabel {...stylex.props(ui.fieldLabel)}>模型别名</FieldLabel>
            <Input
              value={aliasName}
              onValueChange={setAliasName}
              readOnly={!!editingAlias}
              placeholder="例如：support-chat 或 text-embedding"
              {...stylex.props(ui.input)}
            />
          </Field>
          <div {...stylex.props(ui.field)}>
            <span {...stylex.props(ui.fieldLabel)}>
              选择目标模型与负载均衡权重（支持 1~8 个目标节点，已选 {selectedTargets.length}/8）
            </span>
            <div {...stylex.props(ui.stackSm)}>
              {targets.map((t) => {
                const idx = selectedTargets.indexOf(t.id);
                const checked = idx !== -1;
                const totalW = selectedTargets.reduce(
                  (acc, tid) => acc + Math.max(0, Number(targetWeights[tid] ?? 100)),
                  0,
                );
                const curW = Number(targetWeights[t.id] ?? 100);
                const pct = checked && totalW > 0 ? Math.round((curW / totalW) * 100) : 0;
                return (
                  <div key={t.id} {...stylex.props(ui.detailItem, ui.stackSm)}>
                    <UiCheckboxRow
                      checked={checked}
                      onChange={(on) => toggleTarget(t.id, on)}
                      title={
                        <span>
                          {t.upstream_model}{' '}
                          {checked && (
                            <UiBadge
                              tone={idx === 0 ? 'info' : 'neutral'}
                              label={`#${idx + 1} (${idx === 0 ? '主' : '备/池'} · 权重 ${curW} / ${pct}%)`}
                            />
                          )}
                        </span>
                      }
                      subtitle={`${t.provider_name || '上游供应商'} · 输入 ${fmtMoney(t.input_price)} / 输出 ${fmtMoney(t.output_price)} · 能力: ${arr(t.capabilities).join(', ') || '无'}`}
                    />
                    {checked && (
                      <div {...stylex.props(ui.inlineRow)} style={{ paddingLeft: '28px' }}>
                        <span {...stylex.props(ui.mutedText)}>节点流量权重 (0~10000)：</span>
                        <input
                          type="number"
                          min="0"
                          max="10000"
                          value={curW}
                          onChange={(e) =>
                            setTargetWeights((prev) => ({
                              ...prev,
                              [t.id]: Number(e.target.value) || 0,
                            }))
                          }
                          {...stylex.props(ui.input)}
                          style={{ width: '110px', height: '28px' }}
                        />
                      </div>
                    )}
                  </div>
                );
              })}
            </div>
          </div>
          <div {...stylex.props(ui.field)}>
            <span {...stylex.props(ui.fieldLabel)}>声明路由版本能力</span>
            <div {...stylex.props(ui.grid3)}>
              {allCaps.map((c) => (
                <UiCheckboxRow
                  key={c}
                  checked={selectedCaps.includes(c)}
                  onChange={(on) =>
                    setSelectedCaps((prev) =>
                      on ? [...new Set([...prev, c])] : prev.filter((x) => x !== c),
                    )
                  }
                  title={c}
                />
              ))}
            </div>
          </div>
          {formErr && <p {...stylex.props(ui.fieldError)}>{formErr}</p>}
        </div>
      </UiDialog>

      {/* Publish Confirm */}
      <UiConfirmDialog
        open={!!publishingAlias}
        onOpenChange={(o) => !o && setPublishingAlias(null)}
        title="发布模型别名草稿"
        description={
          publishingAlias
            ? `确认将“${publishingAlias.name}”的最新草稿发布为当前生产路由版本？已有在途请求将继续使用原版本快照结算。`
            : ''
        }
        confirmLabel="立即发布"
        onConfirm={async () => {
          if (publishingAlias) await publishDraft(publishingAlias);
        }}
      />

      {/* P1-F Rollback Dialog */}
      <UiDialog
        open={!!rollbackAlias}
        onOpenChange={(o) => !o && setRollbackAlias(null)}
        title={`回滚模型别名：${rollbackAlias?.name || ''}`}
        description="基于指定的历史已发布版本（target_version）克隆并立即发布为最新生产版本，同时通知 Redis 热路径刷新缓存。"
        footer={
          <>
            <UiButton variant="outline" onClick={() => setRollbackAlias(null)}>
              取消
            </UiButton>
            <UiButton variant="primary" onClick={executeRollback}>
              确认回滚并发布
            </UiButton>
          </>
        }
      >
        <div {...stylex.props(ui.formGrid)}>
          {versionHistory.length > 0 && (
            <div {...stylex.props(ui.tableWrap)}>
              <table {...stylex.props(ui.table)}>
                <thead>
                  <tr>
                    <th {...stylex.props(ui.th)}>版本</th>
                    <th {...stylex.props(ui.th)}>状态</th>
                    <th {...stylex.props(ui.th)}>目标与权重</th>
                    <th {...stylex.props(ui.th)}>能力集</th>
                    <th {...stylex.props(ui.th)} />
                  </tr>
                </thead>
                <tbody>
                  {versionHistory.map((v) => (
                    <tr key={v.id}>
                      <td {...stylex.props(ui.td, ui.tdMono)}>v{v.version}</td>
                      <td {...stylex.props(ui.td)}>
                        <UiBadge status={v.status} />
                      </td>
                      <td {...stylex.props(ui.td)}>
                        <div {...stylex.props(ui.inlineRow)}>
                          {arr(v.targets).map((tid: string, i: number) => (
                            <UiBadge
                              key={tid}
                              tone={i === 0 ? 'info' : 'neutral'}
                              label={`${targetLabel(tid, targets)} (w:${v.weights?.[tid] ?? 100})`}
                            />
                          ))}
                        </div>
                      </td>
                      <td {...stylex.props(ui.td)}>{arr(v.capabilities).join(', ') || '—'}</td>
                      <td {...stylex.props(ui.td, ui.tdActions)}>
                        {v.status === 'published' && (
                          <UiButton
                            size="sm"
                            variant={String(v.version) === rollbackVer ? 'primary' : 'outline'}
                            onClick={() => setRollbackVer(String(v.version))}
                          >
                            选择回滚至 v{v.version}
                          </UiButton>
                        )}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
          <Field name="targetVersion" {...stylex.props(ui.field)}>
            <FieldLabel {...stylex.props(ui.fieldLabel)}>目标历史已发布版本号</FieldLabel>
            <Input
              type="number"
              min="1"
              step="1"
              value={rollbackVer}
              onValueChange={setRollbackVer}
              {...stylex.props(ui.input)}
            />
          </Field>
          {rollbackErr && <p {...stylex.props(ui.fieldError)}>{rollbackErr}</p>}
        </div>
      </UiDialog>
    </>
  );
}

// ============================================================================
// 4. API KEYS VIEW (with P1 key_rpm per-key rate limit configuration!)
// ============================================================================
export function KeysView({
  project,
  canWrite,
  refreshTick,
}: {
  project: any;
  canWrite: boolean;
  refreshTick: number;
}) {
  const [keys, setKeys] = useState<any[]>([]);
  const [aliases, setAliases] = useState<any[]>([]);
  const [mcpServers, setMcpServers] = useState<any[]>([]);

  // Create key dialog
  const [createOpen, setCreateOpen] = useState(false);
  const [kName, setKName] = useState('');
  const [kDays, setKDays] = useState('30');
  const [kRpm, setKRpm] = useState('');
  const [kTpm, setKTpm] = useState('');
  const [kBudgetUsd, setKBudgetUsd] = useState('');
  const [kModels, setKModels] = useState<string[]>([]);
  const [kMcpTools, setKMcpTools] = useState<string[]>([]);
  const [kErr, setKErr] = useState('');

  // Created key secret one-time display
  const [createdKey, setCreatedKey] = useState<any | null>(null);

  // Revoke confirm
  const [revokingKey, setRevokingKey] = useState<any | null>(null);

  async function reload() {
    if (!project?.id) return;
    const pid = encodeURIComponent(project.id);
    const [kd, ad, md] = await Promise.all([
      getSafe(`/api/keys?project_id=${pid}`),
      getSafe(`/api/aliases?project_id=${pid}`),
      getSafe('/api/mcp-servers'),
    ]);
    if (kd.ok) setKeys(items(kd.data));
    if (ad.ok) setAliases(items(ad.data));
    if (md.ok) setMcpServers(items(md.data));
  }

  useEffect(() => {
    reload();
  }, [project?.id, refreshTick]);

  function openCreateKey() {
    setKName('');
    setKDays('30');
    setKRpm('');
    setKTpm('');
    setKBudgetUsd('');
    setKModels([]);
    setKMcpTools([]);
    setKErr('');
    setCreateOpen(true);
  }

  async function handleCreateKey() {
    setKErr('');
    if (!kName.trim()) {
      setKErr('请输入密钥名称。');
      return;
    }
    const days = Number(kDays);
    if (!Number.isInteger(days) || days < 1 || days > 365) {
      setKErr('有效期需为 1–365 天之间的整数。');
      return;
    }
    const rpmVal = kRpm.trim() === '' ? null : Number(kRpm);
    if (rpmVal !== null && (!Number.isInteger(rpmVal) || rpmVal < 1)) {
      setKErr('单 Key RPM 限流需为正整数（留空则继承项目 RPM）。');
      return;
    }
    try {
      const expiresAt = new Date(Date.now() + days * 86400000).toISOString();
      const tpmVal = kTpm.trim() === '' ? null : Number(kTpm);
      const budgetMicrosVal = kBudgetUsd.trim() === '' ? null : usdToMicros(kBudgetUsd);
      const body: Record<string, any> = {
        project_id: project.id,
        name: kName.trim(),
        allowed_models: kModels,
        allowed_mcp_tools: kMcpTools,
        expires_at: expiresAt,
      };
      if (rpmVal !== null) body.key_rpm = rpmVal;
      if (tpmVal !== null && Number.isInteger(tpmVal) && tpmVal > 0) body.key_tpm = tpmVal;
      if (budgetMicrosVal !== null) body.budget_micros = budgetMicrosVal;
      const r = await api('/api/keys', { method: 'POST', body });
      setCreateOpen(false);
      setCreatedKey(r);
      notify('项目 API Key 已创建', 'success');
      await reload();
    } catch (e: any) {
      setKErr(e?.message || '创建密钥失败');
    }
  }

  async function handleRotateKey(k: any) {
    try {
      const r = await api(`/api/keys/${encodeURIComponent(k.id)}/rotate`, {
        method: 'POST',
        body: { revoke_old: true },
      });
      setCreatedKey(r);
      notify(`已为“${k.name}”完成零停机密钥轮换并吊销旧密钥`, 'success');
      await reload();
    } catch (e: any) {
      notify(e?.message || '轮换密钥失败', 'error');
    }
  }

  async function handleRevokeKey(k: any) {
    try {
      await api(`/api/keys/${encodeURIComponent(k.id)}/revoke`, { method: 'POST' });
      notify(`密钥“${k.name}”已吊销`, 'success');
      await reload();
    } catch (e: any) {
      notify(e?.message || '吊销密钥失败', 'error');
    }
  }

  const activeCount = keys.filter(isKeyActive).length;
  const publishedAliases = aliases.filter(isPublished);

  return (
    <>
      <PageHeader
        title="API Keys"
        description="项目密钥用于数据面 Bearer 或 x-api-key 鉴权。完整明文仅在签发时展示一次，支持按模型范围与单 Key RPM 速率限制。"
        actions={
          canWrite ? (
            <UiButton variant="primary" onClick={openCreateKey}>
              创建项目密钥
            </UiButton>
          ) : (
            <UiBadge tone="neutral" label="只读角色不可管理密钥" />
          )
        }
      />

      <div {...stylex.props(ui.metricGrid)}>
        <MetricCard
          label="有效密钥"
          value={fmtInt(activeCount)}
          unit="个"
          note={`当前项目共 ${keys.length} 个密钥`}
        />
        <MetricCard
          label="最近鉴权使用"
          value={fmtDate(
            keys
              .map((k) => k.last_used_at || k.lastUsedAt)
              .filter(Boolean)
              .sort()
              .at(-1),
          )}
          note="按最近一次数据面请求记录"
        />
        <MetricCard
          label="配置单 Key RPM"
          value={fmtInt(keys.filter((k) => k.key_rpm != null).length)}
          unit="个"
          note="独立于项目级 RPM 的细粒度限流"
        />
        <MetricCard
          label="已吊销密钥"
          value={fmtInt(keys.filter((k) => k.revoked_at || k.revokedAt).length)}
          unit="个"
          note="吊销后立即通过 Redis Pub/Sub 拦截新请求"
        />
      </div>

      <section {...stylex.props(ui.card)}>
        <div {...stylex.props(ui.cardHeader)}>
          <div>
            <h3 {...stylex.props(ui.cardTitle)}>项目密钥列表</h3>
            <p {...stylex.props(ui.cardSubtitle)}>
              仅保存 SHA-256 摘要与公开前缀，吊销即时生效。
            </p>
          </div>
          <UiBadge tone="neutral" label={`${keys.length} 个密钥`} />
        </div>
        <div {...stylex.props(ui.cardBodyFlush)}>
          {keys.length ? (
            <div {...stylex.props(ui.tableWrap)}>
              <table {...stylex.props(ui.table)}>
                <thead>
                  <tr>
                    <th {...stylex.props(ui.th)}>名称 / 前缀</th>
                    <th {...stylex.props(ui.th)}>模型与 MCP 工具白名单</th>
                    <th {...stylex.props(ui.th, ui.thNum)}>单 Key RPM / TPM / 预算</th>
                    <th {...stylex.props(ui.th)}>状态</th>
                    <th {...stylex.props(ui.th)}>到期时间</th>
                    <th {...stylex.props(ui.th)}>最近使用</th>
                    <th {...stylex.props(ui.th)} />
                  </tr>
                </thead>
                <tbody>
                  {keys.map((k) => {
                    const active = isKeyActive(k);
                    const status = !active
                      ? k.revoked_at
                        ? 'revoked'
                        : 'expired'
                      : k.expires_at &&
                          new Date(k.expires_at).getTime() - Date.now() < 7 * 86400000
                        ? 'expiring'
                        : 'active';
                    const allowed = arr(k.allowed_models ?? k.allowedModels);
                    return (
                      <tr key={k.id}>
                        <td {...stylex.props(ui.td)}>
                          <span {...stylex.props(ui.cellPrimary)}>{k.name}</span>
                          <span {...stylex.props(ui.cellSub, ui.tdMono)}>
                            {k.prefix || 'sk-'}…
                          </span>
                        </td>
                        <td {...stylex.props(ui.td)}>
                          <div {...stylex.props(ui.inlineRow)}>
                            {allowed.length ? (
                              allowed.map((m: string) => (
                                <UiBadge key={m} tone="neutral" label={m} />
                              ))
                            ) : (
                              <span {...stylex.props(ui.mutedText)}>全部别名</span>
                            )}
                            {arr(k.allowed_mcp_tools).map((mt: string) => (
                              <UiBadge key={mt} tone="info" label={`MCP:${mt}`} />
                            ))}
                          </div>
                        </td>
                        <td {...stylex.props(ui.td, ui.tdNum)}>
                          <span {...stylex.props(ui.cellPrimary)}>
                            {k.key_rpm != null ? `${fmtInt(k.key_rpm)} RPM` : '项目 RPM'} ·{' '}
                            {k.key_tpm != null ? `${fmtInt(k.key_tpm)} TPM` : '项目 TPM'}
                          </span>
                          <span {...stylex.props(ui.cellSub)}>
                            预算上限: {k.budget_micros != null ? fmtMoney(k.budget_micros) : '无独立上限'}
                          </span>
                        </td>
                        <td {...stylex.props(ui.td)}>
                          <UiBadge
                            status={status === 'expiring' ? 'unknown' : status}
                            label={status === 'expiring' ? '即将到期' : undefined}
                          />
                        </td>
                        <td {...stylex.props(ui.td)}>
                          {k.expires_at ? fmtDate(k.expires_at) : '不过期'}
                        </td>
                        <td {...stylex.props(ui.td)}>{fmtDate(k.last_used_at)}</td>
                        <td {...stylex.props(ui.td, ui.tdActions)}>
                          {active && canWrite ? (
                            <div
                              {...stylex.props(ui.inlineRow)}
                              style={{ justifyContent: 'flex-end' }}
                            >
                              <UiButton
                                size="sm"
                                variant="secondary"
                                onClick={() => handleRotateKey(k)}
                              >
                                一键轮换
                              </UiButton>
                              <UiButton
                                size="sm"
                                variant="outline"
                                onClick={() => setRevokingKey(k)}
                              >
                                吊销
                              </UiButton>
                            </div>
                          ) : (
                            <span {...stylex.props(ui.mutedText)}>—</span>
                          )}
                        </td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          ) : (
            <EmptyBox
              title="尚未创建项目密钥"
              description="创建密钥后请立即保存完整明文；建议为不同业务服务签发独立密钥。"
              action={
                canWrite && (
                  <UiButton size="sm" variant="primary" onClick={openCreateKey}>
                    创建第一个密钥
                  </UiButton>
                )
              }
            />
          )}
        </div>
      </section>

      {/* Create Key Dialog */}
      <UiDialog
        open={createOpen}
        onOpenChange={setCreateOpen}
        title="创建项目 API Key"
        description="支持限制该密钥允许调用的模型别名列表以及独立的每分钟请求数上限（key_rpm）。"
        wide
        footer={
          <>
            <UiButton variant="outline" onClick={() => setCreateOpen(false)}>
              取消
            </UiButton>
            <UiButton variant="primary" onClick={handleCreateKey}>
              签发密钥
            </UiButton>
          </>
        }
      >
        <div {...stylex.props(ui.formGrid)}>
          <Field name="keyName" {...stylex.props(ui.field)}>
            <FieldLabel {...stylex.props(ui.fieldLabel)}>密钥名称</FieldLabel>
            <Input
              value={kName}
              onValueChange={setKName}
              placeholder="例如：support-agent-prod"
              {...stylex.props(ui.input)}
            />
          </Field>
          <div {...stylex.props(ui.formRow)}>
            <Field name="expiresInDays" {...stylex.props(ui.field)}>
              <FieldLabel {...stylex.props(ui.fieldLabel)}>有效期（天，1–365）</FieldLabel>
              <Input
                type="number"
                min="1"
                max="365"
                value={kDays}
                onValueChange={setKDays}
                {...stylex.props(ui.input)}
              />
            </Field>
            <Field name="keyRpm" {...stylex.props(ui.field)}>
              <FieldLabel {...stylex.props(ui.fieldLabel)}>
                单 Key RPM 上限（可选）
              </FieldLabel>
              <Input
                type="number"
                min="1"
                placeholder="共享项目 RPM"
                value={kRpm}
                onValueChange={setKRpm}
                {...stylex.props(ui.input)}
              />
            </Field>
            <Field name="keyTpm" {...stylex.props(ui.field)}>
              <FieldLabel {...stylex.props(ui.fieldLabel)}>
                单 Key TPM 上限（Tokens/min，可选）
              </FieldLabel>
              <Input
                type="number"
                min="1"
                placeholder="共享项目 TPM"
                value={kTpm}
                onValueChange={setKTpm}
                {...stylex.props(ui.input)}
              />
            </Field>
            <Field name="keyBudgetUsd" {...stylex.props(ui.field)}>
              <FieldLabel {...stylex.props(ui.fieldLabel)}>
                单 Key 独立预算上限（USD，可选）
              </FieldLabel>
              <Input
                type="number"
                min="0"
                step="0.01"
                placeholder="例如：10.00"
                value={kBudgetUsd}
                onValueChange={setKBudgetUsd}
                {...stylex.props(ui.input)}
              />
            </Field>
          </div>
          <div {...stylex.props(ui.field)}>
            <span {...stylex.props(ui.fieldLabel)}>
              允许调用的模型别名（不勾选代表允许当前项目全部已发布别名）
            </span>
            {publishedAliases.length ? (
              <div {...stylex.props(ui.grid3)}>
                {publishedAliases.map((a) => (
                  <UiCheckboxRow
                    key={a.id}
                    checked={kModels.includes(a.name)}
                    onChange={(on) =>
                      setKModels((prev) =>
                        on ? [...new Set([...prev, a.name])] : prev.filter((x) => x !== a.name),
                      )
                    }
                    title={a.name}
                  />
                ))}
              </div>
            ) : (
              <p {...stylex.props(ui.mutedText)}>当前项目尚无已发布别名，可先直接创建密钥。</p>
            )}
          </div>
          <div {...stylex.props(ui.field)}>
            <span {...stylex.props(ui.fieldLabel)}>
              允许调用的 MCP 工具白名单（allowed_mcp_tools，不勾选代表不限制）
            </span>
            {mcpServers.length ? (
              <div {...stylex.props(ui.grid3)}>
                {mcpServers.flatMap((srv) => {
                  const patterns = [
                    `${srv.alias}:*`,
                    ...arr(srv.allowedTools).map((t: string) => `${srv.alias}:${t}`),
                  ];
                  return patterns.map((pat) => (
                    <UiCheckboxRow
                      key={pat}
                      checked={kMcpTools.includes(pat)}
                      onChange={(on) =>
                        setKMcpTools((prev) =>
                          on ? [...new Set([...prev, pat])] : prev.filter((x) => x !== pat),
                        )
                      }
                      title={pat}
                    />
                  ));
                })}
              </div>
            ) : (
              <p {...stylex.props(ui.mutedText)}>
                当前工作空间暂未注册 MCP Server（可前往「MCP 工具网关」注册）。
              </p>
            )}
          </div>
          {kErr && <p {...stylex.props(ui.fieldError)}>{kErr}</p>}
        </div>
      </UiDialog>

      {/* One-Time Plaintext Secret Modal */}
      <UiDialog
        open={!!createdKey}
        onOpenChange={(o) => !o && setCreatedKey(null)}
        title="项目密钥已签发"
        description="完整明文密钥仅在此窗口展示一次，关闭后服务端仅保留 SHA-256 哈希，无法再次读取。"
        wide
        footer={
          <UiButton variant="primary" onClick={() => setCreatedKey(null)}>
            我已妥善保存，关闭
          </UiButton>
        }
      >
        {createdKey && (
          <div {...stylex.props(ui.formGrid)}>
            <div {...stylex.props(ui.callout, ui.calloutWarning)}>
              请立即复制并存入服务端密钥管理或环境变量，切勿硬编码在前端代码仓库中。
            </div>
            <div {...stylex.props(ui.copyRow)}>
              <code
                style={{
                  flex: 1,
                  fontFamily: 'var(--font-mono)',
                  fontSize: '13px',
                  wordBreak: 'break-all',
                }}
              >
                {createdKey.key || createdKey.secret}
              </code>
              <UiButton
                size="sm"
                variant="secondary"
                onClick={() => copyToClipboard(createdKey.key || createdKey.secret || '')}
              >
                复制密钥
              </UiButton>
            </div>
          </div>
        )}
      </UiDialog>

      {/* Revoke Confirm */}
      <UiConfirmDialog
        open={!!revokingKey}
        onOpenChange={(o) => !o && setRevokingKey(null)}
        title="吊销 API Key"
        description={
          revokingKey
            ? `确认立即吊销“${revokingKey.name}”（${revokingKey.prefix}…）？后续携带该密钥的所有请求将立即返回 401。`
            : ''
        }
        confirmLabel="确认吊销"
        danger
        onConfirm={async () => {
          if (revokingKey) await handleRevokeKey(revokingKey);
        }}
      />
    </>
  );
}
