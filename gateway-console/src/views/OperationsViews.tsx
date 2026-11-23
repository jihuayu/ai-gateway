import React, { useEffect, useState } from 'react';
import * as stylex from '@stylexjs/stylex';
import { Tabs, TabsList, TabsPanel, TabsTab } from '@ui-sdk/react/tabs';
import { Field, FieldDescription, FieldLabel } from '@ui-sdk/react/field';
import { Input } from '@ui-sdk/react/input';
import { ui } from '../styles/ui';
import {
  API_BASE,
  api,
  arr,
  fmtDate,
  fmtDuration,
  fmtInt,
  fmtMoney,
  fmtPct,
  getLastRequestId,
  getSafe,
  isPublished,
  items,
  microsToUsd,
  rangeBounds,
  roleName,
  statusText,
  usdToMicros,
} from '../api/client';
import {
  EmptyBox,
  MetricCard,
  PageHeader,
  UiAttemptTimeline,
  UiBadge,
  UiButton,
  UiCheckboxRow,
  UiConfirmDialog,
  UiDialog,
  UiQuotaMeter,
  UiRangeSegmented,
  consoleAgent,
  copyToClipboard,
  notify,
} from '../components/Primitives';

// ============================================================================
// 5. REQUESTS LOG & TIMELINE VIEW
// ============================================================================
export function RequestsView({
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
  const [statusFilter, setStatusFilter] = useState('');
  const [modelFilter, setModelFilter] = useState('');
  const [appliedStatus, setAppliedStatus] = useState('');
  const [appliedModel, setAppliedModel] = useState('');
  const [rows, setRows] = useState<any[]>([]);
  const [nextCursor, setNextCursor] = useState<string | null>(null);
  const [baseQuery, setBaseQuery] = useState('');
  const [detailReq, setDetailReq] = useState<any | null>(null);

  async function loadInitial() {
    if (!project?.id) return;
    const b = rangeBounds(range);
    const sp = new URLSearchParams({
      project_id: project.id,
      from: b.from,
      to: b.to,
      limit: '50',
    });
    if (appliedStatus) sp.set('status', appliedStatus);
    if (appliedModel) sp.set('model', appliedModel);
    const qs = sp.toString();
    setBaseQuery(qs);
    try {
      const r = await api(`/api/requests?${qs}`);
      setRows(items(r));
      setNextCursor(r?.next_cursor || null);
    } catch (e: any) {
      notify(e?.message || '读取请求日志失败', 'error');
    }
  }

  useEffect(() => {
    loadInitial();
  }, [project?.id, range, appliedStatus, appliedModel, refreshTick]);

  async function loadMore() {
    if (!nextCursor || !baseQuery) return;
    try {
      const r = await api(`/api/requests?${baseQuery}&cursor=${encodeURIComponent(nextCursor)}`);
      const more = items(r);
      setRows((prev) => [...prev, ...more]);
      setNextCursor(r?.next_cursor || null);
      notify(`已加载 ${more.length} 条记录`, 'info');
    } catch (e: any) {
      notify(e?.message || '加载更多请求失败', 'error');
    }
  }

  async function openDetail(id: string) {
    try {
      const r = await api(`/api/requests/${encodeURIComponent(id)}`);
      setDetailReq(r);
    } catch (e: any) {
      notify(e?.message || '读取请求详情失败', 'error');
    }
  }

  return (
    <>
      <PageHeader
        title="请求日志"
        description="按项目查询请求元数据与上游主备尝试时间线。支持复合游标分页；不保存提示词、回答正文或鉴权头。"
        actions={<UiRangeSegmented value={range} onChange={onRangeChange} />}
      />

      <section {...stylex.props(ui.card)}>
        <div {...stylex.props(ui.cardHeader)}>
          <div>
            <h3 {...stylex.props(ui.cardTitle)}>筛选条件</h3>
            <p {...stylex.props(ui.cardSubtitle)}>按状态或模型别名精确过滤半开时间区间内的请求。</p>
          </div>
        </div>
        <div {...stylex.props(ui.cardBody)}>
          <div {...stylex.props(ui.formRow)}>
            <div {...stylex.props(ui.field)}>
              <label {...stylex.props(ui.fieldLabel)}>请求状态</label>
              <select
                value={statusFilter}
                onChange={(e) => setStatusFilter(e.target.value)}
                {...stylex.props(ui.input)}
              >
                <option value="">全部状态</option>
                {['completed', 'failed', 'rejected', 'partial', 'cancelled', 'unknown'].map((s) => (
                  <option key={s} value={s}>
                    {statusText(s)}
                  </option>
                ))}
              </select>
            </div>
            <Field name="modelFilter" {...stylex.props(ui.field)}>
              <FieldLabel {...stylex.props(ui.fieldLabel)}>模型别名</FieldLabel>
              <Input
                value={modelFilter}
                onValueChange={setModelFilter}
                placeholder="例如：support-chat"
                {...stylex.props(ui.input)}
              />
            </Field>
            <div {...stylex.props(ui.inlineRow)}>
              <UiButton
                variant="primary"
                onClick={() => {
                  setAppliedStatus(statusFilter);
                  setAppliedModel(modelFilter.trim());
                }}
              >
                应用筛选
              </UiButton>
              {(appliedStatus || appliedModel) && (
                <UiButton
                  variant="outline"
                  onClick={() => {
                    setStatusFilter('');
                    setModelFilter('');
                    setAppliedStatus('');
                    setAppliedModel('');
                  }}
                >
                  重置
                </UiButton>
              )}
            </div>
          </div>
        </div>
      </section>

      <section {...stylex.props(ui.card)}>
        <div {...stylex.props(ui.cardHeader)}>
          <div>
            <h3 {...stylex.props(ui.cardTitle)}>请求列表</h3>
            <p {...stylex.props(ui.cardSubtitle)}>
              当前已加载 {rows.length} 条{nextCursor ? '，可继续游标翻页' : '，已到末页'}
            </p>
          </div>
        </div>
        <div {...stylex.props(ui.cardBodyFlush)}>
          {rows.length ? (
            <div {...stylex.props(ui.tableWrap)}>
              <table {...stylex.props(ui.table)}>
                <thead>
                  <tr>
                    <th {...stylex.props(ui.th)}>创建时间</th>
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
                  {rows.map((r) => {
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
                          <UiButton size="sm" variant="outline" onClick={() => openDetail(id)}>
                            时间线详情
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
              title={appliedStatus || appliedModel ? '没有符合筛选条件的请求' : '尚无请求记录'}
              description="可调整时间范围或前往接入指南发送一次测试调用。"
              action={
                !appliedStatus &&
                !appliedModel && (
                  <UiButton size="sm" variant="secondary" onClick={() => onNavigate('guide')}>
                    发送测试调用
                  </UiButton>
                )
              }
            />
          )}
        </div>
        {nextCursor && (
          <div {...stylex.props(ui.cardFooter)}>
            <span {...stylex.props(ui.mutedText)}>按 (created_at, request_id) 倒序游标分页</span>
            <UiButton size="sm" variant="secondary" onClick={loadMore}>
              加载更多
            </UiButton>
          </div>
        )}
      </section>

      <UiDialog
        open={!!detailReq}
        onOpenChange={(o) => !o && setDetailReq(null)}
        title="请求详情与 Attempt 时间线"
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
                <dt {...stylex.props(ui.detailDt)}>密钥 ID</dt>
                <dd {...stylex.props(ui.detailDd, ui.tdMono)}>{detailReq.key_id || '—'}</dd>
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
                  {detailReq.actual_micros != null
                    ? fmtMoney(detailReq.actual_micros)
                    : '未知（不按 0 计）'}{' '}
                  · {fmtMoney(detailReq.reserve_micros)}
                </dd>
              </div>
              <div {...stylex.props(ui.detailItem)}>
                <dt {...stylex.props(ui.detailDt)}>创建时间</dt>
                <dd {...stylex.props(ui.detailDd)}>{fmtDate(detailReq.created_at)}</dd>
              </div>
              <div {...stylex.props(ui.detailItem)}>
                <dt {...stylex.props(ui.detailDt)}>更新时间</dt>
                <dd {...stylex.props(ui.detailDd)}>{fmtDate(detailReq.updated_at)}</dd>
              </div>
            </dl>
            <h4 style={{ margin: '12px 0 4px 0', fontSize: '14px' }}>
              上游尝试时间线（@ui-sdk/react/timeline）
            </h4>
            <UiAttemptTimeline attempts={arr(detailReq.attempts)} targets={[]} />
          </div>
        )}
      </UiDialog>
    </>
  );
}

// ============================================================================
// 6. USAGE & BILLING VIEW (with P1-E CSV / JSON Export!)
// ============================================================================
export function UsageView({
  project,
  isAdmin,
  range,
  onRangeChange,
  refreshTick,
}: {
  project: any;
  isAdmin: boolean;
  range: string;
  onRangeChange: (r: string) => void;
  refreshTick: number;
}) {
  const [u, setU] = useState<any>({});
  const [breakdown, setBreakdown] = useState<any>({ by_model: [], by_key: [], token_summary: {} });
  const [budgetInput, setBudgetInput] = useState('');

  async function reload() {
    if (!project?.id) return;
    const b = rangeBounds(range);
    const qs = `project_id=${encodeURIComponent(project.id)}&from=${encodeURIComponent(b.from)}&to=${encodeURIComponent(b.to)}`;
    try {
      const [data, bdRes] = await Promise.all([
        api(`/api/usage?${qs}`),
        getSafe(`/api/usage/breakdown?${qs}`),
      ]);
      setU(data || {});
      if (bdRes.ok) setBreakdown(bdRes.data || {});
      const lim = data?.project?.budget_micros ?? project?.budget_micros;
      setBudgetInput(microsToUsd(lim));
    } catch (e: any) {
      notify(e?.message || '读取用量统计失败', 'error');
    }
  }

  useEffect(() => {
    reload();
  }, [project?.id, range, refreshTick]);

  async function saveBudget() {
    const micros = budgetInput.trim() === '' ? null : usdToMicros(budgetInput);
    if (micros !== null && micros < 0) {
      notify('月度预算不能为负数', 'error');
      return;
    }
    try {
      await api(`/api/projects/${encodeURIComponent(project.id)}`, {
        method: 'PATCH',
        body: { budget_micros: micros, budgetMicros: micros },
      });
      notify(micros === null ? '月度预算上限已关闭' : '月度预算已更新', 'success');
      await reload();
    } catch (e: any) {
      notify(e?.message || '更新月度预算失败', 'error');
    }
  }

  function exportBill(format: 'csv' | 'json') {
    if (!project?.id) return;
    const b = rangeBounds(range);
    const url = `${API_BASE}/api/usage/export?project_id=${encodeURIComponent(project.id)}&from=${encodeURIComponent(b.from)}&to=${encodeURIComponent(b.to)}&format=${format}`;
    window.open(url, '_blank', 'noopener,noreferrer');
    notify(`已触发 ${format.toUpperCase()} 用量账单导出`, 'success');
  }

  const budget = u?.project?.budget_micros ?? project?.budget_micros ?? null;
  const actual = u?.actual_micros ?? u?.summary?.actual_micros;
  const estimated = u?.estimated_micros ?? u?.summary?.estimated_micros;
  const reserved = u?.budget?.reserved_micros ?? u?.reserved_micros;
  const unknown = u?.unknown_requests ?? 0;
  const reqs = Number(u?.requests ?? 0);
  const success = Number(u?.successful_requests ?? NaN);
  const rate = Number.isFinite(success) && reqs > 0 ? success / reqs : null;
  const spent = Number(u?.budget?.spent_micros ?? Number(actual || 0) + Number(estimated || 0));
  const series = arr(u?.series || u?.items);

  return (
    <>
      <PageHeader
        title="用量与微美元账本"
        description="已结算、未知用量预估与实时在途预留分开呈现。支持一键导出小时级 CSV / JSON 对账单（P1-E）。"
        actions={
          <>
            <UiButton size="sm" variant="outline" onClick={() => exportBill('csv')}>
              导出 CSV 账单
            </UiButton>
            <UiButton size="sm" variant="outline" onClick={() => exportBill('json')}>
              导出 JSON 账单
            </UiButton>
            <UiRangeSegmented value={range} onChange={onRangeChange} />
          </>
        }
      />

      {u?.stale && (
        <div {...stylex.props(ui.callout, ui.calloutWarning)}>
          Redis Stream 账本事件正在后台增量投影至 PostgreSQL（待投影队列：{fmtInt(u.ingestion_pending_events)} 条），实时准入限额不受影响。
        </div>
      )}

      <div {...stylex.props(ui.metricGrid)}>
        <MetricCard label="已结算费用" value={fmtMoney(actual)} note="来自真实 usage 的结算事件" />
        <MetricCard
          label="预估 / 待核对"
          value={fmtMoney(estimated)}
          note={`${fmtInt(unknown)} 次未知用量请求`}
        />
        <MetricCard
          label="请求总数"
          value={fmtInt(reqs)}
          unit="次"
          note={`完整成功 ${fmtInt(success)} 次`}
        />
        <MetricCard
          label="成功率"
          value={rate === null ? '—' : fmtPct(rate)}
          note="流式中断记为部分响应"
        />
      </div>

      <div {...stylex.props(ui.grid2)}>
        <section {...stylex.props(ui.card)}>
          <div {...stylex.props(ui.cardHeader)}>
            <div>
              <h3 {...stylex.props(ui.cardTitle)}>UTC 月度预算控制</h3>
              <p {...stylex.props(ui.cardSubtitle)}>
                账期：{u?.budget?.period || '当前 UTC 月'} · 实时 Redis Lua 原子扣减
              </p>
            </div>
          </div>
          <div {...stylex.props(ui.cardBody, ui.formGrid)}>
            {budget !== null && budget !== undefined ? (
              <UiQuotaMeter
                label="UTC 本月累计消耗"
                spentMicros={spent}
                budgetMicros={Number(budget)}
              />
            ) : (
              <div {...stylex.props(ui.callout)}>
                当前项目未启用月度预算硬上限。
              </div>
            )}
            {isAdmin ? (
              <div {...stylex.props(ui.formRow)}>
                <Field name="budgetUsd" {...stylex.props(ui.field)}>
                  <FieldLabel {...stylex.props(ui.fieldLabel)}>
                    月度预算上限（USD，留空关闭）
                  </FieldLabel>
                  <Input
                    type="number"
                    min="0"
                    step="0.000001"
                    value={budgetInput}
                    onValueChange={setBudgetInput}
                    placeholder="例如：50.00"
                    {...stylex.props(ui.input)}
                  />
                </Field>
                <UiButton variant="primary" onClick={saveBudget}>
                  保存预算
                </UiButton>
              </div>
            ) : (
              <p {...stylex.props(ui.mutedText)}>仅管理员可修改项目月度预算。</p>
            )}
          </div>
        </section>

        <section {...stylex.props(ui.card)}>
          <div {...stylex.props(ui.cardHeader)}>
            <h3 {...stylex.props(ui.cardTitle)}>Token 与费用构成</h3>
          </div>
          <div {...stylex.props(ui.cardBody)}>
            <dl {...stylex.props(ui.detailGrid)}>
              <div {...stylex.props(ui.detailItem)}>
                <dt {...stylex.props(ui.detailDt)}>实际已结算</dt>
                <dd {...stylex.props(ui.detailDd)}>{fmtMoney(actual)}</dd>
              </div>
              <div {...stylex.props(ui.detailItem)}>
                <dt {...stylex.props(ui.detailDt)}>未知用量预估</dt>
                <dd {...stylex.props(ui.detailDd)}>{fmtMoney(estimated)}</dd>
              </div>
              <div {...stylex.props(ui.detailItem)}>
                <dt {...stylex.props(ui.detailDt)}>实时在途预留</dt>
                <dd {...stylex.props(ui.detailDd)}>{fmtMoney(reserved)}</dd>
              </div>
              <div {...stylex.props(ui.detailItem)}>
                <dt {...stylex.props(ui.detailDt)}>累计输入 Token</dt>
                <dd {...stylex.props(ui.detailDd)}>{fmtInt(u?.input_tokens)}</dd>
              </div>
              <div {...stylex.props(ui.detailItem)}>
                <dt {...stylex.props(ui.detailDt)}>累计输出 Token</dt>
                <dd {...stylex.props(ui.detailDd)}>{fmtInt(u?.output_tokens)}</dd>
              </div>
              <div {...stylex.props(ui.detailItem)}>
                <dt {...stylex.props(ui.detailDt)}>未知用量请求</dt>
                <dd {...stylex.props(ui.detailDd)}>{fmtInt(unknown)} 次</dd>
              </div>
            </dl>
          </div>
        </section>
      </div>

      <div {...stylex.props(ui.grid2)}>
        <section {...stylex.props(ui.card)}>
          <div {...stylex.props(ui.cardHeader)}>
            <div>
              <h3 {...stylex.props(ui.cardTitle)}>FinOps 按模型别名成本与 TTFT 拆解</h3>
              <p {...stylex.props(ui.cardSubtitle)}>
                包含缓存命中 Token（Cached Input）、推理 Token（Reasoning）与平均首字延迟。
              </p>
            </div>
            <UiBadge
              tone="info"
              label={`缓存命中率 ${fmtPct(breakdown?.token_summary?.cache_hit_ratio ?? 0)}`}
            />
          </div>
          <div {...stylex.props(ui.cardBodyFlush)}>
            {arr(breakdown?.by_model).length ? (
              <div {...stylex.props(ui.tableWrap)}>
                <table {...stylex.props(ui.table)}>
                  <thead>
                    <tr>
                      <th {...stylex.props(ui.th)}>模型别名</th>
                      <th {...stylex.props(ui.th, ui.thNum)}>请求数</th>
                      <th {...stylex.props(ui.th, ui.thNum)}>入 / 出 / 缓存 / 推理 Token</th>
                      <th {...stylex.props(ui.th, ui.thNum)}>实结费用</th>
                    </tr>
                  </thead>
                  <tbody>
                    {arr(breakdown?.by_model).map((m: any) => (
                      <tr key={m.model}>
                        <td {...stylex.props(ui.td)}>
                          <span {...stylex.props(ui.cellPrimary)}>{m.model}</span>
                          <span {...stylex.props(ui.cellSub)}>
                            平均 TTFT: {m.avg_ttft_ms ? fmtDuration(m.avg_ttft_ms) : '—'}
                          </span>
                        </td>
                        <td {...stylex.props(ui.td, ui.tdNum)}>{fmtInt(m.requests)}</td>
                        <td {...stylex.props(ui.td, ui.tdNum)}>
                          {fmtInt(m.input_tokens)} / {fmtInt(m.output_tokens)} /{' '}
                          {fmtInt(m.cached_input_tokens)} / {fmtInt(m.reasoning_tokens)}
                        </td>
                        <td {...stylex.props(ui.td, ui.tdNum)}>{fmtMoney(m.actual_micros)}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            ) : (
              <EmptyBox title="暂无模型维度拆解数据" description="发起调用后自动按模型汇总。" />
            )}
          </div>
        </section>

        <section {...stylex.props(ui.card)}>
          <div {...stylex.props(ui.cardHeader)}>
            <div>
              <h3 {...stylex.props(ui.cardTitle)}>FinOps 按 API Key 成本归因拆解</h3>
              <p {...stylex.props(ui.cardSubtitle)}>
                追踪各业务服务或团队密钥的独立微美元消耗。
              </p>
            </div>
          </div>
          <div {...stylex.props(ui.cardBodyFlush)}>
            {arr(breakdown?.by_key).length ? (
              <div {...stylex.props(ui.tableWrap)}>
                <table {...stylex.props(ui.table)}>
                  <thead>
                    <tr>
                      <th {...stylex.props(ui.th)}>密钥名称 / 前缀</th>
                      <th {...stylex.props(ui.th, ui.thNum)}>请求数</th>
                      <th {...stylex.props(ui.th, ui.thNum)}>输入 / 输出 Token</th>
                      <th {...stylex.props(ui.th, ui.thNum)}>实结费用</th>
                    </tr>
                  </thead>
                  <tbody>
                    {arr(breakdown?.by_key).map((k: any) => (
                      <tr key={k.key_id}>
                        <td {...stylex.props(ui.td)}>
                          <span {...stylex.props(ui.cellPrimary)}>{k.key_name}</span>
                          <span {...stylex.props(ui.cellSub, ui.tdMono)}>{k.key_prefix}…</span>
                        </td>
                        <td {...stylex.props(ui.td, ui.tdNum)}>{fmtInt(k.requests)}</td>
                        <td {...stylex.props(ui.td, ui.tdNum)}>
                          {fmtInt(k.input_tokens)} / {fmtInt(k.output_tokens)}
                        </td>
                        <td {...stylex.props(ui.td, ui.tdNum)}>{fmtMoney(k.actual_micros)}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            ) : (
              <EmptyBox title="暂无密钥维度拆解数据" description="发起调用后自动按密钥汇总。" />
            )}
          </div>
        </section>
      </div>

      <section {...stylex.props(ui.card)}>
        <div {...stylex.props(ui.cardHeader)}>
          <div>
            <h3 {...stylex.props(ui.cardTitle)}>按小时增量聚合明细（usage_hourly）</h3>
            <p {...stylex.props(ui.cardSubtitle)}>
              由 HotProjector 通过 O(1) 增量 UPSERT 写入。
            </p>
          </div>
          <UiBadge tone="neutral" label={`${series.length} 个小时桶`} />
        </div>
        <div {...stylex.props(ui.cardBodyFlush)}>
          {series.length ? (
            <div {...stylex.props(ui.tableWrap)}>
              <table {...stylex.props(ui.table)}>
                <thead>
                  <tr>
                    <th {...stylex.props(ui.th)}>时间桶（UTC）</th>
                    <th {...stylex.props(ui.th, ui.thNum)}>账本事件数</th>
                    <th {...stylex.props(ui.th, ui.thNum)}>已结算费用</th>
                    <th {...stylex.props(ui.th, ui.thNum)}>预估费用</th>
                    <th {...stylex.props(ui.th, ui.thNum)}>输入 Token</th>
                    <th {...stylex.props(ui.th, ui.thNum)}>输出 Token</th>
                  </tr>
                </thead>
                <tbody>
                  {series
                    .slice()
                    .reverse()
                    .map((row: any) => (
                      <tr key={row.bucket}>
                        <td {...stylex.props(ui.td)}>{fmtDate(row.bucket)}</td>
                        <td {...stylex.props(ui.td, ui.tdNum)}>{fmtInt(row.event_count)}</td>
                        <td {...stylex.props(ui.td, ui.tdNum)}>{fmtMoney(row.actual_micros)}</td>
                        <td {...stylex.props(ui.td, ui.tdNum)}>
                          {fmtMoney(row.estimated_micros)}
                        </td>
                        <td {...stylex.props(ui.td, ui.tdNum)}>{fmtInt(row.input_tokens)}</td>
                        <td {...stylex.props(ui.td, ui.tdNum)}>{fmtInt(row.output_tokens)}</td>
                      </tr>
                    ))}
                </tbody>
              </table>
            </div>
          ) : (
            <EmptyBox
              title="暂无按小时聚合明细"
              description="产生真实调用并完成结算后，这里将按小时展示微美元账单。"
            />
          )}
        </div>
      </section>
    </>
  );
}

// ============================================================================
// 7. GUIDE, MULTI-PROTOCOL PLAYGROUND & UI-SDK AGENT INSPECTOR VIEW
// ============================================================================
export function GuideView({
  project,
  canWrite,
  onNavigate,
  refreshTick,
}: {
  project: any;
  canWrite: boolean;
  onNavigate: (view: string) => void;
  refreshTick: number;
}) {
  const [aliases, setAliases] = useState<any[]>([]);
  const [protoTab, setProtoTab] = useState<'openai' | 'anthropic' | 'embeddings'>('openai');
  const [testMode, setTestMode] = useState<
    'chat' | 'anthropic' | 'embeddings' | 'rerank' | 'images' | 'systemone'
  >('chat');
  const [testModel, setTestModel] = useState('');
  const [testMaxTokens, setTestMaxTokens] = useState('128');
  const [testPrompt, setTestPrompt] = useState('用一句话解释为什么企业级 AI 网关需要整数微美元预留账本。');
  const [testDocsInput, setTestDocsInput] = useState(
    'Unrelated cooking recipe\nRedis Lua 3-phase micro-USD billing ledger',
  );
  const [testOut, setTestOut] = useState<any | null>(null);
  const [agentSnap, setAgentSnap] = useState<any[] | null>(null);

  useEffect(() => {
    if (!project?.id) return;
    getSafe(`/api/aliases?project_id=${encodeURIComponent(project.id)}`).then((ad) => {
      if (ad.ok) {
        const pub = items(ad.data).filter(isPublished);
        setAliases(pub);
        if (pub.length && !testModel) setTestModel(pub[0].name);
      }
    });
  }, [project?.id, refreshTick]);

  const origin = typeof window !== 'undefined' ? window.location.origin : 'http://localhost:3000';
  const modelName = testModel || aliases[0]?.name || 'support-chat';

  const openaiCurl = `curl -N "${origin}/v1/chat/completions" \\
  -H "Authorization: Bearer $AI_GATEWAY_API_KEY" \\
  -H "Content-Type: application/json" \\
  -d '{
    "model": "${modelName}",
    "messages": [{"role": "user", "content": "你好"}],
    "stream": true,
    "max_tokens": 256
  }'`;

  const anthropicCurl = `curl "${origin}/v1/messages" \\
  -H "x-api-key: $AI_GATEWAY_API_KEY" \\
  -H "anthropic-version: 2023-06-01" \\
  -H "Content-Type: application/json" \\
  -d '{
    "model": "${modelName}",
    "max_tokens": 256,
    "messages": [{"role": "user", "content": "你好"}]
  }'`;

  const embeddingsCurl = `curl "${origin}/v1/embeddings" \\
  -H "Authorization: Bearer $AI_GATEWAY_API_KEY" \\
  -H "Content-Type: application/json" \\
  -d '{
    "model": "${modelName}",
    "input": ["AI Gateway micro-USD hot ledger"]
  }'`;

  async function runTestCall() {
    if (!modelName || !testPrompt.trim()) return;
    const started = performance.now();
    try {
      const docs = testDocsInput
        .split('\n')
        .map((d) => d.trim())
        .filter(Boolean);
      const r = await api('/api/test', {
        method: 'POST',
        body: {
          project_id: project.id,
          mode: testMode,
          model: modelName,
          prompt: testPrompt.trim(),
          query: testPrompt.trim(),
          documents: docs,
          state: testPrompt.trim(),
          messages: [{ role: 'user', content: testPrompt.trim() }],
          max_tokens: Number(testMaxTokens) || 128,
        },
      });
      const elapsed = performance.now() - started;
      const content =
        r?.choices?.[0]?.message?.content ??
        r?.content?.[0]?.text ??
        (r?.results
          ? JSON.stringify(r.results, null, 2)
          : r?.answers
            ? JSON.stringify(r.answers, null, 2)
            : JSON.stringify(r, null, 2));
      setTestOut({
        ok: true,
        elapsed,
        requestId: r?.request_id || getLastRequestId() || '—',
        content,
        raw: r,
        usage: r?.usage || {},
        model: r?.model || modelName,
      });
      notify(`多模态受控测试 (${testMode}) 已完成`, 'success');
    } catch (e: any) {
      setTestOut({
        ok: false,
        error: e?.message || '测试调用失败',
      });
    }
  }

  function inspectUiSdkAgent() {
    try {
      const snap = consoleAgent.snapshot();
      setAgentSnap(Array.isArray(snap) ? snap : [snap]);
      notify('已生成 @ui-sdk/react/agent 页面组件自省快照', 'info');
    } catch (e: any) {
      notify(e?.message || '读取 Agent 快照失败', 'error');
    }
  }

  return (
    <>
      <PageHeader
        title="多协议接入指南与在线调试"
        description="支持 OpenAI Chat Completions、Anthropic Messages (/v1/messages) 与 Embeddings (/v1/embeddings) 三大协议端点（P1-A / P1-C / P1-D）。"
      />

      <section {...stylex.props(ui.card)}>
        <div {...stylex.props(ui.cardHeader)}>
          <div>
            <h3 {...stylex.props(ui.cardTitle)}>多协议调用示例</h3>
            <p {...stylex.props(ui.cardSubtitle)}>
              由 @ui-sdk/react/tabs 驱动切换；所有响应均携带 x-request-id 追踪头。
            </p>
          </div>
          <UiButton
            size="sm"
            variant="secondary"
            onClick={() =>
              copyToClipboard(
                protoTab === 'openai'
                  ? openaiCurl
                  : protoTab === 'anthropic'
                    ? anthropicCurl
                    : embeddingsCurl,
              )
            }
          >
            复制当前命令
          </UiButton>
        </div>
        <div {...stylex.props(ui.cardBody)}>
          <Tabs
            value={protoTab}
            onValueChange={(v) => {
              if (v === 'openai' || v === 'anthropic' || v === 'embeddings') setProtoTab(v);
            }}
          >
            <TabsList aria-label="协议示例选择" {...stylex.props(ui.tabsList)}>
              <TabsTab
                value="openai"
                className={(s) =>
                  stylex.props(ui.tabsTab, s.selected && ui.tabsTabSelected).className
                }
              >
                OpenAI (/v1/chat/completions)
              </TabsTab>
              <TabsTab
                value="anthropic"
                className={(s) =>
                  stylex.props(ui.tabsTab, s.selected && ui.tabsTabSelected).className
                }
              >
                Anthropic (/v1/messages)
              </TabsTab>
              <TabsTab
                value="embeddings"
                className={(s) =>
                  stylex.props(ui.tabsTab, s.selected && ui.tabsTabSelected).className
                }
              >
                Embeddings (/v1/embeddings)
              </TabsTab>
            </TabsList>

            <TabsPanel value="openai" style={{ marginTop: '14px' }}>
              <pre {...stylex.props(ui.codeBlock)}>
                <code>{openaiCurl}</code>
              </pre>
            </TabsPanel>
            <TabsPanel value="anthropic" style={{ marginTop: '14px' }}>
              <pre {...stylex.props(ui.codeBlock)}>
                <code>{anthropicCurl}</code>
              </pre>
            </TabsPanel>
            <TabsPanel value="embeddings" style={{ marginTop: '14px' }}>
              <pre {...stylex.props(ui.codeBlock)}>
                <code>{embeddingsCurl}</code>
              </pre>
            </TabsPanel>
          </Tabs>
        </div>
      </section>

      <div {...stylex.props(ui.grid2)}>
        <section {...stylex.props(ui.card)}>
          <div {...stylex.props(ui.cardHeader)}>
            <div>
              <h3 {...stylex.props(ui.cardTitle)}>受控在线测试（Playground）</h3>
              <p {...stylex.props(ui.cardSubtitle)}>
                签发 1 分钟临时测试密钥走完整准入、限流与微美元结算链路。
              </p>
            </div>
          </div>
          <div {...stylex.props(ui.cardBody, ui.formGrid)}>
            {!canWrite ? (
              <p {...stylex.props(ui.mutedText)}>只读角色不可发送在线测试请求。</p>
            ) : aliases.length ? (
              <>
                <div {...stylex.props(ui.formRow)}>
                  <div {...stylex.props(ui.field)}>
                    <label {...stylex.props(ui.fieldLabel)}>测试端点模式（6 大多模态协议）</label>
                    <select
                      value={testMode}
                      onChange={(e) => setTestMode(e.target.value as any)}
                      {...stylex.props(ui.input)}
                    >
                      <option value="chat">OpenAI Chat (/v1/chat/completions)</option>
                      <option value="anthropic">Anthropic Messages (/v1/messages)</option>
                      <option value="embeddings">Vector Embeddings (/v1/embeddings)</option>
                      <option value="rerank">Cross-Protocol Rerank (/v1/rerank)</option>
                      <option value="images">Image Generation (/v1/images/generations)</option>
                      <option value="systemone">TypeSafe Jev System One (/v1/systemone)</option>
                    </select>
                  </div>
                  <div {...stylex.props(ui.field)}>
                    <label {...stylex.props(ui.fieldLabel)}>已发布模型别名</label>
                    <select
                      value={testModel}
                      onChange={(e) => setTestModel(e.target.value)}
                      {...stylex.props(ui.input)}
                    >
                      {aliases.map((a) => (
                        <option key={a.id} value={a.name}>
                          {a.name}
                        </option>
                      ))}
                    </select>
                  </div>
                  <Field name="maxTokens" {...stylex.props(ui.field)}>
                    <FieldLabel {...stylex.props(ui.fieldLabel)}>最大输出 Token</FieldLabel>
                    <Input
                      type="number"
                      min="1"
                      max="4096"
                      value={testMaxTokens}
                      onValueChange={setTestMaxTokens}
                      {...stylex.props(ui.input)}
                    />
                  </Field>
                </div>
                <div {...stylex.props(ui.field)}>
                  <label {...stylex.props(ui.fieldLabel)}>
                    {testMode === 'rerank'
                      ? '检索查询词 (Query)'
                      : testMode === 'systemone'
                        ? '待评估状态文本 (State)'
                        : '测试提示词 (Prompt)'}
                  </label>
                  <textarea
                    value={testPrompt}
                    onChange={(e) => setTestPrompt(e.target.value)}
                    {...stylex.props(ui.textarea)}
                  />
                </div>
                {testMode === 'rerank' && (
                  <div {...stylex.props(ui.field)}>
                    <label {...stylex.props(ui.fieldLabel)}>
                      待重排候选文档列表（每行一篇文档）
                    </label>
                    <textarea
                      value={testDocsInput}
                      onChange={(e) => setTestDocsInput(e.target.value)}
                      {...stylex.props(ui.textarea)}
                    />
                  </div>
                )}
                <UiButton variant="primary" onClick={runTestCall}>
                  发送受控测试请求
                </UiButton>
                {testOut && (
                  <div {...stylex.props(ui.detailItem, ui.stackSm)}>
                    {testOut.ok ? (
                      <>
                        <div {...stylex.props(ui.inlineRow)}>
                          <UiBadge status="completed" label="调用完成" />
                          <span {...stylex.props(ui.mutedText)}>
                            耗时 {fmtDuration(testOut.elapsed)} · ID: {testOut.requestId}
                          </span>
                        </div>
                        <pre {...stylex.props(ui.codeBlock)} style={{ whiteSpace: 'pre-wrap' }}>
                          {testOut.content}
                        </pre>
                        <div {...stylex.props(ui.mutedText)}>
                          输入 Token: {fmtInt(testOut.usage?.prompt_tokens ?? testOut.usage?.input_tokens)} · 输出 Token:{' '}
                          {fmtInt(testOut.usage?.completion_tokens ?? testOut.usage?.output_tokens)}
                        </div>
                      </>
                    ) : (
                      <p {...stylex.props(ui.fieldError)}>{testOut.error}</p>
                    )}
                  </div>
                )}
              </>
            ) : (
              <EmptyBox
                title="尚无已发布模型别名"
                description="请先前往模型别名页发布至少一个路由版本。"
                action={
                  <UiButton size="sm" variant="secondary" onClick={() => onNavigate('aliases')}>
                    前往模型别名
                  </UiButton>
                }
              />
            )}
          </div>
        </section>

        <section {...stylex.props(ui.card)}>
          <div {...stylex.props(ui.cardHeader)}>
            <div>
              <h3 {...stylex.props(ui.cardTitle)}>UI SDK Agent & WebMCP 自省桥接</h3>
              <p {...stylex.props(ui.cardSubtitle)}>
                基于 @ui-sdk/react/agent 的 createAgent() 实时暴露页面可交互原语。
              </p>
            </div>
            <UiButton size="sm" variant="secondary" onClick={inspectUiSdkAgent}>
              生成组件快照
            </UiButton>
          </div>
          <div {...stylex.props(ui.cardBody, ui.stackSm)}>
            <p {...stylex.props(ui.mutedText)} style={{ margin: 0 }}>
              控制台所有基于 `@ui-sdk/react` 构建的表单、Tabs、SegmentedControl 与按钮均自动注册到语义树中，方便浏览器内置 WebMCP 或运维 Agent 直接读取状态与调用动作。
            </p>
            {agentSnap ? (
              <pre
                {...stylex.props(ui.codeBlock)}
                style={{ maxHeight: '220px', overflowY: 'auto' }}
              >
                <code>{JSON.stringify(agentSnap.slice(0, 12), null, 2)}</code>
              </pre>
            ) : (
              <div {...stylex.props(ui.callout)}>
                点击右上角“生成组件快照”查看 `consoleAgent.snapshot()` 导出的实时组件状态树。
              </div>
            )}
          </div>
        </section>
      </div>
    </>
  );
}

// ============================================================================
// 8. SETTINGS, MEMBERS, RETENTION & AUDIT VIEW
// ============================================================================
export function SettingsView({
  project,
  user,
  isAdmin,
  onProjectRenamed,
  onLogout,
  refreshTick,
}: {
  project: any;
  user: any;
  isAdmin: boolean;
  onProjectRenamed: (name: string) => void;
  onLogout: () => void;
  refreshTick: number;
}) {
  const [projects, setProjects] = useState<any[]>([]);
  const [members, setMembers] = useState<any[]>([]);
  const [audit, setAudit] = useState<any[]>([]);

  // Project settings form
  const [projName, setProjName] = useState('');
  const [projBudget, setProjBudget] = useState('');
  const [projRpm, setProjRpm] = useState('');
  const [projTpm, setProjTpm] = useState('');
  const [projConcurrency, setProjConcurrency] = useState('');
  const [projMaxOut, setProjMaxOut] = useState('');
  const [retentionDays, setRetentionDays] = useState('');

  // Invite modal
  const [inviteOpen, setInviteOpen] = useState(false);
  const [invEmail, setInvEmail] = useState('');
  const [invRole, setInvRole] = useState('developer');
  const [invProjects, setInvProjects] = useState<string[]>([]);
  const [createdInvite, setCreatedInvite] = useState<any | null>(null);

  // Member edit modal
  const [editingMember, setEditingMember] = useState<any | null>(null);
  const [memRole, setMemRole] = useState('developer');
  const [memDisabled, setMemDisabled] = useState(false);
  const [memProjects, setMemProjects] = useState<string[]>([]);

  async function reload() {
    const [pd, ad, md, sd] = await Promise.all([
      getSafe('/api/projects'),
      isAdmin ? getSafe('/api/audit') : Promise.resolve({ ok: false as const, error: null }),
      isAdmin ? getSafe('/api/members') : Promise.resolve({ ok: false as const, error: null }),
      isAdmin ? getSafe('/api/settings') : Promise.resolve({ ok: false as const, error: null }),
    ]);
    const pList = pd.ok ? items(pd.data) : [];
    setProjects(pList);
    const cur = pList.find((x: any) => x.id === project?.id) || project || {};
    setProjName(cur.name || '');
    setProjBudget(microsToUsd(cur.budget_micros ?? cur.budgetMicros));
    setProjRpm(cur.rpm != null ? String(cur.rpm) : '');
    setProjTpm(cur.tpm != null ? String(cur.tpm) : '');
    setProjConcurrency(cur.concurrency != null ? String(cur.concurrency) : '');
    setProjMaxOut(
      cur.max_output_tokens != null
        ? String(cur.max_output_tokens)
        : cur.maxOutputTokens != null
          ? String(cur.maxOutputTokens)
          : '',
    );
    if (md.ok) setMembers(items(md.data));
    if (ad.ok) setAudit(items(ad.data));
    if (sd.ok) {
      const d = sd.data?.request_retention_days;
      setRetentionDays(d != null ? String(d) : '');
    }
  }

  useEffect(() => {
    reload();
  }, [project?.id, isAdmin, refreshTick]);

  async function saveProjectSettings() {
    if (!projName.trim()) {
      notify('项目名称不能为空', 'error');
      return;
    }
    const bMicros = projBudget.trim() === '' ? null : usdToMicros(projBudget);
    const intOrNull = (s: string) => (s.trim() === '' ? null : Number(s));
    try {
      await api(`/api/projects/${encodeURIComponent(project.id)}`, {
        method: 'PATCH',
        body: {
          name: projName.trim(),
          budget_micros: bMicros,
          budgetMicros: bMicros,
          rpm: intOrNull(projRpm),
          tpm: intOrNull(projTpm),
          concurrency: intOrNull(projConcurrency),
          max_output_tokens: intOrNull(projMaxOut),
          maxOutputTokens: intOrNull(projMaxOut),
        },
      });
      onProjectRenamed(projName.trim());
      notify('项目限流与预算设置已保存', 'success');
      await reload();
    } catch (e: any) {
      notify(e?.message || '保存项目设置失败', 'error');
    }
  }

  async function saveRetention() {
    const days = retentionDays.trim() === '' ? null : Number(retentionDays);
    if (days !== null && (!Number.isInteger(days) || days < 1 || days > 3650)) {
      notify('保留天数需为 1–3650 之间的整数', 'error');
      return;
    }
    try {
      await api('/api/settings', {
        method: 'PATCH',
        body: { request_retention_days: days },
      });
      notify('请求元数据保留策略已更新', 'success');
      await reload();
    } catch (e: any) {
      notify(e?.message || '保存保留策略失败', 'error');
    }
  }

  async function createInvitation() {
    if (!invEmail.trim()) {
      notify('请输入受邀成员邮箱', 'error');
      return;
    }
    try {
      const r = await api('/api/invitations', {
        method: 'POST',
        body: {
          email: invEmail.trim(),
          role: invRole,
          project_ids: invProjects,
        },
      });
      setInviteOpen(false);
      setCreatedInvite({
        email: invEmail.trim(),
        token: r.token || '',
        link: `${window.location.origin}/#/accept?token=${encodeURIComponent(r.token || '')}`,
      });
      notify('成员邀请已创建', 'success');
      await reload();
    } catch (e: any) {
      notify(e?.message || '创建邀请失败', 'error');
    }
  }

  async function saveMember() {
    if (!editingMember) return;
    try {
      await api(`/api/members/${encodeURIComponent(editingMember.id)}`, {
        method: 'PATCH',
        body: {
          role: memRole,
          disabled: memDisabled,
          project_ids: memProjects,
        },
      });
      notify('成员权限已更新，其现有会话已被服务端吊销', 'success');
      setEditingMember(null);
      await reload();
    } catch (e: any) {
      notify(e?.message || '更新成员失败', 'error');
    }
  }

  return (
    <>
      <PageHeader
        title="设置、成员权限与审计"
        description="管理当前项目配额参数、工作空间成员 RBAC 授权、请求日志保留周期及管理员审计事件。"
      />

      <div {...stylex.props(ui.grid2)}>
        <section {...stylex.props(ui.card)}>
          <div {...stylex.props(ui.cardHeader)}>
            <div>
              <h3 {...stylex.props(ui.cardTitle)}>项目配额与限流参数</h3>
              <p {...stylex.props(ui.cardSubtitle)}>
                变更立即通过 Redis Pub/Sub 推送至所有数据面网关节点。
              </p>
            </div>
          </div>
          <div {...stylex.props(ui.cardBody, ui.formGrid)}>
            {isAdmin ? (
              <>
                <div {...stylex.props(ui.formRow)}>
                  <Field name="projName" {...stylex.props(ui.field)}>
                    <FieldLabel {...stylex.props(ui.fieldLabel)}>项目名称</FieldLabel>
                    <Input
                      value={projName}
                      onValueChange={setProjName}
                      {...stylex.props(ui.input)}
                    />
                  </Field>
                  <Field name="projBudget" {...stylex.props(ui.field)}>
                    <FieldLabel {...stylex.props(ui.fieldLabel)}>月度预算（USD）</FieldLabel>
                    <Input
                      type="number"
                      min="0"
                      step="0.000001"
                      value={projBudget}
                      onValueChange={setProjBudget}
                      placeholder="留空不设上限"
                      {...stylex.props(ui.input)}
                    />
                  </Field>
                </div>
                <div {...stylex.props(ui.formRow)}>
                  <Field name="projRpm" {...stylex.props(ui.field)}>
                    <FieldLabel {...stylex.props(ui.fieldLabel)}>项目 RPM 上限</FieldLabel>
                    <Input
                      type="number"
                      min="1"
                      value={projRpm}
                      onValueChange={setProjRpm}
                      placeholder="默认"
                      {...stylex.props(ui.input)}
                    />
                  </Field>
                  <Field name="projTpm" {...stylex.props(ui.field)}>
                    <FieldLabel {...stylex.props(ui.fieldLabel)}>项目 TPM 上限 (Tokens/min)</FieldLabel>
                    <Input
                      type="number"
                      min="1"
                      value={projTpm}
                      onValueChange={setProjTpm}
                      placeholder="默认"
                      {...stylex.props(ui.input)}
                    />
                  </Field>
                  <Field name="projConcurrency" {...stylex.props(ui.field)}>
                    <FieldLabel {...stylex.props(ui.fieldLabel)}>最大并发租约</FieldLabel>
                    <Input
                      type="number"
                      min="1"
                      value={projConcurrency}
                      onValueChange={setProjConcurrency}
                      placeholder="默认"
                      {...stylex.props(ui.input)}
                    />
                  </Field>
                  <Field name="projMaxOut" {...stylex.props(ui.field)}>
                    <FieldLabel {...stylex.props(ui.fieldLabel)}>默认最大输出 Token</FieldLabel>
                    <Input
                      type="number"
                      min="1"
                      value={projMaxOut}
                      onValueChange={setProjMaxOut}
                      placeholder="默认"
                      {...stylex.props(ui.input)}
                    />
                  </Field>
                </div>
                <UiButton variant="primary" onClick={saveProjectSettings}>
                  保存项目设置
                </UiButton>
              </>
            ) : (
              <div {...stylex.props(ui.callout)}>
                当前角色为 {roleName(user?.role)}，项目参数为只读。
              </div>
            )}
          </div>
        </section>

        <section {...stylex.props(ui.card)}>
          <div {...stylex.props(ui.cardHeader)}>
            <h3 {...stylex.props(ui.cardTitle)}>当前登录身份与数据保留</h3>
          </div>
          <div {...stylex.props(ui.cardBody, ui.formGrid)}>
            <dl {...stylex.props(ui.detailGrid)}>
              <div {...stylex.props(ui.detailItem)}>
                <dt {...stylex.props(ui.detailDt)}>当前账号</dt>
                <dd {...stylex.props(ui.detailDd)}>{user?.email || '—'}</dd>
              </div>
              <div {...stylex.props(ui.detailItem)}>
                <dt {...stylex.props(ui.detailDt)}>角色</dt>
                <dd {...stylex.props(ui.detailDd)}>{roleName(user?.role)}</dd>
              </div>
              <div {...stylex.props(ui.detailItem)}>
                <dt {...stylex.props(ui.detailDt)}>当前项目 ID</dt>
                <dd {...stylex.props(ui.detailDd, ui.tdMono)}>{project?.id || '—'}</dd>
              </div>
            </dl>
            {isAdmin && (
              <div {...stylex.props(ui.formRow)}>
                <Field name="retentionDays" {...stylex.props(ui.field)}>
                  <FieldLabel {...stylex.props(ui.fieldLabel)}>
                    请求元数据保留天数（1–3650）
                  </FieldLabel>
                  <Input
                    type="number"
                    min="1"
                    max="3650"
                    value={retentionDays}
                    onValueChange={setRetentionDays}
                    placeholder="默认保留期"
                    {...stylex.props(ui.input)}
                  />
                </Field>
                <UiButton variant="secondary" onClick={saveRetention}>
                  保存保留期
                </UiButton>
              </div>
            )}
            <div>
              <UiButton variant="outline" onClick={onLogout}>
                退出当前会话
              </UiButton>
            </div>
          </div>
        </section>
      </div>

      {isAdmin && (
        <>
          <section {...stylex.props(ui.card)}>
            <div {...stylex.props(ui.cardHeader)}>
              <div>
                <h3 {...stylex.props(ui.cardTitle)}>工作空间成员与权限</h3>
                <p {...stylex.props(ui.cardSubtitle)}>
                  调整成员角色或禁用账号会立即吊销该成员的活动会话。
                </p>
              </div>
              <UiButton
                size="sm"
                variant="primary"
                onClick={() => {
                  setInvEmail('');
                  setInvRole('developer');
                  setInvProjects(project?.id ? [project.id] : []);
                  setInviteOpen(true);
                }}
              >
                邀请成员
              </UiButton>
            </div>
            <div {...stylex.props(ui.cardBodyFlush)}>
              {members.length ? (
                <div {...stylex.props(ui.tableWrap)}>
                  <table {...stylex.props(ui.table)}>
                    <thead>
                      <tr>
                        <th {...stylex.props(ui.th)}>成员邮箱</th>
                        <th {...stylex.props(ui.th)}>角色</th>
                        <th {...stylex.props(ui.th)}>状态</th>
                        <th {...stylex.props(ui.th)}>授权项目</th>
                        <th {...stylex.props(ui.th)} />
                      </tr>
                    </thead>
                    <tbody>
                      {members.map((m) => {
                        const pNames = arr(m.project_ids).map(
                          (id: string) => projects.find((x) => x.id === id)?.name || id,
                        );
                        return (
                          <tr key={m.id}>
                            <td {...stylex.props(ui.td)}>
                              <span {...stylex.props(ui.cellPrimary)}>{m.email}</span>
                              {m.id === user?.id && (
                                <span {...stylex.props(ui.cellSub)}>当前登录账号</span>
                              )}
                            </td>
                            <td {...stylex.props(ui.td)}>
                              <UiBadge
                                tone={m.role === 'admin' ? 'info' : 'neutral'}
                                label={roleName(m.role)}
                              />
                            </td>
                            <td {...stylex.props(ui.td)}>
                              <UiBadge status={m.disabled_at ? 'disabled' : 'enabled'} />
                            </td>
                            <td {...stylex.props(ui.td)}>
                              {m.role === 'admin' ? (
                                '全部项目'
                              ) : pNames.length ? (
                                <div {...stylex.props(ui.inlineRow)}>
                                  {pNames.map((n: string) => (
                                    <UiBadge key={n} tone="neutral" label={n} />
                                  ))}
                                </div>
                              ) : (
                                <span {...stylex.props(ui.mutedText)}>无项目</span>
                              )}
                            </td>
                            <td {...stylex.props(ui.td, ui.tdActions)}>
                              <UiButton
                                size="sm"
                                variant="secondary"
                                onClick={() => {
                                  setEditingMember(m);
                                  setMemRole(m.role || 'developer');
                                  setMemDisabled(!!m.disabled_at);
                                  setMemProjects(arr(m.project_ids));
                                }}
                              >
                                管理权限
                              </UiButton>
                            </td>
                          </tr>
                        );
                      })}
                    </tbody>
                  </table>
                </div>
              ) : (
                <EmptyBox title="暂无其他成员" description="点击右上角邀请开发者或只读成员。" />
              )}
            </div>
          </section>

          <section {...stylex.props(ui.card)}>
            <div {...stylex.props(ui.cardHeader)}>
              <div>
                <h3 {...stylex.props(ui.cardTitle)}>控制面审计日志（最近 100 条）</h3>
                <p {...stylex.props(ui.cardSubtitle)}>
                  所有供应商、路由版本发布/回滚、密钥签发/吊销与账单导出均自动记入审计表。
                </p>
              </div>
              <UiBadge tone="neutral" label={`${audit.length} 条记录`} />
            </div>
            <div {...stylex.props(ui.cardBodyFlush)}>
              {audit.length ? (
                <div {...stylex.props(ui.tableWrap)}>
                  <table {...stylex.props(ui.table)}>
                    <thead>
                      <tr>
                        <th {...stylex.props(ui.th)}>时间</th>
                        <th {...stylex.props(ui.th)}>动作</th>
                        <th {...stylex.props(ui.th)}>实体类型</th>
                        <th {...stylex.props(ui.th)}>实体 ID</th>
                        <th {...stylex.props(ui.th)}>变更摘要</th>
                      </tr>
                    </thead>
                    <tbody>
                      {audit.map((a) => (
                        <tr key={a.id}>
                          <td {...stylex.props(ui.td)}>{fmtDate(a.created_at)}</td>
                          <td {...stylex.props(ui.td, ui.tdMono)}>{a.action}</td>
                          <td {...stylex.props(ui.td)}>{a.entity_type || '—'}</td>
                          <td {...stylex.props(ui.td, ui.tdMono)}>{a.entity_id || '—'}</td>
                          <td {...stylex.props(ui.td, ui.tdMono)}>
                            {JSON.stringify(a.changes || {}).slice(0, 160)}
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              ) : (
                <EmptyBox title="暂无审计记录" description="控制面配置变更后将在此处显示。" />
              )}
            </div>
          </section>
        </>
      )}

      {/* Invite Member Dialog */}
      <UiDialog
        open={inviteOpen}
        onOpenChange={setInviteOpen}
        title="邀请工作空间成员"
        description="生成 7 天有效的一次性邀请令牌。"
        wide
        footer={
          <>
            <UiButton variant="outline" onClick={() => setInviteOpen(false)}>
              取消
            </UiButton>
            <UiButton variant="primary" onClick={createInvitation}>
              创建邀请
            </UiButton>
          </>
        }
      >
        <div {...stylex.props(ui.formGrid)}>
          <Field name="invEmail" {...stylex.props(ui.field)}>
            <FieldLabel {...stylex.props(ui.fieldLabel)}>受邀成员邮箱</FieldLabel>
            <Input
              type="email"
              value={invEmail}
              onValueChange={setInvEmail}
              placeholder="developer@example.com"
              {...stylex.props(ui.input)}
            />
          </Field>
          <div {...stylex.props(ui.field)}>
            <label {...stylex.props(ui.fieldLabel)}>工作空间角色</label>
            <select
              value={invRole}
              onChange={(e) => setInvRole(e.target.value)}
              {...stylex.props(ui.input)}
            >
              <option value="developer">开发者（Developer）</option>
              <option value="viewer">只读（Viewer）</option>
              <option value="admin">管理员（Admin）</option>
            </select>
          </div>
          <div {...stylex.props(ui.field)}>
            <span {...stylex.props(ui.fieldLabel)}>初始项目访问权限</span>
            <div {...stylex.props(ui.grid3)}>
              {projects.map((p) => (
                <UiCheckboxRow
                  key={p.id}
                  checked={invProjects.includes(p.id)}
                  onChange={(on) =>
                    setInvProjects((prev) =>
                      on ? [...new Set([...prev, p.id])] : prev.filter((x) => x !== p.id),
                    )
                  }
                  title={p.name}
                />
              ))}
            </div>
          </div>
        </div>
      </UiDialog>

      {/* Created Invitation Token Display */}
      <UiDialog
        open={!!createdInvite}
        onOpenChange={(o) => !o && setCreatedInvite(null)}
        title="成员邀请已生成"
        description="邀请令牌仅显示一次，请通过安全渠道发送给受邀成员。"
        wide
        footer={
          <UiButton variant="primary" onClick={() => setCreatedInvite(null)}>
            已复制保存，关闭
          </UiButton>
        }
      >
        {createdInvite && (
          <div {...stylex.props(ui.formGrid)}>
            <div {...stylex.props(ui.field)}>
              <span {...stylex.props(ui.fieldLabel)}>一次性邀请令牌</span>
              <div {...stylex.props(ui.copyRow)}>
                <code style={{ flex: 1, fontFamily: 'var(--font-mono)', fontSize: '12px' }}>
                  {createdInvite.token}
                </code>
                <UiButton
                  size="sm"
                  variant="secondary"
                  onClick={() => copyToClipboard(createdInvite.token)}
                >
                  复制令牌
                </UiButton>
              </div>
            </div>
            <div {...stylex.props(ui.field)}>
              <span {...stylex.props(ui.fieldLabel)}>邀请直达链接</span>
              <div {...stylex.props(ui.copyRow)}>
                <code style={{ flex: 1, fontFamily: 'var(--font-mono)', fontSize: '12px' }}>
                  {createdInvite.link}
                </code>
                <UiButton
                  size="sm"
                  variant="secondary"
                  onClick={() => copyToClipboard(createdInvite.link)}
                >
                  复制链接
                </UiButton>
              </div>
            </div>
          </div>
        )}
      </UiDialog>

      {/* Manage Member Dialog */}
      <UiDialog
        open={!!editingMember}
        onOpenChange={(o) => !o && setEditingMember(null)}
        title={`管理成员：${editingMember?.email || ''}`}
        wide
        footer={
          <>
            <UiButton variant="outline" onClick={() => setEditingMember(null)}>
              取消
            </UiButton>
            <UiButton variant="primary" onClick={saveMember}>
              保存变更
            </UiButton>
          </>
        }
      >
        {editingMember && (
          <div {...stylex.props(ui.formGrid)}>
            <div {...stylex.props(ui.formRow)}>
              <div {...stylex.props(ui.field)}>
                <label {...stylex.props(ui.fieldLabel)}>工作空间角色</label>
                <select
                  value={memRole}
                  onChange={(e) => setMemRole(e.target.value)}
                  {...stylex.props(ui.input)}
                >
                  <option value="admin">管理员</option>
                  <option value="developer">开发者</option>
                  <option value="viewer">只读</option>
                </select>
              </div>
              <div {...stylex.props(ui.field)}>
                <label {...stylex.props(ui.fieldLabel)}>账号状态</label>
                <select
                  value={memDisabled ? 'true' : 'false'}
                  onChange={(e) => setMemDisabled(e.target.value === 'true')}
                  {...stylex.props(ui.input)}
                >
                  <option value="false">启用</option>
                  <option value="true">禁用</option>
                </select>
              </div>
            </div>
            <div {...stylex.props(ui.field)}>
              <span {...stylex.props(ui.fieldLabel)}>可访问项目</span>
              <div {...stylex.props(ui.grid3)}>
                {projects.map((p) => (
                  <UiCheckboxRow
                    key={p.id}
                    checked={memProjects.includes(p.id)}
                    onChange={(on) =>
                      setMemProjects((prev) =>
                        on ? [...new Set([...prev, p.id])] : prev.filter((x) => x !== p.id),
                      )
                    }
                    title={p.name}
                  />
                ))}
              </div>
            </div>
          </div>
        )}
      </UiDialog>
    </>
  );
}
