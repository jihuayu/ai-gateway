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
  getSafe,
  items,
} from '../api/client';
import {
  EmptyBox,
  MetricCard,
  PageHeader,
  UiBadge,
  UiButton,
  UiCheckboxRow,
  UiConfirmDialog,
  UiDialog,
  UiSwitch,
  copyToClipboard,
  notify,
} from '../components/Primitives';

export interface BuiltinPluginTemplate {
  id: string;
  title: string;
  category: 'observability' | 'routing' | 'security' | 'cache' | 'finops';
  categoryLabel: string;
  className: string;
  defaultPriority: number;
  stages: string[];
  failMode: 'FAIL_OPEN' | 'FAIL_CLOSED';
  summary: string;
  defaultConfig: Record<string, any>;
}

export const BUILTIN_PLUGIN_TEMPLATES: BuiltinPluginTemplate[] = [
  {
    id: 'builtin:obs-prometheus-metrics',
    title: 'Prometheus 多维归因指标插件',
    category: 'observability',
    categoryLabel: '可观测性',
    className:
      'com.jihuayu.aigateway.plugin.MicrokernelObservabilityPlugins$PrometheusMetricsPlugin',
    defaultPriority: 40,
    stages: ['POST_SETTLE'],
    failMode: 'FAIL_OPEN',
    summary:
      '在 POST_SETTLE 阶段将多维 FinOps 归因标签（部门、成本中心、工作流）投影为受基数上限保护的 Prometheus 计数器与延迟/TTFT 直方图。',
    defaultConfig: {
      includeProjectLabel: true,
      includeAttributionLabels: ['department', 'cost_center'],
    },
  },
  {
    id: 'builtin:obs-otel-genai',
    title: 'OpenTelemetry GenAI 追踪与 OTLP 导出',
    category: 'observability',
    categoryLabel: '可观测性',
    className:
      'com.jihuayu.aigateway.plugin.MicrokernelObservabilityPlugins$OpenTelemetryGenAiTracingPlugin',
    defaultPriority: 42,
    stages: ['REQUEST_RECEIVED', 'POST_SETTLE'],
    failMode: 'FAIL_OPEN',
    summary:
      '透传 W3C traceparent 上下文，合并 Java/JS 插件 ctx.telemetry 自定义 Span 属性与事件，并异步非阻塞推送至 OTLP HTTP Collector。',
    defaultConfig: {
      serviceName: 'ai-gateway',
      otlpEndpoint: 'http://127.0.0.1:4318/v1/traces',
      otlpHeaders: {},
      capturePrompt: false,
      captureCompletion: false,
      maxAttrLength: 1024,
    },
  },
  {
    id: 'builtin:obs-langfuse-sink',
    title: 'Langfuse LLMOps 异步评估与成本下沉',
    category: 'observability',
    categoryLabel: '可观测性',
    className:
      'com.jihuayu.aigateway.plugin.MicrokernelObservabilityPlugins$LangfuseLlmOpsSinkPlugin',
    defaultPriority: 45,
    stages: ['POST_SETTLE'],
    failMode: 'FAIL_OPEN',
    summary:
      '结算完成后异步组装 trace-create 与 generation-create 批次上报至 Langfuse，实现微美元成本、Token 用量与 Prompt 质量联合分析。',
    defaultConfig: {
      endpointUrl: 'https://cloud.langfuse.com/api/public/ingestion',
      publicKey: 'pk-lf-replace-me',
      secretKey: 'sk-lf-replace-me',
      environment: 'production',
      captureInputOutput: false,
      timeoutMs: 3000,
    },
  },
  {
    id: 'builtin:obs-access-log',
    title: '慢请求与异常结构化访问日志插件',
    category: 'observability',
    categoryLabel: '可观测性',
    className:
      'com.jihuayu.aigateway.plugin.MicrokernelObservabilityPlugins$StructuredAccessLogPlugin',
    defaultPriority: 48,
    stages: ['POST_SETTLE'],
    failMode: 'FAIL_OPEN',
    summary:
      '按慢请求延迟阈值（slowThresholdMs）或仅错误模式（errorsOnly）过滤高价值诊断日志，写入内存环形缓冲区（供控制台查阅）并可选推送至 Loki/Vector。',
    defaultConfig: {
      slowThresholdMs: 500,
      errorsOnly: false,
      includeHeaders: ['user-agent', 'x-client-version'],
      httpSinkUrl: '',
    },
  },
  {
    id: 'builtin:router-peak-ewma-hedge',
    title: 'Peak-EWMA 负载感知路由与尾延迟对冲',
    category: 'routing',
    categoryLabel: '路由与对冲',
    className:
      'com.jihuayu.aigateway.plugin.MicrokernelRoutingAndPolicyPlugins$LeastTtftRouterPlugin',
    defaultPriority: 20,
    stages: ['ROUTE_SELECT'],
    failMode: 'FAIL_OPEN',
    summary:
      '基于 Peak-EWMA × (1 + activeInflight) 实时探测上游拥塞并动态排序；主节点超时未回首包时并发发起备节点对冲请求，赢家产生即刻中断慢流。',
    defaultConfig: {
      hedgeEnabled: true,
      hedgeMultiplier: 2.0,
      minHedgeDelayMs: 25,
      maxHedgeDelayMs: 2500,
      maxHedgeRatio: 0.2,
    },
  },
  {
    id: 'builtin:router-weighted-cb',
    title: '加权负载均衡与滑动窗口熔断路由',
    category: 'routing',
    categoryLabel: '路由与对冲',
    className:
      'com.jihuayu.aigateway.plugin.MicrokernelRoutingAndPolicyPlugins$WeightedCircuitBreakerRouterPlugin',
    defaultPriority: 15,
    stages: ['ROUTE_SELECT'],
    failMode: 'FAIL_OPEN',
    summary:
      '按路由版本中配置的模型目标权重分配流量，并结合滑动窗口熔断器自动剔除异常上游节点。',
    defaultConfig: {},
  },
  {
    id: 'builtin:router-prefix-cache-affinity',
    title: 'Prompt 前缀 KV-Cache 亲和路由',
    category: 'routing',
    categoryLabel: '路由与对冲',
    className:
      'com.jihuayu.aigateway.plugin.MicrokernelRoutingAndPolicyPlugins$PrefixCacheAffinityRouterPlugin',
    defaultPriority: 18,
    stages: ['ROUTE_SELECT'],
    failMode: 'FAIL_OPEN',
    summary:
      '对系统提示词与长上下文前缀计算一致性哈希，将相同会话前缀稳定路由至同一上游实例以最大化 Prompt Cache 命中率。',
    defaultConfig: {
      prefixChars: 512,
    },
  },
  {
    id: 'builtin:router-jev-semantic',
    title: 'TypeSafe Jev 语义复杂度分层路由',
    category: 'routing',
    categoryLabel: '路由与对冲',
    className:
      'com.jihuayu.aigateway.plugin.MicrokernelRoutingAndPolicyPlugins$JevSemanticRouterPlugin',
    defaultPriority: 22,
    stages: ['ROUTE_SELECT'],
    failMode: 'FAIL_OPEN',
    summary:
      '基于超低延迟 System One 语义分类器评估请求复杂度，简单问答分流至高性价比小模型，复杂推理路由至旗舰模型。',
    defaultConfig: {
      complexityThreshold: 0.65,
    },
  },
  {
    id: 'builtin:exact-cache',
    title: 'Caffeine L1 + Redis L2 精确响应与流式重放缓存',
    category: 'cache',
    categoryLabel: '缓存与护栏',
    className: 'com.jihuayu.aigateway.plugin.ExactCachePlugin',
    defaultPriority: 10,
    stages: ['POST_ADMIT_PRE_DISPATCH', 'RESPONSE_CONTENT'],
    failMode: 'FAIL_OPEN',
    summary:
      '基于 W-TinyLFU 进程内近端缓存与 SingleFlight 防击穿合并，0-RTT 短路完全一致的非流式及 SSE 流式请求并退还预留额度。',
    defaultConfig: {
      ttlSeconds: 600,
      cacheStreaming: true,
    },
  },
  {
    id: 'builtin:pii-guardrail',
    title: 'PII 敏感隐私脱敏与流式安全护栏',
    category: 'cache',
    categoryLabel: '缓存与护栏',
    className: 'com.jihuayu.aigateway.plugin.PiiGuardrailPlugin',
    defaultPriority: 12,
    stages: ['PRE_ADMIT', 'STREAM_CHUNK', 'RESPONSE_CONTENT'],
    failMode: 'FAIL_CLOSED',
    summary:
      '双向拦截或脱敏邮箱、手机号、身份证与密钥等敏感数据，支持 SSE 流式分块实时掩码替换。',
    defaultConfig: {
      mode: 'MASK',
      redactEmail: true,
      redactPhone: true,
    },
  },
  {
    id: 'builtin:auth-hmac-signature',
    title: '企业 AK/SK HMAC-SHA256 签名防重放鉴权',
    category: 'security',
    categoryLabel: '鉴权与限流',
    className:
      'com.jihuayu.aigateway.plugin.MicrokernelAuthAndRateLimitPlugins$HmacSignatureAuthPlugin',
    defaultPriority: 20,
    stages: ['AUTHENTICATE'],
    failMode: 'FAIL_CLOSED',
    summary:
      '校验 X-AK / X-Timestamp / X-Nonce / X-Signature 签名与时间窗偏移，并通过分布式 Nonce 原子锁防御重放攻击。',
    defaultConfig: {
      accessKeyHeader: 'x-ak',
      signatureHeader: 'x-signature',
      timestampHeader: 'x-timestamp',
      nonceHeader: 'x-nonce',
      maxClockSkewSeconds: 300,
      credentials: {},
    },
  },
  {
    id: 'builtin:ratelimit-end-user',
    title: '终端用户（endUserId）细粒度配额限流',
    category: 'security',
    categoryLabel: '鉴权与限流',
    className:
      'com.jihuayu.aigateway.plugin.MicrokernelAuthAndRateLimitPlugins$EndUserRateLimitPlugin',
    defaultPriority: 25,
    stages: ['RATE_LIMIT'],
    failMode: 'FAIL_CLOSED',
    summary:
      '在项目与 API Key 限流之上，按请求中的 user 字段或 X-End-User-Id 请求头对单个终端用户施加独立的每分钟调用上限。',
    defaultConfig: {
      rpmPerUser: 60,
      userHeader: 'x-end-user-id',
    },
  },
  {
    id: 'builtin:ratelimit-spend-velocity',
    title: '微美元消费速率异常熔断器',
    category: 'security',
    categoryLabel: '鉴权与限流',
    className:
      'com.jihuayu.aigateway.plugin.MicrokernelAuthAndRateLimitPlugins$SpendVelocityCircuitPlugin',
    defaultPriority: 28,
    stages: ['RATE_LIMIT', 'POST_SETTLE'],
    failMode: 'FAIL_CLOSED',
    summary:
      '实时监控滚动窗口内的微美元消耗斜率，当死循环脚本或异常流量导致短时账单激增时自动触发限流保护。',
    defaultConfig: {
      windowSeconds: 60,
      maxMicrosPerWindow: 5000000,
    },
  },
  {
    id: 'builtin:billing-cost-attribution',
    title: 'FinOps 多维成本中心归因提取',
    category: 'finops',
    categoryLabel: 'FinOps 计费',
    className:
      'com.jihuayu.aigateway.plugin.MicrokernelBillingSinkPlugins$CostAttributionTagPlugin',
    defaultPriority: 11,
    stages: ['PRE_ADMIT'],
    failMode: 'FAIL_OPEN',
    summary:
      '自动提取 X-Cost-Center、X-Department、X-Agent-Id 与 X-Workflow-Id 写入请求上下文，贯通下游账单导出与监控指标。',
    defaultConfig: {},
  },
  {
    id: 'builtin:billing-webhook-sink',
    title: '微美元结算账单 HMAC Webhook 下沉',
    category: 'finops',
    categoryLabel: 'FinOps 计费',
    className:
      'com.jihuayu.aigateway.plugin.MicrokernelBillingSinkPlugins$WebhookBillingSinkPlugin',
    defaultPriority: 50,
    stages: ['POST_SETTLE'],
    failMode: 'FAIL_OPEN',
    summary:
      '将每笔真实结算事件附带 HMAC-SHA256 签名实时推送至企业 ERP / Lago / Stripe 计费接收端，失败自动进入死信队列。',
    defaultConfig: {
      webhookUrl: 'https://billing.example.com/webhooks/ai-gateway',
      signingSecret: 'replace-with-hmac-secret',
      timeoutMs: 1500,
    },
  },
];

const ALL_STAGES = [
  'AUTHENTICATE',
  'REQUEST_RECEIVED',
  'RATE_LIMIT',
  'PRE_ADMIT',
  'POST_ADMIT_PRE_DISPATCH',
  'ROUTE_SELECT',
  'PRE_UPSTREAM',
  'STREAM_CHUNK',
  'RESPONSE_CONTENT',
  'POST_SETTLE',
];

const DEFAULT_JS_SAMPLE = `function onPreAdmit(ctx) {
  ctx.telemetry.incr('custom_guard_checks', 1, { env: 'prod' });
  ctx.telemetry.setSpanAttr('guard.checked', true);
  return { action: 'CONTINUE' };
}`;

// ============================================================================
// 1. KERNEL HEALTH & OBSERVABILITY TELEMETRY VIEW (#/telemetry)
// ============================================================================
export function TelemetryView({
  onNavigate,
  refreshTick,
}: {
  onNavigate: (view: string) => void;
  refreshTick: number;
}) {
  const [runtime, setRuntime] = useState<any | null>(null);
  const [accessLogs, setAccessLogs] = useState<any[]>([]);
  const [spans, setSpans] = useState<any[]>([]);
  const [metricsRaw, setMetricsRaw] = useState('');
  const [metricsFilter, setMetricsFilter] = useState('');
  const [obsTab, setObsTab] = useState<'logs' | 'spans' | 'metrics'>('logs');

  // Slow / Error log client filters
  const [minLatencyFilter, setMinLatencyFilter] = useState('0');
  const [onlyErrorsFilter, setOnlyErrorsFilter] = useState(false);
  const [selectedLog, setSelectedLog] = useState<any | null>(null);
  const [selectedSpan, setSelectedSpan] = useState<any | null>(null);

  async function reload() {
    const [rtRes, logsRes, spansRes] = await Promise.all([
      getSafe('/api/telemetry/runtime'),
      getSafe('/api/telemetry/access-logs'),
      getSafe('/api/telemetry/spans'),
    ]);
    if (rtRes.ok) setRuntime(rtRes.data);
    if (logsRes.ok) setAccessLogs(items(logsRes.data));
    if (spansRes.ok) setSpans(items(spansRes.data));
  }

  async function loadPrometheusText() {
    try {
      const resp = await fetch(`${API_BASE}/metrics`, { credentials: 'same-origin' });
      const txt = await resp.text();
      setMetricsRaw(txt);
    } catch (e: any) {
      notify(e?.message || '拉取 /metrics 指标失败', 'error');
    }
  }

  useEffect(() => {
    reload();
  }, [refreshTick]);

  useEffect(() => {
    if (obsTab === 'metrics' && !metricsRaw) {
      loadPrometheusText();
    }
  }, [obsTab]);

  const haCode = Number(runtime?.haStateCode ?? 0);
  const haLabel =
    haCode === 0
      ? 'NORMAL · 分布式正常模式'
      : haCode === 1
        ? 'DEGRADED_LOCAL_WAL · 本地 WAL 降级容灾中'
        : 'FENCED_SAFE_MODE · 脑裂隔离安全模式';
  const haTone = haCode === 0 ? 'success' : haCode === 1 ? 'warning' : 'danger';

  const authHits = Number(runtime?.caffeineAuthHits ?? 0);
  const authMisses = Number(runtime?.caffeineAuthMisses ?? 0);
  const authTotal = authHits + authMisses;
  const authHitRate = authTotal > 0 ? authHits / authTotal : null;

  const respHits = Number(runtime?.caffeineResponseHits ?? 0);
  const respMisses = Number(runtime?.caffeineResponseMisses ?? 0);
  const respTotal = respHits + respMisses;
  const respHitRate = respTotal > 0 ? respHits / respTotal : null;

  const mpscSubmitted = Number(runtime?.mpscCommandsSubmitted ?? 0);
  const mpscBatches = Number(runtime?.mpscBatchesFlushed ?? 0);
  const avgBatch = mpscBatches > 0 ? (mpscSubmitted / mpscBatches).toFixed(1) : '1.0';

  const hedgeTriggers = Number(runtime?.hedgeTriggers ?? 0);
  const hedgeWins = Number(runtime?.hedgeSecondaryWins ?? 0);
  const hedgeWinRate = hedgeTriggers > 0 ? hedgeWins / hedgeTriggers : null;

  const minLatMs = Math.max(0, Number(minLatencyFilter) || 0);
  const filteredLogs = accessLogs.filter((entry) => {
    const lat = Number(entry?.latencyMs ?? 0);
    const st = String(entry?.status ?? 'completed').toLowerCase();
    if (minLatMs > 0 && lat < minLatMs) return false;
    if (onlyErrorsFilter && st === 'completed') return false;
    return true;
  });

  const filteredMetricsLines = metricsRaw
    ? metricsRaw
        .split('\n')
        .filter((line) =>
          metricsFilter.trim()
            ? line.toLowerCase().includes(metricsFilter.trim().toLowerCase())
            : true,
        )
        .join('\n')
    : '';

  return (
    <>
      <PageHeader
        title="内核健康与全链路可观测"
        description="实时洞察数据面 Caffeine W-TinyLFU 近端缓存、MPSC 组提交管道、分布式 HA 状态机、Peak-EWMA 对冲引擎，以及慢请求结构化日志与 OpenTelemetry GenAI 追踪。"
        actions={
          <>
            <UiButton variant="outline" onClick={() => onNavigate('plugins')}>
              配置可观测插件
            </UiButton>
            <UiButton
              variant="primary"
              onClick={() => {
                reload();
                if (obsTab === 'metrics') loadPrometheusText();
                notify('内核遥测快照已刷新', 'info');
              }}
            >
              立即刷新探针
            </UiButton>
          </>
        }
      />

      <div
        {...stylex.props(
          ui.callout,
          haCode === 1 && ui.calloutWarning,
          haCode === 2 && ui.calloutDanger,
        )}
      >
        <div {...stylex.props(ui.inlineRow)} style={{ justifyContent: 'space-between' }}>
          <div {...stylex.props(ui.inlineRow)}>
            <strong>分布式高可用引擎状态：</strong>
            <UiBadge tone={haTone} label={haLabel} />
          </div>
          <span {...stylex.props(ui.mutedText)}>
            待回放本地 WAL 记录：{fmtInt(runtime?.haWalPendingRecords ?? 0)} 条 · 累计已对账回放：
            {fmtInt(runtime?.haWalReplayedRecords ?? 0)} 条
          </span>
        </div>
      </div>

      <div {...stylex.props(ui.metricGrid)}>
        <MetricCard
          label="L1 AuthKey 缓存命中率"
          value={authHitRate === null ? '—' : fmtPct(authHitRate)}
          note={`命中 ${fmtInt(authHits)} · 未命中 ${fmtInt(authMisses)} · 淘汰 ${fmtInt(runtime?.caffeineAuthEvictions ?? 0)}`}
        />
        <MetricCard
          label="L1 响应缓存与防击穿"
          value={respHitRate === null ? '—' : fmtPct(respHitRate)}
          note={`SingleFlight 合并 ${fmtInt(runtime?.singleFlightCoalesced ?? 0)} · SWR 兜底 ${fmtInt(runtime?.caffeineStaleFallbacks ?? 0)}`}
        />
        <MetricCard
          label="MPSC 批量组提交管道"
          value={fmtInt(mpscSubmitted)}
          unit="条指令"
          note={`刷盘 ${fmtInt(mpscBatches)} 批 · 均批 ${avgBatch} · 峰值批大小 ${fmtInt(runtime?.mpscMaxBatchSize ?? 0)}`}
        />
        <MetricCard
          label="Peak-EWMA 尾延迟对冲"
          value={fmtInt(hedgeTriggers)}
          unit="次触发"
          note={`备节点抢答胜出 ${fmtInt(hedgeWins)} 次${hedgeWinRate !== null ? `（胜率 ${fmtPct(hedgeWinRate)}）` : ''}`}
        />
        <MetricCard
          label="连接池分发与熔断器"
          value={fmtInt(runtime?.pooledUpstreamDispatched ?? 0)}
          unit="次分发"
          note={`当前开启熔断器：${fmtInt(runtime?.openCircuitBreakers ?? 0)} 个`}
        />
        <MetricCard
          label="环形缓冲区遥测事件"
          value={fmtInt(accessLogs.length + spans.length)}
          unit="条"
          note={`慢/访问日志 ${fmtInt(accessLogs.length)} 条 · OTel Span ${fmtInt(spans.length)} 条`}
        />
      </div>

      <section {...stylex.props(ui.card)}>
        <div {...stylex.props(ui.cardHeader)}>
          <div>
            <h3 {...stylex.props(ui.cardTitle)}>可插拔遥测诊断中心</h3>
            <p {...stylex.props(ui.cardSubtitle)}>
              由微内核 Layer 2 可观测插件套件驱动采集，不启用对应插件时热路径保持零额外开销。
            </p>
          </div>
        </div>
        <div {...stylex.props(ui.cardBody)}>
          <Tabs
            value={obsTab}
            onValueChange={(v) => {
              if (v === 'logs' || v === 'spans' || v === 'metrics') setObsTab(v);
            }}
          >
            <TabsList aria-label="遥测数据视图切换" {...stylex.props(ui.tabsList)}>
              <TabsTab
                value="logs"
                className={(s) =>
                  stylex.props(ui.tabsTab, s.selected && ui.tabsTabSelected).className
                }
              >
                慢请求与访问日志 ({accessLogs.length})
              </TabsTab>
              <TabsTab
                value="spans"
                className={(s) =>
                  stylex.props(ui.tabsTab, s.selected && ui.tabsTabSelected).className
                }
              >
                OpenTelemetry GenAI Spans ({spans.length})
              </TabsTab>
              <TabsTab
                value="metrics"
                className={(s) =>
                  stylex.props(ui.tabsTab, s.selected && ui.tabsTabSelected).className
                }
              >
                Prometheus /metrics 指标流
              </TabsTab>
            </TabsList>

            <TabsPanel value="logs" style={{ marginTop: '16px' }}>
              <div {...stylex.props(ui.formRow)} style={{ marginBottom: '14px' }}>
                <Field name="minLatencyMs" {...stylex.props(ui.field)}>
                  <FieldLabel {...stylex.props(ui.fieldLabel)}>
                    慢请求耗时过滤阈值（ms）
                  </FieldLabel>
                  <Input
                    type="number"
                    min="0"
                    step="50"
                    value={minLatencyFilter}
                    onValueChange={setMinLatencyFilter}
                    placeholder="例如：500"
                    {...stylex.props(ui.input)}
                  />
                </Field>
                <div {...stylex.props(ui.inlineRow)} style={{ paddingBottom: '6px' }}>
                  <UiSwitch
                    checked={onlyErrorsFilter}
                    onChange={setOnlyErrorsFilter}
                    label="仅显示非成功/异常状态请求"
                  />
                </div>
              </div>

              {filteredLogs.length ? (
                <div {...stylex.props(ui.tableWrap)}>
                  <table {...stylex.props(ui.table)}>
                    <thead>
                      <tr>
                        <th {...stylex.props(ui.th)}>时间</th>
                        <th {...stylex.props(ui.th)}>Trace ID / 请求 ID</th>
                        <th {...stylex.props(ui.th)}>模型别名 → 上游模型</th>
                        <th {...stylex.props(ui.th)}>状态</th>
                        <th {...stylex.props(ui.th, ui.thNum)}>总耗时 / TTFT</th>
                        <th {...stylex.props(ui.th, ui.thNum)}>Token (入/出)</th>
                        <th {...stylex.props(ui.th, ui.thNum)}>实结费用</th>
                        <th {...stylex.props(ui.th)} />
                      </tr>
                    </thead>
                    <tbody>
                      {filteredLogs.map((log, idx) => (
                        <tr key={`${log.requestId || idx}-${log.timestamp || idx}`}>
                          <td {...stylex.props(ui.td)}>{fmtDate(log.timestamp)}</td>
                          <td {...stylex.props(ui.td, ui.tdMono)}>
                            <span {...stylex.props(ui.cellPrimary)}>
                              {String(log.traceId || '—').slice(0, 16)}…
                            </span>
                            <span {...stylex.props(ui.cellSub)}>{log.requestId || '—'}</span>
                          </td>
                          <td {...stylex.props(ui.td)}>
                            <span {...stylex.props(ui.cellPrimary)}>
                              {log.modelAlias || '—'}
                            </span>
                            <span {...stylex.props(ui.cellSub)}>
                              {log.upstreamModel || log.endpoint || '—'}
                            </span>
                          </td>
                          <td {...stylex.props(ui.td)}>
                            <UiBadge status={log.status} />
                          </td>
                          <td {...stylex.props(ui.td, ui.tdNum)}>
                            <span {...stylex.props(ui.cellPrimary)}>
                              {fmtDuration(log.latencyMs)}
                            </span>
                            <span {...stylex.props(ui.cellSub)}>
                              TTFT: {log.ttftMs != null ? fmtDuration(log.ttftMs) : '—'}
                            </span>
                          </td>
                          <td {...stylex.props(ui.td, ui.tdNum)}>
                            {fmtInt(log.inputTokens)} / {fmtInt(log.outputTokens)}
                          </td>
                          <td {...stylex.props(ui.td, ui.tdNum)}>
                            {fmtMoney(log.actualMicros)}
                          </td>
                          <td {...stylex.props(ui.td, ui.tdActions)}>
                            <UiButton
                              size="sm"
                              variant="outline"
                              onClick={() => setSelectedLog(log)}
                            >
                              JSON 详情
                            </UiButton>
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              ) : (
                <EmptyBox
                  title={
                    accessLogs.length
                      ? '当前筛选条件下没有慢请求日志'
                      : '环形缓冲区暂无结构化访问日志'
                  }
                  description="结构化慢请求日志由内置插件 builtin:obs-access-log 在 POST_SETTLE 阶段按阈值写入。前往微内核插件页可一键启用模板。"
                  action={
                    <UiButton
                      size="sm"
                      variant="primary"
                      onClick={() => onNavigate('plugins')}
                    >
                      前往启用慢日志插件模板
                    </UiButton>
                  }
                />
              )}
            </TabsPanel>

            <TabsPanel value="spans" style={{ marginTop: '16px' }}>
              {spans.length ? (
                <div {...stylex.props(ui.tableWrap)}>
                  <table {...stylex.props(ui.table)}>
                    <thead>
                      <tr>
                        <th {...stylex.props(ui.th)}>开始时间</th>
                        <th {...stylex.props(ui.th)}>W3C Trace ID / Span ID</th>
                        <th {...stylex.props(ui.th)}>GenAI 操作名</th>
                        <th {...stylex.props(ui.th, ui.thNum)}>耗时 / TTFT</th>
                        <th {...stylex.props(ui.th, ui.thNum)}>自定义事件数</th>
                        <th {...stylex.props(ui.th)} />
                      </tr>
                    </thead>
                    <tbody>
                      {spans.map((sp, idx) => (
                        <tr key={`${sp.spanId || idx}-${sp.startEpochMs || idx}`}>
                          <td {...stylex.props(ui.td)}>
                            {sp.startEpochMs ? fmtDate(new Date(sp.startEpochMs)) : '—'}
                          </td>
                          <td {...stylex.props(ui.td, ui.tdMono)}>
                            <span {...stylex.props(ui.cellPrimary)}>{sp.traceId}</span>
                            <span {...stylex.props(ui.cellSub)}>span: {sp.spanId}</span>
                          </td>
                          <td {...stylex.props(ui.td)}>{sp.operationName || '—'}</td>
                          <td {...stylex.props(ui.td, ui.tdNum)}>
                            <span {...stylex.props(ui.cellPrimary)}>
                              {fmtDuration(sp.durationMs)}
                            </span>
                            <span {...stylex.props(ui.cellSub)}>
                              TTFT: {sp.ttftMs != null ? fmtDuration(sp.ttftMs) : '—'}
                            </span>
                          </td>
                          <td {...stylex.props(ui.td, ui.tdNum)}>
                            {fmtInt(arr(sp.events).length)}
                          </td>
                          <td {...stylex.props(ui.td, ui.tdActions)}>
                            <UiButton
                              size="sm"
                              variant="outline"
                              onClick={() => setSelectedSpan(sp)}
                            >
                              查看 Span 属性
                            </UiButton>
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              ) : (
                <EmptyBox
                  title="暂无 OpenTelemetry GenAI Span"
                  description="发起一次模型请求或绑定 builtin:obs-otel-genai 插件后，这里将展示完整的 gen_ai.* 属性与插件自定义事件。"
                />
              )}
            </TabsPanel>

            <TabsPanel value="metrics" style={{ marginTop: '16px' }}>
              <div {...stylex.props(ui.formRow)} style={{ marginBottom: '12px' }}>
                <Field name="metricsFilter" {...stylex.props(ui.field)}>
                  <FieldLabel {...stylex.props(ui.fieldLabel)}>
                    按指标名称或标签过滤（例如：caffeine、hedge、plugin_metric）
                  </FieldLabel>
                  <Input
                    value={metricsFilter}
                    onValueChange={setMetricsFilter}
                    placeholder="输入关键字即时过滤 OpenMetrics 序列…"
                    {...stylex.props(ui.input)}
                  />
                </Field>
                <div {...stylex.props(ui.inlineRow)}>
                  <UiButton variant="secondary" onClick={loadPrometheusText}>
                    重新抓取 /metrics
                  </UiButton>
                  {metricsRaw && (
                    <UiButton
                      variant="outline"
                      onClick={() => copyToClipboard(filteredMetricsLines)}
                    >
                      复制指标文本
                    </UiButton>
                  )}
                </div>
              </div>
              <pre
                {...stylex.props(ui.codeBlock)}
                style={{ maxHeight: '380px', overflowY: 'auto' }}
              >
                <code>{filteredMetricsLines || '# 正在加载 /metrics 指标流…'}</code>
              </pre>
            </TabsPanel>
          </Tabs>
        </div>
      </section>

      {/* Structured Access Log Detail Dialog */}
      <UiDialog
        open={!!selectedLog}
        onOpenChange={(o) => !o && setSelectedLog(null)}
        title="结构化慢请求与访问日志详情"
        description={selectedLog ? `Trace ID: ${selectedLog.traceId || '—'}` : ''}
        wide
        footer={
          <>
            {selectedLog && (
              <UiButton
                variant="outline"
                onClick={() => copyToClipboard(JSON.stringify(selectedLog, null, 2))}
              >
                复制 JSON
              </UiButton>
            )}
            <UiButton variant="primary" onClick={() => setSelectedLog(null)}>
              关闭
            </UiButton>
          </>
        }
      >
        {selectedLog && (
          <pre
            {...stylex.props(ui.codeBlock)}
            style={{ maxHeight: '420px', overflowY: 'auto' }}
          >
            <code>{JSON.stringify(selectedLog, null, 2)}</code>
          </pre>
        )}
      </UiDialog>

      {/* OpenTelemetry Span Detail Dialog */}
      <UiDialog
        open={!!selectedSpan}
        onOpenChange={(o) => !o && setSelectedSpan(null)}
        title="OpenTelemetry GenAI Span 详情"
        description={selectedSpan ? `${selectedSpan.operationName} (${selectedSpan.spanId})` : ''}
        wide
        footer={
          <>
            {selectedSpan && (
              <UiButton
                variant="outline"
                onClick={() => copyToClipboard(JSON.stringify(selectedSpan, null, 2))}
              >
                复制 Span JSON
              </UiButton>
            )}
            <UiButton variant="primary" onClick={() => setSelectedSpan(null)}>
              关闭
            </UiButton>
          </>
        }
      >
        {selectedSpan && (
          <div {...stylex.props(ui.stackSm)}>
            <pre
              {...stylex.props(ui.codeBlock)}
              style={{ maxHeight: '420px', overflowY: 'auto' }}
            >
              <code>{JSON.stringify(selectedSpan, null, 2)}</code>
            </pre>
          </div>
        )}
      </UiDialog>
    </>
  );
}

// ============================================================================
// 2. MICROKERNEL PLUGINS, BUILT-IN TEMPLATES & DRY-RUN STUDIO (#/plugins)
// ============================================================================
export function PluginsView({
  project,
  canWrite,
  refreshTick,
}: {
  project: any;
  canWrite: boolean;
  refreshTick: number;
}) {
  const [plugins, setPlugins] = useState<any[]>([]);
  const [bindings, setBindings] = useState<any[]>([]);
  const [catFilter, setCatFilter] = useState<string>('all');

  // Create / Install Template Modal
  const [modalOpen, setModalOpen] = useState(false);
  const [pName, setPName] = useState('');
  const [pType, setPType] = useState<'BUILTIN' | 'JS' | 'WEBHOOK' | 'JAVA_SPI'>('BUILTIN');
  const [pClassName, setPClassName] = useState('');
  const [pPriority, setPPriority] = useState('40');
  const [pFailMode, setPFailMode] = useState<'FAIL_OPEN' | 'FAIL_CLOSED'>('FAIL_OPEN');
  const [pStages, setPStages] = useState<string[]>(['POST_SETTLE']);
  const [pConfigJson, setPConfigJson] = useState('{}');
  const [pSourceCode, setPSourceCode] = useState(DEFAULT_JS_SAMPLE);
  const [autoBindScope, setAutoBindScope] = useState<'PROJECT' | 'GLOBAL' | 'NONE'>('PROJECT');
  const [sampleRate, setSampleRate] = useState('1.0');
  const [formErr, setFormErr] = useState('');

  // Dry-run modal
  const [dryRunOpen, setDryRunOpen] = useState(false);
  const [drStage, setDrStage] = useState('PRE_ADMIT');
  const [drSourceCode, setDrSourceCode] = useState(DEFAULT_JS_SAMPLE);
  const [drPrompt, setDrPrompt] = useState('测试微内核插件沙箱与 ctx.telemetry 探针');
  const [drResult, setDrResult] = useState<any | null>(null);

  // Delete confirmation
  const [deletingPlugin, setDeletingPlugin] = useState<any | null>(null);

  async function reload() {
    const [plRes, bdRes] = await Promise.all([
      getSafe('/api/plugins'),
      getSafe('/api/plugins/bindings'),
    ]);
    if (plRes.ok) setPlugins(items(plRes.data));
    if (bdRes.ok) setBindings(items(bdRes.data));
  }

  useEffect(() => {
    reload();
  }, [project?.id, refreshTick]);

  function openFromTemplate(tpl: BuiltinPluginTemplate) {
    setPName(tpl.id);
    setPType('BUILTIN');
    setPClassName(tpl.className);
    setPPriority(String(tpl.defaultPriority));
    setPFailMode(tpl.failMode);
    setPStages([...tpl.stages]);
    setPConfigJson(JSON.stringify(tpl.defaultConfig, null, 2));
    setAutoBindScope(project?.id ? 'PROJECT' : 'GLOBAL');
    setSampleRate('1.0');
    setFormErr('');
    setModalOpen(true);
  }

  function openCustomJsModal() {
    setPName('custom-js-telemetry-guard');
    setPType('JS');
    setPClassName('');
    setPPriority('25');
    setPFailMode('FAIL_CLOSED');
    setPStages(['PRE_ADMIT']);
    setPConfigJson('{}');
    setPSourceCode(DEFAULT_JS_SAMPLE);
    setAutoBindScope(project?.id ? 'PROJECT' : 'GLOBAL');
    setSampleRate('1.0');
    setFormErr('');
    setModalOpen(true);
  }

  async function handleCreatePluginAndBind() {
    setFormErr('');
    if (!pName.trim()) {
      setFormErr('插件名称不能为空。');
      return;
    }
    if (!pStages.length) {
      setFormErr('请至少勾选一个挂载阶段。');
      return;
    }
    let parsedCfg: Record<string, any> = {};
    try {
      parsedCfg = pConfigJson.trim() ? JSON.parse(pConfigJson) : {};
    } catch {
      setFormErr('插件配置 JSON 格式不合法，请检查括号与引号。');
      return;
    }
    const prio = Number(pPriority) || 50;
    const sr = Math.min(1.0, Math.max(0.0, Number(sampleRate) || 1.0));

    try {
      const body: Record<string, any> = {
        name: pName.trim(),
        type: pType,
        priority: prio,
        stages: pStages,
        failMode: pFailMode,
        config: parsedCfg,
        enabled: true,
      };
      if (pType === 'BUILTIN' || pType === 'JAVA_SPI') {
        body.className = pClassName.trim();
      }
      if (pType === 'JS') {
        body.sourceCode = pSourceCode;
        body.maxStatements = 5000;
      }

      const created = await api('/api/plugins', { method: 'POST', body });
      if (autoBindScope !== 'NONE' && created?.id) {
        const scopeId = autoBindScope === 'PROJECT' && project?.id ? project.id : '*';
        await api('/api/plugins/bindings', {
          method: 'POST',
          body: {
            pluginId: created.id,
            scopeType: autoBindScope,
            scopeId,
            priority: prio,
            sampleRate: sr,
            enabled: true,
          },
        });
      }
      notify(
        autoBindScope === 'NONE'
          ? `插件 ${pName} 已注册`
          : `插件 ${pName} 已注册并绑定至 ${autoBindScope === 'PROJECT' ? '当前项目' : '全局'}`,
        'success',
      );
      setModalOpen(false);
      await reload();
    } catch (e: any) {
      setFormErr(e?.message || '注册插件失败');
    }
  }

  async function handleDeletePlugin(pl: any) {
    try {
      const related = bindings.filter((b) => b.pluginId === pl.id);
      for (const b of related) {
        await api(`/api/plugins/bindings/${encodeURIComponent(b.id)}`, { method: 'DELETE' });
      }
      await api(`/api/plugins/${encodeURIComponent(pl.id)}`, { method: 'DELETE' });
      notify(`已卸载插件 ${pl.name}`, 'success');
      await reload();
    } catch (e: any) {
      notify(e?.message || '删除插件失败', 'error');
    }
  }

  async function handleDeleteBinding(b: any) {
    try {
      await api(`/api/plugins/bindings/${encodeURIComponent(b.id)}`, { method: 'DELETE' });
      notify('插件作用域绑定已解除', 'success');
      await reload();
    } catch (e: any) {
      notify(e?.message || '解除绑定失败', 'error');
    }
  }

  async function executeDryRun() {
    try {
      const res = await api('/api/plugins/dry-run', {
        method: 'POST',
        body: {
          type: 'JS',
          stage: drStage,
          sourceCode: drSourceCode,
          requestBody: {
            model: 'support-chat',
            messages: [{ role: 'user', content: drPrompt }],
          },
        },
      });
      setDrResult(res);
      notify('沙箱 Dry-Run 执行完成，不影响生产账本', 'success');
    } catch (e: any) {
      setDrResult({ error: e?.message || 'Dry-Run 调试失败' });
    }
  }

  const visibleTemplates = BUILTIN_PLUGIN_TEMPLATES.filter(
    (t) => catFilter === 'all' || t.category === catFilter,
  );

  return (
    <>
      <PageHeader
        title="8 阶段微内核插件与配置模板"
        description="基于同构 PluginExecutionContext（ctx.kv / ctx.crypto / ctx.telemetry）按需挂载可观测性、自适应对冲路由、安全护栏与 FinOps 归因插件。"
        actions={
          canWrite ? (
            <>
              <UiButton variant="outline" onClick={() => setDryRunOpen(true)}>
                在线沙箱 Dry-Run
              </UiButton>
              <UiButton variant="primary" onClick={openCustomJsModal}>
                新建 GraalJS 插件
              </UiButton>
            </>
          ) : (
            <UiBadge tone="neutral" label="只读角色可浏览模板" />
          )
        }
      />

      {/* Built-in Plugin Configuration Templates Gallery */}
      <section {...stylex.props(ui.card)}>
        <div {...stylex.props(ui.cardHeader)}>
          <div>
            <h3 {...stylex.props(ui.cardTitle)}>内置微内核插件配置模板库</h3>
            <p {...stylex.props(ui.cardSubtitle)}>
              选择任意内置插件模板即可预填推荐阶段、容错策略与 JSON 参数，一键绑定至当前项目或全局。
            </p>
          </div>
          <div {...stylex.props(ui.inlineRow)}>
            {[
              ['all', '全部模板'],
              ['observability', '可观测性 (4)'],
              ['routing', '路由与对冲 (4)'],
              ['cache', '缓存与护栏'],
              ['security', '鉴权与限流'],
              ['finops', 'FinOps 计费'],
            ].map(([k, label]) => (
              <UiButton
                key={k}
                size="sm"
                variant={catFilter === k ? 'primary' : 'secondary'}
                onClick={() => setCatFilter(k)}
              >
                {label}
              </UiButton>
            ))}
          </div>
        </div>
        <div {...stylex.props(ui.cardBody, ui.grid2)}>
          {visibleTemplates.map((tpl) => {
            const installed = plugins.some(
              (p) => p.name === tpl.id || p.className === tpl.className,
            );
            return (
              <div key={tpl.id} {...stylex.props(ui.detailItem, ui.stackSm)}>
                <div {...stylex.props(ui.inlineRow)} style={{ justifyContent: 'space-between' }}>
                  <div {...stylex.props(ui.inlineRow)}>
                    <strong style={{ fontSize: '14px' }}>{tpl.title}</strong>
                    <UiBadge
                      tone={tpl.category === 'observability' ? 'info' : 'neutral'}
                      label={tpl.categoryLabel}
                    />
                  </div>
                  {installed && <UiBadge status="active" label="已实例化" />}
                </div>
                <div {...stylex.props(ui.tdMono)} style={{ color: 'var(--fg-muted)' }}>
                  {tpl.id} · 阶段: {tpl.stages.join(', ')} · 默认优先级 P{tpl.defaultPriority}
                </div>
                <p style={{ margin: 0, fontSize: '12px', color: 'var(--fg-muted)', lineHeight: 1.55 }}>
                  {tpl.summary}
                </p>
                <pre
                  {...stylex.props(ui.codeBlock)}
                  style={{ fontSize: '11px', padding: '8px 10px', maxHeight: '120px' }}
                >
                  <code>{JSON.stringify(tpl.defaultConfig, null, 2)}</code>
                </pre>
                {canWrite && (
                  <div {...stylex.props(ui.inlineRow)} style={{ justifyContent: 'flex-end' }}>
                    <UiButton
                      size="sm"
                      variant="secondary"
                      onClick={() => openFromTemplate(tpl)}
                    >
                      使用此配置模板启用
                    </UiButton>
                  </div>
                )}
              </div>
            );
          })}
        </div>
      </section>

      {/* Registered Plugins & Active Scope Bindings */}
      <div {...stylex.props(ui.grid2)}>
        <section {...stylex.props(ui.card)}>
          <div {...stylex.props(ui.cardHeader)}>
            <div>
              <h3 {...stylex.props(ui.cardTitle)}>已注册插件实例</h3>
              <p {...stylex.props(ui.cardSubtitle)}>
                支持 BUILTIN、GraalJS 确定性沙箱、JAVA_SPI 与外部 Webhook。
              </p>
            </div>
            <UiBadge tone="neutral" label={`${plugins.length} 个实例`} />
          </div>
          <div {...stylex.props(ui.cardBodyFlush)}>
            {plugins.length ? (
              <div {...stylex.props(ui.tableWrap)}>
                <table {...stylex.props(ui.table)}>
                  <thead>
                    <tr>
                      <th {...stylex.props(ui.th)}>名称 / 类型</th>
                      <th {...stylex.props(ui.th)}>挂载阶段</th>
                      <th {...stylex.props(ui.th)}>优先级 / 容错</th>
                      <th {...stylex.props(ui.th)} />
                    </tr>
                  </thead>
                  <tbody>
                    {plugins.map((pl) => (
                      <tr key={pl.id}>
                        <td {...stylex.props(ui.td)}>
                          <span {...stylex.props(ui.cellPrimary)}>{pl.name}</span>
                          <span {...stylex.props(ui.cellSub, ui.tdMono)}>
                            {pl.type} · v{pl.version}
                          </span>
                        </td>
                        <td {...stylex.props(ui.td)}>
                          <div {...stylex.props(ui.inlineRow)}>
                            {arr(pl.stages).map((st: string) => (
                              <UiBadge key={st} tone="neutral" label={st} />
                            ))}
                          </div>
                        </td>
                        <td {...stylex.props(ui.td, ui.tdMono)}>
                          P{pl.priority} · {pl.failMode}
                        </td>
                        <td {...stylex.props(ui.td, ui.tdActions)}>
                          {canWrite && (
                            <UiButton
                              size="sm"
                              variant="outline"
                              onClick={() => setDeletingPlugin(pl)}
                            >
                              卸载
                            </UiButton>
                          )}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            ) : (
              <EmptyBox
                title="尚未注册插件实例"
                description="从上方内置模板库点击“使用此配置模板启用”即可按需挂载可观测或路由对冲能力。"
              />
            )}
          </div>
        </section>

        <section {...stylex.props(ui.card)}>
          <div {...stylex.props(ui.cardHeader)}>
            <div>
              <h3 {...stylex.props(ui.cardTitle)}>作用域绑定与采样率（Plugin Bindings）</h3>
              <p {...stylex.props(ui.cardSubtitle)}>
                支持 GLOBAL / WORKSPACE / PROJECT / ALIAS / API_KEY 五级作用域与比例采样。
              </p>
            </div>
            <UiBadge tone="neutral" label={`${bindings.length} 条绑定`} />
          </div>
          <div {...stylex.props(ui.cardBodyFlush)}>
            {bindings.length ? (
              <div {...stylex.props(ui.tableWrap)}>
                <table {...stylex.props(ui.table)}>
                  <thead>
                    <tr>
                      <th {...stylex.props(ui.th)}>插件名称</th>
                      <th {...stylex.props(ui.th)}>作用域</th>
                      <th {...stylex.props(ui.th, ui.thNum)}>采样率</th>
                      <th {...stylex.props(ui.th)} />
                    </tr>
                  </thead>
                  <tbody>
                    {bindings.map((b) => {
                      const pl = plugins.find((x) => x.id === b.pluginId);
                      return (
                        <tr key={b.id}>
                          <td {...stylex.props(ui.td)}>
                            <span {...stylex.props(ui.cellPrimary)}>
                              {pl?.name || b.pluginId}
                            </span>
                            <span {...stylex.props(ui.cellSub, ui.tdMono)}>P{b.priority}</span>
                          </td>
                          <td {...stylex.props(ui.td, ui.tdMono)}>
                            <UiBadge tone="info" label={b.scopeType} />{' '}
                            <span>{b.scopeId === project?.id ? project.name : b.scopeId}</span>
                          </td>
                          <td {...stylex.props(ui.td, ui.tdNum)}>
                            {fmtPct(b.sampleRate ?? 1.0)}
                          </td>
                          <td {...stylex.props(ui.td, ui.tdActions)}>
                            {canWrite && (
                              <UiButton
                                size="sm"
                                variant="outline"
                                onClick={() => handleDeleteBinding(b)}
                              >
                                解绑
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
                title="暂无生效中的插件绑定"
                description="插件与具体作用域解耦：未绑定的插件不会在热路径中执行。"
              />
            )}
          </div>
        </section>
      </div>

      {/* Create / Configure Plugin from Template Modal */}
      <UiDialog
        open={modalOpen}
        onOpenChange={setModalOpen}
        title={pType === 'BUILTIN' ? '配置并启用内置微内核插件' : '创建自定义微内核插件'}
        description="保存后将通过 Redis Pub/Sub 秒级热更新同步至所有数据面节点。"
        wide
        footer={
          <>
            <UiButton variant="outline" onClick={() => setModalOpen(false)}>
              取消
            </UiButton>
            <UiButton variant="primary" onClick={handleCreatePluginAndBind}>
              注册并热加载生效
            </UiButton>
          </>
        }
      >
        <div {...stylex.props(ui.formGrid)}>
          <div {...stylex.props(ui.formRow)}>
            <Field name="pluginName" {...stylex.props(ui.field)}>
              <FieldLabel {...stylex.props(ui.fieldLabel)}>插件实例标识</FieldLabel>
              <Input value={pName} onValueChange={setPName} {...stylex.props(ui.input)} />
            </Field>
            <Field name="pluginPriority" {...stylex.props(ui.field)}>
              <FieldLabel {...stylex.props(ui.fieldLabel)}>执行优先级（越小越先执行）</FieldLabel>
              <Input
                type="number"
                value={pPriority}
                onValueChange={setPPriority}
                {...stylex.props(ui.input)}
              />
            </Field>
          </div>

          <div {...stylex.props(ui.formRow)}>
            <div {...stylex.props(ui.field)}>
              <label {...stylex.props(ui.fieldLabel)}>异常容错模式（Fail Mode）</label>
              <select
                value={pFailMode}
                onChange={(e) => setPFailMode(e.target.value as 'FAIL_OPEN' | 'FAIL_CLOSED')}
                {...stylex.props(ui.input)}
              >
                <option value="FAIL_OPEN">FAIL_OPEN（可观测/缓存推荐：异常自动旁路放行）</option>
                <option value="FAIL_CLOSED">FAIL_CLOSED（鉴权/安全护栏推荐：异常严格阻断）</option>
              </select>
            </div>
            <div {...stylex.props(ui.field)}>
              <label {...stylex.props(ui.fieldLabel)}>自动绑定作用域</label>
              <select
                value={autoBindScope}
                onChange={(e) =>
                  setAutoBindScope(e.target.value as 'PROJECT' | 'GLOBAL' | 'NONE')
                }
                {...stylex.props(ui.input)}
              >
                <option value="PROJECT">当前项目（{project?.name || 'Project'}）</option>
                <option value="GLOBAL">工作空间全局（GLOBAL *）</option>
                <option value="NONE">仅注册插件，暂不绑定</option>
              </select>
            </div>
            <Field name="sampleRate" {...stylex.props(ui.field)}>
              <FieldLabel {...stylex.props(ui.fieldLabel)}>执行采样率（0.0 ~ 1.0）</FieldLabel>
              <Input
                type="number"
                min="0"
                max="1"
                step="0.05"
                value={sampleRate}
                onValueChange={setSampleRate}
                {...stylex.props(ui.input)}
              />
            </Field>
          </div>

          <div {...stylex.props(ui.field)}>
            <span {...stylex.props(ui.fieldLabel)}>挂载流水线阶段</span>
            <div {...stylex.props(ui.grid3)}>
              {ALL_STAGES.map((st) => (
                <UiCheckboxRow
                  key={st}
                  checked={pStages.includes(st)}
                  onChange={(on) =>
                    setPStages((prev) =>
                      on ? [...new Set([...prev, st])] : prev.filter((x) => x !== st),
                    )
                  }
                  title={st}
                />
              ))}
            </div>
          </div>

          {pType === 'JS' && (
            <div {...stylex.props(ui.field)}>
              <label {...stylex.props(ui.fieldLabel)}>
                GraalJS 确定性沙箱源码（支持 ctx.kv / ctx.crypto / ctx.telemetry）
              </label>
              <textarea
                value={pSourceCode}
                onChange={(e) => setPSourceCode(e.target.value)}
                {...stylex.props(ui.textarea)}
                style={{ fontFamily: 'var(--font-mono)', minHeight: '130px' }}
              />
            </div>
          )}

          <div {...stylex.props(ui.field)}>
            <label {...stylex.props(ui.fieldLabel)}>插件 JSON 配置参数（config）</label>
            <textarea
              value={pConfigJson}
              onChange={(e) => setPConfigJson(e.target.value)}
              {...stylex.props(ui.textarea)}
              style={{ fontFamily: 'var(--font-mono)', minHeight: '130px' }}
            />
            <FieldDescription {...stylex.props(ui.fieldHint)}>
              内置类：{pClassName || 'GraalJS Sandbox'}
            </FieldDescription>
          </div>

          {formErr && <p {...stylex.props(ui.fieldError)}>{formErr}</p>}
        </div>
      </UiDialog>

      {/* Online Sandbox Dry-Run Debugger Dialog */}
      <UiDialog
        open={dryRunOpen}
        onOpenChange={setDryRunOpen}
        title="微内核插件在线沙箱 Dry-Run 调试"
        description="在不触碰生产流量与计费账本的前提下，演练 GraalJS 脚本、AST 语句燃料消耗与 ctx.telemetry 行为。"
        wide
        footer={
          <>
            <UiButton variant="outline" onClick={() => setDryRunOpen(false)}>
              关闭
            </UiButton>
            <UiButton variant="primary" onClick={executeDryRun}>
              执行沙箱演练
            </UiButton>
          </>
        }
      >
        <div {...stylex.props(ui.formGrid)}>
          <div {...stylex.props(ui.formRow)}>
            <div {...stylex.props(ui.field)}>
              <label {...stylex.props(ui.fieldLabel)}>模拟执行阶段</label>
              <select
                value={drStage}
                onChange={(e) => setDrStage(e.target.value)}
                {...stylex.props(ui.input)}
              >
                {ALL_STAGES.map((st) => (
                  <option key={st} value={st}>
                    {st}
                  </option>
                ))}
              </select>
            </div>
            <Field name="drPrompt" {...stylex.props(ui.field)}>
              <FieldLabel {...stylex.props(ui.fieldLabel)}>模拟用户 Prompt</FieldLabel>
              <Input value={drPrompt} onValueChange={setDrPrompt} {...stylex.props(ui.input)} />
            </Field>
          </div>
          <div {...stylex.props(ui.field)}>
            <label {...stylex.props(ui.fieldLabel)}>JS 插件代码</label>
            <textarea
              value={drSourceCode}
              onChange={(e) => setDrSourceCode(e.target.value)}
              {...stylex.props(ui.textarea)}
              style={{ fontFamily: 'var(--font-mono)', minHeight: '130px' }}
            />
          </div>
          {drResult && (
            <pre
              {...stylex.props(ui.codeBlock)}
              style={{ maxHeight: '220px', overflowY: 'auto' }}
            >
              <code>{JSON.stringify(drResult, null, 2)}</code>
            </pre>
          )}
        </div>
      </UiDialog>

      {/* Delete Plugin Confirm */}
      <UiConfirmDialog
        open={!!deletingPlugin}
        onOpenChange={(o) => !o && setDeletingPlugin(null)}
        title="卸载微内核插件"
        description={
          deletingPlugin
            ? `确认卸载插件“${deletingPlugin.name}”并同时解除其关联的作用域绑定？`
            : ''
        }
        confirmLabel="确认卸载"
        danger
        onConfirm={async () => {
          if (deletingPlugin) await handleDeletePlugin(deletingPlugin);
        }}
      />
    </>
  );
}
