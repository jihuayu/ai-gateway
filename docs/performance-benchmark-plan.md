# AI Gateway 性能压测与金融账本极限验证方案 (Performance Benchmarking & Load Testing Plan)

- **项目仓库**：[`jihuayu/ai-gateway`](https://github.com/jihuayu/ai-gateway)
- **适用版本**：`v0.3.0-microkernel`（Java 25 虚拟线程 + Netty 4.2 + Redis 7 Lua 三阶段原子账本 + 八阶段微内核插件管线 + GraalVM 25 Native Image）
- **参考基准工具与规范**：[Grafana k6](https://grafana.com/docs/k6/latest/)、[OpenJDK JMH](https://github.com/openjdk/jmh)、[wrk2](https://github.com/giltene/wrk2)（修正协调遗漏 Coordinated Omission）、[OpenTelemetry GenAI Semantic Conventions](https://opentelemetry.io/docs/specs/semconv/gen-ai/)

---

## 1. 压测目标与核心验证不变量 (Objectives & Core Invariants)

[`jihuayu/ai-gateway`](https://github.com/jihuayu/ai-gateway) 的数据面（`gateway-data-plane`）采用 **Zero-SQL 热路径架构**（鉴权、RPM/TPM 限流、并发租约、微美元预算预扣与结算全部在 Redis 7 单线程 Lua 脚本中完成，PostgreSQL 仅由 `HotProjector` 异步消费 Redis Stream 投射），并在 P3 演进为 **八阶段微内核插件架构**（同时支持 Java `BUILTIN`/`JAVA_SPI` 与 GraalJS `JS` 有状态沙箱插件）。

因此，性能压测不仅要测量传统的 **吞吐量（RPS）与网关附加延迟（$\Delta L_{\text{gw}}$）**，还必须在极限并发与故障注入下严格证明以下 **三大核心不变量**：

1. **微内核极低附加延迟不变量（Sub-5ms Proxy Overhead）**：
   - 在剥离上游大模型推理耗时（使用 0ms/固定延迟 Mock Upstream）的条件下，网关自身的代理附加延迟 $\Delta L_{\text{gw}} = L_{\text{e2e}} - L_{\text{upstream}}$ 与流式首字附加延迟 $\Delta \text{TTFT}_{\text{gw}}$ 必须保持在亚毫秒至个位数毫秒级。
2. **金融账本零漂移与零超支不变量（Zero Ledger Drift & Zero Overdraft）**：
   - 在高并发预算耗尽竞态、客户端中途断连（HTTP `499`）、上游超时/限流（`429`/`502`/`503`/`504`）触发多级 Fallback 场景下，**绝不允许出现任何 1 微美元（$10^{-6}\text{ USD}$）的超额扣费、幽灵预占（Leaked Reservation）或 Redis 与 PostgreSQL 账本不一致**。
3. **有状态沙箱隔离不变量（Sandbox Fuel & Memory Isolation）**：
   - GraalJS 动态插件与 `ctx.kv()` / `ctx.crypto()` 在高并发虚拟线程调度下，Context 池借还无锁饥饿、无内存泄漏，恶意死循环插件在 `maxStatements` 燃料耗尽时 $100\%$ 在毫秒级内被熔断且不影响同节点正常流量。

---

## 2. 核心性能指标与 SLO 验收基线 (Key Metrics & SLO Targets)

压测标准测试节点规格设定为 **4 vCPU / 8 GB RAM**（网关数据面）、**2 vCPU / 4 GB RAM**（Redis 7 单主节点）、**4 vCPU / 8 GB RAM**（PostgreSQL 15 + 控制面 + `HotProjector`）：

| 测试维度 | 关键指标 (Metric) | 计算口径 | 目标 SLO 基线 (4 vCPU Gateway Node) |
| :--- | :--- | :--- | :--- |
| **缓存短路热路径**<br>*(Stage 4 `ExactCachePlugin` HIT)* | 吞吐量 / 延迟 | 经过 `AUTHENTICATE` $\rightarrow$ `ADMIT` $\rightarrow$ `CACHE HIT` $\rightarrow$ `SETTLE(0)` 全流程 | **RPS $\ge 18,000$**<br>**P50 $\le 0.8\text{ ms}$** · **P99 $\le 2.5\text{ ms}$** |
| **零插件基线转发**<br>*(0-Plugin Baseline Proxy)* | 网关附加延迟 $\Delta L_{\text{gw}}$ | Mock Upstream `0ms` 响应下，网关端到端耗时 | **RPS $\ge 6,500$** (`appendfsync=everysec`)<br>**RPS $\ge 2,800$** (`appendfsync=always`)<br>**P50 $\le 1.2\text{ ms}$** · **P95 $\le 3.0\text{ ms}$** · **P99 $\le 5.0\text{ ms}$** |
| **八阶段内置 Java 插件全开**<br>*(8-Stage Built-in Java Suite)* | 插件管线附加耗时 | 开启 `HMAC Auth` + `EndUserRateLimit` + `PiiGuardrail` + `LeastTtftRouter` + `CostAttribution` + `RedisStreamSink` | 相比零插件基线：<br>**$\Delta\text{P50} \le +0.4\text{ ms}$** · **$\Delta\text{P99} \le +1.8\text{ ms}$** |
| **GraalJS 动态有状态插件**<br>*(Sandboxed JS + `ctx.kv` + `ctx.crypto`)* | JS 沙箱执行开销 | 单请求执行 1 个含 `ctx.kv.incr` + `ctx.crypto.hmacSha256` 的 JS 插件 | 相比零插件基线：<br>**$\Delta\text{P50} \le +0.7\text{ ms}$** · **$\Delta\text{P99} \le +3.5\text{ ms}$** |
| **流式 SSE 代理与逐块脱敏**<br>*(SSE Streaming + `onStreamChunk`)* | 首字附加延迟 $\Delta\text{TTFT}_{\text{gw}}$<br>逐块转发抖动 $\Delta\text{ITL}$ | `upstream_sent_ns` 到客户端收到首帧 `data:` 的时间差；单连接 $50\text{ chunks/s}$ 持续 $20\text{s}$ | **单机并发长连接 $\ge 10,000$**<br>**$\Delta\text{TTFT}_{\text{gw}}$ P99 $\le 3.5\text{ ms}$**<br>**单 Chunk 处理 P99 $\le 0.15\text{ ms}$** |
| **TypeSafe Jev `/v1/systemone`**<br>*(System One & Rerank 转译)* | 结构化决策吞吐与转换耗时 | `noul` + `choice` + `score` 混合问题包校验、计费与跨协议 Rerank 转译 | **RPS $\ge 7,000$**<br>**协议转换开销 P99 $\le 0.5\text{ ms}$** |
| **异步账本投射器**<br>*(`HotProjector` Redis $\rightarrow$ PG)* | 投射吞吐与端到端落库延迟 | `gateway:{ns}:hot:v1:journal` Stream 消费写入 PG `requests`/`ledger` | **持续落库吞吐 $\ge 5,000\text{ events/s}$**<br>**Stream Backlog 积压恢复时间 $\le 10\text{s}$** |
| **JVM vs GraalVM Native**<br>*(Runtime Footprint)* | 冷启动耗时 / 10k 连接 RSS 内存 | 进程启动至 `/health/ready` 返回 `200`；10,000 并发 SSE 下 RSS | **Native 冷启动 $\le 80\text{ ms}$**（JVM $\le 1.8\text{s}$）<br>**Native 空载 RSS $\le 65\text{ MB}$**，峰值 RSS $\le 1.2\text{ GB}$ |

---

## 3. 压测拓扑与无成本 Mock 上游设计 (Benchmark Topology & Mock Upstream)

为了避免真实大模型提供商（OpenAI / Anthropic / Gemini / TypeSafe AI）的外部配额限制与公网抖动污染网关自身的性能数据，压测环境采用 **物理隔离的四层拓扑**：

```
┌─────────────────────────────────────────────────────────────────────────┐
│ 1. Load Generator Node (8 vCPU)                                         │
│    • Grafana k6 (HTTP / SSE Stream / Fault Injection Scenarios)         │
│    • wrk2 (Constant-Throughput Latency Histogram without Coord Omission)│
│    • JMH (In-Process Microbenchmarks for PluginPipeline & JsSandbox)    │
└───────────────────────────────────┬─────────────────────────────────────┘
                                    │ 10GbE Loopback / Internal LAN
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│ 2. AI Gateway Data Plane Under Test (4 vCPU / 8 GB RAM)                 │
│    • NettyGatewayServer (NIO Worker Group + Virtual Thread Executor)    │
│    • DataPlaneExecutor (Shared HttpClient Keep-Alive Pool)              │
│    • PluginPipeline (8 Stages + GraalJS Pool + L1 KV/Exact/Sem Cache)   │
└───────────────┬─────────────────────────────────────────┬───────────────┘
                │ Lettuce Async/Sync Connection           │ Pooled HTTP/1.1 Keep-Alive
                ▼                                         ▼
┌───────────────────────────────────────┐ ┌───────────────────────────────────────┐
│ 3. Redis 7 + PG 15 Storage Tier       │ │ 4. High-Speed Netty Mock Upstream     │
│    • Redis 7 (hot.lua + Plugin KV)    │ │    • Configurable TTFT (0ms / 50ms)   │
│    • HotProjector -> PostgreSQL 15    │ │    • Configurable Chunk Rate (50/s)   │
│    • Billing Webhook Receiver         │ │    • Fault Modes (429/502/504/Slow)   │
└───────────────────────────────────────┘ └───────────────────────────────────────┘
```

### 3.1 高性能可编程 Mock Upstream 支持的参数化控制头
在压测专用 Mock Upstream Server 中，通过请求 Header 或路径参数动态控制响应特征：
- `X-Mock-Latency-Ms: 0 | 20 | 100`：控制非流式响应或流式首帧（TTFT）等待时长。
- `X-Mock-Stream-Chunks: 30` & `X-Mock-Chunk-Interval-Ms: 20`：模拟真实 LLM 每秒吐出 50 个 Token 的 SSE 流，并在每个 Chunk 内嵌 `"upstream_sent_ns"` 纳秒时间戳，以便压测客户端精确计算 **网关单帧转发延迟**。
- `X-Mock-Fail-Rate: 0.05` & `X-Mock-Fail-Status: 429 | 502 | 504`：按指定概率注入上游故障，压测网关熔断器（Circuit Breaker）与多 Target 自动 Fallback 性能。

---

## 4. 六大核心压测场景矩阵 (Six Benchmark Scenario Suites)

### 场景 S1：组件级 JMH 微基准测试 (In-Process JMH Microbenchmarks)
在 `gateway-common` 中通过 JMH 直接压测纯内存/沙箱热点函数（排除网络与 Redis I/O）：
1. **`JsPluginSandboxBenchmark`**：
   - 对比 1、8、32、128 并发虚拟线程从 `CompiledJsPlugin` 的 `ArrayBlockingQueue<Context>` 借用 GraalJS `Context` 执行 `onPreAdmit` + `ctx.crypto.hmacSha256` + `ctx.kv.get` 的吞吐（`ops/ms`）与排队等待耗时。
   - 验证当并发度超过池大小（`DEFAULT_POOL_SIZE = max(8, CPU * 2)`）时，动态临时实例回退（Fallback Transient Context）的 P99 延迟边界。
2. **`PiiAndCachePluginBenchmark`**：
   - 测量 `PiiGuardrailPlugin` 在 `1 KB`、`16 KB`、`64 KB` 消息体以及 `32 Bytes` SSE Chunk 上执行邮箱/手机号/身份证/密钥正则脱敏的 CPU 耗时。
   - 测量 `SemanticCachePlugin` 64 维 SimHash 局部敏感哈希与汉明距离（Hamming Distance）检索在 L1 缓存容量 `1,000` / `10,000` 条时的吞吐。
3. **`ProtocolTranslationBenchmark`**：
   - 测量 Anthropic `/v1/messages` $\leftrightarrow$ OpenAI、Gemini Imagen `:predict` $\leftrightarrow$ `/v1/images/generations`、TypeSafe Jev `/v1/systemone` $\leftrightarrow$ `/v1/rerank` 的 JSON 序列化与结构转换开销。

### 场景 S2：端到端阶梯吞吐与微内核八阶段剥离压测 (Step-Load & Stage Ablation)
使用 `wrk2` 与 `k6` 的 `ramping-arrival-rate` 执行器，以 `500 -> 1,500 -> 3,000 -> 6,000 -> 10,000 -> 15,000 RPS` 阶梯加压（每阶梯持续 3 分钟），分别测试以下 5 组配置（Ablation Study）：
- **Group A（纯内核基线）**：0 插件绑定，标准 `Bearer sk-...` 鉴权 + Redis Lua `ADMIT`/`DISPATCH`/`SETTLE`。
- **Group B（奇异鉴权 + 多维限流）**：开启 Stage 1 `HmacSignatureAuthPlugin`（含 `ctx.kv().setNx` 防重放）+ Stage 2 `EndUserRateLimitPlugin` + `SpendVelocityCircuitPlugin`。
- **Group C（AI 护栏 + Jev 智能语义路由）**：开启 Stage 3 `PiiGuardrailPlugin` + `JevCalibratedGuardrailPlugin` + `JevSemanticRouterPlugin` + `PromptDecoratorTrimmerPlugin` + Stage 5 `LeastTtftRouterPlugin`。
- **Group D（动态 GraalJS + 账单 Sink 全开）**：在 Group C 基础上叠加 1 个动态 `JS` 插件 + Stage 8 `CostAttributionTagPlugin` + `WebhookBillingSinkPlugin` + `RedisStreamBillingSinkPlugin`。
- **Group E（缓存命中短路）**：开启 Stage 4 `ExactCachePlugin` / `SemanticCachePlugin`（控制 `80%` 缓存命中率 + `20%` 回源率）。

### 场景 S3：万人并发 SSE 流式长连接与流式缓存回放压测 (10k Concurrent SSE Soak Test)
- **负载模型**：启动 `10,000` 个并发虚拟用户（VUs），每个连接发起 `POST /v1/chat/completions`（`"stream": true`）或 `POST /v1/messages`（`"stream": true`），每次请求持续流式传输 `20 秒`（每秒 25 个 SSE Chunk），同时挂载 `PiiGuardrailPlugin`（`STREAM_CHUNK` 阶段逐块脱敏）与 `ExactCachePlugin`（`cacheStreaming: true`）。
- **核心观测项**：
  1. 网关进程 Netty `PooledByteBufAllocator` 堆外内存（Direct Memory）与虚拟线程 Carrier Thread 是否发生 Pinning 阻塞。
  2. `HotBilling.heartbeat(requestId)` 续约任务在 `10,000` 个活跃请求同时在途时对 Redis 的每秒 `EVALSHA` 压力与租约续期成功率（目标 $100\%$ 无误判 `lease_lost`）。
  3. 重复 Prompt 的 `stream: true` 请求命中 `ExactCachePlugin` 后的 SSE 帧回放吞吐与 `0` 微美元结算准确性。

### 场景 S4：金融账本并发竞态与零超支极限压测 (Financial Concurrency & Zero-Overdraft Torture)
专门验证 `hot.lua` 在极端并发争抢同一项目/Key 预算池时的金融原子性：
1. **预算边界百发争抢（Budget Exhaustion Race）**：
   - 设定项目预算 `budget_micros = 100,000`（$0.10 USD），单次请求预扣 `reserve_micros = 1,000`，实际结算消耗 `actual_micros = 600`（即每次结算退还 `400` 微美元）。
   - 瞬间使用 `500` 个并发线程连续发送 `1,000` 个请求。
   - **验收断言**：
     - 最终成功请求数 $N_{\text{ok}}$ 必须严格满足 $N_{\text{ok}} \times 600 = 99,600 \le 100,000$（即恰有 $166$ 个请求成功，其余全部返回 `402 budget_exceeded`）。
     - Redis `gateway:{ns}:hot:v1:project:{id}` 中的 `spent` 严格等于 `99,600`，`reserved` 严格等于 `0`，`leases` 严格为空对象 `{}`。
     - 等待 `HotProjector` 投射完成后，执行 SQL 对账查询，验证 PostgreSQL 与 Redis 零误差。
2. **TPM / RPM 窗口边界突发压测**：
   - 验证项目级 `project_tpm`、Key 级 `key_tpm` 以及插件级 `EndUserRateLimitPlugin` 在分钟切换边界处的计数精确度与 `Retry-After` 响应头正确性。

### 场景 S5：上游混沌故障注入、熔断切换与客户端断连压测 (Chaos & Failover Resilience)
在 `3,000 RPS` 恒定流量下注入三类故障：
1. **主 Target 突发 503/429 与慢响应（Latency Spike）**：
   - 将 Primary Target 注入 `100%` HTTP `503` 或首包超时（`> firstByteMs`），验证滑动窗口熔断器（`circuitBreakerThreshold = 3`）在 `< 5ms` 内切流至 Fallback Target，且 `LeastTtftRouterPlugin` 的 EWMA 权重自动降级慢节点。
2. **流式传输中途客户端暴烈断连（Client Abrupt Disconnect / HTTP 499）**：
   - `20%` 的 SSE 客户端在收到第 3 个 Chunk 后直接发送 TCP `RST`/`FIN` 关闭连接。
   - **验收断言**：`NettyResponseSender.isClientClosed()` 立即感知并中断上游 `InputStream`，该请求以 `status = 'cancelled'` 或 `'partial'` 走保守上限/已产生成本结算，**绝不遗留未释放的并发槽位（Concurrency Slot）或预算预占**。
3. **恶意 JS 插件死循环与 Redis 瞬断故障隔离**：
   - 在线热更新一个包含 `while(true){}` 的 JS 插件（配置 `maxStatements = 5000`），验证当 `failMode = FAIL_OPEN` 时请求 100% 自动降级放行并携带 `x-gateway-plugin-fallback: fail_open`；当 `failMode = FAIL_CLOSED` 时在 `< 2ms` 内返回 `400 js_plugin_fuel_exhausted`。

### 场景 S6：Redis 持久化策略与集群 Hash-Tag 对比压测 (Redis Durability & Cluster Scaling)
对比同一硬件下三种 Redis 部署配置对 `hot.lua` 账本吞吐的影响：
1. **金融强持久模式（`appendonly=yes`, `appendfsync=always`）**：每次 `ADMIT`/`DISPATCH`/`SETTLE` 落盘，测量磁盘 IOPS 瓶颈点与 Group Commit 表现。
2. **准实时持久模式（`appendonly=yes`, `appendfsync=everysec`）**：测量单 Redis 实例的 CPU 饱和极限（通常可达 `10,000+ RPS`）。
3. **Redis Cluster 分片模式（`HotKeys.forClusterProject(projectId)`）**：使用 `{proj:<projectId>}` CRC16 Hash-Tag 将 100 个不同 Project 分散到 3 主 3 从 Redis Cluster 分片，验证跨项目线性水平扩展能力。

---

## 5. 自动化压测脚本与对账验收套件 (Executable Benchmark & Reconciliation Suite)

### 5.1 Grafana k6 混合工作负载压测脚本 (`benchmarks/k6-microkernel-suite.js`)

```javascript
import http from 'k6/http';
import { check } from 'k6';
import { Trend, Counter, Rate } from 'k6/metrics';
import crypto from 'k6/crypto';

const gwProxyLatency = new Trend('gateway_proxy_latency_ms', true);
const gwStreamTtft = new Trend('gateway_stream_ttft_ms', true);
const cacheHitRate = new Rate('gateway_cache_hit_rate');
const ledgerRejects = new Counter('gateway_ledger_rejects_total');

export const options = {
  scenarios: {
    buffered_chat_and_plugins: {
      executor: 'ramping-arrival-rate',
      startRate: 500,
      timeUnit: '1s',
      preAllocatedVUs: 256,
      maxVUs: 2048,
      stages: [
        { target: 2000, duration: '2m' },
        { target: 5000, duration: '3m' },
        { target: 8000, duration: '2m' },
        { target: 0, duration: '30s' },
      ],
      exec: 'runBufferedChatWithHmacAndPlugins',
    },
    sse_streaming_soak: {
      executor: 'constant-vus',
      vus: 1000,
      duration: '5m',
      exec: 'runStreamingSseChat',
    },
    typesafe_jev_decisions: {
      executor: 'constant-arrival-rate',
      rate: 2000,
      timeUnit: '1s',
      duration: '5m',
      preAllocatedVUs: 128,
      maxVUs: 512,
      exec: 'runTypeSafeJevSystemOne',
    },
  },
  thresholds: {
    'gateway_proxy_latency_ms': ['p(50)<1.5', 'p(95)<3.5', 'p(99)<6.0'],
    'gateway_stream_ttft_ms': ['p(50)<2.0', 'p(99)<5.0'],
    'http_req_failed{expected_response:true}': ['rate<0.001'],
  },
};

const BASE_URL = __ENV.GATEWAY_URL || 'http://127.0.0.1:18080';
const API_KEY = __ENV.GATEWAY_API_KEY || 'sk-benchmark-key-0001';
const HMAC_AK = __ENV.GATEWAY_HMAC_AK || 'ak_bench_01';
const HMAC_SK = __ENV.GATEWAY_HMAC_SK || 'sk_bench_secret_999';

export function runBufferedChatWithHmacAndPlugins() {
  const promptPool = [
    'Explain deterministic microkernel AI gateway billing in one paragraph.',
    'Compare Least-TTFT routing with Prefix-Cache affinity routing.',
    'Contact support@acme.io or 13800138000 for FinOps reconciliation.',
    `Unique uncached request nonce ${__VU}-${__ITER}`,
  ];
  const prompt = promptPool[__ITER % promptPool.length];
  const payload = JSON.stringify({
    model: 'support-chat',
    temperature: 0.5,
    messages: [{ role: 'user', content: prompt }],
  });

  const ts = Math.floor(Date.now() / 1000).toString();
  const nonce = `${__VU}-${__ITER}-${Date.now()}`;
  const canonical = `${HMAC_AK}\n${ts}\n${nonce}\n/v1/chat/completions`;
  const sig = crypto.hmac('sha256', HMAC_SK, canonical, 'hex');

  const res = http.post(`${BASE_URL}/v1/chat/completions`, payload, {
    headers: {
      'Content-Type': 'application/json',
      'Authorization': `Bearer ${API_KEY}`,
      'X-Ak': HMAC_AK,
      'X-Timestamp': ts,
      'X-Nonce': nonce,
      'X-Signature': sig,
      'X-End-User-Id': `user-${__VU % 500}`,
      'X-Cost-Center': 'cc-benchmark',
      'X-Department': 'platform-eng',
    },
  });

  gwProxyLatency.add(res.timings.duration);
  cacheHitRate.add(res.headers['X-Gateway-Cache'] === 'HIT');
  if (res.status === 402 || res.status === 429) {
    ledgerRejects.add(1);
  }
  check(res, {
    'status is 200 or expected rate/budget limit': (r) => r.status === 200 || r.status === 429 || r.status === 402,
  });
}

export function runStreamingSseChat() {
  const payload = JSON.stringify({
    model: 'support-chat',
    stream: true,
    messages: [{ role: 'user', content: `Stream benchmark VU ${__VU}` }],
  });
  const res = http.post(`${BASE_URL}/v1/chat/completions`, payload, {
    headers: {
      'Content-Type': 'application/json',
      'Authorization': `Bearer ${API_KEY}`,
    },
  });
  gwStreamTtft.add(res.timings.waiting);
  check(res, {
    'stream 200 and terminated with [DONE]': (r) => r.status === 200 && r.body.includes('data: [DONE]'),
  });
}

export function runTypeSafeJevSystemOne() {
  const payload = JSON.stringify({
    model: 'jev-decisions',
    state: 'Customer requests an immediate refund for duplicate invoice #9821.',
    questions: {
      billing: { type: 'noul', instructions: 'Is this about billing?' },
      tier: { type: 'choice', instructions: 'Route ticket', options: ['billing_refund', 'tech_support'] },
      urgency: { type: 'score', instructions: 'Urgency 0..3' },
    },
  });
  const res = http.post(`${BASE_URL}/v1/systemone`, payload, {
    headers: {
      'Content-Type': 'application/json',
      'Authorization': `Bearer ${API_KEY}`,
    },
  });
  gwProxyLatency.add(res.timings.duration);
  check(res, {
    'jev 200 with answers': (r) => r.status === 200 && r.body.includes('"answers"'),
  });
}
```

### 5.2 压测后金融账本一致性自动对账脚本 (Post-Benchmark Ledger Audit SQL & Redis Check)
每次压测结束后，必须运行以下自动化对账脚本；任一项不为 `0` 即判定压测失败：

```sql
-- 1. 校验是否存在卡在非终态的僵尸请求或未释放预占 (必须返回 0)
SELECT COUNT(*) AS stuck_inflight_requests
  FROM requests
 WHERE status IN ('preparing', 'reserved', 'dispatch_intent', 'dispatched');

-- 2. 校验 requests 表累计入账金额与 ledger 账本流水表是否 100% 严丝合缝 (diff_micros 必须为 0)
SELECT COALESCE((SELECT SUM(charged_micros) FROM requests WHERE status IN ('completed', 'partial', 'failed', 'timeout', 'cancelled')), 0)
     - COALESCE((SELECT SUM(amount_micros)  FROM ledger), 0) AS pg_internal_diff_micros;

-- 3. 校验预占表 (reservations) 中所有已结算请求的 remaining_reserved_micros 是否全部归零
SELECT COUNT(*) AS leaked_reservations
  FROM reservations
 WHERE status NOT IN ('settled', 'released', 'cancelled');
```

同时对比 Redis 热账本 `gateway:{ns}:hot:v1:project:{projectId}` JSON 中的 `budgets[period].spent`、`budgets[period].reserved` 与 `leases`：
- `budgets[period].reserved == 0`
- `Object.keys(leases).length == 0`
- `budgets[period].spent == SELECT SUM(amount_micros) FROM ledger WHERE project_id = ? AND period = ?`

---

## 6. 性能剖析与瓶颈调优清单 (Profiling & Tuning Checklist)

在压测执行过程中，使用 **JDK Flight Recorder (JFR)** 与 **Async-Profiler** 重点监控并按需调优以下参数：

1. **虚拟线程 Pinning 检测**：
   - 启动参数开启 `-Djdk.tracePinnedThreads=short` 或通过 JFR 事件 `jdk.VirtualThreadPinned` 采集，确保 GraalJS `Context.enter()/leave()`、Lettuce Redis 调用与 Jackson 序列化路径上 **0 Carrier Thread Pinning**。
2. **GraalJS Context Pool 容量与预热**：
   - 默认 `JsPluginSandbox` 每个 `CompiledJsPlugin` 维护 `max(8, CPU * 2)` 个预编译 `Context` 实例；在重度 JS 插件场景（`> 5,000 RPS`）下，可通过压测确定最佳池大小（如 `32` 或 `64`），并在插件发布（`HotRuntime.publish()`）时完成预热编译。
3. **Netty 与上游连接池参数**：
   - 监控 `GET /metrics` 中的 `ai_gateway_pooled_upstream_requests_total` 与 TCP `TIME_WAIT` 连接数，确保 `DataPlaneExecutor.sharedHttpClient` 连接复用率 $\ge 99.5\%$。
4. **Redis Pipeline / Lua 执行耗时监控**：
   - 使用 `redis-cli --latency-history` 与 `SLOWLOG GET 20` 监控 `hot.lua` 各分支耗时，确保单次 `ADMIT` 或 `SETTLE` Lua 脚本在 Redis 内部执行耗时 $\le 120\,\mu\text{s}$。
