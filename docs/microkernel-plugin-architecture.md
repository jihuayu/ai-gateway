# AI Gateway 微内核与全链路插件化架构设计文档 (Microkernel & Full-Lifecycle Plugin Architecture)

- **项目仓库**：[`jihuayu/ai-gateway`](https://github.com/jihuayu/ai-gateway)
- **适用版本**：`v0.3.0-microkernel`（基于 Gradle 9.1 Kotlin DSL 多模块 + Oracle GraalVM 25 Native Image）
- **对标与参考架构**：[Kong AI Gateway](https://developer.konghq.com/ai-gateway/)、[Envoy AI Gateway](https://github.com/envoyproxy/ai-gateway)、[Higress](https://github.com/higress-group/higress)、[LiteLLM](https://github.com/BerriAI/litellm)、[Bifrost](https://github.com/maximhq/bifrost)、[TypeSafe AI Jev (`/v1/systemone`)](https://docs.typesafe.ai/api/system-one)

---

## 1. 设计背景与微内核哲学 (Microkernel Philosophy)

在完成 P2 阶段核心能力（[PR #44](https://github.com/jihuayu/ai-gateway/pull/44) 连接池与多协议计费、[PR #45](https://github.com/jihuayu/ai-gateway/pull/45) 五阶段插件引擎与动态 JS/JAR 热加载、[PR #46](https://github.com/jihuayu/ai-gateway/pull/46) MCP 代理与可观测性、[PR #48](https://github.com/jihuayu/ai-gateway/pull/48) 生图与 Rerank/Embedding 跨协议转换、[PR #50](https://github.com/jihuayu/ai-gateway/pull/50) TypeSafe AI Jev 原生支持）后，数据面执行器 `DataPlaneExecutor` 中仍存在部分硬编码的业务策略（如固定从 `Authorization: Bearer` 提取 API Key、固定模型白名单校验、固定加权熔断路由选择、单一 PostgreSQL 账单投射）。

为了支撑企业内部复杂的定制化场景（如**奇异的私有鉴权协议、多层级防烧钱限流、基于 [TypeSafe AI Jev](https://typesafe.ai/) 的百毫秒级智能语义路由、流式 SSE 逐块脱敏与缓存回放、多目标账单 Sink 实时导出**），我们将 [`jihuayu/ai-gateway`](https://github.com/jihuayu/ai-gateway) 演进为**“确定性微内核（Deterministic Microkernel）+ 八阶段可插拔插件管线（8-Stage Plugin Pipeline）”**架构。

### 1.1 微内核与插件层的职责边界

| 架构层次 | 核心定位 | 包含组件 | 严守的不变量 (Invariants) |
| :--- | :--- | :--- | :--- |
| **微内核底座**<br>*(Microkernel Core)* | 只负责**网络传输、确定性资金状态机、热快照同步、插件调度与安全沙箱**，不包含任何硬编码业务策略 | 1. `NettyGatewayServer` + 虚拟线程 `HttpClient` 连接池<br>2. `HotBilling` (`hot.lua` 三阶段微美元原子账本)<br>3. `HotRuntime` (Redis Snapshot + Pub/Sub 零 SQL 配置总线)<br>4. `PluginPipeline` + `JsPluginSandbox` + `DynamicJarPluginLoader` | 1. **热路径 Zero-SQL**（数据面绝不直连 PostgreSQL）<br>2. **金融确定性**（微美元 $10^{-6}\text{ USD}$ 整数记账，预扣不超支，崩溃租约可回收）<br>3. **GraalVM 25 Native 兼容**（零未声明反射，JS 插件运行时双通道动态热加载） |
| **全链路插件层**<br>*(Pluggable Policy & Extension Layer)* | 负责**鉴权、限流、体系规则、Prompt 治理、护栏、缓存、路由与负载均衡、协议适配、账单 Sink** | 1. **编译期内联内置插件 (`BUILTIN`)**<br>2. **同构 Java SPI / 动态 `.jar` 插件 (`JAVA_SPI`)**<br>3. **运行时动态 `.js` 插件 (`JS`)**<br>4. **外部 HTTP 处理器 (`WEBHOOK` / `ext_proc`)** | 1. 插件按 `(stage, priority ASC, id ASC)` 确定性顺序执行<br>2. 支持绑定级 `FAIL_OPEN` / `FAIL_CLOSED` 故障隔离与 `sample_rate` 灰度采样<br>3. 动态 `.js` 插件受语句燃料上限（`maxStatements`）与宿主配额严格约束 |

---

## 2. 八阶段微内核插件生命周期 (8-Stage Microkernel Lifecycle SPI)

我们将原有的五阶段 `GatewayPlugin` SPI 精细化扩展为与三阶段 Redis Lua 账本（`ADMIT` $\rightarrow$ `DISPATCH` $\rightarrow$ `SETTLE`）严密咬合的**八阶段微内核生命周期**，覆盖非流式与流式（SSE）全场景：

```
[Inbound HTTP / JSON-RPC Request]
       │
       ▼
┌──────────────────────────────────────────────────────────┐
│ Stage 1: ON_AUTH (鉴权与租户身份解析)                     │
│  • 内置/自定义插件从 Bearer / Header / Query / Cookie /   │
│    HMAC 签名 / JWT 提取凭证并绑定 KeyContext              │
└──────────────────────────┬───────────────────────────────┘
                           │ (Rejected -> 401/403, 0 Lua Cost)
                           ▼
┌──────────────────────────────────────────────────────────┐
│ Stage 2: ON_RATE_LIMIT (多维前置限流与防烧钱熔断)         │
│  • 终端用户 (user/IP/Tenant) RPM/TPM 限流                 │
│  • 滑动窗口烧钱速率熔断 (Spend Velocity) & 单请求成本上限 │
└──────────────────────────┬───────────────────────────────┘
                           │ (Rejected -> 429, 0 Lua Cost)
                           ▼
┌──────────────────────────────────────────────────────────┐
│ Stage 3: ON_PRE_ADMIT (体系规则、护栏、Prompt 与语义路由) │
│  • 模型白名单、能力矩阵 (Capabilities) 与参数边界合规策略 │
│  • Prompt 模板注入、长上下文滑动窗口裁剪 (Context Trimmer)│
│  • PII 脱敏 / 正则护栏 / TypeSafe Jev 校准概率护栏        │
│  • Jev 智能语义路由 / 金丝雀路由改写 ctx.setModelAlias()  │
└──────────────────────────┬───────────────────────────────┘
                           │ (Rejected -> 400/403, 0 Lua Cost)
                           ▼
             ╔═══════════════════════════╗
             ║  Redis Lua Phase 1: ADMIT ║ (原子预扣项目/Key 预算、并发租约、RPM/TPM)
             ╚═════════════┬═════════════╝
                           ▼
┌──────────────────────────────────────────────────────────┐
│ Stage 4: ON_POST_ADMIT_PRE_DISPATCH (精确与语义缓存短路)  │
│  • ExactCachePlugin / SemanticCachePlugin 检索            │
│  • 若命中 (ShortCircuit)：支持非流式 JSON 或流式 SSE 回放 │
│    -> 直接进入 SETTLE(actualMicros=0) 全额退还预扣预算！  │
└──────────────────────────┬───────────────────────────────┘
                           ▼
┌──────────────────────────────────────────────────────────┐
│ Stage 5: ON_ROUTE_SELECT (可插拔目标路由与负载均衡排序)   │
│  • 插件链对 List<RouteTarget> 进行过滤与重排序：          │
│    CircuitBreaker -> PrefixCacheAffinity -> LeastTTFT -> │
│    WeightedRoundRobin                                    │
└──────────────────────────┬───────────────────────────────┘
                           ▼
            ╔══════════════════════════════╗
            ║  Redis Lua Phase 2: DISPATCH ║ (状态推进至 dispatched + 周期性 HEARTBEAT)
            ╚══════════════┬═══════════════╝
                           ▼
┌──────────────────────────────────────────────────────────┐
│ Stage 6: ON_PRE_UPSTREAM (目标级协议转换与上游请求注入)   │
│  • OpenAI / Anthropic / Gemini / Cohere / Jina /         │
│    TypeSafe Jev (/v1/systemone) 协议载荷生成与多 Key 轮转 │
└──────────────────────────┬───────────────────────────────┘
                           ▼
┌──────────────────────────────────────────────────────────┐
│ Stage 7a: ON_STREAM_CHUNK (流式 SSE 逐块滑动窗口治理)     │
│  • 流式输出实时 PII 脱敏、违规流式熔断、首字延迟 TTFT 采集│
│ Stage 7b: ON_RESPONSE_CONTENT (完整响应治理与缓存回填)    │
│  • 非流式响应脱敏 + 流式结束后完整内容自动回填 ExactCache │
└──────────────────────────┬───────────────────────────────┘
                           ▼
             ╔════════════════════════════╗
             ║  Redis Lua Phase 3: SETTLE ║ (按真实 Token 与缓存/推理单价多退少补结算)
             ╚═════════════┬══════════════╝
                           ▼
┌──────────────────────────────────────────────────────────┐
│ Stage 8: ON_POST_SETTLE (FinOps 标签归属与账单 Sink 扇出) │
│  • CostAttributionTagPlugin 注入部门/成本中心/Agent 标签  │
│  • BillingWebhookSink / RedisStreamSink / OTLP 异步导出   │
└──────────────────────────────────────────────────────────┘
```

### 2.1 `GatewayPlugin` SPI 扩展接口定义

在 [`gateway-common`](https://github.com/jihuayu/ai-gateway/tree/main/gateway-common) 的 `com.jihuayu.aigateway.plugin.GatewayPlugin` 中扩展向后兼容的默认钩子方法：

```java
public interface GatewayPlugin {
    String id();
    default String name() { return id(); }
    default String type() { return "JAVA_SPI"; }
    default int priority() { return 100; }

    // Stage 1: 鉴权与身份解析（可通过 ctx.bindAuthenticatedKey(KeyContext) 绑定身份）
    default PluginResult onAuthenticate(PluginExecutionContext ctx) {
        return PluginResult.Continue.INSTANCE;
    }

    // Stage 1b: 请求接收通用钩子
    default PluginResult onRequestReceived(PluginExecutionContext ctx) {
        return PluginResult.Continue.INSTANCE;
    }

    // Stage 2: 多维限流与防烧钱速率熔断
    default PluginResult onRateLimit(PluginExecutionContext ctx) {
        return PluginResult.Continue.INSTANCE;
    }

    // Stage 3: 准入前体系规则、护栏、Prompt 治理与模型别名路由
    default PluginResult onPreAdmit(PluginExecutionContext ctx) {
        return PluginResult.Continue.INSTANCE;
    }

    // Stage 4: 准入后、派发前缓存短路（支持流式 SSE 回放与 0 micro-USD 结算）
    default PluginResult onPostAdmitPreDispatch(PluginExecutionContext ctx) {
        return PluginResult.Continue.INSTANCE;
    }

    // Stage 5: 上游目标负载均衡与排序（可通过 ctx.setOrderedTargets(...) 重排目标）
    default PluginResult onRouteSelect(PluginExecutionContext ctx) {
        return PluginResult.Continue.INSTANCE;
    }

    // Stage 6: 向上游目标发起 HTTP 请求前最后一次改写
    default PluginResult onPreUpstream(PluginExecutionContext ctx) {
        return PluginResult.Continue.INSTANCE;
    }

    // Stage 7a: 流式 SSE 逐块拦截与转换（返回修改后的 chunk 文本或触发熔断）
    default PluginResult onStreamChunk(PluginExecutionContext ctx) {
        return PluginResult.Continue.INSTANCE;
    }

    // Stage 7b: 非流式响应体或流式拼合完成后的响应治理与缓存回填
    default PluginResult onResponseContent(PluginExecutionContext ctx) {
        return PluginResult.Continue.INSTANCE;
    }

    // Stage 8: Redis Lua 结算完成后的账单 Sink 导出与审计
    default void onPostSettle(PluginExecutionContext ctx) {}
}
```

---

## 3. 受控有状态 JS 沙箱原语升级 (Stateful & Controlled JS Sandbox SDK)

为了让动态加载的 `.js` 插件（无论是通过控制面 `/api/plugins` 推送到 Redis 快照，还是放置在本地 `PLUGINS_DIR/*.js` 目录热监听）既能拥有编写**自定义鉴权、子限流、签名校验、动态路由**的能力，又绝对不破坏零信任安全边界，我们在 `JsPluginSandbox` 中新增以下**受配额保护的宿主原语（Host Primitives）**：

| 沙箱内置对象 / 方法 | 底层实现与安全配额约束 | 典型应用场景 |
| :--- | :--- | :--- |
| **`ctx.kv.get(key)`**<br>**`ctx.kv.set(key, val, ttlSec)`**<br>**`ctx.kv.incr(key, delta, ttlSec)`**<br>**`ctx.kv.del(key)`** | • 自动强制添加命名空间前缀 `<ns>:plugin:kv:<pluginId>:<key>`，禁止跨插件越权读写其他 Redis Key（尤其禁止触碰 `hot:v1:*` 金融账本 Key）<br>• 单次请求最多允许 **8 次** `ctx.kv` 调用，Key 长度 $\le 128\text{B}$，Value 长度 $\le 4\text{KB}$，`ttlSec` 强制在 `1..86400` 秒之间 | • 用 15 行 JS 实现按自定义 Header / `user` 字段的滑动窗口限流<br>• 累计违规计数（如同一用户连续 3 次触发护栏自动封禁 10 分钟）<br>• A/B 测试用户粘性会话记录 |
| **`Crypto.sha256(str)`**<br>**`Crypto.hmacSha256(secret, msg)`**<br>**`Base64.encode(str)`**<br>**`Base64.decode(str)`** | • 纯内存确定性计算，基于 JDK 标准 `MessageDigest`、`Mac` 与 `Base64`<br>• 输入长度受 `MAX_BODY_BYTES` 保护，计入语句燃料消耗 | • 企业私有 AK/SK + `HMAC-SHA256` 签名鉴权插件<br>• 账单 Sink Webhook 载荷签名生成<br>• 自定义加密 Token 解密与提取 |
| **`RegExp.test(pattern, text)`**<br>**`RegExp.replace(pattern, text, repl)`** | • 使用带字符长度与回溯保护的正则匹配器，防止恶意正则表达式触发 ReDoS 阻塞虚拟线程 | • 行业专属敏感词、工号、内部代码片段的动态脱敏与阻断 |
| **`ctx.bindAuthKey(apiKeyOrHash)`**<br>**`ctx.bindProjectQuota(projectId, keyId, limits)`** | • 仅在 `ON_AUTH` 阶段可用<br>• 允许自定义鉴权插件在校验外部凭证（如 JWT 或 HMAC）通过后，将请求绑定到指定的网关 API Key 或项目虚拟配额上下文 | • 对接企业内部网关 Ticket、SSO JWT、URL Query Key 或双向签名鉴权 |
| **`ctx.targets()`**<br>**`ctx.setTargetOrder(targetIds)`** | • 在 `ON_ROUTE_SELECT` 阶段读取当前候选 `RouteTarget` 列表（含 `targetId`, `upstreamModel`, `protocol`, `inputPrice`, `outputPrice`, `weight`, `circuitOpen`, `ewmaTtftMs`）并动态重排顺序 | • 用 JS 编写自定义成本优先、时段路由、地域亲和或客户等级专属路由策略 |

---

## 4. 六大内置企业级插件族详细规格 (Built-in Enterprise Plugin Suite)

所有内置插件均实现 `GatewayPlugin` SPI，编译期直接内联至 GraalVM Native Image，并可通过控制面 `/api/plugins` 与 `/api/plugins/bindings` 按 `GLOBAL`、`WORKSPACE`、`PROJECT`、`ALIAS`、`API_KEY` 五级作用域动态开启、配置优先级与覆盖参数。

### 4.1 鉴权插件族 (Authentication Plugins — `ON_AUTH`)

当微内核收到请求时，按优先级依次执行绑定在 `ON_AUTH` 阶段的鉴权插件；一旦某个插件成功解析并绑定了 `KeyContext`，鉴权即通过；若所有鉴权插件均未绑定且返回 `Reject`，则返回 `401/403`。

1. **`BearerApiKeyAuthPlugin` (`builtin:auth-bearer-key`，默认内置开启，Priority `10`)**
   - **行为**：兼容标准 `Authorization: Bearer sk-...` 与 Anthropic `x-api-key: sk-...`，通过 `HotRuntime` 执行零 SQL Redis Hash 校验。
2. **`CustomExtractorAuthPlugin` (`builtin:auth-custom-extractor`，Priority `15`)**
   - **解决痛点**：部分老旧客户端、Webhook 回调或浏览器端 SDK 无法自定义 `Authorization` 头，需要把 Key 放在 URL Query、自定义 Header、Cookie 或 Basic Auth 中。
   - **配置项 (`config`)**：
     ```json
     {
       "headerNames": ["x-gateway-key", "x-custom-token", "api-key"],
       "queryParams": ["api_key", "key", "token"],
       "cookieNames": ["gw_api_key"],
       "allowBasicAuthPasswordAsKey": true,
       "prefixStrip": "Token "
     }
     ```
3. **`HmacSignatureAuthPlugin` (`builtin:auth-hmac-signature`，Priority `20`)**
   - **解决痛点**：金融、政企与多租户 SaaS 内部系统调用网关时，要求使用 `AccessKey + Timestamp + Nonce + HMAC-SHA256(method + path + timestamp + nonce + bodySha256)` 签名并防止重放攻击。
   - **配置项 (`config`)**：
     ```json
     {
       "accessKeyHeader": "x-ak",
       "signatureHeader": "x-signature",
       "timestampHeader": "x-timestamp",
       "nonceHeader": "x-nonce",
       "maxClockSkewSeconds": 300,
       "replayProtectionTtlSeconds": 300,
       "credentials": {
         "ak_finance_bot": {
           "secret": "sk_hmac_secret_998877",
           "mappedApiKeyPlain": "sk-proj-finance-mapped-key"
         }
       }
     }
     ```
   - **防重放机制**：利用 Redis `SETNX <ns>:auth:nonce:<ak>:<nonce> 1 EX 300` 保证同一 `nonce` 在时间窗内绝不被重放。
4. **`JwtOidcAuthPlugin` (`builtin:auth-jwt-oidc`，Priority `25`)**
   - **解决痛点**：企业希望直接透传内部 OIDC / Keycloak / Auth0 / 自签发 JWT Token，按 JWT Payload 中的 `iss`、`aud`、`sub` 或自定义 Claim（如 `gateway_project`）自动映射为网关项目配额并把 `sub` 注入为 `endUserId` 供下游限流与账单归属使用。
   - **配置项 (`config`)**：支持 `hs256Secret` 或 `rs256PublicKeyPem`、`expectedIssuer`、`expectedAudience`、`claimToApiKeyMap`、`defaultMappedApiKeyPlain`、`subjectClaim`（默认 `"sub"`，自动写入 `ctx.attributes().put("auth.end_user_id", sub)`）。

---

### 4.2 多维与防烧钱限流插件族 (Rate Limit & Spend Velocity Plugins — `ON_RATE_LIMIT`)

在进入 `hot.lua` 扣减项目总账本之前执行，被限流拦截的请求 **0 账本开销、0 并发租约占用**：

1. **`EndUserRateLimitPlugin` (`builtin:ratelimit-end-user`，Priority `30`)**
   - **解决痛点**：一个项目往往共用一个或几个后端 API Key，但后端服务着成千上万个 C 端终端用户（通过请求体 `"user"` 字段、`X-End-User-Id` Header、JWT `sub` Claim 或客户端 IP 区分）。如果不做终端用户级子限流，单个恶意用户就能刷空整个项目的 RPM/TPM。
   - **维度提取优先级**：`ctx.attributes().get("auth.end_user_id")` $\rightarrow$ Header `x-end-user-id` / `x-tenant-id` $\rightarrow$ JSON Body `"user"` $\rightarrow$ Header `x-forwarded-for` 客户端 IP。
   - **配置项 (`config`)**：
     ```json
     {
       "userRpm": 30,
       "userTpm": 20000,
       "windowSeconds": 60,
       "keyDimensions": ["header:x-end-user-id", "body:user", "header:x-forwarded-for"]
     }
     ```
2. **`SpendVelocityCircuitPlugin` (`builtin:ratelimit-spend-velocity`，Priority `35`)**
   - **解决痛点**：当某个 AI Agent 陷入死循环（例如不断重试携带 100K 上下文的请求）时，虽然没超过月度总预算（如 $1,000），但会在 3 分钟内异常烧掉几十美元。
   - **双重保护机制**：
     1. **单次请求最大预估成本护栏 (`maxSingleRequestMicros`)**：在准入前预估本次请求微美元上限，一旦超过阈值（如单次请求超过 `50,000` micro-USD = `$0.05`）立即返回 `429 single_request_cost_exceeded`；
     2. **滑动时间窗烧钱速率熔断 (`maxWindowSpendMicros` / `windowSeconds`)**：结合 `ON_POST_SETTLE` 在 Redis 累加该 Project / Key / EndUser 在最近 $W$ 秒（如 300 秒）内的真实已结算微美元总额，一旦超过速率阈值（如 5 分钟内烧钱超过 `500,000` micro-USD = `$0.50`），在 `ON_RATE_LIMIT` 阶段直接熔断并返回 `429 spend_velocity_exceeded` 及 `retry-after`。

---

### 4.3 体系规则、Prompt 治理与 MCP 策略插件族 (`ON_PRE_ADMIT`)

将原本散落在 `DataPlaneExecutor` 中的体系规则解耦为可插拔、可细粒度配置的策略插件：

1. **`ModelCapabilityPolicyPlugin` (`builtin:policy-model-capability`，Priority `40`)**
   - **行为**：统一负责 API Key 的 `allowed_models` 校验、模型路由的 `capabilities` 覆盖校验，并支持**企业参数合规边界强制锁定（Parameter Governance）**：
     - `maxAllowedTemperature`（如强制生产环境 `temperature <= 0.7`）；
     - `forbidReasoningEffortHigh`（禁止非授权 Key 使用高成本 `reasoning_effort: "high"`）；
     - `enforceMaxTokensCap`（强制覆写或限制 `max_tokens`）；
     - `disallowedTools`（禁止调用高风险工具名称列表）。
2. **`PromptDecoratorTrimmerPlugin` (`builtin:prompt-decorator-trimmer`，Priority `45`)**
   - **能力 A（System Prompt 强制装饰）**：支持 `prependSystemPrompt` / `appendSystemPrompt`，自动在 `messages`（或 Anthropic `system`）中注入企业安全守则、当前日期/租户上下文或输出格式约束。
   - **能力 B（滑动窗口上下文裁剪 `maxMessages` / `maxTotalChars`）**：当多轮对话 `messages` 条数或总字符数超出阈值时，**始终保留 `system` / `developer` 消息 + 最近 $N$ 条对话消息（并保证 `assistant(tool_calls)` 与紧随其后的 `tool` 结果成对保留、不破坏消息协议结构）**，从根源上消除长对话导致的 `context_length_exceeded` 报错与 Token 浪费。
3. **`McpToolArgumentPolicyPlugin` (`builtin:mcp-argument-policy`，Priority `48`)**
   - **行为**：针对 `POST /mcp/{server_alias}` 的 `tools/call` 请求，深入检查 `params.arguments` JSON 参数：
     - 支持按工具名配置 `blockedArgumentRegexes`（例如对 `execute_sql` 工具拦截 `(?i)\b(DROP|TRUNCATE|ALTER|GRANT)\b`，对 `read_file` 工具拦截 `(\.\./|/etc/passwd|\.env)`）及必填字段校验，违规时返回 `403 mcp_argument_policy_violation`。
4. **`JevCalibratedGuardrailPlugin` (`builtin:jev-calibrated-guardrail`，Priority `50`)**
   - **行为**：调用网关内部或上游的 [TypeSafe AI Jev (`/v1/systemone`)](https://docs.typesafe.ai/api/system-one) 端点，将用户 Prompt 作为 `state` 发送并行 `noul` 问题（如 `jailbreak`、`pii_leak`、`toxic_content`）：
     - 若任一风险维度的 `noul` 校准概率 $\ge \text{blockThreshold}$（默认 `0.85`），直接返回 `400 jev_guardrail_blocked`（0 主模型预算消耗）；
     - 若在 `[\text{flagThreshold}, \text{blockThreshold})` 之间（如 `0.50..0.85`），自动打上 `x-gateway-jev-guardrail: FLAGGED` 与风险概率明细供审计。

---

### 4.4 流式缓存回放与流式逐块护栏升级 (`ON_POST_ADMIT_PRE_DISPATCH` & `ON_STREAM_CHUNK`)

1. **`ExactCachePlugin` 与 `SemanticCachePlugin` 流式 SSE 全面支持**：
   - **流式命中回放（Stream Cache Replay）**：当客户端请求 `stream: true` 且在 `ON_POST_ADMIT_PRE_DISPATCH` 命中精确缓存或语义缓存时，不再跳过缓存，而是直接返回 `ShortCircuit`，由数据面将缓存的响应文本包装为标准 OpenAI SSE 帧（或 Anthropic `message_start` $\rightarrow$ `content_block_delta` $\rightarrow$ `message_stop` 帧）流式回放给客户端，附上 `x-gateway-cache: HIT`，并以 **`0` micro-USD** 结算！
   - **流式完成后自动回填（Stream Cache Population）**：当 `stream: true` 请求未命中缓存并正常流式输出完毕（收到 `[DONE]`）后，数据面自动将流式期间拼合的完整文本构造为标准 JSON 响应体并触发 `ON_RESPONSE_CONTENT`，使流式与非流式请求**无缝共享同一套 Redis + L1 缓存池**。
2. **`PiiGuardrailPlugin` 流式逐块脱敏 (`onStreamChunk`)**：
   - 在流式 SSE 生成过程中，对每个 `delta.content` 块执行实时 PII 脱敏（`MASK` 模式）或违规词检测（`BLOCK` 模式下立即输出错误帧并中止上游流）。

---

### 4.5 可插拔路由与负载均衡插件族 (Pluggable Routing Plugins — `ON_PRE_ADMIT` & `ON_ROUTE_SELECT`)

将 `DataPlaneExecutor.selectOrderedTargets` 改造为默认兜底策略，同时允许在 `ON_PRE_ADMIT`（模型别名级路由）和 `ON_ROUTE_SELECT`（上游 Target 实例级负载均衡）挂载路由插件链：

| 路由插件 ID | 作用阶段 | 核心路由算法与适用场景 |
| :--- | :--- | :--- |
| **`builtin:router-weighted-cb`**<br>(`WeightedCircuitBreakerRouterPlugin`) | `ON_ROUTE_SELECT` | **加权轮询 + 滑动窗口熔断器**：过滤掉处于熔断开启期（Circuit Open）的故障节点，并按 `weight (0..10000)` 低差异序列平滑分配主备 Target 顺序 |
| **`builtin:router-least-ttft`**<br>(`LeastTtftRouterPlugin`) | `ON_ROUTE_SELECT` | **最低首字延迟（EWMA TTFT）优先路由**：实时跟踪每个 `targetId` 的指数移动平均首字延迟（`ewmaTtftMs`）与成功率，自动将延迟最低的健康 Target 排在首位，当主供应商拥堵时毫秒级无感切流至更快节点 |
| **`builtin:router-prefix-cache-affinity`**<br>(`PrefixCacheAffinityRouterPlugin`) | `ON_ROUTE_SELECT` | **Prompt Cache 前缀亲和性粘性路由**：提取请求中的 `system` 提示词 + 前 $K$ 条消息（或 `X-Session-Id` / `user`）计算一致性哈希（Consistent Hash），将具有相同长前缀的请求稳定路由到同一个健康的 `RouteTarget`，大幅提升 OpenAI / Anthropic / DeepSeek 上游 Prompt Cache（`cached_tokens`）命中率，直接节省 50%~90% 输入成本 |
| **`builtin:router-jev-semantic`**<br>(`JevSemanticRouterPlugin`) | `ON_PRE_ADMIT` | **基于 TypeSafe Jev 的智能分级路由**：通过 [TypeSafe Jev `/v1/systemone`](https://docs.typesafe.ai/api/system-one) 的 `choice` + `score` 评估任务复杂度（如 `simple_qa` vs `complex_reasoning` vs `code_generation`），当置信度达标时自动调用 `ctx.setModelAlias(...)` 将简单请求改写至低成本 Flash/Mini 别名，复杂请求改写至旗舰别名 |
| **`builtin:router-canary-header`**<br>(`CanaryHeaderRouterPlugin`) | `ON_PRE_ADMIT` | **金丝雀发布与 Header/百分比分流**：按 `X-Canary: true`、特定租户 ID 或配置的流量百分比（如 `10%`）将请求从基线模型别名灰度分流至新模型别名，并注入 `x-gateway-canary-route` 响应头 |

---

### 4.6 账单 Sink 与 FinOps 成本归属插件族 (Billing Sinks & FinOps — `ON_POST_SETTLE`)

当请求在 Redis Lua 中完成 `SETTLE` 后，微内核构造不可变的终态账单事件 `SettledBillingEvent`（包含 `requestId`, `projectId`, `workspaceId`, `keyId`, `endUserId`, `endpoint`, `modelAlias`, `upstreamModel`, `protocol`, `status`, `reserveMicros`, `actualMicros`, `savedCacheMicros`, `inputTokens`, `outputTokens`, `cachedInputTokens`, `reasoningTokens`, `ttftMs`, `latencyMs`, `attributionTags`, `timestamp`）并传入 `ON_POST_SETTLE` 插件链：

1. **`CostAttributionTagPlugin` (`builtin:billing-cost-attribution`，Priority `10`)**
   - **行为**：在 `ON_PRE_ADMIT` 与 `ON_POST_SETTLE` 阶段，从配置的请求 Header（默认 `x-cost-center`、`x-department`、`x-agent-id`、`x-workflow-id`、`x-env`）、JWT Claims 或请求体 `metadata` 中提取 FinOps 成本归属标签，写入 `ctx.attributionTags()` 与响应头 `x-gateway-cost-micros` / `x-gateway-cost-center`，并持久化至账单明细。
2. **`WebhookBillingSinkPlugin` (`builtin:billing-webhook-sink`，Priority `50`)**
   - **行为**：将终态微美元账单事件实时（或按微批 `batchSize`）通过 HTTP POST 推送到外部计费中心（如企业 ERP、Stripe Billing Meter Event、OpenMeter、Lago 或内部财务系统）。
   - **可靠性与安全性**：
     - 自动使用配置的 `signingSecret` 计算 `X-Gateway-Billing-Signature: sha256=<HMAC-SHA256>`；
     - 支持内存 + Redis 备份死信队列（`<ns>:billing:sink:dlq`）与指数退避重试，绝不阻塞数据面主线程，也绝不丢失账单。
3. **`RedisStreamBillingSinkPlugin` (`builtin:billing-redis-stream-sink`，Priority `60`)**
   - **行为**：将包含完整 FinOps `attributionTags`、Token 分类明细与 `ttftMs` 的标准化 JSON 账单事件实时 `XADD` 到用户指定的外部 Redis Stream（如 `gateway:finops:billing_stream`，支持 `MAXLEN ~ 100000`），方便大数据团队用 Flink / Kafka Connect / ClickHouse 直接消费。

---

## 5. 数据库 Schema 与热快照演进 (`V12__p3_microkernel_plugins.sql`)

为支撑绑定级灰度采样、FinOps 成本归属标签落盘与账单 Sink 审计，新增 Flyway 迁移 `V12__p3_microkernel_plugins.sql`：

```sql
-- 1. 插件绑定支持按采样率 (0.0 .. 1.0) 灰度放量
ALTER TABLE plugin_bindings
    ADD COLUMN IF NOT EXISTS sample_rate DOUBLE PRECISION NOT NULL DEFAULT 1.0
    CHECK (sample_rate >= 0.0 AND sample_rate <= 1.0);

-- 2. 请求表与账本表新增 FinOps 成本归属标签 (cost_center, department, agent_id, end_user_id 等)
ALTER TABLE requests
    ADD COLUMN IF NOT EXISTS attribution_tags JSONB NOT NULL DEFAULT '{}'::jsonb;

ALTER TABLE ledger
    ADD COLUMN IF NOT EXISTS attribution_tags JSONB NOT NULL DEFAULT '{}'::jsonb;
```

---

## 6. 分批实施与 PR 交付路线图 (Implementation Roadmap)

为了保持高代码质量与 100% 集成测试覆盖率，建议将本设计文档拆分为以下 **3 个子 Issues + 1 个父目标 Issue**，依次实现并通过 PR 合并至 `main`：

1. **Batch A — 微内核八阶段管线重构、受控有状态 JS 沙箱 SDK (`ctx.kv` / `Crypto` / `RegExp`) 与奇异鉴权/多维限流插件族**：
   - 升级 `GatewayPlugin` 为八阶段 SPI（新增 `ON_AUTH`、`ON_RATE_LIMIT`、`ON_ROUTE_SELECT`、`ON_STREAM_CHUNK`），支持 `plugin_bindings.sample_rate` 灰度采样；
   - 扩展 `JsPluginSandbox` 支持命名空间隔离的 `ctx.kv`（Redis 受限 KV）、`Crypto`（`sha256`、`hmacSha256`）、`Base64`、`RegExp`、`ctx.bindAuthKey`、`ctx.setTargetOrder`；
   - 实现 `CustomExtractorAuthPlugin`、`HmacSignatureAuthPlugin`、`JwtOidcAuthPlugin`、`EndUserRateLimitPlugin`、`SpendVelocityCircuitPlugin`。
2. **Batch B — 可插拔路由插件链、体系规则/Prompt 治理与流式 SSE 缓存回放/逐块护栏**：
   - 将路由选择微内核化，实现 `WeightedCircuitBreakerRouterPlugin`、`LeastTtftRouterPlugin`、`PrefixCacheAffinityRouterPlugin`、`JevSemanticRouterPlugin`、`CanaryHeaderRouterPlugin`；
   - 实现 `ModelCapabilityPolicyPlugin`、`PromptDecoratorTrimmerPlugin`、`McpToolArgumentPolicyPlugin`、`JevCalibratedGuardrailPlugin`；
   - 升级 `ExactCachePlugin` / `SemanticCachePlugin` 支持 `stream: true` SSE 流式缓存回放与流式完成后自动回填，升级 `PiiGuardrailPlugin` 支持 `onStreamChunk` 流式实时脱敏。
3. **Batch C — 账单 Sink 插件族（Webhook / Redis Stream / FinOps 成本归属标签）与全链路微内核集成测试**：
   - 新增 `V12__p3_microkernel_plugins.sql` 迁移，支持 `attribution_tags` 落盘与 `/api/requests` 查询；
   - 实现 `CostAttributionTagPlugin`、`WebhookBillingSinkPlugin`（带 HMAC 签名与死信队列）、`RedisStreamBillingSinkPlugin`，并在 Prometheus `/metrics` 中新增 `ai_gateway_cache_saved_micros_total` 与 `ai_gateway_billing_sink_events_total` 指标；
   - 编写覆盖全部微内核八阶段与六大内置插件族的端到端集成测试。
