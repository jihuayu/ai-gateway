# AI Gateway 企业级微内核管理平台总体架构与演进设计蓝图

> **关联版本**：后端内核 `v0.4.2`（P2 多模态路由 + P3 8 阶段微内核插件套件 + P4 分布式高可用/Peak-EWMA 对冲/可插拔可观测性） · 前端控制台 `gateway-console v0.4.2`

---

## 1. 背景与核心问题诊断

随着 P2、P3、P4 三个大阶段架构升级落地，`ai-gateway` 后端已经从早期的“OpenAI 代理 + 简单双节点主备切换 + 微美元预留账本（v0.2.0）”，演进为具备 **6 大跨协议多模态转换、8 阶段同构微内核插件流水线（22 个内置企业级插件 + GraalJS 确定性沙箱 + Java SPI 热加载）、MCP 工具代理网关、Caffeine W-TinyLFU L1/L2 近端缓存、MPSC 批量组提交、分布式三级 HA 状态机（本地 WAL 容灾）、Peak-EWMA 尾延迟自适应对冲** 以及 **同构 `ctx.telemetry` 可观测体系** 的企业级 AI 基础设施内核。

相比之下，当前 `gateway-console` 虽然已在 `v0.4.2` 补齐了 `/api/telemetry/runtime` 内核健康探针、`/api/telemetry/access-logs` 慢请求日志、`/api/telemetry/spans` 追踪查看与 15 个内置插件配置模板，但其整体信息架构仍沿用 `v0.2.0` 的单层扁平列表模式，存在 **四大结构性短板**：

1. **后端能力暴露率不足（前后端代差）**：约 **40% 已上线且通过集成测试的后端 API 与数据库字段** 尚未在前端暴露（例如 **MCP Server 管理全缺失**、**新建项目入口缺失**、API Key 的 `key_tpm` / `budget_micros` / `allowed_mcp_tools` 缺失、供应商多凭据池 `secrets[]` 缺失、模型目标 `cached_input_price` / `cache_write_price` / `reasoning_price` 与在线编辑缺失、路由版本 N 节点权重配置缺失）。
2. **工作空间全局域与项目域混杂**：左侧导航将工作空间级基础设施（供应商、全局插件、内核健康）与项目级资源（模型别名、API Keys、请求日志、项目账单）平铺在一起，顶栏的“当前项目切换器”在全局页面处于无效悬空状态，多团队协作时容易造成认知混淆。
3. **插件编排缺乏“有效执行链”可视化**：微内核支持 `GLOBAL → WORKSPACE → PROJECT → ALIAS → API_KEY` 五级作用域叠加与 `priority` 排序，但目前仅以两张独立平铺表展示 `plugins` 与 `plugin_bindings`，运维与研发无法直观看到“某个请求进入特定模型别名时，在 8 个阶段实际按什么顺序执行了哪些插件、合并后的最终参数是什么”。
4. **可观测与排障链路割裂**：请求元数据表（PostgreSQL `requests` / `attempts`）、慢请求访问日志（`StructuredAccessLogPlugin` 环形缓冲）、OpenTelemetry `gen_ai.*` Span 与微内核插件级耗时轨迹（`traceLog`）分散在不同视图，且多副本部署下内存环形缓冲仅反映单节点局部视图。

---

## 2. 目标管理平台产品愿景与信息架构（Console v1.0 蓝图）

### 2.1 四类核心用户画像（Personas）与核心诉求

| 角色画像 | 核心职责 | 高频工作流 |
|---|---|---|
| **平台与 SRE 工程师（Platform / SRE）** | 保障网关高可用、低尾延迟与上游连接安全 | 管理供应商多 Key 凭据池、监控 Caffeine/MPSC/HA/Hedge 内核探针、处置上游熔断与本地 WAL 回放、挂载全局鉴权与可观测插件 |
| **AI / Agent 应用架构师（AI Developer）** | 模型路由编排、MCP 工具接入与策略调优 | 发布加权/灰度/Peak-EWMA/Jev 语义路由版本、注册 MCP Server 并调试 Tool Schema、在沙箱 IDE 中编写和 Dry-Run 调试 JS 插件、多模态 Playground 联调 |
| **安全与合规审计官（SecOps / Compliance）** | 隐私脱敏、提示词防护、最小权限与审计追溯 | 配置 PII 流式脱敏与 Jev 校准安全护栏、审计 API Key 的模型与 MCP 工具白名单（ACL）、执行零停机密钥轮换、查阅控制面与数据面审计轨迹 |
| **FinOps 与研发负责人（FinOps / EM）** | 预算管控、多维成本分摊与财务对账 | 创建多业务项目、设定项目/Key/终端用户三级微美元预算与消费速率熔断、按部门/成本中心/Agent 拆解账单、管理计费 Webhook 与死信重放 |

### 2.2 双层作用域 × 六大业务中心信息架构

将控制台明确划分为 **「项目工作台（Project Scope，受顶栏项目切换器驱动）」** 与 **「平台基础设施域（Workspace & Cluster Scope，全局管理员/SRE 视图）」** 两层导航，并提供 `Cmd/Ctrl + K` 全局命令面板：

```text
AI Gateway 管理平台 (Console v1.0)
├── [顶栏全局控制] 工作空间标识 · 项目快速切换器 (+ 新建项目) · 集群 HA 状态胶囊 · 全局搜索 (Cmd+K)
│
├── 【一、项目工作台 Project Scope】
│   ├── 1. 项目指挥舱 (Overview & Live Pulse)
│   │      ├── 实时吞吐 RPS / 成功率 / P50·P95·P99 延迟与 TTFT 双轴趋势图
│   │      ├── ExactCache / SemanticCache 命中节省金额（Saved USD）与对冲抢答胜出统计
│   │      └── 项目配额水位（月度预算、RPM、TPM、并发租约实时占用）
│   ├── 2. 智能路由与别名编排台 (Routing Studio & Aliases)
│   │      ├── N 目标加权负载均衡滑块 + 主备故障转移链编排（解除硬编码 2 目标上限）
│   │      ├── 路由策略一键挂载（Peak-EWMA 自适应对冲 / Prefix KV-Cache 亲和 / Canary 灰度 / Jev 语义路由）
│   │      └── 不可变路由版本 Diff 对比视图（目标、权重、价格快照、能力差异对比）与一键回滚
│   ├── 3. 密钥与多维访问控制 (API Keys & Access ACL)
│   │      ├── 全维度限额配置：单 Key RPM + 单 Key TPM + 单 Key 独立微美元预算上限 (budget_micros)
│   │      ├── 双白名单矩阵：允许模型别名 (allowed_models) × 允许 MCP 工具集 (allowed_mcp_tools)
│   │      └── 零停机密钥轮换（一键签发同权限继任密钥 + 宽限期自动吊销旧密钥）
│   ├── 4. 多模态与 MCP 交互调试台 (Unified Playground)
│   │      ├── 6 大数据面端点实测：Chat (SSE 流式实时 TTFT 计时)、Anthropic Messages、Embeddings、Rerank、Images (实时预览)、Jev System One (noul/choice/score 置信度可视化)
│   │      └── MCP JSON-RPC 在线工具探测（tools/list 拉取与 tools/call 模拟调用）
│   └── 5. 全链路请求追踪与 FinOps 账本 (Traces, Logs & FinOps)
│          ├── 请求级全链路瀑布图（Attempt 主备/对冲并发时间线 + 8 阶段插件耗时 + OTel Span 事件）
│          ├── 多维成本透视（按模型别名 / API Key / 部门归因标签 / Cached Input / Reasoning Token 拆解）
│          └── 小时级对账单导出与计费死信（DLQ）核查
│
└── 【二、平台基础设施域 Workspace & Platform Scope】
    ├── 6. 多模态供应商与凭据池 (Providers & Target Catalog)
    │      ├── 7 种协议连接管理 + 多 API Key 加密凭据池（轮询/健康剔除/单 Key 连通性拨测）
    │      └── 模型目标全生命周期管理（含缓存读写单价、推理 Token 单价、能力矩阵与在线编辑）
    ├── 7. 微内核流水线编排与插件工坊 (8-Stage Pipeline & Plugin Studio)
    │      ├── 8 阶段可视化泳道图 + 有效执行链模拟器（输入 Project/Alias/Key 预览合并后生效插件链）
    │      ├── 22 个内置插件 Schema 表单化配置器（替代纯手写 JSON）+ 在线更新插件版本（PATCH）
    │      ├── GraalJS 在线 IDE（语法高亮、ctx.kv/crypto/telemetry 补全、AST 燃料表、前后报文 Diff Dry-Run）
    │      └── JVM 动态 SPI JAR 热加载管理面板
    ├── 8. MCP 工具网关中心 (MCP Server Registry)
    │      ├── Streamable HTTP / SSE 远端 MCP Server 注册、加密鉴权与超时治理
    │      └── 工具清单自动发现、敏感参数安全策略绑定（policy-mcp-tool-args）与调用成功率监控
    └── 9. 内核健康、集群高可用与安全审计 (Kernel HA, Telemetry & Audit)
           ├── 多节点集群探针聚合（Caffeine L1/L2、SingleFlight、MPSC 管道、16 分片工作窃取队列、HA 状态机）
           ├── 熔断器手动复位、本地降级 WAL 待回放监控与慢请求/异常结构化日志中心
           └── 项目生命周期管理、成员 RBAC 矩阵、数据保留策略与控制面变更审计 Diff
```

---

## 3. 前后端现状差距全景盘点（Backend v0.4.2 vs. Console v0.4.2）

### 3.1 A 类差距：后端 API 与数据表已完备，前端尚未暴露或能力残缺（纯前端改造即可闭环）

| 功能模块 | 后端已具备的接口 / 字段能力 | 当前 `gateway-console` 现状 | 影响评估 |
|---|---|---|---|
| **MCP 工具网关** | `McpController`: `GET / POST / PATCH / DELETE /api/mcp-servers`（支持 `streamable_http` / `sse`、AES-GCM `secret`、`allowedTools`、`timeoutMs`）及 `/mcp/{alias}` 代理 | **完全缺失**（无导航页、无表单、无列表） | 用户无法通过界面管理任何 MCP Server，只能手写 `curl` 调用控制面 API |
| **多项目管理** | `IdentityController`: `POST /api/projects`、`GET /api/projects/{id}` | 仅有顶栏切换下拉框与当前项目重命名，**无“新建项目”按钮与弹窗** | 管理员无法在界面创建第二个业务项目做多团队隔离 |
| **API Key 高级配额与 MCP ACL** | `CatalogController`: `api_keys` 支持 `key_tpm`（每分钟 Token 上限）、`budget_micros`（单 Key 预算硬上限）、`allowed_mcp_tools`（`server:*` 或 `server:tool`） | `KeysView.tsx` 仅支持 `key_rpm` 与 `allowed_models`，未展示和提交上述 3 个字段 | 无法在界面为单把密钥设置 Token 速率、独立预算或 MCP 工具权限 |
| **项目级 TPM 限流** | `IdentityController`: `projects` 支持 `tpm` 更新与下发 | `SettingsView.tsx` 仅支持 `rpm`、`concurrency`、`max_output_tokens` | 大上下文调用无法从界面配置项目级每分钟 Token 限额 |
| **供应商删除与多 Key 凭据池** | `ProvidersController`: `DELETE /api/providers/{id}`（含引用安全检查）以及 `secrets: string[]` 多密钥轮询池 | `ProvidersView.tsx` 无删除按钮，且密钥输入框仅支持单个字符串 `secret` | 废弃供应商无法清理；无法在界面配置多 Key 池突破上游单账号 RPM 瓶颈 |
| **模型目标 5 维定价与在线编辑** | `ProvidersController`: `PATCH /api/targets/{id}`，以及 `cached_input_price`、`cache_write_price`、`reasoning_price` | `ProvidersView.tsx` 仅支持新增目标且只有输入/输出两项单价，**无编辑/启停按钮** | 命中 Prompt Cache 或使用 o3/R1/Gemini Thinking 推理模型时无法配置差异化单价；价格变更无法修改 |
| **路由版本 N 目标与自定义权重** | `CatalogController`: `targets` 支持任意数量目标及 `{target_id, weight}` 对象数组 | `AliasesView.tsx` 硬编码限制 `selectedTargets.length <= 2` 且无权重输入 | 无法在界面配置 3+ 节点池或 90%/10% 加权金丝雀分流 |
| **微内核插件在线编辑与 JAR 热加载** | `PluginsController`: `PATCH /api/plugins/{pluginId}`、`POST /api/plugins/load-jar`，以及剩余 7 个内置插件模板 | `PluginsView.tsx` 目前支持新增与卸载，尚不支持原位编辑已有插件配置/代码及动态加载 JAR | 调整插件阈值需先卸载再重建，操作路径偏长 |
| **多模态与流式 Playground** | 数据面已支持 `/v1/chat/completions` (SSE)、`/v1/messages`、`/v1/embeddings`、`/v1/rerank`、`/v1/images/generations`、`/v1/systemone`、`/mcp/{alias}` | `GuideView.tsx` 在线测试仅调用非流式 `/api/test`（仅测普通文本问答），其余端点仅有静态 `curl` 示例 | 无法在控制台直观验证流式首字延迟（TTFT）、重排打分、文生图及 Jev 结构化决策 |

### 3.2 B 类差距：迈向生产级企业平台需要前后端协同增强的能力（需新增/扩展后端接口 + 前端开发）

| 能力域 | 当前架构局限 | 需要新增的后端改动 | 需要配套的前端改动 |
|---|---|---|---|
| **1. 有效插件链计算与绑定更新 API** | `plugin_bindings` 不支持 `PATCH`（修改采样率或 `configOverride` 需删了重建）；控制面无“按给定请求上下文预演整条 8 阶段插件链”的接口 | 1. 新增 `PATCH /api/plugins/bindings/{bindingId}`（修改 `priority`、`sampleRate`、`configOverride`、`enabled`）；<br>2. 新增 `POST /api/plugins/resolve-chain`（输入 `projectId`、`aliasName`、`keyId`，返回 8 个阶段排序后的生效插件链及合并配置）；<br>3. 扩展 `POST /api/plugins/dry-run` 支持整条流水线级联联调 | 在插件中心提供 **「8 阶段可视化泳道 + 生效链路模拟器」**，点击任意阶段节点即可原位调整优先级、采样率与覆盖配置 |
| **2. 路由历史版本列表与 Diff 接口** | `GET /api/aliases` 仅返回 `current_version` 与 `draft_version`，回滚弹窗需要用户盲填历史版本号整数 | 新增 `GET /api/aliases/{id}/versions` 返回该别名下所有历史版本列表（含版本号、发布人、发布时间、目标权重、能力与价格快照） | 升级版本管理抽屉：提供历史版本时间轴与 **左右并排 Diff 对比器**，点选任意历史版本一键安全回滚 |
| **3. 多维 FinOps 成本归因聚合查询** | `usage_hourly` 当前仅按 `(project_id, bucket)` 聚合总费用与输入/输出 Token，未在查询接口按 `model_alias`、`key_id`、`attribution_tags`、缓存/推理 Token 及“缓存节省金额”拆解 | 扩展 `HotProjector` 与 `GET /api/usage/breakdown?group_by=model\|key\|department\|cost_center`，返回细分 Token 类型（`cached_input_tokens`、`reasoning_tokens`）与缓存命中节省估算 | 升级 `UsageView` 为 **FinOps 成本透视工作台**：提供模型/密钥/部门成本占比分布、Prompt Cache 节省率卡片与预算预警规则 |
| **4. 集群级遥测汇聚与运维控制动作** | `/api/telemetry/runtime`、`spans`、`access-logs` 读取当前响应节点 JVM 内存，多副本部署时存在视角盲区；控制面无手动复位熔断器或查看计费死信接口 | 1. 各数据面节点定时（如每 5s）将 `KernelEngineSnapshot` 与节点心跳写入 Redis Hash `gw:nodes:runtime`；<br>2. 新增 `GET /api/cluster/nodes`、`POST /api/cluster/circuit-breakers/reset`；<br>3. 新增 `GET /api/billing/dlq` 与 `POST /api/billing/dlq/{id}/replay` | 在 `TelemetryView` 增加 **多实例节点拓扑矩阵**、单节点/集群聚合视图切换、熔断器一键复位按钮，以及 FinOps 死信重放面板 |
| **5. MCP 工具自动探测与连通测试** | 注册 MCP Server 后，控制面没有像供应商 `/test` 那样的探测接口来预拉取远端 `tools/list` Schema | 新增 `POST /api/mcp-servers/{id}/probe`（使用服务端加密存储的凭据向远端 MCP Server 发起 `initialize` + `tools/list`，返回延迟、工具名、描述与 JSON Schema） | 在 MCP 管理页与 API Key 授权弹窗中提供 **工具复选树（Checkbox Tree）**，无需手写 `server:tool` 字符串 |
| **6. 慢日志与 Trace 可选持久化检索** | 内存环形缓冲区最多保留最近 256 条 Span 与慢请求日志，重启清空且不支持按时间窗/项目分页检索 | 可选异步批量写入 PostgreSQL 分区表 `telemetry_spans` / `slow_access_logs`（复用现有 `retention_days` 自动清理任务），扩展查询过滤参数 | 支持按 `project_id`、`trace_id`、`model`、错误码与时间区间跨天检索慢请求与全链路瀑布图 |

---

## 4. 分阶段改造路线图与具体改动清单

为确保每次迭代均可独立交付、零回归合并，建议按 **三个递进迭代（P5-A → P5-B → P5-C）** 推进改造：

### Phase P5-A：控制面既有能力 100% 前端闭环（纯前端改造，最快见效）

- **改动目标**：消除所有“后端已有接口但前端无入口”的断点，重构双层作用域导航。
- **涉及文件与具体改动**：
  1. **`gateway-console/src/App.tsx` & `src/styles/ui.ts`**：
     - 将侧边栏按 `项目工作台（Project）` 与 `平台基础设施（Workspace）` 分组渲染，在非项目作用域视图隐藏或置灰顶栏项目下拉框以避免误导；
     - 在顶栏项目选择器旁新增 **「+ 新建项目」** 对话框（对接 `POST /api/projects`）。
  2. **新增 `gateway-console/src/views/McpViews.tsx`**：
     - 完整对接 `GET / POST / PATCH / DELETE /api/mcp-servers`，支持 `streamable_http` 与 `sse` 传输协议、AES-GCM 密钥轮换、超时配置与允许工具列表维护。
  3. **升级 `gateway-console/src/views/CatalogViews.tsx`**：
     - **`ProvidersView`**：新增供应商删除确认（`DELETE /api/providers/{id}`）；支持切换“单密钥 / 多密钥轮询池（`secrets[]`）”输入；为模型目标表增加 **「编辑 / 启停」** 弹窗（对接 `PATCH /api/targets/{id}`），并补齐 `cached_input_price`、`cache_write_price`、`reasoning_price` 三项微美元单价字段。
     - **`AliasesView`**：解除最多 2 个目标的前端硬限制，改为支持动态添加多个目标并配置整数权重 `{ target_id, weight }`，实时计算分流百分比预览。
     - **`KeysView`**：创建密钥弹窗与列表补齐 `key_tpm`、`budget_micros`（USD 输入自动转微美元）与 `allowed_mcp_tools` 选择器；增加「一键轮换密钥」快捷操作。
  4. **升级 `gateway-console/src/views/MicrokernelViews.tsx` & `OperationsViews.tsx`**：
     - **`PluginsView`**：补齐剩余全部 22 个内置插件模板；新增插件实例编辑弹窗（对接 `PATCH /api/plugins/{id}`）与 JAR 动态加载入口（对接 `POST /api/plugins/load-jar`）；为高频内置插件提供结构化表单字段（自动双向同步 JSON）。
     - **`SettingsView`**：项目配额表单补齐 `tpm`（项目级每分钟 Token 上限）字段。

### Phase P5-B：可视化流水线编排、版本 Diff 与多模态流式 Playground（前后端轻量协同）

- **后端改动（`gateway-control-plane`）**：
  - `CatalogController.java`：新增 `GET /api/aliases/{id}/versions` 返回完整历史路由版本列表；
  - `PluginsController.java`：新增 `PATCH /api/plugins/bindings/{bindingId}` 与 `POST /api/plugins/resolve-chain`（复用 `PluginPipeline.resolveStagePlugins` 计算指定上下文的 8 阶段有序执行链）；
  - `McpController.java`：新增 `POST /api/mcp-servers/{id}/probe` 在线拉取远端 MCP 工具 Schema；
  - `QueriesController.java`：升级 `/api/test`（或新增 `/api/playground/invoke`）支持 SSE 流式透传及多模态路径（`/v1/messages`、`/v1/embeddings`、`/v1/rerank`、`/v1/images/generations`、`/v1/systemone`）受控测试。
- **前端改动（`gateway-console`）**：
  - 实现 **8 阶段微内核泳道可视化组件** 与 **全链路 Dry-Run 前后报文 Diff 视图**；
  - 实现模型别名 **历史版本对比（Diff）抽屉**；
  - 将 `GuideView` 升级为真正的 **多模态流式 Playground**（实时滚动显示 SSE chunk、首字 TTFT 毫秒徽章、重排相关性得分条与文生图预览）。

### Phase P5-C：多维 FinOps 透视、集群多节点治理与全链路瀑布图（企业生产纵深）

- **后端改动（`gateway-common` / `gateway-data-plane` / `gateway-control-plane`）**：
  - `HotProjector` 扩展多维聚合表 `usage_attribution_hourly`（按 `project_id, model_alias, key_id, cost_center` 维度聚合，记录精确结算、缓存命中 Token、推理 Token 与缓存短路节省微美元）；
  - `DataPlaneExecutor` 增加节点级心跳与 `KernelEngineSnapshot` 周期性上报至 Redis，控制面暴露 `GET /api/cluster/nodes`、`POST /api/cluster/circuit-breakers/reset`、`GET /api/billing/dlq` 与 `POST /api/billing/dlq/{id}/replay`；
  - 请求追踪关联：将 `request_id` 与 `trace_id` 写入 `requests` 表索引，支持从请求详情一键拉取关联的 Attempt 对冲时间线与 OTel Span 事件树。
- **前端改动（`gateway-console`）**：
  - 在 `RequestsView` / `TelemetryView` 实现 **请求级 Gantt 瀑布图**（清晰呈现 `Stage 1~4 准入缓存` → `Stage 5 Peak-EWMA 主请求与对冲请求并发竞速` → `SSE 流式输出` → `Stage 8 异步结算与 OTel/Langfuse 导出` 的完整微秒/毫秒时间轴）；
  - 在 `UsageView` 实现多维 FinOps 报表、缓存 ROI 看板与计费 Webhook 死信重放操作台。
