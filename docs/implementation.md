# 模块实施设计与任务拆分

版本：v0.3 · 2026-11-07 · 状态：已形成实现，PR #17 待合并；实际验证与性能边界见 verification.md

## 已确定的技术约束

按用户要求，数据库使用 PostgreSQL；实时限流和计费使用 Redis；Kafka 可以引入，但不作为首版必需部署依赖。前端和服务端语言仍为提案。此文细化 [产品设计](design.md)，冲突时以本版的存储和一致性约定为准。

## Redis 准入与计费协议

所有同一项目的键使用共同 hash tag，例如 `limit:{project_id}:key_id`、`budget:{project_id}:2026-11`、`lease:{project_id}`、`reservation:{project_id}:request_id`。Lua 在同一 slot 内检查项目/key token bucket、并发租约、预算及恢复 epoch，全部通过才一起扣减。不实现跨项目原子配额。单个热点项目的 slot 吞吐需压测。

token bucket 使用 Redis TIME，配置补充速率和 burst；并发使用带截止时间的有序集合和随机 owner token。租约每 10 秒续期，建议有效期 45 秒；不能续期则取消上游，未确认终止时保留预算为待核对，不能因为租约到期就断言上游不再执行。预算预留不依赖普通 TTL 自动退款。幂等释放不会退还已消耗的请求频率 token。

费用单位为单一币种 USD 的整数微单位，价格以每百万 token 的整数微单位记录，计算使用整数/十进制定点数、向上取整。Redis Lua 整数运算必须限制在 2^53-1 安全范围，拒绝超界配置；PG 使用 bigint 并校验非负值及币种。P0 不做汇率换算。

预留额覆盖该路由所有允许尝试的最大费用，或在备用派发前原子追加预留；默认采用前者。固定请求开始时的价格版本和 UTC 月份，跨月完成仍结算到原周期。usage 实际值缺失保留保守估算；若实际费用超过预留，记入真实差额、冻结剩余额度并告警，不能丢弃费用以伪造硬预算保证。

### 状态机与跨存储顺序

1. PG 创建唯一 request_id 的 PREPARING 意图，包含价格快照、周期、最大费用与路由版本；未提交不请求上游。
2. Redis Lua 原子准入，写入 RESERVED 和 epoch。重复 request_id 返回原结果，不重复预留。
3. PG 更新为 RESERVED，再持久化 DISPATCH_INTENT 后才可发送上游。写入失败不派发；保留意图供对账。
4. 完成时先在 PG 事务中保存不可变结算事件、attempt usage、账本和 outbox，再由幂等 Lua 将 Redis 预留转换为实际费用；应用成功后记录确认。PG 成功、Redis 失败时先保留预留并停止受影响项目新准入，重试恢复。
5. 明确未派发的记录可撤销预留；派发后无结果的记录进入 UNKNOWN，按保守金额占用，直到证据充分的对账。更正使用补偿账目，不修改历史原始事件。
6. PG 不可用时拒绝新调用；已有流尽力完成，但结算不能持久化时保留预留和未完成意图。不能保证取回的 usage 标为未知。

Redis 和 PG 不存在跨库原子事务；以上以持久化意图、幂等状态转换及保守预留处理中间失败。Redis AOF/副本提升耐久性，但不能作为零丢失保证。Redis 重启、主从切换和 epoch 不一致时，部署协调器先关闭准入并隔离旧主，提升 PG 中的 generation；由单个恢复任务锁定项目，从 PG 账本与未终结意图重建 Redis，再开放准入。每个 Lua 操作携带 generation，旧 worker 拒绝写入。必须演练网络分区与 fencing；不能用简单的“重连成功”作为恢复完成信号。

## PostgreSQL 物理设计约束

- 核心实体使用 UUID 主键，关系表含 workspace_id/project_id；对跨实体关系使用带项目或工作空间范围的复合唯一约束和外键。
- alias 唯一键 (project_id, name)，route_version 唯一键 (alias_id, version)，ledger 唯一 event_id，attempt 唯一 (request_id, attempt_no)，inbox 唯一 (consumer_name, event_id)。
- requests 索引 (project_id, created_at DESC, id DESC)，attempts 索引 request_id，ledger 索引 (project_id, period, created_at)，outbox 为待投递状态建立局部索引；索引外键引用列，按真实查询计划再增加模型/key 维度索引。
- 时间统一 timestamptz；筛选使用半开区间；游标同时含时间与 ID。财务计算不使用浮点。
- outbox 抢占使用短事务与 SKIP LOCKED/租约，不在持有行锁时等待外部网络。连接池大小总和预留管理和迁移连接，设置 statement_timeout。
- 配置采用版本并发控制；账本与 outbox 同事务。迁移采用扩展、回填、切换、清理顺序；大索引考虑 CONCURRENTLY，提供失败恢复步骤。
- P0 不提前分区小表；根据请求量和保留周期评估分区。请求元数据 30 天、聚合建议 12 个月，账本与去重标记保留期必须覆盖可重放窗口，不能随请求日志一起删除。

## Kafka 可选事件架构

默认 PG outbox worker 直接处理，启用 Kafka 后由 publisher 发布至 gateway.usage.v1、gateway.request.v1、gateway.audit.v1；不双开两个执行相同副作用的消费路径。项目 ID 为分区键，只承诺分区内顺序；消费者仍按实体版本处理迟到事件。

事件 envelope 包含 event_id、schema_version、occurred_at、workspace_id、project_id、request_id、attempt_id、route_version、price_version、generation 和无正文的 payload。兼容性采用新增可选字段，破坏变更新 topic 版本。

生产端采用 acks=all、幂等 producer；生产环境建议 3 副本、min.insync.replicas=2，开发单节点不得宣称同等可靠性。PG outbox 在发布确认后标记成功；崩溃可能重复投递。消费者在一个 PG 事务内写 inbox 与聚合结果，提交后再提交 offset。去重不是依赖 Kafka 的“恰好一次”宣传。

失败指数退避并限制次数，进入 DLQ 后保留原 event_id、失败原因和重放记录。默认建议事件保留 7 天，DLQ 30 天，均需容量测算；去重记录至少覆盖所有获准重放窗口，过期事件重放须先执行独立对账。消费积压 >60 秒告警；outbox 达到可用容量预警线时限制新准入，禁止静默丢计费事件。Kafka 中断时 PG 可持久化的现有链路继续，不能要求 Kafka 充当实时余额真相。

## 页面实现契约

所有页面包含加载、空态、权限不足、失败重试四态；表单有脏状态提示、字段错误和重复提交保护。

| 页面 | API 与主要交互 | 重点验收 |
| --- | --- | --- |
| 概览 | GET /api/usage/summary，项目/时间切换、三步接入 | 无请求显示空态，显示数据水位 |
| 供应商 | GET/POST/PATCH /api/providers，POST /:id/test | 密钥不可读回，测试费用提示，安全错误映射 |
| 模型路由 | /api/model-aliases，/:id/versions，/:id/publish | 发布差异、能力校验、409 冲突恢复 |
| API Keys | /api/keys，/:id/revoke | 一次性展示、范围、到期、轮换向导 |
| 请求日志 | GET /api/requests 与 /:id | 游标分页、筛选入 URL、attempt 时间线 |
| 用量 | GET /api/usage，PATCH /api/projects/:id/budget | 预留/已结算/待核对分别呈现，未知不画成零 |
| 接入指南 | GET /v1/models，受控测试接口 | 测试关联操作者且经过统一授权、限流计费 |
| 设置 | /api/projects、/api/members、/api/audit | 权限矩阵、预算变更审计、保留策略说明 |

POST 创建配置支持管理面幂等键；语义仅保证同一创建操作不重复生成资源。聊天请求不承诺供应商端幂等执行。

## 模块任务

依赖按从基础到闭环排序；P1 不阻塞 P0。每项实现 PR 应提供变更、测试证据和运行限制。

<a id="module-1"></a>

### 1. 基础工程与运行环境

优先级：P0。依赖模块：无。

建立控制面、网关、worker 的模块边界；提供 PostgreSQL、Redis 本地编排，Kafka 使用可选 profile；统一配置校验、结构化日志、健康与就绪探针。

验收：

- 冷启动可运行
- 缺失配置明确报错
- Redis/PG 不可用时 readiness 失败
- CI 执行类型、单元和集成检查。

<a id="module-2"></a>

### 2. PostgreSQL 数据模型与迁移

优先级：P0。依赖模块：1。

建立 workspace、membership、project、provider、model_target、alias、route_version、api_key、request、attempt、reservation、ledger、outbox、consumer_inbox、audit 表；金额使用整数微单位、时间使用 timestamptz；设计租户复合约束、游标索引及连接池。

验收：

- 空库迁移和升级迁移通过
- 跨项目引用被拒绝
- event_id 唯一去重
- 按项目时间范围查询提供 EXPLAIN ANALYZE
- 账本不随日志清理删除。

<a id="module-3"></a>

### 3. 成员、项目权限与管理会话

优先级：P0。依赖模块：2。

实现邀请、管理员/开发者/只读者权限矩阵；项目级授权；安全 cookie、CSRF、退出失效；所有写操作带审计上下文。

验收：

- 逐角色 API 测试
- 跨项目越权测试
- 失效会话拒绝
- 邀请过期及重复接受处理明确。

<a id="module-4"></a>

### 4. 供应商连接与凭据管理

优先级：P0。依赖模块：2、3。

实现连接 CRUD、加密凭据替换、显式连接测试；协议/模型能力登记；自定义 HTTPS 地址 DNS 和重定向 SSRF 校验；默认不回显密钥。

验收：

- 覆盖测试成功、鉴权失败、超时
- 环回/私网/元数据地址拒绝
- 凭据不出现在响应、日志与审计中。

<a id="module-5"></a>

### 5. 模型别名、版本化路由与安全备用

优先级：P0。依赖模块：4。

实现项目级别名、主备能力验证、草稿发布、版本冲突检测；每次请求固定版本；只对未发送故障或明确拒绝执行的限流执行一次备用。

验收：

- 并发发布返回冲突
- 发布后新旧请求使用正确版本
- 首个内容片段后不备用
- 参数/鉴权错误及执行未知不重试。

<a id="module-6"></a>

### 6. API Key 生命周期与数据面鉴权

优先级：P0。依赖模块：3、5。

生成高熵 key，摘要存 PG、完整值仅展示一次；模型范围、有效期、禁用、轮换；请求访问权威状态并检查项目和模型权限。

验收：

- 无效/过期/禁用 key 无上游调用
- 开发者不超范围授权
- 旧 key 禁用不影响新 key
- PG 故障拒绝新调用。

<a id="module-7"></a>

### 7. Redis 原子限流与并发租约

优先级：P0。依赖模块：2、6。

用 Lua 实现项目与 key 双层 token bucket、并发租约及预算预留统一准入；Redis TIME 为时钟；键使用 {project_id} hash tag；租约心跳、幂等释放、过期回收与策略版本。

验收：

- 多实例并发不超发
- 任一条件失败不部分扣减
- 取消/超时不泄漏
- Redis 重启与故障切换时暂停准入，恢复后验证 epoch
- 覆盖长流租约续期。

<a id="module-8"></a>

### 8. Redis 实时计费、预算预留与 PG 对账

优先级：P0。依赖模块：2、7。

实现价格版本、整数微货币单位、预留/派发/结算/待核对状态机；Redis Lua 原子费用操作；PG 持久化意图和最终账本；缺 usage 保留估算；故障 epoch 重建和幂等对账。

验收：

- 并发预算不超估算上限
- 主备分别计费
- 重复事件不重复扣费
- 跨月、价格更新、崩溃窗口和 Redis 丢数据可恢复
- 未知价格不按零放行。

<a id="module-9"></a>

### 9. 统一聊天 API 与 SSE 协议适配

优先级：P0。依赖模块：5、6、7、8。

实现 /v1/models 与 /v1/chat/completions 协议子集；校验参数、限制请求体、首包/空闲/整体超时、背压、取消；统一 request_id 与 attempt；接入限流和计费。

验收：

- 非流式/流式契约测试
- 中途断流报告 partial
- 慢客户端内存受控
- 取消释放租约
- 上游 usage 缺失进入待核对而非免费。

<a id="module-10"></a>

### 10. 可靠事件管道与可选 Kafka

优先级：P0。依赖模块：2、8。

默认 PG outbox worker；可选 Kafka publisher，topic usage.v1/request.v1/audit.v1；project_id 为分区键；至少一次投递、inbox 幂等、重试、DLQ、重放；事务内写业务记录和 outbox。

验收：

- 发布后崩溃及重复消费不重复计费
- 消费者提交 PG 后才提交 offset
- Kafka 故障不丢 outbox
- 积压达容量阈值告警并按策略拒绝新计费调用。

<a id="module-11"></a>

### 11. 请求追踪、用量查询与审计 API

优先级：P0。依赖模块：9、10。

请求与 attempt 时间线、游标分页、多维过滤；按小时聚合 token/费用；分开 actual/estimated/unknown；数据水位、最近异常；保留期清理与脱敏。

验收：

- 统计可与账本核对
- 重放不重复聚合
- 部分响应不计完整成功
- 查询有项目范围
- 超 60 秒延迟显示 stale
- 正文与凭据不落库。

<a id="module-12"></a>

### 12. 控制台页面与首次接入引导

优先级：P0。依赖模块：3、4、5、6、11。

实现概览、供应商、模型路由、API Keys、请求日志、用量、接入指南、设置八组页面；提供表单校验、空态、错误态、冲突提示和移动端布局。

验收：

- 完成三步接入闭环
- key 一次性展示与轮换可用
- 测试请求走相同授权计费链
- 键盘可操作
- 预算和估算状态清晰
- 无权限操作在服务端拒绝。

<a id="module-13"></a>

### 13. 可靠性、安全与性能验收

优先级：P0。依赖模块：12。

建立 A01–A12 的端到端验证、PG/Redis/Kafka 故障注入、备份恢复、计量对账、SSRF/权限测试；压测 100 并发与慢流；提供运行手册和告警。

验收：

- 附测试证据与资源配置
- 演练 Redis failover 后无静默少扣
- PG 失败无无账调用
- Kafka 重放正确
- 验证关停排空、日志清理及账本保留。

<a id="module-14"></a>

### 14. P1 协议扩展与运营能力评估

优先级：P1。依赖模块：13。

形成原生供应商、工具调用、结构化输出、嵌入/多模态的逐项能力矩阵与契约；评估告警、导出、路由权重、回滚与外部身份接入。

验收：

- 每项给出独立范围和兼容测试计划
- 依据首版真实反馈排序
- 不未经验证宣称全协议兼容。
