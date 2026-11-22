# 开发任务索引与完成状态

所有 P0（#2–#14）、零 SQL 热路径（#23–#27）、性能优化（#18–#22）与 P1 协议扩展及运营能力（#15）均已在现代 Java 分支（`feat/java-gateway` / [PR #28](https://github.com/jihuayu/ai-gateway/pull/28)）全部实现并通过端到端测试验证。技术细节见 [实施设计](implementation.md)、[热路径协议](hot-path.md) 与 [验证报告](verification.md)。

| 任务 | 优先级 | 状态 | 核心实现类 / 资源 |
| --- | --- | --- | --- |
| [#2 基础工程与运行环境](https://github.com/jihuayu/ai-gateway/issues/2) | P0 | **已完成** | `AiGatewayApplication`、`GatewayConfig`、`compose.yaml` |
| [#3 PostgreSQL 数据模型与迁移](https://github.com/jihuayu/ai-gateway/issues/3) | P0 | **已完成** | `DatabaseMigrator`、`migrations/001.sql`–`006_incremental_usage.sql` |
| [#4 成员、项目权限与管理会话](https://github.com/jihuayu/ai-gateway/issues/4) | P0 | **已完成** | `ControlAuthService`、`ControlSecurityFilter`、`IdentityController` |
| [#5 供应商连接与凭据管理](https://github.com/jihuayu/ai-gateway/issues/5) | P0 | **已完成** | `ProvidersController`、`SecurityUtils` |
| [#6 模型别名、版本化路由与安全备用](https://github.com/jihuayu/ai-gateway/issues/6) | P0 | **已完成** | `CatalogController`、`HotRuntime`、`DataPlaneExecutor` |
| [#7 API Key 生命周期与数据面鉴权](https://github.com/jihuayu/ai-gateway/issues/7) | P0 | **已完成** | `CatalogController`、`HotRuntime.authenticateBearerHeader` |
| [#8 Redis 原子限流与并发租约](https://github.com/jihuayu/ai-gateway/issues/8) | P0 | **已完成** | `HotBilling`、`src/main/resources/lua/hot.lua` |
| [#9 Redis 实时计费、预算预留与 PG 对账](https://github.com/jihuayu/ai-gateway/issues/9) | P0 | **已完成** | `HotBilling`、`HotProjector`、`hot.lua` |
| [#10 统一聊天 API 与 SSE 协议适配](https://github.com/jihuayu/ai-gateway/issues/10) | P0 | **已完成** | `NettyGatewayServer`、`DataPlaneExecutor` |
| [#11 可靠事件管道与可选 Kafka](https://github.com/jihuayu/ai-gateway/issues/11) | P0 | **已完成** | `EventWorker` |
| [#12 请求追踪、用量查询与审计 API](https://github.com/jihuayu/ai-gateway/issues/12) | P0 | **已完成** | `QueriesController`、`RetentionService` |
| [#13 控制台页面与首次接入引导](https://github.com/jihuayu/ai-gateway/issues/13) | P0 | **已完成** | `src/main/resources/static/`（`index.html`, `app.js`, `styles.css`） |
| [#14 可靠性、安全与性能验收](https://github.com/jihuayu/ai-gateway/issues/14) | P0 | **已完成** | `AiGatewayIntegrationTest`、`docs/operations.md`、`docs/verification.md` |
| [#15 P1 协议扩展与运营能力评估](https://github.com/jihuayu/ai-gateway/issues/15) | P1 | **已完成** | `DataPlaneExecutor`（P1-A..F）、`CatalogController`（回滚）、`QueriesController`（导出） |
| [#18–#22 性能优化、批量投影、增量聚合与 SSE 压测](https://github.com/jihuayu/ai-gateway/issues/18) | P0/P1 | **已完成** | `HotProjector.projectHotRequestBatch`、`EventWorker`（O(1) 增量聚合）、`006_incremental_usage.sql` |
| [#23–#27 零 SQL 热路径与异步 PG 账本](https://github.com/jihuayu/ai-gateway/issues/23) | P0 | **已完成** | `HotContract`、`HotRuntime`、`HotBilling`、`HotStream`、`HotProjector` |
