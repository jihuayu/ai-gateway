# AI Gateway 现代 Java 架构验证与性能报告

日期：2026-11-22（UTC+8） · 分支：`feat/java-gateway` · 交付 PR：[#28](https://github.com/jihuayu/ai-gateway/pull/28)

## 1. 技术栈与模块边界

- **运行时与并发模型**：Eclipse Temurin JDK 25 LTS（`25.0.5.1`），全面启用虚拟线程（`spring.threads.virtual.enabled=true` 与 `Executors.newVirtualThreadPerTaskExecutor()`）。
- **原生 Netty 数据面网关（`com.jihuayu.aigateway.netty`）**：
  - `NettyGatewayServer`：纯 Netty 4.2.20 NIO HTTP/SSE 管道（`HttpServerCodec` + `HttpObjectAggregator`），独立监听数据面端口（默认 `3001`）。
  - `DataPlaneExecutor`：实现 `/v1/models`、`/v1/chat/completions`（非流式与 SSE 流式）、`/v1/embeddings`（嵌入向量）和 `/v1/messages`（原生 Anthropic Messages 协议适配），内置严格参数/能力校验、保守微美元预留、主备自动切换（仅限未发送或 429 明确拒绝）、10 秒租约心跳与客户端断流/写入超时背压保护。
- **Redis 零 SQL 热路径与异步 PostgreSQL 投影（`com.jihuayu.aigateway.hot` & `worker`）**：
  - `HotRuntime`：启动检查 Redis `appendonly=yes`、`appendfsync=always`、`maxmemory-policy=noeviction`；`REPEATABLE READ` 隔离级别 + 会话级 Advisory Lock 串行发布配置快照（解锁失败时自动驱逐持锁连接）；进程内 LRU 缓存 4 个不可变路由快照，请求路径 **0 SQL**。
  - `HotBilling`：执行 `src/main/resources/lua/hot.lua`，在同一 hash slot 内原子完成 `run_id`/`epoch` 围栏校验、项目/密钥双层 Token Bucket 限流、并发租约、预算预留、每次准入预留 7 个日志槽位、2^53-1 安全整数差额结算以及 `XADD` Stream 写入。
  - `HotStream` & `HotProjector`：独立 4 连接后台池通过 `XAUTOCLAIM` + `XREADGROUP` 每次批量读取最多 200 条事件，按 `requestId` 合并旧 `revision`，再**按 `projectId` 批量合并为单次 PostgreSQL 事务**（单次报错自动回退至逐请求隔离事务），事务提交后才执行原子 `XACK` + `XDEL`。
  - `EventWorker`：基于 `006_incremental_usage.sql` 的 `usage_request_state` 表与 `ledger_request_id_idx` 索引，实现 **O(1) 请求级增量 `usage_hourly` 聚合**（替代历史账本全量扫描），支持 `unknown` 估算转 `actual` 净额平移、指数退避重试、DLQ 与可选 Kafka 投递。
- **Spring Boot 4.2.0 管理控制面与静态控制台（`com.jihuayu.aigateway.control`）**：
  - `ControlAuthService`、`ControlSecurityFilter`、`IdentityController`、`ProvidersController`、`CatalogController`、`QueriesController`：提供 Scrypt 密码哈希、HttpOnly Cookie + `X-CSRF-Token`、AES-256-GCM 凭据加密、SSRF IPv4/IPv6 与 DNS 校验、乐观锁路由发布、不可变路由版本回滚（`POST /api/aliases/{id}/rollback`）、用量 CSV/JSON 导出（`GET /api/usage/export`）及静态 Web 控制台（`src/main/resources/static/`）。

## 2. 性能与规模实测（#18–#22, #27）

在真实 PostgreSQL 15.20 与 Redis 7.0.15（`appendonly=yes`、`appendfsync=always`、`maxmemory-policy=noeviction`）环境下通过 `mvn test` 实测：

| 指标项 | 实测结果 | 验收标准 / 对比 |
| :--- | :--- | :--- |
| **请求热路径 SQL 次数（#23, #27）** | **0 次** | 投影前 `requests` 表记录数为 0，完全解耦 PostgreSQL |
| **SSE 块转发延迟 P95（#22）** | **6.95 ms – 8.30 ms** | 优于原设计 `< 20 ms` 的块转发目标 |
| **100 请求高并发（50 虚拟线程）HTTP P50 / P95** | **P50: 474.90 ms / P95: 556.52 ms** | 100% HTTP 200，无连接池饥饿，Redis 预留归零 |
| **100 请求整批 PG 投影 + 增量聚合 Drain 耗时（#21）** | **483.40 ms** | 相比原逐请求事务（`7805.74 ms`）耗时降低 **93.8%** |
| **10,000+ 历史账本下单次增量聚合耗时（#21）** | **< 15 ms**（`EXPLAIN ANALYZE` 走 `Index Scan`） | O(1) 请求级增量更新，不随历史账本增长线性退化 |

## 3. 任务与测试覆盖索引（#2–#27）

`src/test/java/com/jihuayu/aigateway/AiGatewayIntegrationTest.java` 覆盖以下 7 大端到端测试阶段：

1. `Order(1) testSecurityCryptoPasswordAndSsrfGuards`：覆盖 #5 SSRF 校验（环回、私网、`169.254.169.254`、IPv6 映射地址）、AES-256-GCM 凭据加解密与 Scrypt 密码校验。
2. `Order(2) testControlPlaneBootstrapProvidersAliasesAndKeys`：覆盖 #2、#3、#4、#5、#6、#7、#13 静态控制台、管理员 Bootstrap、CSRF 强制拦截、供应商测试、主备模型目标、别名乐观锁冲突（409）与发布、一次性 API Key 签发。
3. `Order(3) testDirectNettyGatewayZeroSqlBufferedAndSseAndFallback`：覆盖 #6、#7、#8、#9、#10、#23、#24、#25、#27 原生 Netty `/v1/models`、未知参数 400 拒绝、非流式与 SSE 流式响应、主目标 429 自动切换备用目标、零 SQL 验证及密钥吊销即时生效（401）。
4. `Order(4) testUnknownSettlementAdjustmentNearMaxSafeIntegerAndRetention`：覆盖 #9、#11、#12、#14、#25、#26 `unknown` 估算结算、首次 `actual` 净额补偿分录、二次不同金额 409 `settlement_conflict`、`usage_hourly` 增量转移以及 `RetentionService` 日志清理对账本与墓碑的保护。
5. `Order(5) testP1ProtocolExtensionsAndOperationalCapabilities`：覆盖 #15 全部 P1 工作包（`P1-A` 能力注册表拦截、`P1-B` Tool Calling、`P1-C` `response_format` JSON Schema 正负向校验、`P1-D` `/v1/embeddings`、`P1-E` 视觉 `image_url` data-URI MIME/大小/SSRF 校验、`P1-F` `/v1/messages` 原生 Anthropic Messages 适配、`P1-G` 路由版本回滚与 CSV 导出）。
6. `Order(6) testIssue21TenThousandHistoryIncrementalAggregationAndExplainPlan`：覆盖 #3、#21 10,000 条历史请求与账本下的 O(1) 增量聚合性能与 `EXPLAIN ANALYZE` 索引扫描证明。
7. `Order(7) testIssue22FixedResourceSseChunkForwardingAndConcurrentLoadBenchmark`：覆盖 #18、#19、#20、#22、#27 SSE 块转发 `< 20ms` 实测、100 并发请求批量投影及 Redis 实时余额与 PostgreSQL 账本微美元级精确核对。
