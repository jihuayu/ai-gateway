# 实现与验收证据

日期：2026-11-07（Asia/Hong_Kong）。分支：`codex/complete-issues`。目标：[#16](https://github.com/jihuayu/ai-gateway/issues/16)。

## 验证范围

Node.js 24.22.1；真实 PostgreSQL 17.11、Redis 7、Kafka 3.9.1；浏览器使用 Playwright Chromium。模型响应来自本地可控 HTTP 上游，用于稳定复现断流、限流、未知用量等情况，**不代表已认证全部商业模型服务**。

### 自动测试

- `npm run typecheck`：通过，包含 src、test、scripts。
- 完整服务测试：设置 DATABASE_URL、REDIS_URL、KAFKA_BROKERS 后运行 `npm test`。
- 56 项测试，其中 54 项通过、2 项专用容器故障测试默认跳过；这两项分别执行并通过。无失败。
- 用例覆盖真实账本与 Redis 原子准入、重复事件、更正、缺失用量、密钥限流、跨月及价格快照、会话/CSRF/角色隔离、供应商加密、路由冲突、SSE 完整性/取消/背压、备用、PG 故障、Kafka 消费和保留清理。

### 专项演练

1. `test/recovery.test.ts`：在独立无持久化 Redis 中先计费800微美元，再真实重启；恢复从 PG 重建800支出，额外300预留被拒绝。配置 RECOVERY_REDIS_URL、RECOVERY_CONTAINER 后单独运行。
2. `test/failover.test.ts`：两个独立 Redis 测试节点，WAIT 确认副本、提升副本、停止旧主、切换 TCP 入口；新 run_id 拒绝旧 generation，重建后仍保留800支出。配置 FAILOVER_REPLICA_URL、FAILOVER_PRIMARY_CONTAINER 后单独运行。只针对专用临时容器执行，不得指向业务节点。
3. PG 备份恢复：`pg_dump -Fc` → 独立恢复数据库 → `pg_restore --clean --if-exists --no-owner --exit-on-error`，恢复账本核对为3笔、合计60微美元（演练快照时的数据），随后删除临时恢复库。未覆盖服务数据库。
4. `scripts/browser-smoke.ts`：管理员登录/初始化、供应商、目标、别名发布、一次性密钥、实际测试请求、八组页面、390px窄屏均通过，无 pageerror。密钥关闭后从 DOM 移除，不进入截图。

截图：[桌面控制台](../artifacts/console-desktop.png)、[移动端](../artifacts/console-mobile.png)。

## 逐 issue 对照

| Issue | 实现 | 验证依据 |
| --- | --- | --- |
| #2 | package/Compose/config/CI/server/health | 类型检查、冷启动、真实服务及浏览器 |
| #3 | migrations 001–003、db.ts | 隔离 schema 迁移、路由归属约束、备份恢复 |
| #4 | control.ts、identity.ts | control/identity tests、浏览器成员与设置页面 |
| #5 | providers.ts、security.ts | provider/security tests、浏览器配置链 |
| #6 | catalog.ts、gateway.ts | stale draft 409、主备/参数/能力测试、不可变版本 |
| #7 | catalog.ts、gateway.ts | 一次性 key、到期/禁用/权限、浏览器 DOM 清理 |
| #8 | billing.ts、lua | 项目/key双层限流、原子并发、租约、100并发 |
| #9 | billing.ts、ledger/outbox | 幂等、unknown→actual、最终实际费用冲突、跨月、重启/主从切换 |
| #10 | gateway.ts | SSE/超时/背压/取消/首片段后无备用、真实 HTTP 集成 |
| #11 | events.ts、worker.ts | PG outbox/inbox、Kafka真实发布消费、去重、死信/重放 |
| #12 | queries.ts、events.ts | 模型筛选、请求详情、未知费用、修正后的聚合、保留测试 |
| #13 | web/ | 八组页面和接入闭环、窄屏、一次性 key、权限处理 |
| #14 | test/、scripts/、operations.md | 完整套件、故障演练、备份恢复、100并发基线 |
| #15 | p1-roadmap.md | 每项能力范围、兼容契约和测试计划；不把P1能力标为已实现 |

## 性能结果与适用边界

[原始报告](../artifacts/benchmark.json)：100并发，100个200响应；账本恰好100笔，合计2000微美元。测量环境可见5个逻辑CPU、约18.9GB内存，不是原设计拟定的2vCPU/4GB隔离基准。

当前直连上游总响应P95约160ms，经网关P95约1732ms；二者差约1572ms，包含并发排队、数据库与响应耗时，不能直接等同于纯处理开销。原设计提出的新增处理P95≤50ms、块转发P95≤20ms**尚未达成或得到证明**。本轮证明功能、计量正确性和并发闭环，性能优化仍是部署评估事项，不将本版称为已达到生产SLO。

Redis 检测 run_id 与 generation；Lua 内再核对实际 server run_id，防止缓存检查与执行间切主。部署方仍须隔离旧主，应用层不能独立解决网络分区下两个可写主节点。真实供应商合同、地区处理、实际账单精度和完整参数兼容仍须逐服务验证。

费用以微美元整数计价，是用量估算与预算准入，不含收款系统。缺失usage保留保守估算；第一次确认actual后金额不可变，不同actual重复提交返回409，后续人工财务更正需单独协议。最终usage不能被重复消息改写或多次扣费。

## DeepSeek 实现与 review

已使用用户指定 [jihuayu/skills DeepSeek subagent](https://github.com/jihuayu/skills/blob/main/deepseek-subagent/SKILL.md)，来源commit `9a63f2539c6fde1e85978c8e0ea166de73af0cab`，模型 `auto/deepseek-v4.1-flash`。实现会话与只读独立review会话分离，主代理检查并整合代码和真实测试。

独立review提出的阻塞项已修复：stale draft 发布、缺失DONE的流截断误成功、下游写入无限等待、重复更正计费、请求模型元数据缺失。更正聚合、Redis单预算键丢失、run_id原子围栏与连接池死锁也有回归证据。最终关闭结论见 [review记录](deepseek-review.md)。skill连接参数、会话及第三方密钥不加入应用仓库。
