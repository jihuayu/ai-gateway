# 开发任务索引

初始#2–#15任务已在 [PR #17](https://github.com/jihuayu/ai-gateway/pull/17) 提供实现或指定评估内容；issues 在合并前保持开放。逐项证据与限制见 [验证报告](verification.md)，技术细节见 [实施设计](implementation.md)。

| 任务 | 优先级 | 依赖 |
| --- | --- | --- |
| [#2 基础工程与运行环境](https://github.com/jihuayu/ai-gateway/issues/2) | P0 | 无 |
| [#3 PostgreSQL 数据模型与迁移](https://github.com/jihuayu/ai-gateway/issues/3) | P0 | [#2](https://github.com/jihuayu/ai-gateway/issues/2) |
| [#4 成员、项目权限与管理会话](https://github.com/jihuayu/ai-gateway/issues/4) | P0 | [#3](https://github.com/jihuayu/ai-gateway/issues/3) |
| [#5 供应商连接与凭据管理](https://github.com/jihuayu/ai-gateway/issues/5) | P0 | [#3](https://github.com/jihuayu/ai-gateway/issues/3)、[#4](https://github.com/jihuayu/ai-gateway/issues/4) |
| [#6 模型别名、版本化路由与安全备用](https://github.com/jihuayu/ai-gateway/issues/6) | P0 | [#5](https://github.com/jihuayu/ai-gateway/issues/5) |
| [#7 API Key 生命周期与数据面鉴权](https://github.com/jihuayu/ai-gateway/issues/7) | P0 | [#4](https://github.com/jihuayu/ai-gateway/issues/4)、[#6](https://github.com/jihuayu/ai-gateway/issues/6) |
| [#8 Redis 原子限流与并发租约](https://github.com/jihuayu/ai-gateway/issues/8) | P0 | [#3](https://github.com/jihuayu/ai-gateway/issues/3)、[#7](https://github.com/jihuayu/ai-gateway/issues/7) |
| [#9 Redis 实时计费、预算预留与 PG 对账](https://github.com/jihuayu/ai-gateway/issues/9) | P0 | [#3](https://github.com/jihuayu/ai-gateway/issues/3)、[#8](https://github.com/jihuayu/ai-gateway/issues/8) |
| [#10 统一聊天 API 与 SSE 协议适配](https://github.com/jihuayu/ai-gateway/issues/10) | P0 | [#6](https://github.com/jihuayu/ai-gateway/issues/6)、[#7](https://github.com/jihuayu/ai-gateway/issues/7)、[#8](https://github.com/jihuayu/ai-gateway/issues/8)、[#9](https://github.com/jihuayu/ai-gateway/issues/9) |
| [#11 可靠事件管道与可选 Kafka](https://github.com/jihuayu/ai-gateway/issues/11) | P0 | [#3](https://github.com/jihuayu/ai-gateway/issues/3)、[#9](https://github.com/jihuayu/ai-gateway/issues/9) |
| [#12 请求追踪、用量查询与审计 API](https://github.com/jihuayu/ai-gateway/issues/12) | P0 | [#10](https://github.com/jihuayu/ai-gateway/issues/10)、[#11](https://github.com/jihuayu/ai-gateway/issues/11) |
| [#13 控制台页面与首次接入引导](https://github.com/jihuayu/ai-gateway/issues/13) | P0 | [#4](https://github.com/jihuayu/ai-gateway/issues/4)、[#5](https://github.com/jihuayu/ai-gateway/issues/5)、[#6](https://github.com/jihuayu/ai-gateway/issues/6)、[#7](https://github.com/jihuayu/ai-gateway/issues/7)、[#12](https://github.com/jihuayu/ai-gateway/issues/12) |
| [#14 可靠性、安全与性能验收](https://github.com/jihuayu/ai-gateway/issues/14) | P0 | [#13](https://github.com/jihuayu/ai-gateway/issues/13) |
| [#15 P1 协议扩展与运营能力评估](https://github.com/jihuayu/ai-gateway/issues/15) | P1 | [#14](https://github.com/jihuayu/ai-gateway/issues/14) |

推荐顺序：基础工程 → PG → 权限/供应商/路由/key → Redis 限流计费 → 网关/事件 → 查询/控制台 → 故障验收。Kafka 为可选传输，outbox 和幂等消费为 P0 必需。

## 性能优化目标 #18

| 任务 | 状态 | 后续依赖 |
| --- | --- | --- |
| [#19 多并发响应/结算剖析](https://github.com/jihuayu/ai-gateway/issues/19) | 已交付脚本与前后报告 | 无 |
| [#20 数据库往返与结算连接等待](https://github.com/jihuayu/ai-gateway/issues/20) | 已交付实现、回归与可比压测 | #19 |
| [#21 后台连接池与增量投影](https://github.com/jihuayu/ai-gateway/issues/21) | 批次消费前置完成；主要范围待开发 | #20，历史增长基线 |
| [#22 固定资源SSE与持续负载门槛](https://github.com/jihuayu/ai-gateway/issues/22) | 待开发 | #21，固定资源环境 |

实现边界和后续验收顺序见 [性能计划](performance-plan.md)，当前测量见 [优化结果](performance-results.md)。
