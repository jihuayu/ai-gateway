# 性能优化计划

日期：2026-11-07（Asia/Hong_Kong）。父目标 [#18](https://github.com/jihuayu/ai-gateway/issues/18)，延续 PR #17。

## 已量化的问题

新基线将HTTP响应、完整结算和连接池等待分开。1/10/50/100并发，每级3次重复，先记录冷请求再执行暖请求；每轮核对真实PG账本。484笔调用必须恰好9680微美元，不把成功响应当作计费已完成。

当前暖单请求约31条SQL；100并发连接获取P95约139–341ms、等待队列峰值134–188。事务在等待同项目PG锁时持有连接，造成结算与新请求争抢；请求/attempt元数据也存在重复往返。各阶段P95相互重叠，不能相加当作总响应时间。

## 阶段与任务

| 顺序 | Issue | 工作 | 验收 |
| --- | --- | --- | --- |
| P0.1 | [#19](https://github.com/jihuayu/ai-gateway/issues/19) | 可复现剖析、SQL/Redis计数、连接获取与结算时间 | 同环境前后报告；无参数/密钥/正文；所有诊断包装可恢复 |
| P0.2 | [#20](https://github.com/jihuayu/ai-gateway/issues/20) | 结算连接占用、元数据写入合并、generation普通读取与意图写入往返 | 鉴权继续查询权威状态；PG锁/行锁、Redis围栏、持久化检查保留；全回归与100并发精确账本 |
| P1.1 | [#21](https://github.com/jihuayu/ai-gateway/issues/21) | 小池隔离聚合；账本增长下改为请求级增量投影 | 万条及更大历史的查询计划；补偿净额、乱序、去重、重放测试；总连接数预算 |
| P1.2 | [#22](https://github.com/jihuayu/ai-gateway/issues/22) | 固定资源SSE与持续负载性能门槛 | 首token/块转发分开测量；慢客户端与故障；CI稳定退化阈值 |

本轮交付P0测量及首轮优化，后两阶段独立跟踪。原设计50ms处理开销与20ms块转发目标仍须在固定资源、明确上游基线下验证；不以总响应时间或差分分位数替代该证明。

## 本轮实现原则

结算先在本进程按项目FIFO排队，再获取PG连接；跨进程依然依赖PG advisory锁，请求行锁负责幂等。普通请求不缓存API Key授权，不删减Redis预算预留与run_id/generation检查，不用异步丢弃账本换取延迟。

请求与attempt元数据使用一条原子SQL写入；已成功落库的attempt不在finalize重复写。失败的增量写入必须在finalize重试。其它数据库往返合并保持提交点先于上游派发。

## 执行与结果

```sh
DATABASE_URL=... REDIS_URL=... PERF_REPORT=artifacts/performance/profile.json node --import tsx scripts/profile-benchmark.ts
```

PERF_CONCURRENCY指定并发级别，PERF_REPEATS指定重复次数。只对隔离测试数据库运行。输出包含SQL模板、耗时与计数，不包含参数或凭据。前后报告与解释见 [首轮优化结果](performance-results.md)：100并发三轮总响应P95中位数1222.52→952.27ms，请求SQL31→22；10并发中位数增加约6%，需要后续持续负载验证。

DeepSeek本轮首次调用403；经用户要求重试后已恢复并完成结算队列实现，独立review使用新的只读会话。审阅结论见 [review记录](deepseek-review.md)。#19/#20已有实现与验证证据；#21已完成批次合并的前置工作，独立连接池、增量投影及#22继续按上述顺序实施。
