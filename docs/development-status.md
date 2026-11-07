# 开发状态

目标 [#16](https://github.com/jihuayu/ai-gateway/issues/16)，开发分支 `codex/complete-issues`，交付 PR #17。

#2–#15 的应用与评估内容已实现，逐项对应文件、测试和适用边界见 [验证报告](verification.md)。DeepSeek 实现和独立 review 已运行，独立关闭审阅结论为 ACCEPT，B1–B5 均已关闭。

PostgreSQL、Redis、可选 Kafka 和中文控制台具备可复现运行方式。skill 和会话保存在执行环境，不加入仓库。

性能实测尚未达到原设计的处理开销目标；详情与生产适用边界见验证报告。issues 在PR合并前仍保持开放。

后续目标 [#18](https://github.com/jihuayu/ai-gateway/issues/18) 的#19/#20已在候选 `994e7d4` 交付：同脚本100并发总响应P95中位数1222.52→952.27ms，请求SQL31→22，前后各484笔精确账本。DeepSeek重试恢复后完成队列实现，独立review为ACCEPT；后续 `6251405` 修复其指出的诊断错误码问题，关闭审阅也为ACCEPT，详见 [审阅记录](deepseek-review.md)。最终业务候选真实服务完整测试65通过、2专用跳过，性能候选的专用重启/切主及浏览器另行通过。#21/#22的剩余范围继续按 [性能计划](performance-plan.md) 跟踪。

最新目标[#23](https://github.com/jihuayu/ai-gateway/issues/23)将默认数据面改为内存/Redis，不再使用PG请求回退。`fa94339`的82项完整测试79通过/3专用跳过，AOF专项另行通过；同环境请求SQL22→0，100并发HTTP P95中位数1518.16→429.89ms。PG整批投影核对约7.81秒，需按#21优化；协议、故障恢复限制与最终review范围见[本轮结果](redis-performance-results.md)。上文#16/#18的ACCEPT与性能数据属于对应历史候选。
