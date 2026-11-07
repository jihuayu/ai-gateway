# 开发目标

完成 [目标 #16](https://github.com/jihuayu/ai-gateway/issues/16) 下 #2–#15 的实现与验收。

固定约束：PostgreSQL 持久化；Redis 限流与实时计费；Kafka 可选事件传输。

DeepSeek 使用 jihuayu/skills 的外部 harness，分别启动 implementation 与独立 review 会话。主代理负责接口、集成、真实服务测试、缺陷修复和提交。只有证据满足验收的任务才标为完成。

交付门槛：可启动应用、迁移、集成与故障测试、浏览器闭环、运行手册、逐 issue 证据、无阻塞 review。未经验证的外部服务兼容性不宣称通过。
