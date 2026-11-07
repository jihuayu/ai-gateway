# 开发状态与继续执行方式

目标：[完成全部任务 #16](https://github.com/jihuayu/ai-gateway/issues/16)。范围：#2–#15。

## 已准备

- 开发分支 `codex/complete-issues`。
- Node 24 / TypeScript / Fastify / PostgreSQL / Redis / Kafka 可选依赖与锁文件。
- PostgreSQL 17、Redis 7 的本地 Compose；Redis 开启 AOF、noeviction。
- 环境变量示例、配置校验、CI 服务定义和配置测试。
- `jihuayu/skills` 的 DeepSeek skill 已安装在本执行环境 `/workspace/local-skills/deepseek-subagent`，来源 commit `9a63f2539c6fde1e85978c8e0ea166de73af0cab`，其 14 项 harness 测试通过。

## 待启动

DeepSeek 的实现和独立 review 尚未运行：自动审批拒绝启动外部 worker，要求用户明确授权向配置端点发送必要项目材料以及文件写入、命令执行范围。没有将拒绝转换为其他模型执行，也没有把业务 issues 标记完成。

获准后依赖顺序：数据模型/控制面 → Redis 准入计费 → 网关/事件 → 控制台 → 集成测试 → 独立 DeepSeek review → 修复与复验。实现和 review 使用独立会话，由主代理核验真实文件与结果。

skill 及其连接设置、会话文件不提交到应用仓库。任务材料仅包含实现需要的需求、代码与测试结果，不包含凭据。

## 验证边界

当前只有基础配置测试，尚无业务应用实现或端到端验收。`start`、`migrate`、`worker` 的入口将在相应实现任务中补齐；当前不能把该分支当作可运行产品交付。
