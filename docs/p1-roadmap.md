# P1 协议扩展与运营能力路线图及实现状态（#15）

状态：**已在现代 Java 分支（`feat/java-gateway` / PR #28）完成核心契约实现并通过端到端契约测试**。

## 1. 原则与证据门槛

1. **按能力声明，不按品牌宣称。** 模型目标与路由别名显式声明 `capabilities`，网关在准入前通过 `DataPlaneExecutor.verifyCapabilities` 严格校验。
2. **静默忽略参数属于缺陷。** 未支持的字段返回明确 `400 unsupported_parameter`，未声明的能力返回 `400 capability_not_supported`。
3. **未知不等于免费。** usage 缺失、价格未知、执行状态未知分别呈现；所有 P1 扩展端点（包括 `/v1/embeddings` 与 `/v1/messages`）均走统一的零 SQL Redis Lua 准入、并发租约、预算预留和异步 PostgreSQL 账本管道。
4. **首个内容片段后不切换目标。** 工具调用、结构化输出与多模态均遵守首个 SSE 数据块发出后不隐式切换目标的约束。

## 2. 已实现能力矩阵（P1-A 至 P1-G）

| 工作包 | 能力项 | 当前状态 | 实现位置与契约验证（`AiGatewayIntegrationTest`） |
| :--- | :--- | :--- | :--- |
| **P1-A** | 能力注册表（Capability Registry） | **已实现并验证** | `DataPlaneExecutor.KNOWN_CAPABILITIES` 与 `verifyCapabilities()`；未声明 `tools`/`json_schema`/`embeddings`/`vision` 的别名调用对应功能时稳定返回 `400 capability_not_supported`，不发起上游调用。 |
| **P1-B** | 工具调用（Tool Calling） | **已实现并验证** | 支持 `tools`（`type: "function"`）、`tool_choice`、`role: "tool"`（校验 `tool_call_id`）及上游 `tool_calls` 响应；强制校验 `function.arguments` 为合法 JSON，防止损坏 JSON 透传。 |
| **P1-C** | 结构化输出（Structured Output） | **已实现并验证** | 支持 `response_format`（`json_object` 与 `json_schema`）；在 `normalizeCompletion` 中校验上游输出是否为合法 JSON 对象/数组，非法 JSON 返回 `502 upstream_invalid_response` 并保留真实 attempt 计费。 |
| **P1-D** | 嵌入向量（Embeddings） | **已实现并验证** | 独立端点 `POST /v1/embeddings`（同时挂载于 Netty 数据面端口与 Spring Boot 控制面端口）；校验 `input`（单字符串或 1–128 字符串数组）、`encoding_format`、`dimensions`，走独立零 SQL Redis 计费与异步账本。 |
| **P1-E** | 视觉输入安全（Vision Input） | **已实现并验证** | 支持 `messages[].content` 多模态分片（`text` + `image_url`）；远程 URL 走 `SecurityUtils.validateProviderUrl` SSRF 校验，内联 `data:image/(png\|jpeg\|webp\|gif);base64,...` 校验 MIME 白名单与 512KB 解码体积上限，并附加保守视觉 token 预留。 |
| **P1-F** | 第二原生协议（Anthropic Messages） | **已实现并验证** | 独立端点 `POST /v1/messages`，支持 `x-api-key` 或 `Authorization: Bearer` 鉴权、顶层 `system` 提示词、`content` 块列表及流式/非流式 Anthropic `message` 响应格式转换。 |
| **P1-G** | 运营能力（导出与路由回滚） | **已实现并验证** | `GET /api/usage/export?project_id=...&format=csv\|json` 支持按时间范围导出小时级聚合账单并记录审计；`POST /api/aliases/{id}/rollback` 支持指定历史已发布版本号创建新的不可变发布版本并实时同步 Redis 快照。 |
