# AI Gateway

面向开发者与小型团队的模型网关和中文 Web 控制台。基于 **现代 Java（JDK 25 LTS + 虚拟线程 Virtual Threads）** 构建：

- **数据面网关（Direct Netty 4.2 + Virtual Threads）**：独立监听高吞吐数据面端口（默认 `GATEWAY_PORT=3001`），直接由原生 Netty HTTP/SSE 管道与 JDK 25 虚拟线程执行器处理 `/v1/models` 与 `/v1/chat/completions`。
- **控制面与管理平台（Spring Boot 4.2.0 + Virtual Threads）**：监听管理端口（默认 `PORT=3000`），承载中文 Web 控制台（`src/main/resources/static/`）、RBAC 权限会话、供应商 SSRF 校验、不可变路由版本发布、API Key 生命周期与审计查询。
- **零 SQL 请求热路径（Redis Lua + Streams → 异步 PostgreSQL 账本）**：请求热路径仅访问进程内不可变路由快照与 Redis（Lua 原子鉴权、项目/密钥双层 Token Bucket 限流、并发租约、预算预留及 Redis Stream 财务日志）；后台独立连接池（`HotProjector` + `EventWorker`）异步将 Redis Stream 幂等投影至 PostgreSQL 不可变账本与 Outbox / 可选 Kafka，详见 [Redis 热路径与异步账本](docs/hot-path.md)。

## 本地运行

要求 **JDK 25**、**Maven 3.9+**、**Docker Compose**（PostgreSQL 15+ 与 Redis 7+，开启 `appendonly=yes`、`appendfsync=always`、`maxmemory-policy=noeviction`）。

```sh
docker compose up -d --wait postgres redis
cp .env.example .env
# 在 .env 中设置 64 位十六进制 ENCRYPTION_KEY（openssl rand -hex 32）
mvn spring-boot:run
```

- **Web 管理控制台与管理 API**：访问 `http://127.0.0.1:3000` 创建首个管理员与项目。
- **Netty 独立数据面网关**：`http://127.0.0.1:3001/v1/models` 与 `http://127.0.0.1:3001/v1/chat/completions`（管理端口 `3000` 同样挂载了 `/v1/*` 以便同源测试）。

## 核心能力

- 工作空间、项目、管理员/开发者/只读权限、成员邀请、CSRF 防护和会话吊销。
- AES-256-GCM 加密供应商凭据、SSRF 私网/环回/云元数据地址拦截、模型目标、不可变路由版本、安全主备切换、一次性 API Key。
- 原生 Netty 文本 Chat Completions 与 SSE 流式响应转发、严格参数能力校验、首包/空闲/整体超时、客户端断流取消与背压控制。
- Redis Lua 原子频率与并发租约、预算预留、Redis Stream 有界财务日志、PostgreSQL 不可变账本、unknown 估算与 actual 净额补偿、故障恢复围栏。
- PostgreSQL Outbox / Consumer Inbox 幂等按小时聚合、可选 Kafka 发布、指数退避重试与死信重放。

## 验证

```sh
mvn test
```

## 文档

- [产品与系统设计](docs/design.md)
- [模块实施设计](docs/implementation.md)
- [任务索引](docs/tasks.md)
- [Redis 热路径与异步账本](docs/hot-path.md)
- [运行、保留和恢复手册](docs/operations.md)
- [P1 能力评估与契约](docs/p1-roadmap.md)

## License

[Apache License 2.0](LICENSE)
