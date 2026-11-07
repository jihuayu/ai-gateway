# AI Gateway

面向开发者与小型团队的模型网关和中文 Web 控制台。使用 PostgreSQL 持久化配置与账本、Redis 实时限流和计费，可选 Kafka 事件通道。

默认请求热路径仅访问内存/Redis：本机路由缓存、Redis鉴权及原子预算/日志，后台异步写PG。PG故障时预热请求可继续，受预算和有界日志容量保护。Redis需AOF always/noeviction；启动、控制台与统计仍使用PG，详见 [热路径协议](docs/hot-path.md)。

## 本地运行

要求 Node.js 24、Docker Compose。

```sh
npm ci
docker compose up -d --wait postgres redis
cp .env.example .env
# 在 .env 中设置 ENCRYPTION_KEY（openssl rand -hex 32）
npm run migrate
npm start
```

访问 http://127.0.0.1:3000 创建首个管理员。供应商需要实际 API 凭据；测试可显式配置 ALLOW_PRIVATE_UPSTREAM=true 连接本地 mock，生产默认关闭。生产使用 HTTPS 与独立存储凭据，保管好加密 key。

## 能力

- 工作空间、项目、管理员/开发者/只读权限、邀请和会话撤销。
- 加密供应商凭据、模型目标、不可变路由版本、安全主备切换、一次性 API Key。
- 文本 Chat Completions 与 SSE、参数能力校验、超时/取消/背压、请求追踪。
- Redis 原子频率与并发限制、预算预留、PG 不可变账本、幂等结算和故障重建。
- PG outbox / inbox、可选 Kafka、重试、死信、重放与用量聚合。
- 概览、供应商、模型、密钥、请求、用量、接入指南和设置控制台。

## 验证

```sh
npm run typecheck
npm test
```

未设置存储连接时，真实服务用例会标记跳过。设置 DATABASE_URL、REDIS_URL 后执行集成测试，设置 KAFKA_BROKERS 后执行 Kafka 用例。故障演练的专用容器参数与证据见 [验证报告](docs/verification.md)。

## 文档

- [产品与系统设计](docs/design.md)
- [模块实施设计](docs/implementation.md)
- [任务索引](docs/tasks.md)
- [开发目标](docs/goal.md)
- [运行、保留和恢复手册](docs/operations.md)
- [P1 能力评估与契约](docs/p1-roadmap.md)
- [验证与限制](docs/verification.md)
- [性能优化计划](docs/performance-plan.md) 与 [首轮结果](docs/performance-results.md)
- [Redis热路径与异步账本](docs/hot-path.md)
- [零SQL验收与压测结果](docs/redis-performance-results.md)

## License

[Apache License 2.0](LICENSE)
