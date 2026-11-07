# 运行与恢复手册

## 首次启动

要求 Node.js 24、npm、Docker Compose。使用 `npm ci` 安装依赖，`docker compose up -d --wait postgres redis` 启动本地存储。

复制 `.env.example` 到 `.env`，使用 `openssl rand -hex 32` 生成 ENCRYPTION_KEY；不要把真实 key 提交到 Git。然后执行：

```sh
node --env-file=.env --import tsx src/migrate.ts
node --env-file=.env --import tsx src/server.ts
```

管理界面默认 http://127.0.0.1:3000 。首次创建管理员后初始化入口关闭。生产部署设置 NODE_ENV=production、HTTPS 终止和正确的 HOST；首次初始化期间限制访问。Compose 里的密码仅用于本地开发，生产需使用独立凭据、网络隔离、备份与持久卷。

ALLOW_PRIVATE_UPSTREAM 默认关闭；测试内网 mock 时才显式开启。开启即允许管理员配置内网模型地址，请限制管理权限。

## 事件通道

默认随网关运行 PG outbox 消费。需要额外吞吐时可运行 `npm run worker` 进程。Kafka 开发节点：`docker compose --profile kafka up -d kafka`，配置 KAFKA_BROKERS=127.0.0.1:9092 后启动应用。生产 Kafka 使用至少三个 broker，KAFKA_REPLICATION_FACTOR=3，并在 broker/topic 配置 min.insync.replicas=2；单节点 profile 仅用于开发。

消费者采用 inbox 去重和 PG 事务，Kafka offset 在处理成功后提交。非法事件进入 dead_letters；投递重试耗尽进入失败 outbox。重放使用 EventWorker.replay(eventId)，保持原 event_id；禁止在去重标记保留期外直接重放，先对账。观察 outbox 最早未完成时间、失败数、Kafka lag 和数据库空间。

## 故障处理

- PG 不可用：新鉴权与计费请求停止；恢复后先检查未终结 reservations、ledger 与 outbox，不能将执行结果未知的请求当免费调用。
- Redis 不可用或状态丢失：暂停新调用、隔离旧主。恢复任务从 PG 账本和未结束意图重建 generation/预算，恢复后核对账本。禁止仅清空 Redis 后立即放行。
- Kafka 不可用：已持久化 outbox 保留并退避重试；监测积压和空间，必要时暂停准入。不要手工删除未发布计费事件。
- 流中断：检查 request_id 的 attempt 和状态。已输出内容后不自动备用，客户端需处理 SSE error，不能只用 HTTP 200 判定完整成功。
- 凭据加密 key 丢失：供应商凭据无法解密，需重新配置；恢复数据库备份时必须同时具备对应加密 key，密钥备份与数据库备份分开管理。

## 备份与保留

定期执行 PostgreSQL `pg_dump` 自定义格式并加密保存，另存部署密钥。恢复到独立数据库后应用迁移，并核对项目、ledger 金额和未结束预留，再在隔离 Redis 中重建；不要覆盖正在服务的数据库做恢复演练。

`node --env-file=.env --import tsx scripts/retention.ts` 清理已终结请求元数据（默认30天）；不会清理账本或待核对请求。outbox 已发布记录30天、inbox31天，Kafka 保留窗口不得长于可去重重放窗口。更长时间的历史重放必须先设计对账方案。

## 验证命令

`npm run typecheck` 与 `npm test`；测试环境设置 DATABASE_URL 和 REDIS_URL 运行真实存储集成测试。KAFKA_BROKERS 指定本地 broker 后额外运行 Kafka 用例。测试使用独立随机 PG schema，避免删除业务数据。

压测：配置专用测试项目与 BENCH_API_KEY 后运行 `node --import tsx scripts/benchmark.ts`。BENCH_CONCURRENCY 默认100。压测会消耗供应商用量，建议使用测试上游；输出总响应耗时，不冒充纯网关开销。正式性能验收需同时记录无网关基线与资源配置。
