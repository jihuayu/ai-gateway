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

默认请求协议见 [Redis热路径设计](hot-path.md)。Redis 7须允许Lua/INFO/Streams及启动策略检查，并配置AOF always/noeviction；Compose已采用该策略。DATA_PLANE_NAMESPACE需长期稳定，一个PG schema只允许一个财务owner。HOT_DURABILITY=memory仅用于开发测试，不能据此宣称零丢单。

## 事件通道

默认随网关运行Redis Stream→PG投影及PG outbox消费，后台独立4连接池。额外 `npm run worker` 进程也使用最多4连接；应用每实例前台12+后台4，总连接数按部署实例累计预算。Kafka 开发节点：`docker compose --profile kafka up -d kafka`，配置 KAFKA_BROKERS=127.0.0.1:9092 后启动应用。生产 Kafka 使用至少三个 broker，KAFKA_REPLICATION_FACTOR=3，并在 broker/topic 配置 min.insync.replicas=2；单节点 profile 仅用于开发。

财务Stream消费者先提交PG事务，再XACK/XDEL；出错保留原记录，不静默丢弃或DLQ后确认。PG outbox的后续消费者采用inbox去重和PG事务，Kafka offset在处理成功后提交。非法事件进入 dead_letters；投递重试耗尽进入失败 outbox。重放使用 EventWorker.replay(eventId)，保持原 event_id；禁止在去重标记保留期外直接重放，先对账。观察 outbox 最早未完成时间、失败数、Kafka lag 和数据库空间。

## 故障处理

- PG不可用：预热后的网关继续使用Redis鉴权、路由及计费，直到预算/限流/日志容量限制；控制面、统计和新启动仍受PG影响。恢复后后台幂等补账，执行结果未知不能按免费处理。
- Redis不可用、run_id变化或状态/日志丢失：拒绝新调用并隔离旧主。当前版本不自动重新绑定身份，也不从可能滞后的PG直接重建放行。须确认完整AOF/复制源、日志连续性和未终结请求，再进行受控恢复；禁止删除manifest、变更命名空间或归零预算绕过围栏。
- Kafka 不可用：已持久化 outbox 保留并退避重试；监测积压和空间，必要时暂停准入。不要手工删除未发布计费事件。
- 流中断：检查 request_id 的 attempt 和状态。已输出内容后不自动备用，客户端需处理 SSE error，不能只用 HTTP 200 判定完整成功。
- 凭据加密 key 丢失：供应商凭据无法解密，需重新配置；恢复数据库备份时必须同时具备对应加密 key，密钥备份与数据库备份分开管理。

可用 `npm run project-journal` 将已知保留的日志补入PG：它验证manifest/epoch和Stream尾游标，提交后才确认记录，**不修改准入身份、不自动放行**。此命令不能证明发生副本丢写前的完整性；仍须独立验证AOF/复制来源并停止旧写入者。

## 备份与保留

定期执行 PostgreSQL `pg_dump` 自定义格式并加密保存，另存部署密钥。恢复到独立数据库后应用迁移，同时保留匹配的Redis AOF/Stream与manifest，核对项目、ledger、实时预算和未结束预留；PG备份单独不足以恢复尚未投影费用。不要覆盖在线数据库做恢复演练。

`node --env-file=.env --import tsx scripts/retention.ts` 清理已终结请求元数据（默认30天）；不会清理账本或待核对请求。hot_projection_state墓碑不随元数据删除，防止旧事件重放复活记录。outbox 已发布记录30天、inbox31天，Kafka 保留窗口不得长于可去重重放窗口。更长时间的历史重放必须先设计对账方案。

## 验证命令

`npm run typecheck` 与 `npm test`；测试环境设置 DATABASE_URL 和 REDIS_URL 运行真实存储集成测试。KAFKA_BROKERS 指定本地 broker 后额外运行 Kafka 用例。测试使用独立随机 PG schema，避免删除业务数据。

压测：配置专用测试项目与 BENCH_API_KEY 后运行 `node --import tsx scripts/benchmark.ts`。BENCH_CONCURRENCY 默认100。压测会消耗供应商用量，建议使用测试上游；输出总响应耗时，不冒充纯网关开销。正式性能验收需同时记录无网关基线与资源配置。

专用新协议AOF演练：创建命名为gateway-hot-restart-*的独立Redis AOF always容器，设置对应REDIS_URL及HOT_RESTART_CONTAINER，单独运行hot-path.test.ts中“hot Redis AOF restart”用例；不要指向业务Redis。旧recovery/failover用例验证兼容性PG计费协议，不能作为新协议自动恢复放行的证据。

## 财务日志阻塞处理

无效schema、缺失关系或不一致快照会保留未确认财务记录，并可能阻塞后续投影；容量最终会关闭新准入。监控Stream长度、slots预留、PG outbox及后台错误，不能只看XLEN，unknown也保留未来更正容量。

处理时先停止相关写入者并保留原记录/来源证据，核对version、epoch、revision和依赖实体。修复写入器/兼容解码器或恢复必要关系后，用相同事件重试投影；事务提交前不XACK、不XDEL，不手工把费用改成0。`npm run project-journal`可重试保留事件，原消费者中断的pending需等待30秒接管窗口。无法证明来源完整或无法修复时保持准入关闭并做受控财务核对；当前没有跳过毒消息继续放行的隔离工具。
