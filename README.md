# AI Gateway — 为智能时代而生的现代 Java 高性能大模型网关

[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://www.apache.org/licenses/LICENSE-2.0)
[![Java](https://img.shields.io/badge/Java-25_LTS_(Virtual_Threads)-ED8B00.svg)](https://www.oracle.com/java/technologies/downloads/)
[![GraalVM](https://img.shields.io/badge/Oracle_GraalVM-25_Native_Image-F80000.svg)](https://www.graalvm.org/)
[![Netty](https://img.shields.io/badge/Data_Plane-Netty_4.2-009688.svg)](https://netty.io/)
[![Spring Boot](https://img.shields.io/badge/Control_Plane-Spring_Boot_4.2-6DB33F.svg)](https://spring.io/projects/spring-boot)

> **“把每一次 Token 流转当成金融级账本对待，把每一毫秒网关开销压缩到物理极限。”**
>
> 当大模型推理从实验室走向大规模生产，AI 网关不再只是一层简单的反向代理——它是算力预算的守门人、多云模型路由的调度中枢、以及高并发 SSE 流式响应的高速通道。
>
> **AI Gateway** 采用**现代 Java 25 LTS 虚拟线程（Project Loom）**与**控制面 / 数据面彻底解耦**的双引擎架构：管理平台基于 **Spring Boot 4.2** 构建，提供直观的多租户与路由编排控制台；数据面网关直连 **纯 Netty 4.2 + Redis 原子热账本**，热路径 **Zero-SQL（零数据库访问）**，并原生支持通过 **Oracle GraalVM 25** 一键编译为独立原生二进制与超轻量 Distroless 容器镜像。

---

## ✨ 为什么选择 AI Gateway？

### 1. 控制面与数据面彻底解耦：纯 Netty 4.2 + JDK 25 虚拟线程
- **直通式数据面（Pure Netty Data Plane）**：数据面网关完全绕过传统 Servlet 容器，由 `NettyGatewayServer` 直接承载 HTTP/1.1 与 SSE（Server-Sent Events）流式帧传输。每一个下游请求在独立的 **JDK 25 虚拟线程**中执行，兼具异步非阻塞网络 I/O 的高吞吐与同步直观的代码可读性。
- **独立零 SQL 启动（`NettyGatewayMain`）**：数据面网关支持脱离 Spring 容器与 PostgreSQL 独立运行，仅需连接 Redis 热状态平面即可处理全量流量。
- **Oracle GraalVM 25 原生编译**：支持一条命令编译为 `ai-gateway-netty` 原生可执行文件（内嵌 CycloneDX SBOM），实现毫秒级冷启动与极低内存占用，天生契合 Serverless 与 Kubernetes 弹性扩缩容。

### 2. 热路径 Zero-SQL + 金融级微美元（micro-USD）精确计费
- **微美元整数精度**：所有账单与预算均以 `$10^{-6}\text{ USD}$（`micro-USD`）整数运算，彻底杜绝浮点数舍入误差。
- **Redis Lua 原子三段式状态机**：
  - `admitAndReserve`：一次 Lua 调用完成 API Key 鉴权、滑动窗口 RPM 限流、并发槽位占用、月度/项目预算预扣以及快照代际（Snapshot Generation）锁定。
  - `markDispatched`：上游请求发出前原子标记派发意图，确保网络异常或崩溃场景下的账务可追溯。
  - `settle` / `release`：精确对账输入/输出 Token 成本（差额多退少补），未派发请求全额释放预留额度。
- **异步批量投影与 $O(1)$ 增量聚合**：账务日志通过 Redis Streams 持久化队列由 `HotProjector` 按项目分组批量落库至 PostgreSQL，并通过 `usage_request_state` 实现 $O(1)$ 增量更新 `usage_hourly` 报表——即便在千万级历史账本下，单笔结算增量聚合延迟依然稳定在毫秒级。

### 3. 全协议栈与企业级安全防护
- **多协议无缝兼容**：
  - OpenAI 兼容接口：`GET /v1/models`、`POST /v1/chat/completions`（含 SSE 流式、`stream_options.include_usage`、`tools` / `tool_choice` 函数调用、`response_format` JSON Schema 校验、Vision 多模态输入）、`POST /v1/embeddings`。
  - Anthropic 原生兼容接口：`POST /v1/messages`（原生支持 `x-api-key` 认证与 Anthropic 响应格式转换）。
- **纵深安全边界**：
  - **SSRF 与 DNS 重绑定防御**：校验上游端点与 Vision `image_url`，拦截 RFC1918 私网、链路本地、云元数据地址（`169.254.169.254`）及重定向跳板。
  - **主密钥信封加密**：上游供应商 API Key 采用 **AES-256-GCM**（独立随机 IV + Auth Tag）加密存储；租户密码采用 **Scrypt** 哈希，控制台内置 HttpOnly 会话与双提交 Cookie CSRF 防护。

---

## 🏗️ 系统架构

```text
                    +-------------------------------------------------------+
                    |             Web Console / Admin REST API              |
                    |        Spring Boot 4.2 Control Plane (:3000)          |
                    +--------------------------+---------------------------+
                                                |
                       1. Publish Encrypted     |  3. Batch Drain & O(1)
                          Config Snapshot       |     Incremental Projection
                                                v
+------------------+                  +-------------------+                  +--------------------+
|  AI SDK / Agent  |  HTTP / SSE      |   Redis 7 (AOF)   |   JDBC Batch     |   PostgreSQL 17    |
|  OpenAI / Claude | ---------------> |  - Config Snapshot| ---------------> |  - Workspaces/Keys |
+------------------+  Netty (:3001)   |  - Lua Hot Ledger |   HotProjector   |  - Immutable Ledger|
                      Virtual Threads |  - Redis Streams  |   & EventWorker  |  - usage_hourly    |
                      Zero-SQL Path   +-------------------+                  +--------------------+
                            |
                            |  2. Priority Failover + Circuit Breaker + SSRF Guard
                            v
              +---------------------------+
              | Upstream LLM Providers    |
              | (OpenAI / Anthropic / vLLM)|
              +---------------------------+
```

---

## ⚡ 实测性能基准（Benchmark）

在固定资源（2 vCPU、PostgreSQL 15+、Redis 7 `appendfsync=always`、50 并发虚拟线程、100 笔并发推理请求、10,000+ 历史账本记录）下的端到端实测表现：

| 核心指标 | 实测结果 | 架构收益说明 |
| :--- | :--- | :--- |
| **数据面热路径 SQL 查询数** | **`0` 次 / 请求** | 鉴权、路由、限流、预算预扣与结算全部在内存与 Redis Lua 中完成 |
| **SSE 流式单帧转发延迟（P95）** | **`7.75 ms`**（目标 `< 20 ms`） | Netty `DefaultHttpContent` 零缓冲直推，支持客户端断连毫秒级感知 |
| **100 并发请求批量 PG 账本落库耗时** | **`460.27 ms` – `483.40 ms`** | 相比逐请求落库基线（`7805.74 ms`）耗时降低 **93.8%+** |
| **10,000+ 历史账本下单笔增量聚合** | **`< 15 ms`** | 基于 `usage_request_state` 增量幂等抵消，彻底告别全表重扫 |
| **Oracle GraalVM 25 原生二进制体积** | **`62 MB`** | 独立零 Spring 依赖二进制（`target/ai-gateway-netty`），毫秒级冷启动 |

---

## 🚀 快速开始

### 1. 启动基础设施（PostgreSQL & Redis AOF）

```bash
docker compose up -d postgres redis
```

### 2. 启动全栈服务（Spring Boot 控制面 + 内嵌 Netty 数据面）

要求 **Oracle GraalVM 25**（推荐默认）或 **JDK 25 LTS**：

```bash
mvn spring-boot:run
```

- **管理控制台与 Control Plane API**：`http://127.0.0.1:3000`
- **Netty 数据面网关 API**：`http://127.0.0.1:3001`

---

## 🔥 Oracle GraalVM 25 原生编译与独立网关镜像

数据面网关提供独立的零 Spring、零 SQL 启动入口 `com.jihuayu.aigateway.netty.NettyGatewayMain`，默认针对 **Oracle GraalVM 25** 优化。

### 单条指令编译原生可执行文件

```bash
mvn -Pgateway-native -DskipTests native:compile
```

编译完成后将生成独立原生二进制文件 `./target/ai-gateway-netty`，可直接连接 Redis 热状态平面运行：

```bash
GATEWAY_PORT=3001 \
REDIS_URL=redis://127.0.0.1:6379 \
ENCRYPTION_KEY=0000000000000000000000000000000000000000000000000000000000000001 \
./target/ai-gateway-netty
```

### 独立构建网关原生 Docker 镜像

项目提供专门的数据面原生多阶段构建文件 [`Dockerfile.gateway-native`](Dockerfile.gateway-native)（基于 `container-registry.oracle.com/graalvm/native-image:25` 编译，运行时采用超轻量 `gcr.io/distroless/base-debian12:nonroot`）：

```bash
# 单条指令构建独立原生网关镜像
docker build -f Dockerfile.gateway-native -t ai-gateway-netty:native .

# 或通过 Docker Compose native profile 一键拉起独立原生网关容器
docker compose --profile native up --build -d gateway-native
```

---

## 📡 数据面网关 API 速览

### 1. OpenAI 兼容聊天补全（支持 SSE、Tool Calling、JSON Schema、Vision）

```bash
curl -X POST http://127.0.0.1:3001/v1/chat/completions \
  -H "Authorization: Bearer agw_your_project_key" \
  -H "Content-Type: application/json" \
  -d '{
    "model": "support-chat",
    "messages": [{"role": "user", "content": "你好，请介绍一下现代 Java 虚拟线程的优势"}],
    "stream": true,
    "stream_options": {"include_usage": true}
  }'
```

### 2. OpenAI 兼容向量嵌入（Embeddings）

```bash
curl -X POST http://127.0.0.1:3001/v1/embeddings \
  -H "Authorization: Bearer agw_your_project_key" \
  -H "Content-Type: application/json" \
  -d '{
    "model": "text-embed",
    "input": ["AI Gateway built with Java 25 and Netty 4.2"]
  }'
```

### 3. Anthropic Messages 原生接口（`/v1/messages`）

```bash
curl -X POST http://127.0.0.1:3001/v1/messages \
  -H "x-api-key: agw_your_project_key" \
  -H "Content-Type: application/json" \
  -d '{
    "model": "agent-pro",
    "max_tokens": 512,
    "system": "You are a helpful architecture assistant.",
    "messages": [{"role": "user", "content": "Explain zero-SQL hot-path billing."}]
  }'
```

---

## 🧪 运行全链路集成与基准测试

测试套件涵盖身份认证、RBAC 权限隔离、API Key 生命周期、SSRF 阻断、三段式微美元账本结算、Redis 故障恢复演练、P1 多协议扩展、10,000+ 历史账本 `EXPLAIN ANALYZE` 校验、SSE 延迟基准以及 **Oracle GraalVM 原生二进制端到端调用**：

```bash
mvn test
```

---

## 📄 开源协议（License）

本项目采用 **[Apache License, Version 2.0](LICENSE)** 开源协议。欢迎自由使用、修改与分发，期待与你一起构建下一代高性能 AI 基础设施。
