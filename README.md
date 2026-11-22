# ai-gateway

[![License](https://img.shields.io/badge/license-Apache_2.0-blue.svg)](LICENSE)
[![Release](https://github.com/jihuayu/ai-gateway/actions/workflows/release.yml/badge.svg)](https://github.com/jihuayu/ai-gateway/actions/workflows/release.yml)

An LLM traffic gateway and financial ledger written in Java 25.

The repository is organized as a multi-project Gradle build (Kotlin DSL + Version Catalogs) that physically separates administrative state from request execution:

- **`:gateway-common`**: Pure Java 25 domain contracts (`record`), exhaustive algebraic state machines (`sealed interface` + pattern-matching `switch`), `ScopedValue<RequestContext>` and `StructuredTaskScope` concurrency primitives, official Spring AI model metadata (`Usage`, `ChatOptions`), AES-256-GCM cryptography, and the Redis Lua hot ledger.
- **`:gateway-data-plane`**: Stateless Netty 4.2 inference gateway running on virtual threads with zero runtime dependency on Spring or PostgreSQL. Nodes form a horizontal cluster over a shared Redis 7 AOF state plane with real-time Redis Pub/Sub snapshot invalidation and compile to a standalone 49 MB native executable via Oracle GraalVM 25.
- **`:gateway-control-plane`**: Spring Boot 4.2 management platform structured with Spring Modulith, Flyway schema migrations, jOOQ type-safe SQL DSL, declarative `@HttpExchange` + `RestClient` upstream probes, and asynchronous per-project JDBC batch ledger projection into PostgreSQL.

---

## Design Principles

- **Strict Plane Separation**: `:gateway-data-plane` (`NettyGatewayMain`) carries no Servlet container, ORM, or JDBC driver on its classpath. Multiple gateway instances scale horizontally behind any L4/L7 load balancer, tagging responses with `x-gateway-node` and synchronizing routing snapshots over Redis Pub/Sub (`gateway:{ns}:hot:v1:config:notify`).
- **Deterministic Accounting**: All costs and budgets are represented as integer micro-USD ($10^{-6}\text{ USD}$). Floating-point arithmetic is never used on financial paths.
- **Exhaustive State Transitions**: Every inference request passes through three atomic Redis Lua transitions modeled as Java 25 `sealed interface` hierarchies (`AdmitOutcome` and `SettleOutcome`):
  1. `admitAndReserve`: Validates the API key, sliding-window RPM, concurrency slot, and project/period budget before reserving the worst-case token cost.
  2. `markDispatched`: Records upstream dispatch intent prior to network I/O so interrupted requests remain auditable.
  3. `settle` / `release`: Reconciles actual input and output token usage against the reservation and produces Spring AI `Usage` metadata, or releases the reservation in full if no upstream attempt was dispatched.
- **Scoped Context & Structured Concurrency**: Per-request metadata (`requestId`, `projectId`, `keyId`, `nodeId`) is bound to an immutable Java 25 `ScopedValue<RequestContext>` rather than `ThreadLocal`, propagating across virtual threads and `StructuredTaskScope` subtasks without memory leaks.
- **Asynchronous Ledger Projection**: Completed billing entries are appended to Redis Streams, drained in per-project batches to an immutable PostgreSQL `ledger` table, and folded into `usage_hourly` in $O(1)$ time via `usage_request_state`.

---

## Architecture

```text
                  +---------------------------------------------+
                  |     :gateway-control-plane (Spring Boot)    |
                  |  Spring Modulith / Flyway / jOOQ / Console  |
                  +----------------------+----------------------+
                                         |
               Publish encrypted         |  Batch drain & O(1)
               snapshot + Pub/Sub        |  incremental projection
                                         v
+--------+        +----------------------+----------------------+        +---------------+
| Client | -----> |           Redis 7 (AOF Durable)             | -----> | PostgreSQL 17 |
+--------+  HTTP  | Config Snapshot / Lua Ledger / PubSub / XADD|  jOOQ  | Ledger & Auth |
            SSE   +----------------------+----------------------+  JDBC  +---------------+
             |                           ^
             v                           |
+----------------------------------------+----+
| :gateway-data-plane Cluster (Node 1..N)     |
| Netty 4.2 / Virtual Threads / ScopedValue   |
| Oracle GraalVM 25 Native (49 MB executable) |
+----------------------+----------------------+
                       |
                       v
         +---------------------------+
         |    Upstream Providers     |
         +---------------------------+
```

---

## Toolchain and Build

Tool versions are declared in [`mise.toml`](mise.toml) and dependencies are centralized in [`gradle/libs.versions.toml`](gradle/libs.versions.toml). Code formatting is enforced via Spotless.

Requires **Oracle GraalVM 25** (default) or **JDK 25**, **PostgreSQL 15+**, and **Redis 7**.

### 1. Start dependencies

```bash
docker compose up -d postgres redis
```

### 2. Run the Control Plane (with embedded or standalone Data Plane)

```bash
./gradlew :gateway-control-plane:bootRun
```

- Control Plane and Console: `http://127.0.0.1:3000`
- Data Plane Gateway: `http://127.0.0.1:3001`

### 3. Compile the standalone Netty Data Plane to a native binary

```bash
./gradlew :gateway-data-plane:nativeCompile
```

This produces `./gateway-data-plane/build/native/nativeCompile/ai-gateway-netty`, a zero-SQL native executable:

```bash
GATEWAY_NODE_ID=gateway-node-1 \
GATEWAY_PORT=3001 \
REDIS_URL=redis://127.0.0.1:6379 \
ENCRYPTION_KEY=0000000000000000000000000000000000000000000000000000000000000001 \
./gateway-data-plane/build/native/nativeCompile/ai-gateway-netty
```

---

## Container Images and Cluster Deployment

Two Dockerfiles are provided:

- [`Dockerfile`](Dockerfile): Control-plane / full-stack image (`ghcr.io/jihuayu/ai-gateway`) built from `:gateway-control-plane:bootJar` on Oracle GraalVM 25.
- [`Dockerfile.gateway-native`](Dockerfile.gateway-native): Standalone native data-plane image (`ghcr.io/jihuayu/ai-gateway-netty`) compiled from `:gateway-data-plane:nativeCompile` with `container-registry.oracle.com/graalvm/native-image:25` and packaged on `oraclelinux:9-slim`.

```bash
# Build the standalone native data-plane image locally
docker build -f Dockerfile.gateway-native -t ai-gateway-netty:native .

# Start separated Control Plane (:3000) + 2-node Native Netty Gateway Cluster (:3001, :3002)
docker compose --profile cluster up --build -d
```

Pushing a version tag matching `v*` (for example `v0.2.0`) triggers [`.github/workflows/release.yml`](.github/workflows/release.yml), which verifies formatting, publishes release archives to GitHub Releases, and pushes both container images to GitHub Container Registry.

---

## Data Plane Endpoints

| Method | Path | Description |
| :--- | :--- | :--- |
| `GET` | `/health/live` | Process liveness probe (includes `node_id` and `x-gateway-node` header) |
| `GET` | `/health/ready` | Redis hot-ledger readiness probe |
| `GET` | `/v1/models` | List published model aliases available to the project key |
| `POST` | `/v1/chat/completions` | OpenAI-compatible chat completions (buffered, SSE streaming, tool calls, `response_format` JSON Schema, vision `image_url`) |
| `POST` | `/v1/embeddings` | OpenAI-compatible embeddings |
| `POST` | `/v1/messages` | Anthropic Messages API adapter (`x-api-key` or `Authorization: Bearer`) |

---

## Measured Characteristics

Measured on a 2-vCPU Linux environment with PostgreSQL 15, Redis 7 (`appendfsync=always`), 50 concurrent virtual threads, and 10,000+ historical ledger rows (`./gradlew test`):

| Metric | Value |
| :--- | :--- |
| Request-path SQL queries | `0` |
| SSE chunk-forwarding overhead (P95) | `7.75 ms` |
| 100-request batch PostgreSQL ledger drain | `460.27 ms` |
| Incremental `usage_hourly` update at 10,000+ rows | `< 15 ms` |
| Standalone Oracle GraalVM 25 native binary size | `49 MB` |

---

## Verification

```bash
./gradlew check
```

---

## License

Licensed under the [Apache License, Version 2.0](LICENSE).
