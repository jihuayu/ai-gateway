# ai-gateway

[![License](https://img.shields.io/badge/license-Apache_2.0-blue.svg)](LICENSE)
[![Release](https://github.com/jihuayu/ai-gateway/actions/workflows/release.yml/badge.svg)](https://github.com/jihuayu/ai-gateway/actions/workflows/release.yml)

An LLM traffic gateway and financial ledger written in Java 25.

The system separates administrative state from request execution. A Spring Boot 4.2 control plane manages workspaces, routing policies, encrypted provider credentials, and audit queries in PostgreSQL. A standalone Netty 4.2 data plane serves inference traffic on virtual threads, executing authentication, rate limiting, and micro-USD budget reservation entirely in Redis without touching SQL on the request path.

---

## Design Principles

- **Strict Plane Separation**: The data plane (`NettyGatewayMain`) has no runtime dependency on Spring or PostgreSQL. It attaches directly to a durable Redis state plane (`appendonly=yes`, `appendfsync=always`, `noeviction`) and compiles to a standalone native binary via Oracle GraalVM 25.
- **Deterministic Accounting**: All costs and budgets are represented as integer micro-USD ($10^{-6}\text{ USD}$). Floating-point arithmetic is never used on financial paths.
- **Three-Phase Reservation**: Every inference request passes through three atomic Redis Lua transitions:
  1. `admitAndReserve`: Validates the API key, sliding-window RPM, concurrency slot, and project/period budget before reserving the worst-case token cost.
  2. `markDispatched`: Records upstream dispatch intent prior to network I/O so interrupted requests remain auditable.
  3. `settle` / `release`: Reconciles actual input and output token usage against the reservation, or releases the reservation in full if no upstream attempt was dispatched.
- **Asynchronous Ledger Projection**: Completed billing entries are appended to Redis Streams, drained in per-project JDBC batches to an immutable PostgreSQL `ledger` table, and folded into `usage_hourly` in $O(1)$ time via per-request delta tracking (`usage_request_state`).

---

## Architecture

```text
                  +-----------------------------------------+
                  |      Control Plane (Spring Boot 4.2)    |
                  |         Admin API & Web Console         |
                  +--------------------+--------------------+
                                       |
             Publish encrypted         |  Batch drain & O(1)
             config snapshot           |  incremental projection
                                       v
+--------+        +--------------------+--------------------+        +---------------+
| Client | -----> |         Redis 7 (AOF Durable)           | -----> | PostgreSQL 17 |
+--------+  HTTP  |  Config Snapshot / Lua Ledger / Streams |  JDBC  | Ledger & Auth |
            SSE   +--------------------+--------------------+        +---------------+
             |
             v
+-------------------------+
| Data Plane (Netty 4.2)  |
| Java 25 Virtual Threads |
| Oracle GraalVM Native   |
+------------+------------+
             |
             v
+-------------------------+
|   Upstream Providers    |
+-------------------------+
```

---

## Build and Run

Requires **Oracle GraalVM 25** (default) or **JDK 25**, **PostgreSQL 15+**, and **Redis 7**.

### 1. Start dependencies

```bash
docker compose up -d postgres redis
```

### 2. Run the full stack (Control Plane + embedded Netty Data Plane)

```bash
mvn spring-boot:run
```

- Control Plane and Console: `http://127.0.0.1:3000`
- Data Plane Gateway: `http://127.0.0.1:3001`

### 3. Compile the standalone Netty Data Plane to a native binary

```bash
mvn -Pgateway-native -DskipTests native:compile
```

This produces `./target/ai-gateway-netty`, a zero-SQL native executable:

```bash
GATEWAY_PORT=3001 \
REDIS_URL=redis://127.0.0.1:6379 \
ENCRYPTION_KEY=0000000000000000000000000000000000000000000000000000000000000001 \
./target/ai-gateway-netty
```

---

## Container Images

Two Dockerfiles are provided:

- [`Dockerfile`](Dockerfile): Full-stack image (`ghcr.io/jihuayu/ai-gateway`) running both the Control Plane (`:3000`) and Data Plane (`:3001`) on Oracle GraalVM 25.
- [`Dockerfile.gateway-native`](Dockerfile.gateway-native): Standalone native data-plane image (`ghcr.io/jihuayu/ai-gateway-netty`) compiled with `container-registry.oracle.com/graalvm/native-image:25` and packaged on `oraclelinux:9-slim`.

```bash
# Build the standalone native data-plane image locally
docker build -f Dockerfile.gateway-native -t ai-gateway-netty:native .

# Or start via Docker Compose
docker compose --profile native up --build -d gateway-native
```

Pushing a version tag matching `v*` (for example `v0.1.0`) triggers [`.github/workflows/release.yml`](.github/workflows/release.yml), which publishes the release archives to GitHub Releases and pushes both container images to GitHub Container Registry.

---

## Data Plane Endpoints

| Method | Path | Description |
| :--- | :--- | :--- |
| `GET` | `/health/live` | Process liveness probe |
| `GET` | `/health/ready` | Redis hot-ledger readiness probe |
| `GET` | `/v1/models` | List published model aliases available to the project key |
| `POST` | `/v1/chat/completions` | OpenAI-compatible chat completions (buffered, SSE streaming, tool calls, `response_format` JSON Schema, vision `image_url`) |
| `POST` | `/v1/embeddings` | OpenAI-compatible embeddings |
| `POST` | `/v1/messages` | Anthropic Messages API adapter (`x-api-key` or `Authorization: Bearer`) |

---

## Measured Characteristics

Measured on a 2-vCPU Linux environment with PostgreSQL 15, Redis 7 (`appendfsync=always`), 50 concurrent virtual threads, and 10,000+ historical ledger rows (`mvn test`):

| Metric | Value |
| :--- | :--- |
| Request-path SQL queries | `0` |
| SSE chunk-forwarding overhead (P95) | `7.75 ms` |
| 100-request batch PostgreSQL ledger drain | `460.27 ms` |
| Incremental `usage_hourly` update at 10,000+ rows | `< 15 ms` |
| Standalone Oracle GraalVM 25 native binary size | `62 MB` |

---

## Verification

```bash
mvn test
```

---

## License

Licensed under the [Apache License, Version 2.0](LICENSE).
