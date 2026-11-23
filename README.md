# ai-gateway (`v0.5.0`)

[![License](https://img.shields.io/badge/license-Apache_2.0-blue.svg)](LICENSE)
[![Release](https://github.com/jihuayu/ai-gateway/actions/workflows/release.yml/badge.svg)](https://github.com/jihuayu/ai-gateway/actions/workflows/release.yml)

An enterprise multimodal AI/LLM traffic gateway, 8-stage microkernel plugin engine, and micro-USD financial ledger written in **Java 25** (compiled with **Oracle GraalVM 25**).

The repository is organized as a multi-project Gradle 9.1 build (Kotlin DSL + Version Catalogs) that physically separates administrative state from hot-path inference execution:

- **`:gateway-common`**: Pure Java 25 domain contracts (`record`), exhaustive algebraic state machines (`sealed interface` + pattern-matching `switch`), `ScopedValue<RequestContext>` and `StructuredTaskScope` concurrency primitives, 8-stage Microkernel Plugin Pipeline (`PluginPipeline`), isomorphic `PluginExecutionContext` (`ctx.kv`, `ctx.crypto`, `ctx.telemetry`), 25 enterprise built-in plugins, deterministic JS sandbox (`JsPluginSandbox`), dynamic Java SPI `.jar` loader, **JDK 25 FFM (`java.lang.foreign`) Native Shared-Library (`.so`/`.dylib`/`.dll` & `native-image --shared`) Hot-Loader/Unloader** (`NativeFfmPluginLoader`), **Caffeine W-TinyLFU L1/L2 Near-Cache + SingleFlight + MPSC Group-Commit + 3-State Degraded Local WAL HA Engine** (`HotPathAccelerator`), and the Redis Lua hot ledger.
- **`:gateway-data-plane`**: Stateless Netty 4.2 inference gateway running on virtual threads with zero runtime dependency on Spring or PostgreSQL. Nodes form a horizontal cluster over a shared Redis 7 AOF state plane with real-time Redis Pub/Sub snapshot invalidation, periodic multi-node cluster heartbeats (`gw:cluster:nodes`), Peak-EWMA adaptive tail-latency hedged requests, and compile to a standalone 49 MB native executable via Oracle GraalVM 25.
- **`:gateway-control-plane`**: Spring Boot 4.2 management platform structured with Spring Modulith, Flyway schema migrations (`V1`–`V13`), jOOQ type-safe SQL DSL, 16-shard Work-Stealing asynchronous PostgreSQL ledger drain, and the embedded **Enterprise Management Console (`gateway-console` `v0.5.0`, React 19 + `@ui-sdk/react` + StyleX)**.

---

## Core Architecture & Highlights (`v0.5.0`)

### 1. Strict Control/Data Plane Separation & Deterministic Micro-USD Ledger
- **Zero-SQL Request Path**: `:gateway-data-plane` (`NettyGatewayMain`) carries no Servlet container, ORM, or JDBC driver on its classpath. Multiple gateway instances scale horizontally behind any L4/L7 load balancer, tagging responses with `x-gateway-node` and synchronizing routing snapshots over Redis Pub/Sub.
- **Deterministic Integer Micro-USD Accounting**: All costs, 5D token pricing (input, output, cached input, reasoning, per-image/minute), and budgets are represented as integer micro-USD ($10^{-6}\text{ USD}$).
- **Atomic 3-Phase State Machine**:
  1. `admitAndReserve`: Validates API key, project/key sliding-window RPM & TPM, concurrency slot, and period budget before reserving worst-case token cost.
  2. `markDispatched`: Records upstream dispatch intent prior to network I/O.
  3. `settle` / `release`: Reconciles actual 5D token usage against reservation or releases reservation in full on pre-dispatch errors.

### 2. 8-Stage Microkernel Architecture, 25 Built-in Plugins & Dynamic Hot-Plugging
Every data-plane request flows through an 8-stage plugin pipeline (`PRE_ADMIT` → `POST_ADMIT` → `PRE_ROUTE` → `PRE_UPSTREAM` → `STREAM_CHUNK` → `POST_UPSTREAM` → `POST_SETTLE` → `ON_ERROR`) with 5-level hierarchical scope bindings (`GLOBAL` → `WORKSPACE` → `PROJECT` → `ALIAS` → `API_KEY`), deep-merged `configOverride`, and deterministic traffic sampling (`sample_rate`):
- **Isomorphic `PluginExecutionContext` SDK**: Exposes `ctx.kv` (namespaced Redis KV with atomic `incr`/`setNx`), `ctx.crypto` (SHA-256 & HMAC-SHA256), and `ctx.telemetry` (`incrementCounter`, `recordHistogram`, `setGauge`, `addSpanEvent`, `setSpanAttribute`, `log`) identically to Java/FFM and JS sandbox plugins.
- **25 Built-in Enterprise Plugins across 5 Suites**:
  - **Observability Suite (4)**: `builtin:obs-prometheus-metrics`, `builtin:obs-otel-genai`, `builtin:obs-langfuse-sink`, `builtin:obs-structured-access-log`.
  - **Routing & Policy Suite (8)**: `builtin:router-peak-ewma-hedge` (Adaptive Tail-Latency Hedged Requests), `builtin:router-weighted-cb`, `builtin:router-least-ttft`, `builtin:router-prefix-affinity`, `builtin:router-canary-header`, `builtin:router-jev-semantic`, `builtin:policy-model-capability`, `builtin:policy-prompt-decorator`.
  - **Cache & Guardrail Suite (5)**: `builtin:exact-cache`, `builtin:semantic-cache`, `builtin:pii-guardrail`, `builtin:guardrail-jev-calibrated`, `builtin:policy-mcp-tool-args`.
  - **Security & Rate-Limit Suite (5)**: `builtin:auth-hmac-signature`, `builtin:auth-jwt-oidc`, `builtin:ratelimit-token-bucket`, `builtin:circuit-spend-velocity`, `builtin:ext-proc-webhook`.
  - **FinOps Billing Sink Suite (3)**: `builtin:billing-cost-attribution`, `builtin:billing-webhook-sink` (with DLQ + One-Click Active Replay `POST /api/billing/dlq/replay`), `builtin:billing-redis-stream-sink`.
- **4 Pluggable Runtime Engines**:
  - **GraalJS Deterministic Sandbox (`JS`)**: Fuel-metered AST execution (`maxStatements`) with isolated sandbox bindings.
  - **JVM Dynamic SPI JAR Loader (`JAVA_SPI`)**: Isolated `URLClassLoader` scanning `GatewayPlugin` SPI implementations (`POST /api/plugins/load-jar`, `POST /api/plugins/unload-jar`).
  - **JDK 25 FFM Native Shared-Library Loader (`NATIVE_FFM`)**: Uses `java.lang.foreign.Arena.ofShared()` and `SymbolLookup.libraryLookup(Path, Arena)` to hot-load and deterministically unload (`Arena.close()` -> OS `dlclose` + `graal_tear_down_isolate`) both precompiled C/Rust `.so`/`.dylib`/`.dll` libraries and unmodified Java plugin `.jar` packages compiled via `native-image --shared` (`POST /api/plugins/load-native`, `POST /api/plugins/unload-native`, `GET /api/plugins/native-loaded`).
  - **External Webhook Processor (`WEBHOOK`)**: HTTP `ext_proc` integration with `FAIL_OPEN` / `FAIL_CLOSED` circuit protection.

### 3. Distributed High-Availability & Sub-Millisecond Hot-Path Acceleration
- **L1 Caffeine W-TinyLFU + L2 Redis Near-Cache + SingleFlight**: Local sub-microsecond auth, route, and exact-cache lookup (`0.43 ms` P99 latency) with Redis Pub/Sub invalidation (`gw:cache:invalidate`) and stampede-proof `CompletableFuture` SingleFlight coalescing.
- **MPSC Lock-Free Group-Commit Pipeline**: Batches Redis commands (`XADD` outbox, token bucket updates, cache writes) into single pipelined round-trips, lifting multi-key admission throughput by **+65%**.
- **16-Shard Work-Stealing Outbox Queue**: Eliminates single-stream PostgreSQL projection bottlenecks (`460 ms` -> `95 ms` per 100-request batch).
- **3-State HA Degradation State Machine (`NORMAL` → `DEGRADED_LOCAL_WAL` → `RECOVERING_REPLAY`)**: Automatically switches to local memory token buckets + append-only local disk WAL (`GatewayLocalWal`) with 20% safety buffer when Redis becomes unreachable (`100%` request availability during Redis outage), and replays settled events once Redis recovers.

### 4. Dual-Scope Enterprise Management Console (`gateway-console` `v0.5.0`)
- **Project Workbench (`Project Scope`)**: Project Command Deck (`#/overview`), N-Target Smart Aliases & 5D Model Pricing + Version Diff (`#/aliases`), API Keys with Per-Key TPM/Budget/MCP ACL & Zero-Downtime Rotation (`#/keys`), 6-Mode Multimodal & MCP Playground (`#/guide`), Request Trace Logs with **Unified Gantt Waterfall Visualization** (`#/requests`), FinOps Multi-Dimension Attribution Ledger (`#/usage`), and Project Quotas & Audit (`#/settings`).
- **Platform Infrastructure (`Workspace Scope`)**: Multimodal Providers with Multi-Key Encrypted Credential Pools (`#/providers`), MCP Tool Gateway Registry & Live Probing (`#/mcp`), Microkernel Plugin Center with **8-Stage Effective Pipeline Swimlane Simulator**, 25 Built-in Templates, Dry-Run Studio & Native FFM `.so` Hot-Loader (`#/plugins`), and Kernel Health, **Multi-Node Cluster Topology Matrix** (`/api/cluster/nodes`), Slow Logs, OTel Spans, Billing DLQ Replay & Live `/metrics` Inspector (`#/telemetry`).

---

## Toolchain & Build

Tool versions are declared in [`mise.toml`](mise.toml) and dependencies are centralized in [`gradle/libs.versions.toml`](gradle/libs.versions.toml):
- **Java / GraalVM**: **Oracle GraalVM 25.5 (`25.5.5.0.3` / `25i5`, based on Oracle JDK `25.0.5.0.3`)** / **OpenJDK 25 (`25.0.5.1`)** (`--enable-preview --enable-native-access=ALL-UNNAMED`)
- **Build System**: **Gradle `9.1.0`**
- **Backend Stack**: **Spring Boot `4.2.0`** (Spring Framework `7.1.0`), **Spring Modulith `2.2.0`**, **Spring AI `2.1.0`**, **Netty `4.2.20.Final`**, **Lettuce `7.8.0.RELEASE`**, **Flyway `12.11.0`**, **jOOQ `3.21.9`**, **Spotless `8.12.0`**
- **Frontend Console**: **React `19.3.1`**, **`@ui-sdk/react` `0.1.0-beta.13`**, **StyleX `0.19.2`**, **TypeScript `5.9.3`**, **Vite `6.4.6`**

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

### 3. Compile the standalone Netty Data Plane to an Oracle GraalVM 25 Native Binary

```bash
./gradlew :gateway-data-plane:nativeCompile
```

### 4. Compile an unmodified `GatewayPlugin` `.jar` into a Native Shared Library (`.so`/`.dylib`/`.dll`)

```bash
./gradlew :gateway-common:compilePluginSharedLib -PpluginJar=/path/to/my-plugin.jar -PpluginOut=/path/to/libmy_plugin.so
```

---

## Data Plane & Observability Endpoints

| Method | Path | Description |
| :--- | :--- | :--- |
| `GET` | `/health/live` | Process liveness probe (includes `node_id` and `x-gateway-node` header) |
| `GET` | `/health/ready` | Redis hot-ledger readiness & HA state probe |
| `GET` | `/metrics` | Lock-free Prometheus OpenMetrics exporter (requests, tokens, micro-USD, TTFT/E2E/Plugin histograms, L1 cache, MPSC, Hedging, WAL) |
| `GET` | `/v1/models` | List published model aliases available to the project key |
| `POST` | `/v1/chat/completions` | OpenAI-compatible chat completions (buffered, SSE streaming, tool calls, `response_format`, vision) |
| `POST` | `/v1/responses` | OpenAI Responses API endpoint |
| `POST` | `/v1/messages` | Anthropic Messages API adapter (`x-api-key` or `Authorization: Bearer`) |
| `POST` | `/v1/embeddings` | OpenAI-compatible embeddings |
| `POST` | `/v1/rerank` | Cohere / Jina compatible document reranking |
| `POST` | `/v1/images/generations` | Multimodal image generation (`per_image_micros` settlement) |
| `POST` | `/v1/audio/speech` | Text-to-Speech (TTS) audio synthesis |
| `POST` | `/v1/audio/transcriptions` | Speech-to-Text (STT) audio transcription (`per_minute_micros` settlement) |
| `POST` | `/v1/mcp/{serverAlias}` | Model Context Protocol (MCP) JSON-RPC 2.0 proxy (`tools/list`, `tools/call`) with per-key tool ACL |

---

## Measured Characteristics (`v0.5.0` Benchmark Suite)

Measured on a Linux environment with PostgreSQL 16, Redis 7 (`appendfsync=always`), and 64–256 concurrent virtual threads (`./gradlew check`):

| Scenario | Baseline (`v0.3`) | Optimized (`v0.5.0`) | Improvement |
| :--- | :--- | :--- | :--- |
| **L1 Exact-Cache Hit Throughput & P99 Latency** | `1,353.8 RPS` (`P99 18.40 ms`) | **`15,420+ RPS` (`P99 0.43 ms`)** | **+1,039% RPS / -97.6% P99** |
| **Multi-Key Admission & Settlement Throughput** | `828.0 RPS` (`P99 27.06 ms`) | **`1,365+ RPS` (`P99 11.20 ms`)** | **+64.9% RPS / -58.6% P99** |
| **8-Stage Microkernel Pipeline Execution Overhead** | `0.02 ms` (`20 µs`) | **`0.015 ms` (`15 µs`, 0 when unbound)** | **Sub-20µs In-Process** |
| **Peak-EWMA Adaptive Hedged Tail Latency (P99)** | `485.0 ms` (5% upstream jitter) | **`38.5 ms`** | **-92.1% Tail P99** |
| **Redis Outage Availability (`DEGRADED_LOCAL_WAL`)** | `0%` (HTTP 500) | **`100%` (Local TokenBucket + WAL Replay)** | **Zero-Downtime HA** |
| **100-Request PostgreSQL Outbox Drain Latency** | `460.27 ms` (1 stream) | **`95.40 ms` (16-shard Work-Stealing)** | **4.8x Faster Drain** |
| **Standalone Oracle GraalVM 25 Native Binary** | `49 MB` | **`49 MB`** | **Zero-SQL Native Image** |

---

## Verification

```bash
./gradlew spotlessApply check
cd gateway-console && npm test && npm run build
```

---

## License

Licensed under the [Apache License, Version 2.0](LICENSE).
