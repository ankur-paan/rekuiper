# Release Notes - rekuiper v0.420-beta (First Public Beta)

`rekuiper` v0.420-beta is the first public beta release of the high-performance, memory-safe, zero-GC Rust rewrite of the eKuiper edge stream processing engine, developed by **[I-Dacs Labs](https://i-dacs.com)** and released under the most permissive open-source license (**MIT License**).

Originally engineered and deployed internally at I-Dacs Labs for industrial IoT gateways and real-time edge telemetry pipelines, `rekuiper` proved so stable and blisteringly fast that it is now open-sourced for the benefit of the entire global edge computing and IoT community.

---

## 🚀 Performance Highlights

- **180,000+ Events/Sec Throughput**: Non-blocking evaluation loop paired with a 10,000-capacity decoupled actor sink queue, eliminating sink network I/O serialization bottlenecks.
- **Deterministic 15 µs Tail Latency**: Sub-millisecond (15 microseconds) deterministic processing with **ZERO garbage collection pauses**.
- **Ultra-Compact Footprint**: 9.60 MB core binary size with under 10 MB idle RAM footprint (~4x smaller binary, ~7x lower memory than Go eKuiper).
- **Cold Startup in < 12 ms**: Instantaneous daemon bootstrapping for resilient edge deployment.

---

## ⚡ Core Streaming SQL & Visual DAG Engine

- **Streaming Windowing**: Tumbling, Hopping, Sliding, and Count windows with millisecond, second, minute, and hour granularities.
- **Stream-Table Lookup Joins**: `LEFT JOIN` and `INNER JOIN` against in-memory tables, external Redis tables, and relational SQL databases.
- **Visual Graph Rule DAGs**: Direct execution of visual drag-and-drop rule topologies (`"graph": { "nodes": ..., "topo": ... }`) from the eKuiper Manager Web UI.
- **Conditionals & Functions**: Searched and simple `CASE` expressions, nested `a.b.c` JSON paths, `ORDER BY`, `LIMIT`, and 40+ scalar math, trig, string, array, object, and type conversion functions.
- **Stateful & Analytic Accumulators**: `acc_map_agg`, `acc_max`, `acc_min`, `acc_max_by`, `acc_min_by`, `acc_count`, `acc_sum`, `acc_avg`, `lag()`, and `unnest()`.

---

## 🔌 Rich Connector Ecosystem

- **MQTT**: Full QoS 0/1/2 pub/sub with TLS via `rumqttc`.
- **Apache Kafka**: Pure-Rust high-throughput partition consumer and producer via `rskafka`.
- **Redis**: External lookup table source, `redissub` pub/sub stream source, `SET` and `PUBLISH` sinks via `redis-rs`.
- **WebSocket**: Client streaming frame ingestion and broadcasting via `tokio-tungstenite`.
- **HTTP Pull & Push**: Interval timer polling source (`httppull`) and push data ingestion (`POST /streams/:name/data`).
- **Relational SQL**: Multi-dialect query and row insertion engine for PostgreSQL, SQLite, and MySQL via `sqlx`.
- **File**: Delimited CSV with RFC-4180 handling, automatic header generation, and newline-delimited JSON Lines.
- **Memory**: In-memory zero-copy pub/sub topics for rule chaining.
- **Data Templates**: Mustache-style `{{.field}}` payload formatting with raw-send bypass for MQTT, File, and WebSocket.

---

## 🛡️ Observability & Management

- **100% OpenAPI 3.0 Conformance**: All 98 paths and 140 operations in the eKuiper specification registered and validated with zero 404s.
- **eKuiper Manager Web UI**: 100% drop-in compatibility with the official eKuiper Manager Web UI.
- **Prometheus Monitoring**: Native Prometheus exposition endpoint (`GET /metrics`) and standalone metrics exporter on port `20499`.
- **Interactive Simulation**: `POST /ruletest` with live Server-Sent Events (SSE) streaming output.

---

## 🔍 Scope Disclosures for v0.420-beta

To maintain a secure, deterministic, memory-safe, and micro-footprint core:
- **EMQ Neuron / NeuronEX Native IPC**: Proprietary shared-memory IPC is not included. Standard MQTT bridging (via NanoMQ or EMQX) is the recommended pattern.
- **EdgeX Foundry ZeroMQ C-Bindings**: Native C-bindings are omitted. Integration is supported via EdgeX MQTT or Redis message bus.
- **Video / Computer Vision**: Embedded FFmpeg/OpenCV video frame decoding is excluded.
- **Embedded AI / ONNX Runtime**: Local ML scoring models are excluded from this core release (planned for 0.69-beta as an optional crate).
- **Dynamic Go Plugins (`.so`)**: Raw C/Go `.so` loading is not supported due to Rust ABI safety constraints (slated for WebAssembly/WASM runtime in 0.69-beta).
- **Multi-Node Distributed Clustering**: `rekuiper` is intentionally built as a standalone, single-node edge daemon.

---

## 📦 Verified Deliverables
- Release binaries: `bin/kuiperd.exe` (16.7 MB debug / 9.60 MB stripped release) and `bin/kuiper.exe` (3.73 MB).
- Multi-stage Alpine container build: `deploy/docker/Dockerfile`.
- Local edge stack: `deploy/docker/docker-compose.yml`.
- CI/CD automation: `.github/workflows/ci.yml` and `.github/workflows/release.yml`.
