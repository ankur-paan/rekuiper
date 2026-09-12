# rekuiper: The High-Performance Edge Stream Processing Engine

[![Release](https://img.shields.io/badge/release-v0.424--beta-blue.svg)](https://github.com/ankur-paan/rekuiper/releases)
[![Rust CI](https://github.com/ankur-paan/rekuiper/actions/workflows/ci.yml/badge.svg)](https://github.com/ankur-paan/rekuiper/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT%20%2F%20Apache--2.0-yellow.svg)](LICENSE)
[![Docker](https://img.shields.io/badge/docker-ankurkrp%2Frekuiper%3A0.424--beta-blue.svg)](https://hub.docker.com/r/ankurkrp/rekuiper)
[![Rust](https://img.shields.io/badge/rust-1.85%2B-orange.svg)](#)
[![OpenAPI 3.0](https://img.shields.io/badge/OpenAPI%203.0-contract%20audited-blue.svg)](openapi.json)
[![Benchmark](https://img.shields.io/badge/benchmark-fair%20over--HTTP%20audit-blue.svg)](BENCHMARK-AUDIT.md)

> **🚀 Pure Rust edge stream processing engine — zero GC pauses by construction, small static binary and low idle RSS, with eKuiper-compatible REST/CLI/YAML surface (scoped parity, see below). Fair over-HTTP source-to-sink results are reported in [BENCHMARK-AUDIT.md](BENCHMARK-AUDIT.md); no latency-histogram p99 is claimed.**

---

## ⚡ Fair Over-HTTP Benchmark (rekuiper vs eKuiper)

Same Python harness delivers the identical 500,000 synthetic events through documented
ingestion endpoints into containers with equal `--cpus=1 --memory=1g` constraints,
sequentially (never concurrent), with equivalent SQL/sink work. Source-to-sink
(rule-status sink counters) is measured, not HTTP-ack alone. 1 warmup + 3 measured
runs per config; raw per-run evidence and variance are preserved. No ranking is
claimed for Flink/Benthos/Telegraf — no equivalent over-HTTP rerun was completed,
so old noncomparable numbers were removed. Nothing here is called bulletproof.

- SQL: `SELECT id, temp * 1.8 + 32 AS temp_f FROM bench WHERE temp > 20.0`, sink `[{"nop":{}}]`
- Payload: `{"id":"dev_N","temp":25.0+(N%10)}` (~30 B, all pass filter), batch 500/POST, 8 HTTP workers
- rekuiper ingest: `POST /streams/bench/data` (implemented; covered by `test_http_push_data_ingestion`)
- eKuiper ingest: `POST :10081/bench/data` with `TYPE="httppush" DATASOURCE="/bench/data"`
  ([HTTP Push source](https://ekuiper.org/docs/en/latest/guide/sources/builtin/http_push.html))
- eKuiper default vs tuned (`bufferLength`/`concurrency`,
  [rule fine-tuning](https://ekuiper.org/docs/en/latest/guide/rules/overview.html#fine-tuning));
  effective options verified via `GET /rules/<id>`

| Config (500k attempted, --cpus=1) | Accepted (HTTP 2xx) | Observed sink-out / loss | End-to-end source-to-sink |
| :--- | :--- | :--- | :--- |
| `rekuiper` 0.424-beta candidate | 500,000 (1000×500) | ~21.2k–22.1k delivered (~95.7% loss under burst) | ~2,570–2,992 eps |
| eKuiper 2.4.1 default (1024/1) | 500,000 | ~219k–366k delivered (~27–56% loss, high variance) | ~4,635–6,808 eps |
| eKuiper 2.4.1 tuned (32768/4) | 500,000 | ~307k–336k delivered (~33–39% loss) | ~4,514–5,397 eps |

Per-run rows, drain timeout (120 s), errors, duration boundaries, image IDs, host/runtime,
and throughput/loss tradeoff notes are in **[BENCHMARK-AUDIT.md](BENCHMARK-AUDIT.md)** and
**[test/BENCHMARKS.md](test/BENCHMARKS.md)**. Larger eKuiper buffers reduce loss at the same
burst; undrained output is not called loss — loss is reported only after the drain window
stabilizes.

### 🔬 Internal microbenchmark (separate, NOT comparable)

`cargo test --release --test perf_throughput -- --nocapture` drives the in-process
engine bus directly (no HTTP, no containers). It is an internal regression floor
(assert > 20,000 eps), never compared against end-to-end HTTP numbers.

```bash
cargo test --release --test perf_throughput -- --nocapture
python3 test/benchmark/bench_http_fair.py --all --events 500000 --batch 500 --concurrency 8
```

### Scoped implementation notes

- No latency-histogram p99 is claimed (no histogram at a defined boundary was measured).
- Idle RSS / binary size / cold-boot figures from prior internal runs are retained only as
  informational, labeled noncomparable in `test/BENCHMARKS.md`.
- API coverage is scoped: 98 paths / 140 operations are registered against the audited
  `openapi.json` baseline with evidence-linked tests; known residual gaps are listed below.

---

## 🚀 Quick Start (Under 60 Seconds)

### Option 1: Docker (Fastest)

Run `rekuiper` with Docker Hub image:

```bash
docker run -d \
  --name rekuiper \
  -p 9081:9081 \
  -p 20499:20499 \
  -e KUIPER__BASIC__CONSOLELOG=true \
  -e KUIPER__BASIC__PROMETHEUS=true \
  ankurkrp/rekuiper:0.424-beta
```

Or spin up an instant end-to-end edge stack (rekuiper + Mosquitto MQTT broker + Redis):

```bash
docker compose -f deploy/docker/docker-compose.yml up -d
```

### Option 2: Pre-Compiled Binary

1. Download the release binary for Linux, macOS, or Windows from [GitHub Releases](https://github.com/ankur-paan/rekuiper/releases).
2. Start the daemon:

```bash
# Linux / macOS
./bin/kuiperd --etc etc

# Windows
.\bin\kuiperd.exe --etc etc
```

### Option 3: Build from Source

Prerequisites: Rust 1.85+ (`rustup update stable`).

```bash
git clone https://github.com/ankur-paan/rekuiper.git
cd rekuiper

# Build release binaries in parallel (placed in target/release/)
cargo build --release

# Or build and bundle binaries into bin/
make build
```

---

## 🏗️ Architectural Overview

`rekuiper` implements a lock-free, pipelined actor topology designed to isolate stream ingestion, query evaluation, and sink network I/O:

```
                    +------------------------------------+
                    |        Data Source Streams         |
                    | (MQTT, Kafka, Redis, WS, HTTP,...) |
                    +-----------------+------------------+
                                      |
                                      v
                    +------------------------------------+
                    |        StreamBus Broadcast         |
                    |  (Zero-allocation in-memory bus)   |
                    +-----------------+------------------+
                                      |
                                      v
             +--------------------------------------------------+
             |            Streaming SQL Engine Loop             |
             |  - Stream-Table Lookup JOINs (Redis / SQL / Mem) |
             |  - Tumbling, Hopping, Sliding, & Count Windows   |
|  - 40+ Math, String, Trig, & Aggregate Functions |
             +------------------------+-------------------------+
                                      |
                               (try_send non-blocking)
                                      v
             +--------------------------------------------------+
             |         Bounded Actor MPSC Channel               |
             |       (10,000 capacity backpressure buffer)      |
             +------------------------+-------------------------+
                                      |
                                      v
             +--------------------------------------------------+
             |         Dedicated Async Sink Worker              |
             |  - Persistent Keep-Alive Connection Pools        |
             |  - Mustache dataTemplate Payload Rendering       |
             |  - MQTT, Kafka, WS, REST, File, Redis, SQL Sinks |
             +--------------------------------------------------+
```

---

## 📋 Supported vs. Unsupported Features Matrix

Detailed capability disclosures for the **v0.424-beta** release:

| Feature Area | Supported in v0.424-beta | Status & Evidence |
| :--- | :--- | :--- |
| **REST API** | **Scoped route coverage** | 98 paths / 140 operations registered against the audited `openapi.json` baseline; covered by `fvt_compat` black-box tests. Not claimed as exhaustive parity — see known gaps in [BENCHMARK-AUDIT.md](BENCHMARK-AUDIT.md#known-residual-gaps) and `TEST-CONTRACT-AUDIT.md` where applicable. |
| **CLI Tool** | **`kuiper` Drop-in Replacement** | ✅ Full stream, table, rule management subcommands. |
| **Streaming Windows** | **Tumbling, Hopping, Sliding, Count** | ✅ Millisecond/second/minute/hour time units and event counts. |
| **Stream-Table JOINs** | **LEFT JOIN & INNER JOIN** | ✅ Join dynamic streams against static or external Redis / SQL lookup tables. |
| **SQL Expressions** | **Conditionals, CASE, Aliases, Logic** | ✅ Searched & simple `CASE`, nested `a.b.c` JSON paths, `ORDER BY`, `LIMIT`. |
| **SQL Functions** | **40+ Built-in Functions** | ✅ Math, trigonometry, string manipulation, JSON, `lag()`, `unnest()`, accumulators. |
| **MQTT Connector** | **Source & Sink** | ✅ Powered by `rumqttc`. Full QoS 0/1/2, TLS, client authentication. |
| **Apache Kafka** | **Source & Sink** | ✅ Pure-Rust high-throughput partition consumer & producer via `rskafka`. |
| **Redis** | **Source, Sink, & Lookup Table** | ✅ Key/value lookup, `redissub` pub/sub stream source, `SET`/`PUBLISH` sinks. |
| **WebSocket** | **Source & Sink** | ✅ Live frame streaming and client sink broadcasting via `tokio-tungstenite`. |
| **HTTP Pull & Push** | **Source & Sink** | ✅ High-speed timer poller (Pull) and `POST /streams/:name/data` (Push). |
| **Relational SQL** | **Source, Lookup, & Sink** | ✅ Multi-dialect query and insert engine for PostgreSQL, SQLite, MySQL via `sqlx`. |
| **File I/O** | **Source & Sink** | ✅ RFC-4180 CSV with automatic headers and JSON Lines. |
| **Sink Data Templates** | **Mustache `{{.field}}` Formatting** | ✅ Dynamic payload transformation bypassing standard JSON serialization. |
| **Observability** | **Prometheus Metrics** | ✅ Native Prometheus text exposition (`GET /metrics`) and standalone port `20499`. |
| **Interactive Ruletest**| **Real-time SSE Streaming** | ✅ Interactive `POST /ruletest` with Server-Sent Events pipeline replay. |
| **Graph Rule DAGs**    | **Visual DAG Engine**               | ✅ Visual drag-and-drop DAGs (`"graph": { "nodes": ..., "topo": ... }`). |
| **Config & Uploads**   | **Persistent Disk & Secret Masking** | ✅ Real disk storage in `data/uploads/`, YAML key maps with dynamic overlays, and recursive secret masking. |
| **EdgeX Foundry IPC** | ❌ **Not Supported in Core** | ⚠️ *Excluded to prevent bundling heavy C/ZeroMQ dependencies. Integrate via EdgeX MQTT/Redis message bus.* |
| **EMQ Neuron / NeuronEX**| ❌ **Not Supported in Core** | ⚠️ *Excluded proprietary IPC. Connect directly via standard MQTT broker (NanoMQ / EMQX).* |
| **Video / CV Pipelines**| ❌ **Not Supported in Core** | ⚠️ *FFmpeg, OpenCV, and RTSP video decoders are excluded to preserve the 9.6 MB micro footprint.* |
| **Embedded AI / ONNX** | ❌ **Not Supported in Core** | ⚠️ *ONNX Runtime and TensorFlow Lite C-bindings are omitted. No release scheduled.* |
| **Dynamic Go Plugins** | ❌ **Not Supported** | ⚠️ *Loading raw Go `.so` shared libraries violates Rust memory safety. Portable supervisor plugins and JS UDF services are fully supported.* |
| **Industrial Protocols**| ❌ **Not Supported in Core** | ⚠️ *Direct binary Modbus, OPC-UA, and BACnet drivers are not bundled. Bridge via an industrial edge gateway or NanoMQ.* |
| **Multi-Node Cluster** | ❌ **Not Supported** | ⚠️ *Designed exclusively as a hyper-specialized, single-node deterministic edge streaming daemon.* |

---

## 💡 Step-by-Step Usage Example

### 1. Create an Ingestion Stream

```bash
curl -X POST http://localhost:9081/streams \
  -H "Content-Type: application/json" \
  -d '{"sql": "CREATE STREAM telemetry () WITH (FORMAT=\"json\")"}'
```

### 2. Define a Streaming Analytics Rule

Filter high-temperature readings, convert Celsius to Fahrenheit, and publish to an MQTT topic with a custom data template:

```bash
curl -X POST http://localhost:9081/rules \
  -H "Content-Type: application/json" \
  -d '{
    "id": "alert_rule",
    "sql": "SELECT id, temp, temp * 1.8 + 32 AS temp_f FROM telemetry WHERE temp > 30.0",
    "actions": [
      {"log": {}},
      {
        "mqtt": {
          "server": "tcp://broker.emqx.io:1883",
          "topic": "alerts/critical",
          "dataTemplate": "{\"alert\": \"OVERHEAT\", \"device\": \"{{.id}}\", \"temp_f\": {{.temp_f}}}"
        }
      }
    ]
  }'
```

### 3. Push Real-Time Telemetry

```bash
curl -X POST http://localhost:9081/streams/telemetry/data \
  -H "Content-Type: application/json" \
  -d '{"id": "sensor_01", "temp": 35.6}'
```

### 4. Monitor Rule Health & Metrics

```bash
# Query rule metrics
curl -X GET http://localhost:9081/rules/alert_rule/status

# Or scrape Prometheus endpoint
curl -X GET http://localhost:20499/metrics
```

---

## 📊 Observability & Management

- **Web UI Compatible**: Works with the official [eKuiper Manager Web UI](https://ankur-paan.github.io/ekuiper-manager/) via the audited OpenAPI contract (scoped coverage; see gaps note above).
- **Prometheus Scrapes**: Scrape port `20499` (or `http://localhost:9081/metrics`) to monitor:
  - `kuiper_rule_count{status="running|stop"}`
  - `kuiper_rule_status{rule="<id>"}`
  - `kuiper_source_records_in_total{rule="<id>"}`
  - `kuiper_source_records_out_total{rule="<id>"}`
  - `kuiper_sink_records_in_total{rule="<id>"}`
  - `kuiper_sink_records_out_total{rule="<id>"}`
  - `kuiper_sink_exceptions_total{rule="<id>"}`
  - `kuiper_sink_latency_us{rule="<id>"}`

---

## 📖 Background & The Story Behind rekuiper

### Why we built and open-sourced `rekuiper`

Like [eKuiper Manager](https://github.com/ankur-paan/ekuiper-manager), `rekuiper` was engineered by **I-Dacs Labs**. We originally built and ran it internally to power our high-throughput edge telemetry pipelines, industrial IoT gateways, and real-time robotics infrastructure.

In production, edge streaming architectures faced a frustrating compromise:
1. **Heavyweight JVM Stream Engines (Apache Flink, Spark Streaming)**: Feature-rich, but require gigabytes of RAM, take seconds to boot, and instantly crash resource-constrained industrial gateways, Raspberry Pis, or embedded edge micro-servers.
2. **Go / Python Stream Processors (Upstream eKuiper, Benthos, Telegraf)**: Substantially lighter than JVM runtimes, but burdened by continuous garbage collection sweeps. Under sustained high-frequency sensor ingestion (10k–100k events/sec), Stop-The-World GC sweeps introduce severe tail latency spikes, dropped packets, and CPU jitter.

`rekuiper` was created to address this compromise. As a Rust implementation it has no GC pauses by construction; measured over-HTTP source-to-sink behavior and limits are reported in [BENCHMARK-AUDIT.md](BENCHMARK-AUDIT.md) rather than as absolute throughput claims. It is released under **MIT / Apache-2.0**.

---

## 🗺️ Roadmap

- **0.424-beta (Current)**: Documented parity fixes for PG/SQL sources and lookups, windowed joins, array/JSONPath, ruletest SSE on port 10081, restart resume, CLI surface, and Docker `restIp` default. Fair over-HTTP benchmark audit added.
- **0.423-beta**: Dynamic Rule Resume (`POST /rules/:name/start`), Envelope Import Support (`POST /ruleset/import` & `POST /data/import`).
- **0.422-beta**: Evaluation fixes across documented root causes (D1–D12), remote MQTT ingestion & CONF_KEY, PostgreSQL data plane, HTTP PUT/PATCH handlers, RSA/JWT guard, SSE ruletest, stream/table schemas.
- **0.421-beta**: Engine throughput test floor, OpenAPI route registration, persistent uploads, YAML overlays & masking, bulk rule control.
- **0.420-beta**: Rust core engine, actor sink queue, connector ecosystem, graph rule DAG engine.

---

## 🤝 Contributing & Community

We welcome contributions from the edge computing, IoT, and Rust communities!
- Please review [CONTRIBUTING.md](CONTRIBUTING.md) for build, testing, and DCO commit guidelines.
- To report security vulnerabilities, follow the procedures outlined in [SECURITY.md](SECURITY.md).

## 📄 License

`rekuiper` is open source software released under the **MIT License** (or at your option, the **Apache License, Version 2.0**) — the most permissive open-source licensing model.

Permission is hereby granted, free of charge, to any person obtaining a copy of this software to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies in personal, commercial, and enterprise production environments with zero fees or royalties.

See [LICENSE](LICENSE) and [LICENSE-APACHE](LICENSE-APACHE) for full terms.

---

**Developed and maintained by [I-Dacs Labs](https://i-dacs.com)**

📧 [measure@i-dacs.com](mailto:measure@i-dacs.com) · 🌐 [i-dacs.com](https://i-dacs.com) · 💼 [LinkedIn](https://www.linkedin.com/company/110770924)

*Building the future of Industrial IoT together.*
