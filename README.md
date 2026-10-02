# rekuiper

[![Release](https://img.shields.io/badge/release-v0.504--beta-blue.svg)](https://github.com/ankur-paan/rekuiper/releases)
[![Rust CI](https://github.com/ankur-paan/rekuiper/actions/workflows/ci.yml/badge.svg)](https://github.com/ankur-paan/rekuiper/actions/workflows/ci.yml)
[![License: MIT or Apache-2.0](https://img.shields.io/badge/license-MIT%20%2F%20Apache--2.0-yellow.svg)](LICENSE)
[![Docker](https://img.shields.io/badge/docker-ankurkrp%2Frekuiper%3A0.504--beta-blue.svg)](https://hub.docker.com/r/ankurkrp/rekuiper)
[![Docker Pulls](https://img.shields.io/docker/pulls/ankurkrp/rekuiper?color=blue&logo=docker)](https://hub.docker.com/r/ankurkrp/rekuiper)

rekuiper is a stream processing engine written in Rust for edge computing systems. The engine is fully compatible with LF Edge eKuiper. It implements the eKuiper REST API, SQL dialect, rule definition format, and the `kuiper` command-line interface (CLI). Existing eKuiper streams, rules, and ecosystem tools (including [eKuiper Manager](https://github.com/ankur-paan/ekuiper-manager)) operate without changes.

The engine is optimized for resource-constrained edge hardware, such as Industrial IoT (IIoT) gateways, ESPHome fleets, connected vehicles, and EV charging stations. These deployments require deterministic stream processing on one or two CPU cores with strict memory limits.

---

## Key Capabilities

- **Drop-in Compatibility**: Runs existing eKuiper rules, SQL queries, and tool integrations directly.
- **Bounded Memory Footprint**: Internal buffers enforce strict memory limits under high ingestion load.
- **Incremental Window Aggregation**: Aggregation state scales with the number of unique groups, not the message rate.
- **Backpressure Protection**: Bounded asynchronous queues connect sources, execution rules, and sinks.
- **Reliable Offline Storage**: Sinks store undelivered messages in memory and spill to disk during target outages.
- **Native AI Tooling**: Includes a Model Context Protocol (MCP) server for automated SQL validation and management.

---

## Performance Evaluation

rekuiper was evaluated against eKuiper 2.4.1, Telegraf 1.40.0, and Redpanda Connect 4.109.0 across five industrial MQTT workloads.

### Test Environment
- **Host Resource Limits**: 1 CPU core, 1 GiB RAM per engine container.
- **Ingestion Broker**: Mosquitto MQTT broker.
- **Workload Generator**: Open-loop synthetic telemetry generator written in Rust.
- **Validation**: Exact end-to-end output verification at the sink (zero message loss, verified message identifiers).

### Maximum Sustained Throughput (Zero Message Loss)

| Workload | rekuiper 0.500 | eKuiper 2.4.1 | Telegraf 1.40.0 | Redpanda Connect 4.109.0 |
| :--- | :--- | :--- | :--- | :--- |
| Telemetry filter (1,000 devices) | **150,000 msg/s** | 20,000 msg/s | 50,000 msg/s (backlog) | 20,000 msg/s |
| 10-second tumbling window per device | **200,000 msg/s** | 20,000 msg/s | Incomplete (6–27% loss) | 5,000 msg/s |
| ESPHome telemetry (10,000 topics, `meta(topic)`) | **150,000 msg/s** | 20,000 msg/s | 50,000 msg/s (backlog) | 20,000 msg/s |
| Vehicle telemetry (10,000 topics, windowed) | **200,000 msg/s** | 20,000 msg/s | Inconsistent output | 5,000 msg/s |
| EV charger sessions (`SESSIONWINDOW`) | **126,000 msg/s** | 20,000 msg/s | Not supported | Not supported |

### CPU Utilization at 20,000 msg/s (% of One Core)

| Workload | rekuiper 0.500 | eKuiper 2.4.1 | Telegraf 1.40.0 | Redpanda Connect 4.109.0 |
| :--- | ---: | ---: | ---: | ---: |
| Telemetry filter | **45%** | 99% | 90% | 99% |
| 10-second tumbling window per device | **43%** | 86% | 81% (9% loss) | 98% (data loss) |
| ESPHome telemetry (10,000 topics) | **50%** | 94% | 70% | 98% |
| Vehicle telemetry (10,000 topics) | **41%** | 91% | 86% (9% loss) | 99% (data loss) |
| EV charger sessions | **50%** | 87% | Not supported | Not supported |

### Memory Allocation at 20,000 msg/s (Anonymous Memory, MiB)

| Workload | rekuiper 0.500 | eKuiper 2.4.1 | Telegraf 1.40.0 | Redpanda Connect 4.109.0 |
| :--- | ---: | ---: | ---: | ---: |
| Telemetry filter | **4.4 MiB** | 15 MiB | 92 MiB | 72 MiB |
| 10-second tumbling window per device | **6.4 MiB** | 536 MiB | 52 MiB (9% loss) | 1,012 MiB (data loss) |
| ESPHome telemetry (10,000 topics) | **4.5 MiB** | 43 MiB | 85 MiB | 68 MiB |
| Vehicle telemetry (10,000 topics) | **10.2 MiB** | 886 MiB | 94 MiB (9% loss) | 993 MiB (data loss) |
| EV charger sessions | **7.3 MiB** | 832 MiB | Not supported | Not supported |

Benchmark tools, system configurations, and raw telemetry data are available in [test/benchmark/iiot-mqtt](test/benchmark/iiot-mqtt/README.md) and [BENCHMARK-0.500.md](test/benchmark/iiot-mqtt/BENCHMARK-0.500.md).

---

## Getting Started

### Run with Docker

Start the engine container with default settings:

```bash
docker run -d --name rekuiper \
  -p 9081:9081 -p 20499:20499 \
  -e KUIPER__BASIC__CONSOLELOG=true \
  -e KUIPER__BASIC__PROMETHEUS=true \
  ankurkrp/rekuiper:0.504-beta
```

### Run with Docker Compose

Deploy rekuiper with Mosquitto and Redis services:

```bash
docker compose -f deploy/docker/docker-compose.yml up -d
```

Configure environment options by copying [deploy/docker/.env.example](deploy/docker/.env.example) to `deploy/docker/.env`. Every setting in `etc/kuiper.yaml` accepts environment variable overrides with the format `KUIPER__<SECTION>__<KEY>` (for example, `KUIPER__BASIC__LOGLEVEL=debug`).

### Deploy to Kubernetes (Helm)

Install the bundled Helm chart:

```bash
helm install rekuiper deploy/chart/ekuiper \
  --set image.repository=ankurkrp/rekuiper \
  --set image.tag=0.504-beta
```

Refer to the [Helm Chart Documentation](deploy/chart/ekuiper/README.md) for persistence and volume configurations.

### Install Prebuilt Binaries

Download prebuilt binary archives for Linux, macOS, or Windows from the [Releases Page](https://github.com/ankur-paan/rekuiper/releases). Start the background service:

```bash
# Linux and macOS
./bin/kuiperd --etc etc

# Windows
.\bin\kuiperd.exe --etc etc
```

### Build from Source

Requirements: Rust compiler version 1.85 or newer.

```bash
git clone https://github.com/ankur-paan/rekuiper.git
cd rekuiper
cargo build --release
```

Compiled binaries are located in `target/release/`.

---

## Network Ports

| Port | Protocol | Purpose | Default Bind |
| :--- | :--- | :--- | :--- |
| `9081` | HTTP / TCP | REST API, OpenAPI docs, stream and rule management, CLI | `0.0.0.0:9081` |
| `20499` | HTTP / TCP | Prometheus metrics (`/metrics`) | `0.0.0.0:20499` |
| `20498` | TCP | RPC protocol compatibility | `127.0.0.1:20498` |

[![Docker Pull History](docs/docker-pulls.svg)](https://hub.docker.com/r/ankurkrp/rekuiper)

---

## Quick Start Tutorial

Follow these steps to create an input stream, configure an alert rule, and verify data processing.

### 1. Create a Stream
Register an input stream named `telemetry`:

```bash
curl -X POST http://localhost:9081/streams \
  -H "Content-Type: application/json" \
  -d '{"sql": "CREATE STREAM telemetry () WITH (FORMAT=\"json\")"}'
```

### 2. Create a Rule
Define an alert rule that detects high temperatures and publishes alerts over MQTT:

```bash
curl -X POST http://localhost:9081/rules \
  -H "Content-Type: application/json" \
  -d '{
    "id": "alert_rule",
    "sql": "SELECT id, temp, temp * 1.8 + 32 AS temp_f FROM telemetry WHERE temp > 30.0",
    "actions": [
      {"log": {}},
      {"mqtt": {
        "server": "tcp://broker.emqx.io:1883",
        "topic": "alerts/critical",
        "dataTemplate": "{\"alert\": \"OVERHEAT\", \"device\": \"{{.id}}\", \"temp_f\": {{.temp_f}}}"
      }}
    ]
  }'
```

### 3. Send Sample Telemetry
Ingest a test message into the stream:

```bash
curl -X POST http://localhost:9081/streams/telemetry/data \
  -H "Content-Type: application/json" \
  -d '{"id": "sensor_01", "temp": 35.6}'
```

### 4. Check Rule Execution Status
Verify that the rule processed the message:

```bash
curl http://localhost:9081/rules/alert_rule/status
```

---

## Functional Scope

| Functional Area | Scope and Compatibility |
| :--- | :--- |
| **REST API** | 98 endpoints and 140 operations compatible with eKuiper specifications, verified against `openapi.json`. |
| **CLI** | Full command compatibility for streams, tables, rules, validation, and configuration import/export. |
| **SQL Engine** | Complete clause support: `WHERE`, `GROUP BY`, `HAVING`, `ORDER BY`, `LIMIT`, `CASE`, nested JSON paths, array indexing and slicing, and `unnest`. Supports mathematical, string, datetime, hashing, aggregate, and analytic functions. |
| **Window Processing** | Tumbling, hopping, sliding, count, and session windows. Common aggregations (`count`, `sum`, `avg`, `min`, `max`) execute incrementally to preserve bounded memory. |
| **Table Joins** | Stream-to-table lookups against in-memory tables, Redis, and SQL databases. Stream-to-stream windowed joins (inner, left, right, full, and cross). |
| **MQTT Connector** | Source and sink support for MQTT 3.1.1 (QoS 0, 1, and 2). Payload formats: JSON objects and arrays, raw binary, delimited text, and Protocol Buffers. Supports topic wildcards and metadata extraction (`meta(topic)`, `meta(qos)`, `meta(messageId)`). |
| **Additional Connectors** | Connectors for Kafka, Redis, WebSocket, HTTP pull/push, SQL databases (PostgreSQL, MySQL, SQLite), and files (CSV, JSON Lines). |
| **Sink Delivery** | Template rendering via `dataTemplate`. Configurable offline caching (`enableCache`, `memoryCacheThreshold`, `maxDiskCache`) stores failed records and resends them in order upon target reconnection. |
| **Rule Testing and Graphs**| Interactive rule testing with Server-Sent Events (`POST /ruletest`); supports Directed Acyclic Graph (DAG) rule definitions. |
| **Metrics and Tracing** | Standard Prometheus metrics exporter and OpenTelemetry integration. |
| **AI Integration (MCP)** | Embedded Model Context Protocol (MCP) server with 42 management tools, 11 system resources, and offline SQL AST validation. |

### Architectural Boundaries
- **Industrial Bus Drivers**: Connect Modbus, OPC UA, or BACnet networks through dedicated edge gateways that publish to MQTT.
- **Vision and Neural Models**: Process video streams or embedded inference models upstream and forward inference results to rekuiper.
- **Node Topology**: Operates as a high-performance single-node streaming engine.

---

## System Architecture

rekuiper uses an asynchronous, backpressure-managed streaming pipeline:

1. **Source Ingestion**: Ingestion connectors read telemetry from external brokers, networks, or files.
2. **Stream Bus**: Messages enter an in-process communication bus with bounded queues. If rule execution slows, backpressure regulates the source.
3. **Rule Execution**: Each active rule runs as an isolated asynchronous task. The task evaluates SQL expressions, manages window state, and executes joins.
4. **Sink Dispatch**: Processed results enter a bounded sink queue (default size: 10,000 records). A dedicated sink worker delivers records to the destination.
5. **Offline Cache**: When target endpoints become unavailable, failed messages transfer to memory and spill to disk storage. The worker resends cached messages in sequence when connectivity recovers.

```
External Sources (MQTT, HTTP, Kafka, Redis, SQL, Files)
                       │
                       ▼
         In-Process Stream Bus (Bounded Queues)
                       │
                       ▼
    Rule Execution Tasks (SQL, Windows, Aggregations)
                       │
                       ▼
          Sink Buffer Queue (Configured Capacity)
                       │
                       ▼
       Sink Dispatcher & Offline Persistent Cache
                       │
                       ▼
External Destinations (MQTT, HTTP, Kafka, Redis, SQL, Files)
```

---

## Reliability and Quality Qualification

Every release candidate undergoes automated differential testing against LF Edge eKuiper 2.4.1 under identical resource constraints:

- **Mathematical Parity**: Verified across 12 rule categories (arithmetic, string operations, conditionals, JSON navigation, array slicing, datetime, stateful windows, and trigonometry) with **99.98% numerical accuracy**.
- **Fault Recovery**:
  - `SIGKILL` Process Termination: Transactional SQLite WAL logging prevents catalog corruption. Active rules restart automatically on engine restart.
  - Network Partitions: Target connection failures trigger bounded disk cache spilling without unbounded memory growth. Queued messages drain automatically after network recovery.
  - Concurrent Mutations: Concurrent rule operations (`POST`, `PUT`, `DELETE`, `start`, `stop`) are synchronized without race conditions.
- **Resource Stability**: Maintains steady anonymous memory usage (~45 MB RSS) under sustained load with zero leaks across 150 consecutive rule lifecycle cycles.

---

## AI Agent Integration (Model Context Protocol)

rekuiper includes a native Model Context Protocol (MCP) server ([`crates/rekuiper-mcp`](crates/rekuiper-mcp/README.md)). The server enables AI coding agents (such as Antigravity IDE, Claude Desktop, and Cursor) to inspect, configure, and operate the engine through JSON-RPC 2.0 stdio transport.

- **Offline SQL Validation**: Inspects queries with `validate_sql` and validates execution plans with `explain_sql` without network access.
- **Lifecycle Management**: Provides 42 tools for full lifecycle management of streams, rules, schemas, and connection pools.
- **REST API Proxy (`execute_rekuiper_api`)**: Executes standard HTTP operations (`GET`, `POST`, `PUT`, `DELETE`, `PATCH`) on engine endpoints.

### MCP Docker Execution

```bash
docker run -i --rm --network host \
  rekuiper-mcp:latest --server-url http://127.0.0.1:9081
```

### Client Configuration (`mcp_config.json`)

```json
{
  "mcpServers": {
    "rekuiper": {
      "command": "docker",
      "args": [
        "run", "-i", "--rm", "--network", "host",
        "rekuiper-mcp:latest",
        "--server-url", "http://127.0.0.1:9081"
      ]
    }
  }
}
```

---

## Monitoring and Metrics

Prometheus metrics are available at `http://localhost:9081/metrics` and on port `20499`:

- `kuiper_rule_count{status="running|stop"}`: Total active and stopped rules.
- `kuiper_rule_status{rule="<id>"}`: Execution status of a specific rule.
- `kuiper_source_records_in_total{rule="<id>"}`: Total records received from sources.
- `kuiper_source_records_out_total{rule="<id>"}`: Total records emitted by sources.
- `kuiper_sink_records_in_total{rule="<id>"}`: Total records received by sinks.
- `kuiper_sink_records_out_total{rule="<id>"}`: Total records written to sinks.
- `kuiper_sink_exceptions_total{rule="<id>"}`: Total exceptions encountered by sinks.
- `kuiper_sink_latency_us{rule="<id>"}`: Sink processing latency in microseconds.

---

## Release History

- **0.504-beta** (Current): Native dual binary entrypoints (`rekuiperd` daemon and `rekuiper` CLI); native `etc/rekuiper.yaml` configuration with `REKUIPER__` and `KUIPER__` environment overrides; repository metadata maintenance for I-Dacs Labs; updated technical documentation site with VitePress.
- **0.503-beta**: Full SQL function library parity (162 functions) verified against live MQTT telemetry; compression extensions (`compress`, `decompress` for zlib, gzip, flate, zstd); timezone conversions (`convert_tz`); high-resolution date arithmetic (`date_calc`); dynamic column projection (`changed_cols`); row unnesting (`unnest`, `extract`); running stream accumulators (`acc_collect`).
- **0.502-beta**: Embedded Model Context Protocol (MCP) server (`rekuiper-mcp`) with 42 tools; offline streaming SQL query simulation; runtime rule tracing controls (`start_rule_trace`, `stop_rule_trace`); 99.98% differential mathematical verification; automated crash qualification harness; multi-platform container images.
- **0.501-beta**: Transactional storage atomicity (`KvOperation`, `apply_transaction`); strict configuration key validation; IIoT MQTT ladder benchmarks with zero packet loss.
- **0.500-beta**: High-performance in-memory catalog; zero-disk hot path for rule execution and REST dispatch; multi-row SQL batch insertions; hot-path connection pooling; certified sustained throughput up to 200,000 msg/s per core.
- **0.426-beta**: Truthful MQTT and sink delivery accounting; sustained-throughput benchmark suite with dedicated Rust load tools.
- **0.425-beta**: Incremental window aggregation (`GROUP BY`, `WHERE`, `HAVING`, `ORDER BY`, `LIMIT`) with bounded memory; session windows (`SESSIONWINDOW`); MQTT binary, delimited, and protobuf formats; persistent MQTT sink with offline cache.
- **0.424-beta**: SQL source and lookup extensions; windowed joins; array and JSONPath operations; rule testing with Server-Sent Events; automatic rule resumption on service restart; CLI parity.
- **0.423-beta**: Rule control endpoint (`POST /rules/:name/start`); ruleset and data import.
- **0.422-beta**: Remote MQTT ingestion; PostgreSQL data plane; PUT and PATCH handlers; JWT authentication; stream and table schema management.
- **0.421-beta**: OpenAPI route registration; persistent file uploads; YAML configuration overlays and secret masking; bulk rule controls.
- **0.420-beta**: Initial release: Core Rust engine, sink queue architecture, primary connectors, and execution graph rules.

Detailed release information is recorded in [CHANGELOG.md](CHANGELOG.md).

---

## Contributing

Build and run automated test suites with:

```bash
cargo build
cargo test --workspace
```

Refer to [CONTRIBUTING.md](CONTRIBUTING.md) for contribution guidelines, and [SECURITY.md](SECURITY.md) for vulnerability reporting procedures.

---

## License

rekuiper is distributed under the Apache License 2.0 or the MIT License. See [LICENSE](LICENSE) and [LICENSE-APACHE](LICENSE-APACHE) for terms.

---

Maintained by [I-Dacs Labs](https://i-dacs.com) · [measure@i-dacs.com](mailto:measure@i-dacs.com) · [LinkedIn](https://www.linkedin.com/company/110770924)
