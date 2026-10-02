# rekuiper

> High-performance stream processing engine for edge devices, written in Rust.

[![Release](https://img.shields.io/badge/release-v0.505--beta-blue.svg)](https://github.com/ankur-paan/rekuiper/releases)
[![Rust CI](https://github.com/ankur-paan/rekuiper/actions/workflows/ci.yml/badge.svg)](https://github.com/ankur-paan/rekuiper/actions/workflows/ci.yml)
[![License: MIT or Apache-2.0](https://img.shields.io/badge/license-MIT%20%2F%20Apache--2.0-yellow.svg)](https://github.com/ankur-paan/rekuiper/blob/main/LICENSE)
[![Docker](https://img.shields.io/badge/docker-ankurkrp%2Frekuiper%3A0.505--beta-blue.svg)](https://hub.docker.com/r/ankurkrp/rekuiper)

rekuiper is a stream processing engine for edge devices, written in Rust. It implements the eKuiper REST API, SQL dialect, rule format, and `kuiper` CLI. Existing eKuiper streams, rules, and tools, including [eKuiper Manager](https://github.com/ankur-paan/ekuiper-manager), operate without modification.

The engine is designed for Industrial Internet of Things (IIoT) gateways, ESPHome fleets, vehicles, and Electric Vehicle (EV) chargers. These small machines receive MQTT, HTTP, or WebSocket telemetry. The engine filters, aggregates, and transmits data reliably on one or two CPU cores.

---

## How It Works

rekuiper processes real-time telemetry on the local machine before it sends data upstream:
![How rekuiper Works](./public/diagrams/how_it_works.svg)

1. **Ingest**: Read continuous sensor streams through standard protocols (MQTT, HTTP, WebSockets, Kafka, Files).
2. **Process**: Filter noise, calculate moving averages, and correlate events across time windows with SQL.
3. **Route**: Send clean anomalies, aggregated metrics, or control commands directly to local databases, brokers, or webhook endpoints.

---

## Why rekuiper?

| Feature | rekuiper (Rust) | eKuiper (Go) | Details |
| :--- | :--- | :--- | :--- |
| **Throughput** | **150k to 200k msg/s** | 20k msg/s | Provides 7.5x to 10x higher message throughput on a single CPU core |
| **Rule Concurrency** | **1,000 rules** | 100 rules | Executes 10x more parallel SQL rules on a shared stream without queue accumulation |
| **Memory Footprint** | **4.4 to 6.4 MiB** | 15 to 536 MiB | Maintains small deterministic memory under load. Fits low-memory edge devices |
| **Garbage Collection** | **Zero GC** | GC pauses (Go runtime) | Provides predictable latency without garbage collection stops or packet loss |
| **Binary Deployment** | **Single static binary** | Dynamic Go runtime | Provides a single binary with zero external runtime dependencies |
| **Compatibility** | **Wire-compatible** | Reference implementation | Operates with existing eKuiper REST API, SQL syntax, and CLI |
| **AI Integration** | **Native MCP Server** | Not available | Enables AI assistants (Cursor, Claude, Antigravity) to query and validate rules |

---

## Performance

rekuiper is evaluated against eKuiper 2.4.1, Telegraf 1.40.0, and Redpanda Connect 4.109.0 on a single CPU core with 1 GiB of RAM.

* **Concurrent Parallel Rules (rekuiper 0.505)**:
  * Maximum sustainable rules: **1,000 rules** (eKuiper 2.4.1: 100 rules — **10x higher concurrency**).
  * Sustained rule evaluations: **500,000 evals/s** (eKuiper 2.4.1: 50,000 evals/s).
  * CPU utilization at 100 rules: **22.8%** of one core (eKuiper 2.4.1: 80.3% of one core).
  * See [Concurrent Parallel Rules Benchmark](./benchmarks/parallel_rules.md).

* **Single-Rule MQTT Throughput (rekuiper 0.500)**:
  * Telemetry filter (1,000 devices): **150k msg/s** (eKuiper 2.4.1: 20k msg/s).
  * 10-second tumbling window: **200k msg/s** (eKuiper 2.4.1: 20k msg/s).
  * ESPHome (10,000 topics, `meta(topic)`): **150k msg/s** (eKuiper 2.4.1: 20k msg/s).
  * EV charger sessions (`SESSIONWINDOW`): **126k msg/s** (eKuiper 2.4.1: 20k msg/s).
  * See [Single-Rule MQTT High-Throughput Benchmark](./benchmarks/throughput.md).

* **Detailed Methodology**:
  * See [Benchmark Overview](./benchmarks/overview.md) for test principles, hardware boundaries, and validation criteria.

---

## Quickstart

Run rekuiper in Docker:

```shell
docker run -d \
  --name rekuiper \
  -p 9081:9081 \
  -p 20498:20498 \
  -p 20499:20499 \
  ankurkrp/rekuiper:0.505-beta
```

Check health:

```shell
curl http://localhost:9081/ping
# Response: pong
```

Create a data stream:

```shell
curl -X POST http://localhost:9081/streams \
  -H "Content-Type: application/json" \
  -d '{"sql": "CREATE STREAM demo () WITH (DATASOURCE=\"demo\", FORMAT=\"JSON\")"}'
```

Create a rule to log events when temperature exceeds 30:

```shell
curl -X POST http://localhost:9081/rules \
  -H "Content-Type: application/json" \
  -d '{
    "id": "rule_temp_alert",
    "sql": "SELECT temperature, humidity FROM demo WHERE temperature > 30",
    "actions": [{ "log": {} }]
  }'
```

Send test data:

```shell
curl -X POST http://localhost:9081/streams/demo/data \
  -H "Content-Type: application/json" \
  -d '{"temperature": 34.5, "humidity": 60.2}'
```

Check rule status:

```shell
curl http://localhost:9081/rules/rule_temp_alert/status
```

---

## Key Capabilities

* **Stream SQL**: Filter, project, join, and aggregate live data streams with tumbling, hopping, sliding, count, and session windows.
* **Connectors**:
  * **Sources**: MQTT, HTTP (Pull and Push), WebSockets, File, Memory, Redis, Kafka, Simulator, SQL.
  * **Sinks**: MQTT, HTTP/REST, Log, File, Memory, Redis, Kafka, WebSockets, Nop, SQL.
* **Model Context Protocol (MCP)**: Native `rekuiper-mcp` server allows AI coding assistants to validate SQL queries offline, test rule events, and inspect streaming topologies.
* **Web UI**: Operates with [eKuiper Manager](https://github.com/ankur-paan/ekuiper-manager) for visual stream, rule, and connector setup.

---

## Documentation

* [Architecture & Rust Design](./concepts/rekuiper.md)
* [Getting Started Guide](./getting_started/getting_started.md)
* [Installation & Deployment](./installation.md)
* [SQL Reference & Functions](./sqls/overview.md)
* [Source & Sink Connectors](./guide/sources/overview.md)
* [REST API Reference](./api/restapi/overview.md)
* [kuiper CLI Reference](./api/cli/overview.md)
* [Model Context Protocol (MCP)](./mcp/overview.md)
