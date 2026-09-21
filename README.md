# rekuiper

[![Release](https://img.shields.io/badge/release-v0.500--beta-blue.svg)](https://github.com/ankur-paan/rekuiper/releases)
[![Rust CI](https://github.com/ankur-paan/rekuiper/actions/workflows/ci.yml/badge.svg)](https://github.com/ankur-paan/rekuiper/actions/workflows/ci.yml)
[![License: MIT or Apache-2.0](https://img.shields.io/badge/license-MIT%20%2F%20Apache--2.0-yellow.svg)](LICENSE)
[![Docker](https://img.shields.io/badge/docker-ankurkrp%2Frekuiper%3A0.500--beta-blue.svg)](https://hub.docker.com/r/ankurkrp/rekuiper)
[![Docker Pulls](https://img.shields.io/docker/pulls/ankurkrp/rekuiper?color=blue&logo=docker)](https://hub.docker.com/r/ankurkrp/rekuiper)

rekuiper is a stream processing engine for edge devices, written in Rust. It implements
eKuiper's REST API, SQL dialect, rule format and `kuiper` CLI, so existing eKuiper streams,
rules and tools, including [eKuiper Manager](https://github.com/ankur-paan/ekuiper-manager),
work against it without changes.

We built it for IIoT gateways, ESPHome fleets, vehicles and EV chargers: small machines that
take in MQTT telemetry and have to filter, aggregate and forward it reliably on one or two cores.

## Performance

We benchmark rekuiper against eKuiper 2.4.1, Telegraf 1.40.0 and Redpanda Connect 4.109.0 on
five MQTT workloads. Every engine gets the same Mosquitto broker, Rust load generator, one CPU
core and 1 GiB of memory. Each ladder step runs for 30 seconds at 5k, 20k, 50k or 100k messages/s,
and correctness is checked exactly at the sink.

**Highest tested rate with an exact, loss-free result**

| Workload | rekuiper 0.500 | eKuiper 2.4.1 | Telegraf 1.40.0 | Redpanda Connect 4.109.0 |
| :--- | :--- | :--- | :--- | :--- |
| Telemetry filter, 1,000 devices | **150k** | 20k | 50k, with backlog | 20k |
| 10-second window per device | **200k** | 20k | lost 6-27% at every rate | 5k |
| ESPHome, 10,000 topics, `meta(topic)` | **150k** | 20k | 50k, with backlog | 20k |
| Vehicles, 10,000 topics, windowed | **200k** | 20k | inconsistent | 5k |
| EV charger sessions (`SESSIONWINDOW`) | **126k** | 20k | not supported | not supported |

**CPU at 20,000 msg/s** (percent of one core)

| Workload | rekuiper 0.500 | eKuiper 2.4.1 | Telegraf 1.40.0 | Redpanda Connect 4.109.0 |
| :--- | ---: | ---: | ---: | ---: |
| Telemetry filter | **45%** | 99% | 90% | 99% |
| 10-second window per device | **43%** | 86% | 81% (losing 9%) | 98% (losing all) |
| ESPHome, 10,000 topics | **50%** | 94% | 70% | 98% |
| Vehicles, 10,000 topics | **41%** | 91% | 86% (losing 9%) | 99% (losing all) |
| EV charger sessions | **50%** | 87% | not supported | not supported |

**Memory at 20,000 msg/s** (engine anonymous memory, MiB)

| Workload | rekuiper 0.500 | eKuiper 2.4.1 | Telegraf 1.40.0 | Redpanda Connect 4.109.0 |
| :--- | ---: | ---: | ---: | ---: |
| Telemetry filter | **4.4** | 15 | 92 | 72 |
| 10-second window per device | **6.4** | 536 | 52 (losing 9%) | 1,012 (losing all) |
| ESPHome, 10,000 topics | **4.5** | 43 | 85 | 68 |
| Vehicles, 10,000 topics | **10.2** | 886 | 94 (losing 9%) | 993 (losing all) |
| EV charger sessions | **7.3** | 832 | not supported | not supported |

The rekuiper column is the repeated 0.500 ladder and exact peak searches. The competitor columns are
the published runs on the same host and harness; those engines were unchanged and were not rerun for
this release. Bounded capacity searches verified that rekuiper reached certified sustained ceilings of
150k on telemetry filter and ESPHome, 200k on per-device and vehicle windows, and 126k on charger sessions.

The complete method, every engine configuration, per-rate tables, raw evidence, sustained trials and
limitations are published in [test/benchmark/iiot-mqtt](test/benchmark/iiot-mqtt/README.md) and
[BENCHMARK-0.500.md](test/benchmark/iiot-mqtt/BENCHMARK-0.500.md). The earlier 0.426 and 0.425 reports
are preserved in [BENCHMARK-0.426.md](test/benchmark/iiot-mqtt/BENCHMARK-0.426.md) and
[ARCHIVE-0.425-COMPARISON.md](test/benchmark/iiot-mqtt/ARCHIVE-0.425-COMPARISON.md).

## Getting started

### Docker

```bash
docker run -d --name rekuiper \
  -p 9081:9081 -p 20499:20499 \
  -e KUIPER__BASIC__CONSOLELOG=true \
  -e KUIPER__BASIC__PROMETHEUS=true \
  ankurkrp/rekuiper:0.500-beta
```

To run rekuiper together with Mosquitto and Redis:

```bash
docker compose -f deploy/docker/docker-compose.yml up -d
```

### Prebuilt binaries

Download a build for Linux, macOS or Windows from
[Releases](https://github.com/ankur-paan/rekuiper/releases), then start the daemon:

```bash
./bin/kuiperd --etc etc          # Linux and macOS
.\bin\kuiperd.exe --etc etc      # Windows
```

### From source

You need Rust 1.85 or newer.

```bash
git clone https://github.com/ankur-paan/rekuiper.git
cd rekuiper
cargo build --release        # binaries in target/release/
make build                   # or copy them into bin/
```

## A first rule

Create a stream, add a rule that flags hot sensors and publishes an alert over MQTT, then send
a reading.

```bash
curl -X POST http://localhost:9081/streams \
  -H "Content-Type: application/json" \
  -d '{"sql": "CREATE STREAM telemetry () WITH (FORMAT=\"json\")"}'

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

curl -X POST http://localhost:9081/streams/telemetry/data \
  -H "Content-Type: application/json" \
  -d '{"id": "sensor_01", "temp": 35.6}'

curl http://localhost:9081/rules/alert_rule/status
```

## What rekuiper supports

| Area | Details |
| :--- | :--- |
| REST API | 98 paths and 140 operations of eKuiper's API, checked against `openapi.json` by the black-box tests in `fvt_compat`. Known gaps are listed in [BENCHMARK-AUDIT.md](BENCHMARK-AUDIT.md#known-residual-gaps). |
| CLI | `kuiper` subcommands for streams, tables, rules, import/export and validation. |
| SQL | `WHERE`, `GROUP BY`, `HAVING`, `ORDER BY`, `LIMIT`, `CASE`, nested JSON paths, array index and slice, `unnest`, built-in math, string, JSON, time, hashing, aggregate and analytic functions. |
| Windows | Tumbling, hopping, sliding, count and session windows. Time windows align to wall-clock boundaries. Common aggregates (`count`, `sum`, `avg`, `min`, `max`) run incrementally, so memory depends on the number of groups rather than the message rate. |
| Joins | Stream-to-table lookups against memory, Redis and SQL tables; stream-to-stream joins inside windows (inner, left, right, full, cross). |
| MQTT | Source and sink over MQTT 3.1.1 with QoS 0, 1 and 2 and username/password. Payload formats: JSON objects and arrays, binary, delimited, and protobuf (via a registered schema). `meta(topic)`, `meta(qos)` and `meta(messageId)`. Wildcard and multiple topics. TLS support is coming in an upcoming release. |
| Other connectors | Kafka, Redis (lookup, pub/sub source, `SET`/`PUBLISH` sink), WebSocket, HTTP pull and push, PostgreSQL/MySQL/SQLite (source, lookup, sink), files (JSON Lines and CSV), memory. These work but are not yet stable; they will be tested and released as stable in upcoming versions. MQTT is the stable, benchmarked path today. |
| Sink delivery | `dataTemplate` payloads. Optional offline cache with eKuiper's options (`enableCache`, `memoryCacheThreshold`, `maxDiskCache`, `bufferPageSize`, `resendInterval`, `resendPriority`, ...): failed records are kept in memory and then on disk, and resent in order when the destination comes back. |
| Rule testing and graphs | `POST /ruletest` with results streamed over Server-Sent Events; graph (DAG) rules. |
| Observability | Prometheus metrics on port 20499 and at `/metrics`. |

Not supported, by design or not yet:

- EdgeX IPC and EMQ Neuron IPC: connect through MQTT or Redis instead.
- Native Go plugins (`.so`): use portable plugins or JavaScript UDFs.
- Video pipelines, embedded ONNX/TFLite models, Modbus, OPC UA and BACnet drivers: use a
  gateway that publishes to MQTT.
- Clustering: rekuiper runs as a single node.

## How it works

Each source publishes into an in-process stream bus with bounded queues, so a slow rule applies
backpressure instead of dropping data. Every rule runs as its own task that evaluates the SQL and
maintains window state. Output goes through a bounded sink queue (`bufferLength`, 10,000 by
default) to a per-rule sink worker, which keeps a persistent connection for MQTT, reuses HTTP
connections, batches file writes, and holds failed records in the offline cache when caching is
enabled.

```
sources (MQTT, HTTP, Kafka, Redis, WebSocket, SQL, files)
   │
   ▼
stream bus: bounded queues, backpressure
   │
   ▼
rule task: SQL evaluation, windows, joins
   │
   ▼
sink queue: bufferLength, default 10,000
   │
   ▼
sink worker: MQTT, HTTP, Kafka, Redis, WebSocket, SQL, files, offline cache
```

## Monitoring

Prometheus metrics are served on port 20499 and at `http://localhost:9081/metrics`:

- `kuiper_rule_count{status="running|stop"}`
- `kuiper_rule_status{rule="<id>"}`
- `kuiper_source_records_in_total{rule="<id>"}` and `kuiper_source_records_out_total{rule="<id>"}`
- `kuiper_sink_records_in_total{rule="<id>"}` and `kuiper_sink_records_out_total{rule="<id>"}`
- `kuiper_sink_exceptions_total{rule="<id>"}`
- `kuiper_sink_latency_us{rule="<id>"}`

## Why we built it

rekuiper comes from I-Dacs Labs, where we run edge telemetry pipelines for industrial gateways
and robotics. JVM engines such as Flink need more memory than those machines have. Go-based tools
such as eKuiper, Telegraf and Benthos fit, but on one core they fell behind well below the message
rates we see from vehicle and charger fleets, and window state grew with traffic. We wanted an
engine that stays within a small, predictable memory budget and keeps up on a single core, while
remaining compatible with the eKuiper ecosystem we already used.

## Earlier HTTP measurements

Before the 0.425 work, we ran an over-HTTP comparison with eKuiper on the 0.424-beta build. That
build lost most of a 500,000-event burst, and the results were marked provisional. The admission
path has since been rewritten, but the HTTP comparison has not been rerun, so we make no HTTP
performance claim here. The original data is kept in [BENCHMARK-AUDIT.md](BENCHMARK-AUDIT.md) and
[test/BENCHMARKS.md](test/BENCHMARKS.md).

## Release history

- **0.500-beta** (current): in-memory Redis-style catalog architecture, zero-disk hot path for
  rule execution and REST dispatch, multi-row SQL batch insertions, hot-path connection pooling,
  and exact peak capacity benchmarks certifying up to 200,000 msg/s per core.
- **0.426-beta**: truthful MQTT and sink delivery accounting, removal of fabricated
  runtime registrations, and a bounded sustained-throughput benchmark with Rust publisher and
  subscriber tools.
- **0.425-beta**: correct window aggregation (`GROUP BY`, `WHERE`, `HAVING`, `ORDER BY`,
  `LIMIT`) with bounded memory, `SESSIONWINDOW`, MQTT binary/delimited/protobuf payloads and
  `meta()`, a persistent MQTT sink with offline cache, and the IIoT MQTT benchmark.
- **0.424-beta**: PostgreSQL and SQL source/lookup fixes, windowed joins, array and JSONPath
  syntax, rule test SSE on port 10081, rules resume after restart, CLI parity.
- **0.423-beta**: `POST /rules/:name/start`, ruleset and data import.
- **0.422-beta**: remote MQTT ingestion and `CONF_KEY`, PostgreSQL data plane, PUT/PATCH handlers,
  JWT auth, stream and table schemas.
- **0.421-beta**: OpenAPI route registration, persistent uploads, YAML overlays and secret masking,
  bulk rule control.
- **0.420-beta**: first release: Rust engine, sink queue, connectors, graph rules.

See [CHANGELOG.md](CHANGELOG.md) for details.

## Contributing

Build and test with `cargo build` and `cargo test --workspace`. See [CONTRIBUTING.md](CONTRIBUTING.md)
for the development workflow and commit sign-off, and [SECURITY.md](SECURITY.md) for reporting
vulnerabilities.

## License

rekuiper is available under the MIT license or the Apache License 2.0, at your option. See
[LICENSE](LICENSE) and [LICENSE-APACHE](LICENSE-APACHE).

---

Maintained by [I-Dacs Labs](https://i-dacs.com) · [measure@i-dacs.com](mailto:measure@i-dacs.com) ·
[LinkedIn](https://www.linkedin.com/company/110770924)
