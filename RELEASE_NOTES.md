# Release Notes - rekuiper v0.501-beta

`rekuiper` v0.501-beta delivers transactional storage guarantees, strict configuration consistency, and process-level crash qualification.

Key highlights:
- **Atomic Multi-Operation Transactions**: `SqliteKvStore` implements atomic multi-operation batch mutations (`KvOperation`, `apply_transaction`) under transactional rollback to ensure catalog and configuration integrity even under abrupt failure.
- **Strict Configuration Consistency**: Configuration key mutations operate under atomic transaction guarantees, and configuration read errors reliably propagate HTTP 500 without leaving inconsistent memory state.
- **Reliability Qualification**: Includes the verified process-level qualification test harness with bounded in-flight message accounting, engine crash simulation, and immediate recovery verification.
- **eKuiper Parity**: Ingestion endpoints seamlessly return HTTP 200 when no active subscribers are attached to a stream, preserving exact drop-in compatibility.
- **Re-Certified Performance**: Re-verified the full 20-step IIoT MQTT ladder (W1–W5 across 5k–100k msg/s) with 0.00% packet loss and up to 22.2% reduction in CPU utilization.

- Reliability documentation: [`docs/en_US/reliability/qualification_and_scope.md`](docs/en_US/reliability/qualification_and_scope.md)
- Docker image: `ankurkrp/rekuiper:0.501-beta` (plus `latest`)
- Helm chart: `deploy/chart/ekuiper` (`AppVersion: 0.501-beta`)

---

# Release Notes - rekuiper v0.500-beta

`rekuiper` v0.500-beta introduces an in-memory Redis-style catalog and zero-disk
hot path architecture. Stream definitions, table schemas, and active rules are
held in memory with synchronized concurrency (`MemoryCatalog`), completely
removing SQLite disk contention from stream ingestion and query dispatch.
SQLite backing files operate under tuned WAL pragmas for durable background
synchronization.

Authentication keys, configuration files, and database connection pools are now
cached on the hot path, and channel buffer depths have been scaled to 32,768
messages to eliminate thread backpressure under heavy burst workloads.

This release includes an exact 1k-resolution peak capacity benchmark under a
bounded Mosquitto broker (4,096-message queue limit). Certified sustained ceilings
reach 150,000 msg/s for telemetry filters and ESPHome topics, 200,000 msg/s for
per-device and vehicle windows, and 126,000 msg/s for EV charger sessions on a
single CPU core and 1 GiB of RAM.

- Benchmark and exact peak results: [`test/benchmark/iiot-mqtt/BENCHMARK-0.500.md`](test/benchmark/iiot-mqtt/BENCHMARK-0.500.md)
- Benchmark suite and harness: [`test/benchmark/iiot-mqtt`](test/benchmark/iiot-mqtt/README.md)
- Archived 0.426 benchmark: [`BENCHMARK-0.426.md`](test/benchmark/iiot-mqtt/BENCHMARK-0.426.md)
- Docker image: `ankurkrp/rekuiper:0.500-beta` (plus `latest`)
- Helm chart: `deploy/chart/ekuiper` (`AppVersion: 0.500-beta`)

---

# Release Notes - rekuiper v0.426-beta

`rekuiper` v0.426-beta makes delivery metrics reflect real writes. File-sink
records count only after a successful flush, MQTT source errors reach the rule
exception counter, and unsupported SQL sink URLs fail instead of reporting a
write. Default portable plugins, external services and JavaScript UDFs no
longer advertise runtimes that are not installed.

The release includes a bounded MQTT benchmark with an external Rust publisher,
a constant-memory Rust subscriber probe, exact sink proofs and raw evidence.
All five workloads sustained 100,000 messages/s for 120 seconds on one CPU core
and 1 GiB RAM; the ESPHome topic workload sustained 150,000 messages/s. The
200,000 messages/s target was tested but is not claimed.

- Benchmark and method: [`test/benchmark/iiot-mqtt`](test/benchmark/iiot-mqtt/README.md)
- Archived 0.425 comparison: [`ARCHIVE-0.425-COMPARISON.md`](test/benchmark/iiot-mqtt/ARCHIVE-0.425-COMPARISON.md)
- Docker image: `ankurkrp/rekuiper:0.426-beta` (plus `latest`)
- Helm chart: `deploy/chart/ekuiper` (`AppVersion: 0.426-beta`)

---

# Release Notes - rekuiper v0.425-beta

`rekuiper` v0.425-beta makes windowed analytics correct and memory-bounded, adds
`SESSIONWINDOW`, extends the MQTT source (binary, delimited and protobuf payloads,
`meta(topic)`, multiple topics, session options), replaces the per-record MQTT sink
client with a persistent connection, and adds an offline sink cache with resend.
See CHANGELOG for details.

Benchmark: [test/benchmark/iiot-mqtt](test/benchmark/iiot-mqtt/README.md) compares
rekuiper with eKuiper 2.4.1, Telegraf 1.40.0 and Redpanda Connect 4.109.0 over MQTT
under identical limits, with configs, raw evidence and reproduction steps.

- Docker image: `ankurkrp/rekuiper:0.425-beta` (plus `latest`)
- Helm chart: `deploy/chart/ekuiper` (`AppVersion: 0.425-beta`)

---

# Release Notes - rekuiper v0.424-beta

`rekuiper` v0.424-beta fixes every documented compatibility defect found in the
0.423 evaluation against official LF Edge eKuiper 2.4.1: PostgreSQL float
sinks, SQL polling sources and lookup joins, windowed stream-stream joins,
array index/slice syntax, `json_path_query` wildcards, ruletest SSE on the
documented port 10081 with lossless replay, process-restart delivery resume,
CLI parity (`describe`/`-f`/`export`/`import`/`gettopo`/`validate`), and the
Docker-default `restIp`. See CHANGELOG for the per-defect list.

Built by **[I-Dacs Labs](https://i-dacs.com)**. Released under the permissive
**MIT License** (and dual Apache-2.0).

- Docker image: `ankurkrp/rekuiper:0.424-beta` (plus `latest`)
- Helm chart: `deploy/chart/ekuiper` (`AppVersion: 0.424-beta`)
- Precompiled binary targets: Linux x86_64, Windows x86_64, macOS x86_64 & Apple Silicon aarch64.

---

# Release Notes - rekuiper v0.423-beta (history)

`rekuiper` v0.423-beta resolves dynamic rule activation on start/restart, supports nested `{ "content": "..." }` and stringified rule map imports for both ruleset and configuration endpoints, and passes the evaluation probe suite used at that time (scoped; see TEST-CONTRACT-AUDIT where applicable).

Released under the permissive **MIT License** (and dual Apache-2.0). No throughput or parity multipliers are claimed here; see BENCHMARK-AUDIT.md for the fair over-HTTP audit.

---

## Scoped evaluation fixes across documented root causes

### 1. Data Plane & Remote Ingestion Integrity (D1)
- Fixed MQTT source broker resolution: `MqttConfig` now treats `topic` as optional with `#[serde(default)]`, allowing topic-less source configuration keys (`CONF_KEY`) to properly decode remote broker addresses (e.g. `{"server": "tcp://broker:1883"}`).
- Added case-insensitive lookup for stream options (`SERVER`, `server`, `CONF_KEY`, `conf_key`, `DATASOURCE`, `topic`).
- Completely resolved Docker container connection drops (`Connection refused`), restoring 100% remote broker delivery.

### 2. Complete REST Resource Updates & Dynamic Patching (D2)
- Added missing HTTP `PUT` handlers on `/streams/:name`, `/tables/:name`, `/rules/:name`, and `/connections/:id`, eliminating all 405 Method Not Allowed errors.
- Added `PATCH /configs` supporting dynamic configuration merging returning `HTTP 204 No Content` matching upstream eKuiper.

### 3. Comprehensive Import/Export Lifecycle (D3)
- Added `POST /data/export` and `POST /v2/data/export` with selective rule filtering (`{"rules": ["r1", "r2"]}`) and automatic dependency extraction (dependent streams, tables, and connections).
- Expanded `/data/import` to accept rules in both array and map forms (`{"id": def}`) with automated ID injection.
- Formatted import responses to match upstream envelopes (`ConfigResponse` and count-based ruleset strings).

### 4. Authentic Plugin & Service Validation (D4)
- Replaced mock/dummy 200/201 responses with genuine filesystem and URL validation.
- Missing files (`file:///nonexistent/...`) now reject with `HTTP 400` and detailed stat errors.
- Unregistered plugins, missing portable status, and unregistered function symbols strictly return `HTTP 404 Not Found`.

### 5. Input & Syntax Validation Parity (D5 & D7)
- Rule creation and update now validate stream existence, returning `HTTP 400` when referencing missing streams.
- Unknown SQL functions in rules are rejected (`HTTP 400` on create, `HTTP 422` on validate).
- `validate_rule` returns eKuiper's JSON envelope: `{"sources": [...], "valid": true}`.
- Ruletest creation rejects empty/invalid SQL with `HTTP 400`.
- Stream management queries (`SHOW STREAMS`, `DESCRIBE STREAM`) are executed via `POST /streams` with `HTTP 201`.
- Added support for the `->` arrow operator syntax (`a->b`, `a->'b'`, `a->b->c`) alongside standard dot notation (`a.b`).

### 6. Schema & Field Definition Preservation (D6)
- Retained parsed `StreamFields` (`[{"Name": "...", "FieldType": "..."}]`) in stream and table definitions.
- `GET /streams/:name` and `GET /tables/:name` return full `StreamFields` metadata and options.
- `GET /streams/:name/schema` and `GET /tables/:name/schema` return typed field maps (`{"col": {"type": "...", "index": 0}}`).
- Missing stream/table describe returns eKuiper's structured JSON 400 error envelope.

### 7. PostgreSQL & Relational Data Plane (D8)
- Real PostgreSQL `$1...$n` parameterized sink writes via `sqlx`.
- Type-safe lookup joins supporting both SQLite and PostgreSQL with cast conversions.
- Implemented `SqlSource` background polling actor for streams with `TYPE="sql"`.
- Added 3-second connection timeout guard to prevent pipeline deadlocks on unreachable databases.

### 8. Enterprise RSA/JWT Authentication Guard (D9)
- Implemented strict auth middleware active when `basic.authentication: true`.
- Enforces RS256 token verification against `etc/mgmt/public.pem` or `etc/public.pem`.
- Enforces token expiration (`exp`) and raw JWT token formatting (strictly rejecting `Bearer ` prefixes per eKuiper specification).
- Exempts public discovery routes (`GET /` and `GET /ping`).

### 9. Graceful Process Termination (D10)
- `POST /stop` and `GET /stop` now trigger a clean delayed shutdown (`std::process::exit(0)`), verifying exit code 0.

### 10. Live Ruletest Server-Sent Events (SSE) Server (D11)
- `POST /ruletest` binds an HTTP server on the returned port, streaming live rule test records as `text/event-stream` (`data: [...]\n\n`).
- Clean shutdown on `DELETE /ruletest/:name`.

### 11. Packaging & Environment Overrides (D12)
- Added multipart form extraction for `PUT /schemas/:type/:name/upload`.
- Added full support for `KUIPER__<SECTION>__<KEY>` environment variable overrides.

---

## Performance notes (scoped)

- Fair over-HTTP source-to-sink behavior is reported in BENCHMARK-AUDIT.md (no multipliers claimed).
- No latency-histogram p99 is claimed.
- Rust has no GC pauses by construction; no comparative GC-latency numbers are claimed.

---

## 📦 Artifacts & Distribution

- Multi-stage Docker image: `ankurkrp/rekuiper:0.423-beta`
- Helm chart: `deploy/chart/ekuiper` (`AppVersion: 0.423-beta`)
- Precompiled binary targets: Linux x86_64, Windows x86_64, macOS x86_64 & Apple Silicon aarch64.
