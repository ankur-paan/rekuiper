# Reliability Qualification, Scope & Verification Status

> **Release Version**: `0.501.0-beta`  
> **Status**: **Beta-Exit Qualification In Progress** (Not Declared GA)

This document defines the verified capabilities, known delivery and failure semantics, operational procedures, and the process-based reliability qualification harness for **rekuiper** (`0.501.0-beta`).

---

## 1. Implemented Capabilities vs. Verification Status

To ensure engineering transparency, this section distinguishes between **implemented capabilities**, **tests actually passed**, **real-broker verification**, and **outstanding qualification requirements**.

### 1.1 Verified Capabilities & Passed Tests

The following capabilities have been implemented and verified with passing test suites:

| Category | Component / Capability | Implementation Details | Verification Evidence |
| :--- | :--- | :--- | :--- |
| **Catalog Persistence** | `SqliteKvStore` / Manager Mutations | Atomic write-to-KV before in-memory commit; serial write locks prevent concurrent mutation divergence; automatic catalog restoration on restart. | `crates/rekuiper-core/tests/test_persistence.rs` (8/8 tests passed) |
| **Rule Lifecycle Consistency** | `RuleManager` & HTTP Routes | Updating running rules resumes stream processing with new SQL/actions and persists `"running"`; stopped rules stay stopped; persistence failures leave previous usable definitions intact; concurrent lifecycle operations serialize safely. | `crates/rekuiper-server/tests/test_rule_lifecycle.rs` (5/5 tests passed) |
| **Connection Consistency** | Connection & Configuration Routes | Single canonical persisted representation (`name/key`) with compatible alias resolution (`name.key`); storage write failures return 500 without partial state; mutations serialized under `config_op_lock`. | `crates/rekuiper-server/tests/test_config_consistency.rs` (6/6 tests passed) |
| **Transport Security** | MQTT TLS (Rustls 0.23 / 0.24) | Native & WebPKI CAs; custom Root CAs; mTLS client certificates/keys (SEC1, PKCS#1, PKCS#8); strict server name verification; `insecure_skip_verify` testing mode; secrets automatically redacted. | `crates/rekuiper-connectors/tests/test_mqtt_tls.rs` (9/9 tests passed) |
| **Real-Broker Integration** | Mosquitto MQTT Integration | QoS 0, 1, 2; wildcards (`+`, `#`); TLS and mTLS; offline sink caching during broker outage with ordered cache replay (`resendPriority: CacheFirst`). | `crates/rekuiper-server/tests/iiot_mqtt.rs` (4/4 tests passed with Mosquitto in WSL Docker) |
| **Process Qualification** | Process-Based Harness (`kuiperd`) | Real child process execution, continuous paced traffic, independent MQTT sink subscription, bounded in-flight crash accounting, storage fault injection, persistent engine logging, true post-restore broker recovery measurement, and strict acceptance criteria. | `crates/rekuiper-server/tests/qualification_harness.rs` (verified process qualification) |

### 1.2 Outstanding Qualification Blockers

The following qualification gates have **NOT** yet been completed and are required before removing the beta label:

1. **Pending 24h–72h Continuous Soak**: The process-based qualification harness is built and verified on short runs (5s–10s), but a continuous 24h–72h endurance soak run has **not** yet been executed.
2. **Outstanding Multi-Binary Upgrade/Rollback Qualification**: The harness supports upgrade/rollback verification via `REKUIPER_OLD_BIN` and `REKUIPER_NEW_BIN`, but formal verification across distinct version binaries is currently **PENDING**.
3. **Multi-Broker Interop Matrix**: Testing against cloud brokers (AWS IoT Core, HiveMQ, EMQX) beyond local Mosquitto is pending.

### 1.3 Experimental Features (Not Qualified for Production)

The following components are functional in `0.501.0-beta` but are explicitly **Experimental** and have not completed endurance qualification:

- **Kafka Source & Sink (`kafka`)**: Built on `rskafka`.
- **Redis Source & Sink (`redis`, `redisPub`)**: Built on `redis-rs`.
- **SQL Source & Sink (`sql`)**: Built on `sqlx` (PostgreSQL / SQLite).
- **WebSocket Sink (`websocket`)**: Built on `tokio-tungstenite`.

### 1.4 Unsupported Features in 0.501.0-beta

- **Shared Subscriptions**: `$share/group/topic` load-balanced subscriptions are not supported.
- **Clustered / Distributed Engine Mode**: Distributed Raft/consensus clustering is not supported.
- **Cross-Node State Migration**: In-memory stateful window states do not automatically migrate between separate physical nodes on hardware failure.

---

## 2. Actual Delivery Semantics & Failure Behavior

### 2.1 MQTT Broker QoS vs. End-to-End Delivery

- **MQTT QoS 0 (At most once)**: Published without broker acknowledgment. Packet loss during network disconnects is expected.
- **MQTT QoS 1 (At least once)**: Delivered to broker and acknowledged via `PUBACK`. If connection drops before `PUBACK`, retransmission may produce duplicate broker deliveries.
- **MQTT QoS 2 (Exactly once at MQTT transport)**: Four-step handshake (`PUBLISH` -> `PUBREC` -> `PUBREL` -> `PUBCOMP`) guarantees no duplicates across the network hop.

> [!IMPORTANT]
> A QoS 1 or 2 MQTT sink guarantees delivery to the downstream broker once a record leaves the engine. It does **not** protect in-flight RAM records from sudden power loss or process kill without upstream persistent source replay.

### 2.2 Broker Outage & Sink Caching Semantics

When downstream MQTT brokers experience transient outages:
1. **Buffering**: When `enableCache: true` is configured, outgoing records are enqueued in the sink's FIFO cache (in-memory up to `memoryCacheThreshold`, spilling to disk pages in `data/cache/`).
2. **Resend Priority**: `resendPriority: 1` (`CacheFirst`) replays cached outage records in strict sequence before live records are emitted.
3. **Resend Indicator**: Replayed records set `"resent": true` (via `resendIndicatorField`).
4. **Drop Policy**: If `maxCacheSize` is exceeded, records are dropped per policy (`dropOldest` or `dropCurrent`) with Prometheus counters incremented.

### 2.3 Abrupt Process Termination & Power-Loss Behavior

| System Component | State Location | Behavior on Abrupt Crash / SIGKILL / Power Loss |
| :--- | :--- | :--- |
| **Catalog Definitions** (Streams, Rules, Tables, Schemas, Configs) | SQLite KV (`data/sqliteKV.db` with WAL) | **100% Preserved**. SQLite WAL ensures committed catalog definitions survive crash and reload on restart. |
| **Rule Lifecycle Status** | SQLite KV (`rules` namespace) | **Preserved**. Rules in `"running"` status before shutdown automatically re-arm and resume execution upon startup. Stopped rules remain stopped. |
| **Sink Cache on Disk** | Filesystem (`data/cache/`) | **Preserved**. Disk cache pages survive daemon restart; pending cached records replay upon broker reconnection. |
| **In-Flight RAM Stream Records** | Tokio MPSC Channels / Memory | **Lost**. In-memory records that entered the engine but were not yet emitted to sinks prior to sudden power loss are lost. Lossless pipelines require upstream persistent QoS 1 buffering. |
| **Window State in RAM** | Memory Aggregators | **Reset**. Unmaterialized tumbling/hopping window buffers in RAM reset on process restart; materialized output already sent to sinks is unaffected. |

---

## 3. Operational Procedures

### 3.1 Backup Procedures

```bash
# 1. Online SQLite KV backup using SQLite CLI
sqlite3 /var/lib/rekuiper/data/sqliteKV.db ".backup /var/backups/rekuiper/sqliteKV_$(date +%Y%m%d_%H%M%S).db"

# 2. Backup schema and configuration assets
tar -czf /var/backups/rekuiper/etc_$(date +%Y%m%d).tar.gz -C /etc/rekuiper etc/
```

### 3.2 Upgrade & Rollback Procedures

1. **Stop Running Engine**: `systemctl stop rekuiper`
2. **Perform Backup**: Run backup procedure above.
3. **Replace Binary**: Install new `kuiperd` binary into execution path.
4. **Start Engine**: `systemctl start rekuiper`
5. **Verify Startup**:
   - `curl -s http://localhost:9081/ping` -> returns 200 OK.
   - `curl -s http://localhost:9081/rules/status/all` -> verify previously running rules resumed `"running"`.
6. **Rollback if Needed**: If unexpected regression occurs, restore previous binary and snapshot `sqliteKV.db`, then restart.

---

## 4. Automated Reliability Qualification Harness

The test suite includes a process-based qualification harness (`crates/rekuiper-server/tests/qualification_harness.rs`):

- **Process-Based**: Launches `kuiperd` child process with isolated config, storage, and ephemeral ports.
- **Paced Continuous Traffic**: Streams records at the requested rate (`msg/s`) for the actual requested duration (`Duration`).
- **Independent MQTT Subscriber**: Connects `rumqttc::AsyncClient` directly to the broker to validate sequence IDs, duplicates, and ordering.
- **Crash Loss Accounting**: Abruptly kills `kuiperd` child process, measures bounded in-flight records, and verifies restart recovery.
- **Broker Outage & Cache Replay**: Simulates broker connection drop and verifies reconnected cache drain.
- **Storage Fault Injection**: Verifies `ENOSPC` disk full simulation fails gracefully with 500 without leaving corrupted memory state.
- **Machine-Readable Reports**: Logs source git commit, dirty tree status, binary path, host hardware specs, and timing to `target/qualification_results.json`.

### 4.1 Running Qualification Tests

```bash
# Short qualification test (Linux / macOS)
./scripts/run_qualification.sh short 10 100

# Short qualification test (Windows PowerShell)
.\scripts\run_qualification.ps1 -Mode short -Duration 10 -Rate 100

# Dedicated 24h soak run (when ready on dedicated host)
./scripts/run_qualification.sh soak 86400 100 /var/log/rekuiper_soak_24h.json
```
