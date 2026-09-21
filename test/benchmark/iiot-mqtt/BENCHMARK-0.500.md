# rekuiper 0.500 MQTT benchmark results

This report contains the measurements published for `0.500-beta`. This release introduces
an in-memory Redis-style catalog and zero-disk hot path architecture:
- In-memory SQLite metadata store (`MemoryCatalog`) with WAL pragma tuning, eliminating disk I/O on stream and rule validation paths.
- Hot-path caching for authentication keys, rule configurations, and shared database connection pools.
- Multi-row SQL batch insertions and expanded queue depth (32,768) preventing channel backpressure during burst ingest.

## Setup

- Release candidate built from the working tree using [`Dockerfile.bench`](Dockerfile.bench).
- Engine container: one pinned CPU, 1 GiB memory, `--memory-swap=1g`, and `TOKIO_WORKER_THREADS=1`.
- Rust `mqttgen` publisher: eight MQTT 3.1.1 QoS 0 connections on host cores 10-11, outside the engine container.
- Mosquitto: separate container on cores 8-9 with a 4,096-message / 1 MiB outgoing queue limit.
- Sink: JSON Lines on WSL ext4. The proof parses sink output and validates counts, unique IDs/topics, devices, duplicates, and aggregate sums for W1-W5.
- `sustained=true` criteria: 0.0% loss, on-schedule generator, exact sink proof, zero rule exceptions, bounded broker, and upstream gap <= 4,096 at send end (or real-time drain within post-send window).

The raw reports are in [`evidence/`](evidence/):

- [`perf-iot-mqtt-current-inmemory.json`](evidence/perf-iot-mqtt-current-inmemory.json) (ladder 5k-100k)
- [`perf-iot-mqtt-current-bounded-peak-inmem.json`](evidence/perf-iot-mqtt-current-bounded-peak-inmem.json) (125k-200k coarse trials)
- [`perf-w1-10k.json`](evidence/perf-w1-10k.json), [`perf-w1-25k.json`](evidence/perf-w1-25k.json), [`perf-w1-1k.json`](evidence/perf-w1-1k.json) (W1 peak search)
- [`perf-w2-10k.json`](evidence/perf-w2-10k.json) (W2 peak search)
- [`perf-w3-10k.json`](evidence/perf-w3-10k.json), [`perf-w3-25k.json`](evidence/perf-w3-25k.json), [`perf-w3-1k.json`](evidence/perf-w3-1k.json) (W3 peak search)
- [`perf-w4-10k.json`](evidence/perf-w4-10k.json) (W4 peak search)
- [`perf-w5-10k.json`](evidence/perf-w5-10k.json), [`perf-w5-25k.json`](evidence/perf-w5-25k.json), [`perf-w5-1k.json`](evidence/perf-w5-1k.json), [`perf-w5-1k-2.json`](evidence/perf-w5-1k-2.json) (W5 peak search)

## Published ladder repeated (0.426 vs 0.500)

All 20 W1-W5 steps at 5k, 20k, 50k, and 100k messages/s produced exact sink proofs with zero
rule exceptions and 0.00% loss.

| Workload | Rate | 0.426 CPU | 0.500 CPU | CPU delta | 0.426 Anon MiB | 0.500 Anon MiB | Loss | Status |
|---|---:|---:|---:|---:|---:|---:|---:|---|
| W1 | 5k | 14.6% | 17.2% | +2.6% | 4.6 | 4.4 | 0.00% | PASS |
| W1 | 20k | 42.3% | 45.4% | +3.1% | 4.6 | 4.4 | 0.00% | PASS |
| W1 | 50k | 63.4% | 57.4% | -6.0% | 5.3 | 4.4 | 0.00% | PASS |
| W1 | 100k | 88.5% | 79.4% | -9.1% | 8.5 | 4.6 | 0.00% | PASS |
| W2 | 5k | 12.7% | 15.4% | +2.7% | 5.2 | 5.0 | 0.00% | PASS |
| W2 | 20k | 38.4% | 42.7% | +4.3% | 5.7 | 6.4 | 0.00% | PASS |
| W2 | 50k | 54.7% | 51.1% | -3.6% | 5.7 | 5.1 | 0.00% | PASS |
| W2 | 100k | 80.1% | 70.9% | -9.2% | 5.8 | 5.1 | 0.00% | PASS |
| W3 | 5k | 28.2% | 16.2% | -12.0% | 4.8 | 4.5 | 0.00% | PASS |
| W3 | 20k | 42.4% | 50.3% | +7.9% | 4.8 | 4.5 | 0.00% | PASS |
| W3 | 50k | 67.1% | 70.7% | +3.6% | 7.8 | 4.8 | 0.00% | PASS |
| W3 | 100k | 93.6% | 99.3% | +5.7% | 9.3 | 57.6 | 0.00% | PASS |
| W4 | 5k | 13.7% | 16.4% | +2.7% | 7.6 | 8.0 | 0.00% | PASS |
| W4 | 20k | 41.3% | 41.1% | -0.2% | 10.1 | 10.2 | 0.00% | PASS |
| W4 | 50k | 66.2% | 57.5% | -8.7% | 14.3 | 10.8 | 0.00% | PASS |
| W4 | 100k | 81.5% | 87.5% | +6.0% | 12.4 | 12.4 | 0.00% | PASS |
| W5 | 5k | 14.0% | 19.6% | +5.6% | 6.0 | 6.7 | 0.00% | PASS |
| W5 | 20k | 37.3% | 50.3% | +13.0% | 6.5 | 7.3 | 0.00% | PASS |
| W5 | 50k | 55.8% | 61.8% | +6.0% | 7.4 | 6.3 | 0.00% | PASS |
| W5 | 100k | 79.4% | 83.3% | +3.9% | 6.1 | 6.8 | 0.00% | PASS |

At 100k msg/s on W1 (telemetry filter) and W2 (device windows), CPU utilization decreased by
more than 9 percentage points due to zero-disk hot-path optimizations and channel buffer sizing.

## Exact Peak Capacity Limits

To determine the true ceiling of each workload beyond fixed ladder points, a hierarchical
binary search (10,000 -> 2,500 -> 1,000 msg/s increments) was executed under the bounded broker
(4,096-message queue cap).

| Workload | Highest Certified Rate | Failure Rate | Limiting Factor | Peak CPU | Peak Anon MiB |
|---|---:|---:|---|---:|---:|
| **W1 (telemetry filter)** | **150,000 msg/s** | 151,000 msg/s | CPU saturation (99.4%), broker drop (20.53% loss) | 94.5% | 17.4 |
| **W2 (device 10s windows)** | **200,000 msg/s** | 210,000 msg/s | Generator schedule (engine lossless to 240k, fails at 250k) | 94.4% | 6.7 |
| **W3 (ESPHome 10k topics)** | **150,000 msg/s** | 151,000 msg/s | CPU saturation (99.3%), broker drop (5.05% loss) | 97.4% | 16.6 |
| **W4 (vehicles 10k VINs)** | **200,000 msg/s** | 210,000 msg/s | Generator schedule (engine lossless to 220k) | 97.5% | 18.1 |
| **W5 (charger sessions)** | **126,000 msg/s** | 127,000 msg/s | Window close lag (16.0s exceeds stability limit) | 86.7% | 6.0 |

### Workload Details

1. **W1 (Telemetry Filter)**: Sustained 150,000 msg/s with 0.00% loss, 94.5% CPU, and 1.0s lag.
   At 151,000 msg/s, the single pinned core reaches 99.4% CPU utilization, causing Mosquitto's
   4,096-message outgoing queue to overflow with 20.53% packet loss. 150,000 msg/s is the exact physical ceiling.
2. **W2 (Per-Device Windows)**: Successfully sustained 200,000 msg/s with 0.00% loss, 94.4% CPU,
   and 6.7 MiB anonymous memory. While the engine successfully ingests and outputs 100% of messages
   at 210,000–240,000 msg/s, the publisher generator falls off schedule on this host. 200,000 msg/s is
   the certified on-schedule ceiling.
3. **W3 (ESPHome Plain-Text Topics)**: Sustained 150,000 msg/s with 0.00% loss, 97.4% CPU, and an end
   source gap of only 15 messages out of 4.5 million. At 151,000 msg/s, single-core CPU saturates (99.3%)
   and broker queue drops 5.05% of traffic.
4. **W4 (Vehicle 10k VIN Windows)**: Sustained 200,000 msg/s with 0.00% loss, 97.5% CPU, and 18.1 MiB
   anonymous memory. Beyond 200k, the external generator fails schedule timing on this host.
5. **W5 (EV Charger Sessions)**: Evaluated with fine-grained 1k steps. 125,000 msg/s and 126,000 msg/s
   passed with 0.00% loss and post-send lags of 3.0s and 4.0s respectively. At 127,000 msg/s, session
   drain lag rises to 16.0s (violating the 5.0s stability threshold). 126,000 msg/s is the certified limit.
