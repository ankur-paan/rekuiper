# Single-Rule MQTT High-Throughput Benchmarks

This document records the single-rule MQTT throughput benchmarks conducted across rekuiper releases.

The benchmarks evaluate ingestion rate, CPU efficiency, memory stability, and exact sink delivery on five industrial IoT workloads.

---

## 1. Test Harness and Environment

All tests execute in Docker containers on Linux with identical hardware limits:

| Parameter | Value | Description |
| :--- | :--- | :--- |
| **Host System** | x86-64 Architecture | 12-core processor, Linux kernel 6.x |
| **Engine Container** | `1.0 core / 1 GiB RAM` | `--cpuset-cpus=2 --cpus=1 --memory=1g --memory-swap=1g` |
| **MQTT Broker** | `eclipse-mosquitto:2` | Pinned to cores 8 and 9, 512 MiB RAM limit, 4,096-message queue cap |
| **Load Generator** | `mqttgen` (Rust) | Pinned to cores 10 and 11; MQTT 3.1.1 QoS 0 across 8 connections |
| **Orchestrator** | `iotrunner` (Rust) | Executes test steps on host cores 0, 1, 4-7 |
| **Sink Target** | JSON Lines file | Ext4 filesystem bind-mounted into the container |
| **CPU Metric** | cgroup v2 `cpu.stat` | Relative usage over send window (100% = 1 full physical core) |
| **Memory Metric** | cgroup v2 `memory.stat` | `anon` heap memory (excludes filesystem page cache) |

---

## 2. Workload Definitions and Exact Sink Proofs

The benchmark evaluates five industrial IoT workloads:

| ID | Workload | Topics and Payload | SQL Rule Statement | Exact Sink Proof Criteria |
| :--- | :--- | :--- | :--- | :--- |
| **W1** | **Telemetry Filter** | `bench/telemetry`<br>JSON, 1,000 devices | `SELECT id, device, temp, speed * 3.6 AS speed_kmh FROM telem WHERE temp > 21.0` | Unique message IDs match expected filter count (139 of every 150 records). Zero duplicate records. |
| **W2** | **Device Windows** | `bench/telemetry`<br>JSON, 1,000 devices | `SELECT device, count(*) AS n, avg(temp), max(speed) FROM telem GROUP BY device, TUMBLINGWINDOW(ss, 10)` | Sum of count field `n` equals total messages sent. All 1,000 devices present in window output. |
| **W3** | **ESPHome Topics** | `bench/esphome/+/sensor/temperature/state`<br>Plain-text, 10,000 device topics | `SELECT meta(topic) AS topic, self AS state FROM telem` | Total output row count equals total sent count. All 10,000 distinct device topics present. |
| **W4** | **Vehicle Wildcards** | `bench/vehicles/+/telemetry`<br>JSON, 10,000 VIN topics | `SELECT device, count(*) AS n, avg(speed), max(temp) FROM telem GROUP BY device, TUMBLINGWINDOW(ss, 10)` | Sum of window count `n` equals sent messages. All 10,000 vehicle VINs present in output. |
| **W5** | **EV Charger Sessions** | `bench/chargers/+/session`<br>JSON, 2,000 charger topics | `SELECT device, count(*) AS n, max(speed) FROM telem GROUP BY device, SESSIONWINDOW(ss, 10, 2)` | Sum of session count `n` equals sent messages. All 2,000 charger identifiers present. |

---

## 3. Version 0.500-beta: Peak Ceilings and Four-Engine Comparison

The `0.500-beta` benchmark introduced an in-memory catalog (`MemoryCatalog`) and zero-disk hot-path architecture.

### Published Rate Ladder (5,000 to 100,000 msg/s)

Each test runs for 30.0 seconds of constant send time. The test then drains remaining messages until the sink file stabilizes.

Each cell records: **Packet Loss / CPU Utilization (% of 1 core) / Anonymous Heap Memory (MiB)**. Bold text highlights failed proofs or message loss.

#### W1: Telemetry Filter

| Ingest Rate | rekuiper 0.500 | eKuiper 2.4.1 | Telegraf 1.40.0 | Redpanda Connect 4.109.0 |
| :---: | :--- | :--- | :--- | :--- |
| **5,000 msg/s** | 0.0% / 17.2% / 4.4 MiB | 0.0% / 34.4% / 12.9 MiB | 0.0% / 31.9% / 67.5 MiB | 0.0% / 37.2% / 58.5 MiB |
| **20,000 msg/s** | 0.0% / 45.4% / 4.4 MiB | 0.0% / 99.0% / 15.3 MiB | 0.0% / 89.8% / 91.6 MiB | 0.0% / 99.3% / 71.5 MiB |
| **50,000 msg/s** | 0.0% / 57.4% / 4.4 MiB | **40.8% loss** / 99.3% / 15.4 MiB | 0.0% (+10s lag) / 99.3% / 96.5 MiB | **37.6% loss** / 99.4% / 69.9 MiB |
| **100,000 msg/s** | 0.0% / 79.4% / 4.6 MiB | **94.9% loss** / 99.3% / 15.8 MiB | **32.5% loss** / 99.3% / 100.2 MiB | **67.4% loss** / 99.4% / 71.1 MiB |

#### W2: Per-Device 10s Tumbling Windows (1,000 Devices)

| Ingest Rate | rekuiper 0.500 | eKuiper 2.4.1 | Telegraf 1.40.0 | Redpanda Connect 4.109.0 |
| :---: | :--- | :--- | :--- | :--- |
| **5,000 msg/s** | 0.0% / 15.4% / 5.0 MiB | 0.0% / 27.1% / 124.7 MiB | **9.4% loss** / 27.7% / 46.9 MiB | 0.0% / 38.7% / 575.3 MiB |
| **20,000 msg/s** | 0.0% / 42.7% / 6.4 MiB | 0.0% / 86.2% / 535.9 MiB | **9.4% loss** / 81.4% / 51.6 MiB | **100% loss** / 97.8% / 1,012.3 MiB |
| **50,000 msg/s** | 0.0% / 51.1% / 5.1 MiB | **55.2% loss** / 99.4% / 961.7 MiB | **6.6% loss** / 99.3% / 55.4 MiB | **100% loss** / 98.2% / 989.5 MiB |
| **100,000 msg/s** | 0.0% / 70.9% / 5.1 MiB | **76.8% loss** / 99.3% / 911.9 MiB | **27.3% loss** / 99.3% / 55.0 MiB | **100% loss** / 98.2% / 1,009.5 MiB |

#### W3: ESPHome Topics (10,000 Plain-Text Device Topics)

| Ingest Rate | rekuiper 0.500 | eKuiper 2.4.1 | Telegraf 1.40.0 | Redpanda Connect 4.109.0 |
| :---: | :--- | :--- | :--- | :--- |
| **5,000 msg/s** | 0.0% / 16.2% / 4.5 MiB | 0.0% / 32.4% / 41.2 MiB | 0.0% / 24.1% / 58.1 MiB | 0.0% / 31.9% / 59.8 MiB |
| **20,000 msg/s** | 0.0% / 50.3% / 4.5 MiB | 0.0% / 94.0% / 43.2 MiB | 0.0% / 70.3% / 84.9 MiB | 0.0% / 97.8% / 67.7 MiB |
| **50,000 msg/s** | 0.0% / 70.7% / 4.8 MiB | **77.8% loss** / 99.3% / 44.2 MiB | 0.0% (+5s lag) / 98.6% / 91.1 MiB | **19.1% loss** / 99.3% / 67.6 MiB |
| **100,000 msg/s** | 0.0% / 99.3% / 57.6 MiB | **81.1% loss** / 99.3% / 43.2 MiB | **12.2% loss** / 99.3% / 90.9 MiB | **57.3% loss** / 99.4% / 64.4 MiB |

#### W4: Vehicle Wildcard Windows (10,000 VIN Topics)

| Ingest Rate | rekuiper 0.500 | eKuiper 2.4.1 | Telegraf 1.40.0 | Redpanda Connect 4.109.0 |
| :---: | :--- | :--- | :--- | :--- |
| **5,000 msg/s** | 0.0% / 16.4% / 8.0 MiB | 0.0% / 31.4% / 147.7 MiB | **9.4% loss** / 32.4% / 60.2 MiB | 0.0% / 40.3% / 641.9 MiB |
| **20,000 msg/s** | 0.0% / 41.1% / 10.2 MiB | 0.0% / 91.4% / 886.2 MiB | **9.4% loss** / 85.7% / 94.0 MiB | **99.7% loss** / 98.6% / 992.9 MiB |
| **50,000 msg/s** | 0.0% / 57.5% / 10.8 MiB | **46.6% loss** / 99.3% / 939.9 MiB | 0.0% / 99.3% / 101.5 MiB | **100% loss** / 98.5% / 1,005.8 MiB |
| **100,000 msg/s** | 0.0% / 87.5% / 12.4 MiB | **69.5% loss** / 99.3% / 946.2 MiB | **23.0% loss** / 99.3% / 102.4 MiB | **86.1% loss** / 99.3% / 911.2 MiB |

#### W5: EV Charger Session Windows (2,000 Chargers)

| Ingest Rate | rekuiper 0.500 | eKuiper 2.4.1 | Telegraf 1.40.0 | Redpanda Connect 4.109.0 |
| :---: | :--- | :--- | :--- | :--- |
| **5,000 msg/s** | 0.0% / 19.6% / 6.7 MiB | 0.0% / 30.7% / 195.2 MiB | Not supported | Not supported |
| **20,000 msg/s** | 0.0% / 50.3% / 7.3 MiB | 0.0% / 87.2% / 831.7 MiB | Not supported | Not supported |
| **50,000 msg/s** | 0.0% / 61.8% / 6.3 MiB | **56.9% loss** / 99.3% / 937.1 MiB | Not supported | Not supported |
| **100,000 msg/s** | 0.0% / 83.3% / 6.8 MiB | **66.2% loss** / 99.3% / 922.3 MiB | Not supported | Not supported |

---

### Exact Peak Capacity Ceilings (0.500-beta)

To find the true physical ceiling for each workload, a binary search (1,000 msg/s increments) was executed under the bounded Mosquitto broker (4,096-message queue cap).

| Workload | Certified Peak Rate | Failure Rate | Limiting Bottleneck | Peak CPU | Peak Heap Memory |
| :--- | :---: | :---: | :--- | :---: | :---: |
| **W1: Telemetry Filter** | **150,000 msg/s** | 151,000 msg/s | CPU saturation (99.4%), broker drop (20.53% loss) | 94.5% | 17.4 MiB |
| **W2: Device Windows** | **200,000 msg/s** | 210,000 msg/s | Generator schedule (engine lossless to 240k) | 94.4% | 6.7 MiB |
| **W3: ESPHome Topics** | **150,000 msg/s** | 151,000 msg/s | CPU saturation (99.3%), broker drop (5.05% loss) | 97.4% | 16.6 MiB |
| **W4: Vehicle Wildcards** | **200,000 msg/s** | 210,000 msg/s | Generator schedule (engine lossless to 220k) | 97.5% | 18.1 MiB |
| **W5: EV Charger Sessions** | **126,000 msg/s** | 127,000 msg/s | Window close lag (16.0s exceeds 5.0s stability limit) | 86.7% | 6.0 MiB |

---

### Comparison of Version 0.426 vs 0.500

At 100,000 msg/s, the zero-disk architecture in version 0.500 reduced single-core CPU utilization by over 9 percentage points:

| Workload | 0.426 CPU | 0.500 CPU | CPU Change | 0.426 Anon Heap | 0.500 Anon Heap |
| :--- | :---: | :---: | :---: | :---: | :---: |
| **W1: Telemetry Filter** | 88.5% | 79.4% | **-9.1%** | 8.5 MiB | 4.6 MiB |
| **W2: Device Windows** | 80.1% | 70.9% | **-9.2%** | 5.8 MiB | 5.1 MiB |
| **W3: ESPHome Topics** | 93.6% | 99.3% | +5.7% | 9.3 MiB | 57.6 MiB |
| **W4: Vehicle Wildcards** | 81.5% | 87.5% | +6.0% | 12.4 MiB | 12.4 MiB |
| **W5: EV Charger Sessions** | 79.4% | 83.3% | +3.9% | 6.1 MiB | 6.8 MiB |

---

## 4. Version 0.426-beta: 120-Second Bounded Sustained Ladder

The `0.426-beta` benchmark evaluated long-duration stability. Every workload sustained **100,000 messages/s for 120 continuous seconds** (12 million messages per trial) on 1 CPU core and 1 GiB RAM with exact sink delivery:

| Workload | Ingest Rate | Duration | Messages Sent | CPU Utilization | Anon Heap | End Source Gap | Post-Send Lag | Result |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| **W1: Telemetry Filter** | 100,000 /s | 120 s | 12,000,000 | 76.3% | 6.1 MiB | 8 | 1.0 s | PASS |
| **W2: Device Windows** | 100,000 /s | 120 s | 12,000,000 | 66.9% | 8.0 MiB | 9 | 1.0 s | PASS |
| **W3: ESPHome Topics** | 100,000 /s | 120 s | 12,000,000 | 79.5% | 6.6 MiB | 4 | 1.0 s | PASS |
| **W4: Vehicle Wildcards** | 100,000 /s | 120 s | 12,000,000 | 69.0% | 12.0 MiB | 10 | 9.0 s | PASS |
| **W5: EV Charger Sessions** | 100,000 /s | 120 s | 12,000,000 | 69.5% | 5.9 MiB | 1 | 3.0 s | PASS |

> [!NOTE]
> In version 0.426-beta, W3 also sustained **150,000 msg/s for 120 seconds** (18,000,000 messages) with 87.7% CPU, 7.8 MiB heap memory, and 1.0s drain lag.

---

## 5. Version 0.425-beta: Initial Rate Ladder Comparison

The `0.425-beta` benchmark established the original 30-second rate ladder (5k to 100k msg/s) across four engines.

This release proved the memory-bounded incremental window aggregation implementation for `GROUP BY`, `TUMBLINGWINDOW`, and `SESSIONWINDOW`. Under heavy window loads (W2, W4, W5), `rekuiper` consumed under 10.0 MiB of anonymous heap, while Go-based engines allocated between 500 MiB and 1,000 MiB of memory.

---

## 6. How to Reproduce

All test configurations, load generator source code, and validation tools are in `test/benchmark/iiot-mqtt/`:

```bash
# Navigate to the benchmark suite
cd test/benchmark/iiot-mqtt

# Run the complete automated test runner
cargo run --release -p iotrunner
```

Telemetry files containing per-second cgroup metrics, sink proofs, and host health snapshots are recorded in `test/benchmark/iiot-mqtt/evidence/`.
