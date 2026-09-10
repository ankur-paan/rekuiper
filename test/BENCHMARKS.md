# ⚡ Streaming Benchmarks & Reproducibility Guide

This document presents the **100% empirical, zero-assumption head-to-head performance benchmarks** evaluating `rekuiper` against **Apache Flink**, **Upstream Go eKuiper**, **Redpanda Connect (Benthos)**, and **Telegraf**.

All benchmarks are reproducible on any Linux or WSL2 environment using the automated scripts included in [`test/benchmark/`](test/benchmark/).

---

## 📊 Benchmark Summary (500,000 Events Head-to-Head)

### Workload & Hardware Environment
- **Dataset**: 500,000 wide-schema telemetry records (`telemetry_500k.jsonl`, 17.06 MB).
- **Pipeline**: Ingest 500,000 events $\rightarrow$ Parse JSON $\rightarrow$ Compute arithmetic formula (`temp * 1.8 + 32.0 AS temp_f`) $\rightarrow$ Filter predicate (`temp > 20.0`) $\rightarrow$ Project fields (`id`, `temp_f`) $\rightarrow$ Sink / Blackhole.
- **Host**: Linux (WSL2 / Ubuntu x86_64, 8 physical cores allocated, native ext4 filesystem).

| Engine | Runtime / Language | 500k Elapsed Time | Throughput (1 Core) | Data Drops / Loss | Memory Footprint (Idle) | Cold Boot Time | Speedup vs Competitor |
| :--- | :--- | :---: | :---: | :---: | :---: | :---: | :---: |
| **`rekuiper` (v0.422-beta)** | **Pure Rust** | **1.176 s** (1,175.6 ms) | **425,308 events/sec** | **0 (0.0% loss)** | **~6 – 8.2 MB** | **~13 ms** (internal) / 123 ms (spawn) | **Baseline (Fastest)** |
| **Apache Flink (v2.3.0)** | Java / Scala (JVM) | **2.144 s** *(vertex)* / 2.940 s *(job)* | **233,209 eps** *(vertex)* / 170,068 eps | 0 (0.0% loss) | **~1,022 MB (1.02 GB)** | ~15 – 30 seconds *(cluster spinup)* | **`rekuiper` is 1.8x – 2.5x faster** |
| **Upstream eKuiper (v2.4.1)** | Go (official `lfedge/ekuiper`) | **11.290 s** (11,290 ms) | **44,287 events/sec** | **72,921 drops (14.6% loss)** *(buffer saturation)* | ~45 – 85 MB | ~1,200 ms | **`rekuiper` is 9.6x faster** |
| **Telegraf (v1.40.0)** | Go (official `telegraf`) | **8.194 s** (8,194 ms) | **61,019 events/sec** *(ingest only)* | 0 (0.0% loss) | ~50 – 80 MB | ~450 ms | **`rekuiper` is 7.0x faster** |
| **Redpanda Connect (Benthos)** | Go (official `redpandadata/connect`)| **19.236 s** (19,236 ms) | **25,993 events/sec** | 0 (0.0% loss) | ~38 – 70 MB | ~350 ms | **`rekuiper` is 16.4x faster** |

---

## 🔍 In-Depth Architectural Findings

### 1. What Happened with Apache Flink?
- **Throughput vs Overhead**: Apache Flink is an enterprise-grade distributed streaming engine designed for massive distributed clusters. In our tests with Flink 2.3.0, the core execution graph processed the 500,000 records in **2.144 seconds** (233,209 events/sec).
- **Resource Footprint**: To achieve this, Flink required spinning up a JobManager and TaskManager running on the Java Virtual Machine. Together they consumed **1,022.4 MiB (~1.02 GB) of RAM** (`flink-jm`: 344 MB, `flink-tm`: 678 MB).
- **Boot Latency**: Spinning up the Flink JVM cluster, establishing RPC bindings, and deploying the job topology took **15 to 30 seconds**.
- **Edge Suitability**: While Flink is capable of high throughput in datacenters, its 1 GB+ RAM footprint and 20-second startup latency make it completely unviable for resource-constrained industrial gateways, robots, and edge devices. `rekuiper` achieves **1.8x – 2.5x higher throughput** while using **125x less RAM** (< 8.2 MB) and booting in **13 milliseconds**.

### 2. What Happened with Upstream Go eKuiper?
- **Throughput**: Upstream Go eKuiper processed the 500,000 dataset in **11.290 seconds** (44,287 events/sec).
- **Channel Saturation & Data Loss**: Under continuous 500,000 record burst ingestion, Go eKuiper's internal Go channel buffers saturated:
  ```text
  "source_stream_500k_0_exceptions_total": 72410,
  "source_stream_500k_0_last_exception": "buffer full, drop message tt 1789008612983, uid from stream_500k to rule_500k.1_2_decoder"
  ```
  Out of 500,000 source records, **72,921 records were dropped** (14.6% data loss). Only 427,079 records reached the sink.
- In contrast, `rekuiper` processed all 500,000 records with **zero drops (100% data integrity)** in **1.176 seconds** (**9.6x faster**).

### 3. What Happened with Redpanda Connect (Benthos)?
- Redpanda Connect executed the identical Bloblang transformation (`root.temp_f = (this.temp * 1.8) + 32`) and JSON filter (`this.temp > 20.0`) in **19.236 seconds** (**25,993 events/sec**).
- `rekuiper` outperformed Redpanda Connect by **16.4x**.

### 4. What Happened with Telegraf?
- Telegraf parsed line-delimited JSON using `data_format = "json_v2"` and discarded it in **8.194 seconds** (**61,019 events/sec**). Note that Telegraf did not compute the arithmetic conversion formula or evaluate SQL expressions.
- `rekuiper` is **7.0x faster** than Telegraf even while performing full arithmetic calculation and SQL filtering.

---

## 🔬 Cold Boot & Memory Footprint

Measured with [`test/benchmark/measure_cold_boot.py`](test/benchmark/measure_cold_boot.py) across multiple cold invocations:

```text
=== Cold Boot & Idle Memory Benchmark ===
Binary: target/release/kuiperd
Test Port: 9092

Run 1: End-to-End = 124.12 ms | Internal Bootstrap = 13.00 ms | RSS = 6.42 MB
Run 2: End-to-End = 121.85 ms | Internal Bootstrap = 12.00 ms | RSS = 6.38 MB
Run 3: End-to-End = 125.40 ms | Internal Bootstrap = 14.00 ms | RSS = 6.45 MB
Run 4: End-to-End = 122.90 ms | Internal Bootstrap = 13.00 ms | RSS = 6.40 MB
Run 5: End-to-End = 124.65 ms | Internal Bootstrap = 13.00 ms | RSS = 6.41 MB

--- Summary ---
End-to-End Process Spawn: min=121.85 ms, avg=123.78 ms, max=125.40 ms
Internal Daemon Init:     min=12.00 ms,  avg=13.00 ms,  max=14.00 ms
Idle Memory RSS:          avg=6.41 MB
```

---

## 🧪 How to Reproduce All Benchmarks

All test scripts are located in the [`test/benchmark/`](test/benchmark/) folder. Anyone can clone this repository and run the full head-to-head evaluation.

### Prerequisites
- **Docker** (for running competitive engines: Flink, Go eKuiper, Redpanda Connect, Telegraf).
- **Python 3.8+** with `requests` (`pip install requests`).
- **Rust toolchain** (to build `rekuiper`).

### 1. One-Click Full Benchmark Suite
Run the master script to generate the dataset and benchmark all engines:

```bash
# On Linux / WSL2:
chmod +x test/benchmark/run_all.sh
./test/benchmark/run_all.sh

# Or via Python directly:
python3 test/benchmark/run_all.py
```

### 2. Running Individual Benchmarks

#### A. rekuiper (Rust)
```bash
cargo test --release --test perf_throughput -- --nocapture
```

#### B. Apache Flink
```bash
python3 test/benchmark/bench_flink.py
```

#### C. Upstream Go eKuiper
```bash
# Start upstream container if not already running:
docker run -d --name ekuiper -p 9081:9081 lfedge/ekuiper:2.4.1-slim

# Run benchmark:
python3 test/benchmark/bench_ekuiper.py
```

#### D. Redpanda Connect (Benthos)
```bash
python3 test/benchmark/bench_benthos.py
```

#### E. Telegraf
```bash
python3 test/benchmark/bench_telegraf.py
```

#### F. Cold Boot & Idle RSS Measurement
```bash
cargo build --release
python3 test/benchmark/measure_cold_boot.py
```
