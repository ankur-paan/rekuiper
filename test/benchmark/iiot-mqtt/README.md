# IIoT / vehicle MQTT benchmark

rekuiper vs eKuiper 2.4.1, Telegraf 1.40.0 and Redpanda Connect 4.109.0 on the same MQTT broker,
the same load generator, the same CPU and memory limits, and an exact loss proof at the sink.
Everything needed to rerun it is in this folder. Raw per-step evidence is in [`evidence/`](evidence/).

The current-code bounded peak and 120-second follow-up is in
[`FOLLOWUP.md`](FOLLOWUP.md).

- Run date: 2026-09-13 (final run 20:21–21:38 local time), 1 repetition per engine and rate.
- Result file: [`evidence/perf-iot-mqtt-final.json`](evidence/perf-iot-mqtt-final.json)
  (every step, per-second CPU, peak memory, generator report, host-health snapshot, image IDs).

## 1. Why this benchmark

rekuiper targets IIoT gateways, ESPHome fleets, vehicles and EV chargers. Their main data path is MQTT.
The workloads below are those shapes: telemetry filtering, per-device windows, many plain-text topics,
per-vehicle aggregation over wildcard topics, and charger sessions.

## 2. Setup (identical for every engine)

| Item | Setting |
|---|---|
| Host | 12-core x86-64 laptop, Windows 11 + WSL2, Docker engine inside WSL2 |
| Engine container | `--cpuset-cpus=2 --cpus=1 --memory=1g --memory-swap=1g` |
| Broker | `eclipse-mosquitto:2`, cpuset 8,9, 512 MB, [`scripts/mosquitto/mosquitto.conf`](scripts/mosquitto/mosquitto.conf) (large queues so the broker does not throttle first) |
| Load | [`scripts/mqttgen`](scripts/mqttgen) (Rust, std only): MQTT 3.1.1 QoS 0, 8 connections, open loop, cpuset 10,11. It was on schedule in every step |
| Orchestrator | [`scripts/iotrunner`](scripts/iotrunner) (Rust), cores 0,1,4-7 |
| Step | 30 s send per rate: 5,000 / 20,000 / 50,000 / 100,000 msg/s, then drain until the sink is stable |
| Sink | JSON lines file on WSL-local ext4, bind-mounted into the engine container |
| CPU | cgroup v2 `cpu.stat usage_usec` over the send window, 100% = one core |
| Memory | `peak_anon_mb` = cgroup anonymous memory (engine heap). `peak_rss_mb` = `memory.current`, which also counts page cache from writing the sink file |

Before each measured step a warm-up burst on `warm_*` devices must reach the engine (rule or input
counter, or the sink for Telegraf). This proves the subscription is live before QoS 0 traffic starts.

Engines and versions:

| Engine | Image | Tuning |
|---|---|---|
| rekuiper | built from this repository with [`Dockerfile.bench`](Dockerfile.bench) | none (`TOKIO_WORKER_THREADS=1`) |
| eKuiper | `lfedge/ekuiper:2.4.1` | default rule options; `GOMAXPROCS=1`, `GOMEMLIMIT=900MiB` |
| Telegraf | `telegraf:1.40.0-alpine` | settings in section 4, each taken from Telegraf docs/source; `GOMEMLIMIT=900MiB` |
| Redpanda Connect | `redpandadata/connect:4.109.0` | defaults; `GOMEMLIMIT=900MiB` |

Apache Flink 2.3.0 is not included: Flink 2.x has no MQTT connector (none in Flink or Apache Bahir),
so it would need a custom source or a Kafka bridge, which is a different ingest path.

## 3. Workloads and proofs

| ID | Scenario | Stream / topics | Rule | Proof (exact) |
|---|---|---|---|---|
| w1 | telemetry filter | `bench/telemetry`, JSON, 1,000 devices | `SELECT id, device, temp, speed * 3.6 AS speed_kmh FROM telem WHERE temp > 21.0` | unique ids with the run tag == expected filtered count (139 of every 150), 0 duplicates |
| w2 | per-device windows | same | `SELECT device, count(*) AS n, avg(temp), max(speed) FROM telem GROUP BY device, TUMBLINGWINDOW(ss, 10)` | sum of `n` == messages sent, all 1,000 devices present |
| w3 | ESPHome | `bench/esphome/+/sensor/temperature/state`, plain-text states, 10,000 device topics, `FORMAT="binary"` | `SELECT meta(topic) AS topic, self AS state FROM telem` | rows == messages sent, all 10,000 topics present |
| w4 | vehicles | `bench/vehicles/+/telemetry`, JSON, 10,000 VIN topics | `SELECT device, count(*) AS n, avg(speed), max(temp) FROM telem GROUP BY device, TUMBLINGWINDOW(ss, 10)` | sum of `n` == sent, all 10,000 vehicles present |
| w5 | EV chargers | `bench/chargers/+/session`, JSON, 2,000 charger topics | `SELECT device, count(*) AS n, max(speed) FROM telem GROUP BY device, SESSIONWINDOW(ss, 10, 2)` | sum of `n` == sent, all 2,000 chargers present |

"Loss" is the proof shortfall. Where an engine cannot keep up, most of the shortfall is QoS 0 messages
the broker drops for a slow subscriber; the engine's own input counter in the evidence shows how many
it received.

## 4. Equivalent pipelines for Telegraf and Redpanda Connect

Neither has a rule API, so iotrunner writes one config per workload (see `telegraf_conf` and
`connect_conf` in [`scripts/iotrunner/src/main.rs`](scripts/iotrunner/src/main.rs)). Neither supports
session windows, so w5 is recorded as unsupported for both.

Telegraf (w1 shown; w2/w4 replace the processor with `aggregators.basicstats`, w3 uses `data_format = "value"`):

```toml
[agent]
  omit_hostname = true
  flush_interval = "1s"
  metric_batch_size = 10000
  metric_buffer_limit = 100000
[[outputs.file]]
  files = ["/lat/rows.jsonl"]
  data_format = "json"
  use_batch_format = true          # per-metric mode does one unbuffered write per metric
[[inputs.mqtt_consumer]]
  servers = ["tcp://rk-bench-mqtt:1883"]
  topics = ["bench/telemetry"]
  qos = 0
  max_undelivered_messages = 10000 # matched to metric_batch_size, as the plugin docs advise
  topic_tag = ""
  data_format = "json"
  tag_keys = ["device"]
  json_string_fields = ["id"]
[[processors.starlark]]
  source = '''
def apply(metric):
    if metric.fields.get("temp", 0.0) > 21.0:
        metric.fields["speed_kmh"] = metric.fields["speed"] * 3.6
        return metric
    return None
'''
# w2/w4:
# [[aggregators.basicstats]]
#   period = "10s"
#   grace = "10s"                  # otherwise metrics stamped before the current period are dropped
#   drop_original = true
#   stats = ["count", "mean", "max"]
```

Redpanda Connect (w2 shown; w1 is a single Bloblang filter mapping, w3 maps `@mqtt_topic` and `content()`):

```yaml
input:
  mqtt:
    urls: ["tcp://rk-bench-mqtt:1883"]
    topics: ["bench/telemetry"]
    client_id: rpconnect-bench
    qos: 0
buffer:
  system_window:
    timestamp_mapping: root = now()
    size: 10s
pipeline:
  processors:
    - group_by_value:
        value: '${! json("device") }'
    - mapping: |
        root = if batch_index() == 0 {
          { "device": this.device, "n": json("device").from_all().length(),
            "avg_temp": json("temp").from_all().sum() / json("temp").from_all().length(),
            "max_speed": json("speed").from_all().max() }
        } else { deleted() }
output:
  file:
    path: /lat/rows.jsonl
    codec: lines
```

## 5. Results

Each cell: **loss / CPU / engine heap (MB)**. Bold loss = the proof failed. 1 repetition.

### w1 telemetry filter

| Rate | rekuiper | eKuiper 2.4.1 | Telegraf 1.40.0 | Redpanda Connect 4.109.0 |
|---|---|---|---|---|
| 5k | 0% / 17.5% / 4.6 | 0% / 34.4% / 12.9 | 0% / 31.9% / 67.5 | 0% / 37.2% / 58.5 |
| 20k | 0% / 48.5% / 4.7 | 0% / 99.0% / 15.3 | 0% / 89.8% / 91.6 | 0% / 99.3% / 71.5 |
| 50k | 0% / 65.9% / 6.1 | **40.8%** / 99.3% / 15.4 | 0% (lag 10 s) / 99.3% / 96.5 | **37.6%** / 99.4% / 69.9 |
| 100k | 0% / 91.8% / 8.9 | **94.9%** / 99.3% / 15.8 | **32.5%** / 99.3% / 100.2 | **67.4%** / 99.4% / 71.1 |

### w2 per-device 10 s windows (1,000 devices)

| Rate | rekuiper | eKuiper 2.4.1 | Telegraf 1.40.0 | Redpanda Connect 4.109.0 |
|---|---|---|---|---|
| 5k | 0% / 16.5% / 5.3 | 0% / 27.1% / 124.7 | **9.4%** / 27.7% / 46.9 | 0% / 38.7% / 575.3 |
| 20k | 0% / 44.4% / 5.4 | 0% / 86.2% / 535.9 | **9.4%** / 81.4% / 51.6 | **100%** / 97.8% / 1012.3 |
| 50k | 0% / 56.9% / 5.4 | **55.2%** / 99.4% / 961.7 | **6.6%** / 99.3% / 55.4 | **100%** / 98.2% / 989.5 |
| 100k | 0% / 77.8% / 5.4 | **76.8%** / 99.3% / 911.9 | **27.3%** / 99.3% / 55.0 | **100%** / 98.2% / 1009.5 |

### w3 ESPHome (10,000 topics, plain-text payloads, `meta(topic)`)

| Rate | rekuiper | eKuiper 2.4.1 | Telegraf 1.40.0 | Redpanda Connect 4.109.0 |
|---|---|---|---|---|
| 5k | 0% / 17.0% / 4.8 | 0% / 32.4% / 41.2 | 0% / 24.1% / 58.1 | 0% / 31.9% / 59.8 |
| 20k | 0% / 48.2% / 4.8 | 0% / 94.0% / 43.2 | 0% / 70.3% / 84.9 | 0% / 97.8% / 67.7 |
| 50k | 0% / 70.3% / 4.8 | **77.8%** / 99.3% / 44.2 | 0% (lag 5 s) / 98.6% / 91.1 | **19.1%** / 99.3% / 67.6 |
| 100k | 0% / 96.6% / 14.0 | **81.1%** / 99.3% / 43.2 | **12.2%** / 99.3% / 90.9 | **57.3%** / 99.4% / 64.4 |

### w4 vehicles (10,000 VIN topics, per-vehicle 10 s windows)

| Rate | rekuiper | eKuiper 2.4.1 | Telegraf 1.40.0 | Redpanda Connect 4.109.0 |
|---|---|---|---|---|
| 5k | 0% / 16.3% / 7.7 | 0% / 31.4% / 147.7 | **9.4%** / 32.4% / 60.2 | 0% / 40.3% / 641.9 |
| 20k | 0% / 46.2% / 10.0 | 0% / 91.4% / 886.2 | **9.4%** / 85.7% / 94.0 | **99.7%** / 98.6% / 992.9 |
| 50k | 0% / 57.0% / 10.9 | **46.6%** / 99.3% / 939.9 | 0% / 99.3% / 101.5 | **100%** / 98.5% / 1005.8 |
| 100k | 0% / 85.3% / 10.9 | **69.5%** / 99.3% / 946.2 | **23.0%** / 99.3% / 102.4 | **86.1%** / 99.3% / 911.2 |

### w5 EV charger sessions (`SESSIONWINDOW(ss, 10, 2)`, 2,000 chargers)

| Rate | rekuiper | eKuiper 2.4.1 | Telegraf | Redpanda Connect |
|---|---|---|---|---|
| 5k | 0% / 15.8% / 5.9 | 0% / 30.7% / 195.2 | not supported | not supported |
| 20k | 0% / 44.8% / 5.9 | 0% / 87.2% / 831.7 | not supported | not supported |
| 50k | 0% / 65.2% / 6.2 | **56.9%** / 99.3% / 937.1 | not supported | not supported |
| 100k | 0% / 80.2% / 6.1 | **66.2%** / 99.3% / 922.3 | not supported | not supported |

### Summary: highest tested rate with an exact, loss-free result

| Workload | rekuiper | eKuiper 2.4.1 | Telegraf 1.40.0 | Redpanda Connect 4.109.0 |
|---|---|---|---|---|
| w1 filter | ≥ 100k | 20k | 50k (20k without backlog) | 20k |
| w2 device windows | ≥ 100k | 20k | none | 5k |
| w3 ESPHome | ≥ 100k | 20k | 50k (20k without backlog) | 20k |
| w4 vehicles | ≥ 100k | 20k | inconsistent (50k only) | 5k |
| w5 charger sessions | ≥ 100k | 20k | n/a | n/a |

rekuiper passed 20 of 20 steps. The ladder stops at 100k msg/s, so rekuiper's ceiling was not measured
(w3 at 100k used 96.6% CPU, the windowed workloads 78–85%). Relative results on this ladder:

- vs eKuiper: ≥ 5× the loss-free rate in every workload; about 2× less CPU at 20k (48% vs 99% on w1)
  and 100–170× less window memory at 20k (5–10 MB vs 536–886 MB).
- vs Telegraf: ≥ 2× on w1/w3; Telegraf never produced exact per-device windows.
- vs Redpanda Connect: ≥ 5× on w1/w3, ≥ 20× on windows (Connect holds each window's messages in memory
  and reaches the 1 GB limit from 20k).

## 6. Caveats

1. One repetition on one machine (laptop, Windows 11 + WSL2). Host-health snapshots are stored per
   engine; several runs are flagged as noisy (Windows host CPU above 30% or other busy processes).
   Numbers are cgroup-accounted per container, but a quieter Linux host and more repetitions are better.
2. The rate ladder has gaps (5k, 20k, 50k, 100k). "20k" means the engine passed 20k and failed 50k;
   its true limit is somewhere in between.
3. Telegraf w2/w4 lost a constant ~9.4% at 5k and 20k (about 2.8 s of data) but w4 passed at 50k.
   We did not find the cause; `grace` was already set. The exact config is published above so this can
   be checked and improved.
4. Redpanda Connect windows use the documented `system_window` pattern, which keeps every message of a
   window in memory. Another design might do better; we did not attempt one.
5. eKuiper was run with default rule options. An earlier run with `bufferLength=131072, concurrency=1`
   showed the same pattern (lossless at 5k, 0.7% loss at 20k, 30% at 50k on w1) and was not repeated.
6. For `FORMAT="binary"`, rekuiper returns UTF-8 payloads as text in `self` (other bytes as base64);
   eKuiper returns bytes. Counts and topics are identical.
7. rekuiper's `peak_rss_mb` rises on w1/w3 at high rates (up to 236 MB) because it includes page cache
   from writing large sink files; its heap stayed under 15 MB. Compare engines by `peak_anon_mb`.
8. MQTT QoS 0 only. QoS 1/2, TLS and reconnect behaviour are not part of this benchmark.

## 7. Earlier runs (superseded)

[`evidence/earlier/`](evidence/earlier/) holds two earlier runs with the same harness but with the sink on
a Windows-drive bind mount, where every write call is slow. That penalised engines that write per
message (Telegraf, Redpanda Connect) and, to a lesser degree, eKuiper, so all engines were rerun on
local disk. `baseline-old-build-windows-sink.json` also shows the rekuiper window bug that this work
fixed (wrong output for windowed GROUP BY, collapse at 100k).

## 8. Reproduce

Requirements: Linux or WSL2 with Docker (cgroup v2), a Rust toolchain, `taskset` (util-linux) and `curl`.
The defaults assume 12 logical CPUs; change the cpusets with the environment variables below.

```bash
cd test/benchmark/iiot-mqtt

# engines and broker
docker build -f Dockerfile.bench -t rekuiper-bench:local ../../..
docker pull eclipse-mosquitto:2
docker pull lfedge/ekuiper:2.4.1
docker pull telegraf:1.40.0-alpine
docker pull redpandadata/connect:4.109.0

# tools
(cd scripts/mqttgen && cargo build --release && cp target/release/mqttgen .)
(cd scripts/iotrunner && cargo build --release && cp target/release/iotrunner .)
(cd scripts/mqttprobe && cargo build --release && cp target/release/mqttprobe .)
mkdir -p evidence

# run (about 1 h 45 min for four engines, five workloads, four rates)
IOT_HOME=$PWD IOT_REK_IMAGE=rekuiper-bench:local \
IOT_ENGINES=rek,eku-def,telegraf,rpconnect IOT_WORKLOADS=w1,w2,w3,w4,w5 IOT_REPS=1 \
IOT_OUT=perf-iot-mqtt.json taskset -c 0,1,4-7 scripts/iotrunner/iotrunner
```

| Variable | Default | Meaning |
|---|---|---|
| `IOT_HOME` | current directory | this folder (evidence is written to `$IOT_HOME/evidence`) |
| `IOT_ENGINES` | `rek,eku-def,eku-tun` | any of `rek`, `eku-def`, `eku-tun`, `telegraf`, `rpconnect` |
| `IOT_WORKLOADS` | `w1,w2` | any of `w1`–`w5` |
| `IOT_RATES` | `5000,20000,50000,100000` | msg/s ladder |
| `IOT_DUR` / `IOT_REPS` | `30` / `2` | seconds per step / repetitions |
| `IOT_REK_IMAGE` / `IOT_EKU_IMAGE` | `rekuiper-bench:local` / `lfedge/ekuiper:2.4.1` | engine images |
| `IOT_LATDIR` | `<tmp>/iot_lat` | sink directory; must be a local filesystem |
| `IOT_BROKER_CONFIG` | `scripts/mosquitto/mosquitto.conf` | absolute broker config path; use `scripts/mosquitto/mosquitto-bounded.conf` for short-run capacity checks with a 4,096-message / 1 MiB queue limit |
| `IOT_TAG_PREFIX` | empty | prefix for run IDs and generator report files; set for follow-up runs to preserve the published evidence |
| `IOT_ENGINE_CPUSET` / `IOT_BROKER_CPUSET` / `IOT_GEN_CPUSET` | `2` / `8,9` / `10,11` | core placement |

Each step prints one line (`complete`, `loss%`, `lag_s`, `cpu`, `rss`, `anon`) and the full record is
saved after every step, so an interrupted run keeps its evidence.

For a separate broker/generator capacity check, [`scripts/mqttprobe`](scripts/mqttprobe)
is a Rust MQTT QoS 0 subscriber. Run it on cores outside the engine container
before the five workload runs. Its per-second receive counts establish that
the publisher and broker can deliver the requested traffic. Keep this probe
out of the engine comparison itself: another subscriber adds broker fanout and
changes the workload. The `source_in_end_send_delta` and
`upstream_gap_at_send_end` fields in each new runner step show how far the
engine fell behind during sending. In new reports, `sustained` requires a
bounded broker, at least 120 seconds of sending, an end-of-send source gap of
at most 4,096 packets, zero rule exceptions, exact sink proof and the workload's
normal window-flush time. It describes the measured trial, not a claim about
months of operation. The older published report's `sustained` field used only
a drain-time criterion, so it cannot establish this bounded steady rate.

Run `bash scripts/probe_capacity.sh` from this directory for a 30-second 200k
publisher/subscriber capacity check. It starts a separate bounded Mosquitto
container on cores 8–9, runs the Rust subscriber on 4–7 and the Rust publisher
on 10–11, then saves the send and receive reports in `evidence/`. The
subscriber checks each publisher connection's sequence using eight counters,
so the proof does not grow in memory with message count.
