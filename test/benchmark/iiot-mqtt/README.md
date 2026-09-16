# rekuiper 0.426 MQTT benchmark

This is the published MQTT benchmark for `0.426-beta`. It measures whether a
single rekuiper CPU core can process a fixed offered rate continuously, execute
the rule, and write the exact expected result to the sink without hiding a
backlog in the broker or in engine memory.

The release claim is **100,000 MQTT messages/s per core across all five
workloads**. W3 also passed at 150,000 messages/s. The 200,000 messages/s target
was not reached and is not claimed.

The release-candidate runtime was measured before the version field changed
from `0.425.0-beta` to `0.426.0-beta`. The only later changes were version and
documentation files plus a test-fixture source declaration; the runtime hot
path did not change. The benchmark image ID is preserved in the evidence
instead of relabelling the run.

Detailed results are in [`BENCHMARK-0.426.md`](BENCHMARK-0.426.md). The previous
four-engine 0.425 comparison remains available in
[`ARCHIVE-0.425-COMPARISON.md`](ARCHIVE-0.425-COMPARISON.md).

## Result

Every 100k trial ran for 120 seconds, sent 12 million messages, reported zero
rule exceptions, and produced an exact sink proof.

| Workload | Highest sustained trial | CPU on one core | Anonymous memory | End-of-send gap | First tested failure above it |
|---|---:|---:|---:|---:|---|
| W1 telemetry filter | 100k msg/s | 76.3% | 6.1 MiB | 8 | 125k: 72,393-message gap |
| W2 per-device windows | 100k msg/s | 66.9% | 8.0 MiB | 9 | 125k: 114,485-message gap in the 120 s trial |
| W3 ESPHome topics | **150k msg/s** | 94.9% | 11.2 MiB | 10 | 200k: 16.0% sink loss |
| W4 vehicle windows | 100k msg/s | 69.0% | 12.0 MiB | 10 | 125k: 68,221-message gap |
| W5 charger sessions | 100k msg/s | 69.5% | 5.9 MiB | 1 | 125k: 69,282-message gap |

An end-of-send gap is the difference between messages sent by the Rust
publisher and messages admitted by the rule source when publishing stops. A
large gap that drains later is backlog, not sustained throughput.

## Test system

| Item | Setting |
|---|---|
| Host | 12-core x86-64 laptop, Windows 11 with WSL2; Docker engine inside WSL2 |
| rekuiper | release build, `TOKIO_WORKER_THREADS=1` |
| Engine limit | CPU 2 only, one CPU quota, 1 GiB RAM, `--memory-swap=1g` (no additional swap) |
| Broker | separate `eclipse-mosquitto:2` container on CPUs 8-9, 512 MiB |
| Broker queue | at most 4,096 messages and 1 MiB per client; persistence disabled |
| Publisher | [`mqttgen`](scripts/mqttgen), Rust/std-only MQTT 3.1.1 QoS 0, eight connections, CPUs 10-11, outside the engine container |
| Subscriber probe | [`mqttprobe`](scripts/mqttprobe), Rust/std-only, constant-memory sequence proof, CPUs 4-7 |
| Orchestrator | [`iotrunner`](scripts/iotrunner), Rust, CPUs 0-1 and 4-7 |
| Sink | JSON Lines on WSL ext4, bind-mounted into the engine container |
| CPU measurement | cgroup v2 `cpu.stat usage_usec`; 100% is the allotted core |
| Memory measurement | cgroup anonymous memory for engine heap; `memory.current` is also recorded and includes sink-file page cache |

The publisher, broker, orchestrator, and engine use disjoint CPU sets. The
publisher and subscriber are not inside the engine container.

## Workloads and exact proofs

| ID | Input and rule | Sink proof |
|---|---|---|
| W1 | JSON telemetry from 1,000 devices; filter `temp > 21` and calculate speed | expected filtered IDs, zero duplicates |
| W2 | JSON telemetry; 10-second tumbling window grouped by 1,000 devices | sum of window counts equals messages sent; all devices present |
| W3 | plain-text ESPHome states across 10,000 MQTT topics; project `meta(topic)` | one row per message; all topics present; zero duplicates |
| W4 | JSON telemetry across 10,000 vehicle topics; per-vehicle 10-second window | sum of window counts equals messages sent; all vehicles present |
| W5 | JSON events across 2,000 charger topics; `SESSIONWINDOW` | sum of session counts equals messages sent; all chargers present |

## Sustained-pass criteria

A step is marked `sustained=true` only when all of these conditions hold:

1. Publishing lasts at least 120 seconds.
2. The Rust publisher reports `on_schedule=true`.
3. The broker uses [`mosquitto-bounded.conf`](scripts/mosquitto/mosquitto-bounded.conf), capped at 4,096 queued messages and 1 MiB.
4. The rule source is no more than 4,096 messages behind the publisher when sending ends.
5. Rule exceptions do not increase.
6. The parsed sink has the exact expected rows or aggregates, with no duplicates.
7. Post-send work stays within the normal rule/window close allowance.

This prevents a short burst followed by a long drain from being reported as
the offered rate. It also prevents an unbounded broker or growing engine queue
from making a run appear healthy.

## Evidence

| File | Purpose |
|---|---|
| [`perf-iot-mqtt-current-bounded-100k-120s.json`](evidence/perf-iot-mqtt-current-bounded-100k-120s.json) | all five workloads at 100k for 120 seconds |
| [`perf-iot-mqtt-current-w3-150k-120s.json`](evidence/perf-iot-mqtt-current-w3-150k-120s.json) | W3 sustained 150k trial |
| [`perf-iot-mqtt-current-w2-125k-120s.json`](evidence/perf-iot-mqtt-current-w2-125k-120s.json) | W2 125k backlog failure |
| [`perf-iot-mqtt-current-bounded-peak.json`](evidence/perf-iot-mqtt-current-bounded-peak.json) | 125k/150k/200k short peak search for W1-W5 |
| [`perf-iot-mqtt-current-ladder.json`](evidence/perf-iot-mqtt-current-ladder.json) | repeated 5k/20k/50k/100k published ladder |
| [`mqttgen-probe-200000.json`](evidence/mqttgen-probe-200000.json) | independent publisher report at nominal 200k |
| [`mqttprobe-200000.json`](evidence/mqttprobe-200000.json) | independent subscriber sequence proof |

The old four-engine evidence and the two earlier Windows-mounted-sink trials
remain in [`evidence/`](evidence/) and [`evidence/earlier/`](evidence/earlier/).

## Reproduce

Requirements: Linux or WSL2 with Docker and cgroup v2, Rust 1.85 or newer,
`taskset`, and `curl`. The commands below assume at least 12 logical CPUs. Change
the CPU-set variables on smaller systems while keeping generator, broker, and
engine cores separate.

### Build the release image and Rust tools

```bash
cd test/benchmark/iiot-mqtt

docker build -f Dockerfile.bench -t rekuiper-bench:0.426 ../../..
docker pull eclipse-mosquitto:2

(cd scripts/mqttgen && cargo build --release && cp target/release/mqttgen .)
(cd scripts/mqttprobe && cargo build --release && cp target/release/mqttprobe .)
(cd scripts/iotrunner && cargo build --release && cp target/release/iotrunner .)

mkdir -p evidence
```

### Check publisher, broker, and subscriber capacity

```bash
IOT_PROBE_RATE=200000 IOT_PROBE_SECS=30 bash scripts/probe_capacity.sh
```

This separate check starts its own bounded broker, runs the Rust subscriber on
CPUs 4-7, and runs the Rust publisher on CPUs 10-11. In the published run all
six million messages arrived with zero sequence gaps or duplicates, but the
publisher took 30.92 seconds and reported `on_schedule=false`. It proves exact
delivery for that probe; it does not establish sustained 200k capacity.

### Run the five 100k sustained trials

```bash
export IOT_HOME="$PWD"
export IOT_REK_IMAGE="rekuiper-bench:0.426"
export IOT_BROKER_CONFIG="$PWD/scripts/mosquitto/mosquitto-bounded.conf"
export IOT_LATDIR="/tmp/rekuiper-iot-0426"
export IOT_ENGINE_CPUSET="2"
export IOT_BROKER_CPUSET="8,9"
export IOT_GEN_CPUSET="10,11"

IOT_ENGINES=rek \
IOT_WORKLOADS=w1,w2,w3,w4,w5 \
IOT_RATES=100000 IOT_DUR=120 IOT_REPS=1 \
IOT_TAG_PREFIX=SUST426 IOT_OUT=perf-iot-mqtt-0426-100k-120s.json \
taskset -c 0,1,4-7 scripts/iotrunner/iotrunner
```

### Search above the common sustained rate

```bash
# Short bounded search. These steps identify candidates and failures; their
# 30-second duration is too short to receive sustained=true.
IOT_ENGINES=rek IOT_WORKLOADS=w1,w2,w3,w4,w5 \
IOT_RATES=125000,150000,200000 IOT_DUR=30 IOT_REPS=1 \
IOT_TAG_PREFIX=PEAK426 IOT_OUT=perf-iot-mqtt-0426-peak.json \
taskset -c 0,1,4-7 scripts/iotrunner/iotrunner

# Confirm any candidate for 120 seconds. W3 at 150k passed; W2 at 125k failed.
IOT_ENGINES=rek IOT_WORKLOADS=w3 IOT_RATES=150000 IOT_DUR=120 IOT_REPS=1 \
IOT_TAG_PREFIX=SUSTW3426 IOT_OUT=perf-iot-mqtt-0426-w3-150k-120s.json \
taskset -c 0,1,4-7 scripts/iotrunner/iotrunner

IOT_ENGINES=rek IOT_WORKLOADS=w2 IOT_RATES=125000 IOT_DUR=120 IOT_REPS=1 \
IOT_TAG_PREFIX=SUSTW2426 IOT_OUT=perf-iot-mqtt-0426-w2-125k-120s.json \
taskset -c 0,1,4-7 scripts/iotrunner/iotrunner
```

`iotrunner` writes the report after every step, so an interrupted run keeps its
completed evidence. Each report includes the image ID, container limits,
per-second CPU, anonymous and total cgroup memory, publisher schedule, source
gap at send end, exception delta, drain time, and parsed sink proof.

## Limits of the result

- Each published point is one run on one WSL2 laptop. The raw reports include
  host-health snapshots; repeated runs on a quiet native-Linux host would give
  tighter CPU estimates.
- Two minutes establishes the bounded behavior of these trials. It is not a
  days- or months-long soak test.
- MQTT QoS 0 is measured. QoS 1/2, TLS, reconnect storms, and multiple rules
  sharing a core are outside this benchmark.
- W1 and W3 write large sink files. Their high `memory.current` values are file
  page cache; the table reports anonymous engine memory separately.
