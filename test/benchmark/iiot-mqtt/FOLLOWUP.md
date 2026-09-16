# Current-code MQTT follow-up

This report measures the `0.425.0-beta` code used by this change. It does not
reuse the earlier `0.423.0-beta` follow-up numbers.

## Setup

- Release image built from the working tree, image ID `sha256:50a92f5e7f1f...`.
- Engine container: one pinned CPU, 1 GiB memory, `--memory-swap=1g`, and
  `TOKIO_WORKER_THREADS=1`.
- Rust `mqttgen` publisher: eight MQTT 3.1.1 QoS 0 connections on host cores
  10-11, outside the engine container.
- Mosquitto: separate container on cores 8-9. The sustained and peak trials
  use a 4,096-message / 1 MiB outgoing queue limit.
- Sink: JSON Lines on WSL ext4. The proof parses the sink output and checks
  counts, unique IDs/topics, devices, duplicates, and aggregate sums as
  appropriate for W1-W5.
- `sustained=true` requires at least 120 seconds, an on-schedule generator,
  exact sink proof, zero rule exceptions, a bounded broker, and no more than
  4,096 messages absent from the rule source counter at the end of sending.

The raw reports are in [`evidence/`](evidence/):

- [`perf-iot-mqtt-current-ladder.json`](evidence/perf-iot-mqtt-current-ladder.json)
- [`perf-iot-mqtt-current-bounded-peak.json`](evidence/perf-iot-mqtt-current-bounded-peak.json)
- [`perf-iot-mqtt-current-bounded-100k-120s.json`](evidence/perf-iot-mqtt-current-bounded-100k-120s.json)
- [`perf-iot-mqtt-current-w2-125k-120s.json`](evidence/perf-iot-mqtt-current-w2-125k-120s.json)
- [`perf-iot-mqtt-current-w3-150k-120s.json`](evidence/perf-iot-mqtt-current-w3-150k-120s.json)
- [`mqttgen-probe-200000.json`](evidence/mqttgen-probe-200000.json) and
  [`mqttprobe-200000.json`](evidence/mqttprobe-200000.json)

## Published ladder repeated

All 20 W1-W5 steps at 5k, 20k, 50k, and 100k messages/s produced exact sink
proofs with zero rule exceptions. CPU is percent of the one allotted core;
anonymous memory is engine heap-like memory and excludes file page cache.
`CPU delta` is current minus the previously published run in percentage points.

| Workload | Rate | Published CPU | Current CPU | CPU delta | Published anon MiB | Current anon MiB | End source gap |
|---|---:|---:|---:|---:|---:|---:|---:|
| W1 | 5k | 17.5 | 14.6 | -2.9 | 4.6 | 4.6 | 7 |
| W1 | 20k | 48.5 | 42.3 | -6.2 | 4.7 | 4.6 | 6 |
| W1 | 50k | 65.9 | 63.4 | -2.5 | 6.1 | 5.3 | 0 |
| W1 | 100k | 91.8 | 88.5 | -3.3 | 8.9 | 8.5 | 0 |
| W2 | 5k | 16.5 | 12.7 | -3.8 | 5.3 | 5.2 | 3 |
| W2 | 20k | 44.4 | 38.4 | -6.0 | 5.4 | 5.7 | 11 |
| W2 | 50k | 56.9 | 54.7 | -2.2 | 5.4 | 5.7 | 0 |
| W2 | 100k | 77.8 | 80.1 | +2.3 | 5.4 | 5.8 | 154,860 |
| W3 | 5k | 17.0 | 28.2 | +11.2 | 4.8 | 4.8 | 0 |
| W3 | 20k | 48.2 | 42.4 | -5.8 | 4.8 | 4.8 | 4 |
| W3 | 50k | 70.3 | 67.1 | -3.2 | 4.8 | 7.8 | 26 |
| W3 | 100k | 96.6 | 93.6 | -3.0 | 14.0 | 9.3 | 0 |
| W4 | 5k | 16.3 | 13.7 | -2.6 | 7.7 | 7.6 | 6 |
| W4 | 20k | 46.2 | 41.3 | -4.9 | 10.0 | 10.1 | 0 |
| W4 | 50k | 57.0 | 66.2 | +9.2 | 10.9 | 14.3 | 364 |
| W4 | 100k | 85.3 | 81.5 | -3.8 | 10.9 | 12.4 | 30,188 |
| W5 | 5k | 15.8 | 14.0 | -1.8 | 5.9 | 6.0 | 6 |
| W5 | 20k | 44.8 | 37.3 | -7.5 | 5.9 | 6.5 | 5 |
| W5 | 50k | 65.2 | 55.8 | -9.4 | 6.2 | 7.4 | 0 |
| W5 | 100k | 80.2 | 79.4 | -0.8 | 6.1 | 6.1 | 9 |

These are single repetitions on a noisy WSL host. The CPU deltas describe the
runs; they do not isolate one code change. The short W2/W4 100k source gaps also
show why exact eventual output alone is insufficient.

## Bounded sustained result

Every workload sustained 100k messages/s for 120 seconds. Each trial sent 12
million messages and produced an exact sink proof.

| Workload | CPU | Anon MiB | End source gap | Post-send activity | Result |
|---|---:|---:|---:|---:|---|
| W1 filter | 76.3% | 6.1 | 8 | 1 s | sustained |
| W2 device windows | 66.9% | 8.0 | 9 | 1 s | sustained |
| W3 plain-text topics | 79.5% | 6.6 | 4 | 1 s | sustained |
| W4 vehicle windows | 69.0% | 12.0 | 10 | 9 s | sustained |
| W5 charger sessions | 69.5% | 5.9 | 1 | 3 s | sustained |

W1 and W3 reached high `memory.current` values because the cgroup accounts the
page cache for their large sink files. Their anonymous memory remained 6.1 and
6.6 MiB respectively. This run is evidence for two minutes under the stated
conditions; it is not a months-long soak test.

## Peak search

The highest common verified sustained rate is **100k messages/s per core**.
The 200k target was not reached on this machine and workload set.

| Workload | Highest sustained trial | First tested failure above it | Failure evidence |
|---|---:|---:|---|
| W1 | 100k | 125k | 72,393 messages absent from the source counter at send end |
| W2 | 100k | 125k | 114,485 messages absent at send end in the 120 s trial |
| W3 | 150k | 200k | 16.0% sink loss in the 30 s bounded trial |
| W4 | 100k | 125k | 68,221 messages absent at send end |
| W5 | 100k | 125k | 69,282 messages absent at send end |

W3 sustained 150k for 120 seconds: 18 million messages, zero loss, an end gap
of 10, zero exceptions, 94.9% CPU, and 11.2 MiB anonymous memory. W2 at 125k
eventually delivered all 15 million messages but failed the steady criterion;
its 114,485-message source gap drained only after publishing stopped.

At offered 200k, W1 eventually wrote every expected filtered row but ended
sending with 735,604 messages upstream and its generator missed schedule. W2
had a 743,528-message gap and an off-schedule generator. W3 lost 16.0%, W4 lost
0.09%, and W5 ended with a 315,382-message gap. These are failed peak trials,
not 200k throughput results.

## Rust generator and subscriber capacity check

The independent Rust publisher sent six million messages at a nominal 200k/s,
and the constant-memory Rust subscriber received all six million with zero
sequence gaps, duplicates, or unmatched IDs. The publisher took 30.92 seconds
and reported `on_schedule=false`; the subscriber also needed several seconds
to drain. This proves exact delivery through the separate bounded broker in
that trial, but it does not prove a sustained 200k/s traffic source. The engine
peak runs therefore retain their own per-step generator schedule result.
