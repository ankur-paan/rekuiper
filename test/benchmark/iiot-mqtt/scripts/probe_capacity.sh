#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."
rate="${IOT_PROBE_RATE:-200000}"
seconds="${IOT_PROBE_SECS:-30}"
mkdir -p evidence
broker_name="rk-probe-capacity"
docker container rm -f "$broker_name" >/dev/null 2>&1 || true
docker run -d --name "$broker_name" --cpuset-cpus=8,9 --memory=512m --memory-swap=512m \
  -p 11884:1883 \
  -v "$PWD/scripts/mosquitto/mosquitto-bounded.conf:/mosquitto/config/mosquitto.conf:ro" \
  eclipse-mosquitto:2 >/dev/null
trap 'docker container rm -f "$broker_name" >/dev/null 2>&1 || true' EXIT

for _ in {1..50}; do
  if (echo > /dev/tcp/127.0.0.1/11884) >/dev/null 2>&1; then break; fi
  sleep 0.1
done
sleep 1
touch "evidence/mqttprobe-$rate.log"
taskset -c 4-7 scripts/mqttprobe/mqttprobe --host 127.0.0.1 --port 11884 \
  --topic bench/telemetry --secs "$((seconds + 5))" \
  --client-id "mqttprobe-$rate" --tag "PROBE${rate}" --out "evidence/mqttprobe-$rate.json" \
  > "evidence/mqttprobe-$rate.log" 2>&1 &
probe_pid=$!
for _ in {1..50}; do
  if grep -q 'mqttprobe subscribed' "evidence/mqttprobe-$rate.log"; then break; fi
  if ! kill -0 "$probe_pid" 2>/dev/null; then wait "$probe_pid"; exit 1; fi
  sleep 0.1
done
grep -q 'mqttprobe subscribed' "evidence/mqttprobe-$rate.log"

taskset -c 10,11 scripts/mqttgen/mqttgen --host 127.0.0.1 --port 11884 \
  --topic bench/telemetry --rate "$rate" --secs "$seconds" --conns 8 \
  --devices 1000 --tag "PROBE${rate}" --out "evidence/mqttgen-probe-$rate.json"
wait "$probe_pid"
cat "evidence/mqttprobe-$rate.json"
