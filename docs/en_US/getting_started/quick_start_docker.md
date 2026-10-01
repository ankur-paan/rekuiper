# 5-Minute Quick Start with Docker

This guide walks you through running **rekuiper** in Docker and deploying your first real-time streaming rule in under five minutes.

---

## 1. Start rekuiper

Launch rekuiper with all standard network ports exposed:

```shell
docker run -d \
  --name rekuiper \
  -p 9081:9081 \
  -p 20498:20498 \
  -p 20499:20499 \
  ankurkrp/rekuiper:0.503-beta
```

Verify that the engine is running and responding:

```shell
curl http://localhost:9081/ping
```

**Expected response:**
```text
pong
```

---

## 2. Create a Data Stream

A **Stream** defines your incoming data schema and protocol source. Let's create an in-memory stream named `demo`:

```shell
curl -X POST http://localhost:9081/streams \
  -H "Content-Type: application/json" \
  -d '{
    "sql": "CREATE STREAM demo (temperature float, humidity bigint) WITH (DATASOURCE=\"demo\", FORMAT=\"JSON\")"
  }'
```

**Expected response:**
```text
Stream demo is created.
```

---

## 3. Deploy a Streaming Rule

A **Rule** defines continuous SQL logic that processes incoming events and routes results to target actions (sinks).

Create a rule named `rule_high_temp` that triggers whenever the `temperature` exceeds `30.0`°C and writes matches to the engine log:

```shell
curl -X POST http://localhost:9081/rules \
  -H "Content-Type: application/json" \
  -d '{
    "id": "rule_high_temp",
    "sql": "SELECT temperature, humidity FROM demo WHERE temperature > 30.0",
    "actions": [
      {
        "log": {}
      }
    ]
  }'
```

**Expected response:**
```text
Rule rule_high_temp was created
```

---

## 4. Send Test Telemetry

Inject test sensor readings directly using rekuiper's HTTP push endpoint:

```shell
# 1. Send an event below threshold (temperature = 22.0) - Should be filtered out
curl -X POST http://localhost:9081/streams/demo/data \
  -H "Content-Type: application/json" \
  -d '{"temperature": 22.0, "humidity": 45}'

# 2. Send an event above threshold (temperature = 34.8) - Should trigger the rule!
curl -X POST http://localhost:9081/streams/demo/data \
  -H "Content-Type: application/json" \
  -d '{"temperature": 34.8, "humidity": 55}'
```

---

## 5. Verify the Execution

Check the live metrics of your rule:

```shell
curl http://localhost:9081/rules/rule_high_temp/status
```

**Output:**
```json
{
  "source_demo_0_records_in_total": 2,
  "source_demo_0_records_out_total": 2,
  "op_filter_0_records_in_total": 2,
  "op_filter_0_records_out_total": 1,
  "op_project_0_records_in_total": 1,
  "op_project_0_records_out_total": 1,
  "sink_log_0_records_in_total": 1,
  "sink_log_0_records_out_total": 1
}
```

Notice:
- `source_demo_0_records_in_total`: `2` (both events were received).
- `op_filter_0_records_out_total`: `1` (the cold event was dropped; the hot event passed).
- `sink_log_0_records_out_total`: `1` (the alert was logged).

Inspect the container logs to view the output:

```shell
docker logs rekuiper | grep "LOG SINK"
```

---

## 6. Optional: Visual Management with eKuiper Manager

If you prefer a web-based dashboard for building rules and visual topologies:

```shell
docker run -d \
  --name ekuiper-manager \
  -p 9082:9082 \
  -e DEFAULT_EKUIPER_ENDPOINT="http://localhost:9081" \
  ankur-paan/ekuiper-manager:latest
```

Open `http://localhost:9082` in your browser to inspect streams, edit rules visually, and monitor real-time execution graphs.

---

## Next Steps

- Explore [Core Architecture & Design](../concepts/ekuiper.md)
- Learn about [Windowing Functions](../sqls/windows.md) (Tumbling, Hopping, Sliding, Session)
- Configure [Production Deployments](../installation.md)
- Connect external [MQTT Brokers](../guide/sources/builtin/mqtt.md)
