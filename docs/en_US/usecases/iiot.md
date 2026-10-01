# Industrial IoT (IIoT) Stream Processing

Industrial IoT environments require reliable, deterministic data processing close to the physical plant. In steel manufacturing, chemical plants, automated assembly lines, and energy grids, hundreds of sensors, PLCs, and SCADA controllers emit high-frequency telemetry.

rekuiper runs directly on industrial PCs, embedded gateways, and edge servers to filter noise, calculate windowed averages, detect anomalies, and trigger control signals with sub-millisecond latency.

---

## Edge Architecture for Industrial IoT

In modern industrial architectures, rekuiper acts as the local real-time compute engine between factory-floor controllers and enterprise systems:

![Industrial IoT Edge Architecture](../public/diagrams/iiot_architecture.svg)

---

## Key Industrial Capabilities

### 1. Deterministic Execution with Zero GC
Edge gateways often run on constrained hardware (single-core ARM Cortex-A, 512 MB to 1 GB RAM). Traditional runtimes introduce garbage collection pauses that can drop packets or delay time-critical alerts. rekuiper provides zero GC pauses, deterministic execution, and a sub-5MB baseline memory footprint.

### 2. Stream SQL for Local Windowing & Filtering
Instead of streaming millions of raw sensor events to the cloud over costly cellular or WAN links, rekuiper computes rolling aggregates locally:
- **Tumbling Windows**: Compute 10-second average temperatures, pressures, and vibration indexes per machine.
- **Sliding Windows**: Detect moving spikes that exceed standard operating limits over 60 seconds.
- **Session Windows**: Track operational states across batch processing runs.

### 3. Sub-Millisecond Anomaly Detection
Detect equipment malfunctions instantly:
- Bearing overheating and excessive motor vibration
- Pressure loss in pneumatic valves
- Voltage fluctuations in sub-distribution panels

When an anomaly triggers a SQL rule, rekuiper immediately publishes an alert to a local broker, activates a PLC digital output via a webhook, and logs the incident.

### 4. Edge AI & Machine Learning Inference
rekuiper interfaces with Python and WebAssembly inference runtimes to evaluate machine learning models in real time:
- Remaining useful life (RUL) estimation
- Automated visual defect classification
- Predictive maintenance scoring

---

## Example: Temperature Spike Alarm

The following SQL rule inspects continuous boiler temperature readings from an MQTT stream and triggers an immediate alert when temperature exceeds 85 degrees:

```sql
SELECT
  deviceId,
  temperature,
  humidity
FROM
  telemetry_stream
WHERE
  temperature > 85.0
```

The output action sends the structured alert payload to both a local alarm webhook and an upstream Kafka topic for compliance logging:

```json
{
  "id": "boiler_overheat_alarm",
  "sql": "SELECT deviceId, temperature FROM telemetry_stream WHERE temperature > 85.0",
  "actions": [
    {
      "rest": {
        "url": "http://127.0.0.1:8080/api/v1/alarms",
        "method": "POST",
        "sendSingle": true
      }
    },
    {
      "mqtt": {
        "server": "tcp://127.0.0.1:1883",
        "topic": "factory/alarms/critical"
      }
    }
  ]
}
```

---

## Benefits for Industrial Deployments

- **Reduced WAN Bandwidth**: Downsample and aggregate high-frequency sensor streams before sending data upstream.
- **Offline Autonomy**: Rules and alerts execute locally even if cloud connectivity is interrupted.
- **Unified Stream SQL**: Manage filtering, joins, and windowing using standard SQL without writing custom firmware code.
