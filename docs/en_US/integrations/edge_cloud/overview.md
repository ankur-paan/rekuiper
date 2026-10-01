# Edge-to-Cloud Streaming

Edge-to-cloud architectures connect local stream processing with upstream cloud storage, analytics platforms, and central brokers. In industrial IoT (IIoT), connected vehicles (IoV), and smart infrastructure, running stream computation at the edge minimizes network bandwidth, ensures sub-millisecond local reactions, and secures sensitive operational data.

![Edge Cloud Architecture](../resources/edge_cloud.png)

## Architecture Overview

In an edge-to-cloud topology:

1. **Edge Ingestion**: Devices, PLCs, and sensors publish telemetry locally via industrial protocols (Modbus, OPC UA) or local brokers (Mosquitto, NanoMQ).
2. **Edge Stream Processing**: rekuiper ingests telemetry locally, executes SQL rules, cleanses noisy sensor data, computes windowed aggregates, and detects anomalies.
3. **Upstream Forwarding**: Only relevant alerts, filtered anomalies, or aggregated metrics are forwarded over WAN connections to central brokers or cloud platforms (such as Kafka, cloud MQTT brokers, or data lakes).
4. **Cloud Control & Feedback**: Central controllers send commands, configuration updates, or dynamic SQL rules back to edge instances via REST or MQTT downlinks.

## Supported Protocols & Brokers

rekuiper connects seamlessly with any standard MQTT 3.1.1/5.0 broker or messaging platform:

- **Local Edge Brokers**: Mosquitto, NanoMQ, or local IPC sockets.
- **Central Cloud Brokers**: Open-source brokers (Mosquitto, EMQX), cloud-managed IoT brokers (AWS IoT Core, Azure IoT Hub), or Apache Kafka clusters.
- **Industrial Gateways**: Protocol translators such as EdgeX Foundry for PLC and Modbus connectivity.

