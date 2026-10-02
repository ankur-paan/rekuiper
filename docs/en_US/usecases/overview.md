# Use Cases and Solutions

rekuiper performs real-time stream computation on resource-constrained edge hardware. This section describes production architectures and streaming SQL implementations across key industrial and IoT domains.

## Industry Solutions

- **[Industrial IoT (IIoT)](iiot.md):** Filter sensor noise, calculate moving averages, detect equipment anomalies, and execute local control loops on industrial PCs and factory edge gateways.
- **[Connected Vehicles and EV Charging](iov.md):** Ingest high-frequency CAN bus telemetry, process dynamic EV charging sessions using `SESSIONWINDOW`, and downsample telematics for cellular bandwidth optimization.
- **[ESPHome Fleet Telemetry](esphome.md):** Route and aggregate thousands of microcontroller sensor streams using `meta(topic)` dynamic routing with sub-5MB memory footprints.
- **[Public Data Analysis](public_data_analysis.md):** Process, aggregate, and transform open data feeds using HTTP pull sources and temporal SQL windows.
