# Use Cases & Solutions

rekuiper is designed for real-time stream computation on resource-constrained edge hardware. This section covers production architectures and streaming SQL implementations across key edge domains:

- **[Industrial IoT (IIoT)](iiot.md)**: Filter noisy sensor data, calculate rolling averages, detect equipment anomalies, and execute local control loops on industrial PCs and factory gateways.
- **[Connected Vehicles & EV Charging](iov.md)**: Ingest high-frequency CAN bus telemetry, process dynamic EV charging sessions using `SESSIONWINDOW`, and downsample telematics for cellular uplinks.
- **[ESPHome Fleet Telemetry](esphome.md)**: Route and aggregate thousands of microcontroller sensor streams using `meta(topic)` dynamic routing with sub-5MB memory consumption.
- **[Public Data Analysis](public_data_analysis.md)**: Process and transform open data streams using REST pull sources and streaming SQL windowing.
