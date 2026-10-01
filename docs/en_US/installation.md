# Installation & Deployment

rekuiper deploys across edge architectures, from single-board computers (Raspberry Pi, BeagleBone) to industrial PCs and edge servers.

---

## 1. Network Ports Reference

rekuiper exposes the following standard network ports:

| Port | Protocol | Service / Purpose | Default Accessibility |
| :--- | :--- | :--- | :--- |
| **`9081`** | HTTP / TCP | REST API, SSE event streaming, and eKuiper Manager UI backend | External (Host & LAN) |
| **`20498`** | TCP | NanoIPC / Edge IPC socket for high-speed local connector streaming | Internal / IPC |
| **`20499`** | TCP | Management RPC daemon for the `kuiper` CLI | Host / Internal |

---

## 2. Running with Docker

Docker is the quickest and most consistent deployment method for edge gateways.

### Quick Run

```shell
docker run -d \
  --name rekuiper \
  -p 9081:9081 \
  -p 20498:20498 \
  -p 20499:20499 \
  ankurkrp/rekuiper:0.502-beta
```

### Production Run with Persistent Storage

For production deployments, mount persistent host directories for stream schemas, rule definitions, and state databases:

```shell
docker run -d \
  --name rekuiper \
  --restart unless-stopped \
  -p 9081:9081 \
  -p 20498:20498 \
  -p 20499:20499 \
  -v /var/lib/rekuiper/data:/kuiper/data \
  -v /var/lib/rekuiper/etc:/kuiper/etc \
  -v /var/log/rekuiper:/kuiper/log \
  -e KUIPER__BASIC__PROMETHEUS="true" \
  -e LOG_LEVEL="info" \
  ankurkrp/rekuiper:0.502-beta
```

Verify service availability:

```shell
curl http://localhost:9081/ping
# Response: pong
```

---

## 3. Production Deployment with Docker Compose

To deploy rekuiper together with an MQTT broker (Eclipse Mosquitto) and the web management dashboard ([eKuiper Manager](https://github.com/ankur-paan/ekuiper-manager)):

Create a `docker-compose.yml` file:

```yaml
version: '3.8'

services:
  rekuiper:
    image: ankurkrp/rekuiper:0.502-beta
    container_name: rekuiper
    restart: unless-stopped
    ports:
      - "9081:9081"
      - "20498:20498"
      - "20499:20499"
    environment:
      KUIPER__BASIC__REST_PORT: "9081"
      KUIPER__BASIC__CLI_PORT: "20499"
      KUIPER__BASIC__PROMETHEUS: "true"
      MQTT_SOURCE__DEFAULT__SERVERS: "[tcp://mosquitto:1883]"
      LOG_LEVEL: "info"
    volumes:
      - rekuiper-data:/kuiper/data
      - rekuiper-etc:/kuiper/etc
    depends_on:
      - mosquitto
    networks:
      - edge-net

  mosquitto:
    image: eclipse-mosquitto:2
    container_name: mosquitto
    restart: unless-stopped
    ports:
      - "1883:1883"
    networks:
      - edge-net

  manager:
    image: ankur-paan/ekuiper-manager:latest
    container_name: ekuiper-manager
    restart: unless-stopped
    ports:
      - "9082:9082"
    environment:
      DEFAULT_EKUIPER_ENDPOINT: "http://rekuiper:9081"
    depends_on:
      - rekuiper
    networks:
      - edge-net

volumes:
  rekuiper-data:
  rekuiper-etc:

networks:
  edge-net:
    driver: bridge
```

Start the stack:

```shell
docker compose up -d
```

- **rekuiper REST API**: `http://localhost:9081`
- **eKuiper Manager Web UI**: `http://localhost:9082`
- **MQTT Broker**: `localhost:1883`

---

## 4. Standalone Binary Installation

For bare-metal Linux gateways, OpenWrt, or systems without container runtimes, rekuiper can run directly as a single native binary.

### Downloading Prebuilt Binaries

Prebuilt binaries are published for every release on [GitHub Releases](https://github.com/ankur-paan/rekuiper/releases):

```shell
# Example for Linux x86_64 (replace with desired release tag)
curl -LO https://github.com/ankur-paan/rekuiper/releases/download/v0.502-beta/kuiperd-x86_64-unknown-linux-musl.tar.gz
tar -xzf kuiperd-x86_64-unknown-linux-musl.tar.gz
cd rekuiper

# Start the daemon
./bin/kuiperd
```

### Compiling from Source

To compile rekuiper with maximum CPU architecture optimizations:

**Prerequisites:** Rust 1.80+ and Cargo.

```shell
# Clone the repository
git clone https://github.com/ankur-paan/rekuiper.git
cd rekuiper

# Compile in release mode
cargo build --release

# The compiled binaries will be located at:
# target/release/kuiperd  (Server engine)
# target/release/kuiper   (CLI tool)
# target/release/rekuiper-mcp (MCP AI server)
```

Run the compiled daemon:

```shell
./target/release/kuiperd --etc etc --data data --log log
```

---

## 5. Configuration Reference

rekuiper reads configuration from `etc/kuiper.yaml` and supports environment variable overrides using the double-underscore (`__`) delimiter syntax:

| Environment Variable | Config Equivalent | Default | Description |
| :--- | :--- | :--- | :--- |
| `KUIPER__BASIC__REST_PORT` | `basic.restPort` | `9081` | Port for the HTTP REST API |
| `KUIPER__BASIC__CLI_PORT` | `basic.cliPort` | `20499` | Port for the internal CLI management RPC |
| `KUIPER__BASIC__PROMETHEUS` | `basic.prometheus` | `false` | Enable Prometheus metrics at `/metrics` |
| `KUIPER__BASIC__CONSOLELOG` | `basic.consoleLog` | `true` | Log events directly to stdout / console |
| `LOG_LEVEL` | - | `info` | Logging verbosity (`debug`, `info`, `warn`, `error`) |
| `MQTT_SOURCE__DEFAULT__SERVERS` | `mqtt.default.servers` | `[tcp://127.0.0.1:1883]` | Default MQTT broker address list |

---

## 6. Systemd Service Setup (Linux)

To run rekuiper as a background system service on Linux gateways:

Create `/etc/systemd/system/rekuiper.service`:

```ini
[Unit]
Description=rekuiper Stream Processing Engine
After=network.target

[Service]
Type=simple
User=rekuiper
ExecStart=/opt/rekuiper/bin/kuiperd --etc /opt/rekuiper/etc --data /opt/rekuiper/data --log /opt/rekuiper/log
Restart=always
RestartSec=5s
LimitNOFILE=65536

[Install]
WantedBy=multi-user.target
```

Enable and start the service:

```shell
sudo systemctl daemon-reload
sudo systemctl enable --now rekuiper
sudo systemctl status rekuiper
```
