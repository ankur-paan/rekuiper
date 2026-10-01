# ankurkrp/rekuiper

[![Docker Pulls](https://img.shields.io/docker/pulls/ankurkrp/rekuiper?color=blue&logo=docker)](https://hub.docker.com/r/ankurkrp/rekuiper)
[![Release](https://img.shields.io/badge/release-v0.503--beta-blue.svg)](https://github.com/ankur-paan/rekuiper/releases)
[![License: MIT or Apache-2.0](https://img.shields.io/badge/license-MIT%20%2F%20Apache--2.0-yellow.svg)](https://github.com/ankur-paan/rekuiper/blob/main/LICENSE)

`rekuiper` is an ultra-fast, lightweight streaming SQL engine for edge computing, IIoT gateways, connected vehicles, and robotics, written in Rust. It serves as a 100% drop-in replacement for LF Edge eKuiper, delivering 150k–200k messages/second per core with minimal memory footprint and zero regression.

It features an embedded **Model Context Protocol (MCP)** server (`rekuiper-mcp`) allowing AI assistants (Antigravity IDE, Claude Desktop, Cursor) to inspect schemas, validate SQL offline, query topologies, and trace streaming rules in real time.

---

## Quick Start

### 1. Run with Docker

Start rekuiper with the REST API and Prometheus metrics enabled:

```bash
docker run -d --name rekuiper \
  -p 9081:9081 -p 20499:20499 \
  -e KUIPER__BASIC__CONSOLELOG=true \
  -e KUIPER__BASIC__PROMETHEUS=true \
  ankurkrp/rekuiper:0.503-beta
```

Verify that the engine is healthy:

```bash
curl http://localhost:9081/ping
# Response: pong
```

Access the OpenAPI Swagger UI in your browser:
```
http://localhost:9081/
```

---

### 2. Run with Docker Compose (Engine + Mosquitto + Redis)

Clone the repository and launch the multi-service edge stack:

```bash
git clone https://github.com/ankur-paan/rekuiper.git
cd rekuiper
docker compose -f deploy/docker/docker-compose.yml up -d
```

This stack includes:
* **rekuiper** (ports `9081`, `20499`)
* **Eclipse Mosquitto MQTT Broker** (port `1883`)
* **Redis Cache** (port `6379`)

To customize ports, image tags, or logging, copy the template and edit `.env`:
```bash
cp deploy/docker/.env.example deploy/docker/.env
docker compose -f deploy/docker/docker-compose.yml up -d
```

---

## Exposed Network Ports

| Port | Protocol | Purpose / Function | Default Host Bind |
| :--- | :--- | :--- | :--- |
| **`9081`** | HTTP / TCP | **REST API, OpenAPI UI & Management**<br>Stream/rule lifecycle, `kuiper` CLI, healthchecks, and rule test endpoints. | `0.0.0.0:9081` |
| **`20499`** | HTTP / TCP | **Prometheus Metrics**<br>Prometheus scraping endpoint (`/metrics`) when `KUIPER__BASIC__PROMETHEUS=true`. | `0.0.0.0:20499` |
| **`20498`** | TCP | **eKuiper RPC Protocol Parity**<br>Legacy Go RPC communication port. | `127.0.0.1:20498` |

---

## Configuration & Environment Variables

`rekuiper` supports granular configuration overrides via environment variables. Any configuration key in `etc/kuiper.yaml` can be overridden by converting sections and keys with double underscores: `KUIPER__<SECTION>__<KEY>`.

### Common Configuration Variables

| Variable | Default | Description |
| :--- | :--- | :--- |
| `KUIPER__BASIC__LOGLEVEL` | `info` | Logging verbosity: `debug`, `info`, `warn`, `error`, `fatal`. |
| `KUIPER__BASIC__CONSOLELOG` | `true` | When `true`, streams logs directly to container stdout/stderr. |
| `KUIPER__BASIC__DEBUG` | `false` | Enables deep query planning and state transition debug output. |
| `KUIPER__BASIC__PROMETHEUS` | `false` | Set to `true` to enable the Prometheus `/metrics` scraping server. |
| `KUIPER__BASIC__PROMETHEUSPORT` | `20499` | Port for the Prometheus metrics listener. |
| `KUIPER__BASIC__AUTHENTICATION` | `false` | When `true`, enforces RSA JWT token validation on REST routes. |
| `KUIPER__BASIC__TIMEZONE` | `Local` | IANA timezone string (e.g. `UTC`, `America/New_York`, `Local`). |
| `MQTT_SOURCE__DEFAULT__SERVER` | `tcp://127.0.0.1:1883` | Default upstream broker URL for MQTT sources without explicit server URLs. |
| `RUST_LOG` | `info` | Rust tracing filter (e.g. `info`, `rekuiper=debug`, `tokio=warn`). |

---

## Persistent Storage & Volumes

| Container Path | Purpose |
| :--- | :--- |
| `/kuiper/data` | Stores SQLite catalog database (`sqliteKV.db`), rule checkpoints, and persisted state. Mount a host volume here for persistence across restarts. |
| `/kuiper/etc` | Contains configuration files (`kuiper.yaml`, `mqtt_source.yaml`, etc.). Mount to supply custom configuration overlays. |
| `/kuiper/log` | File-based logging directory when `KUIPER__BASIC__FILELOG=true`. |

### Example with Persistent Volume

```bash
docker run -d --name rekuiper \
  -p 9081:9081 -p 20499:20499 \
  -v rekuiper-data:/kuiper/data \
  -v rekuiper-etc:/kuiper/etc \
  ankurkrp/rekuiper:0.503-beta
```

---

## Using the Built-In CLI & Tools

The container includes both the `kuiper` CLI and the `rekuiper-mcp` Model Context Protocol binary in `/kuiper/bin`:

```bash
# Ping the engine from inside the container
docker exec -it rekuiper kuiper ping

# Create a stream
docker exec -it rekuiper kuiper create stream demo '(temp float, humidity bigint) WITH (FORMAT="JSON", DATASOURCE="devices/+/events")'

# Check rule status
docker exec -it rekuiper kuiper show streams

# Inspect the Model Context Protocol server capabilities
docker exec -it rekuiper /kuiper/bin/rekuiper-mcp --help
```

---

## Supported Architectures

Official multi-platform images are published for:
* **`linux/amd64`** (x86_64 standard servers and edge gateways)
* **`linux/arm64`** (Raspberry Pi 4/5, NVIDIA Jetson, Apple Silicon, AWS Graviton)

---

## Resources & Documentation

* **GitHub Repository**: [https://github.com/ankur-paan/rekuiper](https://github.com/ankur-paan/rekuiper)
* **Releases & Binary Downloads**: [https://github.com/ankur-paan/rekuiper/releases](https://github.com/ankur-paan/rekuiper/releases)
* **Model Context Protocol Guide**: [crates/rekuiper-mcp](https://github.com/ankur-paan/rekuiper/tree/main/crates/rekuiper-mcp)
* **Issue Tracker**: [https://github.com/ankur-paan/rekuiper/issues](https://github.com/ankur-paan/rekuiper/issues)
* **Benchmark Evidence**: [https://github.com/ankur-paan/rekuiper/tree/main/test/benchmark/iiot-mqtt](https://github.com/ankur-paan/rekuiper/tree/main/test/benchmark/iiot-mqtt)
