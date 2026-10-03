# Extension Architecture

rekuiper provides a flexible extension architecture designed for high performance, portability, and safe sandboxing on resource-constrained edge hardware.

---

## Extension Models in rekuiper

| Mechanism | Purpose | Supported Languages | Isolation and Safety | Status in rekuiper |
| :--- | :--- | :--- | :--- | :--- |
| **Built-in Connectors** | High-performance sources and sinks (Kafka, SQL, Redis, WebSocket, File, MQTT, HTTP) | Rust (compiled into binary) | Process internal | **Fully Supported** |
| **WebAssembly (Wasm)** | Custom scalar and analytical SQL functions | Rust, C, C++, Go, Zig (compiled to `.wasm`) | Sandboxed bytecode runtime (`wasmi`) | **Fully Supported** |
| **External Services** | Direct invocation of external RPC or HTTP services in SQL | Any language (REST or gRPC) | Separate network process | **Fully Supported** |
| **Script Functions (UDF)** | Lightweight scalar functions defined in SQL scripts | JavaScript (ECMAScript) | Embedded Rust interpreter (`boa_engine`) | **Fully Supported** |
| **Model Context Protocol (MCP)** | AI assistant control, query simulation, and validation | Native Rust daemon (`rekuiper-mcp`) | Standard JSON-RPC stdio | **Fully Supported** |
| **Go Native (.so) Plugins** | Legacy C-shared dynamic libraries | Go | In-process dynamic loading | **Unsupported** |
| **Portable Plugins** | Legacy out-of-process IPC plugins | Go, Python | nanomsg IPC sockets | **Unsupported (Returns 501)** |

::: danger Legacy Plugin Support Notice
1. **Go Native (.so) Plugins**: Unsupported. rekuiper is implemented in Rust and does not load Go dynamic shared objects. All high-demand connectors (Kafka, SQL, WebSocket, etc.) are compiled directly into the rekuiper engine binary.
2. **Portable Plugins (`/plugins/portables`)**: Unsupported. The portable plugin endpoints return `501 Not Implemented`. For out-of-process multi-language logic, use [External Services (gRPC/REST)](external/external_func.md).
:::

---

## 1. Built-in Connectors

High-demand data sources and sinks are compiled directly into the rekuiper Rust engine. They deliver zero-copy throughput and zero garbage collection overhead without requiring external plugin compilation:
- **Sources**: MQTT, HTTP (`httppull`, `httppush`), File (JSON, CSV, Parquet), WebSocket, Redis, Kafka (`rskafka`), SQL (`sqlx`: PostgreSQL, SQLite), Simulator, RabbitMQ, EdgeX, Neuron.
- **Sinks**: MQTT, REST/HTTP, File, WebSocket, Redis, Kafka, SQL, RabbitMQ, EdgeX, Neuron, Log, Memory, Nop.

---

## 2. WebAssembly (Wasm) Functions

[WebAssembly (Wasm)](./wasm/overview.md) provides sandboxed, high-performance function extensions. You can write algorithms in Rust, C, C++, or Go and compile them into portable `.wasm` binaries:

- **Safe Execution**: Sandboxed memory space prevents memory corruption and engine panics.
- **Embedded Engine**: Uses the lightweight embedded `wasmi` interpreter without external runtime dependencies.
- **Portability**: The same `.wasm` binary runs across x86_64, ARMv7, and AArch64 architectures without recompilation.
- **Dynamic Deployment**: Install and register Wasm modules dynamically at runtime through the REST API or the CLI.

```sql
SELECT
  deviceId,
  custom_filter(raw_reading) AS filtered_value
FROM
  sensor_stream;
```

For instructions, refer to the [WebAssembly Extension Guide](./wasm/overview.md).

---

## 3. External Services (gRPC and REST)

If you maintain an external microservice, such as a Python AI inference server or a proprietary protocol gateway, rekuiper can invoke it directly from SQL:

```sql
SELECT
  deviceId,
  inference(image_payload)->label AS defect_class
FROM
  camera_stream;
```

- **Protocols**: Supports standard gRPC and HTTP REST endpoints.
- **Direct Configuration**: Point rekuiper to the service configuration and invoke functions immediately without custom plugin code.

For instructions, refer to the [External Services Guide](external/external_func.md).

---

## 4. JavaScript Script Functions (UDF)

For quick transformations that do not require separate compilation, rekuiper supports JavaScript functions registered through the REST API or the CLI.

The engine executes these scripts using an embedded 100% Rust ECMAScript interpreter (`boa_engine`). They are suitable for string formatting, mathematical conversions, and lightweight payload transformations.

For instructions, refer to the [Script Functions Guide](script/overview.md).

---

## 5. Model Context Protocol (MCP)

For developer productivity and automated operational pipelines, rekuiper includes a native [Model Context Protocol (MCP)](../mcp/overview.md) server (`rekuiper-mcp`).

It allows AI assistants (Cursor, Claude Desktop, Antigravity) to:
- Parse and validate streaming SQL ASTs offline without network overhead.
- Test SQL transformations in memory against mock event payloads.
- List, inspect, and deploy stream definitions and rule topologies.
- Query engine metrics and trace event latency across operator nodes.
