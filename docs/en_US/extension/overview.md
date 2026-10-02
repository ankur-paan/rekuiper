# Extension Architecture

rekuiper provides a flexible extension architecture designed for high performance, portability, and safe sandboxing on resource-constrained edge hardware.

---

## Extension Models in rekuiper

| Mechanism | Purpose | Supported Languages | Isolation and Safety | Performance |
| :--- | :--- | :--- | :--- | :--- |
| **Built-in Connectors** | Core sources and sinks (Kafka, SQL, Redis, WebSocket, File, MQTT, HTTP) | Rust (compiled into binary) | Process internal | Highest (zero copy, zero GC) |
| **WebAssembly (Wasm)** | Custom scalar and analytical SQL functions | Rust, C, C++, Go, Zig (compiled to `.wasm`) | Sandboxed bytecode runtime | High (near-native speed) |
| **External Services** | Direct invocation of external RPC or HTTP services in SQL | Any language (REST or gRPC) | Separate network process | Network dependent |
| **Script Functions (UDF)** | Lightweight scalar functions defined in SQL scripts | JavaScript | In-engine interpreter | Medium (interpreted) |
| **Model Context Protocol (MCP)** | AI assistant control, query simulation, and validation | Native Rust daemon (`rekuiper-mcp`) | Standard JSON-RPC stdio | Real-time |

::: tip Status of Go Native (.so) Plugins
Legacy eKuiper Go-based C-shared dynamic plugins (`.so`) are not supported in rekuiper. High-demand connectors (including Kafka, SQL, and WebSocket) are compiled directly into the rekuiper engine binary. For custom user-defined logic, use WebAssembly (Wasm) or an external HTTP/gRPC service.
:::

---

## 1. WebAssembly (Wasm) Functions

[WebAssembly (Wasm)](./wasm/overview.md) provides sandboxed, high-performance function extensions. You can write algorithms in Rust, C, C++, or Go and compile them into portable `.wasm` binaries:

- **Safe Execution**: Sandboxed memory space prevents memory corruption and engine panics.
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

## 2. External Services (gRPC and REST)

If you maintain an external service, such as a Python inference server or a proprietary protocol gateway, rekuiper can invoke it directly from SQL:

```sql
SELECT
  deviceId,
  inference(image_payload)->label AS defect_class
FROM
  camera_stream;
```

- **Protocols**: Supports gRPC and standard HTTP REST endpoints.
- **Direct Configuration**: Point rekuiper to the service configuration and invoke functions immediately without custom plugin code.

For instructions, refer to the [External Services Guide](external/external_func.md).

---

## 3. JavaScript Script Functions (UDF)

For quick transformations that do not require separate compilation, rekuiper supports JavaScript functions registered through the REST API or the CLI.

The engine interprets these functions internally. They are suitable for string formatting, mathematical conversions, and lightweight payload transformations. For high-throughput streams (exceeding 50,000 messages per second), use built-in SQL functions or Wasm.

For instructions, refer to the [Script Functions Guide](script/overview.md).

---

## 4. Model Context Protocol (MCP)

For developer productivity and automated operational pipelines, rekuiper includes a native [Model Context Protocol (MCP)](../mcp/overview.md) server (`rekuiper-mcp`).

It allows AI assistants (Cursor, Claude Desktop, Antigravity) to:
- Parse and validate streaming SQL ASTs offline without network overhead.
- Test SQL transformations in memory against mock event payloads.
- List, inspect, and deploy stream definitions and rule topologies.
- Query engine metrics and trace event latency across operator nodes.
