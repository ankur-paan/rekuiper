# Extension Architecture

rekuiper provides a flexible extension architecture designed for high performance, portability, and safe sandboxing on resource-constrained edge hardware.

---

## Extension Models in rekuiper

| Mechanism | Purpose | Supported Languages | Isolation & Safety | Performance |
| :--- | :--- | :--- | :--- | :--- |
| **Built-in Connectors** | Core sources and sinks (Kafka, SQL, Redis, WebSocket, File, MQTT, HTTP) | Rust (compiled in) | Process internal | Highest (zero copy, zero GC) |
| **WebAssembly (Wasm)** | Custom scalar and analytic SQL functions | Rust, C, C++, Go, Zig (compile to `.wasm`) | Sandboxed bytecode runtime | High (near-native speed) |
| **External Services** | Direct invocation of external RPC/HTTP services in SQL | Any language (REST / gRPC) | Separate network process | Network dependent |
| **Script Functions (UDF)** | Lightweight scalar functions defined in SQL scripts | JavaScript | In-engine interpreter | Medium (interpreted) |
| **Model Context Protocol (MCP)** | Autonomous AI assistant control, query simulation, and validation | Native Rust daemon (`rekuiper-mcp`) | Standard JSON-RPC stdio | Real-time |

> [!IMPORTANT]
> **Status of Go Native (`.so`) Plugins**:
> Legacy eKuiper Go-based C-shared dynamic plugins (`.so`) are not supported in rekuiper. High-demand connectors (including Kafka, SQL, and WebSocket) are compiled directly into the rekuiper engine binary. For custom user-defined logic, use WebAssembly (Wasm) or an external HTTP/gRPC service.

---

## 1. WebAssembly (Wasm) Functions

[WebAssembly (Wasm)](./wasm/overview.md) provides sandboxed, high-performance function extensions. You can write algorithms in Rust, C, C++, or Go and compile them into a portable `.wasm` binary:

- **Safe Execution**: Sandboxed memory space prevents memory corruption or engine panics.
- **Portability**: The same `.wasm` binary runs across x86_64, ARMv7, and AArch64 architectures without recompilation.
- **Hot Deployment**: Install and register Wasm modules dynamically at runtime via the REST API or CLI.

```sql
SELECT
  deviceId,
  custom_filter(raw_reading) AS filtered_value
FROM
  sensor_stream
```

For complete instructions, refer to the [WebAssembly Extension Guide](./wasm/overview.md).

---

## 2. External Services (gRPC & REST)

If you already maintain an external microservice, such as a Python machine learning inference server or a proprietary protocol gateway, rekuiper can call it directly in SQL without custom plugin code:

```sql
SELECT
  deviceId,
  inference(image_payload)->label AS defect_class
FROM
  camera_stream
```

- **Protocols**: Supports gRPC and standard HTTP REST endpoints.
- **Zero Firmware Changes**: Point rekuiper to the service configuration and invoke functions immediately.

For details, refer to the [External Services Guide](external/external_func.md).

---

## 3. JavaScript Script Functions (UDF)

For quick transformations that do not warrant a separate compilation step, rekuiper supports JavaScript functions registered directly through the REST API or CLI.

These functions are interpreted inside the engine and are suitable for string formatting, mathematical conversions, and lightweight payload transformations. For high-frequency calculations (exceeding 50,000 msg/s), prefer built-in SQL functions or Wasm.

For details, refer to the [Script Functions Guide](script/overview.md).

---

## 4. Model Context Protocol (MCP)

For developer productivity and automated operational pipelines, rekuiper includes a native [Model Context Protocol (MCP)](../mcp/overview.md) server (`rekuiper-mcp`).

It allows AI assistants (Cursor, Claude Desktop, Antigravity) to:
- Parse and validate streaming SQL ASTs offline without network overhead.
- Test SQL transformations in memory against mock event payloads.
- Enumerate, inspect, and deploy stream definitions and rule topologies.
- Query engine metrics and trace event latency across operator nodes.
