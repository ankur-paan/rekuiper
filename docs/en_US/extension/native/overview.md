# Native Plugins (Status in rekuiper)

::: danger Status: Unsupported in rekuiper (Legacy eKuiper Reference Only)
Go C-shared (`.so`) dynamic plugins from legacy eKuiper are unsupported in rekuiper. **rekuiper is implemented in Rust and does not load Go dynamic plugins.**
:::

## Status and Rationale

Legacy eKuiper used the Go `plugin` package to load dynamic `.so` libraries at runtime. That mechanism has several critical operational limitations:

1. **Toolchain Coupling**: Go dynamic plugins require the exact compiler version, operating system, and dependency module paths of the host binary.
2. **Platform Incompatibility**: Dynamic Go plugins are unreliable or unsupported on Alpine Linux (musl libc), macOS, Windows, and embedded ARM distributions.
3. **Rust Runtime Architecture**: rekuiper is implemented in Rust to deliver high throughput and zero garbage collection pauses. It does not run a Go runtime and does not load Go `.so` shared objects.

---

## Supported Extensibility Options in rekuiper

To extend rekuiper with custom sources, sinks, or analytical functions, use these supported mechanisms:

| Mechanism | Supported Languages | Use Case |
| :--- | :--- | :--- |
| **[Built-in Connectors](../../guide/sources/overview.md)** | Rust (embedded in binary) | Pre-compiled connectors for Kafka, SQL, Redis, WebSocket, File, MQTT, HTTP. |
| **[WebAssembly (Wasm)](../wasm/overview.md)** | Rust, C, C++, Go (TinyGo), Zig | Sandboxed, high-performance in-process functions and analytical logic. |
| **[External Services](../external/external_func.md)** | Any language (Python, Node.js, Go, Java, Rust) | Invoking external microservices and machine learning models over **gRPC** or **REST**. |
| **[Script Functions (UDF)](../script/overview.md)** | JavaScript | Lightweight runtime functions executed by the embedded `boa_engine` interpreter. |
| **[Model Context Protocol (MCP)](../../mcp/overview.md)** | AI Assistants and JSON-RPC clients | Managing rules, validating SQL ASTs, and testing pipelines through AI tools. |

---

## Legacy Documentation Notice

The child pages in this section describe the legacy Go plugin architecture as a technical reference for migrating from older eKuiper deployments. For new extensions, use WebAssembly, External Services, or Script Functions.
