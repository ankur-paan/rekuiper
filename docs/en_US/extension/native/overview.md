# Native Plugins (Status in rekuiper)

::: tip Status of Go Native Plugins
Go C-shared (`.so`) dynamic plugins from legacy eKuiper are unsupported in the current version of rekuiper.
:::

## Status and Rationale

Legacy eKuiper used the Go `plugin` package to load dynamic `.so` libraries at runtime. That mechanism has several operational limitations:

1. **Toolchain Coupling**: Go dynamic plugins require the exact compiler version, operating system, and dependency module paths of the host binary.
2. **Platform Incompatibility**: Dynamic Go plugins are unreliable or unsupported on Alpine Linux (musl libc), macOS, Windows, and embedded ARM distributions.
3. **Rust Runtime Architecture**: rekuiper is implemented in Rust to deliver high throughput and zero garbage collection pauses. It does not run a Go runtime and does not load Go `.so` shared objects.

---

## Supported Extensibility Options in rekuiper

To extend rekuiper with custom sources, sinks, or analytical functions, use these supported mechanisms:

| Mechanism | Supported Languages | Use Case |
| :--- | :--- | :--- |
| **[WebAssembly (Wasm)](../wasm/overview.md)** | Rust, C, AssemblyScript, Go (TinyGo) | Sandboxed, high-performance in-process functions and logic. |
| **[External Services](../external/external_func.md)** | Any language (Python, Node.js, Go, Java, Rust) | Invoking external microservices and machine learning models over **gRPC** or **REST**. |
| **[Portable Plugins](../portable/overview.md)** | Python, Go | Process-isolated plugins communicating through IPC sockets. |
| **[Model Context Protocol (MCP)](https://github.com/ankur-paan/rekuiper/tree/main/crates/rekuiper-mcp)** | AI Assistants and JSON-RPC clients | Managing rules, validating SQL ASTs, and testing pipelines through AI tools. |

---

## Legacy Documentation Notice

The child pages in this section describe the legacy Go plugin architecture as a reference for migrating from older eKuiper deployments. For new extensions, use WebAssembly or External Services.
