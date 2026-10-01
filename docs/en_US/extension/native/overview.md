# Native Plugins (Status in rekuiper)

> **Important**: Go C-shared (`.so`) dynamic plugins from legacy eKuiper are **unsupported** in the current version of rekuiper and in the near future.

## Status & Rationale

Legacy eKuiper used Go's internal `plugin` package to load dynamic `.so` libraries at runtime. In practice, this mechanism suffered from severe operational fragility:

1. **Strict Toolchain Coupling**: Go dynamic plugins require the exact same compiler version, operating system, and dependency module paths as the host binary.
2. **Platform Incompatibility**: Dynamic Go plugins are unreliable or unsupported on Alpine Linux (musl libc), macOS, Windows, and many embedded ARM distributions.
3. **Rust Runtime Architecture**: rekuiper is built in Rust for high throughput and zero garbage collection pauses. It does not run a Go runtime and does not load Go `.so` shared objects.

---

## Supported Extensibility Options in rekuiper

If you need to extend rekuiper with custom sources, sinks, or analytical functions, use one of the following supported approaches:

| Mechanism | Language Support | Use Case |
| :--- | :--- | :--- |
| **[WebAssembly (Wasm)](../wasm/overview.md)** | Rust, C, AssemblyScript, Go (TinyGo) | Sandboxed, high-performance in-process functions and logic. This is our primary target for future plugin development. |
| **[External Services](../external/external_func.md)** | Any language (Python, Node.js, Go, Java, Rust) | Querying external microservices and ML models over standard **gRPC** or **REST** protocols. |
| **[Portable Plugins](../portable/overview.md)** | Python, Go | Process-isolated plugins communicating via IPC sockets. |
| **[Model Context Protocol (MCP)](https://github.com/ankur-paan/rekuiper/tree/main/crates/rekuiper-mcp)** | Any AI Assistant / JSON-RPC client | Managing rules, validating SQL, and testing pipelines through AI tools (Cursor, Claude, Antigravity). |

---

## Legacy Documentation Notice

The child pages in this section describe the legacy Go plugin contract for reference when migrating from older eKuiper deployments. New extensions should use WebAssembly or External Services instead.
