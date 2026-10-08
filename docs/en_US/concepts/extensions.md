# Extensions

rekuiper includes built-in sources, sinks, and SQL functions. For specialized integrations, rekuiper provides a modular extension framework.

---

## Extension Mechanisms

rekuiper supports four extension mechanisms designed for high performance, portability, and memory safety:

| Mechanism | Description | Supported Languages | Link |
| :--- | :--- | :--- | :--- |
| **WebAssembly (Wasm)** | High-performance in-process functions compiled to WebAssembly bytecode. | Rust, C, C++, Go (TinyGo), Zig | [WebAssembly Guide](../extension/wasm/overview.md) |
| **External Services** | Functions backed by external microservices over gRPC or REST. | Python, Node.js, Go, Java, Rust | [External Services Guide](../extension/external/external_func.md) |
| **Script Functions (UDF)** | Lightweight functions evaluated by an embedded JavaScript interpreter. | JavaScript (ECMAScript) | [Script Functions Guide](../extension/script/overview.md) |
| **Model Context Protocol (MCP)** | Tool server for AI assistant control, query validation, and pipeline tests. | Native JSON-RPC stdio | [MCP Overview](../mcp/overview.md) |

---

## Extension Points

1. **Source Connectors**: Ingest data from external protocols into streams or tables. Core connectors (Kafka, SQL, Redis, WebSocket, File, MQTT, HTTP) are compiled into the binary.
2. **Sink Connectors**: Deliver processed records to external destinations.
3. **Analytical Functions**: Transform and aggregate streaming records using SQL.

---

## Further Reading

- [Extension Architecture Overview](../extension/overview.md)
