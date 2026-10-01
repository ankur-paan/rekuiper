# Model Context Protocol (MCP) Server

rekuiper includes a native Rust [Model Context Protocol (MCP)](https://modelcontextprotocol.io/) server (`rekuiper-mcp`). It connects AI coding assistants and autonomous agents directly to the rekuiper stream processing engine over standard JSON-RPC 2.0 stdio transport.

With `rekuiper-mcp`, assistants like Cursor, Claude Desktop, Antigravity IDE, and Continue.dev can validate streaming SQL offline, simulate rule evaluations against sample telemetry, inspect topologies, and manage live streams without leaving the editor.

---

## Architecture

`rekuiper-mcp` acts as a protocol bridge between your AI development environment and the rekuiper engine:

![rekuiper-mcp Architecture](../public/diagrams/mcp_architecture.svg)

### Stdio Communication Discipline
All JSON-RPC protocol frames are transmitted exclusively over standard input (`stdin`) and standard output (`stdout`). All diagnostic logging, connection notices, and errors are directed to standard error (`stderr`) to prevent protocol corruption.

---

## Client Setup & Configuration

Add `rekuiper-mcp` to your MCP client configuration file:

### 1. Antigravity IDE / Cursor (`mcp_config.json` or `.cursor/mcp.json`)

```json
{
  "mcpServers": {
    "rekuiper": {
      "command": "rekuiper-mcp",
      "args": [
        "--server-url",
        "http://127.0.0.1:9081"
      ],
      "env": {
        "RUST_LOG": "info"
      }
    }
  }
}
```

### 2. Claude Desktop (`claude_desktop_config.json`)

**macOS**: `~/Library/Application Support/Claude/claude_desktop_config.json`  
**Windows**: `%APPDATA%\Claude\claude_desktop_config.json`

```json
{
  "mcpServers": {
    "rekuiper": {
      "command": "/usr/local/bin/rekuiper-mcp",
      "args": [
        "--server-url",
        "http://127.0.0.1:9081"
      ]
    }
  }
}
```

### 3. Windows via WSL

If your rekuiper engine runs inside WSL2 or Docker:

```json
{
  "mcpServers": {
    "rekuiper": {
      "command": "wsl",
      "args": [
        "/usr/local/bin/rekuiper-mcp",
        "--server-url",
        "http://127.0.0.1:9081"
      ]
    }
  }
}
```

---

## Tool Capabilities

`rekuiper-mcp` exposes 42 tools across stream analytics and engine management:

### 1. SQL Intelligence & Offline Simulation

These tools leverage the embedded `rekuiper-sql` parser and evaluator directly, requiring zero network calls to the running daemon:

| Tool | Purpose |
| :--- | :--- |
| `validate_sql` | Parses SQL AST, verifies keywords, and validates streaming DDL syntax offline. |
| `test_sql_expression` | Executes SQL queries and projections against mock JSON event payloads in memory. |
| `explain_sql` | Deconstructs a SQL query into sources, projections, joins, window clauses, and filters. |

#### Example: Testing a Streaming Query Offline
You can ask your AI assistant:
> *"Test if `SELECT temperature * 1.8 + 32 AS temp_f FROM stream WHERE temperature > 20` works with payload `{"temperature": 25.0}`."*

The assistant calls `test_sql_expression` and inspects the computed output in memory without publishing events to a broker.

### 2. Stream & Table DDL Management

| Tool | Purpose |
| :--- | :--- |
| `list_streams` | List all active stream definitions in the catalog. |
| `get_stream` | Inspect stream schema, data format (JSON/Protobuf), and datasource options. |
| `create_stream` | Execute a `CREATE STREAM` statement. |
| `delete_stream` | Drop an existing stream from the catalog. |
| `push_stream_data` | Ingest test events directly into a stream endpoint. |
| `list_tables` | List all dimension and lookup tables. |
| `create_table` | Register an external lookup table (File, SQLite, Redis, Memory). |
| `delete_table` | Drop a dimension table from the catalog. |
| `push_table_data` | Insert or update records in a lookup table. |

### 3. Rule Lifecycle & DAG Inspection

| Tool | Purpose |
| :--- | :--- |
| `list_rules` | Enumerate all deployed rules and their execution statuses. |
| `get_rule` | Retrieve complete rule JSON (SQL query, sinks, QoS, checkpoint settings). |
| `create_rule` | Deploy a new streaming rule topology. |
| `update_rule` | Modify SQL logic or sinks for an active rule. |
| `start_stop_rule` | Start, pause, or restart a rule. |
| `bulk_start_stop_rules` | Batch start or stop multiple rules simultaneously. |
| `get_rule_status` | Retrieve real-time throughput metrics, latency, and error counters. |
| `get_rule_topo` | Retrieve DAG topological execution graph of source, operator, and sink nodes. |
| `reset_rule_state` | Clear checkpointed offsets and state store for clean rule restarts. |

### 4. Distributed Tracing & Diagnostics

| Tool | Purpose |
| :--- | :--- |
| `start_rule_trace` | Begin an active tracing session for a running rule. |
| `stop_rule_trace` | Conclude an active tracing session. |
| `get_rule_traces` | List recorded trace sessions. |
| `get_trace_details` | Inspect per-operator latency, event throughput, and intermediate payloads. |

### 5. Connection Pooling & Extensibility

| Tool | Purpose |
| :--- | :--- |
| `list_connections` | List reusable connection resource definitions. |
| `create_connection` | Register a shared connection (MQTT broker, Kafka cluster, SQL database). |
| `delete_connection` | Remove a connection from the resource pool. |
| `list_plugins` | Enumerate installed native and portable plugins. |
| `list_javascript_udfs` | List registered JavaScript User Defined Functions. |
| `create_javascript_udf` | Register a custom scalar JavaScript function for streaming SQL. |
| `delete_javascript_udf` | Delete a JavaScript UDF. |

### 6. Health & Universal REST Proxy

| Tool | Purpose |
| :--- | :--- |
| `get_engine_metrics` | Fetch engine uptime, memory usage, CPU load, and message throughput. |
| `ping_engine` | Test daemon connectivity and measure ping latency. |
| `export_data` | Export a full or filtered JSON backup of the catalog. |
| `import_data` | Restore catalog configuration from a JSON backup. |
| `execute_rekuiper_api` | Universal proxy executing any arbitrary HTTP method (`GET`, `POST`, `PUT`, `DELETE`, `PATCH`) against any rekuiper REST endpoint. |

---

## Live Resources

`rekuiper-mcp` exposes engine state as readable MCP resources using `rekuiper://` URIs:

| Resource URI | Content Description |
| :--- | :--- |
| `rekuiper://rules` | Active rules catalog with SQL queries and configured sinks. |
| `rekuiper://streams` | Registered stream sources, schemas, and serialization formats. |
| `rekuiper://tables` | Registered lookup and dimension tables. |
| `rekuiper://connections` | Active connection pool definitions. |
| `rekuiper://udfs` | Custom JavaScript UDF definitions. |
| `rekuiper://metrics` | Engine telemetry, memory footprint, and throughput rates. |
| `rekuiper://metadata/sources` | Catalog of available source connectors. |
| `rekuiper://metadata/sinks` | Catalog of available sink connectors. |
| `rekuiper://metadata/functions` | Catalog of built-in SQL mathematical, string, and window functions. |

---

## Specialized Prompts

`rekuiper-mcp` provides pre-engineered prompt workflows that AI assistants can execute on demand:

- `troubleshoot_rule`: Step-by-step diagnostic procedure for rules with high latency or dropped messages.
- `optimize_stream_sql`: SQL analysis for temporal window performance, memory allocation, and predicate pushdown.
- `generate_iot_alert_rule`: Creates end-to-end industrial monitoring rules with deadbanding, windowing, and alert actions.
- `create_end_to_end_pipeline`: Interactive pipeline generator linking streams, lookup tables, and multi-sink fanout.
- `diagnose_data_drop`: Pinpoints discrepancies between source ingestion rates and sink output rates.

---

## Building from Source

To compile the `rekuiper-mcp` binary:

```shell
# Debug build
cargo build -p rekuiper-mcp

# Optimized release binary
cargo build --release -p rekuiper-mcp
```

The resulting binary will be located at `target/release/rekuiper-mcp`.
