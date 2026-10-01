# Source Connectors

In the eKuiper source code, there are built-in sources and sources in extension.

## Ingestion Mode

The source connector provides the connection to an external system to load data in. Regarding data loading mechanism, there are two modes:

- Scan: load the data events one by one like a stream which is driven by event. Such mode of source can be used in stream or scan table.
- Lookup: refer to external content when needed, only used in lookup table.

Each source will support one or both modes. In the source page, a badge will show if the mode is supported.

## Built-in Sources

The following sources are built directly into the rekuiper engine:

- [MQTT source](./builtin/mqtt.md): subscribe to MQTT topics.
- [HTTP pull source](./builtin/http_pull.md): periodically pull data from REST/HTTP endpoints.
- [HTTP push source](./builtin/http_push.md): ingest data pushed to rekuiper via HTTP POST.
- [WebSocket source](./builtin/websocket.md): ingest real-time events over WebSocket connections.
- [Redis source](./builtin/redis.md): read from Redis keys or use Redis as a lookup table.
- [RedisSub source](./builtin/redisSub.md): subscribe to messages from Redis channels.
- [Kafka source](./plugin/kafka.md): consume stream records directly from Apache Kafka topics.
- [SQL source](./plugin/sql.md): periodically query relational databases via SQL.
- [File source](./builtin/file.md): read data from local files or directories.
- [Memory source](./builtin/memory.md): read from internal in-memory topics for rule pipelining.
- [Simulator source](./builtin/simulator.md): generate mock sensor telemetry for testing.

> [!NOTE]
> Legacy eKuiper Go-based C-shared dynamic plugins (`.so`) are not supported in rekuiper. Connectors like Kafka, SQL, and WebSocket are compiled natively into the binary. For custom sources, use WebAssembly (Wasm) or an external HTTP service.

## Use of Sources

The user uses sources by means of streams or tables. The type `TYPE` property needs to be set to the name of the desired source in the stream properties created. The user can also change the behavior of the source during stream creation by configuring various general source attributes, such as the decoding type (default is JSON), etc. For the general properties and creation syntax supported by creating streams, please refer to the [Stream Specification](../streams/overview.md).

## Runtime Nodes

When users create rules, the data source is a logical node. Depending on the type of the data source itself and the
user's configuration, each data source at runtime may generate an execution plan consisting of multiple nodes. The data
source property configuration items are numerous, and the logic during actual runtime is quite complex. By breaking down
the execution plan into multiple nodes, the following benefits are primarily achieved:

- There are many shared properties and implementation logic among various data sources, such as data format decoding.
  Splitting the shared property implementation into independent runtime nodes facilitates node reuse, simplifies the
  implementation of data source nodes (Single Responsibility Principle), and improves the maintainability of nodes.
- The properties of the data source include time-consuming calculations, such as decompression and decoding. With a
  single node's metrics, it is difficult to distinguish the actual execution status of sub-tasks when the data source is
  executed. After splitting the nodes, finer-grained runtime metrics can be supported to understand the status and
  latency of each sub-task.
- After sub-task splitting, parallel computation can be implemented, improving the overall efficiency of rule execution.

### Execution Plan

The physical execution plan of the data source node can be split into:

Connector --> RateLimit --> Decompress --> Decode --> Preprocess

The conditions for generating each node are:

- **Connector**: Implemented for every data source, used to connect to external data sources and read data into the
  system.
- **RateLimit**: Applicable when the data source type is a push source (such as MQTT, a source that reads data in
  through subscription/push rather than pull) and the `interval` property is configured. This node is used to control
  the frequency of data inflow at the data source. For details, please refer to [Down Sampling](./down_sample.md).
- **Decompress**: Applicable when the data source type reads bytecode data (such as MQTT, which allows sending any
  bytecode rather than a fixed format) and the `decompress` property is configured. This node is used to decompress the
  data.
- **Decode**: Applicable when the data source type reads bytecode data and the `format` property is configured. This
  node will deserialize the bytecode based on the format configuration and schema-related configuration.
- **Preprocess**: Applicable when a schema is explicitly defined in the stream definition and `strictValidation` is
  turned on. This node will validate and transform the raw data according to the schema definition. Note that if type
  conversion is frequently required for the input data, this node may incur significant additional performance overhead.
