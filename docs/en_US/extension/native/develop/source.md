# Source Extension

::: danger Status: Unsupported in rekuiper (Legacy eKuiper Reference Only)
Go C-shared native `.so` dynamic plugins are unsupported in rekuiper. rekuiper is implemented in Rust and does not load Go dynamic plugins.

Core source connectors (MQTT, HTTP Pull/Push, WebSocket, Kafka, SQL, Redis, File, Simulator) are compiled directly into the rekuiper engine binary. To ingest data from other systems, push events into:
- **[HTTP Push Source](../../../guide/sources/builtin/httppush.md)** for RESTful ingestion.
- **[MQTT Source](../../../guide/sources/builtin/mqtt.md)** for lightweight messaging.
- **[WebSocket Source](../../../guide/sources/builtin/websocket.md)** for bidirectional streaming.

This guide is preserved as a technical reference for legacy eKuiper installations.
:::

Sources ingest data from external systems into rekuiper streams and tables.

Sources belong to two categories:
- **Scan Source**: Ingests continuous data streams or scans entire tables.
- **Lookup Source**: Performs key-based lookups during table joins.

## Develop a Scan Source

To create a scan source, implement the [api.Source](https://github.com/lf-edge/ekuiper/blob/master/contract/api/source.go) interface and export it from a Go plugin.

Before developing, [configure the plugin development environment](../overview.md#setup-the-plugin-developing-environment).

Scan sources implement one of four interface types based on ingestion model and data representation:

- `ByteSource`: Push-based source receiving raw binary bytes. rekuiper decodes the payload according to stream format configurations.
- `TupleSource`: Push-based source that decodes custom payloads internally and emits structured map tuples.
- `PullBytesSource`: Pull-based source periodically polling external systems for binary payloads.
- `PullTupleSource`: Pull-based source periodically polling external systems and emitting structured map tuples.

### General Methods

All source plugins must implement these lifecycle methods:

1. **Provision**:

   ```go
   Provision(ctx StreamContext, configs map[string]any) error
   ```

   Initializes the source using configuration parameters parsed from the source YAML file.

2. **Connect**:

   ```go
   Connect(ctx StreamContext, sch StatusChangeHandler) error
   ```

   Establishes the connection to the external data source. Connection state changes notify the engine through the status handler callback.

3. **Ingestion Logic**:

   Implement `Subscribe` or `Pull` according to the source interface type.

4. **Close**:

   ```go
   Close(ctx StreamContext) error
   ```

   Closes open connections and releases resources when the rule terminates.

5. **Export the Symbol**:

   Export a constructor function at the end of the file:

   ```go
   func MySource() api.Source {
       return &mySource{}
   }
   ```

### Source Interface Implementations

- **ByteSource**:

  ```go
  Subscribe(ctx StreamContext, ingest BytesIngest, ingestError ErrorIngest) error
  ```

  Subscribes to external notifications and forwards raw bytes through `BytesIngest`.

- **TupleSource**:

  ```go
  Subscribe(ctx StreamContext, ingest TupleIngest, ingestError ErrorIngest) error
  ```

  Subscribes to external notifications and forwards decoded map objects through `TupleIngest`.

- **PullBytesSource**:

  ```go
  Pull(ctx StreamContext, trigger time.Time, ingest BytesIngest, ingestError ErrorIngest)
  ```

  Polls external systems at configured intervals and forwards binary payloads.

- **PullTupleSource**:

  ```go
  Pull(ctx StreamContext, trigger time.Time, ingest TupleIngest, ingestError ErrorIngest)
  ```

  Polls external systems at configured intervals and forwards decoded map objects.

## Develop a Lookup Source

A lookup source implements the [api.LookupSource](https://github.com/lf-edge/ekuiper/blob/master/pkg/api/stream.go) interface to query external systems during join operations.

- `LookupSource`: Decodes records internally and returns maps:

  ```go
  Lookup(ctx StreamContext, fields []string, keys []string, values []any) ([]map[string]any, error)
  ```

- `LookupBytesSource`: Returns binary payloads for automatic decoding:

  ```go
  Lookup(ctx StreamContext, fields []string, keys []string, values []any) ([][]byte, error)
  ```

## Source Traits

Extended sources can implement optional trait interfaces:

- **Rewindable Source (`api.Rewindable`)**: Required for checkpointing and exactly-once processing. Implement `GetOffset()` with thread-safe synchronization.
- **Bounded Source (`api.Bounded`)**: Emits `EOFIngest` when reading completes. The engine terminates the rule automatically upon receiving the end-of-file signal.

## Configuration and Usage

Source configuration files reside under `etc/sources/{sourceName}.yaml`.

Common parameters:
- `interval`: Polling interval in milliseconds for pull sources.
- `bufferLength`: Maximum queue capacity in memory. Default value is `102400`.

To use the custom source, declare it in the stream definition:

```sql
CREATE STREAM demo (
    USERID BIGINT,
    FIRST_NAME STRING,
    LAST_NAME STRING,
    NICKNAMES ARRAY(STRING),
    Gender BOOLEAN,
    ADDRESS STRUCT(STREET_NAME STRING, NUMBER BIGINT)
) WITH (DATASOURCE="mytopic", TYPE="mySource", CONF_KEY="democonf");
```
