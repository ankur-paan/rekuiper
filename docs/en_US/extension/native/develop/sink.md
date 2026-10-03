# Sink Extension

::: danger Status: Unsupported in rekuiper (Legacy eKuiper Reference Only)
Go C-shared native `.so` dynamic plugins are unsupported in rekuiper. rekuiper is implemented in Rust and does not load Go dynamic plugins.

Core sink connectors (MQTT, REST/HTTP, WebSocket, Kafka, SQL, Redis, File) are compiled directly into the rekuiper engine binary. To stream data to custom external destinations, use:
- **[REST Sink](../../../guide/sinks/builtin/rest.md)** for HTTP webhooks or RESTful endpoints.
- **[MQTT Sink](../../../guide/sinks/builtin/mqtt.md)** or **[Kafka Sink](../../../guide/sinks/plugin/kafka.md)** for message streaming.

This guide is preserved as a technical reference for legacy eKuiper installations.
:::

Sinks forward processed stream data to external storage, message brokers, or network endpoints.

## Development

To create a sink plugin, implement the [api.Sink](https://github.com/lf-edge/ekuiper/blob/master/contract/api/sink.go) interface and export it from a Go plugin.

Before developing, [configure the plugin development environment](../overview.md#setup-the-plugin-developing-environment).

Sinks belong to two categories based on payload encoding:
- `BytesCollector`: Receives serialized binary payloads (such as MQTT sink).
- `TupleCollector`: Receives structured map tuples and handles internal serialization (such as SQL sink).

### General Methods

All sink implementations must provide these methods:

1. **Provision**:

   ```go
   Provision(ctx StreamContext, configs map[string]any) error
   ```

   Initializes the sink instance with configuration properties from the rule action definition (such as host, port, credentials).

2. **Connect**:

   ```go
   Connect(ctx StreamContext, sch StatusChangeHandler) error
   ```

   Establishes the connection to the external destination. Reconnection logic should run asynchronously and notify connection status changes through the status handler callback.

3. **Collect**:

   Receives data from upstream operators and writes it to the target system.

4. **Close**:

   ```go
   Close(ctx StreamContext) error
   ```

   Terminates active connections and releases resources when the rule terminates.

5. **Export the Symbol**:

   Export a constructor function at the end of the file:

   ```go
   func MySink() api.Sink {
       return &mySink{}
   }
   ```

### Sink Type Implementations

- **BytesCollector**:

  ```go
  Collect(ctx StreamContext, item RawTuple) error
  ```

  Extract serialized bytes with `item.Raw()`. To enable automatic retry, return error messages starting with `"io error"`.

- **TupleCollector**:

  ```go
  Collect(ctx StreamContext, item MessageTuple) error
  CollectList(ctx StreamContext, items MessageTupleList) error
  ```

  Processes single structured records or batch lists of tuples.

### Updatable Sinks

If the sink supports update or delete mutations, inspect the `rowkindField` property during `Provision`. In `Collect`, extract the action string (`insert`, `update`, `upsert`, or `delete`) to format the corresponding target command.

### Dynamic Properties

To evaluate template expressions in sink properties at runtime, use the dynamic properties helper:

```go
func Collect(ctx StreamContext, item RawTuple) error {
    if dp, ok := item.(api.HasDynamicProps); ok {
        temp, transformed := dp.DynamicProps("propName")
        if transformed {
            tpc = temp
        }
    }
    return nil
}
```

## Usage in Rules

Specify the custom sink by name in the rule `actions` definition:

```json
{
  "id": "rule1",
  "sql": "SELECT demo.temperature, demo1.temp FROM demo LEFT JOIN demo1 ON demo.timestamp = demo1.timestamp WHERE demo.temperature > demo1.temp GROUP BY demo.temperature, HOPPINGWINDOW(ss, 20, 10)",
  "actions": [
    {
      "mySink": {
        "server": "tcp://47.52.67.87:1883",
        "topic": "demoSink"
      }
    }
  ]
}
```
