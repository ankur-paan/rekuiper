# Kafka Sink

::: tip Status: Supported as Built-in Connector in rekuiper
rekuiper includes a built-in Kafka sink connector implemented directly in Rust with `rskafka`. You do not need to compile or deploy Go dynamic plugins (`.so` files) or build custom plugin container images.
:::

The Kafka sink publishes output messages to an Apache Kafka topic.

## Configuration Properties

| Property Name | Optional | Default Value | Description |
|---|---|---|---|
| `brokers` | True | `"127.0.0.1:9092"` | Comma-separated list of Kafka broker addresses (`host:port`). |
| `topic` | False | None | Target Kafka topic name. |
| `partition` | True | `0` | Target topic partition index. |
| `key` | True | None | Field name from the result record used as the Kafka message key. |

rekuiper serializes each outgoing record into JSON bytes and delivers it to the designated Kafka topic partition using bounded asynchronous retries.

## Sample Usage

### Basic Kafka Publication

The following rule processes high-temperature readings and publishes the results to a Kafka topic:

```json
{
  "id": "rule_kafka_alert",
  "sql": "SELECT deviceId, temperature, timestamp FROM sensorStream WHERE temperature > 50.0",
  "actions": [
    {
      "log": {}
    },
    {
      "kafka": {
        "brokers": "10.0.0.10:9092,10.0.0.11:9092",
        "topic": "alerts_telemetry",
        "partition": 0
      }
    }
  ]
}
```

### Partitioning by Message Key

To ensure that events from the same device route consistently to Kafka partitions, configure the `key` property:

```json
{
  "id": "rule_kafka_keyed",
  "sql": "SELECT deviceId, reading, ts FROM rawStream",
  "actions": [
    {
      "kafka": {
        "brokers": "127.0.0.1:9092",
        "topic": "device_events",
        "key": "deviceId"
      }
    }
  ]
}
```

## Network Configuration Notes

When running rekuiper in containerized environments (such as Docker or Kubernetes), verify network reachability to the Kafka brokers:

1. Use container service names or bridge network hostnames instead of `localhost` or `127.0.0.1`.
2. Ensure `KAFKA_CFG_ADVERTISED_LISTENERS` on the Kafka broker is resolvable and reachable from the rekuiper network namespace.
