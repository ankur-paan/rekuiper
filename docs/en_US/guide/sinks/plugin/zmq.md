# ZeroMQ Sink

::: danger Status: Unsupported in rekuiper (Legacy eKuiper Go Plugin)
The ZeroMQ sink was implemented as a Go C-shared dynamic plugin (`.so`) in legacy eKuiper.

**rekuiper is written in Rust and does NOT support or load Go dynamic plugins (`.so`).** The ZeroMQ sink is not implemented in rekuiper.

**Supported Alternatives in rekuiper**:
- For publish-subscribe messaging, use the built-in [MQTT Sink](../builtin/mqtt.md).
- For AMQP messaging, use the built-in [RabbitMQ Sink](../builtin/rabbitmq.md).
- For low-latency streaming, use the built-in [WebSocket Sink](../builtin/websocket.md).
- For high-throughput log streams, use the built-in [Kafka Sink](../builtin/kafka.md).
:::

## Overview (Legacy Reference Only)

In legacy eKuiper (Go), the ZeroMQ sink published query results to a ZeroMQ topic over TCP. This page is preserved only as an architectural reference for users who migrate from legacy Go eKuiper deployments.

## Legacy Configuration Parameters

In legacy eKuiper, the action configuration used these properties:

| Property Name | Optional | Description |
| :--- | :--- | :--- |
| `server` | False | The ZeroMQ server URL address (such as `tcp://127.0.0.1:5563`). |
| `topic` | True | The ZeroMQ topic name to publish to. |

Common sink properties (such as `sendSingle` and `dataTemplate`) were also supported. Refer to [Common Sink Properties](../overview.md#common-properties).

## Legacy Rule Example

```json
{
  "id": "rule_legacy_zmq",
  "sql": "SELECT * FROM demo WHERE temperature > 50",
  "actions": [
    {
      "zmq": {
        "server": "tcp://127.0.0.1:5563",
        "topic": "temp"
      }
    }
  ]
}
```

## Migration Path to rekuiper

To migrate rules that used ZeroMQ to rekuiper:

1. Replace the `zmq` action with a supported built-in sink such as `mqtt` or `websocket`:

```json
{
  "id": "rule_migrated",
  "sql": "SELECT * FROM demo WHERE temperature > 50",
  "actions": [
    {
      "mqtt": {
        "server": "tcp://127.0.0.1:1883",
        "topic": "temp"
      }
    }
  ]
}
```

2. If your downstream consumer requires ZeroMQ, deploy a lightweight bridge process (for example, MQTT-to-ZeroMQ or WebSocket-to-ZeroMQ) outside of rekuiper.
