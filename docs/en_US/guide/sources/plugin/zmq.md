# ZeroMQ Source Connector

::: danger Status: Unsupported in rekuiper (Legacy eKuiper Go Plugin)
The ZeroMQ source was implemented as a Go C-shared dynamic plugin (`.so`) in legacy eKuiper.

**rekuiper is written in Rust and does NOT support or load Go dynamic plugins (`.so`).** The ZeroMQ source is not implemented in rekuiper.

**Supported Alternatives in rekuiper**:
- For publish-subscribe telemetry, use the built-in [MQTT Source](../builtin/mqtt.md).
- For AMQP message ingestion, use the built-in [RabbitMQ Source](../builtin/rabbitmq.md).
- For streaming network sockets, use the built-in [WebSocket Source](../builtin/websocket.md).
- For high-throughput log streams, use the built-in [Kafka Source](../builtin/kafka.md).
:::

## Overview (Legacy Reference Only)

In legacy eKuiper (Go), the ZeroMQ source connector subscribed to ZeroMQ publishers over TCP. This page is preserved only as an architectural reference for users who migrate from legacy Go eKuiper deployments.

## Legacy Configuration Overview

In legacy eKuiper, the connector configuration resided in `etc/sources/zmq.yaml`:

```yaml
default:
  server: tcp://192.168.2.2:5563

test:
  server: tcp://127.0.0.1:5563
```

### Legacy Parameters

- `server`: ZeroMQ publisher endpoint URL (such as `tcp://127.0.0.1:5563`).

## Legacy Stream Definition

```sql
CREATE STREAM demo () WITH (
  DATASOURCE = "demo",
  FORMAT = "JSON",
  CONF_KEY = "test",
  TYPE = "zmq"
);
```

## Migration Path to rekuiper

To migrate streams that used ZeroMQ to rekuiper:

1. Replace the `TYPE = "zmq"` stream definition with a supported built-in connector:

```sql
CREATE STREAM demo () WITH (
  DATASOURCE = "demo/topic",
  FORMAT = "JSON",
  TYPE = "mqtt"
);
```

2. If your producer emits data exclusively over ZeroMQ, forward the messages into MQTT or WebSocket using a proxy process outside rekuiper.
