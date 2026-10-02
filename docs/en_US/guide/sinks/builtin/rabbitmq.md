# RabbitMQ Sink Connector

The RabbitMQ sink connector publishes rule results to a RabbitMQ message broker. The connector uses the native AMQP 0-9-1 protocol.

You can use the RabbitMQ connector as a [source connector](../../sources/builtin/rabbitmq.md) and as a sink connector.

## Configuration Parameters

Configure the RabbitMQ sink in the `actions` array of a rule definition.

| Parameter | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `server` (or `url`) | string | `"amqp://guest:guest@127.0.0.1:5672/%2f"` | The AMQP broker connection URL. Supports TLS (`amqps://`) and secret templates. |
| `exchange` | string | `""` | The destination exchange name. An empty string publishes to the default direct exchange. |
| `routingKey` | string | `""` | The message routing key. Supports mustache templates (such as <code v-pre>devices.{{.device_id}}</code>). |
| `durable` | boolean | `true` | When `true`, exchange and queue declarations survive broker restarts. |
| `autoDelete` | boolean | `false` | When `true`, the broker removes declared resources when no longer in use. |
| `dataTemplate` | string | `""` | A template to format output payloads. If you omit this parameter, the connector publishes raw JSON. |

## Rule Configuration Examples

### Publish to Default Exchange

The following action publishes output data to a queue named `notifications`:

```json
{
  "rabbitmq": {
    "server": "amqp://guest:guest@127.0.0.1:5672/%2f",
    "routingKey": "notifications",
    "durable": true
  }
}
```

### Publish to a Topic Exchange with Template

The following action publishes alerts to an exchange with a dynamic routing key:

```json
{
  "rabbitmq": {
    "server": "amqp://app_user:{{vault://secret/rabbitmq#password}}@rabbitmq.internal:5672/%2f",
    "exchange": "alerts_exchange",
    "routingKey": "alerts.{{.device_id}}.critical",
    "dataTemplate": "{\"alert\": \"High temperature detected: {{.avg_temp}}\"}"
  }
}
```

## Complete Rule Example

This rule filters temperature values and publishes critical alerts to RabbitMQ:

```json
{
  "id": "rule_rabbitmq_alerts",
  "sql": "SELECT device_id, avg(temperature) AS avg_temp FROM factory_sensors GROUP BY device_id, TumblingWindow(ss, 10) HAVING avg(temperature) > 85.0",
  "actions": [
    {
      "rabbitmq": {
        "server": "amqp://app_user:{{env://RABBIT_PASSWORD}}@rabbitmq.internal:5672/%2f",
        "exchange": "alerts",
        "routingKey": "factory.critical",
        "durable": true
      }
    },
    {
      "log": {}
    }
  ]
}
```
