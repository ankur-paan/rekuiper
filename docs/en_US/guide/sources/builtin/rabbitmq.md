# RabbitMQ Source Connector

<span style="background:green;color:white;padding:1px;margin:2px">stream source</span>
<span style="background:green;color:white;padding:1px;margin:2px">scan table source</span>

The RabbitMQ source connector reads messages from an AMQP 0-9-1 queue or exchange.

You can use the RabbitMQ connector as a source connector and as a [sink connector](../../sinks/builtin/rabbitmq.md).

## Configuration Properties

Configure the connector in the `WITH` clause of a `CREATE STREAM` statement, or configure the connector in a configuration file.

| Property | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `server` (or `url`) | string | `"amqp://guest:guest@127.0.0.1:5672/%2f"` | The AMQP broker connection URL. Supports TLS URLs (`amqps://`) and secret templates. |
| `queue` | string | `""` | The name of the queue to consume. |
| `exchange` | string | `""` | The exchange name to bind the queue to. |
| `routingKey` | string | `""` | The routing key or routing pattern for queue binding. |
| `durable` | boolean | `true` | When `true`, the queue survives broker restarts. |
| `autoDelete` | boolean | `false` | When `true`, the broker removes the queue after consumers disconnect. |
| `exclusive` | boolean | `false` | When `true`, only this connection can access the queue. |
| `prefetchCount`| integer | `100` | The AMQP basic QoS prefetch count for message flow control. |

## Stream Definition Examples

### Connect to a Standard Queue

The following statement creates a stream that consumes JSON data from a queue named `telemetry_queue`:

```sql
CREATE STREAM rabbit_telemetry () WITH (
    TYPE = "rabbitmq",
    SERVER = "amqp://guest:guest@127.0.0.1:5672/%2f",
    QUEUE = "telemetry_queue",
    FORMAT = "JSON"
);
```

### Connect with Topic Exchange Binding

The following statement binds a queue to a topic exchange with a routing key pattern:

```sql
CREATE STREAM factory_sensors (
    device_id STRING,
    temperature FLOAT,
    humidity FLOAT
) WITH (
    TYPE = "rabbitmq",
    SERVER = "amqp://guest:guest@10.0.0.15:5672/%2f",
    QUEUE = "sensor_events",
    EXCHANGE = "amq.topic",
    ROUTINGKEY = "sensors.temperature.*",
    DURABLE = "true",
    PREFETCHCOUNT = "50",
    FORMAT = "JSON"
);
```

## Security Credentials

Do not store plain-text passwords in stream definitions.

You can retrieve passwords from environment variables or HashiCorp Vault.

### Example with Environment Variable

```sql
CREATE STREAM secure_stream () WITH (
    TYPE = "rabbitmq",
    SERVER = "amqp://app_user:{{env://RABBIT_PASSWORD}}@rabbitmq.internal:5672/%2f",
    QUEUE = "telemetry",
    FORMAT = "JSON"
);
```

### Example with HashiCorp Vault

```sql
CREATE STREAM vault_stream () WITH (
    TYPE = "rabbitmq",
    SERVER = "amqp://app_user:{{vault://secret/rabbitmq#password}}@rabbitmq.internal:5672/%2f",
    QUEUE = "high_security_events",
    FORMAT = "JSON"
);
```

Refer to the [Dynamic Secrets](../../../configuration/secrets.md) documentation for more details.

## Rule Example

The following rule calculates the average temperature for each device in a 10-second tumbling window:

```sql
SELECT 
    device_id, 
    avg(temperature) AS avg_temp,
    count(*) AS alert_count
FROM factory_sensors
WHERE temperature > 75.0
GROUP BY device_id, TumblingWindow(ss, 10);
```
