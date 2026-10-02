# EdgeX Message Bus Action

The EdgeX action publishes output messages to the EdgeX message bus.

> [!NOTE]
> When you use ZeroMQ as the message bus, the action creates a new EdgeX message bus on the rekuiper host. It does not use the original message bus exposed by the application service.
> Expose the port number on the host server before running the rekuiper server if other hosts need access to the service.

## Properties

| Property name | Optional | Description |
|---|---|---|
| type | true | The message bus type: `zero`, `mqtt`, or `redis`. Default: `redis`. |
| protocol | true | The protocol name. Default: `redis`. |
| host | true | The message bus host address. Default: `localhost`. |
| port | true | The message bus port number. Default: `6379`. |
| connectionSelector | true | Reuses a shared EdgeX message bus connection. Refer to [Connection selector](../../sources/builtin/edgex.md#connectionselector). |
| topic | true | The static publish topic. To use dynamic topics, leave this property empty and specify `topicPrefix`. Configure only one of `topic` or `topicPrefix`. Default: `application`. |
| topicPrefix | true | The prefix for dynamic topic generation. The final topic uses the format `$topicPrefix/$profileName/$deviceName/$sourceName`. |
| contentType | true | The MIME content type of published messages. Default: `application/json`. |
| messageType | true | The EdgeX message model type. To publish messages as application events, use `event`. To publish messages as event requests like device or core data services, use `request`. Default: `event`. |
| metadata | true | The field name in the SQL query that contains EdgeX metadata. Use `meta(*) AS field_name` in the SQL SELECT clause to capture all metadata. |
| profileName | true | The profile name in the EdgeX event structure. Values from `metadata` take precedence if present. |
| deviceName | true | The device name in the EdgeX event structure. Values from `metadata` take precedence if present. |
| sourceName | true | The source name in the EdgeX event structure. Values from `metadata` take precedence if present. |
| optional | true | Optional parameters for the `mqtt` message bus type. Refer to the list below. |

When `type` is set to `mqtt`, the following optional settings are supported:

- `ClientId`
- `Username`
- `Password`
- `Qos`
- `KeepAlive`
- `Retained`
- `ConnectionPayload`
- `CertFile`
- `KeyFile`
- `CertPEMBlock`
- `KeyPEMBlock`
- `SkipCertVerify`

::: v-pre
The EdgeX action supports data templates to format results. The output of the data template must be a JSON object string (for example, <code v-pre>"{\"key\":\"{{.key}}\"}"</code>). JSON array strings and plain text strings are not supported.
:::

Other common sink properties are supported. Refer to [sink common properties](../overview.md#common-properties) for more information.

## Send to Various Targets

Combine properties to publish messages to different EdgeX message bus targets.

### Publish to Redis Message Bus as Application Service

With default settings, the EdgeX sink publishes to Redis as application events:

```json
{
  "id": "ruleRedisEvent",
  "sql": "SELECT temperature * 3 AS t1, humidity FROM events",
  "actions": [
    {
      "edgex": {
        "protocol": "redis",
        "host": "localhost",
        "port": 6379,
        "topic": "application",
        "profileName": "ekuiperProfile",
        "deviceName": "ekuiper",
        "contentType": "application/json"
      }
    }
  ]
}
```

### Publish to Redis Message Bus as Device Service

Set `topicPrefix` and `messageType` to simulate an EdgeX device service:

```json
{
  "id": "ruleRedisDevice",
  "sql": "SELECT temperature * 3 AS t1, humidity FROM events",
  "actions": [
    {
      "edgex": {
        "protocol": "redis",
        "host": "localhost",
        "port": 6379,
        "topicPrefix": "edgex/events/device",
        "messageType": "request",
        "metadata": "metafield_name",
        "contentType": "application/json"
      }
    }
  ]
}
```

## Publish to MQTT Message Bus

The following rule publishes query results to an MQTT message bus:

```json
{
  "id": "ruleMqtt",
  "sql": "SELECT meta(*) AS edgex_meta, temperature, humidity, humidity*2 as h1 FROM demo WHERE temperature = 20",
  "actions": [
    {
      "edgex": {
        "protocol": "tcp",
        "host": "127.0.0.1",
        "port": 1883,
        "topic": "result",
        "type": "mqtt",
        "metadata": "edgex_meta",
        "contentType": "application/json",
        "optional": {
          "ClientId": "edgex_message_bus_001"
        }
      }
    }
  ]
}
```

## Publish to ZeroMQ Message Bus

The following rule publishes query results to a ZeroMQ message bus:

```json
{
  "id": "ruleZmq",
  "sql": "SELECT meta(*) AS edgex_meta, temperature, humidity, humidity*2 as h1 FROM demo WHERE temperature = 20",
  "actions": [
    {
      "edgex": {
        "protocol": "tcp",
        "host": "*",
        "port": 5571,
        "topic": "application",
        "profileName": "myprofile",
        "deviceName": "mydevice",
        "contentType": "application/json"
      }
    }
  ]
}
```

## Connection Reuse Example

To reuse an existing connection, omit connection properties and configure `connectionSelector`. Refer to [Connection selector](../../sources/builtin/edgex.md#connectionselector).

```json
{
  "id": "ruleRedisDevice",
  "sql": "SELECT temperature, humidity, humidity*2 as h1 FROM demo WHERE temperature = 20",
  "actions": [
    {
      "edgex": {
        "connectionSelector": "edgex.redisMsgBus",
        "topic": "application",
        "profileName": "myprofile",
        "deviceName": "mydevice",
        "contentType": "application/json"
      }
    }
  ]
}
```

## Dynamic Metadata

### Publish Results Without Original Metadata

In this mode, original metadata values (such as `id`, `profileName`, `deviceName`, `sourceName`, `origin`, and `tags` in `Events`, and `id`, `profileName`, `deviceName`, `origin`, and `valueType` in `Reading`) are not retained. rekuiper functions as an EdgeX service with its own device name and profile name.

1. Incoming message received on EdgeX `events` topic:

   ```json
   {
     "DeviceName": "demo", "Origin": 0,
     "readings": [
       {"ResourceName": "Temperature", "value": "30", "Origin": 123},
       {"ResourceName": "Humidity", "value": "20", "Origin": 456}
     ]
   }
   ```

2. Rule configuration:

   ```json
   {
     "id": "rule1",
     "sql": "SELECT temperature * 3 AS t1, humidity FROM events",
     "actions": [
       {
         "edgex": {
           "topic": "application",
           "deviceName": "kuiper",
           "profileName": "kuiperProfile",
           "contentType": "application/json"
         }
       }
     ]
   }
   ```

3. Data sent to EdgeX message bus:

   ```json
   {
     "DeviceName": "kuiper", "ProfileName": "kuiperProfile", "Origin": 0,
     "readings": [
       {"ResourceName": "t1", "value": "90", "Origin": 0},
       {"ResourceName": "humidity", "value": "20", "Origin": 0}
     ]
   }
   ```

- The device name changes to `kuiper`, and the profile name changes to `kuiperProfile`.
- All metadata fields are updated with new values generated by rekuiper.

### Publish Results Retaining Original Metadata

In this mode, rekuiper functions as a filter that preserves original metadata:

1. Incoming message received on EdgeX `events` topic:

   ```json
   {
     "DeviceName": "demo", "Origin": 0,
     "readings": [
       {"ResourceName": "Temperature", "value": "30", "Origin": 123},
       {"ResourceName": "Humidity", "value": "20", "Origin": 456}
     ]
   }
   ```

2. Rule configuration specifying `metadata`:

   ```json
   {
     "id": "rule1",
     "sql": "SELECT meta(*) AS edgex_meta, temperature * 3 AS t1, humidity FROM events WHERE temperature > 30",
     "actions": [
       {
         "edgex": {
           "topic": "application",
           "metadata": "edgex_meta",
           "contentType": "application/json"
         }
       }
     ]
   }
   ```

3. Data sent to EdgeX message bus:

   ```json
   {
     "DeviceName": "demo", "Origin": 0,
     "readings": [
       {"ResourceName": "t1", "value": "90", "Origin": 0},
       {"ResourceName": "humidity", "value": "20", "Origin": 456}
     ]
   }
   ```

- The metadata of the `Event` structure (`DeviceName` and `Origin`) is retained.
- Readings present in the original message retain their original metadata (such as `humidity`).
- Computed readings (such as `t1`) receive default metadata generated by rekuiper.
- If the SQL query contains aggregation functions, rekuiper uses metadata from the first message in the window.
