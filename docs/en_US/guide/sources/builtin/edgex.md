# EdgeX Source Connector

<span style="background:green;color:white;padding:1px;margin:2px">stream source</span>
<span style="background:green;color:white;padding:1px;margin:2px">scan table source</span>

The EdgeX source connector subscribes to messages from the EdgeX message bus and routes them into the rekuiper stream processing engine.

The connector processes events without manual schema definitions by using the predefined data types in EdgeX reading objects.

The EdgeX connector operates as both a source connector and a [sink connector](../../sinks/builtin/edgex.md). This document explains source connector configuration and usage.

## Configuration Overview

Configure the connector using [environment variables](../../../configuration/configuration.md#environment-variable-syntax), the [REST API](../../../api/restapi/configKey.md), or the configuration file.

The default configuration file is `$rekuiper/etc/sources/edgex.yaml`. Settings defined in the `default` section serve as global defaults. Custom configurations in separate sections override default values.

Example configuration file:

```yaml
# Global EdgeX configurations
default:
  protocol: tcp
  server: localhost
  port: 5573
  topic: rules-events
  messageType: event
#  optional:
#    ClientId: client1
#    Username: user1
#    Password: password

# Override global configurations
demo1:
  protocol: tcp
  server: 10.211.55.6
  port: 5571
  topic: rules-events
```

## Global Configurations

Properties in the `default` section apply to all EdgeX connections unless explicitly overridden.

### Connection Parameters

- `protocol`: Protocol used to connect to the EdgeX message bus. Default is `tcp`.
- `server`: Server host address of the EdgeX message bus. Default is `localhost`.
- `port`: Port number of the EdgeX message bus. Default is `5573`.

### Connection Reuse

- `connectionSelector`: Specifies a named connection profile from `connections/connection.yaml` (for example, `edgex.redisMsgBus`). For details, refer to [Connection Management](../../connections/overview.md).

```yaml
default:
  protocol: tcp
  server: localhost
  port: 5573
  connectionSelector: edgex.redisMsgBus
  topic: rules-events
  messageType: event
```

> [!NOTE]
> When `connectionSelector` is specified, the engine ignores inline connection parameters (`protocol`, `server`, and `port`).

### Topic and Message Bus Parameters

- `topic`: EdgeX message bus topic name. Default is `rules-events`. Set `messageType` to match the target topic format.
- `type`: Message bus backend type:
  - `redis`: Uses Redis as the message bus. This is the default setting in EdgeX Docker Compose environments.
  - `mqtt`: Uses an MQTT broker as the message bus. Configure parameters in `optional`.
  - `zero`: Uses ZeroMQ as the message bus.
  - `nats-jetstream`: Uses NATS JetStream.
  - `nats-core`: Uses NATS Core.
- `messageType`: EdgeX payload data model:
  - `event`: Decodes payloads as `dtos.Event`. Use this setting when subscribing to EdgeX application service topics. This is the default setting.
  - `request`: Decodes payloads as `requests.AddEventRequest`. Use this setting when subscribing directly to core-data or device-service buses.

### Optional Parameters for MQTT Message Bus

When `type` is set to `mqtt`, configure connection settings under `optional`. Enclose all values in quotation marks:

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

## Custom Configurations

Define custom configuration sections in `edgex.yaml` for specific topics or broker addresses:

```yaml
demo1:
  protocol: tcp
  server: 10.211.55.6
  port: 5571
  topic: rules-events
```

Reference the configuration with `CONF_KEY="demo1"` in the stream DDL statement:

```sql
CREATE STREAM demo1 () WITH (FORMAT = "JSON", TYPE = "edgex", CONF_KEY = "demo1");
```

## Create a Stream Source

The EdgeX connector functions as a [stream source](../../streams/overview.md) or as a [scan table source](../../tables/scan.md).

### Create Stream via REST API

Send a `POST` request to `/streams`:

```json
{
  "sql": "CREATE STREAM demo1 () WITH (FORMAT = \"JSON\", TYPE = \"edgex\", CONF_KEY = \"demo1\")"
}
```

For REST API specifications, refer to [Streams Management with REST API](../../../api/restapi/streams.md).

### Create Stream via CLI

Run the `kuiper create stream` command:

```bash
bin/kuiper create stream demo '() WITH (FORMAT = "json", DATASOURCE = "demo", TYPE = "edgex")'
```

For CLI command syntax, refer to [Streams Management with CLI](../../../api/cli/streams.md).

### Stream Definition for EdgeX

Define EdgeX streams as [schemaless streams](../../streams/overview.md#schemaless-streams) (`CREATE STREAM demo ()`). EdgeX readings include predefined type information in reading objects.

## Automatic Data Type Conversion

rekuiper converts reading values automatically based on the EdgeX `ValueType` property:

- If the engine detects a matching data type, it converts the reading value.
- If no match exists, the original value remains unchanged.
- If type conversion fails, the engine drops the value and logs a warning.

### Boolean

When `ValueType` is `Bool`, rekuiper converts the value to a boolean:

- Values converted to `true`: `"1"`, `"t"`, `"T"`, `"true"`, `"TRUE"`, `"True"`
- Values converted to `false`: `"0"`, `"f"`, `"F"`, `"false"`, `"FALSE"`, `"False"`

### Bigint

When `ValueType` is `INT8`, `INT16`, `INT32`, `INT64`, `UINT`, `UINT8`, `UINT16`, `UINT32`, or `UINT64`, rekuiper converts the value to `bigint`.

### Float

When `ValueType` is `FLOAT32` or `FLOAT64`, rekuiper converts the value to `float`.

### String

When `ValueType` is `String`, rekuiper converts the value to `string`.

### Array Types

- `Bool` arrays convert to `boolean` arrays.
- `INT8`, `INT16`, `INT32`, `INT64`, `UINT`, `UINT8`, `UINT16`, `UINT32`, and `UINT64` arrays convert to `bigint` arrays.
- `FLOAT32` and `FLOAT64` arrays convert to `float` arrays.
