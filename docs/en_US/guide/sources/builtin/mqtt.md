# MQTT Source Connector

<span style="background:green;color:white;padding:1px;margin:2px">stream source</span>
<span style="background:green;color:white;padding:1px;margin:2px">scan table source</span>

The MQTT source connector subscribes to messages from an MQTT broker and channels them into the rekuiper stream processing pipeline.

The MQTT connector functions as both a source connector and a [sink connector](../../sinks/builtin/mqtt.md). This document describes source connector configuration and usage.

## Configuration Overview

Configure the MQTT connector through [environment variables](../../../configuration/configuration.md#environment-variable-syntax), the [REST API](../../../api/restapi/configKey.md), or the configuration file.

The default configuration file resides at `$rekuiper/etc/mqtt_source.yaml`. Settings defined in the `default` section serve as global defaults. Custom configurations in separate sections override default values.

Example configuration file with `default` and `demo_conf` sections:

```yaml
# Global MQTT configurations
default:
  qos: 1
  server: "tcp://127.0.0.1:1883"
  #username: user1
  #password: password
  #certificationPath: /var/kuiper/xyz-certificate.pem
  #privateKeyPath: /var/kuiper/xyz-private.pem.key
  #rootCaPath: /var/kuiper/xyz-rootca.pem
  #insecureSkipVerify: true
  #connectionSelector: mqtt.mqtt_conf1
  #decompression: ""

# Override global configurations
demo_conf:
  qos: 0
  server: "tcp://10.211.55.6:1883"
```

## Global Configurations

Properties in the `default` section apply to all MQTT connections unless explicitly overridden.

### Connection Parameters

- `qos`: Default subscription QoS level (`0`, `1`, or `2`). Default is `1`.
- `server`: Target MQTT broker URL.
- `username`: Username credential for broker authentication.
- `password`: Password credential for broker authentication.
- `protocolVersion`: MQTT protocol version: `3.1` (MQTT 3), `3.1.1` (MQTT 4), or `5` (MQTT 5). Default is `3.1`.
- `clientid`: Client identifier for the connection. If omitted, the engine generates a random UUID.

### Security and TLS Parameters

- `certificationPath`: Path to the client certificate file (for example, `d3807d9fa5-certificate.pem`). Can be absolute or relative to the execution root directory.
- `privateKeyPath`: Path to the client private key file (for example, `d3807d9fa5-private.pem.key`).
- `rootCaPath`: Path to the Root CA certificate file.
- `certficationRaw`: Base64-encoded client certificate text. The engine prefers `certificationPath` if both are defined.
- `privateKeyRaw`: Base64-encoded client private key text. The engine prefers `privateKeyPath` if both are defined.
- `rootCARaw`: Base64-encoded Root CA certificate text. The engine prefers `rootCaPath` if both are defined.
- `insecureSkipVerify`: Boolean. Set to `true` to skip certificate and hostname validation.

For mTLS procedures and secret handling, refer to the [Secure MQTT with TLS Guide](../../connectors/mqtt_tls.md).

### Connection Reuse

- `connectionSelector`: Specifies a named connection resource from `connections/connection.yaml` (for example, `mqtt.localConnection`). For details, refer to [Connection Management](../../connections/overview.md).

```yaml
default:
  qos: 1
  server: "tcp://127.0.0.1:1883"
  connectionSelector: mqtt.localConnection
```

> [!NOTE]
> When `connectionSelector` is configured in a configuration group, the engine ignores broker connection parameters (such as `server`) defined in that group.

Verify broker reachability before runtime using the [Connectivity Check API](../../../api/restapi/connection.md#connectivity-check).

### Payload Handling

- `decompression`: Decompresses incoming binary payloads before parsing. Supported algorithms: `"gzip"` and `"zstd"`.
- `bufferLength`: Maximum number of messages buffered in memory to prevent out-of-memory errors. Default is `102400`.

### KubeEdge Integration

- `kubeedgeVersion`: KubeEdge version number.
- `kubeedgeModelFile`: KubeEdge model template filename located in `etc/sources/`.

Example model file:

```yaml
{
  "deviceModels": [{
    "name": "device1",
    "properties": [{
      "name": "temperature",
      "dataType": "int"
    }, {
      "name": "temperature-enable",
      "dataType": "string"
    }]
  }]
}
```

- `deviceModels.name`: Device name matched against the third and fourth segments of topic `$ke/events/device/device1/data/update`.
- `properties.name`: Property field name.
- `properties.dataType`: Expected property data type.

## Custom Configurations

Define custom configuration sections in `etc/mqtt_source.yaml` to specify connection settings for distinct brokers or topics:

```yaml
demo_conf:
  qos: 0
  server: "tcp://10.211.55.6:1883"
```

To apply this configuration, specify `CONF_KEY="demo_conf"` in the stream definition:

```sql
CREATE STREAM demo () WITH (DATASOURCE="test/", FORMAT="JSON", KEY="USERID", CONF_KEY="demo_conf");
```

Properties in `demo_conf` override corresponding values in the `default` section.

## Create a Stream Source

The MQTT connector operates as a [stream source](../../streams/overview.md) or as a [scan table source](../../tables/scan.md).

### Create Stream via REST API

Send a `POST` request to `/streams`:

```json
{
  "sql": "CREATE STREAM my_stream (id bigint, name string, score float) WITH (DATASOURCE = \"topic/temperature\", FORMAT = \"json\", KEY = \"id\")"
}
```

For REST API specifications, refer to [Streams Management with REST API](../../../api/restapi/streams.md).

### Create Stream via CLI

Run the `kuiper create stream` command:

```bash
bin/kuiper create stream my_stream '(id bigint, name string, score float) WITH (DATASOURCE = "topic/temperature", FORMAT = "json", KEY = "id")'
```

For CLI command syntax, refer to [Streams Management with CLI](../../../api/cli/streams.md).

## MQTT v5 User Properties

When `protocolVersion` is set to `5`, rekuiper exposes incoming MQTT v5 `User Properties` in record metadata under the `properties` key as a map of strings (`map[string]string`).

Access user properties in SQL queries using the `meta` function:

```sql
SELECT meta(properties) AS props FROM demo;
```

## Migration Notes

Starting with version 1.5.0, the MQTT source configuration parameter changed from `servers` (array) to `server` (single URL string).

- When upgrading to 1.5.0 or later, verify that `server` is configured in `etc/mqtt_source.yaml`.
- When using environment variable overrides, replace `MQTT_SOURCE__DEFAULT__SERVERS=[tcp://127.0.0.1:1883]` with `MQTT_SOURCE__DEFAULT__SERVER="tcp://127.0.0.1:1883"`.

## Listen to Multiple Topics

To subscribe to multiple MQTT topics within a single stream, specify a comma-separated list in the `DATASOURCE` property:

```sql
CREATE STREAM my_stream (id bigint, name string, score float)
WITH (DATASOURCE = "t1,t2", FORMAT = "json", KEY = "id");
```
