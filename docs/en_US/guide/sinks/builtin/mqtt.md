# MQTT Action

The MQTT action publishes output messages to an MQTT broker.

## Properties

| Property name | Optional | Description |
|---|---|---|
| server | false | The broker address of the MQTT server, such as `tcp://127.0.0.1:1883`. |
| topic | false | The MQTT topic, such as `analysis/result`. |
| clientId | true | The client identifier for the MQTT connection. If not specified, rekuiper generates a UUID. |
| protocolVersion | true | The MQTT protocol version: `3.1` (MQTT 3) or `3.1.1` (MQTT 4). Default: `3.1`. |
| qos | true | The Quality of Service level for message delivery. Valid values are `0`, `1`, or `2`. |
| username | true | The username for the connection. |
| password | true | The password for the connection. |
| certificationPath | true | The certificate file path. Can be an absolute path or a relative path. For a relative path, the base path is the execution directory of the `kuiperd` command. For example, if you run `bin/kuiperd` from `/var/kuiper`, the base path is `/var/kuiper`. If you run `./kuiperd` from `/var/kuiper/bin`, the base path is `/var/kuiper/bin`. |
| privateKeyPath | true | The private key file path. Can be an absolute path or a relative path, same as `certificationPath`. |
| rootCaPath | true | The root CA file path. Can be an absolute path or a relative path, same as `certificationPath`. |
| certficationRaw | true | Base64-encoded raw text of the certificate. rekuiper uses `certificationPath` first if you define both. |
| privateKeyRaw | true | Base64-encoded raw text of the private key. rekuiper uses `privateKeyPath` first if you define both. |
| rootCARaw | true | Base64-encoded raw text of the root CA certificate. rekuiper uses `rootCaPath` first if you define both. |
| tlsMinVersion | true | Specifies the minimum TLS protocol version negotiated with the client. Accepted values: `tls1.0`, `tls1.1`, `tls1.2`, and `tls1.3`. Default: `tls1.2`. |
| renegotiationSupport | true | Controls how the client handles server-initiated renegotiation requests. Supported values: `never`, `once`, or `freely`. Default: `never`. |
| insecureSkipVerify | true | If `true`, TLS accepts any certificate from the server and any host name in that certificate. In this mode, TLS is vulnerable to man-in-the-middle attacks. Default: `false`. Use only with TLS connections. |
| retained | true | If `true`, the broker stores the last retained message and its QoS for that topic. Default: `false`. |
| compression | true | Compresses the payload with the specified method. Supported methods: `zlib`, `gzip`, `flate`, and `zstd`. |
| connectionSelector | true | Reuses a shared MQTT broker connection. See [Connection selector](../../sources/builtin/mqtt.md#connectionselector). |

Other common sink properties are supported. Refer to [sink common properties](../overview.md#common-properties) for more information.

For detailed TLS and mTLS setup, certificate formats, and secret handling, refer to the [Secure MQTT with TLS Guide](../../connectors/mqtt_tls.md).

The following sample configuration connects to Azure IoT Hub with SAS authentication:

```json
{
  "mqtt": {
    "server": "ssl://xyz.azure-devices.net:8883",
    "topic": "devices/demo_001/messages/events/",
    "protocolVersion": "3.1.1",
    "qos": 1,
    "clientId": "demo_001",
    "username": "xyz.azure-devices.net/demo_001/?api-version=2018-06-30",
    "password": "SharedAccessSignature sr=*******************",
    "retained": false
  }
}
```

The following sample configuration connects to AWS IoT with certificate and private key authentication:

```json
{
  "mqtt": {
    "server": "ssl://xyz-ats.iot.us-east-1.amazonaws.com:8883",
    "topic": "devices/result",
    "qos": 1,
    "clientId": "demo_001",
    "certificationPath": "keys/d3807d9fa5-certificate.pem",
    "privateKeyPath": "keys/d3807d9fa5-private.pem.key",
    "insecureSkipVerify": false,
    "retained": false
  }
}
```

You can verify the connectivity of the sink endpoint before rule execution by using the REST API: [Connectivity Check](../../../api/restapi/connection.md#connectivity-check).

## Dynamic Topic

When the result data contains the topic name, use it in the MQTT action to support dynamic topics.

If the selected data contains a field named `mytopic`, use data template syntax in the `topic` property:

```json
{
  "mqtt": {
    "server": "ssl://xyz-ats.iot.us-east-1.amazonaws.com:8883",
    "topic": "{{.mytopic}}",
    "qos": 1,
    "clientId": "demo_001",
    "certificationPath": "keys/d3807d9fa5-certificate.pem",
    "privateKeyPath": "keys/d3807d9fa5-private.pem.key",
    "retained": false
  }
}
```
