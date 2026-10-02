# InfluxDB v2 Sink

The InfluxDB v2 sink publishes output messages to an InfluxDB v2.x database.

## Properties

### Connection Properties

| Property name | Optional | Description |
|---|---|---|
| addr | false | The network address of the InfluxDB server. |
| token | true | The authentication token used to access InfluxDB. |
| org | false | The InfluxDB organization name. |
| bucket | false | The target InfluxDB bucket name. |
| certificationPath | true | The certificate file path. Can be an absolute path or a relative path. For a relative path, the base path is the execution directory of the `kuiperd` command. For example, if you run `bin/kuiperd` from `/var/kuiper`, the base path is `/var/kuiper`. If you run `./kuiperd` from `/var/kuiper/bin`, the base path is `/var/kuiper/bin`. |
| privateKeyPath | true | The private key file path. Can be an absolute path or a relative path, same as `certificationPath`. |
| rootCaPath | true | The root CA file path. Can be an absolute path or a relative path, same as `certificationPath`. |
| tlsMinVersion | true | Specifies the minimum TLS protocol version negotiated with the client. Accepted values: `tls1.0`, `tls1.1`, `tls1.2`, and `tls1.3`. Default: `tls1.2`. |
| renegotiationSupport | true | Controls how the client handles server-initiated renegotiation requests. Supported values: `never`, `once`, or `freely`. Default: `never`. |
| insecureSkipVerify | true | If `true`, TLS accepts any certificate presented by the server and any host name in that certificate. In this mode, TLS is vulnerable to man-in-the-middle attacks. Default: `false`. Use only with TLS connections. |

### Write Options

| Property name | Optional | Description |
|---|---|---|
| measurement | false | The InfluxDB measurement name. |
| tags | true | Key-value tags to write, formatted as a JSON string such as `{"tag1":"value1"}`. Supports data template syntax such as <span v-pre>`{"tag1":"{{.temperature}}"}`</span>. |
| fields | true | Array of field names to write, such as `["field1", "field2"]`. If omitted, rekuiper writes all fields selected by the SQL query. |
| precision | true | The timestamp precision: `ns`, `us`, `ms`, or `s`. Default: `ms`. |
| tsFieldName | true | The field name containing the record timestamp. If set, rekuiper uses the value from this field. Ensure that the timestamp value matches the configured `precision`. If omitted, rekuiper uses the current timestamp. |
| useLineProtocol | true | Controls whether to use the [InfluxDB line protocol format](https://docs.influxdata.com/influxdb/v2/reference/syntax/line-protocol/). Default: `false`. When enabled, `dataTemplate` must generate valid line protocol syntax. |

Other common sink properties, including batch settings, are supported. Refer to [sink common properties](../overview.md#common-properties) for more information.

## Sample Usage

The following rule filters records where temperature exceeds 50 and writes the results to InfluxDB v2:

```json
{
  "id": "influx",
  "sql": "SELECT * from demo_stream where temperature > 50",
  "actions": [
    {
      "log": {},
      "influx2": {
        "addr": "http://192.168.100.245:8086",
        "token": "test_token",
        "org": "admin",
        "measurement": "test",
        "bucket": "bucketName",
        "tags": "{\"tag1\":\"value1\"}",
        "fields": ["humidity", "temperature", "pressure"]
      }
    }
  ]
}
```
