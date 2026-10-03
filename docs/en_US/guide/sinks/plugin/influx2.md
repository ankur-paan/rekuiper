# InfluxDB v2 Sink

::: danger Status: Unsupported in rekuiper (Legacy eKuiper Go Plugin)
The InfluxDB v2 sink was implemented as a Go C-shared dynamic plugin (`.so`) in legacy eKuiper.

**rekuiper is written in Rust and does NOT support or load Go dynamic plugins (`.so`).** The native InfluxDB v2 plugin is not implemented in rekuiper.

**Supported Alternatives in rekuiper**:
- **REST Sink (Direct InfluxDB v2 API)**: Post line protocol data directly to `/api/v2/write?org={org}&bucket={bucket}` with an authorization header using the built-in [REST Sink](../builtin/rest.md).
- **SQL Sink**: Store timeseries points in PostgreSQL (with TimescaleDB) or SQLite using the built-in [SQL Sink](../builtin/sql.md).
:::

## Direct HTTP Write Example with Built-in REST Sink

You can publish data to InfluxDB v2 without any plugins by using the built-in REST sink:

```json
{
  "id": "rule_influx2_rest",
  "sql": "SELECT concat('air_quality,location=', location, ' co2=', co2, ',temp=', temp) AS line FROM sensor_stream",
  "actions": [
    {
      "rest": {
        "url": "http://influxdb.internal:8086/api/v2/write?org=my-org&bucket=iot-metrics&precision=ms",
        "method": "POST",
        "headers": {
          "Authorization": "Token your-influx-api-token",
          "Content-Type": "text/plain; charset=utf-8"
        },
        "dataTemplate": "{{.line}}",
        "sendSingle": true
      }
    }
  ]
}
```

---

## Overview (Legacy Reference Only)

In legacy eKuiper (Go), the InfluxDB v2 sink wrote records directly to InfluxDB v2.x using Go client libraries. This section is preserved only as an architectural reference.

### Legacy Configuration Properties

| Property Name | Optional | Description |
| :--- | :--- | :--- |
| `addr` | False | Network address of the InfluxDB v2 server (such as `http://127.0.0.1:8086`). |
| `org` | False | Target InfluxDB organization name. |
| `bucket` | False | Target InfluxDB bucket name. |
| `token` | True | API token for InfluxDB v2 authentication. |
| `measurement` | False | Target measurement name. |
| `tags` | True | JSON mapping of tag key-values. |
| `fields` | True | Array of field names to write. |
| `precision` | True | Timestamp precision (`ns`, `us`, `ms`, `s`). |
| `tsFieldName` | True | Field name containing the timestamp. |

### Legacy Rule Example

```json
{
  "id": "influx2_legacy",
  "sql": "SELECT * FROM demo_stream WHERE temperature > 50",
  "actions": [
    {
      "influx2": {
        "addr": "http://192.168.100.245:8086",
        "org": "admin",
        "bucket": "bucketName",
        "token": "my-api-token",
        "measurement": "test",
        "tags": "{\"tag1\":\"value1\"}",
        "fields": ["humidity", "temperature", "pressure"]
      }
    }
  ]
}
```
