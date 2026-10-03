# InfluxDB Sink

::: danger Status: Unsupported in rekuiper (Legacy eKuiper Go Plugin)
The InfluxDB sink was implemented as a Go C-shared dynamic plugin (`.so`) in legacy eKuiper.

**rekuiper is written in Rust and does NOT support or load Go dynamic plugins (`.so`).** The native InfluxDB v1.x plugin is not implemented in rekuiper.

**Supported Alternatives in rekuiper**:
- **REST Sink (Direct HTTP API)**: Write points directly to the InfluxDB v1.x HTTP API (`/write?db=mydb`) using the built-in [REST Sink](../builtin/rest.md).
- **SQL Sink**: Store timeseries points in PostgreSQL (with TimescaleDB) or SQLite using the built-in [SQL Sink](../builtin/sql.md).
- **MQTT / Telegraf**: Publish telemetry via the built-in [MQTT Sink](../builtin/mqtt.md) to Telegraf, which writes to InfluxDB.
:::

## Direct HTTP Write Example with Built-in REST Sink

You can write data to InfluxDB v1.x without any plugins by using the built-in REST sink:

```json
{
  "id": "rule_influx_rest",
  "sql": "SELECT concat('cpu_usage,host=', host, ' value=', usage) AS line FROM sensor_stream",
  "actions": [
    {
      "rest": {
        "url": "http://influxdb.internal:8086/write?db=telemetry",
        "method": "POST",
        "dataTemplate": "{{.line}}",
        "sendSingle": true
      }
    }
  ]
}
```

---

## Overview (Legacy Reference Only)

In legacy eKuiper (Go), the InfluxDB sink wrote records directly to an InfluxDB v1.x database using Go client libraries. This section is preserved only as an architectural reference.

### Legacy Configuration Properties

| Property Name | Optional | Description |
| :--- | :--- | :--- |
| `addr` | False | Network address of the InfluxDB server (such as `http://127.0.0.1:8086`). |
| `database` | False | Target InfluxDB database name. |
| `measurement` | False | Target measurement name. |
| `username` | True | Authentication username. |
| `password` | True | Authentication password. |
| `tags` | True | JSON mapping of tag key-values. |
| `fields` | True | Array of field names to write. |
| `precision` | True | Timestamp precision (`ns`, `us`, `ms`, `s`). |
| `tsFieldName` | True | Field name containing the timestamp. |

### Legacy Rule Example

```json
{
  "id": "influx_legacy",
  "sql": "SELECT * FROM demo_stream WHERE temperature > 50",
  "actions": [
    {
      "influx": {
        "addr": "http://192.168.100.245:8086",
        "database": "databasename",
        "measurement": "test",
        "tags": "{\"tag1\":\"value1\"}",
        "fields": ["humidity", "temperature", "pressure"]
      }
    }
  ]
}
```
