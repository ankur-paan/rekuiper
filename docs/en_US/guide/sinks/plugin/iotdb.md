# IoTDB Sink

::: danger Status: Unsupported in rekuiper (Legacy eKuiper Go Plugin)
The native Thrift RPC `iotdb` sink was a Go dynamic plugin (`Iotdb.so`) in legacy eKuiper. **rekuiper is implemented in Rust and does not load Go dynamic plugins.**

To integrate with Apache IoTDB in rekuiper, use one of the supported production alternatives below:
- **[REST Sink](../builtin/rest.md)**: Send data directly to the Apache IoTDB REST API (`/api/v1/insertTablet` or `/api/v1/insertRecord`).
- **[MQTT Sink](../builtin/mqtt.md)**: Publish messages to an MQTT broker ingested by the Apache IoTDB MQTT service.
:::

## Overview

The legacy IoTDB sink used a Go Thrift RPC client to write tree-model or table-model records to an Apache IoTDB server.

## Recommended Alternative: REST Sink Integration

Apache IoTDB includes a built-in REST service. You can send processed records directly from rekuiper to IoTDB using the built-in [REST Sink](../builtin/rest.md).

### Example REST Sink Action

```json
{
  "id": "rule_iotdb_rest",
  "sql": "SELECT deviceId, temperature, humidity, ts FROM sensorStream WHERE temperature > 40.0",
  "actions": [
    {
      "rest": {
        "url": "http://127.0.0.1:18080/api/v1/insertRecord",
        "method": "POST",
        "headers": {
          "Content-Type": "application/json",
          "Authorization": "Basic cm9vdDpyb290"
        },
        "dataTemplate": "{\"device\":\"root.factory.{{.deviceId}}\",\"timestamps\":[{{.ts}}],\"measurements\":[\"temperature\",\"humidity\"],\"values\":[[{{.temperature}},{{.humidity}}]]}",
        "sendSingle": true
      }
    }
  ]
}
```

## Legacy Configuration Reference

For teams migrating legacy eKuiper rule configurations, the former properties are preserved below for reference:

| Property Name | Legacy Default | Description |
|---|---|---|
| `addr` | `127.0.0.1:6667` | IoTDB server address in `host:port` format. |
| `username` | `root` | Username for authentication. |
| `password` | `root` | Password for authentication. |
| `model` | `tree` | Data model: `tree` or `table`. |
| `device` | `""` | Device path for tree model (e.g. `root.sg1.dev1`). |
| `measurements` | `[]` | List of measurement or column names. |
| `dataTypes` | `[]` | Target data types (`INT32`, `INT64`, `FLOAT`, `DOUBLE`, `BOOLEAN`, `TEXT`, `STRING`, `TIMESTAMP`). |
| `tsFieldName` | `""` | Field containing the timestamp in milliseconds. |
| `batchSize` | `10` | Tablet batch size. |
