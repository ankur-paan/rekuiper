# TDengine 3 Sink

::: danger Status: Unsupported in rekuiper (Legacy eKuiper Go Plugin)
The native `tdengine3` sink was a Cgo/Go dynamic plugin (`Tdengine3.so`) in legacy eKuiper that required the TDengine C client library (`libtaos.so`). **rekuiper is implemented in Rust and does not load Go dynamic plugins.**

To write data into TDengine from rekuiper, use the supported alternative below:
- **[REST Sink](../builtin/rest.md)**: Send SQL insert statements directly to the TDengine RESTful connector API endpoint (`POST /rest/sql`).
:::

## Overview

The legacy TDengine sink used Cgo bindings to invoke the native client library for data ingestion into regular tables or super tables.

## Recommended Alternative: TDengine REST Connector

TDengine provides a built-in RESTful service running on port `6041`. You can post standard SQL statements directly using rekuiper's built-in [REST Sink](../builtin/rest.md).

### Example REST Sink Action for TDengine

The following rule formats an `INSERT` statement and submits it to TDengine via HTTP:

```json
{
  "id": "rule_tdengine_rest",
  "sql": "SELECT deviceId, temperature, humidity, ts FROM sensorStream",
  "actions": [
    {
      "rest": {
        "url": "http://127.0.0.1:6041/rest/sql",
        "method": "POST",
        "headers": {
          "Authorization": "Basic cm9vdDp0YW9zZGF0YQ==",
          "Content-Type": "text/plain"
        },
        "dataTemplate": "INSERT INTO test_db.meters USING test_db.meters_st TAGS('{{.deviceId}}') VALUES ({{.ts}}, {{.temperature}}, {{.humidity}})",
        "sendSingle": true
      }
    }
  ]
}
```

## Legacy Configuration Reference

For teams migrating legacy eKuiper rule configurations, the former properties are preserved below for reference:

| Property Name | Type | Description |
|---|---|---|
| `host` | String | TDengine server hostname or IP address (default: `localhost`). |
| `port` | Integer | TDengine server port (default: `6041`). |
| `user` | String | Database username (default: `root`). |
| `password` | String | Database password (default: `taosdata`). |
| `database` | String | Target database name. |
| `table` | String | Target table name. |
| `fields` | Array | Field names to insert. |
| `provideTs` | Boolean | Whether the record provides an explicit timestamp column. |
| `tsFieldName` | String | Name of the timestamp column. |
| `sTable` | String | Target super table name. |
| `tagFields` | Array | Fields mapped to super table tag columns. |
