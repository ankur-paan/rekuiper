# Nop Sink Connector

The Nop sink connector discards output records without executing I/O operations. Use this sink for performance testing and throughput benchmarking.

When `log` is set to `true`, the connector writes output records to `$rekuiper_install/log/stream.log`.

## Configuration Properties

| Property Name | Optional | Description |
|---|---|---|
| `log` | True | Boolean. When `true`, prints output records to the log file. Default is `false`. |

The Nop sink supports all [common sink properties](../overview.md#common-properties).

## Example Configuration

```json
{
  "nop": {
    "log": false
  }
}
```
