# Log Sink Connector

The Log sink connector writes output messages to the engine log file for diagnostic debugging.

By default, the engine writes messages to `$rekuiper_install/log/stream.log`.

The Log sink supports all [common sink properties](../overview.md#common-properties).

## Example Configuration

```json
{
  "log": {}
}
```
