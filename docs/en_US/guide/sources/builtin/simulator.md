# Simulator Source Connector

<span style="background:green;color:white;padding:1px;margin:2px">stream source</span>
<span style="background:green;color:white;padding:1px;margin:2px">scan table source</span>

The Simulator source connector generates synthetic telemetry data for functional testing and demonstration rules.

The connector simulates streams of sensor readings according to defined payloads, generation intervals, and repetition loops.

## Configuration Overview

Configure the simulator connector through [environment variables](../../../configuration/configuration.md#environment-variable-syntax), the [REST API](../../../api/restapi/configKey.md), or the configuration file.

The default configuration file resides at `$rekuiper/etc/sources/simulator.yaml`:

```yaml
default:
  data:
    - temperature: 22.5
      humidity: 50
  interval: 10
  loop: true
```

### Configuration Parameters

- `data`: Mock payload definition as a YAML mapping or an array of mappings. The connector emits records in the list sequentially.
- `interval`: Emission interval in milliseconds between generated events.
- `loop`: Boolean. When set to `true`, the connector loops through the data array continuously. When set to `false`, the connector stops after emitting all records in the list.

## Create a Stream Source

The Simulator connector operates as a [stream source](../../streams/overview.md) or as a [scan table source](../../tables/scan.md).

### Create Stream via REST API

Send a `POST` request to `/streams`:

```json
{
  "sql": "CREATE STREAM mock_stream () WITH (TYPE = \"simulator\")"
}
```

For REST API specifications, refer to [Streams Management with REST API](../../../api/restapi/streams.md).

### Create Stream via CLI

Run the `kuiper create stream` command:

```bash
bin/kuiper create stream mock_stream '() WITH (TYPE = "simulator")'
```

For CLI command syntax, refer to [Streams Management with CLI](../../../api/cli/streams.md).
