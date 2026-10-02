# Random Source Connector

<span style="background:green;color:white;padding:1px;margin:2px">stream source</span>
<span style="background:green;color:white;padding:1px;margin:2px">scan table source</span>

The Random source connector generates mock random events following a defined schema pattern.

Use this connector for functional testing, rule validation, and performance benchmarking.

## Configuration Overview

Configure the connector in `$rekuiper/etc/sources/random.yaml`:

```yaml
default:
  interval: 1000
  seed: 1
  pattern:
    count: 50
  deduplicate: 0

ext:
  interval: 100

dedup:
  interval: 100
  deduplicate: 50
```

### Configuration Parameters

- `interval`: Generation interval in milliseconds between emitted events.
- `seed`: Maximum integer value generated for randomized fields.
- `pattern`: Schema template for generated records. In the example above, the connector generates records such as `{"count": 50}`.
- `deduplicate`: Controls duplicate filtering:
  - Positive integer $N$: Discards messages that duplicate any of the previous $N$ messages.
  - `0`: Disables deduplication checks.
  - Negative integer: Compares against all historical messages. Avoid negative values during long test runs to prevent high memory usage.

## Custom Configurations

Define custom configuration blocks in `random.yaml` to override global defaults:

```yaml
ext:
  interval: 100
```

Reference the configuration with `CONF_KEY="ext"` when creating a stream:

```sql
CREATE STREAM demo () WITH (
  DATASOURCE = "demo",
  FORMAT = "JSON",
  CONF_KEY = "ext",
  TYPE = "random"
);
```

For stream syntax and management details, refer to [Streams Management](../../streams/overview.md).
