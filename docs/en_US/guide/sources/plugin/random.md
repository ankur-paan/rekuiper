# Random Source Connector

::: warning Status: Replaced by Built-in Simulator Source in rekuiper
The legacy `random` source plugin is replaced by the native **[Simulator Source Connector](../builtin/simulator.md)** in rekuiper.

rekuiper includes a built-in Simulator source that generates synthetic sensor telemetry with configurable intervals, data payloads, and repetition loops without external plugins.
:::

## Overview (Legacy Reference Only)

In legacy eKuiper (Go), the Random source plugin generated synthetic mock records based on a pattern configuration. This page is preserved only as a reference for existing rules.

## Recommended Migration: Use the Simulator Source

To generate mock data in rekuiper, use the built-in `simulator` connector.

### Define a Simulated Stream in rekuiper

```sql
CREATE STREAM mock_sensor_stream () WITH (
  TYPE = "simulator"
);
```

Configure mock payloads and emission intervals in `$rekuiper/etc/sources/simulator.yaml`:

```yaml
default:
  data:
    - temperature: 22.5
      humidity: 50.2
    - temperature: 25.1
      humidity: 48.9
  interval: 500
  loop: true
```

For full details, refer to the [Simulator Source Connector](../builtin/simulator.md) documentation.

---

## Legacy Configuration (eKuiper Go)

In legacy eKuiper, configuration was defined in `etc/sources/random.yaml`:

```yaml
default:
  interval: 1000
  seed: 1
  pattern:
    count: 50
  deduplicate: 0
```

### Legacy Stream Definition

```sql
CREATE STREAM demo () WITH (
  DATASOURCE = "demo",
  FORMAT = "JSON",
  TYPE = "random"
);
```
