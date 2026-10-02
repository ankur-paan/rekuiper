# Memory Source Connector

<span style="background:green;color:white;padding:1px;margin:2px">stream source</span>
<span style="background:green;color:white;padding:1px;margin:2px">scan table source</span>
<span style="background:green;color:white;padding:1px;margin:2px">lookup table source</span>

The Memory source connector consumes events from internal in-memory topics published by the [Memory sink](../../sinks/builtin/memory.md).

This connector enables low-latency inter-rule communication without disk I/O or network serialization overhead. The memory connector requires no external configuration files.

The connector operates as a stream source, a scan table source, or a lookup table source.

## Create a Stream Source

As a [stream source](../../streams/overview.md), the connector subscribes to an in-memory topic and consumes streaming events in real time.

```sql
CREATE STREAM stream1 (
    name STRING,
    size BIGINT,
    id BIGINT
) WITH (DATASOURCE = "devices/result", FORMAT = "json", TYPE = "memory");
```

This stream consumes records published to the in-memory topic `devices/result`.

## Create a Scan Table Source

As a [scan table source](../../tables/scan.md), the connector retains historical in-memory records for join queries:

```sql
CREATE TABLE memoryTableDemo () WITH (DATASOURCE = "topicB", FORMAT = "JSON", TYPE = "memory");
```

## Create a Lookup Table Source

As a lookup table source, the connector supports on-demand key lookups for data enrichment during stream execution:

```sql
CREATE TABLE memoryLookupTableDemo () WITH (DATASOURCE = "topicC", FORMAT = "JSON", KEY = "id", TYPE = "memory");
```

Specify the `KEY` property to define the primary key column for the in-memory index.

### Characteristics of Memory Lookup Tables

- **Rule Independence**: Memory lookup tables exist independently of rule lifecycles. Modifying or deleting rules does not clear table state.
- **Shared Memory State**: Multiple rules that query the same topic and key pair access identical in-memory datasets.
- **Updatable Sink Integration**: Upstream rules update the lookup table dynamically using an [Updatable Memory Sink](../../sinks/builtin/memory.md#updatable-sink).
- **Inter-Rule Pipelining**: Upstream rules store intermediate state in memory topics, while downstream rules join with that state to make real-time decisions.

## Memory Topics and Wildcards

The `DATASOURCE` property defines the target in-memory topic path.

Memory topics support MQTT-style wildcards:

- `+` (Single-level wildcard): Matches exactly one topic level.
  - Example: `home/device1/+/sensor1` matches `home/device1/roomA/sensor1`, but does not match `home/device1/roomA/sub/sensor1`.
- `#` (Multi-level wildcard): Matches multiple topic levels. Place this wildcard only at the end of the topic string.
  - Example: `home/device1/#` matches `home/device1/temp` and `home/device1/roomA/sensor1`.

## Rule Pipelines with Memory Connectors

Use memory sources and sinks to assemble [Rule Pipelines](../../rules/rule_pipeline.md). Pipelines chain rules together: the output of one rule serves as the input to a subsequent rule.

When chaining rules through memory topics, the engine transfers data objects in memory without serializing to bytes. In this pipeline mode, the engine ignores the `FORMAT` property to optimize execution speed.
