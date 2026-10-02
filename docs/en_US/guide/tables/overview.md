# Table Management

rekuiper streams are unbounded and immutable. The engine appends new records to the stream for real-time processing.

A **Table** represents the current state of a data stream or an external data store. A table functions as a data snapshot. Users query tables to access batch data or enrich real-time streaming records.

rekuiper supports two table types:

- **Scan table**: Buffers data in memory. Use scan tables for smaller datasets that do not require state sharing across rules.
- **Lookup table**: Binds to an external data store. Use lookup tables for larger datasets and to share state across multiple rules.

## Syntax

Table creation uses SQL syntax similar to stream definitions:

```sql
CREATE TABLE 
    table_name 
    ( column_name <data_type> [ ,...n ] )
    WITH ( property_name = expression [, ...] );
```

Tables support the same [data types](../streams/overview.md#schema-in-stream-definition) as streams.

Tables support all [stream properties](../streams/overview.md#stream-properties). Therefore, tables support all source connector types.

Many streaming sources produce one event at a time. By default, a table from such a source retains only the latest event. To retain historical events in a scan table, configure the `RETAIN_SIZE` property.

### Lookup Table Syntax

To create a lookup table, set the `KIND` property to `"lookup"`.

The following statement creates a lookup table connected to Redis database `0`:

```sql
CREATE TABLE alertTable() WITH (DATASOURCE="0", TYPE="redis", KIND="lookup");
```

Currently, only `memory`, `redis`, and `sql` sources support the `lookup` table kind.

### Table Properties

Configure table properties in the `WITH` clause:

| Property Name | Optional | Description |
|---|---|---|
| `DATASOURCE` | False | Target topic, table name, or database index. For MQTT, specify the topic name. |
| `FORMAT` | True | Serialization format: `"JSON"`, `"PROTOBUF"`, or `"BINARY"`. Default is `"JSON"`. Refer to [Binary Stream](../streams/overview.md#binary-streams). |
| `SCHEMAID` | True | Schema identifier used to decode payloads. Required when `FORMAT` is `"PROTOBUF"`. |
| `KEY` | True | Primary key of the table. In SQL sources, this property defines the primary key column. |
| `TYPE` | True | Source connector type (such as `redis`, `sql`, `memory`, or `mqtt`). Default is `"mqtt"`. |
| `CONF_KEY` | True | Configuration section in the source YAML file. Refer to [MQTT Source Connector](../sources/builtin/mqtt.md). |
| `KIND` | True | Table kind: `"scan"` or `"lookup"`. Default is `"scan"`. |
| `RETAIN_SIZE` | True | Number of historical records to retain in a scan table snapshot. |

## Usage Scenarios

Tables maintain state for stream-batch hybrid computations. Scan tables store state in memory, whereas lookup tables access external persistent storage.

Refer to the scenario guides for practical implementations:

- [Scan Table Scenarios](scan.md)
- [Lookup Table Scenarios](lookup.md)
