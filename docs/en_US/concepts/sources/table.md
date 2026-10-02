# Tables

A table represents a dataset snapshot in rekuiper. rekuiper provides two table types: scan tables and lookup tables.

- **Scan Table**: Consumes streaming data as a changelog and updates table content continuously. Unlike static batch tables, scan table contents update over time. Stream sources such as MQTT, Kafka, or HTTP can function as scan table sources.
- **Lookup Table**: References an external data store. The engine queries specific values on demand instead of loading entire datasets into memory. Only sources with external query capability, such as SQL databases, can function as lookup tables.

## Scan Tables

The underlying source for a scan table can be bounded or unbounded. Bounded sources create static tables. Unbounded sources create dynamic tables whose records update in memory.

In rekuiper, scan table updates are append-only. Configure size limit properties to control memory usage.

A scan table cannot execute standalone in a rule. Rules join scan tables with streams to enrich telemetry or control calculation logic.

## Lookup Tables

Lookup tables reference external storage systems and do not store complete table datasets in memory.

Supported lookup sources include:

- **Memory Source**: Stores records as an in-memory table. This source converts streaming data into a lookup table.
- **Redis Source**: Queries records by Redis key.
- **SQL Source**: Queries external relational databases with SQL statements.

Lookup tables run independently from rules. All rules that reference a lookup table query the same underlying table data.

## Further Reading

- [Table Reference](../../sqls/tables.md)

