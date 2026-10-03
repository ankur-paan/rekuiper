# SQL Source Connector

::: tip Status: Supported as Built-in Connector in rekuiper
rekuiper includes a built-in SQL source connector implemented directly in Rust with `sqlx`. You do not need to compile or deploy Go dynamic plugins (`.so` files).
:::

<span style="background:green;color:white;padding:1px;margin:2px">stream source</span>
<span style="background:green;color:white;padding:1px;margin:2px">lookup table source</span>

The SQL source connector queries relational databases periodically to ingest streaming records or perform on-demand lookup table joins.

## Supported Database Engines

rekuiper supports the following relational database engines natively:

- **PostgreSQL**: `postgres://username:password@hostname:5432/database` or `postgresql://...`
- **SQLite**: `sqlite:///path/to/database.db` or in-memory `sqlite::memory:`

## Configuration Properties

Configure database sources in `$rekuiper/etc/sources/sql.yaml` or directly in the stream definition properties:

| Property Name | Optional | Default Value | Description |
|---|---|---|---|
| `url` (or `dburl`) | False | None | Connection URL string for the target database. |
| `interval` | True | `10000` | Polling interval in milliseconds between periodic queries. |
| `internalSqlQueryCfg` | True | None | Structured query configuration with automatic index offsets. |
| `templateSqlQueryCfg` | True | None | Custom SQL query template with parameter interpolation. |

### Structured Query Configuration (`internalSqlQueryCfg`)

The `internalSqlQueryCfg` object generates polling queries automatically:

| Field Name | Type | Description |
|---|---|---|
| `table` | String | Target table name to query. |
| `limit` | Integer | Maximum number of rows returned per polling query. |
| `indexField` | String | Column name used as an offset watermark. |
| `indexValue` | Any | Initial index offset value. Subsequent queries update the offset with retrieved maximum values. |
| `indexFields` | Array | Array of composite index fields for multi-column ordering. |

When configured, rekuiper builds the following query structure automatically:

```sql
SELECT * FROM <table> WHERE <indexField> > <indexValue> ORDER BY <indexField> ASC LIMIT <limit>
```

### Template Query Configuration (`templateSqlQueryCfg`)

Use `templateSqlQueryCfg` when you need custom projection, custom joins, or specific filtering criteria:

| Field Name | Type | Description |
|---|---|---|
| `templateSql` | String | Raw SQL statement containing <code v-pre>{{.fieldName}}</code> parameter markers. |
| `indexField` | String | Column name supplying dynamic filter values. |
| `indexValue` | Any | Initial value for the index field marker. |
| `indexFields` | Array | Additional index columns for multi-field templates. |

rekuiper substitutes parameter markers safely with literal SQL representations before query execution.

::: note
If you configure both `templateSqlQueryCfg` and `internalSqlQueryCfg`, `templateSqlQueryCfg` takes precedence.
:::

## Stream Source Configuration Example

Create a streaming source that polls a table in PostgreSQL:

```yaml
# etc/sources/sql.yaml
pg_stream_config:
  url: "postgres://postgres:password@localhost:5432/telemetry"
  interval: 5000
  internalSqlQueryCfg:
    table: "device_metrics"
    limit: 100
    indexField: "id"
    indexValue: 0
```

Define the stream in rekuiper SQL:

```sql
CREATE STREAM deviceMetricsStream () WITH (
  TYPE = "sql",
  CONF_KEY = "pg_stream_config",
  FORMAT = "json"
);
```

## Lookup Table Source Example

Define a lookup table to enrich streaming events with reference data from SQLite:

```yaml
# etc/sources/sql.yaml
sqlite_lookup_config:
  url: "sqlite:///var/data/metadata.db"
  templateSqlQueryCfg:
    templateSql: "SELECT name, location, department FROM devices WHERE device_id = {{.deviceId}}"
    indexField: "deviceId"
    indexValue: ""
```

Define the lookup table and execute a join query:

```sql
CREATE TABLE deviceMetadata () WITH (
  TYPE = "sql",
  CONF_KEY = "sqlite_lookup_config",
  KIND = "lookup"
);

SELECT stream.deviceId, stream.temperature, meta.location, meta.department
FROM sensorStream AS stream
LEFT JOIN deviceMetadata AS meta ON stream.deviceId = meta.deviceId;
```
