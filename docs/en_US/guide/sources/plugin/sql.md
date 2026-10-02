# SQL Source Connector

<span style="background:green;color:white;padding:1px;margin:2px">stream source</span>
<span style="background:green;color:white;padding:1px;margin:2px">scan table source</span>
<span style="background:green;color:white;padding:1px;margin:2px">lookup table source</span>

The SQL source connector queries relational databases periodically to ingest streaming data or to perform on-demand lookup joins.

## Supported Database Drivers

The SQL source supports these database engines:

- MySQL
- PostgreSQL
- SQLite
- Microsoft SQL Server (requires exposed port 1434)
- Oracle Database

## Configuration Overview

Configure database connections in `$rekuiper/etc/sources/sql.yaml`:

```yaml
default:
  interval: 10000
  url: mysql://user:test@140.210.204.147/user?parseTime=true
  internalSqlQueryCfg:
    table: test
    limit: 1
    indexField: registerTime
    indexValue: "2022-04-21 10:23:55"
    indexFieldType: "DATETIME"
    dateTimeFormat: "YYYY-MM-dd HH:mm:ss"

sqlserver_config:
  url: sqlserver://username:password@140.210.204.147/testdb
  internalSqlQueryCfg:
    table: Student
    limit: 10
    indexField: id
    indexValue: 1000

template_config:
  templateSqlQueryCfg:
    TemplateSql: "select * from table where entry_data > {{.entry_data}}"
    indexField: entry_data
    indexValue: "2022-04-13 06:22:32.233"
    indexFieldType: "DATETIME"
    dateTimeFormat: "YYYY-MM-dd HH:mm:ssSSS"
```

Verify database connectivity using the [Connectivity Check API](../../../api/restapi/connection.md#connectivity-check).

### Global Parameters

- `interval`: Polling interval in milliseconds between queries.
- `url`: Database connection URL.

| Database | URL Example |
|---|---|
| MySQL | `mysql://user:test@140.210.204.147/user?parseTime=true` |
| SQL Server | `sqlserver://username:password@140.210.204.147/testdb` |
| PostgreSQL | `postgres://user:pass@localhost/dbname` |
| SQLite | `sqlite:/path/to/file.db` |

### Query Generation via internalSqlQueryCfg

Configure structured query generation parameters:

- `table`: Target table name.
- `limit`: Maximum row count returned per query.
- `indexField`: Column name used as an offset index.
- `indexValue`: Initial index value. Subsequent queries update the offset with the maximum retrieved value.
- `indexFieldType`: Data type of `indexField`. Set to `"DATETIME"` for temporal columns.
- `dateTimeFormat`: Timestamp format string for datetime columns.
- `indexFields`: Array of multiple index columns for composite ordering.

#### Query Examples

| Table | Limit | Index Field | Initial Value | Index Type | Date Format | Generated SQL Statement |
|---|---|---|---|---|---|---|
| `Student` | 10 | (None) | (None) | (None) | (None) | `SELECT * FROM Student LIMIT 10` |
| `Student` | 10 | `stun` | `100` | (None) | (None) | `SELECT * FROM Student WHERE stun > 100 LIMIT 10` |
| `Student` | 10 | `registerTime` | `"2022-04-21 10:23:55"` | `"DATETIME"` | `"YYYY-MM-dd HH:mm:ss"` | `SELECT * FROM Student WHERE registerTime > '2022-04-21 10:23:55' ORDER BY registerTime ASC LIMIT 10` |

### Query Generation via templateSqlQueryCfg

Use `templateSqlQueryCfg` to define custom SQL query templates:

```yaml
template_config:
  templateSqlQueryCfg:
    TemplateSql: "SELECT * FROM Student WHERE stun > {{.stun}} LIMIT 10"
    indexField: stun
    indexValue: 100
```

> [!NOTE]
> Configure either `internalSqlQueryCfg` or `templateSqlQueryCfg`. If you configure both, `templateSqlQueryCfg` takes precedence.

## Create a Stream Source

Define a stream referencing the SQL configuration:

```sql
CREATE STREAM demo () WITH (
  DATASOURCE = "demo",
  FORMAT = "JSON",
  CONF_KEY = "template_config",
  TYPE = "sql"
);
```

## Create a Lookup Table Source

Define a lookup table to query database records on demand:

```sql
CREATE TABLE alertTable () WITH (
  DATASOURCE = "tableName",
  CONF_KEY = "sqlite_config",
  TYPE = "sql",
  KIND = "lookup"
);
```

### Lookup Cache Configuration

To reduce query latency, enable in-memory caching for lookup queries in `sql.yaml`:

```yaml
lookup:
  cache: true
  cacheTtl: 600
  cacheMissingKey: true
```

- `cache`: Boolean. Enables or disables the lookup query cache.
- `cacheTtl`: Cache entry time-to-live in seconds.
- `cacheMissingKey`: Boolean. When set to `true`, caches missing key lookups to prevent duplicate database queries.

### Query Pushdown with Template SQL

Push calculations down to the database engine by defining custom lookup query templates:

```yaml
sqlite3_lookup:
  url: example.db
  templateSqlQueryCfg:
    templateSql: "SELECT aid FROM t WHERE b2 + 1 = {{.bid}};"
```

The database executes the calculation <code v-pre>`b2 + 1 = {{.bid}}`</code> during the join query.
