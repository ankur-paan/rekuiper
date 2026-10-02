# SQL Sink

The SQL sink writes query results to a relational database.

## Compile and Deploy the Plugin

This plugin must be compiled with the required database driver. Build tags specify which drivers to include.

The plugin supports `sqlserver`, `postgres`, `mysql`, `sqlite3`, and `oracle` drivers by default. You can compile the plugin with a single driver by using build tags.

When using Microsoft SQL Server as the target, make sure that SQL Server exposes its TCP port.

### Default Build Command

```shell
cd $rekuiper_src
go build -trimpath --buildmode=plugin -o plugins/sinks/Sql.so extensions/sinks/sql/sql.go
cp plugins/sinks/Sql.so $rekuiper_install/plugins/sinks
```

### MySQL Build Command

```shell
cd $rekuiper_src
go build -trimpath --buildmode=plugin -tags mysql -o plugins/sinks/Sql.so extensions/sinks/sql/sql.go
cp plugins/sinks/Sql.so $rekuiper_install/plugins/sinks
```

Restart the rekuiper server to activate the plugin.

## Properties

| Property name | Optional | Description |
|---|---|---|
| url | false | The connection URL for the target database. |
| table | false | The target table name for the result records. |
| fields | true | The column names to insert. Both the result record and the database table must contain these fields. If omitted, rekuiper inserts all fields from the result record. |
| tableDataField | true | Writes nested array records from this field into the database. |
| rowkindField | true | Specifies the field that indicates the row operation (such as `insert` or `update`). If omitted, all rows default to `insert`. |
| keyField | true | Specifies the primary key column for update and delete operations. |

Other common sink properties are supported. Refer to [sink common properties](../overview.md#common-properties) for more information.

You can verify the connectivity of the sink endpoint before rule execution by using the REST API: [Connectivity Check](../../../api/restapi/connection.md#connectivity-check).

### Dynamic Field Names

When `fields` is not configured, the SQL sink derives column names from keys in the result record (using the first row in a batch). Each derived name must match `[A-Za-z_][A-Za-z0-9_]*`: it must start with an ASCII letter or underscore and contain only ASCII letters, numbers, or underscores. This rule also applies to the field specified by `rowkindField`.

If a derived name does not match this format, rekuiper rejects the write operation before executing the SQL statement. The sink does not silently drop or quote invalid column names.

Explicitly configured values for `table`, `fields`, and `keyField` are passed directly to generated SQL statements. Each configured entry in `fields` must match the map key exactly.

## Sample Usage

The following sample queries data from a stream and inserts records into a MySQL database:

```json
{
  "id": "rule",
  "sql": "SELECT stuno as id, stuName as name, format_time(entry_data,\"YYYY-MM-dd HH:mm:ss\") as registerTime FROM SqlServerStream",
  "actions": [
    {
      "log": {},
      "sql": {
        "url": "mysql://user:test@140.210.204.147/user?parseTime=true",
        "table": "test",
        "fields": ["id", "name", "registerTime"]
      }
    }
  ]
}
```

### Write Nested Array Fields

To write nested records from an array field into the database, configure `tableDataField`:

Incoming payload:

```json
{
  "telemetry": [
    {
      "temperature": 32.32,
      "humidity": 80.8,
      "ts": 1388082430
    },
    {
      "temperature": 34.32,
      "humidity": 81.8,
      "ts": 1388082440
    }
  ]
}
```

Rule definition:

```json
{
  "id": "rule",
  "sql": "SELECT telemetry FROM dataStream",
  "actions": [
    {
      "log": {},
      "sql": {
        "url": "mysql://user:test@140.210.204.147/user?parseTime=true",
        "table": "test",
        "fields": ["temperature", "humidity"],
        "tableDataField": "telemetry"
      }
    }
  ]
}
```

### Update Sample

Configure `rowkindField` and `keyField` to execute insert, update, or delete operations based on primary key values:

```json
{
  "id": "ruleUpdateAlert",
  "sql": "SELECT * FROM alertStream",
  "actions": [
    {
      "sql": {
        "url": "sqlite://test.db",
        "keyField": "id",
        "rowkindField": "action",
        "table": "alertTable",
        "sendSingle": true
      }
    }
  ]
}
```
