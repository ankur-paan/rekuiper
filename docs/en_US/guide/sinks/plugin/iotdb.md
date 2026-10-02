# IoTDB Sink

The IoTDB sink writes data into Apache IoTDB by using the native Thrift RPC client. It supports both the tree model and the table model.

## Compile the Plugin

In the rekuiper source code root directory, run the following command:

```shell
go build -trimpath --buildmode=plugin -o plugins/sinks/Iotdb.so extensions/sinks/iotdb/*.go
```

## Properties

### Connection Properties

| Property name | Optional | Default value | Description |
|---|---|---|---|
| addr | false | `127.0.0.1:6667` | IoTDB server address in `host:port` format. |
| username | true | `root` | The username for authentication. |
| password | true | `root` | The password for authentication. |
| nodeUrls | true | `[]` | Cluster node URLs. When set, overrides the `addr` property. |
| timeout | true | `5000` | Connection timeout in milliseconds. |
| poolSize | true | `3` | The size of the connection pool. |

### Model Selection

| Property name | Optional | Default value | Description |
|---|---|---|---|
| model | false | `tree` | The data model to use: `tree` or `table`. |

### Tree Model Properties

The following properties apply when `model` is set to `tree`:

| Property name | Optional | Default value | Description |
|---|---|---|---|
| device | true | `""` | The device path, such as `root.sg1.dev1`. Required for the tree model. |
| isAligned | true | `false` | Controls whether to use aligned time series. |

### Table Model Properties

The following properties apply when `model` is set to `table`:

| Property name | Optional | Default value | Description |
|---|---|---|---|
| database | true | `""` | The database name. The `root.` prefix is removed automatically. Required for the table model. |
| table | true | `""` | The target table name. Required for the table model. |
| columnCategories | true | `[]` | Column categories mapped one-to-one to `measurements`: `TAG`, `FIELD`, or `ATTRIBUTE`. Required for the table model. |

### Data Mapping Properties

| Property name | Optional | Default value | Description |
|---|---|---|---|
| measurements | false | `[]` | List of measurement or column names. |
| dataTypes | false | `[]` | IoTDB data types mapped one-to-one to `measurements`: `INT32`, `INT64`, `FLOAT`, `DOUBLE`, `BOOLEAN`, `TEXT`, `STRING`, or `TIMESTAMP`. |
| tsFieldName | true | `""` | Field name containing the timestamp in milliseconds. If omitted, rekuiper uses the current time. When set, every record must contain this field. |
| batchSize | true | `10` | The number of rows written per tablet batch. |

Other common sink properties, including batch settings, are supported. Refer to [sink common properties](../overview.md#common-properties) for more information.

## Sample Usage

### Tree Model Example

The following rule selects temperature values greater than 50 and writes data to IoTDB using the tree model:

```json
{
  "id": "iotdb_tree",
  "sql": "SELECT * from demo_stream where temperature > 50",
  "actions": [
    {
      "log": {},
      "iotdb": {
        "addr": "127.0.0.1:6667",
        "username": "root",
        "password": "root",
        "model": "tree",
        "device": "root.sg1.d1",
        "measurements": ["temperature", "humidity"],
        "dataTypes": ["FLOAT", "FLOAT"],
        "tsFieldName": "ts",
        "batchSize": 10
      }
    }
  ]
}
```

### Table Model Example

The following rule selects temperature values greater than 50 and writes data to IoTDB using the table model:

```json
{
  "id": "iotdb_table",
  "sql": "SELECT * from demo_stream where temperature > 50",
  "actions": [
    {
      "log": {},
      "iotdb": {
        "addr": "127.0.0.1:6667",
        "username": "root",
        "password": "root",
        "model": "table",
        "database": "iot_data",
        "table": "sensor_data",
        "measurements": ["device_id", "temperature", "humidity"],
        "dataTypes": ["STRING", "FLOAT", "FLOAT"],
        "columnCategories": ["TAG", "FIELD", "FIELD"],
        "tsFieldName": "ts",
        "batchSize": 10
      }
    }
  ]
}
```

## Data Types

The following IoTDB data types are supported in the `dataTypes` property:

| Data type | Description |
|---|---|
| INT32 | 32-bit signed integer |
| INT64 | 64-bit signed integer |
| FLOAT | Single-precision floating point |
| DOUBLE | Double-precision floating point |
| BOOLEAN | Boolean value (`true` or `false`) |
| TEXT | Text string (legacy IoTDB type) |
| STRING | String value |
| TIMESTAMP | Timestamp value in milliseconds |

## Notes

- Apache IoTDB uses the Thrift RPC protocol on TCP port 6667 by default.
- For the table model, rekuiper automatically creates the database if it does not exist.
- The `root.` prefix in the `database` property is stripped automatically in the table model.
- Missing record fields are written to IoTDB as null values.
