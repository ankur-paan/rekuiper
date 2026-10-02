# TDengine 3 Sink

The TDengine 3 sink writes query results to a TDengine database.

## Compile the Plugin

In the rekuiper source code root directory, run the following command:

```shell
go build -trimpath --buildmode=plugin -o plugins/sinks/Tdengine3.so extensions/sinks/tdengine3/*.go
```

Restart the rekuiper server to activate the plugin.

## Action Configuration

TDengine requires a timestamp column in every table. You must specify the timestamp field name in `tsFieldName`. If your data contains timestamp values, set `provideTs` to `true`. If `provideTs` is `false`, TDengine generates the timestamp automatically.

| Property name | Type | Optional | Description |
|---|---|---|---|
| host | string | false | Database server hostname or IP address. Default: `localhost`. |
| port | int | false | Database server port. Default: `6041`. |
| user | string | false | Database username. Default: `root`. |
| password | string | false | Database password. Default: `taosdata`. |
| database | string | true | Target database name. |
| table | string | true | Target table name. Supports [dynamic properties](../overview.md#dynamic-properties). |
| fields | []string | false | Array of fields to insert. Both the result record and the database table must contain these fields. |
| provideTs | bool | false | Controls whether the record provides a timestamp field value. Default: `false`. |
| tsFieldName | string | true | Name of the timestamp column in the table. |
| sTable | string | false | Name of the super table. Supports [dynamic properties](../overview.md#dynamic-properties). |
| tagFields | []string | false | Result fields used as tag values in order. Required when `sTable` is specified. |

Other common sink properties are supported. Refer to [sink common properties](../overview.md#common-properties) for more information.

## Sample Usage

### Create a Stream

```bash
curl --location --request POST 'http://127.0.0.1:9081/streams' \
  --header 'Content-Type:application/json' \
  --data '{"sql":"create stream demoStream(time string, age BIGINT) WITH ( DATASOURCE = \"device/+/message\", FORMAT = \"json\");"}'
```

### Create a Rule

```bash
curl --location --request POST 'http://127.0.0.1:9081/rules' \
  --header 'Content-Type:application/json' \
  --data '{"id":"demoRule","sql":"SELECT * FROM demoStream;","actions":[{"tdengine3":{"provideTs":true,"tsFieldName":"time","user":"root","password":"taosdata","database":"dbName","table":"tableName","fields":["time","age"]}}]}'
```

### Write to a Fixed Table

```json
{
  "tdengine3": {
    "host": "127.0.0.1",
    "port": 6041,
    "user": "root",
    "password": "taosdata",
    "database": "db",
    "table": "table1",
    "tsFieldName": "ts"
  }
}
```

### Write to a Dynamic Table

```json
{
  "tdengine3": {
    "sendSingle": true,
    "host": "hostname",
    "port": 6041,
    "user": "root",
    "password": "taosdata",
    "database": "db",
    "table": "{{.tName}}",
    "tsFieldName": "ts"
  }
}
```
