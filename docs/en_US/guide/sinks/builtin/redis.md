# Redis Sink

The Redis sink publishes query results to a Redis database.

## Properties

| Property name | Optional | Description |
|---|---|---|
| addr | false | The network address of the Redis server, such as `10.122.48.17:6379`. |
| password | true | The Redis authentication password. |
| db | false | The Redis database index, such as `0`. |
| key | false | The Redis key used when `keyType` is `single`. If both `key` and `field` are set, `field` takes precedence. |
| field | true | The payload field name whose value becomes the Redis key. Applies only when `keyType` is `single`. For example, if `field` is `"deviceName"` and the record is `{"deviceName":"abc"}`, the Redis key is `"abc"`. Do not use data template syntax for this property. |
| keyType | true | Controls how data is saved to Redis: `single` or `multiple`. Default: `single`. In `single` mode, all fields are serialized to a single JSON value. In `multiple` mode, each field is saved as an individual key-value pair. |
| dataType | false | The Redis data type: `string` or `list`. Default: `string`. Delete the existing key before changing the data type. |
| expiration | false | Expiration time for Redis keys in seconds. This parameter applies only to `string` data. Default: `-1` (no expiration). |
| rowkindField | true | The payload field that specifies the row operation (such as `insert` or `update`). If omitted, all rows default to `insert`. |

Other common sink properties are supported. Refer to [sink common properties](../overview.md#common-properties) for more information.

## Sample Usage

The following sample rule filters temperature values greater than 50 and writes the results to Redis:

### /tmp/redisRule.txt

```json
{
  "id": "redis",
  "sql": "SELECT * from demo_stream where temperature > 50",
  "actions": [
    {
      "log": {},
      "redis": {
        "addr": "10.122.48.17:6379",
        "password": "123456",
        "db": 1,
        "dataType": "string",
        "expire": "10000",
        "field": "temperature"
      }
    }
  ]
}
```

### Updatable Sample

Configure `rowkindField` so the sink executes operations according to the action specified in each record:

```json
{
  "id": "ruleUpdateAlert",
  "sql": "SELECT * FROM alertStream",
  "actions": [
    {
      "redis": {
        "addr": "127.0.0.1:6379",
        "dataType": "string",
        "field": "id",
        "rowkindField": "action",
        "sendSingle": true
      }
    }
  ]
}
```

### Upsert Multiple Keys Sample

Set `keyType` to `multiple` so the sink updates multiple keys in Redis:

```json
{
  "id": "ruleUpdateAlert",
  "sql": "SELECT * FROM alertStream",
  "actions": [
    {
      "redis": {
        "addr": "127.0.0.1:6379",
        "dataType": "string",
        "keyType": "multiple",
        "sendSingle": true
      }
    }
  ]
}
```

When the output record has the following format, `temperature` and `humidity` are stored in Redis as separate keys:

```json
{
  "temperature": 40.9,
  "humidity": 30.9
}
```
