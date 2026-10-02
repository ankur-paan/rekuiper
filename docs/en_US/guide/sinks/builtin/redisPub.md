# RedisPub Action

The RedisPub action publishes output messages to a Redis pub/sub channel.

## Properties

| Property name | Optional | Description |
|---|---|---|
| address | false | The network address of Redis, such as `127.0.0.1:6379`. |
| username | true | The Redis login username for authentication. |
| password | true | The Redis login password for authentication. |
| db | false | The Redis database number (0 to 15), such as `0`. |
| channel | false | The Redis pub/sub channel name to which messages are published. |
| compression | true | Compresses the payload with the specified algorithm: `zlib`, `gzip`, `flate`, or `zstd`. |

Other common sink properties are supported. Refer to [sink common properties](../overview.md#common-properties) for more information.

## Sample Usage

The following configuration publishes compressed messages to a local Redis pub/sub channel:

```json
{
  "redisPub": {
    "address": "127.0.0.1:6379",
    "username": "default",
    "password": "123456",
    "db": 0,
    "channel": "exampleChannel",
    "compression": "zlib"
  }
}
```

This configuration sends output messages to channel `exampleChannel` in Redis database `0` and compresses the payload with `zlib`.
