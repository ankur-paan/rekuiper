# RedisSub Source Connector

<span style="background:green;color:white;padding:1px;margin:2px">stream source</span>
<span style="background:green;color:white;padding:1px;margin:2px">scan table source</span>

The RedisSub source connector subscribes to Redis pub/sub channels and channels messages into the stream processing pipeline.

> [!NOTE]
> The `redisSub` connector operates as a stream source and a scan table source. To query Redis keys on demand as a lookup table, use the [Redis source](./redis.md).

## Configuration Overview

Configure connection and channel settings in `etc/sources/redisSub.yaml`:

```yaml
default:
  address: 127.0.0.1:6379
  username: default
  db: 0
  channels:
    - telemetry
```

### Configuration Parameters

- `address`: Redis server address formatted as `host:port` or `ip:port`. Default is `127.0.0.1:6379`.
- `username`: Authentication username for Redis ACL security.
- `password`: Authentication password for the Redis server.
- `db`: Redis database index. Default is `0`.
- `channels`: Array of Redis channel names for subscription.
- `decompression`: Decompresses incoming message payloads. Supported algorithms: `"zlib"`, `"gzip"`, `"flate"`, and `"zstd"`.

## Create a Stream Source

The RedisSub connector operates as a [stream source](../../streams/overview.md) or as a [scan table source](../../tables/scan.md).

### Create Stream via REST API

Send a `POST` request to `/streams`:

```json
{
  "sql": "CREATE STREAM redisSub_stream () WITH (FORMAT = \"json\", TYPE = \"redisSub\")"
}
```

For REST API specifications, refer to [Streams Management with REST API](../../../api/restapi/streams.md).

### Create Stream via CLI

Run the `kuiper create stream` command:

```bash
bin/kuiper create stream redisSub_stream '() WITH (FORMAT = "json", TYPE = "redisSub")'
```

For CLI command syntax, refer to [Streams Management with CLI](../../../api/cli/streams.md).
