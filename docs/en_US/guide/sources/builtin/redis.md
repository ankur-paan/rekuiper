# Redis Source Connector

<span style="background:green;color:white;padding:1px;margin:2px">lookup table source</span>

The Redis source connector retrieves data from a Redis instance for on-demand lookups during stream processing.

> [!NOTE]
> The `redis` connector functions only as a [lookup table](../../tables/lookup.md). To subscribe to streaming messages published on Redis channels, use the [RedisSub source](./redisSub.md).

## Configuration Overview

Configure connection settings in `/etc/sources/redis.yaml`:

```yaml
default:
  addr: "127.0.0.1:6379"
  datatype: "string"
  # username: ""
  # password: ""
```

### Configuration Parameters

- `addr`: Redis server address formatted as `host:port` or `ip:port`. Default is `"127.0.0.1:6379"`.
- `datatype`: Data structure stored in Redis keys: `"string"` or `"list"`.
- `username`: Authentication username when Redis ACL authentication is enabled.
- `password`: Authentication password for the Redis server.

## Create a Lookup Table Source

Define a lookup table using SQL DDL syntax. Set `DATASOURCE` to the target Redis database index (such as `"0"`):

### Create Table via REST API

Send a `POST` request to `/tables`:

```json
{
  "sql": "CREATE TABLE table1 () WITH (DATASOURCE = \"0\", FORMAT = \"json\", TYPE = \"redis\", KIND = \"lookup\")"
}
```

For REST API specifications, refer to [Tables Management with REST API](../../../api/restapi/tables.md).

### Create Table via CLI

Run the `kuiper create table` command:

```bash
bin/kuiper create table redis_table '() WITH (DATASOURCE = "0", FORMAT = "json", TYPE = "redis", KIND = "lookup")'
```

For CLI command syntax, refer to [Tables Management with CLI](../../../api/cli/tables.md).
