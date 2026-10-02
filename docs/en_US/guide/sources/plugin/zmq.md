# ZeroMQ Source Connector

<span style="background:green;color:white;padding:1px;margin:2px">stream source</span>
<span style="background:green;color:white;padding:1px;margin:2px">scan table source</span>

The ZeroMQ source connector subscribes to ZeroMQ publishers and channels messages into the rekuiper stream processing engine.

## Configuration Overview

Configure the connector in `$rekuiper/etc/sources/zmq.yaml`:

```yaml
default:
  server: tcp://192.168.2.2:5563

test:
  server: tcp://127.0.0.1:5563
```

### Configuration Parameters

- `server`: Target ZeroMQ publisher endpoint URL (such as `tcp://127.0.0.1:5563`).

## Custom Configurations

Define custom configuration blocks in `zmq.yaml` to connect to distinct ZeroMQ endpoints:

```yaml
test:
  server: tcp://127.0.0.1:5563
```

Reference the configuration using `CONF_KEY="test"`:

```sql
CREATE STREAM demo () WITH (
  DATASOURCE = "demo",
  FORMAT = "JSON",
  CONF_KEY = "test",
  TYPE = "zmq"
);
```

In this definition, `DATASOURCE` specifies the ZeroMQ subscription topic.

For stream syntax and management details, refer to [Streams Management](../../streams/overview.md).
