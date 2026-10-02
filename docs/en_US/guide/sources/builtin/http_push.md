# HTTP Push Source Connector

<span style="background:green;color:white;padding:1px;margin:2px">stream source</span>
<span style="background:green;color:white;padding:1px;margin:2px">scan table source</span>

The HTTP Push source connector exposes an HTTP server endpoint in rekuiper to receive data pushed directly by external clients.

When a client sends an HTTP request to the endpoint, rekuiper captures the payload and ingests it into the stream processing pipeline.

## Server Configuration

Configure the HTTP data listener in `etc/sources/httppush.yaml`:

```yaml
source:
  httpServerIp: 0.0.0.0
  httpServerPort: 10081
  # httpServerTls:
  #    certfile: /var/https-server.crt
  #    keyfile: /var/https-server.key
```

### Global Listener Parameters

- `httpServerIp`: Network interface IP address bound by the HTTP data server. Default is `0.0.0.0`.
- `httpServerPort`: Network port bound by the HTTP data server. Default is `10081`.
- `httpServerTls`: TLS certificate and key paths for HTTPS termination.

The server starts when any rule referencing an HTTP Push source starts. The server stops when all associated rules stop.

## Source Configuration

Configure endpoint-specific behavior in `etc/sources/httppush.yaml`:

```yaml
default:
  method: "POST"

application_conf:
  method: "PUT"
```

### Source Parameters

- `method`: HTTP request method accepted by the listener. Default is `"POST"`.

## Create a Stream Source

The HTTP Push connector operates as a [stream source](../../streams/overview.md) or as a [scan table source](../../tables/scan.md).

### Create Stream via REST API

Send a `POST` request to `/streams`:

```json
{
  "sql": "CREATE STREAM httpDemo () WITH (FORMAT = \"json\", TYPE = \"httppush\")"
}
```

### Bind Stream to a Specific URL Path

Specify the endpoint path using the `DATASOURCE` property:

```sql
CREATE STREAM httpDemo () WITH (DATASOURCE = "/api/data", FORMAT = "json", TYPE = "httppush");
```

With default server settings, the connector listens on `http://localhost:10081/api/data`.

For API specifications, refer to [Streams Management with REST API](../../../api/restapi/streams.md).

### Create Stream via CLI

Run the `kuiper create stream` command:

```bash
bin/kuiper create stream demo '() WITH (FORMAT = "json", DATASOURCE = "/api/data", TYPE = "httppush")'
```

For CLI command syntax, refer to [Streams Management with CLI](../../../api/cli/streams.md).
