# WebSocket Source Connector

<span style="background:green;color:white;padding:1px;margin:2px">stream source</span>

The WebSocket source connector ingests real-time events over WebSocket network connections.

The connector reads incoming WebSocket `TextMessage` frames and parses payloads as JSON objects.

The connector operates in two modes:

1. **Client Mode**: rekuiper connects to an external WebSocket server.
2. **Server Mode**: rekuiper hosts a WebSocket endpoint and accepts incoming client connections.

## Mode 1: rekuiper as a WebSocket Client

In client mode, rekuiper initiates a WebSocket connection to a remote server and receives streaming data.

Configure the server address in `etc/sources/websocket.yaml`:

```yaml
default:
  addr: 127.0.0.1:8080
  scheme: ws
```

Define a stream referencing the configuration and target path:

```sql
CREATE STREAM demo () WITH (CONF_KEY = "default", DATASOURCE = "/api/data", TYPE = "websocket");
```

rekuiper connects to `ws://127.0.0.1:8080/api/data` and consumes incoming messages.

Verify server reachability using the [Connectivity Check API](../../../api/restapi/connection.md#connectivity-check).

## Mode 2: rekuiper as a WebSocket Server

In server mode, rekuiper hosts a WebSocket endpoint. External clients connect to rekuiper and push messages.

To enable server mode, set `addr: ""` in `etc/sources/websocket.yaml`:

```yaml
default:
  addr: ""
```

Define the stream with the listener path:

```sql
CREATE STREAM demo () WITH (CONF_KEY = "default", DATASOURCE = "/api/data", TYPE = "websocket");
```

rekuiper listens on `/api/data` and receives data pushed by external WebSocket clients.

### Server Listener Configuration

Configure listener binding and TLS settings under `source` in `etc/sources/websocket.yaml`:

```yaml
source:
  httpServerIp: 0.0.0.0
  httpServerPort: 10081
  # httpServerTls:
  #    certfile: /var/https-server.crt
  #    keyfile: /var/https-server.key
```

- `httpServerIp`: Network interface address bound by the WebSocket server. Default is `0.0.0.0`.
- `httpServerPort`: Port number bound by the WebSocket server. Default is `10081`.
- `httpServerTls`: TLS certificate and key paths for secure WebSocket (`wss://`) connections.

The server starts when any rule referencing the WebSocket source starts. The server stops when all referencing rules terminate.

## Create a Stream Source

The WebSocket connector functions as a [stream source](../../streams/overview.md).

### Create Stream via REST API

Send a `POST` request to `/streams`:

```json
{
  "sql": "CREATE STREAM websocketDemo () WITH (DATASOURCE = \"/api/data\", FORMAT = \"json\", TYPE = \"websocket\")"
}
```

With default server settings, the connector listens on `ws://localhost:10081/api/data`.

For REST API specifications, refer to [Streams Management with REST API](../../../api/restapi/streams.md).

### Create Stream via CLI

Run the `kuiper create stream` command:

```bash
bin/kuiper create stream demo '() WITH (FORMAT = "json", DATASOURCE = "/api/data", TYPE = "websocket")'
```

For CLI command syntax, refer to [Streams Management with CLI](../../../api/cli/streams.md).
