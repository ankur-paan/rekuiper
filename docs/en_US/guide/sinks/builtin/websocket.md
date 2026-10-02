# WebSocket Action

The WebSocket action publishes output messages through a WebSocket connection.

## Properties

| Property name | Optional | Description |
|---|---|---|
| addr | false | The network address of the WebSocket server, such as `127.0.0.1:8080`. |
| path | true | The URL path of the WebSocket server endpoint, such as `/api/data`. |
| scheme | true | The URL scheme of the WebSocket server: `ws` or `wss`. |
| insecureSkipVerify | false | Controls whether to skip SSL/TLS certificate verification. Default: `false`. |
| certificationPath | true | The client certificate file path for TLS verification. |
| privateKeyPath | true | The client private key file path for TLS verification. |
| rootCaPath | true | The root CA certificate file path for TLS verification. |
| certficationRaw | true | Base64-encoded raw text of the certificate. rekuiper uses `certificationPath` first if you define both. |
| privateKeyRaw | true | Base64-encoded raw text of the private key. rekuiper uses `privateKeyPath` first if you define both. |
| rootCARaw | true | Base64-encoded raw text of the root CA certificate. rekuiper uses `rootCaPath` first if you define both. |
| checkConnection | false | Controls whether rekuiper verifies that a WebSocket connection exists before rule creation. Default: `false`. |

Other common sink properties are supported. Refer to [sink common properties](../overview.md#common-properties) for more information.

## rekuiper as WebSocket Client

When the WebSocket sink defines both `addr` and `path`, rekuiper operates as a WebSocket client. rekuiper connects to the remote server and pushes messages through that connection.

You can verify the connectivity of the sink endpoint before rule execution by using the REST API: [Connectivity Check](../../../api/restapi/connection.md#connectivity-check).

## rekuiper as WebSocket Server

When the WebSocket sink defines `path` and leaves `addr` empty, rekuiper operates as a WebSocket server. rekuiper waits for remote clients to connect and pushes messages to connected clients.

When `checkConnection` is set to `true`, rekuiper verifies that the corresponding WebSocket endpoint and active connection exist before creating the rule. Refer to [Manage WebSocket Connection](../../../api/restapi/connection.md#manage-websocket-connection) for details on managing connections through the REST API.

### Server Configuration

To configure rekuiper as a WebSocket server endpoint, configure the settings in `etc/sources/websocket.yaml`:

```yaml
source:
  ## Configurations for the global websocket server for websocket source
  # HTTP data service ip
  httpServerIp: 0.0.0.0
  # HTTP data service port
  httpServerPort: 10081
  # httpServerTls:
  #    certfile: /var/https-server.crt
  #    keyfile: /var/https-server.key
```

Configure the following properties:

- `httpServerIp`: The IP address to bind the HTTP data server.
- `httpServerPort`: The TCP port to bind the HTTP data server.
- `httpServerTls`: The TLS configuration for the HTTP server.

The global server starts when any rule that requires a WebSocket endpoint is started. It stops when all associated rules are stopped.

## Sample Usage

The following configuration publishes output data to an external WebSocket server:

```json
{
  "websocket": {
    "addr": "127.0.0.1:8080",
    "path": "/api/data"
  }
}
```
