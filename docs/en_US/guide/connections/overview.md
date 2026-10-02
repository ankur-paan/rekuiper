# Connection Management

Sources and sinks interact with external systems through network connections. This document explains how rekuiper manages connection lifecycles, connection pooling, and connection reuse.

## Connection Types and Pooling

External systems require different connection management strategies. For example, MQTT connections require state tracking and automatic reconnection after network disconnects. In contrast, HTTP connections are stateless.

To unify connection management, rekuiper provides an internal connection pool component. The connection pool manages creation, resource reuse, automatic reconnection, and status monitoring for these protocols:

- MQTT Connections
- EdgeX Connections
- SQL Connections
- HTTP Connections (REST sink, HTTP pull source, and HTTP push source)
- WebSocket Connections
- Kafka Connections

Connection types integrated into the connection pool support independent creation and management through the REST API.

## Connection Lifecycle Categories

rekuiper manages connection lifecycles through three distinct models:

### 1. Connection Attached to a Rule

By default, the source or sink implementation manages its own connection. The lifecycle matches the rule lifecycle. The engine establishes the connection when the rule starts and closes the connection when the rule stops.

In this example, `memStream` uses a memory connector that attaches directly to the rule:

```sql
CREATE STREAM memStream () WITH (TYPE="memory", DATASOURCE="demo");
```

### 2. Anonymous Connection Managed by the Connection Pool

For protocols integrated with the connection pool, rules request an anonymous connection resource from the pool upon startup. The pool generates a unique internal identifier. The connection is not shared with other rules. When you delete the rule, the engine deletes the connection.

In this example, `mqttStream` creates an anonymous connection managed by the pool:

```sql
CREATE STREAM mqttStream () WITH (TYPE="mqtt", DATASOURCE="demo");
```

### 3. User-Created Connection Resource

Users create and manage shared connection resources through the [Connection Management API](../../api/restapi/connection.md).

API-created connections require a unique identifier. These connections exist as independent physical connections that establish immediately upon creation. They operate independently of rules and support sharing across multiple rules, sources, and sinks.

User-created connections automatically reconnect until the connection succeeds.

## Connection Reuse

User-created connection resources operate independently. Multiple rules and data streams can reference a single named connection through the `connectionSelector` property.

Connection reuse simplifies configuration and conserves network sockets.

### Step 1: Create the Connection Resource

Create a connection resource with ID `mqttcon1` through the REST API:

```shell
POST http://localhost:9081/connections
Content-Type: application/json

{
  "id": "mqttcon1",
  "typ": "mqtt",
  "props": {
    "server": "tcp://127.0.0.1:1883"
  }
}
```

### Step 2: Reference the Connection in the Source Configuration

In the MQTT source configuration file (`$rekuiper/etc/mqtt_source.yaml`), reference `mqttcon1` with `connectionSelector`:

```yaml
demo_conf:
  qos: 0
  connectionSelector: mqttcon1
  servers: [ tcp://10.211.55.6:1883, tcp://127.0.0.1 ]

demo2_conf:
  qos: 0
  connectionSelector: mqttcon1
  servers: [ tcp://10.211.55.6:1883, tcp://127.0.0.1 ]
```

### Step 3: Define Streams Using the Configurations

Create streams that use `demo_conf` and `demo2_conf`:

```sql
CREATE STREAM demo () WITH (DATASOURCE="test/", FORMAT="JSON", CONF_KEY="demo_conf");

CREATE STREAM demo2 () WITH (DATASOURCE="test2/", FORMAT="JSON", CONF_KEY="demo2_conf");
```

Rules that query `demo` and `demo2` share the underlying `mqttcon1` physical connection.

> [!TIP]
> If two MQTT streams subscribe to the same topic (`DATASOURCE`) with different `qos` values, only the rule that starts first initiates the MQTT subscription.

### Kafka Sink Connection Reuse

For Kafka sinks, configure `connectionSelector` to reference a shared Kafka connection resource. The Kafka connection manages connection status and tests broker connectivity. When a Kafka sink references the connection, the runtime copies broker, SASL, and TLS settings from the selected connection. The sink creates an independent Kafka producer client to send messages.

## Connection Status

The engine reports connection status using three numerical states:

1. **Connected**: Represented by `1` in metrics.
2. **Connecting**: Represented by `0` in metrics.
3. **Disconnected**: Represented by `-1` in metrics.

Retrieve connection status through the Connection REST API or through Prometheus metrics (for example, `source_demo_0_connection_status`). For metric definitions, refer to [Monitor Rule Status with Prometheus](../../operation/usage/monitor_with_prometheus.md#metric-types).

## Static Provisioning with connection.yaml

You can predefine connections in `etc/connections/connection.yaml`. The engine loads connections from this file during startup.

`connection.yaml` functions as an initialization manifest, not as a runtime state store. At startup, the service computes a SHA-256 hash of the effective configuration (file content combined with environment variable overrides) and compares it with the stored hash:

- **Hash unchanged**: The engine skips provisioning. Connections deleted through the REST API will not be recreated.
- **Hash changed**: The engine executes declared create or delete operations and stores the new hash. Creation operations do not overwrite existing connections in storage. Operations must succeed before the engine updates the stored hash. If an error occurs, the engine retries provisioning at the next startup.

To trigger re-provisioning, modify `connection.yaml` or change an environment variable override (such as `CONNECTION__MQTT__CLOUD__SERVER`). Restarting the engine without configuration changes does not re-apply provisioning operations. Removing an entry from the file does not delete the connection from storage; you must declare deletions explicitly.

### Basic Configuration Example

```yaml
mqtt:
  localConnection:
    server: tcp://127.0.0.1:1883
    username: rekuiper
    password: password
  cloudConnection:
    server: tcp://cloud.example.com:1883
    username: user1
    password: password
```

### The xOperation Property

Use the `xOperation` property in `connection.yaml` to declare creation or deletion:

- `create` (default): Writes the connection to persistent storage. If a connection with the same ID already exists, the engine skips the operation.
- `delete`: Deletes the specified connection from persistent storage.

#### Delete a Connection

```yaml
mqtt:
  cloudConnection:
    xOperation: delete
```

During startup, the engine deletes `connections.mqtt.cloudConnection`.

#### Seed a Connection Without Overwriting

```yaml
mqtt:
  localConnection:
    xOperation: create
    server: tcp://127.0.0.1:1883
```

If `localConnection` already exists in storage, the engine preserves the existing configuration.

### Provisioning Rules

- The engine strips the `xOperation` property before writing connection properties to storage.
- Connections created through the REST API are preserved. The `create` operation does not overwrite existing configurations.
- Deleting an unrecorded connection identifier is an idempotent operation and does not produce an error.
- Emptying `connection.yaml` does not delete existing connections. Declare deletions explicitly using `xOperation: delete`.
