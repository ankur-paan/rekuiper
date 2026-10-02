# Dynamic Secrets Management

This document describes dynamic secret resolution in `rekuiper`.

Do not store passwords, keys, or tokens in plain text in configuration files or stream definitions. Use dynamic secrets instead.

`rekuiper` supports two secret resolution backends:
1. **HashiCorp Vault** (`vault://`): Retrieves secrets from an external Vault service.
2. **Environment Variables** (`env://`): Retrieves secrets from system or container environment variables.

## Secret URI Syntax

You can use secret URIs in connection strings, passwords, and custom headers.

### HashiCorp Vault Format

```text
vault://<mount>/<path>#<field>
```

- `<mount>`: The secret engine mount path (for example, `secret` or `kv`).
- `<path>`: The secret path in Vault (for example, `mqtt/broker` or `devices/factory1`).
- `<field>`: The field name to read from the secret data (for example, `password` or `token`).

### Environment Variable Format

```text
env://<VARIABLE_NAME>
```

- `<VARIABLE_NAME>`: The name of the environment variable (for example, `MQTT_PASSWORD`).

---

## Vault Configuration

`rekuiper` connects to HashiCorp Vault using HTTP requests. Configure connection parameters using these environment variables:

| Variable | Default | Description |
| :--- | :--- | :--- |
| `VAULT_ADDR` | `"http://127.0.0.1:8200"` | The base URL of the Vault server. |
| `VAULT_TOKEN` | (None) | The authentication token sent in the `X-Vault-Token` header. |

`rekuiper` supports both Vault KV Version 1 and KV Version 2 backends automatically.

---

## Secret Caching

To reduce network traffic to the Vault server, `rekuiper` caches resolved secrets in memory.

- **Cache Duration**: Secrets remain in memory for **300 seconds (5 minutes)**.
- **Cache Refresh**: The engine refreshes secrets automatically after the cache expires.
- **Failover**: If the secret backend is unreachable during a refresh, `rekuiper` continues to use the previous cached value.

---

## Usage Examples

### MQTT Connector Example

You can set secrets in configuration files:

```yaml
# etc/mqtt_source.yaml
default:
  server: "tcp://broker.internal:1883"
  username: "edge_collector"
  password: "vault://secret/mqtt#password"
```

Or you can use template interpolation in SQL:

```sql
CREATE STREAM factory_mqtt () WITH (
    TYPE = "mqtt",
    SERVER = "tcp://broker.internal:1883",
    USERNAME = "edge_collector",
    PASSWORD = "{{vault://secret/production/mqtt#token}}",
    DATASOURCE = "telemetry/#",
    FORMAT = "JSON"
);
```

### RabbitMQ Connector Example

```sql
CREATE STREAM amqp_feed () WITH (
    TYPE = "rabbitmq",
    SERVER = "amqp://guest:{{env://RABBIT_PASSWORD}}@10.0.0.15:5672/%2f",
    QUEUE = "events",
    FORMAT = "JSON"
);
```

---

## Secret Redaction

`rekuiper` protects your sensitive information:
- Logs do not print raw secret values. The system replaces secrets with `[REDACTED]`.
- API endpoints (`GET /streams` and `GET /rules`) display the original `vault://` or `env://` URI and do not expose the resolved secret.
