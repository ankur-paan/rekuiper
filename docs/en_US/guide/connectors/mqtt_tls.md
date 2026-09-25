# Secure MQTT with TLS in rekuiper

rekuiper provides enterprise-grade TLS encryption and authentication for MQTT sources and sinks powered by Rustls.

## Key Security Guarantees

1. **Strict Verification by Default**: Hostname verification and certificate chain validation are enabled by default for all TLS connections.
2. **Never Falls Back to Plaintext**: Secure URL schemes (`ssl://`, `tls://`, `tcps://`, `mqtts://`) strictly mandate TLS. If TLS negotiation fails or certificates are invalid, the connection fails immediately—rekuiper will **never** silently fall back to an unencrypted TCP connection.
3. **No Secret Leaks**: Passwords, private keys, and raw certificates are masked (`******` and `<redacted>`) in log outputs and `Debug` formatting.
4. **Standard PKI Support**: Supports both public trusted certificate authorities (using platform native root certificates and WebPKI roots) and private custom Certificate Authorities.
5. **Mutual TLS (mTLS)**: Full support for client certificate and private key authentication (supporting SEC1, PKCS#1, and PKCS#8 PEM formats).

---

## Configuration Options

The following TLS settings are supported in `mqtt_source.yaml`, `mqtt.json` sink actions, connection profiles, and REST API `confKeys`:

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `server` | string | `tcp://127.0.0.1:1883` | Broker URL. Schemes `ssl://`, `tls://`, `tcps://`, `mqtts://` enforce TLS and default port to `8883`. |
| `rootCaPath` | string | `null` | Path to the Root CA certificate file (PEM format). Can be absolute or relative to the working directory. |
| `rootCaRaw` / `rootCARaw` | string | `null` | Root CA certificate content as either raw PEM or base64-encoded PEM. |
| `certificationPath` | string | `null` | Path to client certificate file (PEM format) for mTLS. |
| `certificationRaw` / `certficationRaw` | string | `null` | Client certificate content as either raw PEM or base64-encoded PEM. |
| `privateKeyPath` | string | `null` | Path to client private key file (PEM format) for mTLS. Supports SEC1, PKCS#1, and PKCS#8. |
| `privateKeyRaw` | string | `null` | Client private key content as either raw PEM or base64-encoded PEM. |
| `insecureSkipVerify` | boolean | `false` | When `true`, disables server certificate and hostname verification. **Use only in test/development environments.** |

---

## Working Configuration Examples

### 1. Trusted Public Certificate Authorities (e.g. HiveMQ Cloud, EMQX Cloud)

When connecting to brokers with certificates issued by publicly trusted CAs (Let's Encrypt, DigiCert, etc.), specify the secure URL scheme and credentials. No custom CA certificate is needed:

```json
{
  "server": "ssl://broker.emqx.io:8883",
  "topic": "factory/sensors/telemetry",
  "qos": 1,
  "username": "sensor_agent",
  "password": "SuperSecretPassword123!"
}
```

### 2. Custom CA Certificate (Private / Self-Hosted Broker)

For internal brokers signed by an organizational or internal Root CA:

```json
{
  "server": "ssl://mqtt.internal.factory.net:8883",
  "topic": "assembly/line1/metrics",
  "qos": 1,
  "rootCaPath": "/etc/rekuiper/certs/internal_ca.crt"
}
```

Alternatively, supply the CA inline using `rootCaRaw` (ideal for containerized or Kubernetes environments):

```json
{
  "server": "ssl://mqtt.internal.factory.net:8883",
  "topic": "assembly/line1/metrics",
  "qos": 1,
  "rootCaRaw": "-----BEGIN CERTIFICATE-----\nMIIDXTCCAkWgAwIBAgIU...\n-----END CERTIFICATE-----"
}
```

### 3. Mutual TLS (mTLS / Client Certificate Authentication, e.g. AWS IoT Core)

AWS IoT Core and industrial zero-trust architectures require both client certificate and private key authentication:

```json
{
  "server": "ssl://a1b2c3d4e5f6-ats.iot.us-east-1.amazonaws.com:8883",
  "topic": "devices/vehicle_042/telemetry",
  "qos": 1,
  "clientId": "vehicle_042",
  "rootCaPath": "/etc/rekuiper/certs/AmazonRootCA1.pem",
  "certificationPath": "/etc/rekuiper/certs/vehicle_042.cert.pem",
  "privateKeyPath": "/etc/rekuiper/certs/vehicle_042.private.key"
}
```

Both `certificationPath` and `privateKeyPath` must be provided together. Supplying one without the other will immediately return a descriptive configuration error.

### 4. Testing & Development (Insecure Skip Verify)

For local development or testing with self-signed certificates where hostnames do not match:

```json
{
  "server": "ssl://127.0.0.1:8883",
  "topic": "test/telemetry",
  "insecureSkipVerify": true
}
```

> **Security Warning**: `insecureSkipVerify: true` disables all certificate authenticity checks, making the connection vulnerable to man-in-the-middle attacks. Never use this setting in production.

---

## Secret and Certificate Provisioning Best Practices

To prevent accidental exposure or logging of sensitive keys and passwords:

1. **Avoid Hardcoding Secrets**: Use configuration keys (`CONF_KEY`) via the REST API or environment variable overrides rather than committing passwords or keys in rule definitions.
2. **File Permissions**: When mounting certificates via Docker volumes or Kubernetes Secrets, ensure private key files have restricted permissions (`0600` or read-only by the rekuiper process).
3. **Environment Variable Configuration**:
   Override specific connector parameters with environment variables:
   ```bash
   export MQTT_SOURCE__DEFAULT__PASSWORD="mySecurePassword"
   export MQTT_SOURCE__DEFAULT__PRIVATE_KEY_PATH="/secrets/mqtt.key"
   ```
4. **Log Masking**: All `MqttConfig` structs implement redacted debug formatting. Even when tracing at `DEBUG` or `TRACE` levels, `password` and `privateKeyRaw` values will appear as `******` and `<redacted>`.
