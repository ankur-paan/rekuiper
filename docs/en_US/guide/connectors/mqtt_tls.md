# Secure MQTT with TLS in rekuiper

rekuiper provides TLS encryption and authentication for MQTT sources and sinks.

## Security Guarantees

1. **Strict Verification by Default**: Hostname verification and certificate chain validation are enabled by default for all TLS connections.
2. **No Plaintext Fallback**: Secure URL schemes (`ssl://`, `tls://`, `tcps://`, `mqtts://`) enforce TLS. If TLS negotiation fails or certificates are invalid, the connection terminates immediately. rekuiper never falls back to an unencrypted TCP connection.
3. **Secret Masking**: Passwords, private keys, and raw certificates are masked (`******` and `<redacted>`) in log outputs and debug strings.
4. **Standard PKI Support**: Supports both public trusted certificate authorities (using platform native root certificates and WebPKI roots) and private custom Certificate Authorities.
5. **Mutual TLS (mTLS)**: Supports client certificate and private key authentication using SEC1, PKCS#1, and PKCS#8 PEM formats.

## Configuration Options

Configure TLS settings in `mqtt_source.yaml`, `mqtt.json` sink actions, connection profiles, or REST API `confKeys`:

| Parameter | Type | Default | Description |
|---|---|---|---|
| `server` | string | `tcp://127.0.0.1:1883` | Broker URL. Schemes `ssl://`, `tls://`, `tcps://`, and `mqtts://` enforce TLS and default to port `8883`. |
| `rootCaPath` | string | `null` | Path to the Root CA certificate file in PEM format. Can be absolute or relative to the working directory. |
| `rootCaRaw` / `rootCARaw` | string | `null` | Root CA certificate content as raw PEM or base64-encoded PEM. |
| `certificationPath` | string | `null` | Path to the client certificate file in PEM format for mTLS. |
| `certificationRaw` / `certficationRaw` | string | `null` | Client certificate content as raw PEM or base64-encoded PEM. |
| `privateKeyPath` | string | `null` | Path to the client private key file in PEM format for mTLS. Supports SEC1, PKCS#1, and PKCS#8. |
| `privateKeyRaw` | string | `null` | Client private key content as raw PEM or base64-encoded PEM. |
| `insecureSkipVerify` | boolean | `false` | When `true`, disables server certificate and hostname verification. Use only in development environments. |

## Configuration Examples

### 1. Trusted Public Certificate Authorities

When connecting to brokers that use certificates from public CAs (such as Let's Encrypt or DigiCert), specify the secure URL scheme and credentials. A custom CA certificate is not required:

```json
{
  "server": "ssl://mqtt.example.com:8883",
  "topic": "factory/sensors/telemetry",
  "qos": 1,
  "username": "sensor_agent",
  "password": "SuperSecretPassword123!"
}
```

### 2. Custom CA Certificate for Private Brokers

For internal brokers signed by an organizational Root CA, specify the CA path:

```json
{
  "server": "ssl://mqtt.internal.factory.net:8883",
  "topic": "assembly/line1/metrics",
  "qos": 1,
  "rootCaPath": "/etc/rekuiper/certs/internal_ca.crt"
}
```

You can also provide the CA inline with `rootCaRaw` for containerized environments:

```json
{
  "server": "ssl://mqtt.internal.factory.net:8883",
  "topic": "assembly/line1/metrics",
  "qos": 1,
  "rootCaRaw": "-----BEGIN CERTIFICATE-----\nMIIDXTCCAkWgAwIBAgIU...\n-----END CERTIFICATE-----"
}
```

### 3. Mutual TLS Authentication

AWS IoT Core and zero-trust architectures require both client certificate and private key authentication:

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

Configure both `certificationPath` and `privateKeyPath` together. If you provide only one parameter, the engine returns a configuration error.

### 4. Testing and Development

For local testing with self-signed certificates where hostnames do not match:

```json
{
  "server": "ssl://127.0.0.1:8883",
  "topic": "test/telemetry",
  "insecureSkipVerify": true
}
```

> [!CAUTION]
> Setting `insecureSkipVerify: true` disables certificate validation and leaves the connection vulnerable to man-in-the-middle attacks. Do not use this setting in production deployments.

## Security Best Practices

To prevent credential leaks:

1. **Avoid Hardcoding Secrets**: Reference configuration keys (`CONF_KEY`) or environment variable overrides rather than hardcoding passwords in rule JSON files.
2. **Protect File Permissions**: Restrict private key file permissions (for example, mode `0600` or read-only by the rekuiper process).
3. **Configure Environment Variables**: Override connector settings through environment variables:

   ```bash
   export MQTT_SOURCE__DEFAULT__PASSWORD="mySecurePassword"
   export MQTT_SOURCE__DEFAULT__PRIVATE_KEY_PATH="/secrets/mqtt.key"
   ```

4. **Log Masking**: Sensitive fields implement redacted formatting. Log outputs mask `password` as `******` and `privateKeyRaw` as `<redacted>`.
