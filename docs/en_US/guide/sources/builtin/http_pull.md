# HTTP Pull Source Connector

<span style="background:green;color:white;padding:1px;margin:2px">stream source</span>
<span style="background:green;color:white;padding:1px;margin:2px">scan table source</span>
<span style="background:green;color:white;padding:1px;margin:2px">lookup table source</span>

The HTTP Pull source connector retrieves data periodically from external HTTP servers.

The connector supports fixed polling intervals, conditional fetching, dynamic URL templating, and OAuth 2.0 token management.

## Configuration Overview

Configure the connector using [environment variables](../../../configuration/configuration.md#environment-variable-syntax), the [REST API](../../../api/restapi/configKey.md), or the configuration file.

The configuration file resides at `$rekuiper/etc/sources/http_pull.yaml`. Properties defined in the `default` section provide global default values. Custom sections override default values.

Example configuration file:

```yaml
default:
  url: http://localhost
  method: post
  interval: 10000
  timeout: 5000
  incremental: false
  body: '{}'
  bodyType: json
  insecureSkipVerify: true
  headers:
    Accept: application/json
  states:
  responseType: code

application_conf:
  incremental: true
  url: http://localhost:9090/pull
```

## Global Configurations

Properties in the `default` section apply to all HTTP connections unless explicitly overridden.

### HTTP Request Parameters

- `url`: Target endpoint URL.
- `method`: HTTP method: `post`, `get`, `put`, or `delete`.
- `interval`: Polling interval in milliseconds.
- `timeout`: Request timeout in milliseconds.
- `body`: Request payload string (for example, `'{"data": "telemetry"}'`).
- `bodyType`: Payload content format: `none`, `text`, `json`, `html`, `xml`, `javascript`, or `form`.
- `headers`: Map of HTTP headers sent with the request.
- `states`: Key-value pairs rendered into URL template parameters.
- `responseType`: Response validation method:
  - `code`: Validates success based on the HTTP response status code.
  - `body`: Validates success based on a `code` field inside a JSON response body.

### TLS and Security Parameters

- `certificationPath`: Path to client certificate file in PEM format.
- `privateKeyPath`: Path to client private key file in PEM format.
- `rootCaPath`: Path to Root CA certificate file in PEM format.
- `certficationRaw`: Base64-encoded client certificate string.
- `privateKeyRaw`: Base64-encoded client private key string.
- `rootCARaw`: Base64-encoded Root CA certificate string.
- `insecureSkipVerify`: Boolean. Set to `true` to skip certificate validation.

### OAuth 2.0 Authentication

Configure OAuth 2.0 token acquisition and refresh under the `oAuth` property:

```yaml
oAuth:
  access:
    url: https://127.0.0.1/api/token
    body: '{"username": "admin", "password": "password"}'
    expire: "3600"
  refresh:
    url: https://127.0.0.1/api/refresh
    headers:
      Accept: application/json
      identityId: '{{.data.identityId}}'
      token: '{{.data.token}}'
    body: ''
```

- `access.url`: Endpoint URL for initial token retrieval using HTTP POST.
- `access.body`: Request payload containing user credentials or authorization codes.
- `access.expire`: Token lifetime in seconds. Supports template expressions.
- `refresh.url`: Endpoint URL for token refresh using HTTP POST.
- `refresh.headers`: Headers required for token refresh. Supports template values extracted from initial responses.
- `refresh.body`: Payload required for token refresh.

### Incremental Fetching and State Tracking

#### Incremental Processing

Set `incremental: true` to prevent duplicate processing. The engine compares each response against the previous response. If values are identical, the connector discards the result.

#### Dynamic State Tracking

Configure the `states` property to store runtime values between polling cycles. The engine renders state values into URL parameters using [Data Template](../../sinks/data_template.md) syntax.

When `qos: 1` is configured, the engine flushes state to disk during checkpoints and restores state upon restart.

Configuration example:

```yaml
default:
  url: http://localhost/path?key1={{.key1}}&key2={{.key2}}
  method: get
  interval: 10000
  timeout: 5000
  body: '{}'
  bodyType: json
  headers:
    Accept: application/json
  states:
    key1: value1
    key2: value2
  responseType: code
```

The first request fetches:

```txt
GET http://localhost/path?key1=value1&key2=value2
```

If the response returns:

```json
{
  "key1": "value3",
  "key2": "value4"
}
```

The next request uses the updated values:

```txt
GET http://localhost/path?key1=value3&key2=value4
```

### Dynamic Properties

Dynamic properties resolve values at runtime:

- `PullTime`: Unix timestamp of the current poll execution in milliseconds (`int64`).
- `LastPullTime`: Unix timestamp of the previous poll execution in milliseconds (`int64`).
- OAuth response fields: Access tokens or session IDs returned by authentication endpoints.

::: v-pre
Use these variables to query time-windowed external APIs:

- In URL parameters: `http://localhost:9090/pull?start={{.LastPullTime}}&end={{.PullTime}}`
- In request payloads: `{"start": {{.LastPullTime}}, "end": {{.PullTime}}}`
:::

## Custom Configurations

Define named configuration blocks in `http_pull.yaml`:

```yaml
application_conf:
  incremental: true
  url: http://localhost:9090/pull
```

Reference the configuration with `CONF_KEY="application_conf"` in the stream definition:

```sql
CREATE STREAM demo () WITH (
  DATASOURCE = "test/",
  FORMAT = "JSON",
  TYPE = "httppull",
  CONF_KEY = "application_conf"
);
```

## Create a Stream Source

The HTTP Pull connector operates as a [stream source](../../streams/overview.md) or as a [scan table](../../tables/scan.md).

### Create Stream via REST API

```http
POST http://{{host}}/streams
Content-Type: application/json

{
  "sql": "CREATE STREAM http_stream () WITH (FORMAT = \"json\", TYPE = \"httppull\");"
}
```

### Create Stream via CLI

```bash
bin/kuiper create stream http_stream '() WITH (FORMAT = "json", TYPE = "httppull")'
```

## Create a Lookup Table Source

HTTP Pull supports lookup tables. The engine executes on-demand HTTP requests when a rule joins a stream with the table:

```sql
CREATE TABLE httppullTable () WITH (
  DATASOURCE = "/url",
  CONF_KEY = "default",
  TYPE = "httppull",
  KIND = "lookup"
);
```
