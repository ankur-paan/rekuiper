# REST API Reference

rekuiper provides a comprehensive REST API to manage streams, tables, rules, and system extensions.

By default, the REST API listens on HTTP port `9081`. You can configure the port number using the `restPort` property in `etc/kuiper.yaml`.

## System Information

Use this endpoint to retrieve the version string, host operating system, and process uptime.

```http
GET http://localhost:9081/
```

Example response:

```json
{
  "version": "1.0.1-22-g119ee91",
  "os": "darwin",
  "upTimeSeconds": 14
}
```

## Ping Endpoint

Use this endpoint to verify that the REST server is responsive:

```http
GET http://localhost:9081/ping
```

## Batch Requests

Use this endpoint to execute multiple REST requests sequentially in a single HTTP call:

```http
POST http://localhost:9081/batch/req
```

Request payload:

```json
[
    {
        "method": "POST",
        "path": "/streams",
        "body": "{\"sql\":\"CREATE stream demobatch() WITH (DATASOURCE=\\\"/data1\\\", TYPE=\\\"websocket\\\")\"}"
    },
    {
        "method": "GET",
        "path": "/streams/demobatch"
    }
]
```

Response payload:

```json
[
    {
        "code": 201,
        "response": "Stream demobatch is created."
    },
    {
        "code": 200,
        "response": "{\"Name\":\"demobatch\",\"StreamFields\":null,\"Options\":{\"datasource\":\"/data1\",\"type\":\"websocket\"},\"StreamType\":0,\"Statement\":null}"
    }
]
```

## REST API Sections

- [Authentication](authentication.md)
- [Streams](streams.md)
- [Tables](tables.md)
- [Rules](rules.md)
- [Rule Testing](ruletest.md)
- [Rulesets](ruleset.md)
- [Data Import and Export](data.md)
- [Plugins](plugins.md)
- [Schemas](schemas.md)
- [Services](services.md)
- [User-Defined Functions](udf.md)
- [File Uploads](uploads.md)
- [Configuration Management](configs.md)
- [Configuration Keys](configKey.md)
- [Connections](connection.md)
- [Tracing](trace.md)
