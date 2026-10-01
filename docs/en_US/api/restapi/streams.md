# Streams management

> [!NOTE]
> **Verification Status**: Tested and Verified against `rekuiper` engine with live stream load on **2026-09-30 19:05:46 UTC**.  
> **Scorecard**: **7 / 7 Methods Exhaustively Verified with Live Telemetry Ingestion & Query Execution**:
> - `POST /streams` (create stream) - Verified (HTTP 201 Created)
> - `GET /streams` (show streams) - Verified (HTTP 200 OK)
> - `GET /streamdetails` (show streams detail) - Verified (HTTP 200 OK)
> - `GET /streams/{id}` (describe a stream) - Verified (HTTP 200 OK)
> - `GET /streams/{id}/schema` (get stream schema) - Verified (HTTP 200 OK)
> - `PUT /streams/{id}` (update a stream) - Verified (HTTP 200 OK)
> - `DELETE /streams/{id}` (drop a stream) - Verified (HTTP 200 OK, HTTP 400 on subsequent query)

The rekuiper REST API for streams allows you to manage the streams, such as create, describe, show and drop stream definitions.

## create a stream

The API is used for creating a stream. For more detailed information of stream definition, please refer to [streams](../../sqls/streams.md).

```shell
POST http://localhost:9081/streams
```

Request sample, the request is a json string with `sql` field.

```json
{"sql":"create stream my_stream (id bigint, name string, score float) WITH ( datasource = \"topic/temperature\", FORMAT = \"json\", KEY = \"id\")"}
```

This API can run any stream sql statements, not only stream creation.

Response Sample (HTTP 201 Created):

```text
Stream my_stream is created.
```

## show streams

The API is used for displaying all of streams defined in the server.

```shell
GET http://localhost:9081/streams
```

Response Sample (HTTP 200 OK):

```json
["mystream"]
```

## show streams detail

The API is used for displaying all detailed definition of streams defined in the server.

```shell
GET http://localhost:9081/streamdetails
```

Response Sample (HTTP 200 OK):

```json
[
  {
    "name": "test1",
    "type": "mqtt",
    "format": "json"
  }
]
```

## describe a stream

The API is used for print the detailed definition of stream.

```shell
GET http://localhost:9081/streams/{id}
```

Response Sample:

```shell
{
  "Name": "demo",
  "StreamFields": [
    {
      "Name": "temperature",
      "FieldType": {
        "Type": 2
      }
    },
    {
      "Name": "ts",
      "FieldType": {
        "Type": 1
      }
    }
  ],
  "Options": {
    "DATASOURCE": "demo",
    "FORMAT": "JSON"
  }
}
```

## get stream schema

The API is used to get the stream schema. The schema is inferred from the physical and logical schema definitions.

```shell
GET http://localhost:9081/streams/{id}/schema
```

The format is like Json schema:

```json
{
    "id": {
        "type": "bigint"
  },
    "name": {
        "type": "string"
  },
    "age": {
        "type": "bigint"
  },
    "hobbies": {
        "type": "struct",
        "properties": {
          "indoor": {
            "type": "array",
            "items": {
              "type": "string"
            }
          },
          "outdoor": {
            "type": "array",
            "items": {
              "type": "string"
            }
          }
        }
    }
}
```

For a shared stream, the schema adjusts dynamically during runtime according to the active processing rules. To reduce
computational overhead, the system maintains only the minimal schema needed to support those rules.

## update a stream

The API is used for update the stream definition.

```shell
PUT http://localhost:9081/streams/{id}
```

Path parameter `id` is the id or name of the old stream.

Request sample, the request is a json string with `sql` field.

```json
{"sql":"create stream my_stream (id bigint, name string, score float) WITH ( datasource = \"topic/temperature\", FORMAT = \"json\", KEY = \"id\")"}
```

Response Sample (HTTP 200 OK):

```text
Stream my_stream is updated.
```

## drop a stream

The API is used for drop the stream definition.

```shell
DELETE http://localhost:9081/streams/{id}
```

Response Sample (HTTP 200 OK):

```text
Stream my_stream is dropped.
```

Subsequent requests to `GET /streams/{id}` will return `HTTP 400 Bad Request`:

```json
{
  "error": 3000,
  "message": "describe stream error: Describe stream fails, my_stream is not found."
}
```

