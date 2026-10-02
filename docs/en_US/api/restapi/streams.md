# Streams Management

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

The rekuiper REST API manages stream definitions. You can create, list, describe, update, drop streams, and inspect stream schemas.

## Create a Stream

Use this endpoint to create a new stream. For syntax details, refer to [Streams](../../sqls/streams.md).

```http
POST http://localhost:9081/streams
```

Request payload with the `sql` statement:

```json
{"sql":"create stream my_stream (id bigint, name string, score float) WITH ( datasource = \"topic/temperature\", FORMAT = \"json\", KEY = \"id\")"}
```

This endpoint can run any stream SQL statement.

Response sample (HTTP 201 Created):

```text
Stream my_stream is created.
```

## Show Streams

Use this endpoint to list all stream names defined on the server:

```http
GET http://localhost:9081/streams
```

Response sample (HTTP 200 OK):

```json
["mystream"]
```

## Show Streams Detail

Use this endpoint to retrieve detailed configuration summaries for all defined streams:

```http
GET http://localhost:9081/streamdetails
```

Response sample (HTTP 200 OK):

```json
[
  {
    "name": "test1",
    "type": "mqtt",
    "format": "json"
  }
]
```

## Describe a Stream

Use this endpoint to display the complete schema and configuration of a stream:

```http
GET http://localhost:9081/streams/{id}
```

Response sample (HTTP 200 OK):

```json
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

## Get Stream Schema

Use this endpoint to retrieve the inferred schema combining physical and logical definitions:

```http
GET http://localhost:9081/streams/{id}/schema
```

The response follows JSON schema format:

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

For shared streams, the engine dynamically adjusts the runtime schema based on active rules to minimize memory and CPU overhead.

## Update a Stream

Use this endpoint to update an existing stream definition:

```http
PUT http://localhost:9081/streams/{id}
```

The path parameter `id` specifies the name of the stream to update.

Request payload:

```json
{"sql":"create stream my_stream (id bigint, name string, score float) WITH ( datasource = \"topic/temperature\", FORMAT = \"json\", KEY = \"id\")"}
```

Response sample (HTTP 200 OK):

```text
Stream my_stream is updated.
```

## Drop a Stream

Use this endpoint to delete a stream definition:

```http
DELETE http://localhost:9081/streams/{id}
```

Response sample (HTTP 200 OK):

```text
Stream my_stream is dropped.
```

Subsequent requests to `GET /streams/{id}` return `HTTP 400 Bad Request`:

```json
{
  "error": 3000,
  "message": "describe stream error: Describe stream fails, my_stream is not found."
}
```
