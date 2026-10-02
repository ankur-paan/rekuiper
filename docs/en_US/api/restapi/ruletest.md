# Rule Test Run

The rule testing REST API allows you to execute trial runs of rules with simulated data. You can verify SQL syntax, validate data transformations, and inspect sink output without modifying production streams.

A test rule is temporary and does not persist on the server. Test rules automatically stop and clear after a fixed 10-minute timeout.

## Rule Testing Workflow

1. [Create a test rule](#create-a-test-rule) to obtain the test rule ID and SSE port.
2. Connect an HTTP client to the Server-Sent Events (SSE) endpoint at `http://localhost:10081/test/{id}` using the header `Accept: text/event-stream`.
3. [Start the test rule](#start-the-test-rule) to trigger data processing. Inspect output events emitted through the SSE connection.
4. [Delete the test rule](#delete-the-test-rule) to terminate execution and close the SSE stream.

> [!NOTE]
> The SSE service listens on HTTP port `10081` by default. You can configure this port using `httpServerPort` in `etc/kuiper.yaml`.

## Create a Test Rule

Use this endpoint to define and compile a temporary test rule:

```http
POST http://localhost:9081/ruletest
```

Request payload:

```json
{
  "id": "uuid",
  "sql": "select * from demo",
  "mockSource": {
    "demo": {
      "data": [
        {
          "a": 2
        },
        {
          "b": 3
        }
      ],
      "interval": 100,
      "loop": true
    },
    "demo1": {
      "data": [
        {
          "n": 2
        },
        {
          "n": 3
        }
      ],
      "interval": 200,
      "loop": true
    }
  },
  "sinkProps": {
    "dataTemplate": "xxx",
    "fields": [
      "abc",
      "test"
    ]
  }
}
```

### Request Fields

- `id`: The unique identifier for the test rule. Uniqueness is required across active test rules.
- `sql`: The SQL query statement to test.
- `mockSource` (optional): Simulated input records, injection interval in milliseconds, and looping configuration. If omitted, the engine connects to live sources referenced in SQL.
- `sinkProps` (optional): Common sink parameters, including `dataTemplate` and `fields`.

Successful response (HTTP 200 OK):

```json
{
  "id": "uuid",
  "port": 10081
}
```

After creation, the SSE server starts listening on `http://localhost:10081/test/{id}`.

Error response (HTTP 400 Bad Request):

```json
{
  "msg": "error message here"
}
```

## Start the Test Rule

Use this endpoint to start stream processing for the test rule:

```http
POST http://localhost:9081/ruletest/{id}/start
```

The connected SSE client receives output events as they are produced.

## Delete the Test Rule

Use this endpoint to stop and delete the test rule and terminate the SSE connection:

```http
DELETE http://localhost:9081/ruletest/{id}
```
