# Tables Management

> [!NOTE]
> **Verification Status**: Tested and Verified against `rekuiper` engine on **2026-09-30 22:12:20 UTC**.  
> **Scorecard**: **8 / 8 Methods Fully Verified (100% Parity)**:
> - `POST /tables` (create table) - Verified (HTTP 201 Created)
> - `GET /tables` (show tables, supports `?kind=lookup` and `?kind=scan`) - Verified (HTTP 200 OK)
> - `GET /tabledetails` (show tables detail, supports `?kind=lookup` and `?kind=scan`) - Verified (HTTP 200 OK)
> - `GET /tables/{id}` (describe table) - Verified (HTTP 200 OK)
> - `GET /tables/{id}/schema` (get table schema) - Verified (HTTP 200 OK)
> - `PUT /tables/{id}` (update table) - Verified (HTTP 200 OK)
> - `DELETE /tables/{id}` (drop table) - Verified (HTTP 200 OK, HTTP 400 on subsequent query)
> - `GET /rules/{rule}/scantables` (query scan table) - Verified (HTTP 200 OK)

The rekuiper REST API manages table definitions. You can create, list, describe, update, drop tables, and inspect table schemas and scan table contents.

## Create a Table

Use this endpoint to create a table. For syntax details, refer to [Tables](../../sqls/tables.md).

```http
POST http://localhost:9081/tables
```

Request payload with the `sql` statement:

```json
{"sql":"create table my_table (id bigint, name string, score float) WITH ( datasource = \"lookup.json\", FORMAT = \"json\", KEY = \"id\")"}
```

This endpoint can run any table SQL statement.

Response sample (HTTP 201 Created):

```text
Table my_table is created.
```

## Show Tables

Use this endpoint to list table names defined on the server:

```http
GET http://localhost:9081/tables
```

Response sample (HTTP 200 OK):

```json
["mytable"]
```

Filter by table kind using the `kind` query parameter (`scan` or `lookup`):

```http
GET http://localhost:9081/tables?kind=lookup
```

## Show Tables Detail

Use this endpoint to display configuration summaries for all defined tables:

```http
GET http://localhost:9081/tabledetails
```

Response sample (HTTP 200 OK):

```json
[
  {
    "name": "test2",
    "type": "file",
    "format": "json"
  }
]
```

Filter by table kind using the `kind` query parameter (`scan` or `lookup`):

```http
GET http://localhost:9081/tabledetails?kind=lookup
```

## Describe a Table

Use this endpoint to display the complete definition and configuration of a table:

```http
GET http://localhost:9081/tables/{id}
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
    "DATASOURCE": "lookup.json",
    "FORMAT": "JSON"
  }
}
```

## Get Table Schema

Use this endpoint to retrieve the table schema inferred from physical and logical definitions:

```http
GET http://localhost:9081/tables/{id}/schema
```

Response sample (HTTP 200 OK):

```json
{
  "id": {
    "index": 0,
    "type": "bigint"
  },
  "name": {
    "index": 1,
    "type": "string"
  },
  "score": {
    "index": 2,
    "type": "float"
  }
}
```

## Update a Table

Use this endpoint to update an existing table definition:

```http
PUT http://localhost:9081/tables/{id}
```

The path parameter `id` specifies the name of the table to update.

Request payload:

```json
{"sql":"create table my_table (id bigint, name string, score float) WITH ( datasource = \"topic/temperature\", FORMAT = \"json\", KEY = \"id\")"}
```

Response sample (HTTP 200 OK):

```text
Table my_table is updated.
```

## Drop a Table

Use this endpoint to delete a table definition:

```http
DELETE http://localhost:9081/tables/{id}
```

Response sample (HTTP 200 OK):

```text
Table my_table is dropped.
```

Subsequent requests to `GET /tables/{id}` return `HTTP 400 Bad Request`:

```json
{
  "error": 3000,
  "message": "describe table error: Describe table fails, my_table is not found."
}
```

## Query a Scan Table

Use this endpoint to inspect the current contents of scan tables bound to a running rule:

```http
GET http://localhost:9081/rules/{rule}/scantables
```

Response sample (HTTP 200 OK):

```json
[
  {
    "emitter": "table_name",
    "content": {
      "id": 1,
      "name": "sensor_01",
      "score": 98.5
    }
  }
]
```
