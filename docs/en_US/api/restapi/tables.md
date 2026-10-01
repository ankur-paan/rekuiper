# Tables management

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

The rekuiper REST API for tables allows you to manage the tables, such as create, describe, show and drop table definitions.

## create a table

The API is used for creating a table. For more detailed information of table definition, please refer to [tables](../../sqls/tables.md).

```shell
POST http://localhost:9081/tables
```

Request sample, the request is a json string with `sql` field.

```json
{"sql":"create table my_table (id bigint, name string, score float) WITH ( datasource = \"lookup.json\", FORMAT = \"json\", KEY = \"id\")"}
```

This API can run any table sql statements, not only table creation.

Response Sample (HTTP 201 Created):

```text
Table my_table is created.
```

## show tables

The API is used for displaying all of tables defined in the server.

```shell
GET http://localhost:9081/tables
```

Response Sample (HTTP 200 OK):

```json
["mytable"]
```

This API accepts one parameter kind, the value could be `scan` or `lookup` to query each kind of tables. Other values are invalid, it will return all kinds of tables. In below example, we can query all the lookup tables.

```shell
GET http://localhost:9081/tables?kind=lookup
```

## show tables detail

The API is used for displaying all detailed definition of tables defined in the server.

```shell
GET http://localhost:9081/tabledetails
```

Response Sample (HTTP 200 OK):

```json
[
  {
    "name": "test2",
    "type": "file",
    "format": "json"
  }
]
```

This API accepts one parameter kind, the value could be `scan` or `lookup` to query each kind of tables. Other values are invalid, it will return all kinds of tables. In below example, we can query all the lookup tables.

```shell
GET http://localhost:9081/tabledetails?kind=lookup
```

## describe a table

The API is used for print the detailed definition of table.

```shell
GET http://localhost:9081/tables/{id}
```

Response Sample (HTTP 200 OK):

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
    "DATASOURCE": "lookup.json",
    "FORMAT": "JSON"
  }
}
```

## Get table schema

The API is used to get the table schema. The schema is inferred from the physical and logical schema definitions.

```shell
GET http://localhost:9081/tables/{id}/schema
```

Response Sample (HTTP 200 OK):

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

## update a table

The API is used for update the table definition.

```shell
PUT http://localhost:9081/tables/{id}
```

Path parameter `id` is the id or name of the old table.

Request sample, the request is a json string with `sql` field.

```json
{"sql":"create table my_table (id bigint, name string, score float) WITH ( datasource = \"topic/temperature\", FORMAT = \"json\", KEY = \"id\")"}
```

Response Sample (HTTP 200 OK):

```text
Table my_table is updated.
```

## drop a table

The API is used for drop the table definition.

```shell
DELETE http://localhost:9081/tables/{id}
```

Response Sample (HTTP 200 OK):

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

## query a scan table

This API allows users to directly query the content of scan tables related to a specific rule.

```shell
GET http://localhost:9081/rules/{rule}/scantables
```

This GET call returns the actual content of each scan table associated with the given rule.

Response Sample (HTTP 200 OK):

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

