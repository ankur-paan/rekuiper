# Schemas Management

The rekuiper REST API manages data schemas. You can create, list, describe, update, upload, and delete schemas.

## Create a Schema

Use this endpoint to register a new schema. Each schema format uses a dedicated endpoint. Currently, rekuiper supports `protobuf`.

```http
POST http://localhost:9081/schemas/protobuf
```

### Specify Schema Content Inline

```json
{
  "name": "schema1",
  "content": "message Book {required string title = 1; required int32 price = 2;}"
}
```

### Specify Schema Content by File URI

```json
{
  "name": "schema2",
  "file": "file:///tmp/test2.proto"
}
```

### Specify Schema with Static Plugin

```json
{
  "name": "schema2",
  "file": "file:///tmp/test2.proto",
  "soFile": "file:///tmp/so.proto"
}
```

### Parameters

- `name`: The unique identifier for the schema.
- `content` or `file`: The schema definition content or URI.
  - `file`: Can point to a standalone `.proto` file or a `.zip` archive containing a primary schema and supporting files:

```text
test.zip/
├── test.proto  (Primary schema file)
└── test/       (Optional directory for imported schemas)
    ├── helper.proto
    └── config.json
```

- `soFile`: The compiled static plugin shared library for custom formats. Refer to [Serialization Format Extensions](../../guide/serialization/serialization.md#format-extension).

## Upload a Schema File

Use multipart form data to upload a schema directly from a local file:

```http
PUT http://localhost:9081/schemas/{type}/{name}/upload
```

Example `curl` request:

```bash
curl -X PUT http://localhost:9081/schemas/protobuf/schema1/upload \
  -F "file=@/path/to/schema1.proto"
```

The multipart form accepts a required `file` parameter and an optional `version` field.

The endpoint returns `201 Created` if creating a new schema, or `200 OK` if updating an existing schema.

Response sample:

```json
{
  "type": "protobuf",
  "name": "schema1"
}
```

## Show Schemas

Use this endpoint to list all registered schemas of a specified type:

```http
GET http://localhost:9081/schemas/protobuf
```

Response sample:

```json
["schema1", "schema2"]
```

## Describe a Schema

Use this endpoint to display the complete definition and file path of a schema:

```http
GET http://localhost:9081/schemas/protobuf/{name}
```

Response sample:

```json
{
  "type": "protobuf",
  "name": "schema1",
  "content": "message Book {required string title = 1; required int32 price = 2;}",
  "file": "ekuiper/etc/schemas/protobuf/schema1.proto"
}
```

## Delete a Schema

Use this endpoint to delete a registered schema:

```http
DELETE http://localhost:9081/schemas/protobuf/{name}
```

> [!NOTE]
> **Compatibility Note: 404 Not Found vs 400 Bad Request for Missing Schemas**
> When describing (`GET /schemas/{type}/{name}`) or deleting (`DELETE /schemas/{type}/{name}`) a schema name that does not exist, legacy eKuiper returns `400 Bad Request`. `rekuiper` strictly returns `404 Not Found` per RFC 9110 HTTP semantics.
> 
> **Why we chose this difference**: Returning `400 Bad Request` indicates to HTTP clients that the request URI or request payload was invalid, obscuring whether the endpoint failed or the schema was absent. Emitting `404 Not Found` enables clients to accurately determine resource absence and handle automated schema sync or creation cleanly.

## Update a Schema


Use this endpoint to update an existing schema definition:

```http
PUT http://localhost:9081/schemas/protobuf/{name}
Content-Type: application/json

{
  "name": "schema2",
  "file": "http://example.com/test2.proto"
}
```

## Schema Versioning

Schemas support an optional `version` string field. When updating a schema, the engine applies updates only if the new version string is lexically greater than the existing version string. For version comparison details, refer to [Versioning Logic](../../guide/rules/overview.md#versioning-logic).

Example versioned request payload:

```json
{
  "name": "schema2",
  "file": "file:///tmp/test2.proto",
  "version": "1756436910"
}
```
