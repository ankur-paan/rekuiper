# Schema Registry

The rekuiper schema CLI manages data schemas. You can register, list, describe, and drop schema definitions.

## Register a Schema

Use this command to register a new schema using a JSON specification:

```shell
create schema $schema_type $schema_name $schema_json
```

You can define the schema through two methods:

### Specify Schema Content Inline

```shell
# bin/kuiper create schema protobuf schema1 '{"name": "schema1","content": "message Book {required string title = 1; required int32 price = 2;}"}'
```

This command creates a schema named `schema1` with inline protobuf definitions.

### Specify a Schema File URI

```shell
# bin/kuiper create schema protobuf schema1 '{"name": "schema1","file": "file:///tmp/aschema.proto"}'
```

This command registers `schema1` by importing the file at the specified URI into `data/schemas/protobuf/schema1.proto`.

### Parameters

- `schema_type`: The schema format. Currently, the supported format is `protobuf`.
- `schema_name`: The unique identifier for the schema and file name.
- `schema_json`: A JSON string defining the schema. It must include `name` and either `content` or `file`.

## Show Schemas

Use this command to display all registered schemas for a schema type:

```shell
show schemas $schema_type
```

Example output:

```shell
# bin/kuiper show schemas protobuf
schema1
schema2
```

## Describe a Schema

Use this command to display the definition of a schema:

```shell
describe schema $schema_type $schema_name
```

Example output:

```shell
# bin/kuiper describe schema protobuf schema1
{
  "type": "protobuf",
  "name": "schema1",
  "content": "message Book {required string title = 1; required int32 price = 2;}",
  "file": "ekuiper\\etc\\schemas\\protobuf\\schema1.proto"
}
```

## Drop a Schema

Use this command to delete a schema definition:

```shell
drop schema $schema_type $schema_name
```

Active rules retain loaded schemas in memory until restarted.

Example command:

```shell
# bin/kuiper drop schema protobuf schema1
Schema schema1 is dropped.
```
