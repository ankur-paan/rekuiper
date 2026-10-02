# Sources

Sources read data from external systems. A source can be an unbounded data stream or a bounded dataset named a table. Rules require at least one stream source.

A source defines connection parameters to external resources and ingests streaming data. Configured properties control decoding and schema transformation.

## Definition and Execution

Creating a stream or table creates a logical definition rather than an active physical connection. Rules reference this logical definition in the `FROM` clause. The source starts only when an associated rule starts.

By default, if multiple rules reference the same source, each rule instantiates an isolated source instance. To optimize performance across multiple rules, configure the source as a [shared stream](../../guide/streams/overview.md#share-source-instance-across-rules).

## Decoding

Specify the data format with the `format` property. Supported formats include `json`, `binary`, `protobuf`, and `delimited`. You can also implement custom decoding by setting `format` to `custom`.

## Schema

You can define a source schema similar to a database table. Some formats, such as `protobuf`, contain schema definitions. When configuring a source, set `schemaId` to reference a schema in the Schema Registry.

The schema registry contains the physical schema. The SQL data source definition contains the logical schema.

When both schemas exist, the physical schema overrides the logical schema. The format implementation, such as `protobuf`, performs data validation and formatting.

If only the logical schema is defined with `strictValidation: true`, the runtime validates data and converts data types. If validation is disabled, the logical schema validates SQL statements at compile time.

If input data is pre-cleaned or variable, omit the schema definition to avoid conversion overhead.

## Stream and Table

A source connects to an external system. Configure the source as a stream or a table based on processing requirements. Refer to [Stream](stream.md) and [Table](table.md) for details.

## Further Reading

- [Source Reference](../../guide/sources/overview.md)

