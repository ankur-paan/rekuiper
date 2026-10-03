# Stream Management

A stream represents the runtime instance of an unbounded source connector in rekuiper. The stream definition specifies how the runtime connects to an external resource.

Stream sources must be unbounded. The stream triggers rule calculations. Each incoming event triggers an evaluation cycle in the rule.

Unlike relational databases, rekuiper does not require a predefined schema. This design supports schemaless data, which is common in IoT and edge computing environments. When you process fixed-structure streams, you can define a schema. A schema enables compile-time SQL validation and query optimization. Schemaless mode skips data validation during ingestion, which increases throughput performance.

## Stream Definition

Use the following SQL syntax to define a stream:

```sql
CREATE STREAM 
    stream_name 
    ( column_name <data_type> [ ,...n ] )
    WITH ( property_name = expression [, ...] );
```

A stream definition contains two parts:

1. **Schema definition**: Uses standard SQL table definition syntax. The schema is optional. If you omit column definitions, the stream operates in schemaless mode.
2. **Properties in the `WITH` clause**: Configures the connector type, serialization format, and runtime behaviors.

### Schema in Stream Definition

Schema definitions are optional. Use a schema when ingested data has a fixed structure and requires strict validation.

When the source format is JSON, a defined schema instructs rekuiper to parse only the specified fields. If incoming payloads are large or complex, selective parsing reduces CPU consumption and improves performance.

In rekuiper, each column has an assigned data type. A data type defines and constrains the valid values for a column.

rekuiper supports the following data types:

| # | Data Type | Description |
|---|-----------|-------------|
| 1 | `bigint` | Integer values. |
| 2 | `float` | Floating-point numerical values. |
| 3 | `string` | Unicode text characters. |
| 4 | `datetime` | Date and time values. |
| 5 | `boolean` | Boolean values (`true` or `false`). |
| 6 | `bytea` | Binary byte sequences. In JSON streams, encode binary data as a base64 string. |
| 7 | `array` | Array collections containing simple types or nested arrays. |
| 8 | `struct` | Key-value composite structures. |

You can add a `DEFAULT` clause to a column definition. The engine inserts the default value if the field is absent from the incoming record. rekuiper evaluates default values only when strict validation is enabled. The `DEFAULT` clause supports only `STRING`, `BOOLEAN`, `FLOAT`, and `BIGINT` types.

### Stream Properties

Configure stream properties in the `WITH` clause:

| Property Name | Optional | Description |
|---|---|---|
| `DATASOURCE` | False | Specifies the source topic or path. For MQTT sources, specify the MQTT topic name. |
| `FORMAT` | True | Data serialization format: `"JSON"`, `"PROTOBUF"`, or `"BINARY"`. Default is `"JSON"`. Refer to [Binary Stream](#binary-stream). |
| `SCHEMAID` | True | Schema identifier for decoding events. Required when `FORMAT` is `"PROTOBUF"`. |
| `DELIMITER` | True | Delimiter character when using delimited formats. Default is a comma (`,`). |
| `KEY` | True | Reserved identifier for stream partitioning. |
| `TYPE` | True | Source connector type. Default is `"mqtt"`. |
| `StrictValidation` | True | Controls schema validation for incoming records. Refer to [Strict Validation](#strict-validation). |
| `CONF_KEY` | True | Specifies the configuration section in the source YAML file. Refer to [CONF_KEY Configuration](#conf_key-configuration). |
| `EXTRA` | True | JSON string that overrides source configuration properties. Refer to [EXTRA Configuration](#extra-configuration). |
| `SHARED` | True | Specifies whether rules share a single source instance. Default is `false`. |
| `TIMESTAMP` | True | Specifies the field containing event timestamps. When defined, rules execute in event-time mode. Otherwise, rules run in processing-time mode. Refer to [Time in Stream Processing](../../concepts/streaming/time.md). |
| `TIMESTAMP_FORMAT` | True | Default format string used to parse or convert datetime fields. |
| `VERSION` | True | Version identifier for update management. Refer to [Versioning](#versioning). |
| `TEMP` | True | Specifies whether the stream is temporary. Temporary streams reside in memory only. Default is `false`. Refer to [Temporary Streams](#temporary-streams). |
| `BUFFER_FULL_POLICY` | True | Controls queue behavior when the buffer is full: `"block"` or `"dropOldest"`. Refer to [Buffer Full Policy](#buffer-full-policy). |

### Buffer Full Policy

When an input stream receives records faster than the rule can process them, the internal buffer fills up.
Use the `BUFFER_FULL_POLICY` property to configure buffer behavior:

- `"block"`: Pauses stream ingestion. The source waits until downstream processing frees buffer space.
- `"dropOldest"`: Drops the oldest record from the queue to store the newly arrived record.

If you specify any other value, the server rejects the request with a validation error.

#### Example 1: MQTT Stream with Basic Types

```sql
CREATE STREAM my_stream
  (id bigint, name string, score float)
WITH ( datasource = "topic/temperature", FORMAT = "json", KEY = "id");
```

This stream subscribes to the MQTT topic `topic/temperature`. The connection uses the `default` section of `$rekuiper/etc/mqtt_source.yaml`. For details, refer to the [MQTT Source Connector](../sources/builtin/mqtt.md).

#### Example 2: MQTT Stream with Complex Types

```sql
CREATE STREAM demo (
    USERID BIGINT,
    FIRST_NAME STRING,
    LAST_NAME STRING,
    NICKNAMES ARRAY(STRING),
    Gender BOOLEAN,
    ADDRESS STRUCT(STREET_NAME STRING, NUMBER BIGINT)
) WITH (DATASOURCE="test/", FORMAT="JSON", KEY="USERID", CONF_KEY="demo");
```

This stream subscribes to the MQTT topic `test/`. The connection uses the `demo` section of `$rekuiper/etc/mqtt_source.yaml`.

#### Example 3: Protobuf Stream

```sql
CREATE STREAM demo () WITH (DATASOURCE="test/", FORMAT="protobuf", SCHEMAID="proto1.Book");
```

This stream subscribes to the MQTT topic `test/` and decodes payloads with Protocol Buffers. The engine uses the `Book` message schema from `$rekuiper/data/schemas/protobuf/schema1.proto`. For schema administration, refer to [Schema Registry](../../api/restapi/schemas.md).

To manage streams through the command line, refer to the [CLI Overview](../../api/cli/overview.md).

## Configuration Options

Configure stream properties through two mechanisms:

1. **`CONF_KEY`**: References a configuration section in the source YAML configuration file.
2. **`EXTRA`**: Provides inline JSON properties that override settings from `CONF_KEY`.

### CONF_KEY Configuration

Source configuration files reside at `$rekuiper/etc/sources/{$source_type}.yaml`. Each file can define multiple configuration groups:

```yaml
default:
  conf1: value
  conf2: 2
myconf:
  conf1: value2
  conf2: 3
myconf2:
  conf1: value3
  conf2: 2
```

To apply the `myconf` configuration group, set `CONF_KEY="myconf"`:

```sql
CREATE STREAM demo () WITH (DATASOURCE="topic", CONF_KEY="myconf");
```

### EXTRA Configuration

The `EXTRA` property overrides specific configuration values directly in the SQL statement. In the following example, `EXTRA` changes the value of `conf2` from `3` to `1`:

```sql
CREATE STREAM demo () WITH (DATASOURCE="topic", CONF_KEY="myconf", EXTRA="{\"conf2\":1}");
```

The resulting configuration is `{"conf1":"value2","conf2":1}`.

## Shared Source Instances

By default, each rule instantiates an independent source instance.

When multiple rules process identical stream data, independent source instances create separate network connections. Variations in network latency can cause rules to receive data in different sequences.

A shared source instance resolves this issue. All rules connect to one source instance and process identical records in the same order. Sharing the source instance also decreases memory and network overhead.

To share a source instance, set `SHARED="true"`:

```sql
CREATE STREAM demo () WITH (DATASOURCE="test", FORMAT="JSON", KEY="USERID", SHARED="true");
```

When rules share a source instance, they form a unified execution topology. The runtime manages the shared source and downstream rules as a single composite rule.

> [!WARNING]
> Because shared streams create coupled topologies, source-level state checkpointing cannot run on shared sources.

## Schema Modes

rekuiper supports three schema modes:

1. **Schemaless**: The stream does not define a schema. Use this mode for unstructured data or rapidly changing payloads.
2. **Logical schema**: The stream defines columns in the SQL DDL statement without a schema registry file. Use this mode with formats like JSON. Enable `StrictValidation` to enforce field validation.
3. **Physical schema**: Strongly typed formats (such as Protobuf or custom formats) use `SCHEMAID`. The format plugin enforces binary payload validation.

The engine uses logical and physical schemas for SQL validation during rule compilation and for query plan optimization. To view the inferred schema of a stream, use the [Get Stream Schema API](../../api/restapi/streams.md#get-stream-schema).

### Strict Validation

Strict validation applies to streams that have logical schemas. When enabled, rekuiper verifies field existence and validates field data types against the schema. If incoming data is known to match the schema, disable strict validation to maximize ingestion speed.

### Schemaless Streams

If incoming data structures vary, define the stream without column definitions:

```sql
CREATE STREAM schemaless_stream ()
WITH (DATASOURCE = "topic/temperature", FORMAT = "json", KEY = "id");
```

In schemaless streams, rekuiper resolves field data types at runtime. If a query applies an invalid operation to a dynamic field (such as `WHERE temperature > 30` when `temperature` contains a string), the engine generates an error and routes it to the sink.

For SQL syntax details, refer to [Query Language Elements](../../sqls/query_language_elements.md).

### Binary Streams

Use `FORMAT = "BINARY"` for raw binary payloads such as image, audio, or video streams.

Binary payloads contain continuous byte arrays without discrete fields. Define the stream with a single `bytea` column:

```sql
CREATE STREAM demoBin (
    image BYTEA
) WITH (DATASOURCE="test/", FORMAT="BINARY");
```

If you define a binary stream without columns, rekuiper automatically maps the binary payload to a field named `self`.

## Temporary Streams

Temporary streams reside only in memory and do not persist to disk. Use temporary streams for intermediate data processing and testing.

Temporary streams have these properties:

- **In-memory storage**: Stream definitions disappear when rekuiper restarts.
- **Immutable definition**: The `REPLACE STREAM` statement cannot modify temporary streams.
- **Rule restriction**: Only temporary rules can query temporary streams. Persistent rules cannot access temporary streams.

### Create a Temporary Stream

To create a temporary stream, set `TEMP="true"`:

```sql
CREATE STREAM temp_sensor (
    temperature FLOAT,
    humidity FLOAT
) WITH (DATASOURCE="sensor/data", FORMAT="json", TEMP="true");
```

## Versioning

You can configure an optional `VERSION` property to control stream updates.

When you update a stream, the engine compares the new version string to the existing version string. The engine accepts the update only if the new version is lexically greater than the current version. The comparison evaluates characters sequentially. For details, refer to [Versioning Logic](../../guide/rules/overview.md#versioning-logic).

```sql
CREATE STREAM version_stream ()
WITH (DATASOURCE = "topic", FORMAT = "json", VERSION = "1756436910");
```

### Stream with Default Values Example

```sql
CREATE STREAM demo (
    temperature FLOAT DEFAULT 12.0,
    status STRING DEFAULT "unknown",
    active BOOLEAN DEFAULT false,
    user_id BIGINT DEFAULT 2
) WITH (DATASOURCE="sensor/data", FORMAT="JSON", STRICT_VALIDATION="true");
```
