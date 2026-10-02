# File Source Connector

<span style="background:green;color:white;padding:1px;margin:2px">stream source</span>
<span style="background:green;color:white;padding:1px;margin:2px">scan table source</span>

The File Source connector reads file content into the rekuiper stream processing pipeline.

The connector supports batch file processing and real-time directory monitoring. When monitoring a directory, all files in the directory must share the same format. The engine processes files in alphabetical order by filename.

## Supported File Types

The file connector supports the following file structures:

- **JSON**: Files containing standard JSON arrays.
- **CSV**: Comma-separated or custom-delimited tabular text files.
- **Lines**: Text files with one record per line.
- **Parquet**: Columnar Apache Parquet files using Arrow schema inference.
- **Raw**: Reads the entire file as a single binary payload.

### JSON Array Example

```json
[
  {"id": 1, "name": "John Doe"},
  {"id": 2, "name": "Jane Smith"}
]
```

> [!NOTE]
> If a file contains multiple JSON records separated by newlines, set `fileType` to `"lines"` and `FORMAT` to `"json"`.

### CSV Example

```csv
id,name,age
1,John Doe,30
2,Jane Smith,25
```

Custom separators (such as spaces or semicolons) are supported:

```csv
id name age
1 John Doe 30
2 Jane Smith 25
```

### Lines Example

Each line represents a distinct event record:

```text
{"id": 1, "name": "John Doe"}
{"id": 2, "name": "Jane Smith"}
```

You can combine `lines` with binary formats. For example, set `FORMAT` to `"protobuf"` and provide a schema to parse newline-separated Protobuf messages.

## Configuration Parameters

Configure the connector in `etc/sources/file.yaml`:

```yaml
default:
  fileType: json
  path: data
  interval: 0
  sendInterval: 0
  actionAfterRead: 0
  moveTo: /tmp/kuiper/moved
  hasHeader: false
  # columns: [id, name]
  ignoreStartLines: 0
  ignoreEndLines: 0
  decompression: ""
```

### File Type and Directory

- `fileType`: File format: `"raw"`, `"json"`, `"csv"`, or `"lines"`. When using `"raw"`, set the stream format to `"binary"`.
- `path`: Directory path relative to the rekuiper root directory, or an absolute path. Do not include filenames here; specify filenames in `DATASOURCE`.

### Reading and Sending Intervals

- `interval`: Polling interval in milliseconds. Setting `interval: 0` activates filesystem change monitoring instead of polling. When files change or new files appear, the engine reads them immediately.
- `sendInterval`: Delay in milliseconds between emitted event records.

### Post-Read Actions

- `actionAfterRead`: Defines file handling after reading completes:
  - `0`: Keep the file.
  - `1`: Delete the file.
  - `2`: Move the file to the path specified in `moveTo`.
- `moveTo`: Target directory path when `actionAfterRead` is set to `2`.

### CSV Parsing Options

- `hasHeader`: Boolean indicating whether the first row contains column headers.
- `columns`: List of column names when files lack headers (for example, `columns: [id, name]`).
- `ignoreStartLines`: Number of lines to skip at the beginning of the file. Empty lines are ignored and not counted.
- `ignoreEndLines`: Number of lines to skip at the end of the file.

### Decompression

- `decompression`: Decompresses incoming archive files. Supported algorithms: `"gzip"` and `"zstd"`.

## Create a Table Source

The file source commonly operates as a scan table for static reference lookups:

```sql
CREATE TABLE table1 (
    name STRING,
    size BIGINT,
    id BIGINT
) WITH (DATASOURCE = "lookup.json", FORMAT = "json", TYPE = "file");
```

Create a rule that joins stream events with the file table:

```sql
CREATE RULE rule1 AS SELECT * FROM fileDemo WHERE temperature > 50 INTO mySink;
```

To manage tables through the REST API or CLI, refer to [Tables Management with REST API](../../../api/restapi/tables.md) and [Tables Management with CLI](../../../api/cli/tables.md).

## Configuration Tutorials

### Tutorial 1: Parse Space-Delimited CSV Files

Consider this space-separated data file:

```csv
id name age
1 John 56
2 Jane 34
```

1. Configure `etc/sources/file.yaml`:

   ```yaml
   csv:
     fileType: csv
     hasHeader: true
   ```

2. Create a stream using the `DELIMITED` format and specify a space character delimiter:

   ```sql
   CREATE STREAM csvFileDemo () WITH (
     FORMAT = "DELIMITED",
     DATASOURCE = "abc.csv",
     TYPE = "file",
     DELIMITER = " ",
     CONF_KEY = "csv"
   );
   ```

### Tutorial 2: Parse Multi-Line JSON Files

Consider a file with multiple JSON objects separated by newlines:

```text
{"id": 1, "name": "John Doe"}
{"id": 2, "name": "Jane Doe"}
{"id": 3, "name": "John Smith"}
```

1. Configure `etc/sources/file.yaml`:

   ```yaml
   jsonlines:
     fileType: lines
   ```

2. Define a stream with `FORMAT = "JSON"`:

   ```sql
   CREATE STREAM linesFileDemo () WITH (
     FORMAT = "JSON",
     TYPE = "file",
     CONF_KEY = "jsonlines"
   );
   ```

### Tutorial 3: Monitor a Directory for New Binary Files

This scenario monitors `data/watch` for new image files and publishes the raw binary payloads to MQTT.

#### Step 1: Create the Monitoring Configuration

Create a configuration named `watch` using the REST API:

```http
PUT http://{{host}}/metadata/sources/file/confKeys/watch
Content-Type: application/json

{
  "interval": 0,
  "fileType": "raw",
  "path": "data"
}
```

- `interval: 0` activates filesystem event notifications.
- `fileType: "raw"` reads file content as unparsed binary bytes.
- `path: "data"` sets the base directory.

#### Step 2: Create the Stream

Create the stream using the REST API:

```http
POST http://{{host}}/streams
Content-Type: application/json

{
  "sql": "CREATE STREAM watch() WITH (TYPE=\"file\", FORMAT=\"binary\", DATASOURCE=\"watch\", CONF_KEY=\"watch\", SHARED=\"true\");"
}
```

The engine monitors the combined path `data/watch`.

#### Step 3: Create the Ingestion Rule

Create a rule to forward binary payloads to MQTT:

```http
POST http://{{host}}/rules
Content-Type: application/json

{
  "id": "ruleWatch",
  "name": "Watch image folder and send raw binary data to MQTT",
  "sql": "SELECT self FROM watch",
  "actions": [
    {
      "mqtt": {
        "server": "tcp://127.0.0.1:1883",
        "topic": "result",
        "sendSingle": true,
        "format": "binary"
      }
    }
  ]
}
```

#### Step 4: Validate File Monitoring

Subscribe to MQTT topic `result`. Copy an image file into `data/watch`. The engine reads the image file and publishes the binary data to the MQTT broker.
