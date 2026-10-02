# Streams Management

The rekuiper stream command-line interface manages stream definitions. You can create, describe, list, drop, and query streams.

## Create a Stream

Use this command to create a new stream. For syntax details, refer to [Streams](../../sqls/streams.md).

```shell
create stream $stream_name '$stream_def' | create stream -f $stream_def_file
```

### Specify the Stream Definition on the Command Line

Example command:

```shell
# bin/kuiper create stream my_stream '(id bigint, name string, score float) WITH ( datasource = "topic/temperature", FORMAT = "json", KEY = "id")'
stream my_stream created
```

This command creates a stream named `my_stream`.

### Specify the Stream Definition in a File

Use the `-f` flag to load complex stream definitions from a text file:

```shell
# bin/kuiper create stream -f /tmp/my_stream.txt
stream my_stream created
```

The file `/tmp/my_stream.txt` contains:

```sql
my_stream(id bigint, name string, score float)
    WITH ( datasource = "topic/temperature", FORMAT = "json", KEY = "id");
```

## Show Streams

Use this command to display all streams defined in the server:

```shell
show streams
```

Example command:

```shell
# bin/kuiper show streams
my_stream
```

## Describe a Stream

Use this command to display the schema and configuration options of a stream:

```shell
describe stream $stream_name | describe stream $stream_name -json
```

Example standard output:

```shell
# bin/kuiper describe stream my_stream
Fields
--------------------------------------------------------------------------------
id  bigint
name  string
score  float

FORMAT: json
KEY: id
DATASOURCE: topic/temperature
```

Use the `-json` option to format the output as JSON:

```shell
# bin/kuiper describe stream my_stream -json
'{
    "Fields": [
        {
            "Name": "id",
            "Type": "bigint"
        },
        {
            "Name": "name",
            "Type": "string"
        },
        {
            "Name": "score",
            "Type": "float"
        }
    ],
    "Options": {
        "DATASOURCE:": "topic/temperature",
        "FORMAT:": "json",
        "KEY:": "id"
    }
}'
```

## Drop a Stream

Use this command to delete a stream definition:

```shell
drop stream $stream_name
```

Example command:

```shell
# bin/kuiper drop stream my_stream
stream my_stream dropped
```

## Query Against Streams

Use this command to run interactive SQL queries against streams from the console:

```shell
query
```

Example interactive session:

```shell
# bin/kuiper query
kuiper >
```

After the `kuiper >` prompt displays, enter an SQL statement (refer to [rekuiper SQL Reference](../../sqls/overview.md)) and press Enter.

The CLI displays the results in the terminal:

```shell
kuiper > SELECT * FROM my_stream WHERE id > 10;
[{"...":"..." ....}]
...
```

- Press `CTRL + C` to terminate the active query.
- Enter `quit` or `exit` to exit the interactive prompt console.
