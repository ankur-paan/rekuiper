# Data Import and Export Management

The rekuiper CLI manages system-wide configuration data import and export operations.

## Data Format

System data files use JSON format. The document can contain `streams`, `tables`, `rules`, `nativePlugins`, `portablePlugins`, `sourceConfig`, `sinkConfig`, `connectionConfig`, `Service`, `Schema`, `uploads`, and `scripts`.

Example configuration export:

```json
{
  "streams": {
    "demo": "CREATE STREAM demo () WITH (DATASOURCE=\"users\", FORMAT=\"JSON\")"
  },
  "tables": {
    "T110": "\n CREATE TABLE T110\n (\n S1 string\n )\n WITH (DATASOURCE=\"test.json\", FORMAT=\"json\", TYPE=\"file\", KIND=\"scan\", );\n "
  },
  "rules": {
    "rule1": "{\"id\": \"rule1\",\"sql\": \"SELECT * FROM demo\",\"actions\": [{\"log\": {}}]}",
    "rule2": "{\"id\": \"rule2\",\"sql\": \"SELECT * FROM demo\",\"actions\": [{  \"log\": {}}]}"
  },
  "nativePlugins": {
    "functions_image": "{\"name\":\"image\",\"file\":\"https://example.com/plugins/image_amd64.zip\",\"shellParas\":[]}",
    "sources_video": "{\"name\":\"video\",\"file\":\"https://example.com/plugins/video_amd64.zip\",\"shellParas\":[]}"
  },
  "portablePlugins": {},
  "sourceConfig": {
    "mqtt": "{\"td\":{\"insecureSkipVerify\":false,\"password\":\"public\",\"protocolVersion\":\"3.1.1\",\"qos\":1,\"server\":\"tcp://10.0.0.1:1883\",\"username\":\"admin\"},\"test\":{\"insecureSkipVerify\":false,\"password\":\"public\",\"protocolVersion\":\"3.1.1\",\"qos\":1,\"server\":\"tcp://127.0.0.1:1883\",\"username\":\"admin\"}}"
  },
  "sinkConfig": {
    "edgex": "{\"test\":{\"bufferLength\":1024,\"contentType\":\"application/json\",\"enableCache\":false,\"format\":\"json\",\"messageType\":\"event\",\"omitIfEmpty\":false,\"port\":6379,\"protocol\":\"redis\",\"sendSingle\":true,\"server\":\"localhost\",\"topic\":\"application\",\"type\":\"redis\"}}"
  },
  "connectionConfig": {},
  "Service": {},
  "Schema": {},
  "uploads": {},
  "scripts": {}
}
```

## Reset and Import Data

Use this command to reset all existing system data and then import configuration from a file:

```shell
# bin/kuiper import data -f myrules.json -s false
```

## Import Data

Use this command to import and merge configuration data into the system. This overwrites existing tables, streams, rules, and configurations, and installs missing schemas and plugins:

```shell
# bin/kuiper import data -f myrules.json -p true
```

## Inspect Import Status

Use this command to check for errors encountered during data import. If the output is empty, all resources imported successfully:

```shell
# bin/kuiper getstatus import
```

## Export Data

Use this command to export all system data to a JSON file:

```shell
# bin/kuiper export data myrules.json
```

Use the `-r` option to export data associated with specific rules:

```shell
# bin/kuiper export data myrules.json -r '["rules1", "rules2"]'
```
