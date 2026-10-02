# Ruleset Management

The rekuiper CLI provides commands to import and export entire collections of streams, tables, and rules.

## Ruleset Format

Rulesets use JSON formatting. The JSON document contains three top-level maps: `streams`, `tables`, and `rules`. Each map contains key-value pairs of object names and their creation statements.

Example ruleset JSON:

```json
{
    "streams": {
        "demo": "CREATE STREAM demo () WITH (DATASOURCE=\"users\", FORMAT=\"JSON\")"
    },
    "tables": {},
    "rules": {
        "rule1": "{\"id\": \"rule1\",\"sql\": \"SELECT * FROM demo\",\"actions\": [{\"log\": {}}]}",
        "rule2": "{\"id\": \"rule2\",\"sql\": \"SELECT * FROM demo\",\"actions\": [{  \"log\": {}}]}"
    }
}
```

## Import Ruleset

Use this command to import a ruleset file into the server. If a stream, table, or rule already exists, the server skips creating that duplicate item. The engine starts imported rules immediately. The CLI outputs the count of created resources.

```shell
# bin/kuiper import ruleset -f myrules.json
```

## Export Ruleset

Use this command to export active streams, tables, and rules into a specified file. The CLI outputs the count of exported resources.

```shell
# bin/kuiper export ruleset myrules.json
```
