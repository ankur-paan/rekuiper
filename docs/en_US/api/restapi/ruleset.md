# Ruleset Management

The rekuiper REST API imports and exports bulk stream, table, and rule configurations.

## Ruleset Format

Rulesets use JSON formatting. The document contains three top-level maps: `streams`, `tables`, and `rules`. Each map contains key-value pairs of resource names and their SQL or JSON creation definitions.

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

Use this endpoint to import a ruleset into the server. If a stream, table, or rule already exists, the server skips creating that duplicate item. The API returns a message reporting the count of created resources.

### Import Using Inline Content

```http
POST http://localhost:9081/ruleset/import
Content-Type: application/json

{
  "content": "{\"streams\":{\"demo\":\"CREATE STREAM demo () WITH (DATASOURCE=\\\"users\\\", FORMAT=\\\"JSON\\\")\"},\"tables\":{},\"rules\":{}}"
}
```

### Import Using a File URI

```http
POST http://localhost:9081/ruleset/import
Content-Type: application/json

{
  "file": "file:///tmp/a.json"
}
```

## Export Ruleset

Use this endpoint to export active rulesets as a downloadable JSON file:

```http
POST http://localhost:9081/ruleset/export
```
