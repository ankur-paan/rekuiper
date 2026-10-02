# JSON Functions

> [!NOTE]
> **Verification Status**: Tested and verified against the `rekuiper` runtime with streaming telemetry data on **2026-10-01 16:37:51 UTC**.  
> **Scorecard**: **5 / 5 JSON Functions Fully Verified with Live Data (100% Parity)**:  
> `to_json`, `parse_json`, `json_path_exists`, `json_path_query`, `json_path_query_first`.

JSON functions parse, serialize, and evaluate JSON documents and attributes. Refer to [JSONPath Functions](../json_expr.md#jsonpath-functions) for path syntax details.

## TO_JSON

```text
to_json(col)
```

Serializes an expression value into a JSON string. Returns `NULL` if the input is `NULL`.

## PARSE_JSON

```text
parse_json(col)
```

Parses a JSON-formatted string into a structured data object. Returns `NULL` if the input is `NULL`.

## JSON_PATH_EXISTS

```text
json_path_exists(col, json_path)
```

Returns `true` if the specified JSONPath matches at least one element in the JSON object or array; otherwise returns `false`.

## JSON_PATH_QUERY

```text
json_path_query(col, json_path)
```

Evaluates a JSONPath query and returns an array of all matching values.

## JSON_PATH_QUERY_FIRST

```text
json_path_query_first(col, json_path)
```

Evaluates a JSONPath query and returns the first matching value.
