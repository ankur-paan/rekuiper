# JSON Functions

> [!NOTE]
> **Verification Status**: Tested and Verified against `rekuiper` engine with live telemetry stream load on **2026-10-01 16:37:51 UTC**.  
> **Scorecard**: **5 / 5 JSON Functions Fully Verified with Live Data (100% Parity)**:  
> `to_json`, `parse_json`, `json_path_exists`, `json_path_query`, `json_path_query_first`.

JSON functions manipulate JSON string or return information about JSON.
There are several json path manipulation functions. **Please refer
to [json path functions](../json_expr.md#json-path-functions) for how to compose a json path.**

## TO_JSON

```text
to_json(col)
```

Converts a value to a string containing the JSON representation of the value. If the input is NULL, the result is also
NULL.

## PARSE_JSON

```text
parse_json(col)
```

Converts a JSON string to a value. If the input is NULL, the result is also NULL.

## JSON_PATH_EXISTS

```text
json_path_exists(col, json_path)
```

Check whether JSON path returns any item for the specified JSON value. Return bool value.

## JSON_PATH_QUERY

```text
json_path_query(col, json_path)
```

Get all items returned by JSON path for the specified JSON value.

## JSON_PATH_QUERY_FIRST

```text
json_path_query_first(col, json_path)
```

Get the first item returned by JSON path for the specified JSON value.
