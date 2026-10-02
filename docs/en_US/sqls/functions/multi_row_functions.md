# Multiple Row Functions

> [!NOTE]
> **Verification Status**: Tested and Verified against `rekuiper` engine with live telemetry stream load on **2026-10-01 17:56:45 UTC**.  
> **Scorecard**: **2 Verified, 0 Unsupported, 0 Broken**:  
> - **Verified (2)**: `unnest`, `extract`.

A multiple-row function expands input data structures into multiple output rows.

You can use multiple-row functions only in the `SELECT` clause of a query. The engine currently permits only one multiple-row function per `SELECT` clause.

## UNNEST

```text
unnest(array)
```

Expands an array into multiple output rows. The argument must evaluate to an array.

When array elements are objects (`map[string]interface{}`), the function converts the object fields into distinct columns in the resulting rows.

### Examples

Create a stream named `demo` with the following input record:

```json lines
{
  "a": [
    1,
    2
  ],
  "b": 3
}
```

Rule to unnest array values:

```text
SQL: SELECT unnest(a) FROM demo
___________________________________________________
{"unnest":1}
{"unnest":2}
```

Rule to unnest array values with adjacent columns:

```text
SQL: SELECT unnest(a), b FROM demo
___________________________________________________
{"unnest":1, "b":3}
{"unnest":2, "b":3}
```

Create a stream named `demo` with objects inside an array:

```json lines
{
  "x": [
    {
      "a": 1,
      "b": 2
    },
    {
      "a": 3,
      "b": 4
    }
  ],
  "c": 5
}
```

Rule to unnest array objects into columns:

```text
SQL: SELECT unnest(x), c FROM demo
___________________________________________________
{"a":1, "b":2, "c": 5}
{"a":3, "b":4, "c": 5}
```

## EXTRACT

```text
extract(map[string]interface{})
```

Expands a map or JSON object and flattens its key-value pairs into individual columns on the current row. The argument must evaluate to a map object.

### Example

Create a stream named `demo` with the following input record:

```json lines
{
  "data": {
    "k1": "v1",
    "k2": "v2"
  }
}
```

Rule to extract map fields into row columns:

```text
SQL: SELECT extract(data) FROM demo
__________________________________________________
{"k1":"v1","k2":"v2"}
```
