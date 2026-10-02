# Data Templates

Sink data templates transform query results before delivering payloads to external systems.

Different external targets require different payload formats. For example, an IoT rule can send a JSON webhook to a cloud REST API while simultaneously sending a binary control payload to a local device through MQTT. Data templates format results to match the requirements of each destination.

## Golang Template Overview

rekuiper uses the [Go text/template](https://golang.org/pkg/text/template/) engine to format output records.

A template executes against an input data structure. Actions inside double braces (<span v-pre>`{{`</span> and <span v-pre>`}}`</span>) evaluate fields, variables, or functions:

- The cursor dot (`.`) represents the current data element.
- Text outside actions passes unchanged to the output string.

## Input Data Granularity

The input data structure flowing into a sink is a slice of maps:

```go
[]map[string]interface{}
```

The `sendSingle` sink property controls template input granularity:

- **`sendSingle: true`**: The engine iterates through the record slice and applies the template to each map individually.
- **`sendSingle: false`**: The engine passes the entire record array to the template.

### Example Input

```go
[]map[string]interface{}{
    {"ab": "hello1"},
    {"ab": "hello2"}
}
```

### Templates with `sendSingle: true`

- Output the entire record as JSON:

  ```json
  "dataTemplate": "{\"content\": {{json .}}}"
  ```

- Output field `ab`:

  ```json
  "dataTemplate": "{\"content\": {{.ab}}}"
  ```

- Output field `ab` with string quotation marks:

  ```json
  "dataTemplate": "{\"content\": \"{{.ab}}\"}"
  ```

### Templates with `sendSingle: false`

- Output the entire array as JSON:

  ```json
  "dataTemplate": "{\"content\": {{json .}}}"
  ```

- Output the first array element:

  ```json
  "dataTemplate": "{\"content\": {{json (index . 0)}}}"
  ```

- Output field `ab` of the first record:

  ```json
  "dataTemplate": "{\"content\": {{index . 0 \"ab\"}}}"
  ```

- Format all records as an HTML list:

  ```json
  "dataTemplate": "<div>results</div><ul>{{range .}}<li>{{.ab}}</li>{{end}}</ul>"
  ```

## Supported Template Functions

Templates support three function sets:

1. Standard [Go text/template functions](https://golang.org/pkg/text/template/#hdr-Functions).
2. Extended functions from the [Sprig library](http://masterminds.github.io/sprig/).
3. Built-in functions:
   - `toJson`: Converts maps or structures into valid JSON strings.
   - `b64enc`: Encodes string data as base64.

> [!NOTE]
> The legacy functions `json` and `base64` are deprecated. Use `toJson` and `b64enc` from the Sprig library.

## Control Actions

Templates support Go control actions:

### Conditional Logic

```text
{{if pipeline}} T1 {{else}} T0 {{end}}
```

Example in JSON output:

```text
{{if .condition}} {"field1": true} {{else}} {"field1": false} {{end}}
```

### Iteration

```text
{{range pipeline}} T1 {{else}} T0 {{end}}
```

## Practical Conversion Examples

### Example 1: Emitting Individual Records

Consider a window aggregation that outputs multiple records:

```json
[
  {"device_id": "1", "t_av": 36.25, "t_count": 4, "t_max": 80, "t_min": 10},
  {"device_id": "2", "t_av": 27.0, "t_count": 4, "t_max": 45, "t_min": 12}
]
```

To deliver records individually, configure `sendSingle: true` and `dataTemplate`:

```json
{
  "sendSingle": true,
  "dataTemplate": "{{toJson .}}"
}
```

The sink delivers two discrete messages:

```json
{"device_id": "1", "t_av": 36.25, "t_count": 4, "t_max": 80, "t_min": 10}
```

```json
{"device_id": "2", "t_av": 27.0, "t_count": 4, "t_max": 45, "t_min": 12}
```

### Example 2: Conditional Field Modification

This example adds a textual description based on average temperature (`t_av`):

- When `t_av < 30.0`, the description is `"Current temperature is $t_av, it's normal."`
- When `t_av >= 30.0`, the description is `"Current temperature is $t_av, it's high."`

Template configuration:

```json
{
  "sendSingle": true,
  "dataTemplate": "{\"device_id\": {{.device_id}}, \"description\": \"{{if lt .t_av 30.0}}Current temperature is {{.t_av}}, it's normal.{{else if ge .t_av 30.0}}Current temperature is {{.t_av}}, it's high.{{end}}\"}"
}
```

Comparison functions:
- `lt`: Less than.
- `ge`: Greater than or equal to.

> [!IMPORTANT]
> The second parameter in `lt` and `ge` must match the field data type. Because `t_av` is a float, pass `30.0` instead of `30`.

Resulting output:

```json
{"device_id": "1", "description": "Current temperature is 36.25, it's high."}
{"device_id": "2", "description": "Current temperature is 27.0, it's normal."}
```

### Example 3: Nested Array Iteration

Consider input records containing nested sensor arrays:

```json
{
  "device_id": "1",
  "values": [
    {"temperature": 10.5},
    {"temperature": 20.3},
    {"temperature": 30.3}
  ]
}
```

The sink must output a list of status strings: `"fine"` when `temperature <= 25.0`, and `"high"` when `temperature > 25.0`.

Template configuration:

```json
{
  "sendSingle": true,
  "dataTemplate": "{{$len := len .values}}{{$loopsize := add $len -1}}{\"device_id\": \"{{.device_id}}\", \"description\": [{{range $index, $ele := .values}}{{if le .temperature 25.0}}\"fine\"{{else if gt .temperature 25.0}}\"high\"{{end}}{{if eq $loopsize $index}}]{{else}},{{end}}{{end}}}"
}
```

Expanded view of iteration logic:

```text
{{range $index, $ele := .values}}
  {{if le .temperature 25.0}}
    "fine"
  {{else if gt .temperature 25.0}}
    "high"
  {{end}}
  {{if eq $loopsize $index}}
    ]
  {{else}}
    ,
  {{end}}
{{end}}
```

Resulting output:

```json
{"device_id": "1", "description": ["fine", "fine", "high"]}
```

## Interactions Between Configuration Properties

| Property | Responsibility |
|---|---|
| `sendSingle` | Controls input granularity to `dataTemplate`. When `false`, the template receives the entire array. When `true`, the engine invokes the template once per map. |
| `dataTemplate` | Transforms inputs selected by `sendSingle`. In batch mode, write the template for one batch item, not the entire batch. Output is treated as pre-encoded. |
| `format` | Defines serialization and batch framing. The JSON writer adds brackets and commas; delimited writers add newlines. |
| `batchSize` / `lingerInterval` | Defines when the sink flushes accumulated records. Batching does not alter input to `dataTemplate`. |

Pipeline order of execution:

```txt
Input Record Array
  --> sendSingle selects array or iterates maps
  --> dataTemplate transforms each element
  --> Format Writer frames transformed records
  --> batchSize / lingerInterval triggers payload emission
```

> [!WARNING]
> When batching is enabled, write `dataTemplate` to format a single element. Do not add outer array brackets or comma delimiters in the template. The batch writer adds framing automatically.

### Example: Record Transformation in JSON Batches

Given two records:

```json
[
  {"id": 1, "temperature": 20},
  {"id": 2, "temperature": 30}
]
```

Configure `sendSingle: true`, `format: "json"`, and `batchSize: 100`:

```json
{
  "batchSize": 100,
  "sendSingle": true,
  "format": "json",
  "dataTemplate": "{\"deviceId\": {{.id}}, \"value\": {{.temperature}}}"
}
```

The template formats each record:

```json
{"deviceId": 1, "value": 20}
{"deviceId": 2, "value": 30}
```

The JSON batch writer frames elements into an array when the batch flushes:

```json
[
  {"deviceId": 1, "value": 20},
  {"deviceId": 2, "value": 30}
]
```

### Omitted Data Templates

When you omit `dataTemplate`, the sink writer serializes records using `format` without pre-encoding transformations.
