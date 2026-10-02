# Scripts Management

The rekuiper script CLI manages JavaScript user-defined functions (UDFs). You can register, list, describe, and drop scripts.

## Register a Script

Use this command to register a JavaScript function using JSON format:

```shell
create script $script_json
```

Example command:

```shell
# bin/kuiper create script "{\"id\": \"area\",\"description\": \"calculate the area\",\"script\": \"function area(x, y) { return x * y; }\",\"isAgg\": false}"
```

### JSON Fields

- `id`: The unique identifier for the function. The script code must define a function with this matching name.
- `description`: An explanation of function behavior.
- `script`: The JavaScript source code containing the function implementation.
- `isAgg`: A boolean indicating whether the function operates as an aggregate function.

Example JSON definition:

```json
{
   "id": "area",
   "description": "calculate area",
   "script": "function area(x, y) { return x * y; }",
   "isAgg": false
}
```

## Show All Scripts

Use this command to display all registered JavaScript functions:

```shell
show scripts
```

Example command and output:

```shell
# bin/kuiper show scripts
["area"]
```

## Describe a Script

Use this command to display the definition of a JavaScript function:

```shell
describe script $script_name
```

Example command and output:

```shell
# bin/kuiper describe script area
{
   "id": "area",
   "description": "calculate area",
   "script": "function area(x, y) { return x * y; }",
   "isAgg": false
}
```

## Delete a Script

Use this command to delete a JavaScript function:

```shell
drop script $script_name
```

Example command:

```shell
# bin/kuiper drop script area
```
