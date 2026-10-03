# Script Functions

Script functions provide a lightweight mechanism to extend SQL functions without compiling binary plugins. You can define script functions in plain text, register them dynamically at runtime, and invoke them directly in SQL queries.

rekuiper supports JavaScript (ECMAScript) as the scripting language.

::: tip Embedded Rust Runtime
rekuiper executes JavaScript functions using **[Boa Engine](https://github.com/boa-dev/boa)**, an experimental ECMAScript engine written entirely in Rust. JavaScript support is built directly into the standard rekuiper binary. You do not need to install Node.js, V8, or external C shared libraries.
:::

## JavaScript Functions

Development workflow:
1. Write and test JavaScript functions.
2. Register the script function in rekuiper using the REST API or the CLI.
3. Invoke the function in SQL rules.
4. Ingest stream data and verify the computed outputs.

### Function Implementation

Write functions using standard ECMAScript syntax:

```javascript
function echo(msg) {
    return msg;
}
```

Handle type conversions explicitly when converting between JavaScript dynamic types and SQL data types.

#### Aggregate Functions

For aggregate functions, the function receives an array argument containing grouped window values and returns a single scalar value:

```javascript
function count_by_js(msgs) {
    return msgs.length;
}
```

### Manage Functions

Register, inspect, and delete script functions using:
- [REST API](../../api/restapi/udf.md)
- [CLI](../../api/cli/scripts.md)

### Usage in SQL

Registered functions are immediately available in SQL queries. Exceptions thrown inside JavaScript functions raise runtime errors during rule execution.

## Example Use Case

This example registers a rectangle area calculation function and invokes it in an MQTT rule.

1. **Register the Function**:

   ```http
   POST /udf/javascript
   Content-Type: application/json

   {
     "id": "area",
     "description": "Calculate rectangle area",
     "script": "function area(x, y) { return x * y; }",
     "isAgg": false
   }
   ```

2. **Define the Rule**:

   ```json
   {
     "id": "ruleArea",
     "sql": "SELECT area(length, width) AS area FROM mqttDemo",
     "actions": [
       {
         "mqtt": {
           "server": "tcp://127.0.0.1:1883",
           "topic": "result/area",
           "sendSingle": true
         }
       }
     ]
   }
   ```

3. **Publish Input Telemetry**:

   ```json
   {"length": 3, "width": 4}
   ```

4. **Inspect Output Topic (`result/area`)**:

   ```json
   {"area": 12}
   ```
