# Script Functions

Script functions provide a lightweight mechanism to extend SQL functions without compiling binary plugins. You can define script functions in plain text, register them dynamically at runtime, and export or import them across instances.

rekuiper supports JavaScript as the scripting language.

::: tip Notice
Script functions require the Goja JavaScript engine. They are included in full and slim-python binaries and Docker images. To compile with script support from source, add the `script` build tag.
:::

## JavaScript Functions

rekuiper includes the [Goja](https://github.com/dop251/goja) JavaScript runtime, which conforms to the ECMA 5.1 standard.

Development workflow:
1. Write and test JavaScript functions.
2. Register the script function in rekuiper.
3. Invoke the function in SQL rules.
4. Ingest test stream data and inspect output.

### Function Implementation

Write functions using standard ECMA 5.1 syntax:

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
