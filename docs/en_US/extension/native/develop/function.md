# Function Extension

::: tip Note
Go C-shared native `.so` dynamic plugins are unsupported in rekuiper. High-performance connectors are compiled directly into the rekuiper Rust binary. Custom function extensions run through [WebAssembly (Wasm)](../../wasm/overview.md) or [External Services](../../external/external_func.md). This guide is preserved as a technical reference for legacy eKuiper installations.
:::

rekuiper provides [built-in functions](../../../sqls/functions/overview.md) for data processing. You can create custom function extensions to implement domain-specific business logic.

## Development

### Develop a Custom Function

To create a custom function, implement the [api.Function](https://github.com/lf-edge/ekuiper/blob/master/pkg/api/stream.go) interface and export it from a Go plugin.

Before developing, [configure the plugin development environment](../overview.md#setup-the-plugin-developing-environment).

1. Implement `Validate`:

   ```go
   Validate(args []interface{}) error
   ```

   The engine calls `Validate` during SQL plan verification. The parameter is a slice of AST expressions. Check argument counts and types. Return `nil` if validation succeeds; otherwise, return an error.

2. Implement `IsAggregate`:

   ```go
   IsAggregate() bool
   ```

   Return `true` if the function operates as an aggregate function over grouped window slices; otherwise, return `false`.

3. Implement `Exec`:

   ```go
   Exec(args []interface{}) (interface{}, bool)
   ```

   The engine calls `Exec` to compute the function result. The `args` parameter contains the runtime argument values. Return the result and `true` on success, or return `nil` and `false` on failure.

4. Export the Symbol:

   The plugin must reside in the `main` package. Export the function as a variable symbol:

   ```go
   var MyFunction myFunction
   ```

Refer to the [Echo Function](https://github.com/lf-edge/ekuiper/blob/master/extensions/functions/echo/echo.go) implementation for a complete reference.

### Export Multiple Functions

A single plugin can export multiple functions:

```go
var (
    Function1 function1
    Function2 function2
    FunctionN functionN
)
```

Combining related functions into one plugin simplifies distribution and deployment.

### Package the Plugin

Compile the function into a Go plugin `.so` file in the `plugins/functions` directory:

```bash
go build -trimpath --buildmode=plugin -o plugins/functions/MyFunction.so extensions/functions/my_function.go
```

### Register Functions

The engine automatically loads plugins in the plugin directory. If a plugin exports multiple functions, register them explicitly:

1. **Development**: Place the compiled `.so` file into `plugins/functions`, then invoke the [CLI register command](../../../api/cli/plugins.md#register-functions) or the [REST register API](../../../api/restapi/plugins.md#register-functions).
2. **Production**: [Package the plugin into a zip archive](plugins_tutorial.md#deployment), then invoke the [CLI create command](../../../api/cli/plugins.md#create-a-plugin) or the [REST create API](../../../api/restapi/plugins.md#create-a-plugin) with the function list.

## Usage in SQL Rules

Invoke the custom function directly in SQL:

```json
{
  "id": "rule1",
  "sql": "SELECT myFunction(name) FROM demo",
  "actions": [
    {
      "log": {}
    }
  ]
}
```
