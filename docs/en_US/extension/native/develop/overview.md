# Native Plugin Development

::: tip Notice
Go native `.so` dynamic plugin compilation is unsupported in the current version of rekuiper. rekuiper is implemented in Rust and uses portable plugin architectures through [WebAssembly (Wasm)](../../wasm/overview.md) and [External Services](../../external/external_func.md). This documentation is preserved as a technical reference for legacy eKuiper compatibility.
:::

In legacy eKuiper, developers used the Go plugin mechanism to build Source, Sink, and Function extensions:

1. Create a plugin project.
2. Implement the interface methods for the extension type.
3. Build the plugin into a `.so` shared object.
4. Package the `.so` binary, metadata JSON, and configuration YAML into a zip archive.

## Set Up the Development Environment

You must compile plugins with the exact dependency versions of the target eKuiper binary, particularly `github.com/lf-edge/ekuiper/contract/v2`. Ensure that the Go compiler version and module dependencies in `go.mod` match the main project.

Example `go.mod`:

```text
module mycompany.com/myplugin

require github.com/lf-edge/ekuiper/contract/v2 v2.0.0

go 1.25.4
```

### Implement Plugin Symbols

A plugin implements a specific interface and exports symbols by name. Two symbol export patterns are supported:

1. **Export a Constructor Function (Recommended)**: The engine invokes the constructor function to create an isolated instance for each rule:

   ```go
   func Random() api.Source {
       return random.GetSource()
   }
   ```

2. **Export an Instance (Singleton)**: All rules share the exported singleton instance. The plugin implementation must handle thread safety and shared state:

   ```go
   var Random = random.GetSource()
   ```

For detailed interface specifications, refer to:
- [Source Interface](./source.md)
- [Sink Interface](./sink.md)
- [Function Interface](./function.md)

## State Storage

eKuiper extensions access key-value state storage through the context object. State storage is available for Source, Sink, and Function extensions.

Keys are scoped to the current instance. Available state methods include `putState`, `getState`, `incrCounter`, `getCounter`, and `deleteState`.

Example state usage in a function extension:

```go
func (f *accumulateWordCountFunc) Exec(args []interface{}, ctx api.FunctionContext) (interface{}, bool) {
    logger := ctx.GetLogger()
    err := ctx.IncrCounter("allwordcount", len(strings.Split(args[0].(string), args[1].(string))))
    if err != nil {
        return err, false
    }
    if c, err := ctx.GetCounter("allwordcount"); err != nil {
        return err, false
    } else {
        return c, true
    }
}
```

## Runtime Dependencies

Plugins can store runtime files under <span v-pre>`{{rekuiperPath}}/etc/{{pluginType}}/{{pluginName}}`</span>. Place these files in the `etc` directory when packaging the archive.

Retrieve the root installation path in code:

```go
ctx.GetRootPath()
```

## Plugin Compilation

Compile the `.so` shared object by using the exact compiler environment of the target binary:

```bash
go build -trimpath --buildmode=plugin -o plugins/sources/MySource.so plugins/sources/my_source.go
```

### Symbol and File Naming

- The exported symbol must use CamelCase with an uppercase first letter (for example, plugin `file` exports symbol `File`).
- The `.so` filename must match the export symbol or the plugin name (for example, `MySource.so` or `mySink.so`).

### Version Identifiers

You can append a version string after an `@` symbol in the `.so` file name:
- `MySource@v1.0.0.so`
- `MySource@20200331.so`

When multiple versions exist, the engine loads the highest version string.

## Plugin Packaging

Package the compiled `.so` file, configuration YAML (for sources), and metadata JSON into the root of a `.zip` archive without subdirectories.

## Related Resources

Refer to the [Plugin Tutorial](./plugins_tutorial.md) for a complete walkthrough of building and deploying plugins.
