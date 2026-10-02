# WebAssembly (Wasm) Plugins

WebAssembly (Wasm) plugins provide sandboxed, high-performance function extensions. You can write extensions in languages that compile to WebAssembly bytecode, including Rust, C, C++, and Go.

Development workflow:
1. Implement the function logic in your chosen programming language.
2. Compile the source code into a `.wasm` binary module.
3. Package the binary and register the plugin in rekuiper.

## Prerequisites and Tooling

This guide uses TinyGo to compile Go source code into WebAssembly modules running on the WasmEdge runtime.

1. Verify the Go compiler installation:

   ```shell
   go version
   ```

2. Verify the [TinyGo](https://github.com/tinygo-org/tinygo/releases) compiler:

   ```shell
   tinygo version
   ```

3. Verify the [WasmEdge](https://wasmedge.org/book/en/quick_start/install.html) runtime:

   ```shell
   wasmedge -v
   ```

   To install WasmEdge on Linux or macOS:

   ```shell
   curl -sSf https://raw.githubusercontent.com/WasmEdge/WasmEdge/master/utils/install.sh | bash
   source $HOME/.wasmedge/env
   ```

## Function Implementation

Create `fibonacci.go`:

```go
package main

func main() {}

//export fib
func fibArray(n int32) int32 {
  arr := make([]int32, n)
  for i := int32(0); i < n; i++ {
    switch {
    case i < 2:
      arr[i] = i
    default:
      arr[i] = arr[i-1] + arr[i-2]
    }
  }
  return arr[n-1]
}
```

Compile the source code to a WASI bytecode target:

```shell
tinygo build -o fibonacci.wasm -target wasi fibonacci.go
```

Verify execution with WasmEdge:

```shell
wasmedge --reactor fibonacci.wasm fib 10
```

Expected output: `34`.

## Packaging

Package the plugin files into a `.zip` archive containing:
- `fibonacci.json`: Plugin descriptor matching the plugin name.
- `fibonacci.wasm`: Compiled bytecode module matching the plugin name.

Example descriptor `fibonacci.json`:

```json
{
  "version": "v1.0.0",
  "functions": [
    "fib"
  ],
  "wasmEngine": "wasmedge"
}
```

## Compilation and Installation

To enable Wasm support when building from source:

```shell
make build_with_wasm
```

Install the packaged plugin using the CLI:

```shell
bin/kuiper create plugin wasm fibonacci '{"file":"file:///$HOME/ekuiper/internal/plugin/testzips/wasm/fibonacci.zip"}'
```

Verify the plugin installation:

```shell
bin/kuiper describe plugin wasm fibonacci
```

## Query Execution

1. Create a stream and start a query:

   ```shell
   bin/kuiper create stream demo_fib '(num float) WITH (FORMAT="JSON", DATASOURCE="demo_fib")'
   bin/kuiper query
   SELECT fib(num) FROM demo_fib;
   ```

2. Send test telemetry using HTTP push:

   ```shell
   curl -X POST http://localhost:9081/streams/demo_fib/data \
     -H "Content-Type: application/json" \
     -d '{"num": 25}'
   ```

   Or publish using MQTT:

   ```shell
   mosquitto_pub -h 127.0.0.1 -t demo_fib -m '{"num": 25}'
   ```

3. The query invokes the Wasm function and returns the computed Fibonacci result.

## Management

- **File System Autoload**: Place uncompressed plugin directories under `plugins/wasm/{pluginName}` to load modules at engine startup.
- **Dynamic API**: Manage Wasm plugins at runtime through the [REST API](../../api/restapi/plugins.md) or the [CLI](../../api/cli/plugins.md).
