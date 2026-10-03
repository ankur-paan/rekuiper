# Go SDK for Portable Plugins

::: danger Status: Unsupported in rekuiper (Legacy eKuiper SDK Only)
The legacy Go SDK for portable plugins is unsupported in rekuiper because portable plugin execution is not implemented (HTTP 501).

To write Go extensions for rekuiper:
- **Compile to WebAssembly (Wasm)** using TinyGo and run with rekuiper's embedded [WebAssembly Runtime](../wasm/overview.md).
- **Run as a gRPC/REST Microservice** and integrate using [External Services](../external/external_func.md).

This SDK guide is preserved as a technical reference for legacy eKuiper installations.
:::

The Go SDK allowed developers to build portable plugins in Go in legacy eKuiper.

## Development

### Dependency Configuration

Import the Go SDK module:

```text
require github.com/lf-edge/ekuiper/sdk/go v0.0.0
```

Implement the extension interfaces defined in `github.com/lf-edge/ekuiper/sdk/go/api`.

### Implement Extension Interfaces

- **Source Interface**:

  ```go
  type Source interface {
      Open(ctx StreamContext, consumer chan<- SourceTuple, errCh chan<- error)
      Configure(datasource string, props map[string]interface{}) error
      Closable
  }
  ```

- **Sink Interface**:

  ```go
  type Sink interface {
      Open(ctx StreamContext) error
      Configure(props map[string]interface{}) error
      Collect(ctx StreamContext, data interface{}) error
      Closable
  }
  ```

- **Function Interface**:

  ```go
  type Function interface {
      Validate(args []interface{}) error
      Exec(args []interface{}, ctx FunctionContext) (interface{}, bool)
      IsAggregate() bool
  }
  ```

### Main Entry Program

Implement an executable entry point that calls `sdk.Start`:

```go
package main

import (
    "os"
    "github.com/lf-edge/ekuiper/sdk/go/api"
    sdk "github.com/lf-edge/ekuiper/sdk/go/runtime"
)

func main() {
    sdk.Start(os.Args, &sdk.PluginConfig{
        Name: "mirror",
        Sources: map[string]sdk.NewSourceFunc{
            "random": func() api.Source {
                return &randomSource{}
            },
        },
        Functions: map[string]sdk.NewFunctionFunc{
            "echo": func() api.Function {
                return &echo{}
            },
        },
        Sinks: map[string]sdk.NewSinkFunc{
            "file": func() api.Sink {
                return &fileSink{}
            },
        },
    })
}
```

The names declared in `PluginConfig` must match the names defined in the plugin JSON metadata.

Refer to the [Go SDK Mirror Example](https://github.com/lf-edge/ekuiper/tree/master/sdk/go/example/mirror) for complete sample code.

## Packaging

Compile the main program with `go build`:

```shell
go build -o mirror main.go
```

Package the executable and the JSON metadata file into a `.zip` archive. Refer to [Packaging Portable Plugins](./overview.md#packaging) for details.
