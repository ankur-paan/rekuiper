# Native Plugin Development Tutorial

::: tip Note
Go C-shared native `.so` dynamic plugins are unsupported in rekuiper. High-performance connectors are compiled directly into the rekuiper Rust binary. Custom function extensions run through [WebAssembly (Wasm)](../../wasm/overview.md) or [External Services](../../external/external_func.md). This tutorial is preserved as a technical reference for legacy eKuiper installations.
:::

## Overview

In legacy eKuiper, users compiled Go plugins as dynamically loaded `.so` shared libraries. Go native plugins impose strict toolchain constraints:
- They do not support Windows.
- The compilation environment must match the target eKuiper binary exactly (Go compiler version, dependency library versions, and GOPATH).

This tutorial explains how to develop, compile, and deploy a sample MySQL sink plugin in legacy eKuiper environments.

## Development Workflow

1. Create a Go module project.
2. Implement the [api.TupleCollector](https://github.com/lf-edge/ekuiper/blob/master/contract/api/sink.go) interface in `sinks/mysql.go`.
3. Export the constructor symbol `Mysql`.
4. Configure dependencies in `go.mod`.
5. Compile and test the plugin.

### Project Layout

```text
samplePlugin
  sinks
    mysql.go
  go.mod
```

### Implement the Sink Plugin

Create `sinks/mysql.go`:

```go
package main

import (
  "database/sql"
  "fmt"

  _ "github.com/go-sql-driver/mysql"
  "github.com/lf-edge/ekuiper/contract/v2/api"
  "github.com/mitchellh/mapstructure"
)

type mysqlConfig struct {
  Url   string `json:"url"`
  Table string `json:"table"`
}

type mysqlSink struct {
  conf *mysqlConfig
  db   *sql.DB
}

func (m *mysqlSink) Provision(ctx api.StreamContext, configs map[string]any) error {
  cfg := &mysqlConfig{}
  config := &mapstructure.DecoderConfig{
    TagName: "json",
    Result:  cfg,
  }
  decoder, err := mapstructure.NewDecoder(config)
  if err != nil {
    return err
  }
  err = decoder.Decode(configs)
  if err != nil {
    return fmt.Errorf("read properties %v fail with error: %v", configs, err)
  }
  if cfg.Url == "" {
    return fmt.Errorf("property Url is required")
  }
  if cfg.Table == "" {
    return fmt.Errorf("property Table is required")
  }
  m.conf = cfg
  ctx.GetLogger().Infof("mysql provisioning started with props: %v", cfg)
  return nil
}

func (m *mysqlSink) Connect(ctx api.StreamContext) error {
  ctx.GetLogger().Debugf("Opening mysql sink %v", m.conf)
  var err error
  m.db, err = sql.Open("mysql", m.conf.Url)
  return err
}

func (m *mysqlSink) Collect(ctx api.StreamContext, item api.MessageTuple) error {
  ctx.GetLogger().Debugf("mysql sink receive %s", item)
  v, ok := item.Value("name", "")
  if !ok {
    return fmt.Errorf("received value does not have name field")
  }
  query := fmt.Sprintf("INSERT INTO %s (`name`) VALUES ('%s')", m.conf.Table, v)
  ctx.GetLogger().Debugf(query)
  insert, err := m.db.Query(query)
  if err != nil {
    return err
  }
  defer insert.Close()
  return nil
}

func (m *mysqlSink) CollectList(ctx api.StreamContext, item api.MessageTupleList) error {
  ctx.GetLogger().Debugf("mysql sink receive %s", item)
  if item.Len() <= 0 {
    return fmt.Errorf("received empty item list")
  }
  item.RangeOfTuples(func(index int, tuple api.MessageTuple) bool {
    v, ok := tuple.Value("", "name")
    if !ok {
      return false
    }
    query := fmt.Sprintf("INSERT INTO %s (`name`) VALUES ('%s')", m.conf.Table, v)
    ctx.GetLogger().Debugf(query)
    insert, err := m.db.Query(query)
    if err != nil {
      return false
    }
    defer insert.Close()
    return true
  })
  return nil
}

func (m *mysqlSink) Close(ctx api.StreamContext) error {
  if m.db != nil {
    return m.db.Close()
  }
  return nil
}

func Mysql() api.Sink {
  return &mysqlSink{}
}
```

### Module Configuration

Configure `go.mod` to match the target eKuiper contract version:

```text
module samplePlugin

go 1.25

require (
  github.com/lf-edge/ekuiper/contract/v2 v2.0.0
  github.com/go-sql-driver/mysql v1.5.0
)
```

## Compilation

### Local Compilation

Compile the plugin into a shared object:

```shell
go build -trimpath --buildmode=plugin -o Mysql@v1.0.0.so ./sinks/mysql.go
```

### Docker Compilation

Compile plugins in the official development Docker container to match build environments:

```shell
docker run -d --name kuiper-dev --mount type=bind,source=/var/git,target=/go/plugins lfedge/ekuiper:2.0.0
docker exec -it kuiper-dev /bin/sh
```

Inside the container:

```shell
cd /go/plugins
go build -trimpath --buildmode=plugin -o Mysql@v1.0.0.so ./samplePlugin/sinks/mysql.go
```

If compiling for Alpine environments, install `gcompat`:

```shell
apk add gcompat
cd /lib
ln -s libgcompat.so.0 /usr/lib/libresolve.so.2
```

## Testing

Deploy a test rule using the MySQL action:

```json
{
  "id": "ruleTest",
  "sql": "SELECT * FROM demo",
  "actions": [
    {
      "log": {},
      "mysql": {
        "url": "user:password@tcp(localhost:3306)/database",
        "table": "test"
      }
    }
  ]
}
```

## Deployment

1. Package `Mysql@v1.0.0.so` and optional configuration files into `mysqlSink.zip` and host it on an HTTP server.
2. Install the plugin using the REST API:

   ```shell
   curl -X POST http://{host}:9081/plugins/sinks \
     -H "Content-Type: application/json" \
     -d '{"name":"mysql","file":"http://{http_server_ip}/plugins/sinks/mysqlSink.zip"}'
   ```

3. Verify installation:

   ```shell
   curl http://{host}:9081/plugins/sinks/mysql
   ```

   Response:

   ```json
   {
     "name": "mysql",
     "version": "1.0.0"
   }
   ```
