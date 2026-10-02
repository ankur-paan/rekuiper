# Python SDK for Portable Plugins

The Python SDK allows developers to build portable plugins in Python. It provides interfaces for source, sink, and function extensions, as well as runtime entry points to manage the plugin lifecycle.

## Prerequisites

- Python 3.x runtime.
- Required packages: install with `pip install nng ekuiper`.

By default, the engine executes Python plugins using the `python` command. You can specify a custom Python binary in the [configuration file](../../configuration/global_configurations.md#portable-plugin-configurations).

## Development

Implement extension classes by subclassing the abstract base classes provided by the SDK.

### Source Interface

```python
from abc import abstractmethod
from ekuiper import Context

class Source(object):
    """Abstract base class for rekuiper source extensions."""

    @abstractmethod
    def configure(self, datasource: str, conf: dict):
        """Initializes configuration properties."""
        pass

    @abstractmethod
    def open(self, ctx: Context):
        """Starts continuous ingestion and emits data or errors."""
        pass

    @abstractmethod
    def close(self, ctx: Context):
        """Releases resources and stops ingestion."""
        pass
```

### Sink Interface

```python
from abc import abstractmethod
from typing import Any
from ekuiper import Context

class Sink(object):
    """Abstract base class for rekuiper sink extensions."""

    @abstractmethod
    def configure(self, conf: dict):
        """Initializes sink configuration properties."""
        pass

    @abstractmethod
    def open(self, ctx: Context):
        """Establishes connections to target systems."""
        pass

    @abstractmethod
    def collect(self, ctx: Context, data: Any):
        """Processes and forwards incoming records."""
        pass

    @abstractmethod
    def close(self, ctx: Context):
        """Closes connections and releases resources."""
        pass
```

#### Sink Acknowledgments

When `requireAck` is enabled in the rule action, the sink must acknowledge each message before receiving the next record:

```json
{
  "id": "rulePort1",
  "sql": "SELECT * FROM mqttStream",
  "actions": [
    {
      "print": {
        "requireAck": true
      }
    }
  ]
}
```

The sink implementation calls `ctx.ack_ok()` on success or `ctx.ack_error(msg)` on failure:

```python
def collect(self, ctx: Context, data: Any):
    print("Received:", data)
    ctx.ack_ok()
```

### Function Interface

```python
from abc import abstractmethod
from typing import List, Any
from ekuiper import Context

class Function(object):
    """Abstract base class for rekuiper function extensions."""

    @abstractmethod
    def validate(self, args: List[Any]):
        """Validates arguments against expected signatures."""
        pass

    @abstractmethod
    def exec(self, args: List[Any], ctx: Context) -> Any:
        """Computes the function output."""
        pass

    @abstractmethod
    def is_aggregate(self) -> bool:
        """Specifies whether the function is an aggregate function."""
        pass
```

### Main Entry Program

Declare the plugin configuration and start the runtime:

```python
from ekuiper import PluginConfig, plugin

if __name__ == "__main__":
    c = PluginConfig(
        "pysam",
        {"pyjson": lambda: PyJson()},
        {"print": lambda: PrintSink()},
        {"revert": lambda: revertIns}
    )
    plugin.start(c)
```

Refer to the [Python SDK PySam Example](https://github.com/lf-edge/ekuiper/tree/master/sdk/python/example/pysam) for complete sample code.

## Packaging and Deployment

Specify the main Python script file in the plugin JSON descriptor. Refer to [Packaging Portable Plugins](./overview.md#packaging) for details.

### Virtual Environments (Conda)

To execute the plugin inside a Conda environment, configure the metadata descriptor:

```json
{
  "version": "v1.0.0",
  "language": "python",
  "executable": "pysam.py",
  "virtualEnvType": "conda",
  "env": "myenv",
  "sources": [
    "pyjson"
  ],
  "sinks": [
    "print"
  ],
  "functions": [
    "revert"
  ]
}
```
