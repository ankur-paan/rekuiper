# ZeroMQ Sink

The ZeroMQ sink publishes query results to a ZeroMQ topic.

## Compile and Deploy the Plugin

Run the following commands to compile and install the plugin:

```shell
cd $rekuiper_src
go build -trimpath --buildmode=plugin -o plugins/sinks/Zmq.so extensions/sinks/zmq/zmq.go
cp plugins/sinks/Zmq.so $rekuiper_install/plugins/sinks
```

Restart the rekuiper server to activate the plugin.

## Properties

| Property name | Optional | Description |
|---|---|---|
| server | false | The URL address of the ZeroMQ server, such as `tcp://127.0.0.1:5563`. |
| topic | true | The ZeroMQ topic name to publish to. |

Other common sink properties are supported. Refer to [sink common properties](../overview.md#common-properties) for more information.

## Sample Usage

The following sample rule filters records where temperature exceeds 50 and publishes results to ZeroMQ topic `temp`:

```json
{
  "sql": "SELECT * from demo where temperature > 50",
  "actions": [
    {
      "zmq": {
        "server": "tcp://127.0.0.1:5563",
        "topic": "temp"
      }
    }
  ]
}
```
