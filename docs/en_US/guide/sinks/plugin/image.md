# Image Sink

The image sink saves binary image data to a specified directory.

## Compile and Deploy the Plugin

Run the following commands to compile and install the plugin:

```shell
cd $rekuiper_src
go build -trimpath --buildmode=plugin -o plugins/sinks/Image.so extensions/sinks/image/image.go
cp plugins/sinks/Image.so $rekuiper_install/plugins/sinks
```

Restart the rekuiper server to activate the plugin.

## Properties

| Property name | Optional | Description |
|---|---|---|
| path | false | Target directory path for saved images, such as `./tmp`. Do not use the same directory across different rules to prevent file deletion conflicts. |
| format | false | Image file format: `jpeg` or `png`. |
| maxAge | true | Maximum retention time in hours. Default: `72` (3 days). |
| maxCount | true | Maximum number of stored image files. Default: `1000`. rekuiper deletes older images when this threshold is exceeded. Evaluated with `maxAge` by using logical OR. |

Other common sink properties are supported. Refer to [sink common properties](../overview.md#common-properties) for more information.

## Usage Example

The following rule receives images and saves them to the `/tmp` directory. If the image count exceeds 1000, rekuiper deletes the oldest images. If images remain for more than 72 hours, rekuiper deletes expired files:

```json
{
  "sql": "SELECT * from demo",
  "actions": [
    {
      "image": {
        "path": "/tmp",
        "format": "png",
        "maxCount": 1000,
        "maxAge": 72
      }
    }
  ]
}
```

## Demonstration

The following example uses the `zmq` source to receive image data and the `image` sink to store images in the specified directory:

```shell
curl http://127.0.0.1:9081/streams -X POST -d '{"sql":"create stream s(image bytea) WITH (DATASOURCE = \"\", FORMAT = \"binary\", TYPE = \"zmq\");"}'

curl http://127.0.0.1:9081/rules -X POST -d '{"id":"r","sql":"SELECT * FROM s","actions":[{"image":{"path":"./tmp","format":"png"}}]}'
```
