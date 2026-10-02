# Execute TensorFlow Lite Models with External Functions

By integrating rekuiper and TensorFlow Lite, you can analyze stream records using pre-trained machine learning models. This tutorial explains how to build an external gRPC function service to label images captured by edge devices.

External functions run in independent processes or on separate hosts. This architecture decouples the lifecycle of inference services from rekuiper and allows external services to serve multiple clients simultaneously.

## Prerequisites

Prepare the following components before you begin:

- Basic knowledge of gRPC services. Download the [sample code package](https://github.com/lf-edge/ekuiper/blob/master/docs/resources/pythonGRPC.zip).
- A working Docker installation.

## Develop the External Function

The gRPC server exposes a `label` remote procedure call (RPC) method. The method executes image classification using `tflite_runtime`. Refer to `label.py` in the sample code repository for implementation details.

The following Protocol Buffers definition describes the service interface. The `label` method accepts a Base64-encoded image:

```protobuf
syntax = "proto3";

package sample;

// The algorithms service definition.
service Algorithms {
  rpc label(LabelRequest) returns(LabelReply) {}
}

// The request message containing the base64 encoded image.
message LabelRequest {
  string base64_img = 1;
}

message LabelResult {
  float  confidence = 1;
  string label = 2;
}

// The response message containing classification results.
message LabelReply {
  repeated LabelResult results = 1;
}
```

## Build and Start the gRPC Server

Use the provided Dockerfile to build and start the gRPC service container. In the root directory of the extracted sample code, run:

```shell
docker build -t test:1.1.1 -f deploy/Dockerfile-slim-python .
```

Start the service container:

```shell
docker run -d -p 50051:50051 --name rpc-test test:1.1.1
```

The gRPC server listens on TCP port `50051`.

## Package and Register the External Function

### Package the Service Archive

Create a ZIP archive containing the service description JSON file and the `.proto` schema file:

- `schemas/`
  - `sample.proto`
- `sample.json`

Refer to the [External Function documentation](../../extension/external/external_func.md) for descriptor schema details. You can find pre-packaged files in the `ekuiper_package` folder of the sample repository.

### Register the External Service

Copy the `sample.zip` archive to `/tmp` on the host where rekuiper runs, and register the service by using the command-line interface:

```shell
bin/kuiper create service sample '{"name": "sample", "file": "file:///tmp/sample.zip"}'
```

## Run the External Function in Rules

Once registered, you can invoke the function directly in streaming SQL rules.

### Create the Stream

Define a stream that subscribes to MQTT topic `tfdemo`:

```shell
bin/kuiper create stream demo '() WITH (DATASOURCE = "tfdemo")'
```

### Create the Rule

Execute a test query using the command-line tool:

```shell
bin/kuiper query
kuiper > SELECT label(image) FROM demo
```

### Publish Test Data

Send JSON records containing Base64-encoded image payloads to the `tfdemo` topic:

```json
{
  "image": "base64_encoded_image_bytes"
}
```

You can use sample payloads from `images/example.json` in the example code repository.

### Verify the Result

When you publish an image, the rule outputs classification labels and confidence values:

```json
[
  {
    "label": {
      "results": [
        {"confidence": 0.5789139866828918, "label": "tailed frog"},
        {"confidence": 0.3095814287662506, "label": "bullfrog"},
        {"confidence": 0.040725912898778915, "label": "whiptail"},
        {"confidence": 0.03226377069950104, "label": "frilled lizard"},
        {"confidence": 0.01566782221198082, "label": "agama"}
      ]
    }
  }
]
```

## Conclusion

External function services enable pre-trained TensorFlow Lite inference in separate processes. You can adapt this pattern to connect any gRPC algorithm service to rekuiper streaming rules.
