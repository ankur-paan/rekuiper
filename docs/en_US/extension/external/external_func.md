# External Functions

## Overview

rekuiper can map external services to SQL functions through configuration. When a rule executes an external function, rekuiper converts incoming SQL arguments, invokes the external endpoint, and returns the response into the stream pipeline.

External functions belong to two categories:
- **Schema-Based**: Uses a schema file to describe service APIs, method signatures, parameter types, and return types. Recommended for gRPC and complex REST services.
- **Schemaless**: Uses only a JSON configuration file without a schema definition. Recommended for simple REST services.

## Configuration

### Schema-Based External Functions

A schema-based external function requires two configuration files:
- **JSON File**: Defines service metadata, interface addresses, protocols, and function aliases. The file name defines the service name in rekuiper.
- **Schema File**: Defines API methods and parameter types. rekuiper supports [Protobuf](https://developers.google.com/protocol-buffers) schema files.

The JSON configuration file contains these sections:

- `about`: Contains service metadata, including author, description, and documentation URLs.
- `interfaces`: Defines a group of service interfaces sharing a common address. Each interface contains these properties:
  - `protocol`: Transport protocol. Supported values are `"grpc"` and `"rest"`. You can also enable `"msgpack-rpc"` by compiling with the `msgpack` build tag. Refer to [Feature Compilation](../../installation.md#compile-with-selected-features).
  - `address`: Target URL of the service (for example, `"tcp://localhost:50051"` or `"http://localhost:8090"`).
  - `schemaType`: Type of schema description. rekuiper supports `"protobuf"`.
  - `schemaFile`: Name of the `.proto` file in the schemas directory.
  - `functions`: Array of function aliases mapping Protobuf RPC methods to SQL function names. For example, `{"name":"helloFromMsgpack","serviceName":"SayHello"}` maps RPC `SayHello` to SQL function `helloFromMsgpack`. Unmapped RPC methods keep their original names.
  - `options`: Interface options. For REST services, options include:
    - `headers`: Map of HTTP headers.
    - `insecureSkipVerify`: Boolean flag to skip HTTPS TLS certificate verification.

Example configuration file `sample.json`:

```json
{
  "about": {
    "author": {
      "name": "Author Name",
      "email": "developer@example.org",
      "company": "Example Corp",
      "website": "https://example.org"
    },
    "helpUrl": {
      "en_US": "https://github.com/lf-edge/ekuiper/blob/master/docs/en_US/plugins/functions/functions.md",
      "zh_CN": "https://github.com/lf-edge/ekuiper/blob/master/docs/zh_CN/plugins/functions/functions.md"
    },
    "description": {
      "en_US": "Sample external services for testing",
      "zh_CN": "示例外部函数配置，仅供测试"
    }
  },
  "interfaces": {
    "trueno": {
      "address": "tcp://localhost:50051",
      "protocol": "grpc",
      "schemaType": "protobuf",
      "schemaFile": "trueno.proto"
    },
    "tsrest": {
      "address": "http://localhost:8090",
      "protocol": "rest",
      "options": {
        "insecureSkipVerify": true,
        "headers": {
          "Accept-Charset": "utf-8"
        }
      },
      "schemaType": "protobuf",
      "schemaFile": "tsrest.proto",
      "functions": [
        {
          "name": "objectDetect",
          "serviceName": "object_detection"
        }
      ]
    },
    "tsrpc": {
      "address": "tcp://localhost:9000",
      "protocol": "msgpack-rpc",
      "schemaType": "protobuf",
      "schemaFile": "tsrpc.proto",
      "functions": [
        {
          "name": "getFeature",
          "serviceName": "get_feature"
        },
        {
          "name": "getSimilarity",
          "serviceName": "get_similarity"
        }
      ]
    }
  }
}
```

The schema file `tsrest.proto` defines the interface methods:

```protobuf
syntax = "proto3";
package ts;

service TSRest {
  rpc object_detection(ObjectDetectionRequest) returns(ObjectDetectionResponse) {}
}

message ObjectDetectionRequest {
  string cmd = 1;
  string base64_img = 2 [json_name="base64_img"];
}

message ObjectDetectionResponse {
  string info = 1;
  int32 code = 2;
  string image = 3;
  string result = 4;
  string type = 5;
}
```

#### HTTP Transcoding Options

To configure HTTP request methods, URL paths, query parameters, and request bodies in REST services, add `google.api.http` annotations to the `.proto` file.

This example sets the HTTP method to `POST`, sets the endpoint to `/v1/computation/object_detection`, and sets the request body to all input parameters (`body: "*"`):

```protobuf
service TSRest {
  rpc object_detection(ObjectDetectionRequest) returns(ObjectDetectionResponse) {
    option (google.api.http) = {
      post: "/v1/computation/object_detection"
      body: "*"
    };
  }
}
```

To bind a field to a URL path variable, specify path parameters in braces:

```protobuf
service TSRest {
  rpc object_detection(ObjectDetectionRequest) returns(ObjectDetectionResponse) {
    option (google.api.http) = {
      post: "/v1/computation/object_detection/{cmd}"
      body: "base64_img"
    };
  }
}
```

To map parameters to HTTP query strings for GET requests, omit the `body` field:

```protobuf
service TSRest {
  rpc SearchMessage(MessageRequest) returns(Message) {
    option (google.api.http) = {
      get: "/v1/messages"
    };
  }
}

message MessageRequest {
  string author = 1;
  string title = 2;
}
```

Invoking `SearchMessage({"author":"Author","title":"Message1"})` in SQL generates `GET /v1/messages?author=Author&title=Message1`.

To use HTTP annotations, import `google/api/annotations.proto` in your `.proto` file:

```protobuf
syntax = "proto3";

package yourpackage;

import "google/api/annotations.proto";
```

rekuiper bundles these proto files under `etc/services/schemas/google`.

#### Three-Layer Mapping Architecture

Schema-based services use a three-layer mapping model:

1. **Service Layer**: Defined by the JSON file name (for example, `sample.json`). Identifies the service in the [REST API](../../api/restapi/services.md).
2. **Interface Layer**: Defined in the `interfaces` section of the JSON file. Groups methods sharing common network endpoints, protocols, and schema files.
3. **Function Layer**: Defined as RPC methods in the `.proto` file. By default, the SQL function name matches the RPC name unless overridden in the `functions` mapping array.

In REST invocations, rekuiper serializes parameters to JSON. Field names convert to `lowerCamelCase` keys by default. To preserve exact field names, specify the `json_name` field option in the `.proto` definition.

#### Protocol Constraints

- **REST Services**:
  - Default HTTP method is `POST` unless configured in HTTP options.
  - If HTTP options are omitted, the input parameter must be a Protobuf `Message` or `google.protobuf.StringValue`.
  - 64-bit integers (`int64`) serialize to JSON strings.
- **msgpack-rpc Services**:
  - Function input cannot be empty.

### Schemaless External Functions

Schemaless external functions require only a JSON file without a `.proto` schema file.

Example configuration file `sample.json`:

```json
{
  "about": {
    "author": {
      "name": "Author Name",
      "email": "developer@example.org",
      "company": "Example Corp",
      "website": "https://example.org"
    },
    "helpUrl": {
      "en_US": "https://github.com/lf-edge/ekuiper/blob/master/docs/en_US/plugins/functions/functions.md",
      "zh_CN": "https://github.com/lf-edge/ekuiper/blob/master/docs/zh_CN/plugins/functions/functions.md"
    },
    "description": {
      "en_US": "Sample schemaless external service",
      "zh_CN": "示例无模式外部服务"
    }
  },
  "interfaces": {
    "tsschemaless": {
      "address": "http://localhost:8090",
      "protocol": "rest",
      "options": {
        "insecureSkipVerify": true,
        "headers": {
          "Accept-Charset": "utf-8"
        }
      },
      "schemaless": true
    }
  }
}
```

In schemaless mode:
- The SQL function name matches the interface name (`tsschemaless`).
- Schemaless functions support only the `rest` protocol.

## Registration and Management

You can register external services through two methods:

### File-Based Registration

Place service files in the `etc/services` directory before starting rekuiper:

- Service definitions: `etc/services/{serviceName}.json`
- Schema definitions: `etc/services/schemas/{schemaName}.proto`

Directory layout:

```text
etc
  services
    schemas
      sample.proto
      random.proto
    sample.json
    other.json
```

::: tip
rekuiper reads `etc/services` during startup. Modifying these files after startup does not reload services automatically. Use the REST API for dynamic updates.
:::

### Dynamic REST API Registration

Refer to the [External Services REST API](../../api/restapi/services.md) to register, update, describe, and delete external services at runtime.

## Usage in SQL Rules

### Schema-Based Function Invocation

After registering the service, invoke the mapped function in SQL rules:

```sql
SELECT objectDetection(cmd, img) FROM commandStream;
```

Ensure that the target service runs at `http://localhost:8090` and handles the `/object_detection` path.

#### Parameter Passing

You can pass arguments to schema-based functions in two ways:
1. **Single Struct Parameter**: Pass the entire object containing all request fields.
2. **Expanded Parameters**: Pass arguments as individual columns in the order defined by the Protobuf message:

```protobuf
message ObjectDetectionRequest {
  string cmd = 1;
  string base64_img = 2 [json_name="base64_img"];
}
```

In SQL, pass either a single struct or two individual string arguments (`cmd`, `base64_img`).

### Schemaless Function Invocation

Invoke schemaless functions by specifying the HTTP method, endpoint path, and payload parameters:

```sql
SELECT tsschemaless("post", "/object_detection", *) FROM schemalessStream;
```

- Parameter 1: HTTP method string (such as `"post"` or `"get"`).
- Parameter 2: Relative URL path (appended to the interface base address).
- Remaining Parameters: Serialized to JSON as the request body. If you provide one remaining parameter, it serializes as a JSON object. If you provide two or more parameters, they serialize as a JSON array.
