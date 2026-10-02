# Services Management

The rekuiper REST API manages external service integrations and their associated functions. You can register, list, describe, update, and delete external services.

## Register an External Service

Use this endpoint to register a new external service using a JSON specification:

```http
POST http://localhost:9081/services
```

Request payload using a remote HTTP URL:

```json
{
  "name": "sample",
  "file": "http://127.0.0.1/services/sample.zip"
}
```

Request payload using a local filesystem URI:

```json
{
  "name": "sample",
  "file": "file:///var/services/sample.zip"
}
```

### Parameters

- `name`: The unique identifier for the service. It must match the filename of the JSON service definition inside the `.zip` archive.
- `file`: The URL or local filesystem URI pointing to the service `.zip` archive.

### Service Archive Structure

An example `sample.zip` archive contains:
1. `sample.json`: Service definition metadata and interface mappings.
2. `schema/`: A directory containing schema files used by the service (for example, `sample.proto`).

## Display External Services

Use this endpoint to list all registered external services:

```http
GET http://localhost:9081/services
```

Response sample:

```json
["sample", "sample2"]
```

## Describe an External Service

Use this endpoint to display the definition of a specific external service:

```http
GET http://localhost:9081/services/{name}
```

The path parameter `name` specifies the service name.

## Delete an External Service

Use this endpoint to delete an external service and unregister all its functions:

```http
DELETE http://localhost:9081/services/{name}
```

## Update an External Service

Use this endpoint to update an existing external service definition:

```http
PUT http://localhost:9081/services/{name}
Content-Type: application/json

{
  "name": "sample",
  "file": "http://127.0.0.1/services/sample.zip"
}
```

## Display All External Functions

Use this endpoint to list all registered external functions available for use in SQL queries:

```http
GET http://localhost:9081/services/functions
```

Response sample:

```json
[
  {
    "ServiceName": "serviceName",
    "InterfaceName": "interfaceName",
    "Addr": "http://192.168.2.102:9090",
    "MethodName": "funcName",
    "FuncName": "funcName"
  }
]
```

## Describe an External Function

Use this endpoint to display service endpoint mappings for a specific function:

```http
GET http://localhost:9081/services/functions/{name}
```

Response sample:

```json
{
  "ServiceName": "serviceName",
  "InterfaceName": "interfaceName",
  "Addr": "http://192.168.2.102:9090",
  "MethodName": "funcName",
  "FuncName": "funcName"
}
```
