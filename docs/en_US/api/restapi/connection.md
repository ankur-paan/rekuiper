# Connection Management

> [!NOTE]
> **Verification Status**: Tested and Verified against `rekuiper` engine on **2026-09-30 22:15:00 UTC**.  
> The `/connections` REST endpoints allow defining and managing independent connection objects (`mqtt`, `kafka`, `sql`, etc.). In stream, table, and rule definitions, connections can be referenced via `CONF_KEY` or `connectionSelector` (supported as a first-class alias), which resolves configuration properties directly or from `connections/{id}`.

The rekuiper REST API manages shared connection profiles for sources and sinks. You can create, list, inspect, update, delete connections, and run connectivity health checks.

## Manage Connections

Supported connection types include `mqtt`, `nng`, `httppush`, `websocket`, `edgex`, `sql`, and `kafka`.

### Create a Connection

Use this endpoint to define a reusable connection profile:

```http
POST http://localhost:9081/connections
Content-Type: application/json
```

Example MQTT connection:

```json
{
  "id": "connection-1",
  "typ": "mqtt",
  "props": {
    "server": "tcp://127.0.0.1:1883"
  }
}
```

Example Kafka connection:

```json
{
  "id": "kafka-1",
  "typ": "kafka",
  "props": {
    "brokers": "127.0.0.1:9092",
    "saslAuthType": "none"
  }
}
```

### Update a Connection

Use this endpoint to update an existing connection profile:

```http
PUT http://localhost:9081/connections/{id}
Content-Type: application/json

{
  "id": "connection-1",
  "typ": "mqtt",
  "props": {
    "server": "tcp://127.0.0.1:1883"
  }
}
```

> [!NOTE]
> You cannot modify a connection profile while active rules reference it.

### Get All Connections

Use this endpoint to retrieve all connection profiles and their runtime statuses:

```http
GET http://localhost:9081/connections
```

### Get a Single Connection Status

Use this endpoint to inspect the status of a specific connection:

```http
GET http://localhost:9081/connections/{id}
```

### Delete a Connection

Use this endpoint to delete a connection profile:

```http
DELETE http://localhost:9081/connections/{id}
```

> [!NOTE]
> You cannot delete a connection profile while active rules reference it. Stop or update dependent rules before deleting the connection.

> [!NOTE]
> **Compatibility Note: 404 Not Found vs 400 Bad Request for Missing Connections**
> In legacy eKuiper, querying (`GET /connections/{id}`) or deleting (`DELETE /connections/{id}`) a non-existent connection ID returns `400 Bad Request`. `rekuiper` strictly returns `404 Not Found`.
> 
> **Why we chose this difference**: Per RFC 9110, HTTP `400 Bad Request` indicates client syntax error (such as an unparseable body or missing mandatory fields), whereas `404 Not Found` specifically indicates that the identified target resource does not exist. Using `404` allows client applications to cleanly detect missing connection profiles and automatically trigger provisioning or fallbacks without misinterpreting the error as a syntax malfunction.


## Connectivity Health Checks

Test external endpoint reachability using these diagnostic endpoints:

### Sink Connectivity Check

Use this endpoint to verify reachability and credential validity for a sink destination:

```http
POST http://localhost:9081/metadata/sinks/connection/{sink}
Content-Type: application/json
```

Example MySQL SQL sink connectivity check:

```json
{
  "url": "mysql://root@127.0.0.1:4000/test",
  "table": "test",
  "fields": ["a", "b", "c"]
}
```

### Source Connectivity Check

Use this endpoint to verify reachability and credential validity for an upstream source:

```http
POST http://localhost:9081/metadata/sources/connection/{source}
Content-Type: application/json
```

Example MySQL SQL source connectivity check:

```json
{
  "url": "mysql://root@127.0.0.1:4000/test"
}
```
