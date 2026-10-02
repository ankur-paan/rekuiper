# Kafka Source Connector

<span style="background:green;color:white;padding:1px;margin:2px">stream source</span>

The Kafka source connector consumes streaming records from Apache Kafka topics into the rekuiper stream processing engine.

## Configuration Overview

Configure the Kafka source in `$rekuiper/etc/sources/kafka.yaml`:

```yaml
default:
  brokers: "127.0.0.1:9091,127.0.0.1:9092"
  groupID: ""
  partition: 0
  maxBytes: 1000000
```

Verify broker reachability before runtime using the [Connectivity Check API](../../../api/restapi/connection.md#connectivity-check).

### Configuration Properties

| Property Name | Optional | Description |
|---|---|---|
| `brokers` | False | Comma-separated list of Kafka broker addresses (`host:port`). |
| `saslAuthType` | True | SASL authentication mechanism: `"none"`, `"plain"`, or `"scram"`. Default is `"none"`. |
| `saslUserName` | True | SASL username credential. |
| `password` | True | SASL password credential. |
| `insecureSkipVerify` | True | Boolean. Set to `true` to skip TLS certificate verification. |
| `certificationPath` | True | Path to client certificate file for mTLS. |
| `privateKeyPath` | True | Path to client private key file for mTLS. |
| `rootCaPath` | True | Path to Root CA certificate file. |
| `certficationRaw` | True | Base64-encoded client certificate string. |
| `privateKeyRaw` | True | Base64-encoded client private key string. |
| `rootCARaw` | True | Base64-encoded Root CA certificate string. |
| `maxBytes` | True | Maximum bytes fetched per Kafka message batch. Default is `1000000` (1 MB). |
| `groupID` | True | Kafka consumer group identifier. |
| `partition` | True | Specific partition index consumed by the connector. |

## Create a Stream Source

Define a stream using SQL DDL. Set `DATASOURCE` to the target Kafka topic:

```sql
CREATE STREAM kafka_stream () WITH (
  TYPE = "kafka",
  DATASOURCE = "telemetry_topic",
  FORMAT = "json"
);
```

For REST API and CLI management procedures, refer to [Streams Management with REST API](../../../api/restapi/streams.md) and [Streams Management with CLI](../../../api/cli/streams.md).
