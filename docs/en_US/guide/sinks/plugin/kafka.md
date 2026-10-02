# Kafka Sink

The Kafka sink publishes output messages to an Apache Kafka topic.

## Compile and Deploy the Plugin

### Build in Shell

Run the following commands to compile and install the plugin:

```shell
cd $rekuiper_src
go build -trimpath --buildmode=plugin -o plugins/sinks/kafka.so extensions/sinks/kafka/kafka.go
zip kafka.zip plugins/sinks/kafka.so
cp kafka.zip /root/tomcat_path/webapps/ROOT/
bin/kuiper create plugin sink kafka -f /tmp/kafkaPlugin.txt
bin/kuiper create rule kafka -f /tmp/kafkaRule.txt
```

### Build with Docker Image

```shell
docker build -t demo/plugins:v1 -f build/plugins/Dockerfile .
docker run demo/plugins:v1
docker cp 90eae15a7245:/workspace/_plugins/debian/sinks /tmp
```

Example Dockerfile:

```dockerfile
ARG GO_VERSION=1.25.4
FROM ghcr.io/lf-edge/ekuiper/base:$GO_VERSION-debian AS builder
WORKDIR /workspace
ADD . /workspace/
RUN go env -w GOPROXY=https://goproxy.cn,direct
RUN make plugins_c
CMD ["sleep","3600"]
```

Add the following lines to your `Makefile`:

```dockerfile
PLUGINS_CUSTOM := sinks/kafka

.PHONY: plugins_c $(PLUGINS_CUSTOM)
plugins_c: $(PLUGINS_CUSTOM)

$(PLUGINS_CUSTOM): PLUGIN_TYPE = $(word 1, $(subst /, , $@))
$(PLUGINS_CUSTOM): PLUGIN_NAME = $(word 2, $(subst /, , $@))
$(PLUGINS_CUSTOM):
	@$(CURDIR)/build-plugins.sh $(PLUGIN_TYPE) $(PLUGIN_NAME)
```

Restart the rekuiper server to activate the plugin.

## Properties

| Property name | Optional | Description |
|---|---|---|
| connectionSelector | true | Reuses a configured Kafka connection. When set, rekuiper copies broker addresses, SASL credentials, and TLS settings from the selected connection. |
| brokers | true | Comma-separated list of broker addresses. Required when `connectionSelector` is not configured. |
| topic | false | The target Kafka topic name. |
| saslAuthType | false | The SASL authentication mechanism: `none`, `plain`, or `scram`. |
| saslUserName | true | The SASL authentication username. |
| password | true | The SASL authentication password. |
| insecureSkipVerify | true | Controls whether to skip SSL/TLS certificate verification. |
| certificationPath | true | The client certificate file path for TLS verification. |
| privateKeyPath | true | The client private key file path for TLS verification. |
| rootCaPath | true | The root CA certificate file path for TLS verification. |
| certficationRaw | true | Base64-encoded raw text of the client certificate. rekuiper uses `certificationPath` first if you define both. |
| privateKeyRaw | true | Base64-encoded raw text of the private key. rekuiper uses `privateKeyPath` first if you define both. |
| rootCARaw | true | Base64-encoded raw text of the root CA certificate. rekuiper uses `rootCaPath` first if you define both. |
| maxAttempts | true | Number of retry attempts when sending messages to the broker. Default: `1`. |
| requiredACKs | true | Producer acknowledgment mode: `1` waits for leader confirmation, `-1` waits for all replicas, `0` does not wait for confirmation. Default: `1`. |
| key | true | Key metadata attached to messages sent to Kafka. |
| headers | true | Header metadata attached to messages sent to Kafka. |
| compression | true | Compression codec for published messages: `gzip`, `snappy`, `lz4`, or `zstd`. |
| batchBytes | true | Maximum batch size in bytes for message publication. Default: `1048576`. |

You can verify the connectivity of the sink endpoint before rule execution by using the REST API: [Connectivity Check](../../../api/restapi/connection.md#connectivity-check).

### Connection Reuse

You can create a reusable Kafka connection and reference it in sinks by using `connectionSelector`. The connection manages broker health checks and status reporting. The Kafka sink copies settings from the connection and creates a dedicated producer:

Create a Kafka connection:

```shell
POST http://localhost:9081/connections
{
  "id": "kafka-1",
  "typ": "kafka",
  "props": {
    "brokers": "127.0.0.1:9092",
    "saslAuthType": "none"
  }
}
```

Reference the connection in the sink configuration:

```json
{
  "id": "kafka",
  "sql": "SELECT * FROM demo_stream",
  "actions": [
    {
      "kafka": {
        "connectionSelector": "kafka-1",
        "topic": "test_topic"
      }
    }
  ]
}
```

When `connectionSelector` is configured, the sink ignores locally specified connection settings including `brokers`, `saslAuthType`, `saslUserName`, `password`, `insecureSkipVerify`, and TLS certificate properties.

### Set Kafka Key and Headers

Configure static metadata on published messages:

```json
{
  "key": "keyValue",
  "headers": {
    "headerKey1": "headerValue1",
    "headerKey2": "headerValue2"
  }
}
```

Configure dynamic metadata using template syntax:

```json
{
  "key": "{{.data.key}}",
  "headers": {
    "headerKey1": "{{.data.col1}}",
    "headerKey2": "{{.data.col2}}"
  }
}
```

Configure a JSON map structure for the message key:

```json
{
  "key": "{\"keyMapkey\":\"{{.data.key.value}}\"}"
}
```

Other common sink properties are supported. Refer to [sink common properties](../overview.md#common-properties) for more information.

## Sample Usage

The following sample rule filters records where temperature exceeds 50 and publishes results to Kafka:

### /tmp/kafkaRule.txt

```json
{
  "id": "kafka",
  "sql": "SELECT * from demo_stream where temperature > 50",
  "actions": [
    {
      "log": {}
    },
    {
      "kafka": {
        "brokers": "127.0.0.1:9092,127.0.0.2:9092",
        "topic": "test_topic",
        "saslAuthType": "none"
      }
    }
  ]
}
```

### /tmp/kafkaPlugin.txt

```json
{
  "file": "http://localhost:8080/kafka.zip"
}
```

## Docker Configuration Note

When rekuiper and Kafka run in the same Docker network, configure broker addresses using the Kafka container hostname.

In Kafka, configure `KAFKA_CFG_ADVERTISED_LISTENERS` to the host IP address:

```yaml
zookeeper:
  image: docker.io/bitnami/zookeeper:3.8
  hostname: zookeeper
  container_name: zookeeper
  ports:
    - "2181:2181"
  volumes:
    - "zookeeper_data:/bitnami"
  environment:
    - ALLOW_ANONYMOUS_LOGIN=yes
kafka:
  image: docker.io/soldevelo/kafka:3.4
  hostname: kafka
  container_name: kafka
  ports:
    - "9092:9092"
  volumes:
    - "kafka_data:/bitnami"
  environment:
    - KAFKA_CFG_ZOOKEEPER_CONNECT=zookeeper:2181
    - ALLOW_PLAINTEXT_LISTENER=yes
    - KAFKA_CFG_LISTENERS=PLAINTEXT://:9092
    - KAFKA_CFG_ADVERTISED_LISTENERS=PLAINTEXT://<YOUR_HOST_IP>:9092
  depends_on:
    - zookeeper
```
