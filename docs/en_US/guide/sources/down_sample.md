# Down Sampling

Down sampling aggregates incoming records at the source layer before payload decoding and transformation.

You can also aggregate streaming data using time windows. However, time windows decode every incoming event before evaluating the window condition. Discarded events consume CPU cycles during decoding.

Source down sampling filters or merges raw events before decoding. This design reduces CPU usage and memory consumption. As the ratio between input frequency and sampling frequency increases, efficiency gains become more significant.

## Applicable Source Categories

Connectors belong to two categories based on data ingestion:

- **Pull Sources**: The engine polls the external system at scheduled intervals. Set the polling interval to control down sampling.
- **Push Sources**: The external publisher controls ingestion frequency. The push source buffers incoming records and emits them based on the configured down sampling strategy.

## Configuration Properties

Configure down sampling using these source properties:

### interval

Specifies the sampling duration using a duration string (such as `"10s"` or `"500ms"`).

- For pull sources, `interval` defines the polling period.
- For push sources, the connector accumulates incoming events during the interval and emits records when the interval elapses.

### mergeField

Defines the column name used for record aggregation (for example, `"id"`).

The default down sampling strategy emits the last record received during the sampling period. If you configure `mergeField`, the engine retains the last record for each distinct key value and merges them into a single composite record.

> [!NOTE]
> - `mergeField` requires formats that support partial decoding (such as JSON). Custom formats can implement `message.PartialDecoder`.
> - `mergeField` supports top-level non-composite fields. Nested objects or array structures resolve to the same key.

## Down Sampling Strategies

Source down sampling converts multiple events received during an interval into a single output event. rekuiper supports two strategies:

1. **Latest Value**: Emits the final record received during the sampling period.
2. **Column Aggregation**: Merges the latest records for each unique key in `mergeField`.

### Strategy 1: Latest Value

This strategy outputs the last record received during the sampling interval.

#### Step 1: Create the Source Configuration

Create an MQTT configuration named `onesec` with a 1-second sampling period:

```http
PUT http://{{host}}/metadata/sources/mqtt/confKeys/onesec
Content-Type: application/json

{
  "interval": "1s"
}
```

#### Step 2: Create the Stream

Create a stream that uses the `onesec` configuration:

```http
POST http://{{host}}/streams
Content-Type: application/json

{
  "sql": "CREATE STREAM mqttOneSec() WITH (TYPE=\"mqtt\", FORMAT=\"json\", DATASOURCE=\"demo\", CONF_KEY=\"onesec\");"
}
```

#### Step 3: Create the Rule

Create a rule to process the downsampled stream:

```http
POST http://{{host}}/rules
Content-Type: application/json

{
  "id": "ruleOneSecLatest",
  "sql": "SELECT * FROM mqttOneSec",
  "actions": [
    {
      "mqtt": {
        "server": "tcp://127.0.0.1:1883",
        "topic": "result/onesec",
        "sendSingle": true
      }
    }
  ]
}
```

The rule receives records once per second and emits the latest record for each interval.

### Strategy 2: Column Aggregation

This strategy groups records by key and combines the latest values across distinct fields into a single record.

Consider this sequence of input events within a 1-second window:

```json
{"id": 1, "temperature": 20}
{"id": 2, "humidity": 80}
{"id": 1, "temperature": 30}
```

The engine merges the events into one composite record:

```json
{
  "id": 1,
  "temperature": 30,
  "humidity": 80
}
```

#### Step 1: Create the Merge Configuration

Create an MQTT configuration named `onesec_merge` with `interval` and `mergeField`:

```http
PUT http://{{host}}/metadata/sources/mqtt/confKeys/onesec_merge
Content-Type: application/json

{
  "interval": "1s",
  "mergeField": "id"
}
```

#### Step 2: Create the Stream

Create a stream referencing `onesec_merge`:

```http
POST http://{{host}}/streams
Content-Type: application/json

{
  "sql": "CREATE STREAM mqttOneSecM() WITH (TYPE=\"mqtt\", FORMAT=\"json\", DATASOURCE=\"demo\", CONF_KEY=\"onesec_merge\");"
}
```

#### Step 3: Create the Rule

Create a rule to process the merged stream:

```http
POST http://{{host}}/rules
Content-Type: application/json

{
  "id": "RuleOneSecM",
  "sql": "SELECT * FROM mqttOneSecM",
  "actions": [
    {
      "mqtt": {
        "server": "tcp://127.0.0.1:1883",
        "topic": "result/onesecm",
        "sendSingle": true
      }
    }
  ]
}
```

### Full Aggregation with Windows

To aggregate records across all columns without specifying a key field, use a time window with the `merge_agg` function:

```sql
SELECT merge_agg(*)
FROM normalStream
GROUP BY TumblingWindow(ss, 1);
```

Source down sampling minimizes decoding overhead by inspecting only key fields before decoding. Full aggregation decodes every event payload. For full aggregation, ingest records using a standard stream and apply window functions in SQL.

## Down Sampling Observability

The `ratelimit` operator executes source down sampling. Inspect the `ratelimit` metrics to monitor down sampling efficiency.

In the following status output:
- `source_mqttOneMiMerge_0_records_out_total` indicates the MQTT connector ingested 25 records.
- `op_2_ratelimit_0_records_in_total` is 25, and `op_2_ratelimit_0_records_out_total` is 1. The operator reduced 25 incoming records to 1 record.
- The downstream `op_3_payload_decoder_0` decoded only the single downsampled record.

```json
{
  "status": "running",
  "lastStartTimestamp": "1720151899579",
  "lastStopTimestamp": "0",
  "nextStopTimestamp": "0",
  "source_mqttOneMiMerge_0_records_in_total": 25,
  "source_mqttOneMiMerge_0_records_out_total": 25,
  "source_mqttOneMiMerge_0_messages_processed_total": 25,
  "source_mqttOneMiMerge_0_process_latency_us": 0,
  "source_mqttOneMiMerge_0_buffer_length": 0,
  "source_mqttOneMiMerge_0_last_invocation": "2024-07-05T11:58:40.733398",
  "source_mqttOneMiMerge_0_exceptions_total": 0,
  "source_mqttOneMiMerge_0_last_exception": "",
  "source_mqttOneMiMerge_0_last_exception_time": 0,
  "op_2_ratelimit_0_records_in_total": 25,
  "op_2_ratelimit_0_records_out_total": 1,
  "op_2_ratelimit_0_messages_processed_total": 25,
  "op_2_ratelimit_0_process_latency_us": 0,
  "op_2_ratelimit_0_buffer_length": 0,
  "op_2_ratelimit_0_last_invocation": "2024-07-05T11:58:40.733398",
  "op_2_ratelimit_0_exceptions_total": 0,
  "op_2_ratelimit_0_last_exception": "",
  "op_2_ratelimit_0_last_exception_time": 0,
  "op_3_payload_decoder_0_records_in_total": 1,
  "op_3_payload_decoder_0_records_out_total": 1,
  "op_3_payload_decoder_0_messages_processed_total": 1,
  "op_3_payload_decoder_0_process_latency_us": 0,
  "op_3_payload_decoder_0_buffer_length": 0,
  "op_3_payload_decoder_0_last_invocation": "2024-07-05T11:59:19.59698",
  "op_3_payload_decoder_0_exceptions_total": 0,
  "op_3_payload_decoder_0_last_exception": "",
  "op_3_payload_decoder_0_last_exception_time": 0,
  "op_4_project_0_records_in_total": 1,
  "op_4_project_0_records_out_total": 1,
  "op_4_project_0_messages_processed_total": 1,
  "op_4_project_0_process_latency_us": 0,
  "op_4_project_0_buffer_length": 0,
  "op_4_project_0_last_invocation": "2024-07-05T11:59:19.59698",
  "op_4_project_0_exceptions_total": 0,
  "op_4_project_0_last_exception": "",
  "op_4_project_0_last_exception_time": 0,
  "op_mqtt_0_0_transform_0_records_in_total": 1,
  "op_mqtt_0_0_transform_0_records_out_total": 1,
  "op_mqtt_0_0_transform_0_messages_processed_total": 1,
  "op_mqtt_0_0_transform_0_process_latency_us": 0,
  "op_mqtt_0_0_transform_0_buffer_length": 0,
  "op_mqtt_0_0_transform_0_last_invocation": "2024-07-05T11:59:19.59698",
  "op_mqtt_0_0_transform_0_exceptions_total": 0,
  "op_mqtt_0_0_transform_0_last_exception": "",
  "op_mqtt_0_0_transform_0_last_exception_time": 0,
  "op_mqtt_0_1_encode_0_records_in_total": 1,
  "op_mqtt_0_1_encode_0_records_out_total": 1,
  "op_mqtt_0_1_encode_0_messages_processed_total": 1,
  "op_mqtt_0_1_encode_0_process_latency_us": 0,
  "op_mqtt_0_1_encode_0_buffer_length": 0,
  "op_mqtt_0_1_encode_0_last_invocation": "2024-07-05T11:59:19.59698",
  "op_mqtt_0_1_encode_0_exceptions_total": 0,
  "op_mqtt_0_1_encode_0_last_exception": "",
  "op_mqtt_0_1_encode_0_last_exception_time": 0,
  "sink_mqtt_0_0_records_in_total": 1,
  "sink_mqtt_0_0_records_out_total": 1,
  "sink_mqtt_0_0_messages_processed_total": 1,
  "sink_mqtt_0_0_process_latency_us": 0,
  "sink_mqtt_0_0_buffer_length": 0,
  "sink_mqtt_0_0_last_invocation": "2024-07-05T11:59:19.59698",
  "sink_mqtt_0_0_exceptions_total": 0,
  "sink_mqtt_0_0_last_exception": "",
  "sink_mqtt_0_0_last_exception_time": 0
}
```
