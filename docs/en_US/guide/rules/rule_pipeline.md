# Rule Pipelines

You can create rule pipelines by routing the output of an upstream rule into downstream rules. Pipelines can use external message brokers, such as MQTT, or internal connectors.

Using the [Memory Source](../sources/builtin/memory.md) and [Memory Sink](../sinks/builtin/memory.md) creates internal pipelines without external broker dependencies.

## Implementation Procedure

Rule pipelines connect through shared memory topics. You create each pipeline stage independently through the REST API or CLI:

```shell
# 1. Create the input source stream
{"sql": "CREATE STREAM demo () WITH (DATASOURCE=\"demo\", FORMAT=\"JSON\")"}

# 2. Create the first rule that routes output to an in-memory topic
{
  "id": "rule1",
  "sql": "SELECT * FROM demo WHERE isNull(temperature)=false",
  "actions": [{
    "log": {},
    "memory": {
      "topic": "home/ch1/sensor1"
    }
  }]
}

# 3. Create a downstream stream from the memory topic
{"sql": "CREATE STREAM sensor1 () WITH (DATASOURCE=\"home/+/sensor1\", FORMAT=\"JSON\", TYPE=\"memory\")"}

# 4. Create downstream rules that consume the memory topic
{
  "id": "rule2-1",
  "sql": "SELECT avg(temperature) FROM sensor1 GROUP BY CountWindow(10)",
  "actions": [{
    "log": {},
    "memory": {
      "topic": "analytic/sensors"
    }
  }]
}

{
  "id": "rule2-2",
  "sql": "SELECT temperature + 273.15 as k FROM sensor1",
  "actions": [{
    "log": {}
  }]
}
```

The memory topic connects `rule1` to downstream rules: `rule1 -> {rule2-1, rule2-2}`.

Notes:
- You can combine the memory sink with other sinks in the `actions` array of a single rule.
- Memory sources support MQTT-style wildcards (such as `+`) to subscribe to multiple memory topics.

