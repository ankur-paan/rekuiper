# Memory Sink Connector

<span style="background:green;color:white;padding:1px;margin:2px">updatable</span>

The Memory sink connector publishes query results to internal in-memory topics consumed by the [Memory source](../../sources/builtin/memory.md).

Multiple sinks can publish to the same topic, and multiple sources can subscribe to the same topic. Use memory sinks and sources to construct [Rule Pipelines](../../rules/rule_pipeline.md).

## Configuration Properties

| Property Name | Optional | Description |
|---|---|---|
| `topic` | False | Target in-memory topic path (for example, `analysis/result`). Supports dynamic templates (such as <code v-pre>{{.topic}}</code>). |
| `rowkindField` | True | Field name specifying the action command (`insert`, `update`, `upsert`, or `delete`). Default is `insert`. |
| `keyField` | True | Primary key field used for table index updates in updatable sink configurations. |

The Memory sink supports all [common sink properties](../overview.md#common-properties).

### Example Configurations

Static topic destination:

```json
{
  "memory": {
    "topic": "devices/result"
  }
}
```

Dynamic topic destination:

```json
{
  "memory": {
    "topic": "{{.topic}}"
  }
}
```

## Data Templates in Memory Sinks

Memory sinks transfer data objects directly in memory without serialization or deserialization. The sink ignores general format properties.

If you specify `dataTemplate`, the template must produce a valid JSON object string (for example, <code v-pre>{"key": "{{.key}}"}</code>). Array strings or raw non-JSON text are not supported.

## Updatable Memory Sinks

The Memory sink functions as an [Updatable Sink](../overview.md#updatable-sinks). Updatable memory sinks modify the state of in-memory lookup tables subscribed to the same topic.

In this example, the rule writes state updates to the in-memory topic `alertVal`. The `action` field specifies the operation verb (`upsert`, `delete`), and `id` serves as the primary key:

```json
{
  "id": "ruleUpdateAlert",
  "sql": "SELECT * FROM alertStream",
  "actions": [
    {
      "memory": {
        "keyField": "id",
        "rowkindField": "action",
        "topic": "alertVal",
        "sendSingle": true
      }
    }
  ]
}
```
