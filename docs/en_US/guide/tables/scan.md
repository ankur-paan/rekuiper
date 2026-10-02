# Scan Table Scenarios

Rules join scan tables with data streams, with or without time windows. When a rule joins a stream with a table, the table acts as reference data. Changes to table records do not trigger rule evaluation.

## Data Enrichment

Use a scan table to enrich real-time events with static reference data.

The following example defines a table that reads data from a local JSON file:

```sql
CREATE TABLE table1 (
    id BIGINT,
    name STRING
) WITH (DATASOURCE="lookup.json", FORMAT="JSON", TYPE="file");

SELECT * FROM demo INNER JOIN table1 ON demo.id = table1.id;
```

In this query, rekuiper joins the `demo` stream with `table1`. The query matches `demo.id` with `table1.id` to retrieve the `name` field.

The `lookup.json` file must contain an array of JSON objects:

```json
[
  {
    "id": 1541152486013,
    "name": "name1"
  },
  {
    "id": 1541152487632,
    "name": "name2"
  },
  {
    "id": 1541152489252,
    "name": "name3"
  }
]
```

## Filter by Historical State

You can use a scan table to filter a data stream based on control signals from a separate topic:

```sql
CREATE TABLE stateTable (
    id BIGINT,
    triggered bool
) WITH (DATASOURCE="myTopic", FORMAT="JSON", TYPE="mqtt");

SELECT * FROM demo LEFT JOIN stateTable ON demo.id = stateTable.id WHERE triggered = true;
```

In this example, `stateTable` stores the latest trigger status received from the MQTT topic `myTopic`. The rule filters records from the `demo` stream and processes only events where `triggered` equals `true`.
