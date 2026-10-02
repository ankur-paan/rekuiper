# Rules

Each rule represents a processing task in rekuiper. It defines the continuous data source input, the computing logic, and the output actions.

## Rule Lifecycle

rekuiper supports streaming rules. These rules require at least one continuous stream as an input data source.

Once started, a rule executes continuously until one of these conditions occurs:

1. An operator sends an explicit stop command.
2. The rule terminates because of an internal error or engine shutdown.

### Asynchronous Rule Startup and Status Management

Rule startup is asynchronous. When a client submits a start command, rekuiper completes static checks and begins rule startup asynchronously.

Therefore:

* The response confirms only that rekuiper accepted the start request. The engine sets the **Expected Status** of the rule to `started`.
* The response does not confirm that rule execution has begun. Check the **Runtime Status** of the rule to verify that execution is active.

### Rule Updates and Error Rollback

rekuiper provides rollback support during rule updates. If an updated rule fails to start, the engine continues to execute the previous rule version.

## Rule Relationships

You can run multiple rules simultaneously. rekuiper runs as a single process, and all rules share memory space. The engine isolates rules at runtime so an error in one rule does not terminate other rules.

All rules share hardware resources. You can configure operator buffer limits on individual rules to control resource consumption.

When multiple rules reference a **[Shared Stream](../guide/streams/overview.md#share-source-instance-across-rules)**, they share upstream source components for ingestion and decoding.

Rules that reference a shared stream form a single Directed Acyclic Graph (DAG). You can add or remove downstream rules dynamically.

### Effects of Shared Streams

Rules within a shared stream DAG interact in these ways:

* **Backpressure Propagation**: Backpressure from one rule propagates to the shared source component.
* **System Impact**: Backpressure on the shared source affects the performance of all rules connected to that source.
* **Checkpoints**: The shared source ignores checkpoint operations.

## Rule Pipelines

You can connect multiple rules into a processing pipeline through intermediate sources and sinks. For example, a first rule sends output to an in-memory sink topic, and a second rule reads that topic through an in-memory source.

You can also use MQTT topics or other connector pairs to connect rules.

## Further Reading

* [Rule Reference](../guide/rules/overview.md)

