# State and Fault Tolerance

rekuiper supports stateful rule stream execution. This document explains state types, checkpoint configuration, and end-to-end processing guarantees.

## State Types

rekuiper manages two types of states:

1. **Internal state**: Used for window calculations and rewindable source offsets.
2. **User state**: Exposed to custom extensions through the stream context. For details, refer to [state storage](../../extension/native/overview.md#state-storage).

## Fault Tolerance

By default, all states reside only in memory. If a rule stops unexpectedly, the runtime loses all state data.

To make states fault-tolerant, rekuiper creates periodic checkpoints of the rule state in persistent storage. Checkpoints permit state restoration after a system failure.

### Enable Checkpointing

To enable state checkpointing, set the rule option `qos` to `1` or `2`. Set the checkpoint interval with the `checkpointInterval` option.

If a failure occurs during stream execution, data can be lost or duplicated. The three `qos` options provide these behaviors:

1. **At-most-once (`0`)**: rekuiper does not recover state after failures. Data can be lost.
2. **At-least-once (`1`)**: rekuiper prevents data loss, but duplicate records can occur.
3. **Exactly-once (`2`)**: rekuiper prevents data loss and duplicate state updates.

rekuiper recovers from faults when it rewinds and replays source data streams. "Exactly-once" does not mean that every event executes through the pipeline only once. It means that every event updates the managed state in rekuiper exactly once.

If your application does not require exactly-once processing, select `1` (`AT_LEAST_ONCE`) to maximize throughput performance.

### End-to-End Exactly-Once Processing

#### Source Requirements

To achieve end-to-end quality of service, the stream source must be rewindable.

After a recovery operation, the source resets its position to the checkpointed offset. The source then replays data from that offset. This replay restores the stream state from the moment before the failure.

For custom source extensions, implement the `api.Rewindable` interface and the standard `api.Source` interface. rekuiper manages the rewind procedure automatically.

```go
type Rewindable interface {
    GetOffset() (interface{}, error)
    Rewind(offset interface{}) error
}
```

#### Sink Requirements

rekuiper cannot guarantee that an external sink receives records exactly once.

If a failure occurs between checkpoints, the engine replays records that it already sent to the sink. The sink can receive these duplicate records.

To achieve end-to-end exactly-once delivery, configure deduplication mechanisms in the target sink system.
