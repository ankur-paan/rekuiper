# Time in Stream Processing

Streaming data forms a time-sequenced series. Time is an intrinsic attribute of streaming data. Time attributes determine how window aggregations group and process records.

## Time Domains

rekuiper supports two time domains:

- **Event Time**: The timestamp when the event occurred on the source device. Records typically include a timestamp field that indicates production time.
- **Processing Time**: The timestamp when the rekuiper engine receives and processes the event.

## Event Time and Watermarks

Stream processors require a mechanism to measure event time progress. For example, an hourly window operator must determine when event time passes the hour boundary to close the window.

rekuiper uses watermarks to track event time progress. Watermarks flow through the data stream and carry a timestamp `t`.

A watermark `Watermark(t)` declares that event time reached timestamp `t`. Subsequent records in that stream must not have timestamps `t' <= t`.

In rekuiper, watermarks operate at the rule level. When a rule consumes multiple streams, the watermark tracks time progress across all input streams.

