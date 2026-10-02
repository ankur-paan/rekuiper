# Joining Sources

In rekuiper, `JOIN` operations correlate data from multiple sources.

rekuiper supports two join combinations:

- **Stream-to-Stream Join**: Joins multiple unbounded streams. Stream-to-stream joins require an explicit window definition.
- **Stream-to-Table Join**: Joins a stream to a table. Arriving stream events trigger join calculations against current table records.

Supported join types include `INNER`, `LEFT`, `RIGHT`, `FULL`, and `CROSS`.

