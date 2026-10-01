# Join of sources

Currently, join is the only way to merge multiple sources in rekuiper. It requires a way to align multiple sources and trigger the join result.

The supported joins in rekuiper include:

- Join of streams: must do in a window.
- Join of stream and table: the stream will be the trigger of join operation.

The supported join type includes LEFT, RIGHT, FULL & CROSS in rekuiper.
