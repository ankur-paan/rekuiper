# Data Merging

In IoT environments, applications frequently collect data from multiple related sensors. Devices often report telemetry independently at different sample rates and through different channels. This section describes common data merging patterns and explains how to combine sensor telemetry with rekuiper SQL:

- [Merge Multiple Devices' Data in a Single Stream](./merge_single_stream.md): Combines interleaved sensor records from one stream into unified records.
- [Merge Data in Multiple Streams](./merge_multi_stream.md): Combines records from separate streams by using memory pipelines or stream joins.
