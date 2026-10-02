# Notify When Current Changes

In IoT environments, monitoring metric variations to trigger events is a common requirement. This article uses electrical current measurements to demonstrate rekuiper SQL rules for event triggering.

## Background

In this scenario, an input stream transmits current measurements, timestamps, and device identifiers. The examples use this sample input data:

```json
{
  "current": 300,
  "ts": 1,
  "deviceId": 1
}
{
  "current": 400,
  "ts": 2,
  "deviceId": 2
}
{
  "current": 200,
  "ts": 3,
  "deviceId": 1
}
{
  "current": 200,
  "ts": 4,
  "deviceId": 2
}
{
  "current": 500,
  "ts": 5,
  "deviceId": 1
}
{
  "current": 200,
  "ts": 6,
  "deviceId": 2
}
{
  "current": 400,
  "ts": 7,
  "deviceId": 1
}
{
  "current": 600,
  "ts": 8,
  "deviceId": 2
}
```

### Trigger When a Changed Value Crosses a Threshold

In IoT applications, monitoring whether a value exceeds a threshold often triggers alarms. Simple comparison (`current > 300`) emits an alarm for every incoming event while the condition remains true. You can detect state transitions to trigger an alarm only when the value transitions from below the threshold to above the threshold.

#### 1. Changed Current Value Exceeds 300 Across All Devices

```sql
SELECT current, ts
FROM demo
WHERE current > 300 AND lag(current) <= 300;
```

This rule compares the current value with the previous value from the stream. The rule triggers an event only when the value transitions across 300:

```json
{"current":400,"ts":2}
{
  "current": 500,
  "ts": 5
}
{
  "current": 400,
  "ts": 7
}
```

This rule evaluates all devices together. To evaluate devices independently, partition by device identifier.

#### 2. Changed Current Value Exceeds 300 Partitioned by Device

```sql
SELECT current, deviceId, ts
FROM demo
WHERE current > 300 AND lag(current) OVER (PARTITION BY deviceId) < 300;
```

This rule calculates the previous value independently for each `deviceId`. The rule produces this output:

```json
{
  "current": 500,
  "ts": 5,
  "deviceId": 1
}
{
  "current": 600,
  "ts": 8,
  "deviceId": 2
}
```

Although the input stream contains data from multiple devices, the partition clause separates state evaluation by device.

#### 3. Changed Value for a Specific Device

To monitor only a specific device, use the `OVER (WHEN ...)` clause:

```sql
SELECT current, deviceId, ts
FROM demo
WHERE current > 300 AND deviceId = 1 AND lag(current) OVER (WHEN deviceId = 1) < 300;
```

The rule produces this output:

```json
{
  "current": 500,
  "ts": 5,
  "deviceId": 1
}
```

The `WHERE` clause filters out events from other devices. The `OVER (WHEN deviceId = 1)` clause records the previous value only when the condition matches.

Other analytical functions such as `had_changed` also support the `OVER` clause. Refer to [Analytical Functions](../sqls/functions/analytic_functions.md) for details.

### Trigger When a Value Exceeds a Threshold for a Time Duration

```sql
SELECT current FROM demo GROUP BY SLIDINGWINDOW(ss, 0, 10) OVER (WHEN current > 200) HAVING min(current) > 200;
```

This rule evaluates a sliding window when incoming values exceed 200. If the minimum value in the 10-second window exceeds 200, the rule emits an event:

```json
{"current":100,"ts":1}
{"current":300,"ts":2}
{"current":300,"ts":3}
{"current":300,"ts":4}
{"current":300,"ts":5}
{"current":300,"ts":6}
{"current":300,"ts":7}
{"current":300,"ts":8}
{"current":300,"ts":9}
{"current":300,"ts":10}
{
  "current": 300,
  "ts": 11
}
```
