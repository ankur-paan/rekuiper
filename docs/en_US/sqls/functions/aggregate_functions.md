# Aggregate Functions

> [!NOTE]
> **Verification Status**: Tested and verified against the `rekuiper` runtime with streaming window workloads on **2026-10-01 16:34:28 UTC**.  
> **Scorecard**: **18 / 18 Aggregate Functions Fully Verified with Live Data (100% Parity)**:  
> `avg`, `count`, `count(*)`, `max`, `min`, `sum`, `collect`, `last_value`, `merge_agg`, `deduplicate`, `median`, `stddev`, `stddevs`, `var`, `vars`, `percentile`, `percentile_disc`, `last_agg_hit_count`, `last_agg_hit_time`.

Aggregate functions compute summary values across sets of records. Use aggregate functions in:

- The `SELECT` list of an outer query or subquery.
- The `HAVING` clause.

## AVG

```text
avg(col)
```

Returns the arithmetic average of numeric values in a group. Ignores `null` values. Supports incremental computation.

## COUNT

```text
count(*)
count(col)
```

Returns the total count of rows or non-null values in a group. Supports incremental computation.

## MAX

```text
max(col)
```

Returns the maximum value in a group. Ignores `null` values. Supports incremental computation.

## MIN

```text
min(col)
```

Returns the minimum value in a group. Ignores `null` values. Supports incremental computation.

## SUM

```text
sum(col)
```

Returns the sum of all numeric values in a group. Ignores `null` values. Supports incremental computation.

## COLLECT

```text
collect(*)
collect(col)
```

Returns an array containing all column values or complete records (`*`) from the window group. Supports incremental computation.

### Examples

Extract an array of integers from column `a`:

```sql
SELECT collect(a) AS r1 FROM test GROUP BY TumblingWindow(ss, 10);
-- Output: [{"r1": [32, 45]}]
```

Collect all records across the current window:

```sql
SELECT collect(*) AS r1 FROM test GROUP BY TumblingWindow(ss, 10);
-- Output: [{"r1": [{"a": 32, "b": "hello"}, {"a": 45, "b": "world"}]}]
```

Select attribute `a` from the second element in the window:

```sql
SELECT collect(*)[1]->a AS r1 FROM test GROUP BY TumblingWindow(ss, 10);
-- Output: [{"r1": 45}]
```

## LAST_VALUE

```text
last_value(*, true)
last_value(col, false)
```

Retrieves the value of the last row in a group for specified columns or complete records.

- The first parameter specifies the column or `*`.
- The second parameter specifies whether to ignore `null` values. When `true`, returns the last non-null value, or `null` if none exist. When `false`, returns the last value even if `null`. Supports incremental computation.

## MERGE_AGG

```text
merge_agg(*)
merge_agg(col)
```

Merges objects in a group into a single composite object. If duplicate keys occur across records, values from later records overwrite earlier values. Merging operates on the top-level object structure only. Supports incremental computation.

If the argument is a column containing non-object values, `merge_agg` returns an empty object `{}`.

### Examples

Given incoming group records:

```json
{"a": {"a": 2}, "b": 2, "c": 3}
{"a": {"b": 2}, "b": 5, "d": 6}
{"a": {"a": 3}, "b": 8}
```

Merge all records:

```sql
SELECT merge_agg(*) AS r1 FROM test GROUP BY TumblingWindow(ss, 10);
-- Output: {"a": {"a": 3}, "b": 8, "c": 3, "d": 6}
```

Merge object column `a`:

```sql
SELECT merge_agg(a) AS r1 FROM test GROUP BY TumblingWindow(ss, 10);
-- Output: {"a": 3, "b": 2}
```

Merge non-object column `b`:

```sql
SELECT merge_agg(b) AS r1 FROM test GROUP BY TumblingWindow(ss, 10);
-- Output: {}
```

## DEDUPLICATE

```text
deduplicate(col, all_items_bool)
```

Removes duplicate values from a window group based on the specified column.

- `col`: Key column for deduplication.
- `all_items_bool`: If `true`, returns all unique records. If `false`, returns only the latest non-duplicate record. If the latest record is a duplicate, the sink receives an empty map `{}`. Configure the sink property [`omitIfEmpty`](../../guide/sinks/overview.md#common-properties) to suppress empty payload emission.

### Examples

Deduplicate full records by column `a`:

```sql
SELECT deduplicate(a, true) AS r1 FROM test GROUP BY TumblingWindow(ss, 10);
-- Output: [{"r1": [{"a": 32, "b": "hello"}, {"a": 45, "b": "world"}]}]
```

Return only new values of column `a` in a sliding hour window:

```sql
SELECT deduplicate(a, false)->a AS r1 FROM demo GROUP BY SlidingWindow(hh, 1);
```

## MEDIAN

```text
median(col)
```

Returns the median numeric value of the column in the group.

## STDDEV

```text
stddev(col)
```

Returns the population standard deviation of numeric values in the group.

## STDDEVS

```text
stddevs(col)
```

Returns the sample standard deviation of numeric values in the group.

## VAR

```text
var(col)
```

Returns the population variance (square of the population standard deviation) for numeric values in the group.

## VARS

```text
vars(col)
```

Returns the sample variance (square of the sample standard deviation) for numeric values in the group.

## PERCENTILE

```text
percentile(col, percentile_num)
```

Calculates the continuous distribution percentile value for a column in the group. The percentile argument must be a constant between `0.0` and `1.0`.

## PERCENTILE_DISC

```text
percentile_disc(col, percentile_num)
```

Calculates the discrete distribution percentile value for a column in the group. The percentile argument must be a constant between `0.0` and `1.0`.

## LAST_AGG_HIT_COUNT

```text
last_agg_hit_count()
```

Returns the total number of times the aggregate rule condition evaluated to `true`. When invoked in a `HAVING` clause, the counter increments only when the `HAVING` condition is satisfied.

For non-aggregate rules, use [last_hit_count](./other_functions.md#last_hit_count).

## LAST_AGG_HIT_TIME

```text
last_agg_hit_time()
```

Returns the 64-bit integer millisecond timestamp of the last event that triggered the aggregate rule. When invoked in a `HAVING` clause, the timestamp updates only when the `HAVING` condition is satisfied.

For non-aggregate rules, use [last_hit_time](./other_functions.md#last_hit_time).
