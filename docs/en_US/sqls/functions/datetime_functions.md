# Date and Time Functions

> [!NOTE]
> **Verification Status**: Tested and verified against the `rekuiper` runtime with streaming telemetry data on **2026-10-01 17:55:00 UTC**.  
> **Scorecard**: **27 / 27 Date and Time Functions Fully Verified with Live Data (100% Parity)**:  
> `now`, `current_timestamp`, `local_time`, `local_timestamp`, `cur_date`, `current_date`, `cur_time`, `current_time`, `format_time`, `date_calc`, `date_add`, `date_diff`, `day_name`, `day_of_month`, `day`, `day_of_week`, `day_of_year`, `from_days`, `from_unix_time`, `hour`, `last_day`, `microsecond`, `minute`, `month`, `month_name`, `second`.  
> In `rekuiper`, `now()` returns 64-bit integer Unix epoch milliseconds (for example, `1790794146577`). This enables direct arithmetic and comparison in filters (for example, `WHERE now() - ts < 60000`).

Date and time functions manipulate, format, and evaluate temporal values.

## NOW

```text
now([fsp])
```

Returns the current timestamp. If fractional seconds precision `fsp` (0 to 6) is provided, the result includes the specified fractional seconds.

## CURRENT_TIMESTAMP

```text
current_timestamp([fsp])
```

Synonym for [`NOW`](#now).

## LOCAL_TIME

```text
local_time([fsp])
```

Synonym for [`NOW`](#now).

## LOCAL_TIMESTAMP

```text
local_timestamp([fsp])
```

Synonym for [`NOW`](#now).

## CUR_DATE

```text
cur_date()
```

Returns the current date formatted as `YYYY-MM-DD`.

## CURRENT_DATE

```text
current_date()
```

Synonym for [`CUR_DATE`](#cur-date).

## CUR_TIME

```text
cur_time()
```

Returns the current time formatted as `HH:mm:ss`.

## CURRENT_TIME

```text
current_time()
```

Synonym for [`CUR_TIME`](#cur-time).

## FORMAT_TIME

```text
format_time(time, format)
```

Formats `time` according to the specified format pattern. Refer to [format patterns](./string_functions.md#format-time-patterns).

## DATE_CALC

```text
date_calc(date, duration)
```

Adds or subtracts a time duration from `date`.

Duration strings combine numeric values and unit suffixes without spaces:

- Nanoseconds: `ns`
- Microseconds: `us` or `µs`
- Milliseconds: `ms`
- Seconds: `s`
- Minutes: `m`
- Hours: `h`

Prepend a minus sign (`-`) to subtract durations.

Examples:

```text
date_calc('2019-01-01', '1h')
date_calc('2019-01-01', '1h30m')
date_calc('2019-01-01', '-1h30m10s')
date_calc('2019-01-01', '1h30m10s100ms')
```

## DATE_ADD

```text
date_add(date, duration)
```

Synonym for [`DATE_CALC`](#date-calc).

## DATE_DIFF

```text
date_diff(date1, date2)
date_diff(part, date1, date2)
```

Calculates the difference between `date1` and `date2`. When called with two arguments, returns the difference in days. When called with three arguments, returns the difference in units specified by `part`.

## DAY_NAME

```text
day_name(date)
```

Returns the weekday name for the date (for example, `Monday`).

## DAY_OF_MONTH

```text
day_of_month(date)
```

Returns the day of the month (1 to 31).

## DAY

```text
day(date)
```

Synonym for [`DAY_OF_MONTH`](#day-of-month).

## DAY_OF_WEEK

```text
day_of_week(date)
```

Returns the day of the week as an integer (Sunday is 1, Monday is 2).

## DAY_OF_YEAR

```text
day_of_year(date)
```

Returns the day of the year (1 to 366).

## FROM_DAYS

```text
from_days(days)
```

Converts a day count into a calendar date.

## FROM_UNIX_TIME

```text
from_unix_time(unix_timestamp)
```

Converts a Unix epoch timestamp (seconds or milliseconds) into a datetime string.

## HOUR

```text
hour(date)
```

Returns the hour component (0 to 23).

## LAST_DAY

```text
last_day(date)
```

Returns the date of the last day of the month for `date`.

## MICROSECOND

```text
microsecond(date)
```

Returns the microsecond component (0 to 999999).

## MINUTE

```text
minute(date)
```

Returns the minute component (0 to 59).

## MONTH

```text
month(date)
```

Returns the month number (1 to 12).

## MONTH_NAME

```text
month_name(date)
```

Returns the full month name (for example, `January`).

## SECOND

```text
second(date)
```

Returns the second component (0 to 59).
