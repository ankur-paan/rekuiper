# eKuiper Drop-In Parity Tracking & Ticket Backlog

This document is the single source of truth for achieving **100% authentic, zero-fake feature parity with LF Edge eKuiper** in `rekuiper`. Every item listed here represents a verified difference or missing capability discovered during the forensic audit of the eKuiper documentation, source code, and integration test suites.

No item may be marked complete without:
1. Genuine production implementation (no dummy `{}` returns, no silent fallback to stateless execution).
2. Automated unit/integration tests verifying the exact eKuiper behavior.
3. Clean `cargo check`, `cargo fmt --check`, and `cargo clippy --workspace --all-targets -- -D warnings`.

---

## Progress Overview

| Epic | Description | Total Items | Completed | Remaining |
| :--- | :--- | :---: | :---: | :---: |
| **Epic 1** | Real Streaming Source Lifecycle (MQTT & File) | 2 | 2 | 0 |
| **Epic 2** | SQL Function Library Parity (118 Missing Functions) | 7 | 0 | 7 |
| **Epic 3** | Windowing Engine Parity (Hopping, Sliding, Hop-Count) | 3 | 0 | 3 |
| **Epic 4** | Rule Execution Options & Event-Time Tracking | 2 | 0 | 2 |
| **Epic 5** | REST API Realism & System Introspection | 3 | 0 | 3 |
| **Total** | | **17 Tasks** | **2** | **15** |

---

## Epic 1: Real Streaming Source Lifecycle

- [x] **Ticket 1.1: Automatic MQTT Source Bootstrapping**
  - **Status**: Completed & Verified. Streams without `TYPE` or with `TYPE="mqtt"` resolve MQTT config (broker, topic, stream options), spawn background `MqttSource` with `tokio::select!` cancellation, decode incoming payloads to `StreamRecord`, and broadcast to `stream_bus`. Stopping rule cancels MQTT client. Covered by `test_mqtt_source_lifecycle_and_defaults` in `fvt_compat.rs`.
  - **Files**: `crates/rekuiper-connectors/src/lib.rs`, `crates/rekuiper-server/src/routes.rs`, `crates/rekuiper-server/tests/fvt_compat.rs`.

- [x] **Ticket 1.2: Streaming File Source Ingestion**
  - **Status**: Completed & Verified. Implemented `FileSourceConfig` and unified streaming `FileSource` reading line-delimited JSON or CSV rows with `parse_delimited_line`, header capture, pacing intervals, and cancellation loop. Wired `resolve_file_source` and spawn in `bootstrap_rule_sources`. Covered by `test_file_source_streaming_ingestion` in `fvt_compat.rs` and unit tests in `rekuiper-connectors`.
  - **Files**: `crates/rekuiper-connectors/src/lib.rs`, `crates/rekuiper-server/src/routes.rs`, `crates/rekuiper-server/tests/fvt_compat.rs`.

---

## Epic 2: Complete SQL Function Library Parity

### Ticket 2.1: System, Metadata & Context Functions
- [ ] **Functions**:
  - `meta(key)`: Extract metadata properties from the current stream envelope (e.g. `meta(topic)` returns MQTT/bus topic).
  - `mqtt(topic)` / `mqtt(key)`: MQTT header/topic metadata.
  - `isnull(val)`: Returns `true` if `val` is `Value::Null`, else `false`.
  - `newuuid()` / `uuid()`: Generates a new random UUID v4 string.
  - `tstamp()`: Returns current system wall-clock epoch timestamp in milliseconds.
  - `event_time()`: Returns the event timestamp of the current [`StreamRecord`].
  - `rule_id()`: Returns the ID of the rule currently executing.
  - `window_start()`, `window_end()`: Return the start and end timestamps (epoch ms) of the current window.
- **Files**: `crates/rekuiper-sql/src/eval.rs`, `crates/rekuiper-sql/src/parser.rs`
- **Verification**: Unit tests covering all 8 functions in `crates/rekuiper-sql/tests/test_sql_functions.rs`.

### Ticket 2.2: Math & Bitwise Operations
- [ ] **Functions**:
  - `bitand(a, b)`: Bitwise AND of integer-converted arguments.
  - `bitor(a, b)`: Bitwise OR of integer-converted arguments.
  - `bitxor(a, b)`: Bitwise XOR of integer-converted arguments.
  - `bitnot(a)`: Bitwise NOT of integer-converted argument.
  - `pi()`: Returns mathematical constant π (`3.141592653589793`).
  - `rand()`: Returns pseudo-random float in `[0.0, 1.0)`.
  - `power(base, exp)` / `pow`: Exponentiation `base ^ exp`.
  - `log(val)`: Natural logarithm (alias to `ln`), plus `log2(val)` and `log10(val)`.
  - `cosh(x)`, `sinh(x)`, `tanh(x)`, `cot(x)`: Hyperbolic and cotangent trigonometry.
  - `radians(deg)`, `degrees(rad)`: Degree/radian conversions.
  - `conv(num, from_base, to_base)`: Convert number representation between arbitrary bases 2–36.
- **Files**: `crates/rekuiper-sql/src/eval.rs`
- **Verification**: Unit tests for bitwise masks, trigonometric edge cases, log, and base conversion.

### Ticket 2.3: Array Manipulation Functions
- [ ] **Functions**:
  - `cardinality(arr)` / `array_cardinality(arr)`: Returns element count.
  - `element_at(arr, index)`: 1-based indexing into array; negative indices count from end (`-1` = last).
  - `array_contains(arr, val)`: Boolean check if array contains value.
  - `array_contains_any(arr1, arr2)`: Boolean check if any element of `arr2` is in `arr1`.
  - `array_remove(arr, val)`: Returns copy of `arr` with all occurrences of `val` removed.
  - `array_distinct(arr)`: Returns deduplicated array preserving order.
  - `array_intersect(arr1, arr2)`: Set intersection of two arrays.
  - `array_union(arr1, arr2)`: Set union of two arrays.
  - `array_except(arr1, arr2)`: Set difference `arr1 - arr2`.
  - `array_max(arr)`, `array_min(arr)`, `array_avg(arr)`: Statistical aggregates over numeric array.
  - `array_flatten(arr)`: Flattens 2D array by one level.
  - `array_sort(arr)`: Sorts elements ascending.
  - `repeat(val, n)`: Returns array containing `val` repeated `n` times.
  - `sequence(start, stop, step)`: Generates array sequence `[start, start+step, ...]`.
  - `kvpair_array_to_obj(arr)`: Converts array of `[{"key": k, "value": v}]` into a single JSON object.
- **Files**: `crates/rekuiper-sql/src/eval.rs`
- **Verification**: Unit tests testing all array operators against edge cases (empty arrays, mixed types, nulls).

### Ticket 2.4: DateTime Functions & Calendar Extractors
- [ ] **Functions**:
  - `current_timestamp()`: Current UTC epoch in milliseconds (alias to `now()`).
  - `current_date()`, `cur_date()`: Current UTC date string `YYYY-MM-DD`.
  - `current_time()`, `cur_time()`: Current UTC time string `HH:MM:SS`.
  - `from_unix_time(ms, format)`: Formats epoch milliseconds into date string with optional format pattern.
  - `day_of_week(ts)`: Day of week: 1 (Sunday) to 7 (Saturday).
  - `day_of_month(ts)`: Day of month: 1 to 31.
  - `day_of_year(ts)`: Day of year: 1 to 366.
  - `day_name(ts)`: Full day name ("Monday", "Tuesday", etc.).
  - `month_name(ts)`: Full month name ("January", "February", etc.).
  - `microsecond(ts)`: Microsecond extract.
  - `last_day(ts)`: Timestamp of the last day of the given month.
  - `from_days(n)`: Converts day count from year 0 to date epoch.
  - `to_seconds(ts)`: Converts timestamp to seconds since year 0.
- **Files**: `crates/rekuiper-sql/src/eval.rs`
- **Verification**: Exact parity unit tests comparing leap years, week boundaries, and formatters.

### Ticket 2.5: Object & JSON Navigation Functions
- [ ] **Functions**:
  - `keys(obj)`: Returns JSON array of string keys.
  - `values(obj)`: Returns JSON array of values.
  - `object_construct(k1, v1, k2, v2, ...)`: Constructs dynamic JSON object from key/value pairs.
  - `object_concat(obj1, obj2)`: Shallow merge of two objects.
  - `erase(obj, key)`: Returns copy of object with `key` removed.
  - `object_pick(obj, k1, k2, ...)`: Returns sub-object with only the specified keys.
  - `obj_to_kvpair_array(obj)`: Converts object to `[{"key": k, "value": v}]`.
  - `to_json(val)`: Serializes value to JSON string.
  - `parse_json(str)`: Parses JSON string into structured value (or Null on malformed).
- **Files**: `crates/rekuiper-sql/src/eval.rs`
- **Verification**: Tests for object manipulation, nested extraction, and roundtrip JSON conversion.

### Ticket 2.6: String, Regex & Encoding Functions
- [ ] **Functions**:
  - `regexp_matches(str, regex)`: Returns true if regex matches anywhere in `str`.
  - `regexp_replace(str, regex, repl)`: Replaces all regex occurrences with `repl`.
  - `regexp_substring(str, regex)`: Extracts first regex capture or match.
  - `split_value(str, delimiter, index)`: 1-based index from delimited string.
  - `numbytes(str)`: Byte length of UTF-8 string.
  - `chr(code)`: Converts ASCII/Unicode code integer into 1-character string.
  - `trunc(num, decimals)`: Truncates numeric value to `decimals` decimal places without rounding.
  - `hex2dec(hex_str)`, `dec2hex(num)`: Hexadecimal to decimal string conversion.
  - `crc32(str)`: CRC32 checksum integer.
  - `sha1(str)`, `sha384(str)`: Cryptographic hex digests.
- **Files**: `crates/rekuiper-sql/src/eval.rs`
- **Verification**: Regex edge cases, multibyte UTF-8 byte counting, and hash verification vectors.

### Ticket 2.7: Statistical & Window Aggregate Functions
- [ ] **Functions**:
  - `median(col)`: Computes statistical median over window records.
  - `stddev(col)` / `stddevs(col)`: Sample and population standard deviation.
  - `var(col)` / `vars(col)`: Sample and population variance.
  - `percentile(col, p)`: Continuous percentile interpolation over window.
  - `percentile_disc(col, p)`: Discrete percentile over window.
  - `last_value(col)`: Most recent value in window buffer.
  - `merge_agg(col)`: Merges all map objects in the window into a single combined map.
  - `row_number()`: Evaluates sequential 1-based index of row within window or partition.
- **Files**: `crates/rekuiper-sql/src/eval.rs`
- **Verification**: Exact statistical formulas matching eKuiper's math over tumbling/count window buffers.

---

## Epic 3: Windowing Engine Parity

- [ ] **Ticket 3.1: Real Hopping Window Execution (`HOPPINGWINDOW(unit, length, hop)`)**
  - **Problem**: In `crates/rekuiper-server/src/routes.rs:L1115`, `WindowDef::Hopping` falls back to `run_stateless_rule`, processing each record individually instead of buffering and aggregating across overlapping time windows.
  - **Requirements**:
    - Implement `run_hopping_window_rule` actor loop.
    - Buffer records with their arrival/event timestamps.
    - Every `hop` duration (e.g. 5 seconds), trigger aggregation over all buffered records whose timestamp falls within `[now - length, now)`.
    - Expire records older than `length`.
  - **Files**: `crates/rekuiper-server/src/routes.rs`
  - **Verification**: Test sending 10 records over time, verifying output triggers every `hop` interval with overlapping data.

- [ ] **Ticket 3.2: Real Sliding Window Execution (`SLIDINGWINDOW(unit, length, delay)`)**
  - **Problem**: In `routes.rs:L1115`, `WindowDef::Sliding` falls back to `run_stateless_rule`.
  - **Requirements**:
    - Implement `run_sliding_window_rule` actor loop.
    - On every record arrival, evaluate aggregates over all records in the sliding buffer within `(record_ts - length, record_ts]`.
    - Support optional `delay` if specified.
  - **Files**: `crates/rekuiper-server/src/routes.rs`
  - **Verification**: Test sliding window firing on every event with aggregate over trailing time horizon.

- [ ] **Ticket 3.3: Count Window Hop Processing (`COUNTWINDOW(count, hop)`)**
  - **Problem**: `run_count_window_rule` in `routes.rs` accepts only `size` and clears the entire buffer on each batch, ignoring the `hop` parameter.
  - **Requirements**:
    - If `hop` is specified, when buffer reaches `size`, emit aggregate and discard only the oldest `hop` records (retaining `size - hop` records for overlapping count windows).
  - **Files**: `crates/rekuiper-server/src/routes.rs`
  - **Verification**: Test `COUNTWINDOW(4, 2)` producing overlapping 4-record aggregates every 2 records.

---

## Epic 4: Rule Execution Options & Event-Time Tracking

- [ ] **Ticket 4.1: `isEventTime` & Watermarking Engine**
  - **Problem**: All window engines use local machine arrival time (`chrono::Utc::now()`). Rule option `"isEventTime": true` is ignored.
  - **Requirements**:
    - Extract record timestamp from payload (e.g. `timestamp` field) when `isEventTime: true`.
    - Implement watermark tracking and `lateTolerance` window bounds.
  - **Files**: `crates/rekuiper-server/src/routes.rs`, `crates/rekuiper-core/src/model.rs`
  - **Verification**: Test out-of-order records processed in event-time order, with late records discarded past `lateTolerance`.

- [ ] **Ticket 4.2: Enforce Buffer Length and Error Dispatch Options**
  - **Problem**: Rule option `bufferLength` is ignored; bounded channel is hardcoded to 10,000. Option `sendError` is not checked.
  - **Requirements**:
    - Pass `bufferLength` from rule options to MPSC channel creation (default 10,000).
    - If `sendError: true`, format execution errors into error records and route to sinks.
  - **Files**: `crates/rekuiper-server/src/routes.rs`
  - **Verification**: Test bounded queue backpressure honoring custom `bufferLength`.

---

## Epic 5: REST API Realism & System Introspection

- [ ] **Ticket 5.1: Dynamic Metadata Endpoints**
  - **Problem**: `/metadata/functions` returns a hardcoded 25-item slice.
  - **Requirements**:
    - Dynamically generate the function metadata from all built-in functions in `rekuiper-sql` and registered plugins in `PluginManager`.
    - Return accurate arity, description, and function categories.
  - **Files**: `crates/rekuiper-server/src/routes.rs`
  - **Verification**: Endpoint test verifying all 184 functions appear in `/metadata/functions`.

- [ ] **Ticket 5.2: Real Rule Schema Introspection (`GET /rules/:id/schema`)**
  - **Problem**: `get_rule_schema` returns an empty `{}` object.
  - **Requirements**:
    - Introspect the rule's SQL AST projection items and stream definitions to return the expected output schema fields and inferred types.
  - **Files**: `crates/rekuiper-server/src/routes.rs`, `crates/rekuiper-sql/src/eval.rs`
  - **Verification**: Test verifying `SELECT a, b + 1 AS c FROM demo` returns schema `{ "a": "any", "c": "float" }`.

- [ ] **Ticket 5.3: Process CPU and Memory Metrics**
  - **Problem**: `GET /rules/:id/cpu` and `GET /metrics/dump` return empty objects.
  - **Requirements**:
    - Use `sysinfo` (already in workspace `Cargo.toml`) to query real process CPU % and resident memory (RSS).
  - **Files**: `crates/rekuiper-server/src/routes.rs`
  - **Verification**: Test verifying real non-empty CPU usage and memory stats are returned.
