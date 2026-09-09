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
| **Epic 2** | SQL Function Library Parity (118 Missing Functions) | 7 | 6 | 1 |
| **Epic 3** | Windowing Engine Parity (Hopping, Sliding, Hop-Count) | 3 | 0 | 3 |
| **Epic 4** | Rule Execution Options & Event-Time Tracking | 2 | 0 | 2 |
| **Epic 5** | REST API Realism & System Introspection | 3 | 0 | 3 |
| **Total** | | **17 Tasks** | **8** | **9** |

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

- [x] **Ticket 2.1: System, Metadata & Context Functions**
  - **Status**: Completed & Verified. Added `meta(key)` and `mqtt(key)` envelope-and-fallback lookup, `isnull(val)`, `uuid()`/`newuuid()` via uuid v4, `tstamp()`, `event_time()` with record-field priority, `rule_id()`, `window_start()`, `window_end()`. Wired into `eval_val`, `eval_stateful_call`, and `eval_agg_expr`. Covered by `test_system_and_meta_functions` in `test_sql_functions.rs`.
  - **Files**: `Cargo.toml`, `crates/rekuiper-sql/Cargo.toml`, `crates/rekuiper-sql/src/eval.rs`, `crates/rekuiper-sql/tests/test_sql_functions.rs`.

- [x] **Ticket 2.2: Math & Bitwise Operations**
  - **Status**: Completed & Verified. Implemented `bitand`, `bitor`, `bitxor`, `bitnot` via `to_i64_arg`; `pi()`, `rand()`; 1-arg & 2-arg `log`/`ln`, `log2`, `log10`; `power`/`pow` with checked integer pow; `cosh`, `sinh`, `tanh`, `cot`; `radians`, `degrees`; and `conv` radix 2–36 string/number converter with sign and magnitude handling. Covered by `test_math_and_bitwise_parity` in `test_sql_functions.rs`.
  - **Files**: `Cargo.toml`, `crates/rekuiper-sql/Cargo.toml`, `crates/rekuiper-sql/src/eval.rs`, `crates/rekuiper-sql/tests/test_sql_functions.rs`.

- [x] **Ticket 2.3: Array Manipulation Functions**
  - **Status**: Completed & Verified. Added parser desugaring for array `[e...]` and object `{k: v...}` literals. Implemented `cardinality`/`array_cardinality`, 1-based & negative-indexed `element_at`, `array_contains_any`, `array_remove`, `array_distinct` (=deduplicate), `array_intersect`, `array_union`, `array_except`, `array_max`, `array_min`, `array_avg`, `array_flatten`, `array_sort`, `repeat`, `sequence`, and `kvpair_array_to_obj`. Covered by `test_array_functions_parity` in `test_sql_functions.rs`.
  - **Files**: `crates/rekuiper-sql/src/parser.rs`, `crates/rekuiper-sql/src/eval.rs`, `crates/rekuiper-sql/tests/test_sql_functions.rs`.

- [x] **Ticket 2.4: DateTime Functions & Calendar Extractors**
  - **Status**: Completed & Verified. Implemented `current_timestamp`/`local_timestamp`, `current_date`/`cur_date`, `current_time`/`cur_time`/`local_time`, `from_unix_time` (with optional custom format), MySQL-convention `day_of_week` (1=Sun..7=Sat), `day_of_month` (day alias), `day_of_year`, `day_name`, `month_name`, `microsecond`, `last_day` (month-end date), and `to_seconds`/`from_days` (year-0 conversions). Covered by `test_datetime_calendar_parity` in `test_sql_functions.rs`.
  - **Files**: `crates/rekuiper-sql/src/eval.rs`, `crates/rekuiper-sql/tests/test_sql_functions.rs`.

### Ticket 2.5: Object & JSON Navigation Functions
- [x] **Status**: Completed & Verified. Implemented `object_concat` (left-to-right merge), `erase`/`object_erase` (variadic + array keys, missing keys ignored), `object_pick` (variadic + array keys), `obj_to_kvpair_array`/`object_to_kvpair_array` (exact inverse of `kvpair_array_to_obj`), `to_json`/`tojson` and `parse_json`/`parsejson`/`json_parse` (Null-safe round-trip, structured passthrough). Corrected `object_construct` to eKuiper parity by omitting Null-valued pairs (verified no existing caller passes Nulls). `keys`/`values` already matched spec - covered as-is. Covered by `test_object_json_parity` in `test_sql_functions.rs` (literals, aliases, arity/type errors, both round-trips).
  - **Files**: `crates/rekuiper-sql/src/eval.rs`, `crates/rekuiper-sql/tests/test_sql_functions.rs`.

### Ticket 2.6: String, Regex & Encoding Functions
- [x] **Status**: Completed & Verified. Implemented `regexp_matches`/`regexp_replace`/`regexp_substring` (invalid patterns and misses to Null; group-1 preferred), 0-based `split_value`, UTF-8 `numbytes`, `chr` (validated Unicode scalar), `trunc` (toward-zero, [0,34] clamp, integral results), `hex2dec`/`dec2hex` (`0x`-prefixed lowercase, signed), IEEE `crc32`, and `sha1`/`sha384` hex digests (vectors verified against Python hashlib). Covered by `test_string_regex_encoding_parity` in `test_sql_functions.rs`.
  - **Files**: `Cargo.toml`, `crates/rekuiper-sql/Cargo.toml`, `crates/rekuiper-sql/src/eval.rs`, `crates/rekuiper-sql/tests/test_sql_functions.rs`.

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
