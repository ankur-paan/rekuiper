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
| **Epic 2** | SQL Function Library Parity (118 Missing Functions) | 7 | 7 | 0 |
| **Epic 3** | Windowing Engine Parity (Hopping, Sliding, Hop-Count) | 3 | 3 | 0 |
| **Epic 4** | Rule Execution Options & Event-Time Tracking | 2 | 2 | 0 |
| **Epic 5** | REST API Realism & System Introspection | 3 | 3 | 0 |
| **Epic 6** | Real Source, Sink & Connection Metadata | 3 | 2 | 1 |
| **Epic 7** | Rule Tagging & Trace Diagnostics | 2 | 0 | 2 |
| **Epic 8** | Async Task Lifecycle & Batch Operations | 2 | 0 | 2 |
| **Epic 9** | Plugin Ecosystem & Extension Realism | 3 | 0 | 3 |
| **Total** | | **27 Tasks** | **19** | **8** |

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
- [x] **Status**: Completed & Verified. Implemented `median` (type-preserving odd counts), population `stddev`/`var` (0.0 on singletons) and sample `stddevs`/`vars` (Null under 2 points), continuous `percentile` (linear interpolation) and discrete `percentile_disc` (type-preserving), `last_value` (column/wildcard with null-skipping flag), `merge_agg` (left-to-right map merge, `{}` when nothing merges), and `row_number` (1 per batch, stateful per-partition counters). All wired into `is_aggregate_call`/`eval_aggregate_call`, with `row_number` also stateful via `RuleState` and scalar-fallback to 1. Covered by `test_statistical_window_aggregates_parity` in `test_sql_functions.rs` (Python-verified formulas, partitioning, empty/all-null batches).
  - **Files**: `crates/rekuiper-sql/src/eval.rs`, `crates/rekuiper-sql/tests/test_sql_functions.rs`.

---

## Epic 3: Windowing Engine Parity

- [x] **Ticket 3.1: Real Hopping Window Execution (`HOPPINGWINDOW(unit, length, hop)`)**
  - **Status**: Completed & Verified. Added an explicit `WindowDef::HoppingTime` arm in `spawn_rule_task` and a stateful `run_hopping_window_rule` actor loop: records buffer with arrival timestamps, a hop-interval ticker (first immediate tick consumed for grid alignment) evaluates `eval_aggregate` over records in `[now - length, now]`, expired records are retained-pruned each tick so overlapping data survives across hops, and empty windows emit nothing. Covered by `test_hopping_window_overlapping_execution` in `fvt_compat.rs` (300ms/100ms windows proving overlap retention then expiration).
  - **Files**: `crates/rekuiper-server/src/routes.rs`, `crates/rekuiper-server/tests/fvt_compat.rs`.

- [x] **Ticket 3.2: Real Sliding Window Execution (`SLIDINGWINDOW(unit, length, delay)`)**
  - **Status**: Completed & Verified. `WindowDef::SlidingTime` gained `delay: Option<u64>` (parser accepts 2- or 3-arg form; explain renders both). New `run_sliding_window_rule` actor loop: event-driven (no clock ticks) — each arrival buffers with timestamp, optionally sleeps `delay`, then prunes to the trailing horizon `[eval_time - length, eval_time]` and aggregates. Covered by `test_sliding_window_event_triggered_execution` in `fvt_compat.rs` (300ms window proving per-event firing, overlap accumulation, and full expiration).
  - **Files**: `crates/rekuiper-sql/src/ast.rs`, `crates/rekuiper-sql/src/parser.rs`, `crates/rekuiper-sql/tests/sql_test.rs`, `crates/rekuiper-server/src/routes.rs`, `crates/rekuiper-server/tests/fvt_compat.rs`.

- [x] **Ticket 3.3: Count Window Hop Processing (`COUNTWINDOW(count, hop)`)**
  - **Status**: Completed & Verified. `spawn_rule_task` passes `interval` through and `run_count_window_rule` implements hop semantics: tumbling when `hop >= size` (drain-all, preserving prior behavior), overlapping `drain(0..hop)` otherwise, plus a sparse-sampling path for `hop > size`. Covered by `test_count_window_hopping_overlap` in `fvt_compat.rs` (`COUNTWINDOW(4, 2)` proving 30/40 retention across hops, then sliding to 50-80).
  - **Files**: `crates/rekuiper-server/src/routes.rs`, `crates/rekuiper-server/tests/fvt_compat.rs`.

---

## Epic 4: Rule Execution Options & Event-Time Tracking

- [x] **Ticket 4.1: `isEventTime` & Watermarking Engine**
  - **Status**: Completed & Verified. `spawn_rule_task` accepts rule options and derives `EventTimeConfig` (`isEventTime`, `lateTolerance`, stream `TIMESTAMP` field); timestamp extraction tries the configured field, then `timestamp`/`ts`/`event_time`/`time` (int/float/RFC3339/numeric-string), then wall clock. Sliding windows buffer `(event_ts, row)`, drop rows with `event_ts < W`, advance `W = max(W, ts - tol)`, and evaluate over the age-pruned horizon; tumbling windows align `[Tstart, Tend)` in event time and close on watermark. Covered by `test_event_time_watermark_and_late_tolerance` in `fvt_compat.rs` (out-of-order acceptance, expiry, late drop, source 4 / sink 3 metrics).
  - **Files**: `crates/rekuiper-server/src/routes.rs`, `crates/rekuiper-server/tests/fvt_compat.rs`.

- [x] **Ticket 4.2: Enforce Buffer Length and Error Dispatch Options**
  - **Status**: Completed & Verified. `spawn_rule_task` parses per-rule `bufferLength` (usize, min 1, default 10,000) to size the bounded sink MPSC channel and `sendError` (bool, default false). New `check_record_error` detects upstream error records (`error`/`__error` fields); shared async helper `handle_error_record` counts `inc_exceptions`, formats `{error, rule_id}` and forwards immediately to the sink when `sendError: true`, otherwise drops it — always `continue`-ing before projection or window-buffer insertion in all five runners (stateless, count, tumbling, hopping, sliding), so window aggregation ignores error events per eKuiper docs. Covered by `test_buffer_length_and_send_error_options` in `fvt_compat.rs` (forward + metrics 1/1/1 when true; processed-but-silent 1/0/1 when false; both rules clean-deleted).
  - **Files**: `crates/rekuiper-server/src/routes.rs`, `crates/rekuiper-server/tests/fvt_compat.rs`.

---

## Epic 5: REST API Realism & System Introspection

- [x] **Ticket 5.1: Dynamic Metadata Endpoints**
  - **Status**: Completed & Verified. New `crates/rekuiper-sql/src/functions.rs` defines `FunctionMeta` (`name/category/description/aggregate/arity/example`) and `builtin_function_metadata()` with all 185 built-ins (35 math, 16 string, 39 array/object, 28 datetime, 4 JSON-path, 19 crypto/encoding/regex, 9 conversion, 18 aggregates, 11 analytics, 6 system), every name and arity verified against the evaluator dispatch; unit test asserts count 185 plus uniqueness. `list_function_metadata` now takes `State` and serves built-ins plus `list_plugins("function")`/`list_plugins("udf")` entries (category `plugin`/`udf`, unknown arity reported honestly, built-ins win collisions). Covered by `test_dynamic_metadata_functions` in `fvt_compat.rs` (>= 185 entries, spot presence/category/aggregate checks, plugin register → visible → delete → removed).
  - **Files**: `crates/rekuiper-sql/src/functions.rs`, `crates/rekuiper-sql/src/lib.rs`, `crates/rekuiper-server/src/routes.rs`, `crates/rekuiper-server/tests/fvt_compat.rs`.

- [x] **Ticket 5.2: Real Rule Schema Introspection (`GET /rules/:id/schema`)**
  - **Status**: Completed & Verified. `Evaluator::infer_expr_type` statically types every `Expr` (literals, boolean/arithmetic/unary ops with bigint-widening, `BETWEEN`/`IN`/`IS NULL`, a ~150-name call table grouped float/bigint/boolean/string/array/struct, first-concrete `CASE` branch, `Over` delegation) and `Evaluator::infer_select_schema` maps each SELECT field to its alias (or `column_name`) plus type. `get_rule_schema` returns 404 for unknown rules, `{}` when the SQL won't parse (graph rules), else the inferred object. Covered by `test_rule_schema_introspection` in `fvt_compat.rs` (`{"a": "any", "c": "float"}`, greeting/cnt/is_missing string/bigint/boolean, 404, cleanup).
  - **Files**: `crates/rekuiper-sql/src/eval.rs`, `crates/rekuiper-server/src/routes.rs`, `crates/rekuiper-server/tests/fvt_compat.rs`.

- [x] **Ticket 5.3: Process CPU and Memory Metrics**
  - **Status**: Completed & Verified. New `GET /rules/:id/cpu` (`get_rule_cpu`) validates the name, 404s unknown rules, and returns live `{rule_id, cpu, cpu_percent, memory, memory_bytes}` via a shared `current_process_stats` helper (`get_current_pid` + `System::new_all`/`refresh_all`, falling back to global CPU/used memory). `GET /rules/usage/cpu` now maps every listed rule to the process CPU (0.0 when stopped); `GET /metrics/dump` reports process cpu/memory plus system total/used memory and uptime. Covered by `test_process_cpu_and_memory_metrics` in `fvt_compat.rs` (memory > 0, cpu >= 0.0, usage object contains the rule, dump memory > 0, 404, cleanup).
  - **Files**: `crates/rekuiper-server/src/routes.rs`, `crates/rekuiper-server/tests/fvt_compat.rs`.

---

## Epic 6: Real Source, Sink & Connection Metadata

- [x] **Ticket 6.1: Disk-Backed Source & Sink JSON and YAML Metadata**
  - **Status**: Completed & Verified. Replaced scaffolded `{"name": name, "about": {}}` and `{"yaml": ""}` with authentic disk-backed loaders via `find_etc_file`: resolves `etc/sources/{name}.json` (and `etc/mqtt_source.json`), `etc/sinks/{name}.json`, `etc/sources/{name}.yaml` (and `etc/mqtt_source.yaml`), and `etc/sinks/{name}.yaml`. Expanded `list_source_metadata` and `list_sink_metadata` to include all 14 sources and 13 sinks. Covered by `test_metadata_source_and_sink_documents` in `fvt_compat.rs`.
  - **Files**: `crates/rekuiper-server/src/routes.rs`, `crates/rekuiper-server/tests/fvt_compat.rs`.

- [x] **Ticket 6.2: Real Connection & Resource Discovery**
  - **Status**: Completed & Verified. Completely eliminated `empty_yaml` stub; routed `/metadata/connections/:name` to dedicated `get_connection_metadata` (reads registered connection or `etc/connections/{name}.json` or connector profile), populated `/metadata/connections` and `/metadata/resources` directly from `state.connections`, and implemented `get_connection_yaml` resolving `etc/connections/{name}.yaml` or `etc/connections/connection.yaml`. Covered by `test_connection_metadata_and_resource_discovery` in `fvt_compat.rs`.
  - **Files**: `crates/rekuiper-server/src/routes.rs`, `crates/rekuiper-server/tests/fvt_compat.rs`.

- [ ] **Ticket 6.3: Sink & Connection Configuration Key Persistence**
  - **Problem**: `PUT, DELETE /metadata/sinks/:name/confKeys/:conf_key` and `PUT, DELETE /metadata/connections/:name/confKeys/:conf_key` return `empty_ok` without saving.
  - **Implementation**:
    - Store sink configurations under `sink_configs` in `AppState` (mirroring `source_configs`).
    - Store connection configuration keys in `state.connections`.
  - **Verification**: CRUD test creating, reading, and deleting sink confKeys.
  - **Files**: `crates/rekuiper-server/src/routes.rs`, `crates/rekuiper-server/tests/fvt_compat.rs`.

---

## Epic 7: Rule Tagging & Trace Diagnostics

- [ ] **Ticket 7.1: Persistent Rule Tagging & Tag-Based Matching**
  - **Problem**: `PUT, PATCH, DELETE /rules/:name/tags` returns `empty_ok`. `/rules/tags/match` returns `[]`. Tags are lost.
  - **Implementation**:
    - Add `tags: HashSet<String>` to `RuleDefinition`.
    - Implement `put_rule_tags`, `patch_rule_tags`, `delete_rule_tags` with KV persistence.
    - Implement `GET /rules/tags/match?tags=t1,t2` to return all matching rule IDs.
  - **Verification**: Test adding tags to rule, filtering via `/rules/tags/match`, and deleting tags.
  - **Files**: `crates/rekuiper-core/src/rule.rs`, `crates/rekuiper-server/src/routes.rs`, `crates/rekuiper-server/tests/fvt_compat.rs`.

- [ ] **Ticket 7.2: Real Rule Execution Trace Buffer**
  - **Problem**: `/rules/:name/trace/start`, `/rules/:name/trace/stop`, `/trace/rule/:rule_id`, `/trace/:id`, `/tracer` return `empty_ok`/`empty_array`/`empty_object`.
  - **Implementation**:
    - Add an in-memory ring-buffer (`VecDeque<TraceRecord>`) per running rule in `AppState`.
    - When tracing is enabled on a rule, copy stream records and evaluated outputs into the trace buffer with microsecond timestamps.
    - Serve trace records on `GET /trace/rule/:rule_id` and single traces on `GET /trace/:id`.
  - **Verification**: Start trace on rule, send 3 records, query `/trace/rule/:id`, assert 3 records captured with correct fields, stop trace.
  - **Files**: `crates/rekuiper-server/src/routes.rs`, `crates/rekuiper-server/tests/fvt_compat.rs`.

---

## Epic 8: Async Task Lifecycle & Batch Operations

- [ ] **Ticket 8.1: Authentic Background Task Manager**
  - **Problem**: `/async/data/import` returns hardcoded `task_1`; `/async/task/:id` returns hardcoded `completed`.
  - **Implementation**:
    - Create a real `TaskManager` tracking background jobs with unique UUIDs, statuses (`running`, `completed`, `failed`, `cancelled`), start/end times, and cancellation tokens.
    - `POST /async/data/import` spawns the actual import in background, returns generated `task_id`.
    - `GET /async/task/:id` returns real task status and progress.
    - `POST /async/task/:id/cancel` cancels the running background task.
  - **Verification**: Spawn async import task, check status transitions from running to completed, and test cancellation.
  - **Files**: `crates/rekuiper-server/src/routes.rs`, `crates/rekuiper-server/tests/fvt_compat.rs`.

- [ ] **Ticket 8.2: Batch Request Pipeline (`POST /batch/req`)**
  - **Problem**: `POST /batch/req` returns empty `[]`.
  - **Implementation**:
    - Parse batch array of HTTP request specs (`method`, `url`, `body`, `headers`).
    - Internal loop dispatching requests through the local Axum router and returning an array of responses.
  - **Verification**: Send batch request creating a stream and a rule in one call; verify both are created.
  - **Files**: `crates/rekuiper-server/src/routes.rs`, `crates/rekuiper-server/tests/fvt_compat.rs`.

---

## Epic 9: Plugin Ecosystem & Extension Realism

- [ ] **Ticket 9.1: Universal Source & Sink Plugin Registry**
  - **Problem**: `/plugins/sources` and `/plugins/sinks` return `empty_array`.
  - **Implementation**:
    - Extend `PluginManager` to store and list source and sink plugin definitions using the existing KV store backing.
    - Support install, update, and delete for source and sink plugins matching `/plugins/functions`.
  - **Verification**: CRUD integration test installing, listing, and deleting a source and sink plugin definition.
  - **Files**: `crates/rekuiper-core/src/plugin.rs`, `crates/rekuiper-server/src/routes.rs`, `crates/rekuiper-server/tests/fvt_compat.rs`.

- [ ] **Ticket 9.2: Portable Plugin Process Lifecycle Manager**
  - **Problem**: `/plugins/portables` returns `empty_array` and `/plugins/portables/:name/status` returns `{}`.
  - **Implementation**:
    - Track portable plugin definitions and supervisor state in `PluginManager`.
    - Provide real status (`running`, `stopped`) and configuration details.
  - **Verification**: Install portable plugin definition, start, verify status reports real state, and stop.
  - **Files**: `crates/rekuiper-core/src/plugin.rs`, `crates/rekuiper-server/src/routes.rs`, `crates/rekuiper-server/tests/fvt_compat.rs`.

- [ ] **Ticket 9.3: External Services & Embedded JavaScript UDF Engine**
  - **Problem**: `/services` and `/udf/javascript` return `empty_array`.
  - **Implementation**:
    - Store service definitions in `AppState` with schema and endpoint registration.
    - Integrate an embedded JavaScript engine (such as `boa_engine`) to compile and execute JavaScript UDF functions in stream processing.
  - **Verification**: Register JS UDF and service definition, verify they appear in `/services` and `/udf/javascript`, and verify execution of a JS UDF in SQL.
  - **Files**: `Cargo.toml`, `crates/rekuiper-core/Cargo.toml`, `crates/rekuiper-core/src/plugin.rs`, `crates/rekuiper-server/src/routes.rs`, `crates/rekuiper-server/tests/fvt_compat.rs`.

