use crate::engine::joins::apply_lookup_joins;
use crate::engine::sinks::enqueue_sink_record;
use parking_lot::RwLock;
use rekuiper_core::model::{RuleStatus, StreamRecord};
use rekuiper_core::{RuleCounters, StreamReceiver, TableManager};
use rekuiper_sql::{Evaluator, RuleState, SelectStmt};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;

pub(crate) fn is_rule_running(status: &Arc<RwLock<RuleStatus>>) -> bool {
    status.read().status == "running"
}

/// Extracts an upstream error message when the record carries an `error` or
/// `__error` field (emitted by source decoders on malformed payloads).
pub(crate) fn check_record_error(data: &HashMap<String, Value>) -> Option<String> {
    if let Some(err) = data.get("error").or_else(|| data.get("__error")) {
        return Some(
            err.as_str()
                .map(|s| s.to_string())
                .unwrap_or_else(|| err.to_string()),
        );
    }
    None
}

pub(crate) async fn handle_runtime_error(
    counters: &RuleCounters,
    rule_id: &str,
    send_error: bool,
    sink: &tokio::sync::mpsc::Sender<StreamRecord>,
    err_msg: String,
) {
    counters.record_exception(&err_msg);
    if send_error {
        let mut err_data = HashMap::new();
        err_data.insert("error".to_string(), Value::String(err_msg.clone()));
        err_data.insert("rule_id".to_string(), Value::String(rule_id.to_string()));
        err_data.insert("__raw_error__".to_string(), Value::String(err_msg));
        enqueue_sink_record(counters, sink, StreamRecord::new(err_data)).await;
    }
}

/// Handles an upstream error record per the rule `sendError` option. Returns
/// `true` when the record was an error record (counted as an exception and,
/// when enabled, forwarded immediately to the sink); the caller must then
/// `continue` without normal projection or window-buffer insertion, mirroring
/// eKuiper semantics where the error event bypasses window aggregation.
pub(crate) async fn handle_error_record(
    counters: &RuleCounters,
    rule_id: &str,
    send_error: bool,
    sink: &tokio::sync::mpsc::Sender<StreamRecord>,
    record: &StreamRecord,
) -> bool {
    let Some(err_msg) = check_record_error(&record.data) else {
        return false;
    };
    handle_runtime_error(counters, rule_id, send_error, sink, err_msg).await;
    true
}

/// Event-time configuration for windowed rules: when `enabled`, window
/// boundaries derive from payload event timestamps instead of arrival time,
/// with `late_tolerance_ms` grace for out-of-order events.
#[derive(Clone, Default)]
pub(crate) struct EventTimeConfig {
    pub(crate) enabled: bool,
    pub(crate) late_tolerance_ms: i64,
    pub(crate) timestamp_field: Option<String>,
    pub(crate) source_timestamp_fields: HashMap<String, String>,
}

pub(crate) fn parse_timestamp_val(v: &Value) -> Option<i64> {
    if let Some(n) = v.as_i64() {
        return Some(n);
    }
    if let Some(n) = v.as_u64() {
        return Some(n as i64);
    }
    if let Some(f) = v.as_f64() {
        return Some(f as i64);
    }
    if let Some(s) = v.as_str() {
        if let Ok(n) = s.parse::<i64>() {
            return Some(n);
        }
        if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(s) {
            return Some(dt.timestamp_millis());
        }
    }
    None
}

pub(crate) fn extract_event_timestamp(
    data: &HashMap<String, Value>,
    configured_field: Option<&str>,
) -> i64 {
    if let Some(field) = configured_field {
        if let Some(v) = data.get(field).and_then(parse_timestamp_val) {
            return v;
        }
    }
    for key in ["timestamp", "ts", "event_time", "time"] {
        if let Some(v) = data.get(key).and_then(parse_timestamp_val) {
            return v;
        }
    }
    chrono::Utc::now().timestamp_millis()
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn run_stateless_rule(
    counters: Arc<RuleCounters>,
    running: Arc<RwLock<RuleStatus>>,
    rule_id: String,
    select_stmt: SelectStmt,
    mut rx: StreamReceiver,
    table_manager: TableManager,
    source_configs: Arc<RwLock<HashMap<String, Value>>>,
    sink: tokio::sync::mpsc::Sender<StreamRecord>,
    send_error: bool,
) {
    // Running analytic state for acc_* cumulative functions. The stateful
    // projection below runs for every input row (advancing cumulative state
    // even for rows the WHERE filter later drops, mirroring eKuiper analytic
    // semantics); the input-row filter then decides emission.
    let rule_state = RuleState::default();
    let start_time_ms = chrono::Utc::now().timestamp_millis();
    rule_state
        .state
        .write()
        .insert("__rule_id__".to_string(), Value::String(rule_id.clone()));
    rule_state
        .state
        .write()
        .insert("__rule_start__".to_string(), Value::from(start_time_ms));
    // Stateless fast path: no table/stream joins, so skip the join machinery
    // and borrow the input map instead of cloning the full record.
    let has_joins = !select_stmt.joins.is_empty();
    // Channel close (all bus senders dropped) ends the loop; there is no
    // lossy Lagged path anymore — backpressure holds producers instead.
    while let Some(mut record) = rx.recv().await {
        if !is_rule_running(&running) {
            continue;
        }
        counters.inc_source(1);
        if handle_error_record(&counters, &rule_id, send_error, &sink, &record).await {
            continue;
        }
        record
            .data
            .entry("__rule_id__".to_string())
            .or_insert_with(|| Value::String(rule_id.clone()));
        record
            .data
            .entry("__rule_start__".to_string())
            .or_insert_with(|| Value::from(start_time_ms));
        if has_joins {
            let Some(mut joined) =
                apply_lookup_joins(&table_manager, &source_configs, &select_stmt, &record.data)
                    .await
            else {
                // Inner join without a matching table row: drop the record.
                counters.inc_filtered(1);
                continue;
            };
            joined
                .entry("__rule_id__".to_string())
                .or_insert_with(|| Value::String(rule_id.clone()));
            joined
                .entry("__rule_start__".to_string())
                .or_insert_with(|| Value::from(start_time_ms));
            match Evaluator::eval_select_filtered_stateful_fallible(
                &select_stmt,
                &joined,
                &rule_state,
            ) {
                Ok(Some(output)) => {
                    if !enqueue_sink_record(&counters, &sink, StreamRecord::new(output)).await {
                        break;
                    }
                }
                Ok(None) => counters.inc_filtered(1),
                Err(err) => handle_runtime_error(&counters, &rule_id, send_error, &sink, err).await,
            }
        } else {
            match Evaluator::eval_select_filtered_stateful_fallible(
                &select_stmt,
                &record.data,
                &rule_state,
            ) {
                Ok(Some(output)) => {
                    if !enqueue_sink_record(&counters, &sink, StreamRecord::new(output)).await {
                        break;
                    }
                }
                Ok(None) => counters.inc_filtered(1),
                Err(err) => handle_runtime_error(&counters, &rule_id, send_error, &sink, err).await,
            }
        }
    }
}
