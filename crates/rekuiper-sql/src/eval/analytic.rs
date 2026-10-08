use super::{Evaluator, RuleState};
use serde_json::Value;

impl Evaluator {
    // ---------- analytic / window navigation ----------

    /// `lag(val, [offset], [default])`: returns the value seen `offset`
    /// rows ago within the same partition (default 1), or `default`
    /// (default `Null`) when fewer rows have been seen. The current value is
    /// appended to history after the lookup. State key:
    /// `lag:{func_call_id}:{partition_key}`.
    pub(crate) fn eval_lag(
        args: &[Value],
        state: &RuleState,
        call_id: &str,
        partition_key: &str,
        skip_update: bool,
    ) -> Value {
        if args.is_empty() || args.len() > 4 {
            return Value::Null;
        }
        let offset = args.get(1).and_then(Self::to_i64_arg).unwrap_or(1);
        if offset <= 0 {
            return args[0].clone();
        }
        let default = args.get(2).cloned().unwrap_or(Value::Null);
        let ignore_null = args.get(3).and_then(|v| v.as_bool()).unwrap_or(false);
        let state_key = format!("lag:{}:{}", call_id, partition_key);
        let mut guard = state.state.write();
        let mut history: Vec<Value> = match guard.get(&state_key) {
            Some(Value::Array(arr)) => arr.clone(),
            _ => Vec::new(),
        };
        let out = if (history.len() as i64) >= offset {
            history[history.len() - offset as usize].clone()
        } else {
            default
        };
        if !skip_update && (!ignore_null || !args[0].is_null()) {
            history.push(args[0].clone());
            guard.insert(state_key, Value::Array(history));
        }
        out
    }

    /// Stateful change detection shared by `had_changed` / `changed_col`:
    /// compares against the stored previous value (both-null counts as
    /// unchanged), then stores the current value.
    pub(crate) fn eval_changed(
        lowered_name: &str,
        args: &[Value],
        state: &RuleState,
        call_id: &str,
        partition_key: &str,
    ) -> Value {
        if args.is_empty() {
            return Value::Null;
        }
        let (ignore_null, cols) = if args.len() >= 2 && args[0].is_boolean() {
            (args[0].as_bool().unwrap_or(false), &args[1..])
        } else {
            (false, args)
        };

        let mut any_changed = false;
        let mut first_val = Value::Null;

        for (idx, current) in cols.iter().enumerate() {
            let state_key = format!("{}:{}:{}:{}", lowered_name, call_id, idx, partition_key);
            if current.is_null() {
                if ignore_null {
                    continue;
                }
                continue;
            }
            let previous = state.state.read().get(&state_key).cloned();
            let changed = match &previous {
                None => true,
                Some(p) => !Self::values_equal(p, current),
            };
            if changed {
                state.state.write().insert(state_key, current.clone());
                any_changed = true;
                if first_val.is_null() {
                    first_val = current.clone();
                }
            }
        }

        if lowered_name == "had_changed" {
            Value::Bool(any_changed)
        } else if any_changed {
            first_val
        } else {
            Value::Null
        }
    }

    // ---------- analytic scalar fallbacks (single-record context) ----------

    pub(crate) fn func_collect_scalar(args: &[Value]) -> Value {
        if args.len() != 1 {
            return Value::Null;
        }
        if args[0].is_null() {
            return Value::Array(Vec::new());
        }
        Value::Array(vec![args[0].clone()])
    }

    pub(crate) fn func_lead_scalar(args: &[Value]) -> Value {
        if args.is_empty() || args.len() > 4 {
            return Value::Null;
        }
        // A single row has no future rows: resolve to the default.
        args.get(2).cloned().unwrap_or(Value::Null)
    }

    pub(crate) fn func_lag_scalar(args: &[Value]) -> Value {
        if args.is_empty() || args.len() > 4 {
            return Value::Null;
        }
        // A single row without prior history: resolve to the default (arg 2).
        args.get(2).cloned().unwrap_or(Value::Null)
    }

    pub(crate) fn func_latest_scalar(args: &[Value]) -> Value {
        if args.is_empty() || args.len() > 2 {
            return Value::Null;
        }
        let default_val = args.get(1).cloned().unwrap_or(Value::Null);
        if args[0].is_null() {
            return default_val;
        }
        args[0].clone()
    }

    pub(crate) fn func_row_number_scalar(args: &[Value]) -> Value {
        if !args.is_empty() {
            return Value::Null;
        }
        Value::from(1)
    }

    pub(crate) fn func_had_changed_scalar(args: &[Value]) -> Value {
        if args.len() != 1 {
            return Value::Null;
        }
        // Without history every value counts as changed.
        Value::Bool(true)
    }

    pub(crate) fn func_changed_col_scalar(args: &[Value]) -> Value {
        if args.len() != 1 {
            return Value::Null;
        }
        if args[0].is_null() {
            return Value::Null;
        }
        args[0].clone()
    }
}
