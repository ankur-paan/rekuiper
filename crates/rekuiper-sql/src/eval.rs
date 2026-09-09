use crate::ast::{BinaryOperator, Expr, SelectStmt, SetOp, UnaryOperator};
use base64::Engine as _;
use chrono::{Datelike, Timelike};
use parking_lot::RwLock;
use serde_json::Value;
use sha2::Digest as _;
use std::collections::HashMap;
use std::sync::Arc;

/// Per-rule running state for analytic cumulative functions (`acc_*`).
///
/// State keys have the format `{func_name}:{func_call_id}:{partition_key}`,
/// so distinct call sites and `OVER (PARTITION BY ...)` groups evolve
/// independently.
#[derive(Default, Clone)]
pub struct RuleState {
    pub state: Arc<RwLock<HashMap<String, Value>>>,
}

pub struct Evaluator;

impl Evaluator {
    /// Copy of a statement with any trailing `UNION` detached, so a single
    /// branch can be evaluated without recursing into the set operation.
    fn without_set_op(stmt: &SelectStmt) -> SelectStmt {
        let mut single = stmt.clone();
        single.set_op = None;
        single
    }

    /// Merge two single-row UNION branch outputs into one row: key union with
    /// the right branch winning conflicts. `Union` vs `UnionAll` multiplicity
    /// only manifests in multi-row APIs; see `eval_select_stateful_multi`.
    fn merge_union_rows(
        left: Option<HashMap<String, Value>>,
        right: Option<HashMap<String, Value>>,
    ) -> Option<HashMap<String, Value>> {
        match (left, right) {
            (Some(mut l), Some(r)) => {
                for (k, v) in r {
                    l.insert(k, v);
                }
                Some(l)
            }
            (one @ Some(_), None) | (None, one @ Some(_)) => one,
            (None, None) => None,
        }
    }

    pub fn eval_select(
        stmt: &SelectStmt,
        record: &HashMap<String, Value>,
    ) -> Option<HashMap<String, Value>> {
        if let Some((_, rhs)) = &stmt.set_op {
            // Each branch filters/projects the same record independently.
            let left = Self::eval_select(&Self::without_set_op(stmt), record);
            let right = Self::eval_select(rhs, record);
            return Self::merge_union_rows(left, right);
        }
        if let Some(ref condition) = stmt.where_clause {
            let matches = Self::eval_bool(condition, record);
            if !matches {
                return None;
            }
        }

        let mut output = HashMap::new();
        for (idx, field) in stmt.fields.iter().enumerate() {
            // An explicit `AS alias` wins over the default column name.
            let alias = stmt.field_aliases.get(idx).and_then(|a| a.clone());
            match field {
                Expr::Wildcard => {
                    for (k, v) in record {
                        output.insert(k.clone(), v.clone());
                    }
                }
                Expr::Identifier(name) => {
                    let key = alias.unwrap_or_else(|| name.clone());
                    if let Some(val) = record.get(name) {
                        output.insert(key, val.clone());
                    } else {
                        output.insert(key, Value::Null);
                    }
                }
                Expr::FieldAccess {
                    parent: _,
                    field: leaf,
                } => {
                    let val = Self::eval_val(field, record);
                    // Use leaf field name as output key (flattened projection)
                    output.insert(alias.unwrap_or_else(|| leaf.clone()), val);
                }
                _ => {
                    let val = Self::eval_val(field, record);
                    let name = alias.unwrap_or_else(|| Self::column_name(field, idx));
                    output.insert(name, val);
                }
            }
        }

        Some(output)
    }

    /// Batch evaluation over a window of records.
    ///
    /// Computes `count/sum/avg/min/max` aggregates in `stmt.fields` over
    /// `records`, retains `group_by` column values (from the first record),
    /// and applies `having` (evaluated against the aggregated row, with
    /// aggregate calls resolved over the batch). Returns `None` when `having`
    /// filters the row out.
    pub fn eval_aggregate(
        stmt: &SelectStmt,
        records: &[HashMap<String, Value>],
    ) -> Option<HashMap<String, Value>> {
        if let Some((_, rhs)) = &stmt.set_op {
            // Each branch aggregates the same batch independently; the two
            // single-row outputs merge (right wins on conflict).
            let left = Self::eval_aggregate(&Self::without_set_op(stmt), records);
            let right = Self::eval_aggregate(rhs, records);
            return Self::merge_union_rows(left, right);
        }
        let first: Option<&HashMap<String, Value>> = records.first();
        let mut output = HashMap::new();

        // Retain grouped column values so `SELECT id, count(*) ... GROUP BY id`
        // keeps `id` in the output.
        for (idx, g) in stmt.group_by.iter().enumerate() {
            let key = match g {
                Expr::Identifier(name) => name.clone(),
                Expr::FieldAccess {
                    parent: _,
                    field: leaf,
                } => leaf.clone(),
                _ => Self::column_name(g, idx),
            };
            let val = match first {
                Some(rec) => Self::eval_val(g, rec),
                None => Value::Null,
            };
            output.insert(key, val);
        }

        for (idx, field) in stmt.fields.iter().enumerate() {
            match field {
                Expr::Wildcard => {
                    if let Some(rec) = first {
                        for (k, v) in rec {
                            output.entry(k.clone()).or_insert_with(|| v.clone());
                        }
                    }
                }
                Expr::Identifier(name) => {
                    let key = stmt
                        .field_aliases
                        .get(idx)
                        .and_then(|a| a.clone())
                        .unwrap_or_else(|| name.clone());
                    // Prefer already-retained group value; otherwise first record.
                    output.entry(key).or_insert_with(|| {
                        first
                            .and_then(|rec| rec.get(name).cloned())
                            .unwrap_or(Value::Null)
                    });
                }
                Expr::FieldAccess {
                    parent: _,
                    field: leaf,
                } => {
                    let key = stmt
                        .field_aliases
                        .get(idx)
                        .and_then(|a| a.clone())
                        .unwrap_or_else(|| leaf.clone());
                    output.entry(key).or_insert_with(|| match first {
                        Some(rec) => Self::eval_val(field, rec),
                        None => Value::Null,
                    });
                }
                Expr::Call { name, args } if Self::is_aggregate_call(name) => {
                    let val = Self::eval_aggregate_call(name, args, records);
                    let key = stmt
                        .field_aliases
                        .get(idx)
                        .and_then(|a| a.clone())
                        .unwrap_or_else(|| Self::column_name(field, idx));
                    output.insert(key, val);
                }
                _ => {
                    // General expression: may contain nested aggregates
                    // (e.g. `avg(temp) + 1`), so evaluate aggregate-aware.
                    let val = Self::eval_agg_expr(field, records, &output);
                    let key = stmt
                        .field_aliases
                        .get(idx)
                        .and_then(|a| a.clone())
                        .unwrap_or_else(|| Self::column_name(field, idx));
                    // Don't overwrite group keys / wildcard copies with same key.
                    output.entry(key).or_insert(val);
                }
            }
        }

        if let Some(having) = &stmt.having {
            let v = Self::eval_agg_expr(having, records, &output);
            match v {
                Value::Bool(true) => {}
                _ => return None,
            }
        }

        Some(output)
    }

    fn is_aggregate_call(name: &str) -> bool {
        matches!(
            name.to_ascii_lowercase().as_str(),
            "count" | "sum" | "avg" | "min" | "max" | "collect" | "lead" | "latest"
        )
    }

    fn eval_aggregate_call(name: &str, args: &[Expr], records: &[HashMap<String, Value>]) -> Value {
        match name.to_ascii_lowercase().as_str() {
            "count" => Self::agg_count(args, records),
            "sum" => Self::agg_sum(args, records),
            "avg" => Self::agg_avg(args, records),
            "min" => Self::agg_min(args, records),
            "max" => Self::agg_max(args, records),
            "collect" => Self::agg_collect(args, records),
            "lead" => Self::agg_lead(args, records),
            "latest" => Self::agg_latest(args, records),
            _ => Value::Null,
        }
    }

    /// Evaluate an expression in aggregate (window) context.
    ///
    /// Aggregate calls (`count/sum/avg/min/max`) are resolved over `records`;
    /// plain columns resolve from `output` (group keys / already computed
    /// aggregates) falling back to the first record; everything else recurses.
    fn eval_agg_expr(
        expr: &Expr,
        records: &[HashMap<String, Value>],
        output: &HashMap<String, Value>,
    ) -> Value {
        match expr {
            Expr::Wildcard => Value::Null,
            Expr::Literal(v) => v.clone(),
            Expr::Identifier(name) => {
                if let Some(v) = output.get(name) {
                    return v.clone();
                }
                records
                    .first()
                    .and_then(|rec| rec.get(name).cloned())
                    .unwrap_or(Value::Null)
            }
            Expr::FieldAccess {
                parent: _,
                field: leaf,
            } => {
                if let Some(v) = output.get(leaf) {
                    return v.clone();
                }
                // Also try the full dotted path key.
                let full = Self::column_name(expr, 0);
                if let Some(v) = output.get(&full) {
                    return v.clone();
                }
                match records.first() {
                    Some(rec) => Self::eval_val(expr, rec),
                    None => Value::Null,
                }
            }
            Expr::Call { name, args } => {
                if Self::is_aggregate_call(name) {
                    return Self::eval_aggregate_call(name, args, records);
                }
                // Contextual system functions resolve against the first batch
                // record when one exists.
                if let Some(first) = records.first() {
                    if let Some(v) = Self::eval_context_call(name, args, first) {
                        return v;
                    }
                }
                let vals: Vec<Value> = args
                    .iter()
                    .map(|a| Self::eval_agg_expr(a, records, output))
                    .collect();
                Self::eval_call(name, &vals)
            }
            // Batch window evaluation has no per-partition running state;
            // evaluate the inner call against the batch instead.
            Expr::Over { call, .. } => Self::eval_agg_expr(call, records, output),
            Expr::BinaryOp { left, op, right } => {
                let l = Self::eval_agg_expr(left, records, output);
                let r = Self::eval_agg_expr(right, records, output);
                Self::eval_binary_op(&l, op, &r)
            }
            Expr::UnaryOp { op, expr } => {
                let v = Self::eval_agg_expr(expr, records, output);
                Self::eval_unary_op(op, &v)
            }
            Expr::Between {
                expr,
                low,
                high,
                negated,
            } => {
                let v = Self::eval_agg_expr(expr, records, output);
                let l = Self::eval_agg_expr(low, records, output);
                let h = Self::eval_agg_expr(high, records, output);
                Self::eval_between(&v, &l, &h, *negated)
            }
            Expr::InList {
                expr,
                list,
                negated,
            } => {
                let v = Self::eval_agg_expr(expr, records, output);
                let mut matched = false;
                for item in list {
                    let iv = Self::eval_agg_expr(item, records, output);
                    if Self::values_equal(&v, &iv) {
                        matched = true;
                        break;
                    }
                }
                Value::Bool(if *negated { !matched } else { matched })
            }
            Expr::IsNull { expr, negated } => {
                let v = Self::eval_agg_expr(expr, records, output);
                Value::Bool(if *negated { !v.is_null() } else { v.is_null() })
            }
            Expr::Case {
                operand,
                when_clauses,
                else_clause,
            } => Self::eval_case(operand, when_clauses, else_clause, |e| {
                Self::eval_agg_expr(e, records, output)
            }),
        }
    }

    /// Shared CASE evaluation over an arbitrary sub-expression evaluator.
    /// Simple CASE compares the operand with each WHEN value via
    /// [`Self::values_equal`]; searched CASE treats each WHEN as a boolean
    /// condition. Falls back to ELSE or `Null` when nothing matches.
    fn eval_case<E>(
        operand: &Option<Box<Expr>>,
        when_clauses: &[(Expr, Expr)],
        else_clause: &Option<Box<Expr>>,
        eval: E,
    ) -> Value
    where
        E: Fn(&Expr) -> Value,
    {
        if let Some(op) = operand {
            let op_val = eval(op);
            for (when_expr, then_expr) in when_clauses {
                if Self::values_equal(&op_val, &eval(when_expr)) {
                    return eval(then_expr);
                }
            }
        } else {
            for (when_cond, then_expr) in when_clauses {
                if matches!(eval(when_cond), Value::Bool(true)) {
                    return eval(then_expr);
                }
            }
        }
        match else_clause {
            Some(e) => eval(e),
            None => Value::Null,
        }
    }

    // ---------- stateful analytic evaluation (acc_* + OVER) ----------

    /// Stateful per-record projection for rules using analytic cumulative
    /// functions.
    ///
    /// Unlike [`Self::eval_select`], this deliberately does **not** apply
    /// `where_clause`: the projection (and therefore the analytic state)
    /// advances on every input row, while the caller applies the input-row
    /// filter to decide emission. This mirrors eKuiper analytic semantics,
    /// where e.g. `acc_map_agg` accumulates rows that a later `WHERE` filters
    /// out of the output.
    pub fn eval_select_stateful(
        stmt: &SelectStmt,
        record: &HashMap<String, Value>,
        state: &RuleState,
    ) -> Option<HashMap<String, Value>> {
        if let Some((_, rhs)) = &stmt.set_op {
            let left = Self::eval_select_stateful(&Self::without_set_op(stmt), record, state);
            let right = Self::eval_select_stateful(rhs, record, state);
            return Self::merge_union_rows(left, right);
        }
        let mut output = HashMap::new();
        for (idx, field) in stmt.fields.iter().enumerate() {
            let alias = stmt.field_aliases.get(idx).and_then(|a| a.clone());
            match field {
                Expr::Wildcard => {
                    for (k, v) in record {
                        output.insert(k.clone(), v.clone());
                    }
                }
                Expr::Identifier(name) => {
                    let key = alias.unwrap_or_else(|| name.clone());
                    if let Some(val) = record.get(name) {
                        output.insert(key, val.clone());
                    } else {
                        output.insert(key, Value::Null);
                    }
                }
                Expr::FieldAccess {
                    parent: _,
                    field: leaf,
                } => {
                    let val = Self::eval_stateful_expr(field, record, state);
                    output.insert(alias.unwrap_or_else(|| leaf.clone()), val);
                }
                _ => {
                    let val = Self::eval_stateful_expr(field, record, state);
                    let name = alias.unwrap_or_else(|| Self::column_name(field, idx));
                    output.insert(name, val);
                }
            }
        }
        Some(output)
    }

    /// Multi-row stateful projection supporting `unnest(array_expr)`.
    ///
    /// Evaluates fields with [`Self::eval_select_stateful`]; when a top-level
    /// field is an `unnest(array)` call, the base row is cloned once per
    /// array item: object items merge their sub-keys into the row (the
    /// placeholder entry is dropped), while scalar items bind under the
    /// field alias (or generated column name). A non-array argument passes
    /// the base row through unchanged; without any `unnest` field the result
    /// is a single-element vector.
    pub fn eval_select_stateful_multi(
        stmt: &SelectStmt,
        record: &HashMap<String, Value>,
        state: &RuleState,
    ) -> Vec<HashMap<String, Value>> {
        let mut rows = Self::eval_own_rows_multi(stmt, record, state);
        if let Some((op, rhs)) = &stmt.set_op {
            let right = Self::eval_select_stateful_multi(rhs, record, state);
            if *op == SetOp::Union {
                for row in right {
                    if !rows.contains(&row) {
                        rows.push(row);
                    }
                }
            } else {
                rows.extend(right);
            }
        }
        rows
    }

    /// Single-statement portion of [`Self::eval_select_stateful_multi`]:
    /// projection (+ unnest expansion) without following `set_op`.
    fn eval_own_rows_multi(
        stmt: &SelectStmt,
        record: &HashMap<String, Value>,
        state: &RuleState,
    ) -> Vec<HashMap<String, Value>> {
        // NOTE: set_op-free base on purpose — the caller combines branches.
        let single = Self::without_set_op(stmt);
        let Some(base) = Self::eval_select_stateful(&single, record, state) else {
            return Vec::new();
        };
        let unnest_pos = stmt.fields.iter().position(
            |f| matches!(f, Expr::Call { name, .. } if name.eq_ignore_ascii_case("unnest")),
        );
        let Some(idx) = unnest_pos else {
            return vec![base];
        };
        let Expr::Call { args, .. } = &stmt.fields[idx] else {
            return vec![base];
        };
        let items = match args.first() {
            Some(arg) => Self::eval_stateful_expr(arg, record, state),
            None => Value::Null,
        };
        let Value::Array(items) = items else {
            return vec![base];
        };
        let placeholder = stmt
            .field_aliases
            .get(idx)
            .and_then(|a| a.clone())
            .unwrap_or_else(|| Self::column_name(&stmt.fields[idx], idx));
        let mut rows = Vec::with_capacity(items.len());
        for item in items {
            let mut row = base.clone();
            row.remove(&placeholder);
            match item {
                Value::Object(map) => {
                    for (k, v) in map {
                        row.insert(k, v);
                    }
                }
                scalar => {
                    row.insert(placeholder.clone(), scalar);
                }
            }
            rows.push(row);
        }
        rows
    }

    /// Evaluate an expression with analytic (`acc_*`) calls resolved against
    /// `state`. All other expressions behave exactly like [`Self::eval_val`].
    fn eval_stateful_expr(
        expr: &Expr,
        record: &HashMap<String, Value>,
        state: &RuleState,
    ) -> Value {
        match expr {
            Expr::Wildcard => Value::Null,
            Expr::Literal(val) => val.clone(),
            Expr::Identifier(name) => record.get(name).cloned().unwrap_or(Value::Null),
            Expr::FieldAccess { parent, field } => {
                let parent_val = Self::eval_stateful_expr(parent, record, state);
                match parent_val {
                    Value::Object(map) => map.get(field).cloned().unwrap_or(Value::Null),
                    _ => Value::Null,
                }
            }
            Expr::BinaryOp { left, op, right } => {
                let l = Self::eval_stateful_expr(left, record, state);
                let r = Self::eval_stateful_expr(right, record, state);
                Self::eval_binary_op(&l, op, &r)
            }
            Expr::UnaryOp { op, expr } => {
                let v = Self::eval_stateful_expr(expr, record, state);
                Self::eval_unary_op(op, &v)
            }
            Expr::Between {
                expr,
                low,
                high,
                negated,
            } => {
                let v = Self::eval_stateful_expr(expr, record, state);
                let l = Self::eval_stateful_expr(low, record, state);
                let h = Self::eval_stateful_expr(high, record, state);
                Self::eval_between(&v, &l, &h, *negated)
            }
            Expr::InList {
                expr,
                list,
                negated,
            } => {
                let v = Self::eval_stateful_expr(expr, record, state);
                let mut matched = false;
                for item in list {
                    let item_val = Self::eval_stateful_expr(item, record, state);
                    if Self::values_equal(&v, &item_val) {
                        matched = true;
                        break;
                    }
                }
                if *negated {
                    Value::Bool(!matched)
                } else {
                    Value::Bool(matched)
                }
            }
            Expr::IsNull { expr, negated } => {
                let v = Self::eval_stateful_expr(expr, record, state);
                let is_null = v.is_null();
                if *negated {
                    Value::Bool(!is_null)
                } else {
                    Value::Bool(is_null)
                }
            }
            Expr::Call { .. } => Self::eval_stateful_call(expr, record, state, None),
            Expr::Over { call, partition_by } => {
                let partition_key = match partition_by {
                    Some(p) => Self::value_to_key(&Self::eval_stateful_expr(p, record, state)),
                    None => String::new(),
                };
                Self::eval_stateful_call(call, record, state, Some(&partition_key))
            }
            Expr::Case {
                operand,
                when_clauses,
                else_clause,
            } => Self::eval_case(operand, when_clauses, else_clause, |e| {
                Self::eval_stateful_expr(e, record, state)
            }),
        }
    }

    /// Evaluate a call expression statefully: cumulative `acc_*` functions go
    /// through [`Self::eval_acc_call`], everything else through the scalar
    /// [`Self::eval_call`] (with statefully-resolved arguments so nesting
    /// like `object_construct('x', acc_max(t))` works).
    fn eval_stateful_call(
        expr: &Expr,
        record: &HashMap<String, Value>,
        state: &RuleState,
        partition_key: Option<&str>,
    ) -> Value {
        let (name, args) = match expr {
            Expr::Call { name, args } => (name, args),
            _ => return Self::eval_stateful_expr(expr, record, state),
        };
        let lowered = name.to_ascii_lowercase();
        if Self::is_acc_call(&lowered) {
            let vals: Vec<Value> = args
                .iter()
                .map(|a| Self::eval_stateful_expr(a, record, state))
                .collect();
            let call_id = Self::column_name(expr, 0);
            return Self::eval_acc_call(
                &lowered,
                &vals,
                state,
                &call_id,
                partition_key.unwrap_or(""),
            );
        }
        if lowered == "lag" {
            let vals: Vec<Value> = args
                .iter()
                .map(|a| Self::eval_stateful_expr(a, record, state))
                .collect();
            let call_id = Self::column_name(expr, 0);
            return Self::eval_lag(&vals, state, &call_id, partition_key.unwrap_or(""));
        }
        if lowered == "had_changed" || lowered == "changed_col" {
            let vals: Vec<Value> = args
                .iter()
                .map(|a| Self::eval_stateful_expr(a, record, state))
                .collect();
            let call_id = Self::column_name(expr, 0);
            return Self::eval_changed(
                &lowered,
                &vals,
                state,
                &call_id,
                partition_key.unwrap_or(""),
            );
        }
        // Contextual system functions resolve against the record itself.
        if let Some(v) = Self::eval_context_call(name, args, record) {
            return v;
        }
        let vals: Vec<Value> = args
            .iter()
            .map(|a| Self::eval_stateful_expr(a, record, state))
            .collect();
        Self::eval_call(name, &vals)
    }

    fn is_acc_call(lowered_name: &str) -> bool {
        matches!(
            lowered_name,
            "acc_map_agg"
                | "acc_max"
                | "acc_max_by"
                | "acc_min"
                | "acc_min_by"
                | "acc_count"
                | "acc_sum"
                | "acc_avg"
        )
    }

    /// Deterministic, type-tagged rendering of a partition value for state keys.
    fn value_to_key(v: &Value) -> String {
        match v {
            Value::Null => "null:".to_string(),
            Value::Bool(b) => format!("bool:{}", b),
            Value::Number(n) => format!("num:{}", n),
            Value::String(s) => format!("str:{}", s),
            Value::Array(_) | Value::Object(_) => {
                format!("json:{}", serde_json::to_string(v).unwrap_or_default())
            }
        }
    }

    fn eval_acc_call(
        lowered_name: &str,
        args: &[Value],
        state: &RuleState,
        call_id: &str,
        partition_key: &str,
    ) -> Value {
        let state_key = format!("{}:{}:{}", lowered_name, call_id, partition_key);
        match lowered_name {
            "acc_map_agg" => Self::acc_map_agg(state, &state_key, args),
            "acc_max" => Self::acc_extreme(state, &state_key, args, true),
            "acc_max_by" => Self::acc_extreme_by(state, &state_key, args, true),
            "acc_min" => Self::acc_extreme(state, &state_key, args, false),
            "acc_min_by" => Self::acc_extreme_by(state, &state_key, args, false),
            "acc_count" => Self::acc_count(state, &state_key, args),
            "acc_sum" => Self::acc_sum(state, &state_key, args),
            "acc_avg" => Self::acc_avg(state, &state_key, args),
            _ => Value::Null,
        }
    }

    /// `lag(val, [offset], [default])`: returns the value seen `offset`
    /// rows ago within the same partition (default 1), or `default`
    /// (default `Null`) when fewer rows have been seen. The current value is
    /// appended to history after the lookup. State key:
    /// `lag:{func_call_id}:{partition_key}`.
    fn eval_lag(args: &[Value], state: &RuleState, call_id: &str, partition_key: &str) -> Value {
        if args.is_empty() || args.len() > 3 {
            return Value::Null;
        }
        let offset = args.get(1).and_then(Self::to_i64_arg).unwrap_or(1);
        if offset <= 0 {
            return args[0].clone();
        }
        let default = args.get(2).cloned().unwrap_or(Value::Null);
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
        history.push(args[0].clone());
        guard.insert(state_key, Value::Array(history));
        out
    }

    /// `acc_map_agg(key, val)`: maintains an array of `{key, value}` objects
    /// in state, updating the value in place when the key already exists.
    fn acc_map_agg(state: &RuleState, state_key: &str, args: &[Value]) -> Value {
        if args.len() != 2 {
            return Value::Null;
        }
        let key_str = Self::to_string_always(&args[0]);
        let mut entries: Vec<(String, Value)> = Vec::new();
        {
            let guard = state.state.read();
            if let Some(Value::Array(arr)) = guard.get(state_key) {
                for item in arr {
                    if let Value::Object(map) = item {
                        if let (Some(Value::String(k)), Some(v)) =
                            (map.get("key"), map.get("value"))
                        {
                            entries.push((k.clone(), v.clone()));
                        }
                    }
                }
            }
        }
        match entries.iter_mut().find(|(k, _)| *k == key_str) {
            Some((_, v)) => *v = args[1].clone(),
            None => entries.push((key_str, args[1].clone())),
        }
        let out = Value::Array(
            entries
                .iter()
                .map(|(k, v)| {
                    let mut map = serde_json::Map::with_capacity(2);
                    map.insert("key".to_string(), Value::String(k.clone()));
                    map.insert("value".to_string(), v.clone());
                    Value::Object(map)
                })
                .collect(),
        );
        state
            .state
            .write()
            .insert(state_key.to_string(), out.clone());
        out
    }

    /// Shared running-extremum logic for `acc_max`/`acc_min` (single value)
    /// using [`Self::compare_values`] so numbers and strings both work.
    /// Null or incomparable inputs leave the state untouched.
    fn acc_extreme(
        state: &RuleState,
        state_key: &str,
        args: &[Value],
        take_greater: bool,
    ) -> Value {
        if args.len() != 1 {
            return Value::Null;
        }
        let val = &args[0];
        if val.is_null() {
            return state
                .state
                .read()
                .get(state_key)
                .cloned()
                .unwrap_or(Value::Null);
        }
        let mut guard = state.state.write();
        match guard.get(state_key).cloned() {
            Some(current) => match Self::compare_values(val, &current) {
                Some(ord)
                    if (take_greater && ord == std::cmp::Ordering::Greater)
                        || (!take_greater && ord == std::cmp::Ordering::Less) =>
                {
                    guard.insert(state_key.to_string(), val.clone());
                    val.clone()
                }
                _ => current,
            },
            None => {
                guard.insert(state_key.to_string(), val.clone());
                val.clone()
            }
        }
    }

    /// Shared logic for `acc_max_by(val, compare)` / `acc_min_by`: the stored
    /// `(compare, value)` pair is replaced when the new `compare` wins
    /// (greater-or-equal for max, less-or-equal for min); the running `value`
    /// is returned.
    fn acc_extreme_by(
        state: &RuleState,
        state_key: &str,
        args: &[Value],
        take_greater: bool,
    ) -> Value {
        if args.len() != 2 {
            return Value::Null;
        }
        let (val, compare) = (&args[0], &args[1]);
        let stored: Option<(Value, Value)> = state.state.read().get(state_key).and_then(|v| {
            if let Value::Object(map) = v {
                match (map.get("compare"), map.get("value")) {
                    (Some(c), Some(val)) => Some((c.clone(), val.clone())),
                    _ => None,
                }
            } else {
                None
            }
        });
        if val.is_null() || compare.is_null() {
            return stored.map(|(_, v)| v).unwrap_or(Value::Null);
        }
        let should_store = match &stored {
            None => true,
            Some((stored_compare, _)) => match Self::compare_values(compare, stored_compare) {
                Some(ord) => {
                    ord == std::cmp::Ordering::Equal
                        || (take_greater && ord == std::cmp::Ordering::Greater)
                        || (!take_greater && ord == std::cmp::Ordering::Less)
                }
                None => false,
            },
        };
        if should_store {
            let mut map = serde_json::Map::with_capacity(2);
            map.insert("compare".to_string(), compare.clone());
            map.insert("value".to_string(), val.clone());
            state
                .state
                .write()
                .insert(state_key.to_string(), Value::Object(map));
            val.clone()
        } else {
            stored.map(|(_, v)| v).unwrap_or(Value::Null)
        }
    }

    /// `acc_count(val)`: running counter, skipping Null inputs.
    fn acc_count(state: &RuleState, state_key: &str, args: &[Value]) -> Value {
        if args.len() != 1 {
            return Value::Null;
        }
        let mut guard = state.state.write();
        if args[0].is_null() {
            return guard.get(state_key).cloned().unwrap_or(Value::from(0));
        }
        let next = guard
            .get(state_key)
            .and_then(|v| v.as_i64())
            .unwrap_or(0)
            .saturating_add(1);
        guard.insert(state_key.to_string(), Value::from(next));
        Value::from(next)
    }

    /// `acc_sum(val)`: running sum over numeric inputs (int-preserving via
    /// [`Self::eval_arith`]); non-numeric inputs leave state untouched.
    fn acc_sum(state: &RuleState, state_key: &str, args: &[Value]) -> Value {
        if args.len() != 1 {
            return Value::Null;
        }
        let val = &args[0];
        if val.is_null() || !val.is_number() {
            return state
                .state
                .read()
                .get(state_key)
                .cloned()
                .unwrap_or(Value::Null);
        }
        let mut guard = state.state.write();
        let next = match guard.get(state_key).cloned() {
            Some(cur) => Self::eval_arith(&cur, val, ArithOp::Add),
            None => val.clone(),
        };
        guard.insert(state_key.to_string(), next.clone());
        next
    }

    /// `acc_avg(val)`: maintains `(sum, count)` in state, returns `sum/count`
    /// as a float; non-numeric inputs leave state untouched.
    fn acc_avg(state: &RuleState, state_key: &str, args: &[Value]) -> Value {
        if args.len() != 1 {
            return Value::Null;
        }
        let val = &args[0];
        if val.is_null() || !val.is_number() {
            return Self::acc_avg_current(state, state_key);
        }
        let mut guard = state.state.write();
        let (sum, count) = match guard.get(state_key) {
            Some(Value::Object(map)) => {
                let sum = map.get("sum").cloned().unwrap_or(serde_json::json!(0));
                let count = map.get("count").and_then(|v| v.as_i64()).unwrap_or(0);
                (sum, count)
            }
            _ => (serde_json::json!(0), 0),
        };
        let sum = Self::eval_arith(&sum, val, ArithOp::Add);
        let count = count.saturating_add(1);
        let avg = match (sum.as_f64(), count) {
            (Some(s), c) if c > 0 => serde_json::json!(s / (c as f64)),
            _ => Value::Null,
        };
        let mut map = serde_json::Map::with_capacity(2);
        map.insert("sum".to_string(), sum);
        map.insert("count".to_string(), Value::from(count));
        guard.insert(state_key.to_string(), Value::Object(map));
        avg
    }

    /// Current average without updating state.
    fn acc_avg_current(state: &RuleState, state_key: &str) -> Value {
        let guard = state.state.read();
        match guard.get(state_key) {
            Some(Value::Object(map)) => {
                let sum = map.get("sum").and_then(|v| v.as_f64());
                let count = map.get("count").and_then(|v| v.as_i64());
                match (sum, count) {
                    (Some(s), Some(c)) if c > 0 => serde_json::json!(s / (c as f64)),
                    _ => Value::Null,
                }
            }
            _ => Value::Null,
        }
    }

    fn agg_count(args: &[Expr], records: &[HashMap<String, Value>]) -> Value {
        if args.is_empty() {
            return Value::Null;
        }
        // count(*) counts all rows.
        if args.len() == 1 && matches!(args[0], Expr::Wildcard) {
            return Value::from(records.len() as i64);
        }
        let mut n: i64 = 0;
        for rec in records {
            let v = Self::eval_val(&args[0], rec);
            if !v.is_null() {
                n += 1;
            }
        }
        Value::from(n)
    }

    /// Collect numeric arg values across the batch, skipping nulls/non-numerics.
    fn agg_numeric_values(arg: &Expr, records: &[HashMap<String, Value>]) -> Vec<Value> {
        let mut out = Vec::new();
        for rec in records {
            let v = Self::eval_val(arg, rec);
            if v.is_null() {
                continue;
            }
            if v.is_number() {
                out.push(v);
            }
        }
        out
    }

    fn agg_sum(args: &[Expr], records: &[HashMap<String, Value>]) -> Value {
        if args.len() != 1 {
            return Value::Null;
        }
        if matches!(args[0], Expr::Wildcard) {
            return Value::Null;
        }
        let vals = Self::agg_numeric_values(&args[0], records);
        if vals.is_empty() {
            return Value::Null;
        }
        if vals.iter().all(|v| v.as_i64().is_some()) {
            let mut acc: i64 = 0;
            for v in &vals {
                let i = v.as_i64().unwrap();
                match acc.checked_add(i) {
                    Some(n) => acc = n,
                    None => {
                        // Overflow: fall back to float sum.
                        let f: f64 = vals.iter().filter_map(|x| x.as_f64()).sum();
                        return serde_json::json!(f);
                    }
                }
            }
            return Value::from(acc);
        }
        let sum: f64 = vals.iter().filter_map(|v| v.as_f64()).sum();
        serde_json::json!(sum)
    }

    fn agg_avg(args: &[Expr], records: &[HashMap<String, Value>]) -> Value {
        if args.len() != 1 {
            return Value::Null;
        }
        if matches!(args[0], Expr::Wildcard) {
            return Value::Null;
        }
        let vals = Self::agg_numeric_values(&args[0], records);
        if vals.is_empty() {
            return Value::Null;
        }
        let sum: f64 = vals.iter().filter_map(|v| v.as_f64()).sum();
        serde_json::json!(sum / (vals.len() as f64))
    }

    fn agg_min(args: &[Expr], records: &[HashMap<String, Value>]) -> Value {
        if args.len() != 1 {
            return Value::Null;
        }
        if matches!(args[0], Expr::Wildcard) {
            return Value::Null;
        }
        let vals = Self::agg_numeric_values(&args[0], records);
        if vals.is_empty() {
            return Value::Null;
        }
        let mut best = &vals[0];
        for v in &vals[1..] {
            if let Some(ord) = Self::compare_values(v, best) {
                if ord == std::cmp::Ordering::Less {
                    best = v;
                }
            }
        }
        best.clone()
    }

    fn agg_max(args: &[Expr], records: &[HashMap<String, Value>]) -> Value {
        if args.len() != 1 {
            return Value::Null;
        }
        if matches!(args[0], Expr::Wildcard) {
            return Value::Null;
        }
        let vals = Self::agg_numeric_values(&args[0], records);
        if vals.is_empty() {
            return Value::Null;
        }
        let mut best = &vals[0];
        for v in &vals[1..] {
            if let Some(ord) = Self::compare_values(v, best) {
                if ord == std::cmp::Ordering::Greater {
                    best = v;
                }
            }
        }
        best.clone()
    }

    /// `collect(col)`: all non-null values of `col` across the window batch,
    /// in row order.
    fn agg_collect(args: &[Expr], records: &[HashMap<String, Value>]) -> Value {
        if args.len() != 1 {
            return Value::Null;
        }
        if matches!(args[0], Expr::Wildcard) {
            return Value::Null;
        }
        Value::Array(
            records
                .iter()
                .map(|rec| Self::eval_val(&args[0], rec))
                .filter(|v| !v.is_null())
                .collect(),
        )
    }

    /// `latest(col)`: the most recent (last) non-null value of `col` in the
    /// batch, or `Null` when there is none.
    fn agg_latest(args: &[Expr], records: &[HashMap<String, Value>]) -> Value {
        if args.len() != 1 {
            return Value::Null;
        }
        if matches!(args[0], Expr::Wildcard) {
            return Value::Null;
        }
        records
            .iter()
            .rev()
            .map(|rec| Self::eval_val(&args[0], rec))
            .find(|v| !v.is_null())
            .unwrap_or(Value::Null)
    }

    /// `lead(col, [offset], [default])`: forward lookup within the window
    /// batch. The single output row represents the whole window, so `offset`
    /// (default 1) counts forward from the first row (0-based); out-of-range
    /// offsets yield `default` (default `Null`).
    fn agg_lead(args: &[Expr], records: &[HashMap<String, Value>]) -> Value {
        if args.is_empty() || args.len() > 3 {
            return Value::Null;
        }
        if matches!(args[0], Expr::Wildcard) {
            return Value::Null;
        }
        let first = records.first();
        let offset: i64 = if args.len() >= 2 {
            first
                .and_then(|rec| Self::to_i64_arg(&Self::eval_val(&args[1], rec)))
                .unwrap_or(1)
        } else {
            1
        };
        let default: Value = if args.len() >= 3 {
            first
                .map(|rec| Self::eval_val(&args[2], rec))
                .unwrap_or(Value::Null)
        } else {
            Value::Null
        };
        if offset < 0 {
            return default;
        }
        records
            .get(offset as usize)
            .map(|rec| Self::eval_val(&args[0], rec))
            .unwrap_or(default)
    }

    fn op_str(op: &BinaryOperator) -> &'static str {
        match op {
            BinaryOperator::Eq => "=",
            BinaryOperator::Neq => "!=",
            BinaryOperator::Lt => "<",
            BinaryOperator::Lte => "<=",
            BinaryOperator::Gt => ">",
            BinaryOperator::Gte => ">=",
            BinaryOperator::And => "AND",
            BinaryOperator::Or => "OR",
            BinaryOperator::Add => "+",
            BinaryOperator::Sub => "-",
            BinaryOperator::Mul => "*",
            BinaryOperator::Div => "/",
            BinaryOperator::Mod => "%",
            BinaryOperator::Like => "LIKE",
        }
    }

    fn column_name(expr: &Expr, idx: usize) -> String {
        match expr {
            Expr::Wildcard => "*".to_string(),
            Expr::Identifier(name) => name.clone(),
            Expr::FieldAccess { parent, field } => {
                // Full dotted path for uniqueness, e.g. dev.temp
                let parent_name = Self::column_name(parent, idx);
                if parent_name.is_empty() {
                    field.clone()
                } else {
                    format!("{}.{}", parent_name, field)
                }
            }
            Expr::Literal(v) => match v {
                Value::Null => "NULL".to_string(),
                Value::Bool(b) => b.to_string(),
                Value::Number(n) => n.to_string(),
                Value::String(s) => s.clone(),
                _ => format!("col{}", idx),
            },
            Expr::BinaryOp { left, op, right } => {
                format!(
                    "{} {} {}",
                    Self::column_name(left, idx),
                    Self::op_str(op),
                    Self::column_name(right, idx)
                )
            }
            Expr::UnaryOp { op, expr } => match op {
                UnaryOperator::Not => format!("NOT {}", Self::column_name(expr, idx)),
                UnaryOperator::Neg => format!("-{}", Self::column_name(expr, idx)),
            },
            Expr::Between {
                expr,
                low,
                high,
                negated,
            } => {
                let not = if *negated { "NOT " } else { "" };
                format!(
                    "{} {}BETWEEN {} AND {}",
                    Self::column_name(expr, idx),
                    not,
                    Self::column_name(low, idx),
                    Self::column_name(high, idx)
                )
            }
            Expr::InList {
                expr,
                list,
                negated,
            } => {
                let not = if *negated { "NOT " } else { "" };
                let items: Vec<String> = list.iter().map(|e| Self::column_name(e, idx)).collect();
                format!(
                    "{} {}IN ({})",
                    Self::column_name(expr, idx),
                    not,
                    items.join(", ")
                )
            }
            Expr::IsNull { expr, negated } => {
                let not = if *negated { "NOT " } else { "" };
                format!("{} IS {}NULL", Self::column_name(expr, idx), not)
            }
            Expr::Call { name, args } => {
                let inner: Vec<String> = args.iter().map(|a| Self::column_name(a, idx)).collect();
                format!("{}({})", name, inner.join(", "))
            }
            Expr::Over { call, partition_by } => match partition_by {
                Some(p) => format!(
                    "{} OVER (PARTITION BY {})",
                    Self::column_name(call, idx),
                    Self::column_name(p, idx)
                ),
                None => format!("{} OVER ()", Self::column_name(call, idx)),
            },
            Expr::Case {
                operand,
                when_clauses,
                else_clause,
            } => {
                let mut s = String::from("CASE");
                if let Some(op) = operand {
                    s.push(' ');
                    s.push_str(&Self::column_name(op, idx));
                }
                for (w, t) in when_clauses {
                    s.push_str(&format!(
                        " WHEN {} THEN {}",
                        Self::column_name(w, idx),
                        Self::column_name(t, idx)
                    ));
                }
                if let Some(e) = else_clause {
                    s.push_str(&format!(" ELSE {}", Self::column_name(e, idx)));
                }
                s.push_str(" END");
                s
            }
        }
    }

    pub fn eval_bool(expr: &Expr, record: &HashMap<String, Value>) -> bool {
        match Self::eval_val(expr, record) {
            Value::Bool(b) => b,
            Value::Null => false,
            _ => false,
        }
    }

    pub fn eval_val(expr: &Expr, record: &HashMap<String, Value>) -> Value {
        match expr {
            Expr::Wildcard => Value::Null,
            Expr::Literal(val) => val.clone(),
            Expr::Identifier(name) => record.get(name).cloned().unwrap_or(Value::Null),
            Expr::FieldAccess { parent, field } => {
                let parent_val = Self::eval_val(parent, record);
                match parent_val {
                    Value::Object(map) => map.get(field).cloned().unwrap_or(Value::Null),
                    _ => Value::Null,
                }
            }
            Expr::BinaryOp { left, op, right } => {
                let l = Self::eval_val(left, record);
                let r = Self::eval_val(right, record);
                Self::eval_binary_op(&l, op, &r)
            }
            Expr::UnaryOp { op, expr } => {
                let v = Self::eval_val(expr, record);
                Self::eval_unary_op(op, &v)
            }
            Expr::Between {
                expr,
                low,
                high,
                negated,
            } => {
                let v = Self::eval_val(expr, record);
                let l = Self::eval_val(low, record);
                let h = Self::eval_val(high, record);
                Self::eval_between(&v, &l, &h, *negated)
            }
            Expr::InList {
                expr,
                list,
                negated,
            } => {
                let v = Self::eval_val(expr, record);
                let mut matched = false;
                for item in list {
                    let item_val = Self::eval_val(item, record);
                    if Self::values_equal(&v, &item_val) {
                        matched = true;
                        break;
                    }
                }
                if *negated {
                    Value::Bool(!matched)
                } else {
                    Value::Bool(matched)
                }
            }
            Expr::IsNull { expr, negated } => {
                let v = Self::eval_val(expr, record);
                let is_null = v.is_null();
                if *negated {
                    Value::Bool(!is_null)
                } else {
                    Value::Bool(is_null)
                }
            }
            Expr::Call { name, args } => {
                // Contextual system functions (meta/mqtt/event_time/...)
                // need the raw argument expressions plus the record.
                if let Some(v) = Self::eval_context_call(name, args, record) {
                    return v;
                }
                let vals: Vec<Value> = args.iter().map(|a| Self::eval_val(a, record)).collect();
                Self::eval_call(name, &vals)
            }
            // Stateless single-record evaluation ignores partitioning (there is
            // no shared state); cumulative `acc_*` calls yield `Null` here.
            Expr::Over { call, .. } => Self::eval_val(call, record),
            Expr::Case {
                operand,
                when_clauses,
                else_clause,
            } => Self::eval_case(operand, when_clauses, else_clause, |e| {
                Self::eval_val(e, record)
            }),
        }
    }

    fn eval_unary_op(op: &UnaryOperator, val: &Value) -> Value {
        match op {
            UnaryOperator::Not => match val {
                Value::Bool(b) => Value::Bool(!b),
                Value::Null => Value::Null,
                _ => Value::Null,
            },
            UnaryOperator::Neg => {
                if val.is_null() {
                    return Value::Null;
                }
                if let Some(i) = val.as_i64() {
                    // checked_neg to avoid overflow on i64::MIN
                    if let Some(n) = i.checked_neg() {
                        return Value::from(n);
                    } else {
                        // i64::MIN negated overflows; fall back to float
                        return serde_json::json!(-(i as f64));
                    }
                }
                if let Some(u) = val.as_u64() {
                    // u64 that didn't fit in i64 path (large); negate to i64 if fits else float
                    if u <= i64::MAX as u64 {
                        return Value::from(-(u as i64));
                    } else {
                        return serde_json::json!(-(u as f64));
                    }
                }
                if let Some(f) = val.as_f64() {
                    return serde_json::json!(-f);
                }
                Value::Null
            }
        }
    }

    fn eval_binary_op(left: &Value, op: &BinaryOperator, right: &Value) -> Value {
        match op {
            BinaryOperator::And => {
                let lb = left.as_bool().unwrap_or(false);
                let rb = right.as_bool().unwrap_or(false);
                Value::Bool(lb && rb)
            }
            BinaryOperator::Or => {
                let lb = left.as_bool().unwrap_or(false);
                let rb = right.as_bool().unwrap_or(false);
                Value::Bool(lb || rb)
            }
            BinaryOperator::Eq => Value::Bool(Self::values_equal(left, right)),
            BinaryOperator::Neq => Value::Bool(!Self::values_equal(left, right)),
            BinaryOperator::Lt => {
                Self::eval_ordering(left, right, |o| o == std::cmp::Ordering::Less)
            }
            BinaryOperator::Lte => Self::eval_ordering(left, right, |o| {
                o == std::cmp::Ordering::Less || o == std::cmp::Ordering::Equal
            }),
            BinaryOperator::Gt => {
                Self::eval_ordering(left, right, |o| o == std::cmp::Ordering::Greater)
            }
            BinaryOperator::Gte => Self::eval_ordering(left, right, |o| {
                o == std::cmp::Ordering::Greater || o == std::cmp::Ordering::Equal
            }),
            BinaryOperator::Add => Self::eval_arith(left, right, ArithOp::Add),
            BinaryOperator::Sub => Self::eval_arith(left, right, ArithOp::Sub),
            BinaryOperator::Mul => Self::eval_arith(left, right, ArithOp::Mul),
            BinaryOperator::Div => Self::eval_arith(left, right, ArithOp::Div),
            BinaryOperator::Mod => Self::eval_arith(left, right, ArithOp::Mod),
            BinaryOperator::Like => Self::eval_like(left, right),
        }
    }

    /// Numeric-aware equality: 1 == 1.0 is true; otherwise strict Value equality.
    fn values_equal(a: &Value, b: &Value) -> bool {
        if a.is_null() || b.is_null() {
            // In SQL, NULL = NULL is UNKNOWN (not true). For WHERE filtering,
            // UNKNOWN is treated as false. But for direct Eq evaluation, follow
            // eKuiper-ish simple semantics: NULL == NULL -> false? Or true?
            // Most engines: NULL = NULL is NULL (false in WHERE).
            // Return false when either is NULL to keep WHERE correct.
            // However, for IN list with NULL? NULL IN (...) should be false.
            // So return false if either NULL.
            return false;
        }
        if a.is_number() && b.is_number() {
            // Compare as f64; handles int/float mixing.
            // For large ints beyond 2^53, f64 loses precision, so also try exact i64/u64 compare first.
            if let (Some(ai), Some(bi)) = (a.as_i64(), b.as_i64()) {
                return ai == bi;
            }
            if let (Some(au), Some(bu)) = (a.as_u64(), b.as_u64()) {
                return au == bu;
            }
            // Mixed int/uint/float: compare as f64
            if let (Some(af), Some(bf)) = (a.as_f64(), b.as_f64()) {
                return af == bf;
            }
            return false;
        }
        a == b
    }

    fn compare_values(a: &Value, b: &Value) -> Option<std::cmp::Ordering> {
        if a.is_number() && b.is_number() {
            let af = a.as_f64()?;
            let bf = b.as_f64()?;
            return af.partial_cmp(&bf);
        }
        if let (Some(as_), Some(bs)) = (a.as_str(), b.as_str()) {
            return Some(as_.cmp(bs));
        }
        // Booleans? false < true
        if let (Some(ab), Some(bb)) = (a.as_bool(), b.as_bool()) {
            return Some(ab.cmp(&bb));
        }
        None
    }

    fn eval_ordering<F>(left: &Value, right: &Value, pred: F) -> Value
    where
        F: Fn(std::cmp::Ordering) -> bool,
    {
        if left.is_null() || right.is_null() {
            return Value::Bool(false);
        }
        match Self::compare_values(left, right) {
            Some(ord) => Value::Bool(pred(ord)),
            None => Value::Bool(false),
        }
    }

    fn eval_between(val: &Value, low: &Value, high: &Value, negated: bool) -> Value {
        if val.is_null() || low.is_null() || high.is_null() {
            // SQL: NULL BETWEEN ... is UNKNOWN -> false in WHERE.
            // For negated (NOT BETWEEN), NULL is still UNKNOWN -> false.
            return Value::Bool(false);
        }
        let in_range = match (
            Self::compare_values(val, low),
            Self::compare_values(val, high),
        ) {
            (Some(o1), Some(o2)) => {
                (o1 == std::cmp::Ordering::Greater || o1 == std::cmp::Ordering::Equal)
                    && (o2 == std::cmp::Ordering::Less || o2 == std::cmp::Ordering::Equal)
            }
            _ => false,
        };
        if negated {
            Value::Bool(!in_range)
        } else {
            Value::Bool(in_range)
        }
    }

    fn eval_like(left: &Value, right: &Value) -> Value {
        let (Some(s), Some(p)) = (left.as_str(), right.as_str()) else {
            return Value::Bool(false);
        };
        Value::Bool(Self::like_match(s, p))
    }

    /// SQL LIKE matching: % matches any sequence (including empty),
    /// _ matches exactly one char, \ escapes next char to literal.
    fn like_match(s: &str, pattern: &str) -> bool {
        // Convert LIKE pattern to regex, escaping regex meta chars except % and _.
        let mut re = String::with_capacity(pattern.len() * 2 + 2);
        re.push('^');
        let mut chars = pattern.chars();
        while let Some(c) = chars.next() {
            match c {
                '\\' => {
                    // Escape next char as literal, if any; else literal backslash
                    if let Some(n) = chars.next() {
                        re.push_str(&regex::escape(&n.to_string()));
                    } else {
                        re.push_str(&regex::escape("\\"));
                    }
                }
                '%' => re.push_str(".*"),
                '_' => re.push('.'),
                _ => re.push_str(&regex::escape(&c.to_string())),
            }
        }
        re.push('$');
        match regex::Regex::new(&re) {
            Ok(r) => r.is_match(s),
            Err(_) => false,
        }
    }

    fn eval_arith(left: &Value, right: &Value, op: ArithOp) -> Value {
        if left.is_null() || right.is_null() {
            return Value::Null;
        }
        // Fast path: both i64 -> preserve integer (except Div which returns float)
        if let (Some(l), Some(r)) = (left.as_i64(), right.as_i64()) {
            // Ensure both were truly i64 (not float truncated). as_i64 returns Some only for
            // integer JSON numbers, so safe.
            match op {
                ArithOp::Add => {
                    return match l.checked_add(r) {
                        Some(v) => Value::from(v),
                        None => Value::Null,
                    }
                }
                ArithOp::Sub => {
                    return match l.checked_sub(r) {
                        Some(v) => Value::from(v),
                        None => Value::Null,
                    }
                }
                ArithOp::Mul => {
                    return match l.checked_mul(r) {
                        Some(v) => Value::from(v),
                        None => Value::Null,
                    }
                }
                ArithOp::Mod => {
                    if r == 0 {
                        return Value::Null;
                    }
                    return Value::from(l % r);
                }
                ArithOp::Div => {
                    if r == 0 {
                        return Value::Null;
                    }
                    // SQL division returns float
                    let res = (l as f64) / (r as f64);
                    return serde_json::json!(res);
                }
            }
        }
        // General numeric path: convert both to f64
        if let (Some(l), Some(r)) = (left.as_f64(), right.as_f64()) {
            let res = match op {
                ArithOp::Add => l + r,
                ArithOp::Sub => l - r,
                ArithOp::Mul => l * r,
                ArithOp::Div => {
                    if r == 0.0 {
                        return Value::Null;
                    }
                    l / r
                }
                ArithOp::Mod => {
                    if r == 0.0 {
                        return Value::Null;
                    }
                    l % r
                }
            };
            return serde_json::json!(res);
        }
        // Non-numeric operands
        Value::Null
    }

    fn eval_call(name: &str, args: &[Value]) -> Value {
        match name.to_ascii_lowercase().as_str() {
            // ---- Math ----
            "abs" => Self::func_abs(args),
            "ceil" | "ceiling" => Self::func_ceil(args),
            "floor" => Self::func_floor(args),
            "round" => Self::func_round(args),
            "sqrt" => Self::func_sqrt(args),
            "power" | "pow" => Self::func_power(args),
            // ---- Math & trig ----
            "sin" => Self::func_sin(args),
            "cos" => Self::func_cos(args),
            "tan" => Self::func_tan(args),
            "asin" => Self::func_asin(args),
            "acos" => Self::func_acos(args),
            "atan" => Self::func_atan(args),
            "atan2" => Self::func_atan2(args),
            "exp" => Self::func_exp(args),
            "ln" => Self::func_ln(args),
            "log" => Self::func_log(args),
            "log2" => Self::func_log2(args),
            "log10" => Self::func_log10(args),
            "sign" => Self::func_sign(args),
            "mod" => Self::func_mod(args),
            "cosh" => Self::func_cosh(args),
            "sinh" => Self::func_sinh(args),
            "tanh" => Self::func_tanh(args),
            "cot" => Self::func_cot(args),
            "radians" => Self::func_radians(args),
            "degrees" => Self::func_degrees(args),
            "bitand" => Self::func_bitand(args),
            "bitor" => Self::func_bitor(args),
            "bitxor" => Self::func_bitxor(args),
            "bitnot" => Self::func_bitnot(args),
            "pi" => Self::func_pi(args),
            "rand" => Self::func_rand(args),
            "conv" => Self::func_conv(args),
            // ---- String ----
            "concat" => Self::func_concat(args),
            "lower" => Self::func_lower(args),
            "upper" => Self::func_upper(args),
            "length" => Self::func_length(args),
            "trim" => Self::func_trim(args),
            "ltrim" => Self::func_ltrim(args),
            "rtrim" => Self::func_rtrim(args),
            "lpad" => Self::func_lpad(args),
            "rpad" => Self::func_rpad(args),
            "replace" => Self::func_replace(args),
            "split" => Self::func_split(args),
            "reverse" => Self::func_reverse(args),
            "substr" | "substring" => Self::func_substr(args),
            "startswith" => Self::func_startswith(args),
            "endswith" => Self::func_endswith(args),
            // ---- Array & object ----
            "array_contains" => Self::func_array_contains(args),
            "array_join" => Self::func_array_join(args),
            "keys" => Self::func_keys(args),
            "values" => Self::func_values(args),
            // ---- Conversion & utility ----
            "cast" => Self::func_cast(args),
            "coalesce" => Self::func_coalesce(args),
            // ---- Validation & utility ----
            "isnan" => Self::func_isnan(args),
            "isnumeric" => Self::func_isnumeric(args),
            "nvl" => Self::func_coalesce(args),
            // ---- Object construction ----
            "object_construct" => Self::func_object_construct(args),
            // ---- DateTime ----
            "now" => Self::func_now(args),
            "format_date" => Self::func_format_date(args),
            "date_parse" => Self::func_date_parse(args),
            "date_add" => Self::func_date_add(args),
            "date_diff" => Self::func_date_diff(args),
            "year" => Self::func_year(args),
            "month" => Self::func_month(args),
            "day" => Self::func_day(args),
            "hour" => Self::func_hour(args),
            "minute" => Self::func_minute(args),
            "second" => Self::func_second(args),
            // ---- JSON path ----
            "json_path_query" => Self::func_json_path_query(args),
            "json_path_query_first" => Self::func_json_path_query_first(args),
            "json_path_exists" => Self::func_json_path_exists(args),
            "json_map" => Self::func_json_map(args),
            // ---- Crypto & encoding ----
            "md5" => Self::func_md5(args),
            "sha256" => Self::func_sha256(args),
            "sha512" => Self::func_sha512(args),
            "encode" => Self::func_encode(args),
            "base64_encode" => Self::func_base64_encode(args),
            "decode" => Self::func_decode(args),
            "base64_decode" => Self::func_base64_decode(args),
            // ---- Extended array ----
            "array_create" => Self::func_array_create(args),
            "array_position" => Self::func_array_position(args),
            "array_length" => Self::func_array_length(args),
            "array_slice" => Self::func_array_slice(args),
            "array_concat" => Self::func_array_concat(args),
            "deduplicate" => Self::func_deduplicate(args),
            // ---- Analytic scalar fallbacks (batch/stateful paths below) ----
            "collect" => Self::func_collect_scalar(args),
            "lead" => Self::func_lead_scalar(args),
            "latest" => Self::func_latest_scalar(args),
            "had_changed" => Self::func_had_changed_scalar(args),
            "changed_col" => Self::func_changed_col_scalar(args),
            // ---- System & metadata ----
            "isnull" => Value::Bool(match args.first() {
                Some(v) => v.is_null(),
                None => true,
            }),
            "tstamp" => serde_json::json!(chrono::Utc::now().timestamp_millis()),
            "uuid" | "newuuid" => Self::func_uuid(args),
            // Contextual functions without record context: static defaults.
            // (With a record in scope, `eval_context_call` resolves them.)
            "window_start" | "window_end" => Value::Null,
            "rule_id" => Value::String(String::new()),
            "meta" | "mqtt" => Value::Null,
            "event_time" => serde_json::json!(chrono::Utc::now().timestamp_millis()),
            // ---- Registered UDFs (anything not built in) ----
            _ => Self::call_global_udf(name, args),
        }
    }

    /// RFC 4122 UUID v4 as a hyphenated lowercase string.
    fn func_uuid(_args: &[Value]) -> Value {
        Value::String(uuid::Uuid::new_v4().to_string())
    }

    /// Resolve a contextual system function against the current record.
    /// Returns `None` for non-contextual names so callers fall through to
    /// normal argument evaluation.
    fn eval_context_call(
        name: &str,
        args: &[Expr],
        record: &HashMap<String, Value>,
    ) -> Option<Value> {
        match name.to_ascii_lowercase().as_str() {
            "meta" => Some(Self::resolve_meta(
                args.first(),
                record,
                &["__meta__", "meta"],
            )),
            "mqtt" => Some(Self::resolve_meta(
                args.first(),
                record,
                &["__mqtt__", "mqtt"],
            )),
            "event_time" => Some(Self::resolve_event_time(record)),
            "rule_id" => Some(Self::resolve_rule_id(record)),
            "window_start" => Some(Self::resolve_window_bound(
                record,
                &["window_start", "__window_start__"],
            )),
            "window_end" => Some(Self::resolve_window_bound(
                record,
                &["window_end", "__window_end__"],
            )),
            _ => None,
        }
    }

    /// Look a metadata key up in the first present meta object, falling back
    /// to a top-level record field. No argument returns the whole object.
    fn resolve_meta(
        arg: Option<&Expr>,
        record: &HashMap<String, Value>,
        object_keys: &[&str],
    ) -> Value {
        let meta_obj = object_keys
            .iter()
            .find_map(|k| record.get(*k))
            .and_then(|v| v.as_object());
        let Some(arg) = arg else {
            return meta_obj
                .map(|m| Value::Object(m.clone()))
                .unwrap_or(Value::Null);
        };
        // Unquoted identifiers name the key directly; anything else is
        // evaluated first and must yield a string.
        let key = match arg {
            Expr::Identifier(name) => Some(name.clone()),
            other => match Self::eval_val(other, record) {
                Value::String(s) => Some(s),
                _ => None,
            },
        };
        let Some(key) = key else {
            return Value::Null;
        };
        if let Some(value) = meta_obj.and_then(|m| m.get(&key)) {
            return value.clone();
        }
        record.get(&key).cloned().unwrap_or(Value::Null)
    }

    fn resolve_event_time(record: &HashMap<String, Value>) -> Value {
        for key in ["timestamp", "event_time", "__timestamp__"] {
            if let Some(v) = record.get(key) {
                if !v.is_null() {
                    return v.clone();
                }
            }
        }
        serde_json::json!(chrono::Utc::now().timestamp_millis())
    }

    fn resolve_rule_id(record: &HashMap<String, Value>) -> Value {
        match record.get("__rule_id__") {
            Some(Value::String(s)) => Value::String(s.clone()),
            _ => Value::String(String::new()),
        }
    }

    fn resolve_window_bound(record: &HashMap<String, Value>, keys: &[&str]) -> Value {
        keys.iter()
            .find_map(|k| record.get(*k))
            .cloned()
            .unwrap_or(Value::Null)
    }

    /// Fall back to a user-registered UDF for names outside the built-in
    /// match list; `Value::Null` when nothing is registered under the name.
    fn call_global_udf(name: &str, args: &[Value]) -> Value {
        rekuiper_core::plugin::get_global_udf_registry()
            .call_udf(name, args)
            .unwrap_or(Value::Null)
    }

    // ---------- helpers for function args ----------

    /// Convert a JSON value to f64 if numeric or numeric-string.
    fn to_f64(v: &Value) -> Option<f64> {
        if let Some(f) = v.as_f64() {
            return Some(f);
        }
        if let Some(s) = v.as_str() {
            let t = s.trim();
            if let Ok(f) = t.parse::<f64>() {
                return Some(f);
            }
        }
        None
    }

    fn to_i64_arg(v: &Value) -> Option<i64> {
        if let Some(i) = v.as_i64() {
            return Some(i);
        }
        if let Some(u) = v.as_u64() {
            if u <= i64::MAX as u64 {
                return Some(u as i64);
            } else {
                return None;
            }
        }
        if let Some(f) = v.as_f64() {
            // Truncate toward zero like eKuiper cast
            if f.is_finite() {
                return Some(f.trunc() as i64);
            }
            return None;
        }
        if let Some(s) = v.as_str() {
            let t = s.trim();
            if let Ok(i) = t.parse::<i64>() {
                return Some(i);
            }
            if let Ok(f) = t.parse::<f64>() {
                if f.is_finite() {
                    return Some(f.trunc() as i64);
                }
            }
        }
        if let Some(b) = v.as_bool() {
            return Some(if b { 1 } else { 0 });
        }
        None
    }

    /// eKuiper `ToStringAlways`: nil -> "", otherwise minimal string form.
    fn to_string_always(v: &Value) -> String {
        match v {
            Value::Null => String::new(),
            Value::String(s) => s.clone(),
            Value::Number(n) => n.to_string(),
            Value::Bool(b) => b.to_string(),
            Value::Array(_) | Value::Object(_) => serde_json::to_string(v).unwrap_or_default(),
        }
    }

    // ---------- math functions ----------

    fn func_abs(args: &[Value]) -> Value {
        if args.len() != 1 {
            return Value::Null;
        }
        let v = &args[0];
        if v.is_null() {
            return Value::Null;
        }
        if let Some(i) = v.as_i64() {
            if let Some(n) = i.checked_abs() {
                return Value::from(n);
            }
            return serde_json::json!((i as f64).abs());
        }
        if let Some(u) = v.as_u64() {
            // u64 is already non-negative
            return Value::from(u);
        }
        if let Some(f) = v.as_f64() {
            return serde_json::json!(f.abs());
        }
        if let Some(s) = v.as_str() {
            let t = s.trim();
            if let Ok(i) = t.parse::<i64>() {
                if let Some(n) = i.checked_abs() {
                    return Value::from(n);
                }
                return serde_json::json!((i as f64).abs());
            }
            if let Ok(f) = t.parse::<f64>() {
                return serde_json::json!(f.abs());
            }
        }
        Value::Null
    }

    fn func_ceil(args: &[Value]) -> Value {
        if args.len() != 1 {
            return Value::Null;
        }
        if args[0].is_null() {
            return Value::Null;
        }
        match Self::to_f64(&args[0]) {
            Some(f) => serde_json::json!(f.ceil()),
            None => Value::Null,
        }
    }

    fn func_floor(args: &[Value]) -> Value {
        if args.len() != 1 {
            return Value::Null;
        }
        if args[0].is_null() {
            return Value::Null;
        }
        match Self::to_f64(&args[0]) {
            Some(f) => serde_json::json!(f.floor()),
            None => Value::Null,
        }
    }

    fn func_round(args: &[Value]) -> Value {
        if args.is_empty() || args.len() > 2 {
            return Value::Null;
        }
        if args.iter().any(|v| v.is_null()) {
            return Value::Null;
        }
        let v = match Self::to_f64(&args[0]) {
            Some(f) => f,
            None => return Value::Null,
        };
        let precision: i32 = if args.len() == 2 {
            match Self::to_i64_arg(&args[1]) {
                Some(p) => p as i32,
                None => return Value::Null,
            }
        } else {
            0
        };
        let factor = 10f64.powi(precision);
        if !factor.is_finite() {
            return Value::Null;
        }
        let scaled = v * factor;
        if !scaled.is_finite() {
            return Value::Null;
        }
        serde_json::json!(scaled.round() / factor)
    }

    fn func_sqrt(args: &[Value]) -> Value {
        if args.len() != 1 {
            return Value::Null;
        }
        if args[0].is_null() {
            return Value::Null;
        }
        match Self::to_f64(&args[0]) {
            Some(f) => {
                if f < 0.0 {
                    return Value::Null;
                }
                let r = f.sqrt();
                if r.is_nan() {
                    return Value::Null;
                }
                serde_json::json!(r)
            }
            None => Value::Null,
        }
    }

    fn func_power(args: &[Value]) -> Value {
        if args.len() != 2 {
            return Value::Null;
        }
        if args.iter().any(|v| v.is_null()) {
            return Value::Null;
        }
        // Integer fast path: exact results for non-negative exponents.
        if let (Some(base), Some(exp)) = (args[0].as_i64(), args[1].as_i64()) {
            if exp >= 0 {
                return match u32::try_from(exp).ok().and_then(|e| base.checked_pow(e)) {
                    Some(n) => Value::from(n),
                    None => Value::Null,
                };
            }
        }
        let (Some(x), Some(y)) = (Self::to_f64(&args[0]), Self::to_f64(&args[1])) else {
            return Value::Null;
        };
        let r = x.powf(y);
        if r.is_nan() || r.is_infinite() {
            return Value::Null;
        }
        serde_json::json!(r)
    }

    // ---------- string functions ----------

    fn func_concat(args: &[Value]) -> Value {
        if args.is_empty() {
            return Value::Null;
        }
        let mut out = String::new();
        for a in args {
            out.push_str(&Self::to_string_always(a));
        }
        Value::String(out)
    }

    fn func_lower(args: &[Value]) -> Value {
        if args.len() != 1 {
            return Value::Null;
        }
        if args[0].is_null() {
            return Value::Null;
        }
        Value::String(Self::to_string_always(&args[0]).to_lowercase())
    }

    fn func_upper(args: &[Value]) -> Value {
        if args.len() != 1 {
            return Value::Null;
        }
        if args[0].is_null() {
            return Value::Null;
        }
        Value::String(Self::to_string_always(&args[0]).to_uppercase())
    }

    fn func_length(args: &[Value]) -> Value {
        if args.len() != 1 {
            return Value::Null;
        }
        match &args[0] {
            Value::Null => Value::from(0),
            Value::Array(a) => Value::from(a.len() as i64),
            Value::Object(m) => Value::from(m.len() as i64),
            Value::String(s) => Value::from(s.chars().count() as i64),
            other => Value::from(Self::to_string_always(other).chars().count() as i64),
        }
    }

    fn func_trim(args: &[Value]) -> Value {
        if args.len() != 1 {
            return Value::Null;
        }
        if args[0].is_null() {
            return Value::Null;
        }
        Value::String(Self::to_string_always(&args[0]).trim().to_string())
    }

    /// 1-indexed substring per SQL/eKuiper standard: substr(s, start, [len]).
    /// start=1 is the first character. Uses char (not byte) indexing.
    fn func_substr(args: &[Value]) -> Value {
        if args.len() != 2 && args.len() != 3 {
            return Value::Null;
        }
        if args.iter().any(|v| v.is_null()) {
            return Value::Null;
        }
        let s = Self::to_string_always(&args[0]);
        let chars: Vec<char> = s.chars().collect();
        let n = chars.len() as i64;
        let start = match Self::to_i64_arg(&args[1]) {
            Some(v) => v,
            None => return Value::Null,
        };
        // 1-indexed -> 0-indexed; clamp start<=0 to first char (MySQL-like).
        let mut start_idx: i64 = if start <= 0 { 0 } else { start - 1 };
        if start_idx >= n {
            return Value::String(String::new());
        }
        if start_idx < 0 {
            start_idx = 0;
        }
        let start_usize = start_idx as usize;
        if args.len() == 3 {
            let len = match Self::to_i64_arg(&args[2]) {
                Some(v) => v,
                None => return Value::Null,
            };
            if len < 0 {
                return Value::Null;
            }
            if len == 0 {
                return Value::String(String::new());
            }
            let end = ((start_usize as i64) + len).min(n) as usize;
            Value::String(chars[start_usize..end].iter().collect())
        } else {
            Value::String(chars[start_usize..].iter().collect())
        }
    }

    fn func_startswith(args: &[Value]) -> Value {
        if args.len() != 2 {
            return Value::Null;
        }
        if args.iter().any(|v| v.is_null()) {
            return Value::Bool(false);
        }
        let s = Self::to_string_always(&args[0]);
        let prefix = Self::to_string_always(&args[1]);
        Value::Bool(s.starts_with(prefix.as_str()))
    }

    fn func_endswith(args: &[Value]) -> Value {
        if args.len() != 2 {
            return Value::Null;
        }
        if args.iter().any(|v| v.is_null()) {
            return Value::Bool(false);
        }
        let s = Self::to_string_always(&args[0]);
        let suffix = Self::to_string_always(&args[1]);
        Value::Bool(s.ends_with(suffix.as_str()))
    }

    // ---------- conversion & utility ----------

    fn func_cast(args: &[Value]) -> Value {
        if args.len() != 2 {
            return Value::Null;
        }
        if args.iter().any(|v| v.is_null()) {
            return Value::Null;
        }
        let Some(target) = args[1].as_str() else {
            return Value::Null;
        };
        match target.trim().to_ascii_lowercase().as_str() {
            "bigint" | "int" => Self::cast_to_bigint(&args[0]),
            "float" | "double" => Self::cast_to_float(&args[0]),
            "string" => Self::cast_to_string(&args[0]),
            "boolean" | "bool" => Self::cast_to_bool(&args[0]),
            _ => Value::Null,
        }
    }

    fn cast_to_bigint(v: &Value) -> Value {
        if v.is_null() {
            return Value::Null;
        }
        if let Some(i) = v.as_i64() {
            return Value::from(i);
        }
        if let Some(u) = v.as_u64() {
            if u <= i64::MAX as u64 {
                return Value::from(u as i64);
            }
            return Value::Null;
        }
        if let Some(f) = v.as_f64() {
            if f.is_finite() {
                return Value::from(f.trunc() as i64);
            }
            return Value::Null;
        }
        if let Some(b) = v.as_bool() {
            return Value::from(if b { 1 } else { 0 });
        }
        if let Some(s) = v.as_str() {
            let t = s.trim();
            if let Ok(i) = t.parse::<i64>() {
                return Value::from(i);
            }
            if let Ok(f) = t.parse::<f64>() {
                if f.is_finite() {
                    return Value::from(f.trunc() as i64);
                }
            }
        }
        Value::Null
    }

    fn cast_to_float(v: &Value) -> Value {
        if v.is_null() {
            return Value::Null;
        }
        if let Some(f) = v.as_f64() {
            return serde_json::json!(f);
        }
        if let Some(b) = v.as_bool() {
            return serde_json::json!(if b { 1.0 } else { 0.0 });
        }
        if let Some(s) = v.as_str() {
            if let Ok(f) = s.trim().parse::<f64>() {
                return serde_json::json!(f);
            }
        }
        Value::Null
    }

    fn cast_to_string(v: &Value) -> Value {
        if v.is_null() {
            return Value::Null;
        }
        match v {
            Value::String(s) => Value::String(s.clone()),
            Value::Number(n) => Value::String(n.to_string()),
            Value::Bool(b) => Value::String(b.to_string()),
            Value::Array(_) | Value::Object(_) => match serde_json::to_string(v) {
                Ok(s) => Value::String(s),
                Err(_) => Value::Null,
            },
            Value::Null => Value::Null,
        }
    }

    fn cast_to_bool(v: &Value) -> Value {
        if v.is_null() {
            return Value::Null;
        }
        if let Some(b) = v.as_bool() {
            return Value::Bool(b);
        }
        if v.is_number() {
            if let Some(f) = v.as_f64() {
                return Value::Bool(f != 0.0);
            }
            return Value::Null;
        }
        if let Some(s) = v.as_str() {
            // Mirror Go strconv.ParseBool: 1,t,T,TRUE,true,True,0,f,F,FALSE,false,False
            match s.trim().to_ascii_lowercase().as_str() {
                "1" | "t" | "true" => return Value::Bool(true),
                "0" | "f" | "false" => return Value::Bool(false),
                _ => return Value::Null,
            }
        }
        Value::Null
    }

    fn func_coalesce(args: &[Value]) -> Value {
        if args.is_empty() {
            return Value::Null;
        }
        for a in args {
            if !a.is_null() {
                return a.clone();
            }
        }
        Value::Null
    }

    // ---------- extended math & trig functions ----------

    /// Single-arg trig-style helper: numeric input via [`Self::to_f64`],
    /// `Null` for null/non-numeric input or NaN/infinite results.
    fn func_float1(args: &[Value], f: fn(f64) -> f64) -> Value {
        if args.len() != 1 || args[0].is_null() {
            return Value::Null;
        }
        match Self::to_f64(&args[0]) {
            Some(v) => {
                let r = f(v);
                if r.is_nan() || r.is_infinite() {
                    return Value::Null;
                }
                serde_json::json!(r)
            }
            None => Value::Null,
        }
    }

    fn func_sin(args: &[Value]) -> Value {
        Self::func_float1(args, f64::sin)
    }

    fn func_cos(args: &[Value]) -> Value {
        Self::func_float1(args, f64::cos)
    }

    fn func_tan(args: &[Value]) -> Value {
        Self::func_float1(args, f64::tan)
    }

    fn func_asin(args: &[Value]) -> Value {
        Self::func_float1(args, f64::asin)
    }

    fn func_acos(args: &[Value]) -> Value {
        Self::func_float1(args, f64::acos)
    }

    fn func_atan(args: &[Value]) -> Value {
        Self::func_float1(args, f64::atan)
    }

    fn func_atan2(args: &[Value]) -> Value {
        if args.len() != 2 || args.iter().any(|v| v.is_null()) {
            return Value::Null;
        }
        match (Self::to_f64(&args[0]), Self::to_f64(&args[1])) {
            (Some(y), Some(x)) => {
                let r = y.atan2(x);
                if r.is_nan() || r.is_infinite() {
                    return Value::Null;
                }
                serde_json::json!(r)
            }
            _ => Value::Null,
        }
    }

    fn func_exp(args: &[Value]) -> Value {
        Self::func_float1(args, f64::exp)
    }

    fn func_ln(args: &[Value]) -> Value {
        if args.len() != 1 || args[0].is_null() {
            return Value::Null;
        }
        match Self::to_f64(&args[0]) {
            Some(v) if v > 0.0 => {
                let r = v.ln();
                if r.is_nan() || r.is_infinite() {
                    return Value::Null;
                }
                serde_json::json!(r)
            }
            _ => Value::Null,
        }
    }

    fn func_log10(args: &[Value]) -> Value {
        if args.len() != 1 || args[0].is_null() {
            return Value::Null;
        }
        match Self::to_f64(&args[0]) {
            Some(v) if v > 0.0 => {
                let r = v.log10();
                if r.is_nan() || r.is_infinite() {
                    return Value::Null;
                }
                serde_json::json!(r)
            }
            _ => Value::Null,
        }
    }

    /// `log(x)` is the natural logarithm; `log(base, x)` computes `x` in the
    /// given base. Non-positive `x` (and non-finite results) yield `Null`.
    fn func_log(args: &[Value]) -> Value {
        if args.iter().any(|v| v.is_null()) {
            return Value::Null;
        }
        match args.len() {
            1 => match Self::to_f64(&args[0]) {
                Some(v) if v > 0.0 => {
                    let r = v.ln();
                    if r.is_nan() || r.is_infinite() {
                        return Value::Null;
                    }
                    serde_json::json!(r)
                }
                _ => Value::Null,
            },
            2 => match (Self::to_f64(&args[0]), Self::to_f64(&args[1])) {
                (Some(base), Some(x)) if x > 0.0 => {
                    let r = x.log(base);
                    if r.is_nan() || r.is_infinite() {
                        return Value::Null;
                    }
                    serde_json::json!(r)
                }
                _ => Value::Null,
            },
            _ => Value::Null,
        }
    }

    fn func_log2(args: &[Value]) -> Value {
        if args.len() != 1 || args[0].is_null() {
            return Value::Null;
        }
        match Self::to_f64(&args[0]) {
            Some(v) if v > 0.0 => {
                let r = v.log2();
                if r.is_nan() || r.is_infinite() {
                    return Value::Null;
                }
                serde_json::json!(r)
            }
            _ => Value::Null,
        }
    }

    fn func_cosh(args: &[Value]) -> Value {
        Self::func_float1(args, f64::cosh)
    }

    fn func_sinh(args: &[Value]) -> Value {
        Self::func_float1(args, f64::sinh)
    }

    fn func_tanh(args: &[Value]) -> Value {
        Self::func_float1(args, f64::tanh)
    }

    fn func_cot(args: &[Value]) -> Value {
        if args.len() != 1 || args[0].is_null() {
            return Value::Null;
        }
        match Self::to_f64(&args[0]) {
            Some(v) => {
                let t = v.tan();
                if t == 0.0 {
                    return Value::Null;
                }
                let r = 1.0 / t;
                if r.is_nan() || r.is_infinite() {
                    return Value::Null;
                }
                serde_json::json!(r)
            }
            None => Value::Null,
        }
    }

    fn func_radians(args: &[Value]) -> Value {
        if args.len() != 1 || args[0].is_null() {
            return Value::Null;
        }
        match Self::to_f64(&args[0]) {
            Some(v) if v.is_finite() => serde_json::json!(v.to_radians()),
            _ => Value::Null,
        }
    }

    fn func_degrees(args: &[Value]) -> Value {
        if args.len() != 1 || args[0].is_null() {
            return Value::Null;
        }
        match Self::to_f64(&args[0]) {
            Some(v) if v.is_finite() => serde_json::json!(v.to_degrees()),
            _ => Value::Null,
        }
    }

    fn func_bitand(args: &[Value]) -> Value {
        if args.len() != 2 {
            return Value::Null;
        }
        match (Self::to_i64_arg(&args[0]), Self::to_i64_arg(&args[1])) {
            (Some(a), Some(b)) => Value::from(a & b),
            _ => Value::Null,
        }
    }

    fn func_bitor(args: &[Value]) -> Value {
        if args.len() != 2 {
            return Value::Null;
        }
        match (Self::to_i64_arg(&args[0]), Self::to_i64_arg(&args[1])) {
            (Some(a), Some(b)) => Value::from(a | b),
            _ => Value::Null,
        }
    }

    fn func_bitxor(args: &[Value]) -> Value {
        if args.len() != 2 {
            return Value::Null;
        }
        match (Self::to_i64_arg(&args[0]), Self::to_i64_arg(&args[1])) {
            (Some(a), Some(b)) => Value::from(a ^ b),
            _ => Value::Null,
        }
    }

    fn func_bitnot(args: &[Value]) -> Value {
        if args.len() != 1 {
            return Value::Null;
        }
        match Self::to_i64_arg(&args[0]) {
            Some(a) => Value::from(!a),
            None => Value::Null,
        }
    }

    fn func_pi(args: &[Value]) -> Value {
        if !args.is_empty() {
            return Value::Null;
        }
        serde_json::json!(std::f64::consts::PI)
    }

    fn func_rand(args: &[Value]) -> Value {
        if !args.is_empty() {
            return Value::Null;
        }
        serde_json::json!(rand::random::<f64>())
    }

    /// `conv(num, from_base, to_base)`: radix conversion (2-36) rendering a
    /// lowercase string. `num` may be an integer or its string form.
    fn func_conv(args: &[Value]) -> Value {
        if args.len() != 3 {
            return Value::Null;
        }
        if args.iter().any(|v| v.is_null()) {
            return Value::Null;
        }
        let (Some(from_base), Some(to_base)) =
            (Self::to_i64_arg(&args[1]), Self::to_i64_arg(&args[2]))
        else {
            return Value::Null;
        };
        if !(2..=36).contains(&from_base) || !(2..=36).contains(&to_base) {
            return Value::Null;
        }
        let digits = match &args[0] {
            Value::String(s) => s.trim().to_string(),
            Value::Number(_) => match Self::to_i64_arg(&args[0]) {
                Some(n) => n.to_string(),
                None => return Value::Null,
            },
            _ => return Value::Null,
        };
        let value = match Self::parse_radix(&digits, from_base as u32) {
            Some(n) => n,
            None => return Value::Null,
        };
        Value::String(Self::format_radix(value, to_base as u32))
    }

    fn parse_radix(text: &str, base: u32) -> Option<i64> {
        let text = text.trim();
        let (negative, digits) = match text.strip_prefix('-') {
            Some(rest) => (true, rest),
            None => (false, text.strip_prefix('+').unwrap_or(text)),
        };
        if digits.is_empty() {
            return None;
        }
        let mut acc: i64 = 0;
        for c in digits.chars() {
            let d = c.to_digit(base)? as i64;
            acc = acc.checked_mul(base as i64)?.checked_add(d)?;
        }
        Some(if negative { acc.checked_neg()? } else { acc })
    }

    fn format_radix(value: i64, base: u32) -> String {
        if value == 0 {
            return "0".to_string();
        }
        let negative = value < 0;
        // Work in unsigned space so i64::MIN converts losslessly.
        let mut n = if negative {
            (value as u64).wrapping_neg()
        } else {
            value as u64
        };
        let mut out = Vec::new();
        while n > 0 {
            let d = (n % base as u64) as u32;
            out.push(char::from_digit(d, base).unwrap_or('?'));
            n /= base as u64;
        }
        if negative {
            out.push('-');
        }
        out.iter().rev().collect()
    }

    fn func_sign(args: &[Value]) -> Value {
        if args.len() != 1 || args[0].is_null() {
            return Value::Null;
        }
        if let Some(i) = args[0].as_i64() {
            return Value::from(i.signum());
        }
        if let Some(u) = args[0].as_u64() {
            // u64 values are non-negative; zero maps to 0.
            return Value::from(if u == 0 { 0 } else { 1 });
        }
        match Self::to_f64(&args[0]) {
            Some(f) if f > 0.0 => Value::from(1),
            Some(f) if f < 0.0 => Value::from(-1),
            Some(_) => Value::from(0),
            None => Value::Null,
        }
    }

    fn func_mod(args: &[Value]) -> Value {
        if args.len() != 2 {
            return Value::Null;
        }
        Self::eval_arith(&args[0], &args[1], ArithOp::Mod)
    }

    // ---------- extended string functions ----------

    fn func_ltrim(args: &[Value]) -> Value {
        if args.len() != 1 {
            return Value::Null;
        }
        if args[0].is_null() {
            return Value::Null;
        }
        Value::String(Self::to_string_always(&args[0]).trim_start().to_string())
    }

    fn func_rtrim(args: &[Value]) -> Value {
        if args.len() != 1 {
            return Value::Null;
        }
        if args[0].is_null() {
            return Value::Null;
        }
        Value::String(Self::to_string_always(&args[0]).trim_end().to_string())
    }

    fn pad_char(args: &[Value]) -> char {
        if args.len() >= 3 && !args[2].is_null() {
            let s = Self::to_string_always(&args[2]);
            if let Some(c) = s.chars().next() {
                return c;
            }
        }
        ' '
    }

    fn func_lpad(args: &[Value]) -> Value {
        if args.len() != 2 && args.len() != 3 {
            return Value::Null;
        }
        if args.iter().any(|v| v.is_null()) {
            return Value::Null;
        }
        let s = Self::to_string_always(&args[0]);
        let Some(total) = Self::to_i64_arg(&args[1]) else {
            return Value::Null;
        };
        if total < 0 {
            return Value::Null;
        }
        let total = total as usize;
        let len = s.chars().count();
        if len >= total {
            return Value::String(s);
        }
        let pad = Self::pad_char(args);
        let mut out = String::with_capacity(total);
        for _ in 0..(total - len) {
            out.push(pad);
        }
        out.push_str(&s);
        Value::String(out)
    }

    fn func_rpad(args: &[Value]) -> Value {
        if args.len() != 2 && args.len() != 3 {
            return Value::Null;
        }
        if args.iter().any(|v| v.is_null()) {
            return Value::Null;
        }
        let s = Self::to_string_always(&args[0]);
        let Some(total) = Self::to_i64_arg(&args[1]) else {
            return Value::Null;
        };
        if total < 0 {
            return Value::Null;
        }
        let total = total as usize;
        let len = s.chars().count();
        if len >= total {
            return Value::String(s);
        }
        let pad = Self::pad_char(args);
        let mut out = s;
        for _ in 0..(total - len) {
            out.push(pad);
        }
        Value::String(out)
    }

    fn func_replace(args: &[Value]) -> Value {
        if args.len() != 3 {
            return Value::Null;
        }
        if args.iter().any(|v| v.is_null()) {
            return Value::Null;
        }
        Value::String(Self::to_string_always(&args[0]).replace(
            &Self::to_string_always(&args[1]),
            &Self::to_string_always(&args[2]),
        ))
    }

    fn func_split(args: &[Value]) -> Value {
        if args.len() != 2 {
            return Value::Null;
        }
        if args.iter().any(|v| v.is_null()) {
            return Value::Null;
        }
        Value::Array(
            Self::to_string_always(&args[0])
                .split(&Self::to_string_always(&args[1]))
                .map(|part| Value::String(part.to_string()))
                .collect(),
        )
    }

    fn func_reverse(args: &[Value]) -> Value {
        if args.len() != 1 {
            return Value::Null;
        }
        if args[0].is_null() {
            return Value::Null;
        }
        Value::String(Self::to_string_always(&args[0]).chars().rev().collect())
    }

    // ---------- array & object functions ----------

    fn func_array_contains(args: &[Value]) -> Value {
        if args.len() != 2 {
            return Value::Null;
        }
        let Some(arr) = args[0].as_array() else {
            return Value::Bool(false);
        };
        Value::Bool(arr.iter().any(|item| Self::values_equal(item, &args[1])))
    }

    fn func_array_join(args: &[Value]) -> Value {
        if args.is_empty() || args.len() > 2 {
            return Value::Null;
        }
        let Some(arr) = args[0].as_array() else {
            return Value::Null;
        };
        let sep = if args.len() == 2 {
            if args[1].is_null() {
                return Value::Null;
            }
            Self::to_string_always(&args[1])
        } else {
            ",".to_string()
        };
        Value::String(
            arr.iter()
                .map(Self::to_string_always)
                .collect::<Vec<_>>()
                .join(&sep),
        )
    }

    fn func_keys(args: &[Value]) -> Value {
        if args.len() != 1 {
            return Value::Null;
        }
        let Some(obj) = args[0].as_object() else {
            return Value::Null;
        };
        Value::Array(obj.keys().map(|k| Value::String(k.clone())).collect())
    }

    fn func_values(args: &[Value]) -> Value {
        if args.len() != 1 {
            return Value::Null;
        }
        let Some(obj) = args[0].as_object() else {
            return Value::Null;
        };
        Value::Array(obj.values().cloned().collect())
    }

    /// Builds an object from alternating key/value arguments:
    /// `object_construct(k1, v1, k2, v2, ...)`. Keys are stringified via
    /// [`Self::to_string_always`]; an odd argument count yields `Null`.
    #[allow(clippy::manual_is_multiple_of)]
    fn func_object_construct(args: &[Value]) -> Value {
        if args.len() % 2 != 0 {
            return Value::Null;
        }
        let mut map = serde_json::Map::with_capacity(args.len() / 2);
        let mut it = args.iter();
        while let (Some(k), Some(v)) = (it.next(), it.next()) {
            map.insert(Self::to_string_always(k), v.clone());
        }
        Value::Object(map)
    }

    // ---------- validation functions ----------

    fn func_isnan(args: &[Value]) -> Value {
        if args.len() != 1 {
            return Value::Null;
        }
        Value::Bool(matches!(args[0].as_f64(), Some(f) if f.is_nan()))
    }

    fn func_isnumeric(args: &[Value]) -> Value {
        if args.len() != 1 {
            return Value::Null;
        }
        Value::Bool(Self::to_f64(&args[0]).is_some())
    }

    // ---------- datetime functions (all in UTC) ----------

    /// Resolve a value to epoch milliseconds: numbers directly, RFC3339
    /// strings via parsing, other strings when numerically parseable.
    fn to_epoch_millis(v: &Value) -> Option<i64> {
        if let Some(s) = v.as_str() {
            let t = s.trim();
            if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(t) {
                return Some(dt.timestamp_millis());
            }
        }
        Self::to_i64_arg(v)
    }

    fn datetime_from_millis(ms: i64) -> Option<chrono::DateTime<chrono::Utc>> {
        chrono::DateTime::from_timestamp_millis(ms)
    }

    fn func_now(args: &[Value]) -> Value {
        if !args.is_empty() {
            return Value::Null;
        }
        Value::from(chrono::Utc::now().timestamp_millis())
    }

    fn func_format_date(args: &[Value]) -> Value {
        if args.len() != 2 {
            return Value::Null;
        }
        let Some(fmt) = args[1].as_str() else {
            return Value::Null;
        };
        let dt = match &args[0] {
            Value::Number(_) => {
                Self::to_epoch_millis(&args[0]).and_then(Self::datetime_from_millis)
            }
            Value::String(_) => {
                if let Some(s) = args[0].as_str().and_then(|s| {
                    chrono::DateTime::parse_from_rfc3339(s.trim())
                        .ok()
                        .map(|dt| dt.with_timezone(&chrono::Utc))
                }) {
                    Some(s)
                } else {
                    Self::to_epoch_millis(&args[0]).and_then(Self::datetime_from_millis)
                }
            }
            _ => None,
        };
        match dt {
            Some(dt) => Value::String(dt.format(fmt).to_string()),
            None => Value::Null,
        }
    }

    fn func_date_parse(args: &[Value]) -> Value {
        if args.len() != 2 {
            return Value::Null;
        }
        let (Some(s), Some(fmt)) = (args[0].as_str(), args[1].as_str()) else {
            return Value::Null;
        };
        if let Ok(dt) = chrono::DateTime::parse_from_str(s, fmt) {
            return Value::from(dt.timestamp_millis());
        }
        if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(s, fmt) {
            return Value::from(dt.and_utc().timestamp_millis());
        }
        if let Ok(d) = chrono::NaiveDate::parse_from_str(s, fmt) {
            if let Some(dt) = d.and_hms_opt(0, 0, 0) {
                return Value::from(dt.and_utc().timestamp_millis());
            }
        }
        Value::Null
    }

    /// Milliseconds per interval unit: dd|day, hh|hour, mi|minute|min,
    /// ss|second|sec, ms|millisecond (case-insensitive).
    fn interval_unit_millis(part: &str) -> Option<i64> {
        match part.trim().to_ascii_lowercase().as_str() {
            "dd" | "day" => Some(86_400_000),
            "hh" | "hour" => Some(3_600_000),
            "mi" | "minute" | "min" => Some(60_000),
            "ss" | "second" | "sec" => Some(1_000),
            "ms" | "millisecond" => Some(1),
            _ => None,
        }
    }

    fn func_date_add(args: &[Value]) -> Value {
        if args.len() != 3 {
            return Value::Null;
        }
        let (Some(part), Some(num)) = (args[0].as_str(), Self::to_i64_arg(&args[1])) else {
            return Value::Null;
        };
        let (Some(unit), Some(ts)) = (
            Self::interval_unit_millis(part),
            Self::to_epoch_millis(&args[2]),
        ) else {
            return Value::Null;
        };
        match num.checked_mul(unit).and_then(|d| ts.checked_add(d)) {
            Some(ms) => Value::from(ms),
            None => Value::Null,
        }
    }

    fn func_date_diff(args: &[Value]) -> Value {
        if args.len() != 3 {
            return Value::Null;
        }
        let Some(part) = args[0].as_str() else {
            return Value::Null;
        };
        let (Some(unit), Some(t1), Some(t2)) = (
            Self::interval_unit_millis(part),
            Self::to_epoch_millis(&args[1]),
            Self::to_epoch_millis(&args[2]),
        ) else {
            return Value::Null;
        };
        match t2.checked_sub(t1) {
            // Integer division truncates toward zero, matching SQL semantics.
            Some(diff) => Value::from(diff / unit),
            None => Value::Null,
        }
    }

    fn datetime_component<F>(args: &[Value], extract: F) -> Value
    where
        F: Fn(chrono::DateTime<chrono::Utc>) -> i32,
    {
        if args.len() != 1 {
            return Value::Null;
        }
        match Self::to_epoch_millis(&args[0]).and_then(Self::datetime_from_millis) {
            Some(dt) => Value::from(extract(dt)),
            None => Value::Null,
        }
    }

    fn func_year(args: &[Value]) -> Value {
        Self::datetime_component(args, |dt| dt.year())
    }

    fn func_month(args: &[Value]) -> Value {
        Self::datetime_component(args, |dt| dt.month() as i32)
    }

    fn func_day(args: &[Value]) -> Value {
        Self::datetime_component(args, |dt| dt.day() as i32)
    }

    fn func_hour(args: &[Value]) -> Value {
        Self::datetime_component(args, |dt| dt.hour() as i32)
    }

    fn func_minute(args: &[Value]) -> Value {
        Self::datetime_component(args, |dt| dt.minute() as i32)
    }

    fn func_second(args: &[Value]) -> Value {
        Self::datetime_component(args, |dt| dt.second() as i32)
    }

    // ---------- JSON path functions ----------

    /// Split a dot-notation segment like `a[0][1]` into its field name and
    /// index list. Malformed brackets fall back to the literal segment.
    fn split_path_segment(seg: &str) -> (&str, Vec<usize>) {
        match seg.find('[') {
            None => (seg, Vec::new()),
            Some(pos) => {
                let (name, mut rest) = seg.split_at(pos);
                let mut indices = Vec::new();
                while let Some(inner) = rest.strip_prefix('[') {
                    match inner.find(']') {
                        Some(end) => match inner[..end].parse::<usize>() {
                            Ok(i) => {
                                indices.push(i);
                                rest = &inner[end + 1..];
                            }
                            Err(_) => return (seg, Vec::new()),
                        },
                        None => return (seg, Vec::new()),
                    }
                }
                if rest.is_empty() {
                    (name, indices)
                } else {
                    (seg, Vec::new())
                }
            }
        }
    }

    /// Resolve a JSON pointer (`/a/b/0`, RFC 6901) or dot-notation path
    /// (`a.b.c`, with optional `[n]` indices) against a value.
    fn json_resolve_path<'v>(val: &'v Value, path: &str) -> Option<&'v Value> {
        let path = path.trim();
        if path.is_empty() {
            return Some(val);
        }
        if path.starts_with('/') {
            let mut current = val;
            for token in path.split('/').skip(1) {
                let token = token.replace("~1", "/").replace("~0", "~");
                match current {
                    Value::Object(map) => current = map.get(&token)?,
                    Value::Array(arr) => current = arr.get(token.parse::<usize>().ok()?)?,
                    _ => return None,
                }
            }
            return Some(current);
        }
        let mut current = val;
        for seg in path.split('.') {
            let (name, indices) = Self::split_path_segment(seg);
            if !name.is_empty() {
                match current {
                    Value::Object(map) => current = map.get(name)?,
                    // A bare numeric segment also indexes arrays.
                    Value::Array(arr) => {
                        current = arr.get(name.parse::<usize>().ok()?)?;
                    }
                    _ => return None,
                }
            }
            for idx in indices {
                match current {
                    Value::Array(arr) => current = arr.get(idx)?,
                    _ => return None,
                }
            }
        }
        Some(current)
    }

    fn func_json_path_query(args: &[Value]) -> Value {
        if args.len() != 2 {
            return Value::Null;
        }
        let Some(path) = args[1].as_str() else {
            return Value::Null;
        };
        Self::json_resolve_path(&args[0], path)
            .cloned()
            .unwrap_or(Value::Null)
    }

    fn func_json_path_query_first(args: &[Value]) -> Value {
        if args.len() != 2 {
            return Value::Null;
        }
        let Some(path) = args[1].as_str() else {
            return Value::Null;
        };
        match Self::json_resolve_path(&args[0], path) {
            Some(Value::Array(arr)) => arr.first().cloned().unwrap_or(Value::Null),
            Some(v) => v.clone(),
            None => Value::Null,
        }
    }

    fn func_json_path_exists(args: &[Value]) -> Value {
        if args.len() != 2 {
            return Value::Null;
        }
        let Some(path) = args[1].as_str() else {
            return Value::Bool(false);
        };
        Value::Bool(Self::json_resolve_path(&args[0], path).is_some())
    }

    fn func_json_map(args: &[Value]) -> Value {
        if !args.len().is_multiple_of(2) {
            return Value::Null;
        }
        let mut map = serde_json::Map::with_capacity(args.len() / 2);
        let mut it = args.iter();
        while let (Some(k), Some(v)) = (it.next(), it.next()) {
            map.insert(Self::to_string_always(k), v.clone());
        }
        Value::Object(map)
    }

    // ---------- crypto & encoding functions ----------

    fn hash_input(value: &Value) -> Option<Vec<u8>> {
        if value.is_null() {
            return None;
        }
        Some(Self::to_string_always(value).into_bytes())
    }

    fn func_md5(args: &[Value]) -> Value {
        if args.len() != 1 {
            return Value::Null;
        }
        match Self::hash_input(&args[0]) {
            // `md5::Digest` is the same trait object as `sha2::Digest`
            // (shared `digest` crate), already imported above.
            Some(bytes) => Value::String(format!("{:x}", md5::Md5::digest(bytes))),
            None => Value::Null,
        }
    }

    fn func_sha256(args: &[Value]) -> Value {
        if args.len() != 1 {
            return Value::Null;
        }
        match Self::hash_input(&args[0]) {
            Some(bytes) => Value::String(format!("{:x}", sha2::Sha256::digest(bytes))),
            None => Value::Null,
        }
    }

    fn func_sha512(args: &[Value]) -> Value {
        if args.len() != 1 {
            return Value::Null;
        }
        match Self::hash_input(&args[0]) {
            Some(bytes) => Value::String(format!("{:x}", sha2::Sha512::digest(bytes))),
            None => Value::Null,
        }
    }

    fn func_encode(args: &[Value]) -> Value {
        if args.len() != 2 {
            return Value::Null;
        }
        let Some(method) = args[1].as_str() else {
            return Value::Null;
        };
        if args[0].is_null() {
            return Value::Null;
        }
        match method.trim().to_ascii_lowercase().as_str() {
            "base64" => Value::String(
                base64::engine::general_purpose::STANDARD.encode(Self::to_string_always(&args[0])),
            ),
            _ => Value::Null,
        }
    }

    fn func_base64_encode(args: &[Value]) -> Value {
        if args.len() != 1 {
            return Value::Null;
        }
        if args[0].is_null() {
            return Value::Null;
        }
        Value::String(
            base64::engine::general_purpose::STANDARD.encode(Self::to_string_always(&args[0])),
        )
    }

    fn func_decode(args: &[Value]) -> Value {
        if args.len() != 2 {
            return Value::Null;
        }
        let (Some(text), Some(method)) = (args[0].as_str(), args[1].as_str()) else {
            return Value::Null;
        };
        match method.trim().to_ascii_lowercase().as_str() {
            "base64" => match base64::engine::general_purpose::STANDARD.decode(text.trim()) {
                Ok(bytes) => match String::from_utf8(bytes) {
                    Ok(s) => Value::String(s),
                    Err(_) => Value::Null,
                },
                Err(_) => Value::Null,
            },
            _ => Value::Null,
        }
    }

    fn func_base64_decode(args: &[Value]) -> Value {
        if args.len() != 1 {
            return Value::Null;
        }
        let Some(text) = args[0].as_str() else {
            return Value::Null;
        };
        match base64::engine::general_purpose::STANDARD.decode(text.trim()) {
            Ok(bytes) => match String::from_utf8(bytes) {
                Ok(s) => Value::String(s),
                Err(_) => Value::Null,
            },
            Err(_) => Value::Null,
        }
    }

    // ---------- extended array functions ----------

    fn func_array_create(args: &[Value]) -> Value {
        Value::Array(args.to_vec())
    }

    fn func_array_position(args: &[Value]) -> Value {
        if args.len() != 2 {
            return Value::Null;
        }
        let Some(arr) = args[0].as_array() else {
            return Value::Null;
        };
        match arr
            .iter()
            .position(|item| Self::values_equal(item, &args[1]))
        {
            // 1-based index, 0 when absent.
            Some(i) => Value::from((i + 1) as i64),
            None => Value::from(0),
        }
    }

    fn func_array_length(args: &[Value]) -> Value {
        if args.len() != 1 {
            return Value::Null;
        }
        match args[0].as_array() {
            Some(arr) => Value::from(arr.len() as i64),
            None => Value::Null,
        }
    }

    fn func_array_slice(args: &[Value]) -> Value {
        if args.len() != 3 {
            return Value::Null;
        }
        let Some(arr) = args[0].as_array() else {
            return Value::Null;
        };
        let (Some(mut start), Some(mut end)) =
            (Self::to_i64_arg(&args[1]), Self::to_i64_arg(&args[2]))
        else {
            return Value::Null;
        };
        // 1-based inclusive bounds, clamped into range.
        let len = arr.len() as i64;
        if start < 1 {
            start = 1;
        }
        if end > len {
            end = len;
        }
        if start > end || start > len || end < 1 {
            return Value::Array(Vec::new());
        }
        Value::Array(arr[(start - 1) as usize..end as usize].to_vec())
    }

    fn func_array_concat(args: &[Value]) -> Value {
        if args.len() != 2 {
            return Value::Null;
        }
        let (Some(a), Some(b)) = (args[0].as_array(), args[1].as_array()) else {
            return Value::Null;
        };
        Value::Array(a.iter().chain(b.iter()).cloned().collect())
    }

    fn func_deduplicate(args: &[Value]) -> Value {
        if args.len() != 1 {
            return Value::Null;
        }
        let Some(arr) = args[0].as_array() else {
            return Value::Null;
        };
        let mut out: Vec<Value> = Vec::with_capacity(arr.len());
        for item in arr {
            if !out.iter().any(|seen| Self::values_equal(seen, item)) {
                out.push(item.clone());
            }
        }
        Value::Array(out)
    }

    // ---------- analytic scalar fallbacks (single-record context) ----------

    fn func_collect_scalar(args: &[Value]) -> Value {
        if args.len() != 1 {
            return Value::Null;
        }
        if args[0].is_null() {
            return Value::Array(Vec::new());
        }
        Value::Array(vec![args[0].clone()])
    }

    fn func_lead_scalar(args: &[Value]) -> Value {
        if args.is_empty() || args.len() > 3 {
            return Value::Null;
        }
        // A single row has no future rows: resolve to the default.
        args.get(2).cloned().unwrap_or(Value::Null)
    }

    fn func_latest_scalar(args: &[Value]) -> Value {
        if args.len() != 1 {
            return Value::Null;
        }
        if args[0].is_null() {
            return Value::Null;
        }
        args[0].clone()
    }

    fn func_had_changed_scalar(args: &[Value]) -> Value {
        if args.len() != 1 {
            return Value::Null;
        }
        // Without history every value counts as changed.
        Value::Bool(true)
    }

    fn func_changed_col_scalar(args: &[Value]) -> Value {
        if args.len() != 1 {
            return Value::Null;
        }
        if args[0].is_null() {
            return Value::Null;
        }
        args[0].clone()
    }

    /// Stateful change detection shared by `had_changed` / `changed_col`:
    /// compares against the stored previous value (both-null counts as
    /// unchanged), then stores the current value.
    fn eval_changed(
        lowered_name: &str,
        args: &[Value],
        state: &RuleState,
        call_id: &str,
        partition_key: &str,
    ) -> Value {
        if args.len() != 1 {
            return Value::Null;
        }
        let current = &args[0];
        let state_key = format!("{}:{}:{}", lowered_name, call_id, partition_key);
        let previous = state.state.read().get(&state_key).cloned();
        let changed = match (&previous, current) {
            (None, _) => true,
            (Some(p), c) if p.is_null() && c.is_null() => false,
            (Some(p), c) => !Self::values_equal(p, c),
        };
        state.state.write().insert(state_key, current.clone());
        if lowered_name == "had_changed" {
            Value::Bool(changed)
        } else if changed {
            current.clone()
        } else {
            Value::Null
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ArithOp {
    Add,
    Sub,
    Mul,
    Div,
    Mod,
}
