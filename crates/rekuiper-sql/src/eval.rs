use crate::ast::{BinaryOperator, Expr, SelectStmt, SetOp, UnaryOperator};
use base64::Engine as _;
use chrono::{Datelike, Timelike};
use parking_lot::RwLock;
use serde_json::Value;
use sha2::Digest as _;
use std::collections::HashMap;
use std::sync::Arc;

mod window;
pub use window::IncrementalWindow;

/// Record key holding source metadata (MQTT topic/qos/messageId). Read by
/// `meta()`/`mqtt()`, never projected by `SELECT *`.
pub const META_KEY: &str = "__meta__";

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

/// One compiled step of a dot-notation JSON path.
#[derive(Debug, Clone, PartialEq)]
enum JsonPathStep {
    Field(String),
    Index(usize),
    Wildcard,
}

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
                        if k != META_KEY && !k.starts_with("__") {
                            output.insert(k.clone(), v.clone());
                        }
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
                            if k != META_KEY && !k.starts_with("__") {
                                output.entry(k.clone()).or_insert_with(|| v.clone());
                            }
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

    /// True for window/batch aggregate function names (`count`, `avg`, …).
    /// Used by the server to decide between per-row projection and batch
    /// aggregation for windowed JOIN outputs.
    pub fn is_aggregate_call(name: &str) -> bool {
        matches!(
            name.to_ascii_lowercase().as_str(),
            "count"
                | "sum"
                | "avg"
                | "min"
                | "max"
                | "collect"
                | "lead"
                | "latest"
                | "median"
                | "stddev"
                | "stddevs"
                | "var"
                | "vars"
                | "percentile"
                | "percentile_disc"
                | "last_value"
                | "merge_agg"
                | "row_number"
                | "last_agg_hit_count"
                | "last_agg_hit_time"
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
            "median" => Self::agg_median(args, records),
            "stddev" => Self::agg_stddev(args, records),
            "stddevs" => Self::agg_stddevs(args, records),
            "var" => Self::agg_var(args, records),
            "vars" => Self::agg_vars(args, records),
            "percentile" => Self::agg_percentile(args, records),
            "percentile_disc" => Self::agg_percentile_disc(args, records),
            "last_value" => Self::agg_last_value(args, records),
            "merge_agg" => Self::agg_merge_agg(args, records),
            "row_number" => Self::agg_row_number(args, records),
            "last_agg_hit_count" => Self::agg_last_agg_hit_count(records),
            "last_agg_hit_time" => Self::agg_last_agg_hit_time(records),
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
            Expr::Index { .. } | Expr::Slice { .. } => match records.first() {
                Some(rec) => Self::eval_val(expr, rec),
                None => Value::Null,
            },
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
                        if k != META_KEY && !k.starts_with("__") {
                            output.insert(k.clone(), v.clone());
                        }
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
                Expr::Call { name, args } if name.eq_ignore_ascii_case("extract") => {
                    if let Some(arg) = args.first() {
                        let val = Self::eval_stateful_expr(arg, record, state);
                        if let Value::Object(map) = val {
                            for (k, v) in map {
                                output.insert(k, v);
                            }
                        }
                    }
                }
                Expr::Call { name, args }
                    if name.eq_ignore_ascii_case("changed_cols") && args.len() >= 2 =>
                {
                    let prefix = match Self::eval_stateful_expr(&args[0], record, state) {
                        Value::String(s) => s,
                        _ => String::new(),
                    };
                    let ignore_null = match Self::eval_stateful_expr(&args[1], record, state) {
                        Value::Bool(b) => b,
                        _ => false,
                    };
                    for (arg_idx, expr_arg) in args[2..].iter().enumerate() {
                        if matches!(expr_arg, Expr::Wildcard) {
                            for (k, v) in record {
                                if k == META_KEY {
                                    continue;
                                }
                                if ignore_null && v.is_null() {
                                    continue;
                                }
                                let state_key = format!("changed_cols:{}:{}:{}", idx, k, "");
                                let prev = state.state.read().get(&state_key).cloned();
                                let changed = match (&prev, v) {
                                    (None, _) => true,
                                    (Some(p), c) if p.is_null() && c.is_null() => false,
                                    (Some(p), c) => !Self::values_equal(p, c),
                                };
                                if changed {
                                    state.state.write().insert(state_key, v.clone());
                                    output.insert(format!("{}{}", prefix, k), v.clone());
                                }
                            }
                        } else {
                            let col_name = Self::column_name(expr_arg, arg_idx);
                            let val = Self::eval_stateful_expr(expr_arg, record, state);
                            if ignore_null && val.is_null() {
                                continue;
                            }
                            let state_key = format!("changed_cols:{}:{}:{}", idx, col_name, "");
                            let prev = state.state.read().get(&state_key).cloned();
                            let changed = match (&prev, &val) {
                                (None, _) => true,
                                (Some(p), c) if p.is_null() && c.is_null() => false,
                                (Some(p), c) => !Self::values_equal(p, c),
                            };
                            if changed {
                                state.state.write().insert(state_key, val.clone());
                                output.insert(format!("{}{}", prefix, col_name), val);
                            }
                        }
                    }
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
            |f| matches!(f, Expr::Call { name, .. } if name.eq_ignore_ascii_case("unnest") || name.eq_ignore_ascii_case("extract")),
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
            Expr::Index { base, index } => {
                let b = Self::eval_stateful_expr(base, record, state);
                let i = Self::eval_stateful_expr(index, record, state);
                Self::index_value(&b, &i)
            }
            Expr::Slice { base, lo, hi } => {
                let b = Self::eval_stateful_expr(base, record, state);
                let l = lo
                    .as_ref()
                    .map(|e| Self::eval_stateful_expr(e, record, state));
                let h = hi
                    .as_ref()
                    .map(|e| Self::eval_stateful_expr(e, record, state));
                Self::slice_value(&b, l.as_ref(), h.as_ref())
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
        if lowered == "changed_cols" && args.len() >= 2 {
            let vals: Vec<Value> = args
                .iter()
                .map(|a| Self::eval_stateful_expr(a, record, state))
                .collect();
            let prefix = match vals.first() {
                Some(Value::String(s)) => s.as_str(),
                _ => "",
            };
            let ignore_null = match vals.get(1) {
                Some(Value::Bool(b)) => *b,
                _ => false,
            };
            let mut result_map = serde_json::Map::new();
            for (arg_idx, expr_arg) in args.iter().skip(2).enumerate() {
                let col_name = Self::column_name(expr_arg, arg_idx);
                let val = &vals[arg_idx + 2];
                if ignore_null && val.is_null() {
                    continue;
                }
                let call_id = Self::column_name(expr, 0);
                let state_key = format!(
                    "changed_cols:{}:{}:{}",
                    call_id,
                    col_name,
                    partition_key.unwrap_or("")
                );
                let prev = state.state.read().get(&state_key).cloned();
                let changed = match (&prev, val) {
                    (None, _) => true,
                    (Some(p), c) if p.is_null() && c.is_null() => false,
                    (Some(p), c) => !Self::values_equal(p, c),
                };
                if changed {
                    state.state.write().insert(state_key, (*val).clone());
                    result_map.insert(format!("{}{}", prefix, col_name), (*val).clone());
                }
            }
            if result_map.is_empty() {
                return Value::Null;
            }
            return Value::Object(result_map);
        }
        if lowered == "row_number" {
            if !args.is_empty() {
                return Value::Null;
            }
            let call_id = Self::column_name(expr, 0);
            let state_key = format!("row_number:{}:{}", call_id, partition_key.unwrap_or(""));
            let mut guard = state.state.write();
            let next = guard
                .get(&state_key)
                .and_then(|v| v.as_i64())
                .unwrap_or(0)
                .saturating_add(1);
            guard.insert(state_key, Value::from(next));
            return Value::from(next);
        }
        if lowered == "last_hit_count" {
            let call_id = Self::column_name(expr, 0);
            let state_key = format!(
                "$$last_hit_count:{}:{}",
                call_id,
                partition_key.unwrap_or("")
            );
            let mut guard = state.state.write();
            let current = guard.get(&state_key).and_then(|v| v.as_i64()).unwrap_or(0);
            guard.insert(state_key, Value::from(current.saturating_add(1)));
            return Value::from(current);
        }
        if lowered == "last_hit_time" {
            let call_id = Self::column_name(expr, 0);
            let state_key = format!(
                "$$last_hit_time:{}:{}",
                call_id,
                partition_key.unwrap_or("")
            );
            let event_time = Self::resolve_event_time(record).as_i64().unwrap_or(0);
            let mut guard = state.state.write();
            let prev = guard.get(&state_key).and_then(|v| v.as_i64()).unwrap_or(0);
            guard.insert(state_key, Value::from(event_time));
            return Value::from(prev);
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
                | "acc_collect"
                | "acc_distinct_collect"
                | "distinct_acc"
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
            "acc_collect" => Self::acc_collect(state, &state_key, args),
            "acc_distinct_collect" | "distinct_acc" => {
                Self::acc_distinct_collect(state, &state_key, args)
            }
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

    /// `acc_collect(val)`: accumulates non-null expression values into a running array.
    fn acc_collect(state: &RuleState, state_key: &str, args: &[Value]) -> Value {
        if args.is_empty() {
            return Value::Null;
        }
        let mut list: Vec<Value> = {
            let guard = state.state.read();
            if let Some(Value::Array(arr)) = guard.get(state_key) {
                arr.clone()
            } else {
                Vec::new()
            }
        };
        if !args[0].is_null() {
            list.push(args[0].clone());
            state
                .state
                .write()
                .insert(state_key.to_string(), Value::Array(list.clone()));
        }
        Value::Array(list)
    }

    /// `acc_distinct_collect(val)` / `distinct_acc(val)`: accumulates unique non-null
    /// expression values into a running array, preserving insertion order.
    fn acc_distinct_collect(state: &RuleState, state_key: &str, args: &[Value]) -> Value {
        if args.is_empty() {
            return Value::Null;
        }
        let mut list: Vec<Value> = {
            let guard = state.state.read();
            if let Some(Value::Array(arr)) = guard.get(state_key) {
                arr.clone()
            } else {
                Vec::new()
            }
        };
        if !args[0].is_null()
            && !list
                .iter()
                .any(|existing| Self::values_equal(existing, &args[0]))
        {
            list.push(args[0].clone());
            state
                .state
                .write()
                .insert(state_key.to_string(), Value::Array(list.clone()));
        }
        Value::Array(list)
    }

    fn agg_last_agg_hit_count(_records: &[HashMap<String, Value>]) -> Value {
        static COUNT: std::sync::atomic::AtomicI64 = std::sync::atomic::AtomicI64::new(0);
        let cur = COUNT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Value::from(cur)
    }

    fn agg_last_agg_hit_time(records: &[HashMap<String, Value>]) -> Value {
        static LAST_TIME: std::sync::atomic::AtomicI64 = std::sync::atomic::AtomicI64::new(0);
        let prev = LAST_TIME.load(std::sync::atomic::Ordering::Relaxed);
        let window_end = records
            .first()
            .and_then(|r| r.get("__window_end__").or_else(|| r.get("window_end")))
            .and_then(|v| v.as_i64())
            .unwrap_or_else(|| chrono::Utc::now().timestamp_millis());
        LAST_TIME.store(window_end, std::sync::atomic::Ordering::Relaxed);
        Value::from(prev)
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
        let vals: Vec<Value> = records
            .iter()
            .map(|rec| Self::eval_val(&args[0], rec))
            .filter(|v| !v.is_null())
            .collect();
        if vals.is_empty() {
            return Value::Null;
        }
        let has_numbers = vals.iter().any(|v| v.is_number());
        let filtered: Vec<&Value> = if has_numbers {
            vals.iter().filter(|v| v.is_number()).collect()
        } else {
            vals.iter().collect()
        };
        if filtered.is_empty() {
            return Value::Null;
        }
        let mut best = filtered[0];
        for v in &filtered[1..] {
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
        let vals: Vec<Value> = records
            .iter()
            .map(|rec| Self::eval_val(&args[0], rec))
            .filter(|v| !v.is_null())
            .collect();
        if vals.is_empty() {
            return Value::Null;
        }
        let has_numbers = vals.iter().any(|v| v.is_number());
        let filtered: Vec<&Value> = if has_numbers {
            vals.iter().filter(|v| v.is_number()).collect()
        } else {
            vals.iter().collect()
        };
        if filtered.is_empty() {
            return Value::Null;
        }
        let mut best = filtered[0];
        for v in &filtered[1..] {
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
            return Value::Array(
                records
                    .iter()
                    .map(|rec| {
                        let mut map = serde_json::Map::new();
                        for (k, v) in rec {
                            if k != META_KEY && !k.starts_with("__") {
                                map.insert(k.clone(), v.clone());
                            }
                        }
                        Value::Object(map)
                    })
                    .collect(),
            );
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

    /// `lead(col, [offset], [default], [ignoreNull])`: forward lookup within the window
    /// batch. The single output row represents the whole window, so `offset`
    /// (default 1) counts forward from the first row (0-based); out-of-range
    /// offsets yield `default` (default `Null`). `ignoreNull` (default `true`)
    /// skips null records while counting forward.
    fn agg_lead(args: &[Expr], records: &[HashMap<String, Value>]) -> Value {
        if args.is_empty() || args.len() > 4 {
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
        let ignore_null: bool = if args.len() >= 4 {
            first
                .and_then(|rec| match Self::eval_val(&args[3], rec) {
                    Value::Bool(b) => Some(b),
                    _ => None,
                })
                .unwrap_or(true)
        } else {
            true
        };

        if ignore_null {
            let mut count = 0i64;
            for rec in records {
                let v = Self::eval_val(&args[0], rec);
                if !v.is_null() {
                    if count == offset {
                        return v;
                    }
                    count += 1;
                }
            }
            default
        } else {
            records
                .get(offset as usize)
                .map(|rec| Self::eval_val(&args[0], rec))
                .unwrap_or(default)
        }
    }

    /// Sorted numeric column values across the window batch, skipping
    /// nulls and non-numerics. Shared by the statistical aggregates.
    fn agg_sorted_numbers(arg: &Expr, records: &[HashMap<String, Value>]) -> Vec<f64> {
        let mut vals: Vec<f64> = Self::agg_numeric_values(arg, records)
            .iter()
            .filter_map(|v| v.as_f64())
            .collect();
        vals.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        vals
    }

    fn agg_median(args: &[Expr], records: &[HashMap<String, Value>]) -> Value {
        if args.len() != 1 {
            return Value::Null;
        }
        if matches!(args[0], Expr::Wildcard) {
            return Value::Null;
        }
        // Median over the raw values preserves integer results for odd
        // counts; the even-count average is always a float.
        let mut raw: Vec<Value> = Vec::new();
        for rec in records {
            let v = Self::eval_val(&args[0], rec);
            if v.is_null() || !v.is_number() {
                continue;
            }
            raw.push(v);
        }
        if raw.is_empty() {
            return Value::Null;
        }
        raw.sort_by(|a, b| Self::compare_values(a, b).unwrap_or(std::cmp::Ordering::Equal));
        let mid = raw.len() / 2;
        if raw.len() % 2 == 1 {
            raw[mid].clone()
        } else {
            let (Some(lo), Some(hi)) = (raw[mid - 1].as_f64(), raw[mid].as_f64()) else {
                return Value::Null;
            };
            serde_json::json!((lo + hi) / 2.0)
        }
    }

    /// Shared sum-of-squared-deviations helper; returns `(ssd, count)`.
    fn agg_ssd(args: &[Expr], records: &[HashMap<String, Value>]) -> Option<(f64, usize)> {
        if args.len() != 1 {
            return None;
        }
        if matches!(args[0], Expr::Wildcard) {
            return None;
        }
        let vals = Self::agg_sorted_numbers(&args[0], records);
        if vals.is_empty() {
            return None;
        }
        let mean = vals.iter().sum::<f64>() / vals.len() as f64;
        let ssd: f64 = vals.iter().map(|v| (v - mean).powi(2)).sum();
        Some((ssd, vals.len()))
    }

    fn agg_stddev(args: &[Expr], records: &[HashMap<String, Value>]) -> Value {
        match Self::agg_ssd(args, records) {
            None => Value::Null,
            Some((_, 0)) => Value::Null,
            Some((ssd, n)) => serde_json::json!((ssd / n as f64).sqrt()),
        }
    }

    fn agg_stddevs(args: &[Expr], records: &[HashMap<String, Value>]) -> Value {
        match Self::agg_ssd(args, records) {
            // Sample statistics need at least 2 points (0 dof otherwise).
            None => Value::Null,
            Some((_, n)) if n < 2 => Value::Null,
            Some((ssd, n)) => serde_json::json!((ssd / (n - 1) as f64).sqrt()),
        }
    }

    fn agg_var(args: &[Expr], records: &[HashMap<String, Value>]) -> Value {
        match Self::agg_ssd(args, records) {
            None => Value::Null,
            Some((_, 0)) => Value::Null,
            Some((ssd, n)) => serde_json::json!(ssd / n as f64),
        }
    }

    fn agg_vars(args: &[Expr], records: &[HashMap<String, Value>]) -> Value {
        match Self::agg_ssd(args, records) {
            None => Value::Null,
            Some((_, n)) if n < 2 => Value::Null,
            Some((ssd, n)) => serde_json::json!(ssd / (n - 1) as f64),
        }
    }

    /// Evaluate the percentile fraction `p` against the first batch record
    /// (constants in practice); `None` when missing, non-numeric or outside
    /// `[0, 1]`.
    fn agg_percentile_p(args: &[Expr], records: &[HashMap<String, Value>]) -> Option<f64> {
        if args.len() != 2 {
            return None;
        }
        if matches!(args[0], Expr::Wildcard) {
            return None;
        }
        let first = records.first()?;
        let p = Self::to_f64(&Self::eval_val(&args[1], first))?;
        if !(0.0..=1.0).contains(&p) {
            return None;
        }
        Some(p)
    }

    fn agg_percentile(args: &[Expr], records: &[HashMap<String, Value>]) -> Value {
        let Some(p) = Self::agg_percentile_p(args, records) else {
            return Value::Null;
        };
        let vals = Self::agg_sorted_numbers(&args[0], records);
        let n = vals.len();
        if n == 0 {
            return Value::Null;
        }
        if n == 1 {
            return serde_json::json!(vals[0]);
        }
        let rank = p * (n - 1) as f64;
        let i = rank.floor() as usize;
        if i + 1 >= n {
            return serde_json::json!(vals[n - 1]);
        }
        let frac = rank - i as f64;
        serde_json::json!(vals[i] + frac * (vals[i + 1] - vals[i]))
    }

    fn agg_percentile_disc(args: &[Expr], records: &[HashMap<String, Value>]) -> Value {
        let Some(p) = Self::agg_percentile_p(args, records) else {
            return Value::Null;
        };
        // Reuse the numeric ordering but keep original values so integer
        // types survive.
        let mut raw: Vec<Value> = Vec::new();
        for rec in records {
            let v = Self::eval_val(&args[0], rec);
            if v.is_null() || !v.is_number() {
                continue;
            }
            raw.push(v);
        }
        if raw.is_empty() {
            return Value::Null;
        }
        raw.sort_by(|a, b| Self::compare_values(a, b).unwrap_or(std::cmp::Ordering::Equal));
        let n = raw.len();
        let index = if p == 0.0 {
            0
        } else {
            ((p * n as f64).ceil() as usize)
                .saturating_sub(1)
                .min(n - 1)
        };
        raw[index].clone()
    }

    fn agg_last_value(args: &[Expr], records: &[HashMap<String, Value>]) -> Value {
        if args.is_empty() || args.len() > 2 {
            return Value::Null;
        }
        let ignore_nulls = if args.len() == 2 {
            let Some(first) = records.first() else {
                return Value::Null;
            };
            Self::eval_val(&args[1], first).as_bool().unwrap_or(false)
        } else {
            false
        };
        if matches!(args[0], Expr::Wildcard) {
            if !ignore_nulls {
                return records
                    .last()
                    .map(|rec| {
                        Value::Object(rec.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
                    })
                    .unwrap_or(Value::Null);
            }
            return records
                .iter()
                .rev()
                .find(|rec| rec.values().any(|v| !v.is_null()))
                .map(|rec| Value::Object(rec.iter().map(|(k, v)| (k.clone(), v.clone())).collect()))
                .unwrap_or(Value::Null);
        }
        if !ignore_nulls {
            return records
                .last()
                .map(|rec| Self::eval_val(&args[0], rec))
                .unwrap_or(Value::Null);
        }
        records
            .iter()
            .rev()
            .map(|rec| Self::eval_val(&args[0], rec))
            .find(|v| !v.is_null())
            .unwrap_or(Value::Null)
    }

    fn agg_merge_agg(args: &[Expr], records: &[HashMap<String, Value>]) -> Value {
        if args.len() != 1 {
            return Value::Null;
        }
        let mut merged = serde_json::Map::new();
        if matches!(args[0], Expr::Wildcard) {
            for rec in records {
                for (k, v) in rec {
                    merged.insert(k.clone(), v.clone());
                }
            }
            return Value::Object(merged);
        }
        for rec in records {
            if let Value::Object(map) = Self::eval_val(&args[0], rec) {
                for (k, v) in map {
                    merged.insert(k, v);
                }
            }
        }
        Value::Object(merged)
    }

    fn agg_row_number(args: &[Expr], _records: &[HashMap<String, Value>]) -> Value {
        if !args.is_empty() {
            return Value::Null;
        }
        Value::from(1)
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
            Expr::Index { base, .. } => Self::column_name(base, idx),
            Expr::Slice { base, .. } => Self::column_name(base, idx),
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

    /// Static result-type inference for schema introspection (`GET
    /// /rules/:id/schema`). Dynamic inputs (identifiers, untyped calls)
    /// infer to `"any"`.
    pub fn infer_expr_type(expr: &Expr) -> &'static str {
        match expr {
            Expr::Wildcard | Expr::Identifier(_) | Expr::FieldAccess { .. } => "any",
            Expr::Index { .. } => "any",
            Expr::Slice { .. } => "array",
            Expr::Literal(val) => match val {
                Value::Bool(_) => "boolean",
                Value::Number(n) => {
                    if n.is_i64() || n.is_u64() {
                        "bigint"
                    } else {
                        "float"
                    }
                }
                Value::String(_) => "string",
                Value::Array(_) => "array",
                Value::Object(_) => "struct",
                Value::Null => "any",
            },
            Expr::BinaryOp { left, op, right } => match op {
                BinaryOperator::Eq
                | BinaryOperator::Neq
                | BinaryOperator::Lt
                | BinaryOperator::Lte
                | BinaryOperator::Gt
                | BinaryOperator::Gte
                | BinaryOperator::And
                | BinaryOperator::Or
                | BinaryOperator::Like => "boolean",
                BinaryOperator::Div => "float",
                BinaryOperator::Add
                | BinaryOperator::Sub
                | BinaryOperator::Mul
                | BinaryOperator::Mod => {
                    if Self::infer_expr_type(left) == "bigint"
                        && Self::infer_expr_type(right) == "bigint"
                    {
                        "bigint"
                    } else {
                        "float"
                    }
                }
            },
            Expr::UnaryOp { op, expr } => match op {
                UnaryOperator::Not => "boolean",
                UnaryOperator::Neg => {
                    if Self::infer_expr_type(expr) == "bigint" {
                        "bigint"
                    } else {
                        "float"
                    }
                }
            },
            Expr::Between { .. } | Expr::InList { .. } | Expr::IsNull { .. } => "boolean",
            Expr::Call { name, .. } => match name.to_ascii_lowercase().as_str() {
                "avg" | "stddev" | "stddevs" | "var" | "vars" | "percentile" | "sin" | "cos"
                | "tan" | "asin" | "acos" | "atan" | "atan2" | "cosh" | "sinh" | "tanh" | "cot"
                | "radians" | "degrees" | "exp" | "ln" | "log" | "log2" | "log10" | "sqrt"
                | "pi" | "rand" => "float",
                "count" | "length" | "cardinality" | "array_cardinality" | "array_length"
                | "array_position" | "row_number" | "acc_count" | "year" | "month" | "day"
                | "day_of_week" | "day_of_month" | "day_of_year" | "hour" | "minute" | "second"
                | "microsecond" | "tstamp" | "to_seconds" | "from_days" | "bitand" | "bitor"
                | "bitxor" | "bitnot" => "bigint",
                "isnan" | "isnumeric" | "isnull" | "had_changed" | "changed_col"
                | "regexp_matches" | "startswith" | "endswith" | "json_path_exists"
                | "array_contains" | "array_contains_any" => "boolean",
                "concat" | "lower" | "upper" | "trim" | "ltrim" | "rtrim" | "lpad" | "rpad"
                | "replace" | "reverse" | "substr" | "substring" | "regexp_replace"
                | "regexp_substring" | "split_value" | "chr" | "hex2dec" | "dec2hex" | "encode"
                | "base64_encode" | "decode" | "base64_decode" | "uuid" | "newuuid"
                | "format_date" | "day_name" | "month_name" | "to_json" | "tojson" | "rule_id" => {
                    "string"
                }
                "split"
                | "array_create"
                | "array_slice"
                | "array_concat"
                | "deduplicate"
                | "array_remove"
                | "array_distinct"
                | "array_intersect"
                | "array_union"
                | "array_except"
                | "array_flatten"
                | "array_sort"
                | "repeat"
                | "sequence"
                | "obj_to_kvpair_array"
                | "object_to_kvpair_array"
                | "collect"
                | "keys"
                | "values"
                | "items"
                | "acc_collect" => "array",
                "object_construct"
                | "object_concat"
                | "object"
                | "zip"
                | "erase"
                | "object_erase"
                | "object_pick"
                | "kvpair_array_to_obj"
                | "json_map"
                | "merge_agg"
                | "acc_map_agg" => "struct",
                _ => "any",
            },
            Expr::Case {
                when_clauses,
                else_clause,
                ..
            } => {
                for (_, then_expr) in when_clauses {
                    let inferred = Self::infer_expr_type(then_expr);
                    if inferred != "any" {
                        return inferred;
                    }
                }
                else_clause
                    .as_ref()
                    .map(|e| Self::infer_expr_type(e))
                    .unwrap_or("any")
            }
            Expr::Over { call, .. } => Self::infer_expr_type(call),
        }
    }

    /// Output column name to inferred type for every SELECT field, honoring
    /// explicit `AS` aliases.
    pub fn infer_select_schema(stmt: &SelectStmt) -> serde_json::Map<String, Value> {
        let mut schema = serde_json::Map::new();
        for (idx, field) in stmt.fields.iter().enumerate() {
            let field_name = stmt
                .field_aliases
                .get(idx)
                .and_then(|a| a.clone())
                .unwrap_or_else(|| Self::column_name(field, idx));
            schema.insert(
                field_name,
                Value::String(Self::infer_expr_type(field).to_string()),
            );
        }
        schema
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
            Expr::Index { base, index } => {
                let b = Self::eval_val(base, record);
                let i = Self::eval_val(index, record);
                Self::index_value(&b, &i)
            }
            Expr::Slice { base, lo, hi } => {
                let b = Self::eval_val(base, record);
                let l = lo.as_ref().map(|e| Self::eval_val(e, record));
                let h = hi.as_ref().map(|e| Self::eval_val(e, record));
                Self::slice_value(&b, l.as_ref(), h.as_ref())
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
            "indexof" => Self::func_indexof(args),
            "format" => Self::func_format(args),
            // ---- Vector similarity & math ----
            "cosine_similarity" => Self::func_cosine_similarity(args),
            "vector_l2" | "euclidean_distance" => Self::func_vector_l2(args),
            "vector_dot" | "vector_dot_product" => Self::func_vector_dot(args),
            "vector_match" => Self::func_vector_match(args),
            // ---- WebAssembly (WASM) plugin functions ----
            "wasm" | "wasm_run" => Self::func_wasm_run(args),
            // ---- Array & object ----
            "array_contains" => Self::func_array_contains(args),
            "array_join" => Self::func_array_join(args),
            "keys" => Self::func_keys(args),
            "values" => Self::func_values(args),
            "object" => Self::func_object(args),
            "zip" => Self::func_zip(args),
            "items" => Self::func_items(args),
            // ---- Conversion & utility ----
            "cast" => Self::func_cast(args),
            "coalesce" => Self::func_coalesce(args),
            "delay" => Self::func_delay(args),
            "compress" => Self::func_compress(args),
            "decompress" => Self::func_decompress(args),
            "extract" => Self::func_extract(args),
            "unnest" => Self::func_unnest_scalar(args),
            "changed_cols" => Self::func_changed_cols_scalar(args),
            // ---- Validation & utility ----
            "isnan" => Self::func_isnan(args),
            "isnumeric" => Self::func_isnumeric(args),
            "nvl" => Self::func_coalesce(args),
            // ---- Object construction ----
            "object_construct" => Self::func_object_construct(args),
            "object_concat" => Self::func_object_concat(args),
            "erase" | "object_erase" => Self::func_erase(args),
            "object_pick" => Self::func_object_pick(args),
            "obj_to_kvpair_array" | "object_to_kvpair_array" => {
                Self::func_obj_to_kvpair_array(args)
            }
            "to_json" | "tojson" => Self::func_to_json(args),
            "parse_json" | "parsejson" | "json_parse" => Self::func_parse_json(args),
            // ---- DateTime ----
            "now" => Self::func_now(args),
            "current_timestamp" | "local_timestamp" => Self::func_now(args),
            "current_date" | "cur_date" => Self::func_current_date(args),
            "current_time" | "cur_time" | "local_time" => Self::func_current_time(args),
            "format_date" | "format_time" => Self::func_format_date(args),
            "from_unix_time" => Self::func_from_unix_time(args),
            "date_parse" => Self::func_date_parse(args),
            "date_add" => Self::func_date_add(args),
            "date_diff" => Self::func_date_diff(args),
            "date_calc" => Self::func_date_calc(args),
            "convert_tz" => Self::func_convert_tz(args),
            "year" => Self::func_year(args),
            "month" => Self::func_month(args),
            "day" => Self::func_day(args),
            "day_of_week" => Self::func_day_of_week(args),
            "day_of_month" => Self::func_day(args),
            "day_of_year" => Self::func_day_of_year(args),
            "day_name" => Self::func_day_name(args),
            "month_name" => Self::func_month_name(args),
            "microsecond" => Self::func_microsecond(args),
            "last_day" => Self::func_last_day(args),
            "to_seconds" => Self::func_to_seconds(args),
            "from_days" => Self::func_from_days(args),
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
            "sha1" => Self::func_sha1(args),
            "sha384" => Self::func_sha384(args),
            "crc32" => Self::func_crc32(args),
            "regexp_matches" => Self::func_regexp_matches(args),
            "regexp_replace" => Self::func_regexp_replace(args),
            "regexp_substring" => Self::func_regexp_substring(args),
            "split_value" => Self::func_split_value(args),
            "numbytes" => Self::func_numbytes(args),
            "chr" => Self::func_chr(args),
            "trunc" => Self::func_trunc(args),
            "hex2dec" => Self::func_hex2dec(args),
            "dec2hex" => Self::func_dec2hex(args),
            "encode" => Self::func_encode(args),
            "base64_encode" => Self::func_base64_encode(args),
            "decode" => Self::func_decode(args),
            "base64_decode" => Self::func_base64_decode(args),
            // ---- Extended array ----
            "array_create" => Self::func_array_create(args),
            "array_position" => Self::func_array_position(args),
            "array_positions" => Self::func_array_positions(args),
            "array_last_position" => Self::func_array_last_position(args),
            "array_shuffle" => Self::func_array_shuffle(args),
            "array_map" => Self::func_array_map(args),
            "array_length" => Self::func_array_length(args),
            "array_slice" => Self::func_array_slice(args),
            "array_concat" => Self::func_array_concat(args),
            "deduplicate" => Self::func_deduplicate(args),
            "cardinality" | "array_cardinality" => Self::func_cardinality(args),
            "element_at" => Self::func_element_at(args),
            "array_contains_any" => Self::func_array_contains_any(args),
            "array_remove" => Self::func_array_remove(args),
            "array_distinct" => Self::func_array_distinct(args),
            "array_intersect" => Self::func_array_intersect(args),
            "array_union" => Self::func_array_union(args),
            "array_except" => Self::func_array_except(args),
            "array_max" => Self::func_array_max(args),
            "array_min" => Self::func_array_min(args),
            "array_avg" => Self::func_array_avg(args),
            "array_flatten" => Self::func_array_flatten(args),
            "array_sort" => Self::func_array_sort(args),
            "repeat" => Self::func_repeat(args),
            "sequence" => Self::func_sequence(args),
            "kvpair_array_to_obj" => Self::func_kvpair_array_to_obj(args),
            // ---- Analytic scalar fallbacks (batch/stateful paths below) ----
            "collect" => Self::func_collect_scalar(args),
            "lead" => Self::func_lead_scalar(args),
            "latest" => Self::func_latest_scalar(args),
            "had_changed" => Self::func_had_changed_scalar(args),
            "changed_col" => Self::func_changed_col_scalar(args),
            "row_number" => Self::func_row_number_scalar(args),
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
            "rule_start" => serde_json::json!(chrono::Utc::now().timestamp_millis()),
            "last_hit_count" | "last_hit_time" => Value::from(0),
            "last_agg_hit_count" | "last_agg_hit_time" => Value::from(0),
            "get_keyed_state" => Self::func_get_keyed_state(args),
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

    fn func_get_keyed_state(args: &[Value]) -> Value {
        if args.len() != 3 {
            return Value::Null;
        }
        let Some(key) = args[0].as_str() else {
            return args.get(2).cloned().unwrap_or(Value::Null);
        };
        let Some(data_type) = args[1].as_str() else {
            return args.get(2).cloned().unwrap_or(Value::Null);
        };
        let default_val = &args[2];

        match rekuiper_core::get_keyed_state(key) {
            Some(val_str) => match data_type.to_ascii_lowercase().as_str() {
                "bigint" => val_str
                    .parse::<i64>()
                    .map(Value::from)
                    .unwrap_or_else(|_| default_val.clone()),
                "float" => val_str
                    .parse::<f64>()
                    .map(|f| serde_json::json!(f))
                    .unwrap_or_else(|_| default_val.clone()),
                "boolean" => val_str
                    .parse::<bool>()
                    .map(Value::from)
                    .unwrap_or_else(|_| default_val.clone()),
                "string" => Value::String(val_str),
                "datetime" => val_str
                    .parse::<i64>()
                    .map(Value::from)
                    .unwrap_or_else(|_| default_val.clone()),
                _ => default_val.clone(),
            },
            None => default_val.clone(),
        }
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
                &["__mqtt__", "mqtt", META_KEY],
            )),
            "event_time" => Some(Self::resolve_event_time(record)),
            "rule_id" => Some(Self::resolve_rule_id(record)),
            "rule_start" => Some(Self::resolve_rule_start(record)),
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
            Expr::Wildcard => {
                return meta_obj
                    .map(|m| Value::Object(m.clone()))
                    .unwrap_or(Value::Null);
            }
            Expr::Identifier(name) => Some(name.clone()),
            other => match Self::eval_val(other, record) {
                Value::String(s) => Some(s),
                _ => None,
            },
        };
        let Some(key) = key else {
            return Value::Null;
        };
        if let Some(m) = meta_obj {
            // eKuiper metadata keys are case-insensitive (`messageid`).
            if let Some(value) = m.get(&key).or_else(|| {
                m.iter()
                    .find(|(k, _)| k.eq_ignore_ascii_case(&key))
                    .map(|(_, v)| v)
            }) {
                return value.clone();
            }
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

    fn resolve_rule_start(record: &HashMap<String, Value>) -> Value {
        match record.get("__rule_start__") {
            Some(Value::Number(n)) => Value::Number(n.clone()),
            _ => Value::from(chrono::Utc::now().timestamp_millis()),
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

    /// Normalize a bracket bound to an integer offset. Numbers truncate
    /// toward zero; numeric strings parse; anything else is invalid.
    fn as_index_i64(v: &Value) -> Option<i64> {
        match v {
            Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    Some(i)
                } else if let Some(u) = n.as_u64() {
                    i64::try_from(u).ok()
                } else {
                    n.as_f64()
                        .filter(|f| f.is_finite())
                        .map(|f| f.trunc() as i64)
                }
            }
            Value::String(s) => s.trim().parse::<i64>().ok(),
            _ => None,
        }
    }

    /// Postfix index evaluation (0-based; negative counts back from the
    /// end; string keys look up object fields). Out-of-range and
    /// type mismatches yield Null.
    fn index_value(base: &Value, index: &Value) -> Value {
        match base {
            Value::Array(arr) => {
                let Some(i) = Self::as_index_i64(index) else {
                    return Value::Null;
                };
                let len = arr.len() as i64;
                let pos = if i >= 0 { i } else { len + i };
                if pos < 0 || pos >= len {
                    return Value::Null;
                }
                arr[pos as usize].clone()
            }
            Value::Object(map) => match index {
                Value::String(k) => map.get(k).cloned().unwrap_or(Value::Null),
                _ => Value::Null,
            },
            _ => Value::Null,
        }
    }

    /// Postfix slice evaluation: `base[lo:hi)` with end-exclusive `hi`,
    /// negatives from the end, omitted bounds meaning array start/end.
    /// Bounds clamp into range; an empty/inverted range yields `[]`.
    fn slice_value(base: &Value, lo: Option<&Value>, hi: Option<&Value>) -> Value {
        let Value::Array(arr) = base else {
            return Value::Null;
        };
        let len = arr.len() as i64;
        let norm = |v: Option<&Value>, default: i64| -> Option<i64> {
            match v {
                None => Some(default),
                Some(x) => {
                    let i = Self::as_index_i64(x)?;
                    Some(if i >= 0 { i } else { len + i })
                }
            }
        };
        let (Some(mut l), Some(mut h)) = (norm(lo, 0), norm(hi, len)) else {
            return Value::Null;
        };
        l = l.clamp(0, len);
        h = h.clamp(0, len);
        if l >= h {
            return Value::Array(Vec::new());
        }
        Value::Array(arr[l as usize..h as usize].to_vec())
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
            "datetime" => {
                if let Some(ms) = Self::to_epoch_millis(&args[0]) {
                    if let Some(dt) = Self::datetime_from_millis(ms) {
                        let formatted = if dt.timestamp_subsec_millis() == 0 {
                            dt.format("%Y-%m-%dT%H:%M:%SZ").to_string()
                        } else {
                            dt.format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string()
                        };
                        Value::String(formatted)
                    } else {
                        Value::Null
                    }
                } else {
                    Value::Null
                }
            }
            "bytea" => {
                let s = Self::to_string_always(&args[0]);
                if s.is_empty() {
                    Value::String(String::new())
                } else {
                    Value::String(base64::engine::general_purpose::STANDARD.encode(s.as_bytes()))
                }
            }
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
                    let r = v.log10();
                    if r.is_nan() || r.is_infinite() {
                        return Value::Null;
                    }
                    serde_json::json!(r)
                }
                _ => Value::Null,
            },
            2 => match (Self::to_f64(&args[0]), Self::to_f64(&args[1])) {
                (Some(base), Some(x)) if x > 0.0 && base > 0.0 => {
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
        if args.len() == 2 {
            let mut out = String::with_capacity(total + s.len());
            for _ in 0..total {
                out.push(' ');
            }
            out.push_str(&s);
            return Value::String(out);
        }
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
        if args.len() == 2 {
            let mut out = s;
            for _ in 0..total {
                out.push(' ');
            }
            return Value::String(out);
        }
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

    // ---------- vector similarity & distance functions ----------

    fn extract_f64_vec(val: &Value) -> Option<Vec<f64>> {
        match val {
            Value::Array(arr) => {
                let mut res = Vec::with_capacity(arr.len());
                for item in arr {
                    if let Some(f) = item.as_f64() {
                        res.push(f);
                    } else if let Some(i) = item.as_i64() {
                        res.push(i as f64);
                    } else {
                        let s = item.as_str()?;
                        let f = s.trim().parse::<f64>().ok()?;
                        res.push(f);
                    }
                }
                Some(res)
            }
            Value::String(s) => {
                if let Ok(Value::Array(arr)) = serde_json::from_str::<Value>(s.trim()) {
                    Self::extract_f64_vec(&Value::Array(arr))
                } else {
                    None
                }
            }
            _ => None,
        }
    }

    fn func_cosine_similarity(args: &[Value]) -> Value {
        if args.len() != 2 {
            return Value::Null;
        }
        let (Some(v1), Some(v2)) = (
            Self::extract_f64_vec(&args[0]),
            Self::extract_f64_vec(&args[1]),
        ) else {
            return Value::Null;
        };
        if v1.is_empty() || v1.len() != v2.len() {
            return Value::Null;
        }

        let mut dot = 0.0f64;
        let mut norm1 = 0.0f64;
        let mut norm2 = 0.0f64;

        for (a, b) in v1.iter().zip(v2.iter()) {
            dot += a * b;
            norm1 += a * a;
            norm2 += b * b;
        }

        if norm1 <= 0.0 || norm2 <= 0.0 {
            return serde_json::json!(0.0);
        }

        let sim = (dot / (norm1.sqrt() * norm2.sqrt())).clamp(-1.0, 1.0);
        serde_json::json!(sim)
    }

    fn func_vector_l2(args: &[Value]) -> Value {
        if args.len() != 2 {
            return Value::Null;
        }
        let (Some(v1), Some(v2)) = (
            Self::extract_f64_vec(&args[0]),
            Self::extract_f64_vec(&args[1]),
        ) else {
            return Value::Null;
        };
        if v1.is_empty() || v1.len() != v2.len() {
            return Value::Null;
        }

        let sum_sq: f64 = v1
            .iter()
            .zip(v2.iter())
            .map(|(a, b)| (a - b) * (a - b))
            .sum();
        serde_json::json!(sum_sq.sqrt())
    }

    fn func_vector_dot(args: &[Value]) -> Value {
        if args.len() != 2 {
            return Value::Null;
        }
        let (Some(v1), Some(v2)) = (
            Self::extract_f64_vec(&args[0]),
            Self::extract_f64_vec(&args[1]),
        ) else {
            return Value::Null;
        };
        if v1.is_empty() || v1.len() != v2.len() {
            return Value::Null;
        }

        let dot: f64 = v1.iter().zip(v2.iter()).map(|(a, b)| a * b).sum();
        serde_json::json!(dot)
    }

    fn func_vector_match(args: &[Value]) -> Value {
        if args.len() < 2 || args.len() > 3 {
            return Value::Null;
        }
        let Some(query_vec) = Self::extract_f64_vec(&args[0]) else {
            return Value::Null;
        };
        let Some(candidates) = args[1].as_array() else {
            return Value::Null;
        };
        let top_k = if args.len() == 3 {
            args[2].as_u64().unwrap_or(5) as usize
        } else {
            5
        };

        let mut scored: Vec<(f64, Value)> = Vec::new();

        for candidate in candidates {
            let cand_vec_opt = if let Some(obj) = candidate.as_object() {
                obj.get("embedding")
                    .or_else(|| obj.get("vector"))
                    .and_then(Self::extract_f64_vec)
            } else {
                Self::extract_f64_vec(candidate)
            };

            if let Some(cand_vec) = cand_vec_opt {
                if cand_vec.len() == query_vec.len() {
                    let mut dot = 0.0f64;
                    let mut norm1 = 0.0f64;
                    let mut norm2 = 0.0f64;
                    for (a, b) in query_vec.iter().zip(cand_vec.iter()) {
                        dot += a * b;
                        norm1 += a * a;
                        norm2 += b * b;
                    }
                    let sim = if norm1 > 0.0 && norm2 > 0.0 {
                        (dot / (norm1.sqrt() * norm2.sqrt())).clamp(-1.0, 1.0)
                    } else {
                        0.0
                    };
                    let mut item_obj = serde_json::Map::new();
                    item_obj.insert("similarity".to_string(), serde_json::json!(sim));
                    item_obj.insert("item".to_string(), candidate.clone());
                    scored.push((sim, Value::Object(item_obj)));
                }
            }
        }

        scored.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));
        let results: Vec<Value> = scored.into_iter().take(top_k).map(|(_, v)| v).collect();
        Value::Array(results)
    }

    // ---------- WebAssembly plugin invocation ----------

    fn func_wasm_run(args: &[Value]) -> Value {
        if args.len() < 2 {
            return Value::Null;
        }
        let Some(module_name) = args[0].as_str() else {
            return Value::Null;
        };
        let Some(func_name) = args[1].as_str() else {
            return Value::Null;
        };
        let func_args = &args[2..];
        rekuiper_core::get_global_wasm_registry()
            .call_module_func(module_name, func_name, func_args)
            .unwrap_or(Value::Null)
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
        if args.is_empty() || args.len() > 3 {
            return Value::Null;
        }
        let Some(arr) = args[0].as_array() else {
            return Value::Null;
        };
        if arr.is_empty() {
            return Value::Null;
        }
        let sep = if args.len() >= 2 {
            if args[1].is_null() {
                return Value::Null;
            }
            Self::to_string_always(&args[1])
        } else {
            ",".to_string()
        };
        let null_replacement = if args.len() == 3 {
            Some(Self::to_string_always(&args[2]))
        } else {
            None
        };
        let mut items = Vec::new();
        for item in arr {
            if item.is_null() {
                if let Some(ref nr) = null_replacement {
                    items.push(nr.clone());
                }
            } else {
                items.push(Self::to_string_always(item));
            }
        }
        Value::String(items.join(&sep))
    }

    fn func_indexof(args: &[Value]) -> Value {
        if args.len() != 2 {
            return Value::Null;
        }
        let (Some(s), Some(sub)) = (args[0].as_str(), args[1].as_str()) else {
            return Value::Null;
        };
        Value::from(s.find(sub).map(|i| i as i64).unwrap_or(-1))
    }

    fn func_format(args: &[Value]) -> Value {
        if args.is_empty() || args.len() > 3 {
            return Value::Null;
        }
        let Some(num) = Self::to_f64(&args[0]) else {
            return Value::Null;
        };
        let decimals = args.get(1).and_then(|v| v.as_i64()).unwrap_or(0).max(0) as usize;
        if args.len() <= 2 {
            return Value::String(format!("{:.prec$}", num, prec = decimals));
        }
        let locale = args.get(2).and_then(|v| v.as_str()).unwrap_or("en_US");
        let is_comma_decimal =
            locale.starts_with("de") || locale.starts_with("fr") || locale.starts_with("it");
        let (thousand_sep, decimal_sep) = if is_comma_decimal {
            ('.', ',')
        } else {
            (',', '.')
        };

        let formatted_base = format!("{:.prec$}", num, prec = decimals);
        let parts: Vec<&str> = formatted_base.split('.').collect();
        let int_part = parts[0];
        let is_neg = int_part.starts_with('-');
        let raw_int = if is_neg { &int_part[1..] } else { int_part };

        let mut with_commas = String::new();
        let len = raw_int.len();
        for (i, c) in raw_int.chars().enumerate() {
            if i > 0 && (len - i) % 3 == 0 {
                with_commas.push(thousand_sep);
            }
            with_commas.push(c);
        }
        let res = if is_neg {
            format!("-{}", with_commas)
        } else {
            with_commas
        };
        if decimals > 0 && parts.len() > 1 {
            Value::String(format!("{}{}{}", res, decimal_sep, parts[1]))
        } else {
            Value::String(res)
        }
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

    fn func_object(args: &[Value]) -> Value {
        if args.len() != 2 {
            return Value::Null;
        }
        let (Some(keys), Some(vals)) = (args[0].as_array(), args[1].as_array()) else {
            return Value::Null;
        };
        if keys.len() != vals.len() {
            return Value::Null;
        }
        let mut map = serde_json::Map::with_capacity(keys.len());
        for (k, v) in keys.iter().zip(vals.iter()) {
            let key_str = match k {
                Value::String(s) => s.clone(),
                other => Self::to_string_always(other),
            };
            map.insert(key_str, v.clone());
        }
        Value::Object(map)
    }

    fn func_zip(args: &[Value]) -> Value {
        if args.len() != 1 {
            return Value::Null;
        }
        let Some(entries) = args[0].as_array() else {
            return Value::Null;
        };
        let mut map = serde_json::Map::with_capacity(entries.len());
        for entry in entries {
            let Some(pair) = entry.as_array() else {
                return Value::Null;
            };
            if pair.len() != 2 {
                return Value::Null;
            }
            let key_str = match &pair[0] {
                Value::String(s) => s.clone(),
                other => Self::to_string_always(other),
            };
            map.insert(key_str, pair[1].clone());
        }
        Value::Object(map)
    }

    fn func_items(args: &[Value]) -> Value {
        if args.len() != 1 {
            return Value::Null;
        }
        let Some(obj) = args[0].as_object() else {
            return Value::Null;
        };
        let items: Vec<Value> = obj
            .iter()
            .map(|(k, v)| Value::Array(vec![Value::String(k.clone()), v.clone()]))
            .collect();
        Value::Array(items)
    }

    /// Builds an object from alternating key/value arguments:
    /// `object_construct(k1, v1, k2, v2, ...)`. Keys are stringified via
    /// [`Self::to_string_always`]; pairs with `Null` values are omitted
    /// (eKuiper parity); an odd argument count yields `Null`.
    #[allow(clippy::manual_is_multiple_of)]
    fn func_object_construct(args: &[Value]) -> Value {
        if args.len() % 2 != 0 {
            return Value::Null;
        }
        let mut map = serde_json::Map::with_capacity(args.len() / 2);
        let mut it = args.iter();
        while let (Some(k), Some(v)) = (it.next(), it.next()) {
            if v.is_null() {
                continue;
            }
            map.insert(Self::to_string_always(k), v.clone());
        }
        Value::Object(map)
    }

    /// Merges two or more objects left to right; later keys win.
    fn func_object_concat(args: &[Value]) -> Value {
        if args.len() < 2 {
            return Value::Null;
        }
        let mut map = serde_json::Map::new();
        for arg in args {
            let Some(obj) = arg.as_object() else {
                return Value::Null;
            };
            for (k, v) in obj {
                map.insert(k.clone(), v.clone());
            }
        }
        Value::Object(map)
    }

    /// Collects key names to erase/pick: plain values stringify, arrays
    /// contribute each element as a string.
    fn key_names(args: &[Value]) -> Vec<String> {
        let mut keys = Vec::new();
        for arg in args {
            if let Some(arr) = arg.as_array() {
                keys.extend(arr.iter().map(Self::to_string_always));
            } else {
                keys.push(Self::to_string_always(arg));
            }
        }
        keys
    }

    fn func_erase(args: &[Value]) -> Value {
        if args.len() < 2 {
            return Value::Null;
        }
        let Some(obj) = args[0].as_object() else {
            return Value::Null;
        };
        let mut map = obj.clone();
        for key in Self::key_names(&args[1..]) {
            map.remove(&key);
        }
        Value::Object(map)
    }

    fn func_object_pick(args: &[Value]) -> Value {
        if args.len() < 2 {
            return Value::Null;
        }
        let Some(obj) = args[0].as_object() else {
            return Value::Null;
        };
        let mut map = serde_json::Map::new();
        for key in Self::key_names(&args[1..]) {
            if let Some(v) = obj.get(&key) {
                map.insert(key, v.clone());
            }
        }
        Value::Object(map)
    }

    /// Inverse of [`Self::func_kvpair_array_to_obj`]: object entries become
    /// `{"key": k, "value": v}` elements.
    fn func_obj_to_kvpair_array(args: &[Value]) -> Value {
        if args.len() != 1 {
            return Value::Null;
        }
        let Some(obj) = args[0].as_object() else {
            return Value::Null;
        };
        Value::Array(
            obj.iter()
                .map(|(k, v)| {
                    let mut entry = serde_json::Map::with_capacity(2);
                    entry.insert("key".to_string(), Value::String(k.clone()));
                    entry.insert("value".to_string(), v.clone());
                    Value::Object(entry)
                })
                .collect(),
        )
    }

    fn func_to_json(args: &[Value]) -> Value {
        if args.len() != 1 {
            return Value::Null;
        }
        if args[0].is_null() {
            return Value::Null;
        }
        match serde_json::to_string(&args[0]) {
            Ok(s) => Value::String(s),
            Err(_) => Value::Null,
        }
    }

    fn func_parse_json(args: &[Value]) -> Value {
        if args.len() != 1 {
            return Value::Null;
        }
        match &args[0] {
            Value::Null => Value::Null,
            Value::String(s) => serde_json::from_str::<Value>(s).unwrap_or(Value::Null),
            structured => structured.clone(),
        }
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
            if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(t, "%Y-%m-%d %H:%M:%S") {
                return Some(dt.and_utc().timestamp_millis());
            }
            if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(t, "%Y-%m-%dT%H:%M:%S") {
                return Some(dt.and_utc().timestamp_millis());
            }
            if let Ok(d) = chrono::NaiveDate::parse_from_str(t, "%Y-%m-%d") {
                if let Some(dt) = d.and_hms_opt(0, 0, 0) {
                    return Some(dt.and_utc().timestamp_millis());
                }
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
        Value::String(chrono::Utc::now().format("%Y-%m-%d %H:%M:%S").to_string())
    }

    fn java_to_strftime(fmt: &str) -> String {
        if fmt.contains('%') {
            return fmt.to_string();
        }
        fmt.replace("YYYY", "%Y")
            .replace("yyyy", "%Y")
            .replace("yy", "%y")
            .replace("MM", "%m")
            .replace("dd", "%d")
            .replace("DD", "%d")
            .replace("HH", "%H")
            .replace("hh", "%I")
            .replace("mm", "%M")
            .replace("ss", "%S")
            .replace("SSS", "%3f")
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
        let pattern = Self::java_to_strftime(fmt);
        match dt {
            Some(dt) => Value::String(dt.format(&pattern).to_string()),
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
        if args.len() == 2 {
            let (Some(unit), Some(t1), Some(t2)) = (
                Self::interval_unit_millis("day"),
                Self::to_epoch_millis(&args[0]),
                Self::to_epoch_millis(&args[1]),
            ) else {
                return Value::Null;
            };
            return match t2.checked_sub(t1) {
                Some(diff) => Value::from(diff / unit),
                None => Value::Null,
            };
        }
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

    fn parse_duration_millis(s: &str) -> Option<i64> {
        let s = s.trim();
        if s.is_empty() {
            return None;
        }
        let (neg, s) = if let Some(rest) = s.strip_prefix('-') {
            (true, rest)
        } else if let Some(rest) = s.strip_prefix('+') {
            (false, rest)
        } else {
            (false, s)
        };

        let mut total_millis: i64 = 0;
        let bytes = s.as_bytes();
        let mut i = 0;
        while i < bytes.len() {
            let start_num = i;
            let mut has_dot = false;
            while i < bytes.len() && (bytes[i].is_ascii_digit() || (bytes[i] == b'.' && !has_dot)) {
                if bytes[i] == b'.' {
                    has_dot = true;
                }
                i += 1;
            }
            if i == start_num {
                return None;
            }
            let num: f64 = s[start_num..i].parse().ok()?;

            let start_unit = i;
            while i < bytes.len() && (bytes[i].is_ascii_alphabetic() || s[i..].starts_with('µ')) {
                if s[i..].starts_with('µ') {
                    i += 'µ'.len_utf8();
                } else {
                    i += 1;
                }
            }
            let unit = &s[start_unit..i];
            let factor = match unit {
                "ns" => 0.000_001,
                "us" | "µs" => 0.001,
                "ms" => 1.0,
                "s" => 1_000.0,
                "m" => 60_000.0,
                "h" => 3_600_000.0,
                "d" => 86_400_000.0,
                _ => return None,
            };
            total_millis = total_millis.checked_add((num * factor).round() as i64)?;
        }
        if neg {
            Some(-total_millis)
        } else {
            Some(total_millis)
        }
    }

    fn func_date_calc(args: &[Value]) -> Value {
        if args.len() != 2 {
            return Value::Null;
        }
        let Some(ts) = Self::to_epoch_millis(&args[0]) else {
            return Value::Null;
        };
        let Some(dur_str) = args[1].as_str() else {
            return Value::Null;
        };
        let Some(diff_ms) = Self::parse_duration_millis(dur_str) else {
            return Value::Null;
        };
        match ts.checked_add(diff_ms) {
            Some(res) => Value::from(res),
            None => Value::Null,
        }
    }

    fn func_convert_tz(args: &[Value]) -> Value {
        if args.len() < 2 || args.len() > 3 {
            return Value::Null;
        }
        let Some(to_tz_name) = (match args.len() {
            2 => args[1].as_str(),
            3 => args[2].as_str(),
            _ => None,
        }) else {
            return Value::Null;
        };
        let is_local = to_tz_name.eq_ignore_ascii_case("local");
        let to_tz: Option<chrono_tz::Tz> = if is_local {
            None
        } else if to_tz_name.eq_ignore_ascii_case("utc") {
            Some(chrono_tz::UTC)
        } else {
            to_tz_name.parse().ok()
        };
        if !is_local && to_tz.is_none() {
            return Value::Null;
        }

        let Some(millis) = Self::to_epoch_millis(&args[0]) else {
            return Value::Null;
        };
        let Some(dt) = Self::datetime_from_millis(millis) else {
            return Value::Null;
        };

        if is_local {
            let converted = dt.with_timezone(&chrono::Local);
            Value::String(converted.format("%Y-%m-%d %H:%M:%S").to_string())
        } else {
            let converted = dt.with_timezone(&to_tz.unwrap());
            Value::String(converted.format("%Y-%m-%d %H:%M:%S").to_string())
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

    fn func_current_date(args: &[Value]) -> Value {
        if !args.is_empty() {
            return Value::Null;
        }
        Value::String(chrono::Utc::now().format("%Y-%m-%d").to_string())
    }

    fn func_current_time(args: &[Value]) -> Value {
        if !args.is_empty() {
            return Value::Null;
        }
        Value::String(chrono::Utc::now().format("%H:%M:%S").to_string())
    }

    fn func_from_unix_time(args: &[Value]) -> Value {
        if args.is_empty() || args.len() > 2 {
            return Value::Null;
        }
        if args[0].is_null() {
            return Value::Null;
        }
        if args.len() == 2 {
            return Self::func_format_date(args);
        }
        match Self::to_epoch_millis(&args[0]).and_then(Self::datetime_from_millis) {
            Some(dt) => Value::String(dt.format("%Y-%m-%d %H:%M:%S").to_string()),
            None => Value::Null,
        }
    }

    /// MySQL convention: 1 = Sunday through 7 = Saturday.
    fn func_day_of_week(args: &[Value]) -> Value {
        Self::datetime_component(args, |dt| dt.weekday().num_days_from_sunday() as i32 + 1)
    }

    fn func_day_of_year(args: &[Value]) -> Value {
        Self::datetime_component(args, |dt| dt.ordinal() as i32)
    }

    fn func_day_name(args: &[Value]) -> Value {
        if args.len() != 1 {
            return Value::Null;
        }
        const NAMES: [&str; 7] = [
            "Sunday",
            "Monday",
            "Tuesday",
            "Wednesday",
            "Thursday",
            "Friday",
            "Saturday",
        ];
        match Self::to_epoch_millis(&args[0]).and_then(Self::datetime_from_millis) {
            Some(dt) => {
                Value::String(NAMES[dt.weekday().num_days_from_sunday() as usize].to_string())
            }
            None => Value::Null,
        }
    }

    fn func_month_name(args: &[Value]) -> Value {
        if args.len() != 1 {
            return Value::Null;
        }
        const NAMES: [&str; 12] = [
            "January",
            "February",
            "March",
            "April",
            "May",
            "June",
            "July",
            "August",
            "September",
            "October",
            "November",
            "December",
        ];
        match Self::to_epoch_millis(&args[0]).and_then(Self::datetime_from_millis) {
            Some(dt) => Value::String(NAMES[dt.month() as usize - 1].to_string()),
            None => Value::Null,
        }
    }

    fn func_microsecond(args: &[Value]) -> Value {
        Self::datetime_component(args, |dt| (dt.timestamp_subsec_micros() % 1_000_000) as i32)
    }

    /// Last calendar day of the argument's month as `"YYYY-MM-DD"`.
    fn func_last_day(args: &[Value]) -> Value {
        if args.len() != 1 {
            return Value::Null;
        }
        let dt = match Self::to_epoch_millis(&args[0]).and_then(Self::datetime_from_millis) {
            Some(dt) => dt.date_naive(),
            None => return Value::Null,
        };
        let (next_year, next_month) = if dt.month() == 12 {
            (dt.year() + 1, 1)
        } else {
            (dt.year(), dt.month() + 1)
        };
        match chrono::NaiveDate::from_ymd_opt(next_year, next_month, 1)
            .and_then(|first| first.pred_opt())
        {
            Some(last) => Value::String(last.format("%Y-%m-%d").to_string()),
            None => Value::Null,
        }
    }

    /// MySQL `TO_SECONDS`: seconds from year 0 to `ts`.
    /// `num_days_from_ce` counts from 0001-01-01, and year 0 contributes a
    /// further 365 days (matching `TO_SECONDS('0001-01-01') = 31622400`).
    fn func_to_seconds(args: &[Value]) -> Value {
        if args.len() != 1 {
            return Value::Null;
        }
        match Self::to_epoch_millis(&args[0]) {
            Some(ms) => Value::from(ms / 1000),
            None => Value::Null,
        }
    }

    /// MySQL `FROM_DAYS`: day count since year 0 back to `"YYYY-MM-DD"`.
    fn func_from_days(args: &[Value]) -> Value {
        if args.len() != 1 {
            return Value::Null;
        }
        let Some(n) = Self::to_i64_arg(&args[0]) else {
            return Value::Null;
        };
        // The chrono constructor takes i32 days; out-of-range inputs fail.
        let days = n.checked_sub(365).and_then(|d| i32::try_from(d).ok());
        match days.and_then(chrono::NaiveDate::from_num_days_from_ce_opt) {
            Some(date) => Value::String(date.format("%Y-%m-%d").to_string()),
            None => Value::Null,
        }
    }

    // ---------- JSON path functions ----------

    /// Compile a dot-notation path (`$.a.b[0]`, `a.Group[*].last`) into
    /// steps. A leading `$` root is identity. Returns `None` for malformed
    /// bracket expressions (filters, quotes) — those simply never match.
    fn compile_json_path(path: &str) -> Option<Vec<JsonPathStep>> {
        let mut p = path.trim();
        if p.is_empty() {
            return Some(Vec::new());
        }
        p = p.strip_prefix('$').unwrap_or(p);
        p = p.strip_prefix('.').unwrap_or(p);
        if p.is_empty() {
            return Some(Vec::new());
        }
        let mut steps = Vec::new();
        for seg in p.split('.') {
            if seg.is_empty() {
                return None;
            }
            let (name, mut rest) = match seg.find('[') {
                None => (seg, ""),
                Some(pos) => seg.split_at(pos),
            };
            // A bare numeric segment also indexes arrays.
            if !name.is_empty() {
                steps.push(JsonPathStep::Field(name.to_string()));
            }
            while let Some(inner) = rest.strip_prefix('[') {
                let end = inner.find(']')?;
                let token = &inner[..end];
                if token == "*" {
                    steps.push(JsonPathStep::Wildcard);
                } else if let Ok(i) = token.parse::<usize>() {
                    steps.push(JsonPathStep::Index(i));
                } else {
                    return None;
                }
                rest = &inner[end + 1..];
            }
            if !rest.is_empty() {
                return None;
            }
        }
        Some(steps)
    }

    /// Collect every value a compiled path selects. `[*]` fans out over
    /// array elements (objects yield Null for a wildcard step).
    fn json_collect<'v>(val: &'v Value, steps: &[JsonPathStep], out: &mut Vec<&'v Value>) {
        if steps.is_empty() {
            out.push(val);
            return;
        }
        match &steps[0] {
            JsonPathStep::Field(name) => match val {
                Value::Object(map) => {
                    if let Some(next) = map.get(name) {
                        Self::json_collect(next, &steps[1..], out);
                    }
                }
                Value::Array(arr) => {
                    if let Ok(i) = name.parse::<usize>() {
                        if let Some(next) = arr.get(i) {
                            Self::json_collect(next, &steps[1..], out);
                        }
                    }
                }
                _ => {}
            },
            JsonPathStep::Index(i) => {
                if let Value::Array(arr) = val {
                    if let Some(next) = arr.get(*i) {
                        Self::json_collect(next, &steps[1..], out);
                    }
                }
            }
            JsonPathStep::Wildcard => {
                if let Value::Array(arr) = val {
                    for next in arr {
                        Self::json_collect(next, &steps[1..], out);
                    }
                }
            }
        }
    }

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
        if path.trim().starts_with('/') {
            return Self::json_resolve_path(&args[0], path)
                .cloned()
                .unwrap_or(Value::Null);
        }
        let Some(steps) = Self::compile_json_path(path) else {
            return Value::Null;
        };
        let mut out: Vec<&Value> = Vec::new();
        Self::json_collect(&args[0], &steps, &mut out);
        if out.is_empty() {
            return Value::Null;
        }
        if steps.iter().any(|s| matches!(s, JsonPathStep::Wildcard)) {
            return Value::Array(out.into_iter().cloned().collect());
        }
        out.into_iter().next().cloned().unwrap_or(Value::Null)
    }

    fn func_json_path_query_first(args: &[Value]) -> Value {
        if args.len() != 2 {
            return Value::Null;
        }
        let Some(path) = args[1].as_str() else {
            return Value::Null;
        };
        if path.trim().starts_with('/') {
            return match Self::json_resolve_path(&args[0], path) {
                Some(Value::Array(arr)) => arr.first().cloned().unwrap_or(Value::Null),
                Some(v) => v.clone(),
                None => Value::Null,
            };
        }
        let Some(steps) = Self::compile_json_path(path) else {
            return Value::Null;
        };
        let mut out: Vec<&Value> = Vec::new();
        Self::json_collect(&args[0], &steps, &mut out);
        match out.into_iter().next() {
            // Arrays collapse to their first element; scalars pass through.
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
        if path.trim().starts_with('/') {
            return Value::Bool(Self::json_resolve_path(&args[0], path).is_some());
        }
        let Some(steps) = Self::compile_json_path(path) else {
            return Value::Bool(false);
        };
        let mut out: Vec<&Value> = Vec::new();
        Self::json_collect(&args[0], &steps, &mut out);
        Value::Bool(!out.is_empty())
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

    fn func_sha1(args: &[Value]) -> Value {
        if args.len() != 1 {
            return Value::Null;
        }
        match Self::hash_input(&args[0]) {
            Some(bytes) => Value::String(format!("{:x}", sha1::Sha1::digest(bytes))),
            None => Value::Null,
        }
    }

    fn func_sha384(args: &[Value]) -> Value {
        if args.len() != 1 {
            return Value::Null;
        }
        match Self::hash_input(&args[0]) {
            Some(bytes) => Value::String(format!("{:x}", sha2::Sha384::digest(bytes))),
            None => Value::Null,
        }
    }

    fn func_crc32(args: &[Value]) -> Value {
        if args.len() != 1 {
            return Value::Null;
        }
        if args[0].is_null() {
            return Value::Null;
        }
        let s = Self::to_string_always(&args[0]);
        if s.is_empty() {
            return Value::String("0".to_string());
        }
        let hash = crc32fast::hash(s.as_bytes());
        Value::String(format!("{:x}", hash))
    }

    fn func_regexp_matches(args: &[Value]) -> Value {
        if args.len() != 2 {
            return Value::Null;
        }
        if args.iter().any(|v| v.is_null()) {
            return Value::Null;
        }
        let (Some(text), Some(pattern)) = (args[0].as_str(), args[1].as_str()) else {
            return Value::Null;
        };
        match regex::Regex::new(pattern) {
            Ok(re) => Value::Bool(re.is_match(text)),
            Err(_) => Value::Null,
        }
    }

    fn func_regexp_replace(args: &[Value]) -> Value {
        if args.len() != 3 {
            return Value::Null;
        }
        if args.iter().any(|v| v.is_null()) {
            return Value::Null;
        }
        let (Some(text), Some(pattern), Some(repl)) =
            (args[0].as_str(), args[1].as_str(), args[2].as_str())
        else {
            return Value::Null;
        };
        match regex::Regex::new(pattern) {
            Ok(re) => Value::String(re.replace_all(text, repl).into_owned()),
            Err(_) => Value::Null,
        }
    }

    fn func_regexp_substring(args: &[Value]) -> Value {
        if args.len() != 2 {
            return Value::Null;
        }
        if args.iter().any(|v| v.is_null()) {
            return Value::Null;
        }
        let (Some(text), Some(pattern)) = (args[0].as_str(), args[1].as_str()) else {
            return Value::Null;
        };
        let Ok(re) = regex::Regex::new(pattern) else {
            return Value::Null;
        };
        let Some(caps) = re.captures(text) else {
            return Value::Null;
        };
        match caps.get(1).or_else(|| caps.get(0)) {
            Some(m) => Value::String(m.as_str().to_string()),
            None => Value::Null,
        }
    }

    /// 0-based split: index counts from the leading (possibly empty) segment.
    fn func_split_value(args: &[Value]) -> Value {
        if args.len() != 3 {
            return Value::Null;
        }
        if args.iter().any(|v| v.is_null()) {
            return Value::Null;
        }
        let (Some(text), Some(sep)) = (args[0].as_str(), args[1].as_str()) else {
            return Value::Null;
        };
        let Some(index) = Self::to_i64_arg(&args[2]) else {
            return Value::Null;
        };
        if index < 0 || sep.is_empty() {
            return Value::Null;
        }
        let parts: Vec<&str> = text.split(sep).collect();
        match parts.get(index as usize) {
            Some(part) => Value::String(part.to_string()),
            None => Value::Null,
        }
    }

    fn func_numbytes(args: &[Value]) -> Value {
        if args.len() != 1 {
            return Value::Null;
        }
        match &args[0] {
            Value::String(s) => Value::from(s.len() as i64),
            _ => Value::Null,
        }
    }

    fn func_chr(args: &[Value]) -> Value {
        if args.len() != 1 {
            return Value::Null;
        }
        if args[0].is_null() {
            return Value::Null;
        }
        let Some(code) = Self::to_i64_arg(&args[0]) else {
            return Value::Null;
        };
        if !(0..=u32::MAX as i64).contains(&code) {
            return Value::Null;
        }
        match char::from_u32(code as u32) {
            Some(c) => Value::String(c.to_string()),
            None => Value::Null,
        }
    }

    /// Truncate toward zero at `decimals` places (clamped to [0, 34]).
    /// Whole results come back as integers.
    fn func_trunc(args: &[Value]) -> Value {
        if args.is_empty() || args.len() > 2 {
            return Value::Null;
        }
        if args.iter().any(|v| v.is_null()) {
            return Value::Null;
        }
        let Some(num) = Self::to_f64(&args[0]) else {
            return Value::Null;
        };
        if !num.is_finite() {
            return Value::Null;
        }
        let decimals = if args.len() == 2 {
            let Some(d) = Self::to_i64_arg(&args[1]) else {
                return Value::Null;
            };
            d.clamp(0, 34)
        } else {
            0
        };
        let factor = 10f64.powi(decimals as i32);
        let truncated = (num * factor).trunc() / factor;
        // Whole results as integers when exactly representable.
        if decimals == 0
            && (-9_007_199_254_740_992.0..=9_007_199_254_740_992.0).contains(&truncated)
        {
            return Value::from(truncated as i64);
        }
        match serde_json::Number::from_f64(truncated) {
            Some(n) => Value::Number(n),
            None => Value::Null,
        }
    }

    fn func_hex2dec(args: &[Value]) -> Value {
        if args.len() != 1 {
            return Value::Null;
        }
        if args[0].is_null() {
            return Value::Null;
        }
        let Some(s) = args[0].as_str() else {
            return Value::Null;
        };
        let trimmed = s.trim();
        let hex = trimmed
            .strip_prefix("0x")
            .or_else(|| trimmed.strip_prefix("0X"))
            .unwrap_or(trimmed);
        match i64::from_str_radix(hex, 16) {
            Ok(n) => Value::from(n),
            Err(_) => Value::Null,
        }
    }

    fn func_dec2hex(args: &[Value]) -> Value {
        if args.len() != 1 {
            return Value::Null;
        }
        if args[0].is_null() {
            return Value::Null;
        }
        let Some(n) = Self::to_i64_arg(&args[0]) else {
            return Value::Null;
        };
        if n >= 0 {
            Value::String(format!("0x{:x}", n))
        } else {
            Value::String(format!("-0x{:x}", n.unsigned_abs()))
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

    fn func_compress(args: &[Value]) -> Value {
        use std::io::Write;
        if args.len() != 2 {
            return Value::Null;
        }
        if args[0].is_null() {
            return Value::Null;
        }
        let Some(method) = args[1].as_str() else {
            return Value::Null;
        };
        let input_bytes = match &args[0] {
            Value::String(s) => s.as_bytes().to_vec(),
            other => Self::to_string_always(other).into_bytes(),
        };
        let compressed = match method.trim().to_ascii_lowercase().as_str() {
            "zlib" => {
                let mut encoder =
                    flate2::write::ZlibEncoder::new(Vec::new(), flate2::Compression::default());
                if encoder.write_all(&input_bytes).is_err() {
                    return Value::Null;
                }
                encoder.finish().ok()
            }
            "gzip" => {
                let mut encoder =
                    flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
                if encoder.write_all(&input_bytes).is_err() {
                    return Value::Null;
                }
                encoder.finish().ok()
            }
            "flate" | "deflate" => {
                let mut encoder =
                    flate2::write::DeflateEncoder::new(Vec::new(), flate2::Compression::default());
                if encoder.write_all(&input_bytes).is_err() {
                    return Value::Null;
                }
                encoder.finish().ok()
            }
            "zstd" => zstd::encode_all(&input_bytes[..], 0).ok(),
            _ => return Value::Null,
        };
        match compressed {
            Some(bytes) => Value::String(base64::engine::general_purpose::STANDARD.encode(bytes)),
            None => Value::Null,
        }
    }

    fn func_decompress(args: &[Value]) -> Value {
        use std::io::Read;
        if args.len() != 2 {
            return Value::Null;
        }
        if args[0].is_null() {
            return Value::Null;
        }
        let Some(method) = args[1].as_str() else {
            return Value::Null;
        };
        let compressed_bytes = match &args[0] {
            Value::String(s) => base64::engine::general_purpose::STANDARD
                .decode(s.trim())
                .unwrap_or_else(|_| s.as_bytes().to_vec()),
            other => Self::to_string_always(other).into_bytes(),
        };
        let decompressed = match method.trim().to_ascii_lowercase().as_str() {
            "zlib" => {
                let mut decoder = flate2::read::ZlibDecoder::new(&compressed_bytes[..]);
                let mut out = Vec::new();
                if decoder.read_to_end(&mut out).is_err() {
                    return Value::Null;
                }
                Some(out)
            }
            "gzip" => {
                let mut decoder = flate2::read::GzDecoder::new(&compressed_bytes[..]);
                let mut out = Vec::new();
                if decoder.read_to_end(&mut out).is_err() {
                    return Value::Null;
                }
                Some(out)
            }
            "flate" | "deflate" => {
                let mut decoder = flate2::read::DeflateDecoder::new(&compressed_bytes[..]);
                let mut out = Vec::new();
                if decoder.read_to_end(&mut out).is_err() {
                    return Value::Null;
                }
                Some(out)
            }
            "zstd" => zstd::decode_all(&compressed_bytes[..]).ok(),
            _ => return Value::Null,
        };
        match decompressed {
            Some(bytes) => match String::from_utf8(bytes) {
                Ok(s) => Value::String(s),
                Err(e) => {
                    Value::String(base64::engine::general_purpose::STANDARD.encode(e.into_bytes()))
                }
            },
            None => Value::Null,
        }
    }

    fn func_delay(args: &[Value]) -> Value {
        if args.len() != 2 {
            return Value::Null;
        }
        if let Some(ms) = args[0].as_i64() {
            if ms > 0 {
                let sleep_ms = ms.min(10_000) as u64;
                std::thread::sleep(std::time::Duration::from_millis(sleep_ms));
            }
        }
        args[1].clone()
    }

    fn func_extract(args: &[Value]) -> Value {
        if args.len() != 1 {
            return Value::Null;
        }
        args[0].clone()
    }

    fn func_unnest_scalar(args: &[Value]) -> Value {
        if args.len() != 1 {
            return Value::Null;
        }
        args[0].clone()
    }

    fn func_changed_cols_scalar(args: &[Value]) -> Value {
        if args.len() < 3 {
            return Value::Null;
        }
        args[2].clone()
    }

    // ---------- extended array functions ----------

    fn func_array_create(args: &[Value]) -> Value {
        Value::Array(args.to_vec())
    }

    fn func_array_position(args: &[Value]) -> Value {
        if args.len() != 2 {
            return Value::Null;
        }
        if args[0].is_null() {
            return Value::from(-1);
        }
        let Some(arr) = args[0].as_array() else {
            return Value::Null;
        };
        match arr
            .iter()
            .position(|item| Self::values_equal(item, &args[1]))
        {
            // 0-based index, -1 when absent or nil (matching eKuiper specification).
            Some(i) => Value::from(i as i64),
            None => Value::from(-1),
        }
    }

    fn func_array_last_position(args: &[Value]) -> Value {
        if args.len() != 2 {
            return Value::Null;
        }
        if args[0].is_null() {
            return Value::from(-1);
        }
        let Some(arr) = args[0].as_array() else {
            return Value::Null;
        };
        match arr
            .iter()
            .rposition(|item| Self::values_equal(item, &args[1]))
        {
            // 0-based index, -1 when absent or nil (matching eKuiper specification).
            Some(i) => Value::from(i as i64),
            None => Value::from(-1),
        }
    }

    fn func_array_positions(args: &[Value]) -> Value {
        if args.len() != 2 {
            return Value::Null;
        }
        if args[0].is_null() {
            return Value::Array(Vec::new());
        }
        let Some(arr) = args[0].as_array() else {
            return Value::Null;
        };
        let target = &args[1];
        let mut positions = Vec::new();
        for (i, item) in arr.iter().enumerate() {
            if Self::values_equal(item, target) {
                positions.push(Value::from(i as i64));
            }
        }
        Value::Array(positions)
    }

    fn func_array_shuffle(args: &[Value]) -> Value {
        if args.len() != 1 {
            return Value::Null;
        }
        let Some(arr) = args[0].as_array() else {
            return Value::Null;
        };
        let mut out = arr.clone();
        use rand::seq::SliceRandom;
        let mut rng = rand::thread_rng();
        out.shuffle(&mut rng);
        Value::Array(out)
    }

    fn func_array_map(args: &[Value]) -> Value {
        if args.len() != 2 {
            return Value::Null;
        }
        let Some(func_name) = args[0].as_str() else {
            return Value::Null;
        };
        let Some(arr) = args[1].as_array() else {
            return Value::Null;
        };
        let mapped: Vec<Value> = arr
            .iter()
            .map(|item| Self::eval_call(func_name, std::slice::from_ref(item)))
            .collect();
        Value::Array(mapped)
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

    fn func_cardinality(args: &[Value]) -> Value {
        if args.len() != 1 {
            return Value::Null;
        }
        match &args[0] {
            Value::Array(arr) => Value::from(arr.len() as i64),
            Value::Null => Value::from(0),
            _ => Value::Null,
        }
    }

    fn func_element_at(args: &[Value]) -> Value {
        if args.len() != 2 {
            return Value::Null;
        }
        if let Some(obj) = args[0].as_object() {
            let key = match &args[1] {
                Value::String(s) => s.as_str(),
                _ => return Value::Null,
            };
            return obj.get(key).cloned().unwrap_or(Value::Null);
        }
        let Some(arr) = args[0].as_array() else {
            return Value::Null;
        };
        let Some(index) = Self::to_i64_arg(&args[1]) else {
            return Value::Null;
        };
        // 0-based indexing for positive indices; negatives count back from the end (-1 is last).
        let len = arr.len() as i64;
        let pos = if index >= 0 {
            index
        } else {
            len + index
        };
        if pos < 0 || pos >= len {
            return Value::Null;
        }
        arr[pos as usize].clone()
    }

    fn func_array_contains_any(args: &[Value]) -> Value {
        if args.len() != 2 {
            return Value::Null;
        }
        let (Some(haystack), Some(needles)) = (args[0].as_array(), args[1].as_array()) else {
            return Value::Bool(false);
        };
        Value::Bool(
            needles
                .iter()
                .any(|n| haystack.iter().any(|h| Self::values_equal(h, n))),
        )
    }

    fn func_array_remove(args: &[Value]) -> Value {
        if args.len() != 2 {
            return Value::Null;
        }
        let Some(arr) = args[0].as_array() else {
            return Value::Null;
        };
        Value::Array(
            arr.iter()
                .filter(|item| !Self::values_equal(item, &args[1]))
                .cloned()
                .collect(),
        )
    }

    fn func_array_distinct(args: &[Value]) -> Value {
        Self::func_deduplicate(args)
    }

    fn func_array_intersect(args: &[Value]) -> Value {
        if args.len() != 2 {
            return Value::Null;
        }
        let (Some(a), Some(b)) = (args[0].as_array(), args[1].as_array()) else {
            return Value::Null;
        };
        let mut out = Vec::new();
        for item in a {
            if b.iter().any(|other| Self::values_equal(item, other))
                && !out.iter().any(|seen| Self::values_equal(seen, item))
            {
                out.push(item.clone());
            }
        }
        Value::Array(out)
    }

    fn func_array_union(args: &[Value]) -> Value {
        if args.len() != 2 {
            return Value::Null;
        }
        let (Some(a), Some(b)) = (args[0].as_array(), args[1].as_array()) else {
            return Value::Null;
        };
        let mut out: Vec<Value> = Vec::with_capacity(a.len() + b.len());
        for item in a.iter().chain(b.iter()) {
            if !out.iter().any(|seen| Self::values_equal(seen, item)) {
                out.push(item.clone());
            }
        }
        Value::Array(out)
    }

    fn func_array_except(args: &[Value]) -> Value {
        if args.len() != 2 {
            return Value::Null;
        }
        let (Some(a), Some(b)) = (args[0].as_array(), args[1].as_array()) else {
            return Value::Null;
        };
        Value::Array(
            a.iter()
                .filter(|item| !b.iter().any(|other| Self::values_equal(item, other)))
                .cloned()
                .collect(),
        )
    }

    /// Shared numeric scan for `array_max`/`array_min`: extreme value among
    /// numeric elements, preserving the original JSON representation.
    fn array_extreme<F>(arr: &[Value], better: F) -> Value
    where
        F: Fn(std::cmp::Ordering) -> bool,
    {
        let mut best: Option<&Value> = None;
        for item in arr {
            if !item.is_number() {
                continue;
            }
            best = Some(match best {
                None => item,
                Some(current) => match Self::compare_values(item, current) {
                    Some(ord) if better(ord) => item,
                    _ => current,
                },
            });
        }
        best.cloned().unwrap_or(Value::Null)
    }

    fn func_array_max(args: &[Value]) -> Value {
        if args.len() != 1 {
            return Value::Null;
        }
        let Some(arr) = args[0].as_array() else {
            return Value::Null;
        };
        Self::array_extreme(arr, |ord| ord == std::cmp::Ordering::Greater)
    }

    fn func_array_min(args: &[Value]) -> Value {
        if args.len() != 1 {
            return Value::Null;
        }
        let Some(arr) = args[0].as_array() else {
            return Value::Null;
        };
        Self::array_extreme(arr, |ord| ord == std::cmp::Ordering::Less)
    }

    fn func_array_avg(args: &[Value]) -> Value {
        if args.len() != 1 {
            return Value::Null;
        }
        let Some(arr) = args[0].as_array() else {
            return Value::Null;
        };
        let mut sum = 0.0;
        let mut count = 0u64;
        for item in arr {
            if let Some(f) = item.as_f64() {
                sum += f;
                count += 1;
            }
        }
        if count == 0 {
            return Value::Null;
        }
        serde_json::json!(sum / count as f64)
    }

    fn func_array_flatten(args: &[Value]) -> Value {
        if args.len() != 1 {
            return Value::Null;
        }
        let Some(arr) = args[0].as_array() else {
            return Value::Null;
        };
        let mut out = Vec::new();
        for item in arr {
            match item {
                Value::Array(inner) => out.extend(inner.iter().cloned()),
                other => out.push(other.clone()),
            }
        }
        Value::Array(out)
    }

    fn func_array_sort(args: &[Value]) -> Value {
        if args.len() != 1 {
            return Value::Null;
        }
        let Some(arr) = args[0].as_array() else {
            return Value::Null;
        };
        let mut out = arr.clone();
        out.sort_by(|a, b| {
            Self::compare_values(a, b)
                .unwrap_or_else(|| Self::to_string_always(a).cmp(&Self::to_string_always(b)))
        });
        Value::Array(out)
    }

    fn func_repeat(args: &[Value]) -> Value {
        if args.len() != 2 {
            return Value::Null;
        }
        let Some(n) = Self::to_i64_arg(&args[1]) else {
            return Value::Null;
        };
        if n < 0 {
            return Value::Null;
        }
        Value::Array(vec![args[0].clone(); n as usize])
    }

    fn func_sequence(args: &[Value]) -> Value {
        if args.len() != 2 && args.len() != 3 {
            return Value::Null;
        }
        let (Some(start), Some(stop)) = (Self::to_i64_arg(&args[0]), Self::to_i64_arg(&args[1]))
        else {
            return Value::Null;
        };
        let step = if args.len() == 3 {
            let Some(step) = Self::to_i64_arg(&args[2]) else {
                return Value::Null;
            };
            if step == 0 {
                return Value::Null;
            }
            step
        } else if start <= stop {
            1
        } else {
            -1
        };
        // An explicit step fighting the direction would loop forever.
        if (step > 0 && start > stop) || (step < 0 && start < stop) {
            return Value::Null;
        }
        let mut out = Vec::new();
        let mut current = start;
        loop {
            out.push(Value::from(current));
            if current == stop {
                break;
            }
            match current.checked_add(step) {
                Some(next) => current = next,
                None => break,
            }
        }
        Value::Array(out)
    }

    fn func_kvpair_array_to_obj(args: &[Value]) -> Value {
        if args.len() != 1 {
            return Value::Null;
        }
        let Some(arr) = args[0].as_array() else {
            return Value::Null;
        };
        let mut map = serde_json::Map::new();
        for item in arr {
            let Some(obj) = item.as_object() else {
                continue;
            };
            let Some(key) = obj
                .get("key")
                .or_else(|| obj.get("Key"))
                .or_else(|| obj.get("k"))
                .map(Self::to_string_always)
            else {
                continue;
            };
            let value = obj
                .get("value")
                .or_else(|| obj.get("Value"))
                .or_else(|| obj.get("v"))
                .cloned()
                .unwrap_or(Value::Null);
            map.insert(key, value);
        }
        Value::Object(map)
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
        if args.is_empty() || args.len() > 4 {
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

    fn func_row_number_scalar(args: &[Value]) -> Value {
        if !args.is_empty() {
            return Value::Null;
        }
        Value::from(1)
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
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ArithOp {
    Add,
    Sub,
    Mul,
    Div,
    Mod,
}
