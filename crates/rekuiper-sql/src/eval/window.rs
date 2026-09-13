//! Window trigger evaluation with eKuiper semantics.
//!
//! A window trigger turns the rows collected for one window into output rows:
//! `WHERE` filters the input rows, `GROUP BY` partitions them (one output row
//! per group, groups in first-seen order), `HAVING` filters groups, and
//! `ORDER BY` / `LIMIT` shape the result. A statement without aggregates or
//! `GROUP BY` projects every surviving row.
//!
//! [`IncrementalWindow`] computes the same result for the common IIoT shape
//! (group columns plus `count/sum/avg/min/max`) without retaining rows: memory
//! is O(groups) instead of O(rows in the window).

use super::Evaluator;
use crate::ast::{Expr, SelectStmt, SortOrder};
use serde_json::Value;
use std::borrow::Cow;
use std::collections::HashMap;
use std::fmt::Write as _;

static NULL: Value = Value::Null;

type Row = HashMap<String, Value>;

impl Evaluator {
    /// True when `expr` calls an aggregate function anywhere inside it.
    pub fn contains_aggregate(expr: &Expr) -> bool {
        match expr {
            Expr::Call { name, args } => {
                Self::is_aggregate_call(name) || args.iter().any(Self::contains_aggregate)
            }
            Expr::BinaryOp { left, right, .. } => {
                Self::contains_aggregate(left) || Self::contains_aggregate(right)
            }
            Expr::UnaryOp { expr, .. } => Self::contains_aggregate(expr),
            Expr::Between {
                expr, low, high, ..
            } => {
                Self::contains_aggregate(expr)
                    || Self::contains_aggregate(low)
                    || Self::contains_aggregate(high)
            }
            Expr::InList { expr, list, .. } => {
                Self::contains_aggregate(expr) || list.iter().any(Self::contains_aggregate)
            }
            Expr::IsNull { expr, .. } => Self::contains_aggregate(expr),
            Expr::FieldAccess { parent, .. } => Self::contains_aggregate(parent),
            Expr::Index { base, index } => {
                Self::contains_aggregate(base) || Self::contains_aggregate(index)
            }
            Expr::Slice { base, lo, hi } => {
                Self::contains_aggregate(base)
                    || lo.as_ref().is_some_and(|e| Self::contains_aggregate(e))
                    || hi.as_ref().is_some_and(|e| Self::contains_aggregate(e))
            }
            Expr::Case {
                operand,
                when_clauses,
                else_clause,
            } => {
                operand
                    .as_ref()
                    .is_some_and(|e| Self::contains_aggregate(e))
                    || when_clauses
                        .iter()
                        .any(|(w, t)| Self::contains_aggregate(w) || Self::contains_aggregate(t))
                    || else_clause
                        .as_ref()
                        .is_some_and(|e| Self::contains_aggregate(e))
            }
            Expr::Over { call, partition_by } => {
                Self::contains_aggregate(call)
                    || partition_by
                        .as_ref()
                        .is_some_and(|e| Self::contains_aggregate(e))
            }
            Expr::Wildcard | Expr::Identifier(_) | Expr::Literal(_) => false,
        }
    }

    /// True when a window trigger yields grouped rows (aggregates or
    /// `GROUP BY`) rather than one projected row per input row.
    pub fn is_grouped_window(stmt: &SelectStmt) -> bool {
        !stmt.group_by.is_empty()
            || stmt.fields.iter().any(Self::contains_aggregate)
            || stmt.having.as_ref().is_some_and(Self::contains_aggregate)
    }

    /// Input-row `WHERE` test (rows pass when there is no `WHERE`).
    pub fn passes_where(stmt: &SelectStmt, row: &Row) -> bool {
        match &stmt.where_clause {
            Some(cond) => Self::eval_bool(cond, row),
            None => true,
        }
    }

    /// Evaluate one window trigger over its rows (applies `WHERE`).
    pub fn eval_window(stmt: &SelectStmt, rows: Vec<Row>) -> Vec<Row> {
        let rows = if stmt.where_clause.is_some() {
            rows.into_iter()
                .filter(|r| Self::passes_where(stmt, r))
                .collect()
        } else {
            rows
        };
        Self::eval_window_filtered(stmt, rows)
    }

    /// [`Self::eval_window`] for rows that already passed `WHERE`.
    pub fn eval_window_filtered(stmt: &SelectStmt, rows: Vec<Row>) -> Vec<Row> {
        if rows.is_empty() {
            return Vec::new();
        }
        let mut out: Vec<Row> = if !Self::is_grouped_window(stmt) {
            let projection = SelectStmt {
                where_clause: None,
                ..stmt.clone()
            };
            rows.iter()
                .filter_map(|r| Self::eval_select(&projection, r))
                .collect()
        } else if stmt.group_by.is_empty() {
            Self::eval_aggregate(stmt, &rows).into_iter().collect()
        } else {
            Self::partition_groups(&stmt.group_by, rows)
                .iter()
                .filter_map(|group| Self::eval_aggregate(stmt, group))
                .collect()
        };
        Self::apply_order_limit(stmt, &mut out);
        out
    }

    /// Split rows into `GROUP BY` partitions, groups in first-seen order and
    /// rows in arrival order within each group.
    fn partition_groups(group_by: &[Expr], rows: Vec<Row>) -> Vec<Vec<Row>> {
        let mut index: HashMap<String, usize> = HashMap::new();
        let mut groups: Vec<Vec<Row>> = Vec::new();
        let mut key = String::new();
        for row in rows {
            key.clear();
            for expr in group_by {
                write_group_key(&mut key, &Self::group_value(expr, &row));
            }
            match index.get(key.as_str()) {
                Some(&i) => groups[i].push(row),
                None => {
                    index.insert(key.clone(), groups.len());
                    groups.push(vec![row]);
                }
            }
        }
        groups
    }

    /// Value of a grouping or accumulator expression, borrowing plain columns.
    fn group_value<'a>(expr: &Expr, row: &'a Row) -> Cow<'a, Value> {
        match expr {
            Expr::Identifier(name) => Cow::Borrowed(row.get(name).unwrap_or(&NULL)),
            other => Cow::Owned(Self::eval_val(other, row)),
        }
    }

    /// Stable `ORDER BY` over output rows, then `LIMIT`.
    pub(crate) fn apply_order_limit(stmt: &SelectStmt, rows: &mut Vec<Row>) {
        if !stmt.order_by.is_empty() && rows.len() > 1 {
            rows.sort_by(|a, b| {
                for (idx, item) in stmt.order_by.iter().enumerate() {
                    let va = Self::order_value(&item.expr, a, idx);
                    let vb = Self::order_value(&item.expr, b, idx);
                    let ord = Self::compare_values(&va, &vb).unwrap_or(std::cmp::Ordering::Equal);
                    let ord = match item.order {
                        SortOrder::Asc => ord,
                        SortOrder::Desc => ord.reverse(),
                    };
                    if ord != std::cmp::Ordering::Equal {
                        return ord;
                    }
                }
                std::cmp::Ordering::Equal
            });
        }
        if let Some(limit) = stmt.limit {
            rows.truncate(limit);
        }
    }

    /// Sort key of an output row: the expression over the row, or for an
    /// aggregate call the projected column it produced.
    fn order_value(expr: &Expr, row: &Row, idx: usize) -> Value {
        let v = Self::eval_val(expr, row);
        if v.is_null() && matches!(expr, Expr::Call { .. }) {
            if let Some(found) = row.get(&Self::column_name(expr, idx)) {
                return found.clone();
            }
        }
        v
    }
}

/// Appends an unambiguous encoding of one grouping value to `key`.
fn write_group_key(key: &mut String, value: &Value) {
    match value {
        // Length-prefixed so strings never collide with other types or with
        // the next key component.
        Value::String(s) => {
            let _ = write!(key, "s{}:", s.len());
            key.push_str(s);
        }
        other => {
            let _ = write!(key, "v{}", other);
        }
    }
    key.push('\u{1f}');
}

#[derive(Debug, Clone, Copy)]
enum AggKind {
    CountAll,
    Count,
    Sum,
    Avg,
    Min,
    Max,
}

#[derive(Debug, Clone)]
enum Plan {
    /// Plain column: first value in the group (eKuiper non-aggregate field).
    First { name: String, expr: Expr },
    Agg {
        name: String,
        kind: AggKind,
        arg: Option<Expr>,
    },
}

#[derive(Debug, Clone)]
enum Acc {
    Count(i64),
    Sum {
        int: i64,
        float: f64,
        int_ok: bool,
        any: bool,
    },
    Avg {
        sum: f64,
        n: u64,
    },
    Extreme(Option<Value>),
}

impl Acc {
    fn new(kind: AggKind) -> Self {
        match kind {
            AggKind::CountAll | AggKind::Count => Acc::Count(0),
            AggKind::Sum => Acc::Sum {
                int: 0,
                float: 0.0,
                int_ok: true,
                any: false,
            },
            AggKind::Avg => Acc::Avg { sum: 0.0, n: 0 },
            AggKind::Min | AggKind::Max => Acc::Extreme(None),
        }
    }

    /// Mirrors `agg_count/sum/avg/min/max` exactly, one row at a time.
    fn update(&mut self, kind: AggKind, value: Option<&Value>) {
        match (self, kind) {
            (Acc::Count(n), AggKind::CountAll) => *n += 1,
            (Acc::Count(n), _) => {
                if value.is_some_and(|v| !v.is_null()) {
                    *n += 1;
                }
            }
            (
                Acc::Sum {
                    int,
                    float,
                    int_ok,
                    any,
                },
                _,
            ) => {
                let Some(v) = value.filter(|v| v.is_number()) else {
                    return;
                };
                *any = true;
                *float += v.as_f64().unwrap_or(0.0);
                if *int_ok {
                    match v.as_i64().and_then(|i| int.checked_add(i)) {
                        Some(next) => *int = next,
                        None => *int_ok = false,
                    }
                }
            }
            (Acc::Avg { sum, n }, _) => {
                if let Some(v) = value.filter(|v| v.is_number()) {
                    *sum += v.as_f64().unwrap_or(0.0);
                    *n += 1;
                }
            }
            (Acc::Extreme(best), kind) => {
                let Some(v) = value.filter(|v| v.is_number()) else {
                    return;
                };
                let wanted = match kind {
                    AggKind::Min => std::cmp::Ordering::Less,
                    _ => std::cmp::Ordering::Greater,
                };
                match best {
                    None => *best = Some(v.clone()),
                    Some(current) => {
                        if Evaluator::compare_values(v, current) == Some(wanted) {
                            *best = Some(v.clone());
                        }
                    }
                }
            }
        }
    }

    fn finish(self) -> Value {
        match self {
            Acc::Count(n) => Value::from(n),
            Acc::Sum {
                int,
                float,
                int_ok,
                any,
            } => {
                if !any {
                    Value::Null
                } else if int_ok {
                    Value::from(int)
                } else {
                    serde_json::json!(float)
                }
            }
            Acc::Avg { sum, n } => {
                if n == 0 {
                    Value::Null
                } else {
                    serde_json::json!(sum / (n as f64))
                }
            }
            Acc::Extreme(best) => best.unwrap_or(Value::Null),
        }
    }
}

struct Group {
    group_values: Vec<Value>,
    firsts: Vec<Value>,
    accs: Vec<Acc>,
}

/// Row-free window state for statements whose projection is only group
/// columns, plain columns and `count/sum/avg/min/max` of simple arguments.
/// Produces exactly what [`Evaluator::eval_window`] produces for the same rows.
pub struct IncrementalWindow {
    where_clause: Option<Expr>,
    group_by: Vec<Expr>,
    group_names: Vec<String>,
    plans: Vec<Plan>,
    order_stmt: SelectStmt,
    index: HashMap<String, usize>,
    groups: Vec<Group>,
    key: String,
}

impl IncrementalWindow {
    /// Builds the incremental plan, or `None` when the statement needs rows
    /// (joins, `HAVING`, `UNION`, other aggregates or expressions).
    pub fn try_new(stmt: &SelectStmt) -> Option<Self> {
        if stmt.set_op.is_some()
            || !stmt.joins.is_empty()
            || stmt.having.is_some()
            || !Evaluator::is_grouped_window(stmt)
            || stmt.group_by.iter().any(Evaluator::contains_aggregate)
        {
            return None;
        }
        let mut plans = Vec::with_capacity(stmt.fields.len());
        for (idx, field) in stmt.fields.iter().enumerate() {
            let alias = stmt.field_aliases.get(idx).and_then(|a| a.clone());
            let plan = match field {
                Expr::Identifier(name) => Plan::First {
                    name: alias.unwrap_or_else(|| name.clone()),
                    expr: field.clone(),
                },
                Expr::FieldAccess {
                    parent,
                    field: leaf,
                } => {
                    if Evaluator::contains_aggregate(parent) {
                        return None;
                    }
                    Plan::First {
                        name: alias.unwrap_or_else(|| leaf.clone()),
                        expr: field.clone(),
                    }
                }
                Expr::Call { name, args } => {
                    let lower = name.to_ascii_lowercase();
                    if args.len() != 1 || Evaluator::contains_aggregate(&args[0]) {
                        return None;
                    }
                    let wildcard = matches!(args[0], Expr::Wildcard);
                    let kind = match (lower.as_str(), wildcard) {
                        ("count", true) => AggKind::CountAll,
                        ("count", false) => AggKind::Count,
                        ("sum", false) => AggKind::Sum,
                        ("avg", false) => AggKind::Avg,
                        ("min", false) => AggKind::Min,
                        ("max", false) => AggKind::Max,
                        _ => return None,
                    };
                    Plan::Agg {
                        name: alias.unwrap_or_else(|| Evaluator::column_name(field, idx)),
                        kind,
                        arg: (!wildcard).then(|| args[0].clone()),
                    }
                }
                _ => return None,
            };
            plans.push(plan);
        }
        let group_names = stmt
            .group_by
            .iter()
            .enumerate()
            .map(|(idx, g)| match g {
                Expr::Identifier(name) => name.clone(),
                Expr::FieldAccess { field: leaf, .. } => leaf.clone(),
                _ => Evaluator::column_name(g, idx),
            })
            .collect();
        Some(Self {
            where_clause: stmt.where_clause.clone(),
            group_by: stmt.group_by.clone(),
            group_names,
            plans,
            order_stmt: stmt.clone(),
            index: HashMap::new(),
            groups: Vec::new(),
            key: String::new(),
        })
    }

    pub fn is_empty(&self) -> bool {
        self.groups.is_empty()
    }

    /// Number of open groups.
    pub fn group_count(&self) -> usize {
        self.groups.len()
    }

    /// Accumulates one input row. Returns `false` when `WHERE` rejected it.
    pub fn push(&mut self, row: &Row) -> bool {
        if let Some(cond) = &self.where_clause {
            if !Evaluator::eval_bool(cond, row) {
                return false;
            }
        }
        self.key.clear();
        for expr in &self.group_by {
            write_group_key(&mut self.key, &Evaluator::group_value(expr, row));
        }
        let gi = match self.index.get(self.key.as_str()) {
            Some(&i) => i,
            None => {
                let group = Group {
                    group_values: self
                        .group_by
                        .iter()
                        .map(|g| Evaluator::group_value(g, row).into_owned())
                        .collect(),
                    firsts: self
                        .plans
                        .iter()
                        .map(|p| match p {
                            Plan::First { expr, .. } => {
                                Evaluator::group_value(expr, row).into_owned()
                            }
                            Plan::Agg { .. } => Value::Null,
                        })
                        .collect(),
                    accs: self
                        .plans
                        .iter()
                        .map(|p| match p {
                            Plan::Agg { kind, .. } => Acc::new(*kind),
                            Plan::First { .. } => Acc::Count(0),
                        })
                        .collect(),
                };
                self.index.insert(self.key.clone(), self.groups.len());
                self.groups.push(group);
                self.groups.len() - 1
            }
        };
        let group = &mut self.groups[gi];
        for (plan, acc) in self.plans.iter().zip(group.accs.iter_mut()) {
            if let Plan::Agg { kind, arg, .. } = plan {
                match arg {
                    None => acc.update(*kind, None),
                    Some(expr) => {
                        let v = Evaluator::group_value(expr, row);
                        acc.update(*kind, Some(v.as_ref()));
                    }
                }
            }
        }
        true
    }

    /// Closes the window: output rows for every group, state reset.
    pub fn take(&mut self) -> Vec<Row> {
        self.index.clear();
        let groups = std::mem::take(&mut self.groups);
        let mut out = Vec::with_capacity(groups.len());
        for group in groups {
            let mut row: Row = HashMap::with_capacity(self.group_names.len() + self.plans.len());
            for (name, value) in self.group_names.iter().zip(group.group_values) {
                row.insert(name.clone(), value);
            }
            for ((plan, first), acc) in self.plans.iter().zip(group.firsts).zip(group.accs) {
                match plan {
                    Plan::First { name, .. } => {
                        row.entry(name.clone()).or_insert(first);
                    }
                    Plan::Agg { name, .. } => {
                        row.insert(name.clone(), acc.finish());
                    }
                }
            }
            out.push(row);
        }
        Evaluator::apply_order_limit(&self.order_stmt, &mut out);
        out
    }
}
