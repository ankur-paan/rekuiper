use std::collections::HashMap;
use serde_json::Value;

#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    Wildcard,
    Identifier(String),
    Literal(Value),
    BinaryOp {
        left: Box<Expr>,
        op: BinaryOperator,
        right: Box<Expr>,
    },
    UnaryOp {
        op: UnaryOperator,
        expr: Box<Expr>,
    },
    Between {
        expr: Box<Expr>,
        low: Box<Expr>,
        high: Box<Expr>,
        negated: bool,
    },
    InList {
        expr: Box<Expr>,
        list: Vec<Expr>,
        negated: bool,
    },
    IsNull {
        expr: Box<Expr>,
        negated: bool,
    },
    FieldAccess {
        parent: Box<Expr>,
        field: String,
    },
    Call {
        name: String,
        args: Vec<Expr>,
    },
    Case {
        operand: Option<Box<Expr>>,
        when_clauses: Vec<(Expr, Expr)>,
        else_clause: Option<Box<Expr>>,
    },
    Over {
        call: Box<Expr>,
        partition_by: Option<Box<Expr>>,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub enum BinaryOperator {
    Eq,
    Neq,
    Lt,
    Lte,
    Gt,
    Gte,
    And,
    Or,
    Add,
    Sub,
    Mul,
    Div,
    Mod,
    Like,
}

#[derive(Debug, Clone, PartialEq)]
pub enum UnaryOperator {
    Not,
    Neg,
}

#[derive(Debug, Clone, PartialEq)]
pub enum TimeUnit {
    Dd,
    Hh,
    Mi,
    Ss,
    Ms,
}

#[derive(Debug, Clone, PartialEq)]
pub enum WindowDef {
    TumblingTime { unit: TimeUnit, length: u64 },
    HoppingTime { unit: TimeUnit, length: u64, interval: u64 },
    SlidingTime { unit: TimeUnit, length: u64 },
    Count { size: usize, interval: Option<usize> },
}

#[derive(Debug, Clone, PartialEq)]
pub enum SortOrder {
    Asc,
    Desc,
}

#[derive(Debug, Clone, PartialEq)]
pub struct OrderByItem {
    pub expr: Expr,
    pub order: SortOrder,
}

#[derive(Debug, Clone, PartialEq)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
    Cross,
}

#[derive(Debug, Clone, PartialEq)]
pub struct JoinClause {
    pub join_type: JoinType,
    pub target: String,
    pub on: Option<Expr>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SelectStmt {
    pub fields: Vec<Expr>,
    /// Output alias per field (`None` when the field has no `AS alias`).
    pub field_aliases: Vec<Option<String>>,
    pub from: String,
    pub joins: Vec<JoinClause>,
    pub where_clause: Option<Expr>,
    pub group_by: Vec<Expr>,
    pub window: Option<WindowDef>,
    pub having: Option<Expr>,
    pub order_by: Vec<OrderByItem>,
    pub limit: Option<usize>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CreateStreamStmt {
    pub name: String,
    pub options: HashMap<String, String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CreateTableStmt {
    pub name: String,
    pub options: HashMap<String, String>,
}
