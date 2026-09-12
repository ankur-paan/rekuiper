use serde_json::Value;
use std::collections::HashMap;

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
    /// Postfix index: `arr[0]`, `arr[-1]`, `obj["k"]`. Zero-based for
    /// arrays (negative counts back from the end); a string index on an
    /// object is a field lookup. Mirrors
    /// https://ekuiper.org/docs/en/latest/sqls/json_expr.html#index-expression.
    Index {
        base: Box<Expr>,
        index: Box<Expr>,
    },
    /// Postfix slice: `arr[from:to)` (end-exclusive, negatives from the
    /// end, omitted bound means array start/end). Mirrors
    /// https://ekuiper.org/docs/en/latest/sqls/json_expr.html#slicing.
    Slice {
        base: Box<Expr>,
        lo: Option<Box<Expr>>,
        hi: Option<Box<Expr>>,
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
    TumblingTime {
        unit: TimeUnit,
        length: u64,
    },
    HoppingTime {
        unit: TimeUnit,
        length: u64,
        interval: u64,
    },
    SlidingTime {
        unit: TimeUnit,
        length: u64,
        delay: Option<u64>,
    },
    Count {
        size: usize,
        interval: Option<usize>,
    },
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
    /// Optional `AS alias` (or bare alias) for the join target; qualified
    /// references may use either the target name or the alias.
    pub alias: Option<String>,
    pub on: Option<Expr>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum SetOp {
    Union,
    UnionAll,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SelectStmt {
    pub fields: Vec<Expr>,
    /// Output alias per field (`None` when the field has no `AS alias`).
    pub field_aliases: Vec<Option<String>>,
    pub from: String,
    /// Optional `AS alias` (or bare alias) for the FROM source.
    pub from_alias: Option<String>,
    pub joins: Vec<JoinClause>,
    pub where_clause: Option<Expr>,
    pub group_by: Vec<Expr>,
    pub window: Option<WindowDef>,
    pub having: Option<Expr>,
    pub order_by: Vec<OrderByItem>,
    pub limit: Option<usize>,
    pub set_op: Option<(SetOp, Box<SelectStmt>)>,
}

/// One parsed `CREATE STREAM/TABLE` column: the column name plus its
/// declared data type, lowercased (`bigint`, `string`, ...).
#[derive(Debug, Clone, PartialEq)]
pub struct StreamColumn {
    pub name: String,
    pub data_type: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CreateStreamStmt {
    pub name: String,
    pub fields: Vec<StreamColumn>,
    pub options: HashMap<String, String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CreateTableStmt {
    pub name: String,
    pub fields: Vec<StreamColumn>,
    pub options: HashMap<String, String>,
}
