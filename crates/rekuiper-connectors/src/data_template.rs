//! Go template (`text/template`) engine for sink `dataTemplate` rendering.
//!
//! Supports:
//! - Plain and nested field access: `{{.id}}`, `{{.obj.k}}`, with `<no value>` for missing fields.
//! - Functions: `json .` / `toJson .`, `base64 .` / `b64enc .`, `printf "%.2f" .temp`,
//!   `index . "dev"`, `upper`, `lower`, `trim`, `add`, `sub`, `mul`, `div`, `len`, `default`.
//! - Comparison operators: `gt`, `lt`, `eq`, `ne`, `ge`, `le`.
//! - Control structures: `{{if ...}} ... {{else if ...}} ... {{else}} ... {{end}}`
//!   and `{{range ...}} ... {{end}}`.
//! - Variable assignments: `{{$len := len .values}}`, `{{range $index, $ele := .values}}`.
//! - Pipelines: `{{.dev | upper}}`.
//! - Upfront syntax validation (`validate_data_template`).

use serde_json::Value;
use std::collections::BTreeMap;
use std::collections::HashMap;

/// Representation of a value during template evaluation.
#[derive(Debug, Clone, PartialEq)]
pub enum TemplateValue {
    NoValue,
    Nil,
    Bool(bool),
    Int(i64),
    Float(f64),
    String(String),
    Array(Vec<TemplateValue>),
    Map(BTreeMap<String, TemplateValue>),
}

impl TemplateValue {
    pub fn from_json(val: &Value) -> Self {
        match val {
            Value::Null => TemplateValue::Nil,
            Value::Bool(b) => TemplateValue::Bool(*b),
            Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    TemplateValue::Int(i)
                } else if let Some(u) = n.as_u64() {
                    TemplateValue::Int(u as i64)
                } else {
                    TemplateValue::Float(n.as_f64().unwrap_or(0.0))
                }
            }
            Value::String(s) => TemplateValue::String(s.clone()),
            Value::Array(arr) => {
                TemplateValue::Array(arr.iter().map(TemplateValue::from_json).collect())
            }
            Value::Object(map) => {
                let mut btree = BTreeMap::new();
                for (k, v) in map {
                    if !k.starts_with("__") {
                        btree.insert(k.clone(), TemplateValue::from_json(v));
                    }
                }
                TemplateValue::Map(btree)
            }
        }
    }

    pub fn to_json(&self) -> Value {
        match self {
            TemplateValue::NoValue | TemplateValue::Nil => Value::Null,
            TemplateValue::Bool(b) => Value::Bool(*b),
            TemplateValue::Int(i) => Value::Number((*i).into()),
            TemplateValue::Float(f) => serde_json::Number::from_f64(*f)
                .map(Value::Number)
                .unwrap_or(Value::Null),
            TemplateValue::String(s) => Value::String(s.clone()),
            TemplateValue::Array(arr) => Value::Array(arr.iter().map(|v| v.to_json()).collect()),
            TemplateValue::Map(map) => {
                let mut obj = serde_json::Map::new();
                for (k, v) in map {
                    obj.insert(k.clone(), v.to_json());
                }
                Value::Object(obj)
            }
        }
    }

    pub fn is_truthy(&self) -> bool {
        match self {
            TemplateValue::NoValue | TemplateValue::Nil => false,
            TemplateValue::Bool(b) => *b,
            TemplateValue::Int(i) => *i != 0,
            TemplateValue::Float(f) => *f != 0.0,
            TemplateValue::String(s) => !s.is_empty(),
            TemplateValue::Array(arr) => !arr.is_empty(),
            TemplateValue::Map(map) => !map.is_empty(),
        }
    }

    pub fn render(&self, out: &mut String) {
        match self {
            TemplateValue::NoValue | TemplateValue::Nil => out.push_str("<no value>"),
            TemplateValue::Bool(b) => out.push_str(if *b { "true" } else { "false" }),
            TemplateValue::Int(i) => out.push_str(&i.to_string()),
            TemplateValue::Float(f) => {
                if f.fract() == 0.0 && f.abs() < 1e15 {
                    out.push_str(&format!("{:.0}", f));
                } else {
                    out.push_str(&f.to_string());
                }
            }
            TemplateValue::String(s) => out.push_str(s),
            TemplateValue::Array(arr) => {
                out.push('[');
                for (i, v) in arr.iter().enumerate() {
                    if i > 0 {
                        out.push(' ');
                    }
                    v.render(out);
                }
                out.push(']');
            }
            TemplateValue::Map(map) => {
                out.push_str("map[");
                for (i, (k, v)) in map.iter().enumerate() {
                    if i > 0 {
                        out.push(' ');
                    }
                    out.push_str(k);
                    out.push(':');
                    v.render(out);
                }
                out.push(']');
            }
        }
    }
}

// ---------------------------------------------------------------------------
// AST Definitions
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
enum Node {
    Text(String),
    Action {
        assign_vars: Option<Vec<String>>,
        pipeline: Pipeline,
    },
    If {
        branches: Vec<(Pipeline, Vec<Node>)>,
        else_branch: Option<Vec<Node>>,
    },
    Range {
        vars: Option<(String, Option<String>)>,
        pipeline: Pipeline,
        body: Vec<Node>,
        else_branch: Option<Vec<Node>>,
    },
}

#[derive(Debug, Clone)]
struct Pipeline {
    commands: Vec<Command>,
}

#[derive(Debug, Clone)]
struct Command {
    terms: Vec<Term>,
}

#[derive(Debug, Clone)]
enum Term {
    FieldPath { base: PathBase, fields: Vec<String> },
    StringLit(String),
    IntLit(i64),
    FloatLit(f64),
    BoolLit(bool),
    NilLit,
    Ident(String),
    SubExpr(Pipeline),
}

#[derive(Debug, Clone)]
enum PathBase {
    Dot,
    Var(String),
}

// ---------------------------------------------------------------------------
// Lexer
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
enum Token {
    Dot,
    DotField(Vec<String>),
    VarField(String, Vec<String>),
    StringLit(String),
    IntLit(i64),
    FloatLit(f64),
    BoolLit(bool),
    NilLit,
    Ident(String),
    Pipe,
    Assign,
    Comma,
    LParen,
    RParen,
}

#[derive(Debug, Clone)]
enum RawItem {
    Text(String),
    Action(Vec<Token>),
}

fn tokenize_action(raw: &str) -> Result<Vec<Token>, String> {
    let mut tokens = Vec::new();
    let chars: Vec<char> = raw.chars().collect();
    let len = chars.len();
    let mut i = 0;

    while i < len {
        let ch = chars[i];
        if ch.is_whitespace() {
            i += 1;
            continue;
        }

        if ch == '|' {
            tokens.push(Token::Pipe);
            i += 1;
            continue;
        }

        if ch == ',' {
            tokens.push(Token::Comma);
            i += 1;
            continue;
        }

        if ch == '(' {
            tokens.push(Token::LParen);
            i += 1;
            continue;
        }

        if ch == ')' {
            tokens.push(Token::RParen);
            i += 1;
            continue;
        }

        if ch == ':' && i + 1 < len && chars[i + 1] == '=' {
            tokens.push(Token::Assign);
            i += 2;
            continue;
        }

        if ch == '=' {
            tokens.push(Token::Assign);
            i += 1;
            continue;
        }

        // Strings
        if ch == '"' || ch == '`' {
            let quote = ch;
            i += 1;
            let mut s = String::new();
            let mut closed = false;
            while i < len {
                let c = chars[i];
                if c == quote {
                    closed = true;
                    i += 1;
                    break;
                }
                if quote == '"' && c == '\\' && i + 1 < len {
                    i += 1;
                    match chars[i] {
                        'n' => s.push('\n'),
                        't' => s.push('\t'),
                        'r' => s.push('\r'),
                        '\\' => s.push('\\'),
                        '"' => s.push('"'),
                        other => {
                            s.push('\\');
                            s.push(other);
                        }
                    }
                } else {
                    s.push(c);
                }
                i += 1;
            }
            if !closed {
                return Err("template: sink:1: unclosed quote".to_string());
            }
            tokens.push(Token::StringLit(s));
            continue;
        }

        // Numbers (could be negative if followed by digit)
        let is_neg_num =
            ch == '-' && i + 1 < len && (chars[i + 1].is_ascii_digit() || chars[i + 1] == '.');
        if ch.is_ascii_digit() || is_neg_num {
            let start = i;
            if ch == '-' {
                i += 1;
            }
            let mut has_dot = false;
            while i < len
                && (chars[i].is_ascii_digit()
                    || (!has_dot
                        && chars[i] == '.'
                        && i + 1 < len
                        && chars[i + 1].is_ascii_digit()))
            {
                if chars[i] == '.' {
                    has_dot = true;
                }
                i += 1;
            }
            let s: String = chars[start..i].iter().collect();
            if has_dot {
                if let Ok(f) = s.parse::<f64>() {
                    tokens.push(Token::FloatLit(f));
                    continue;
                }
            } else if let Ok(n) = s.parse::<i64>() {
                tokens.push(Token::IntLit(n));
                continue;
            }
        }

        // Dot path: . or .foo or .foo.bar
        if ch == '.' {
            i += 1;
            if i < len && (chars[i].is_alphanumeric() || chars[i] == '_') {
                let start = i;
                while i < len && (chars[i].is_alphanumeric() || chars[i] == '_' || chars[i] == '.')
                {
                    i += 1;
                }
                let path_str: String = chars[start..i].iter().collect();
                let fields: Vec<String> = path_str.split('.').map(|s| s.to_string()).collect();
                tokens.push(Token::DotField(fields));
            } else {
                tokens.push(Token::Dot);
            }
            continue;
        }

        // Variable: $ or $foo or $foo.bar
        if ch == '$' {
            i += 1;
            let start = i;
            while i < len && (chars[i].is_alphanumeric() || chars[i] == '_' || chars[i] == '.') {
                i += 1;
            }
            let full: String = chars[start..i].iter().collect();
            if full.is_empty() {
                tokens.push(Token::VarField("$".to_string(), vec![]));
            } else {
                let parts: Vec<&str> = full.split('.').collect();
                let var_name = format!("${}", parts[0]);
                let fields = parts[1..].iter().map(|s| s.to_string()).collect();
                tokens.push(Token::VarField(var_name, fields));
            }
            continue;
        }

        // Identifier or keyword
        if ch.is_alphabetic() || ch == '_' {
            let start = i;
            while i < len && (chars[i].is_alphanumeric() || chars[i] == '_') {
                i += 1;
            }
            let word: String = chars[start..i].iter().collect();
            match word.as_str() {
                "true" => tokens.push(Token::BoolLit(true)),
                "false" => tokens.push(Token::BoolLit(false)),
                "nil" => tokens.push(Token::NilLit),
                _ => tokens.push(Token::Ident(word)),
            }
            continue;
        }

        return Err(format!("template: sink:1: unexpected character '{}'", ch));
    }

    Ok(tokens)
}

fn scan_raw_items(template: &str) -> Result<Vec<RawItem>, String> {
    let mut items = Vec::new();
    let mut rest = template;

    while !rest.is_empty() {
        if let Some(start) = rest.find("{{") {
            let text_before = &rest[..start];
            let after_open = &rest[start + 2..];

            let (trim_prev, action_start) = if let Some(stripped) = after_open.strip_prefix('-') {
                (true, stripped)
            } else {
                (false, after_open)
            };

            let text_to_push = if trim_prev {
                text_before.trim_end()
            } else {
                text_before
            };
            if !text_to_push.is_empty() {
                items.push(RawItem::Text(text_to_push.to_string()));
            }

            // Find matching `}}` or `-}}` considering quotes
            let chars: Vec<char> = action_start.chars().collect();
            let mut i = 0;
            let mut end_pos = None;
            let mut trim_next = false;

            while i < chars.len() {
                let ch = chars[i];
                if ch == '"' || ch == '`' {
                    let quote = ch;
                    i += 1;
                    while i < chars.len() {
                        let c = chars[i];
                        if c == quote {
                            i += 1;
                            break;
                        }
                        if quote == '"' && c == '\\' && i + 1 < chars.len() {
                            i += 2;
                        } else {
                            i += 1;
                        }
                    }
                    continue;
                }

                if ch == '-' && i + 2 < chars.len() && chars[i + 1] == '}' && chars[i + 2] == '}' {
                    trim_next = true;
                    end_pos = Some((i, i + 3));
                    break;
                }

                if ch == '}' && i + 1 < chars.len() && chars[i + 1] == '}' {
                    end_pos = Some((i, i + 2));
                    break;
                }

                i += 1;
            }

            let (act_end, after_end) = match end_pos {
                Some(pos) => pos,
                None => return Err("template: sink:1: unclosed action".to_string()),
            };

            let act_end_byte: usize = chars[..act_end].iter().map(|c| c.len_utf8()).sum();
            let after_end_byte: usize = chars[..after_end].iter().map(|c| c.len_utf8()).sum();

            let action_content = &action_start[..act_end_byte];
            let action_tokens = tokenize_action(action_content)?;
            if !action_tokens.is_empty() {
                items.push(RawItem::Action(action_tokens));
            }

            let remaining = &action_start[after_end_byte..];
            if trim_next {
                rest = remaining.trim_start();
            } else {
                rest = remaining;
            }
        } else {
            items.push(RawItem::Text(rest.to_string()));
            break;
        }
    }

    Ok(items)
}

// ---------------------------------------------------------------------------
// Parser
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
enum ParsedAction {
    End,
    Else,
    ElseIf(Pipeline),
    If(Pipeline),
    Range {
        vars: Option<(String, Option<String>)>,
        pipeline: Pipeline,
    },
    Pipeline {
        assign_vars: Option<Vec<String>>,
        pipeline: Pipeline,
    },
}

fn parse_term(tokens: &[Token], idx: &mut usize) -> Result<Term, String> {
    if *idx >= tokens.len() {
        return Err("template: sink:1: unexpected end of action".to_string());
    }

    let tok = &tokens[*idx];
    *idx += 1;
    match tok {
        Token::Dot => Ok(Term::FieldPath {
            base: PathBase::Dot,
            fields: vec![],
        }),
        Token::DotField(fields) => Ok(Term::FieldPath {
            base: PathBase::Dot,
            fields: fields.clone(),
        }),
        Token::VarField(var, fields) => Ok(Term::FieldPath {
            base: PathBase::Var(var.clone()),
            fields: fields.clone(),
        }),
        Token::StringLit(s) => Ok(Term::StringLit(s.clone())),
        Token::IntLit(i) => Ok(Term::IntLit(*i)),
        Token::FloatLit(f) => Ok(Term::FloatLit(*f)),
        Token::BoolLit(b) => Ok(Term::BoolLit(*b)),
        Token::NilLit => Ok(Term::NilLit),
        Token::Ident(s) => Ok(Term::Ident(s.clone())),
        Token::LParen => {
            let start = *idx;
            let mut depth = 1;
            while *idx < tokens.len() {
                if tokens[*idx] == Token::LParen {
                    depth += 1;
                } else if tokens[*idx] == Token::RParen {
                    depth -= 1;
                    if depth == 0 {
                        break;
                    }
                }
                *idx += 1;
            }
            if *idx >= tokens.len() || tokens[*idx] != Token::RParen {
                return Err("template: sink:1: unclosed parenthesis".to_string());
            }
            let sub_slice = &tokens[start..*idx];
            *idx += 1; // consume RParen
            let sub_pipeline = parse_pipeline_tokens(sub_slice)?;
            Ok(Term::SubExpr(sub_pipeline))
        }
        _ => Err("template: sink:1: syntax error in expression".to_string()),
    }
}

fn parse_pipeline_tokens(tokens: &[Token]) -> Result<Pipeline, String> {
    let mut commands = Vec::new();
    let mut idx = 0;

    while idx < tokens.len() {
        let mut terms = Vec::new();
        while idx < tokens.len() && tokens[idx] != Token::Pipe {
            terms.push(parse_term(tokens, &mut idx)?);
        }
        if terms.is_empty() {
            return Err("template: sink:1: empty command in pipeline".to_string());
        }
        commands.push(Command { terms });
        if idx < tokens.len() && tokens[idx] == Token::Pipe {
            idx += 1; // consume pipe
        }
    }

    if commands.is_empty() {
        return Err("template: sink:1: empty pipeline".to_string());
    }
    Ok(Pipeline { commands })
}

fn parse_action_tokens(tokens: &[Token]) -> Result<ParsedAction, String> {
    if tokens.is_empty() {
        return Err("template: sink:1: empty action".to_string());
    }

    if let Token::Ident(ref id) = tokens[0] {
        if id == "end" {
            return Ok(ParsedAction::End);
        }
        if id == "else" {
            if tokens.len() > 1 {
                if let Token::Ident(ref next_id) = tokens[1] {
                    if next_id == "if" {
                        let pipe = parse_pipeline_tokens(&tokens[2..])?;
                        return Ok(ParsedAction::ElseIf(pipe));
                    }
                }
            }
            return Ok(ParsedAction::Else);
        }
        if id == "if" {
            let pipe = parse_pipeline_tokens(&tokens[1..])?;
            return Ok(ParsedAction::If(pipe));
        }
        if id == "range" {
            // Check for assignment: range $i, $v := pipeline or range $v := pipeline
            if let Some(assign_idx) = tokens.iter().position(|t| *t == Token::Assign) {
                let var_tokens = &tokens[1..assign_idx];
                let mut vars = Vec::new();
                for vt in var_tokens {
                    if let Token::VarField(v, _) = vt {
                        vars.push(v.clone());
                    }
                }
                let pipeline = parse_pipeline_tokens(&tokens[assign_idx + 1..])?;
                let var_pair = match vars.len() {
                    1 => Some((vars[0].clone(), None)),
                    2 => Some((vars[0].clone(), Some(vars[1].clone()))),
                    _ => None,
                };
                return Ok(ParsedAction::Range {
                    vars: var_pair,
                    pipeline,
                });
            } else {
                let pipeline = parse_pipeline_tokens(&tokens[1..])?;
                return Ok(ParsedAction::Range {
                    vars: None,
                    pipeline,
                });
            }
        }
    }

    // Check for variable assignment: $var := pipeline or $v1, $v2 := pipeline
    if let Some(assign_idx) = tokens.iter().position(|t| *t == Token::Assign) {
        let left = &tokens[..assign_idx];
        let mut vars = Vec::new();
        for lt in left {
            if let Token::VarField(v, _) = lt {
                vars.push(v.clone());
            }
        }
        if !vars.is_empty() {
            let pipeline = parse_pipeline_tokens(&tokens[assign_idx + 1..])?;
            return Ok(ParsedAction::Pipeline {
                assign_vars: Some(vars),
                pipeline,
            });
        }
    }

    let pipeline = parse_pipeline_tokens(tokens)?;
    Ok(ParsedAction::Pipeline {
        assign_vars: None,
        pipeline,
    })
}

fn parse_block(
    items: &[RawItem],
    idx: &mut usize,
    stop_at_else_or_end: bool,
) -> Result<Vec<Node>, String> {
    let mut nodes = Vec::new();

    while *idx < items.len() {
        match &items[*idx] {
            RawItem::Text(s) => {
                nodes.push(Node::Text(s.clone()));
                *idx += 1;
            }
            RawItem::Action(tokens) => {
                let action = parse_action_tokens(tokens)?;
                match action {
                    ParsedAction::End | ParsedAction::Else | ParsedAction::ElseIf(_) => {
                        if stop_at_else_or_end {
                            return Ok(nodes);
                        } else {
                            return Err(match action {
                                ParsedAction::End => "template: sink:1: unexpected {{end}}",
                                ParsedAction::Else => "template: sink:1: unexpected {{else}}",
                                _ => "template: sink:1: unexpected {{else if}}",
                            }
                            .to_string());
                        }
                    }
                    ParsedAction::If(cond) => {
                        *idx += 1;
                        let mut branches = vec![(cond, Vec::new())];
                        let mut else_branch = None;

                        let then_body = parse_block(items, idx, true)?;
                        branches[0].1 = then_body;

                        loop {
                            if *idx >= items.len() {
                                return Err("template: sink:1: unexpected EOF".to_string());
                            }
                            match &items[*idx] {
                                RawItem::Action(next_tokens) => {
                                    let next_act = parse_action_tokens(next_tokens)?;
                                    match next_act {
                                        ParsedAction::ElseIf(elif_cond) => {
                                            *idx += 1;
                                            let elif_body = parse_block(items, idx, true)?;
                                            branches.push((elif_cond, elif_body));
                                        }
                                        ParsedAction::Else => {
                                            *idx += 1;
                                            let eb = parse_block(items, idx, true)?;
                                            else_branch = Some(eb);
                                            if *idx >= items.len() {
                                                return Err(
                                                    "template: sink:1: unexpected EOF".to_string()
                                                );
                                            }
                                            if let RawItem::Action(end_tokens) = &items[*idx] {
                                                if matches!(
                                                    parse_action_tokens(end_tokens),
                                                    Ok(ParsedAction::End)
                                                ) {
                                                    *idx += 1;
                                                    break;
                                                }
                                            }
                                            return Err(
                                                "template: sink:1: unexpected EOF".to_string()
                                            );
                                        }
                                        ParsedAction::End => {
                                            *idx += 1;
                                            break;
                                        }
                                        _ => {
                                            return Err(
                                                "template: sink:1: syntax error in control block"
                                                    .to_string(),
                                            )
                                        }
                                    }
                                }
                                _ => return Err("template: sink:1: unexpected EOF".to_string()),
                            }
                        }
                        nodes.push(Node::If {
                            branches,
                            else_branch,
                        });
                    }
                    ParsedAction::Range { vars, pipeline } => {
                        *idx += 1;
                        let body = parse_block(items, idx, true)?;
                        let mut else_branch = None;

                        if *idx >= items.len() {
                            return Err("template: sink:1: unexpected EOF".to_string());
                        }
                        if let RawItem::Action(next_tokens) = &items[*idx] {
                            let next_act = parse_action_tokens(next_tokens)?;
                            match next_act {
                                ParsedAction::Else => {
                                    *idx += 1;
                                    let eb = parse_block(items, idx, true)?;
                                    else_branch = Some(eb);
                                    if *idx >= items.len() {
                                        return Err("template: sink:1: unexpected EOF".to_string());
                                    }
                                    if let RawItem::Action(end_tokens) = &items[*idx] {
                                        if matches!(
                                            parse_action_tokens(end_tokens),
                                            Ok(ParsedAction::End)
                                        ) {
                                            *idx += 1;
                                        } else {
                                            return Err(
                                                "template: sink:1: unexpected EOF".to_string()
                                            );
                                        }
                                    } else {
                                        return Err("template: sink:1: unexpected EOF".to_string());
                                    }
                                }
                                ParsedAction::End => {
                                    *idx += 1;
                                }
                                _ => {
                                    return Err(
                                        "template: sink:1: syntax error in range block".to_string()
                                    )
                                }
                            }
                        } else {
                            return Err("template: sink:1: unexpected EOF".to_string());
                        }
                        nodes.push(Node::Range {
                            vars,
                            pipeline,
                            body,
                            else_branch,
                        });
                    }
                    ParsedAction::Pipeline {
                        assign_vars,
                        pipeline,
                    } => {
                        *idx += 1;
                        nodes.push(Node::Action {
                            assign_vars,
                            pipeline,
                        });
                    }
                }
            }
        }
    }

    if stop_at_else_or_end {
        return Err("template: sink:1: unexpected EOF".to_string());
    }
    Ok(nodes)
}

fn parse_template(template: &str) -> Result<Vec<Node>, String> {
    let raw_items = scan_raw_items(template)?;
    let mut idx = 0;
    parse_block(&raw_items, &mut idx, false)
}

/// Upfront syntax validation for a sink `dataTemplate`.
pub fn validate_data_template(template: &str) -> Result<(), String> {
    parse_template(template).map(|_| ())
}

// ---------------------------------------------------------------------------
// Evaluator
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct ExecContext<'a> {
    dot: &'a TemplateValue,
    root: &'a TemplateValue,
    vars: HashMap<String, TemplateValue>,
}

fn eval_node(node: &Node, ctx: &mut ExecContext, out: &mut String) -> Result<(), String> {
    match node {
        Node::Text(s) => {
            out.push_str(s);
            Ok(())
        }
        Node::Action {
            assign_vars,
            pipeline,
        } => {
            let val = eval_pipeline(pipeline, ctx)?;
            if let Some(names) = assign_vars {
                if names.len() == 1 {
                    ctx.vars.insert(names[0].clone(), val);
                }
            } else {
                val.render(out);
            }
            Ok(())
        }
        Node::If {
            branches,
            else_branch,
        } => {
            for (cond_pipe, body) in branches {
                let cond_val = eval_pipeline(cond_pipe, ctx)?;
                if cond_val.is_truthy() {
                    for n in body {
                        eval_node(n, ctx, out)?;
                    }
                    return Ok(());
                }
            }
            if let Some(eb) = else_branch {
                for n in eb {
                    eval_node(n, ctx, out)?;
                }
            }
            Ok(())
        }
        Node::Range {
            vars,
            pipeline,
            body,
            else_branch,
        } => {
            let target = eval_pipeline(pipeline, ctx)?;
            match target {
                TemplateValue::Array(items) if !items.is_empty() => {
                    for (i, elem) in items.iter().enumerate() {
                        let mut inner_ctx = ctx.clone();
                        inner_ctx.dot = elem;
                        if let Some((v1, v2)) = vars {
                            if let Some(v2_name) = v2 {
                                inner_ctx
                                    .vars
                                    .insert(v1.clone(), TemplateValue::Int(i as i64));
                                inner_ctx.vars.insert(v2_name.clone(), elem.clone());
                            } else {
                                inner_ctx.vars.insert(v1.clone(), elem.clone());
                            }
                        }
                        for n in body {
                            eval_node(n, &mut inner_ctx, out)?;
                        }
                    }
                    Ok(())
                }
                TemplateValue::Map(map) if !map.is_empty() => {
                    for (k, v) in map.iter() {
                        let mut inner_ctx = ctx.clone();
                        inner_ctx.dot = v;
                        if let Some((v1, v2)) = vars {
                            if let Some(v2_name) = v2 {
                                inner_ctx
                                    .vars
                                    .insert(v1.clone(), TemplateValue::String(k.clone()));
                                inner_ctx.vars.insert(v2_name.clone(), v.clone());
                            } else {
                                inner_ctx.vars.insert(v1.clone(), v.clone());
                            }
                        }
                        for n in body {
                            eval_node(n, &mut inner_ctx, out)?;
                        }
                    }
                    Ok(())
                }
                _ => {
                    if let Some(eb) = else_branch {
                        for n in eb {
                            eval_node(n, ctx, out)?;
                        }
                    }
                    Ok(())
                }
            }
        }
    }
}

fn eval_pipeline(pipeline: &Pipeline, ctx: &mut ExecContext) -> Result<TemplateValue, String> {
    let mut current_val = None;
    for cmd in &pipeline.commands {
        current_val = Some(eval_command(cmd, current_val, ctx)?);
    }
    Ok(current_val.unwrap_or(TemplateValue::NoValue))
}

fn eval_term(term: &Term, ctx: &mut ExecContext) -> Result<TemplateValue, String> {
    match term {
        Term::FieldPath { base, fields } => {
            let base_val = match base {
                PathBase::Dot => ctx.dot,
                PathBase::Var(name) => {
                    if name == "$" {
                        ctx.root
                    } else if let Some(v) = ctx.vars.get(name) {
                        v
                    } else {
                        &TemplateValue::NoValue
                    }
                }
            };
            let mut cur = base_val;
            for f in fields {
                match cur {
                    TemplateValue::Map(m) => {
                        if let Some(v) = m.get(f) {
                            cur = v;
                        } else {
                            return Ok(TemplateValue::NoValue);
                        }
                    }
                    _ => return Ok(TemplateValue::NoValue),
                }
            }
            Ok(cur.clone())
        }
        Term::StringLit(s) => Ok(TemplateValue::String(s.clone())),
        Term::IntLit(i) => Ok(TemplateValue::Int(*i)),
        Term::FloatLit(f) => Ok(TemplateValue::Float(*f)),
        Term::BoolLit(b) => Ok(TemplateValue::Bool(*b)),
        Term::NilLit => Ok(TemplateValue::Nil),
        Term::Ident(name) => {
            // Function or identifier with no arguments
            match name.as_str() {
                "json" | "toJson" => call_json(std::slice::from_ref(ctx.dot)),
                _ => Ok(TemplateValue::String(name.clone())),
            }
        }
        Term::SubExpr(pipe) => eval_pipeline(pipe, ctx),
    }
}

fn eval_command(
    cmd: &Command,
    piped_arg: Option<TemplateValue>,
    ctx: &mut ExecContext,
) -> Result<TemplateValue, String> {
    if cmd.terms.is_empty() {
        return Ok(TemplateValue::NoValue);
    }

    if cmd.terms.len() == 1 && piped_arg.is_none() {
        return eval_term(&cmd.terms[0], ctx);
    }

    // First term is function name or receiver
    let fn_name = match &cmd.terms[0] {
        Term::Ident(s) => s.clone(),
        Term::FieldPath { fields, .. } if !fields.is_empty() => fields.last().unwrap().clone(),
        _ => "".to_string(),
    };

    let mut args = Vec::new();
    for term in &cmd.terms[1..] {
        args.push(eval_term(term, ctx)?);
    }
    if let Some(piped) = piped_arg {
        args.push(piped);
    }

    call_function(&fn_name, &args, ctx)
}

fn call_function(
    name: &str,
    args: &[TemplateValue],
    ctx: &ExecContext,
) -> Result<TemplateValue, String> {
    match name {
        "json" | "toJson" => {
            let target = args.first().unwrap_or(ctx.dot);
            call_json(std::slice::from_ref(target))
        }
        "base64" | "b64enc" => {
            use base64::Engine;
            let target = args.first().unwrap_or(ctx.dot);
            let bytes = match target {
                TemplateValue::String(s) => s.as_bytes().to_vec(),
                TemplateValue::NoValue => vec![],
                other => {
                    let mut s = String::new();
                    other.render(&mut s);
                    s.into_bytes()
                }
            };
            Ok(TemplateValue::String(
                base64::prelude::BASE64_STANDARD.encode(bytes),
            ))
        }
        "b64dec" => {
            use base64::Engine;
            if let Some(TemplateValue::String(s)) = args.first() {
                let decoded = base64::prelude::BASE64_STANDARD
                    .decode(s.trim())
                    .map(|b| String::from_utf8_lossy(&b).to_string())
                    .unwrap_or_default();
                Ok(TemplateValue::String(decoded))
            } else {
                Ok(TemplateValue::String(String::new()))
            }
        }
        "printf" => {
            if args.is_empty() {
                return Ok(TemplateValue::String(String::new()));
            }
            let fmt_str = match &args[0] {
                TemplateValue::String(s) => s.clone(),
                other => {
                    let mut s = String::new();
                    other.render(&mut s);
                    s
                }
            };
            let formatted = format_printf(&fmt_str, &args[1..]);
            Ok(TemplateValue::String(formatted))
        }
        "index" => {
            if args.is_empty() {
                return Ok(TemplateValue::NoValue);
            }
            let mut cur = args[0].clone();
            for key_arg in &args[1..] {
                match (&cur, key_arg) {
                    (TemplateValue::Map(m), TemplateValue::String(k)) => {
                        if let Some(val) = m.get(k) {
                            cur = val.clone();
                        } else {
                            return Ok(TemplateValue::NoValue);
                        }
                    }
                    (TemplateValue::Array(arr), TemplateValue::Int(idx))
                        if *idx >= 0 && (*idx as usize) < arr.len() =>
                    {
                        cur = arr[*idx as usize].clone();
                    }
                    (TemplateValue::Array(_), TemplateValue::Int(_)) => {
                        return Ok(TemplateValue::NoValue);
                    }
                    _ => return Ok(TemplateValue::NoValue),
                }
            }
            Ok(cur)
        }
        "upper" => {
            let s = to_string_val(args.first());
            Ok(TemplateValue::String(s.to_uppercase()))
        }
        "lower" => {
            let s = to_string_val(args.first());
            Ok(TemplateValue::String(s.to_lowercase()))
        }
        "trim" => {
            let s = to_string_val(args.first());
            Ok(TemplateValue::String(s.trim().to_string()))
        }
        "trimPrefix" => {
            if args.len() >= 2 {
                let prefix = to_string_val(Some(&args[0]));
                let s = to_string_val(Some(&args[1]));
                Ok(TemplateValue::String(
                    s.strip_prefix(&prefix).unwrap_or(&s).to_string(),
                ))
            } else {
                Ok(TemplateValue::String(String::new()))
            }
        }
        "trimSuffix" => {
            if args.len() >= 2 {
                let suffix = to_string_val(Some(&args[0]));
                let s = to_string_val(Some(&args[1]));
                Ok(TemplateValue::String(
                    s.strip_suffix(&suffix).unwrap_or(&s).to_string(),
                ))
            } else {
                Ok(TemplateValue::String(String::new()))
            }
        }
        "replace" => {
            if args.len() >= 3 {
                let old = to_string_val(Some(&args[0]));
                let new = to_string_val(Some(&args[1]));
                let s = to_string_val(Some(&args[2]));
                Ok(TemplateValue::String(s.replace(&old, &new)))
            } else {
                Ok(TemplateValue::String(String::new()))
            }
        }
        "len" => {
            let len = match args.first() {
                Some(TemplateValue::Array(a)) => a.len() as i64,
                Some(TemplateValue::Map(m)) => m.len() as i64,
                Some(TemplateValue::String(s)) => s.chars().count() as i64,
                _ => 0,
            };
            Ok(TemplateValue::Int(len))
        }
        "add" => {
            let mut float_mode = false;
            let mut int_sum: i64 = 0;
            let mut float_sum: f64 = 0.0;
            for arg in args {
                match arg {
                    TemplateValue::Float(f) => {
                        if !float_mode {
                            float_mode = true;
                            float_sum = int_sum as f64;
                        }
                        float_sum += *f;
                    }
                    TemplateValue::Int(i) => {
                        if float_mode {
                            float_sum += *i as f64;
                        } else {
                            int_sum += *i;
                        }
                    }
                    _ => {}
                }
            }
            if float_mode {
                Ok(TemplateValue::Float(float_sum))
            } else {
                Ok(TemplateValue::Int(int_sum))
            }
        }
        "sub" => {
            let a = args.first();
            let b = args.get(1);
            match (a, b) {
                (Some(TemplateValue::Int(x)), Some(TemplateValue::Int(y))) => {
                    Ok(TemplateValue::Int(x - y))
                }
                _ => {
                    let x = to_f64_val(a);
                    let y = to_f64_val(b);
                    Ok(TemplateValue::Float(x - y))
                }
            }
        }
        "mul" => {
            let a = args.first();
            let b = args.get(1);
            match (a, b) {
                (Some(TemplateValue::Int(x)), Some(TemplateValue::Int(y))) => {
                    Ok(TemplateValue::Int(x * y))
                }
                _ => {
                    let x = to_f64_val(a);
                    let y = to_f64_val(b);
                    Ok(TemplateValue::Float(x * y))
                }
            }
        }
        "div" => {
            let a = to_f64_val(args.first());
            let b = to_f64_val(args.get(1));
            if b == 0.0 {
                Ok(TemplateValue::Float(0.0))
            } else {
                let res = a / b;
                if res.fract() == 0.0 {
                    Ok(TemplateValue::Int(res as i64))
                } else {
                    Ok(TemplateValue::Float(res))
                }
            }
        }
        "gt" => {
            let cmp = compare_vals(args.first(), args.get(1));
            Ok(TemplateValue::Bool(cmp > 0))
        }
        "ge" => {
            let cmp = compare_vals(args.first(), args.get(1));
            Ok(TemplateValue::Bool(cmp >= 0))
        }
        "lt" => {
            let cmp = compare_vals(args.first(), args.get(1));
            Ok(TemplateValue::Bool(cmp < 0))
        }
        "le" => {
            let cmp = compare_vals(args.first(), args.get(1));
            Ok(TemplateValue::Bool(cmp <= 0))
        }
        "eq" => {
            let cmp = compare_vals(args.first(), args.get(1));
            Ok(TemplateValue::Bool(cmp == 0))
        }
        "ne" => {
            let cmp = compare_vals(args.first(), args.get(1));
            Ok(TemplateValue::Bool(cmp != 0))
        }
        "default" => {
            let default_val = args.first().cloned().unwrap_or(TemplateValue::NoValue);
            let val = args.get(1).cloned().unwrap_or(TemplateValue::NoValue);
            if val.is_truthy() {
                Ok(val)
            } else {
                Ok(default_val)
            }
        }
        _ => Ok(args.first().cloned().unwrap_or(TemplateValue::NoValue)),
    }
}

fn call_json(args: &[TemplateValue]) -> Result<TemplateValue, String> {
    if let Some(target) = args.first() {
        let json_val = target.to_json();
        let s = serde_json::to_string(&json_val).unwrap_or_default();
        Ok(TemplateValue::String(s))
    } else {
        Ok(TemplateValue::String("null".to_string()))
    }
}

fn to_string_val(arg: Option<&TemplateValue>) -> String {
    match arg {
        Some(TemplateValue::String(s)) => s.clone(),
        Some(TemplateValue::NoValue) => "".to_string(),
        Some(other) => {
            let mut s = String::new();
            other.render(&mut s);
            s
        }
        None => "".to_string(),
    }
}

fn to_f64_val(arg: Option<&TemplateValue>) -> f64 {
    match arg {
        Some(TemplateValue::Float(f)) => *f,
        Some(TemplateValue::Int(i)) => *i as f64,
        Some(TemplateValue::String(s)) => s.parse::<f64>().unwrap_or(0.0),
        _ => 0.0,
    }
}

fn compare_vals(a: Option<&TemplateValue>, b: Option<&TemplateValue>) -> i32 {
    match (a, b) {
        (Some(TemplateValue::Int(x)), Some(TemplateValue::Int(y))) => x.cmp(y) as i32,
        (Some(TemplateValue::Float(x)), Some(TemplateValue::Float(y))) => {
            if x < y {
                -1
            } else if x > y {
                1
            } else {
                0
            }
        }
        (Some(TemplateValue::Int(x)), Some(TemplateValue::Float(y))) => {
            let fx = *x as f64;
            if fx < *y {
                -1
            } else if fx > *y {
                1
            } else {
                0
            }
        }
        (Some(TemplateValue::Float(x)), Some(TemplateValue::Int(y))) => {
            let fy = *y as f64;
            if *x < fy {
                -1
            } else if *x > fy {
                1
            } else {
                0
            }
        }
        (Some(TemplateValue::String(x)), Some(TemplateValue::String(y))) => x.cmp(y) as i32,
        (Some(TemplateValue::Bool(x)), Some(TemplateValue::Bool(y))) => x.cmp(y) as i32,
        (None, None)
        | (Some(TemplateValue::Nil), Some(TemplateValue::Nil))
        | (Some(TemplateValue::NoValue), Some(TemplateValue::NoValue)) => 0,
        _ => -1,
    }
}

fn format_printf(fmt: &str, args: &[TemplateValue]) -> String {
    let mut out = String::new();
    let mut chars = fmt.chars().peekable();
    let mut arg_idx = 0;

    while let Some(ch) = chars.next() {
        if ch != '%' {
            out.push(ch);
            continue;
        }
        if chars.peek() == Some(&'%') {
            chars.next();
            out.push('%');
            continue;
        }

        let mut width: Option<usize> = None;
        let mut precision: Option<usize> = None;
        let mut is_zero_padded = false;

        if chars.peek() == Some(&'0') {
            is_zero_padded = true;
            chars.next();
        }

        let mut w_digits = String::new();
        while let Some(&c) = chars.peek() {
            if c.is_ascii_digit() {
                w_digits.push(c);
                chars.next();
            } else {
                break;
            }
        }
        if !w_digits.is_empty() {
            width = w_digits.parse().ok();
        }

        if chars.peek() == Some(&'.') {
            chars.next();
            let mut p_digits = String::new();
            while let Some(&c) = chars.peek() {
                if c.is_ascii_digit() {
                    p_digits.push(c);
                    chars.next();
                } else {
                    break;
                }
            }
            precision = if p_digits.is_empty() {
                Some(0)
            } else {
                p_digits.parse().ok()
            };
        }

        let specifier = chars.next().unwrap_or('%');
        let arg = args.get(arg_idx);
        arg_idx += 1;

        match specifier {
            'f' | 'F' => {
                let num = match arg {
                    Some(TemplateValue::Float(f)) => *f,
                    Some(TemplateValue::Int(i)) => *i as f64,
                    Some(TemplateValue::String(s)) => s.parse::<f64>().unwrap_or(0.0),
                    _ => 0.0,
                };
                let formatted = if let Some(prec) = precision {
                    format!("{:.prec$}", num)
                } else {
                    format!("{:.6}", num)
                };
                pad(&mut out, &formatted, width, is_zero_padded);
            }
            'd' | 'i' => {
                let int_val = match arg {
                    Some(TemplateValue::Int(i)) => *i,
                    Some(TemplateValue::Float(f)) => *f as i64,
                    Some(TemplateValue::String(s)) => s.parse::<i64>().unwrap_or(0),
                    _ => 0,
                };
                let formatted = format!("{}", int_val);
                pad(&mut out, &formatted, width, is_zero_padded);
            }
            's' => {
                let s = match arg {
                    Some(TemplateValue::String(s)) => s.clone(),
                    Some(TemplateValue::NoValue) => "<no value>".to_string(),
                    Some(other) => {
                        let mut tmp = String::new();
                        other.render(&mut tmp);
                        tmp
                    }
                    None => "".to_string(),
                };
                let s = if let Some(prec) = precision {
                    s.chars().take(prec).collect()
                } else {
                    s
                };
                pad(&mut out, &s, width, false);
            }
            'v' => {
                let s = match arg {
                    Some(TemplateValue::String(s)) => s.clone(),
                    Some(TemplateValue::NoValue) => "<no value>".to_string(),
                    Some(other) => {
                        let mut tmp = String::new();
                        other.render(&mut tmp);
                        tmp
                    }
                    None => "<nil>".to_string(),
                };
                pad(&mut out, &s, width, false);
            }
            't' => {
                let b = match arg {
                    Some(TemplateValue::Bool(b)) => *b,
                    _ => false,
                };
                pad(&mut out, &b.to_string(), width, false);
            }
            'x' | 'X' => {
                let int_val = match arg {
                    Some(TemplateValue::Int(i)) => *i,
                    Some(TemplateValue::Float(f)) => *f as i64,
                    _ => 0,
                };
                let formatted = if specifier == 'X' {
                    format!("{:X}", int_val)
                } else {
                    format!("{:x}", int_val)
                };
                pad(&mut out, &formatted, width, is_zero_padded);
            }
            other => {
                out.push('%');
                out.push(other);
            }
        }
    }
    out
}

fn pad(out: &mut String, s: &str, width: Option<usize>, zero_padded: bool) {
    if let Some(w) = width {
        if s.len() < w {
            let pad_char = if zero_padded { '0' } else { ' ' };
            let diff = w - s.len();
            for _ in 0..diff {
                out.push(pad_char);
            }
        }
    }
    out.push_str(s);
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Render a sink `dataTemplate` against arbitrary JSON data (Object, Array, etc.).
pub fn apply_data_template_value(template: &str, data: &Value) -> String {
    let nodes = match parse_template(template) {
        Ok(n) => n,
        Err(_) => return template.to_string(),
    };

    let root_val = TemplateValue::from_json(data);
    let mut ctx = ExecContext {
        dot: &root_val,
        root: &root_val,
        vars: HashMap::new(),
    };

    let mut out = String::with_capacity(template.len());
    for node in &nodes {
        if eval_node(node, &mut ctx, &mut out).is_err() {
            return template.to_string();
        }
    }
    out
}

/// Render a sink `dataTemplate` against record map data.
pub fn apply_data_template(template: &str, data: &serde_json::Map<String, Value>) -> String {
    apply_data_template_value(template, &Value::Object(data.clone()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn make_issue19_data() -> Value {
        json!({
            "id": 0,
            "temp": 20.5,
            "dev": "d0"
        })
    }

    #[test]
    fn test_issue19_case1_plain_fields() {
        let data = make_issue19_data();
        assert_eq!(
            apply_data_template_value(r#"{"x":{{.id}},"t":"{{.dev}}"}"#, &data),
            r#"{"x":0,"t":"d0"}"#
        );
    }

    #[test]
    fn test_issue19_case2_json() {
        let data = make_issue19_data();
        assert_eq!(
            apply_data_template_value("{{json .}}", &data),
            r#"{"dev":"d0","id":0,"temp":20.5}"#
        );
        assert_eq!(
            apply_data_template_value("{{toJson .}}", &data),
            r#"{"dev":"d0","id":0,"temp":20.5}"#
        );
    }

    #[test]
    fn test_issue19_case3_printf() {
        let data = make_issue19_data();
        assert_eq!(
            apply_data_template_value(r#"{{printf "%.2f" .temp}}|{{.id}}"#, &data),
            "20.50|0"
        );
    }

    #[test]
    fn test_issue19_case4_conditionals() {
        let data = make_issue19_data();
        assert_eq!(
            apply_data_template_value("{{if gt .temp 21.6}}hot{{else}}cold{{end}}", &data),
            "cold"
        );
        let hot_data = json!({"temp": 25.0});
        assert_eq!(
            apply_data_template_value("{{if gt .temp 21.6}}hot{{else}}cold{{end}}", &hot_data),
            "hot"
        );
    }

    #[test]
    fn test_issue19_case5_range_array() {
        let data = json!([
            {"id": 0, "temp": 20.5, "dev": "d0"}
        ]);
        assert_eq!(
            apply_data_template_value("{{range .}}{{.id}};{{end}}", &data),
            "0;"
        );

        let data2 = json!([{"id": 0}, {"id": 1}, {"id": 2}]);
        assert_eq!(
            apply_data_template_value("{{range .}}{{.id}};{{end}}", &data2),
            "0;1;2;"
        );
    }

    #[test]
    fn test_issue19_case6_index() {
        let data = make_issue19_data();
        assert_eq!(
            apply_data_template_value(r#"{{index . "dev"}}"#, &data),
            "d0"
        );
    }

    #[test]
    fn test_issue19_case7_helpers() {
        let data = make_issue19_data();
        assert_eq!(
            apply_data_template_value(r#"{{upper .dev}}-{{add .id 1}}-{{trim " x "}}"#, &data),
            "D0-1-x"
        );
    }

    #[test]
    fn test_issue19_case8_base64() {
        let data = make_issue19_data();
        assert_eq!(apply_data_template_value("{{base64 .dev}}", &data), "ZDA=");
        assert_eq!(apply_data_template_value("{{b64enc .dev}}", &data), "ZDA=");
    }

    #[test]
    fn test_issue19_case9_nested_fields() {
        let data = json!({
            "obj": {
                "k": 0,
                "z": "v"
            }
        });
        assert_eq!(
            apply_data_template_value("{{.obj.k}}/{{.obj.z}}", &data),
            "0/v"
        );
    }

    #[test]
    fn test_issue19_case10_missing_fields() {
        let data = make_issue19_data();
        assert_eq!(
            apply_data_template_value("[{{.nosuch}}]", &data),
            "[<no value>]"
        );
    }

    #[test]
    fn test_issue19_case11_unclosed_validation() {
        let err = validate_data_template("{{.id");
        assert!(err.is_err());
        assert_eq!(err.unwrap_err(), "template: sink:1: unclosed action");

        let err2 = validate_data_template("{{if .temp}}hot");
        assert!(err2.is_err());
        assert_eq!(err2.unwrap_err(), "template: sink:1: unexpected EOF");

        let err3 = validate_data_template("{{end}}");
        assert!(err3.is_err());
        assert_eq!(err3.unwrap_err(), "template: sink:1: unexpected {{end}}");
    }

    #[test]
    fn test_pipeline_and_nested_subexpr() {
        let data = json!([{"ab": "hello1"}, {"ab": "hello2"}]);
        assert_eq!(
            apply_data_template_value(r#"{{json (index . 0)}}"#, &data),
            r#"{"ab":"hello1"}"#
        );
        let single = json!({"ab": "hello"});
        assert_eq!(
            apply_data_template_value("{{.ab | upper}}", &single),
            "HELLO"
        );
    }

    #[test]
    fn test_nested_array_iteration_with_vars() {
        let data = json!({
            "device_id": "1",
            "values": [
                {"temperature": 10.5},
                {"temperature": 20.3},
                {"temperature": 30.3}
            ]
        });
        let tpl = r#"{{$len := len .values}}{{$loopsize := add $len -1}}{"device_id": "{{.device_id}}", "description": [{{range $index, $ele := .values}}{{if le .temperature 25.0}}"fine"{{else if gt .temperature 25.0}}"high"{{end}}{{if eq $loopsize $index}}]{{else}},{{end}}{{end}}}"#;
        assert_eq!(
            apply_data_template_value(tpl, &data),
            r#"{"device_id": "1", "description": ["fine","fine","high"]}"#
        );
    }
}
