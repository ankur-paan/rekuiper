use crate::ast::{
    BinaryOperator, CreateStreamStmt, CreateTableStmt, Expr, JoinClause, JoinType, OrderByItem,
    SelectStmt, SetOp, SortOrder, TimeUnit, UnaryOperator, WindowDef,
};
use anyhow::{bail, Result};
use std::collections::HashMap;

pub struct Parser<'a> {
    input: &'a str,
    pos: usize,
}

impl<'a> Parser<'a> {
    pub fn new(input: &'a str) -> Self {
        Self { input, pos: 0 }
    }

    fn skip_whitespace(&mut self) {
        while self.pos < self.input.len() {
            let ch = self.input[self.pos..].chars().next().unwrap();
            if ch.is_whitespace() {
                self.pos += ch.len_utf8();
            } else {
                break;
            }
        }
    }

    fn peek_word(&self) -> Option<String> {
        let remaining = &self.input[self.pos..];
        let trimmed = remaining.trim_start();
        let end = trimmed
            .find(|c: char| !c.is_alphanumeric() && c != '_')
            .unwrap_or(trimmed.len());
        if end == 0 {
            None
        } else {
            Some(trimmed[..end].to_string())
        }
    }

    fn peek_word_is(&self, kw: &str) -> bool {
        if let Some(word) = self.peek_word() {
            word.eq_ignore_ascii_case(kw)
        } else {
            false
        }
    }

    fn expect_keyword(&mut self, kw: &str) -> Result<()> {
        self.skip_whitespace();
        let word = self
            .peek_word()
            .ok_or_else(|| anyhow::anyhow!("Expected keyword {}, found EOF", kw))?;
        if word.eq_ignore_ascii_case(kw) {
            self.skip_whitespace();
            self.pos += word.len();
            Ok(())
        } else {
            bail!("Expected keyword {}, found '{}'", kw, word);
        }
    }

    fn match_keyword(&mut self, kw: &str) -> bool {
        self.skip_whitespace();
        if let Some(word) = self.peek_word() {
            if word.eq_ignore_ascii_case(kw) {
                // pos is already after skip_whitespace, word starts at pos
                self.pos += word.len();
                return true;
            }
        }
        false
    }

    fn expect_char(&mut self, c: char) -> Result<()> {
        self.skip_whitespace();
        if self.pos < self.input.len() && self.input[self.pos..].starts_with(c) {
            self.pos += c.len_utf8();
            Ok(())
        } else {
            bail!("Expected '{}'", c);
        }
    }

    pub fn parse_create_stream(&mut self) -> Result<CreateStreamStmt> {
        self.expect_keyword("CREATE")?;
        self.expect_keyword("STREAM")?;
        self.skip_whitespace();

        let word = self
            .peek_word()
            .ok_or_else(|| anyhow::anyhow!("Expected stream name"))?;
        self.skip_whitespace();
        self.pos += word.len();
        let name = word;

        self.skip_whitespace();
        // Optional schema definition in parens: ()
        if self.pos < self.input.len() && self.input[self.pos..].starts_with('(') {
            let close = self.input[self.pos..]
                .find(')')
                .ok_or_else(|| anyhow::anyhow!("Unclosed parenthesis"))?;
            self.pos += close + 1;
        }

        let mut options = HashMap::new();
        if self.match_keyword("WITH") {
            self.skip_whitespace();
            if self.pos < self.input.len() && self.input[self.pos..].starts_with('(') {
                self.pos += 1;
                let close = self.input[self.pos..]
                    .find(')')
                    .ok_or_else(|| anyhow::anyhow!("Unclosed WITH parenthesis"))?;
                let with_content = &self.input[self.pos..self.pos + close];
                self.pos += close + 1;

                // parse key = "value" pairs separated by comma
                for part in with_content.split(',') {
                    let part = part.trim();
                    if let Some((k, v)) = part.split_once('=') {
                        let k = k.trim().trim_matches('"').trim_matches('\'').to_uppercase();
                        let v = v.trim().trim_matches('"').trim_matches('\'').to_string();
                        options.insert(k, v);
                    }
                }
            }
        }

        Ok(CreateStreamStmt { name, options })
    }

    pub fn parse_create_table(&mut self) -> Result<CreateTableStmt> {
        self.expect_keyword("CREATE")?;
        self.expect_keyword("TABLE")?;
        self.skip_whitespace();

        let word = self
            .peek_word()
            .ok_or_else(|| anyhow::anyhow!("Expected table name"))?;
        self.skip_whitespace();
        self.pos += word.len();
        let name = word;

        self.skip_whitespace();
        // Optional schema definition in parens: ()
        if self.pos < self.input.len() && self.input[self.pos..].starts_with('(') {
            let close = self.input[self.pos..]
                .find(')')
                .ok_or_else(|| anyhow::anyhow!("Unclosed parenthesis"))?;
            self.pos += close + 1;
        }

        let mut options = HashMap::new();
        if self.match_keyword("WITH") {
            self.skip_whitespace();
            if self.pos < self.input.len() && self.input[self.pos..].starts_with('(') {
                self.pos += 1;
                let close = self.input[self.pos..]
                    .find(')')
                    .ok_or_else(|| anyhow::anyhow!("Unclosed WITH parenthesis"))?;
                let with_content = &self.input[self.pos..self.pos + close];
                self.pos += close + 1;

                // parse key = "value" pairs separated by comma
                for part in with_content.split(',') {
                    let part = part.trim();
                    if let Some((k, v)) = part.split_once('=') {
                        let k = k.trim().trim_matches('"').trim_matches('\'').to_uppercase();
                        let v = v.trim().trim_matches('"').trim_matches('\'').to_string();
                        options.insert(k, v);
                    }
                }
            }
        }

        Ok(CreateTableStmt { name, options })
    }

    pub fn parse_select(&mut self) -> Result<SelectStmt> {
        self.expect_keyword("SELECT")?;
        self.skip_whitespace();

        // Fields: wildcard or full expressions, each with an optional AS alias.
        let mut fields = Vec::new();
        let mut field_aliases: Vec<Option<String>> = Vec::new();
        loop {
            self.skip_whitespace();
            if self.pos < self.input.len() && self.input[self.pos..].starts_with('*') {
                self.pos += 1;
                fields.push(Expr::Wildcard);
                field_aliases.push(None);
            } else {
                if self.peek_word_is("FROM") {
                    bail!("Unexpected FROM in select field list");
                }
                let expr = self.parse_expr()?;
                fields.push(expr);
                // Optional alias: AS alias (stored in field_aliases).
                let mut alias: Option<String> = None;
                self.skip_whitespace();
                if self.peek_word_is("AS") {
                    self.match_keyword("AS");
                    if let Some(name) = self.peek_word() {
                        if !name.eq_ignore_ascii_case("FROM")
                            && !name.eq_ignore_ascii_case("WHERE")
                            && !name.eq_ignore_ascii_case("AND")
                            && !name.eq_ignore_ascii_case("OR")
                        {
                            self.skip_whitespace();
                            self.pos += name.len();
                            alias = Some(name);
                        }
                    }
                }
                field_aliases.push(alias);
            }

            self.skip_whitespace();
            if self.pos < self.input.len() && self.input[self.pos..].starts_with(',') {
                self.pos += 1;
            } else {
                break;
            }
        }

        self.expect_keyword("FROM")?;
        self.skip_whitespace();
        let from = self
            .peek_word()
            .ok_or_else(|| anyhow::anyhow!("Expected stream name after FROM"))?;
        self.skip_whitespace();
        self.pos += from.len();

        // Optional JOIN clauses: [LEFT|RIGHT|FULL|CROSS|INNER] [OUTER] JOIN
        // <target> [ON <condition>], one or more in sequence.
        let mut joins = Vec::new();
        loop {
            let is_join = matches!(
                self.peek_word().map(|w| w.to_ascii_uppercase()).as_deref(),
                Some("LEFT")
                    | Some("RIGHT")
                    | Some("FULL")
                    | Some("CROSS")
                    | Some("INNER")
                    | Some("JOIN")
                    | Some("OUTER")
            );
            if !is_join {
                break;
            }
            // Look ahead: only commit when a JOIN header really follows, so a
            // stream merely named e.g. `left` does not break parsing.
            let saved = self.pos;
            let prefix = match self.peek_word().map(|w| w.to_ascii_uppercase()).as_deref() {
                Some("LEFT") | Some("RIGHT") | Some("FULL") | Some("CROSS") | Some("INNER") => {
                    let word = self.peek_word().unwrap_or_default();
                    self.skip_whitespace();
                    self.pos += word.len();
                    Some(word.to_ascii_uppercase())
                }
                _ => None,
            };
            if self.peek_word_is("OUTER") {
                self.match_keyword("OUTER");
            }
            if !self.match_keyword("JOIN") {
                self.pos = saved;
                break;
            }
            let join_type = match prefix.as_deref() {
                Some("LEFT") => JoinType::Left,
                Some("RIGHT") => JoinType::Right,
                Some("FULL") => JoinType::Full,
                Some("CROSS") => JoinType::Cross,
                _ => JoinType::Inner,
            };
            let target = match self.peek_word() {
                Some(t) => {
                    if matches!(
                        t.to_ascii_uppercase().as_str(),
                        "WHERE" | "GROUP" | "ORDER" | "LIMIT" | "HAVING" | "JOIN" | "ON"
                    ) {
                        bail!("Expected join target after JOIN, found '{}'", t);
                    }
                    self.skip_whitespace();
                    self.pos += t.len();
                    t
                }
                None => bail!("Expected join target after JOIN"),
            };
            let mut on = None;
            if self.match_keyword("ON") {
                on = Some(self.parse_expr()?);
            }
            joins.push(JoinClause {
                join_type,
                target,
                on,
            });
        }

        let mut where_clause = None;
        if self.match_keyword("WHERE") {
            where_clause = Some(self.parse_expr()?);
        }

        let mut group_by = Vec::new();
        let mut window: Option<WindowDef> = None;
        // GROUP BY <items> — items may include window calls which go to `window`.
        if self.peek_word_is("GROUP") {
            self.expect_keyword("GROUP")?;
            self.expect_keyword("BY")?;
            loop {
                self.skip_whitespace();
                // Allow trailing commas / empty? No — require an expression.
                let item = self.parse_expr()?;
                // Recognize window calls case-insensitively; they go to `window`,
                // remaining expressions go to `group_by`.
                if let Some(w) = Self::try_parse_window_def(&item)? {
                    window = Some(w);
                } else {
                    group_by.push(item);
                }
                self.skip_whitespace();
                if self.pos < self.input.len() && self.input[self.pos..].starts_with(',') {
                    self.pos += 1;
                } else {
                    break;
                }
            }
        }

        let mut having = None;
        if self.match_keyword("HAVING") {
            having = Some(self.parse_expr()?);
        }

        let mut order_by = Vec::new();
        if self.match_keyword("ORDER") {
            self.expect_keyword("BY")?;
            loop {
                self.skip_whitespace();
                let expr = self.parse_expr()?;
                let order = if self.peek_word_is("DESC") {
                    self.match_keyword("DESC");
                    SortOrder::Desc
                } else {
                    if self.peek_word_is("ASC") {
                        self.match_keyword("ASC");
                    }
                    SortOrder::Asc
                };
                order_by.push(OrderByItem { expr, order });
                self.skip_whitespace();
                if self.pos < self.input.len() && self.input[self.pos..].starts_with(',') {
                    self.pos += 1;
                } else {
                    break;
                }
            }
        }

        let mut limit: Option<usize> = None;
        if self.match_keyword("LIMIT") {
            self.skip_whitespace();
            let token = self
                .peek_word()
                .ok_or_else(|| anyhow::anyhow!("Expected integer after LIMIT"))?;
            let n: usize = token
                .parse()
                .map_err(|_| anyhow::anyhow!("Expected integer after LIMIT, found '{}'", token))?;
            self.skip_whitespace();
            self.pos += token.len();
            limit = Some(n);
        }

        self.skip_whitespace();
        if self.pos < self.input.len() && self.input[self.pos..].starts_with(';') {
            self.pos += 1;
        }

        // Set operations bind loosest of all: `SELECT ... UNION [ALL] SELECT ...`.
        // Right-recursive, so `A UNION B UNION C` parses as `A UNION (B UNION C)`.
        let mut set_op: Option<(SetOp, Box<SelectStmt>)> = None;
        if self.match_keyword("UNION") {
            let op = if self.match_keyword("ALL") {
                SetOp::UnionAll
            } else {
                SetOp::Union
            };
            set_op = Some((op, Box::new(self.parse_select()?)));
        }

        Ok(SelectStmt {
            fields,
            field_aliases,
            from,
            joins,
            where_clause,
            group_by,
            window,
            having,
            order_by,
            limit,
            set_op,
        })
    }

    /// If `expr` is a recognized window call, convert it to a `WindowDef`.
    /// Returns `Ok(None)` for non-window expressions.
    fn try_parse_window_def(expr: &Expr) -> Result<Option<WindowDef>> {
        let (name, args) = match expr {
            Expr::Call { name, args } => (name, args),
            _ => return Ok(None),
        };
        match name.to_ascii_lowercase().as_str() {
            "tumblingwindow" => {
                if args.len() != 2 {
                    bail!(
                        "TUMBLINGWINDOW expects 2 arguments (unit, length), got {}",
                        args.len()
                    );
                }
                let unit = Self::parse_window_unit(&args[0])?;
                let length = Self::parse_window_u64(&args[1])?;
                Ok(Some(WindowDef::TumblingTime { unit, length }))
            }
            "hoppingwindow" => {
                if args.len() != 3 {
                    bail!(
                        "HOPPINGWINDOW expects 3 arguments (unit, length, interval), got {}",
                        args.len()
                    );
                }
                let unit = Self::parse_window_unit(&args[0])?;
                let length = Self::parse_window_u64(&args[1])?;
                let interval = Self::parse_window_u64(&args[2])?;
                Ok(Some(WindowDef::HoppingTime {
                    unit,
                    length,
                    interval,
                }))
            }
            "slidingwindow" => {
                if args.len() != 2 && args.len() != 3 {
                    bail!(
                        "SLIDINGWINDOW expects 2 arguments (unit, length), got {}",
                        args.len()
                    );
                }
                let unit = Self::parse_window_unit(&args[0])?;
                let length = Self::parse_window_u64(&args[1])?;
                Ok(Some(WindowDef::SlidingTime { unit, length }))
            }
            "countwindow" => {
                if args.len() != 1 && args.len() != 2 {
                    bail!(
                        "COUNTWINDOW expects 1 or 2 arguments (size[, interval]), got {}",
                        args.len()
                    );
                }
                let size = Self::parse_window_usize(&args[0])?;
                let interval = if args.len() == 2 {
                    Some(Self::parse_window_usize(&args[1])?)
                } else {
                    None
                };
                Ok(Some(WindowDef::Count { size, interval }))
            }
            _ => Ok(None),
        }
    }

    fn parse_window_unit(expr: &Expr) -> Result<TimeUnit> {
        match expr {
            Expr::Identifier(name) => Self::time_unit_from_str(name),
            Expr::Literal(serde_json::Value::String(s)) => Self::time_unit_from_str(s),
            _ => bail!("Window time unit must be one of dd, hh, mi, ss, ms"),
        }
    }

    fn time_unit_from_str(s: &str) -> Result<TimeUnit> {
        match s.to_ascii_lowercase().as_str() {
            "dd" => Ok(TimeUnit::Dd),
            "hh" => Ok(TimeUnit::Hh),
            "mi" => Ok(TimeUnit::Mi),
            "ss" => Ok(TimeUnit::Ss),
            "ms" => Ok(TimeUnit::Ms),
            _ => bail!(
                "Unknown time unit '{}', expected one of dd, hh, mi, ss, ms",
                s
            ),
        }
    }

    fn parse_window_u64(expr: &Expr) -> Result<u64> {
        match expr {
            Expr::Literal(v) => {
                if let Some(u) = v.as_u64() {
                    return Ok(u);
                }
                if let Some(i) = v.as_i64() {
                    if i >= 0 {
                        return Ok(i as u64);
                    }
                    bail!("Window length/interval must be non-negative, got {}", i);
                }
                if let Some(f) = v.as_f64() {
                    if f.is_finite() && f >= 0.0 && f.trunc() == f {
                        return Ok(f as u64);
                    }
                    bail!(
                        "Window length/interval must be a non-negative integer, got {}",
                        f
                    );
                }
                bail!("Window length/interval must be an integer")
            }
            _ => bail!("Window length/interval must be an integer literal"),
        }
    }

    fn parse_window_usize(expr: &Expr) -> Result<usize> {
        Ok(Self::parse_window_u64(expr)? as usize)
    }

    fn parse_expr(&mut self) -> Result<Expr> {
        self.parse_or()
    }

    fn parse_or(&mut self) -> Result<Expr> {
        let mut left = self.parse_and()?;
        while self.match_keyword("OR") {
            let right = self.parse_and()?;
            left = Expr::BinaryOp {
                left: Box::new(left),
                op: BinaryOperator::Or,
                right: Box::new(right),
            };
        }
        Ok(left)
    }

    fn parse_and(&mut self) -> Result<Expr> {
        let mut left = self.parse_range()?;
        while self.peek_word_is("AND") {
            // Need to be careful: AND could be part of BETWEEN's low AND high,
            // but BETWEEN's AND is already consumed inside parse_range,
            // so any remaining AND here is a logical conjunction.
            // However, to avoid consuming an AND that belongs to an unfinished BETWEEN
            // (should not happen since parse_range consumes it), just consume.
            self.match_keyword("AND");
            let right = self.parse_range()?;
            left = Expr::BinaryOp {
                left: Box::new(left),
                op: BinaryOperator::And,
                right: Box::new(right),
            };
        }
        Ok(left)
    }

    /// Range/Set/Null level: [NOT] BETWEEN, [NOT] IN, IS [NOT] NULL
    /// Binds looser than comparison, tighter than AND.
    fn parse_range(&mut self) -> Result<Expr> {
        let left = self.parse_comparison()?;

        self.skip_whitespace();
        let next = self.peek_word();

        let Some(w) = next else {
            return Ok(left);
        };

        if w.eq_ignore_ascii_case("BETWEEN") {
            self.match_keyword("BETWEEN");
            let low = self.parse_comparison()?;
            self.expect_keyword("AND")?;
            let high = self.parse_comparison()?;
            return Ok(Expr::Between {
                expr: Box::new(left),
                low: Box::new(low),
                high: Box::new(high),
                negated: false,
            });
        } else if w.eq_ignore_ascii_case("NOT") {
            // Lookahead: NOT BETWEEN / NOT IN (NOT LIKE is handled in comparison layer,
            // but handle it here as fallback too)
            let saved = self.pos;
            self.match_keyword("NOT");
            if let Some(w2) = self.peek_word() {
                if w2.eq_ignore_ascii_case("BETWEEN") {
                    self.match_keyword("BETWEEN");
                    let low = self.parse_comparison()?;
                    self.expect_keyword("AND")?;
                    let high = self.parse_comparison()?;
                    return Ok(Expr::Between {
                        expr: Box::new(left),
                        low: Box::new(low),
                        high: Box::new(high),
                        negated: true,
                    });
                } else if w2.eq_ignore_ascii_case("IN") {
                    self.match_keyword("IN");
                    let list = self.parse_in_list()?;
                    return Ok(Expr::InList {
                        expr: Box::new(left),
                        list,
                        negated: true,
                    });
                } else if w2.eq_ignore_ascii_case("LIKE") {
                    self.match_keyword("LIKE");
                    let right = self.parse_comparison()?;
                    let like_expr = Expr::BinaryOp {
                        left: Box::new(left),
                        op: BinaryOperator::Like,
                        right: Box::new(right),
                    };
                    return Ok(Expr::UnaryOp {
                        op: UnaryOperator::Not,
                        expr: Box::new(like_expr),
                    });
                } else {
                    // NOT not followed by BETWEEN/IN/LIKE: restore and return left.
                    self.pos = saved;
                    return Ok(left);
                }
            } else {
                self.pos = saved;
                return Ok(left);
            }
        } else if w.eq_ignore_ascii_case("IN") {
            self.match_keyword("IN");
            let list = self.parse_in_list()?;
            return Ok(Expr::InList {
                expr: Box::new(left),
                list,
                negated: false,
            });
        } else if w.eq_ignore_ascii_case("IS") {
            self.match_keyword("IS");
            let negated = if self.peek_word_is("NOT") {
                self.match_keyword("NOT");
                true
            } else {
                false
            };
            self.expect_keyword("NULL")?;
            return Ok(Expr::IsNull {
                expr: Box::new(left),
                negated,
            });
        } else if w.eq_ignore_ascii_case("LIKE") {
            // Fallback in case comparison layer didn't consume (should already be consumed,
            // but handle for robustness when left contains range? Actually comparison already
            // handles LIKE, so this branch is normally unreachable. Keep for safety.)
            self.match_keyword("LIKE");
            let right = self.parse_comparison()?;
            return Ok(Expr::BinaryOp {
                left: Box::new(left),
                op: BinaryOperator::Like,
                right: Box::new(right),
            });
        }

        Ok(left)
    }

    fn parse_in_list(&mut self) -> Result<Vec<Expr>> {
        self.skip_whitespace();
        self.expect_char('(')?;
        let mut list = Vec::new();
        self.skip_whitespace();
        // Allow empty list? Treat as empty (IN () is always false). But require ')' handling.
        if self.pos < self.input.len() && self.input[self.pos..].starts_with(')') {
            self.pos += 1;
            return Ok(list);
        }
        loop {
            self.skip_whitespace();
            // Trailing ')' without element would be error unless empty (handled above)
            if self.pos < self.input.len() && self.input[self.pos..].starts_with(')') {
                break;
            }
            let e = self.parse_expr()?;
            list.push(e);
            self.skip_whitespace();
            if self.pos < self.input.len() && self.input[self.pos..].starts_with(',') {
                self.pos += 1;
            } else {
                break;
            }
        }
        self.skip_whitespace();
        self.expect_char(')')?;
        Ok(list)
    }

    fn parse_comparison(&mut self) -> Result<Expr> {
        let left = self.parse_additive()?;

        self.skip_whitespace();
        // Check for NOT LIKE (infix): left NOT LIKE right
        if self.peek_word_is("NOT") {
            let saved = self.pos;
            self.match_keyword("NOT");
            if self.peek_word_is("LIKE") {
                self.match_keyword("LIKE");
                let right = self.parse_additive()?;
                let like_expr = Expr::BinaryOp {
                    left: Box::new(left),
                    op: BinaryOperator::Like,
                    right: Box::new(right),
                };
                return Ok(Expr::UnaryOp {
                    op: UnaryOperator::Not,
                    expr: Box::new(like_expr),
                });
            } else {
                // Not a NOT LIKE: restore so parse_range can handle NOT BETWEEN / NOT IN.
                self.pos = saved;
                return Ok(left);
            }
        }

        if self.peek_word_is("LIKE") {
            self.match_keyword("LIKE");
            let right = self.parse_additive()?;
            return Ok(Expr::BinaryOp {
                left: Box::new(left),
                op: BinaryOperator::Like,
                right: Box::new(right),
            });
        }

        self.skip_whitespace();
        if self.pos >= self.input.len() {
            return Ok(left);
        }
        let rem = &self.input[self.pos..];

        let (op, len) = if rem.starts_with(">=") {
            (BinaryOperator::Gte, 2)
        } else if rem.starts_with("<=") {
            (BinaryOperator::Lte, 2)
        } else if rem.starts_with("!=") || rem.starts_with("<>") {
            (BinaryOperator::Neq, 2)
        } else if rem.starts_with("==") {
            (BinaryOperator::Eq, 2)
        } else if rem.starts_with('>') {
            (BinaryOperator::Gt, 1)
        } else if rem.starts_with('<') {
            (BinaryOperator::Lt, 1)
        } else if rem.starts_with('=') {
            (BinaryOperator::Eq, 1)
        } else {
            return Ok(left);
        };

        self.pos += len;
        let right = self.parse_additive()?;
        Ok(Expr::BinaryOp {
            left: Box::new(left),
            op,
            right: Box::new(right),
        })
    }

    fn parse_additive(&mut self) -> Result<Expr> {
        let mut left = self.parse_multiplicative()?;
        loop {
            self.skip_whitespace();
            if self.pos >= self.input.len() {
                break;
            }
            let rem = &self.input[self.pos..];
            if rem.starts_with('+') {
                self.pos += 1;
                let right = self.parse_multiplicative()?;
                left = Expr::BinaryOp {
                    left: Box::new(left),
                    op: BinaryOperator::Add,
                    right: Box::new(right),
                };
            } else if rem.starts_with('-') {
                self.pos += 1;
                let right = self.parse_multiplicative()?;
                left = Expr::BinaryOp {
                    left: Box::new(left),
                    op: BinaryOperator::Sub,
                    right: Box::new(right),
                };
            } else {
                break;
            }
        }
        Ok(left)
    }

    fn parse_multiplicative(&mut self) -> Result<Expr> {
        let mut left = self.parse_unary()?;
        loop {
            self.skip_whitespace();
            if self.pos >= self.input.len() {
                break;
            }
            let rem = &self.input[self.pos..];
            if rem.starts_with('*') {
                self.pos += 1;
                let right = self.parse_unary()?;
                left = Expr::BinaryOp {
                    left: Box::new(left),
                    op: BinaryOperator::Mul,
                    right: Box::new(right),
                };
            } else if rem.starts_with('/') {
                self.pos += 1;
                let right = self.parse_unary()?;
                left = Expr::BinaryOp {
                    left: Box::new(left),
                    op: BinaryOperator::Div,
                    right: Box::new(right),
                };
            } else if rem.starts_with('%') {
                self.pos += 1;
                let right = self.parse_unary()?;
                left = Expr::BinaryOp {
                    left: Box::new(left),
                    op: BinaryOperator::Mod,
                    right: Box::new(right),
                };
            } else {
                break;
            }
        }
        Ok(left)
    }

    fn parse_unary(&mut self) -> Result<Expr> {
        self.skip_whitespace();
        if self.peek_word_is("NOT") {
            self.match_keyword("NOT");
            let expr = self.parse_unary()?;
            return Ok(Expr::UnaryOp {
                op: UnaryOperator::Not,
                expr: Box::new(expr),
            });
        }
        self.skip_whitespace();
        if self.pos < self.input.len() {
            let rem = &self.input[self.pos..];
            if rem.starts_with('-') {
                self.pos += 1;
                let expr = self.parse_unary()?;
                return Ok(Expr::UnaryOp {
                    op: UnaryOperator::Neg,
                    expr: Box::new(expr),
                });
            } else if rem.starts_with('+') {
                // Unary plus: no-op, just consume and recurse
                self.pos += 1;
                return self.parse_unary();
            }
        }
        self.parse_primary()
    }

    fn parse_primary(&mut self) -> Result<Expr> {
        self.skip_whitespace();
        if self.pos >= self.input.len() {
            bail!("Unexpected end of expression");
        }

        let rem = &self.input[self.pos..];
        // Parentheses: (...)
        if rem.starts_with('(') {
            self.pos += 1;
            let expr = self.parse_expr()?;
            self.skip_whitespace();
            self.expect_char(')')?;
            return Ok(expr);
        }

        // String literal
        if rem.starts_with('\'') || rem.starts_with('"') {
            let quote = rem.chars().next().unwrap();
            let rest = &rem[quote.len_utf8()..];
            let end = rest
                .find(quote)
                .ok_or_else(|| anyhow::anyhow!("Unterminated string literal"))?;
            let s = &rest[..end];
            self.pos += quote.len_utf8() + end + quote.len_utf8();
            return Ok(Expr::Literal(serde_json::Value::String(s.to_string())));
        }

        // Numeric literal: digit or '.' followed by digit
        if Self::is_number_start(rem) {
            return self.parse_number();
        }

        let word = self
            .peek_word()
            .ok_or_else(|| anyhow::anyhow!("Expected expression token"))?;
        // Consume word: pos is at word start after skip_whitespace
        self.skip_whitespace();
        self.pos += word.len();

        if word.eq_ignore_ascii_case("true") {
            Ok(Expr::Literal(serde_json::Value::Bool(true)))
        } else if word.eq_ignore_ascii_case("false") {
            Ok(Expr::Literal(serde_json::Value::Bool(false)))
        } else if word.eq_ignore_ascii_case("null") {
            Ok(Expr::Literal(serde_json::Value::Null))
        } else if word.eq_ignore_ascii_case("CASE") {
            self.parse_case()
        } else {
            // Function call: name '(' args ')' — usable in SELECT and WHERE/expressions.
            // Allow optional whitespace between name and '('.
            self.skip_whitespace();
            if self.pos < self.input.len() && self.input[self.pos..].starts_with('(') {
                self.pos += 1;
                let mut args = Vec::new();
                self.skip_whitespace();
                if !(self.pos < self.input.len() && self.input[self.pos..].starts_with(')')) {
                    loop {
                        self.skip_whitespace();
                        // Support '*' as wildcard arg (e.g. count(*)) for forward-compat.
                        if self.pos < self.input.len() && self.input[self.pos..].starts_with('*') {
                            // Only treat as lone '*' (followed by ',' or ')').
                            let after_star = &self.input[self.pos + 1..];
                            let after_trim = after_star.trim_start();
                            if after_trim.starts_with(',') || after_trim.starts_with(')') {
                                self.pos += 1;
                                args.push(Expr::Wildcard);
                                self.skip_whitespace();
                                if self.pos < self.input.len()
                                    && self.input[self.pos..].starts_with(',')
                                {
                                    self.pos += 1;
                                    continue;
                                } else {
                                    break;
                                }
                            }
                        }
                        let arg = self.parse_expr()?;
                        args.push(arg);
                        self.skip_whitespace();
                        if self.pos < self.input.len() && self.input[self.pos..].starts_with(',') {
                            self.pos += 1;
                        } else {
                            break;
                        }
                    }
                }
                self.skip_whitespace();
                self.expect_char(')')?;
                let call_expr = Expr::Call { name: word, args };
                // Analytic OVER clause: func(...) OVER ([PARTITION BY expr]).
                if self.match_keyword("OVER") {
                    self.expect_char('(')?;
                    let mut partition_by = None;
                    if self.match_keyword("PARTITION") {
                        self.expect_keyword("BY")?;
                        partition_by = Some(Box::new(self.parse_expr()?));
                    }
                    self.expect_char(')')?;
                    return Ok(Expr::Over {
                        call: Box::new(call_expr),
                        partition_by,
                    });
                }
                return Ok(call_expr);
            }
            // Identifier with dot navigation: a.b.c
            let mut expr = Expr::Identifier(word);
            loop {
                self.skip_whitespace();
                if self.pos < self.input.len() && self.input[self.pos..].starts_with('.') {
                    self.pos += 1;
                    self.skip_whitespace();
                    let field = self
                        .peek_word()
                        .ok_or_else(|| anyhow::anyhow!("Expected field name after '.'"))?;
                    self.skip_whitespace();
                    self.pos += field.len();
                    expr = Expr::FieldAccess {
                        parent: Box::new(expr),
                        field,
                    };
                } else {
                    break;
                }
            }
            Ok(expr)
        }
    }

    /// Parse a CASE expression. Called after the `CASE` keyword is consumed.
    /// Supports both forms:
    /// - searched: `CASE WHEN <cond> THEN <result> ... [ELSE <result>] END`
    /// - simple:   `CASE <operand> WHEN <value> THEN <result> ... [ELSE <result>] END`
    fn parse_case(&mut self) -> Result<Expr> {
        // Searched CASE when the next word is WHEN; otherwise parse the operand.
        let operand = if self.peek_word_is("WHEN") {
            None
        } else {
            Some(Box::new(self.parse_expr()?))
        };

        let mut when_clauses = Vec::new();
        while self.match_keyword("WHEN") {
            let condition = self.parse_expr()?;
            self.expect_keyword("THEN")?;
            let result = self.parse_expr()?;
            when_clauses.push((condition, result));
        }
        if when_clauses.is_empty() {
            bail!("CASE expression requires at least one WHEN clause");
        }

        let else_clause = if self.match_keyword("ELSE") {
            Some(Box::new(self.parse_expr()?))
        } else {
            None
        };
        self.expect_keyword("END")?;

        Ok(Expr::Case {
            operand,
            when_clauses,
            else_clause,
        })
    }

    fn is_number_start(s: &str) -> bool {
        let mut chars = s.chars();
        match chars.next() {
            Some(c) if c.is_ascii_digit() => true,
            Some('.') => matches!(chars.next(), Some(n) if n.is_ascii_digit()),
            _ => false,
        }
    }

    fn parse_number(&mut self) -> Result<Expr> {
        self.skip_whitespace();
        let start = self.pos;
        let bytes = self.input.as_bytes();
        let mut i = self.pos;
        // Integer part
        while i < self.input.len() && bytes[i].is_ascii_digit() {
            i += 1;
        }
        // Fractional part
        if i < self.input.len() && bytes[i] == b'.' {
            // Only treat '.' as decimal if followed by digit OR if we already have digits
            // (to support "25.0" and ".2" but not "a.b" which is handled as identifier path;
            // here we are already in number branch so at least one side is digit)
            let next_is_digit = i + 1 < self.input.len() && bytes[i + 1].is_ascii_digit();
            let has_int_part = i > start;
            if next_is_digit || has_int_part {
                // Consume '.' if followed by digit, or if has int part allow trailing '.'?
                // Require digit after '.' to be a valid float unless it's like "25."?
                // For simplicity, consume '.' and following digits (if any).
                i += 1;
                while i < self.input.len() && bytes[i].is_ascii_digit() {
                    i += 1;
                }
            }
        }
        // Exponent part
        if i < self.input.len() && (bytes[i] == b'e' || bytes[i] == b'E') {
            let mut j = i + 1;
            if j < self.input.len() && (bytes[j] == b'+' || bytes[j] == b'-') {
                j += 1;
            }
            let exp_start = j;
            while j < self.input.len() && bytes[j].is_ascii_digit() {
                j += 1;
            }
            if j > exp_start {
                i = j;
            }
            // else: not a valid exponent, leave 'e' unconsumed
        }

        let text = &self.input[start..i];
        if text.is_empty() || text == "." {
            bail!("Invalid number literal");
        }
        self.pos = i;

        // Try integer first if no float markers
        if !text.contains('.') && !text.contains('e') && !text.contains('E') {
            if let Ok(n) = text.parse::<i64>() {
                return Ok(Expr::Literal(serde_json::json!(n)));
            }
            // Fall back to u64 (large positive) then f64
            if let Ok(n) = text.parse::<u64>() {
                return Ok(Expr::Literal(serde_json::json!(n)));
            }
        }
        if let Ok(f) = text.parse::<f64>() {
            return Ok(Expr::Literal(serde_json::json!(f)));
        }
        bail!("Invalid number literal '{}'", text)
    }
}
