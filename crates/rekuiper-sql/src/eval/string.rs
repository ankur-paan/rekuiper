use super::Evaluator;
use serde_json::Value;

impl Evaluator {
    // ---------- string functions ----------

    pub(crate) fn func_concat(args: &[Value]) -> Value {
        if args.is_empty() {
            return Value::Null;
        }
        let mut out = String::new();
        for a in args {
            out.push_str(&Self::to_string_always(a));
        }
        Value::String(out)
    }

    pub(crate) fn func_lower(args: &[Value]) -> Value {
        if args.len() != 1 {
            return Value::Null;
        }
        if args[0].is_null() {
            return Value::Null;
        }
        Value::String(Self::to_string_always(&args[0]).to_lowercase())
    }

    pub(crate) fn func_upper(args: &[Value]) -> Value {
        if args.len() != 1 {
            return Value::Null;
        }
        if args[0].is_null() {
            return Value::Null;
        }
        Value::String(Self::to_string_always(&args[0]).to_uppercase())
    }

    pub(crate) fn func_length(args: &[Value]) -> Value {
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

    pub(crate) fn func_trim(args: &[Value]) -> Value {
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
    pub(crate) fn func_substr(args: &[Value]) -> Value {
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

    pub(crate) fn func_startswith(args: &[Value]) -> Value {
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

    pub(crate) fn func_endswith(args: &[Value]) -> Value {
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

    // ---------- extended string functions ----------

    pub(crate) fn func_ltrim(args: &[Value]) -> Value {
        if args.len() != 1 {
            return Value::Null;
        }
        if args[0].is_null() {
            return Value::Null;
        }
        Value::String(Self::to_string_always(&args[0]).trim_start().to_string())
    }

    pub(crate) fn func_rtrim(args: &[Value]) -> Value {
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

    pub(crate) fn func_lpad(args: &[Value]) -> Value {
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

    pub(crate) fn func_rpad(args: &[Value]) -> Value {
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

    pub(crate) fn func_replace(args: &[Value]) -> Value {
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

    pub(crate) fn func_split(args: &[Value]) -> Value {
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

    pub(crate) fn func_reverse(args: &[Value]) -> Value {
        if args.len() != 1 {
            return Value::Null;
        }
        if args[0].is_null() {
            return Value::Null;
        }
        Value::String(Self::to_string_always(&args[0]).chars().rev().collect())
    }

    pub(crate) fn func_indexof(args: &[Value]) -> Value {
        if args.len() != 2 {
            return Value::Null;
        }
        let (Some(s), Some(sub)) = (args[0].as_str(), args[1].as_str()) else {
            return Value::Null;
        };
        Value::from(s.find(sub).map(|i| i as i64).unwrap_or(-1))
    }

    pub(crate) fn func_format(args: &[Value]) -> Value {
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
}
