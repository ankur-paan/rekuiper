use super::Evaluator;
use serde_json::Value;

/// One compiled step of a dot-notation JSON path.
#[derive(Debug, Clone, PartialEq)]
pub enum JsonPathStep {
    Field(String),
    Index(usize),
    Wildcard,
}

impl Evaluator {
    // ---------- JSON path functions ----------

    /// Compile a dot-notation path (`$.a.b[0]`, `a.Group[*].last`) into
    /// steps. A leading `$` root is identity. Returns `None` for malformed
    /// bracket expressions (filters, quotes) — those simply never match.
    pub(crate) fn compile_json_path(path: &str) -> Option<Vec<JsonPathStep>> {
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
    pub(crate) fn json_collect<'v>(
        val: &'v Value,
        steps: &[JsonPathStep],
        out: &mut Vec<&'v Value>,
    ) {
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

    pub(crate) fn func_json_path_query(args: &[Value]) -> Value {
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

    pub(crate) fn func_json_path_query_first(args: &[Value]) -> Value {
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

    pub(crate) fn func_json_path_exists(args: &[Value]) -> Value {
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

    pub(crate) fn func_json_map(args: &[Value]) -> Value {
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
}
