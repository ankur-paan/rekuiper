//! Pure-Rust serialization codecs: binary blobs, delimiter-separated values
//! and protobuf wire format. No C dependencies, no `protoc`.
//!
//! The JSON mapping conventions mirror eKuiper: binary payloads travel as
//! `{"_binary": "<base64>"}`, delimited rows map positionally onto headers,
//! and protobuf messages map field names to JSON values.

use anyhow::Result;
use base64::Engine as _;
use serde_json::Value;
use std::collections::HashMap;

// ---------------------------------------------------------------------------
// Binary codec
// ---------------------------------------------------------------------------

/// Opaque-byte codec: raw bytes <-> `{"_binary": "<base64>"}`.
pub struct BinaryCodec;

impl BinaryCodec {
    pub fn decode(bytes: &[u8]) -> Value {
        let mut map = serde_json::Map::with_capacity(1);
        map.insert(
            "_binary".to_string(),
            Value::String(base64::engine::general_purpose::STANDARD.encode(bytes)),
        );
        Value::Object(map)
    }

    pub fn encode(val: &Value) -> Vec<u8> {
        if let Value::Object(map) = val {
            for key in ["_binary", "_raw"] {
                if let Some(Value::String(s)) = map.get(key) {
                    match base64::engine::general_purpose::STANDARD.decode(s) {
                        Ok(bytes) => return bytes,
                        Err(_) => return Vec::new(),
                    }
                }
            }
        }
        match val {
            Value::String(s) => s.clone().into_bytes(),
            _ => serde_json::to_vec(val).unwrap_or_default(),
        }
    }
}

// ---------------------------------------------------------------------------
// Delimited codec
// ---------------------------------------------------------------------------

fn parse_token(token: &str) -> Value {
    let token = token.trim();
    if let Ok(i) = token.parse::<i64>() {
        return Value::from(i);
    }
    if let Ok(f) = token.parse::<f64>() {
        if let Some(n) = serde_json::Number::from_f64(f) {
            return Value::Number(n);
        }
    }
    Value::String(token.to_string())
}

/// Split one line on `delimiter`, honoring RFC-4180 style double quotes
/// (`"a,b",c` -> two fields, `""` inside quotes -> literal `"`).
/// Returns `(fields, quoted_flags)`; quoted content is preserved verbatim
/// while unquoted fields are whitespace-trimmed.
fn split_quoted(line: &str, delimiter: char) -> Vec<(String, bool)> {
    let mut fields: Vec<(String, bool)> = Vec::new();
    let mut current = String::new();
    let mut quoted = false;
    let mut in_quotes = false;
    let mut chars = line.chars().peekable();
    while let Some(c) = chars.next() {
        if in_quotes {
            if c == '"' {
                if chars.peek() == Some(&'"') {
                    // Escaped quote inside a quoted field.
                    chars.next();
                    current.push('"');
                } else {
                    in_quotes = false;
                }
            } else {
                current.push(c);
            }
        } else if c == '"' && current.trim().is_empty() {
            // An opening quote only counts at the start of a field.
            current.clear();
            quoted = true;
            in_quotes = true;
        } else if c == delimiter {
            if quoted {
                fields.push((std::mem::take(&mut current), true));
            } else {
                fields.push((current.trim().to_string(), false));
            }
            current = String::new();
            quoted = false;
        } else {
            current.push(c);
        }
    }
    if quoted {
        fields.push((current, true));
    } else {
        fields.push((current.trim().to_string(), false));
    }
    fields
}

fn quote_cell(cell: &str, delimiter: char) -> String {
    if cell.contains(delimiter) || cell.contains(['"', '\n', '\r']) {
        format!("\"{}\"", cell.replace('"', "\"\""))
    } else {
        cell.to_string()
    }
}

/// Delimited row codec with configurable delimiter, headers and quoting.
#[derive(Debug, Clone)]
pub struct DelimitedCodec {
    pub delimiter: char,
    pub headers: Vec<String>,
}

impl DelimitedCodec {
    pub fn new(delimiter: char, headers: Vec<String>) -> Self {
        Self { delimiter, headers }
    }

    /// Resolve `"comma"` / `"tab"` / `"pipe"` (or a literal character).
    pub fn delimiter_from_name(name: &str) -> char {
        match name.trim().to_ascii_lowercase().as_str() {
            "tab" | "\\t" => '\t',
            "pipe" => '|',
            "comma" => ',',
            "semicolon" => ';',
            other => other.chars().next().unwrap_or(','),
        }
    }

    /// Decode one line into a record, mapping fields positionally onto the
    /// configured headers with numeric auto-conversion.
    pub fn decode(&self, line: &str) -> HashMap<String, Value> {
        let mut map = HashMap::with_capacity(self.headers.len());
        let fields = split_quoted(line, self.delimiter);
        for (i, header) in self.headers.iter().enumerate() {
            let raw = fields.get(i).map(|(f, _)| f.as_str()).unwrap_or("");
            map.insert(header.clone(), parse_token(raw));
        }
        map
    }

    /// Encode a record as one line in header order, quoting cells that need
    /// it. Missing/`Null` values become empty cells.
    pub fn encode(&self, record: &HashMap<String, Value>) -> String {
        self.headers
            .iter()
            .map(|h| match record.get(h) {
                None | Some(Value::Null) => String::new(),
                Some(Value::String(s)) => quote_cell(s, self.delimiter),
                Some(Value::Number(n)) => n.to_string(),
                Some(Value::Bool(b)) => b.to_string(),
                Some(other) => quote_cell(
                    &serde_json::to_string(other).unwrap_or_default(),
                    self.delimiter,
                ),
            })
            .collect::<Vec<_>>()
            .join(&self.delimiter.to_string())
    }
}

// ---------------------------------------------------------------------------
// Protobuf wire codec (pure Rust)
// ---------------------------------------------------------------------------

/// One parsed `.proto` message field.
#[derive(Debug, Clone, PartialEq)]
pub struct ProtoField {
    pub name: String,
    pub proto_type: String,
    pub number: u32,
    pub repeated: bool,
}

/// One parsed `.proto` message, including messages nested inside it.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct ProtoMessage {
    pub name: String,
    pub fields: Vec<ProtoField>,
    pub nested: HashMap<String, ProtoMessage>,
}

/// Strip `//` line and `/* */` block comments, respecting string literals.
fn strip_proto_comments(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut chars = text.chars().peekable();
    let mut in_string = false;
    while let Some(c) = chars.next() {
        if in_string {
            out.push(c);
            if c == '\\' {
                if let Some(esc) = chars.next() {
                    out.push(esc);
                }
            } else if c == '"' {
                in_string = false;
            }
            continue;
        }
        match c {
            '"' => {
                in_string = true;
                out.push(c);
            }
            '/' if chars.peek() == Some(&'/') => {
                for c2 in chars.by_ref() {
                    if c2 == '\n' {
                        out.push('\n');
                        break;
                    }
                }
            }
            '/' if chars.peek() == Some(&'*') => {
                chars.next();
                let mut prev_star = false;
                for c2 in chars.by_ref() {
                    if prev_star && c2 == '/' {
                        break;
                    }
                    prev_star = c2 == '*';
                }
                out.push(' ');
            }
            _ => out.push(c),
        }
    }
    out
}

struct ProtoCursor<'a> {
    text: &'a str,
    pos: usize,
}

impl<'a> ProtoCursor<'a> {
    fn new(text: &'a str) -> Self {
        Self { text, pos: 0 }
    }

    fn rest(&self) -> &'a str {
        &self.text[self.pos..]
    }

    fn skip_ws(&mut self) {
        while let Some(c) = self.rest().chars().next() {
            if c.is_whitespace() {
                self.pos += c.len_utf8();
            } else {
                break;
            }
        }
    }

    fn at_end(&mut self) -> bool {
        self.skip_ws();
        self.pos >= self.text.len()
    }

    fn consume_ident(&mut self) -> Option<String> {
        self.skip_ws();
        let rest = self.rest();
        let mut end = 0;
        for (i, c) in rest.char_indices() {
            if i == 0 {
                if !(c.is_alphabetic() || c == '_') {
                    return None;
                }
            } else if !(c.is_alphanumeric() || c == '_' || c == '.') {
                break;
            }
            end = i + c.len_utf8();
        }
        if end == 0 {
            return None;
        }
        let ident = rest[..end].to_string();
        self.pos += end;
        Some(ident)
    }

    fn expect_char(&mut self, expected: char) -> bool {
        self.skip_ws();
        if self.rest().starts_with(expected) {
            self.pos += expected.len_utf8();
            true
        } else {
            false
        }
    }

    /// Consume a balanced `{...}` block, returning its inner text.
    fn consume_block(&mut self) -> Option<&'a str> {
        if !self.expect_char('{') {
            return None;
        }
        let start = self.pos;
        let mut depth = 1usize;
        let mut in_string = false;
        let mut chars = self.rest().char_indices().peekable();
        while let Some((i, c)) = chars.next() {
            if in_string {
                if c == '\\' {
                    chars.next();
                } else if c == '"' {
                    in_string = false;
                }
                continue;
            }
            match c {
                '"' => in_string = true,
                '{' => depth += 1,
                '}' => {
                    depth -= 1;
                    if depth == 0 {
                        let inner = &self.text[start..self.pos + i];
                        self.pos += i + 1;
                        return Some(inner);
                    }
                }
                _ => {}
            }
        }
        None
    }

    /// Skip a `;`-terminated statement or a balanced block.
    fn skip_statement(&mut self) {
        self.skip_ws();
        let mut in_string = false;
        let mut pos = self.pos;
        let bytes = self.text.as_bytes();
        while pos < bytes.len() {
            let c = bytes[pos] as char;
            if in_string {
                if c == '\\' {
                    pos += 1;
                } else if c == '"' {
                    in_string = false;
                }
                pos += 1;
                continue;
            }
            match c {
                '"' => {
                    in_string = true;
                    pos += 1;
                }
                ';' => {
                    self.pos = pos + 1;
                    return;
                }
                '{' => {
                    // Rewind-safe: consume from current pos as a block.
                    let mut sub = ProtoCursor {
                        text: self.text,
                        pos,
                    };
                    if sub.consume_block().is_some() {
                        self.pos = sub.pos;
                    } else {
                        self.pos = bytes.len();
                    }
                    return;
                }
                _ => pos += 1,
            }
        }
        self.pos = bytes.len();
    }
}

/// Parse `.proto` source into top-level messages keyed by name.
pub fn parse_proto(text: &str) -> Result<HashMap<String, ProtoMessage>, String> {
    let cleaned = strip_proto_comments(text);
    let mut cursor = ProtoCursor::new(&cleaned);
    let mut messages = HashMap::new();
    while !cursor.at_end() {
        match cursor.consume_ident().as_deref() {
            Some("message") => {
                let name = cursor
                    .consume_ident()
                    .ok_or_else(|| "Expected message name".to_string())?;
                let inner = cursor
                    .consume_block()
                    .ok_or_else(|| format!("Unclosed message {}", name))?;
                messages.insert(name.clone(), parse_message_body(&name, inner)?);
            }
            Some(_) => cursor.skip_statement(),
            None => break,
        }
    }
    Ok(messages)
}

fn parse_message_body(name: &str, body: &str) -> Result<ProtoMessage, String> {
    let mut cursor = ProtoCursor::new(body);
    let mut msg = ProtoMessage {
        name: name.to_string(),
        fields: Vec::new(),
        nested: HashMap::new(),
    };
    while !cursor.at_end() {
        // Nested message / enum definitions attach to the parent message.
        if cursor.rest().trim_start().starts_with("message ") {
            cursor.consume_ident(); // "message"
            let nested_name = cursor
                .consume_ident()
                .ok_or_else(|| format!("Expected nested message name in {}", name))?;
            let inner = cursor
                .consume_block()
                .ok_or_else(|| format!("Unclosed nested message {}", nested_name))?;
            let nested = parse_message_body(&nested_name, inner)?;
            msg.nested.insert(nested_name, nested);
            continue;
        }
        if cursor.rest().trim_start().starts_with("enum ") {
            cursor.consume_ident(); // "enum"
            cursor.consume_ident(); // name (ignored)
            if cursor.consume_block().is_none() {
                cursor.skip_statement();
            }
            continue;
        }
        if cursor.rest().trim_start().starts_with("oneof ") {
            cursor.consume_ident(); // "oneof"
            cursor.consume_ident(); // name (ignored)
            if cursor.consume_block().is_none() {
                cursor.skip_statement();
            }
            continue;
        }
        // Field: [label] type name = number [options];
        let first = cursor.consume_ident().ok_or_else(|| {
            format!(
                "Expected field declaration in message {}, got {:?}",
                name,
                {
                    let r = cursor.rest();
                    &r[..r.len().min(20)]
                }
            )
        })?;
        let (repeated, type_name) = match first.as_str() {
            "repeated" | "optional" | "required" => (
                first == "repeated",
                cursor
                    .consume_ident()
                    .ok_or_else(|| format!("Expected field type in message {}", name))?,
            ),
            _ => (false, first),
        };
        // Skip option/reserved/extension statements masquerading as fields.
        if matches!(
            type_name.as_str(),
            "option" | "reserved" | "extensions" | "extend"
        ) {
            cursor.skip_statement();
            continue;
        }
        let field_name = cursor
            .consume_ident()
            .ok_or_else(|| format!("Expected field name in message {}", name))?;
        if !cursor.expect_char('=') {
            return Err(format!(
                "Expected '=' after field {} in message {}",
                field_name, name
            ));
        }
        cursor.skip_ws();
        let rest = cursor.rest();
        let num_end = rest
            .find(|c: char| !c.is_ascii_digit())
            .unwrap_or(rest.len());
        if num_end == 0 {
            return Err(format!(
                "Expected field number for {} in message {}",
                field_name, name
            ));
        }
        let number: u32 = rest[..num_end]
            .parse()
            .map_err(|_| format!("Bad field number for {} in message {}", field_name, name))?;
        cursor.pos += num_end;
        cursor.skip_statement();
        msg.fields.push(ProtoField {
            name: field_name,
            proto_type: type_name
                .trim_start_matches('.')
                .rsplit('.')
                .next()
                .unwrap_or(&type_name)
                .to_string(),
            number,
            repeated,
        });
    }
    Ok(msg)
}

// ---------------------------------------------------------------------------
// Protobuf wire encoding
// ---------------------------------------------------------------------------

fn read_varint(bytes: &[u8], pos: &mut usize) -> Option<u64> {
    let mut result: u64 = 0;
    for shift in (0..64).step_by(7) {
        let b = *bytes.get(*pos)?;
        *pos += 1;
        result |= ((b & 0x7F) as u64) << shift;
        if b & 0x80 == 0 {
            return Some(result);
        }
    }
    None
}

fn write_varint(out: &mut Vec<u8>, mut value: u64) {
    loop {
        let bits = (value & 0x7F) as u8;
        value >>= 7;
        if value == 0 {
            out.push(bits);
            return;
        }
        out.push(bits | 0x80);
    }
}

fn write_tag(out: &mut Vec<u8>, number: u32, wire_type: u8) {
    write_varint(out, ((number as u64) << 3) | wire_type as u64);
}

fn num_to_i64(v: &Value) -> Option<i64> {
    if let Some(i) = v.as_i64() {
        return Some(i);
    }
    if let Some(u) = v.as_u64() {
        return i64::try_from(u).ok();
    }
    if let Some(f) = v.as_f64() {
        if f.is_finite() {
            return Some(f.trunc() as i64);
        }
    }
    if let Some(s) = v.as_str() {
        let t = s.trim();
        if let Ok(i) = t.parse::<i64>() {
            return Some(i);
        }
    }
    if let Some(b) = v.as_bool() {
        return Some(i64::from(b));
    }
    None
}

fn num_to_u64(v: &Value) -> Option<u64> {
    if let Some(u) = v.as_u64() {
        return Some(u);
    }
    if let Some(i) = v.as_i64() {
        return u64::try_from(i).ok();
    }
    if let Some(f) = v.as_f64() {
        if f.is_finite() && f >= 0.0 {
            return Some(f.trunc() as u64);
        }
    }
    if let Some(s) = v.as_str() {
        let t = s.trim();
        if let Ok(u) = t.parse::<u64>() {
            return Some(u);
        }
    }
    if let Some(b) = v.as_bool() {
        return Some(u64::from(b));
    }
    None
}

fn value_to_bool(v: &Value) -> Option<bool> {
    match v {
        Value::Bool(b) => Some(*b),
        Value::Number(_) => v.as_f64().map(|f| f != 0.0),
        Value::String(s) => match s.trim().to_ascii_lowercase().as_str() {
            "true" | "1" => Some(true),
            "false" | "0" => Some(false),
            _ => None,
        },
        _ => None,
    }
}

/// Pure-Rust protobuf wire codec operating on parsed message definitions.
pub struct ProtobufCodec;

impl ProtobufCodec {
    /// Decode wire bytes into a JSON object following `msg`. Unknown field
    /// numbers are skipped; malformed input yields `Value::Null`.
    pub fn decode(bytes: &[u8], msg: &ProtoMessage) -> Value {
        Self::decode_inner(bytes, msg).unwrap_or(Value::Null)
    }

    fn decode_inner(bytes: &[u8], msg: &ProtoMessage) -> Option<Value> {
        let mut map = serde_json::Map::new();
        let mut pos = 0;
        while pos < bytes.len() {
            let tag = read_varint(bytes, &mut pos)?;
            let number = (tag >> 3) as u32;
            let wire_type = (tag & 0x07) as u8;
            let field = msg.fields.iter().find(|f| f.number == number);
            match field {
                None => {
                    Self::skip_field(bytes, &mut pos, wire_type)?;
                }
                Some(field) => match Self::decode_field(bytes, &mut pos, wire_type, field, msg) {
                    Err(()) => return None,
                    Ok(None) => {}
                    Ok(Some(value)) => {
                        if field.repeated {
                            match map.remove(&field.name) {
                                Some(Value::Array(mut arr)) => {
                                    arr.push(value);
                                    map.insert(field.name.clone(), Value::Array(arr));
                                }
                                Some(prev) => {
                                    map.insert(field.name.clone(), Value::Array(vec![prev, value]));
                                }
                                None => {
                                    map.insert(field.name.clone(), Value::Array(vec![value]));
                                }
                            }
                        } else {
                            map.insert(field.name.clone(), value);
                        }
                    }
                },
            }
        }
        Some(Value::Object(map))
    }

    fn skip_field(bytes: &[u8], pos: &mut usize, wire_type: u8) -> Option<()> {
        match wire_type {
            0 => {
                read_varint(bytes, pos)?;
            }
            1 => {
                *pos = pos.checked_add(8)?;
                if *pos > bytes.len() {
                    return None;
                }
            }
            2 => {
                let len = read_varint(bytes, pos)? as usize;
                *pos = pos.checked_add(len)?;
                if *pos > bytes.len() {
                    return None;
                }
            }
            5 => {
                *pos = pos.checked_add(4)?;
                if *pos > bytes.len() {
                    return None;
                }
            }
            _ => return None,
        }
        Some(())
    }

    fn read_len_bytes<'a>(bytes: &'a [u8], pos: &mut usize) -> Option<&'a [u8]> {
        let len = read_varint(bytes, pos)? as usize;
        let end = pos.checked_add(len)?;
        if end > bytes.len() {
            return None;
        }
        let slice = &bytes[*pos..end];
        *pos = end;
        Some(slice)
    }

    /// Decode one known field: `Ok(Some)` decoded value, `Ok(None)` skip
    /// without failing (unknown nested type), `Err` malformed input, which
    /// fails the whole message. Every path either advances `pos` or errors,
    /// so callers can never spin.
    fn decode_field(
        bytes: &[u8],
        pos: &mut usize,
        wire_type: u8,
        field: &ProtoField,
        msg: &ProtoMessage,
    ) -> Result<Option<Value>, ()> {
        match (field.proto_type.as_str(), wire_type) {
            ("int32", 0) => read_varint(bytes, pos)
                .map(|v| Value::from(v as i32))
                .map(Some)
                .ok_or(()),
            ("int64", 0) => read_varint(bytes, pos)
                .map(|v| Value::from(v as i64))
                .map(Some)
                .ok_or(()),
            ("uint32", 0) => read_varint(bytes, pos)
                .map(|v| Value::from(v as u32))
                .map(Some)
                .ok_or(()),
            ("uint64", 0) => read_varint(bytes, pos).map(Value::from).map(Some).ok_or(()),
            ("sint32", 0) => read_varint(bytes, pos)
                .map(|v| ((v >> 1) as i64) ^ -((v & 1) as i64))
                .map(|v| Value::from(v as i32))
                .map(Some)
                .ok_or(()),
            ("sint64", 0) => read_varint(bytes, pos)
                .map(|v| ((v >> 1) as i64) ^ -((v & 1) as i64))
                .map(Value::from)
                .map(Some)
                .ok_or(()),
            ("bool", 0) => read_varint(bytes, pos)
                .map(|v| Value::Bool(v != 0))
                .map(Some)
                .ok_or(()),
            _ => Self::decode_field_sized(bytes, pos, wire_type, field, msg),
        }
    }

    /// JSON numbers cannot hold NaN/infinity: non-finite floats decode to Null.
    fn finite_number(f: f64) -> Option<Value> {
        serde_json::Number::from_f64(f).map(Value::Number)
    }

    fn decode_field_sized(
        bytes: &[u8],
        pos: &mut usize,
        wire_type: u8,
        field: &ProtoField,
        msg: &ProtoMessage,
    ) -> Result<Option<Value>, ()> {
        match (field.proto_type.as_str(), wire_type) {
            ("string", 2) => {
                let raw = Self::read_len_bytes(bytes, pos).ok_or(())?;
                Ok(Some(Value::String(
                    String::from_utf8_lossy(raw).into_owned(),
                )))
            }
            ("bytes", 2) => {
                let raw = Self::read_len_bytes(bytes, pos).ok_or(())?;
                Ok(Some(Value::String(
                    base64::engine::general_purpose::STANDARD.encode(raw),
                )))
            }
            ("float", 5) => {
                let raw = Self::read_fixed::<4>(bytes, pos).ok_or(())?;
                Ok(Self::finite_number(f32::from_le_bytes(raw) as f64))
            }
            ("double", 1) => {
                let raw = Self::read_fixed::<8>(bytes, pos).ok_or(())?;
                Ok(Self::finite_number(f64::from_le_bytes(raw)))
            }
            ("fixed64", 1) => {
                let raw = Self::read_fixed::<8>(bytes, pos).ok_or(())?;
                Ok(Some(Value::from(u64::from_le_bytes(raw))))
            }
            ("sfixed64", 1) => {
                let raw = Self::read_fixed::<8>(bytes, pos).ok_or(())?;
                Ok(Some(Value::from(i64::from_le_bytes(raw))))
            }
            ("fixed32", 5) => {
                let raw = Self::read_fixed::<4>(bytes, pos).ok_or(())?;
                Ok(Some(Value::from(u32::from_le_bytes(raw))))
            }
            ("sfixed32", 5) => {
                let raw = Self::read_fixed::<4>(bytes, pos).ok_or(())?;
                Ok(Some(Value::from(i32::from_le_bytes(raw))))
            }
            _ => {
                // Nested message or unrecognized pairing: recurse when the
                // nested definition is known, otherwise skip the field (still
                // consuming its bytes so decoding always advances).
                if wire_type == 2 {
                    if let Some(nested) = msg.nested.get(field.proto_type.as_str()) {
                        let raw = Self::read_len_bytes(bytes, pos).ok_or(())?;
                        return Ok(Self::decode_inner(raw, nested));
                    }
                }
                Self::skip_field(bytes, pos, wire_type).ok_or(())?;
                Ok(None)
            }
        }
    }

    fn read_fixed<const N: usize>(bytes: &[u8], pos: &mut usize) -> Option<[u8; N]> {
        let end = pos.checked_add(N)?;
        if end > bytes.len() {
            return None;
        }
        let mut out = [0u8; N];
        out.copy_from_slice(&bytes[*pos..end]);
        *pos = end;
        Some(out)
    }

    /// Encode a JSON object into wire bytes following `msg`. Fields missing
    /// from the value (or not convertible) are skipped; unknown JSON keys
    /// are ignored.
    pub fn encode(value: &Value, msg: &ProtoMessage) -> Vec<u8> {
        let mut out = Vec::new();
        let Value::Object(map) = value else {
            return out;
        };
        for field in &msg.fields {
            let values: Vec<&Value> = match map.get(&field.name) {
                Some(Value::Array(items)) if field.repeated => items.iter().collect(),
                Some(v) => vec![v],
                None => continue,
            };
            for v in values {
                Self::encode_field(&mut out, field, v, msg);
            }
        }
        out
    }

    fn encode_field(out: &mut Vec<u8>, field: &ProtoField, v: &Value, msg: &ProtoMessage) {
        match field.proto_type.as_str() {
            "int32" => {
                if let Some(i) = num_to_i64(v) {
                    write_tag(out, field.number, 0);
                    write_varint(out, (i as i32 as i64) as u64);
                }
            }
            "int64" => {
                if let Some(i) = num_to_i64(v) {
                    write_tag(out, field.number, 0);
                    write_varint(out, i as u64);
                }
            }
            "uint32" => {
                if let Some(u) = num_to_u64(v) {
                    write_tag(out, field.number, 0);
                    write_varint(out, u);
                }
            }
            "uint64" => {
                if let Some(u) = num_to_u64(v) {
                    write_tag(out, field.number, 0);
                    write_varint(out, u);
                }
            }
            "sint32" => {
                if let Some(i) = num_to_i64(v) {
                    let i = i as i32;
                    write_tag(out, field.number, 0);
                    write_varint(out, (((i << 1) ^ (i >> 31)) as u32) as u64);
                }
            }
            "sint64" => {
                if let Some(i) = num_to_i64(v) {
                    write_tag(out, field.number, 0);
                    write_varint(out, ((i << 1) ^ (i >> 63)) as u64);
                }
            }
            "bool" => {
                if let Some(b) = value_to_bool(v) {
                    write_tag(out, field.number, 0);
                    write_varint(out, u64::from(b));
                }
            }
            "float" => {
                if let Some(f) = v.as_f64().or_else(|| num_to_i64(v).map(|i| i as f64)) {
                    write_tag(out, field.number, 5);
                    out.extend_from_slice(&(f as f32).to_le_bytes());
                } else if let Some(s) = v.as_str() {
                    if let Ok(f) = s.trim().parse::<f32>() {
                        write_tag(out, field.number, 5);
                        out.extend_from_slice(&f.to_le_bytes());
                    }
                }
            }
            "double" => {
                if let Some(f) = v.as_f64().or_else(|| num_to_i64(v).map(|i| i as f64)) {
                    write_tag(out, field.number, 1);
                    out.extend_from_slice(&f.to_le_bytes());
                } else if let Some(s) = v.as_str() {
                    if let Ok(f) = s.trim().parse::<f64>() {
                        write_tag(out, field.number, 1);
                        out.extend_from_slice(&f.to_le_bytes());
                    }
                }
            }
            "fixed64" => {
                if let Some(u) = num_to_u64(v) {
                    write_tag(out, field.number, 1);
                    out.extend_from_slice(&u.to_le_bytes());
                }
            }
            "sfixed64" => {
                if let Some(i) = num_to_i64(v) {
                    write_tag(out, field.number, 1);
                    out.extend_from_slice(&i.to_le_bytes());
                }
            }
            "fixed32" => {
                if let Some(u) = num_to_u64(v) {
                    write_tag(out, field.number, 5);
                    out.extend_from_slice(&(u as u32).to_le_bytes());
                }
            }
            "sfixed32" => {
                if let Some(i) = num_to_i64(v) {
                    write_tag(out, field.number, 5);
                    out.extend_from_slice(&(i as i32).to_le_bytes());
                }
            }
            "string" => {
                let s = match v {
                    Value::String(s) => s.clone(),
                    Value::Null => return,
                    other => serde_json::to_string(other).unwrap_or_default(),
                };
                write_tag(out, field.number, 2);
                write_varint(out, s.len() as u64);
                out.extend_from_slice(s.as_bytes());
            }
            "bytes" => {
                let raw = match v {
                    Value::String(s) => base64::engine::general_purpose::STANDARD
                        .decode(s.trim())
                        .unwrap_or_else(|_| s.as_bytes().to_vec()),
                    Value::Null => return,
                    other => serde_json::to_vec(other).unwrap_or_default(),
                };
                write_tag(out, field.number, 2);
                write_varint(out, raw.len() as u64);
                out.extend_from_slice(&raw);
            }
            _ => {
                if let Value::Object(_) = v {
                    if let Some(nested) = msg.nested.get(field.proto_type.as_str()) {
                        let inner = Self::encode(v, nested);
                        write_tag(out, field.number, 2);
                        write_varint(out, inner.len() as u64);
                        out.extend_from_slice(&inner);
                    }
                }
            }
        }
    }
}
