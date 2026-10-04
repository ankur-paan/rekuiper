use rekuiper_sql::{Evaluator, Parser};
use serde_json::Value;
use std::collections::HashMap;

#[test]
fn test_single_stream_qualified_field_access() {
    let mut parser = Parser::new("SELECT s.k AS k, s.av AS av FROM s");
    let stmt = parser.parse_select().unwrap();

    let mut record = HashMap::new();
    record.insert("k".to_string(), Value::from(1));
    record.insert("av".to_string(), Value::from("a1"));

    let out = Evaluator::eval_select(&stmt, &record).unwrap();
    assert_eq!(out.get("k"), Some(&Value::from(1)));
    assert_eq!(out.get("av"), Some(&Value::from("a1")));
}

#[test]
fn test_single_stream_qualified_wildcard() {
    let mut parser = Parser::new("SELECT s.* FROM s");
    let stmt = parser.parse_select().unwrap();

    let mut record = HashMap::new();
    record.insert("k".to_string(), Value::from(1));
    record.insert("av".to_string(), Value::from("a1"));

    let out = Evaluator::eval_select(&stmt, &record).unwrap();
    assert_eq!(out.get("k"), Some(&Value::from(1)));
    assert_eq!(out.get("av"), Some(&Value::from("a1")));
}

#[test]
fn test_nested_object_vs_qualified_column() {
    // If a row genuinely has a nested object `s: { k: 2 }`, it should take precedence.
    let mut parser = Parser::new("SELECT s.k AS k FROM s");
    let stmt = parser.parse_select().unwrap();

    let mut record = HashMap::new();
    record.insert("s".to_string(), serde_json::json!({"k": 2}));
    record.insert("k".to_string(), Value::from(1));

    let out = Evaluator::eval_select(&stmt, &record).unwrap();
    assert_eq!(out.get("k"), Some(&Value::from(2)));
}
