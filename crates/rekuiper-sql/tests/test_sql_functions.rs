use rekuiper_sql::{Evaluator, Parser, RuleState};
use serde_json::{json, Value};
use std::collections::HashMap;

type Record = HashMap<String, Value>;

fn empty() -> Record {
    HashMap::new()
}

fn rec(pairs: &[(&str, Value)]) -> Record {
    pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.clone()))
        .collect()
}

/// Evaluate a single-projection `SELECT <expr> AS v ...` and return `v`.
fn eval_one(sql: &str, record: &Record) -> Value {
    let mut parser = Parser::new(sql);
    let stmt = parser.parse_select().expect("Should parse");
    assert_eq!(stmt.fields.len(), 1);
    Evaluator::eval_select(&stmt, record)
        .expect("Should project")
        .remove("v")
        .unwrap()
}

/// Evaluate a single-projection aggregate query over a batch.
fn eval_agg_one(sql: &str, records: &[Record]) -> Value {
    let mut parser = Parser::new(sql);
    let stmt = parser.parse_select().expect("Should parse");
    assert_eq!(stmt.fields.len(), 1);
    Evaluator::eval_aggregate(&stmt, records)
        .expect("Should aggregate")
        .remove("v")
        .unwrap()
}

// Fixed point in time: 2024-01-01T00:00:00Z (a Monday, UTC).
const T0: i64 = 1_704_067_200_000;

// ---------------------------------------------------------------------------
// DateTime functions
// ---------------------------------------------------------------------------

#[test]
fn test_datetime_now() {
    // Current epoch millis: a positive integer near "now".
    let before = chrono::Utc::now().timestamp_millis();
    let v = eval_one("SELECT now() AS v FROM demo", &empty());
    let after = chrono::Utc::now().timestamp_millis();
    let t = v.as_i64().expect("now() returns an integer");
    assert!(t >= before && t <= after, "now() out of range: {}", t);
    // Arity is strict.
    assert_eq!(
        eval_one("SELECT now(1) AS v FROM demo", &empty()),
        Value::Null
    );
}

#[test]
fn test_datetime_format_date() {
    assert_eq!(
        eval_one(
            "SELECT format_date(1704067200000, '%Y-%m-%d %H:%M:%S') AS v FROM demo",
            &empty()
        ),
        json!("2024-01-01 00:00:00")
    );
    assert_eq!(
        eval_one(
            "SELECT format_date(1704067200000, '%Y-%m-%dT%H:%M:%SZ') AS v FROM demo",
            &empty()
        ),
        json!("2024-01-01T00:00:00Z")
    );
    // RFC3339 input is accepted as well.
    assert_eq!(
        eval_one(
            "SELECT format_date('2024-01-01T00:00:00Z', '%Y-%m-%d') AS v FROM demo",
            &empty()
        ),
        json!("2024-01-01")
    );
    // Garbage in -> Null.
    assert_eq!(
        eval_one(
            "SELECT format_date('not-a-date', '%Y') AS v FROM demo",
            &empty()
        ),
        Value::Null
    );
    assert_eq!(
        eval_one(
            "SELECT format_date(1704067200000, 42) AS v FROM demo",
            &empty()
        ),
        Value::Null
    );
}

#[test]
fn test_datetime_date_parse() {
    assert_eq!(
        eval_one(
            "SELECT date_parse('2024-01-01 00:00:00', '%Y-%m-%d %H:%M:%S') AS v FROM demo",
            &empty()
        ),
        json!(T0)
    );
    // Date-only format resolves to midnight UTC.
    assert_eq!(
        eval_one(
            "SELECT date_parse('2024-01-01', '%Y-%m-%d') AS v FROM demo",
            &empty()
        ),
        json!(T0)
    );
    // Round-trips with format_date (2024-06-15T12:30:00Z).
    assert_eq!(
        eval_one(
            "SELECT date_parse('2024-06-15T12:30:00Z', '%Y-%m-%dT%H:%M:%SZ') AS v FROM demo",
            &empty()
        ),
        json!(1_718_454_600_000i64)
    );
    assert_eq!(
        eval_one("SELECT date_parse('nope', '%Y') AS v FROM demo", &empty()),
        Value::Null
    );
}

#[test]
fn test_datetime_date_add() {
    assert_eq!(
        eval_one(
            "SELECT date_add('hh', 2, 1704067200000) AS v FROM demo",
            &empty()
        ),
        json!(1_704_074_400_000i64)
    );
    assert_eq!(
        eval_one(
            "SELECT date_add('dd', 1, 1704067200000) AS v FROM demo",
            &empty()
        ),
        json!(1_704_153_600_000i64)
    );
    assert_eq!(
        eval_one(
            "SELECT date_add('mi', 90, 1704067200000) AS v FROM demo",
            &empty()
        ),
        json!(1_704_072_600_000i64)
    );
    assert_eq!(
        eval_one(
            "SELECT date_add('ss', 30, 1704067200000) AS v FROM demo",
            &empty()
        ),
        json!(1_704_067_230_000i64)
    );
    assert_eq!(
        eval_one(
            "SELECT date_add('ms', 500, 1704067200000) AS v FROM demo",
            &empty()
        ),
        json!(1_704_067_200_500i64)
    );
    // Aliases and negatives work.
    assert_eq!(
        eval_one(
            "SELECT date_add('day', -1, 1704067200000) AS v FROM demo",
            &empty()
        ),
        json!(1_703_980_800_000i64)
    );
    assert_eq!(
        eval_one(
            "SELECT date_add('minute', 1, 1704067200000) AS v FROM demo",
            &empty()
        ),
        json!(1_704_067_260_000i64)
    );
    // Unknown unit -> Null.
    assert_eq!(
        eval_one(
            "SELECT date_add('xx', 1, 1704067200000) AS v FROM demo",
            &empty()
        ),
        Value::Null
    );
}

#[test]
fn test_datetime_date_diff() {
    assert_eq!(
        eval_one(
            "SELECT date_diff('hh', 1704067200000, 1704074400000) AS v FROM demo",
            &empty()
        ),
        json!(2)
    );
    assert_eq!(
        eval_one(
            "SELECT date_diff('dd', 1704067200000, 1704153600000) AS v FROM demo",
            &empty()
        ),
        json!(1)
    );
    assert_eq!(
        eval_one(
            "SELECT date_diff('mi', 1704067200000, 1704067260000) AS v FROM demo",
            &empty()
        ),
        json!(1)
    );
    assert_eq!(
        eval_one(
            "SELECT date_diff('ss', 1704067200000, 1704067200500) AS v FROM demo",
            &empty()
        ),
        json!(0)
    );
    assert_eq!(
        eval_one(
            "SELECT date_diff('ms', 1704067200000, 1704067200500) AS v FROM demo",
            &empty()
        ),
        json!(500)
    );
    // Reversed order goes negative.
    assert_eq!(
        eval_one(
            "SELECT date_diff('hh', 1704074400000, 1704067200000) AS v FROM demo",
            &empty()
        ),
        json!(-2)
    );
}

#[test]
fn test_datetime_components() {
    // 2024-01-01T00:00:00Z plus 36h01m02s = 2024-01-02T12:01:02Z.
    let ts = T0 + 36 * 3_600_000 + 60_000 + 2_000;
    let r = rec(&[("ts", json!(ts))]);
    assert_eq!(eval_one("SELECT year(ts) AS v FROM demo", &r), json!(2024));
    assert_eq!(eval_one("SELECT month(ts) AS v FROM demo", &r), json!(1));
    assert_eq!(eval_one("SELECT day(ts) AS v FROM demo", &r), json!(2));
    assert_eq!(eval_one("SELECT hour(ts) AS v FROM demo", &r), json!(12));
    assert_eq!(eval_one("SELECT minute(ts) AS v FROM demo", &r), json!(1));
    assert_eq!(eval_one("SELECT second(ts) AS v FROM demo", &r), json!(2));
    // Missing column -> Null.
    assert_eq!(
        eval_one("SELECT year(nope) AS v FROM demo", &empty()),
        Value::Null
    );
}

// ---------------------------------------------------------------------------
// JSON path functions
// ---------------------------------------------------------------------------

fn doc_record() -> Record {
    rec(&[
        ("doc", json!({"a": {"b": 1, "c": [10, 20]}})),
        ("arr", json!([{"k": "x"}, {"k": "y"}])),
        ("nums", json!([7, 8])),
    ])
}

#[test]
fn test_json_path_query() {
    let r = doc_record();
    // Dot notation.
    assert_eq!(
        eval_one("SELECT json_path_query(doc, 'a.b') AS v FROM demo", &r),
        json!(1)
    );
    assert_eq!(
        eval_one("SELECT json_path_query(doc, 'a.c[1]') AS v FROM demo", &r),
        json!(20)
    );
    // JSON pointer.
    assert_eq!(
        eval_one("SELECT json_path_query(doc, '/a/b') AS v FROM demo", &r),
        json!(1)
    );
    assert_eq!(
        eval_one("SELECT json_path_query(arr, '/0/k') AS v FROM demo", &r),
        json!("x")
    );
    // Whole subtrees come back intact.
    assert_eq!(
        eval_one("SELECT json_path_query(doc, 'a.c') AS v FROM demo", &r),
        json!([10, 20])
    );
    // Missing paths -> Null.
    assert_eq!(
        eval_one("SELECT json_path_query(doc, 'a.zzz') AS v FROM demo", &r),
        Value::Null
    );
    assert_eq!(
        eval_one("SELECT json_path_query(doc, '/a/9') AS v FROM demo", &r),
        Value::Null
    );
}

#[test]
fn test_json_path_query_first() {
    let r = doc_record();
    // Arrays collapse to their first element; scalars pass through.
    assert_eq!(
        eval_one(
            "SELECT json_path_query_first(doc, 'a.c') AS v FROM demo",
            &r
        ),
        json!(10)
    );
    assert_eq!(
        eval_one("SELECT json_path_query_first(nums, '') AS v FROM demo", &r),
        json!(7)
    );
    assert_eq!(
        eval_one(
            "SELECT json_path_query_first(doc, 'a.b') AS v FROM demo",
            &r
        ),
        json!(1)
    );
    assert_eq!(
        eval_one(
            "SELECT json_path_query_first(doc, 'missing') AS v FROM demo",
            &r
        ),
        Value::Null
    );
}

#[test]
fn test_json_path_exists() {
    let r = doc_record();
    assert_eq!(
        eval_one("SELECT json_path_exists(doc, 'a.b') AS v FROM demo", &r),
        json!(true)
    );
    assert_eq!(
        eval_one("SELECT json_path_exists(doc, 'a.nope') AS v FROM demo", &r),
        json!(false)
    );
    assert_eq!(
        eval_one("SELECT json_path_exists(arr, '/1/k') AS v FROM demo", &r),
        json!(true)
    );
}

#[test]
fn test_json_map() {
    assert_eq!(
        eval_one(
            "SELECT json_map('k1', 1, 'k2', 'v') AS v FROM demo",
            &empty()
        ),
        json!({"k1": 1, "k2": "v"})
    );
    assert_eq!(
        eval_one("SELECT json_map() AS v FROM demo", &empty()),
        json!({})
    );
    // Non-string keys are stringified; odd arity -> Null.
    assert_eq!(
        eval_one("SELECT json_map(1, 'a') AS v FROM demo", &empty()),
        json!({"1": "a"})
    );
    assert_eq!(
        eval_one("SELECT json_map('k1') AS v FROM demo", &empty()),
        Value::Null
    );
}

// ---------------------------------------------------------------------------
// Crypto & encoding functions
// ---------------------------------------------------------------------------

#[test]
fn test_crypto_hashes() {
    assert_eq!(
        eval_one("SELECT md5('hello') AS v FROM demo", &empty()),
        json!("5d41402abc4b2a76b9719d911017c592")
    );
    assert_eq!(
        eval_one("SELECT sha256('hello') AS v FROM demo", &empty()),
        json!("2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824")
    );
    assert_eq!(
        eval_one("SELECT sha512('hello') AS v FROM demo", &empty()),
        json!("9b71d224bd62f3785d96d46ad3ea3d73319bfbc2890caadae2dff72519673ca72323c3d99ba5c11d7c7acc6e14b8c5da0c4663475c2e5c3adef46f73bcdec043")
    );
    // Empty string hashes to the well-known empty digests.
    assert_eq!(
        eval_one("SELECT md5('') AS v FROM demo", &empty()),
        json!("d41d8cd98f00b204e9800998ecf8427e")
    );
    // Null input -> Null.
    assert_eq!(
        eval_one("SELECT sha256(nothing_here) AS v FROM demo", &empty()),
        Value::Null
    );
}

#[test]
fn test_base64_encode_decode() {
    assert_eq!(
        eval_one("SELECT encode('hello', 'base64') AS v FROM demo", &empty()),
        json!("aGVsbG8=")
    );
    assert_eq!(
        eval_one("SELECT base64_encode('hello') AS v FROM demo", &empty()),
        json!("aGVsbG8=")
    );
    assert_eq!(
        eval_one(
            "SELECT decode('aGVsbG8=', 'base64') AS v FROM demo",
            &empty()
        ),
        json!("hello")
    );
    assert_eq!(
        eval_one("SELECT base64_decode('aGVsbG8=') AS v FROM demo", &empty()),
        json!("hello")
    );
    // Round-trip through both spellings.
    assert_eq!(
        eval_one(
            "SELECT base64_decode(base64_encode('rekuiper')) AS v FROM demo",
            &empty()
        ),
        json!("rekuiper")
    );
    // Unknown methods and garbage -> Null.
    assert_eq!(
        eval_one("SELECT encode('hello', 'hex') AS v FROM demo", &empty()),
        Value::Null
    );
    assert_eq!(
        eval_one("SELECT base64_decode('!!!') AS v FROM demo", &empty()),
        Value::Null
    );
}

// ---------------------------------------------------------------------------
// Extended array functions
// ---------------------------------------------------------------------------

fn arr_record() -> Record {
    rec(&[
        ("arr", json!([10, 20, 30])),
        ("dups", json!([1, 2, 2, 3, 1])),
        ("words", json!(["a", "b"])),
    ])
}

#[test]
fn test_array_create_position_length() {
    assert_eq!(
        eval_one("SELECT array_create(1, 'a', true) AS v FROM demo", &empty()),
        json!([1, "a", true])
    );
    assert_eq!(
        eval_one("SELECT array_create() AS v FROM demo", &empty()),
        json!([])
    );
    let r = arr_record();
    assert_eq!(
        eval_one("SELECT array_position(arr, 20) AS v FROM demo", &r),
        json!(2)
    );
    assert_eq!(
        eval_one("SELECT array_position(arr, 10) AS v FROM demo", &r),
        json!(1)
    );
    assert_eq!(
        eval_one("SELECT array_position(arr, 99) AS v FROM demo", &r),
        json!(0)
    );
    assert_eq!(
        eval_one("SELECT array_length(arr) AS v FROM demo", &r),
        json!(3)
    );
    assert_eq!(
        eval_one("SELECT array_length(words) AS v FROM demo", &r),
        json!(2)
    );
    // Non-arrays -> Null.
    assert_eq!(
        eval_one("SELECT array_length(5) AS v FROM demo", &empty()),
        Value::Null
    );
    assert_eq!(
        eval_one("SELECT array_position(5, 5) AS v FROM demo", &empty()),
        Value::Null
    );
}

#[test]
fn test_array_slice_concat_deduplicate() {
    let r = arr_record();
    assert_eq!(
        eval_one("SELECT array_slice(arr, 2, 3) AS v FROM demo", &r),
        json!([20, 30])
    );
    assert_eq!(
        eval_one("SELECT array_slice(arr, 1, 1) AS v FROM demo", &r),
        json!([10])
    );
    // Bounds clamp; inverted ranges are empty.
    assert_eq!(
        eval_one("SELECT array_slice(arr, 2, 99) AS v FROM demo", &r),
        json!([20, 30])
    );
    assert_eq!(
        eval_one("SELECT array_slice(arr, 3, 1) AS v FROM demo", &r),
        json!([])
    );
    assert_eq!(
        eval_one("SELECT array_concat(arr, words) AS v FROM demo", &r),
        json!([10, 20, 30, "a", "b"])
    );
    assert_eq!(
        eval_one("SELECT deduplicate(dups) AS v FROM demo", &r),
        json!([1, 2, 3])
    );
    assert_eq!(
        eval_one("SELECT deduplicate(words) AS v FROM demo", &r),
        json!(["a", "b"])
    );
}

// ---------------------------------------------------------------------------
// Aggregate & analytic functions
// ---------------------------------------------------------------------------

fn temp_batch() -> Vec<Record> {
    vec![
        rec(&[("temp", json!(10))]),
        rec(&[("temp", json!(20))]),
        rec(&[("temp", json!(30))]),
    ]
}

#[test]
fn test_aggregate_collect_latest_lead() {
    let rows = temp_batch();
    assert_eq!(
        eval_agg_one("SELECT collect(temp) AS v FROM demo", &rows),
        json!([10, 20, 30])
    );
    assert_eq!(
        eval_agg_one("SELECT latest(temp) AS v FROM demo", &rows),
        json!(30)
    );
    // lead() reads forward from the window start (0-based offset).
    assert_eq!(
        eval_agg_one("SELECT lead(temp) AS v FROM demo", &rows),
        json!(20)
    );
    assert_eq!(
        eval_agg_one("SELECT lead(temp, 2) AS v FROM demo", &rows),
        json!(30)
    );
    assert_eq!(
        eval_agg_one("SELECT lead(temp, 0) AS v FROM demo", &rows),
        json!(10)
    );
    assert_eq!(
        eval_agg_one("SELECT lead(temp, 5, -1) AS v FROM demo", &rows),
        json!(-1)
    );
    assert_eq!(
        eval_agg_one("SELECT lead(temp, 5) AS v FROM demo", &rows),
        Value::Null
    );

    // Nulls are skipped by collect, latest sees past them.
    let gappy = vec![
        rec(&[("temp", json!(10))]),
        rec(&[("temp", Value::Null)]),
        rec(&[("temp", json!(30))]),
    ];
    assert_eq!(
        eval_agg_one("SELECT collect(temp) AS v FROM demo", &gappy),
        json!([10, 30])
    );
    assert_eq!(
        eval_agg_one("SELECT latest(temp) AS v FROM demo", &gappy),
        json!(30)
    );
    let trailing_null = vec![rec(&[("temp", json!(10))]), rec(&[("temp", Value::Null)])];
    assert_eq!(
        eval_agg_one("SELECT latest(temp) AS v FROM demo", &trailing_null),
        json!(10)
    );
    // Degenerate batches.
    let empty: Vec<Record> = vec![];
    assert_eq!(
        eval_agg_one("SELECT collect(temp) AS v FROM demo", &empty),
        json!([])
    );
    assert_eq!(
        eval_agg_one("SELECT latest(temp) AS v FROM demo", &empty),
        Value::Null
    );
}

#[test]
fn test_stateful_had_changed_and_changed_col() {
    let mut parser = Parser::new("SELECT had_changed(temp) AS ch FROM demo");
    let stmt = parser.parse_select().expect("Should parse");
    let state = RuleState::default();
    let row = |t: Value| {
        Evaluator::eval_select_stateful(&stmt, &rec(&[("temp", t)]), &state)
            .expect("row always projects")
            .remove("ch")
            .unwrap()
    };
    assert_eq!(row(json!(10)), json!(true));
    assert_eq!(row(json!(10)), json!(false));
    assert_eq!(row(json!(20)), json!(true));

    let mut parser = Parser::new("SELECT changed_col(temp) AS c FROM demo");
    let stmt = parser.parse_select().expect("Should parse");
    let state = RuleState::default();
    let row = |t: Value| {
        Evaluator::eval_select_stateful(&stmt, &rec(&[("temp", t)]), &state)
            .expect("row always projects")
            .remove("c")
            .unwrap()
    };
    assert_eq!(row(json!(10)), json!(10));
    assert_eq!(row(json!(10)), Value::Null);
    assert_eq!(row(json!(20)), json!(20));

    // Scalar (stateless) fallbacks stay total.
    assert_eq!(
        eval_one("SELECT had_changed(5) AS v FROM demo", &empty()),
        json!(true)
    );
    assert_eq!(
        eval_one("SELECT changed_col(5) AS v FROM demo", &empty()),
        json!(5)
    );
    assert_eq!(
        eval_one("SELECT latest(7) AS v FROM demo", &empty()),
        json!(7)
    );
    assert_eq!(
        eval_one("SELECT collect(7) AS v FROM demo", &empty()),
        json!([7])
    );
    assert_eq!(
        eval_one("SELECT lead(7) AS v FROM demo", &empty()),
        Value::Null
    );
    assert_eq!(
        eval_one("SELECT lead(7, 0, 99) AS v FROM demo", &empty()),
        json!(99)
    );
}
