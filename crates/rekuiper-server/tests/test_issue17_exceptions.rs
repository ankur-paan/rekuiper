use rekuiper_conf::KuiperConfig;
use rekuiper_core::manager::{RuleManager, StreamManager, TableManager};
use rekuiper_core::StreamBus;
use rekuiper_server::routes::{create_router, AppState};
use serde_json::json;
use tokio::net::TcpListener;

async fn spawn_test_server_with_state() -> (String, tokio::task::JoinHandle<()>, AppState) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("Failed to bind ephemeral port");
    let local_addr = listener.local_addr().unwrap();

    let stream_bus = StreamBus::new();
    let stream_manager = StreamManager::new();
    let rule_manager = RuleManager::new(stream_bus.clone());

    let state = AppState::new(
        "test_issue17".to_string(),
        KuiperConfig::default(),
        stream_manager,
        TableManager::new(),
        rule_manager,
        stream_bus,
    );

    let app = create_router(state.clone());
    let handle = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    (format!("http://{}", local_addr), handle, state)
}

#[tokio::test]
async fn test_select_runtime_error_drops_row_and_counts_exception() {
    let (base_url, _handle, state) = spawn_test_server_with_state().await;
    let client = reqwest::Client::new();

    // Create stream s
    let resp = client
        .post(format!("{}/streams", base_url))
        .json(&json!({
            "sql": "CREATE STREAM s () WITH (FORMAT=\"json\")"
        }))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    let mut sink_rx = state.stream_bus.subscribe("sink_abs_default");

    // Create rule: SELECT id, abs(dev) AS v FROM s with sendError: false (default)
    let resp = client
        .post(format!("{}/rules", base_url))
        .json(&json!({
            "id": "rule_abs_default",
            "sql": "SELECT id, abs(dev) AS v FROM s",
            "actions": [{"memory": {"topic": "sink_abs_default"}}]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);

    // 1. Send valid row: abs(-10) -> 10
    let resp = client
        .post(format!("{}/streams/s/data", base_url))
        .json(&json!({"id": 1, "dev": -10}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    let rec = tokio::time::timeout(std::time::Duration::from_secs(2), sink_rx.recv())
        .await
        .expect("timed out waiting for sink output")
        .expect("sink closed");
    assert_eq!(rec.data.get("id"), Some(&json!(1)));
    assert_eq!(rec.data.get("v"), Some(&json!(10)));

    // 2. Send invalid row: abs("d0") -> runtime error!
    let resp = client
        .post(format!("{}/streams/s/data", base_url))
        .json(&json!({"id": 2, "dev": "d0"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    // Wait a moment; no output should be emitted since sendError is false
    let timeout_res =
        tokio::time::timeout(std::time::Duration::from_millis(300), sink_rx.recv()).await;
    assert!(
        timeout_res.is_err(),
        "row with runtime error should be dropped"
    );

    // 3. Verify rule status has exceptions_total = 1 and last_exception recorded
    let resp = client
        .get(format!("{}/rules/rule_abs_default/status", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let status: serde_json::Value = resp.json().await.unwrap();
    let exc_total = status
        .get("exceptions_total")
        .and_then(|v| v.as_u64())
        .unwrap_or(0);
    assert_eq!(exc_total, 1, "exceptions_total must be 1");

    let last_exc = status
        .get("last_exception")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    assert!(
        last_exc.contains("run Select error: alias: v")
            && last_exc.contains("call func abs error: only float64 & int type are supported"),
        "unexpected last_exception: {}",
        last_exc
    );

    // 4. Send next valid row: abs(5) -> 5 (rule still works)
    let resp = client
        .post(format!("{}/streams/s/data", base_url))
        .json(&json!({"id": 3, "dev": 5}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    let rec = tokio::time::timeout(std::time::Duration::from_secs(2), sink_rx.recv())
        .await
        .expect("timed out waiting for next sink output")
        .expect("sink closed");
    assert_eq!(rec.data.get("id"), Some(&json!(3)));
    assert_eq!(rec.data.get("v"), Some(&json!(5)));
}

#[tokio::test]
async fn test_select_runtime_error_with_send_error_true() {
    let (base_url, _handle, state) = spawn_test_server_with_state().await;
    let client = reqwest::Client::new();

    // Create stream s
    let resp = client
        .post(format!("{}/streams", base_url))
        .json(&json!({
            "sql": "CREATE STREAM s () WITH (FORMAT=\"json\")"
        }))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    let mut sink_rx = state.stream_bus.subscribe("sink_abs_send_error");

    // Create rule: SELECT id, abs(dev) AS v FROM s with sendError: true
    let resp = client
        .post(format!("{}/rules", base_url))
        .json(&json!({
            "id": "rule_abs_send_error",
            "sql": "SELECT id, abs(dev) AS v FROM s",
            "actions": [{"memory": {"topic": "sink_abs_send_error"}}],
            "options": {"sendError": true}
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);

    // Post failing row
    let resp = client
        .post(format!("{}/streams/s/data", base_url))
        .json(&json!({"id": 0, "dev": "d0"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    let rec = tokio::time::timeout(std::time::Duration::from_secs(2), sink_rx.recv())
        .await
        .expect("timed out waiting for error record in sink")
        .expect("sink closed");

    let err_msg = rec
        .data
        .get("error")
        .and_then(|v| v.as_str())
        .expect("must contain error field");
    assert!(
        err_msg.contains("run Select error: alias: v expr: Call:{ name:abs, args:[s.dev] } meet error, err:call func abs error: only float64 & int type are supported"),
        "unexpected error message: {}",
        err_msg
    );
    assert_eq!(rec.data.get("rule_id"), Some(&json!("rule_abs_send_error")));
}

#[tokio::test]
async fn test_math_and_cast_runtime_errors() {
    let (base_url, _handle, state) = spawn_test_server_with_state().await;
    let client = reqwest::Client::new();

    let resp = client
        .post(format!("{}/streams", base_url))
        .json(&json!({
            "sql": "CREATE STREAM s () WITH (FORMAT=\"json\")"
        }))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    // Test ln(0)
    let mut sink_ln = state.stream_bus.subscribe("sink_ln");
    let resp = client
        .post(format!("{}/rules", base_url))
        .json(&json!({
            "id": "rule_ln",
            "sql": "SELECT ln(n) AS val FROM s",
            "actions": [{"memory": {"topic": "sink_ln"}}],
            "options": {"sendError": true}
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);

    client
        .post(format!("{}/streams/s/data", base_url))
        .json(&json!({"n": 0}))
        .send()
        .await
        .unwrap();

    let rec = tokio::time::timeout(std::time::Duration::from_secs(2), sink_ln.recv())
        .await
        .unwrap()
        .unwrap();
    let err = rec.data.get("error").and_then(|v| v.as_str()).unwrap();
    assert!(
        err.contains(
            "call func ln error: The argument must be a strictly positive number but got 0"
        ),
        "unexpected ln(0) error: {}",
        err
    );

    // Test division by zero
    let mut sink_div = state.stream_bus.subscribe("sink_div");
    let resp = client
        .post(format!("{}/rules", base_url))
        .json(&json!({
            "id": "rule_div",
            "sql": "SELECT temp / 0 AS val FROM s",
            "actions": [{"memory": {"topic": "sink_div"}}],
            "options": {"sendError": true}
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);

    client
        .post(format!("{}/streams/s/data", base_url))
        .json(&json!({"temp": 25.5}))
        .send()
        .await
        .unwrap();

    let rec = tokio::time::timeout(std::time::Duration::from_secs(2), sink_div.recv())
        .await
        .unwrap()
        .unwrap();
    let err = rec.data.get("error").and_then(|v| v.as_str()).unwrap();
    assert!(
        err.contains("divided by zero"),
        "unexpected div by zero error: {}",
        err
    );

    // Test cast("1.9", "bigint")
    let mut sink_cast = state.stream_bus.subscribe("sink_cast");
    let resp = client
        .post(format!("{}/rules", base_url))
        .json(&json!({
            "id": "rule_cast",
            "sql": "SELECT cast(sval, \"bigint\") AS val FROM s",
            "actions": [{"memory": {"topic": "sink_cast"}}],
            "options": {"sendError": true}
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);

    client
        .post(format!("{}/streams/s/data", base_url))
        .json(&json!({"sval": "1.9"}))
        .send()
        .await
        .unwrap();

    let rec = tokio::time::timeout(std::time::Duration::from_secs(2), sink_cast.recv())
        .await
        .unwrap()
        .unwrap();
    let err = rec.data.get("error").and_then(|v| v.as_str()).unwrap();
    assert!(
        err.contains("call func cast error: not supported type conversion, got error cannot convert string(1.9) to int"),
        "unexpected cast error: {}",
        err
    );
}

#[tokio::test]
async fn test_where_clause_runtime_error() {
    let (base_url, _handle, state) = spawn_test_server_with_state().await;
    let client = reqwest::Client::new();

    let resp = client
        .post(format!("{}/streams", base_url))
        .json(&json!({
            "sql": "CREATE STREAM s () WITH (FORMAT=\"json\")"
        }))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    // 1. WHERE numstr > 5 (string comparison with int)
    let mut sink_where1 = state.stream_bus.subscribe("sink_where1");
    let resp = client
        .post(format!("{}/rules", base_url))
        .json(&json!({
            "id": "rule_where1",
            "sql": "SELECT id FROM s WHERE numstr > 5",
            "actions": [{"memory": {"topic": "sink_where1"}}],
            "options": {"sendError": true}
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);

    client
        .post(format!("{}/streams/s/data", base_url))
        .json(&json!({"id": 1, "numstr": "42"}))
        .send()
        .await
        .unwrap();

    let rec = tokio::time::timeout(std::time::Duration::from_secs(2), sink_where1.recv())
        .await
        .unwrap()
        .unwrap();
    let err = rec.data.get("error").and_then(|v| v.as_str()).unwrap();
    assert!(
        err.contains("run Where error: invalid operation string(42) > int64(5)"),
        "unexpected WHERE error: {}",
        err
    );

    // 2. WHERE temp (float returned for boolean condition)
    let mut sink_where2 = state.stream_bus.subscribe("sink_where2");
    let resp = client
        .post(format!("{}/rules", base_url))
        .json(&json!({
            "id": "rule_where2",
            "sql": "SELECT id FROM s WHERE temp",
            "actions": [{"memory": {"topic": "sink_where2"}}],
            "options": {"sendError": true}
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);

    client
        .post(format!("{}/streams/s/data", base_url))
        .json(&json!({"id": 2, "temp": 25.5}))
        .send()
        .await
        .unwrap();

    let rec = tokio::time::timeout(std::time::Duration::from_secs(2), sink_where2.recv())
        .await
        .unwrap()
        .unwrap();
    let err = rec.data.get("error").and_then(|v| v.as_str()).unwrap();
    assert!(
        err.contains(
            "run Where error: invalid condition that returns non-bool value float64(25.5)"
        ),
        "unexpected non-bool WHERE error: {}",
        err
    );
}
