use anyhow::Result;
use rekuiper_conf::KuiperConfig;
use rekuiper_core::{RuleManager, StreamBus, StreamManager, TableManager};
use rekuiper_server::routes::{
    create_router, load_config_maps, restore_running_rules, test_sse_router, AppState,
};
use serde_json::{json, Value};
use std::time::Duration;
use tokio::net::TcpListener;

struct TestServer {
    rest_url: String,
    #[allow(dead_code)]
    data_url: String,
    state: AppState,
    _handles: Vec<tokio::task::JoinHandle<()>>,
}

impl TestServer {
    async fn start() -> Self {
        let rest_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let rest_addr = rest_listener.local_addr().unwrap();

        let data_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let data_addr = data_listener.local_addr().unwrap();

        let mut config = KuiperConfig::default();
        config.basic.http_server_ip = "127.0.0.1".to_string();
        config.basic.http_server_port = data_addr.port();

        let stream_bus = StreamBus::new();
        let stream_manager = StreamManager::new();
        let table_manager = TableManager::new();
        let rule_manager = RuleManager::new(stream_bus.clone());

        let state = AppState::new(
            "test".to_string(),
            config,
            stream_manager,
            table_manager,
            rule_manager,
            stream_bus,
        );

        load_config_maps(&state).await.unwrap();
        restore_running_rules(&state).await;

        let rest_app = create_router(state.clone());
        let rest_handle = tokio::spawn(async move {
            let _ = axum::serve(rest_listener, rest_app).await;
        });

        let data_app = test_sse_router(state.clone());
        let data_handle = tokio::spawn(async move {
            let _ = axum::serve(data_listener, data_app).await;
        });

        Self {
            rest_url: format!("http://{}", rest_addr),
            data_url: format!("http://{}", data_addr),
            state,
            _handles: vec![rest_handle, data_handle],
        }
    }
}

/// Helper: wait for a record from stream_bus receiver
async fn recv_timeout(
    rx: &mut tokio::sync::mpsc::Receiver<rekuiper_core::StreamRecord>,
    timeout_ms: u64,
) -> Option<rekuiper_core::StreamRecord> {
    tokio::time::timeout(Duration::from_millis(timeout_ms), rx.recv())
        .await
        .ok()
        .flatten()
}

#[tokio::test]
async fn test_issue23_ddl_type_validation_rejected() -> Result<()> {
    let server = TestServer::start().await;
    let client = reqwest::Client::new();

    // 1. Invalid column type in CREATE STREAM rejected with 400 Bad Request
    let resp = client
        .post(format!("{}/streams", server.rest_url))
        .json(&json!({
            "sql": "CREATE STREAM bad_stream (id BIGINT, col invalid_type) WITH (FORMAT=\"json\")"
        }))
        .send()
        .await?;
    assert_eq!(resp.status(), reqwest::StatusCode::BAD_REQUEST);
    let body = resp.text().await?;
    assert!(
        body.contains("Unsupported or invalid data type 'invalid_type' for column 'col'"),
        "Unexpected error: {body}"
    );

    // 2. Invalid column type in CREATE TABLE rejected with 400 Bad Request
    let resp = client
        .post(format!("{}/tables", server.rest_url))
        .json(&json!({
            "sql": "CREATE TABLE bad_table (id BIGINT, col foobar) WITH (TYPE=\"sql\")"
        }))
        .send()
        .await?;
    assert_eq!(resp.status(), reqwest::StatusCode::BAD_REQUEST);
    let body = resp.text().await?;
    assert!(
        body.contains("Unsupported or invalid data type 'foobar' for column 'col'"),
        "Unexpected error: {body}"
    );

    Ok(())
}

#[tokio::test]
async fn test_issue23_ddl_valid_types_accepted() -> Result<()> {
    let server = TestServer::start().await;
    let client = reqwest::Client::new();

    // Valid catalog types accepted: bigint, float, string, boolean, datetime, bytea, array(string), struct(...)
    let resp = client
        .post(format!("{}/streams", server.rest_url))
        .json(&json!({
            "sql": "CREATE STREAM good_stream (
                id BIGINT,
                temp FLOAT,
                name STRING,
                active BOOLEAN,
                ts DATETIME,
                data BYTEA,
                arr ARRAY(STRING),
                st STRUCT(a INT, b STRING)
            ) WITH (FORMAT=\"json\")"
        }))
        .send()
        .await?;
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);

    // Describe stream verifies fields
    let resp = client
        .get(format!("{}/streams/good_stream", server.rest_url))
        .send()
        .await?;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let body: Value = resp.json().await?;
    let fields = body.get("StreamFields").and_then(|f| f.as_array()).unwrap();
    assert_eq!(fields.len(), 8);

    Ok(())
}

#[tokio::test]
async fn test_issue23_ingress_schema_coercion() -> Result<()> {
    let server = TestServer::start().await;
    let client = reqwest::Client::new();

    // 1. Create stream with declared types
    let resp = client
        .post(format!("{}/streams", server.rest_url))
        .json(&json!({
            "sql": "CREATE STREAM s_typed (
                id BIGINT,
                temp FLOAT,
                name STRING,
                active BOOLEAN,
                ts DATETIME
            ) WITH (FORMAT=\"json\")"
        }))
        .send()
        .await?;
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);

    // 2. Subscribe to sink output
    let mut sink_rx = server.state.stream_bus.subscribe("out_typed");

    // 3. Create rule
    let resp = client
        .post(format!("{}/rules", server.rest_url))
        .json(&json!({
            "id": "r_typed",
            "sql": "SELECT id, temp, name, active, ts FROM s_typed",
            "actions": [{"memory": {"topic": "out_typed"}}]
        }))
        .send()
        .await?;
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);
    tokio::time::sleep(Duration::from_millis(100)).await;

    // 4. Push data containing string numbers, ISO datetime, string boolean, number for string
    let resp = client
        .post(format!("{}/streams/s_typed/data", server.rest_url))
        .json(&json!({
            "id": "42",
            "temp": "23.5",
            "name": 999,
            "active": "true",
            "ts": "2023-01-01T12:00:00Z"
        }))
        .send()
        .await?;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    // 5. Verify received record fields are coerced
    let rec = recv_timeout(&mut sink_rx, 2000)
        .await
        .expect("Expected output record");
    assert_eq!(rec.data.get("id"), Some(&Value::from(42i64)));
    assert_eq!(rec.data.get("temp"), Some(&Value::from(23.5f64)));
    assert_eq!(
        rec.data.get("name"),
        Some(&Value::String("999".to_string()))
    );
    assert_eq!(rec.data.get("active"), Some(&Value::Bool(true)));
    assert_eq!(rec.data.get("ts"), Some(&Value::from(1672574400000i64)));

    Ok(())
}

#[tokio::test]
async fn test_issue23_ingress_mismatched_types_nullified() -> Result<()> {
    let server = TestServer::start().await;
    let client = reqwest::Client::new();

    // 1. Create stream
    let resp = client
        .post(format!("{}/streams", server.rest_url))
        .json(&json!({
            "sql": "CREATE STREAM s_nullify (
                id BIGINT,
                temp FLOAT,
                active BOOLEAN
            ) WITH (FORMAT=\"json\")"
        }))
        .send()
        .await?;
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);

    // 2. Subscribe to sink output
    let mut sink_rx = server.state.stream_bus.subscribe("out_nullify");

    // 3. Create rule
    let resp = client
        .post(format!("{}/rules", server.rest_url))
        .json(&json!({
            "id": "r_nullify",
            "sql": "SELECT id, temp, active FROM s_nullify",
            "actions": [{"memory": {"topic": "out_nullify", "sendNilField": true}}]
        }))
        .send()
        .await?;
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);
    tokio::time::sleep(Duration::from_millis(100)).await;

    // 4. Push incompatible types: unparseable strings for numeric/bool fields
    let resp = client
        .post(format!("{}/streams/s_nullify/data", server.rest_url))
        .json(&json!({
            "id": "not_an_int",
            "temp": "not_a_float",
            "active": "not_a_bool"
        }))
        .send()
        .await?;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    // 5. Verify fields are nullified
    let rec = recv_timeout(&mut sink_rx, 2000)
        .await
        .expect("Expected output record");
    assert_eq!(rec.data.get("id"), Some(&Value::Null));
    assert_eq!(rec.data.get("temp"), Some(&Value::Null));
    assert_eq!(rec.data.get("active"), Some(&Value::Null));

    Ok(())
}

#[tokio::test]
async fn test_issue23_binary_ingress_raw_and_self() -> Result<()> {
    let server = TestServer::start().await;
    let client = reqwest::Client::new();

    // 1. Create binary stream without columns
    let resp = client
        .post(format!("{}/streams", server.rest_url))
        .json(&json!({
            "sql": "CREATE STREAM s_bin () WITH (FORMAT=\"binary\")"
        }))
        .send()
        .await?;
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);

    let mut sink_rx = server.state.stream_bus.subscribe("out_bin");

    let resp = client
        .post(format!("{}/rules", server.rest_url))
        .json(&json!({
            "id": "r_bin",
            "sql": "SELECT self FROM s_bin",
            "actions": [{"memory": {"topic": "out_bin"}}]
        }))
        .send()
        .await?;
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Push raw binary string
    let resp = client
        .post(format!("{}/streams/s_bin/data", server.rest_url))
        .header("Content-Type", "application/octet-stream")
        .body("hello binary world")
        .send()
        .await?;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    let rec = recv_timeout(&mut sink_rx, 2000)
        .await
        .expect("Expected binary output");
    assert_eq!(
        rec.data.get("self"),
        Some(&Value::String("hello binary world".to_string()))
    );

    // 2. Create binary stream with a single bytea column
    let resp = client
        .post(format!("{}/streams", server.rest_url))
        .json(&json!({
            "sql": "CREATE STREAM s_bin_col (image BYTEA) WITH (FORMAT=\"binary\")"
        }))
        .send()
        .await?;
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);

    let mut sink_col_rx = server.state.stream_bus.subscribe("out_bin_col");

    let resp = client
        .post(format!("{}/rules", server.rest_url))
        .json(&json!({
            "id": "r_bin_col",
            "sql": "SELECT image, self FROM s_bin_col",
            "actions": [{"memory": {"topic": "out_bin_col"}}]
        }))
        .send()
        .await?;
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);
    tokio::time::sleep(Duration::from_millis(100)).await;

    let resp = client
        .post(format!("{}/streams/s_bin_col/data", server.rest_url))
        .header("Content-Type", "application/octet-stream")
        .body("image_bytes_123")
        .send()
        .await?;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    let rec = recv_timeout(&mut sink_col_rx, 2000)
        .await
        .expect("Expected image output");
    assert_eq!(
        rec.data.get("image"),
        Some(&Value::String("image_bytes_123".to_string()))
    );
    assert_eq!(
        rec.data.get("self"),
        Some(&Value::String("image_bytes_123".to_string()))
    );

    Ok(())
}

#[tokio::test]
async fn test_issue23_delimited_ingress_csv() -> Result<()> {
    let server = TestServer::start().await;
    let client = reqwest::Client::new();

    // 1. Create delimited stream
    let resp = client
        .post(format!("{}/streams", server.rest_url))
        .json(&json!({
            "sql": "CREATE STREAM s_csv (id BIGINT, temp FLOAT, dev STRING) WITH (FORMAT=\"delimited\", DELIMITER=\",\")"
        }))
        .send()
        .await?;
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);

    let mut sink_rx = server.state.stream_bus.subscribe("out_csv");

    let resp = client
        .post(format!("{}/rules", server.rest_url))
        .json(&json!({
            "id": "r_csv",
            "sql": "SELECT id, temp, dev FROM s_csv",
            "actions": [{"memory": {"topic": "out_csv"}}]
        }))
        .send()
        .await?;
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);
    tokio::time::sleep(Duration::from_millis(100)).await;

    // 2. Push raw CSV lines
    let csv_payload = "1,23.5,sensor1\n2,25.0,sensor2\n";
    let resp = client
        .post(format!("{}/streams/s_csv/data", server.rest_url))
        .header("Content-Type", "text/plain")
        .body(csv_payload)
        .send()
        .await?;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    // 3. Receive both records with coerced fields
    let rec1 = recv_timeout(&mut sink_rx, 2000)
        .await
        .expect("Expected csv rec 1");
    assert_eq!(rec1.data.get("id"), Some(&Value::from(1i64)));
    assert_eq!(rec1.data.get("temp"), Some(&Value::from(23.5f64)));
    assert_eq!(
        rec1.data.get("dev"),
        Some(&Value::String("sensor1".to_string()))
    );

    let rec2 = recv_timeout(&mut sink_rx, 2000)
        .await
        .expect("Expected csv rec 2");
    assert_eq!(rec2.data.get("id"), Some(&Value::from(2i64)));
    assert_eq!(rec2.data.get("temp"), Some(&Value::from(25.0f64)));
    assert_eq!(
        rec2.data.get("dev"),
        Some(&Value::String("sensor2".to_string()))
    );

    Ok(())
}

#[tokio::test]
async fn test_issue23_schemaless_preserves_flexibility() -> Result<()> {
    let server = TestServer::start().await;
    let client = reqwest::Client::new();

    // Schemaless stream: empty parens
    let resp = client
        .post(format!("{}/streams", server.rest_url))
        .json(&json!({
            "sql": "CREATE STREAM s_schemaless () WITH (FORMAT=\"json\")"
        }))
        .send()
        .await?;
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);

    let mut sink_rx = server.state.stream_bus.subscribe("out_schemaless");

    let resp = client
        .post(format!("{}/rules", server.rest_url))
        .json(&json!({
            "id": "r_schemaless",
            "sql": "SELECT * FROM s_schemaless",
            "actions": [{"memory": {"topic": "out_schemaless"}}]
        }))
        .send()
        .await?;
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Push arbitrary nested structure
    let arbitrary = json!({
        "device": "hub-01",
        "nested": {"sensors": [1, 2, 3]},
        "raw_string_number": "123.45"
    });
    let resp = client
        .post(format!("{}/streams/s_schemaless/data", server.rest_url))
        .json(&arbitrary)
        .send()
        .await?;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    let rec = recv_timeout(&mut sink_rx, 2000)
        .await
        .expect("Expected schemaless output");
    assert_eq!(
        rec.data.get("device"),
        Some(&Value::String("hub-01".to_string()))
    );
    assert_eq!(
        rec.data.get("raw_string_number"),
        Some(&Value::String("123.45".to_string()))
    );
    assert!(rec.data.contains_key("nested"));

    Ok(())
}
