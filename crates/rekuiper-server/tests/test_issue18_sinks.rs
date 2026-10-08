use rekuiper_conf::KuiperConfig;
use rekuiper_core::manager::{RuleManager, StreamManager, TableManager};
use rekuiper_core::StreamBus;
use rekuiper_server::routes::{create_router, AppState};
use serde_json::{json, Value};
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::Mutex;

async fn spawn_test_server() -> (String, tokio::task::JoinHandle<()>, AppState) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("Failed to bind ephemeral port");
    let local_addr = listener.local_addr().unwrap();

    let stream_bus = StreamBus::new();
    let stream_manager = StreamManager::new();
    let rule_manager = RuleManager::new(stream_bus.clone());

    let mut config = KuiperConfig::default();
    config.basic.enable_private_net = true;

    let state = AppState::new(
        "test_issue18".to_string(),
        config,
        stream_manager,
        TableManager::new(),
        rule_manager,
        stream_bus,
    );

    let app = create_router(state.clone());
    let handle = tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });

    (format!("http://{}", local_addr), handle, state)
}

async fn spawn_mock_receiver() -> (String, Arc<Mutex<Vec<String>>>, Arc<Mutex<Vec<String>>>) {
    let payloads = Arc::new(Mutex::new(Vec::new()));
    let content_types = Arc::new(Mutex::new(Vec::new()));
    let p_clone = payloads.clone();
    let ct_clone = content_types.clone();

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    tokio::spawn(async move {
        let app = axum::Router::new().route(
            "/sink",
            axum::routing::post(move |headers: axum::http::HeaderMap, body: String| {
                let p = p_clone.clone();
                let ct = ct_clone.clone();
                async move {
                    let content_type = headers
                        .get(axum::http::header::CONTENT_TYPE)
                        .and_then(|v| v.to_str().ok())
                        .unwrap_or("")
                        .to_string();
                    ct.lock().await.push(content_type);
                    p.lock().await.push(body);
                    axum::http::StatusCode::OK
                }
            }),
        );
        let _ = axum::serve(listener, app).await;
    });

    (format!("http://{}/sink", addr), payloads, content_types)
}

async fn wait_for_payloads(
    payloads: &Arc<Mutex<Vec<String>>>,
    expected_count: usize,
    timeout_ms: u64,
) -> Vec<String> {
    let start = std::time::Instant::now();
    while start.elapsed().as_millis() < timeout_ms as u128 {
        {
            let guard = payloads.lock().await;
            if guard.len() >= expected_count {
                return guard.clone();
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(30)).await;
    }
    payloads.lock().await.clone()
}

#[tokio::test]
async fn test_rest_sink_send_single_false_default() {
    let (sink_url, payloads, _) = spawn_mock_receiver().await;
    let (base_url, _handle, _state) = spawn_test_server().await;
    let client = reqwest::Client::new();

    // 1. Create stream
    let resp = client
        .post(format!("{}/streams", base_url))
        .json(&json!({
            "sql": "CREATE STREAM s_single_false () WITH (DATASOURCE=\"s_single_false\");"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);

    // 2. Create rule with default sendSingle (omitted => false)
    let resp = client
        .post(format!("{}/rules", base_url))
        .json(&json!({
            "id": "rule_single_false",
            "sql": "SELECT dev, id, temp FROM s_single_false",
            "actions": [{
                "rest": {
                    "url": sink_url,
                    "method": "POST"
                }
            }]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);

    // 3. Send record
    let resp = client
        .post(format!("{}/streams/s_single_false/data", base_url))
        .json(&json!({"dev": "d0", "id": 0, "temp": 20.5}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    let recs = wait_for_payloads(&payloads, 1, 2000).await;
    assert_eq!(recs.len(), 1, "Expected 1 request to mock receiver");

    // sendSingle: false must emit a JSON array: [{"dev":"d0","id":0,"temp":20.5}]
    let parsed: Value = serde_json::from_str(&recs[0]).expect("JSON array expected");
    assert!(
        parsed.is_array(),
        "Default REST sink payload must be a JSON array, got: {}",
        recs[0]
    );
    let arr = parsed.as_array().unwrap();
    assert_eq!(arr.len(), 1);
    assert_eq!(arr[0]["dev"], "d0");
    assert_eq!(arr[0]["id"], 0);
    assert_eq!(arr[0]["temp"], 20.5);
}

#[tokio::test]
async fn test_rest_sink_send_single_true() {
    let (sink_url, payloads, _) = spawn_mock_receiver().await;
    let (base_url, _handle, _state) = spawn_test_server().await;
    let client = reqwest::Client::new();

    let resp = client
        .post(format!("{}/streams", base_url))
        .json(&json!({
            "sql": "CREATE STREAM s_single_true () WITH (DATASOURCE=\"s_single_true\");"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);

    let resp = client
        .post(format!("{}/rules", base_url))
        .json(&json!({
            "id": "rule_single_true",
            "sql": "SELECT dev, id, temp FROM s_single_true",
            "actions": [{
                "rest": {
                    "url": sink_url,
                    "method": "POST",
                    "sendSingle": true
                }
            }]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);

    let resp = client
        .post(format!("{}/streams/s_single_true/data", base_url))
        .json(&json!({"dev": "d0", "id": 0, "temp": 20.5}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    let recs = wait_for_payloads(&payloads, 1, 2000).await;
    assert_eq!(recs.len(), 1);

    // sendSingle: true must emit a bare JSON object: {"dev":"d0","id":0,"temp":20.5}
    let parsed: Value = serde_json::from_str(&recs[0]).expect("JSON object expected");
    assert!(
        parsed.is_object(),
        "sendSingle=true must emit a JSON object, got: {}",
        recs[0]
    );
    assert_eq!(parsed["dev"], "d0");
    assert_eq!(parsed["id"], 0);
    assert_eq!(parsed["temp"], 20.5);
}

#[tokio::test]
async fn test_rest_sink_fields_and_missing_null() {
    let (sink_url, payloads, _) = spawn_mock_receiver().await;
    let (base_url, _handle, _state) = spawn_test_server().await;
    let client = reqwest::Client::new();

    let resp = client
        .post(format!("{}/streams", base_url))
        .json(&json!({
            "sql": "CREATE STREAM s_fields () WITH (DATASOURCE=\"s_fields\");"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);

    // fields: ["id", "nosuch"] -> must emit only "id" and explicit null for "nosuch"
    let resp = client
        .post(format!("{}/rules", base_url))
        .json(&json!({
            "id": "rule_fields",
            "sql": "SELECT id, dev, temp FROM s_fields",
            "actions": [{
                "rest": {
                    "url": sink_url,
                    "method": "POST",
                    "sendSingle": true,
                    "fields": ["id", "nosuch"]
                }
            }]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);

    let resp = client
        .post(format!("{}/streams/s_fields/data", base_url))
        .json(&json!({"dev": "d0", "id": 0, "temp": 20.5}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    let recs = wait_for_payloads(&payloads, 1, 2000).await;
    assert_eq!(recs.len(), 1);

    let parsed: Value = serde_json::from_str(&recs[0]).unwrap();
    assert_eq!(parsed["id"], 0);
    assert_eq!(
        parsed["nosuch"],
        Value::Null,
        "Missing field specified in fields must output explicit null"
    );
    assert!(
        !parsed.as_object().unwrap().contains_key("dev"),
        "Unlisted field 'dev' must not be present"
    );
    assert!(
        !parsed.as_object().unwrap().contains_key("temp"),
        "Unlisted field 'temp' must not be present"
    );
}

#[tokio::test]
async fn test_rest_sink_exclude_fields() {
    let (sink_url, payloads, _) = spawn_mock_receiver().await;
    let (base_url, _handle, _state) = spawn_test_server().await;
    let client = reqwest::Client::new();

    let resp = client
        .post(format!("{}/streams", base_url))
        .json(&json!({
            "sql": "CREATE STREAM s_exclude () WITH (DATASOURCE=\"s_exclude\");"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);

    let resp = client
        .post(format!("{}/rules", base_url))
        .json(&json!({
            "id": "rule_exclude",
            "sql": "SELECT dev, id, temp FROM s_exclude",
            "actions": [{
                "rest": {
                    "url": sink_url,
                    "method": "POST",
                    "sendSingle": true,
                    "excludeFields": ["temp"]
                }
            }]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);

    let resp = client
        .post(format!("{}/streams/s_exclude/data", base_url))
        .json(&json!({"dev": "d0", "id": 0, "temp": 20.5}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    let recs = wait_for_payloads(&payloads, 1, 2000).await;
    assert_eq!(recs.len(), 1);

    let parsed: Value = serde_json::from_str(&recs[0]).unwrap();
    assert_eq!(parsed["dev"], "d0");
    assert_eq!(parsed["id"], 0);
    assert!(
        !parsed.as_object().unwrap().contains_key("temp"),
        "excludeFields: ['temp'] must omit temp"
    );
}

#[tokio::test]
async fn test_rest_sink_data_field() {
    let (sink_url, payloads, _) = spawn_mock_receiver().await;
    let (base_url, _handle, _state) = spawn_test_server().await;
    let client = reqwest::Client::new();

    let resp = client
        .post(format!("{}/streams", base_url))
        .json(&json!({
            "sql": "CREATE STREAM s_datafield () WITH (DATASOURCE=\"s_datafield\");"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);

    // Rule outputs {"payload": {"device": "edge1", "val": 42}, "outer": 99}
    // dataField: "payload" should unroll and send only the payload content
    let resp = client
        .post(format!("{}/rules", base_url))
        .json(&json!({
            "id": "rule_datafield",
            "sql": "SELECT payload, outer FROM s_datafield",
            "actions": [{
                "rest": {
                    "url": sink_url,
                    "method": "POST",
                    "sendSingle": true,
                    "dataField": "payload"
                }
            }]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);

    let resp = client
        .post(format!("{}/streams/s_datafield/data", base_url))
        .json(&json!({
            "payload": {"device": "edge1", "val": 42},
            "outer": 99
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    let recs = wait_for_payloads(&payloads, 1, 2000).await;
    assert_eq!(recs.len(), 1);

    let parsed: Value = serde_json::from_str(&recs[0]).unwrap();
    assert_eq!(parsed["device"], "edge1");
    assert_eq!(parsed["val"], 42);
    assert!(
        !parsed.as_object().unwrap().contains_key("outer"),
        "dataField extraction must discard other fields"
    );
}

#[tokio::test]
async fn test_rest_sink_delimited_format() {
    let (sink_url, payloads, content_types) = spawn_mock_receiver().await;
    let (base_url, _handle, _state) = spawn_test_server().await;
    let client = reqwest::Client::new();

    let resp = client
        .post(format!("{}/streams", base_url))
        .json(&json!({
            "sql": "CREATE STREAM s_delim () WITH (DATASOURCE=\"s_delim\");"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);

    let resp = client
        .post(format!("{}/rules", base_url))
        .json(&json!({
            "id": "rule_delim",
            "sql": "SELECT dev, id, temp FROM s_delim",
            "actions": [{
                "rest": {
                    "url": sink_url,
                    "method": "POST",
                    "format": "delimited",
                    "delimiter": "|",
                    "fields": ["dev", "id", "temp"]
                }
            }]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);

    let resp = client
        .post(format!("{}/streams/s_delim/data", base_url))
        .json(&json!({"dev": "d0", "id": 0, "temp": 20.5}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    let recs = wait_for_payloads(&payloads, 1, 2000).await;
    assert_eq!(recs.len(), 1);
    assert_eq!(recs[0], "d0|0|20.5");

    let cts = content_types.lock().await.clone();
    assert!(
        cts[0].contains("text/plain"),
        "Delimited format must send Content-Type: text/plain, got: {}",
        cts[0]
    );
}

#[tokio::test]
async fn test_rest_sink_batch_size_send_single_false() {
    let (sink_url, payloads, _) = spawn_mock_receiver().await;
    let (base_url, _handle, _state) = spawn_test_server().await;
    let client = reqwest::Client::new();

    let resp = client
        .post(format!("{}/streams", base_url))
        .json(&json!({
            "sql": "CREATE STREAM s_batch_false () WITH (DATASOURCE=\"s_batch_false\");"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);

    // batchSize: 2, sendSingle: false -> flushes 1 HTTP request with an array of 2 objects
    let resp = client
        .post(format!("{}/rules", base_url))
        .json(&json!({
            "id": "rule_batch_false",
            "sql": "SELECT id FROM s_batch_false",
            "actions": [{
                "rest": {
                    "url": sink_url,
                    "method": "POST",
                    "batchSize": 2,
                    "sendSingle": false
                }
            }]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);

    // Send record 1: should not flush yet
    client
        .post(format!("{}/streams/s_batch_false/data", base_url))
        .json(&json!({"id": 1}))
        .send()
        .await
        .unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    assert_eq!(
        payloads.lock().await.len(),
        0,
        "batchSize=2 must buffer record 1 without sending"
    );

    // Send record 2: triggers batch flush
    client
        .post(format!("{}/streams/s_batch_false/data", base_url))
        .json(&json!({"id": 2}))
        .send()
        .await
        .unwrap();

    let recs = wait_for_payloads(&payloads, 1, 2000).await;
    assert_eq!(
        recs.len(),
        1,
        "Must flush exactly 1 request containing the 2-element batch"
    );

    let parsed: Value = serde_json::from_str(&recs[0]).unwrap();
    assert!(parsed.is_array());
    let arr = parsed.as_array().unwrap();
    assert_eq!(arr.len(), 2);
    assert_eq!(arr[0]["id"], 1);
    assert_eq!(arr[1]["id"], 2);
}

#[tokio::test]
async fn test_rest_sink_batch_size_send_single_true() {
    let (sink_url, payloads, _) = spawn_mock_receiver().await;
    let (base_url, _handle, _state) = spawn_test_server().await;
    let client = reqwest::Client::new();

    let resp = client
        .post(format!("{}/streams", base_url))
        .json(&json!({
            "sql": "CREATE STREAM s_batch_true () WITH (DATASOURCE=\"s_batch_true\");"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);

    // batchSize: 2, sendSingle: true -> flushes 2 separate HTTP requests when batch threshold is reached
    let resp = client
        .post(format!("{}/rules", base_url))
        .json(&json!({
            "id": "rule_batch_true",
            "sql": "SELECT id FROM s_batch_true",
            "actions": [{
                "rest": {
                    "url": sink_url,
                    "method": "POST",
                    "batchSize": 2,
                    "sendSingle": true
                }
            }]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);

    // Record 1
    client
        .post(format!("{}/streams/s_batch_true/data", base_url))
        .json(&json!({"id": 1}))
        .send()
        .await
        .unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    assert_eq!(payloads.lock().await.len(), 0);

    // Record 2 -> flushes 2 messages
    client
        .post(format!("{}/streams/s_batch_true/data", base_url))
        .json(&json!({"id": 2}))
        .send()
        .await
        .unwrap();

    let recs = wait_for_payloads(&payloads, 2, 2000).await;
    assert_eq!(
        recs.len(),
        2,
        "batchSize=2 with sendSingle=true must flush 2 separate requests"
    );

    let p1: Value = serde_json::from_str(&recs[0]).unwrap();
    let p2: Value = serde_json::from_str(&recs[1]).unwrap();
    assert_eq!(p1["id"], 1);
    assert_eq!(p2["id"], 2);
}

#[tokio::test]
async fn test_rest_sink_linger_interval() {
    let (sink_url, payloads, _) = spawn_mock_receiver().await;
    let (base_url, _handle, _state) = spawn_test_server().await;
    let client = reqwest::Client::new();

    let resp = client
        .post(format!("{}/streams", base_url))
        .json(&json!({
            "sql": "CREATE STREAM s_linger () WITH (DATASOURCE=\"s_linger\");"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);

    // batchSize: 10, lingerInterval: 150ms -> 1 record will linger and then flush automatically
    let resp = client
        .post(format!("{}/rules", base_url))
        .json(&json!({
            "id": "rule_linger",
            "sql": "SELECT id FROM s_linger",
            "actions": [{
                "rest": {
                    "url": sink_url,
                    "method": "POST",
                    "batchSize": 10,
                    "lingerInterval": 150,
                    "sendSingle": true
                }
            }]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);

    // Send 1 record: batch size not reached
    client
        .post(format!("{}/streams/s_linger/data", base_url))
        .json(&json!({"id": 99}))
        .send()
        .await
        .unwrap();

    // Within 50ms, should not have flushed
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    assert_eq!(
        payloads.lock().await.len(),
        0,
        "Record must not flush before linger interval"
    );

    // Wait for linger interval (150ms) + margin
    let recs = wait_for_payloads(&payloads, 1, 1500).await;
    assert_eq!(
        recs.len(),
        1,
        "Record must be flushed when linger interval expires"
    );
    let parsed: Value = serde_json::from_str(&recs[0]).unwrap();
    assert_eq!(parsed["id"], 99);
}

#[tokio::test]
async fn test_window_emits_batch_rows_to_rest_sink() {
    let (sink_url, payloads, _) = spawn_mock_receiver().await;
    let (base_url, _handle, _state) = spawn_test_server().await;
    let client = reqwest::Client::new();

    let resp = client
        .post(format!("{}/streams", base_url))
        .json(&json!({
            "sql": "CREATE STREAM s_win () WITH (DATASOURCE=\"s_win\");"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);

    // Countwindow(2) emits both records when trigger fires
    let resp = client
        .post(format!("{}/rules", base_url))
        .json(&json!({
            "id": "rule_win_batch",
            "sql": "SELECT id, dev FROM s_win GROUP BY COUNTWINDOW(2)",
            "actions": [{
                "rest": {
                    "url": sink_url,
                    "method": "POST"
                }
            }]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);

    // Send row 1
    client
        .post(format!("{}/streams/s_win/data", base_url))
        .json(&json!({"dev": "d1", "id": 1}))
        .send()
        .await
        .unwrap();

    // Send row 2 -> triggers window
    client
        .post(format!("{}/streams/s_win/data", base_url))
        .json(&json!({"dev": "d2", "id": 2}))
        .send()
        .await
        .unwrap();

    let recs = wait_for_payloads(&payloads, 1, 2000).await;
    assert_eq!(recs.len(), 1, "Window trigger must emit to REST sink");

    let parsed: Value = serde_json::from_str(&recs[0]).unwrap();
    assert!(
        parsed.is_array(),
        "Window trigger should emit array batch by default to REST sink"
    );
    let arr = parsed.as_array().unwrap();
    assert_eq!(arr.len(), 2);
}

#[tokio::test]
async fn test_sink_validation() {
    let (base_url, _handle, _state) = spawn_test_server().await;
    let client = reqwest::Client::new();

    let resp = client
        .post(format!("{}/streams", base_url))
        .json(&json!({
            "sql": "CREATE STREAM s_val () WITH (DATASOURCE=\"s_val\");"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);

    // 1. Unsupported format
    let resp = client
        .post(format!("{}/rules/validate", base_url))
        .json(&json!({
            "id": "rule_bad_fmt",
            "sql": "SELECT * FROM s_val",
            "actions": [{
                "rest": {
                    "url": "http://127.0.0.1:8080",
                    "format": "invalid_xyz"
                }
            }]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::BAD_REQUEST);
    let body = resp.text().await.unwrap();
    assert!(
        body.contains("format type invalid_xyz not supported"),
        "Got: {}",
        body
    );

    // 2. MQTT missing topic
    let resp = client
        .post(format!("{}/rules/validate", base_url))
        .json(&json!({
            "id": "rule_mqtt_no_topic",
            "sql": "SELECT * FROM s_val",
            "actions": [{
                "mqtt": {
                    "server": "tcp://127.0.0.1:1883"
                }
            }]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::BAD_REQUEST);
    let body = resp.text().await.unwrap();
    assert!(
        body.contains("mqtt sink is missing property topic"),
        "Got: {}",
        body
    );

    // 3. MQTT missing server
    let resp = client
        .post(format!("{}/rules/validate", base_url))
        .json(&json!({
            "id": "rule_mqtt_no_server",
            "sql": "SELECT * FROM s_val",
            "actions": [{
                "mqtt": {
                    "topic": "t1"
                }
            }]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::BAD_REQUEST);
    let body = resp.text().await.unwrap();
    assert!(body.contains("missing server property"), "Got: {}", body);

    // 4. Valid MQTT sink
    let resp = client
        .post(format!("{}/rules/validate", base_url))
        .json(&json!({
            "id": "rule_mqtt_valid",
            "sql": "SELECT * FROM s_val",
            "actions": [{
                "mqtt": {
                    "server": "tcp://127.0.0.1:1883",
                    "topic": "t1"
                }
            }]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
}
