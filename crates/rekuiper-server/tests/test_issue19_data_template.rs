use rekuiper_conf::KuiperConfig;
use rekuiper_core::manager::{RuleManager, StreamManager, TableManager};
use rekuiper_core::StreamBus;
use rekuiper_server::routes::{create_router, AppState};
use serde_json::json;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::Mutex;

async fn spawn_test_server(name: &str) -> (String, tokio::task::JoinHandle<()>, AppState) {
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
        name.to_string(),
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

async fn spawn_mock_receiver() -> (String, Arc<Mutex<Vec<String>>>) {
    let payloads = Arc::new(Mutex::new(Vec::new()));
    let p_clone = payloads.clone();

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    tokio::spawn(async move {
        let app = axum::Router::new().route(
            "/sink",
            axum::routing::post(move |_headers: axum::http::HeaderMap, body: String| {
                let p = p_clone.clone();
                async move {
                    p.lock().await.push(body);
                    axum::http::StatusCode::OK
                }
            }),
        );
        let _ = axum::serve(listener, app).await;
    });

    (format!("http://{}/sink", addr), payloads)
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
async fn test_issue19_validation_rejects_unclosed_action() {
    let (base_url, _handle, _state) = spawn_test_server("test_issue19_validation").await;
    let client = reqwest::Client::new();

    // 1. Create stream
    let resp = client
        .post(format!("{}/streams", base_url))
        .json(&json!({
            "sql": "CREATE STREAM s_val () WITH (DATASOURCE=\"s_val\");"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);

    // 2. Create rule with unclosed action: "{{.id"
    let resp = client
        .post(format!("{}/rules", base_url))
        .json(&json!({
            "id": "rule_bad_tpl",
            "sql": "SELECT * FROM s_val",
            "actions": [{
                "rest": {
                    "url": "http://127.0.0.1:9999/dummy",
                    "method": "POST",
                    "dataTemplate": "{{.id"
                }
            }]
        }))
        .send()
        .await
        .unwrap();

    assert_eq!(
        resp.status(),
        reqwest::StatusCode::BAD_REQUEST,
        "Unclosed action must be rejected with 400 Bad Request"
    );
    let body_text = resp.text().await.unwrap();
    assert!(
        body_text.contains("template: sink:1: unclosed action"),
        "Error message should contain exact format: 'template: sink:1: unclosed action', got: {}",
        body_text
    );
}

#[tokio::test]
async fn test_issue19_plain_and_nested_fields() {
    let (sink_url, payloads) = spawn_mock_receiver().await;
    let (base_url, _handle, _state) = spawn_test_server("test_issue19_fields").await;
    let client = reqwest::Client::new();

    let _ = client
        .post(format!("{}/streams", base_url))
        .json(&json!({
            "sql": "CREATE STREAM s_fields () WITH (DATASOURCE=\"s_fields\");"
        }))
        .send()
        .await
        .unwrap();

    let _ = client
        .post(format!("{}/rules", base_url))
        .json(&json!({
            "id": "rule_fields",
            "sql": "SELECT id, obj, nosuch FROM s_fields",
            "actions": [{
                "rest": {
                    "url": sink_url,
                    "method": "POST",
                    "sendSingle": true,
                    "bodyType": "text",
                    "dataTemplate": "id={{.id}}, k={{.obj.k}}, missing=[{{.nosuch}}]"
                }
            }]
        }))
        .send()
        .await
        .unwrap();

    let _ = client
        .post(format!("{}/streams/s_fields/data", base_url))
        .json(&json!({"id": 42, "obj": {"k": "val"}}))
        .send()
        .await
        .unwrap();

    let recs = wait_for_payloads(&payloads, 1, 2000).await;
    assert_eq!(recs.len(), 1);
    assert_eq!(recs[0], "id=42, k=val, missing=[<no value>]");
}

#[tokio::test]
async fn test_issue19_json_and_to_json() {
    let (sink_url, payloads) = spawn_mock_receiver().await;
    let (base_url, _handle, _state) = spawn_test_server("test_issue19_json").await;
    let client = reqwest::Client::new();

    let _ = client
        .post(format!("{}/streams", base_url))
        .json(&json!({
            "sql": "CREATE STREAM s_json () WITH (DATASOURCE=\"s_json\");"
        }))
        .send()
        .await
        .unwrap();

    let _ = client
        .post(format!("{}/rules", base_url))
        .json(&json!({
            "id": "rule_json",
            "sql": "SELECT b, a FROM s_json",
            "actions": [{
                "rest": {
                    "url": sink_url,
                    "method": "POST",
                    "sendSingle": true,
                    "bodyType": "text",
                    "dataTemplate": "{{toJson .}}"
                }
            }]
        }))
        .send()
        .await
        .unwrap();

    let _ = client
        .post(format!("{}/streams/s_json/data", base_url))
        .json(&json!({"b": 2, "a": 1}))
        .send()
        .await
        .unwrap();

    let recs = wait_for_payloads(&payloads, 1, 2000).await;
    assert_eq!(recs.len(), 1);
    assert_eq!(recs[0], "{\"a\":1,\"b\":2}");
}

#[tokio::test]
async fn test_issue19_base64_and_b64enc_b64dec() {
    let (sink_url, payloads) = spawn_mock_receiver().await;
    let (base_url, _handle, _state) = spawn_test_server("test_issue19_b64").await;
    let client = reqwest::Client::new();

    let _ = client
        .post(format!("{}/streams", base_url))
        .json(&json!({
            "sql": "CREATE STREAM s_b64 () WITH (DATASOURCE=\"s_b64\");"
        }))
        .send()
        .await
        .unwrap();

    let _ = client
        .post(format!("{}/rules", base_url))
        .json(&json!({
            "id": "rule_b64",
            "sql": "SELECT dev, encoded FROM s_b64",
            "actions": [{
                "rest": {
                    "url": sink_url,
                    "method": "POST",
                    "sendSingle": true,
                    "bodyType": "text",
                    "dataTemplate": "ENC={{b64enc .dev}}, DEC={{b64dec .encoded}}"
                }
            }]
        }))
        .send()
        .await
        .unwrap();

    let _ = client
        .post(format!("{}/streams/s_b64/data", base_url))
        .json(&json!({"dev": "d0", "encoded": "ZDA="}))
        .send()
        .await
        .unwrap();

    let recs = wait_for_payloads(&payloads, 1, 2000).await;
    assert_eq!(recs.len(), 1);
    assert_eq!(recs[0], "ENC=ZDA=, DEC=d0");
}

#[tokio::test]
async fn test_issue19_printf_formatting() {
    let (sink_url, payloads) = spawn_mock_receiver().await;
    let (base_url, _handle, _state) = spawn_test_server("test_issue19_printf").await;
    let client = reqwest::Client::new();

    let _ = client
        .post(format!("{}/streams", base_url))
        .json(&json!({
            "sql": "CREATE STREAM s_printf () WITH (DATASOURCE=\"s_printf\");"
        }))
        .send()
        .await
        .unwrap();

    let _ = client
        .post(format!("{}/rules", base_url))
        .json(&json!({
            "id": "rule_printf",
            "sql": "SELECT temp FROM s_printf",
            "actions": [{
                "rest": {
                    "url": sink_url,
                    "method": "POST",
                    "sendSingle": true,
                    "bodyType": "text",
                    "dataTemplate": "formatted={{printf \"%.2f\" .temp}}"
                }
            }]
        }))
        .send()
        .await
        .unwrap();

    let _ = client
        .post(format!("{}/streams/s_printf/data", base_url))
        .json(&json!({"temp": 20.5}))
        .send()
        .await
        .unwrap();

    let recs = wait_for_payloads(&payloads, 1, 2000).await;
    assert_eq!(recs.len(), 1);
    assert_eq!(recs[0], "formatted=20.50");
}

#[tokio::test]
async fn test_issue19_conditionals() {
    let (sink_url, payloads) = spawn_mock_receiver().await;
    let (base_url, _handle, _state) = spawn_test_server("test_issue19_cond").await;
    let client = reqwest::Client::new();

    let _ = client
        .post(format!("{}/streams", base_url))
        .json(&json!({
            "sql": "CREATE STREAM s_cond () WITH (DATASOURCE=\"s_cond\");"
        }))
        .send()
        .await
        .unwrap();

    let _ = client
        .post(format!("{}/rules", base_url))
        .json(&json!({
            "id": "rule_cond",
            "sql": "SELECT temp FROM s_cond",
            "actions": [{
                "rest": {
                    "url": sink_url,
                    "method": "POST",
                    "sendSingle": true,
                    "bodyType": "text",
                    "dataTemplate": "status={{if gt .temp 21.6}}hot{{else}}cold{{end}}"
                }
            }]
        }))
        .send()
        .await
        .unwrap();

    let _ = client
        .post(format!("{}/streams/s_cond/data", base_url))
        .json(&json!({"temp": 25.0}))
        .send()
        .await
        .unwrap();

    let _ = client
        .post(format!("{}/streams/s_cond/data", base_url))
        .json(&json!({"temp": 18.0}))
        .send()
        .await
        .unwrap();

    let recs = wait_for_payloads(&payloads, 2, 2000).await;
    assert_eq!(recs.len(), 2);
    assert_eq!(recs[0], "status=hot");
    assert_eq!(recs[1], "status=cold");
}

#[tokio::test]
async fn test_issue19_loops_send_single_false() {
    let (sink_url, payloads) = spawn_mock_receiver().await;
    let (base_url, _handle, _state) = spawn_test_server("test_issue19_loops").await;
    let client = reqwest::Client::new();

    let _ = client
        .post(format!("{}/streams", base_url))
        .json(&json!({
            "sql": "CREATE STREAM s_loops () WITH (DATASOURCE=\"s_loops\");"
        }))
        .send()
        .await
        .unwrap();

    let _ = client
        .post(format!("{}/rules", base_url))
        .json(&json!({
            "id": "rule_loops",
            "sql": "SELECT id FROM s_loops",
            "actions": [{
                "rest": {
                    "url": sink_url,
                    "method": "POST",
                    "sendSingle": false,
                    "bodyType": "text",
                    "dataTemplate": "IDs: {{range .}}{{.id}};{{end}}"
                }
            }]
        }))
        .send()
        .await
        .unwrap();

    let _ = client
        .post(format!("{}/streams/s_loops/data", base_url))
        .json(&json!({"id": 1}))
        .send()
        .await
        .unwrap();

    let recs = wait_for_payloads(&payloads, 1, 2000).await;
    assert_eq!(recs.len(), 1);
    assert_eq!(recs[0], "IDs: 1;");
}

#[tokio::test]
async fn test_issue19_variable_assignment_and_range_vars() {
    let (sink_url, payloads) = spawn_mock_receiver().await;
    let (base_url, _handle, _state) = spawn_test_server("test_issue19_vars").await;
    let client = reqwest::Client::new();

    let _ = client
        .post(format!("{}/streams", base_url))
        .json(&json!({
            "sql": "CREATE STREAM s_vars () WITH (DATASOURCE=\"s_vars\");"
        }))
        .send()
        .await
        .unwrap();

    let _ = client
        .post(format!("{}/rules", base_url))
        .json(&json!({
            "id": "rule_vars",
            "sql": "SELECT device_id, values FROM s_vars",
            "actions": [{
                "rest": {
                    "url": sink_url,
                    "method": "POST",
                    "sendSingle": true,
                    "bodyType": "text",
                    "dataTemplate": "{{$len := len .values}}{{$loopsize := add $len -1}}{\"device_id\": \"{{.device_id}}\", \"description\": [{{range $index, $ele := .values}}{{if le .temperature 25.0}}\"fine\"{{else if gt .temperature 25.0}}\"high\"{{end}}{{if eq $loopsize $index}}]{{else}},{{end}}{{end}}}"
                }
            }]
        }))
        .send()
        .await
        .unwrap();

    let _ = client
        .post(format!("{}/streams/s_vars/data", base_url))
        .json(&json!({
            "device_id": "1",
            "values": [
                {"temperature": 10.5},
                {"temperature": 20.3},
                {"temperature": 30.3}
            ]
        }))
        .send()
        .await
        .unwrap();

    let recs = wait_for_payloads(&payloads, 1, 2000).await;
    assert_eq!(recs.len(), 1);
    assert_eq!(
        recs[0],
        "{\"device_id\": \"1\", \"description\": [\"fine\",\"fine\",\"high\"]}"
    );
}

#[tokio::test]
async fn test_issue19_sprig_functions_and_pipeline() {
    let (sink_url, payloads) = spawn_mock_receiver().await;
    let (base_url, _handle, _state) = spawn_test_server("test_issue19_sprig").await;
    let client = reqwest::Client::new();

    let _ = client
        .post(format!("{}/streams", base_url))
        .json(&json!({
            "sql": "CREATE STREAM s_sprig () WITH (DATASOURCE=\"s_sprig\");"
        }))
        .send()
        .await
        .unwrap();

    let _ = client
        .post(format!("{}/rules", base_url))
        .json(&json!({
            "id": "rule_sprig",
            "sql": "SELECT dev, name FROM s_sprig",
            "actions": [{
                "rest": {
                    "url": sink_url,
                    "method": "POST",
                    "sendSingle": true,
                    "bodyType": "text",
                    "dataTemplate": "DEV={{index . \"dev\"}}, UPPER={{.name | trim | upper}}, LOWER={{.name | trim | lower}}, TRIM={{trim .name}}, ADD={{add 1 2 3}}"
                }
            }]
        }))
        .send()
        .await
        .unwrap();

    let _ = client
        .post(format!("{}/streams/s_sprig/data", base_url))
        .json(&json!({"dev": "device-1", "name": "  Hello World  "}))
        .send()
        .await
        .unwrap();

    let recs = wait_for_payloads(&payloads, 1, 2000).await;
    assert_eq!(recs.len(), 1);
    assert_eq!(
        recs[0],
        "DEV=device-1, UPPER=HELLO WORLD, LOWER=hello world, TRIM=Hello World, ADD=6"
    );
}

#[tokio::test]
async fn test_issue19_file_sink_with_template() {
    let temp_dir = std::env::temp_dir().join(format!("test_issue19_file_{}", uuid::Uuid::new_v4()));
    let file_path = temp_dir.join("output.txt");
    let file_path_str = file_path.to_string_lossy().to_string();

    let (base_url, _handle, _state) = spawn_test_server("test_issue19_file").await;
    let client = reqwest::Client::new();

    let _ = client
        .post(format!("{}/streams", base_url))
        .json(&json!({
            "sql": "CREATE STREAM s_file () WITH (DATASOURCE=\"s_file\");"
        }))
        .send()
        .await
        .unwrap();

    let _ = client
        .post(format!("{}/rules", base_url))
        .json(&json!({
            "id": "rule_file",
            "sql": "SELECT id, val FROM s_file",
            "actions": [{
                "file": {
                    "path": file_path_str,
                    "sendSingle": true,
                    "dataTemplate": "RECORD: id={{.id}}, val={{.val}}"
                }
            }]
        }))
        .send()
        .await
        .unwrap();

    let _ = client
        .post(format!("{}/streams/s_file/data", base_url))
        .json(&json!({"id": 101, "val": "item101"}))
        .send()
        .await
        .unwrap();

    // Give file sink time to flush
    let start = std::time::Instant::now();
    let mut file_content = String::new();
    while start.elapsed().as_millis() < 3000 {
        if let Ok(c) = tokio::fs::read_to_string(&file_path).await {
            if !c.is_empty() {
                file_content = c;
                break;
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }

    assert!(
        file_content.contains("RECORD: id=101, val=item101"),
        "File should contain rendered template line, got: {}",
        file_content
    );

    let _ = tokio::fs::remove_dir_all(&temp_dir).await;
}
