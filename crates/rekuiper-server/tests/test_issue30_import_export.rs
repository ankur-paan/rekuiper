use rekuiper_conf::KuiperConfig;
use rekuiper_core::{RuleManager, StreamBus, StreamManager, TableManager};
use rekuiper_server::routes::{
    create_router, load_config_maps, restore_running_rules, test_sse_router, AppState,
};
use reqwest::StatusCode;
use serde_json::{json, Value};
use tokio::net::TcpListener;

struct TestServer {
    rest_url: String,
    _data_url: String,
    _state: AppState,
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
            "test_issue30".to_string(),
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
            _data_url: format!("http://{}", data_addr),
            _state: state,
            _handles: vec![rest_handle, data_handle],
        }
    }
}

#[tokio::test]
async fn test_issue30_import_resets_by_default() {
    let server = TestServer::start().await;
    let base_url = server.rest_url;
    let client = reqwest::Client::new();

    // 1. Seed two streams and a rule
    for (_name, sql) in [
        (
            "ps_s",
            "CREATE STREAM ps_s (id BIGINT) WITH (FORMAT=\"json\")",
        ),
        (
            "ps_s2",
            "CREATE STREAM ps_s2 (id BIGINT) WITH (FORMAT=\"json\")",
        ),
    ] {
        let resp = client
            .post(format!("{}/streams", base_url))
            .json(&json!({"sql": sql}))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::CREATED);
    }

    let resp = client
        .post(format!("{}/rules", base_url))
        .json(&json!({
            "id": "ps_r1",
            "sql": "SELECT * FROM ps_s",
            "actions": [{"log": {}}]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);

    // Verify initial streams
    let resp = client
        .get(format!("{}/streams", base_url))
        .send()
        .await
        .unwrap();
    let streams: Vec<String> = resp.json().await.unwrap();
    assert_eq!(streams.len(), 2);
    assert!(streams.contains(&"ps_s".to_string()));
    assert!(streams.contains(&"ps_s2".to_string()));

    // 2. Perform import without partial=1 (default = reset existing definitions)
    let import_content = json!({
        "streams": {
            "ps_imp": "CREATE STREAM ps_imp (id BIGINT) WITH (FORMAT=\"json\")"
        },
        "rules": {
            "ps_r_imp": "{\"id\":\"ps_r_imp\",\"sql\":\"SELECT * FROM ps_imp\",\"actions\":[{\"log\":{}}]}"
        }
    }).to_string();

    let resp = client
        .post(format!("{}/data/import", base_url))
        .json(&json!({"content": import_content}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    // 3. Verify only ps_imp exists now
    let resp = client
        .get(format!("{}/streams", base_url))
        .send()
        .await
        .unwrap();
    let streams: Vec<String> = resp.json().await.unwrap();
    assert_eq!(streams, vec!["ps_imp".to_string()]);

    // Old rule ps_r1 is gone
    let resp = client
        .get(format!("{}/rules/ps_r1", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);

    // New rule ps_r_imp exists
    let resp = client
        .get(format!("{}/rules/ps_r_imp", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_issue30_import_partial_merge() {
    let server = TestServer::start().await;
    let base_url = server.rest_url;
    let client = reqwest::Client::new();

    // 1. Seed two streams and a rule
    for (_name, sql) in [
        (
            "ps_s",
            "CREATE STREAM ps_s (id BIGINT) WITH (FORMAT=\"json\")",
        ),
        (
            "ps_s2",
            "CREATE STREAM ps_s2 (id BIGINT) WITH (FORMAT=\"json\")",
        ),
    ] {
        let resp = client
            .post(format!("{}/streams", base_url))
            .json(&json!({"sql": sql}))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::CREATED);
    }

    let resp = client
        .post(format!("{}/rules", base_url))
        .json(&json!({
            "id": "ps_r1",
            "sql": "SELECT * FROM ps_s",
            "actions": [{"log": {}}]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);

    // 2. Perform partial import (partial=1 -> merge mode)
    let import_content = json!({
        "streams": {
            "ps_imp": "CREATE STREAM ps_imp (id BIGINT) WITH (FORMAT=\"json\")"
        },
        "rules": {
            "ps_r_imp": "{\"id\":\"ps_r_imp\",\"sql\":\"SELECT * FROM ps_imp\",\"actions\":[{\"log\":{}}]}"
        }
    }).to_string();

    let resp = client
        .post(format!("{}/data/import?partial=1", base_url))
        .json(&json!({"content": import_content}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    // 3. Verify all 3 streams exist
    let resp = client
        .get(format!("{}/streams", base_url))
        .send()
        .await
        .unwrap();
    let mut streams: Vec<String> = resp.json().await.unwrap();
    streams.sort();
    assert_eq!(
        streams,
        vec![
            "ps_imp".to_string(),
            "ps_s".to_string(),
            "ps_s2".to_string()
        ]
    );

    // Both rules exist
    let resp = client
        .get(format!("{}/rules/ps_r1", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let resp = client
        .get(format!("{}/rules/ps_r_imp", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_issue30_export_structure() {
    let server = TestServer::start().await;
    let base_url = server.rest_url;
    let client = reqwest::Client::new();

    // Create stream and rule
    let resp = client
        .post(format!("{}/streams", base_url))
        .json(&json!({"sql": "CREATE STREAM exp_s (id BIGINT) WITH (FORMAT=\"json\")"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);

    let resp = client
        .post(format!("{}/rules", base_url))
        .json(&json!({
            "id": "exp_r1",
            "sql": "SELECT * FROM exp_s",
            "actions": [{"log": {}}]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);

    // 1. POST /ruleset/export returns map-form streams, tables, and stringified rule JSON
    let resp = client
        .post(format!("{}/ruleset/export", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let val: Value = resp.json().await.unwrap();
    assert!(val["streams"].is_object());
    assert!(val["streams"]["exp_s"].is_string());
    assert!(val["tables"].is_object());
    assert!(val["rules"].is_object());
    let rule_json_str = val["rules"]["exp_r1"]
        .as_str()
        .expect("rule should be stringified JSON");
    let parsed_rule: Value = serde_json::from_str(rule_json_str).unwrap();
    assert_eq!(parsed_rule["id"], "exp_r1");

    // 2. GET /data/export returns map-form Configuration with all 12 categories
    let resp = client
        .get(format!("{}/data/export", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let val: Value = resp.json().await.unwrap();
    assert!(val["streams"]["exp_s"].is_string());
    let rule_json_str = val["rules"]["exp_r1"]
        .as_str()
        .expect("rule should be stringified JSON");
    let parsed_rule: Value = serde_json::from_str(rule_json_str).unwrap();
    assert_eq!(parsed_rule["id"], "exp_r1");
    assert!(val["nativePlugins"].is_object());
    assert!(val["sourceConfig"].is_object());

    // 3. GET /v2/data/export returns YAML
    let resp = client
        .get(format!("{}/v2/data/export", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let text = resp.text().await.unwrap();
    let yaml_val: Value = serde_yaml::from_str(&text).expect("export should be valid YAML");
    assert!(yaml_val["streams"]["exp_s"]["sql"].is_string());
    assert_eq!(yaml_val["rules"]["exp_r1"]["id"], "exp_r1");
}

#[tokio::test]
async fn test_issue30_invalid_inputs_and_status() {
    let server = TestServer::start().await;
    let base_url = server.rest_url;
    let client = reqwest::Client::new();

    // 1. POST /data/import with invalid JSON returns 400
    let resp = client
        .post(format!("{}/data/import", base_url))
        .header("Content-Type", "application/json")
        .body("not valid json at all")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let err_json: Value = resp.json().await.unwrap();
    assert!(err_json["message"]
        .as_str()
        .unwrap()
        .contains("configuration unmarshal with error"));

    // 2. POST /data/import with invalid content string returns 400
    let resp = client
        .post(format!("{}/data/import", base_url))
        .json(&json!({"content": "{corrupted json}"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);

    // 3. POST /v2/data/import with missing file returns 400 "Fail to read file"
    let resp = client
        .post(format!("{}/v2/data/import", base_url))
        .json(&json!({"file": "file:///tmp/definitely_not_found_12345.yaml"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let err_json: Value = resp.json().await.unwrap();
    assert_eq!(err_json["message"], "Fail to read file");

    // 4. Import with error populates /data/import/status
    let resp = client
        .post(format!("{}/data/import?partial=1", base_url))
        .json(&json!({
            "rules": {
                "bad_sql_rule": {"sql": "INVALID SQL SYNTAX HERE", "actions": [{"log": {}}]}
            }
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let resp_body: Value = resp.json().await.unwrap();
    assert!(resp_body["ConfigResponse"]["rules"]["bad_sql_rule"].is_string());

    let resp = client
        .get(format!("{}/data/import/status", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let status_val: Value = resp.json().await.unwrap();
    assert!(status_val["rules"]["bad_sql_rule"].is_string());
}
