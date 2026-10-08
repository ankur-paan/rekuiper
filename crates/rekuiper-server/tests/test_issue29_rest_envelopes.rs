use anyhow::Result;
use rekuiper_conf::KuiperConfig;
use rekuiper_core::{RuleManager, StreamBus, StreamManager, TableManager};
use rekuiper_server::routes::{
    create_router, load_config_maps, restore_running_rules, test_sse_router, AppState,
};
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
            "test_issue29".to_string(),
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
async fn test_issue29_rules_endpoints_error_envelopes() -> Result<()> {
    let server = TestServer::start().await;
    let client = reqwest::Client::new();

    // 1. Setup stream
    let resp = client
        .post(format!("{}/streams", server.rest_url))
        .json(&json!({
            "sql": "CREATE STREAM demo () WITH (TYPE=\"memory\", FORMAT=\"json\")"
        }))
        .send()
        .await?;
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);

    // 2. POST /rules with no id -> 400 Bad Request with {"error": 1000, "message": "Missing rule id."}
    let resp = client
        .post(format!("{}/rules", server.rest_url))
        .json(&json!({
            "sql": "SELECT * FROM demo",
            "actions": [{"log": {}}]
        }))
        .send()
        .await?;
    assert_eq!(resp.status(), reqwest::StatusCode::BAD_REQUEST);
    assert_eq!(
        resp.headers().get("content-type").unwrap(),
        "application/json"
    );
    let body: Value = resp.json().await?;
    assert_eq!(body["error"], 1000);
    assert_eq!(body["message"], "Missing rule id.");

    // 3. POST /rules with empty actions -> 400 Bad Request with {"error": 1000, "message": "invalid rule json: Missing rule actions."}
    let resp = client
        .post(format!("{}/rules", server.rest_url))
        .json(&json!({
            "id": "r_no_act",
            "sql": "SELECT * FROM demo",
            "actions": []
        }))
        .send()
        .await?;
    assert_eq!(resp.status(), reqwest::StatusCode::BAD_REQUEST);
    let body: Value = resp.json().await?;
    assert_eq!(body["error"], 1000);
    assert_eq!(body["message"], "invalid rule json: Missing rule actions.");

    // 4. POST /rules valid rule -> 201 Created
    let resp = client
        .post(format!("{}/rules", server.rest_url))
        .json(&json!({
            "id": "r_valid",
            "sql": "SELECT * FROM demo",
            "actions": [{"log": {}}]
        }))
        .send()
        .await?;
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);

    // 5. POST /rules duplicate rule -> 400 Bad Request with {"error": 1000, "message": "Rule r_valid already exists"}
    let resp = client
        .post(format!("{}/rules", server.rest_url))
        .json(&json!({
            "id": "r_valid",
            "sql": "SELECT * FROM demo",
            "actions": [{"log": {}}]
        }))
        .send()
        .await?;
    assert_eq!(resp.status(), reqwest::StatusCode::BAD_REQUEST);
    let body: Value = resp.json().await?;
    assert_eq!(body["error"], 1000);
    assert_eq!(body["message"], "Rule r_valid already exists");

    // 6. GET /rules/missing_rule -> 404 Not Found with {"error": 1002, "message": "Rule missing_rule not found"}
    let resp = client
        .get(format!("{}/rules/missing_rule", server.rest_url))
        .send()
        .await?;
    assert_eq!(resp.status(), reqwest::StatusCode::NOT_FOUND);
    let body: Value = resp.json().await?;
    assert_eq!(body["error"], 1002);
    assert_eq!(body["message"], "Rule missing_rule not found");

    Ok(())
}

#[tokio::test]
async fn test_issue29_rules_validate_error_envelopes() -> Result<()> {
    let server = TestServer::start().await;
    let client = reqwest::Client::new();

    // 1. POST /rules/validate with empty actions -> 422 Unprocessable Entity
    let resp = client
        .post(format!("{}/rules/validate", server.rest_url))
        .json(&json!({
            "id": "v1",
            "sql": "SELECT * FROM demo",
            "actions": []
        }))
        .send()
        .await?;
    assert_eq!(resp.status(), reqwest::StatusCode::UNPROCESSABLE_ENTITY);
    let body: Value = resp.json().await?;
    assert_eq!(body["error"], 1000);
    assert_eq!(body["message"], "invalid rule json: Missing rule actions.");

    // 2. POST /rules/validate with unknown stream -> 422 Unprocessable Entity
    let resp = client
        .post(format!("{}/rules/validate", server.rest_url))
        .json(&json!({
            "id": "v2",
            "sql": "SELECT * FROM nostream",
            "actions": [{"log": {}}]
        }))
        .send()
        .await?;
    assert_eq!(resp.status(), reqwest::StatusCode::UNPROCESSABLE_ENTITY);
    let body: Value = resp.json().await?;
    assert_eq!(body["error"], 1000);
    assert!(body["message"]
        .as_str()
        .unwrap()
        .contains("fail to get stream nostream"));

    // 3. POST /rules/validate with bad SQL -> 400 Bad Request with error: 1000 JSON
    let resp = client
        .post(format!("{}/rules/validate", server.rest_url))
        .json(&json!({
            "id": "v3",
            "sql": "SELECT FROM WHERE",
            "actions": [{"log": {}}]
        }))
        .send()
        .await?;
    assert_eq!(resp.status(), reqwest::StatusCode::BAD_REQUEST);
    let body: Value = resp.json().await?;
    assert_eq!(body["error"], 1000);
    assert!(body["message"]
        .as_str()
        .unwrap()
        .contains("Invalid rule SQL"));

    Ok(())
}

#[tokio::test]
async fn test_issue29_streams_and_tables_error_envelopes() -> Result<()> {
    let server = TestServer::start().await;
    let client = reqwest::Client::new();

    // 1. POST /streams bad SQL -> 400 with {"error": 3000, "message": "Stream command error: ..."}
    let resp = client
        .post(format!("{}/streams", server.rest_url))
        .json(&json!({
            "sql": "CREATE STREEM bad_s ()"
        }))
        .send()
        .await?;
    assert_eq!(resp.status(), reqwest::StatusCode::BAD_REQUEST);
    let body: Value = resp.json().await?;
    assert_eq!(body["error"], 3000);
    assert!(body["message"]
        .as_str()
        .unwrap()
        .contains("Stream command error"));

    // 2. Create stream valid
    let resp = client
        .post(format!("{}/streams", server.rest_url))
        .json(&json!({
            "sql": "CREATE STREAM s1 () WITH (TYPE=\"memory\")"
        }))
        .send()
        .await?;
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);

    // 3. POST /streams duplicate stream -> 400 with {"error": 3000, "message": "Stream command error: Create stream fails: Item s1 already exists."}
    let resp = client
        .post(format!("{}/streams", server.rest_url))
        .json(&json!({
            "sql": "CREATE STREAM s1 () WITH (TYPE=\"memory\")"
        }))
        .send()
        .await?;
    assert_eq!(resp.status(), reqwest::StatusCode::BAD_REQUEST);
    let body: Value = resp.json().await?;
    assert_eq!(body["error"], 3000);
    assert_eq!(
        body["message"],
        "Stream command error: Create stream fails: Item s1 already exists."
    );

    // 4. Create table valid
    let resp = client
        .post(format!("{}/tables", server.rest_url))
        .json(&json!({
            "sql": "CREATE TABLE t1 () WITH (TYPE=\"memory\")"
        }))
        .send()
        .await?;
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);

    // 5. POST /tables duplicate table -> 400 with {"error": 3000, "message": "Table command error: Create table fails: Item t1 already exists."}
    let resp = client
        .post(format!("{}/tables", server.rest_url))
        .json(&json!({
            "sql": "CREATE TABLE t1 () WITH (TYPE=\"memory\")"
        }))
        .send()
        .await?;
    assert_eq!(resp.status(), reqwest::StatusCode::BAD_REQUEST);
    let body: Value = resp.json().await?;
    assert_eq!(body["error"], 3000);
    assert_eq!(
        body["message"],
        "Table command error: Create table fails: Item t1 already exists."
    );

    Ok(())
}

#[tokio::test]
async fn test_issue29_connections_schemas_configs() -> Result<()> {
    let server = TestServer::start().await;
    let client = reqwest::Client::new();

    // 1. POST /connections missing id -> 400 with {"message": "Missing connection id"}
    let resp = client
        .post(format!("{}/connections", server.rest_url))
        .json(&json!({}))
        .send()
        .await?;
    assert_eq!(resp.status(), reqwest::StatusCode::BAD_REQUEST);
    let body: Value = resp.json().await?;
    assert_eq!(body["message"], "Missing connection id");

    // 2. POST /connections valid -> 201
    let resp = client
        .post(format!("{}/connections", server.rest_url))
        .json(&json!({ "id": "conn1" }))
        .send()
        .await?;
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);

    // 3. POST /connections duplicate -> 400 with {"message": "connection conn1 already been created"}
    let resp = client
        .post(format!("{}/connections", server.rest_url))
        .json(&json!({ "id": "conn1" }))
        .send()
        .await?;
    assert_eq!(resp.status(), reqwest::StatusCode::BAD_REQUEST);
    let body: Value = resp.json().await?;
    assert_eq!(body["message"], "connection conn1 already been created");

    // 4. GET /connections/missing -> 404 with {"message": "Connection missing not found"}
    let resp = client
        .get(format!("{}/connections/missing", server.rest_url))
        .send()
        .await?;
    assert_eq!(resp.status(), reqwest::StatusCode::NOT_FOUND);
    let body: Value = resp.json().await?;
    assert_eq!(body["message"], "Connection missing not found");

    // 5. POST /schemas/avro (unsupported) -> 400 with {"message": "unsupported schema type avro"}
    let resp = client
        .post(format!("{}/schemas/avro", server.rest_url))
        .json(&json!({ "name": "schema1" }))
        .send()
        .await?;
    assert_eq!(resp.status(), reqwest::StatusCode::BAD_REQUEST);
    let body: Value = resp.json().await?;
    assert_eq!(body["message"], "unsupported schema type avro");

    // 6. POST /schemas/protobuf valid -> 201
    let resp = client
        .post(format!("{}/schemas/protobuf", server.rest_url))
        .json(&json!({ "name": "proto1", "content": "syntax = \"proto3\";" }))
        .send()
        .await?;
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);

    // 7. POST /schemas/protobuf duplicate -> 400 with {"message": "Schema proto1 already registered"}
    let resp = client
        .post(format!("{}/schemas/protobuf", server.rest_url))
        .json(&json!({ "name": "proto1", "content": "syntax = \"proto3\";" }))
        .send()
        .await?;
    assert_eq!(resp.status(), reqwest::StatusCode::BAD_REQUEST);
    let body: Value = resp.json().await?;
    assert_eq!(body["message"], "Schema proto1 already registered");

    // 8. DELETE /schemas/protobuf/missing -> 404 with {"message": "Schema protobuf/missing not found"}
    let resp = client
        .delete(format!("{}/schemas/protobuf/missing", server.rest_url))
        .send()
        .await?;
    assert_eq!(resp.status(), reqwest::StatusCode::NOT_FOUND);
    let body: Value = resp.json().await?;
    assert_eq!(body["message"], "Schema protobuf/missing not found");

    // 9. PATCH /configs with invalid timezone -> 400 with {"error": 3000, "message": "Invalid TZ"}
    let resp = client
        .patch(format!("{}/configs", server.rest_url))
        .json(&json!({ "timezone": "Not/AZone" }))
        .send()
        .await?;
    assert_eq!(resp.status(), reqwest::StatusCode::BAD_REQUEST);
    let body: Value = resp.json().await?;
    assert_eq!(body["error"], 3000);
    assert_eq!(body["message"], "Invalid TZ");

    // 10. PUT /rules/:id/reset_state with unknown stream in topo -> 400
    // First create a rule
    let _ = client
        .post(format!("{}/streams", server.rest_url))
        .json(&json!({
            "sql": "CREATE STREAM reset_s () WITH (TYPE=\"memory\")"
        }))
        .send()
        .await?;
    let _ = client
        .post(format!("{}/rules", server.rest_url))
        .json(&json!({
            "id": "r_reset",
            "sql": "SELECT * FROM reset_s",
            "actions": [{"log": {}}]
        }))
        .send()
        .await?;
    let resp = client
        .put(format!("{}/rules/r_reset/reset_state", server.rest_url))
        .json(&json!({
            "type": 1,
            "params": {
                "streamName": "unknown_stream"
            }
        }))
        .send()
        .await?;
    assert_eq!(resp.status(), reqwest::StatusCode::BAD_REQUEST);
    let body: Value = resp.json().await?;
    assert_eq!(body["message"], "stream unknown_stream not found in topo");

    Ok(())
}
