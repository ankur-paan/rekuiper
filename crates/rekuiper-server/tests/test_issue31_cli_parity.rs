use anyhow::Result;
use rekuiper_conf::KuiperConfig;
use rekuiper_core::{RuleManager, StreamBus, StreamManager, TableManager};
use rekuiper_server::routes::{create_router, load_config_maps, restore_running_rules, AppState};
use serde_json::{json, Value};
use tokio::net::TcpListener;

struct TestServer {
    rest_url: String,
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
            "test_issue31".to_string(),
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

        Self {
            rest_url: format!("http://127.0.0.1:{}", rest_addr.port()),
            _handles: vec![rest_handle],
        }
    }
}

#[tokio::test]
async fn test_issue31_stream_and_table_garbage_rejection() -> Result<()> {
    let server = TestServer::start().await;
    let client = reqwest::Client::new();

    // 1. Unquoted trailing tokens after stream name
    let resp = client
        .post(format!("{}/streams", server.rest_url))
        .json(&json!({
            "sql": "CREATE STREAM cli_bad this is not a definition"
        }))
        .send()
        .await?;
    assert_eq!(resp.status(), reqwest::StatusCode::BAD_REQUEST);
    let body: Value = resp.json().await?;
    assert_eq!(
        body["message"],
        "Stream command error: found \"this\", expected lparen after stream name."
    );

    // 2. Quoted trailing tokens after stream name
    let resp = client
        .post(format!("{}/streams", server.rest_url))
        .json(&json!({
            "sql": "CREATE STREAM cli_bad \"this is not a definition\""
        }))
        .send()
        .await?;
    assert_eq!(resp.status(), reqwest::StatusCode::BAD_REQUEST);
    let body: Value = resp.json().await?;
    assert_eq!(
        body["message"],
        "Stream command error: found \"this\", expected lparen after stream name."
    );

    // 3. Unquoted trailing tokens after table name
    let resp = client
        .post(format!("{}/tables", server.rest_url))
        .json(&json!({
            "sql": "CREATE TABLE cli_bad this is not a definition"
        }))
        .send()
        .await?;
    assert_eq!(resp.status(), reqwest::StatusCode::BAD_REQUEST);
    let body: Value = resp.json().await?;
    assert_eq!(
        body["message"],
        "Table command error: found \"this\", expected lparen after table name."
    );

    // 4. Trailing tokens after valid column definitions
    let resp = client
        .post(format!("{}/streams", server.rest_url))
        .json(&json!({
            "sql": "CREATE STREAM cli_bad (id bigint) WITH (FORMAT=\"json\") extra_token"
        }))
        .send()
        .await?;
    assert_eq!(resp.status(), reqwest::StatusCode::BAD_REQUEST);
    let body: Value = resp.json().await?;
    assert!(body["message"]
        .as_str()
        .unwrap()
        .contains("expected semicolon or EOF after stream options"));

    // 5. Valid stream creation succeeds
    let resp = client
        .post(format!("{}/streams", server.rest_url))
        .json(&json!({
            "sql": "CREATE STREAM cli_valid (id bigint, name string) WITH (FORMAT=\"json\", DATASOURCE=\"demo\")"
        }))
        .send()
        .await?;
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);

    // 6. Valid rule creation and explain
    let resp = client
        .post(format!("{}/rules", server.rest_url))
        .json(&json!({
            "id": "cli_r",
            "sql": "SELECT id, name FROM cli_valid WHERE id > 10",
            "actions": [{ "log": {} }]
        }))
        .send()
        .await?;
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);

    // 7. GET /rules/cli_r/explain returns plan
    let resp = client
        .get(format!("{}/rules/cli_r/explain", server.rest_url))
        .send()
        .await?;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let plan: Value = resp.json().await?;
    assert_eq!(plan["rule"], "cli_r");
    assert_eq!(plan["source"], "cli_valid");

    Ok(())
}
