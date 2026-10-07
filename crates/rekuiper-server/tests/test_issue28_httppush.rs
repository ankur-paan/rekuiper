use anyhow::{anyhow, Result};
use rekuiper_conf::KuiperConfig;
use rekuiper_core::{RuleManager, StreamBus, StreamManager, TableManager};
use rekuiper_server::routes::{
    create_router, load_config_maps, restore_running_rules, test_sse_router, AppState,
};
use serde_json::json;
use std::time::Duration;
use tokio::net::TcpListener;

struct TestServer {
    rest_url: String,
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

#[tokio::test]
async fn test_issue28_httppush_single_and_batch_records() -> Result<()> {
    let server = TestServer::start().await;
    let client = reqwest::Client::new();

    // 1. Create stream with TYPE="httppush" and DATASOURCE="/xp/push"
    let resp = client
        .post(format!("{}/streams", server.rest_url))
        .json(&json!({
            "sql": "CREATE STREAM h () WITH (DATASOURCE=\"/xp/push\", FORMAT=\"json\", TYPE=\"httppush\")"
        }))
        .send()
        .await?;
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);

    // Subscribe to rule output
    let mut sink_rx = server.state.stream_bus.subscribe("h_out");

    // 2. Before any rule starts, push request should return 404 Not Found
    let resp = client
        .post(format!("{}/xp/push", server.data_url))
        .json(&json!({"id": 0, "v": "pre_rule"}))
        .send()
        .await?;
    assert_eq!(resp.status(), reqwest::StatusCode::NOT_FOUND);

    // 3. Create and start rule: SELECT id, v FROM h
    let resp = client
        .post(format!("{}/rules", server.rest_url))
        .json(&json!({
            "id": "hr",
            "sql": "SELECT id, v FROM h",
            "actions": [{"memory": {"topic": "h_out"}}]
        }))
        .send()
        .await?;
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);

    // 4. Send single record push: POST http://<engine-host>:10081/xp/push
    let resp = client
        .post(format!("{}/xp/push", server.data_url))
        .json(&json!({"id": 1, "v": "pushed"}))
        .send()
        .await?;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let body = resp.text().await?;
    assert_eq!(body, "ok");

    let rec = tokio::time::timeout(Duration::from_secs(2), sink_rx.recv())
        .await?
        .ok_or_else(|| anyhow!("sink closed"))?;
    assert_eq!(rec.data.get("id"), Some(&json!(1)));
    assert_eq!(rec.data.get("v"), Some(&json!("pushed")));

    // 5. Send batch array push: POST http://<engine-host>:10081/xp/push with [{"id":2},{"id":3}]
    let resp = client
        .post(format!("{}/xp/push", server.data_url))
        .json(&json!([{"id": 2, "v": "batch2"}, {"id": 3, "v": "batch3"}]))
        .send()
        .await?;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    assert_eq!(resp.text().await?, "ok");

    let rec2 = tokio::time::timeout(Duration::from_secs(2), sink_rx.recv())
        .await?
        .ok_or_else(|| anyhow!("sink closed"))?;
    assert_eq!(rec2.data.get("id"), Some(&json!(2)));
    assert_eq!(rec2.data.get("v"), Some(&json!("batch2")));

    let rec3 = tokio::time::timeout(Duration::from_secs(2), sink_rx.recv())
        .await?
        .ok_or_else(|| anyhow!("sink closed"))?;
    assert_eq!(rec3.data.get("id"), Some(&json!(3)));
    assert_eq!(rec3.data.get("v"), Some(&json!("batch3")));

    Ok(())
}

#[tokio::test]
async fn test_issue28_httppush_lifecycle_and_shared_rules() -> Result<()> {
    let server = TestServer::start().await;
    let client = reqwest::Client::new();

    // 1. Create stream
    let resp = client
        .post(format!("{}/streams", server.rest_url))
        .json(&json!({
            "sql": "CREATE STREAM s_shared () WITH (DATASOURCE=\"/api/telemetry\", FORMAT=\"json\", TYPE=\"httppush\")"
        }))
        .send()
        .await?;
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);

    let mut sink_r1 = server.state.stream_bus.subscribe("out_r1");
    let mut sink_r2 = server.state.stream_bus.subscribe("out_r2");

    // 2. Start rule 1
    let resp = client
        .post(format!("{}/rules", server.rest_url))
        .json(&json!({
            "id": "r1",
            "sql": "SELECT id FROM s_shared",
            "actions": [{"memory": {"topic": "out_r1"}}]
        }))
        .send()
        .await?;
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);

    // 3. Start rule 2 (consuming same stream)
    let resp = client
        .post(format!("{}/rules", server.rest_url))
        .json(&json!({
            "id": "r2",
            "sql": "SELECT id FROM s_shared",
            "actions": [{"memory": {"topic": "out_r2"}}]
        }))
        .send()
        .await?;
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);

    // 4. Push message to /api/telemetry
    let resp = client
        .post(format!("{}/api/telemetry", server.data_url))
        .json(&json!({"id": 100}))
        .send()
        .await?;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    // Both rules receive the event
    let rec1 = tokio::time::timeout(Duration::from_secs(2), sink_r1.recv())
        .await?
        .ok_or_else(|| anyhow!("sink r1 closed"))?;
    let rec2 = tokio::time::timeout(Duration::from_secs(2), sink_r2.recv())
        .await?
        .ok_or_else(|| anyhow!("sink r2 closed"))?;
    assert_eq!(rec1.data.get("id"), Some(&json!(100)));
    assert_eq!(rec2.data.get("id"), Some(&json!(100)));

    // 5. Stop rule 1 -> rule 2 is still running, endpoint must remain active
    let resp = client
        .post(format!("{}/rules/r1/stop", server.rest_url))
        .send()
        .await?;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    let resp = client
        .post(format!("{}/api/telemetry", server.data_url))
        .json(&json!({"id": 101}))
        .send()
        .await?;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    let rec2_next = tokio::time::timeout(Duration::from_secs(2), sink_r2.recv())
        .await?
        .ok_or_else(|| anyhow!("sink r2 closed"))?;
    assert_eq!(rec2_next.data.get("id"), Some(&json!(101)));

    // 6. Stop rule 2 -> now no rules consume s_shared, endpoint must return 404
    let resp = client
        .post(format!("{}/rules/r2/stop", server.rest_url))
        .send()
        .await?;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    let resp = client
        .post(format!("{}/api/telemetry", server.data_url))
        .json(&json!({"id": 102}))
        .send()
        .await?;
    assert_eq!(resp.status(), reqwest::StatusCode::NOT_FOUND);

    // 7. Restart rule 2 -> endpoint is active again
    let resp = client
        .post(format!("{}/rules/r2/start", server.rest_url))
        .send()
        .await?;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    let resp = client
        .post(format!("{}/api/telemetry", server.data_url))
        .json(&json!({"id": 103}))
        .send()
        .await?;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    let rec2_resumed = tokio::time::timeout(Duration::from_secs(2), sink_r2.recv())
        .await?
        .ok_or_else(|| anyhow!("sink r2 closed"))?;
    assert_eq!(rec2_resumed.data.get("id"), Some(&json!(103)));

    Ok(())
}

#[tokio::test]
async fn test_issue28_httppush_method_and_rest_fallback() -> Result<()> {
    let server = TestServer::start().await;
    let client = reqwest::Client::new();

    // Create stream with METHOD="PUT" and DATASOURCE="/put/endpoint"
    let resp = client
        .post(format!("{}/streams", server.rest_url))
        .json(&json!({
            "sql": "CREATE STREAM s_put () WITH (DATASOURCE=\"/put/endpoint\", METHOD=\"PUT\", FORMAT=\"json\", TYPE=\"httppush\")"
        }))
        .send()
        .await?;
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);

    let mut sink_rx = server.state.stream_bus.subscribe("put_out");

    let resp = client
        .post(format!("{}/rules", server.rest_url))
        .json(&json!({
            "id": "r_put",
            "sql": "SELECT v FROM s_put",
            "actions": [{"memory": {"topic": "put_out"}}]
        }))
        .send()
        .await?;
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);

    // 1. POST should return 405 Method Not Allowed
    let resp = client
        .post(format!("{}/put/endpoint", server.data_url))
        .json(&json!({"v": 10}))
        .send()
        .await?;
    assert_eq!(resp.status(), reqwest::StatusCode::METHOD_NOT_ALLOWED);

    // 2. PUT to data port should succeed with 200 ok
    let resp = client
        .put(format!("{}/put/endpoint", server.data_url))
        .json(&json!({"v": 20}))
        .send()
        .await?;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    assert_eq!(resp.text().await?, "ok");

    let rec = tokio::time::timeout(Duration::from_secs(2), sink_rx.recv())
        .await?
        .ok_or_else(|| anyhow!("sink closed"))?;
    assert_eq!(rec.data.get("v"), Some(&json!(20)));

    // 3. PUT to REST port also succeeds via router fallback
    let resp = client
        .put(format!("{}/put/endpoint", server.rest_url))
        .json(&json!({"v": 30}))
        .send()
        .await?;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    assert_eq!(resp.text().await?, "ok");

    let rec_rest = tokio::time::timeout(Duration::from_secs(2), sink_rx.recv())
        .await?
        .ok_or_else(|| anyhow!("sink closed"))?;
    assert_eq!(rec_rest.data.get("v"), Some(&json!(30)));

    Ok(())
}
