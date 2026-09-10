use rekuiper_conf::KuiperConfig;
use rekuiper_core::{RuleManager, StreamBus, StreamManager, TableManager};
use rekuiper_server::routes::{create_router, AppState};
use serde_json::json;
use tokio::net::TcpListener;

async fn spawn_test_server() -> (String, tokio::task::JoinHandle<()>) {
    let (base_url, handle, _) = spawn_test_server_with_state().await;
    (base_url, handle)
}

async fn spawn_test_server_with_state() -> (String, tokio::task::JoinHandle<()>, AppState) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("Failed to bind ephemeral port");
    let local_addr = listener.local_addr().unwrap();

    let stream_bus = StreamBus::new();
    let stream_manager = StreamManager::new();
    let rule_manager = RuleManager::new(stream_bus.clone());

    let state = AppState::new(
        "fvt".to_string(),
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
async fn test_fvt_server_ping_and_root() {
    let (base_url, _handle) = spawn_test_server().await;
    let client = reqwest::Client::new();

    // 1. Ping test (matches fvt/server_test.go)
    let resp = client
        .get(format!("{}/ping", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    // 2. Root metadata test (matches fvt/server_test.go)
    let resp = client.get(&base_url).send().await.unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let json: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(json["version"], "fvt");
    assert!(json["os"].is_string());
    assert!(json["arch"].is_string());
    assert!(json["upTimeSeconds"].is_number());
    assert!(json["cpuUsage"].is_number());
    assert!(json["memoryUsed"].is_number());
    assert!(json["memoryTotal"].is_number());
}

#[tokio::test]
async fn test_fvt_stream_and_rule_lifecycle() {
    let (base_url, _handle) = spawn_test_server().await;
    let client = reqwest::Client::new();

    // 1. Create stream
    let create_stream_payload = json!({
        "sql": "CREATE STREAM demo () WITH (DATASOURCE=\"demo\", FORMAT=\"json\")"
    });
    let resp = client
        .post(format!("{}/streams", base_url))
        .json(&create_stream_payload)
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    // 2. List streams
    let resp = client
        .get(format!("{}/streams", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let streams: Vec<String> = resp.json().await.unwrap();
    assert!(streams.contains(&"demo".to_string()));

    // 3. Create rule
    let create_rule_payload = json!({
        "id": "rule_fvt_1",
        "sql": "SELECT * FROM demo",
        "actions": [{ "log": {} }]
    });
    let resp = client
        .post(format!("{}/rules", base_url))
        .json(&create_rule_payload)
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    // 4. Check rule status
    let resp = client
        .get(format!("{}/rules/rule_fvt_1/status", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let status: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(status["status"], "running");

    // 5. Stop rule
    let resp = client
        .post(format!("{}/rules/rule_fvt_1/stop", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    let resp = client
        .get(format!("{}/rules/rule_fvt_1/status", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let status: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(status["status"], "stopped");

    // 6. Delete rule
    let resp = client
        .delete(format!("{}/rules/rule_fvt_1", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    // 7. Delete stream
    let resp = client
        .delete(format!("{}/streams/demo", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
}

#[tokio::test]
async fn test_http_push_data_ingestion() {
    let (base_url, _handle) = spawn_test_server().await;
    let client = reqwest::Client::new();

    // 1. Create stream `demo` via POST /streams.
    let create_stream_payload = json!({
        "sql": "CREATE STREAM demo () WITH (DATASOURCE=\"demo\", FORMAT=\"json\")"
    });
    let resp = client
        .post(format!("{}/streams", base_url))
        .json(&create_stream_payload)
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    // 2. Create rule `rule_filter` via POST /rules.
    let create_rule_payload = json!({
        "id": "rule_filter",
        "sql": "SELECT * FROM demo WHERE temp > 25",
        "actions": [{ "log": {} }]
    });
    let resp = client
        .post(format!("{}/rules", base_url))
        .json(&create_rule_payload)
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    // 3. Ingest an array of objects via POST /streams/demo/data.
    let resp = client
        .post(format!("{}/streams/demo/data", base_url))
        .json(&json!([{"temp": 20.0}, {"temp": 30.0}]))
        .send()
        .await
        .unwrap();
    // 4. Check status 200 OK.
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let body = resp.text().await.unwrap();
    assert_eq!(body, "Data ingested successfully.\n");

    // Single-object ingestion is also accepted.
    let resp = client
        .post(format!("{}/streams/demo/data", base_url))
        .json(&json!({"temp": 25.0}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    // 5. Post data to a non-existent stream and verify 404 NOT_FOUND.
    let resp = client
        .post(format!("{}/streams/no_such_stream/data", base_url))
        .json(&json!([{"temp": 20.0}]))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::NOT_FOUND);
}

async fn fetch_rule_status(
    client: &reqwest::Client,
    base_url: &str,
    rule: &str,
) -> serde_json::Value {
    client
        .get(format!("{}/rules/{}/status", base_url, rule))
        .send()
        .await
        .unwrap()
        .json::<serde_json::Value>()
        .await
        .unwrap()
}

async fn wait_for_rule_status(
    client: &reqwest::Client,
    base_url: &str,
    rule: &str,
    want_source: u64,
    want_sink: u64,
) -> serde_json::Value {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        let status = fetch_rule_status(client, base_url, rule).await;
        let source = status["sourceRecordsInTotal"].as_u64().unwrap_or(u64::MAX);
        let sink = status["sinkRecordsOutTotal"].as_u64().unwrap_or(u64::MAX);
        if source == want_source && sink == want_sink {
            return status;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "timed out waiting for rule {} status source={} sink={} (got source={} sink={})",
            rule,
            want_source,
            want_sink,
            source,
            sink
        );
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
}

#[tokio::test]
async fn test_rule_count_window_execution() {
    let (base_url, _handle) = spawn_test_server().await;
    let client = reqwest::Client::new();

    // 1. Create stream `sensor_stream` via POST /streams.
    let resp = client
        .post(format!("{}/streams", base_url))
        .json(&json!({
            "sql": "CREATE STREAM sensor_stream () WITH (DATASOURCE=\"sensor_stream\", FORMAT=\"json\")"
        }))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    // 2. Create rule `rule_count_win` via POST /rules.
    let resp = client
        .post(format!("{}/rules", base_url))
        .json(&json!({
            "id": "rule_count_win",
            "sql": "SELECT count(*), avg(temp) FROM sensor_stream GROUP BY COUNTWINDOW(3)",
            "actions": [{ "log": {} }]
        }))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    // 3. Post 3 records via POST /streams/sensor_stream/data.
    let resp = client
        .post(format!("{}/streams/sensor_stream/data", base_url))
        .json(&json!([{"temp": 10.0}, {"temp": 20.0}, {"temp": 30.0}]))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    // 4. The window fires once: 3 records in, 1 aggregate out.
    let status = wait_for_rule_status(&client, &base_url, "rule_count_win", 3, 1).await;
    assert_eq!(status["sourceRecordsInTotal"], 3);
    assert_eq!(status["sinkRecordsOutTotal"], 1);
}

#[tokio::test]
async fn test_rule_metrics_increment() {
    let (base_url, _handle) = spawn_test_server().await;
    let client = reqwest::Client::new();

    let resp = client
        .post(format!("{}/streams", base_url))
        .json(&json!({
            "sql": "CREATE STREAM metric_stream () WITH (DATASOURCE=\"metric_stream\", FORMAT=\"json\")"
        }))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    let resp = client
        .post(format!("{}/rules", base_url))
        .json(&json!({
            "id": "rule_metrics",
            "sql": "SELECT * FROM metric_stream WHERE temp > 25",
            "actions": [{ "log": {} }]
        }))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    // Non-matching record: source increments, sink does not.
    let resp = client
        .post(format!("{}/streams/metric_stream/data", base_url))
        .json(&json!({"temp": 10.0}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let status = wait_for_rule_status(&client, &base_url, "rule_metrics", 1, 0).await;
    assert_eq!(status["sourceRecordsInTotal"], 1);
    assert_eq!(status["sinkRecordsOutTotal"], 0);

    // Matching record: both counters increment.
    let resp = client
        .post(format!("{}/streams/metric_stream/data", base_url))
        .json(&json!({"temp": 30.0}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let status = wait_for_rule_status(&client, &base_url, "rule_metrics", 2, 1).await;
    assert_eq!(status["sourceRecordsInTotal"], 2);
    assert_eq!(status["sinkRecordsOutTotal"], 1);
}

#[tokio::test]
async fn test_tables_and_details_lifecycle() {
    let (base_url, _handle) = spawn_test_server().await;
    let client = reqwest::Client::new();

    // Seed a stream so /streamdetails has content.
    let resp = client
        .post(format!("{}/streams", base_url))
        .json(&json!({
            "sql": "CREATE STREAM demo () WITH (DATASOURCE=\"demo\", FORMAT=\"json\")"
        }))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    // 1. Create table `my_table` via POST /tables.
    let resp = client
        .post(format!("{}/tables", base_url))
        .json(&json!({
            "sql": "CREATE TABLE my_table () WITH (DATASOURCE=\"my_table\", FORMAT=\"json\")"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);
    assert_eq!(resp.text().await.unwrap(), "Table my_table is created.\n");

    // 2. Verify GET /tables includes "my_table".
    let resp = client
        .get(format!("{}/tables", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let tables: Vec<String> = resp.json().await.unwrap();
    assert!(tables.contains(&"my_table".to_string()));

    // 3. Verify GET /tabledetails contains the definition of "my_table".
    let resp = client
        .get(format!("{}/tabledetails", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let details: Vec<serde_json::Value> = resp.json().await.unwrap();
    let entry = details.iter().find(|d| d["name"] == "my_table");
    assert!(
        entry.is_some(),
        "tabledetails should contain my_table: {:?}",
        details
    );

    // 4. Verify GET /streamdetails returns streams with details.
    let resp = client
        .get(format!("{}/streamdetails", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let stream_details: Vec<serde_json::Value> = resp.json().await.unwrap();
    let stream_entry = stream_details.iter().find(|d| d["name"] == "demo");
    assert!(
        stream_entry.is_some(),
        "streamdetails should contain demo: {:?}",
        stream_details
    );

    // 5. Verify GET /tables/my_table/schema returns the schema object.
    let resp = client
        .get(format!("{}/tables/my_table/schema", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let schema: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(schema["name"], "my_table");
    assert!(schema["options"].is_object());

    // Stream schema endpoint works too.
    let resp = client
        .get(format!("{}/streams/demo/schema", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    // 6. Drop the table and verify it is gone.
    let resp = client
        .delete(format!("{}/tables/my_table", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    assert_eq!(resp.text().await.unwrap(), "Table my_table is dropped.\n");

    let resp = client
        .get(format!("{}/tables", base_url))
        .send()
        .await
        .unwrap();
    let tables: Vec<String> = resp.json().await.unwrap();
    assert!(!tables.contains(&"my_table".to_string()));

    let resp = client
        .get(format!("{}/tables/my_table", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn test_rule_validation_topo_and_migration() {
    let (base_url, _handle) = spawn_test_server().await;
    let client = reqwest::Client::new();

    // 1. Validate a valid rule via POST /rules/validate -> 200 OK.
    let resp = client
        .post(format!("{}/rules/validate", base_url))
        .json(&json!({
            "id": "test_rule",
            "sql": "SELECT * FROM test_stream WHERE temp > 25",
            "actions": [{ "log": {} }]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    assert_eq!(
        resp.text().await.unwrap(),
        "The rule has been validated successfully\n"
    );

    // 2. Validate an invalid rule (bad SQL) -> 400 Bad Request.
    let resp = client
        .post(format!("{}/rules/validate", base_url))
        .json(&json!({
            "id": "bad_rule",
            "sql": "SELECT FROM WHERE",
            "actions": [{ "log": {} }]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::BAD_REQUEST);

    // 3. Deploy stream `test_stream` and rule `test_rule`.
    let resp = client
        .post(format!("{}/streams", base_url))
        .json(&json!({
            "sql": "CREATE STREAM test_stream () WITH (DATASOURCE=\"test_stream\", FORMAT=\"json\")"
        }))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    let resp = client
        .post(format!("{}/rules", base_url))
        .json(&json!({
            "id": "test_rule",
            "sql": "SELECT * FROM test_stream WHERE temp > 25",
            "actions": [{ "log": {} }]
        }))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    // 4. Query GET /rules/test_rule/topo -> sources contains "test_stream".
    let resp = client
        .get(format!("{}/rules/test_rule/topo", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let topo: serde_json::Value = resp.json().await.unwrap();
    let sources = topo["sources"]
        .as_array()
        .expect("topo.sources is an array");
    assert!(sources.iter().any(|s| s == "test_stream"));

    // Unknown rule topo -> 404.
    let resp = client
        .get(format!("{}/rules/no_such_rule/topo", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::NOT_FOUND);

    // 5. Query GET /rules/test_rule/explain -> 200 OK.
    let resp = client
        .get(format!("{}/rules/test_rule/explain", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let plan: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(plan["source"], "test_stream");

    // 6. Query GET /rules/status/all -> contains key "test_rule".
    let resp = client
        .get(format!("{}/rules/status/all", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let all: serde_json::Value = resp.json().await.unwrap();
    assert!(all.get("test_rule").is_some());

    // 7. Export via GET /data/export -> contains "test_stream" and "test_rule".
    let resp = client
        .get(format!("{}/data/export", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let body = resp.text().await.unwrap();
    assert!(
        body.contains("test_stream"),
        "export should contain test_stream"
    );
    assert!(
        body.contains("test_rule"),
        "export should contain test_rule"
    );
}

#[tokio::test]
async fn test_openapi_metadata_and_connections() {
    let (base_url, _handle) = spawn_test_server().await;
    let client = reqwest::Client::new();

    // GET /metadata/sources contains mqtt, http, file.
    let resp = client
        .get(format!("{}/metadata/sources", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let sources: Vec<serde_json::Value> = resp.json().await.unwrap();
    let names: Vec<&str> = sources
        .iter()
        .filter_map(|s| s.get("name").and_then(|n| n.as_str()))
        .collect();
    assert!(names.contains(&"mqtt"));
    assert!(names.contains(&"http"));
    assert!(names.contains(&"file"));

    // GET /metadata/sinks contains mqtt, log, file.
    let resp = client
        .get(format!("{}/metadata/sinks", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let sinks: Vec<serde_json::Value> = resp.json().await.unwrap();
    let names: Vec<&str> = sinks
        .iter()
        .filter_map(|s| s.get("name").and_then(|n| n.as_str()))
        .collect();
    assert!(names.contains(&"mqtt"));
    assert!(names.contains(&"log"));
    assert!(names.contains(&"file"));

    // GET /metadata/functions returns 200 OK.
    let resp = client
        .get(format!("{}/metadata/functions", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let functions: Vec<serde_json::Value> = resp.json().await.unwrap();
    assert!(!functions.is_empty());

    // GET /connections returns 200 OK array.
    let resp = client
        .get(format!("{}/connections", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let conns: Vec<serde_json::Value> = resp.json().await.unwrap();
    assert!(conns.is_empty());

    // POST /connections creates a connection.
    let resp = client
        .post(format!("{}/connections", base_url))
        .json(&json!({
            "id": "test_conn",
            "type": "mqtt",
            "server": "tcp://127.0.0.1:1883"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);

    // GET /connections/:id retrieves it.
    let resp = client
        .get(format!("{}/connections/test_conn", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let conn: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(conn["id"], "test_conn");

    // GET /connections now lists it.
    let resp = client
        .get(format!("{}/connections", base_url))
        .send()
        .await
        .unwrap();
    let conns: Vec<serde_json::Value> = resp.json().await.unwrap();
    assert!(conns.iter().any(|c| c["id"] == "test_conn"));

    // DELETE /connections/:id drops it.
    let resp = client
        .delete(format!("{}/connections/test_conn", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    let resp = client
        .get(format!("{}/connections/test_conn", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::NOT_FOUND);

    // GET /plugins/sources returns 200 OK array.
    let resp = client
        .get(format!("{}/plugins/sources", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let plugins: Vec<serde_json::Value> = resp.json().await.unwrap();
    assert!(plugins.is_empty());
}

#[tokio::test]
async fn test_all_openapi_paths_responding() {
    let (base_url, _handle) = spawn_test_server().await;
    let client = reqwest::Client::new();

    // Seed a stream + rule for the endpoints that require existing entities.
    client
        .post(format!("{}/streams", base_url))
        .json(&json!({
            "sql": "CREATE STREAM demo () WITH (DATASOURCE=\"demo\", FORMAT=\"json\")"
        }))
        .send()
        .await
        .unwrap();
    client
        .post(format!("{}/rules", base_url))
        .json(&json!({
            "id": "rule_openapi",
            "sql": "SELECT * FROM demo WHERE temp > 25",
            "actions": [{ "log": {} }]
        }))
        .send()
        .await
        .unwrap();

    for path in [
        "/v2/rules/rule_openapi/status",
        "/rules/rule_openapi/schema",
        "/trace/rule/rule_openapi",
        "/trace/trace_1",
        "/async/task/task_1",
        "/metadata/connections/mqtt",
        "/metadata/sources/yaml/mqtt",
        "/metadata/sinks/yaml/mqtt",
        "/metadata/connections/yaml/mqtt",
        "/plugins/sources/mqtt",
        "/plugins/sinks/mqtt",
        "/plugins/portables/pyfunc",
        "/plugins/portables/pyfunc/status",
        "/services/edgex",
        "/services/functions/echo",
        "/udf/javascript/func1",
    ] {
        let resp = client
            .get(format!("{}{}", base_url, path))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), reqwest::StatusCode::OK);
    }

    for path in [
        "/ruletest",
        "/ruletest/rule_openapi/start",
        "/rules/rule_openapi/trace/start",
        "/rules/rule_openapi/trace/stop",
        "/tracer",
        "/async/data/import",
        "/async/task/task_1/cancel",
        "/batch/req",
        "/plugins/functions/echo/register",
        "/metadata/sources/connection/mqtt",
        "/metadata/sinks/connection/mqtt",
        "/metadata/lookups/connection/mqtt",
    ] {
        let resp = client
            .post(format!("{}{}", base_url, path))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), reqwest::StatusCode::OK, "POST {}", path);
    }

    for path in [
        "/rules/rule_openapi/tags",
        "/schemas/stream/myschema",
        "/schemas/stream/myschema/upload",
        "/plugins/sources/mqtt",
        "/plugins/sinks/mqtt",
        "/plugins/functions/echo",
        "/plugins/portables/pyfunc",
        "/services/edgex",
        "/udf/javascript/func1",
        "/metadata/sources/mqtt/confKeys/testConf",
        "/metadata/sinks/mqtt/confKeys/testConf",
        "/metadata/connections/mqtt/confKeys/testConf",
    ] {
        let resp = client
            .put(format!("{}{}", base_url, path))
            .json(&json!({}))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), reqwest::StatusCode::OK, "PUT {}", path);
    }

    let resp = client
        .patch(format!("{}/rules/rule_openapi/tags", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    for path in [
        "/rules/rule_openapi/tags",
        "/ruletest/rule_openapi",
        "/config/uploads/cfg.json",
        "/schemas/stream/myschema",
        "/plugins/sources/mqtt",
        "/plugins/sinks/mqtt",
        "/plugins/functions/echo",
        "/plugins/portables/pyfunc",
        "/services/edgex",
        "/udf/javascript/func1",
        "/metadata/sources/mqtt/confKeys/testConf",
        "/metadata/sinks/mqtt/confKeys/testConf",
        "/metadata/connections/mqtt/confKeys/testConf",
    ] {
        let resp = client
            .delete(format!("{}{}", base_url, path))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), reqwest::StatusCode::OK, "DELETE {}", path);
    }

    // Rule schema for a missing rule is 404.
    let resp = client
        .get(format!("{}/rules/no_such_rule/schema", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn test_udf_plugin_endpoints() {
    let (base_url, _handle) = spawn_test_server().await;
    let client = reqwest::Client::new();

    // Empty registries list nothing.
    for path in ["/plugins/functions", "/plugins/udfs"] {
        let resp = client
            .get(format!("{}{}", base_url, path))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), reqwest::StatusCode::OK, "GET {}", path);
        let items: Vec<serde_json::Value> = resp.json().await.unwrap();
        assert!(items.is_empty());
    }

    // Register a function plugin and a UDF plugin.
    let resp = client
        .post(format!("{}/plugins/functions", base_url))
        .json(&json!({"name": "my_math", "functions": ["cube", "clamp"]}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);

    let resp = client
        .post(format!("{}/plugins/udfs", base_url))
        .json(&json!({"name": "my_udf", "functions": ["myfunc"]}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);

    // Missing names are rejected.
    let resp = client
        .post(format!("{}/plugins/functions", base_url))
        .json(&json!({"functions": ["x"]}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::BAD_REQUEST);

    // Listings are segregated by plugin type.
    let resp = client
        .get(format!("{}/plugins/functions", base_url))
        .send()
        .await
        .unwrap();
    let items: Vec<serde_json::Value> = resp.json().await.unwrap();
    assert_eq!(items.len(), 1);
    assert_eq!(items[0]["name"], "my_math");
    assert_eq!(items[0]["plugin_type"], "function");

    let resp = client
        .get(format!("{}/plugins/udfs", base_url))
        .send()
        .await
        .unwrap();
    let items: Vec<serde_json::Value> = resp.json().await.unwrap();
    assert_eq!(items.len(), 1);
    assert_eq!(items[0]["name"], "my_udf");

    // Detail lookups, including cross-type misses.
    let resp = client
        .get(format!("{}/plugins/functions/my_math", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let def: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(def["functions"], json!(["cube", "clamp"]));

    let resp = client
        .get(format!("{}/plugins/udfs/my_math", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::NOT_FOUND);

    let resp = client
        .get(format!("{}/plugins/functions/missing", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::NOT_FOUND);

    // Unregister; deletes stay idempotent.
    let resp = client
        .delete(format!("{}/plugins/functions/my_math", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let resp = client
        .get(format!("{}/plugins/functions", base_url))
        .send()
        .await
        .unwrap();
    let items: Vec<serde_json::Value> = resp.json().await.unwrap();
    assert!(items.is_empty());

    let resp = client
        .delete(format!("{}/plugins/udfs/my_udf", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
}

#[tokio::test]
async fn test_schema_registry_crud() {
    let (base_url, _handle) = spawn_test_server().await;
    let client = reqwest::Client::new();
    let book = "syntax = \"proto3\";\nmessage Book { string title = 1; int32 price = 2; }\n";

    // Missing schemas 404.
    let resp = client
        .get(format!("{}/schemas/protobuf/book", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::NOT_FOUND);

    // Register, then list.
    let resp = client
        .post(format!("{}/schemas/protobuf", base_url))
        .json(&json!({"name": "book", "content": book}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);

    let resp = client
        .get(format!("{}/schemas/protobuf", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let names: Vec<String> = resp.json().await.unwrap();
    assert_eq!(names, vec!["book".to_string()]);
    // Other kinds are isolated.
    let resp = client
        .get(format!("{}/schemas/json", base_url))
        .send()
        .await
        .unwrap();
    let names: Vec<String> = resp.json().await.unwrap();
    assert!(names.is_empty());

    // Definition JSON.
    let resp = client
        .get(format!("{}/schemas/protobuf/book", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let def: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(def["name"], "book");
    assert_eq!(def["kind"], "protobuf");
    assert_eq!(def["content"], book);

    // Raw proto text via Accept: text/plain.
    let resp = client
        .get(format!("{}/schemas/protobuf/book", base_url))
        .header(reqwest::header::ACCEPT, "text/plain")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    assert_eq!(resp.text().await.unwrap(), book);

    // Update content, then delete.
    let resp = client
        .put(format!("{}/schemas/protobuf/book", base_url))
        .json(&json!({"content": "syntax = \"proto3\";\nmessage Book { string title = 1; }"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let resp = client
        .get(format!("{}/schemas/protobuf/book", base_url))
        .send()
        .await
        .unwrap();
    let def: serde_json::Value = resp.json().await.unwrap();
    assert!(def["content"].as_str().unwrap().contains("string title"));

    // Upload endpoint upserts too.
    let resp = client
        .put(format!("{}/schemas/protobuf/mag/upload", base_url))
        .json(&json!({"content": book}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let resp = client
        .get(format!("{}/schemas/protobuf", base_url))
        .send()
        .await
        .unwrap();
    let names: Vec<String> = resp.json().await.unwrap();
    assert_eq!(names, vec!["book".to_string(), "mag".to_string()]);

    let resp = client
        .delete(format!("{}/schemas/protobuf/book", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let resp = client
        .get(format!("{}/schemas/protobuf/book", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn test_simulator_and_memory_pipeline() {
    let (base_url, _handle, state) = spawn_test_server_with_state().await;
    let client = reqwest::Client::new();

    // 1. Configure the simulator source data.
    let resp = client
        .put(format!(
            "{}/metadata/sources/simulator/confKeys/sim1",
            base_url
        ))
        .json(&json!({
            "data": [{"a": 10}, {"a": 20}],
            "interval": "5ms",
            "loop": false,
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    // 2. Create a simulator-backed stream.
    let resp = client
        .post(format!("{}/streams", base_url))
        .json(&json!({
            "sql": "CREATE STREAM sim1 () WITH (TYPE=\"simulator\", CONF_KEY=\"sim1\")"
        }))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    // 3. Subscribe to the memory sink topic before the rule starts emitting.
    let mut rx = state.stream_bus.subscribe("test_mem_out");

    // 4. Create a rule projecting into the memory sink.
    let resp = client
        .post(format!("{}/rules", base_url))
        .json(&json!({
            "id": "rule_sim_mem",
            "sql": "SELECT a * 2 AS result FROM sim1",
            "actions": [{"memory": {"topic": "test_mem_out"}}]
        }))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    // 5. Await both records with a 2-second timeout each.
    let mut results = Vec::new();
    for _ in 0..2 {
        let record = tokio::time::timeout(std::time::Duration::from_secs(2), rx.recv())
            .await
            .expect("timed out waiting for memory sink record")
            .expect("memory topic closed");
        results.push(
            record
                .data
                .get("result")
                .cloned()
                .unwrap_or(serde_json::Value::Null),
        );
    }
    assert_eq!(results, vec![json!(20), json!(40)]);

    // 6. Both outputs went through the rule's sink path.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    loop {
        let status: serde_json::Value = client
            .get(format!("{}/rules/rule_sim_mem/status", base_url))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        let sink = status["sinkRecordsOutTotal"].as_u64().unwrap_or(0);
        if sink >= 2 {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "timed out waiting for sinkRecordsOutTotal >= 2 (got {})",
            sink
        );
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
}

#[tokio::test]
async fn test_upstream_fvt_acc_max_by_vehicle_flow() {
    // Replicates ekuiper-upstream fvt/rule_test.go TestAccMaxByMapAggChargeCycle:
    // Rule 1 tracks per-SOC running max temp + its timestamp; Rule 2 reads
    // Rule 1's output through a memory stream and emits the full SOC map once
    // the upload flag is set.
    let (base_url, _handle, state) = spawn_test_server_with_state().await;
    let client = reqwest::Client::new();

    for sql in [
        "CREATE STREAM vehicle_in (soc BIGINT, temp BIGINT, ts BIGINT, upload_flag BIGINT) WITH (FORMAT=\"json\")",
        "CREATE STREAM vehicle_stat_stream () WITH (TYPE=\"memory\", DATASOURCE=\"vehicle_stat\", FORMAT=\"json\")",
    ] {
        let resp = client
            .post(format!("{}/streams", base_url))
            .json(&json!({ "sql": sql }))
            .send()
            .await
            .unwrap();
        assert!(resp.status().is_success(), "create stream: {}", sql);
    }

    // Subscribe before creating rules so no memory message is lost.
    let mut rx = state.stream_bus.subscribe("vehicle_result");

    // Downstream rule first (mirrors the upstream test ordering).
    let resp = client
        .post(format!("{}/rules", base_url))
        .json(&json!({
            "id": "rule_vehicle_stat",
            "sql": "SELECT acc_map_agg(soc, object_construct('max_temp', max_temp, 'max_temp_ts', max_temp_ts)) AS data FROM vehicle_stat_stream WHERE upload_flag = 1",
            "actions": [{"memory": {"topic": "vehicle_result"}}]
        }))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    let resp = client
        .post(format!("{}/rules", base_url))
        .json(&json!({
            "id": "rule_vehicle_in",
            "sql": "SELECT soc, acc_max(temp) OVER (PARTITION BY soc) AS max_temp, acc_max_by(ts, temp) OVER (PARTITION BY soc) AS max_temp_ts, upload_flag FROM vehicle_in",
            "actions": [{"memory": {"topic": "vehicle_stat"}}]
        }))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    // Push the four charge-cycle records.
    let resp = client
        .post(format!("{}/streams/vehicle_in/data", base_url))
        .json(&json!([
            {"soc": 18, "temp": 28, "ts": 1788000000000i64},
            {"soc": 18, "temp": 30, "ts": 1788000060000i64},
            {"soc": 19, "temp": 32, "ts": 1788000120000i64},
            {"soc": 20, "temp": 33, "ts": 1788000210000i64, "upload_flag": 1}
        ]))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    // Await the single aggregated result.
    let record = tokio::time::timeout(std::time::Duration::from_secs(10), rx.recv())
        .await
        .expect("timed out waiting for vehicle_result")
        .expect("vehicle_result topic closed");
    let data = record
        .data
        .get("data")
        .and_then(|v| v.as_array())
        .expect("result should carry a data array");
    assert_eq!(data.len(), 3);
    let find = |key: &str| {
        data.iter()
            .find(|e| e.get("key").and_then(|k| k.as_str()) == Some(key))
            .unwrap_or_else(|| panic!("missing soc entry {}", key))
            .get("value")
            .cloned()
            .unwrap()
    };
    assert_eq!(find("18")["max_temp"], json!(30));
    assert_eq!(find("18")["max_temp_ts"], json!(1788000060000i64));
    assert_eq!(find("19")["max_temp"], json!(32));
    assert_eq!(find("19")["max_temp_ts"], json!(1788000120000i64));
    assert_eq!(find("20")["max_temp"], json!(33));
    assert_eq!(find("20")["max_temp_ts"], json!(1788000210000i64));
}

#[tokio::test]
async fn test_name_validation() {
    let (base_url, _handle) = spawn_test_server().await;
    let client = reqwest::Client::new();

    // Names with spaces (sent percent-encoded) are rejected with 400.
    for path in [
        "streams/invalid%20name",
        "tables/invalid%20name",
        "rules/invalid%20name",
    ] {
        let resp = client
            .get(format!("{}/{}", base_url, path))
            .send()
            .await
            .unwrap();
        assert_eq!(
            resp.status(),
            reqwest::StatusCode::BAD_REQUEST,
            "GET {}",
            path
        );
        let body = resp.text().await.unwrap();
        assert!(
            body.contains("invalid characters"),
            "GET {} body should mention invalid characters: {}",
            path,
            body
        );
    }

    // Sane names still resolve (404 here, proving validation passed).
    let resp = client
        .get(format!("{}/streams/no_such_stream", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn test_csv_file_sink() {
    use rekuiper_connectors::{FileSink, Sink};
    use rekuiper_core::model::StreamRecord;
    use std::collections::HashMap;

    let mut path = std::env::temp_dir();
    path.push(format!(
        "rekuiper-csv-sink-{}-{}.csv",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0)
    ));
    // Fresh file: no header exists yet.
    let _ = tokio::fs::remove_file(&path).await;

    let sink: FileSink = serde_json::from_value(json!({
        "path": path.to_string_lossy(),
        "format": "delimited",
        "hasHeader": true,
    }))
    .expect("FileSink should deserialize delimited options");

    for (a, b) in [(1, "x"), (2, "y")] {
        let mut data = HashMap::new();
        data.insert("a".to_string(), json!(a));
        data.insert("b".to_string(), json!(b));
        sink.send(&StreamRecord::new(data)).await.unwrap();
    }

    let content = tokio::fs::read_to_string(&path).await.unwrap();
    let lines: Vec<&str> = content.lines().collect();
    assert_eq!(lines.len(), 3, "header plus two rows: {:?}", lines);
    assert_eq!(lines[0], "a,b");
    assert_eq!(lines[1], "1,x");
    assert_eq!(lines[2], "2,y");

    let _ = tokio::fs::remove_file(&path).await;
}

#[tokio::test]
async fn test_ruletest_sse_and_unnest() {
    let (base_url, _handle) = spawn_test_server().await;
    let client = reqwest::Client::new();

    // 1. Create the ruletest session with mock source data.
    let resp = client
        .post(format!("{}/ruletest", base_url))
        .json(&json!({
            "id": "rt_test_1",
            "sql": "SELECT id, time, unnest(data) FROM demo",
            "mockSource": {
                "demo": {
                    "loop": false,
                    "data": [{"id": "id1", "time": "2026-01-01", "data": [{"k": 1}, {"k": 2}]}]
                }
            }
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    // 2. The response carries the session id and the REST port.
    let created: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(created["id"], "rt_test_1");
    assert!(created["port"].is_number());

    // 3. Start the simulation.
    let resp = client
        .post(format!("{}/ruletest/rt_test_1/start", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    // 4. Drop the session.
    let resp = client
        .delete(format!("{}/ruletest/rt_test_1", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
}

#[tokio::test]
async fn test_stream_table_lookup_join() {
    let (base_url, _handle, state) = spawn_test_server_with_state().await;
    let client = reqwest::Client::new();

    // 1. Create table `alert_tbl`.
    let resp = client
        .post(format!("{}/tables", base_url))
        .json(&json!({
            "sql": "CREATE TABLE alert_tbl () WITH (FORMAT=\"json\")"
        }))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    // 2. Populate it with two lookup rows.
    let resp = client
        .post(format!("{}/tables/alert_tbl/data", base_url))
        .json(&json!([
            {"id": "d1", "status": "active"},
            {"id": "d2", "status": "warning"}
        ]))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    // 3. Create stream `sensor_str`.
    let resp = client
        .post(format!("{}/streams", base_url))
        .json(&json!({
            "sql": "CREATE STREAM sensor_str () WITH (FORMAT=\"json\")"
        }))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    // 4. LEFT JOIN rule fanning out to a memory topic.
    let resp = client
        .post(format!("{}/rules", base_url))
        .json(&json!({
            "id": "rule_join",
            "sql": "SELECT id, temp, status FROM sensor_str LEFT JOIN alert_tbl ON sensor_str.id = alert_tbl.id",
            "actions": [{"memory": {"topic": "join_res"}}]
        }))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    // 5. Subscribe before ingesting so nothing is lost.
    let mut rx = state.stream_bus.subscribe("join_res");

    // 6. Ingest one matching and one unmatched record.
    let resp = client
        .post(format!("{}/streams/sensor_str/data", base_url))
        .json(&json!([{"id": "d1", "temp": 25.0}, {"id": "d99", "temp": 30.0}]))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    // 7. Matched row carries the table's status.
    let first = tokio::time::timeout(std::time::Duration::from_secs(5), rx.recv())
        .await
        .expect("timed out waiting for first join output")
        .expect("join_res topic closed");
    assert_eq!(first.data.get("id"), Some(&json!("d1")));
    assert_eq!(first.data.get("temp"), Some(&json!(25.0)));
    assert_eq!(first.data.get("status"), Some(&json!("active")));

    // Unmatched row still emits under LEFT JOIN with null status.
    let second = tokio::time::timeout(std::time::Duration::from_secs(5), rx.recv())
        .await
        .expect("timed out waiting for second join output")
        .expect("join_res topic closed");
    assert_eq!(second.data.get("id"), Some(&json!("d99")));
    assert_eq!(second.data.get("temp"), Some(&json!(30.0)));
    assert!(
        matches!(
            second.data.get("status"),
            None | Some(serde_json::Value::Null)
        ),
        "unmatched status should be null or missing, got {:?}",
        second.data.get("status")
    );
}

#[tokio::test]
async fn test_prometheus_metrics_scraping() {
    let (base_url, _handle, _state) = spawn_test_server_with_state().await;
    let client = reqwest::Client::new();

    // Create a stream and a rule.
    let resp = client
        .post(format!("{}/streams", base_url))
        .json(&json!({
            "sql": "CREATE STREAM prom_stream () WITH (FORMAT=\"json\")"
        }))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    let resp = client
        .post(format!("{}/rules", base_url))
        .json(&json!({
            "id": "rule_prom",
            "sql": "SELECT * FROM prom_stream",
            "actions": [{"log": {}}]
        }))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    // Push a record through so counters advance.
    let resp = client
        .post(format!("{}/streams/prom_stream/data", base_url))
        .json(&json!({"temp": 25.0}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    // Wait until the rule has ingested the record.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    loop {
        let status: serde_json::Value = client
            .get(format!("{}/rules/rule_prom/status", base_url))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        if status["sourceRecordsInTotal"].as_u64().unwrap_or(0) >= 1 {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "timed out waiting for rule_prom to ingest data"
        );
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }

    // Scrape the Prometheus exposition.
    let resp = client
        .get(format!("{}/metrics", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let content_type = resp
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();
    assert!(
        content_type.contains("text/plain"),
        "Content-Type should be Prometheus text exposition, got {}",
        content_type
    );
    let body = resp.text().await.unwrap();
    assert!(
        body.contains("kuiper_rule_count{status=\"running\"}"),
        "missing running rule count: {}",
        body
    );
    assert!(
        body.contains("kuiper_rule_status{rule=\"rule_prom\"} 1"),
        "missing rule_prom status: {}",
        body
    );
    assert!(
        body.contains("kuiper_sink_records_in_total{rule=\"rule_prom\"}"),
        "missing rule_prom sink records: {}",
        body
    );
}

#[tokio::test]
async fn test_http_pull_source_pipeline() {
    let (base_url, _handle, state) = spawn_test_server_with_state().await;
    let client = reqwest::Client::new();

    // Seed rule whose status endpoint doubles as the mock JSON source.
    // NOTE: /ping returns plain text, so a JSON object endpoint is used.
    let resp = client
        .post(format!("{}/streams", base_url))
        .json(&json!({
            "sql": "CREATE STREAM seed () WITH (FORMAT=\"json\")"
        }))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    let resp = client
        .post(format!("{}/rules", base_url))
        .json(&json!({
            "id": "rule_seed",
            "sql": "SELECT * FROM seed",
            "actions": [{"log": {}}]
        }))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    // 3. Point an httppull conf key at the seed rule's status endpoint.
    let mock_url = format!("{}/rules/rule_seed/status", base_url);
    let resp = client
        .put(format!(
            "{}/metadata/sources/httppull/confKeys/pull_cfg",
            base_url
        ))
        .json(&json!({"url": mock_url, "method": "get", "interval": 100}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    // 4. Create the pull-backed stream.
    let resp = client
        .post(format!("{}/streams", base_url))
        .json(&json!({
            "sql": "CREATE STREAM ping_stream () WITH (TYPE=\"httppull\", CONF_KEY=\"pull_cfg\")"
        }))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    // 5. Rule the pulled records through a memory sink.
    let resp = client
        .post(format!("{}/rules", base_url))
        .json(&json!({
            "id": "rule_pull",
            "sql": "SELECT * FROM ping_stream",
            "actions": [{"memory": {"topic": "pull_results"}}]
        }))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    // 6. Subscribe to the results topic.
    let mut rx = state.stream_bus.subscribe("pull_results");

    // 7. Polled status objects arrive within 2 seconds.
    let record = tokio::time::timeout(std::time::Duration::from_secs(2), rx.recv())
        .await
        .expect("timed out waiting for http pull record")
        .expect("pull_results topic closed");
    assert_eq!(
        record.data.get("status"),
        Some(&json!("running")),
        "unexpected pull record: {:?}",
        record.data
    );
}

#[tokio::test]
async fn test_websocket_source_and_sink() {
    use futures::{SinkExt, StreamExt};
    use tokio_tungstenite::{accept_async, tungstenite::Message};

    let (base_url, _handle, state) = spawn_test_server_with_state().await;
    let client = reqwest::Client::new();

    // 1. Sink-side probe server: captures the first text frame it receives.
    let sink_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let sink_port = sink_listener.local_addr().unwrap().port();
    let (frame_tx, mut frame_rx) = tokio::sync::mpsc::channel::<String>(4);
    tokio::spawn(async move {
        let Ok((stream, _)) = sink_listener.accept().await else {
            return;
        };
        let Ok(mut ws) = accept_async(stream).await else {
            return;
        };
        while let Some(msg) = ws.next().await {
            match msg {
                Ok(Message::Text(text)) => {
                    let _ = frame_tx.send(text).await;
                    return;
                }
                Ok(_) => continue,
                Err(_) => return,
            }
        }
    });

    // 2. Source-side probe server: pushes one device reading per connection.
    let source_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let source_port = source_listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        let Ok((stream, _)) = source_listener.accept().await else {
            return;
        };
        let Ok(mut ws) = accept_async(stream).await else {
            return;
        };
        let _ = ws
            .send(Message::Text(
                r#"{"id": "ws_device", "temp": 33.3}"#.to_string(),
            ))
            .await;
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        let _ = ws.close(None).await;
    });

    // 3a. Sink test: rule fans stream records out over WebSocket.
    let resp = client
        .post(format!("{}/streams", base_url))
        .json(&json!({
            "sql": "CREATE STREAM sink_stream () WITH (FORMAT=\"json\")"
        }))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    let resp = client
        .post(format!("{}/rules", base_url))
        .json(&json!({
            "id": "rule_ws_sink",
            "sql": "SELECT * FROM sink_stream",
            "actions": [{"websocket": {
                "addr": format!("127.0.0.1:{}", sink_port),
                "path": "/ws_sink"
            }}]
        }))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    let resp = client
        .post(format!("{}/streams/sink_stream/data", base_url))
        .json(&json!({"id": "ws_device", "temp": 33.3}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    let frame = tokio::time::timeout(std::time::Duration::from_secs(2), frame_rx.recv())
        .await
        .expect("timed out waiting for websocket sink frame")
        .expect("sink probe channel closed");
    let frame_json: serde_json::Value = serde_json::from_str(&frame).unwrap();
    assert_eq!(frame_json["id"], "ws_device");
    assert_eq!(frame_json["temp"], 33.3);

    // 3b. Source test: subscribe first so no broadcast is lost.
    let mut rx = state.stream_bus.subscribe("ws_out");

    let resp = client
        .post(format!("{}/streams", base_url))
        .json(&json!({
            "sql": format!(
                "CREATE STREAM ws_source_stream () WITH (TYPE=\"websocket\", DATASOURCE=\"ws://127.0.0.1:{}/ws_source\")",
                source_port
            )
        }))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    // 4. Rule the pulled readings into a memory topic.
    let resp = client
        .post(format!("{}/rules", base_url))
        .json(&json!({
            "id": "rule_ws_source",
            "sql": "SELECT * FROM ws_source_stream",
            "actions": [{"memory": {"topic": "ws_out"}}]
        }))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    let record = tokio::time::timeout(std::time::Duration::from_secs(2), rx.recv())
        .await
        .expect("timed out waiting for websocket source record")
        .expect("ws_out topic closed");
    assert_eq!(record.data.get("id"), Some(&json!("ws_device")));
    assert_eq!(record.data.get("temp"), Some(&json!(33.3)));
}

// ---------------------------------------------------------------------------
// Hermetic in-process Redis replacement speaking just enough RESP for the
// redis connector paths (SET/GET/PUBLISH/SUBSCRIBE), so no external daemon
// is required on any platform.
// ---------------------------------------------------------------------------

#[derive(Debug)]
struct MockRedisCmd {
    cmd: String,
    args: Vec<String>,
}

async fn read_resp_command<R>(reader: &mut R) -> Option<Vec<String>>
where
    R: tokio::io::AsyncBufRead + Unpin,
{
    use tokio::io::{AsyncBufReadExt, AsyncReadExt};
    let trim = |s: &str| s.trim_end_matches(['\r', '\n']).to_string();
    let mut line = String::new();
    reader.read_line(&mut line).await.ok()?;
    let count: usize = trim(&line).strip_prefix('*')?.parse().ok()?;
    let mut args = Vec::with_capacity(count);
    for _ in 0..count {
        line.clear();
        reader.read_line(&mut line).await.ok()?;
        let len: usize = trim(&line).strip_prefix('$')?.parse().ok()?;
        let mut buf = vec![0u8; len];
        reader.read_exact(&mut buf).await.ok()?;
        let mut crlf = [0u8; 2];
        reader.read_exact(&mut crlf).await.ok()?;
        args.push(String::from_utf8_lossy(&buf).into_owned());
    }
    Some(args)
}

async fn handle_mock_redis_conn(
    stream: tokio::net::TcpStream,
    cmd_tx: tokio::sync::mpsc::Sender<MockRedisCmd>,
) {
    use tokio::io::{AsyncWriteExt, BufReader};
    let (rh, mut wh) = stream.into_split();
    let mut rd = BufReader::new(rh);
    loop {
        let Some(argv) = read_resp_command(&mut rd).await else {
            break;
        };
        if argv.is_empty() {
            continue;
        }
        let cmd = argv[0].to_ascii_uppercase();
        let args = argv[1..].to_vec();
        let _ = cmd_tx
            .send(MockRedisCmd {
                cmd: cmd.clone(),
                args: args.clone(),
            })
            .await;
        let reply: Vec<u8> = match cmd.as_str() {
            "PING" => b"+PONG\r\n".to_vec(),
            "QUIT" => b"+OK\r\n".to_vec(),
            "CLIENT" => b"+OK\r\n".to_vec(),
            "SET" => b"+OK\r\n".to_vec(),
            "GET" => {
                let key = args.first().map(|s| s.as_str()).unwrap_or("");
                // Only d1 exists; anything else is a cache miss.
                if key == "d1" {
                    let body = r#"{"id":"d1","status":"active"}"#;
                    format!("${}\r\n{}\r\n", body.len(), body).into_bytes()
                } else {
                    b"$-1\r\n".to_vec()
                }
            }
            "PUBLISH" => b":1\r\n".to_vec(),
            "SUBSCRIBE" => {
                let channel = args.first().cloned().unwrap_or_default();
                let msg = r#"{"id":"r1","temp":21.5}"#;
                format!(
                    "*3\r\n$9\r\nsubscribe\r\n${}\r\n{}\r\n:1\r\n*3\r\n$7\r\nmessage\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
                    channel.len(),
                    channel,
                    channel.len(),
                    channel,
                    msg.len(),
                    msg
                )
                .into_bytes()
            }
            _ => format!("-ERR unknown command '{}'\r\n", argv[0]).into_bytes(),
        };
        if wh.write_all(&reply).await.is_err() {
            break;
        }
        if cmd == "QUIT" {
            break;
        }
    }
}

async fn spawn_mock_redis() -> (String, tokio::sync::mpsc::Receiver<MockRedisCmd>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = format!("127.0.0.1:{}", listener.local_addr().unwrap().port());
    let (cmd_tx, cmd_rx) = tokio::sync::mpsc::channel(64);
    tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                break;
            };
            let cmd_tx = cmd_tx.clone();
            tokio::spawn(async move { handle_mock_redis_conn(stream, cmd_tx).await });
        }
    });
    (addr, cmd_rx)
}

async fn next_mock_cmd(
    rx: &mut tokio::sync::mpsc::Receiver<MockRedisCmd>,
    want: &str,
) -> MockRedisCmd {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    loop {
        assert!(
            std::time::Instant::now() < deadline,
            "timed out waiting for Redis command {}",
            want
        );
        match tokio::time::timeout(std::time::Duration::from_millis(200), rx.recv()).await {
            Ok(Some(cmd)) if cmd.cmd == want => return cmd,
            // Unrelated commands (e.g. lookups from other flows) are skipped.
            _ => continue,
        }
    }
}

#[tokio::test]
async fn test_redis_connectors_pipeline() {
    let (redis_addr, mut cmd_rx) = spawn_mock_redis().await;
    let (base_url, _handle, state) = spawn_test_server_with_state().await;
    let client = reqwest::Client::new();

    // Lookup-table config pointing at the mock daemon.
    let resp = client
        .put(format!(
            "{}/metadata/sources/redis/confKeys/rt_redis",
            base_url
        ))
        .json(&json!({"addr": redis_addr}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    // a/b) One stream fanning out to a Redis SET rule and a PUBLISH rule.
    let resp = client
        .post(format!("{}/streams", base_url))
        .json(&json!({
            "sql": "CREATE STREAM redis_stream () WITH (FORMAT=\"json\")"
        }))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    let resp = client
        .post(format!("{}/rules", base_url))
        .json(&json!({
            "id": "rule_redis_set",
            "sql": "SELECT * FROM redis_stream",
            "actions": [{"redis": {"addr": redis_addr, "field": "id"}}]
        }))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    let resp = client
        .post(format!("{}/rules", base_url))
        .json(&json!({
            "id": "rule_redis_pub",
            "sql": "SELECT * FROM redis_stream",
            "actions": [{"redisPub": {"addr": redis_addr, "topic": "channel1"}}]
        }))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    // c) Redis lookup table + LEFT JOIN rule.
    let resp = client
        .post(format!("{}/tables", base_url))
        .json(&json!({
            "sql": "CREATE TABLE redis_tbl () WITH (TYPE=\"redis\", CONF_KEY=\"rt_redis\")"
        }))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    let resp = client
        .post(format!("{}/streams", base_url))
        .json(&json!({
            "sql": "CREATE STREAM sensor_redis () WITH (FORMAT=\"json\")"
        }))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    let mut rx = state.stream_bus.subscribe("join_redis_res");
    let resp = client
        .post(format!("{}/rules", base_url))
        .json(&json!({
            "id": "rule_join_redis",
            "sql": "SELECT id, temp, status FROM sensor_redis LEFT JOIN redis_tbl ON sensor_redis.id = redis_tbl.id",
            "actions": [{"memory": {"topic": "join_redis_res"}}]
        }))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    // a) Sink path: SET rk1 <json>.
    let resp = client
        .post(format!("{}/streams/redis_stream/data", base_url))
        .json(&json!({"id": "rk1", "temp": 22.5}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    let set = next_mock_cmd(&mut cmd_rx, "SET").await;
    assert_eq!(set.args.first().map(|s| s.as_str()), Some("rk1"));
    let stored: serde_json::Value =
        serde_json::from_str(set.args.get(1).map(|s| s.as_str()).unwrap_or("")).unwrap();
    assert_eq!(stored["id"], "rk1");

    // b) Pub path: PUBLISH channel1 <json>.
    let published = next_mock_cmd(&mut cmd_rx, "PUBLISH").await;
    assert_eq!(published.args.first().map(|s| s.as_str()), Some("channel1"));
    let published_json: serde_json::Value =
        serde_json::from_str(published.args.get(1).map(|s| s.as_str()).unwrap_or("")).unwrap();
    assert_eq!(published_json["id"], "rk1");

    // c) Lookup path: matched row merges, unmatched row stays null.
    let resp = client
        .post(format!("{}/streams/sensor_redis/data", base_url))
        .json(&json!([{"id": "d1", "temp": 25.0}, {"id": "d99", "temp": 30.0}]))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    let first = tokio::time::timeout(std::time::Duration::from_secs(5), rx.recv())
        .await
        .expect("timed out waiting for first redis join output")
        .expect("join_redis_res topic closed");
    assert_eq!(first.data.get("id"), Some(&json!("d1")));
    assert_eq!(first.data.get("temp"), Some(&json!(25.0)));
    assert_eq!(first.data.get("status"), Some(&json!("active")));

    let second = tokio::time::timeout(std::time::Duration::from_secs(5), rx.recv())
        .await
        .expect("timed out waiting for second redis join output")
        .expect("join_redis_res topic closed");
    assert_eq!(second.data.get("id"), Some(&json!("d99")));
    assert_eq!(second.data.get("temp"), Some(&json!(30.0)));
    assert!(
        matches!(
            second.data.get("status"),
            None | Some(serde_json::Value::Null)
        ),
        "unmatched status should be null or missing, got {:?}",
        second.data.get("status")
    );
}

#[tokio::test]
async fn test_redis_sub_source() {
    let (redis_addr, _cmd_rx) = spawn_mock_redis().await;
    let (base_url, _handle, state) = spawn_test_server_with_state().await;
    let client = reqwest::Client::new();

    // Point the redissub stream at the mock daemon.
    let resp = client
        .put(format!(
            "{}/metadata/sources/redis/confKeys/sub_conf",
            base_url
        ))
        .json(&json!({"addr": redis_addr}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    let resp = client
        .post(format!("{}/streams", base_url))
        .json(&json!({
            "sql": "CREATE STREAM sub_stream () WITH (TYPE=\"redissub\", CONF_KEY=\"sub_conf\", DATASOURCE=\"testchan\")"
        }))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    let mut rx = state.stream_bus.subscribe("sub_res");
    let resp = client
        .post(format!("{}/rules", base_url))
        .json(&json!({
            "id": "rule_redis_sub",
            "sql": "SELECT * FROM sub_stream",
            "actions": [{"memory": {"topic": "sub_res"}}]
        }))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    let record = tokio::time::timeout(std::time::Duration::from_secs(5), rx.recv())
        .await
        .expect("timed out waiting for redis sub record")
        .expect("sub_res topic closed");
    assert_eq!(record.data.get("id"), Some(&json!("r1")));
    assert_eq!(record.data.get("temp"), Some(&json!(21.5)));
}

#[tokio::test]
async fn test_kafka_config_and_pipeline() {
    use rekuiper_connectors::KafkaConfig;

    // KafkaConfig parsing, defaults and topic handling.
    let cfg: KafkaConfig = serde_json::from_value(json!({})).unwrap();
    assert_eq!(cfg.brokers, "127.0.0.1:9092");
    assert_eq!(cfg.topic, None);
    assert_eq!(cfg.partition, 0);
    let cfg: KafkaConfig =
        serde_json::from_value(json!({"brokers": "a:9092,b:9092", "topic": "events"})).unwrap();
    assert_eq!(
        cfg.broker_list(),
        vec!["a:9092".to_string(), "b:9092".to_string()]
    );
    assert_eq!(cfg.topic.as_deref(), Some("events"));

    let (base_url, _handle, _state) = spawn_test_server_with_state().await;
    let client = reqwest::Client::new();

    // Reserve a localhost port and leave it closed so broker dials fail fast.
    let closed_port = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let dead_brokers = format!("127.0.0.1:{}", closed_port);

    // Kafka stream + rule with a kafka action: full registration path.
    let resp = client
        .post(format!("{}/streams", base_url))
        .json(&json!({
            "sql": "CREATE STREAM kafka_stream () WITH (TYPE=\"kafka\", DATASOURCE=\"events\")"
        }))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    let rule_sql = "SELECT * FROM kafka_stream";
    let resp = client
        .post(format!("{}/rules", base_url))
        .json(&json!({
            "id": "rule_kafka",
            "sql": rule_sql,
            "actions": [{"kafka": {"brokers": dead_brokers, "topic": "output_events", "key": "id"}}]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);

    // Topology exposes the kafka source stream.
    let resp = client
        .get(format!("{}/rules/rule_kafka/topo", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let topo: serde_json::Value = resp.json().await.unwrap();
    let sources = topo["sources"]
        .as_array()
        .expect("topo.sources is an array");
    assert!(sources.iter().any(|s| s == "kafka_stream"));

    // Ingest a record: the unreachable broker must surface as a counted
    // exception rather than crashing the server.
    let resp = client
        .post(format!("{}/streams/kafka_stream/data", base_url))
        .json(&json!({"id": "k1", "temp": 21.5}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(20);
    loop {
        let status: serde_json::Value = client
            .get(format!("{}/rules/rule_kafka/status", base_url))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        if status["exceptionsTotal"].as_u64().unwrap_or(0) >= 1 {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "timed out waiting for kafka exception counter"
        );
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }

    // Stopping the rule (and its kafka consumer) works cleanly.
    let resp = client
        .post(format!("{}/rules/rule_kafka/stop", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    // Server is still alive afterwards.
    let resp = client
        .get(format!("{}/ping", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
}

#[tokio::test]
async fn test_file_source_streaming_ingestion() {
    let (base_url, _handle, state) = spawn_test_server_with_state().await;
    let client = reqwest::Client::new();

    // 1. Temporary JSON Lines file with three records.
    let mut db_path = std::env::temp_dir();
    db_path.push(format!(
        "rekuiper-file-source-{}-{}.json",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0)
    ));
    // Forward slashes keep the SQL string literal simple on every platform.
    let data_path = db_path.to_string_lossy().replace('\\', "/");
    tokio::fs::write(&db_path, "{\"temp\": 20}\n{\"temp\": 25}\n{\"temp\": 30}\n")
        .await
        .unwrap();

    // 2. File-backed stream; the rule fans out to a memory topic.
    let resp = client
        .post(format!("{}/streams", base_url))
        .json(&json!({
            "sql": format!(
                "CREATE STREAM test_file_stream () WITH (TYPE=\"file\", DATASOURCE=\"{}\", FORMAT=\"json\")",
                data_path
            )
        }))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    let mut rx = state.stream_bus.subscribe("file_out");
    let resp = client
        .post(format!("{}/rules", base_url))
        .json(&json!({
            "id": "rule_file_src",
            "sql": "SELECT * FROM test_file_stream",
            "actions": [{"memory": {"topic": "file_out"}}]
        }))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    // 3. All three file records flow through the rule, in file order.
    let mut temps = Vec::new();
    for _ in 0..3 {
        let record = tokio::time::timeout(std::time::Duration::from_secs(2), rx.recv())
            .await
            .expect("timed out waiting for file record")
            .expect("file_out topic closed");
        temps.push(
            record
                .data
                .get("temp")
                .cloned()
                .unwrap_or(serde_json::Value::Null),
        );
    }
    assert_eq!(temps, vec![json!(20), json!(25), json!(30)]);

    // Rule saw every record on its source side too.
    let status: serde_json::Value = client
        .get(format!("{}/rules/rule_file_src/status", base_url))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(status["sourceRecordsInTotal"].as_u64().unwrap_or(0) >= 3);

    // 4. Stopping the rule cancels the file reader cleanly.
    let resp = client
        .post(format!("{}/rules/rule_file_src/stop", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    assert!(
        !state.source_cancels.read().contains_key("rule_file_src"),
        "stopped rule should release its source handle"
    );

    let _ = tokio::fs::remove_file(&db_path).await;
}

#[tokio::test]
async fn test_mqtt_source_lifecycle_and_defaults() {
    let (base_url, _handle, state) = spawn_test_server_with_state().await;
    let client = reqwest::Client::new();

    async fn create_stream(client: &reqwest::Client, base_url: &str, sql: &str) {
        let resp = client
            .post(format!("{}/streams", base_url))
            .json(&json!({ "sql": sql }))
            .send()
            .await
            .unwrap();
        assert!(resp.status().is_success(), "create stream: {}", sql);
    }

    async fn create_rule(client: &reqwest::Client, base_url: &str, id: &str, sql: &str) {
        let resp = client
            .post(format!("{}/rules", base_url))
            .json(&json!({
                "id": id,
                "sql": sql,
                "actions": [{"log": {}}]
            }))
            .send()
            .await
            .unwrap();
        assert!(resp.status().is_success(), "create rule: {}", id);
    }

    fn has_source_handle(state: &AppState, rule_id: &str) -> bool {
        state.source_cancels.read().contains_key(rule_id)
    }

    // 1. Typeless stream defaults to MQTT: subscriber task is registered.
    create_stream(
        &client,
        &base_url,
        "CREATE STREAM demo () WITH (DATASOURCE=\"demo\")",
    )
    .await;
    create_rule(
        &client,
        &base_url,
        "rule_mqtt_default",
        "SELECT * FROM demo",
    )
    .await;
    assert!(
        has_source_handle(&state, "rule_mqtt_default"),
        "typeless stream should bootstrap an MQTT subscriber"
    );

    // 2. Explicit TYPE="mqtt" also bootstraps.
    create_stream(
        &client,
        &base_url,
        "CREATE STREAM m_mqtt () WITH (TYPE=\"mqtt\", DATASOURCE=\"telemetry\")",
    )
    .await;
    create_rule(
        &client,
        &base_url,
        "rule_mqtt_explicit",
        "SELECT * FROM m_mqtt",
    )
    .await;
    assert!(
        has_source_handle(&state, "rule_mqtt_explicit"),
        "TYPE=mqtt stream should bootstrap an MQTT subscriber"
    );

    // 3. Other source types must not bootstrap MQTT.
    create_stream(
        &client,
        &base_url,
        "CREATE STREAM m_mem () WITH (TYPE=\"memory\")",
    )
    .await;
    create_rule(&client, &base_url, "rule_mem", "SELECT * FROM m_mem").await;
    assert!(
        !has_source_handle(&state, "rule_mem"),
        "TYPE=memory stream must not bootstrap an MQTT subscriber"
    );

    // 4. Stopping the rule cancels the source task and drops its handle.
    let resp = client
        .post(format!("{}/rules/rule_mqtt_default/stop", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    assert!(
        !has_source_handle(&state, "rule_mqtt_default"),
        "stopped rule should release its source handle"
    );
    let status: serde_json::Value = client
        .get(format!("{}/rules/rule_mqtt_default/status", base_url))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(status["status"], "stopped");
}

#[tokio::test]
async fn test_sql_connector_and_data_template() {
    use rekuiper_connectors::apply_data_template;
    use std::collections::HashMap;

    // 1. Template rendering substitutes record fields.
    let mut data = HashMap::new();
    data.insert("id".to_string(), json!("d1"));
    data.insert("temp".to_string(), json!(25));
    let map: serde_json::Map<String, serde_json::Value> =
        data.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
    assert_eq!(
        apply_data_template("device: {{.id}}, temp: {{.temp}}", &map),
        "device: d1, temp: 25"
    );

    // 2. Back the test with a temp-file SQLite database (file-backed so that
    // the per-operation pools used by sinks and lookups share one database).
    let mut db_path = std::env::temp_dir();
    db_path.push(format!(
        "rekuiper-sql-test-{}-{}.db",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0)
    ));
    // `mode=rwc` or `create_if_missing`: ensure SQLite file is created cleanly on Windows.
    let db_url = format!("sqlite://{}", db_path.to_string_lossy().replace('\\', "/"));
    let pool = sqlx::sqlite::SqlitePool::connect_with(
        sqlx::sqlite::SqliteConnectOptions::new()
            .filename(&db_path)
            .create_if_missing(true),
    )
    .await
    .unwrap();
    sqlx::query("CREATE TABLE alerts (id TEXT, status TEXT)")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO alerts (id, status) VALUES ('d1', 'active')")
        .execute(&pool)
        .await
        .unwrap();

    let (base_url, _handle, state) = spawn_test_server_with_state().await;
    let client = reqwest::Client::new();

    // 3. Rule writing rows through the sql sink action.
    let resp = client
        .post(format!("{}/streams", base_url))
        .json(&json!({
            "sql": "CREATE STREAM sql_stream () WITH (FORMAT=\"json\")"
        }))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    let resp = client
        .post(format!("{}/rules", base_url))
        .json(&json!({
            "id": "rule_sql_sink",
            "sql": "SELECT * FROM sql_stream",
            "actions": [{"sql": {"url": db_url, "table": "alerts", "fields": ["id", "status"]}}]
        }))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    let resp = client
        .post(format!("{}/streams/sql_stream/data", base_url))
        .json(&json!({"id": "d9", "temp": 30}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    loop {
        let row: Option<(String, String)> =
            sqlx::query_as("SELECT id, status FROM alerts WHERE id = 'd9'")
                .fetch_optional(&pool)
                .await
                .unwrap();
        if let Some((id, status)) = row {
            assert_eq!(id, "d9");
            assert_eq!(status, "");
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "timed out waiting for sql sink insert"
        );
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }

    // 4. LEFT JOIN enrichment against the TYPE=sql lookup table.
    let resp = client
        .put(format!(
            "{}/metadata/sources/sql/confKeys/sql_cfg",
            base_url
        ))
        .json(&json!({"url": db_url, "table": "alerts"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    let resp = client
        .post(format!("{}/tables", base_url))
        .json(&json!({
            "sql": "CREATE TABLE sql_tbl () WITH (TYPE=\"sql\", CONF_KEY=\"sql_cfg\")"
        }))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    let resp = client
        .post(format!("{}/streams", base_url))
        .json(&json!({
            "sql": "CREATE STREAM sensor_sql () WITH (FORMAT=\"json\")"
        }))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    let mut rx = state.stream_bus.subscribe("join_sql_res");
    let resp = client
        .post(format!("{}/rules", base_url))
        .json(&json!({
            "id": "rule_join_sql",
            "sql": "SELECT id, temp, status FROM sensor_sql LEFT JOIN sql_tbl ON sensor_sql.id = sql_tbl.id",
            "actions": [{"memory": {"topic": "join_sql_res"}}]
        }))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    let resp = client
        .post(format!("{}/streams/sensor_sql/data", base_url))
        .json(&json!({"id": "d1", "temp": 25.0}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    let joined = tokio::time::timeout(std::time::Duration::from_secs(5), rx.recv())
        .await
        .expect("timed out waiting for sql join output")
        .expect("join_sql_res topic closed");
    assert_eq!(joined.data.get("id"), Some(&json!("d1")));
    assert_eq!(joined.data.get("temp"), Some(&json!(25.0)));
    assert_eq!(joined.data.get("status"), Some(&json!("active")));

    let _ = tokio::fs::remove_file(&db_path).await;
}

#[tokio::test]
async fn test_graph_rule_lifecycle_and_dag_execution() {
    let (base_url, _handle, _state) = spawn_test_server_with_state().await;
    let client = reqwest::Client::new();

    let resp = client
        .post(format!("{}/streams", base_url))
        .json(&json!({
            "sql": "CREATE STREAM graph_test_stream () WITH (FORMAT=\"json\")"
        }))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    let graph = json!({
        "nodes": {
            "src": {"type": "source", "nodeType": "stream", "props": {"sourceName": "graph_test_stream"}},
            "f": {"type": "operator", "nodeType": "filter", "props": {"expr": "temperature > 25"}},
            "out": {"type": "sink", "nodeType": "log", "props": {}}
        },
        "topo": {
            "sources": ["src"],
            "edges": {"src": ["f"], "f": ["out"]}
        }
    });

    // Validate the graph rule without deploying it.
    let resp = client
        .post(format!("{}/rules/validate", base_url))
        .json(&json!({ "id": "rule_graph_val", "graph": graph }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    // Deploy the compiled DAG as a live rule.
    let resp = client
        .post(format!("{}/rules", base_url))
        .json(&json!({ "id": "rule_graph_active", "graph": graph }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);

    // The native DAG topology is reported back.
    let resp = client
        .get(format!("{}/rules/rule_graph_active/topo", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let topo: serde_json::Value = resp.json().await.unwrap();
    let sources = topo["sources"]
        .as_array()
        .expect("topo.sources is an array");
    assert!(
        sources
            .iter()
            .any(|s| s == "graph_test_stream" || s == "src"),
        "unexpected topo sources: {}",
        topo
    );
    assert_eq!(topo["edges"]["src"], json!(["f"]));

    // A matching record flows through the compiled filter to the sink.
    let resp = client
        .post(format!("{}/streams/graph_test_stream/data", base_url))
        .json(&json!({"temperature": 32.0}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    loop {
        let status: serde_json::Value = client
            .get(format!("{}/rules/rule_graph_active/status", base_url))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        if status["sourceRecordsInTotal"].as_u64().unwrap_or(0) >= 1
            && status["sinkRecordsOutTotal"].as_u64().unwrap_or(0) >= 1
        {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "timed out waiting for graph rule metrics: {}",
            status
        );
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }

    // Cleanup: drop the rule (the stream stays for other tests to ignore).
    let resp = client
        .delete(format!("{}/rules/rule_graph_active", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
}

#[tokio::test]
async fn test_hopping_window_overlapping_execution() {
    let (base_url, _handle, state) = spawn_test_server_with_state().await;
    let client = reqwest::Client::new();

    let resp = client
        .post(format!("{}/streams", base_url))
        .json(&json!({
            "sql": "CREATE STREAM hop_stream () WITH (FORMAT=\"json\")"
        }))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    // Subscribe before the rule exists so no window output is lost.
    let mut sink_rx = state.stream_bus.subscribe("hop_sink_topic");

    // Window length 300ms, hop 100ms: consecutive hops overlap heavily.
    let resp = client
        .post(format!("{}/rules", base_url))
        .json(&json!({
            "id": "rule_hop_test",
            "sql": "SELECT count(*) AS cnt, sum(val) AS total FROM hop_stream GROUP BY HOPPINGWINDOW(ms, 300, 100)",
            "actions": [{"memory": {"topic": "hop_sink_topic"}}]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);

    async fn post_data(client: &reqwest::Client, base_url: &str, payload: serde_json::Value) {
        let resp = client
            .post(format!("{}/streams/hop_stream/data", base_url))
            .json(&payload)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), reqwest::StatusCode::OK);
    }

    async fn recv_output(
        rx: &mut tokio::sync::broadcast::Receiver<rekuiper_core::StreamRecord>,
    ) -> serde_json::Value {
        tokio::time::timeout(std::time::Duration::from_secs(2), rx.recv())
            .await
            .expect("timed out waiting for window output")
            .expect("hop_sink_topic closed")
            .data
            .into_iter()
            .collect::<serde_json::Map<String, serde_json::Value>>()
            .into()
    }

    // Phase 1: two events in one POST land in the same hop-age bracket; the
    // first emission covering both proves the window fired over buffered rows.
    post_data(&client, &base_url, json!([{"val": 10}, {"val": 20}])).await;
    let hop1 = recv_output(&mut sink_rx).await;
    assert_eq!(hop1["cnt"], json!(2), "hop 1 output: {}", hop1);
    assert_eq!(hop1["total"], json!(30), "hop 1 output: {}", hop1);

    // Phase 2 (overlap): a third event arrives while the first two are still
    // inside the 300ms window — the next hop must retain all three. Stale
    // duplicate emissions are skipped by waiting for cnt == 3.
    post_data(&client, &base_url, json!([{"val": 30}])).await;
    let hop2 = loop {
        let out = recv_output(&mut sink_rx).await;
        if out["cnt"] == json!(3) {
            break out;
        }
    };
    assert_eq!(hop2["total"], json!(60), "hop 2 output: {}", hop2);

    // Phase 3 (expiration): wait until every buffered record is older than
    // the 300ms window, drain stale emissions, then send two fresh events.
    // The next hop must contain exactly the fresh pair.
    tokio::time::sleep(std::time::Duration::from_millis(450)).await;
    while tokio::time::timeout(std::time::Duration::from_millis(50), sink_rx.recv())
        .await
        .is_ok()
    {}
    post_data(&client, &base_url, json!([{"val": 40}, {"val": 30}])).await;
    let hop3 = recv_output(&mut sink_rx).await;
    assert_eq!(hop3["cnt"], json!(2), "hop 3 output: {}", hop3);
    assert_eq!(hop3["total"], json!(70), "hop 3 output: {}", hop3);

    // Metrics: all five source records seen; at least the three key hops out.
    let status: serde_json::Value = client
        .get(format!("{}/rules/rule_hop_test/status", base_url))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(status["sourceRecordsInTotal"], json!(5));
    assert!(
        status["sinkRecordsOutTotal"].as_u64().unwrap_or(0) >= 3,
        "status: {}",
        status
    );

    // Delete the rule cleanly.
    let resp = client
        .delete(format!("{}/rules/rule_hop_test", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
}

#[tokio::test]
async fn test_sliding_window_event_triggered_execution() {
    let (base_url, _handle, state) = spawn_test_server_with_state().await;
    let client = reqwest::Client::new();

    let resp = client
        .post(format!("{}/streams", base_url))
        .json(&json!({
            "sql": "CREATE STREAM slide_stream () WITH (FORMAT=\"json\")"
        }))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    // Subscribe before the rule exists so no window output is lost.
    let mut sink_rx = state.stream_bus.subscribe("slide_sink_topic");

    // Trailing 300ms horizon, fired per event (no delay).
    let resp = client
        .post(format!("{}/rules", base_url))
        .json(&json!({
            "id": "rule_slide_test",
            "sql": "SELECT count(*) AS cnt, sum(val) AS total FROM slide_stream GROUP BY SLIDINGWINDOW(ms, 300)",
            "actions": [{"memory": {"topic": "slide_sink_topic"}}]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);

    async fn post_val(client: &reqwest::Client, base_url: &str, val: i64) {
        let resp = client
            .post(format!("{}/streams/slide_stream/data", base_url))
            .json(&json!({"val": val}))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), reqwest::StatusCode::OK);
    }

    async fn recv_output(
        rx: &mut tokio::sync::broadcast::Receiver<rekuiper_core::StreamRecord>,
    ) -> serde_json::Value {
        tokio::time::timeout(std::time::Duration::from_secs(2), rx.recv())
            .await
            .expect("timed out waiting for window output")
            .expect("slide_sink_topic closed")
            .data
            .into_iter()
            .collect::<serde_json::Map<String, serde_json::Value>>()
            .into()
    }

    // Each arrival immediately emits over its trailing horizon.
    post_val(&client, &base_url, 10).await;
    let hop1 = recv_output(&mut sink_rx).await;
    assert_eq!(hop1["cnt"], json!(1), "event 1 output: {}", hop1);
    assert_eq!(hop1["total"], json!(10), "event 1 output: {}", hop1);

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    post_val(&client, &base_url, 20).await;
    let hop2 = recv_output(&mut sink_rx).await;
    assert_eq!(hop2["cnt"], json!(2), "event 2 output: {}", hop2);
    assert_eq!(hop2["total"], json!(30), "event 2 output: {}", hop2);

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    post_val(&client, &base_url, 30).await;
    let hop3 = recv_output(&mut sink_rx).await;
    assert_eq!(hop3["cnt"], json!(3), "event 3 output: {}", hop3);
    assert_eq!(hop3["total"], json!(60), "event 3 output: {}", hop3);

    // After 350ms all three are past the horizon: a fresh event sees only itself.
    tokio::time::sleep(std::time::Duration::from_millis(350)).await;
    post_val(&client, &base_url, 40).await;
    let hop4 = recv_output(&mut sink_rx).await;
    assert_eq!(hop4["cnt"], json!(1), "event 4 output: {}", hop4);
    assert_eq!(hop4["total"], json!(40), "event 4 output: {}", hop4);

    // Metrics: four source records, four window emissions.
    let status: serde_json::Value = client
        .get(format!("{}/rules/rule_slide_test/status", base_url))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(status["sourceRecordsInTotal"], json!(4));
    assert_eq!(status["sinkRecordsOutTotal"], json!(4));

    // Delete the rule cleanly.
    let resp = client
        .delete(format!("{}/rules/rule_slide_test", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
}

#[tokio::test]
async fn test_count_window_hopping_overlap() {
    let (base_url, _handle, state) = spawn_test_server_with_state().await;
    let client = reqwest::Client::new();

    let resp = client
        .post(format!("{}/streams", base_url))
        .json(&json!({
            "sql": "CREATE STREAM count_hop_stream () WITH (FORMAT=\"json\")"
        }))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    // Subscribe before the rule exists so no window output is lost.
    let mut sink_rx = state.stream_bus.subscribe("count_hop_sink_topic");

    // Overlapping count window: 4-row windows advancing 2 rows at a time.
    let resp = client
        .post(format!("{}/rules", base_url))
        .json(&json!({
            "id": "rule_count_hop_test",
            "sql": "SELECT count(*) AS cnt, sum(val) AS total FROM count_hop_stream GROUP BY COUNTWINDOW(4, 2)",
            "actions": [{"memory": {"topic": "count_hop_sink_topic"}}]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);

    async fn post_vals(client: &reqwest::Client, base_url: &str, vals: &[i64]) {
        let payload: Vec<serde_json::Value> = vals.iter().map(|v| json!({"val": v})).collect();
        let resp = client
            .post(format!("{}/streams/count_hop_stream/data", base_url))
            .json(&payload)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), reqwest::StatusCode::OK);
    }

    async fn recv_output(
        rx: &mut tokio::sync::broadcast::Receiver<rekuiper_core::StreamRecord>,
    ) -> serde_json::Value {
        tokio::time::timeout(std::time::Duration::from_secs(2), rx.recv())
            .await
            .expect("timed out waiting for window output")
            .expect("count_hop_sink_topic closed")
            .data
            .into_iter()
            .collect::<serde_json::Map<String, serde_json::Value>>()
            .into()
    }

    async fn wait_source(client: &reqwest::Client, base_url: &str, want: u64) {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        loop {
            let status: serde_json::Value = client
                .get(format!("{}/rules/rule_count_hop_test/status", base_url))
                .send()
                .await
                .unwrap()
                .json()
                .await
                .unwrap();
            if status["sourceRecordsInTotal"].as_u64().unwrap_or(0) >= want {
                return;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "timed out waiting for {} source records",
                want
            );
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    }

    // Events 1..3 buffer without firing (3 < 4).
    post_vals(&client, &base_url, &[10, 20, 30]).await;
    wait_source(&client, &base_url, 3).await;
    assert!(
        sink_rx.try_recv().is_err(),
        "no window output expected before 4 records"
    );

    // Event 4 completes the first window: cnt = 4, total = 100.
    post_vals(&client, &base_url, &[40]).await;
    let hop1 = recv_output(&mut sink_rx).await;
    assert_eq!(hop1["cnt"], json!(4), "hop 1 output: {}", hop1);
    assert_eq!(hop1["total"], json!(100), "hop 1 output: {}", hop1);

    // Oldest two drain; 30 and 40 are retained across the hop.
    post_vals(&client, &base_url, &[50]).await;
    wait_source(&client, &base_url, 5).await;
    assert!(
        sink_rx.try_recv().is_err(),
        "no window output expected at 3 buffered records"
    );
    post_vals(&client, &base_url, &[60]).await;
    let hop2 = recv_output(&mut sink_rx).await;
    assert_eq!(hop2["cnt"], json!(4), "hop 2 output: {}", hop2);
    assert_eq!(hop2["total"], json!(180), "hop 2 output: {}", hop2);

    // Next hop slides to [50, 60, 70, 80].
    post_vals(&client, &base_url, &[70, 80]).await;
    let hop3 = recv_output(&mut sink_rx).await;
    assert_eq!(hop3["cnt"], json!(4), "hop 3 output: {}", hop3);
    assert_eq!(hop3["total"], json!(260), "hop 3 output: {}", hop3);

    // Metrics: 8 records in, 3 window emissions out.
    let status: serde_json::Value = client
        .get(format!("{}/rules/rule_count_hop_test/status", base_url))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(status["sourceRecordsInTotal"], json!(8));
    assert_eq!(status["sinkRecordsOutTotal"], json!(3));

    // Delete the rule cleanly.
    let resp = client
        .delete(format!("{}/rules/rule_count_hop_test", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
}

#[tokio::test]
async fn test_event_time_watermark_and_late_tolerance() {
    let (base_url, _handle, state) = spawn_test_server_with_state().await;
    let client = reqwest::Client::new();

    let resp = client
        .post(format!("{}/streams", base_url))
        .json(&json!({
            "sql": "CREATE STREAM et_stream () WITH (FORMAT=\"json\", TIMESTAMP=\"ts\")"
        }))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    // Subscribe before the rule exists so no window output is lost.
    let mut sink_rx = state.stream_bus.subscribe("et_sink_topic");

    let resp = client
        .post(format!("{}/rules", base_url))
        .json(&json!({
            "id": "rule_et_test",
            "sql": "SELECT count(*) AS cnt, max(ts) AS max_ts FROM et_stream GROUP BY SLIDINGWINDOW(ms, 500)",
            "actions": [{"memory": {"topic": "et_sink_topic"}}],
            "options": {"isEventTime": true, "lateTolerance": 100}
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);

    async fn post_event(client: &reqwest::Client, base_url: &str, val: i64, ts: i64) {
        let resp = client
            .post(format!("{}/streams/et_stream/data", base_url))
            .json(&json!({"val": val, "ts": ts}))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), reqwest::StatusCode::OK);
    }

    async fn recv_output(
        rx: &mut tokio::sync::broadcast::Receiver<rekuiper_core::StreamRecord>,
    ) -> serde_json::Value {
        tokio::time::timeout(std::time::Duration::from_secs(2), rx.recv())
            .await
            .expect("timed out waiting for window output")
            .expect("et_sink_topic closed")
            .data
            .into_iter()
            .collect::<serde_json::Map<String, serde_json::Value>>()
            .into()
    }

    // Event 1 (t=1000): watermark 900, output cnt=1/max_ts=1000.
    post_event(&client, &base_url, 1, 1000).await;
    let out1 = recv_output(&mut sink_rx).await;
    assert_eq!(out1["cnt"], json!(1), "output 1: {}", out1);
    assert_eq!(out1["max_ts"], json!(1000), "output 1: {}", out1);

    // Event 2 (t=950, out of order but >= W=900): accepted, buffer [950, 1000].
    post_event(&client, &base_url, 2, 950).await;
    let out2 = recv_output(&mut sink_rx).await;
    assert_eq!(out2["cnt"], json!(2), "output 2: {}", out2);
    assert_eq!(out2["max_ts"], json!(1000), "output 2: {}", out2);

    // Event 3 (t=1500): watermark 1400, horizon [1000, 1500] drops 950.
    post_event(&client, &base_url, 3, 1500).await;
    let out3 = recv_output(&mut sink_rx).await;
    assert_eq!(out3["cnt"], json!(2), "output 3: {}", out3);
    assert_eq!(out3["max_ts"], json!(1500), "output 3: {}", out3);

    // Event 4 (t=1100 < W=1400): late, dropped with no output.
    post_event(&client, &base_url, 4, 1100).await;
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(50), sink_rx.recv())
            .await
            .is_err(),
        "late event must not emit"
    );

    // Metrics prove the dropped event still counted as a source record.
    let status: serde_json::Value = client
        .get(format!("{}/rules/rule_et_test/status", base_url))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(status["sourceRecordsInTotal"], json!(4));
    assert_eq!(status["sinkRecordsOutTotal"], json!(3));

    // Delete the rule cleanly.
    let resp = client
        .delete(format!("{}/rules/rule_et_test", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
}

#[tokio::test]
async fn test_buffer_length_and_send_error_options() {
    let (base_url, _handle, state) = spawn_test_server_with_state().await;
    let client = reqwest::Client::new();

    // Shared source stream for both rules.
    let resp = client
        .post(format!("{}/streams", base_url))
        .json(&json!({
            "sql": "CREATE STREAM opt_stream () WITH (FORMAT=\"json\")"
        }))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    async fn post_error(client: &reqwest::Client, base_url: &str) {
        let resp = client
            .post(format!("{}/streams/opt_stream/data", base_url))
            .json(&json!({"error": "sensor connection reset"}))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), reqwest::StatusCode::OK);
    }

    async fn rule_status(
        client: &reqwest::Client,
        base_url: &str,
        rule: &str,
    ) -> serde_json::Value {
        client
            .get(format!("{}/rules/{}/status", base_url, rule))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap()
    }

    // sendError: true — the error record is formatted and forwarded immediately.
    let mut sink_rx_true = state.stream_bus.subscribe("sink_err_true");
    let resp = client
        .post(format!("{}/rules", base_url))
        .json(&json!({
            "id": "rule_err_true",
            "sql": "SELECT * FROM opt_stream",
            "actions": [{"memory": {"topic": "sink_err_true"}}],
            "options": {"sendError": true, "bufferLength": 50}
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);

    post_error(&client, &base_url).await;
    let out = tokio::time::timeout(std::time::Duration::from_secs(2), sink_rx_true.recv())
        .await
        .expect("sendError:true must forward the error record")
        .expect("sink_err_true closed");
    let out_json: serde_json::Value = out
        .data
        .into_iter()
        .collect::<serde_json::Map<String, serde_json::Value>>()
        .into();
    assert_eq!(
        out_json["error"],
        json!("sensor connection reset"),
        "forwarded: {}",
        out_json
    );
    assert_eq!(
        out_json["rule_id"],
        json!("rule_err_true"),
        "forwarded: {}",
        out_json
    );
    let status = rule_status(&client, &base_url, "rule_err_true").await;
    assert_eq!(status["sourceRecordsInTotal"], json!(1));
    assert_eq!(status["sinkRecordsOutTotal"], json!(1));
    assert_eq!(status["exceptionsTotal"], json!(1));

    // sendError: false (default) — counted as an exception, never forwarded.
    let mut sink_rx_false = state.stream_bus.subscribe("sink_err_false");
    let resp = client
        .post(format!("{}/rules", base_url))
        .json(&json!({
            "id": "rule_err_false",
            "sql": "SELECT * FROM opt_stream",
            "actions": [{"memory": {"topic": "sink_err_false"}}],
            "options": {"sendError": false, "bufferLength": 50}
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);

    post_error(&client, &base_url).await;
    // Wait until the rule has demonstrably processed the error record (source
    // counter), so the subsequent empty assertion is meaningful and not a race.
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            let s = rule_status(&client, &base_url, "rule_err_false").await;
            if s["sourceRecordsInTotal"] == json!(1) {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("rule_err_false never processed the error record");
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(50), sink_rx_false.recv())
            .await
            .is_err(),
        "sendError:false must not forward error records"
    );
    let status = rule_status(&client, &base_url, "rule_err_false").await;
    assert_eq!(status["sinkRecordsOutTotal"], json!(0));
    assert_eq!(status["exceptionsTotal"], json!(1));

    // Clean delete of both rules.
    for rule in ["rule_err_true", "rule_err_false"] {
        let resp = client
            .delete(format!("{}/rules/{}", base_url, rule))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), reqwest::StatusCode::OK);
    }
}

#[tokio::test]
async fn test_dynamic_metadata_functions() {
    let (base_url, _handle, _state) = spawn_test_server_with_state().await;
    let client = reqwest::Client::new();

    async fn fetch_functions(client: &reqwest::Client, base_url: &str) -> Vec<serde_json::Value> {
        let resp = client
            .get(format!("{}/metadata/functions", base_url))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), reqwest::StatusCode::OK);
        resp.json().await.unwrap()
    }

    fn find<'a>(list: &'a [serde_json::Value], name: &str) -> &'a serde_json::Value {
        list.iter()
            .find(|entry| entry["name"] == json!(name))
            .unwrap_or_else(|| panic!("function {} missing from metadata", name))
    }

    // The catalog covers every built-in function.
    let functions = fetch_functions(&client, &base_url).await;
    assert!(
        functions.len() >= 185,
        "expected >= 185 functions, got {}",
        functions.len()
    );
    for name in [
        "crc32",
        "day_of_week",
        "object_pick",
        "percentile",
        "split_value",
        "row_number",
    ] {
        find(&functions, name);
    }

    // Spot-check categories and the aggregate flag.
    assert_eq!(find(&functions, "crc32")["category"], json!("crypto"));
    assert_eq!(find(&functions, "crc32")["aggregate"], json!(false));
    assert_eq!(
        find(&functions, "day_of_week")["category"],
        json!("datetime")
    );
    assert_eq!(find(&functions, "object_pick")["category"], json!("array"));
    assert_eq!(
        find(&functions, "percentile")["category"],
        json!("aggregate")
    );
    assert_eq!(find(&functions, "percentile")["aggregate"], json!(true));
    assert_eq!(
        find(&functions, "row_number")["category"],
        json!("aggregate")
    );
    assert_eq!(find(&functions, "row_number")["aggregate"], json!(true));

    // Registered plugin functions extend the catalog dynamically.
    let resp = client
        .post(format!("{}/plugins/functions", base_url))
        .json(&json!({
            "name": "meta_test_plugin",
            "functions": ["custom_fn1", "custom_fn2"]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);

    let functions = fetch_functions(&client, &base_url).await;
    let custom = find(&functions, "custom_fn1");
    assert_eq!(custom["category"], json!("plugin"));
    find(&functions, "custom_fn2");

    // Deleting the plugin removes its functions again.
    let resp = client
        .delete(format!("{}/plugins/functions/meta_test_plugin", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    let functions = fetch_functions(&client, &base_url).await;
    assert!(
        !functions
            .iter()
            .any(|entry| entry["name"] == json!("custom_fn1")),
        "custom_fn1 must disappear after plugin deletion"
    );
    assert!(
        !functions
            .iter()
            .any(|entry| entry["name"] == json!("custom_fn2")),
        "custom_fn2 must disappear after plugin deletion"
    );
}

#[tokio::test]
async fn test_rule_schema_introspection() {
    let (base_url, _handle, _state) = spawn_test_server_with_state().await;
    let client = reqwest::Client::new();

    let resp = client
        .post(format!("{}/streams", base_url))
        .json(&json!({
            "sql": "CREATE STREAM demo () WITH (FORMAT=\"json\")"
        }))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    async fn create_rule(client: &reqwest::Client, base_url: &str, id: &str, sql: &str) {
        let resp = client
            .post(format!("{}/rules", base_url))
            .json(&json!({
                "id": id,
                "sql": sql,
                "actions": [{"memory": {"topic": "schema_sink"}}]
            }))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), reqwest::StatusCode::CREATED);
    }

    async fn fetch_schema(
        client: &reqwest::Client,
        base_url: &str,
        id: &str,
    ) -> (reqwest::StatusCode, serde_json::Value) {
        let resp = client
            .get(format!("{}/rules/{}/schema", base_url, id))
            .send()
            .await
            .unwrap();
        let status = resp.status();
        // Error responses are plain text; only success carries a JSON schema.
        let body = resp.json().await.unwrap_or(serde_json::Value::Null);
        (status, body)
    }

    // Dynamic identifier stays "any"; bigint literal widens `b + 1` to float.
    create_rule(
        &client,
        &base_url,
        "rule_schema_test",
        "SELECT a, b + 1 AS c FROM demo",
    )
    .await;
    let (status, schema) = fetch_schema(&client, &base_url, "rule_schema_test").await;
    assert_eq!(status, reqwest::StatusCode::OK);
    assert_eq!(schema, json!({"a": "any", "c": "float"}));

    // Typed function calls resolve to their static return types.
    create_rule(
        &client,
        &base_url,
        "rule_typed_test",
        "SELECT concat(name, '!') AS greeting, count(*) AS cnt, isnull(val) AS is_missing FROM demo",
    )
    .await;
    let (status, schema) = fetch_schema(&client, &base_url, "rule_typed_test").await;
    assert_eq!(status, reqwest::StatusCode::OK);
    assert_eq!(schema["greeting"], json!("string"), "schema: {}", schema);
    assert_eq!(schema["cnt"], json!("bigint"), "schema: {}", schema);
    assert_eq!(schema["is_missing"], json!("boolean"), "schema: {}", schema);

    // Unknown rule ids 404.
    let (status, _) = fetch_schema(&client, &base_url, "non_existent").await;
    assert_eq!(status, reqwest::StatusCode::NOT_FOUND);

    // Clean up rules and stream.
    for rule in ["rule_schema_test", "rule_typed_test"] {
        let resp = client
            .delete(format!("{}/rules/{}", base_url, rule))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), reqwest::StatusCode::OK);
    }
    let resp = client
        .delete(format!("{}/streams/demo", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
}

#[tokio::test]
async fn test_process_cpu_and_memory_metrics() {
    let (base_url, _handle, _state) = spawn_test_server_with_state().await;
    let client = reqwest::Client::new();

    let resp = client
        .post(format!("{}/streams", base_url))
        .json(&json!({
            "sql": "CREATE STREAM demo () WITH (FORMAT=\"json\")"
        }))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    let resp = client
        .post(format!("{}/rules", base_url))
        .json(&json!({
            "id": "metric_test_rule",
            "sql": "SELECT * FROM demo",
            "actions": [{"memory": {"topic": "metric_sink"}}]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);

    // Per-rule CPU/memory reflects the live process.
    let resp = client
        .get(format!("{}/rules/metric_test_rule/cpu", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let cpu: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(cpu["rule_id"], json!("metric_test_rule"));
    assert!(
        cpu["memory"].as_u64().unwrap_or(0) > 0,
        "memory must be positive: {}",
        cpu
    );
    assert!(
        cpu["cpu"].as_f64().unwrap_or(-1.0) >= 0.0,
        "cpu must be non-negative: {}",
        cpu
    );

    // Aggregate usage maps every known rule id.
    let resp = client
        .get(format!("{}/rules/usage/cpu", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let usage: serde_json::Value = resp.json().await.unwrap();
    assert!(usage.is_object(), "usage must be an object: {}", usage);
    assert!(
        usage.get("metric_test_rule").is_some(),
        "usage must contain the rule: {}",
        usage
    );

    // Metrics dump carries live process and system memory.
    let resp = client
        .get(format!("{}/metrics/dump", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let dump: serde_json::Value = resp.json().await.unwrap();
    assert!(
        dump["metrics"]["memory"].as_u64().unwrap_or(0) > 0,
        "dump memory must be positive: {}",
        dump
    );

    // Unknown rule ids 404.
    let resp = client
        .get(format!("{}/rules/non_existent/cpu", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::NOT_FOUND);

    // Clean up rule and stream.
    let resp = client
        .delete(format!("{}/rules/metric_test_rule", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let resp = client
        .delete(format!("{}/streams/demo", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
}

#[tokio::test]
async fn test_metadata_source_and_sink_documents() {
    let (base_url, _handle) = spawn_test_server().await;
    let client = reqwest::Client::new();

    // 1. List source metadata: contains expanded set of connectors.
    let resp = client
        .get(format!("{}/metadata/sources", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let sources: Vec<serde_json::Value> = resp.json().await.unwrap();
    let source_names: Vec<&str> = sources
        .iter()
        .filter_map(|s| s.get("name").and_then(|v| v.as_str()))
        .collect();
    for expected in &[
        "mqtt",
        "file",
        "http",
        "httppull",
        "redis",
        "simulator",
        "memory",
    ] {
        assert!(
            source_names.contains(expected),
            "sources must contain '{}': {:?}",
            expected,
            source_names
        );
    }

    // 2. List sink metadata: contains expanded set of connectors.
    let resp = client
        .get(format!("{}/metadata/sinks", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let sinks: Vec<serde_json::Value> = resp.json().await.unwrap();
    let sink_names: Vec<&str> = sinks
        .iter()
        .filter_map(|s| s.get("name").and_then(|v| v.as_str()))
        .collect();
    for expected in &["mqtt", "file", "log", "memory", "redis", "rest"] {
        assert!(
            sink_names.contains(expected),
            "sinks must contain '{}': {:?}",
            expected,
            sink_names
        );
    }

    // 3. Disk-backed source metadata for mqtt: has about and properties.
    let resp = client
        .get(format!("{}/metadata/sources/mqtt", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let mqtt_src: serde_json::Value = resp.json().await.unwrap();
    assert!(
        mqtt_src.get("about").is_some(),
        "mqtt source metadata must have 'about': {}",
        mqtt_src
    );
    assert!(
        mqtt_src.get("properties").is_some() || mqtt_src.get("dataSource").is_some(),
        "mqtt source metadata must have properties or dataSource: {}",
        mqtt_src
    );

    // 4. Disk-backed source metadata for file.
    let resp = client
        .get(format!("{}/metadata/sources/file", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let file_src: serde_json::Value = resp.json().await.unwrap();
    assert!(
        file_src.get("about").is_some(),
        "file source metadata must have 'about': {}",
        file_src
    );

    // 5. Disk-backed sink metadata for mqtt.
    let resp = client
        .get(format!("{}/metadata/sinks/mqtt", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let mqtt_sink: serde_json::Value = resp.json().await.unwrap();
    assert!(
        mqtt_sink.get("about").is_some(),
        "mqtt sink metadata must have 'about': {}",
        mqtt_sink
    );

    // 6. Source YAML for mqtt returns real configuration from disk.
    let resp = client
        .get(format!("{}/metadata/sources/yaml/mqtt", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let mqtt_yaml: serde_json::Value = resp.json().await.unwrap();
    let yaml_str = mqtt_yaml["yaml"].as_str().unwrap_or("");
    assert!(
        yaml_str.contains("server:"),
        "mqtt source YAML must contain 'server:': {}",
        yaml_str
    );

    // 7. Source YAML for file returns real configuration from disk.
    let resp = client
        .get(format!("{}/metadata/sources/yaml/file", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let file_yaml: serde_json::Value = resp.json().await.unwrap();
    let yaml_str = file_yaml["yaml"].as_str().unwrap_or("");
    assert!(
        yaml_str.contains("fileType:"),
        "file source YAML must contain 'fileType:': {}",
        yaml_str
    );

    // 8. Invalid resource names are rejected with 400 Bad Request.
    let resp = client
        .get(format!("{}/metadata/sources/invalid%20name", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn test_connection_metadata_and_resource_discovery() {
    let (base_url, _handle) = spawn_test_server().await;
    let client = reqwest::Client::new();

    // 1. Initially connections are empty.
    let resp = client
        .get(format!("{}/metadata/connections", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let conns: Vec<serde_json::Value> = resp.json().await.unwrap();
    assert!(conns.is_empty(), "initially empty: {:?}", conns);

    // 2. Create connection.
    let resp = client
        .post(format!("{}/connections", base_url))
        .json(&serde_json::json!({
            "id": "test_conn_1",
            "type": "mqtt",
            "server": "tcp://127.0.0.1:1883"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);

    // 3. /metadata/connections reflects the new connection.
    let resp = client
        .get(format!("{}/metadata/connections", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let conns: Vec<serde_json::Value> = resp.json().await.unwrap();
    assert_eq!(conns.len(), 1);
    assert_eq!(conns[0]["id"], "test_conn_1");

    // 4. /metadata/connections/:name returns connection details.
    let resp = client
        .get(format!("{}/metadata/connections/test_conn_1", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let conn_meta: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(conn_meta["id"], "test_conn_1");
    assert_eq!(conn_meta["type"], "mqtt");

    // 5. /metadata/resources reflects connection as a live resource.
    let resp = client
        .get(format!("{}/metadata/resources", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let resources: Vec<serde_json::Value> = resp.json().await.unwrap();
    assert_eq!(resources.len(), 1);
    assert_eq!(resources[0]["id"], "test_conn_1");
    assert_eq!(resources[0]["type"], "mqtt");

    // 6. /metadata/connections/yaml/:name returns connection YAML from disk.
    let resp = client
        .get(format!("{}/metadata/connections/yaml/mqtt", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let yaml_resp: serde_json::Value = resp.json().await.unwrap();
    assert!(yaml_resp.get("yaml").is_some());

    // 7. Delete connection.
    let resp = client
        .delete(format!("{}/connections/test_conn_1", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    // 8. /metadata/connections is empty again.
    let resp = client
        .get(format!("{}/metadata/connections", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let conns: Vec<serde_json::Value> = resp.json().await.unwrap();
    assert!(conns.is_empty());
}

#[tokio::test]
async fn test_confkeys_persistence_and_registration() {
    let (base_url, _handle) = spawn_test_server().await;
    let client = reqwest::Client::new();

    // 1. Source confKeys CRUD
    let resp = client
        .put(format!(
            "{}/metadata/sources/mqtt/confKeys/custom_conf",
            base_url
        ))
        .json(&serde_json::json!({"server": "tcp://broker:1883"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    let resp = client
        .get(format!(
            "{}/metadata/sources/mqtt/confKeys/custom_conf",
            base_url
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let cfg: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(cfg["server"], "tcp://broker:1883");

    let resp = client
        .delete(format!(
            "{}/metadata/sources/mqtt/confKeys/custom_conf",
            base_url
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    let resp = client
        .get(format!(
            "{}/metadata/sources/mqtt/confKeys/custom_conf",
            base_url
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::NOT_FOUND);

    // 2. Sink confKeys CRUD
    let resp = client
        .put(format!(
            "{}/metadata/sinks/mqtt/confKeys/sink_conf",
            base_url
        ))
        .json(&serde_json::json!({"topic": "out/events"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    let resp = client
        .get(format!(
            "{}/metadata/sinks/mqtt/confKeys/sink_conf",
            base_url
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let cfg: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(cfg["topic"], "out/events");

    let resp = client
        .delete(format!(
            "{}/metadata/sinks/mqtt/confKeys/sink_conf",
            base_url
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    let resp = client
        .get(format!(
            "{}/metadata/sinks/mqtt/confKeys/sink_conf",
            base_url
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::NOT_FOUND);

    // 3. Connection confKeys CRUD
    let resp = client
        .put(format!(
            "{}/metadata/connections/mqtt/confKeys/conn_conf",
            base_url
        ))
        .json(&serde_json::json!({"server": "tcp://conn:1883"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    let resp = client
        .get(format!(
            "{}/metadata/connections/mqtt/confKeys/conn_conf",
            base_url
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let cfg: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(cfg["server"], "tcp://conn:1883");

    let resp = client
        .delete(format!(
            "{}/metadata/connections/mqtt/confKeys/conn_conf",
            base_url
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    let resp = client
        .get(format!(
            "{}/metadata/connections/mqtt/confKeys/conn_conf",
            base_url
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::NOT_FOUND);

    // 4. Source/sink/lookup connection bindings
    let resp = client
        .post(format!("{}/metadata/sources/connection/mqtt", base_url))
        .json(&serde_json::json!({"id": "mqtt_reg_src", "server": "tcp://127.0.0.1:1883"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    let resp = client
        .get(format!("{}/metadata/connections/mqtt_reg_src", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
}

#[tokio::test]
async fn test_rule_tags_lifecycle_and_matching() {
    let (base_url, _handle) = spawn_test_server().await;
    let client = reqwest::Client::new();

    // 1. Create a stream for rules to use
    let resp = client
        .post(format!("{}/streams", base_url))
        .json(&serde_json::json!({
            "sql": "create stream stream_tags () WITH (FORMAT=\"JSON\");"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);

    // 2. Create rule 1 with initial tags ["edge", "production"]
    let resp = client
        .post(format!("{}/rules", base_url))
        .json(&serde_json::json!({
            "id": "rule_tag_1",
            "sql": "SELECT * FROM stream_tags",
            "actions": [{"log": {}}],
            "tags": ["edge", "production"]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);

    // 3. Create rule 2 with no initial tags
    let resp = client
        .post(format!("{}/rules", base_url))
        .json(&serde_json::json!({
            "id": "rule_tag_2",
            "sql": "SELECT * FROM stream_tags",
            "actions": [{"log": {}}]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);

    // 4. Verify rule 1 tags in GET /rules/:id
    let resp = client
        .get(format!("{}/rules/rule_tag_1", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let r1: serde_json::Value = resp.json().await.unwrap();
    let r1_tags: Vec<String> = serde_json::from_value(r1["tags"].clone()).unwrap();
    assert_eq!(r1_tags, vec!["edge", "production"]);

    // 5. Match tags via query parameters
    let resp = client
        .get(format!("{}/rules/tags/match?tags=edge", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let matched: Vec<String> = resp.json().await.unwrap();
    assert_eq!(matched, vec!["rule_tag_1"]);

    let resp = client
        .get(format!(
            "{}/rules/tags/match?tags=edge,production",
            base_url
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let matched: Vec<String> = resp.json().await.unwrap();
    assert_eq!(matched, vec!["rule_tag_1"]);

    let resp = client
        .get(format!("{}/rules/tags/match?tags=staging", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let matched: Vec<String> = resp.json().await.unwrap();
    assert!(matched.is_empty());

    // 6. PATCH /rules/rule_tag_2/tags to add ["staging", "production"]
    let resp = client
        .patch(format!("{}/rules/rule_tag_2/tags", base_url))
        .json(&serde_json::json!({"tags": ["staging", "production"]}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    // 7. Query match for "production" should now match both rules
    let resp = client
        .get(format!("{}/rules/tags/match?tags=production", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let matched: Vec<String> = resp.json().await.unwrap();
    assert_eq!(matched, vec!["rule_tag_1", "rule_tag_2"]);

    // 8. Test POST /rules/tags/match with body {"keys": ["staging"]}
    let resp = client
        .post(format!("{}/rules/tags/match", base_url))
        .json(&serde_json::json!({"keys": ["staging"]}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let matched: Vec<String> = resp.json().await.unwrap();
    assert_eq!(matched, vec!["rule_tag_2"]);

    // 9. PUT /rules/rule_tag_1/tags to replace tags with ["canary"]
    let resp = client
        .put(format!("{}/rules/rule_tag_1/tags", base_url))
        .json(&serde_json::json!({"tags": ["canary"]}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    let resp = client
        .get(format!("{}/rules/tags/match?tags=canary", base_url))
        .send()
        .await
        .unwrap();
    let matched: Vec<String> = resp.json().await.unwrap();
    assert_eq!(matched, vec!["rule_tag_1"]);

    let resp = client
        .get(format!("{}/rules/tags/match?tags=edge", base_url))
        .send()
        .await
        .unwrap();
    let matched: Vec<String> = resp.json().await.unwrap();
    assert!(matched.is_empty());

    // 10. DELETE /rules/rule_tag_2/tags with {"keys": ["staging"]} removes staging only
    let resp = client
        .delete(format!("{}/rules/rule_tag_2/tags", base_url))
        .json(&serde_json::json!({"keys": ["staging"]}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    let resp = client
        .get(format!("{}/rules/tags/match?tags=production", base_url))
        .send()
        .await
        .unwrap();
    let matched: Vec<String> = resp.json().await.unwrap();
    assert_eq!(matched, vec!["rule_tag_2"]);

    // 11. DELETE /rules/rule_tag_2/tags with {} removes all tags
    let resp = client
        .delete(format!("{}/rules/rule_tag_2/tags", base_url))
        .json(&serde_json::json!({}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    let resp = client
        .get(format!("{}/rules/tags/match?tags=production", base_url))
        .send()
        .await
        .unwrap();
    let matched: Vec<String> = resp.json().await.unwrap();
    assert!(matched.is_empty());

    // 12. Cleanup rules & stream
    let _ = client
        .delete(format!("{}/rules/rule_tag_1", base_url))
        .send()
        .await;
    let _ = client
        .delete(format!("{}/rules/rule_tag_2", base_url))
        .send()
        .await;
    let _ = client
        .delete(format!("{}/streams/stream_tags", base_url))
        .send()
        .await;
}

#[tokio::test]
async fn test_rule_execution_trace_buffer() {
    let (base_url, _handle) = spawn_test_server().await;
    let client = reqwest::Client::new();

    // 1. Create stream
    let resp = client
        .post(format!("{}/streams", base_url))
        .json(&serde_json::json!({
            "sql": "create stream stream_trace () WITH (FORMAT=\"JSON\");"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);

    // 2. Create rule
    let resp = client
        .post(format!("{}/rules", base_url))
        .json(&serde_json::json!({
            "id": "rule_trace_1",
            "sql": "SELECT * FROM stream_trace",
            "actions": [{"log": {}}]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);

    // 3. Initial trace IDs list should be empty
    let resp = client
        .get(format!("{}/trace/rule/rule_trace_1", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let trace_ids: Vec<String> = resp.json().await.unwrap();
    assert!(trace_ids.is_empty());

    // 4. Non-existent rule trace start/stop should return 404
    let resp = client
        .post(format!("{}/rules/nonexistent/trace/start", base_url))
        .json(&serde_json::json!({"strategy": "always"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::NOT_FOUND);

    let resp = client
        .post(format!("{}/rules/nonexistent/trace/stop", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::NOT_FOUND);

    // 5. Start trace on rule_trace_1
    let resp = client
        .post(format!("{}/rules/rule_trace_1/trace/start", base_url))
        .json(&serde_json::json!({"strategy": "always"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    // 6. Push 3 records to stream_trace
    for i in 1..=3 {
        let resp = client
            .post(format!("{}/streams/stream_trace/data", base_url))
            .json(&serde_json::json!({
                "temperature": 20.0 + (i as f64),
                "humidity": 50 + i
            }))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), reqwest::StatusCode::OK);
    }

    tokio::time::sleep(std::time::Duration::from_millis(150)).await;

    // 7. Verify 3 trace IDs captured
    let resp = client
        .get(format!("{}/trace/rule/rule_trace_1", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let trace_ids: Vec<String> = resp.json().await.unwrap();
    assert_eq!(trace_ids.len(), 3);

    // 8. Test limit query param
    let resp = client
        .get(format!("{}/trace/rule/rule_trace_1?limit=2", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let limited_ids: Vec<String> = resp.json().await.unwrap();
    assert_eq!(limited_ids.len(), 2);

    // 9. Inspect a trace by ID
    let sample_id = &trace_ids[0];
    let resp = client
        .get(format!("{}/trace/{}", base_url, sample_id))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let span: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(span["Name"], "rule_trace_1");
    assert_eq!(span["TraceID"], sample_id.as_str());
    assert_eq!(span["ParentSpanID"], "0000000000000000");
    assert!(span["ChildSpan"].is_array());
    let child_spans = span["ChildSpan"].as_array().unwrap();
    assert!(!child_spans.is_empty());
    assert_eq!(child_spans[0]["Name"], "rule_trace_1_decoder");

    // 10. Stop tracing
    let resp = client
        .post(format!("{}/rules/rule_trace_1/trace/stop", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    // 11. Push a 4th record
    let resp = client
        .post(format!("{}/streams/stream_trace/data", base_url))
        .json(&serde_json::json!({
            "temperature": 35.0,
            "humidity": 75
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    tokio::time::sleep(std::time::Duration::from_millis(150)).await;

    // 12. Verify count is still 3 (tracing was stopped)
    let resp = client
        .get(format!("{}/trace/rule/rule_trace_1", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let trace_ids_after: Vec<String> = resp.json().await.unwrap();
    assert_eq!(trace_ids_after.len(), 3);

    // 13. Test /tracer configuration endpoint
    let resp = client
        .post(format!("{}/tracer", base_url))
        .json(&serde_json::json!({
            "service_name": "rekuiper",
            "action": "start",
            "collector_url": "http://localhost:4318"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    // 14. Cleanup
    let _ = client
        .delete(format!("{}/rules/rule_trace_1", base_url))
        .send()
        .await;
    let _ = client
        .delete(format!("{}/streams/stream_trace", base_url))
        .send()
        .await;
}

#[tokio::test]
async fn test_async_task_lifecycle_and_cancellation() {
    let (base_url, _handle) = spawn_test_server().await;
    let client = reqwest::Client::new();

    // 1. Unknown task returns 404
    let resp = client
        .get(format!("{}/async/task/nonexistent_task", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::NOT_FOUND);

    // 2. Pre-seeded task_1 returns 200
    let resp = client
        .get(format!("{}/async/task/task_1", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let t1: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(t1["id"], "task_1");
    assert_eq!(t1["status"], "completed");

    // 3. Spawn real background async data import
    let resp = client
        .post(format!("{}/async/data/import", base_url))
        .json(&serde_json::json!({
            "streams": [{
                "name": "async_stream_1",
                "sql": "create stream async_stream_1 () WITH (FORMAT=\"JSON\");",
                "options": {}
            }]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let import_resp: serde_json::Value = resp.json().await.unwrap();
    let task_id = import_resp["id"].as_str().unwrap().to_string();
    assert!(task_id.starts_with("dataImport-"));
    assert_eq!(import_resp["status"], "running");

    // 4. Poll status until completed (max 2 seconds)
    let mut completed = false;
    for _ in 0..20 {
        let resp = client
            .get(format!("{}/async/task/{}", base_url, task_id))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), reqwest::StatusCode::OK);
        let status_val: serde_json::Value = resp.json().await.unwrap();
        if status_val["status"] == "completed" {
            completed = true;
            assert!(status_val["createdTimestamp"].as_i64().unwrap() > 0);
            assert!(status_val["updatedTimestamp"].as_i64().unwrap() > 0);
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(30)).await;
    }
    assert!(completed, "Async import task should have completed");

    // 5. Verify that the background import actually created the stream
    let resp = client
        .get(format!("{}/streams/async_stream_1", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    // 6. Test task cancellation on a new task
    let resp = client
        .post(format!("{}/async/data/import", base_url))
        .json(&serde_json::json!({
            "streams": []
        }))
        .send()
        .await
        .unwrap();
    let task_2_resp: serde_json::Value = resp.json().await.unwrap();
    let task_2_id = task_2_resp["id"].as_str().unwrap();

    let resp = client
        .post(format!("{}/async/task/{}/cancel", base_url, task_2_id))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let cancel_resp: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(cancel_resp["status"], "cancelled");

    let resp = client
        .get(format!("{}/async/task/{}", base_url, task_2_id))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let task_2_status: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(task_2_status["status"], "cancelled");

    // 7. Cleanup
    let _ = client
        .delete(format!("{}/streams/async_stream_1", base_url))
        .send()
        .await;
}

#[tokio::test]
async fn test_batch_request_pipeline() {
    let (base_url, _handle) = spawn_test_server().await;
    let client = reqwest::Client::new();

    // 1. Empty batch request returns empty array
    let resp = client
        .post(format!("{}/batch/req", base_url))
        .json(&serde_json::json!([]))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let items: Vec<serde_json::Value> = resp.json().await.unwrap();
    assert!(items.is_empty());

    // 2. Batch request creating a stream, checking it, and pinging
    let batch_payload = serde_json::json!([
        {
            "method": "POST",
            "path": "/streams",
            "body": "{\"sql\":\"CREATE STREAM demobatch () WITH (DATASOURCE=\\\"/data1\\\", TYPE=\\\"websocket\\\")\"}"
        },
        {
            "method": "GET",
            "path": "/streams/demobatch"
        },
        {
            "method": "GET",
            "path": "/ping"
        }
    ]);

    let resp = client
        .post(format!("{}/batch/req", base_url))
        .json(&batch_payload)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let items: Vec<serde_json::Value> = resp.json().await.unwrap();
    assert_eq!(items.len(), 3);

    // Stream creation response
    assert_eq!(items[0]["code"], 201);
    assert!(items[0]["response"]
        .as_str()
        .unwrap()
        .contains("Stream demobatch is created"));

    // Stream get response
    assert_eq!(items[1]["code"], 200);
    assert!(items[1]["response"].as_str().unwrap().contains("demobatch"));

    // Ping response
    assert_eq!(items[2]["code"], 200);
    assert_eq!(items[2]["response"].as_str().unwrap(), "pong");

    // 3. Verify server state directly
    let resp = client
        .get(format!("{}/streams/demobatch", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    // 4. Batch request with object body (creating a rule) and error cases
    let batch_2 = serde_json::json!([
        {
            "action": "POST",
            "url": "/rules",
            "payload": {
                "id": "rule_batch_1",
                "sql": "SELECT * FROM demobatch",
                "actions": [{"log": {}}]
            }
        },
        {
            "method": "GET",
            "path": "/rules/rule_batch_1"
        },
        {
            "method": "GET",
            "path": "/streams/non_existent_stream"
        },
        {
            "method": "POST",
            "path": "/batch/req",
            "body": "[]"
        },
        {
            "method": "INVALID_METHOD",
            "path": "/ping"
        }
    ]);

    let resp = client
        .post(format!("{}/batch/req", base_url))
        .json(&batch_2)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let items: Vec<serde_json::Value> = resp.json().await.unwrap();
    assert_eq!(items.len(), 5);

    // Rule created
    assert_eq!(items[0]["code"], 201);
    assert!(items[0]["response"]
        .as_str()
        .unwrap()
        .contains("rule_batch_1 was created"));

    // Rule fetched
    assert_eq!(items[1]["code"], 200);
    assert!(items[1]["response"]
        .as_str()
        .unwrap()
        .contains("rule_batch_1"));

    // Not found stream
    assert_eq!(items[2]["code"], 404);
    assert!(items[2]["error"].as_str().unwrap().contains("not found"));

    // Nested batch request rejected
    assert_eq!(items[3]["code"], 400);
    assert!(items[3]["error"]
        .as_str()
        .unwrap()
        .contains("nested batch requests are not supported"));

    // Invalid method rejected
    assert_eq!(items[4]["code"], 400);
    assert!(items[4]["error"]
        .as_str()
        .unwrap()
        .contains("unsupported HTTP method"));

    // 5. Cleanup
    let _ = client
        .delete(format!("{}/rules/rule_batch_1", base_url))
        .send()
        .await;
    let _ = client
        .delete(format!("{}/streams/demobatch", base_url))
        .send()
        .await;
}

#[tokio::test]
async fn test_source_and_sink_plugin_registries() {
    let (base_url, _handle) = spawn_test_server().await;
    let client = reqwest::Client::new();

    // 1. Initial state: no native plugins installed
    for path in ["/plugins/sources", "/plugins/sinks"] {
        let resp = client
            .get(format!("{}{}", base_url, path))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), reqwest::StatusCode::OK);
        let items: Vec<serde_json::Value> = resp.json().await.unwrap();
        assert!(items.is_empty());
    }

    // 2. Built-in introspection returns 200 OK
    let resp = client
        .get(format!("{}/plugins/sources/mqtt", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let mqtt_src: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(mqtt_src["name"], "mqtt");
    assert_eq!(mqtt_src["plugin_type"], "source");

    let resp = client
        .get(format!("{}/plugins/sinks/mqtt", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let mqtt_snk: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(mqtt_snk["name"], "mqtt");
    assert_eq!(mqtt_snk["plugin_type"], "sink");

    // Non-existent returns 404
    let resp = client
        .get(format!("{}/plugins/sources/non_existent_src", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::NOT_FOUND);

    // 3. Source plugin CRUD lifecycle
    let resp = client
        .post(format!("{}/plugins/sources", base_url))
        .json(&serde_json::json!({
            "name": "custom_src",
            "file": "file:///tmp/custom_src.zip",
            "description": "Initial Custom Source"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);

    let resp = client
        .get(format!("{}/plugins/sources", base_url))
        .send()
        .await
        .unwrap();
    let sources: Vec<serde_json::Value> = resp.json().await.unwrap();
    assert_eq!(sources.len(), 1);
    assert_eq!(sources[0]["name"], "custom_src");
    assert_eq!(sources[0]["plugin_type"], "source");

    let resp = client
        .get(format!("{}/plugins/sources/custom_src", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let src_info: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(src_info["description"], "Initial Custom Source");

    // Update source plugin
    let resp = client
        .put(format!("{}/plugins/sources/custom_src", base_url))
        .json(&serde_json::json!({
            "description": "Updated Custom Source"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    let resp = client
        .get(format!("{}/plugins/sources/custom_src", base_url))
        .send()
        .await
        .unwrap();
    let updated_src: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(updated_src["description"], "Updated Custom Source");

    // Delete source plugin
    let resp = client
        .delete(format!("{}/plugins/sources/custom_src", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    let resp = client
        .get(format!("{}/plugins/sources", base_url))
        .send()
        .await
        .unwrap();
    let sources_after: Vec<serde_json::Value> = resp.json().await.unwrap();
    assert!(sources_after.is_empty());

    // 4. Sink plugin CRUD lifecycle
    let resp = client
        .post(format!("{}/plugins/sinks", base_url))
        .json(&serde_json::json!({
            "name": "custom_snk",
            "file": "file:///tmp/custom_snk.zip",
            "description": "Custom Sink"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);

    let resp = client
        .get(format!("{}/plugins/sinks", base_url))
        .send()
        .await
        .unwrap();
    let sinks: Vec<serde_json::Value> = resp.json().await.unwrap();
    assert_eq!(sinks.len(), 1);
    assert_eq!(sinks[0]["name"], "custom_snk");
    assert_eq!(sinks[0]["plugin_type"], "sink");

    let resp = client
        .delete(format!("{}/plugins/sinks/custom_snk", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    let resp = client
        .get(format!("{}/plugins/sinks", base_url))
        .send()
        .await
        .unwrap();
    let sinks_after: Vec<serde_json::Value> = resp.json().await.unwrap();
    assert!(sinks_after.is_empty());

    // 5. Prebuild endpoints return 200 OK
    for path in [
        "/plugins/sources/prebuild",
        "/plugins/sinks/prebuild",
        "/plugins/functions/prebuild",
    ] {
        let resp = client
            .get(format!("{}{}", base_url, path))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), reqwest::StatusCode::OK, "GET {}", path);
    }
}

#[tokio::test]
async fn test_portable_plugin_process_lifecycle() {
    let (base_url, _handle) = spawn_test_server().await;
    let client = reqwest::Client::new();

    // 1. Initial portable plugins list contains pre-seeded pyfunc
    let resp = client
        .get(format!("{}/plugins/portables", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let names: Vec<String> = resp.json().await.unwrap();
    assert!(names.contains(&"pyfunc".to_string()));

    // 2. Query pre-seeded pyfunc metadata and status
    let resp = client
        .get(format!("{}/plugins/portables/pyfunc", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let pyfunc_info: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(pyfunc_info["name"], "pyfunc");
    assert_eq!(pyfunc_info["language"], "python");

    let resp = client
        .get(format!("{}/plugins/portables/pyfunc/status", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let pyfunc_status: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(pyfunc_status["status"], "running");

    // 3. Install a new portable plugin
    let resp = client
        .post(format!("{}/plugins/portables", base_url))
        .json(&serde_json::json!({
            "name": "mirror",
            "file": "file:///var/plugins/portables/mirror.zip"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);

    let resp = client
        .get(format!("{}/plugins/portables", base_url))
        .send()
        .await
        .unwrap();
    let names: Vec<String> = resp.json().await.unwrap();
    assert!(names.contains(&"mirror".to_string()));

    let resp = client
        .get(format!("{}/plugins/portables/mirror", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let mirror_info: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(mirror_info["name"], "mirror");

    let resp = client
        .get(format!("{}/plugins/portables/mirror/status", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let mirror_status: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(mirror_status["status"], "running");

    // 4. Update portable plugin
    let resp = client
        .put(format!("{}/plugins/portables/mirror", base_url))
        .json(&serde_json::json!({
            "version": "2.0.0",
            "executable": "custom_mirror.py"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    let resp = client
        .get(format!("{}/plugins/portables/mirror", base_url))
        .send()
        .await
        .unwrap();
    let updated_mirror: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(updated_mirror["version"], "2.0.0");
    assert_eq!(updated_mirror["executable"], "custom_mirror.py");

    // 5. Delete portable plugin
    let resp = client
        .delete(format!("{}/plugins/portables/mirror", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    let resp = client
        .get(format!("{}/plugins/portables/mirror", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::NOT_FOUND);

    let resp = client
        .get(format!("{}/plugins/portables/mirror/status", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::NOT_FOUND);

    let resp = client
        .get(format!("{}/plugins/portables", base_url))
        .send()
        .await
        .unwrap();
    let names_after: Vec<String> = resp.json().await.unwrap();
    assert!(!names_after.contains(&"mirror".to_string()));
}
