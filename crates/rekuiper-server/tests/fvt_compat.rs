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
        assert_eq!(resp.status(), reqwest::StatusCode::OK, "GET {}", path);
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
