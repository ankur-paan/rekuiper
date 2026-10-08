use anyhow::Result;
use rekuiper_conf::KuiperConfig;
use rekuiper_core::{RuleManager, StreamBus, StreamManager, TableManager};
use rekuiper_server::routes::{create_router, load_config_maps, restore_running_rules, AppState};
use serde_json::json;
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
            "test_issue32".to_string(),
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
async fn test_issue32_prometheus_metrics_32_families() -> Result<()> {
    let server = TestServer::start().await;
    let client = reqwest::Client::new();

    // 1. Create stream
    let resp = client
        .post(format!("{}/streams", server.rest_url))
        .json(&json!({
            "sql": "CREATE STREAM demo (id bigint, name string) WITH (FORMAT=\"json\", DATASOURCE=\"demo\")"
        }))
        .send()
        .await?;
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);

    // 2. Create rule with multi-sink
    let resp = client
        .post(format!("{}/rules", server.rest_url))
        .json(&json!({
            "id": "rule_prom_32",
            "sql": "SELECT id, name FROM demo WHERE id > 0",
            "actions": [
                { "log": {} },
                { "mqtt": { "server": "tcp://127.0.0.1:1883", "topic": "prom/out" } }
            ]
        }))
        .send()
        .await?;
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);

    // 3. Scrape Prometheus endpoint
    let resp = client
        .get(format!("{}/metrics", server.rest_url))
        .send()
        .await?;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let body = resp.text().await?;

    // 4. Validate all 32 metric families
    let families = [
        // Rule level
        "kuiper_rule_count",
        "kuiper_rule_status",
        "kuiper_conn_status_gauge",
        // Source metrics (8)
        "kuiper_source_records_in_total",
        "kuiper_source_records_out_total",
        "kuiper_source_messages_processed_total",
        "kuiper_source_exceptions_total",
        "kuiper_source_buffer_length",
        "kuiper_source_connection_status",
        "kuiper_source_process_latency_us",
        "kuiper_source_process_latency_us_hist_bucket",
        "kuiper_source_process_latency_us_hist_count",
        "kuiper_source_process_latency_us_hist_sum",
        // Operator metrics (9)
        "kuiper_op_records_in_total",
        "kuiper_op_records_out_total",
        "kuiper_op_messages_processed_total",
        "kuiper_op_exceptions_total",
        "kuiper_op_buffer_length",
        "kuiper_op_process_latency_us",
        "kuiper_op_process_latency_us_hist_bucket",
        "kuiper_op_process_latency_us_hist_count",
        "kuiper_op_process_latency_us_hist_sum",
        // Sink metrics (6)
        "kuiper_sink_records_in_total",
        "kuiper_sink_records_out_total",
        "kuiper_sink_messages_processed_total",
        "kuiper_sink_exceptions_total",
        "kuiper_sink_buffer_length",
        "kuiper_sink_connection_status",
        "kuiper_sink_process_latency_us",
        "kuiper_sink_process_latency_us_hist_bucket",
        "kuiper_sink_process_latency_us_hist_count",
        "kuiper_sink_process_latency_us_hist_sum",
    ];

    for fam in &families {
        assert!(
            body.contains(fam),
            "Expected metric family '{}' in Prometheus exposition:\n{}",
            fam,
            body
        );
    }

    // 5. Validate label taxonomy: rule, type, op, op_instance, name, status, le
    assert!(body.contains("rule=\"rule_prom_32\""));
    assert!(body.contains("type=\"source\""));
    assert!(body.contains("type=\"op\""));
    assert!(body.contains("type=\"sink\""));
    assert!(body.contains("op=\"demo\""));
    assert!(body.contains("op=\"project\""));
    assert!(body.contains("op=\"log_0\""));
    assert!(body.contains("op=\"mqtt_1\""));
    assert!(body.contains("op_instance=\"0\""));
    assert!(body.contains("name=\"demo\""));
    assert!(body.contains("status=\"running\""));
    assert!(body.contains("le=\"100\""));
    assert!(body.contains("le=\"+Inf\""));

    // 6. Validate backward-compatible rule-only series
    assert!(body.contains("kuiper_sink_records_in_total{rule=\"rule_prom_32\"}"));
    assert!(body.contains("kuiper_source_records_in_total{rule=\"rule_prom_32\"}"));

    Ok(())
}
