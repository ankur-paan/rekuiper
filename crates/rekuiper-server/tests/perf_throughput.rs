use std::collections::HashMap;
use serde_json::{json, Value};
use tokio::net::TcpListener;
use rekuiper_conf::KuiperConfig;
use rekuiper_core::{RuleManager, StreamBus, StreamManager, TableManager};
use rekuiper_core::model::StreamRecord;
use rekuiper_server::routes::{create_router, AppState};

async fn spawn_test_server() -> (String, tokio::task::JoinHandle<()>, AppState) {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("Failed to bind ephemeral port");
    let local_addr = listener.local_addr().unwrap();

    let stream_bus = StreamBus::new();
    let stream_manager = StreamManager::new();
    let rule_manager = RuleManager::new(stream_bus.clone());

    let state = AppState::new(
        "perf".to_string(),
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
async fn test_high_throughput_streaming_pipeline() {
    let (base_url, _handle, state) = spawn_test_server().await;
    let client = reqwest::Client::new();

    // 2. High-frequency stream.
    let resp = client
        .post(format!("{}/streams", base_url))
        .json(&json!({
            "sql": "CREATE STREAM perf_stream () WITH (FORMAT=\"json\")"
        }))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    // 3. Arithmetic transformation + filter into a no-op sink.
    let resp = client
        .post(format!("{}/rules", base_url))
        .json(&json!({
            "id": "rule_perf",
            "sql": "SELECT id, temp * 1.8 + 32 AS temp_f FROM perf_stream WHERE temp > 20.0",
            "actions": [{"nop": {}}]
        }))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    // 4. Blast 50,000 records straight into the broadcast bus, in chunks with
    // catch-up waits: on a single-threaded runtime an unyielding send loop
    // would starve the rule task and overflow the broadcast buffer (lag
    // drops). Chunking bounds in-flight records below capacity, so every
    // record is counted exactly once.
    let tx = state.stream_bus.get_or_create("perf_stream");
    let start = std::time::Instant::now();
    let mut sent = 0u64;
    for _chunk in 0..100 {
        for _ in 0..500 {
            let n = sent;
            let data: HashMap<String, Value> = serde_json::from_str(
                &format!(r#"{{"id": "dev_{}", "temp": {}}}"#, n, 25.0 + (n % 10) as f64),
            )
            .unwrap();
            let _ = tx.send(StreamRecord { timestamp: n as i64, data });
            sent += 1;
        }
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        loop {
            let seen = state
                .rule_manager
                .get_rule_status("rule_perf")
                .map(|s| s.source_records_in_total)
                .unwrap_or(0);
            if seen >= sent {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "consumer stalled at {seen}/50,000"
            );
            tokio::task::yield_now().await;
        }
    }
    assert_eq!(sent, 50_000);

    // 5. Rule status reflects every ingested record.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        let status: serde_json::Value = client
            .get(format!("{}/rules/rule_perf/status", base_url))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        if status["sourceRecordsInTotal"].as_u64().unwrap_or(0) >= 50_000 {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "timed out waiting for 50,000 ingested records"
        );
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }

    // 6. Throughput report + floor assertion.
    let elapsed = start.elapsed();
    let events_per_sec = 50_000.0 / elapsed.as_secs_f64();
    println!(
        "High-Speed Pipeline Throughput: {:.2} events/sec in {:?}",
        events_per_sec, elapsed
    );
    assert!(
        events_per_sec > 20_000.0,
        "Throughput too low: {:.2} events/sec",
        events_per_sec
    );
}
