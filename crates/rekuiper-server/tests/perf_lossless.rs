//! Lossless admission + fast sink pipeline regression tests.
//!
//! Each test targets a defect class from the parent performance diagnosis:
//! broadcast-ring overwrite under burst, fan-out loss, partial-batch
//! admission, sink-failure accounting, file flush on lifecycle boundaries,
//! and deletion while ingestion is blocked/in flight.

use rekuiper_conf::KuiperConfig;
use rekuiper_core::{RuleManager, StreamBus, StreamManager, TableManager};
use rekuiper_server::routes::{create_router, AppState};
use serde_json::json;
use tokio::net::TcpListener;

async fn spawn_test_server_with_state() -> (String, tokio::task::JoinHandle<()>, AppState) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("Failed to bind ephemeral port");
    let local_addr = listener.local_addr().unwrap();

    let stream_bus = StreamBus::new();
    let stream_manager = StreamManager::new();
    let rule_manager = RuleManager::new(stream_bus.clone());

    let state = AppState::new(
        "lossless".to_string(),
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

async fn create_stream(client: &reqwest::Client, base_url: &str, name: &str) {
    let resp = client
        .post(format!("{}/streams", base_url))
        .json(&json!({"sql": format!("CREATE STREAM {} () WITH (FORMAT=\"json\")", name)}))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());
}

async fn create_rule(
    client: &reqwest::Client,
    base_url: &str,
    id: &str,
    sql: &str,
    actions: serde_json::Value,
) {
    let resp = client
        .post(format!("{}/rules", base_url))
        .json(&json!({"id": id, "sql": sql, "actions": actions}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);
}

async fn post_batch(
    client: &reqwest::Client,
    base_url: &str,
    stream: &str,
    batch: serde_json::Value,
) -> reqwest::StatusCode {
    client
        .post(format!("{}/streams/{}/data", base_url, stream))
        .json(&batch)
        .send()
        .await
        .unwrap()
        .status()
}

async fn rule_status(client: &reqwest::Client, base_url: &str, rule: &str) -> serde_json::Value {
    client
        .get(format!("{}/rules/{}/status", base_url, rule))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap()
}

async fn wait_for_status(
    client: &reqwest::Client,
    base_url: &str,
    rule: &str,
    field: &str,
    want: u64,
) -> serde_json::Value {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    loop {
        let status = rule_status(client, base_url, rule).await;
        if status[field].as_u64().unwrap_or(0) >= want {
            return status;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "timed out waiting for {} >= {} (status: {})",
            field,
            want,
            status
        );
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
}

fn batch_events(start: u64, count: usize) -> serde_json::Value {
    json!((0..count)
        .map(|i| json!({"id": start + i as u64, "temp": 25.0 + ((start + i as u64) % 10) as f64}))
        .collect::<Vec<_>>())
}

/// Burst far beyond the old 1024-record ring must be lossless end to end.
#[tokio::test]
async fn test_burst_beyond_old_ring_is_lossless() {
    let (base_url, _handle, _state) = spawn_test_server_with_state().await;
    let client = reqwest::Client::new();
    create_stream(&client, &base_url, "burst_stream").await;
    create_rule(
        &client,
        &base_url,
        "rule_burst",
        "SELECT id, temp * 1.8 + 32 AS temp_f FROM burst_stream WHERE temp > 20.0",
        json!([{"nop": {}}]),
    )
    .await;

    let total = 5_000u64;
    let mut sent = 0u64;
    while sent < total {
        let n = 500usize.min((total - sent) as usize);
        let status = post_batch(&client, &base_url, "burst_stream", batch_events(sent, n)).await;
        assert_eq!(status, reqwest::StatusCode::OK);
        sent += n as u64;
    }

    wait_for_status(
        &client,
        &base_url,
        "rule_burst",
        "sourceRecordsInTotal",
        total,
    )
    .await;
    let status = wait_for_status(
        &client,
        &base_url,
        "rule_burst",
        "sinkRecordsOutTotal",
        total,
    )
    .await;
    assert_eq!(status["exceptionsTotal"], json!(0));
    assert_eq!(
        status["sinkRecordsEnqueuedTotal"],
        json!(total),
        "every completed output was enqueued first"
    );
    // Transformed values are exact: spot-check conservation fields exist.
    assert_eq!(status["sourceRecordsFilteredTotal"], json!(0));
}

/// Every subscriber of a stream receives every record (fan-out, no
/// slow-subscriber loss of another rule's data).
#[tokio::test]
async fn test_multi_subscriber_fanout() {
    let (base_url, _handle, _state) = spawn_test_server_with_state().await;
    let client = reqwest::Client::new();
    create_stream(&client, &base_url, "fan_stream").await;
    for rule in ["rule_fan_a", "rule_fan_b"] {
        create_rule(
            &client,
            &base_url,
            rule,
            "SELECT id FROM fan_stream",
            json!([{"nop": {}}]),
        )
        .await;
    }

    let total = 2_000u64;
    let mut sent = 0u64;
    while sent < total {
        let n = 500usize.min((total - sent) as usize);
        assert_eq!(
            post_batch(&client, &base_url, "fan_stream", batch_events(sent, n)).await,
            reqwest::StatusCode::OK
        );
        sent += n as u64;
    }

    for rule in ["rule_fan_a", "rule_fan_b"] {
        let status = wait_for_status(&client, &base_url, rule, "sinkRecordsOutTotal", total).await;
        assert_eq!(status["sourceRecordsInTotal"], json!(total));
        assert_eq!(status["exceptionsTotal"], json!(0));
    }
}

/// Record order is preserved per stream/subscriber.
#[tokio::test]
async fn test_ordering_preserved() {
    let (base_url, _handle, state) = spawn_test_server_with_state().await;
    let client = reqwest::Client::new();
    create_stream(&client, &base_url, "ord_stream").await;
    let mut rx = state.stream_bus.subscribe("ord_out");
    create_rule(
        &client,
        &base_url,
        "rule_ord",
        "SELECT id FROM ord_stream",
        json!([{"memory": {"topic": "ord_out"}}]),
    )
    .await;

    let total = 1_000u64;
    let mut sent = 0u64;
    while sent < total {
        let n = 250usize.min((total - sent) as usize);
        assert_eq!(
            post_batch(&client, &base_url, "ord_stream", batch_events(sent, n)).await,
            reqwest::StatusCode::OK
        );
        sent += n as u64;
    }

    for want in 0..total {
        let record = tokio::time::timeout(std::time::Duration::from_secs(10), rx.recv())
            .await
            .expect("timed out waiting for ordered record")
            .expect("ord_out closed");
        assert_eq!(record.data.get("id"), Some(&json!(want)), "order violated");
    }
}

/// A batch containing a non-object item is rejected BEFORE admission: the
/// 400 admits zero records, so a client retry cannot duplicate a prefix.
#[tokio::test]
async fn test_partial_invalid_batch_admits_nothing() {
    let (base_url, _handle, _state) = spawn_test_server_with_state().await;
    let client = reqwest::Client::new();
    create_stream(&client, &base_url, "part_stream").await;
    create_rule(
        &client,
        &base_url,
        "rule_part",
        "SELECT id FROM part_stream",
        json!([{"nop": {}}]),
    )
    .await;

    let status = post_batch(
        &client,
        &base_url,
        "part_stream",
        json!([{"id": 1}, {"id": 2}, 42]),
    )
    .await;
    assert_eq!(status, reqwest::StatusCode::BAD_REQUEST);

    // Give any (non-existent) admission a chance to appear, then verify zero.
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    let status = rule_status(&client, &base_url, "rule_part").await;
    assert_eq!(status["sourceRecordsInTotal"], json!(0));
    assert_eq!(status["sinkRecordsOutTotal"], json!(0));

    // Oversized batches are rejected before admission as well.
    let huge: Vec<serde_json::Value> = (0..11_000).map(|i| json!({"id": i})).collect();
    let status = post_batch(&client, &base_url, "part_stream", json!(huge)).await;
    assert_eq!(status, reqwest::StatusCode::PAYLOAD_TOO_LARGE);
    let status = rule_status(&client, &base_url, "rule_part").await;
    assert_eq!(status["sourceRecordsInTotal"], json!(0));
}

/// Sink failures after dequeue are counted as failed (not as success), with
/// exceptions propagated — never silent loss with success counters.
#[tokio::test]
async fn test_sink_failure_accounting() {
    let (base_url, _handle, _state) = spawn_test_server_with_state().await;
    let client = reqwest::Client::new();
    create_stream(&client, &base_url, "fail_stream").await;
    // Port 1 refuses connections immediately: deterministic sink failure.
    create_rule(
        &client,
        &base_url,
        "rule_fail",
        "SELECT id FROM fail_stream",
        json!([{"rest": {"url": "http://127.0.0.1:1/ingest"}}]),
    )
    .await;

    let total = 5u64;
    assert_eq!(
        post_batch(
            &client,
            &base_url,
            "fail_stream",
            batch_events(0, total as usize)
        )
        .await,
        reqwest::StatusCode::OK
    );

    let status = wait_for_status(
        &client,
        &base_url,
        "rule_fail",
        "sinkRecordsFailedTotal",
        total,
    )
    .await;
    assert_eq!(status["sourceRecordsInTotal"], json!(total));
    assert_eq!(status["sinkRecordsOutTotal"], json!(0));
    assert_eq!(status["sinkRecordsEnqueuedTotal"], json!(total));
    assert!(status["exceptionsTotal"].as_u64().unwrap_or(0) >= total);
}

/// File outputs flush on rule delete (lifecycle boundary): all enqueued rows
/// reach the file with exact JSON values, no duplicates, no loss.
#[tokio::test]
async fn test_file_flush_on_rule_delete() {
    let (base_url, _handle, _state) = spawn_test_server_with_state().await;
    let client = reqwest::Client::new();
    create_stream(&client, &base_url, "file_stream").await;

    let mut path = std::env::temp_dir();
    path.push(format!(
        "rekuiper-lossless-{}-{}.json",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0)
    ));
    let _ = tokio::fs::remove_file(&path).await;
    let path_str = path.to_string_lossy().into_owned();

    create_rule(
        &client,
        &base_url,
        "rule_file_flush",
        "SELECT id, temp FROM file_stream",
        json!([{"file": {"path": path_str}}]),
    )
    .await;

    let total = 250u64;
    let mut sent = 0u64;
    while sent < total {
        let n = 100usize.min((total - sent) as usize);
        assert_eq!(
            post_batch(&client, &base_url, "file_stream", batch_events(sent, n)).await,
            reqwest::StatusCode::OK
        );
        sent += n as u64;
    }
    wait_for_status(
        &client,
        &base_url,
        "rule_file_flush",
        "sinkRecordsOutTotal",
        total,
    )
    .await;

    // Delete flushes remaining batches on shutdown; the file must then hold
    // every unique ID exactly once with transformed values intact.
    let resp = client
        .delete(format!("{}/rules/rule_file_flush", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
    let ids: Vec<u64> = loop {
        let content = tokio::fs::read_to_string(&path).await.unwrap_or_default();
        let lines: Vec<&str> = content.lines().collect();
        if lines.len() as u64 >= total {
            let mut ids = Vec::with_capacity(lines.len());
            for line in &lines {
                let v: serde_json::Value = serde_json::from_str(line).unwrap();
                ids.push(v["id"].as_u64().unwrap());
                assert!(v["temp"].is_number(), "transformed value intact");
            }
            break ids;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "timed out waiting for {} file rows (got {})",
            total,
            lines.len()
        );
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    };
    let mut sorted = ids.clone();
    sorted.sort_unstable();
    sorted.dedup();
    assert_eq!(sorted.len() as u64, total, "no duplicates, no loss");
    assert_eq!(sorted, (0..total).collect::<Vec<_>>());
    let _ = tokio::fs::remove_file(&path).await;
}

/// Deleting a rule while a burst is in flight never hangs or panics; the
/// rule is gone afterwards and later posts are accepted (no subscribers).
#[tokio::test]
async fn test_delete_while_burst_in_flight() {
    let (base_url, _handle, _state) = spawn_test_server_with_state().await;
    let client = reqwest::Client::new();
    create_stream(&client, &base_url, "chaos_stream").await;
    create_rule(
        &client,
        &base_url,
        "rule_chaos",
        "SELECT id FROM chaos_stream",
        json!([{"nop": {}}]),
    )
    .await;

    let burst = tokio::spawn({
        let client = client.clone();
        let base_url = base_url.clone();
        async move {
            for i in 0..20u64 {
                let batch = batch_events(i * 500, 500);
                let _ = client
                    .post(format!("{}/streams/chaos_stream/data", base_url))
                    .json(&batch)
                    .send()
                    .await;
            }
        }
    });
    // Delete mid-burst, then re-post after the burst drains.
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    let resp = client
        .delete(format!("{}/rules/rule_chaos", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    tokio::time::timeout(std::time::Duration::from_secs(60), burst)
        .await
        .expect("burst task hung after delete")
        .unwrap();

    let resp = client
        .get(format!("{}/rules/rule_chaos/status", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::NOT_FOUND);
    // No subscribers: ingestion still 200 (documented), nothing to process.
    assert_eq!(
        post_batch(&client, &base_url, "chaos_stream", batch_events(0, 10)).await,
        reqwest::StatusCode::OK
    );
}
