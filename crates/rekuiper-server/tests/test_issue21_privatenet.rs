use rekuiper_conf::KuiperConfig;
use rekuiper_core::manager::{RuleManager, StreamManager, TableManager};
use rekuiper_core::StreamBus;
use rekuiper_server::routes::{create_router, is_private_or_internal_ip, AppState};
use serde_json::json;
use std::net::IpAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::net::TcpListener;

async fn spawn_test_server(enable_private_net: bool) -> (String, tokio::task::JoinHandle<()>, AppState) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("Failed to bind ephemeral port");
    let local_addr = listener.local_addr().unwrap();

    let stream_bus = StreamBus::new();
    let stream_manager = StreamManager::new();
    let rule_manager = RuleManager::new(stream_bus.clone());

    let mut config = KuiperConfig::default();
    config.basic.enable_private_net = enable_private_net;

    let state = AppState::new(
        "test_issue21".to_string(),
        config,
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
async fn test_rest_sink_blocks_private_net_by_default() {
    // 1. Start mock server on 127.0.0.1
    let mock_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mock_addr = mock_listener.local_addr().unwrap();
    let requests_received = Arc::new(AtomicUsize::new(0));
    let mock_reqs = requests_received.clone();

    tokio::spawn(async move {
        let app = axum::Router::new().route(
            "/hook",
            axum::routing::post(move || {
                let r = mock_reqs.clone();
                async move {
                    r.fetch_add(1, Ordering::SeqCst);
                    axum::http::StatusCode::OK
                }
            }),
        );
        axum::serve(mock_listener, app).await.unwrap();
    });

    // 2. Start rekuiper with default config (enablePrivateNet: false)
    let (base_url, _handle, _state) = spawn_test_server(false).await;
    let client = reqwest::Client::new();

    // Create stream
    let resp = client
        .post(format!("{}/streams", base_url))
        .json(&json!({
            "sql": "CREATE STREAM s () WITH (FORMAT=\"json\")"
        }))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    // Create rule pointing REST sink to 127.0.0.1
    let hook_url = format!("http://127.0.0.1:{}/hook", mock_addr.port());
    let resp = client
        .post(format!("{}/rules", base_url))
        .json(&json!({
            "id": "r_private",
            "sql": "SELECT id FROM s",
            "actions": [{"rest": {"url": hook_url, "method": "post", "sendSingle": true}}]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);

    // Ingest event
    let resp = client
        .post(format!("{}/streams/s/data", base_url))
        .json(&json!({"id": 1}))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    // Wait for rule execution and check status
    let mut got_exception = false;
    for _ in 0..50 {
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        let resp = client
            .get(format!("{}/rules/r_private/status", base_url))
            .send()
            .await
            .unwrap();
        let status: serde_json::Value = resp.json().await.unwrap();
        let exceptions = status["exceptions_total"].as_u64().unwrap_or(0);
        let last_exc = status["last_exception"].as_str().unwrap_or("");
        if exceptions > 0 && last_exc.contains("is in internal network") {
            got_exception = true;
            assert!(last_exc.contains("dial tcp 127.0.0.1:"));
            assert!(last_exc.contains("ip 127.0.0.1 is in internal network"));
            assert!(last_exc.contains("recoverAble=false"));
            break;
        }
    }
    assert!(got_exception, "Expected internal network exception in rule status");
    // Mock server must never have received the request
    assert_eq!(requests_received.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn test_rest_sink_allows_private_net_when_configured() {
    // 1. Start mock server on 127.0.0.1
    let mock_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mock_addr = mock_listener.local_addr().unwrap();
    let requests_received = Arc::new(AtomicUsize::new(0));
    let mock_reqs = requests_received.clone();

    tokio::spawn(async move {
        let app = axum::Router::new().route(
            "/hook",
            axum::routing::post(move || {
                let r = mock_reqs.clone();
                async move {
                    r.fetch_add(1, Ordering::SeqCst);
                    axum::http::StatusCode::OK
                }
            }),
        );
        axum::serve(mock_listener, app).await.unwrap();
    });

    // 2. Start rekuiper with enablePrivateNet: true
    let (base_url, _handle, _state) = spawn_test_server(true).await;
    let client = reqwest::Client::new();

    // Create stream
    let resp = client
        .post(format!("{}/streams", base_url))
        .json(&json!({
            "sql": "CREATE STREAM s_allowed () WITH (FORMAT=\"json\")"
        }))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    // Create rule pointing REST sink to 127.0.0.1
    let hook_url = format!("http://127.0.0.1:{}/hook", mock_addr.port());
    let resp = client
        .post(format!("{}/rules", base_url))
        .json(&json!({
            "id": "r_allowed",
            "sql": "SELECT id FROM s_allowed",
            "actions": [{"rest": {"url": hook_url, "method": "post", "sendSingle": true}}]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);

    // Ingest event
    let resp = client
        .post(format!("{}/streams/s_allowed/data", base_url))
        .json(&json!({"id": 1}))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    // Wait for delivery to mock server
    for _ in 0..50 {
        if requests_received.load(Ordering::SeqCst) > 0 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    assert_eq!(requests_received.load(Ordering::SeqCst), 1);

    let resp = client
        .get(format!("{}/rules/r_allowed/status", base_url))
        .send()
        .await
        .unwrap();
    let status: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(status["exceptionsTotal"], json!(0));
    assert_eq!(status["sinkRecordsOutTotal"], json!(1));
}

#[tokio::test]
async fn test_rest_sink_validation_rejects_bad_method_and_form() {
    let (base_url, _handle, _state) = spawn_test_server(false).await;
    let client = reqwest::Client::new();

    // Bad method
    let resp = client
        .post(format!("{}/rules", base_url))
        .json(&json!({
            "id": "r_bad_method",
            "sql": "SELECT id FROM s",
            "actions": [{"rest": {"url": "http://example.com", "method": "frobnicate"}}]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::BAD_REQUEST);
    let body = resp.text().await.unwrap();
    assert!(body.contains("Not supported HTTP method frobnicate."));

    // Form bodyType without urlencoded format
    let resp = client
        .post(format!("{}/rules", base_url))
        .json(&json!({
            "id": "r_bad_form",
            "sql": "SELECT id FROM s",
            "actions": [{"rest": {"url": "http://example.com", "bodyType": "form"}}]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::BAD_REQUEST);
    let body = resp.text().await.unwrap();
    assert!(body.contains("format must be urlencoded if bodyType is form"));
}

#[test]
fn test_is_private_or_internal_ip_unit() {
    // IPv4 private/internal
    assert!(is_private_or_internal_ip("127.0.0.1".parse::<IpAddr>().unwrap()));
    assert!(is_private_or_internal_ip("127.0.0.2".parse::<IpAddr>().unwrap()));
    assert!(is_private_or_internal_ip("10.0.0.1".parse::<IpAddr>().unwrap()));
    assert!(is_private_or_internal_ip("172.16.0.1".parse::<IpAddr>().unwrap()));
    assert!(is_private_or_internal_ip("192.168.1.1".parse::<IpAddr>().unwrap()));
    assert!(is_private_or_internal_ip("192.168.77.246".parse::<IpAddr>().unwrap()));
    assert!(is_private_or_internal_ip("169.254.1.1".parse::<IpAddr>().unwrap()));
    assert!(is_private_or_internal_ip("100.64.0.1".parse::<IpAddr>().unwrap()));
    assert!(is_private_or_internal_ip("0.0.0.0".parse::<IpAddr>().unwrap()));
    assert!(is_private_or_internal_ip("255.255.255.255".parse::<IpAddr>().unwrap()));

    // IPv6 private/internal
    assert!(is_private_or_internal_ip("::1".parse::<IpAddr>().unwrap()));
    assert!(is_private_or_internal_ip("::".parse::<IpAddr>().unwrap()));
    assert!(is_private_or_internal_ip("fe80::1".parse::<IpAddr>().unwrap()));
    assert!(is_private_or_internal_ip("fc00::1".parse::<IpAddr>().unwrap()));
    assert!(is_private_or_internal_ip("fd00::1".parse::<IpAddr>().unwrap()));

    // Public IPs
    assert!(!is_private_or_internal_ip("8.8.8.8".parse::<IpAddr>().unwrap()));
    assert!(!is_private_or_internal_ip("1.1.1.1".parse::<IpAddr>().unwrap()));
    assert!(!is_private_or_internal_ip("93.184.216.34".parse::<IpAddr>().unwrap()));
    assert!(!is_private_or_internal_ip("2606:4700:4700::1111".parse::<IpAddr>().unwrap()));
}
