//! Broker-backed IIoT/vehicle tests: MQTT source formats and metadata,
//! grouped windows over MQTT, and sink cache/resend across a broker outage.
//!
//! These need a real MQTT broker and are skipped unless
//! `REKUIPER_TEST_MQTT` names one, e.g.
//! `REKUIPER_TEST_MQTT=tcp://127.0.0.1:11883 cargo test -p rekuiper-server --test iiot_mqtt`.

use rekuiper_conf::KuiperConfig;
use rekuiper_connectors::{MqttConfig, MqttSink, MqttSource};
use rekuiper_core::{RuleManager, StreamBus, StreamManager, StreamReceiver, TableManager};
use rekuiper_server::routes::{create_router, AppState};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::time::Duration;
use tokio::net::{TcpListener, TcpStream};

fn broker() -> Option<String> {
    std::env::var("REKUIPER_TEST_MQTT")
        .ok()
        .filter(|s| !s.trim().is_empty())
}

fn unique(prefix: &str) -> String {
    format!(
        "{}{}_{}",
        prefix,
        std::process::id(),
        chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0)
    )
}

async fn spawn_server() -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let stream_bus = StreamBus::new();
    let state = AppState::new(
        "iiot".to_string(),
        KuiperConfig::default(),
        StreamManager::new(),
        TableManager::new(),
        RuleManager::new(stream_bus.clone()),
        stream_bus,
    );
    let app = create_router(state);
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    format!("http://{}", addr)
}

async fn post(client: &reqwest::Client, url: String, body: Value) {
    let resp = client.post(&url).json(&body).send().await.unwrap();
    let status = resp.status();
    let text = resp.text().await.unwrap_or_default();
    assert!(status.is_success(), "POST {url} -> {status}: {text}");
}

fn mqtt_config(server: &str, topic: &str) -> MqttConfig {
    serde_json::from_value(json!({"server": server, "topic": topic})).unwrap()
}

/// Publisher that retries until the rule's subscription is live (QoS 0 is
/// not replayed), then returns.
async fn publisher(server: &str, topic: &str) -> MqttSink {
    let sink = MqttSink::new(mqtt_config(server, topic)).unwrap();
    for _ in 0..100 {
        if sink.is_connected() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    sink
}

/// Wait for JSON lines in a file sink until `done` accepts them.
async fn wait_rows(path: &std::path::Path, done: impl Fn(&[Value]) -> bool) -> Vec<Value> {
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    loop {
        let rows: Vec<Value> = std::fs::read_to_string(path)
            .unwrap_or_default()
            .lines()
            .filter_map(|l| serde_json::from_str(l).ok())
            .collect();
        if done(&rows) {
            return rows;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "timed out waiting for rows in {path:?}; got {rows:?}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::test]
async fn esphome_binary_payload_with_topic_metadata() {
    let Some(server) = broker() else { return };
    let base = spawn_server().await;
    let client = reqwest::Client::new();
    let id = unique("esp");
    let out = std::env::temp_dir().join(format!("{id}.jsonl"));
    let wildcard = format!("rk-it/{id}/esphome/+/state");
    post(
        &client,
        format!("{base}/metadata/sources/mqtt/confKeys/{id}"),
        json!({"server": server}),
    )
    .await;
    post(
        &client,
        format!("{base}/streams"),
        json!({"sql": format!(
            "CREATE STREAM {id} () WITH (DATASOURCE=\"{wildcard}\", FORMAT=\"binary\", CONF_KEY=\"{id}\", TYPE=\"mqtt\")"
        )}),
    )
    .await;
    post(
        &client,
        format!("{base}/rules"),
        json!({
            "id": id,
            "sql": format!("SELECT meta(topic) AS topic, self AS state FROM {id}"),
            "actions": [{"file": {"path": out.to_string_lossy()}}]
        }),
    )
    .await;

    let topic = format!("rk-it/{id}/esphome/kitchen/state");
    let sink = publisher(&server, &topic).await;
    // Repeat until the rule's subscription is live; the file then holds rows.
    let rows = {
        let mut rows = Vec::new();
        for _ in 0..40 {
            sink.send_raw(b"23.5".to_vec()).await.unwrap();
            tokio::time::sleep(Duration::from_millis(250)).await;
            rows = std::fs::read_to_string(&out)
                .unwrap_or_default()
                .lines()
                .filter_map(|l| serde_json::from_str::<Value>(l).ok())
                .collect();
            if !rows.is_empty() {
                break;
            }
        }
        rows
    };
    assert!(!rows.is_empty(), "no ESPHome rows arrived");
    assert_eq!(rows[0]["topic"], json!(topic));
    assert_eq!(rows[0]["state"], json!("23.5"));
    assert!(rows[0].get("__meta__").is_none());
    let _ = std::fs::remove_file(out);
}

#[tokio::test]
async fn json_array_payloads_group_per_device_in_windows() {
    let Some(server) = broker() else { return };
    let base = spawn_server().await;
    let client = reqwest::Client::new();
    let id = unique("veh");
    let out = std::env::temp_dir().join(format!("{id}.jsonl"));
    let topic = format!("rk-it/{id}/vehicles");
    post(
        &client,
        format!("{base}/metadata/sources/mqtt/confKeys/{id}"),
        json!({"server": server}),
    )
    .await;
    post(
        &client,
        format!("{base}/streams"),
        json!({"sql": format!(
            "CREATE STREAM {id} () WITH (DATASOURCE=\"{topic}\", FORMAT=\"json\", CONF_KEY=\"{id}\", TYPE=\"mqtt\")"
        )}),
    )
    .await;
    post(
        &client,
        format!("{base}/rules"),
        json!({
            "id": id,
            "sql": format!(
                "SELECT vin, count(*) AS n, max(speed) AS top FROM {id} WHERE speed > 0 GROUP BY vin, TUMBLINGWINDOW(ss, 2)"
            ),
            "actions": [{"file": {"path": out.to_string_lossy()}}]
        }),
    )
    .await;

    let sink = publisher(&server, &topic).await;
    // Warm up until the subscription is live (a warm-up vin is ignored).
    let deadline = std::time::Instant::now() + Duration::from_secs(15);
    loop {
        sink.send_raw(br#"[{"vin":"warm","speed":1}]"#.to_vec())
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(300)).await;
        let warm = std::fs::read_to_string(&out).unwrap_or_default();
        if warm.contains("warm") {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "subscription never went live"
        );
    }
    for _ in 0..10 {
        sink.send_raw(
            br#"[{"vin":"V1","speed":40},{"vin":"V2","speed":0},{"vin":"V2","speed":55},{"vin":"V1","speed":90}]"#
                .to_vec(),
        )
        .await
        .unwrap();
    }
    let rows = wait_rows(&out, |rows| {
        let n = |vin: &str| -> u64 {
            rows.iter()
                .filter(|r| r["vin"] == json!(vin))
                .filter_map(|r| r["n"].as_u64())
                .sum()
        };
        n("V1") == 20 && n("V2") == 10
    })
    .await;
    assert!(rows
        .iter()
        .filter(|r| r["vin"] == json!("V1"))
        .all(|r| r["top"] == json!(90)));
    let _ = std::fs::remove_file(out);
}

type Tasks = std::sync::Arc<parking_lot::Mutex<Vec<tokio::task::JoinHandle<()>>>>;

/// TCP proxy in front of the broker that can drop and restore connectivity.
struct Proxy {
    port: u16,
    upstream: String,
    /// Accept loop plus every proxied connection; aborting them cuts the link.
    tasks: Tasks,
}

impl Proxy {
    async fn start(upstream: String) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let proxy = Self {
            port,
            upstream,
            tasks: Tasks::default(),
        };
        proxy.serve(listener);
        proxy
    }

    fn serve(&self, listener: TcpListener) {
        let upstream = self.upstream.clone();
        let tasks = self.tasks.clone();
        let accept = tokio::spawn(async move {
            while let Ok((mut inbound, _)) = listener.accept().await {
                let upstream = upstream.clone();
                let conn = tokio::spawn(async move {
                    if let Ok(mut outbound) = TcpStream::connect(upstream).await {
                        let _ = tokio::io::copy_bidirectional(&mut inbound, &mut outbound).await;
                    }
                });
                tasks.lock().push(conn);
            }
        });
        self.tasks.lock().push(accept);
    }

    /// Outage: stop accepting and cut every proxied connection.
    fn down(&self) {
        for task in self.tasks.lock().drain(..) {
            task.abort();
        }
    }

    fn up(&self) {
        let socket = tokio::net::TcpSocket::new_v4().unwrap();
        socket.set_reuseaddr(true).unwrap();
        socket
            .bind(format!("127.0.0.1:{}", self.port).parse().unwrap())
            .unwrap();
        self.serve(socket.listen(64).unwrap());
    }
}

fn host_port(server: &str) -> String {
    let (host, port) = rekuiper_connectors::parse_mqtt_server_url(server).unwrap();
    format!("{host}:{port}")
}

async fn collect(
    rx: &mut StreamReceiver,
    want: usize,
    within: Duration,
) -> Vec<HashMap<String, Value>> {
    let mut got = Vec::new();
    let deadline = tokio::time::Instant::now() + within;
    while got.len() < want {
        match tokio::time::timeout_at(deadline, rx.recv()).await {
            Ok(Some(record)) => got.push(record.data),
            _ => break,
        }
    }
    got
}

#[tokio::test]
async fn mqtt_sink_caches_through_broker_outage_and_resends_in_order() {
    let Some(server) = broker() else { return };
    let base = spawn_server().await;
    let client = reqwest::Client::new();
    let id = unique("uplink");
    let topic = format!("rk-it/{id}/uplink");
    let proxy = Proxy::start(host_port(&server)).await;

    // Cloud-side subscriber connected straight to the broker (the cancel
    // sender must outlive the test or the source stops at once).
    let bus = StreamBus::new();
    let mut rx = bus.subscribe("cloud");
    let (_cancel_tx, cancel_rx) = tokio::sync::watch::channel(false);
    MqttSource::new(mqtt_config(&server, &topic), bus.get_or_create("cloud")).spawn(cancel_rx);

    post(
        &client,
        format!("{base}/streams"),
        json!({"sql": format!("CREATE STREAM {id} () WITH (DATASOURCE=\"{id}\", FORMAT=\"json\", TYPE=\"httppush\")")}),
    )
    .await;
    post(
        &client,
        format!("{base}/rules"),
        json!({
            "id": id,
            "sql": format!("SELECT seq FROM {id}"),
            "actions": [{"mqtt": {
                "server": format!("tcp://127.0.0.1:{}", proxy.port),
                "topic": topic,
                "enableCache": true,
                "memoryCacheThreshold": 4,
                "bufferPageSize": 3,
                "resendIndicatorField": "resent",
                "resendPriority": 1
            }}]
        }),
    )
    .await;

    let push = |from: u64, to: u64| {
        let rows: Vec<Value> = (from..to).map(|seq| json!({"seq": seq})).collect();
        let client = client.clone();
        let url = format!("{base}/streams/{id}/data");
        async move { post(&client, url, Value::Array(rows)).await }
    };

    // Subscriber readiness: probe until the cloud side sees a record.
    let mut warm = Vec::new();
    for seq in 1_000_000..1_000_050u64 {
        push(seq, seq + 1).await;
        warm = collect(&mut rx, 1, Duration::from_millis(300)).await;
        if !warm.is_empty() {
            break;
        }
    }
    assert!(!warm.is_empty(), "uplink never reached the broker");
    let _ = collect(&mut rx, usize::MAX, Duration::from_millis(500)).await;

    // Online.
    push(0, 5).await;
    let online = collect(&mut rx, 5, Duration::from_secs(10)).await;
    assert_eq!(online.len(), 5);

    // Outage: the sink sees its connection drop, then data keeps coming.
    proxy.down();
    tokio::time::sleep(Duration::from_millis(1500)).await;
    push(5, 25).await;
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert!(
        collect(&mut rx, 1, Duration::from_millis(300))
            .await
            .is_empty(),
        "nothing can arrive during the outage"
    );

    // Recovery: everything cached arrives once, in order, marked as resent.
    proxy.up();
    let recovered = collect(&mut rx, 20, Duration::from_secs(30)).await;
    let seqs: Vec<u64> = recovered.iter().filter_map(|r| r["seq"].as_u64()).collect();
    assert_eq!(
        seqs,
        (5..25).collect::<Vec<_>>(),
        "resend order and completeness"
    );
    assert!(recovered
        .iter()
        .all(|r| r.get("resent") == Some(&json!(true))));
    assert!(
        collect(&mut rx, 1, Duration::from_secs(2)).await.is_empty(),
        "no duplicates after recovery"
    );
}
