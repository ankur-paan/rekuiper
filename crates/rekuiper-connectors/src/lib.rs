use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use anyhow::{bail, Context, Result};
use async_trait::async_trait;
use parking_lot::RwLock;
use rumqttc::{AsyncClient, Event, MqttOptions, Packet, QoS};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use rekuiper_core::model::StreamRecord;

#[async_trait]
pub trait Sink: Send + Sync {
    async fn send(&self, record: &StreamRecord) -> Result<()>;
}

pub struct LogSink;

#[async_trait]
impl Sink for LogSink {
    async fn send(&self, record: &StreamRecord) -> Result<()> {
        tracing::info!("[LOG SINK] Output: {:?}", record.data);
        Ok(())
    }
}

/// Nop sink following the eKuiper nop sink: discards data without error.
pub struct NopSink;

#[async_trait]
impl Sink for NopSink {
    async fn send(&self, _record: &StreamRecord) -> Result<()> {
        Ok(())
    }
}

#[derive(Clone, Default)]
pub struct MemorySink {
    pub records: Arc<RwLock<Vec<StreamRecord>>>,
}

impl MemorySink {
    pub fn new() -> Self {
        Self {
            records: Arc::new(RwLock::new(Vec::new())),
        }
    }

    pub fn get_records(&self) -> Vec<StreamRecord> {
        self.records.read().clone()
    }

    pub fn clear(&self) {
        self.records.write().clear();
    }
}

#[async_trait]
impl Sink for MemorySink {
    async fn send(&self, record: &StreamRecord) -> Result<()> {
        self.records.write().push(record.clone());
        Ok(())
    }
}

pub struct HttpSink {
    pub url: String,
    client: reqwest::Client,
}

impl HttpSink {
    pub fn new(url: String) -> Self {
        Self {
            url,
            client: reqwest::Client::new(),
        }
    }
}

#[async_trait]
impl Sink for HttpSink {
    async fn send(&self, record: &StreamRecord) -> Result<()> {
        self.client
            .post(&self.url)
            .json(&record.data)
            .send()
            .await?;
        Ok(())
    }
}

/// File sink following the eKuiper file sink convention.
///
/// By default appends each [`StreamRecord`] as a single JSON object per line
/// (line-delimited JSON) with a trailing newline. With
/// `format: "delimited"` (or `fileType: "csv"`) it writes CSV rows instead,
/// emitting a sorted-key header row first when `hasHeader` is set.
///
/// Deserializable from eKuiper file action options, e.g.
/// `{"path": "data/result.txt"}` or
/// `{"path": "data/result.csv", "format": "delimited", "hasHeader": true}`.
#[derive(Debug, Clone, Deserialize)]
pub struct FileSink {
    pub path: PathBuf,
    #[serde(default)]
    pub format: Option<String>,
    #[serde(default, rename = "fileType")]
    pub file_type: Option<String>,
    #[serde(default, rename = "hasHeader")]
    pub has_header: bool,
    #[serde(default)]
    pub delimiter: Option<String>,
}

impl FileSink {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self {
            path: path.into(),
            format: None,
            file_type: None,
            has_header: false,
            delimiter: None,
        }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    fn is_delimited(&self) -> bool {
        self.format
            .as_deref()
            .is_some_and(|f| f.eq_ignore_ascii_case("delimited"))
            || self
                .file_type
                .as_deref()
                .is_some_and(|t| t.eq_ignore_ascii_case("csv"))
    }

    fn delimiter_str(&self) -> &str {
        match self.delimiter.as_deref() {
            Some(d) if !d.is_empty() => d,
            _ => ",",
        }
    }
}

#[async_trait]
impl Sink for FileSink {
    async fn send(&self, record: &StreamRecord) -> Result<()> {
        if let Some(parent) = self.path.parent() {
            if !parent.as_os_str().is_empty() {
                tokio::fs::create_dir_all(parent)
                    .await
                    .with_context(|| format!("Failed to create parent dirs for {:?}", self.path))?;
            }
        }
        if self.is_delimited() {
            self.send_delimited(record).await
        } else {
            let mut line = serde_json::to_string(&record.data)?;
            line.push('\n');
            append_text(&self.path, line.as_bytes()).await
        }
    }
}

impl FileSink {
    /// Appends a pre-rendered line (e.g. a `dataTemplate` result).
    pub async fn send_text(&self, text: &str) -> Result<()> {
        if let Some(parent) = self.path.parent() {
            if !parent.as_os_str().is_empty() {
                tokio::fs::create_dir_all(parent)
                    .await
                    .with_context(|| format!("Failed to create parent dirs for {:?}", self.path))?;
            }
        }
        let mut line = text.to_string();
        line.push('\n');
        append_text(&self.path, line.as_bytes()).await
    }

    async fn send_delimited(&self, record: &StreamRecord) -> Result<()> {
        let delim = self.delimiter_str();
        let mut keys: Vec<String> = record.data.keys().cloned().collect();
        keys.sort();
        let mut out = String::new();
        if self.has_header && is_missing_or_empty(&self.path).await {
            out.push_str(&keys.join(delim));
            out.push('\n');
        }
        let row: String = keys
            .iter()
            .map(|k| csv_field(record.data.get(k), delim))
            .collect::<Vec<_>>()
            .join(delim);
        out.push_str(&row);
        out.push('\n');
        append_text(&self.path, out.as_bytes()).await
    }
}

async fn is_missing_or_empty(path: &Path) -> bool {
    match tokio::fs::metadata(path).await {
        Err(_) => true,
        Ok(meta) => meta.len() == 0,
    }
}

/// Render one CSV cell: missing/`Null` as empty, strings CSV-escaped,
/// numbers/booleans plain, containers as compact JSON.
fn csv_field(value: Option<&Value>, delim: &str) -> String {
    match value {
        None | Some(Value::Null) => String::new(),
        Some(Value::String(s)) => csv_escape(s, delim),
        Some(Value::Number(n)) => n.to_string(),
        Some(Value::Bool(b)) => b.to_string(),
        Some(other) => serde_json::to_string(other).unwrap_or_default(),
    }
}

/// Minimal RFC-4180 escaping: quote the cell when it contains the delimiter,
/// a quote, or a line break, doubling embedded quotes.
fn csv_escape(s: &str, delim: &str) -> String {
    if s.contains(delim) || s.contains(['"', '\n', '\r']) {
        format!("\"{}\"", s.replace('"', "\"\""))
    } else {
        s.to_string()
    }
}

async fn append_text(path: &Path, bytes: &[u8]) -> Result<()> {
    use tokio::fs::OpenOptions;
    use tokio::io::AsyncWriteExt;
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .await
        .with_context(|| format!("Failed to open file {:?}", path))?;
    file.write_all(bytes)
        .await
        .with_context(|| format!("Failed to write to file {:?}", path))?;
    file.flush().await?;
    Ok(())
}

/// File source following the eKuiper file source convention.
///
/// Reads line-delimited JSON records from a file, where each non-empty line
/// is a JSON object, and converts them into [`StreamRecord`]s.
pub struct FileSource {
    pub path: PathBuf,
}

impl FileSource {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Read all records from the file without sending them anywhere.
    pub async fn read_records(&self) -> Result<Vec<StreamRecord>> {
        let content = tokio::fs::read_to_string(&self.path)
            .await
            .with_context(|| format!("Failed to read file {:?}", self.path))?;
        parse_ldjson(&content)
    }

    /// Read records from the file and push each one to `sender`.
    ///
    /// Returns the number of records pushed.
    pub async fn run(&self, sender: &tokio::sync::mpsc::Sender<StreamRecord>) -> Result<usize> {
        let records = self.read_records().await?;
        let n = records.len();
        for record in records {
            sender
                .send(record)
                .await
                .map_err(|e| anyhow::anyhow!("FileSource receiver dropped: {}", e))?;
        }
        Ok(n)
    }
}

fn parse_ldjson(content: &str) -> Result<Vec<StreamRecord>> {
    let mut records = Vec::new();
    for (idx, line) in content.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let value: Value = serde_json::from_str(line)
            .with_context(|| format!("Invalid JSON on line {} of file source", idx + 1))?;
        let obj = value.as_object().cloned().unwrap_or_else(|| {
            let mut m = serde_json::Map::new();
            m.insert("value".to_string(), value);
            m
        });
        let data: HashMap<String, Value> = obj.into_iter().collect();
        records.push(StreamRecord::new(data));
    }
    Ok(records)
}

// ---------------------------------------------------------------------------
// MQTT source / sink following the eKuiper MQTT documentation.
// ---------------------------------------------------------------------------

/// MQTT connection settings (eKuiper MQTT source/sink action format).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MqttConfig {
    /// Broker URL, e.g. `tcp://127.0.0.1:1883`.
    pub server: String,
    /// Topic to publish to / subscribe to.
    pub topic: String,
    /// Client id (`clientId` in eKuiper JSON). Generated when absent.
    #[serde(default)]
    pub client_id: Option<String>,
    /// MQTT QoS level (0, 1 or 2). Defaults to 0.
    #[serde(default)]
    pub qos: u8,
    /// Optional username.
    #[serde(default)]
    pub username: Option<String>,
    /// Optional password.
    #[serde(default)]
    pub password: Option<String>,
}

impl MqttConfig {
    pub fn qos_level(&self) -> QoS {
        qos_from_u8(self.qos)
    }

    pub fn effective_client_id(&self) -> String {
        if let Some(id) = &self.client_id {
            if !id.is_empty() {
                return id.clone();
            }
        }
        generate_client_id()
    }
}

fn qos_from_u8(qos: u8) -> QoS {
    match qos {
        1 => QoS::AtLeastOnce,
        2 => QoS::ExactlyOnce,
        _ => QoS::AtMostOnce,
    }
}

fn generate_client_id() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    format!("rekuiper-{}-{}", std::process::id(), nanos)
}

/// Extract `(host, port)` from an eKuiper MQTT server URL.
///
/// Accepts forms like `tcp://127.0.0.1:1883`, `127.0.0.1:1883`,
/// `ssl://broker.emqx.io:8883` or a bare hostname. The port defaults to
/// 1883, or 8883 for `ssl`/`tls`/`tcps` schemes.
pub fn parse_mqtt_server_url(server: &str) -> Result<(String, u16)> {
    let s = server.trim();
    if s.is_empty() {
        bail!("MQTT server URL is empty");
    }
    let (scheme, remainder) = match s.split_once("://") {
        Some((scheme, rest)) => (scheme.trim().to_ascii_lowercase(), rest.trim()),
        None => (String::new(), s),
    };
    if remainder.is_empty() {
        bail!("MQTT server URL {:?} has no host", server);
    }
    // Strip any trailing path, query or fragment: host[:port][/...].
    let hostport = remainder
        .split(['/', '?', '#'])
        .next()
        .unwrap_or("")
        .trim();
    if hostport.is_empty() {
        bail!("MQTT server URL {:?} has no host", server);
    }
    let default_port: u16 = match scheme.as_str() {
        "ssl" | "tls" | "tcps" => 8883,
        _ => 1883,
    };
    // Split host and port on the last ':' when the suffix is numeric.
    // Bracketed IPv6 (`[::1]:1883`) is unwrapped to `::1`.
    let (mut host, port) = match hostport.rsplit_once(':') {
        Some((h, p)) if !p.is_empty() && p.chars().all(|c| c.is_ascii_digit()) => {
            let port: u16 = p
                .parse()
                .with_context(|| format!("Invalid port in MQTT server URL {:?}", server))?;
            (h.trim().to_string(), port)
        }
        _ => (hostport.to_string(), default_port),
    };
    if host.starts_with('[') && host.ends_with(']') && host.len() >= 2 {
        host = host[1..host.len() - 1].to_string();
    }
    if host.is_empty() {
        bail!("MQTT server URL {:?} has no host", server);
    }
    Ok((host, port))
}

fn mqtt_options(config: &MqttConfig) -> Result<MqttOptions> {
    let (host, port) = parse_mqtt_server_url(&config.server)?;
    let mut opts = MqttOptions::new(config.effective_client_id(), host, port);
    opts.set_keep_alive(std::time::Duration::from_secs(30));
    if let (Some(u), Some(p)) = (config.username.clone(), config.password.clone()) {
        opts.set_credentials(u, p);
    } else if let Some(u) = config.username.clone() {
        opts.set_credentials(u, String::new());
    }
    Ok(opts)
}

fn spawn_event_loop_driver(mut eventloop: rumqttc::EventLoop) {
    tokio::spawn(async move {
        loop {
            match eventloop.poll().await {
                Ok(_) => {}
                Err(e) => {
                    tracing::warn!("MQTT event loop error: {}", e);
                    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                }
            }
        }
    });
}

/// MQTT sink: publishes each record's `data` as JSON to the configured topic.
pub struct MqttSink {
    pub config: MqttConfig,
    client: AsyncClient,
}

impl MqttSink {
    pub fn new(config: MqttConfig) -> Result<Self> {
        let opts = mqtt_options(&config)?;
        let (client, eventloop) = AsyncClient::new(opts, 64);
        spawn_event_loop_driver(eventloop);
        Ok(Self { config, client })
    }

    pub fn config(&self) -> &MqttConfig {
        &self.config
    }
}

#[async_trait]
impl Sink for MqttSink {
    async fn send(&self, record: &StreamRecord) -> Result<()> {
        let payload = serde_json::to_vec(&record.data)?;
        self.send_raw(payload).await
    }
}

impl MqttSink {
    /// Publishes pre-rendered bytes (e.g. a `dataTemplate` result).
    pub async fn send_raw(&self, payload: Vec<u8>) -> Result<()> {
        self.client
            .publish(
                self.config.topic.clone(),
                self.config.qos_level(),
                false,
                payload,
            )
            .await
            .map_err(|e| anyhow::anyhow!("MQTT publish failed: {}", e))?;
        Ok(())
    }
}

/// Decode a raw MQTT publish payload into a [`StreamRecord`].
///
/// The payload must be a JSON object; its members become the record data.
pub fn decode_mqtt_payload(payload: &[u8]) -> Result<StreamRecord> {
    let value: Value = serde_json::from_slice(payload).context("MQTT payload is not valid JSON")?;
    let obj = value
        .as_object()
        .ok_or_else(|| anyhow::anyhow!("MQTT payload must be a JSON object"))?;
    let data: HashMap<String, Value> = obj.clone().into_iter().collect();
    Ok(StreamRecord::new(data))
}

/// MQTT source: subscribes to the configured topic and forwards each
/// incoming JSON message as a [`StreamRecord`].
pub struct MqttSource {
    pub config: MqttConfig,
}

impl MqttSource {
    pub fn new(config: MqttConfig) -> Self {
        Self { config }
    }

    pub fn config(&self) -> &MqttConfig {
        &self.config
    }

    pub async fn run(self, tx: tokio::sync::mpsc::Sender<StreamRecord>) -> Result<()> {
        let opts = mqtt_options(&self.config)?;
        let (client, mut eventloop) = AsyncClient::new(opts, 64);
        client
            .subscribe(self.config.topic.clone(), self.config.qos_level())
            .await
            .map_err(|e| anyhow::anyhow!("MQTT subscribe failed: {}", e))?;
        loop {
            match eventloop.poll().await {
                Ok(Event::Incoming(Packet::Publish(p))) => {
                    match decode_mqtt_payload(&p.payload) {
                        Ok(record) => {
                            if tx.send(record).await.is_err() {
                                // Receiver dropped; shut down cleanly.
                                return Ok(());
                            }
                        }
                        Err(e) => {
                            tracing::warn!("Skipping invalid MQTT payload: {}", e);
                        }
                    }
                }
                Ok(_) => {}
                Err(e) => {
                    tracing::warn!("MQTT connection error: {}", e);
                    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                }
            }
        }
    }
}

/// HTTP pull source configuration (eKuiper httppull source format).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HttpPullConfig {
    /// Target URL to poll (or stream `DATASOURCE` when no conf key matches).
    pub url: String,
    /// HTTP method, `"get"` (default) or `"post"`.
    #[serde(default = "default_http_method")]
    pub method: String,
    /// Poll interval in milliseconds.
    #[serde(default = "default_http_interval")]
    pub interval: u64,
    /// Extra request headers.
    #[serde(default)]
    pub headers: HashMap<String, String>,
    /// Optional POST body.
    #[serde(default)]
    pub body: Option<String>,
}

fn default_http_method() -> String {
    "get".to_string()
}

fn default_http_interval() -> u64 {
    1000
}

/// HTTP pull source: periodically GETs (or POSTs) a JSON endpoint and
/// broadcasts each response object as a [`StreamRecord`]. A JSON array
/// response yields one record per object element.
pub struct HttpPullSource {
    pub config: HttpPullConfig,
    pub tx: tokio::sync::broadcast::Sender<StreamRecord>,
}

impl HttpPullSource {
    pub fn spawn(self, mut cancel_rx: tokio::sync::watch::Receiver<bool>) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let client = reqwest::Client::new();
            let mut ticker = tokio::time::interval(std::time::Duration::from_millis(
                self.config.interval.max(1),
            ));
            loop {
                tokio::select! {
                    _ = ticker.tick() => {
                        match self.fetch_once(&client).await {
                            Ok(records) => {
                                for record in records {
                                    // All subscribers dropped: shut down cleanly.
                                    if self.tx.send(record).is_err() {
                                        return;
                                    }
                                }
                            }
                            Err(e) => {
                                tracing::warn!(
                                    "HTTP pull source error for {}: {}",
                                    self.config.url,
                                    e
                                );
                            }
                        }
                    }
                    changed = cancel_rx.changed() => {
                        match changed {
                            Ok(_) => {
                                if *cancel_rx.borrow() {
                                    return;
                                }
                            }
                            // Cancellation sender dropped: shut down.
                            Err(_) => return,
                        }
                    }
                }
            }
        })
    }

    async fn fetch_once(&self, client: &reqwest::Client) -> Result<Vec<StreamRecord>> {
        let mut req = if self.config.method.eq_ignore_ascii_case("post") {
            let mut req = client.post(&self.config.url);
            if let Some(body) = &self.config.body {
                req = req.body(body.clone());
            }
            req
        } else {
            client.get(&self.config.url)
        };
        for (name, value) in &self.config.headers {
            req = req.header(name, value);
        }
        let resp = req
            .send()
            .await
            .with_context(|| format!("HTTP pull request to {} failed", self.config.url))?;
        let resp = resp
            .error_for_status()
            .with_context(|| format!("HTTP pull request to {} failed", self.config.url))?;
        let payload: Value = resp
            .json()
            .await
            .with_context(|| format!("HTTP pull response from {} is not JSON", self.config.url))?;
        match payload {
            Value::Object(map) => Ok(vec![StreamRecord::new(map.into_iter().collect())]),
            Value::Array(items) => {
                let mut records = Vec::new();
                for item in items {
                    if let Value::Object(map) = item {
                        records.push(StreamRecord::new(map.into_iter().collect()));
                    } else {
                        tracing::warn!(
                            "Skipping non-object element in HTTP pull response from {}",
                            self.config.url
                        );
                    }
                }
                Ok(records)
            }
            _ => bail!(
                "HTTP pull response from {} must be a JSON object or array",
                self.config.url
            ),
        }
    }
}

/// WebSocket endpoint configuration (eKuiper websocket source/sink format).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WebSocketConfig {
    /// `host:port`, e.g. `"127.0.0.1:8080"`.
    #[serde(default)]
    pub addr: Option<String>,
    /// Alias for [`WebSocketConfig::addr`].
    #[serde(default)]
    pub address: Option<String>,
    /// URL scheme, `"ws"` (default) or `"wss"`.
    #[serde(default = "default_ws_scheme")]
    pub scheme: String,
    /// URL path, e.g. `"/api/data"`.
    #[serde(default = "default_ws_path")]
    pub path: String,
}

fn default_ws_scheme() -> String {
    "ws".to_string()
}

fn default_ws_path() -> String {
    "/".to_string()
}

impl WebSocketConfig {
    pub fn target_url(&self) -> String {
        let host = self
            .addr
            .as_deref()
            .or(self.address.as_deref())
            .unwrap_or("127.0.0.1:8080");
        let mut p = self.path.clone();
        if !p.starts_with('/') {
            p = format!("/{}", p);
        }
        format!("{}://{}{}", self.scheme, host, p)
    }
}

/// Decode one incoming WebSocket text payload into records: a JSON object
/// yields a single [`StreamRecord`], a JSON array yields one record per
/// object element (non-objects are skipped).
pub fn decode_websocket_message(text: &str) -> Result<Vec<StreamRecord>> {
    let value: Value = serde_json::from_str(text).context("WebSocket payload is not valid JSON")?;
    match value {
        Value::Object(map) => Ok(vec![StreamRecord::new(map.into_iter().collect())]),
        Value::Array(items) => {
            let mut records = Vec::new();
            for item in items {
                if let Value::Object(map) = item {
                    records.push(StreamRecord::new(map.into_iter().collect()));
                } else {
                    tracing::warn!("Skipping non-object element in WebSocket message");
                }
            }
            Ok(records)
        }
        _ => anyhow::bail!("WebSocket message must be a JSON object or array"),
    }
}

/// WebSocket source: streams incoming text messages as [`StreamRecord`]s.
pub struct WebSocketSource {
    pub url: String,
    pub tx: tokio::sync::broadcast::Sender<StreamRecord>,
}

impl WebSocketSource {
    pub fn spawn(self, mut cancel_rx: tokio::sync::watch::Receiver<bool>) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let (mut ws_stream, _) = match tokio_tungstenite::connect_async(&self.url).await {
                Ok(conn) => conn,
                Err(e) => {
                    tracing::warn!("WebSocket source connect to {} failed: {}", self.url, e);
                    return;
                }
            };
            use futures::StreamExt;
            loop {
                tokio::select! {
                    msg = ws_stream.next() => {
                        match msg {
                            Some(Ok(tokio_tungstenite::tungstenite::Message::Text(text))) => {
                                match decode_websocket_message(&text) {
                                    Ok(records) => {
                                        for record in records {
                                            // All subscribers dropped: shut down cleanly.
                                            if self.tx.send(record).is_err() {
                                                return;
                                            }
                                        }
                                    }
                                    Err(e) => {
                                        tracing::warn!("Skipping invalid WebSocket message: {}", e);
                                    }
                                }
                            }
                            Some(Ok(_)) => {}
                            Some(Err(e)) => {
                                tracing::warn!("WebSocket source error on {}: {}", self.url, e);
                                break;
                            }
                            None => break,
                        }
                    }
                    changed = cancel_rx.changed() => {
                        match changed {
                            Ok(_) => {
                                if *cancel_rx.borrow() {
                                    return;
                                }
                            }
                            // Cancellation sender dropped: shut down.
                            Err(_) => return,
                        }
                    }
                }
            }
        })
    }
}

/// WebSocket sink: transmits each record as a JSON text message over a fresh
/// connection.
pub struct WebSocketSink {
    pub url: String,
}

impl WebSocketSink {
    pub async fn send(&self, record: &StreamRecord) -> Result<()> {
        let json_str = serde_json::to_string(&record.data)?;
        self.send_text(&json_str).await
    }

    /// Transmits a pre-rendered text payload (e.g. a `dataTemplate` result).
    pub async fn send_text(&self, text: &str) -> Result<()> {
        use futures::SinkExt;
        let (mut ws_stream, _) = tokio_tungstenite::connect_async(&self.url).await?;
        ws_stream
            .send(tokio_tungstenite::tungstenite::Message::Text(text.to_string()))
            .await?;
        let _ = ws_stream.close(None).await;
        Ok(())
    }
}

/// Redis sink configuration (eKuiper redis / redisPub sink format).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RedisSinkConfig {
    /// Server address, e.g. `"127.0.0.1:6379"` (or a full `redis://` URL).
    #[serde(default = "default_redis_addr")]
    pub addr: String,
    /// Optional logical database number.
    #[serde(default)]
    pub db: Option<u32>,
    /// Static key for `SET` (overridden by `field` when both resolve).
    #[serde(default)]
    pub key: Option<String>,
    /// Record field whose value supplies the `SET` key.
    #[serde(default)]
    pub field: Option<String>,
    /// Value encoding, `"string"` (default).
    #[serde(default = "default_redis_datatype")]
    pub data_type: String,
    /// When present the record is `PUBLISH`ed to this topic instead of `SET`.
    #[serde(default)]
    pub topic: Option<String>,
}

fn default_redis_addr() -> String {
    "127.0.0.1:6379".to_string()
}

fn default_redis_datatype() -> String {
    "string".to_string()
}

impl RedisSinkConfig {
    pub fn connection_url(&self) -> String {
        let addr = if self.addr.contains("://") {
            self.addr.clone()
        } else {
            format!("redis://{}", self.addr)
        };
        if let Some(db) = self.db {
            format!("{}/{}", addr.trim_end_matches('/'), db)
        } else {
            addr
        }
    }
}

/// Redis sink: `SET <key> <json>` per record, or `PUBLISH <topic> <json>`
/// when a topic is configured (redisPub action).
pub struct RedisSink {
    pub config: RedisSinkConfig,
}

impl RedisSink {
    async fn connection(&self) -> Result<redis::aio::MultiplexedConnection> {
        let client = redis::Client::open(self.config.connection_url())?;
        let conn = client.get_multiplexed_async_connection().await?;
        Ok(conn)
    }

    pub async fn send(&self, record: &StreamRecord) -> Result<()> {
        let json_str = serde_json::to_string(&record.data)?;
        let mut conn = self.connection().await?;
        if let Some(topic) = self.config.topic.as_deref() {
            redis::cmd("PUBLISH")
                .arg(topic)
                .arg(&json_str)
                .query_async::<()>(&mut conn)
                .await?;
            return Ok(());
        }
        let key = self
            .config
            .field
            .as_deref()
            .and_then(|f| record.data.get(f))
            .map(redis_scalar_key)
            .filter(|k| !k.is_empty())
            .or_else(|| self.config.key.clone())
            .ok_or_else(|| {
                anyhow::anyhow!("Redis sink needs a key: set `field` or `key`")
            })?;
        redis::cmd("SET")
            .arg(&key)
            .arg(&json_str)
            .query_async::<()>(&mut conn)
            .await?;
        Ok(())
    }
}

/// Render a record value as a Redis key fragment.
fn redis_scalar_key(v: &Value) -> String {
    match v {
        Value::Null => String::new(),
        Value::String(s) => s.clone(),
        Value::Number(n) => n.to_string(),
        Value::Bool(b) => b.to_string(),
        other => serde_json::to_string(other).unwrap_or_default(),
    }
}

/// Redis pub/sub source: subscribes to a channel and forwards each JSON
/// payload as [`StreamRecord`]s.
pub struct RedisSubSource {
    pub url: String,
    pub channel: String,
    pub tx: tokio::sync::broadcast::Sender<StreamRecord>,
}

impl RedisSubSource {
    pub fn spawn(self, mut cancel_rx: tokio::sync::watch::Receiver<bool>) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let client = match redis::Client::open(self.url.clone()) {
                Ok(client) => client,
                Err(e) => {
                    tracing::warn!("Redis sub source cannot parse url {}: {}", self.url, e);
                    return;
                }
            };
            let mut pubsub = match client.get_async_pubsub().await {
                Ok(pubsub) => pubsub,
                Err(e) => {
                    tracing::warn!("Redis sub source connect to {} failed: {}", self.url, e);
                    return;
                }
            };
            if let Err(e) = pubsub.subscribe(self.channel.clone()).await {
                tracing::warn!(
                    "Redis sub source subscribe to {} failed: {}",
                    self.channel,
                    e
                );
                return;
            }
            use futures::StreamExt;
            let mut messages = pubsub.on_message();
            loop {
                tokio::select! {
                    msg = messages.next() => {
                        let Some(msg) = msg else {
                            // Message stream ended.
                            break;
                        };
                        let payload: Vec<u8> = match msg.get_payload() {
                            Ok(payload) => payload,
                            Err(e) => {
                                tracing::warn!("Skipping invalid Redis message: {}", e);
                                continue;
                            }
                        };
                        match decode_redis_payload(&payload) {
                            Ok(records) => {
                                for record in records {
                                    // All subscribers dropped: shut down cleanly.
                                    if self.tx.send(record).is_err() {
                                        return;
                                    }
                                }
                            }
                            Err(e) => {
                                tracing::warn!("Skipping invalid Redis payload: {}", e);
                            }
                        }
                    }
                    changed = cancel_rx.changed() => {
                        match changed {
                            Ok(_) => {
                                if *cancel_rx.borrow() {
                                    return;
                                }
                            }
                            // Cancellation sender dropped: shut down.
                            Err(_) => return,
                        }
                    }
                }
            }
        })
    }
}

/// Decode one Redis message payload: a JSON object yields a single record,
/// a JSON array yields one record per object element.
fn decode_redis_payload(payload: &[u8]) -> Result<Vec<StreamRecord>> {
    let value: Value =
        serde_json::from_slice(payload).context("Redis payload is not valid JSON")?;
    match value {
        Value::Object(map) => Ok(vec![StreamRecord::new(map.into_iter().collect())]),
        Value::Array(items) => {
            let mut records = Vec::new();
            for item in items {
                if let Value::Object(map) = item {
                    records.push(StreamRecord::new(map.into_iter().collect()));
                } else {
                    tracing::warn!("Skipping non-object element in Redis message");
                }
            }
            Ok(records)
        }
        _ => anyhow::bail!("Redis message must be a JSON object or array"),
    }
}

/// Point lookup against Redis: `GET <key>`, decoding the stored bytes as
/// JSON when possible and falling back to the raw scalar string.
/// Returns `None` for missing keys.
pub async fn redis_lookup_key(addr: &str, key: &str) -> Result<Option<Value>> {
    let url = if addr.contains("://") {
        addr.to_string()
    } else {
        format!("redis://{}", addr)
    };
    let client = redis::Client::open(url)?;
    let mut conn = client.get_multiplexed_async_connection().await?;
    let stored: Option<Vec<u8>> = redis::cmd("GET")
        .arg(key)
        .query_async(&mut conn)
        .await?;
    match stored {
        None => Ok(None),
        Some(bytes) => match serde_json::from_slice::<Value>(&bytes) {
            Ok(value) => Ok(Some(value)),
            Err(_) => Ok(Some(Value::String(
                String::from_utf8_lossy(&bytes).into_owned(),
            ))),
        },
    }
}

/// Kafka connection settings (eKuiper kafka source/sink format).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct KafkaConfig {
    /// Comma-separated broker addresses, e.g. `"127.0.0.1:9092"`.
    #[serde(default = "default_kafka_brokers")]
    pub brokers: String,
    /// Topic to consume from / produce to (source streams take it from
    /// `DATASOURCE` when absent here).
    #[serde(default)]
    pub topic: Option<String>,
    /// Consumer group id (source side informational).
    #[serde(default)]
    pub group_id: Option<String>,
    /// Partition number (default 0).
    #[serde(default)]
    pub partition: i32,
    /// Record field supplying the produce message key (sink side).
    #[serde(default)]
    pub key: Option<String>,
}

fn default_kafka_brokers() -> String {
    "127.0.0.1:9092".to_string()
}

impl KafkaConfig {
    pub fn broker_list(&self) -> Vec<String> {
        self.brokers
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect()
    }
}

/// Bounded retry policy for Kafka control-plane calls: without a deadline
/// rskafka backs off indefinitely, which would wedge rule tasks and tests
/// when brokers are unreachable.
fn kafka_backoff_config() -> rskafka::BackoffConfig {
    rskafka::BackoffConfig {
        init_backoff: std::time::Duration::from_millis(100),
        max_backoff: std::time::Duration::from_secs(1),
        base: 2.0,
        deadline: Some(std::time::Duration::from_secs(5)),
    }
}

/// Kafka sink: serializes each record as JSON and produces it to the
/// configured topic/partition, keyed by the optional `key` field.
pub struct KafkaSink {
    pub config: KafkaConfig,
}

impl KafkaSink {
    pub async fn send(&self, record: &StreamRecord) -> Result<()> {
        // Bound the whole produce path: rskafka retries control-plane calls
        // internally, so without this an unreachable broker would wedge the
        // caller instead of surfacing a countable error.
        match tokio::time::timeout(std::time::Duration::from_secs(10), self.send_inner(record))
            .await
        {
            Ok(res) => res,
            Err(_) => anyhow::bail!(
                "Kafka produce to {} timed out",
                self.config.topic.as_deref().unwrap_or("default")
            ),
        }
    }

    async fn send_inner(&self, record: &StreamRecord) -> Result<()> {
        use rskafka::client::{
            partition::{Compression, UnknownTopicHandling},
            ClientBuilder,
        };
        use rskafka::record::Record;
        let brokers = self.config.broker_list();
        if brokers.is_empty() {
            anyhow::bail!("Kafka sink needs at least one broker");
        }
        let client = ClientBuilder::new(brokers)
            .backoff_config(kafka_backoff_config())
            .build()
            .await?;
        let topic = self.config.topic.as_deref().unwrap_or("default");
        let partition_client = client
            .partition_client(topic, self.config.partition, UnknownTopicHandling::Error)
            .await?;
        let json_bytes = serde_json::to_vec(&record.data)?;
        let key_bytes = self
            .config
            .key
            .as_ref()
            .and_then(|k| record.data.get(k))
            .map(|v| value_to_key_bytes(v));
        let kafka_record = Record {
            key: key_bytes,
            value: Some(json_bytes),
            headers: std::collections::BTreeMap::new(),
            timestamp: chrono::Utc::now(),
        };
        partition_client
            .produce(vec![kafka_record], Compression::NoCompression)
            .await?;
        Ok(())
    }
}

/// Render a record value as Kafka message key bytes.
fn value_to_key_bytes(v: &Value) -> Vec<u8> {
    match v {
        Value::Null => Vec::new(),
        Value::String(s) => s.clone().into_bytes(),
        Value::Number(n) => n.to_string().into_bytes(),
        Value::Bool(b) => b.to_string().into_bytes(),
        other => serde_json::to_vec(other).unwrap_or_default(),
    }
}

/// Kafka source: consumes a topic partition from the latest offset and
/// forwards each JSON payload as [`StreamRecord`]s.
pub struct KafkaSource {
    pub config: KafkaConfig,
    pub tx: tokio::sync::broadcast::Sender<StreamRecord>,
}

impl KafkaSource {
    pub fn spawn(self, mut cancel_rx: tokio::sync::watch::Receiver<bool>) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            use rskafka::client::{
                partition::{OffsetAt, UnknownTopicHandling},
                ClientBuilder,
            };
            let brokers = self.config.broker_list();
            if brokers.is_empty() {
                tracing::warn!("Kafka source has no brokers configured");
                return;
            }
            let client = match ClientBuilder::new(brokers)
                .backoff_config(kafka_backoff_config())
                .build()
                .await
            {
                Ok(client) => client,
                Err(e) => {
                    tracing::warn!("Kafka source connect failed: {}", e);
                    return;
                }
            };
            let topic = self.config.topic.as_deref().unwrap_or("default");
            let partition_client = match client
                .partition_client(topic, self.config.partition, UnknownTopicHandling::Error)
                .await
            {
                Ok(partition_client) => partition_client,
                Err(e) => {
                    tracing::warn!("Kafka source partition client failed: {}", e);
                    return;
                }
            };
            let mut offset = partition_client
                .get_offset(OffsetAt::Latest)
                .await
                .unwrap_or(0);
            loop {
                tokio::select! {
                    fetched = partition_client.fetch_records(offset, 1..1_000_000, 1000) => {
                        match fetched {
                            Ok((records, _high_watermark)) => {
                                for rec in records {
                                    offset = rec.offset + 1;
                                    let payload = rec.record.value.as_deref().unwrap_or_default();
                                    match decode_kafka_payload(payload) {
                                        Ok(decoded) => {
                                            for record in decoded {
                                                // All subscribers dropped: shut down cleanly.
                                                if self.tx.send(record).is_err() {
                                                    return;
                                                }
                                            }
                                        }
                                        Err(e) => {
                                            tracing::warn!("Skipping invalid Kafka payload: {}", e);
                                        }
                                    }
                                }
                            }
                            Err(e) => {
                                tracing::warn!("Kafka source fetch failed: {}", e);
                                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                            }
                        }
                    }
                    changed = cancel_rx.changed() => {
                        match changed {
                            Ok(_) => {
                                if *cancel_rx.borrow() {
                                    return;
                                }
                            }
                            // Cancellation sender dropped: shut down.
                            Err(_) => return,
                        }
                    }
                }
            }
        })
    }
}

/// Decode one Kafka record payload: a JSON object yields a single record, a
/// JSON array yields one record per object element.
fn decode_kafka_payload(payload: &[u8]) -> Result<Vec<StreamRecord>> {
    let value: Value =
        serde_json::from_slice(payload).context("Kafka payload is not valid JSON")?;
    match value {
        Value::Object(map) => Ok(vec![StreamRecord::new(map.into_iter().collect())]),
        Value::Array(items) => {
            let mut records = Vec::new();
            for item in items {
                if let Value::Object(map) = item {
                    records.push(StreamRecord::new(map.into_iter().collect()));
                } else {
                    tracing::warn!("Skipping non-object element in Kafka message");
                }
            }
            Ok(records)
        }
        _ => anyhow::bail!("Kafka message must be a JSON object or array"),
    }
}

fn default_true() -> bool {
    true
}

fn default_interval_ms() -> Value {
    Value::from(10)
}

/// Interval for [`SimulatorSource`]: integer milliseconds (e.g. `10`) or a
/// duration string (e.g. `"10ms"`, `"1s"`).
pub fn parse_interval_ms(interval: &Value) -> std::time::Duration {
    const DEFAULT: std::time::Duration = std::time::Duration::from_millis(10);
    match interval {
        Value::Number(n) => {
            if let Some(ms) = n.as_u64() {
                std::time::Duration::from_millis(ms)
            } else if let Some(ms) = n.as_i64() {
                if ms >= 0 {
                    std::time::Duration::from_millis(ms as u64)
                } else {
                    DEFAULT
                }
            } else if let Some(f) = n.as_f64() {
                if f.is_finite() && f >= 0.0 {
                    std::time::Duration::from_millis(f as u64)
                } else {
                    DEFAULT
                }
            } else {
                DEFAULT
            }
        }
        Value::String(s) => parse_duration_str(s).unwrap_or(DEFAULT),
        _ => DEFAULT,
    }
}

fn parse_duration_str(s: &str) -> Option<std::time::Duration> {
    let s = s.trim();
    if s.is_empty() {
        return None;
    }
    let idx = s.find(|c: char| c.is_alphabetic()).unwrap_or(s.len());
    let (num_part, unit_part) = s.split_at(idx);
    if num_part.trim().is_empty() {
        return None;
    }
    let n: f64 = num_part.trim().parse().ok()?;
    if !n.is_finite() || n < 0.0 {
        return None;
    }
    let mult_ms: f64 = match unit_part.trim().to_ascii_lowercase().as_str() {
        "" | "ms" => 1.0,
        "ns" => 1.0 / 1_000_000.0,
        "us" | "µs" => 1.0 / 1_000.0,
        "s" => 1_000.0,
        "m" => 60_000.0,
        "h" => 3_600_000.0,
        _ => return None,
    };
    Some(std::time::Duration::from_millis((n * mult_ms) as u64))
}

/// Simulator source configuration (eKuiper simulator source format).
#[derive(Debug, Clone, Deserialize)]
pub struct SimulatorConfig {
    /// Sequence of records to emit, in order.
    #[serde(default)]
    pub data: Vec<HashMap<String, Value>>,
    /// Emit interval: integer milliseconds or duration string. Defaults to 10ms.
    #[serde(default = "default_interval_ms")]
    pub interval: Value,
    /// Repeat the sequence indefinitely until the receiver is dropped.
    #[serde(rename = "loop", default = "default_true")]
    pub loop_data: bool,
}

/// Simulator source: replays a fixed sequence of records at a fixed interval.
///
/// With `loop_data: true` the sequence repeats until the receiver is closed;
/// with `false` a single pass is emitted and the total count is returned.
pub struct SimulatorSource {
    pub config: SimulatorConfig,
}

impl SimulatorSource {
    pub fn new(config: SimulatorConfig) -> Self {
        Self { config }
    }

    pub async fn run(self, sender: tokio::sync::mpsc::Sender<StreamRecord>) -> usize {
        if self.config.data.is_empty() {
            return 0;
        }
        let interval = parse_interval_ms(&self.config.interval);
        let mut sent = 0usize;
        loop {
            for item in &self.config.data {
                if sender
                    .send(StreamRecord::new(item.clone()))
                    .await
                    .is_err()
                {
                    return sent;
                }
                sent += 1;
                tokio::time::sleep(interval).await;
            }
            if !self.config.loop_data {
                break;
            }
        }
        sent
    }
}

/// Delimited-format parser (eKuiper delimited serialization):
/// splits `line` on `delimiter`, trims each token and maps it to the
/// corresponding header, converting numeric tokens to [`Value::Number`].
pub fn parse_delimited_line(
    line: &str,
    delimiter: char,
    headers: &[String],
) -> HashMap<String, Value> {
    let mut map = HashMap::with_capacity(headers.len());
    for (i, header) in headers.iter().enumerate() {
        let token = line.split(delimiter).nth(i).unwrap_or("").trim();
        map.insert(header.clone(), parse_delimited_token(token));
    }
    map
}

fn parse_delimited_token(token: &str) -> Value {
    if let Ok(i) = token.parse::<i64>() {
        return Value::from(i);
    }
    if let Ok(f) = token.parse::<f64>() {
        if let Some(n) = serde_json::Number::from_f64(f) {
            return Value::Number(n);
        }
    }
    Value::String(token.to_string())
}

// ---------------------------------------------------------------------------
// Sink data templates and SQL connectors (SQLite / PostgreSQL via sqlx).
// ---------------------------------------------------------------------------

/// Render a sink `dataTemplate` such as
/// `{"device": "{{.id}}", "celsius": {{.temp}}}` by substituting `{{.field}}`
/// placeholders from the record data (strings raw, other values as JSON).
pub fn apply_data_template(template: &str, data: &serde_json::Map<String, serde_json::Value>) -> String {
    let mut result = template.to_string();
    for (k, v) in data {
        let placeholder = format!("{{{{.{}}}}}", k);
        let val_str = match v {
            serde_json::Value::String(s) => s.clone(),
            other => other.to_string(),
        };
        result = result.replace(&placeholder, &val_str);
    }
    result
}

/// SQL connector configuration (SQLite / PostgreSQL sink, lookup and
/// polling source).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SqlConnectorConfig {
    /// Database URL, e.g. `"sqlite::memory:"` or
    /// `"postgres://user:pass@localhost:5432/db"`.
    pub url: String,
    /// Target table name.
    pub table: String,
    /// Columns to write (defaults to the record's own keys).
    #[serde(default)]
    pub fields: Vec<String>,
    /// Poll interval in milliseconds (polling sources).
    #[serde(default = "default_sql_interval")]
    pub interval: u64,
}

fn default_sql_interval() -> u64 {
    1000
}

/// SQL sink: executes parameterized row inserts into the database.
/// Currently SQLite URLs are executed; other schemes return `Ok(())`.
pub struct SqlSink {
    pub config: SqlConnectorConfig,
}

impl SqlSink {
    pub async fn insert_record(&self, record: &StreamRecord) -> Result<()> {
        if self.config.url.starts_with("sqlite") {
            let pool = sqlx::sqlite::SqlitePool::connect(&self.config.url).await?;
            let fields = if self.config.fields.is_empty() {
                record.data.keys().cloned().collect::<Vec<_>>()
            } else {
                self.config.fields.clone()
            };
            let cols = fields.join(", ");
            let placeholders = vec!["?"; fields.len()].join(", ");
            let sql = format!("INSERT INTO {} ({}) VALUES ({})", self.config.table, cols, placeholders);
            let mut query = sqlx::query(&sql);
            for f in &fields {
                let val = record.data.get(f).map(|v| match v {
                    serde_json::Value::String(s) => s.clone(),
                    other => other.to_string(),
                }).unwrap_or_default();
                query = query.bind(val);
            }
            query.execute(&pool).await?;
        }
        Ok(())
    }
}

/// Point lookup against a SQL database: `SELECT * ... WHERE key_col = ?
/// LIMIT 1`, mapping the row columns to string values. Returns `None` when
/// no row matches (or for non-SQLite URLs).
pub async fn sql_lookup_key(url: &str, table: &str, key_col: &str, key_val: &str) -> Result<Option<serde_json::Value>> {
    if url.starts_with("sqlite") {
        let pool = sqlx::sqlite::SqlitePool::connect(url).await?;
        let sql = format!("SELECT * FROM {} WHERE {} = ? LIMIT 1", table, key_col);
        let row = sqlx::query(&sql).bind(key_val).fetch_optional(&pool).await?;
        if let Some(r) = row {
            use sqlx::{Column, Row};
            let mut map = serde_json::Map::new();
            for col in r.columns() {
                let name = col.name();
                let val: String = r.try_get(name).unwrap_or_default();
                map.insert(name.to_string(), serde_json::Value::String(val));
            }
            return Ok(Some(serde_json::Value::Object(map)));
        }
    }
    Ok(None)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use serde_json::json;

    #[tokio::test]
    async fn test_kafka_sink_unreachable_errors() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);
        let sink = KafkaSink {
            config: KafkaConfig {
                brokers: format!("127.0.0.1:{}", port),
                topic: Some("t".to_string()),
                group_id: None,
                partition: 0,
                key: None,
            },
        };
        let mut data = HashMap::new();
        data.insert("id".to_string(), json!("k1"));
        // Must fail (not hang): the sink bounds the produce path.
        let res = tokio::time::timeout(
            std::time::Duration::from_secs(25),
            sink.send(&StreamRecord::new(data)),
        )
        .await
        .expect("kafka send hung past its internal timeout");
        assert!(res.is_err());
    }

    #[test]
    fn test_apply_data_template() {
        let mut data = serde_json::Map::new();
        data.insert("id".to_string(), json!("d1"));
        data.insert("temp".to_string(), json!(25.5));
        assert_eq!(
            apply_data_template("device: {{.id}}, temp: {{.temp}}", &data),
            "device: d1, temp: 25.5"
        );
        // Unknown placeholders are left untouched.
        assert_eq!(apply_data_template("x={{.missing}}", &data), "x={{.missing}}");
    }

    #[test]
    fn test_sql_connector_config_defaults() {
        let cfg: SqlConnectorConfig =
            serde_json::from_value(json!({"url": "sqlite::memory:", "table": "alerts"})).unwrap();
        assert_eq!(cfg.url, "sqlite::memory:");
        assert_eq!(cfg.table, "alerts");
        assert!(cfg.fields.is_empty());
        assert_eq!(cfg.interval, 1000);
    }

    #[test]
    fn test_kafka_config_defaults() {
        let cfg: KafkaConfig = serde_json::from_value(json!({})).unwrap();
        assert_eq!(cfg.brokers, "127.0.0.1:9092");
        assert_eq!(cfg.broker_list(), vec!["127.0.0.1:9092".to_string()]);
        assert_eq!(cfg.topic, None);
        assert_eq!(cfg.partition, 0);

        let cfg: KafkaConfig =
            serde_json::from_value(json!({"brokers": "a:9092, b:9092 ", "topic": "events"}))
                .unwrap();
        assert_eq!(
            cfg.broker_list(),
            vec!["a:9092".to_string(), "b:9092".to_string()]
        );
        assert_eq!(cfg.topic.as_deref(), Some("events"));
    }

    #[test]
    fn test_redis_sink_config_defaults() {
        let cfg: RedisSinkConfig =
            serde_json::from_value(json!({"field": "id"})).unwrap();
        assert_eq!(cfg.addr, "127.0.0.1:6379");
        assert_eq!(cfg.connection_url(), "redis://127.0.0.1:6379");
        assert_eq!(cfg.data_type, "string");
        assert_eq!(cfg.topic, None);
        let with_db: RedisSinkConfig =
            serde_json::from_value(json!({"addr": "127.0.0.1:6379", "db": 2})).unwrap();
        assert_eq!(with_db.connection_url(), "redis://127.0.0.1:6379/2");
    }

    #[test]
    fn test_http_pull_config_defaults() {
        let cfg: HttpPullConfig =
            serde_json::from_value(json!({"url": "http://127.0.0.1:8080/data"})).unwrap();
        assert_eq!(cfg.url, "http://127.0.0.1:8080/data");
        assert_eq!(cfg.method, "get");
        assert_eq!(cfg.interval, 1000);
        assert!(cfg.headers.is_empty());
        assert_eq!(cfg.body, None);
    }

    #[test]
    fn test_websocket_target_url() {
        let cfg: WebSocketConfig = serde_json::from_value(json!({
            "addr": "127.0.0.1:9001",
            "path": "/ws_sink",
        }))
        .unwrap();
        assert_eq!(cfg.scheme, "ws");
        assert_eq!(cfg.target_url(), "ws://127.0.0.1:9001/ws_sink");

        // `address` is accepted as an alias and missing slashes are fixed.
        let cfg: WebSocketConfig = serde_json::from_value(json!({
            "address": "example.com:443",
            "scheme": "wss",
            "path": "api/data",
        }))
        .unwrap();
        assert_eq!(cfg.target_url(), "wss://example.com:443/api/data");

        // Bare defaults point at the local endpoint root.
        let cfg: WebSocketConfig = serde_json::from_value(json!({})).unwrap();
        assert_eq!(cfg.target_url(), "ws://127.0.0.1:8080/");
    }

    fn unique_temp_path(tag: &str) -> PathBuf {
        use std::time::{SystemTime, UNIX_EPOCH};
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        let mut p = std::env::temp_dir();
        p.push(format!(
            "rekuiper-{}-{}-{}.ldjson",
            tag,
            std::process::id(),
            nanos
        ));
        p
    }

    #[tokio::test]
    async fn test_file_sink_roundtrip() {
        let path = unique_temp_path("filesink");
        let sink = FileSink::new(&path);

        let mut d1 = HashMap::new();
        d1.insert("temp".to_string(), json!(25.0));
        let mut d2 = HashMap::new();
        d2.insert("temp".to_string(), json!(30.0));
        d2.insert("status".to_string(), json!("ok"));

        sink.send(&StreamRecord::new(d1.clone())).await.unwrap();
        sink.send(&StreamRecord::new(d2.clone())).await.unwrap();

        // Read raw file back: two newline-terminated JSON lines.
        let content = tokio::fs::read_to_string(&path).await.unwrap();
        assert!(content.ends_with('\n'));
        let lines: Vec<&str> = content.lines().collect();
        assert_eq!(lines.len(), 2);
        let v0: Value = serde_json::from_str(lines[0]).unwrap();
        let v1: Value = serde_json::from_str(lines[1]).unwrap();
        assert_eq!(v0["temp"], json!(25.0));
        assert_eq!(v1["temp"], json!(30.0));
        assert_eq!(v1["status"], json!("ok"));

        // FileSource reads the same file back into StreamRecords.
        let source = FileSource::new(&path);
        let records = source.read_records().await.unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].data.get("temp"), Some(&json!(25.0)));
        assert_eq!(records[1].data.get("status"), Some(&json!("ok")));

        let _ = tokio::fs::remove_file(&path).await;
    }

    #[test]
    fn test_parse_mqtt_server_url() {
        assert_eq!(
            super::parse_mqtt_server_url("tcp://127.0.0.1:1883").unwrap(),
            ("127.0.0.1".to_string(), 1883)
        );
        assert_eq!(
            super::parse_mqtt_server_url("127.0.0.1:1883").unwrap(),
            ("127.0.0.1".to_string(), 1883)
        );
        assert_eq!(
            super::parse_mqtt_server_url("ssl://broker.emqx.io:8883").unwrap(),
            ("broker.emqx.io".to_string(), 8883)
        );
        // Scheme defaults.
        assert_eq!(
            super::parse_mqtt_server_url("tcp://127.0.0.1").unwrap(),
            ("127.0.0.1".to_string(), 1883)
        );
        assert_eq!(
            super::parse_mqtt_server_url("ssl://broker.emqx.io").unwrap(),
            ("broker.emqx.io".to_string(), 8883)
        );
    }

    #[test]
    fn test_mqtt_config_serde() {
        let json = serde_json::json!({
            "server": "tcp://127.0.0.1:1883",
            "topic": "devices/result",
            "qos": 1,
            "clientId": "demo_001"
        });
        let cfg: super::MqttConfig = serde_json::from_value(json).unwrap();
        assert_eq!(cfg.server, "tcp://127.0.0.1:1883");
        assert_eq!(cfg.topic, "devices/result");
        assert_eq!(cfg.qos, 1);
        assert_eq!(cfg.client_id.as_deref(), Some("demo_001"));
        assert_eq!(cfg.username, None);
        assert_eq!(cfg.password, None);

        // Serializing preserves the eKuiper `clientId` field name.
        let ser = serde_json::to_value(&cfg).unwrap();
        assert_eq!(ser["server"], json!("tcp://127.0.0.1:1883"));
        assert_eq!(ser["topic"], json!("devices/result"));
        assert_eq!(ser["qos"], json!(1));
        assert_eq!(ser["clientId"], json!("demo_001"));

        // Round-trip back into the same config.
        let back: super::MqttConfig = serde_json::from_value(ser).unwrap();
        assert_eq!(back, cfg);
    }

    #[tokio::test]
    async fn test_nop_sink() {
        let sink = super::NopSink;
        let mut data = HashMap::new();
        data.insert("temp".to_string(), json!(25.0));
        sink.send(&StreamRecord::new(data)).await.unwrap();
    }

    #[tokio::test]
    async fn test_simulator_source() {
        let config: super::SimulatorConfig = serde_json::from_value(json!({
            "data": [{"temp": 10.0}, {"temp": 20.0}, {"temp": 30.0}],
            "interval": 1,
            "loop": false,
        }))
        .unwrap();
        assert!(!config.loop_data);

        // `loop` defaults to true when missing.
        let defaulted: super::SimulatorConfig =
            serde_json::from_value(json!({"data": []})).unwrap();
        assert!(defaulted.loop_data);

        let (tx, mut rx) = tokio::sync::mpsc::channel(10);
        let count = super::SimulatorSource::new(config).run(tx).await;
        assert_eq!(count, 3);

        let mut temps = Vec::new();
        while let Some(record) = rx.recv().await {
            temps.push(record.data.get("temp").cloned().unwrap());
        }
        assert_eq!(temps, vec![json!(10.0), json!(20.0), json!(30.0)]);
    }

    #[test]
    fn test_delimited_parsing() {
        let headers = ["temp", "humidity", "status"]
            .iter()
            .map(|s| s.to_string())
            .collect::<Vec<_>>();
        let map = super::parse_delimited_line("22.5,50,ok", ',', &headers);
        assert_eq!(map.get("temp"), Some(&json!(22.5)));
        assert!(map.get("temp").unwrap().is_number());
        assert_eq!(map.get("humidity"), Some(&json!(50)));
        assert!(map.get("humidity").unwrap().is_number());
        assert_eq!(map.get("status"), Some(&json!("ok")));
        assert!(map.get("status").unwrap().is_string());
    }

    #[test]
    fn test_mqtt_payload_decode() {
        let payload = br#"{"temp": 25.0, "status": "ok"}"#;
        let record = super::decode_mqtt_payload(payload).unwrap();
        assert_eq!(record.data.get("temp"), Some(&json!(25.0)));
        assert_eq!(record.data.get("status"), Some(&json!("ok")));

        // Non-object payloads are rejected.
        assert!(super::decode_mqtt_payload(br#"[1, 2]"#).is_err());
        assert!(super::decode_mqtt_payload(br#"not json"#).is_err());
    }
}
