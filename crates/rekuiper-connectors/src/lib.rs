use anyhow::{bail, Context, Result};
use async_trait::async_trait;
use parking_lot::RwLock;
use rekuiper_core::model::StreamRecord;
use rumqttc::{AsyncClient, Event, MqttOptions, Packet, QoS};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::io::AsyncBufReadExt;

pub mod codec;
pub use codec::*;

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
/// Streams line-delimited records from a file into [`StreamRecord`]s,
/// decoding each line per the configured [`FileSourceConfig`] format.
pub struct FileSource {
    pub config: FileSourceConfig,
    pub tx: tokio::sync::broadcast::Sender<StreamRecord>,
}

impl FileSource {
    pub fn new(config: FileSourceConfig, tx: tokio::sync::broadcast::Sender<StreamRecord>) -> Self {
        Self { config, tx }
    }

    pub fn path(&self) -> &Path {
        Path::new(&self.config.path)
    }

    /// Read all records from the file without sending them anywhere.
    pub async fn read_records(&self) -> Result<Vec<StreamRecord>> {
        let content = tokio::fs::read_to_string(&self.config.path)
            .await
            .with_context(|| format!("Failed to read file {:?}", self.config.path))?;
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
///
/// `server` and `topic` both default: source `CONF_KEY` entries typically
/// carry only connection parameters (no topic), and must still deserialize
/// so the stored broker URL is honored instead of silently falling back.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MqttConfig {
    /// Broker URL, e.g. `tcp://127.0.0.1:1883`.
    #[serde(default = "default_mqtt_server")]
    pub server: String,
    /// Topic to publish to / subscribe to.
    #[serde(default)]
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

fn default_mqtt_server() -> String {
    "tcp://127.0.0.1:1883".to_string()
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
    let hostport = remainder.split(['/', '?', '#']).next().unwrap_or("").trim();
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
    pub tx: tokio::sync::broadcast::Sender<StreamRecord>,
}

impl MqttSource {
    pub fn new(config: MqttConfig, tx: tokio::sync::broadcast::Sender<StreamRecord>) -> Self {
        Self { config, tx }
    }

    pub fn config(&self) -> &MqttConfig {
        &self.config
    }

    pub fn spawn(
        self,
        mut cancel_rx: tokio::sync::watch::Receiver<bool>,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            tracing::info!(
                "[MQTT SOURCE] Connecting to broker '{}', topic '{}', qos {}",
                self.config.server,
                self.config.topic,
                self.config.qos
            );
            let opts = match mqtt_options(&self.config) {
                Ok(opts) => opts,
                Err(e) => {
                    tracing::warn!("MQTT source bad broker config: {}", e);
                    return;
                }
            };
            let (client, mut eventloop) = AsyncClient::new(opts, 64);
            if let Err(e) = client
                .subscribe(self.config.topic.clone(), self.config.qos_level())
                .await
            {
                tracing::warn!("MQTT subscribe to {} failed: {}", self.config.topic, e);
                return;
            }
            loop {
                tokio::select! {
                    event = eventloop.poll() => {
                        match event {
                            Ok(Event::Incoming(Packet::Publish(p))) => {
                                match decode_mqtt_payload(&p.payload) {
                                    Ok(record) => {
                                        // Ignore SendError: subscribers dropped.
                                        let _ = self.tx.send(record);
                                    }
                                    Err(e) => {
                                        tracing::warn!("Skipping invalid MQTT payload: {}", e);
                                    }
                                }
                            }
                            // rumqttc does not restore subscriptions across
                            // reconnects: every (re)connect handshake must
                            // re-subscribe or the source goes silently deaf
                            // after a broker restart.
                            Ok(Event::Incoming(Packet::ConnAck(_))) => {
                                if let Err(e) = client
                                    .subscribe(self.config.topic.clone(), self.config.qos_level())
                                    .await
                                {
                                    tracing::warn!(
                                        "MQTT resubscribe to {} failed: {}",
                                        self.config.topic,
                                        e
                                    );
                                }
                            }
                            Ok(_) => {}
                            Err(e) => {
                                tracing::warn!("MQTT connection error: {}", e);
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
                // See `spawn`: subscriptions die with the connection.
                Ok(Event::Incoming(Packet::ConnAck(_))) => {
                    client
                        .subscribe(self.config.topic.clone(), self.config.qos_level())
                        .await
                        .map_err(|e| anyhow::anyhow!("MQTT resubscribe failed: {}", e))?;
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
    pub fn spawn(
        self,
        mut cancel_rx: tokio::sync::watch::Receiver<bool>,
    ) -> tokio::task::JoinHandle<()> {
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
    pub fn spawn(
        self,
        mut cancel_rx: tokio::sync::watch::Receiver<bool>,
    ) -> tokio::task::JoinHandle<()> {
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
            .send(tokio_tungstenite::tungstenite::Message::Text(
                text.to_string(),
            ))
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
            .ok_or_else(|| anyhow::anyhow!("Redis sink needs a key: set `field` or `key`"))?;
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
    pub fn spawn(
        self,
        mut cancel_rx: tokio::sync::watch::Receiver<bool>,
    ) -> tokio::task::JoinHandle<()> {
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
    let stored: Option<Vec<u8>> = redis::cmd("GET").arg(key).query_async(&mut conn).await?;
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
            .map(value_to_key_bytes);
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
    pub fn spawn(
        self,
        mut cancel_rx: tokio::sync::watch::Receiver<bool>,
    ) -> tokio::task::JoinHandle<()> {
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
                if sender.send(StreamRecord::new(item.clone())).await.is_err() {
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

/// File source configuration (eKuiper file source format).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FileSourceConfig {
    /// File path or directory path to read (from DATASOURCE or config).
    pub path: String,
    /// Format / file type: "json" (default), "lines", "delimited", or "csv".
    #[serde(default = "default_file_source_format")]
    pub format: String,
    /// Whether the first row is a header for delimited/csv format.
    #[serde(default)]
    pub has_header: bool,
    /// Delimiter character for CSV/TSV (default ',').
    #[serde(default, deserialize_with = "deserialize_delimiter")]
    pub delimiter: Option<char>,
    /// Optional pacing interval between emitted records in milliseconds
    /// (defaults to 0 for immediate replay).
    #[serde(default)]
    pub interval: u64,
}

fn default_file_source_format() -> String {
    "json".to_string()
}

/// Accepts a single delimiter character (`","`, `"\t"`, `"|"`) as well as
/// friendly names (`"comma"`, `"tab"`, `"pipe"`, `"semicolon"`).
fn deserialize_delimiter<'de, D>(deserializer: D) -> Result<Option<char>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let opt: Option<String> = Option::deserialize(deserializer)?;
    Ok(opt.map(|s| crate::codec::DelimitedCodec::delimiter_from_name(&s)))
}

/// Streaming reader: [`FileSource::spawn`] tails the file, decoding each
/// line per format — JSON lines as objects (arrays fan out per element),
/// delimited/CSV rows mapped positionally onto headers (using the first row
/// when `has_header` is set) — until cancelled.
impl FileSource {
    pub fn spawn(
        self,
        mut cancel_rx: tokio::sync::watch::Receiver<bool>,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let file = match tokio::fs::File::open(&self.config.path).await {
                Ok(file) => file,
                Err(e) => {
                    tracing::warn!("File source cannot open {}: {}", self.config.path, e);
                    return;
                }
            };
            let mut lines = tokio::io::BufReader::new(file).lines();
            let delimiter = self.config.delimiter.unwrap_or(',');
            let mut headers: Option<Vec<String>> = None;
            loop {
                tokio::select! {
                    line_res = lines.next_line() => {
                        match line_res {
                            Ok(Some(line)) => {
                                let line = line.trim();
                                if line.is_empty() {
                                    continue;
                                }
                                if let Some(records) =
                                    Self::decode_line(&self.config, &mut headers, delimiter, line)
                                {
                                    for record in records {
                                        // All subscribers dropped: shut down cleanly.
                                        if self.tx.send(record).is_err() {
                                            return;
                                        }
                                    }
                                }
                                if self.config.interval > 0 {
                                    tokio::select! {
                                        _ = tokio::time::sleep(std::time::Duration::from_millis(
                                            self.config.interval,
                                        )) => {}
                                        exit = Self::is_cancelled(&mut cancel_rx) => {
                                            if exit {
                                                return;
                                            }
                                        }
                                    }
                                }
                            }
                            Ok(None) => {
                                // EOF: pause before re-polling (tail -f) while
                                // staying responsive to cancellation.
                                tokio::select! {
                                    _ = tokio::time::sleep(std::time::Duration::from_millis(500)) => {}
                                    exit = Self::is_cancelled(&mut cancel_rx) => {
                                        if exit {
                                            return;
                                        }
                                    }
                                }
                            }
                            Err(e) => {
                                tracing::warn!(
                                    "File source read error on {}: {}",
                                    self.config.path,
                                    e
                                );
                                break;
                            }
                        }
                    }
                    exit = Self::is_cancelled(&mut cancel_rx) => {
                        if exit {
                            return;
                        }
                    }
                }
            }
        })
    }

    async fn is_cancelled(cancel_rx: &mut tokio::sync::watch::Receiver<bool>) -> bool {
        match cancel_rx.changed().await {
            Ok(_) => *cancel_rx.borrow(),
            // Cancellation sender dropped: shut down.
            Err(_) => true,
        }
    }

    /// Decode one non-empty line according to the configured format.
    /// Returns `None` for header rows (consumed, not emitted).
    fn decode_line(
        config: &FileSourceConfig,
        headers: &mut Option<Vec<String>>,
        delimiter: char,
        line: &str,
    ) -> Option<Vec<StreamRecord>> {
        if config.format.eq_ignore_ascii_case("delimited")
            || config.format.eq_ignore_ascii_case("csv")
        {
            if config.has_header && headers.is_none() {
                *headers = Some(
                    line.split(delimiter)
                        .map(|h| h.trim().trim_matches('"').to_string())
                        .collect(),
                );
                return None;
            }
            let headers: Vec<String> = match headers {
                Some(h) => h.clone(),
                None => {
                    tracing::warn!("Delimited file source has no headers; skipping line");
                    return Some(Vec::new());
                }
            };
            let data = parse_delimited_line(line, delimiter, &headers);
            return Some(vec![StreamRecord::new(data)]);
        }
        // JSON / lines: objects emit directly, arrays fan out per element.
        match serde_json::from_str::<Value>(line) {
            Ok(Value::Object(map)) => Some(vec![StreamRecord::new(map.into_iter().collect())]),
            Ok(Value::Array(items)) => {
                let mut records = Vec::new();
                for item in items {
                    if let Value::Object(map) = item {
                        records.push(StreamRecord::new(map.into_iter().collect()));
                    } else {
                        tracing::warn!("Skipping non-object element in file source line");
                    }
                }
                Some(records)
            }
            Ok(_) => {
                tracing::warn!("Skipping non-object JSON line in file source");
                Some(Vec::new())
            }
            Err(e) => {
                tracing::warn!("Skipping invalid JSON line in file source: {}", e);
                Some(Vec::new())
            }
        }
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
pub fn apply_data_template(
    template: &str,
    data: &serde_json::Map<String, serde_json::Value>,
) -> String {
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
/// polling source). Accepts both the native rekuiper shape (`url`/`table`)
/// and the documented eKuiper plugin shape (`dburl`,
/// `templateSqlQueryCfg`/`internalSqlQueryCfg`; see
/// https://ekuiper.org/docs/en/latest/guide/sources/plugin/sql.html).
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct SqlConnectorConfig {
    /// Database URL, e.g. `"sqlite::memory:"` or
    /// `"postgres://user:pass@localhost:5432/db"`. `dburl` is the
    /// documented plugin spelling.
    #[serde(default, alias = "dburl")]
    pub url: String,
    /// Target table name.
    #[serde(default)]
    pub table: String,
    /// Columns to write (defaults to the record's own keys).
    #[serde(default)]
    pub fields: Vec<String>,
    /// Poll interval in milliseconds (polling sources).
    #[serde(default = "default_sql_interval")]
    pub interval: u64,
    /// Documented template-SQL query config (takes precedence when set).
    #[serde(default)]
    pub template_sql_query_cfg: Option<TemplateSqlQueryCfg>,
    /// Documented internal query-builder config.
    #[serde(default)]
    pub internal_sql_query_cfg: Option<InternalSqlQueryCfg>,
}

/// One indexed column of a SQL source query config.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct IndexFieldCfg {
    #[serde(default)]
    pub index_field: String,
    #[serde(default)]
    pub index_value: serde_json::Value,
    #[serde(default)]
    pub index_field_type: String,
    #[serde(default)]
    pub date_time_format: String,
}

/// Documented `templateSqlQueryCfg`: a raw SQL template where `{{.field}}`
/// placeholders render from the configured index values.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct TemplateSqlQueryCfg {
    #[serde(default, alias = "TemplateSql", alias = "templateSql")]
    pub template_sql: String,
    #[serde(default)]
    pub index_field: String,
    #[serde(default)]
    pub index_value: serde_json::Value,
    #[serde(default)]
    pub index_field_type: String,
    #[serde(default)]
    pub index_fields: Vec<IndexFieldCfg>,
    #[serde(default)]
    pub date_time_format: String,
}

impl TemplateSqlQueryCfg {
    fn index_pairs(&self) -> Vec<(String, serde_json::Value)> {
        let mut pairs = Vec::new();
        if !self.index_field.is_empty() {
            pairs.push((self.index_field.clone(), self.index_value.clone()));
        }
        for f in &self.index_fields {
            if !f.index_field.is_empty() {
                pairs.push((f.index_field.clone(), f.index_value.clone()));
            }
        }
        pairs
    }
}

/// Documented `internalSqlQueryCfg`: table + limit + index columns from
/// which the source builds its polling query.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct InternalSqlQueryCfg {
    #[serde(default)]
    pub table: String,
    #[serde(default)]
    pub limit: u64,
    #[serde(default)]
    pub index_field: String,
    #[serde(default)]
    pub index_value: serde_json::Value,
    #[serde(default)]
    pub index_field_type: String,
    #[serde(default)]
    pub index_fields: Vec<IndexFieldCfg>,
    #[serde(default)]
    pub date_time_format: String,
}

impl InternalSqlQueryCfg {
    fn index_pairs(&self) -> Vec<(String, serde_json::Value)> {
        let mut pairs = Vec::new();
        if !self.index_field.is_empty() {
            pairs.push((self.index_field.clone(), self.index_value.clone()));
        }
        for f in &self.index_fields {
            if !f.index_field.is_empty() {
                pairs.push((f.index_field.clone(), f.index_value.clone()));
            }
        }
        pairs
    }
}

fn default_sql_interval() -> u64 {
    1000
}

/// Render a JSON value as a SQL literal for template substitution.
fn sql_literal(v: &serde_json::Value) -> String {
    match v {
        serde_json::Value::Null => "NULL".to_string(),
        serde_json::Value::Bool(b) => {
            if *b {
                "TRUE".to_string()
            } else {
                "FALSE".to_string()
            }
        }
        serde_json::Value::Number(n) => n.to_string(),
        serde_json::Value::String(s) => format!("'{}'", s.replace('\'', "''")),
        serde_json::Value::Array(_) | serde_json::Value::Object(_) => {
            format!("'{}'", v.to_string().replace('\'', "''"))
        }
    }
}

/// Substitute `{{.field}}` (or `{{ .field }}`) placeholders in a template
/// SQL string with SQL literals. Unknown placeholders are left untouched.
pub fn render_template_sql(template: &str, pairs: &[(String, serde_json::Value)]) -> String {
    let mut out = String::with_capacity(template.len());
    let mut rest = template;
    while let Some(start) = rest.find("{{") {
        out.push_str(&rest[..start]);
        let after = &rest[start + 2..];
        match after.find("}}") {
            Some(end) => {
                let key = after[..end].trim().trim_start_matches('.').trim();
                match pairs.iter().find(|(k, _)| k == key) {
                    Some((_, v)) => out.push_str(&sql_literal(v)),
                    None => {
                        out.push_str("{{");
                        out.push_str(&after[..end]);
                        out.push_str("}}");
                    }
                }
                rest = &after[end + 2..];
            }
            None => {
                out.push_str(&rest[start..]);
                rest = "";
            }
        }
    }
    out.push_str(rest);
    out
}

/// Build the polling query for a SQL source: the template SQL wins when
/// present, then the internal query-builder config, else `SELECT *`.
pub fn sql_source_query(config: &SqlConnectorConfig) -> String {
    sql_source_query_with(config, &initial_index_pairs(config))
}

/// Initial index pairs from the configured query cfgs (template wins).
fn initial_index_pairs(config: &SqlConnectorConfig) -> Vec<(String, serde_json::Value)> {
    if let Some(t) = &config.template_sql_query_cfg {
        if !t.template_sql.trim().is_empty() {
            let mut pairs = t.index_pairs();
            if pairs.is_empty() && !t.index_field.is_empty() {
                pairs.push((t.index_field.clone(), t.index_value.clone()));
            }
            return pairs;
        }
    }
    if let Some(i) = &config.internal_sql_query_cfg {
        let mut pairs = i.index_pairs();
        if pairs.is_empty() && !i.index_field.is_empty() {
            pairs.push((i.index_field.clone(), i.index_value.clone()));
        }
        return pairs;
    }
    Vec::new()
}

/// Render the polling query with explicit per-poll index values (advanced
/// across polls by [`SqlSource`]).
pub fn sql_source_query_with(
    config: &SqlConnectorConfig,
    pairs: &[(String, serde_json::Value)],
) -> String {
    if let Some(t) = &config.template_sql_query_cfg {
        if !t.template_sql.trim().is_empty() {
            return render_template_sql(&t.template_sql, pairs);
        }
    }
    if let Some(i) = &config.internal_sql_query_cfg {
        let table = if i.table.is_empty() {
            config.table.clone()
        } else {
            i.table.clone()
        };
        let mut sql = format!("SELECT * FROM {}", table);
        if !pairs.is_empty() {
            let conds = pairs
                .iter()
                .map(|(k, v)| format!("{} > {}", k, sql_literal(v)))
                .collect::<Vec<_>>()
                .join(" AND ");
            sql.push_str(&format!(" WHERE {}", conds));
            // Declaration order sets the pagination order (last row wins),
            // mirroring upstream `order by {field} ASC`.
            let order = pairs
                .iter()
                .map(|(k, _)| format!("{} ASC", k))
                .collect::<Vec<_>>()
                .join(", ");
            sql.push_str(&format!(" ORDER BY {}", order));
        }
        if i.limit > 0 {
            sql.push_str(&format!(" LIMIT {}", i.limit));
        }
        return sql;
    }
    format!("SELECT * FROM {}", config.table)
}

/// Advance tracked index columns from fetched rows, last row wins (rows
/// arrive in `ORDER BY .. ASC` pagination order for internal queries),
/// mirroring upstream `UpdateMaxIndexValue`.
fn advance_index(index: &mut [(String, serde_json::Value)], rows: &[StreamRecord]) {
    for row in rows {
        for (field, value) in index.iter_mut() {
            if let Some(v) = row.data.get(field) {
                *value = v.clone();
            }
        }
    }
}

/// A dynamically-typed bind parameter: numbers keep their JSON type so
/// drivers infer the right column type (a float must not arrive as text).
/// Null/missing values are NOT bound: PostgreSQL types even a NULL
/// parameter from its Rust type, so `None::<String>` is rejected by
/// non-text columns. Instead the sink omits null columns from the INSERT
/// (database defaults, i.e. NULL, apply).
#[derive(Debug, Clone)]
enum SqlBindVal {
    Int(i64),
    Float(f64),
    Bool(bool),
    Text(String),
}

fn json_to_bind(v: Option<&serde_json::Value>) -> Option<SqlBindVal> {
    match v {
        None | Some(serde_json::Value::Null) => None,
        Some(serde_json::Value::Bool(b)) => Some(SqlBindVal::Bool(*b)),
        Some(serde_json::Value::Number(n)) => {
            if let Some(i) = n.as_i64() {
                Some(SqlBindVal::Int(i))
            } else if let Some(u) = n.as_u64() {
                if u <= i64::MAX as u64 {
                    Some(SqlBindVal::Int(u as i64))
                } else {
                    Some(SqlBindVal::Float(u as f64))
                }
            } else if let Some(f) = n.as_f64() {
                Some(SqlBindVal::Float(f))
            } else {
                Some(SqlBindVal::Text(n.to_string()))
            }
        }
        Some(serde_json::Value::String(s)) => Some(SqlBindVal::Text(s.clone())),
        Some(other) => Some(SqlBindVal::Text(other.to_string())),
    }
}

fn bind_sqlite_arg<'q>(
    query: sqlx::query::Query<'q, sqlx::Sqlite, sqlx::sqlite::SqliteArguments<'q>>,
    val: &'q SqlBindVal,
) -> sqlx::query::Query<'q, sqlx::Sqlite, sqlx::sqlite::SqliteArguments<'q>> {
    match val {
        SqlBindVal::Int(i) => query.bind(*i),
        SqlBindVal::Float(f) => query.bind(*f),
        SqlBindVal::Bool(b) => query.bind(*b),
        SqlBindVal::Text(s) => query.bind(s),
    }
}

fn bind_pg_arg<'q>(
    query: sqlx::query::Query<'q, sqlx::Postgres, sqlx::postgres::PgArguments>,
    val: &'q SqlBindVal,
) -> sqlx::query::Query<'q, sqlx::Postgres, sqlx::postgres::PgArguments> {
    match val {
        SqlBindVal::Int(i) => query.bind(*i),
        SqlBindVal::Float(f) => query.bind(*f),
        SqlBindVal::Bool(b) => query.bind(*b),
        SqlBindVal::Text(s) => query.bind(s),
    }
}

/// SQL sink: executes parameterized row inserts into the database (SQLite
/// `?` placeholders, PostgreSQL `$n` placeholders). Unknown URL schemes
/// remain a no-op for forward compatibility.
pub struct SqlSink {
    pub config: SqlConnectorConfig,
}

impl SqlSink {
    pub async fn insert_record(&self, record: &StreamRecord) -> Result<()> {
        let fields = if self.config.fields.is_empty() {
            record.data.keys().cloned().collect::<Vec<_>>()
        } else {
            self.config.fields.clone()
        };
        // Null/missing columns become an untyped SQL NULL literal (the
        // server infers the column type, so no mistyped-parameter rejection),
        // never a typed NULL parameter and never silent omission (which
        // would wrongly apply column DEFAULTs where baseline stores NULL).
        // Only a completely column-less row falls back to DEFAULT VALUES.
        let cells: Vec<(&String, Option<SqlBindVal>)> = fields
            .iter()
            .map(|f| (f, json_to_bind(record.data.get(f))))
            .collect();
        if self.config.url.starts_with("sqlite") {
            let pool = sqlx::sqlite::SqlitePool::connect(&self.config.url).await?;
            if cells.is_empty() {
                sqlx::query(&format!("INSERT INTO {} DEFAULT VALUES", self.config.table))
                    .execute(&pool)
                    .await?;
                return Ok(());
            }
            let cols: Vec<String> = cells.iter().map(|(f, _)| (*f).clone()).collect();
            let nulls: Vec<bool> = cells.iter().map(|(_, v)| v.is_none()).collect();
            let sql = sqlite_insert_row_sql(&self.config.table, &cols, &nulls);
            let mut query = sqlx::query(&sql);
            for v in cells.iter().filter_map(|(_, v)| v.as_ref()) {
                query = bind_sqlite_arg(query, v);
            }
            query.execute(&pool).await?;
        } else if self.config.url.starts_with("postgres") {
            let pool = pg_pool(&self.config.url).await?;
            if cells.is_empty() {
                sqlx::query(&format!("INSERT INTO {} DEFAULT VALUES", self.config.table))
                    .execute(&pool)
                    .await?;
                return Ok(());
            }
            let cols: Vec<String> = cells.iter().map(|(f, _)| (*f).clone()).collect();
            let nulls: Vec<bool> = cells.iter().map(|(_, v)| v.is_none()).collect();
            let sql = pg_insert_row_sql(&self.config.table, &cols, &nulls);
            let mut query = sqlx::query(&sql);
            for v in cells.iter().filter_map(|(_, v)| v.as_ref()) {
                query = bind_pg_arg(query, v);
            }
            query.execute(&pool).await?;
        }
        Ok(())
    }
}

/// `INSERT INTO {table} ({cols}) VALUES (?, ...)` for SQLite.
pub fn sqlite_insert_sql(table: &str, fields: &[String]) -> String {
    let cols = fields.join(", ");
    let placeholders = vec!["?"; fields.len()].join(", ");
    format!("INSERT INTO {} ({}) VALUES ({})", table, cols, placeholders)
}

/// Row-aware variant: null columns render as an untyped SQL `NULL` literal
/// (never a typed parameter), other columns keep `?` placeholders.
pub fn sqlite_insert_row_sql(table: &str, cols: &[String], nulls: &[bool]) -> String {
    let names = cols.join(", ");
    let placeholders = cols
        .iter()
        .enumerate()
        .map(|(i, _)| {
            if nulls.get(i).copied().unwrap_or(false) {
                "NULL".to_string()
            } else {
                "?".to_string()
            }
        })
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        "INSERT INTO {} ({}) VALUES ({})",
        table, names, placeholders
    )
}

/// `INSERT INTO {table} ({cols}) VALUES ($1, ...)` for PostgreSQL, whose
/// wire protocol numbers placeholders instead of accepting `?`.
pub fn pg_insert_sql(table: &str, fields: &[String]) -> String {
    let cols = fields.join(", ");
    let placeholders = (1..=fields.len())
        .map(|i| format!("${}", i))
        .collect::<Vec<_>>()
        .join(", ");
    format!("INSERT INTO {} ({}) VALUES ({})", table, cols, placeholders)
}

/// Row-aware variant: null columns render as an untyped SQL `NULL` literal
/// (the server infers the column type), other columns keep numbered `$n`
/// placeholders in bind order.
pub fn pg_insert_row_sql(table: &str, cols: &[String], nulls: &[bool]) -> String {
    let names = cols.join(", ");
    let mut next_param = 1u32;
    let placeholders = cols
        .iter()
        .enumerate()
        .map(|(i, _)| {
            if nulls.get(i).copied().unwrap_or(false) {
                "NULL".to_string()
            } else {
                let token = format!("${}", next_param);
                next_param += 1;
                token
            }
        })
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        "INSERT INTO {} ({}) VALUES ({})",
        table, names, placeholders
    )
}

/// Connect a PostgreSQL pool with a bounded handshake timeout so dead
/// brokers fail fast (instead of stalling rule pipelines on the 30s sqlx
/// default) while refused/blackholed hosts still surface real errors.
async fn pg_pool(url: &str) -> Result<sqlx::postgres::PgPool> {
    use std::str::FromStr;
    let opts = sqlx::postgres::PgConnectOptions::from_str(url)?;
    Ok(sqlx::postgres::PgPoolOptions::new()
        .acquire_timeout(std::time::Duration::from_secs(3))
        .connect_with(opts)
        .await?)
}

/// `SELECT * ... WHERE {key_col} = $1 LIMIT 1` for PostgreSQL. The
/// stream-side key arrives stringified, so the comparison runs on the text
/// image: `integer = text` has no operator in PostgreSQL, while the
/// `CAST(... AS TEXT)` form works for every column type.
pub fn pg_lookup_sql(table: &str, key_col: &str) -> String {
    format!(
        "SELECT * FROM {} WHERE CAST({} AS TEXT) = $1 LIMIT 1",
        table, key_col
    )
}

/// Decode one SQLite column, preserving JSON types from the column's
/// declared affinity (INTEGER → number, REAL → number, TEXT → string),
/// following https://sqlite.org/datatype3.html affinity rules. Untyped
/// columns fall back to an integer-first cascade; NULL becomes Null.
fn sqlite_column_value(row: &sqlx::sqlite::SqliteRow, col: &str) -> serde_json::Value {
    use sqlx::{Column, Row, TypeInfo};
    let affinity = row
        .columns()
        .iter()
        .find(|c| c.name() == col)
        .map(|c| c.type_info().name().to_string())
        .unwrap_or_default();
    let upper = affinity.to_ascii_uppercase();
    if upper.contains("INT") {
        if let Ok(v) = row.try_get::<i64, _>(col) {
            return serde_json::Value::from(v);
        }
    } else if upper.contains("CHAR") || upper.contains("CLOB") || upper.contains("TEXT") {
        if let Ok(v) = row.try_get::<String, _>(col) {
            return serde_json::Value::String(v);
        }
    } else if upper.contains("REAL") || upper.contains("FLOA") || upper.contains("DOUB") {
        if let Ok(v) = row.try_get::<f64, _>(col) {
            return serde_json::json!(v);
        }
    } else if upper.contains("BOOL") {
        if let Ok(v) = row.try_get::<bool, _>(col) {
            return serde_json::Value::Bool(v);
        }
        if let Ok(v) = row.try_get::<i64, _>(col) {
            return serde_json::Value::from(v);
        }
    }
    if let Ok(v) = row.try_get::<i64, _>(col) {
        return serde_json::Value::from(v);
    }
    if let Ok(v) = row.try_get::<f64, _>(col) {
        return serde_json::json!(v);
    }
    if let Ok(v) = row.try_get::<String, _>(col) {
        return serde_json::Value::String(v);
    }
    serde_json::Value::Null
}

/// Decode one PostgreSQL column, preserving JSON types (numbers stay
/// numbers) so downstream typed comparisons keep working. Falls back to
/// `Null` for exotic types the cascade cannot represent.
fn pg_column_value(row: &sqlx::postgres::PgRow, col: &str) -> serde_json::Value {
    use sqlx::Row;
    if let Ok(v) = row.try_get::<String, _>(col) {
        return serde_json::Value::String(v);
    }
    if let Ok(v) = row.try_get::<i32, _>(col) {
        return serde_json::Value::from(v);
    }
    if let Ok(v) = row.try_get::<i64, _>(col) {
        return serde_json::Value::from(v);
    }
    if let Ok(v) = row.try_get::<f64, _>(col) {
        return serde_json::json!(v);
    }
    if let Ok(v) = row.try_get::<f32, _>(col) {
        return serde_json::json!(f64::from(v));
    }
    if let Ok(v) = row.try_get::<bool, _>(col) {
        return serde_json::Value::Bool(v);
    }
    serde_json::Value::Null
}

/// Point lookup against a SQL database, mapping the row columns to
/// type-preserving values (numbers stay numbers on both backends).
/// Returns `None` when no row matches.
pub async fn sql_lookup_key(
    url: &str,
    table: &str,
    key_col: &str,
    key_val: &str,
) -> Result<Option<serde_json::Value>> {
    if url.starts_with("sqlite") {
        let pool = sqlx::sqlite::SqlitePool::connect(url).await?;
        let sql = format!("SELECT * FROM {} WHERE {} = ? LIMIT 1", table, key_col);
        let row = sqlx::query(&sql)
            .bind(key_val)
            .fetch_optional(&pool)
            .await?;
        if let Some(r) = row {
            use sqlx::{Column, Row};
            let mut map = serde_json::Map::new();
            for col in r.columns() {
                let name = col.name();
                map.insert(name.to_string(), sqlite_column_value(&r, name));
            }
            return Ok(Some(serde_json::Value::Object(map)));
        }
    } else if url.starts_with("postgres") {
        let pool = pg_pool(url).await?;
        let sql = pg_lookup_sql(table, key_col);
        let row = sqlx::query(&sql)
            .bind(key_val)
            .fetch_optional(&pool)
            .await?;
        if let Some(r) = row {
            use sqlx::{Column, Row};
            let mut map = serde_json::Map::new();
            for col in r.columns() {
                let name = col.name();
                map.insert(name.to_string(), pg_column_value(&r, name));
            }
            return Ok(Some(serde_json::Value::Object(map)));
        }
    }
    Ok(None)
}

/// Polling SQL source: every `interval` ms runs the configured query and
/// broadcasts each row as a [`StreamRecord`]. Index columns (`indexFields`,
/// plus the legacy singular pair) advance across polls: each poll renders
/// with the current values and the last row wins per column (with `ORDER BY
/// .. ASC` pagination for internal queries), so subsequent polls emit only
/// newer rows — mirroring upstream incremental polling. Mirrors the
/// `HttpPullSource` ticker/cancellation discipline.
pub struct SqlSource {
    pub config: SqlConnectorConfig,
    pub tx: tokio::sync::broadcast::Sender<StreamRecord>,
    index: Vec<(String, serde_json::Value)>,
}

impl SqlSource {
    pub fn new(
        config: SqlConnectorConfig,
        tx: tokio::sync::broadcast::Sender<StreamRecord>,
    ) -> Self {
        let index = initial_index_pairs(&config);
        Self { config, tx, index }
    }

    pub fn spawn(
        mut self,
        mut cancel_rx: tokio::sync::watch::Receiver<bool>,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(std::time::Duration::from_millis(
                self.config.interval.max(1),
            ));
            loop {
                tokio::select! {
                    _ = ticker.tick() => {
                        match self.poll_once().await {
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
                                    "SQL source poll of {} failed: {}",
                                    self.config.table,
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

    /// Poll once with the current index values, then advance tracked
    /// indexes from the fetched rows (last row wins).
    async fn poll_once(&mut self) -> Result<Vec<StreamRecord>> {
        let sql = sql_source_query_with(&self.config, &self.index);
        if self.config.url.starts_with("sqlite") {
            let pool = sqlx::sqlite::SqlitePool::connect(&self.config.url).await?;
            let rows = sqlx::query(&sql).fetch_all(&pool).await?;
            use sqlx::{Column, Row};
            let mut out = Vec::with_capacity(rows.len());
            for r in &rows {
                let mut map = serde_json::Map::new();
                for col in r.columns() {
                    let name = col.name();
                    map.insert(name.to_string(), sqlite_column_value(r, name));
                }
                out.push(StreamRecord::new(map.into_iter().collect()));
            }
            advance_index(&mut self.index, &out);
            return Ok(out);
        }
        if self.config.url.starts_with("postgres") {
            let pool = pg_pool(&self.config.url).await?;
            let rows = sqlx::query(&sql).fetch_all(&pool).await?;
            use sqlx::{Column, Row};
            let mut out = Vec::with_capacity(rows.len());
            for r in &rows {
                let mut map = serde_json::Map::new();
                for col in r.columns() {
                    let name = col.name();
                    map.insert(name.to_string(), pg_column_value(r, name));
                }
                out.push(StreamRecord::new(map.into_iter().collect()));
            }
            advance_index(&mut self.index, &out);
            return Ok(out);
        }
        bail!("Unsupported SQL source URL scheme: {}", self.config.url)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::collections::HashMap;

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
        assert_eq!(
            apply_data_template("x={{.missing}}", &data),
            "x={{.missing}}"
        );
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
        let cfg: RedisSinkConfig = serde_json::from_value(json!({"field": "id"})).unwrap();
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
        let source = FileSource::new(
            FileSourceConfig {
                path: path.to_string_lossy().into_owned(),
                format: "json".to_string(),
                has_header: false,
                delimiter: None,
                interval: 0,
            },
            tokio::sync::broadcast::channel(16).0,
        );
        let records = source.read_records().await.unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].data.get("temp"), Some(&json!(25.0)));
        assert_eq!(records[1].data.get("status"), Some(&json!("ok")));

        let _ = tokio::fs::remove_file(&path).await;
    }

    #[tokio::test]
    async fn test_file_source_streaming() {
        let path = unique_temp_path("filesource");
        tokio::fs::write(&path, "{\"temp\": 20}\n{\"temp\": 25}\n{\"temp\": 30}\n")
            .await
            .unwrap();
        let (tx, mut rx) = tokio::sync::broadcast::channel(16);
        let source = FileSource::new(
            FileSourceConfig {
                path: path.to_string_lossy().into_owned(),
                format: "json".to_string(),
                has_header: false,
                delimiter: None,
                interval: 0,
            },
            tx,
        );
        let (cancel_tx, cancel_rx) = tokio::sync::watch::channel(false);
        let handle = source.spawn(cancel_rx);

        let mut temps = Vec::new();
        for _ in 0..3 {
            let record = tokio::time::timeout(std::time::Duration::from_secs(2), rx.recv())
                .await
                .expect("timed out waiting for file record")
                .expect("file channel closed");
            temps.push(record.data.get("temp").cloned().unwrap());
        }
        assert_eq!(temps, vec![json!(20), json!(25), json!(30)]);

        // Cancellation stops the tail loop cleanly.
        cancel_tx.send(true).unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(2), handle)
            .await
            .expect("file source did not exit after cancel")
            .unwrap();

        let _ = tokio::fs::remove_file(&path).await;
    }

    #[test]
    fn test_file_source_config_defaults() {
        let cfg: FileSourceConfig = serde_json::from_value(json!({"path": "data/a.json"})).unwrap();
        assert_eq!(cfg.format, "json");
        assert!(!cfg.has_header);
        assert_eq!(cfg.delimiter, None);
        assert_eq!(cfg.interval, 0);

        // Delimiter accepts single chars and friendly names.
        let cfg: FileSourceConfig =
            serde_json::from_value(json!({"path": "a.csv", "delimiter": "tab"})).unwrap();
        assert_eq!(cfg.delimiter, Some('\t'));
        let cfg: FileSourceConfig =
            serde_json::from_value(json!({"path": "a.csv", "delimiter": "|"})).unwrap();
        assert_eq!(cfg.delimiter, Some('|'));
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

    #[test]
    fn test_mqtt_config_confkey_without_topic() {
        // D1: source CONF_KEY entries carry only connection parameters (no
        // topic). They must decode with the stored broker URL instead of
        // failing and silently falling back to the loopback broker.
        let conf = serde_json::json!({"server": "tcp://broker:1883"});
        let cfg: super::MqttConfig = serde_json::from_value(conf).unwrap();
        assert_eq!(cfg.server, "tcp://broker:1883");
        assert_eq!(cfg.topic, "");
        // An empty object still yields the loopback default server.
        let cfg: super::MqttConfig = serde_json::from_value(serde_json::json!({})).unwrap();
        assert_eq!(cfg.server, "tcp://127.0.0.1:1883");
        assert_eq!(cfg.topic, "");
    }

    #[test]
    fn test_sql_statement_builders() {
        // D8: SQLite keeps `?` placeholders; PostgreSQL numbers them, since
        // its wire protocol rejects `?`.
        let fields = vec!["id".to_string(), "val".to_string()];
        assert_eq!(
            super::sqlite_insert_sql("readings", &fields),
            "INSERT INTO readings (id, val) VALUES (?, ?)"
        );
        assert_eq!(
            super::pg_insert_sql("readings", &fields),
            "INSERT INTO readings (id, val) VALUES ($1, $2)"
        );
        assert!(
            !super::pg_insert_sql("readings", &fields).contains('?'),
            "postgres statements must not contain `?`"
        );
        assert_eq!(
            super::pg_lookup_sql("readings", "id"),
            "SELECT * FROM readings WHERE CAST(id AS TEXT) = $1 LIMIT 1"
        );
    }

    #[tokio::test]
    async fn test_pg_paths_fail_loudly_without_broker() {
        // D8: postgres URLs must attempt a real connection and surface the
        // error, never silently report Ok with zero rows written or read.
        // Port 1 on loopback refuses immediately, so this cannot hang.
        let url = "postgres://127.0.0.1:1/nonexistent";
        let cfg = super::SqlConnectorConfig {
            url: url.to_string(),
            table: "t".to_string(),
            fields: vec!["id".to_string()],
            interval: 1000,
            template_sql_query_cfg: None,
            internal_sql_query_cfg: None,
        };
        let sink = super::SqlSink {
            config: cfg.clone(),
        };
        let mut data = HashMap::new();
        data.insert("id".to_string(), json!(1));
        assert!(sink.insert_record(&StreamRecord::new(data)).await.is_err());
        assert!(super::sql_lookup_key(url, "t", "id", "1").await.is_err());
        let mut src =
            super::SqlSource::new(cfg, tokio::sync::broadcast::channel::<StreamRecord>(8).0);
        assert!(src.poll_once().await.is_err());
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
    fn test_sql_connector_config_documented_shape() {
        // Documented eKuiper plugin shape: dburl + templateSqlQueryCfg.
        let cfg: super::SqlConnectorConfig = serde_json::from_value(json!({
            "dburl": "postgres://u:p@h/db?sslmode=disable",
            "interval": 5000,
            "templateSqlQueryCfg": {"templateSql": "SELECT id, val FROM rksrc"}
        }))
        .unwrap();
        assert_eq!(cfg.url, "postgres://u:p@h/db?sslmode=disable");
        assert_eq!(cfg.interval, 5000);
        let t = cfg.template_sql_query_cfg.expect("template cfg parsed");
        assert_eq!(t.template_sql, "SELECT id, val FROM rksrc");
        // Native shape keeps working.
        let native: super::SqlConnectorConfig = serde_json::from_value(json!({
            "url": "sqlite::memory:", "table": "alerts"
        }))
        .unwrap();
        assert_eq!(native.table, "alerts");
        assert_eq!(native.interval, 1000);
        assert!(native.template_sql_query_cfg.is_none());
        // Internal incremental shape parses exactly as PUT by probes.
        let inc: super::SqlConnectorConfig = serde_json::from_value(json!({
            "dburl": "postgres://u:p@h/db?sslmode=disable",
            "interval": 3000,
            "internalSqlQueryCfg": {
                "table": "rksrc",
                "limit": 10,
                "indexFields": [{"indexField": "id", "indexValue": 0, "indexFieldType": "bigint"}],
            },
        }))
        .unwrap();
        assert_eq!(inc.url, "postgres://u:p@h/db?sslmode=disable");
        let inner = inc
            .internal_sql_query_cfg
            .clone()
            .expect("internal cfg parsed");
        assert_eq!(inner.table, "rksrc");
        assert_eq!(inner.limit, 10);
        assert_eq!(inner.index_fields.len(), 1);
        assert_eq!(inner.index_fields[0].index_field, "id");
        assert_eq!(
            super::sql_source_query(&inc),
            "SELECT * FROM rksrc WHERE id > 0 ORDER BY id ASC LIMIT 10"
        );
    }

    #[test]
    fn test_sql_source_query_builders() {
        let base = super::SqlConnectorConfig {
            url: "x".to_string(),
            table: "rksrc".to_string(),
            ..Default::default()
        };
        assert_eq!(super::sql_source_query(&base), "SELECT * FROM rksrc");
        let tpl = super::SqlConnectorConfig {
            template_sql_query_cfg: Some(super::TemplateSqlQueryCfg {
                template_sql: "SELECT id, val FROM rksrc".to_string(),
                ..Default::default()
            }),
            ..Default::default()
        };
        assert_eq!(super::sql_source_query(&tpl), "SELECT id, val FROM rksrc");
        let tpl_vars = super::SqlConnectorConfig {
            template_sql_query_cfg: Some(super::TemplateSqlQueryCfg {
                template_sql: "select * from t where a > {{.a}} and b > {{ .b }}".to_string(),
                index_fields: vec![
                    super::IndexFieldCfg {
                        index_field: "a".to_string(),
                        index_value: json!(3),
                        ..Default::default()
                    },
                    super::IndexFieldCfg {
                        index_field: "b".to_string(),
                        index_value: json!("x'y"),
                        ..Default::default()
                    },
                ],
                ..Default::default()
            }),
            ..Default::default()
        };
        assert_eq!(
            super::sql_source_query(&tpl_vars),
            "select * from t where a > 3 and b > 'x''y'"
        );
        let internal = super::SqlConnectorConfig {
            internal_sql_query_cfg: Some(super::InternalSqlQueryCfg {
                table: "Student".to_string(),
                limit: 10,
                index_field: "stun".to_string(),
                index_value: json!(100),
                ..Default::default()
            }),
            ..Default::default()
        };
        assert_eq!(
            super::sql_source_query(&internal),
            "SELECT * FROM Student WHERE stun > 100 ORDER BY stun ASC LIMIT 10"
        );
    }

    fn sqlite_file_url(name: &str) -> String {
        let mut p = std::env::temp_dir();
        p.push(format!("rekuiper-{}-{}.db", name, std::process::id()));
        let _ = std::fs::remove_file(&p);
        format!("sqlite://{}?mode=rwc", p.display())
    }

    #[tokio::test]
    async fn test_sqlite_sink_sink_preserves_float_and_poll_typed() {
        let url = sqlite_file_url("sinkfloat");
        let pool = sqlx::sqlite::SqlitePool::connect(&url).await.unwrap();
        sqlx::query("CREATE TABLE rksink(eid TEXT PRIMARY KEY, temp REAL)")
            .execute(&pool)
            .await
            .unwrap();
        drop(pool);
        let sink = super::SqlSink {
            config: super::SqlConnectorConfig {
                url: url.clone(),
                table: "rksink".to_string(),
                fields: vec!["eid".to_string(), "temp".to_string()],
                ..Default::default()
            },
        };
        let mut data = HashMap::new();
        data.insert("eid".to_string(), json!("row-pg1"));
        data.insert("temp".to_string(), json!(33.5));
        sink.insert_record(&StreamRecord::new(data)).await.unwrap();
        // REAL column holds a real number, not text.
        let pool = sqlx::sqlite::SqlitePool::connect(&url).await.unwrap();
        let row: (String, f64) = sqlx::query_as("SELECT eid, temp FROM rksink WHERE eid='row-pg1'")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(row, ("row-pg1".to_string(), 33.5));
        let typeof_temp: String = sqlx::query_scalar("SELECT typeof(temp) FROM rksink")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(typeof_temp, "real");
        drop(pool);
        // Polling source decodes typed rows.
        let mut src = super::SqlSource::new(
            super::SqlConnectorConfig {
                url,
                table: "rksink".to_string(),
                ..Default::default()
            },
            tokio::sync::broadcast::channel::<StreamRecord>(8).0,
        );
        let rows = src.poll_once().await.unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].data.get("temp"), Some(&json!(33.5)));
        assert!(rows[0].data.get("temp").unwrap().is_number());
        // Point lookup decodes typed values too.
        let hit = super::sql_lookup_key(&src.config.url, "rksink", "eid", "row-pg1")
            .await
            .unwrap()
            .expect("lookup hits");
        assert_eq!(hit.get("temp"), Some(&json!(33.5)));
        assert!(hit.get("temp").unwrap().is_number());
    }

    #[tokio::test]
    async fn test_sqlite_sink_null_is_null_not_default() {
        // Explicit JSON nulls and missing fields must land as SQL NULL —
        // never column DEFAULTs and never a mistyped NULL parameter.
        let url = sqlite_file_url("sinknulls");
        let pool = sqlx::sqlite::SqlitePool::connect(&url).await.unwrap();
        sqlx::query("CREATE TABLE rknull(eid TEXT PRIMARY KEY, temp REAL NULL DEFAULT 99.9, flag INTEGER NULL DEFAULT 1)")
            .execute(&pool)
            .await
            .unwrap();
        drop(pool);
        // Row-SQL builders: NULL literal for nulls, ordered placeholders else.
        assert_eq!(
            super::pg_insert_row_sql(
                "t",
                &["a".to_string(), "b".to_string(), "c".to_string()],
                &[false, true, false]
            ),
            "INSERT INTO t (a, b, c) VALUES ($1, NULL, $2)"
        );
        assert_eq!(
            super::sqlite_insert_row_sql("t", &["a".to_string(), "b".to_string()], &[true, false]),
            "INSERT INTO t (a, b) VALUES (NULL, ?)"
        );
        let sink = super::SqlSink {
            config: super::SqlConnectorConfig {
                url: url.clone(),
                table: "rknull".to_string(),
                fields: vec!["eid".to_string(), "temp".to_string(), "flag".to_string()],
                ..Default::default()
            },
        };
        let mut all_null = HashMap::new();
        all_null.insert("eid".to_string(), json!("n1"));
        all_null.insert("temp".to_string(), serde_json::Value::Null);
        all_null.insert("flag".to_string(), serde_json::Value::Null);
        sink.insert_record(&StreamRecord::new(all_null))
            .await
            .unwrap();
        let mut missing = HashMap::new();
        missing.insert("eid".to_string(), json!("n2"));
        sink.insert_record(&StreamRecord::new(missing))
            .await
            .unwrap();
        let mut mixed = HashMap::new();
        mixed.insert("eid".to_string(), json!("n3"));
        mixed.insert("temp".to_string(), json!(21));
        mixed.insert("flag".to_string(), json!(true));
        sink.insert_record(&StreamRecord::new(mixed)).await.unwrap();
        let pool = sqlx::sqlite::SqlitePool::connect(&url).await.unwrap();
        let rows: Vec<(String, Option<f64>, Option<i64>)> =
            sqlx::query_as("SELECT eid, temp, flag FROM rknull ORDER BY eid")
                .fetch_all(&pool)
                .await
                .unwrap();
        assert_eq!(
            rows,
            vec![
                ("n1".to_string(), None, None),
                ("n2".to_string(), None, None),
                ("n3".to_string(), Some(21.0), Some(1)),
            ]
        );
    }

    #[tokio::test]
    async fn test_sqlite_lookup_typed_values() {
        let url = sqlite_file_url("lookuptyped");
        let pool = sqlx::sqlite::SqlitePool::connect(&url).await.unwrap();
        sqlx::query("CREATE TABLE rklook(id INTEGER PRIMARY KEY, name TEXT)")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("INSERT INTO rklook VALUES (1, 'one'), (2, 'two')")
            .execute(&pool)
            .await
            .unwrap();
        drop(pool);
        let hit = super::sql_lookup_key(&url, "rklook", "id", "1")
            .await
            .unwrap()
            .expect("lookup hits");
        assert_eq!(hit.get("id"), Some(&json!(1)));
        assert!(hit.get("id").unwrap().is_number());
        assert_eq!(hit.get("name"), Some(&json!("one")));
    }

    fn src_ids(rows: &[super::StreamRecord]) -> Vec<i64> {
        let mut ids: Vec<i64> = rows
            .iter()
            .filter_map(|r| r.data.get("id").and_then(|v| v.as_i64()))
            .collect();
        ids.sort_unstable();
        ids
    }

    #[tokio::test]
    async fn test_sqlite_source_advances_index_across_polls() {
        // Incremental polling: each poll emits only rows newer than the
        // tracked index (last row wins, ORDER BY .. ASC pagination).
        let url = sqlite_file_url("srcindex");
        let pool = sqlx::sqlite::SqlitePool::connect(&url).await.unwrap();
        sqlx::query("CREATE TABLE rksrc(id INTEGER PRIMARY KEY, val INTEGER)")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("INSERT INTO rksrc VALUES (1,10),(2,20),(3,30)")
            .execute(&pool)
            .await
            .unwrap();
        drop(pool);
        let mut src = super::SqlSource::new(
            super::SqlConnectorConfig {
                url: url.clone(),
                interval: 100,
                internal_sql_query_cfg: Some(super::InternalSqlQueryCfg {
                    table: "rksrc".to_string(),
                    limit: 10,
                    index_fields: vec![super::IndexFieldCfg {
                        index_field: "id".to_string(),
                        index_value: json!(0),
                        ..Default::default()
                    }],
                    ..Default::default()
                }),
                ..Default::default()
            },
            tokio::sync::broadcast::channel::<StreamRecord>(8).0,
        );
        assert_eq!(src_ids(&src.poll_once().await.unwrap()), vec![1, 2, 3]);
        let pool = sqlx::sqlite::SqlitePool::connect(&url).await.unwrap();
        sqlx::query("INSERT INTO rksrc VALUES (4,40),(5,50)")
            .execute(&pool)
            .await
            .unwrap();
        drop(pool);
        assert_eq!(src_ids(&src.poll_once().await.unwrap()), vec![4, 5]);
        assert!(src.poll_once().await.unwrap().is_empty());
        // Template variant with the same index contract.
        let mut tsrc = super::SqlSource::new(
            super::SqlConnectorConfig {
                url,
                interval: 100,
                template_sql_query_cfg: Some(super::TemplateSqlQueryCfg {
                    template_sql: "select * from rksrc where id > {{.id}}".to_string(),
                    index_fields: vec![super::IndexFieldCfg {
                        index_field: "id".to_string(),
                        index_value: json!(3),
                        ..Default::default()
                    }],
                    ..Default::default()
                }),
                ..Default::default()
            },
            tokio::sync::broadcast::channel::<StreamRecord>(8).0,
        );
        assert_eq!(src_ids(&tsrc.poll_once().await.unwrap()), vec![4, 5]);
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
