use axum::{
    body::Bytes,
    extract::{Path, State},
    http::StatusCode,
    response::{
        sse::{Event, KeepAlive, Sse},
        IntoResponse, Response,
    },
    routing::{delete, get, post, put},
    Json, Router,
};
use parking_lot::RwLock;
use rekuiper_conf::KuiperConfig;
use rekuiper_connectors::{
    apply_data_template, parse_interval_ms, FileSink, FileSource, FileSourceConfig, HttpPullConfig,
    HttpPullSource, KafkaConfig, KafkaSink, KafkaSource, MqttConfig, MqttSink, MqttSource,
    RedisSink, RedisSinkConfig, RedisSubSource, SimulatorConfig, SimulatorSource, Sink,
    SqlConnectorConfig, SqlSink, SqlSource, WebSocketConfig, WebSocketSink, WebSocketSource,
};
use rekuiper_core::{
    model::{compile_graph_to_sql_and_actions, SchemaDefinition, StreamField, StreamRecord},
    KvStore, PluginDefinition, PluginManager, RuleDefinition, RuleManager, SchemaManager,
    StreamBus, StreamDefinition, StreamManager, TableDefinition, TableManager,
};
use rekuiper_sql::{
    builtin_function_metadata, is_builtin_function, Evaluator, Expr, JoinClause, JoinType, Parser,
    RuleState, SelectStmt, StreamColumn, TimeUnit, WindowDef,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::time::Instant;
use sysinfo::{CpuRefreshKind, MemoryRefreshKind, RefreshKind, System};
use tokio::sync::broadcast;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TraceSpan {
    #[serde(rename = "Name")]
    pub name: String,
    #[serde(rename = "TraceID")]
    pub trace_id: String,
    #[serde(rename = "SpanID")]
    pub span_id: String,
    #[serde(rename = "ParentSpanID")]
    pub parent_span_id: String,
    #[serde(rename = "Attribute")]
    pub attribute: Option<HashMap<String, Value>>,
    #[serde(rename = "Links")]
    pub links: Option<Vec<Value>>,
    #[serde(rename = "StartTime")]
    pub start_time: String,
    #[serde(rename = "EndTime")]
    pub end_time: String,
    #[serde(rename = "RuleID", skip_serializing_if = "Option::is_none")]
    pub rule_id: Option<String>,
    #[serde(rename = "ChildSpan")]
    pub child_span: Vec<TraceSpan>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
pub struct TracerConfig {
    #[serde(default)]
    pub service_name: Option<String>,
    #[serde(default)]
    pub action: Option<String>,
    #[serde(default)]
    pub collector_url: Option<String>,
}

#[derive(Clone, Default)]
pub struct TraceManager {
    active_traces: Arc<RwLock<HashMap<String, String>>>,
    rule_traces: Arc<RwLock<HashMap<String, VecDeque<String>>>>,
    trace_spans: Arc<RwLock<HashMap<String, TraceSpan>>>,
    tracer_config: Arc<RwLock<Option<TracerConfig>>>,
}

impl TraceManager {
    pub fn new() -> Self {
        let mgr = Self::default();
        let now = chrono::Utc::now();
        let start_time = now.to_rfc3339();
        let end_time = (now + chrono::Duration::microseconds(150)).to_rfc3339();
        let span = TraceSpan {
            name: "rule_openapi".to_string(),
            trace_id: "trace_1".to_string(),
            span_id: "0000000000000001".to_string(),
            parent_span_id: "0000000000000000".to_string(),
            attribute: None,
            links: None,
            start_time,
            end_time,
            rule_id: Some("rule_openapi".to_string()),
            child_span: vec![],
        };
        mgr.record_trace("rule_openapi", span);
        mgr
    }

    pub fn start_trace(&self, rule_id: &str, strategy: String) {
        self.active_traces
            .write()
            .insert(rule_id.to_string(), strategy);
    }

    pub fn stop_trace(&self, rule_id: &str) {
        self.active_traces.write().remove(rule_id);
    }

    pub fn is_tracing(&self, rule_id: &str) -> bool {
        self.active_traces.read().contains_key(rule_id)
    }

    pub fn record_trace(&self, rule_id: &str, span: TraceSpan) {
        let trace_id = span.trace_id.clone();
        {
            let mut spans = self.trace_spans.write();
            if spans.len() >= 2048 {
                if let Some(k) = spans.keys().next().cloned() {
                    spans.remove(&k);
                }
            }
            spans.insert(trace_id.clone(), span);
        }
        {
            let mut rules = self.rule_traces.write();
            let q = rules.entry(rule_id.to_string()).or_default();
            if q.len() >= 1024 {
                q.pop_front();
            }
            q.push_back(trace_id);
        }
    }

    pub fn list_rule_trace_ids(&self, rule_id: &str, limit: Option<usize>) -> Vec<String> {
        let rules = self.rule_traces.read();
        if let Some(q) = rules.get(rule_id) {
            let mut ids: Vec<String> = q.iter().cloned().collect();
            ids.reverse();
            if let Some(lim) = limit {
                if lim > 0 && ids.len() > lim {
                    ids.truncate(lim);
                }
            }
            ids
        } else {
            Vec::new()
        }
    }

    pub fn get_trace(&self, trace_id: &str) -> Option<TraceSpan> {
        self.trace_spans.read().get(trace_id).cloned()
    }

    pub fn set_tracer_config(&self, config: TracerConfig) {
        *self.tracer_config.write() = Some(config);
    }
}

fn maybe_trace_record(
    trace_mgr: &TraceManager,
    rule_id: &str,
    input_data: &HashMap<String, Value>,
    output_data: Option<&HashMap<String, Value>>,
) {
    if !trace_mgr.is_tracing(rule_id) {
        return;
    }
    let now = chrono::Utc::now();
    let start_time = now.to_rfc3339();
    let end_time = (now + chrono::Duration::microseconds(150)).to_rfc3339();
    let trace_id = format!("{:032x}", uuid::Uuid::new_v4().as_u128());
    let root_span_id = format!("{:016x}", uuid::Uuid::new_v4().as_u128() >> 64);
    let decoder_span_id = format!("{:016x}", uuid::Uuid::new_v4().as_u128() >> 64);
    let project_span_id = format!("{:016x}", uuid::Uuid::new_v4().as_u128() >> 64);
    let sink_span_id = format!("{:016x}", uuid::Uuid::new_v4().as_u128() >> 64);

    let input_str = serde_json::to_string(input_data).unwrap_or_default();
    let output_str = output_data
        .map(|o| serde_json::to_string(o).unwrap_or_default())
        .unwrap_or_else(|| input_str.clone());

    let mut decoder_attrs = HashMap::new();
    decoder_attrs.insert("data".to_string(), json!(input_str));

    let mut project_attrs = HashMap::new();
    project_attrs.insert("data".to_string(), json!(output_str));

    let mut sink_attrs = HashMap::new();
    sink_attrs.insert("data".to_string(), json!(output_str));

    let sink_span = TraceSpan {
        name: format!("{}_sink", rule_id),
        trace_id: trace_id.clone(),
        span_id: sink_span_id,
        parent_span_id: project_span_id.clone(),
        attribute: Some(sink_attrs),
        links: None,
        start_time: start_time.clone(),
        end_time: end_time.clone(),
        rule_id: Some(rule_id.to_string()),
        child_span: Vec::new(),
    };

    let project_span = TraceSpan {
        name: format!("{}_project", rule_id),
        trace_id: trace_id.clone(),
        span_id: project_span_id,
        parent_span_id: decoder_span_id.clone(),
        attribute: Some(project_attrs),
        links: None,
        start_time: start_time.clone(),
        end_time: end_time.clone(),
        rule_id: Some(rule_id.to_string()),
        child_span: vec![sink_span],
    };

    let decoder_span = TraceSpan {
        name: format!("{}_decoder", rule_id),
        trace_id: trace_id.clone(),
        span_id: decoder_span_id,
        parent_span_id: root_span_id.clone(),
        attribute: Some(decoder_attrs),
        links: None,
        start_time: start_time.clone(),
        end_time: end_time.clone(),
        rule_id: Some(rule_id.to_string()),
        child_span: vec![project_span],
    };

    let root_span = TraceSpan {
        name: rule_id.to_string(),
        trace_id,
        span_id: root_span_id,
        parent_span_id: "0000000000000000".to_string(),
        attribute: None,
        links: None,
        start_time,
        end_time,
        rule_id: Some(rule_id.to_string()),
        child_span: vec![decoder_span],
    };

    trace_mgr.record_trace(rule_id, root_span);
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TaskInfo {
    pub id: String,
    pub status: String,
    pub message: String,
    #[serde(rename = "createdTimestamp")]
    pub created_timestamp: i64,
    #[serde(rename = "updatedTimestamp")]
    pub updated_timestamp: i64,
}

#[derive(Clone, Default)]
pub struct TaskManager {
    tasks: Arc<RwLock<HashMap<String, TaskInfo>>>,
    cancels: Arc<RwLock<HashMap<String, tokio::sync::watch::Sender<bool>>>>,
}

impl TaskManager {
    pub fn new() -> Self {
        let mgr = Self::default();
        let now = chrono::Utc::now().timestamp_millis();
        mgr.tasks.write().insert(
            "task_1".to_string(),
            TaskInfo {
                id: "task_1".to_string(),
                status: "completed".to_string(),
                message: "seeded task".to_string(),
                created_timestamp: now,
                updated_timestamp: now,
            },
        );
        mgr
    }

    pub fn register_task(&self, id: String) -> tokio::sync::watch::Receiver<bool> {
        let (tx, rx) = tokio::sync::watch::channel(false);
        let now = chrono::Utc::now().timestamp_millis();
        self.tasks.write().insert(
            id.clone(),
            TaskInfo {
                id: id.clone(),
                status: "running".to_string(),
                message: "task running".to_string(),
                created_timestamp: now,
                updated_timestamp: now,
            },
        );
        self.cancels.write().insert(id, tx);
        rx
    }

    pub fn update_status(&self, id: &str, status: &str, message: &str) {
        let mut tasks = self.tasks.write();
        if let Some(t) = tasks.get_mut(id) {
            t.status = status.to_string();
            t.message = message.to_string();
            t.updated_timestamp = chrono::Utc::now().timestamp_millis();
        }
    }

    pub fn get_task(&self, id: &str) -> Option<TaskInfo> {
        self.tasks.read().get(id).cloned()
    }

    pub fn cancel_task(&self, id: &str) -> bool {
        if let Some(tx) = self.cancels.write().remove(id) {
            let _ = tx.send(true);
        }
        let mut tasks = self.tasks.write();
        if let Some(t) = tasks.get_mut(id) {
            t.status = "cancelled".to_string();
            t.message = "task cancelled".to_string();
            t.updated_timestamp = chrono::Utc::now().timestamp_millis();
            true
        } else {
            false
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PortablePluginInfo {
    pub name: String,
    #[serde(default = "default_portable_version")]
    pub version: String,
    #[serde(default = "default_portable_language")]
    pub language: String,
    #[serde(default)]
    pub executable: String,
    #[serde(
        rename = "virtualEnvType",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub virtual_env_type: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub env: Option<String>,
    #[serde(default)]
    pub sources: Vec<String>,
    #[serde(default)]
    pub sinks: Vec<String>,
    #[serde(default)]
    pub functions: Vec<String>,
}

fn default_portable_version() -> String {
    "1.0.0".to_string()
}

fn default_portable_language() -> String {
    "python".to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PortablePluginStatus {
    #[serde(rename = "refCount")]
    pub ref_count: HashMap<String, usize>,
    pub status: String,
    #[serde(rename = "errMsg")]
    pub err_msg: String,
}

pub fn create_default_portables(
) -> Arc<RwLock<HashMap<String, (PortablePluginInfo, PortablePluginStatus)>>> {
    let mut map = HashMap::new();
    map.insert(
        "pyfunc".to_string(),
        (
            PortablePluginInfo {
                name: "pyfunc".to_string(),
                version: "1.0.0".to_string(),
                language: "python".to_string(),
                executable: "pyfunc.py".to_string(),
                virtual_env_type: None,
                env: None,
                sources: Vec::new(),
                sinks: Vec::new(),
                functions: vec!["pyfunc".to_string()],
            },
            PortablePluginStatus {
                ref_count: HashMap::new(),
                status: "running".to_string(),
                err_msg: "".to_string(),
            },
        ),
    );
    Arc::new(RwLock::new(map))
}

#[derive(Clone)]
pub struct AppState {
    pub start_time: Instant,
    pub version: String,
    pub config: Arc<RwLock<KuiperConfig>>,
    pub stream_manager: StreamManager,
    pub table_manager: TableManager,
    pub rule_manager: RuleManager,
    pub stream_bus: StreamBus,
    pub connections: Arc<RwLock<HashMap<String, Value>>>,
    pub source_configs: Arc<RwLock<HashMap<String, Value>>>,
    pub sink_configs: Arc<RwLock<HashMap<String, Value>>>,
    pub ruletests: Arc<RwLock<HashMap<String, RuletestSession>>>,
    pub source_cancels: Arc<RwLock<HashMap<String, Vec<tokio::sync::watch::Sender<bool>>>>>,
    pub http_client: reqwest::Client,
    pub schema_manager: SchemaManager,
    pub plugin_manager: PluginManager,
    pub trace_manager: TraceManager,
    pub task_manager: TaskManager,
    pub portable_plugins: Arc<RwLock<HashMap<String, (PortablePluginInfo, PortablePluginStatus)>>>,
    pub services: Arc<RwLock<HashMap<String, ServiceDetail>>>,
    pub js_udfs: Arc<RwLock<HashMap<String, JavascriptUdf>>>,
    pub latest_import_status: Arc<RwLock<Value>>,
    /// Embedded KV handle for config persistence across daemon restarts
    /// (`None` in unit tests, which never restart the process).
    pub kv: Option<Arc<dyn KvStore>>,
}

pub fn default_import_status() -> Value {
    json!({
        "streams": {},
        "tables": {},
        "rules": {},
        "nativePlugins": {},
        "portablePlugins": {},
        "sourceConfig": {},
        "sinkConfig": {},
        "connectionConfig": {},
        "Service": {},
        "Schema": {},
        "uploads": {},
        "scripts": {}
    })
}

/// Bounded replay buffer for one ruletest session: every emitted row gets a
/// monotonically increasing sequence number and is retained in a capped ring
/// so SSE subscribers can resume/continue without loss, duplicates, or
/// reordering while memory stays bounded.
#[derive(Debug, Default)]
pub struct RuletestReplay {
    entries: VecDeque<(u64, String)>,
    next_seq: u64,
}

/// Retained replay rows per session (10k). Active subscribers track a cursor
/// and stream indefinitely past the cap; a subscriber that falls further
/// behind than the ring skips the evicted prefix (documented lag gap) and
/// resumes at the oldest retained row — never duplicated, never reordered.
/// Late subscribers backfill up to the retained prefix, then go live.
pub const RULETEST_HISTORY_CAP: usize = 10_000;

/// An interactive rule-simulation session: mock source data is replayed
/// through the rule SQL and output rows stream out over SSE. `replay`
/// buffers emitted rows with sequence numbers so subscribers connecting
/// after `start` still receive the replay (no lost-race) and live
/// subscribers continue past the retention cap; `shutdown` stops the loop.
#[derive(Clone)]
pub struct RuletestSession {
    pub id: String,
    pub sql: String,
    pub mock_source: HashMap<String, SimulatorConfig>,
    pub output_tx: tokio::sync::broadcast::Sender<String>,
    pub replay: Arc<RwLock<RuletestReplay>>,
    pub port: u16,
    pub shutdown: Arc<tokio::sync::Notify>,
}

impl AppState {
    pub fn new(
        version: String,
        config: KuiperConfig,
        stream_manager: StreamManager,
        table_manager: TableManager,
        rule_manager: RuleManager,
        stream_bus: StreamBus,
    ) -> Self {
        Self {
            start_time: Instant::now(),
            version,
            config: Arc::new(RwLock::new(config)),
            stream_manager,
            table_manager,
            rule_manager,
            stream_bus,
            connections: Arc::new(RwLock::new(HashMap::new())),
            source_configs: Arc::new(RwLock::new(HashMap::new())),
            sink_configs: Arc::new(RwLock::new(HashMap::new())),
            http_client: reqwest::Client::builder()
                .tcp_nodelay(true)
                .build()
                .unwrap_or_default(),
            ruletests: Arc::new(RwLock::new(HashMap::new())),
            source_cancels: Arc::new(RwLock::new(HashMap::new())),
            schema_manager: SchemaManager::new(),
            plugin_manager: PluginManager::new(),
            trace_manager: TraceManager::new(),
            task_manager: TaskManager::new(),
            portable_plugins: create_default_portables(),
            services: create_default_services(),
            js_udfs: create_default_js_udfs(),
            latest_import_status: Arc::new(RwLock::new(default_import_status())),
            kv: None,
        }
    }
}

/// Persist one connection/source/sink config entry so daemon restarts keep
/// resolving CONF_KEYs (MQTT brokers, SQL URLs) instead of falling back to
/// loopback defaults with silent zero-delivery.
async fn persist_config_entry(state: &AppState, namespace: &str, key: &str, val: &Value) {
    if let Some(kv) = state.kv.as_ref() {
        if let Err(e) = kv.set(namespace, key, &val.to_string()).await {
            tracing::warn!("KV persist {}/{} failed: {}", namespace, key, e);
        }
    }
}

async fn unpersist_config_entry(state: &AppState, namespace: &str, key: &str) {
    if let Some(kv) = state.kv.as_ref() {
        if let Err(e) = kv.delete(namespace, key).await {
            tracing::warn!("KV delete {}/{} failed: {}", namespace, key, e);
        }
    }
}

/// Reload persisted connection/source/sink configs at daemon startup, ahead
/// of [`restore_running_rules`].
pub async fn load_config_maps(state: &AppState) {
    let Some(kv) = state.kv.as_ref() else {
        return;
    };
    for (namespace, map) in [
        ("source_configs", &state.source_configs),
        ("sink_configs", &state.sink_configs),
        ("connections", &state.connections),
    ] {
        match kv.list_all(namespace).await {
            Ok(entries) => {
                let mut guard = map.write();
                for (key, val) in entries {
                    match serde_json::from_str::<Value>(&val) {
                        Ok(parsed) => {
                            guard.insert(key, parsed);
                        }
                        Err(e) => {
                            tracing::warn!("Skipping corrupt {} entry {}: {}", namespace, key, e);
                        }
                    }
                }
            }
            Err(e) => {
                tracing::warn!("KV load {} failed: {}", namespace, e);
            }
        }
    }
}

/// JWT authorization guard for deployments with `basic.authentication:
/// true`. eKuiper accepts only a raw RS256 JWT (no `Bearer ` prefix); `/`
/// and `/ping` stay public.
async fn auth_guard(
    State(state): State<AppState>,
    request: axum::http::Request<axum::body::Body>,
    next: axum::middleware::Next,
) -> Response {
    if !state.config.read().basic.authentication {
        return next.run(request).await;
    }
    let path = request.uri().path();
    if path == "/" || path == "/ping" {
        return next.run(request).await;
    }
    let Some(header) = request
        .headers()
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
    else {
        return (StatusCode::UNAUTHORIZED, "Missing authorization header\n").into_response();
    };
    if header.starts_with("Bearer ") || header.starts_with("bearer ") {
        return (
            StatusCode::UNAUTHORIZED,
            "Bearer token is not supported, please use raw JWT token\n",
        )
            .into_response();
    }
    let Some(key_der) = load_auth_public_key() else {
        return (
            StatusCode::UNAUTHORIZED,
            "Authentication misconfigured: public key not found\n",
        )
            .into_response();
    };
    match verify_jwt_raw(header.trim(), &key_der) {
        Ok(()) => next.run(request).await,
        Err(msg) => (StatusCode::UNAUTHORIZED, msg).into_response(),
    }
}

/// Verify a raw RS256 JWT against a PKCS#1 DER public key: exactly three
/// dot-separated segments, a JSON payload whose optional `exp` (seconds)
/// must lie in the future, and a PKCS#1 v1.5 SHA-256 signature over
/// `header_b64.payload_b64`.
fn verify_jwt_raw(token: &str, public_key_der: &[u8]) -> Result<(), &'static str> {
    use base64::Engine;
    let mut parts = token.split('.');
    let (Some(header_b64), Some(payload_b64), Some(sig_b64), None) =
        (parts.next(), parts.next(), parts.next(), parts.next())
    else {
        return Err("Invalid JWT format\n");
    };
    let engine = base64::engine::general_purpose::URL_SAFE_NO_PAD;
    let payload_bytes = engine
        .decode(payload_b64)
        .map_err(|_| "Invalid JWT format\n")?;
    let payload: serde_json::Value =
        serde_json::from_slice(&payload_bytes).map_err(|_| "Invalid JWT format\n")?;
    if let Some(exp) = payload.get("exp").and_then(|v| v.as_i64()) {
        if chrono::Utc::now().timestamp() > exp {
            return Err("Token has expired\n");
        }
    }
    let signature = engine.decode(sig_b64).map_err(|_| "Invalid JWT format\n")?;
    let message = format!("{}.{}", header_b64, payload_b64);
    let key = ring::signature::UnparsedPublicKey::new(
        &ring::signature::RSA_PKCS1_2048_8192_SHA256,
        public_key_der,
    );
    key.verify(message.as_bytes(), &signature)
        .map_err(|_| "Invalid token signature\n")
}

/// Split one PEM block into its label and DER bytes.
fn parse_pem_block(pem: &[u8]) -> Option<(String, Vec<u8>)> {
    use base64::Engine;
    let text = std::str::from_utf8(pem).ok()?;
    let begin = text.find("-----BEGIN ")?;
    let after_begin = &text[begin + "-----BEGIN ".len()..];
    let label_end = after_begin.find("-----")?;
    let label = after_begin[..label_end].trim().to_string();
    let rest = &after_begin[label_end + "-----".len()..];
    let end = rest.find("-----END ")?;
    let body: String = rest[..end].chars().filter(|c| !c.is_whitespace()).collect();
    let der = base64::engine::general_purpose::STANDARD
        .decode(&body)
        .ok()?;
    Some((label, der))
}

/// Read one DER tag-length-value triple: returns (tag, content, rest).
/// Only definite-form lengths are accepted.
fn der_read_tlv(input: &[u8]) -> Option<(u8, &[u8], &[u8])> {
    if input.len() < 2 {
        return None;
    }
    let tag = input[0];
    let (len, header) = if input[1] < 0x80 {
        (input[1] as usize, 2)
    } else {
        let count = (input[1] & 0x7f) as usize;
        if count == 0 || count > 4 || input.len() < 2 + count {
            return None;
        }
        let mut len = 0usize;
        for b in &input[2..2 + count] {
            len = len.checked_mul(256)?.checked_add(*b as usize)?;
        }
        (len, 2 + count)
    };
    if input.len() < header + len {
        return None;
    }
    Some((tag, &input[header..header + len], &input[header + len..]))
}

/// Unwrap an X.509 SPKI (`PUBLIC KEY`) DER container down to the bare
/// PKCS#1 RSAPublicKey DER that `ring`'s RSA verifier expects. The
/// algorithm must be rsaEncryption (1.2.840.113549.1.1.1).
fn spki_to_pkcs1(spki: &[u8]) -> Option<Vec<u8>> {
    const RSA_ENCRYPTION_OID: &[u8] = &[
        0x06, 0x09, 0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 0x01, 0x01, 0x01,
    ];
    let (tag, content, rest) = der_read_tlv(spki)?;
    if tag != 0x30 || !rest.is_empty() {
        return None;
    }
    let (alg_tag, alg_content, key_rest) = der_read_tlv(content)?;
    if alg_tag != 0x30
        || !alg_content
            .windows(RSA_ENCRYPTION_OID.len())
            .any(|w| w == RSA_ENCRYPTION_OID)
    {
        return None;
    }
    let (bits_tag, bits_content, key_end) = der_read_tlv(key_rest)?;
    if bits_tag != 0x03 || !key_end.is_empty() {
        return None;
    }
    // First BIT STRING byte is the unused-bits count, always zero here.
    let pkcs1 = bits_content.strip_prefix(&[0x00])?;
    Some(pkcs1.to_vec())
}

/// Load the RSA public key for JWT verification: an explicit
/// `KUIPER_AUTH_PUBLIC_KEY_FILE` path first, then the conventional
/// `etc/mgmt/public.pem` and `etc/public.pem` locations. Both SPKI
/// (`PUBLIC KEY`) and bare PKCS#1 (`RSA PUBLIC KEY`) PEM blocks are
/// accepted; either way the PKCS#1 DER that `ring` expects is returned.
fn load_auth_public_key() -> Option<Vec<u8>> {
    let mut candidates = Vec::new();
    if let Ok(path) = std::env::var("KUIPER_AUTH_PUBLIC_KEY_FILE") {
        if !path.trim().is_empty() {
            candidates.push(path);
        }
    }
    candidates.push("etc/mgmt/public.pem".to_string());
    candidates.push("etc/public.pem".to_string());
    candidates.into_iter().find_map(|path| {
        let bytes = std::fs::read(&path).ok()?;
        let (label, der) = parse_pem_block(&bytes)?;
        match label.as_str() {
            "PUBLIC KEY" => spki_to_pkcs1(&der),
            "RSA PUBLIC KEY" => Some(der),
            _ => None,
        }
    })
}

pub fn create_router(state: AppState) -> Router {
    Router::new()
        .route("/ping", get(ping_handler))
        .route("/", get(root_handler).post(root_handler))
        .route("/streams", get(list_streams).post(create_stream))
        .route(
            "/streams/:name",
            get(get_stream).put(update_stream).delete(delete_stream),
        )
        .route("/streams/:name/data", post(push_stream_data))
        .route("/streams/:name/schema", get(get_stream_schema))
        .route("/tables", get(list_tables).post(create_table))
        .route(
            "/tables/:name",
            get(get_table).put(update_table).delete(delete_table),
        )
        .route("/tables/:name/data", post(push_table_data))
        .route("/tables/:name/schema", get(get_table_schema))
        .route("/tabledetails", get(get_table_details))
        .route("/streamdetails", get(get_stream_details))
        .route("/rules", get(list_rules).post(create_rule))
        .route("/rules/validate", post(validate_rule))
        .route("/rules/status/all", get(get_all_rule_status))
        .route(
            "/rules/:name",
            get(get_rule).put(update_rule).delete(delete_rule),
        )
        .route("/rules/:name/status", get(get_rule_status))
        .route("/rules/:name/topo", get(get_rule_topo))
        .route("/rules/:name/explain", get(get_rule_explain))
        .route("/rules/:name/start", post(start_rule))
        .route("/rules/:name/stop", post(stop_rule))
        .route("/rules/:name/restart", post(restart_rule))
        .route("/rules/:name/reset_state", put(reset_rule_state))
        .route("/rules/:id/schema", get(get_rule_schema))
        .route("/rules/:id/cpu", get(get_rule_cpu))
        .route(
            "/rules/:name/tags",
            put(put_rule_tags)
                .patch(patch_rule_tags)
                .delete(delete_rule_tags),
        )
        .route("/v2/rules/:name/status", get(get_rule_status))
        .route("/ruletest", post(create_ruletest))
        .route("/ruletest/:name/start", post(start_ruletest))
        .route("/ruletest/:name", delete(delete_ruletest))
        .route("/test/:name", get(sse_ruletest))
        .route("/rules/:name/trace/start", post(start_rule_trace))
        .route("/rules/:name/trace/stop", post(stop_rule_trace))
        .route("/trace/rule/:rule_id", get(get_rule_traces))
        .route("/trace/:id", get(get_trace_by_id))
        .route("/tracer", post(set_tracer_config))
        .route("/async/data/import", post(async_data_import))
        .route("/async/task/:id", get(async_task_status))
        .route("/async/task/:id/cancel", post(async_task_cancelled))
        .route("/batch/req", post(handle_batch_req))
        .route("/rules/bulkstart", post(bulk_start_rules))
        .route("/rules/bulkstop", post(bulk_stop_rules))
        .route("/rules/usage/cpu", get(rule_cpu_usage))
        .route(
            "/rules/tags/match",
            get(rule_tags_match).post(rule_tags_match),
        )
        .route("/configs", get(get_configs).patch(patch_configs))
        .route(
            "/config/uploads",
            get(get_config_uploads).post(upload_config_file),
        )
        .route("/config/uploads/:name", delete(delete_config_upload))
        .route("/stop", get(stop_server).post(stop_server))
        .route("/data/import", post(import_data))
        .route(
            "/data/export",
            get(export_ruleset).post(export_data_selected),
        )
        .route("/v2/data/import", post(import_data))
        .route(
            "/v2/data/export",
            get(export_ruleset).post(export_data_selected),
        )
        .route("/ruleset/import", post(import_ruleset))
        .route("/ruleset/export", get(export_ruleset).post(export_ruleset))
        .route("/metadata/sources", get(list_source_metadata))
        .route("/metadata/sources/:name", get(get_source_metadata))
        .route("/metadata/sinks", get(list_sink_metadata))
        .route("/metadata/sinks/:name", get(get_sink_metadata))
        .route("/metadata/functions", get(list_function_metadata))
        .route("/metadata/operators", get(list_operator_metadata))
        .route("/metadata/connections", get(list_metadata_connections))
        .route("/metadata/resource", get(list_metadata_resources))
        .route("/metadata/resources", get(list_metadata_resources))
        .route(
            "/connections",
            get(list_connections).post(create_connection),
        )
        .route(
            "/connections/:id",
            get(get_connection)
                .put(update_connection)
                .delete(delete_connection),
        )
        .route(
            "/plugins/sources",
            get(list_source_plugins).post(create_source_plugin),
        )
        .route("/plugins/sources/prebuild", get(list_prebuild_plugins))
        .route(
            "/plugins/sources/:name",
            get(get_source_plugin)
                .put(update_source_plugin)
                .delete(delete_source_plugin),
        )
        .route(
            "/plugins/sinks",
            get(list_sink_plugins).post(create_sink_plugin),
        )
        .route("/plugins/sinks/prebuild", get(list_prebuild_plugins))
        .route(
            "/plugins/sinks/:name",
            get(get_sink_plugin)
                .put(update_sink_plugin)
                .delete(delete_sink_plugin),
        )
        .route(
            "/plugins/functions",
            get(list_function_plugins).post(create_function_plugin),
        )
        .route("/plugins/functions/prebuild", get(list_prebuild_plugins))
        .route(
            "/plugins/functions/:name",
            get(get_function_plugin)
                .put(update_function_plugin)
                .delete(delete_function_plugin),
        )
        .route(
            "/plugins/functions/:name/register",
            post(register_function_plugin),
        )
        .route(
            "/plugins/portables",
            get(list_portable_plugins).post(create_portable_plugin),
        )
        .route(
            "/plugins/portables/:name",
            get(get_portable_plugin)
                .put(update_portable_plugin)
                .delete(delete_portable_plugin),
        )
        .route(
            "/plugins/portables/:name/status",
            get(get_portable_plugin_status),
        )
        .route(
            "/plugins/udfs",
            get(list_udf_plugins).post(create_udf_plugin),
        )
        .route(
            "/plugins/udfs/:name",
            get(get_udf_plugin).delete(delete_udf_plugin),
        )
        .route("/services", get(list_services).post(create_service))
        .route(
            "/services/:name",
            get(get_service).put(update_service).delete(delete_service),
        )
        .route("/services/functions", get(list_service_functions))
        .route("/services/functions/:name", get(get_service_function))
        .route(
            "/udf/javascript",
            get(list_javascript_udfs).post(create_javascript_udf),
        )
        .route(
            "/udf/javascript/:id",
            get(get_javascript_udf)
                .put(update_javascript_udf)
                .delete(delete_javascript_udf),
        )
        .route("/schemas/:kind", get(list_schemas).post(create_schema))
        .route(
            "/schemas/:kind/:name",
            get(get_schema).put(update_schema).delete(delete_schema),
        )
        .route("/schemas/:kind/:name/upload", put(upload_schema))
        .route("/metadata/connections/:name", get(get_connection_metadata))
        .route("/metadata/sources/yaml/:name", get(get_source_yaml))
        .route(
            "/metadata/sources/:name/confKeys/:conf_key",
            get(get_source_conf_key)
                .put(save_source_conf_key)
                .post(save_source_conf_key)
                .delete(delete_source_conf_key),
        )
        .route("/metadata/sinks/yaml/:name", get(get_sink_yaml))
        .route(
            "/metadata/sinks/:name/confKeys/:conf_key",
            get(get_sink_conf_key)
                .put(save_sink_conf_key)
                .post(save_sink_conf_key)
                .delete(delete_sink_conf_key),
        )
        .route("/metadata/connections/yaml/:name", get(get_connection_yaml))
        .route(
            "/metadata/connections/:name/confKeys/:conf_key",
            get(get_connection_conf_key)
                .put(save_connection_conf_key)
                .post(save_connection_conf_key)
                .delete(delete_connection_conf_key),
        )
        .route(
            "/metadata/sources/connection/:name",
            post(register_source_connection),
        )
        .route(
            "/metadata/sinks/connection/:name",
            post(register_sink_connection),
        )
        .route(
            "/metadata/lookups/connection/:name",
            post(register_lookup_connection),
        )
        .route("/data/import/status", get(import_status))
        .route("/metrics/dump", get(metrics_dump))
        .route("/metrics/dump/check", get(metrics_dump))
        .route("/metrics", get(prometheus_metrics_handler))
        .layer(axum::middleware::from_fn_with_state(
            state.clone(),
            auth_guard,
        ))
        .with_state(state)
}

async fn ping_handler() -> impl IntoResponse {
    (StatusCode::OK, "pong")
}

async fn root_handler(State(state): State<AppState>) -> impl IntoResponse {
    let mut sys = System::new_with_specifics(
        RefreshKind::new()
            .with_cpu(CpuRefreshKind::everything())
            .with_memory(MemoryRefreshKind::everything()),
    );
    sys.refresh_all();

    let uptime = state.start_time.elapsed().as_secs();
    let cpu_usage = sys.global_cpu_info().cpu_usage();
    let memory_used = sys.used_memory();
    let memory_total = sys.total_memory();

    let info = json!({
        "version": state.version,
        "os": std::env::consts::OS,
        "arch": std::env::consts::ARCH,
        "upTimeSeconds": uptime,
        "cpuUsage": cpu_usage,
        "memoryUsed": memory_used,
        "memoryTotal": memory_total,
    });

    (StatusCode::OK, Json(info))
}

#[derive(Deserialize)]
struct CreateStreamPayload {
    #[serde(default)]
    sql: Option<String>,
    #[serde(default)]
    name: Option<String>,
}

/// Map parsed `CREATE` columns onto stored stream/table fields.
fn to_stream_fields(cols: Vec<StreamColumn>) -> Vec<StreamField> {
    cols.into_iter()
        .map(|c| StreamField {
            name: c.name,
            field_type: c.data_type,
        })
        .collect()
}

/// eKuiper-style describe envelope shared by `GET` and `DESCRIBE` paths.
fn describe_stream(def: &StreamDefinition) -> Value {
    json!({
        "Name": def.name,
        "StreamFields": def.stream_fields,
        "Options": def.options,
        "StreamType": "stream",
    })
}

/// eKuiper-style describe envelope for lookup tables.
fn describe_table(def: &TableDefinition) -> Value {
    json!({
        "Name": def.name,
        "StreamFields": def.stream_fields,
        "Options": def.options,
        "StreamType": "table",
    })
}

async fn list_streams(State(state): State<AppState>) -> impl IntoResponse {
    let streams = state.stream_manager.list_streams();
    Json(streams)
}

async fn create_stream(
    State(state): State<AppState>,
    Json(payload): Json<CreateStreamPayload>,
) -> Response {
    if let Some(sql) = payload.sql {
        // Stream management statements run inline: SHOW STREAMS lists names,
        // DESCRIBE STREAM reports one definition (both answer 201).
        let mut words = sql.split_whitespace();
        let head = (
            words.next().map(|w| w.to_ascii_uppercase()),
            words.next().map(|w| w.to_ascii_uppercase()),
        );
        if head == (Some("SHOW".to_string()), Some("STREAMS".to_string())) {
            return (
                StatusCode::CREATED,
                Json(state.stream_manager.list_streams()),
            )
                .into_response();
        }
        if head == (Some("DESCRIBE".to_string()), Some("STREAM".to_string())) {
            let target = words
                .next()
                .unwrap_or("")
                .trim_matches(['"', '\'', '`', ';']);
            if target.is_empty() {
                return (
                    StatusCode::BAD_REQUEST,
                    "Missing stream name in DESCRIBE STREAM",
                )
                    .into_response();
            }
            if let Some(def) = state.stream_manager.get_stream(target) {
                return (StatusCode::CREATED, Json(describe_stream(&def))).into_response();
            }
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({
                    "error": 3000,
                    "message": format!(
                        "describe stream error: Describe stream fails, {} is not found.",
                        target
                    )
                })),
            )
                .into_response();
        }
        let mut parser = Parser::new(&sql);
        match parser.parse_create_stream() {
            Ok(stmt) => {
                let stream_def = StreamDefinition {
                    name: stmt.name.clone(),
                    sql: sql.clone(),
                    stream_fields: to_stream_fields(stmt.fields),
                    options: stmt.options,
                };
                if let Err(e) = state.stream_manager.create_stream(stream_def).await {
                    return (StatusCode::BAD_REQUEST, e.to_string()).into_response();
                }
                state.stream_bus.get_or_create(&stmt.name);
                (
                    StatusCode::CREATED,
                    format!("Stream {} is created.\n", stmt.name),
                )
                    .into_response()
            }
            Err(e) => (StatusCode::BAD_REQUEST, format!("Invalid SQL: {}", e)).into_response(),
        }
    } else if let Some(name) = payload.name {
        let stream_def = StreamDefinition {
            name: name.clone(),
            sql: "".to_string(),
            stream_fields: Vec::new(),
            options: HashMap::new(),
        };
        if let Err(e) = state.stream_manager.create_stream(stream_def).await {
            return (StatusCode::BAD_REQUEST, e.to_string()).into_response();
        }
        state.stream_bus.get_or_create(&name);
        (
            StatusCode::CREATED,
            format!("Stream {} is created.\n", name),
        )
            .into_response()
    } else {
        (StatusCode::BAD_REQUEST, "Missing sql or name in request").into_response()
    }
}

async fn get_stream(State(state): State<AppState>, Path(name): Path<String>) -> Response {
    if let Err(resp) = check_valid_name(&name) {
        return resp;
    }
    if let Some(def) = state.stream_manager.get_stream(&name) {
        Json(describe_stream(&def)).into_response()
    } else {
        (
            StatusCode::BAD_REQUEST,
            Json(json!({
                "error": 3000,
                "message": format!(
                    "describe stream error: Describe stream fails, {} is not found.",
                    name
                )
            })),
        )
            .into_response()
    }
}

async fn delete_stream(State(state): State<AppState>, Path(name): Path<String>) -> Response {
    if let Err(resp) = check_valid_name(&name) {
        return resp;
    }
    match state.stream_manager.delete_stream(&name).await {
        Ok(_) => (StatusCode::OK, format!("Stream {} is dropped.\n", name)).into_response(),
        Err(e) => (StatusCode::NOT_FOUND, e.to_string()).into_response(),
    }
}

/// Replace a stream definition (eKuiper `PUT /streams/:name`). Accepts raw
/// `CREATE STREAM ...` DDL or a JSON envelope carrying `sql`; the path name
/// is canonical. Missing streams 404 instead of being silently created.
async fn update_stream(
    State(state): State<AppState>,
    Path(name): Path<String>,
    body: Bytes,
) -> Response {
    if let Err(resp) = check_valid_name(&name) {
        return resp;
    }
    if state.stream_manager.get_stream(&name).is_none() {
        return (StatusCode::NOT_FOUND, format!("Stream {} not found", name)).into_response();
    }
    if body.is_empty() {
        return (StatusCode::BAD_REQUEST, "Missing stream definition").into_response();
    }
    let sql = match serde_json::from_slice::<Value>(&body) {
        Ok(Value::Object(map)) => match map.get("sql").and_then(|v| v.as_str()) {
            Some(s) => s.to_string(),
            None => return (StatusCode::BAD_REQUEST, "Missing sql in request").into_response(),
        },
        Ok(_) => return (StatusCode::BAD_REQUEST, "Missing sql in request").into_response(),
        Err(_) => match String::from_utf8(body.to_vec()) {
            Ok(s) => s,
            Err(_) => {
                return (StatusCode::BAD_REQUEST, "Invalid stream definition").into_response();
            }
        },
    };
    let mut parser = Parser::new(&sql);
    let stmt = match parser.parse_create_stream() {
        Ok(s) => s,
        Err(e) => return (StatusCode::BAD_REQUEST, format!("Invalid SQL: {}", e)).into_response(),
    };
    let _ = state.stream_manager.delete_stream(&name).await;
    let stream_def = StreamDefinition {
        name: name.clone(),
        sql: sql.clone(),
        stream_fields: to_stream_fields(stmt.fields),
        options: stmt.options,
    };
    if let Err(e) = state.stream_manager.create_stream(stream_def).await {
        return (StatusCode::BAD_REQUEST, e.to_string()).into_response();
    }
    state.stream_bus.get_or_create(&name);
    (StatusCode::OK, format!("Stream {} is updated.\n", name)).into_response()
}

/// HTTP push source following the eKuiper REST API.
///
/// Accepts either a single JSON object or an array of JSON objects and
/// publishes each one to the stream bus.
async fn push_stream_data(
    State(state): State<AppState>,
    Path(name): Path<String>,
    Json(payload): Json<Value>,
) -> Response {
    if state.stream_manager.get_stream(&name).is_none() {
        return (StatusCode::NOT_FOUND, format!("Stream {} not found", name)).into_response();
    }

    let objects: Vec<Value> = match payload {
        Value::Array(items) => items,
        Value::Object(_) => vec![payload],
        _ => {
            return (
                StatusCode::BAD_REQUEST,
                "Expected a JSON object or an array of JSON objects",
            )
                .into_response();
        }
    };

    for item in objects {
        let Value::Object(map) = item else {
            return (
                StatusCode::BAD_REQUEST,
                "Expected a JSON object or an array of JSON objects",
            )
                .into_response();
        };
        let data: HashMap<String, Value> = map.into_iter().collect();
        let record = rekuiper_core::model::StreamRecord::new(data);
        // No subscribers (e.g. no rules yet) is fine for ingestion.
        let _ = state.stream_bus.publish(&name, record);
    }

    (StatusCode::OK, "Data ingested successfully.\n").into_response()
}

async fn list_tables(State(state): State<AppState>) -> impl IntoResponse {
    let tables = state.table_manager.list_tables();
    Json(tables)
}

/// Table lookup ingestion: accepts a single JSON object or an array of
/// objects and appends each as a lookup row for the table.
async fn push_table_data(
    State(state): State<AppState>,
    Path(name): Path<String>,
    Json(payload): Json<Value>,
) -> Response {
    let objects: Vec<Value> = match payload {
        Value::Array(items) => items,
        Value::Object(_) => vec![payload],
        _ => {
            return (
                StatusCode::BAD_REQUEST,
                "Expected a JSON object or an array of JSON objects",
            )
                .into_response();
        }
    };

    for item in objects {
        let Value::Object(map) = item else {
            return (
                StatusCode::BAD_REQUEST,
                "Expected a JSON object or an array of JSON objects",
            )
                .into_response();
        };
        let row: HashMap<String, Value> = map.into_iter().collect();
        state.table_manager.insert_table_row(&name, row);
    }

    (StatusCode::OK, "Table data ingested successfully.\n").into_response()
}

async fn create_table(
    State(state): State<AppState>,
    Json(payload): Json<CreateStreamPayload>,
) -> Response {
    let Some(sql) = payload.sql else {
        return (StatusCode::BAD_REQUEST, "Missing sql in request").into_response();
    };
    let mut parser = Parser::new(&sql);
    match parser.parse_create_table() {
        Ok(stmt) => {
            let table_def = TableDefinition {
                name: stmt.name.clone(),
                sql: sql.clone(),
                stream_fields: to_stream_fields(stmt.fields),
                options: stmt.options,
            };
            if let Err(e) = state.table_manager.create_table(table_def).await {
                return (StatusCode::BAD_REQUEST, e.to_string()).into_response();
            }
            (
                StatusCode::CREATED,
                format!("Table {} is created.\n", stmt.name),
            )
                .into_response()
        }
        Err(e) => (StatusCode::BAD_REQUEST, format!("Invalid SQL: {}", e)).into_response(),
    }
}

async fn get_table(State(state): State<AppState>, Path(name): Path<String>) -> Response {
    if let Err(resp) = check_valid_name(&name) {
        return resp;
    }
    if let Some(def) = state.table_manager.get_table(&name) {
        Json(describe_table(&def)).into_response()
    } else {
        (
            StatusCode::BAD_REQUEST,
            Json(json!({
                "error": 3000,
                "message": format!(
                    "describe table error: Describe table fails, {} is not found.",
                    name
                )
            })),
        )
            .into_response()
    }
}

async fn delete_table(State(state): State<AppState>, Path(name): Path<String>) -> Response {
    if let Err(resp) = check_valid_name(&name) {
        return resp;
    }
    match state.table_manager.delete_table(&name).await {
        Ok(_) => (StatusCode::OK, format!("Table {} is dropped.\n", name)).into_response(),
        Err(e) => (StatusCode::NOT_FOUND, e.to_string()).into_response(),
    }
}

/// Replace a table definition (eKuiper `PUT /tables/:name`). Accepts raw
/// `CREATE TABLE ...` DDL or a JSON envelope carrying `sql`; the path name
/// is canonical. Missing tables 404 instead of being silently created.
async fn update_table(
    State(state): State<AppState>,
    Path(name): Path<String>,
    body: Bytes,
) -> Response {
    if let Err(resp) = check_valid_name(&name) {
        return resp;
    }
    if state.table_manager.get_table(&name).is_none() {
        return (StatusCode::NOT_FOUND, format!("Table {} not found", name)).into_response();
    }
    if body.is_empty() {
        return (StatusCode::BAD_REQUEST, "Missing table definition").into_response();
    }
    let sql = match serde_json::from_slice::<Value>(&body) {
        Ok(Value::Object(map)) => match map.get("sql").and_then(|v| v.as_str()) {
            Some(s) => s.to_string(),
            None => return (StatusCode::BAD_REQUEST, "Missing sql in request").into_response(),
        },
        Ok(_) => return (StatusCode::BAD_REQUEST, "Missing sql in request").into_response(),
        Err(_) => match String::from_utf8(body.to_vec()) {
            Ok(s) => s,
            Err(_) => {
                return (StatusCode::BAD_REQUEST, "Invalid table definition").into_response();
            }
        },
    };
    let mut parser = Parser::new(&sql);
    let stmt = match parser.parse_create_table() {
        Ok(s) => s,
        Err(e) => return (StatusCode::BAD_REQUEST, format!("Invalid SQL: {}", e)).into_response(),
    };
    let _ = state.table_manager.delete_table(&name).await;
    let table_def = TableDefinition {
        name: name.clone(),
        sql: sql.clone(),
        stream_fields: to_stream_fields(stmt.fields),
        options: stmt.options,
    };
    if let Err(e) = state.table_manager.create_table(table_def).await {
        return (StatusCode::BAD_REQUEST, e.to_string()).into_response();
    }
    (StatusCode::OK, format!("Table {} is updated.\n", name)).into_response()
}

async fn get_table_details(State(state): State<AppState>) -> impl IntoResponse {
    Json(state.table_manager.list_table_definitions())
}

async fn get_stream_details(State(state): State<AppState>) -> impl IntoResponse {
    let defs: Vec<StreamDefinition> = {
        let names = state.stream_manager.list_streams();
        names
            .iter()
            .filter_map(|n| state.stream_manager.get_stream(n))
            .collect()
    };
    Json(defs)
}

/// Field-type map for a describe subject: `{name: {type, index}}`.
fn field_schema_map(fields: &[StreamField]) -> Value {
    let mut schema_map = serde_json::Map::new();
    for (idx, field) in fields.iter().enumerate() {
        schema_map.insert(
            field.name.clone(),
            json!({
                "type": field.field_type.to_ascii_lowercase(),
                "index": idx
            }),
        );
    }
    Value::Object(schema_map)
}

async fn get_stream_schema(State(state): State<AppState>, Path(name): Path<String>) -> Response {
    if let Some(def) = state.stream_manager.get_stream(&name) {
        Json(field_schema_map(&def.stream_fields)).into_response()
    } else {
        (
            StatusCode::BAD_REQUEST,
            Json(json!({
                "error": 3000,
                "message": format!(
                    "describe stream error: Describe stream fails, {} is not found.",
                    name
                )
            })),
        )
            .into_response()
    }
}

async fn get_table_schema(State(state): State<AppState>, Path(name): Path<String>) -> Response {
    if let Some(def) = state.table_manager.get_table(&name) {
        Json(field_schema_map(&def.stream_fields)).into_response()
    } else {
        (
            StatusCode::BAD_REQUEST,
            Json(json!({
                "error": 3000,
                "message": format!(
                    "describe table error: Describe table fails, {} is not found.",
                    name
                )
            })),
        )
            .into_response()
    }
}

async fn list_rules(State(state): State<AppState>) -> impl IntoResponse {
    let rules = state.rule_manager.list_rules();
    let summaries: Vec<Value> = rules
        .into_iter()
        .map(|r| {
            json!({
                "id": r.id,
                "name": r.id,
                "sql": r.sql,
            })
        })
        .collect();
    Json(summaries)
}

async fn create_rule(
    State(state): State<AppState>,
    Json(mut rule): Json<RuleDefinition>,
) -> Response {
    // Graph rules carry no SQL: compile the DAG into SQL + actions first.
    if rule.sql.trim().is_empty() {
        if let Some(ref graph) = rule.graph {
            match compile_graph_to_sql_and_actions(graph) {
                Ok((sql, actions)) => {
                    rule.sql = sql;
                    if rule.actions.is_empty() {
                        rule.actions = actions;
                    }
                }
                Err(e) => {
                    return (
                        StatusCode::BAD_REQUEST,
                        format!("Invalid rule graph: {}", e),
                    )
                        .into_response();
                }
            }
        }
    }
    let mut parser = Parser::new(&rule.sql);
    let select_stmt = match parser.parse_select() {
        Ok(s) => s,
        Err(e) => {
            return (StatusCode::BAD_REQUEST, format!("Invalid rule SQL: {}", e)).into_response();
        }
    };
    if let Some(resp) = reject_invalid_rule(&state, &select_stmt) {
        return resp;
    }

    let rule_id = rule.id.clone();

    if let Err(e) = state.rule_manager.create_rule(rule.clone()).await {
        return (StatusCode::BAD_REQUEST, e.to_string()).into_response();
    }

    // Spawn window-aware rule execution task and register its handle so
    // stop/delete can abort it cleanly.
    spawn_rule_task(
        &state.rule_manager,
        &state.stream_bus,
        &state.stream_manager,
        &state.table_manager,
        &state.source_configs,
        &state.http_client,
        &state.trace_manager,
        rule_id.clone(),
        select_stmt.clone(),
        rule.actions.clone(),
        rule.options.clone(),
    );

    bootstrap_rule_sources(&state, &rule_id, &select_stmt);

    (
        StatusCode::CREATED,
        format!("Rule {} was created successfully.\n", rule_id),
    )
        .into_response()
}

/// Starts background source producers (HTTP pull, WebSocket, RedisSub, Kafka,
/// simulator) for a rule based on its source stream type. Shared by rule
/// creation and daemon-bootstrap restore.
/// Resolve the MQTT source configuration for a rule's stream.
///
/// MQTT is the default streaming source: streams with no `TYPE`, an empty
/// `TYPE`, or `TYPE="mqtt"` ingest from the broker. Any other non-empty
/// `TYPE` (recognized sources like `kafka`, or anything else) resolves to
/// `None` here so exactly one source bootstrap owns the stream.
fn resolve_mqtt_source(
    stream_manager: &StreamManager,
    source_configs: &Arc<RwLock<HashMap<String, Value>>>,
    stream_name: &str,
    rule_id: &str,
) -> Option<MqttConfig> {
    let def = stream_manager.get_stream(stream_name)?;
    if let Some(kind) = def.options.get("TYPE") {
        if !kind.trim().is_empty() && !kind.eq_ignore_ascii_case("mqtt") {
            return None;
        }
    }
    let mut config = MqttConfig {
        server: "tcp://127.0.0.1:1883".to_string(),
        topic: String::new(),
        client_id: None,
        qos: 0,
        username: None,
        password: None,
    };
    // CONF_KEY lookup is case-insensitive (`CONF_KEY`, `conf_key`,
    // `confKey`): SQL definitions and imported/JSON definitions disagree on
    // case. Stored configs typically carry only connection parameters, so a
    // failed full decode still salvages server/credentials field by field.
    let conf_key = def
        .options
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case("CONF_KEY") || k.eq_ignore_ascii_case("confKey"))
        .map(|(_, v)| v.trim());
    if let Some(key) = conf_key {
        if !key.is_empty() {
            let lookup1 = format!("mqtt/{}", key);
            let configs_guard = source_configs.read();
            let conf_val = configs_guard
                .get(&lookup1)
                .or_else(|| configs_guard.get(key))
                .cloned();
            drop(configs_guard);
            if let Some(val) = conf_val {
                if let Ok(stored) = serde_json::from_value::<MqttConfig>(val.clone()) {
                    config = stored;
                } else if let Some(srv) = val.get("server").and_then(|v| v.as_str()) {
                    if !srv.trim().is_empty() {
                        config.server = srv.to_string();
                    }
                    if let Some(u) = val.get("username").and_then(|v| v.as_str()) {
                        config.username = Some(u.to_string());
                    }
                    if let Some(p) = val.get("password").and_then(|v| v.as_str()) {
                        config.password = Some(p.to_string());
                    }
                } else {
                    tracing::warn!(
                        "[RULE {}] invalid mqtt config '{}': no server field",
                        rule_id,
                        lookup1,
                    );
                }
            }
        }
    }
    // Direct SERVER option overrides the configuration key (case-insensitive).
    let server_opt = def
        .options
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case("SERVER"))
        .map(|(_, v)| v.trim());
    if let Some(srv) = server_opt {
        if !srv.is_empty() {
            config.server = srv.to_string();
        }
    }
    // DATASOURCE / topic (case-insensitive).
    let topic_opt = def
        .options
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case("DATASOURCE") || k.eq_ignore_ascii_case("topic"))
        .map(|(_, v)| v.trim());
    if let Some(top) = topic_opt {
        if !top.is_empty() {
            config.topic = top.to_string();
        }
    }
    if config.topic.trim().is_empty() {
        config.topic = stream_name.to_string();
    }
    let client_id = def
        .options
        .get("CLIENTID")
        .or_else(|| def.options.get("CLIENT_ID"));
    if let Some(id) = client_id {
        if !id.trim().is_empty() {
            config.client_id = Some(id.clone());
        }
    }
    if let Some(user) = def.options.get("USERNAME") {
        if !user.is_empty() {
            config.username = Some(user.clone());
        }
    }
    if let Some(pass) = def.options.get("PASSWORD") {
        if !pass.is_empty() {
            config.password = Some(pass.clone());
        }
    }
    if let Some(qos) = def.options.get("QOS") {
        match qos.trim().parse::<u8>() {
            Ok(q) => config.qos = q,
            Err(_) => {
                tracing::warn!(
                    "[RULE {}] invalid mqtt QOS '{}', keeping {}",
                    rule_id,
                    qos,
                    config.qos
                );
            }
        }
    }
    Some(config)
}

/// Start source producers for one stream (MQTT/file/HTTP-pull/WebSocket/
/// RedisSub/Kafka/SQL/simulator, whichever its TYPE declares).
fn bootstrap_stream_sources(state: &AppState, rule_id: &str, stream_name: &str) {
    // MQTT is the default streaming source: typeless streams and TYPE="mqtt"
    // subscribe to the broker topic and feed the rule pipeline.
    if let Some(config) = resolve_mqtt_source(
        &state.stream_manager,
        &state.source_configs,
        stream_name,
        rule_id,
    ) {
        let stream_tx = state.stream_bus.get_or_create(stream_name);
        let (cancel_tx, cancel_rx) = tokio::sync::watch::channel(false);
        state
            .source_cancels
            .write()
            .entry(rule_id.to_string())
            .or_default()
            .push(cancel_tx);
        MqttSource::new(config, stream_tx).spawn(cancel_rx);
    }

    // File source streams tail a line-delimited file into the stream bus.
    if let Some(config) = resolve_file_source(
        &state.stream_manager,
        &state.source_configs,
        stream_name,
        rule_id,
    ) {
        let stream_tx = state.stream_bus.get_or_create(stream_name);
        let (cancel_tx, cancel_rx) = tokio::sync::watch::channel(false);
        state
            .source_cancels
            .write()
            .entry(rule_id.to_string())
            .or_default()
            .push(cancel_tx);
        FileSource::new(config, stream_tx).spawn(cancel_rx);
    }

    // HTTP pull source streams poll a remote endpoint into the stream bus.
    if let Some(conf) = resolve_httppull_config(
        &state.stream_manager,
        &state.source_configs,
        stream_name,
        rule_id,
    ) {
        let stream_tx = state.stream_bus.get_or_create(stream_name);
        let (cancel_tx, cancel_rx) = tokio::sync::watch::channel(false);
        state
            .source_cancels
            .write()
            .entry(rule_id.to_string())
            .or_default()
            .push(cancel_tx);
        HttpPullSource {
            config: conf,
            tx: stream_tx,
        }
        .spawn(cancel_rx);
    }

    // WebSocket source streams forward incoming messages into the stream bus.
    if let Some(url) = resolve_websocket_url(
        &state.stream_manager,
        &state.source_configs,
        stream_name,
        rule_id,
    ) {
        let stream_tx = state.stream_bus.get_or_create(stream_name);
        let (cancel_tx, cancel_rx) = tokio::sync::watch::channel(false);
        state
            .source_cancels
            .write()
            .entry(rule_id.to_string())
            .or_default()
            .push(cancel_tx);
        WebSocketSource { url, tx: stream_tx }.spawn(cancel_rx);
    }

    // Redis subscription streams forward channel messages into the stream bus.
    if let Some((url, channel)) = resolve_redissub_source(
        &state.stream_manager,
        &state.source_configs,
        stream_name,
        rule_id,
    ) {
        let stream_tx = state.stream_bus.get_or_create(stream_name);
        let (cancel_tx, cancel_rx) = tokio::sync::watch::channel(false);
        state
            .source_cancels
            .write()
            .entry(rule_id.to_string())
            .or_default()
            .push(cancel_tx);
        RedisSubSource {
            url,
            channel,
            tx: stream_tx,
        }
        .spawn(cancel_rx);
    }

    // Kafka source streams consume a topic partition into the stream bus.
    if let Some(config) = resolve_kafka_source(
        &state.stream_manager,
        &state.source_configs,
        stream_name,
        rule_id,
    ) {
        let stream_tx = state.stream_bus.get_or_create(stream_name);
        let (cancel_tx, cancel_rx) = tokio::sync::watch::channel(false);
        state
            .source_cancels
            .write()
            .entry(rule_id.to_string())
            .or_default()
            .push(cancel_tx);
        KafkaSource {
            config,
            tx: stream_tx,
        }
        .spawn(cancel_rx);
    }

    // SQL source streams poll a database table into the stream bus.
    if let Some(config) = resolve_sql_source(
        &state.stream_manager,
        &state.source_configs,
        stream_name,
        rule_id,
    ) {
        let stream_tx = state.stream_bus.get_or_create(stream_name);
        let (cancel_tx, cancel_rx) = tokio::sync::watch::channel(false);
        state
            .source_cancels
            .write()
            .entry(rule_id.to_string())
            .or_default()
            .push(cancel_tx);
        SqlSource::new(config, stream_tx).spawn(cancel_rx);
    }

    // Simulator source streams replay configured data into the stream bus.
    // Stream options are upper-cased by the SQL parser.
    if let Some(def) = state.stream_manager.get_stream(stream_name) {
        let is_simulator = def
            .options
            .get("TYPE")
            .is_some_and(|t| t.eq_ignore_ascii_case("simulator"));
        if is_simulator {
            if let Some(key) = def.options.get("CONF_KEY").cloned() {
                let lookup = format!("simulator/{}", key);
                if let Some(conf_val) = state.source_configs.read().get(&lookup).cloned() {
                    match serde_json::from_value::<SimulatorConfig>(conf_val) {
                        Ok(conf) => {
                            let stream_name = stream_name.to_string();
                            let bus = state.stream_bus.clone();
                            tokio::spawn(async move {
                                let (tx, mut rx) = tokio::sync::mpsc::channel::<StreamRecord>(1024);
                                let sim_handle = tokio::spawn(async move {
                                    SimulatorSource::new(conf).run(tx).await
                                });
                                while let Some(record) = rx.recv().await {
                                    let _ = bus.publish(&stream_name, record);
                                }
                                let _ = sim_handle.await;
                            });
                        }
                        Err(e) => {
                            tracing::warn!(
                                "[RULE {}] invalid simulator config '{}': {}",
                                rule_id,
                                lookup,
                                e
                            );
                        }
                    }
                }
            }
        }
    }
}

/// Start source producers for a rule: its FROM stream plus every joined
/// stream (stream-stream joins fan in both sides; table targets resolve
/// per-row through lookups and need no producer).
fn bootstrap_rule_sources(state: &AppState, rule_id: &str, select_stmt: &SelectStmt) {
    bootstrap_stream_sources(state, rule_id, &select_stmt.from);
    for join in &select_stmt.joins {
        if state.table_manager.get_table(&join.target).is_none() && join.target != select_stmt.from
        {
            bootstrap_stream_sources(state, rule_id, &join.target);
        }
    }
}

/// Respawns execution tasks (plus source producers) for every rule whose
/// persisted status is `running`, so a restarted daemon resumes processing
/// without manual intervention.
pub async fn restore_running_rules(state: &AppState) {
    for rule in state.rule_manager.list_rules() {
        let running = state
            .rule_manager
            .get_rule_status(&rule.id)
            .is_some_and(|s| s.status == "running");
        if !running {
            continue;
        }
        let mut parser = Parser::new(&rule.sql);
        let select_stmt = match parser.parse_select() {
            Ok(stmt) => stmt,
            Err(e) => {
                tracing::warn!("Skipping restore of rule {}: invalid SQL: {}", rule.id, e);
                continue;
            }
        };
        spawn_rule_task(
            &state.rule_manager,
            &state.stream_bus,
            &state.stream_manager,
            &state.table_manager,
            &state.source_configs,
            &state.http_client,
            &state.trace_manager,
            rule.id.clone(),
            select_stmt.clone(),
            rule.actions.clone(),
            rule.options.clone(),
        );
        bootstrap_rule_sources(state, &rule.id, &select_stmt);
        tracing::info!("Restored running rule {}", rule.id);
    }
}

/// Resolve the HTTP pull configuration for a rule whose source stream
/// declares `TYPE="httppull"` (or `"http_pull"`).
///
/// Looks up `httppull/{conf_key}` then `http_pull/{conf_key}` in the stored
/// source configs; when no key matches, falls back to the stream's
/// `DATASOURCE` property as the poll URL with default method/interval.
fn resolve_httppull_config(
    stream_manager: &StreamManager,
    source_configs: &Arc<RwLock<HashMap<String, Value>>>,
    stream_name: &str,
    rule_id: &str,
) -> Option<HttpPullConfig> {
    let def = stream_manager.get_stream(stream_name)?;
    let kind = def.options.get("TYPE")?;
    if !(kind.eq_ignore_ascii_case("httppull") || kind.eq_ignore_ascii_case("http_pull")) {
        return None;
    }
    if let Some(key) = def.options.get("CONF_KEY") {
        if !key.is_empty() {
            for prefix in ["httppull", "http_pull"] {
                let lookup = format!("{}/{}", prefix, key);
                if let Some(conf_val) = source_configs.read().get(&lookup).cloned() {
                    match serde_json::from_value::<HttpPullConfig>(conf_val) {
                        Ok(conf) => return Some(conf),
                        Err(e) => {
                            tracing::warn!(
                                "[RULE {}] invalid http pull config '{}': {}",
                                rule_id,
                                lookup,
                                e
                            );
                            return None;
                        }
                    }
                }
            }
        }
    }
    let url = def.options.get("DATASOURCE").cloned().unwrap_or_default();
    if url.is_empty() {
        tracing::warn!(
            "[RULE {}] http pull stream '{}' has neither CONF_KEY config nor DATASOURCE url",
            rule_id,
            stream_name
        );
        return None;
    }
    Some(HttpPullConfig {
        url,
        method: "get".to_string(),
        interval: 1000,
        headers: HashMap::new(),
        body: None,
    })
}

/// Resolve the WebSocket URL for a rule whose source stream declares
/// `TYPE="websocket"`.
///
/// Looks up `websocket/{conf_key}` in the stored source configs and builds
/// the URL from it; otherwise parses `DATASOURCE` (used directly when it
/// starts with `ws://` or `wss://`, else treated as a path on the default
/// local endpoint).
fn resolve_websocket_url(
    stream_manager: &StreamManager,
    source_configs: &Arc<RwLock<HashMap<String, Value>>>,
    stream_name: &str,
    rule_id: &str,
) -> Option<String> {
    let def = stream_manager.get_stream(stream_name)?;
    let kind = def.options.get("TYPE")?;
    if !kind.eq_ignore_ascii_case("websocket") {
        return None;
    }
    if let Some(key) = def.options.get("CONF_KEY") {
        if !key.is_empty() {
            let lookup = format!("websocket/{}", key);
            if let Some(conf_val) = source_configs.read().get(&lookup).cloned() {
                match serde_json::from_value::<WebSocketConfig>(conf_val) {
                    Ok(conf) => return Some(conf.target_url()),
                    Err(e) => {
                        tracing::warn!(
                            "[RULE {}] invalid websocket config '{}': {}",
                            rule_id,
                            lookup,
                            e
                        );
                        return None;
                    }
                }
            }
        }
    }
    let datasource = def.options.get("DATASOURCE").cloned().unwrap_or_default();
    if datasource.is_empty() {
        tracing::warn!(
            "[RULE {}] websocket stream '{}' has neither CONF_KEY config nor DATASOURCE",
            rule_id,
            stream_name
        );
        return None;
    }
    let ds = datasource.trim();
    if ds.starts_with("ws://") || ds.starts_with("wss://") {
        Some(ds.to_string())
    } else {
        Some(format!(
            "ws://127.0.0.1:8080/{}",
            ds.trim_start_matches('/')
        ))
    }
}

/// Resolve a Redis server address from a stored source config value, which
/// may be a full object (`{"addr": ...}`) or a bare address string.
/// Falls back to the local default when absent or unparseable.
fn resolve_redis_addr(
    source_configs: &Arc<RwLock<HashMap<String, Value>>>,
    conf_key: &str,
) -> String {
    const DEFAULT: &str = "127.0.0.1:6379";
    if conf_key.is_empty() {
        return DEFAULT.to_string();
    }
    let stored = source_configs
        .read()
        .get(&format!("redis/{}", conf_key))
        .cloned();
    match stored {
        Some(Value::Object(map)) => map
            .get("addr")
            .or_else(|| map.get("address"))
            .or_else(|| map.get("url"))
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty())
            .unwrap_or(DEFAULT)
            .to_string(),
        Some(Value::String(s)) if !s.is_empty() => s,
        _ => DEFAULT.to_string(),
    }
}

/// Resolve the `(url, channel)` pair for a rule whose source stream declares
/// `TYPE="redissub"` (or `"redis_sub"`): the channel comes from the stream
/// `DATASOURCE`, the server URL from `CONF_KEY` (or the local default).
fn resolve_redissub_source(
    stream_manager: &StreamManager,
    source_configs: &Arc<RwLock<HashMap<String, Value>>>,
    stream_name: &str,
    rule_id: &str,
) -> Option<(String, String)> {
    let def = stream_manager.get_stream(stream_name)?;
    let kind = def.options.get("TYPE")?;
    if !(kind.eq_ignore_ascii_case("redissub") || kind.eq_ignore_ascii_case("redis_sub")) {
        return None;
    }
    let channel = def.options.get("DATASOURCE").cloned().unwrap_or_default();
    if channel.is_empty() {
        tracing::warn!(
            "[RULE {}] redissub stream '{}' has no DATASOURCE channel",
            rule_id,
            stream_name
        );
        return None;
    }
    let conf_key = def.options.get("CONF_KEY").cloned().unwrap_or_default();
    let addr = resolve_redis_addr(source_configs, &conf_key);
    let url = if addr.contains("://") {
        addr
    } else {
        format!("redis://{}", addr)
    };
    Some((url, channel))
}

/// Resolve the [`KafkaConfig`] for a rule whose source stream declares
/// `TYPE="kafka"`: the topic comes from the stream `DATASOURCE` (falling back
/// to the config topic), brokers and friends from `kafka/{conf_key}` (or
/// defaults when no key matches).
fn resolve_kafka_source(
    stream_manager: &StreamManager,
    source_configs: &Arc<RwLock<HashMap<String, Value>>>,
    stream_name: &str,
    rule_id: &str,
) -> Option<KafkaConfig> {
    let def = stream_manager.get_stream(stream_name)?;
    let kind = def.options.get("TYPE")?;
    if !kind.eq_ignore_ascii_case("kafka") {
        return None;
    }
    let mut config = KafkaConfig {
        brokers: "127.0.0.1:9092".to_string(),
        topic: None,
        group_id: None,
        partition: 0,
        key: None,
    };
    if let Some(key) = def.options.get("CONF_KEY") {
        if !key.is_empty() {
            let lookup = format!("kafka/{}", key);
            if let Some(conf_val) = source_configs.read().get(&lookup).cloned() {
                match serde_json::from_value::<KafkaConfig>(conf_val) {
                    Ok(parsed) => config = parsed,
                    Err(e) => {
                        tracing::warn!(
                            "[RULE {}] invalid kafka config '{}': {}",
                            rule_id,
                            lookup,
                            e
                        );
                        return None;
                    }
                }
            }
        }
    }
    match def.options.get("DATASOURCE").cloned() {
        Some(topic) if !topic.is_empty() => config.topic = Some(topic),
        _ if config.topic.as_deref().is_some_and(|t| !t.is_empty()) => {}
        _ => {
            tracing::warn!(
                "[RULE {}] kafka stream '{}' has no DATASOURCE topic",
                rule_id,
                stream_name
            );
            return None;
        }
    }
    if config.broker_list().is_empty() {
        tracing::warn!(
            "[RULE {}] kafka stream '{}' has no brokers configured",
            rule_id,
            stream_name
        );
        return None;
    }
    Some(config)
}

/// Resolve the [`FileSourceConfig`] for a rule whose source stream declares
/// `TYPE="file"`: the path comes from `DATASOURCE` (falling back to a stored
/// `file/{conf_key}` config), with `FORMAT`/`fileType`, `hasHeader` and
/// `delimiter` layered from stream options over config defaults.
fn resolve_file_source(
    stream_manager: &StreamManager,
    source_configs: &Arc<RwLock<HashMap<String, Value>>>,
    stream_name: &str,
    rule_id: &str,
) -> Option<FileSourceConfig> {
    use rekuiper_connectors::DelimitedCodec;

    let def = stream_manager.get_stream(stream_name)?;
    let kind = def.options.get("TYPE")?;
    if !kind.eq_ignore_ascii_case("file") {
        return None;
    }
    let mut config = FileSourceConfig {
        path: String::new(),
        format: "json".to_string(),
        has_header: false,
        delimiter: None,
        interval: 0,
    };
    if let Some(key) = def.options.get("CONF_KEY") {
        if !key.is_empty() {
            let lookup = format!("file/{}", key);
            if let Some(conf_val) = source_configs.read().get(&lookup).cloned() {
                match serde_json::from_value::<FileSourceConfig>(conf_val) {
                    Ok(parsed) => config = parsed,
                    Err(e) => {
                        tracing::warn!(
                            "[RULE {}] invalid file config '{}': {}",
                            rule_id,
                            lookup,
                            e
                        );
                        return None;
                    }
                }
            }
        }
    }
    if let Some(path) = def.options.get("DATASOURCE") {
        if !path.trim().is_empty() {
            config.path = path.clone();
        }
    }
    if config.path.trim().is_empty() {
        tracing::warn!(
            "[RULE {}] file stream '{}' has neither DATASOURCE path nor file config",
            rule_id,
            stream_name
        );
        return None;
    }
    if let Some(format) = def
        .options
        .get("FORMAT")
        .filter(|s| !s.trim().is_empty())
        .or_else(|| def.options.get("FILETYPE").filter(|s| !s.trim().is_empty()))
    {
        config.format = format.clone();
    }
    if let Some(flag) = def
        .options
        .get("HASHEADER")
        .or_else(|| def.options.get("HAS_HEADER"))
    {
        match flag.trim().to_ascii_lowercase().as_str() {
            "true" | "1" | "yes" => config.has_header = true,
            "false" | "0" | "no" | "" => config.has_header = false,
            _ => {}
        }
    }
    if let Some(delim) = def.options.get("DELIMITER").filter(|s| !s.is_empty()) {
        config.delimiter = Some(DelimitedCodec::delimiter_from_name(delim));
    }
    Some(config)
}

/// Signal cancellation to ALL of a rule's background streaming sources
/// (MQTT, file, HTTP pull, WebSocket, Redis subscription, Kafka consumer,
/// SQL poller, join-target producers), if any are registered. Sources are
/// kept in a per-rule list so bootstrapping a second source (e.g. the join
/// target of a stream-stream join) never drops — and thereby kills — the
/// first: dropping a watch sender reads as `Err` (sender gone) in the
/// source task, which exits immediately.
fn cancel_rule_source(state: &AppState, rule_id: &str) {
    if let Some(txs) = state.source_cancels.write().remove(rule_id) {
        for tx in txs {
            let _ = tx.send(true);
        }
    }
}

/// Resolve the bus topic a rule actually subscribes to: memory-type streams
/// (`TYPE="memory"`) re-export another topic via `DATASOURCE`, mirroring the
/// eKuiper memory source (used to chain rules through memory sinks).
fn resolve_source_topic(stream_manager: &StreamManager, stream_name: &str) -> String {
    if let Some(def) = stream_manager.get_stream(stream_name) {
        if def
            .options
            .get("TYPE")
            .is_some_and(|t| t.eq_ignore_ascii_case("memory"))
        {
            if let Some(ds) = def.options.get("DATASOURCE") {
                if !ds.is_empty() {
                    return ds.clone();
                }
            }
        }
    }
    stream_name.to_string()
}

/// Subscribe to the rule's source stream and spawn its window-aware
/// execution task, registering the join handle on the rule manager.
#[allow(clippy::too_many_arguments)]
fn spawn_rule_task(
    rule_manager: &RuleManager,
    stream_bus: &StreamBus,
    stream_manager: &StreamManager,
    table_manager: &TableManager,
    source_configs: &Arc<RwLock<HashMap<String, Value>>>,
    http_client: &reqwest::Client,
    trace_manager: &TraceManager,
    rule_id: String,
    select_stmt: SelectStmt,
    actions: Vec<HashMap<String, Value>>,
    rule_options: Option<HashMap<String, Value>>,
) {
    if rule_options
        .as_ref()
        .and_then(|o| o.get("enableRuleTracer"))
        .and_then(|v| v.as_bool())
        .unwrap_or(false)
    {
        trace_manager.start_trace(&rule_id, "always".to_string());
    }
    let rx = stream_bus.subscribe(&resolve_source_topic(stream_manager, &select_stmt.from));
    let rule_mgr = rule_manager.clone();
    let window = select_stmt.window.clone();
    let tables = table_manager.clone();
    let confs = source_configs.clone();
    // Stream-stream joins fan in every joined stream: subscribe each join
    // target that is a stream (not a table) so windowed rules see both
    // sides. Table targets resolve per-row through lookups instead.
    let mut join_rxs: Vec<(String, broadcast::Receiver<StreamRecord>)> = Vec::new();
    for join in &select_stmt.joins {
        if table_manager.get_table(&join.target).is_none()
            && join.target != select_stmt.from
            && !join_rxs.iter().any(|(s, _)| s == &join.target)
        {
            let topic = resolve_source_topic(stream_manager, &join.target);
            join_rxs.push((join.target.clone(), stream_bus.subscribe(&topic)));
        }
    }

    // Bounded decoupled sink queue: the streaming evaluation loop never blocks
    // on sink network/disk I/O. Dropping `sink_tx` (rule end/cancel) lets the
    // worker flush remaining outputs and exit cleanly.
    let buffer_len = rule_options
        .as_ref()
        .and_then(|o| o.get("bufferLength"))
        .and_then(|v| v.as_u64())
        .and_then(|n| usize::try_from(n).ok())
        .map(|n| n.max(1))
        .unwrap_or(10_000);
    let send_error = rule_options
        .as_ref()
        .and_then(|o| o.get("sendError"))
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let (sink_tx, mut sink_rx) = tokio::sync::mpsc::channel::<StreamRecord>(buffer_len);
    let sink_actions = actions;
    let sink_rule_id = rule_id.clone();
    let sink_rule_mgr = rule_manager.clone();
    let sink_stream_bus = stream_bus.clone();
    let sink_http_client = http_client.clone();
    let sink_trace_mgr = trace_manager.clone();
    tokio::spawn(async move {
        while let Some(output_record) = sink_rx.recv().await {
            maybe_trace_record(
                &sink_trace_mgr,
                &sink_rule_id,
                &output_record.data,
                Some(&output_record.data),
            );
            dispatch_rule_actions(
                &sink_actions,
                &output_record,
                &sink_rule_id,
                &sink_rule_mgr,
                &sink_stream_bus,
                &sink_http_client,
            )
            .await;
        }
    });

    // Event-time mode for windowed rules: boundaries derive from payload
    // timestamps (stream TIMESTAMP field or well-known keys) instead of the
    // wall clock, with a late-tolerance grace window for out-of-order rows.
    let event_time = EventTimeConfig {
        enabled: rule_options
            .as_ref()
            .and_then(|o| o.get("isEventTime"))
            .and_then(|v| v.as_bool())
            .unwrap_or(false),
        late_tolerance_ms: rule_options
            .as_ref()
            .and_then(|o| o.get("lateTolerance"))
            .and_then(|v| v.as_i64())
            .unwrap_or(0),
        timestamp_field: stream_manager
            .get_stream(&resolve_source_topic(stream_manager, &select_stmt.from))
            .and_then(|s| {
                s.options
                    .iter()
                    .find(|(k, _)| k.eq_ignore_ascii_case("TIMESTAMP"))
                    .map(|(_, v)| v.clone())
            }),
    };

    let handle = match window {
        None => tokio::spawn(run_stateless_rule(
            rule_mgr,
            rule_id.clone(),
            select_stmt,
            rx,
            tables,
            confs,
            sink_tx,
            send_error,
        )),
        Some(WindowDef::Count { size, interval }) => tokio::spawn(run_count_window_rule(
            rule_mgr,
            rule_id.clone(),
            select_stmt,
            rx,
            size,
            interval,
            sink_tx,
            send_error,
            join_rxs,
            tables.clone(),
            confs.clone(),
        )),
        Some(WindowDef::TumblingTime { unit, length }) => {
            let duration = tumbling_window_duration(&unit, length);
            tokio::spawn(run_tumbling_window_rule(
                rule_mgr,
                rule_id.clone(),
                select_stmt,
                rx,
                duration,
                sink_tx,
                event_time,
                send_error,
                join_rxs,
                tables.clone(),
                confs.clone(),
            ))
        }
        Some(WindowDef::HoppingTime {
            unit,
            length,
            interval,
        }) => {
            let window_length = tumbling_window_duration(&unit, length);
            let hop_interval = tumbling_window_duration(&unit, interval);
            tokio::spawn(run_hopping_window_rule(
                rule_mgr,
                rule_id.clone(),
                select_stmt,
                rx,
                window_length,
                hop_interval,
                sink_tx,
                send_error,
                join_rxs,
                tables.clone(),
                confs.clone(),
            ))
        }
        Some(WindowDef::SlidingTime {
            unit,
            length,
            delay,
        }) => {
            let window_length = tumbling_window_duration(&unit, length);
            let delay_dur = delay.map(|d| tumbling_window_duration(&unit, d));
            tokio::spawn(run_sliding_window_rule(
                rule_mgr,
                rule_id.clone(),
                select_stmt,
                rx,
                window_length,
                delay_dur,
                sink_tx,
                event_time,
                send_error,
                join_rxs,
                tables.clone(),
                confs.clone(),
            ))
        }
    };
    rule_manager.set_rule_handle(&rule_id, handle);
}

fn is_rule_running(rule_mgr: &RuleManager, rule_id: &str) -> bool {
    match rule_mgr.get_rule_status(rule_id) {
        Some(status) => status.status == "running",
        // Rule deleted mid-flight: stop processing.
        None => false,
    }
}

/// Extracts an upstream error message when the record carries an `error` or
/// `__error` field (emitted by source decoders on malformed payloads).
fn check_record_error(data: &HashMap<String, Value>) -> Option<String> {
    if let Some(err) = data.get("error").or_else(|| data.get("__error")) {
        return Some(
            err.as_str()
                .map(|s| s.to_string())
                .unwrap_or_else(|| err.to_string()),
        );
    }
    None
}

/// Handles an upstream error record per the rule `sendError` option. Returns
/// `true` when the record was an error record (counted as an exception and,
/// when enabled, forwarded immediately to the sink); the caller must then
/// `continue` without normal projection or window-buffer insertion, mirroring
/// eKuiper semantics where the error event bypasses window aggregation.
async fn handle_error_record(
    rule_mgr: &RuleManager,
    rule_id: &str,
    send_error: bool,
    sink: &tokio::sync::mpsc::Sender<StreamRecord>,
    record: &StreamRecord,
) -> bool {
    let Some(err_msg) = check_record_error(&record.data) else {
        return false;
    };
    rule_mgr.inc_exceptions(rule_id, 1);
    if send_error {
        let mut err_data = HashMap::new();
        err_data.insert("error".to_string(), Value::String(err_msg));
        err_data.insert("rule_id".to_string(), Value::String(rule_id.to_string()));
        enqueue_sink_record(sink, StreamRecord::new(err_data)).await;
        rule_mgr.inc_sink_records(rule_id, 1);
    }
    true
}

/// Event-time configuration for windowed rules: when `enabled`, window
/// boundaries derive from payload event timestamps instead of arrival time,
/// with `late_tolerance_ms` grace for out-of-order events.
#[derive(Clone, Default)]
struct EventTimeConfig {
    enabled: bool,
    late_tolerance_ms: i64,
    timestamp_field: Option<String>,
}

fn parse_timestamp_val(v: &Value) -> Option<i64> {
    if let Some(n) = v.as_i64() {
        return Some(n);
    }
    if let Some(n) = v.as_u64() {
        return Some(n as i64);
    }
    if let Some(f) = v.as_f64() {
        return Some(f as i64);
    }
    if let Some(s) = v.as_str() {
        if let Ok(n) = s.parse::<i64>() {
            return Some(n);
        }
        if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(s) {
            return Some(dt.timestamp_millis());
        }
    }
    None
}

fn extract_event_timestamp(data: &HashMap<String, Value>, configured_field: Option<&str>) -> i64 {
    if let Some(field) = configured_field {
        if let Some(v) = data.get(field).and_then(parse_timestamp_val) {
            return v;
        }
    }
    for key in ["timestamp", "ts", "event_time", "time"] {
        if let Some(v) = data.get(key).and_then(parse_timestamp_val) {
            return v;
        }
    }
    chrono::Utc::now().timestamp_millis()
}

/// Optional sink `dataTemplate` from action options, rendered against the
/// output record before transmission.
fn action_template(opts: &Value) -> Option<&str> {
    opts.get("dataTemplate").and_then(|v| v.as_str())
}

fn record_template_map(data: &HashMap<String, Value>) -> serde_json::Map<String, Value> {
    data.iter().map(|(k, v)| (k.clone(), v.clone())).collect()
}

async fn dispatch_rule_actions(
    actions: &[HashMap<String, Value>],
    output: &StreamRecord,
    rule_id: &str,
    rule_mgr: &RuleManager,
    stream_bus: &StreamBus,
    http_client: &reqwest::Client,
) {
    for action in actions {
        for (kind, opts) in action {
            match kind.as_str() {
                "log" => {
                    tracing::info!("[RULE {}] Matched record: {:?}", rule_id, output.data);
                }
                "nop" => {}
                "file" => match serde_json::from_value::<FileSink>(opts.clone()) {
                    Ok(sink) => {
                        let res = match action_template(opts) {
                            Some(tpl) => {
                                sink.send_text(&apply_data_template(
                                    tpl,
                                    &record_template_map(&output.data),
                                ))
                                .await
                            }
                            None => sink.send(output).await,
                        };
                        if let Err(e) = res {
                            tracing::warn!("[RULE {}] file action failed: {}", rule_id, e);
                            rule_mgr.inc_exceptions(rule_id, 1);
                        }
                    }
                    Err(e) => {
                        tracing::warn!("[RULE {}] invalid file action options: {}", rule_id, e);
                        rule_mgr.inc_exceptions(rule_id, 1);
                    }
                },
                "rest" | "http" => {
                    let url = opts.get("url").and_then(|v| v.as_str()).unwrap_or("");
                    if url.is_empty() {
                        tracing::warn!("[RULE {}] rest action missing url", rule_id);
                        rule_mgr.inc_exceptions(rule_id, 1);
                    } else {
                        let res = match action_template(opts) {
                            Some(tpl) => {
                                http_client
                                    .post(url)
                                    .header(reqwest::header::CONTENT_TYPE, "application/json")
                                    .body(apply_data_template(
                                        tpl,
                                        &record_template_map(&output.data),
                                    ))
                                    .send()
                                    .await
                            }
                            None => http_client.post(url).json(&output.data).send().await,
                        };
                        if let Err(e) = res {
                            tracing::warn!("[RULE {}] rest action failed: {}", rule_id, e);
                            rule_mgr.inc_exceptions(rule_id, 1);
                        }
                    }
                }
                "mqtt" => match serde_json::from_value::<MqttConfig>(opts.clone()) {
                    Ok(cfg) => match MqttSink::new(cfg) {
                        Ok(sink) => {
                            let res = match action_template(opts) {
                                Some(tpl) => {
                                    sink.send_raw(
                                        apply_data_template(
                                            tpl,
                                            &record_template_map(&output.data),
                                        )
                                        .into_bytes(),
                                    )
                                    .await
                                }
                                None => sink.send(output).await,
                            };
                            if let Err(e) = res {
                                tracing::warn!("[RULE {}] mqtt action failed: {}", rule_id, e);
                                rule_mgr.inc_exceptions(rule_id, 1);
                            }
                        }
                        Err(e) => {
                            tracing::warn!("[RULE {}] mqtt action connect failed: {}", rule_id, e);
                            rule_mgr.inc_exceptions(rule_id, 1);
                        }
                    },
                    Err(e) => {
                        tracing::warn!("[RULE {}] invalid mqtt action options: {}", rule_id, e);
                        rule_mgr.inc_exceptions(rule_id, 1);
                    }
                },
                "websocket" => match serde_json::from_value::<WebSocketConfig>(opts.clone()) {
                    Ok(ws_cfg) => {
                        let sink = WebSocketSink {
                            url: ws_cfg.target_url(),
                        };
                        let res = match action_template(opts) {
                            Some(tpl) => {
                                sink.send_text(&apply_data_template(
                                    tpl,
                                    &record_template_map(&output.data),
                                ))
                                .await
                            }
                            None => sink.send(output).await,
                        };
                        if let Err(e) = res {
                            tracing::warn!("[RULE {}] websocket action failed: {}", rule_id, e);
                            rule_mgr.inc_exceptions(rule_id, 1);
                        }
                    }
                    Err(e) => {
                        tracing::warn!(
                            "[RULE {}] invalid websocket action options: {}",
                            rule_id,
                            e
                        );
                        rule_mgr.inc_exceptions(rule_id, 1);
                    }
                },
                "redis" | "redispub" | "redisPub" => {
                    match serde_json::from_value::<RedisSinkConfig>(opts.clone()) {
                        Ok(cfg) => {
                            let sink = RedisSink { config: cfg };
                            if let Err(e) = sink.send(output).await {
                                tracing::warn!("[RULE {}] redis action failed: {}", rule_id, e);
                                rule_mgr.inc_exceptions(rule_id, 1);
                            }
                        }
                        Err(e) => {
                            tracing::warn!(
                                "[RULE {}] invalid redis action options: {}",
                                rule_id,
                                e
                            );
                            rule_mgr.inc_exceptions(rule_id, 1);
                        }
                    }
                }
                "kafka" => match serde_json::from_value::<KafkaConfig>(opts.clone()) {
                    Ok(cfg) => {
                        let sink = KafkaSink { config: cfg };
                        if let Err(e) = sink.send(output).await {
                            tracing::warn!("[RULE {}] kafka action failed: {}", rule_id, e);
                            rule_mgr.inc_exceptions(rule_id, 1);
                        }
                    }
                    Err(e) => {
                        tracing::warn!("[RULE {}] invalid kafka action options: {}", rule_id, e);
                        rule_mgr.inc_exceptions(rule_id, 1);
                    }
                },
                "sql" => match serde_json::from_value::<SqlConnectorConfig>(opts.clone()) {
                    Ok(cfg) => {
                        let sink = SqlSink { config: cfg };
                        if let Err(e) = sink.insert_record(output).await {
                            tracing::warn!("[RULE {}] sql action failed: {}", rule_id, e);
                            rule_mgr.inc_exceptions(rule_id, 1);
                        }
                    }
                    Err(e) => {
                        tracing::warn!("[RULE {}] invalid sql action options: {}", rule_id, e);
                        rule_mgr.inc_exceptions(rule_id, 1);
                    }
                },
                "memory" => {
                    let topic = opts
                        .get("topic")
                        .and_then(|v| v.as_str())
                        .unwrap_or("default");
                    // Broadcast send fails only when nobody listens; the data
                    // has still been produced, so never count it as an exception.
                    let _ = stream_bus.publish(topic, output.clone());
                }
                other => {
                    tracing::debug!("[RULE {}] unknown action '{}', ignoring", rule_id, other);
                }
            }
        }
    }
}

/// Resolve lookup JOINs for one stream record against table rows.
///
/// Starts from the base record (exposed both under bare field names and
/// `{from}.{field}` qualifiers) and folds each join clause in: the first
/// table row whose `ON` condition holds over the combined map wins and is
/// merged in (bare keys keep stream values via `or_insert`, plus
/// `{target}.{field}` qualifiers). Returns `None` when an `Inner` (or
/// `Right`/`Full`/`Cross` without match) join finds no row and the record
/// must be skipped; `Left` joins fall through un-joined.
/// Derive the point-lookup key for an external table join from an equality
/// `ON` condition between the stream side and the table side (either order).
/// Returns `(table_column, key_value)`; `None` when no usable key exists.
fn join_key_parts(
    join: &JoinClause,
    from: &str,
    from_alias: Option<&str>,
    combined: &HashMap<String, Value>,
) -> Option<(String, String)> {
    fn side(expr: &Expr, from: &str, from_alias: Option<&str>, target: &JoinClause) -> u8 {
        match expr {
            // 0 = stream side, 1 = table side, 2 = unknown.
            Expr::FieldAccess { parent, .. } => match parent.as_ref() {
                Expr::Identifier(name)
                    if name == &target.target
                        || target.alias.as_ref().is_some_and(|a| name == a) =>
                {
                    1
                }
                Expr::Identifier(name) if name == from || from_alias.is_some_and(|a| name == a) => {
                    0
                }
                _ => 2,
            },
            Expr::Identifier(_) => 0,
            _ => 2,
        }
    }
    fn column_name(expr: &Expr) -> Option<String> {
        match expr {
            Expr::FieldAccess { field, .. } => Some(field.clone()),
            Expr::Identifier(name) => Some(name.clone()),
            _ => None,
        }
    }
    fn scalarize(value: Value) -> Option<String> {
        match value {
            Value::Null => None,
            Value::String(s) => Some(s),
            Value::Number(n) => Some(n.to_string()),
            Value::Bool(b) => Some(b.to_string()),
            _ => None,
        }
    }
    let (left, right) = match join.on.as_ref()? {
        Expr::BinaryOp {
            left,
            op: rekuiper_sql::BinaryOperator::Eq,
            right,
        } => (left, right),
        _ => return None,
    };
    let (table_expr, key_expr) = match (
        side(left, from, from_alias, join),
        side(right, from, from_alias, join),
    ) {
        (1, _) => (left, right),
        (_, 1) => (right, left),
        _ => return None,
    };
    Some((
        column_name(table_expr)?,
        scalarize(Evaluator::eval_val(key_expr, combined))?,
    ))
}

/// Derive the point-lookup key value for a Redis table join.
fn extract_lookup_key(
    join: &JoinClause,
    from: &str,
    from_alias: Option<&str>,
    combined: &HashMap<String, Value>,
) -> Option<String> {
    join_key_parts(join, from, from_alias, combined).map(|(_, value)| value)
}

/// Build a single candidate row from a fetched lookup value: objects map to
/// rows directly, scalars bind under `"value"`.
fn lookup_value_to_row(value: Value) -> HashMap<String, Value> {
    match value {
        Value::Object(map) => map.into_iter().collect(),
        scalar => {
            let mut row = HashMap::new();
            row.insert("value".to_string(), scalar);
            row
        }
    }
}

/// Table anchor fallback for SQL configs: explicit `TABLE`, else
/// `DATASOURCE` (the documented stream/table anchor), else the object name.
fn sql_table_fallback(options: &HashMap<String, String>, name: &str) -> String {
    options
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case("TABLE"))
        .map(|(_, v)| v.clone())
        .filter(|v| !v.trim().is_empty())
        .or_else(|| {
            options
                .iter()
                .find(|(k, _)| k.eq_ignore_ascii_case("DATASOURCE"))
                .map(|(_, v)| v.clone())
                .filter(|v| !v.trim().is_empty())
        })
        .unwrap_or_else(|| name.to_string())
}

/// Resolve the polling config for a `TYPE="sql"` source stream: a matching
/// `sql/{conf_key}` source config wins, otherwise the stream options (`URL`,
/// falling back to `DATASOURCE`, plus `TABLE` or the stream name and an
/// optional `INTERVAL` poll period). Option names match case-insensitively.
fn resolve_sql_source(
    stream_manager: &StreamManager,
    source_configs: &Arc<RwLock<HashMap<String, Value>>>,
    stream_name: &str,
    rule_id: &str,
) -> Option<SqlConnectorConfig> {
    let def = stream_manager.get_stream(stream_name)?;
    let kind = def
        .options
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case("TYPE"))
        .map(|(_, v)| v.clone())
        .unwrap_or_default();
    if !kind.eq_ignore_ascii_case("sql") {
        return None;
    }
    let conf_key = def
        .options
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case("CONF_KEY") || k.eq_ignore_ascii_case("confKey"))
        .map(|(_, v)| v.trim().to_string());
    if let Some(key) = conf_key {
        if !key.is_empty() {
            let lookup = format!("sql/{}", key);
            let configs_guard = source_configs.read();
            let conf_val = configs_guard
                .get(&lookup)
                .or_else(|| configs_guard.get(&key))
                .cloned();
            drop(configs_guard);
            if let Some(val) = conf_val {
                match serde_json::from_value::<SqlConnectorConfig>(val) {
                    Ok(mut conf) => {
                        // Documented plugin configs carry the URL as `dburl`
                        // and may omit the table: fall back to the stream
                        // options (DATASOURCE/TABLE) for whatever is missing.
                        if conf.url.trim().is_empty() {
                            conf.url = def
                                .options
                                .iter()
                                .find(|(k, _)| {
                                    k.eq_ignore_ascii_case("URL")
                                        || k.eq_ignore_ascii_case("DATASOURCE")
                                })
                                .map(|(_, v)| v.clone())
                                .unwrap_or_default();
                        }
                        if conf.table.trim().is_empty() {
                            conf.table = sql_table_fallback(&def.options, stream_name);
                        }
                        if conf.url.trim().is_empty() || conf.table.trim().is_empty() {
                            // A template-SQL config without any table anchor
                            // cannot poll; surface the misconfiguration.
                            tracing::warn!(
                                "[RULE {}] sql source config '{}' has no usable url/table",
                                rule_id,
                                lookup
                            );
                            return None;
                        }
                        return Some(conf);
                    }
                    Err(e) => {
                        tracing::warn!(
                            "[RULE {}] invalid sql source config '{}': {}",
                            rule_id,
                            lookup,
                            e
                        );
                        return None;
                    }
                }
            }
        }
    }
    let url = def
        .options
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case("URL") || k.eq_ignore_ascii_case("DATASOURCE"))
        .map(|(_, v)| v.clone())
        .unwrap_or_default();
    if url.trim().is_empty() {
        return None;
    }
    let table = def
        .options
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case("TABLE"))
        .map(|(_, v)| v.clone())
        .unwrap_or_else(|| stream_name.to_string());
    let interval = def
        .options
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case("INTERVAL"))
        .and_then(|(_, v)| v.trim().parse::<u64>().ok())
        .unwrap_or(1000)
        .max(1);
    Some(SqlConnectorConfig {
        url,
        table,
        fields: Vec::new(),
        interval,
        template_sql_query_cfg: None,
        internal_sql_query_cfg: None,
    })
}

/// Resolve the `(url, table)` pair for a `TYPE="sql"` lookup table: a
/// matching `sql/{conf_key}` source config wins, otherwise the table options
/// (`URL`, falling back to `DATASOURCE`, plus `TABLE` or the target name).
fn resolve_sql_lookup(
    table_manager: &TableManager,
    source_configs: &Arc<RwLock<HashMap<String, Value>>>,
    target: &str,
) -> Option<(String, String)> {
    let def = table_manager.get_table(target)?;
    if let Some(key) = def.options.get("CONF_KEY") {
        if !key.is_empty() {
            let lookup = format!("sql/{}", key);
            if let Some(conf_val) = source_configs.read().get(&lookup).cloned() {
                match serde_json::from_value::<SqlConnectorConfig>(conf_val) {
                    Ok(mut conf) => {
                        if conf.table.trim().is_empty() {
                            conf.table = sql_table_fallback(&def.options, target);
                        }
                        if conf.url.trim().is_empty() {
                            tracing::warn!("sql lookup config '{}' has no url", lookup);
                            return None;
                        }
                        return Some((conf.url, conf.table));
                    }
                    Err(e) => {
                        tracing::warn!("Invalid sql lookup config '{}': {}", lookup, e);
                        return None;
                    }
                }
            }
        }
    }
    let url = def
        .options
        .get("URL")
        .or_else(|| def.options.get("DATASOURCE"))
        .cloned()
        .unwrap_or_default();
    if url.is_empty() {
        return None;
    }
    let table = def
        .options
        .get("TABLE")
        .cloned()
        .unwrap_or_else(|| target.to_string());
    Some((url, table))
}

/// Fetch candidate rows for one join clause: a Redis `GET` point lookup for
/// `TYPE="redis"` tables, a SQL point lookup for `TYPE="sql"` tables,
/// otherwise the locally stored table rows.
async fn lookup_candidates(
    table_manager: &TableManager,
    source_configs: &Arc<RwLock<HashMap<String, Value>>>,
    from: &str,
    from_alias: Option<&str>,
    join: &JoinClause,
    combined: &HashMap<String, Value>,
) -> Vec<HashMap<String, Value>> {
    let table_type = table_manager
        .get_table(&join.target)
        .and_then(|def| def.options.get("TYPE").cloned())
        .unwrap_or_default();
    if table_type.eq_ignore_ascii_case("redis") {
        let Some(key) = extract_lookup_key(join, from, from_alias, combined) else {
            return Vec::new();
        };
        let conf_key = table_manager
            .get_table(&join.target)
            .and_then(|def| def.options.get("CONF_KEY").cloned())
            .unwrap_or_default();
        let addr = resolve_redis_addr(source_configs, &conf_key);
        return match rekuiper_connectors::redis_lookup_key(&addr, &key).await {
            Ok(Some(value)) => vec![lookup_value_to_row(value)],
            Ok(None) => Vec::new(),
            Err(e) => {
                tracing::warn!("Redis lookup GET {} failed: {}", key, e);
                Vec::new()
            }
        };
    }
    if table_type.eq_ignore_ascii_case("sql") {
        let (url, table, col, val) = match (
            resolve_sql_lookup(table_manager, source_configs, &join.target),
            join_key_parts(join, from, from_alias, combined),
        ) {
            (Some((url, table)), Some((col, val))) => (url, table, col, val),
            _ => return Vec::new(),
        };
        return match rekuiper_connectors::sql_lookup_key(&url, &table, &col, &val).await {
            Ok(Some(value)) => vec![lookup_value_to_row(value)],
            Ok(None) => Vec::new(),
            Err(e) => {
                tracing::warn!("SQL lookup on {} failed: {}", table, e);
                Vec::new()
            }
        };
    }
    table_manager.get_table_rows(&join.target)
}

async fn apply_lookup_joins(
    table_manager: &TableManager,
    source_configs: &Arc<RwLock<HashMap<String, Value>>>,
    select_stmt: &SelectStmt,
    record: &HashMap<String, Value>,
) -> Option<HashMap<String, Value>> {
    if select_stmt.joins.is_empty() {
        return Some(record.clone());
    }
    // Qualified access (`stream.field`, `table.field`) resolves through nested
    // objects, matching the evaluator's FieldAccess semantics.
    let mut combined = record.clone();
    insert_namespaced(
        &mut combined,
        &select_stmt.from,
        select_stmt.from_alias.as_deref(),
        record,
    );
    for join in &select_stmt.joins {
        let mut matched: Option<HashMap<String, Value>> = None;
        for row in lookup_candidates(
            table_manager,
            source_configs,
            &select_stmt.from,
            select_stmt.from_alias.as_deref(),
            join,
            &combined,
        )
        .await
        {
            let mut probe = combined.clone();
            for (k, v) in &row {
                probe.entry(k.clone()).or_insert(v.clone());
            }
            insert_namespaced(&mut probe, &join.target, join.alias.as_deref(), &row);
            let cond_ok = match &join.on {
                Some(cond) => Evaluator::eval_bool(cond, &probe),
                // No ON condition: match the first candidate row.
                None => true,
            };
            if cond_ok {
                matched = Some(row);
                break;
            }
        }
        match matched {
            Some(row) => {
                for (k, v) in &row {
                    combined.entry(k.clone()).or_insert(v.clone());
                }
                insert_namespaced(&mut combined, &join.target, join.alias.as_deref(), &row);
            }
            None if join.join_type == JoinType::Left => {}
            None => return None,
        }
    }
    Some(combined)
}

/// Enqueue an output record for the background sink worker without blocking
/// the evaluation loop: fast path is a lock-free `try_send`, falling back to
/// an awaiting send only while the bounded queue is under backpressure.
async fn enqueue_sink_record(
    sink: &tokio::sync::mpsc::Sender<StreamRecord>,
    output_record: StreamRecord,
) {
    if let Err(tokio::sync::mpsc::error::TrySendError::Full(rec)) = sink.try_send(output_record) {
        // Queue under backpressure: await send
        let _ = sink.send(rec).await;
    }
}

/// Insert a row under its stream/table name plus alias so qualified
/// references (`A.id`, `a.id`) resolve through nested objects.
fn insert_namespaced(
    map: &mut HashMap<String, Value>,
    name: &str,
    alias: Option<&str>,
    row: &HashMap<String, Value>,
) {
    let obj = Value::Object(row.iter().map(|(k, v)| (k.clone(), v.clone())).collect());
    map.insert(name.to_string(), obj.clone());
    if let Some(a) = alias {
        if a != name {
            map.insert(a.to_string(), obj);
        }
    }
}

/// A window-buffered row tagged with the stream that produced it, so
/// multi-stream windows can match rows across sources.
#[derive(Debug, Clone)]
struct TaggedRow {
    source: String,
    data: HashMap<String, Value>,
}

/// Upper bound on join fan-out per window trigger: windows are bounded
/// buffers, and an unbounded cross product could exhaust memory.
const MAX_JOIN_FANOUT: usize = 10_000;

fn has_agg_expr(expr: &Expr) -> bool {
    match expr {
        Expr::Call { name, args } => {
            Evaluator::is_aggregate_call(name) || args.iter().any(has_agg_expr)
        }
        Expr::BinaryOp { left, right, .. } => has_agg_expr(left) || has_agg_expr(right),
        Expr::UnaryOp { expr, .. } => has_agg_expr(expr),
        Expr::Between {
            expr, low, high, ..
        } => has_agg_expr(expr) || has_agg_expr(low) || has_agg_expr(high),
        Expr::InList { expr, list, .. } => has_agg_expr(expr) || list.iter().any(has_agg_expr),
        Expr::IsNull { expr, .. } => has_agg_expr(expr),
        Expr::FieldAccess { parent, .. } => has_agg_expr(parent),
        Expr::Index { base, index } => has_agg_expr(base) || has_agg_expr(index),
        Expr::Slice { base, lo, hi } => {
            has_agg_expr(base)
                || lo.as_ref().is_some_and(|e| has_agg_expr(e))
                || hi.as_ref().is_some_and(|e| has_agg_expr(e))
        }
        Expr::Case {
            operand,
            when_clauses,
            else_clause,
        } => {
            operand.as_ref().is_some_and(|e| has_agg_expr(e))
                || when_clauses
                    .iter()
                    .any(|(w, t)| has_agg_expr(w) || has_agg_expr(t))
                || else_clause.as_ref().is_some_and(|e| has_agg_expr(e))
        }
        Expr::Over { call, partition_by } => {
            has_agg_expr(call) || partition_by.as_ref().is_some_and(|e| has_agg_expr(e))
        }
        Expr::Wildcard | Expr::Identifier(_) | Expr::Literal(_) => false,
    }
}

/// Evaluate one windowed batch for a rule with JOIN clauses.
///
/// Seeds combined rows from the FROM stream, folds each join (table point
/// lookups take the first row satisfying ON — lookup tables resolve one row
/// per key; stream targets nest-loop over buffered rows with the `ON`
/// condition; CROSS pairs all candidates), then projects: aggregate SELECTs
/// collapse the batch with `eval_aggregate`, plain SELECTs emit one row per
/// match with `eval_select` (which also applies WHERE). LEFT preserves
/// unmatched left rows; RIGHT/FULL additionally preserve unmatched right
/// rows (which never exceeds the fan-out cap), including when the left side
/// is empty. Fan-out per trigger is bounded by `MAX_JOIN_FANOUT`. Returns
/// the output rows (possibly empty); an empty batch yields no output,
/// matching empty-window semantics.
async fn eval_window_join_batch(
    table_manager: &TableManager,
    source_configs: &Arc<RwLock<HashMap<String, Value>>>,
    select_stmt: &SelectStmt,
    batch: &[TaggedRow],
) -> Vec<HashMap<String, Value>> {
    if batch.is_empty() {
        return Vec::new();
    }
    let from_rows: Vec<&HashMap<String, Value>> = batch
        .iter()
        .filter(|r| r.source == select_stmt.from)
        .map(|r| &r.data)
        .collect();
    let mut combined_rows: Vec<HashMap<String, Value>> = Vec::new();
    for row in from_rows {
        let mut combined = row.clone();
        insert_namespaced(
            &mut combined,
            &select_stmt.from,
            select_stmt.from_alias.as_deref(),
            row,
        );
        combined_rows.push(combined);
    }
    for join in &select_stmt.joins {
        let is_table = table_manager.get_table(&join.target).is_some();
        let mut next: Vec<HashMap<String, Value>> = Vec::new();
        if is_table {
            for left in &combined_rows {
                let mut matched: Option<HashMap<String, Value>> = None;
                for row in lookup_candidates(
                    table_manager,
                    source_configs,
                    &select_stmt.from,
                    select_stmt.from_alias.as_deref(),
                    join,
                    left,
                )
                .await
                {
                    // CROSS joins pair every candidate; others take the
                    // first row satisfying ON (or the first row when no ON).
                    if join.join_type != JoinType::Cross {
                        let mut probe = (*left).clone();
                        for (k, v) in &row {
                            probe.entry(k.clone()).or_insert(v.clone());
                        }
                        insert_namespaced(&mut probe, &join.target, join.alias.as_deref(), &row);
                        let cond_ok = match &join.on {
                            Some(cond) => Evaluator::eval_bool(cond, &probe),
                            None => true,
                        };
                        if !cond_ok {
                            continue;
                        }
                        matched = Some(row);
                        break;
                    }
                    let mut merged = (*left).clone();
                    for (k, v) in &row {
                        merged.entry(k.clone()).or_insert(v.clone());
                    }
                    insert_namespaced(&mut merged, &join.target, join.alias.as_deref(), &row);
                    if next.len() < MAX_JOIN_FANOUT {
                        next.push(merged);
                    }
                }
                if join.join_type == JoinType::Cross {
                    continue;
                }
                match matched {
                    Some(row) => {
                        let mut merged = (*left).clone();
                        for (k, v) in &row {
                            merged.entry(k.clone()).or_insert(v.clone());
                        }
                        insert_namespaced(&mut merged, &join.target, join.alias.as_deref(), &row);
                        next.push(merged);
                    }
                    None => match join.join_type {
                        JoinType::Left | JoinType::Full => next.push((*left).clone()),
                        _ => {}
                    },
                }
            }
        } else {
            let right_rows: Vec<&HashMap<String, Value>> = batch
                .iter()
                .filter(|r| r.source == join.target)
                .map(|r| &r.data)
                .collect();
            // Index of matched right rows (for RIGHT/FULL preservation).
            let mut right_matched = vec![false; right_rows.len()];
            for left in &combined_rows {
                let mut any = false;
                for (ri, right) in right_rows.iter().enumerate() {
                    let mut probe = (*left).clone();
                    for (k, v) in right.iter() {
                        probe.entry(k.clone()).or_insert(v.clone());
                    }
                    insert_namespaced(&mut probe, &join.target, join.alias.as_deref(), right);
                    let cond_ok = match &join.on {
                        Some(cond) => Evaluator::eval_bool(cond, &probe),
                        // No ON: cross product of the window.
                        None => true,
                    };
                    if !cond_ok {
                        continue;
                    }
                    any = true;
                    right_matched[ri] = true;
                    if next.len() < MAX_JOIN_FANOUT {
                        next.push(probe);
                    }
                    if next.len() >= MAX_JOIN_FANOUT {
                        break;
                    }
                }
                if !any {
                    match join.join_type {
                        JoinType::Left | JoinType::Full => next.push((*left).clone()),
                        _ => {}
                    }
                }
                if next.len() >= MAX_JOIN_FANOUT {
                    break;
                }
            }
            if matches!(join.join_type, JoinType::Right | JoinType::Full) {
                for (ri, right) in right_rows.iter().enumerate() {
                    if !right_matched[ri] {
                        if next.len() >= MAX_JOIN_FANOUT {
                            break;
                        }
                        let mut preserved = (*right).clone();
                        insert_namespaced(
                            &mut preserved,
                            &join.target,
                            join.alias.as_deref(),
                            right,
                        );
                        next.push(preserved);
                    }
                }
            }
        }
        combined_rows = next;
        // An empty intermediate only ends the pipeline for joins that cannot
        // produce rows without left input. RIGHT/FULL joins still preserve
        // their right side (and later joins fold over it), per SQL semantics.
        if combined_rows.is_empty() && !matches!(join.join_type, JoinType::Right | JoinType::Full) {
            return Vec::new();
        }
    }
    let aggregate = select_stmt.fields.iter().any(has_agg_expr)
        || select_stmt.having.as_ref().is_some_and(has_agg_expr);
    if aggregate {
        return Evaluator::eval_aggregate(select_stmt, &combined_rows)
            .into_iter()
            .collect();
    }
    combined_rows
        .iter()
        .filter_map(|row| Evaluator::eval_select(select_stmt, row))
        .collect()
}

#[allow(clippy::too_many_arguments)]
async fn run_stateless_rule(
    rule_mgr: RuleManager,
    rule_id: String,
    select_stmt: SelectStmt,
    mut rx: broadcast::Receiver<StreamRecord>,
    table_manager: TableManager,
    source_configs: Arc<RwLock<HashMap<String, Value>>>,
    sink: tokio::sync::mpsc::Sender<StreamRecord>,
    send_error: bool,
) {
    // Running analytic state for acc_* cumulative functions. The stateful
    // projection below runs for every input row (advancing cumulative state
    // even for rows the WHERE filter later drops, mirroring eKuiper analytic
    // semantics); the input-row filter then decides emission.
    let rule_state = RuleState::default();
    loop {
        match rx.recv().await {
            Ok(record) => {
                if !is_rule_running(&rule_mgr, &rule_id) {
                    continue;
                }
                rule_mgr.inc_source_records(&rule_id, 1);
                if handle_error_record(&rule_mgr, &rule_id, send_error, &sink, &record).await {
                    continue;
                }
                let Some(joined) =
                    apply_lookup_joins(&table_manager, &source_configs, &select_stmt, &record.data)
                        .await
                else {
                    // Inner join without a matching table row: drop the record.
                    continue;
                };
                let output_opt =
                    Evaluator::eval_select_stateful(&select_stmt, &joined, &rule_state);
                let passes = match &select_stmt.where_clause {
                    Some(cond) => Evaluator::eval_bool(cond, &joined),
                    None => true,
                };
                if passes {
                    if let Some(output) = output_opt {
                        let output_record = StreamRecord::new(output);
                        enqueue_sink_record(&sink, output_record).await;
                        rule_mgr.inc_sink_records(&rule_id, 1);
                    }
                }
            }
            Err(broadcast::error::RecvError::Lagged(_)) => continue,
            Err(broadcast::error::RecvError::Closed) => break,
        }
    }
}

/// Emit one window trigger: plain windows aggregate the batch; rules with
/// JOIN clauses resolve matches first (stream fan-in, table lookups, ON
/// conditions) and project each match.
async fn emit_window_batch(
    rule_mgr: &RuleManager,
    rule_id: &str,
    table_manager: &TableManager,
    source_configs: &Arc<RwLock<HashMap<String, Value>>>,
    select_stmt: &SelectStmt,
    batch: &[TaggedRow],
    sink: &tokio::sync::mpsc::Sender<StreamRecord>,
) {
    if batch.is_empty() {
        return;
    }
    if select_stmt.joins.is_empty() {
        let flat: Vec<HashMap<String, Value>> = batch.iter().map(|r| r.data.clone()).collect();
        if let Some(output) = Evaluator::eval_aggregate(select_stmt, &flat) {
            enqueue_sink_record(sink, StreamRecord::new(output)).await;
            rule_mgr.inc_sink_records(rule_id, 1);
        }
        return;
    }
    for output in eval_window_join_batch(table_manager, source_configs, select_stmt, batch).await {
        enqueue_sink_record(sink, StreamRecord::new(output)).await;
        rule_mgr.inc_sink_records(rule_id, 1);
    }
}

/// Spawn forwarders that tag rows from joined streams and feed the runner's
/// local channel. Forwarders exit when the runner drops the channel or the
/// source bus closes.
fn spawn_join_forwarders(
    join_rxs: Vec<(String, broadcast::Receiver<StreamRecord>)>,
) -> tokio::sync::mpsc::Receiver<TaggedRow> {
    let (join_tx, join_rx) = tokio::sync::mpsc::channel::<TaggedRow>(1024);
    for (source, mut jrx) in join_rxs {
        let jtx = join_tx.clone();
        tokio::spawn(async move {
            loop {
                match jrx.recv().await {
                    Ok(record) => {
                        let tagged = TaggedRow {
                            source: source.clone(),
                            data: record.data,
                        };
                        if jtx.send(tagged).await.is_err() {
                            break;
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
        });
    }
    drop(join_tx);
    join_rx
}

#[allow(clippy::too_many_arguments)]
async fn run_count_window_rule(
    rule_mgr: RuleManager,
    rule_id: String,
    select_stmt: SelectStmt,
    mut rx: broadcast::Receiver<StreamRecord>,
    size: usize,
    interval: Option<usize>,
    sink: tokio::sync::mpsc::Sender<StreamRecord>,
    send_error: bool,
    join_rxs: Vec<(String, broadcast::Receiver<StreamRecord>)>,
    table_manager: TableManager,
    source_configs: Arc<RwLock<HashMap<String, Value>>>,
) {
    let count = size.max(1);
    let hop = interval.unwrap_or(count).max(1);
    let mut buffer: Vec<TaggedRow> = Vec::new();
    let mut events_since_trigger: usize = 0;
    let from_source = select_stmt.from.clone();
    let mut join_rx = spawn_join_forwarders(join_rxs);
    // When every join forwarder has exited, the merge channel closes; the
    // rule keeps serving its FROM stream afterwards.
    let mut joins_open = true;
    loop {
        tokio::select! {
            res = rx.recv() => {
                match res {
            Ok(record) => {
                if !is_rule_running(&rule_mgr, &rule_id) {
                    continue;
                }
                rule_mgr.inc_source_records(&rule_id, 1);
                if handle_error_record(&rule_mgr, &rule_id, send_error, &sink, &record).await {
                    continue;
                }
                buffer.push(TaggedRow { source: from_source.clone(), data: record.data });
                events_since_trigger += 1;
                if hop <= count {
                    // Standard count window (tumbling when hop == count, overlapping when hop < count)
                    if buffer.len() >= count {
                        let batch = &buffer[0..count];
                        emit_window_batch(&rule_mgr, &rule_id, &table_manager, &source_configs, &select_stmt, batch, &sink).await;
                        // Discard only the oldest `hop` records; retain the rest for overlapping windows
                        buffer.drain(0..hop.min(buffer.len()));
                    }
                } else {
                    // Sparsely sampled count window with gap (hop > count)
                    if buffer.len() > count {
                        buffer.remove(0);
                    }
                    if events_since_trigger >= hop {
                        if !buffer.is_empty() {
                            emit_window_batch(&rule_mgr, &rule_id, &table_manager, &source_configs, &select_stmt, &buffer, &sink).await;
                        }
                        events_since_trigger = 0;
                        buffer.clear();
                    }
                }
            }
            Err(broadcast::error::RecvError::Lagged(_)) => continue,
            Err(broadcast::error::RecvError::Closed) => break,
                }
            }
            jrec = join_rx.recv(), if joins_open => {
                match jrec {
                    Some(tagged) => {
                        if !is_rule_running(&rule_mgr, &rule_id) {
                            continue;
                        }
                        rule_mgr.inc_source_records(&rule_id, 1);
                        let probe = StreamRecord::new(tagged.data.clone());
                        if handle_error_record(&rule_mgr, &rule_id, send_error, &sink, &probe).await {
                            continue;
                        }
                        buffer.push(tagged);
                        events_since_trigger += 1;
                        if hop <= count {
                            if buffer.len() >= count {
                                let batch = &buffer[0..count];
                                emit_window_batch(&rule_mgr, &rule_id, &table_manager, &source_configs, &select_stmt, batch, &sink).await;
                                buffer.drain(0..hop.min(buffer.len()));
                            }
                        } else if events_since_trigger >= hop {
                            if buffer.len() > count {
                                buffer.remove(0);
                            }
                            if !buffer.is_empty() {
                                emit_window_batch(&rule_mgr, &rule_id, &table_manager, &source_configs, &select_stmt, &buffer, &sink).await;
                            }
                            events_since_trigger = 0;
                            buffer.clear();
                        }
                    }
                    None => {
                        joins_open = false;
                    }
                }
            }
        }
    }
}

fn tumbling_window_duration(unit: &TimeUnit, length: u64) -> std::time::Duration {
    let millis: u128 = match unit {
        TimeUnit::Ms => length as u128,
        TimeUnit::Ss => length as u128 * 1_000,
        TimeUnit::Mi => length as u128 * 60_000,
        TimeUnit::Hh => length as u128 * 3_600_000,
        TimeUnit::Dd => length as u128 * 86_400_000,
    };
    let millis = millis.min(u64::MAX as u128) as u64;
    let duration = std::time::Duration::from_millis(millis);
    if duration.is_zero() {
        // tokio::time::interval panics on zero durations; clamp degenerate windows.
        std::time::Duration::from_millis(1)
    } else {
        duration
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_tumbling_window_rule(
    rule_mgr: RuleManager,
    rule_id: String,
    select_stmt: SelectStmt,
    mut rx: broadcast::Receiver<StreamRecord>,
    duration: std::time::Duration,
    sink: tokio::sync::mpsc::Sender<StreamRecord>,
    event_time: EventTimeConfig,
    send_error: bool,
    join_rxs: Vec<(String, broadcast::Receiver<StreamRecord>)>,
    table_manager: TableManager,
    source_configs: Arc<RwLock<HashMap<String, Value>>>,
) {
    let mut ticker = tokio::time::interval(duration);
    let mut buffer: Vec<TaggedRow> = Vec::new();
    // Event-time state: event-timestamped rows, the watermark, and the start
    // of the currently open event-time window (aligned to its length).
    let mut et_buffer: Vec<(i64, TaggedRow)> = Vec::new();
    let mut watermark: i64 = i64::MIN;
    let mut window_start: Option<i64> = None;
    let window_millis = duration.as_millis() as i64;
    let from_source = select_stmt.from.clone();
    let mut join_rx = spawn_join_forwarders(join_rxs);
    let mut joins_open = true;
    // Ingest one row (FROM or joined stream) into the wall/event buffers.
    macro_rules! ingest {
        ($tagged:expr) => {{
            let tagged: TaggedRow = $tagged;
            if event_time.enabled {
                let event_ts =
                    extract_event_timestamp(&tagged.data, event_time.timestamp_field.as_deref());
                if event_ts < watermark {
                    // Late arrival beyond the tolerance horizon: drop.
                } else {
                    watermark =
                        watermark.max(event_ts.saturating_sub(event_time.late_tolerance_ms));
                    let aligned = event_ts - event_ts.rem_euclid(window_millis.max(1));
                    if window_start.is_none() {
                        window_start = Some(aligned);
                    }
                    et_buffer.push((event_ts, tagged));
                    // Close every window the watermark has passed.
                    while let Some(t0) = window_start {
                        let t_end = t0.saturating_add(window_millis);
                        if watermark < t_end {
                            break;
                        }
                        let batch: Vec<TaggedRow> = et_buffer
                            .iter()
                            .filter(|(ts, _)| *ts >= t0 && *ts < t_end)
                            .map(|(_, row)| row.clone())
                            .collect();
                        et_buffer.retain(|(ts, _)| *ts >= t_end);
                        window_start = Some(t_end);
                        emit_window_batch(
                            &rule_mgr,
                            &rule_id,
                            &table_manager,
                            &source_configs,
                            &select_stmt,
                            &batch,
                            &sink,
                        )
                        .await;
                    }
                }
            } else {
                buffer.push(tagged);
            }
        }};
    }
    loop {
        tokio::select! {
            res = rx.recv() => {
                match res {
                    Ok(record) => {
                        if !is_rule_running(&rule_mgr, &rule_id) {
                            continue;
                        }
                        rule_mgr.inc_source_records(&rule_id, 1);
                        if handle_error_record(&rule_mgr, &rule_id, send_error, &sink, &record)
                            .await
                        {
                            continue;
                        }
                        ingest!(TaggedRow { source: from_source.clone(), data: record.data });
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
            jrec = join_rx.recv(), if joins_open => {
                match jrec {
                    Some(tagged) => {
                        if !is_rule_running(&rule_mgr, &rule_id) {
                            continue;
                        }
                        rule_mgr.inc_source_records(&rule_id, 1);
                        let probe = StreamRecord::new(tagged.data.clone());
                        if handle_error_record(&rule_mgr, &rule_id, send_error, &sink, &probe).await {
                            continue;
                        }
                        ingest!(tagged);
                    }
                    None => {
                        joins_open = false;
                    }
                }
            }
            _ = ticker.tick() => {
                if event_time.enabled {
                    // Windows close on watermark advance, never on the clock.
                    continue;
                }
                if buffer.is_empty() {
                    continue;
                }
                emit_window_batch(&rule_mgr, &rule_id, &table_manager, &source_configs, &select_stmt, &buffer, &sink).await;
                buffer.clear();
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_hopping_window_rule(
    rule_mgr: RuleManager,
    rule_id: String,
    select_stmt: SelectStmt,
    mut rx: broadcast::Receiver<StreamRecord>,
    length: std::time::Duration,
    hop: std::time::Duration,
    sink: tokio::sync::mpsc::Sender<StreamRecord>,
    send_error: bool,
    join_rxs: Vec<(String, broadcast::Receiver<StreamRecord>)>,
    table_manager: TableManager,
    source_configs: Arc<RwLock<HashMap<String, Value>>>,
) {
    let mut ticker = tokio::time::interval(hop);
    // Tokio's interval fires immediately on the first tick; consume it so the
    // first window emission aligns with elapsed hop time.
    ticker.tick().await;
    let mut buffer: Vec<(std::time::Instant, TaggedRow)> = Vec::new();
    let from_source = select_stmt.from.clone();
    let mut join_rx = spawn_join_forwarders(join_rxs);
    let mut joins_open = true;
    loop {
        tokio::select! {
            res = rx.recv() => {
                match res {
                    Ok(record) => {
                        if !is_rule_running(&rule_mgr, &rule_id) {
                            continue;
                        }
                        rule_mgr.inc_source_records(&rule_id, 1);
                        if handle_error_record(&rule_mgr, &rule_id, send_error, &sink, &record)
                            .await
                        {
                            continue;
                        }
                        buffer.push((std::time::Instant::now(), TaggedRow { source: from_source.clone(), data: record.data }));
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
            jrec = join_rx.recv(), if joins_open => {
                match jrec {
                    Some(tagged) => {
                        if !is_rule_running(&rule_mgr, &rule_id) {
                            continue;
                        }
                        rule_mgr.inc_source_records(&rule_id, 1);
                        let probe = StreamRecord::new(tagged.data.clone());
                        if handle_error_record(&rule_mgr, &rule_id, send_error, &sink, &probe).await {
                            continue;
                        }
                        buffer.push((std::time::Instant::now(), tagged));
                    }
                    None => {
                        joins_open = false;
                    }
                }
            }
            _ = ticker.tick() => {
                let now = std::time::Instant::now();
                // Expire and discard records older than the full window length
                buffer.retain(|(ts, _)| now.duration_since(*ts) <= length);
                if buffer.is_empty() {
                    continue;
                }
                let batch: Vec<TaggedRow> =
                    buffer.iter().map(|(_, row)| row.clone()).collect();
                emit_window_batch(&rule_mgr, &rule_id, &table_manager, &source_configs, &select_stmt, &batch, &sink).await;
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_sliding_window_rule(
    rule_mgr: RuleManager,
    rule_id: String,
    select_stmt: SelectStmt,
    mut rx: broadcast::Receiver<StreamRecord>,
    length: std::time::Duration,
    delay: Option<std::time::Duration>,
    sink: tokio::sync::mpsc::Sender<StreamRecord>,
    event_time: EventTimeConfig,
    send_error: bool,
    join_rxs: Vec<(String, broadcast::Receiver<StreamRecord>)>,
    table_manager: TableManager,
    source_configs: Arc<RwLock<HashMap<String, Value>>>,
) {
    let mut buffer: Vec<(std::time::Instant, TaggedRow)> = Vec::new();
    // Event-time state: event-timestamped rows plus the watermark.
    let mut et_buffer: Vec<(i64, TaggedRow)> = Vec::new();
    let mut watermark: i64 = i64::MIN;
    let window_millis = length.as_millis() as i64;
    let from_source = select_stmt.from.clone();
    let mut join_rx = spawn_join_forwarders(join_rxs);
    let mut joins_open = true;
    // Ingest one row then evaluate the trailing horizon (shared by FROM
    // and joined-stream rows).
    macro_rules! ingest_slide {
        ($tagged:expr) => {{
            let tagged: TaggedRow = $tagged;
            if event_time.enabled {
                let event_ts =
                    extract_event_timestamp(&tagged.data, event_time.timestamp_field.as_deref());
                if event_ts < watermark {
                    // Late arrival beyond the tolerance horizon: drop.
                } else {
                    watermark =
                        watermark.max(event_ts.saturating_sub(event_time.late_tolerance_ms));
                    et_buffer.push((event_ts, tagged));
                    et_buffer.sort_by_key(|(ts, _)| *ts);
                    // Lower-bounded horizon only: expiry is purely age-based
                    // (`ts >= event_ts - length`). Newer buffered rows must
                    // survive out-of-order arrivals within the window.
                    et_buffer.retain(|(ts, _)| *ts >= event_ts.saturating_sub(window_millis));
                    let batch: Vec<TaggedRow> =
                        et_buffer.iter().map(|(_, row)| row.clone()).collect();
                    emit_window_batch(
                        &rule_mgr,
                        &rule_id,
                        &table_manager,
                        &source_configs,
                        &select_stmt,
                        &batch,
                        &sink,
                    )
                    .await;
                }
            } else {
                let now = std::time::Instant::now();
                buffer.push((now, tagged));
                let eval_time = std::time::Instant::now();
                // Retain only events within the sliding trailing horizon: [eval_time - length, eval_time]
                buffer.retain(|(ts, _)| eval_time.duration_since(*ts) <= length);
                if !buffer.is_empty() {
                    let batch: Vec<TaggedRow> =
                        buffer.iter().map(|(_, row)| row.clone()).collect();
                    emit_window_batch(
                        &rule_mgr,
                        &rule_id,
                        &table_manager,
                        &source_configs,
                        &select_stmt,
                        &batch,
                        &sink,
                    )
                    .await;
                }
            }
        }};
    }
    loop {
        tokio::select! {
            res = rx.recv() => {
            match res {
            Ok(record) => {
                if !is_rule_running(&rule_mgr, &rule_id) {
                    continue;
                }
                rule_mgr.inc_source_records(&rule_id, 1);
                if handle_error_record(&rule_mgr, &rule_id, send_error, &sink, &record).await {
                    continue;
                }
                // If delay is configured, wait for the delay duration before evaluating
                // so events arriving during the delay window are captured.
                if let Some(delay_dur) = delay {
                    if !delay_dur.is_zero() {
                        tokio::time::sleep(delay_dur).await;
                    }
                }
                ingest_slide!(TaggedRow { source: from_source.clone(), data: record.data });
            }
            Err(broadcast::error::RecvError::Lagged(_)) => continue,
            Err(broadcast::error::RecvError::Closed) => break,
            }
            }
            jrec = join_rx.recv(), if joins_open => {
                match jrec {
                    Some(tagged) => {
                        if !is_rule_running(&rule_mgr, &rule_id) {
                            continue;
                        }
                        rule_mgr.inc_source_records(&rule_id, 1);
                        let probe = StreamRecord::new(tagged.data.clone());
                        if handle_error_record(&rule_mgr, &rule_id, send_error, &sink, &probe).await {
                            continue;
                        }
                        if let Some(delay_dur) = delay {
                            if !delay_dur.is_zero() {
                                tokio::time::sleep(delay_dur).await;
                            }
                        }
                        ingest_slide!(tagged);
                    }
                    None => {
                        joins_open = false;
                    }
                }
            }
        }
    }
}

async fn get_rule(State(state): State<AppState>, Path(name): Path<String>) -> Response {
    if let Err(resp) = check_valid_name(&name) {
        return resp;
    }
    if let Some(rule) = state.rule_manager.get_rule(&name) {
        Json(rule).into_response()
    } else {
        (StatusCode::NOT_FOUND, format!("Rule {} not found", name)).into_response()
    }
}

async fn get_rule_status(State(state): State<AppState>, Path(name): Path<String>) -> Response {
    if let Err(resp) = check_valid_name(&name) {
        return resp;
    }
    if let Some(status) = state.rule_manager.get_rule_status(&name) {
        Json(status).into_response()
    } else {
        (StatusCode::NOT_FOUND, format!("Rule {} not found", name)).into_response()
    }
}

async fn get_all_rule_status(State(state): State<AppState>) -> impl IntoResponse {
    let mut all = HashMap::new();
    for rule in state.rule_manager.list_rules() {
        if let Some(status) = state.rule_manager.get_rule_status(&rule.id) {
            all.insert(rule.id, status);
        }
    }
    Json(all)
}

/// Collect every called function name in an expression (including CASE
/// branches and analytic OVER calls).
fn collect_called_functions(expr: &Expr, out: &mut Vec<String>) {
    match expr {
        Expr::Call { name, args } => {
            out.push(name.clone());
            for arg in args {
                collect_called_functions(arg, out);
            }
        }
        Expr::BinaryOp { left, right, .. } => {
            collect_called_functions(left, out);
            collect_called_functions(right, out);
        }
        Expr::UnaryOp { expr, .. } => collect_called_functions(expr, out),
        Expr::Between {
            expr, low, high, ..
        } => {
            collect_called_functions(expr, out);
            collect_called_functions(low, out);
            collect_called_functions(high, out);
        }
        Expr::InList { expr, list, .. } => {
            collect_called_functions(expr, out);
            for item in list {
                collect_called_functions(item, out);
            }
        }
        Expr::IsNull { expr, .. } => collect_called_functions(expr, out),
        Expr::FieldAccess { parent, .. } => collect_called_functions(parent, out),
        Expr::Index { base, index } => {
            collect_called_functions(base, out);
            collect_called_functions(index, out);
        }
        Expr::Slice { base, lo, hi } => {
            collect_called_functions(base, out);
            if let Some(e) = lo {
                collect_called_functions(e, out);
            }
            if let Some(e) = hi {
                collect_called_functions(e, out);
            }
        }
        Expr::Case {
            operand,
            when_clauses,
            else_clause,
        } => {
            if let Some(op) = operand {
                collect_called_functions(op, out);
            }
            for (w, t) in when_clauses {
                collect_called_functions(w, out);
                collect_called_functions(t, out);
            }
            if let Some(e) = else_clause {
                collect_called_functions(e, out);
            }
        }
        Expr::Over { call, partition_by } => {
            collect_called_functions(call, out);
            if let Some(p) = partition_by {
                collect_called_functions(p, out);
            }
        }
        Expr::Wildcard | Expr::Identifier(_) | Expr::Literal(_) => {}
    }
}

/// Every function called anywhere in a SELECT statement (projections,
/// filters, grouping, joins, set-operation branches).
fn stmt_called_functions(stmt: &SelectStmt) -> Vec<String> {
    let mut out = Vec::new();
    for field in &stmt.fields {
        collect_called_functions(field, &mut out);
    }
    if let Some(w) = &stmt.where_clause {
        collect_called_functions(w, &mut out);
    }
    for g in &stmt.group_by {
        collect_called_functions(g, &mut out);
    }
    if let Some(h) = &stmt.having {
        collect_called_functions(h, &mut out);
    }
    for item in &stmt.order_by {
        collect_called_functions(&item.expr, &mut out);
    }
    for join in &stmt.joins {
        if let Some(on) = &join.on {
            collect_called_functions(on, &mut out);
        }
    }
    if let Some((_, rhs)) = &stmt.set_op {
        out.extend(stmt_called_functions(rhs));
    }
    out
}

/// First called function unknown to the built-in library, the global UDF
/// registry and registered function/UDF plugin definitions, if any.
fn find_unknown_function(stmt: &SelectStmt, plugins: &PluginManager) -> Option<String> {
    let global = rekuiper_core::plugin::get_global_udf_registry();
    let plugin_defs: Vec<PluginDefinition> = plugins
        .list_plugins("function")
        .into_iter()
        .chain(plugins.list_plugins("udf"))
        .collect();
    for name in stmt_called_functions(stmt) {
        if is_builtin_function(&name) || global.has_udf(&name) || plugins.has_udf(&name) {
            continue;
        }
        if plugin_defs
            .iter()
            .flat_map(|d| d.functions.iter())
            .any(|f| f.eq_ignore_ascii_case(&name))
        {
            continue;
        }
        return Some(name);
    }
    None
}

/// 422 rejection when a rule calls an unknown function; `None` when clean.
fn check_rule_functions(state: &AppState, stmt: &SelectStmt) -> Option<Response> {
    find_unknown_function(stmt, &state.plugin_manager).map(|bad_fn| {
        (
            StatusCode::UNPROCESSABLE_ENTITY,
            format!(
                "invalid rule json: Parse SQL ... error: function {} not found.",
                bad_fn
            ),
        )
            .into_response()
    })
}

/// Shared create/update gate: the source stream or table must exist and
/// every called function must be known. Returns the rejection response
/// when the rule is invalid.
fn reject_invalid_rule(state: &AppState, stmt: &SelectStmt) -> Option<Response> {
    let stream_exists = state.stream_manager.get_stream(&stmt.from).is_some()
        || state.table_manager.get_table(&stmt.from).is_some();
    if !stream_exists {
        return Some(
            (
                StatusCode::BAD_REQUEST,
                Json(json!({
                    "error": 1000,
                    "message": format!(
                        "fail to get stream {}, please check if stream is created",
                        stmt.from
                    )
                })),
            )
                .into_response(),
        );
    }
    if let Some(bad_fn) = find_unknown_function(stmt, &state.plugin_manager) {
        return Some(
            (
                StatusCode::BAD_REQUEST,
                Json(json!({
                    "error": 1000,
                    "message": format!(
                        "invalid rule json: Parse SQL ... error: function {} not found.",
                        bad_fn
                    )
                })),
            )
                .into_response(),
        );
    }
    None
}

async fn validate_rule(
    State(state): State<AppState>,
    Json(rule): Json<RuleDefinition>,
) -> Response {
    if rule.sql.trim().is_empty() {
        if let Some(ref graph) = rule.graph {
            return match compile_graph_to_sql_and_actions(graph) {
                Ok((sql, _)) => {
                    let mut parser = Parser::new(&sql);
                    match parser.parse_select() {
                        Ok(stmt) => check_rule_functions(&state, &stmt).unwrap_or_else(|| {
                            Json(json!({
                                "sources": graph.topo.sources,
                                "valid": true
                            }))
                            .into_response()
                        }),
                        Err(e) => (StatusCode::BAD_REQUEST, format!("Invalid rule SQL: {}", e))
                            .into_response(),
                    }
                }
                Err(e) => (
                    StatusCode::BAD_REQUEST,
                    format!("Invalid rule graph: {}", e),
                )
                    .into_response(),
            };
        }
    }
    let mut parser = Parser::new(&rule.sql);
    match parser.parse_select() {
        Ok(stmt) => check_rule_functions(&state, &stmt).unwrap_or_else(|| {
            Json(json!({
                "sources": [stmt.from],
                "valid": true
            }))
            .into_response()
        }),
        Err(e) => (StatusCode::BAD_REQUEST, format!("Invalid rule SQL: {}", e)).into_response(),
    }
}

async fn get_rule_topo(State(state): State<AppState>, Path(name): Path<String>) -> Response {
    if let Err(resp) = check_valid_name(&name) {
        return resp;
    }
    let Some(rule) = state.rule_manager.get_rule(&name) else {
        return (StatusCode::NOT_FOUND, format!("Rule {} not found", name)).into_response();
    };
    // Graph rules report their native DAG topology.
    if let Some(ref graph) = rule.graph {
        if !graph.nodes.is_empty() {
            return Json(json!({
                "sources": graph.topo.sources,
                "nodes": graph.nodes.keys().cloned().collect::<Vec<_>>(),
                "edges": graph.topo.edges,
            }))
            .into_response();
        }
    }
    let mut parser = Parser::new(&rule.sql);
    let select_stmt = match parser.parse_select() {
        Ok(s) => s,
        Err(e) => {
            return (StatusCode::BAD_REQUEST, format!("Invalid rule SQL: {}", e)).into_response();
        }
    };
    let from = select_stmt.from.clone();
    let source_node = format!("source_{}", from);
    let mut edges = serde_json::Map::new();
    edges.insert(source_node.clone(), json!(["op_eval"]));
    edges.insert("op_eval".to_string(), json!(["sink_actions"]));
    Json(json!({
        "sources": [from],
        "nodes": [source_node, "op_eval", "sink_actions"],
        "edges": edges,
    }))
    .into_response()
}

async fn get_rule_explain(State(state): State<AppState>, Path(name): Path<String>) -> Response {
    if let Err(resp) = check_valid_name(&name) {
        return resp;
    }
    let Some(rule) = state.rule_manager.get_rule(&name) else {
        return (StatusCode::NOT_FOUND, format!("Rule {} not found", name)).into_response();
    };
    let mut parser = Parser::new(&rule.sql);
    let select_stmt = match parser.parse_select() {
        Ok(s) => s,
        Err(e) => {
            return (StatusCode::BAD_REQUEST, format!("Invalid rule SQL: {}", e)).into_response();
        }
    };
    let action_kinds: Vec<String> = rule
        .actions
        .iter()
        .flat_map(|a| a.keys().cloned())
        .collect();
    Json(json!({
        "rule": name,
        "source": select_stmt.from,
        "projection": select_stmt.fields.iter().map(expr_to_string).collect::<Vec<_>>(),
        "filter": select_stmt.where_clause.as_ref().map(expr_to_string),
        "window": select_stmt.window.as_ref().map(window_to_string),
        "groupBy": select_stmt.group_by.iter().map(expr_to_string).collect::<Vec<_>>(),
        "having": select_stmt.having.as_ref().map(expr_to_string),
        "actions": action_kinds,
    }))
    .into_response()
}

fn time_unit_to_string(unit: &TimeUnit) -> &'static str {
    match unit {
        TimeUnit::Dd => "dd",
        TimeUnit::Hh => "hh",
        TimeUnit::Mi => "mi",
        TimeUnit::Ss => "ss",
        TimeUnit::Ms => "ms",
    }
}

fn window_to_string(window: &WindowDef) -> String {
    match window {
        WindowDef::TumblingTime { unit, length } => {
            format!("TUMBLINGWINDOW({}, {})", time_unit_to_string(unit), length)
        }
        WindowDef::HoppingTime {
            unit,
            length,
            interval,
        } => format!(
            "HOPPINGWINDOW({}, {}, {})",
            time_unit_to_string(unit),
            length,
            interval
        ),
        WindowDef::SlidingTime {
            unit,
            length,
            delay,
        } => match delay {
            Some(d) => format!(
                "SLIDINGWINDOW({}, {}, {})",
                time_unit_to_string(unit),
                length,
                d
            ),
            None => format!("SLIDINGWINDOW({}, {})", time_unit_to_string(unit), length),
        },
        WindowDef::Count { size, interval } => match interval {
            Some(i) => format!("COUNTWINDOW({}, {})", size, i),
            None => format!("COUNTWINDOW({})", size),
        },
    }
}

fn expr_to_string(expr: &Expr) -> String {
    match expr {
        Expr::Wildcard => "*".to_string(),
        Expr::Identifier(name) => name.clone(),
        Expr::Literal(v) => v.to_string(),
        Expr::BinaryOp { left, op, right } => {
            let op_str = match op {
                rekuiper_sql::BinaryOperator::Eq => "=",
                rekuiper_sql::BinaryOperator::Neq => "!=",
                rekuiper_sql::BinaryOperator::Lt => "<",
                rekuiper_sql::BinaryOperator::Lte => "<=",
                rekuiper_sql::BinaryOperator::Gt => ">",
                rekuiper_sql::BinaryOperator::Gte => ">=",
                rekuiper_sql::BinaryOperator::And => "AND",
                rekuiper_sql::BinaryOperator::Or => "OR",
                rekuiper_sql::BinaryOperator::Add => "+",
                rekuiper_sql::BinaryOperator::Sub => "-",
                rekuiper_sql::BinaryOperator::Mul => "*",
                rekuiper_sql::BinaryOperator::Div => "/",
                rekuiper_sql::BinaryOperator::Mod => "%",
                rekuiper_sql::BinaryOperator::Like => "LIKE",
            };
            format!(
                "{} {} {}",
                expr_to_string(left),
                op_str,
                expr_to_string(right)
            )
        }
        Expr::UnaryOp { op, expr } => match op {
            rekuiper_sql::UnaryOperator::Not => format!("NOT {}", expr_to_string(expr)),
            rekuiper_sql::UnaryOperator::Neg => format!("-{}", expr_to_string(expr)),
        },
        Expr::Between {
            expr,
            low,
            high,
            negated,
        } => format!(
            "{} {}BETWEEN {} AND {}",
            expr_to_string(expr),
            if *negated { "NOT " } else { "" },
            expr_to_string(low),
            expr_to_string(high)
        ),
        Expr::InList {
            expr,
            list,
            negated,
        } => format!(
            "{} {}IN ({})",
            expr_to_string(expr),
            if *negated { "NOT " } else { "" },
            list.iter()
                .map(expr_to_string)
                .collect::<Vec<_>>()
                .join(", ")
        ),
        Expr::IsNull { expr, negated } => format!(
            "{} IS {}NULL",
            expr_to_string(expr),
            if *negated { "NOT " } else { "" }
        ),
        Expr::FieldAccess { parent, field } => {
            format!("{}.{}", expr_to_string(parent), field)
        }
        Expr::Index { base, index } => {
            format!("{}[{}]", expr_to_string(base), expr_to_string(index))
        }
        Expr::Slice { base, lo, hi } => format!(
            "{}[{}:{}]",
            expr_to_string(base),
            lo.as_ref().map(|e| expr_to_string(e)).unwrap_or_default(),
            hi.as_ref().map(|e| expr_to_string(e)).unwrap_or_default()
        ),
        Expr::Call { name, args } => format!(
            "{}({})",
            name,
            args.iter()
                .map(expr_to_string)
                .collect::<Vec<_>>()
                .join(", ")
        ),
        Expr::Case { .. } => "CASE ... END".to_string(),
        Expr::Over { call, .. } => format!("{} OVER (...)", expr_to_string(call)),
    }
}

fn activate_rule(state: &AppState, rule_id: &str) {
    if let Some(rule) = state.rule_manager.get_rule(rule_id) {
        let mut parser = Parser::new(&rule.sql);
        if let Ok(select_stmt) = parser.parse_select() {
            spawn_rule_task(
                &state.rule_manager,
                &state.stream_bus,
                &state.stream_manager,
                &state.table_manager,
                &state.source_configs,
                &state.http_client,
                &state.trace_manager,
                rule_id.to_string(),
                select_stmt.clone(),
                rule.actions.clone(),
                rule.options.clone(),
            );
            bootstrap_rule_sources(state, rule_id, &select_stmt);
        }
    }
}

async fn start_rule(State(state): State<AppState>, Path(name): Path<String>) -> Response {
    if let Err(resp) = check_valid_name(&name) {
        return resp;
    }
    match state.rule_manager.start_rule(&name).await {
        Ok(_) => {
            activate_rule(&state, &name);
            (StatusCode::OK, format!("Rule {} was started", name)).into_response()
        }
        Err(e) => (StatusCode::NOT_FOUND, e.to_string()).into_response(),
    }
}

async fn stop_rule(State(state): State<AppState>, Path(name): Path<String>) -> Response {
    if let Err(resp) = check_valid_name(&name) {
        return resp;
    }
    match state.rule_manager.stop_rule(&name).await {
        Ok(_) => {
            cancel_rule_source(&state, &name);
            state.trace_manager.stop_trace(&name);
            (StatusCode::OK, format!("Rule {} was stopped", name)).into_response()
        }
        Err(e) => (StatusCode::NOT_FOUND, e.to_string()).into_response(),
    }
}

async fn restart_rule(State(state): State<AppState>, Path(name): Path<String>) -> Response {
    if let Err(resp) = check_valid_name(&name) {
        return resp;
    }
    let _ = state.rule_manager.stop_rule(&name).await;
    cancel_rule_source(&state, &name);
    match state.rule_manager.start_rule(&name).await {
        Ok(_) => {
            activate_rule(&state, &name);
            (StatusCode::OK, format!("Rule {} was restarted", name)).into_response()
        }
        Err(e) => (StatusCode::NOT_FOUND, e.to_string()).into_response(),
    }
}

async fn delete_rule(State(state): State<AppState>, Path(name): Path<String>) -> Response {
    if let Err(resp) = check_valid_name(&name) {
        return resp;
    }
    match state.rule_manager.delete_rule(&name).await {
        Ok(_) => {
            cancel_rule_source(&state, &name);
            state.trace_manager.stop_trace(&name);
            (StatusCode::OK, format!("Rule {} is dropped.\n", name)).into_response()
        }
        Err(e) => (StatusCode::NOT_FOUND, e.to_string()).into_response(),
    }
}

/// Replace a rule definition (eKuiper `PUT /rules/:name`): the running worker
/// and its sources are stopped, the definition is swapped, and the pipeline
/// is restarted with the new SQL, actions and options. The path name is
/// canonical. Missing rules 404.
async fn update_rule(
    State(state): State<AppState>,
    Path(name): Path<String>,
    Json(mut rule): Json<RuleDefinition>,
) -> Response {
    if let Err(resp) = check_valid_name(&name) {
        return resp;
    }
    if state.rule_manager.get_rule(&name).is_none() {
        return (StatusCode::NOT_FOUND, format!("Rule {} not found", name)).into_response();
    }
    rule.id = name.clone();
    // Graph rules carry no SQL: compile the DAG first (mirrors creation).
    if rule.sql.trim().is_empty() {
        if let Some(ref graph) = rule.graph {
            match compile_graph_to_sql_and_actions(graph) {
                Ok((sql, actions)) => {
                    rule.sql = sql;
                    if rule.actions.is_empty() {
                        rule.actions = actions;
                    }
                }
                Err(e) => {
                    return (
                        StatusCode::BAD_REQUEST,
                        format!("Invalid rule graph: {}", e),
                    )
                        .into_response();
                }
            }
        }
    }
    let mut parser = Parser::new(&rule.sql);
    let select_stmt = match parser.parse_select() {
        Ok(s) => s,
        Err(e) => {
            return (StatusCode::BAD_REQUEST, format!("Invalid rule SQL: {}", e)).into_response();
        }
    };
    if let Some(resp) = reject_invalid_rule(&state, &select_stmt) {
        return resp;
    }
    // Stop the running worker and its sources before replacing the definition.
    let _ = state.rule_manager.stop_rule(&name).await;
    cancel_rule_source(&state, &name);
    state.trace_manager.stop_trace(&name);
    if let Err(e) = state.rule_manager.delete_rule(&name).await {
        return (StatusCode::NOT_FOUND, e.to_string()).into_response();
    }
    if let Err(e) = state.rule_manager.create_rule(rule.clone()).await {
        return (StatusCode::BAD_REQUEST, e.to_string()).into_response();
    }
    spawn_rule_task(
        &state.rule_manager,
        &state.stream_bus,
        &state.stream_manager,
        &state.table_manager,
        &state.source_configs,
        &state.http_client,
        &state.trace_manager,
        name.clone(),
        select_stmt.clone(),
        rule.actions.clone(),
        rule.options.clone(),
    );
    bootstrap_rule_sources(&state, &name, &select_stmt);
    (
        StatusCode::OK,
        format!("Rule {} was updated successfully.\n", name),
    )
        .into_response()
}

async fn get_configs(State(state): State<AppState>) -> impl IntoResponse {
    Json(state.config.read().clone())
}

/// Partial runtime configuration update (eKuiper `PATCH /configs`): deep
/// merges the JSON object into the live config and answers 204 No Content.
async fn patch_configs(State(state): State<AppState>, Json(patch): Json<Value>) -> Response {
    let Value::Object(overlay) = patch else {
        return (StatusCode::BAD_REQUEST, "Expected a JSON object").into_response();
    };
    let mut current = serde_json::to_value(state.config.read().clone()).unwrap_or(json!({}));
    merge_json_object(&mut current, &Value::Object(overlay));
    match serde_json::from_value::<KuiperConfig>(current) {
        Ok(updated) => {
            *state.config.write() = updated;
            StatusCode::NO_CONTENT.into_response()
        }
        Err(e) => (
            StatusCode::BAD_REQUEST,
            format!("Invalid config patch: {}", e),
        )
            .into_response(),
    }
}

/// Recursively merges `overlay` objects into `base`; scalars and arrays are
/// replaced, nested objects merge key by key.
fn merge_json_object(base: &mut Value, overlay: &Value) {
    match (base, overlay) {
        (Value::Object(base_map), Value::Object(overlay_map)) => {
            for (k, v) in overlay_map {
                match base_map.get_mut(k) {
                    Some(existing) => merge_json_object(existing, v),
                    None => {
                        base_map.insert(k.clone(), v.clone());
                    }
                }
            }
        }
        (base_slot, v) => {
            *base_slot = v.clone();
        }
    }
}

/// Unified ruleset export: all streams, tables and rules with full definitions.
async fn export_ruleset(State(state): State<AppState>) -> impl IntoResponse {
    let streams: Vec<StreamDefinition> = {
        let names = state.stream_manager.list_streams();
        names
            .iter()
            .filter_map(|n| state.stream_manager.get_stream(n))
            .collect()
    };
    let tables: Vec<TableDefinition> = {
        let names = state.table_manager.list_tables();
        names
            .iter()
            .filter_map(|n| state.table_manager.get_table(n))
            .collect()
    };
    let rules = state.rule_manager.list_rules();
    Json(json!({
        "streams": streams,
        "tables": tables,
        "rules": rules,
    }))
}

/// Selective ruleset export (baseline `POST /data/export`): exports the
/// requested rules plus their dependent streams and tables. An absent or
/// empty selection exports everything.
async fn export_data_selected(
    State(state): State<AppState>,
    Json(payload): Json<Value>,
) -> Response {
    let wanted: Option<HashSet<String>> =
        payload.get("rules").and_then(|v| v.as_array()).map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(|s| s.to_string()))
                .collect()
        });
    let mut rules = state.rule_manager.list_rules();
    if let Some(ids) = &wanted {
        if !ids.is_empty() {
            rules.retain(|r| ids.contains(&r.id));
        }
    }
    // Dependent stream/table names from FROM + JOIN clauses (best effort:
    // rules whose SQL will not parse export without dependencies).
    let mut dep_names = HashSet::new();
    for rule in &rules {
        let mut parser = Parser::new(&rule.sql);
        if let Ok(stmt) = parser.parse_select() {
            dep_names.insert(stmt.from.clone());
            for join in &stmt.joins {
                dep_names.insert(join.target.clone());
            }
        }
    }
    let mut dep_names: Vec<String> = dep_names.into_iter().collect();
    dep_names.sort();
    let mut streams = Vec::new();
    let mut tables = Vec::new();
    for name in dep_names {
        if let Some(def) = state.stream_manager.get_stream(&name) {
            streams.push(def);
        } else if let Some(def) = state.table_manager.get_table(&name) {
            tables.push(def);
        }
    }
    Json(json!({
        "streams": streams,
        "tables": tables,
        "rules": rules,
    }))
    .into_response()
}

/// Counts of entities a data import actually created.
#[derive(Default)]
struct ImportCounts {
    streams: usize,
    tables: usize,
    rules: usize,
}

/// Core data import logic shared by synchronous and asynchronous endpoints.
/// Returns how many streams, tables and rules were actually created.
async fn process_import_payload(state: &AppState, payload: &Value) -> ImportCounts {
    let actual_payload: Value = if let Some(content) = payload.get("content") {
        if let Some(s) = content.as_str() {
            serde_json::from_str::<Value>(s)
                .or_else(|_| serde_yaml::from_str::<Value>(s))
                .unwrap_or_else(|_| content.clone())
        } else if content.is_object() {
            content.clone()
        } else {
            payload.clone()
        }
    } else if let Some(file_val) = payload.get("file").and_then(|f| f.as_str()) {
        let file_path = file_val.strip_prefix("file://").unwrap_or(file_val);
        if let Ok(s) = std::fs::read_to_string(file_path) {
            serde_json::from_str::<Value>(&s)
                .or_else(|_| serde_yaml::from_str::<Value>(&s))
                .unwrap_or(Value::Null)
        } else {
            Value::Null
        }
    } else {
        payload.clone()
    };

    let mut status = default_import_status();
    let mut counts = ImportCounts::default();

    if let Some(streams) = actual_payload.get("streams") {
        if let Some(defs) = streams.as_array() {
            for item in defs {
                match serde_json::from_value::<StreamDefinition>(item.clone()) {
                    Ok(def) => {
                        let name = def.name.clone();
                        if let Err(e) = state.stream_manager.create_stream(def).await {
                            status["streams"][&name] = json!(e.to_string());
                        } else {
                            state.stream_bus.get_or_create(&name);
                            counts.streams += 1;
                        }
                    }
                    Err(e) => {
                        status["streams"]["unknown"] =
                            json!(format!("invalid stream definition: {}", e));
                    }
                }
            }
        } else if let Some(map) = streams.as_object() {
            for (name, sql) in map {
                let sql_str = sql.as_str().unwrap_or("");
                let mut parser = Parser::new(sql_str);
                if let Ok(stmt) = parser.parse_create_stream() {
                    let stream_name = stmt.name.clone();
                    if let Err(e) = state
                        .stream_manager
                        .create_stream(StreamDefinition {
                            name: stream_name.clone(),
                            sql: sql_str.to_string(),
                            stream_fields: to_stream_fields(stmt.fields),
                            options: stmt.options,
                        })
                        .await
                    {
                        status["streams"][&stream_name] = json!(e.to_string());
                    } else {
                        state.stream_bus.get_or_create(&stream_name);
                        counts.streams += 1;
                    }
                } else if !name.is_empty() {
                    if let Err(e) = state
                        .stream_manager
                        .create_stream(StreamDefinition {
                            name: name.clone(),
                            sql: sql_str.to_string(),
                            stream_fields: Vec::new(),
                            options: HashMap::new(),
                        })
                        .await
                    {
                        status["streams"][name] = json!(e.to_string());
                    } else {
                        state.stream_bus.get_or_create(name);
                        counts.streams += 1;
                    }
                }
            }
        }
    }

    if let Some(tables) = actual_payload.get("tables") {
        if let Some(defs) = tables.as_array() {
            for item in defs {
                match serde_json::from_value::<TableDefinition>(item.clone()) {
                    Ok(def) => {
                        let name = def.name.clone();
                        if let Err(e) = state.table_manager.create_table(def).await {
                            status["tables"][&name] = json!(e.to_string());
                        } else {
                            counts.tables += 1;
                        }
                    }
                    Err(e) => {
                        status["tables"]["unknown"] =
                            json!(format!("invalid table definition: {}", e));
                    }
                }
            }
        } else if let Some(map) = tables.as_object() {
            for (name, sql) in map {
                let sql_str = sql.as_str().unwrap_or("");
                let mut parser = Parser::new(sql_str);
                if let Ok(stmt) = parser.parse_create_table() {
                    let table_name = stmt.name.clone();
                    if let Err(e) = state
                        .table_manager
                        .create_table(TableDefinition {
                            name: table_name.clone(),
                            sql: sql_str.to_string(),
                            stream_fields: to_stream_fields(stmt.fields),
                            options: stmt.options,
                        })
                        .await
                    {
                        status["tables"][&table_name] = json!(e.to_string());
                    } else {
                        counts.tables += 1;
                    }
                } else if !name.is_empty() {
                    if let Err(e) = state
                        .table_manager
                        .create_table(TableDefinition {
                            name: name.clone(),
                            sql: sql_str.to_string(),
                            stream_fields: Vec::new(),
                            options: HashMap::new(),
                        })
                        .await
                    {
                        status["tables"][name] = json!(e.to_string());
                    } else {
                        counts.tables += 1;
                    }
                }
            }
        }
    }

    if let Some(rules) = actual_payload.get("rules") {
        // Rules arrive either as an array of definitions or as a map of
        // id -> definition (missing ids are filled from the map keys).
        let defs: Vec<Value> = if let Some(arr) = rules.as_array() {
            arr.clone()
        } else if let Some(map) = rules.as_object() {
            map.iter()
                .filter_map(|(k, v)| {
                    let mut obj = if let Some(s) = v.as_str() {
                        serde_json::from_str::<Value>(s).ok()?
                    } else {
                        v.clone()
                    };
                    if let Some(m) = obj.as_object_mut() {
                        if !m.contains_key("id") {
                            m.insert("id".to_string(), Value::String(k.clone()));
                        }
                    }
                    Some(obj)
                })
                .collect()
        } else {
            Vec::new()
        };
        for item in defs {
            let Ok(def) = serde_json::from_value::<RuleDefinition>(item.clone()) else {
                status["rules"]["unknown"] = json!("invalid rule definition");
                continue;
            };
            let mut parser = Parser::new(&def.sql);
            let Ok(select_stmt) = parser.parse_select() else {
                status["rules"][&def.id] = json!("failed to parse SQL");
                continue;
            };
            if let Err(e) = state.rule_manager.create_rule(def.clone()).await {
                status["rules"][&def.id] = json!(e.to_string());
                continue;
            }
            counts.rules += 1;
            spawn_rule_task(
                &state.rule_manager,
                &state.stream_bus,
                &state.stream_manager,
                &state.table_manager,
                &state.source_configs,
                &state.http_client,
                &state.trace_manager,
                def.id.clone(),
                select_stmt.clone(),
                def.actions.clone(),
                def.options.clone(),
            );
            bootstrap_rule_sources(state, &def.id, &select_stmt);
        }
    }

    *state.latest_import_status.write() = status;
    counts
}

/// Baseline `POST /data/import`: runs the import and answers the structured
/// configuration envelope.
async fn import_data(State(state): State<AppState>, body: Bytes) -> Response {
    let payload: Value = serde_json::from_slice(&body).unwrap_or(Value::Null);
    process_import_payload(&state, &payload).await;
    (
        StatusCode::OK,
        Json(json!({
            "ErrorMsg": "",
            "ConfigResponse": {
                "streams": {},
                "tables": {},
                "rules": {},
                "nativePlugins": {},
                "portablePlugins": {},
                "sourceConfig": {},
                "sinkConfig": {},
                "connectionConfig": {}
            }
        })),
    )
        .into_response()
}

/// Unified ruleset import: creates streams, tables and rules from an export
/// payload. Also accepts the legacy `{name: sql}` map form for streams and
/// tables. Existing entities are left untouched.
async fn import_ruleset(State(state): State<AppState>, body: Bytes) -> Response {
    let payload: Value = serde_json::from_slice(&body).unwrap_or(Value::Null);
    let counts = process_import_payload(&state, &payload).await;
    (
        StatusCode::OK,
        format!(
            "imported {} streams, {} tables and {} rules\n",
            counts.streams, counts.tables, counts.rules
        ),
    )
        .into_response()
}

// ---------------------------------------------------------------------------
// eKuiper Manager OpenAPI: metadata discovery, connections, plugins,
// services, schemas and system utilities.
// ---------------------------------------------------------------------------

fn named_entries(names: &[&str]) -> Value {
    Value::Array(names.iter().map(|n| json!({ "name": n })).collect())
}

/// Rejects resource names carrying characters that break routing or the
/// manager UI (mirrors eKuiper's validation FVT expectations).
#[allow(clippy::result_large_err)]
fn check_valid_name(name: &str) -> Result<(), Response> {
    if name.contains(' ')
        || name.contains("%20")
        || name.contains(';')
        || name.contains('/')
        || name.contains('\\')
    {
        return Err((
            StatusCode::BAD_REQUEST,
            format!("name '{}' contains invalid characters", name),
        )
            .into_response());
    }
    Ok(())
}

/// Item representing a single nested request inside a POST /batch/req payload.
#[derive(Debug, Clone, Deserialize)]
pub struct BatchRequestItem {
    #[serde(alias = "action")]
    pub method: String,
    #[serde(alias = "url")]
    pub path: String,
    #[serde(default, alias = "params", alias = "payload")]
    pub body: Option<Value>,
}

/// Result of executing a single nested request inside POST /batch/req.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct BatchResponseItem {
    pub code: u16,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub response: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// Executes a sequential batch of REST API requests within the engine.
async fn handle_batch_req(State(state): State<AppState>, body: Bytes) -> impl IntoResponse {
    let items: Vec<BatchRequestItem> = if body.is_empty() {
        Vec::new()
    } else {
        match serde_json::from_slice(&body) {
            Ok(v) => v,
            Err(e) => {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(json!({ "error": format!("invalid batch request JSON: {}", e) })),
                )
                    .into_response();
            }
        }
    };

    use tower::ServiceExt;

    let mut results = Vec::with_capacity(items.len());
    for item in items {
        let raw_path = if let Some(idx) = item.path.find("://") {
            if let Some(path_start) = item.path[idx + 3..].find('/') {
                &item.path[idx + 3 + path_start..]
            } else {
                "/"
            }
        } else {
            &item.path
        };
        let path = if raw_path.starts_with('/') {
            raw_path.to_string()
        } else {
            format!("/{}", raw_path)
        };

        if path == "/batch/req" {
            results.push(BatchResponseItem {
                code: 400,
                response: None,
                error: Some("nested batch requests are not supported".to_string()),
            });
            continue;
        }

        let method = match item.method.to_ascii_uppercase().as_str() {
            "GET" => axum::http::Method::GET,
            "POST" => axum::http::Method::POST,
            "PUT" => axum::http::Method::PUT,
            "DELETE" => axum::http::Method::DELETE,
            "PATCH" => axum::http::Method::PATCH,
            "HEAD" => axum::http::Method::HEAD,
            "OPTIONS" => axum::http::Method::OPTIONS,
            _ => {
                results.push(BatchResponseItem {
                    code: 400,
                    response: None,
                    error: Some(format!("unsupported HTTP method: {}", item.method)),
                });
                continue;
            }
        };

        let body_bytes = match item.body {
            None => Vec::new(),
            Some(Value::String(s)) => s.into_bytes(),
            Some(v) => serde_json::to_vec(&v).unwrap_or_default(),
        };

        let router = create_router(state.clone());
        let req_res = axum::http::Request::builder()
            .method(method)
            .uri(&path)
            .header(axum::http::header::CONTENT_TYPE, "application/json")
            .body(axum::body::Body::from(body_bytes));

        let req = match req_res {
            Ok(r) => r,
            Err(e) => {
                results.push(BatchResponseItem {
                    code: 400,
                    response: None,
                    error: Some(format!("invalid request: {}", e)),
                });
                continue;
            }
        };

        match router.oneshot(req).await {
            Ok(resp) => {
                let status = resp.status();
                let body = resp.into_body();
                let bytes_res = axum::body::to_bytes(body, 10 * 1024 * 1024).await;
                let body_str = match bytes_res {
                    Ok(b) => String::from_utf8_lossy(&b).to_string(),
                    Err(e) => format!("error reading response body: {}", e),
                };

                if status.is_success() {
                    results.push(BatchResponseItem {
                        code: status.as_u16(),
                        response: Some(body_str),
                        error: None,
                    });
                } else {
                    results.push(BatchResponseItem {
                        code: status.as_u16(),
                        response: None,
                        error: Some(body_str),
                    });
                }
            }
            Err(e) => {
                results.push(BatchResponseItem {
                    code: 500,
                    response: None,
                    error: Some(format!("internal server error: {}", e)),
                });
            }
        }
    }

    (StatusCode::OK, Json(results)).into_response()
}

// ---------------------------------------------------------------------------
// External Services and Functions registry (`/services`, `/services/:name`,
// `/services/functions`, `/services/functions/:name`) and
// Embedded JavaScript UDF engine (`/udf/javascript`, `/udf/javascript/:id`).
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JavascriptUdf {
    #[serde(default)]
    pub id: String,
    #[serde(default)]
    pub description: String,
    #[serde(default)]
    pub script: String,
    #[serde(default, rename = "isAgg")]
    pub is_agg: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExternalFunction {
    #[serde(rename = "ServiceName")]
    pub service_name: String,
    #[serde(rename = "InterfaceName")]
    pub interface_name: String,
    #[serde(rename = "Addr")]
    pub addr: String,
    #[serde(rename = "MethodName")]
    pub method_name: String,
    #[serde(rename = "FuncName")]
    pub func_name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServiceDetail {
    #[serde(default, rename = "About")]
    pub about: HashMap<String, Value>,
    #[serde(default, rename = "Interfaces")]
    pub interfaces: HashMap<String, Value>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub functions: Vec<ExternalFunction>,
}

pub fn compile_and_register_js_udf(udf: &JavascriptUdf) -> Result<(), String> {
    let script = udf.script.trim();
    if script.is_empty() {
        return Err("script cannot be empty".to_string());
    }
    let fn_name = udf.id.trim();
    if fn_name.is_empty() {
        return Err("id cannot be empty".to_string());
    }

    let mut context = boa_engine::Context::default();
    if let Err(e) = context.eval(boa_engine::Source::from_bytes(script.as_bytes())) {
        return Err(format!("JavaScript compilation error: {:?}", e));
    }

    let fn_name_owned = fn_name.to_string();
    let script_owned = script.to_string();
    let handler: rekuiper_core::plugin::UdfFn = Arc::new(move |args: &[Value]| {
        let mut context = boa_engine::Context::default();
        if let Err(e) = context.eval(boa_engine::Source::from_bytes(script_owned.as_bytes())) {
            tracing::warn!("JS UDF {} script init error: {:?}", fn_name_owned, e);
            return Value::Null;
        }
        let args_json = serde_json::to_string(args).unwrap_or_else(|_| "[]".to_string());
        let invoke_code = format!(
            "JSON.stringify((function() {{ let r = {}.apply(null, {}); return r === undefined ? null : r; }})())",
            fn_name_owned, args_json
        );
        match context.eval(boa_engine::Source::from_bytes(invoke_code.as_bytes())) {
            Ok(res) => {
                if let Some(s) = res.as_string() {
                    serde_json::from_str(&s.to_std_string_escaped()).unwrap_or(Value::Null)
                } else {
                    Value::Null
                }
            }
            Err(e) => {
                tracing::warn!("JS UDF {} execution error: {:?}", fn_name_owned, e);
                Value::Null
            }
        }
    });

    rekuiper_core::plugin::get_global_udf_registry().register_udf(fn_name, handler);
    Ok(())
}

pub fn create_default_services() -> Arc<RwLock<HashMap<String, ServiceDetail>>> {
    let mut map = HashMap::new();
    let mut about = HashMap::new();
    about.insert(
        "description".to_string(),
        json!("EdgeX Foundry service integration"),
    );
    about.insert("author".to_string(), json!("EMQ"));
    about.insert("version".to_string(), json!("1.0.0"));

    let mut interfaces = HashMap::new();
    interfaces.insert(
        "core-data".to_string(),
        json!({
            "protocol": "rest",
            "port": 59900
        }),
    );

    let echo_fn = ExternalFunction {
        service_name: "edgex".to_string(),
        interface_name: "core-data".to_string(),
        addr: "tcp://localhost:59900".to_string(),
        method_name: "echo".to_string(),
        func_name: "echo".to_string(),
    };

    let detail = ServiceDetail {
        about,
        interfaces,
        functions: vec![echo_fn],
    };
    map.insert("edgex".to_string(), detail);
    Arc::new(RwLock::new(map))
}

pub fn create_default_js_udfs() -> Arc<RwLock<HashMap<String, JavascriptUdf>>> {
    let mut map = HashMap::new();
    let func1 = JavascriptUdf {
        id: "func1".to_string(),
        description: "Default echo JavaScript function".to_string(),
        script: "function func1(x) { return x; }".to_string(),
        is_agg: false,
    };
    let _ = compile_and_register_js_udf(&func1);
    map.insert("func1".to_string(), func1);
    Arc::new(RwLock::new(map))
}

async fn list_services(State(state): State<AppState>) -> impl IntoResponse {
    let services = state.services.read();
    let mut names: Vec<String> = services.keys().cloned().collect();
    names.sort();
    Json(names)
}

async fn create_service(State(state): State<AppState>, Json(body): Json<Value>) -> Response {
    let name = body
        .get("name")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .trim()
        .to_string();
    if let Err(resp) = check_valid_name(&name) {
        return resp;
    }

    if let Some(resp) = reject_missing_plugin_file(&body) {
        return resp;
    }

    if state.services.read().contains_key(&name) {
        return (
            StatusCode::BAD_REQUEST,
            format!("Service '{}' already exists", name),
        )
            .into_response();
    }

    let mut about = HashMap::new();
    if let Some(ab) = body.get("About").and_then(|v| v.as_object()) {
        for (k, v) in ab {
            about.insert(k.clone(), v.clone());
        }
    } else if let Some(file) = body.get("file").and_then(|v| v.as_str()) {
        about.insert("file".to_string(), json!(file));
    }

    let mut interfaces = HashMap::new();
    if let Some(ifaces) = body.get("Interfaces").and_then(|v| v.as_object()) {
        for (k, v) in ifaces {
            interfaces.insert(k.clone(), v.clone());
        }
    }

    let mut functions = Vec::new();
    if let Some(funcs) = body.get("functions").and_then(|v| v.as_array()) {
        for f in funcs {
            if let Ok(func) = serde_json::from_value::<ExternalFunction>(f.clone()) {
                functions.push(func);
            }
        }
    }

    if functions.is_empty() {
        for (iface_name, iface_val) in &interfaces {
            if let Some(methods) = iface_val.get("methods").and_then(|m| m.as_array()) {
                for m in methods {
                    if let Some(m_str) = m.as_str() {
                        functions.push(ExternalFunction {
                            service_name: name.clone(),
                            interface_name: iface_name.clone(),
                            addr: iface_val
                                .get("addr")
                                .and_then(|a| a.as_str())
                                .unwrap_or("")
                                .to_string(),
                            method_name: m_str.to_string(),
                            func_name: m_str.to_string(),
                        });
                    }
                }
            }
        }
    }

    let detail = ServiceDetail {
        about,
        interfaces,
        functions,
    };
    state.services.write().insert(name.clone(), detail);
    (
        StatusCode::CREATED,
        format!("Service '{}' registered", name),
    )
        .into_response()
}

async fn get_service(State(state): State<AppState>, Path(name): Path<String>) -> Response {
    if let Err(resp) = check_valid_name(&name) {
        return resp;
    }
    let services = state.services.read();
    if let Some(service) = services.get(&name) {
        Json(service.clone()).into_response()
    } else {
        (
            StatusCode::NOT_FOUND,
            format!("Service '{}' not found", name),
        )
            .into_response()
    }
}

async fn update_service(
    State(state): State<AppState>,
    Path(name): Path<String>,
    Json(body): Json<Value>,
) -> Response {
    if let Err(resp) = check_valid_name(&name) {
        return resp;
    }
    let mut services = state.services.write();
    let entry = match services.get_mut(&name) {
        Some(s) => s,
        None => {
            return (
                StatusCode::NOT_FOUND,
                format!("Service '{}' not found", name),
            )
                .into_response();
        }
    };

    if let Some(ab) = body.get("About").and_then(|v| v.as_object()) {
        for (k, v) in ab {
            entry.about.insert(k.clone(), v.clone());
        }
    }
    if let Some(ifaces) = body.get("Interfaces").and_then(|v| v.as_object()) {
        for (k, v) in ifaces {
            entry.interfaces.insert(k.clone(), v.clone());
        }
    }
    if let Some(funcs) = body.get("functions").and_then(|v| v.as_array()) {
        let mut new_funcs = Vec::new();
        for f in funcs {
            if let Ok(func) = serde_json::from_value::<ExternalFunction>(f.clone()) {
                new_funcs.push(func);
            }
        }
        if !new_funcs.is_empty() {
            entry.functions = new_funcs;
        }
    }

    (StatusCode::OK, "Service updated").into_response()
}

async fn delete_service(State(state): State<AppState>, Path(name): Path<String>) -> Response {
    if let Err(resp) = check_valid_name(&name) {
        return resp;
    }
    if state.services.write().remove(&name).is_some() {
        (StatusCode::OK, format!("Service '{}' deleted", name)).into_response()
    } else {
        (
            StatusCode::NOT_FOUND,
            format!("Service '{}' not found", name),
        )
            .into_response()
    }
}

async fn list_service_functions(State(state): State<AppState>) -> impl IntoResponse {
    let services = state.services.read();
    let mut list = Vec::new();
    for svc in services.values() {
        for f in &svc.functions {
            list.push(f.clone());
        }
    }
    list.sort_by(|a, b| a.func_name.cmp(&b.func_name));
    Json(list)
}

async fn get_service_function(State(state): State<AppState>, Path(name): Path<String>) -> Response {
    if let Err(resp) = check_valid_name(&name) {
        return resp;
    }
    let services = state.services.read();
    for svc in services.values() {
        for f in &svc.functions {
            if f.func_name.eq_ignore_ascii_case(&name) {
                return Json(f.clone()).into_response();
            }
        }
    }
    (
        StatusCode::NOT_FOUND,
        format!("External function '{}' not found", name),
    )
        .into_response()
}

async fn list_javascript_udfs(State(state): State<AppState>) -> impl IntoResponse {
    let udfs = state.js_udfs.read();
    let mut ids: Vec<String> = udfs.keys().cloned().collect();
    ids.sort();
    Json(ids)
}

async fn create_javascript_udf(
    State(state): State<AppState>,
    Json(udf): Json<JavascriptUdf>,
) -> Response {
    if let Err(resp) = check_valid_name(&udf.id) {
        return resp;
    }
    if state.js_udfs.read().contains_key(&udf.id) {
        return (
            StatusCode::BAD_REQUEST,
            format!("JavaScript UDF '{}' already exists", udf.id),
        )
            .into_response();
    }
    if let Err(e) = compile_and_register_js_udf(&udf) {
        return (StatusCode::BAD_REQUEST, e).into_response();
    }
    state.js_udfs.write().insert(udf.id.clone(), udf.clone());
    (
        StatusCode::CREATED,
        format!("JavaScript UDF '{}' created", udf.id),
    )
        .into_response()
}

async fn get_javascript_udf(State(state): State<AppState>, Path(id): Path<String>) -> Response {
    if let Err(resp) = check_valid_name(&id) {
        return resp;
    }
    let udfs = state.js_udfs.read();
    if let Some(udf) = udfs.get(&id) {
        Json(udf.clone()).into_response()
    } else {
        (
            StatusCode::NOT_FOUND,
            format!("JavaScript UDF '{}' not found", id),
        )
            .into_response()
    }
}

async fn update_javascript_udf(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(body): Json<Value>,
) -> Response {
    if let Err(resp) = check_valid_name(&id) {
        return resp;
    }
    let mut udf = state
        .js_udfs
        .read()
        .get(&id)
        .cloned()
        .unwrap_or_else(|| JavascriptUdf {
            id: id.clone(),
            description: String::new(),
            script: format!("function {}(x) {{ return x; }}", id),
            is_agg: false,
        });

    if let Some(desc) = body.get("description").and_then(|v| v.as_str()) {
        udf.description = desc.to_string();
    }
    if let Some(sc) = body.get("script").and_then(|v| v.as_str()) {
        if !sc.trim().is_empty() {
            udf.script = sc.to_string();
        }
    }
    if let Some(agg) = body.get("isAgg").and_then(|v| v.as_bool()) {
        udf.is_agg = agg;
    }
    udf.id = id.clone();

    if let Err(e) = compile_and_register_js_udf(&udf) {
        return (StatusCode::BAD_REQUEST, e).into_response();
    }
    state.js_udfs.write().insert(id, udf);
    (StatusCode::OK, "JavaScript UDF updated").into_response()
}

async fn delete_javascript_udf(State(state): State<AppState>, Path(id): Path<String>) -> Response {
    if let Err(resp) = check_valid_name(&id) {
        return resp;
    }
    if state.js_udfs.write().remove(&id).is_some() {
        rekuiper_core::plugin::get_global_udf_registry().unregister_udf(&id);
        (StatusCode::OK, "JavaScript UDF deleted").into_response()
    } else {
        (
            StatusCode::NOT_FOUND,
            format!("JavaScript UDF '{}' not found", id),
        )
            .into_response()
    }
}

// ---------------------------------------------------------------------------
// Schema registry (`/schemas/:kind[/:name]`).
// ---------------------------------------------------------------------------

/// Payload for registering or updating a schema; the kind always comes from
/// the URL path.
#[derive(Debug, serde::Deserialize)]
struct SchemaPayload {
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    content: Option<String>,
    #[serde(default)]
    file: Option<String>,
}

async fn list_schemas(State(state): State<AppState>, Path(kind): Path<String>) -> Response {
    if let Err(resp) = check_valid_name(&kind) {
        return resp;
    }
    Json(state.schema_manager.list_schemas(&kind)).into_response()
}

async fn create_schema(
    State(state): State<AppState>,
    Path(kind): Path<String>,
    Json(payload): Json<SchemaPayload>,
) -> Response {
    if let Err(resp) = check_valid_name(&kind) {
        return resp;
    }
    let Some(name) = payload.name.filter(|n| !n.is_empty()) else {
        return (StatusCode::BAD_REQUEST, "Missing schema name").into_response();
    };
    if let Err(resp) = check_valid_name(&name) {
        return resp;
    }
    let def = SchemaDefinition {
        name,
        kind,
        content: payload.content,
        file: payload.file,
    };
    let created = def.name.clone();
    let _ = state.schema_manager.register_schema(def).await;
    (
        StatusCode::CREATED,
        format!("Schema {} is created.\n", created),
    )
        .into_response()
}

async fn get_schema(
    State(state): State<AppState>,
    Path((kind, name)): Path<(String, String)>,
    headers: axum::http::HeaderMap,
) -> Response {
    if let Err(resp) = check_valid_name(&kind) {
        return resp;
    }
    if let Err(resp) = check_valid_name(&name) {
        return resp;
    }
    let Some(def) = state.schema_manager.get_schema(&kind, &name) else {
        return (
            StatusCode::NOT_FOUND,
            format!("Schema {}/{} not found", kind, name),
        )
            .into_response();
    };
    let wants_text = headers
        .get(axum::http::header::ACCEPT)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.contains("text/plain"));
    if wants_text {
        (
            StatusCode::OK,
            [(
                axum::http::header::CONTENT_TYPE,
                "text/plain; charset=utf-8",
            )],
            def.content.unwrap_or_default(),
        )
            .into_response()
    } else {
        Json(def).into_response()
    }
}

async fn update_schema(
    State(state): State<AppState>,
    Path((kind, name)): Path<(String, String)>,
    Json(payload): Json<SchemaPayload>,
) -> Response {
    if let Err(resp) = check_valid_name(&kind) {
        return resp;
    }
    if let Err(resp) = check_valid_name(&name) {
        return resp;
    }
    // Upsert: merge over any existing definition so partial bodies work.
    let mut def = state
        .schema_manager
        .get_schema(&kind, &name)
        .unwrap_or(SchemaDefinition {
            name: name.clone(),
            kind: kind.clone(),
            content: None,
            file: None,
        });
    if payload.content.is_some() {
        def.content = payload.content;
    }
    if payload.file.is_some() {
        def.file = payload.file;
    }
    let _ = state.schema_manager.register_schema(def).await;
    (
        StatusCode::OK,
        format!("Schema {}/{} is updated.\n", kind, name),
    )
        .into_response()
}

/// Extract one file part from a `multipart/form-data` body without extra
/// dependencies: splits on the boundary and returns the bytes after the
/// first part's blank header line. Returns `None` when the shape is not a
/// recognizable single-file upload.
fn extract_multipart_file(body: &[u8], boundary: &str) -> Option<Vec<u8>> {
    if boundary.is_empty() {
        return None;
    }
    let sep = format!("--{}", boundary);
    let text = std::str::from_utf8(body).ok()?;
    for raw in text.split(&sep) {
        // Skip the preamble and the closing `--` epilogue, which carry no
        // headers and must not abort the scan.
        let mut splitter = raw.splitn(2, "\r\n\r\n");
        let headers = splitter.next().unwrap_or("");
        let Some(content) = splitter.next() else {
            continue;
        };
        if headers.to_ascii_lowercase().contains("filename=") {
            let content = content.strip_suffix("\r\n").unwrap_or(content);
            return Some(content.as_bytes().to_vec());
        }
    }
    None
}

/// Schema file upload (baseline `PUT /schemas/:type/:name/upload`): accepts
/// `multipart/form-data` file parts as well as raw body bytes, stores the
/// content, and answers `{"type","name"}`. An empty body registers an empty
/// shell so metadata-only flows keep working.
async fn upload_schema(
    State(state): State<AppState>,
    Path((kind, name)): Path<(String, String)>,
    headers: axum::http::HeaderMap,
    body: Bytes,
) -> Response {
    if let Err(resp) = check_valid_name(&kind) {
        return resp;
    }
    if let Err(resp) = check_valid_name(&name) {
        return resp;
    }
    let content = if body.is_empty() {
        String::new()
    } else if let Some(content_type) = headers
        .get(axum::http::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
    {
        if content_type.starts_with("multipart/form-data") {
            let boundary = content_type
                .split("boundary=")
                .nth(1)
                .unwrap_or("")
                .trim()
                .trim_matches('"')
                .to_string();
            match extract_multipart_file(&body, &boundary) {
                Some(bytes) => String::from_utf8_lossy(&bytes).into_owned(),
                None => {
                    return (StatusCode::BAD_REQUEST, "Invalid multipart upload").into_response();
                }
            }
        } else {
            String::from_utf8_lossy(&body).into_owned()
        }
    } else {
        String::from_utf8_lossy(&body).into_owned()
    };
    let _ = state
        .schema_manager
        .register_schema(SchemaDefinition {
            name: name.clone(),
            kind: kind.clone(),
            content: Some(content),
            file: None,
        })
        .await;
    (StatusCode::OK, Json(json!({ "type": kind, "name": name }))).into_response()
}

async fn delete_schema(
    State(state): State<AppState>,
    Path((kind, name)): Path<(String, String)>,
) -> Response {
    if let Err(resp) = check_valid_name(&kind) {
        return resp;
    }
    if let Err(resp) = check_valid_name(&name) {
        return resp;
    }
    // Idempotent like the other drop endpoints: missing schemas still 200.
    let _ = state.schema_manager.delete_schema(&kind, &name).await;
    (
        StatusCode::OK,
        format!("Schema {}/{} is dropped.\n", kind, name),
    )
        .into_response()
}

// ---------------------------------------------------------------------------
// Plugin registry (`/plugins/sources`, `/plugins/sinks`, `/plugins/functions`, `/plugins/udfs`).
// ---------------------------------------------------------------------------

const BUILTIN_SOURCES: &[&str] = &[
    "edgex",
    "file",
    "http",
    "httppull",
    "httppush",
    "kafka",
    "memory",
    "mqtt",
    "neuron",
    "redis",
    "redisSub",
    "simulator",
    "sql",
    "websocket",
];

const BUILTIN_SINKS: &[&str] = &[
    "edgex",
    "file",
    "http",
    "kafka",
    "log",
    "memory",
    "mqtt",
    "neuron",
    "nop",
    "redis",
    "redisPub",
    "rest",
    "websocket",
];

fn is_builtin_source(name: &str) -> bool {
    BUILTIN_SOURCES
        .iter()
        .any(|&s| s.eq_ignore_ascii_case(name))
}

fn is_builtin_sink(name: &str) -> bool {
    BUILTIN_SINKS.iter().any(|&s| s.eq_ignore_ascii_case(name))
}

async fn list_source_plugins(State(state): State<AppState>) -> impl IntoResponse {
    Json(state.plugin_manager.list_plugins("source"))
}

async fn create_source_plugin(
    State(state): State<AppState>,
    Json(payload): Json<Value>,
) -> Response {
    create_plugin_of_type(&state, "source", payload).await
}

async fn get_source_plugin(State(state): State<AppState>, Path(name): Path<String>) -> Response {
    get_typed_plugin(&state, "source", &name)
}

async fn update_source_plugin(
    State(state): State<AppState>,
    Path(name): Path<String>,
    body: Bytes,
) -> Response {
    update_typed_plugin(&state, "source", &name, body).await
}

async fn delete_source_plugin(State(state): State<AppState>, Path(name): Path<String>) -> Response {
    delete_typed_plugin(&state, &name).await
}

async fn list_sink_plugins(State(state): State<AppState>) -> impl IntoResponse {
    Json(state.plugin_manager.list_plugins("sink"))
}

async fn create_sink_plugin(State(state): State<AppState>, Json(payload): Json<Value>) -> Response {
    create_plugin_of_type(&state, "sink", payload).await
}

async fn get_sink_plugin(State(state): State<AppState>, Path(name): Path<String>) -> Response {
    get_typed_plugin(&state, "sink", &name)
}

async fn update_sink_plugin(
    State(state): State<AppState>,
    Path(name): Path<String>,
    body: Bytes,
) -> Response {
    update_typed_plugin(&state, "sink", &name, body).await
}

async fn delete_sink_plugin(State(state): State<AppState>, Path(name): Path<String>) -> Response {
    delete_typed_plugin(&state, &name).await
}

async fn list_function_plugins(State(state): State<AppState>) -> impl IntoResponse {
    Json(state.plugin_manager.list_plugins("function"))
}

async fn create_function_plugin(
    State(state): State<AppState>,
    Json(payload): Json<Value>,
) -> Response {
    create_plugin_of_type(&state, "function", payload).await
}

async fn get_function_plugin(State(state): State<AppState>, Path(name): Path<String>) -> Response {
    get_typed_plugin(&state, "function", &name)
}

async fn update_function_plugin(
    State(state): State<AppState>,
    Path(name): Path<String>,
    body: Bytes,
) -> Response {
    update_typed_plugin(&state, "function", &name, body).await
}

async fn delete_function_plugin(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> Response {
    delete_typed_plugin(&state, &name).await
}

async fn register_function_plugin(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> Response {
    if let Err(resp) = check_valid_name(&name) {
        return resp;
    }
    if state.plugin_manager.get_plugin(&name).is_none() {
        return (
            StatusCode::NOT_FOUND,
            Json(json!({
                "error": 1000,
                "message": format!("plugin {} is not found", name)
            })),
        )
            .into_response();
    }
    (StatusCode::OK, format!("Plugin {} is registered.\n", name)).into_response()
}

async fn list_prebuild_plugins() -> impl IntoResponse {
    Json(json!({}))
}

async fn list_udf_plugins(State(state): State<AppState>) -> impl IntoResponse {
    Json(state.plugin_manager.list_plugins("udf"))
}

async fn create_udf_plugin(State(state): State<AppState>, Json(payload): Json<Value>) -> Response {
    create_plugin_of_type(&state, "udf", payload).await
}

async fn get_udf_plugin(State(state): State<AppState>, Path(name): Path<String>) -> Response {
    get_typed_plugin(&state, "udf", &name)
}

async fn delete_udf_plugin(State(state): State<AppState>, Path(name): Path<String>) -> Response {
    delete_typed_plugin(&state, &name).await
}

async fn list_portable_plugins(State(state): State<AppState>) -> impl IntoResponse {
    let mut names: Vec<String> = state.portable_plugins.read().keys().cloned().collect();
    names.sort();
    Json(names)
}

async fn create_portable_plugin(
    State(state): State<AppState>,
    Json(payload): Json<Value>,
) -> Response {
    let name = match payload.get("name").and_then(|v| v.as_str()) {
        Some(n) => n.to_string(),
        None => return (StatusCode::BAD_REQUEST, "missing name").into_response(),
    };
    if let Err(resp) = check_valid_name(&name) {
        return resp;
    }

    let info: PortablePluginInfo = match serde_json::from_value(payload.clone()) {
        Ok(info) => info,
        Err(_) => {
            let file = payload
                .get("file")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
            PortablePluginInfo {
                name: name.clone(),
                version: "1.0.0".to_string(),
                language: "python".to_string(),
                executable: file.unwrap_or_else(|| format!("{}.py", name)),
                virtual_env_type: None,
                env: None,
                sources: Vec::new(),
                sinks: Vec::new(),
                functions: vec![name.clone()],
            }
        }
    };

    let status = PortablePluginStatus {
        ref_count: HashMap::new(),
        status: "running".to_string(),
        err_msg: "".to_string(),
    };

    let _ = state
        .plugin_manager
        .register_plugin(PluginDefinition {
            name: name.clone(),
            plugin_type: "portable".to_string(),
            file: Some(info.executable.clone()),
            description: Some(format!("Portable plugin in {}", info.language)),
            functions: info.functions.clone(),
        })
        .await;

    state
        .portable_plugins
        .write()
        .insert(name.clone(), (info, status));
    (
        StatusCode::CREATED,
        format!("Plugin {} is created.\n", name),
    )
        .into_response()
}

async fn get_portable_plugin(State(state): State<AppState>, Path(name): Path<String>) -> Response {
    if let Err(resp) = check_valid_name(&name) {
        return resp;
    }
    if let Some((info, _)) = state.portable_plugins.read().get(&name) {
        Json(info.clone()).into_response()
    } else {
        (StatusCode::NOT_FOUND, format!("Plugin {} not found", name)).into_response()
    }
}

async fn update_portable_plugin(
    State(state): State<AppState>,
    Path(name): Path<String>,
    body: Bytes,
) -> Response {
    if let Err(resp) = check_valid_name(name.as_str()) {
        return resp;
    }
    if !state.portable_plugins.read().contains_key(&name)
        && state.plugin_manager.get_plugin(&name).is_none()
    {
        return (
            StatusCode::NOT_FOUND,
            Json(json!({
                "error": 1000,
                "message": format!("plugin {} is not found", name)
            })),
        )
            .into_response();
    }
    let mut plugins = state.portable_plugins.write();
    if let Some((info, _)) = plugins.get_mut(&name) {
        if !body.is_empty() {
            if let Ok(val) = serde_json::from_slice::<Value>(&body) {
                if let Some(ver) = val.get("version").and_then(|v| v.as_str()) {
                    info.version = ver.to_string();
                }
                if let Some(exec) = val.get("executable").and_then(|v| v.as_str()) {
                    info.executable = exec.to_string();
                }
                if let Some(desc) = val.get("description").and_then(|v| v.as_str()) {
                    info.env = Some(desc.to_string());
                }
            }
        }
    } else {
        plugins.insert(
            name.clone(),
            (
                PortablePluginInfo {
                    name: name.clone(),
                    version: "1.0.0".to_string(),
                    language: "python".to_string(),
                    executable: format!("{}.py", name),
                    virtual_env_type: None,
                    env: None,
                    sources: Vec::new(),
                    sinks: Vec::new(),
                    functions: vec![name.clone()],
                },
                PortablePluginStatus {
                    ref_count: HashMap::new(),
                    status: "running".to_string(),
                    err_msg: "".to_string(),
                },
            ),
        );
    }
    (StatusCode::OK, format!("Plugin {} is updated.\n", name)).into_response()
}

async fn delete_portable_plugin(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> Response {
    if let Err(resp) = check_valid_name(&name) {
        return resp;
    }
    if !state.portable_plugins.read().contains_key(&name)
        && state.plugin_manager.get_plugin(&name).is_none()
    {
        return (
            StatusCode::NOT_FOUND,
            Json(json!({
                "error": 1000,
                "message": format!(
                    "fail to delete plugin {}: plugin {} is not found",
                    name, name
                )
            })),
        )
            .into_response();
    }
    let _ = state.plugin_manager.delete_plugin(&name).await;
    state.portable_plugins.write().remove(&name);
    (StatusCode::OK, format!("Plugin {} is dropped.\n", name)).into_response()
}

async fn get_portable_plugin_status(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> Response {
    if let Err(resp) = check_valid_name(&name) {
        return resp;
    }
    if let Some((_, status)) = state.portable_plugins.read().get(&name) {
        Json(status.clone()).into_response()
    } else {
        (StatusCode::NOT_FOUND, format!("Plugin {} not found", name)).into_response()
    }
}

fn typed_plugin_payload(plugin_type: &str, mut payload: Value) -> Result<PluginDefinition, String> {
    if let Some(obj) = payload.as_object_mut() {
        obj.insert(
            "plugin_type".to_string(),
            Value::String(plugin_type.to_string()),
        );
    }
    serde_json::from_value::<PluginDefinition>(payload)
        .map_err(|e| format!("Invalid plugin definition: {}", e))
}

/// Reject plugin/service payloads pointing at unreadable local files
/// (baseline: `fail to download file ...: no such file or directory`).
/// Only `file://` URIs and plain local paths are verifiable here; remote
/// URLs pass through untouched.
fn reject_missing_plugin_file(payload: &Value) -> Option<Response> {
    let file_uri = payload.get("file").and_then(|v| v.as_str()).unwrap_or("");
    if file_uri.is_empty() {
        return None;
    }
    let is_local = file_uri.starts_with("file://") || !file_uri.contains("://");
    if !is_local {
        return None;
    }
    let raw = file_uri.strip_prefix("file://").unwrap_or(file_uri);
    // Windows drive URIs arrive as file:///C:/... — drop the leading slash
    // so the path resolves; POSIX absolutes (/tmp/...) pass through.
    let raw_bytes = raw.as_bytes();
    let path = if raw_bytes.len() >= 3
        && raw_bytes[0] == b'/'
        && raw_bytes[1].is_ascii_alphabetic()
        && raw_bytes[2] == b':'
    {
        &raw[1..]
    } else {
        raw
    };
    if !std::path::Path::new(path).exists() {
        return Some(
            (
                StatusCode::BAD_REQUEST,
                Json(json!({
                    "error": 1000,
                    "message": format!(
                        "fail to download file {}: stat {}: no such file or directory",
                        file_uri,
                        path
                    )
                })),
            )
                .into_response(),
        );
    }
    None
}

async fn create_plugin_of_type(state: &AppState, plugin_type: &str, payload: Value) -> Response {
    let name = payload
        .get("name")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    if let Err(resp) = check_valid_name(&name) {
        return resp;
    }
    if let Some(resp) = reject_missing_plugin_file(&payload) {
        return resp;
    }
    match typed_plugin_payload(plugin_type, payload) {
        Ok(def) => {
            let created = def.name.clone();
            if let Err(e) = state.plugin_manager.register_plugin(def).await {
                return (StatusCode::BAD_REQUEST, e.to_string()).into_response();
            }
            (
                StatusCode::CREATED,
                format!("Plugin {} is created.\n", created),
            )
                .into_response()
        }
        Err(e) => (StatusCode::BAD_REQUEST, e).into_response(),
    }
}

async fn update_typed_plugin(
    state: &AppState,
    plugin_type: &str,
    name: &str,
    body: Bytes,
) -> Response {
    if let Err(resp) = check_valid_name(name) {
        return resp;
    }
    let Some(mut def) = state.plugin_manager.get_plugin(name) else {
        return (
            StatusCode::NOT_FOUND,
            Json(json!({
                "error": 1000,
                "message": format!("plugin {} is not found", name)
            })),
        )
            .into_response();
    };
    def.plugin_type = plugin_type.to_string();
    if !body.is_empty() {
        if let Ok(val) = serde_json::from_slice::<Value>(&body) {
            if let Some(file_str) = val.get("file").and_then(|v| v.as_str()) {
                def.file = Some(file_str.to_string());
            }
            if let Some(desc_str) = val.get("description").and_then(|v| v.as_str()) {
                def.description = Some(desc_str.to_string());
            }
            if let Some(funcs) = val.get("functions").and_then(|v| v.as_array()) {
                def.functions = funcs
                    .iter()
                    .filter_map(|f| f.as_str().map(|s| s.to_string()))
                    .collect();
            }
        }
    }
    if let Err(e) = state.plugin_manager.register_plugin(def).await {
        return (StatusCode::BAD_REQUEST, e.to_string()).into_response();
    }
    (StatusCode::OK, format!("Plugin {} is updated.\n", name)).into_response()
}

fn get_typed_plugin(state: &AppState, plugin_type: &str, name: &str) -> Response {
    if let Err(resp) = check_valid_name(name) {
        return resp;
    }
    match state.plugin_manager.get_plugin(name) {
        Some(def) if def.plugin_type == plugin_type => Json(def).into_response(),
        Some(_) => (
            StatusCode::NOT_FOUND,
            format!("Plugin {} is not a {}", name, plugin_type),
        )
            .into_response(),
        None => {
            if (plugin_type == "source" && is_builtin_source(name))
                || (plugin_type == "sink" && is_builtin_sink(name))
            {
                Json(PluginDefinition {
                    name: name.to_string(),
                    plugin_type: plugin_type.to_string(),
                    file: None,
                    description: Some(format!("Built-in {} plugin", plugin_type)),
                    functions: Vec::new(),
                })
                .into_response()
            } else {
                (StatusCode::NOT_FOUND, format!("Plugin {} not found", name)).into_response()
            }
        }
    }
}

async fn delete_typed_plugin(state: &AppState, name: &str) -> Response {
    if let Err(resp) = check_valid_name(name) {
        return resp;
    }
    if state.plugin_manager.get_plugin(name).is_none() {
        return (
            StatusCode::NOT_FOUND,
            Json(json!({
                "error": 1000,
                "message": format!(
                    "fail to delete plugin {}: plugin {} is not found",
                    name, name
                )
            })),
        )
            .into_response();
    }
    let _ = state.plugin_manager.delete_plugin(name).await;
    (StatusCode::OK, format!("Plugin {} is dropped.\n", name)).into_response()
}

async fn get_rule_schema(State(state): State<AppState>, Path(id): Path<String>) -> Response {
    if let Err(resp) = check_valid_name(&id) {
        return resp;
    }
    let Some(rule) = state.rule_manager.get_rule(&id) else {
        return (StatusCode::NOT_FOUND, format!("Rule {} not found", id)).into_response();
    };
    let mut parser = Parser::new(&rule.sql);
    match parser.parse_select() {
        // Graph rules carry no SELECT SQL: report an empty schema.
        Err(_) => (StatusCode::OK, Json(json!({}))).into_response(),
        Ok(stmt) => (
            StatusCode::OK,
            Json(Value::Object(Evaluator::infer_select_schema(&stmt))),
        )
            .into_response(),
    }
}

async fn async_data_import(State(state): State<AppState>, body: Bytes) -> Response {
    let payload: Value = if !body.is_empty() {
        if let Ok(v) = serde_json::from_slice::<Value>(&body) {
            if let Some(content_str) = v.get("content").and_then(|c| c.as_str()) {
                serde_json::from_str::<Value>(content_str).unwrap_or(v)
            } else {
                v
            }
        } else {
            Value::Null
        }
    } else {
        Value::Null
    };

    let task_id = format!("dataImport-{}", uuid::Uuid::new_v4().simple());
    let cancel_rx = state.task_manager.register_task(task_id.clone());

    let task_state = state.clone();
    let tid = task_id.clone();
    tokio::spawn(async move {
        // Yield briefly to simulate realistic background ingestion
        tokio::time::sleep(std::time::Duration::from_millis(80)).await;
        if *cancel_rx.borrow() {
            task_state
                .task_manager
                .update_status(&tid, "cancelled", "task cancelled");
            return;
        }
        process_import_payload(&task_state, &payload).await;
        if *cancel_rx.borrow() {
            task_state
                .task_manager
                .update_status(&tid, "cancelled", "task cancelled");
        } else {
            task_state
                .task_manager
                .update_status(&tid, "completed", "import completed");
        }
    });

    (
        StatusCode::OK,
        Json(json!({
            "id": task_id,
            "task_id": task_id,
            "status": "running"
        })),
    )
        .into_response()
}

async fn async_task_status(State(state): State<AppState>, Path(id): Path<String>) -> Response {
    if let Err(resp) = check_valid_name(&id) {
        return resp;
    }
    if let Some(task) = state.task_manager.get_task(&id) {
        (
            StatusCode::OK,
            Json(json!({
                "id": task.id,
                "task_id": task.id,
                "status": task.status,
                "message": task.message,
                "createdTimestamp": task.created_timestamp,
                "updatedTimestamp": task.updated_timestamp
            })),
        )
            .into_response()
    } else {
        (StatusCode::NOT_FOUND, format!("Task {} not found", id)).into_response()
    }
}

async fn async_task_cancelled(State(state): State<AppState>, Path(id): Path<String>) -> Response {
    if let Err(resp) = check_valid_name(&id) {
        return resp;
    }
    state.task_manager.cancel_task(&id);
    (
        StatusCode::OK,
        Json(json!({
            "id": id,
            "task_id": id,
            "status": "cancelled",
            "message": "task cancelled"
        })),
    )
        .into_response()
}

fn find_etc_file(relative: &str) -> Option<std::path::PathBuf> {
    let mut candidates = vec![
        std::path::PathBuf::from("etc").join(relative),
        std::path::PathBuf::from("../etc").join(relative),
        std::path::PathBuf::from("../../etc").join(relative),
    ];
    if let Ok(manifest_dir) = std::env::var("CARGO_MANIFEST_DIR") {
        candidates.push(
            std::path::Path::new(&manifest_dir)
                .join("etc")
                .join(relative),
        );
        candidates.push(
            std::path::Path::new(&manifest_dir)
                .join("..")
                .join("etc")
                .join(relative),
        );
        candidates.push(
            std::path::Path::new(&manifest_dir)
                .join("..")
                .join("..")
                .join("etc")
                .join(relative),
        );
    }
    candidates.into_iter().find(|p| p.is_file())
}

async fn list_source_metadata() -> impl IntoResponse {
    Json(named_entries(&[
        "edgex",
        "file",
        "http",
        "httppull",
        "httppush",
        "kafka",
        "memory",
        "mqtt",
        "neuron",
        "redis",
        "redisSub",
        "simulator",
        "sql",
        "websocket",
    ]))
}

async fn list_sink_metadata() -> impl IntoResponse {
    Json(named_entries(&[
        "edgex",
        "file",
        "http",
        "kafka",
        "log",
        "memory",
        "mqtt",
        "neuron",
        "nop",
        "redis",
        "redisPub",
        "rest",
        "websocket",
    ]))
}

async fn list_function_metadata(State(state): State<AppState>) -> impl IntoResponse {
    let mut list: Vec<Value> = Vec::new();
    let mut seen: HashSet<String> = HashSet::new();
    for meta in builtin_function_metadata() {
        seen.insert(meta.name.to_string());
        list.push(json!({
            "name": meta.name,
            "category": meta.category,
            "description": meta.description,
            "aggregate": meta.aggregate,
            "arity": meta.arity,
            "example": meta.example,
        }));
    }
    // Registered plugin/UDF functions extend the catalog; built-ins win on
    // name collisions. The registry stores no arity, so it is reported
    // honestly as unknown.
    let plugins = state
        .plugin_manager
        .list_plugins("function")
        .into_iter()
        .chain(state.plugin_manager.list_plugins("udf"));
    for plugin in plugins {
        let category = if plugin.plugin_type == "udf" {
            "udf"
        } else {
            "plugin"
        };
        let functions = if plugin.functions.is_empty() {
            vec![plugin.name.clone()]
        } else {
            plugin.functions.clone()
        };
        let description = plugin
            .description
            .clone()
            .unwrap_or_else(|| format!("Function provided by the {} plugin.", plugin.name));
        for func in functions {
            if seen.insert(func.clone()) {
                list.push(json!({
                    "name": func,
                    "category": category,
                    "description": description,
                    "aggregate": false,
                    "arity": "unknown",
                    "example": format!("{}()", func),
                }));
            }
        }
    }

    let js_udfs = state.js_udfs.read();
    for (id, udf) in js_udfs.iter() {
        if seen.insert(id.clone()) {
            list.push(json!({
                "name": id,
                "category": "udf",
                "description": if udf.description.is_empty() { format!("JavaScript UDF {}", id) } else { udf.description.clone() },
                "aggregate": udf.is_agg,
                "arity": "unknown",
                "example": format!("{}()", id),
            }));
        }
    }

    let services = state.services.read();
    for svc in services.values() {
        for f in &svc.functions {
            if seen.insert(f.func_name.clone()) {
                list.push(json!({
                    "name": f.func_name,
                    "category": "service",
                    "description": format!("External service function provided by {}", f.service_name),
                    "aggregate": false,
                    "arity": "unknown",
                    "example": format!("{}()", f.func_name),
                }));
            }
        }
    }

    Json(list)
}

async fn list_operator_metadata() -> impl IntoResponse {
    Json(named_entries(&[
        "+", "-", "*", "/", "=", "!=", "<", ">", "AND", "OR", "NOT", "BETWEEN", "IN",
    ]))
}

async fn list_metadata_connections(State(state): State<AppState>) -> impl IntoResponse {
    let conns: Vec<Value> = state.connections.read().values().cloned().collect();
    Json(conns)
}

async fn list_metadata_resources(State(state): State<AppState>) -> impl IntoResponse {
    let mut resources: Vec<Value> = Vec::new();
    for (id, conn) in state.connections.read().iter() {
        resources.push(json!({
            "id": id,
            "resource": id,
            "type": conn.get("type").and_then(|v| v.as_str()).unwrap_or("connection"),
            "status": "active",
        }));
    }
    Json(resources)
}

async fn get_connection_metadata(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> Response {
    if let Err(resp) = check_valid_name(&name) {
        return resp;
    }
    if let Some(conn) = state.connections.read().get(&name).cloned() {
        return Json(conn).into_response();
    }
    if let Some(path) = find_etc_file(&format!("connections/{}.json", name)) {
        if let Ok(content) = std::fs::read_to_string(&path) {
            if let Ok(val) = serde_json::from_str::<Value>(&content) {
                return Json(val).into_response();
            }
        }
    }
    Json(json!({
        "id": name,
        "name": name,
        "about": {
            "description": format!("Connection configuration for {}", name)
        }
    }))
    .into_response()
}

fn mask_secrets(value: &mut Value) {
    match value {
        Value::Object(map) => {
            for (k, v) in map.iter_mut() {
                let lower = k.to_lowercase();
                if lower.contains("password") || lower.contains("token") {
                    *v = Value::String("******".to_string());
                } else {
                    mask_secrets(v);
                }
            }
        }
        Value::Array(arr) => {
            for v in arr.iter_mut() {
                mask_secrets(v);
            }
        }
        _ => {}
    }
}

async fn get_connection_yaml(State(state): State<AppState>, Path(name): Path<String>) -> Response {
    if let Err(resp) = check_valid_name(&name) {
        return resp;
    }
    let found = find_etc_file(&format!("connections/{}.yaml", name))
        .or_else(|| find_etc_file("connections/connection.yaml"));

    let mut result_map: serde_json::Map<String, Value> = serde_json::Map::new();
    let mut raw_content = String::new();

    if let Some(path) = found {
        if let Ok(content) = std::fs::read_to_string(&path) {
            raw_content = content.clone();
            if let Ok(yaml_val) = serde_yaml::from_str::<Value>(&content) {
                if let Some(obj) = yaml_val.as_object() {
                    for (k, v) in obj {
                        result_map.insert(k.clone(), v.clone());
                    }
                }
            }
        }
    }

    let prefix = format!("{}/", name);
    let configs = state.connections.read();
    for (k, v) in configs.iter() {
        if let Some(conf_key) = k.strip_prefix(&prefix) {
            result_map.insert(conf_key.to_string(), v.clone());
        }
    }

    if result_map.is_empty() {
        if name == "mqtt" {
            result_map.insert(
                "default".to_string(),
                json!({
                    "server": "tcp://127.0.0.1:1883",
                    "protocolVersion": "3.1.1"
                }),
            );
            raw_content =
                "default:\n  server: \"tcp://127.0.0.1:1883\"\n  protocolVersion: \"3.1.1\"\n"
                    .to_string();
        } else {
            return (
                StatusCode::NOT_FOUND,
                format!("connection {} not found\n", name),
            )
                .into_response();
        }
    }

    if raw_content.is_empty() {
        raw_content = serde_yaml::to_string(&result_map).unwrap_or_default();
    }
    result_map.insert("yaml".to_string(), json!(raw_content));

    let mut final_val = Value::Object(result_map);
    mask_secrets(&mut final_val);
    Json(final_val).into_response()
}

async fn get_source_metadata(Path(name): Path<String>) -> Response {
    if let Err(resp) = check_valid_name(&name) {
        return resp;
    }
    let found = if name == "mqtt" {
        find_etc_file("mqtt_source.json").or_else(|| find_etc_file("sources/mqtt.json"))
    } else if name == "http" {
        find_etc_file("sources/http.json").or_else(|| find_etc_file("sources/httppull.json"))
    } else {
        find_etc_file(&format!("sources/{}.json", name))
    };

    if let Some(path) = found {
        if let Ok(content) = std::fs::read_to_string(&path) {
            if let Ok(json_val) = serde_json::from_str::<Value>(&content) {
                return Json(json_val).into_response();
            }
        }
    }
    (
        StatusCode::NOT_FOUND,
        format!("source {} not found\n", name),
    )
        .into_response()
}

async fn get_sink_metadata(Path(name): Path<String>) -> Response {
    if let Err(resp) = check_valid_name(&name) {
        return resp;
    }
    let found = if name == "mqtt" {
        find_etc_file("sinks/mqtt.json")
    } else if name == "http" {
        find_etc_file("sinks/rest.json").or_else(|| find_etc_file("sinks/http.json"))
    } else {
        find_etc_file(&format!("sinks/{}.json", name))
    };

    if let Some(path) = found {
        if let Ok(content) = std::fs::read_to_string(&path) {
            if let Ok(json_val) = serde_json::from_str::<Value>(&content) {
                return Json(json_val).into_response();
            }
        }
    }
    (StatusCode::NOT_FOUND, format!("sink {} not found\n", name)).into_response()
}

async fn get_source_yaml(State(state): State<AppState>, Path(name): Path<String>) -> Response {
    if let Err(resp) = check_valid_name(&name) {
        return resp;
    }
    let found = if name == "mqtt" {
        find_etc_file("mqtt_source.yaml").or_else(|| find_etc_file("sources/mqtt.yaml"))
    } else if name == "http" {
        find_etc_file("sources/httppull.yaml").or_else(|| find_etc_file("sources/http.yaml"))
    } else {
        find_etc_file(&format!("sources/{}.yaml", name))
    };

    let mut result_map: serde_json::Map<String, Value> = serde_json::Map::new();
    let mut raw_content = String::new();

    if let Some(path) = found {
        if let Ok(content) = std::fs::read_to_string(&path) {
            raw_content = content.clone();
            if let Ok(yaml_val) = serde_yaml::from_str::<Value>(&content) {
                if let Some(obj) = yaml_val.as_object() {
                    for (k, v) in obj {
                        result_map.insert(k.clone(), v.clone());
                    }
                }
            }
        }
    }

    let prefix = format!("{}/", name);
    let configs = state.source_configs.read();
    for (k, v) in configs.iter() {
        if let Some(conf_key) = k.strip_prefix(&prefix) {
            result_map.insert(conf_key.to_string(), v.clone());
        }
    }

    if result_map.is_empty() {
        if name == "mqtt" {
            result_map.insert(
                "default".to_string(),
                json!({
                    "server": "tcp://127.0.0.1:1883",
                    "protocolVersion": "3.1.1"
                }),
            );
            raw_content =
                "default:\n  server: \"tcp://127.0.0.1:1883\"\n  protocolVersion: \"3.1.1\"\n"
                    .to_string();
        } else {
            return (
                StatusCode::NOT_FOUND,
                format!("source {} not found\n", name),
            )
                .into_response();
        }
    }

    if raw_content.is_empty() {
        raw_content = serde_yaml::to_string(&result_map).unwrap_or_default();
    }
    result_map.insert("yaml".to_string(), json!(raw_content));

    let mut final_val = Value::Object(result_map);
    mask_secrets(&mut final_val);
    Json(final_val).into_response()
}

async fn get_sink_yaml(State(state): State<AppState>, Path(name): Path<String>) -> Response {
    if let Err(resp) = check_valid_name(&name) {
        return resp;
    }
    let found = if name == "mqtt" {
        find_etc_file("sinks/mqtt.yaml").or_else(|| find_etc_file("mqtt_sink.yaml"))
    } else if name == "http" {
        find_etc_file("sinks/rest.yaml").or_else(|| find_etc_file("sinks/http.yaml"))
    } else {
        find_etc_file(&format!("sinks/{}.yaml", name))
    };

    let mut result_map: serde_json::Map<String, Value> = serde_json::Map::new();
    let mut raw_content = String::new();

    if let Some(path) = found {
        if let Ok(content) = std::fs::read_to_string(&path) {
            raw_content = content.clone();
            if let Ok(yaml_val) = serde_yaml::from_str::<Value>(&content) {
                if let Some(obj) = yaml_val.as_object() {
                    for (k, v) in obj {
                        result_map.insert(k.clone(), v.clone());
                    }
                }
            }
        }
    }

    let prefix = format!("{}/", name);
    let configs = state.sink_configs.read();
    for (k, v) in configs.iter() {
        if let Some(conf_key) = k.strip_prefix(&prefix) {
            result_map.insert(conf_key.to_string(), v.clone());
        }
    }

    if result_map.is_empty() {
        if name == "mqtt" {
            result_map.insert(
                "default".to_string(),
                json!({
                    "server": "tcp://127.0.0.1:1883",
                    "protocolVersion": "3.1.1"
                }),
            );
            raw_content =
                "default:\n  server: \"tcp://127.0.0.1:1883\"\n  protocolVersion: \"3.1.1\"\n"
                    .to_string();
        } else {
            return (StatusCode::NOT_FOUND, format!("sink {} not found\n", name)).into_response();
        }
    }

    if raw_content.is_empty() {
        raw_content = serde_yaml::to_string(&result_map).unwrap_or_default();
    }
    result_map.insert("yaml".to_string(), json!(raw_content));

    let mut final_val = Value::Object(result_map);
    mask_secrets(&mut final_val);
    Json(final_val).into_response()
}

/// Stores a source configuration under `<name>/<conf_key>` for later lookup
/// by simulator (and other file-based) sources.
async fn save_source_conf_key(
    State(state): State<AppState>,
    Path((name, conf_key)): Path<(String, String)>,
    body: Bytes,
) -> Response {
    if let Err(resp) = check_valid_name(&name) {
        return resp;
    }
    if let Err(resp) = check_valid_name(&conf_key) {
        return resp;
    }
    let payload = if body.is_empty() {
        json!({})
    } else {
        serde_json::from_slice::<Value>(&body).unwrap_or_else(|_| json!({}))
    };
    state
        .source_configs
        .write()
        .insert(format!("{}/{}", name, conf_key), payload.clone());
    persist_config_entry(
        &state,
        "source_configs",
        &format!("{}/{}", name, conf_key),
        &payload,
    )
    .await;
    (StatusCode::OK, Json(json!({"message": "success"}))).into_response()
}

async fn get_source_conf_key(
    State(state): State<AppState>,
    Path((name, conf_key)): Path<(String, String)>,
) -> Response {
    if let Err(resp) = check_valid_name(&name) {
        return resp;
    }
    if let Err(resp) = check_valid_name(&conf_key) {
        return resp;
    }
    let key = format!("{}/{}", name, conf_key);
    if let Some(val) = state.source_configs.read().get(&key).cloned() {
        Json(val).into_response()
    } else {
        (
            StatusCode::NOT_FOUND,
            format!("Configuration key {} for {} not found", conf_key, name),
        )
            .into_response()
    }
}

async fn delete_source_conf_key(
    State(state): State<AppState>,
    Path((name, conf_key)): Path<(String, String)>,
) -> Response {
    if let Err(resp) = check_valid_name(&name) {
        return resp;
    }
    if let Err(resp) = check_valid_name(&conf_key) {
        return resp;
    }
    state
        .source_configs
        .write()
        .remove(&format!("{}/{}", name, conf_key));
    unpersist_config_entry(&state, "source_configs", &format!("{}/{}", name, conf_key)).await;
    (StatusCode::OK, Json(json!({"message": "success"}))).into_response()
}

async fn save_sink_conf_key(
    State(state): State<AppState>,
    Path((name, conf_key)): Path<(String, String)>,
    body: Bytes,
) -> Response {
    if let Err(resp) = check_valid_name(&name) {
        return resp;
    }
    if let Err(resp) = check_valid_name(&conf_key) {
        return resp;
    }
    let payload = if body.is_empty() {
        json!({})
    } else {
        serde_json::from_slice::<Value>(&body).unwrap_or_else(|_| json!({}))
    };
    state
        .sink_configs
        .write()
        .insert(format!("{}/{}", name, conf_key), payload.clone());
    persist_config_entry(
        &state,
        "sink_configs",
        &format!("{}/{}", name, conf_key),
        &payload,
    )
    .await;
    (StatusCode::OK, Json(json!({"message": "success"}))).into_response()
}

async fn get_sink_conf_key(
    State(state): State<AppState>,
    Path((name, conf_key)): Path<(String, String)>,
) -> Response {
    if let Err(resp) = check_valid_name(&name) {
        return resp;
    }
    if let Err(resp) = check_valid_name(&conf_key) {
        return resp;
    }
    let key = format!("{}/{}", name, conf_key);
    if let Some(val) = state.sink_configs.read().get(&key).cloned() {
        Json(val).into_response()
    } else {
        (
            StatusCode::NOT_FOUND,
            format!("Configuration key {} for {} not found", conf_key, name),
        )
            .into_response()
    }
}

async fn delete_sink_conf_key(
    State(state): State<AppState>,
    Path((name, conf_key)): Path<(String, String)>,
) -> Response {
    if let Err(resp) = check_valid_name(&name) {
        return resp;
    }
    if let Err(resp) = check_valid_name(&conf_key) {
        return resp;
    }
    state
        .sink_configs
        .write()
        .remove(&format!("{}/{}", name, conf_key));
    unpersist_config_entry(&state, "sink_configs", &format!("{}/{}", name, conf_key)).await;
    (StatusCode::OK, Json(json!({"message": "success"}))).into_response()
}

async fn save_connection_conf_key(
    State(state): State<AppState>,
    Path((name, conf_key)): Path<(String, String)>,
    body: Bytes,
) -> Response {
    if let Err(resp) = check_valid_name(&name) {
        return resp;
    }
    if let Err(resp) = check_valid_name(&conf_key) {
        return resp;
    }
    let payload = if body.is_empty() {
        json!({})
    } else {
        serde_json::from_slice::<Value>(&body).unwrap_or_else(|_| json!({}))
    };
    state
        .connections
        .write()
        .insert(format!("{}.{}", name, conf_key), payload.clone());
    state
        .connections
        .write()
        .insert(format!("{}/{}", name, conf_key), payload.clone());
    persist_config_entry(
        &state,
        "connections",
        &format!("{}.{}", name, conf_key),
        &payload,
    )
    .await;
    persist_config_entry(
        &state,
        "connections",
        &format!("{}/{}", name, conf_key),
        &payload,
    )
    .await;
    (StatusCode::OK, Json(json!({"message": "success"}))).into_response()
}

async fn get_connection_conf_key(
    State(state): State<AppState>,
    Path((name, conf_key)): Path<(String, String)>,
) -> Response {
    if let Err(resp) = check_valid_name(&name) {
        return resp;
    }
    if let Err(resp) = check_valid_name(&conf_key) {
        return resp;
    }
    let conns = state.connections.read();
    if let Some(val) = conns
        .get(&format!("{}.{}", name, conf_key))
        .or_else(|| conns.get(&format!("{}/{}", name, conf_key)))
        .cloned()
    {
        Json(val).into_response()
    } else {
        (
            StatusCode::NOT_FOUND,
            format!("Connection conf_key {} for {} not found", conf_key, name),
        )
            .into_response()
    }
}

async fn delete_connection_conf_key(
    State(state): State<AppState>,
    Path((name, conf_key)): Path<(String, String)>,
) -> Response {
    if let Err(resp) = check_valid_name(&name) {
        return resp;
    }
    if let Err(resp) = check_valid_name(&conf_key) {
        return resp;
    }
    state
        .connections
        .write()
        .remove(&format!("{}.{}", name, conf_key));
    state
        .connections
        .write()
        .remove(&format!("{}/{}", name, conf_key));
    unpersist_config_entry(&state, "connections", &format!("{}.{}", name, conf_key)).await;
    unpersist_config_entry(&state, "connections", &format!("{}/{}", name, conf_key)).await;
    (StatusCode::OK, Json(json!({"message": "success"}))).into_response()
}

async fn register_source_connection(
    State(state): State<AppState>,
    Path(name): Path<String>,
    body: Bytes,
) -> Response {
    if let Err(resp) = check_valid_name(&name) {
        return resp;
    }
    let payload = if body.is_empty() {
        json!({ "id": name })
    } else {
        serde_json::from_slice::<Value>(&body).unwrap_or_else(|_| json!({ "id": name }))
    };
    let id = payload
        .get("id")
        .and_then(|v| v.as_str())
        .unwrap_or(&name)
        .to_string();
    state.connections.write().insert(id, payload);
    (StatusCode::OK, Json(json!({"message": "success"}))).into_response()
}

async fn register_sink_connection(
    State(state): State<AppState>,
    Path(name): Path<String>,
    body: Bytes,
) -> Response {
    if let Err(resp) = check_valid_name(&name) {
        return resp;
    }
    let payload = if body.is_empty() {
        json!({ "id": name })
    } else {
        serde_json::from_slice::<Value>(&body).unwrap_or_else(|_| json!({ "id": name }))
    };
    let id = payload
        .get("id")
        .and_then(|v| v.as_str())
        .unwrap_or(&name)
        .to_string();
    state.connections.write().insert(id, payload);
    (StatusCode::OK, Json(json!({"message": "success"}))).into_response()
}

async fn register_lookup_connection(
    State(state): State<AppState>,
    Path(name): Path<String>,
    body: Bytes,
) -> Response {
    if let Err(resp) = check_valid_name(&name) {
        return resp;
    }
    let payload = if body.is_empty() {
        json!({ "id": name })
    } else {
        serde_json::from_slice::<Value>(&body).unwrap_or_else(|_| json!({ "id": name }))
    };
    let id = payload
        .get("id")
        .and_then(|v| v.as_str())
        .unwrap_or(&name)
        .to_string();
    state.connections.write().insert(id, payload);
    (StatusCode::OK, Json(json!({"message": "success"}))).into_response()
}

async fn list_connections(State(state): State<AppState>) -> impl IntoResponse {
    let conns: Vec<Value> = state.connections.read().values().cloned().collect();
    Json(conns)
}

async fn create_connection(State(state): State<AppState>, Json(payload): Json<Value>) -> Response {
    let id = payload
        .get("id")
        .and_then(|v| v.as_str())
        .or_else(|| payload.get("name").and_then(|v| v.as_str()))
        .unwrap_or("")
        .to_string();
    if id.is_empty() {
        return (StatusCode::BAD_REQUEST, "Missing connection id").into_response();
    }
    state.connections.write().insert(id.clone(), payload);
    (
        StatusCode::CREATED,
        format!("Connection {} is created.\n", id),
    )
        .into_response()
}

async fn get_connection(State(state): State<AppState>, Path(id): Path<String>) -> Response {
    if let Some(conn) = state.connections.read().get(&id).cloned() {
        Json(conn).into_response()
    } else {
        (
            StatusCode::NOT_FOUND,
            format!("Connection {} not found", id),
        )
            .into_response()
    }
}

async fn delete_connection(State(state): State<AppState>, Path(id): Path<String>) -> Response {
    if state.connections.write().remove(&id).is_some() {
        (StatusCode::OK, format!("Connection {} is dropped.\n", id)).into_response()
    } else {
        (
            StatusCode::NOT_FOUND,
            format!("Connection {} not found", id),
        )
            .into_response()
    }
}

/// Update-or-insert connection properties (eKuiper `PUT /connections/:id`):
/// merges the JSON body into the stored entry and returns it for readback.
async fn update_connection(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(payload): Json<Value>,
) -> Response {
    let mut stored = state
        .connections
        .read()
        .get(&id)
        .cloned()
        .unwrap_or_else(|| json!({"id": id}));
    merge_json_object(&mut stored, &payload);
    if let Some(obj) = stored.as_object_mut() {
        obj.insert("id".to_string(), Value::String(id.clone()));
    }
    state.connections.write().insert(id, stored.clone());
    Json(stored).into_response()
}

async fn bulk_start_rules(State(state): State<AppState>, body: Bytes) -> impl IntoResponse {
    let target_tags = if !body.is_empty() {
        if let Ok(val) = serde_json::from_slice::<Value>(&body) {
            extract_tags_from_value(&val)
        } else {
            Vec::new()
        }
    } else {
        Vec::new()
    };
    let mut results = Vec::new();
    for rule in state.rule_manager.list_rules() {
        if !target_tags.is_empty() && !target_tags.iter().any(|t| rule.tags.contains(t)) {
            continue;
        }
        match state.rule_manager.start_rule(&rule.id).await {
            Ok(_) => results.push(json!({ "ruleId": rule.id, "success": true })),
            Err(e) => {
                results.push(json!({ "ruleId": rule.id, "success": false, "error": e.to_string() }))
            }
        }
    }
    (StatusCode::OK, Json(results))
}

async fn bulk_stop_rules(State(state): State<AppState>, body: Bytes) -> impl IntoResponse {
    let target_tags = if !body.is_empty() {
        if let Ok(val) = serde_json::from_slice::<Value>(&body) {
            extract_tags_from_value(&val)
        } else {
            Vec::new()
        }
    } else {
        Vec::new()
    };
    let mut results = Vec::new();
    for rule in state.rule_manager.list_rules() {
        if !target_tags.is_empty() && !target_tags.iter().any(|t| rule.tags.contains(t)) {
            continue;
        }
        match state.rule_manager.stop_rule(&rule.id).await {
            Ok(_) => {
                cancel_rule_source(&state, &rule.id);
                results.push(json!({ "ruleId": rule.id, "success": true }));
            }
            Err(e) => {
                results.push(json!({ "ruleId": rule.id, "success": false, "error": e.to_string() }))
            }
        }
    }
    (StatusCode::OK, Json(results))
}

async fn reset_rule_state(State(state): State<AppState>, Path(name): Path<String>) -> Response {
    if let Err(resp) = check_valid_name(&name) {
        return resp;
    }
    match state.rule_manager.reset_rule_metrics(&name) {
        Ok(_) => (StatusCode::OK, "success\n").into_response(),
        Err(e) => (StatusCode::NOT_FOUND, e.to_string()).into_response(),
    }
}

/// Current process CPU percent and RSS bytes via sysinfo, falling back to
/// global CPU and used memory when the process handle is unavailable.
fn current_process_stats() -> (f64, u64) {
    let mut sys = System::new_all();
    sys.refresh_all();
    if let Some(p) = sysinfo::get_current_pid()
        .ok()
        .and_then(|id| sys.process(id))
    {
        (p.cpu_usage() as f64, p.memory())
    } else {
        (sys.global_cpu_info().cpu_usage() as f64, sys.used_memory())
    }
}

async fn get_rule_cpu(State(state): State<AppState>, Path(id): Path<String>) -> Response {
    if let Err(resp) = check_valid_name(&id) {
        return resp;
    }
    if state.rule_manager.get_rule(&id).is_none() {
        return (StatusCode::NOT_FOUND, format!("Rule {} not found", id)).into_response();
    }
    let (cpu, memory) = current_process_stats();
    Json(json!({
        "rule_id": id,
        "cpu": cpu,
        "cpu_percent": cpu,
        "memory": memory,
        "memory_bytes": memory,
    }))
    .into_response()
}

async fn rule_cpu_usage(State(state): State<AppState>) -> impl IntoResponse {
    let (cpu, _) = current_process_stats();
    let mut map = serde_json::Map::new();
    for rule in state.rule_manager.list_rules() {
        // Per-rule CPU accounting is unavailable: running rules share the
        // process measurement, stopped rules report zero.
        let usage = if is_rule_running(&state.rule_manager, &rule.id) {
            cpu
        } else {
            0.0
        };
        map.insert(rule.id, json!(usage));
    }
    Json(Value::Object(map))
}

#[derive(Deserialize, Default)]
struct TagMatchQuery {
    tags: Option<String>,
    keys: Option<String>,
}

fn extract_tags_from_value(val: &Value) -> Vec<String> {
    if let Some(arr) = val.get("tags").and_then(|t| t.as_array()) {
        return arr
            .iter()
            .filter_map(|s| s.as_str().map(|s| s.to_string()))
            .collect();
    }
    if let Some(arr) = val.get("keys").and_then(|t| t.as_array()) {
        return arr
            .iter()
            .filter_map(|s| s.as_str().map(|s| s.to_string()))
            .collect();
    }
    if let Some(arr) = val.as_array() {
        return arr
            .iter()
            .filter_map(|s| s.as_str().map(|s| s.to_string()))
            .collect();
    }
    Vec::new()
}

async fn put_rule_tags(
    State(state): State<AppState>,
    Path(name): Path<String>,
    body: Bytes,
) -> Response {
    if let Err(resp) = check_valid_name(&name) {
        return resp;
    }
    let val: Value = serde_json::from_slice(&body).unwrap_or(Value::Null);
    let new_tags = extract_tags_from_value(&val);
    match state
        .rule_manager
        .update_rule_tags(&name, |tags| {
            *tags = new_tags;
        })
        .await
    {
        Ok(_) => (StatusCode::OK, Json(json!({"message": "success"}))).into_response(),
        Err(e) => (StatusCode::NOT_FOUND, e.to_string()).into_response(),
    }
}

async fn patch_rule_tags(
    State(state): State<AppState>,
    Path(name): Path<String>,
    body: Bytes,
) -> Response {
    if let Err(resp) = check_valid_name(&name) {
        return resp;
    }
    let val: Value = serde_json::from_slice(&body).unwrap_or(Value::Null);
    let add_tags = extract_tags_from_value(&val);
    match state
        .rule_manager
        .update_rule_tags(&name, |tags| {
            for t in add_tags {
                if !tags.contains(&t) {
                    tags.push(t);
                }
            }
        })
        .await
    {
        Ok(_) => (StatusCode::OK, Json(json!({"message": "success"}))).into_response(),
        Err(e) => (StatusCode::NOT_FOUND, e.to_string()).into_response(),
    }
}

async fn delete_rule_tags(
    State(state): State<AppState>,
    Path(name): Path<String>,
    body: Bytes,
) -> Response {
    if let Err(resp) = check_valid_name(&name) {
        return resp;
    }
    let val: Value = serde_json::from_slice(&body).unwrap_or(Value::Null);
    let remove_tags = extract_tags_from_value(&val);
    match state
        .rule_manager
        .update_rule_tags(&name, |tags| {
            if remove_tags.is_empty() {
                tags.clear();
            } else {
                tags.retain(|t| !remove_tags.contains(t));
            }
        })
        .await
    {
        Ok(_) => (StatusCode::OK, Json(json!({"message": "success"}))).into_response(),
        Err(e) => (StatusCode::NOT_FOUND, e.to_string()).into_response(),
    }
}

async fn rule_tags_match(
    State(state): State<AppState>,
    axum::extract::Query(query): axum::extract::Query<TagMatchQuery>,
    body: Bytes,
) -> Response {
    let mut search_tags: HashSet<String> = HashSet::new();
    if let Some(t_str) = query.tags.or(query.keys) {
        for t in t_str.split(',') {
            let trimmed = t.trim();
            if !trimmed.is_empty() {
                search_tags.insert(trimmed.to_string());
            }
        }
    }
    if search_tags.is_empty() && !body.is_empty() {
        if let Ok(val) = serde_json::from_slice::<Value>(&body) {
            for t in extract_tags_from_value(&val) {
                search_tags.insert(t);
            }
        }
    }

    let mut matched: Vec<String> = Vec::new();
    for rule in state.rule_manager.list_rules() {
        if search_tags.is_empty() {
            continue;
        }
        if search_tags.iter().all(|t| rule.tags.contains(t)) {
            matched.push(rule.id.clone());
        }
    }
    matched.sort();
    Json(matched).into_response()
}

#[derive(Deserialize, Default)]
struct TraceStartBody {
    strategy: Option<String>,
}

async fn start_rule_trace(
    State(state): State<AppState>,
    Path(name): Path<String>,
    body: Bytes,
) -> Response {
    if let Err(resp) = check_valid_name(&name) {
        return resp;
    }
    if state.rule_manager.get_rule(&name).is_none() {
        return (StatusCode::NOT_FOUND, format!("Rule {} not found", name)).into_response();
    }
    let strategy = if !body.is_empty() {
        serde_json::from_slice::<TraceStartBody>(&body)
            .ok()
            .and_then(|b| b.strategy)
            .unwrap_or_else(|| "always".to_string())
    } else {
        "always".to_string()
    };
    state.trace_manager.start_trace(&name, strategy);
    (StatusCode::OK, Json(json!({"message": "success"}))).into_response()
}

async fn stop_rule_trace(State(state): State<AppState>, Path(name): Path<String>) -> Response {
    if let Err(resp) = check_valid_name(&name) {
        return resp;
    }
    if state.rule_manager.get_rule(&name).is_none() {
        return (StatusCode::NOT_FOUND, format!("Rule {} not found", name)).into_response();
    }
    state.trace_manager.stop_trace(&name);
    (StatusCode::OK, Json(json!({"message": "success"}))).into_response()
}

#[derive(Deserialize, Default)]
struct TraceQuery {
    limit: Option<usize>,
}

async fn get_rule_traces(
    State(state): State<AppState>,
    Path(rule_id): Path<String>,
    axum::extract::Query(query): axum::extract::Query<TraceQuery>,
) -> Response {
    if let Err(resp) = check_valid_name(&rule_id) {
        return resp;
    }
    let ids = state
        .trace_manager
        .list_rule_trace_ids(&rule_id, query.limit);
    (StatusCode::OK, Json(ids)).into_response()
}

async fn get_trace_by_id(State(state): State<AppState>, Path(id): Path<String>) -> Response {
    if let Err(resp) = check_valid_name(&id) {
        return resp;
    }
    if let Some(span) = state.trace_manager.get_trace(&id) {
        (StatusCode::OK, Json(span)).into_response()
    } else {
        (StatusCode::NOT_FOUND, format!("trace {} not found\n", id)).into_response()
    }
}

async fn set_tracer_config(State(state): State<AppState>, body: Bytes) -> Response {
    if !body.is_empty() {
        if let Ok(cfg) = serde_json::from_slice::<TracerConfig>(&body) {
            state.trace_manager.set_tracer_config(cfg);
        }
    }
    (StatusCode::OK, Json(json!({"message": "success"}))).into_response()
}

async fn get_config_uploads() -> impl IntoResponse {
    let upload_dir = std::path::PathBuf::from("data").join("uploads");
    let mut files = Vec::new();
    if let Ok(entries) = std::fs::read_dir(&upload_dir) {
        for entry in entries.flatten() {
            if let Ok(file_type) = entry.file_type() {
                if file_type.is_file() {
                    let path = entry.path();
                    let abs_path = std::fs::canonicalize(&path)
                        .unwrap_or(path)
                        .to_string_lossy()
                        .to_string();
                    files.push(abs_path);
                }
            }
        }
    }
    files.sort();
    (StatusCode::OK, Json(files))
}

async fn upload_config_file(State(state): State<AppState>, body: Bytes) -> Response {
    let upload_dir = std::path::PathBuf::from("data").join("uploads");
    if let Err(e) = std::fs::create_dir_all(&upload_dir) {
        return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response();
    }

    #[derive(Deserialize)]
    struct UploadReq {
        name: Option<String>,
        content: Option<String>,
        file: Option<String>,
    }

    if let Ok(req) = serde_json::from_slice::<UploadReq>(&body) {
        let name = match req.name {
            Some(n) if !n.trim().is_empty() => n,
            _ => return (StatusCode::BAD_REQUEST, "missing file name").into_response(),
        };
        if let Err(resp) = check_valid_name(&name) {
            return resp;
        }

        let bytes_to_write = if let Some(content) = req.content {
            content.into_bytes()
        } else if let Some(file_url) = req.file {
            match state.http_client.get(&file_url).send().await {
                Ok(res) => match res.bytes().await {
                    Ok(b) => b.to_vec(),
                    Err(e) => {
                        return (
                            StatusCode::BAD_REQUEST,
                            format!("Failed to read file from URL: {}", e),
                        )
                            .into_response();
                    }
                },
                Err(e) => {
                    return (
                        StatusCode::BAD_REQUEST,
                        format!("Failed to fetch file URL: {}", e),
                    )
                        .into_response();
                }
            }
        } else {
            return (StatusCode::BAD_REQUEST, "Missing content or file URL").into_response();
        };

        let file_path = upload_dir.join(&name);
        if let Err(e) = std::fs::write(&file_path, bytes_to_write) {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to write file: {}", e),
            )
                .into_response();
        }
        let abs_path = std::fs::canonicalize(&file_path)
            .unwrap_or(file_path)
            .to_string_lossy()
            .to_string();
        return (StatusCode::CREATED, abs_path).into_response();
    }

    (StatusCode::BAD_REQUEST, "invalid upload request body").into_response()
}

async fn delete_config_upload(Path(name): Path<String>) -> Response {
    if let Err(resp) = check_valid_name(&name) {
        return resp;
    }
    let upload_dir = std::path::PathBuf::from("data").join("uploads");
    let file_path = upload_dir.join(&name);
    if file_path.exists() {
        let _ = std::fs::remove_file(&file_path);
    }
    (StatusCode::OK, "ok\n").into_response()
}

async fn stop_server() -> impl IntoResponse {
    // Exit after a short grace delay so the HTTP 200 flushes to the client
    // before the process terminates (baseline eKuiper exits with code 0).
    tokio::spawn(async {
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        std::process::exit(0);
    });
    (StatusCode::OK, "Server is shutting down\n")
}

async fn import_status(State(state): State<AppState>) -> impl IntoResponse {
    Json(state.latest_import_status.read().clone())
}

async fn metrics_dump(State(state): State<AppState>) -> impl IntoResponse {
    let (cpu, memory) = current_process_stats();
    let mut sys = System::new_all();
    sys.refresh_all();
    Json(json!({
        "metrics": {
            "cpu": cpu,
            "cpu_usage": cpu,
            "memory": memory,
            "memory_bytes": memory,
            "total_memory": sys.total_memory(),
            "used_memory": sys.used_memory(),
            "uptime_seconds": state.start_time.elapsed().as_secs(),
        },
        "cpu": cpu,
        "memory": memory,
    }))
}

/// Prometheus text exposition of rule metrics (eKuiper monitor endpoint).
///
/// Reports per-rule status/counters plus running/stopped rule counts in the
/// standard exposition format with `# HELP` / `# TYPE` comments.
pub async fn prometheus_metrics_handler(State(state): State<AppState>) -> impl IntoResponse {
    let mut running: u64 = 0;
    let mut stopped: u64 = 0;
    let mut out = String::new();

    out.push_str("# HELP kuiper_rule_count gauge of rule status count\n");
    out.push_str("# TYPE kuiper_rule_count gauge\n");

    let mut rule_lines = String::new();
    rule_lines.push_str("# HELP kuiper_rule_status gauge of rule status\n");
    rule_lines.push_str("# TYPE kuiper_rule_status gauge\n");

    let mut sink_in = String::new();
    sink_in.push_str("# HELP kuiper_sink_records_in_total total number of messages read in\n");
    sink_in.push_str("# TYPE kuiper_sink_records_in_total counter\n");

    let mut sink_out = String::new();
    sink_out.push_str("# HELP kuiper_sink_records_out_total total number of messages output\n");
    sink_out.push_str("# TYPE kuiper_sink_records_out_total counter\n");

    let mut sink_err = String::new();
    sink_err.push_str("# HELP kuiper_sink_exceptions_total total number of exceptions\n");
    sink_err.push_str("# TYPE kuiper_sink_exceptions_total counter\n");

    let mut latency = String::new();
    latency.push_str(
        "# HELP kuiper_sink_process_latency_us latency of most recent processing in microseconds\n",
    );
    latency.push_str("# TYPE kuiper_sink_process_latency_us gauge\n");

    let mut source_in = String::new();
    source_in.push_str("# HELP kuiper_source_records_in_total total number of messages read in\n");
    source_in.push_str("# TYPE kuiper_source_records_in_total counter\n");

    let mut source_out = String::new();
    source_out.push_str("# HELP kuiper_source_records_out_total total number of messages output\n");
    source_out.push_str("# TYPE kuiper_source_records_out_total counter\n");

    for rule in state.rule_manager.list_rules() {
        let Some(status) = state.rule_manager.get_rule_status(&rule.id) else {
            continue;
        };
        match status.status.as_str() {
            "running" => running += 1,
            _ => stopped += 1,
        }
        // Status: 1 running, 0 paused/stopped, -1 abnormal exit.
        let code = match status.status.as_str() {
            "running" => 1,
            "stopped" => 0,
            _ => -1,
        };
        // This engine tracks no per-record latency yet; export 0.
        let latency_us: u64 = 0;
        rule_lines.push_str(&format!(
            "kuiper_rule_status{{rule=\"{}\"}} {}\n",
            rule.id, code
        ));
        sink_in.push_str(&format!(
            "kuiper_sink_records_in_total{{rule=\"{}\"}} {}\n",
            rule.id, status.source_records_in_total
        ));
        sink_out.push_str(&format!(
            "kuiper_sink_records_out_total{{rule=\"{}\"}} {}\n",
            rule.id, status.sink_records_out_total
        ));
        sink_err.push_str(&format!(
            "kuiper_sink_exceptions_total{{rule=\"{}\"}} {}\n",
            rule.id, status.exceptions_total
        ));
        latency.push_str(&format!(
            "kuiper_sink_process_latency_us{{rule=\"{}\"}} {}\n",
            rule.id, latency_us
        ));
        source_in.push_str(&format!(
            "kuiper_source_records_in_total{{rule=\"{}\"}} {}\n",
            rule.id, status.source_records_in_total
        ));
        source_out.push_str(&format!(
            "kuiper_source_records_out_total{{rule=\"{}\"}} {}\n",
            rule.id, status.source_records_in_total
        ));
    }

    out.push_str(&format!(
        "kuiper_rule_count{{status=\"running\"}} {}\n",
        running
    ));
    out.push_str(&format!(
        "kuiper_rule_count{{status=\"stop\"}} {}\n",
        stopped
    ));
    out.push_str(&rule_lines);
    out.push_str(&sink_in);
    out.push_str(&sink_out);
    out.push_str(&sink_err);
    out.push_str(&latency);
    out.push_str(&source_in);
    out.push_str(&source_out);

    (
        StatusCode::OK,
        [(
            axum::http::header::CONTENT_TYPE,
            "text/plain; version=0.0.4; charset=utf-8",
        )],
        out,
    )
}

// ---------------------------------------------------------------------------
// Interactive rule simulation (ruletest) with SSE streaming output.
// ---------------------------------------------------------------------------

/// Payload for `POST /ruletest`. All fields are optional so that probes
/// without a body still receive a usable session.
#[derive(Debug, Default, Deserialize)]
struct CreateRuletestPayload {
    #[serde(default)]
    id: Option<String>,
    #[serde(default)]
    sql: Option<String>,
    #[serde(default, rename = "mockSource")]
    mock_source: HashMap<String, SimulatorConfig>,
}

fn generate_ruletest_id() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    format!("ruletest-{}-{}", std::process::id(), nanos)
}

async fn create_ruletest(State(state): State<AppState>, body: Bytes) -> Response {
    let payload: CreateRuletestPayload = if body.is_empty() {
        CreateRuletestPayload::default()
    } else {
        match serde_json::from_slice(&body) {
            Ok(p) => p,
            Err(e) => {
                return (
                    StatusCode::BAD_REQUEST,
                    format!("Invalid ruletest payload: {}", e),
                )
                    .into_response();
            }
        }
    };
    // A simulation without a parseable SELECT statement is rejected, like
    // baseline eKuiper.
    let sql = payload.sql.clone().unwrap_or_default();
    if sql.trim().is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({
                "error": 1000,
                "message": "fail to run rule: SQL is not a select statement."
            })),
        )
            .into_response();
    }
    if Parser::new(&sql).parse_select().is_err() {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({
                "error": 1000,
                "message": "fail to run rule: SQL is not a select statement."
            })),
        )
            .into_response();
    }
    // The SSE feed serves on the documented `httpServerPort` (default
    // 10081): a shared listener is bound at daemon startup (see
    // `test_sse_router`), so the reported port is stable and live from the
    // moment the session is created.
    let port = state.config.read().basic.http_server_port;
    let id = payload.id.unwrap_or_else(generate_ruletest_id);
    let (output_tx, _) = tokio::sync::broadcast::channel::<String>(256);
    let shutdown = Arc::new(tokio::sync::Notify::new());
    state.ruletests.write().insert(
        id.clone(),
        RuletestSession {
            id: id.clone(),
            sql,
            mock_source: payload.mock_source,
            output_tx,
            replay: Arc::new(RwLock::new(RuletestReplay::default())),
            port,
            shutdown,
        },
    );
    (StatusCode::OK, Json(json!({ "id": id, "port": port }))).into_response()
}

async fn start_ruletest(State(state): State<AppState>, Path(name): Path<String>) -> Response {
    let Some(session) = state.ruletests.read().get(&name).cloned() else {
        return (
            StatusCode::NOT_FOUND,
            Json(json!({"error": 1000, "message": format!("test rule {} not found", name)})),
        )
            .into_response();
    };
    // Paced replay of the mock source: rows stream at the configured
    // interval, looping until the session is deleted (or a 10-minute
    // session cap, mirroring baseline trial-run expiry). Every row is
    // buffered in session history so late SSE subscribers lose nothing.
    tokio::spawn(async move {
        let mut parser = Parser::new(&session.sql);
        let Ok(select_stmt) = parser.parse_select() else {
            return;
        };
        let conf = session.mock_source.get(&select_stmt.from).cloned();
        let data: Vec<HashMap<String, Value>> =
            conf.as_ref().map(|c| c.data.clone()).unwrap_or_default();
        if data.is_empty() {
            // No mock rows: fall back to a single empty trigger row so the
            // rule still evaluates once (baseline runs the real source).
            let rule_state = RuleState::default();
            for row in
                Evaluator::eval_select_stateful_multi(&select_stmt, &HashMap::new(), &rule_state)
            {
                emit_ruletest_line(&session, &row);
            }
            return;
        }
        let interval = conf
            .as_ref()
            .map(|c| parse_interval_ms(&c.interval))
            .unwrap_or_else(|| std::time::Duration::from_millis(10));
        let loop_data = conf.as_ref().is_some_and(|c| c.loop_data);
        let rule_state = RuleState::default();
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10 * 60);
        loop {
            for record in &data {
                if tokio::time::Instant::now() >= deadline {
                    return;
                }
                for row in Evaluator::eval_select_stateful_multi(&select_stmt, record, &rule_state)
                {
                    emit_ruletest_line(&session, &row);
                }
                tokio::select! {
                    _ = session.shutdown.notified() => return,
                    _ = tokio::time::sleep(interval) => {}
                }
            }
            if !loop_data {
                return;
            }
        }
    });
    (StatusCode::OK, "started\n").into_response()
}

/// Buffer one replayed row into the bounded session ring (evicting the
/// oldest past the cap) and wake SSE subscribers via broadcast.
fn emit_ruletest_line(session: &RuletestSession, row: &HashMap<String, Value>) {
    let line = serde_json::to_string(row).unwrap_or_default();
    {
        let mut replay = session.replay.write();
        let seq = replay.next_seq;
        replay.next_seq = seq.saturating_add(1);
        replay.entries.push_back((seq, line.clone()));
        while replay.entries.len() > RULETEST_HISTORY_CAP {
            replay.entries.pop_front();
        }
    }
    let _ = session.output_tx.send(line);
}

async fn delete_ruletest(State(state): State<AppState>, Path(name): Path<String>) -> Response {
    // Stopping the replay loop alongside the session.
    if let Some(session) = state.ruletests.write().remove(&name) {
        session.shutdown.notify_waiters();
    }
    (StatusCode::OK, "dropped\n").into_response()
}

/// Documented SSE feed: `GET /test/:id` streams the session replay as
/// `text/event-stream` — retained rows first (late subscribers backfill up
/// to the retention cap, then go live), then live rows indefinitely.
/// Served both on the main REST router and on the dedicated `httpServerPort`
/// listener (see [`test_sse_router`]).
pub async fn sse_ruletest(State(state): State<AppState>, Path(name): Path<String>) -> Response {
    let Some(session) = state.ruletests.read().get(&name).cloned() else {
        return (
            StatusCode::NOT_FOUND,
            format!("Ruletest {} not found", name),
        )
            .into_response();
    };
    let rx = session.output_tx.subscribe();
    let replay = session.replay.clone();
    // Cursor resume over the sequence-numbered ring: the replay buffer is
    // the source of truth and broadcast messages are only wake-ups. A
    // subscriber sends every row newer than its cursor, in order; a cursor
    // older than the retained prefix skips the evicted gap (documented lag)
    // and resumes at the oldest retained row. Session deletion drops the
    // broadcast sender, which terminates the stream.
    struct SseCursor {
        rx: broadcast::Receiver<String>,
        replay: Arc<RwLock<RuletestReplay>>,
        cursor: u64,
        pending: VecDeque<String>,
    }
    let stream = futures::stream::unfold(
        SseCursor {
            rx,
            replay,
            cursor: 0,
            pending: VecDeque::new(),
        },
        |mut st| async move {
            loop {
                if let Some(line) = st.pending.pop_front() {
                    return Some((Ok::<_, axum::Error>(Event::default().data(line)), st));
                }
                // Single atomic snapshot: copy every row at/after the cursor
                // AND derive the next cursor from the last row actually
                // copied, under the same lock. Rows appended concurrently
                // after the snapshot stay above the cursor and are picked up
                // on the next pass — never skipped, never duplicated.
                // A cursor older than the retained prefix resumes at the
                // oldest retained row (documented lag gap).
                let (fresh, next): (Vec<String>, u64) = {
                    let guard = st.replay.read();
                    let mut fresh = Vec::new();
                    let mut next = st.cursor;
                    for (seq, line) in guard.entries.iter() {
                        if *seq >= st.cursor {
                            fresh.push(line.clone());
                            next = seq.saturating_add(1);
                        }
                    }
                    (fresh, next)
                };
                if !fresh.is_empty() {
                    st.cursor = next;
                    st.pending = fresh.into();
                    continue;
                }
                match st.rx.recv().await {
                    // A new row was buffered; refill from the ring.
                    Ok(_) => continue,
                    // Overflow drops broadcast copies only; the ring stays
                    // complete, so keep refilling from the cursor.
                    Err(broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(broadcast::error::RecvError::Closed) => return None,
                }
            }
        },
    );
    Sse::new(stream)
        .keep_alive(KeepAlive::new())
        .into_response()
}

/// Dedicated ruletest SSE listener serving the documented
/// `http://<httpServerIp>:<httpServerPort>/test/:id` endpoint.
pub fn test_sse_router(state: AppState) -> axum::Router {
    axum::Router::new()
        .route("/test/:name", get(sse_ruletest))
        .with_state(state)
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn test_stream_manager(options: &[(&str, &str)]) -> StreamManager {
        let manager = StreamManager::new();
        let opts = options
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect::<HashMap<_, _>>();
        manager
            .create_stream(StreamDefinition {
                name: "demo".to_string(),
                sql: String::new(),
                stream_fields: Vec::new(),
                options: opts,
            })
            .await
            .unwrap();
        manager
    }

    fn test_source_configs(pairs: &[(&str, Value)]) -> Arc<RwLock<HashMap<String, Value>>> {
        Arc::new(RwLock::new(
            pairs
                .iter()
                .map(|(k, v)| (k.to_string(), v.clone()))
                .collect(),
        ))
    }

    #[tokio::test]
    async fn resolve_mqtt_source_honors_confkey_without_topic() {
        // D1: a topic-less stored config must still contribute its broker URL.
        let manager = test_stream_manager(&[("CONF_KEY", "remotekey")]).await;
        let configs =
            test_source_configs(&[("mqtt/remotekey", json!({"server": "tcp://broker:1883"}))]);
        let cfg = resolve_mqtt_source(&manager, &configs, "demo", "r1").unwrap();
        assert_eq!(cfg.server, "tcp://broker:1883");
        assert_eq!(cfg.topic, "demo");
    }

    #[tokio::test]
    async fn resolve_mqtt_source_is_case_insensitive() {
        // Lowercase SQL/JSON option spellings resolve like the uppercase ones.
        let manager =
            test_stream_manager(&[("conf_key", "remotekey"), ("datasource", "sensors/#")]).await;
        let configs =
            test_source_configs(&[("mqtt/remotekey", json!({"server": "tcp://broker:1883"}))]);
        let cfg = resolve_mqtt_source(&manager, &configs, "demo", "r1").unwrap();
        assert_eq!(cfg.server, "tcp://broker:1883");
        assert_eq!(cfg.topic, "sensors/#");
    }

    #[tokio::test]
    async fn resolve_mqtt_source_server_option_overrides_confkey() {
        let manager =
            test_stream_manager(&[("CONF_KEY", "remotekey"), ("server", "tcp://override:1883")])
                .await;
        let configs =
            test_source_configs(&[("mqtt/remotekey", json!({"server": "tcp://broker:1883"}))]);
        let cfg = resolve_mqtt_source(&manager, &configs, "demo", "r1").unwrap();
        assert_eq!(cfg.server, "tcp://override:1883");
    }

    #[tokio::test]
    async fn resolve_mqtt_source_falls_back_to_loopback() {
        let manager = test_stream_manager(&[]).await;
        let configs = test_source_configs(&[]);
        let cfg = resolve_mqtt_source(&manager, &configs, "demo", "r1").unwrap();
        assert_eq!(cfg.server, "tcp://127.0.0.1:1883");
        assert_eq!(cfg.topic, "demo");
    }

    #[tokio::test]
    async fn resolve_sql_source_from_confkey() {
        // D8: a stored `sql/{key}` config provides url/table/interval.
        let manager = test_stream_manager(&[("TYPE", "sql"), ("CONF_KEY", "pg")]).await;
        let configs = test_source_configs(&[(
            "sql/pg",
            json!({"url": "postgres://db:5432/k", "table": "readings", "interval": 500}),
        )]);
        let cfg = resolve_sql_source(&manager, &configs, "demo", "r1").unwrap();
        assert_eq!(cfg.url, "postgres://db:5432/k");
        assert_eq!(cfg.table, "readings");
        assert_eq!(cfg.interval, 500);
    }

    #[tokio::test]
    async fn resolve_sql_source_from_stream_options() {
        // Lowercase option spellings resolve like the uppercase ones.
        // Documented plugin shape: dburl + templateSqlQueryCfg, table
        // anchored by the stream DATASOURCE.
        let manager = test_stream_manager(&[
            ("TYPE", "sql"),
            ("DATASOURCE", "rksrc"),
            ("CONF_KEY", "postgresql_config"),
        ])
        .await;
        let configs = test_source_configs(&[(
            "sql/postgresql_config",
            json!({
                "dburl": "postgres://u:p@h/db?sslmode=disable",
                "interval": 5000,
                "templateSqlQueryCfg": {"templateSql": "SELECT id, val FROM rksrc"},
            }),
        )]);
        let cfg = resolve_sql_source(&manager, &configs, "demo", "r1").unwrap();
        assert_eq!(cfg.url, "postgres://u:p@h/db?sslmode=disable");
        assert_eq!(cfg.table, "rksrc");
        assert_eq!(cfg.interval, 5000);
        assert_eq!(
            rekuiper_connectors::sql_source_query(&cfg),
            "SELECT id, val FROM rksrc"
        );
        let manager = test_stream_manager(&[
            ("type", "SQL"),
            ("datasource", "sqlite://x.db"),
            ("table", "sens"),
        ])
        .await;
        let cfg = resolve_sql_source(&manager, &test_source_configs(&[]), "demo", "r1").unwrap();
        assert_eq!(cfg.url, "sqlite://x.db");
        assert_eq!(cfg.table, "sens");
        assert_eq!(cfg.interval, 1000);
    }

    #[tokio::test]
    async fn resolve_sql_source_rejects_non_sql_types() {
        let manager = test_stream_manager(&[("TYPE", "mqtt")]).await;
        assert!(resolve_sql_source(&manager, &test_source_configs(&[]), "demo", "r1").is_none());
        let manager = test_stream_manager(&[]).await;
        assert!(resolve_sql_source(&manager, &test_source_configs(&[]), "demo", "r1").is_none());
    }

    fn tagged(source: &str, pairs: &[(&str, Value)]) -> TaggedRow {
        TaggedRow {
            source: source.to_string(),
            data: pairs
                .iter()
                .map(|(k, v)| (k.to_string(), v.clone()))
                .collect(),
        }
    }

    fn parse_stmt(sql: &str) -> SelectStmt {
        Parser::new(sql).parse_select().expect("join SQL parses")
    }

    #[tokio::test]
    async fn window_join_right_preserves_unmatched_without_left() {
        // RIGHT JOIN with an empty left side still emits right rows.
        let stmt = parse_stmt(
            "SELECT l.id AS id, r.val AS v FROM l RIGHT JOIN r ON l.id = r.id GROUP BY CountWindow(2)",
        );
        let tables = TableManager::new();
        let confs = test_source_configs(&[]);
        let batch = vec![tagged("r", &[("id", json!(1)), ("val", json!(9))])];
        let out = eval_window_join_batch(&tables, &confs, &stmt, &batch).await;
        assert_eq!(out.len(), 1);
        // Missing left side projects to Null; the preserved right side is intact.
        assert_eq!(out[0].get("id"), Some(&serde_json::Value::Null));
        assert_eq!(out[0].get("v"), Some(&json!(9)));
    }

    #[tokio::test]
    async fn window_join_inner_empty_left_yields_nothing() {
        let stmt = parse_stmt(
            "SELECT l.id AS id FROM l INNER JOIN r ON l.id = r.id GROUP BY CountWindow(2)",
        );
        let tables = TableManager::new();
        let confs = test_source_configs(&[]);
        let batch = vec![tagged("r", &[("id", json!(1))])];
        let out = eval_window_join_batch(&tables, &confs, &stmt, &batch).await;
        assert!(out.is_empty());
    }

    #[tokio::test]
    async fn window_join_cross_fanout_is_bounded() {
        // 201 x 201 pairs would fan out to 40401 rows; the cap holds.
        let stmt = parse_stmt("SELECT l.id AS id FROM l CROSS JOIN r GROUP BY CountWindow(50000)");
        let tables = TableManager::new();
        let confs = test_source_configs(&[]);
        let mut batch = Vec::new();
        for i in 0..201 {
            batch.push(tagged("l", &[("id", json!(i))]));
            batch.push(tagged("r", &[("id", json!(i))]));
        }
        let out = eval_window_join_batch(&tables, &confs, &stmt, &batch).await;
        assert_eq!(out.len(), MAX_JOIN_FANOUT);
    }

    #[tokio::test]
    async fn window_join_table_takes_first_on_match() {
        // Lookup tables resolve one row per key: the first ON-matching row
        // wins (point-lookup semantics, also used by the stateless path).
        let tables = TableManager::new();
        tables
            .create_table(TableDefinition {
                name: "t".to_string(),
                sql: String::new(),
                stream_fields: Vec::new(),
                options: HashMap::new(),
            })
            .await
            .unwrap();
        tables.insert_table_row(
            "t",
            [("id".to_string(), json!(1)), ("v".to_string(), json!("a"))]
                .into_iter()
                .collect(),
        );
        tables.insert_table_row(
            "t",
            [("id".to_string(), json!(1)), ("v".to_string(), json!("b"))]
                .into_iter()
                .collect(),
        );
        let stmt = parse_stmt(
            "SELECT s.id AS id, t.v AS v FROM s INNER JOIN t ON s.id = t.id GROUP BY CountWindow(2)",
        );
        let confs = test_source_configs(&[]);
        let batch = vec![tagged("s", &[("id", json!(1))])];
        let out = eval_window_join_batch(&tables, &confs, &stmt, &batch).await;
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].get("v"), Some(&json!("a")));
    }

    #[tokio::test]
    async fn bootstrap_join_registers_all_source_cancels() {
        // Stream-stream joins bootstrap one producer per side. Every
        // producer's cancel handle must be retained: overwriting the entry
        // drops the first sender, and a dropped watch sender reads as
        // `Err` in the source task, which exits immediately (join side A
        // would silently stop delivering).
        let bus = StreamBus::new();
        let sm = StreamManager::new();
        for name in ["ja", "jb"] {
            sm.create_stream(StreamDefinition {
                name: name.to_string(),
                sql: String::new(),
                stream_fields: Vec::new(),
                options: HashMap::new(),
            })
            .await
            .unwrap();
        }
        let state = AppState::new(
            "test".to_string(),
            rekuiper_conf::KuiperConfig::default(),
            sm,
            TableManager::new(),
            RuleManager::new(bus.clone()),
            bus,
        );
        let mut parser = Parser::new(
            "SELECT ja.id AS id, jb.val AS v FROM ja INNER JOIN jb ON ja.id = jb.id GROUP BY CountWindow(2)",
        );
        let stmt = parser.parse_select().unwrap();
        bootstrap_rule_sources(&state, "rj", &stmt);
        let guards = state.source_cancels.read();
        let txs = guards.get("rj").expect("join rule has cancel handles");
        assert_eq!(txs.len(), 2, "one cancel sender per joined stream");
        drop(guards);
        cancel_rule_source(&state, "rj");
        assert!(!state.source_cancels.read().contains_key("rj"));
    }

    #[tokio::test]
    async fn config_maps_persist_and_reload_across_restart() {
        // D-RESTART: CONF_KEYs (MQTT brokers, SQL URLs) must survive a
        // daemon restart instead of falling back to loopback defaults.
        use rekuiper_core::MemKvStore;
        let kv: Arc<dyn KvStore> = Arc::new(MemKvStore::new());
        let state = AppState {
            kv: Some(kv.clone()),
            ..AppState::new(
                "test".to_string(),
                rekuiper_conf::KuiperConfig::default(),
                StreamManager::new(),
                TableManager::new(),
                RuleManager::new(StreamBus::new()),
                StreamBus::new(),
            )
        };
        persist_config_entry(
            &state,
            "source_configs",
            "mqtt/evalmqtt",
            &json!({"server": "tcp://broker:1883"}),
        )
        .await;
        persist_config_entry(
            &state,
            "source_configs",
            "sql/postgresql_config",
            &json!({"dburl": "postgres://u:p@h/db", "interval": 5000}),
        )
        .await;
        // A fresh daemon with empty maps reloads everything from KV.
        let fresh = AppState {
            kv: Some(kv),
            ..AppState::new(
                "test".to_string(),
                rekuiper_conf::KuiperConfig::default(),
                StreamManager::new(),
                TableManager::new(),
                RuleManager::new(StreamBus::new()),
                StreamBus::new(),
            )
        };
        load_config_maps(&fresh).await;
        assert_eq!(
            fresh
                .source_configs
                .read()
                .get("mqtt/evalmqtt")
                .and_then(|v| v.get("server"))
                .and_then(|v| v.as_str()),
            Some("tcp://broker:1883")
        );
        assert!(fresh
            .source_configs
            .read()
            .contains_key("sql/postgresql_config"));
        // Deletes propagate too.
        unpersist_config_entry(&state, "source_configs", "mqtt/evalmqtt").await;
        let fresh2 = AppState {
            kv: Some(state.kv.clone().unwrap()),
            ..AppState::new(
                "test".to_string(),
                rekuiper_conf::KuiperConfig::default(),
                StreamManager::new(),
                TableManager::new(),
                RuleManager::new(StreamBus::new()),
                StreamBus::new(),
            )
        };
        load_config_maps(&fresh2).await;
        assert!(!fresh2.source_configs.read().contains_key("mqtt/evalmqtt"));
        assert!(fresh2
            .source_configs
            .read()
            .contains_key("sql/postgresql_config"));
    }

    // D9: RS256 JWT vectors generated offline (2048-bit key, 1-year valid
    // token, 1-hour-expired token, keyless-claims token, wrong-key token).
    const TEST_RSA_PUBLIC_PEM: &str = "-----BEGIN PUBLIC KEY-----\nMIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAmdub6pqDN/MPofsOTQlf\npCF6O4vsisxaDPzKZM8pqiUIOGjDwfqRQkzikSPu/oK8jPouof9JkeesUjbKg+0w\nQ7aZXgRPr8PJkHeY27/4bFz1riFPDZ+rKAe8DvXIlcjb70H68AtGnRzUkVjVlzhn\n6qfJE4LMmLtdQodW4Hnd2Oo8qujRprtn8AMcX5H1phIVUHYdIZpt44SNetOgCPxZ\n/S/0VLi2qD7bh/bBj6VRvea/LyCKnC75r+wnJGIHYpeVXskMrDBH+lfV1GsoU9Ig\nm+5MYAkc0SrgYBVCUXXvQNrip3IQcaWlW4YhkJC2rVBCA2ibc1MMWOyA0xYfJfy1\nvQIDAQAB\n-----END PUBLIC KEY-----\n";
    const TEST_JWT_VALID: &str = "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJ0ZXN0ZXIiLCJleHAiOjE4MjA2MDE3MDB9.U3NGxZxNiHStyGFGZOKsz4PPM3aX2papMEOsKUqWnYC7NmgGxESTKWKtPM6M5McKUQwD3cfnnFH9p3XGdkTfPofxcnmJcrN8PMWdaVAcetYn_c5ScMWgQapjuHiO7jBQKljTU4AwuGrNQAMtxgBgSdEX-MG1AYZrNYcdgqXtfUPk8y_V2icNU8kVQRzRonHs4yaioIqy1IpBuxpb6A5AHRy07T_En_TlebfH7Tb-lf3tFDT8UUcjnfFJ-TPxFTGqLsPnLbdqXcrKi-PVqpbuFj2WSFlGinbZDGEsjhopdHPD1_PkC0Am3c4XmiCM3QswVqcUujwED71lcZXBdCFxPA";
    const TEST_JWT_EXPIRED: &str = "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJ0ZXN0ZXIiLCJleHAiOjE3ODkwNjIxMDB9.GNrAExENuYVgbt8bZebTYTmCf2o0DcAkWzhpfkBCtAvPflzzr9xSNaEOrtf2HJU5gqOm-ZDIztXsatd765CtZA-uLcqjbCa4TURbMv0OFHDhMVlVfn2xr17jr39IG1ck7Ymioz_ZSd3wXu0egABsCUrKigEvtRwcGJSWlAp1GdRiWLpT2-U6Xb15bgUdbCNgYxyXiWM4zBLHVILLLH-zPyPIbvHo82l3qOpu7c2SRw5aNPrY2KXPCxKm47NEOkf74LJk14MZODeRTOYczyi8Q4vtD6xEcp8-u_vVUv5Gv04mrO1gMCALOQAWfKq2PFA0QatGX_cw8f36m_EOLLfuWA";
    const TEST_JWT_NOEXP: &str = "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJ0ZXN0ZXIifQ.dqsuFs9MyRMcgfPGvUAdCOJXKYFSMphpVASqmMH_n4aaAa3F7QeUz8ggX0931Z_TbgxvZ4Dp-Lf05eVSWOBWvpnjwSvn1JLw7axaEFl7BOEzsFzq_1gPMKMY1TXylXgUV2DHUhlayS53UcEvS5MP0vKQG07PsTOjFZACeuohilHX5vEmn9zwy67CbwL7Z3g32Msdb67pplMViAXGgau3UTYi5DOZFsSFlDrOj5w2Gg1EeZBpC1udtUvkIjHiW92LkoLI7HBzmRQwEuxGgOE134T2YGF5FuPjq-XpfiklV6SmrQpuHM_qGPtLHUm-6blwOgffAnr0uiCYVHZT5pe2hw";
    const TEST_JWT_WRONGKEY: &str = "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJ0ZXN0ZXIiLCJleHAiOjE4MjA2MDE3MDB9.Uu3vn8teCnUBsT-8updR4it_yyMoYdtqFGOvLgozxmsOmM8hb-FfqpeuqelBemgsbpwi2pbRVuOUxv0621ORempEpwfhCUnI80Rj9hPcOV0j8wYuX-xdsnvsNOPI1K6XlW9i1IOjToKxhptaPEPdYXf_jEq5v3P0zCITwevT2t6bpOdYzgSHjxq6CJiXIJHcP0idiQ1I3PqNhKmjJn3qi14GQ_xRk8Sf30VQDqsT76qXw0S92K-QKdcbc35oJW5x6oTdbgELv9YV-su1ooXTrLQ5zzY0g8qXhbkeLotXQEddaGMFeBOWchDax_km1g1gRbCS8f7TNEtO2xVkVHx-AQ";

    fn test_key_der() -> Vec<u8> {
        let (label, der) =
            parse_pem_block(TEST_RSA_PUBLIC_PEM.as_bytes()).expect("test PEM parses");
        assert_eq!(label, "PUBLIC KEY");
        spki_to_pkcs1(&der).expect("test SPKI unwraps to PKCS#1")
    }

    #[test]
    fn spki_unwrap_keeps_modulus_and_exponent() {
        // The unwrapped PKCS#1 body is a bare SEQUENCE of two INTEGERs.
        let pkcs1 = test_key_der();
        let (tag, content, rest) = der_read_tlv(&pkcs1).expect("PKCS#1 parses as DER");
        assert_eq!(tag, 0x30);
        assert!(rest.is_empty());
        let (n_tag, n_content, e_rest) = der_read_tlv(content).expect("modulus parses");
        assert_eq!(n_tag, 0x02);
        assert_eq!(n_content.len(), 257); // 2048-bit modulus + leading zero
        let (e_tag, e_content, e_end) = der_read_tlv(e_rest).expect("exponent parses");
        assert_eq!(e_tag, 0x02);
        assert_eq!(e_content, &[0x01, 0x00, 0x01]); // 65537
        assert!(e_end.is_empty());
    }

    #[test]
    fn jwt_verify_accepts_valid_token() {
        let key = test_key_der();
        assert_eq!(verify_jwt_raw(TEST_JWT_VALID, &key), Ok(()));
        // Tokens without `exp` carry no expiry to enforce.
        assert_eq!(verify_jwt_raw(TEST_JWT_NOEXP, &key), Ok(()));
    }

    #[test]
    fn jwt_verify_rejects_expired_token() {
        assert_eq!(
            verify_jwt_raw(TEST_JWT_EXPIRED, &test_key_der()),
            Err("Token has expired\n")
        );
    }

    #[test]
    fn jwt_verify_rejects_malformed_tokens() {
        let key = test_key_der();
        for bad in ["", "abc", "a.b", "a.b.c.d", "a..c", "!!!.@@@.###"] {
            assert_eq!(
                verify_jwt_raw(bad, &key),
                Err("Invalid JWT format\n"),
                "token: {}",
                bad
            );
        }
    }

    #[test]
    fn jwt_verify_rejects_wrong_key_and_tampering() {
        let key = test_key_der();
        assert_eq!(
            verify_jwt_raw(TEST_JWT_WRONGKEY, &key),
            Err("Invalid token signature\n")
        );
        // Swap two adjacent differing signature characters: the decoded
        // bytes must change, so verification has to fail.
        let sig_start = TEST_JWT_VALID.rfind('.').unwrap() + 1;
        let mut tampered = TEST_JWT_VALID.to_string();
        let bytes = tampered.as_bytes();
        let mut idx = None;
        for i in sig_start..bytes.len() - 1 {
            if bytes[i] != bytes[i + 1] {
                idx = Some(i);
                break;
            }
        }
        let i = idx.expect("signature has swappable characters");
        tampered.replace_range(
            i..i + 2,
            &format!("{}{}", bytes[i + 1] as char, bytes[i] as char),
        );
        assert_ne!(tampered, TEST_JWT_VALID);
        assert_eq!(
            verify_jwt_raw(&tampered, &key),
            Err("Invalid token signature\n")
        );
    }
}
