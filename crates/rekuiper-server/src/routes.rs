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
    apply_data_template, FileSink, FileSource, FileSourceConfig, HttpPullConfig, HttpPullSource,
    KafkaConfig, KafkaSink, KafkaSource, MqttConfig, MqttSink, MqttSource, RedisSink,
    RedisSinkConfig, RedisSubSource, SimulatorConfig, SimulatorSource, Sink, SqlConnectorConfig,
    SqlSink, WebSocketConfig, WebSocketSink, WebSocketSource,
};
use rekuiper_core::{
    model::{compile_graph_to_sql_and_actions, SchemaDefinition, StreamRecord},
    PluginDefinition, PluginManager, RuleDefinition, RuleManager, SchemaManager, StreamBus,
    StreamDefinition, StreamManager, TableDefinition, TableManager,
};
use rekuiper_sql::{
    builtin_function_metadata, Evaluator, Expr, JoinClause, JoinType, Parser, RuleState,
    SelectStmt, TimeUnit, WindowDef,
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
        Self::default()
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

#[derive(Clone)]
pub struct AppState {
    pub start_time: Instant,
    pub version: String,
    pub config: KuiperConfig,
    pub stream_manager: StreamManager,
    pub table_manager: TableManager,
    pub rule_manager: RuleManager,
    pub stream_bus: StreamBus,
    pub connections: Arc<RwLock<HashMap<String, Value>>>,
    pub source_configs: Arc<RwLock<HashMap<String, Value>>>,
    pub sink_configs: Arc<RwLock<HashMap<String, Value>>>,
    pub ruletests: Arc<RwLock<HashMap<String, RuletestSession>>>,
    pub source_cancels: Arc<RwLock<HashMap<String, tokio::sync::watch::Sender<bool>>>>,
    pub http_client: reqwest::Client,
    pub schema_manager: SchemaManager,
    pub plugin_manager: PluginManager,
    pub trace_manager: TraceManager,
    pub task_manager: TaskManager,
}

/// An interactive rule-simulation session: mock source data is replayed
/// through the rule SQL and output rows stream out over SSE.
#[derive(Clone)]
pub struct RuletestSession {
    pub id: String,
    pub sql: String,
    pub mock_source: HashMap<String, SimulatorConfig>,
    pub output_tx: tokio::sync::broadcast::Sender<String>,
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
            config,
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
        }
    }
}

pub fn create_router(state: AppState) -> Router {
    Router::new()
        .route("/ping", get(ping_handler))
        .route("/", get(root_handler).post(root_handler))
        .route("/streams", get(list_streams).post(create_stream))
        .route("/streams/:name", get(get_stream).delete(delete_stream))
        .route("/streams/:name/data", post(push_stream_data))
        .route("/streams/:name/schema", get(get_stream_schema))
        .route("/tables", get(list_tables).post(create_table))
        .route("/tables/:name", get(get_table).delete(delete_table))
        .route("/tables/:name/data", post(push_table_data))
        .route("/tables/:name/schema", get(get_table_schema))
        .route("/tabledetails", get(get_table_details))
        .route("/streamdetails", get(get_stream_details))
        .route("/rules", get(list_rules).post(create_rule))
        .route("/rules/validate", post(validate_rule))
        .route("/rules/status/all", get(get_all_rule_status))
        .route("/rules/:name", get(get_rule).delete(delete_rule))
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
        .route("/configs", get(get_configs))
        .route("/config/uploads", get(get_config_uploads))
        .route("/config/uploads/:name", delete(empty_ok))
        .route("/stop", get(stop_server).post(stop_server))
        .route("/data/import", post(import_ruleset))
        .route("/data/export", get(export_ruleset))
        .route("/v2/data/import", post(import_ruleset))
        .route("/v2/data/export", get(export_ruleset))
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
            get(get_connection).delete(delete_connection),
        )
        .route("/plugins/sources", get(empty_array))
        .route("/plugins/sources/prebuild", get(empty_array))
        .route(
            "/plugins/sources/:name",
            get(validated_empty_object)
                .put(validated_empty_ok)
                .delete(validated_empty_ok),
        )
        .route("/plugins/sinks", get(empty_array))
        .route("/plugins/sinks/prebuild", get(empty_array))
        .route(
            "/plugins/sinks/:name",
            get(validated_empty_object)
                .put(validated_empty_ok)
                .delete(validated_empty_ok),
        )
        .route(
            "/plugins/functions",
            get(list_function_plugins).post(create_function_plugin),
        )
        .route("/plugins/functions/prebuild", get(empty_array))
        .route(
            "/plugins/functions/:name",
            get(get_function_plugin)
                .put(validated_empty_ok)
                .delete(delete_function_plugin),
        )
        .route(
            "/plugins/functions/:name/register",
            post(validated_empty_ok),
        )
        .route("/plugins/portables", get(empty_array))
        .route(
            "/plugins/portables/:name",
            get(validated_empty_object)
                .put(validated_empty_ok)
                .delete(validated_empty_ok),
        )
        .route(
            "/plugins/portables/:name/status",
            get(validated_empty_object),
        )
        .route(
            "/plugins/udfs",
            get(list_udf_plugins).post(create_udf_plugin),
        )
        .route(
            "/plugins/udfs/:name",
            get(get_udf_plugin).delete(delete_udf_plugin),
        )
        .route("/services", get(empty_array))
        .route(
            "/services/:name",
            get(validated_empty_object)
                .put(validated_empty_ok)
                .delete(validated_empty_ok),
        )
        .route("/services/functions", get(empty_array))
        .route("/services/functions/:name", get(validated_empty_object))
        .route("/udf/javascript", get(empty_array))
        .route(
            "/udf/javascript/:id",
            get(validated_empty_object)
                .put(validated_empty_ok)
                .delete(validated_empty_ok),
        )
        .route("/schemas/:kind", get(list_schemas).post(create_schema))
        .route(
            "/schemas/:kind/:name",
            get(get_schema).put(update_schema).delete(delete_schema),
        )
        .route("/schemas/:kind/:name/upload", put(update_schema))
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

async fn list_streams(State(state): State<AppState>) -> impl IntoResponse {
    let streams = state.stream_manager.list_streams();
    Json(streams)
}

async fn create_stream(
    State(state): State<AppState>,
    Json(payload): Json<CreateStreamPayload>,
) -> Response {
    if let Some(sql) = payload.sql {
        let mut parser = Parser::new(&sql);
        match parser.parse_create_stream() {
            Ok(stmt) => {
                let stream_def = StreamDefinition {
                    name: stmt.name.clone(),
                    sql: sql.clone(),
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
        Json(def).into_response()
    } else {
        (StatusCode::NOT_FOUND, format!("Stream {} not found", name)).into_response()
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
        Json(def).into_response()
    } else {
        (StatusCode::NOT_FOUND, format!("Table {} not found", name)).into_response()
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

async fn get_stream_schema(State(state): State<AppState>, Path(name): Path<String>) -> Response {
    if let Some(def) = state.stream_manager.get_stream(&name) {
        Json(json!({ "name": def.name, "options": def.options })).into_response()
    } else {
        (StatusCode::NOT_FOUND, format!("Stream {} not found", name)).into_response()
    }
}

async fn get_table_schema(State(state): State<AppState>, Path(name): Path<String>) -> Response {
    if let Some(def) = state.table_manager.get_table(&name) {
        Json(json!({ "name": def.name, "options": def.options })).into_response()
    } else {
        (StatusCode::NOT_FOUND, format!("Table {} not found", name)).into_response()
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
    if let Some(key) = def.options.get("CONF_KEY") {
        if !key.trim().is_empty() {
            let lookup = format!("mqtt/{}", key);
            if let Some(conf_val) = source_configs.read().get(&lookup).cloned() {
                match serde_json::from_value::<MqttConfig>(conf_val) {
                    Ok(stored) => config = stored,
                    Err(e) => {
                        tracing::warn!(
                            "[RULE {}] invalid mqtt config '{}': {}",
                            rule_id,
                            lookup,
                            e
                        );
                    }
                }
            }
        }
    }
    if let Some(server) = def.options.get("SERVER") {
        if !server.trim().is_empty() {
            config.server = server.clone();
        }
    }
    if let Some(topic) = def.options.get("DATASOURCE") {
        if !topic.trim().is_empty() {
            config.topic = topic.clone();
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

fn bootstrap_rule_sources(state: &AppState, rule_id: &str, select_stmt: &SelectStmt) {
    // MQTT is the default streaming source: typeless streams and TYPE="mqtt"
    // subscribe to the broker topic and feed the rule pipeline.
    if let Some(config) = resolve_mqtt_source(
        &state.stream_manager,
        &state.source_configs,
        &select_stmt.from,
        rule_id,
    ) {
        let stream_tx = state.stream_bus.get_or_create(&select_stmt.from);
        let (cancel_tx, cancel_rx) = tokio::sync::watch::channel(false);
        state
            .source_cancels
            .write()
            .insert(rule_id.to_string(), cancel_tx);
        MqttSource::new(config, stream_tx).spawn(cancel_rx);
    }

    // File source streams tail a line-delimited file into the stream bus.
    if let Some(config) = resolve_file_source(
        &state.stream_manager,
        &state.source_configs,
        &select_stmt.from,
        rule_id,
    ) {
        let stream_tx = state.stream_bus.get_or_create(&select_stmt.from);
        let (cancel_tx, cancel_rx) = tokio::sync::watch::channel(false);
        state
            .source_cancels
            .write()
            .insert(rule_id.to_string(), cancel_tx);
        FileSource::new(config, stream_tx).spawn(cancel_rx);
    }

    // HTTP pull source streams poll a remote endpoint into the stream bus.
    if let Some(conf) = resolve_httppull_config(
        &state.stream_manager,
        &state.source_configs,
        &select_stmt.from,
        rule_id,
    ) {
        let stream_tx = state.stream_bus.get_or_create(&select_stmt.from);
        let (cancel_tx, cancel_rx) = tokio::sync::watch::channel(false);
        state
            .source_cancels
            .write()
            .insert(rule_id.to_string(), cancel_tx);
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
        &select_stmt.from,
        rule_id,
    ) {
        let stream_tx = state.stream_bus.get_or_create(&select_stmt.from);
        let (cancel_tx, cancel_rx) = tokio::sync::watch::channel(false);
        state
            .source_cancels
            .write()
            .insert(rule_id.to_string(), cancel_tx);
        WebSocketSource { url, tx: stream_tx }.spawn(cancel_rx);
    }

    // Redis subscription streams forward channel messages into the stream bus.
    if let Some((url, channel)) = resolve_redissub_source(
        &state.stream_manager,
        &state.source_configs,
        &select_stmt.from,
        rule_id,
    ) {
        let stream_tx = state.stream_bus.get_or_create(&select_stmt.from);
        let (cancel_tx, cancel_rx) = tokio::sync::watch::channel(false);
        state
            .source_cancels
            .write()
            .insert(rule_id.to_string(), cancel_tx);
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
        &select_stmt.from,
        rule_id,
    ) {
        let stream_tx = state.stream_bus.get_or_create(&select_stmt.from);
        let (cancel_tx, cancel_rx) = tokio::sync::watch::channel(false);
        state
            .source_cancels
            .write()
            .insert(rule_id.to_string(), cancel_tx);
        KafkaSource {
            config,
            tx: stream_tx,
        }
        .spawn(cancel_rx);
    }

    // Simulator source streams replay configured data into the stream bus.
    // Stream options are upper-cased by the SQL parser.
    if let Some(def) = state.stream_manager.get_stream(&select_stmt.from) {
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
                            let stream_name = select_stmt.from.clone();
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

/// Signal cancellation to a rule's background streaming source (MQTT, file,
/// HTTP pull, WebSocket, Redis subscription or Kafka consumer), if one is
/// registered.
fn cancel_rule_source(state: &AppState, rule_id: &str) {
    if let Some(tx) = state.source_cancels.write().remove(rule_id) {
        let _ = tx.send(true);
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
    combined: &HashMap<String, Value>,
) -> Option<(String, String)> {
    fn side(expr: &Expr, from: &str, target: &str) -> u8 {
        match expr {
            // 0 = stream side, 1 = table side, 2 = unknown.
            Expr::FieldAccess { parent, .. } => match parent.as_ref() {
                Expr::Identifier(name) if name == target => 1,
                Expr::Identifier(name) if name == from => 0,
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
        side(left, from, &join.target),
        side(right, from, &join.target),
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
    combined: &HashMap<String, Value>,
) -> Option<String> {
    join_key_parts(join, from, combined).map(|(_, value)| value)
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
                    Ok(conf) => return Some((conf.url, conf.table)),
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
    join: &JoinClause,
    combined: &HashMap<String, Value>,
) -> Vec<HashMap<String, Value>> {
    let table_type = table_manager
        .get_table(&join.target)
        .and_then(|def| def.options.get("TYPE").cloned())
        .unwrap_or_default();
    if table_type.eq_ignore_ascii_case("redis") {
        let Some(key) = extract_lookup_key(join, from, combined) else {
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
            join_key_parts(join, from, combined),
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
    fn as_object(row: &HashMap<String, Value>) -> Value {
        Value::Object(row.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
    }
    let mut combined = record.clone();
    combined.insert(select_stmt.from.clone(), as_object(record));
    for join in &select_stmt.joins {
        let mut matched: Option<HashMap<String, Value>> = None;
        for row in lookup_candidates(
            table_manager,
            source_configs,
            &select_stmt.from,
            join,
            &combined,
        )
        .await
        {
            let mut probe = combined.clone();
            for (k, v) in &row {
                probe.entry(k.clone()).or_insert(v.clone());
            }
            probe.insert(join.target.clone(), as_object(&row));
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
                combined.insert(join.target.clone(), as_object(&row));
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
) {
    let count = size.max(1);
    let hop = interval.unwrap_or(count).max(1);
    let mut buffer: Vec<HashMap<String, Value>> = Vec::new();
    let mut events_since_trigger: usize = 0;
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
                buffer.push(record.data);
                events_since_trigger += 1;
                if hop <= count {
                    // Standard count window (tumbling when hop == count, overlapping when hop < count)
                    if buffer.len() >= count {
                        let batch = &buffer[0..count];
                        if let Some(output) = Evaluator::eval_aggregate(&select_stmt, batch) {
                            let output_record = StreamRecord::new(output);
                            enqueue_sink_record(&sink, output_record).await;
                            rule_mgr.inc_sink_records(&rule_id, 1);
                        }
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
                            if let Some(output) = Evaluator::eval_aggregate(&select_stmt, &buffer) {
                                let output_record = StreamRecord::new(output);
                                enqueue_sink_record(&sink, output_record).await;
                                rule_mgr.inc_sink_records(&rule_id, 1);
                            }
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
) {
    let mut ticker = tokio::time::interval(duration);
    let mut buffer: Vec<HashMap<String, Value>> = Vec::new();
    // Event-time state: event-timestamped rows, the watermark, and the start
    // of the currently open event-time window (aligned to its length).
    let mut et_buffer: Vec<(i64, HashMap<String, Value>)> = Vec::new();
    let mut watermark: i64 = i64::MIN;
    let mut window_start: Option<i64> = None;
    let window_millis = duration.as_millis() as i64;
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
                        if event_time.enabled {
                            let event_ts = extract_event_timestamp(
                                &record.data,
                                event_time.timestamp_field.as_deref(),
                            );
                            if event_ts < watermark {
                                // Late arrival beyond the tolerance horizon: drop.
                                continue;
                            }
                            watermark = watermark
                                .max(event_ts.saturating_sub(event_time.late_tolerance_ms));
                            let aligned =
                                event_ts - event_ts.rem_euclid(window_millis.max(1));
                            if window_start.is_none() {
                                window_start = Some(aligned);
                            }
                            et_buffer.push((event_ts, record.data));
                            // Close every window the watermark has passed.
                            while let Some(t0) = window_start {
                                let t_end = t0.saturating_add(window_millis);
                                if watermark < t_end {
                                    break;
                                }
                                let batch: Vec<HashMap<String, Value>> = et_buffer
                                    .iter()
                                    .filter(|(ts, _)| *ts >= t0 && *ts < t_end)
                                    .map(|(_, data)| data.clone())
                                    .collect();
                                et_buffer.retain(|(ts, _)| *ts >= t_end);
                                window_start = Some(t_end);
                                if batch.is_empty() {
                                    continue;
                                }
                                if let Some(output) =
                                    Evaluator::eval_aggregate(&select_stmt, &batch)
                                {
                                    let output_record = StreamRecord::new(output);
                                    enqueue_sink_record(&sink, output_record).await;
                                    rule_mgr.inc_sink_records(&rule_id, 1);
                                }
                            }
                            continue;
                        }
                        buffer.push(record.data);
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(broadcast::error::RecvError::Closed) => break,
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
                if let Some(output) = Evaluator::eval_aggregate(&select_stmt, &buffer) {
                    let output_record = StreamRecord::new(output);
                    enqueue_sink_record(&sink, output_record).await;
                    rule_mgr.inc_sink_records(&rule_id, 1);
                }
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
) {
    let mut ticker = tokio::time::interval(hop);
    // Tokio's interval fires immediately on the first tick; consume it so the
    // first window emission aligns with elapsed hop time.
    ticker.tick().await;
    let mut buffer: Vec<(std::time::Instant, HashMap<String, Value>)> = Vec::new();
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
                        buffer.push((std::time::Instant::now(), record.data));
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
            _ = ticker.tick() => {
                let now = std::time::Instant::now();
                // Expire and discard records older than the full window length
                buffer.retain(|(ts, _)| now.duration_since(*ts) <= length);
                if buffer.is_empty() {
                    continue;
                }
                let batch: Vec<HashMap<String, Value>> =
                    buffer.iter().map(|(_, data)| data.clone()).collect();
                if let Some(output) = Evaluator::eval_aggregate(&select_stmt, &batch) {
                    let output_record = StreamRecord::new(output);
                    enqueue_sink_record(&sink, output_record).await;
                    rule_mgr.inc_sink_records(&rule_id, 1);
                }
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
) {
    let mut buffer: Vec<(std::time::Instant, HashMap<String, Value>)> = Vec::new();
    // Event-time state: event-timestamped rows plus the watermark.
    let mut et_buffer: Vec<(i64, HashMap<String, Value>)> = Vec::new();
    let mut watermark: i64 = i64::MIN;
    let window_millis = length.as_millis() as i64;
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
                // If delay is configured, wait for the delay duration before evaluating
                // so events arriving during the delay window are captured.
                if let Some(delay_dur) = delay {
                    if !delay_dur.is_zero() {
                        tokio::time::sleep(delay_dur).await;
                    }
                }
                if event_time.enabled {
                    let event_ts = extract_event_timestamp(
                        &record.data,
                        event_time.timestamp_field.as_deref(),
                    );
                    if event_ts < watermark {
                        // Late arrival beyond the tolerance horizon: drop.
                        continue;
                    }
                    watermark =
                        watermark.max(event_ts.saturating_sub(event_time.late_tolerance_ms));
                    et_buffer.push((event_ts, record.data));
                    et_buffer.sort_by_key(|(ts, _)| *ts);
                    // Lower-bounded horizon only: expiry is purely age-based
                    // (`ts >= event_ts - length`). Newer buffered rows must
                    // survive out-of-order arrivals within the window.
                    et_buffer.retain(|(ts, _)| *ts >= event_ts.saturating_sub(window_millis));
                    if et_buffer.is_empty() {
                        continue;
                    }
                    let batch: Vec<HashMap<String, Value>> =
                        et_buffer.iter().map(|(_, data)| data.clone()).collect();
                    if let Some(output) = Evaluator::eval_aggregate(&select_stmt, &batch) {
                        let output_record = StreamRecord::new(output);
                        enqueue_sink_record(&sink, output_record).await;
                        rule_mgr.inc_sink_records(&rule_id, 1);
                    }
                    continue;
                }
                let now = std::time::Instant::now();
                buffer.push((now, record.data));
                let eval_time = std::time::Instant::now();
                // Retain only events within the sliding trailing horizon: [eval_time - length, eval_time]
                buffer.retain(|(ts, _)| eval_time.duration_since(*ts) <= length);
                if buffer.is_empty() {
                    continue;
                }
                let batch: Vec<HashMap<String, Value>> =
                    buffer.iter().map(|(_, data)| data.clone()).collect();
                if let Some(output) = Evaluator::eval_aggregate(&select_stmt, &batch) {
                    let output_record = StreamRecord::new(output);
                    enqueue_sink_record(&sink, output_record).await;
                    rule_mgr.inc_sink_records(&rule_id, 1);
                }
            }
            Err(broadcast::error::RecvError::Lagged(_)) => continue,
            Err(broadcast::error::RecvError::Closed) => break,
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

async fn validate_rule(Json(rule): Json<RuleDefinition>) -> Response {
    if rule.sql.trim().is_empty() {
        if let Some(ref graph) = rule.graph {
            return match compile_graph_to_sql_and_actions(graph) {
                Ok((sql, _)) => {
                    let mut parser = Parser::new(&sql);
                    match parser.parse_select() {
                        Ok(_) => (StatusCode::OK, "The rule has been validated successfully\n")
                            .into_response(),
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
        Ok(_) => (StatusCode::OK, "The rule has been validated successfully\n").into_response(),
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

async fn start_rule(State(state): State<AppState>, Path(name): Path<String>) -> Response {
    if let Err(resp) = check_valid_name(&name) {
        return resp;
    }
    match state.rule_manager.start_rule(&name).await {
        Ok(_) => (StatusCode::OK, format!("Rule {} was started", name)).into_response(),
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
        Ok(_) => (StatusCode::OK, format!("Rule {} was restarted", name)).into_response(),
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

async fn get_configs(State(state): State<AppState>) -> impl IntoResponse {
    Json(state.config.clone())
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

/// Core data import logic shared by synchronous and asynchronous endpoints.
async fn process_import_payload(state: &AppState, payload: &Value) {
    if let Some(streams) = payload.get("streams") {
        if let Some(defs) = streams.as_array() {
            for item in defs {
                if let Ok(def) = serde_json::from_value::<StreamDefinition>(item.clone()) {
                    let name = def.name.clone();
                    let _ = state.stream_manager.create_stream(def).await;
                    state.stream_bus.get_or_create(&name);
                }
            }
        } else if let Some(map) = streams.as_object() {
            for (name, sql) in map {
                let sql_str = sql.as_str().unwrap_or("");
                let mut parser = Parser::new(sql_str);
                if let Ok(stmt) = parser.parse_create_stream() {
                    let stream_name = stmt.name.clone();
                    let _ = state
                        .stream_manager
                        .create_stream(StreamDefinition {
                            name: stream_name.clone(),
                            sql: sql_str.to_string(),
                            options: stmt.options,
                        })
                        .await;
                    state.stream_bus.get_or_create(&stream_name);
                } else if !name.is_empty() {
                    let _ = state
                        .stream_manager
                        .create_stream(StreamDefinition {
                            name: name.clone(),
                            sql: sql_str.to_string(),
                            options: HashMap::new(),
                        })
                        .await;
                    state.stream_bus.get_or_create(name);
                }
            }
        }
    }

    if let Some(tables) = payload.get("tables") {
        if let Some(defs) = tables.as_array() {
            for item in defs {
                if let Ok(def) = serde_json::from_value::<TableDefinition>(item.clone()) {
                    let _ = state.table_manager.create_table(def).await;
                }
            }
        } else if let Some(map) = tables.as_object() {
            for (name, sql) in map {
                let sql_str = sql.as_str().unwrap_or("");
                let mut parser = Parser::new(sql_str);
                if let Ok(stmt) = parser.parse_create_table() {
                    let _ = state
                        .table_manager
                        .create_table(TableDefinition {
                            name: stmt.name.clone(),
                            sql: sql_str.to_string(),
                            options: stmt.options,
                        })
                        .await;
                } else if !name.is_empty() {
                    let _ = state
                        .table_manager
                        .create_table(TableDefinition {
                            name: name.clone(),
                            sql: sql_str.to_string(),
                            options: HashMap::new(),
                        })
                        .await;
                }
            }
        }
    }

    if let Some(rules) = payload.get("rules") {
        if let Some(defs) = rules.as_array() {
            for item in defs {
                let Ok(def) = serde_json::from_value::<RuleDefinition>(item.clone()) else {
                    continue;
                };
                let mut parser = Parser::new(&def.sql);
                let Ok(select_stmt) = parser.parse_select() else {
                    continue;
                };
                if state.rule_manager.create_rule(def.clone()).await.is_err() {
                    continue;
                }
                spawn_rule_task(
                    &state.rule_manager,
                    &state.stream_bus,
                    &state.stream_manager,
                    &state.table_manager,
                    &state.source_configs,
                    &state.http_client,
                    &state.trace_manager,
                    def.id.clone(),
                    select_stmt,
                    def.actions.clone(),
                    def.options.clone(),
                );
            }
        }
    }
}

/// Unified ruleset import: creates streams, tables and rules from an export
/// payload. Also accepts the legacy `{name: sql}` map form for streams and
/// tables. Existing entities are left untouched.
async fn import_ruleset(State(state): State<AppState>, body: Bytes) -> Response {
    let payload: Value = serde_json::from_slice(&body).unwrap_or(Value::Null);
    process_import_payload(&state, &payload).await;
    (StatusCode::OK, "imported successfully\n").into_response()
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

/// Generic empty-list response for discovery endpoints with nothing installed.
async fn empty_array() -> impl IntoResponse {
    Json(Value::Array(Vec::new()))
}

/// Validated variants of the discovery stubs below: they accept any number of
/// path captures (`Path<HashMap<..>>` also matches capture-less routes) and
/// reject names with invalid characters before responding as usual.
async fn validated_empty_object(Path(params): Path<HashMap<String, String>>) -> Response {
    for name in params.values() {
        if let Err(resp) = check_valid_name(name) {
            return resp;
        }
    }
    Json(json!({})).into_response()
}

async fn validated_empty_ok(Path(params): Path<HashMap<String, String>>) -> Response {
    for name in params.values() {
        if let Err(resp) = check_valid_name(name) {
            return resp;
        }
    }
    (StatusCode::OK, Json(json!({"message": "success"}))).into_response()
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

/// Generic success acknowledgement for fire-and-forget endpoints.
async fn empty_ok() -> impl IntoResponse {
    (StatusCode::OK, Json(json!({"message": "success"})))
}

// ---------------------------------------------------------------------------
// Plugin registry (`/plugins/functions`, `/plugins/udfs`).
// ---------------------------------------------------------------------------

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

async fn delete_function_plugin(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> Response {
    delete_typed_plugin(&state, &name).await
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

async fn create_plugin_of_type(state: &AppState, plugin_type: &str, payload: Value) -> Response {
    let name = payload
        .get("name")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    if let Err(resp) = check_valid_name(&name) {
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
        None => (StatusCode::NOT_FOUND, format!("Plugin {} not found", name)).into_response(),
    }
}

async fn delete_typed_plugin(state: &AppState, name: &str) -> Response {
    if let Err(resp) = check_valid_name(name) {
        return resp;
    }
    // Idempotent like the other drop endpoints: missing plugins still 200.
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

async fn get_connection_yaml(Path(name): Path<String>) -> Response {
    if let Err(resp) = check_valid_name(&name) {
        return resp;
    }
    let found = find_etc_file(&format!("connections/{}.yaml", name))
        .or_else(|| find_etc_file("connections/connection.yaml"));

    if let Some(path) = found {
        if let Ok(content) = std::fs::read_to_string(&path) {
            return Json(json!({ "yaml": content })).into_response();
        }
    }
    Json(json!({ "yaml": "" })).into_response()
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
    Json(json!({ "name": name, "about": {} })).into_response()
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
    Json(json!({ "name": name, "about": {} })).into_response()
}

async fn get_source_yaml(Path(name): Path<String>) -> Response {
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

    if let Some(path) = found {
        if let Ok(content) = std::fs::read_to_string(&path) {
            return Json(json!({ "yaml": content })).into_response();
        }
    }
    Json(json!({ "yaml": "" })).into_response()
}

async fn get_sink_yaml(Path(name): Path<String>) -> Response {
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

    if let Some(path) = found {
        if let Ok(content) = std::fs::read_to_string(&path) {
            return Json(json!({ "yaml": content })).into_response();
        }
    }
    Json(json!({ "yaml": "" })).into_response()
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
        .insert(format!("{}/{}", name, conf_key), payload);
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
        .insert(format!("{}/{}", name, conf_key), payload);
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
    let mut conns = state.connections.write();
    conns.insert(format!("{}.{}", name, conf_key), payload.clone());
    conns.insert(format!("{}/{}", name, conf_key), payload);
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
    let mut conns = state.connections.write();
    conns.remove(&format!("{}.{}", name, conf_key));
    conns.remove(&format!("{}/{}", name, conf_key));
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
    for rule in state.rule_manager.list_rules() {
        if !target_tags.is_empty() && !target_tags.iter().any(|t| rule.tags.contains(t)) {
            continue;
        }
        let _ = state.rule_manager.start_rule(&rule.id).await;
    }
    (StatusCode::OK, Json(json!({})))
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
    for rule in state.rule_manager.list_rules() {
        if !target_tags.is_empty() && !target_tags.iter().any(|t| rule.tags.contains(t)) {
            continue;
        }
        let _ = state.rule_manager.stop_rule(&rule.id).await;
        cancel_rule_source(&state, &rule.id);
    }
    (StatusCode::OK, Json(json!({})))
}

async fn reset_rule_state(State(state): State<AppState>, Path(name): Path<String>) -> Response {
    if let Err(resp) = check_valid_name(&name) {
        return resp;
    }
    match state.rule_manager.reset_rule_metrics(&name) {
        Ok(_) => (StatusCode::OK, Json(json!({}))).into_response(),
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
        (StatusCode::OK, Json(json!({}))).into_response()
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
    Json(Value::Array(Vec::new()))
}

async fn stop_server() -> impl IntoResponse {
    (StatusCode::OK, "Server is shutting down\n")
}

async fn import_status() -> impl IntoResponse {
    Json(json!({ "status": "completed" }))
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
    let id = payload.id.unwrap_or_else(generate_ruletest_id);
    let (output_tx, _) = tokio::sync::broadcast::channel::<String>(256);
    state.ruletests.write().insert(
        id.clone(),
        RuletestSession {
            id: id.clone(),
            sql: payload.sql.unwrap_or_default(),
            mock_source: payload.mock_source,
            output_tx,
        },
    );
    (
        StatusCode::OK,
        Json(json!({ "id": id, "port": state.config.basic.port })),
    )
        .into_response()
}

async fn start_ruletest(State(state): State<AppState>, Path(name): Path<String>) -> Response {
    let Some(session) = state.ruletests.read().get(&name).cloned() else {
        // Keep the endpoint total: unknown sessions are still acknowledged.
        return (StatusCode::OK, "started\n").into_response();
    };
    tokio::spawn(async move {
        let mut parser = Parser::new(&session.sql);
        let Ok(select_stmt) = parser.parse_select() else {
            return;
        };
        // Replay the mock data registered for the rule's source stream.
        let data: Vec<HashMap<String, Value>> = session
            .mock_source
            .get(&select_stmt.from)
            .map(|conf| conf.data.clone())
            .unwrap_or_default();
        let rule_state = RuleState::default();
        for record in &data {
            for row in Evaluator::eval_select_stateful_multi(&select_stmt, record, &rule_state) {
                let line = serde_json::to_string(&row).unwrap_or_default();
                let _ = session.output_tx.send(line);
            }
        }
    });
    (StatusCode::OK, "started\n").into_response()
}

async fn delete_ruletest(State(state): State<AppState>, Path(name): Path<String>) -> Response {
    state.ruletests.write().remove(&name);
    (StatusCode::OK, "dropped\n").into_response()
}

async fn sse_ruletest(State(state): State<AppState>, Path(name): Path<String>) -> Response {
    let Some(session) = state.ruletests.read().get(&name).cloned() else {
        return (
            StatusCode::NOT_FOUND,
            format!("Ruletest {} not found", name),
        )
            .into_response();
    };
    let rx = session.output_tx.subscribe();
    let stream = futures::stream::unfold(rx, |mut rx| async move {
        match rx.recv().await {
            Ok(line) => Some((Ok::<_, axum::Error>(Event::default().data(line)), rx)),
            Err(_) => None,
        }
    });
    Sse::new(stream)
        .keep_alive(KeepAlive::new())
        .into_response()
}
