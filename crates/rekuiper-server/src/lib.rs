pub mod routes;

use anyhow::Result;
use parking_lot::RwLock;
use rekuiper_conf::KuiperConfig;
use rekuiper_core::{
    PluginManager, RuleManager, SchemaManager, SqliteKvStore, StreamBus, StreamManager,
    TableManager,
};
use routes::{create_router, prometheus_metrics_handler, restore_running_rules, AppState};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Instant;
use tower_http::cors::CorsLayer;
use tower_http::trace::TraceLayer;

pub async fn start_server(config: KuiperConfig, version: String) -> Result<()> {
    // Embedded KV persistence (eKuiper `data/sqliteKV.db`): definitions
    // survive daemon restarts and running rules resume automatically.
    let kv: Arc<dyn rekuiper_core::KvStore> =
        Arc::new(SqliteKvStore::new("data/sqliteKV.db").await?);
    let stream_bus = StreamBus::new();
    let stream_manager = StreamManager::new_with_kv(kv.clone());
    let rule_manager = RuleManager::new_with_kv(stream_bus.clone(), kv.clone());
    let table_manager = TableManager::new_with_kv(kv.clone());
    let schema_manager = SchemaManager::new_with_kv(kv.clone());
    let plugin_manager = PluginManager::new_with_kv(kv.clone());

    stream_manager.load_from_kv(&kv).await?;
    table_manager.load_from_kv(&kv).await?;
    rule_manager.load_from_kv(&kv).await?;
    schema_manager.load_from_kv(&kv).await?;
    plugin_manager.load_from_kv(&kv).await?;
    let proto_schemas = schema_manager.load_proto_dir(std::path::Path::new("etc/schemas"));
    if proto_schemas > 0 {
        tracing::info!("Loaded {} schemas from etc/schemas", proto_schemas);
    }

    let state = AppState {
        start_time: Instant::now(),
        version,
        config: config.clone(),
        stream_manager,
        table_manager,
        rule_manager,
        schema_manager,
        plugin_manager,
        stream_bus,
        connections: Arc::new(RwLock::new(HashMap::new())),
        source_configs: Arc::new(RwLock::new(HashMap::new())),
        sink_configs: Arc::new(RwLock::new(HashMap::new())),
        ruletests: Arc::new(RwLock::new(HashMap::new())),
        source_cancels: Arc::new(RwLock::new(HashMap::new())),
        http_client: reqwest::Client::builder()
            .tcp_nodelay(true)
            .build()
            .unwrap_or_default(),
        trace_manager: routes::TraceManager::new(),
        task_manager: routes::TaskManager::new(),
        portable_plugins: routes::create_default_portables(),
    };

    restore_running_rules(&state).await;

    let app = create_router(state.clone())
        .layer(CorsLayer::permissive())
        .layer(TraceLayer::new_for_http());

    // Dedicated Prometheus listener when enabled on its own port, mirroring
    // eKuiper's prome_init behaviour.
    if config.basic.prometheus && config.basic.prometheus_port != config.basic.rest_port {
        let prom_port = config.basic.prometheus_port;
        let prom_app =
            axum::Router::new().route("/metrics", axum::routing::get(prometheus_metrics_handler));
        let prom_app = prom_app.with_state(state.clone());
        tokio::spawn(async move {
            let addr = SocketAddr::from(([0, 0, 0, 0], prom_port));
            match tokio::net::TcpListener::bind(addr).await {
                Ok(listener) => {
                    tracing::info!(
                        "Serving prometheus metrics on port http://localhost:{}/metrics",
                        prom_port
                    );
                    if let Err(e) = axum::serve(listener, prom_app).await {
                        tracing::warn!("Prometheus metrics server error: {}", e);
                    }
                }
                Err(e) => {
                    tracing::warn!(
                        "Failed to bind prometheus metrics port {}: {}",
                        prom_port,
                        e
                    );
                }
            }
        });
    }

    let addr: SocketAddr = format!("{}:{}", config.basic.rest_ip, config.basic.rest_port)
        .parse()
        .unwrap_or_else(|_| SocketAddr::from(([0, 0, 0, 0], config.basic.rest_port)));

    let rest_http_type = "http";
    println!(
        "Serving kuiper (version - {}) on port {}, and restful api on {}://{}.",
        state.version, config.basic.port, rest_http_type, addr
    );

    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}
