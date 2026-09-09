pub mod routes;

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Instant;
use parking_lot::RwLock;
use anyhow::Result;
use tower_http::cors::CorsLayer;
use tower_http::trace::TraceLayer;
use rekuiper_conf::KuiperConfig;
use rekuiper_core::{RuleManager, StreamBus, StreamManager, TableManager};
use routes::{create_router, prometheus_metrics_handler, AppState};

pub async fn start_server(config: KuiperConfig, version: String) -> Result<()> {
    let stream_bus = StreamBus::new();
    let stream_manager = StreamManager::new();
    let rule_manager = RuleManager::new(stream_bus.clone());

    let state = AppState {
        start_time: Instant::now(),
        version,
        config: config.clone(),
        stream_manager,
        table_manager: TableManager::new(),
        rule_manager,
        stream_bus,
        connections: Arc::new(RwLock::new(HashMap::new())),
        source_configs: Arc::new(RwLock::new(HashMap::new())),
        ruletests: Arc::new(RwLock::new(HashMap::new())),
        source_cancels: Arc::new(RwLock::new(HashMap::new())),
        http_client: reqwest::Client::builder()
            .tcp_nodelay(true)
            .build()
            .unwrap_or_default(),
    };

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
                    tracing::warn!("Failed to bind prometheus metrics port {}: {}", prom_port, e);
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
