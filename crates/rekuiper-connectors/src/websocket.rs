use crate::META_KEY;
use anyhow::{Context, Result};
use rekuiper_core::model::StreamRecord;
use rekuiper_core::StreamSender;
use serde::{Deserialize, Serialize};
use serde_json::Value;

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
    pub tx: StreamSender,
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
                                            if self.tx.send(record).await.is_err() {
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
        let mut map = std::collections::BTreeMap::new();
        for (k, v) in &record.data {
            if k == META_KEY || k.starts_with("__") {
                continue;
            }
            map.insert(k.clone(), v.clone());
        }
        let json_str = serde_json::to_string(&map)?;
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
