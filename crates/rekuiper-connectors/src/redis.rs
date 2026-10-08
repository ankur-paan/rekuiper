use crate::META_KEY;
use anyhow::{Context, Result};
use rekuiper_core::model::StreamRecord;
use rekuiper_core::StreamSender;
use serde::{Deserialize, Serialize};
use serde_json::Value;

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

    pub async fn send_raw(&self, payload: &str, record: &StreamRecord) -> Result<()> {
        let mut conn = self.connection().await?;
        if let Some(topic) = self.config.topic.as_deref() {
            redis::cmd("PUBLISH")
                .arg(topic)
                .arg(payload)
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
            .arg(payload)
            .query_async::<()>(&mut conn)
            .await?;
        Ok(())
    }

    pub async fn send(&self, record: &StreamRecord) -> Result<()> {
        let mut map = std::collections::BTreeMap::new();
        for (k, v) in &record.data {
            if k == META_KEY || k.starts_with("__") {
                continue;
            }
            map.insert(k.clone(), v.clone());
        }
        let json_str = serde_json::to_string(&map)?;
        self.send_raw(&json_str, record).await
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
    pub tx: StreamSender,
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
                                    if self.tx.send(record).await.is_err() {
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
