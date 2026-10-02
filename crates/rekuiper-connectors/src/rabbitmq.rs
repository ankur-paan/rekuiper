use anyhow::{Context, Result};
use async_trait::async_trait;
use futures::StreamExt;
use lapin::{
    options::*, types::FieldTable, BasicProperties, Connection, ConnectionProperties,
};
use rekuiper_core::model::StreamRecord;
use rekuiper_core::StreamSender;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::Mutex;

use crate::decode_payload_into;
use crate::secrets::SecretResolver;
use crate::{PayloadFormat, Sink};

/// RabbitMQ AMQP 0-9-1 connection and routing configuration.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RabbitMqConfig {
    /// AMQP broker URL, e.g. `amqp://guest:guest@127.0.0.1:5672/%2f` or `amqps://...`.
    #[serde(default = "default_rabbitmq_server", alias = "url")]
    pub server: String,
    /// Exchange name to bind to or publish to. Defaults to empty string (default direct exchange).
    #[serde(default)]
    pub exchange: String,
    /// Routing key for publishing or queue binding.
    #[serde(default, alias = "routingKey")]
    pub routing_key: String,
    /// Queue name for consuming or declaring.
    #[serde(default)]
    pub queue: String,
    /// Whether the queue/exchange is durable across broker restarts.
    #[serde(default = "default_true")]
    pub durable: bool,
    /// Whether the queue/exchange is auto-deleted when no longer in use.
    #[serde(default)]
    pub auto_delete: bool,
    /// Exclusive queue flag (used by only one connection).
    #[serde(default)]
    pub exclusive: bool,
    /// Consumer prefetch count (QoS).
    #[serde(default = "default_prefetch")]
    pub prefetch_count: u16,
    /// Payload decoding format (stream FORMAT).
    #[serde(skip)]
    pub format: PayloadFormat,
}

fn default_rabbitmq_server() -> String {
    "amqp://guest:guest@127.0.0.1:5672/%2f".to_string()
}

fn default_true() -> bool {
    true
}

fn default_prefetch() -> u16 {
    100
}

impl Default for RabbitMqConfig {
    fn default() -> Self {
        Self {
            server: default_rabbitmq_server(),
            exchange: String::new(),
            routing_key: String::new(),
            queue: String::new(),
            durable: true,
            auto_delete: false,
            exclusive: false,
            prefetch_count: 100,
            format: PayloadFormat::Json,
        }
    }
}

impl RabbitMqConfig {
    /// Dynamically resolve any secrets (e.g. `vault://` or `env://`) in server URL.
    pub async fn resolve_secrets(&mut self, resolver: &SecretResolver) -> Result<()> {
        self.server = resolver.resolve(&self.server).await?;
        Ok(())
    }
}

/// RabbitMQ AMQP 0-9-1 Streaming Source.
pub struct RabbitMqSource {
    pub config: RabbitMqConfig,
    pub tx: StreamSender,
}

impl RabbitMqSource {
    pub fn new(config: RabbitMqConfig, tx: StreamSender) -> Self {
        Self { config, tx }
    }

    /// Spawns background consumer task receiving AMQP deliveries and pushing to the stream pipeline.
    pub fn spawn(
        self,
        mut cancel_rx: tokio::sync::watch::Receiver<bool>,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let conn = match Connection::connect(
                &self.config.server,
                ConnectionProperties::default(),
            )
            .await
            {
                Ok(c) => c,
                Err(e) => {
                    tracing::warn!("RabbitMQ source failed to connect to {}: {}", self.config.server, e);
                    return;
                }
            };

            let channel = match conn.create_channel().await {
                Ok(ch) => ch,
                Err(e) => {
                    tracing::warn!("RabbitMQ source failed to create channel: {}", e);
                    return;
                }
            };

            // Set QoS prefetch
            let _ = channel
                .basic_qos(
                    self.config.prefetch_count,
                    BasicQosOptions::default(),
                )
                .await;

            // Declare queue if configured
            let queue_name = if !self.config.queue.is_empty() {
                let opts = QueueDeclareOptions {
                    durable: self.config.durable,
                    auto_delete: self.config.auto_delete,
                    exclusive: self.config.exclusive,
                    nowait: false,
                    passive: false,
                };
                match channel.queue_declare(&self.config.queue, opts, FieldTable::default()).await {
                    Ok(q) => q.name().to_string(),
                    Err(e) => {
                        tracing::warn!("RabbitMQ failed to declare queue {}: {}", self.config.queue, e);
                        return;
                    }
                }
            } else {
                String::new()
            };

            // Bind queue to exchange if exchange is specified
            if !self.config.exchange.is_empty() && !queue_name.is_empty() {
                let _ = channel
                    .queue_bind(
                        &queue_name,
                        &self.config.exchange,
                        &self.config.routing_key,
                        QueueBindOptions::default(),
                        FieldTable::default(),
                    )
                    .await;
            }

            let mut consumer = match channel
                .basic_consume(
                    &queue_name,
                    "rekuiper_consumer",
                    BasicConsumeOptions {
                        no_ack: true,
                        ..Default::default()
                    },
                    FieldTable::default(),
                )
                .await
            {
                Ok(c) => c,
                Err(e) => {
                    tracing::warn!("RabbitMQ basic_consume failed: {}", e);
                    return;
                }
            };

            loop {
                tokio::select! {
                    delivery_opt = consumer.next() => {
                        match delivery_opt {
                            Some(Ok(delivery)) => {
                                let payload = &delivery.data;
                                let mut records = Vec::new();
                                match decode_payload_into(&self.config.format, payload, None, &mut records) {
                                    Ok(_) => {
                                        for record in records {
                                            if self.tx.send(record).await.is_err() {
                                                return;
                                            }
                                        }
                                    }
                                    Err(e) => {
                                        tracing::warn!("RabbitMQ payload decoding error: {}", e);
                                    }
                                }
                            }
                            Some(Err(e)) => {
                                tracing::warn!("RabbitMQ delivery error: {}", e);
                                break;
                            }
                            None => break,
                        }
                    }
                    exit = cancel_rx.changed() => {
                        if exit.is_err() || *cancel_rx.borrow() {
                            return;
                        }
                    }
                }
            }
        })
    }
}

/// RabbitMQ AMQP 0-9-1 Streaming Sink.
pub struct RabbitMqSink {
    pub config: RabbitMqConfig,
    channel: Arc<Mutex<Option<lapin::Channel>>>,
}

impl RabbitMqSink {
    pub fn new(config: RabbitMqConfig) -> Self {
        Self {
            config,
            channel: Arc::new(Mutex::new(None)),
        }
    }

    async fn get_or_connect(&self) -> Result<lapin::Channel> {
        let mut lock = self.channel.lock().await;
        if let Some(ch) = &*lock {
            if ch.status().connected() {
                return Ok(ch.clone());
            }
        }

        let conn = Connection::connect(
            &self.config.server,
            ConnectionProperties::default(),
        )
        .await
        .with_context(|| format!("Failed to connect to RabbitMQ server at {}", self.config.server))?;

        let channel = conn.create_channel().await.context("Failed to create RabbitMQ channel")?;
        *lock = Some(channel.clone());
        Ok(channel)
    }
}

#[async_trait]
impl Sink for RabbitMqSink {
    async fn send(&self, record: &StreamRecord) -> Result<()> {
        let channel = self.get_or_connect().await?;
        let payload = serde_json::to_vec(&record.data)?;

        channel
            .basic_publish(
                &self.config.exchange,
                &self.config.routing_key,
                BasicPublishOptions::default(),
                &payload,
                BasicProperties::default(),
            )
            .await
            .context("Failed to publish message to RabbitMQ")?
            .await
            .context("Failed to confirm RabbitMQ publish")?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rabbitmq_config_defaults() {
        let json = r#"{"server": "amqp://user:pass@127.0.0.1:5672", "exchange": "events", "routingKey": "sensor.temp"}"#;
        let cfg: RabbitMqConfig = serde_json::from_str(json).expect("deserialize");
        assert_eq!(cfg.server, "amqp://user:pass@127.0.0.1:5672");
        assert_eq!(cfg.exchange, "events");
        assert_eq!(cfg.routing_key, "sensor.temp");
        assert!(cfg.durable);
        assert_eq!(cfg.prefetch_count, 100);
    }
}
