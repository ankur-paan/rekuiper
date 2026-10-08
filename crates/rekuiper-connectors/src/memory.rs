use crate::Sink;
use anyhow::Result;
use async_trait::async_trait;
use parking_lot::RwLock;
use rekuiper_core::model::StreamRecord;
use std::sync::Arc;

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
