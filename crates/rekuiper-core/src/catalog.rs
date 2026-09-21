//! Preloaded in-memory catalog holding all metadata, configs, and definitions in RAM.
//!
//! Eliminates runtime filesystem reads and SQLite lookups from the streaming hot path.

use crate::model::{RuleDefinition, SchemaDefinition, StreamDefinition, TableDefinition};
use parking_lot::RwLock;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;

/// In-memory catalog snapshot representing the entire application configuration and definitions.
#[derive(Debug, Clone, Default)]
pub struct MemoryCatalog {
    pub streams: HashMap<String, StreamDefinition>,
    pub tables: HashMap<String, TableDefinition>,
    pub rules: HashMap<String, RuleDefinition>,
    pub schemas: HashMap<String, SchemaDefinition>,
    pub source_configs: HashMap<String, Value>,
    pub sink_configs: HashMap<String, Value>,
    pub connections: HashMap<String, Value>,
    pub auth_public_key_der: Option<Vec<u8>>,
}

impl MemoryCatalog {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn get_stream(&self, name: &str) -> Option<&StreamDefinition> {
        self.streams.get(name)
    }

    pub fn get_table(&self, name: &str) -> Option<&TableDefinition> {
        self.tables.get(name)
    }

    pub fn get_rule(&self, id: &str) -> Option<&RuleDefinition> {
        self.rules.get(id)
    }

    pub fn get_source_config(&self, key: &str) -> Option<&Value> {
        self.source_configs.get(key)
    }

    pub fn get_sink_config(&self, key: &str) -> Option<&Value> {
        self.sink_configs.get(key)
    }

    pub fn get_connection(&self, key: &str) -> Option<&Value> {
        self.connections.get(key)
    }
}

/// Thread-safe handle to the shared in-memory catalog.
#[derive(Debug, Clone, Default)]
pub struct SharedCatalog {
    inner: Arc<RwLock<MemoryCatalog>>,
}

impl SharedCatalog {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn from_catalog(catalog: MemoryCatalog) -> Self {
        Self {
            inner: Arc::new(RwLock::new(catalog)),
        }
    }

    pub fn read(&self) -> parking_lot::RwLockReadGuard<'_, MemoryCatalog> {
        self.inner.read()
    }

    pub fn write(&self) -> parking_lot::RwLockWriteGuard<'_, MemoryCatalog> {
        self.inner.write()
    }

    pub fn snapshot(&self) -> MemoryCatalog {
        self.inner.read().clone()
    }
}
