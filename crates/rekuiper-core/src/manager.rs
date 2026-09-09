use std::collections::HashMap;
use std::sync::Arc;
use anyhow::{bail, Result};
use serde_json::Value;
use parking_lot::RwLock;
use tokio::task::JoinHandle;
use crate::model::{RuleDefinition, RuleStatus, StreamDefinition, TableDefinition};
use crate::runtime::StreamBus;

#[derive(Clone, Default)]
pub struct StreamManager {
    streams: Arc<RwLock<HashMap<String, StreamDefinition>>>,
}

impl StreamManager {
    pub fn new() -> Self {
        Self {
            streams: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn create_stream(&self, def: StreamDefinition) -> Result<()> {
        let mut map = self.streams.write();
        if map.contains_key(&def.name) {
            bail!("Stream {} already exists", def.name);
        }
        map.insert(def.name.clone(), def);
        Ok(())
    }

    pub fn get_stream(&self, name: &str) -> Option<StreamDefinition> {
        self.streams.read().get(name).cloned()
    }

    pub fn list_streams(&self) -> Vec<String> {
        self.streams.read().keys().cloned().collect()
    }

    pub fn delete_stream(&self, name: &str) -> Result<()> {
        let mut map = self.streams.write();
        if map.remove(name).is_none() {
            bail!("Stream {} not found", name);
        }
        Ok(())
    }
}

#[derive(Clone, Default)]
pub struct TableManager {
    tables: Arc<RwLock<HashMap<String, TableDefinition>>>,
    rows: Arc<RwLock<HashMap<String, Vec<HashMap<String, Value>>>>>,
}

impl TableManager {
    pub fn new() -> Self {
        Self {
            tables: Arc::new(RwLock::new(HashMap::new())),
            rows: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Appends a lookup row to a table (creates the row list on demand,
    /// even if the table definition itself was never registered).
    pub fn insert_table_row(&self, table: &str, row: HashMap<String, Value>) {
        self.rows
            .write()
            .entry(table.to_string())
            .or_default()
            .push(row);
    }

    /// Returns all lookup rows stored for a table (empty when none).
    pub fn get_table_rows(&self, table: &str) -> Vec<HashMap<String, Value>> {
        self.rows.read().get(table).cloned().unwrap_or_default()
    }

    pub fn create_table(&self, def: TableDefinition) -> Result<()> {
        let mut map = self.tables.write();
        if map.contains_key(&def.name) {
            bail!("Table {} already exists", def.name);
        }
        map.insert(def.name.clone(), def);
        Ok(())
    }

    pub fn get_table(&self, name: &str) -> Option<TableDefinition> {
        self.tables.read().get(name).cloned()
    }

    pub fn list_tables(&self) -> Vec<String> {
        self.tables.read().keys().cloned().collect()
    }

    pub fn list_table_definitions(&self) -> Vec<TableDefinition> {
        self.tables.read().values().cloned().collect()
    }

    pub fn delete_table(&self, name: &str) -> Result<()> {
        let mut map = self.tables.write();
        if map.remove(name).is_none() {
            bail!("Table {} not found", name);
        }
        Ok(())
    }
}

pub struct ActiveRule {
    pub def: RuleDefinition,
    pub status: Arc<RwLock<RuleStatus>>,
    pub handle: Option<JoinHandle<()>>,
}

#[derive(Clone)]
pub struct RuleManager {
    rules: Arc<RwLock<HashMap<String, Arc<RwLock<ActiveRule>>>>>,
    pub stream_bus: StreamBus,
}

impl RuleManager {
    pub fn new(stream_bus: StreamBus) -> Self {
        Self {
            rules: Arc::new(RwLock::new(HashMap::new())),
            stream_bus,
        }
    }

    pub fn create_rule(&self, def: RuleDefinition) -> Result<()> {
        let mut map = self.rules.write();
        if map.contains_key(&def.id) {
            bail!("Rule {} already exists", def.id);
        }

        let rule_id = def.id.clone();
        let active = ActiveRule {
            def,
            status: Arc::new(RwLock::new(RuleStatus {
                status: "running".to_string(),
                message: "".to_string(),
                source_records_in_total: 0,
                sink_records_out_total: 0,
                exceptions_total: 0,
            })),
            handle: None,
        };

        map.insert(rule_id, Arc::new(RwLock::new(active)));
        Ok(())
    }

    pub fn get_rule(&self, id: &str) -> Option<RuleDefinition> {
        self.rules.read().get(id).map(|r| r.read().def.clone())
    }

    pub fn list_rules(&self) -> Vec<RuleDefinition> {
        self.rules.read().values().map(|r| r.read().def.clone()).collect()
    }

    pub fn get_rule_status(&self, id: &str) -> Option<RuleStatus> {
        self.rules.read().get(id).map(|r| r.read().status.read().clone())
    }

    pub fn inc_source_records(&self, id: &str, count: u64) {
        let map = self.rules.read();
        if let Some(rule) = map.get(id) {
            rule.read().status.write().source_records_in_total += count;
        }
    }

    pub fn inc_sink_records(&self, id: &str, count: u64) {
        let map = self.rules.read();
        if let Some(rule) = map.get(id) {
            rule.read().status.write().sink_records_out_total += count;
        }
    }

    pub fn inc_exceptions(&self, id: &str, count: u64) {
        let map = self.rules.read();
        if let Some(rule) = map.get(id) {
            rule.read().status.write().exceptions_total += count;
        }
    }

    pub fn reset_rule_metrics(&self, id: &str) -> Result<()> {
        let map = self.rules.read();
        let rule_arc = map.get(id).ok_or_else(|| anyhow::anyhow!("Rule {} not found", id))?;
        let status = rule_arc.read().status.clone();
        let mut guard = status.write();
        guard.source_records_in_total = 0;
        guard.sink_records_out_total = 0;
        guard.exceptions_total = 0;
        Ok(())
    }

    pub fn set_rule_handle(&self, id: &str, handle: JoinHandle<()>) {
        let map = self.rules.read();
        if let Some(rule) = map.get(id) {
            let mut active = rule.write();
            if let Some(old) = active.handle.replace(handle) {
                old.abort();
            }
        } else {
            handle.abort();
        }
    }

    pub fn start_rule(&self, id: &str) -> Result<()> {
        let map = self.rules.read();
        let rule_arc = map.get(id).ok_or_else(|| anyhow::anyhow!("Rule {} not found", id))?;
        let rule = rule_arc.write();
        rule.status.write().status = "running".to_string();
        Ok(())
    }

    pub fn stop_rule(&self, id: &str) -> Result<()> {
        let map = self.rules.read();
        let rule_arc = map.get(id).ok_or_else(|| anyhow::anyhow!("Rule {} not found", id))?;
        let mut rule = rule_arc.write();
        if let Some(handle) = rule.handle.take() {
            handle.abort();
        }
        rule.status.write().status = "stopped".to_string();
        Ok(())
    }

    pub fn delete_rule(&self, id: &str) -> Result<()> {
        let mut map = self.rules.write();
        if let Some(rule_arc) = map.remove(id) {
            let mut rule = rule_arc.write();
            if let Some(handle) = rule.handle.take() {
                handle.abort();
            }
            Ok(())
        } else {
            bail!("Rule {} not found", id);
        }
    }
}
