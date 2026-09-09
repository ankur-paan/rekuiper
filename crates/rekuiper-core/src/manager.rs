use crate::kv::KvStore;
use crate::model::{RuleDefinition, RuleStatus, StreamDefinition, TableDefinition};
use crate::runtime::StreamBus;
use anyhow::{bail, Result};
use parking_lot::RwLock;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::task::JoinHandle;

#[derive(Clone, Default)]
pub struct StreamManager {
    streams: Arc<RwLock<HashMap<String, StreamDefinition>>>,
    kv: Option<Arc<dyn KvStore>>,
}

impl StreamManager {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn new_with_kv(kv: Arc<dyn KvStore>) -> Self {
        Self {
            streams: Arc::new(RwLock::new(HashMap::new())),
            kv: Some(kv),
        }
    }

    async fn persist(&self, namespace: &str, key: &str, val: &str) {
        if let Some(kv) = &self.kv {
            if let Err(e) = kv.set(namespace, key, val).await {
                tracing::warn!("KV persist {}/{} failed: {}", namespace, key, e);
            }
        }
    }

    async fn unpersist(&self, namespace: &str, key: &str) {
        if let Some(kv) = &self.kv {
            if let Err(e) = kv.delete(namespace, key).await {
                tracing::warn!("KV delete {}/{} failed: {}", namespace, key, e);
            }
        }
    }

    pub async fn create_stream(&self, def: StreamDefinition) -> Result<()> {
        let (name, snapshot) = {
            let mut map = self.streams.write();
            if map.contains_key(&def.name) {
                bail!("Stream {} already exists", def.name);
            }
            map.insert(def.name.clone(), def.clone());
            (
                def.name.clone(),
                serde_json::to_string(&def).unwrap_or_default(),
            )
        };
        self.persist("streams", &name, &snapshot).await;
        Ok(())
    }

    pub fn get_stream(&self, name: &str) -> Option<StreamDefinition> {
        self.streams.read().get(name).cloned()
    }

    pub fn list_streams(&self) -> Vec<String> {
        self.streams.read().keys().cloned().collect()
    }

    pub async fn delete_stream(&self, name: &str) -> Result<()> {
        {
            let mut map = self.streams.write();
            if map.remove(name).is_none() {
                bail!("Stream {} not found", name);
            }
        }
        self.unpersist("streams", name).await;
        Ok(())
    }

    /// Repopulates definitions previously stored under the `streams`
    /// namespace, skipping corrupt entries.
    pub async fn load_from_kv(&self, kv: &Arc<dyn KvStore>) -> Result<()> {
        for (key, val) in kv.list_all("streams").await? {
            match serde_json::from_str::<StreamDefinition>(&val) {
                Ok(def) => {
                    self.streams.write().insert(def.name.clone(), def);
                }
                Err(e) => {
                    tracing::warn!("Skipping corrupt stream entry {}: {}", key, e);
                }
            }
        }
        Ok(())
    }
}

pub type TableRow = HashMap<String, Value>;

#[derive(Clone, Default)]
pub struct TableManager {
    tables: Arc<RwLock<HashMap<String, TableDefinition>>>,
    rows: Arc<RwLock<HashMap<String, Vec<TableRow>>>>,
    kv: Option<Arc<dyn KvStore>>,
}

impl TableManager {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn new_with_kv(kv: Arc<dyn KvStore>) -> Self {
        Self {
            tables: Arc::new(RwLock::new(HashMap::new())),
            rows: Arc::new(RwLock::new(HashMap::new())),
            kv: Some(kv),
        }
    }

    async fn persist(&self, namespace: &str, key: &str, val: &str) {
        if let Some(kv) = &self.kv {
            if let Err(e) = kv.set(namespace, key, val).await {
                tracing::warn!("KV persist {}/{} failed: {}", namespace, key, e);
            }
        }
    }

    async fn unpersist(&self, namespace: &str, key: &str) {
        if let Some(kv) = &self.kv {
            if let Err(e) = kv.delete(namespace, key).await {
                tracing::warn!("KV delete {}/{} failed: {}", namespace, key, e);
            }
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

    pub async fn create_table(&self, def: TableDefinition) -> Result<()> {
        let (name, snapshot) = {
            let mut map = self.tables.write();
            if map.contains_key(&def.name) {
                bail!("Table {} already exists", def.name);
            }
            map.insert(def.name.clone(), def.clone());
            (
                def.name.clone(),
                serde_json::to_string(&def).unwrap_or_default(),
            )
        };
        self.persist("tables", &name, &snapshot).await;
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

    pub async fn delete_table(&self, name: &str) -> Result<()> {
        {
            let mut map = self.tables.write();
            if map.remove(name).is_none() {
                bail!("Table {} not found", name);
            }
        }
        self.unpersist("tables", name).await;
        Ok(())
    }

    /// Repopulates table definitions previously stored under the `tables`
    /// namespace, skipping corrupt entries. Lookup rows are runtime data and
    /// are intentionally not restored.
    pub async fn load_from_kv(&self, kv: &Arc<dyn KvStore>) -> Result<()> {
        for (key, val) in kv.list_all("tables").await? {
            match serde_json::from_str::<TableDefinition>(&val) {
                Ok(def) => {
                    self.tables.write().insert(def.name.clone(), def);
                }
                Err(e) => {
                    tracing::warn!("Skipping corrupt table entry {}: {}", key, e);
                }
            }
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
    kv: Option<Arc<dyn KvStore>>,
}

/// Persisted rule envelope: the definition plus the last known lifecycle
/// status, so a restarted daemon knows which rules to resume.
#[derive(serde::Serialize, serde::Deserialize)]
struct PersistedRule {
    def: RuleDefinition,
    status: String,
}

impl RuleManager {
    pub fn new(stream_bus: StreamBus) -> Self {
        Self {
            rules: Arc::new(RwLock::new(HashMap::new())),
            stream_bus,
            kv: None,
        }
    }

    pub fn new_with_kv(stream_bus: StreamBus, kv: Arc<dyn KvStore>) -> Self {
        Self {
            rules: Arc::new(RwLock::new(HashMap::new())),
            stream_bus,
            kv: Some(kv),
        }
    }

    async fn persist_rule(&self, id: &str, def: &RuleDefinition, status: &str) {
        if let Some(kv) = &self.kv {
            let envelope = serde_json::json!({ "def": def, "status": status }).to_string();
            if let Err(e) = kv.set("rules", id, &envelope).await {
                tracing::warn!("KV persist rules/{} failed: {}", id, e);
            }
        }
    }

    async fn unpersist_rule(&self, id: &str) {
        if let Some(kv) = &self.kv {
            if let Err(e) = kv.delete("rules", id).await {
                tracing::warn!("KV delete rules/{} failed: {}", id, e);
            }
        }
    }

    pub async fn create_rule(&self, def: RuleDefinition) -> Result<()> {
        let snapshot = def.clone();
        {
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
        }
        self.persist_rule(&snapshot.id, &snapshot, "running").await;
        Ok(())
    }

    pub fn get_rule(&self, id: &str) -> Option<RuleDefinition> {
        self.rules.read().get(id).map(|r| r.read().def.clone())
    }

    pub fn list_rules(&self) -> Vec<RuleDefinition> {
        self.rules
            .read()
            .values()
            .map(|r| r.read().def.clone())
            .collect()
    }

    pub fn get_rule_status(&self, id: &str) -> Option<RuleStatus> {
        self.rules
            .read()
            .get(id)
            .map(|r| r.read().status.read().clone())
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
        let rule_arc = map
            .get(id)
            .ok_or_else(|| anyhow::anyhow!("Rule {} not found", id))?;
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

    pub async fn start_rule(&self, id: &str) -> Result<()> {
        let snapshot = {
            let map = self.rules.read();
            let rule_arc = map
                .get(id)
                .ok_or_else(|| anyhow::anyhow!("Rule {} not found", id))?;
            let rule = rule_arc.write();
            rule.status.write().status = "running".to_string();
            rule.def.clone()
        };
        self.persist_rule(id, &snapshot, "running").await;
        Ok(())
    }

    pub async fn stop_rule(&self, id: &str) -> Result<()> {
        let snapshot = {
            let map = self.rules.read();
            let rule_arc = map
                .get(id)
                .ok_or_else(|| anyhow::anyhow!("Rule {} not found", id))?;
            let mut rule = rule_arc.write();
            if let Some(handle) = rule.handle.take() {
                handle.abort();
            }
            rule.status.write().status = "stopped".to_string();
            rule.def.clone()
        };
        self.persist_rule(id, &snapshot, "stopped").await;
        Ok(())
    }

    pub async fn delete_rule(&self, id: &str) -> Result<()> {
        {
            let mut map = self.rules.write();
            if let Some(rule_arc) = map.remove(id) {
                let mut rule = rule_arc.write();
                if let Some(handle) = rule.handle.take() {
                    handle.abort();
                }
            } else {
                bail!("Rule {} not found", id);
            }
        }
        self.unpersist_rule(id).await;
        Ok(())
    }

    /// Repopulates rules previously stored under the `rules` namespace,
    /// restoring each persisted lifecycle status with fresh zeroed metrics.
    /// Unknown statuses are treated as stopped so nothing auto-starts that
    /// shouldn't.
    pub async fn load_from_kv(&self, kv: &Arc<dyn KvStore>) -> Result<()> {
        for (key, val) in kv.list_all("rules").await? {
            let stored: PersistedRule = match serde_json::from_str(&val) {
                Ok(stored) => stored,
                Err(e) => {
                    tracing::warn!("Skipping corrupt rule entry {}: {}", key, e);
                    continue;
                }
            };
            let status = if stored.status == "running" {
                "running"
            } else {
                "stopped"
            };
            let active = ActiveRule {
                def: stored.def,
                status: Arc::new(RwLock::new(RuleStatus {
                    status: status.to_string(),
                    message: String::new(),
                    source_records_in_total: 0,
                    sink_records_out_total: 0,
                    exceptions_total: 0,
                })),
                handle: None,
            };
            self.rules
                .write()
                .insert(active.def.id.clone(), Arc::new(RwLock::new(active)));
        }
        Ok(())
    }
}
