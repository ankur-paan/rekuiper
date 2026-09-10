//! Extensible plugin and user-defined function (UDF) engine.
//!
//! [`PluginManager`] tracks installed plugin *definitions* (persisted under
//! the `"plugins"` KV namespace) plus the in-process scalar function
//! handlers those plugins provide. A process-wide [`get_global_udf_registry`]
//! lets the SQL evaluator resolve names that are not built-in functions.
//! Note that only definitions persist across restarts — handlers are code
//! and must be re-registered by the host on bootstrap.

use crate::kv::KvStore;
use anyhow::{bail, Result};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::{Arc, LazyLock};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PluginDefinition {
    pub name: String,
    #[serde(default)]
    pub plugin_type: String, // "function", "source", "sink", "udf"
    #[serde(default)]
    pub file: Option<String>,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub functions: Vec<String>,
}

/// A scalar UDF handler: pure function from evaluated arguments to a value.
pub type UdfFn = Arc<dyn Fn(&[Value]) -> Value + Send + Sync>;

fn normalize_udf_name(name: &str) -> String {
    name.trim().to_ascii_lowercase()
}

#[derive(Clone, Default)]
pub struct PluginManager {
    plugins: Arc<RwLock<HashMap<String, PluginDefinition>>>,
    handlers: Arc<RwLock<HashMap<String, UdfFn>>>,
    kv: Option<Arc<dyn KvStore>>,
}

impl PluginManager {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn new_with_kv(kv: Arc<dyn KvStore>) -> Self {
        Self {
            plugins: Arc::new(RwLock::new(HashMap::new())),
            handlers: Arc::new(RwLock::new(HashMap::new())),
            kv: Some(kv),
        }
    }

    pub async fn register_plugin(&self, def: PluginDefinition) -> Result<()> {
        if def.name.trim().is_empty() {
            bail!("Plugin name must not be empty");
        }
        let snapshot = serde_json::to_string(&def).unwrap_or_default();
        self.plugins.write().insert(def.name.clone(), def.clone());
        if let Some(kv) = &self.kv {
            if let Err(e) = kv.set("plugins", &def.name, &snapshot).await {
                tracing::warn!("KV persist plugins/{} failed: {}", def.name, e);
            }
        }
        Ok(())
    }

    pub fn get_plugin(&self, name: &str) -> Option<PluginDefinition> {
        self.plugins.read().get(name).cloned()
    }

    /// Definitions of exactly `plugin_type`, sorted by name for determinism.
    pub fn list_plugins(&self, plugin_type: &str) -> Vec<PluginDefinition> {
        let mut out: Vec<PluginDefinition> = self
            .plugins
            .read()
            .values()
            .filter(|def| def.plugin_type == plugin_type)
            .cloned()
            .collect();
        out.sort_by(|a, b| a.name.cmp(&b.name));
        out
    }

    pub async fn delete_plugin(&self, name: &str) -> Result<()> {
        if self.plugins.write().remove(name).is_none() {
            bail!("Plugin {} not found", name);
        }
        if let Some(kv) = &self.kv {
            if let Err(e) = kv.delete("plugins", name).await {
                tracing::warn!("KV delete plugins/{} failed: {}", name, e);
            }
        }
        Ok(())
    }

    /// Register (or replace) an in-process scalar handler. Names are matched
    /// case-insensitively, mirroring SQL function resolution.
    pub fn register_udf(&self, name: &str, handler: UdfFn) {
        self.handlers
            .write()
            .insert(normalize_udf_name(name), handler);
    }

    /// Unregister an in-process scalar handler.
    pub fn unregister_udf(&self, name: &str) {
        self.handlers.write().remove(&normalize_udf_name(name));
    }

    pub fn call_udf(&self, name: &str, args: &[Value]) -> Option<Value> {
        self.handlers
            .read()
            .get(&normalize_udf_name(name))
            .map(|handler| handler(args))
    }

    /// Whether an in-process scalar handler is registered under `name`
    /// (matched case-insensitively, like resolution).
    pub fn has_udf(&self, name: &str) -> bool {
        self.handlers.read().contains_key(&normalize_udf_name(name))
    }

    /// Repopulates plugin definitions previously stored under the `plugins`
    /// namespace, skipping corrupt entries. Handlers are code and are never
    /// persisted — re-register them after loading.
    pub async fn load_from_kv(&self, kv: &Arc<dyn KvStore>) -> Result<()> {
        for (key, val) in kv.list_all("plugins").await? {
            match serde_json::from_str::<PluginDefinition>(&val) {
                Ok(def) => {
                    self.plugins.write().insert(key, def);
                }
                Err(e) => {
                    tracing::warn!("Skipping corrupt plugin entry {}: {}", key, e);
                }
            }
        }
        Ok(())
    }
}

static GLOBAL_UDF_REGISTRY: LazyLock<PluginManager> = LazyLock::new(PluginManager::new);

/// Process-wide UDF registry consulted by the SQL evaluator for function
/// names outside its built-in match list.
pub fn get_global_udf_registry() -> &'static PluginManager {
    &GLOBAL_UDF_REGISTRY
}
