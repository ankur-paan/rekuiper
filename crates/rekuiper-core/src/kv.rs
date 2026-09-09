//! Namespaced key/value persistence backing the managers.
//!
//! The SQLite implementation mirrors eKuiper's `data/sqliteKV.db`: a single
//! `kv(namespace, key, value)` table holding stream/table/rule definitions so
//! a restarted daemon reloads its catalog and resumes running rules.

use anyhow::{Context, Result};
use async_trait::async_trait;
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

#[async_trait]
pub trait KvStore: Send + Sync {
    async fn get(&self, namespace: &str, key: &str) -> Result<Option<String>>;
    async fn set(&self, namespace: &str, key: &str, val: &str) -> Result<()>;
    async fn delete(&self, namespace: &str, key: &str) -> Result<()>;
    async fn list_all(&self, namespace: &str) -> Result<Vec<(String, String)>>;
}

/// SQLite-backed [`KvStore`].
pub struct SqliteKvStore {
    pool: sqlx::sqlite::SqlitePool,
}

impl SqliteKvStore {
    /// Opens (creating parent directories and the file as needed) and
    /// migrates the `kv` table. Accepts a filesystem path such as
    /// `data/sqliteKV.db`, or `:memory:` for an ephemeral store.
    pub async fn new(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent)
                    .with_context(|| format!("Failed to create KV parent dirs for {:?}", path))?;
            }
        }
        // `mode=rwc` makes sqlx create a missing database file; `:memory:`
        // keeps its canonical URL form.
        let db_url = if path.as_os_str() == ":memory:" {
            "sqlite::memory:".to_string()
        } else {
            format!(
                "sqlite:{}?mode=rwc",
                path.to_string_lossy().replace('\\', "/")
            )
        };
        let pool = sqlx::sqlite::SqlitePoolOptions::new()
            .connect(&db_url)
            .await
            .with_context(|| format!("Failed to open KV database at {:?}", path))?;
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS kv (namespace TEXT NOT NULL, key TEXT NOT NULL, value TEXT NOT NULL, PRIMARY KEY (namespace, key))",
        )
        .execute(&pool)
        .await
        .context("Failed to migrate KV table")?;
        Ok(Self { pool })
    }
}

#[async_trait]
impl KvStore for SqliteKvStore {
    async fn get(&self, namespace: &str, key: &str) -> Result<Option<String>> {
        let row: Option<(String,)> =
            sqlx::query_as("SELECT value FROM kv WHERE namespace = ? AND key = ?")
                .bind(namespace)
                .bind(key)
                .fetch_optional(&self.pool)
                .await?;
        Ok(row.map(|(value,)| value))
    }

    async fn set(&self, namespace: &str, key: &str, val: &str) -> Result<()> {
        sqlx::query(
            "INSERT INTO kv (namespace, key, value) VALUES (?, ?, ?) ON CONFLICT(namespace, key) DO UPDATE SET value = excluded.value",
        )
        .bind(namespace)
        .bind(key)
        .bind(val)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn delete(&self, namespace: &str, key: &str) -> Result<()> {
        sqlx::query("DELETE FROM kv WHERE namespace = ? AND key = ?")
            .bind(namespace)
            .bind(key)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn list_all(&self, namespace: &str) -> Result<Vec<(String, String)>> {
        let rows: Vec<(String, String)> =
            sqlx::query_as("SELECT key, value FROM kv WHERE namespace = ? ORDER BY key")
                .bind(namespace)
                .fetch_all(&self.pool)
                .await?;
        Ok(rows)
    }
}

/// In-memory [`KvStore`] for unit tests and ephemeral daemons.
#[derive(Debug, Default, Clone)]
pub struct MemKvStore {
    data: Arc<parking_lot::RwLock<HashMap<(String, String), String>>>,
}

impl MemKvStore {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl KvStore for MemKvStore {
    async fn get(&self, namespace: &str, key: &str) -> Result<Option<String>> {
        Ok(self
            .data
            .read()
            .get(&(namespace.to_string(), key.to_string()))
            .cloned())
    }

    async fn set(&self, namespace: &str, key: &str, val: &str) -> Result<()> {
        self.data
            .write()
            .insert((namespace.to_string(), key.to_string()), val.to_string());
        Ok(())
    }

    async fn delete(&self, namespace: &str, key: &str) -> Result<()> {
        self.data
            .write()
            .remove(&(namespace.to_string(), key.to_string()));
        Ok(())
    }

    async fn list_all(&self, namespace: &str) -> Result<Vec<(String, String)>> {
        let mut out: Vec<(String, String)> = self
            .data
            .read()
            .iter()
            .filter(|((ns, _), _)| ns == namespace)
            .map(|((_, k), v)| (k.clone(), v.clone()))
            .collect();
        out.sort_by(|a, b| a.0.cmp(&b.0));
        Ok(out)
    }
}
