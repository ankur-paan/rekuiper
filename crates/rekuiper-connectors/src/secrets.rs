use anyhow::{bail, Context, Result};
use parking_lot::RwLock;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

#[derive(Clone, Debug)]
struct CachedSecret {
    value: String,
    expires_at: Instant,
}

/// Dynamic Secret Resolver supporting HashiCorp Vault (`vault://`) and Environment variables (`env://`).
///
/// Features:
/// - Supports HashiCorp Vault KV v1 and KV v2 secret engines.
/// - Authenticates using `VAULT_ADDR`, `VAULT_TOKEN`, and optional `VAULT_NAMESPACE`.
/// - Automatic secret caching with configurable TTL (default: 5 minutes) to avoid Vault rate-limiting.
/// - Supports `env://<VAR_NAME>` for secure local and container environments.
#[derive(Clone, Debug)]
pub struct SecretResolver {
    client: reqwest::Client,
    pub vault_addr: Option<String>,
    pub vault_token: Option<String>,
    pub vault_namespace: Option<String>,
    cache: Arc<RwLock<HashMap<String, CachedSecret>>>,
    ttl: Duration,
}

impl Default for SecretResolver {
    fn default() -> Self {
        Self::from_env()
    }
}

impl SecretResolver {
    /// Create a resolver using environment configuration:
    /// - `VAULT_ADDR` (e.g. `http://127.0.0.1:8200` or `https://vault.example.com`)
    /// - `VAULT_TOKEN` (Vault client token)
    /// - `VAULT_NAMESPACE` (optional enterprise namespace)
    pub fn from_env() -> Self {
        let vault_addr = std::env::var("VAULT_ADDR")
            .ok()
            .map(|s| s.trim_end_matches('/').to_string());
        let vault_token = std::env::var("VAULT_TOKEN").ok();
        let vault_namespace = std::env::var("VAULT_NAMESPACE").ok();

        Self {
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(10))
                .build()
                .unwrap_or_else(|_| reqwest::Client::new()),
            vault_addr,
            vault_token,
            vault_namespace,
            cache: Arc::new(RwLock::new(HashMap::new())),
            ttl: Duration::from_secs(300),
        }
    }

    /// Create a resolver with explicit Vault connection parameters.
    pub fn new(
        vault_addr: Option<String>,
        vault_token: Option<String>,
        vault_namespace: Option<String>,
        ttl: Duration,
    ) -> Self {
        Self {
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(10))
                .build()
                .unwrap_or_else(|_| reqwest::Client::new()),
            vault_addr: vault_addr.map(|s| s.trim_end_matches('/').to_string()),
            vault_token,
            vault_namespace,
            cache: Arc::new(RwLock::new(HashMap::new())),
            ttl,
        }
    }

    /// Clears the in-memory secrets cache.
    pub fn clear_cache(&self) {
        self.cache.write().clear();
    }

    /// Resolves a secret reference string.
    ///
    /// Patterns:
    /// - `env://VAR_NAME`: reads from `std::env::var("VAR_NAME")`
    /// - `vault://secret/path#field`: reads from HashiCorp Vault
    /// - plain string: returned as-is
    pub async fn resolve(&self, secret_ref: &str) -> Result<String> {
        let trimmed = secret_ref.trim();
        if let Some(var_name) = trimmed.strip_prefix("env://") {
            return std::env::var(var_name).with_context(|| {
                format!(
                    "Environment variable '{}' referenced by secret not found",
                    var_name
                )
            });
        }

        if let Some(vault_target) = trimmed.strip_prefix("vault://") {
            return self.resolve_vault(vault_target).await;
        }

        if trimmed.contains("{{") && trimmed.contains("}}") {
            let mut result = trimmed.to_string();
            while let Some(start) = result.find("{{") {
                if let Some(end) = result[start..].find("}}") {
                    let end_pos = start + end;
                    let inner = result[start + 2..end_pos].trim();
                    let resolved = if let Some(var_name) = inner.strip_prefix("env://") {
                        std::env::var(var_name).with_context(|| {
                            format!(
                                "Environment variable '{}' referenced by secret not found",
                                var_name
                            )
                        })?
                    } else if let Some(vault_target) = inner.strip_prefix("vault://") {
                        self.resolve_vault(vault_target).await?
                    } else {
                        inner.to_string()
                    };
                    result.replace_range(start..end_pos + 2, &resolved);
                } else {
                    break;
                }
            }
            return Ok(result);
        }

        Ok(trimmed.to_string())
    }

    /// Helper for optional secret fields.
    pub async fn resolve_opt(&self, opt: Option<&str>) -> Result<Option<String>> {
        match opt {
            Some(s) => Ok(Some(self.resolve(s).await?)),
            None => Ok(None),
        }
    }

    /// Resolves a secret from HashiCorp Vault.
    /// Format: `path/to/secret#field_name` (e.g. `secret/data/mqtt#password` or `secret/mqtt#password`)
    async fn resolve_vault(&self, vault_target: &str) -> Result<String> {
        // Check cache first
        let cache_key = vault_target.to_string();
        {
            let cache = self.cache.read();
            if let Some(entry) = cache.get(&cache_key) {
                if Instant::now() < entry.expires_at {
                    return Ok(entry.value.clone());
                }
            }
        }

        let addr = self.vault_addr.as_deref().ok_or_else(|| {
            anyhow::anyhow!("Cannot resolve Vault secret '{}': VAULT_ADDR environment variable is not configured", vault_target)
        })?;

        let token = self.vault_token.as_deref().ok_or_else(|| {
            anyhow::anyhow!("Cannot resolve Vault secret '{}': VAULT_TOKEN environment variable is not configured", vault_target)
        })?;

        let (path, field) = match vault_target.split_once('#') {
            Some((p, f)) => (p.trim_start_matches('/'), f.trim()),
            None => (vault_target.trim_start_matches('/'), "value"),
        };

        // Standard Vault v1 URL: /v1/{path}
        let url = format!("{}/v1/{}", addr, path);
        let mut req = self.client.get(&url).header("X-Vault-Token", token);

        if let Some(ns) = &self.vault_namespace {
            req = req.header("X-Vault-Namespace", ns);
        }

        let resp = req
            .send()
            .await
            .with_context(|| format!("Failed to connect to Vault server at {}", url))?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            bail!(
                "Vault returned HTTP {} for path '{}': {}",
                status,
                path,
                body
            );
        }

        let json: Value = resp.json().await.with_context(|| {
            format!(
                "Failed to parse JSON response from Vault for path '{}'",
                path
            )
        })?;

        // Extract secret field handling both KV v2 and KV v1
        let val_found = if let Some(v2_data) = json.pointer("/data/data") {
            v2_data.get(field)
        } else if let Some(v1_data) = json.pointer("/data") {
            v1_data.get(field)
        } else {
            json.get(field)
        };

        let secret_value = match val_found {
            Some(Value::String(s)) => s.clone(),
            Some(Value::Number(n)) => n.to_string(),
            Some(Value::Bool(b)) => b.to_string(),
            Some(other) => other.to_string(),
            None => {
                bail!(
                    "Field '{}' not found in Vault secret at path '{}'",
                    field,
                    path
                );
            }
        };

        // Cache secret
        {
            let mut cache = self.cache.write();
            cache.insert(
                cache_key,
                CachedSecret {
                    value: secret_value.clone(),
                    expires_at: Instant::now() + self.ttl,
                },
            );
        }

        Ok(secret_value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_resolve_plain_string() {
        let resolver = SecretResolver::from_env();
        let res = resolver.resolve("plain_password_123").await.unwrap();
        assert_eq!(res, "plain_password_123");
    }

    #[tokio::test]
    async fn test_resolve_env_var() {
        std::env::set_var("REKUIPER_TEST_SECRET", "super_secret_token_abc");
        let resolver = SecretResolver::from_env();
        let res = resolver
            .resolve("env://REKUIPER_TEST_SECRET")
            .await
            .unwrap();
        assert_eq!(res, "super_secret_token_abc");
    }

    #[tokio::test]
    async fn test_resolve_env_var_missing() {
        let resolver = SecretResolver::from_env();
        let err = resolver.resolve("env://NON_EXISTENT_VAR_XYZ").await;
        assert!(err.is_err());
    }

    #[tokio::test]
    async fn test_resolve_vault_missing_addr() {
        let resolver = SecretResolver::new(None, None, None, Duration::from_secs(60));
        let err = resolver.resolve("vault://secret/mqtt#password").await;
        assert!(err.is_err());
        assert!(err.unwrap_err().to_string().contains("VAULT_ADDR"));
    }

    #[tokio::test]
    async fn test_resolve_template_env() {
        std::env::set_var("RMQ_USER", "app_operator");
        std::env::set_var("RMQ_PASS", "s3cr3t_p@ss!");
        let resolver = SecretResolver::from_env();
        let conn_str = "amqp://{{env://RMQ_USER}}:{{env://RMQ_PASS}}@127.0.0.1:5672/%2f";
        let res = resolver.resolve(conn_str).await.unwrap();
        assert_eq!(res, "amqp://app_operator:s3cr3t_p@ss!@127.0.0.1:5672/%2f");
    }
}
