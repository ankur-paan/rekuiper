use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct KuiperConfig {
    #[serde(default)]
    pub basic: BasicConfig,
    #[serde(default)]
    pub store: StoreConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BasicConfig {
    #[serde(default = "default_ip")]
    pub ip: String,
    #[serde(default = "default_port")]
    pub port: u16,
    #[serde(default = "default_rest_ip")]
    pub rest_ip: String,
    #[serde(default = "default_rest_port")]
    pub rest_port: u16,
    #[serde(default = "default_log_level")]
    pub log_level: String,
    #[serde(default)]
    pub debug: bool,
    #[serde(default = "default_true")]
    pub console_log: bool,
    #[serde(default)]
    pub authentication: bool,
    #[serde(default = "default_timezone")]
    pub timezone: String,
    #[serde(default)]
    pub prometheus: bool,
    #[serde(default = "default_prometheus_port")]
    pub prometheus_port: u16,
}

fn default_ip() -> String {
    "127.0.0.1".to_string()
}
fn default_port() -> u16 {
    20498
}
fn default_rest_ip() -> String {
    "0.0.0.0".to_string()
}
fn default_rest_port() -> u16 {
    9081
}
fn default_log_level() -> String {
    "info".to_string()
}
fn default_true() -> bool {
    true
}
fn default_timezone() -> String {
    "Local".to_string()
}
fn default_prometheus_port() -> u16 {
    20499
}

impl Default for BasicConfig {
    fn default() -> Self {
        Self {
            ip: default_ip(),
            port: default_port(),
            rest_ip: default_rest_ip(),
            rest_port: default_rest_port(),
            log_level: default_log_level(),
            debug: false,
            console_log: true,
            authentication: false,
            timezone: default_timezone(),
            prometheus: false,
            prometheus_port: default_prometheus_port(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StoreConfig {
    #[serde(default = "default_store_type")]
    pub r#type: String,
}

fn default_store_type() -> String {
    "memory".to_string()
}

impl Default for StoreConfig {
    fn default() -> Self {
        Self {
            r#type: default_store_type(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct PathConfig {
    pub etc_dir: PathBuf,
    pub data_dir: PathBuf,
    pub log_dir: PathBuf,
}

impl PathConfig {
    pub fn new(etc_path: Option<&str>, data_path: Option<&str>, log_path: Option<&str>) -> Self {
        let etc_dir = etc_path
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("etc"));
        let data_dir = data_path
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("data"));
        let log_dir = log_path
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("log"));

        Self {
            etc_dir,
            data_dir,
            log_dir,
        }
    }

    pub fn load_config(&self) -> Result<KuiperConfig> {
        let conf_file = self.etc_dir.join("kuiper.yaml");
        if !conf_file.exists() {
            tracing::warn!(
                "Configuration file {:?} not found, using defaults",
                conf_file
            );
            let mut config = KuiperConfig::default();
            apply_env_overrides(&mut config);
            return Ok(config);
        }

        let content = std::fs::read_to_string(&conf_file)
            .with_context(|| format!("Failed to read configuration file: {:?}", conf_file))?;

        let mut config: KuiperConfig = serde_yaml::from_str(&content)
            .with_context(|| format!("Failed to parse YAML from {:?}", conf_file))?;

        apply_env_overrides(&mut config);
        Ok(config)
    }
}

fn parse_env_bool(val: &str) -> Option<bool> {
    match val.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Some(true),
        "0" | "false" | "no" | "off" => Some(false),
        _ => None,
    }
}

fn apply_basic_override(basic: &mut BasicConfig, key: &str, val: &str) {
    match key {
        "IP" => basic.ip = val.to_string(),
        "PORT" => {
            if let Ok(v) = val.trim().parse() {
                basic.port = v;
            }
        }
        "RESTIP" => basic.rest_ip = val.to_string(),
        "RESTPORT" => {
            if let Ok(v) = val.trim().parse() {
                basic.rest_port = v;
            }
        }
        "LOGLEVEL" => basic.log_level = val.to_string(),
        "DEBUG" => {
            if let Some(v) = parse_env_bool(val) {
                basic.debug = v;
            }
        }
        "CONSOLELOG" => {
            if let Some(v) = parse_env_bool(val) {
                basic.console_log = v;
            }
        }
        "AUTHENTICATION" => {
            if let Some(v) = parse_env_bool(val) {
                basic.authentication = v;
            }
        }
        "TIMEZONE" => basic.timezone = val.to_string(),
        "PROMETHEUS" => {
            if let Some(v) = parse_env_bool(val) {
                basic.prometheus = v;
            }
        }
        "PROMETHEUSPORT" => {
            if let Ok(v) = val.trim().parse() {
                basic.prometheus_port = v;
            }
        }
        _ => {}
    }
}

/// Override configuration from `KUIPER__<SECTION>__<KEY>` environment
/// variables (container deployments), accepting both `RESTIP`-style and
/// `REST_IP`-style spellings. Unknown sections and keys are ignored, and
/// unparseable numbers/bools leave the current value untouched.
pub fn apply_env_overrides(config: &mut KuiperConfig) {
    for (key, val) in std::env::vars() {
        let Some(rest) = key.strip_prefix("KUIPER__") else {
            continue;
        };
        let mut parts = rest.split("__");
        let (Some(section), Some(name)) = (parts.next(), parts.next()) else {
            continue;
        };
        if parts.next().is_some() {
            continue;
        }
        let flat: String = name.chars().filter(|c| *c != '_').collect();
        match (
            section.to_ascii_uppercase().as_str(),
            flat.to_ascii_uppercase().as_str(),
        ) {
            ("BASIC", key) => apply_basic_override(&mut config.basic, key, &val),
            ("STORE", "TYPE") => config.store.r#type = val,
            _ => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn env_overrides_basic_network_settings() {
        std::env::set_var("KUIPER__BASIC__RESTIP", "0.0.0.0");
        std::env::set_var("KUIPER__BASIC__REST_PORT", "19081");
        std::env::set_var("KUIPER__BASIC__PORT", "12001");
        std::env::set_var("KUIPER__BASIC__DEBUG", "true");
        let mut config = KuiperConfig::default();
        apply_env_overrides(&mut config);
        assert_eq!(config.basic.rest_ip, "0.0.0.0");
        assert_eq!(config.basic.rest_port, 19081);
        assert_eq!(config.basic.port, 12001);
        assert!(config.basic.debug);
        std::env::remove_var("KUIPER__BASIC__RESTIP");
        std::env::remove_var("KUIPER__BASIC__REST_PORT");
        std::env::remove_var("KUIPER__BASIC__PORT");
        std::env::remove_var("KUIPER__BASIC__DEBUG");
    }
}
