use std::path::PathBuf;
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KuiperConfig {
    #[serde(default)]
    pub basic: BasicConfig,
    #[serde(default)]
    pub store: StoreConfig,
}

impl Default for KuiperConfig {
    fn default() -> Self {
        Self {
            basic: BasicConfig::default(),
            store: StoreConfig::default(),
        }
    }
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

fn default_ip() -> String { "127.0.0.1".to_string() }
fn default_port() -> u16 { 20498 }
fn default_rest_ip() -> String { "0.0.0.0".to_string() }
fn default_rest_port() -> u16 { 9081 }
fn default_log_level() -> String { "info".to_string() }
fn default_true() -> bool { true }
fn default_timezone() -> String { "Local".to_string() }
fn default_prometheus_port() -> u16 { 20499 }

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

fn default_store_type() -> String { "memory".to_string() }

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
            tracing::warn!("Configuration file {:?} not found, using defaults", conf_file);
            return Ok(KuiperConfig::default());
        }

        let content = std::fs::read_to_string(&conf_file)
            .with_context(|| format!("Failed to read configuration file: {:?}", conf_file))?;

        let config: KuiperConfig = serde_yaml::from_str(&content)
            .with_context(|| format!("Failed to parse YAML from {:?}", conf_file))?;

        Ok(config)
    }
}
