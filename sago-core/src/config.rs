use serde::Deserialize;
use std::collections::HashMap;
use crate::Result;

#[derive(Debug, Deserialize)]
pub struct Config {
    pub project: ProjectConfig,
    pub connections: HashMap<String, ConnectionConfig>,
    pub schema: SchemaConfig,
    pub checks: ChecksConfig,
}

#[derive(Debug, Deserialize)]
pub struct ProjectConfig {
    pub name: String,
    pub version: String,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type")]
pub enum ConnectionConfig {
    #[serde(rename = "postgres")]
    Postgres { url: String },
    #[serde(rename = "s3")]
    S3 { bucket: String, region: String },
}

#[derive(Debug, Deserialize)]
pub struct SchemaConfig {
    pub provider: String,
    pub tables: Vec<String>,
}

#[derive(Debug, Deserialize)]
pub struct ChecksConfig {
    pub drift_threshold: f64,
}

impl Config {
    pub fn from_toml(content: &str) -> Result<Self> {
        toml::from_str(content).map_err(|e| crate::SagoError::Config(e.to_string()))
    }
}
