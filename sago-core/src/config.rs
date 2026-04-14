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

#[cfg(test)]
mod tests {
    use super::*;

    const VALID_TOML: &str = r#"
[project]
name = "sago-project"
version = "0.1.0"

[connections.postgres]
type = "postgres"
url = "postgres://user:password@localhost/db"

[connections.s3]
type = "s3"
bucket = "my-data-bucket"
region = "us-east-1"

[schema]
provider = "postgres"
tables = ["users", "orders"]

[checks]
drift_threshold = 0.05
"#;

    #[test]
    fn test_valid_full_config() {
        let config = Config::from_toml(VALID_TOML).unwrap();
        assert_eq!(config.project.name, "sago-project");
        assert_eq!(config.project.version, "0.1.0");
        assert_eq!(config.schema.provider, "postgres");
        assert_eq!(config.schema.tables, vec!["users", "orders"]);
        assert_eq!(config.checks.drift_threshold, 0.05);
        assert!(config.connections.contains_key("postgres"));
        assert!(config.connections.contains_key("s3"));
    }

    #[test]
    fn test_postgres_connection_deserialization() {
        let config = Config::from_toml(VALID_TOML).unwrap();
        match config.connections.get("postgres").unwrap() {
            ConnectionConfig::Postgres { url } => {
                assert_eq!(url, "postgres://user:password@localhost/db");
            }
            _ => panic!("Expected Postgres connection"),
        }
    }

    #[test]
    fn test_s3_connection_deserialization() {
        let config = Config::from_toml(VALID_TOML).unwrap();
        match config.connections.get("s3").unwrap() {
            ConnectionConfig::S3 { bucket, region } => {
                assert_eq!(bucket, "my-data-bucket");
                assert_eq!(region, "us-east-1");
            }
            _ => panic!("Expected S3 connection"),
        }
    }

    #[test]
    fn test_drift_threshold_value() {
        let toml = r#"
[project]
name = "p"
version = "1"

[connections]

[schema]
provider = "postgres"
tables = []

[checks]
drift_threshold = 0.1
"#;
        let config = Config::from_toml(toml).unwrap();
        assert_eq!(config.checks.drift_threshold, 0.1);
    }

    #[test]
    fn test_missing_required_field() {
        // Missing [project] section
        let toml = r#"
[connections.postgres]
type = "postgres"
url = "postgres://localhost/db"

[schema]
provider = "postgres"
tables = []

[checks]
drift_threshold = 0.05
"#;
        let result = Config::from_toml(toml);
        assert!(result.is_err());
        match result.unwrap_err() {
            crate::SagoError::Config(_) => {}
            e => panic!("Expected Config error, got: {:?}", e),
        }
    }

    #[test]
    fn test_invalid_toml_syntax() {
        let result = Config::from_toml("this is not valid toml ][[[");
        assert!(result.is_err());
        match result.unwrap_err() {
            crate::SagoError::Config(_) => {}
            e => panic!("Expected Config error, got: {:?}", e),
        }
    }
}
