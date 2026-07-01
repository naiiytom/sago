use crate::Result;
use serde::Deserialize;
use std::collections::HashMap;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub project: ProjectConfig,
    pub connections: HashMap<String, ConnectionConfig>,
    pub targets: HashMap<String, TargetConfig>,
    pub checks: ChecksConfig,
}

#[derive(Debug, Deserialize)]
pub struct ProjectConfig {
    pub name: String,
    pub version: String,
}

#[derive(Debug, Deserialize, PartialEq, Clone)]
#[serde(rename_all = "lowercase")]
pub enum S3Format {
    Parquet,
    Csv,
    Json,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type")]
pub enum ConnectionConfig {
    #[serde(rename = "postgres")]
    Postgres { url: String },
    #[serde(rename = "s3")]
    S3 {
        bucket: String,
        region: String,
        #[serde(default)]
        format: Option<S3Format>,
    },
}

#[derive(Debug, Deserialize)]
pub struct TargetConfig {
    pub connection: String,
    pub identifier: String,
    #[serde(default)]
    pub sample: Option<SampleConfig>,
    /// Data-mesh domain this target belongs to (e.g. "marketing", "finance").
    /// Optional; lets a single Sago project federate targets owned by different
    /// teams. See the "Decentralized Data Architectures" note in the roadmap.
    #[serde(default)]
    pub domain: Option<String>,
    /// Owning team / contact for this target, for governance in a federated
    /// (data-mesh) setup. Optional and free-form.
    #[serde(default)]
    pub owner: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct SampleConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_sample_n")]
    pub n: usize,
}

fn default_sample_n() -> usize {
    1000
}

#[derive(Debug, Deserialize)]
pub struct ChecksConfig {
    pub drift_threshold: f64,
}

impl Config {
    pub fn from_toml(content: &str) -> Result<Self> {
        // Detect the obsolete top-level `[schema]` table *structurally* rather
        // than by scanning the raw text: a naive `content.contains("[schema]")`
        // also fires on the literal appearing in a comment or inside a string
        // value (e.g. `identifier = "events_[schema]_v2"`), rejecting valid
        // configs. Parse into a generic value first and check for a real
        // top-level `schema` key.
        let value: toml::Value = toml::from_str(content)?;
        if value.get("schema").is_some() {
            return Err(crate::SagoError::Config(
                "config uses obsolete [schema] block — replace with [targets.<name>] entries; see docs".into(),
            ));
        }
        Ok(value.try_into()?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const VALID_TOML: &str = r#"
[project]
name = "sago-project"
version = "0.1.0"

[connections.warehouse]
type = "postgres"
url = "postgres://user:password@localhost/db"

[connections.archive]
type = "s3"
bucket = "my-data-bucket"
region = "us-east-1"

[targets.users]
connection = "warehouse"
identifier = "public.users"

[targets.events_2024]
connection = "archive"
identifier = "events/2024.parquet"

[targets.events_2024.sample]
enabled = true
n = 500

[checks]
drift_threshold = 0.05
"#;

    #[test]
    fn test_valid_full_config() {
        let cfg = Config::from_toml(VALID_TOML).unwrap();
        assert_eq!(cfg.project.name, "sago-project");
        assert_eq!(cfg.targets.len(), 2);

        let users = cfg.targets.get("users").unwrap();
        assert_eq!(users.connection, "warehouse");
        assert_eq!(users.identifier, "public.users");
        assert!(users.sample.is_none());

        let events = cfg.targets.get("events_2024").unwrap();
        let sample = events.sample.as_ref().unwrap();
        assert!(sample.enabled);
        assert_eq!(sample.n, 500);
    }

    #[test]
    fn test_target_domain_and_owner_default_none() {
        // Targets without explicit data-mesh metadata deserialize with None.
        let cfg = Config::from_toml(VALID_TOML).unwrap();
        let users = cfg.targets.get("users").unwrap();
        assert!(users.domain.is_none());
        assert!(users.owner.is_none());
    }

    #[test]
    fn test_target_domain_and_owner_parsed() {
        let toml = r#"
[project]
name = "mesh"
version = "1"

[connections.c]
type = "s3"
bucket = "b"
region = "r"

[targets.orders]
connection = "c"
identifier = "orders.parquet"
domain = "sales"
owner = "sales-data-team"

[checks]
drift_threshold = 0.05
"#;
        let cfg = Config::from_toml(toml).unwrap();
        let orders = cfg.targets.get("orders").unwrap();
        assert_eq!(orders.domain.as_deref(), Some("sales"));
        assert_eq!(orders.owner.as_deref(), Some("sales-data-team"));
    }

    #[test]
    fn test_postgres_connection_deserialization() {
        let cfg = Config::from_toml(VALID_TOML).unwrap();
        match cfg.connections.get("warehouse").unwrap() {
            ConnectionConfig::Postgres { url } => {
                assert_eq!(url, "postgres://user:password@localhost/db");
            }
            _ => panic!("expected Postgres"),
        }
    }

    #[test]
    fn test_s3_connection_deserialization() {
        let cfg = Config::from_toml(VALID_TOML).unwrap();
        match cfg.connections.get("archive").unwrap() {
            ConnectionConfig::S3 { bucket, region, .. } => {
                assert_eq!(bucket, "my-data-bucket");
                assert_eq!(region, "us-east-1");
            }
            _ => panic!("expected S3"),
        }
    }

    #[test]
    fn test_sample_default_n() {
        let toml = r#"
[project]
name = "p"
version = "1"

[connections.c]
type = "s3"
bucket = "b"
region = "r"

[targets.t]
connection = "c"
identifier = "x"
[targets.t.sample]
enabled = true

[checks]
drift_threshold = 0.05
"#;
        let cfg = Config::from_toml(toml).unwrap();
        let sample = cfg.targets["t"].sample.as_ref().unwrap();
        assert!(sample.enabled);
        assert_eq!(sample.n, 1000); // default
    }

    #[test]
    fn test_legacy_schema_block_rejected() {
        let toml = r#"
[project]
name = "p"
version = "1"

[schema]
provider = "postgres"
tables = ["users"]

[checks]
drift_threshold = 0.05
"#;
        let err = Config::from_toml(toml).unwrap_err();
        match err {
            crate::SagoError::Config(msg) => {
                assert!(msg.contains("[schema]"));
                assert!(msg.contains("[targets"));
            }
            other => panic!("expected Config error, got {:?}", other),
        }
    }

    #[test]
    fn test_schema_substring_in_value_is_not_rejected() {
        // Regression: the literal "[schema]" appearing inside a string value or a
        // comment must NOT be mistaken for the obsolete top-level [schema] table.
        let toml = r#"
# migration note: the old [schema] block is gone
[project]
name = "p"
version = "1"

[connections.archive]
type = "s3"
bucket = "b"
region = "r"

[targets.t]
connection = "archive"
identifier = "events_[schema]_v2.parquet"

[checks]
drift_threshold = 0.05
"#;
        let cfg = Config::from_toml(toml).expect("valid config must not be rejected");
        assert_eq!(cfg.targets["t"].identifier, "events_[schema]_v2.parquet");
    }

    #[test]
    fn test_unknown_field_rejected() {
        // deny_unknown_fields guards against silent typos in top-level keys.
        let toml = r#"
[project]
name = "p"
version = "1"

[connections.c]
type = "s3"
bucket = "b"
region = "r"

[targets.t]
connection = "c"
identifier = "x"

[checks]
drift_threshold = 0.05

[bogus]
whatever = true
"#;
        assert!(Config::from_toml(toml).is_err());
    }

    #[test]
    fn test_missing_required_field() {
        let toml = r#"
[connections.c]
type = "s3"
bucket = "b"
region = "r"

[targets]

[checks]
drift_threshold = 0.05
"#;
        let result = Config::from_toml(toml);
        assert!(result.is_err());
    }

    #[test]
    fn test_invalid_toml_syntax() {
        let result = Config::from_toml("this is not valid toml ][[[");
        assert!(result.is_err());
    }

    #[test]
    fn test_s3_format_override_in_config() {
        let toml = r#"
[project]
name = "p"
version = "1"

[connections.archive]
type = "s3"
bucket = "my-bucket"
region = "us-east-1"
format = "csv"

[targets.t]
connection = "archive"
identifier = "data/export.csv"

[checks]
drift_threshold = 0.05
"#;
        let cfg = Config::from_toml(toml).unwrap();
        match cfg.connections.get("archive").unwrap() {
            ConnectionConfig::S3 { format, .. } => {
                assert_eq!(*format, Some(S3Format::Csv));
            }
            _ => panic!("expected S3"),
        }
    }

    #[test]
    fn test_s3_format_defaults_to_none() {
        let toml = r#"
[project]
name = "p"
version = "1"

[connections.archive]
type = "s3"
bucket = "b"
region = "r"

[targets.t]
connection = "archive"
identifier = "data/file.parquet"

[checks]
drift_threshold = 0.05
"#;
        let cfg = Config::from_toml(toml).unwrap();
        match cfg.connections.get("archive").unwrap() {
            ConnectionConfig::S3 { format, .. } => assert!(format.is_none()),
            _ => panic!("expected S3"),
        }
    }
}
