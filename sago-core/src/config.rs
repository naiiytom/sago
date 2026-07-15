use crate::Result;
use serde::Deserialize;
use std::collections::HashMap;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub project: ProjectConfig,
    /// Named data-source connections. Defaults to empty so a freshly
    /// `sago init`ed project (which has all connections commented out) still
    /// parses; `apply`/`plan` then simply report "nothing to do".
    #[serde(default)]
    pub connections: HashMap<String, ConnectionConfig>,
    /// Datasets to track. Defaults to empty for the same reason as `connections`.
    #[serde(default)]
    pub targets: HashMap<String, TargetConfig>,
    /// Per-domain governance for the data-mesh model (see
    /// `sago-core::rbac` and `docs/DECENTRALIZED.md`). Defaults to empty: a
    /// domain with no entry here is unrestricted, so existing configs are
    /// unaffected by this table's mere existence.
    #[serde(default)]
    pub domains: HashMap<String, DomainConfig>,
    pub checks: ChecksConfig,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
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
#[serde(deny_unknown_fields)]
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
#[serde(deny_unknown_fields)]
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

/// Governance and discovery metadata for a data-mesh `domain`: who may run
/// `sago apply` against the targets tagged with it, and where to reach the
/// `SagoService` node that domain's team operates. See `sago-core::rbac` and
/// `sago-core::registry`.
#[derive(Debug, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct DomainConfig {
    /// Identities allowed to `apply` targets in this domain. Empty means the
    /// domain has an entry but nobody is authorized — a deliberate lockout,
    /// not "unrestricted" (that is what *omitting* the domain from this table
    /// means). Matched case-sensitively against the actor identity resolved
    /// by the CLI (`--as` or `SAGO_ACTOR`).
    #[serde(default)]
    pub operators: Vec<String>,
    /// The `SagoService` gRPC endpoint (e.g. `http://sales.internal:50051`)
    /// that this domain's team operates, for reconciliation via
    /// `sago_sdk::grpc::reconcile`. Optional — a domain can exist purely for
    /// RBAC/grouping without exposing a node. This is Sago's discovery
    /// mechanism: the registry of domains and their endpoints is the config
    /// file itself, not a live announce protocol.
    #[serde(default)]
    pub endpoint: Option<String>,
}

/// Default number of numeric values retained per column when sampling.
///
/// Large enough for a stable 10-bin PSI without materializing whole columns; the
/// single source of truth shared by config defaults and the CLI's live sampling.
pub const DEFAULT_SAMPLE_N: usize = 1000;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SampleConfig {
    /// Whether to persist per-column numeric samples for this target. Defaults to
    /// `true`: samples are what `sago plan`'s PSI drift gate compares against, so
    /// without them the gate is silently inert. Set `enabled = false` to opt out.
    #[serde(default = "default_sample_enabled")]
    pub enabled: bool,
    #[serde(default = "default_sample_n")]
    pub n: usize,
}

fn default_sample_enabled() -> bool {
    true
}

fn default_sample_n() -> usize {
    DEFAULT_SAMPLE_N
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChecksConfig {
    pub drift_threshold: f64,
    /// Minimum blended confidence for a removed/added column pair to be reported
    /// as a rename rather than a drop + add. Defaults to
    /// [`crate::rename::DEFAULT_MIN_CONFIDENCE`]. Raise it for stricter matching
    /// (fewer false-positive renames), lower it to catch more renames.
    #[serde(default = "default_rename_confidence_threshold")]
    pub rename_confidence_threshold: f64,
}

fn default_rename_confidence_threshold() -> f64 {
    crate::rename::DEFAULT_MIN_CONFIDENCE
}

/// Normalize a domain name (trim whitespace, lowercase) for matching a
/// target's `domain` value against a `[domains.<name>]` table key. Domain
/// names are declared and referenced independently in different parts of
/// `Sago.toml` (and possibly by different authors/times), so an exact,
/// case-sensitive `HashMap` lookup would silently fail-open — a target with
/// `domain = "sales"` against a declared `[domains.Sales]` would find no
/// entry and be treated as *unrestricted*, with the RBAC list present in the
/// file but silently inert.
pub fn normalize_domain_name(name: &str) -> String {
    name.trim().to_lowercase()
}

impl Config {
    /// Look up a `[domains.<name>]` entry by normalized name (see
    /// [`normalize_domain_name`]), returning the entry's *actual* declared key
    /// alongside its config. Every caller that matches a target's `domain`
    /// against `self.domains` must go through this rather than
    /// `self.domains.get(domain)` directly, so a casing/whitespace mismatch
    /// can't silently degrade a restricted domain into an unrestricted one.
    pub fn find_domain(&self, domain: &str) -> Option<(&str, &DomainConfig)> {
        let target = normalize_domain_name(domain);
        self.domains
            .iter()
            .find(|(name, _)| normalize_domain_name(name) == target)
            .map(|(name, cfg)| (name.as_str(), cfg))
    }

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
        // Parse `content` directly into `Config` rather than converting the
        // already-parsed `Value` via `.try_into()`: the latter loses the
        // original source's line/column span and text snippet, degrading
        // ordinary type-mismatch errors (e.g. a quoted number) to a bare
        // message with no indication of where in the file the problem is.
        let config: Config = toml::from_str(content)?;
        config.validate()?;
        Ok(config)
    }

    /// Semantic validation applied after deserialization succeeds.
    ///
    /// `drift_threshold` gates on the Population Stability Index, which is a
    /// non-negative divergence in `[0, ∞)` but is only meaningful for drift
    /// alerting within `[0, 1]` (the rules of thumb cap at 0.25 "major shift").
    /// A negative threshold makes *every* column breach; a threshold above 1.0
    /// effectively disables detection. Both are almost certainly misconfigurations,
    /// so reject them at parse time rather than silently inverting/disabling the gate.
    fn validate(&self) -> Result<()> {
        let t = self.checks.drift_threshold;
        if !t.is_finite() || !(0.0..=1.0).contains(&t) {
            return Err(crate::SagoError::Config(format!(
                "checks.drift_threshold must be in [0.0, 1.0], got {t}"
            )));
        }
        let rt = self.checks.rename_confidence_threshold;
        if !rt.is_finite() || !(0.0..=1.0).contains(&rt) {
            return Err(crate::SagoError::Config(format!(
                "checks.rename_confidence_threshold must be in [0.0, 1.0], got {rt}"
            )));
        }

        // A target naming an undeclared connection only ever surfaced as a
        // per-target runtime error deep inside `apply`/`plan` (after other
        // targets may already have done real I/O). Catching it here gives a
        // single, immediate error before any target is touched.
        for (target_name, target) in &self.targets {
            if !self.connections.contains_key(&target.connection) {
                return Err(crate::SagoError::Config(format!(
                    "targets.{target_name}.connection = \"{}\" does not match any [connections.*] entry",
                    target.connection
                )));
            }
        }

        let mut seen_normalized: HashMap<String, &str> = HashMap::new();
        for (domain_name, domain) in &self.domains {
            if let Some(endpoint) = &domain.endpoint {
                validate_endpoint(domain_name, endpoint)?;
            }
            let normalized = normalize_domain_name(domain_name);
            if let Some(other) = seen_normalized.insert(normalized, domain_name) {
                return Err(crate::SagoError::Config(format!(
                    "domains.{domain_name} and domains.{other} both normalize to the same domain name — declare it once"
                )));
            }
        }

        Ok(())
    }
}

/// A minimal `scheme://host[:port]` shape check for a domain's `SagoService`
/// endpoint. Deliberately not a full URI parser (this crate stays
/// dependency-light so it can compile to `wasm32-unknown-unknown`) — it only
/// catches the common failure modes (missing scheme, empty host, garbage
/// like a stray space) at config-load time rather than letting them surface
/// as an opaque tonic/transport error far from the config file that caused it.
fn validate_endpoint(domain_name: &str, endpoint: &str) -> Result<()> {
    let Some((scheme, rest)) = endpoint.split_once("://") else {
        return Err(crate::SagoError::Config(format!(
            "domains.{domain_name}.endpoint = \"{endpoint}\" is missing a scheme (expected e.g. \"http://host:port\")"
        )));
    };
    if scheme != "http" && scheme != "https" {
        return Err(crate::SagoError::Config(format!(
            "domains.{domain_name}.endpoint = \"{endpoint}\" has unsupported scheme \"{scheme}\" (expected \"http\" or \"https\")"
        )));
    }
    let host = rest.split(['/', '?', '#']).next().unwrap_or("");
    if host.is_empty() || host.contains(char::is_whitespace) {
        return Err(crate::SagoError::Config(format!(
            "domains.{domain_name}.endpoint = \"{endpoint}\" has an invalid host"
        )));
    }
    Ok(())
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
    fn test_domains_table_defaults_empty() {
        // A config with no [domains] table at all parses fine and leaves every
        // domain unrestricted (no entry to enforce against).
        let cfg = Config::from_toml(VALID_TOML).unwrap();
        assert!(cfg.domains.is_empty());
    }

    #[test]
    fn test_domains_table_parses_operators() {
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

[domains.sales]
operators = ["alice", "bob"]

[checks]
drift_threshold = 0.05
"#;
        let cfg = Config::from_toml(toml).unwrap();
        let sales = cfg.domains.get("sales").unwrap();
        assert_eq!(
            sales.operators,
            vec!["alice".to_string(), "bob".to_string()]
        );
    }

    #[test]
    fn test_domain_entry_without_operators_defaults_empty() {
        let toml = r#"
[project]
name = "mesh"
version = "1"

[connections.c]
type = "s3"
bucket = "b"
region = "r"

[domains.sales]

[checks]
drift_threshold = 0.05
"#;
        let cfg = Config::from_toml(toml).unwrap();
        assert!(cfg.domains.get("sales").unwrap().operators.is_empty());
    }

    #[test]
    fn test_domain_endpoint_parsed() {
        let toml = r#"
[project]
name = "mesh"
version = "1"

[connections.c]
type = "s3"
bucket = "b"
region = "r"

[domains.sales]
endpoint = "http://sales.internal:50051"

[checks]
drift_threshold = 0.05
"#;
        let cfg = Config::from_toml(toml).unwrap();
        assert_eq!(
            cfg.domains.get("sales").unwrap().endpoint.as_deref(),
            Some("http://sales.internal:50051")
        );
    }

    #[test]
    fn test_domain_endpoint_defaults_none() {
        let cfg = Config::from_toml(
            r#"
[project]
name = "mesh"
version = "1"

[connections.c]
type = "s3"
bucket = "b"
region = "r"

[domains.sales]
operators = ["alice"]

[checks]
drift_threshold = 0.05
"#,
        )
        .unwrap();
        assert!(cfg.domains.get("sales").unwrap().endpoint.is_none());
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
        assert_eq!(sample.n, DEFAULT_SAMPLE_N); // default
    }

    #[test]
    fn test_sample_block_enabled_defaults_true() {
        // A sample block present but without `enabled` must default to enabled,
        // so drift sampling is on unless the user explicitly opts out.
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
n = 50

[checks]
drift_threshold = 0.05
"#;
        let cfg = Config::from_toml(toml).unwrap();
        let sample = cfg.targets["t"].sample.as_ref().unwrap();
        assert!(sample.enabled, "sample.enabled should default to true");
        assert_eq!(sample.n, 50);
    }

    #[test]
    fn test_sample_can_be_explicitly_disabled() {
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
enabled = false

[checks]
drift_threshold = 0.05
"#;
        let cfg = Config::from_toml(toml).unwrap();
        assert!(!cfg.targets["t"].sample.as_ref().unwrap().enabled);
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
    fn test_missing_required_project_field() {
        // `project` and `checks` are still required; `connections`/`targets`
        // default to empty. Omitting `project` must error.
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
    fn test_minimal_config_without_connections_or_targets_parses() {
        // Regression: a freshly `sago init`ed project comments out all
        // connections/targets. That config must parse (both default to empty),
        // so the user's first `apply` reports "nothing to do" instead of a
        // cryptic `missing field 'connections'`.
        let toml = r#"
[project]
name = "fresh"
version = "0.1.0"

[checks]
drift_threshold = 0.05
"#;
        let cfg = Config::from_toml(toml).expect("minimal init config must parse");
        assert!(cfg.connections.is_empty());
        assert!(cfg.targets.is_empty());
    }

    #[test]
    fn test_invalid_toml_syntax() {
        let result = Config::from_toml("this is not valid toml ][[[");
        assert!(result.is_err());
    }

    fn config_with_threshold(t: &str) -> crate::Result<Config> {
        let toml = format!(
            r#"
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
drift_threshold = {t}
"#
        );
        Config::from_toml(&toml)
    }

    #[test]
    fn test_drift_threshold_valid_bounds_accepted() {
        for t in ["0.0", "0.05", "0.25", "1.0"] {
            assert!(
                config_with_threshold(t).is_ok(),
                "threshold {t} should be accepted"
            );
        }
    }

    #[test]
    fn test_drift_threshold_negative_rejected() {
        let err = config_with_threshold("-0.1").unwrap_err();
        match err {
            crate::SagoError::Config(msg) => assert!(msg.contains("drift_threshold")),
            other => panic!("expected Config error, got {other:?}"),
        }
    }

    #[test]
    fn test_drift_threshold_above_one_rejected() {
        let err = config_with_threshold("100.0").unwrap_err();
        assert!(matches!(err, crate::SagoError::Config(_)));
    }

    #[test]
    fn test_rename_confidence_threshold_defaults() {
        // Omitted → falls back to the shared library default.
        let cfg = config_with_threshold("0.05").unwrap();
        assert_eq!(
            cfg.checks.rename_confidence_threshold,
            crate::rename::DEFAULT_MIN_CONFIDENCE
        );
    }

    #[test]
    fn test_rename_confidence_threshold_parsed_and_validated() {
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
rename_confidence_threshold = 0.8
"#;
        let cfg = Config::from_toml(toml).unwrap();
        assert_eq!(cfg.checks.rename_confidence_threshold, 0.8);

        // Out-of-range value is rejected at parse time.
        let bad = toml.replace("0.8", "1.5");
        let err = Config::from_toml(&bad).unwrap_err();
        match err {
            crate::SagoError::Config(msg) => {
                assert!(msg.contains("rename_confidence_threshold"))
            }
            other => panic!("expected Config error, got {other:?}"),
        }
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

    // ── nested deny_unknown_fields ────────────────────────────────────────────

    #[test]
    fn test_unknown_field_in_target_is_rejected() {
        // Regression: a typo'd nested field (e.g. "doamin" instead of "domain")
        // used to be silently dropped since only the top-level Config struct
        // enforced deny_unknown_fields — a target that the user believed was
        // RBAC-restricted via `domain` would deserialize with domain = None
        // (unrestricted) and no error anywhere.
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
doamin = "sales"

[checks]
drift_threshold = 0.05
"#;
        assert!(Config::from_toml(toml).is_err());
    }

    #[test]
    fn test_unknown_field_in_connection_is_rejected() {
        let toml = r#"
[project]
name = "p"
version = "1"

[connections.c]
type = "s3"
bucket = "b"
region = "r"
bukcet = "typo"

[targets.t]
connection = "c"
identifier = "x"

[checks]
drift_threshold = 0.05
"#;
        assert!(Config::from_toml(toml).is_err());
    }

    #[test]
    fn test_unknown_field_in_domain_is_rejected() {
        let toml = r#"
[project]
name = "p"
version = "1"

[connections.c]
type = "s3"
bucket = "b"
region = "r"

[domains.sales]
operatorz = ["alice"]

[checks]
drift_threshold = 0.05
"#;
        assert!(Config::from_toml(toml).is_err());
    }

    #[test]
    fn test_unknown_field_in_checks_is_rejected() {
        let toml = r#"
[project]
name = "p"
version = "1"

[checks]
drift_threshold = 0.05
bogus_check = true
"#;
        assert!(Config::from_toml(toml).is_err());
    }

    // ── target.connection cross-validation ───────────────────────────────────

    #[test]
    fn test_target_referencing_unknown_connection_is_rejected() {
        // A typo in `connection` (e.g. "warehosue") used to only surface as a
        // per-target runtime error inside apply/plan, potentially after other
        // targets had already done real I/O. Config::validate now catches it
        // up front.
        let toml = r#"
[project]
name = "p"
version = "1"

[connections.warehouse]
type = "postgres"
url = "postgres://user:pass@localhost/db"

[targets.orders]
connection = "warehosue"
identifier = "orders"

[checks]
drift_threshold = 0.05
"#;
        let err = Config::from_toml(toml).unwrap_err();
        match err {
            crate::SagoError::Config(msg) => {
                assert!(msg.contains("orders"));
                assert!(msg.contains("warehosue"));
            }
            other => panic!("expected Config error, got {other:?}"),
        }
    }

    #[test]
    fn test_target_referencing_known_connection_is_accepted() {
        assert!(Config::from_toml(VALID_TOML).is_ok());
    }

    // ── domain endpoint validation ────────────────────────────────────────────

    #[test]
    fn test_domain_endpoint_missing_scheme_rejected() {
        let toml = r#"
[project]
name = "mesh"
version = "1"

[domains.sales]
endpoint = "sales.internal:50051"

[checks]
drift_threshold = 0.05
"#;
        let err = Config::from_toml(toml).unwrap_err();
        assert!(matches!(err, crate::SagoError::Config(_)));
    }

    #[test]
    fn test_domain_endpoint_with_space_rejected() {
        let toml = r#"
[project]
name = "mesh"
version = "1"

[domains.sales]
endpoint = "sales.internal 50051"

[checks]
drift_threshold = 0.05
"#;
        let err = Config::from_toml(toml).unwrap_err();
        assert!(matches!(err, crate::SagoError::Config(_)));
    }

    #[test]
    fn test_domain_endpoint_empty_rejected() {
        let toml = r#"
[project]
name = "mesh"
version = "1"

[domains.sales]
endpoint = ""

[checks]
drift_threshold = 0.05
"#;
        let err = Config::from_toml(toml).unwrap_err();
        assert!(matches!(err, crate::SagoError::Config(_)));
    }

    #[test]
    fn test_domain_endpoint_valid_http_and_https_accepted() {
        for endpoint in ["http://sales.internal:50051", "https://sales.internal"] {
            let toml = format!(
                r#"
[project]
name = "mesh"
version = "1"

[domains.sales]
endpoint = "{endpoint}"

[checks]
drift_threshold = 0.05
"#
            );
            assert!(
                Config::from_toml(&toml).is_ok(),
                "endpoint {endpoint} should be accepted"
            );
        }
    }

    // ── TOML error span preservation ──────────────────────────────────────────

    #[test]
    fn test_type_mismatch_error_includes_line_information() {
        // Regression: parsing via toml::Value then .try_into() discarded the
        // original source's span/line info, degrading a type-mismatch error to
        // a bare message with no indication of where in the file the problem
        // is. Parsing directly into Config must preserve it.
        let toml = r#"
[project]
name = "p"
version = "1"

[checks]
drift_threshold = "0.05"
"#;
        let err = Config::from_toml(toml).unwrap_err();
        match err {
            crate::SagoError::TomlParse(e) => {
                assert!(
                    e.span().is_some(),
                    "expected a source span on the TOML parse error, got {e:?}"
                );
            }
            other => panic!("expected TomlParse error, got {other:?}"),
        }
    }

    // ── domain name normalization ────────────────────────────────────────────

    #[test]
    fn test_find_domain_matches_case_insensitively() {
        let toml = r#"
[project]
name = "mesh"
version = "1"

[domains.Sales]
operators = ["alice"]

[checks]
drift_threshold = 0.05
"#;
        let cfg = Config::from_toml(toml).unwrap();
        let (name, domain_cfg) = cfg.find_domain("sales").expect("should find via lowercase");
        assert_eq!(name, "Sales");
        assert_eq!(domain_cfg.operators, vec!["alice".to_string()]);
        assert!(cfg.find_domain("SALES").is_some());
        assert!(cfg.find_domain("  sales  ").is_some());
        assert!(cfg.find_domain("marketing").is_none());
    }

    #[test]
    fn test_duplicate_domain_names_after_normalization_rejected() {
        // [domains.Sales] and [domains.sales] are two distinct TOML keys but
        // normalize to the same domain — declaring both is ambiguous and
        // must be rejected rather than one silently shadowing the other.
        let toml = r#"
[project]
name = "mesh"
version = "1"

[domains.Sales]
operators = ["alice"]

[domains.sales]
operators = ["bob"]

[checks]
drift_threshold = 0.05
"#;
        let err = Config::from_toml(toml).unwrap_err();
        assert!(matches!(err, crate::SagoError::Config(_)));
    }
}
