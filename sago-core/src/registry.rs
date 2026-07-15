//! Domain discovery for the data-mesh model.
//!
//! Sago's discovery mechanism is deliberately the plainest thing that works: the
//! registry of domains and how to reach them *is* `Sago.toml`'s `[domains]`
//! table, distributed however the team already manages that file (git, config
//! management, etc.) — not a live announce/gossip protocol between running
//! nodes. This module turns that table into a queryable list and resolves a
//! domain name to the `SagoService` endpoint declared for it, so `sago domains`
//! and `sago_sdk::grpc::reconcile` callers don't have to walk `Config` by hand.
//! See `docs/DECENTRALIZED.md`.

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

use crate::config::{Config, normalize_domain_name};

/// A domain known to this project, either because it has a `[domains.<name>]`
/// entry, or because at least one target declares it via `domain = "..."` (or
/// both).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DomainInfo {
    pub name: String,
    /// The `SagoService` endpoint from `[domains.<name>].endpoint`, if this
    /// domain has an entry and declared one.
    pub endpoint: Option<String>,
    /// Number of identities in `[domains.<name>].operators`. Zero can mean
    /// either "no entry" or "entry with an empty allowlist" — see
    /// `has_domain_entry` to distinguish a declared lockout from an
    /// undeclared, unrestricted domain.
    pub operator_count: usize,
    /// Number of targets in this project tagged with this domain.
    pub target_count: usize,
    /// Whether `[domains.<name>]` actually appears in the config, as opposed
    /// to this `DomainInfo` existing only because a target referenced the
    /// name in `domain = "..."` with no matching registry entry.
    pub has_domain_entry: bool,
}

/// Every domain this project knows about: the union of `[domains]` table keys
/// and every target's `domain`, sorted by name. A domain referenced by a
/// target but never declared in `[domains]` still appears here (with
/// `has_domain_entry: false`, `endpoint: None`) — `sago domains` treats a
/// registered-but-undocumented domain as worth surfacing, not silently
/// dropping.
pub fn list_domains(cfg: &Config) -> Vec<DomainInfo> {
    // Group by *normalized* name so a target's `domain = "sales"` and a
    // declared `[domains.Sales]` collapse into one entry instead of
    // appearing as two unrelated domains (one with a RBAC entry and zero
    // targets, one with targets and no visible entry) purely due to casing.
    let mut normalized_names: BTreeSet<String> = cfg
        .domains
        .keys()
        .map(|n| normalize_domain_name(n))
        .collect();
    normalized_names.extend(
        cfg.targets
            .values()
            .filter_map(|t| t.domain.as_deref())
            .map(normalize_domain_name),
    );

    let mut infos: Vec<DomainInfo> = normalized_names
        .into_iter()
        .map(|normalized| {
            let entry = cfg.find_domain(&normalized);
            // Prefer the declared entry's actual key for display; fall back
            // to whatever casing a target used if there's no entry at all.
            let display_name = entry.map(|(name, _)| name.to_string()).unwrap_or_else(|| {
                cfg.targets
                    .values()
                    .filter_map(|t| t.domain.as_deref())
                    .find(|d| normalize_domain_name(d) == normalized)
                    .unwrap_or(&normalized)
                    .to_string()
            });
            DomainInfo {
                name: display_name,
                endpoint: entry.and_then(|(_, d)| d.endpoint.clone()),
                operator_count: entry.map_or(0, |(_, d)| d.operators.len()),
                target_count: cfg
                    .targets
                    .values()
                    .filter(|t| {
                        t.domain.as_deref().map(normalize_domain_name).as_deref()
                            == Some(normalized.as_str())
                    })
                    .count(),
                has_domain_entry: entry.is_some(),
            }
        })
        .collect();
    infos.sort_by(|a, b| a.name.cmp(&b.name));
    infos
}

/// Why [`resolve_endpoint`] could not return an endpoint.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResolveError {
    /// No target references this domain and it has no `[domains.<name>]` entry.
    UnknownDomain { domain: String },
    /// The domain is known but has no `endpoint` set — it may still be a valid
    /// RBAC/grouping-only domain.
    NoEndpoint { domain: String },
}

impl std::fmt::Display for ResolveError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ResolveError::UnknownDomain { domain } => {
                write!(f, "unknown domain '{domain}'")
            }
            ResolveError::NoEndpoint { domain } => {
                write!(f, "domain '{domain}' has no registered endpoint")
            }
        }
    }
}

/// Resolve `domain` to its declared `SagoService` endpoint.
///
/// Errors if the domain is not known at all ([`ResolveError::UnknownDomain`])
/// or is known but has no `endpoint` configured
/// ([`ResolveError::NoEndpoint`]) — a domain that exists purely for RBAC or
/// `sago federate` grouping is a legitimate config, so the two failure modes
/// are distinguished rather than collapsed into one "not found".
pub fn resolve_endpoint<'a>(cfg: &'a Config, domain: &str) -> Result<&'a str, ResolveError> {
    let normalized = normalize_domain_name(domain);
    let known = cfg.find_domain(domain).is_some()
        || cfg.targets.values().any(|t| {
            t.domain.as_deref().map(normalize_domain_name).as_deref() == Some(normalized.as_str())
        });
    if !known {
        return Err(ResolveError::UnknownDomain {
            domain: domain.to_string(),
        });
    }
    cfg.find_domain(domain)
        .and_then(|(_, d)| d.endpoint.as_deref())
        .ok_or_else(|| ResolveError::NoEndpoint {
            domain: domain.to_string(),
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config(extra: &str) -> Config {
        let toml = format!(
            r#"
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

[targets.invoices]
connection = "c"
identifier = "invoices.parquet"
domain = "finance"

[targets.misc]
connection = "c"
identifier = "misc.parquet"

{extra}

[checks]
drift_threshold = 0.05
"#
        );
        Config::from_toml(&toml).unwrap()
    }

    // ── list_domains ─────────────────────────────────────────────────────────

    #[test]
    fn test_list_domains_includes_target_referenced_domains_without_entry() {
        let cfg = config("");
        let domains = list_domains(&cfg);
        let names: Vec<&str> = domains.iter().map(|d| d.name.as_str()).collect();
        assert_eq!(names, vec!["finance", "sales"]);
        assert!(!domains[0].has_domain_entry);
        assert!(domains[0].endpoint.is_none());
    }

    #[test]
    fn test_list_domains_excludes_unassigned_targets() {
        let cfg = config("");
        let domains = list_domains(&cfg);
        assert!(!domains.iter().any(|d| d.name == "misc"));
    }

    #[test]
    fn test_list_domains_sorted_alphabetically() {
        let cfg = config(
            r#"
[domains.zzz]
endpoint = "http://zzz:1"
"#,
        );
        let domains = list_domains(&cfg);
        let names: Vec<&str> = domains.iter().map(|d| d.name.as_str()).collect();
        assert_eq!(names, vec!["finance", "sales", "zzz"]);
    }

    #[test]
    fn test_list_domains_reports_endpoint_and_operator_count() {
        let cfg = config(
            r#"
[domains.sales]
operators = ["alice", "bob"]
endpoint = "http://sales.internal:50051"
"#,
        );
        let domains = list_domains(&cfg);
        let sales = domains.iter().find(|d| d.name == "sales").unwrap();
        assert_eq!(
            sales.endpoint.as_deref(),
            Some("http://sales.internal:50051")
        );
        assert_eq!(sales.operator_count, 2);
        assert!(sales.has_domain_entry);
    }

    #[test]
    fn test_list_domains_target_count() {
        let cfg = config("");
        let domains = list_domains(&cfg);
        let sales = domains.iter().find(|d| d.name == "sales").unwrap();
        assert_eq!(sales.target_count, 1);
    }

    #[test]
    fn test_list_domains_collapses_case_mismatch_with_target() {
        // Regression: a target's `domain = "sales"` (lowercase) and a
        // declared `[domains.Sales]` (capital S) must collapse into ONE
        // DomainInfo with both the RBAC entry and the target counted,
        // instead of appearing as two unrelated domains.
        let cfg = config(
            r#"
[domains.Sales]
operators = ["alice"]
endpoint = "http://sales.internal:50051"
"#,
        );
        let domains = list_domains(&cfg);
        let matches: Vec<&DomainInfo> = domains
            .iter()
            .filter(|d| d.name.eq_ignore_ascii_case("sales"))
            .collect();
        assert_eq!(
            matches.len(),
            1,
            "expected exactly one collapsed entry, got {domains:?}"
        );
        let sales = matches[0];
        assert!(sales.has_domain_entry);
        assert_eq!(sales.operator_count, 1);
        assert_eq!(sales.target_count, 1);
        assert_eq!(
            sales.endpoint.as_deref(),
            Some("http://sales.internal:50051")
        );
    }

    #[test]
    fn test_list_domains_entry_with_no_targets_still_appears() {
        // A domain declared purely for future use, with no target tagged yet.
        let cfg = config(
            r#"
[domains.marketing]
endpoint = "http://marketing.internal:50051"
"#,
        );
        let domains = list_domains(&cfg);
        let marketing = domains.iter().find(|d| d.name == "marketing").unwrap();
        assert_eq!(marketing.target_count, 0);
        assert!(marketing.has_domain_entry);
    }

    #[test]
    fn test_list_domains_empty_project_is_empty() {
        let toml = r#"
[project]
name = "p"
version = "1"

[checks]
drift_threshold = 0.05
"#;
        let cfg = Config::from_toml(toml).unwrap();
        assert!(list_domains(&cfg).is_empty());
    }

    // ── resolve_endpoint ─────────────────────────────────────────────────────

    #[test]
    fn test_resolve_endpoint_returns_configured_endpoint() {
        let cfg = config(
            r#"
[domains.sales]
endpoint = "http://sales.internal:50051"
"#,
        );
        assert_eq!(
            resolve_endpoint(&cfg, "sales"),
            Ok("http://sales.internal:50051")
        );
    }

    #[test]
    fn test_resolve_endpoint_case_insensitive_lookup() {
        let cfg = config(
            r#"
[domains.Sales]
endpoint = "http://sales.internal:50051"
"#,
        );
        assert_eq!(
            resolve_endpoint(&cfg, "sales"),
            Ok("http://sales.internal:50051")
        );
        assert_eq!(
            resolve_endpoint(&cfg, "SALES"),
            Ok("http://sales.internal:50051")
        );
    }

    #[test]
    fn test_resolve_endpoint_unknown_domain_errors() {
        let cfg = config("");
        assert_eq!(
            resolve_endpoint(&cfg, "nope"),
            Err(ResolveError::UnknownDomain {
                domain: "nope".to_string()
            })
        );
    }

    #[test]
    fn test_resolve_endpoint_known_domain_without_endpoint_errors() {
        // "sales" is known (a target references it) but has no [domains.sales]
        // entry at all, so no endpoint — distinct from "unknown".
        let cfg = config("");
        assert_eq!(
            resolve_endpoint(&cfg, "sales"),
            Err(ResolveError::NoEndpoint {
                domain: "sales".to_string()
            })
        );
    }

    #[test]
    fn test_resolve_endpoint_entry_without_endpoint_field_errors() {
        // Domain has a [domains.sales] entry (e.g. for RBAC) but no endpoint.
        let cfg = config(
            r#"
[domains.sales]
operators = ["alice"]
"#,
        );
        assert_eq!(
            resolve_endpoint(&cfg, "sales"),
            Err(ResolveError::NoEndpoint {
                domain: "sales".to_string()
            })
        );
    }

    #[test]
    fn test_resolve_error_display_messages() {
        let unknown = ResolveError::UnknownDomain { domain: "x".into() };
        assert!(unknown.to_string().contains("unknown domain"));
        assert!(unknown.to_string().contains('x'));

        let no_endpoint = ResolveError::NoEndpoint { domain: "y".into() };
        assert!(no_endpoint.to_string().contains("no registered endpoint"));
        assert!(no_endpoint.to_string().contains('y'));
    }
}
