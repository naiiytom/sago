//! Per-domain ownership / RBAC enforcement for the data-mesh model.
//!
//! A federated Sago project tags targets with a `domain` (`TargetConfig::domain`)
//! so drift can be grouped by the team that owns the underlying data (`sago
//! federate`). This module adds the enforcement half: a domain can declare, via
//! `[domains.<name>] operators = [...]` in `Sago.toml`, which identities are
//! allowed to `apply` (snapshot/overwrite the baseline for) its targets. See
//! `docs/DECENTRALIZED.md`.
//!
//! The check is deliberately simple — a flat, config-declared allowlist per
//! domain, no roles or inheritance — because a data-mesh project's real
//! authority boundary is "which team owns this domain", not a general
//! permission system. Domains not mentioned in `[domains]` at all are
//! unrestricted, so existing configs (and targets with no `domain` set) are
//! unaffected.

use crate::config::{Config, TargetConfig};

/// Why an actor was denied permission to `apply` a target, with enough detail
/// for the CLI to print an actionable error.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AccessDenied {
    pub target: String,
    pub domain: String,
    pub actor: String,
}

impl std::fmt::Display for AccessDenied {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "actor '{}' is not authorized to apply target '{}' (domain '{}')",
            self.actor, self.target, self.domain
        )
    }
}

/// Check whether `actor` may `apply` `target` (named `target_name`, for the
/// error message) under `cfg`'s `[domains]` governance.
///
/// A target with no `domain` set, or whose `domain` has no `[domains.<name>]`
/// entry in `cfg`, is unrestricted — `Ok(())` regardless of `actor`. A domain
/// *with* an entry restricts to its `operators` list, matched case-sensitively;
/// an entry with an empty `operators` list is a deliberate lockout (nobody is
/// authorized) rather than "unrestricted".
pub fn authorize_apply(
    cfg: &Config,
    target_name: &str,
    target: &TargetConfig,
    actor: &str,
) -> Result<(), AccessDenied> {
    let Some(domain) = &target.domain else {
        return Ok(());
    };
    let Some(domain_cfg) = cfg.domains.get(domain) else {
        return Ok(());
    };
    if domain_cfg.operators.iter().any(|op| op == actor) {
        return Ok(());
    }
    Err(AccessDenied {
        target: target_name.to_string(),
        domain: domain.clone(),
        actor: actor.to_string(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;

    fn config_with_domains(domains_block: &str) -> Config {
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

[targets.misc]
connection = "c"
identifier = "misc.parquet"

{domains_block}

[checks]
drift_threshold = 0.05
"#
        );
        Config::from_toml(&toml).unwrap()
    }

    #[test]
    fn test_target_without_domain_is_unrestricted() {
        let cfg = config_with_domains(
            r#"
[domains.sales]
operators = ["alice"]
"#,
        );
        let misc = &cfg.targets["misc"];
        assert!(authorize_apply(&cfg, "misc", misc, "anyone-at-all").is_ok());
    }

    #[test]
    fn test_domain_with_no_entry_is_unrestricted() {
        let cfg = config_with_domains("");
        let orders = &cfg.targets["orders"];
        assert!(authorize_apply(&cfg, "orders", orders, "anyone-at-all").is_ok());
    }

    #[test]
    fn test_listed_operator_is_authorized() {
        let cfg = config_with_domains(
            r#"
[domains.sales]
operators = ["alice", "bob"]
"#,
        );
        let orders = &cfg.targets["orders"];
        assert!(authorize_apply(&cfg, "orders", orders, "bob").is_ok());
    }

    #[test]
    fn test_unlisted_actor_is_denied() {
        let cfg = config_with_domains(
            r#"
[domains.sales]
operators = ["alice"]
"#,
        );
        let orders = &cfg.targets["orders"];
        let err = authorize_apply(&cfg, "orders", orders, "eve").unwrap_err();
        assert_eq!(err.target, "orders");
        assert_eq!(err.domain, "sales");
        assert_eq!(err.actor, "eve");
    }

    #[test]
    fn test_empty_operators_list_is_a_lockout() {
        let cfg = config_with_domains(
            r#"
[domains.sales]
operators = []
"#,
        );
        let orders = &cfg.targets["orders"];
        assert!(authorize_apply(&cfg, "orders", orders, "alice").is_err());
    }

    #[test]
    fn test_domain_entry_with_no_operators_field_defaults_to_lockout() {
        let cfg = config_with_domains(
            r#"
[domains.sales]
"#,
        );
        let orders = &cfg.targets["orders"];
        assert!(authorize_apply(&cfg, "orders", orders, "alice").is_err());
    }

    #[test]
    fn test_match_is_case_sensitive() {
        let cfg = config_with_domains(
            r#"
[domains.sales]
operators = ["Alice"]
"#,
        );
        let orders = &cfg.targets["orders"];
        assert!(authorize_apply(&cfg, "orders", orders, "alice").is_err());
        assert!(authorize_apply(&cfg, "orders", orders, "Alice").is_ok());
    }

    #[test]
    fn test_access_denied_display_message() {
        let err = AccessDenied {
            target: "orders".into(),
            domain: "sales".into(),
            actor: "eve".into(),
        };
        let msg = err.to_string();
        assert!(msg.contains("eve"));
        assert!(msg.contains("orders"));
        assert!(msg.contains("sales"));
    }
}
