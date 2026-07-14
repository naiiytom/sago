use anyhow::{Context, Result};
use clap::Args;
use sago_core::config::Config;
use sago_core::connection::build_provider;
use sago_core::rbac::authorize_apply;
use sago_core::state::{ProjectState, capture_snapshot};
use std::path::Path;

#[derive(Args, Debug)]
pub struct ApplyArgs {
    /// Apply only the named target (default: all)
    #[arg(long)]
    pub target: Option<String>,

    /// Identity to authorize against `[domains.<name>].operators` for
    /// domain-restricted targets (default: the `SAGO_ACTOR` environment
    /// variable). Required only for targets whose `domain` has a
    /// `[domains.<name>]` entry in `Sago.toml`; unrestricted targets ignore it.
    #[arg(long = "as")]
    pub actor: Option<String>,
}

pub async fn run(args: &ApplyArgs) -> Result<()> {
    let cwd = std::env::current_dir()?;
    let cfg_path = cwd.join("Sago.toml");
    let state_path = cwd.join(".sago").join("state.json");

    let cfg = load_config(&cfg_path)?;
    let mut state = ProjectState::load_or_default(&state_path)?;
    let actor = resolve_actor(args.actor.clone(), std::env::var("SAGO_ACTOR").ok());

    let mut applied = Vec::new();
    for (name, tgt) in &cfg.targets {
        if let Some(filter) = &args.target
            && filter != name
        {
            continue;
        }
        if let Some(domain) = &tgt.domain
            && cfg.domains.contains_key(domain)
        {
            let actor = actor.as_deref().ok_or_else(|| {
                anyhow::anyhow!(
                    "target '{}' is in domain '{}', which requires authorization — pass --as <actor> or set SAGO_ACTOR",
                    name, domain
                )
            })?;
            authorize_apply(&cfg, name, tgt, actor).map_err(|e| anyhow::anyhow!(e.to_string()))?;
        }
        let conn = cfg.connections.get(&tgt.connection).with_context(|| {
            format!(
                "target '{}' references unknown connection '{}'",
                name, tgt.connection
            )
        })?;
        let provider = build_provider(conn)
            .await
            .with_context(|| format!("failed to build provider for '{}'", name))?;
        // Persist per-column samples by default so `sago plan`'s PSI drift gate
        // has a baseline to compare against. A target opts out only with an
        // explicit `[targets.x.sample] enabled = false`; a missing block means
        // "sample with the default size", not "no samples" (the latter silently
        // disabled drift detection out of the box).
        let sample_n = tgt
            .sample
            .as_ref()
            .map_or(Some(sago_core::config::DEFAULT_SAMPLE_N), |s| {
                s.enabled.then_some(s.n)
            });
        let snap = capture_snapshot(provider, &tgt.identifier, sample_n)
            .await
            .with_context(|| format!("failed to capture snapshot for '{}'", name))?;
        state.snapshots.insert(name.clone(), snap);
        applied.push(name.clone());
    }

    state
        .save(&state_path)
        .with_context(|| format!("failed to write {}", state_path.display()))?;

    if applied.is_empty() {
        println!("nothing to apply (no matching targets)");
    } else {
        println!("applied: {}", applied.join(", "));
    }
    Ok(())
}

fn load_config(path: &Path) -> Result<Config> {
    let content = std::fs::read_to_string(path).with_context(|| {
        format!(
            "Sago.toml not found at {} (run `sago init`)",
            path.display()
        )
    })?;
    Ok(Config::from_toml(&content)?)
}

/// The effective actor identity: the `--as` flag if given, else the
/// `SAGO_ACTOR` environment variable, else `None` (only an error for targets
/// that actually require authorization — see `run`).
fn resolve_actor(flag: Option<String>, env: Option<String>) -> Option<String> {
    flag.or(env)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_resolve_actor_flag_overrides_env() {
        assert_eq!(
            resolve_actor(Some("alice".into()), Some("bob".into())),
            Some("alice".into())
        );
    }

    #[test]
    fn test_resolve_actor_falls_back_to_env() {
        assert_eq!(resolve_actor(None, Some("bob".into())), Some("bob".into()));
    }

    #[test]
    fn test_resolve_actor_none_when_neither_set() {
        assert_eq!(resolve_actor(None, None), None);
    }
}
