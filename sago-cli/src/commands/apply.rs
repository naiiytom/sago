use anyhow::{Context, Result};
use clap::Args;
use sago_core::config::Config;
use sago_core::connection::build_provider;
use sago_core::state::{ProjectState, capture_snapshot};
use std::path::Path;

#[derive(Args, Debug)]
pub struct ApplyArgs {
    /// Apply only the named target (default: all)
    #[arg(long)]
    pub target: Option<String>,
}

pub async fn run(args: &ApplyArgs) -> Result<()> {
    let cwd = std::env::current_dir()?;
    let cfg_path = cwd.join("Sago.toml");
    let state_path = cwd.join(".sago").join("state.json");

    let cfg = load_config(&cfg_path)?;
    let mut state = ProjectState::load_or_default(&state_path)?;

    let mut applied = Vec::new();
    for (name, tgt) in &cfg.targets {
        if let Some(filter) = &args.target
            && filter != name
        {
            continue;
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
        let sample_n = tgt.sample.as_ref().filter(|s| s.enabled).map(|s| s.n);
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
