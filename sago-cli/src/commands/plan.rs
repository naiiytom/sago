use anyhow::{Context, Result, anyhow, bail};
use clap::Args;
use sago_core::config::Config;
use sago_core::connection::build_provider;
use sago_core::diff::DiffReport;
use sago_core::drift::{SemanticDrift, detect_data_drift_from_stats, detect_schema_drift};
use sago_core::semantic::SemanticType;
use sago_core::state::{ProjectState, capture_snapshot};
use std::collections::HashMap;
use std::path::{Path, PathBuf};

use crate::report::{default_artifact_path, print_terminal, write_artifact};

#[derive(Args, Debug)]
pub struct PlanArgs {
    /// Plan only the named target (default: all)
    #[arg(long)]
    pub target: Option<String>,

    /// Where to write the JSON artifact
    #[arg(long)]
    pub out: Option<PathBuf>,
}

pub async fn run(args: &PlanArgs) -> Result<()> {
    let cwd = std::env::current_dir()?;
    let cfg_path = cwd.join("Sago.toml");
    let state_path = cwd.join(".sago").join("state.json");

    let cfg = load_config(&cfg_path)?;
    if !state_path.exists() {
        bail!(
            "no state file at {} — run `sago apply` first",
            state_path.display()
        );
    }
    let state = ProjectState::load(&state_path)?;

    let mut reports = Vec::new();
    for (name, tgt) in &cfg.targets {
        if let Some(filter) = &args.target
            && filter != name
        {
            continue;
        }
        let snap = state.snapshots.get(name).ok_or_else(|| {
            anyhow!(
                "target '{}' has no snapshot in state — run `sago apply` first",
                name
            )
        })?;

        let conn = cfg.connections.get(&tgt.connection).with_context(|| {
            format!(
                "target '{}' references unknown connection '{}'",
                name, tgt.connection
            )
        })?;
        let provider = build_provider(conn).await?;
        let live_snap = capture_snapshot(provider, &tgt.identifier, None).await?;

        let baseline_schema = snap.schema.to_arrow_schema()?;
        let live_schema = live_snap.schema.to_arrow_schema()?;
        let schema_drift = detect_schema_drift(&baseline_schema, &live_schema);

        let data_drift = detect_data_drift_from_stats(&snap.column_stats, &live_snap.column_stats);

        let semantic_drifts =
            compute_semantic_drift(&snap.semantic_types, &live_snap.semantic_types);

        reports.push(DiffReport {
            source_identifier: format!("baseline:{}", name),
            target_identifier: format!("live:{}", name),
            schema_drift,
            data_drift,
            semantic_drifts,
        });
    }

    if reports.is_empty() {
        println!("nothing to plan (no matching targets)");
        return Ok(());
    }

    print_terminal(&reports);
    let out = args.out.clone().unwrap_or_else(default_artifact_path);
    write_artifact(&reports, &out)?;
    println!("plan written to {}", out.display());
    Ok(())
}

fn compute_semantic_drift(
    baseline: &HashMap<String, SemanticType>,
    live: &HashMap<String, SemanticType>,
) -> Vec<SemanticDrift> {
    let mut out = Vec::new();
    for (name, b_type) in baseline {
        if let Some(l_type) = live.get(name)
            && b_type != l_type
        {
            out.push(SemanticDrift {
                field_name: name.clone(),
                source_type: b_type.clone(),
                target_type: l_type.clone(),
            });
        }
    }
    out
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
