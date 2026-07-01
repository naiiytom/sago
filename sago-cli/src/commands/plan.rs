use anyhow::{Context, Result, anyhow, bail};
use clap::Args;
use sago_core::config::Config;
use sago_core::connection::build_provider;
use sago_core::diff::DiffReport;
use sago_core::drift::{
    SemanticDrift, detect_data_drift_from_stats, detect_schema_drift, psi_from_samples,
};
use sago_core::rename::{RenameOptions, profile_columns, refine_renames};
use sago_core::semantic::SemanticType;
use sago_core::state::{ProjectState, capture_snapshot};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use crate::report::{default_artifact_path, print_terminal, write_artifact};

/// Number of numeric values to retain per column when sampling the live dataset
/// for the PSI drift metric. Matches the config default; large enough for a
/// stable 10-bin PSI without materializing the whole column.
const PLAN_SAMPLE_N: usize = 1000;

#[derive(Args, Debug)]
pub struct PlanArgs {
    /// Plan only the named target (default: all)
    #[arg(long)]
    pub target: Option<String>,

    /// Where to write the JSON artifact
    #[arg(long)]
    pub out: Option<PathBuf>,
}

pub async fn run(args: &PlanArgs) -> Result<ExitCode> {
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
    let threshold = cfg.checks.drift_threshold;

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
        // Capture live samples so we can compute the (scale-free) PSI drift
        // metric against the persisted baseline samples and gate on
        // `checks.drift_threshold`.
        let live_snap = capture_snapshot(provider, &tgt.identifier, Some(PLAN_SAMPLE_N)).await?;

        let baseline_schema = snap.schema.to_arrow_schema()?;
        let live_schema = live_snap.schema.to_arrow_schema()?;
        let mut schema_drift = detect_schema_drift(&baseline_schema, &live_schema);

        // Recognise renames using the persisted baseline signals vs. the live ones.
        let baseline_profiles =
            profile_columns(&baseline_schema, &snap.semantic_types, &snap.column_stats);
        let live_profiles = profile_columns(
            &live_schema,
            &live_snap.semantic_types,
            &live_snap.column_stats,
        );
        refine_renames(
            &mut schema_drift,
            &baseline_profiles,
            &live_profiles,
            &RenameOptions::default(),
        );

        let mut data_drift =
            detect_data_drift_from_stats(&snap.column_stats, &live_snap.column_stats);

        // Populate PSI per column from baseline vs. live samples when both sides
        // persisted them; without samples PSI stays None and the column simply
        // doesn't participate in threshold gating.
        if let (Some(base_samples), Some(live_samples)) = (&snap.samples, &live_snap.samples) {
            for (col, drift) in data_drift.column_drifts.iter_mut() {
                if let (Some(b), Some(l)) = (base_samples.get(col), live_samples.get(col)) {
                    drift.psi_statistic = psi_from_samples(b, l);
                }
            }
        }

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
        return Ok(ExitCode::SUCCESS);
    }

    print_terminal(&reports);

    // Gate on the configured drift threshold: any column whose PSI exceeds it is
    // a breach. `sago plan` then exits non-zero so CI pipelines can fail on drift.
    let breaches = collect_breaches(&reports, threshold);

    let out = args.out.clone().unwrap_or_else(default_artifact_path);
    write_artifact(&reports, &out)?;
    println!("plan written to {}", out.display());

    if breaches.is_empty() {
        Ok(ExitCode::SUCCESS)
    } else {
        eprintln!(
            "drift threshold {:.4} exceeded by {} column(s): {}",
            threshold,
            breaches.len(),
            breaches.join(", ")
        );
        Ok(ExitCode::FAILURE)
    }
}

/// Names (`target:column`) of every column whose PSI breaches `threshold`.
fn collect_breaches(reports: &[DiffReport], threshold: f64) -> Vec<String> {
    let mut breaches = Vec::new();
    for r in reports {
        for (col, drift) in &r.data_drift.column_drifts {
            if drift.breaches_threshold(threshold) {
                breaches.push(format!("{}:{}", r.target_identifier, col));
            }
        }
    }
    breaches.sort();
    breaches
}

pub(crate) fn compute_semantic_drift(
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

#[cfg(test)]
mod tests {
    use super::*;
    use sago_core::drift::{ColumnDrift, ColumnStats, DataDrift, SchemaDrift};
    use sago_core::semantic::SemanticType;
    use std::collections::HashMap;

    fn drift_report_with_psi(target: &str, col: &str, psi: Option<f64>) -> DiffReport {
        let stats = ColumnStats {
            null_count: 0,
            row_count: 10,
            mean: Some(1.0),
            min: Some(0.0),
            max: Some(2.0),
        };
        let mut column_drifts = HashMap::new();
        column_drifts.insert(
            col.to_string(),
            ColumnDrift {
                source_stats: stats.clone(),
                target_stats: stats,
                mean_drift: Some(0.0),
                null_count_drift: 0,
                ks_statistic: None,
                ks_p_value: None,
                psi_statistic: psi,
            },
        );
        DiffReport {
            source_identifier: "baseline".into(),
            target_identifier: target.into(),
            schema_drift: SchemaDrift {
                added_fields: vec![],
                removed_fields: vec![],
                changed_types: vec![],
                semantic_drifts: vec![],
                renamed_fields: vec![],
            },
            data_drift: DataDrift { column_drifts },
            semantic_drifts: vec![],
        }
    }

    #[test]
    fn test_collect_breaches_flags_psi_over_threshold() {
        let reports = vec![drift_report_with_psi("live:t", "score", Some(0.4))];
        let breaches = collect_breaches(&reports, 0.25);
        assert_eq!(breaches, vec!["live:t:score".to_string()]);
    }

    #[test]
    fn test_collect_breaches_ignores_psi_below_threshold() {
        let reports = vec![drift_report_with_psi("live:t", "score", Some(0.05))];
        assert!(collect_breaches(&reports, 0.25).is_empty());
    }

    #[test]
    fn test_collect_breaches_ignores_missing_psi() {
        // A large threshold with no PSI (e.g. non-numeric column) must not breach.
        let reports = vec![drift_report_with_psi("live:t", "name", None)];
        assert!(collect_breaches(&reports, 0.0).is_empty());
    }

    #[test]
    fn test_compute_semantic_drift_empty_maps() {
        let result = compute_semantic_drift(&HashMap::new(), &HashMap::new());
        assert!(result.is_empty());
    }

    #[test]
    fn test_compute_semantic_drift_identical_types_no_drift() {
        let mut baseline = HashMap::new();
        baseline.insert("email".into(), SemanticType::Email);
        baseline.insert("id".into(), SemanticType::UUID);

        let live = baseline.clone();
        let result = compute_semantic_drift(&baseline, &live);
        assert!(result.is_empty());
    }

    #[test]
    fn test_compute_semantic_drift_detects_change() {
        let mut baseline = HashMap::new();
        baseline.insert("contact".into(), SemanticType::Email);

        let mut live = HashMap::new();
        live.insert("contact".into(), SemanticType::Unknown);

        let result = compute_semantic_drift(&baseline, &live);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].field_name, "contact");
        assert_eq!(result[0].source_type, SemanticType::Email);
        assert_eq!(result[0].target_type, SemanticType::Unknown);
    }

    #[test]
    fn test_compute_semantic_drift_detects_multiple_changes() {
        let mut baseline = HashMap::new();
        baseline.insert("a".into(), SemanticType::Email);
        baseline.insert("b".into(), SemanticType::UUID);
        baseline.insert("c".into(), SemanticType::PhoneNumber);

        let mut live = HashMap::new();
        live.insert("a".into(), SemanticType::Unknown);
        live.insert("b".into(), SemanticType::UUID); // unchanged
        live.insert("c".into(), SemanticType::Unknown);

        let result = compute_semantic_drift(&baseline, &live);
        assert_eq!(result.len(), 2);
        let names: Vec<&str> = result.iter().map(|d| d.field_name.as_str()).collect();
        assert!(names.contains(&"a"));
        assert!(names.contains(&"c"));
        assert!(!names.contains(&"b"));
    }

    #[test]
    fn test_compute_semantic_drift_field_absent_in_live_is_ignored() {
        let mut baseline = HashMap::new();
        baseline.insert("email".into(), SemanticType::Email);
        baseline.insert("dropped".into(), SemanticType::UUID);

        let mut live = HashMap::new();
        live.insert("email".into(), SemanticType::Email);
        // "dropped" is absent from live

        let result = compute_semantic_drift(&baseline, &live);
        assert!(result.is_empty());
    }
}
