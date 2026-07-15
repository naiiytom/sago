use anyhow::{Context, Result, anyhow, bail};
use clap::Args;
use sago_core::config::Config;
use sago_core::connection::ProviderCache;
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

use crate::report::{
    OutputFormat, default_artifact_path, print_json, print_terminal, write_artifact,
};

/// Number of numeric values to retain per column when sampling the live dataset
/// for the PSI drift metric. Shares the config default so live and baseline
/// sample sizes match; large enough for a stable 10-bin PSI without
/// materializing the whole column.
const PLAN_SAMPLE_N: usize = sago_core::config::DEFAULT_SAMPLE_N;

#[derive(Args, Debug)]
pub struct PlanArgs {
    /// Plan only the named target (default: all)
    #[arg(long)]
    pub target: Option<String>,

    /// Where to write the JSON artifact
    #[arg(long)]
    pub out: Option<PathBuf>,

    /// Override the rename-detection confidence threshold in [0, 1]
    /// (default: checks.rename_confidence_threshold from Sago.toml).
    #[arg(long)]
    pub rename_threshold: Option<f64>,

    /// Output format: human-readable text (default) or JSON on stdout.
    #[arg(long, value_enum, default_value_t = OutputFormat::Text)]
    pub format: OutputFormat,
}

/// A target's computed drift report plus the data-mesh metadata (`domain`,
/// `owner`) needed to group it in `sago federate`.
pub(crate) struct TargetReport {
    pub name: String,
    pub domain: Option<String>,
    pub owner: Option<String>,
    pub report: DiffReport,
}

/// Compute baseline-vs-live drift reports for every target matching the
/// optional `target_filter` (exact name) and `domain_filter` (exact
/// `TargetConfig::domain`). Shared by `sago plan` and `sago federate` so both
/// commands see identical drift computation — only the presentation differs.
///
/// Filtering happens before any provider I/O, so scoping to one domain never
/// reaches out to another domain's connections.
pub(crate) async fn build_target_reports(
    cfg: &Config,
    state: &ProjectState,
    target_filter: Option<&str>,
    domain_filter: Option<&str>,
    rename_opts: &RenameOptions,
) -> Result<Vec<TargetReport>> {
    // A typo'd --target used to silently produce zero matching targets (and
    // thus a quiet "nothing to plan" success), indistinguishable from a
    // correctly-scoped run that legitimately has no targets. Validate the
    // name against the known set up front so a typo is a clear error.
    if let Some(filter) = target_filter
        && !cfg.targets.contains_key(filter)
    {
        bail!("'{filter}' is not a known target name (checked Sago.toml's [targets.*] entries)");
    }

    let mut reports = Vec::new();
    // Shared across every target so targets on the same named connection
    // reuse one provider/connection pool instead of each building its own —
    // see ProviderCache's doc comment for the Postgres connection-budget
    // impact this avoids.
    let providers = ProviderCache::new();
    for (name, tgt) in &cfg.targets {
        if let Some(filter) = target_filter
            && filter != name
        {
            continue;
        }
        if let Some(domain) = domain_filter
            && tgt.domain.as_deref() != Some(domain)
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
        let provider = providers.get_or_build(&tgt.connection, conn).await?;
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
            rename_opts,
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

        let semantic_drifts = compute_semantic_drift(
            &snap.semantic_types,
            &live_snap.semantic_types,
            &schema_drift,
        );

        reports.push(TargetReport {
            name: name.clone(),
            domain: tgt.domain.clone(),
            owner: tgt.owner.clone(),
            report: DiffReport {
                source_identifier: format!("baseline:{}", name),
                target_identifier: format!("live:{}", name),
                schema_drift,
                data_drift,
                semantic_drifts,
            },
        });
    }

    // Deterministic order: HashMap iteration over `cfg.targets` is unordered.
    reports.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(reports)
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

    // CLI flag overrides the configured rename threshold; otherwise use config.
    let rename_confidence = resolve_rename_threshold(
        args.rename_threshold,
        cfg.checks.rename_confidence_threshold,
    )?;
    let rename_opts = RenameOptions::with_min_confidence(rename_confidence);

    let target_reports =
        build_target_reports(&cfg, &state, args.target.as_deref(), None, &rename_opts).await?;

    if target_reports.is_empty() {
        if args.format == OutputFormat::Json {
            print_json(&[])?;
        } else {
            println!("nothing to plan (no matching targets)");
        }
        return Ok(ExitCode::SUCCESS);
    }
    let reports: Vec<DiffReport> = target_reports.into_iter().map(|tr| tr.report).collect();

    if args.format == OutputFormat::Json {
        print_json(&reports)?;
    } else {
        print_terminal(&reports);
    }

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
pub(crate) fn collect_breaches(reports: &[DiffReport], threshold: f64) -> Vec<String> {
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
    schema_drift: &sago_core::drift::SchemaDrift,
) -> Vec<SemanticDrift> {
    let mut out = Vec::new();

    // Columns present in both: report a change in inferred semantic type.
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

    // New live columns (absent from baseline) that carry a *concrete* semantic
    // type are worth flagging too — e.g. a freshly-added `ssn`/`email` column is
    // a governance signal, not a silent addition. Rename targets already surface
    // as renames, so skip them here to avoid double-reporting.
    let rename_targets: std::collections::HashSet<&str> = schema_drift
        .renamed_fields
        .iter()
        .map(|r| r.to.as_str())
        .collect();
    for (name, l_type) in live {
        if !baseline.contains_key(name)
            && *l_type != SemanticType::Unknown
            && !rename_targets.contains(name.as_str())
        {
            out.push(SemanticDrift {
                field_name: name.clone(),
                source_type: SemanticType::Unknown,
                target_type: l_type.clone(),
            });
        }
    }

    // Deterministic order (HashMap iteration is unordered).
    out.sort_by(|a, b| a.field_name.cmp(&b.field_name));
    out
}

pub(crate) fn load_config(path: &Path) -> Result<Config> {
    let content = std::fs::read_to_string(path).with_context(|| {
        format!(
            "Sago.toml not found at {} (run `sago init`)",
            path.display()
        )
    })?;
    Ok(Config::from_toml(&content)?)
}

/// The effective rename confidence: the `--rename-threshold` flag if given
/// (validated to `[0, 1]`), else the value from config.
pub(crate) fn resolve_rename_threshold(flag: Option<f64>, from_config: f64) -> Result<f64> {
    match flag {
        Some(t) if !(0.0..=1.0).contains(&t) => {
            Err(anyhow!("--rename-threshold must be in [0.0, 1.0], got {t}"))
        }
        Some(t) => Ok(t),
        None => Ok(from_config),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sago_core::drift::{ColumnDrift, ColumnStats, DataDrift, SchemaDrift};
    use sago_core::semantic::SemanticType;
    use std::collections::HashMap;

    #[test]
    fn test_resolve_rename_threshold_flag_overrides_config() {
        assert_eq!(resolve_rename_threshold(Some(0.8), 0.6).unwrap(), 0.8);
    }

    #[test]
    fn test_resolve_rename_threshold_falls_back_to_config() {
        assert_eq!(resolve_rename_threshold(None, 0.6).unwrap(), 0.6);
    }

    #[test]
    fn test_resolve_rename_threshold_rejects_out_of_range_flag() {
        assert!(resolve_rename_threshold(Some(1.5), 0.6).is_err());
        assert!(resolve_rename_threshold(Some(-0.1), 0.6).is_err());
    }

    fn drift_report_with_psi(target: &str, col: &str, psi: Option<f64>) -> DiffReport {
        let stats = ColumnStats {
            null_count: 0,
            row_count: 10,
            mean: Some(1.0),
            min: Some(0.0),
            max: Some(2.0),
            variance: None,
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
                categorical_drift: None,
            },
        );
        DiffReport {
            source_identifier: "baseline".into(),
            target_identifier: target.into(),
            schema_drift: SchemaDrift {
                added_fields: vec![],
                removed_fields: vec![],
                changed_types: vec![],
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

    fn empty_schema_drift() -> SchemaDrift {
        SchemaDrift {
            added_fields: vec![],
            removed_fields: vec![],
            changed_types: vec![],
            renamed_fields: vec![],
        }
    }

    #[test]
    fn test_compute_semantic_drift_empty_maps() {
        let result =
            compute_semantic_drift(&HashMap::new(), &HashMap::new(), &empty_schema_drift());
        assert!(result.is_empty());
    }

    #[test]
    fn test_compute_semantic_drift_identical_types_no_drift() {
        let mut baseline = HashMap::new();
        baseline.insert("email".into(), SemanticType::Email);
        baseline.insert("id".into(), SemanticType::UUID);

        let live = baseline.clone();
        let result = compute_semantic_drift(&baseline, &live, &empty_schema_drift());
        assert!(result.is_empty());
    }

    #[test]
    fn test_compute_semantic_drift_detects_change() {
        let mut baseline = HashMap::new();
        baseline.insert("contact".into(), SemanticType::Email);

        let mut live = HashMap::new();
        live.insert("contact".into(), SemanticType::Unknown);

        let result = compute_semantic_drift(&baseline, &live, &empty_schema_drift());
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

        let result = compute_semantic_drift(&baseline, &live, &empty_schema_drift());
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

        let result = compute_semantic_drift(&baseline, &live, &empty_schema_drift());
        assert!(result.is_empty());
    }

    #[test]
    fn test_compute_semantic_drift_reports_new_typed_live_column() {
        // A brand-new live column carrying a concrete semantic type is surfaced,
        // with Unknown as the (absent) baseline type.
        let baseline = HashMap::new();
        let mut live = HashMap::new();
        live.insert("ssn".into(), SemanticType::CreditCard);
        live.insert("noise".into(), SemanticType::Unknown); // must be ignored

        let result = compute_semantic_drift(&baseline, &live, &empty_schema_drift());
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].field_name, "ssn");
        assert_eq!(result[0].source_type, SemanticType::Unknown);
        assert_eq!(result[0].target_type, SemanticType::CreditCard);
    }

    #[test]
    fn test_compute_semantic_drift_skips_rename_target() {
        // A new typed live column that is actually a rename target must not be
        // double-reported (it already surfaces as a rename).
        use sago_core::rename::{FieldRename, RenameSignals};
        let baseline = HashMap::new();
        let mut live = HashMap::new();
        live.insert("email_address".into(), SemanticType::Email);

        let mut sd = empty_schema_drift();
        sd.renamed_fields = vec![FieldRename {
            from: "email".into(),
            to: "email_address".into(),
            confidence: 0.95,
            signals: RenameSignals {
                type_match: true,
                semantic_match: true,
                name_similarity: 0.6,
                stats_similarity: None,
            },
        }];

        let result = compute_semantic_drift(&baseline, &live, &sd);
        assert!(result.is_empty(), "rename target must not be reported");
    }
}
