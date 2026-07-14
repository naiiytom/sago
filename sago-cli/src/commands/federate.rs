use anyhow::Result;
use clap::Args;
use sago_core::diff::DiffReport;
use sago_core::rename::RenameOptions;
use sago_core::state::ProjectState;
use std::cmp::Ordering;
use std::path::PathBuf;
use std::process::ExitCode;

use crate::commands::plan::{
    TargetReport, build_target_reports, collect_breaches, load_config, resolve_rename_threshold,
};
use crate::report::{default_artifact_path, print_terminal_to, write_artifact};

#[derive(Args, Debug)]
pub struct FederateArgs {
    /// Federate only the named domain (default: every domain, plus any
    /// targets with no `domain` set)
    #[arg(long)]
    pub domain: Option<String>,

    /// Where to write the JSON artifact
    #[arg(long)]
    pub out: Option<PathBuf>,

    /// Override the rename-detection confidence threshold in [0, 1]
    /// (default: checks.rename_confidence_threshold from Sago.toml).
    #[arg(long)]
    pub rename_threshold: Option<f64>,
}

pub async fn run(args: &FederateArgs) -> Result<ExitCode> {
    let cwd = std::env::current_dir()?;
    let cfg_path = cwd.join("Sago.toml");
    let state_path = cwd.join(".sago").join("state.json");

    let cfg = load_config(&cfg_path)?;
    if !state_path.exists() {
        anyhow::bail!(
            "no state file at {} — run `sago apply` first",
            state_path.display()
        );
    }
    let state = ProjectState::load(&state_path)?;
    let threshold = cfg.checks.drift_threshold;

    let rename_confidence = resolve_rename_threshold(
        args.rename_threshold,
        cfg.checks.rename_confidence_threshold,
    )?;
    let rename_opts = RenameOptions::with_min_confidence(rename_confidence);

    let target_reports =
        build_target_reports(&cfg, &state, None, args.domain.as_deref(), &rename_opts).await?;

    if target_reports.is_empty() {
        match &args.domain {
            Some(d) => println!("nothing to federate (no targets in domain '{d}')"),
            None => println!("nothing to federate (no targets configured)"),
        }
        return Ok(ExitCode::SUCCESS);
    }

    print_federated(&target_reports);

    let reports: Vec<DiffReport> = target_reports.iter().map(|tr| tr.report.clone()).collect();
    let breaches = collect_breaches(&reports, threshold);

    let out = args.out.clone().unwrap_or_else(default_artifact_path);
    write_artifact(&reports, &out)?;
    println!("federated plan written to {}", out.display());

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

/// Order domains alphabetically, with untagged targets (`domain = None`)
/// grouped last under "(unassigned)" rather than interleaved.
fn domain_sort_key(a: &Option<&str>, b: &Option<&str>) -> Ordering {
    match (a, b) {
        (None, None) => Ordering::Equal,
        (None, Some(_)) => Ordering::Greater,
        (Some(_), None) => Ordering::Less,
        (Some(x), Some(y)) => x.cmp(y),
    }
}

fn print_federated(target_reports: &[TargetReport]) {
    let _ = print_federated_to(target_reports, &mut std::io::stdout());
}

fn print_federated_to<W: std::io::Write>(
    target_reports: &[TargetReport],
    w: &mut W,
) -> std::io::Result<()> {
    let mut domains: Vec<Option<&str>> = target_reports
        .iter()
        .map(|tr| tr.domain.as_deref())
        .collect();
    domains.sort_by(domain_sort_key);
    domains.dedup();

    for domain in domains {
        let label = domain.unwrap_or("(unassigned)");
        writeln!(w, "=== domain: {label} ===")?;
        for tr in target_reports
            .iter()
            .filter(|tr| tr.domain.as_deref() == domain)
        {
            if let Some(owner) = &tr.owner {
                writeln!(w, "  owner: {owner}")?;
            }
            print_terminal_to(std::slice::from_ref(&tr.report), w)?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use sago_core::diff::DiffReport;
    use sago_core::drift::{DataDrift, SchemaDrift};

    fn target_report(name: &str, domain: Option<&str>, owner: Option<&str>) -> TargetReport {
        TargetReport {
            name: name.to_string(),
            domain: domain.map(String::from),
            owner: owner.map(String::from),
            report: DiffReport {
                source_identifier: format!("baseline:{name}"),
                target_identifier: format!("live:{name}"),
                schema_drift: SchemaDrift {
                    added_fields: vec![],
                    removed_fields: vec![],
                    changed_types: vec![],
                    renamed_fields: vec![],
                },
                data_drift: DataDrift {
                    column_drifts: Default::default(),
                },
                semantic_drifts: vec![],
            },
        }
    }

    fn capture(target_reports: &[TargetReport]) -> String {
        let mut buf = Vec::new();
        print_federated_to(target_reports, &mut buf).unwrap();
        String::from_utf8(buf).unwrap()
    }

    #[test]
    fn test_print_federated_groups_by_domain() {
        let reports = vec![
            target_report("orders", Some("sales"), Some("sales-team")),
            target_report("users", None, None),
            target_report("invoices", Some("finance"), None),
        ];
        let out = capture(&reports);
        assert!(out.contains("=== domain: sales ==="));
        assert!(out.contains("=== domain: finance ==="));
        assert!(out.contains("=== domain: (unassigned) ==="));
        assert!(out.contains("owner: sales-team"));
    }

    #[test]
    fn test_print_federated_domains_sorted_unassigned_last() {
        let reports = vec![
            target_report("users", None, None),
            target_report("invoices", Some("finance"), None),
            target_report("orders", Some("sales"), None),
        ];
        let out = capture(&reports);
        let finance = out.find("finance").unwrap();
        let sales = out.find("sales").unwrap();
        let unassigned = out.find("(unassigned)").unwrap();
        assert!(finance < sales, "domains should be alphabetically sorted");
        assert!(
            sales < unassigned,
            "unassigned targets should be grouped last"
        );
    }

    #[test]
    fn test_print_federated_no_owner_line_when_absent() {
        let reports = vec![target_report("orders", Some("sales"), None)];
        let out = capture(&reports);
        assert!(!out.contains("owner:"));
    }

    #[test]
    fn test_print_federated_single_domain_no_stray_groups() {
        let reports = vec![target_report("orders", Some("sales"), None)];
        let out = capture(&reports);
        assert_eq!(out.matches("=== domain:").count(), 1);
    }
}
