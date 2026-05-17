use anyhow::{Context, Result};
use sago_core::diff::DiffReport;
use std::path::Path;

pub fn print_terminal(reports: &[DiffReport]) {
    let _ = print_terminal_to(reports, &mut std::io::stdout());
}

pub fn print_terminal_to<W: std::io::Write>(
    reports: &[DiffReport],
    w: &mut W,
) -> std::io::Result<()> {
    for r in reports {
        writeln!(
            w,
            "── {}  ↔  {} ──",
            r.source_identifier, r.target_identifier
        )?;
        let s = &r.schema_drift;
        if !s.added_fields.is_empty() {
            writeln!(w, "  added fields:   {}", s.added_fields.join(", "))?;
        }
        if !s.removed_fields.is_empty() {
            writeln!(w, "  removed fields: {}", s.removed_fields.join(", "))?;
        }
        if !s.changed_types.is_empty() {
            for c in &s.changed_types {
                writeln!(
                    w,
                    "  type change:    {} ({} -> {})",
                    c.field_name, c.old_type, c.new_type
                )?;
            }
        }
        if !r.data_drift.column_drifts.is_empty() {
            for (col, d) in &r.data_drift.column_drifts {
                if d.mean_drift.unwrap_or(0.0).abs() > f64::EPSILON || d.null_count_drift != 0 {
                    writeln!(
                        w,
                        "  data drift:     {} mean_drift={:?} null_count_drift={}",
                        col, d.mean_drift, d.null_count_drift,
                    )?;
                }
            }
        }
        if !r.semantic_drifts.is_empty() {
            for sd in &r.semantic_drifts {
                writeln!(
                    w,
                    "  semantic drift: {} ({:?} -> {:?})",
                    sd.field_name, sd.source_type, sd.target_type,
                )?;
            }
        }
    }
    Ok(())
}

#[allow(dead_code)]
pub fn write_artifact(reports: &[DiffReport], path: &Path) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let json = serde_json::to_string_pretty(reports).context("failed to serialize plan reports")?;
    std::fs::write(path, json).with_context(|| format!("failed to write {}", path.display()))?;
    Ok(())
}

#[allow(dead_code)]
pub fn default_artifact_path() -> std::path::PathBuf {
    let ts = chrono::Utc::now().format("%Y%m%dT%H%M%SZ").to_string();
    std::path::PathBuf::from(".sago")
        .join("plans")
        .join(format!("{}.json", ts))
}

#[cfg(test)]
mod tests {
    use super::*;
    use sago_core::diff::DiffReport;
    use sago_core::drift::SemanticDrift;
    use sago_core::drift::{ColumnDrift, ColumnStats, DataDrift, SchemaDrift, TypeChange};
    use sago_core::semantic::SemanticType;
    use std::collections::HashMap;

    fn empty_report() -> DiffReport {
        DiffReport {
            source_identifier: "src".into(),
            target_identifier: "tgt".into(),
            schema_drift: SchemaDrift {
                added_fields: vec![],
                removed_fields: vec![],
                changed_types: vec![],
                semantic_drifts: vec![],
            },
            data_drift: DataDrift {
                column_drifts: HashMap::new(),
            },
            semantic_drifts: vec![],
        }
    }

    fn capture(reports: &[DiffReport]) -> String {
        let mut buf = Vec::new();
        print_terminal_to(reports, &mut buf).unwrap();
        String::from_utf8(buf).unwrap()
    }

    // ── print_terminal_to ────────────────────────────────────────────────────

    #[test]
    fn test_print_terminal_header_contains_identifiers() {
        let out = capture(&[empty_report()]);
        assert!(out.contains("src"));
        assert!(out.contains("tgt"));
    }

    #[test]
    fn test_print_terminal_no_drift_is_quiet() {
        let out = capture(&[empty_report()]);
        assert!(!out.contains("added"));
        assert!(!out.contains("removed"));
        assert!(!out.contains("type change"));
        assert!(!out.contains("data drift"));
        assert!(!out.contains("semantic drift"));
    }

    #[test]
    fn test_print_terminal_schema_added_and_removed_fields() {
        let mut r = empty_report();
        r.schema_drift.added_fields = vec!["email".into()];
        r.schema_drift.removed_fields = vec!["phone".into()];
        let out = capture(&[r]);
        assert!(out.contains("added fields"));
        assert!(out.contains("email"));
        assert!(out.contains("removed fields"));
        assert!(out.contains("phone"));
    }

    #[test]
    fn test_print_terminal_type_change() {
        let mut r = empty_report();
        r.schema_drift.changed_types = vec![TypeChange {
            field_name: "age".into(),
            old_type: "Int32".into(),
            new_type: "Int64".into(),
        }];
        let out = capture(&[r]);
        assert!(out.contains("type change"));
        assert!(out.contains("age"));
        assert!(out.contains("Int32"));
        assert!(out.contains("Int64"));
    }

    #[test]
    fn test_print_terminal_data_drift_shown_when_mean_drifts() {
        let mut drifts = HashMap::new();
        let stats = ColumnStats {
            null_count: 0,
            row_count: 100,
            mean: Some(1.0),
            min: Some(0.0),
            max: Some(2.0),
        };
        drifts.insert(
            "score".into(),
            ColumnDrift {
                source_stats: stats.clone(),
                target_stats: ColumnStats {
                    mean: Some(5.0),
                    ..stats
                },
                mean_drift: Some(4.0),
                null_count_drift: 0,
                ks_statistic: None,
                ks_p_value: None,
                psi_statistic: None,
            },
        );
        let mut r = empty_report();
        r.data_drift = DataDrift {
            column_drifts: drifts,
        };
        let out = capture(&[r]);
        assert!(out.contains("data drift"));
        assert!(out.contains("score"));
    }

    #[test]
    fn test_print_terminal_data_drift_hidden_when_no_drift() {
        let mut drifts = HashMap::new();
        let stats = ColumnStats {
            null_count: 0,
            row_count: 10,
            mean: Some(1.0),
            min: Some(1.0),
            max: Some(1.0),
        };
        drifts.insert(
            "stable".into(),
            ColumnDrift {
                source_stats: stats.clone(),
                target_stats: stats,
                mean_drift: Some(0.0),
                null_count_drift: 0,
                ks_statistic: None,
                ks_p_value: None,
                psi_statistic: None,
            },
        );
        let mut r = empty_report();
        r.data_drift = DataDrift {
            column_drifts: drifts,
        };
        let out = capture(&[r]);
        assert!(!out.contains("data drift"));
    }

    #[test]
    fn test_print_terminal_semantic_drift() {
        let mut r = empty_report();
        r.semantic_drifts = vec![SemanticDrift {
            field_name: "contact".into(),
            source_type: SemanticType::Email,
            target_type: SemanticType::Unknown,
        }];
        let out = capture(&[r]);
        assert!(out.contains("semantic drift"));
        assert!(out.contains("contact"));
        assert!(out.contains("Email"));
        assert!(out.contains("Unknown"));
    }

    #[test]
    fn test_print_terminal_multiple_reports() {
        let mut r1 = empty_report();
        r1.source_identifier = "left".into();
        r1.target_identifier = "right".into();
        let mut r2 = empty_report();
        r2.source_identifier = "a".into();
        r2.target_identifier = "b".into();
        let out = capture(&[r1, r2]);
        assert!(out.contains("left"));
        assert!(out.contains("right"));
        assert!(out.contains("── a"));
        assert!(out.contains("b"));
    }

    #[test]
    fn test_write_artifact_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("out.json");
        write_artifact(&[empty_report()], &path).unwrap();
        let content = std::fs::read_to_string(&path).unwrap();
        let parsed: Vec<DiffReport> = serde_json::from_str(&content).unwrap();
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].source_identifier, "src");
    }

    #[test]
    fn test_default_artifact_path_under_dot_sago_plans() {
        let p = default_artifact_path();
        let s = p.to_string_lossy();
        assert!(s.contains(".sago"));
        assert!(s.contains("plans"));
        assert!(s.ends_with(".json"));
    }
}
