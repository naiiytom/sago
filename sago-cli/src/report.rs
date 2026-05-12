use anyhow::{Context, Result};
use sago_core::diff::DiffReport;
use std::path::Path;

#[allow(dead_code)]
pub fn print_terminal(reports: &[DiffReport]) {
    for r in reports {
        println!("── {}  ↔  {} ──", r.source_identifier, r.target_identifier);
        let s = &r.schema_drift;
        if !s.added_fields.is_empty() {
            println!("  added fields:   {}", s.added_fields.join(", "));
        }
        if !s.removed_fields.is_empty() {
            println!("  removed fields: {}", s.removed_fields.join(", "));
        }
        if !s.changed_types.is_empty() {
            for c in &s.changed_types {
                println!(
                    "  type change:    {} ({} -> {})",
                    c.field_name, c.old_type, c.new_type
                );
            }
        }
        if !r.data_drift.column_drifts.is_empty() {
            for (col, d) in &r.data_drift.column_drifts {
                if d.mean_drift.unwrap_or(0.0).abs() > f64::EPSILON || d.null_count_drift != 0 {
                    println!(
                        "  data drift:     {} mean_drift={:?} null_count_drift={}",
                        col, d.mean_drift, d.null_count_drift,
                    );
                }
            }
        }
        if !r.semantic_drifts.is_empty() {
            for sd in &r.semantic_drifts {
                println!(
                    "  semantic drift: {} ({:?} -> {:?})",
                    sd.field_name, sd.source_type, sd.target_type,
                );
            }
        }
    }
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
    use sago_core::drift::{DataDrift, SchemaDrift};
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
