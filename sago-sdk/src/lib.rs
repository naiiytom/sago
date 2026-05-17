pub mod client;

// Re-export core types so downstream users only need sago-sdk
pub use sago_core::config::ConnectionConfig;
pub use sago_core::diff::DiffReport;
pub use sago_core::drift::{ColumnDrift, ColumnStats, DataDrift, SchemaDrift, SemanticDrift};
pub use sago_core::semantic::SemanticType;
pub use sago_core::state::TargetSnapshot;

pub use client::SagoClient;

/// One-shot diff between two arbitrary endpoints.
pub async fn diff(
    source_cfg: &ConnectionConfig,
    source_id: &str,
    target_cfg: &ConnectionConfig,
    target_id: &str,
) -> anyhow::Result<DiffReport> {
    use sago_core::connection::build_provider;
    use sago_core::diff::diff_datasets;

    let source = build_provider(source_cfg)
        .await
        .map_err(|e| anyhow::anyhow!(e))?;
    let target = build_provider(target_cfg)
        .await
        .map_err(|e| anyhow::anyhow!(e))?;
    diff_datasets(source, source_id, target, target_id)
        .await
        .map_err(|e| anyhow::anyhow!(e))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn test_re_exports_accessible() {
        let _: Option<DiffReport> = None;
        let _: Option<TargetSnapshot> = None;
        let _: Option<ConnectionConfig> = None;
    }

    #[tokio::test]
    async fn test_client_new_does_not_panic() {
        let cfg = ConnectionConfig::Postgres {
            url: "postgres://localhost/test".into(),
        };
        let _client = SagoClient::new(cfg);
    }

    #[test]
    fn test_diff_report_fields_accessible() {
        let report = DiffReport {
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
        };
        assert_eq!(report.source_identifier, "src");
        assert!(report.schema_drift.added_fields.is_empty());
    }

    #[test]
    fn test_column_stats_accessible() {
        let stats = ColumnStats {
            null_count: 0,
            row_count: 10,
            mean: Some(5.0),
            min: Some(1.0),
            max: Some(9.0),
        };
        assert_eq!(stats.row_count, 10);
        assert_eq!(stats.mean, Some(5.0));
    }

    #[test]
    fn test_connection_config_serializable() {
        let cfg = ConnectionConfig::Postgres {
            url: "postgres://localhost/mydb".into(),
        };
        match &cfg {
            ConnectionConfig::Postgres { url } => assert!(url.contains("localhost")),
            _ => panic!("expected Postgres"),
        }
    }
}
