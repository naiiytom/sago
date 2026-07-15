//! # sago-sdk
//!
//! High-level Rust SDK for [Sago](https://github.com/naiiytom/sago), a
//! declarative data-reliability engine. `sago-sdk` is the single entry point for
//! downstream consumers: it re-exports the core domain types and traits so you
//! do not need a direct dependency on `sago-core`.
//!
//! ## Quick start
//!
//! ```no_run
//! use sago_sdk::{SagoClient, ConnectionConfig};
//!
//! # async fn run() -> sago_sdk::Result<()> {
//! // Snapshot a live dataset through a managed connection.
//! let cfg = ConnectionConfig::Postgres { url: "postgres://localhost/db".into() };
//! let client = SagoClient::new(cfg);
//! let snap = client.snapshot("public.users", Some(1000)).await?;
//! println!("captured {} columns", snap.schema.fields.len());
//! # Ok(())
//! # }
//! ```
//!
//! One-shot comparison of two arbitrary endpoints without a baseline:
//!
//! ```no_run
//! # async fn run() -> sago_sdk::Result<()> {
//! use sago_sdk::ConnectionConfig;
//! let src = ConnectionConfig::Postgres { url: "postgres://localhost/db".into() };
//! let dst = ConnectionConfig::S3 { bucket: "archive".into(), region: "us-east-1".into(), format: None };
//! let report = sago_sdk::diff(&src, "public.users", &dst, "users.parquet").await?;
//! println!("added: {:?}", report.schema_drift.added_fields);
//! # Ok(())
//! # }
//! ```
//!
//! ## Custom providers
//!
//! To integrate a data source Sago doesn't ship, implement [`SchemaProvider`]
//! and [`DataProvider`] (both re-exported here) and pass your provider to the
//! free [`diff`] function's building blocks in `sago_core`.

pub mod client;

/// Reference gRPC server/client for `SagoService` (requires the `grpc` feature).
#[cfg(feature = "grpc")]
pub mod grpc;

// Re-export core types so downstream users only need sago-sdk
pub use sago_core::config::ConnectionConfig;
pub use sago_core::diff::{DiffReport, diff_datasets, diff_datasets_with_options};
pub use sago_core::drift::{ColumnDrift, ColumnStats, DataDrift, SchemaDrift, SemanticDrift};
pub use sago_core::merge::{ConflictKind, MergeConflict, MergeResult, three_way_merge};
pub use sago_core::merkle::{InclusionProof, MerkleTree, ProofStep, verify_proof};
pub use sago_core::rename::{
    ColumnProfile, DEFAULT_MIN_CONFIDENCE, FieldRename, RenameOptions, RenameSignals,
};
pub use sago_core::semantic::SemanticType;
pub use sago_core::state::TargetSnapshot;

// Re-export the provider traits so downstream users can implement custom data
// sources without taking a direct dependency on `sago-core`.
pub use sago_core::{DataProvider, SchemaProvider};

// Re-export the core error/result types so SDK consumers get a stable, typed
// error surface and are not forced to depend on `anyhow`.
pub use sago_core::{Result, SagoError};

pub use client::SagoClient;

/// One-shot diff between two arbitrary endpoints.
pub async fn diff(
    source_cfg: &ConnectionConfig,
    source_id: &str,
    target_cfg: &ConnectionConfig,
    target_id: &str,
) -> Result<DiffReport> {
    use sago_core::connection::build_provider;
    use sago_core::diff::diff_datasets;

    let source = build_provider(source_cfg).await?;
    let target = build_provider(target_cfg).await?;
    diff_datasets(source, source_id, target, target_id).await
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

    #[test]
    fn test_provider_traits_reexported() {
        // Custom-provider authors must be able to name the traits via sago-sdk
        // alone. A trivial impl proves both traits are in scope and object-safe.
        use arrow::datatypes::Schema;
        use arrow::record_batch::RecordBatch;

        struct Custom;

        #[async_trait::async_trait]
        impl SchemaProvider for Custom {
            async fn get_schema(&self, _id: &str) -> Result<Schema> {
                Ok(Schema::empty())
            }
        }

        #[async_trait::async_trait]
        impl DataProvider for Custom {
            async fn get_data(&self, _id: &str) -> Result<Vec<RecordBatch>> {
                Ok(vec![])
            }
        }

        let _boxed: Box<dyn DataProvider> = Box::new(Custom);
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
                renamed_fields: vec![],
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
            variance: None,
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
