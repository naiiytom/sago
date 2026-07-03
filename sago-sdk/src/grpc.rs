//! Reference gRPC server for [`SagoService`](sago_proto::SagoServiceServer).
//!
//! [`ProviderService`] wraps any [`DataProvider`] and answers the `GetSchema`
//! and `Diff` RPCs by delegating to the same `sago-core` logic the CLI uses,
//! translating between the proto wire types and the core domain types. This is
//! the piece that lets a data domain expose its provider over the network — the
//! transport the decentralized architecture in `docs/DECENTRALIZED.md` assumes.
//!
//! Available only with the `grpc` feature.
//!
//! ```no_run
//! # #[cfg(feature = "grpc")]
//! # async fn run() -> Result<(), Box<dyn std::error::Error>> {
//! use std::sync::Arc;
//! use sago_sdk::grpc::{ProviderService, sago_service_server::SagoServiceServer};
//! use sago_core::connection::build_provider;
//! use sago_core::config::ConnectionConfig;
//!
//! let provider = build_provider(&ConnectionConfig::S3 {
//!     bucket: "data".into(), region: "us-east-1".into(), format: None,
//! }).await?;
//! let svc = ProviderService::new(provider);
//!
//! tonic::transport::Server::builder()
//!     .add_service(SagoServiceServer::new(svc))
//!     .serve("127.0.0.1:50051".parse()?)
//!     .await?;
//! # Ok(())
//! # }
//! ```

use std::sync::Arc;

use sago_core::DataProvider;
use sago_core::diff::diff_datasets;
use sago_proto::v1;
use tonic::{Request, Response, Status};

// Re-export the generated server plumbing so callers only need `sago_sdk::grpc`.
pub use sago_proto::v1::sago_service_client::SagoServiceClient;
pub use sago_proto::v1::sago_service_server::{self, SagoServiceServer};

/// A [`SagoService`](sago_service_server::SagoService) backed by a single
/// [`DataProvider`]. Both `source` and `target` identifiers in a `Diff` request
/// are resolved against this one provider (a node serves its own domain's data).
pub struct ProviderService {
    provider: Arc<dyn DataProvider>,
}

impl ProviderService {
    /// Wrap `provider` as a servable gRPC service.
    #[must_use]
    pub fn new(provider: Arc<dyn DataProvider>) -> Self {
        Self { provider }
    }
}

#[tonic::async_trait]
impl sago_service_server::SagoService for ProviderService {
    async fn get_schema(
        &self,
        request: Request<v1::GetSchemaRequest>,
    ) -> Result<Response<v1::GetSchemaResponse>, Status> {
        let identifier = request.into_inner().identifier;
        let schema = self
            .provider
            .get_schema(&identifier)
            .await
            .map_err(core_err_to_status)?;
        Ok(Response::new(v1::GetSchemaResponse {
            schema: Some(schema_to_proto(&schema)),
        }))
    }

    async fn diff(
        &self,
        request: Request<v1::DiffRequest>,
    ) -> Result<Response<v1::DiffResponse>, Status> {
        let req = request.into_inner();
        let report = diff_datasets(
            self.provider.clone(),
            &req.source_identifier,
            self.provider.clone(),
            &req.target_identifier,
        )
        .await
        .map_err(core_err_to_status)?;
        Ok(Response::new(v1::DiffResponse {
            report: Some(diff_report_to_proto(&report)),
        }))
    }
}

/// Map a `sago-core` error to a gRPC status. Not-found schema errors become
/// `NOT_FOUND`; everything else is `INTERNAL`.
fn core_err_to_status(e: sago_core::SagoError) -> Status {
    match &e {
        sago_core::SagoError::Schema(msg) => Status::not_found(msg.clone()),
        sago_core::SagoError::Config(msg) => Status::invalid_argument(msg.clone()),
        other => Status::internal(other.to_string()),
    }
}

// ── proto conversions ────────────────────────────────────────────────────────

fn schema_to_proto(schema: &arrow::datatypes::Schema) -> v1::Schema {
    v1::Schema {
        fields: schema
            .fields()
            .iter()
            .map(|f| v1::Field {
                name: f.name().clone(),
                data_type: sago_core::schema_codec::serialize_data_type(f.data_type()),
                nullable: f.is_nullable(),
            })
            .collect(),
    }
}

fn semantic_to_proto(s: &sago_core::semantic::SemanticType) -> i32 {
    use sago_core::semantic::SemanticType as S;
    let v = match s {
        S::Unknown => v1::SemanticType::Unknown,
        S::Email => v1::SemanticType::Email,
        S::CreditCard => v1::SemanticType::CreditCard,
        S::PhoneNumber => v1::SemanticType::PhoneNumber,
        S::UUID => v1::SemanticType::Uuid,
        S::IPAddress => v1::SemanticType::IpAddress,
        S::Url => v1::SemanticType::Url,
    };
    v as i32
}

fn semantic_drift_to_proto(d: &sago_core::drift::SemanticDrift) -> v1::SemanticDrift {
    v1::SemanticDrift {
        field_name: d.field_name.clone(),
        source_type: semantic_to_proto(&d.source_type),
        target_type: semantic_to_proto(&d.target_type),
    }
}

fn stats_to_proto(s: &sago_core::drift::ColumnStats) -> v1::ColumnStats {
    v1::ColumnStats {
        null_count: s.null_count as u64,
        row_count: s.row_count as u64,
        mean: s.mean,
        min: s.min,
        max: s.max,
    }
}

fn schema_drift_to_proto(d: &sago_core::drift::SchemaDrift) -> v1::SchemaDrift {
    v1::SchemaDrift {
        added_fields: d.added_fields.clone(),
        removed_fields: d.removed_fields.clone(),
        changed_types: d
            .changed_types
            .iter()
            .map(|c| v1::TypeChange {
                field_name: c.field_name.clone(),
                old_type: c.old_type.clone(),
                new_type: c.new_type.clone(),
            })
            .collect(),
        renamed_fields: d
            .renamed_fields
            .iter()
            .map(|r| v1::FieldRename {
                from: r.from.clone(),
                to: r.to.clone(),
                confidence: r.confidence,
            })
            .collect(),
    }
}

fn data_drift_to_proto(d: &sago_core::drift::DataDrift) -> v1::DataDrift {
    v1::DataDrift {
        column_drifts: d
            .column_drifts
            .iter()
            .map(|(name, c)| {
                (
                    name.clone(),
                    v1::ColumnDrift {
                        source_stats: Some(stats_to_proto(&c.source_stats)),
                        target_stats: Some(stats_to_proto(&c.target_stats)),
                        mean_drift: c.mean_drift,
                        null_count_drift: c.null_count_drift,
                        ks_statistic: c.ks_statistic,
                        ks_p_value: c.ks_p_value,
                        psi_statistic: c.psi_statistic,
                    },
                )
            })
            .collect(),
    }
}

fn diff_report_to_proto(r: &sago_core::diff::DiffReport) -> v1::DiffReport {
    v1::DiffReport {
        source_identifier: r.source_identifier.clone(),
        target_identifier: r.target_identifier.clone(),
        schema_drift: Some(schema_drift_to_proto(&r.schema_drift)),
        semantic_drifts: r
            .semantic_drifts
            .iter()
            .map(semantic_drift_to_proto)
            .collect(),
        data_drift: Some(data_drift_to_proto(&r.data_drift)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};
    use sago_core::drift::{ColumnStats, SchemaDrift, SemanticDrift, TypeChange};
    use sago_core::semantic::SemanticType;

    #[test]
    fn test_schema_to_proto_round_trips_names_and_types() {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("email", DataType::Utf8, true),
        ]);
        let p = schema_to_proto(&schema);
        assert_eq!(p.fields.len(), 2);
        assert_eq!(p.fields[0].name, "id");
        assert_eq!(p.fields[0].data_type, "Int64");
        assert!(!p.fields[0].nullable);
        assert_eq!(p.fields[1].data_type, "Utf8");
        assert!(p.fields[1].nullable);
    }

    #[test]
    fn test_semantic_enum_maps_to_proto_discriminants() {
        assert_eq!(
            semantic_to_proto(&SemanticType::Email),
            v1::SemanticType::Email as i32
        );
        assert_eq!(
            semantic_to_proto(&SemanticType::UUID),
            v1::SemanticType::Uuid as i32
        );
        assert_eq!(
            semantic_to_proto(&SemanticType::Unknown),
            v1::SemanticType::Unknown as i32
        );
    }

    #[test]
    fn test_schema_drift_to_proto_carries_renames_and_types() {
        let d = SchemaDrift {
            added_fields: vec!["a".into()],
            removed_fields: vec!["b".into()],
            changed_types: vec![TypeChange {
                field_name: "c".into(),
                old_type: "Int32".into(),
                new_type: "Int64".into(),
            }],
            renamed_fields: vec![sago_core::rename::FieldRename {
                from: "old".into(),
                to: "new".into(),
                confidence: 0.9,
                signals: sago_core::rename::RenameSignals {
                    type_match: true,
                    semantic_match: false,
                    name_similarity: 0.5,
                    stats_similarity: None,
                },
            }],
        };
        let p = schema_drift_to_proto(&d);
        assert_eq!(p.added_fields, vec!["a".to_string()]);
        assert_eq!(p.changed_types[0].new_type, "Int64");
        assert_eq!(p.renamed_fields[0].to, "new");
        assert!((p.renamed_fields[0].confidence - 0.9).abs() < 1e-9);
    }

    #[test]
    fn test_semantic_drift_to_proto() {
        let d = SemanticDrift {
            field_name: "contact".into(),
            source_type: SemanticType::Email,
            target_type: SemanticType::Unknown,
        };
        let p = semantic_drift_to_proto(&d);
        assert_eq!(p.field_name, "contact");
        assert_eq!(p.source_type, v1::SemanticType::Email as i32);
        assert_eq!(p.target_type, v1::SemanticType::Unknown as i32);
    }

    #[test]
    fn test_stats_to_proto() {
        let s = ColumnStats {
            null_count: 3,
            row_count: 10,
            mean: Some(4.2),
            min: Some(0.0),
            max: Some(9.0),
        };
        let p = stats_to_proto(&s);
        assert_eq!(p.null_count, 3);
        assert_eq!(p.row_count, 10);
        assert_eq!(p.mean, Some(4.2));
    }

    #[test]
    fn test_core_err_to_status_maps_kinds() {
        assert_eq!(
            core_err_to_status(sago_core::SagoError::Schema("nope".into())).code(),
            tonic::Code::NotFound
        );
        assert_eq!(
            core_err_to_status(sago_core::SagoError::Config("bad".into())).code(),
            tonic::Code::InvalidArgument
        );
        assert_eq!(
            core_err_to_status(sago_core::SagoError::Unknown("x".into())).code(),
            tonic::Code::Internal
        );
    }
}
