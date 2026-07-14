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
//!
//! [`reconcile`] is the client-side counterpart: given a Merkle tree built
//! from your own copy of a dataset, it confirms the remote node's copy is
//! identical, or reports which rows diverge, without either side transferring
//! its data.
//!
//! ```no_run
//! # #[cfg(feature = "grpc")]
//! # async fn run() -> Result<(), Box<dyn std::error::Error>> {
//! use sago_core::merkle::MerkleTree;
//! use sago_sdk::grpc::{Reconciliation, SagoServiceClient, reconcile};
//!
//! let mut client = SagoServiceClient::connect("http://127.0.0.1:50051").await?;
//! let local = MerkleTree::from_batches(&my_own_batches())?;
//! match reconcile(&mut client, "public.orders", &local).await? {
//!     Reconciliation::InSync => println!("consistent with the remote copy"),
//!     Reconciliation::Diverged { divergent_rows } => {
//!         println!("rows out of sync: {divergent_rows:?}")
//!     }
//! }
//! # Ok(())
//! # }
//! # fn my_own_batches() -> Vec<arrow::record_batch::RecordBatch> { vec![] }
//! ```

use std::sync::Arc;

use sago_core::DataProvider;
use sago_core::diff::diff_datasets;
use sago_core::merkle::{MerkleTree, to_hex};
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

    async fn get_merkle_root(
        &self,
        request: Request<v1::GetMerkleRootRequest>,
    ) -> Result<Response<v1::GetMerkleRootResponse>, Status> {
        let identifier = request.into_inner().identifier;
        let tree = self.merkle_tree(&identifier).await?;
        Ok(Response::new(v1::GetMerkleRootResponse {
            root_hex: tree.root_hex(),
            leaf_count: tree.leaf_count() as u64,
        }))
    }

    async fn get_inclusion_proof(
        &self,
        request: Request<v1::GetInclusionProofRequest>,
    ) -> Result<Response<v1::GetInclusionProofResponse>, Status> {
        let req = request.into_inner();
        let tree = self.merkle_tree(&req.identifier).await?;
        let leaf_index = req.leaf_index as usize;
        let leaf = tree.leaf(leaf_index).ok_or_else(|| {
            Status::out_of_range(format!(
                "leaf_index {} out of range (dataset has {} rows)",
                req.leaf_index,
                tree.leaf_count()
            ))
        })?;
        // leaf() and proof() share the same bounds check, so this cannot fail
        // now that leaf_index has already been validated above.
        let proof = tree.proof(leaf_index).expect("index validated by leaf()");
        Ok(Response::new(v1::GetInclusionProofResponse {
            leaf_hex: to_hex(&leaf),
            steps: proof
                .steps
                .into_iter()
                .map(|s| v1::ProofStep {
                    sibling_hex: s.sibling,
                    sibling_is_left: s.sibling_is_left,
                })
                .collect(),
        }))
    }
}

impl ProviderService {
    /// Build the Merkle tree over `identifier`'s current data. Recomputed on
    /// every call rather than cached: the provider is the source of truth, and
    /// a node's whole point is to reflect its *live* data, not a stale
    /// snapshot the caller must remember to invalidate.
    async fn merkle_tree(&self, identifier: &str) -> Result<MerkleTree, Status> {
        let batches = self
            .provider
            .get_data(identifier)
            .await
            .map_err(core_err_to_status)?;
        MerkleTree::from_batches(&batches)
            .map_err(|e| Status::internal(format!("failed to build Merkle tree: {e}")))
    }
}

// ── client-side reconciliation ──────────────────────────────────────────────

/// The outcome of [`reconcile`]: whether a local dataset matches a remote
/// node's copy, and if not, which specific rows diverge.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Reconciliation {
    /// The local and remote Merkle roots match — the datasets are identical,
    /// row for row, without either side having transferred its data.
    InSync,
    /// The roots differ. `divergent_rows` lists the local row indices whose
    /// leaf hash does not match the remote node's leaf at the same index,
    /// checked against the remote root via that row's inclusion proof.
    /// A row absent from this list is *not* a live guarantee that it matches
    /// — see the `divergent_rows` doc on why only the checked prefix is
    /// covered when the two sides have a different row count.
    Diverged { divergent_rows: Vec<usize> },
}

/// Reconcile `local`, a Merkle tree built from the caller's own copy of a
/// dataset (e.g. via [`sago_core::merkle::MerkleTree::from_batches`]), against
/// `identifier` as served by `client` — without transferring either side's
/// data. This is the trust-minimised sync primitive from
/// `docs/DECENTRALIZED.md`: two domains confirm consistency, or localise a
/// divergence, by exchanging only root hashes and inclusion proofs.
///
/// When the roots already match, this makes exactly one round trip
/// (`GetMerkleRoot`) and returns [`Reconciliation::InSync`].
///
/// When they differ, one `GetInclusionProof` round trip per row in
/// `local`'s *shorter* side is made — comparing `min(local.leaf_count(),
/// remote.leaf_count())` rows — and [`Reconciliation::Diverged`] lists every
/// row whose remote-verified leaf doesn't match the local one. Rows beyond
/// the shorter side's length are never fetched: if the row counts differ, the
/// dataset has grown or shrunk, which is itself already signalled by the root
/// mismatch and is a change the caller should treat as a divergence at the
/// point the sequences run out — this function only pinpoints the divergent
/// prefix that *can* be checked leaf-by-leaf.
pub async fn reconcile<T>(
    client: &mut SagoServiceClient<T>,
    identifier: &str,
    local: &MerkleTree,
) -> std::result::Result<Reconciliation, Status>
where
    T: tonic::client::GrpcService<tonic::body::Body>,
    T::Error: Into<tonic::codegen::StdError>,
    T::ResponseBody:
        tonic::codegen::Body<Data = tonic::codegen::Bytes> + std::marker::Send + 'static,
    <T::ResponseBody as tonic::codegen::Body>::Error:
        Into<tonic::codegen::StdError> + std::marker::Send,
{
    let remote_root = client
        .get_merkle_root(v1::GetMerkleRootRequest {
            identifier: identifier.to_string(),
        })
        .await?
        .into_inner();

    if remote_root.root_hex == local.root_hex() {
        return Ok(Reconciliation::InSync);
    }

    let remote_root_hash = sago_core::merkle::from_hex(&remote_root.root_hex).ok_or_else(|| {
        Status::internal(format!(
            "server returned malformed root hash: {}",
            remote_root.root_hex
        ))
    })?;

    let checked_len = local.leaf_count().min(remote_root.leaf_count as usize);
    let mut divergent_rows = Vec::new();
    for i in 0..checked_len {
        let local_leaf = local
            .leaf(i)
            .expect("i < local.leaf_count() by the min() above");

        let resp = client
            .get_inclusion_proof(v1::GetInclusionProofRequest {
                identifier: identifier.to_string(),
                leaf_index: i as u64,
            })
            .await?
            .into_inner();

        let remote_leaf = sago_core::merkle::from_hex(&resp.leaf_hex).ok_or_else(|| {
            Status::internal(format!(
                "server returned malformed leaf hash: {}",
                resp.leaf_hex
            ))
        })?;

        let proof = sago_core::merkle::InclusionProof {
            leaf_index: i,
            steps: resp
                .steps
                .into_iter()
                .map(|s| sago_core::merkle::ProofStep {
                    sibling: s.sibling_hex,
                    sibling_is_left: s.sibling_is_left,
                })
                .collect(),
        };

        // Two independent checks: the remote leaf must actually be part of
        // the remote tree (guards against a lying/buggy server), and it must
        // match our local leaf (the actual consistency check).
        let remote_leaf_verified =
            sago_core::merkle::verify_proof(&remote_root_hash, &remote_leaf, &proof);
        if !remote_leaf_verified || remote_leaf != local_leaf {
            divergent_rows.push(i);
        }
    }

    Ok(Reconciliation::Diverged { divergent_rows })
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
