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
            schema: Some(schema_to_proto(&schema)?),
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
        let (leaf, proof) = leaf_and_proof(&tree, req.leaf_index)?;
        Ok(Response::new(proof_to_proto(&leaf, proof)))
    }

    async fn get_inclusion_proofs(
        &self,
        request: Request<v1::GetInclusionProofsRequest>,
    ) -> Result<Response<v1::GetInclusionProofsResponse>, Status> {
        let req = request.into_inner();
        // Built once and reused for every requested index, rather than once
        // per index (as repeated GetInclusionProof calls would each trigger
        // internally): a full reconcile() checking N rows previously cost N
        // full-dataset fetches and Merkle tree rebuilds; this RPC brings it
        // down to one.
        let tree = self.merkle_tree(&req.identifier).await?;
        let mut proofs = Vec::with_capacity(req.leaf_indices.len());
        let mut found = Vec::with_capacity(req.leaf_indices.len());
        for &idx in &req.leaf_indices {
            match leaf_and_proof(&tree, idx) {
                Ok((leaf, proof)) => {
                    proofs.push(proof_to_proto(&leaf, proof));
                    found.push(true);
                }
                Err(_) => {
                    proofs.push(v1::GetInclusionProofResponse::default());
                    found.push(false);
                }
            }
        }
        Ok(Response::new(v1::GetInclusionProofsResponse { proofs, found }))
    }
}

/// The leaf hash and inclusion proof at `leaf_index`, or an out-of-range
/// error. `leaf_index` arrives over the wire as a `u64` (proto has no
/// native `usize`); converting with `try_from` rather than `as usize`
/// matters on a 32-bit build target, where `as` would silently truncate an
/// out-of-range index (e.g. 2^32) down to an in-range one instead of
/// rejecting it.
fn leaf_and_proof(
    tree: &MerkleTree,
    leaf_index: u64,
) -> Result<(sago_core::merkle::Hash, sago_core::merkle::InclusionProof), Status> {
    let out_of_range = || {
        Status::out_of_range(format!(
            "leaf_index {} out of range (dataset has {} rows)",
            leaf_index,
            tree.leaf_count()
        ))
    };
    let idx = usize::try_from(leaf_index).map_err(|_| out_of_range())?;
    let leaf = tree.leaf(idx).ok_or_else(out_of_range)?;
    // leaf() and proof() share the same bounds check, so this cannot fail
    // now that idx has already been validated above.
    let proof = tree.proof(idx).expect("index validated by leaf()");
    Ok((leaf, proof))
}

fn proof_to_proto(
    leaf: &sago_core::merkle::Hash,
    proof: sago_core::merkle::InclusionProof,
) -> v1::GetInclusionProofResponse {
    v1::GetInclusionProofResponse {
        leaf_hex: to_hex(leaf),
        steps: proof
            .steps
            .into_iter()
            .map(|s| v1::ProofStep {
                sibling_hex: s.sibling,
                sibling_is_left: s.sibling_is_left,
            })
            .collect(),
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

    // One batched round trip for every row to check, rather than one
    // GetInclusionProof call per row: the server builds/fetches the
    // dataset's Merkle tree once for this whole request instead of once per
    // row, turning what was an O(n) full-dataset-rebuild cost per
    // reconcile() into a single rebuild.
    let leaf_indices: Vec<u64> = (0..checked_len as u64).collect();
    let resp = client
        .get_inclusion_proofs(v1::GetInclusionProofsRequest {
            identifier: identifier.to_string(),
            leaf_indices: leaf_indices.clone(),
        })
        .await?
        .into_inner();

    if resp.proofs.len() != checked_len || resp.found.len() != checked_len {
        return Err(Status::internal(format!(
            "server returned {} proofs for {} requested indices",
            resp.proofs.len(),
            checked_len
        )));
    }

    let mut divergent_rows = Vec::new();
    for (i, (proof_resp, &was_found)) in resp.proofs.iter().zip(&resp.found).enumerate() {
        if !was_found {
            // The server's dataset is shorter than checked_len implied (a
            // race with a concurrent write, or a misbehaving server) — treat
            // as divergent rather than silently skipping it.
            divergent_rows.push(i);
            continue;
        }

        let local_leaf = local
            .leaf(i)
            .expect("i < local.leaf_count() by the min() above");

        let remote_leaf =
            sago_core::merkle::from_hex(&proof_resp.leaf_hex).ok_or_else(|| {
                Status::internal(format!(
                    "server returned malformed leaf hash: {}",
                    proof_resp.leaf_hex
                ))
            })?;

        let proof = sago_core::merkle::InclusionProof {
            leaf_index: i,
            steps: proof_resp
                .steps
                .iter()
                .map(|s| sago_core::merkle::ProofStep {
                    sibling: s.sibling_hex.clone(),
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

// ── proto conversions (core -> proto) ────────────────────────────────────────

/// Converts `schema` to its proto form, or an `INVALID_ARGUMENT`/`INTERNAL`
/// status if any field's Arrow type falls outside `schema_codec`'s
/// serialize/parse whitelist (e.g. `Decimal128`, `List`, `Struct`, `UInt*`
/// beyond what's supported). Previously this silently fell back to Arrow's
/// `Debug` string, which the RPC always returned success for even though
/// `parse_data_type` — the crate's own documented inverse — could never
/// parse it back, losing the schema over the wire with no error anywhere.
fn schema_to_proto(schema: &arrow::datatypes::Schema) -> Result<v1::Schema, Status> {
    let fields = schema
        .fields()
        .iter()
        .map(|f| {
            let data_type = sago_core::schema_codec::serialize_data_type(f.data_type());
            // serialize_data_type never fails outright, but round-trips only
            // for its supported whitelist; verify it parses back before
            // handing the string to a client who will call parse_data_type
            // on it, rather than shipping a string that's already known to
            // be unparseable.
            if sago_core::schema_codec::parse_data_type(&data_type).is_err() {
                return Err(Status::invalid_argument(format!(
                    "column '{}' has Arrow type {:?}, which schema_codec cannot round-trip through gRPC",
                    f.name(),
                    f.data_type()
                )));
            }
            Ok(v1::Field {
                name: f.name().clone(),
                data_type,
                nullable: f.is_nullable(),
                metadata: f.metadata().clone(),
            })
        })
        .collect::<Result<Vec<_>, Status>>()?;
    Ok(v1::Schema { fields })
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
        variance: s.variance,
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
                        categorical_drift: c.categorical_drift,
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

// ── proto conversions (proto -> core) ────────────────────────────────────────
//
// Only the core->proto direction existed before: a client calling
// get_schema/diff via SagoServiceClient got back raw v1 types (e.g.
// semantic_drifts[i].source_type as a bare i32) with no SDK-provided,
// checked path back to sago_core types — asymmetric with reconcile(), which
// fully wraps the Merkle RPCs and returns a core-friendly Reconciliation
// enum. These give every RPC response the same treatment.

/// A conversion error between a proto message and its `sago-core` domain
/// type: a required field was `None`, or a value was outside its expected
/// range/whitelist.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ProtoConvertError {
    #[error("missing required field '{0}'")]
    MissingField(&'static str),
    #[error("invalid data: {0}")]
    Invalid(String),
}

/// Converts a proto [`v1::Schema`] back to an Arrow [`arrow::datatypes::Schema`].
pub fn proto_to_schema(s: &v1::Schema) -> Result<arrow::datatypes::Schema, ProtoConvertError> {
    let fields = s
        .fields
        .iter()
        .map(|f| {
            let dt = sago_core::schema_codec::parse_data_type(&f.data_type)
                .map_err(|e| ProtoConvertError::Invalid(e.to_string()))?;
            Ok(arrow::datatypes::Field::new(&f.name, dt, f.nullable)
                .with_metadata(f.metadata.clone()))
        })
        .collect::<Result<Vec<_>, ProtoConvertError>>()?;
    Ok(arrow::datatypes::Schema::new(fields))
}

/// Converts a proto semantic-type discriminant back to
/// [`sago_core::semantic::SemanticType`]. prost generates a safe,
/// range-checked `TryFrom<i32>` for proto3 enums, so an out-of-range value
/// from a newer/buggy server is rejected rather than reinterpreted.
pub fn proto_to_semantic(
    v: i32,
) -> Result<sago_core::semantic::SemanticType, ProtoConvertError> {
    use sago_core::semantic::SemanticType as S;
    match v1::SemanticType::try_from(v) {
        Ok(v1::SemanticType::Unknown) | Ok(v1::SemanticType::Unspecified) => Ok(S::Unknown),
        Ok(v1::SemanticType::Email) => Ok(S::Email),
        Ok(v1::SemanticType::CreditCard) => Ok(S::CreditCard),
        Ok(v1::SemanticType::PhoneNumber) => Ok(S::PhoneNumber),
        Ok(v1::SemanticType::Uuid) => Ok(S::UUID),
        Ok(v1::SemanticType::IpAddress) => Ok(S::IPAddress),
        Ok(v1::SemanticType::Url) => Ok(S::Url),
        Err(_) => Err(ProtoConvertError::Invalid(format!(
            "unknown SemanticType discriminant {v}"
        ))),
    }
}

fn proto_to_semantic_drift(
    d: &v1::SemanticDrift,
) -> Result<sago_core::drift::SemanticDrift, ProtoConvertError> {
    Ok(sago_core::drift::SemanticDrift {
        field_name: d.field_name.clone(),
        source_type: proto_to_semantic(d.source_type)?,
        target_type: proto_to_semantic(d.target_type)?,
    })
}

fn proto_to_stats(s: &v1::ColumnStats) -> sago_core::drift::ColumnStats {
    sago_core::drift::ColumnStats {
        null_count: s.null_count as usize,
        row_count: s.row_count as usize,
        mean: s.mean,
        min: s.min,
        max: s.max,
        variance: s.variance,
    }
}

fn proto_to_schema_drift(d: &v1::SchemaDrift) -> sago_core::drift::SchemaDrift {
    sago_core::drift::SchemaDrift {
        added_fields: d.added_fields.clone(),
        removed_fields: d.removed_fields.clone(),
        changed_types: d
            .changed_types
            .iter()
            .map(|c| sago_core::drift::TypeChange {
                field_name: c.field_name.clone(),
                old_type: c.old_type.clone(),
                new_type: c.new_type.clone(),
            })
            .collect(),
        renamed_fields: d
            .renamed_fields
            .iter()
            .map(|r| sago_core::rename::FieldRename {
                from: r.from.clone(),
                to: r.to.clone(),
                confidence: r.confidence,
                // The proto FieldRename only carries the final confidence
                // score, not the per-signal breakdown (type_match,
                // semantic_match, name_similarity, stats_similarity) — that
                // breakdown is server-side diagnostic detail, not part of
                // the wire contract. Signals are conservatively marked
                // absent/false rather than fabricated.
                signals: sago_core::rename::RenameSignals {
                    type_match: false,
                    semantic_match: false,
                    name_similarity: 0.0,
                    stats_similarity: None,
                },
            })
            .collect(),
    }
}

fn proto_to_data_drift(d: &v1::DataDrift) -> sago_core::drift::DataDrift {
    sago_core::drift::DataDrift {
        column_drifts: d
            .column_drifts
            .iter()
            .map(|(name, c)| {
                (
                    name.clone(),
                    sago_core::drift::ColumnDrift {
                        source_stats: c
                            .source_stats
                            .as_ref()
                            .map(proto_to_stats)
                            .unwrap_or(sago_core::drift::ColumnStats {
                                null_count: 0,
                                row_count: 0,
                                mean: None,
                                min: None,
                                max: None,
                                variance: None,
                            }),
                        target_stats: c
                            .target_stats
                            .as_ref()
                            .map(proto_to_stats)
                            .unwrap_or(sago_core::drift::ColumnStats {
                                null_count: 0,
                                row_count: 0,
                                mean: None,
                                min: None,
                                max: None,
                                variance: None,
                            }),
                        mean_drift: c.mean_drift,
                        null_count_drift: c.null_count_drift,
                        ks_statistic: c.ks_statistic,
                        ks_p_value: c.ks_p_value,
                        psi_statistic: c.psi_statistic,
                        categorical_drift: c.categorical_drift,
                    },
                )
            })
            .collect(),
    }
}

/// Converts a proto [`v1::DiffReport`] back to [`sago_core::diff::DiffReport`].
pub fn proto_to_diff_report(
    r: &v1::DiffReport,
) -> Result<sago_core::diff::DiffReport, ProtoConvertError> {
    let schema_drift = r
        .schema_drift
        .as_ref()
        .map(proto_to_schema_drift)
        .ok_or(ProtoConvertError::MissingField("schema_drift"))?;
    let semantic_drifts = r
        .semantic_drifts
        .iter()
        .map(proto_to_semantic_drift)
        .collect::<Result<Vec<_>, _>>()?;
    let data_drift = r
        .data_drift
        .as_ref()
        .map(proto_to_data_drift)
        .ok_or(ProtoConvertError::MissingField("data_drift"))?;
    Ok(sago_core::diff::DiffReport {
        source_identifier: r.source_identifier.clone(),
        target_identifier: r.target_identifier.clone(),
        schema_drift,
        semantic_drifts,
        data_drift,
    })
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
        let p = schema_to_proto(&schema).unwrap();
        assert_eq!(p.fields.len(), 2);
        assert_eq!(p.fields[0].name, "id");
        assert_eq!(p.fields[0].data_type, "Int64");
        assert!(!p.fields[0].nullable);
        assert_eq!(p.fields[1].data_type, "Utf8");
        assert!(p.fields[1].nullable);
    }

    #[test]
    fn test_schema_to_proto_carries_field_metadata() {
        // Regression: schema_to_proto only mapped name/data_type/nullable,
        // silently dropping any Arrow field metadata (e.g. a column comment
        // or extension-type marker a custom DataProvider attached).
        let field = Field::new("amount", DataType::Int64, false).with_metadata(
            std::collections::HashMap::from([("unit".to_string(), "USD".to_string())]),
        );
        let schema = Schema::new(vec![field]);
        let p = schema_to_proto(&schema).unwrap();
        assert_eq!(p.fields[0].metadata.get("unit"), Some(&"USD".to_string()));
    }

    #[test]
    fn test_schema_to_proto_rejects_unsupported_type() {
        // Regression: a Decimal128/List/Struct/etc. column used to silently
        // fall back to Arrow's Debug string (via serialize_data_type's
        // catch-all), which parse_data_type can never read back — the
        // schema was lost over the wire with the RPC still reporting
        // success. It must now error instead.
        let schema = Schema::new(vec![Field::new(
            "items",
            DataType::List(std::sync::Arc::new(Field::new(
                "item",
                DataType::Int32,
                true,
            ))),
            true,
        )]);
        let err = schema_to_proto(&schema).unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
    }

    #[test]
    fn test_schema_to_proto_accepts_decimal128_now_supported() {
        // Decimal128 is now in schema_codec's whitelist (added alongside the
        // Postgres numeric-precision mapping fix), so it must round-trip
        // through the gRPC schema conversion too.
        let schema = Schema::new(vec![Field::new("price", DataType::Decimal128(10, 2), false)]);
        let p = schema_to_proto(&schema).unwrap();
        assert_eq!(p.fields[0].data_type, "Decimal128(10, 2)");
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
            variance: None,
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

    // ── leaf_and_proof / leaf_index bounds ────────────────────────────────────

    #[test]
    fn test_leaf_and_proof_rejects_out_of_range_index() {
        let tree = MerkleTree::from_records([b"a".as_ref(), b"b", b"c"]);
        let err = leaf_and_proof(&tree, 99).unwrap_err();
        assert_eq!(err.code(), tonic::Code::OutOfRange);
    }

    #[test]
    fn test_leaf_and_proof_rejects_index_beyond_usize_range_on_conceptual_32_bit() {
        // Regression: `leaf_index as usize` used to silently truncate a
        // huge u64 (e.g. 2^32) down to an in-range usize on a 32-bit
        // target instead of erroring. usize::try_from cannot itself
        // reproduce the 32-bit truncation on this (64-bit) test host, but it
        // does prove the conversion is checked rather than an infallible
        // `as` cast: a value that doesn't fit any usize must always error.
        // On a real 32-bit build, u64::MAX also fails try_from::<usize>,
        // exercising the exact code path this regression test guards.
        let tree = MerkleTree::from_records([b"a".as_ref(), b"b"]);
        let err = leaf_and_proof(&tree, u64::MAX).unwrap_err();
        assert_eq!(err.code(), tonic::Code::OutOfRange);
    }

    #[test]
    fn test_leaf_and_proof_succeeds_for_valid_index() {
        let tree = MerkleTree::from_records([b"a".as_ref(), b"b", b"c"]);
        let (leaf, _proof) = leaf_and_proof(&tree, 1).unwrap();
        assert_eq!(leaf, sago_core::merkle::hash_leaf(b"b"));
    }

    // ── proto -> core conversions ─────────────────────────────────────────────

    #[test]
    fn test_proto_to_schema_round_trips() {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("email", DataType::Utf8, true),
        ]);
        let p = schema_to_proto(&schema).unwrap();
        let back = proto_to_schema(&p).unwrap();
        assert_eq!(back, schema);
    }

    #[test]
    fn test_proto_to_schema_preserves_metadata() {
        let field = Field::new("amount", DataType::Int64, false).with_metadata(
            std::collections::HashMap::from([("unit".to_string(), "USD".to_string())]),
        );
        let schema = Schema::new(vec![field.clone()]);
        let p = schema_to_proto(&schema).unwrap();
        let back = proto_to_schema(&p).unwrap();
        assert_eq!(back.field(0).metadata(), field.metadata());
    }

    #[test]
    fn test_proto_to_semantic_round_trips_every_variant() {
        for s in [
            SemanticType::Unknown,
            SemanticType::Email,
            SemanticType::CreditCard,
            SemanticType::PhoneNumber,
            SemanticType::UUID,
            SemanticType::IPAddress,
            SemanticType::Url,
        ] {
            let v = semantic_to_proto(&s);
            assert_eq!(proto_to_semantic(v).unwrap(), s);
        }
    }

    #[test]
    fn test_proto_to_semantic_rejects_out_of_range_discriminant() {
        let err = proto_to_semantic(9999).unwrap_err();
        assert!(matches!(err, ProtoConvertError::Invalid(_)));
    }

    #[test]
    fn test_proto_to_diff_report_round_trips() {
        let report = sago_core::diff::DiffReport {
            source_identifier: "src".into(),
            target_identifier: "tgt".into(),
            schema_drift: SchemaDrift {
                added_fields: vec!["email".into()],
                removed_fields: vec![],
                changed_types: vec![],
                renamed_fields: vec![],
            },
            semantic_drifts: vec![SemanticDrift {
                field_name: "contact".into(),
                source_type: SemanticType::Email,
                target_type: SemanticType::Unknown,
            }],
            data_drift: sago_core::drift::DataDrift {
                column_drifts: Default::default(),
            },
        };
        let p = diff_report_to_proto(&report);
        let back = proto_to_diff_report(&p).unwrap();
        assert_eq!(back.source_identifier, "src");
        assert_eq!(back.schema_drift.added_fields, vec!["email".to_string()]);
        assert_eq!(back.semantic_drifts[0].field_name, "contact");
        assert_eq!(back.semantic_drifts[0].source_type, SemanticType::Email);
    }

    #[test]
    fn test_proto_to_diff_report_missing_schema_drift_errors() {
        let p = v1::DiffReport {
            source_identifier: "a".into(),
            target_identifier: "b".into(),
            schema_drift: None,
            semantic_drifts: vec![],
            data_drift: Some(v1::DataDrift {
                column_drifts: Default::default(),
            }),
        };
        let err = proto_to_diff_report(&p).unwrap_err();
        assert_eq!(err, ProtoConvertError::MissingField("schema_drift"));
    }
}
