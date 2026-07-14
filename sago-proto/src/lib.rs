//! # sago-proto
//!
//! gRPC / Protocol Buffer definitions for Sago's plugin / microservice
//! interface. The `.proto` source lives in `proto/sago/v1/sago.proto` and is
//! compiled at build time by [`build.rs`] using the **pure-Rust `protox`
//! compiler** — no system `protoc` toolchain is required.
//!
//! The generated module mirrors the core domain types in `sago-core`
//! (schemas, drift, semantic types, diff reports) and exposes a
//! [`v1::sago_service_client::SagoServiceClient`] /
//! [`v1::sago_service_server::SagoServiceServer`] pair so a remote provider can
//! serve schema and diff requests over gRPC.

/// Generated types for the `sago.v1` package.
pub mod v1 {
    tonic::include_proto!("sago.v1");
}

// Re-export the commonly used generated items at the crate root for
// convenience, so consumers write `sago_proto::Field` instead of
// `sago_proto::v1::Field`. Covers the message/enum types a gRPC server or client
// needs when converting to/from the core domain types.
pub use v1::{
    ColumnDrift, ColumnStats, DataDrift, DiffReport, DiffRequest, DiffResponse, Field, FieldRename,
    GetInclusionProofRequest, GetInclusionProofResponse, GetMerkleRootRequest,
    GetMerkleRootResponse, GetSchemaRequest, GetSchemaResponse, ProofStep, Schema, SchemaDrift,
    SemanticDrift, SemanticType, TypeChange, sago_service_client::SagoServiceClient,
    sago_service_server::SagoServiceServer,
};

#[cfg(test)]
mod tests {
    use super::v1;

    #[test]
    fn test_messages_construct_and_default() {
        // The generated prost types exist and derive Default/Clone.
        let field = v1::Field {
            name: "id".into(),
            data_type: "Int64".into(),
            nullable: false,
        };
        let schema = v1::Schema {
            fields: vec![field.clone()],
        };
        assert_eq!(schema.fields.len(), 1);
        assert_eq!(schema.fields[0].name, "id");

        let req = v1::DiffRequest {
            source_identifier: "a".into(),
            target_identifier: "b".into(),
        };
        assert_eq!(req.source_identifier, "a");
    }

    #[test]
    fn test_semantic_type_enum_values() {
        // proto3 enums generate an i32-backed Rust enum with the declared variants.
        assert_eq!(v1::SemanticType::Email as i32, 2);
        assert_eq!(v1::SemanticType::Unspecified as i32, 0);
    }

    #[test]
    fn test_prost_message_round_trip() {
        use prost::Message;

        let report = v1::DiffReport {
            source_identifier: "src".into(),
            target_identifier: "tgt".into(),
            schema_drift: Some(v1::SchemaDrift {
                added_fields: vec!["email".into()],
                removed_fields: vec![],
                changed_types: vec![],
                renamed_fields: vec![v1::FieldRename {
                    from: "phone".into(),
                    to: "phone_number".into(),
                    confidence: 0.91,
                }],
            }),
            semantic_drifts: vec![],
            data_drift: None,
        };

        let mut buf = Vec::new();
        report.encode(&mut buf).unwrap();
        let back = v1::DiffReport::decode(&buf[..]).unwrap();
        assert_eq!(report, back);
        let drift = back.schema_drift.unwrap();
        assert_eq!(drift.added_fields, vec!["email".to_string()]);
        assert_eq!(drift.renamed_fields[0].to, "phone_number");
    }
}
