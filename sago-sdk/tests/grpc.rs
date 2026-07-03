//! End-to-end gRPC test: a real tonic server (backed by a mock `DataProvider`)
//! over a TCP socket, exercised by the generated client. Only built with `grpc`.
#![cfg(feature = "grpc")]

use std::sync::Arc;
use std::time::Duration;

use arrow::array::{Int32Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use sago_core::{DataProvider, Result, SagoError, SchemaProvider};
use sago_proto::v1;
use sago_sdk::grpc::{ProviderService, SagoServiceClient, SagoServiceServer};

/// A provider whose data depends on the identifier, so a Diff has something to
/// compare: "left" is an email column, "right" renames it and adds a row.
struct MockProvider;

fn batch(col: &str, emails: &[&str]) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new(col, DataType::Utf8, true),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(
                (0..emails.len() as i32).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(emails.to_vec())),
        ],
    )
    .unwrap()
}

#[async_trait]
impl SchemaProvider for MockProvider {
    async fn get_schema(&self, identifier: &str) -> Result<Schema> {
        match identifier {
            "left" => Ok(batch("email", &["a@x.com"]).schema().as_ref().clone()),
            "right" => Ok(batch("contact_email", &["a@x.com"])
                .schema()
                .as_ref()
                .clone()),
            other => Err(SagoError::Schema(format!("no such table '{other}'"))),
        }
    }
}

#[async_trait]
impl DataProvider for MockProvider {
    async fn get_data(&self, identifier: &str) -> Result<Vec<RecordBatch>> {
        match identifier {
            "left" => Ok(vec![batch("email", &["a@x.com", "b@x.com", "c@x.com"])]),
            "right" => Ok(vec![batch(
                "contact_email",
                &["a@x.com", "b@x.com", "c@x.com"],
            )]),
            other => Err(SagoError::Schema(format!("no such table '{other}'"))),
        }
    }
}

async fn spawn_server() -> String {
    // Bind an ephemeral port and hand the listener to tonic.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);

    let svc = ProviderService::new(Arc::new(MockProvider));
    tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(SagoServiceServer::new(svc))
            .serve_with_incoming(incoming)
            .await
            .unwrap();
    });

    // Give the server a moment to start accepting.
    tokio::time::sleep(Duration::from_millis(100)).await;
    format!("http://{addr}")
}

#[tokio::test]
async fn test_get_schema_over_grpc() {
    let endpoint = spawn_server().await;
    let mut client = SagoServiceClient::connect(endpoint).await.unwrap();

    let resp = client
        .get_schema(v1::GetSchemaRequest {
            identifier: "left".into(),
        })
        .await
        .unwrap()
        .into_inner();

    let schema = resp.schema.expect("schema present");
    let names: Vec<&str> = schema.fields.iter().map(|f| f.name.as_str()).collect();
    assert!(names.contains(&"id"));
    assert!(names.contains(&"email"));
}

#[tokio::test]
async fn test_diff_over_grpc_detects_rename() {
    let endpoint = spawn_server().await;
    let mut client = SagoServiceClient::connect(endpoint).await.unwrap();

    let resp = client
        .diff(v1::DiffRequest {
            source_identifier: "left".into(),
            target_identifier: "right".into(),
        })
        .await
        .unwrap()
        .into_inner();

    let report = resp.report.expect("report present");
    let drift = report.schema_drift.expect("schema drift present");
    // email -> contact_email, same Utf8 + email semantics, should fold to a rename.
    assert_eq!(drift.renamed_fields.len(), 1, "expected a detected rename");
    assert_eq!(drift.renamed_fields[0].from, "email");
    assert_eq!(drift.renamed_fields[0].to, "contact_email");
}

#[tokio::test]
async fn test_get_schema_unknown_identifier_is_not_found() {
    let endpoint = spawn_server().await;
    let mut client = SagoServiceClient::connect(endpoint).await.unwrap();

    let status = client
        .get_schema(v1::GetSchemaRequest {
            identifier: "does_not_exist".into(),
        })
        .await
        .unwrap_err();

    assert_eq!(status.code(), tonic::Code::NotFound);
}
