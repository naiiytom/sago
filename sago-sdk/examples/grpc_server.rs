//! Minimal `SagoService` gRPC server over an S3/object-store provider.
//!
//! Run with: `cargo run -p sago-sdk --features grpc --example grpc_server`
//!
//! Then call it with any gRPC client (e.g. `grpcurl`) against `GetSchema` / `Diff`.

use sago_core::config::ConnectionConfig;
use sago_core::connection::build_provider;
use sago_sdk::grpc::{ProviderService, SagoServiceServer};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "127.0.0.1:50051".parse()?;

    // Any DataProvider works; here an S3/object-store one.
    let provider = build_provider(&ConnectionConfig::S3 {
        bucket: "my-data".into(),
        region: "us-east-1".into(),
        format: None,
    })
    .await?;

    let service = ProviderService::new(provider);

    println!("SagoService listening on {addr}");
    tonic::transport::Server::builder()
        .add_service(SagoServiceServer::new(service))
        .serve(addr)
        .await?;

    Ok(())
}
