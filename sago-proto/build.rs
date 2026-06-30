//! Build script: compile the Sago `.proto` definitions into Rust bindings.
//!
//! We deliberately avoid a system `protoc`. `protox` (pure Rust) parses the
//! `.proto` files into a `FileDescriptorSet`, which `tonic-prost-build` then
//! turns into prost message types and a tonic gRPC service — all from source,
//! so the crate builds anywhere a Rust toolchain is present.

use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto_root = PathBuf::from("proto");
    let proto_file = proto_root.join("sago/v1/sago.proto");

    // Rebuild only when the schema changes.
    println!("cargo:rerun-if-changed={}", proto_file.display());
    println!("cargo:rerun-if-changed=build.rs");

    // protox parses the .proto and produces a FileDescriptorSet in-process.
    let file_descriptors = protox::compile([&proto_file], [&proto_root])?;

    // tonic-prost-build consumes the descriptor set (no protoc needed) and emits
    // the Rust bindings into OUT_DIR.
    tonic_prost_build::configure()
        .build_client(true)
        .build_server(true)
        .compile_fds(file_descriptors)?;

    Ok(())
}
