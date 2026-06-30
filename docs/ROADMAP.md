# Roadmap

## Phase 1: Foundation (Current)
- [x] Initial project structure
- [x] Core schema provider interface
- [x] Basic CLI framework
- [x] PostgreSQL and S3 connection boilerplate
- [x] Arrow integration

## Phase 2: Core Functionality
- [x] Implement `SchemaProvider` for PostgreSQL
- [x] Implement `SchemaProvider` for Parquet on S3
- [x] Basic Schema Drift Detection (column additions/removals)
- [x] Data Value Drift Detection (simple statistical metrics)

## Phase 3: Advanced Features
- [x] **Cross-Modal Diffing**: Efficiently compare tables in Postgres vs. Parquet files in S3.
- [x] **Semantic Schema Analysis**: Infer semantic meaning of columns (e.g., "email", "credit_card") to detect drifts beyond type changes.
- [x] **Statistical Drift Detection**: Use advanced algorithms (KS-test, etc.) to detect distribution shifts.

## Phase 4: Near-Term Work
- [x] **CLI Implementation**: `init`, `apply`, `plan`, `diff` wired against `sago-core` (Phase 4A).
- [x] **PSI Metric**: Population Stability Index added alongside KS test in `detect_data_drift` (Phase 4B).
- [x] **Additional S3 Formats**: CSV and NDJSON support added to `S3SchemaProvider` with extension-based auto-detection and optional `format` override in config (Phase 4C).
- [x] **TUI**: `sago explore` subcommand with ratatui list/detail UI, keyboard navigation, and `TestBackend`-based unit tests (Phase 4D).
- [x] **sago-sdk**: `SagoClient` with `snapshot` method and `diff` free function; re-exports all core types (Phase 4E).
- [x] **sago-proto**: `.proto` message/service definitions for `sago.v1` (schema, drift, semantic types, diff report, `SagoService` gRPC). Compiled with the **pure-Rust `protox`** toolchain via `build.rs` + `tonic-prost-build`, so no system `protoc` is required.

## Phase 5: Future Directions
- [x] **Semantic Smart Renaming**: Removed/added column pairs are recognised as renames using data type, inferred semantic type, distribution statistics, and name similarity, then folded into `SchemaDrift::renamed_fields` (wired into both `diff` and `plan`).
- [x] **3-Way Merge**: `sago-core::merge::three_way_merge` reconciles two divergent schema edits against a common ancestor, auto-resolving non-conflicting changes and reporting add/add, modify/modify, and remove/modify conflicts.
- [x] **Merkle Trees**: `sago-core::merkle::MerkleTree` provides SHA-256 commitments with domain-separated leaf/node hashing, a root digest, and inclusion proofs (`proof`/`verify_proof`) for verifiable data synchronization.
- [x] **WASM Integration**: `sago-core` gained an `io` feature (default-on) that gates the data-source providers; with `default-features = false` the pure-analysis modules build for `wasm32-unknown-unknown`. The new `sago-wasm` crate exposes semantic inference, three-way schema merge, and Merkle roots to JavaScript via `wasm-bindgen`.
- [x] **Decentralized Data Architectures** *(foundation laid)*: This is a direction rather than a single feature. The enabling primitives now exist — Merkle commitments for trust-minimised sync, three-way merge for federated schema governance, the `SagoService` gRPC interface, and per-target `domain`/`owner` metadata in config. The target architecture and concrete follow-up steps are documented in [DECENTRALIZED.md](./DECENTRALIZED.md).
