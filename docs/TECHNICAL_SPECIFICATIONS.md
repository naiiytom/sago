# Technical Specifications

## Core Technologies
- **Rust**: Language of choice for performance and safety.
- **Tokio**: Asynchronous runtime for efficient I/O handling.
- **Apache Arrow**: Columnar memory format for high-speed data processing.
- **PSI (Population Stability Index)**: Distribution-shift metric computed alongside the KS test in `detect_data_drift` (implemented — Phase 4B).
- **Merkle Trees**: Verifiable data synchronization via SHA-256 Merkle commitments with inclusion proofs (implemented — Phase 5, `sago-core::merkle`).
- **sha2**: SHA-256 implementation backing the Merkle commitments.
- **wasm-bindgen**: JS/WebAssembly interop for the `sago-wasm` crate (Phase 5 WASM integration).
- **protox**: Pure-Rust Protobuf compiler used by `sago-proto`'s `build.rs` (no system `protoc` required).
- **sqlx**: Asynchronous SQL toolkit for database interactions.
- **object_store**: Backend-agnostic object access used for S3 (Parquet/CSV/JSON).
- **clap**: Command-line argument parser.
- **ratatui**: TUI (Text User Interface) library powering the `sago explore` command (implemented — Phase 4D).

## Architecture Overview
Sago is designed as a modular system with a core engine responsible for the heavy lifting. The CLI acts as the primary interface, while the SDK allows developers to integrate Sago's capabilities into their own applications.

### Components
- **sago-core**:
  - `SchemaProvider` trait: Abstract interface for fetching Arrow schemas.
  - `DataProvider` trait: Extends `SchemaProvider`; provides full record batch data for drift analysis.
  - Diff Engine (`diff` module): Orchestrates cross-modal comparison using both providers.
  - Drift Detector (`drift` module): Statistical analysis — schema drift, data drift, KS test.
  - Semantic Analyzer (`semantic` module): Infers column semantic types (email, credit card, UUID, etc.).
  - Rename Detector (`rename` module): Recognises that a removed/added column pair is actually a rename by comparing data type (gate), inferred semantic type, distribution statistics, and name similarity (token Jaccard blended with normalised Levenshtein). Confident, greedy 1:1 matches are folded out of `added_fields`/`removed_fields` into `SchemaDrift::renamed_fields`. Used by both `diff` (profiles built from record batches) and `plan` (profiles built from the persisted snapshot).
  - Schema Merge (`merge` module): `three_way_merge(base, ours, theirs)` reconciles two divergent schema edits against a common ancestor, auto-resolving one-sided/identical changes and surfacing `AddAdd` / `ModifyModify` / `RemoveModify` conflicts. Conflicting fields fall back to the `ours` definition so the merged schema stays constructible.
  - Merkle Commitments (`merkle` module): SHA-256 binary Merkle tree over ordered records with domain-separated leaf (`0x00`) / node (`0x01`) hashing, a root commitment, and inclusion proofs (`proof` / `verify_proof`) for verifiable, trust-minimised data synchronization.
- **sago-cli**:
  - `clap`-based CLI for `init`, `apply`, `plan`, `diff` — wired against `sago-core` as of Phase 4A.
  - State persisted at `.sago/state.json`; plan artifacts at `.sago/plans/<timestamp>.json`.
  - `ratatui`-based TUI (`sago explore`) for interactive exploration (implemented — Phase 4D).
- **sago-sdk**:
  - `SagoClient` with a `snapshot` method and a one-shot `diff` free function; re-exports the core types (implemented — Phase 4E).
- **sago-wasm**:
  - WebAssembly bindings (`wasm-bindgen`) over the pure-analysis core: `infer_semantic`, `merge_schemas`, and `merkle_root`. Depends on `sago-core` with `default-features = false` so it builds for `wasm32-unknown-unknown` (browser / edge). Marked `publish = false`.
- **sago-proto**:
  - gRPC/Protobuf definitions for a remote provider / plugin architecture (`sago.v1`: schema, drift, semantic types, `DiffReport`, and the `SagoService` service with `GetSchema`/`Diff` RPCs). The `.proto` source is compiled at build time by the **pure-Rust `protox`** compiler driving `tonic-prost-build` (`build.rs`), so the crate builds with no system `protoc`. Marked `publish = false`.

## Known Limitations
- S3 provider supports Parquet, CSV, and NDJSON/JSON, selected by file extension or an explicit `format` override; other formats are not yet supported.
- KS test p-value uses a 100-term asymptotic expansion; accuracy degrades for very small samples (n < 30).
- PSI is computed with 10 bins whose edges are the **deciles of the reference (baseline) sample** (quantile binning), so skewed columns keep resolution where the data is dense. Ties in the reference can collapse adjacent edges, leaving fewer effective bins.
- Semantic type inference samples the first 100 values per column only.
- `plan` compares persisted column statistics against a freshly captured snapshot, so it reports schema, semantic, mean, and null-count drift; full-distribution metrics (KS, PSI) are produced only by `diff`, which reads complete record batches from both sides.
- Rename detection requires an exact data-type match (the type gate) and a blended confidence ≥ 0.6; type-changing renames and very low-signal pairs (unrelated names, no semantics, no comparable stats) are left as separate add/remove entries. Matching is greedy 1:1, so an ambiguous many-to-many rename resolves to the highest-confidence non-conflicting pairing.
