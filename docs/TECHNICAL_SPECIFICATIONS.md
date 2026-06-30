# Technical Specifications

## Core Technologies
- **Rust**: Language of choice for performance and safety.
- **Tokio**: Asynchronous runtime for efficient I/O handling.
- **Apache Arrow**: Columnar memory format for high-speed data processing.
- **PSI (Population Stability Index)**: Distribution-shift metric computed alongside the KS test in `detect_data_drift` (implemented — Phase 4B).
- **Merkle Trees**: Verifiable data synchronization via Merkle commitments (planned — Phase 5).
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
- **sago-cli**:
  - `clap`-based CLI for `init`, `apply`, `plan`, `diff` — wired against `sago-core` as of Phase 4A.
  - State persisted at `.sago/state.json`; plan artifacts at `.sago/plans/<timestamp>.json`.
  - `ratatui`-based TUI (`sago explore`) for interactive exploration (implemented — Phase 4D).
- **sago-sdk**:
  - `SagoClient` with a `snapshot` method and a one-shot `diff` free function; re-exports the core types (implemented — Phase 4E).
- **sago-proto**:
  - gRPC/Protobuf definitions for a future microservice or plugin architecture. Intentional placeholder — no `.proto` files yet, blocked on a `protoc` toolchain (Phase 4 / Phase 5). Marked `publish = false`.

## Known Limitations
- S3 provider supports Parquet, CSV, and NDJSON/JSON, selected by file extension or an explicit `format` override; other formats are not yet supported.
- KS test p-value uses a 100-term asymptotic expansion; accuracy degrades for very small samples (n < 30).
- PSI is computed with 10 equal-width bins; very skewed distributions may benefit from quantile binning (future work).
- Semantic type inference samples the first 100 values per column only.
- `plan` compares persisted column statistics against a freshly captured snapshot, so it reports schema, semantic, mean, and null-count drift; full-distribution metrics (KS, PSI) are produced only by `diff`, which reads complete record batches from both sides.
