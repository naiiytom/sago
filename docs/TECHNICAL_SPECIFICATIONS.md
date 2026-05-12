# Technical Specifications

## Core Technologies
- **Rust**: Language of choice for performance and safety.
- **Tokio**: Asynchronous runtime for efficient I/O handling.
- **Apache Arrow**: Columnar memory format for high-speed data processing.
- **Merkle Trees**: Efficiently verify data integrity and consistency (planned — Phase 4).
- **PSI (Private Set Intersection) Calculation**: Compare datasets without revealing sensitive information (planned — Phase 4).
- **sqlx**: Asynchronous SQL toolkit for database interactions.
- **aws-sdk-s3**: AWS SDK for interacting with S3 storage.
- **clap**: Command-line argument parser.
- **ratatui**: TUI (Text User Interface) library for rich terminal interfaces (dependency added — implementation planned Phase 4).

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
  - `ratatui`-based TUI for interactive exploration (planned — Phase 4D).
- **sago-sdk**:
  - Bindings to `sago-core` functionality (placeholder — Phase 4).
- **sago-proto**:
  - gRPC/Protobuf definitions for potential future microservices or plugin architecture (placeholder, no `.proto` files yet — Phase 4).

## Known Limitations
- S3 provider currently supports Parquet format only (no CSV, JSON, etc.).
- KS test p-value uses a 100-term asymptotic expansion; accuracy degrades for very small samples (n < 30).
- Semantic type inference samples the first 100 values per column only.
