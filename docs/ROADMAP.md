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
- [ ] **CLI Implementation**: Wire `init`, `plan`, and `apply` commands to actual sago-core logic (currently stubs).
- [ ] **TUI**: Implement `ratatui`-based interactive terminal UI for exploration (workspace dep declared; re-enable in `sago-cli/Cargo.toml` when scaffolding lands).
- [ ] **PSI Metric**: Implement Population Stability Index alongside the existing KS test for richer distribution drift detection.
- [ ] **sago-sdk**: Implement real SDK bindings to sago-core (currently placeholder).
- [ ] **sago-proto**: Define `.proto` files and gRPC service definitions (currently placeholder, no proto files).
- [ ] **Additional S3 Formats**: Support CSV and JSON in addition to Parquet.

## Phase 5: Future Directions
- [ ] **Semantic Smart Renaming**: Intelligently handle column renames without breaking pipelines.
- [ ] **3-Way Merge**: Handle conflicting schema changes gracefully.
- [ ] **Merkle Trees**: Verifiable data synchronization using Merkle tree commitments (PSI is tracked under Phase 4).
- [ ] **WASM Integration**: Compile core logic to WebAssembly for browser-based tools or edge execution.
- [ ] **Decentralized Data Architectures**: Support for distributed data mesh concepts.
