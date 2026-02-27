# Roadmap

## Phase 1: Foundation (Current)
- [x] Initial project structure
- [x] Core schema provider interface
- [x] Basic CLI framework
- [x] PostgreSQL and S3 connection boilerplate
- [x] Arrow integration

## Phase 2: Core Functionality
- [ ] Implement `SchemaProvider` for PostgreSQL
- [ ] Implement `SchemaProvider` for Parquet on S3
- [ ] Basic Schema Drift Detection (column additions/removals)
- [ ] Data Value Drift Detection (simple statistical metrics)

## Phase 3: Advanced Features
- [ ] **Cross-Modal Diffing**: Efficiently compare tables in Postgres vs. Parquet files in S3.
- [ ] **Semantic Schema Analysis**: Infer semantic meaning of columns (e.g., "email", "credit_card") to detect drifts beyond type changes.
- [ ] **Statistical Drift Detection**: Use advanced algorithms (KS-test, etc.) to detect distribution shifts.

## Phase 4: Future Directions
- [ ] **Semantic Smart Renaming**: Intelligently handle column renames without breaking pipelines.
- [ ] **3-Way Merge**: Handle conflicting schema changes gracefully.
- [ ] **WASM Integration**: Compile core logic to WebAssembly for browser-based tools or edge execution.
- [ ] **Decentralized Data Architectures**: Support for distributed data mesh concepts.
