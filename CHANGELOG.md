# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Fixed

- **Drift gating now works out of the box**: `sago apply` persists per-column
  samples by default (previously only when a target explicitly opted in), so
  `sago plan`'s PSI drift gate is no longer a silent no-op. Opt a target out with
  `[targets.<name>.sample] enabled = false`.
- **Statistical robustness**: `NaN` values are excluded from PSI/stats instead of
  collapsing every value into the first bin; unsigned and 8-bit integer columns
  now participate in KS/PSI drift; Postgres `date`/`timestamp`/`numeric`/`bytea`
  columns are now extracted with real values instead of coming back all-null.
- **Config validation**: `checks.drift_threshold` (and the new
  `rename_confidence_threshold`) are validated to `[0, 1]` at parse time.
- Tightened email/URL semantic-type regexes and raised the name-only rename
  floor so near-miss sibling columns (`address_line1`/`address_line2`) are not
  mistaken for renames.

### Removed

- Dead `SchemaDrift.semantic_drifts` field (semantic drift lives on `DiffReport`);
  the corresponding `sago.v1.SchemaDrift` proto field 4 is now `reserved`.

### Added

- **PSI / KS in reports**: `sago plan` and `sago diff` now print the `psi=`,
  `ks=`, and `p=` values per drifted column — the metrics that gate the exit
  code — with deterministic (sorted) column ordering.
- **Configurable rename detection**: `checks.rename_confidence_threshold` in
  `Sago.toml` and a `--rename-threshold` flag on `plan`/`diff`.
- `sago plan` now reports newly-added live columns that carry a concrete semantic
  type (e.g. a fresh `email`/`ssn` column) as semantic drift.

- **Data-mesh target metadata**: `TargetConfig` gained optional `domain` and
  `owner` fields, the first concrete step toward decentralized / federated data
  architectures. Existing configs are unaffected (both default to `None`). The
  target architecture and follow-ups are documented in `docs/DECENTRALIZED.md`.
- **WebAssembly support**: `sago-core` now has an `io` feature (enabled by
  default) gating the PostgreSQL/S3 providers, the async runtime, and
  filesystem state. With `default-features = false` the pure-analysis modules
  (`semantic`, `drift`, `rename`, `merge`, `merkle`) compile to
  `wasm32-unknown-unknown`. New `sago-wasm` crate exposes `infer_semantic`,
  `merge_schemas`, and `merkle_root` to JavaScript via `wasm-bindgen` for
  browser / edge execution.
- **sago-proto gRPC interface (definitions + codegen)**: `.proto` definitions
  for the `sago.v1` package (schema, drift, semantic types, `DiffReport`, and a
  `SagoService` with `GetSchema`/`Diff` RPCs). Compiled at build time with the
  **pure-Rust `protox`** compiler driving `tonic-prost-build`, so the crate
  builds with no system `protoc` toolchain — unblocking the item previously
  deferred for that reason. Generates both client and server stubs; a concrete
  `SagoService` **server implementation** is not yet provided (tracked in
  `docs/DECENTRALIZED.md`).
- **Three-way schema merge** (`sago-core::merge`): `three_way_merge(base, ours,
  theirs)` reconciles two independently evolved schemas against their common
  ancestor. Non-conflicting changes (one-sided edits, identical edits, shared
  removals) auto-resolve into a best-effort merged `Schema`; genuine
  disagreements are reported as `MergeConflict`s classified `AddAdd`,
  `ModifyModify`, or `RemoveModify`. Re-exported from `sago-sdk`.
- **Merkle tree commitments** (`sago-core::merkle`): `MerkleTree` builds a
  SHA-256 binary Merkle tree with domain-separated leaf/node hashing (second-
  preimage resistant), exposes the `root`/`root_hex` commitment, and produces
  `InclusionProof`s verifiable with `verify_proof` — the primitive for
  verifiable data synchronization. Adds a direct `sha2` dependency. Re-exported
  from `sago-sdk`.
- **Semantic smart renaming** (`sago-core::rename`): removed/added column pairs
  are recognised as renames rather than a drop + add by comparing data type
  (a hard gate), inferred semantic type, distribution statistics, and name
  similarity (token Jaccard blended with normalised Levenshtein via `max`).
  Confident, greedy 1:1 matches are folded out of `added_fields`/`removed_fields`
  into the new `SchemaDrift::renamed_fields` (`#[serde(default)]`, backward
  compatible). Wired into both `sago diff` (profiles built from record batches)
  and `sago plan` (profiles built from the persisted snapshot), surfaced in the
  terminal report, and re-exported from `sago-sdk`.

## [0.1.0] - 2026-06-30

First public release. Sago brings Infrastructure-as-Code principles to DataOps:
declarative, high-performance data reliability checks over heterogeneous sources.

### Added

- **sago-core** — the reliability engine:
  - `SchemaProvider` / `DataProvider` traits over Apache Arrow schemas and record batches.
  - PostgreSQL provider (`sqlx`) with SQL-injection-safe identifier quoting and a
    Postgres→Arrow type mapping.
  - S3 / object-store provider supporting **Parquet, CSV, and NDJSON/JSON**, selected
    by file extension or an explicit `format` override.
  - Schema drift (added/removed fields, type changes) and semantic drift detection.
  - Semantic type inference for email, credit card, phone, UUID, IP address, and URL.
  - Statistical data drift: **Kolmogorov–Smirnov (KS) test** and
    **Population Stability Index (PSI)** per numeric column.
  - Project state persistence (`.sago/state.json`) with a versioned schema.
- **sago-cli** — `sago` binary with `init`, `apply`, `plan`, `diff`, and an
  interactive `explore` TUI (built on `ratatui`). Plan/diff artifacts written to
  `.sago/plans/<timestamp>.json`.
- **sago-sdk** — `SagoClient` (`snapshot`) and a one-shot `diff` free function, with
  the core types re-exported for downstream consumers.
- **sago-proto** — placeholder for future gRPC/Protobuf plugin interfaces
  (not yet implemented; `publish = false`).
- Workspace package metadata (license, repository, keywords, MSRV) for publishing.
- Committed `Cargo.lock` for reproducible builds.

### Changed

- Upgraded dependencies to current releases: `arrow`/`parquet` 59, `object_store` 0.14,
  `ratatui` 0.30, `toml` 1.0.
- Replaced the `lazy_static` dependency with the standard library's `std::sync::LazyLock`.
- Documentation (`README`, technical specs, benchmarks) and the sample `Sago.toml`
  updated to match the implemented feature set.

### Removed

- Unused dependencies: `aws-sdk-s3` (S3 access goes through `object_store`),
  `colored`, and the unimplemented `prost`/`tonic`/`tonic-build` stack in `sago-proto`.

### Notes

- Minimum supported Rust version (MSRV): **1.85** (Rust 2024 edition).

[0.1.0]: https://github.com/naiiytom/sago/releases/tag/v0.1.0
