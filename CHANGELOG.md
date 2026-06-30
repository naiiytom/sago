# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

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
