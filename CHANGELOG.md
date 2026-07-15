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

### Changed

- **PSI now uses quantile (decile) binning** instead of 10 equal-width bins. Bin
  edges are the reference sample's deciles, so drift in the dense region of a
  skewed column is detected instead of being diluted across mostly-empty bins.
- **MSRV raised to Rust 1.89** (from 1.85). The bump is driven entirely by the
  dependency tree — `ratatui` 0.30 and `tonic` 0.14 require 1.88, and transitive
  deps (e.g. `crc-fast`) require 1.89 — not by the crate's own source. CI pins
  the MSRV job to 1.89.

### Removed

- Dead `SchemaDrift.semantic_drifts` field (semantic drift lives on `DiffReport`);
  the corresponding `sago.v1.SchemaDrift` proto field 4 is now `reserved`.

### Added

- **Merkle benchmarks**: `sago-core/benches/merkle.rs` (Criterion, `cargo
  bench -p sago-core --bench merkle`) measures tree construction and
  per-proof generation/verification cost across N=10^3 to 10^6 records,
  closing the "planned for Phase 5" item in `docs/BENCHMARKS.md`. Results:
  construction is O(N) at ~240 ns/record; proof generation and verification
  stay in single-digit microseconds even at 10^6 leaves, confirming the O(log
  N) shape that makes `sago_sdk::grpc::reconcile` viable on large partitions.
- **`sago domains`**: a new subcommand plus `sago-core::registry` that lists
  every data-mesh domain a project knows about — the union of `[domains]`
  entries and every target's `domain =` reference — with its registered
  `SagoService` endpoint (`[domains.<name>].endpoint`, new on `DomainConfig`),
  operator count, and target count. `sago domains --resolve <name>` prints
  just the endpoint for scripting, erroring distinctly for "unknown domain"
  vs. "known domain, no endpoint configured". This is Sago's domain-discovery
  mechanism: a config-declared registry (the `[domains]` table itself,
  distributed however the team already manages `Sago.toml`) rather than a
  live gossip/announce protocol. Fourth and final concrete deliverable from
  the Decentralized Data Architectures follow-ups in `docs/DECENTRALIZED.md`.
- **Per-domain RBAC on `apply`**: a new `[domains.<name>]` table in
  `Sago.toml` declares an `operators` allowlist (`sago-core::rbac`) for a
  data-mesh `domain`. A domain absent from the table is unrestricted, so
  existing configs are unaffected; a domain with an entry restricts `sago
  apply` to its listed operators (an empty list is a deliberate lockout).
  `sago apply` resolves the actor from the new `--as <name>` flag or the
  `SAGO_ACTOR` environment variable and checks it before any
  connection/provider I/O for targets in a governed domain. Third concrete
  deliverable from the Decentralized Data Architectures follow-ups in
  `docs/DECENTRALIZED.md`.
- **Merkle-based reconciliation over gRPC**: `SagoService` gained
  `GetMerkleRoot`/`GetInclusionProof` RPCs, served by `ProviderService` via a
  new `MerkleTree::from_batches` (`sago-core::merkle`) that commits to a
  dataset's rows in order, canonically serialized the same way `sago explore`
  displays them. `sago_sdk::grpc::reconcile` is the client-side counterpart:
  given a Merkle tree built from a caller's own copy of a dataset, it fetches
  the remote root and, when it differs, walks per-row inclusion proofs to
  report exactly which local rows diverge from the remote node's — one round
  trip when in sync, otherwise one `GetInclusionProof` call per row in the
  shorter side. This is the second concrete deliverable from the Decentralized
  Data Architectures follow-ups in `docs/DECENTRALIZED.md`.
- **`sago federate`**: a new subcommand that runs the same baseline-vs-live
  drift computation as `sago plan`, but groups the report by each target's
  `domain` (data-mesh metadata already on `TargetConfig`) instead of a flat
  list — alphabetically, with untagged targets grouped last under
  "(unassigned)", and each domain's targets annotated with their `owner`.
  Supports `--domain <name>` to scope to a single domain, gates its exit code
  on `checks.drift_threshold` exactly like `plan`, and writes the same JSON
  artifact shape. The first concrete deliverable from the Decentralized Data
  Architectures follow-ups in `docs/DECENTRALIZED.md`.
- **gRPC `SagoService` server**: `sago-sdk::grpc::ProviderService` (behind the
  new `sago-sdk` `grpc` feature) wraps any `DataProvider` and serves the
  `GetSchema`/`Diff` RPCs over tonic, with proto↔core type conversions, an
  end-to-end client/server test, and a `grpc_server` example. This turns the
  previously stub-only `sago-proto` service into a runnable node.
- **SDK ergonomics**: `sago-sdk` now re-exports the `DataProvider`/`SchemaProvider`
  traits (implement a custom source without depending on `sago-core` directly),
  the `diff`/`diff_datasets*` functions, a crate-level guide, and docs on
  `SagoClient::snapshot`. `sago-proto` re-exports its `v1::*` message types at
  the crate root.
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
