# Decentralized Data Architectures

> Phase 5 design note. Unlike the other Phase 5 items this is a *direction*, not
> a single feature — it composes the primitives Sago already ships. This note
> records the target architecture, what exists today, and the concrete next
> steps so the work is scoped rather than open-ended.

## Goal

Let a single Sago project reason about data that is **owned and operated by
different teams** (the data-mesh model) and verify consistency across nodes that
do not fully trust one another, without shipping whole datasets around.

## What already exists (building blocks)

Sago's Phase 5 work produced the primitives a decentralized setup needs:

- **Merkle commitments** (`sago-core::merkle`) — each node can publish a single
  root hash committing to its data, and prove that a specific record is included
  with an `InclusionProof`. Two nodes compare roots to detect divergence in O(1)
  and exchange proofs instead of raw data. This is the trust-minimised sync
  primitive.
- **Three-way schema merge** (`sago-core::merge`) — when two domains evolve a
  shared contract independently, `three_way_merge` reconciles the changes and
  flags genuine conflicts, so federated schema governance has a deterministic
  resolution step.
- **gRPC interface** (`sago-proto`) — `SagoService` lets a node expose
  `GetSchema` / `Diff` / `GetMerkleRoot` / `GetInclusionProof` remotely, the
  natural transport for a federation of independently deployed providers.
- **Per-target ownership metadata** (`config::TargetConfig::domain` / `owner`) —
  targets can now declare which **domain** owns them, the minimal config-level
  step toward treating a project as a federation of independently owned targets.
- **Per-domain governance metadata** (`config::DomainConfig::operators` /
  `endpoint`) — a `[domains.<name>]` entry declares who may `apply` a domain's
  targets and where to reach its `SagoService` node.

## Target architecture

```
        ┌──────────────┐        roots + proofs        ┌──────────────┐
        │  domain: sales│  ◄──────────────────────────►│ domain: finance│
        │  SagoService  │        (Merkle sync)         │  SagoService   │
        └──────┬───────┘                               └───────┬───────┘
               │  schema contracts (3-way merge on change)     │
               └───────────────────┬───────────────────────────┘
                                   ▼
                        federated Sago project
                   (targets tagged by domain / owner)
```

Each domain runs its own provider and owns its targets. A federated project
references them by `domain`, compares schema contracts with the merge engine,
and verifies data consistency via Merkle roots exchanged over `SagoService` —
no central data warehouse and no bulk data movement required.

## Concrete next steps

1. ~~A `sago federate` view that groups `plan`/`diff` output by `domain`.~~
   **Done**: `sago federate [--domain <name>]` runs the same baseline-vs-live
   drift computation as `sago plan`, but groups the report by each target's
   `domain` (alphabetically, with untagged targets grouped last under
   "(unassigned)") and prints the `owner` alongside each domain's targets. It
   gates the exit code and writes a JSON artifact identically to `plan`.
2. ~~A `SagoService` client wrapper in `sago-sdk` that fetches a remote node's
   Merkle root and reconciles divergence using inclusion proofs.~~ **Done**:
   `SagoService` gained `GetMerkleRoot`/`GetInclusionProof` RPCs (served by
   `ProviderService` over the caller's `DataProvider`, via the new
   `MerkleTree::from_batches` row-level commitment in `sago-core::merkle`).
   `sago_sdk::grpc::reconcile` is the client-side counterpart: given a Merkle
   tree built from your own copy of a dataset, it fetches the remote root, and
   if it differs, walks per-row inclusion proofs to report exactly which rows
   diverge — one round trip when in sync, otherwise one `GetInclusionProof`
   call per row in the shorter side.
3. ~~Per-domain ownership/RBAC enforcement on who may `apply` a target.~~
   **Done**: `[domains.<name>]` in `Sago.toml` declares an `operators`
   allowlist for a domain (`sago-core::rbac`). A domain with no entry is
   unrestricted (existing configs are unaffected); one with an entry allows
   only the listed identities, and an entry with an empty list is a
   deliberate lockout. `sago apply` resolves the actor from `--as <name>` or
   the `SAGO_ACTOR` environment variable and checks it before touching any
   target in a governed domain — before any connection/provider I/O, so a
   denied target never reaches the network.
4. ~~A gossip/registry mechanism for domains to discover one another.~~
   **Done**, as a config-declared registry rather than a live announce
   protocol: `[domains.<name>].endpoint` in `Sago.toml` records the
   `SagoService` address a domain's team operates. The registry *is* the
   config file — distributed however the team already manages it (git,
   config management, etc.) — not a network protocol between running nodes,
   which kept this incremental rather than introducing peer state, TTLs, or
   failure handling for a project of Sago's current scope. `sago-core::registry`
   turns the table into a queryable list (`list_domains`) and a single lookup
   (`resolve_endpoint`, which distinguishes "unknown domain" from "known
   domain, no endpoint configured"). `sago domains` lists every domain a
   project knows about — the union of `[domains]` entries and every target's
   `domain =` reference — with its endpoint, operator count, and target
   count; `sago domains --resolve <name>` prints just the endpoint for
   scripting (e.g. piping into `sago_sdk::grpc::reconcile`'s connect step).

All four follow-ups above are now shipped. This composes every primitive
listed in "What already exists" into the consumer-facing surface: `sago
federate` groups by domain, `sago domains` discovers them and their
endpoints, `sago_sdk::grpc::reconcile` verifies consistency against them, and
`sago-core::rbac` gates who may `apply` their targets. A live
gossip/heartbeat protocol, cross-domain schema-merge automation, and
richer RBAC (roles, inheritance) remain open — genuinely separate features
to take up only once there is a concrete consumer, not something to build
speculatively ahead of one.
