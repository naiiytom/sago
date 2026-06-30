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
  `GetSchema` / `Diff` remotely, the natural transport for a federation of
  independently deployed providers.
- **Per-target ownership metadata** (`config::TargetConfig::domain` / `owner`) —
  targets can now declare which **domain** owns them, the minimal config-level
  step toward treating a project as a federation of independently owned targets.

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

## Concrete next steps (not yet implemented)

1. A `sago federate` view that groups `plan`/`diff` output by `domain`.
2. A `SagoService` client wrapper in `sago-sdk` that fetches a remote node's
   Merkle root and reconciles divergence using inclusion proofs.
3. Per-domain ownership/RBAC enforcement on who may `apply` a target.
4. A gossip/registry mechanism for domains to discover one another.

These are deliberately left as follow-ups: each is a feature in its own right,
and shipping them speculatively before there is a consumer would be premature.
The architecture above and the primitives already in `sago-core` make them
incremental rather than foundational.
