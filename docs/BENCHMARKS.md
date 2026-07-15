# Benchmark References for Sago

This document lists external benchmarks and research papers that inform the drift detection methodologies and performance testing for Sago.

## Key Research Papers

### 1. Authenticated Private Set Intersection: A Merkle Tree-Based Approach for Enhancing Data Integrity (arXiv:2506.04647)
- **Methodology:** Integrates Merkle Trees as a commitment layer to PSI (Private Set Intersection).
- **Benchmarking Insight:**
  - **Complexity:** $O(n \lambda + n \log n)$, where $n$ is set size.
  - **Performance Drivers:** Merkle Path Verification becomes the dominant cost for sets $> 2^{10}$.
  - **Communication Overhead:** Path size increases linearly with $\log n$.
- **Relevance:** Sago uses Merkle Trees and PSI for verifiable data synchronization.

### 2. Engineering Risk-Aware, Security-by-Design Frameworks for Assurance of Large-Scale Autonomous AI Models (arXiv:2505.06409)
- **Author:** Krti Tallam (SentinelAI)
- **Benchmarking Focus:**
  - **WILDS Benchmark:** Evaluating model performance under in-the-wild distribution shifts.
  - **Robustness Benchmarks:** Using ImageNet-C metrics for common corruptions and perturbations.
- **Relevance:** Provides a framework for "Proactive Resilience" which aligns with Sago's goal of ensuring data reliability.

### 3. CheXstray: Real-time Multi-Modal Data Concordance for Drift Detection in Medical Imaging AI (2022)
- **Authors:** Arjun Soin, et al. (Microsoft Research & Stanford University)
- **Focus:** Detecting drift in real-time medical imaging pipelines by combining DICOM metadata, image appearance (via VAE latents), and model outputs.
- **Key Method:** **Multi-Modal Concordance (MMC) Score** which utilizes the **Kolmogorov–Smirnov (KS) test** for continuous feature drift detection.

### 4. Adapting Multi-modal Large Language Model to Concept Drift (2024 / ICLR 2025)
- **Authors:** Xiaoyu Yang, Jie Lu, and En Yu (UTS)
- **Key Contribution:** The **OpenMMlo** benchmark dataset for handling long-tailed distributions and OOD sudden shifts.

## Benchmarking Strategy for Sago Phase 4

1. **Distribution Drift:** **Kolmogorov–Smirnov (KS) test** ✅ and **Population Stability Index (PSI)** ✅ — both implemented in `sago-core/src/drift.rs` and computed per numeric column in `detect_data_drift`.
2. **Merkle Efficiency:** ✅ Measured — `sago-core/benches/merkle.rs` (Criterion, `cargo bench -p sago-core --bench merkle`) covers tree construction, single-proof generation, and proof verification across N=10^3 to 10^6 records. See [Merkle Benchmark Results](#merkle-benchmark-results) below.
3. **Cross-Modal Validation:** Compare synchronization performance between PostgreSQL and S3 Parquet using the "Security-by-Design" framework principles.

## Merkle Benchmark Results

Run via `cargo bench -p sago-core --bench merkle` (Criterion, release profile). Numbers are from a single local run and will vary by machine; re-run locally for your own hardware. Construction hashes `N` synthetic ~14-byte records into leaves and builds every tree level; proof generation/verification operate on the middle leaf of an already-built tree of size `N`.

| N (records) | Construction | Proof generation | Proof verification |
|---|---|---|---|
| 1,000 | ~219 µs | ~2.3 µs | ~2.0 µs |
| 10,000 | ~2.1 ms | ~3.0 µs | ~2.9 µs |
| 100,000 | ~22.7 ms | ~3.6 µs | ~3.0 µs |
| 1,000,000 | ~241 ms | ~3.5 µs* | ~3.2 µs |

\* the proof-generation step count is ⌈log₂ N⌉, so 10^6 costs at most ~4 more
hash-comparison steps than 10^5 — well within this benchmark's run-to-run
noise at `--quick` sample sizes; use `cargo bench` (full sampling) for a
tighter reading if you need to distinguish these two rows precisely.

Takeaways, consistent with the O(n) construction / O(log n) proof complexity the tree's shape implies:

- **Construction scales linearly with N** — SHA-256 hashing every leaf and internal node dominates, at roughly 240 ns/record end to end (leaf hash + its share of internal-node hashes).
- **Proof generation and verification scale with log N**, not N — both stay in single-digit microseconds even at a million leaves (a proof for 10^6 leaves is only ~20 hash steps), confirming a proof stays cheap regardless of dataset size. This is what makes `sago_sdk::grpc::reconcile` viable for large partitions: the expensive step is building the tree once, not exchanging or checking individual proofs.
