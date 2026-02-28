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

## Benchmarking Strategy for Sago Phase 3

1. **PSI & Merkle Efficiency:** Measure the overhead of Merkle Tree generation and path verification across different dataset sizes (N=10^3 to 10^6).
2. **Distribution Drift:** Implement the **Kolmogorov-Smirnov (KS) test** and **Population Stability Index (PSI)** metrics identified in NannyML and CheXstray.
3. **Cross-Modal Validation:** Compare synchronization performance between PostgreSQL and S3 Parquet using the "Security-by-Design" framework principles.
