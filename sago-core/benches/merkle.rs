//! Benchmarks for `sago-core::merkle`: tree construction (hashing all leaves
//! and building every level) and per-proof generation/verification cost,
//! across dataset sizes from N=10^3 to N=10^6. See `docs/BENCHMARKS.md`.
//!
//! Run with `cargo bench -p sago-core --bench merkle`.

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use sago_core::merkle::{MerkleTree, verify_proof};

const SIZES: [usize; 4] = [1_000, 10_000, 100_000, 1_000_000];

/// Synthetic records standing in for serialized partition rows.
fn records(n: usize) -> Vec<Vec<u8>> {
    (0..n)
        .map(|i| format!("record-{i:08}").into_bytes())
        .collect()
}

fn bench_construction(c: &mut Criterion) {
    let mut group = c.benchmark_group("merkle_tree_construction");
    // Building a tree of 10^6 leaves is slow enough that criterion's default
    // 100-sample target would take minutes; 10 samples is still a valid
    // (if noisier) estimate and keeps a full run tractable.
    group.sample_size(10);
    for &n in &SIZES {
        let recs = records(n);
        group.throughput(Throughput::Elements(n as u64));
        group.bench_with_input(BenchmarkId::from_parameter(n), &recs, |b, recs| {
            b.iter(|| MerkleTree::from_records(recs.iter()));
        });
    }
    group.finish();
}

fn bench_proof_generation(c: &mut Criterion) {
    let mut group = c.benchmark_group("merkle_proof_generation");
    for &n in &SIZES {
        let tree = MerkleTree::from_records(records(n).iter());
        let mid = n / 2;
        group.bench_with_input(BenchmarkId::from_parameter(n), &tree, |b, tree| {
            b.iter(|| tree.proof(mid));
        });
    }
    group.finish();
}

fn bench_proof_verification(c: &mut Criterion) {
    let mut group = c.benchmark_group("merkle_proof_verification");
    for &n in &SIZES {
        let tree = MerkleTree::from_records(records(n).iter());
        let mid = n / 2;
        let leaf = tree.leaf(mid).unwrap();
        let root = tree.root();
        let proof = tree.proof(mid).unwrap();
        group.bench_with_input(
            BenchmarkId::from_parameter(n),
            &(root, leaf, proof),
            |b, (root, leaf, proof)| {
                b.iter(|| verify_proof(root, leaf, proof));
            },
        );
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_construction,
    bench_proof_generation,
    bench_proof_verification
);
criterion_main!(benches);
