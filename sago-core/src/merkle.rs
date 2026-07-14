//! Merkle tree commitments for verifiable data synchronization.
//!
//! A Merkle tree reduces an ordered list of records (e.g. the rows of a
//! partition, or per-column snapshot digests) to a single 32-byte root hash.
//! Two parties can then confirm they hold identical data by exchanging only
//! that root, and — when they differ — an *inclusion proof* lets a verifier
//! confirm that one specific leaf belongs to a committed root without
//! transferring the whole dataset. This is the primitive that backs efficient,
//! trust-minimised data sync.
//!
//! ## Construction
//!
//! Leaves are SHA-256 hashes of the input records; internal nodes hash the
//! concatenation of their two children. Leaf and node hashing use distinct
//! domain-separation prefixes (`0x00` / `0x01`) so a leaf digest can never be
//! reinterpreted as an internal node — the standard defence against
//! second-preimage attacks on Merkle trees. When a level has an odd number of
//! nodes the last node is promoted unchanged to the next level (a duplication-
//! free convention that keeps proofs unambiguous).

use std::fmt::Write as _;

use arrow::record_batch::RecordBatch;
use arrow::util::display::ArrayFormatter;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::Result;

/// Separates canonically-formatted column values within a row before hashing.
/// The Unicode "unit separator" control character is vanishingly unlikely to
/// appear in real column data, so two differently-shaped rows do not collide
/// onto the same leaf bytes just because a value happens to contain a comma or
/// pipe.
const FIELD_SEP: char = '\u{1f}';

/// A 32-byte SHA-256 digest.
pub type Hash = [u8; 32];

const LEAF_PREFIX: u8 = 0x00;
const NODE_PREFIX: u8 = 0x01;

/// Hash of an empty tree (no leaves): SHA-256 of the empty input.
fn empty_root() -> Hash {
    Sha256::new().finalize().into()
}

/// Domain-separated hash of a single leaf's bytes.
pub fn hash_leaf(data: &[u8]) -> Hash {
    let mut hasher = Sha256::new();
    hasher.update([LEAF_PREFIX]);
    hasher.update(data);
    hasher.finalize().into()
}

/// Domain-separated hash of an internal node from its two children.
fn hash_nodes(left: &Hash, right: &Hash) -> Hash {
    let mut hasher = Sha256::new();
    hasher.update([NODE_PREFIX]);
    hasher.update(left);
    hasher.update(right);
    hasher.finalize().into()
}

/// Lowercase hex encoding of a hash, for display / serialization.
pub fn to_hex(hash: &Hash) -> String {
    let mut s = String::with_capacity(64);
    for byte in hash {
        s.push(char::from_digit((byte >> 4) as u32, 16).unwrap());
        s.push(char::from_digit((byte & 0x0f) as u32, 16).unwrap());
    }
    s
}

/// One step of an inclusion proof: a sibling hash and which side it sits on.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProofStep {
    pub sibling: String, // hex-encoded sibling hash
    /// `true` when the sibling is on the left (so it is concatenated *before*
    /// the running hash), `false` when on the right.
    pub sibling_is_left: bool,
}

/// An inclusion proof: the sibling hashes from a leaf up to the root.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InclusionProof {
    pub leaf_index: usize,
    pub steps: Vec<ProofStep>,
}

/// A binary Merkle tree over an ordered list of leaves.
///
/// All levels are retained so inclusion proofs can be produced without
/// recomputation. `levels[0]` is the leaf level; the final level is the single
/// root (except for the empty tree, which has an explicit [`empty_root`]).
#[derive(Debug, Clone)]
pub struct MerkleTree {
    levels: Vec<Vec<Hash>>,
}

impl MerkleTree {
    /// Build a tree from already-hashed leaves.
    pub fn from_leaves(leaves: Vec<Hash>) -> Self {
        if leaves.is_empty() {
            return MerkleTree {
                levels: vec![vec![empty_root()]],
            };
        }

        let mut levels = vec![leaves];
        while levels.last().unwrap().len() > 1 {
            let current = levels.last().unwrap();
            let mut next = Vec::with_capacity(current.len().div_ceil(2));
            let mut i = 0;
            while i < current.len() {
                if i + 1 < current.len() {
                    next.push(hash_nodes(&current[i], &current[i + 1]));
                    i += 2;
                } else {
                    // Odd node out: promote it unchanged.
                    next.push(current[i]);
                    i += 1;
                }
            }
            levels.push(next);
        }

        MerkleTree { levels }
    }

    /// Build a tree by hashing each record's raw bytes as a leaf.
    pub fn from_records<I, T>(records: I) -> Self
    where
        I: IntoIterator<Item = T>,
        T: AsRef<[u8]>,
    {
        let leaves = records.into_iter().map(|r| hash_leaf(r.as_ref())).collect();
        Self::from_leaves(leaves)
    }

    /// Build a tree over the rows of `batches`, in order, one leaf per row.
    ///
    /// Each row is canonically serialized as its column values (in schema
    /// order) rendered via Arrow's display formatting and joined with a
    /// control-character separator — the same value a person would see in
    /// `sago explore`, not a type-specific byte encoding. This means the
    /// commitment is over *displayed* values: two datasets that print
    /// identically hash identically, even if the underlying column types
    /// differ (e.g. `Int32` vs `Int64`), matching how the rest of Sago treats
    /// value-level equality. Row order matters — reordering a dataset changes
    /// the root — since a data-mesh consumer needs to know if rows were
    /// reshuffled, not only whether the row *set* changed.
    pub fn from_batches(batches: &[RecordBatch]) -> Result<Self> {
        let opts = arrow::util::display::FormatOptions::default();
        let mut leaves = Vec::new();
        for batch in batches {
            let formatters: Vec<ArrayFormatter<'_>> = batch
                .columns()
                .iter()
                .map(|col| ArrayFormatter::try_new(col.as_ref(), &opts))
                .collect::<std::result::Result<_, _>>()?;
            for row in 0..batch.num_rows() {
                let mut buf = String::new();
                for (i, f) in formatters.iter().enumerate() {
                    if i > 0 {
                        buf.push(FIELD_SEP);
                    }
                    write!(buf, "{}", f.value(row)).expect("String write is infallible");
                }
                leaves.push(hash_leaf(buf.as_bytes()));
            }
        }
        Ok(Self::from_leaves(leaves))
    }

    /// The number of leaves committed by this tree.
    pub fn leaf_count(&self) -> usize {
        // The empty tree stores a single synthetic root and no real leaves.
        if self.levels.len() == 1 && self.levels[0].len() == 1 && self.is_empty_tree() {
            0
        } else {
            self.levels[0].len()
        }
    }

    fn is_empty_tree(&self) -> bool {
        self.levels.len() == 1 && self.levels[0] == vec![empty_root()]
    }

    /// The Merkle root committing to all leaves.
    pub fn root(&self) -> Hash {
        *self.levels.last().unwrap().first().unwrap()
    }

    /// Hex-encoded [`root`](Self::root).
    pub fn root_hex(&self) -> String {
        to_hex(&self.root())
    }

    /// The leaf hash at `index`, or `None` if out of range. Mirrors the same
    /// bound as [`proof`](Self::proof) so a caller can pair a leaf with its
    /// inclusion proof.
    pub fn leaf(&self, index: usize) -> Option<Hash> {
        if index >= self.leaf_count() {
            return None;
        }
        self.levels[0].get(index).copied()
    }

    /// Produce an inclusion proof for the leaf at `index`, or `None` if out of
    /// range.
    pub fn proof(&self, index: usize) -> Option<InclusionProof> {
        if index >= self.leaf_count() {
            return None;
        }

        let mut steps = Vec::new();
        let mut idx = index;
        // Walk every level except the root.
        for level in &self.levels[..self.levels.len() - 1] {
            // A promoted odd node has no sibling at this level; it simply rises.
            if idx == level.len() - 1 && !level.len().is_multiple_of(2) {
                idx /= 2;
                continue;
            }
            let (sibling, sibling_is_left) = if idx.is_multiple_of(2) {
                (level[idx + 1], false)
            } else {
                (level[idx - 1], true)
            };
            steps.push(ProofStep {
                sibling: to_hex(&sibling),
                sibling_is_left,
            });
            idx /= 2;
        }

        Some(InclusionProof {
            leaf_index: index,
            steps,
        })
    }
}

/// Verify that `leaf` is included in the tree committing to `root`, given
/// `proof`. The proof's sibling hashes are folded into the leaf hash and the
/// final value is compared against `root`.
#[must_use = "the verification result must be checked; ignoring it defeats the proof"]
pub fn verify_proof(root: &Hash, leaf: &Hash, proof: &InclusionProof) -> bool {
    let mut acc = *leaf;
    for step in &proof.steps {
        let sibling = match from_hex(&step.sibling) {
            Some(h) => h,
            None => return false,
        };
        acc = if step.sibling_is_left {
            hash_nodes(&sibling, &acc)
        } else {
            hash_nodes(&acc, &sibling)
        };
    }
    &acc == root
}

/// Parse a 64-char lowercase/uppercase hex string into a [`Hash`].
pub fn from_hex(s: &str) -> Option<Hash> {
    if s.len() != 64 {
        return None;
    }
    let mut out = [0u8; 32];
    let bytes = s.as_bytes();
    for (i, slot) in out.iter_mut().enumerate() {
        let hi = (bytes[2 * i] as char).to_digit(16)?;
        let lo = (bytes[2 * i + 1] as char).to_digit(16)?;
        *slot = (hi * 16 + lo) as u8;
    }
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── domain separation ──────────────────────────────────────────────────────

    #[test]
    fn test_leaf_and_node_hashing_are_domain_separated() {
        // A leaf hash of 64 zero bytes must differ from an internal node hash of
        // two zero children, even though the post-prefix input is identical.
        let zero = [0u8; 32];
        let leaf = hash_leaf(&[0u8; 64]);
        let node = hash_nodes(&zero, &zero);
        assert_ne!(leaf, node);
    }

    // ── hex round-trip ───────────────────────────────────────────────────────────

    #[test]
    fn test_hex_round_trip() {
        let h = hash_leaf(b"hello");
        let hex = to_hex(&h);
        assert_eq!(hex.len(), 64);
        assert_eq!(from_hex(&hex), Some(h));
    }

    #[test]
    fn test_from_hex_rejects_bad_input() {
        assert!(from_hex("xyz").is_none());
        assert!(from_hex(&"z".repeat(64)).is_none());
    }

    // ── construction & determinism ───────────────────────────────────────────────

    #[test]
    fn test_empty_tree() {
        let tree = MerkleTree::from_records(Vec::<Vec<u8>>::new());
        assert_eq!(tree.leaf_count(), 0);
        assert_eq!(tree.root(), empty_root());
        assert!(tree.proof(0).is_none());
    }

    #[test]
    fn test_single_leaf_root_is_that_leaf() {
        let tree = MerkleTree::from_records([b"only"]);
        assert_eq!(tree.leaf_count(), 1);
        assert_eq!(tree.root(), hash_leaf(b"only"));
    }

    #[test]
    fn test_root_is_deterministic() {
        let a = MerkleTree::from_records([b"a".as_ref(), b"b", b"c"]);
        let b = MerkleTree::from_records([b"a".as_ref(), b"b", b"c"]);
        assert_eq!(a.root(), b.root());
    }

    #[test]
    fn test_order_sensitivity() {
        let a = MerkleTree::from_records([b"a".as_ref(), b"b"]);
        let b = MerkleTree::from_records([b"b".as_ref(), b"a"]);
        assert_ne!(a.root(), b.root());
    }

    #[test]
    fn test_changing_one_leaf_changes_root() {
        let a = MerkleTree::from_records([b"a".as_ref(), b"b", b"c", b"d"]);
        let b = MerkleTree::from_records([b"a".as_ref(), b"b", b"c", b"D"]);
        assert_ne!(a.root(), b.root());
    }

    // ── inclusion proofs ─────────────────────────────────────────────────────────

    #[test]
    fn test_proof_verifies_for_every_leaf_power_of_two() {
        let records: Vec<&[u8]> = vec![b"a", b"b", b"c", b"d"];
        let tree = MerkleTree::from_records(records.iter().copied());
        let root = tree.root();
        for (i, rec) in records.iter().enumerate() {
            let proof = tree.proof(i).unwrap();
            assert!(
                verify_proof(&root, &hash_leaf(rec), &proof),
                "leaf {i} failed to verify"
            );
        }
    }

    #[test]
    fn test_proof_verifies_for_every_leaf_odd_count() {
        // 5 leaves exercises the promoted-odd-node path at multiple levels.
        let records: Vec<&[u8]> = vec![b"a", b"b", b"c", b"d", b"e"];
        let tree = MerkleTree::from_records(records.iter().copied());
        let root = tree.root();
        for (i, rec) in records.iter().enumerate() {
            let proof = tree.proof(i).unwrap();
            assert!(
                verify_proof(&root, &hash_leaf(rec), &proof),
                "leaf {i} failed to verify"
            );
        }
    }

    #[test]
    fn test_proof_rejects_wrong_leaf() {
        let records: Vec<&[u8]> = vec![b"a", b"b", b"c"];
        let tree = MerkleTree::from_records(records.iter().copied());
        let proof = tree.proof(0).unwrap();
        // Verifying the proof against a different leaf must fail.
        assert!(!verify_proof(&tree.root(), &hash_leaf(b"not-a"), &proof));
    }

    #[test]
    fn test_proof_rejects_wrong_root() {
        let tree = MerkleTree::from_records([b"a".as_ref(), b"b", b"c"]);
        let proof = tree.proof(1).unwrap();
        let wrong_root = hash_leaf(b"tampered");
        assert!(!verify_proof(&wrong_root, &hash_leaf(b"b"), &proof));
    }

    #[test]
    fn test_proof_out_of_range_is_none() {
        let tree = MerkleTree::from_records([b"a".as_ref(), b"b"]);
        assert!(tree.proof(2).is_none());
        assert!(tree.proof(99).is_none());
    }

    #[test]
    fn test_single_leaf_proof_is_empty_and_verifies() {
        let tree = MerkleTree::from_records([b"solo"]);
        let proof = tree.proof(0).unwrap();
        assert!(proof.steps.is_empty());
        assert!(verify_proof(&tree.root(), &hash_leaf(b"solo"), &proof));
    }

    #[test]
    fn test_proof_json_round_trip() {
        let tree = MerkleTree::from_records([b"a".as_ref(), b"b", b"c"]);
        let proof = tree.proof(2).unwrap();
        let json = serde_json::to_string(&proof).unwrap();
        let back: InclusionProof = serde_json::from_str(&json).unwrap();
        assert_eq!(proof, back);
        assert!(verify_proof(&tree.root(), &hash_leaf(b"c"), &back));
    }

    #[test]
    fn test_verify_rejects_proof_with_malformed_sibling() {
        let tree = MerkleTree::from_records([b"a".as_ref(), b"b"]);
        let mut proof = tree.proof(0).unwrap();
        proof.steps[0].sibling = "not-hex".into();
        assert!(!verify_proof(&tree.root(), &hash_leaf(b"a"), &proof));
    }

    // ── leaf() ───────────────────────────────────────────────────────────────

    #[test]
    fn test_leaf_returns_hash_at_index() {
        let tree = MerkleTree::from_records([b"a".as_ref(), b"b", b"c"]);
        assert_eq!(tree.leaf(0), Some(hash_leaf(b"a")));
        assert_eq!(tree.leaf(2), Some(hash_leaf(b"c")));
    }

    #[test]
    fn test_leaf_out_of_range_is_none() {
        let tree = MerkleTree::from_records([b"a".as_ref(), b"b"]);
        assert!(tree.leaf(2).is_none());
    }

    #[test]
    fn test_leaf_empty_tree_is_none() {
        let tree = MerkleTree::from_records(Vec::<Vec<u8>>::new());
        assert!(tree.leaf(0).is_none());
    }

    // ── from_batches() ───────────────────────────────────────────────────────

    fn batch(schema: std::sync::Arc<arrow::datatypes::Schema>, ids: Vec<i32>) -> RecordBatch {
        use arrow::array::Int32Array;
        RecordBatch::try_new(schema, vec![std::sync::Arc::new(Int32Array::from(ids))]).unwrap()
    }

    fn int32_schema() -> std::sync::Arc<arrow::datatypes::Schema> {
        use arrow::datatypes::{DataType, Field, Schema};
        std::sync::Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, true)]))
    }

    #[test]
    fn test_from_batches_leaf_count_matches_row_count() {
        let schema = int32_schema();
        let b = batch(schema, vec![1, 2, 3]);
        let tree = MerkleTree::from_batches(&[b]).unwrap();
        assert_eq!(tree.leaf_count(), 3);
    }

    #[test]
    fn test_from_batches_empty_is_empty_tree() {
        let tree = MerkleTree::from_batches(&[]).unwrap();
        assert_eq!(tree.leaf_count(), 0);
        assert_eq!(tree.root(), empty_root());
    }

    #[test]
    fn test_from_batches_deterministic() {
        let schema = int32_schema();
        let a = MerkleTree::from_batches(&[batch(schema.clone(), vec![1, 2, 3])]).unwrap();
        let b = MerkleTree::from_batches(&[batch(schema, vec![1, 2, 3])]).unwrap();
        assert_eq!(a.root(), b.root());
    }

    #[test]
    fn test_from_batches_row_order_sensitive() {
        let schema = int32_schema();
        let a = MerkleTree::from_batches(&[batch(schema.clone(), vec![1, 2, 3])]).unwrap();
        let b = MerkleTree::from_batches(&[batch(schema, vec![3, 2, 1])]).unwrap();
        assert_ne!(a.root(), b.root());
    }

    #[test]
    fn test_from_batches_multiple_batches_concatenate_in_order() {
        let schema = int32_schema();
        let single = MerkleTree::from_batches(&[batch(schema.clone(), vec![1, 2, 3, 4])]).unwrap();
        let split = MerkleTree::from_batches(&[
            batch(schema.clone(), vec![1, 2]),
            batch(schema, vec![3, 4]),
        ])
        .unwrap();
        assert_eq!(single.root(), split.root());
    }

    #[test]
    fn test_from_batches_differing_value_changes_root() {
        let schema = int32_schema();
        let a = MerkleTree::from_batches(&[batch(schema.clone(), vec![1, 2, 3])]).unwrap();
        let b = MerkleTree::from_batches(&[batch(schema, vec![1, 2, 99])]).unwrap();
        assert_ne!(a.root(), b.root());
    }

    #[test]
    fn test_from_batches_null_and_value_produce_different_leaves() {
        let schema = int32_schema();
        let b = batch(schema, vec![1]);
        let null_batch = {
            use arrow::array::Int32Array;
            let schema = int32_schema();
            RecordBatch::try_new(
                schema,
                vec![std::sync::Arc::new(Int32Array::from(vec![None::<i32>]))],
            )
            .unwrap()
        };
        let with_value = MerkleTree::from_batches(&[b]).unwrap();
        let with_null = MerkleTree::from_batches(&[null_batch]).unwrap();
        assert_ne!(with_value.root(), with_null.root());
    }
}
