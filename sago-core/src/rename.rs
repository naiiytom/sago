//! Semantic smart renaming.
//!
//! A column rename normally surfaces as a `removed_field` + `added_field` pair
//! in [`SchemaDrift`](crate::drift::SchemaDrift), which looks like data loss and
//! breaks downstream pipelines that try to "re-add" the dropped column. This
//! module recognises that the two columns are the *same* column under a new
//! name by comparing the signals that survive a rename — data type, inferred
//! semantic type, distribution statistics, and name similarity — and folds the
//! pair into a single [`FieldRename`].
//!
//! Detection works from two callers with different amounts of information:
//!   * `diff` has full record batches and builds rich profiles from them.
//!   * `plan` only has the persisted snapshot (schema + stats + semantic types).
//!
//! Both assemble the same [`ColumnProfile`] map via [`profile_columns`], so the
//! matching logic is shared and identical regardless of the data source.

use std::collections::HashMap;

use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use serde::{Deserialize, Serialize};

use crate::drift::{ColumnStats, SchemaDrift, calculate_column_stats};
use crate::schema_codec::serialize_data_type;
use crate::semantic::{SemanticType, infer_semantic_type};

/// The signals about a single column that survive a rename and can therefore be
/// used to re-identify it under a new name.
#[derive(Debug, Clone, PartialEq)]
pub struct ColumnProfile {
    /// Debug representation of the Arrow data type (matches the form persisted
    /// in [`SerializableField`](crate::state::SerializableField)).
    pub data_type: String,
    /// Inferred semantic type (email, UUID, …) or `Unknown`.
    pub semantic_type: SemanticType,
    /// Distribution statistics, when available (numeric columns / captured snapshots).
    pub stats: Option<ColumnStats>,
}

/// A detected column rename: the old name, the new name, the confidence in the
/// match, and the per-signal breakdown that produced it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FieldRename {
    pub from: String,
    pub to: String,
    pub confidence: f64,
    pub signals: RenameSignals,
}

/// The individual evidence that supported (or weakened) a rename match.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RenameSignals {
    /// Whether the two columns share the same Arrow data type.
    pub type_match: bool,
    /// Whether both columns carry the same, non-`Unknown` semantic type.
    pub semantic_match: bool,
    /// Name similarity in `[0, 1]` (token overlap blended with edit distance).
    pub name_similarity: f64,
    /// Distribution similarity in `[0, 1]`, when both sides expose numeric stats.
    pub stats_similarity: Option<f64>,
}

/// Tunables for rename detection.
#[derive(Debug, Clone, PartialEq)]
pub struct RenameOptions {
    /// Minimum blended confidence for a pair to be accepted as a rename.
    pub min_confidence: f64,
    /// Require an exact data-type match before a pair is even considered.
    pub require_type_match: bool,
    /// When *name similarity is the only available signal* (both columns have an
    /// `Unknown` semantic type and neither exposes numeric stats), require at
    /// least this name similarity. Without corroboration, the blended confidence
    /// degenerates to the raw name similarity, so two merely-similar names could
    /// otherwise clear `min_confidence` and be declared a rename on a coincidental
    /// spelling overlap. Held to a high bar: near-miss siblings like
    /// `address_line1` ↔ `address_line2` (a single-character edit ⇒ ~0.92 name
    /// similarity) are distinct columns, not a rename, and must be rejected; only
    /// essentially-identical names (normalised-equal ⇒ 1.0) clear this floor.
    pub min_name_only_similarity: f64,
}

/// Default minimum blended confidence for accepting a rename. Shared with the
/// config layer so the CLI default and the library default never diverge.
pub const DEFAULT_MIN_CONFIDENCE: f64 = 0.6;

impl RenameOptions {
    /// [`RenameOptions::default`] but with a caller-supplied confidence floor
    /// (e.g. from `checks.rename_confidence_threshold` or `--rename-threshold`).
    #[must_use]
    pub fn with_min_confidence(min_confidence: f64) -> Self {
        RenameOptions {
            min_confidence,
            ..RenameOptions::default()
        }
    }
}

impl Default for RenameOptions {
    fn default() -> Self {
        RenameOptions {
            min_confidence: DEFAULT_MIN_CONFIDENCE,
            require_type_match: true,
            min_name_only_similarity: 0.92,
        }
    }
}

// Relative weights of each available signal. Only the signals actually present
// for a given pair contribute; the weights are renormalised over them so the
// resulting confidence always lands in `[0, 1]`. Semantic agreement carries the
// most weight: the whole point of *semantic* renaming is to recognise a column
// whose name changed completely but whose meaning (and type) did not.
const W_NAME: f64 = 0.25;
const W_SEMANTIC: f64 = 0.45;
const W_STATS: f64 = 0.30;

/// Build a [`ColumnProfile`] for every field in `schema`, drawing the semantic
/// type and stats from the supplied maps when present.
///
/// Both `diff` (which assembles the maps from live record batches) and `plan`
/// (which reads them from a persisted snapshot) call this so the two paths
/// produce identical profiles.
pub fn profile_columns(
    schema: &Schema,
    semantic_types: &HashMap<String, SemanticType>,
    column_stats: &HashMap<String, ColumnStats>,
) -> HashMap<String, ColumnProfile> {
    let mut profiles = HashMap::new();
    for field in schema.fields() {
        let name = field.name();
        profiles.insert(
            name.clone(),
            ColumnProfile {
                data_type: serialize_data_type(field.data_type()),
                semantic_type: semantic_types
                    .get(name)
                    .cloned()
                    .unwrap_or(SemanticType::Unknown),
                stats: column_stats.get(name).cloned(),
            },
        );
    }
    profiles
}

/// Build a [`ColumnProfile`] for every column present in `batches`, inferring
/// the semantic type from the first batch and computing distribution stats
/// across all batches. This is the `diff` entry point, where full record
/// batches are available.
pub fn profile_columns_from_batches(batches: &[RecordBatch]) -> HashMap<String, ColumnProfile> {
    let mut profiles = HashMap::new();
    let Some(first) = batches.first() else {
        return profiles;
    };
    let schema = first.schema();
    for field in schema.fields() {
        let name = field.name();
        let semantic_type = first
            .column_by_name(name)
            .map(|col| infer_semantic_type(name, col.as_ref()))
            .unwrap_or(SemanticType::Unknown);
        profiles.insert(
            name.clone(),
            ColumnProfile {
                data_type: serialize_data_type(field.data_type()),
                semantic_type,
                stats: calculate_column_stats(batches, name),
            },
        );
    }
    profiles
}

/// Detect renames among the removed/added columns and rewrite `drift` in place:
/// every confident match is removed from `added_fields`/`removed_fields` and
/// recorded in `renamed_fields`.
///
/// `removed_profiles` and `added_profiles` are the *full* profile maps for each
/// side; only the names currently listed as removed/added are considered.
pub fn refine_renames(
    drift: &mut SchemaDrift,
    removed_profiles: &HashMap<String, ColumnProfile>,
    added_profiles: &HashMap<String, ColumnProfile>,
    opts: &RenameOptions,
) {
    let removed: HashMap<&String, &ColumnProfile> = drift
        .removed_fields
        .iter()
        .filter_map(|n| removed_profiles.get(n).map(|p| (n, p)))
        .collect();
    let added: HashMap<&String, &ColumnProfile> = drift
        .added_fields
        .iter()
        .filter_map(|n| added_profiles.get(n).map(|p| (n, p)))
        .collect();

    if removed.is_empty() || added.is_empty() {
        return;
    }

    let renames = match_renames(&removed, &added, opts);
    if renames.is_empty() {
        return;
    }

    let renamed_from: std::collections::HashSet<&str> =
        renames.iter().map(|r| r.from.as_str()).collect();
    let renamed_to: std::collections::HashSet<&str> =
        renames.iter().map(|r| r.to.as_str()).collect();

    drift
        .removed_fields
        .retain(|n| !renamed_from.contains(n.as_str()));
    drift
        .added_fields
        .retain(|n| !renamed_to.contains(n.as_str()));
    drift.renamed_fields.extend(renames);
}

/// Score every removed×added pair and greedily select a deterministic 1:1
/// set of renames whose confidence clears `opts.min_confidence`.
fn match_renames(
    removed: &HashMap<&String, &ColumnProfile>,
    added: &HashMap<&String, &ColumnProfile>,
    opts: &RenameOptions,
) -> Vec<FieldRename> {
    let mut candidates: Vec<FieldRename> = Vec::new();
    for (from, from_profile) in removed {
        for (to, to_profile) in added {
            if let Some(rename) = score_pair(from, from_profile, to, to_profile, opts) {
                candidates.push(rename);
            }
        }
    }

    // Deterministic order: highest confidence first, then by name so HashMap
    // iteration order never changes the result.
    candidates.sort_by(|a, b| {
        b.confidence
            .total_cmp(&a.confidence)
            .then_with(|| a.from.cmp(&b.from))
            .then_with(|| a.to.cmp(&b.to))
    });

    let mut used_from: std::collections::HashSet<String> = std::collections::HashSet::new();
    let mut used_to: std::collections::HashSet<String> = std::collections::HashSet::new();
    let mut accepted = Vec::new();
    for cand in candidates {
        if used_from.contains(&cand.from) || used_to.contains(&cand.to) {
            continue;
        }
        used_from.insert(cand.from.clone());
        used_to.insert(cand.to.clone());
        accepted.push(cand);
    }
    accepted
}

/// Compute the rename confidence for a single (from, to) pair, or `None` if the
/// pair fails the type gate or falls below the confidence threshold.
fn score_pair(
    from: &str,
    from_profile: &ColumnProfile,
    to: &str,
    to_profile: &ColumnProfile,
    opts: &RenameOptions,
) -> Option<FieldRename> {
    let type_match = from_profile.data_type == to_profile.data_type;
    if opts.require_type_match && !type_match {
        return None;
    }

    let name_similarity = name_similarity(from, to);

    // Semantics only count when *both* sides resolved to a concrete type.
    let semantic_known = from_profile.semantic_type != SemanticType::Unknown
        && to_profile.semantic_type != SemanticType::Unknown;
    let semantic_match = from_profile.semantic_type == to_profile.semantic_type && semantic_known;

    let stats_similarity = match (&from_profile.stats, &to_profile.stats) {
        (Some(a), Some(b)) => stats_similarity(a, b),
        _ => None,
    };

    // Weighted average over the signals that are actually available for this
    // pair, so a missing signal neither helps nor hurts.
    let mut weighted = W_NAME * name_similarity;
    let mut total_weight = W_NAME;
    if semantic_known {
        weighted += W_SEMANTIC * if semantic_match { 1.0 } else { 0.0 };
        total_weight += W_SEMANTIC;
    }
    if let Some(s) = stats_similarity {
        weighted += W_STATS * s;
        total_weight += W_STATS;
    }
    let confidence = weighted / total_weight;

    if confidence < opts.min_confidence {
        return None;
    }

    // Name-only match (no semantic agreement, no stats to compare): demand a
    // stricter name-similarity floor so a coincidental spelling overlap can't
    // masquerade as a rename on the strength of the name signal alone.
    let name_only = !semantic_known && stats_similarity.is_none();
    if name_only && name_similarity < opts.min_name_only_similarity {
        return None;
    }

    Some(FieldRename {
        from: from.to_string(),
        to: to.to_string(),
        confidence,
        signals: RenameSignals {
            type_match,
            semantic_match,
            name_similarity,
            stats_similarity,
        },
    })
}

/// Similarity of two column names in `[0, 1]`, taking the stronger of two
/// independent measures: token-set overlap (catches reordering and shared
/// words like `customer_email` ↔ `email_address`) and normalised edit distance
/// over the separator-stripped form (catches small spelling changes like
/// `created` ↔ `createdat`). A pair is "similar" if it scores well on *either*,
/// so the two measures are combined with `max`, not averaged.
fn name_similarity(a: &str, b: &str) -> f64 {
    let na = normalize_name(a);
    let nb = normalize_name(b);
    if na == nb {
        return 1.0;
    }

    // Tokenise the *raw* names so word boundaries survive (normalisation strips
    // the separators that delimit tokens).
    let jaccard = token_jaccard(&tokenize(a), &tokenize(b));

    let edit = levenshtein(&na, &nb);
    let max_len = na.chars().count().max(nb.chars().count());
    let edit_sim = if max_len == 0 {
        1.0
    } else {
        1.0 - (edit as f64 / max_len as f64)
    };

    jaccard.max(edit_sim)
}

/// Lowercase and strip every non-alphanumeric character, collapsing separators.
fn normalize_name(s: &str) -> String {
    s.chars()
        .filter(|c| c.is_alphanumeric())
        .flat_map(|c| c.to_lowercase())
        .collect()
}

/// Split a name into lowercase tokens on any non-alphanumeric boundary.
fn tokenize(s: &str) -> Vec<String> {
    s.split(|c: char| !c.is_alphanumeric())
        .filter(|t| !t.is_empty())
        .map(|t| t.to_lowercase())
        .collect()
}

/// Jaccard index over two token sets: |A ∩ B| / |A ∪ B|.
fn token_jaccard(a: &[String], b: &[String]) -> f64 {
    use std::collections::HashSet;
    let sa: HashSet<&String> = a.iter().collect();
    let sb: HashSet<&String> = b.iter().collect();
    if sa.is_empty() && sb.is_empty() {
        return 1.0;
    }
    let inter = sa.intersection(&sb).count() as f64;
    let union = sa.union(&sb).count() as f64;
    if union == 0.0 { 0.0 } else { inter / union }
}

/// Classic Levenshtein edit distance over Unicode scalar values.
fn levenshtein(a: &str, b: &str) -> usize {
    let a: Vec<char> = a.chars().collect();
    let b: Vec<char> = b.chars().collect();
    if a.is_empty() {
        return b.len();
    }
    if b.is_empty() {
        return a.len();
    }

    let mut prev: Vec<usize> = (0..=b.len()).collect();
    let mut curr = vec![0usize; b.len() + 1];
    for (i, &ca) in a.iter().enumerate() {
        curr[0] = i + 1;
        for (j, &cb) in b.iter().enumerate() {
            let cost = if ca == cb { 0 } else { 1 };
            curr[j + 1] = (prev[j + 1] + 1).min(curr[j] + 1).min(prev[j] + cost);
        }
        std::mem::swap(&mut prev, &mut curr);
    }
    prev[b.len()]
}

/// Distribution similarity in `[0, 1]` between two numeric columns, blending
/// closeness of mean, min, max, and null ratio. Returns `None` if either side
/// lacks a numeric mean (i.e. the column is non-numeric).
fn stats_similarity(a: &ColumnStats, b: &ColumnStats) -> Option<f64> {
    let (a_mean, b_mean) = (a.mean?, b.mean?);

    let mut sum = relative_closeness(a_mean, b_mean);
    let mut count = 1.0;

    if let (Some(a_min), Some(b_min)) = (a.min, b.min) {
        sum += relative_closeness(a_min, b_min);
        count += 1.0;
    }
    if let (Some(a_max), Some(b_max)) = (a.max, b.max) {
        sum += relative_closeness(a_max, b_max);
        count += 1.0;
    }

    // Null ratio is comparable across columns of any size.
    let a_null_ratio = null_ratio(a);
    let b_null_ratio = null_ratio(b);
    sum += 1.0 - (a_null_ratio - b_null_ratio).abs();
    count += 1.0;

    Some(sum / count)
}

fn null_ratio(s: &ColumnStats) -> f64 {
    if s.row_count == 0 {
        0.0
    } else {
        s.null_count as f64 / s.row_count as f64
    }
}

/// Map two scalars to `[0, 1]`: `1.0` when equal, decaying toward `0` as they
/// diverge relative to their own magnitude (so the measure is scale-free).
fn relative_closeness(a: f64, b: f64) -> f64 {
    // A NaN operand has no meaningful closeness; treat it as maximally distant
    // rather than letting NaN propagate through the average (where it would make
    // the whole stats-similarity score NaN and silently drop the signal).
    if a.is_nan() || b.is_nan() {
        return 0.0;
    }
    let denom = a.abs().max(b.abs());
    if denom < f64::EPSILON {
        return 1.0; // both ~0
    }
    let rel = (a - b).abs() / denom;
    (1.0 - rel).clamp(0.0, 1.0)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn profile(dt: &str, sem: SemanticType, stats: Option<ColumnStats>) -> ColumnProfile {
        ColumnProfile {
            data_type: dt.to_string(),
            semantic_type: sem,
            stats,
        }
    }

    fn stats(null: usize, rows: usize, mean: f64, min: f64, max: f64) -> ColumnStats {
        ColumnStats {
            null_count: null,
            row_count: rows,
            mean: Some(mean),
            min: Some(min),
            max: Some(max),
        }
    }

    fn drift(removed: &[&str], added: &[&str]) -> SchemaDrift {
        SchemaDrift {
            added_fields: added.iter().map(|s| s.to_string()).collect(),
            removed_fields: removed.iter().map(|s| s.to_string()).collect(),
            changed_types: Vec::new(),
            renamed_fields: Vec::new(),
        }
    }

    // ── name similarity ──────────────────────────────────────────────────────

    #[test]
    fn test_name_similarity_identical() {
        assert_eq!(name_similarity("email", "email"), 1.0);
    }

    #[test]
    fn test_name_similarity_normalized_equal() {
        // Separators/case stripped → identical normalized form.
        assert_eq!(name_similarity("user_id", "UserId"), 1.0);
    }

    #[test]
    fn test_name_similarity_shared_token_high() {
        // "customer_email" vs "email_address" share the "email" token.
        let sim = name_similarity("customer_email", "email_address");
        assert!(sim > 0.0 && sim < 1.0);
    }

    #[test]
    fn test_name_similarity_small_edit_high() {
        let sim = name_similarity("created", "createdat");
        assert!(sim > 0.4, "got {sim}");
    }

    #[test]
    fn test_name_similarity_unrelated_low() {
        let sim = name_similarity("price", "shipping_country");
        assert!(sim < 0.3, "got {sim}");
    }

    #[test]
    fn test_levenshtein_basics() {
        assert_eq!(levenshtein("", ""), 0);
        assert_eq!(levenshtein("abc", "abc"), 0);
        assert_eq!(levenshtein("abc", "abd"), 1);
        assert_eq!(levenshtein("kitten", "sitting"), 3);
        assert_eq!(levenshtein("", "abc"), 3);
    }

    #[test]
    fn test_token_jaccard() {
        let a = tokenize("user_email_address");
        let b = tokenize("email_address");
        // tokens {user,email,address} vs {email,address} → 2/3
        assert!((token_jaccard(&a, &b) - 2.0 / 3.0).abs() < 1e-9);
    }

    // ── stats similarity ───────────────────────────────────────────────────────

    #[test]
    fn test_stats_similarity_identical() {
        let a = stats(0, 100, 50.0, 0.0, 100.0);
        let s = stats_similarity(&a, &a).unwrap();
        assert!((s - 1.0).abs() < 1e-9);
    }

    #[test]
    fn test_stats_similarity_far_apart_low() {
        let a = stats(0, 100, 1.0, 0.0, 2.0);
        let b = stats(0, 100, 1000.0, 900.0, 1100.0);
        let s = stats_similarity(&a, &b).unwrap();
        assert!(s < 0.4, "got {s}");
    }

    #[test]
    fn test_stats_similarity_none_for_non_numeric() {
        let a = ColumnStats {
            null_count: 0,
            row_count: 10,
            mean: None,
            min: None,
            max: None,
        };
        assert!(stats_similarity(&a, &a).is_none());
    }

    #[test]
    fn test_relative_closeness_both_zero() {
        assert_eq!(relative_closeness(0.0, 0.0), 1.0);
    }

    #[test]
    fn test_relative_closeness_nan_is_zero() {
        // A NaN operand must not propagate into the similarity average.
        assert_eq!(relative_closeness(f64::NAN, 1.0), 0.0);
        assert_eq!(relative_closeness(1.0, f64::NAN), 0.0);
    }

    // ── score_pair gating ──────────────────────────────────────────────────────

    #[test]
    fn test_score_pair_type_gate_rejects() {
        let from = profile("Int32", SemanticType::Unknown, None);
        let to = profile("Utf8", SemanticType::Unknown, None);
        let opts = RenameOptions::default();
        assert!(score_pair("a", &from, "a_renamed", &to, &opts).is_none());
    }

    #[test]
    fn test_score_pair_semantic_boosts_confidence() {
        // Different names, but both Email and same type + identical stats → match.
        let from = profile("Utf8", SemanticType::Email, None);
        let to = profile("Utf8", SemanticType::Email, None);
        let opts = RenameOptions::default();
        let r = score_pair("contact", &from, "email_address", &to, &opts);
        assert!(r.is_some(), "semantic + type should clear threshold");
        assert!(r.unwrap().signals.semantic_match);
    }

    #[test]
    fn test_score_pair_name_only_strong_match() {
        // No semantics, no stats — but near-identical name + type match.
        let from = profile("Int64", SemanticType::Unknown, None);
        let to = profile("Int64", SemanticType::Unknown, None);
        let opts = RenameOptions::default();
        let r = score_pair("customer_id", &from, "customerid", &to, &opts);
        assert!(r.is_some());
    }

    #[test]
    fn test_score_pair_name_only_weak_match_rejected() {
        // No semantics, no stats, and only a moderate name overlap. This clears
        // the raw 0.6 confidence gate (confidence == name_similarity here) but
        // must be rejected by the stricter name-only floor: without corroborating
        // evidence, "created" ↔ "updated" is not a rename.
        let from = profile("Utf8", SemanticType::Unknown, None);
        let to = profile("Utf8", SemanticType::Unknown, None);
        let opts = RenameOptions::default();
        assert!(
            score_pair("created", &from, "updated", &to, &opts).is_none(),
            "moderate name-only overlap must not auto-accept as a rename"
        );
    }

    #[test]
    fn test_score_pair_name_only_near_miss_sibling_rejected() {
        // `address_line1` ↔ `address_line2` differ by one char (~0.92 name sim)
        // but are distinct columns. With no semantic/stats corroboration the
        // stricter name-only floor must reject them as a rename.
        let from = profile("Utf8", SemanticType::Unknown, None);
        let to = profile("Utf8", SemanticType::Unknown, None);
        let opts = RenameOptions::default();
        assert!(
            score_pair("address_line1", &from, "address_line2", &to, &opts).is_none(),
            "single-char-different sibling columns must not auto-accept as a rename"
        );
    }

    #[test]
    fn test_score_pair_name_only_exact_normalized_still_accepts() {
        // The name-only floor must still admit a near-certain name match
        // (normalized-equal), which is the legitimate rename case.
        let from = profile("Int64", SemanticType::Unknown, None);
        let to = profile("Int64", SemanticType::Unknown, None);
        let opts = RenameOptions::default();
        assert!(score_pair("customer_id", &from, "customerid", &to, &opts).is_some());
    }

    #[test]
    fn test_score_pair_unrelated_rejected() {
        let from = profile(
            "Int64",
            SemanticType::Unknown,
            Some(stats(0, 100, 5.0, 1.0, 9.0)),
        );
        let to = profile(
            "Int64",
            SemanticType::Unknown,
            Some(stats(0, 100, 9000.0, 8000.0, 10000.0)),
        );
        let opts = RenameOptions::default();
        // Unrelated names + wildly different stats → below threshold.
        assert!(score_pair("price", &from, "population", &to, &opts).is_none());
    }

    // ── refine_renames end-to-end ──────────────────────────────────────────────

    #[test]
    fn test_refine_renames_simple_match() {
        let mut d = drift(&["email"], &["email_address"]);

        let mut removed = HashMap::new();
        removed.insert(
            "email".to_string(),
            profile("Utf8", SemanticType::Email, None),
        );
        let mut added = HashMap::new();
        added.insert(
            "email_address".to_string(),
            profile("Utf8", SemanticType::Email, None),
        );

        refine_renames(&mut d, &removed, &added, &RenameOptions::default());

        assert!(d.removed_fields.is_empty());
        assert!(d.added_fields.is_empty());
        assert_eq!(d.renamed_fields.len(), 1);
        assert_eq!(d.renamed_fields[0].from, "email");
        assert_eq!(d.renamed_fields[0].to, "email_address");
    }

    #[test]
    fn test_refine_renames_no_match_keeps_fields() {
        let mut d = drift(&["price"], &["shipping_country"]);

        let mut removed = HashMap::new();
        removed.insert(
            "price".to_string(),
            profile(
                "Int64",
                SemanticType::Unknown,
                Some(stats(0, 100, 5.0, 1.0, 9.0)),
            ),
        );
        let mut added = HashMap::new();
        added.insert(
            "shipping_country".to_string(),
            profile("Utf8", SemanticType::Unknown, None),
        );

        refine_renames(&mut d, &removed, &added, &RenameOptions::default());

        // Type mismatch + unrelated name → nothing moved.
        assert_eq!(d.removed_fields, vec!["price".to_string()]);
        assert_eq!(d.added_fields, vec!["shipping_country".to_string()]);
        assert!(d.renamed_fields.is_empty());
    }

    #[test]
    fn test_refine_renames_one_to_one_greedy() {
        // Two removed, two added, all same type+semantics. Each removed should
        // pair with its best-named counterpart, not double-book.
        let mut d = drift(&["user_email", "user_phone"], &["email_addr", "phone_no"]);

        let mut removed = HashMap::new();
        removed.insert(
            "user_email".to_string(),
            profile("Utf8", SemanticType::Email, None),
        );
        removed.insert(
            "user_phone".to_string(),
            profile("Utf8", SemanticType::PhoneNumber, None),
        );
        let mut added = HashMap::new();
        added.insert(
            "email_addr".to_string(),
            profile("Utf8", SemanticType::Email, None),
        );
        added.insert(
            "phone_no".to_string(),
            profile("Utf8", SemanticType::PhoneNumber, None),
        );

        refine_renames(&mut d, &removed, &added, &RenameOptions::default());

        assert_eq!(d.renamed_fields.len(), 2);
        let pairs: std::collections::HashSet<(String, String)> = d
            .renamed_fields
            .iter()
            .map(|r| (r.from.clone(), r.to.clone()))
            .collect();
        assert!(pairs.contains(&("user_email".to_string(), "email_addr".to_string())));
        assert!(pairs.contains(&("user_phone".to_string(), "phone_no".to_string())));
    }

    #[test]
    fn test_refine_renames_empty_sides_noop() {
        let mut d = drift(&[], &["new_col"]);
        let removed = HashMap::new();
        let mut added = HashMap::new();
        added.insert(
            "new_col".to_string(),
            profile("Int32", SemanticType::Unknown, None),
        );
        refine_renames(&mut d, &removed, &added, &RenameOptions::default());
        assert_eq!(d.added_fields, vec!["new_col".to_string()]);
        assert!(d.renamed_fields.is_empty());
    }

    #[test]
    fn test_profile_columns_assembles_signals() {
        use arrow::datatypes::{DataType, Field};

        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("email", DataType::Utf8, true),
        ]);
        let mut sem = HashMap::new();
        sem.insert("email".to_string(), SemanticType::Email);
        let mut cs = HashMap::new();
        cs.insert("id".to_string(), stats(0, 10, 5.0, 1.0, 10.0));

        let profiles = profile_columns(&schema, &sem, &cs);
        assert_eq!(profiles["id"].data_type, "Int64");
        assert_eq!(profiles["id"].semantic_type, SemanticType::Unknown);
        assert!(profiles["id"].stats.is_some());
        assert_eq!(profiles["email"].data_type, "Utf8");
        assert_eq!(profiles["email"].semantic_type, SemanticType::Email);
        assert!(profiles["email"].stats.is_none());
    }

    #[test]
    fn test_field_rename_json_round_trip() {
        let r = FieldRename {
            from: "a".into(),
            to: "b".into(),
            confidence: 0.87,
            signals: RenameSignals {
                type_match: true,
                semantic_match: true,
                name_similarity: 0.5,
                stats_similarity: Some(0.9),
            },
        };
        let json = serde_json::to_string(&r).unwrap();
        let back: FieldRename = serde_json::from_str(&json).unwrap();
        assert_eq!(r, back);
    }
}
