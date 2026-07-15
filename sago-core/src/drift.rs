use crate::schema_codec::serialize_data_type;
use crate::semantic::{SemanticType, infer_semantic_type_multi};
use arrow::array::{
    Array, Float32Array, Float64Array, Int8Array, Int16Array, Int32Array, Int64Array,
    LargeStringArray, StringArray, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
};
use arrow::datatypes::{DataType, Schema};
use arrow::record_batch::RecordBatch;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct SchemaDrift {
    pub added_fields: Vec<String>,
    pub removed_fields: Vec<String>,
    pub changed_types: Vec<TypeChange>,
    /// Columns recognised as renames of removed columns rather than genuine
    /// drops/additions. Populated by [`crate::rename::refine_renames`]; the
    /// involved names are removed from `added_fields`/`removed_fields`.
    #[serde(default)]
    pub renamed_fields: Vec<crate::rename::FieldRename>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct SemanticDrift {
    pub field_name: String,
    pub source_type: SemanticType,
    pub target_type: SemanticType,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct TypeChange {
    pub field_name: String,
    pub old_type: String,
    pub new_type: String,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct ColumnStats {
    pub null_count: usize,
    pub row_count: usize,
    pub mean: Option<f64>,
    pub min: Option<f64>,
    pub max: Option<f64>,
    /// Population variance of the numeric values, when available. Lets
    /// distribution-similarity checks (e.g. rename matching) distinguish
    /// columns that share a mean/min/max but have very different shapes
    /// (e.g. uniform vs. bimodal), which `mean`/`min`/`max` alone cannot.
    #[serde(default)]
    pub variance: Option<f64>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct DataDrift {
    pub column_drifts: HashMap<String, ColumnDrift>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct ColumnDrift {
    pub source_stats: ColumnStats,
    pub target_stats: ColumnStats,
    pub mean_drift: Option<f64>,
    pub null_count_drift: i64,
    pub ks_statistic: Option<f64>,
    pub ks_p_value: Option<f64>,
    #[serde(default)]
    pub psi_statistic: Option<f64>,
    /// PSI computed over value-frequency distributions, for non-numeric
    /// (string/categorical) columns that `ks_statistic`/`psi_statistic` never
    /// populate. `None` for numeric columns (which use `psi_statistic`
    /// instead) or when either side has no non-null values.
    #[serde(default)]
    pub categorical_drift: Option<f64>,
}

impl ColumnDrift {
    /// Whether this column's distribution has drifted beyond `threshold`,
    /// measured by the Population Stability Index (PSI) — a scale-free metric
    /// (unlike the raw `mean_drift`), so a single `checks.drift_threshold`
    /// applies uniformly across columns. Columns without a PSI (non-numeric, or
    /// stats-only plans where PSI isn't computed) fall back to
    /// `categorical_drift`; a column with neither is treated as not-breaching.
    ///
    /// Common PSI rules of thumb: < 0.1 no significant shift, 0.1–0.25 moderate,
    /// > 0.25 major. A caller-supplied `threshold` overrides that judgement.
    #[must_use]
    pub fn breaches_threshold(&self, threshold: f64) -> bool {
        self.psi_statistic.is_some_and(|psi| psi > threshold)
            || self.categorical_drift.is_some_and(|psi| psi > threshold)
    }
}

/// Field names present in *both* schemas. Shared by the batch-based detectors so
/// they compute "columns in common" the same way.
fn common_field_names(a: &Schema, b: &Schema) -> Vec<String> {
    let b_names: HashSet<&str> = b.fields().iter().map(|f| f.name().as_str()).collect();
    a.fields()
        .iter()
        .map(|f| f.name())
        .filter(|n| b_names.contains(n.as_str()))
        .cloned()
        .collect()
}

pub fn detect_schema_drift(source: &Schema, target: &Schema) -> SchemaDrift {
    let source_fields: HashSet<_> = source.fields().iter().map(|f| f.name().clone()).collect();
    let target_fields: HashSet<_> = target.fields().iter().map(|f| f.name().clone()).collect();

    let added_fields: Vec<String> = target_fields.difference(&source_fields).cloned().collect();
    let removed_fields: Vec<String> = source_fields.difference(&target_fields).cloned().collect();

    let mut changed_types = Vec::new();

    for field_name in source_fields.intersection(&target_fields) {
        let source_field = source.field_with_name(field_name).unwrap();
        let target_field = target.field_with_name(field_name).unwrap();

        if source_field.data_type() != target_field.data_type() {
            changed_types.push(TypeChange {
                field_name: field_name.clone(),
                old_type: serialize_data_type(source_field.data_type()),
                new_type: serialize_data_type(target_field.data_type()),
            });
        }
    }

    SchemaDrift {
        added_fields,
        removed_fields,
        changed_types,
        renamed_fields: Vec::new(),
    }
}

pub fn detect_semantic_drift(
    source_batches: &[RecordBatch],
    target_batches: &[RecordBatch],
) -> Vec<SemanticDrift> {
    let mut semantic_drifts = Vec::new();

    if source_batches.is_empty() || target_batches.is_empty() {
        return semantic_drifts;
    }

    let source_schema = source_batches[0].schema();
    let target_schema = target_batches[0].schema();

    for field_name in common_field_names(&source_schema, &target_schema) {
        let source_cols: Vec<_> = source_batches
            .iter()
            .filter_map(|b| b.column_by_name(&field_name).cloned())
            .collect();
        let target_cols: Vec<_> = target_batches
            .iter()
            .filter_map(|b| b.column_by_name(&field_name).cloned())
            .collect();

        let source_semantic = infer_semantic_type_multi(&field_name, &source_cols);
        let target_semantic = infer_semantic_type_multi(&field_name, &target_cols);

        if source_semantic != target_semantic {
            semantic_drifts.push(SemanticDrift {
                field_name: field_name.clone(),
                source_type: source_semantic,
                target_type: target_semantic,
            });
        }
    }

    semantic_drifts
}

/// The Arrow numeric types Sago treats as drift-comparable, and the single place
/// that authoritative list lives. Used by both [`calculate_column_stats`] and
/// [`extract_numeric_values`] so they can never disagree about what "numeric" is.
fn is_supported_numeric(dt: &DataType) -> bool {
    matches!(
        dt,
        DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Float32
            | DataType::Float64
    )
}

/// Call `f` with every non-null value of `column` as an `f64`, downcasting the
/// array exactly once. Does nothing for non-numeric columns. Centralises the
/// per-type downcast so callers don't re-match `data_type()` per row.
///
/// `NaN` values are skipped: they carry no distributional information and would
/// otherwise poison downstream statistics — a single `NaN` collapses PSI's
/// min/max-derived bins (every value maps to bin 0) and drags a running mean to
/// `NaN`. Treating them like nulls keeps every metric well-defined.
fn for_each_numeric(column: &dyn Array, mut f: impl FnMut(f64)) {
    macro_rules! iter_array {
        ($ty:ty, $cast:expr) => {{
            let arr = column.as_any().downcast_ref::<$ty>().unwrap();
            for i in 0..arr.len() {
                if !arr.is_null(i) {
                    let v = $cast(arr.value(i));
                    if !v.is_nan() {
                        f(v);
                    }
                }
            }
        }};
    }
    match column.data_type() {
        DataType::Int8 => iter_array!(Int8Array, |v| v as f64),
        DataType::Int16 => iter_array!(Int16Array, |v| v as f64),
        DataType::Int32 => iter_array!(Int32Array, |v| v as f64),
        DataType::Int64 => iter_array!(Int64Array, |v| v as f64),
        DataType::UInt8 => iter_array!(UInt8Array, |v| v as f64),
        DataType::UInt16 => iter_array!(UInt16Array, |v| v as f64),
        DataType::UInt32 => iter_array!(UInt32Array, |v| v as f64),
        DataType::UInt64 => iter_array!(UInt64Array, |v| v as f64),
        DataType::Float32 => iter_array!(Float32Array, |v| v as f64),
        DataType::Float64 => iter_array!(Float64Array, |v: f64| v),
        _ => {}
    }
}

/// Derives mean/min/max/variance from an already-extracted value slice (e.g.
/// from [`extract_numeric_values`]) rather than re-scanning the source
/// arrays, so callers that need both the raw values (KS, PSI) and the
/// summary stats only pay for one full traversal of the column.
fn numeric_summary(vals: &[f64]) -> (Option<f64>, Option<f64>, Option<f64>, Option<f64>) {
    if vals.is_empty() {
        return (None, None, None, None);
    }
    let n = vals.len() as f64;
    let sum: f64 = vals.iter().sum();
    let mean = sum / n;
    let mut min = f64::MAX;
    let mut max = f64::MIN;
    let mut sq_diff_sum = 0.0;
    for &v in vals {
        if v < min {
            min = v;
        }
        if v > max {
            max = v;
        }
        sq_diff_sum += (v - mean) * (v - mean);
    }
    (Some(mean), Some(min), Some(max), Some(sq_diff_sum / n))
}

pub fn calculate_column_stats(batches: &[RecordBatch], column_name: &str) -> Option<ColumnStats> {
    if batches.is_empty() {
        return None;
    }

    let mut null_count = 0;
    let mut row_count = 0;
    let mut has_numeric = false;

    for batch in batches {
        let column = batch.column_by_name(column_name)?;
        null_count += column.null_count();
        row_count += batch.num_rows();
        has_numeric |= is_supported_numeric(column.data_type());
    }

    // Single traversal of the numeric values (shared with any caller that
    // also needs the raw values, e.g. detect_data_drift's KS/PSI path)
    // rather than a second downcast-and-iterate pass just for sum/min/max.
    let vals = if has_numeric {
        extract_numeric_values(batches, column_name)
    } else {
        Vec::new()
    };
    let (mean, min, max, variance) = numeric_summary(&vals);

    Some(ColumnStats {
        null_count,
        row_count,
        mean,
        min,
        max,
        variance,
    })
}

pub fn detect_data_drift_from_stats(
    source: &HashMap<String, ColumnStats>,
    target: &HashMap<String, ColumnStats>,
) -> DataDrift {
    let mut column_drifts = HashMap::new();

    let source_keys: HashSet<&String> = source.keys().collect();
    let target_keys: HashSet<&String> = target.keys().collect();

    for field_name in source_keys.intersection(&target_keys) {
        let source_stats = source.get(*field_name).unwrap().clone();
        let target_stats = target.get(*field_name).unwrap().clone();

        let mean_drift = if let (Some(s), Some(t)) = (source_stats.mean, target_stats.mean) {
            Some((t - s).abs())
        } else {
            None
        };

        let null_count_drift = target_stats.null_count as i64 - source_stats.null_count as i64;

        column_drifts.insert(
            (*field_name).clone(),
            ColumnDrift {
                source_stats,
                target_stats,
                mean_drift,
                null_count_drift,
                ks_statistic: None,
                ks_p_value: None,
                psi_statistic: None,
                categorical_drift: None,
            },
        );
    }

    DataDrift { column_drifts }
}

pub fn detect_data_drift(
    source_batches: &[RecordBatch],
    target_batches: &[RecordBatch],
) -> DataDrift {
    let mut column_drifts = HashMap::new();

    if source_batches.is_empty() || target_batches.is_empty() {
        return DataDrift { column_drifts };
    }

    let source_schema = source_batches[0].schema();
    let target_schema = target_batches[0].schema();

    for field_name in common_field_names(&source_schema, &target_schema) {
        let field_name = field_name.as_str();
        let (source_null_count, source_row_count) =
            match null_and_row_count(source_batches, field_name) {
                Some(v) => v,
                None => continue,
            };
        let (target_null_count, target_row_count) =
            match null_and_row_count(target_batches, field_name) {
                Some(v) => v,
                None => continue,
            };

        // Extract each side's numeric values exactly once, then derive
        // mean/min/max/variance from that same extraction (numeric_summary)
        // instead of a second full downcast-and-iterate pass over the source
        // arrays, and reuse the values again for the KS statistic (needs them
        // sorted) and PSI (bins the raw values).
        let src_vals = extract_numeric_values(source_batches, field_name);
        let tgt_vals = extract_numeric_values(target_batches, field_name);

        let (s_mean, s_min, s_max, s_var) = numeric_summary(&src_vals);
        let (t_mean, t_min, t_max, t_var) = numeric_summary(&tgt_vals);

        let source_stats = ColumnStats {
            null_count: source_null_count,
            row_count: source_row_count,
            mean: s_mean,
            min: s_min,
            max: s_max,
            variance: s_var,
        };
        let target_stats = ColumnStats {
            null_count: target_null_count,
            row_count: target_row_count,
            mean: t_mean,
            min: t_min,
            max: t_max,
            variance: t_var,
        };

        let mean_drift = if let (Some(s), Some(t)) = (s_mean, t_mean) {
            Some((t - s).abs())
        } else {
            None
        };

        let null_count_drift = target_null_count as i64 - source_null_count as i64;

        let psi_statistic = calculate_psi(&src_vals, &tgt_vals);

        let mut src_sorted = src_vals;
        let mut tgt_sorted = tgt_vals;
        src_sorted.sort_by(|a, b| a.total_cmp(b));
        tgt_sorted.sort_by(|a, b| a.total_cmp(b));
        let (ks_statistic, ks_p_value) = ks_from_sorted(&src_sorted, &tgt_sorted);

        let categorical_drift =
            detect_categorical_drift(source_batches, target_batches, field_name);

        column_drifts.insert(
            field_name.to_string(),
            ColumnDrift {
                source_stats,
                target_stats,
                mean_drift,
                null_count_drift,
                ks_statistic,
                ks_p_value,
                psi_statistic,
                categorical_drift,
            },
        );
    }

    DataDrift { column_drifts }
}

/// Total null/row count for `column_name` across `batches`, or `None` if the
/// column is missing from any batch. Split out of [`calculate_column_stats`]
/// so [`detect_data_drift`] can compute this cheap metadata without also
/// paying for a second numeric extraction pass.
fn null_and_row_count(batches: &[RecordBatch], column_name: &str) -> Option<(usize, usize)> {
    let mut null_count = 0;
    let mut row_count = 0;
    for batch in batches {
        let column = batch.column_by_name(column_name)?;
        null_count += column.null_count();
        row_count += batch.num_rows();
    }
    Some((null_count, row_count))
}

pub(crate) fn extract_numeric_values(batches: &[RecordBatch], column_name: &str) -> Vec<f64> {
    let mut values = Vec::new();
    for batch in batches {
        if let Some(column) = batch.column_by_name(column_name) {
            for_each_numeric(column.as_ref(), |v| values.push(v));
        }
    }
    values
}

/// Call `f` with every non-null string value of `column`. Mirrors
/// [`for_each_numeric`]'s "downcast once, iterate" shape but for Utf8/LargeUtf8
/// columns, so categorical drift can be computed without cloning values into
/// an intermediate `Vec<String>` per batch.
fn for_each_string(column: &dyn Array, mut f: impl FnMut(&str)) {
    match column.data_type() {
        DataType::Utf8 => {
            let arr = column.as_any().downcast_ref::<StringArray>().unwrap();
            for i in 0..arr.len() {
                if !arr.is_null(i) {
                    f(arr.value(i));
                }
            }
        }
        DataType::LargeUtf8 => {
            let arr = column.as_any().downcast_ref::<LargeStringArray>().unwrap();
            for i in 0..arr.len() {
                if !arr.is_null(i) {
                    f(arr.value(i));
                }
            }
        }
        _ => {}
    }
}

fn value_frequencies(batches: &[RecordBatch], column_name: &str) -> HashMap<String, usize> {
    let mut counts = HashMap::new();
    for batch in batches {
        if let Some(column) = batch.column_by_name(column_name) {
            for_each_string(column.as_ref(), |v| {
                *counts.entry(v.to_string()).or_insert(0) += 1;
            });
        }
    }
    counts
}

/// Population Stability Index over value-frequency distributions, for
/// string/categorical columns. Unlike the numeric `calculate_psi` (which
/// bins continuous values into quantile ranges), each *distinct value* is its
/// own bin here — the natural analogue for categorical data, where "buckets"
/// are already discrete. Returns `None` for numeric columns (handled by
/// `detect_data_drift`'s KS/PSI path instead) or if either side has no
/// non-null string values.
fn detect_categorical_drift(
    source_batches: &[RecordBatch],
    target_batches: &[RecordBatch],
    column_name: &str,
) -> Option<f64> {
    let is_string_column = source_batches
        .first()
        .and_then(|b| b.column_by_name(column_name))
        .is_some_and(|c| matches!(c.data_type(), DataType::Utf8 | DataType::LargeUtf8));
    if !is_string_column {
        return None;
    }

    let source_counts = value_frequencies(source_batches, column_name);
    let target_counts = value_frequencies(target_batches, column_name);

    let n_src: usize = source_counts.values().sum();
    let n_tgt: usize = target_counts.values().sum();
    if n_src == 0 || n_tgt == 0 {
        return None;
    }
    let n_src = n_src as f64;
    let n_tgt = n_tgt as f64;

    let all_values: HashSet<&String> = source_counts.keys().chain(target_counts.keys()).collect();
    let mut psi = 0.0;
    for value in all_values {
        let e = (*source_counts.get(value).unwrap_or(&0) as f64 / n_src).max(PSI_EPSILON);
        let a = (*target_counts.get(value).unwrap_or(&0) as f64 / n_tgt).max(PSI_EPSILON);
        psi += (a - e) * (a / e).ln();
    }
    Some(psi)
}

/// Two-sample KS statistic and p-value from already-sorted samples (ascending by
/// `total_cmp`). Returns `(None, None)` if either side is empty. Kept separate
/// from extraction so callers can sort once and reuse the buffers.
fn ks_from_sorted(source_vals: &[f64], target_vals: &[f64]) -> (Option<f64>, Option<f64>) {
    if source_vals.is_empty() || target_vals.is_empty() {
        return (None, None);
    }

    let n1 = source_vals.len() as f64;
    let n2 = target_vals.len() as f64;

    let mut max_dist: f64 = 0.0;
    let mut i = 0;
    let mut j = 0;

    // Merge-walk the two sorted samples. The empirical CDFs are step functions
    // that only jump at observed values, so the KS statistic — sup|F1 - F2| — is
    // attained at one of those jump points. On each step we take the smaller of
    // the two current values and advance *past every copy* of it in both samples
    // before sampling the gap. Advancing one element at a time instead (the naive
    // approach) samples |F1 - F2| in the middle of a tie run, which can only
    // overestimate the true statistic and fires false-positive drift on tied /
    // low-cardinality (e.g. integer, categorical) columns.
    while i < source_vals.len() && j < target_vals.len() {
        // `total_cmp` gives a total order over f64 (NaN sorts last, consistent
        // with the sort above) and defines equality even for NaN, so the inner
        // loops always advance and the walk terminates.
        let v = if source_vals[i].total_cmp(&target_vals[j]) == std::cmp::Ordering::Greater {
            target_vals[j]
        } else {
            source_vals[i]
        };
        while i < source_vals.len() && source_vals[i].total_cmp(&v) == std::cmp::Ordering::Equal {
            i += 1;
        }
        while j < target_vals.len() && target_vals[j].total_cmp(&v) == std::cmp::Ordering::Equal {
            j += 1;
        }

        let dist = (i as f64 / n1 - j as f64 / n2).abs();
        if dist > max_dist {
            max_dist = dist;
        }
    }

    // Asymptotic 2-sample KS p-value (Kolmogorov distribution).
    let en = (n1 * n2) / (n1 + n2);
    let sqrt_en = en.sqrt();
    let lambda = (sqrt_en + 0.12 + 0.11 / sqrt_en) * max_dist;

    let p_value = if lambda <= 0.0 {
        // max_dist == 0 ⇒ the empirical distributions are identical ⇒ no evidence
        // against the null, i.e. p = 1.0. (The series below degenerates at
        // lambda = 0 and must not be used — it would leave p at 0.0, which reads
        // as "maximally significant drift", the exact opposite of the truth.)
        1.0
    } else {
        let mut sum = 0.0;
        for k in 1u32..=100 {
            let sign = if k.is_multiple_of(2) { -1.0 } else { 1.0 };
            sum += sign * (-2.0 * (k as f64 * lambda).powi(2)).exp();
        }
        (2.0 * sum).clamp(0.0, 1.0)
    };

    (Some(max_dist), Some(p_value))
}

const PSI_NUM_BINS: usize = 10;
const PSI_EPSILON: f64 = 0.0001;

/// Population Stability Index between two numeric samples, or `None` if either
/// side is empty. Exposed so callers that only persist samples (e.g. the `plan`
/// baseline vs. live comparison) can compute the same normalized drift metric
/// that [`detect_data_drift`] computes from full record batches.
pub fn psi_from_samples(source: &[f64], target: &[f64]) -> Option<f64> {
    calculate_psi(source, target)
}

/// The `q`-quantile (`q` in `[0, 1]`) of an already-ascending-sorted slice,
/// using linear interpolation between order statistics (the common "type 7"
/// definition). `sorted` must be non-empty.
fn quantile_sorted(sorted: &[f64], q: f64) -> f64 {
    let n = sorted.len();
    if n == 1 {
        return sorted[0];
    }
    let pos = q * (n - 1) as f64;
    let lo = pos.floor() as usize;
    let hi = pos.ceil() as usize;
    if lo == hi {
        return sorted[lo];
    }
    let frac = pos - lo as f64;
    sorted[lo] + frac * (sorted[hi] - sorted[lo])
}

fn calculate_psi(source: &[f64], target: &[f64]) -> Option<f64> {
    // Drop NaN up front so the metric is self-consistent regardless of caller.
    // `for_each_numeric` already skips NaN on the batch path, but the public
    // `psi_from_samples` entry point can be handed arbitrary slices; an unfiltered
    // NaN both escapes the finite min/max (which ignore NaN) and then bins into
    // bin 0 via `NaN as usize == 0`, fabricating a meaningless PSI.
    let source: Vec<f64> = source.iter().copied().filter(|v| !v.is_nan()).collect();
    let target: Vec<f64> = target.iter().copied().filter(|v| !v.is_nan()).collect();
    if source.is_empty() || target.is_empty() {
        return None;
    }

    let min_val = source
        .iter()
        .chain(target.iter())
        .copied()
        .fold(f64::INFINITY, f64::min);
    let max_val = source
        .iter()
        .chain(target.iter())
        .copied()
        .fold(f64::NEG_INFINITY, f64::max);

    if (max_val - min_val).abs() < f64::EPSILON {
        return Some(0.0);
    }

    // Bin edges are the reference (source) distribution's deciles rather than
    // fixed equal-width cuts. Equal-width bins put almost all mass in one bucket
    // for skewed columns, so a real shift *within* the dense region is diluted
    // across empty bins and missed; quantile bins allocate resolution where the
    // data actually is. This is the standard PSI construction.
    let mut sorted_src = source.clone();
    sorted_src.sort_by(|a, b| a.total_cmp(b));
    // PSI_NUM_BINS bins ⇒ PSI_NUM_BINS-1 interior edges at deciles 0.1..0.9.
    // Edges are non-decreasing; duplicates (from ties in the reference) simply
    // yield unreachable bins, which contribute 0 to the sum.
    let edges: Vec<f64> = (1..PSI_NUM_BINS)
        .map(|i| quantile_sorted(&sorted_src, i as f64 / PSI_NUM_BINS as f64))
        .collect();

    // Bin index of `v`: the number of edges it lies at or above, in
    // `0..PSI_NUM_BINS`. Same rule for both samples, so the comparison is
    // consistent regardless of the `<=` vs `<` boundary choice.
    let bin_of = |v: f64| -> usize { edges.partition_point(|&e| e <= v) };

    let mut source_counts = [0usize; PSI_NUM_BINS];
    let mut target_counts = [0usize; PSI_NUM_BINS];
    for &v in &source {
        source_counts[bin_of(v)] += 1;
    }
    for &v in &target {
        target_counts[bin_of(v)] += 1;
    }

    let n_src = source.len() as f64;
    let n_tgt = target.len() as f64;
    let mut psi = 0.0;

    for i in 0..PSI_NUM_BINS {
        let e = (source_counts[i] as f64 / n_src).max(PSI_EPSILON);
        let a = (target_counts[i] as f64 / n_tgt).max(PSI_EPSILON);
        psi += (a - e) * (a / e).ln();
    }

    Some(psi)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Float64Array, Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field};
    use std::sync::Arc;

    // ── helpers ──────────────────────────────────────────────────────────────

    fn int32_batch(name: &str, values: Vec<Option<i32>>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new(name, DataType::Int32, true)]));
        RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(values))]).unwrap()
    }

    fn f64_batch(name: &str, values: Vec<Option<f64>>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new(name, DataType::Float64, true)]));
        RecordBatch::try_new(schema, vec![Arc::new(Float64Array::from(values))]).unwrap()
    }

    fn str_batch(name: &str, values: Vec<Option<&str>>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new(name, DataType::Utf8, true)]));
        RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(values))]).unwrap()
    }

    // ── detect_schema_drift ──────────────────────────────────────────────────

    #[test]
    fn test_detect_schema_drift() {
        let source = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("age", DataType::Int32, false),
        ]);

        let target = Schema::new(vec![
            Field::new("id", DataType::Int64, false), // type change
            Field::new("name", DataType::Utf8, false),
            Field::new("email", DataType::Utf8, true), // added
        ]); // removed "age"

        let drift = detect_schema_drift(&source, &target);

        assert!(drift.added_fields.contains(&"email".to_string()));
        assert!(drift.removed_fields.contains(&"age".to_string()));
        assert_eq!(drift.changed_types.len(), 1);
        assert_eq!(drift.changed_types[0].field_name, "id");
    }

    #[test]
    fn test_detect_schema_drift_no_changes() {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]);
        let drift = detect_schema_drift(&schema, &schema);
        assert!(drift.added_fields.is_empty());
        assert!(drift.removed_fields.is_empty());
        assert!(drift.changed_types.is_empty());
    }

    #[test]
    fn test_detect_schema_drift_empty_schemas() {
        let empty = Schema::new(vec![] as Vec<Field>);
        let drift = detect_schema_drift(&empty, &empty);
        assert!(drift.added_fields.is_empty());
        assert!(drift.removed_fields.is_empty());
        assert!(drift.changed_types.is_empty());
    }

    // ── calculate_column_stats ───────────────────────────────────────────────

    #[test]
    fn test_column_stats_int32() {
        let batch = int32_batch("val", vec![Some(1), Some(2), Some(3)]);
        let stats = calculate_column_stats(&[batch], "val").unwrap();
        assert_eq!(stats.row_count, 3);
        assert_eq!(stats.null_count, 0);
        assert_eq!(stats.mean, Some(2.0));
        assert_eq!(stats.min, Some(1.0));
        assert_eq!(stats.max, Some(3.0));
    }

    #[test]
    fn test_column_stats_float64() {
        let batch = f64_batch("score", vec![Some(1.5), Some(2.5), Some(3.0)]);
        let stats = calculate_column_stats(&[batch], "score").unwrap();
        assert_eq!(stats.row_count, 3);
        assert_eq!(stats.null_count, 0);
        assert!((stats.mean.unwrap() - 7.0 / 3.0).abs() < 1e-10);
    }

    #[test]
    fn test_column_stats_with_nulls() {
        let batch = int32_batch("val", vec![Some(10), None, Some(20)]);
        let stats = calculate_column_stats(&[batch], "val").unwrap();
        assert_eq!(stats.null_count, 1);
        assert_eq!(stats.row_count, 3);
        assert_eq!(stats.mean, Some(15.0)); // (10+20)/2
        assert_eq!(stats.min, Some(10.0));
        assert_eq!(stats.max, Some(20.0));
    }

    #[test]
    fn test_column_stats_all_nulls() {
        let batch = int32_batch("val", vec![None, None, None]);
        let stats = calculate_column_stats(&[batch], "val").unwrap();
        assert_eq!(stats.null_count, 3);
        assert_eq!(stats.row_count, 3);
        assert_eq!(stats.mean, None);
        assert_eq!(stats.min, None);
        assert_eq!(stats.max, None);
    }

    #[test]
    fn test_column_stats_non_numeric() {
        let batch = str_batch("name", vec![Some("alice"), Some("bob")]);
        let stats = calculate_column_stats(&[batch], "name").unwrap();
        assert_eq!(stats.row_count, 2);
        assert_eq!(stats.null_count, 0);
        assert_eq!(stats.mean, None);
        assert_eq!(stats.min, None);
        assert_eq!(stats.max, None);
    }

    #[test]
    fn test_column_stats_empty_batches() {
        let result = calculate_column_stats(&[], "val");
        assert!(result.is_none());
    }

    #[test]
    fn test_column_stats_multiple_batches() {
        let b1 = int32_batch("val", vec![Some(1), Some(2)]);
        let b2 = int32_batch("val", vec![Some(3), Some(4)]);
        let stats = calculate_column_stats(&[b1, b2], "val").unwrap();
        assert_eq!(stats.row_count, 4);
        assert_eq!(stats.mean, Some(2.5));
        assert_eq!(stats.min, Some(1.0));
        assert_eq!(stats.max, Some(4.0));
    }

    // ── detect_data_drift ────────────────────────────────────────────────────

    #[test]
    fn test_detect_data_drift() {
        let schema = Arc::new(Schema::new(vec![Field::new("val", DataType::Int32, true)]));

        let source_batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![Some(1), Some(2), Some(3)]))],
        )
        .unwrap();

        let target_batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![Some(10), Some(20), None]))],
        )
        .unwrap();

        let drift = detect_data_drift(&[source_batch], &[target_batch]);

        let val_drift = drift.column_drifts.get("val").unwrap();
        assert_eq!(val_drift.source_stats.mean, Some(2.0));
        assert_eq!(val_drift.target_stats.mean, Some(15.0));
        assert_eq!(val_drift.mean_drift, Some(13.0));
        assert_eq!(val_drift.null_count_drift, 1);
        assert!(val_drift.ks_statistic.is_some());
        assert!(val_drift.ks_p_value.is_some());
    }

    #[test]
    fn test_detect_data_drift_empty_batches() {
        let drift = detect_data_drift(&[], &[]);
        assert!(drift.column_drifts.is_empty());
    }

    #[test]
    fn test_detect_data_drift_populates_variance() {
        // detect_data_drift must derive variance from the same extraction it
        // already does for KS/PSI (numeric_summary), not skip it.
        let source_batch = int32_batch("val", vec![Some(1), Some(2), Some(3)]);
        let target_batch = int32_batch("val", vec![Some(1), Some(1), Some(1)]);
        let drift = detect_data_drift(&[source_batch], &[target_batch]);
        let val_drift = drift.column_drifts.get("val").unwrap();
        // source [1,2,3]: mean 2, variance = ((1)^2+(0)^2+(1)^2)/3 = 2/3
        assert!((val_drift.source_stats.variance.unwrap() - 2.0 / 3.0).abs() < 1e-9);
        // target [1,1,1]: constant, variance 0
        assert!((val_drift.target_stats.variance.unwrap() - 0.0).abs() < 1e-9);
    }

    // ── KS test (via detect_data_drift) ──────────────────────────────────────

    #[test]
    fn test_ks_identical_distributions() {
        let b1 = int32_batch("val", vec![Some(1), Some(2), Some(3), Some(4)]);
        let b2 = int32_batch("val", vec![Some(1), Some(2), Some(3), Some(4)]);
        let drift = detect_data_drift(&[b1], &[b2]);
        let col = drift.column_drifts.get("val").unwrap();
        assert_eq!(col.ks_statistic, Some(0.0));
    }

    #[test]
    fn test_ks_disjoint_distributions() {
        let b1 = int32_batch("val", vec![Some(1), Some(2), Some(3)]);
        let b2 = int32_batch("val", vec![Some(100), Some(200), Some(300)]);
        let drift = detect_data_drift(&[b1], &[b2]);
        let col = drift.column_drifts.get("val").unwrap();
        assert_eq!(col.ks_statistic, Some(1.0));
    }

    #[test]
    fn test_ks_non_numeric_column() {
        let b1 = str_batch("name", vec![Some("a"), Some("b")]);
        let b2 = str_batch("name", vec![Some("c"), Some("d")]);
        let drift = detect_data_drift(&[b1], &[b2]);
        let col = drift.column_drifts.get("name").unwrap();
        assert_eq!(col.ks_statistic, None);
        assert_eq!(col.ks_p_value, None);
        assert_eq!(col.mean_drift, None);
    }

    // ── detect_semantic_drift ────────────────────────────────────────────────

    #[test]
    fn test_detect_semantic_drift_empty_batches() {
        let result = detect_semantic_drift(&[], &[]);
        assert!(result.is_empty());
    }

    #[test]
    fn test_detect_semantic_drift_none() {
        // Same email data in both — no drift expected
        let b1 = str_batch("email", vec![Some("a@example.com"), Some("b@example.com")]);
        let b2 = str_batch("email", vec![Some("c@example.com"), Some("d@example.com")]);
        let result = detect_semantic_drift(&[b1], &[b2]);
        assert!(result.is_empty());
    }

    #[test]
    fn test_detect_semantic_drift_detected() {
        // Source: column named "contact" with email data → Email
        // Target: same column name but with plain strings → Unknown
        let source = str_batch(
            "contact",
            vec![
                Some("a@x.com"),
                Some("b@x.com"),
                Some("c@x.com"),
                Some("d@x.com"),
                Some("e@x.com"),
            ],
        );
        let target = str_batch(
            "contact",
            vec![
                Some("not-an-email"),
                Some("also-not"),
                Some("nope"),
                Some("random"),
                Some("text"),
            ],
        );
        let result = detect_semantic_drift(&[source], &[target]);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].field_name, "contact");
        assert_eq!(result[0].source_type, SemanticType::Email);
        assert_eq!(result[0].target_type, SemanticType::Unknown);
    }

    #[test]
    fn test_detect_semantic_drift_samples_across_all_batches() {
        // Regression: a garbage-looking first batch must not, by itself,
        // determine the classification of an otherwise-uniform multi-batch
        // column. batch[0] here is 3 non-email strings; batches 1..3 are all
        // emails — the column as a whole is >80% email and must classify as
        // Email on both sides, so no drift is reported.
        let junk_batch = str_batch("contact", vec![Some("nope"), Some("nah"), Some("no")]);
        let email_batch_1 = str_batch(
            "contact",
            vec![
                Some("a@x.com"),
                Some("b@x.com"),
                Some("c@x.com"),
                Some("d@x.com"),
                Some("e@x.com"),
                Some("f@x.com"),
                Some("g@x.com"),
                Some("h@x.com"),
                Some("i@x.com"),
                Some("j@x.com"),
            ],
        );
        let email_batch_2 = email_batch_1.clone();

        let source = vec![
            junk_batch.clone(),
            email_batch_1.clone(),
            email_batch_2.clone(),
        ];
        let target = vec![junk_batch, email_batch_1, email_batch_2];

        let result = detect_semantic_drift(&source, &target);
        assert!(
            result.is_empty(),
            "expected no drift (both sides classify Email from the full column), got {result:?}"
        );
    }

    // ── extract_numeric_values ───────────────────────────────────────────────

    #[test]
    fn test_extract_numeric_values_int32() {
        let batch = int32_batch("v", vec![Some(10), None, Some(30)]);
        let vals = extract_numeric_values(&[batch], "v");
        assert_eq!(vals, vec![10.0, 30.0]);
    }

    #[test]
    fn test_extract_numeric_values_float64() {
        let batch = f64_batch("v", vec![Some(1.5), Some(2.5)]);
        let vals = extract_numeric_values(&[batch], "v");
        assert_eq!(vals, vec![1.5, 2.5]);
    }

    #[test]
    fn test_extract_numeric_values_non_numeric_returns_empty() {
        let batch = str_batch("v", vec![Some("a"), Some("b")]);
        let vals = extract_numeric_values(&[batch], "v");
        assert!(vals.is_empty());
    }

    #[test]
    fn test_extract_numeric_values_missing_column_returns_empty() {
        let batch = int32_batch("v", vec![Some(1)]);
        let vals = extract_numeric_values(&[batch], "other");
        assert!(vals.is_empty());
    }

    #[test]
    fn test_extract_numeric_values_multiple_batches_concatenated() {
        let b1 = int32_batch("v", vec![Some(1), Some(2)]);
        let b2 = int32_batch("v", vec![Some(3), Some(4)]);
        let vals = extract_numeric_values(&[b1, b2], "v");
        assert_eq!(vals, vec![1.0, 2.0, 3.0, 4.0]);
    }

    // ── calculate_ks_test (via detect_data_drift) ────────────────────────────

    #[test]
    fn test_ks_p_value_is_in_valid_range() {
        let b1 = int32_batch("v", vec![Some(1), Some(2), Some(3), Some(4), Some(5)]);
        let b2 = int32_batch("v", vec![Some(10), Some(20), Some(30), Some(40), Some(50)]);
        let drift = detect_data_drift(&[b1], &[b2]);
        let col = drift.column_drifts.get("v").unwrap();
        let p = col.ks_p_value.unwrap();
        assert!((0.0..=1.0).contains(&p), "p-value {p} outside [0, 1]");
    }

    #[test]
    fn test_ks_partial_overlap_statistic_between_0_and_1() {
        // Two distributions with partial overlap
        let b1 = int32_batch("v", vec![Some(1), Some(2), Some(3), Some(4), Some(5)]);
        let b2 = int32_batch("v", vec![Some(3), Some(4), Some(5), Some(6), Some(7)]);
        let drift = detect_data_drift(&[b1], &[b2]);
        let col = drift.column_drifts.get("v").unwrap();
        let ks = col.ks_statistic.unwrap();
        assert!(
            ks > 0.0 && ks < 1.0,
            "partial overlap ks={ks} should be in (0, 1)"
        );
    }

    #[test]
    fn test_ks_single_value_each_produces_result() {
        let b1 = int32_batch("v", vec![Some(1)]);
        let b2 = int32_batch("v", vec![Some(2)]);
        let drift = detect_data_drift(&[b1], &[b2]);
        let col = drift.column_drifts.get("v").unwrap();
        assert!(col.ks_statistic.is_some());
        assert!(col.ks_p_value.is_some());
    }

    #[test]
    fn test_ks_tied_values_not_overestimated() {
        // Regression: the merge-walk must advance past every copy of a tied value
        // before sampling the ECDF gap. source = four 1s, target = one 1 → the two
        // empirical distributions are identical (both a point mass at 1), so the
        // KS statistic is exactly 0, NOT 0.75.
        let b1 = int32_batch("v", vec![Some(1), Some(1), Some(1), Some(1)]);
        let b2 = int32_batch("v", vec![Some(1)]);
        let drift = detect_data_drift(&[b1], &[b2]);
        let col = drift.column_drifts.get("v").unwrap();
        assert_eq!(col.ks_statistic, Some(0.0));
    }

    #[test]
    fn test_ks_partial_tie_matches_textbook() {
        // source = [0, 1], target = [1, 1, 1, 2, 3].
        // F_src jumps to 0.5 at 0 and 1.0 at 1. F_tgt is 0 below 1, 0.6 at 1,
        // 0.8 at 2, 1.0 at 3. The sup gap is at value 0: |0.5 - 0| = 0.5.
        let b1 = int32_batch("v", vec![Some(0), Some(1)]);
        let b2 = int32_batch("v", vec![Some(1), Some(1), Some(1), Some(2), Some(3)]);
        let drift = detect_data_drift(&[b1], &[b2]);
        let col = drift.column_drifts.get("v").unwrap();
        assert!(
            (col.ks_statistic.unwrap() - 0.5).abs() < 1e-12,
            "expected 0.5, got {:?}",
            col.ks_statistic
        );
    }

    #[test]
    fn test_ks_p_value_is_one_for_identical_distributions() {
        // Regression: identical data ⇒ max_dist = 0 ⇒ p-value must be 1.0 (no
        // evidence of drift), not the previous buggy 0.0 (which read as certain
        // drift).
        let b1 = int32_batch("v", vec![Some(1), Some(2), Some(3), Some(4)]);
        let b2 = int32_batch("v", vec![Some(1), Some(2), Some(3), Some(4)]);
        let drift = detect_data_drift(&[b1], &[b2]);
        let col = drift.column_drifts.get("v").unwrap();
        assert_eq!(col.ks_statistic, Some(0.0));
        assert_eq!(col.ks_p_value, Some(1.0));
    }

    #[test]
    fn test_ks_empty_source_yields_none() {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, true)]));
        let empty = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(Vec::<Option<i32>>::new()))],
        )
        .unwrap();
        let non_empty = int32_batch("v", vec![Some(1), Some(2)]);
        let drift = detect_data_drift(&[empty], &[non_empty]);
        let col = drift.column_drifts.get("v").unwrap();
        // empty batch (0 rows) → ks undefined
        assert_eq!(col.ks_statistic, None);
        assert_eq!(col.ks_p_value, None);
    }

    // ── ColumnStats round-trip (JSON serialization) ──────────────────────────

    #[test]
    fn test_column_stats_json_round_trip() {
        let stats = ColumnStats {
            null_count: 3,
            row_count: 10,
            mean: Some(4.2),
            min: Some(0.0),
            max: Some(10.0),
            variance: Some(2.1),
        };
        let json = serde_json::to_string(&stats).unwrap();
        let back: ColumnStats = serde_json::from_str(&json).unwrap();
        assert_eq!(stats, back);
    }

    // ── detect_data_drift_from_stats ─────────────────────────────────────────

    #[test]
    fn test_detect_data_drift_from_stats() {
        use std::collections::HashMap;

        let mut source = HashMap::new();
        source.insert(
            "score".to_string(),
            ColumnStats {
                null_count: 0,
                row_count: 100,
                mean: Some(50.0),
                min: Some(0.0),
                max: Some(100.0),
                variance: None,
            },
        );
        source.insert(
            "extra".to_string(),
            ColumnStats {
                null_count: 0,
                row_count: 100,
                mean: Some(1.0),
                min: Some(1.0),
                max: Some(1.0),
                variance: None,
            },
        );

        let mut target = HashMap::new();
        target.insert(
            "score".to_string(),
            ColumnStats {
                null_count: 5,
                row_count: 100,
                mean: Some(60.0),
                min: Some(0.0),
                max: Some(100.0),
                variance: None,
            },
        );
        // 'extra' missing from target — should be skipped (intersection only)

        let drift = detect_data_drift_from_stats(&source, &target);
        assert_eq!(drift.column_drifts.len(), 1);
        let score = drift.column_drifts.get("score").unwrap();
        assert_eq!(score.mean_drift, Some(10.0));
        assert_eq!(score.null_count_drift, 5);
        assert_eq!(score.ks_statistic, None);
        assert_eq!(score.ks_p_value, None);
    }

    // ── PSI metric ───────────────────────────────────────────────────────────────

    #[test]
    fn test_psi_none_for_non_numeric() {
        let b1 = str_batch("name", vec![Some("a"), Some("b"), Some("c")]);
        let b2 = str_batch("name", vec![Some("d"), Some("e"), Some("f")]);
        let drift = detect_data_drift(&[b1], &[b2]);
        let col = drift.column_drifts.get("name").unwrap();
        assert_eq!(col.psi_statistic, None);
    }

    #[test]
    fn test_psi_none_in_stats_based_drift() {
        use std::collections::HashMap;

        let mut source = HashMap::new();
        source.insert(
            "x".to_string(),
            ColumnStats {
                null_count: 0,
                row_count: 5,
                mean: Some(2.0),
                min: Some(1.0),
                max: Some(3.0),
                variance: None,
            },
        );
        let mut target = HashMap::new();
        target.insert(
            "x".to_string(),
            ColumnStats {
                null_count: 0,
                row_count: 5,
                mean: Some(5.0),
                min: Some(4.0),
                max: Some(6.0),
                variance: None,
            },
        );
        let drift = detect_data_drift_from_stats(&source, &target);
        let col = drift.column_drifts.get("x").unwrap();
        assert_eq!(col.psi_statistic, None);
    }

    #[test]
    fn test_psi_zero_for_identical_distributions() {
        let b1 = int32_batch("val", vec![Some(1), Some(2), Some(3), Some(4), Some(5)]);
        let b2 = int32_batch("val", vec![Some(1), Some(2), Some(3), Some(4), Some(5)]);
        let drift = detect_data_drift(&[b1], &[b2]);
        let col = drift.column_drifts.get("val").unwrap();
        assert!(col.psi_statistic.is_some());
        assert!((col.psi_statistic.unwrap() - 0.0).abs() < 1e-10);
    }

    #[test]
    fn test_psi_positive_for_shifted_distributions() {
        let b1 = int32_batch("val", vec![Some(1), Some(2), Some(3), Some(4), Some(5)]);
        let b2 = int32_batch("val", vec![Some(6), Some(7), Some(8), Some(9), Some(10)]);
        let drift = detect_data_drift(&[b1], &[b2]);
        let col = drift.column_drifts.get("val").unwrap();
        assert!(col.psi_statistic.is_some());
        assert!(col.psi_statistic.unwrap() > 0.1);
    }

    // ── categorical drift (string-column PSI) ───────────────────────────────

    #[test]
    fn test_categorical_drift_none_for_numeric_column() {
        let b1 = int32_batch("val", vec![Some(1), Some(2), Some(3)]);
        let b2 = int32_batch("val", vec![Some(1), Some(2), Some(3)]);
        let drift = detect_data_drift(&[b1], &[b2]);
        let col = drift.column_drifts.get("val").unwrap();
        assert_eq!(col.categorical_drift, None);
    }

    #[test]
    fn test_categorical_drift_zero_for_identical_frequencies() {
        let b1 = str_batch(
            "status",
            vec![Some("active"), Some("active"), Some("inactive")],
        );
        let b2 = str_batch(
            "status",
            vec![Some("active"), Some("active"), Some("inactive")],
        );
        let drift = detect_data_drift(&[b1], &[b2]);
        let col = drift.column_drifts.get("status").unwrap();
        let psi = col
            .categorical_drift
            .expect("string column should get categorical_drift");
        assert!(psi.abs() < 1e-9, "expected ~0 PSI, got {psi}");
    }

    #[test]
    fn test_categorical_drift_detects_frequency_flip() {
        // A category shifting from 90% active / 10% inactive to the reverse
        // is invisible to numeric PSI/KS (there's no numeric column here) and
        // to detect_semantic_drift (both sides are still SemanticType::Unknown
        // plain strings) — categorical_drift must be the signal that catches it.
        let mut source_vals = vec![Some("active"); 9];
        source_vals.push(Some("inactive"));
        let mut target_vals = vec![Some("inactive"); 9];
        target_vals.push(Some("active"));

        let b1 = str_batch("status", source_vals);
        let b2 = str_batch("status", target_vals);
        let drift = detect_data_drift(&[b1], &[b2]);
        let col = drift.column_drifts.get("status").unwrap();
        let psi = col
            .categorical_drift
            .expect("string column should get categorical_drift");
        assert!(psi > 0.25, "expected a major shift, got psi={psi}");
        assert!(col.breaches_threshold(0.25));
    }

    #[test]
    fn test_categorical_drift_aggregates_across_batches() {
        let b1a = str_batch("status", vec![Some("active"); 5]);
        let b1b = str_batch("status", vec![Some("inactive"); 5]);
        let b2 = str_batch(
            "status",
            vec![Some("active"), Some("active"), Some("inactive")],
        );
        let drift = detect_data_drift(&[b1a, b1b], &[b2]);
        let col = drift.column_drifts.get("status").unwrap();
        // source is 50/50 active/inactive across its two batches; target is
        // ~67/33 — a real but moderate shift, not the ~0 a single-batch read
        // (either all-active or all-inactive) would report.
        assert!(col.categorical_drift.is_some());
    }

    // ── quantile binning ─────────────────────────────────────────────────────

    #[test]
    fn test_quantile_sorted_interpolates() {
        let s = [0.0, 10.0, 20.0, 30.0, 40.0]; // n = 5
        assert_eq!(quantile_sorted(&s, 0.0), 0.0);
        assert_eq!(quantile_sorted(&s, 1.0), 40.0);
        assert_eq!(quantile_sorted(&s, 0.5), 20.0); // median = middle element
        // 0.25 * (5-1) = pos 1.0 → exactly the second element.
        assert_eq!(quantile_sorted(&s, 0.25), 10.0);
        // 0.125 * 4 = pos 0.5 → halfway between 0 and 10.
        assert_eq!(quantile_sorted(&s, 0.125), 5.0);
    }

    #[test]
    fn test_quantile_sorted_single_element() {
        assert_eq!(quantile_sorted(&[7.0], 0.0), 7.0);
        assert_eq!(quantile_sorted(&[7.0], 0.9), 7.0);
    }

    #[test]
    fn test_psi_quantile_identical_distributions_is_zero() {
        // Reference deciles bin its own data evenly; identical target ⇒ PSI ~0.
        let vals: Vec<f64> = (0..100).map(|i| i as f64).collect();
        let psi = psi_from_samples(&vals, &vals).unwrap();
        assert!(
            psi.abs() < 1e-9,
            "identical dists should give ~0 PSI, got {psi}"
        );
    }

    #[test]
    fn test_psi_quantile_detects_shift_in_dense_region_of_skewed_data() {
        // Heavily right-skewed reference: most mass in [0, 10], a long thin tail
        // out to 1000. The target keeps the same tail but shifts the *dense*
        // bulk from ~[0,10] up to ~[10,20]. Equal-width bins (width ~100) put the
        // entire bulk in bin 0 for both samples and miss this; quantile bins,
        // cut at the reference's deciles (which are all down in the dense
        // region), resolve the shift.
        let mut reference = Vec::new();
        for i in 0..90 {
            reference.push((i % 10) as f64); // bulk in [0, 9]
        }
        for i in 0..10 {
            reference.push(100.0 + i as f64 * 90.0); // sparse tail up to ~910
        }
        let mut current = Vec::new();
        for i in 0..90 {
            current.push(10.0 + (i % 10) as f64); // bulk shifted to [10, 19]
        }
        for i in 0..10 {
            current.push(100.0 + i as f64 * 90.0); // same tail
        }

        let psi = psi_from_samples(&reference, &current).unwrap();
        assert!(
            psi > 0.25,
            "quantile PSI should flag a major shift in the dense region, got {psi}"
        );
    }

    // ── NaN handling ─────────────────────────────────────────────────────────

    #[test]
    fn test_nan_values_are_skipped_in_stats() {
        // A NaN in a float column must be ignored like a null, not poison the mean.
        let batch = f64_batch("v", vec![Some(1.0), Some(f64::NAN), Some(3.0)]);
        let stats = calculate_column_stats(&[batch], "v").unwrap();
        assert_eq!(stats.mean, Some(2.0)); // (1+3)/2, NaN excluded
        assert_eq!(stats.min, Some(1.0));
        assert_eq!(stats.max, Some(3.0));
    }

    #[test]
    fn test_extract_numeric_values_skips_nan() {
        let batch = f64_batch("v", vec![Some(1.0), Some(f64::NAN), Some(2.0)]);
        let vals = extract_numeric_values(&[batch], "v");
        assert_eq!(vals, vec![1.0, 2.0]);
    }

    #[test]
    fn test_psi_finite_with_nan_present() {
        // Regression: a NaN used to collapse every value into bin 0, distorting
        // PSI. With NaN skipped, identical (NaN-laced) distributions give PSI 0.
        let b1 = f64_batch("v", vec![Some(1.0), Some(2.0), Some(f64::NAN), Some(3.0)]);
        let b2 = f64_batch("v", vec![Some(1.0), Some(2.0), Some(3.0)]);
        let drift = detect_data_drift(&[b1], &[b2]);
        let col = drift.column_drifts.get("v").unwrap();
        let psi = col.psi_statistic.unwrap();
        assert!(psi.is_finite());
        assert!((psi - 0.0).abs() < 1e-9, "expected ~0 PSI, got {psi}");
    }

    #[test]
    fn test_psi_from_samples_all_nan_is_none() {
        // Public entry point handed a degenerate all-NaN slice must not fabricate
        // a PSI: the range is non-finite, so the metric is undefined.
        let nans = vec![f64::NAN, f64::NAN];
        assert_eq!(psi_from_samples(&nans, &[1.0, 2.0]), None);
    }

    // ── unsigned & 8-bit integer support ─────────────────────────────────────

    #[test]
    fn test_uint32_column_participates_in_drift() {
        use arrow::array::UInt32Array;
        let schema = Arc::new(Schema::new(vec![Field::new("u", DataType::UInt32, true)]));
        let src = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(UInt32Array::from(vec![1u32, 2, 3, 4, 5]))],
        )
        .unwrap();
        let tgt = RecordBatch::try_new(
            schema,
            vec![Arc::new(UInt32Array::from(vec![6u32, 7, 8, 9, 10]))],
        )
        .unwrap();
        let drift = detect_data_drift(&[src], &[tgt]);
        let col = drift.column_drifts.get("u").unwrap();
        assert_eq!(col.source_stats.mean, Some(3.0));
        assert!(col.ks_statistic.is_some());
        assert!(col.psi_statistic.is_some());
    }

    #[test]
    fn test_int8_and_uint64_supported_numeric() {
        assert!(is_supported_numeric(&DataType::Int8));
        assert!(is_supported_numeric(&DataType::UInt8));
        assert!(is_supported_numeric(&DataType::UInt16));
        assert!(is_supported_numeric(&DataType::UInt32));
        assert!(is_supported_numeric(&DataType::UInt64));
        assert!(!is_supported_numeric(&DataType::Utf8));
    }
}
