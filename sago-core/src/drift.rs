use arrow::array::{Array, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array};
use arrow::datatypes::{DataType, Schema};
use arrow::record_batch::RecordBatch;
use serde::Serialize;
use std::collections::{HashMap, HashSet};
use crate::semantic::{infer_semantic_type, SemanticType};

#[derive(Debug, Serialize, PartialEq)]
pub struct SchemaDrift {
    pub added_fields: Vec<String>,
    pub removed_fields: Vec<String>,
    pub changed_types: Vec<TypeChange>,
    pub semantic_drifts: Vec<SemanticDrift>,
}

#[derive(Debug, Serialize, PartialEq)]
pub struct SemanticDrift {
    pub field_name: String,
    pub source_type: SemanticType,
    pub target_type: SemanticType,
}

#[derive(Debug, Serialize, PartialEq)]
pub struct TypeChange {
    pub field_name: String,
    pub old_type: String,
    pub new_type: String,
}

#[derive(Debug, Serialize, PartialEq, Clone)]
pub struct ColumnStats {
    pub null_count: usize,
    pub row_count: usize,
    pub mean: Option<f64>,
    pub min: Option<f64>,
    pub max: Option<f64>,
}

#[derive(Debug, Serialize, PartialEq)]
pub struct DataDrift {
    pub column_drifts: HashMap<String, ColumnDrift>,
}

#[derive(Debug, Serialize, PartialEq)]
pub struct ColumnDrift {
    pub source_stats: ColumnStats,
    pub target_stats: ColumnStats,
    pub mean_drift: Option<f64>,
    pub null_count_drift: i64,
    pub ks_statistic: Option<f64>,
    pub ks_p_value: Option<f64>,
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
                old_type: format!("{:?}", source_field.data_type()),
                new_type: format!("{:?}", target_field.data_type()),
            });
        }
    }

    SchemaDrift {
        added_fields,
        removed_fields,
        changed_types,
        semantic_drifts: Vec::new(),
    }
}

pub fn detect_semantic_drift(source_batches: &[RecordBatch], target_batches: &[RecordBatch]) -> Vec<SemanticDrift> {
    let mut semantic_drifts = Vec::new();

    if source_batches.is_empty() || target_batches.is_empty() {
        return semantic_drifts;
    }

    let source_schema = source_batches[0].schema();
    let target_schema = target_batches[0].schema();

    let source_fields: HashSet<_> = source_schema.fields().iter().map(|f| f.name().clone()).collect();
    let target_fields: HashSet<_> = target_schema.fields().iter().map(|f| f.name().clone()).collect();

    for field_name in source_fields.intersection(&target_fields) {
        let source_col = source_batches[0].column_by_name(field_name).unwrap();
        let target_col = target_batches[0].column_by_name(field_name).unwrap();

        let source_semantic = infer_semantic_type(field_name, source_col.as_ref());
        let target_semantic = infer_semantic_type(field_name, target_col.as_ref());

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

pub fn calculate_column_stats(batches: &[RecordBatch], column_name: &str) -> Option<ColumnStats> {
    if batches.is_empty() {
        return None;
    }

    let mut null_count = 0;
    let mut row_count = 0;
    let mut sum = 0.0;
    let mut min = f64::MAX;
    let mut max = f64::MIN;
    let mut has_numeric = false;
    let mut numeric_count = 0;

    for batch in batches {
        let column = batch.column_by_name(column_name)?;
        null_count += column.null_count();
        row_count += batch.num_rows();

        match column.data_type() {
            DataType::Int16 | DataType::Int32 | DataType::Int64 | DataType::Float32 | DataType::Float64 => {
                has_numeric = true;
                for i in 0..column.len() {
                    if !column.is_null(i) {
                        let val = match column.data_type() {
                            DataType::Int16 => column.as_any().downcast_ref::<Int16Array>().unwrap().value(i) as f64,
                            DataType::Int32 => column.as_any().downcast_ref::<Int32Array>().unwrap().value(i) as f64,
                            DataType::Int64 => column.as_any().downcast_ref::<Int64Array>().unwrap().value(i) as f64,
                            DataType::Float32 => column.as_any().downcast_ref::<Float32Array>().unwrap().value(i) as f64,
                            DataType::Float64 => column.as_any().downcast_ref::<Float64Array>().unwrap().value(i),
                            _ => unreachable!(),
                        };
                        sum += val;
                        numeric_count += 1;
                        if val < min { min = val; }
                        if val > max { max = val; }
                    }
                }
            }
            _ => {}
        }
    }

    Some(ColumnStats {
        null_count,
        row_count,
        mean: if has_numeric && numeric_count > 0 { Some(sum / numeric_count as f64) } else { None },
        min: if has_numeric && numeric_count > 0 { Some(min) } else { None },
        max: if has_numeric && numeric_count > 0 { Some(max) } else { None },
    })
}

pub fn detect_data_drift(source_batches: &[RecordBatch], target_batches: &[RecordBatch]) -> DataDrift {
    let mut column_drifts = HashMap::new();

    if source_batches.is_empty() || target_batches.is_empty() {
        return DataDrift { column_drifts };
    }

    let source_schema = source_batches[0].schema();
    let target_schema = target_batches[0].schema();

    let source_fields: HashSet<_> = source_schema.fields().iter().map(|f| f.name().clone()).collect();
    let target_fields: HashSet<_> = target_schema.fields().iter().map(|f| f.name().clone()).collect();

    for field_name in source_fields.intersection(&target_fields) {
        let source_stats = calculate_column_stats(source_batches, field_name).unwrap();
        let target_stats = calculate_column_stats(target_batches, field_name).unwrap();

        let mean_drift = if let (Some(s_mean), Some(t_mean)) = (source_stats.mean, target_stats.mean) {
            Some((t_mean - s_mean).abs())
        } else {
            None
        };

        let null_count_drift = target_stats.null_count as i64 - source_stats.null_count as i64;

        let (ks_statistic, ks_p_value) = calculate_ks_test(source_batches, target_batches, field_name);

        column_drifts.insert(field_name.clone(), ColumnDrift {
            source_stats,
            target_stats,
            mean_drift,
            null_count_drift,
            ks_statistic,
            ks_p_value,
        });
    }

    DataDrift { column_drifts }
}

fn extract_numeric_values(batches: &[RecordBatch], column_name: &str) -> Vec<f64> {
    let mut values = Vec::new();
    for batch in batches {
        if let Some(column) = batch.column_by_name(column_name) {
            match column.data_type() {
                DataType::Int16 => {
                    let arr = column.as_any().downcast_ref::<Int16Array>().unwrap();
                    for i in 0..arr.len() {
                        if !arr.is_null(i) { values.push(arr.value(i) as f64); }
                    }
                }
                DataType::Int32 => {
                    let arr = column.as_any().downcast_ref::<Int32Array>().unwrap();
                    for i in 0..arr.len() {
                        if !arr.is_null(i) { values.push(arr.value(i) as f64); }
                    }
                }
                DataType::Int64 => {
                    let arr = column.as_any().downcast_ref::<Int64Array>().unwrap();
                    for i in 0..arr.len() {
                        if !arr.is_null(i) { values.push(arr.value(i) as f64); }
                    }
                }
                DataType::Float32 => {
                    let arr = column.as_any().downcast_ref::<Float32Array>().unwrap();
                    for i in 0..arr.len() {
                        if !arr.is_null(i) { values.push(arr.value(i) as f64); }
                    }
                }
                DataType::Float64 => {
                    let arr = column.as_any().downcast_ref::<Float64Array>().unwrap();
                    for i in 0..arr.len() {
                        if !arr.is_null(i) { values.push(arr.value(i)); }
                    }
                }
                _ => {}
            }
        }
    }
    values
}

fn calculate_ks_test(source_batches: &[RecordBatch], target_batches: &[RecordBatch], column_name: &str) -> (Option<f64>, Option<f64>) {
    let mut source_vals = extract_numeric_values(source_batches, column_name);
    let mut target_vals = extract_numeric_values(target_batches, column_name);

    if source_vals.is_empty() || target_vals.is_empty() {
        return (None, None);
    }

    // Sort the arrays to compute the Empirical CDF
    source_vals.sort_by(|a, b| a.total_cmp(b));
    target_vals.sort_by(|a, b| a.total_cmp(b));

    let n1 = source_vals.len() as f64;
    let n2 = target_vals.len() as f64;

    let mut max_dist = 0.0;
    let mut i = 0;
    let mut j = 0;

    while i < source_vals.len() && j < target_vals.len() {
        let val1 = source_vals[i];
        let val2 = target_vals[j];

        if val1 <= val2 {
            i += 1;
        }
        if val2 <= val1 {
            j += 1;
        }

        let cdf1 = i as f64 / n1;
        let cdf2 = j as f64 / n2;
        let dist = (cdf1 - cdf2).abs();

        if dist > max_dist {
            max_dist = dist;
        }
    }

    // Very basic KS p-value approximation for 2-sample test
    let en = (n1 * n2) / (n1 + n2);
    let lambda = (en.sqrt() + 0.12 + 0.11 / en.sqrt()) * max_dist;

    // Using asymptotic formula for Kolmogorov distribution
    let mut p_value = 0.0;
    if lambda > 0.0 {
        let mut sum = 0.0;
        for k in 1..=100 {
            let sign = if k % 2 == 0 { -1.0 } else { 1.0 };
            sum += sign * (-2.0 * (k as f64 * lambda).powi(2)).exp();
        }
        p_value = 2.0 * sum;
        if p_value < 0.0 { p_value = 0.0; }
        if p_value > 1.0 { p_value = 1.0; }
    }

    (Some(max_dist), Some(p_value))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field};
    use std::sync::Arc;

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
    fn test_detect_data_drift() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("val", DataType::Int32, true),
        ]));

        let source_batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![Some(1), Some(2), Some(3)]))],
        ).unwrap();

        let target_batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![Some(10), Some(20), None]))],
        ).unwrap();

        let drift = detect_data_drift(&[source_batch], &[target_batch]);
        
        let val_drift = drift.column_drifts.get("val").unwrap();
        assert_eq!(val_drift.source_stats.mean, Some(2.0));
        assert_eq!(val_drift.target_stats.mean, Some(15.0));
        assert_eq!(val_drift.mean_drift, Some(13.0));
        assert_eq!(val_drift.null_count_drift, 1);
        assert!(val_drift.ks_statistic.is_some());
        assert!(val_drift.ks_p_value.is_some());
    }
}
