use std::collections::HashMap;
use std::path::Path;

use arrow::datatypes::{DataType, Field, Schema};
use serde::{Deserialize, Serialize};

use crate::drift::{ColumnStats, calculate_column_stats, extract_numeric_values};
use crate::semantic::{SemanticType, infer_semantic_type};
use crate::{DataProvider, Result, SagoError};

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct SerializableSchema {
    pub fields: Vec<SerializableField>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct SerializableField {
    pub name: String,
    pub data_type: String,
    pub nullable: bool,
}

impl From<&Schema> for SerializableSchema {
    fn from(schema: &Schema) -> Self {
        SerializableSchema {
            fields: schema
                .fields()
                .iter()
                .map(|f| SerializableField {
                    name: f.name().clone(),
                    data_type: format!("{:?}", f.data_type()),
                    nullable: f.is_nullable(),
                })
                .collect(),
        }
    }
}

impl SerializableSchema {
    pub fn to_arrow_schema(&self) -> Result<Schema> {
        let fields: Result<Vec<Field>> = self
            .fields
            .iter()
            .map(|f| {
                let dt = parse_data_type(&f.data_type)?;
                Ok(Field::new(&f.name, dt, f.nullable))
            })
            .collect();
        Ok(Schema::new(fields?))
    }
}

fn parse_data_type(s: &str) -> Result<DataType> {
    // Supports the types produced by PostgresSchemaProvider and Parquet primitives
    // we round-trip today. Extend as new types are needed.
    match s {
        "Boolean" => Ok(DataType::Boolean),
        "Int16" => Ok(DataType::Int16),
        "Int32" => Ok(DataType::Int32),
        "Int64" => Ok(DataType::Int64),
        "Float32" => Ok(DataType::Float32),
        "Float64" => Ok(DataType::Float64),
        "Utf8" => Ok(DataType::Utf8),
        "LargeUtf8" => Ok(DataType::LargeUtf8),
        "Binary" => Ok(DataType::Binary),
        "Date32" => Ok(DataType::Date32),
        "Timestamp(Nanosecond, None)" => Ok(DataType::Timestamp(
            arrow::datatypes::TimeUnit::Nanosecond,
            None,
        )),
        other if other.starts_with("Timestamp(Nanosecond, Some(") => {
            // Extract timezone from debug repr: Timestamp(Nanosecond, Some("tz"))
            let tz = other
                .trim_start_matches("Timestamp(Nanosecond, Some(\"")
                .trim_end_matches("\"))")
                .to_string();
            Ok(DataType::Timestamp(
                arrow::datatypes::TimeUnit::Nanosecond,
                Some(tz.into()),
            ))
        }
        other => Err(SagoError::Schema(format!(
            "unsupported serialized data type: {}",
            other
        ))),
    }
}

const CURRENT_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct ProjectState {
    pub schema_version: u32,
    pub snapshots: HashMap<String, TargetSnapshot>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct TargetSnapshot {
    pub captured_at: String,
    pub schema: SerializableSchema,
    pub column_stats: HashMap<String, ColumnStats>,
    pub semantic_types: HashMap<String, SemanticType>,
    pub samples: Option<HashMap<String, Vec<f64>>>,
}

impl ProjectState {
    pub fn empty() -> Self {
        ProjectState {
            schema_version: CURRENT_SCHEMA_VERSION,
            snapshots: HashMap::new(),
        }
    }

    pub fn load(path: &Path) -> Result<Self> {
        let bytes = std::fs::read(path).map_err(SagoError::Io)?;
        let state: ProjectState =
            serde_json::from_slice(&bytes).map_err(|e| SagoError::Config(e.to_string()))?;
        if state.schema_version != CURRENT_SCHEMA_VERSION {
            return Err(SagoError::Config(format!(
                "unsupported state schema_version: {} (expected {})",
                state.schema_version, CURRENT_SCHEMA_VERSION
            )));
        }
        Ok(state)
    }

    pub fn load_or_default(path: &Path) -> Result<Self> {
        match std::fs::read(path) {
            Ok(bytes) => {
                let state: ProjectState =
                    serde_json::from_slice(&bytes).map_err(|e| SagoError::Config(e.to_string()))?;
                if state.schema_version != CURRENT_SCHEMA_VERSION {
                    return Err(SagoError::Config(format!(
                        "unsupported state schema_version: {} (expected {})",
                        state.schema_version, CURRENT_SCHEMA_VERSION
                    )));
                }
                Ok(state)
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Self::empty()),
            Err(e) => Err(SagoError::Io(e)),
        }
    }

    pub fn save(&self, path: &Path) -> Result<()> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(SagoError::Io)?;
        }
        let json =
            serde_json::to_string_pretty(self).map_err(|e| SagoError::Config(e.to_string()))?;
        std::fs::write(path, json).map_err(SagoError::Io)?;
        Ok(())
    }
}

/// Capture a snapshot of `identifier` from `provider`.
///
/// `sample_n` is `Some(n)` to retain up to `n` numeric values per numeric column,
/// `None` to skip sample persistence.
pub async fn capture_snapshot(
    provider: std::sync::Arc<dyn DataProvider>,
    identifier: &str,
    sample_n: Option<usize>,
) -> Result<TargetSnapshot> {
    let batches = provider.get_data(identifier).await?;
    let schema = if let Some(b) = batches.first() {
        b.schema().as_ref().clone()
    } else {
        provider.get_schema(identifier).await?
    };

    let mut column_stats = HashMap::new();
    let mut semantic_types = HashMap::new();
    for field in schema.fields() {
        if let Some(stats) = calculate_column_stats(&batches, field.name()) {
            column_stats.insert(field.name().clone(), stats);
        }
        if let Some(b) = batches.first()
            && let Some(col) = b.column_by_name(field.name())
        {
            semantic_types.insert(
                field.name().clone(),
                infer_semantic_type(field.name(), col.as_ref()),
            );
        }
    }

    let samples = match sample_n {
        Some(n) if n > 0 => Some(extract_samples(&batches, &schema, n)),
        _ => None,
    };

    Ok(TargetSnapshot {
        captured_at: chrono::Utc::now().to_rfc3339(),
        schema: SerializableSchema::from(&schema),
        column_stats,
        semantic_types,
        samples,
    })
}

fn extract_samples(
    batches: &[arrow::record_batch::RecordBatch],
    schema: &Schema,
    n: usize,
) -> HashMap<String, Vec<f64>> {
    let mut out = HashMap::new();
    for field in schema.fields() {
        if matches!(
            field.data_type(),
            DataType::Int16
                | DataType::Int32
                | DataType::Int64
                | DataType::Float32
                | DataType::Float64
        ) {
            let mut values = extract_numeric_values(batches, field.name());
            if values.len() > n {
                values.truncate(n);
            }
            out.insert(field.name().clone(), values);
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_schema_round_trip_basic() {
        let original = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("email", DataType::Utf8, true),
            Field::new("active", DataType::Boolean, false),
            Field::new(
                "created_at",
                DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None),
                true,
            ),
            Field::new(
                "updated_at",
                DataType::Timestamp(
                    arrow::datatypes::TimeUnit::Nanosecond,
                    Some("+00:00".into()),
                ),
                false,
            ),
        ]);
        let s: SerializableSchema = (&original).into();
        let json = serde_json::to_string(&s).unwrap();
        let parsed: SerializableSchema = serde_json::from_str(&json).unwrap();
        let restored = parsed.to_arrow_schema().unwrap();

        assert_eq!(restored.fields().len(), 5);
        assert_eq!(restored.field(0).name(), "id");
        assert_eq!(restored.field(0).data_type(), &DataType::Int64);
        assert_eq!(restored.field(1).data_type(), &DataType::Utf8);
        assert!(!restored.field(2).is_nullable());
        assert_eq!(
            restored.field(3).data_type(),
            &DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None)
        );
        assert_eq!(
            restored.field(4).data_type(),
            &DataType::Timestamp(
                arrow::datatypes::TimeUnit::Nanosecond,
                Some("+00:00".into())
            )
        );
    }

    #[test]
    fn test_schema_unsupported_type_errors() {
        let s = SerializableSchema {
            fields: vec![SerializableField {
                name: "x".into(),
                data_type: "List(Int32)".into(),
                nullable: false,
            }],
        };
        let err = s.to_arrow_schema().unwrap_err();
        match err {
            SagoError::Schema(msg) => assert!(msg.contains("unsupported")),
            other => panic!("expected Schema error, got {:?}", other),
        }
    }

    #[test]
    fn test_project_state_round_trip() {
        let mut snapshots = HashMap::new();
        let mut column_stats = HashMap::new();
        column_stats.insert(
            "id".to_string(),
            ColumnStats {
                null_count: 0,
                row_count: 100,
                mean: Some(50.0),
                min: Some(1.0),
                max: Some(100.0),
            },
        );
        let mut semantic_types = HashMap::new();
        semantic_types.insert("email".to_string(), SemanticType::Email);

        snapshots.insert(
            "users".to_string(),
            TargetSnapshot {
                captured_at: "2026-05-09T14:00:00Z".into(),
                schema: SerializableSchema { fields: vec![] },
                column_stats,
                semantic_types,
                samples: None,
            },
        );

        let state = ProjectState {
            schema_version: 1,
            snapshots,
        };

        let json = serde_json::to_string_pretty(&state).unwrap();
        let back: ProjectState = serde_json::from_str(&json).unwrap();
        assert_eq!(back.schema_version, 1);
        assert_eq!(back.snapshots.len(), 1);
        assert_eq!(back.snapshots["users"].captured_at, "2026-05-09T14:00:00Z");
    }

    #[test]
    fn test_load_save_via_tempdir() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("state.json");

        let state = ProjectState {
            schema_version: 1,
            snapshots: HashMap::new(),
        };
        state.save(&path).unwrap();

        let loaded = ProjectState::load(&path).unwrap();
        assert_eq!(loaded.schema_version, 1);
        assert!(loaded.snapshots.is_empty());
    }

    #[test]
    fn test_load_missing_file_returns_default() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("nope.json");
        let state = ProjectState::load_or_default(&path).unwrap();
        assert_eq!(state.schema_version, 1);
        assert!(state.snapshots.is_empty());
    }

    #[test]
    fn test_load_unknown_schema_version_errors() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("state.json");
        std::fs::write(&path, r#"{"schema_version": 99, "snapshots": {}}"#).unwrap();
        let err = ProjectState::load(&path).unwrap_err();
        match err {
            SagoError::Config(msg) => assert!(msg.contains("schema_version")),
            other => panic!("expected Config error, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_capture_snapshot_with_mock_provider() {
        use crate::{DataProvider, SchemaProvider};
        use arrow::array::{Int32Array, StringArray};
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use async_trait::async_trait;
        use std::sync::Arc;

        struct Mock {
            schema: Schema,
            batch: RecordBatch,
        }

        #[async_trait]
        impl SchemaProvider for Mock {
            async fn get_schema(&self, _: &str) -> Result<Schema> {
                Ok(self.schema.clone())
            }
        }

        #[async_trait]
        impl DataProvider for Mock {
            async fn get_data(&self, _: &str) -> Result<Vec<RecordBatch>> {
                Ok(vec![self.batch.clone()])
            }
        }

        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("email", DataType::Utf8, true),
        ]);
        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5])),
                Arc::new(StringArray::from(vec![
                    Some("a@x.com"),
                    Some("b@x.com"),
                    Some("c@x.com"),
                    Some("d@x.com"),
                    Some("e@x.com"),
                ])),
            ],
        )
        .unwrap();
        let provider: Arc<dyn DataProvider> = Arc::new(Mock { schema, batch });

        let snap = capture_snapshot(provider, "tbl", None).await.unwrap();
        assert_eq!(snap.schema.fields.len(), 2);
        assert!(snap.column_stats.contains_key("id"));
        assert_eq!(snap.semantic_types["email"], SemanticType::Email);
        assert!(snap.samples.is_none());
    }

    #[tokio::test]
    async fn test_capture_snapshot_with_samples() {
        use crate::{DataProvider, SchemaProvider};
        use arrow::array::Int32Array;
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use async_trait::async_trait;
        use std::sync::Arc;

        struct Mock {
            schema: Schema,
            batch: RecordBatch,
        }
        #[async_trait]
        impl SchemaProvider for Mock {
            async fn get_schema(&self, _: &str) -> Result<Schema> {
                Ok(self.schema.clone())
            }
        }
        #[async_trait]
        impl DataProvider for Mock {
            async fn get_data(&self, _: &str) -> Result<Vec<RecordBatch>> {
                Ok(vec![self.batch.clone()])
            }
        }

        let schema = Schema::new(vec![Field::new("v", DataType::Int32, false)]);
        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![Arc::new(Int32Array::from((0..10).collect::<Vec<i32>>()))],
        )
        .unwrap();
        let provider: Arc<dyn DataProvider> = Arc::new(Mock { schema, batch });

        let snap = capture_snapshot(provider, "tbl", Some(5)).await.unwrap();
        let samples = snap.samples.unwrap();
        assert!(samples.contains_key("v"));
        assert_eq!(samples["v"].len(), 5);
    }
}
