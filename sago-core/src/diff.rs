use crate::{DataProvider, Result};
use crate::drift::{detect_data_drift, detect_schema_drift, detect_semantic_drift, DataDrift, SchemaDrift, SemanticDrift};
use serde::Serialize;
use std::sync::Arc;

#[derive(Debug, Serialize, PartialEq)]
pub struct DiffReport {
    pub source_identifier: String,
    pub target_identifier: String,
    pub schema_drift: SchemaDrift,
    pub semantic_drifts: Vec<SemanticDrift>,
    pub data_drift: DataDrift,
}

pub async fn diff_datasets(
    source_provider: Arc<dyn DataProvider>,
    source_identifier: &str,
    target_provider: Arc<dyn DataProvider>,
    target_identifier: &str,
) -> Result<DiffReport> {
    let source_schema = source_provider.get_schema(source_identifier).await?;
    let target_schema = target_provider.get_schema(target_identifier).await?;

    let schema_drift = detect_schema_drift(&source_schema, &target_schema);

    let source_data = source_provider.get_data(source_identifier).await?;
    let target_data = target_provider.get_data(target_identifier).await?;

    let semantic_drifts = detect_semantic_drift(&source_data, &target_data);
    let data_drift = detect_data_drift(&source_data, &target_data);

    Ok(DiffReport {
        source_identifier: source_identifier.to_string(),
        target_identifier: target_identifier.to_string(),
        schema_drift,
        semantic_drifts,
        data_drift,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, Int32Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use async_trait::async_trait;
    use crate::SagoError;
    use std::sync::Arc;

    // ── Mock provider ────────────────────────────────────────────────────────

    struct MockDataProvider {
        schema: Schema,
        batches: Vec<RecordBatch>,
    }

    impl MockDataProvider {
        fn new(schema: Schema, batches: Vec<RecordBatch>) -> Arc<Self> {
            Arc::new(Self { schema, batches })
        }
    }

    #[async_trait]
    impl crate::SchemaProvider for MockDataProvider {
        async fn get_schema(&self, _identifier: &str) -> crate::Result<Schema> {
            Ok(self.schema.clone())
        }
    }

    #[async_trait]
    impl DataProvider for MockDataProvider {
        async fn get_data(&self, _identifier: &str) -> crate::Result<Vec<RecordBatch>> {
            Ok(self.batches.clone())
        }
    }

    struct ErrorProvider;

    #[async_trait]
    impl crate::SchemaProvider for ErrorProvider {
        async fn get_schema(&self, _identifier: &str) -> crate::Result<Schema> {
            Err(SagoError::Schema("table not found".to_string()))
        }
    }

    #[async_trait]
    impl DataProvider for ErrorProvider {
        async fn get_data(&self, _identifier: &str) -> crate::Result<Vec<RecordBatch>> {
            unreachable!()
        }
    }

    // ── helpers ──────────────────────────────────────────────────────────────

    fn int32_schema(name: &str) -> Schema {
        Schema::new(vec![Field::new(name, DataType::Int32, true)])
    }

    fn int32_batch(schema: Arc<Schema>, values: Vec<Option<i32>>) -> RecordBatch {
        RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(values))]).unwrap()
    }

    // ── tests ────────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn test_diff_identical_datasets() {
        let schema = Arc::new(int32_schema("val"));
        let batch = int32_batch(schema.clone(), vec![Some(1), Some(2), Some(3)]);

        let provider = MockDataProvider::new(schema.as_ref().clone(), vec![batch]);

        let report = diff_datasets(
            provider.clone(),
            "source_table",
            provider.clone(),
            "target_table",
        )
        .await
        .unwrap();

        assert!(report.schema_drift.added_fields.is_empty());
        assert!(report.schema_drift.removed_fields.is_empty());
        assert!(report.schema_drift.changed_types.is_empty());
        assert!(report.semantic_drifts.is_empty());

        let col = report.data_drift.column_drifts.get("val").unwrap();
        assert_eq!(col.mean_drift, Some(0.0));
        assert_eq!(col.null_count_drift, 0);
    }

    #[tokio::test]
    async fn test_diff_schema_drift() {
        let source_schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]);
        let target_schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false), // type change
            Field::new("email", DataType::Utf8, true), // added; "name" removed
        ]);

        let source_batch = RecordBatch::try_new(
            Arc::new(source_schema.clone()),
            vec![
                Arc::new(Int32Array::from(vec![1i32])) as Arc<dyn Array>,
                Arc::new(StringArray::from(vec!["alice"])) as Arc<dyn Array>,
            ],
        )
        .unwrap();
        let target_batch = RecordBatch::try_new(
            Arc::new(target_schema.clone()),
            vec![
                Arc::new(Int64Array::from(vec![1i64])) as Arc<dyn Array>,
                Arc::new(StringArray::from(vec!["a@x.com"])) as Arc<dyn Array>,
            ],
        )
        .unwrap();

        let source = MockDataProvider::new(source_schema, vec![source_batch]);
        let target = MockDataProvider::new(target_schema, vec![target_batch]);

        let report = diff_datasets(source, "s", target, "t").await.unwrap();

        assert!(report.schema_drift.added_fields.contains(&"email".to_string()));
        assert!(report.schema_drift.removed_fields.contains(&"name".to_string()));
        assert_eq!(report.schema_drift.changed_types.len(), 1);
        assert_eq!(report.schema_drift.changed_types[0].field_name, "id");
        assert_eq!(report.source_identifier, "s");
        assert_eq!(report.target_identifier, "t");
    }

    #[tokio::test]
    async fn test_diff_semantic_drift() {
        let schema = Schema::new(vec![Field::new("contact", DataType::Utf8, true)]);

        let source_batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![Arc::new(StringArray::from(vec![
                Some("a@x.com"), Some("b@x.com"), Some("c@x.com"),
                Some("d@x.com"), Some("e@x.com"),
            ]))],
        )
        .unwrap();
        let target_batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![Arc::new(StringArray::from(vec![
                Some("not-email"), Some("not-email"), Some("not-email"),
                Some("not-email"), Some("not-email"),
            ]))],
        )
        .unwrap();

        let source = MockDataProvider::new(schema.clone(), vec![source_batch]);
        let target = MockDataProvider::new(schema, vec![target_batch]);

        let report = diff_datasets(source, "s", target, "t").await.unwrap();
        assert!(!report.semantic_drifts.is_empty());
        assert_eq!(report.semantic_drifts[0].field_name, "contact");
    }

    #[tokio::test]
    async fn test_diff_provider_error() {
        let error_provider: Arc<dyn DataProvider> = Arc::new(ErrorProvider);
        let schema = int32_schema("val");
        let dummy: Arc<dyn DataProvider> =
            MockDataProvider::new(schema, vec![]);

        let result = diff_datasets(error_provider, "s", dummy, "t").await;
        assert!(result.is_err());
        match result.unwrap_err() {
            SagoError::Schema(_) => {}
            e => panic!("Expected Schema error, got: {:?}", e),
        }
    }
}
