use crate::{DataProvider, Result, SagoError, SchemaProvider};
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use bytes::Bytes;
use object_store::aws::AmazonS3Builder;
use object_store::{ObjectStore, path::Path};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::sync::Arc;

pub struct S3SchemaProvider {
    store: Arc<dyn ObjectStore>,
}

impl S3SchemaProvider {
    pub fn new(bucket: &str, region: &str) -> Result<Self> {
        let store = AmazonS3Builder::new()
            .with_bucket_name(bucket)
            .with_region(region)
            .build()
            .map_err(|e| SagoError::Config(format!("Failed to build S3 store: {}", e)))?;

        Ok(Self {
            store: Arc::new(store),
        })
    }

    #[cfg(test)]
    fn new_with_store(store: Arc<dyn ObjectStore>) -> Self {
        Self { store }
    }
}

#[async_trait]
impl SchemaProvider for S3SchemaProvider {
    async fn get_schema(&self, identifier: &str) -> Result<Schema> {
        let path = Path::from(identifier);

        let result = self
            .store
            .get(&path)
            .await
            .map_err(|e| SagoError::Io(std::io::Error::other(e)))?;
        let bytes: Bytes = result
            .bytes()
            .await
            .map_err(|e| SagoError::Io(std::io::Error::other(e)))?;

        let builder = ParquetRecordBatchReaderBuilder::try_new(bytes)
            .map_err(|e| SagoError::Schema(format!("Failed to parse parquet schema: {}", e)))?;

        Ok(builder.schema().as_ref().clone())
    }
}

#[async_trait]
impl DataProvider for S3SchemaProvider {
    async fn get_data(&self, identifier: &str) -> Result<Vec<RecordBatch>> {
        let path = Path::from(identifier);

        let result = self
            .store
            .get(&path)
            .await
            .map_err(|e| SagoError::Io(std::io::Error::other(e)))?;
        let bytes: Bytes = result
            .bytes()
            .await
            .map_err(|e| SagoError::Io(std::io::Error::other(e)))?;

        let builder = ParquetRecordBatchReaderBuilder::try_new(bytes)
            .map_err(|e| SagoError::Schema(format!("Failed to parse parquet schema: {}", e)))?;

        let reader = builder
            .build()
            .map_err(|e| SagoError::Schema(format!("Failed to build parquet reader: {}", e)))?;

        let mut batches = Vec::new();
        for batch_result in reader {
            let batch = batch_result.map_err(SagoError::Arrow)?;
            batches.push(batch);
        }

        Ok(batches)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use object_store::memory::InMemory;
    use object_store::path::Path;

    fn make_parquet_bytes(schema: Arc<Schema>, batches: &[RecordBatch]) -> Bytes {
        let mut buf = Vec::new();
        let mut writer = parquet::arrow::ArrowWriter::try_new(&mut buf, schema, None).unwrap();
        for batch in batches {
            writer.write(batch).unwrap();
        }
        writer.close().unwrap();
        Bytes::from(buf)
    }

    fn two_column_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]))
    }

    fn two_column_batch(schema: Arc<Schema>) -> RecordBatch {
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec![Some("alice"), Some("bob"), None])),
            ],
        )
        .unwrap()
    }

    async fn store_with_parquet(path: &str, bytes: Bytes) -> Arc<InMemory> {
        let store = Arc::new(InMemory::new());
        store.put(&Path::from(path), bytes.into()).await.unwrap();
        store
    }

    // ── get_schema ───────────────────────────────────────────────────────────

    #[tokio::test]
    async fn test_get_schema_returns_correct_fields() {
        let schema = two_column_schema();
        let batch = two_column_batch(schema.clone());
        let bytes = make_parquet_bytes(schema, &[batch]);

        let store = store_with_parquet("data.parquet", bytes).await;
        let provider = S3SchemaProvider::new_with_store(store);

        let result = provider.get_schema("data.parquet").await.unwrap();
        assert_eq!(result.fields().len(), 2);
        assert_eq!(result.field(0).name(), "id");
        assert_eq!(result.field(1).name(), "name");
        assert_eq!(result.field(0).data_type(), &DataType::Int32);
    }

    #[tokio::test]
    async fn test_get_schema_missing_path_returns_io_error() {
        let store = Arc::new(InMemory::new());
        let provider = S3SchemaProvider::new_with_store(store);

        let result = provider.get_schema("nonexistent.parquet").await;
        assert!(result.is_err());
        match result.unwrap_err() {
            SagoError::Io(_) => {}
            e => panic!("expected Io error, got {:?}", e),
        }
    }

    #[tokio::test]
    async fn test_get_schema_invalid_bytes_returns_schema_error() {
        let store = store_with_parquet("bad.parquet", Bytes::from(b"not parquet".as_slice())).await;
        let provider = S3SchemaProvider::new_with_store(store);

        let result = provider.get_schema("bad.parquet").await;
        assert!(result.is_err());
        match result.unwrap_err() {
            SagoError::Schema(_) => {}
            e => panic!("expected Schema error, got {:?}", e),
        }
    }

    // ── get_data ─────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn test_get_data_returns_batches_with_correct_row_count() {
        let schema = two_column_schema();
        let batch = two_column_batch(schema.clone());
        let bytes = make_parquet_bytes(schema, &[batch]);

        let store = store_with_parquet("data.parquet", bytes).await;
        let provider = S3SchemaProvider::new_with_store(store);

        let batches = provider.get_data("data.parquet").await.unwrap();
        assert!(!batches.is_empty());
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 3);
    }

    #[tokio::test]
    async fn test_get_data_preserves_null_values() {
        let schema = two_column_schema();
        let batch = two_column_batch(schema.clone());
        let bytes = make_parquet_bytes(schema, &[batch]);

        let store = store_with_parquet("data.parquet", bytes).await;
        let provider = S3SchemaProvider::new_with_store(store);

        let batches = provider.get_data("data.parquet").await.unwrap();
        let name_col = batches[0].column_by_name("name").unwrap();
        assert_eq!(name_col.null_count(), 1);
    }

    #[tokio::test]
    async fn test_get_data_missing_path_returns_error() {
        let store = Arc::new(InMemory::new());
        let provider = S3SchemaProvider::new_with_store(store);

        let result = provider.get_data("nonexistent.parquet").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_get_data_invalid_bytes_returns_schema_error() {
        let store = store_with_parquet("bad.parquet", Bytes::from(b"garbage".as_slice())).await;
        let provider = S3SchemaProvider::new_with_store(store);

        let result = provider.get_data("bad.parquet").await;
        assert!(result.is_err());
        match result.unwrap_err() {
            SagoError::Schema(_) => {}
            e => panic!("expected Schema error, got {:?}", e),
        }
    }

    #[tokio::test]
    async fn test_get_data_schema_matches_get_schema() {
        let schema = two_column_schema();
        let batch = two_column_batch(schema.clone());
        let bytes = make_parquet_bytes(schema, &[batch]);

        let store = store_with_parquet("data.parquet", bytes).await;
        let provider = S3SchemaProvider::new_with_store(store);

        let inferred_schema = provider.get_schema("data.parquet").await.unwrap();
        let batches = provider.get_data("data.parquet").await.unwrap();
        let batch_schema = batches[0].schema();

        assert_eq!(inferred_schema.fields().len(), batch_schema.fields().len());
        assert_eq!(
            inferred_schema.field(0).name(),
            batch_schema.field(0).name()
        );
    }
}
