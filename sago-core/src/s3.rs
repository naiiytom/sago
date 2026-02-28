use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use crate::{Result, SchemaProvider, DataProvider, SagoError};
use object_store::aws::AmazonS3Builder;
use object_store::{ObjectStore, path::Path};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::sync::Arc;
use bytes::Bytes;

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
        
        Ok(Self { store: Arc::new(store) })
    }
}

#[async_trait]
impl SchemaProvider for S3SchemaProvider {
    async fn get_schema(&self, identifier: &str) -> Result<Schema> {
        let path = Path::from(identifier);
        
        let result = self.store.get(&path).await.map_err(|e| SagoError::Io(std::io::Error::new(std::io::ErrorKind::Other, e)))?;
        let bytes: Bytes = result.bytes().await.map_err(|e| SagoError::Io(std::io::Error::new(std::io::ErrorKind::Other, e)))?;
        
        let builder = ParquetRecordBatchReaderBuilder::try_new(bytes)
            .map_err(|e| SagoError::Schema(format!("Failed to parse parquet schema: {}", e)))?;
        
        Ok(builder.schema().as_ref().clone())
    }
}

#[async_trait]
impl DataProvider for S3SchemaProvider {
    async fn get_data(&self, identifier: &str) -> Result<Vec<RecordBatch>> {
        let path = Path::from(identifier);
        
        let result = self.store.get(&path).await.map_err(|e| SagoError::Io(std::io::Error::new(std::io::ErrorKind::Other, e)))?;
        let bytes: Bytes = result.bytes().await.map_err(|e| SagoError::Io(std::io::Error::new(std::io::ErrorKind::Other, e)))?;
        
        let builder = ParquetRecordBatchReaderBuilder::try_new(bytes)
            .map_err(|e| SagoError::Schema(format!("Failed to parse parquet schema: {}", e)))?;
        
        let reader = builder.build()
            .map_err(|e| SagoError::Schema(format!("Failed to build parquet reader: {}", e)))?;
        
        let mut batches = Vec::new();
        for batch_result in reader {
            let batch = batch_result.map_err(|e| SagoError::Arrow(e.into()))?;
            batches.push(batch);
        }
        
        Ok(batches)
    }
}
