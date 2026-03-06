use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum SagoError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Database error: {0}")]
    Database(#[from] sqlx::Error),

    #[error("Configuration error: {0}")]
    Config(String),

    #[error("Schema error: {0}")]
    Schema(String),

    #[error("Arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),

    #[error("Unknown error: {0}")]
    Unknown(String),
}

pub mod config;
pub mod postgres;
pub mod s3;
pub mod drift;
pub mod semantic;
pub mod diff;

pub type Result<T> = std::result::Result<T, SagoError>;

/// The SchemaProvider trait defines the interface for fetching schemas from data sources.
#[async_trait]
pub trait SchemaProvider: Send + Sync {
    /// Retrieves the Arrow Schema for a given identifier (e.g., table name, file path).
    async fn get_schema(&self, identifier: &str) -> Result<Schema>;
}

/// The DataProvider trait defines the interface for fetching data from data sources.
#[async_trait]
pub trait DataProvider: SchemaProvider {
    /// Retrieves the data as a collection of Arrow RecordBatches.
    async fn get_data(&self, identifier: &str) -> Result<Vec<RecordBatch>>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{Field, DataType};
    

    struct MockSchemaProvider;

    #[async_trait]
    impl SchemaProvider for MockSchemaProvider {
        async fn get_schema(&self, _identifier: &str) -> Result<Schema> {
            let schema = Schema::new(vec![
                Field::new("id", DataType::Int32, false),
                Field::new("name", DataType::Utf8, false),
            ]);
            Ok(schema)
        }
    }

    #[tokio::test]
    async fn test_schema_provider() {
        let provider = MockSchemaProvider;
        let schema = provider.get_schema("test_table").await.unwrap();
        assert_eq!(schema.fields().len(), 2);
    }
}
