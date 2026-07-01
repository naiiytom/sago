use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum SagoError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[cfg(feature = "io")]
    #[error("Database error: {0}")]
    Database(#[from] sqlx::Error),

    /// TOML deserialization failure (syntax, unknown field, missing field).
    #[error("TOML parse error: {0}")]
    TomlParse(#[from] toml::de::Error),

    /// JSON (de)serialization failure — e.g. reading or writing the state file.
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    /// A semantically-invalid configuration that parsed fine (e.g. an obsolete
    /// block). Reserve for genuine config-logic errors, not parse failures.
    #[error("Configuration error: {0}")]
    Config(String),

    #[error("Schema error: {0}")]
    Schema(String),

    /// A persisted Arrow data type that the state codec cannot parse back.
    #[error("unsupported serialized data type: {0}")]
    UnsupportedDataType(String),

    /// The on-disk state file was written by an incompatible version of sago.
    #[error("unsupported state schema_version: {found} (expected {expected})")]
    UnsupportedStateVersion { found: u32, expected: u32 },

    #[error("Arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),

    #[error("Unknown error: {0}")]
    Unknown(String),
}

pub mod config;
pub mod diff;
pub mod drift;
pub mod merge;
pub mod merkle;
pub mod rename;
pub mod schema_codec;
pub mod semantic;

// Data-source providers and filesystem state persistence require the async
// runtime / IO crates and are only available with the `io` feature.
#[cfg(feature = "io")]
pub mod connection;
#[cfg(feature = "io")]
pub mod postgres;
#[cfg(feature = "io")]
pub mod s3;
#[cfg(feature = "io")]
pub mod state;

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
    use arrow::datatypes::{DataType, Field};

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

    // ── SagoError display formatting ─────────────────────────────────────────

    #[test]
    fn test_error_config_display() {
        let e = SagoError::Config("bad value".into());
        assert!(e.to_string().contains("bad value"));
    }

    #[test]
    fn test_error_schema_display() {
        let e = SagoError::Schema("table not found".into());
        assert!(e.to_string().contains("table not found"));
    }

    #[test]
    fn test_error_io_display() {
        let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "file missing");
        let e = SagoError::Io(io_err);
        assert!(e.to_string().contains("file missing"));
    }

    #[test]
    fn test_error_unknown_display() {
        let e = SagoError::Unknown("something weird".into());
        assert!(e.to_string().contains("something weird"));
    }

    // ── DataProvider trait with a mock ───────────────────────────────────────

    struct MockDataProvider;

    #[async_trait]
    impl SchemaProvider for MockDataProvider {
        async fn get_schema(&self, _identifier: &str) -> Result<Schema> {
            Ok(Schema::new(vec![Field::new("id", DataType::Int32, false)]))
        }
    }

    #[async_trait]
    impl DataProvider for MockDataProvider {
        async fn get_data(&self, _identifier: &str) -> Result<Vec<RecordBatch>> {
            use arrow::array::Int32Array;
            use std::sync::Arc;
            let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
            let batch =
                RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![1, 2, 3]))])
                    .map_err(SagoError::Arrow)?;
            Ok(vec![batch])
        }
    }

    #[tokio::test]
    async fn test_data_provider_get_data() {
        let provider = MockDataProvider;
        let batches = provider.get_data("tbl").await.unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 3);
    }

    #[tokio::test]
    async fn test_data_provider_schema_and_data_consistent() {
        let provider = MockDataProvider;
        let schema = provider.get_schema("tbl").await.unwrap();
        let batches = provider.get_data("tbl").await.unwrap();
        assert_eq!(schema.fields().len(), batches[0].schema().fields().len());
    }
}
