use crate::config::S3Format;
use crate::{DataProvider, Result, SagoError, SchemaProvider};
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use bytes::Bytes;
use futures::TryStreamExt;
use object_store::aws::AmazonS3Builder;
use object_store::{ObjectStore, ObjectStoreExt, path::Path};
use parquet::arrow::ParquetRecordBatchStreamBuilder;
use parquet::arrow::async_reader::ParquetObjectReader;
use std::sync::Arc;

enum S3FormatResolved {
    Parquet,
    Csv,
    /// Strictly newline-delimited JSON (one JSON object per line) — the
    /// `.ndjson` extension, or an explicit `format = "json"` override,
    /// signals this convention unambiguously.
    Ndjson,
    /// A `.json` file, whose content may be *either* NDJSON or a single
    /// top-level JSON array (the more common shape for a plain REST/API
    /// export like `[{...},{...}]`). Distinguished from `Ndjson` so the
    /// data path can detect and unwrap a top-level array before handing the
    /// bytes to Arrow's line-oriented JSON reader, which otherwise errors on
    /// a bare array (`ValueIter` expects one JSON object per line).
    JsonAmbiguous,
}

fn detect_format(override_format: Option<&S3Format>, identifier: &str) -> S3FormatResolved {
    if let Some(fmt) = override_format {
        return match fmt {
            S3Format::Parquet => S3FormatResolved::Parquet,
            S3Format::Csv => S3FormatResolved::Csv,
            // An explicit override name of "json" is deliberately treated as
            // ambiguous too (not forced-NDJSON), so a user who overrides the
            // format for a `.dat`-extensioned array export still gets the
            // array-unwrapping behavior below.
            S3Format::Json => S3FormatResolved::JsonAmbiguous,
        };
    }
    let lower = identifier.to_lowercase();
    if lower.ends_with(".csv") {
        S3FormatResolved::Csv
    } else if lower.ends_with(".ndjson") {
        S3FormatResolved::Ndjson
    } else if lower.ends_with(".json") {
        S3FormatResolved::JsonAmbiguous
    } else {
        S3FormatResolved::Parquet
    }
}

/// Arrow's JSON reader (`ValueIter`) is strictly newline-delimited: it reads
/// one line at a time and requires each to parse as a JSON object. A `.json`
/// export is just as commonly a single top-level JSON array
/// (`[{...},{...}]`), which fails that reader outright ("Expected JSON
/// record to be an object, found Array"). If `bytes` parses as a top-level
/// JSON array, re-serialize it into newline-delimited form so the rest of
/// the JSON pipeline (built entirely around NDJSON) can handle it
/// unmodified; otherwise return `bytes` as-is (already NDJSON, or malformed
/// — in which case the downstream NDJSON reader reports the real error).
fn normalize_json_bytes(bytes: &Bytes) -> Bytes {
    let trimmed_start = bytes
        .iter()
        .position(|b| !b.is_ascii_whitespace())
        .unwrap_or(bytes.len());
    if bytes.get(trimmed_start) != Some(&b'[') {
        return bytes.clone();
    }
    let Ok(values) = serde_json::from_slice::<Vec<serde_json::Value>>(bytes) else {
        return bytes.clone();
    };
    let mut out = Vec::new();
    for value in &values {
        if serde_json::to_writer(&mut out, value).is_err() {
            return bytes.clone();
        }
        out.push(b'\n');
    }
    Bytes::from(out)
}

pub struct S3SchemaProvider {
    store: Arc<dyn ObjectStore>,
    format: Option<S3Format>,
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
            format: None,
        })
    }

    pub fn with_format(mut self, format: Option<S3Format>) -> Self {
        self.format = format;
        self
    }

    #[cfg(test)]
    fn new_with_store(store: Arc<dyn ObjectStore>) -> Self {
        Self {
            store,
            format: None,
        }
    }

    async fn fetch_bytes(&self, identifier: &str) -> Result<Bytes> {
        let path = Path::from(identifier);
        let result = self
            .store
            .get(&path)
            .await
            .map_err(|e| SagoError::Io(std::io::Error::other(e)))?;
        result
            .bytes()
            .await
            .map_err(|e| SagoError::Io(std::io::Error::other(e)))
    }

    /// A [`ParquetObjectReader`] over `identifier`, pre-sized via a `HEAD`
    /// request so the reader issues bounded range requests for the footer
    /// instead of a suffix request (not universally supported, and an extra
    /// round-trip when the size is already known).
    async fn parquet_object_reader(&self, identifier: &str) -> Result<ParquetObjectReader> {
        let path = Path::from(identifier);
        let meta = self
            .store
            .head(&path)
            .await
            .map_err(|e| SagoError::Io(std::io::Error::other(e)))?;
        Ok(ParquetObjectReader::new(self.store.clone(), path).with_file_size(meta.size))
    }

    /// Read only the Parquet footer (schema + metadata) via bounded range
    /// requests, never fetching the row-group data.
    async fn schema_from_parquet_streaming(&self, identifier: &str) -> Result<Schema> {
        let reader = self.parquet_object_reader(identifier).await?;
        let builder = ParquetRecordBatchStreamBuilder::new(reader)
            .await
            .map_err(|e| SagoError::Schema(format!("Failed to parse parquet schema: {e}")))?;
        Ok(builder.schema().as_ref().clone())
    }

    /// Stream every row group from the object store, decoding one at a time,
    /// rather than materializing the whole object in memory up front.
    async fn data_from_parquet_streaming(&self, identifier: &str) -> Result<Vec<RecordBatch>> {
        let reader = self.parquet_object_reader(identifier).await?;
        let builder = ParquetRecordBatchStreamBuilder::new(reader)
            .await
            .map_err(|e| SagoError::Schema(format!("Failed to parse parquet schema: {e}")))?;
        let stream = builder
            .build()
            .map_err(|e| SagoError::Schema(format!("Failed to build parquet reader: {e}")))?;
        stream
            .try_collect::<Vec<_>>()
            .await
            .map_err(|e| SagoError::Schema(format!("Failed to read parquet row group: {e}")))
    }
}

#[async_trait]
impl SchemaProvider for S3SchemaProvider {
    async fn get_schema(&self, identifier: &str) -> Result<Schema> {
        match detect_format(self.format.as_ref(), identifier) {
            // Parquet's footer-only metadata read goes straight through the
            // object store (a couple of small range requests), never
            // materializing the whole object just to read the schema.
            S3FormatResolved::Parquet => self.schema_from_parquet_streaming(identifier).await,
            S3FormatResolved::Csv => schema_from_csv(&self.fetch_bytes(identifier).await?),
            S3FormatResolved::Ndjson => schema_from_json(&self.fetch_bytes(identifier).await?),
            S3FormatResolved::JsonAmbiguous => {
                schema_from_json(&normalize_json_bytes(&self.fetch_bytes(identifier).await?))
            }
        }
    }
}

#[async_trait]
impl DataProvider for S3SchemaProvider {
    async fn get_data(&self, identifier: &str) -> Result<Vec<RecordBatch>> {
        match detect_format(self.format.as_ref(), identifier) {
            // Streamed row-group-at-a-time from the object store rather than
            // buffering the whole object into memory first: a multi-GB
            // Parquet file no longer risks OOMing a memory-constrained
            // runner before a single row is parsed.
            S3FormatResolved::Parquet => self.data_from_parquet_streaming(identifier).await,
            S3FormatResolved::Csv => data_from_csv(&self.fetch_bytes(identifier).await?),
            S3FormatResolved::Ndjson => data_from_json(&self.fetch_bytes(identifier).await?),
            S3FormatResolved::JsonAmbiguous => {
                data_from_json(&normalize_json_bytes(&self.fetch_bytes(identifier).await?))
            }
        }
    }
}

fn schema_from_csv(bytes: &Bytes) -> Result<Schema> {
    use arrow::csv::reader::Format;
    let format = Format::default().with_header(true);
    let (schema, _) = format
        .infer_schema(&mut std::io::Cursor::new(bytes.as_ref()), Some(100))
        .map_err(|e| SagoError::Schema(format!("CSV schema inference failed: {e}")))?;
    Ok(schema)
}

fn schema_from_json(bytes: &Bytes) -> Result<Schema> {
    use arrow::json::reader::infer_json_schema;
    let (schema, _) = infer_json_schema(std::io::Cursor::new(bytes.as_ref()), Some(100))
        .map_err(|e| SagoError::Schema(format!("JSON schema inference failed: {e}")))?;
    Ok(schema)
}

fn data_from_csv(bytes: &Bytes) -> Result<Vec<RecordBatch>> {
    use arrow::csv::ReaderBuilder;
    use arrow::csv::reader::Format;
    let format = Format::default().with_header(true);
    let (schema, _) = format
        .infer_schema(&mut std::io::Cursor::new(bytes.as_ref()), Some(100))
        .map_err(|e| SagoError::Schema(format!("CSV schema inference failed: {e}")))?;
    let reader = ReaderBuilder::new(Arc::new(schema))
        .with_header(true)
        .build(std::io::Cursor::new(bytes.as_ref()))
        .map_err(|e| SagoError::Schema(format!("Failed to build CSV reader: {e}")))?;
    let mut batches = Vec::new();
    for batch_result in reader {
        batches.push(batch_result.map_err(SagoError::Arrow)?);
    }
    Ok(batches)
}

fn data_from_json(bytes: &Bytes) -> Result<Vec<RecordBatch>> {
    use arrow::json::ReaderBuilder;
    use arrow::json::reader::infer_json_schema;
    let (schema, _) = infer_json_schema(std::io::Cursor::new(bytes.as_ref()), Some(100))
        .map_err(|e| SagoError::Schema(format!("JSON schema inference failed: {e}")))?;
    let reader = ReaderBuilder::new(Arc::new(schema))
        .build(std::io::Cursor::new(bytes.as_ref()))
        .map_err(|e| SagoError::Schema(format!("Failed to build JSON reader: {e}")))?;
    let mut batches = Vec::new();
    for batch_result in reader {
        batches.push(batch_result.map_err(SagoError::Arrow)?);
    }
    Ok(batches)
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

    // ── detect_format ────────────────────────────────────────────────────────

    #[test]
    fn test_detect_format_parquet() {
        assert!(matches!(
            detect_format(None, "data/file.parquet"),
            S3FormatResolved::Parquet
        ));
        assert!(matches!(
            detect_format(None, "data/file.PARQUET"),
            S3FormatResolved::Parquet
        ));
        assert!(matches!(
            detect_format(None, "data/file.unknown"),
            S3FormatResolved::Parquet
        ));
    }

    #[test]
    fn test_detect_format_csv() {
        assert!(matches!(
            detect_format(None, "data/file.csv"),
            S3FormatResolved::Csv
        ));
        assert!(matches!(
            detect_format(None, "data/file.CSV"),
            S3FormatResolved::Csv
        ));
    }

    #[test]
    fn test_detect_format_json() {
        // `.json` is ambiguous (may be NDJSON or a top-level array);
        // `.ndjson` unambiguously signals strict newline-delimited JSON.
        assert!(matches!(
            detect_format(None, "data/file.json"),
            S3FormatResolved::JsonAmbiguous
        ));
        assert!(matches!(
            detect_format(None, "data/file.ndjson"),
            S3FormatResolved::Ndjson
        ));
    }

    #[test]
    fn test_detect_format_override() {
        use crate::config::S3Format;
        assert!(matches!(
            detect_format(Some(&S3Format::Csv), "data/file.parquet"),
            S3FormatResolved::Csv
        ));
        assert!(matches!(
            detect_format(Some(&S3Format::Json), "data/file.csv"),
            S3FormatResolved::JsonAmbiguous
        ));
    }

    // ── CSV helpers ──────────────────────────────────────────────────────────

    #[test]
    fn test_schema_from_csv_basic() {
        let csv_data = b"id,name,score\n1,alice,9.5\n2,bob,8.0\n";
        let schema = schema_from_csv(&bytes::Bytes::from(csv_data.as_ref())).unwrap();
        assert_eq!(schema.fields().len(), 3);
        assert!(schema.field_with_name("id").is_ok());
        assert!(schema.field_with_name("name").is_ok());
        assert!(schema.field_with_name("score").is_ok());
    }

    #[test]
    fn test_data_from_csv_basic() {
        let csv_data = b"id,name\n1,alice\n2,bob\n3,carol\n";
        let batches = data_from_csv(&bytes::Bytes::from(csv_data.as_ref())).unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 3);
    }

    // ── JSON helpers ─────────────────────────────────────────────────────────

    #[test]
    fn test_schema_from_json_basic() {
        let json_data = b"{\"id\":1,\"name\":\"alice\"}\n{\"id\":2,\"name\":\"bob\"}\n";
        let schema = schema_from_json(&bytes::Bytes::from(json_data.as_ref())).unwrap();
        assert!(schema.field_with_name("id").is_ok());
        assert!(schema.field_with_name("name").is_ok());
    }

    #[test]
    fn test_data_from_json_basic() {
        let json_data = b"{\"id\":1,\"val\":1.5}\n{\"id\":2,\"val\":2.5}\n";
        let batches = data_from_json(&bytes::Bytes::from(json_data.as_ref())).unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 2);
    }

    // ── normalize_json_bytes (top-level array vs NDJSON) ─────────────────────

    #[test]
    fn test_normalize_json_bytes_unwraps_top_level_array() {
        // Regression: a `.json` export shaped as a single top-level array
        // (a common REST/API dump convention) must be usable by the
        // line-oriented NDJSON reader after normalization.
        let array_json = Bytes::from(b"[{\"id\":1,\"name\":\"a\"},{\"id\":2,\"name\":\"b\"}]".as_slice());
        let normalized = normalize_json_bytes(&array_json);
        let schema = schema_from_json(&normalized).unwrap();
        assert!(schema.field_with_name("id").is_ok());
        assert!(schema.field_with_name("name").is_ok());
        let batches = data_from_json(&normalized).unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 2);
    }

    #[test]
    fn test_normalize_json_bytes_leaves_ndjson_unchanged() {
        let ndjson = Bytes::from(b"{\"id\":1}\n{\"id\":2}\n".as_slice());
        let normalized = normalize_json_bytes(&ndjson);
        assert_eq!(normalized, ndjson);
    }

    #[test]
    fn test_normalize_json_bytes_handles_leading_whitespace_before_array() {
        let array_json = Bytes::from(b"  \n[{\"id\":1}]".as_slice());
        let normalized = normalize_json_bytes(&array_json);
        let batches = data_from_json(&normalized).unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 1);
    }

    #[test]
    fn test_normalize_json_bytes_empty_array_produces_no_rows() {
        let array_json = Bytes::from(b"[]".as_slice());
        let normalized = normalize_json_bytes(&array_json);
        assert_eq!(normalized.as_ref(), b"");
    }

    #[test]
    fn test_get_schema_and_get_data_unwrap_json_array_via_provider() {
        // End-to-end: a .json object holding a top-level array must work
        // through the full S3SchemaProvider, not just the unit-level helper.
        let store = Arc::new(object_store::memory::InMemory::new());
        futures::executor::block_on(async {
            store
                .put(
                    &Path::from("export.json"),
                    Bytes::from(b"[{\"id\":1,\"name\":\"a\"},{\"id\":2,\"name\":\"b\"}]".as_slice())
                        .into(),
                )
                .await
                .unwrap();
        });
        let provider = S3SchemaProvider::new_with_store(store);

        let schema = futures::executor::block_on(provider.get_schema("export.json")).unwrap();
        assert!(schema.field_with_name("id").is_ok());

        let batches = futures::executor::block_on(provider.get_data("export.json")).unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 2);
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
