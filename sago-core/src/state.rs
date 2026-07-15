use std::collections::HashMap;
use std::path::Path;

use arrow::datatypes::{DataType, Field, Schema};
use serde::{Deserialize, Serialize};

use crate::drift::{ColumnStats, calculate_column_stats, extract_numeric_values};
use crate::schema_codec::{parse_data_type, serialize_data_type};
use crate::semantic::{SemanticType, infer_semantic_type_multi};
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
                    data_type: serialize_data_type(f.data_type()),
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
        let bytes = std::fs::read(path)?;
        Self::from_slice(&bytes)
    }

    pub fn load_or_default(path: &Path) -> Result<Self> {
        match std::fs::read(path) {
            Ok(bytes) => Self::from_slice(&bytes),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Self::empty()),
            Err(e) => Err(SagoError::Io(e)),
        }
    }

    /// Deserialize and version-check a state document from raw JSON bytes.
    fn from_slice(bytes: &[u8]) -> Result<Self> {
        let state: ProjectState = serde_json::from_slice(bytes)?;
        if state.schema_version != CURRENT_SCHEMA_VERSION {
            return Err(SagoError::UnsupportedStateVersion {
                found: state.schema_version,
                expected: CURRENT_SCHEMA_VERSION,
            });
        }
        Ok(state)
    }

    /// Persist this state to `path`, merging with whatever is currently on
    /// disk under an advisory lock.
    ///
    /// A plain load-modify-save cycle races across processes: two concurrent
    /// `sago apply` invocations targeting different snapshots each load their
    /// own in-memory copy, insert only the target(s) they're applying, and
    /// whichever `save()` runs last would otherwise overwrite the file with
    /// its own copy — silently dropping the other process's snapshot update
    /// (or any snapshot that existed before either process started, if it
    /// wasn't in either process's in-memory copy). Locking the read-merge-
    /// write sequence closes that window: the on-disk file always ends up
    /// containing the union of every process's `snapshots`, with the calling
    /// process's own entries taking precedence for keys it touched.
    pub fn save(&self, path: &Path) -> Result<()> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(SagoError::Io)?;
        }

        let _lock = FileLock::acquire(&lock_sibling(path))?;

        let mut merged = match std::fs::read(path) {
            Ok(bytes) => Self::from_slice(&bytes).unwrap_or_else(|_| self.clone()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => self.clone(),
            Err(e) => return Err(SagoError::Io(e)),
        };
        merged.schema_version = self.schema_version;
        for (name, snap) in &self.snapshots {
            merged.snapshots.insert(name.clone(), snap.clone());
        }

        let json = serde_json::to_string_pretty(&merged)?;
        // Write to a sibling temp file then atomically rename over the target so
        // an interrupted write (crash, full disk) can never truncate a
        // previously-valid state.json.
        let tmp = tmp_sibling(path);
        std::fs::write(&tmp, json).map_err(SagoError::Io)?;
        std::fs::rename(&tmp, path).map_err(SagoError::Io)?;
        Ok(())
    }
}

/// A temp path alongside `path` for the write-then-rename in [`ProjectState::save`].
fn tmp_sibling(path: &Path) -> std::path::PathBuf {
    let mut name = path.file_name().unwrap_or_default().to_os_string();
    name.push(".tmp");
    path.with_file_name(name)
}

/// A lock-file path alongside `path`, distinct from [`tmp_sibling`]'s path so
/// the two never collide.
fn lock_sibling(path: &Path) -> std::path::PathBuf {
    let mut name = path.file_name().unwrap_or_default().to_os_string();
    name.push(".lock");
    path.with_file_name(name)
}

/// A simple cross-platform advisory lock built on atomic file creation
/// (`O_EXCL`-equivalent `create_new`), rather than a platform-specific
/// `flock`/`LockFileEx` syscall — this keeps `sago-core` free of a new
/// dependency and works identically on every target this crate builds for.
/// Held for the duration of [`ProjectState::save`]'s read-merge-write
/// sequence; released (the lock file removed) on drop.
struct FileLock {
    path: std::path::PathBuf,
}

impl FileLock {
    /// Retries with a short backoff rather than blocking indefinitely: a
    /// `sago apply` invocation should fail loudly after a bounded wait if
    /// another process holds the lock unexpectedly long (e.g. crashed while
    /// holding it) rather than hang forever.
    fn acquire(path: &Path) -> Result<Self> {
        const MAX_ATTEMPTS: u32 = 50;
        const RETRY_DELAY: std::time::Duration = std::time::Duration::from_millis(20);

        for attempt in 0..MAX_ATTEMPTS {
            match std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(path)
            {
                Ok(_) => {
                    return Ok(FileLock {
                        path: path.to_path_buf(),
                    });
                }
                Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                    if attempt + 1 == MAX_ATTEMPTS {
                        return Err(SagoError::Io(std::io::Error::new(
                            std::io::ErrorKind::WouldBlock,
                            format!(
                                "timed out waiting for lock file {} (another process may be stuck holding it)",
                                path.display()
                            ),
                        )));
                    }
                    std::thread::sleep(RETRY_DELAY);
                }
                Err(e) => return Err(SagoError::Io(e)),
            }
        }
        unreachable!("loop always returns before exhausting MAX_ATTEMPTS iterations")
    }
}

impl Drop for FileLock {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
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
        let cols: Vec<_> = batches
            .iter()
            .filter_map(|b| b.column_by_name(field.name()).cloned())
            .collect();
        if !cols.is_empty() {
            semantic_types.insert(
                field.name().clone(),
                infer_semantic_type_multi(field.name(), &cols),
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
            SagoError::UnsupportedDataType(msg) => assert!(msg.contains("List")),
            other => panic!("expected UnsupportedDataType error, got {:?}", other),
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
                variance: None,
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
    fn test_save_is_atomic_and_overwrites() {
        // Saving over an existing state file must replace it wholesale (via the
        // temp+rename path) and leave no stray temp file behind.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("state.json");

        ProjectState::empty().save(&path).unwrap();

        let mut state = ProjectState::empty();
        state.snapshots.insert(
            "t".into(),
            TargetSnapshot {
                captured_at: "2026-01-01T00:00:00Z".into(),
                schema: SerializableSchema { fields: vec![] },
                column_stats: HashMap::new(),
                semantic_types: HashMap::new(),
                samples: None,
            },
        );
        state.save(&path).unwrap();

        let reloaded = ProjectState::load(&path).unwrap();
        assert_eq!(reloaded.snapshots.len(), 1);
        // No leftover temp sibling.
        assert!(!path.with_file_name("state.json.tmp").exists());
    }

    #[test]
    fn test_load_missing_file_returns_default() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("nope.json");
        let state = ProjectState::load_or_default(&path).unwrap();
        assert_eq!(state.schema_version, 1);
        assert!(state.snapshots.is_empty());
    }

    fn snapshot(captured_at: &str) -> TargetSnapshot {
        TargetSnapshot {
            captured_at: captured_at.into(),
            schema: SerializableSchema { fields: vec![] },
            column_stats: HashMap::new(),
            semantic_types: HashMap::new(),
            samples: None,
        }
    }

    #[test]
    fn test_save_merges_with_concurrently_written_disjoint_snapshot() {
        // Regression: simulates two `sago apply --target <name>` invocations
        // on different targets racing against the same state.json. Before
        // save() merged with the on-disk file, whichever call's save() ran
        // last would clobber the whole file with only its own snapshot(s),
        // silently losing the other process's update.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("state.json");

        // Process A: loads empty state, applies target "orders", saves.
        let mut state_a = ProjectState::load_or_default(&path).unwrap();
        state_a
            .snapshots
            .insert("orders".into(), snapshot("2026-01-01T00:00:00Z"));
        state_a.save(&path).unwrap();

        // Process B: loaded its own (now-stale) empty state *before* A saved,
        // applies target "invoices" (disjoint from A), saves after A.
        let mut state_b = ProjectState::empty();
        state_b
            .snapshots
            .insert("invoices".into(), snapshot("2026-01-01T00:00:01Z"));
        state_b.save(&path).unwrap();

        // Both snapshots must be present — B's save must not have dropped A's.
        let on_disk = ProjectState::load(&path).unwrap();
        assert_eq!(on_disk.snapshots.len(), 2, "snapshots: {on_disk:?}");
        assert!(on_disk.snapshots.contains_key("orders"));
        assert!(on_disk.snapshots.contains_key("invoices"));
    }

    #[test]
    fn test_save_own_snapshot_takes_precedence_over_stale_disk_copy() {
        // If the same target key is re-applied, the calling process's own
        // (presumably newer) copy must win over whatever is on disk, not be
        // silently discarded by the merge.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("state.json");

        let mut older = ProjectState::empty();
        older
            .snapshots
            .insert("orders".into(), snapshot("2026-01-01T00:00:00Z"));
        older.save(&path).unwrap();

        let mut newer = ProjectState::empty();
        newer
            .snapshots
            .insert("orders".into(), snapshot("2026-01-02T00:00:00Z"));
        newer.save(&path).unwrap();

        let on_disk = ProjectState::load(&path).unwrap();
        assert_eq!(on_disk.snapshots.len(), 1);
        assert_eq!(
            on_disk.snapshots["orders"].captured_at,
            "2026-01-02T00:00:00Z"
        );
    }

    #[test]
    fn test_concurrent_saves_from_multiple_threads_lose_no_snapshot() {
        // A stronger version of the two-process regression test above: N
        // threads each load a stale empty snapshot of the state, insert
        // their own disjoint target, and race to save(). Every target must
        // survive regardless of interleaving.
        let dir = tempfile::tempdir().unwrap();
        let path = std::sync::Arc::new(dir.path().join("state.json"));
        const N: usize = 8;

        let handles: Vec<_> = (0..N)
            .map(|i| {
                let path = path.clone();
                std::thread::spawn(move || {
                    let mut state = ProjectState::empty();
                    state
                        .snapshots
                        .insert(format!("target-{i}"), snapshot("2026-01-01T00:00:00Z"));
                    state.save(&path).unwrap();
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }

        let on_disk = ProjectState::load(&path).unwrap();
        assert_eq!(
            on_disk.snapshots.len(),
            N,
            "expected all {N} targets, got: {:?}",
            on_disk.snapshots.keys().collect::<Vec<_>>()
        );
        for i in 0..N {
            assert!(on_disk.snapshots.contains_key(&format!("target-{i}")));
        }
    }

    #[test]
    fn test_load_unknown_schema_version_errors() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("state.json");
        std::fs::write(&path, r#"{"schema_version": 99, "snapshots": {}}"#).unwrap();
        let err = ProjectState::load(&path).unwrap_err();
        match err {
            SagoError::UnsupportedStateVersion { found, expected } => {
                assert_eq!(found, 99);
                assert_eq!(expected, CURRENT_SCHEMA_VERSION);
            }
            other => panic!("expected UnsupportedStateVersion error, got {:?}", other),
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
