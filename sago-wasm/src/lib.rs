//! WebAssembly bindings for Sago's pure-analysis core.
//!
//! This crate exposes the parts of `sago-core` that need no I/O — semantic type
//! inference, three-way schema merge, and Merkle commitments — to JavaScript via
//! `wasm-bindgen`, so they can run in a browser or at the edge. It depends on
//! `sago-core` with `default-features = false`, which excludes the PostgreSQL /
//! S3 providers and the async runtime, leaving a `wasm32-unknown-unknown`-clean
//! build.
//!
//! Structured values cross the boundary as JSON-compatible objects via
//! `serde-wasm-bindgen`; scalars use plain strings.

use arrow::array::StringArray;
use arrow::datatypes::{Field, Schema};
use serde::{Deserialize, Serialize};
use wasm_bindgen::prelude::*;

use sago_core::merge::{MergeConflict, three_way_merge};
use sago_core::merkle::MerkleTree;
use sago_core::schema_codec::{parse_data_type, serialize_data_type};
use sago_core::semantic::infer_semantic_type;

/// Installs a panic hook that forwards Rust panic messages to the browser's
/// `console.error`, run automatically once per module instantiation via
/// `#[wasm_bindgen(start)]`. Without this, a panic anywhere in the dependency
/// chain (e.g. an Arrow allocation/formatting panic) surfaces to JS only as
/// an opaque `RuntimeError: unreachable executed` with no message or Rust
/// stack context — undiagnosable from the JS side, unlike this crate's own
/// careful use of `Result<JsValue, JsValue>` everywhere else.
#[wasm_bindgen(start)]
pub fn init_panic_hook() {
    console_error_panic_hook::set_once();
}

/// A JS-facing field definition: name, Arrow data type (debug form, e.g.
/// "Int64", "Utf8"), and nullability.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WasmField {
    pub name: String,
    pub data_type: String,
    #[serde(default)]
    pub nullable: bool,
}

/// The result of a three-way merge, in JS-friendly form.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WasmMergeResult {
    pub merged: Vec<WasmField>,
    pub conflicts: Vec<MergeConflict>,
    pub clean: bool,
}

/// Infer the semantic type of a column from its name and a sample of string
/// values. Returns the semantic type name (e.g. `"Email"`, `"Unknown"`).
#[wasm_bindgen]
pub fn infer_semantic(name: &str, values: Vec<String>) -> String {
    let refs: Vec<&str> = values.iter().map(|s| s.as_str()).collect();
    let array = StringArray::from(refs);
    let semantic = infer_semantic_type(name, &array);
    format!("{semantic:?}")
}

/// Compute the hex-encoded Merkle root committing to an ordered list of records.
#[wasm_bindgen]
pub fn merkle_root(records: Vec<String>) -> String {
    MerkleTree::from_records(records).root_hex()
}

/// Three-way merge of `base` / `ours` / `theirs` schemas (each a JS array of
/// `{name, data_type, nullable}`), returning the merged fields plus any
/// conflicts. Inputs/outputs are passed as JS values via serde.
#[wasm_bindgen]
pub fn merge_schemas(base: JsValue, ours: JsValue, theirs: JsValue) -> Result<JsValue, JsValue> {
    let base: Vec<WasmField> = serde_wasm_bindgen::from_value(base)?;
    let ours: Vec<WasmField> = serde_wasm_bindgen::from_value(ours)?;
    let theirs: Vec<WasmField> = serde_wasm_bindgen::from_value(theirs)?;

    let base = build_schema(&base).map_err(to_js_err)?;
    let ours = build_schema(&ours).map_err(to_js_err)?;
    let theirs = build_schema(&theirs).map_err(to_js_err)?;

    let result = three_way_merge(&base, &ours, &theirs);
    let out = WasmMergeResult {
        merged: result
            .merged
            .fields()
            .iter()
            .map(|f| WasmField {
                name: f.name().clone(),
                data_type: serialize_data_type(f.data_type()),
                nullable: f.is_nullable(),
            })
            .collect(),
        clean: result.is_clean(),
        conflicts: result.conflicts,
    };
    Ok(serde_wasm_bindgen::to_value(&out)?)
}

fn to_js_err(msg: String) -> JsValue {
    JsValue::from_str(&msg)
}

/// Build an Arrow [`Schema`] from JS field definitions, parsing type names via
/// the shared [`sago_core::schema_codec`] so the wasm crate and the native state
/// module accept exactly the same type strings.
fn build_schema(fields: &[WasmField]) -> Result<Schema, String> {
    let parsed: Result<Vec<Field>, String> = fields
        .iter()
        .map(|f| {
            let dt = parse_data_type(&f.data_type).map_err(|e| e.to_string())?;
            Ok(Field::new(&f.name, dt, f.nullable))
        })
        .collect();
    Ok(Schema::new(parsed?))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_infer_semantic_email() {
        let values = vec![
            "a@x.com".to_string(),
            "b@x.com".to_string(),
            "c@x.com".to_string(),
        ];
        assert_eq!(infer_semantic("contact", values), "Email");
    }

    #[test]
    fn test_infer_semantic_unknown() {
        let values = vec!["foo".to_string(), "bar".to_string()];
        assert_eq!(infer_semantic("misc", values), "Unknown");
    }

    #[test]
    fn test_infer_semantic_empty_values_does_not_panic() {
        // A JS caller may pass an empty sample; this must not panic.
        assert_eq!(infer_semantic("col", Vec::new()), "Unknown");
    }

    #[test]
    fn test_merkle_root_empty_is_stable_hex() {
        let root = merkle_root(Vec::new());
        assert_eq!(root.len(), 64);
    }

    #[test]
    fn test_merkle_root_deterministic_and_hex() {
        let a = merkle_root(vec!["x".into(), "y".into(), "z".into()]);
        let b = merkle_root(vec!["x".into(), "y".into(), "z".into()]);
        assert_eq!(a, b);
        assert_eq!(a.len(), 64);
        assert!(a.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn test_merkle_root_order_sensitive() {
        let a = merkle_root(vec!["x".into(), "y".into()]);
        let b = merkle_root(vec!["y".into(), "x".into()]);
        assert_ne!(a, b);
    }

    #[test]
    fn test_build_schema_and_merge_clean() {
        let base = vec![WasmField {
            name: "id".into(),
            data_type: "Int64".into(),
            nullable: false,
        }];
        let ours = vec![
            WasmField {
                name: "id".into(),
                data_type: "Int64".into(),
                nullable: false,
            },
            WasmField {
                name: "email".into(),
                data_type: "Utf8".into(),
                nullable: true,
            },
        ];
        let theirs = base.clone();

        let b = build_schema(&base).unwrap();
        let o = build_schema(&ours).unwrap();
        let t = build_schema(&theirs).unwrap();
        let result = three_way_merge(&b, &o, &t);
        assert!(result.is_clean());
        assert_eq!(result.merged.fields().len(), 2);
    }

    #[test]
    fn test_build_schema_rejects_unknown_type() {
        let fields = vec![WasmField {
            name: "x".into(),
            data_type: "List".into(),
            nullable: false,
        }];
        assert!(build_schema(&fields).is_err());
    }

    #[test]
    fn test_build_schema_error_names_the_bad_type() {
        // The error surfaced to JS (via to_js_err) must carry the offending type
        // string so a caller can diagnose it, not an opaque failure.
        let fields = vec![WasmField {
            name: "x".into(),
            data_type: "NotAType".into(),
            nullable: false,
        }];
        let err = build_schema(&fields).unwrap_err();
        assert!(
            err.contains("NotAType"),
            "error should name the bad type, got: {err}"
        );
    }

    #[test]
    fn test_merge_reports_conflict_without_erroring() {
        // A genuine add/add conflict is data, not an error: merge_schemas returns
        // a result whose `clean` is false, rather than failing the call.
        let base: Vec<WasmField> = vec![];
        let mk = |dt: &str| {
            vec![WasmField {
                name: "tag".into(),
                data_type: dt.into(),
                nullable: false,
            }]
        };
        let b = build_schema(&base).unwrap();
        let o = build_schema(&mk("Utf8")).unwrap();
        let t = build_schema(&mk("Int64")).unwrap();
        let result = three_way_merge(&b, &o, &t);
        assert!(!result.is_clean());
        assert_eq!(result.conflicts.len(), 1);
    }
}
