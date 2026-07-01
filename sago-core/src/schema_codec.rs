//! Canonical string codec for Arrow [`DataType`]s.
//!
//! Snapshots, rename profiles, and the wasm bindings all need to round-trip an
//! Arrow `DataType` through a string. Historically each site did this with an
//! ad-hoc `format!("{:?}", dt)` on the way out and a hand-written `match` on the
//! way back — three whitelists that silently diverged (the wasm parser, for
//! instance, was missing the `Timestamp` arms that the state parser had). This
//! module is the single source of truth for both directions so they cannot drift
//! apart, and so callers stop depending on the stability of Arrow's `Debug`
//! format.
//!
//! It is deliberately **ungated** (no `io` feature) because both the native
//! state module and the wasm crate consume it.

use arrow::datatypes::{DataType, TimeUnit};

use crate::{Result, SagoError};

/// Serialize a [`DataType`] to its canonical string form.
///
/// The encoding matches Arrow's `Debug` representation for the supported
/// primitive types (so it is stable across existing persisted snapshots), but is
/// produced explicitly here rather than via `{:?}` so the set of supported types
/// is defined in exactly one place.
pub fn serialize_data_type(dt: &DataType) -> String {
    match dt {
        DataType::Boolean => "Boolean".to_string(),
        DataType::Int16 => "Int16".to_string(),
        DataType::Int32 => "Int32".to_string(),
        DataType::Int64 => "Int64".to_string(),
        DataType::Float32 => "Float32".to_string(),
        DataType::Float64 => "Float64".to_string(),
        DataType::Utf8 => "Utf8".to_string(),
        DataType::LargeUtf8 => "LargeUtf8".to_string(),
        DataType::Binary => "Binary".to_string(),
        DataType::Date32 => "Date32".to_string(),
        DataType::Timestamp(TimeUnit::Nanosecond, None) => {
            "Timestamp(Nanosecond, None)".to_string()
        }
        DataType::Timestamp(TimeUnit::Nanosecond, Some(tz)) => {
            format!("Timestamp(Nanosecond, Some(\"{tz}\"))")
        }
        // For any other type, fall back to the Debug form. It will not parse back
        // (parse_data_type returns UnsupportedDataType), which is the same
        // behaviour as before — but callers get a faithful string for display.
        other => format!("{other:?}"),
    }
}

/// Parse a canonical string form (as produced by [`serialize_data_type`]) back
/// into a [`DataType`].
pub fn parse_data_type(s: &str) -> Result<DataType> {
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
        "Timestamp(Nanosecond, None)" => Ok(DataType::Timestamp(TimeUnit::Nanosecond, None)),
        other if other.starts_with("Timestamp(Nanosecond, Some(") => {
            // Debug repr: Timestamp(Nanosecond, Some("tz"))
            let tz = other
                .trim_start_matches("Timestamp(Nanosecond, Some(\"")
                .trim_end_matches("\"))")
                .to_string();
            Ok(DataType::Timestamp(TimeUnit::Nanosecond, Some(tz.into())))
        }
        other => Err(SagoError::UnsupportedDataType(other.to_string())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every type the codec claims to support must round-trip losslessly.
    #[test]
    fn test_round_trip_all_supported_types() {
        let types = [
            DataType::Boolean,
            DataType::Int16,
            DataType::Int32,
            DataType::Int64,
            DataType::Float32,
            DataType::Float64,
            DataType::Utf8,
            DataType::LargeUtf8,
            DataType::Binary,
            DataType::Date32,
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            DataType::Timestamp(TimeUnit::Nanosecond, Some("+00:00".into())),
        ];
        for dt in types {
            let s = serialize_data_type(&dt);
            let back = parse_data_type(&s).unwrap_or_else(|_| panic!("failed to parse {s}"));
            assert_eq!(back, dt, "round-trip mismatch for {dt:?} -> {s}");
        }
    }

    /// The canonical encoding must equal Arrow's Debug form for supported types,
    /// so previously-persisted snapshots (written via `format!("{:?}", ..)`) load.
    #[test]
    fn test_encoding_matches_debug_for_supported_types() {
        let types = [
            DataType::Int64,
            DataType::Utf8,
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            DataType::Timestamp(TimeUnit::Nanosecond, Some("+00:00".into())),
        ];
        for dt in types {
            assert_eq!(serialize_data_type(&dt), format!("{dt:?}"));
        }
    }

    #[test]
    fn test_unsupported_type_errors() {
        let err = parse_data_type("List(Int32)").unwrap_err();
        assert!(matches!(err, SagoError::UnsupportedDataType(_)));
    }
}
