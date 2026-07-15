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
        DataType::Int8 => "Int8".to_string(),
        DataType::Int16 => "Int16".to_string(),
        DataType::Int32 => "Int32".to_string(),
        DataType::Int64 => "Int64".to_string(),
        DataType::UInt8 => "UInt8".to_string(),
        DataType::UInt16 => "UInt16".to_string(),
        DataType::UInt32 => "UInt32".to_string(),
        DataType::UInt64 => "UInt64".to_string(),
        DataType::Float32 => "Float32".to_string(),
        DataType::Float64 => "Float64".to_string(),
        DataType::Utf8 => "Utf8".to_string(),
        DataType::LargeUtf8 => "LargeUtf8".to_string(),
        DataType::Binary => "Binary".to_string(),
        DataType::LargeBinary => "LargeBinary".to_string(),
        DataType::FixedSizeBinary(len) => format!("FixedSizeBinary({len})"),
        DataType::Date32 => "Date32".to_string(),
        DataType::Date64 => "Date64".to_string(),
        DataType::Time32(unit) => format!("Time32({})", time_unit_name(*unit)),
        DataType::Time64(unit) => format!("Time64({})", time_unit_name(*unit)),
        DataType::Decimal128(precision, scale) => format!("Decimal128({precision}, {scale})"),
        DataType::Decimal256(precision, scale) => format!("Decimal256({precision}, {scale})"),
        DataType::Timestamp(unit, None) => format!("Timestamp({}, None)", time_unit_name(*unit)),
        DataType::Timestamp(unit, Some(tz)) => {
            format!("Timestamp({}, Some(\"{tz}\"))", time_unit_name(*unit))
        }
        // For any other type, fall back to the Debug form. It will not parse back
        // (parse_data_type returns UnsupportedDataType), which is the same
        // behaviour as before — but callers get a faithful string for display.
        other => format!("{other:?}"),
    }
}

fn time_unit_name(unit: TimeUnit) -> &'static str {
    match unit {
        TimeUnit::Second => "Second",
        TimeUnit::Millisecond => "Millisecond",
        TimeUnit::Microsecond => "Microsecond",
        TimeUnit::Nanosecond => "Nanosecond",
    }
}

fn parse_time_unit(s: &str) -> Result<TimeUnit> {
    match s {
        "Second" => Ok(TimeUnit::Second),
        "Millisecond" => Ok(TimeUnit::Millisecond),
        "Microsecond" => Ok(TimeUnit::Microsecond),
        "Nanosecond" => Ok(TimeUnit::Nanosecond),
        other => Err(SagoError::UnsupportedDataType(format!(
            "unknown time unit: {other}"
        ))),
    }
}

/// Parse a canonical string form (as produced by [`serialize_data_type`]) back
/// into a [`DataType`].
pub fn parse_data_type(s: &str) -> Result<DataType> {
    match s {
        "Boolean" => return Ok(DataType::Boolean),
        "Int8" => return Ok(DataType::Int8),
        "Int16" => return Ok(DataType::Int16),
        "Int32" => return Ok(DataType::Int32),
        "Int64" => return Ok(DataType::Int64),
        "UInt8" => return Ok(DataType::UInt8),
        "UInt16" => return Ok(DataType::UInt16),
        "UInt32" => return Ok(DataType::UInt32),
        "UInt64" => return Ok(DataType::UInt64),
        "Float32" => return Ok(DataType::Float32),
        "Float64" => return Ok(DataType::Float64),
        "Utf8" => return Ok(DataType::Utf8),
        "LargeUtf8" => return Ok(DataType::LargeUtf8),
        "Binary" => return Ok(DataType::Binary),
        "LargeBinary" => return Ok(DataType::LargeBinary),
        "Date32" => return Ok(DataType::Date32),
        "Date64" => return Ok(DataType::Date64),
        _ => {}
    }

    if let Some(inner) = strip_wrapped(s, "FixedSizeBinary(", ")") {
        let len = inner
            .parse::<i32>()
            .map_err(|_| SagoError::UnsupportedDataType(s.to_string()))?;
        return Ok(DataType::FixedSizeBinary(len));
    }
    if let Some(inner) = strip_wrapped(s, "Time32(", ")") {
        return Ok(DataType::Time32(parse_time_unit(inner)?));
    }
    if let Some(inner) = strip_wrapped(s, "Time64(", ")") {
        return Ok(DataType::Time64(parse_time_unit(inner)?));
    }
    if let Some(inner) = strip_wrapped(s, "Decimal128(", ")") {
        let (precision, scale) = parse_decimal_args(inner, s)?;
        return Ok(DataType::Decimal128(precision, scale));
    }
    if let Some(inner) = strip_wrapped(s, "Decimal256(", ")") {
        let (precision, scale) = parse_decimal_args(inner, s)?;
        return Ok(DataType::Decimal256(precision, scale));
    }
    if let Some(inner) = strip_wrapped(s, "Timestamp(", ")") {
        let (unit_str, rest) = inner
            .split_once(", ")
            .ok_or_else(|| SagoError::UnsupportedDataType(s.to_string()))?;
        let unit = parse_time_unit(unit_str)?;
        if rest == "None" {
            return Ok(DataType::Timestamp(unit, None));
        }
        if let Some(tz) = strip_wrapped(rest, "Some(\"", "\")") {
            return Ok(DataType::Timestamp(unit, Some(tz.into())));
        }
        return Err(SagoError::UnsupportedDataType(s.to_string()));
    }

    Err(SagoError::UnsupportedDataType(s.to_string()))
}

/// If `s` starts with `prefix` and ends with `suffix`, returns the substring
/// between them; otherwise `None`.
fn strip_wrapped<'a>(s: &'a str, prefix: &str, suffix: &str) -> Option<&'a str> {
    s.strip_prefix(prefix).and_then(|r| r.strip_suffix(suffix))
}

fn parse_decimal_args(inner: &str, original: &str) -> Result<(u8, i8)> {
    let (precision_str, scale_str) = inner
        .split_once(", ")
        .ok_or_else(|| SagoError::UnsupportedDataType(original.to_string()))?;
    let precision = precision_str
        .parse::<u8>()
        .map_err(|_| SagoError::UnsupportedDataType(original.to_string()))?;
    let scale = scale_str
        .parse::<i8>()
        .map_err(|_| SagoError::UnsupportedDataType(original.to_string()))?;
    Ok((precision, scale))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every type the codec claims to support must round-trip losslessly.
    #[test]
    fn test_round_trip_all_supported_types() {
        let types = [
            DataType::Boolean,
            DataType::Int8,
            DataType::Int16,
            DataType::Int32,
            DataType::Int64,
            DataType::UInt8,
            DataType::UInt16,
            DataType::UInt32,
            DataType::UInt64,
            DataType::Float32,
            DataType::Float64,
            DataType::Utf8,
            DataType::LargeUtf8,
            DataType::Binary,
            DataType::LargeBinary,
            DataType::FixedSizeBinary(4),
            DataType::Date32,
            DataType::Date64,
            DataType::Time32(TimeUnit::Second),
            DataType::Time64(TimeUnit::Microsecond),
            DataType::Decimal128(38, 10),
            DataType::Decimal256(50, 20),
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

    #[test]
    fn test_timestamp_non_nanosecond_round_trips() {
        for unit in [
            TimeUnit::Second,
            TimeUnit::Millisecond,
            TimeUnit::Microsecond,
            TimeUnit::Nanosecond,
        ] {
            for tz in [None, Some("+00:00".into())] {
                let dt = DataType::Timestamp(unit, tz);
                let s = serialize_data_type(&dt);
                let back = parse_data_type(&s).unwrap_or_else(|_| panic!("failed to parse {s}"));
                assert_eq!(back, dt, "round-trip mismatch for {dt:?} -> {s}");
            }
        }
    }
}
