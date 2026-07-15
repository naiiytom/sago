use crate::{DataProvider, Result, SagoError, SchemaProvider};
use arrow::array::{
    ArrayRef, BinaryBuilder, BooleanBuilder, Date32Builder, Decimal128Builder, Float32Builder,
    Float64Builder, Int16Builder, Int32Builder, Int64Builder, StringBuilder,
    TimestampNanosecondArray,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use chrono::{DateTime, NaiveDate, NaiveDateTime, Utc};
use sqlx::postgres::PgRow;
use sqlx::{Pool, Postgres, Row};
use std::sync::Arc;

/// Days between the Unix epoch and `d`, the encoding Arrow's `Date32` uses.
fn date_to_days(d: NaiveDate) -> i32 {
    // `from_ymd_opt(1970, 1, 1)` is always valid; the panic is unreachable.
    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).expect("epoch date is valid");
    (d - epoch).num_days() as i32
}

/// Nanoseconds since the Unix epoch for a timezone-naive timestamp, interpreting
/// it as UTC (Postgres `timestamp without time zone` carries no offset). Returns
/// `None` only for timestamps outside the ~584-year range representable in i64ns.
fn naive_datetime_to_nanos(dt: NaiveDateTime) -> Option<i64> {
    dt.and_utc().timestamp_nanos_opt()
}

/// Best-effort `f64` for a column that Sago maps to `Float64`. Postgres
/// `double precision` decodes directly; `numeric`/`decimal` cannot be read as
/// `f64` by sqlx, so fall back to decoding the arbitrary-precision value and
/// narrowing it (lossy, but the column is already declared `Float64`).
fn pg_f64(row: &PgRow, name: &str) -> Result<Option<f64>> {
    if let Ok(v) = row.try_get::<Option<f64>, _>(name) {
        return Ok(v);
    }
    row.try_get::<Option<sqlx::types::BigDecimal>, _>(name)
        .map_err(SagoError::Database)
        .map(|opt| opt.and_then(|d| d.to_string().parse::<f64>().ok()))
}

/// The unscaled `i128` representation of `d` at exactly `target_scale`
/// decimal places — the encoding Arrow's `Decimal128Array` uses (a value `v`
/// represents `v / 10^target_scale`). `with_scale_round(_, HalfUp)` rescales
/// (rounding half-away-from-zero, matching Postgres `numeric`'s own rounding
/// convention, if `target_scale` has fewer digits than `d`'s own scale)
/// before extracting the digits, so the result is always exact at the
/// declared column scale even if Postgres returned a different number of
/// decimal digits for a particular row (`numeric` doesn't pad to its
/// declared scale the way a fixed-point type would).
fn bigdecimal_to_i128(d: &sqlx::types::BigDecimal, target_scale: i8) -> i128 {
    let scaled = d.with_scale_round(target_scale as i64, bigdecimal::RoundingMode::HalfUp);
    let (digits, _exponent) = scaled.as_bigint_and_exponent();
    // as_bigint_and_exponent()'s exponent equals -target_scale here (by
    // construction of with_scale_round), so `digits` is already the value
    // scaled by exactly 10^target_scale — i.e. the Decimal128 unscaled
    // representation. `to_string().parse()` avoids depending on num-bigint's
    // ToPrimitive trait directly (bigdecimal re-exports the type but not
    // necessarily every trait impl at a version this crate pins to).
    digits.to_string().parse::<i128>().unwrap_or(0)
}

/// The value of column `name`, or `Ok(None)` for a genuine SQL `NULL`.
///
/// Distinguishes a NULL from a decode/type-compatibility error rather than
/// collapsing both into `None`: sqlx rejects decoding a column whose
/// Postgres OID it doesn't consider compatible with `T` (e.g. `uuid`,
/// `jsonb`, `inet`, `money`, `interval` decoded as `String`) *before* even
/// checking nullness, so silently mapping that to `None` would export an
/// entire non-null column as all-NULL with no error — indistinguishable from
/// genuinely empty data, and liable to show up as bogus `null_count_drift`
/// rather than the real problem (an unsupported column type).
fn opt<T>(row: &PgRow, name: &str) -> Result<Option<T>>
where
    T: for<'r> sqlx::Decode<'r, Postgres> + sqlx::Type<Postgres>,
{
    row.try_get::<Option<T>, _>(name)
        .map_err(SagoError::Database)
}

/// Materialize one Arrow column for `field` from every row, honouring the exact
/// Arrow type Sago declared for it (including timestamp timezone metadata) so the
/// resulting array always matches the schema `RecordBatch::try_new` validates against.
///
/// Returns an error (rather than silently exporting `None`s) if any row's
/// value can't be decoded as the target type — most commonly because
/// `map_postgres_type` mapped the column's real Postgres type (`uuid`,
/// `jsonb`, `inet`, `money`, `interval`, an enum, …) to `Utf8`, but sqlx's
/// `Type::compatible` check rejects decoding that OID as a plain `String`.
/// Collapsing that into `None` would silently export the entire column as
/// all-NULL, indistinguishable from genuinely empty data.
fn build_column(rows: &[PgRow], field: &Field) -> Result<ArrayRef> {
    let name = field.name().as_str();
    let array: ArrayRef = match field.data_type() {
        DataType::Boolean => {
            let mut b = BooleanBuilder::with_capacity(rows.len());
            for row in rows {
                b.append_option(opt::<bool>(row, name)?);
            }
            Arc::new(b.finish())
        }
        DataType::Int16 => {
            let mut b = Int16Builder::with_capacity(rows.len());
            for row in rows {
                b.append_option(opt::<i16>(row, name)?);
            }
            Arc::new(b.finish())
        }
        DataType::Int32 => {
            let mut b = Int32Builder::with_capacity(rows.len());
            for row in rows {
                b.append_option(opt::<i32>(row, name)?);
            }
            Arc::new(b.finish())
        }
        DataType::Int64 => {
            let mut b = Int64Builder::with_capacity(rows.len());
            for row in rows {
                b.append_option(opt::<i64>(row, name)?);
            }
            Arc::new(b.finish())
        }
        DataType::Float32 => {
            let mut b = Float32Builder::with_capacity(rows.len());
            for row in rows {
                b.append_option(opt::<f32>(row, name)?);
            }
            Arc::new(b.finish())
        }
        DataType::Float64 => {
            let mut b = Float64Builder::with_capacity(rows.len());
            for row in rows {
                b.append_option(pg_f64(row, name)?);
            }
            Arc::new(b.finish())
        }
        DataType::Date32 => {
            let mut b = Date32Builder::with_capacity(rows.len());
            for row in rows {
                b.append_option(opt::<NaiveDate>(row, name)?.map(date_to_days));
            }
            Arc::new(b.finish())
        }
        DataType::Timestamp(TimeUnit::Nanosecond, tz) => {
            let mut vals: Vec<Option<i64>> = Vec::with_capacity(rows.len());
            for row in rows {
                let nanos = if tz.is_some() {
                    opt::<DateTime<Utc>>(row, name)?.and_then(|d| d.timestamp_nanos_opt())
                } else {
                    opt::<NaiveDateTime>(row, name)?.and_then(naive_datetime_to_nanos)
                };
                vals.push(nanos);
            }
            Arc::new(TimestampNanosecondArray::from(vals).with_timezone_opt(tz.clone()))
        }
        DataType::Decimal128(precision, scale) => {
            let mut b = Decimal128Builder::with_capacity(rows.len())
                .with_precision_and_scale(*precision, *scale)?;
            for row in rows {
                let v = opt::<sqlx::types::BigDecimal>(row, name)?
                    .map(|d| bigdecimal_to_i128(&d, *scale));
                b.append_option(v);
            }
            Arc::new(b.finish())
        }
        DataType::Binary => {
            let mut b = BinaryBuilder::new();
            for row in rows {
                b.append_option(opt::<Vec<u8>>(row, name)?);
            }
            Arc::new(b.finish())
        }
        // Utf8 and anything Sago fell back to Utf8 for: read as text.
        _ => {
            let mut b = StringBuilder::new();
            for row in rows {
                b.append_option(opt::<String>(row, name)?);
            }
            Arc::new(b.finish())
        }
    };
    Ok(array)
}

pub struct PostgresSchemaProvider {
    pool: Pool<Postgres>,
}

impl PostgresSchemaProvider {
    pub fn new(pool: Pool<Postgres>) -> Self {
        Self { pool }
    }

    pub(crate) fn quote_identifier(identifier: &str) -> String {
        Self::split_dotted_segments(identifier)
            .into_iter()
            .map(|part| format!("\"{}\"", part.replace('"', "\"\"")))
            .collect::<Vec<_>>()
            .join(".")
    }

    /// Split `identifier` on `.` into its component parts, treating a
    /// backslash-escaped dot (`\.`) as a literal dot within a segment rather
    /// than a schema/table separator. This lets a table name that itself
    /// contains a literal dot — e.g. a Postgres table created as
    /// `"weird.table"` — be addressed unambiguously by writing
    /// `identifier = "weird\\.table"` in `Sago.toml` (schema-qualified:
    /// `identifier = "public.weird\\.table"`), rather than the bare dot
    /// always being read as a separator. A lone trailing backslash (no `.`
    /// following it) is kept as a literal backslash, since it can't be part
    /// of an escape sequence.
    fn split_dotted_segments(identifier: &str) -> Vec<String> {
        let mut segments = Vec::new();
        let mut current = String::new();
        let mut chars = identifier.chars().peekable();
        while let Some(c) = chars.next() {
            if c == '\\' && chars.peek() == Some(&'.') {
                current.push('.');
                chars.next();
            } else if c == '.' {
                segments.push(std::mem::take(&mut current));
            } else {
                current.push(c);
            }
        }
        segments.push(current);
        segments
    }

    /// Split a table identifier into `(schema, table)`, defaulting the schema to
    /// `public`. Accepts only `table` or `schema.table` (each part may contain a
    /// literal dot via `\.`, see [`split_dotted_segments`]); anything else is an
    /// error rather than a silent truncation.
    pub(crate) fn split_identifier(identifier: &str) -> Result<(String, String)> {
        match &Self::split_dotted_segments(identifier)[..] {
            [table] => Ok(("public".to_string(), table.clone())),
            [schema, table] => Ok((schema.clone(), table.clone())),
            _ => Err(SagoError::Config(format!(
                "invalid Postgres identifier '{identifier}': expected 'table' or 'schema.table'"
            ))),
        }
    }

    /// `map_postgres_type_with_precision` without precision/scale context,
    /// for tests that only care about the bare type name. `numeric`/`decimal`
    /// falls back to the lossy `Float64` mapping here, since there's no
    /// precision/scale to build an exact `Decimal128`.
    #[cfg(test)]
    pub(crate) fn map_postgres_type(data_type: &str) -> DataType {
        Self::map_postgres_type_with_precision(data_type, None, None)
    }

    /// Map a Postgres `information_schema.columns.data_type` to an Arrow
    /// type, using `numeric_precision`/`numeric_scale` (also from
    /// `information_schema.columns`) to map `numeric`/`decimal` to an exact
    /// `Decimal128` rather than a lossy `Float64` narrowing, when Postgres
    /// reports a precision that fits `Decimal128`'s 38-digit limit. A
    /// `numeric` column declared without precision (arbitrary precision,
    /// `numeric_precision` NULL) or with precision >38 has no bound that fits
    /// `Decimal128`, so it still falls back to `Float64`.
    pub(crate) fn map_postgres_type_with_precision(
        data_type: &str,
        precision: Option<i32>,
        scale: Option<i32>,
    ) -> DataType {
        match data_type {
            "boolean" => DataType::Boolean,
            "smallint" | "int2" | "smallserial" => DataType::Int16,
            "integer" | "int4" | "serial" => DataType::Int32,
            "bigint" | "int8" | "bigserial" => DataType::Int64,
            "real" | "float4" => DataType::Float32,
            "double precision" | "float8" => DataType::Float64,
            "numeric" | "decimal" => match (precision, scale) {
                (Some(p), Some(s))
                    if (1..=arrow::datatypes::DECIMAL128_MAX_PRECISION as i32).contains(&p)
                        && (0..=arrow::datatypes::DECIMAL128_MAX_SCALE as i32).contains(&s) =>
                {
                    DataType::Decimal128(p as u8, s as i8)
                }
                _ => DataType::Float64, // Arbitrary-precision or out-of-range: lossy fallback.
            },
            "character varying" | "varchar" | "text" | "character" | "char" => DataType::Utf8,
            "bytea" => DataType::Binary,
            "date" => DataType::Date32,
            "timestamp" | "timestamp without time zone" => {
                DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None)
            }
            "timestamp with time zone" => DataType::Timestamp(
                arrow::datatypes::TimeUnit::Nanosecond,
                Some("+00:00".into()),
            ),
            "json" | "jsonb" => DataType::Utf8,
            _ => DataType::Utf8, // Fallback to Utf8 for unknown types
        }
    }
}

#[async_trait]
impl SchemaProvider for PostgresSchemaProvider {
    async fn get_schema(&self, identifier: &str) -> Result<Schema> {
        // Accept exactly `table` (defaults to the `public` schema) or
        // `schema.table`. Anything with more dotted segments is rejected rather
        // than silently truncated — previously `a.b.c` kept only `a.b` and
        // dropped `c`, then get_data quoted the full string, so schema lookup and
        // data fetch disagreed.
        let (schema_name, table_name) = Self::split_identifier(identifier)?;

        let rows = sqlx::query(
            "SELECT column_name, data_type, is_nullable, numeric_precision, numeric_scale
             FROM information_schema.columns
             WHERE table_schema = $1 AND table_name = $2
             ORDER BY ordinal_position",
        )
        .bind(&schema_name)
        .bind(&table_name)
        .fetch_all(&self.pool)
        .await
        .map_err(SagoError::Database)?;

        if rows.is_empty() {
            return Err(SagoError::Schema(format!(
                "Table '{}' not found in schema '{}'",
                table_name, schema_name
            )));
        }

        let fields: Vec<Field> = rows
            .iter()
            .map(|row| {
                let name: String = row.get("column_name");
                let data_type_str: String = row.get("data_type");
                let is_nullable_str: String = row.get("is_nullable");
                let precision: Option<i32> = row.get("numeric_precision");
                let scale: Option<i32> = row.get("numeric_scale");

                let is_nullable = is_nullable_str == "YES";
                let data_type =
                    Self::map_postgres_type_with_precision(&data_type_str, precision, scale);

                Field::new(name, data_type, is_nullable)
            })
            .collect();

        Ok(Schema::new(fields))
    }
}

#[async_trait]
impl DataProvider for PostgresSchemaProvider {
    async fn get_data(&self, identifier: &str) -> Result<Vec<RecordBatch>> {
        let schema = self.get_schema(identifier).await?;

        let quoted_identifier = Self::quote_identifier(identifier);

        let query = format!("SELECT * FROM {}", quoted_identifier);

        let rows = sqlx::query(&query)
            .fetch_all(&self.pool)
            .await
            .map_err(SagoError::Database)?;

        if rows.is_empty() {
            return Ok(vec![]);
        }

        // Build each column in one pass so we can honour the full Arrow type Sago
        // declared — including Date32, Timestamp (with timezone metadata) and
        // Binary, which the old row-at-a-time StringBuilder fallback silently
        // dropped to all-null.
        let columns: Vec<ArrayRef> = schema
            .fields()
            .iter()
            .map(|field| build_column(&rows, field))
            .collect::<Result<_>>()?;
        let batch = RecordBatch::try_new(Arc::new(schema), columns)?;

        Ok(vec![batch])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_map_postgres_type_integers() {
        assert_eq!(
            PostgresSchemaProvider::map_postgres_type("smallint"),
            DataType::Int16
        );
        assert_eq!(
            PostgresSchemaProvider::map_postgres_type("int2"),
            DataType::Int16
        );
        assert_eq!(
            PostgresSchemaProvider::map_postgres_type("integer"),
            DataType::Int32
        );
        assert_eq!(
            PostgresSchemaProvider::map_postgres_type("int4"),
            DataType::Int32
        );
        assert_eq!(
            PostgresSchemaProvider::map_postgres_type("serial"),
            DataType::Int32
        );
        assert_eq!(
            PostgresSchemaProvider::map_postgres_type("bigint"),
            DataType::Int64
        );
        assert_eq!(
            PostgresSchemaProvider::map_postgres_type("int8"),
            DataType::Int64
        );
        assert_eq!(
            PostgresSchemaProvider::map_postgres_type("bigserial"),
            DataType::Int64
        );
    }

    #[test]
    fn test_map_postgres_type_floats() {
        assert_eq!(
            PostgresSchemaProvider::map_postgres_type("real"),
            DataType::Float32
        );
        assert_eq!(
            PostgresSchemaProvider::map_postgres_type("float4"),
            DataType::Float32
        );
        assert_eq!(
            PostgresSchemaProvider::map_postgres_type("double precision"),
            DataType::Float64
        );
        assert_eq!(
            PostgresSchemaProvider::map_postgres_type("float8"),
            DataType::Float64
        );
        assert_eq!(
            PostgresSchemaProvider::map_postgres_type("numeric"),
            DataType::Float64
        );
        assert_eq!(
            PostgresSchemaProvider::map_postgres_type("decimal"),
            DataType::Float64
        );
    }

    #[test]
    fn test_map_postgres_type_strings() {
        assert_eq!(
            PostgresSchemaProvider::map_postgres_type("character varying"),
            DataType::Utf8
        );
        assert_eq!(
            PostgresSchemaProvider::map_postgres_type("varchar"),
            DataType::Utf8
        );
        assert_eq!(
            PostgresSchemaProvider::map_postgres_type("text"),
            DataType::Utf8
        );
        assert_eq!(
            PostgresSchemaProvider::map_postgres_type("character"),
            DataType::Utf8
        );
        assert_eq!(
            PostgresSchemaProvider::map_postgres_type("char"),
            DataType::Utf8
        );
        assert_eq!(
            PostgresSchemaProvider::map_postgres_type("json"),
            DataType::Utf8
        );
        assert_eq!(
            PostgresSchemaProvider::map_postgres_type("jsonb"),
            DataType::Utf8
        );
    }

    #[test]
    fn test_map_postgres_type_other() {
        assert_eq!(
            PostgresSchemaProvider::map_postgres_type("boolean"),
            DataType::Boolean
        );
        assert_eq!(
            PostgresSchemaProvider::map_postgres_type("bytea"),
            DataType::Binary
        );
        assert_eq!(
            PostgresSchemaProvider::map_postgres_type("date"),
            DataType::Date32
        );
        assert!(matches!(
            PostgresSchemaProvider::map_postgres_type("timestamp"),
            DataType::Timestamp(_, _)
        ));
        assert!(matches!(
            PostgresSchemaProvider::map_postgres_type("timestamp without time zone"),
            DataType::Timestamp(_, _)
        ));
        assert!(matches!(
            PostgresSchemaProvider::map_postgres_type("timestamp with time zone"),
            DataType::Timestamp(_, Some(_))
        ));
    }

    // ── split_identifier ─────────────────────────────────────────────────────

    #[test]
    fn test_split_identifier_bare_table_defaults_public() {
        assert_eq!(
            PostgresSchemaProvider::split_identifier("users").unwrap(),
            ("public".to_string(), "users".to_string())
        );
    }

    #[test]
    fn test_split_identifier_schema_dot_table() {
        assert_eq!(
            PostgresSchemaProvider::split_identifier("analytics.events").unwrap(),
            ("analytics".to_string(), "events".to_string())
        );
    }

    #[test]
    fn test_split_identifier_extra_segments_rejected() {
        // Regression: `db.schema.table` must error, not silently drop `table`.
        let err = PostgresSchemaProvider::split_identifier("db.analytics.events").unwrap_err();
        match err {
            SagoError::Config(msg) => assert!(msg.contains("expected 'table' or 'schema.table'")),
            other => panic!("expected Config error, got {other:?}"),
        }
    }

    #[test]
    fn test_split_identifier_escaped_dot_is_literal() {
        // Regression: a table literally named `weird.table` must not be
        // mis-parsed into schema="weird", table="table". A backslash-escaped
        // dot disambiguates a literal dot from the schema/table separator.
        assert_eq!(
            PostgresSchemaProvider::split_identifier("weird\\.table").unwrap(),
            ("public".to_string(), "weird.table".to_string())
        );
    }

    #[test]
    fn test_split_identifier_schema_and_table_with_escaped_literal_dot() {
        assert_eq!(
            PostgresSchemaProvider::split_identifier("public.weird\\.table").unwrap(),
            ("public".to_string(), "weird.table".to_string())
        );
    }

    #[test]
    fn test_split_identifier_trailing_backslash_is_literal() {
        // A lone trailing backslash can't be an escape sequence (no `.`
        // follows it), so it must survive as a literal character.
        assert_eq!(
            PostgresSchemaProvider::split_identifier("name\\").unwrap(),
            ("public".to_string(), "name\\".to_string())
        );
    }

    // ── quote_identifier ─────────────────────────────────────────────────────

    #[test]
    fn test_quote_identifier_simple_name() {
        assert_eq!(
            PostgresSchemaProvider::quote_identifier("users"),
            "\"users\""
        );
    }

    #[test]
    fn test_quote_identifier_schema_dot_table() {
        assert_eq!(
            PostgresSchemaProvider::quote_identifier("public.users"),
            "\"public\".\"users\""
        );
    }

    #[test]
    fn test_quote_identifier_escapes_embedded_double_quote() {
        assert_eq!(
            PostgresSchemaProvider::quote_identifier("bad\"name"),
            "\"bad\"\"name\""
        );
    }

    #[test]
    fn test_quote_identifier_prevents_injection() {
        // A closing " would end a quoted identifier and allow SQL injection.
        // The quoting must escape " to "" so the whole string remains a single token.
        let malicious = "users\"; DROP TABLE users; --";
        let quoted = PostgresSchemaProvider::quote_identifier(malicious);
        // Wraps in outer quotes
        assert!(quoted.starts_with('"'));
        assert!(quoted.ends_with('"'));
        // The embedded " is doubled so it cannot break out of the identifier
        assert!(quoted.contains("\"\""));
        // The result equals wrapping the content with all " escaped
        let expected = format!("\"{}\"", malicious.replace('"', "\"\""));
        assert_eq!(quoted, expected);
    }

    // ── map_postgres_type ────────────────────────────────────────────────────

    #[test]
    fn test_map_postgres_type_round_trips_through_state_codec() {
        // Guard: every Arrow type that map_postgres_type can produce must
        // serialize+parse cleanly through schema_codec, so a snapshot captured
        // from Postgres always reloads. Catches divergence in CI rather than at
        // production load time.
        use crate::schema_codec::{parse_data_type, serialize_data_type};
        let pg_types = [
            "boolean",
            "smallint",
            "integer",
            "bigint",
            "real",
            "double precision",
            "numeric",
            "text",
            "bytea",
            "date",
            "timestamp",
            "timestamp with time zone",
            "json",
            "some_unknown_type",
        ];
        for pg in pg_types {
            let dt = PostgresSchemaProvider::map_postgres_type(pg);
            let s = serialize_data_type(&dt);
            let back = parse_data_type(&s)
                .unwrap_or_else(|e| panic!("pg type {pg} -> {dt:?} -> {s} failed to parse: {e}"));
            assert_eq!(back, dt, "round-trip mismatch for pg type {pg}");
        }
    }

    #[test]
    fn test_map_postgres_type_unknown_falls_back_to_utf8() {
        assert_eq!(
            PostgresSchemaProvider::map_postgres_type("uuid"),
            DataType::Utf8
        );
        assert_eq!(
            PostgresSchemaProvider::map_postgres_type("interval"),
            DataType::Utf8
        );
        assert_eq!(
            PostgresSchemaProvider::map_postgres_type("some_custom_type"),
            DataType::Utf8
        );
    }

    // ── map_postgres_type_with_precision (Decimal128) ────────────────────────

    #[test]
    fn test_numeric_with_precision_and_scale_maps_to_decimal128() {
        assert_eq!(
            PostgresSchemaProvider::map_postgres_type_with_precision(
                "numeric",
                Some(38),
                Some(10)
            ),
            DataType::Decimal128(38, 10)
        );
        assert_eq!(
            PostgresSchemaProvider::map_postgres_type_with_precision(
                "decimal",
                Some(10),
                Some(2)
            ),
            DataType::Decimal128(10, 2)
        );
    }

    #[test]
    fn test_numeric_without_precision_falls_back_to_float64() {
        // `numeric` with no declared precision (arbitrary precision) has no
        // bound that fits Decimal128 — must stay the lossy Float64 mapping.
        assert_eq!(
            PostgresSchemaProvider::map_postgres_type_with_precision("numeric", None, None),
            DataType::Float64
        );
    }

    #[test]
    fn test_numeric_precision_out_of_decimal128_range_falls_back_to_float64() {
        assert_eq!(
            PostgresSchemaProvider::map_postgres_type_with_precision(
                "numeric",
                Some(39), // exceeds DECIMAL128_MAX_PRECISION (38)
                Some(0)
            ),
            DataType::Float64
        );
    }

    #[test]
    fn test_decimal128_round_trips_through_state_codec() {
        use crate::schema_codec::{parse_data_type, serialize_data_type};
        let dt = PostgresSchemaProvider::map_postgres_type_with_precision(
            "numeric",
            Some(38),
            Some(10),
        );
        let s = serialize_data_type(&dt);
        let back = parse_data_type(&s).unwrap();
        assert_eq!(back, dt);
    }

    // ── BigDecimal -> Decimal128 unscaled i128 ────────────────────────────────

    #[test]
    fn test_bigdecimal_to_i128_matches_declared_scale() {
        use std::str::FromStr;
        let d = sqlx::types::BigDecimal::from_str("123.45").unwrap();
        assert_eq!(bigdecimal_to_i128(&d, 2), 12345);
        assert_eq!(bigdecimal_to_i128(&d, 4), 1234500);
    }

    #[test]
    fn test_bigdecimal_to_i128_negative_value() {
        use std::str::FromStr;
        let d = sqlx::types::BigDecimal::from_str("-42.5").unwrap();
        assert_eq!(bigdecimal_to_i128(&d, 2), -4250);
    }

    #[test]
    fn test_bigdecimal_to_i128_rescale_rounds_extra_digits() {
        // Postgres numeric doesn't pad to a fixed number of digits per row;
        // a value with more fractional digits than the column's declared
        // scale must round to fit rather than truncate silently wrong.
        use std::str::FromStr;
        let d = sqlx::types::BigDecimal::from_str("1.239").unwrap();
        assert_eq!(bigdecimal_to_i128(&d, 2), 124); // rounds to 1.24
    }

    #[test]
    fn test_bigdecimal_to_i128_high_precision_value() {
        // A realistic numeric(38,10) value: precision this large is exactly
        // the case Float64 would silently lose digits on.
        use std::str::FromStr;
        let d = sqlx::types::BigDecimal::from_str("123456789012345678.1234567890").unwrap();
        assert_eq!(bigdecimal_to_i128(&d, 10), 1234567890123456781234567890i128);
    }

    // ── date / timestamp encoding helpers ────────────────────────────────────

    #[test]
    fn test_date_to_days_epoch_is_zero() {
        let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
        assert_eq!(date_to_days(epoch), 0);
    }

    #[test]
    fn test_date_to_days_after_and_before_epoch() {
        assert_eq!(
            date_to_days(NaiveDate::from_ymd_opt(1970, 1, 2).unwrap()),
            1
        );
        assert_eq!(
            date_to_days(NaiveDate::from_ymd_opt(1969, 12, 31).unwrap()),
            -1
        );
        // 2000-01-01 is 30 years (with 7 leap days) after the epoch = 10957 days.
        assert_eq!(
            date_to_days(NaiveDate::from_ymd_opt(2000, 1, 1).unwrap()),
            10957
        );
    }

    #[test]
    fn test_naive_datetime_to_nanos_epoch_is_zero() {
        let dt = NaiveDate::from_ymd_opt(1970, 1, 1)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap();
        assert_eq!(naive_datetime_to_nanos(dt), Some(0));
    }

    #[test]
    fn test_naive_datetime_to_nanos_one_second() {
        let dt = NaiveDate::from_ymd_opt(1970, 1, 1)
            .unwrap()
            .and_hms_opt(0, 0, 1)
            .unwrap();
        assert_eq!(naive_datetime_to_nanos(dt), Some(1_000_000_000));
    }
}
