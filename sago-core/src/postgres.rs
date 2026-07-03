use crate::{DataProvider, Result, SagoError, SchemaProvider};
use arrow::array::{
    ArrayRef, BinaryBuilder, BooleanBuilder, Date32Builder, Float32Builder, Float64Builder,
    Int16Builder, Int32Builder, Int64Builder, StringBuilder, TimestampNanosecondArray,
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
fn pg_f64(row: &PgRow, name: &str) -> Option<f64> {
    if let Ok(v) = row.try_get::<Option<f64>, _>(name) {
        return v;
    }
    row.try_get::<Option<sqlx::types::BigDecimal>, _>(name)
        .ok()
        .flatten()
        .and_then(|d| d.to_string().parse::<f64>().ok())
}

/// A NULL, or a decode error, both surface as `None` for column `name`.
fn opt<T>(row: &PgRow, name: &str) -> Option<T>
where
    T: for<'r> sqlx::Decode<'r, Postgres> + sqlx::Type<Postgres>,
{
    row.try_get::<Option<T>, _>(name).ok().flatten()
}

/// Materialize one Arrow column for `field` from every row, honouring the exact
/// Arrow type Sago declared for it (including timestamp timezone metadata) so the
/// resulting array always matches the schema `RecordBatch::try_new` validates against.
fn build_column(rows: &[PgRow], field: &Field) -> ArrayRef {
    let name = field.name().as_str();
    match field.data_type() {
        DataType::Boolean => {
            let mut b = BooleanBuilder::with_capacity(rows.len());
            for row in rows {
                b.append_option(opt::<bool>(row, name));
            }
            Arc::new(b.finish())
        }
        DataType::Int16 => {
            let mut b = Int16Builder::with_capacity(rows.len());
            for row in rows {
                b.append_option(opt::<i16>(row, name));
            }
            Arc::new(b.finish())
        }
        DataType::Int32 => {
            let mut b = Int32Builder::with_capacity(rows.len());
            for row in rows {
                b.append_option(opt::<i32>(row, name));
            }
            Arc::new(b.finish())
        }
        DataType::Int64 => {
            let mut b = Int64Builder::with_capacity(rows.len());
            for row in rows {
                b.append_option(opt::<i64>(row, name));
            }
            Arc::new(b.finish())
        }
        DataType::Float32 => {
            let mut b = Float32Builder::with_capacity(rows.len());
            for row in rows {
                b.append_option(opt::<f32>(row, name));
            }
            Arc::new(b.finish())
        }
        DataType::Float64 => {
            let mut b = Float64Builder::with_capacity(rows.len());
            for row in rows {
                b.append_option(pg_f64(row, name));
            }
            Arc::new(b.finish())
        }
        DataType::Date32 => {
            let mut b = Date32Builder::with_capacity(rows.len());
            for row in rows {
                b.append_option(opt::<NaiveDate>(row, name).map(date_to_days));
            }
            Arc::new(b.finish())
        }
        DataType::Timestamp(TimeUnit::Nanosecond, tz) => {
            let mut vals: Vec<Option<i64>> = Vec::with_capacity(rows.len());
            for row in rows {
                let nanos = if tz.is_some() {
                    opt::<DateTime<Utc>>(row, name).and_then(|d| d.timestamp_nanos_opt())
                } else {
                    opt::<NaiveDateTime>(row, name).and_then(naive_datetime_to_nanos)
                };
                vals.push(nanos);
            }
            Arc::new(TimestampNanosecondArray::from(vals).with_timezone_opt(tz.clone()))
        }
        DataType::Binary => {
            let mut b = BinaryBuilder::new();
            for row in rows {
                b.append_option(opt::<Vec<u8>>(row, name));
            }
            Arc::new(b.finish())
        }
        // Utf8 and anything Sago fell back to Utf8 for: read as text.
        _ => {
            let mut b = StringBuilder::new();
            for row in rows {
                b.append_option(opt::<String>(row, name));
            }
            Arc::new(b.finish())
        }
    }
}

pub struct PostgresSchemaProvider {
    pool: Pool<Postgres>,
}

impl PostgresSchemaProvider {
    pub fn new(pool: Pool<Postgres>) -> Self {
        Self { pool }
    }

    pub(crate) fn quote_identifier(identifier: &str) -> String {
        identifier
            .split('.')
            .map(|part| part.replace('"', "\"\""))
            .map(|part| format!("\"{}\"", part))
            .collect::<Vec<_>>()
            .join(".")
    }

    /// Split a table identifier into `(schema, table)`, defaulting the schema to
    /// `public`. Accepts only `table` or `schema.table`; anything else is an
    /// error rather than a silent truncation.
    pub(crate) fn split_identifier(identifier: &str) -> Result<(&str, &str)> {
        match identifier.split('.').collect::<Vec<_>>()[..] {
            [table] => Ok(("public", table)),
            [schema, table] => Ok((schema, table)),
            _ => Err(SagoError::Config(format!(
                "invalid Postgres identifier '{identifier}': expected 'table' or 'schema.table'"
            ))),
        }
    }

    pub(crate) fn map_postgres_type(data_type: &str) -> DataType {
        match data_type {
            "boolean" => DataType::Boolean,
            "smallint" | "int2" | "smallserial" => DataType::Int16,
            "integer" | "int4" | "serial" => DataType::Int32,
            "bigint" | "int8" | "bigserial" => DataType::Int64,
            "real" | "float4" => DataType::Float32,
            "double precision" | "float8" => DataType::Float64,
            "numeric" | "decimal" => DataType::Float64, // Simplified mapping
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
            "SELECT column_name, data_type, is_nullable 
             FROM information_schema.columns 
             WHERE table_schema = $1 AND table_name = $2 
             ORDER BY ordinal_position",
        )
        .bind(schema_name)
        .bind(table_name)
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

                let is_nullable = is_nullable_str == "YES";
                let data_type = Self::map_postgres_type(&data_type_str);

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
            .collect();
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
            ("public", "users")
        );
    }

    #[test]
    fn test_split_identifier_schema_dot_table() {
        assert_eq!(
            PostgresSchemaProvider::split_identifier("analytics.events").unwrap(),
            ("analytics", "events")
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
