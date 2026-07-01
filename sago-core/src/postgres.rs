use crate::{DataProvider, Result, SagoError, SchemaProvider};
use arrow::array::{
    ArrayBuilder, BooleanBuilder, Float32Builder, Float64Builder, Int16Builder, Int32Builder,
    Int64Builder, StringBuilder,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use sqlx::{Pool, Postgres, Row};
use std::sync::Arc;

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

        let mut builders: Vec<Box<dyn ArrayBuilder>> = Vec::new();
        for field in schema.fields() {
            let builder: Box<dyn ArrayBuilder> = match field.data_type() {
                DataType::Boolean => Box::new(BooleanBuilder::new()),
                DataType::Int16 => Box::new(Int16Builder::new()),
                DataType::Int32 => Box::new(Int32Builder::new()),
                DataType::Int64 => Box::new(Int64Builder::new()),
                DataType::Float32 => Box::new(Float32Builder::new()),
                DataType::Float64 => Box::new(Float64Builder::new()),
                DataType::Utf8 => Box::new(StringBuilder::new()),
                _ => Box::new(StringBuilder::new()), // Fallback
            };
            builders.push(builder);
        }

        for row in rows {
            for (i, field) in schema.fields().iter().enumerate() {
                let col_name = field.name();
                match field.data_type() {
                    DataType::Boolean => {
                        let val: Option<bool> = row.try_get(col_name.as_str()).ok();
                        builders[i]
                            .as_any_mut()
                            .downcast_mut::<BooleanBuilder>()
                            .unwrap()
                            .append_option(val);
                    }
                    DataType::Int16 => {
                        let val: Option<i16> = row.try_get(col_name.as_str()).ok();
                        builders[i]
                            .as_any_mut()
                            .downcast_mut::<Int16Builder>()
                            .unwrap()
                            .append_option(val);
                    }
                    DataType::Int32 => {
                        let val: Option<i32> = row.try_get(col_name.as_str()).ok();
                        builders[i]
                            .as_any_mut()
                            .downcast_mut::<Int32Builder>()
                            .unwrap()
                            .append_option(val);
                    }
                    DataType::Int64 => {
                        let val: Option<i64> = row.try_get(col_name.as_str()).ok();
                        builders[i]
                            .as_any_mut()
                            .downcast_mut::<Int64Builder>()
                            .unwrap()
                            .append_option(val);
                    }
                    DataType::Float32 => {
                        let val: Option<f32> = row.try_get(col_name.as_str()).ok();
                        builders[i]
                            .as_any_mut()
                            .downcast_mut::<Float32Builder>()
                            .unwrap()
                            .append_option(val);
                    }
                    DataType::Float64 => {
                        let val: Option<f64> = row.try_get(col_name.as_str()).ok();
                        builders[i]
                            .as_any_mut()
                            .downcast_mut::<Float64Builder>()
                            .unwrap()
                            .append_option(val);
                    }
                    DataType::Utf8 => {
                        let val: Option<String> = row.try_get(col_name.as_str()).ok();
                        builders[i]
                            .as_any_mut()
                            .downcast_mut::<StringBuilder>()
                            .unwrap()
                            .append_option(val);
                    }
                    _ => {
                        // For fallback, try to get as string
                        let val: Option<String> = row.try_get::<String, _>(col_name.as_str()).ok();
                        builders[i]
                            .as_any_mut()
                            .downcast_mut::<StringBuilder>()
                            .unwrap()
                            .append_option(val);
                    }
                }
            }
        }

        let columns = builders.into_iter().map(|mut b| b.finish()).collect();
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
}
