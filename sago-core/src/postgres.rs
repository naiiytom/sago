use arrow::array::{ArrayBuilder, BooleanBuilder, Float32Builder, Float64Builder, Int16Builder, Int32Builder, Int64Builder, StringBuilder};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use crate::{Result, SchemaProvider, DataProvider, SagoError};
use sqlx::{Pool, Postgres, Row};
use std::sync::Arc;

pub struct PostgresSchemaProvider {
    pool: Pool<Postgres>,
}

impl PostgresSchemaProvider {
    pub fn new(pool: Pool<Postgres>) -> Self {
        Self { pool }
    }

    pub(crate) fn map_postgres_type(data_type: &str) -> DataType {
        match data_type {
            "boolean" => DataType::Boolean,
            "smallint" | "int2" => DataType::Int16,
            "integer" | "int4" | "serial" => DataType::Int32,
            "bigint" | "int8" | "bigserial" => DataType::Int64,
            "real" | "float4" => DataType::Float32,
            "double precision" | "float8" => DataType::Float64,
            "numeric" | "decimal" => DataType::Float64, // Simplified mapping
            "character varying" | "varchar" | "text" | "character" | "char" => DataType::Utf8,
            "bytea" => DataType::Binary,
            "date" => DataType::Date32,
            "timestamp" | "timestamp without time zone" => DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None),
            "timestamp with time zone" => DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, Some("+00:00".into())),
            "json" | "jsonb" => DataType::Utf8,
            _ => DataType::Utf8, // Fallback to Utf8 for unknown types
        }
    }
}

#[async_trait]
impl SchemaProvider for PostgresSchemaProvider {
    async fn get_schema(&self, identifier: &str) -> Result<Schema> {
        let (schema_name, table_name) = if identifier.contains('.') {
            let parts: Vec<&str> = identifier.split('.').collect();
            (parts[0], parts[1])
        } else {
            ("public", identifier)
        };

        let rows = sqlx::query(
            "SELECT column_name, data_type, is_nullable 
             FROM information_schema.columns 
             WHERE table_schema = $1 AND table_name = $2 
             ORDER BY ordinal_position"
        )
        .bind(schema_name)
        .bind(table_name)
        .fetch_all(&self.pool)
        .await
        .map_err(SagoError::Database)?;

        if rows.is_empty() {
            return Err(SagoError::Schema(format!("Table '{}' not found in schema '{}'", table_name, schema_name)));
        }

        let fields: Vec<Field> = rows.iter().map(|row| {
            let name: String = row.get("column_name");
            let data_type_str: String = row.get("data_type");
            let is_nullable_str: String = row.get("is_nullable");
            
            let is_nullable = is_nullable_str == "YES";
            let data_type = Self::map_postgres_type(&data_type_str);
            
            Field::new(name, data_type, is_nullable)
        }).collect();

        Ok(Schema::new(fields))
    }
}

#[async_trait]
impl DataProvider for PostgresSchemaProvider {
    async fn get_data(&self, identifier: &str) -> Result<Vec<RecordBatch>> {
        let schema = self.get_schema(identifier).await?;
        
        // Securely quote identifiers to prevent SQL injection while supporting multi-part names
        let quoted_identifier = identifier
            .split('.')
            .map(|part| part.replace('"', "\"\""))
            .map(|part| format!("\"{}\"", part))
            .collect::<Vec<_>>()
            .join(".");

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
                        builders[i].as_any_mut().downcast_mut::<BooleanBuilder>().unwrap().append_option(val);
                    }
                    DataType::Int16 => {
                        let val: Option<i16> = row.try_get(col_name.as_str()).ok();
                        builders[i].as_any_mut().downcast_mut::<Int16Builder>().unwrap().append_option(val);
                    }
                    DataType::Int32 => {
                        let val: Option<i32> = row.try_get(col_name.as_str()).ok();
                        builders[i].as_any_mut().downcast_mut::<Int32Builder>().unwrap().append_option(val);
                    }
                    DataType::Int64 => {
                        let val: Option<i64> = row.try_get(col_name.as_str()).ok();
                        builders[i].as_any_mut().downcast_mut::<Int64Builder>().unwrap().append_option(val);
                    }
                    DataType::Float32 => {
                        let val: Option<f32> = row.try_get(col_name.as_str()).ok();
                        builders[i].as_any_mut().downcast_mut::<Float32Builder>().unwrap().append_option(val);
                    }
                    DataType::Float64 => {
                        let val: Option<f64> = row.try_get(col_name.as_str()).ok();
                        builders[i].as_any_mut().downcast_mut::<Float64Builder>().unwrap().append_option(val);
                    }
                    DataType::Utf8 => {
                        let val: Option<String> = row.try_get(col_name.as_str()).ok();
                        builders[i].as_any_mut().downcast_mut::<StringBuilder>().unwrap().append_option(val);
                    }
                    _ => {
                        // For fallback, try to get as string
                        let val: Option<String> = row.try_get::<String, _>(col_name.as_str()).ok();
                        builders[i].as_any_mut().downcast_mut::<StringBuilder>().unwrap().append_option(val);
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
        assert_eq!(PostgresSchemaProvider::map_postgres_type("smallint"), DataType::Int16);
        assert_eq!(PostgresSchemaProvider::map_postgres_type("int2"), DataType::Int16);
        assert_eq!(PostgresSchemaProvider::map_postgres_type("integer"), DataType::Int32);
        assert_eq!(PostgresSchemaProvider::map_postgres_type("int4"), DataType::Int32);
        assert_eq!(PostgresSchemaProvider::map_postgres_type("serial"), DataType::Int32);
        assert_eq!(PostgresSchemaProvider::map_postgres_type("bigint"), DataType::Int64);
        assert_eq!(PostgresSchemaProvider::map_postgres_type("int8"), DataType::Int64);
        assert_eq!(PostgresSchemaProvider::map_postgres_type("bigserial"), DataType::Int64);
    }

    #[test]
    fn test_map_postgres_type_floats() {
        assert_eq!(PostgresSchemaProvider::map_postgres_type("real"), DataType::Float32);
        assert_eq!(PostgresSchemaProvider::map_postgres_type("float4"), DataType::Float32);
        assert_eq!(PostgresSchemaProvider::map_postgres_type("double precision"), DataType::Float64);
        assert_eq!(PostgresSchemaProvider::map_postgres_type("float8"), DataType::Float64);
        assert_eq!(PostgresSchemaProvider::map_postgres_type("numeric"), DataType::Float64);
        assert_eq!(PostgresSchemaProvider::map_postgres_type("decimal"), DataType::Float64);
    }

    #[test]
    fn test_map_postgres_type_strings() {
        assert_eq!(PostgresSchemaProvider::map_postgres_type("character varying"), DataType::Utf8);
        assert_eq!(PostgresSchemaProvider::map_postgres_type("varchar"), DataType::Utf8);
        assert_eq!(PostgresSchemaProvider::map_postgres_type("text"), DataType::Utf8);
        assert_eq!(PostgresSchemaProvider::map_postgres_type("character"), DataType::Utf8);
        assert_eq!(PostgresSchemaProvider::map_postgres_type("char"), DataType::Utf8);
        assert_eq!(PostgresSchemaProvider::map_postgres_type("json"), DataType::Utf8);
        assert_eq!(PostgresSchemaProvider::map_postgres_type("jsonb"), DataType::Utf8);
    }

    #[test]
    fn test_map_postgres_type_other() {
        assert_eq!(PostgresSchemaProvider::map_postgres_type("boolean"), DataType::Boolean);
        assert_eq!(PostgresSchemaProvider::map_postgres_type("bytea"), DataType::Binary);
        assert_eq!(PostgresSchemaProvider::map_postgres_type("date"), DataType::Date32);
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

    #[test]
    fn test_map_postgres_type_unknown_falls_back_to_utf8() {
        assert_eq!(PostgresSchemaProvider::map_postgres_type("uuid"), DataType::Utf8);
        assert_eq!(PostgresSchemaProvider::map_postgres_type("interval"), DataType::Utf8);
        assert_eq!(PostgresSchemaProvider::map_postgres_type("some_custom_type"), DataType::Utf8);
    }
}
