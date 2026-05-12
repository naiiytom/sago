use arrow::datatypes::{DataType, Field, Schema};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use crate::{Result, SagoError};

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
                    data_type: format!("{:?}", f.data_type()),
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

fn parse_data_type(s: &str) -> Result<DataType> {
    // Supports the types produced by PostgresSchemaProvider and Parquet primitives
    // we round-trip today. Extend as new types are needed.
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
        other if other.starts_with("Timestamp(Nanosecond, None)") => {
            Ok(DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None))
        }
        other if other.starts_with("Timestamp(Nanosecond,") => Ok(DataType::Timestamp(
            arrow::datatypes::TimeUnit::Nanosecond,
            Some("+00:00".into()),
        )),
        other => Err(SagoError::Schema(format!(
            "unsupported serialized data type: {}",
            other
        ))),
    }
}

// Silence unused-Arc warning; will be used in Task 6 once full state types land.
#[allow(dead_code)]
fn _arc_marker() -> Arc<u8> { Arc::new(0) }

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_schema_round_trip_basic() {
        let original = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("email", DataType::Utf8, true),
            Field::new("active", DataType::Boolean, false),
        ]);
        let s: SerializableSchema = (&original).into();
        let json = serde_json::to_string(&s).unwrap();
        let parsed: SerializableSchema = serde_json::from_str(&json).unwrap();
        let restored = parsed.to_arrow_schema().unwrap();

        assert_eq!(restored.fields().len(), 3);
        assert_eq!(restored.field(0).name(), "id");
        assert_eq!(restored.field(0).data_type(), &DataType::Int64);
        assert_eq!(restored.field(1).data_type(), &DataType::Utf8);
        assert_eq!(restored.field(2).is_nullable(), false);
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
            SagoError::Schema(msg) => assert!(msg.contains("unsupported")),
            other => panic!("expected Schema error, got {:?}", other),
        }
    }
}
