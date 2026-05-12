use arrow::datatypes::{DataType, Field, Schema};
use serde::{Deserialize, Serialize};

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
        "Timestamp(Nanosecond, None)" => {
            Ok(DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None))
        }
        other if other.starts_with("Timestamp(Nanosecond, Some(") => {
            // Extract timezone from debug repr: Timestamp(Nanosecond, Some("tz"))
            let tz = other
                .trim_start_matches("Timestamp(Nanosecond, Some(\"")
                .trim_end_matches("\"))")
                .to_string();
            Ok(DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, Some(tz.into())))
        }
        other => Err(SagoError::Schema(format!(
            "unsupported serialized data type: {}",
            other
        ))),
    }
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
            Field::new("created_at", DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None), true),
            Field::new("updated_at", DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, Some("+00:00".into())), false),
        ]);
        let s: SerializableSchema = (&original).into();
        let json = serde_json::to_string(&s).unwrap();
        let parsed: SerializableSchema = serde_json::from_str(&json).unwrap();
        let restored = parsed.to_arrow_schema().unwrap();

        assert_eq!(restored.fields().len(), 5);
        assert_eq!(restored.field(0).name(), "id");
        assert_eq!(restored.field(0).data_type(), &DataType::Int64);
        assert_eq!(restored.field(1).data_type(), &DataType::Utf8);
        assert_eq!(restored.field(2).is_nullable(), false);
        assert_eq!(restored.field(3).data_type(), &DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None));
        assert_eq!(restored.field(4).data_type(), &DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, Some("+00:00".into())));
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
