use arrow::datatypes::Schema;
use serde::Serialize;
use std::collections::HashSet;

#[derive(Debug, Serialize, PartialEq)]
pub struct SchemaDrift {
    pub added_fields: Vec<String>,
    pub removed_fields: Vec<String>,
    pub changed_types: Vec<TypeChange>,
}

#[derive(Debug, Serialize, PartialEq)]
pub struct TypeChange {
    pub field_name: String,
    pub old_type: String,
    pub new_type: String,
}

pub fn detect_schema_drift(source: &Schema, target: &Schema) -> SchemaDrift {
    let source_fields: HashSet<_> = source.fields().iter().map(|f| f.name().clone()).collect();
    let target_fields: HashSet<_> = target.fields().iter().map(|f| f.name().clone()).collect();

    let added_fields: Vec<String> = target_fields.difference(&source_fields).cloned().collect();
    let removed_fields: Vec<String> = source_fields.difference(&target_fields).cloned().collect();

    let mut changed_types = Vec::new();

    for field_name in source_fields.intersection(&target_fields) {
        let source_field = source.field_with_name(field_name).unwrap();
        let target_field = target.field_with_name(field_name).unwrap();

        if source_field.data_type() != target_field.data_type() {
            changed_types.push(TypeChange {
                field_name: field_name.clone(),
                old_type: format!("{:?}", source_field.data_type()),
                new_type: format!("{:?}", target_field.data_type()),
            });
        }
    }

    SchemaDrift {
        added_fields,
        removed_fields,
        changed_types,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field};

    #[test]
    fn test_detect_schema_drift() {
        let source = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("age", DataType::Int32, false),
        ]);

        let target = Schema::new(vec![
            Field::new("id", DataType::Int64, false), // type change
            Field::new("name", DataType::Utf8, false),
            Field::new("email", DataType::Utf8, true), // added
        ]); // removed "age"

        let drift = detect_schema_drift(&source, &target);

        assert!(drift.added_fields.contains(&"email".to_string()));
        assert!(drift.removed_fields.contains(&"age".to_string()));
        assert_eq!(drift.changed_types.len(), 1);
        assert_eq!(drift.changed_types[0].field_name, "id");
    }
}
