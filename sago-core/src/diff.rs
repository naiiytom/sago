use crate::{DataProvider, Result};
use crate::drift::{detect_data_drift, detect_schema_drift, detect_semantic_drift, DataDrift, SchemaDrift, SemanticDrift};
use serde::Serialize;
use std::sync::Arc;

#[derive(Debug, Serialize, PartialEq)]
pub struct DiffReport {
    pub source_identifier: String,
    pub target_identifier: String,
    pub schema_drift: SchemaDrift,
    pub semantic_drifts: Vec<SemanticDrift>,
    pub data_drift: DataDrift,
}

pub async fn diff_datasets(
    source_provider: Arc<dyn DataProvider>,
    source_identifier: &str,
    target_provider: Arc<dyn DataProvider>,
    target_identifier: &str,
) -> Result<DiffReport> {
    let source_schema = source_provider.get_schema(source_identifier).await?;
    let target_schema = target_provider.get_schema(target_identifier).await?;

    let schema_drift = detect_schema_drift(&source_schema, &target_schema);

    let source_data = source_provider.get_data(source_identifier).await?;
    let target_data = target_provider.get_data(target_identifier).await?;

    let semantic_drifts = detect_semantic_drift(&source_data, &target_data);
    let data_drift = detect_data_drift(&source_data, &target_data);

    Ok(DiffReport {
        source_identifier: source_identifier.to_string(),
        target_identifier: target_identifier.to_string(),
        schema_drift,
        semantic_drifts,
        data_drift,
    })
}
