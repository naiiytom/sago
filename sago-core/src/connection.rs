use crate::config::ConnectionConfig;
use crate::s3::S3SchemaProvider;
use crate::{DataProvider, Result, SagoError};
use std::sync::Arc;

pub async fn build_provider(cfg: &ConnectionConfig) -> Result<Arc<dyn DataProvider>> {
    match cfg {
        ConnectionConfig::Postgres { url } => {
            let pool = sqlx::postgres::PgPoolOptions::new()
                .max_connections(5)
                .connect(url)
                .await
                .map_err(SagoError::Database)?;
            Ok(Arc::new(crate::postgres::PostgresSchemaProvider::new(pool)))
        }
        ConnectionConfig::S3 {
            bucket,
            region,
            format,
        } => {
            let p = S3SchemaProvider::new(bucket, region)?.with_format(format.clone());
            Ok(Arc::new(p))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_build_provider_s3_succeeds() {
        let cfg = ConnectionConfig::S3 {
            bucket: "test-bucket".into(),
            region: "us-east-1".into(),
            format: None,
        };
        let p = build_provider(&cfg).await;
        assert!(p.is_ok());
    }

    // We do NOT test Postgres construction here — it requires a live database.
    // Connection-error path is covered indirectly by the apply integration test.
}
