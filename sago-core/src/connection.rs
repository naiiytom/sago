use crate::config::ConnectionConfig;
use crate::s3::S3SchemaProvider;
use crate::{DataProvider, Result, SagoError};
use std::collections::HashMap;
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

/// Builds and caches one provider per named `[connections.<name>]` entry,
/// keyed by connection name, for the lifetime of a single `sago apply`/`plan`
/// invocation.
///
/// Without this, a caller iterating N targets that all share one
/// `[connections.warehouse]` (a common data-mesh setup) would call
/// `build_provider` once per target — for Postgres, that means N independent
/// `PgPoolOptions::new().max_connections(5)` pools (up to `5*N` physical
/// connections) instead of one pool of up to 5 reused across every target,
/// which can exhaust a shared database's connection budget.
#[derive(Default)]
pub struct ProviderCache {
    providers: tokio::sync::Mutex<HashMap<String, Arc<dyn DataProvider>>>,
}

impl ProviderCache {
    pub fn new() -> Self {
        Self::default()
    }

    /// The cached provider for `connection_name`, building and caching one
    /// via [`build_provider`] on first use. `cfg` must be the
    /// [`ConnectionConfig`] that `connection_name` actually resolves to (the
    /// caller already has it from the lookup needed to report an "unknown
    /// connection" error); this function does not re-validate that mapping.
    pub async fn get_or_build(
        &self,
        connection_name: &str,
        cfg: &ConnectionConfig,
    ) -> Result<Arc<dyn DataProvider>> {
        let mut providers = self.providers.lock().await;
        if let Some(provider) = providers.get(connection_name) {
            return Ok(provider.clone());
        }
        let provider = build_provider(cfg).await?;
        providers.insert(connection_name.to_string(), provider.clone());
        Ok(provider)
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

    // ── ProviderCache ─────────────────────────────────────────────────────────

    fn s3_config(bucket: &str) -> ConnectionConfig {
        ConnectionConfig::S3 {
            bucket: bucket.into(),
            region: "us-east-1".into(),
            format: None,
        }
    }

    #[tokio::test]
    async fn test_provider_cache_reuses_provider_for_same_connection_name() {
        // Regression: N targets sharing one named connection must reuse a
        // single provider (and, for Postgres, one connection pool) instead
        // of building a fresh one per target.
        let cache = ProviderCache::new();
        let cfg = s3_config("shared-bucket");

        let a = cache.get_or_build("warehouse", &cfg).await.unwrap();
        let b = cache.get_or_build("warehouse", &cfg).await.unwrap();

        assert!(
            Arc::ptr_eq(&a, &b),
            "second get_or_build for the same connection name must return the cached provider"
        );
    }

    #[tokio::test]
    async fn test_provider_cache_builds_distinct_providers_for_distinct_names() {
        let cache = ProviderCache::new();
        let a = cache
            .get_or_build("warehouse", &s3_config("bucket-a"))
            .await
            .unwrap();
        let b = cache
            .get_or_build("archive", &s3_config("bucket-b"))
            .await
            .unwrap();

        assert!(
            !Arc::ptr_eq(&a, &b),
            "distinct connection names must get distinct providers"
        );
    }

    #[tokio::test]
    async fn test_provider_cache_many_targets_one_connection_builds_once() {
        // End-to-end version of the regression scenario: simulate 20 targets
        // all resolving to the same named connection, as apply.rs/plan.rs do
        // when iterating cfg.targets. Every lookup must return the same
        // cached provider.
        let cache = ProviderCache::new();
        let cfg = s3_config("shared-bucket");

        let first = cache.get_or_build("warehouse", &cfg).await.unwrap();
        for _ in 0..19 {
            let p = cache.get_or_build("warehouse", &cfg).await.unwrap();
            assert!(Arc::ptr_eq(&first, &p));
        }
    }
}
