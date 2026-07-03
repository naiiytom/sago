use std::sync::Arc;

use sago_core::config::ConnectionConfig;
use sago_core::state::TargetSnapshot;
use sago_core::{DataProvider, Result};
use tokio::sync::OnceCell;

pub struct SagoClient {
    cfg: ConnectionConfig,
    /// The connection/provider is built lazily on first use and reused across
    /// calls, so we don't stand up a fresh pool (5 Postgres connections) on
    /// every `snapshot`. `new` stays synchronous and infallible.
    provider: OnceCell<Arc<dyn DataProvider>>,
}

impl SagoClient {
    pub fn new(cfg: ConnectionConfig) -> Self {
        Self {
            cfg,
            provider: OnceCell::new(),
        }
    }

    /// The shared provider, built once on first access.
    async fn provider(&self) -> Result<Arc<dyn DataProvider>> {
        use sago_core::connection::build_provider;
        let provider = self
            .provider
            .get_or_try_init(|| build_provider(&self.cfg))
            .await?;
        Ok(provider.clone())
    }

    /// Capture a [`TargetSnapshot`] of `identifier` (a table name for Postgres,
    /// an object key for S3) — its schema, per-column stats, and inferred
    /// semantic types.
    ///
    /// The first call lazily builds the underlying provider and reuses it for
    /// every subsequent call; for Postgres this stands up a connection pool
    /// (5 connections), so a `SagoClient` is cheap to create but the first
    /// `snapshot` pays the connection cost.
    ///
    /// `sample_n` controls persistence of the per-column numeric samples used
    /// for the PSI drift metric: `Some(n)` retains up to `n` values per numeric
    /// column, `None` skips sample capture (so PSI-based drift can't later be
    /// computed against this snapshot).
    pub async fn snapshot(
        &self,
        identifier: &str,
        sample_n: Option<usize>,
    ) -> Result<TargetSnapshot> {
        use sago_core::state::capture_snapshot;

        let provider = self.provider().await?;
        capture_snapshot(provider, identifier, sample_n).await
    }
}
