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
