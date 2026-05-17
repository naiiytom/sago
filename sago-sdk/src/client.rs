use sago_core::config::ConnectionConfig;
use sago_core::state::TargetSnapshot;

pub struct SagoClient {
    cfg: ConnectionConfig,
}

impl SagoClient {
    pub fn new(cfg: ConnectionConfig) -> Self {
        Self { cfg }
    }

    pub async fn snapshot(
        &self,
        identifier: &str,
        sample_n: Option<usize>,
    ) -> anyhow::Result<TargetSnapshot> {
        use sago_core::connection::build_provider;
        use sago_core::state::capture_snapshot;

        let provider = build_provider(&self.cfg)
            .await
            .map_err(|e| anyhow::anyhow!(e))?;
        capture_snapshot(provider, identifier, sample_n)
            .await
            .map_err(|e| anyhow::anyhow!(e))
    }
}
