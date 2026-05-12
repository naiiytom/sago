use anyhow::Result;
use clap::Args;

#[derive(Args, Debug)]
pub struct ApplyArgs {
    /// Apply only the named target (default: all)
    #[arg(long)]
    pub target: Option<String>,
}

pub async fn run(_args: &ApplyArgs) -> Result<()> {
    tracing::info!("apply (not yet implemented)");
    Ok(())
}
