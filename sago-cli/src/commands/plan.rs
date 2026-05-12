use anyhow::Result;
use clap::Args;
use std::path::PathBuf;

#[derive(Args, Debug)]
pub struct PlanArgs {
    /// Plan only the named target (default: all)
    #[arg(long)]
    pub target: Option<String>,

    /// Where to write the JSON artifact (default: .sago/plans/<timestamp>.json)
    #[arg(long)]
    pub out: Option<PathBuf>,
}

pub async fn run(_args: &PlanArgs) -> Result<()> {
    tracing::info!("plan (not yet implemented)");
    Ok(())
}
