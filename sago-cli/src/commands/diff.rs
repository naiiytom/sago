use anyhow::Result;
use clap::Args;
use std::path::PathBuf;

#[derive(Args, Debug)]
pub struct DiffArgs {
    /// Left source: <connection>:<identifier> or <target_name>
    pub left: String,
    /// Right source: <connection>:<identifier> or <target_name>
    pub right: String,
    /// Where to write the JSON artifact (default: .sago/plans/<timestamp>.json)
    #[arg(long)]
    pub out: Option<PathBuf>,
}

pub async fn run(_args: &DiffArgs) -> Result<()> {
    tracing::info!("diff (not yet implemented)");
    Ok(())
}
