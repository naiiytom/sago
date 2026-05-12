use anyhow::Result;
use clap::Args;

#[derive(Args, Debug)]
pub struct InitArgs {
    /// Name of the project
    pub name: String,
}

pub async fn run(args: &InitArgs) -> Result<()> {
    tracing::info!("init: {} (not yet implemented)", args.name);
    Ok(())
}
