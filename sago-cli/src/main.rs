use clap::{Parser, Subcommand};
use tracing::Level;
use tracing_subscriber::FmtSubscriber;

mod commands;
mod report;

use commands::{apply::ApplyArgs, diff::DiffArgs, init::InitArgs, plan::PlanArgs};

#[derive(Parser)]
#[command(name = "sago")]
#[command(version, about = "Terraform for DataOps", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,

    /// Sets the level of verbosity
    #[arg(short, long, default_value_t = Level::INFO)]
    log_level: Level,
}

#[derive(Subcommand)]
enum Commands {
    /// Initialize a new Sago project
    Init(InitArgs),
    /// Snapshot live data into the baseline
    Apply(ApplyArgs),
    /// Show drift since the last apply
    Plan(PlanArgs),
    /// One-shot cross-modal comparison of two sources
    Diff(DiffArgs),
    /// Launch interactive terminal explorer
    Explore,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let subscriber = FmtSubscriber::builder()
        .with_max_level(cli.log_level)
        .finish();
    tracing::subscriber::set_global_default(subscriber).expect("setting subscriber failed");

    match &cli.command {
        Commands::Init(a) => commands::init::run(a).await,
        Commands::Apply(a) => commands::apply::run(a).await,
        Commands::Plan(a) => commands::plan::run(a).await,
        Commands::Diff(a) => commands::diff::run(a).await,
        Commands::Explore => Ok(commands::explore::run()?),
    }
}
