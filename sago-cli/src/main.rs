use clap::{Parser, Subcommand};
use tracing::Level;
use tracing_subscriber::FmtSubscriber;

mod commands;
mod report;

use commands::{
    apply::ApplyArgs, diff::DiffArgs, domains::DomainsArgs, federate::FederateArgs, init::InitArgs,
    plan::PlanArgs,
};

#[derive(Parser)]
#[command(name = "sago")]
#[command(version, about = "Terraform for DataOps", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,

    /// Sets the level of verbosity
    #[arg(short, long, default_value_t = Level::INFO, global = true)]
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
    /// Show drift grouped by data-mesh domain
    Federate(FederateArgs),
    /// List known data-mesh domains and their registered SagoService endpoints
    Domains(DomainsArgs),
    /// Launch interactive terminal explorer
    Explore,
}

#[tokio::main]
async fn main() -> anyhow::Result<std::process::ExitCode> {
    use std::process::ExitCode;

    let cli = Cli::parse();
    let subscriber = FmtSubscriber::builder()
        .with_max_level(cli.log_level)
        .finish();
    tracing::subscriber::set_global_default(subscriber).expect("setting subscriber failed");

    match &cli.command {
        Commands::Init(a) => commands::init::run(a).await.map(|()| ExitCode::SUCCESS),
        Commands::Apply(a) => commands::apply::run(a).await.map(|()| ExitCode::SUCCESS),
        // `plan` returns a non-zero ExitCode when drift breaches the configured
        // threshold, so CI can gate on it.
        Commands::Plan(a) => commands::plan::run(a).await,
        // Like `plan`/`federate`, `diff` exits non-zero on a drift-threshold breach.
        Commands::Diff(a) => commands::diff::run(a).await,
        // Like `plan`, `federate` exits non-zero on a drift-threshold breach.
        Commands::Federate(a) => commands::federate::run(a).await,
        Commands::Domains(a) => commands::domains::run(a).await.map(|()| ExitCode::SUCCESS),
        Commands::Explore => {
            commands::explore::run()?;
            Ok(ExitCode::SUCCESS)
        }
    }
}
