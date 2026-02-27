use clap::{Parser, Subcommand};
use tracing::{info, Level};
use tracing_subscriber::FmtSubscriber;

#[derive(Parser)]
#[command(name = "sago")]
#[command(about = "Terraform for DataOps", long_about = None)]
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
    Init {
        /// Name of the project
        name: String,
    },
    /// Show the execution plan
    Plan,
    /// Apply changes to the data infrastructure
    Apply,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    let subscriber = FmtSubscriber::builder()
        .with_max_level(cli.log_level)
        .finish();

    tracing::subscriber::set_global_default(subscriber)
        .expect("setting default subscriber failed");

    match &cli.command {
        Commands::Init { name } => {
            info!("Initializing project: {}", name);
            // TODO: Create initial Sago.toml
        }
        Commands::Plan => {
            info!("Planning execution...");
            // TODO: Parse config and detect drift
        }
        Commands::Apply => {
            info!("Applying changes...");
            // TODO: Execute plan
        }
    }

    Ok(())
}
