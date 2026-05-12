use anyhow::{Context, Result, bail};
use clap::Args;

#[derive(Args, Debug)]
pub struct InitArgs {
    /// Name of the project
    pub name: String,
}

pub async fn run(args: &InitArgs) -> Result<()> {
    let cwd = std::env::current_dir().context("failed to read current directory")?;
    let toml_path = cwd.join("Sago.toml");
    if toml_path.exists() {
        bail!("Sago.toml already exists in {}", cwd.display());
    }

    std::fs::write(&toml_path, skeleton(&args.name))
        .with_context(|| format!("failed to write {}", toml_path.display()))?;

    let dot_sago = cwd.join(".sago");
    std::fs::create_dir_all(&dot_sago)?;
    std::fs::write(dot_sago.join(".gitignore"), "plans/\n")?;

    println!("initialized sago project '{}'", args.name);
    println!("next: edit Sago.toml then run `sago apply`");
    Ok(())
}

fn skeleton(name: &str) -> String {
    format!(
        r#"[project]
name = "{name}"
version = "0.1.0"

# Define one or more named connections.
# [connections.warehouse]
# type = "postgres"
# url  = "postgres://user:pw@host/db"

# Define the datasets to track.
# [targets.users]
# connection = "warehouse"
# identifier = "public.users"

# Optional sample persistence per target:
# [targets.users.sample]
# enabled = true
# n       = 1000

[checks]
drift_threshold = 0.05
"#
    )
}
