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

# Distribution-drift sampling is ON by default (it backs `sago plan`'s PSI
# gate). Add this block only to tune the sample size or opt out per target:
# [targets.users.sample]
# enabled = false   # opt this target out of drift sampling
# n       = 1000    # or just tune the sample size

# drift_threshold gates `sago plan` on PSI (must be in [0, 1]): a column whose
# PSI exceeds it fails the plan with a non-zero exit code, so CI can gate on it.
[checks]
drift_threshold = 0.05
"#
    )
}
