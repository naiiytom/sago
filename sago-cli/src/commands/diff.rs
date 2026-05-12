use anyhow::{Context, Result, anyhow};
use clap::Args;
use sago_core::config::Config;
use sago_core::connection::build_provider;
use sago_core::diff::diff_datasets;
use std::path::{Path, PathBuf};

use crate::report::{default_artifact_path, print_terminal, write_artifact};

#[derive(Args, Debug)]
pub struct DiffArgs {
    /// Left source: <connection>:<identifier> or <target_name>
    pub left: String,
    /// Right source: <connection>:<identifier> or <target_name>
    pub right: String,
    /// Where to write the JSON artifact
    #[arg(long)]
    pub out: Option<PathBuf>,
}

pub async fn run(args: &DiffArgs) -> Result<()> {
    let cwd = std::env::current_dir()?;
    let cfg = load_config(&cwd.join("Sago.toml"))?;

    let (left_conn, left_id) = resolve(&cfg, &args.left)?;
    let (right_conn, right_id) = resolve(&cfg, &args.right)?;

    let left = build_provider(left_conn)
        .await
        .with_context(|| format!("failed to build provider for left side '{}'", args.left))?;
    let right = build_provider(right_conn)
        .await
        .with_context(|| format!("failed to build provider for right side '{}'", args.right))?;

    let report = diff_datasets(left, &left_id, right, &right_id).await?;

    print_terminal(std::slice::from_ref(&report));

    let out = args.out.clone().unwrap_or_else(default_artifact_path);
    write_artifact(&[report], &out)?;
    println!("diff written to {}", out.display());
    Ok(())
}

fn resolve<'a>(
    cfg: &'a Config,
    arg: &'a str,
) -> Result<(&'a sago_core::config::ConnectionConfig, String)> {
    if let Some((conn_name, id)) = arg.split_once(':') {
        let conn = cfg
            .connections
            .get(conn_name)
            .ok_or_else(|| anyhow!("unknown connection '{}'", conn_name))?;
        Ok((conn, id.to_string()))
    } else {
        let tgt = cfg
            .targets
            .get(arg)
            .ok_or_else(|| anyhow!("'{}' is not a target name or '<connection>:<id>'", arg))?;
        let conn = cfg.connections.get(&tgt.connection).ok_or_else(|| {
            anyhow!(
                "target '{}' references unknown connection '{}'",
                arg,
                tgt.connection
            )
        })?;
        Ok((conn, tgt.identifier.clone()))
    }
}

fn load_config(path: &Path) -> Result<Config> {
    let content = std::fs::read_to_string(path).with_context(|| {
        format!(
            "Sago.toml not found at {} (run `sago init`)",
            path.display()
        )
    })?;
    Ok(Config::from_toml(&content)?)
}
