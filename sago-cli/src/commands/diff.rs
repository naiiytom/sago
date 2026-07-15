use anyhow::{Context, Result, anyhow};
use clap::Args;
use sago_core::config::Config;
use sago_core::connection::build_provider;
use sago_core::diff::diff_datasets_with_options;
use sago_core::rename::RenameOptions;
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use crate::commands::plan::{collect_breaches, resolve_rename_threshold};
use crate::report::{OutputFormat, default_artifact_path, print_json, print_terminal, write_artifact};

#[derive(Args, Debug)]
pub struct DiffArgs {
    /// Left source: `<connection>:<identifier>` or `<target_name>`
    pub left: String,
    /// Right source: `<connection>:<identifier>` or `<target_name>`
    pub right: String,
    /// Where to write the JSON artifact
    #[arg(long)]
    pub out: Option<PathBuf>,

    /// Override the rename-detection confidence threshold in [0, 1]
    /// (default: checks.rename_confidence_threshold from Sago.toml).
    #[arg(long)]
    pub rename_threshold: Option<f64>,

    /// Output format: human-readable text (default) or JSON on stdout.
    #[arg(long, value_enum, default_value_t = OutputFormat::Text)]
    pub format: OutputFormat,
}

pub async fn run(args: &DiffArgs) -> Result<ExitCode> {
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

    let rename_confidence = resolve_rename_threshold(
        args.rename_threshold,
        cfg.checks.rename_confidence_threshold,
    )?;
    let rename_opts = RenameOptions::with_min_confidence(rename_confidence);

    let report = diff_datasets_with_options(left, &left_id, right, &right_id, &rename_opts).await?;

    if args.format == OutputFormat::Json {
        print_json(std::slice::from_ref(&report))?;
    } else {
        print_terminal(std::slice::from_ref(&report));
    }

    // Gate the exit code on the same drift threshold `plan`/`federate` use,
    // so `sago diff` can drive a CI pipeline the same way — previously it
    // always exited 0 regardless of how severe the PSI/KS drift was.
    let threshold = cfg.checks.drift_threshold;
    let breaches = collect_breaches(std::slice::from_ref(&report), threshold);

    let out = args.out.clone().unwrap_or_else(default_artifact_path);
    write_artifact(&[report], &out)?;
    println!("diff written to {}", out.display());

    if breaches.is_empty() {
        Ok(ExitCode::SUCCESS)
    } else {
        eprintln!(
            "drift threshold {:.4} exceeded by {} column(s): {}",
            threshold,
            breaches.len(),
            breaches.join(", ")
        );
        Ok(ExitCode::FAILURE)
    }
}

pub(crate) fn resolve<'a>(
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

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> Config {
        Config::from_toml(
            r#"
[project]
name = "test"
version = "0.1.0"

[connections.warehouse]
type = "postgres"
url = "postgres://localhost/db"

[connections.archive]
type = "s3"
bucket = "my-bucket"
region = "us-east-1"

[targets.users]
connection = "warehouse"
identifier = "public.users"

[targets.events]
connection = "archive"
identifier = "events/2024.parquet"

[checks]
drift_threshold = 0.05
"#,
        )
        .unwrap()
    }

    #[test]
    fn test_resolve_by_connection_colon_postgres() {
        let cfg = test_config();
        let (conn, id) = resolve(&cfg, "warehouse:public.orders").unwrap();
        match conn {
            sago_core::config::ConnectionConfig::Postgres { url } => {
                assert_eq!(url, "postgres://localhost/db");
            }
            _ => panic!("expected Postgres"),
        }
        assert_eq!(id, "public.orders");
    }

    #[test]
    fn test_resolve_by_connection_colon_s3() {
        let cfg = test_config();
        let (conn, id) = resolve(&cfg, "archive:data/file.parquet").unwrap();
        match conn {
            sago_core::config::ConnectionConfig::S3 { bucket, .. } => {
                assert_eq!(bucket, "my-bucket");
            }
            _ => panic!("expected S3"),
        }
        assert_eq!(id, "data/file.parquet");
    }

    #[test]
    fn test_resolve_by_target_name() {
        let cfg = test_config();
        let (_, id) = resolve(&cfg, "users").unwrap();
        assert_eq!(id, "public.users");
    }

    #[test]
    fn test_resolve_by_target_name_s3() {
        let cfg = test_config();
        let (_, id) = resolve(&cfg, "events").unwrap();
        assert_eq!(id, "events/2024.parquet");
    }

    #[test]
    fn test_resolve_unknown_connection_returns_error() {
        let cfg = test_config();
        let result = resolve(&cfg, "unknown_conn:table");
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("unknown connection")
        );
    }

    #[test]
    fn test_resolve_unknown_target_returns_error() {
        let cfg = test_config();
        let result = resolve(&cfg, "no_such_target");
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("not a target name")
        );
    }
}
