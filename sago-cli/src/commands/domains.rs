use anyhow::{Result, anyhow};
use clap::Args;
use sago_core::registry::{DomainInfo, list_domains, resolve_endpoint};

use crate::commands::plan::load_config;
use crate::report::OutputFormat;

#[derive(Args, Debug)]
pub struct DomainsArgs {
    /// Print only the SagoService endpoint registered for this domain (for
    /// scripting, e.g. `sago domains --resolve sales`). Errors if the domain
    /// is unknown or has no `endpoint` configured.
    #[arg(long)]
    pub resolve: Option<String>,

    /// Only show domains that have at least one target tagged with them.
    #[arg(long)]
    pub with_targets: bool,

    /// Only show domains missing a registered `endpoint`.
    #[arg(long)]
    pub missing_endpoint: bool,

    /// Output format: human-readable text (default) or JSON on stdout.
    #[arg(long, value_enum, default_value_t = OutputFormat::Text)]
    pub format: OutputFormat,
}

pub async fn run(args: &DomainsArgs) -> Result<()> {
    let cwd = std::env::current_dir()?;
    let cfg = load_config(&cwd.join("Sago.toml"))?;

    if let Some(domain) = &args.resolve {
        let endpoint = resolve_endpoint(&cfg, domain).map_err(|e| anyhow!(e.to_string()))?;
        println!("{endpoint}");
        return Ok(());
    }

    let domains: Vec<DomainInfo> = list_domains(&cfg)
        .into_iter()
        .filter(|d| !args.with_targets || d.target_count > 0)
        .filter(|d| !args.missing_endpoint || d.endpoint.is_none())
        .collect();

    if args.format == OutputFormat::Json {
        println!(
            "{}",
            serde_json::to_string_pretty(&domains).expect("DomainInfo always serializes")
        );
        return Ok(());
    }

    if domains.is_empty() {
        println!("no domains known (no [domains] entries or target `domain =` references)");
        return Ok(());
    }

    for d in &domains {
        let endpoint = d.endpoint.as_deref().unwrap_or("(no endpoint registered)");
        let registered = if d.has_domain_entry {
            ""
        } else {
            " (undeclared — referenced by a target only)"
        };
        println!(
            "{}: {} — {} target(s), {} operator(s){}",
            d.name, endpoint, d.target_count, d.operator_count, registered
        );
    }
    Ok(())
}
