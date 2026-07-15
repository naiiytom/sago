use anyhow::{Result, anyhow};
use clap::Args;
use sago_core::registry::{list_domains, resolve_endpoint};

use crate::commands::plan::load_config;

#[derive(Args, Debug)]
pub struct DomainsArgs {
    /// Print only the SagoService endpoint registered for this domain (for
    /// scripting, e.g. `sago domains --resolve sales`). Errors if the domain
    /// is unknown or has no `endpoint` configured.
    #[arg(long)]
    pub resolve: Option<String>,
}

pub async fn run(args: &DomainsArgs) -> Result<()> {
    let cwd = std::env::current_dir()?;
    let cfg = load_config(&cwd.join("Sago.toml"))?;

    if let Some(domain) = &args.resolve {
        let endpoint = resolve_endpoint(&cfg, domain).map_err(|e| anyhow!(e.to_string()))?;
        println!("{endpoint}");
        return Ok(());
    }

    let domains = list_domains(&cfg);
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
