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

/// Escape `s` for embedding in a TOML basic string (`"..."`), per the TOML
/// spec: backslash and double-quote are backslash-escaped, and control
/// characters get their short escape (or `\uXXXX` for the rest). Without
/// this, a project name containing a `"` or a newline could break out of the
/// generated `name = "..."` string and inject arbitrary sibling keys/tables
/// into Sago.toml, or otherwise silently produce a differently-structured
/// config than the user intended.
fn escape_toml_string(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '\u{08}' => out.push_str("\\b"),
            '\u{0C}' => out.push_str("\\f"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out
}

fn skeleton(name: &str) -> String {
    let name = escape_toml_string(name);
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
# rename_confidence_threshold = 0.6   # min confidence to report a rename (0..1)
"#
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_skeleton_with_normal_name_parses() {
        let toml = skeleton("my-project");
        let cfg: toml::Value = toml::from_str(&toml).expect("skeleton must be valid TOML");
        assert_eq!(cfg["project"]["name"].as_str(), Some("my-project"));
    }

    #[test]
    fn test_skeleton_escapes_embedded_quote() {
        // Regression: a raw '"' in the name used to close the TOML string
        // early, letting anything after it (e.g. an injected table) become
        // sibling config rather than part of the string.
        let toml = skeleton("foo\" \n[injected]\nevil = true");
        let cfg: toml::Value = toml::from_str(&toml).expect("must still be valid TOML");
        // The whole malicious payload round-trips as literal string content,
        // not as an injected [injected] table.
        assert!(cfg.get("injected").is_none());
        assert_eq!(
            cfg["project"]["name"].as_str(),
            Some("foo\" \n[injected]\nevil = true")
        );
    }

    #[test]
    fn test_skeleton_escapes_backslash() {
        let toml = skeleton(r"back\slash");
        let cfg: toml::Value = toml::from_str(&toml).expect("must be valid TOML");
        assert_eq!(cfg["project"]["name"].as_str(), Some(r"back\slash"));
    }

    #[test]
    fn test_skeleton_escapes_control_characters() {
        let toml = skeleton("tab\there");
        let cfg: toml::Value = toml::from_str(&toml).expect("must be valid TOML");
        assert_eq!(cfg["project"]["name"].as_str(), Some("tab\there"));
    }

    #[test]
    fn test_escape_toml_string_basics() {
        assert_eq!(escape_toml_string("plain"), "plain");
        assert_eq!(escape_toml_string(r#"a"b"#), r#"a\"b"#);
        assert_eq!(escape_toml_string("a\\b"), "a\\\\b");
        assert_eq!(escape_toml_string("a\nb"), "a\\nb");
    }
}
