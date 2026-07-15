use assert_cmd::Command;
use predicates::prelude::*;

#[test]
fn test_help_lists_all_four_commands() {
    let mut cmd = Command::cargo_bin("sago").unwrap();
    let out = cmd.arg("--help").assert().success();
    let stdout = String::from_utf8_lossy(&out.get_output().stdout).to_string();
    for word in ["init", "apply", "plan", "diff", "federate", "domains"] {
        assert!(
            stdout.contains(word),
            "help text missing '{}': {}",
            word,
            stdout
        );
    }
}

#[test]
fn test_init_creates_skeleton() {
    let tmp = tempfile::tempdir().unwrap();
    let mut cmd = Command::cargo_bin("sago").unwrap();
    cmd.arg("init")
        .arg("my-project")
        .current_dir(tmp.path())
        .assert()
        .success();

    let toml = tmp.path().join("Sago.toml");
    assert!(toml.exists(), "Sago.toml should be created");
    let content = std::fs::read_to_string(&toml).unwrap();
    assert!(content.contains("name = \"my-project\""));
    assert!(content.contains("[targets"));

    let dir = tmp.path().join(".sago");
    assert!(dir.is_dir(), ".sago/ should be created");
    let gitignore = dir.join(".gitignore");
    assert!(gitignore.exists(), ".sago/.gitignore should be created");
    let ig = std::fs::read_to_string(&gitignore).unwrap();
    assert!(ig.contains("plans/"));
}

#[test]
fn test_init_scaffold_is_valid_and_usable() {
    // Regression: a freshly-initialized project must produce a *parseable*
    // Sago.toml. Previously the skeleton commented out all connections/targets
    // while those fields were required, so the user's first `apply` died with
    // `missing field 'connections'`. `apply` should now succeed with a
    // "nothing to apply" message instead.
    let tmp = tempfile::tempdir().unwrap();
    Command::cargo_bin("sago")
        .unwrap()
        .args(["init", "fresh"])
        .current_dir(tmp.path())
        .assert()
        .success();

    Command::cargo_bin("sago")
        .unwrap()
        .arg("apply")
        .current_dir(tmp.path())
        .assert()
        .success()
        .stdout(predicate::str::contains("nothing to apply"));
}

#[test]
fn test_init_refuses_existing_project() {
    let tmp = tempfile::tempdir().unwrap();
    std::fs::write(tmp.path().join("Sago.toml"), "# already here").unwrap();

    let mut cmd = Command::cargo_bin("sago").unwrap();
    cmd.arg("init")
        .arg("my-project")
        .current_dir(tmp.path())
        .assert()
        .failure()
        .stderr(predicate::str::contains("already exists"));
}

const SAMPLE_TOML: &str = r#"
[project]
name = "test"
version = "0.1.0"

[connections.archive]
type = "s3"
bucket = "my-data"
region = "us-east-1"

[targets.events]
connection = "archive"
identifier = "events.parquet"

[checks]
drift_threshold = 0.05
"#;

// A config whose target references a connection that does not exist, to
// exercise apply/plan error paths without needing a live data source.
const BAD_CONN_TOML: &str = r#"
[project]
name = "test"
version = "0.1.0"

[connections.archive]
type = "s3"
bucket = "my-data"
region = "us-east-1"

[targets.events]
connection = "archive"
identifier = "events.parquet"

[targets.orphan]
connection = "does_not_exist"
identifier = "x.parquet"

[checks]
drift_threshold = 0.05
"#;

#[test]
fn test_apply_fails_on_unknown_connection_reference() {
    // A target referencing a non-existent connection must fail loudly, at
    // config-load time (before any provider I/O), not buried in per-target
    // apply output.
    let tmp = tempfile::tempdir().unwrap();
    std::fs::write(tmp.path().join("Sago.toml"), BAD_CONN_TOML).unwrap();
    Command::cargo_bin("sago")
        .unwrap()
        .args(["apply", "--target", "orphan"])
        .current_dir(tmp.path())
        .assert()
        .failure()
        .stderr(predicate::str::contains("does not match any"));
}

#[test]
fn test_apply_target_filter_still_validates_unrelated_bad_connections() {
    // Regression: Config::validate() checks every target's connection
    // reference up front, regardless of --target — so a config with an
    // unrelated broken target ("orphan") fails to load at all, even when
    // filtering to a different, well-formed target ("events"). This is a
    // deliberate trade: a config-wide typo is now caught immediately with a
    // single clear error, rather than only surfacing once `apply` happens to
    // iterate the broken target.
    let tmp = tempfile::tempdir().unwrap();
    std::fs::write(tmp.path().join("Sago.toml"), BAD_CONN_TOML).unwrap();
    Command::cargo_bin("sago")
        .unwrap()
        .args(["apply", "--target", "events"])
        .current_dir(tmp.path())
        .assert()
        .failure()
        .stderr(predicate::str::contains("orphan"));
}

#[test]
fn test_apply_nonexistent_target_fails_loudly() {
    // Regression: a typo'd --target used to silently no-op with exit 0
    // ("nothing to apply"), indistinguishable from a correctly-scoped run
    // with genuinely no matching targets. It must now error.
    let tmp = tempfile::tempdir().unwrap();
    std::fs::write(tmp.path().join("Sago.toml"), SAMPLE_TOML).unwrap();
    Command::cargo_bin("sago")
        .unwrap()
        .args(["apply", "--target", "no_such_target"])
        .current_dir(tmp.path())
        .assert()
        .failure()
        .stderr(predicate::str::contains("not a known target"));
}

#[test]
fn test_apply_fails_without_sago_toml() {
    let tmp = tempfile::tempdir().unwrap();
    let mut cmd = Command::cargo_bin("sago").unwrap();
    cmd.arg("apply")
        .current_dir(tmp.path())
        .assert()
        .failure()
        .stderr(predicate::str::contains("Sago.toml not found"));
}

// A target in a restricted domain, to exercise the RBAC gate without needing a
// live data source (the gate runs before any provider/connection I/O).
const RESTRICTED_DOMAIN_TOML: &str = r#"
[project]
name = "mesh"
version = "0.1.0"

[connections.archive]
type = "s3"
bucket = "my-data"
region = "us-east-1"

[targets.orders]
connection = "archive"
identifier = "orders.parquet"
domain = "sales"

[targets.misc]
connection = "archive"
identifier = "misc.parquet"

[domains.sales]
operators = ["alice"]

[checks]
drift_threshold = 0.05
"#;

#[test]
fn test_apply_restricted_target_without_actor_fails() {
    let tmp = tempfile::tempdir().unwrap();
    std::fs::write(tmp.path().join("Sago.toml"), RESTRICTED_DOMAIN_TOML).unwrap();
    Command::cargo_bin("sago")
        .unwrap()
        .args(["apply", "--target", "orders"])
        .env_remove("SAGO_ACTOR")
        .current_dir(tmp.path())
        .assert()
        .failure()
        .stderr(predicate::str::contains("requires authorization"));
}

#[test]
fn test_apply_restricted_target_with_unauthorized_actor_fails() {
    let tmp = tempfile::tempdir().unwrap();
    std::fs::write(tmp.path().join("Sago.toml"), RESTRICTED_DOMAIN_TOML).unwrap();
    Command::cargo_bin("sago")
        .unwrap()
        .args(["apply", "--target", "orders", "--as", "eve"])
        .current_dir(tmp.path())
        .assert()
        .failure()
        .stderr(predicate::str::contains("not authorized"));
}

#[test]
fn test_apply_restricted_target_with_authorized_actor_passes_rbac_gate() {
    // "alice" is authorized, so the RBAC check must pass; the command still
    // fails afterward on network I/O (no real S3 endpoint), but crucially NOT
    // with an authorization error — proving the gate let it through.
    let tmp = tempfile::tempdir().unwrap();
    std::fs::write(tmp.path().join("Sago.toml"), RESTRICTED_DOMAIN_TOML).unwrap();
    let out = Command::cargo_bin("sago")
        .unwrap()
        .args(["apply", "--target", "orders", "--as", "alice"])
        .current_dir(tmp.path())
        .assert();
    let stderr = String::from_utf8_lossy(&out.get_output().stderr).to_string();
    assert!(
        !stderr.contains("not authorized") && !stderr.contains("requires authorization"),
        "authorized actor should pass the RBAC gate, but got: {stderr}"
    );
}

#[test]
fn test_apply_restricted_target_authorized_via_env_var() {
    let tmp = tempfile::tempdir().unwrap();
    std::fs::write(tmp.path().join("Sago.toml"), RESTRICTED_DOMAIN_TOML).unwrap();
    let out = Command::cargo_bin("sago")
        .unwrap()
        .args(["apply", "--target", "orders"])
        .env("SAGO_ACTOR", "alice")
        .current_dir(tmp.path())
        .assert();
    let stderr = String::from_utf8_lossy(&out.get_output().stderr).to_string();
    assert!(
        !stderr.contains("not authorized") && !stderr.contains("requires authorization"),
        "SAGO_ACTOR should authorize the same as --as, but got: {stderr}"
    );
}

#[test]
fn test_apply_as_flag_overrides_env_var() {
    let tmp = tempfile::tempdir().unwrap();
    std::fs::write(tmp.path().join("Sago.toml"), RESTRICTED_DOMAIN_TOML).unwrap();
    Command::cargo_bin("sago")
        .unwrap()
        .args(["apply", "--target", "orders", "--as", "eve"])
        .env("SAGO_ACTOR", "alice")
        .current_dir(tmp.path())
        .assert()
        .failure()
        .stderr(predicate::str::contains("not authorized"));
}

#[test]
fn test_apply_unrestricted_target_ignores_missing_actor() {
    // "misc" has no domain, so it is unrestricted regardless of RBAC config
    // elsewhere in the same file — no actor needed at all.
    let tmp = tempfile::tempdir().unwrap();
    std::fs::write(tmp.path().join("Sago.toml"), RESTRICTED_DOMAIN_TOML).unwrap();
    let out = Command::cargo_bin("sago")
        .unwrap()
        .args(["apply", "--target", "misc"])
        .env_remove("SAGO_ACTOR")
        .current_dir(tmp.path())
        .assert();
    let stderr = String::from_utf8_lossy(&out.get_output().stderr).to_string();
    assert!(
        !stderr.contains("not authorized") && !stderr.contains("requires authorization"),
        "unrestricted target should not require an actor, but got: {stderr}"
    );
}

#[test]
fn test_apply_fails_with_legacy_schema_block() {
    let tmp = tempfile::tempdir().unwrap();
    std::fs::write(
        tmp.path().join("Sago.toml"),
        r#"
[project]
name = "x"
version = "1"

[schema]
provider = "p"
tables = []

[checks]
drift_threshold = 0.1
"#,
    )
    .unwrap();

    let mut cmd = Command::cargo_bin("sago").unwrap();
    cmd.arg("apply")
        .current_dir(tmp.path())
        .assert()
        .failure()
        .stderr(predicate::str::contains("[schema]"));
}

#[test]
fn test_plan_fails_without_state() {
    let tmp = tempfile::tempdir().unwrap();
    std::fs::write(tmp.path().join("Sago.toml"), SAMPLE_TOML).unwrap();
    let mut cmd = Command::cargo_bin("sago").unwrap();
    cmd.arg("plan")
        .current_dir(tmp.path())
        .assert()
        .failure()
        .stderr(predicate::str::contains("apply").or(predicate::str::contains("state")));
}

#[test]
fn test_plan_nonexistent_target_fails_loudly() {
    // Regression: a typo'd --target used to silently no-op with exit 0
    // ("nothing to plan"), the same bug as apply's --target. It must now error.
    let tmp = tempfile::tempdir().unwrap();
    std::fs::write(tmp.path().join("Sago.toml"), SAMPLE_TOML).unwrap();
    let sago_dir = tmp.path().join(".sago");
    std::fs::create_dir_all(&sago_dir).unwrap();
    std::fs::write(
        sago_dir.join("state.json"),
        r#"{"schema_version":1,"snapshots":{}}"#,
    )
    .unwrap();

    Command::cargo_bin("sago")
        .unwrap()
        .args(["plan", "--target", "no_such_target"])
        .current_dir(tmp.path())
        .assert()
        .failure()
        .stderr(predicate::str::contains("not a known target"));
}

#[test]
fn test_plan_rejects_out_of_range_rename_threshold() {
    // The --rename-threshold flag is validated to [0, 1].
    let tmp = tempfile::tempdir().unwrap();
    std::fs::write(tmp.path().join("Sago.toml"), SAMPLE_TOML).unwrap();
    let sago_dir = tmp.path().join(".sago");
    std::fs::create_dir_all(&sago_dir).unwrap();
    std::fs::write(
        sago_dir.join("state.json"),
        r#"{"schema_version":1,"snapshots":{}}"#,
    )
    .unwrap();

    Command::cargo_bin("sago")
        .unwrap()
        .args(["plan", "--rename-threshold", "1.5"])
        .current_dir(tmp.path())
        .assert()
        .failure()
        .stderr(predicate::str::contains("rename-threshold"));
}

#[test]
fn test_federate_fails_without_state() {
    let tmp = tempfile::tempdir().unwrap();
    std::fs::write(tmp.path().join("Sago.toml"), SAMPLE_TOML).unwrap();
    let mut cmd = Command::cargo_bin("sago").unwrap();
    cmd.arg("federate")
        .current_dir(tmp.path())
        .assert()
        .failure()
        .stderr(predicate::str::contains("apply").or(predicate::str::contains("state")));
}

#[test]
fn test_federate_nonexistent_domain_fails_loudly() {
    // Regression: a typo'd/unknown --domain used to silently no-op with
    // exit 0 ("nothing to federate"), indistinguishable from a real domain
    // that simply has zero targets right now. It must now error, since
    // SAMPLE_TOML declares no domains at all.
    let tmp = tempfile::tempdir().unwrap();
    std::fs::write(tmp.path().join("Sago.toml"), SAMPLE_TOML).unwrap();
    let sago_dir = tmp.path().join(".sago");
    std::fs::create_dir_all(&sago_dir).unwrap();
    std::fs::write(
        sago_dir.join("state.json"),
        r#"{"schema_version":1,"snapshots":{}}"#,
    )
    .unwrap();

    Command::cargo_bin("sago")
        .unwrap()
        .args(["federate", "--domain", "no_such_domain"])
        .current_dir(tmp.path())
        .assert()
        .failure()
        .stderr(predicate::str::contains("not a known domain"));
}

#[test]
fn test_federate_known_domain_with_zero_targets_exits_success() {
    // A domain that IS declared (via [domains.<name>]) but has no target
    // tagged with it yet must still report the softer "nothing to
    // federate" message and exit 0 — only a genuinely unknown domain name
    // should error.
    let toml = format!("{SAMPLE_TOML}\n[domains.marketing]\n");
    let tmp = tempfile::tempdir().unwrap();
    std::fs::write(tmp.path().join("Sago.toml"), toml).unwrap();
    let sago_dir = tmp.path().join(".sago");
    std::fs::create_dir_all(&sago_dir).unwrap();
    std::fs::write(
        sago_dir.join("state.json"),
        r#"{"schema_version":1,"snapshots":{}}"#,
    )
    .unwrap();

    Command::cargo_bin("sago")
        .unwrap()
        .args(["federate", "--domain", "marketing"])
        .current_dir(tmp.path())
        .assert()
        .success()
        .stdout(predicate::str::contains("nothing to federate"));
}

#[test]
fn test_federate_rejects_out_of_range_rename_threshold() {
    let tmp = tempfile::tempdir().unwrap();
    std::fs::write(tmp.path().join("Sago.toml"), SAMPLE_TOML).unwrap();
    let sago_dir = tmp.path().join(".sago");
    std::fs::create_dir_all(&sago_dir).unwrap();
    std::fs::write(
        sago_dir.join("state.json"),
        r#"{"schema_version":1,"snapshots":{}}"#,
    )
    .unwrap();

    Command::cargo_bin("sago")
        .unwrap()
        .args(["federate", "--rename-threshold", "1.5"])
        .current_dir(tmp.path())
        .assert()
        .failure()
        .stderr(predicate::str::contains("rename-threshold"));
}

#[test]
fn test_federate_help() {
    Command::cargo_bin("sago")
        .unwrap()
        .args(["federate", "--help"])
        .assert()
        .success();
}

#[test]
fn test_diff_fails_with_unknown_connection() {
    let tmp = tempfile::tempdir().unwrap();
    std::fs::write(tmp.path().join("Sago.toml"), SAMPLE_TOML).unwrap();
    let mut cmd = Command::cargo_bin("sago").unwrap();
    cmd.arg("diff")
        .arg("nope:foo")
        .arg("archive:bar.parquet")
        .current_dir(tmp.path())
        .assert()
        .failure()
        .stderr(predicate::str::contains("unknown connection"));
}

#[test]
fn test_log_level_accepted_after_subcommand() {
    // Regression: --log-level was a top-level-only arg (no global = true), so
    // clap rejected it when placed after the subcommand — the natural place
    // a user would put it (`sago apply --log-level debug`).
    let tmp = tempfile::tempdir().unwrap();
    std::fs::write(tmp.path().join("Sago.toml"), SAMPLE_TOML).unwrap();
    Command::cargo_bin("sago")
        .unwrap()
        .args(["apply", "--log-level", "debug", "--target", "no_such_target"])
        .current_dir(tmp.path())
        .assert()
        .failure() // still fails on the typo'd target — but NOT a clap arg-parsing error
        .stderr(predicate::str::contains("not a known target"));
}

#[test]
fn test_log_level_still_accepted_before_subcommand() {
    let tmp = tempfile::tempdir().unwrap();
    std::fs::write(tmp.path().join("Sago.toml"), SAMPLE_TOML).unwrap();
    Command::cargo_bin("sago")
        .unwrap()
        .args(["--log-level", "debug", "apply", "--target", "no_such_target"])
        .current_dir(tmp.path())
        .assert()
        .failure()
        .stderr(predicate::str::contains("not a known target"));
}

#[test]
fn test_full_init_then_help_subcommands() {
    let tmp = tempfile::tempdir().unwrap();

    // init
    Command::cargo_bin("sago")
        .unwrap()
        .arg("init")
        .arg("acme")
        .current_dir(tmp.path())
        .assert()
        .success();

    // each subcommand --help still works inside an initialized project
    for sub in ["apply", "plan", "diff", "federate", "domains"] {
        Command::cargo_bin("sago")
            .unwrap()
            .arg(sub)
            .arg("--help")
            .current_dir(tmp.path())
            .assert()
            .success();
    }

    // re-init should fail loudly
    Command::cargo_bin("sago")
        .unwrap()
        .arg("init")
        .arg("acme")
        .current_dir(tmp.path())
        .assert()
        .failure();
}

#[test]
fn test_explore_appears_in_help() {
    Command::cargo_bin("sago")
        .unwrap()
        .arg("--help")
        .assert()
        .success()
        .stdout(predicates::str::contains("explore"));
}

#[test]
fn test_explore_help() {
    Command::cargo_bin("sago")
        .unwrap()
        .args(["explore", "--help"])
        .assert()
        .success();
}

#[test]
fn test_explore_fails_without_sago_toml() {
    let dir = tempfile::tempdir().unwrap();
    Command::cargo_bin("sago")
        .unwrap()
        .arg("explore")
        .current_dir(dir.path())
        .assert()
        .failure()
        .stderr(predicates::str::contains("Sago.toml"));
}

// ── sago domains ─────────────────────────────────────────────────────────────

const MESH_TOML: &str = r#"
[project]
name = "mesh"
version = "0.1.0"

[connections.archive]
type = "s3"
bucket = "my-data"
region = "us-east-1"

[targets.orders]
connection = "archive"
identifier = "orders.parquet"
domain = "sales"

[targets.misc]
connection = "archive"
identifier = "misc.parquet"

[domains.sales]
operators = ["alice"]
endpoint = "http://sales.internal:50051"

[checks]
drift_threshold = 0.05
"#;

#[test]
fn test_domains_lists_known_domains_with_endpoint() {
    let tmp = tempfile::tempdir().unwrap();
    std::fs::write(tmp.path().join("Sago.toml"), MESH_TOML).unwrap();
    Command::cargo_bin("sago")
        .unwrap()
        .arg("domains")
        .current_dir(tmp.path())
        .assert()
        .success()
        .stdout(predicate::str::contains("sales"))
        .stdout(predicate::str::contains("http://sales.internal:50051"));
}

#[test]
fn test_domains_excludes_unassigned_targets() {
    let tmp = tempfile::tempdir().unwrap();
    std::fs::write(tmp.path().join("Sago.toml"), MESH_TOML).unwrap();
    let out = Command::cargo_bin("sago")
        .unwrap()
        .arg("domains")
        .current_dir(tmp.path())
        .assert()
        .success();
    let stdout = String::from_utf8_lossy(&out.get_output().stdout).to_string();
    assert!(
        !stdout.contains("misc"),
        "unassigned target 'misc' should not appear as a domain: {stdout}"
    );
}

#[test]
fn test_domains_no_domains_configured_reports_empty() {
    let tmp = tempfile::tempdir().unwrap();
    std::fs::write(tmp.path().join("Sago.toml"), SAMPLE_TOML).unwrap();
    Command::cargo_bin("sago")
        .unwrap()
        .arg("domains")
        .current_dir(tmp.path())
        .assert()
        .success()
        .stdout(predicate::str::contains("no domains known"));
}

#[test]
fn test_domains_resolve_known_domain_prints_endpoint() {
    let tmp = tempfile::tempdir().unwrap();
    std::fs::write(tmp.path().join("Sago.toml"), MESH_TOML).unwrap();
    Command::cargo_bin("sago")
        .unwrap()
        .args(["domains", "--resolve", "sales"])
        .current_dir(tmp.path())
        .assert()
        .success()
        .stdout("http://sales.internal:50051\n");
}

#[test]
fn test_domains_resolve_unknown_domain_fails() {
    let tmp = tempfile::tempdir().unwrap();
    std::fs::write(tmp.path().join("Sago.toml"), MESH_TOML).unwrap();
    Command::cargo_bin("sago")
        .unwrap()
        .args(["domains", "--resolve", "no-such-domain"])
        .current_dir(tmp.path())
        .assert()
        .failure()
        .stderr(predicate::str::contains("unknown domain"));
}

#[test]
fn test_domains_resolve_domain_without_endpoint_fails() {
    let tmp = tempfile::tempdir().unwrap();
    // "misc" has no domain, "sales" has a domain but this config gives it no
    // endpoint — a target with an unregistered domain should also error.
    let toml = r#"
[project]
name = "mesh"
version = "0.1.0"

[connections.archive]
type = "s3"
bucket = "my-data"
region = "us-east-1"

[targets.orders]
connection = "archive"
identifier = "orders.parquet"
domain = "sales"

[checks]
drift_threshold = 0.05
"#;
    std::fs::write(tmp.path().join("Sago.toml"), toml).unwrap();
    Command::cargo_bin("sago")
        .unwrap()
        .args(["domains", "--resolve", "sales"])
        .current_dir(tmp.path())
        .assert()
        .failure()
        .stderr(predicate::str::contains("no registered endpoint"));
}

#[test]
fn test_domains_fails_without_sago_toml() {
    let tmp = tempfile::tempdir().unwrap();
    Command::cargo_bin("sago")
        .unwrap()
        .arg("domains")
        .current_dir(tmp.path())
        .assert()
        .failure()
        .stderr(predicate::str::contains("Sago.toml not found"));
}

#[test]
fn test_domains_format_json_outputs_valid_array() {
    let tmp = tempfile::tempdir().unwrap();
    std::fs::write(tmp.path().join("Sago.toml"), MESH_TOML).unwrap();
    let out = Command::cargo_bin("sago")
        .unwrap()
        .args(["domains", "--format", "json"])
        .current_dir(tmp.path())
        .assert()
        .success();
    let stdout = String::from_utf8_lossy(&out.get_output().stdout).to_string();
    let parsed: serde_json::Value = serde_json::from_str(&stdout).expect("valid JSON");
    let arr = parsed.as_array().expect("top-level array");
    assert!(arr.iter().any(|d| d["name"] == "sales"));
    let sales = arr.iter().find(|d| d["name"] == "sales").unwrap();
    assert_eq!(sales["endpoint"], "http://sales.internal:50051");
}

#[test]
fn test_domains_with_targets_filter_excludes_empty_domains() {
    let toml = format!(
        "{MESH_TOML}\n[domains.marketing]\nendpoint = \"http://marketing.internal:1\"\n"
    );
    let tmp = tempfile::tempdir().unwrap();
    std::fs::write(tmp.path().join("Sago.toml"), toml).unwrap();
    let out = Command::cargo_bin("sago")
        .unwrap()
        .args(["domains", "--with-targets"])
        .current_dir(tmp.path())
        .assert()
        .success();
    let stdout = String::from_utf8_lossy(&out.get_output().stdout).to_string();
    assert!(stdout.contains("sales"), "sales has a target, should show");
    assert!(
        !stdout.contains("marketing"),
        "marketing has zero targets, should be filtered out: {stdout}"
    );
}

#[test]
fn test_domains_missing_endpoint_filter() {
    let toml = format!("{MESH_TOML}\n[domains.finance]\noperators = [\"bob\"]\n");
    let tmp = tempfile::tempdir().unwrap();
    std::fs::write(tmp.path().join("Sago.toml"), toml).unwrap();
    let out = Command::cargo_bin("sago")
        .unwrap()
        .args(["domains", "--missing-endpoint"])
        .current_dir(tmp.path())
        .assert()
        .success();
    let stdout = String::from_utf8_lossy(&out.get_output().stdout).to_string();
    assert!(
        !stdout.contains("sales:"),
        "sales has an endpoint, should be filtered out"
    );
    assert!(
        stdout.contains("finance"),
        "finance has no endpoint, should show: {stdout}"
    );
}

#[test]
fn test_plan_format_json_outputs_valid_array_when_empty() {
    // A config with no targets at all has nothing to plan, regardless of
    // state contents — the empty-JSON-array path.
    let toml = r#"
[project]
name = "empty"
version = "0.1.0"

[checks]
drift_threshold = 0.05
"#;
    let tmp = tempfile::tempdir().unwrap();
    std::fs::write(tmp.path().join("Sago.toml"), toml).unwrap();
    let sago_dir = tmp.path().join(".sago");
    std::fs::create_dir_all(&sago_dir).unwrap();
    std::fs::write(
        sago_dir.join("state.json"),
        r#"{"schema_version":1,"snapshots":{}}"#,
    )
    .unwrap();

    let out = Command::cargo_bin("sago")
        .unwrap()
        .args(["plan", "--format", "json"])
        .current_dir(tmp.path())
        .assert()
        .success();
    let stdout = String::from_utf8_lossy(&out.get_output().stdout).to_string();
    let parsed: serde_json::Value = serde_json::from_str(&stdout).expect("valid JSON");
    assert!(parsed.as_array().unwrap().is_empty());
}
