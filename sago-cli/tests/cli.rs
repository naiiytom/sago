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
    // A target referencing a non-existent connection must fail loudly.
    let tmp = tempfile::tempdir().unwrap();
    std::fs::write(tmp.path().join("Sago.toml"), BAD_CONN_TOML).unwrap();
    Command::cargo_bin("sago")
        .unwrap()
        .args(["apply", "--target", "orphan"])
        .current_dir(tmp.path())
        .assert()
        .failure()
        .stderr(predicate::str::contains("unknown connection"));
}

#[test]
fn test_apply_target_filter_skips_other_targets() {
    // With --target naming a well-formed (but S3) target, the orphan target's
    // bad connection is never touched, so we don't get the unknown-connection
    // error. (The S3 fetch itself will fail on network/creds, but crucially NOT
    // with "unknown connection" — proving the filter excluded 'orphan'.)
    let tmp = tempfile::tempdir().unwrap();
    std::fs::write(tmp.path().join("Sago.toml"), BAD_CONN_TOML).unwrap();
    let out = Command::cargo_bin("sago")
        .unwrap()
        .args(["apply", "--target", "events"])
        .current_dir(tmp.path())
        .assert();
    let stderr = String::from_utf8_lossy(&out.get_output().stderr).to_string();
    assert!(
        !stderr.contains("unknown connection"),
        "the orphan target should have been filtered out, but got: {stderr}"
    );
}

#[test]
fn test_apply_nonexistent_target_does_nothing() {
    let tmp = tempfile::tempdir().unwrap();
    std::fs::write(tmp.path().join("Sago.toml"), SAMPLE_TOML).unwrap();
    Command::cargo_bin("sago")
        .unwrap()
        .args(["apply", "--target", "no_such_target"])
        .current_dir(tmp.path())
        .assert()
        .success()
        .stdout(predicate::str::contains("nothing to apply"));
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
fn test_plan_nonexistent_target_exits_success() {
    // With a state file present but --target naming an unknown target, plan has
    // nothing to compare and must exit 0 (not the drift-breach failure code).
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
        .success()
        .stdout(predicate::str::contains("nothing to plan"));
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
fn test_federate_nonexistent_domain_exits_success() {
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
