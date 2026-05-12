use assert_cmd::Command;
use predicates::prelude::*;

#[test]
fn test_help_lists_all_four_commands() {
    let mut cmd = Command::cargo_bin("sago").unwrap();
    let out = cmd.arg("--help").assert().success();
    let stdout = String::from_utf8_lossy(&out.get_output().stdout).to_string();
    for word in ["init", "apply", "plan", "diff"] {
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
