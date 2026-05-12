use assert_cmd::Command;

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
