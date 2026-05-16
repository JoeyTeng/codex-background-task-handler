use std::process::Command;

fn help(args: &[&str]) -> String {
    let output = Command::new(env!("CARGO_BIN_EXE_cbth"))
        .args(args)
        .output()
        .expect("run cbth help");
    assert!(
        output.status.success(),
        "help failed\nstatus: {}\nstdout: {}\nstderr: {}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).expect("help is UTF-8")
}

#[test]
fn top_level_help_describes_public_command_groups() {
    let stdout = help(&["--help"]);
    assert!(stdout.contains("Run and inspect local supervised background tasks"));
    assert!(stdout.contains("Run Codex through the managed cbth CLI bridge"));
    assert!(stdout.contains("Control the same-user cbth daemon"));
    assert!(stdout.contains("Run the cbth host plugin service"));
    assert!(stdout.contains("Inspect host-level plugins"));
    assert!(stdout.contains("Use an alternate cbth home directory"));
}

#[test]
fn plugin_status_help_describes_optional_name_and_json() {
    let stdout = help(&["plugin", "status", "--help"]);
    assert!(stdout.contains("[NAME]"));
    assert!(stdout.contains("--json"));
    assert!(stdout.contains("Inspect configured host-level plugin supervisor state"));
}

#[test]
fn plugin_status_human_does_not_autostart_service() {
    let home = tempfile::tempdir().expect("temp home");
    let output = Command::new(env!("CARGO_BIN_EXE_cbth"))
        .arg("--home")
        .arg(home.path())
        .arg("plugin")
        .arg("status")
        .output()
        .expect("run plugin status human");
    assert!(
        output.status.success(),
        "plugin status failed\nstatus: {}\nstdout: {}\nstderr: {}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("No plugins configured."));
    assert!(!home.path().join("run/plugin-rpc.sock").exists());
}

#[test]
fn cli_help_lists_app_server_operator_command() {
    let stdout = help(&["cli", "--help"]);
    assert!(stdout.contains("app-servers"));
    assert!(stdout.contains("List running daemon-owned Codex app-servers"));
}

#[test]
fn cli_app_servers_help_describes_formats_and_non_autostart_behavior() {
    let stdout = help(&["cli", "app-servers", "--help"]);
    assert!(stdout.contains("-H"));
    assert!(stdout.contains("--human"));
    assert!(stdout.contains("--format"));
    assert!(stdout.contains("--latest-generation"));
    assert!(stdout.contains("--all-daemons"));
    assert!(stdout.contains("without starting a daemon"));
    assert!(stdout.contains("Newer generations are shown first"));
    assert!(stdout.contains("websocket URL"));
}

#[test]
fn self_update_help_describes_interactive_mode() {
    let stdout = help(&["self", "update", "--help"]);
    assert!(stdout.contains("-i"));
    assert!(stdout.contains("--interactive"));
    assert!(stdout.contains("Prompt before installing"));
    assert!(stdout.contains("--check"));
    assert!(stdout.contains("--yes"));
}

#[test]
fn cli_app_servers_human_does_not_autostart_daemon() {
    let home = tempfile::tempdir().expect("temp home");
    let output = Command::new(env!("CARGO_BIN_EXE_cbth"))
        .arg("--home")
        .arg(home.path())
        .arg("cli")
        .arg("app-servers")
        .arg("-H")
        .output()
        .expect("run app-servers human");
    assert!(
        output.status.success(),
        "app-servers failed\nstatus: {}\nstdout: {}\nstderr: {}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("No managed CLI app-servers are running."));
    assert!(!home.path().join("run/cbth.sock").exists());
}

#[test]
fn self_update_interactive_requires_tty_before_network() {
    let output = Command::new(env!("CARGO_BIN_EXE_cbth"))
        .arg("self")
        .arg("update")
        .arg("-i")
        .output()
        .expect("run self update interactive");
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("--interactive requires a TTY"));
}

#[test]
fn self_update_modes_are_mutually_exclusive() {
    let output = Command::new(env!("CARGO_BIN_EXE_cbth"))
        .arg("self")
        .arg("update")
        .arg("--check")
        .arg("-i")
        .output()
        .expect("run conflicting self update modes");
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("cannot be used with"));
}

#[test]
fn cli_app_servers_human_short_conflicts_with_explicit_format() {
    let output = Command::new(env!("CARGO_BIN_EXE_cbth"))
        .arg("cli")
        .arg("app-servers")
        .arg("-H")
        .arg("--format")
        .arg("json")
        .output()
        .expect("run conflicting app-servers output modes");
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("cannot be used with"));
}

#[test]
fn cli_app_servers_latest_generation_conflicts_with_all_daemons() {
    let output = Command::new(env!("CARGO_BIN_EXE_cbth"))
        .arg("cli")
        .arg("app-servers")
        .arg("--latest-generation")
        .arg("--all-daemons")
        .output()
        .expect("run conflicting app-server scopes");
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("cannot be used with"));
}
