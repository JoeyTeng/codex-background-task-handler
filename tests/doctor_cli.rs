#![cfg(unix)]

use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use serde_json::Value;
use tempfile::TempDir;

use std::os::unix::fs::PermissionsExt;

fn temp_home() -> TempDir {
    let home = tempfile::tempdir().expect("temp home");
    fs::set_permissions(home.path(), fs::Permissions::from_mode(0o700)).expect("chmod temp home");
    home
}

fn write_fake_codex_script(path: &Path) -> PathBuf {
    fs::write(
        path,
        r#"#!/bin/sh
log="${FAKE_CODEX_LOG:?}"

if [ "${1:-}" = "--version" ]; then
  if [ "${FAKE_CODEX_VERSION_GRANDCHILD_STDOUT:-}" = "1" ]; then
    (trap '' TERM; while :; do sleep 1; done) &
  fi
  printf '%s\n' "${FAKE_CODEX_VERSION:-codex-cli 9.9.9}"
  exit 0
fi

emit_listener_line() {
  if [ "${FAKE_CODEX_LISTENER_STREAM:-stdout}" = "stderr" ]; then
    printf '%s\n' "$1" >&2
  else
    printf '%s\n' "$1"
  fi
}

if [ "${1:-}" = "app-server" ]; then
  printf 'app-server' >> "$log"
  for arg in "$@"; do
    printf '\t%s' "$arg" >> "$log"
  done
  printf '\n' >> "$log"

  emit_listener_line 'codex app-server'
  if [ "${FAKE_CODEX_APP_SERVER_MODE:-}" = "malformed" ]; then
    emit_listener_line 'listener unavailable'
    exit 0
  fi

  url="${FAKE_CODEX_APP_SERVER_URL:-ws://127.0.0.1:45678}"
  emit_listener_line "  listening on: $url"
  while :; do
    sleep 1
  done
fi

printf 'unexpected invocation:' >> "$log"
for arg in "$@"; do
  printf '\t%s' "$arg" >> "$log"
done
printf '\n' >> "$log"
exit 2
"#,
    )
    .expect("write fake codex");
    fs::set_permissions(path, fs::Permissions::from_mode(0o700)).expect("chmod fake codex");
    path.to_path_buf()
}

fn write_bad_shebang_script(path: &Path) -> PathBuf {
    fs::write(path, "#!/missing-codex-doctor-interpreter\n").expect("write bad codex");
    fs::set_permissions(path, fs::Permissions::from_mode(0o700)).expect("chmod bad codex");
    path.to_path_buf()
}

fn run_doctor(home: &TempDir, fake_codex: &Path, extra_env: &[(&str, &str)]) -> Output {
    let log_path = home.path().join("fake-codex.log");
    let mut command = Command::new(env!("CARGO_BIN_EXE_cbth"));
    command
        .arg("--home")
        .arg(home.path())
        .arg("doctor")
        .arg("cli")
        .arg("--codex-bin")
        .arg(fake_codex)
        .env("FAKE_CODEX_LOG", &log_path);
    for (name, value) in extra_env {
        command.env(name, value);
    }
    output_with_timeout(command, Duration::from_secs(10))
}

fn run_doctor_missing(home: &TempDir, missing_codex: &Path) -> Output {
    let mut command = Command::new(env!("CARGO_BIN_EXE_cbth"));
    command
        .arg("--home")
        .arg(home.path())
        .arg("doctor")
        .arg("cli")
        .arg("--codex-bin")
        .arg(missing_codex);
    output_with_timeout(command, Duration::from_secs(10))
}

fn output_with_timeout(mut command: Command, timeout: Duration) -> Output {
    command.stdout(Stdio::piped()).stderr(Stdio::piped());
    let child = command.spawn().expect("spawn command");
    wait_with_timeout(child, timeout)
}

fn wait_with_timeout(mut child: std::process::Child, timeout: Duration) -> Output {
    let deadline = Instant::now() + timeout;
    loop {
        match child.try_wait().expect("poll child") {
            Some(_) => return child.wait_with_output().expect("collect child output"),
            None if Instant::now() >= deadline => {
                let _ = child.kill();
                let output = child
                    .wait_with_output()
                    .expect("collect timed-out child output");
                panic!(
                    "child timed out\nstatus: {}\nstdout: {}\nstderr: {}",
                    output.status,
                    String::from_utf8_lossy(&output.stdout),
                    String::from_utf8_lossy(&output.stderr)
                );
            }
            None => thread::sleep(Duration::from_millis(50)),
        }
    }
}

fn stop_daemon(home: &TempDir) {
    let _ = Command::new(env!("CARGO_BIN_EXE_cbth"))
        .arg("--home")
        .arg(home.path())
        .arg("daemon")
        .arg("stop")
        .output();
}

fn parse_stdout(output: &Output) -> Value {
    serde_json::from_slice(&output.stdout).unwrap_or_else(|error| {
        panic!(
            "stdout was not JSON: {error}\nstatus: {}\nstdout: {}\nstderr: {}",
            output.status,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        )
    })
}

fn check_status<'a>(value: &'a Value, name: &str) -> &'a str {
    value["doctor"]["checks"]
        .as_array()
        .expect("checks array")
        .iter()
        .find(|check| check["name"] == name)
        .unwrap_or_else(|| panic!("missing check {name}: {value}"))["status"]
        .as_str()
        .expect("check status")
}

#[test]
fn doctor_cli_readiness_succeeds_with_stdout_listener() {
    let home = temp_home();
    let fake_codex = write_fake_codex_script(&home.path().join("fake-codex"));
    let output = run_doctor(&home, &fake_codex, &[]);
    stop_daemon(&home);

    assert!(
        output.status.success(),
        "doctor failed\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let value = parse_stdout(&output);
    assert_eq!(value["doctor"]["ok"], true);
    assert_eq!(check_status(&value, "codex-app-server-listener"), "ok");
    assert!(
        fs::read_to_string(home.path().join("fake-codex.log"))
            .expect("fake codex log")
            .contains("app-server\tapp-server\t--listen\tws://127.0.0.1:0")
    );
}

#[test]
fn doctor_cli_readiness_accepts_stderr_listener() {
    let home = temp_home();
    let fake_codex = write_fake_codex_script(&home.path().join("fake-codex"));
    let output = run_doctor(
        &home,
        &fake_codex,
        &[("FAKE_CODEX_LISTENER_STREAM", "stderr")],
    );
    stop_daemon(&home);

    assert!(
        output.status.success(),
        "doctor failed\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let value = parse_stdout(&output);
    assert_eq!(value["doctor"]["ok"], true);
    assert_eq!(check_status(&value, "codex-app-server-listener"), "ok");
}

#[test]
fn doctor_cli_reports_missing_codex_binary_as_json_failure() {
    let home = temp_home();
    let output = run_doctor_missing(&home, &home.path().join("missing-codex"));
    stop_daemon(&home);

    assert!(!output.status.success());
    let value = parse_stdout(&output);
    assert_eq!(value["doctor"]["ok"], false);
    assert_eq!(check_status(&value, "codex-binary"), "fail");
    assert_eq!(check_status(&value, "codex-app-server-listener"), "skipped");
}

#[test]
fn doctor_cli_cleans_version_capture_files_when_version_spawn_fails() {
    let home = temp_home();
    let bad_codex = write_bad_shebang_script(&home.path().join("bad-codex"));
    let output = run_doctor(&home, &bad_codex, &[]);
    stop_daemon(&home);

    assert!(!output.status.success());
    let value = parse_stdout(&output);
    assert_eq!(value["doctor"]["ok"], false);
    assert_eq!(check_status(&value, "codex-binary"), "fail");

    let run_dir = home.path().join("run");
    let leftovers = fs::read_dir(&run_dir)
        .unwrap_or_else(|error| panic!("read run dir {}: {error}", run_dir.display()))
        .filter_map(Result::ok)
        .filter(|entry| {
            entry
                .file_name()
                .to_string_lossy()
                .starts_with("doctor-version-")
        })
        .collect::<Vec<_>>();
    assert!(
        leftovers.is_empty(),
        "leftover doctor version capture files: {:?}",
        leftovers
            .iter()
            .map(|entry| entry.file_name())
            .collect::<Vec<_>>()
    );
}

#[test]
fn doctor_cli_version_probe_does_not_wait_for_inherited_stdout_grandchild() {
    let home = temp_home();
    let fake_codex = write_fake_codex_script(&home.path().join("fake-codex"));
    let output = run_doctor(
        &home,
        &fake_codex,
        &[("FAKE_CODEX_VERSION_GRANDCHILD_STDOUT", "1")],
    );
    stop_daemon(&home);

    assert!(
        output.status.success(),
        "doctor failed\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let value = parse_stdout(&output);
    assert_eq!(value["doctor"]["ok"], true);
    assert_eq!(check_status(&value, "codex-binary"), "ok");
}

#[test]
fn doctor_cli_reports_non_loopback_app_server_listener() {
    let home = temp_home();
    let fake_codex = write_fake_codex_script(&home.path().join("fake-codex"));
    let output = run_doctor(
        &home,
        &fake_codex,
        &[("FAKE_CODEX_APP_SERVER_URL", "ws://192.0.2.1:45678")],
    );
    stop_daemon(&home);

    assert!(!output.status.success());
    let value = parse_stdout(&output);
    assert_eq!(value["doctor"]["ok"], false);
    assert_eq!(check_status(&value, "codex-app-server-listener"), "fail");
}

#[test]
fn doctor_cli_reports_malformed_app_server_listener() {
    let home = temp_home();
    let fake_codex = write_fake_codex_script(&home.path().join("fake-codex"));
    let output = run_doctor(
        &home,
        &fake_codex,
        &[("FAKE_CODEX_APP_SERVER_MODE", "malformed")],
    );
    stop_daemon(&home);

    assert!(!output.status.success());
    let value = parse_stdout(&output);
    assert_eq!(value["doctor"]["ok"], false);
    assert_eq!(check_status(&value, "codex-app-server-listener"), "fail");
}

#[test]
fn doctor_cli_help_documents_readiness_side_effects() {
    let output = Command::new(env!("CARGO_BIN_EXE_cbth"))
        .arg("doctor")
        .arg("cli")
        .arg("--help")
        .output()
        .expect("run doctor help");

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("Check local CLI dogfood readiness"));
    assert!(stdout.contains("--codex-bin"));
    assert!(stdout.contains("briefly start a loopback codex app-server"));
}
