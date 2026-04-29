use std::fs;
use std::process::{Child, Command, Output, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use rusqlite::Connection;
use tempfile::TempDir;

#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

fn temp_home() -> TempDir {
    let home = tempfile::tempdir().expect("temp home");
    #[cfg(unix)]
    fs::set_permissions(home.path(), fs::Permissions::from_mode(0o700)).expect("chmod temp home");
    home
}

#[cfg(unix)]
fn fake_codex_script(dir: &TempDir) -> std::path::PathBuf {
    let path = dir.path().join("fake-codex");
    fs::write(
        &path,
        r#"#!/bin/sh
log="${FAKE_CODEX_LOG:?}"
if [ "${1:-}" = "app-server" ]; then
  printf 'app-server' >> "$log"
  for arg in "$@"; do
    printf '\t%s' "$arg" >> "$log"
  done
  printf '\n' >> "$log"
  if [ -n "${FAKE_CODEX_APP_SERVER_PREFIX_BYTES:-}" ]; then
    i=0
    while [ "$i" -lt "$FAKE_CODEX_APP_SERVER_PREFIX_BYTES" ]; do
      printf x
      i=$((i + 1))
    done
  fi
  url="${FAKE_CODEX_APP_SERVER_URL:-ws://127.0.0.1:45678}"
  printf 'codex app-server\n'
  if [ -n "${FAKE_CODEX_APP_SERVER_STARTUP_SLEEP_SECONDS:-}" ]; then
    sleep "$FAKE_CODEX_APP_SERVER_STARTUP_SLEEP_SECONDS"
  fi
  printf '  listening on: %s\n' "$url"
  if [ "${FAKE_CODEX_APP_SERVER_GRANDCHILD_STDOUT:-}" = "1" ]; then
    (trap '' TERM; while :; do sleep 1; done) &
    exit 0
  fi
  while :; do
    sleep 1
  done
fi

printf 'foreground' >> "$log"
for arg in "$@"; do
  printf '\t%s' "$arg" >> "$log"
done
printf '\n' >> "$log"
if [ -n "${FAKE_CODEX_FOREGROUND_SLEEP_SECONDS:-}" ]; then
  sleep "$FAKE_CODEX_FOREGROUND_SLEEP_SECONDS"
fi
exit 0
"#,
    )
    .expect("write fake codex");
    fs::set_permissions(&path, fs::Permissions::from_mode(0o700)).expect("chmod fake codex");
    path
}

#[cfg(unix)]
fn wait_for_log_contains(path: &std::path::Path, needle: &str) {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if fs::read_to_string(path).is_ok_and(|log| log.contains(needle)) {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for log entry {needle:?}"
        );
        thread::sleep(Duration::from_millis(50));
    }
}

#[cfg(unix)]
fn wait_with_timeout(mut child: Child, timeout: Duration) -> Output {
    let deadline = Instant::now() + timeout;
    loop {
        match child.try_wait().expect("poll child") {
            Some(_status) => return child.wait_with_output().expect("collect child output"),
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

#[cfg(unix)]
#[test]
fn cli_run_binds_session_starts_foreground_codex_and_stops_app_server() {
    let home = temp_home();
    let client_cwd = tempfile::tempdir().expect("client cwd");
    let script_dir = tempfile::tempdir().expect("script dir");
    let fake_codex = fake_codex_script(&script_dir);
    let log_path = script_dir.path().join("fake-codex.log");

    let output = Command::new(env!("CARGO_BIN_EXE_cbth"))
        .arg("--home")
        .arg(home.path())
        .arg("cli")
        .arg("run")
        .arg("--bind-thread-id")
        .arg("thread-cli-run")
        .arg("--session-allows-approval")
        .arg("false")
        .arg("--session-allows-network")
        .arg("false")
        .arg("--session-allows-write-access")
        .arg("false")
        .arg("--codex-bin")
        .arg(&fake_codex)
        .arg("--")
        .arg("--model")
        .arg("gpt-test")
        .current_dir(client_cwd.path())
        .env("FAKE_CODEX_LOG", &log_path)
        .output()
        .expect("run cbth cli run");

    assert!(
        output.status.success(),
        "cbth cli run failed\nstatus: {}\nstdout: {}\nstderr: {}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    let log = fs::read_to_string(&log_path).expect("read fake codex log");
    assert!(log.contains("app-server\tapp-server\t--listen\tws://127.0.0.1:0"));
    assert!(log.contains("foreground\t--remote\tws://127.0.0.1:45678\t--cd\t"));
    assert!(log.contains(&client_cwd.path().display().to_string()));
    assert!(log.contains("\t--model\tgpt-test"));

    let conn = Connection::open(home.path().join("cbth.sqlite3")).expect("open db");
    let (managed_session_id, session_state, session_epoch): (String, String, i64) = conn
        .query_row(
            "SELECT managed_session_id, session_state, session_epoch
             FROM cli_managed_sessions
             WHERE bound_thread_id = ?",
            ["thread-cli-run"],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .expect("query managed session");
    assert!(!managed_session_id.is_empty());
    assert_eq!(session_state, "live");
    assert_eq!(session_epoch, 1);

    let status_output = Command::new(env!("CARGO_BIN_EXE_cbth"))
        .arg("--home")
        .arg(home.path())
        .arg("daemon")
        .arg("status")
        .output()
        .expect("daemon status");
    assert!(
        status_output.status.success(),
        "daemon status failed\nstatus: {}\nstdout: {}\nstderr: {}",
        status_output.status,
        String::from_utf8_lossy(&status_output.stdout),
        String::from_utf8_lossy(&status_output.stderr)
    );
    let status: serde_json::Value =
        serde_json::from_slice(&status_output.stdout).expect("status json");
    assert_eq!(status["cli_app_servers"], serde_json::json!([]));

    let _ = Command::new(env!("CARGO_BIN_EXE_cbth"))
        .arg("--home")
        .arg(home.path())
        .arg("daemon")
        .arg("stop")
        .output();
}

#[cfg(unix)]
#[test]
fn cli_run_reservation_rejects_duplicate_before_session_epoch_bump() {
    let home = temp_home();
    let script_dir = tempfile::tempdir().expect("script dir");
    let fake_codex = fake_codex_script(&script_dir);
    let log_path = script_dir.path().join("fake-codex.log");

    let first = Command::new(env!("CARGO_BIN_EXE_cbth"))
        .arg("--home")
        .arg(home.path())
        .arg("cli")
        .arg("run")
        .arg("--bind-thread-id")
        .arg("thread-cli-run-reservation")
        .arg("--session-allows-approval")
        .arg("false")
        .arg("--session-allows-network")
        .arg("false")
        .arg("--session-allows-write-access")
        .arg("false")
        .arg("--codex-bin")
        .arg(&fake_codex)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .env("FAKE_CODEX_LOG", &log_path)
        .env("FAKE_CODEX_APP_SERVER_STARTUP_SLEEP_SECONDS", "2")
        .spawn()
        .expect("spawn first cbth cli run");

    wait_for_log_contains(
        &log_path,
        "app-server\tapp-server\t--listen\tws://127.0.0.1:0",
    );

    let second = Command::new(env!("CARGO_BIN_EXE_cbth"))
        .arg("--home")
        .arg(home.path())
        .arg("cli")
        .arg("run")
        .arg("--bind-thread-id")
        .arg("thread-cli-run-reservation")
        .arg("--session-allows-approval")
        .arg("false")
        .arg("--session-allows-network")
        .arg("false")
        .arg("--session-allows-write-access")
        .arg("false")
        .arg("--codex-bin")
        .arg(&fake_codex)
        .env("FAKE_CODEX_LOG", &log_path)
        .output()
        .expect("run second cbth cli run");

    assert!(
        !second.status.success(),
        "second cli run unexpectedly succeeded\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&second.stdout),
        String::from_utf8_lossy(&second.stderr)
    );
    assert!(
        String::from_utf8_lossy(&second.stderr).contains("active CLI app-server reservation"),
        "unexpected second stderr: {}",
        String::from_utf8_lossy(&second.stderr)
    );

    let first_output = first.wait_with_output().expect("wait for first cli run");
    assert!(
        first_output.status.success(),
        "first cli run failed\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&first_output.stdout),
        String::from_utf8_lossy(&first_output.stderr)
    );

    let conn = Connection::open(home.path().join("cbth.sqlite3")).expect("open db");
    let session_epoch: i64 = conn
        .query_row(
            "SELECT session_epoch FROM cli_managed_sessions WHERE bound_thread_id = ?",
            ["thread-cli-run-reservation"],
            |row| row.get(0),
        )
        .expect("query managed session epoch");
    assert_eq!(session_epoch, 1);

    let _ = Command::new(env!("CARGO_BIN_EXE_cbth"))
        .arg("--home")
        .arg(home.path())
        .arg("daemon")
        .arg("stop")
        .output();
}

#[cfg(unix)]
#[test]
fn cli_run_stop_returns_when_grandchild_keeps_app_server_stdout_open() {
    let home = temp_home();
    let script_dir = tempfile::tempdir().expect("script dir");
    let fake_codex = fake_codex_script(&script_dir);
    let log_path = script_dir.path().join("fake-codex.log");

    let child = Command::new(env!("CARGO_BIN_EXE_cbth"))
        .arg("--home")
        .arg(home.path())
        .arg("cli")
        .arg("run")
        .arg("--bind-thread-id")
        .arg("thread-cli-run-grandchild")
        .arg("--session-allows-approval")
        .arg("false")
        .arg("--session-allows-network")
        .arg("false")
        .arg("--session-allows-write-access")
        .arg("false")
        .arg("--codex-bin")
        .arg(&fake_codex)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .env("FAKE_CODEX_LOG", &log_path)
        .env("FAKE_CODEX_APP_SERVER_GRANDCHILD_STDOUT", "1")
        .spawn()
        .expect("spawn cbth cli run");
    let output = wait_with_timeout(child, Duration::from_secs(5));

    assert!(
        output.status.success(),
        "cli run failed\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    let _ = Command::new(env!("CARGO_BIN_EXE_cbth"))
        .arg("--home")
        .arg(home.path())
        .arg("daemon")
        .arg("stop")
        .output();
}

#[cfg(unix)]
#[test]
fn cli_run_rejects_duplicate_active_thread_before_stealing_lease() {
    let home = temp_home();
    let script_dir = tempfile::tempdir().expect("script dir");
    let fake_codex = fake_codex_script(&script_dir);
    let log_path = script_dir.path().join("fake-codex.log");

    let first = Command::new(env!("CARGO_BIN_EXE_cbth"))
        .arg("--home")
        .arg(home.path())
        .arg("cli")
        .arg("run")
        .arg("--bind-thread-id")
        .arg("thread-cli-run-duplicate")
        .arg("--session-allows-approval")
        .arg("false")
        .arg("--session-allows-network")
        .arg("false")
        .arg("--session-allows-write-access")
        .arg("false")
        .arg("--codex-bin")
        .arg(&fake_codex)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .env("FAKE_CODEX_LOG", &log_path)
        .env("FAKE_CODEX_FOREGROUND_SLEEP_SECONDS", "2")
        .spawn()
        .expect("spawn first cbth cli run");

    wait_for_log_contains(&log_path, "foreground\t--remote\tws://127.0.0.1:45678");

    let second = Command::new(env!("CARGO_BIN_EXE_cbth"))
        .arg("--home")
        .arg(home.path())
        .arg("cli")
        .arg("run")
        .arg("--bind-thread-id")
        .arg("thread-cli-run-duplicate")
        .arg("--session-allows-approval")
        .arg("false")
        .arg("--session-allows-network")
        .arg("false")
        .arg("--session-allows-write-access")
        .arg("false")
        .arg("--codex-bin")
        .arg(&fake_codex)
        .env("FAKE_CODEX_LOG", &log_path)
        .output()
        .expect("run second cbth cli run");

    assert!(
        !second.status.success(),
        "second cli run unexpectedly succeeded\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&second.stdout),
        String::from_utf8_lossy(&second.stderr)
    );
    assert!(
        String::from_utf8_lossy(&second.stderr).contains("already has an active CLI app-server"),
        "unexpected second stderr: {}",
        String::from_utf8_lossy(&second.stderr)
    );

    let first_output = first.wait_with_output().expect("wait for first cli run");
    assert!(
        first_output.status.success(),
        "first cli run failed\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&first_output.stdout),
        String::from_utf8_lossy(&first_output.stderr)
    );

    let log = fs::read_to_string(&log_path).expect("read fake codex log");
    assert_eq!(log.matches("foreground\t--remote").count(), 1);
    let conn = Connection::open(home.path().join("cbth.sqlite3")).expect("open db");
    let session_epoch: i64 = conn
        .query_row(
            "SELECT session_epoch FROM cli_managed_sessions WHERE bound_thread_id = ?",
            ["thread-cli-run-duplicate"],
            |row| row.get(0),
        )
        .expect("query managed session epoch");
    assert_eq!(session_epoch, 1);

    let _ = Command::new(env!("CARGO_BIN_EXE_cbth"))
        .arg("--home")
        .arg(home.path())
        .arg("daemon")
        .arg("stop")
        .output();
}

#[cfg(unix)]
#[test]
fn cli_run_rejects_non_loopback_app_server_listener() {
    let home = temp_home();
    let script_dir = tempfile::tempdir().expect("script dir");
    let fake_codex = fake_codex_script(&script_dir);
    let log_path = script_dir.path().join("fake-codex.log");

    let output = Command::new(env!("CARGO_BIN_EXE_cbth"))
        .arg("--home")
        .arg(home.path())
        .arg("cli")
        .arg("run")
        .arg("--bind-thread-id")
        .arg("thread-cli-run-bad-url")
        .arg("--session-allows-approval")
        .arg("false")
        .arg("--session-allows-network")
        .arg("false")
        .arg("--session-allows-write-access")
        .arg("false")
        .arg("--codex-bin")
        .arg(&fake_codex)
        .env("FAKE_CODEX_LOG", &log_path)
        .env(
            "FAKE_CODEX_APP_SERVER_URL",
            "ws://127.0.0.1:45678@remote.example",
        )
        .output()
        .expect("run cbth cli run");

    assert!(
        !output.status.success(),
        "cli run unexpectedly succeeded\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("non-loopback listener"),
        "unexpected stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let log = fs::read_to_string(&log_path).expect("read fake codex log");
    assert!(!log.contains("foreground\t--remote"));

    let _ = Command::new(env!("CARGO_BIN_EXE_cbth"))
        .arg("--home")
        .arg(home.path())
        .arg("daemon")
        .arg("stop")
        .output();
}
