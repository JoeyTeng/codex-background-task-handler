#![cfg(unix)]

use std::fs;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStderr, ChildStdout, Command, Output, Stdio};
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

use rusqlite::Connection;
use tempfile::TempDir;

use std::os::unix::fs::PermissionsExt;

fn temp_home() -> TempDir {
    let home = tempfile::tempdir().expect("temp home");
    fs::set_permissions(home.path(), fs::Permissions::from_mode(0o700)).expect("chmod temp home");
    home
}

#[test]
#[ignore = "requires a real codex login, model access, node, and network"]
fn live_codex_trusted_all_auto_delivery_is_opt_in() {
    if std::env::var("CBTH_RUN_LIVE_TRUSTED_ALL_E2E").as_deref() != Ok("1") {
        eprintln!("set CBTH_RUN_LIVE_TRUSTED_ALL_E2E=1 to run the live trusted-all e2e");
        return;
    }

    let codex_bin = std::env::var_os("CBTH_LIVE_CODEX_BIN").unwrap_or_else(|| "codex".into());
    let node_bin = std::env::var_os("CBTH_LIVE_NODE_BIN").unwrap_or_else(|| "node".into());
    let timeout = live_timeout("CBTH_LIVE_TRUSTED_ALL_E2E_TIMEOUT_SECONDS", 360);
    let home = temp_home();
    let thread_id = create_live_codex_thread(&codex_bin, &node_bin, timeout);

    let wrapper_dir = tempfile::tempdir().expect("wrapper dir");
    let exit_file = wrapper_dir.path().join("foreground-exit");
    let wrapper_log = wrapper_dir.path().join("codex-wrapper.log");
    let wrapper = write_live_codex_wrapper(wrapper_dir.path());
    let cbth_run = CbthRunGuard::spawn(
        home.path(),
        &thread_id,
        &wrapper,
        &codex_bin,
        &exit_file,
        &wrapper_log,
        timeout,
    );

    wait_for_live_session_ready(&home, &thread_id, timeout);
    let batch_id = submit_failed_live_batch(&home, &thread_id);
    wait_for_batch_close_reason(&home, &batch_id, "delivered", timeout);
    assert_trusted_all_delivery_audit(&home, &thread_id);

    let output = cbth_run.finish(Duration::from_secs(20));
    assert!(
        output.status.success(),
        "cbth cli run failed\nstatus: {}\nstdout: {}\nstderr: {}\nwrapper log: {}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
        fs::read_to_string(&wrapper_log).unwrap_or_default()
    );
}

fn create_live_codex_thread(
    codex_bin: &std::ffi::OsStr,
    node_bin: &std::ffi::OsStr,
    timeout: Duration,
) -> String {
    let mut child = Command::new(codex_bin)
        .arg("app-server")
        .arg("--listen")
        .arg("ws://127.0.0.1:0")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn bootstrap codex app-server");
    let stdout = child
        .stdout
        .take()
        .expect("capture bootstrap app-server stdout");
    let stderr = child
        .stderr
        .take()
        .expect("capture bootstrap app-server stderr");
    let mut app_server = ChildGuard(child);
    let listener_url = wait_for_app_server_listener(stdout, stderr, Duration::from_secs(15));

    let script =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("scripts/cli_shared_app_server_poc.mjs");
    let output = Command::new(node_bin)
        .arg(script)
        .arg("--url")
        .arg(&listener_url)
        .arg("--cwd")
        .arg(env!("CARGO_MANIFEST_DIR"))
        .arg("--timeout-ms")
        .arg(timeout.as_millis().to_string())
        .arg("--seed-message")
        .arg("Reply with exactly `CBTH_LIVE_TRUSTED_ALL_BOOTSTRAP_READY` and nothing else.")
        .arg("--seed-only")
        .output()
        .expect("bootstrap live caller thread");
    app_server.kill_and_wait();

    assert!(
        output.status.success(),
        "live thread bootstrap failed\nstatus: {}\nstdout: {}\nstderr: {}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let summary: serde_json::Value =
        serde_json::from_slice(&output.stdout).expect("bootstrap summary JSON");
    summary["thread_id"]
        .as_str()
        .expect("bootstrap thread id")
        .to_owned()
}

fn write_live_codex_wrapper(dir: &Path) -> PathBuf {
    let path = dir.join("codex-live-wrapper");
    fs::write(
        &path,
        r#"#!/bin/sh
set -eu

log="${CBTH_LIVE_WRAPPER_LOG:?}"
{
  for arg in "$@"; do
    printf '%s\t' "$arg"
  done
  printf '\n'
} >> "$log"

if [ "${1:-}" = "app-server" ]; then
  exec "${CBTH_LIVE_REAL_CODEX_BIN:?}" "$@"
fi

exit_file="${CBTH_LIVE_FOREGROUND_EXIT_FILE:?}"
timeout_seconds="${CBTH_LIVE_FOREGROUND_TIMEOUT_SECONDS:-360}"
elapsed=0
while [ "$elapsed" -lt "$timeout_seconds" ]; do
  if [ -f "$exit_file" ]; then
    exit 0
  fi
  sleep 1
  elapsed=$((elapsed + 1))
done

echo "cbth live foreground wrapper timed out" >&2
exit 124
"#,
    )
    .expect("write live codex wrapper");
    fs::set_permissions(&path, fs::Permissions::from_mode(0o700))
        .expect("chmod live codex wrapper");
    path
}

struct CbthRunGuard {
    child: Option<Child>,
    exit_file: PathBuf,
    home: PathBuf,
}

impl CbthRunGuard {
    fn spawn(
        home: &Path,
        thread_id: &str,
        wrapper: &Path,
        real_codex_bin: &std::ffi::OsStr,
        exit_file: &Path,
        wrapper_log: &Path,
        timeout: Duration,
    ) -> Self {
        let child = Command::new(env!("CARGO_BIN_EXE_cbth"))
            .arg("--home")
            .arg(home)
            .arg("cli")
            .arg("run")
            .arg("--bind-thread-id")
            .arg(thread_id)
            .arg("--session-allows-approval")
            .arg("false")
            .arg("--session-allows-network")
            .arg("false")
            .arg("--session-allows-write-access")
            .arg("false")
            .arg("--auto-delivery-policy")
            .arg("trusted-all")
            .arg("--codex-bin")
            .arg(wrapper)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .env("CBTH_LIVE_REAL_CODEX_BIN", real_codex_bin)
            .env("CBTH_LIVE_FOREGROUND_EXIT_FILE", exit_file)
            .env("CBTH_LIVE_WRAPPER_LOG", wrapper_log)
            .env(
                "CBTH_LIVE_FOREGROUND_TIMEOUT_SECONDS",
                timeout.as_secs().to_string(),
            )
            .spawn()
            .expect("spawn cbth cli run live trusted-all");

        Self {
            child: Some(child),
            exit_file: exit_file.to_path_buf(),
            home: home.to_path_buf(),
        }
    }

    fn finish(mut self, timeout: Duration) -> Output {
        self.signal_exit();
        let child = self.child.take().expect("cbth child");
        let output = wait_with_timeout(child, timeout);
        stop_daemon(&self.home);
        output
    }

    fn signal_exit(&self) {
        let _ = fs::write(&self.exit_file, b"exit\n");
    }
}

impl Drop for CbthRunGuard {
    fn drop(&mut self) {
        self.signal_exit();
        if let Some(child) = &mut self.child {
            let _ = child.kill();
            let _ = child.wait();
        }
        stop_daemon(&self.home);
    }
}

struct ChildGuard(Child);

impl ChildGuard {
    fn kill_and_wait(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

impl Drop for ChildGuard {
    fn drop(&mut self) {
        self.kill_and_wait();
    }
}

fn submit_failed_live_batch(home: &TempDir, thread_id: &str) -> String {
    let submitted = cbth_json(
        home.path(),
        &[
            "job",
            "submit",
            "--source-thread-id",
            thread_id,
            "--summary",
            "live trusted-all e2e auto delivery",
            "--delivery-read-only",
            "true",
            "--delivery-requires-approval",
            "false",
            "--delivery-requires-network",
            "false",
            "--delivery-requires-write-access",
            "false",
        ],
    );
    let job_id = submitted["job"]["job_id"].as_str().expect("job id");
    let failed = cbth_json(
        home.path(),
        &[
            "job",
            "fail",
            "--job-id",
            job_id,
            "--reason",
            "Live trusted-all e2e marker job is ready for automatic delivery.",
            "--max-delivery-attempts",
            "2",
        ],
    );
    failed["batch"]["batch"]["batch_id"]
        .as_str()
        .expect("batch id")
        .to_owned()
}

fn wait_for_live_session_ready(home: &TempDir, bound_thread_id: &str, timeout: Duration) {
    let db_path = home.path().join("cbth.sqlite3");
    let deadline = Instant::now() + timeout;
    let mut last_seen = "no session row".to_owned();
    loop {
        if db_path.exists()
            && let Ok(conn) = Connection::open(&db_path)
        {
            let queried: rusqlite::Result<(String, i64, i64, i64, i64, i64)> = conn.query_row(
                "SELECT activity_state, capability_thread_resume, capability_turn_start,
                        capability_current_state_sync, capability_turn_completed_event,
                        capability_negative_terminal_events
                 FROM cli_managed_sessions
                 WHERE bound_thread_id = ?",
                [bound_thread_id],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                        row.get(5)?,
                    ))
                },
            );
            match queried {
                Ok((
                    activity_state,
                    thread_resume,
                    turn_start,
                    current_state_sync,
                    turn_completed_event,
                    negative_terminal_events,
                )) => {
                    last_seen = format!(
                        "activity_state={activity_state}, thread_resume={thread_resume}, \
                         turn_start={turn_start}, current_state_sync={current_state_sync}, \
                         turn_completed_event={turn_completed_event}, \
                         negative_terminal_events={negative_terminal_events}"
                    );
                    if activity_state == "idle"
                        && thread_resume != 0
                        && turn_start != 0
                        && current_state_sync != 0
                        && turn_completed_event != 0
                        && negative_terminal_events != 0
                    {
                        return;
                    }
                }
                Err(error) => {
                    last_seen = format!("session row unavailable: {error}");
                }
            }
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for live session readiness for {bound_thread_id}; last seen: {last_seen}"
        );
        thread::sleep(Duration::from_millis(250));
    }
}

fn wait_for_batch_close_reason(
    home: &TempDir,
    batch_id: &str,
    close_reason: &str,
    timeout: Duration,
) {
    let deadline = Instant::now() + timeout;
    loop {
        let inspected =
            cbth_direct_json(home.path(), &["batch", "inspect", "--batch-id", batch_id]);
        let batch = &inspected["batch"]["batch"];
        if batch["state"] == "closed" && batch["close_reason"] == close_reason {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for batch {batch_id} to close as {close_reason}; last batch: {batch}"
        );
        thread::sleep(Duration::from_secs(1));
    }
}

fn assert_trusted_all_delivery_audit(home: &TempDir, thread_id: &str) {
    let audit = cbth_direct_json(
        home.path(),
        &[
            "audit",
            "list",
            "--source-thread-id",
            thread_id,
            "--limit",
            "50",
        ],
    );
    let decisions = audit["audit"].as_array().expect("audit list");
    assert!(
        decisions
            .iter()
            .any(|decision| decision["decision"] == "allow"
                && decision["policy_kind"] == "trusted_all"
                && decision["details"]["marker"]
                    .as_str()
                    .is_some_and(|marker| marker.starts_with("cbth-delivery-marker:"))),
        "audit missing trusted-all allow marker: {audit}"
    );
    assert!(
        decisions
            .iter()
            .any(|decision| decision["decision"] == "attempt-start"),
        "audit missing attempt-start: {audit}"
    );
    assert!(
        decisions
            .iter()
            .any(|decision| decision["decision"] == "accepted"),
        "audit missing accepted: {audit}"
    );
    assert!(
        decisions
            .iter()
            .any(|decision| decision["decision"] == "observed"
                || decision["decision"] == "reconciled"),
        "audit missing terminal observation or reconcile: {audit}"
    );
}

fn cbth_json(home: &Path, args: &[&str]) -> serde_json::Value {
    run_cbth_json(home, args, false)
}

fn cbth_direct_json(home: &Path, args: &[&str]) -> serde_json::Value {
    run_cbth_json(home, args, true)
}

fn run_cbth_json(home: &Path, args: &[&str], direct_store: bool) -> serde_json::Value {
    let mut command = Command::new(env!("CARGO_BIN_EXE_cbth"));
    if direct_store {
        command.env("CBTH_ALLOW_DIRECT_STORE", "1");
        command.arg("--direct-store");
    }
    let output = command
        .arg("--home")
        .arg(home)
        .args(args)
        .output()
        .expect("run cbth");

    assert!(
        output.status.success(),
        "cbth failed\nstatus: {}\nstdout: {}\nstderr: {}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).expect("valid cbth JSON")
}

fn stop_daemon(home: &Path) {
    let _ = Command::new(env!("CARGO_BIN_EXE_cbth"))
        .arg("--home")
        .arg(home)
        .arg("daemon")
        .arg("stop")
        .output();
}

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
            None => thread::sleep(Duration::from_millis(100)),
        }
    }
}

fn wait_for_app_server_listener(
    stdout: ChildStdout,
    stderr: ChildStderr,
    timeout: Duration,
) -> String {
    let (tx, rx) = mpsc::channel();
    spawn_listener_reader(stdout, tx.clone());
    spawn_listener_reader(stderr, tx);

    let deadline = Instant::now() + timeout;
    let mut closed_streams = 0;
    while closed_streams < 2 {
        let remaining = deadline.saturating_duration_since(Instant::now());
        assert!(
            !remaining.is_zero(),
            "timed out waiting for codex app-server listener URL"
        );
        match rx.recv_timeout(remaining) {
            Ok(ListenerEvent::Url(url)) => return url,
            Ok(ListenerEvent::Closed) => closed_streams += 1,
            Err(error) => panic!("timed out waiting for codex app-server listener URL: {error}"),
        }
    }
    panic!("codex app-server did not print a listener URL");
}

enum ListenerEvent {
    Url(String),
    Closed,
}

fn spawn_listener_reader<R>(stream: R, tx: mpsc::Sender<ListenerEvent>)
where
    R: std::io::Read + Send + 'static,
{
    thread::spawn(move || {
        let reader = BufReader::new(stream);
        for line in reader.lines() {
            let Ok(line) = line else {
                break;
            };
            if let Some(url) = parse_app_server_listener_url(&line) {
                let _ = tx.send(ListenerEvent::Url(url));
                return;
            }
        }
        let _ = tx.send(ListenerEvent::Closed);
    });
}

fn parse_app_server_listener_url(line: &str) -> Option<String> {
    let value = line.trim_start().strip_prefix("listening on:")?;
    let url = value.split_whitespace().next()?;
    url.starts_with("ws://").then(|| url.to_owned())
}

fn live_timeout(name: &str, default_seconds: u64) -> Duration {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .map(Duration::from_secs)
        .unwrap_or_else(|| Duration::from_secs(default_seconds))
}
