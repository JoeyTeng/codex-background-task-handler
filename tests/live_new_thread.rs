#![cfg(unix)]

use std::fs;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStderr, Command, Output, Stdio};
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
#[ignore = "requires a real codex login, model access, and network"]
fn live_codex_new_thread_trusted_all_auto_delivery_is_opt_in() {
    if std::env::var("CBTH_RUN_LIVE_NEW_THREAD_E2E").as_deref() != Ok("1") {
        eprintln!("set CBTH_RUN_LIVE_NEW_THREAD_E2E=1 to run the live new-thread e2e");
        return;
    }

    let codex_bin = std::env::var_os("CBTH_LIVE_CODEX_BIN").unwrap_or_else(|| "codex".into());
    let timeout = live_timeout("CBTH_LIVE_NEW_THREAD_E2E_TIMEOUT_SECONDS", 360);
    let home = temp_home();
    let wrapper_dir = tempfile::tempdir().expect("wrapper dir");
    let exit_file = wrapper_dir.path().join("foreground-exit");
    let wrapper_log = wrapper_dir.path().join("codex-wrapper.log");
    let wrapper = write_live_codex_wrapper(wrapper_dir.path());
    let mut cbth_run = CbthRunGuard::spawn_new_thread(
        home.path(),
        &wrapper,
        &codex_bin,
        &exit_file,
        &wrapper_log,
        timeout,
    );
    let thread_id = cbth_run.wait_for_bound_thread_id(timeout);

    wait_for_live_session_ready(&home, &thread_id, timeout);
    let batch_id = submit_failed_live_batch(&home, &thread_id);
    wait_for_batch_close_reason(&home, &batch_id, "delivered", timeout);
    assert_trusted_all_delivery_audit(&home, &thread_id);

    let (output, stderr) = cbth_run.finish(Duration::from_secs(20));
    assert!(
        output.status.success(),
        "cbth cli run --new-thread failed\nstatus: {}\nstdout: {}\nstderr: {}\nwrapper log: {}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        stderr,
        fs::read_to_string(&wrapper_log).unwrap_or_default()
    );
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
"${CBTH_LIVE_REAL_CODEX_BIN:?}" "$@" &
child="$!"
trap 'kill "$child" 2>/dev/null || true; wait "$child" 2>/dev/null || true' INT TERM EXIT

elapsed=0
while [ "$elapsed" -lt "$timeout_seconds" ]; do
  if [ -f "$exit_file" ]; then
    kill "$child" 2>/dev/null || true
    wait "$child" 2>/dev/null || true
    trap - INT TERM EXIT
    exit 0
  fi
  sleep 1
  elapsed=$((elapsed + 1))
done

kill "$child" 2>/dev/null || true
wait "$child" 2>/dev/null || true
trap - INT TERM EXIT
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
    bound_thread_rx: mpsc::Receiver<String>,
    stderr_reader: Option<thread::JoinHandle<String>>,
}

impl CbthRunGuard {
    fn spawn_new_thread(
        home: &Path,
        wrapper: &Path,
        real_codex_bin: &std::ffi::OsStr,
        exit_file: &Path,
        wrapper_log: &Path,
        timeout: Duration,
    ) -> Self {
        let mut child = Command::new(env!("CARGO_BIN_EXE_cbth"))
            .arg("--home")
            .arg(home)
            .arg("cli")
            .arg("run")
            .arg("--new-thread")
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
            .current_dir(env!("CARGO_MANIFEST_DIR"))
            .env("CBTH_LIVE_REAL_CODEX_BIN", real_codex_bin)
            .env("CBTH_LIVE_FOREGROUND_EXIT_FILE", exit_file)
            .env("CBTH_LIVE_WRAPPER_LOG", wrapper_log)
            .env(
                "CBTH_LIVE_FOREGROUND_TIMEOUT_SECONDS",
                timeout.as_secs().to_string(),
            )
            .spawn()
            .expect("spawn cbth cli run live new-thread");
        let stderr = child.stderr.take().expect("capture cbth stderr");
        let (bound_thread_tx, bound_thread_rx) = mpsc::channel();
        let stderr_reader = spawn_cbth_stderr_reader(stderr, bound_thread_tx);

        Self {
            child: Some(child),
            exit_file: exit_file.to_path_buf(),
            home: home.to_path_buf(),
            bound_thread_rx,
            stderr_reader: Some(stderr_reader),
        }
    }

    fn wait_for_bound_thread_id(&mut self, timeout: Duration) -> String {
        self.bound_thread_rx
            .recv_timeout(timeout)
            .expect("cbth did not print a bound thread id")
    }

    fn finish(mut self, timeout: Duration) -> (Output, String) {
        self.signal_exit();
        let child = self.child.take().expect("cbth child");
        let output = wait_with_timeout(child, timeout);
        let stderr = self
            .stderr_reader
            .take()
            .expect("stderr reader")
            .join()
            .unwrap_or_else(|_| "stderr reader panicked".to_owned());
        stop_daemon(&self.home);
        (output, stderr)
    }

    fn signal_exit(&self) {
        let _ = fs::write(&self.exit_file, b"exit\n");
    }
}

impl Drop for CbthRunGuard {
    fn drop(&mut self) {
        self.signal_exit();
        if let Some(child) = self.child.take() {
            terminate_child_best_effort(child, Duration::from_secs(5));
        }
        if let Some(stderr_reader) = self.stderr_reader.take() {
            let _ = stderr_reader.join();
        }
        stop_daemon(&self.home);
    }
}

fn terminate_child_best_effort(mut child: Child, graceful_timeout: Duration) {
    let deadline = Instant::now() + graceful_timeout;
    loop {
        match child.try_wait() {
            Ok(Some(_)) => {
                let _ = child.wait();
                return;
            }
            Ok(None) if Instant::now() < deadline => {
                thread::sleep(Duration::from_millis(100));
            }
            Ok(None) | Err(_) => {
                let _ = child.kill();
                let _ = child.wait();
                return;
            }
        }
    }
}

fn spawn_cbth_stderr_reader(
    stderr: ChildStderr,
    bound_thread_tx: mpsc::Sender<String>,
) -> thread::JoinHandle<String> {
    thread::spawn(move || {
        let reader = BufReader::new(stderr);
        let mut stderr_text = String::new();
        let mut sent = false;
        for line in reader.lines() {
            let Ok(line) = line else {
                break;
            };
            if !sent && let Some(thread_id) = parse_bound_thread_line(&line) {
                let _ = bound_thread_tx.send(thread_id);
                sent = true;
            }
            stderr_text.push_str(&line);
            stderr_text.push('\n');
        }
        stderr_text
    })
}

fn parse_bound_thread_line(line: &str) -> Option<String> {
    line.strip_prefix("cbth: bound thread id: ")
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
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
            "live new-thread trusted-all e2e exact-reply marker",
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
            "Live new-thread trusted-all e2e: reply with exactly `CBTH_LIVE_NEW_THREAD_DELIVERED` and nothing else. Do not run tools.",
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
            let queried: rusqlite::Result<(String, i64, i64, i64, i64, i64, i64)> = conn.query_row(
                "SELECT activity_state, capability_thread_resume, capability_thread_start, capability_turn_start,
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
                        row.get(6)?,
                    ))
                },
            );
            match queried {
                Ok((
                    activity_state,
                    thread_resume,
                    thread_start,
                    turn_start,
                    current_state_sync,
                    turn_completed_event,
                    negative_terminal_events,
                )) => {
                    last_seen = format!(
                        "activity_state={activity_state}, thread_resume={thread_resume}, \
                         thread_start={thread_start}, \
                         turn_start={turn_start}, current_state_sync={current_state_sync}, \
                         turn_completed_event={turn_completed_event}, \
                         negative_terminal_events={negative_terminal_events}"
                    );
                    if activity_state == "idle"
                        && (thread_resume != 0 || thread_start != 0)
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
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
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
        let has_allow = decisions.iter().any(|decision| {
            decision["decision"] == "allow"
                && decision["policy_kind"] == "trusted_all"
                && decision["details"]["marker"]
                    .as_str()
                    .is_some_and(|marker| marker.starts_with("cbth-delivery-marker:"))
        });
        let has_attempt_start = decisions
            .iter()
            .any(|decision| decision["decision"] == "attempt-start");
        let has_accepted = decisions
            .iter()
            .any(|decision| decision["decision"] == "accepted");
        let has_terminal = decisions.iter().any(|decision| {
            decision["decision"] == "observed" || decision["decision"] == "reconciled"
        });
        if has_allow && has_attempt_start && has_accepted && has_terminal {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "audit missing expected trusted-all records: allow={has_allow}, \
             attempt-start={has_attempt_start}, accepted={has_accepted}, \
             terminal={has_terminal}; audit: {audit}"
        );
        thread::sleep(Duration::from_millis(250));
    }
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

fn live_timeout(name: &str, default_seconds: u64) -> Duration {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .map(Duration::from_secs)
        .unwrap_or_else(|| Duration::from_secs(default_seconds))
}
