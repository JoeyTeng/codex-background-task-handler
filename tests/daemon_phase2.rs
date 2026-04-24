use std::fs;
use std::io::Write;
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use rusqlite::Connection;
use serde_json::Value;
use tempfile::TempDir;

#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
#[cfg(unix)]
use std::os::unix::net::UnixListener;

fn cbth(home: &TempDir, args: &[&str]) -> Value {
    let output = Command::new(env!("CARGO_BIN_EXE_cbth"))
        .arg("--home")
        .arg(home.path())
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

    serde_json::from_slice(&output.stdout).expect("valid json output")
}

fn cbth_failure(home: &TempDir, args: &[&str]) -> String {
    let output = Command::new(env!("CARGO_BIN_EXE_cbth"))
        .arg("--home")
        .arg(home.path())
        .args(args)
        .output()
        .expect("run cbth");

    assert!(
        !output.status.success(),
        "cbth unexpectedly succeeded\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8_lossy(&output.stderr).into_owned()
}

fn try_cbth(home: &TempDir, args: &[&str]) -> Option<Value> {
    let output = Command::new(env!("CARGO_BIN_EXE_cbth"))
        .arg("--home")
        .arg(home.path())
        .args(args)
        .output()
        .expect("run cbth");

    if output.status.success() {
        Some(serde_json::from_slice(&output.stdout).expect("valid json output"))
    } else {
        None
    }
}

fn spawn_daemon(home: &TempDir, idle_timeout_seconds: &str, extra_args: &[&str]) -> Child {
    Command::new(env!("CARGO_BIN_EXE_cbth"))
        .arg("--home")
        .arg(home.path())
        .arg("daemon")
        .arg("serve")
        .arg("--idle-timeout-seconds")
        .arg(idle_timeout_seconds)
        .args(extra_args)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn daemon")
}

fn wait_for_ping(home: &TempDir) -> Value {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if let Some(value) = try_cbth(home, &["daemon", "ping"]) {
            return value;
        }
        assert!(Instant::now() < deadline, "daemon did not become ready");
        thread::sleep(Duration::from_millis(50));
    }
}

fn wait_for_socket_removed(home: &TempDir) {
    let socket_path = home.path().join("run").join("cbth.sock");
    let deadline = Instant::now() + Duration::from_secs(5);
    while socket_path.exists() {
        assert!(Instant::now() < deadline, "daemon socket was not removed");
        thread::sleep(Duration::from_millis(50));
    }
}

#[test]
fn concurrent_daemon_ensure_uses_one_daemon() {
    let home = tempfile::tempdir().expect("temp home");
    let mut children = Vec::new();
    for _ in 0..6 {
        children.push(
            Command::new(env!("CARGO_BIN_EXE_cbth"))
                .arg("--home")
                .arg(home.path())
                .arg("daemon")
                .arg("ensure")
                .arg("--idle-timeout-seconds")
                .arg("10")
                .arg("--startup-timeout-seconds")
                .arg("5")
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .spawn()
                .expect("spawn ensure"),
        );
    }

    let mut daemon_pid = None;
    let mut started_count = 0;
    for child in children {
        let output = child.wait_with_output().expect("run ensure");
        assert!(
            output.status.success(),
            "ensure failed\nstdout: {}\nstderr: {}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        let value: Value = serde_json::from_slice(&output.stdout).expect("ensure json");
        if value["started"] == true {
            started_count += 1;
        }
        let pid = value["daemon"]["pid"].as_u64().expect("daemon pid");
        if let Some(existing) = daemon_pid {
            assert_eq!(pid, existing);
        } else {
            daemon_pid = Some(pid);
        }
    }

    assert_eq!(started_count, 1);
    cbth(&home, &["daemon", "stop"]);
    wait_for_socket_removed(&home);
}

#[test]
fn daemon_ensure_starts_ping_status_and_stop() {
    let home = tempfile::tempdir().expect("temp home");

    let ensured = cbth(
        &home,
        &[
            "daemon",
            "ensure",
            "--idle-timeout-seconds",
            "10",
            "--startup-timeout-seconds",
            "5",
        ],
    );
    assert_eq!(ensured["started"], true);
    assert!(ensured["daemon"]["pid"].as_u64().expect("pid") > 0);

    let ping = cbth(&home, &["daemon", "ping"]);
    assert_eq!(ping["message"], "pong");
    assert_eq!(ping["daemon"]["idle_timeout_seconds"], 10);

    let status = cbth(&home, &["daemon", "status"]);
    assert_eq!(status["daemon"]["stop_requested"], false);
    assert!(status["startup_sweep"].is_object());

    #[cfg(unix)]
    {
        let socket_path = home.path().join("run").join("cbth.sock");
        let mode = fs::symlink_metadata(&socket_path)
            .expect("socket metadata")
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(mode, 0o600);
    }

    let stopped = cbth(&home, &["daemon", "stop"]);
    assert_eq!(stopped["stopping"], true);
    wait_for_socket_removed(&home);
}

#[test]
fn daemon_ensure_timeout_does_not_publish_socket_when_startup_is_blocked() {
    let home = tempfile::tempdir().expect("temp home");
    cbth(
        &home,
        &[
            "job",
            "submit",
            "--source-thread-id",
            "thread-startup-lock",
            "--summary",
            "initialize db",
        ],
    );

    let db_path = home.path().join("cbth.sqlite3");
    let conn = Connection::open(&db_path).expect("open db");
    conn.execute_batch("PRAGMA locking_mode=EXCLUSIVE; BEGIN EXCLUSIVE;")
        .expect("hold exclusive db lock");

    let stderr = cbth_failure(
        &home,
        &[
            "daemon",
            "ensure",
            "--idle-timeout-seconds",
            "10",
            "--startup-timeout-seconds",
            "1",
        ],
    );
    assert!(stderr.contains("daemon did not become ready"));
    assert!(!home.path().join("run").join("cbth.sock").exists());

    drop(conn);
}

#[cfg(unix)]
#[test]
fn daemon_ensure_timeout_is_not_extended_by_unresponsive_socket() {
    let home = tempfile::tempdir().expect("temp home");
    let run_dir = home.path().join("run");
    fs::create_dir_all(&run_dir).expect("create run dir");
    fs::set_permissions(home.path(), fs::Permissions::from_mode(0o700)).expect("chmod home");
    fs::set_permissions(&run_dir, fs::Permissions::from_mode(0o700)).expect("chmod run dir");

    let socket_path = run_dir.join("cbth.sock");
    let listener = UnixListener::bind(&socket_path).expect("bind dummy socket");
    fs::set_permissions(&socket_path, fs::Permissions::from_mode(0o600)).expect("chmod socket");
    let handle = thread::spawn(move || {
        if let Ok((_stream, _addr)) = listener.accept() {
            thread::sleep(Duration::from_secs(3));
        }
    });

    let started = Instant::now();
    let stderr = cbth_failure(
        &home,
        &[
            "daemon",
            "ensure",
            "--idle-timeout-seconds",
            "10",
            "--startup-timeout-seconds",
            "1",
        ],
    );
    let elapsed = started.elapsed();
    assert!(stderr.contains("daemon did not become ready"));
    assert!(
        elapsed < Duration::from_secs(3),
        "ensure waited too long: {elapsed:?}"
    );

    handle.join().expect("dummy listener thread");
}

#[cfg(unix)]
#[test]
fn daemon_ensure_timeout_is_not_extended_by_slow_trickle_socket() {
    let home = tempfile::tempdir().expect("temp home");
    let run_dir = home.path().join("run");
    fs::create_dir_all(&run_dir).expect("create run dir");
    fs::set_permissions(home.path(), fs::Permissions::from_mode(0o700)).expect("chmod home");
    fs::set_permissions(&run_dir, fs::Permissions::from_mode(0o700)).expect("chmod run dir");

    let socket_path = run_dir.join("cbth.sock");
    let listener = UnixListener::bind(&socket_path).expect("bind dummy socket");
    fs::set_permissions(&socket_path, fs::Permissions::from_mode(0o600)).expect("chmod socket");
    let handle = thread::spawn(move || {
        if let Ok((mut stream, _addr)) = listener.accept() {
            for _ in 0..10 {
                if stream.write_all(b" ").is_err() {
                    break;
                }
                thread::sleep(Duration::from_millis(200));
            }
        }
    });

    let started = Instant::now();
    let stderr = cbth_failure(
        &home,
        &[
            "daemon",
            "ensure",
            "--idle-timeout-seconds",
            "10",
            "--startup-timeout-seconds",
            "1",
        ],
    );
    let elapsed = started.elapsed();
    assert!(stderr.contains("daemon did not become ready"));
    assert!(
        elapsed < Duration::from_secs(2),
        "ensure waited too long: {elapsed:?}"
    );

    handle.join().expect("dummy listener thread");
}

#[test]
fn daemon_exits_after_idle_timeout() {
    let home = tempfile::tempdir().expect("temp home");
    let mut child = spawn_daemon(&home, "1", &[]);

    let ping = wait_for_ping(&home);
    assert_eq!(ping["message"], "pong");

    let deadline = Instant::now() + Duration::from_secs(5);
    let status = loop {
        if let Some(status) = child.try_wait().expect("check daemon status") {
            break status;
        }
        assert!(
            Instant::now() < deadline,
            "daemon did not exit after idle timeout"
        );
        thread::sleep(Duration::from_millis(100));
    };
    assert!(status.success());

    let output = child.wait_with_output().expect("daemon output");
    let shutdown: Value = serde_json::from_slice(&output.stdout).expect("daemon shutdown json");
    assert_eq!(shutdown["shutdown_reason"], "idle_timeout");
    wait_for_socket_removed(&home);
}

#[test]
fn daemon_startup_sweep_closes_expired_batches() {
    let home = tempfile::tempdir().expect("temp home");
    let submitted = cbth(
        &home,
        &[
            "job",
            "submit",
            "--source-thread-id",
            "thread-daemon-sweep",
            "--summary",
            "wait for external reviewer",
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
    let failed = cbth(
        &home,
        &[
            "job",
            "fail",
            "--job-id",
            job_id,
            "--reason",
            "review rejected",
            "--redelivery-window-seconds",
            "1",
        ],
    );
    let batch = &failed["batch"]["batch"];
    let batch_id = batch["batch_id"].as_str().expect("batch id");
    let sweep_now = batch["redelivery_window_ends_at"]
        .as_i64()
        .expect("redelivery window")
        + 1;
    let sweep_now_arg = sweep_now.to_string();

    let mut child = spawn_daemon(&home, "10", &["--now", &sweep_now_arg]);
    wait_for_ping(&home);

    let status = cbth(&home, &["daemon", "status"]);
    assert_eq!(
        status["startup_sweep"]["expired_automatic_batches_closed"],
        1
    );

    let inspected = cbth(&home, &["batch", "inspect", "--batch-id", batch_id]);
    assert_eq!(inspected["batch"]["batch"]["state"], "closed");
    assert_eq!(
        inspected["batch"]["batch"]["close_reason"],
        "redelivery_window_exhausted"
    );

    cbth(&home, &["daemon", "stop"]);
    let exit_status = child.wait().expect("daemon exit");
    assert!(exit_status.success());
}

#[cfg(unix)]
#[test]
fn daemon_client_fails_closed_when_run_dir_is_too_permissive() {
    let home = tempfile::tempdir().expect("temp home");
    cbth(
        &home,
        &[
            "daemon",
            "ensure",
            "--idle-timeout-seconds",
            "3",
            "--startup-timeout-seconds",
            "5",
        ],
    );

    let run_dir = home.path().join("run");
    fs::set_permissions(&run_dir, fs::Permissions::from_mode(0o755)).expect("chmod run dir");
    let stderr = cbth_failure(&home, &["daemon", "ping"]);
    assert!(stderr.contains("cbth run directory permissions are wider than 0700"));

    fs::set_permissions(&run_dir, fs::Permissions::from_mode(0o700)).expect("restore run dir");
    cbth(&home, &["daemon", "stop"]);
    wait_for_socket_removed(&home);
}
