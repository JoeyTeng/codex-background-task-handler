use std::fs;
use std::io::{Read, Write};
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

fn temp_home() -> TempDir {
    let home = tempfile::tempdir().expect("temp home");
    #[cfg(unix)]
    fs::set_permissions(home.path(), fs::Permissions::from_mode(0o700)).expect("chmod temp home");
    home
}

fn cbth(home: &TempDir, args: &[&str]) -> Value {
    let output = Command::new(env!("CARGO_BIN_EXE_cbth"))
        .env("CBTH_ALLOW_DIRECT_STORE", "1")
        .arg("--direct-store")
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
        .env("CBTH_ALLOW_DIRECT_STORE", "1")
        .arg("--direct-store")
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
        .env("CBTH_ALLOW_DIRECT_STORE", "1")
        .arg("--direct-store")
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
    let home = temp_home();
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
    let home = temp_home();

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
    assert_eq!(ping["protocol_version"], 1);
    assert_eq!(ping["capabilities"][0], "dispatch");
    assert_eq!(ping["daemon"]["idle_timeout_seconds"], 10);

    let status = cbth(&home, &["daemon", "status"]);
    assert_eq!(status["daemon"]["stop_requested"], false);
    assert_eq!(status["protocol_version"], 1);
    assert_eq!(status["capabilities"][0], "dispatch");
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

#[cfg(unix)]
#[test]
fn daemon_ensure_restarts_incompatible_daemon() {
    let home = temp_home();
    let run_dir = home.path().join("run");
    fs::create_dir(&run_dir).expect("create run dir");
    fs::set_permissions(&run_dir, fs::Permissions::from_mode(0o700)).expect("chmod run dir");
    let socket_path = run_dir.join("cbth.sock");
    let listener = UnixListener::bind(&socket_path).expect("bind legacy daemon socket");
    fs::set_permissions(&socket_path, fs::Permissions::from_mode(0o600)).expect("chmod socket");
    let legacy_socket_path = socket_path.clone();
    let handle = thread::spawn(move || {
        for _ in 0..2 {
            let (mut stream, _addr) = listener.accept().expect("accept legacy request");
            let mut request = String::new();
            stream
                .read_to_string(&mut request)
                .expect("read legacy request");
            let response = if request.contains("\"stop\"") {
                r#"{"ok":true,"response":{"stopping":true}}"#
            } else {
                r#"{"ok":true,"response":{"daemon":{"pid":1},"message":"pong"}}"#
            };
            stream
                .write_all(response.as_bytes())
                .expect("write response");
            stream.write_all(b"\n").expect("write response newline");
            if request.contains("\"stop\"") {
                break;
            }
        }
        drop(listener);
        fs::remove_file(&legacy_socket_path).expect("remove legacy socket");
    });

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
    assert!(ensured["daemon"]["pid"].as_u64().expect("pid") > 1);
    handle.join().expect("legacy daemon thread");

    let ping = cbth(&home, &["daemon", "ping"]);
    assert_eq!(ping["protocol_version"], 1);
    assert_eq!(ping["capabilities"][0], "dispatch");

    cbth(&home, &["daemon", "stop"]);
    wait_for_socket_removed(&home);
}

#[cfg(unix)]
#[test]
fn daemon_ensure_accepts_concurrent_compatible_replacement() {
    let home = temp_home();
    let run_dir = home.path().join("run");
    fs::create_dir(&run_dir).expect("create run dir");
    fs::set_permissions(&run_dir, fs::Permissions::from_mode(0o700)).expect("chmod run dir");
    let socket_path = run_dir.join("cbth.sock");
    let legacy_listener = UnixListener::bind(&socket_path).expect("bind legacy daemon socket");
    fs::set_permissions(&socket_path, fs::Permissions::from_mode(0o600)).expect("chmod socket");
    let replacement_socket_path = socket_path.clone();
    let handle = thread::spawn(move || {
        for _ in 0..2 {
            let (mut stream, _addr) = legacy_listener.accept().expect("accept legacy request");
            let mut request = String::new();
            stream
                .read_to_string(&mut request)
                .expect("read legacy request");
            let response = if request.contains("\"stop\"") {
                r#"{"ok":true,"response":{"stopping":true}}"#
            } else {
                r#"{"ok":true,"response":{"daemon":{"pid":1},"message":"pong"}}"#
            };
            stream
                .write_all(response.as_bytes())
                .expect("write response");
            stream.write_all(b"\n").expect("write response newline");
            if request.contains("\"stop\"") {
                break;
            }
        }
        drop(legacy_listener);
        fs::remove_file(&replacement_socket_path).expect("remove legacy socket");

        let replacement_listener =
            UnixListener::bind(&replacement_socket_path).expect("bind replacement socket");
        fs::set_permissions(&replacement_socket_path, fs::Permissions::from_mode(0o600))
            .expect("chmod replacement socket");
        for _ in 0..2 {
            let (mut stream, _addr) = replacement_listener
                .accept()
                .expect("accept replacement request");
            let mut request = String::new();
            stream
                .read_to_string(&mut request)
                .expect("read replacement request");
            assert!(request.contains("\"ping\""));
            stream
                .write_all(
                    br#"{"ok":true,"response":{"daemon":{"pid":5151},"protocol_version":1,"capabilities":["dispatch"],"message":"pong"}}"#,
                )
                .expect("write replacement response");
            stream.write_all(b"\n").expect("write response newline");
        }
        drop(replacement_listener);
        fs::remove_file(&replacement_socket_path).expect("remove replacement socket");
    });

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
    assert_eq!(ensured["started"], false);
    assert_eq!(ensured["daemon"]["pid"], 5151);
    handle.join().expect("replacement daemon thread");
}

#[cfg(unix)]
#[test]
fn daemon_ensure_retries_busy_daemon_without_spawning() {
    let home = temp_home();
    let run_dir = home.path().join("run");
    fs::create_dir(&run_dir).expect("create run dir");
    fs::set_permissions(&run_dir, fs::Permissions::from_mode(0o700)).expect("chmod run dir");
    let socket_path = run_dir.join("cbth.sock");
    let listener = UnixListener::bind(&socket_path).expect("bind busy daemon socket");
    fs::set_permissions(&socket_path, fs::Permissions::from_mode(0o600)).expect("chmod socket");
    let busy_socket_path = socket_path.clone();
    let handle = thread::spawn(move || {
        for index in 0..2 {
            let (mut stream, _addr) = listener.accept().expect("accept busy daemon request");
            let mut request = String::new();
            stream
                .read_to_string(&mut request)
                .expect("read busy daemon request");
            assert!(request.contains("\"ping\""));
            let response = if index == 0 {
                r#"{"ok":false,"error":"daemon is busy"}"#
            } else {
                r#"{"ok":true,"response":{"daemon":{"pid":4242},"protocol_version":1,"capabilities":["dispatch"],"message":"pong"}}"#
            };
            stream
                .write_all(response.as_bytes())
                .expect("write response");
            stream.write_all(b"\n").expect("write response newline");
        }
        drop(listener);
        fs::remove_file(&busy_socket_path).expect("remove busy socket");
    });

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
    assert_eq!(ensured["started"], false);
    assert_eq!(ensured["daemon"]["pid"], 4242);
    handle.join().expect("busy daemon thread");
}

#[test]
fn daemon_ensure_timeout_does_not_publish_socket_when_startup_is_blocked() {
    let home = temp_home();
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
    let home = temp_home();
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
    let home = temp_home();
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

#[cfg(unix)]
#[test]
fn daemon_serve_refuses_to_replace_active_socket() {
    let home = temp_home();
    let run_dir = home.path().join("run");
    fs::create_dir_all(&run_dir).expect("create run dir");
    fs::set_permissions(home.path(), fs::Permissions::from_mode(0o700)).expect("chmod home");
    fs::set_permissions(&run_dir, fs::Permissions::from_mode(0o700)).expect("chmod run dir");

    let socket_path = run_dir.join("cbth.sock");
    let _listener = UnixListener::bind(&socket_path).expect("bind dummy socket");
    fs::set_permissions(&socket_path, fs::Permissions::from_mode(0o600)).expect("chmod socket");

    let stderr = cbth_failure(&home, &["daemon", "serve", "--idle-timeout-seconds", "1"]);
    assert!(stderr.contains("daemon socket is already active"));
    assert!(socket_path.exists());
}

#[cfg(unix)]
#[test]
fn daemon_serve_replaces_connection_refused_stale_socket() {
    let home = temp_home();
    let run_dir = home.path().join("run");
    fs::create_dir_all(&run_dir).expect("create run dir");
    fs::set_permissions(home.path(), fs::Permissions::from_mode(0o700)).expect("chmod home");
    fs::set_permissions(&run_dir, fs::Permissions::from_mode(0o700)).expect("chmod run dir");

    let socket_path = run_dir.join("cbth.sock");
    {
        let _listener = UnixListener::bind(&socket_path).expect("bind stale socket");
        fs::set_permissions(&socket_path, fs::Permissions::from_mode(0o600)).expect("chmod socket");
    }
    assert!(socket_path.exists());

    let shutdown = cbth(&home, &["daemon", "serve", "--idle-timeout-seconds", "1"]);
    assert_eq!(shutdown["shutdown_reason"], "idle_timeout");
    assert!(!socket_path.exists());
}

#[test]
fn daemon_exits_after_idle_timeout() {
    let home = temp_home();
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
    let home = temp_home();
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
    let home = temp_home();
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

#[cfg(unix)]
#[test]
fn daemon_ensure_fails_closed_before_autostart_with_permissive_run_dir() {
    let home = temp_home();
    let run_dir = home.path().join("run");
    fs::create_dir(&run_dir).expect("create run dir");
    fs::set_permissions(&run_dir, fs::Permissions::from_mode(0o755)).expect("chmod run dir");

    let stderr = cbth_failure(
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
    assert!(stderr.contains("cbth run directory permissions are wider than 0700"));
    assert!(!run_dir.join("cbth.sock").exists());
}
