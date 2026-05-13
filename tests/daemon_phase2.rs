use std::fs;
use std::io::{self, Read, Write};
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use rusqlite::{Connection, params};
use semver::Version;
use serde_json::{Value, json};
use tempfile::TempDir;

#[cfg(unix)]
use std::os::unix::ffi::OsStringExt;
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
#[cfg(unix)]
use std::os::unix::net::{UnixListener, UnixStream};

fn temp_home() -> TempDir {
    let home = tempfile::tempdir().expect("temp home");
    #[cfg(unix)]
    fs::set_permissions(home.path(), fs::Permissions::from_mode(0o700)).expect("chmod temp home");
    home
}

fn handoff_eligible_for_version(version: &str) -> bool {
    Version::parse(version).expect("test package version is semver")
        >= Version::parse("0.2.0").expect("handoff minimum version is semver")
}

#[cfg(unix)]
fn is_peer_disconnect(error: &std::io::Error) -> bool {
    is_peer_disconnect_kind(error.kind())
}

#[cfg(unix)]
fn is_peer_disconnect_kind(kind: std::io::ErrorKind) -> bool {
    matches!(
        kind,
        std::io::ErrorKind::BrokenPipe
            | std::io::ErrorKind::ConnectionReset
            | std::io::ErrorKind::NotConnected
    )
}

const TEST_DAEMON_CAPABILITIES_JSON: &str = r#"["dispatch","attempt-dispatch","cli-app-server-lifecycle","cli-app-server-probe","cli-thread-start-bootstrap","cli-thread-start-params","cli-foreground-thread-bootstrap","cli-session-dispatch","cli-session-capability-dispatch","cli-session-permission-dispatch","cli-session-proof-invalidation-dispatch","cli-session-recovery-dispatch","cli-turn-observation-dispatch","cli-turn-observation-expiry-dispatch","cli-auto-delivery-dispatch","task-supervisor","desktop-bridge-foundation-dispatch","desktop-inbox-revisioned-installation-state","desktop-writeback-helper-foundation","desktop-writeback-live-validation-fixture","desktop-transcript-relay-consumer","desktop-transcript-relay-scanner","desktop-ready-arm-workflow","daemon-handoff-v1"]"#;

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

fn cbth_daemon(home: &TempDir, args: &[&str]) -> Value {
    let output = Command::new(env!("CARGO_BIN_EXE_cbth"))
        .arg("--home")
        .arg(home.path())
        .args(args)
        .output()
        .expect("run cbth through daemon");

    assert!(
        output.status.success(),
        "cbth daemon command failed\nstatus: {}\nstdout: {}\nstderr: {}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    serde_json::from_slice(&output.stdout).expect("valid json output")
}

fn cbth_daemon_failure(home: &TempDir, args: &[&str]) -> String {
    let output = Command::new(env!("CARGO_BIN_EXE_cbth"))
        .arg("--home")
        .arg(home.path())
        .args(args)
        .output()
        .expect("run cbth through daemon");

    assert!(
        !output.status.success(),
        "cbth daemon command unexpectedly succeeded\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8_lossy(&output.stderr).into_owned()
}

fn write_function_call_rollout(path: &Path, output: &str) {
    let record = json!({
        "type": "response_item",
        "payload": {
            "type": "function_call_output",
            "output": output,
        }
    });
    fs::write(path, serde_json::to_string(&record).unwrap()).expect("write rollout");
}

fn hold_exclusive_db_lock(home: &TempDir) -> Connection {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let conn = Connection::open(home.path().join("cbth.sqlite3")).expect("open db");
        match conn.execute_batch("PRAGMA locking_mode=EXCLUSIVE; BEGIN EXCLUSIVE;") {
            Ok(()) => return conn,
            Err(error) if Instant::now() < deadline => {
                drop(conn);
                thread::sleep(Duration::from_millis(20));
                let _ = error;
            }
            Err(error) => panic!("hold exclusive db lock: {error}"),
        }
    }
}

fn wait_for_task_status(home: &TempDir, task_id: &str, status: &str) -> Value {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let task = cbth(home, &["task", "inspect", "--task-id", task_id]);
        if task["task"]["status"] == status {
            return task;
        }
        assert!(
            Instant::now() < deadline,
            "task {task_id} did not reach {status}: {task}"
        );
        thread::sleep(Duration::from_millis(100));
    }
}

fn bind_cli_session(home: &TempDir, bound_thread_id: &str) -> String {
    let session = cbth(
        home,
        &[
            "cli",
            "session",
            "bind",
            "--bound-thread-id",
            bound_thread_id,
            "--session-allows-approval",
            "false",
            "--session-allows-network",
            "false",
            "--session-allows-write-access",
            "false",
        ],
    );
    session["cli_session"]["session"]["managed_session_id"]
        .as_str()
        .expect("managed session id")
        .to_owned()
}

fn bind_idle_cli_session(home: &TempDir, bound_thread_id: &str) -> String {
    let managed_session_id = bind_cli_session(home, bound_thread_id);
    cbth(
        home,
        &[
            "cli",
            "session",
            "note-capabilities",
            "--managed-session-id",
            &managed_session_id,
            "--session-epoch",
            "1",
            "--capability-revision",
            "1",
            "--thread-resume",
            "true",
            "--turn-start",
            "true",
            "--current-state-sync",
            "true",
            "--turn-completed-event",
            "true",
            "--negative-terminal-events",
            "true",
        ],
    );
    cbth(
        home,
        &[
            "cli",
            "session",
            "note-activity",
            "--managed-session-id",
            &managed_session_id,
            "--session-epoch",
            "1",
            "--activity-state",
            "idle",
            "--activity-revision",
            "1",
        ],
    );
    managed_session_id
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

fn wait_for_path(path: &Path) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while !path.exists() {
        assert!(
            Instant::now() < deadline,
            "{} did not appear",
            path.display()
        );
        thread::sleep(Duration::from_millis(50));
    }
}

fn wait_for_nonempty_file(path: &Path) {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if fs::metadata(path).is_ok_and(|metadata| metadata.len() > 0) {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "{} did not become non-empty",
            path.display()
        );
        thread::sleep(Duration::from_millis(50));
    }
}

fn wait_for_socket_removed_with_timeout(home: &TempDir, timeout: Duration) {
    let socket_path = home.path().join("run").join("cbth.sock");
    wait_for_socket_path_removed(&socket_path, timeout);
}

fn wait_for_socket_path_removed(socket_path: &Path, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    while socket_path.exists() {
        assert!(Instant::now() < deadline, "daemon socket was not removed");
        thread::sleep(Duration::from_millis(50));
    }
}

#[cfg(unix)]
fn stop_daemon_at_socket_path(socket_path: &Path) {
    let mut stream = UnixStream::connect(socket_path).expect("connect daemon socket");
    stream
        .write_all(br#"{"command":"stop","payload":null}"#)
        .expect("write daemon stop");
    stream.write_all(b"\n").expect("write daemon stop newline");
    stream
        .shutdown(std::net::Shutdown::Write)
        .expect("shutdown daemon stop write");
    let mut response = String::new();
    stream
        .read_to_string(&mut response)
        .expect("read daemon stop response");
    assert!(
        response.contains(r#""ok":true"#),
        "daemon stop failed: {response}"
    );
}

#[cfg(unix)]
fn daemon_command_at_socket_path(socket_path: &Path, command: &str) -> Value {
    let mut stream = UnixStream::connect(socket_path).expect("connect daemon socket");
    let request = json!({
        "command": command,
        "payload": null,
    });
    stream
        .write_all(request.to_string().as_bytes())
        .expect("write daemon request");
    stream
        .write_all(b"\n")
        .expect("write daemon request newline");
    stream
        .shutdown(std::net::Shutdown::Write)
        .expect("shutdown daemon request write");
    let mut response = Vec::new();
    stream
        .read_to_end(&mut response)
        .expect("read daemon response");
    let response: Value = serde_json::from_slice(&response).expect("daemon response json");
    assert_eq!(response["ok"], true, "daemon request failed: {response}");
    response["response"].clone()
}

fn process_group_exists(pid: u32) -> bool {
    if unsafe { libc::killpg(pid as libc::pid_t, 0) } == 0 {
        return true;
    }
    !matches!(
        std::io::Error::last_os_error().raw_os_error(),
        Some(libc::ESRCH)
    )
}

fn process_group_id(pid: u32) -> libc::pid_t {
    let pgid = unsafe { libc::getpgid(pid as libc::pid_t) };
    assert!(pgid > 0, "getpgid({pid}) failed");
    pgid
}

fn wait_for_process_group_gone(pid: u32) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while process_group_exists(pid) {
        assert!(
            Instant::now() < deadline,
            "process group {pid} was not removed"
        );
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

#[cfg(unix)]
#[test]
fn daemon_ensure_starts_daemon_in_own_process_group() {
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
    let daemon_pid = ensured["daemon"]["pid"].as_u64().expect("daemon pid") as u32;
    assert_eq!(process_group_id(daemon_pid), daemon_pid as libc::pid_t);

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
            "--replace-incompatible",
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
    assert_eq!(ping["daemon"]["binary_version"], env!("CARGO_PKG_VERSION"));
    assert_eq!(ping["daemon"]["quiescing"], false);
    assert_eq!(ping["daemon"]["handoff_minimum_binary_version"], "0.2.0");
    assert_eq!(
        ping["daemon"]["handoff_eligible"],
        handoff_eligible_for_version(env!("CARGO_PKG_VERSION"))
    );
    assert_eq!(
        ping["capabilities"],
        json!([
            "dispatch",
            "attempt-dispatch",
            "cli-app-server-lifecycle",
            "cli-app-server-probe",
            "cli-thread-start-bootstrap",
            "cli-thread-start-params",
            "cli-foreground-thread-bootstrap",
            "cli-session-dispatch",
            "cli-session-capability-dispatch",
            "cli-session-permission-dispatch",
            "cli-session-proof-invalidation-dispatch",
            "cli-session-recovery-dispatch",
            "cli-turn-observation-dispatch",
            "cli-turn-observation-expiry-dispatch",
            "cli-auto-delivery-dispatch",
            "task-supervisor",
            "desktop-bridge-foundation-dispatch",
            "desktop-inbox-revisioned-installation-state",
            "desktop-writeback-helper-foundation",
            "desktop-writeback-live-validation-fixture",
            "desktop-transcript-relay-consumer",
            "desktop-transcript-relay-scanner",
            "desktop-ready-arm-workflow",
            "daemon-handoff-v1"
        ])
    );
    assert_eq!(ping["daemon"]["idle_timeout_seconds"], 10);

    let status = cbth(&home, &["daemon", "status"]);
    assert_eq!(status["daemon"]["stop_requested"], false);
    assert_eq!(
        status["daemon"]["binary_version"],
        env!("CARGO_PKG_VERSION")
    );
    assert_eq!(status["daemon"]["quiescing"], false);
    assert_eq!(status["daemon"]["handoff_minimum_binary_version"], "0.2.0");
    assert_eq!(
        status["daemon"]["handoff_eligible"],
        handoff_eligible_for_version(env!("CARGO_PKG_VERSION"))
    );
    assert_eq!(status["protocol_version"], 1);
    assert_eq!(
        status["capabilities"],
        json!([
            "dispatch",
            "attempt-dispatch",
            "cli-app-server-lifecycle",
            "cli-app-server-probe",
            "cli-thread-start-bootstrap",
            "cli-thread-start-params",
            "cli-foreground-thread-bootstrap",
            "cli-session-dispatch",
            "cli-session-capability-dispatch",
            "cli-session-permission-dispatch",
            "cli-session-proof-invalidation-dispatch",
            "cli-session-recovery-dispatch",
            "cli-turn-observation-dispatch",
            "cli-turn-observation-expiry-dispatch",
            "cli-auto-delivery-dispatch",
            "task-supervisor",
            "desktop-bridge-foundation-dispatch",
            "desktop-inbox-revisioned-installation-state",
            "desktop-writeback-helper-foundation",
            "desktop-writeback-live-validation-fixture",
            "desktop-transcript-relay-consumer",
            "desktop-transcript-relay-scanner",
            "desktop-ready-arm-workflow",
            "daemon-handoff-v1"
        ])
    );
    assert!(status["startup_sweep"].is_object());
    assert_eq!(status["cli_app_servers"], json!([]));

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
            let is_stop = request.contains("\"stop\"");
            let response = if is_stop {
                r#"{"ok":true,"response":{"stopping":true}}"#
            } else {
                r#"{"ok":true,"response":{"daemon":{"pid":1},"message":"pong"}}"#
            };
            if let Err(error) = stream.write_all(response.as_bytes()) {
                if !is_stop && is_peer_disconnect(&error) {
                    continue;
                }
                panic!("write legacy response: {error}");
            }
            if let Err(error) = stream.write_all(b"\n") {
                if !is_stop && is_peer_disconnect(&error) {
                    continue;
                }
                panic!("write legacy response newline: {error}");
            }
            if is_stop {
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
            "--replace-incompatible",
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
    assert_eq!(
        ping["capabilities"],
        json!([
            "dispatch",
            "attempt-dispatch",
            "cli-app-server-lifecycle",
            "cli-app-server-probe",
            "cli-thread-start-bootstrap",
            "cli-thread-start-params",
            "cli-foreground-thread-bootstrap",
            "cli-session-dispatch",
            "cli-session-capability-dispatch",
            "cli-session-permission-dispatch",
            "cli-session-proof-invalidation-dispatch",
            "cli-session-recovery-dispatch",
            "cli-turn-observation-dispatch",
            "cli-turn-observation-expiry-dispatch",
            "cli-auto-delivery-dispatch",
            "task-supervisor",
            "desktop-bridge-foundation-dispatch",
            "desktop-inbox-revisioned-installation-state",
            "desktop-writeback-helper-foundation",
            "desktop-writeback-live-validation-fixture",
            "desktop-transcript-relay-consumer",
            "desktop-transcript-relay-scanner",
            "desktop-ready-arm-workflow",
            "daemon-handoff-v1"
        ])
    );

    cbth(&home, &["daemon", "stop"]);
    wait_for_socket_removed(&home);
}

#[cfg(unix)]
#[test]
fn daemon_ensure_coexists_with_incompatible_default_without_stop() {
    let home = temp_home();
    let run_dir = home.path().join("run");
    fs::create_dir(&run_dir).expect("create run dir");
    fs::set_permissions(&run_dir, fs::Permissions::from_mode(0o700)).expect("chmod run dir");
    let socket_path = run_dir.join("cbth.sock");
    let generation_socket_path = run_dir
        .join("daemons")
        .join(env!("CARGO_PKG_VERSION"))
        .join("cbth.sock");
    let listener = UnixListener::bind(&socket_path).expect("bind old daemon socket");
    fs::set_permissions(&socket_path, fs::Permissions::from_mode(0o600)).expect("chmod socket");
    listener
        .set_nonblocking(true)
        .expect("set old listener nonblocking");
    let old_socket_path = socket_path.clone();
    let (done_tx, done_rx) = mpsc::channel();
    let (request_tx, request_rx) = mpsc::channel();
    let handle = thread::spawn(move || {
        loop {
            match listener.accept() {
                Ok((mut stream, _addr)) => {
                    let mut request = String::new();
                    stream
                        .read_to_string(&mut request)
                        .expect("read old request");
                    assert!(
                        !request.contains("\"stop\""),
                        "default ensure must not stop incompatible daemon: {request}"
                    );
                    assert!(
                        !request.contains("\"handoff_quiesce\""),
                        "below-minimum daemon must not be quiesced for handoff: {request}"
                    );
                    request_tx.send(request).expect("send observed request");
                    stream
                        .write_all(
                            br#"{"ok":true,"response":{"daemon":{"pid":1313,"binary_version":"0.1.5"},"protocol_version":1,"capabilities":["dispatch","daemon-handoff-v1"],"message":"pong"}}"#,
                        )
                        .expect("write old response");
                    stream.write_all(b"\n").expect("write old response newline");
                }
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    if done_rx.try_recv().is_ok() {
                        break;
                    }
                    thread::sleep(Duration::from_millis(20));
                }
                Err(error) => panic!("accept old daemon request: {error}"),
            }
        }
        drop(listener);
        fs::remove_file(&old_socket_path).expect("remove old socket");
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
    let request = request_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("legacy daemon should be probed");
    assert!(request.contains("\"ping\""));
    assert_eq!(ensured["started"], true);
    assert_eq!(ensured["coexisting_with_incompatible_daemon"], true);
    assert_eq!(ensured["legacy_daemon"]["pid"], 1313);
    assert_eq!(
        ensured["daemon"]["socket_path"],
        generation_socket_path.display().to_string()
    );
    assert!(
        socket_path.exists(),
        "legacy socket should remain owned by old daemon"
    );
    let all_status = cbth(&home, &["daemon", "status", "--all"]);
    let all_status_daemons = all_status["daemons"].as_array().expect("status daemons");
    assert!(
        all_status_daemons.iter().any(|daemon| {
            daemon["socket_path"] == socket_path.display().to_string() && daemon["ok"] == true
        }),
        "legacy daemon endpoint should be visible in status --all: {all_status}"
    );
    assert!(
        all_status_daemons.iter().any(|daemon| {
            daemon["socket_path"] == generation_socket_path.display().to_string()
                && daemon["ok"] == true
        }),
        "generation daemon endpoint should be visible in status --all: {all_status}"
    );
    let app_servers = cbth(&home, &["cli", "app-servers", "--all-daemons"]);
    let app_server_daemons = app_servers["daemons"]
        .as_array()
        .expect("app-server daemon reports");
    assert!(
        app_server_daemons.len() >= 2,
        "app-servers --all-daemons should include both endpoints: {app_servers}"
    );
    assert_eq!(app_servers["cli_app_servers"], json!([]));

    done_tx.send(()).expect("signal old daemon");
    handle.join().expect("old daemon thread");
    stop_daemon_at_socket_path(&generation_socket_path);
    wait_for_socket_path_removed(&generation_socket_path, Duration::from_secs(10));
}

#[cfg(unix)]
#[test]
fn daemon_ensure_quiesces_handoff_eligible_incompatible_default() {
    let home = temp_home();
    let run_dir = home.path().join("run");
    fs::create_dir(&run_dir).expect("create run dir");
    fs::set_permissions(&run_dir, fs::Permissions::from_mode(0o700)).expect("chmod run dir");
    let socket_path = run_dir.join("cbth.sock");
    let generation_socket_path = run_dir
        .join("daemons")
        .join(env!("CARGO_PKG_VERSION"))
        .join("cbth.sock");
    let listener = UnixListener::bind(&socket_path).expect("bind handoff daemon socket");
    fs::set_permissions(&socket_path, fs::Permissions::from_mode(0o600)).expect("chmod socket");
    listener
        .set_nonblocking(true)
        .expect("set handoff listener nonblocking");
    let old_socket_path = socket_path.clone();
    let (done_tx, done_rx) = mpsc::channel();
    let (request_tx, request_rx) = mpsc::channel();
    let handle = thread::spawn(move || {
        loop {
            match listener.accept() {
                Ok((mut stream, _addr)) => {
                    let mut request = String::new();
                    stream
                        .read_to_string(&mut request)
                        .expect("read handoff request");
                    request_tx
                        .send(request.clone())
                        .expect("send handoff request");
                    if request.contains("\"handoff_quiesce\"") {
                        stream
                            .write_all(
                                br#"{"ok":true,"response":{"daemon":{"pid":1313,"binary_version":"0.2.0","quiescing":true},"quiescing":true}}"#,
                            )
                            .expect("write quiesce response");
                    } else {
                        assert!(
                            request.contains("\"ping\""),
                            "unexpected request: {request}"
                        );
                        stream
                            .write_all(
                                br#"{"ok":true,"response":{"daemon":{"pid":1313,"binary_version":"0.2.0"},"protocol_version":1,"capabilities":["dispatch","daemon-handoff-v1"],"message":"pong"}}"#,
                            )
                            .expect("write handoff ping response");
                    }
                    stream
                        .write_all(b"\n")
                        .expect("write handoff response newline");
                }
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    if done_rx.try_recv().is_ok() {
                        break;
                    }
                    thread::sleep(Duration::from_millis(20));
                }
                Err(error) => panic!("accept handoff daemon request: {error}"),
            }
        }
        drop(listener);
        fs::remove_file(&old_socket_path).expect("remove handoff daemon socket");
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
    let first_request = request_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("handoff daemon should be probed");
    let second_request = request_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("handoff daemon should be quiesced");
    assert!(first_request.contains("\"ping\""));
    assert!(second_request.contains("\"handoff_quiesce\""));
    assert!(second_request.contains("\"expected_pid\":1313"));
    assert!(second_request.contains("\"expected_binary_version\":\"0.2.0\""));
    assert_eq!(ensured["started"], true);
    assert_eq!(ensured["coexisting_with_incompatible_daemon"], true);
    assert_eq!(ensured["legacy_daemon"]["pid"], 1313);
    assert_eq!(ensured["legacy_daemon_quiesced"], true);
    assert_eq!(ensured["legacy_handoff_gate"], "eligible");
    assert_eq!(ensured["legacy_handoff_quiesce"]["quiescing"], true);
    assert_eq!(
        ensured["daemon"]["socket_path"],
        generation_socket_path.display().to_string()
    );

    done_tx.send(()).expect("signal handoff daemon");
    handle.join().expect("handoff daemon thread");
    stop_daemon_at_socket_path(&generation_socket_path);
    wait_for_socket_path_removed(&generation_socket_path, Duration::from_secs(10));
}

#[cfg(unix)]
#[test]
fn task_run_uses_generation_daemon_when_incompatible_default_daemon_is_quiescing() {
    let home = temp_home();
    let run_dir = home.path().join("run");
    fs::create_dir_all(&run_dir).expect("create run dir");
    fs::set_permissions(home.path(), fs::Permissions::from_mode(0o700)).expect("chmod home");
    fs::set_permissions(&run_dir, fs::Permissions::from_mode(0o700)).expect("chmod run dir");
    let socket_path = run_dir.join("cbth.sock");
    let generation_socket_path = run_dir
        .join("daemons")
        .join(env!("CARGO_PKG_VERSION"))
        .join("cbth.sock");
    let listener = UnixListener::bind(&socket_path).expect("bind quiescing daemon socket");
    fs::set_permissions(&socket_path, fs::Permissions::from_mode(0o600)).expect("chmod socket");
    listener
        .set_nonblocking(true)
        .expect("set quiescing listener nonblocking");
    let mut capabilities: Vec<Value> =
        serde_json::from_str(TEST_DAEMON_CAPABILITIES_JSON).expect("capabilities json");
    capabilities
        .retain(|capability| capability.as_str() != Some("desktop-transcript-relay-consumer"));
    let (requests_tx, requests_rx) = mpsc::channel::<String>();
    let handle = thread::spawn(move || {
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            match listener.accept() {
                Ok((mut stream, _addr)) => {
                    stream
                        .set_nonblocking(false)
                        .expect("set quiescing stream blocking");
                    let mut request = [0_u8; 4096];
                    let request_len = stream.read(&mut request).expect("read request");
                    let request = String::from_utf8_lossy(&request[..request_len]).into_owned();
                    requests_tx.send(request.clone()).expect("send request");
                    assert!(
                        !request.contains("\"task_run\""),
                        "new task must not be routed to quiescing default daemon: {request}"
                    );
                    assert!(
                        !request.contains("\"handoff_quiesce\""),
                        "already-quiescing default may drain-exit before a second quiesce: {request}"
                    );
                    assert!(request.contains("\"ping\""), "{request}");
                    let response = json!({
                        "ok": true,
                        "response": {
                            "daemon": {
                                "pid": 1313,
                                "binary_version": "0.2.0",
                                "quiescing": true,
                            },
                            "protocol_version": 1,
                            "capabilities": capabilities.clone(),
                            "message": "pong",
                        }
                    });
                    stream
                        .write_all(response.to_string().as_bytes())
                        .expect("write response");
                    stream.write_all(b"\n").expect("write response newline");
                    break;
                }
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    assert!(
                        Instant::now() < deadline,
                        "quiescing default daemon listener timed out"
                    );
                    thread::sleep(Duration::from_millis(20));
                }
                Err(error) => panic!("accept quiescing daemon request: {error}"),
            }
        }
    });

    let started = cbth_daemon(
        &home,
        &[
            "task",
            "run",
            "--source-thread-id",
            "thread-generation-task-route",
            "--summary",
            "generation task route",
            "--",
            "/bin/sh",
            "-c",
            "printf generation",
        ],
    );
    assert_eq!(started["task"]["status"], "queued");
    let task_id = started["task"]["task_id"].as_str().expect("task id");
    let job_id = started["task"]["job_id"].as_str().expect("job id");
    let task = wait_for_task_status(&home, task_id, "succeeded");
    let stdout_log_path = task["task"]["stdout_log_path"]
        .as_str()
        .expect("stdout log path");
    let stdout = fs::read_to_string(home.path().join(stdout_log_path)).expect("stdout log");
    assert_eq!(stdout, "generation");
    let conn = Connection::open(home.path().join("cbth.sqlite3")).expect("open db");
    let supervisor_generation: Option<String> = conn
        .query_row(
            "SELECT supervisor_daemon_generation FROM jobs WHERE job_id = ?",
            params![job_id],
            |row| row.get(0),
        )
        .expect("query job supervisor generation");
    assert_eq!(
        supervisor_generation.as_deref(),
        Some(env!("CARGO_PKG_VERSION"))
    );

    handle.join().expect("quiescing daemon thread");
    let requests = requests_rx.try_iter().collect::<Vec<_>>();
    assert!(
        requests.iter().any(|request| request.contains("\"ping\"")),
        "default daemon should be probed before routing task"
    );
    assert!(
        requests
            .iter()
            .all(|request| !request.contains("\"task_run\"")),
        "quiescing default daemon received task_run: {requests:?}"
    );
    let _ = fs::remove_file(&socket_path);
    stop_daemon_at_socket_path(&generation_socket_path);
    wait_for_socket_path_removed(&generation_socket_path, Duration::from_secs(10));
}

#[cfg(unix)]
#[test]
fn daemon_ensure_unquiesces_legacy_when_app_server_adopt_fails() {
    let home = temp_home();
    let run_dir = home.path().join("run");
    fs::create_dir(&run_dir).expect("create run dir");
    fs::set_permissions(&run_dir, fs::Permissions::from_mode(0o700)).expect("chmod run dir");
    let socket_path = run_dir.join("cbth.sock");
    let generation_socket_path = run_dir
        .join("daemons")
        .join(env!("CARGO_PKG_VERSION"))
        .join("cbth.sock");
    let listener = UnixListener::bind(&socket_path).expect("bind handoff daemon socket");
    fs::set_permissions(&socket_path, fs::Permissions::from_mode(0o600)).expect("chmod socket");
    listener
        .set_nonblocking(true)
        .expect("set handoff listener nonblocking");
    let old_socket_path = socket_path.clone();
    let (done_tx, done_rx) = mpsc::channel();
    let (request_tx, request_rx) = mpsc::channel();
    let handle = thread::spawn(move || {
        loop {
            match listener.accept() {
                Ok((mut stream, _addr)) => {
                    let mut request = String::new();
                    stream
                        .read_to_string(&mut request)
                        .expect("read handoff request");
                    request_tx
                        .send(request.clone())
                        .expect("send handoff request");
                    if request.contains("\"handoff_quiesce\"") {
                        stream
                            .write_all(
                                br#"{"ok":true,"response":{"daemon":{"pid":1313,"binary_version":"0.2.0","quiescing":true},"quiescing":true,"cli_app_servers":[{"managed_session_id":"managed-stale-export","bound_thread_id":"thread-stale-export","session_epoch":1,"url":"ws://127.0.0.1:1","pid":1,"pid_identity":"stale-export","started_at":1,"lease_id":"lease-stale-export","lease_millis_remaining":60000}]}}"#,
                            )
                            .expect("write quiesce response");
                    } else if request.contains("\"handoff_unquiesce\"") {
                        stream
                            .write_all(
                                br#"{"ok":true,"response":{"daemon":{"pid":1313,"binary_version":"0.2.0","quiescing":false},"quiescing":false}}"#,
                            )
                            .expect("write unquiesce response");
                    } else {
                        assert!(
                            request.contains("\"ping\""),
                            "unexpected request: {request}"
                        );
                        stream
                            .write_all(
                                br#"{"ok":true,"response":{"daemon":{"pid":1313,"binary_version":"0.2.0"},"protocol_version":1,"capabilities":["dispatch","daemon-handoff-v1"],"message":"pong"}}"#,
                            )
                            .expect("write handoff ping response");
                    }
                    stream
                        .write_all(b"\n")
                        .expect("write handoff response newline");
                }
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    if done_rx.try_recv().is_ok() {
                        break;
                    }
                    thread::sleep(Duration::from_millis(20));
                }
                Err(error) => panic!("accept handoff daemon request: {error}"),
            }
        }
        drop(listener);
        fs::remove_file(&old_socket_path).expect("remove handoff daemon socket");
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
    let first_request = request_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("handoff daemon should be probed");
    let second_request = request_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("handoff daemon should be quiesced");
    let third_request = request_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("handoff daemon should be unquiesced after stale export");
    assert!(first_request.contains("\"ping\""));
    assert!(second_request.contains("\"handoff_quiesce\""));
    assert!(third_request.contains("\"handoff_unquiesce\""));
    assert!(third_request.contains("\"expected_pid\":1313"));
    assert_eq!(ensured["started"], true);
    assert_eq!(ensured["coexisting_with_incompatible_daemon"], true);
    assert_eq!(ensured["legacy_daemon_quiesced"], false);
    assert_eq!(ensured["legacy_handoff_unquiesce"]["quiescing"], false);
    assert_eq!(
        ensured["legacy_cli_app_server_handoff_skipped"]["reason"],
        "adopt_failed"
    );
    assert_eq!(
        ensured["daemon"]["socket_path"],
        generation_socket_path.display().to_string()
    );

    done_tx.send(()).expect("signal handoff daemon");
    handle.join().expect("handoff daemon thread");
    stop_daemon_at_socket_path(&generation_socket_path);
    wait_for_socket_path_removed(&generation_socket_path, Duration::from_secs(10));
}

#[cfg(unix)]
#[test]
fn daemon_ensure_coexists_when_handoff_quiesce_reports_active_bootstrap() {
    let home = temp_home();
    let run_dir = home.path().join("run");
    fs::create_dir(&run_dir).expect("create run dir");
    fs::set_permissions(&run_dir, fs::Permissions::from_mode(0o700)).expect("chmod run dir");
    let socket_path = run_dir.join("cbth.sock");
    let generation_socket_path = run_dir
        .join("daemons")
        .join(env!("CARGO_PKG_VERSION"))
        .join("cbth.sock");
    let listener = UnixListener::bind(&socket_path).expect("bind handoff daemon socket");
    fs::set_permissions(&socket_path, fs::Permissions::from_mode(0o600)).expect("chmod socket");
    listener
        .set_nonblocking(true)
        .expect("set handoff listener nonblocking");
    let old_socket_path = socket_path.clone();
    let (done_tx, done_rx) = mpsc::channel();
    let (request_tx, request_rx) = mpsc::channel();
    let handle = thread::spawn(move || {
        loop {
            match listener.accept() {
                Ok((mut stream, _addr)) => {
                    let mut request = String::new();
                    stream
                        .read_to_string(&mut request)
                        .expect("read handoff request");
                    request_tx
                        .send(request.clone())
                        .expect("send handoff request");
                    if request.contains("\"handoff_quiesce\"") {
                        stream
                            .write_all(
                                br#"{"ok":false,"error":"cannot handoff while CLI thread/start bootstrap app-servers are active"}"#,
                            )
                            .expect("write active bootstrap response");
                    } else {
                        assert!(
                            request.contains("\"ping\""),
                            "unexpected request: {request}"
                        );
                        stream
                            .write_all(
                                br#"{"ok":true,"response":{"daemon":{"pid":1314,"binary_version":"0.2.0"},"protocol_version":1,"capabilities":["dispatch","daemon-handoff-v1"],"message":"pong"}}"#,
                            )
                            .expect("write handoff ping response");
                    }
                    stream
                        .write_all(b"\n")
                        .expect("write handoff response newline");
                }
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    if done_rx.try_recv().is_ok() {
                        break;
                    }
                    thread::sleep(Duration::from_millis(20));
                }
                Err(error) => panic!("accept handoff daemon request: {error}"),
            }
        }
        drop(listener);
        fs::remove_file(&old_socket_path).expect("remove handoff daemon socket");
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
    let first_request = request_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("handoff daemon should be probed");
    let second_request = request_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("handoff daemon should receive quiesce attempt");
    assert!(first_request.contains("\"ping\""));
    assert!(second_request.contains("\"handoff_quiesce\""));
    assert_eq!(ensured["started"], true);
    assert_eq!(ensured["coexisting_with_incompatible_daemon"], true);
    assert_eq!(ensured["legacy_daemon"]["pid"], 1314);
    assert!(ensured.get("legacy_daemon_quiesced").is_none());
    assert!(ensured.get("legacy_handoff_quiesce").is_none());
    assert_eq!(
        ensured["daemon"]["socket_path"],
        generation_socket_path.display().to_string()
    );

    done_tx.send(()).expect("signal handoff daemon");
    handle.join().expect("handoff daemon thread");
    stop_daemon_at_socket_path(&generation_socket_path);
    wait_for_socket_path_removed(&generation_socket_path, Duration::from_secs(10));
}

#[cfg(unix)]
#[test]
fn daemon_ensure_replaces_stale_incompatible_generation_daemon() {
    let home = temp_home();
    let run_dir = home.path().join("run");
    fs::create_dir(&run_dir).expect("create run dir");
    fs::set_permissions(&run_dir, fs::Permissions::from_mode(0o700)).expect("chmod run dir");
    let socket_path = run_dir.join("cbth.sock");
    let generation_dir = run_dir.join("daemons").join(env!("CARGO_PKG_VERSION"));
    fs::create_dir_all(&generation_dir).expect("create generation dir");
    fs::set_permissions(run_dir.join("daemons"), fs::Permissions::from_mode(0o700))
        .expect("chmod daemons dir");
    fs::set_permissions(&generation_dir, fs::Permissions::from_mode(0o700))
        .expect("chmod generation dir");
    let generation_socket_path = generation_dir.join("cbth.sock");

    let legacy_listener = UnixListener::bind(&socket_path).expect("bind old daemon socket");
    fs::set_permissions(&socket_path, fs::Permissions::from_mode(0o600)).expect("chmod socket");
    legacy_listener
        .set_nonblocking(true)
        .expect("set old listener nonblocking");
    let legacy_socket_path = socket_path.clone();
    let (done_tx, done_rx) = mpsc::channel();
    let legacy_handle = thread::spawn(move || {
        loop {
            match legacy_listener.accept() {
                Ok((mut stream, _addr)) => {
                    let mut request = String::new();
                    stream
                        .read_to_string(&mut request)
                        .expect("read old request");
                    assert!(
                        !request.contains("\"stop\""),
                        "generation replacement must not stop legacy default daemon: {request}"
                    );
                    stream
                        .write_all(
                            br#"{"ok":true,"response":{"daemon":{"pid":1313,"binary_version":"0.1.5"},"protocol_version":1,"capabilities":["dispatch"],"message":"pong"}}"#,
                        )
                        .expect("write old response");
                    stream.write_all(b"\n").expect("write old response newline");
                }
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    if done_rx.try_recv().is_ok() {
                        break;
                    }
                    thread::sleep(Duration::from_millis(20));
                }
                Err(error) => panic!("accept old daemon request: {error}"),
            }
        }
        drop(legacy_listener);
        fs::remove_file(&legacy_socket_path).expect("remove old socket");
    });

    let generation_listener =
        UnixListener::bind(&generation_socket_path).expect("bind stale generation daemon socket");
    fs::set_permissions(&generation_socket_path, fs::Permissions::from_mode(0o600))
        .expect("chmod generation socket");
    let stale_generation_socket_path = generation_socket_path.clone();
    let generation_handle = thread::spawn(move || {
        let mut stopped = false;
        for _ in 0..3 {
            let (mut stream, _addr) = generation_listener
                .accept()
                .expect("accept stale generation request");
            let mut request = String::new();
            stream
                .read_to_string(&mut request)
                .expect("read stale generation request");
            if request.contains("\"stop\"") {
                stopped = true;
                stream
                    .write_all(br#"{"ok":true,"response":{"stopping":true}}"#)
                    .expect("write stale generation stop response");
                stream
                    .write_all(b"\n")
                    .expect("write stale generation stop response newline");
                break;
            }
            assert!(request.contains("\"ping\""));
            stream
                .write_all(
                    br#"{"ok":true,"response":{"daemon":{"pid":5151,"binary_version":"0.1.5"},"protocol_version":1,"capabilities":["dispatch"],"message":"pong"}}"#,
                )
                .expect("write stale generation ping response");
            stream
                .write_all(b"\n")
                .expect("write stale generation ping response newline");
        }
        assert!(stopped, "stale generation daemon was not stopped");
        drop(generation_listener);
        fs::remove_file(&stale_generation_socket_path)
            .expect("remove stale generation daemon socket");
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
    assert_eq!(ensured["coexisting_with_incompatible_daemon"], true);
    assert_eq!(ensured["replaced_incompatible_generation_daemon"], true);
    assert_eq!(
        ensured["daemon"]["socket_path"],
        generation_socket_path.display().to_string()
    );

    generation_handle.join().expect("stale generation thread");
    stop_daemon_at_socket_path(&generation_socket_path);
    wait_for_socket_path_removed(&generation_socket_path, Duration::from_secs(10));
    done_tx.send(()).expect("signal old daemon");
    legacy_handle.join().expect("old daemon thread");
}

#[cfg(unix)]
#[test]
fn daemon_ensure_replaces_stale_generation_daemon_without_legacy_default() {
    let home = temp_home();
    let run_dir = home.path().join("run");
    fs::create_dir(&run_dir).expect("create run dir");
    fs::set_permissions(&run_dir, fs::Permissions::from_mode(0o700)).expect("chmod run dir");
    let socket_path = run_dir.join("cbth.sock");
    let generation_dir = run_dir.join("daemons").join(env!("CARGO_PKG_VERSION"));
    fs::create_dir_all(&generation_dir).expect("create generation dir");
    fs::set_permissions(run_dir.join("daemons"), fs::Permissions::from_mode(0o700))
        .expect("chmod daemons dir");
    fs::set_permissions(&generation_dir, fs::Permissions::from_mode(0o700))
        .expect("chmod generation dir");
    let generation_socket_path = generation_dir.join("cbth.sock");

    let generation_listener =
        UnixListener::bind(&generation_socket_path).expect("bind stale generation daemon socket");
    fs::set_permissions(&generation_socket_path, fs::Permissions::from_mode(0o600))
        .expect("chmod generation socket");
    let stale_generation_socket_path = generation_socket_path.clone();
    let generation_handle = thread::spawn(move || {
        let mut stopped = false;
        for _ in 0..3 {
            let (mut stream, _addr) = generation_listener
                .accept()
                .expect("accept stale generation request");
            let mut request = String::new();
            stream
                .read_to_string(&mut request)
                .expect("read stale generation request");
            if request.contains("\"stop\"") {
                stopped = true;
                stream
                    .write_all(br#"{"ok":true,"response":{"stopping":true}}"#)
                    .expect("write stale generation stop response");
                stream
                    .write_all(b"\n")
                    .expect("write stale generation stop response newline");
                break;
            }
            assert!(request.contains("\"ping\""));
            stream
                .write_all(
                    br#"{"ok":true,"response":{"daemon":{"pid":5151,"binary_version":"0.1.5"},"protocol_version":1,"capabilities":["dispatch"],"message":"pong"}}"#,
                )
                .expect("write stale generation ping response");
            stream
                .write_all(b"\n")
                .expect("write stale generation ping response newline");
        }
        assert!(stopped, "stale generation daemon was not stopped");
        drop(generation_listener);
        fs::remove_file(&stale_generation_socket_path)
            .expect("remove stale generation daemon socket");
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
    assert_eq!(ensured["replaced_incompatible_generation_daemon"], true);
    assert_eq!(
        ensured["daemon"]["socket_path"],
        socket_path.display().to_string()
    );

    generation_handle.join().expect("stale generation thread");
    stop_daemon_at_socket_path(&socket_path);
    wait_for_socket_path_removed(&socket_path, Duration::from_secs(10));
}

#[cfg(unix)]
#[test]
fn daemon_dispatch_uses_probed_socket_when_ping_omits_socket_path() {
    let home = temp_home();
    let run_dir = home.path().join("run");
    fs::create_dir(&run_dir).expect("create run dir");
    fs::set_permissions(&run_dir, fs::Permissions::from_mode(0o700)).expect("chmod run dir");
    let socket_path = run_dir.join("cbth.sock");
    let listener = UnixListener::bind(&socket_path).expect("bind compatible daemon socket");
    fs::set_permissions(&socket_path, fs::Permissions::from_mode(0o600)).expect("chmod socket");
    let cleanup_socket_path = socket_path.clone();
    let handle = thread::spawn(move || {
        let (mut ping_stream, _addr) = listener.accept().expect("accept ping request");
        let mut ping_request = String::new();
        ping_stream
            .read_to_string(&mut ping_request)
            .expect("read ping request");
        assert!(ping_request.contains("\"ping\""));
        let ping_response = json!({
            "ok": true,
            "response": {
                "daemon": {
                    "pid": 5151,
                    "binary_version": env!("CARGO_PKG_VERSION")
                },
                "protocol_version": 1,
                "capabilities": [
                    "dispatch",
                    "attempt-dispatch",
                    "cli-app-server-lifecycle",
                    "cli-app-server-probe",
                    "cli-thread-start-bootstrap",
                    "cli-thread-start-params",
                    "cli-foreground-thread-bootstrap",
                    "cli-session-dispatch",
                    "cli-session-capability-dispatch",
                    "cli-session-permission-dispatch",
                    "cli-session-proof-invalidation-dispatch",
                    "cli-session-recovery-dispatch",
                    "cli-turn-observation-dispatch",
                    "cli-turn-observation-expiry-dispatch",
                    "cli-auto-delivery-dispatch",
                    "task-supervisor",
                    "desktop-bridge-foundation-dispatch",
                    "desktop-inbox-revisioned-installation-state",
                    "desktop-writeback-helper-foundation",
                    "desktop-writeback-live-validation-fixture",
                    "desktop-transcript-relay-consumer",
                    "desktop-transcript-relay-scanner",
                    "desktop-ready-arm-workflow",
                    "daemon-handoff-v1"
                ],
                "message": "pong"
            }
        });
        writeln!(ping_stream, "{ping_response}").expect("write ping response");
        drop(ping_stream);

        let (mut dispatch_stream, _addr) = listener.accept().expect("accept dispatch request");
        let mut dispatch_request = String::new();
        dispatch_stream
            .read_to_string(&mut dispatch_request)
            .expect("read dispatch request");
        assert!(dispatch_request.contains("\"dispatch\""));
        let dispatch_response = json!({
            "ok": true,
            "response": {
                "job": {
                    "job_id": "accepted-via-fallback",
                    "status": "pending"
                }
            }
        });
        writeln!(dispatch_stream, "{dispatch_response}").expect("write dispatch response");
        drop(dispatch_stream);
        drop(listener);
        fs::remove_file(&cleanup_socket_path).expect("remove compatible daemon socket");
    });

    let submitted = cbth_daemon(
        &home,
        &[
            "job",
            "submit",
            "--source-thread-id",
            "socket-path-fallback",
            "--summary",
            "socket path fallback",
        ],
    );
    assert_eq!(submitted["job"]["job_id"], "accepted-via-fallback");
    handle.join().expect("compatible daemon thread");
}

#[cfg(unix)]
#[test]
fn desktop_relay_dispatch_uses_ensured_generation_daemon_endpoint() {
    let home = temp_home();
    let run_dir = home.path().join("run");
    fs::create_dir(&run_dir).expect("create run dir");
    fs::set_permissions(&run_dir, fs::Permissions::from_mode(0o700)).expect("chmod run dir");
    let default_socket_path = run_dir.join("cbth.sock");
    let default_listener =
        UnixListener::bind(&default_socket_path).expect("bind incompatible default daemon socket");
    fs::set_permissions(&default_socket_path, fs::Permissions::from_mode(0o600))
        .expect("chmod default socket");
    let cleanup_socket_path = default_socket_path.clone();
    let default_handle = thread::spawn(move || {
        let (mut ping_stream, _addr) = default_listener
            .accept()
            .expect("accept default ping request");
        let mut ping_request = String::new();
        ping_stream
            .read_to_string(&mut ping_request)
            .expect("read default ping request");
        assert!(ping_request.contains("\"ping\""));
        let ping_response = json!({
            "ok": true,
            "response": {
                "daemon": {
                    "pid": 6161,
                    "binary_version": "0.1.5"
                },
                "protocol_version": 1,
                "capabilities": [
                    "dispatch",
                    "attempt-dispatch",
                    "cli-app-server-lifecycle",
                    "cli-app-server-probe",
                    "cli-thread-start-bootstrap",
                    "cli-thread-start-params",
                    "cli-foreground-thread-bootstrap",
                    "cli-session-dispatch",
                    "cli-session-capability-dispatch",
                    "cli-session-permission-dispatch",
                    "cli-session-proof-invalidation-dispatch",
                    "cli-session-recovery-dispatch",
                    "cli-turn-observation-dispatch",
                    "cli-turn-observation-expiry-dispatch",
                    "cli-auto-delivery-dispatch",
                    "task-supervisor",
                    "desktop-bridge-foundation-dispatch",
                    "desktop-inbox-revisioned-installation-state",
                    "desktop-writeback-helper-foundation",
                    "desktop-writeback-live-validation-fixture",
                    "daemon-handoff-v1"
                ],
                "message": "pong"
            }
        });
        writeln!(ping_stream, "{ping_response}").expect("write default ping response");
        drop(ping_stream);

        default_listener
            .set_nonblocking(true)
            .expect("set default listener nonblocking");
        let deadline = Instant::now() + Duration::from_millis(500);
        while Instant::now() < deadline {
            match default_listener.accept() {
                Ok((mut stream, _addr)) => {
                    let mut request = String::new();
                    let _ = stream.read_to_string(&mut request);
                    panic!("default daemon received unexpected relay dispatch: {request}");
                }
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    thread::sleep(Duration::from_millis(20));
                }
                Err(error) => panic!("accept default request: {error}"),
            }
        }
        drop(default_listener);
        fs::remove_file(&cleanup_socket_path).expect("remove default socket");
    });

    let generation_socket_path = run_dir
        .join("daemons")
        .join(env!("CARGO_PKG_VERSION"))
        .join("cbth.sock");
    let mut generation_daemon = spawn_daemon(&home, "300", &["--socket-kind", "generation"]);
    wait_for_path(&generation_socket_path);
    let now: i64 = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before epoch")
        .as_secs()
        .try_into()
        .expect("epoch seconds fit in i64");
    let fixture_now = now.to_string();
    let consume_now = (now + 20).to_string();

    let fixture = cbth(
        &home,
        &[
            "desktop",
            "validation",
            "prepare-writeback-fixture",
            "--source-thread-id",
            "thread-generation-relay",
            "--caller-automation-id",
            "automation-generation-relay",
            "--bridge-request-id",
            "bridge-request-generation-relay",
            "--now",
            &fixture_now,
            "--json",
        ],
    );
    let attempt_id = fixture["desktop_writeback_fixture"]["attempt"]["attempt_id"]
        .as_str()
        .expect("attempt id");
    let marker = "CBTH_GENERATION_RELAY_ENDPOINT";
    let envelope = json!({
        "schema_version": 1,
        "channel": "desktop_transcript_writeback",
        "kind": "arm_pending_requested",
        "source_thread_id": "thread-generation-relay",
        "attempt_id": attempt_id,
        "generation": 1,
        "bridge_request_id": "bridge-request-generation-relay",
        "marker": marker,
        "created_at": now + 10,
    });
    let rollout = home.path().join("generation-relay-rollout.jsonl");
    write_function_call_rollout(
        &rollout,
        &format!(
            "CBTH_TRANSCRIPT_WRITEBACK_V1 {}",
            serde_json::to_string(&envelope).expect("serialize envelope")
        ),
    );

    let consumed = cbth_daemon(
        &home,
        &[
            "desktop",
            "relay",
            "consume-transcript",
            "--rollout-path",
            rollout.to_str().unwrap(),
            "--marker",
            marker,
            "--json",
            "--now",
            &consume_now,
        ],
    );
    assert_eq!(
        consumed["desktop_transcript_relay_consumption"]["record"]["outcome"]["outcome"],
        "arm_pending"
    );

    default_handle.join().expect("default daemon thread");
    stop_daemon_at_socket_path(&generation_socket_path);
    wait_for_socket_path_removed(&generation_socket_path, Duration::from_secs(10));
    generation_daemon.wait().expect("generation daemon exits");
}

#[cfg(unix)]
#[test]
fn daemon_ensure_reuses_generation_daemon_when_legacy_socket_is_absent() {
    let home = temp_home();
    let generation_socket_path = home
        .path()
        .join("run")
        .join("daemons")
        .join(env!("CARGO_PKG_VERSION"))
        .join("cbth.sock");
    let mut daemon = spawn_daemon(
        &home,
        "1",
        &["--socket-kind", "generation", "--skip-startup-sweep"],
    );
    wait_for_path(&generation_socket_path);

    let ensured = cbth(
        &home,
        &[
            "daemon",
            "ensure",
            "--idle-timeout-seconds",
            "1",
            "--startup-timeout-seconds",
            "5",
        ],
    );

    assert_eq!(ensured["started"], false);
    assert_eq!(ensured["using_generation_daemon"], true);
    assert_eq!(
        ensured["daemon"]["socket_path"],
        generation_socket_path.display().to_string()
    );
    assert!(
        !home.path().join("run").join("cbth.sock").exists(),
        "default daemon socket should not be created while generation daemon is alive"
    );

    wait_for_socket_path_removed(&generation_socket_path, Duration::from_secs(5));
    daemon.wait().expect("generation daemon exits after idle");
}

#[cfg(unix)]
#[test]
fn daemon_ensure_replace_incompatible_starts_default_instead_of_reusing_generation() {
    let home = temp_home();
    let socket_path = home.path().join("run").join("cbth.sock");
    let generation_socket_path = home
        .path()
        .join("run")
        .join("daemons")
        .join(env!("CARGO_PKG_VERSION"))
        .join("cbth.sock");
    let mut generation_daemon = spawn_daemon(
        &home,
        "30",
        &["--socket-kind", "generation", "--skip-startup-sweep"],
    );
    wait_for_path(&generation_socket_path);

    let legacy_listener =
        UnixListener::bind(&socket_path).expect("bind incompatible default daemon socket");
    fs::set_permissions(&socket_path, fs::Permissions::from_mode(0o600))
        .expect("chmod default socket");
    let legacy_socket_path = socket_path.clone();
    let legacy_handle = thread::spawn(move || {
        for _ in 0..2 {
            let (mut stream, _addr) = legacy_listener
                .accept()
                .expect("accept incompatible default request");
            let mut request = String::new();
            stream
                .read_to_string(&mut request)
                .expect("read incompatible default request");
            let is_stop = request.contains("\"stop\"");
            let response = if is_stop {
                r#"{"ok":true,"response":{"stopping":true}}"#
            } else {
                r#"{"ok":true,"response":{"daemon":{"pid":4242,"binary_version":"0.1.5"},"message":"pong"}}"#
            };
            stream
                .write_all(response.as_bytes())
                .expect("write incompatible default response");
            stream
                .write_all(b"\n")
                .expect("write incompatible default response newline");
            if is_stop {
                break;
            }
        }
        drop(legacy_listener);
        fs::remove_file(&legacy_socket_path).expect("remove incompatible default socket");
    });

    let ensured = cbth(
        &home,
        &[
            "daemon",
            "ensure",
            "--replace-incompatible",
            "--idle-timeout-seconds",
            "10",
            "--startup-timeout-seconds",
            "5",
        ],
    );
    assert_eq!(ensured["started"], true);
    assert!(ensured.get("using_generation_daemon").is_none());
    assert_eq!(
        ensured["daemon"]["socket_path"],
        socket_path.display().to_string()
    );
    assert!(generation_socket_path.exists());

    legacy_handle.join().expect("incompatible default thread");
    let ping = cbth(&home, &["daemon", "ping"]);
    assert_eq!(ping["protocol_version"], 1);

    cbth(&home, &["daemon", "stop"]);
    wait_for_socket_path_removed(&socket_path, Duration::from_secs(10));
    stop_daemon_at_socket_path(&generation_socket_path);
    wait_for_socket_path_removed(&generation_socket_path, Duration::from_secs(10));
    generation_daemon.wait().expect("generation daemon exits");
}

#[cfg(unix)]
#[test]
fn generation_daemon_startup_recovers_own_lost_task_process_group() {
    let home = temp_home();
    let generation_socket_path = home
        .path()
        .join("run")
        .join("daemons")
        .join(env!("CARGO_PKG_VERSION"))
        .join("cbth.sock");
    let marker = home.path().join("generation-lost-task-marker");
    let marker_arg = marker.to_string_lossy().to_string();
    let mut daemon = spawn_daemon(&home, "300", &["--socket-kind", "generation"]);
    wait_for_path(&generation_socket_path);
    let started = cbth_daemon(
        &home,
        &[
            "task",
            "run",
            "--source-thread-id",
            "thread-generation-lost-task",
            "--summary",
            "generation daemon lost task",
            "--",
            "/bin/sh",
            "-c",
            "sleep 3; printf done > \"$1\"",
            "cbth-task",
            &marker_arg,
        ],
    );
    let task_id = started["task"]["task_id"].as_str().expect("task id");
    wait_for_task_status(&home, task_id, "running");

    daemon.kill().expect("kill generation daemon");
    let _ = daemon.wait().expect("wait generation daemon");
    let mut replacement = spawn_daemon(&home, "30", &["--socket-kind", "generation"]);
    wait_for_path(&generation_socket_path);

    let task = wait_for_task_status(&home, task_id, "lost");
    assert_eq!(
        task["task"]["failure_reason"],
        "task supervisor lost after daemon restart"
    );
    thread::sleep(Duration::from_secs(4));
    assert!(
        !marker.exists(),
        "generation-owned lost supervised process group survived startup recovery"
    );
    stop_daemon_at_socket_path(&generation_socket_path);
    wait_for_socket_path_removed(&generation_socket_path, Duration::from_secs(10));
    replacement
        .wait()
        .expect("replacement generation daemon exits");
}

#[cfg(unix)]
#[test]
fn generation_daemon_startup_recovers_stale_previous_generation_task_process_group() {
    let home = temp_home();
    let generation_socket_path = home
        .path()
        .join("run")
        .join("daemons")
        .join(env!("CARGO_PKG_VERSION"))
        .join("cbth.sock");
    let marker = home.path().join("stale-generation-lost-task-marker");
    let marker_arg = marker.to_string_lossy().to_string();
    let mut daemon = spawn_daemon(&home, "300", &["--socket-kind", "generation"]);
    wait_for_path(&generation_socket_path);
    let started = cbth_daemon(
        &home,
        &[
            "task",
            "run",
            "--source-thread-id",
            "thread-stale-generation-lost-task",
            "--summary",
            "stale generation daemon lost task",
            "--",
            "/bin/sh",
            "-c",
            "sleep 3; printf done > \"$1\"",
            "cbth-task",
            &marker_arg,
        ],
    );
    let task_id = started["task"]["task_id"].as_str().expect("task id");
    wait_for_task_status(&home, task_id, "running");

    daemon.kill().expect("kill generation daemon");
    let _ = daemon.wait().expect("wait generation daemon");
    let conn = Connection::open(home.path().join("cbth.sqlite3")).expect("open db");
    conn.execute(
        "UPDATE jobs
         SET supervisor_daemon_generation = '0.1.4'
         WHERE job_id = (SELECT job_id FROM tasks WHERE task_id = ?)",
        params![task_id],
    )
    .expect("retag stale generation job");
    conn.execute(
        "UPDATE tasks
         SET supervisor_daemon_generation = '0.1.4'
         WHERE task_id = ?",
        params![task_id],
    )
    .expect("retag stale generation task");
    drop(conn);

    let mut replacement = spawn_daemon(&home, "30", &["--socket-kind", "generation"]);
    wait_for_path(&generation_socket_path);

    let task = wait_for_task_status(&home, task_id, "lost");
    assert_eq!(
        task["task"]["failure_reason"],
        "task supervisor lost after daemon restart"
    );
    thread::sleep(Duration::from_secs(4));
    assert!(
        !marker.exists(),
        "stale generation lost supervised process group survived startup recovery"
    );
    stop_daemon_at_socket_path(&generation_socket_path);
    wait_for_socket_path_removed(&generation_socket_path, Duration::from_secs(10));
    replacement
        .wait()
        .expect("replacement generation daemon exits");
}

#[cfg(unix)]
#[test]
fn generation_daemon_ignores_live_previous_generation_tasks_for_idle_exit() {
    let home = temp_home();
    let current_generation_socket_path = home
        .path()
        .join("run")
        .join("daemons")
        .join(env!("CARGO_PKG_VERSION"))
        .join("cbth.sock");
    let previous_generation_dir = home.path().join("run").join("daemons").join("0.1.4");
    fs::create_dir_all(&previous_generation_dir).expect("create previous generation dir");
    fs::set_permissions(home.path().join("run"), fs::Permissions::from_mode(0o700))
        .expect("chmod run dir");
    fs::set_permissions(
        home.path().join("run").join("daemons"),
        fs::Permissions::from_mode(0o700),
    )
    .expect("chmod daemons dir");
    fs::set_permissions(&previous_generation_dir, fs::Permissions::from_mode(0o700))
        .expect("chmod previous generation dir");
    let previous_generation_socket_path = previous_generation_dir.join("cbth.sock");
    let previous_listener =
        UnixListener::bind(&previous_generation_socket_path).expect("bind previous generation");
    fs::set_permissions(
        &previous_generation_socket_path,
        fs::Permissions::from_mode(0o600),
    )
    .expect("chmod previous generation socket");

    let submitted = cbth(
        &home,
        &[
            "job",
            "submit",
            "--source-thread-id",
            "previous-generation-thread",
            "--summary",
            "previous generation pending job",
        ],
    );
    let job_id = submitted["job"]["job_id"].as_str().expect("job id");
    let conn = Connection::open(home.path().join("cbth.sqlite3")).expect("open db");
    conn.execute(
        "UPDATE jobs
         SET supervisor_daemon_generation = '0.1.4'
         WHERE job_id = ?",
        params![job_id],
    )
    .expect("retag previous generation job");
    conn.execute(
        "INSERT INTO tasks (
            task_id, job_id, source_thread_id, status, summary, command_json,
            cwd, max_delivery_attempts, redelivery_window_seconds,
            supervisor_daemon_generation, created_at
         ) VALUES (
            'previous-generation-live-task', ?, 'previous-generation-thread',
            'queued', 'previous generation live task', '[]', '/', 3, 86400,
            '0.1.4', 1
         )",
        params![job_id],
    )
    .expect("insert previous generation task");
    drop(conn);

    let mut daemon = spawn_daemon(&home, "1", &["--socket-kind", "generation"]);
    wait_for_path(&current_generation_socket_path);
    wait_for_socket_path_removed(&current_generation_socket_path, Duration::from_secs(10));
    daemon
        .wait()
        .expect("current generation daemon exits after idle");

    let task = cbth(
        &home,
        &[
            "task",
            "inspect",
            "--task-id",
            "previous-generation-live-task",
        ],
    );
    assert_eq!(task["task"]["status"], "queued");
    drop(previous_listener);
    let _ = fs::remove_file(previous_generation_socket_path);
}

#[cfg(unix)]
#[test]
fn generation_daemon_ignores_unowned_legacy_tasks_for_idle_exit() {
    let home = temp_home();
    let generation_socket_path = home
        .path()
        .join("run")
        .join("daemons")
        .join(env!("CARGO_PKG_VERSION"))
        .join("cbth.sock");
    let submitted = cbth(
        &home,
        &[
            "job",
            "submit",
            "--source-thread-id",
            "legacy-thread",
            "--summary",
            "legacy pending job",
        ],
    );
    let job_id = submitted["job"]["job_id"].as_str().expect("job id");
    let conn = Connection::open(home.path().join("cbth.sqlite3")).expect("open db");
    conn.execute(
        "INSERT INTO tasks (
            task_id, job_id, source_thread_id, status, summary, command_json,
            cwd, max_delivery_attempts, redelivery_window_seconds, created_at
         ) VALUES (
            'legacy-unowned-task', ?, 'legacy-thread', 'queued',
            'legacy unowned task', '[]', '/', 3, 86400, 1
         )",
        params![job_id],
    )
    .expect("insert legacy unowned task");
    drop(conn);

    let mut daemon = spawn_daemon(&home, "1", &["--socket-kind", "generation"]);
    wait_for_path(&generation_socket_path);
    wait_for_socket_path_removed(&generation_socket_path, Duration::from_secs(10));
    daemon.wait().expect("generation daemon exits after idle");

    let task = cbth(
        &home,
        &["task", "inspect", "--task-id", "legacy-unowned-task"],
    );
    assert_eq!(task["task"]["status"], "queued");
}

#[cfg(unix)]
#[test]
fn generation_daemon_keeps_owned_pending_job_as_idle_blocker() {
    let home = temp_home();
    let generation_socket_path = home
        .path()
        .join("run")
        .join("daemons")
        .join(env!("CARGO_PKG_VERSION"))
        .join("cbth.sock");
    let mut daemon = spawn_daemon(&home, "1", &["--socket-kind", "generation"]);
    wait_for_path(&generation_socket_path);

    let submitted = cbth_daemon(
        &home,
        &[
            "job",
            "submit",
            "--source-thread-id",
            "generation-owned-pending-job",
            "--summary",
            "generation owned pending job",
        ],
    );
    let job_id = submitted["job"]["job_id"].as_str().expect("job id");
    let conn = Connection::open(home.path().join("cbth.sqlite3")).expect("open db");
    let supervisor_generation: Option<String> = conn
        .query_row(
            "SELECT supervisor_daemon_generation FROM jobs WHERE job_id = ?",
            params![job_id],
            |row| row.get(0),
        )
        .expect("query job supervisor generation");
    assert_eq!(
        supervisor_generation.as_deref(),
        Some(env!("CARGO_PKG_VERSION"))
    );

    thread::sleep(Duration::from_secs(2));
    assert!(
        generation_socket_path.exists(),
        "generation daemon exited while it owned a pending job"
    );
    assert!(
        daemon.try_wait().expect("poll generation daemon").is_none(),
        "generation daemon process exited while it owned a pending job"
    );

    stop_daemon_at_socket_path(&generation_socket_path);
    wait_for_socket_path_removed(&generation_socket_path, Duration::from_secs(10));
    daemon.wait().expect("generation daemon exits");
}

#[cfg(unix)]
#[test]
fn daemon_ensure_restarts_daemon_missing_turn_observation_capability() {
    let home = temp_home();
    let run_dir = home.path().join("run");
    fs::create_dir(&run_dir).expect("create run dir");
    fs::set_permissions(&run_dir, fs::Permissions::from_mode(0o700)).expect("chmod run dir");
    let socket_path = run_dir.join("cbth.sock");
    let listener = UnixListener::bind(&socket_path).expect("bind old daemon socket");
    fs::set_permissions(&socket_path, fs::Permissions::from_mode(0o600)).expect("chmod socket");
    let old_socket_path = socket_path.clone();
    let handle = thread::spawn(move || {
        for _ in 0..2 {
            let (mut stream, _addr) = listener.accept().expect("accept old request");
            let mut request = String::new();
            stream
                .read_to_string(&mut request)
                .expect("read old request");
            let response = if request.contains("\"stop\"") {
                r#"{"ok":true,"response":{"stopping":true}}"#
            } else {
                r#"{"ok":true,"response":{"daemon":{"pid":1313},"protocol_version":1,"capabilities":["dispatch","attempt-dispatch","cli-app-server-lifecycle","cli-thread-start-bootstrap","cli-thread-start-params",
            "cli-foreground-thread-bootstrap","cli-session-dispatch","cli-session-capability-dispatch","cli-session-proof-invalidation-dispatch"],"message":"pong"}}"#
            };
            stream
                .write_all(response.as_bytes())
                .expect("write old response");
            stream.write_all(b"\n").expect("write old response newline");
            if request.contains("\"stop\"") {
                break;
            }
        }
        drop(listener);
        fs::remove_file(&old_socket_path).expect("remove old socket");
    });

    let ensured = cbth(
        &home,
        &[
            "daemon",
            "ensure",
            "--replace-incompatible",
            "--idle-timeout-seconds",
            "10",
            "--startup-timeout-seconds",
            "5",
        ],
    );
    assert_eq!(ensured["started"], true);
    assert_ne!(ensured["daemon"]["pid"].as_u64().expect("pid"), 1313);
    handle.join().expect("old daemon thread");

    let ping = cbth(&home, &["daemon", "ping"]);
    assert_eq!(
        ping["capabilities"],
        json!([
            "dispatch",
            "attempt-dispatch",
            "cli-app-server-lifecycle",
            "cli-app-server-probe",
            "cli-thread-start-bootstrap",
            "cli-thread-start-params",
            "cli-foreground-thread-bootstrap",
            "cli-session-dispatch",
            "cli-session-capability-dispatch",
            "cli-session-permission-dispatch",
            "cli-session-proof-invalidation-dispatch",
            "cli-session-recovery-dispatch",
            "cli-turn-observation-dispatch",
            "cli-turn-observation-expiry-dispatch",
            "cli-auto-delivery-dispatch",
            "task-supervisor",
            "desktop-bridge-foundation-dispatch",
            "desktop-inbox-revisioned-installation-state",
            "desktop-writeback-helper-foundation",
            "desktop-writeback-live-validation-fixture",
            "desktop-transcript-relay-consumer",
            "desktop-transcript-relay-scanner",
            "desktop-ready-arm-workflow",
            "daemon-handoff-v1"
        ])
    );

    cbth(&home, &["daemon", "stop"]);
    wait_for_socket_removed(&home);
}

#[cfg(unix)]
#[test]
fn daemon_ensure_restarts_daemon_missing_auto_delivery_capability() {
    let home = temp_home();
    let run_dir = home.path().join("run");
    fs::create_dir(&run_dir).expect("create run dir");
    fs::set_permissions(&run_dir, fs::Permissions::from_mode(0o700)).expect("chmod run dir");
    let socket_path = run_dir.join("cbth.sock");
    let listener = UnixListener::bind(&socket_path).expect("bind old daemon socket");
    fs::set_permissions(&socket_path, fs::Permissions::from_mode(0o600)).expect("chmod socket");
    let old_socket_path = socket_path.clone();
    let handle = thread::spawn(move || {
        for _ in 0..2 {
            let (mut stream, _addr) = listener.accept().expect("accept old request");
            let mut request = String::new();
            stream
                .read_to_string(&mut request)
                .expect("read old request");
            let response = if request.contains("\"stop\"") {
                r#"{"ok":true,"response":{"stopping":true}}"#
            } else {
                r#"{"ok":true,"response":{"daemon":{"pid":1323},"protocol_version":1,"capabilities":["dispatch","attempt-dispatch","cli-app-server-lifecycle","cli-thread-start-bootstrap","cli-thread-start-params",
            "cli-foreground-thread-bootstrap","cli-session-dispatch","cli-session-capability-dispatch","cli-session-proof-invalidation-dispatch","cli-turn-observation-dispatch","cli-turn-observation-expiry-dispatch"],"message":"pong"}}"#
            };
            stream
                .write_all(response.as_bytes())
                .expect("write old response");
            stream.write_all(b"\n").expect("write old response newline");
            if request.contains("\"stop\"") {
                break;
            }
        }
        drop(listener);
        fs::remove_file(&old_socket_path).expect("remove old socket");
    });

    let ensured = cbth(
        &home,
        &[
            "daemon",
            "ensure",
            "--replace-incompatible",
            "--idle-timeout-seconds",
            "10",
            "--startup-timeout-seconds",
            "5",
        ],
    );
    assert_eq!(ensured["started"], true);
    assert_ne!(ensured["daemon"]["pid"].as_u64().expect("pid"), 1323);
    handle.join().expect("old daemon thread");

    let ping = cbth(&home, &["daemon", "ping"]);
    assert_eq!(
        ping["capabilities"],
        json!([
            "dispatch",
            "attempt-dispatch",
            "cli-app-server-lifecycle",
            "cli-app-server-probe",
            "cli-thread-start-bootstrap",
            "cli-thread-start-params",
            "cli-foreground-thread-bootstrap",
            "cli-session-dispatch",
            "cli-session-capability-dispatch",
            "cli-session-permission-dispatch",
            "cli-session-proof-invalidation-dispatch",
            "cli-session-recovery-dispatch",
            "cli-turn-observation-dispatch",
            "cli-turn-observation-expiry-dispatch",
            "cli-auto-delivery-dispatch",
            "task-supervisor",
            "desktop-bridge-foundation-dispatch",
            "desktop-inbox-revisioned-installation-state",
            "desktop-writeback-helper-foundation",
            "desktop-writeback-live-validation-fixture",
            "desktop-transcript-relay-consumer",
            "desktop-transcript-relay-scanner",
            "desktop-ready-arm-workflow",
            "daemon-handoff-v1"
        ])
    );

    cbth(&home, &["daemon", "stop"]);
    wait_for_socket_removed(&home);
}

#[cfg(unix)]
#[test]
fn daemon_ensure_restarts_daemon_missing_session_capability_dispatch() {
    let home = temp_home();
    let run_dir = home.path().join("run");
    fs::create_dir(&run_dir).expect("create run dir");
    fs::set_permissions(&run_dir, fs::Permissions::from_mode(0o700)).expect("chmod run dir");
    let socket_path = run_dir.join("cbth.sock");
    let listener = UnixListener::bind(&socket_path).expect("bind old daemon socket");
    fs::set_permissions(&socket_path, fs::Permissions::from_mode(0o600)).expect("chmod socket");
    let old_socket_path = socket_path.clone();
    let handle = thread::spawn(move || {
        for _ in 0..2 {
            let (mut stream, _addr) = listener.accept().expect("accept old request");
            let mut request = String::new();
            stream
                .read_to_string(&mut request)
                .expect("read old request");
            let response = if request.contains("\"stop\"") {
                r#"{"ok":true,"response":{"stopping":true}}"#
            } else {
                r#"{"ok":true,"response":{"daemon":{"pid":1414},"protocol_version":1,"capabilities":["dispatch","attempt-dispatch","cli-app-server-lifecycle","cli-thread-start-bootstrap","cli-thread-start-params",
            "cli-foreground-thread-bootstrap","cli-session-dispatch","cli-turn-observation-dispatch"],"message":"pong"}}"#
            };
            stream
                .write_all(response.as_bytes())
                .expect("write old response");
            stream.write_all(b"\n").expect("write old response newline");
            if request.contains("\"stop\"") {
                break;
            }
        }
        drop(listener);
        fs::remove_file(&old_socket_path).expect("remove old socket");
    });

    let ensured = cbth(
        &home,
        &[
            "daemon",
            "ensure",
            "--replace-incompatible",
            "--idle-timeout-seconds",
            "10",
            "--startup-timeout-seconds",
            "5",
        ],
    );
    assert_eq!(ensured["started"], true);
    assert_ne!(ensured["daemon"]["pid"].as_u64().expect("pid"), 1414);
    handle.join().expect("old daemon thread");

    let ping = cbth(&home, &["daemon", "ping"]);
    assert_eq!(
        ping["capabilities"],
        json!([
            "dispatch",
            "attempt-dispatch",
            "cli-app-server-lifecycle",
            "cli-app-server-probe",
            "cli-thread-start-bootstrap",
            "cli-thread-start-params",
            "cli-foreground-thread-bootstrap",
            "cli-session-dispatch",
            "cli-session-capability-dispatch",
            "cli-session-permission-dispatch",
            "cli-session-proof-invalidation-dispatch",
            "cli-session-recovery-dispatch",
            "cli-turn-observation-dispatch",
            "cli-turn-observation-expiry-dispatch",
            "cli-auto-delivery-dispatch",
            "task-supervisor",
            "desktop-bridge-foundation-dispatch",
            "desktop-inbox-revisioned-installation-state",
            "desktop-writeback-helper-foundation",
            "desktop-writeback-live-validation-fixture",
            "desktop-transcript-relay-consumer",
            "desktop-transcript-relay-scanner",
            "desktop-ready-arm-workflow",
            "daemon-handoff-v1"
        ])
    );

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
    let replacement_temp_socket_path = run_dir.join("replacement.sock");
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

        let replacement_listener =
            UnixListener::bind(&replacement_temp_socket_path).expect("bind replacement socket");
        fs::set_permissions(
            &replacement_temp_socket_path,
            fs::Permissions::from_mode(0o600),
        )
        .expect("chmod replacement socket");
        fs::rename(&replacement_temp_socket_path, &replacement_socket_path)
            .expect("publish replacement socket");
        replacement_listener
            .set_nonblocking(true)
            .expect("set replacement listener nonblocking");
        let deadline = Instant::now() + Duration::from_secs(5);
        let mut accepted = 0;
        while accepted < 2 && Instant::now() < deadline {
            match replacement_listener.accept() {
                Ok((mut stream, _addr)) => {
                    stream
                        .set_nonblocking(false)
                        .expect("set replacement stream blocking");
                    let mut request = [0_u8; 1024];
                    let request_len = stream.read(&mut request).expect("read replacement request");
                    let request = String::from_utf8_lossy(&request[..request_len]);
                    assert!(request.contains("\"ping\""));
                    if let Err(error) = stream.write_all(
                        br#"{"ok":true,"response":{"daemon":{"pid":5151},"protocol_version":1,"capabilities":["dispatch","attempt-dispatch","cli-app-server-lifecycle","cli-app-server-probe","cli-thread-start-bootstrap","cli-thread-start-params",
            "cli-foreground-thread-bootstrap","cli-session-dispatch","cli-session-capability-dispatch","cli-session-permission-dispatch","cli-session-proof-invalidation-dispatch","cli-session-recovery-dispatch","cli-turn-observation-dispatch","cli-turn-observation-expiry-dispatch","cli-auto-delivery-dispatch","task-supervisor","desktop-bridge-foundation-dispatch","desktop-inbox-revisioned-installation-state","desktop-writeback-helper-foundation","desktop-writeback-live-validation-fixture","desktop-transcript-relay-consumer","desktop-transcript-relay-scanner","desktop-ready-arm-workflow","daemon-handoff-v1"],"message":"pong"}}"#,
                    ) {
                        if is_peer_disconnect(&error) {
                            continue;
                        }
                        panic!("write replacement response: {error}");
                    }
                    if let Err(error) = stream.write_all(b"\n") {
                        if is_peer_disconnect(&error) {
                            continue;
                        }
                        panic!("write response newline: {error}");
                    }
                    accepted += 1;
                }
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    thread::sleep(Duration::from_millis(20));
                }
                Err(error) => panic!("accept replacement request: {error}"),
            }
        }
        assert!(accepted >= 1, "replacement daemon was not probed");
        drop(replacement_listener);
        fs::remove_file(&replacement_socket_path).expect("remove replacement socket");
    });

    let ensured = cbth(
        &home,
        &[
            "daemon",
            "ensure",
            "--replace-incompatible",
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
        for index in 0..3 {
            let (mut stream, _addr) = listener.accept().expect("accept busy daemon request");
            let mut request = String::new();
            stream
                .read_to_string(&mut request)
                .expect("read busy daemon request");
            assert!(request.contains("\"ping\""));
            let response = if index == 0 {
                r#"{"ok":false,"error":"daemon is busy"}"#
            } else if index == 1 {
                r#"{"ok":false,"error":"daemon connection limit reached"}"#
            } else {
                r#"{"ok":true,"response":{"daemon":{"pid":4242},"protocol_version":1,"capabilities":["dispatch","attempt-dispatch","cli-app-server-lifecycle","cli-app-server-probe","cli-thread-start-bootstrap","cli-thread-start-params",
            "cli-foreground-thread-bootstrap","cli-session-dispatch","cli-session-capability-dispatch","cli-session-permission-dispatch","cli-session-proof-invalidation-dispatch","cli-session-recovery-dispatch","cli-turn-observation-dispatch","cli-turn-observation-expiry-dispatch","cli-auto-delivery-dispatch","task-supervisor","desktop-bridge-foundation-dispatch","desktop-inbox-revisioned-installation-state","desktop-writeback-helper-foundation","desktop-writeback-live-validation-fixture","desktop-transcript-relay-consumer","desktop-transcript-relay-scanner","desktop-ready-arm-workflow","daemon-handoff-v1"],"message":"pong"}}"#
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
            "15",
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

    let conn = hold_exclusive_db_lock(&home);

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

#[test]
fn daemon_lifecycle_refresh_does_not_block_control_requests_when_db_is_locked() {
    let home = temp_home();
    cbth(
        &home,
        &[
            "job",
            "submit",
            "--source-thread-id",
            "thread-control-while-locked",
            "--summary",
            "initialize db before daemon starts",
        ],
    );
    let mut child = spawn_daemon(&home, "1", &[]);

    let ping = wait_for_ping(&home);
    assert_eq!(ping["message"], "pong");

    let conn = hold_exclusive_db_lock(&home);

    thread::sleep(Duration::from_secs(2));
    assert!(
        child.try_wait().expect("check daemon status").is_none(),
        "daemon exited while lifecycle status could not be refreshed"
    );

    let ping_started = Instant::now();
    let ping = cbth(&home, &["daemon", "ping"]);
    assert_eq!(ping["message"], "pong");
    assert!(
        ping_started.elapsed() < Duration::from_secs(2),
        "daemon ping was blocked by lifecycle refresh"
    );

    let stopped = cbth(&home, &["daemon", "stop"]);
    assert_eq!(stopped["stopping"], true);
    drop(conn);

    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if let Some(status) = child.try_wait().expect("check daemon status") {
            assert!(status.success());
            break;
        }
        assert!(
            Instant::now() < deadline,
            "daemon did not exit after stop request"
        );
        thread::sleep(Duration::from_millis(100));
    }
    wait_for_socket_removed(&home);
}

#[test]
fn daemon_skip_startup_sweep_exits_when_due_batch_waits_for_explicit_dispatch() {
    let home = temp_home();
    let submitted = cbth(
        &home,
        &[
            "job",
            "submit",
            "--source-thread-id",
            "thread-skip-startup-sweep",
            "--summary",
            "preserve explicit sweep report",
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
            "ready for explicit sweep",
            "--redelivery-window-seconds",
            "1",
        ],
    );
    let batch_id = failed["batch"]["batch"]["batch_id"]
        .as_str()
        .expect("batch id");
    thread::sleep(Duration::from_secs(2));

    let mut child = spawn_daemon(&home, "1", &["--skip-startup-sweep"]);
    let ping = wait_for_ping(&home);
    assert_eq!(ping["message"], "pong");

    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if let Some(status) = child.try_wait().expect("check daemon status") {
            assert!(status.success());
            break;
        }
        assert!(
            Instant::now() < deadline,
            "daemon did not exit while lifecycle maintenance was suppressed"
        );
        thread::sleep(Duration::from_millis(100));
    }

    let inspected = cbth(&home, &["batch", "inspect", "--batch-id", batch_id]);
    assert_eq!(inspected["batch"]["batch"]["state"], "open");
    wait_for_socket_removed(&home);
}

#[test]
fn daemon_skip_startup_sweep_exits_when_due_cli_observation_waits_for_explicit_dispatch() {
    let home = temp_home();
    let submitted = cbth(
        &home,
        &[
            "job",
            "submit",
            "--source-thread-id",
            "thread-skip-cli-observation",
            "--summary",
            "preserve explicit CLI observation sweep",
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
            "ready for explicit sweep",
            "--redelivery-window-seconds",
            "60",
        ],
    );
    let batch_id = failed["batch"]["batch"]["batch_id"]
        .as_str()
        .expect("batch id");
    let managed_session_id = bind_idle_cli_session(&home, "thread-skip-cli-observation");
    let pending = cbth(
        &home,
        &[
            "attempt",
            "begin-cli-accept",
            "--batch-id",
            batch_id,
            "--managed-session-id",
            &managed_session_id,
            "--session-epoch",
            "1",
            "--rpc-kind",
            "turn-start",
            "--rpc-request-id",
            "rpc-request-skip-cli",
            "--now",
            "100",
        ],
    );
    let attempt_id = pending["attempt"]["attempt_id"]
        .as_str()
        .expect("attempt id");
    cbth(
        &home,
        &[
            "attempt",
            "accept-cli",
            "--attempt-id",
            attempt_id,
            "--delivery-turn-id",
            "turn-skip-cli",
            "--observation-window-seconds",
            "1",
            "--now",
            "101",
        ],
    );

    let mut child = spawn_daemon(&home, "1", &["--skip-startup-sweep"]);
    let ping = wait_for_ping(&home);
    assert_eq!(ping["message"], "pong");

    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if let Some(status) = child.try_wait().expect("check daemon status") {
            assert!(status.success());
            break;
        }
        assert!(
            Instant::now() < deadline,
            "daemon did not exit while lifecycle maintenance was suppressed"
        );
        thread::sleep(Duration::from_millis(100));
    }

    let attempt = cbth(&home, &["attempt", "inspect", "--attempt-id", attempt_id]);
    assert_eq!(attempt["attempt"]["state"], "cooldown");
    assert_eq!(attempt["attempt"]["delivery_observation_state"], "tracking");
    let inspected = cbth(&home, &["batch", "inspect", "--batch-id", batch_id]);
    assert_eq!(inspected["batch"]["batch"]["replay_policy"], "automatic");
    wait_for_socket_removed(&home);
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
fn daemon_does_not_idle_exit_while_job_is_pending() {
    let home = temp_home();
    let mut child = spawn_daemon(&home, "1", &[]);

    let ping = wait_for_ping(&home);
    assert_eq!(ping["message"], "pong");
    let submitted = cbth(
        &home,
        &[
            "job",
            "submit",
            "--source-thread-id",
            "thread-pending-job-keeps-daemon",
            "--summary",
            "wait for long external task",
        ],
    );
    let job_id = submitted["job"]["job_id"].as_str().expect("job id");

    thread::sleep(Duration::from_secs(2));
    assert!(
        child.try_wait().expect("check daemon status").is_none(),
        "daemon exited while a job was still pending"
    );

    cbth(
        &home,
        &[
            "job",
            "fail",
            "--job-id",
            job_id,
            "--reason",
            "operator cancelled",
            "--redelivery-window-seconds",
            "60",
        ],
    );
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if let Some(status) = child.try_wait().expect("check daemon status") {
            assert!(status.success());
            break;
        }
        assert!(
            Instant::now() < deadline,
            "daemon did not exit after pending job cleared"
        );
        thread::sleep(Duration::from_millis(100));
    }
    let output = child.wait_with_output().expect("daemon output");
    let shutdown: Value = serde_json::from_slice(&output.stdout).expect("daemon shutdown json");
    assert_eq!(shutdown["shutdown_reason"], "idle_timeout");
    wait_for_socket_removed(&home);
}

#[test]
fn quiescing_daemon_waits_for_pending_job_then_exits_without_idle_timeout() {
    let home = temp_home();
    let mut child = spawn_daemon(&home, "300", &[]);

    let ping = wait_for_ping(&home);
    assert_eq!(ping["message"], "pong");
    let submitted = cbth_daemon(
        &home,
        &[
            "job",
            "submit",
            "--source-thread-id",
            "thread-quiesce-pending-job",
            "--summary",
            "pending job keeps quiescing daemon alive",
        ],
    );
    let job_id = submitted["job"]["job_id"].as_str().expect("job id");

    let quiesce = cbth_daemon(&home, &["daemon", "handoff-quiesce"]);
    assert_eq!(quiesce["quiescing"], true);
    thread::sleep(Duration::from_millis(500));
    assert!(
        child.try_wait().expect("check daemon status").is_none(),
        "quiescing daemon exited before its pending job drained"
    );

    cbth(
        &home,
        &[
            "job",
            "fail",
            "--job-id",
            job_id,
            "--reason",
            "operator drained legacy daemon",
            "--redelivery-window-seconds",
            "60",
        ],
    );

    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if let Some(status) = child.try_wait().expect("check daemon status") {
            assert!(status.success());
            break;
        }
        assert!(
            Instant::now() < deadline,
            "quiescing daemon did not exit after pending job cleared"
        );
        thread::sleep(Duration::from_millis(100));
    }
    let output = child.wait_with_output().expect("daemon output");
    let shutdown: Value = serde_json::from_slice(&output.stdout).expect("daemon shutdown json");
    assert_eq!(shutdown["shutdown_reason"], "handoff_drain_complete");
    wait_for_socket_removed(&home);
}

#[test]
fn quiescing_daemon_supervises_existing_task_to_terminal_before_exit() {
    let home = temp_home();
    let mut child = spawn_daemon(&home, "300", &[]);

    let ping = wait_for_ping(&home);
    assert_eq!(ping["message"], "pong");
    let started = cbth_daemon(
        &home,
        &[
            "task",
            "run",
            "--source-thread-id",
            "thread-quiesce-live-task",
            "--summary",
            "live task drains before daemon exit",
            "--",
            "/bin/sh",
            "-c",
            "sleep 1; printf drained",
        ],
    );
    let task_id = started["task"]["task_id"].as_str().expect("task id");

    let quiesce = cbth_daemon(&home, &["daemon", "handoff-quiesce"]);
    assert_eq!(quiesce["quiescing"], true);
    let task = wait_for_task_status(&home, task_id, "succeeded");
    let stdout_log_path = task["task"]["stdout_log_path"]
        .as_str()
        .expect("stdout log path");
    let stdout = fs::read_to_string(home.path().join(stdout_log_path)).expect("stdout log");
    assert_eq!(stdout, "drained");

    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if let Some(status) = child.try_wait().expect("check daemon status") {
            assert!(status.success());
            break;
        }
        assert!(
            Instant::now() < deadline,
            "quiescing daemon did not exit after supervised task drained"
        );
        thread::sleep(Duration::from_millis(100));
    }
    let output = child.wait_with_output().expect("daemon output");
    let shutdown: Value = serde_json::from_slice(&output.stdout).expect("daemon shutdown json");
    assert_eq!(shutdown["shutdown_reason"], "handoff_drain_complete");
    wait_for_socket_removed(&home);
}

#[test]
fn task_cancel_routes_to_quiescing_daemon_that_owns_live_task() {
    let home = temp_home();
    let mut child = spawn_daemon(&home, "300", &[]);

    let ping = wait_for_ping(&home);
    assert_eq!(ping["message"], "pong");
    let started = cbth_daemon(
        &home,
        &[
            "task",
            "run",
            "--source-thread-id",
            "thread-quiesce-cancel-task",
            "--summary",
            "cancel live task on quiescing daemon",
            "--",
            "/bin/sh",
            "-c",
            "sleep 30",
        ],
    );
    let task_id = started["task"]["task_id"].as_str().expect("task id");

    let quiesce = cbth_daemon(&home, &["daemon", "handoff-quiesce"]);
    assert_eq!(quiesce["quiescing"], true);
    let cancelled = cbth_daemon(&home, &["task", "cancel", "--task-id", task_id]);
    assert!(cancelled["task"]["cancel_requested_at"].is_number());
    let task = wait_for_task_status(&home, task_id, "cancelled");
    assert_eq!(task["task"]["failure_reason"], "task cancelled");

    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if let Some(status) = child.try_wait().expect("check daemon status") {
            assert!(status.success());
            break;
        }
        assert!(
            Instant::now() < deadline,
            "quiescing daemon did not exit after cancelled task drained"
        );
        thread::sleep(Duration::from_millis(100));
    }
    let output = child.wait_with_output().expect("daemon output");
    let shutdown: Value = serde_json::from_slice(&output.stdout).expect("daemon shutdown json");
    assert_eq!(shutdown["shutdown_reason"], "handoff_drain_complete");
    wait_for_socket_removed(&home);
}

#[cfg(unix)]
#[test]
fn task_cancel_routes_to_quiescing_generation_daemon_that_owns_live_task() {
    let home = temp_home();
    let generation_socket_path = home
        .path()
        .join("run")
        .join("daemons")
        .join(env!("CARGO_PKG_VERSION"))
        .join("cbth.sock");
    let mut child = spawn_daemon(&home, "300", &["--socket-kind", "generation"]);
    wait_for_path(&generation_socket_path);

    let started = cbth_daemon(
        &home,
        &[
            "task",
            "run",
            "--source-thread-id",
            "thread-quiesce-generation-cancel-task",
            "--summary",
            "cancel live task on quiescing generation daemon",
            "--",
            "/bin/sh",
            "-c",
            "sleep 30",
        ],
    );
    let task_id = started["task"]["task_id"].as_str().expect("task id");
    let quiesce = daemon_command_at_socket_path(&generation_socket_path, "handoff_quiesce");
    assert_eq!(quiesce["quiescing"], true);

    let cancelled = cbth_daemon(&home, &["task", "cancel", "--task-id", task_id]);
    assert!(cancelled["task"]["cancel_requested_at"].is_number());
    let task = wait_for_task_status(&home, task_id, "cancelled");
    assert_eq!(task["task"]["failure_reason"], "task cancelled");

    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if let Some(status) = child.try_wait().expect("check daemon status") {
            assert!(status.success());
            break;
        }
        assert!(
            Instant::now() < deadline,
            "quiescing generation daemon did not exit after cancelled task drained"
        );
        thread::sleep(Duration::from_millis(100));
    }
    let output = child.wait_with_output().expect("daemon output");
    let shutdown: Value = serde_json::from_slice(&output.stdout).expect("daemon shutdown json");
    assert_eq!(shutdown["shutdown_reason"], "handoff_drain_complete");
    wait_for_socket_path_removed(&generation_socket_path, Duration::from_secs(10));
}

#[cfg(unix)]
#[test]
fn task_cancel_falls_back_to_generation_recovery_when_owner_socket_is_stale() {
    let home = temp_home();
    let generation_socket_path = home
        .path()
        .join("run")
        .join("daemons")
        .join(env!("CARGO_PKG_VERSION"))
        .join("cbth.sock");
    let marker = home.path().join("stale-generation-cancel-marker");
    let marker_arg = marker.to_string_lossy().to_string();
    let mut daemon = spawn_daemon(&home, "300", &["--socket-kind", "generation"]);
    wait_for_path(&generation_socket_path);

    let started = cbth_daemon(
        &home,
        &[
            "task",
            "run",
            "--source-thread-id",
            "thread-stale-generation-cancel-task",
            "--summary",
            "stale generation owner cancel",
            "--",
            "/bin/sh",
            "-c",
            "sleep 3; printf done > \"$1\"",
            "cbth-task",
            &marker_arg,
        ],
    );
    let task_id = started["task"]["task_id"].as_str().expect("task id");
    wait_for_task_status(&home, task_id, "running");

    daemon.kill().expect("kill generation daemon");
    let _ = daemon.wait().expect("wait generation daemon");
    assert!(
        generation_socket_path.exists(),
        "killed generation daemon should leave a stale owner socket"
    );

    let cancelled = cbth_daemon(&home, &["task", "cancel", "--task-id", task_id]);
    assert_ne!(cancelled["task"]["status"], "running");
    let task = wait_for_task_status(&home, task_id, "lost");
    assert_eq!(
        task["task"]["failure_reason"],
        "task supervisor lost after daemon restart"
    );
    thread::sleep(Duration::from_secs(4));
    assert!(
        !marker.exists(),
        "generation-owned task process survived stale-owner cancel fallback"
    );

    stop_daemon_at_socket_path(&generation_socket_path);
    wait_for_socket_path_removed(&generation_socket_path, Duration::from_secs(10));
}

#[cfg(unix)]
#[test]
fn task_cancel_reused_generation_daemon_recovers_stale_previous_generation_task() {
    let home = temp_home();
    let generation_socket_path = home
        .path()
        .join("run")
        .join("daemons")
        .join(env!("CARGO_PKG_VERSION"))
        .join("cbth.sock");
    let previous_generation_dir = home.path().join("run").join("daemons").join("0.1.4");
    let previous_generation_socket_path = previous_generation_dir.join("cbth.sock");
    let marker = home.path().join("stale-previous-generation-cancel-marker");
    let marker_arg = marker.to_string_lossy().to_string();
    let mut daemon = spawn_daemon(&home, "300", &["--socket-kind", "generation"]);
    wait_for_path(&generation_socket_path);

    let started = cbth_daemon(
        &home,
        &[
            "task",
            "run",
            "--source-thread-id",
            "thread-stale-previous-generation-cancel-task",
            "--summary",
            "stale previous generation owner cancel",
            "--",
            "/bin/sh",
            "-c",
            "sleep 30; printf done > \"$1\"",
            "cbth-task",
            &marker_arg,
        ],
    );
    let task_id = started["task"]["task_id"].as_str().expect("task id");
    wait_for_task_status(&home, task_id, "running");

    daemon.kill().expect("kill generation daemon");
    let _ = daemon.wait().expect("wait generation daemon");

    fs::create_dir_all(&previous_generation_dir).expect("create previous generation dir");
    fs::set_permissions(&previous_generation_dir, fs::Permissions::from_mode(0o700))
        .expect("chmod previous generation dir");
    let previous_listener =
        UnixListener::bind(&previous_generation_socket_path).expect("bind previous generation");
    fs::set_permissions(
        &previous_generation_socket_path,
        fs::Permissions::from_mode(0o600),
    )
    .expect("chmod previous generation socket");

    let conn = Connection::open(home.path().join("cbth.sqlite3")).expect("open db");
    let pid = conn
        .query_row(
            "SELECT pid FROM tasks WHERE task_id = ?",
            params![task_id],
            |row| row.get::<_, i64>(0),
        )
        .expect("read task pid") as u32;
    conn.execute(
        "UPDATE jobs
         SET supervisor_daemon_generation = '0.1.4'
         WHERE job_id = (SELECT job_id FROM tasks WHERE task_id = ?)",
        params![task_id],
    )
    .expect("retag stale previous generation job");
    conn.execute(
        "UPDATE tasks
         SET supervisor_daemon_generation = '0.1.4'
         WHERE task_id = ?",
        params![task_id],
    )
    .expect("retag stale previous generation task");
    drop(conn);

    let mut replacement = spawn_daemon(&home, "30", &["--socket-kind", "generation"]);
    wait_for_path(&generation_socket_path);
    assert_eq!(
        wait_for_task_status(&home, task_id, "running")["task"]["status"],
        "running"
    );
    let maintenance_blocker =
        UnixStream::connect(&generation_socket_path).expect("hold generation daemon client");
    thread::sleep(Duration::from_millis(100));

    drop(previous_listener);
    fs::remove_file(&previous_generation_socket_path).expect("remove previous generation socket");

    let cancelled = cbth_daemon(&home, &["task", "cancel", "--task-id", task_id]);
    assert_eq!(cancelled["task"]["status"], "lost");
    assert_eq!(
        cancelled["task"]["failure_reason"],
        "task supervisor lost after daemon restart"
    );
    wait_for_process_group_gone(pid);
    assert!(
        !marker.exists(),
        "stale previous generation task process survived reused-daemon cancel recovery"
    );
    drop(maintenance_blocker);

    stop_daemon_at_socket_path(&generation_socket_path);
    wait_for_socket_path_removed(&generation_socket_path, Duration::from_secs(10));
    replacement
        .wait()
        .expect("replacement generation daemon exits");
}

#[test]
fn daemon_sweeps_batches_due_within_idle_window_before_exit() {
    let home = temp_home();
    let mut child = spawn_daemon(&home, "3", &[]);

    let ping = wait_for_ping(&home);
    assert_eq!(ping["message"], "pong");
    let submitted = cbth(
        &home,
        &[
            "job",
            "submit",
            "--source-thread-id",
            "thread-near-term-batch",
            "--summary",
            "near term notification",
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
            "ready for caller",
            "--redelivery-window-seconds",
            "1",
        ],
    );
    let batch_id = failed["batch"]["batch"]["batch_id"]
        .as_str()
        .expect("batch id");

    let deadline = Instant::now() + Duration::from_secs(8);
    loop {
        if let Some(status) = child.try_wait().expect("check daemon status") {
            assert!(status.success());
            break;
        }
        assert!(
            Instant::now() < deadline,
            "daemon did not exit after sweeping near-term batch"
        );
        thread::sleep(Duration::from_millis(100));
    }
    let output = child.wait_with_output().expect("daemon output");
    let shutdown: Value = serde_json::from_slice(&output.stdout).expect("daemon shutdown json");
    assert_eq!(shutdown["shutdown_reason"], "idle_timeout");

    let inspected = cbth(&home, &["batch", "inspect", "--batch-id", batch_id]);
    assert_eq!(inspected["batch"]["batch"]["state"], "closed");
    assert_eq!(
        inspected["batch"]["batch"]["close_reason"],
        "redelivery_window_exhausted"
    );
    wait_for_socket_removed(&home);
}

#[test]
fn daemon_exits_when_open_batch_is_due_after_current_idle_window() {
    let home = temp_home();
    let mut child = spawn_daemon(&home, "2", &[]);

    let ping = wait_for_ping(&home);
    assert_eq!(ping["message"], "pong");
    let submitted = cbth(
        &home,
        &[
            "job",
            "submit",
            "--source-thread-id",
            "thread-future-batch",
            "--summary",
            "future notification",
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
            "ready later",
            "--redelivery-window-seconds",
            "10",
        ],
    );
    let batch_id = failed["batch"]["batch"]["batch_id"]
        .as_str()
        .expect("batch id");

    let deadline = Instant::now() + Duration::from_secs(6);
    loop {
        if let Some(status) = child.try_wait().expect("check daemon status") {
            assert!(status.success());
            break;
        }
        assert!(
            Instant::now() < deadline,
            "daemon did not exit before future batch became due"
        );
        thread::sleep(Duration::from_millis(100));
    }
    let output = child.wait_with_output().expect("daemon output");
    let shutdown: Value = serde_json::from_slice(&output.stdout).expect("daemon shutdown json");
    assert_eq!(shutdown["shutdown_reason"], "idle_timeout");

    let inspected = cbth(&home, &["batch", "inspect", "--batch-id", batch_id]);
    assert_eq!(inspected["batch"]["batch"]["state"], "open");
    wait_for_socket_removed(&home);
}

#[test]
fn daemon_keeps_alive_for_active_cli_observation_then_expires_it() {
    let home = temp_home();
    let submitted = cbth(
        &home,
        &[
            "job",
            "submit",
            "--source-thread-id",
            "thread-active-cli-observation",
            "--summary",
            "wait for accepted CLI turn",
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
            "ready for CLI",
            "--redelivery-window-seconds",
            "60",
        ],
    );
    let batch_id = failed["batch"]["batch"]["batch_id"]
        .as_str()
        .expect("batch id");
    let managed_session_id = bind_idle_cli_session(&home, "thread-active-cli-observation");
    let pending = cbth(
        &home,
        &[
            "attempt",
            "begin-cli-accept",
            "--batch-id",
            batch_id,
            "--managed-session-id",
            &managed_session_id,
            "--session-epoch",
            "1",
            "--rpc-kind",
            "turn-start",
            "--rpc-request-id",
            "rpc-request-daemon-observed",
        ],
    );
    let attempt_id = pending["attempt"]["attempt_id"]
        .as_str()
        .expect("attempt id");
    cbth(
        &home,
        &[
            "attempt",
            "accept-cli",
            "--attempt-id",
            attempt_id,
            "--delivery-turn-id",
            "turn-daemon-observed",
            "--observation-window-seconds",
            "5",
        ],
    );

    let mut child = spawn_daemon(&home, "1", &[]);
    let ping = wait_for_ping(&home);
    assert_eq!(ping["message"], "pong");

    thread::sleep(Duration::from_secs(2));
    assert!(
        child.try_wait().expect("check daemon status").is_none(),
        "daemon exited while a CLI observation was still active"
    );

    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if let Some(status) = child.try_wait().expect("check daemon status") {
            assert!(status.success());
            break;
        }
        assert!(
            Instant::now() < deadline,
            "daemon did not exit after expiring the CLI observation"
        );
        thread::sleep(Duration::from_millis(100));
    }
    let output = child.wait_with_output().expect("daemon output");
    let shutdown: Value = serde_json::from_slice(&output.stdout).expect("daemon shutdown json");
    assert_eq!(shutdown["shutdown_reason"], "idle_timeout");

    let attempt = cbth(&home, &["attempt", "inspect", "--attempt-id", attempt_id]);
    assert_eq!(attempt["attempt"]["state"], "abandoned");
    assert_eq!(attempt["attempt"]["delivery_observation_state"], "expired");
    let batch = cbth(&home, &["batch", "inspect", "--batch-id", batch_id]);
    assert_eq!(
        batch["batch"]["batch"]["replay_policy"],
        "manual_resolution_only"
    );
    wait_for_socket_removed(&home);
}

#[test]
fn daemon_task_run_success_creates_completed_job_and_log_artifact() {
    let home = temp_home();
    let started = cbth_daemon(
        &home,
        &[
            "task",
            "run",
            "--source-thread-id",
            "thread-task-success",
            "--summary",
            "successful task",
            "--",
            "/bin/sh",
            "-c",
            "printf 'hello stdout'; printf 'hello stderr' >&2",
        ],
    );
    let task_id = started["task"]["task_id"].as_str().expect("task id");
    let job_id = started["task"]["job_id"].as_str().expect("job id");

    let task = wait_for_task_status(&home, task_id, "succeeded");
    assert_eq!(task["task"]["job_id"], job_id);
    assert_eq!(task["task"]["exit_code"], 0);
    assert_eq!(task["task"]["stdout_truncated"], false);
    assert_eq!(task["task"]["stderr_truncated"], false);

    let job = cbth(&home, &["job", "inspect", "--job-id", job_id]);
    assert_eq!(job["job"]["status"], "completed");
    assert!(job["job"]["result_artifact_id"].as_str().is_some());

    let head = cbth(
        &home,
        &[
            "batch",
            "inspect-head",
            "--source-thread-id",
            "thread-task-success",
        ],
    );
    assert_eq!(head["batch"]["batch"]["state"], "open");
    assert_eq!(head["batch"]["batch"]["requires_artifact_read"], true);
    cbth(&home, &["daemon", "stop"]);
    wait_for_socket_removed(&home);
}

#[test]
fn daemon_task_run_nonzero_fails_job_with_log_artifact() {
    let home = temp_home();
    let started = cbth_daemon(
        &home,
        &[
            "task",
            "run",
            "--source-thread-id",
            "thread-task-fail",
            "--summary",
            "failing task",
            "--",
            "/bin/sh",
            "-c",
            "printf 'failure details'; exit 7",
        ],
    );
    let task_id = started["task"]["task_id"].as_str().expect("task id");
    let job_id = started["task"]["job_id"].as_str().expect("job id");

    let task = wait_for_task_status(&home, task_id, "failed");
    assert_eq!(task["task"]["exit_code"], 7);
    assert!(
        task["task"]["failure_reason"]
            .as_str()
            .expect("failure reason")
            .contains("status 7")
    );

    let job = cbth(&home, &["job", "inspect", "--job-id", job_id]);
    assert_eq!(job["job"]["status"], "failed");
    assert!(job["job"]["result_artifact_id"].as_str().is_some());
    cbth(&home, &["daemon", "stop"]);
    wait_for_socket_removed(&home);
}

#[test]
fn daemon_task_cancel_terminates_process_group_and_fails_job() {
    let home = temp_home();
    let started = cbth_daemon(
        &home,
        &[
            "task",
            "run",
            "--source-thread-id",
            "thread-task-cancel",
            "--summary",
            "cancel task",
            "--",
            "/bin/sh",
            "-c",
            "sleep 30",
        ],
    );
    let task_id = started["task"]["task_id"].as_str().expect("task id");
    let job_id = started["task"]["job_id"].as_str().expect("job id");
    cbth_daemon(&home, &["task", "cancel", "--task-id", task_id]);

    let task = wait_for_task_status(&home, task_id, "cancelled");
    assert_eq!(task["task"]["failure_reason"], "task cancelled");
    let job = cbth(&home, &["job", "inspect", "--job-id", job_id]);
    assert_eq!(job["job"]["status"], "failed");
    cbth(&home, &["daemon", "stop"]);
    wait_for_socket_removed(&home);
}

#[test]
fn daemon_task_cancel_wins_when_sigterm_trap_exits_zero() {
    let home = temp_home();
    let started = cbth_daemon(
        &home,
        &[
            "task",
            "run",
            "--source-thread-id",
            "thread-task-cancel-trap",
            "--summary",
            "cancel trap task",
            "--",
            "/bin/sh",
            "-c",
            "trap 'exit 0' TERM; while true; do sleep 1; done",
        ],
    );
    let task_id = started["task"]["task_id"].as_str().expect("task id");
    let job_id = started["task"]["job_id"].as_str().expect("job id");
    wait_for_task_status(&home, task_id, "running");

    cbth_daemon(&home, &["task", "cancel", "--task-id", task_id]);

    let task = wait_for_task_status(&home, task_id, "cancelled");
    assert_eq!(task["task"]["failure_reason"], "task cancelled");
    assert_eq!(task["task"]["exit_code"], 0);
    let job = cbth(&home, &["job", "inspect", "--job-id", job_id]);
    assert_eq!(job["job"]["status"], "failed");
    cbth(&home, &["daemon", "stop"]);
    wait_for_socket_removed(&home);
}

#[test]
fn daemon_task_cancel_after_leader_exit_terminates_live_process_group() {
    let home = temp_home();
    let pid_file = home.path().join("leader-exit-cancel.pid");
    let leader_done = home.path().join("leader-exit-cancel.done");
    let pid_file_arg = pid_file.to_string_lossy().to_string();
    let leader_done_arg = leader_done.to_string_lossy().to_string();
    let started = cbth_daemon(
        &home,
        &[
            "task",
            "run",
            "--source-thread-id",
            "thread-task-cancel-after-leader-exit",
            "--summary",
            "cancel after leader exit",
            "--",
            "/bin/sh",
            "-c",
            "printf '%s\n' \"$$\" > \"$1\"; (sleep 30) & printf done > \"$2\"; exit 0",
            "cbth-task",
            &pid_file_arg,
            &leader_done_arg,
        ],
    );
    let task_id = started["task"]["task_id"].as_str().expect("task id");
    let job_id = started["task"]["job_id"].as_str().expect("job id");
    wait_for_path(&pid_file);
    wait_for_nonempty_file(&pid_file);
    wait_for_path(&leader_done);
    let pid = fs::read_to_string(&pid_file)
        .expect("read pid file")
        .trim()
        .parse::<u32>()
        .expect("task pid");
    assert!(
        process_group_exists(pid),
        "background child should keep process group alive"
    );

    cbth_daemon(&home, &["task", "cancel", "--task-id", task_id]);

    let task = wait_for_task_status(&home, task_id, "cancelled");
    assert_eq!(task["task"]["failure_reason"], "task cancelled");
    let job = cbth(&home, &["job", "inspect", "--job-id", job_id]);
    assert_eq!(job["job"]["status"], "failed");
    wait_for_process_group_gone(pid);
    cbth(&home, &["daemon", "stop"]);
    wait_for_socket_removed(&home);
}

#[test]
fn daemon_task_cancel_persists_running_cancel_before_signaling() {
    let home = temp_home();
    let marker = home.path().join("cancel-after-store-marker");
    let marker_arg = marker.to_string_lossy().to_string();
    let started = cbth_daemon(
        &home,
        &[
            "task",
            "run",
            "--source-thread-id",
            "thread-task-cancel-durable-first",
            "--summary",
            "durable cancel before signal",
            "--",
            "/bin/sh",
            "-c",
            "trap 'printf term > \"$1\"; exit 0' TERM; while true; do sleep 1; done",
            "cbth-task",
            &marker_arg,
        ],
    );
    let task_id = started["task"]["task_id"].as_str().expect("task id");
    wait_for_task_status(&home, task_id, "running");

    let conn = hold_exclusive_db_lock(&home);

    let cancel = Command::new(env!("CARGO_BIN_EXE_cbth"))
        .arg("--home")
        .arg(home.path())
        .args(["task", "cancel", "--task-id", task_id])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn cancel");

    let deadline = Instant::now() + Duration::from_secs(2);
    while !marker.exists() && Instant::now() < deadline {
        thread::sleep(Duration::from_millis(50));
    }
    assert!(
        !marker.exists(),
        "running task was signaled before cancel intent was durable"
    );

    drop(conn);
    let output = cancel.wait_with_output().expect("wait cancel");
    assert!(
        output.status.success(),
        "cancel failed\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let task = wait_for_task_status(&home, task_id, "cancelled");
    assert_eq!(task["task"]["failure_reason"], "task cancelled");
    assert!(
        marker.exists(),
        "running task was not signaled after cancel intent became durable"
    );
    cbth_daemon(&home, &["daemon", "stop"]);
    wait_for_socket_removed(&home);
}

#[test]
fn daemon_task_timeout_terminates_process_group_and_fails_job() {
    let home = temp_home();
    let started = cbth_daemon(
        &home,
        &[
            "task",
            "run",
            "--source-thread-id",
            "thread-task-timeout",
            "--summary",
            "timeout task",
            "--timeout-seconds",
            "1",
            "--",
            "/bin/sh",
            "-c",
            "sleep 30",
        ],
    );
    let task_id = started["task"]["task_id"].as_str().expect("task id");
    let job_id = started["task"]["job_id"].as_str().expect("job id");

    let task = wait_for_task_status(&home, task_id, "timed_out");
    assert_eq!(task["task"]["failure_reason"], "task timed out");
    let job = cbth(&home, &["job", "inspect", "--job-id", job_id]);
    assert_eq!(job["job"]["status"], "failed");
    cbth(&home, &["daemon", "stop"]);
    wait_for_socket_removed(&home);
}

#[test]
fn daemon_task_timeout_wins_over_later_cancel() {
    let home = temp_home();
    let started = cbth_daemon(
        &home,
        &[
            "task",
            "run",
            "--source-thread-id",
            "thread-task-timeout-then-cancel",
            "--summary",
            "timeout then cancel task",
            "--timeout-seconds",
            "1",
            "--",
            "/bin/sh",
            "-c",
            "trap '' TERM; while :; do sleep 1; done",
        ],
    );
    let task_id = started["task"]["task_id"].as_str().expect("task id");
    let job_id = started["task"]["job_id"].as_str().expect("job id");

    thread::sleep(Duration::from_secs(2));
    cbth_daemon(&home, &["task", "cancel", "--task-id", task_id]);

    let task = wait_for_task_status(&home, task_id, "timed_out");
    assert_eq!(task["task"]["failure_reason"], "task timed out");
    let job = cbth(&home, &["job", "inspect", "--job-id", job_id]);
    assert_eq!(job["job"]["status"], "failed");
    cbth_daemon(&home, &["daemon", "stop"]);
    wait_for_socket_removed(&home);
}

#[test]
fn daemon_stop_cancels_active_supervised_task() {
    let home = temp_home();
    let marker = home.path().join("daemon-stop-orphan-marker");
    let marker_arg = marker.to_string_lossy().to_string();
    let started = cbth_daemon(
        &home,
        &[
            "task",
            "run",
            "--source-thread-id",
            "thread-task-daemon-stop",
            "--summary",
            "daemon stop task",
            "--",
            "/bin/sh",
            "-c",
            "sleep 2; printf done > \"$1\"",
            "cbth-task",
            &marker_arg,
        ],
    );
    let task_id = started["task"]["task_id"].as_str().expect("task id");
    let job_id = started["task"]["job_id"].as_str().expect("job id");
    wait_for_task_status(&home, task_id, "running");

    cbth_daemon(&home, &["daemon", "stop"]);
    wait_for_socket_removed(&home);

    let task = wait_for_task_status(&home, task_id, "cancelled");
    assert_eq!(task["task"]["failure_reason"], "task cancelled");
    let job = cbth(&home, &["job", "inspect", "--job-id", job_id]);
    assert_eq!(job["job"]["status"], "failed");
    thread::sleep(Duration::from_secs(3));
    assert!(!marker.exists(), "supervised child escaped daemon stop");
}

#[test]
fn daemon_stop_terminalizes_term_ignoring_task_before_exit() {
    let home = temp_home();
    let pid_file = home.path().join("daemon-stop-term-ignoring-task.pid");
    let pid_file_arg = pid_file.to_string_lossy().to_string();
    let started = cbth_daemon(
        &home,
        &[
            "task",
            "run",
            "--source-thread-id",
            "thread-task-daemon-stop-term-ignoring",
            "--summary",
            "daemon stop term ignoring task",
            "--",
            "/bin/sh",
            "-c",
            "printf '%s\n' \"$$\" > \"$1\"; trap '' TERM; while :; do sleep 1; done",
            "cbth-task",
            &pid_file_arg,
        ],
    );
    let task_id = started["task"]["task_id"].as_str().expect("task id");
    let job_id = started["task"]["job_id"].as_str().expect("job id");
    wait_for_task_status(&home, task_id, "running");
    wait_for_path(&pid_file);
    wait_for_nonempty_file(&pid_file);
    let pid = fs::read_to_string(&pid_file)
        .expect("read pid file")
        .trim()
        .parse::<u32>()
        .expect("task pid");

    cbth_daemon(&home, &["daemon", "stop"]);
    wait_for_socket_removed_with_timeout(&home, Duration::from_secs(10));

    let task = wait_for_task_status(&home, task_id, "cancelled");
    assert_eq!(task["task"]["failure_reason"], "task cancelled");
    let job = cbth(&home, &["job", "inspect", "--job-id", job_id]);
    assert_eq!(job["job"]["status"], "failed");
    assert!(
        !process_group_exists(pid),
        "TERM-ignoring supervised process group survived daemon stop"
    );
}

#[test]
fn daemon_stop_waits_for_locked_cancel_store_before_killing_task() {
    let home = temp_home();
    let pid_file = home.path().join("daemon-stop-locked-task.pid");
    let pid_file_arg = pid_file.to_string_lossy().to_string();
    let started = cbth_daemon(
        &home,
        &[
            "task",
            "run",
            "--source-thread-id",
            "thread-task-daemon-stop-locked",
            "--summary",
            "daemon stop locked task",
            "--",
            "/bin/sh",
            "-c",
            "printf '%s\n' \"$$\" > \"$1\"; trap '' TERM; while :; do sleep 1; done",
            "cbth-task",
            &pid_file_arg,
        ],
    );
    let task_id = started["task"]["task_id"].as_str().expect("task id");
    wait_for_task_status(&home, task_id, "running");
    wait_for_path(&pid_file);
    wait_for_nonempty_file(&pid_file);
    let pid = fs::read_to_string(&pid_file)
        .expect("read pid file")
        .trim()
        .parse::<u32>()
        .expect("task pid");

    let conn = hold_exclusive_db_lock(&home);
    cbth_daemon(&home, &["daemon", "stop"]);
    thread::sleep(Duration::from_secs(2));
    assert!(
        process_group_exists(pid),
        "daemon stop should not kill before shutdown cancel is durable"
    );
    drop(conn);
    wait_for_socket_removed_with_timeout(&home, Duration::from_secs(30));

    assert!(
        !process_group_exists(pid),
        "TERM-ignoring supervised process group survived daemon stop"
    );
}

#[test]
fn daemon_stop_waits_for_durable_cancel_before_signaling_blocked_worker() {
    let home = temp_home();
    let pid_file = home.path().join("daemon-stop-blocked-cancel.pid");
    let pid_file_arg = pid_file.to_string_lossy().to_string();
    let started = cbth_daemon(
        &home,
        &[
            "task",
            "run",
            "--source-thread-id",
            "thread-task-daemon-stop-blocked-cancel",
            "--summary",
            "daemon stop with blocked cancel",
            "--",
            "/bin/sh",
            "-c",
            "printf '%s\n' \"$$\" > \"$1\"; trap '' TERM; while :; do sleep 1; done",
            "cbth-task",
            &pid_file_arg,
        ],
    );
    let task_id = started["task"]["task_id"].as_str().expect("task id");
    wait_for_task_status(&home, task_id, "running");
    wait_for_path(&pid_file);
    wait_for_nonempty_file(&pid_file);
    let pid = fs::read_to_string(&pid_file)
        .expect("read pid file")
        .trim()
        .parse::<u32>()
        .expect("task pid");

    let conn = hold_exclusive_db_lock(&home);
    let (cancel_tx, cancel_rx) = mpsc::channel();
    let cancel_socket_path = home.path().join("run").join("cbth.sock");
    let cancel_task_id = task_id.to_owned();
    thread::spawn(move || {
        let result = (|| -> io::Result<Vec<u8>> {
            let mut stream = UnixStream::connect(cancel_socket_path)?;
            let request = json!({
                "command": "dispatch",
                "payload": {
                    "argv": [
                        b"task".to_vec(),
                        b"cancel".to_vec(),
                        b"--task-id".to_vec(),
                        cancel_task_id.into_bytes(),
                    ],
                },
            });
            stream.write_all(request.to_string().as_bytes())?;
            stream.write_all(b"\n")?;
            stream.shutdown(std::net::Shutdown::Write)?;
            let mut response = Vec::new();
            stream.read_to_end(&mut response)?;
            Ok(response)
        })()
        .map_err(|error| (error.kind(), error.to_string()));
        let _ = cancel_tx.send(result);
    });
    thread::sleep(Duration::from_millis(300));
    match cancel_rx.try_recv() {
        Ok(result) => {
            panic!("cancel worker completed before held database lock released: {result:?}")
        }
        Err(mpsc::TryRecvError::Empty) => {}
        Err(mpsc::TryRecvError::Disconnected) => {
            panic!("cancel worker exited without reporting a result")
        }
    }

    cbth_daemon(&home, &["daemon", "stop"]);
    thread::sleep(Duration::from_secs(2));
    assert!(
        process_group_exists(pid),
        "shutdown should not kill the supervised process before durable cancel succeeds"
    );
    drop(conn);
    wait_for_socket_removed_with_timeout(&home, Duration::from_secs(30));
    assert!(
        !process_group_exists(pid),
        "shutdown should kill the supervised process after durable cancel succeeds"
    );
    match cancel_rx.recv_timeout(Duration::from_secs(2)) {
        Ok(Ok(_response)) => {}
        Ok(Err((kind, message))) if is_peer_disconnect_kind(kind) => {
            let _ = message;
        }
        Ok(Err((kind, message))) => {
            panic!(
                "blocked cancel client failed unexpectedly after daemon shutdown: {kind:?}: {message}"
            )
        }
        Err(mpsc::RecvTimeoutError::Timeout) => {
            panic!("blocked cancel client did not exit after daemon shutdown")
        }
        Err(mpsc::RecvTimeoutError::Disconnected) => {
            panic!("blocked cancel client exited without reporting a result")
        }
    }
}

#[test]
fn daemon_task_run_rejects_above_supervisor_limit_without_creating_job() {
    let home = temp_home();
    let mut task_ids = Vec::new();
    for index in 0..16 {
        let summary = format!("limit task {index}");
        let started = cbth_daemon(
            &home,
            &[
                "task",
                "run",
                "--source-thread-id",
                "thread-task-limit",
                "--summary",
                &summary,
                "--",
                "/bin/sh",
                "-c",
                "sleep 30",
            ],
        );
        task_ids.push(
            started["task"]["task_id"]
                .as_str()
                .expect("task id")
                .to_owned(),
        );
    }
    for task_id in &task_ids {
        wait_for_task_status(&home, task_id, "running");
    }

    let stderr = cbth_daemon_failure(
        &home,
        &[
            "task",
            "run",
            "--source-thread-id",
            "thread-task-limit",
            "--summary",
            "limit overflow task",
            "--",
            "/bin/sh",
            "-c",
            "sleep 30",
        ],
    );

    assert!(stderr.contains("maximum supervised task limit reached (16)"));
    let jobs = cbth(
        &home,
        &[
            "job",
            "list",
            "--source-thread-id",
            "thread-task-limit",
            "--status",
            "pending",
            "--limit",
            "100",
        ],
    );
    assert_eq!(jobs["jobs"].as_array().expect("jobs").len(), 16);
    cbth_daemon(&home, &["daemon", "stop"]);
    wait_for_socket_removed(&home);
}

#[test]
fn daemon_task_run_rejects_redelivery_window_overflow_before_creating_job() {
    let home = temp_home();
    let stderr = cbth_daemon_failure(
        &home,
        &[
            "task",
            "run",
            "--source-thread-id",
            "thread-task-overflow",
            "--summary",
            "overflow task",
            "--redelivery-window-seconds",
            "9223372036854775807",
            "--",
            "/bin/sh",
            "-c",
            "true",
        ],
    );

    assert!(stderr.contains("redelivery_window_seconds overflows timestamp range"));
    let jobs = cbth(
        &home,
        &[
            "job",
            "list",
            "--source-thread-id",
            "thread-task-overflow",
            "--limit",
            "100",
        ],
    );
    assert_eq!(jobs["jobs"].as_array().expect("jobs").len(), 0);
    cbth_daemon(&home, &["daemon", "stop"]);
    wait_for_socket_removed(&home);
}

#[cfg(unix)]
#[test]
fn daemon_task_run_resolves_bare_command_with_caller_path() {
    let home = temp_home();
    let bin_dir = home.path().join("caller-bin");
    fs::create_dir(&bin_dir).expect("create caller bin");
    let tool_path = bin_dir.join("cbth-caller-path-tool");
    fs::write(&tool_path, "#!/bin/sh\nprintf caller-path-ok\n").expect("write tool");
    fs::set_permissions(&tool_path, fs::Permissions::from_mode(0o755)).expect("chmod tool");

    let ensure = Command::new(env!("CARGO_BIN_EXE_cbth"))
        .env("PATH", "/usr/bin:/bin")
        .arg("--home")
        .arg(home.path())
        .args([
            "daemon",
            "ensure",
            "--idle-timeout-seconds",
            "30",
            "--startup-timeout-seconds",
            "5",
        ])
        .output()
        .expect("start daemon");
    assert!(
        ensure.status.success(),
        "daemon ensure failed\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&ensure.stdout),
        String::from_utf8_lossy(&ensure.stderr)
    );

    let client_path = format!("{}:/usr/bin:/bin", bin_dir.display());
    let started_output = Command::new(env!("CARGO_BIN_EXE_cbth"))
        .env("PATH", client_path)
        .arg("--home")
        .arg(home.path())
        .args([
            "task",
            "run",
            "--source-thread-id",
            "thread-task-caller-path",
            "--summary",
            "caller path task",
            "--",
            "cbth-caller-path-tool",
        ])
        .output()
        .expect("run task");
    assert!(
        started_output.status.success(),
        "task run failed\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&started_output.stdout),
        String::from_utf8_lossy(&started_output.stderr)
    );
    let started: Value = serde_json::from_slice(&started_output.stdout).expect("task json");
    let task_id = started["task"]["task_id"].as_str().expect("task id");
    let task = wait_for_task_status(&home, task_id, "succeeded");
    let stdout_log_path = task["task"]["stdout_log_path"]
        .as_str()
        .expect("stdout log path");
    let stdout = fs::read_to_string(home.path().join(stdout_log_path)).expect("stdout log");
    assert_eq!(stdout, "caller-path-ok");
    cbth_daemon(&home, &["daemon", "stop"]);
    wait_for_socket_removed(&home);
}

#[cfg(unix)]
#[test]
fn daemon_task_run_resolves_relative_path_entries_against_task_cwd() {
    let home = temp_home();
    let caller_cwd = home.path().join("caller-cwd");
    let caller_bin = caller_cwd.join("bin");
    let task_cwd = home.path().join("task-cwd");
    let task_bin = task_cwd.join("bin");
    fs::create_dir_all(&caller_bin).expect("create caller bin");
    fs::create_dir_all(&task_bin).expect("create task bin");
    let caller_tool = caller_bin.join("cbth-relative-path-tool");
    let task_tool = task_bin.join("cbth-relative-path-tool");
    fs::write(&caller_tool, "#!/bin/sh\nprintf wrong-cwd\n").expect("write caller tool");
    fs::write(&task_tool, "#!/bin/sh\nprintf task-cwd-ok\n").expect("write task tool");
    fs::set_permissions(&caller_tool, fs::Permissions::from_mode(0o755))
        .expect("chmod caller tool");
    fs::set_permissions(&task_tool, fs::Permissions::from_mode(0o755)).expect("chmod task tool");

    let ensure = Command::new(env!("CARGO_BIN_EXE_cbth"))
        .env("PATH", "/usr/bin:/bin")
        .arg("--home")
        .arg(home.path())
        .args([
            "daemon",
            "ensure",
            "--idle-timeout-seconds",
            "30",
            "--startup-timeout-seconds",
            "5",
        ])
        .output()
        .expect("start daemon");
    assert!(
        ensure.status.success(),
        "daemon ensure failed\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&ensure.stdout),
        String::from_utf8_lossy(&ensure.stderr)
    );

    let started_output = Command::new(env!("CARGO_BIN_EXE_cbth"))
        .current_dir(&caller_cwd)
        .env("PATH", "bin:/usr/bin:/bin")
        .arg("--home")
        .arg(home.path())
        .args([
            "task",
            "run",
            "--source-thread-id",
            "thread-task-relative-path",
            "--summary",
            "relative path task",
            "--cwd",
        ])
        .arg(&task_cwd)
        .args(["--", "cbth-relative-path-tool"])
        .output()
        .expect("run task");
    assert!(
        started_output.status.success(),
        "task run failed\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&started_output.stdout),
        String::from_utf8_lossy(&started_output.stderr)
    );
    let started: Value = serde_json::from_slice(&started_output.stdout).expect("task json");
    let task_id = started["task"]["task_id"].as_str().expect("task id");
    let task = wait_for_task_status(&home, task_id, "succeeded");
    let stdout_log_path = task["task"]["stdout_log_path"]
        .as_str()
        .expect("stdout log path");
    let stdout = fs::read_to_string(home.path().join(stdout_log_path)).expect("stdout log");
    assert_eq!(stdout, "task-cwd-ok");
    cbth_daemon(&home, &["daemon", "stop"]);
    wait_for_socket_removed(&home);
}

#[cfg(unix)]
#[test]
fn daemon_task_run_accepts_non_utf8_cwd() {
    let home = temp_home();
    let invalid_name = std::ffi::OsString::from_vec(b"invalid-\xff".to_vec());
    let invalid_cwd = home.path().join(invalid_name);
    if let Err(error) = fs::create_dir(&invalid_cwd) {
        if error.raw_os_error() == Some(libc::EILSEQ) {
            return;
        }
        panic!("create non-UTF-8 cwd: {error}");
    }
    let marker = home.path().join("non-utf8-cwd-marker");

    let output = Command::new(env!("CARGO_BIN_EXE_cbth"))
        .arg("--home")
        .arg(home.path())
        .args([
            "task",
            "run",
            "--source-thread-id",
            "thread-task-invalid-cwd",
            "--summary",
            "invalid cwd task",
            "--cwd",
        ])
        .arg(&invalid_cwd)
        .args(["--", "/bin/sh", "-c", "printf ok > \"$1\"", "cbth-task"])
        .arg(&marker)
        .output()
        .expect("run task");

    assert!(
        output.status.success(),
        "task run failed\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let started: Value = serde_json::from_slice(&output.stdout).expect("task json");
    let task_id = started["task"]["task_id"].as_str().expect("task id");
    wait_for_task_status(&home, task_id, "succeeded");
    assert_eq!(fs::read_to_string(marker).expect("marker"), "ok");
    cbth_daemon(&home, &["daemon", "stop"]);
    wait_for_socket_removed(&home);
}

#[test]
fn daemon_task_run_uses_caller_environment_with_existing_daemon() {
    let home = temp_home();
    let ensure = Command::new(env!("CARGO_BIN_EXE_cbth"))
        .env_remove("CBTH_TASK_ENV_PROBE")
        .arg("--home")
        .arg(home.path())
        .args([
            "daemon",
            "ensure",
            "--idle-timeout-seconds",
            "30",
            "--startup-timeout-seconds",
            "5",
        ])
        .output()
        .expect("start daemon");
    assert!(
        ensure.status.success(),
        "daemon ensure failed\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&ensure.stdout),
        String::from_utf8_lossy(&ensure.stderr)
    );

    let started_output = Command::new(env!("CARGO_BIN_EXE_cbth"))
        .env("CBTH_TASK_ENV_PROBE", "caller-env-ok")
        .arg("--home")
        .arg(home.path())
        .args([
            "task",
            "run",
            "--source-thread-id",
            "thread-task-caller-env",
            "--summary",
            "caller env task",
            "--",
            "/bin/sh",
            "-c",
            "printf '%s' \"$CBTH_TASK_ENV_PROBE\"",
        ])
        .output()
        .expect("run task");
    assert!(
        started_output.status.success(),
        "task run failed\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&started_output.stdout),
        String::from_utf8_lossy(&started_output.stderr)
    );
    let started: Value = serde_json::from_slice(&started_output.stdout).expect("task json");
    let task_id = started["task"]["task_id"].as_str().expect("task id");
    let task = wait_for_task_status(&home, task_id, "succeeded");
    let stdout_log_path = task["task"]["stdout_log_path"]
        .as_str()
        .expect("stdout log path");
    let stdout = fs::read_to_string(home.path().join(stdout_log_path)).expect("stdout log");
    assert_eq!(stdout, "caller-env-ok");
    cbth_daemon(&home, &["daemon", "stop"]);
    wait_for_socket_removed(&home);
}

#[test]
fn daemon_task_run_rewrites_pwd_to_task_cwd() {
    let home = temp_home();
    let caller_cwd = home.path().join("caller-pwd");
    let task_cwd = home.path().join("task-pwd");
    fs::create_dir(&caller_cwd).expect("create caller cwd");
    fs::create_dir(&task_cwd).expect("create task cwd");
    let ensure = Command::new(env!("CARGO_BIN_EXE_cbth"))
        .arg("--home")
        .arg(home.path())
        .args([
            "daemon",
            "ensure",
            "--idle-timeout-seconds",
            "30",
            "--startup-timeout-seconds",
            "5",
        ])
        .output()
        .expect("start daemon");
    assert!(
        ensure.status.success(),
        "daemon ensure failed\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&ensure.stdout),
        String::from_utf8_lossy(&ensure.stderr)
    );

    let started_output = Command::new(env!("CARGO_BIN_EXE_cbth"))
        .current_dir(&caller_cwd)
        .env("PWD", &caller_cwd)
        .arg("--home")
        .arg(home.path())
        .args([
            "task",
            "run",
            "--source-thread-id",
            "thread-task-pwd",
            "--summary",
            "pwd task",
            "--cwd",
        ])
        .arg(&task_cwd)
        .args(["--", "/bin/sh", "-c", "printf '%s' \"$PWD\""])
        .output()
        .expect("run task");
    assert!(
        started_output.status.success(),
        "task run failed\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&started_output.stdout),
        String::from_utf8_lossy(&started_output.stderr)
    );
    let started: Value = serde_json::from_slice(&started_output.stdout).expect("task json");
    let task_id = started["task"]["task_id"].as_str().expect("task id");
    let task = wait_for_task_status(&home, task_id, "succeeded");
    let stdout_log_path = task["task"]["stdout_log_path"]
        .as_str()
        .expect("stdout log path");
    let stdout = fs::read_to_string(home.path().join(stdout_log_path)).expect("stdout log");
    assert_eq!(stdout, task_cwd.to_string_lossy());
    cbth_daemon(&home, &["daemon", "stop"]);
    wait_for_socket_removed(&home);
}

#[test]
fn daemon_task_run_does_not_leak_exec_gate_fd() {
    let home = temp_home();
    let started = cbth_daemon(
        &home,
        &[
            "task",
            "run",
            "--source-thread-id",
            "thread-task-gate-fd",
            "--summary",
            "gate fd task",
            "--",
            "/bin/sh",
            "-c",
            "if /bin/sh -c 'true <&3' 2>/dev/null; then printf leaked; else printf closed; fi",
        ],
    );
    let task_id = started["task"]["task_id"].as_str().expect("task id");
    let task = wait_for_task_status(&home, task_id, "succeeded");
    let stdout_log_path = task["task"]["stdout_log_path"]
        .as_str()
        .expect("stdout log path");
    let stdout = fs::read_to_string(home.path().join(stdout_log_path)).expect("stdout log");
    assert_eq!(stdout, "closed");
    cbth_daemon(&home, &["daemon", "stop"]);
    wait_for_socket_removed(&home);
}

#[test]
fn daemon_startup_recovery_terminates_lost_task_process_group() {
    let home = temp_home();
    let marker = home.path().join("lost-task-marker");
    let marker_arg = marker.to_string_lossy().to_string();
    let mut daemon = spawn_daemon(&home, "300", &[]);
    wait_for_ping(&home);
    let started = cbth_daemon(
        &home,
        &[
            "task",
            "run",
            "--source-thread-id",
            "thread-task-lost-pgid",
            "--summary",
            "lost pgid task",
            "--",
            "/bin/sh",
            "-c",
            "sleep 3; printf done > \"$1\"",
            "cbth-task",
            &marker_arg,
        ],
    );
    let task_id = started["task"]["task_id"].as_str().expect("task id");
    wait_for_task_status(&home, task_id, "running");

    daemon.kill().expect("kill daemon");
    let _ = daemon.wait().expect("wait daemon");
    cbth_daemon(
        &home,
        &[
            "daemon",
            "ensure",
            "--idle-timeout-seconds",
            "30",
            "--startup-timeout-seconds",
            "5",
        ],
    );

    let task = wait_for_task_status(&home, task_id, "lost");
    assert_eq!(
        task["task"]["failure_reason"],
        "task supervisor lost after daemon restart"
    );
    thread::sleep(Duration::from_secs(4));
    assert!(
        !marker.exists(),
        "lost supervised process group survived daemon startup recovery"
    );
    cbth_daemon(&home, &["daemon", "stop"]);
    wait_for_socket_removed(&home);
}

#[test]
fn daemon_startup_recovery_terminates_lost_task_process_group_for_closed_job() {
    let home = temp_home();
    let marker = home.path().join("lost-task-closed-job-marker");
    let marker_arg = marker.to_string_lossy().to_string();
    let mut daemon = spawn_daemon(&home, "300", &[]);
    wait_for_ping(&home);
    let started = cbth_daemon(
        &home,
        &[
            "task",
            "run",
            "--source-thread-id",
            "thread-task-lost-closed-job-pgid",
            "--summary",
            "lost pgid task whose job was closed",
            "--",
            "/bin/sh",
            "-c",
            "sleep 3; printf done > \"$1\"",
            "cbth-task",
            &marker_arg,
        ],
    );
    let task_id = started["task"]["task_id"].as_str().expect("task id");
    let job_id = started["task"]["job_id"].as_str().expect("job id");
    wait_for_task_status(&home, task_id, "running");
    cbth(
        &home,
        &[
            "job",
            "fail",
            "--job-id",
            job_id,
            "--reason",
            "external job failure before daemon restart",
        ],
    );

    daemon.kill().expect("kill daemon");
    let _ = daemon.wait().expect("wait daemon");
    cbth_daemon(
        &home,
        &[
            "daemon",
            "ensure",
            "--idle-timeout-seconds",
            "30",
            "--startup-timeout-seconds",
            "5",
        ],
    );

    let task = wait_for_task_status(&home, task_id, "failed");
    assert_eq!(
        task["task"]["failure_reason"],
        "external job failure before daemon restart"
    );
    thread::sleep(Duration::from_secs(4));
    assert!(
        !marker.exists(),
        "lost process group for externally closed job survived daemon startup recovery"
    );
    cbth_daemon(&home, &["daemon", "stop"]);
    wait_for_socket_removed(&home);
}

#[test]
fn maintenance_sweep_autostart_recovers_lost_task_process_group() {
    let home = temp_home();
    let marker = home.path().join("lost-task-maintenance-marker");
    let marker_arg = marker.to_string_lossy().to_string();
    let mut daemon = spawn_daemon(&home, "300", &[]);
    wait_for_ping(&home);
    let started = cbth_daemon(
        &home,
        &[
            "task",
            "run",
            "--source-thread-id",
            "thread-task-lost-maintenance-pgid",
            "--summary",
            "lost pgid task before maintenance sweep",
            "--",
            "/bin/sh",
            "-c",
            "sleep 3; printf done > \"$1\"",
            "cbth-task",
            &marker_arg,
        ],
    );
    let task_id = started["task"]["task_id"].as_str().expect("task id");
    wait_for_task_status(&home, task_id, "running");

    daemon.kill().expect("kill daemon");
    let _ = daemon.wait().expect("wait daemon");
    cbth_daemon(&home, &["maintenance", "sweep"]);

    let task = wait_for_task_status(&home, task_id, "lost");
    assert_eq!(
        task["task"]["failure_reason"],
        "task supervisor lost after daemon restart"
    );
    thread::sleep(Duration::from_secs(4));
    assert!(
        !marker.exists(),
        "lost supervised process group survived maintenance autostart recovery"
    );
    cbth_daemon(&home, &["daemon", "stop"]);
    wait_for_socket_removed(&home);
}

#[test]
fn duplicate_daemon_serve_does_not_recover_tasks_before_socket_exclusivity() {
    let home = temp_home();
    cbth_daemon(
        &home,
        &[
            "daemon",
            "ensure",
            "--idle-timeout-seconds",
            "30",
            "--startup-timeout-seconds",
            "5",
        ],
    );
    let started = cbth_daemon(
        &home,
        &[
            "task",
            "run",
            "--source-thread-id",
            "thread-task-duplicate-daemon",
            "--summary",
            "duplicate daemon task",
            "--",
            "/bin/sh",
            "-c",
            "sleep 30",
        ],
    );
    let task_id = started["task"]["task_id"].as_str().expect("task id");
    wait_for_task_status(&home, task_id, "running");

    let duplicate = Command::new(env!("CARGO_BIN_EXE_cbth"))
        .arg("--home")
        .arg(home.path())
        .arg("daemon")
        .arg("serve")
        .arg("--idle-timeout-seconds")
        .arg("30")
        .arg("--now")
        .arg("100")
        .output()
        .expect("run duplicate daemon");

    assert!(
        !duplicate.status.success(),
        "duplicate daemon unexpectedly succeeded"
    );
    let stderr = String::from_utf8_lossy(&duplicate.stderr);
    assert!(
        stderr.contains("daemon socket is already active"),
        "unexpected duplicate daemon stderr: {stderr}"
    );
    let task = cbth(&home, &["task", "inspect", "--task-id", task_id]);
    assert_eq!(task["task"]["status"], "running");

    cbth_daemon(&home, &["task", "cancel", "--task-id", task_id]);
    wait_for_task_status(&home, task_id, "cancelled");
    cbth_daemon(&home, &["daemon", "stop"]);
    wait_for_socket_removed(&home);
}

#[test]
fn maintenance_sweep_removes_expired_task_log_dirs() {
    let home = temp_home();
    let started = cbth_daemon(
        &home,
        &[
            "task",
            "run",
            "--source-thread-id",
            "thread-task-log-retention",
            "--summary",
            "log retention task",
            "--",
            "/bin/sh",
            "-c",
            "printf task-log-retention",
        ],
    );
    let task_id = started["task"]["task_id"].as_str().expect("task id");
    let task = wait_for_task_status(&home, task_id, "succeeded");
    let completed_at = task["task"]["completed_at"].as_i64().expect("completed_at");
    let stdout_log_path = task["task"]["stdout_log_path"]
        .as_str()
        .expect("stdout log path");
    let task_dir = home.path().join("tasks").join(task_id);
    assert!(home.path().join(stdout_log_path).exists());

    let batch_close_now = completed_at + 72 * 60 * 60 + 1;
    let batch_close_now_arg = batch_close_now.to_string();
    let report = cbth(
        &home,
        &["maintenance", "sweep", "--now", &batch_close_now_arg],
    );

    assert_eq!(report["sweep"]["expired_automatic_batches_closed"], 1);
    assert_eq!(report["sweep"]["task_log_dirs_deleted"], 0);
    assert!(task_dir.exists());

    let log_delete_now = batch_close_now + 72 * 60 * 60 + 1;
    let log_delete_now_arg = log_delete_now.to_string();
    let report = cbth(
        &home,
        &["maintenance", "sweep", "--now", &log_delete_now_arg],
    );

    assert_eq!(report["sweep"]["task_log_dirs_deleted"], 1);
    assert!(!task_dir.exists());
    let inspected = cbth(&home, &["task", "inspect", "--task-id", task_id]);
    assert!(inspected["task"]["stdout_log_path"].is_null());
    assert!(inspected["task"]["stderr_log_path"].is_null());
    cbth_daemon(&home, &["daemon", "stop"]);
    wait_for_socket_removed(&home);
}

#[test]
fn daemon_task_run_rejects_transport_oversized_environment_before_daemon_start() {
    let home = temp_home();
    let mut command = Command::new(env!("CARGO_BIN_EXE_cbth"));
    command.env_clear();
    for index in 0..150 {
        command.env(format!("CBTH_HUGE_ENV_{index}"), "x".repeat(4096));
    }
    let output = command
        .arg("--home")
        .arg(home.path())
        .args([
            "task",
            "run",
            "--source-thread-id",
            "thread-task-env-too-large",
            "--summary",
            "env too large",
            "--",
            "/usr/bin/true",
        ])
        .output()
        .expect("run task with oversized env");

    assert!(
        !output.status.success(),
        "task run unexpectedly succeeded\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("task_run request exceeds daemon IPC budget"),
        "missing transport budget error: {stderr}"
    );
    assert!(
        !home.path().join("run").join("cbth.sock").exists(),
        "oversized task run started daemon before transport preflight"
    );
}

#[test]
fn daemon_task_timeout_works_after_direct_child_exits_but_pipe_is_held() {
    let home = temp_home();
    let started = cbth_daemon(
        &home,
        &[
            "task",
            "run",
            "--source-thread-id",
            "thread-task-held-pipe-timeout",
            "--summary",
            "held pipe timeout task",
            "--timeout-seconds",
            "1",
            "--",
            "/bin/sh",
            "-c",
            "printf started; exec 3>&1; trap '' HUP; (sleep 30; printf late >&3) &",
        ],
    );
    let task_id = started["task"]["task_id"].as_str().expect("task id");
    let job_id = started["task"]["job_id"].as_str().expect("job id");

    let task = wait_for_task_status(&home, task_id, "timed_out");
    assert_eq!(task["task"]["failure_reason"], "task timed out");
    let job = cbth(&home, &["job", "inspect", "--job-id", job_id]);
    assert_eq!(job["job"]["status"], "failed");
    cbth(&home, &["daemon", "stop"]);
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
