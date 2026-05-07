use std::fs;
#[cfg(unix)]
use std::io::{Read, Write};
#[cfg(unix)]
use std::os::unix::net::UnixListener;
use std::process::{Command, Output};
#[cfg(unix)]
use std::thread;

use serde_json::{Value, json};
use tempfile::TempDir;

#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

fn temp_home() -> TempDir {
    let home = tempfile::tempdir().expect("temp home");
    #[cfg(unix)]
    fs::set_permissions(home.path(), fs::Permissions::from_mode(0o700)).expect("chmod temp home");
    home
}

fn cbth(home: &TempDir, args: &[&str]) -> Value {
    let output = cbth_output(home, args, true);
    assert!(
        output.status.success(),
        "cbth failed\nstatus: {}\nstdout: {}\nstderr: {}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).expect("valid json output")
}

fn cbth_daemon(home: &TempDir, args: &[&str]) -> Value {
    let output = cbth_output(home, args, false);
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
    let output = cbth_output(home, args, true);
    assert!(
        !output.status.success(),
        "cbth unexpectedly succeeded\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8_lossy(&output.stderr).into_owned()
}

fn cbth_daemon_failure(home: &TempDir, args: &[&str]) -> String {
    let output = cbth_output(home, args, false);
    assert!(
        !output.status.success(),
        "cbth unexpectedly succeeded\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8_lossy(&output.stderr).into_owned()
}

fn cbth_output(home: &TempDir, args: &[&str], direct_store: bool) -> Output {
    let mut command = Command::new(env!("CARGO_BIN_EXE_cbth"));
    if direct_store {
        command.env("CBTH_ALLOW_DIRECT_STORE", "1");
        command.arg("--direct-store");
    }
    command.arg("--home").arg(home.path()).args(args);
    command.output().expect("run cbth")
}

fn stop_daemon(home: &TempDir) {
    let _ = Command::new(env!("CARGO_BIN_EXE_cbth"))
        .arg("--home")
        .arg(home.path())
        .arg("daemon")
        .arg("stop")
        .output();
}

#[test]
fn desktop_installation_state_defaults_and_repairs_without_extra_writes() {
    let home = temp_home();

    let initial = cbth(&home, &["desktop", "installation-state", "--json"]);
    let state = &initial["desktop_installation_state"];
    assert_eq!(state["read_transport"], "direct_file_read");
    assert_eq!(state["read_transport_generation"], 0);
    assert_eq!(state["read_transport_capability"], "unknown");
    assert_eq!(state["artifact_read_capability"], "unknown");
    assert_eq!(state["writeback_capability"], "unknown");
    assert_eq!(state["validated_at"], Value::Null);

    let repaired = cbth(
        &home,
        &[
            "desktop",
            "installation-state",
            "repair",
            "--read-transport",
            "direct-file-read",
            "--read-transport-capability",
            "validated",
            "--validation-fingerprint",
            "fingerprint-a",
            "--json",
            "--now",
            "1000",
        ],
    );
    let repair = &repaired["desktop_installation_state"];
    assert_eq!(repair["changed"], true);
    assert_eq!(repair["degraded_bindings"], 0);
    assert_eq!(repair["state"]["read_transport_generation"], 1);
    assert_eq!(repair["state"]["read_transport_capability"], "validated");
    assert_eq!(repair["state"]["artifact_read_capability"], "unknown");
    assert_eq!(repair["state"]["writeback_capability"], "unknown");
    assert_eq!(repair["state"]["validation_fingerprint"], "fingerprint-a");
    assert_eq!(repair["state"]["validated_at"], 1000);

    let no_op = cbth(
        &home,
        &[
            "desktop",
            "installation-state",
            "repair",
            "--read-transport",
            "direct-file-read",
            "--read-transport-capability",
            "validated",
            "--validation-fingerprint",
            "fingerprint-a",
            "--json",
            "--now",
            "1001",
        ],
    );
    assert_eq!(no_op["desktop_installation_state"]["changed"], false);
    assert_eq!(
        no_op["desktop_installation_state"]["state"]["read_transport_generation"],
        1
    );
    assert_eq!(
        no_op["desktop_installation_state"]["state"]["updated_at"],
        1000
    );
}

#[test]
fn desktop_binding_repair_mirrors_installation_and_degrades_on_drift() {
    let home = temp_home();
    cbth(
        &home,
        &[
            "desktop",
            "installation-state",
            "repair",
            "--read-transport",
            "direct-file-read",
            "--read-transport-capability",
            "validated",
            "--validation-fingerprint",
            "fingerprint-a",
            "--json",
            "--now",
            "1000",
        ],
    );

    let binding = cbth(
        &home,
        &[
            "desktop",
            "binding",
            "repair",
            "--source-thread-id",
            "thread-desktop",
            "--caller-automation-id",
            "automation-1",
            "--json",
            "--now",
            "1001",
        ],
    );
    let binding = &binding["desktop_binding"]["binding"];
    assert_eq!(binding["source_thread_id"], "thread-desktop");
    assert_eq!(binding["caller_automation_id"], "automation-1");
    assert_eq!(binding["binding_state"], "bound");
    assert_eq!(binding["read_transport_generation"], 1);
    assert_eq!(binding["validation_fingerprint"], "fingerprint-a");
    assert_eq!(binding["degraded_at"], Value::Null);

    let drift = cbth(
        &home,
        &[
            "desktop",
            "installation-state",
            "repair",
            "--read-transport",
            "direct-file-read",
            "--read-transport-capability",
            "validated",
            "--validation-fingerprint",
            "fingerprint-b",
            "--json",
            "--now",
            "1002",
        ],
    );
    assert_eq!(drift["desktop_installation_state"]["changed"], true);
    assert_eq!(drift["desktop_installation_state"]["degraded_bindings"], 1);
    assert_eq!(
        drift["desktop_installation_state"]["state"]["read_transport_generation"],
        2
    );

    let repaired = cbth(
        &home,
        &[
            "desktop",
            "binding",
            "repair",
            "--source-thread-id",
            "thread-desktop",
            "--caller-automation-id",
            "automation-1",
            "--json",
            "--now",
            "1003",
        ],
    );
    let repaired = &repaired["desktop_binding"]["binding"];
    assert_eq!(repaired["binding_state"], "bound");
    assert_eq!(repaired["read_transport_generation"], 2);
    assert_eq!(repaired["validation_fingerprint"], "fingerprint-b");
    assert_eq!(repaired["degraded_at"], Value::Null);

    let duplicate_automation = cbth_failure(
        &home,
        &[
            "desktop",
            "binding",
            "repair",
            "--source-thread-id",
            "other-thread",
            "--caller-automation-id",
            "automation-1",
            "--json",
            "--now",
            "1004",
        ],
    );
    assert!(duplicate_automation.contains("already bound to source_thread_id thread-desktop"));
}

#[test]
fn desktop_commands_fail_closed_for_invalid_inputs() {
    let home = temp_home();

    let invalid_transport = cbth_failure(
        &home,
        &[
            "desktop",
            "installation-state",
            "repair",
            "--read-transport",
            "helper-cli-read",
            "--json",
        ],
    );
    assert!(invalid_transport.contains("invalid value"));

    let invalid_capability = cbth_failure(
        &home,
        &[
            "desktop",
            "installation-state",
            "repair",
            "--read-transport",
            "direct-file-read",
            "--writeback-capability",
            "trusted",
            "--json",
        ],
    );
    assert!(invalid_capability.contains("invalid value"));

    let empty_binding = cbth_failure(
        &home,
        &[
            "desktop",
            "binding",
            "repair",
            "--source-thread-id",
            "",
            "--caller-automation-id",
            "automation-1",
            "--json",
        ],
    );
    assert!(empty_binding.contains("source_thread_id must not be empty"));
}

#[cfg(unix)]
#[test]
fn desktop_bridge_preflight_publishes_revision_consistent_private_snapshots() {
    let home = temp_home();

    let preflight = cbth(
        &home,
        &[
            "desktop",
            "bridge-preflight",
            "--bridge-thread-id",
            "bridge-thread",
            "--json",
            "--now",
            "2000",
        ],
    );
    let preflight = &preflight["desktop_bridge_preflight"];
    let revision = preflight["snapshot_revision"]
        .as_str()
        .expect("snapshot revision");
    assert!(!revision.is_empty());
    assert_eq!(preflight["schema_version"], 1);
    assert_eq!(preflight["bridge_thread_id"], "bridge-thread");
    assert_eq!(preflight["snapshots"]["ready_threads"]["count"], 0);
    assert_eq!(preflight["snapshots"]["arm_pending_bindings"]["count"], 0);
    assert_eq!(preflight["snapshots"]["pause_due_bindings"]["count"], 0);
    let installation_state_path = preflight["installation_state_path"]
        .as_str()
        .expect("installation state path")
        .to_owned();
    assert_eq!(
        installation_state_path,
        home.path()
            .join("inbox")
            .join("desktop-installation-state.json")
            .display()
            .to_string()
    );
    let ready_path = preflight["snapshots"]["ready_threads"]["path"]
        .as_str()
        .expect("ready path")
        .to_owned();
    assert!(
        ready_path.contains(&format!("/snapshots/{revision}/")),
        "ready snapshot path should be revision-specific: {ready_path}"
    );
    let revision_dir = home.path().join("inbox").join("snapshots").join(revision);
    let revision_dir_mode = fs::metadata(&revision_dir)
        .expect("stat snapshot revision dir")
        .permissions()
        .mode()
        & 0o777;
    assert_eq!(revision_dir_mode, 0o700);

    let inbox_dir = home.path().join("inbox");
    let inbox_mode = fs::metadata(&inbox_dir)
        .expect("stat inbox")
        .permissions()
        .mode()
        & 0o777;
    assert_eq!(inbox_mode, 0o700);

    for key in [
        "snapshot_manifest_path",
        "snapshots.ready_threads.path",
        "snapshots.arm_pending_bindings.path",
        "snapshots.pause_due_bindings.path",
    ] {
        let path = match key {
            "snapshot_manifest_path" => preflight[key].as_str().expect(key),
            "snapshots.ready_threads.path" => preflight["snapshots"]["ready_threads"]["path"]
                .as_str()
                .expect(key),
            "snapshots.arm_pending_bindings.path" => preflight["snapshots"]["arm_pending_bindings"]
                ["path"]
                .as_str()
                .expect(key),
            "snapshots.pause_due_bindings.path" => {
                preflight["snapshots"]["pause_due_bindings"]["path"]
                    .as_str()
                    .expect(key)
            }
            _ => unreachable!(),
        };
        let metadata = fs::metadata(path).unwrap_or_else(|error| panic!("stat {path}: {error}"));
        assert_eq!(metadata.permissions().mode() & 0o777, 0o600);
        let value: Value = serde_json::from_slice(
            &fs::read(path).unwrap_or_else(|error| panic!("read {path}: {error}")),
        )
        .unwrap_or_else(|error| panic!("parse {path}: {error}"));
        assert_eq!(value["snapshot_revision"], revision);
        assert_eq!(value["schema_version"], 1);
        assert_eq!(value["bridge_thread_id"], "bridge-thread");
    }

    let second = cbth(
        &home,
        &[
            "desktop",
            "bridge-preflight",
            "--bridge-thread-id",
            "bridge-thread",
            "--json",
            "--now",
            "2001",
        ],
    );
    let second = &second["desktop_bridge_preflight"];
    let second_revision = second["snapshot_revision"]
        .as_str()
        .expect("second snapshot revision");
    assert_ne!(second_revision, revision);
    assert_ne!(
        second["snapshots"]["ready_threads"]["path"]
            .as_str()
            .expect("second ready path"),
        ready_path
    );
    let old_ready: Value = serde_json::from_slice(
        &fs::read(&ready_path).unwrap_or_else(|error| panic!("read {ready_path}: {error}")),
    )
    .unwrap_or_else(|error| panic!("parse {ready_path}: {error}"));
    assert_eq!(old_ready["snapshot_revision"], revision);
    let installation_state: Value = serde_json::from_slice(
        &fs::read(&installation_state_path)
            .unwrap_or_else(|error| panic!("read {installation_state_path}: {error}")),
    )
    .unwrap_or_else(|error| panic!("parse {installation_state_path}: {error}"));
    assert_eq!(installation_state["schema_version"], 1);
    assert_eq!(installation_state["published_at"], 2001);
    assert_eq!(installation_state["bridge_thread_id"], "bridge-thread");
    assert_eq!(
        installation_state["desktop_installation_state"]["read_transport_generation"],
        0
    );
    assert_eq!(
        installation_state["desktop_installation_state"]["read_transport_capability"],
        "unknown"
    );
    let durable_state = cbth(&home, &["desktop", "installation-state", "--json"]);
    assert_eq!(
        durable_state["desktop_installation_state"]["read_transport_generation"],
        0
    );
    assert_eq!(durable_state["desktop_installation_state"]["updated_at"], 0);
}

#[test]
fn desktop_bridge_preflight_routes_through_daemon() {
    let home = temp_home();
    let preflight = cbth_daemon(
        &home,
        &[
            "desktop",
            "bridge-preflight",
            "--bridge-thread-id",
            "bridge-thread-daemon",
            "--json",
        ],
    );
    stop_daemon(&home);

    assert_eq!(
        preflight["desktop_bridge_preflight"]["bridge_thread_id"],
        "bridge-thread-daemon"
    );
    assert_eq!(
        preflight["desktop_bridge_preflight"]["snapshots"]["ready_threads"]["count"],
        0
    );
}

#[test]
fn desktop_bridge_preflight_helper_direct_store_bypasses_daemon() {
    let home = temp_home();
    let first = cbth_daemon(
        &home,
        &[
            "desktop",
            "bridge-preflight",
            "--bridge-thread-id",
            "bridge-thread-helper",
            "--helper-direct-store",
            "--json",
            "--now",
            "2200",
        ],
    );
    let first = &first["desktop_bridge_preflight"];
    assert_eq!(first["schema_version"], 1);
    assert_eq!(first["bridge_thread_id"], "bridge-thread-helper");
    assert_eq!(first["snapshots"]["ready_threads"]["count"], 0);
    assert_eq!(first["snapshots"]["arm_pending_bindings"]["count"], 0);
    assert_eq!(first["snapshots"]["pause_due_bindings"]["count"], 0);
    assert!(
        !home.path().join("run").join("startup.lock").exists(),
        "helper direct-store preflight must not autostart the daemon"
    );

    let revision = first["snapshot_revision"]
        .as_str()
        .expect("snapshot revision")
        .to_owned();
    let manifest_path = first["snapshot_manifest_path"]
        .as_str()
        .expect("manifest path")
        .to_owned();
    let ready_path = first["snapshots"]["ready_threads"]["path"]
        .as_str()
        .expect("ready path")
        .to_owned();
    assert!(
        ready_path.contains(&format!("/snapshots/{revision}/")),
        "ready snapshot path should be revision-specific: {ready_path}"
    );
    let manifest: Value = serde_json::from_slice(
        &fs::read(&manifest_path).unwrap_or_else(|error| panic!("read {manifest_path}: {error}")),
    )
    .unwrap_or_else(|error| panic!("parse {manifest_path}: {error}"));
    assert_eq!(manifest["snapshot_revision"], revision);
    assert_eq!(manifest["bridge_thread_id"], "bridge-thread-helper");

    let second = cbth_daemon(
        &home,
        &[
            "desktop",
            "bridge-preflight",
            "--bridge-thread-id",
            "bridge-thread-helper",
            "--helper-direct-store",
            "--json",
            "--now",
            "2201",
        ],
    );
    let second = &second["desktop_bridge_preflight"];
    assert_ne!(second["snapshot_revision"], revision);
    assert_eq!(second["created_at"], 2201);

    let durable_state = cbth(&home, &["desktop", "installation-state", "--json"]);
    assert_eq!(
        durable_state["desktop_installation_state"]["read_transport_generation"],
        0
    );
    assert_eq!(durable_state["desktop_installation_state"]["updated_at"], 0);
}

#[test]
fn desktop_bridge_preflight_helper_direct_store_rejects_existing_daemon_mode() {
    let home = temp_home();
    let stderr = cbth_daemon_failure(
        &home,
        &[
            "desktop",
            "bridge-preflight",
            "--bridge-thread-id",
            "bridge-thread-conflict",
            "--helper-direct-store",
            "--require-existing-daemon",
            "--json",
        ],
    );
    assert!(
        stderr.contains("--helper-direct-store cannot be combined with --require-existing-daemon"),
        "unexpected stderr: {stderr}"
    );
    assert!(
        !home.path().join("run").join("startup.lock").exists(),
        "conflicting helper modes must not autostart the daemon"
    );
}

#[test]
fn desktop_bridge_preflight_helper_direct_store_fails_without_daemon_fallback() {
    let home = temp_home();
    fs::create_dir(home.path().join("cbth.sqlite3")).expect("create directory at db path");

    let stderr = cbth_daemon_failure(
        &home,
        &[
            "desktop",
            "bridge-preflight",
            "--bridge-thread-id",
            "bridge-thread-store-failure",
            "--helper-direct-store",
            "--json",
        ],
    );
    assert!(
        stderr.contains("path exists but is not a regular file"),
        "unexpected stderr: {stderr}"
    );
    assert!(
        !home.path().join("run").join("startup.lock").exists(),
        "helper direct-store failure must not fall back to daemon autostart"
    );
}

#[test]
fn desktop_bridge_preflight_requires_existing_daemon_when_requested() {
    let home = temp_home();
    let preflight = cbth_daemon(
        &home,
        &[
            "daemon",
            "ensure",
            "--idle-timeout-seconds",
            "60",
            "--startup-timeout-seconds",
            "5",
        ],
    );
    assert_eq!(preflight["started"], true);

    let preflight = cbth_daemon(
        &home,
        &[
            "desktop",
            "bridge-preflight",
            "--bridge-thread-id",
            "bridge-thread-existing-daemon",
            "--require-existing-daemon",
            "--json",
        ],
    );
    stop_daemon(&home);

    assert_eq!(
        preflight["desktop_bridge_preflight"]["bridge_thread_id"],
        "bridge-thread-existing-daemon"
    );
    assert_eq!(
        preflight["desktop_bridge_preflight"]["snapshots"]["ready_threads"]["count"],
        0
    );
}

#[test]
fn desktop_bridge_preflight_require_existing_daemon_does_not_autostart() {
    let home = temp_home();
    let stderr = cbth_daemon_failure(
        &home,
        &[
            "desktop",
            "bridge-preflight",
            "--bridge-thread-id",
            "bridge-thread-no-daemon",
            "--require-existing-daemon",
            "--json",
        ],
    );
    assert!(
        stderr.contains("probe existing daemon"),
        "unexpected stderr: {stderr}"
    );
    assert!(
        !home.path().join("run").join("startup.lock").exists(),
        "require-existing-daemon must not create startup lock"
    );
}

#[test]
fn desktop_bridge_preflight_require_existing_daemon_rejects_direct_store() {
    let home = temp_home();
    let stderr = cbth_failure(
        &home,
        &[
            "desktop",
            "bridge-preflight",
            "--bridge-thread-id",
            "bridge-thread-direct-store",
            "--require-existing-daemon",
            "--json",
        ],
    );
    assert!(
        stderr.contains("--require-existing-daemon cannot be combined with --direct-store"),
        "unexpected stderr: {stderr}"
    );
}

#[cfg(unix)]
#[test]
fn desktop_bridge_preflight_require_existing_daemon_rejects_incompatible_daemon() {
    let home = temp_home();
    let run_dir = home.path().join("run");
    fs::create_dir(&run_dir).expect("create run dir");
    fs::set_permissions(&run_dir, fs::Permissions::from_mode(0o700)).expect("chmod run dir");
    let socket_path = run_dir.join("cbth.sock");
    let listener = UnixListener::bind(&socket_path).expect("bind fake daemon socket");
    fs::set_permissions(&socket_path, fs::Permissions::from_mode(0o600)).expect("chmod socket");
    let fake_socket_path = socket_path.clone();
    let handle = thread::spawn(move || {
        let (mut stream, _addr) = listener.accept().expect("accept fake daemon request");
        let mut request = String::new();
        stream
            .read_to_string(&mut request)
            .expect("read fake daemon request");
        assert!(request.contains("\"ping\""));
        stream
            .write_all(
                br#"{"ok":true,"response":{"daemon":{"pid":4242},"protocol_version":1,"capabilities":["dispatch"],"message":"pong"}}"#,
            )
            .expect("write fake daemon response");
        stream.write_all(b"\n").expect("write fake daemon newline");
        drop(listener);
        fs::remove_file(&fake_socket_path).expect("remove fake socket");
    });

    let stderr = cbth_daemon_failure(
        &home,
        &[
            "desktop",
            "bridge-preflight",
            "--bridge-thread-id",
            "bridge-thread-incompatible",
            "--require-existing-daemon",
            "--json",
        ],
    );
    handle.join().expect("fake daemon thread");

    assert!(
        stderr.contains("existing daemon is missing required capabilities"),
        "unexpected stderr: {stderr}"
    );
    assert!(
        !home.path().join("run").join("startup.lock").exists(),
        "incompatible existing daemon must not trigger startup lock"
    );
}

#[cfg(unix)]
#[test]
fn desktop_bridge_preflight_require_existing_daemon_does_not_forward_client_only_flag() {
    let home = temp_home();
    let run_dir = home.path().join("run");
    fs::create_dir(&run_dir).expect("create run dir");
    fs::set_permissions(&run_dir, fs::Permissions::from_mode(0o700)).expect("chmod run dir");
    let socket_path = run_dir.join("cbth.sock");
    let listener = UnixListener::bind(&socket_path).expect("bind fake daemon socket");
    fs::set_permissions(&socket_path, fs::Permissions::from_mode(0o600)).expect("chmod socket");
    let fake_socket_path = socket_path.clone();
    let handle = thread::spawn(move || {
        let ping_response = json!({
            "ok": true,
            "response": {
                "daemon": { "pid": 4242 },
                "protocol_version": 1,
                "capabilities": [
                    "dispatch",
                    "attempt-dispatch",
                    "cli-app-server-lifecycle",
                    "cli-app-server-probe",
                    "cli-thread-start-bootstrap",
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
                    "desktop-inbox-revisioned-installation-state"
                ],
                "message": "pong"
            }
        });
        let (mut ping_stream, _addr) = listener.accept().expect("accept fake ping");
        let mut ping_request = String::new();
        ping_stream
            .read_to_string(&mut ping_request)
            .expect("read fake ping");
        assert!(ping_request.contains("\"ping\""));
        ping_stream
            .write_all(serde_json::to_string(&ping_response).unwrap().as_bytes())
            .expect("write fake ping");
        ping_stream.write_all(b"\n").expect("write ping newline");
        drop(ping_stream);

        let (mut dispatch_stream, _addr) = listener.accept().expect("accept fake dispatch");
        let mut dispatch_request = String::new();
        dispatch_stream
            .read_to_string(&mut dispatch_request)
            .expect("read fake dispatch");
        let parsed: Value = serde_json::from_str(&dispatch_request).expect("dispatch json");
        assert_eq!(parsed["command"], "dispatch");
        let argv = parsed["payload"]["argv"]
            .as_array()
            .expect("argv array")
            .iter()
            .map(|arg| {
                let bytes = arg
                    .as_array()
                    .expect("arg bytes")
                    .iter()
                    .map(|byte| byte.as_u64().expect("byte") as u8)
                    .collect::<Vec<_>>();
                String::from_utf8(bytes).expect("utf8 argv")
            })
            .collect::<Vec<_>>();
        assert_eq!(argv[0], "desktop");
        assert_eq!(argv[1], "bridge-preflight");
        assert!(argv.contains(&"--json".to_owned()));
        assert!(
            !argv.contains(&"--require-existing-daemon".to_owned()),
            "client-only flag must not be forwarded to daemon: {argv:?}"
        );

        let dispatch_response = json!({
            "ok": true,
            "response": {
                "desktop_bridge_preflight": {
                    "bridge_thread_id": "bridge-thread-fake-compatible",
                    "snapshots": {
                        "ready_threads": { "count": 0 }
                    }
                }
            }
        });
        dispatch_stream
            .write_all(
                serde_json::to_string(&dispatch_response)
                    .unwrap()
                    .as_bytes(),
            )
            .expect("write fake dispatch");
        dispatch_stream
            .write_all(b"\n")
            .expect("write dispatch newline");
        drop(listener);
        fs::remove_file(&fake_socket_path).expect("remove fake socket");
    });

    let preflight = cbth_daemon(
        &home,
        &[
            "desktop",
            "bridge-preflight",
            "--bridge-thread-id",
            "bridge-thread-fake-compatible",
            "--require-existing-daemon",
            "--json",
        ],
    );
    handle.join().expect("fake daemon thread");

    assert_eq!(
        preflight["desktop_bridge_preflight"]["bridge_thread_id"],
        "bridge-thread-fake-compatible"
    );
}

#[cfg(unix)]
#[test]
fn desktop_bridge_preflight_exports_repaired_installation_state_for_direct_read() {
    let home = temp_home();

    cbth(
        &home,
        &[
            "desktop",
            "installation-state",
            "repair",
            "--read-transport",
            "direct-file-read",
            "--read-transport-capability",
            "validated",
            "--validation-fingerprint",
            "desktop-live-preflight",
            "--json",
            "--now",
            "2100",
        ],
    );

    let preflight = cbth(
        &home,
        &[
            "desktop",
            "bridge-preflight",
            "--bridge-thread-id",
            "bridge-thread-live",
            "--json",
            "--now",
            "2101",
        ],
    );
    let preflight = &preflight["desktop_bridge_preflight"];
    let installation_state_path = preflight["installation_state_path"]
        .as_str()
        .expect("installation state path");
    let metadata = fs::metadata(installation_state_path)
        .unwrap_or_else(|error| panic!("stat {installation_state_path}: {error}"));
    assert_eq!(metadata.permissions().mode() & 0o777, 0o600);

    let installation_state: Value = serde_json::from_slice(
        &fs::read(installation_state_path)
            .unwrap_or_else(|error| panic!("read {installation_state_path}: {error}")),
    )
    .unwrap_or_else(|error| panic!("parse {installation_state_path}: {error}"));
    assert_eq!(installation_state["schema_version"], 1);
    assert_eq!(installation_state["published_at"], 2101);
    assert_eq!(installation_state["bridge_thread_id"], "bridge-thread-live");
    assert_eq!(
        installation_state["desktop_installation_state"]["read_transport"],
        "direct_file_read"
    );
    assert_eq!(
        installation_state["desktop_installation_state"]["read_transport_generation"],
        1
    );
    assert_eq!(
        installation_state["desktop_installation_state"]["read_transport_capability"],
        "validated"
    );
    assert_eq!(
        installation_state["desktop_installation_state"]["artifact_read_capability"],
        "unknown"
    );
    assert_eq!(
        installation_state["desktop_installation_state"]["writeback_capability"],
        "unknown"
    );
    assert_eq!(
        installation_state["desktop_installation_state"]["validation_fingerprint"],
        "desktop-live-preflight"
    );
    assert_eq!(
        installation_state["desktop_installation_state"]["validated_at"],
        2100
    );
    assert_eq!(
        installation_state["desktop_installation_state"]["created_at"],
        2100
    );
    assert_eq!(
        installation_state["desktop_installation_state"]["updated_at"],
        2100
    );
}
