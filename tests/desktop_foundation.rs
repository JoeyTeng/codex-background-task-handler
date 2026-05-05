use std::fs;
use std::process::{Command, Output};

use serde_json::Value;
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
