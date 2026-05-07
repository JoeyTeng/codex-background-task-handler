use std::fs;
#[cfg(unix)]
use std::io::{Read, Write};
#[cfg(unix)]
use std::os::unix::net::UnixListener;
use std::process::{Command, Output};
#[cfg(unix)]
use std::thread;

use rusqlite::{Connection, params};
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

fn read_json_file(path: &str) -> Value {
    serde_json::from_slice(&fs::read(path).unwrap_or_else(|error| panic!("read {path}: {error}")))
        .unwrap_or_else(|error| panic!("parse {path}: {error}"))
}

fn write_json_file(path: &str, value: &Value) {
    let bytes = serde_json::to_vec_pretty(value).expect("serialize json");
    fs::write(path, bytes).unwrap_or_else(|error| panic!("write {path}: {error}"));
}

fn create_desktop_batch_and_prepared_attempt(
    home: &TempDir,
    source_thread_id: &str,
    attempt_id: &str,
    generation: i64,
    now: i64,
) -> String {
    let submitted = cbth(
        home,
        &[
            "job",
            "submit",
            "--source-thread-id",
            source_thread_id,
            "--summary",
            "desktop writeback fixture",
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
        home,
        &[
            "job",
            "fail",
            "--job-id",
            job_id,
            "--reason",
            "ready for Desktop writeback",
            "--max-delivery-attempts",
            "3",
            "--redelivery-window-seconds",
            "3600",
        ],
    );
    let batch_id = failed["batch"]["batch"]["batch_id"]
        .as_str()
        .expect("batch id")
        .to_owned();
    insert_desktop_prepared_attempt(
        home,
        source_thread_id,
        &batch_id,
        attempt_id,
        generation,
        now,
    );
    batch_id
}

fn insert_desktop_prepared_attempt(
    home: &TempDir,
    source_thread_id: &str,
    batch_id: &str,
    attempt_id: &str,
    generation: i64,
    now: i64,
) {
    let conn = Connection::open(home.path().join("cbth.sqlite3")).expect("open db");
    conn.execute(
        "INSERT INTO delivery_attempts (
            attempt_id, batch_id, source_thread_id, adapter_kind,
            authorization_mode, state, generation, created_at, updated_at
        ) VALUES (?, ?, ?, 'desktop', 'strict_safe', 'prepared', ?, ?, ?)",
        params![attempt_id, batch_id, source_thread_id, generation, now, now],
    )
    .expect("insert prepared Desktop attempt");
}

fn force_desktop_attempt_arm_pending(home: &TempDir, attempt_id: &str, now: i64) {
    let conn = Connection::open(home.path().join("cbth.sqlite3")).expect("open db");
    conn.execute(
        "UPDATE delivery_attempts
         SET state = 'arm_pending',
             bridge_request_id = ?,
             bridge_arm_lease_id = ?,
             bridge_arm_lease_deadline = ?,
             arm_pending_since = ?,
             arm_pending_deadline = ?,
             updated_at = ?
         WHERE attempt_id = ?",
        params![
            format!("bridge-request-{attempt_id}"),
            format!("lease-{attempt_id}"),
            now + 300,
            now,
            now + 300,
            now,
            attempt_id,
        ],
    )
    .expect("force Desktop arm_pending attempt");
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

#[test]
fn desktop_note_arm_pending_and_note_arm_are_idempotent_and_exported() {
    let home = temp_home();
    cbth(
        &home,
        &[
            "desktop",
            "binding",
            "repair",
            "--source-thread-id",
            "thread-desktop-writeback",
            "--caller-automation-id",
            "automation-writeback",
            "--json",
            "--now",
            "2000",
        ],
    );
    let batch_id = create_desktop_batch_and_prepared_attempt(
        &home,
        "thread-desktop-writeback",
        "attempt-desktop-writeback",
        1,
        2001,
    );

    let pending = cbth(
        &home,
        &[
            "desktop",
            "note-arm-pending",
            "--source-thread-id",
            "thread-desktop-writeback",
            "--attempt-id",
            "attempt-desktop-writeback",
            "--generation",
            "1",
            "--bridge-request-id",
            "bridge-request-1",
            "--json",
            "--now",
            "2100",
        ],
    );
    let pending = &pending["desktop_arm_pending"];
    assert_eq!(pending["outcome"], "arm_pending");
    assert_eq!(pending["attempt"]["state"], "arm_pending");
    assert_eq!(pending["attempt"]["bridge_request_id"], "bridge-request-1");
    let lease = pending["bridge_arm_lease_id"]
        .as_str()
        .expect("bridge arm lease")
        .to_owned();
    assert_eq!(pending["bridge_arm_lease_deadline"], 2400);
    assert_eq!(pending["arm_pending_deadline"], 2400);

    let repeated_pending = cbth(
        &home,
        &[
            "desktop",
            "note-arm-pending",
            "--source-thread-id",
            "thread-desktop-writeback",
            "--attempt-id",
            "attempt-desktop-writeback",
            "--generation",
            "1",
            "--bridge-request-id",
            "bridge-request-1",
            "--json",
            "--now",
            "2101",
        ],
    );
    assert_eq!(
        repeated_pending["desktop_arm_pending"]["outcome"],
        "already_pending"
    );
    assert_eq!(
        repeated_pending["desktop_arm_pending"]["bridge_arm_lease_id"],
        lease
    );

    let busy = cbth_failure(
        &home,
        &[
            "desktop",
            "note-arm-pending",
            "--source-thread-id",
            "thread-desktop-writeback",
            "--attempt-id",
            "attempt-desktop-writeback",
            "--generation",
            "1",
            "--bridge-request-id",
            "bridge-request-2",
            "--json",
            "--now",
            "2102",
        ],
    );
    assert!(busy.contains("already arm_pending for another bridge request"));

    let preflight = cbth(
        &home,
        &[
            "desktop",
            "bridge-preflight",
            "--helper-direct-store",
            "--bridge-thread-id",
            "bridge-thread",
            "--json",
            "--now",
            "2103",
        ],
    );
    assert_eq!(
        preflight["desktop_bridge_preflight"]["snapshots"]["arm_pending_bindings"]["count"],
        1
    );
    let arm_pending = cbth(
        &home,
        &[
            "desktop",
            "list-arm-pending",
            "--bridge-thread-id",
            "bridge-thread",
            "--json",
        ],
    );
    let pending_entries = arm_pending["desktop_arm_pending_bindings"]["entries"]
        .as_array()
        .expect("arm pending entries");
    assert_eq!(pending_entries.len(), 1);
    assert_eq!(pending_entries[0]["batch_id"], batch_id);
    assert_eq!(
        pending_entries[0]["attempt_id"],
        "attempt-desktop-writeback"
    );
    assert_eq!(pending_entries[0]["bridge_request_id"], "bridge-request-1");
    assert!(pending_entries[0].get("bridge_arm_lease_id").is_none());

    let wrong_lease = cbth_failure(
        &home,
        &[
            "desktop",
            "note-arm",
            "--source-thread-id",
            "thread-desktop-writeback",
            "--attempt-id",
            "attempt-desktop-writeback",
            "--generation",
            "1",
            "--bridge-request-id",
            "bridge-request-1",
            "--bridge-arm-lease-id",
            "wrong-lease",
            "--json",
            "--now",
            "2110",
        ],
    );
    assert!(wrong_lease.contains("bridge_arm_lease_id does not match"));

    let armed = cbth(
        &home,
        &[
            "desktop",
            "note-arm",
            "--source-thread-id",
            "thread-desktop-writeback",
            "--attempt-id",
            "attempt-desktop-writeback",
            "--generation",
            "1",
            "--bridge-request-id",
            "bridge-request-1",
            "--bridge-arm-lease-id",
            &lease,
            "--json",
            "--now",
            "2110",
        ],
    );
    let armed = &armed["desktop_arm"];
    assert_eq!(armed["outcome"], "armed");
    assert_eq!(armed["attempt"]["state"], "cooldown");
    assert_eq!(armed["delivery_attempt_count"], 1);
    assert_eq!(armed["binding"]["armed_generation"], 1);
    assert_eq!(armed["pause_not_before"], 2200);
    assert_eq!(armed["pause_deadline"], 2290);

    let repeated_arm = cbth(
        &home,
        &[
            "desktop",
            "note-arm",
            "--source-thread-id",
            "thread-desktop-writeback",
            "--attempt-id",
            "attempt-desktop-writeback",
            "--generation",
            "1",
            "--bridge-request-id",
            "bridge-request-1",
            "--bridge-arm-lease-id",
            &lease,
            "--json",
            "--now",
            "2111",
        ],
    );
    assert_eq!(repeated_arm["desktop_arm"]["outcome"], "already_armed");
    assert_eq!(repeated_arm["desktop_arm"]["delivery_attempt_count"], 1);
    let inspected = cbth(&home, &["batch", "inspect", "--batch-id", &batch_id]);
    assert_eq!(inspected["batch"]["batch"]["delivery_attempt_count"], 1);

    let due_preflight = cbth(
        &home,
        &[
            "desktop",
            "bridge-preflight",
            "--helper-direct-store",
            "--bridge-thread-id",
            "bridge-thread",
            "--json",
            "--now",
            "2291",
        ],
    );
    assert_eq!(
        due_preflight["desktop_bridge_preflight"]["snapshots"]["pause_due_bindings"]["count"],
        1
    );
    let pause_due = cbth(
        &home,
        &[
            "desktop",
            "list-pause-due",
            "--bridge-thread-id",
            "bridge-thread",
            "--json",
        ],
    );
    let pause_entries = pause_due["desktop_pause_due_bindings"]["entries"]
        .as_array()
        .expect("pause due entries");
    assert_eq!(pause_entries.len(), 1);
    assert_eq!(
        pause_entries[0]["source_thread_id"],
        "thread-desktop-writeback"
    );
    assert_eq!(pause_entries[0]["armed_generation"], 1);

    cbth(
        &home,
        &[
            "batch",
            "close-head",
            "--source-thread-id",
            "thread-desktop-writeback",
            "--reason",
            "operator-confirmed-delivery",
        ],
    );
    let repeated_arm_after_close = cbth(
        &home,
        &[
            "desktop",
            "note-arm",
            "--source-thread-id",
            "thread-desktop-writeback",
            "--attempt-id",
            "attempt-desktop-writeback",
            "--generation",
            "1",
            "--bridge-request-id",
            "bridge-request-1",
            "--bridge-arm-lease-id",
            &lease,
            "--json",
            "--now",
            "2292",
        ],
    );
    assert_eq!(
        repeated_arm_after_close["desktop_arm"]["outcome"],
        "already_armed"
    );
    assert_eq!(
        repeated_arm_after_close["desktop_arm"]["delivery_attempt_count"],
        1
    );
    let repeated_pending_after_close = cbth(
        &home,
        &[
            "desktop",
            "note-arm-pending",
            "--source-thread-id",
            "thread-desktop-writeback",
            "--attempt-id",
            "attempt-desktop-writeback",
            "--generation",
            "1",
            "--bridge-request-id",
            "bridge-request-1",
            "--json",
            "--now",
            "2293",
        ],
    );
    assert_eq!(
        repeated_pending_after_close["desktop_arm_pending"]["outcome"],
        "already_armed"
    );
    create_desktop_batch_and_prepared_attempt(
        &home,
        "thread-desktop-writeback",
        "attempt-desktop-writeback-next",
        1,
        2300,
    );
    let unquiesced_retry = cbth_failure(
        &home,
        &[
            "desktop",
            "note-arm-pending",
            "--source-thread-id",
            "thread-desktop-writeback",
            "--attempt-id",
            "attempt-desktop-writeback-next",
            "--generation",
            "1",
            "--bridge-request-id",
            "bridge-request-next",
            "--json",
            "--now",
            "2301",
        ],
    );
    assert!(unquiesced_retry.contains("still has unquiesced armed_generation 1"));
}

#[test]
fn desktop_bridge_preflight_exports_only_current_bound_eligible_arm_pending() {
    let degraded_home = temp_home();
    cbth(
        &degraded_home,
        &[
            "desktop",
            "binding",
            "repair",
            "--source-thread-id",
            "thread-degraded-export",
            "--caller-automation-id",
            "automation-degraded-export",
            "--json",
            "--now",
            "2400",
        ],
    );
    create_desktop_batch_and_prepared_attempt(
        &degraded_home,
        "thread-degraded-export",
        "attempt-degraded-export",
        1,
        2401,
    );
    force_desktop_attempt_arm_pending(&degraded_home, "attempt-degraded-export", 2402);
    let current = cbth(
        &degraded_home,
        &[
            "desktop",
            "bridge-preflight",
            "--helper-direct-store",
            "--bridge-thread-id",
            "bridge-thread",
            "--json",
            "--now",
            "2403",
        ],
    );
    assert_eq!(
        current["desktop_bridge_preflight"]["snapshots"]["arm_pending_bindings"]["count"],
        1
    );
    cbth(
        &degraded_home,
        &[
            "desktop",
            "installation-state",
            "repair",
            "--read-transport",
            "direct-file-read",
            "--validation-fingerprint",
            "drifted-fingerprint",
            "--json",
            "--now",
            "2404",
        ],
    );
    let degraded = cbth(
        &degraded_home,
        &[
            "desktop",
            "bridge-preflight",
            "--helper-direct-store",
            "--bridge-thread-id",
            "bridge-thread",
            "--json",
            "--now",
            "2405",
        ],
    );
    assert_eq!(
        degraded["desktop_bridge_preflight"]["snapshots"]["arm_pending_bindings"]["count"],
        0
    );
    let degraded_retry = cbth_failure(
        &degraded_home,
        &[
            "desktop",
            "note-arm-pending",
            "--source-thread-id",
            "thread-degraded-export",
            "--attempt-id",
            "attempt-degraded-export",
            "--generation",
            "1",
            "--bridge-request-id",
            "bridge-request-attempt-degraded-export",
            "--json",
            "--now",
            "2406",
        ],
    );
    assert!(degraded_retry.contains("Desktop binding thread-degraded-export is degraded"));

    let non_head_home = temp_home();
    cbth(
        &non_head_home,
        &[
            "desktop",
            "binding",
            "repair",
            "--source-thread-id",
            "thread-non-head-export",
            "--caller-automation-id",
            "automation-non-head-export",
            "--json",
            "--now",
            "2500",
        ],
    );
    create_desktop_batch_and_prepared_attempt(
        &non_head_home,
        "thread-non-head-export",
        "attempt-head-export",
        1,
        2501,
    );
    let later_submit = cbth(
        &non_head_home,
        &[
            "job",
            "submit",
            "--source-thread-id",
            "thread-non-head-export",
            "--summary",
            "later export batch",
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
    let later_job_id = later_submit["job"]["job_id"].as_str().expect("job id");
    let later_failed = cbth(
        &non_head_home,
        &[
            "job",
            "fail",
            "--job-id",
            later_job_id,
            "--reason",
            "later ready",
        ],
    );
    let later_batch = later_failed["batch"]["batch"]["batch_id"]
        .as_str()
        .expect("later batch");
    insert_desktop_prepared_attempt(
        &non_head_home,
        "thread-non-head-export",
        later_batch,
        "attempt-non-head-export",
        1,
        2502,
    );
    force_desktop_attempt_arm_pending(&non_head_home, "attempt-non-head-export", 2503);
    let non_head = cbth(
        &non_head_home,
        &[
            "desktop",
            "bridge-preflight",
            "--helper-direct-store",
            "--bridge-thread-id",
            "bridge-thread",
            "--json",
            "--now",
            "2504",
        ],
    );
    assert_eq!(
        non_head["desktop_bridge_preflight"]["snapshots"]["arm_pending_bindings"]["count"],
        0
    );

    let stale_generation_home = temp_home();
    cbth(
        &stale_generation_home,
        &[
            "desktop",
            "binding",
            "repair",
            "--source-thread-id",
            "thread-stale-generation-export",
            "--caller-automation-id",
            "automation-stale-generation-export",
            "--json",
            "--now",
            "2600",
        ],
    );
    let stale_generation_batch = create_desktop_batch_and_prepared_attempt(
        &stale_generation_home,
        "thread-stale-generation-export",
        "attempt-stale-generation-export",
        1,
        2601,
    );
    force_desktop_attempt_arm_pending(
        &stale_generation_home,
        "attempt-stale-generation-export",
        2602,
    );
    insert_desktop_prepared_attempt(
        &stale_generation_home,
        "thread-stale-generation-export",
        &stale_generation_batch,
        "attempt-current-generation-export",
        2,
        2603,
    );
    let stale_generation = cbth(
        &stale_generation_home,
        &[
            "desktop",
            "bridge-preflight",
            "--helper-direct-store",
            "--bridge-thread-id",
            "bridge-thread",
            "--json",
            "--now",
            "2604",
        ],
    );
    assert_eq!(
        stale_generation["desktop_bridge_preflight"]["snapshots"]["arm_pending_bindings"]["count"],
        0
    );
}

#[test]
fn desktop_writeback_helpers_fail_closed_for_stale_or_unsafe_inputs() {
    let home = temp_home();
    let missing_binding_batch = create_desktop_batch_and_prepared_attempt(
        &home,
        "thread-missing-binding",
        "attempt-missing-binding",
        1,
        3000,
    );
    let missing_binding = cbth_failure(
        &home,
        &[
            "desktop",
            "note-arm-pending",
            "--source-thread-id",
            "thread-missing-binding",
            "--attempt-id",
            "attempt-missing-binding",
            "--generation",
            "1",
            "--bridge-request-id",
            "bridge-request-missing-binding",
            "--json",
        ],
    );
    assert!(missing_binding.contains("desktop binding not found"));

    cbth(
        &home,
        &[
            "desktop",
            "binding",
            "repair",
            "--source-thread-id",
            "thread-mismatch",
            "--caller-automation-id",
            "automation-mismatch",
            "--json",
            "--now",
            "3001",
        ],
    );
    create_desktop_batch_and_prepared_attempt(
        &home,
        "thread-mismatch",
        "attempt-mismatch",
        2,
        3002,
    );
    let wrong_generation = cbth_failure(
        &home,
        &[
            "desktop",
            "note-arm-pending",
            "--source-thread-id",
            "thread-mismatch",
            "--attempt-id",
            "attempt-mismatch",
            "--generation",
            "1",
            "--bridge-request-id",
            "bridge-request-wrong-generation",
            "--json",
        ],
    );
    assert!(wrong_generation.contains("is generation 2, not 1"));

    cbth(
        &home,
        &[
            "desktop",
            "binding",
            "repair",
            "--source-thread-id",
            "thread-active-conflict",
            "--caller-automation-id",
            "automation-active-conflict",
            "--json",
            "--now",
            "3002",
        ],
    );
    let active_conflict_batch = create_desktop_batch_and_prepared_attempt(
        &home,
        "thread-active-conflict",
        "attempt-active-conflict-old",
        1,
        3003,
    );
    force_desktop_attempt_arm_pending(&home, "attempt-active-conflict-old", 3004);
    insert_desktop_prepared_attempt(
        &home,
        "thread-active-conflict",
        &active_conflict_batch,
        "attempt-active-conflict-new",
        2,
        3005,
    );
    let active_conflict = cbth_failure(
        &home,
        &[
            "desktop",
            "note-arm-pending",
            "--source-thread-id",
            "thread-active-conflict",
            "--attempt-id",
            "attempt-active-conflict-new",
            "--generation",
            "2",
            "--bridge-request-id",
            "bridge-request-active-conflict",
            "--json",
            "--now",
            "3006",
        ],
    );
    assert!(
        active_conflict
            .contains("thread thread-active-conflict already has active delivery attempt")
    );

    cbth(
        &home,
        &[
            "desktop",
            "binding",
            "repair",
            "--source-thread-id",
            "thread-expired-lease",
            "--caller-automation-id",
            "automation-expired-lease",
            "--json",
            "--now",
            "3003",
        ],
    );
    let expired_batch = create_desktop_batch_and_prepared_attempt(
        &home,
        "thread-expired-lease",
        "attempt-expired-lease",
        1,
        3004,
    );
    let expired_pending = cbth(
        &home,
        &[
            "desktop",
            "note-arm-pending",
            "--source-thread-id",
            "thread-expired-lease",
            "--attempt-id",
            "attempt-expired-lease",
            "--generation",
            "1",
            "--bridge-request-id",
            "bridge-request-expired-lease",
            "--json",
            "--now",
            "3005",
        ],
    );
    let expired_lease = expired_pending["desktop_arm_pending"]["bridge_arm_lease_id"]
        .as_str()
        .expect("expired lease id");
    let expired_arm = cbth_failure(
        &home,
        &[
            "desktop",
            "note-arm",
            "--source-thread-id",
            "thread-expired-lease",
            "--attempt-id",
            "attempt-expired-lease",
            "--generation",
            "1",
            "--bridge-request-id",
            "bridge-request-expired-lease",
            "--bridge-arm-lease-id",
            expired_lease,
            "--json",
            "--now",
            "3305",
        ],
    );
    assert!(expired_arm.contains("bridge arm lease expired at 3305"));
    let expired = cbth(&home, &["batch", "inspect", "--batch-id", &expired_batch]);
    assert_eq!(expired["batch"]["batch"]["delivery_attempt_count"], 0);
    let expired_attempt = cbth(
        &home,
        &[
            "attempt",
            "inspect",
            "--attempt-id",
            "attempt-expired-lease",
        ],
    );
    assert_eq!(expired_attempt["attempt"]["state"], "abandoned");
    assert_eq!(expired_attempt["attempt"]["abandoned_at"], 3305);

    cbth(
        &home,
        &[
            "desktop",
            "binding",
            "repair",
            "--source-thread-id",
            "thread-expired-pending-retry",
            "--caller-automation-id",
            "automation-expired-pending-retry",
            "--json",
            "--now",
            "3306",
        ],
    );
    let expired_retry_batch = create_desktop_batch_and_prepared_attempt(
        &home,
        "thread-expired-pending-retry",
        "attempt-expired-pending-retry",
        1,
        3307,
    );
    let expired_retry_pending = cbth(
        &home,
        &[
            "desktop",
            "note-arm-pending",
            "--source-thread-id",
            "thread-expired-pending-retry",
            "--attempt-id",
            "attempt-expired-pending-retry",
            "--generation",
            "1",
            "--bridge-request-id",
            "bridge-request-expired-pending-retry",
            "--json",
            "--now",
            "3308",
        ],
    );
    assert_eq!(
        expired_retry_pending["desktop_arm_pending"]["bridge_arm_lease_deadline"],
        3608
    );
    let expired_pending_retry = cbth_failure(
        &home,
        &[
            "desktop",
            "note-arm-pending",
            "--source-thread-id",
            "thread-expired-pending-retry",
            "--attempt-id",
            "attempt-expired-pending-retry",
            "--generation",
            "1",
            "--bridge-request-id",
            "bridge-request-expired-pending-retry",
            "--json",
            "--now",
            "3608",
        ],
    );
    assert!(expired_pending_retry.contains("bridge arm lease expired at 3608"));
    let expired_pending_attempt = cbth(
        &home,
        &[
            "attempt",
            "inspect",
            "--attempt-id",
            "attempt-expired-pending-retry",
        ],
    );
    assert_eq!(expired_pending_attempt["attempt"]["state"], "abandoned");
    insert_desktop_prepared_attempt(
        &home,
        "thread-expired-pending-retry",
        &expired_retry_batch,
        "attempt-expired-pending-retry-next",
        2,
        3609,
    );
    let fresh_after_expiry = cbth(
        &home,
        &[
            "desktop",
            "note-arm-pending",
            "--source-thread-id",
            "thread-expired-pending-retry",
            "--attempt-id",
            "attempt-expired-pending-retry-next",
            "--generation",
            "2",
            "--bridge-request-id",
            "bridge-request-expired-pending-retry-next",
            "--json",
            "--now",
            "3610",
        ],
    );
    assert_eq!(
        fresh_after_expiry["desktop_arm_pending"]["outcome"],
        "arm_pending"
    );

    cbth(
        &home,
        &[
            "desktop",
            "binding",
            "repair",
            "--source-thread-id",
            "thread-non-head",
            "--caller-automation-id",
            "automation-non-head",
            "--json",
            "--now",
            "3003",
        ],
    );
    let first_batch = create_desktop_batch_and_prepared_attempt(
        &home,
        "thread-non-head",
        "attempt-non-head-first",
        1,
        3004,
    );
    let second_submit = cbth(
        &home,
        &[
            "job",
            "submit",
            "--source-thread-id",
            "thread-non-head",
            "--summary",
            "second batch",
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
    let second_job_id = second_submit["job"]["job_id"].as_str().expect("job id");
    let second_failed = cbth(
        &home,
        &[
            "job",
            "fail",
            "--job-id",
            second_job_id,
            "--reason",
            "second ready",
        ],
    );
    let second_batch = second_failed["batch"]["batch"]["batch_id"]
        .as_str()
        .expect("second batch id")
        .to_owned();
    insert_desktop_prepared_attempt(
        &home,
        "thread-non-head",
        &second_batch,
        "attempt-non-head-second",
        1,
        3005,
    );
    let non_head = cbth_failure(
        &home,
        &[
            "desktop",
            "note-arm-pending",
            "--source-thread-id",
            "thread-non-head",
            "--attempt-id",
            "attempt-non-head-second",
            "--generation",
            "1",
            "--bridge-request-id",
            "bridge-request-non-head",
            "--json",
        ],
    );
    assert!(non_head.contains(&format!("batch {second_batch} is not the head batch")));
    assert!(non_head.contains("thread-non-head"));
    let first = cbth(&home, &["batch", "inspect", "--batch-id", &first_batch]);
    assert_eq!(first["batch"]["batch"]["delivery_attempt_count"], 0);

    let unsafe_submit = cbth(
        &home,
        &[
            "job",
            "submit",
            "--source-thread-id",
            "thread-unsafe",
            "--summary",
            "unsafe batch",
        ],
    );
    cbth(
        &home,
        &[
            "desktop",
            "binding",
            "repair",
            "--source-thread-id",
            "thread-unsafe",
            "--caller-automation-id",
            "automation-unsafe",
            "--json",
        ],
    );
    let unsafe_job_id = unsafe_submit["job"]["job_id"].as_str().expect("job id");
    let unsafe_failed = cbth(
        &home,
        &[
            "job",
            "fail",
            "--job-id",
            unsafe_job_id,
            "--reason",
            "unsafe ready",
        ],
    );
    let unsafe_batch = unsafe_failed["batch"]["batch"]["batch_id"]
        .as_str()
        .expect("unsafe batch")
        .to_owned();
    insert_desktop_prepared_attempt(
        &home,
        "thread-unsafe",
        &unsafe_batch,
        "attempt-unsafe",
        1,
        3006,
    );
    let unsafe_result = cbth_failure(
        &home,
        &[
            "desktop",
            "note-arm-pending",
            "--source-thread-id",
            "thread-unsafe",
            "--attempt-id",
            "attempt-unsafe",
            "--generation",
            "1",
            "--bridge-request-id",
            "bridge-request-unsafe",
            "--json",
        ],
    );
    assert!(unsafe_result.contains("is not eligible for Desktop delivery"));

    let missing_batch = cbth(
        &home,
        &["batch", "inspect", "--batch-id", &missing_binding_batch],
    );
    assert_eq!(missing_batch["batch"]["batch"]["delivery_attempt_count"], 0);
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
            .join("snapshots")
            .join(revision)
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
        "installation_state_path",
        "snapshots.ready_threads.path",
        "snapshots.arm_pending_bindings.path",
        "snapshots.pause_due_bindings.path",
    ] {
        let path = match key {
            "snapshot_manifest_path" => preflight[key].as_str().expect(key),
            "installation_state_path" => preflight[key].as_str().expect(key),
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
    assert_eq!(installation_state["snapshot_revision"], revision);
    assert_eq!(installation_state["published_at"], 2000);
    assert_eq!(installation_state["bridge_thread_id"], "bridge-thread");
    assert_eq!(
        installation_state["desktop_installation_state"]["read_transport_generation"],
        0
    );
    assert_eq!(
        installation_state["desktop_installation_state"]["read_transport_capability"],
        "unknown"
    );
    let latest_installation_state_path = home
        .path()
        .join("inbox")
        .join("desktop-installation-state.json");
    let latest_installation_state: Value =
        serde_json::from_slice(&fs::read(&latest_installation_state_path).unwrap_or_else(
            |error| panic!("read {}: {error}", latest_installation_state_path.display()),
        ))
        .unwrap_or_else(|error| {
            panic!(
                "parse {}: {error}",
                latest_installation_state_path.display()
            )
        });
    assert_eq!(
        latest_installation_state["snapshot_revision"],
        second_revision
    );
    assert_eq!(latest_installation_state["published_at"], 2001);
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
fn desktop_no_db_read_helpers_consume_published_snapshot_without_store_or_daemon() {
    let home = temp_home();
    let preflight = cbth_daemon(
        &home,
        &[
            "desktop",
            "bridge-preflight",
            "--bridge-thread-id",
            "bridge-thread-reader",
            "--helper-direct-store",
            "--json",
            "--now",
            "2300",
        ],
    );
    let preflight = &preflight["desktop_bridge_preflight"];
    let revision = preflight["snapshot_revision"]
        .as_str()
        .expect("snapshot revision")
        .to_owned();
    fs::remove_file(home.path().join("cbth.sqlite3")).expect("remove db file");
    fs::create_dir(home.path().join("cbth.sqlite3")).expect("replace db with directory");

    let snapshot = cbth_daemon(
        &home,
        &[
            "desktop",
            "read-snapshot",
            "--bridge-thread-id",
            "bridge-thread-reader",
            "--json",
        ],
    );
    let snapshot = &snapshot["desktop_snapshot"];
    assert_eq!(snapshot["snapshot_revision"], revision);
    assert_eq!(snapshot["bridge_thread_id"], "bridge-thread-reader");
    assert_eq!(snapshot["snapshots"]["ready_threads"]["count"], 0);
    assert_eq!(snapshot["snapshots"]["arm_pending_bindings"]["count"], 0);
    assert_eq!(snapshot["snapshots"]["pause_due_bindings"]["count"], 0);

    let arm_pending = cbth_daemon(
        &home,
        &[
            "desktop",
            "list-arm-pending",
            "--bridge-thread-id",
            "bridge-thread-reader",
            "--json",
        ],
    );
    assert_eq!(arm_pending["desktop_arm_pending_bindings"]["count"], 0);
    assert_eq!(
        arm_pending["desktop_arm_pending_bindings"]["entries"]
            .as_array()
            .expect("arm entries")
            .len(),
        0
    );

    let pause_due = cbth_daemon(
        &home,
        &[
            "desktop",
            "list-pause-due",
            "--bridge-thread-id",
            "bridge-thread-reader",
            "--json",
        ],
    );
    assert_eq!(pause_due["desktop_pause_due_bindings"]["count"], 0);

    let first_claim = cbth_daemon(
        &home,
        &[
            "desktop",
            "claim-next-ready",
            "--bridge-thread-id",
            "bridge-thread-reader",
            "--json",
        ],
    );
    let second_claim = cbth_daemon(
        &home,
        &[
            "desktop",
            "claim-next-ready",
            "--bridge-thread-id",
            "bridge-thread-reader",
            "--json",
        ],
    );
    assert_eq!(first_claim["desktop_ready_claim"]["entry"], Value::Null);
    assert_eq!(second_claim, first_claim);
    assert!(
        !home.path().join("run").join("startup.lock").exists(),
        "no-DB read helpers must not autostart the daemon"
    );
}

#[test]
fn desktop_no_db_read_helpers_use_revision_specific_installation_state_export() {
    let home = temp_home();
    let first = cbth_daemon(
        &home,
        &[
            "desktop",
            "bridge-preflight",
            "--bridge-thread-id",
            "bridge-thread-race",
            "--helper-direct-store",
            "--json",
            "--now",
            "2400",
        ],
    );
    let first = &first["desktop_bridge_preflight"];
    let first_revision = first["snapshot_revision"]
        .as_str()
        .expect("first revision")
        .to_owned();
    let manifest_path = first["snapshot_manifest_path"]
        .as_str()
        .expect("manifest path")
        .to_owned();
    let first_manifest = read_json_file(&manifest_path);

    let second = cbth_daemon(
        &home,
        &[
            "desktop",
            "bridge-preflight",
            "--bridge-thread-id",
            "bridge-thread-race",
            "--helper-direct-store",
            "--json",
            "--now",
            "2401",
        ],
    );
    let second_revision = second["desktop_bridge_preflight"]["snapshot_revision"]
        .as_str()
        .expect("second revision");
    assert_ne!(second_revision, first_revision);

    let latest_installation_state_path = home
        .path()
        .join("inbox")
        .join("desktop-installation-state.json");
    let latest_installation_state =
        read_json_file(&latest_installation_state_path.display().to_string());
    assert_eq!(
        latest_installation_state["snapshot_revision"],
        second_revision
    );
    assert_eq!(latest_installation_state["published_at"], 2401);

    write_json_file(&manifest_path, &first_manifest);
    let snapshot = cbth_daemon(
        &home,
        &[
            "desktop",
            "read-snapshot",
            "--bridge-thread-id",
            "bridge-thread-race",
            "--json",
        ],
    );
    let snapshot = &snapshot["desktop_snapshot"];
    assert_eq!(snapshot["snapshot_revision"], first_revision);
    assert_eq!(
        snapshot["installation_state"]["snapshot_revision"],
        first_revision
    );
    assert_eq!(snapshot["installation_state"]["published_at"], 2400);
    assert!(
        snapshot["installation_state_path"]
            .as_str()
            .expect("installation state path")
            .contains(&format!("/snapshots/{first_revision}/"))
    );
}

#[test]
fn desktop_no_db_read_helpers_fail_closed_for_snapshot_mismatch_and_path_escape() {
    let home = temp_home();
    let preflight = cbth_daemon(
        &home,
        &[
            "desktop",
            "bridge-preflight",
            "--bridge-thread-id",
            "bridge-thread-invalid",
            "--helper-direct-store",
            "--json",
        ],
    );
    let preflight = &preflight["desktop_bridge_preflight"];
    let ready_path = preflight["snapshots"]["ready_threads"]["path"]
        .as_str()
        .expect("ready path")
        .to_owned();
    let mut ready = read_json_file(&ready_path);
    ready["snapshot_revision"] = json!("different-revision");
    write_json_file(&ready_path, &ready);
    let revision_mismatch = cbth_daemon_failure(
        &home,
        &[
            "desktop",
            "read-snapshot",
            "--bridge-thread-id",
            "bridge-thread-invalid",
            "--json",
        ],
    );
    assert!(
        revision_mismatch.contains("ready_threads.snapshot_revision must be"),
        "unexpected stderr: {revision_mismatch}"
    );

    ready["snapshot_revision"] = preflight["snapshot_revision"].clone();
    write_json_file(&ready_path, &ready);
    let manifest_path = preflight["snapshot_manifest_path"]
        .as_str()
        .expect("manifest path")
        .to_owned();
    let mut manifest = read_json_file(&manifest_path);
    manifest["snapshots"]["ready_threads"]["path"] =
        json!(home.path().join("outside.json").display().to_string());
    write_json_file(&manifest_path, &manifest);
    let path_escape = cbth_daemon_failure(
        &home,
        &[
            "desktop",
            "read-snapshot",
            "--bridge-thread-id",
            "bridge-thread-invalid",
            "--json",
        ],
    );
    assert!(
        path_escape.contains("ready_threads.path must be"),
        "unexpected stderr: {path_escape}"
    );
}

#[test]
fn desktop_no_db_read_helpers_fail_closed_for_missing_malformed_and_oversized_files() {
    let missing_home = temp_home();
    let missing = cbth_daemon_failure(
        &missing_home,
        &[
            "desktop",
            "read-snapshot",
            "--bridge-thread-id",
            "bridge-thread-missing",
            "--json",
        ],
    );
    assert!(
        missing.contains("current-snapshot.json"),
        "unexpected stderr: {missing}"
    );

    let malformed_home = temp_home();
    let malformed_preflight = cbth_daemon(
        &malformed_home,
        &[
            "desktop",
            "bridge-preflight",
            "--bridge-thread-id",
            "bridge-thread-malformed",
            "--helper-direct-store",
            "--json",
        ],
    );
    let installation_state_path =
        malformed_preflight["desktop_bridge_preflight"]["installation_state_path"]
            .as_str()
            .expect("installation state path")
            .to_owned();
    fs::write(&installation_state_path, b"{not json").expect("write malformed json");
    let malformed = cbth_daemon_failure(
        &malformed_home,
        &[
            "desktop",
            "read-snapshot",
            "--bridge-thread-id",
            "bridge-thread-malformed",
            "--json",
        ],
    );
    assert!(
        malformed.contains("parse") && malformed.contains("desktop-installation-state.json"),
        "unexpected stderr: {malformed}"
    );

    let oversized_home = temp_home();
    let oversized_preflight = cbth_daemon(
        &oversized_home,
        &[
            "desktop",
            "bridge-preflight",
            "--bridge-thread-id",
            "bridge-thread-oversized",
            "--helper-direct-store",
            "--json",
        ],
    );
    let ready_path = oversized_preflight["desktop_bridge_preflight"]["snapshots"]["ready_threads"]
        ["path"]
        .as_str()
        .expect("ready path")
        .to_owned();
    fs::write(&ready_path, vec![b' '; 1024 * 1024 + 1]).expect("write oversized snapshot");
    let oversized = cbth_daemon_failure(
        &oversized_home,
        &[
            "desktop",
            "read-snapshot",
            "--bridge-thread-id",
            "bridge-thread-oversized",
            "--json",
        ],
    );
    assert!(
        oversized.contains("Desktop inbox file exceeds"),
        "unexpected stderr: {oversized}"
    );
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
                    "cli-session-proof-invalidation-dispatch",
                    "cli-session-recovery-dispatch",
                    "cli-turn-observation-dispatch",
                    "cli-turn-observation-expiry-dispatch",
                    "cli-auto-delivery-dispatch",
                    "task-supervisor",
                    "desktop-bridge-foundation-dispatch",
                    "desktop-inbox-revisioned-installation-state",
                    "desktop-writeback-helper-foundation"
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
    assert_eq!(
        installation_state["snapshot_revision"],
        preflight["snapshot_revision"]
    );
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
