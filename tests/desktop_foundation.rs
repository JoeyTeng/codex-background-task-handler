#[cfg(unix)]
use std::ffi::CString;
use std::fs;
#[cfg(unix)]
use std::io::Read;
use std::io::Write;
#[cfg(unix)]
use std::os::unix::ffi::OsStrExt;
#[cfg(unix)]
use std::os::unix::net::UnixListener;
use std::process::{Command, Output};
#[cfg(unix)]
use std::thread;
use std::time::{SystemTime, UNIX_EPOCH};

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

#[cfg(unix)]
fn mkfifo(path: &std::path::Path) {
    let c_path = CString::new(path.as_os_str().as_bytes()).expect("fifo path has no nul");
    let rc = unsafe { libc::mkfifo(c_path.as_ptr(), 0o600) };
    assert_eq!(rc, 0, "mkfifo {}", path.display());
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

fn marker_state(home: &TempDir, marker: &str) -> String {
    Connection::open(home.path().join("cbth.sqlite3"))
        .expect("open db")
        .query_row(
            "SELECT marker_state
             FROM desktop_transcript_relay_markers
             WHERE marker = ?",
            params![marker],
            |row| row.get(0),
        )
        .expect("query marker state")
}

fn write_json_file(path: &str, value: &Value) {
    let bytes = serde_json::to_vec_pretty(value).expect("serialize json");
    fs::write(path, bytes).unwrap_or_else(|error| panic!("write {path}: {error}"));
}

fn write_function_call_rollout(path: &std::path::Path, output: &str) {
    let record = json!({
        "type": "response_item",
        "payload": {
            "type": "function_call_output",
            "output": output,
        }
    });
    fs::write(path, serde_json::to_string(&record).unwrap()).expect("write rollout");
}

fn append_function_call_rollout_line(path: &std::path::Path, output: &str) {
    let record = json!({
        "type": "response_item",
        "payload": {
            "type": "function_call_output",
            "output": output,
        }
    });
    let mut file = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .expect("open rollout append");
    writeln!(file, "{}", serde_json::to_string(&record).unwrap()).expect("append rollout");
}

fn write_user_prompt_rollout(path: &std::path::Path, message: &str) {
    let record = json!({
        "type": "event_msg",
        "payload": {
            "type": "user_message",
            "message": message,
        }
    });
    fs::write(path, serde_json::to_string(&record).unwrap()).expect("write rollout");
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

fn create_desktop_batch(home: &TempDir, source_thread_id: &str) -> String {
    let submitted = cbth(
        home,
        &[
            "job",
            "submit",
            "--source-thread-id",
            source_thread_id,
            "--summary",
            "desktop ready fixture",
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
            "ready for Desktop materialization",
            "--max-delivery-attempts",
            "3",
            "--redelivery-window-seconds",
            "3600",
        ],
    );
    failed["batch"]["batch"]["batch_id"]
        .as_str()
        .expect("batch id")
        .to_owned()
}

fn repair_validated_desktop_installation_and_binding(
    home: &TempDir,
    source_thread_id: &str,
    caller_automation_id: &str,
    now: i64,
) {
    cbth(
        home,
        &[
            "desktop",
            "installation-state",
            "repair",
            "--read-transport",
            "direct-file-read",
            "--read-transport-capability",
            "validated",
            "--artifact-read-capability",
            "unknown",
            "--writeback-capability",
            "validated",
            "--json",
            "--now",
            &now.to_string(),
        ],
    );
    cbth(
        home,
        &[
            "desktop",
            "binding",
            "repair",
            "--source-thread-id",
            source_thread_id,
            "--caller-automation-id",
            caller_automation_id,
            "--json",
            "--now",
            &(now + 1).to_string(),
        ],
    );
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

fn mark_consumed_pending_marker_for_bridge(
    home: &TempDir,
    bridge_thread_id: &str,
    source_thread_id: &str,
    attempt_id: &str,
    generation: i64,
    bridge_request_id: &str,
    now: i64,
) {
    let marker = cbth(
        home,
        &[
            "desktop",
            "relay",
            "marker",
            "issue",
            "--bridge-thread-id",
            bridge_thread_id,
            "--kind",
            "arm-pending",
            "--source-thread-id",
            source_thread_id,
            "--attempt-id",
            attempt_id,
            "--generation",
            &generation.to_string(),
            "--bridge-request-id",
            bridge_request_id,
            "--json",
            "--now",
            &now.to_string(),
        ],
    );
    let marker = marker["desktop_transcript_relay_marker"]["record"]["marker"]
        .as_str()
        .expect("marker")
        .to_owned();
    let conn = Connection::open(home.path().join("cbth.sqlite3")).expect("open db");
    conn.execute(
        "UPDATE desktop_transcript_relay_markers
         SET marker_state = 'consumed',
             consumed_at = ?,
             envelope_hash = ?
         WHERE marker = ?",
        params![now + 1, format!("hash-{marker}"), marker],
    )
    .expect("mark pending marker consumed");
}

#[allow(clippy::too_many_arguments)]
fn issue_desktop_relay_marker(
    home: &TempDir,
    bridge_thread_id: &str,
    kind: &str,
    source_thread_id: &str,
    attempt_id: &str,
    generation: i64,
    bridge_request_id: &str,
    now: i64,
) -> String {
    let marker = cbth(
        home,
        &[
            "desktop",
            "relay",
            "marker",
            "issue",
            "--bridge-thread-id",
            bridge_thread_id,
            "--kind",
            kind,
            "--source-thread-id",
            source_thread_id,
            "--attempt-id",
            attempt_id,
            "--generation",
            &generation.to_string(),
            "--bridge-request-id",
            bridge_request_id,
            "--json",
            "--now",
            &now.to_string(),
        ],
    );
    marker["desktop_transcript_relay_marker"]["record"]["marker"]
        .as_str()
        .expect("marker")
        .to_owned()
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
    repair_validated_desktop_installation_and_binding(
        &home,
        "thread-desktop-writeback",
        "automation-writeback",
        2000,
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

    mark_consumed_pending_marker_for_bridge(
        &home,
        "bridge-thread",
        "thread-desktop-writeback",
        "attempt-desktop-writeback",
        1,
        "bridge-request-1",
        2140,
    );
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
fn desktop_bridge_preflight_materializes_ready_entry_and_claim_peeks_it() {
    let home = temp_home();
    repair_validated_desktop_installation_and_binding(
        &home,
        "thread-ready-materialize",
        "automation-ready-materialize",
        3000,
    );
    let batch_id = create_desktop_batch(&home, "thread-ready-materialize");
    let conn = Connection::open(home.path().join("cbth.sqlite3")).expect("open db");
    conn.execute(
        "UPDATE batches SET redelivery_window_ends_at = ? WHERE batch_id = ?",
        params![3015, &batch_id],
    )
    .expect("shorten redelivery window");
    drop(conn);

    let first = cbth(
        &home,
        &[
            "desktop",
            "bridge-preflight",
            "--helper-direct-store",
            "--bridge-thread-id",
            "bridge-ready-materialize",
            "--json",
            "--now",
            "3010",
        ],
    );
    let first = &first["desktop_bridge_preflight"];
    assert_eq!(first["snapshots"]["ready_threads"]["count"], 1);
    let ready_path = first["snapshots"]["ready_threads"]["path"]
        .as_str()
        .expect("ready path");
    let ready = read_json_file(ready_path);
    let entries = ready["ready_threads"]["entries"].as_array().unwrap();
    assert_eq!(entries.len(), 1);
    let entry = &entries[0];
    assert_eq!(entry["source_thread_id"], "thread-ready-materialize");
    assert_eq!(
        entry["caller_automation_id"],
        "automation-ready-materialize"
    );
    assert_eq!(entry["batch_id"], batch_id);
    assert_eq!(entry["generation"], 1);
    assert_eq!(entry["snapshot_revision"], first["snapshot_revision"]);
    assert_eq!(entry["requires_artifact_read"], false);
    assert!(
        entry["arm_pending_marker"]
            .as_str()
            .unwrap()
            .starts_with("CBTH_DESKTOP_RELAY_ARM_PENDING_")
    );
    assert_eq!(entry["marker_expires_at"], 3015);
    let attempt_id = entry["attempt_id"].as_str().unwrap().to_owned();
    let marker = entry["arm_pending_marker"].as_str().unwrap().to_owned();
    let claim = cbth(
        &home,
        &[
            "desktop",
            "claim-next-ready",
            "--bridge-thread-id",
            "bridge-ready-materialize",
            "--json",
        ],
    );
    assert_eq!(claim["desktop_ready_claim"]["entry"], *entry);

    let second = cbth(
        &home,
        &[
            "desktop",
            "bridge-preflight",
            "--helper-direct-store",
            "--bridge-thread-id",
            "bridge-ready-materialize",
            "--json",
            "--now",
            "3011",
        ],
    );
    let second = &second["desktop_bridge_preflight"];
    assert_eq!(second["snapshots"]["ready_threads"]["count"], 1);
    let second_ready = read_json_file(
        second["snapshots"]["ready_threads"]["path"]
            .as_str()
            .unwrap(),
    );
    let second_entry = &second_ready["ready_threads"]["entries"][0];
    assert_eq!(second_entry["attempt_id"], attempt_id);
    assert_eq!(second_entry["arm_pending_marker"], marker);
}

#[test]
fn desktop_bridge_preflight_caps_reused_ready_marker_after_redelivery_shortens() {
    let home = temp_home();
    repair_validated_desktop_installation_and_binding(
        &home,
        "thread-ready-cap-reuse",
        "automation-ready-cap-reuse",
        3050,
    );
    let batch_id = create_desktop_batch(&home, "thread-ready-cap-reuse");

    let first = cbth(
        &home,
        &[
            "desktop",
            "bridge-preflight",
            "--helper-direct-store",
            "--bridge-thread-id",
            "bridge-ready-cap-reuse",
            "--json",
            "--now",
            "3060",
        ],
    );
    let ready_path = first["desktop_bridge_preflight"]["snapshots"]["ready_threads"]["path"]
        .as_str()
        .expect("ready path");
    let ready = read_json_file(ready_path);
    let entry = &ready["ready_threads"]["entries"][0];
    let marker = entry["arm_pending_marker"].as_str().unwrap().to_owned();
    assert!(entry["marker_expires_at"].as_i64().unwrap() > 3070);

    let conn = Connection::open(home.path().join("cbth.sqlite3")).expect("open db");
    conn.execute(
        "UPDATE batches SET redelivery_window_ends_at = ? WHERE batch_id = ?",
        params![3070, &batch_id],
    )
    .expect("shorten redelivery window");
    drop(conn);

    let second = cbth(
        &home,
        &[
            "desktop",
            "bridge-preflight",
            "--helper-direct-store",
            "--bridge-thread-id",
            "bridge-ready-cap-reuse",
            "--json",
            "--now",
            "3061",
        ],
    );
    let ready_path = second["desktop_bridge_preflight"]["snapshots"]["ready_threads"]["path"]
        .as_str()
        .expect("ready path");
    let ready = read_json_file(ready_path);
    let entry = &ready["ready_threads"]["entries"][0];
    assert_eq!(entry["arm_pending_marker"], marker);
    assert_eq!(entry["marker_expires_at"], 3070);
    let conn = Connection::open(home.path().join("cbth.sqlite3")).expect("open db");
    let expires_at: i64 = conn
        .query_row(
            "SELECT expires_at FROM desktop_transcript_relay_markers WHERE marker = ?",
            params![marker],
            |row| row.get(0),
        )
        .expect("query marker expiry");
    assert_eq!(expires_at, 3070);
}

#[test]
fn desktop_bridge_preflight_refuses_stale_installation_fingerprint() {
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
            "--artifact-read-capability",
            "unknown",
            "--writeback-capability",
            "validated",
            "--validation-fingerprint",
            "stale-helper-fingerprint",
            "--json",
            "--now",
            "3090",
        ],
    );
    cbth(
        &home,
        &[
            "desktop",
            "binding",
            "repair",
            "--source-thread-id",
            "thread-ready-stale-fingerprint",
            "--caller-automation-id",
            "automation-ready-stale-fingerprint",
            "--json",
            "--now",
            "3091",
        ],
    );
    create_desktop_batch(&home, "thread-ready-stale-fingerprint");

    let preflight = cbth(
        &home,
        &[
            "desktop",
            "bridge-preflight",
            "--helper-direct-store",
            "--bridge-thread-id",
            "bridge-ready-stale-fingerprint",
            "--json",
            "--now",
            "3092",
        ],
    );
    assert_eq!(
        preflight["desktop_bridge_preflight"]["snapshots"]["ready_threads"]["count"],
        0
    );
    assert_eq!(
        preflight["desktop_bridge_preflight"]["installation_state"]["validation_fingerprint"],
        "stale-helper-fingerprint"
    );
    assert_eq!(
        preflight["desktop_bridge_preflight"]["installation_state"]["read_transport_capability"],
        "unknown"
    );
    assert_eq!(
        preflight["desktop_bridge_preflight"]["installation_state"]["writeback_capability"],
        "unknown"
    );
    let ready_path = preflight["desktop_bridge_preflight"]["snapshots"]["ready_threads"]["path"]
        .as_str()
        .expect("ready path");
    let ready = read_json_file(ready_path);
    assert_eq!(
        ready["ready_threads"]["entries"].as_array().unwrap().len(),
        0
    );
}

#[test]
fn desktop_marker_migration_preserves_valid_generation_zero_snapshot() {
    let home = temp_home();
    cbth(
        &home,
        &[
            "desktop",
            "binding",
            "repair",
            "--source-thread-id",
            "thread-marker-generation-zero",
            "--caller-automation-id",
            "automation-marker-generation-zero-a",
            "--json",
            "--now",
            "3120",
        ],
    );
    create_desktop_batch_and_prepared_attempt(
        &home,
        "thread-marker-generation-zero",
        "attempt-marker-generation-zero",
        1,
        3121,
    );
    let marker = cbth(
        &home,
        &[
            "desktop",
            "relay",
            "marker",
            "issue",
            "--bridge-thread-id",
            "bridge-marker-generation-zero",
            "--kind",
            "arm-pending",
            "--source-thread-id",
            "thread-marker-generation-zero",
            "--attempt-id",
            "attempt-marker-generation-zero",
            "--generation",
            "1",
            "--bridge-request-id",
            "bridge-request-marker-generation-zero",
            "--json",
            "--now",
            "3122",
        ],
    );
    let marker = marker["desktop_transcript_relay_marker"]["record"]["marker"]
        .as_str()
        .unwrap()
        .to_owned();
    cbth(
        &home,
        &[
            "desktop",
            "binding",
            "repair",
            "--source-thread-id",
            "thread-marker-generation-zero",
            "--caller-automation-id",
            "automation-marker-generation-zero-b",
            "--json",
            "--now",
            "3123",
        ],
    );

    let conn = Connection::open(home.path().join("cbth.sqlite3")).expect("open db");
    let (caller_automation_id, read_transport_generation): (String, i64) = conn
        .query_row(
            "SELECT caller_automation_id, read_transport_generation
             FROM desktop_transcript_relay_markers
             WHERE marker = ?",
            params![marker],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .expect("query marker snapshot");
    assert_eq!(caller_automation_id, "automation-marker-generation-zero-a");
    assert_eq!(read_transport_generation, 0);
}

#[test]
fn desktop_bridge_preflight_rotates_past_active_ready_marker() {
    let home = temp_home();
    repair_validated_desktop_installation_and_binding(
        &home,
        "thread-ready-rotate-a",
        "automation-ready-rotate-a",
        3130,
    );
    repair_validated_desktop_installation_and_binding(
        &home,
        "thread-ready-rotate-b",
        "automation-ready-rotate-b",
        3132,
    );
    create_desktop_batch(&home, "thread-ready-rotate-a");
    create_desktop_batch(&home, "thread-ready-rotate-b");

    let first = cbth(
        &home,
        &[
            "desktop",
            "bridge-preflight",
            "--helper-direct-store",
            "--bridge-thread-id",
            "bridge-ready-rotate",
            "--json",
            "--now",
            "3140",
        ],
    );
    let first_ready_path = first["desktop_bridge_preflight"]["snapshots"]["ready_threads"]["path"]
        .as_str()
        .expect("first ready path");
    let first_ready = read_json_file(first_ready_path);
    let first_entry = &first_ready["ready_threads"]["entries"][0];
    assert_eq!(first_entry["source_thread_id"], "thread-ready-rotate-a");

    let second = cbth(
        &home,
        &[
            "desktop",
            "bridge-preflight",
            "--helper-direct-store",
            "--bridge-thread-id",
            "bridge-ready-rotate",
            "--json",
            "--now",
            "3141",
        ],
    );
    let second_ready_path =
        second["desktop_bridge_preflight"]["snapshots"]["ready_threads"]["path"]
            .as_str()
            .expect("second ready path");
    let second_ready = read_json_file(second_ready_path);
    let second_entries = second_ready["ready_threads"]["entries"].as_array().unwrap();
    assert_eq!(second_entries.len(), 2);
    assert_eq!(
        second_entries[0]["source_thread_id"],
        "thread-ready-rotate-a"
    );
    assert_eq!(
        second_entries[0]["arm_pending_marker"],
        first_entry["arm_pending_marker"]
    );
    let second_entry = &second_entries[1];
    assert_eq!(second_entry["source_thread_id"], "thread-ready-rotate-b");
    assert_ne!(
        second_entry["arm_pending_marker"],
        first_entry["arm_pending_marker"]
    );
}

#[test]
fn desktop_bridge_preflight_reissues_ready_marker_after_binding_repair() {
    let home = temp_home();
    repair_validated_desktop_installation_and_binding(
        &home,
        "thread-ready-repair",
        "automation-ready-repair-old",
        3150,
    );
    create_desktop_batch(&home, "thread-ready-repair");

    let first = cbth(
        &home,
        &[
            "desktop",
            "bridge-preflight",
            "--helper-direct-store",
            "--bridge-thread-id",
            "bridge-ready-repair",
            "--json",
            "--now",
            "3160",
        ],
    );
    let first_ready_path = first["desktop_bridge_preflight"]["snapshots"]["ready_threads"]["path"]
        .as_str()
        .expect("first ready path");
    let first_ready = read_json_file(first_ready_path);
    let first_entry = &first_ready["ready_threads"]["entries"][0];
    let attempt_id = first_entry["attempt_id"].as_str().unwrap().to_owned();
    let old_marker = first_entry["arm_pending_marker"]
        .as_str()
        .unwrap()
        .to_owned();

    cbth(
        &home,
        &[
            "desktop",
            "binding",
            "repair",
            "--source-thread-id",
            "thread-ready-repair",
            "--caller-automation-id",
            "automation-ready-repair-new",
            "--json",
            "--now",
            "3161",
        ],
    );

    let second = cbth(
        &home,
        &[
            "desktop",
            "bridge-preflight",
            "--helper-direct-store",
            "--bridge-thread-id",
            "bridge-ready-repair",
            "--json",
            "--now",
            "3162",
        ],
    );
    let second_ready_path =
        second["desktop_bridge_preflight"]["snapshots"]["ready_threads"]["path"]
            .as_str()
            .expect("second ready path");
    let second_ready = read_json_file(second_ready_path);
    let second_entries = second_ready["ready_threads"]["entries"].as_array().unwrap();
    assert_eq!(second_entries.len(), 1);
    let second_entry = &second_entries[0];
    assert_eq!(second_entry["attempt_id"], attempt_id);
    assert_eq!(
        second_entry["caller_automation_id"],
        "automation-ready-repair-new"
    );
    assert_ne!(second_entry["arm_pending_marker"], old_marker);
    assert_eq!(marker_state(&home, &old_marker), "issued");
}

#[test]
fn desktop_bridge_preflight_abandons_expired_prepared_attempt_before_ready() {
    let home = temp_home();
    repair_validated_desktop_installation_and_binding(
        &home,
        "thread-ready-expired",
        "automation-ready-expired",
        3100,
    );
    let batch_id = create_desktop_batch(&home, "thread-ready-expired");

    let first = cbth(
        &home,
        &[
            "desktop",
            "bridge-preflight",
            "--helper-direct-store",
            "--bridge-thread-id",
            "bridge-ready-expired",
            "--json",
            "--now",
            "3110",
        ],
    );
    assert_eq!(
        first["desktop_bridge_preflight"]["snapshots"]["ready_threads"]["count"],
        1
    );
    let ready_path = first["desktop_bridge_preflight"]["snapshots"]["ready_threads"]["path"]
        .as_str()
        .unwrap();
    let ready = read_json_file(ready_path);
    let attempt_id = ready["ready_threads"]["entries"][0]["attempt_id"]
        .as_str()
        .unwrap()
        .to_owned();

    let conn = Connection::open(home.path().join("cbth.sqlite3")).expect("open db");
    conn.execute(
        "UPDATE batches SET redelivery_window_ends_at = ? WHERE batch_id = ?",
        params![3111, &batch_id],
    )
    .unwrap();
    drop(conn);

    let second = cbth(
        &home,
        &[
            "desktop",
            "bridge-preflight",
            "--helper-direct-store",
            "--bridge-thread-id",
            "bridge-ready-expired",
            "--json",
            "--now",
            "3112",
        ],
    );
    assert_eq!(
        second["desktop_bridge_preflight"]["snapshots"]["ready_threads"]["count"],
        0
    );

    let conn = Connection::open(home.path().join("cbth.sqlite3")).expect("open db");
    let attempt_state: String = conn
        .query_row(
            "SELECT state FROM delivery_attempts WHERE attempt_id = ?",
            params![attempt_id],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(attempt_state, "abandoned");
    let batch_state: String = conn
        .query_row(
            "SELECT state FROM batches WHERE batch_id = ?",
            params![batch_id],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(batch_state, "closed");
}

#[test]
fn desktop_bridge_preflight_filters_ineligible_ready_candidates() {
    for (case, mutate) in [
        ("unknown-capability", "none"),
        ("requires-artifact-read", "artifact"),
        ("unsafe-network", "network"),
        ("degraded-binding", "degraded"),
        ("unquiesced-binding", "unquiesced"),
        ("active-arm-pending", "arm-pending"),
        ("active-cli-attempt", "cli-active"),
        ("attempt-budget", "budget"),
    ] {
        let home = temp_home();
        let source_thread_id = format!("thread-ready-filter-{case}");
        let automation_id = format!("automation-ready-filter-{case}");
        if mutate != "none" {
            repair_validated_desktop_installation_and_binding(
                &home,
                &source_thread_id,
                &automation_id,
                3200,
            );
        } else {
            cbth(
                &home,
                &[
                    "desktop",
                    "binding",
                    "repair",
                    "--source-thread-id",
                    &source_thread_id,
                    "--caller-automation-id",
                    &automation_id,
                    "--json",
                    "--now",
                    "3201",
                ],
            );
        }
        let batch_id = create_desktop_batch(&home, &source_thread_id);
        let conn = Connection::open(home.path().join("cbth.sqlite3")).expect("open db");
        match mutate {
            "artifact" => {
                conn.execute(
                    "UPDATE batches SET requires_artifact_read = 1 WHERE batch_id = ?",
                    params![&batch_id],
                )
                .unwrap();
            }
            "network" => {
                conn.execute(
                    "UPDATE batches SET delivery_requires_network = 1 WHERE batch_id = ?",
                    params![&batch_id],
                )
                .unwrap();
            }
            "degraded" => {
                conn.execute(
                    "UPDATE desktop_bindings SET binding_state = 'degraded' WHERE source_thread_id = ?",
                    params![&source_thread_id],
                )
                .unwrap();
            }
            "unquiesced" => {
                conn.execute(
                    "UPDATE desktop_bindings
                     SET armed_generation = 7,
                         armed_generation_quiesced_at = NULL,
                         pause_deadline = 3300
                     WHERE source_thread_id = ?",
                    params![&source_thread_id],
                )
                .unwrap();
            }
            "arm-pending" => {
                insert_desktop_prepared_attempt(
                    &home,
                    &source_thread_id,
                    &batch_id,
                    "attempt-ready-filter-arm-pending",
                    1,
                    3202,
                );
                force_desktop_attempt_arm_pending(&home, "attempt-ready-filter-arm-pending", 3203);
            }
            "cli-active" => {
                conn.execute(
                    "INSERT INTO delivery_attempts (
                        attempt_id, batch_id, source_thread_id, adapter_kind,
                        authorization_mode, state, generation, created_at, updated_at
                    ) VALUES (?, ?, ?, 'cli', 'strict_safe', 'accept_pending', 1, 3202, 3202)",
                    params![
                        format!("attempt-ready-filter-cli-active-{case}"),
                        &batch_id,
                        &source_thread_id
                    ],
                )
                .unwrap();
            }
            "budget" => {
                conn.execute(
                    "UPDATE batches
                     SET delivery_attempt_count = max_delivery_attempts
                     WHERE batch_id = ?",
                    params![&batch_id],
                )
                .unwrap();
            }
            "none" => {}
            other => panic!("unknown mutate case {other}"),
        }
        drop(conn);

        let preflight = cbth(
            &home,
            &[
                "desktop",
                "bridge-preflight",
                "--helper-direct-store",
                "--bridge-thread-id",
                "bridge-ready-filter",
                "--json",
                "--now",
                "3210",
            ],
        );
        assert_eq!(
            preflight["desktop_bridge_preflight"]["snapshots"]["ready_threads"]["count"], 0,
            "case {case}"
        );
    }
}

#[test]
fn desktop_ready_markers_drive_scanner_to_cooldown() {
    let home = temp_home();
    repair_validated_desktop_installation_and_binding(
        &home,
        "thread-ready-scanner",
        "automation-ready-scanner",
        3400,
    );
    let batch_id = create_desktop_batch(&home, "thread-ready-scanner");
    let rollout = home.path().join("ready-scanner-rollout.jsonl");
    fs::write(&rollout, "").expect("create rollout");
    cbth(
        &home,
        &[
            "desktop",
            "relay",
            "scanner",
            "bind",
            "--bridge-thread-id",
            "bridge-ready-scanner",
            "--rollout-path",
            rollout.to_str().unwrap(),
            "--from-start",
            "--json",
            "--now",
            "3401",
        ],
    );

    let ready_preflight = cbth(
        &home,
        &[
            "desktop",
            "bridge-preflight",
            "--helper-direct-store",
            "--bridge-thread-id",
            "bridge-ready-scanner",
            "--json",
            "--now",
            "3410",
        ],
    );
    let ready_path =
        ready_preflight["desktop_bridge_preflight"]["snapshots"]["ready_threads"]["path"]
            .as_str()
            .unwrap();
    let ready = read_json_file(ready_path);
    let ready_entry = &ready["ready_threads"]["entries"][0];
    let attempt_id = ready_entry["attempt_id"].as_str().unwrap();
    let generation = ready_entry["generation"].as_i64().unwrap().to_string();
    let bridge_request_id = ready_entry["bridge_request_id"].as_str().unwrap();
    let pending_marker = ready_entry["arm_pending_marker"].as_str().unwrap();

    let other_bridge_ready_preflight = cbth(
        &home,
        &[
            "desktop",
            "bridge-preflight",
            "--helper-direct-store",
            "--bridge-thread-id",
            "bridge-ready-scanner-other",
            "--json",
            "--now",
            "3410",
        ],
    );
    assert_eq!(
        other_bridge_ready_preflight["desktop_bridge_preflight"]["snapshots"]["ready_threads"]["count"],
        0
    );

    let pending_emit = cbth_output(
        &home,
        &[
            "desktop",
            "relay",
            "emit-arm-pending",
            "--source-thread-id",
            "thread-ready-scanner",
            "--attempt-id",
            attempt_id,
            "--generation",
            &generation,
            "--bridge-request-id",
            bridge_request_id,
            "--marker",
            pending_marker,
            "--json",
            "--now",
            "3411",
        ],
        false,
    );
    assert!(pending_emit.status.success());
    append_function_call_rollout_line(&rollout, &String::from_utf8(pending_emit.stdout).unwrap());
    let pending_scan = cbth(
        &home,
        &[
            "desktop",
            "relay",
            "scanner",
            "scan-once",
            "--bridge-thread-id",
            "bridge-ready-scanner",
            "--json",
            "--now",
            "3420",
        ],
    );
    assert_eq!(
        pending_scan["desktop_relay_scanner_scan"]["consumed_markers"],
        1
    );
    let pending_attempt = cbth(&home, &["attempt", "inspect", "--attempt-id", attempt_id]);
    assert_eq!(pending_attempt["attempt"]["state"], "arm_pending");

    let other_bridge_arm_preflight = cbth(
        &home,
        &[
            "desktop",
            "bridge-preflight",
            "--helper-direct-store",
            "--bridge-thread-id",
            "bridge-ready-scanner-other",
            "--json",
            "--now",
            "3421",
        ],
    );
    assert_eq!(
        other_bridge_arm_preflight["desktop_bridge_preflight"]["snapshots"]["arm_pending_bindings"]
            ["count"],
        0
    );

    let arm_preflight = cbth(
        &home,
        &[
            "desktop",
            "bridge-preflight",
            "--helper-direct-store",
            "--bridge-thread-id",
            "bridge-ready-scanner",
            "--json",
            "--now",
            "3421",
        ],
    );
    assert_eq!(
        arm_preflight["desktop_bridge_preflight"]["snapshots"]["ready_threads"]["count"],
        0
    );
    assert_eq!(
        arm_preflight["desktop_bridge_preflight"]["snapshots"]["arm_pending_bindings"]["count"],
        1
    );
    let arm_path =
        arm_preflight["desktop_bridge_preflight"]["snapshots"]["arm_pending_bindings"]["path"]
            .as_str()
            .unwrap();
    let arm_pending = read_json_file(arm_path);
    let arm_entry = &arm_pending["arm_pending_bindings"]["entries"][0];
    let arm_marker = arm_entry["arm_accepted_marker"].as_str().unwrap();
    assert!(arm_marker.starts_with("CBTH_DESKTOP_RELAY_ARM_ACCEPTED_"));
    assert!(
        arm_entry["marker_expires_at"].as_i64().unwrap()
            <= arm_entry["bridge_arm_lease_deadline"].as_i64().unwrap()
    );
    assert!(
        arm_entry["marker_expires_at"].as_i64().unwrap()
            <= arm_entry["arm_pending_deadline"].as_i64().unwrap()
    );

    let arm_emit = cbth_output(
        &home,
        &[
            "desktop",
            "relay",
            "emit-arm-accepted",
            "--source-thread-id",
            "thread-ready-scanner",
            "--attempt-id",
            attempt_id,
            "--generation",
            &generation,
            "--bridge-request-id",
            bridge_request_id,
            "--marker",
            arm_marker,
            "--json",
            "--now",
            "3422",
        ],
        false,
    );
    assert!(arm_emit.status.success());
    append_function_call_rollout_line(&rollout, &String::from_utf8(arm_emit.stdout).unwrap());
    let arm_scan = cbth(
        &home,
        &[
            "desktop",
            "relay",
            "scanner",
            "scan-once",
            "--bridge-thread-id",
            "bridge-ready-scanner",
            "--json",
            "--now",
            "3430",
        ],
    );
    assert_eq!(
        arm_scan["desktop_relay_scanner_scan"]["consumed_markers"],
        1
    );
    let final_attempt = cbth(&home, &["attempt", "inspect", "--attempt-id", attempt_id]);
    assert_eq!(final_attempt["attempt"]["state"], "cooldown");
    let final_batch = cbth(&home, &["batch", "inspect", "--batch-id", &batch_id]);
    assert_eq!(final_batch["batch"]["batch"]["delivery_attempt_count"], 1);

    let repeat_scan = cbth(
        &home,
        &[
            "desktop",
            "relay",
            "scanner",
            "scan-once",
            "--bridge-thread-id",
            "bridge-ready-scanner",
            "--json",
            "--now",
            "3431",
        ],
    );
    assert_eq!(
        repeat_scan["desktop_relay_scanner_scan"]["scanned_bindings"],
        0
    );
    let repeated_batch = cbth(&home, &["batch", "inspect", "--batch-id", &batch_id]);
    assert_eq!(
        repeated_batch["batch"]["batch"]["delivery_attempt_count"],
        1
    );
}

#[test]
fn desktop_bridge_preflight_reconciles_manual_consumed_pending_marker() {
    let home = temp_home();
    repair_validated_desktop_installation_and_binding(
        &home,
        "thread-ready-manual-consume",
        "automation-ready-manual-consume",
        3440,
    );
    create_desktop_batch(&home, "thread-ready-manual-consume");

    let ready_preflight = cbth(
        &home,
        &[
            "desktop",
            "bridge-preflight",
            "--helper-direct-store",
            "--bridge-thread-id",
            "bridge-ready-manual-consume",
            "--json",
            "--now",
            "3441",
        ],
    );
    let ready_path =
        ready_preflight["desktop_bridge_preflight"]["snapshots"]["ready_threads"]["path"]
            .as_str()
            .unwrap();
    let ready = read_json_file(ready_path);
    let ready_entry = &ready["ready_threads"]["entries"][0];
    let attempt_id = ready_entry["attempt_id"].as_str().unwrap();
    let generation = ready_entry["generation"].as_i64().unwrap().to_string();
    let bridge_request_id = ready_entry["bridge_request_id"].as_str().unwrap();
    let pending_marker = ready_entry["arm_pending_marker"].as_str().unwrap();

    let pending_emit = cbth_output(
        &home,
        &[
            "desktop",
            "relay",
            "emit-arm-pending",
            "--source-thread-id",
            "thread-ready-manual-consume",
            "--attempt-id",
            attempt_id,
            "--generation",
            &generation,
            "--bridge-request-id",
            bridge_request_id,
            "--marker",
            pending_marker,
            "--json",
            "--now",
            "3442",
        ],
        false,
    );
    assert!(pending_emit.status.success());
    let pending_rollout = home.path().join("manual-consume-pending-rollout.jsonl");
    write_function_call_rollout(
        &pending_rollout,
        &String::from_utf8(pending_emit.stdout).unwrap(),
    );
    let pending = cbth(
        &home,
        &[
            "desktop",
            "relay",
            "consume-transcript",
            "--rollout-path",
            pending_rollout.to_str().unwrap(),
            "--marker",
            pending_marker,
            "--json",
            "--now",
            "3443",
        ],
    );
    assert_eq!(
        pending["desktop_transcript_relay_consumption"]["record"]["outcome"]["outcome"],
        "arm_pending"
    );
    assert_eq!(
        marker_state(&home, pending_marker),
        "issued",
        "manual consume writes the replay fence before marker reconciliation"
    );

    let arm_preflight = cbth(
        &home,
        &[
            "desktop",
            "bridge-preflight",
            "--helper-direct-store",
            "--bridge-thread-id",
            "bridge-ready-manual-consume",
            "--json",
            "--now",
            "3444",
        ],
    );
    assert_eq!(
        arm_preflight["desktop_bridge_preflight"]["snapshots"]["arm_pending_bindings"]["count"],
        1
    );
    assert_eq!(marker_state(&home, pending_marker), "consumed");
    let arm_path =
        arm_preflight["desktop_bridge_preflight"]["snapshots"]["arm_pending_bindings"]["path"]
            .as_str()
            .unwrap();
    let arm_pending = read_json_file(arm_path);
    let arm_entry = &arm_pending["arm_pending_bindings"]["entries"][0];
    assert_eq!(arm_entry["attempt_id"], attempt_id);
    assert!(
        arm_entry["arm_accepted_marker"]
            .as_str()
            .unwrap()
            .starts_with("CBTH_DESKTOP_RELAY_ARM_ACCEPTED_")
    );
}

#[test]
fn desktop_writeback_validation_fixture_prepares_safe_attempt() {
    let home = temp_home();
    let fixture = cbth(
        &home,
        &[
            "desktop",
            "validation",
            "prepare-writeback-fixture",
            "--source-thread-id",
            "thread-desktop-live-writeback",
            "--caller-automation-id",
            "automation-desktop-live-writeback",
            "--bridge-request-id",
            "bridge-request-live-writeback",
            "--now",
            "4100",
            "--json",
        ],
    );
    let fixture = &fixture["desktop_writeback_fixture"];
    assert_eq!(fixture["source_thread_id"], "thread-desktop-live-writeback");
    assert_eq!(
        fixture["caller_automation_id"],
        "automation-desktop-live-writeback"
    );
    assert_eq!(
        fixture["bridge_request_id"],
        "bridge-request-live-writeback"
    );
    assert_eq!(fixture["batch"]["state"], "open");
    assert_eq!(fixture["batch"]["replay_policy"], "automatic");
    assert_eq!(fixture["batch"]["requires_artifact_read"], false);
    assert_eq!(
        fixture["batch"]["delivery_policy"]["delivery_read_only"],
        true
    );
    assert_eq!(
        fixture["batch"]["delivery_policy"]["delivery_requires_approval"],
        false
    );
    assert_eq!(fixture["attempt"]["adapter_kind"], "desktop");
    assert_eq!(fixture["attempt"]["state"], "prepared");
    assert_eq!(fixture["attempt"]["generation"], 1);
    assert_eq!(fixture["binding"]["binding_state"], "bound");

    let attempt_id = fixture["attempt"]["attempt_id"].as_str().unwrap();
    let pending = cbth(
        &home,
        &[
            "desktop",
            "note-arm-pending",
            "--source-thread-id",
            "thread-desktop-live-writeback",
            "--attempt-id",
            attempt_id,
            "--generation",
            "1",
            "--bridge-request-id",
            "bridge-request-live-writeback",
            "--now",
            "4110",
            "--json",
        ],
    );
    assert_eq!(pending["desktop_arm_pending"]["outcome"], "arm_pending");
    let lease_id = pending["desktop_arm_pending"]["bridge_arm_lease_id"]
        .as_str()
        .unwrap();

    let repeated_pending = cbth(
        &home,
        &[
            "desktop",
            "note-arm-pending",
            "--source-thread-id",
            "thread-desktop-live-writeback",
            "--attempt-id",
            attempt_id,
            "--generation",
            "1",
            "--bridge-request-id",
            "bridge-request-live-writeback",
            "--now",
            "4120",
            "--json",
        ],
    );
    assert_eq!(
        repeated_pending["desktop_arm_pending"]["outcome"],
        "already_pending"
    );
    assert_eq!(
        repeated_pending["desktop_arm_pending"]["bridge_arm_lease_id"],
        lease_id
    );

    let armed = cbth(
        &home,
        &[
            "desktop",
            "note-arm",
            "--source-thread-id",
            "thread-desktop-live-writeback",
            "--attempt-id",
            attempt_id,
            "--generation",
            "1",
            "--bridge-request-id",
            "bridge-request-live-writeback",
            "--bridge-arm-lease-id",
            lease_id,
            "--now",
            "4130",
            "--json",
        ],
    );
    assert_eq!(armed["desktop_arm"]["outcome"], "armed");
    assert_eq!(armed["desktop_arm"]["delivery_attempt_count"], 1);

    let repeated_arm = cbth(
        &home,
        &[
            "desktop",
            "note-arm",
            "--source-thread-id",
            "thread-desktop-live-writeback",
            "--attempt-id",
            attempt_id,
            "--generation",
            "1",
            "--bridge-request-id",
            "bridge-request-live-writeback",
            "--bridge-arm-lease-id",
            lease_id,
            "--now",
            "4140",
            "--json",
        ],
    );
    assert_eq!(repeated_arm["desktop_arm"]["outcome"], "already_armed");
    assert_eq!(repeated_arm["desktop_arm"]["delivery_attempt_count"], 1);
}

#[test]
fn desktop_writeback_validation_fixture_fail_closed_cases_and_help_hidden() {
    let home = temp_home();
    let help = cbth_output(&home, &["desktop", "--help"], true);
    assert!(help.status.success(), "desktop help should succeed");
    let help = String::from_utf8_lossy(&help.stdout);
    assert!(!help.contains("validation"));
    assert!(!help.contains("prepare-writeback-fixture"));

    let empty_source = cbth_failure(
        &home,
        &[
            "desktop",
            "validation",
            "prepare-writeback-fixture",
            "--source-thread-id",
            "",
            "--caller-automation-id",
            "automation-empty-source",
            "--json",
        ],
    );
    assert!(empty_source.contains("source_thread_id must not be empty"));

    cbth(
        &home,
        &[
            "desktop",
            "binding",
            "repair",
            "--source-thread-id",
            "thread-bound",
            "--caller-automation-id",
            "automation-original",
            "--now",
            "4200",
            "--json",
        ],
    );
    let incompatible_binding = cbth_failure(
        &home,
        &[
            "desktop",
            "validation",
            "prepare-writeback-fixture",
            "--source-thread-id",
            "thread-bound",
            "--caller-automation-id",
            "automation-replacement",
            "--bridge-request-id",
            "bridge-request-replacement",
            "--now",
            "4210",
            "--json",
        ],
    );
    assert!(incompatible_binding.contains(
        "source_thread_id thread-bound is already bound to caller_automation_id automation-original"
    ));

    cbth(
        &home,
        &[
            "desktop",
            "validation",
            "prepare-writeback-fixture",
            "--source-thread-id",
            "thread-open-batch",
            "--caller-automation-id",
            "automation-open-batch",
            "--bridge-request-id",
            "bridge-request-open-batch",
            "--now",
            "4300",
            "--json",
        ],
    );
    let duplicate_fixture = cbth_failure(
        &home,
        &[
            "desktop",
            "validation",
            "prepare-writeback-fixture",
            "--source-thread-id",
            "thread-open-batch",
            "--caller-automation-id",
            "automation-open-batch",
            "--bridge-request-id",
            "bridge-request-open-batch-again",
            "--now",
            "4310",
            "--json",
        ],
    );
    assert!(duplicate_fixture.contains("already has open batch"));
}

#[test]
fn desktop_transcript_writeback_probe_emits_prefixed_envelope_without_store() {
    let home = temp_home();
    let output = cbth_output(
        &home,
        &[
            "desktop",
            "validation",
            "emit-transcript-writeback-probe",
            "--bridge-thread-id",
            "bridge-thread",
            "--probe-id",
            "probe-transcript",
            "--marker",
            "CBTH_TRANSCRIPT_WRITEBACK_TEST",
            "--json",
            "--now",
            "6100",
        ],
        false,
    );
    assert!(
        output.status.success(),
        "transcript probe failed\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).expect("utf8 stdout");
    let prefix = "CBTH_TRANSCRIPT_WRITEBACK_V1 ";
    assert!(stdout.starts_with(prefix), "stdout: {stdout}");
    let envelope: Value =
        serde_json::from_str(stdout.trim_start_matches(prefix)).expect("valid envelope");
    assert_eq!(envelope["schema_version"], 1);
    assert_eq!(envelope["channel"], "desktop_transcript_writeback");
    assert_eq!(envelope["kind"], "validation_probe");
    assert_eq!(envelope["bridge_thread_id"], "bridge-thread");
    assert_eq!(envelope["probe_id"], "probe-transcript");
    assert_eq!(envelope["marker"], "CBTH_TRANSCRIPT_WRITEBACK_TEST");
    assert_eq!(envelope["created_at"], 6100);
    assert!(!home.path().join("run").join("startup.lock").exists());
    assert!(!home.path().join("cbth.sqlite3").exists());
    assert!(!home.path().join("inbox").exists());
}

#[test]
fn desktop_transcript_arm_emitters_output_prefixed_envelopes_without_store() {
    let home = temp_home();
    let pending = cbth_output(
        &home,
        &[
            "desktop",
            "validation",
            "emit-transcript-arm-pending",
            "--source-thread-id",
            "thread-relay",
            "--attempt-id",
            "attempt-relay",
            "--generation",
            "1",
            "--bridge-request-id",
            "bridge-request-relay",
            "--marker",
            "CBTH_TRANSCRIPT_ARM_PENDING",
            "--json",
            "--now",
            "6500",
        ],
        false,
    );
    assert!(
        pending.status.success(),
        "arm-pending emit failed\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&pending.stdout),
        String::from_utf8_lossy(&pending.stderr)
    );
    let prefix = "CBTH_TRANSCRIPT_WRITEBACK_V1 ";
    let pending_stdout = String::from_utf8(pending.stdout).expect("utf8 stdout");
    let pending_envelope: Value = serde_json::from_str(pending_stdout.trim_start_matches(prefix))
        .expect("valid pending envelope");
    assert_eq!(pending_envelope["kind"], "arm_pending_requested");
    assert_eq!(pending_envelope["source_thread_id"], "thread-relay");
    assert_eq!(pending_envelope["attempt_id"], "attempt-relay");
    assert_eq!(pending_envelope["generation"], 1);
    assert_eq!(
        pending_envelope["bridge_request_id"],
        "bridge-request-relay"
    );
    assert_eq!(pending_envelope["marker"], "CBTH_TRANSCRIPT_ARM_PENDING");
    assert_eq!(pending_envelope["created_at"], 6500);

    let armed = cbth_output(
        &home,
        &[
            "desktop",
            "validation",
            "emit-transcript-arm",
            "--source-thread-id",
            "thread-relay",
            "--attempt-id",
            "attempt-relay",
            "--generation",
            "1",
            "--bridge-request-id",
            "bridge-request-relay",
            "--bridge-arm-lease-id",
            "lease-relay",
            "--marker",
            "CBTH_TRANSCRIPT_ARM",
            "--json",
            "--now",
            "6510",
        ],
        false,
    );
    assert!(
        armed.status.success(),
        "arm emit failed\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&armed.stdout),
        String::from_utf8_lossy(&armed.stderr)
    );
    let armed_stdout = String::from_utf8(armed.stdout).expect("utf8 stdout");
    let armed_envelope: Value =
        serde_json::from_str(armed_stdout.trim_start_matches(prefix)).expect("valid arm envelope");
    assert_eq!(armed_envelope["kind"], "arm_requested");
    assert_eq!(armed_envelope["bridge_arm_lease_id"], "lease-relay");
    assert_eq!(armed_envelope["marker"], "CBTH_TRANSCRIPT_ARM");
    assert_eq!(armed_envelope["created_at"], 6510);

    assert!(!home.path().join("run").join("startup.lock").exists());
    assert!(!home.path().join("cbth.sqlite3").exists());
    assert!(!home.path().join("inbox").exists());
}

#[test]
fn desktop_transcript_writeback_scan_classifies_rollout_carriers() {
    let home = temp_home();
    let rollout_path = home.path().join("rollout.jsonl");
    let marker = "CBTH_TRANSCRIPT_WRITEBACK_SCAN_TEST";
    let prefix = "CBTH_TRANSCRIPT_WRITEBACK_V1 ";
    let envelope = json!({
        "schema_version": 1,
        "channel": "desktop_transcript_writeback",
        "kind": "validation_probe",
        "bridge_thread_id": "bridge-thread",
        "probe_id": "probe-scan",
        "marker": marker,
        "created_at": 6200,
    });
    let trusted_line = format!("{prefix}{}", serde_json::to_string(&envelope).unwrap());
    let diagnostic_line = format!("{prefix}{}", serde_json::to_string(&envelope).unwrap());
    let prompt_line = format!("{prefix}{}", serde_json::to_string(&envelope).unwrap());
    let records = [
        json!({
            "timestamp": "2026-05-11T00:00:00Z",
            "type": "event_msg",
            "payload": {
                "type": "user_message",
                "message": format!("prompt mentions {marker} and sample {prompt_line}")
            }
        }),
        json!({
            "timestamp": "2026-05-11T00:00:01Z",
            "type": "response_item",
            "payload": {
                "type": "function_call_output",
                "output": format!("Chunk ID: abc\nINFO: quoted {trusted_line}\nOutput:\n{trusted_line}\n")
            }
        }),
        json!({
            "timestamp": "2026-05-11T00:00:02Z",
            "type": "event_msg",
            "payload": {
                "type": "agent_message",
                "message": diagnostic_line
            }
        }),
        json!({
            "timestamp": "2026-05-11T00:00:03Z",
            "type": "event_msg",
            "payload": {
                "type": "task_complete",
                "last_agent_message": format!("{marker} final text mention only")
            }
        }),
    ];
    fs::write(
        &rollout_path,
        records
            .iter()
            .map(|record| serde_json::to_string(record).unwrap())
            .collect::<Vec<_>>()
            .join("\n"),
    )
    .expect("write rollout");

    let scan = cbth(
        &home,
        &[
            "desktop",
            "validation",
            "scan-transcript-writeback",
            "--rollout-path",
            rollout_path.to_str().unwrap(),
            "--marker",
            marker,
            "--json",
        ],
    );
    let scan = &scan["desktop_transcript_writeback_scan"];
    assert_eq!(scan["counts"]["trusted_auto"], 1);
    assert_eq!(scan["counts"]["diagnostic_only"], 2);
    assert_eq!(scan["counts"]["ignored_prompt"], 1);
    assert_eq!(scan["counts"]["rejected"], 0);
    assert_eq!(scan["auto_decision"]["trusted"], true);
    assert_eq!(
        scan["auto_decision"]["reason"],
        "single_trusted_auto_envelope"
    );
    assert_eq!(scan["trusted_auto"][0]["carrier"], "trusted_auto");
    assert_eq!(
        scan["trusted_auto"][0]["envelope"]["marker"],
        "CBTH_TRANSCRIPT_WRITEBACK_SCAN_TEST"
    );
    assert_eq!(scan["ignored_prompt"][0]["carrier"], "ignored_prompt");
    assert!(!home.path().join("run").join("startup.lock").exists());
    assert!(!home.path().join("cbth.sqlite3").exists());
}

#[test]
fn desktop_transcript_writeback_scan_fails_closed_for_unsafe_carriers() {
    let home = temp_home();
    let prefix = "CBTH_TRANSCRIPT_WRITEBACK_V1 ";
    let marker = "CBTH_TRANSCRIPT_WRITEBACK_FAIL_CLOSED";
    let envelope = json!({
        "schema_version": 1,
        "channel": "desktop_transcript_writeback",
        "kind": "validation_probe",
        "bridge_thread_id": "bridge-thread",
        "probe_id": "probe-scan",
        "marker": marker,
        "created_at": 6300,
    });
    let envelope_line = format!("{prefix}{}", serde_json::to_string(&envelope).unwrap());

    let prompt_only_path = home.path().join("prompt-only.jsonl");
    fs::write(
        &prompt_only_path,
        serde_json::to_string(&json!({
            "type": "event_msg",
            "payload": {
                "type": "user_message",
                "message": envelope_line
            }
        }))
        .unwrap(),
    )
    .expect("write prompt-only rollout");
    let prompt_only = cbth(
        &home,
        &[
            "desktop",
            "validation",
            "scan-transcript-writeback",
            "--rollout-path",
            prompt_only_path.to_str().unwrap(),
            "--marker",
            marker,
            "--json",
        ],
    );
    let prompt_only = &prompt_only["desktop_transcript_writeback_scan"];
    assert_eq!(prompt_only["counts"]["ignored_prompt"], 1);
    assert_eq!(prompt_only["counts"]["trusted_auto"], 0);
    assert_eq!(prompt_only["auto_decision"]["trusted"], false);
    assert_eq!(
        prompt_only["auto_decision"]["reason"],
        "no_trusted_auto_envelope"
    );

    let duplicate_path = home.path().join("duplicate.jsonl");
    let duplicate_record = json!({
        "type": "response_item",
        "payload": {
            "type": "function_call_output",
            "output": format!("{envelope_line}\n{envelope_line}\n")
        }
    });
    fs::write(
        &duplicate_path,
        serde_json::to_string(&duplicate_record).unwrap(),
    )
    .expect("write duplicate rollout");
    let duplicate = cbth(
        &home,
        &[
            "desktop",
            "validation",
            "scan-transcript-writeback",
            "--rollout-path",
            duplicate_path.to_str().unwrap(),
            "--marker",
            marker,
            "--json",
        ],
    );
    let duplicate = &duplicate["desktop_transcript_writeback_scan"];
    assert_eq!(duplicate["counts"]["trusted_auto"], 2);
    assert_eq!(duplicate["auto_decision"]["trusted"], false);
    assert_eq!(
        duplicate["auto_decision"]["reason"],
        "duplicate_trusted_auto_envelopes"
    );

    let malformed_path = home.path().join("malformed.jsonl");
    let malformed_record = json!({
        "type": "response_item",
        "payload": {
            "type": "function_call_output",
            "output": format!("{prefix}{{\"marker\":\"{marker}\"")
        }
    });
    fs::write(
        &malformed_path,
        serde_json::to_string(&malformed_record).unwrap(),
    )
    .expect("write malformed rollout");
    let malformed = cbth(
        &home,
        &[
            "desktop",
            "validation",
            "scan-transcript-writeback",
            "--rollout-path",
            malformed_path.to_str().unwrap(),
            "--marker",
            marker,
            "--json",
        ],
    );
    let malformed = &malformed["desktop_transcript_writeback_scan"];
    assert_eq!(malformed["counts"]["trusted_auto"], 0);
    assert_eq!(malformed["counts"]["rejected"], 1);
    assert_eq!(malformed["auto_decision"]["trusted"], false);
    assert_eq!(
        malformed["auto_decision"]["reason"],
        "rejected_trusted_auto_envelopes"
    );

    let diagnostic_malformed_path = home.path().join("diagnostic-malformed.jsonl");
    let diagnostic_malformed_record = json!({
        "type": "event_msg",
        "payload": {
            "type": "agent_message",
            "message": format!("{prefix}{{\"marker\":\"{marker}\"")
        }
    });
    fs::write(
        &diagnostic_malformed_path,
        serde_json::to_string(&diagnostic_malformed_record).unwrap(),
    )
    .expect("write diagnostic malformed rollout");
    let diagnostic_malformed = cbth(
        &home,
        &[
            "desktop",
            "validation",
            "scan-transcript-writeback",
            "--rollout-path",
            diagnostic_malformed_path.to_str().unwrap(),
            "--marker",
            marker,
            "--json",
        ],
    );
    let diagnostic_malformed = &diagnostic_malformed["desktop_transcript_writeback_scan"];
    assert_eq!(diagnostic_malformed["counts"]["trusted_auto"], 0);
    assert_eq!(diagnostic_malformed["counts"]["diagnostic_only"], 1);
    assert_eq!(diagnostic_malformed["counts"]["rejected"], 0);
    assert_eq!(diagnostic_malformed["auto_decision"]["trusted"], false);
    assert_eq!(
        diagnostic_malformed["auto_decision"]["reason"],
        "no_trusted_auto_envelope"
    );

    let wrong_marker_path = home.path().join("wrong-marker.jsonl");
    let wrong_marker = json!({
        "schema_version": 1,
        "channel": "desktop_transcript_writeback",
        "kind": "validation_probe",
        "bridge_thread_id": "bridge-thread",
        "probe_id": "probe-scan",
        "marker": "OTHER_MARKER",
        "created_at": 6400,
    });
    let wrong_marker_record = json!({
        "type": "response_item",
        "payload": {
            "type": "function_call_output",
            "output": format!("{prefix}{}", serde_json::to_string(&wrong_marker).unwrap())
        }
    });
    fs::write(
        &wrong_marker_path,
        serde_json::to_string(&wrong_marker_record).unwrap(),
    )
    .expect("write wrong-marker rollout");
    let wrong_marker = cbth(
        &home,
        &[
            "desktop",
            "validation",
            "scan-transcript-writeback",
            "--rollout-path",
            wrong_marker_path.to_str().unwrap(),
            "--marker",
            marker,
            "--json",
        ],
    );
    let wrong_marker = &wrong_marker["desktop_transcript_writeback_scan"];
    assert_eq!(wrong_marker["counts"]["trusted_auto"], 0);
    assert_eq!(wrong_marker["counts"]["rejected"], 0);
    assert_eq!(wrong_marker["auto_decision"]["trusted"], false);
    assert_eq!(
        wrong_marker["auto_decision"]["reason"],
        "no_trusted_auto_envelope"
    );
}

#[test]
fn desktop_transcript_relay_consumer_drives_arm_cas_and_replay_fence() {
    let home = temp_home();
    let fixture = cbth(
        &home,
        &[
            "desktop",
            "validation",
            "prepare-writeback-fixture",
            "--source-thread-id",
            "thread-relay-consumer",
            "--caller-automation-id",
            "automation-relay-consumer",
            "--bridge-request-id",
            "bridge-request-relay-consumer",
            "--now",
            "7000",
            "--json",
        ],
    );
    let fixture = &fixture["desktop_writeback_fixture"];
    let attempt_id = fixture["attempt"]["attempt_id"].as_str().unwrap();
    let batch_id = fixture["batch"]["batch_id"].as_str().unwrap();

    let pending_marker = issue_desktop_relay_marker(
        &home,
        "bridge-relay-consumer",
        "arm-pending",
        "thread-relay-consumer",
        attempt_id,
        1,
        "bridge-request-relay-consumer",
        7005,
    );
    let pending_emit = cbth_output(
        &home,
        &[
            "desktop",
            "validation",
            "emit-transcript-arm-pending",
            "--source-thread-id",
            "thread-relay-consumer",
            "--attempt-id",
            attempt_id,
            "--generation",
            "1",
            "--bridge-request-id",
            "bridge-request-relay-consumer",
            "--marker",
            &pending_marker,
            "--json",
            "--now",
            "7010",
        ],
        false,
    );
    assert!(
        pending_emit.status.success(),
        "pending emit failed\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&pending_emit.stdout),
        String::from_utf8_lossy(&pending_emit.stderr)
    );
    let pending_stdout = String::from_utf8(pending_emit.stdout).unwrap();
    let pending_rollout = home.path().join("pending-rollout.jsonl");
    write_function_call_rollout(&pending_rollout, &pending_stdout);

    let pending = cbth(
        &home,
        &[
            "desktop",
            "relay",
            "consume-transcript",
            "--rollout-path",
            pending_rollout.to_str().unwrap(),
            "--marker",
            &pending_marker,
            "--json",
            "--now",
            "7020",
        ],
    );
    let pending = &pending["desktop_transcript_relay_consumption"];
    assert_eq!(pending["record"]["replay_state"], "fresh");
    assert_eq!(pending["record"]["envelope_kind"], "arm_pending_requested");
    assert_eq!(pending["record"]["outcome"]["outcome"], "arm_pending");
    assert_eq!(
        pending["record"]["outcome"]["attempt"]["state"],
        "arm_pending"
    );
    let lease_id = pending["record"]["outcome"]["bridge_arm_lease_id"]
        .as_str()
        .unwrap()
        .to_owned();

    let repeated_pending = cbth(
        &home,
        &[
            "desktop",
            "relay",
            "consume-transcript",
            "--rollout-path",
            pending_rollout.to_str().unwrap(),
            "--marker",
            &pending_marker,
            "--json",
            "--now",
            "7021",
        ],
    );
    assert_eq!(
        repeated_pending["desktop_transcript_relay_consumption"]["record"]["replay_state"],
        "replayed"
    );
    assert_eq!(
        repeated_pending["desktop_transcript_relay_consumption"]["record"]["outcome"]["bridge_arm_lease_id"],
        lease_id
    );

    let conflicting_emit = cbth_output(
        &home,
        &[
            "desktop",
            "validation",
            "emit-transcript-arm-pending",
            "--source-thread-id",
            "thread-relay-consumer",
            "--attempt-id",
            attempt_id,
            "--generation",
            "1",
            "--bridge-request-id",
            "bridge-request-relay-consumer",
            "--marker",
            &pending_marker,
            "--json",
            "--now",
            "7022",
        ],
        false,
    );
    assert!(conflicting_emit.status.success());
    let conflicting_rollout = home.path().join("pending-conflict-rollout.jsonl");
    write_function_call_rollout(
        &conflicting_rollout,
        &String::from_utf8(conflicting_emit.stdout).unwrap(),
    );
    let conflict = cbth_failure(
        &home,
        &[
            "desktop",
            "relay",
            "consume-transcript",
            "--rollout-path",
            conflicting_rollout.to_str().unwrap(),
            "--marker",
            &pending_marker,
            "--json",
            "--now",
            "7023",
        ],
    );
    assert!(conflict.contains("already consumed with another envelope hash"));

    let arm_marker = issue_desktop_relay_marker(
        &home,
        "bridge-relay-consumer",
        "arm-accepted",
        "thread-relay-consumer",
        attempt_id,
        1,
        "bridge-request-relay-consumer",
        7025,
    );
    let arm_emit = cbth_output(
        &home,
        &[
            "desktop",
            "validation",
            "emit-transcript-arm",
            "--source-thread-id",
            "thread-relay-consumer",
            "--attempt-id",
            attempt_id,
            "--generation",
            "1",
            "--bridge-request-id",
            "bridge-request-relay-consumer",
            "--bridge-arm-lease-id",
            &lease_id,
            "--marker",
            &arm_marker,
            "--json",
            "--now",
            "7030",
        ],
        false,
    );
    assert!(arm_emit.status.success());
    let arm_rollout = home.path().join("arm-rollout.jsonl");
    write_function_call_rollout(&arm_rollout, &String::from_utf8(arm_emit.stdout).unwrap());
    let armed = cbth(
        &home,
        &[
            "desktop",
            "relay",
            "consume-transcript",
            "--rollout-path",
            arm_rollout.to_str().unwrap(),
            "--marker",
            &arm_marker,
            "--json",
            "--now",
            "7040",
        ],
    );
    let armed = &armed["desktop_transcript_relay_consumption"];
    assert_eq!(armed["record"]["replay_state"], "fresh");
    assert_eq!(armed["record"]["envelope_kind"], "arm_requested");
    assert_eq!(armed["record"]["outcome"]["outcome"], "armed");
    assert_eq!(armed["record"]["outcome"]["attempt"]["state"], "cooldown");
    assert_eq!(armed["record"]["outcome"]["delivery_attempt_count"], 1);

    let repeated_arm = cbth(
        &home,
        &[
            "desktop",
            "relay",
            "consume-transcript",
            "--rollout-path",
            arm_rollout.to_str().unwrap(),
            "--marker",
            &arm_marker,
            "--json",
            "--now",
            "7041",
        ],
    );
    assert_eq!(
        repeated_arm["desktop_transcript_relay_consumption"]["record"]["replay_state"],
        "replayed"
    );
    let inspected = cbth(&home, &["batch", "inspect", "--batch-id", batch_id]);
    assert_eq!(inspected["batch"]["batch"]["delivery_attempt_count"], 1);
}

#[test]
fn desktop_transcript_relay_consumer_rejects_stale_binding_marker() {
    let home = temp_home();
    let fixture = cbth(
        &home,
        &[
            "desktop",
            "validation",
            "prepare-writeback-fixture",
            "--source-thread-id",
            "thread-relay-consumer-stale-binding",
            "--caller-automation-id",
            "automation-relay-consumer-stale-binding",
            "--bridge-request-id",
            "bridge-request-relay-consumer-stale-binding",
            "--now",
            "7060",
            "--json",
        ],
    );
    let fixture = &fixture["desktop_writeback_fixture"];
    let attempt_id = fixture["attempt"]["attempt_id"].as_str().unwrap();
    let marker = issue_desktop_relay_marker(
        &home,
        "bridge-relay-consumer-stale-binding",
        "arm-pending",
        "thread-relay-consumer-stale-binding",
        attempt_id,
        1,
        "bridge-request-relay-consumer-stale-binding",
        7061,
    );

    let pending_emit = cbth_output(
        &home,
        &[
            "desktop",
            "validation",
            "emit-transcript-arm-pending",
            "--source-thread-id",
            "thread-relay-consumer-stale-binding",
            "--attempt-id",
            attempt_id,
            "--generation",
            "1",
            "--bridge-request-id",
            "bridge-request-relay-consumer-stale-binding",
            "--marker",
            &marker,
            "--json",
            "--now",
            "7062",
        ],
        false,
    );
    assert!(pending_emit.status.success());
    let rollout = home.path().join("pending-stale-binding-rollout.jsonl");
    write_function_call_rollout(&rollout, &String::from_utf8(pending_emit.stdout).unwrap());

    let conn = Connection::open(home.path().join("cbth.sqlite3")).expect("open db");
    conn.execute(
        "UPDATE desktop_bindings
         SET caller_automation_id = ?
         WHERE source_thread_id = ?",
        params![
            "automation-relay-consumer-stale-binding-repaired",
            "thread-relay-consumer-stale-binding"
        ],
    )
    .expect("simulate Desktop binding repair after marker issue");
    drop(conn);

    let stale = cbth_failure(
        &home,
        &[
            "desktop",
            "relay",
            "consume-transcript",
            "--rollout-path",
            rollout.to_str().unwrap(),
            "--marker",
            &marker,
            "--json",
            "--now",
            "7063",
        ],
    );
    assert!(stale.contains("changed since transcript relay marker"));

    let attempt = cbth(&home, &["attempt", "inspect", "--attempt-id", attempt_id]);
    assert_eq!(attempt["attempt"]["state"], "prepared");
    let conn = Connection::open(home.path().join("cbth.sqlite3")).expect("open db");
    let consumptions: i64 = conn
        .query_row(
            "SELECT COUNT(*)
             FROM desktop_transcript_relay_consumptions
             WHERE marker = ?",
            params![marker],
            |row| row.get(0),
        )
        .expect("count relay consumptions");
    assert_eq!(consumptions, 0);
}

#[test]
fn desktop_transcript_relay_scanner_consumes_issued_markers_to_cooldown() {
    let home = temp_home();
    let fixture = cbth(
        &home,
        &[
            "desktop",
            "validation",
            "prepare-writeback-fixture",
            "--source-thread-id",
            "thread-relay-scanner",
            "--caller-automation-id",
            "automation-relay-scanner",
            "--bridge-request-id",
            "bridge-request-relay-scanner",
            "--now",
            "7600",
            "--json",
        ],
    );
    let fixture = &fixture["desktop_writeback_fixture"];
    let attempt_id = fixture["attempt"]["attempt_id"].as_str().unwrap();
    let batch_id = fixture["batch"]["batch_id"].as_str().unwrap();
    let rollout = home.path().join("scanner-rollout.jsonl");
    fs::write(&rollout, "").expect("create rollout");

    let bind = cbth(
        &home,
        &[
            "desktop",
            "relay",
            "scanner",
            "bind",
            "--bridge-thread-id",
            "bridge-relay-scanner",
            "--rollout-path",
            rollout.to_str().unwrap(),
            "--from-start",
            "--json",
            "--now",
            "7601",
        ],
    );
    assert_eq!(
        bind["desktop_relay_scanner_binding"]["bridge_thread_id"],
        "bridge-relay-scanner"
    );
    assert!(!home.path().join("run").join("startup.lock").exists());

    let pending_marker = cbth(
        &home,
        &[
            "desktop",
            "relay",
            "marker",
            "issue",
            "--bridge-thread-id",
            "bridge-relay-scanner",
            "--kind",
            "arm-pending",
            "--source-thread-id",
            "thread-relay-scanner",
            "--attempt-id",
            attempt_id,
            "--generation",
            "1",
            "--bridge-request-id",
            "bridge-request-relay-scanner",
            "--json",
            "--now",
            "7610",
        ],
    );
    let pending_marker = pending_marker["desktop_transcript_relay_marker"]["record"]["marker"]
        .as_str()
        .unwrap()
        .to_owned();
    let pending_emit = cbth_output(
        &home,
        &[
            "desktop",
            "relay",
            "emit-arm-pending",
            "--source-thread-id",
            "thread-relay-scanner",
            "--attempt-id",
            attempt_id,
            "--generation",
            "1",
            "--bridge-request-id",
            "bridge-request-relay-scanner",
            "--marker",
            &pending_marker,
            "--json",
            "--now",
            "7611",
        ],
        false,
    );
    assert!(pending_emit.status.success());
    append_function_call_rollout_line(&rollout, &String::from_utf8(pending_emit.stdout).unwrap());

    let pending_scan = cbth(
        &home,
        &[
            "desktop",
            "relay",
            "scanner",
            "scan-once",
            "--bridge-thread-id",
            "bridge-relay-scanner",
            "--json",
            "--now",
            "7620",
        ],
    );
    let pending_scan = &pending_scan["desktop_relay_scanner_scan"];
    assert_eq!(pending_scan["consumed_markers"], 1);
    assert_eq!(
        pending_scan["bindings"][0]["consumed_markers"][0]["consumption"]["outcome"]["outcome"],
        "arm_pending"
    );
    let attempt = cbth(&home, &["attempt", "inspect", "--attempt-id", attempt_id]);
    assert_eq!(attempt["attempt"]["state"], "arm_pending");

    let arm_marker = cbth(
        &home,
        &[
            "desktop",
            "relay",
            "marker",
            "issue",
            "--bridge-thread-id",
            "bridge-relay-scanner",
            "--kind",
            "arm-accepted",
            "--source-thread-id",
            "thread-relay-scanner",
            "--attempt-id",
            attempt_id,
            "--generation",
            "1",
            "--bridge-request-id",
            "bridge-request-relay-scanner",
            "--json",
            "--now",
            "7630",
        ],
    );
    let arm_marker = arm_marker["desktop_transcript_relay_marker"]["record"]["marker"]
        .as_str()
        .unwrap()
        .to_owned();
    let arm_emit = cbth_output(
        &home,
        &[
            "desktop",
            "relay",
            "emit-arm-accepted",
            "--source-thread-id",
            "thread-relay-scanner",
            "--attempt-id",
            attempt_id,
            "--generation",
            "1",
            "--bridge-request-id",
            "bridge-request-relay-scanner",
            "--marker",
            &arm_marker,
            "--json",
            "--now",
            "7631",
        ],
        false,
    );
    assert!(arm_emit.status.success());
    let arm_stdout = String::from_utf8(arm_emit.stdout).unwrap();
    assert!(arm_stdout.contains("\"kind\":\"arm_accepted\""));
    assert!(!arm_stdout.contains("bridge_arm_lease_id"));
    append_function_call_rollout_line(&rollout, &arm_stdout);

    let arm_scan = cbth(
        &home,
        &[
            "desktop",
            "relay",
            "scanner",
            "scan-once",
            "--bridge-thread-id",
            "bridge-relay-scanner",
            "--json",
            "--now",
            "7640",
        ],
    );
    let arm_scan = &arm_scan["desktop_relay_scanner_scan"];
    assert_eq!(arm_scan["consumed_markers"], 1);
    assert_eq!(
        arm_scan["bindings"][0]["consumed_markers"][0]["consumption"]["outcome"]["outcome"],
        "armed"
    );
    let inspected_attempt = cbth(&home, &["attempt", "inspect", "--attempt-id", attempt_id]);
    assert_eq!(inspected_attempt["attempt"]["state"], "cooldown");
    let inspected_batch = cbth(&home, &["batch", "inspect", "--batch-id", batch_id]);
    assert_eq!(
        inspected_batch["batch"]["batch"]["delivery_attempt_count"],
        1
    );

    let repeat_scan = cbth(
        &home,
        &[
            "desktop",
            "relay",
            "scanner",
            "scan-once",
            "--bridge-thread-id",
            "bridge-relay-scanner",
            "--json",
            "--now",
            "7641",
        ],
    );
    assert_eq!(
        repeat_scan["desktop_relay_scanner_scan"]["scanned_bindings"],
        0
    );
    let repeated_batch = cbth(&home, &["batch", "inspect", "--batch-id", batch_id]);
    assert_eq!(
        repeated_batch["batch"]["batch"]["delivery_attempt_count"],
        1
    );
}

#[test]
fn desktop_transcript_relay_scanner_rejects_arm_accepted_with_embedded_lease() {
    let home = temp_home();
    let fixture = cbth(
        &home,
        &[
            "desktop",
            "validation",
            "prepare-writeback-fixture",
            "--source-thread-id",
            "thread-relay-scanner-accepted-lease",
            "--caller-automation-id",
            "automation-relay-scanner-accepted-lease",
            "--bridge-request-id",
            "bridge-request-relay-scanner-accepted-lease",
            "--now",
            "7645",
            "--json",
        ],
    );
    let fixture = &fixture["desktop_writeback_fixture"];
    let attempt_id = fixture["attempt"]["attempt_id"].as_str().unwrap();
    let rollout = home.path().join("scanner-accepted-lease-rollout.jsonl");
    fs::write(&rollout, "").expect("create rollout");
    cbth(
        &home,
        &[
            "desktop",
            "relay",
            "scanner",
            "bind",
            "--bridge-thread-id",
            "bridge-relay-scanner-accepted-lease",
            "--rollout-path",
            rollout.to_str().unwrap(),
            "--from-start",
            "--json",
            "--now",
            "7646",
        ],
    );
    let pending = cbth(
        &home,
        &[
            "desktop",
            "note-arm-pending",
            "--source-thread-id",
            "thread-relay-scanner-accepted-lease",
            "--attempt-id",
            attempt_id,
            "--generation",
            "1",
            "--bridge-request-id",
            "bridge-request-relay-scanner-accepted-lease",
            "--json",
            "--now",
            "7647",
        ],
    );
    let lease_id = pending["desktop_arm_pending"]["bridge_arm_lease_id"]
        .as_str()
        .unwrap();
    let arm_marker = cbth(
        &home,
        &[
            "desktop",
            "relay",
            "marker",
            "issue",
            "--bridge-thread-id",
            "bridge-relay-scanner-accepted-lease",
            "--kind",
            "arm-accepted",
            "--source-thread-id",
            "thread-relay-scanner-accepted-lease",
            "--attempt-id",
            attempt_id,
            "--generation",
            "1",
            "--bridge-request-id",
            "bridge-request-relay-scanner-accepted-lease",
            "--json",
            "--now",
            "7648",
        ],
    );
    let arm_marker = arm_marker["desktop_transcript_relay_marker"]["record"]["marker"]
        .as_str()
        .unwrap();
    let envelope = json!({
        "schema_version": 1,
        "channel": "desktop_transcript_writeback",
        "kind": "arm_accepted",
        "source_thread_id": "thread-relay-scanner-accepted-lease",
        "attempt_id": attempt_id,
        "generation": 1,
        "bridge_request_id": "bridge-request-relay-scanner-accepted-lease",
        "marker": arm_marker,
        "bridge_arm_lease_id": lease_id,
        "created_at": 7649,
    });
    append_function_call_rollout_line(
        &rollout,
        &format!(
            "CBTH_TRANSCRIPT_WRITEBACK_V1 {}",
            serde_json::to_string(&envelope).unwrap()
        ),
    );

    let scan = cbth(
        &home,
        &[
            "desktop",
            "relay",
            "scanner",
            "scan-once",
            "--bridge-thread-id",
            "bridge-relay-scanner-accepted-lease",
            "--json",
            "--now",
            "7650",
        ],
    );
    let scan = &scan["desktop_relay_scanner_scan"];
    assert_eq!(scan["consumed_markers"], 0);
    assert_eq!(scan["rejected_markers"], 1);
    assert!(
        scan["bindings"][0]["rejected_markers"][0]["reason"]
            .as_str()
            .unwrap()
            .contains("bridge_arm_lease_id is not allowed for arm_accepted")
    );
    let attempt = cbth(&home, &["attempt", "inspect", "--attempt-id", attempt_id]);
    assert_eq!(attempt["attempt"]["state"], "arm_pending");
}

#[test]
fn desktop_transcript_relay_scanner_expired_arm_accepted_abandons_attempt() {
    let home = temp_home();
    let fixture = cbth(
        &home,
        &[
            "desktop",
            "validation",
            "prepare-writeback-fixture",
            "--source-thread-id",
            "thread-relay-scanner-expired-arm",
            "--caller-automation-id",
            "automation-relay-scanner-expired-arm",
            "--bridge-request-id",
            "bridge-request-relay-scanner-expired-arm",
            "--now",
            "7650",
            "--json",
        ],
    );
    let attempt_id = fixture["desktop_writeback_fixture"]["attempt"]["attempt_id"]
        .as_str()
        .unwrap();
    let rollout = home.path().join("scanner-expired-arm-rollout.jsonl");
    fs::write(&rollout, "").expect("create rollout");
    cbth(
        &home,
        &[
            "desktop",
            "relay",
            "scanner",
            "bind",
            "--bridge-thread-id",
            "bridge-relay-scanner-expired-arm",
            "--rollout-path",
            rollout.to_str().unwrap(),
            "--from-start",
            "--json",
            "--now",
            "7651",
        ],
    );

    let pending_marker = cbth(
        &home,
        &[
            "desktop",
            "relay",
            "marker",
            "issue",
            "--bridge-thread-id",
            "bridge-relay-scanner-expired-arm",
            "--kind",
            "arm-pending",
            "--source-thread-id",
            "thread-relay-scanner-expired-arm",
            "--attempt-id",
            attempt_id,
            "--generation",
            "1",
            "--bridge-request-id",
            "bridge-request-relay-scanner-expired-arm",
            "--json",
            "--now",
            "7660",
        ],
    );
    let pending_marker = pending_marker["desktop_transcript_relay_marker"]["record"]["marker"]
        .as_str()
        .unwrap()
        .to_owned();
    let pending_emit = cbth_output(
        &home,
        &[
            "desktop",
            "relay",
            "emit-arm-pending",
            "--source-thread-id",
            "thread-relay-scanner-expired-arm",
            "--attempt-id",
            attempt_id,
            "--generation",
            "1",
            "--bridge-request-id",
            "bridge-request-relay-scanner-expired-arm",
            "--marker",
            &pending_marker,
            "--json",
            "--now",
            "7661",
        ],
        false,
    );
    append_function_call_rollout_line(&rollout, &String::from_utf8(pending_emit.stdout).unwrap());
    let pending_scan = cbth(
        &home,
        &[
            "desktop",
            "relay",
            "scanner",
            "scan-once",
            "--bridge-thread-id",
            "bridge-relay-scanner-expired-arm",
            "--json",
            "--now",
            "7670",
        ],
    );
    let lease_deadline = pending_scan["desktop_relay_scanner_scan"]["bindings"][0]
        ["consumed_markers"][0]["consumption"]["outcome"]["bridge_arm_lease_deadline"]
        .as_i64()
        .unwrap();

    let arm_marker = cbth(
        &home,
        &[
            "desktop",
            "relay",
            "marker",
            "issue",
            "--bridge-thread-id",
            "bridge-relay-scanner-expired-arm",
            "--kind",
            "arm-accepted",
            "--source-thread-id",
            "thread-relay-scanner-expired-arm",
            "--attempt-id",
            attempt_id,
            "--generation",
            "1",
            "--bridge-request-id",
            "bridge-request-relay-scanner-expired-arm",
            "--json",
            "--now",
            "7680",
        ],
    );
    let arm_marker = arm_marker["desktop_transcript_relay_marker"]["record"]["marker"]
        .as_str()
        .unwrap()
        .to_owned();
    let arm_emit = cbth_output(
        &home,
        &[
            "desktop",
            "relay",
            "emit-arm-accepted",
            "--source-thread-id",
            "thread-relay-scanner-expired-arm",
            "--attempt-id",
            attempt_id,
            "--generation",
            "1",
            "--bridge-request-id",
            "bridge-request-relay-scanner-expired-arm",
            "--marker",
            &arm_marker,
            "--json",
            "--now",
            "7681",
        ],
        false,
    );
    append_function_call_rollout_line(&rollout, &String::from_utf8(arm_emit.stdout).unwrap());

    let expired_now = (lease_deadline + 1).to_string();
    let expired_scan = cbth(
        &home,
        &[
            "desktop",
            "relay",
            "scanner",
            "scan-once",
            "--bridge-thread-id",
            "bridge-relay-scanner-expired-arm",
            "--json",
            "--now",
            &expired_now,
        ],
    );
    let expired_scan = &expired_scan["desktop_relay_scanner_scan"];
    assert_eq!(expired_scan["consumed_markers"], 0);
    assert_eq!(expired_scan["rejected_markers"], 0);
    assert_eq!(expired_scan["expired_markers"], 1);
    cbth(&home, &["maintenance", "sweep", "--now", &expired_now]);
    let attempt = cbth(&home, &["attempt", "inspect", "--attempt-id", attempt_id]);
    assert_eq!(attempt["attempt"]["state"], "abandoned");
}

#[test]
fn desktop_transcript_relay_marker_issue_rejects_arm_accepted_before_pending() {
    let home = temp_home();
    let fixture = cbth(
        &home,
        &[
            "desktop",
            "validation",
            "prepare-writeback-fixture",
            "--source-thread-id",
            "thread-relay-scanner-premature-arm",
            "--caller-automation-id",
            "automation-relay-scanner-premature-arm",
            "--bridge-request-id",
            "bridge-request-relay-scanner-premature-arm",
            "--now",
            "7690",
            "--json",
        ],
    );
    let fixture = &fixture["desktop_writeback_fixture"];
    let attempt_id = fixture["attempt"]["attempt_id"].as_str().unwrap();
    let rollout = home.path().join("scanner-premature-arm-rollout.jsonl");
    fs::write(&rollout, "").expect("create rollout");
    cbth(
        &home,
        &[
            "desktop",
            "relay",
            "scanner",
            "bind",
            "--bridge-thread-id",
            "bridge-relay-scanner-premature-arm",
            "--rollout-path",
            rollout.to_str().unwrap(),
            "--from-start",
            "--json",
            "--now",
            "7691",
        ],
    );

    let error = cbth_failure(
        &home,
        &[
            "desktop",
            "relay",
            "marker",
            "issue",
            "--bridge-thread-id",
            "bridge-relay-scanner-premature-arm",
            "--kind",
            "arm-accepted",
            "--source-thread-id",
            "thread-relay-scanner-premature-arm",
            "--attempt-id",
            attempt_id,
            "--generation",
            "1",
            "--bridge-request-id",
            "bridge-request-relay-scanner-premature-arm",
            "--json",
            "--now",
            "7692",
        ],
    );
    assert!(error.contains("not arm_pending for Desktop arm-accepted marker issuance"));
    let attempt = cbth(&home, &["attempt", "inspect", "--attempt-id", attempt_id]);
    assert_eq!(attempt["attempt"]["state"], "prepared");
}

#[test]
fn desktop_transcript_relay_scanner_defers_partial_lines_and_dedupes_retries() {
    let home = temp_home();
    let fixture = cbth(
        &home,
        &[
            "desktop",
            "validation",
            "prepare-writeback-fixture",
            "--source-thread-id",
            "thread-relay-scanner-partial",
            "--caller-automation-id",
            "automation-relay-scanner-partial",
            "--bridge-request-id",
            "bridge-request-relay-scanner-partial",
            "--now",
            "7700",
            "--json",
        ],
    );
    let fixture = &fixture["desktop_writeback_fixture"];
    let attempt_id = fixture["attempt"]["attempt_id"].as_str().unwrap();
    let rollout = home.path().join("scanner-partial-rollout.jsonl");
    fs::write(&rollout, "").expect("create rollout");
    cbth(
        &home,
        &[
            "desktop",
            "relay",
            "scanner",
            "bind",
            "--bridge-thread-id",
            "bridge-relay-scanner-partial",
            "--rollout-path",
            rollout.to_str().unwrap(),
            "--from-start",
            "--json",
            "--now",
            "7701",
        ],
    );
    let partial_marker = cbth(
        &home,
        &[
            "desktop",
            "relay",
            "marker",
            "issue",
            "--bridge-thread-id",
            "bridge-relay-scanner-partial",
            "--kind",
            "arm-pending",
            "--source-thread-id",
            "thread-relay-scanner-partial",
            "--attempt-id",
            attempt_id,
            "--generation",
            "1",
            "--bridge-request-id",
            "bridge-request-relay-scanner-partial",
            "--json",
            "--now",
            "7710",
        ],
    );
    let partial_marker = partial_marker["desktop_transcript_relay_marker"]["record"]["marker"]
        .as_str()
        .unwrap()
        .to_owned();
    let partial_emit = cbth_output(
        &home,
        &[
            "desktop",
            "relay",
            "emit-arm-pending",
            "--source-thread-id",
            "thread-relay-scanner-partial",
            "--attempt-id",
            attempt_id,
            "--generation",
            "1",
            "--bridge-request-id",
            "bridge-request-relay-scanner-partial",
            "--marker",
            &partial_marker,
            "--json",
            "--now",
            "7711",
        ],
        false,
    );
    assert!(partial_emit.status.success());
    let record = json!({
        "type": "response_item",
        "payload": {
            "type": "function_call_output",
            "output": String::from_utf8(partial_emit.stdout).unwrap(),
        }
    });
    fs::write(&rollout, serde_json::to_string(&record).unwrap()).expect("write partial rollout");
    let partial_scan = cbth(
        &home,
        &[
            "desktop",
            "relay",
            "scanner",
            "scan-once",
            "--bridge-thread-id",
            "bridge-relay-scanner-partial",
            "--json",
            "--now",
            "7720",
        ],
    );
    assert_eq!(
        partial_scan["desktop_relay_scanner_scan"]["bindings"][0]["lines_read"],
        0
    );
    let attempt = cbth(&home, &["attempt", "inspect", "--attempt-id", attempt_id]);
    assert_eq!(attempt["attempt"]["state"], "prepared");

    {
        let mut file = fs::OpenOptions::new()
            .append(true)
            .open(&rollout)
            .expect("open partial rollout");
        writeln!(file).expect("finish partial line");
    }
    let completed_scan = cbth(
        &home,
        &[
            "desktop",
            "relay",
            "scanner",
            "scan-once",
            "--bridge-thread-id",
            "bridge-relay-scanner-partial",
            "--json",
            "--now",
            "7721",
        ],
    );
    assert_eq!(
        completed_scan["desktop_relay_scanner_scan"]["consumed_markers"],
        1
    );

    let duplicate_fixture = cbth(
        &home,
        &[
            "desktop",
            "validation",
            "prepare-writeback-fixture",
            "--source-thread-id",
            "thread-relay-scanner-duplicate",
            "--caller-automation-id",
            "automation-relay-scanner-duplicate",
            "--bridge-request-id",
            "bridge-request-relay-scanner-duplicate",
            "--now",
            "7730",
            "--json",
        ],
    );
    let duplicate_attempt = duplicate_fixture["desktop_writeback_fixture"]["attempt"]["attempt_id"]
        .as_str()
        .unwrap();
    let duplicate_rollout = home.path().join("scanner-duplicate-rollout.jsonl");
    fs::write(&duplicate_rollout, "").expect("create duplicate rollout");
    cbth(
        &home,
        &[
            "desktop",
            "relay",
            "scanner",
            "bind",
            "--bridge-thread-id",
            "bridge-relay-scanner-duplicate",
            "--rollout-path",
            duplicate_rollout.to_str().unwrap(),
            "--from-start",
            "--json",
            "--now",
            "7731",
        ],
    );
    let duplicate_marker = cbth(
        &home,
        &[
            "desktop",
            "relay",
            "marker",
            "issue",
            "--bridge-thread-id",
            "bridge-relay-scanner-duplicate",
            "--kind",
            "arm-pending",
            "--source-thread-id",
            "thread-relay-scanner-duplicate",
            "--attempt-id",
            duplicate_attempt,
            "--generation",
            "1",
            "--bridge-request-id",
            "bridge-request-relay-scanner-duplicate",
            "--json",
            "--now",
            "7740",
        ],
    );
    let duplicate_marker = duplicate_marker["desktop_transcript_relay_marker"]["record"]["marker"]
        .as_str()
        .unwrap()
        .to_owned();
    let first = cbth_output(
        &home,
        &[
            "desktop",
            "relay",
            "emit-arm-pending",
            "--source-thread-id",
            "thread-relay-scanner-duplicate",
            "--attempt-id",
            duplicate_attempt,
            "--generation",
            "1",
            "--bridge-request-id",
            "bridge-request-relay-scanner-duplicate",
            "--marker",
            &duplicate_marker,
            "--json",
            "--now",
            "7741",
        ],
        false,
    );
    let first_stdout = String::from_utf8(first.stdout).unwrap();
    let output = format!("{}{}", first_stdout, first_stdout);
    append_function_call_rollout_line(&duplicate_rollout, &output);
    let duplicate_scan = cbth(
        &home,
        &[
            "desktop",
            "relay",
            "scanner",
            "scan-once",
            "--bridge-thread-id",
            "bridge-relay-scanner-duplicate",
            "--json",
            "--now",
            "7750",
        ],
    );
    let duplicate_scan = &duplicate_scan["desktop_relay_scanner_scan"];
    assert_eq!(duplicate_scan["consumed_markers"], 1);
    assert_eq!(duplicate_scan["rejected_markers"], 0);
    let duplicate_attempt_state = cbth(
        &home,
        &["attempt", "inspect", "--attempt-id", duplicate_attempt],
    );
    assert_eq!(duplicate_attempt_state["attempt"]["state"], "arm_pending");
}

#[test]
fn desktop_transcript_relay_scanner_degrades_marker_evidence_before_tick_eof() {
    let home = temp_home();
    let fixture = cbth(
        &home,
        &[
            "desktop",
            "validation",
            "prepare-writeback-fixture",
            "--source-thread-id",
            "thread-relay-scanner-before-eof",
            "--caller-automation-id",
            "automation-relay-scanner-before-eof",
            "--bridge-request-id",
            "bridge-request-relay-scanner-before-eof",
            "--now",
            "7780",
            "--json",
        ],
    );
    let attempt_id = fixture["desktop_writeback_fixture"]["attempt"]["attempt_id"]
        .as_str()
        .unwrap();
    let rollout = home.path().join("scanner-before-eof-rollout.jsonl");
    fs::write(&rollout, "").expect("create before-eof rollout");
    cbth(
        &home,
        &[
            "desktop",
            "relay",
            "scanner",
            "bind",
            "--bridge-thread-id",
            "bridge-relay-scanner-before-eof",
            "--rollout-path",
            rollout.to_str().unwrap(),
            "--from-start",
            "--json",
            "--now",
            "7781",
        ],
    );
    let marker = cbth(
        &home,
        &[
            "desktop",
            "relay",
            "marker",
            "issue",
            "--bridge-thread-id",
            "bridge-relay-scanner-before-eof",
            "--kind",
            "arm-pending",
            "--source-thread-id",
            "thread-relay-scanner-before-eof",
            "--attempt-id",
            attempt_id,
            "--generation",
            "1",
            "--bridge-request-id",
            "bridge-request-relay-scanner-before-eof",
            "--json",
            "--now",
            "7782",
        ],
    );
    let marker = marker["desktop_transcript_relay_marker"]["record"]["marker"]
        .as_str()
        .unwrap()
        .to_owned();
    let emit = cbth_output(
        &home,
        &[
            "desktop",
            "relay",
            "emit-arm-pending",
            "--source-thread-id",
            "thread-relay-scanner-before-eof",
            "--attempt-id",
            attempt_id,
            "--generation",
            "1",
            "--bridge-request-id",
            "bridge-request-relay-scanner-before-eof",
            "--marker",
            &marker,
            "--json",
            "--now",
            "7783",
        ],
        false,
    );
    assert!(emit.status.success());
    append_function_call_rollout_line(&rollout, String::from_utf8(emit.stdout).unwrap().as_str());
    {
        let mut file = fs::OpenOptions::new()
            .append(true)
            .open(&rollout)
            .expect("open before-eof rollout");
        for index in 0..256 {
            writeln!(file, "{}", json!({ "type": "noop", "index": index }))
                .expect("append filler line");
        }
    }

    let scan = cbth(
        &home,
        &[
            "desktop",
            "relay",
            "scanner",
            "scan-once",
            "--bridge-thread-id",
            "bridge-relay-scanner-before-eof",
            "--json",
            "--now",
            "7784",
        ],
    );
    let binding_report = &scan["desktop_relay_scanner_scan"]["bindings"][0];
    assert_eq!(binding_report["outcome"], "degraded");
    assert!(
        binding_report["reason"]
            .as_str()
            .unwrap()
            .contains("before scanner reached tick-start EOF")
    );
    assert_eq!(binding_report["binding"]["binding_state"], "degraded");
    let attempt = cbth(&home, &["attempt", "inspect", "--attempt-id", attempt_id]);
    assert_eq!(attempt["attempt"]["state"], "prepared");
}

#[test]
fn desktop_transcript_relay_scanner_rejects_marker_mention_without_matching_envelope() {
    let home = temp_home();
    let fixture = cbth(
        &home,
        &[
            "desktop",
            "validation",
            "prepare-writeback-fixture",
            "--source-thread-id",
            "thread-relay-scanner-wrong-marker",
            "--caller-automation-id",
            "automation-relay-scanner-wrong-marker",
            "--bridge-request-id",
            "bridge-request-relay-scanner-wrong-marker",
            "--now",
            "7750",
            "--json",
        ],
    );
    let attempt_id = fixture["desktop_writeback_fixture"]["attempt"]["attempt_id"]
        .as_str()
        .unwrap();
    let rollout = home.path().join("scanner-wrong-marker-rollout.jsonl");
    fs::write(&rollout, "").expect("create wrong-marker rollout");
    cbth(
        &home,
        &[
            "desktop",
            "relay",
            "scanner",
            "bind",
            "--bridge-thread-id",
            "bridge-relay-scanner-wrong-marker",
            "--rollout-path",
            rollout.to_str().unwrap(),
            "--from-start",
            "--json",
            "--now",
            "7751",
        ],
    );
    let marker = cbth(
        &home,
        &[
            "desktop",
            "relay",
            "marker",
            "issue",
            "--bridge-thread-id",
            "bridge-relay-scanner-wrong-marker",
            "--kind",
            "arm-pending",
            "--source-thread-id",
            "thread-relay-scanner-wrong-marker",
            "--attempt-id",
            attempt_id,
            "--generation",
            "1",
            "--bridge-request-id",
            "bridge-request-relay-scanner-wrong-marker",
            "--json",
            "--now",
            "7752",
        ],
    );
    let marker = marker["desktop_transcript_relay_marker"]["record"]["marker"]
        .as_str()
        .unwrap()
        .to_owned();
    let wrong_envelope = json!({
        "schema_version": 1,
        "channel": "desktop_transcript_writeback",
        "kind": "arm_pending_requested",
        "source_thread_id": "thread-relay-scanner-wrong-marker",
        "attempt_id": attempt_id,
        "generation": 1,
        "bridge_request_id": "bridge-request-relay-scanner-wrong-marker",
        "marker": "other-marker",
        "created_at": 7753,
        "cbth_version": env!("CARGO_PKG_VERSION"),
    });
    append_function_call_rollout_line(
        &rollout,
        &format!(
            "mentioned marker {marker}\nCBTH_TRANSCRIPT_WRITEBACK_V1 {}",
            serde_json::to_string(&wrong_envelope).unwrap()
        ),
    );

    let scan = cbth(
        &home,
        &[
            "desktop",
            "relay",
            "scanner",
            "scan-once",
            "--bridge-thread-id",
            "bridge-relay-scanner-wrong-marker",
            "--json",
            "--now",
            "7754",
        ],
    );
    let scan = &scan["desktop_relay_scanner_scan"];
    assert_eq!(scan["consumed_markers"], 0);
    assert_eq!(scan["rejected_markers"], 1);
    assert!(
        scan["bindings"][0]["rejected_markers"][0]["reason"]
            .as_str()
            .unwrap()
            .contains("contained no matching relay envelope")
    );
    let attempt = cbth(&home, &["attempt", "inspect", "--attempt-id", attempt_id]);
    assert_eq!(attempt["attempt"]["state"], "prepared");
}

#[test]
fn desktop_transcript_relay_scanner_degrades_oversized_tick_line() {
    let home = temp_home();
    let fixture = cbth(
        &home,
        &[
            "desktop",
            "validation",
            "prepare-writeback-fixture",
            "--source-thread-id",
            "thread-relay-scanner-oversized",
            "--caller-automation-id",
            "automation-relay-scanner-oversized",
            "--bridge-request-id",
            "bridge-request-relay-scanner-oversized",
            "--now",
            "7760",
            "--json",
        ],
    );
    let attempt_id = fixture["desktop_writeback_fixture"]["attempt"]["attempt_id"]
        .as_str()
        .unwrap();
    let rollout = home.path().join("scanner-oversized-rollout.jsonl");
    let oversized_line = format!("{}\n", "x".repeat(1024 * 1024 + 1));
    fs::write(&rollout, oversized_line).expect("write oversized rollout");
    cbth(
        &home,
        &[
            "desktop",
            "relay",
            "scanner",
            "bind",
            "--bridge-thread-id",
            "bridge-relay-scanner-oversized",
            "--rollout-path",
            rollout.to_str().unwrap(),
            "--from-start",
            "--json",
            "--now",
            "7761",
        ],
    );
    cbth(
        &home,
        &[
            "desktop",
            "relay",
            "marker",
            "issue",
            "--bridge-thread-id",
            "bridge-relay-scanner-oversized",
            "--kind",
            "arm-pending",
            "--source-thread-id",
            "thread-relay-scanner-oversized",
            "--attempt-id",
            attempt_id,
            "--generation",
            "1",
            "--bridge-request-id",
            "bridge-request-relay-scanner-oversized",
            "--json",
            "--now",
            "7762",
        ],
    );

    let scan = cbth(
        &home,
        &[
            "desktop",
            "relay",
            "scanner",
            "scan-once",
            "--bridge-thread-id",
            "bridge-relay-scanner-oversized",
            "--json",
            "--now",
            "7763",
        ],
    );
    let binding_report = &scan["desktop_relay_scanner_scan"]["bindings"][0];
    assert_eq!(binding_report["outcome"], "degraded");
    assert!(
        binding_report["reason"]
            .as_str()
            .unwrap()
            .contains("exceeds 1048576 bytes")
    );
    assert_eq!(binding_report["binding"]["binding_state"], "degraded");
    let attempt = cbth(&home, &["attempt", "inspect", "--attempt-id", attempt_id]);
    assert_eq!(attempt["attempt"]["state"], "prepared");
}

#[cfg(unix)]
#[test]
fn desktop_transcript_relay_scanner_degrades_replaced_fifo_before_open() {
    let home = temp_home();
    let rollout = home.path().join("scanner-fifo-rollout.jsonl");
    fs::write(&rollout, "").expect("create rollout");
    cbth(
        &home,
        &[
            "desktop",
            "relay",
            "scanner",
            "bind",
            "--bridge-thread-id",
            "bridge-relay-scanner-fifo",
            "--rollout-path",
            rollout.to_str().unwrap(),
            "--from-start",
            "--json",
            "--now",
            "7770",
        ],
    );
    fs::remove_file(&rollout).expect("remove rollout");
    mkfifo(&rollout);

    let scan = cbth(
        &home,
        &[
            "desktop",
            "relay",
            "scanner",
            "scan-once",
            "--bridge-thread-id",
            "bridge-relay-scanner-fifo",
            "--json",
            "--now",
            "7771",
        ],
    );
    let binding_report = &scan["desktop_relay_scanner_scan"]["bindings"][0];
    assert_eq!(binding_report["outcome"], "degraded");
    assert!(
        binding_report["reason"]
            .as_str()
            .unwrap()
            .contains("not a regular file before open")
    );
    assert_eq!(binding_report["binding"]["binding_state"], "degraded");
}

#[test]
fn desktop_transcript_relay_consumer_records_failed_cas_replay_fence() {
    let home = temp_home();
    let fixture = cbth(
        &home,
        &[
            "desktop",
            "validation",
            "prepare-writeback-fixture",
            "--source-thread-id",
            "thread-relay-expired",
            "--caller-automation-id",
            "automation-relay-expired",
            "--bridge-request-id",
            "bridge-request-relay-expired",
            "--now",
            "7300",
            "--json",
        ],
    );
    let fixture = &fixture["desktop_writeback_fixture"];
    let attempt_id = fixture["attempt"]["attempt_id"].as_str().unwrap();

    let pending_marker = issue_desktop_relay_marker(
        &home,
        "bridge-relay-expired",
        "arm-pending",
        "thread-relay-expired",
        attempt_id,
        1,
        "bridge-request-relay-expired",
        7305,
    );
    let pending_emit = cbth_output(
        &home,
        &[
            "desktop",
            "validation",
            "emit-transcript-arm-pending",
            "--source-thread-id",
            "thread-relay-expired",
            "--attempt-id",
            attempt_id,
            "--generation",
            "1",
            "--bridge-request-id",
            "bridge-request-relay-expired",
            "--marker",
            &pending_marker,
            "--json",
            "--now",
            "7310",
        ],
        false,
    );
    assert!(pending_emit.status.success());
    let pending_rollout = home.path().join("expired-pending-rollout.jsonl");
    write_function_call_rollout(
        &pending_rollout,
        &String::from_utf8(pending_emit.stdout).unwrap(),
    );
    let pending = cbth(
        &home,
        &[
            "desktop",
            "relay",
            "consume-transcript",
            "--rollout-path",
            pending_rollout.to_str().unwrap(),
            "--marker",
            &pending_marker,
            "--json",
            "--now",
            "7320",
        ],
    );
    let pending = &pending["desktop_transcript_relay_consumption"];
    let lease_id = pending["record"]["outcome"]["bridge_arm_lease_id"]
        .as_str()
        .unwrap()
        .to_owned();

    let arm_marker = issue_desktop_relay_marker(
        &home,
        "bridge-relay-expired",
        "arm-accepted",
        "thread-relay-expired",
        attempt_id,
        1,
        "bridge-request-relay-expired",
        7325,
    );
    let conn = Connection::open(home.path().join("cbth.sqlite3")).expect("open db");
    conn.execute(
        "UPDATE delivery_attempts
         SET bridge_arm_lease_deadline = ?,
             arm_pending_deadline = ?
         WHERE attempt_id = ?",
        params![7335, 7335, attempt_id],
    )
    .expect("shorten arm deadlines after marker issue");
    drop(conn);
    let arm_emit = cbth_output(
        &home,
        &[
            "desktop",
            "validation",
            "emit-transcript-arm",
            "--source-thread-id",
            "thread-relay-expired",
            "--attempt-id",
            attempt_id,
            "--generation",
            "1",
            "--bridge-request-id",
            "bridge-request-relay-expired",
            "--bridge-arm-lease-id",
            &lease_id,
            "--marker",
            &arm_marker,
            "--json",
            "--now",
            "7330",
        ],
        false,
    );
    assert!(arm_emit.status.success());
    let arm_rollout = home.path().join("expired-arm-rollout.jsonl");
    write_function_call_rollout(&arm_rollout, &String::from_utf8(arm_emit.stdout).unwrap());
    let expired_error = cbth_failure(
        &home,
        &[
            "desktop",
            "relay",
            "consume-transcript",
            "--rollout-path",
            arm_rollout.to_str().unwrap(),
            "--marker",
            &arm_marker,
            "--json",
            "--now",
            "7340",
        ],
    );
    assert!(
        expired_error.contains("bridge arm lease expired"),
        "unexpected error: {expired_error}"
    );
    let inspected = cbth(&home, &["attempt", "inspect", "--attempt-id", attempt_id]);
    assert_eq!(inspected["attempt"]["state"], "abandoned");

    let replay_error = cbth_failure(
        &home,
        &[
            "desktop",
            "relay",
            "consume-transcript",
            "--rollout-path",
            arm_rollout.to_str().unwrap(),
            "--marker",
            &arm_marker,
            "--json",
            "--now",
            "7341",
        ],
    );
    assert!(replay_error.contains("replayed failed CAS"));
    assert!(replay_error.contains("bridge arm lease expired"));

    let conflicting_emit = cbth_output(
        &home,
        &[
            "desktop",
            "validation",
            "emit-transcript-arm",
            "--source-thread-id",
            "thread-relay-expired",
            "--attempt-id",
            attempt_id,
            "--generation",
            "1",
            "--bridge-request-id",
            "bridge-request-relay-expired",
            "--bridge-arm-lease-id",
            &lease_id,
            "--marker",
            &arm_marker,
            "--json",
            "--now",
            "7331",
        ],
        false,
    );
    assert!(conflicting_emit.status.success());
    let conflicting_rollout = home.path().join("expired-arm-conflict-rollout.jsonl");
    write_function_call_rollout(
        &conflicting_rollout,
        &String::from_utf8(conflicting_emit.stdout).unwrap(),
    );
    let conflict_error = cbth_failure(
        &home,
        &[
            "desktop",
            "relay",
            "consume-transcript",
            "--rollout-path",
            conflicting_rollout.to_str().unwrap(),
            "--marker",
            &arm_marker,
            "--json",
            "--now",
            "7341",
        ],
    );
    assert!(conflict_error.contains("already consumed with another envelope hash"));
}

#[test]
fn desktop_transcript_relay_consumer_rolls_back_non_durable_cas_failure() {
    let home = temp_home();
    let fixture = cbth(
        &home,
        &[
            "desktop",
            "validation",
            "prepare-writeback-fixture",
            "--source-thread-id",
            "thread-relay-rollback",
            "--caller-automation-id",
            "automation-relay-rollback",
            "--bridge-request-id",
            "bridge-request-relay-rollback",
            "--now",
            "7400",
            "--json",
        ],
    );
    let fixture = &fixture["desktop_writeback_fixture"];
    let attempt_id = fixture["attempt"]["attempt_id"].as_str().unwrap();

    let pending_marker = issue_desktop_relay_marker(
        &home,
        "bridge-relay-rollback",
        "arm-pending",
        "thread-relay-rollback",
        attempt_id,
        1,
        "bridge-request-relay-rollback",
        7405,
    );
    let pending_emit = cbth_output(
        &home,
        &[
            "desktop",
            "validation",
            "emit-transcript-arm-pending",
            "--source-thread-id",
            "thread-relay-rollback",
            "--attempt-id",
            attempt_id,
            "--generation",
            "1",
            "--bridge-request-id",
            "bridge-request-relay-rollback",
            "--marker",
            &pending_marker,
            "--json",
            "--now",
            "7410",
        ],
        false,
    );
    assert!(pending_emit.status.success());
    let pending_rollout = home.path().join("rollback-pending-rollout.jsonl");
    write_function_call_rollout(
        &pending_rollout,
        &String::from_utf8(pending_emit.stdout).unwrap(),
    );
    let pending = cbth(
        &home,
        &[
            "desktop",
            "relay",
            "consume-transcript",
            "--rollout-path",
            pending_rollout.to_str().unwrap(),
            "--marker",
            &pending_marker,
            "--json",
            "--now",
            "7420",
        ],
    );
    let lease_id =
        pending["desktop_transcript_relay_consumption"]["record"]["outcome"]["bridge_arm_lease_id"]
            .as_str()
            .unwrap()
            .to_owned();

    let arm_marker = issue_desktop_relay_marker(
        &home,
        "bridge-relay-rollback",
        "arm-accepted",
        "thread-relay-rollback",
        attempt_id,
        1,
        "bridge-request-relay-rollback",
        7425,
    );
    let wrong_arm_emit = cbth_output(
        &home,
        &[
            "desktop",
            "validation",
            "emit-transcript-arm",
            "--source-thread-id",
            "thread-relay-rollback",
            "--attempt-id",
            attempt_id,
            "--generation",
            "1",
            "--bridge-request-id",
            "bridge-request-relay-rollback",
            "--bridge-arm-lease-id",
            "wrong-lease",
            "--marker",
            &arm_marker,
            "--json",
            "--now",
            "7430",
        ],
        false,
    );
    assert!(wrong_arm_emit.status.success());
    let wrong_arm_rollout = home.path().join("rollback-wrong-arm-rollout.jsonl");
    write_function_call_rollout(
        &wrong_arm_rollout,
        &String::from_utf8(wrong_arm_emit.stdout).unwrap(),
    );
    let wrong_error = cbth_failure(
        &home,
        &[
            "desktop",
            "relay",
            "consume-transcript",
            "--rollout-path",
            wrong_arm_rollout.to_str().unwrap(),
            "--marker",
            &arm_marker,
            "--json",
            "--now",
            "7440",
        ],
    );
    assert!(wrong_error.contains("bridge_arm_lease_id does not match"));
    let inspected_after_error = cbth(&home, &["attempt", "inspect", "--attempt-id", attempt_id]);
    assert_eq!(inspected_after_error["attempt"]["state"], "arm_pending");

    let correct_arm_emit = cbth_output(
        &home,
        &[
            "desktop",
            "validation",
            "emit-transcript-arm",
            "--source-thread-id",
            "thread-relay-rollback",
            "--attempt-id",
            attempt_id,
            "--generation",
            "1",
            "--bridge-request-id",
            "bridge-request-relay-rollback",
            "--bridge-arm-lease-id",
            &lease_id,
            "--marker",
            &arm_marker,
            "--json",
            "--now",
            "7450",
        ],
        false,
    );
    assert!(correct_arm_emit.status.success());
    let correct_arm_rollout = home.path().join("rollback-correct-arm-rollout.jsonl");
    write_function_call_rollout(
        &correct_arm_rollout,
        &String::from_utf8(correct_arm_emit.stdout).unwrap(),
    );
    let armed = cbth(
        &home,
        &[
            "desktop",
            "relay",
            "consume-transcript",
            "--rollout-path",
            correct_arm_rollout.to_str().unwrap(),
            "--marker",
            &arm_marker,
            "--json",
            "--now",
            "7460",
        ],
    );
    let armed = &armed["desktop_transcript_relay_consumption"]["record"];
    assert_eq!(armed["replay_state"], "fresh");
    assert_eq!(armed["outcome"]["outcome"], "armed");
    assert_eq!(armed["outcome"]["delivery_attempt_count"], 1);
}

#[test]
fn desktop_transcript_relay_consumer_fails_closed_without_trusted_single_envelope() {
    let home = temp_home();
    let fixture = cbth(
        &home,
        &[
            "desktop",
            "validation",
            "prepare-writeback-fixture",
            "--source-thread-id",
            "thread-relay-fail-closed",
            "--caller-automation-id",
            "automation-relay-fail-closed",
            "--bridge-request-id",
            "bridge-request-relay-fail-closed",
            "--now",
            "7100",
            "--json",
        ],
    );
    let attempt_id = fixture["desktop_writeback_fixture"]["attempt"]["attempt_id"]
        .as_str()
        .unwrap();
    let marker = "CBTH_RELAY_CONSUMER_FAIL_CLOSED";
    let emit = cbth_output(
        &home,
        &[
            "desktop",
            "validation",
            "emit-transcript-arm-pending",
            "--source-thread-id",
            "thread-relay-fail-closed",
            "--attempt-id",
            attempt_id,
            "--generation",
            "1",
            "--bridge-request-id",
            "bridge-request-relay-fail-closed",
            "--marker",
            marker,
            "--json",
            "--now",
            "7110",
        ],
        false,
    );
    assert!(emit.status.success());
    let envelope = String::from_utf8(emit.stdout).unwrap();

    let prompt_only = home.path().join("relay-prompt-only.jsonl");
    write_user_prompt_rollout(&prompt_only, &envelope);
    let prompt_error = cbth_failure(
        &home,
        &[
            "desktop",
            "relay",
            "consume-transcript",
            "--rollout-path",
            prompt_only.to_str().unwrap(),
            "--marker",
            marker,
            "--json",
            "--now",
            "7120",
        ],
    );
    assert!(prompt_error.contains("no_trusted_auto_envelope"));

    let duplicate = home.path().join("relay-duplicate.jsonl");
    write_function_call_rollout(&duplicate, &format!("{envelope}\n{envelope}\n"));
    let duplicate_error = cbth_failure(
        &home,
        &[
            "desktop",
            "relay",
            "consume-transcript",
            "--rollout-path",
            duplicate.to_str().unwrap(),
            "--marker",
            marker,
            "--json",
            "--now",
            "7121",
        ],
    );
    assert!(duplicate_error.contains("duplicate_trusted_auto_envelopes"));

    let malformed = home.path().join("relay-malformed.jsonl");
    write_function_call_rollout(
        &malformed,
        "CBTH_TRANSCRIPT_WRITEBACK_V1 {\"marker\":\"CBTH_RELAY_CONSUMER_FAIL_CLOSED\"",
    );
    let malformed_error = cbth_failure(
        &home,
        &[
            "desktop",
            "relay",
            "consume-transcript",
            "--rollout-path",
            malformed.to_str().unwrap(),
            "--marker",
            marker,
            "--json",
            "--now",
            "7122",
        ],
    );
    assert!(malformed_error.contains("rejected_trusted_auto_envelopes"));

    let wrong_marker = home.path().join("relay-wrong-marker.jsonl");
    write_function_call_rollout(&wrong_marker, &envelope);
    let wrong_marker_error = cbth_failure(
        &home,
        &[
            "desktop",
            "relay",
            "consume-transcript",
            "--rollout-path",
            wrong_marker.to_str().unwrap(),
            "--marker",
            "OTHER_RELAY_MARKER",
            "--json",
            "--now",
            "7123",
        ],
    );
    assert!(wrong_marker_error.contains("no_trusted_auto_envelope"));

    let inspected = cbth(&home, &["attempt", "inspect", "--attempt-id", attempt_id]);
    assert_eq!(inspected["attempt"]["state"], "prepared");
}

#[test]
fn desktop_transcript_relay_consumer_rejects_untrusted_without_opening_store() {
    let marker = "CBTH_RELAY_CONSUMER_NO_STORE";
    let prefix = "CBTH_TRANSCRIPT_WRITEBACK_V1 ";
    let envelope = json!({
        "schema_version": 1,
        "channel": "desktop_transcript_writeback",
        "kind": "arm_pending_requested",
        "source_thread_id": "thread-no-store",
        "attempt_id": "attempt-no-store",
        "generation": 1,
        "bridge_request_id": "bridge-request-no-store",
        "marker": marker,
        "created_at": 7200,
    });
    let line = format!("{prefix}{}", serde_json::to_string(&envelope).unwrap());

    let prompt_home = temp_home();
    let prompt_only = prompt_home.path().join("relay-prompt-only-no-store.jsonl");
    write_user_prompt_rollout(&prompt_only, &line);
    let prompt_error = cbth_failure(
        &prompt_home,
        &[
            "desktop",
            "relay",
            "consume-transcript",
            "--rollout-path",
            prompt_only.to_str().unwrap(),
            "--marker",
            marker,
            "--json",
            "--now",
            "7210",
        ],
    );
    assert!(prompt_error.contains("no_trusted_auto_envelope"));
    assert!(!prompt_home.path().join("cbth.sqlite3").exists());
    assert!(!prompt_home.path().join("run").join("startup.lock").exists());

    let probe_home = temp_home();
    let probe_marker = "CBTH_RELAY_CONSUMER_PROBE_NO_STORE";
    let probe_envelope = json!({
        "schema_version": 1,
        "channel": "desktop_transcript_writeback",
        "kind": "validation_probe",
        "bridge_thread_id": "bridge-thread-no-store",
        "probe_id": "probe-no-store",
        "marker": probe_marker,
        "created_at": 7220,
    });
    let probe_line = format!(
        "{prefix}{}",
        serde_json::to_string(&probe_envelope).unwrap()
    );
    let probe_rollout = probe_home.path().join("relay-probe-no-store.jsonl");
    write_function_call_rollout(&probe_rollout, &probe_line);
    let probe_error = cbth_failure(
        &probe_home,
        &[
            "desktop",
            "relay",
            "consume-transcript",
            "--rollout-path",
            probe_rollout.to_str().unwrap(),
            "--marker",
            probe_marker,
            "--json",
            "--now",
            "7221",
        ],
    );
    assert!(probe_error.contains("is not consumable"));
    assert!(!probe_home.path().join("cbth.sqlite3").exists());
    assert!(!probe_home.path().join("run").join("startup.lock").exists());

    let empty_home = temp_home();
    let empty_marker = "CBTH_RELAY_CONSUMER_EMPTY_NO_STORE";
    let empty_envelope = json!({
        "schema_version": 1,
        "channel": "desktop_transcript_writeback",
        "kind": "arm_pending_requested",
        "source_thread_id": "",
        "attempt_id": "attempt-empty-no-store",
        "generation": 1,
        "bridge_request_id": "bridge-request-empty-no-store",
        "marker": empty_marker,
        "created_at": 7230,
    });
    let empty_line = format!(
        "{prefix}{}",
        serde_json::to_string(&empty_envelope).unwrap()
    );
    let empty_rollout = empty_home.path().join("relay-empty-no-store.jsonl");
    write_function_call_rollout(&empty_rollout, &empty_line);
    let empty_error = cbth_failure(
        &empty_home,
        &[
            "desktop",
            "relay",
            "consume-transcript",
            "--rollout-path",
            empty_rollout.to_str().unwrap(),
            "--marker",
            empty_marker,
            "--json",
            "--now",
            "7231",
        ],
    );
    assert!(empty_error.contains("rejected_trusted_auto_envelopes"));
    assert!(!empty_home.path().join("cbth.sqlite3").exists());
    assert!(!empty_home.path().join("run").join("startup.lock").exists());
}

#[test]
fn desktop_transcript_relay_consumer_scans_before_daemon_autostart() {
    let marker = "CBTH_RELAY_DAEMON_NO_STORE";
    let prefix = "CBTH_TRANSCRIPT_WRITEBACK_V1 ";
    let envelope = json!({
        "schema_version": 1,
        "channel": "desktop_transcript_writeback",
        "kind": "arm_pending_requested",
        "source_thread_id": "thread-daemon-no-store",
        "attempt_id": "attempt-daemon-no-store",
        "generation": 1,
        "bridge_request_id": "bridge-request-daemon-no-store",
        "marker": marker,
        "created_at": 7240,
    });
    let line = format!("{prefix}{}", serde_json::to_string(&envelope).unwrap());
    let untrusted_home = temp_home();
    let prompt_only = untrusted_home
        .path()
        .join("relay-daemon-prompt-only-no-store.jsonl");
    write_user_prompt_rollout(&prompt_only, &line);
    let prompt_error = cbth_daemon_failure(
        &untrusted_home,
        &[
            "desktop",
            "relay",
            "consume-transcript",
            "--rollout-path",
            prompt_only.to_str().unwrap(),
            "--marker",
            marker,
            "--json",
            "--now",
            "7241",
        ],
    );
    assert!(prompt_error.contains("no_trusted_auto_envelope"));
    assert!(!untrusted_home.path().join("cbth.sqlite3").exists());
    assert!(
        !untrusted_home
            .path()
            .join("run")
            .join("startup.lock")
            .exists()
    );

    let forged_home = temp_home();
    let forged_error = cbth_failure(
        &forged_home,
        &[
            "desktop",
            "relay",
            "consume-prepared-transcript",
            "--rollout-path",
            "forged-rollout.jsonl",
            "--marker",
            "CBTH_FORGED_PREPARED",
            "--envelope-hash",
            "forged-hash",
            "--envelope-kind",
            "arm_pending_requested",
            "--envelope-json",
            "{\"kind\":\"arm_pending_requested\"}",
            "--trusted-entry-json",
            "{\"carrier\":\"trusted_auto\",\"record_line\":1,\"record_type\":\"response_item\",\"payload_type\":\"function_call_output\"}",
            "--source-thread-id",
            "thread-forged",
            "--attempt-id",
            "attempt-forged",
            "--generation",
            "1",
            "--bridge-request-id",
            "bridge-request-forged",
            "--json",
        ],
    );
    assert!(forged_error.contains("daemon-internal"));
    assert!(!forged_home.path().join("cbth.sqlite3").exists());

    let trusted_home = temp_home();
    let trusted_now: i64 = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time after Unix epoch")
        .as_secs()
        .try_into()
        .expect("epoch seconds fit i64");
    let fixture_now = trusted_now.saturating_add(60).to_string();
    let emit_now = trusted_now.saturating_add(70).to_string();
    let consume_now = trusted_now.saturating_add(80).to_string();
    let fixture = cbth(
        &trusted_home,
        &[
            "desktop",
            "validation",
            "prepare-writeback-fixture",
            "--source-thread-id",
            "thread-relay-daemon",
            "--caller-automation-id",
            "automation-relay-daemon",
            "--bridge-request-id",
            "bridge-request-relay-daemon",
            "--now",
            &fixture_now,
            "--json",
        ],
    );
    let attempt_id = fixture["desktop_writeback_fixture"]["attempt"]["attempt_id"]
        .as_str()
        .unwrap();
    let trusted_marker = issue_desktop_relay_marker(
        &trusted_home,
        "bridge-relay-daemon",
        "arm-pending",
        "thread-relay-daemon",
        attempt_id,
        1,
        "bridge-request-relay-daemon",
        trusted_now.saturating_add(65),
    );
    let emit = cbth_output(
        &trusted_home,
        &[
            "desktop",
            "validation",
            "emit-transcript-arm-pending",
            "--source-thread-id",
            "thread-relay-daemon",
            "--attempt-id",
            attempt_id,
            "--generation",
            "1",
            "--bridge-request-id",
            "bridge-request-relay-daemon",
            "--marker",
            &trusted_marker,
            "--json",
            "--now",
            &emit_now,
        ],
        false,
    );
    assert!(emit.status.success());
    let rollout = trusted_home.path().join("relay-daemon-trusted.jsonl");
    write_function_call_rollout(&rollout, &String::from_utf8(emit.stdout).unwrap());
    let consumed = cbth_daemon(
        &trusted_home,
        &[
            "desktop",
            "relay",
            "consume-transcript",
            "--rollout-path",
            rollout.to_str().unwrap(),
            "--marker",
            &trusted_marker,
            "--json",
            "--now",
            &consume_now,
        ],
    );
    assert_eq!(
        consumed["desktop_transcript_relay_consumption"]["record"]["outcome"]["outcome"],
        "arm_pending"
    );
    stop_daemon(&trusted_home);
}

#[test]
fn desktop_writeback_dropbox_probe_writes_once_without_daemon_or_store() {
    let home = temp_home();
    let output = cbth_output(
        &home,
        &[
            "desktop",
            "validation",
            "writeback-dropbox-probe",
            "--bridge-thread-id",
            "bridge-thread",
            "--probe-id",
            "probe-1",
            "--marker",
            "CBTH_DESKTOP_WRITEBACK_DROPBOX_PROBE_TEST",
            "--json",
            "--now",
            "5100",
        ],
        false,
    );
    assert!(
        output.status.success(),
        "dropbox probe failed\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let result: Value = serde_json::from_slice(&output.stdout).expect("valid probe json");
    let probe = &result["desktop_writeback_dropbox_probe"];
    assert_eq!(probe["probe_id"], "probe-1");
    assert_eq!(probe["bridge_thread_id"], "bridge-thread");
    assert_eq!(probe["marker"], "CBTH_DESKTOP_WRITEBACK_DROPBOX_PROBE_TEST");
    assert_eq!(probe["created_at"], 5100);
    assert_eq!(probe["write_mode"], "create_new");
    let path = probe["path"].as_str().expect("probe path");
    assert!(path.ends_with("inbox/writeback-dropbox/probes/probe-1.json"));
    assert!(!home.path().join("run").join("startup.lock").exists());
    assert!(!home.path().join("cbth.sqlite3").exists());

    let written = read_json_file(path);
    assert_eq!(written["schema_version"], 1);
    assert_eq!(written["probe_id"], "probe-1");
    assert_eq!(written["bridge_thread_id"], "bridge-thread");
    assert_eq!(
        written["marker"],
        "CBTH_DESKTOP_WRITEBACK_DROPBOX_PROBE_TEST"
    );

    #[cfg(unix)]
    {
        let metadata = fs::metadata(path).expect("stat probe file");
        assert_eq!(metadata.permissions().mode() & 0o7777, 0o600);
        let dir_metadata = fs::metadata(home.path().join("inbox/writeback-dropbox/probes"))
            .expect("stat probe dir");
        assert_eq!(dir_metadata.permissions().mode() & 0o7777, 0o700);
    }

    let duplicate = cbth_output(
        &home,
        &[
            "desktop",
            "validation",
            "writeback-dropbox-probe",
            "--bridge-thread-id",
            "bridge-thread",
            "--probe-id",
            "probe-1",
            "--marker",
            "duplicate",
            "--json",
        ],
        false,
    );
    assert!(!duplicate.status.success());
    let duplicate_stderr = String::from_utf8_lossy(&duplicate.stderr);
    assert!(duplicate_stderr.contains("create file"));
    assert_eq!(
        read_json_file(path)["marker"],
        "CBTH_DESKTOP_WRITEBACK_DROPBOX_PROBE_TEST"
    );

    let invalid_probe = cbth_output(
        &home,
        &[
            "desktop",
            "validation",
            "writeback-dropbox-probe",
            "--bridge-thread-id",
            "bridge-thread",
            "--probe-id",
            "../escape",
            "--marker",
            "marker",
            "--json",
        ],
        false,
    );
    assert!(!invalid_probe.status.success());
    assert!(
        String::from_utf8_lossy(&invalid_probe.stderr)
            .contains("probe_id contains unsupported path characters")
    );

    let append_path = home
        .path()
        .join("inbox/writeback-dropbox/probes/probe-append.json");
    fs::write(&append_path, "").expect("precreate append probe file");
    #[cfg(unix)]
    fs::set_permissions(&append_path, fs::Permissions::from_mode(0o600))
        .expect("chmod append probe file");
    let appended = cbth_output(
        &home,
        &[
            "desktop",
            "validation",
            "writeback-dropbox-probe",
            "--bridge-thread-id",
            "bridge-thread",
            "--probe-id",
            "probe-append",
            "--marker",
            "CBTH_DESKTOP_WRITEBACK_DROPBOX_APPEND_TEST",
            "--append-existing",
            "--json",
            "--now",
            "5110",
        ],
        false,
    );
    assert!(
        appended.status.success(),
        "append probe failed\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&appended.stdout),
        String::from_utf8_lossy(&appended.stderr)
    );
    let appended: Value = serde_json::from_slice(&appended.stdout).expect("valid append json");
    let appended = &appended["desktop_writeback_dropbox_probe"];
    assert_eq!(appended["probe_id"], "probe-append");
    assert_eq!(appended["write_mode"], "append_existing");
    assert_eq!(
        read_json_file(append_path.to_str().unwrap())["marker"],
        "CBTH_DESKTOP_WRITEBACK_DROPBOX_APPEND_TEST"
    );

    let missing_append = cbth_output(
        &home,
        &[
            "desktop",
            "validation",
            "writeback-dropbox-probe",
            "--bridge-thread-id",
            "bridge-thread",
            "--probe-id",
            "probe-missing-append",
            "--marker",
            "marker",
            "--append-existing",
            "--json",
        ],
        false,
    );
    assert!(!missing_append.status.success());
    assert!(String::from_utf8_lossy(&missing_append.stderr).contains("stat"));
}

#[test]
fn desktop_bridge_preflight_exports_only_current_bound_eligible_arm_pending() {
    let degraded_home = temp_home();
    repair_validated_desktop_installation_and_binding(
        &degraded_home,
        "thread-degraded-export",
        "automation-degraded-export",
        2400,
    );
    create_desktop_batch_and_prepared_attempt(
        &degraded_home,
        "thread-degraded-export",
        "attempt-degraded-export",
        1,
        2401,
    );
    force_desktop_attempt_arm_pending(&degraded_home, "attempt-degraded-export", 2402);
    let conn = Connection::open(degraded_home.path().join("cbth.sqlite3")).expect("open db");
    conn.execute(
        "UPDATE delivery_attempts
         SET bridge_arm_lease_deadline = ?, arm_pending_deadline = ?
         WHERE attempt_id = ?",
        params![2402, 2402, "attempt-degraded-export"],
    )
    .unwrap();
    drop(conn);
    cbth(
        &degraded_home,
        &[
            "desktop",
            "binding",
            "repair",
            "--source-thread-id",
            "thread-second-export",
            "--caller-automation-id",
            "automation-second-export",
            "--json",
            "--now",
            "2402",
        ],
    );
    create_desktop_batch_and_prepared_attempt(
        &degraded_home,
        "thread-second-export",
        "attempt-second-export",
        1,
        2402,
    );
    force_desktop_attempt_arm_pending(&degraded_home, "attempt-second-export", 2402);
    mark_consumed_pending_marker_for_bridge(
        &degraded_home,
        "bridge-thread",
        "thread-second-export",
        "attempt-second-export",
        1,
        "bridge-request-attempt-second-export",
        2402,
    );
    cbth(
        &degraded_home,
        &[
            "desktop",
            "binding",
            "repair",
            "--source-thread-id",
            "thread-third-export",
            "--caller-automation-id",
            "automation-third-export",
            "--json",
            "--now",
            "2402",
        ],
    );
    create_desktop_batch_and_prepared_attempt(
        &degraded_home,
        "thread-third-export",
        "attempt-third-export",
        1,
        2402,
    );
    force_desktop_attempt_arm_pending(&degraded_home, "attempt-third-export", 2402);
    mark_consumed_pending_marker_for_bridge(
        &degraded_home,
        "bridge-thread",
        "thread-third-export",
        "attempt-third-export",
        1,
        "bridge-request-attempt-third-export",
        2402,
    );
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
        2
    );
    let current_path =
        current["desktop_bridge_preflight"]["snapshots"]["arm_pending_bindings"]["path"]
            .as_str()
            .unwrap();
    let current_arm_pending = read_json_file(current_path);
    assert_eq!(
        current_arm_pending["arm_pending_bindings"]["entries"][0]["source_thread_id"],
        "thread-second-export"
    );
    assert_eq!(
        current_arm_pending["arm_pending_bindings"]["entries"][1]["source_thread_id"],
        "thread-third-export"
    );
    assert!(
        current_arm_pending["arm_pending_bindings"]["entries"][0]["arm_accepted_marker"]
            .as_str()
            .unwrap()
            .starts_with("CBTH_DESKTOP_RELAY_ARM_ACCEPTED_")
    );
    assert!(
        current_arm_pending["arm_pending_bindings"]["entries"][1]["arm_accepted_marker"]
            .as_str()
            .unwrap()
            .starts_with("CBTH_DESKTOP_RELAY_ARM_ACCEPTED_")
    );
    let conn = Connection::open(degraded_home.path().join("cbth.sqlite3")).expect("open db");
    conn.execute(
        "UPDATE desktop_installation_state
         SET validation_fingerprint = ?
         WHERE id = 1",
        params!["helper-fingerprint-drift-without-binding-repair"],
    )
    .expect("simulate helper fingerprint drift without binding repair");
    drop(conn);
    let helper_drift = cbth(
        &degraded_home,
        &[
            "desktop",
            "bridge-preflight",
            "--helper-direct-store",
            "--bridge-thread-id",
            "bridge-thread",
            "--json",
            "--now",
            "2404",
        ],
    );
    assert_eq!(
        helper_drift["desktop_bridge_preflight"]["snapshots"]["arm_pending_bindings"]["count"], 0,
        "helper-local capability downgrade must not publish entries without accepted markers"
    );
    let expired_attempt = cbth(
        &degraded_home,
        &[
            "attempt",
            "inspect",
            "--attempt-id",
            "attempt-degraded-export",
        ],
    );
    assert_eq!(expired_attempt["attempt"]["state"], "abandoned");
    assert_eq!(expired_attempt["attempt"]["abandoned_at"], 2403);
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
            "2405",
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
            "2406",
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
            "2407",
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
            "thread-expired-ready",
            "--caller-automation-id",
            "automation-expired-ready",
            "--json",
            "--now",
            "3007",
        ],
    );
    let expired_batch = create_desktop_batch_and_prepared_attempt(
        &home,
        "thread-expired-ready",
        "attempt-expired-ready",
        1,
        3008,
    );
    let conn = Connection::open(home.path().join("cbth.sqlite3")).expect("open db");
    conn.execute(
        "UPDATE batches SET redelivery_window_ends_at = ? WHERE batch_id = ?",
        params![3009, expired_batch],
    )
    .unwrap();
    drop(conn);
    let expired_ready = cbth_failure(
        &home,
        &[
            "desktop",
            "note-arm-pending",
            "--source-thread-id",
            "thread-expired-ready",
            "--attempt-id",
            "attempt-expired-ready",
            "--generation",
            "1",
            "--bridge-request-id",
            "bridge-request-expired-ready",
            "--json",
            "--now",
            "3010",
        ],
    );
    assert!(expired_ready.contains("redelivery window is closed"));

    cbth(
        &home,
        &[
            "desktop",
            "binding",
            "repair",
            "--source-thread-id",
            "thread-degraded-armed",
            "--caller-automation-id",
            "automation-degraded-armed",
            "--json",
            "--now",
            "3007",
        ],
    );
    create_desktop_batch_and_prepared_attempt(
        &home,
        "thread-degraded-armed",
        "attempt-degraded-armed",
        1,
        3008,
    );
    let degraded_armed_pending = cbth(
        &home,
        &[
            "desktop",
            "note-arm-pending",
            "--source-thread-id",
            "thread-degraded-armed",
            "--attempt-id",
            "attempt-degraded-armed",
            "--generation",
            "1",
            "--bridge-request-id",
            "bridge-request-degraded-armed",
            "--json",
            "--now",
            "3009",
        ],
    );
    let degraded_armed_lease = degraded_armed_pending["desktop_arm_pending"]["bridge_arm_lease_id"]
        .as_str()
        .expect("degraded armed lease");
    cbth(
        &home,
        &[
            "desktop",
            "note-arm",
            "--source-thread-id",
            "thread-degraded-armed",
            "--attempt-id",
            "attempt-degraded-armed",
            "--generation",
            "1",
            "--bridge-request-id",
            "bridge-request-degraded-armed",
            "--bridge-arm-lease-id",
            degraded_armed_lease,
            "--json",
            "--now",
            "3010",
        ],
    );
    cbth(
        &home,
        &[
            "desktop",
            "installation-state",
            "repair",
            "--read-transport",
            "direct-file-read",
            "--validation-fingerprint",
            "drifted-after-arm",
            "--json",
            "--now",
            "3011",
        ],
    );
    let degraded_arm_retry = cbth_failure(
        &home,
        &[
            "desktop",
            "note-arm",
            "--source-thread-id",
            "thread-degraded-armed",
            "--attempt-id",
            "attempt-degraded-armed",
            "--generation",
            "1",
            "--bridge-request-id",
            "bridge-request-degraded-armed",
            "--bridge-arm-lease-id",
            degraded_armed_lease,
            "--json",
            "--now",
            "3012",
        ],
    );
    assert!(degraded_arm_retry.contains("Desktop binding thread-degraded-armed is degraded"));
    let degraded_pending_retry = cbth_failure(
        &home,
        &[
            "desktop",
            "note-arm-pending",
            "--source-thread-id",
            "thread-degraded-armed",
            "--attempt-id",
            "attempt-degraded-armed",
            "--generation",
            "1",
            "--bridge-request-id",
            "bridge-request-degraded-armed",
            "--json",
            "--now",
            "3013",
        ],
    );
    assert!(degraded_pending_retry.contains("Desktop binding thread-degraded-armed is degraded"));

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
            "thread-expired-redelivery-after-pending",
            "--caller-automation-id",
            "automation-expired-redelivery-after-pending",
            "--json",
            "--now",
            "3306",
        ],
    );
    let expired_redelivery_batch = create_desktop_batch_and_prepared_attempt(
        &home,
        "thread-expired-redelivery-after-pending",
        "attempt-expired-redelivery-after-pending",
        1,
        3307,
    );
    let expired_redelivery_pending = cbth(
        &home,
        &[
            "desktop",
            "note-arm-pending",
            "--source-thread-id",
            "thread-expired-redelivery-after-pending",
            "--attempt-id",
            "attempt-expired-redelivery-after-pending",
            "--generation",
            "1",
            "--bridge-request-id",
            "bridge-request-expired-redelivery-after-pending",
            "--json",
            "--now",
            "3308",
        ],
    );
    let expired_redelivery_lease =
        expired_redelivery_pending["desktop_arm_pending"]["bridge_arm_lease_id"]
            .as_str()
            .expect("expired redelivery lease id");
    let conn = Connection::open(home.path().join("cbth.sqlite3")).expect("open db");
    conn.execute(
        "UPDATE batches SET redelivery_window_ends_at = ? WHERE batch_id = ?",
        params![3309, expired_redelivery_batch],
    )
    .unwrap();
    drop(conn);
    let expired_redelivery_arm = cbth_failure(
        &home,
        &[
            "desktop",
            "note-arm",
            "--source-thread-id",
            "thread-expired-redelivery-after-pending",
            "--attempt-id",
            "attempt-expired-redelivery-after-pending",
            "--generation",
            "1",
            "--bridge-request-id",
            "bridge-request-expired-redelivery-after-pending",
            "--bridge-arm-lease-id",
            expired_redelivery_lease,
            "--json",
            "--now",
            "3310",
        ],
    );
    assert!(expired_redelivery_arm.contains("redelivery window is closed at 3309"));
    let expired_redelivery_attempt = cbth(
        &home,
        &[
            "attempt",
            "inspect",
            "--attempt-id",
            "attempt-expired-redelivery-after-pending",
        ],
    );
    assert_eq!(expired_redelivery_attempt["attempt"]["state"], "abandoned");
    assert_eq!(expired_redelivery_attempt["attempt"]["abandoned_at"], 3310);

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

    let repair = cbth(
        &home,
        &[
            "desktop",
            "installation-state",
            "repair",
            "--read-transport",
            "direct-file-read",
            "--read-transport-capability",
            "validated",
            "--json",
            "--now",
            "2100",
        ],
    );
    let validation_fingerprint =
        repair["desktop_installation_state"]["state"]["validation_fingerprint"]
            .as_str()
            .expect("validation fingerprint")
            .to_owned();

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
        validation_fingerprint
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
