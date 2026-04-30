use std::fs;
use std::process::Command;
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{SystemTime, UNIX_EPOCH};

use rusqlite::{Connection, params};
use serde_json::Value;
use tempfile::TempDir;

#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

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

fn bind_cli_session(home: &TempDir, bound_thread_id: &str) -> String {
    bind_cli_session_with_profile(home, bound_thread_id, false, false, false)
}

fn bind_cli_session_with_profile(
    home: &TempDir,
    bound_thread_id: &str,
    session_allows_approval: bool,
    session_allows_network: bool,
    session_allows_write_access: bool,
) -> String {
    let session = cbth(
        home,
        &[
            "cli",
            "session",
            "bind",
            "--bound-thread-id",
            bound_thread_id,
            "--session-allows-approval",
            &session_allows_approval.to_string(),
            "--session-allows-network",
            &session_allows_network.to_string(),
            "--session-allows-write-access",
            &session_allows_write_access.to_string(),
        ],
    );
    assert!(matches!(
        session["cli_session"]["outcome"].as_str(),
        Some("created") | Some("attached")
    ));
    session["cli_session"]["session"]["managed_session_id"]
        .as_str()
        .expect("managed session id")
        .to_owned()
}

fn note_cli_session_idle(home: &TempDir, managed_session_id: &str) {
    let inspected = cbth(
        home,
        &[
            "cli",
            "session",
            "inspect",
            "--managed-session-id",
            managed_session_id,
        ],
    );
    let next_revision = inspected["cli_session"]["activity_revision"]
        .as_i64()
        .expect("activity revision")
        + 1;
    let next_revision = next_revision.to_string();
    let session_epoch = inspected["cli_session"]["session_epoch"]
        .as_i64()
        .expect("session epoch")
        .to_string();
    let session = cbth(
        home,
        &[
            "cli",
            "session",
            "note-activity",
            "--managed-session-id",
            managed_session_id,
            "--session-epoch",
            &session_epoch,
            "--activity-state",
            "idle",
            "--activity-revision",
            &next_revision,
        ],
    );
    assert_eq!(session["cli_session"]["session"]["activity_state"], "idle");
}

fn note_cli_session_minimum_capabilities(home: &TempDir, managed_session_id: &str) {
    let inspected = cbth(
        home,
        &[
            "cli",
            "session",
            "inspect",
            "--managed-session-id",
            managed_session_id,
        ],
    );
    let next_revision = inspected["cli_session"]["capability_revision"]
        .as_i64()
        .expect("capability revision")
        + 1;
    let next_revision = next_revision.to_string();
    let session_epoch = inspected["cli_session"]["session_epoch"]
        .as_i64()
        .expect("session epoch")
        .to_string();
    let session = cbth(
        home,
        &[
            "cli",
            "session",
            "note-capabilities",
            "--managed-session-id",
            managed_session_id,
            "--session-epoch",
            &session_epoch,
            "--capability-revision",
            &next_revision,
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
    assert_eq!(
        session["cli_session"]["session"]["capability_current_state_sync"],
        true
    );
}

fn bind_idle_cli_session(home: &TempDir, bound_thread_id: &str) -> String {
    let managed_session_id = bind_cli_session(home, bound_thread_id);
    note_cli_session_minimum_capabilities(home, &managed_session_id);
    note_cli_session_idle(home, &managed_session_id);
    managed_session_id
}

fn create_accepted_cli_attempt(
    home: &TempDir,
    source_thread_id: &str,
    delivery_turn_id: &str,
) -> (String, String, String) {
    let submitted = cbth(
        home,
        &[
            "job",
            "submit",
            "--source-thread-id",
            source_thread_id,
            "--summary",
            "ready for CLI observation",
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
        &["job", "fail", "--job-id", job_id, "--reason", "ready"],
    );
    let batch_id = failed["batch"]["batch"]["batch_id"]
        .as_str()
        .expect("batch id")
        .to_owned();
    let managed_session_id = bind_idle_cli_session(home, source_thread_id);
    let rpc_request_id = format!("rpc-request-{source_thread_id}");
    let pending = cbth(
        home,
        &[
            "attempt",
            "begin-cli-accept",
            "--batch-id",
            &batch_id,
            "--managed-session-id",
            &managed_session_id,
            "--session-epoch",
            "1",
            "--rpc-kind",
            "turn-start",
            "--rpc-request-id",
            &rpc_request_id,
            "--now",
            "1000",
        ],
    );
    let attempt_id = pending["attempt"]["attempt_id"]
        .as_str()
        .expect("attempt id")
        .to_owned();
    cbth(
        home,
        &[
            "attempt",
            "accept-cli",
            "--attempt-id",
            &attempt_id,
            "--delivery-turn-id",
            delivery_turn_id,
            "--observation-window-seconds",
            "60",
            "--now",
            "1001",
        ],
    );
    (batch_id, attempt_id, managed_session_id)
}

#[test]
fn cli_fake_e2e_job_batch_attempt_observation_delivers_head() {
    let home = tempfile::tempdir().expect("temp home");
    let submitted = cbth(
        &home,
        &[
            "job",
            "submit",
            "--source-thread-id",
            "thread-cli-fake-e2e",
            "--summary",
            "fake e2e job",
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
            "fake e2e result ready",
            "--max-delivery-attempts",
            "2",
        ],
    );
    let batch = &failed["batch"]["batch"];
    let batch_id = batch["batch_id"].as_str().expect("batch id");
    assert_eq!(batch["source_thread_id"], "thread-cli-fake-e2e");
    assert_eq!(batch["state"], "open");
    assert_eq!(batch["replay_policy"], "automatic");
    assert_eq!(batch["delivery_policy"]["delivery_read_only"], true);
    assert_eq!(
        batch["delivery_policy"]["delivery_requires_approval"],
        false
    );
    assert_eq!(batch["delivery_policy"]["delivery_requires_network"], false);
    assert_eq!(
        batch["delivery_policy"]["delivery_requires_write_access"],
        false
    );
    assert_eq!(batch["requires_artifact_read"], false);
    assert_eq!(batch["delivery_attempt_count"], 0);

    let managed_session_id = bind_idle_cli_session(&home, "thread-cli-fake-e2e");
    let session = cbth(
        &home,
        &[
            "cli",
            "session",
            "inspect",
            "--managed-session-id",
            &managed_session_id,
        ],
    );
    let session_epoch = session["cli_session"]["session_epoch"]
        .as_i64()
        .expect("session epoch")
        .to_string();

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
            &session_epoch,
            "--rpc-kind",
            "turn-start",
            "--rpc-request-id",
            "rpc-fake-e2e-1",
            "--rpc-correlation-marker",
            "cbth:fake-e2e-1",
            "--now",
            "2000",
        ],
    );
    let attempt_id = pending["attempt"]["attempt_id"]
        .as_str()
        .expect("attempt id");
    assert_eq!(pending["attempt"]["state"], "accept_pending");
    assert_eq!(
        pending["attempt"]["delivery_rpc_state"],
        "pending_acceptance"
    );
    assert_eq!(
        pending["attempt"]["delivery_rpc_correlation_marker"],
        "cbth:fake-e2e-1"
    );

    let accepted = cbth(
        &home,
        &[
            "attempt",
            "accept-cli",
            "--attempt-id",
            attempt_id,
            "--delivery-turn-id",
            "turn-fake-e2e-1",
            "--observation-window-seconds",
            "60",
            "--now",
            "2001",
        ],
    );
    assert_eq!(accepted["attempt"]["state"], "cooldown");
    assert_eq!(accepted["attempt"]["delivery_rpc_state"], "accepted");
    assert_eq!(accepted["attempt"]["delivery_turn_id"], "turn-fake-e2e-1");
    assert_eq!(
        accepted["attempt"]["delivery_observation_state"],
        "tracking"
    );
    assert_eq!(accepted["attempt"]["delivery_observation_deadline"], 2061);

    let observed = cbth(
        &home,
        &[
            "attempt",
            "observe-cli-turn",
            "--attempt-id",
            attempt_id,
            "--delivery-turn-id",
            "turn-fake-e2e-1",
            "--turn-event",
            "turn-completed",
            "--now",
            "2002",
        ],
    );
    assert_eq!(observed["attempt"]["state"], "closed");
    assert_eq!(
        observed["attempt"]["delivery_observation_state"],
        "completed"
    );

    let delivered = cbth(&home, &["batch", "inspect", "--batch-id", batch_id]);
    assert_eq!(delivered["batch"]["batch"]["state"], "closed");
    assert_eq!(delivered["batch"]["batch"]["close_reason"], "delivered");
    assert_eq!(delivered["batch"]["batch"]["delivery_attempt_count"], 1);
    let head = cbth(
        &home,
        &[
            "batch",
            "inspect-head",
            "--source-thread-id",
            "thread-cli-fake-e2e",
        ],
    );
    assert!(head["batch"].is_null());
}

#[test]
fn direct_store_requires_explicit_test_gate() {
    let home = tempfile::tempdir().expect("temp home");
    let output = Command::new(env!("CARGO_BIN_EXE_cbth"))
        .arg("--direct-store")
        .arg("--home")
        .arg(home.path())
        .args(["job", "list"])
        .output()
        .expect("run cbth");

    assert!(
        !output.status.success(),
        "cbth unexpectedly succeeded\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(String::from_utf8_lossy(&output.stderr).contains("CBTH_ALLOW_DIRECT_STORE=1"));
}

#[test]
fn cli_session_bind_creates_and_attaches_matching_profile() {
    let home = tempfile::tempdir().expect("temp home");
    let created = cbth(
        &home,
        &[
            "cli",
            "session",
            "bind",
            "--bound-thread-id",
            "thread-cli-session",
            "--session-allows-approval",
            "false",
            "--session-allows-network",
            "false",
            "--session-allows-write-access",
            "false",
            "--now",
            "100",
        ],
    );
    assert_eq!(created["cli_session"]["outcome"], "created");
    let managed_session_id = created["cli_session"]["session"]["managed_session_id"]
        .as_str()
        .expect("managed session id");
    assert_eq!(
        created["cli_session"]["session"]["bound_thread_id"],
        "thread-cli-session"
    );
    assert_eq!(created["cli_session"]["session"]["session_epoch"], 1);
    assert_eq!(created["cli_session"]["session"]["session_state"], "live");
    assert_eq!(
        created["cli_session"]["session"]["activity_state"],
        "unknown"
    );
    assert_eq!(created["cli_session"]["session"]["activity_revision"], 0);

    let attached = cbth(
        &home,
        &[
            "cli",
            "session",
            "bind",
            "--bound-thread-id",
            "thread-cli-session",
            "--session-allows-approval",
            "false",
            "--session-allows-network",
            "false",
            "--session-allows-write-access",
            "false",
            "--now",
            "200",
        ],
    );
    assert_eq!(attached["cli_session"]["outcome"], "attached");
    assert_eq!(
        attached["cli_session"]["session"]["managed_session_id"],
        managed_session_id
    );
    assert_eq!(attached["cli_session"]["session"]["session_epoch"], 2);
    assert_eq!(attached["cli_session"]["session"]["updated_at"], 200);
    assert_eq!(attached["cli_session"]["session"]["activity_revision"], 0);
    assert_eq!(
        attached["cli_session"]["session"]["activity_state"],
        "unknown"
    );

    let inspected = cbth(
        &home,
        &[
            "cli",
            "session",
            "inspect",
            "--managed-session-id",
            managed_session_id,
        ],
    );
    assert_eq!(
        inspected["cli_session"]["managed_session_id"],
        managed_session_id
    );
}

#[test]
fn cli_session_bind_requires_explicit_profile() {
    let home = tempfile::tempdir().expect("temp home");
    let stderr = cbth_failure(
        &home,
        &[
            "cli",
            "session",
            "bind",
            "--bound-thread-id",
            "thread-cli-missing-profile",
        ],
    );
    assert!(stderr.contains("session-allows-approval"));
}

#[test]
fn cli_session_bind_rejects_profile_drift() {
    let home = tempfile::tempdir().expect("temp home");
    bind_cli_session(&home, "thread-cli-profile-drift");

    let stderr = cbth_failure(
        &home,
        &[
            "cli",
            "session",
            "bind",
            "--bound-thread-id",
            "thread-cli-profile-drift",
            "--session-allows-approval",
            "false",
            "--session-allows-network",
            "true",
            "--session-allows-write-access",
            "false",
        ],
    );
    assert!(stderr.contains("profile does not match requested profile"));
}

#[test]
fn cli_session_note_activity_rejects_stale_revision() {
    let home = tempfile::tempdir().expect("temp home");
    let managed_session_id = bind_cli_session(&home, "thread-cli-activity-revision");

    cbth(
        &home,
        &[
            "cli",
            "session",
            "note-activity",
            "--managed-session-id",
            &managed_session_id,
            "--session-epoch",
            "1",
            "--activity-state",
            "active",
            "--activity-revision",
            "1",
        ],
    );
    let stale_idle = cbth_failure(
        &home,
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
    assert!(stale_idle.contains("activity revision"));
    let inspected = cbth(
        &home,
        &[
            "cli",
            "session",
            "inspect",
            "--managed-session-id",
            &managed_session_id,
        ],
    );
    assert_eq!(inspected["cli_session"]["activity_state"], "active");
    assert_eq!(inspected["cli_session"]["activity_revision"], 1);

    let jumped_revision = cbth_failure(
        &home,
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
            "3",
        ],
    );
    assert!(jumped_revision.contains("not the next revision"));
}

#[test]
fn cli_session_note_capabilities_records_epoch_local_probe() {
    let home = tempfile::tempdir().expect("temp home");
    let managed_session_id = bind_cli_session(&home, "thread-cli-capability-probe");

    note_cli_session_minimum_capabilities(&home, &managed_session_id);
    let inspected = cbth(
        &home,
        &[
            "cli",
            "session",
            "inspect",
            "--managed-session-id",
            &managed_session_id,
        ],
    );
    assert_eq!(inspected["cli_session"]["capability_revision"], 1);
    assert_eq!(inspected["cli_session"]["capability_thread_resume"], true);
    assert_eq!(inspected["cli_session"]["capability_turn_start"], true);
    assert_eq!(
        inspected["cli_session"]["capability_current_state_sync"],
        true
    );
    assert_eq!(
        inspected["cli_session"]["capability_turn_completed_event"],
        true
    );
    assert_eq!(
        inspected["cli_session"]["capability_negative_terminal_events"],
        true
    );
    assert_eq!(inspected["cli_session"]["capability_thread_start"], false);
    assert_eq!(inspected["cli_session"]["capability_turn_steer"], false);

    let stale_capability = cbth_failure(
        &home,
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
            "false",
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
    assert!(stale_capability.contains("capability revision"));

    let reattached = cbth(
        &home,
        &[
            "cli",
            "session",
            "bind",
            "--bound-thread-id",
            "thread-cli-capability-probe",
            "--session-allows-approval",
            "false",
            "--session-allows-network",
            "false",
            "--session-allows-write-access",
            "false",
        ],
    );
    assert_eq!(reattached["cli_session"]["session"]["session_epoch"], 2);
    assert_eq!(
        reattached["cli_session"]["session"]["capability_revision"],
        0
    );
    assert_eq!(
        reattached["cli_session"]["session"]["capability_thread_resume"],
        false
    );
}

#[test]
fn cli_session_invalidate_proof_resets_activity_and_capabilities() {
    let home = tempfile::tempdir().expect("temp home");
    let managed_session_id = bind_idle_cli_session(&home, "thread-cli-proof-invalidation");

    let invalidated = cbth(
        &home,
        &[
            "cli",
            "session",
            "invalidate-proof",
            "--managed-session-id",
            &managed_session_id,
            "--session-epoch",
            "1",
            "--now",
            "300",
        ],
    );
    assert_eq!(invalidated["cli_session"]["session_epoch"], 2);
    assert_eq!(invalidated["cli_session"]["activity_state"], "unknown");
    assert_eq!(invalidated["cli_session"]["activity_revision"], 0);
    assert_eq!(invalidated["cli_session"]["capability_revision"], 0);
    assert_eq!(
        invalidated["cli_session"]["capability_thread_resume"],
        false
    );
    assert_eq!(invalidated["cli_session"]["updated_at"], 300);

    let replayed_invalidation = cbth(
        &home,
        &[
            "cli",
            "session",
            "invalidate-proof",
            "--managed-session-id",
            &managed_session_id,
            "--session-epoch",
            "1",
            "--now",
            "301",
        ],
    );
    assert_eq!(replayed_invalidation["cli_session"]["session_epoch"], 2);
    assert_eq!(
        replayed_invalidation["cli_session"]["activity_state"],
        "unknown"
    );
    assert_eq!(replayed_invalidation["cli_session"]["activity_revision"], 0);
    assert_eq!(
        replayed_invalidation["cli_session"]["capability_revision"],
        0
    );
    assert_eq!(replayed_invalidation["cli_session"]["updated_at"], 300);

    let stale_epoch = cbth_failure(
        &home,
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
    assert!(stale_epoch.contains("is at epoch 2, not 1"));
}

#[test]
fn cli_session_rebind_fences_old_activity_writer() {
    let home = tempfile::tempdir().expect("temp home");
    let managed_session_id = bind_idle_cli_session(&home, "thread-cli-activity-fence");

    let reattached = cbth(
        &home,
        &[
            "cli",
            "session",
            "bind",
            "--bound-thread-id",
            "thread-cli-activity-fence",
            "--session-allows-approval",
            "false",
            "--session-allows-network",
            "false",
            "--session-allows-write-access",
            "false",
        ],
    );
    assert_eq!(reattached["cli_session"]["session"]["session_epoch"], 2);
    assert_eq!(
        reattached["cli_session"]["session"]["activity_state"],
        "unknown"
    );

    let old_epoch = cbth_failure(
        &home,
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
            "2",
        ],
    );
    assert!(old_epoch.contains("is at epoch 2, not 1"));

    let jumped_revision = cbth_failure(
        &home,
        &[
            "cli",
            "session",
            "note-activity",
            "--managed-session-id",
            &managed_session_id,
            "--session-epoch",
            "2",
            "--activity-state",
            "idle",
            "--activity-revision",
            "100",
        ],
    );
    assert!(jumped_revision.contains("not the next revision"));

    note_cli_session_idle(&home, &managed_session_id);
    let inspected = cbth(
        &home,
        &[
            "cli",
            "session",
            "inspect",
            "--managed-session-id",
            &managed_session_id,
        ],
    );
    assert_eq!(inspected["cli_session"]["activity_state"], "idle");
    assert_eq!(inspected["cli_session"]["activity_revision"], 1);
}

#[test]
fn submit_defaults_to_fail_closed_policy() {
    let home = tempfile::tempdir().expect("temp home");

    let output = cbth(
        &home,
        &[
            "job",
            "submit",
            "--source-thread-id",
            "thread-defaults",
            "--summary",
            "wait for CI",
        ],
    );

    let policy = &output["job"]["delivery_policy"];
    assert_eq!(policy["delivery_read_only"], false);
    assert_eq!(policy["delivery_requires_approval"], true);
    assert_eq!(policy["delivery_requires_network"], true);
    assert_eq!(policy["delivery_requires_write_access"], true);
}

#[test]
fn metadata_policy_can_be_overridden_by_cli_flags() {
    let home = tempfile::tempdir().expect("temp home");
    let metadata_path = home.path().join("metadata.json");
    fs::write(
        &metadata_path,
        r#"{
          "delivery_policy": {
            "read_only": true,
            "requires_approval": false,
            "requires_network": true,
            "requires_write_access": false
          },
          "external_url": "https://example.invalid/build/1"
        }"#,
    )
    .expect("write metadata");

    let metadata_arg = metadata_path.to_string_lossy().to_string();
    let output = cbth(
        &home,
        &[
            "job",
            "submit",
            "--source-thread-id",
            "thread-metadata",
            "--summary",
            "wait for reviewer",
            "--metadata-file",
            &metadata_arg,
            "--delivery-requires-network",
            "false",
        ],
    );

    let job = &output["job"];
    assert_eq!(
        job["metadata"]["external_url"],
        "https://example.invalid/build/1"
    );
    assert_eq!(job["delivery_policy"]["delivery_read_only"], true);
    assert_eq!(job["delivery_policy"]["delivery_requires_approval"], false);
    assert_eq!(job["delivery_policy"]["delivery_requires_network"], false);
    assert_eq!(
        job["delivery_policy"]["delivery_requires_write_access"],
        false
    );
}

#[test]
fn metadata_policy_rejects_unknown_keys() {
    let home = tempfile::tempdir().expect("temp home");
    let metadata_path = home.path().join("metadata.json");
    fs::write(
        &metadata_path,
        r#"{
          "delivery_policy": {
            "read_only": true,
            "requires_approval": false,
            "requires_network": false,
            "requires_write_acess": false
          }
        }"#,
    )
    .expect("write metadata");

    let metadata_arg = metadata_path.to_string_lossy().to_string();
    let stderr = cbth_failure(
        &home,
        &[
            "job",
            "submit",
            "--source-thread-id",
            "thread-unknown-policy",
            "--summary",
            "wait for reviewer",
            "--metadata-file",
            &metadata_arg,
        ],
    );

    assert!(stderr.contains("unknown field"));
}

#[test]
fn metadata_file_must_be_regular_and_bounded() {
    let home = tempfile::tempdir().expect("temp home");
    let metadata_dir = home.path().join("metadata-dir");
    fs::create_dir(&metadata_dir).expect("create metadata dir");
    let metadata_dir_arg = metadata_dir.to_string_lossy().to_string();
    let dir_stderr = cbth_failure(
        &home,
        &[
            "job",
            "submit",
            "--source-thread-id",
            "thread-metadata-dir",
            "--summary",
            "wait for reviewer",
            "--metadata-file",
            &metadata_dir_arg,
        ],
    );
    assert!(dir_stderr.contains("metadata file must be a regular file"));

    let large_metadata_path = home.path().join("large-metadata.json");
    fs::write(&large_metadata_path, vec![b' '; 1024 * 1024 + 1]).expect("write large metadata");
    let large_metadata_arg = large_metadata_path.to_string_lossy().to_string();
    let large_stderr = cbth_failure(
        &home,
        &[
            "job",
            "submit",
            "--source-thread-id",
            "thread-metadata-large",
            "--summary",
            "wait for reviewer",
            "--metadata-file",
            &large_metadata_arg,
        ],
    );
    assert!(large_stderr.contains("metadata file is too large"));
}

#[test]
fn complete_job_ingests_artifact_and_creates_closeable_head_batch() {
    let home = tempfile::tempdir().expect("temp home");
    let result_path = home.path().join("result.txt");
    fs::write(&result_path, "CI passed\n").expect("write result");

    let submit = cbth(
        &home,
        &[
            "job",
            "submit",
            "--source-thread-id",
            "thread-complete",
            "--summary",
            "wait for CI",
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
    let job_id = submit["job"]["job_id"].as_str().expect("job id");
    let result_arg = result_path.to_string_lossy().to_string();

    let completed = cbth(
        &home,
        &[
            "job",
            "complete",
            "--job-id",
            job_id,
            "--result-file",
            &result_arg,
            "--summary",
            "CI passed",
        ],
    );

    let batch = &completed["batch"]["batch"];
    assert_eq!(batch["source_thread_id"], "thread-complete");
    assert_eq!(batch["state"], "open");
    assert_eq!(batch["inline_payload_bytes"], 0);
    assert_eq!(batch["requires_artifact_read"], true);
    assert_eq!(batch["delivery_policy"]["delivery_read_only"], true);

    let artifact = &completed["batch"]["jobs"][0]["artifact"];
    let relative_path = artifact["relative_path"].as_str().expect("relative path");
    let artifact_path = home.path().join(relative_path);
    assert!(artifact_path.exists());
    assert_eq!(
        artifact["sha256"],
        "5bfa1a61c872bc988971fd55dc15dfadd05a8d5d8a0fdca620429b2f229236b0"
    );

    #[cfg(unix)]
    {
        assert_eq!(mode(home.path()), 0o700);
        assert_eq!(mode(&home.path().join("cbth.sqlite3")), 0o600);
        assert_eq!(mode(&artifact_path), 0o600);
    }

    let head = cbth(
        &home,
        &[
            "batch",
            "inspect-head",
            "--source-thread-id",
            "thread-complete",
        ],
    );
    assert_eq!(
        head["batch"]["batch"]["batch_id"],
        completed["batch"]["batch"]["batch_id"]
    );

    let closed = cbth(
        &home,
        &[
            "batch",
            "close-head",
            "--source-thread-id",
            "thread-complete",
            "--reason",
            "operator-closed-unconfirmed",
            "--note",
            "verified by test",
        ],
    );
    assert_eq!(closed["batch"]["batch"]["state"], "closed");
    assert_eq!(
        closed["batch"]["batch"]["close_reason"],
        "operator_closed_unconfirmed"
    );
    let retention_until = closed["batch"]["jobs"][0]["artifact"]["retention_until"]
        .as_i64()
        .expect("retention timestamp");
    let manifest_path = artifact_path
        .parent()
        .expect("artifact dir")
        .join("manifest.json");
    let manifest: Value = serde_json::from_slice(&fs::read(&manifest_path).expect("read manifest"))
        .expect("parse manifest");
    assert_eq!(manifest["retention_until"], retention_until);

    let empty_head = cbth(
        &home,
        &[
            "batch",
            "inspect-head",
            "--source-thread-id",
            "thread-complete",
        ],
    );
    assert!(empty_head["batch"].is_null());

    let sweep_now = (retention_until + 1).to_string();
    let sweep = cbth(&home, &["maintenance", "sweep", "--now", &sweep_now]);
    assert_eq!(sweep["sweep"]["artifacts_deleted"], 1);
    assert!(!artifact_path.exists());
}

#[test]
fn maintenance_sweep_closes_expired_automatic_batches() {
    let home = tempfile::tempdir().expect("temp home");
    let result_path = home.path().join("result.txt");
    fs::write(&result_path, "ready\n").expect("write result");

    let submit = cbth(
        &home,
        &[
            "job",
            "submit",
            "--source-thread-id",
            "thread-expire",
            "--summary",
            "wait for timeout",
        ],
    );
    let job_id = submit["job"]["job_id"].as_str().expect("job id");
    let result_arg = result_path.to_string_lossy().to_string();
    let completed = cbth(
        &home,
        &[
            "job",
            "complete",
            "--job-id",
            job_id,
            "--result-file",
            &result_arg,
            "--redelivery-window-seconds",
            "1",
        ],
    );
    let batch_id = completed["batch"]["batch"]["batch_id"]
        .as_str()
        .expect("batch id");
    let redelivery_window_ends_at = completed["batch"]["batch"]["redelivery_window_ends_at"]
        .as_i64()
        .expect("redelivery window");

    let sweep_now = (redelivery_window_ends_at + 1).to_string();
    let sweep = cbth(&home, &["maintenance", "sweep", "--now", &sweep_now]);
    assert_eq!(sweep["sweep"]["expired_automatic_batches_closed"], 1);
    assert_eq!(sweep["sweep"]["artifact_manifests_synced"], 1);

    let inspected = cbth(&home, &["batch", "inspect", "--batch-id", batch_id]);
    assert_eq!(inspected["batch"]["batch"]["state"], "closed");
    assert_eq!(
        inspected["batch"]["batch"]["close_reason"],
        "redelivery_window_exhausted"
    );
}

#[test]
fn cli_attempt_acceptance_tracks_deadline_and_attempt_count() {
    let home = tempfile::tempdir().expect("temp home");
    let submitted = cbth(
        &home,
        &[
            "job",
            "submit",
            "--source-thread-id",
            "thread-cli-attempt",
            "--summary",
            "wait for CLI continuation",
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
            "--max-delivery-attempts",
            "2",
        ],
    );
    let batch_id = failed["batch"]["batch"]["batch_id"]
        .as_str()
        .expect("batch id");
    let managed_session_id = bind_idle_cli_session(&home, "thread-cli-attempt");

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
            "rpc-request-1",
            "--rpc-correlation-marker",
            "cbth:test-marker-1",
            "--now",
            "1000",
        ],
    );
    let attempt_id = pending["attempt"]["attempt_id"]
        .as_str()
        .expect("attempt id");
    assert_eq!(pending["attempt"]["state"], "accept_pending");
    assert_eq!(pending["attempt"]["generation"], 1);
    assert_eq!(
        pending["attempt"]["delivery_rpc_state"],
        "pending_acceptance"
    );
    let active_activity = cbth(
        &home,
        &[
            "cli",
            "session",
            "note-activity",
            "--managed-session-id",
            &managed_session_id,
            "--session-epoch",
            "1",
            "--activity-state",
            "active",
            "--activity-revision",
            "2",
            "--now",
            "1000",
        ],
    );
    assert_eq!(
        active_activity["cli_session"]["session"]["activity_state"],
        "active"
    );
    let retried_pending = cbth(
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
            "rpc-request-1",
            "--rpc-correlation-marker",
            "cbth:test-marker-1",
            "--now",
            "1001",
        ],
    );
    assert_eq!(
        retried_pending["attempt"]["attempt_id"],
        pending["attempt"]["attempt_id"]
    );
    assert_eq!(retried_pending["attempt"]["created_at"], 1000);
    let retry_wrong_epoch = cbth_failure(
        &home,
        &[
            "attempt",
            "begin-cli-accept",
            "--batch-id",
            batch_id,
            "--managed-session-id",
            &managed_session_id,
            "--session-epoch",
            "2",
            "--rpc-kind",
            "turn-start",
            "--rpc-request-id",
            "rpc-request-1",
            "--rpc-correlation-marker",
            "cbth:test-marker-1",
            "--now",
            "1002",
        ],
    );
    assert!(retry_wrong_epoch.contains("different session epoch"));

    let accepted = cbth(
        &home,
        &[
            "attempt",
            "accept-cli",
            "--attempt-id",
            attempt_id,
            "--delivery-turn-id",
            "turn-1",
            "--observation-window-seconds",
            "60",
            "--now",
            "1005",
        ],
    );
    assert_eq!(accepted["attempt"]["state"], "cooldown");
    assert_eq!(accepted["attempt"]["delivery_rpc_state"], "accepted");
    assert_eq!(accepted["attempt"]["delivery_turn_id"], "turn-1");
    assert_eq!(
        accepted["attempt"]["delivery_observation_state"],
        "tracking"
    );
    assert_eq!(accepted["attempt"]["delivery_observation_deadline"], 1065);

    cbth(
        &home,
        &[
            "attempt",
            "accept-cli",
            "--attempt-id",
            attempt_id,
            "--delivery-turn-id",
            "turn-1",
            "--observation-window-seconds",
            "60",
            "--now",
            "1006",
        ],
    );

    let inspected = cbth(&home, &["attempt", "inspect", "--attempt-id", attempt_id]);
    assert_eq!(inspected["attempt"]["delivery_observation_deadline"], 1065);
    let batch = cbth(&home, &["batch", "inspect", "--batch-id", batch_id]);
    assert_eq!(batch["batch"]["batch"]["delivery_attempt_count"], 1);
}

#[test]
fn cli_attempt_acceptance_rejects_oversized_observation_window() {
    let home = tempfile::tempdir().expect("temp home");
    let submitted = cbth(
        &home,
        &[
            "job",
            "submit",
            "--source-thread-id",
            "thread-cli-observation-bound",
            "--summary",
            "bound CLI observation",
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
        &["job", "fail", "--job-id", job_id, "--reason", "ready"],
    );
    let batch_id = failed["batch"]["batch"]["batch_id"]
        .as_str()
        .expect("batch id");
    let managed_session_id = bind_idle_cli_session(&home, "thread-cli-observation-bound");
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
            "rpc-request-observation-bound",
        ],
    );
    let attempt_id = pending["attempt"]["attempt_id"]
        .as_str()
        .expect("attempt id");

    let stderr = cbth_failure(
        &home,
        &[
            "attempt",
            "accept-cli",
            "--attempt-id",
            attempt_id,
            "--delivery-turn-id",
            "turn-observation-bound",
            "--observation-window-seconds",
            "21601",
        ],
    );
    assert!(stderr.contains("observation_window_seconds must be <= 21600"));
}

#[test]
fn cli_attempt_acceptance_rejects_empty_delivery_turn_id() {
    let home = tempfile::tempdir().expect("temp home");
    let submitted = cbth(
        &home,
        &[
            "job",
            "submit",
            "--source-thread-id",
            "thread-cli-empty-turn",
            "--summary",
            "empty turn id",
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
        &["job", "fail", "--job-id", job_id, "--reason", "ready"],
    );
    let batch_id = failed["batch"]["batch"]["batch_id"]
        .as_str()
        .expect("batch id");
    let managed_session_id = bind_idle_cli_session(&home, "thread-cli-empty-turn");
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
            "rpc-request-empty-turn",
        ],
    );
    let attempt_id = pending["attempt"]["attempt_id"]
        .as_str()
        .expect("attempt id");

    let stderr = cbth_failure(
        &home,
        &[
            "attempt",
            "accept-cli",
            "--attempt-id",
            attempt_id,
            "--delivery-turn-id",
            "",
            "--observation-window-seconds",
            "60",
        ],
    );
    assert!(stderr.contains("delivery_turn_id must not be empty"));
}

#[test]
fn cli_turn_observation_started_keeps_attempt_tracking() {
    let home = tempfile::tempdir().expect("temp home");
    let (batch_id, attempt_id, _managed_session_id) =
        create_accepted_cli_attempt(&home, "thread-cli-turn-started", "turn-started");

    let observed = cbth(
        &home,
        &[
            "attempt",
            "observe-cli-turn",
            "--attempt-id",
            &attempt_id,
            "--delivery-turn-id",
            "turn-started",
            "--turn-event",
            "turn-started",
            "--now",
            "1002",
        ],
    );
    assert_eq!(observed["attempt"]["state"], "cooldown");
    assert_eq!(
        observed["attempt"]["delivery_observation_state"],
        "tracking"
    );
    assert_eq!(
        observed["attempt"]["last_observed_turn_event"],
        "turn_started"
    );
    assert_eq!(observed["attempt"]["last_observed_turn_event_at"], 1002);

    let batch = cbth(&home, &["batch", "inspect", "--batch-id", &batch_id]);
    assert_eq!(batch["batch"]["batch"]["state"], "open");
    assert_eq!(batch["batch"]["batch"]["replay_policy"], "automatic");
}

#[test]
fn cli_turn_observation_completed_closes_batch() {
    let home = tempfile::tempdir().expect("temp home");
    let (batch_id, attempt_id, managed_session_id) =
        create_accepted_cli_attempt(&home, "thread-cli-turn-completed", "turn-completed");

    let observed = cbth(
        &home,
        &[
            "attempt",
            "observe-cli-turn",
            "--attempt-id",
            &attempt_id,
            "--delivery-turn-id",
            "turn-completed",
            "--turn-event",
            "turn-completed",
            "--now",
            "1002",
        ],
    );
    assert_eq!(observed["attempt"]["state"], "closed");
    assert_eq!(
        observed["attempt"]["delivery_observation_state"],
        "completed"
    );
    assert_eq!(
        observed["attempt"]["last_observed_turn_event"],
        "turn_completed"
    );

    let batch = cbth(&home, &["batch", "inspect", "--batch-id", &batch_id]);
    assert_eq!(batch["batch"]["batch"]["state"], "closed");
    assert_eq!(batch["batch"]["batch"]["close_reason"], "delivered");
    let head = cbth(
        &home,
        &[
            "batch",
            "inspect-head",
            "--source-thread-id",
            "thread-cli-turn-completed",
        ],
    );
    assert!(head["batch"].is_null());

    let session = cbth(
        &home,
        &[
            "cli",
            "session",
            "inspect",
            "--managed-session-id",
            &managed_session_id,
        ],
    );
    assert_eq!(session["cli_session"]["session_epoch"], 2);
    assert_eq!(session["cli_session"]["activity_state"], "unknown");

    let retried = cbth(
        &home,
        &[
            "attempt",
            "observe-cli-turn",
            "--attempt-id",
            &attempt_id,
            "--delivery-turn-id",
            "turn-completed",
            "--turn-event",
            "turn-completed",
            "--now",
            "1003",
        ],
    );
    assert_eq!(retried["attempt"]["state"], "closed");
    assert_eq!(
        retried["attempt"]["last_observed_turn_event_at"],
        observed["attempt"]["last_observed_turn_event_at"]
    );
}

#[test]
fn cli_turn_observation_negative_terminal_event_manualizes_batch() {
    let home = tempfile::tempdir().expect("temp home");
    let (batch_id, attempt_id, managed_session_id) =
        create_accepted_cli_attempt(&home, "thread-cli-turn-failed", "turn-failed");

    let observed = cbth(
        &home,
        &[
            "attempt",
            "observe-cli-turn",
            "--attempt-id",
            &attempt_id,
            "--delivery-turn-id",
            "turn-failed",
            "--turn-event",
            "turn-failed",
            "--now",
            "1002",
        ],
    );
    assert_eq!(observed["attempt"]["state"], "abandoned");
    assert_eq!(
        observed["attempt"]["delivery_observation_state"],
        "abandoned"
    );
    assert_eq!(
        observed["attempt"]["last_observed_turn_event"],
        "turn_failed"
    );
    assert_eq!(observed["attempt"]["abandoned_at"], 1002);

    let batch = cbth(&home, &["batch", "inspect", "--batch-id", &batch_id]);
    assert_eq!(batch["batch"]["batch"]["state"], "open");
    assert_eq!(
        batch["batch"]["batch"]["replay_policy"],
        "manual_resolution_only"
    );
    let session = cbth(
        &home,
        &[
            "cli",
            "session",
            "inspect",
            "--managed-session-id",
            &managed_session_id,
        ],
    );
    assert_eq!(session["cli_session"]["session_epoch"], 2);
    assert_eq!(session["cli_session"]["activity_state"], "unknown");
}

#[test]
fn cli_turn_observation_after_deadline_manualizes_without_delivery() {
    let home = tempfile::tempdir().expect("temp home");
    let (batch_id, attempt_id, _managed_session_id) =
        create_accepted_cli_attempt(&home, "thread-cli-turn-late", "turn-late");

    let observed = cbth(
        &home,
        &[
            "attempt",
            "observe-cli-turn",
            "--attempt-id",
            &attempt_id,
            "--delivery-turn-id",
            "turn-late",
            "--turn-event",
            "turn-completed",
            "--now",
            "1062",
        ],
    );
    assert_eq!(observed["attempt"]["state"], "abandoned");
    assert_eq!(observed["attempt"]["delivery_observation_state"], "expired");
    assert_eq!(
        observed["attempt"]["last_observed_turn_event"],
        "turn_completed"
    );

    let batch = cbth(&home, &["batch", "inspect", "--batch-id", &batch_id]);
    assert_eq!(batch["batch"]["batch"]["state"], "open");
    assert_eq!(
        batch["batch"]["batch"]["replay_policy"],
        "manual_resolution_only"
    );
    assert!(batch["batch"]["batch"]["close_reason"].is_null());
}

#[test]
fn cli_turn_observation_at_deadline_manualizes_without_delivery() {
    let home = tempfile::tempdir().expect("temp home");
    let (batch_id, attempt_id, _managed_session_id) =
        create_accepted_cli_attempt(&home, "thread-cli-turn-deadline", "turn-deadline");

    let observed = cbth(
        &home,
        &[
            "attempt",
            "observe-cli-turn",
            "--attempt-id",
            &attempt_id,
            "--delivery-turn-id",
            "turn-deadline",
            "--turn-event",
            "turn-completed",
            "--now",
            "1061",
        ],
    );
    assert_eq!(observed["attempt"]["state"], "abandoned");
    assert_eq!(observed["attempt"]["delivery_observation_state"], "expired");
    assert_eq!(
        observed["attempt"]["last_observed_turn_event"],
        "turn_completed"
    );

    let batch = cbth(&home, &["batch", "inspect", "--batch-id", &batch_id]);
    assert_eq!(batch["batch"]["batch"]["state"], "open");
    assert_eq!(
        batch["batch"]["batch"]["replay_policy"],
        "manual_resolution_only"
    );
    assert!(batch["batch"]["batch"]["close_reason"].is_null());
}

#[test]
fn cli_turn_observation_requires_matching_delivery_turn() {
    let home = tempfile::tempdir().expect("temp home");
    let (_batch_id, attempt_id, _managed_session_id) =
        create_accepted_cli_attempt(&home, "thread-cli-turn-mismatch", "turn-real");

    let stderr = cbth_failure(
        &home,
        &[
            "attempt",
            "observe-cli-turn",
            "--attempt-id",
            &attempt_id,
            "--delivery-turn-id",
            "turn-other",
            "--turn-event",
            "turn-completed",
        ],
    );
    assert!(stderr.contains("different delivery turn"));
}

#[test]
fn cli_turn_observation_rejects_empty_delivery_turn_id() {
    let home = tempfile::tempdir().expect("temp home");
    let (_batch_id, attempt_id, _managed_session_id) =
        create_accepted_cli_attempt(&home, "thread-cli-empty-observed-turn", "turn-real");

    let stderr = cbth_failure(
        &home,
        &[
            "attempt",
            "observe-cli-turn",
            "--attempt-id",
            &attempt_id,
            "--delivery-turn-id",
            "",
            "--turn-event",
            "turn-completed",
        ],
    );
    assert!(stderr.contains("delivery_turn_id must not be empty"));
}

#[test]
fn cli_turn_observation_with_stale_session_epoch_manualizes_batch() {
    let home = tempfile::tempdir().expect("temp home");
    let (batch_id, attempt_id, managed_session_id) =
        create_accepted_cli_attempt(&home, "thread-cli-turn-stale-epoch", "turn-stale-epoch");

    cbth(
        &home,
        &[
            "cli",
            "session",
            "bind",
            "--bound-thread-id",
            "thread-cli-turn-stale-epoch",
            "--session-allows-approval",
            "false",
            "--session-allows-network",
            "false",
            "--session-allows-write-access",
            "false",
            "--now",
            "1002",
        ],
    );
    let observed = cbth(
        &home,
        &[
            "attempt",
            "observe-cli-turn",
            "--attempt-id",
            &attempt_id,
            "--delivery-turn-id",
            "turn-stale-epoch",
            "--turn-event",
            "turn-completed",
            "--now",
            "1003",
        ],
    );
    assert_eq!(observed["attempt"]["state"], "abandoned");
    assert_eq!(
        observed["attempt"]["delivery_observation_state"],
        "abandoned"
    );
    assert_eq!(
        observed["attempt"]["last_observed_turn_event"],
        "turn_completed"
    );

    let batch = cbth(&home, &["batch", "inspect", "--batch-id", &batch_id]);
    assert_eq!(batch["batch"]["batch"]["state"], "open");
    assert_eq!(
        batch["batch"]["batch"]["replay_policy"],
        "manual_resolution_only"
    );
    let session = cbth(
        &home,
        &[
            "cli",
            "session",
            "inspect",
            "--managed-session-id",
            &managed_session_id,
        ],
    );
    assert_eq!(session["cli_session"]["session_epoch"], 2);
    assert_eq!(session["cli_session"]["activity_state"], "unknown");
}

#[test]
fn cli_turn_completion_after_continuity_loss_does_not_deliver() {
    let home = tempfile::tempdir().expect("temp home");
    let (batch_id, attempt_id, _managed_session_id) = create_accepted_cli_attempt(
        &home,
        "thread-cli-turn-continuity-loss",
        "turn-continuity-loss",
    );

    cbth(
        &home,
        &[
            "cli",
            "session",
            "bind",
            "--bound-thread-id",
            "thread-cli-turn-continuity-loss",
            "--session-allows-approval",
            "false",
            "--session-allows-network",
            "false",
            "--session-allows-write-access",
            "false",
            "--now",
            "1002",
        ],
    );
    let abandoned = cbth(&home, &["attempt", "inspect", "--attempt-id", &attempt_id]);
    assert_eq!(abandoned["attempt"]["state"], "abandoned");
    assert_eq!(
        abandoned["attempt"]["delivery_observation_state"],
        "abandoned"
    );
    assert!(abandoned["attempt"]["last_observed_turn_event"].is_null());

    let completed = cbth(
        &home,
        &[
            "attempt",
            "observe-cli-turn",
            "--attempt-id",
            &attempt_id,
            "--delivery-turn-id",
            "turn-continuity-loss",
            "--turn-event",
            "turn-completed",
            "--now",
            "1003",
        ],
    );
    assert_eq!(completed["attempt"]["state"], "abandoned");
    assert_eq!(
        completed["attempt"]["delivery_observation_state"],
        "abandoned"
    );
    assert_eq!(
        completed["attempt"]["last_observed_turn_event"],
        "turn_completed"
    );

    let batch = cbth(&home, &["batch", "inspect", "--batch-id", &batch_id]);
    assert_eq!(batch["batch"]["batch"]["state"], "open");
    assert_eq!(
        batch["batch"]["batch"]["replay_policy"],
        "manual_resolution_only"
    );
    assert!(batch["batch"]["batch"]["close_reason"].is_null());
}

#[test]
fn cli_turn_completion_after_continuity_loss_and_sweep_does_not_deliver() {
    let home = tempfile::tempdir().expect("temp home");
    let (batch_id, attempt_id, _managed_session_id) = create_accepted_cli_attempt(
        &home,
        "thread-cli-turn-continuity-loss-sweep",
        "turn-continuity-loss-sweep",
    );

    cbth(
        &home,
        &[
            "cli",
            "session",
            "bind",
            "--bound-thread-id",
            "thread-cli-turn-continuity-loss-sweep",
            "--session-allows-approval",
            "false",
            "--session-allows-network",
            "false",
            "--session-allows-write-access",
            "false",
            "--now",
            "1002",
        ],
    );
    cbth(&home, &["maintenance", "sweep", "--now", "1061"]);

    let expired = cbth(&home, &["attempt", "inspect", "--attempt-id", &attempt_id]);
    assert_eq!(expired["attempt"]["state"], "abandoned");
    assert_eq!(
        expired["attempt"]["delivery_observation_state"],
        "abandoned"
    );
    assert!(expired["attempt"]["last_observed_turn_event"].is_null());

    let completed = cbth(
        &home,
        &[
            "attempt",
            "observe-cli-turn",
            "--attempt-id",
            &attempt_id,
            "--delivery-turn-id",
            "turn-continuity-loss-sweep",
            "--turn-event",
            "turn-completed",
            "--now",
            "1060",
        ],
    );
    assert_eq!(completed["attempt"]["state"], "abandoned");
    assert_eq!(
        completed["attempt"]["delivery_observation_state"],
        "abandoned"
    );
    assert_eq!(
        completed["attempt"]["last_observed_turn_event"],
        "turn_completed"
    );

    let batch = cbth(&home, &["batch", "inspect", "--batch-id", &batch_id]);
    assert_eq!(batch["batch"]["batch"]["state"], "open");
    assert_eq!(
        batch["batch"]["batch"]["replay_policy"],
        "manual_resolution_only"
    );
    assert!(batch["batch"]["batch"]["close_reason"].is_null());
}

#[test]
fn cli_attempt_begin_requires_rpc_request_id() {
    let home = tempfile::tempdir().expect("temp home");
    let stderr = cbth_failure(
        &home,
        &[
            "attempt",
            "begin-cli-accept",
            "--batch-id",
            "batch-missing-rpc-id",
            "--managed-session-id",
            "managed-cli-missing-rpc-id",
            "--session-epoch",
            "1",
            "--rpc-kind",
            "turn-start",
        ],
    );
    assert!(stderr.contains("--rpc-request-id"));
}

#[test]
fn cli_attempt_begin_requires_bound_managed_session() {
    let home = tempfile::tempdir().expect("temp home");
    let submitted = cbth(
        &home,
        &[
            "job",
            "submit",
            "--source-thread-id",
            "thread-cli-session-required",
            "--summary",
            "session required",
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
        &["job", "fail", "--job-id", job_id, "--reason", "ready"],
    );
    let batch_id = failed["batch"]["batch"]["batch_id"]
        .as_str()
        .expect("batch id");

    let stderr = cbth_failure(
        &home,
        &[
            "attempt",
            "begin-cli-accept",
            "--batch-id",
            batch_id,
            "--managed-session-id",
            "missing-managed-session",
            "--session-epoch",
            "1",
            "--rpc-kind",
            "turn-start",
            "--rpc-request-id",
            "rpc-request-session-required",
        ],
    );
    assert!(stderr.contains("CLI managed session not found"));
}

#[test]
fn cli_attempt_idempotent_paths_require_current_managed_session() {
    let home = tempfile::tempdir().expect("temp home");
    let submitted = cbth(
        &home,
        &[
            "job",
            "submit",
            "--source-thread-id",
            "thread-cli-legacy-attempt",
            "--summary",
            "legacy pending attempt",
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
        &["job", "fail", "--job-id", job_id, "--reason", "ready"],
    );
    let batch_id = failed["batch"]["batch"]["batch_id"]
        .as_str()
        .expect("batch id");

    let conn = Connection::open(home.path().join("cbth.sqlite3")).expect("open db");
    conn.execute(
        "INSERT INTO delivery_attempts (
            attempt_id, batch_id, source_thread_id, adapter_kind, state,
            generation, delivery_rpc_request_id, delivery_rpc_kind,
            delivery_rpc_state, delivery_rpc_started_at, managed_session_id,
            session_epoch, created_at, updated_at
        ) VALUES (
            'legacy-cli-attempt', ?, 'thread-cli-legacy-attempt', 'cli',
            'accept_pending', 1, 'rpc-request-legacy', 'turn_start',
            'pending_acceptance', 100, 'missing-managed-session', 1, 100, 100
        )",
        params![batch_id],
    )
    .expect("insert legacy attempt");
    drop(conn);

    let retry = cbth_failure(
        &home,
        &[
            "attempt",
            "begin-cli-accept",
            "--batch-id",
            batch_id,
            "--managed-session-id",
            "missing-managed-session",
            "--session-epoch",
            "1",
            "--rpc-kind",
            "turn-start",
            "--rpc-request-id",
            "rpc-request-legacy",
        ],
    );
    assert!(retry.contains("CLI managed session not found"));

    let accept = cbth_failure(
        &home,
        &[
            "attempt",
            "accept-cli",
            "--attempt-id",
            "legacy-cli-attempt",
            "--delivery-turn-id",
            "turn-legacy",
            "--observation-window-seconds",
            "60",
        ],
    );
    assert!(accept.contains("CLI managed session not found"));
}

#[test]
fn cli_attempt_idempotent_paths_require_recorded_delivery_proof() {
    let home = tempfile::tempdir().expect("temp home");
    let submitted = cbth(
        &home,
        &[
            "job",
            "submit",
            "--source-thread-id",
            "thread-cli-legacy-proof",
            "--summary",
            "legacy proofless pending attempt",
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
        &["job", "fail", "--job-id", job_id, "--reason", "ready"],
    );
    let batch_id = failed["batch"]["batch"]["batch_id"]
        .as_str()
        .expect("batch id");
    let managed_session_id = bind_idle_cli_session(&home, "thread-cli-legacy-proof");

    let conn = Connection::open(home.path().join("cbth.sqlite3")).expect("open db");
    conn.execute(
        "INSERT INTO delivery_attempts (
            attempt_id, batch_id, source_thread_id, adapter_kind, state,
            generation, delivery_rpc_request_id, delivery_rpc_kind,
            delivery_rpc_state, delivery_rpc_started_at, managed_session_id,
            session_epoch, created_at, updated_at
        ) VALUES (
            'legacy-proofless-cli-attempt', ?, 'thread-cli-legacy-proof', 'cli',
            'accept_pending', 1, 'rpc-request-legacy-proof', 'turn_start',
            'pending_acceptance', 100, ?, 1, 100, 100
        )",
        params![batch_id, managed_session_id],
    )
    .expect("insert proofless legacy attempt");
    drop(conn);

    let retry = cbth_failure(
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
            "rpc-request-legacy-proof",
        ],
    );
    assert!(retry.contains("was not created with a CLI detached delivery proof"));

    let accept = cbth_failure(
        &home,
        &[
            "attempt",
            "accept-cli",
            "--attempt-id",
            "legacy-proofless-cli-attempt",
            "--delivery-turn-id",
            "turn-legacy-proof",
            "--observation-window-seconds",
            "60",
        ],
    );
    assert!(accept.contains("was not created with a CLI detached delivery proof"));
}

#[test]
fn delayed_cli_completion_requires_recorded_delivery_proof() {
    let home = tempfile::tempdir().expect("temp home");
    let submitted = cbth(
        &home,
        &[
            "job",
            "submit",
            "--source-thread-id",
            "thread-cli-delayed-legacy-proof",
            "--summary",
            "legacy proofless delayed completion",
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
        &["job", "fail", "--job-id", job_id, "--reason", "ready"],
    );
    let batch_id = failed["batch"]["batch"]["batch_id"]
        .as_str()
        .expect("batch id");
    let managed_session_id = bind_idle_cli_session(&home, "thread-cli-delayed-legacy-proof");

    let conn = Connection::open(home.path().join("cbth.sqlite3")).expect("open db");
    conn.execute(
        "INSERT INTO delivery_attempts (
            attempt_id, batch_id, source_thread_id, adapter_kind, state,
            generation, delivery_rpc_request_id, delivery_rpc_kind,
            delivery_rpc_state, delivery_rpc_started_at, managed_session_id,
            session_epoch, delivery_turn_id, delivery_accepted_at,
            delivery_observation_state, delivery_observation_deadline,
            created_at, updated_at, abandoned_at
        ) VALUES (
            'legacy-proofless-delayed-cli-attempt', ?,
            'thread-cli-delayed-legacy-proof', 'cli', 'abandoned', 1,
            'rpc-request-legacy-delayed-proof', 'turn_start', 'accepted',
            1000, ?, 1, 'turn-legacy-delayed-proof', 1001, 'expired',
            1061, 1000, 1061, 1061
        )",
        params![batch_id, managed_session_id],
    )
    .expect("insert proofless delayed legacy attempt");
    conn.execute(
        "UPDATE batches
         SET replay_policy = 'manual_resolution_only',
             updated_at = 1061
         WHERE batch_id = ?",
        params![batch_id],
    )
    .expect("manualize batch");
    drop(conn);

    let observed = cbth(
        &home,
        &[
            "attempt",
            "observe-cli-turn",
            "--attempt-id",
            "legacy-proofless-delayed-cli-attempt",
            "--delivery-turn-id",
            "turn-legacy-delayed-proof",
            "--turn-event",
            "turn-completed",
            "--now",
            "1060",
        ],
    );
    assert_eq!(observed["attempt"]["state"], "abandoned");
    assert_eq!(observed["attempt"]["delivery_observation_state"], "expired");
    assert_eq!(
        observed["attempt"]["last_observed_turn_event"],
        "turn_completed"
    );

    let batch = cbth(&home, &["batch", "inspect", "--batch-id", batch_id]);
    assert_eq!(batch["batch"]["batch"]["state"], "open");
    assert_eq!(
        batch["batch"]["batch"]["replay_policy"],
        "manual_resolution_only"
    );
    assert!(batch["batch"]["batch"]["close_reason"].is_null());
}

#[test]
fn cli_attempt_begin_requires_idle_managed_session() {
    let home = tempfile::tempdir().expect("temp home");
    let submitted = cbth(
        &home,
        &[
            "job",
            "submit",
            "--source-thread-id",
            "thread-cli-idle-required",
            "--summary",
            "idle required",
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
        &["job", "fail", "--job-id", job_id, "--reason", "ready"],
    );
    let batch_id = failed["batch"]["batch"]["batch_id"]
        .as_str()
        .expect("batch id");
    let managed_session_id = bind_cli_session(&home, "thread-cli-idle-required");
    note_cli_session_minimum_capabilities(&home, &managed_session_id);

    let stderr = cbth_failure(
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
            "rpc-request-idle-required",
        ],
    );
    assert!(stderr.contains("activity state is unknown, not idle"));
}

#[test]
fn cli_attempt_begin_requires_minimum_capability_probe() {
    let home = tempfile::tempdir().expect("temp home");
    let submitted = cbth(
        &home,
        &[
            "job",
            "submit",
            "--source-thread-id",
            "thread-cli-capability-required",
            "--summary",
            "capabilities required",
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
        &["job", "fail", "--job-id", job_id, "--reason", "ready"],
    );
    let batch_id = failed["batch"]["batch"]["batch_id"]
        .as_str()
        .expect("batch id");
    let managed_session_id = bind_cli_session(&home, "thread-cli-capability-required");
    note_cli_session_idle(&home, &managed_session_id);

    let stderr = cbth_failure(
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
            "rpc-request-capability-required",
        ],
    );
    assert!(stderr.contains("minimum turn_start capability probe"));
}

#[test]
fn cli_attempt_begin_rejects_fail_closed_batch_policy() {
    let home = tempfile::tempdir().expect("temp home");
    let submitted = cbth(
        &home,
        &[
            "job",
            "submit",
            "--source-thread-id",
            "thread-cli-policy",
            "--summary",
            "fail closed delivery",
        ],
    );
    let job_id = submitted["job"]["job_id"].as_str().expect("job id");
    let failed = cbth(
        &home,
        &["job", "fail", "--job-id", job_id, "--reason", "ready"],
    );
    let batch_id = failed["batch"]["batch"]["batch_id"]
        .as_str()
        .expect("batch id");

    let stderr = cbth_failure(
        &home,
        &[
            "attempt",
            "begin-cli-accept",
            "--batch-id",
            batch_id,
            "--managed-session-id",
            "managed-cli-policy",
            "--session-epoch",
            "1",
            "--rpc-kind",
            "turn-start",
            "--rpc-request-id",
            "rpc-request-policy",
        ],
    );
    assert!(stderr.contains("not eligible for detached CLI delivery"));
}

#[test]
fn cli_attempt_begin_requires_thread_head_batch() {
    let home = tempfile::tempdir().expect("temp home");
    let first = cbth(
        &home,
        &[
            "job",
            "submit",
            "--source-thread-id",
            "thread-cli-fifo",
            "--summary",
            "first",
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
    let second = cbth(
        &home,
        &[
            "job",
            "submit",
            "--source-thread-id",
            "thread-cli-fifo",
            "--summary",
            "second",
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
    let first_job_id = first["job"]["job_id"].as_str().expect("first job id");
    let second_job_id = second["job"]["job_id"].as_str().expect("second job id");
    cbth(
        &home,
        &[
            "job",
            "fail",
            "--job-id",
            first_job_id,
            "--reason",
            "first ready",
        ],
    );
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
    let second_batch_id = second_failed["batch"]["batch"]["batch_id"]
        .as_str()
        .expect("second batch id");

    let stderr = cbth_failure(
        &home,
        &[
            "attempt",
            "begin-cli-accept",
            "--batch-id",
            second_batch_id,
            "--managed-session-id",
            "managed-cli-fifo",
            "--session-epoch",
            "1",
            "--rpc-kind",
            "turn-start",
            "--rpc-request-id",
            "rpc-request-fifo",
        ],
    );
    assert!(stderr.contains("is not the head batch"));
}

#[test]
fn maintenance_sweep_does_not_close_active_cli_observation_before_deadline() {
    let home = tempfile::tempdir().expect("temp home");
    let submitted = cbth(
        &home,
        &[
            "job",
            "submit",
            "--source-thread-id",
            "thread-cli-window",
            "--summary",
            "accepted observation beats redelivery window",
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
            "1",
        ],
    );
    let batch = &failed["batch"]["batch"];
    let batch_id = batch["batch_id"].as_str().expect("batch id");
    let sweep_now = (batch["redelivery_window_ends_at"]
        .as_i64()
        .expect("redelivery window")
        + 1)
    .to_string();
    let managed_session_id = bind_idle_cli_session(&home, "thread-cli-window");
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
            "rpc-request-window",
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
            "turn-window",
            "--observation-window-seconds",
            "60",
        ],
    );

    let sweep = cbth(&home, &["maintenance", "sweep", "--now", &sweep_now]);
    assert_eq!(sweep["sweep"]["expired_automatic_batches_closed"], 0);
    assert_eq!(sweep["sweep"]["expired_cli_observations_abandoned"], 0);

    let attempt = cbth(&home, &["attempt", "inspect", "--attempt-id", attempt_id]);
    assert_eq!(attempt["attempt"]["state"], "cooldown");
    assert_eq!(attempt["attempt"]["delivery_observation_state"], "tracking");
    let inspected = cbth(&home, &["batch", "inspect", "--batch-id", batch_id]);
    assert_eq!(inspected["batch"]["batch"]["state"], "open");
    assert_eq!(inspected["batch"]["batch"]["replay_policy"], "automatic");
}

#[test]
fn maintenance_sweep_abandons_stale_cli_accept_pending_attempt() {
    let home = tempfile::tempdir().expect("temp home");
    let submitted = cbth(
        &home,
        &[
            "job",
            "submit",
            "--source-thread-id",
            "thread-cli-stale-accept",
            "--summary",
            "stale accept pending",
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
            "3600",
        ],
    );
    let batch_id = failed["batch"]["batch"]["batch_id"]
        .as_str()
        .expect("batch id");
    let managed_session_id = bind_idle_cli_session(&home, "thread-cli-stale-accept");
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
            "rpc-request-stale",
            "--now",
            "100",
        ],
    );
    let attempt_id = pending["attempt"]["attempt_id"]
        .as_str()
        .expect("attempt id");

    let sweep = cbth(&home, &["maintenance", "sweep", "--now", "401"]);
    assert_eq!(sweep["sweep"]["stale_cli_acceptances_abandoned"], 1);

    let attempt = cbth(&home, &["attempt", "inspect", "--attempt-id", attempt_id]);
    assert_eq!(attempt["attempt"]["state"], "abandoned");
    assert_eq!(attempt["attempt"]["delivery_rpc_state"], "unknown");
    assert_eq!(
        attempt["attempt"]["delivery_observation_state"],
        "abandoned"
    );
    let inspected = cbth(&home, &["batch", "inspect", "--batch-id", batch_id]);
    assert_eq!(
        inspected["batch"]["batch"]["replay_policy"],
        "manual_resolution_only"
    );
    assert_eq!(inspected["batch"]["batch"]["state"], "open");
    let session = cbth(
        &home,
        &[
            "cli",
            "session",
            "inspect",
            "--managed-session-id",
            &managed_session_id,
        ],
    );
    assert_eq!(session["cli_session"]["session_epoch"], 2);
    assert_eq!(session["cli_session"]["activity_state"], "unknown");

    let second = cbth(
        &home,
        &[
            "job",
            "submit",
            "--source-thread-id",
            "thread-cli-stale-accept",
            "--summary",
            "second after stale accept",
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
    let second_job_id = second["job"]["job_id"].as_str().expect("second job id");
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
    let second_batch_id = second_failed["batch"]["batch"]["batch_id"]
        .as_str()
        .expect("second batch id");
    cbth(
        &home,
        &[
            "batch",
            "close-head",
            "--source-thread-id",
            "thread-cli-stale-accept",
            "--reason",
            "operator-closed-unconfirmed",
        ],
    );
    let missing_capabilities = cbth_failure(
        &home,
        &[
            "attempt",
            "begin-cli-accept",
            "--batch-id",
            second_batch_id,
            "--managed-session-id",
            &managed_session_id,
            "--session-epoch",
            "2",
            "--rpc-kind",
            "turn-start",
            "--rpc-request-id",
            "rpc-request-stale-next-before-proof",
        ],
    );
    assert!(missing_capabilities.contains("minimum turn_start capability probe"));
    note_cli_session_minimum_capabilities(&home, &managed_session_id);
    let not_idle = cbth_failure(
        &home,
        &[
            "attempt",
            "begin-cli-accept",
            "--batch-id",
            second_batch_id,
            "--managed-session-id",
            &managed_session_id,
            "--session-epoch",
            "2",
            "--rpc-kind",
            "turn-start",
            "--rpc-request-id",
            "rpc-request-stale-next-after-capabilities",
        ],
    );
    assert!(not_idle.contains("not idle"));
    note_cli_session_idle(&home, &managed_session_id);
    let second_attempt = cbth(
        &home,
        &[
            "attempt",
            "begin-cli-accept",
            "--batch-id",
            second_batch_id,
            "--managed-session-id",
            &managed_session_id,
            "--session-epoch",
            "2",
            "--rpc-kind",
            "turn-start",
            "--rpc-request-id",
            "rpc-request-stale-next-after-proof",
        ],
    );
    assert_eq!(second_attempt["attempt"]["state"], "accept_pending");
}

#[test]
fn operator_close_releases_active_cli_attempt_for_next_head_batch() {
    let home = tempfile::tempdir().expect("temp home");
    let first = cbth(
        &home,
        &[
            "job",
            "submit",
            "--source-thread-id",
            "thread-cli-close-release",
            "--summary",
            "first",
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
    let second = cbth(
        &home,
        &[
            "job",
            "submit",
            "--source-thread-id",
            "thread-cli-close-release",
            "--summary",
            "second",
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
    let first_job_id = first["job"]["job_id"].as_str().expect("first job id");
    let second_job_id = second["job"]["job_id"].as_str().expect("second job id");
    let first_failed = cbth(
        &home,
        &[
            "job",
            "fail",
            "--job-id",
            first_job_id,
            "--reason",
            "first ready",
        ],
    );
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
    let first_batch_id = first_failed["batch"]["batch"]["batch_id"]
        .as_str()
        .expect("first batch id");
    let second_batch_id = second_failed["batch"]["batch"]["batch_id"]
        .as_str()
        .expect("second batch id");
    let managed_session_id = bind_idle_cli_session(&home, "thread-cli-close-release");
    let first_attempt = cbth(
        &home,
        &[
            "attempt",
            "begin-cli-accept",
            "--batch-id",
            first_batch_id,
            "--managed-session-id",
            &managed_session_id,
            "--session-epoch",
            "1",
            "--rpc-kind",
            "turn-start",
            "--rpc-request-id",
            "rpc-request-close-release-1",
        ],
    );
    let first_attempt_id = first_attempt["attempt"]["attempt_id"]
        .as_str()
        .expect("first attempt id");

    cbth(
        &home,
        &[
            "batch",
            "close-head",
            "--source-thread-id",
            "thread-cli-close-release",
            "--reason",
            "operator-closed-unconfirmed",
        ],
    );
    let first_attempt = cbth(
        &home,
        &["attempt", "inspect", "--attempt-id", first_attempt_id],
    );
    assert_eq!(first_attempt["attempt"]["state"], "closed");

    let session = cbth(
        &home,
        &[
            "cli",
            "session",
            "inspect",
            "--managed-session-id",
            &managed_session_id,
        ],
    );
    assert_eq!(session["cli_session"]["session_epoch"], 2);
    assert_eq!(session["cli_session"]["activity_state"], "unknown");
    let old_epoch = cbth_failure(
        &home,
        &[
            "attempt",
            "begin-cli-accept",
            "--batch-id",
            second_batch_id,
            "--managed-session-id",
            &managed_session_id,
            "--session-epoch",
            "1",
            "--rpc-kind",
            "turn-start",
            "--rpc-request-id",
            "rpc-request-close-release-old-epoch",
        ],
    );
    assert!(old_epoch.contains("is at epoch 2, not 1"));
    let missing_capabilities = cbth_failure(
        &home,
        &[
            "attempt",
            "begin-cli-accept",
            "--batch-id",
            second_batch_id,
            "--managed-session-id",
            &managed_session_id,
            "--session-epoch",
            "2",
            "--rpc-kind",
            "turn-start",
            "--rpc-request-id",
            "rpc-request-close-release-before-proof",
        ],
    );
    assert!(missing_capabilities.contains("minimum turn_start capability probe"));
    note_cli_session_minimum_capabilities(&home, &managed_session_id);
    let not_idle = cbth_failure(
        &home,
        &[
            "attempt",
            "begin-cli-accept",
            "--batch-id",
            second_batch_id,
            "--managed-session-id",
            &managed_session_id,
            "--session-epoch",
            "2",
            "--rpc-kind",
            "turn-start",
            "--rpc-request-id",
            "rpc-request-close-release-after-capabilities",
        ],
    );
    assert!(not_idle.contains("not idle"));
    note_cli_session_idle(&home, &managed_session_id);
    let second_attempt = cbth(
        &home,
        &[
            "attempt",
            "begin-cli-accept",
            "--batch-id",
            second_batch_id,
            "--managed-session-id",
            &managed_session_id,
            "--session-epoch",
            "2",
            "--rpc-kind",
            "turn-start",
            "--rpc-request-id",
            "rpc-request-close-release-2",
        ],
    );
    assert_eq!(second_attempt["attempt"]["state"], "accept_pending");
}

#[test]
fn maintenance_sweep_expires_cli_observation_to_manual_resolution() {
    let home = tempfile::tempdir().expect("temp home");
    let submitted = cbth(
        &home,
        &[
            "job",
            "submit",
            "--source-thread-id",
            "thread-cli-expiry",
            "--summary",
            "wait for CLI observation expiry",
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
            "1",
        ],
    );
    let failed_batch = &failed["batch"]["batch"];
    let batch_id = failed_batch["batch_id"].as_str().expect("batch id");
    let redelivery_window_ends_at = failed_batch["redelivery_window_ends_at"]
        .as_i64()
        .expect("redelivery window");
    let begin_now = redelivery_window_ends_at + 1;
    let accept_now = begin_now + 1;
    let sweep_now = accept_now + 6;
    let begin_now = begin_now.to_string();
    let accept_now = accept_now.to_string();
    let sweep_now = sweep_now.to_string();
    let managed_session_id = bind_idle_cli_session(&home, "thread-cli-expiry");
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
            "rpc-request-expiry",
            "--now",
            &begin_now,
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
            "turn-expired",
            "--observation-window-seconds",
            "5",
            "--now",
            &accept_now,
        ],
    );

    let sweep = cbth(&home, &["maintenance", "sweep", "--now", &sweep_now]);
    assert_eq!(sweep["sweep"]["expired_cli_observations_abandoned"], 1);
    assert_eq!(sweep["sweep"]["expired_manual_batches_closed"], 0);

    let attempt = cbth(&home, &["attempt", "inspect", "--attempt-id", attempt_id]);
    assert_eq!(attempt["attempt"]["state"], "abandoned");
    assert_eq!(attempt["attempt"]["delivery_observation_state"], "expired");
    assert_eq!(
        attempt["attempt"]["abandoned_at"],
        sweep_now.parse::<i64>().expect("sweep now")
    );

    let batch = cbth(&home, &["batch", "inspect", "--batch-id", batch_id]);
    assert_eq!(batch["batch"]["batch"]["state"], "open");
    assert_eq!(
        batch["batch"]["batch"]["replay_policy"],
        "manual_resolution_only"
    );
    assert_eq!(batch["batch"]["batch"]["delivery_attempt_count"], 1);
    assert!(
        batch["batch"]["batch"]["redelivery_window_ends_at"]
            .as_i64()
            .expect("manual window")
            > sweep_now.parse::<i64>().expect("sweep now")
    );
    let session = cbth(
        &home,
        &[
            "cli",
            "session",
            "inspect",
            "--managed-session-id",
            &managed_session_id,
        ],
    );
    assert_eq!(session["cli_session"]["session_epoch"], 2);
    assert_eq!(session["cli_session"]["activity_state"], "unknown");
}

#[test]
fn maintenance_sweep_cleans_old_orphan_artifact_dirs() {
    let home = tempfile::tempdir().expect("temp home");
    let submit = cbth(
        &home,
        &[
            "job",
            "submit",
            "--source-thread-id",
            "thread-orphan",
            "--summary",
            "seed store",
        ],
    );
    let job_id = submit["job"]["job_id"].as_str().expect("job id");

    let orphan_dir = home.path().join("artifacts").join("orphan-artifact");
    fs::create_dir_all(&orphan_dir).expect("create orphan dir");
    fs::write(orphan_dir.join("payload"), "orphan").expect("write orphan payload");
    let stuck_path = home.path().join("artifacts").join("stuck-ingest");
    fs::write(&stuck_path, "not a directory").expect("write stuck file");
    let outside_victim = home
        .path()
        .parent()
        .expect("temp home parent")
        .join("cbth-outside-victim");
    fs::create_dir_all(&outside_victim).expect("create outside victim");

    let conn = Connection::open(home.path().join("cbth.sqlite3")).expect("open db");
    conn.execute(
        "INSERT INTO artifact_ingests (
            artifact_id, job_id, relative_path, created_at, updated_at
        ) VALUES ('orphan-artifact', ?, 'artifacts/orphan-artifact/payload', 1, 1)",
        params![job_id],
    )
    .expect("insert stale ingest");
    conn.execute(
        "INSERT INTO artifact_ingests (
            artifact_id, job_id, relative_path, created_at, updated_at
        ) VALUES ('stuck-ingest', ?, 'artifacts/stuck-ingest/payload', 1, 1)",
        params![job_id],
    )
    .expect("insert stuck ingest");
    conn.execute(
        "INSERT INTO artifact_ingests (
            artifact_id, job_id, relative_path, created_at, updated_at
        ) VALUES ('../../cbth-outside-victim', ?, 'artifacts/../../cbth-outside-victim/payload', 1, 1)",
        params![job_id],
    )
    .expect("insert malformed ingest");

    let future_now = (now_epoch_seconds() + 2 * 60 * 60).to_string();
    let sweep = cbth(&home, &["maintenance", "sweep", "--now", &future_now]);
    assert_eq!(sweep["sweep"]["orphan_artifacts_deleted"], 1);
    assert_eq!(sweep["sweep"]["orphan_artifact_delete_failures"], 2);
    assert!(!orphan_dir.exists());
    assert!(stuck_path.exists());
    assert!(outside_victim.exists());
    fs::remove_dir_all(outside_victim).expect("cleanup outside victim");
}

#[test]
fn active_ingest_marker_refresh_uses_wall_clock_not_synthetic_sweep_time() {
    let home = tempfile::tempdir().expect("temp home");
    let submit = cbth(
        &home,
        &[
            "job",
            "submit",
            "--source-thread-id",
            "thread-active-ingest",
            "--summary",
            "seed active ingest",
        ],
    );
    let job_id = submit["job"]["job_id"].as_str().expect("job id");
    let artifact_id = "active-ingest";
    let artifact_dir = home.path().join("artifacts").join(artifact_id);
    fs::create_dir_all(&artifact_dir).expect("create artifact dir");
    fs::write(artifact_dir.join("payload"), "partial").expect("write payload");
    fs::write(artifact_dir.join(".ingest-active"), "active\n").expect("write marker");

    let conn = Connection::open(home.path().join("cbth.sqlite3")).expect("open db");
    conn.execute(
        "INSERT INTO artifact_ingests (
            artifact_id, job_id, relative_path, created_at, updated_at
        ) VALUES (?, ?, 'artifacts/active-ingest/payload', 1, 1)",
        params![artifact_id, job_id],
    )
    .expect("insert active ingest");
    drop(conn);

    let future_now = (now_epoch_seconds() + 2 * 60 * 60).to_string();
    let first_sweep = cbth(&home, &["maintenance", "sweep", "--now", &future_now]);
    assert_eq!(first_sweep["sweep"]["orphan_artifacts_deleted"], 0);
    assert!(artifact_dir.exists());

    let conn = Connection::open(home.path().join("cbth.sqlite3")).expect("open db");
    let refreshed_at = conn
        .query_row(
            "SELECT updated_at FROM artifact_ingests WHERE artifact_id = ?",
            params![artifact_id],
            |row| row.get::<_, i64>(0),
        )
        .expect("read refreshed timestamp");
    assert!(refreshed_at < future_now.parse::<i64>().expect("future now"));
    conn.execute(
        "UPDATE artifact_ingests SET updated_at = 1 WHERE artifact_id = ?",
        params![artifact_id],
    )
    .expect("age ingest after active marker observation");
    drop(conn);

    fs::remove_file(artifact_dir.join(".ingest-active")).expect("remove marker");
    let second_sweep = cbth(&home, &["maintenance", "sweep", "--now", &future_now]);
    assert_eq!(second_sweep["sweep"]["orphan_artifacts_deleted"], 1);
    assert!(!artifact_dir.exists());
}

#[test]
fn future_sweep_does_not_drop_wall_clock_fresh_ingest_without_marker() {
    let home = tempfile::tempdir().expect("temp home");
    let submit = cbth(
        &home,
        &[
            "job",
            "submit",
            "--source-thread-id",
            "thread-fresh-ingest",
            "--summary",
            "seed fresh ingest",
        ],
    );
    let job_id = submit["job"]["job_id"].as_str().expect("job id");
    let artifact_id = "fresh-ingest";
    let now = now_epoch_seconds();
    let conn = Connection::open(home.path().join("cbth.sqlite3")).expect("open db");
    conn.execute(
        "INSERT INTO artifact_ingests (
            artifact_id, job_id, relative_path, created_at, updated_at
        ) VALUES (?, ?, 'artifacts/fresh-ingest/payload', ?, ?)",
        params![artifact_id, job_id, now, now],
    )
    .expect("insert fresh ingest");
    drop(conn);

    let future_now = (now + 2 * 60 * 60).to_string();
    let sweep = cbth(&home, &["maintenance", "sweep", "--now", &future_now]);
    assert_eq!(sweep["sweep"]["orphan_artifacts_deleted"], 0);

    let conn = Connection::open(home.path().join("cbth.sqlite3")).expect("open db");
    assert_eq!(
        conn.query_row(
            "SELECT count(*) FROM artifact_ingests WHERE artifact_id = ?",
            params![artifact_id],
            |row| row.get::<_, i64>(0),
        )
        .expect("ingest count"),
        1
    );
}

#[test]
fn concurrent_submits_share_fresh_home() {
    let home = tempfile::tempdir().expect("temp home");
    let worker_count = 12;
    let barrier = Arc::new(Barrier::new(worker_count));
    let mut handles = Vec::new();

    for index in 0..worker_count {
        let barrier = Arc::clone(&barrier);
        let home_path = home.path().to_path_buf();
        handles.push(thread::spawn(move || {
            barrier.wait();
            Command::new(env!("CARGO_BIN_EXE_cbth"))
                .env("CBTH_ALLOW_DIRECT_STORE", "1")
                .arg("--direct-store")
                .arg("--home")
                .arg(home_path)
                .args([
                    "job",
                    "submit",
                    "--source-thread-id",
                    "thread-concurrent",
                    "--summary",
                    &format!("concurrent job {index}"),
                ])
                .output()
                .expect("run cbth")
        }));
    }

    for handle in handles {
        let output = handle.join().expect("join worker");
        assert!(
            output.status.success(),
            "concurrent submit failed\nstdout: {}\nstderr: {}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }

    let jobs = cbth(
        &home,
        &[
            "job",
            "list",
            "--source-thread-id",
            "thread-concurrent",
            "--limit",
            "20",
        ],
    );
    assert_eq!(
        jobs["jobs"].as_array().expect("jobs array").len(),
        worker_count
    );
}

#[test]
fn large_result_requires_artifact_read() {
    let home = tempfile::tempdir().expect("temp home");
    let result_path = home.path().join("large-result.bin");
    fs::write(&result_path, vec![b'x'; 70 * 1024]).expect("write large result");

    let submit = cbth(
        &home,
        &[
            "job",
            "submit",
            "--source-thread-id",
            "thread-large",
            "--summary",
            "wait for large report",
        ],
    );
    let job_id = submit["job"]["job_id"].as_str().expect("job id");
    let result_arg = result_path.to_string_lossy().to_string();

    let completed = cbth(
        &home,
        &[
            "job",
            "complete",
            "--job-id",
            job_id,
            "--result-file",
            &result_arg,
        ],
    );

    let batch = &completed["batch"]["batch"];
    assert_eq!(batch["inline_payload_bytes"], 0);
    assert_eq!(batch["requires_artifact_read"], true);
}

#[test]
fn failed_result_ingest_keeps_cleanup_ownership_until_sweep() {
    let home = tempfile::tempdir().expect("temp home");
    let submit = cbth(
        &home,
        &[
            "job",
            "submit",
            "--source-thread-id",
            "thread-failed-ingest",
            "--summary",
            "wait for missing result",
        ],
    );
    let job_id = submit["job"]["job_id"].as_str().expect("job id");
    let missing_arg = home
        .path()
        .join("missing-result.txt")
        .to_string_lossy()
        .to_string();

    let stderr = cbth_failure(
        &home,
        &[
            "job",
            "complete",
            "--job-id",
            job_id,
            "--result-file",
            &missing_arg,
        ],
    );
    assert!(stderr.contains("stat result file"));

    let conn = Connection::open(home.path().join("cbth.sqlite3")).expect("open db");
    assert_eq!(
        conn.query_row("SELECT count(*) FROM artifact_ingests", [], |row| row
            .get::<_, i64>(0))
            .expect("ingest count"),
        1
    );
    conn.execute("UPDATE artifact_ingests SET updated_at = 1", [])
        .expect("age failed ingest");
    drop(conn);

    let future_now = (now_epoch_seconds() + 2 * 60 * 60).to_string();
    let sweep = cbth(&home, &["maintenance", "sweep", "--now", &future_now]);
    assert_eq!(sweep["sweep"]["orphan_artifacts_deleted"], 1);
    let conn = Connection::open(home.path().join("cbth.sqlite3")).expect("open db");
    assert_eq!(
        conn.query_row("SELECT count(*) FROM artifact_ingests", [], |row| row
            .get::<_, i64>(0))
            .expect("ingest count"),
        0
    );
}

#[test]
fn redelivery_window_overflow_is_rejected() {
    let home = tempfile::tempdir().expect("temp home");
    let result_path = home.path().join("result.txt");
    fs::write(&result_path, "ready\n").expect("write result");

    let complete_submit = cbth(
        &home,
        &[
            "job",
            "submit",
            "--source-thread-id",
            "thread-overflow",
            "--summary",
            "complete overflow",
        ],
    );
    let complete_job_id = complete_submit["job"]["job_id"].as_str().expect("job id");
    let result_arg = result_path.to_string_lossy().to_string();
    let complete_stderr = cbth_failure(
        &home,
        &[
            "job",
            "complete",
            "--job-id",
            complete_job_id,
            "--result-file",
            &result_arg,
            "--redelivery-window-seconds",
            "9223372036854775807",
        ],
    );
    assert!(complete_stderr.contains("overflows timestamp range"));
    let conn = Connection::open(home.path().join("cbth.sqlite3")).expect("open db");
    assert_eq!(
        conn.query_row("SELECT count(*) FROM artifact_ingests", [], |row| row
            .get::<_, i64>(0))
            .expect("ingest count"),
        0
    );
    assert_eq!(
        fs::read_dir(home.path().join("artifacts"))
            .expect("read artifacts dir")
            .count(),
        0
    );

    let fail_submit = cbth(
        &home,
        &[
            "job",
            "submit",
            "--source-thread-id",
            "thread-overflow",
            "--summary",
            "fail overflow",
        ],
    );
    let fail_job_id = fail_submit["job"]["job_id"].as_str().expect("job id");
    let fail_stderr = cbth_failure(
        &home,
        &[
            "job",
            "fail",
            "--job-id",
            fail_job_id,
            "--reason",
            "boom",
            "--redelivery-window-seconds",
            "9223372036854775807",
        ],
    );
    assert!(fail_stderr.contains("overflows timestamp range"));
}

#[test]
fn trusted_all_cli_attempt_bypasses_policy_and_session_risk_gates() {
    let home = tempfile::tempdir().expect("temp home");
    let submitted = cbth(
        &home,
        &[
            "job",
            "submit",
            "--source-thread-id",
            "thread-trusted-all",
            "--summary",
            "unsafe but trusted",
            "--delivery-read-only",
            "false",
            "--delivery-requires-approval",
            "true",
            "--delivery-requires-network",
            "true",
            "--delivery-requires-write-access",
            "true",
        ],
    );
    let job_id = submitted["job"]["job_id"].as_str().expect("job id");
    let failed = cbth(
        &home,
        &["job", "fail", "--job-id", job_id, "--reason", "ready"],
    );
    let batch_id = failed["batch"]["batch"]["batch_id"]
        .as_str()
        .expect("batch id");
    let managed_session_id =
        bind_cli_session_with_profile(&home, "thread-trusted-all", true, true, true);
    note_cli_session_minimum_capabilities(&home, &managed_session_id);
    note_cli_session_idle(&home, &managed_session_id);

    let strict_stderr = cbth_failure(
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
            "rpc-strict-trusted-all-test",
            "--authorization-mode",
            "strict-safe",
        ],
    );
    assert!(strict_stderr.contains("not eligible for detached CLI delivery"));

    let trusted = cbth(
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
            "rpc-trusted-all-test",
            "--authorization-mode",
            "trusted-all",
        ],
    );
    assert_eq!(trusted["attempt"]["authorization_mode"], "trusted_all");
    assert_eq!(
        trusted["attempt"]["delivery_rpc_state"],
        "pending_acceptance"
    );
}

#[test]
fn reject_cli_before_accept_leaves_batch_retryable_without_attempt_charge() {
    let home = tempfile::tempdir().expect("temp home");
    let submitted = cbth(
        &home,
        &[
            "job",
            "submit",
            "--source-thread-id",
            "thread-reject-before-accept",
            "--summary",
            "reject before accept",
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
        &["job", "fail", "--job-id", job_id, "--reason", "ready"],
    );
    let batch_id = failed["batch"]["batch"]["batch_id"]
        .as_str()
        .expect("batch id");
    let managed_session_id = bind_idle_cli_session(&home, "thread-reject-before-accept");
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
            "rpc-reject-before-accept",
            "--now",
            "100",
        ],
    );
    let attempt_id = pending["attempt"]["attempt_id"]
        .as_str()
        .expect("attempt id");

    let rejected = cbth(
        &home,
        &[
            "attempt",
            "reject-cli-before-accept",
            "--attempt-id",
            attempt_id,
            "--now",
            "101",
        ],
    );
    assert_eq!(rejected["attempt"]["state"], "abandoned");
    assert_eq!(
        rejected["attempt"]["delivery_rpc_state"],
        "rejected_before_accept"
    );

    let inspected = cbth(&home, &["batch", "inspect", "--batch-id", batch_id]);
    assert_eq!(inspected["batch"]["batch"]["state"], "open");
    assert_eq!(inspected["batch"]["batch"]["replay_policy"], "automatic");
    assert_eq!(inspected["batch"]["batch"]["delivery_attempt_count"], 0);
}

#[test]
fn audit_record_and_list_round_trip_details() {
    let home = tempfile::tempdir().expect("temp home");
    cbth(
        &home,
        &[
            "audit",
            "record",
            "--source-thread-id",
            "thread-audit",
            "--batch-id",
            "batch-audit",
            "--attempt-id",
            "attempt-audit",
            "--managed-session-id",
            "session-audit",
            "--session-epoch",
            "2",
            "--policy-kind",
            "trusted_all",
            "--decision",
            "accepted",
            "--reason",
            "test_round_trip",
            "--details-json",
            r#"{"delivery_turn_id":"turn-audit"}"#,
            "--now",
            "123",
        ],
    );
    let listed = cbth(
        &home,
        &[
            "audit",
            "list",
            "--source-thread-id",
            "thread-audit",
            "--limit",
            "10",
        ],
    );
    let first = &listed["audit"][0];
    assert_eq!(first["recorded_at"], 123);
    assert_eq!(first["policy_kind"], "trusted_all");
    assert_eq!(first["decision"], "accepted");
    assert_eq!(first["details"]["delivery_turn_id"], "turn-audit");
}

#[cfg(unix)]
fn mode(path: &std::path::Path) -> u32 {
    fs::metadata(path).expect("metadata").permissions().mode() & 0o777
}

fn now_epoch_seconds() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time after unix epoch")
        .as_secs()
        .try_into()
        .expect("epoch seconds fit i64")
}
