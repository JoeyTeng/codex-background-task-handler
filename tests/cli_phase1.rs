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

    let pending = cbth(
        &home,
        &[
            "attempt",
            "begin-cli-accept",
            "--batch-id",
            batch_id,
            "--managed-session-id",
            "managed-cli-1",
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
    let retried_pending = cbth(
        &home,
        &[
            "attempt",
            "begin-cli-accept",
            "--batch-id",
            batch_id,
            "--managed-session-id",
            "managed-cli-1",
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
    let pending = cbth(
        &home,
        &[
            "attempt",
            "begin-cli-accept",
            "--batch-id",
            batch_id,
            "--managed-session-id",
            "managed-cli-observation-bound",
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
    let pending = cbth(
        &home,
        &[
            "attempt",
            "begin-cli-accept",
            "--batch-id",
            batch_id,
            "--managed-session-id",
            "managed-cli-window",
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
    let pending = cbth(
        &home,
        &[
            "attempt",
            "begin-cli-accept",
            "--batch-id",
            batch_id,
            "--managed-session-id",
            "managed-cli-stale",
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
    let first_attempt = cbth(
        &home,
        &[
            "attempt",
            "begin-cli-accept",
            "--batch-id",
            first_batch_id,
            "--managed-session-id",
            "managed-cli-close-release",
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

    let second_attempt = cbth(
        &home,
        &[
            "attempt",
            "begin-cli-accept",
            "--batch-id",
            second_batch_id,
            "--managed-session-id",
            "managed-cli-close-release",
            "--session-epoch",
            "1",
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
    let pending = cbth(
        &home,
        &[
            "attempt",
            "begin-cli-accept",
            "--batch-id",
            batch_id,
            "--managed-session-id",
            "managed-cli-2",
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
