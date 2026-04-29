use std::fs;
use std::path::Path;
use std::process::Command;
use std::thread;
use std::time::{Duration, Instant};

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
    cbth_in_dir(home, args, None, false)
}

fn cbth_direct(home: &TempDir, args: &[&str]) -> Value {
    cbth_in_dir(home, args, None, true)
}

fn cbth_in_dir(home: &TempDir, args: &[&str], cwd: Option<&Path>, direct_store: bool) -> Value {
    let mut command = Command::new(env!("CARGO_BIN_EXE_cbth"));
    if direct_store {
        command.env("CBTH_ALLOW_DIRECT_STORE", "1");
        command.arg("--direct-store");
    }
    command.arg("--home").arg(home.path()).args(args);
    if let Some(cwd) = cwd {
        command.current_dir(cwd);
    }
    let output = command.output().expect("run cbth");

    assert!(
        output.status.success(),
        "cbth failed\nstatus: {}\nstdout: {}\nstderr: {}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    serde_json::from_slice(&output.stdout).expect("valid json output")
}

fn bind_idle_cli_session_direct(home: &TempDir, bound_thread_id: &str) -> String {
    let session = cbth_direct(
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
            "--now",
            "1000",
        ],
    );
    let managed_session_id = session["cli_session"]["session"]["managed_session_id"]
        .as_str()
        .expect("managed session id")
        .to_owned();
    cbth_direct(
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
            "--now",
            "1000",
        ],
    );
    managed_session_id
}

fn create_accepted_cli_attempt_direct(
    home: &TempDir,
    source_thread_id: &str,
    delivery_turn_id: &str,
    rpc_request_id: &str,
) -> (String, String) {
    let submitted = cbth_direct(
        home,
        &[
            "job",
            "submit",
            "--source-thread-id",
            source_thread_id,
            "--summary",
            "accepted CLI observation",
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
    let failed = cbth_direct(
        home,
        &["job", "fail", "--job-id", job_id, "--reason", "ready"],
    );
    let batch_id = failed["batch"]["batch"]["batch_id"]
        .as_str()
        .expect("batch id")
        .to_owned();
    let managed_session_id = bind_idle_cli_session_direct(home, source_thread_id);
    let pending = cbth_direct(
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
            rpc_request_id,
            "--now",
            "1000",
        ],
    );
    let attempt_id = pending["attempt"]["attempt_id"]
        .as_str()
        .expect("attempt id")
        .to_owned();
    cbth_direct(
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
    (batch_id, attempt_id)
}

#[cfg(unix)]
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

fn wait_for_socket_removed(home: &TempDir) {
    let socket_path = home.path().join("run").join("cbth.sock");
    let deadline = Instant::now() + Duration::from_secs(5);
    while socket_path.exists() {
        assert!(Instant::now() < deadline, "daemon socket was not removed");
        thread::sleep(Duration::from_millis(50));
    }
}

#[test]
fn mutating_commands_default_to_daemon_dispatch() {
    let home = temp_home();

    let submitted = cbth(
        &home,
        &[
            "job",
            "submit",
            "--source-thread-id",
            "thread-domain-rpc",
            "--summary",
            "wait for external reviewer",
        ],
    );
    let job_id = submitted["job"]["job_id"].as_str().expect("job id");

    let status = cbth(&home, &["daemon", "status"]);
    assert_eq!(status["daemon"]["stop_requested"], false);

    let session = cbth(
        &home,
        &[
            "cli",
            "session",
            "bind",
            "--bound-thread-id",
            "thread-domain-rpc",
            "--session-allows-approval",
            "false",
            "--session-allows-network",
            "false",
            "--session-allows-write-access",
            "false",
        ],
    );
    assert_eq!(session["cli_session"]["outcome"], "created");

    let failed = cbth(
        &home,
        &[
            "job",
            "fail",
            "--job-id",
            job_id,
            "--reason",
            "review rejected",
        ],
    );
    let batch_id = failed["batch"]["batch"]["batch_id"]
        .as_str()
        .expect("batch id");

    let closed = cbth(
        &home,
        &[
            "batch",
            "close-head",
            "--source-thread-id",
            "thread-domain-rpc",
            "--reason",
            "operator-confirmed-delivery",
            "--note",
            "delivered by domain RPC test",
        ],
    );
    assert_eq!(closed["batch"]["batch"]["state"], "closed");
    assert_eq!(
        closed["batch"]["batch"]["close_reason"],
        "operator_confirmed_delivery"
    );

    let sweep = cbth(&home, &["maintenance", "sweep"]);
    assert!(sweep["sweep"].is_object());

    let inspected = cbth(&home, &["batch", "inspect", "--batch-id", batch_id]);
    assert_eq!(inspected["batch"]["batch"]["state"], "closed");

    cbth(&home, &["daemon", "stop"]);
    wait_for_socket_removed(&home);
}

#[test]
fn routed_mutating_commands_accept_daemon_startup_timeout_override() {
    let home = temp_home();

    let submitted = cbth(
        &home,
        &[
            "--auto-daemon-startup-timeout-seconds",
            "6",
            "job",
            "submit",
            "--source-thread-id",
            "thread-timeout-override",
            "--summary",
            "wait with custom startup timeout",
        ],
    );
    assert_eq!(
        submitted["job"]["source_thread_id"],
        "thread-timeout-override"
    );

    cbth(&home, &["daemon", "stop"]);
    wait_for_socket_removed(&home);
}

#[test]
fn daemon_dispatch_resolves_file_paths_before_handoff() {
    let home = temp_home();
    let client_cwd = tempfile::tempdir().expect("client cwd");
    let result_path = client_cwd.path().join("result.txt");
    fs::write(&result_path, "ready from client cwd\n").expect("write result");

    let submitted = cbth(
        &home,
        &[
            "job",
            "submit",
            "--source-thread-id",
            "thread-relative-result",
            "--summary",
            "wait for relative file",
        ],
    );
    let job_id = submitted["job"]["job_id"].as_str().expect("job id");

    let completed = cbth_in_dir(
        &home,
        &[
            "job",
            "complete",
            "--job-id",
            job_id,
            "--result-file",
            "result.txt",
            "--summary",
            "relative result consumed",
        ],
        Some(client_cwd.path()),
        false,
    );
    let artifact = &completed["batch"]["jobs"][0]["artifact"];
    assert_eq!(artifact["size_bytes"], 22);
    assert_eq!(artifact["original_filename"], "result.txt");

    cbth(&home, &["daemon", "stop"]);
    wait_for_socket_removed(&home);
}

#[test]
fn daemon_dispatch_preserves_string_values_that_start_with_dash() {
    let home = temp_home();

    let submitted = cbth(
        &home,
        &[
            "job",
            "submit",
            "--source-thread-id=thread-leading-dash",
            "--summary=-wait for dash-prefixed reviewer",
        ],
    );
    assert_eq!(
        submitted["job"]["summary"],
        "-wait for dash-prefixed reviewer"
    );
    let job_id = submitted["job"]["job_id"].as_str().expect("job id");

    let failed = cbth(
        &home,
        &[
            "job",
            "fail",
            "--job-id",
            job_id,
            "--reason=-review rejected",
        ],
    );
    assert_eq!(
        failed["batch"]["batch"]["summary"],
        "Background job failed: -review rejected"
    );

    cbth(&home, &["daemon", "stop"]);
    wait_for_socket_removed(&home);
}

#[cfg(unix)]
#[test]
fn mutating_dispatch_fails_closed_when_run_dir_is_too_permissive() {
    let home = temp_home();
    let submitted = cbth(
        &home,
        &[
            "job",
            "submit",
            "--source-thread-id",
            "thread-permission-proof",
            "--summary",
            "seed daemon",
        ],
    );
    let job_id = submitted["job"]["job_id"].as_str().expect("job id");

    let run_dir = home.path().join("run");
    fs::set_permissions(&run_dir, fs::Permissions::from_mode(0o755)).expect("chmod run dir");
    let stderr = cbth_failure(
        &home,
        &[
            "job",
            "fail",
            "--job-id",
            job_id,
            "--reason",
            "should not dispatch through permissive socket dir",
        ],
    );
    assert!(stderr.contains("cbth run directory permissions are wider than 0700"));

    fs::set_permissions(&run_dir, fs::Permissions::from_mode(0o700)).expect("restore run dir");
    cbth(&home, &["daemon", "stop"]);
    wait_for_socket_removed(&home);
}

#[cfg(unix)]
#[test]
fn mutating_dispatch_fails_closed_before_autostart_with_permissive_run_dir() {
    let home = temp_home();
    let run_dir = home.path().join("run");
    fs::create_dir(&run_dir).expect("create run dir");
    fs::set_permissions(&run_dir, fs::Permissions::from_mode(0o755)).expect("chmod run dir");

    let stderr = cbth_failure(
        &home,
        &[
            "job",
            "submit",
            "--source-thread-id",
            "thread-preflight-proof",
            "--summary",
            "must not autostart through permissive run dir",
        ],
    );
    assert!(stderr.contains("cbth run directory permissions are wider than 0700"));
    assert!(!run_dir.join("cbth.sock").exists());
}

#[test]
fn maintenance_sweep_autostart_returns_requested_sweep_report() {
    let home = temp_home();
    let result_path = home.path().join("result.txt");
    fs::write(&result_path, "ready\n").expect("write result");

    let submitted = cbth_direct(
        &home,
        &[
            "job",
            "submit",
            "--source-thread-id",
            "thread-sweep-report",
            "--summary",
            "wait for timeout",
        ],
    );
    let job_id = submitted["job"]["job_id"].as_str().expect("job id");
    let result_arg = result_path.to_string_lossy().to_string();
    let completed = cbth_direct(
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
    let sweep_now = (completed["batch"]["batch"]["redelivery_window_ends_at"]
        .as_i64()
        .expect("redelivery window")
        + 1)
    .to_string();

    let sweep = cbth(&home, &["maintenance", "sweep", "--now", &sweep_now]);
    assert_eq!(sweep["sweep"]["expired_automatic_batches_closed"], 1);

    let status = cbth(&home, &["daemon", "status"]);
    assert_eq!(
        status["startup_sweep"]["expired_automatic_batches_closed"],
        0
    );

    cbth(&home, &["daemon", "stop"]);
    wait_for_socket_removed(&home);
}

#[test]
fn daemon_routed_late_cli_turn_observation_survives_startup_sweep() {
    let home = temp_home();
    let (batch_id, attempt_id) = create_accepted_cli_attempt_direct(
        &home,
        "thread-late-observation-startup-sweep",
        "turn-late-startup-sweep",
        "rpc-request-late-observation-startup-sweep",
    );
    cbth_direct(
        &home,
        &[
            "attempt",
            "observe-cli-turn",
            "--attempt-id",
            &attempt_id,
            "--delivery-turn-id",
            "turn-late-startup-sweep",
            "--turn-event",
            "turn-started",
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
            "turn-late-startup-sweep",
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
    assert_eq!(observed["attempt"]["last_observed_turn_event_at"], 1062);

    let batch = cbth(&home, &["batch", "inspect", "--batch-id", &batch_id]);
    assert_eq!(batch["batch"]["batch"]["state"], "open");
    assert_eq!(
        batch["batch"]["batch"]["replay_policy"],
        "manual_resolution_only"
    );

    cbth(&home, &["daemon", "stop"]);
    wait_for_socket_removed(&home);
}

#[test]
fn daemon_routed_on_time_cli_turn_completion_survives_startup_sweep() {
    let home = temp_home();
    let (batch_id, attempt_id) = create_accepted_cli_attempt_direct(
        &home,
        "thread-on-time-observation-startup-sweep",
        "turn-on-time-startup-sweep",
        "rpc-request-on-time-observation-startup-sweep",
    );
    cbth_direct(
        &home,
        &[
            "attempt",
            "observe-cli-turn",
            "--attempt-id",
            &attempt_id,
            "--delivery-turn-id",
            "turn-on-time-startup-sweep",
            "--turn-event",
            "turn-started",
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
            "turn-on-time-startup-sweep",
            "--turn-event",
            "turn-completed",
            "--now",
            "1060",
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
    assert_eq!(observed["attempt"]["last_observed_turn_event_at"], 1060);
    assert!(observed["attempt"]["abandoned_at"].is_null());

    let batch = cbth(&home, &["batch", "inspect", "--batch-id", &batch_id]);
    assert_eq!(batch["batch"]["batch"]["state"], "closed");
    assert_eq!(batch["batch"]["batch"]["close_reason"], "delivered");

    cbth(&home, &["daemon", "stop"]);
    wait_for_socket_removed(&home);
}

#[test]
fn daemon_routed_bind_after_startup_sweep_blocks_delayed_completion() {
    let home = temp_home();
    let (batch_id, attempt_id) = create_accepted_cli_attempt_direct(
        &home,
        "thread-bind-after-startup-sweep",
        "turn-bind-after-startup-sweep",
        "rpc-request-bind-after-startup-sweep",
    );

    let attached = cbth(
        &home,
        &[
            "cli",
            "session",
            "bind",
            "--bound-thread-id",
            "thread-bind-after-startup-sweep",
            "--session-allows-approval",
            "false",
            "--session-allows-network",
            "false",
            "--session-allows-write-access",
            "false",
            "--now",
            "1062",
        ],
    );
    assert_eq!(attached["cli_session"]["outcome"], "attached");

    let expired = cbth(&home, &["attempt", "inspect", "--attempt-id", &attempt_id]);
    assert_eq!(expired["attempt"]["state"], "abandoned");
    assert_eq!(
        expired["attempt"]["delivery_observation_state"],
        "abandoned"
    );

    let observed = cbth(
        &home,
        &[
            "attempt",
            "observe-cli-turn",
            "--attempt-id",
            &attempt_id,
            "--delivery-turn-id",
            "turn-bind-after-startup-sweep",
            "--turn-event",
            "turn-completed",
            "--now",
            "1060",
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
    assert!(batch["batch"]["batch"]["close_reason"].is_null());

    cbth(&home, &["daemon", "stop"]);
    wait_for_socket_removed(&home);
}
