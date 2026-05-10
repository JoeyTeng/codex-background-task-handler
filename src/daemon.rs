use std::collections::{HashMap, VecDeque};
use std::ffi::{OsStr, OsString};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::mem;
use std::net::IpAddr;
use std::os::fd::{AsRawFd, FromRawFd, RawFd};
use std::os::unix::ffi::{OsStrExt, OsStringExt};
use std::os::unix::fs::{FileTypeExt, MetadataExt, PermissionsExt};
use std::os::unix::net::{UnixListener, UnixStream};
use std::os::unix::process::{CommandExt, ExitStatusExt};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail};
use rusqlite::ErrorCode;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};

use crate::artifact::{ingest_result_file, remove_ingest_marker_best_effort};
use crate::cli_app_server_client::AppServerJsonRpcClient;
use crate::fs_layout::{FsLayout, create_private_file, ensure_private_dir, sync_dir};
use crate::models::{
    DaemonLifecycleStatus, DeliveryPolicy, NewJob, NewTask, SweepReport, TaskRecord,
};
use crate::store::{Store, TaskFinishUpdate, new_id};

pub(crate) const MAX_REQUEST_BYTES: usize = 2 * 1024 * 1024;
const MAX_RESPONSE_BYTES: usize = 16 * 1024 * 1024;
const CLIENT_READ_TIMEOUT: Duration = Duration::from_secs(5);
const DOMAIN_REQUEST_TIMEOUT: Duration = Duration::from_secs(60 * 60);
const IDLE_POLL_INTERVAL: Duration = Duration::from_millis(250);
const READ_POLL_INTERVAL: Duration = Duration::from_millis(10);
const SOCKET_LIVENESS_TIMEOUT: Duration = Duration::from_millis(250);
const STARTUP_POLL_INTERVAL: Duration = Duration::from_millis(50);
const DAEMON_LIFECYCLE_POLL_INTERVAL: Duration = Duration::from_secs(1);
const DAEMON_MAINTENANCE_RETRY_INTERVAL: Duration = Duration::from_secs(5);
const CLI_APP_SERVER_STARTUP_TIMEOUT: Duration = Duration::from_secs(10);
const CLI_THREAD_START_BOOTSTRAP_REQUEST_TIMEOUT: Duration = Duration::from_secs(5);
const CLI_APP_SERVER_TERM_GRACE: Duration = Duration::from_secs(2);
const CLI_APP_SERVER_KILL_GRACE: Duration = Duration::from_secs(2);
const CLI_APP_SERVER_DRAIN_POLL_INTERVAL: Duration = Duration::from_millis(100);
const CLI_APP_SERVER_LISTENER_SCAN_BYTES: usize = 8 * 1024;
const CLI_APP_SERVER_DRAIN_CHUNK_BYTES: usize = 4 * 1024;
const TASK_STREAM_TAIL_BYTES: usize = 64 * 1024;
const TASK_PROMPT_COMMAND_PREVIEW_BYTES: usize = 4 * 1024;
const TASK_STREAM_SPOOL_MAX_BYTES: u64 = 64 * 1024 * 1024;
const TASK_WAIT_POLL_INTERVAL: Duration = Duration::from_millis(100);
const TASK_TERM_GRACE: Duration = Duration::from_secs(5);
const TASK_LEADER_EXITED_PROCESS_GROUP_GRACE: Duration = Duration::from_secs(5);
const TASK_SHUTDOWN_DURABLE_CANCEL_GRACE: Duration = Duration::from_secs(1);
const TASK_SHUTDOWN_UNPERSISTED_CANCEL_GRACE: Duration = Duration::from_secs(3);
const TASK_SHUTDOWN_WORKER_DRAIN_GRACE: Duration = Duration::from_secs(1);
const TASK_SHUTDOWN_COMPLETION_DRAIN_GRACE: Duration =
    Duration::from_secs(TASK_STORE_COMPLETION_RETRY_TIMEOUT.as_secs() * 2 + 5);
const TASK_STREAM_DRAIN_HARD_GRACE: Duration = Duration::from_secs(1);
const DAEMON_STOP_WORKER_DRAIN_GRACE: Duration = Duration::from_secs(2);
const TASK_SPAWN_EXEC_GATE_FD: RawFd = 3;
const TASK_SPAWN_EXEC_GATE_SCRIPT: &str =
    "if IFS= read -r _ <&3; then exec 3<&-; exec \"$@\"; else exit 127; fi";
const TASK_STORE_COMPLETION_RETRY_TIMEOUT: Duration = Duration::from_secs(60);
const TASK_STORE_SETUP_RETRY_TIMEOUT: Duration = Duration::from_secs(5);
const TASK_STORE_COMPLETION_RETRY_INITIAL_DELAY: Duration = Duration::from_millis(50);
const TASK_STORE_COMPLETION_RETRY_MAX_DELAY: Duration = Duration::from_millis(500);
const MAX_SUPERVISED_TASKS: usize = 16;
const MAX_TASK_ENV_VARS: usize = 4096;
const MAX_TASK_ENV_BYTES: usize = 1024 * 1024;
const DEFAULT_CLI_APP_SERVER_LEASE_TTL_SECONDS: u64 = 60;
const DAEMON_PROTOCOL_VERSION: u64 = 1;
const DAEMON_CAPABILITIES: &[&str] = &[
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
];
const CLI_THREAD_START_BOOTSTRAP_BOUND_THREAD_ID: &str = "__cbth_thread_start_bootstrap__";
const CLI_FOREGROUND_THREAD_BOOTSTRAP_BOUND_THREAD_ID: &str =
    "__cbth_foreground_thread_bootstrap__";
const MAX_DISPATCH_WORKERS: usize = 32;
const RESERVED_CONTROL_WORKERS: usize = 8;
const MAX_CLIENT_WORKERS: usize = MAX_DISPATCH_WORKERS + RESERVED_CONTROL_WORKERS;
const DAEMON_BUSY_ERROR: &str = "daemon is busy";
const DAEMON_CONNECTION_LIMIT_ERROR: &str = "daemon connection limit reached";
static PROCESS_SPAWN_LOCK: Mutex<()> = Mutex::new(());

fn acquire_process_spawn_lock() -> Result<std::sync::MutexGuard<'static, ()>> {
    PROCESS_SPAWN_LOCK
        .lock()
        .map_err(|_| anyhow::anyhow!("process spawn lock poisoned"))
}

fn spawn_command_locked(command: &mut Command) -> Result<Child> {
    let _spawn_lock = acquire_process_spawn_lock()?;
    command.spawn().context("spawn process")
}

#[derive(Clone, Copy, Debug)]
pub struct DaemonServeOptions {
    pub idle_timeout_seconds: u64,
    pub startup_sweep_now: Option<i64>,
}

#[derive(Clone, Copy, Debug)]
pub struct DaemonEnsureOptions {
    pub idle_timeout_seconds: u64,
    pub startup_timeout_seconds: u64,
    pub startup_sweep_now: Option<i64>,
}

struct SocketCleanup<'a> {
    path: &'a Path,
}

impl Drop for SocketCleanup<'_> {
    fn drop(&mut self) {
        let _ = fs::remove_file(self.path);
        if let Some(parent) = self.path.parent() {
            let _ = sync_dir(parent);
        }
    }
}

struct StartupLock {
    file: File,
}

impl Drop for StartupLock {
    fn drop(&mut self) {
        let _ = unsafe { libc::flock(self.file.as_raw_fd(), libc::LOCK_UN) };
    }
}

struct FdGuard {
    fd: RawFd,
}

impl FdGuard {
    fn new(fd: RawFd) -> Self {
        Self { fd }
    }

    fn into_unix_stream(self) -> UnixStream {
        let fd = self.fd;
        mem::forget(self);
        unsafe { UnixStream::from_raw_fd(fd) }
    }
}

impl Drop for FdGuard {
    fn drop(&mut self) {
        let _ = unsafe { libc::close(self.fd) };
    }
}

struct TaskSpawnExecGate {
    read_fd: FdGuard,
    write_fd: FdGuard,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
struct DaemonRequest {
    command: String,
    #[serde(default)]
    payload: Value,
}

#[derive(Debug, Deserialize)]
struct DispatchPayload {
    argv: Vec<Vec<u8>>,
}

#[derive(Debug, Deserialize)]
struct TaskRunPayload {
    source_thread_id: String,
    summary: String,
    metadata_json: String,
    policy: DeliveryPolicy,
    cwd: Vec<u8>,
    cwd_display: String,
    timeout_seconds: Option<i64>,
    max_delivery_attempts: i64,
    redelivery_window_seconds: i64,
    command: Vec<Vec<u8>>,
    #[serde(default)]
    environment: Vec<(Vec<u8>, Vec<u8>)>,
}

#[derive(Debug, Deserialize)]
struct TaskCancelPayload {
    task_id: String,
}

#[derive(Debug, Deserialize)]
struct CliAppServerEnsurePayload {
    managed_session_id: String,
    bound_thread_id: String,
    session_epoch: i64,
    codex_binary: Vec<u8>,
    lease_id: String,
    #[serde(default)]
    lease_ttl_seconds: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct CliAppServerProbePayload {
    codex_binary: Vec<u8>,
}

#[derive(Debug, Deserialize)]
struct CliAppServerLeasePayload {
    managed_session_id: String,
    lease_id: String,
    #[serde(default)]
    lease_ttl_seconds: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct CliAppServerReservePayload {
    bound_thread_id: String,
    lease_id: String,
    #[serde(default)]
    lease_ttl_seconds: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct CliAppServerReleasePayload {
    bound_thread_id: String,
    lease_id: String,
}

#[derive(Debug, Deserialize)]
struct CliAppServerStopPayload {
    managed_session_id: String,
    lease_id: String,
}

#[derive(Debug, Deserialize)]
struct CliThreadStartPayload {
    codex_binary: Vec<u8>,
    cwd: String,
    lease_id: String,
    lease_ttl_seconds: Option<u64>,
    #[serde(default)]
    thread_start_params: Option<Value>,
}

#[derive(Debug, Deserialize)]
struct CliForegroundThreadStartPayload {
    codex_binary: Vec<u8>,
    cwd: String,
    lease_id: String,
    lease_ttl_seconds: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct CliThreadStartPromotePayload {
    bootstrap_id: String,
    managed_session_id: String,
    bound_thread_id: String,
    session_epoch: i64,
    lease_id: String,
    lease_ttl_seconds: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct CliThreadStartAbortPayload {
    bootstrap_id: String,
    lease_id: String,
}

#[derive(Debug, Serialize)]
struct DaemonInfo {
    pid: u32,
    started_at: i64,
    uptime_seconds: u64,
    socket_path: String,
    idle_timeout_seconds: u64,
    stop_requested: bool,
}

#[derive(Clone, Debug, Serialize)]
struct CliAppServerInfo {
    managed_session_id: String,
    bound_thread_id: String,
    url: String,
    pid: u32,
    started_at: i64,
    lease_seconds_remaining: u64,
}

#[derive(Clone, Debug, Serialize)]
struct CliAppServerReservationInfo {
    bound_thread_id: String,
    lease_seconds_remaining: u64,
}

#[derive(Debug, Serialize)]
struct CliThreadStartInfo {
    bootstrap_id: String,
    thread_id: String,
}

#[derive(Debug, Serialize)]
struct CliForegroundThreadStartInfo {
    bootstrap_id: String,
    url: String,
}

struct DaemonState {
    layout: FsLayout,
    started_instant: Instant,
    started_at: i64,
    idle_timeout: Duration,
    startup_sweep: SweepReport,
    stop_requested: AtomicBool,
    lifecycle_maintenance_suppressed: AtomicBool,
    activity_generation: AtomicU64,
    active_clients: AtomicUsize,
    active_dispatches: AtomicUsize,
    cli_app_servers: Mutex<HashMap<String, ManagedCliAppServer>>,
    cli_app_server_reservations: Mutex<HashMap<String, CliAppServerReservation>>,
    cli_thread_start_bootstraps: Mutex<HashMap<String, ManagedCliAppServer>>,
    supervised_tasks: Arc<Mutex<HashMap<String, Arc<SupervisedTaskControl>>>>,
}

#[derive(Default)]
struct SupervisedTaskControl {
    process: Mutex<SupervisedTaskProcessState>,
    cancel_intents: AtomicUsize,
    exec_release_blocked: AtomicBool,
    cancel_requested: AtomicBool,
    cancel_signal_sent: AtomicBool,
    child_exit_observed: AtomicBool,
    spawn_gate: Mutex<()>,
    exec_release_decision: Mutex<()>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct SupervisedTaskProcess {
    pid: u32,
    pid_identity: String,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
enum SupervisedTaskProcessState {
    #[default]
    Pending,
    Running(SupervisedTaskProcess),
    Completing,
}

struct ManagedCliAppServer {
    managed_session_id: String,
    bound_thread_id: String,
    session_epoch: i64,
    url: String,
    child: Child,
    started_at: i64,
    lease_id: String,
    lease_expires_at: Instant,
    drain_running: Arc<AtomicBool>,
    stdout_worker: Option<thread::JoinHandle<()>>,
    stderr_worker: Option<thread::JoinHandle<()>>,
}

#[derive(Clone)]
struct ManagedCliAppServerProof {
    managed_session_id: String,
    bound_thread_id: String,
    session_epoch: i64,
    lease_id: String,
    child_pid: u32,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ChildStatusWithoutReaping {
    Running,
    Exited,
    NotWaitable,
}

struct CliAppServerReservation {
    bound_thread_id: String,
    lease_id: String,
    lease_expires_at: Instant,
}

struct ActiveClientGuard<'a> {
    active_clients: &'a AtomicUsize,
    activity_generation: &'a AtomicU64,
}

impl Drop for ActiveClientGuard<'_> {
    fn drop(&mut self) {
        self.activity_generation.fetch_add(1, Ordering::AcqRel);
        self.active_clients.fetch_sub(1, Ordering::AcqRel);
    }
}

struct DispatchGuard<'a> {
    active_dispatches: &'a AtomicUsize,
}

impl Drop for DispatchGuard<'_> {
    fn drop(&mut self) {
        self.active_dispatches.fetch_sub(1, Ordering::AcqRel);
    }
}

#[derive(Default)]
struct DaemonLifecycleCache {
    refreshed_at: Option<Instant>,
    refresh_failed: bool,
    status: DaemonLifecycleStatus,
}

impl DaemonLifecycleCache {
    fn has_exit_blockers(&self, maintenance_suppressed: bool) -> bool {
        self.refresh_failed
            || self.status.active_jobs > 0
            || self.status.nonterminal_tasks > 0
            || self.status.active_cli_acceptances > 0
            || self.status.active_cli_observations > 0
            || (!maintenance_suppressed && self.status.cli_acceptances_stale_now > 0)
            || (!maintenance_suppressed && self.status.cli_observations_due_now > 0)
            || (!maintenance_suppressed && self.status.open_batches_due_within_idle > 0)
    }
}

pub fn daemon_serve(layout: &FsLayout, options: DaemonServeOptions) -> Result<Value> {
    layout.ensure_run_dir()?;
    let socket_path = layout.daemon_socket_path();
    prepare_socket_path(&socket_path)?;

    let listener = UnixListener::bind(&socket_path)
        .with_context(|| format!("bind daemon socket {}", socket_path.display()))?;
    let _cleanup = SocketCleanup { path: &socket_path };
    set_socket_permissions(&socket_path)?;
    listener
        .set_nonblocking(true)
        .with_context(|| format!("set nonblocking {}", socket_path.display()))?;
    recover_lost_task_process_groups(layout)?;
    let recovery_now = match options.startup_sweep_now {
        Some(now) => now,
        None => current_epoch_seconds()?,
    };
    let mut store = Store::open(layout)?;
    let _ = store.fail_lost_tasks(recovery_now)?;
    let startup_sweep = if let Some(now) = options.startup_sweep_now {
        store.sweep(layout, now)?
    } else {
        SweepReport::default()
    };
    drop(store);

    let started_at = current_epoch_seconds()?;
    let state = Arc::new(DaemonState {
        layout: layout.clone(),
        started_instant: Instant::now(),
        started_at,
        idle_timeout: Duration::from_secs(options.idle_timeout_seconds),
        startup_sweep,
        stop_requested: AtomicBool::new(false),
        lifecycle_maintenance_suppressed: AtomicBool::new(options.startup_sweep_now.is_none()),
        activity_generation: AtomicU64::new(0),
        active_clients: AtomicUsize::new(0),
        active_dispatches: AtomicUsize::new(0),
        cli_app_servers: Mutex::new(HashMap::new()),
        cli_app_server_reservations: Mutex::new(HashMap::new()),
        cli_thread_start_bootstraps: Mutex::new(HashMap::new()),
        supervised_tasks: Arc::new(Mutex::new(HashMap::new())),
    });
    let mut last_activity = Instant::now();
    let mut last_activity_epoch = started_at;
    let mut observed_activity_generation = 0;
    let mut workers = Vec::new();
    let mut lifecycle_cache = DaemonLifecycleCache::default();
    let mut lifecycle_maintenance_worker = None;
    let mut next_lifecycle_maintenance_at = Instant::now();
    let shutdown_reason;

    loop {
        reap_finished_workers(&mut workers);
        reap_finished_lifecycle_maintenance(&mut lifecycle_maintenance_worker);
        let activity_generation = state.activity_generation.load(Ordering::Acquire);
        if activity_generation != observed_activity_generation {
            observed_activity_generation = activity_generation;
            last_activity = Instant::now();
            last_activity_epoch = current_epoch_seconds()?;
        }
        if state.stop_requested.load(Ordering::Acquire) {
            shutdown_reason = "stop_requested";
            break;
        }
        match listener.accept() {
            Ok((mut stream, _addr)) => {
                last_activity = Instant::now();
                last_activity_epoch = current_epoch_seconds()?;
                observed_activity_generation =
                    state.activity_generation.fetch_add(1, Ordering::AcqRel) + 1;
                if client_worker_capacity_reached(workers.len()) {
                    let _ = write_error_response(&mut stream, DAEMON_CONNECTION_LIMIT_ERROR);
                    continue;
                }
                state.active_clients.fetch_add(1, Ordering::AcqRel);
                let worker_state = Arc::clone(&state);
                workers.push(thread::spawn(move || {
                    let _active_client = ActiveClientGuard {
                        active_clients: &worker_state.active_clients,
                        activity_generation: &worker_state.activity_generation,
                    };
                    if let Err(error) = handle_client(&mut stream, &worker_state) {
                        let _ = write_error_response(&mut stream, &error.to_string());
                    }
                }));
            }
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                reap_expired_cli_app_servers(&state);
                reap_expired_cli_app_server_reservations(&state);
                reap_expired_cli_thread_start_bootstraps(&state);
                if state.active_clients.load(Ordering::Acquire) == 0 {
                    let activity_generation = state.activity_generation.load(Ordering::Acquire);
                    if activity_generation != observed_activity_generation {
                        observed_activity_generation = activity_generation;
                        last_activity = Instant::now();
                        last_activity_epoch = current_epoch_seconds()?;
                    }
                    let idle_elapsed = last_activity.elapsed() >= state.idle_timeout;
                    let idle_deadline = last_activity
                        .checked_add(state.idle_timeout)
                        .unwrap_or(last_activity);
                    let force_refresh = idle_elapsed
                        && lifecycle_cache
                            .refreshed_at
                            .is_none_or(|at| at < idle_deadline);
                    let idle_horizon_at = checked_epoch_add(
                        last_activity_epoch,
                        state.idle_timeout.as_secs(),
                        "idle_timeout",
                    )?;
                    refresh_lifecycle_cache_if_due(
                        &state,
                        &mut lifecycle_cache,
                        force_refresh,
                        idle_horizon_at,
                    );
                    maybe_spawn_lifecycle_maintenance(
                        &state,
                        &lifecycle_cache,
                        &mut lifecycle_maintenance_worker,
                        &mut next_lifecycle_maintenance_at,
                    );
                    if idle_elapsed
                        && lifecycle_maintenance_worker.is_none()
                        && !lifecycle_cache.has_exit_blockers(
                            state
                                .lifecycle_maintenance_suppressed
                                .load(Ordering::Acquire),
                        )
                        && !has_active_cli_app_servers(&state)
                        && !has_active_cli_app_server_reservations(&state)
                        && !has_active_cli_thread_start_bootstraps(&state)
                        && !has_active_supervised_tasks(&state)
                    {
                        shutdown_reason = "idle_timeout";
                        break;
                    }
                }
                thread::sleep(IDLE_POLL_INTERVAL);
            }
            Err(error) => return Err(error).context("accept daemon connection"),
        }
    }
    if shutdown_reason == "stop_requested" {
        stop_all_supervised_tasks(&state);
        stop_all_cli_app_servers(&state);
        stop_all_cli_thread_start_bootstraps(&state);
        clear_cli_app_server_reservations(&state);
        drain_workers_until(
            &mut workers,
            Instant::now() + DAEMON_STOP_WORKER_DRAIN_GRACE,
        );
        stop_all_cli_app_servers(&state);
        stop_all_cli_thread_start_bootstraps(&state);
        clear_cli_app_server_reservations(&state);
        reap_finished_workers(&mut workers);
        drain_lifecycle_maintenance_until(
            &mut lifecycle_maintenance_worker,
            Instant::now() + DAEMON_STOP_WORKER_DRAIN_GRACE,
        );
    } else {
        join_workers(workers);
        join_lifecycle_maintenance(lifecycle_maintenance_worker);
        stop_all_supervised_tasks(&state);
        stop_all_cli_app_servers(&state);
        stop_all_cli_thread_start_bootstraps(&state);
        clear_cli_app_server_reservations(&state);
    }

    Ok(json!({
        "daemon": daemon_info(&state),
        "shutdown_reason": shutdown_reason,
        "startup_sweep": &state.startup_sweep,
    }))
}

pub fn daemon_ensure(layout: &FsLayout, options: DaemonEnsureOptions) -> Result<Value> {
    validate_daemon_autostart_endpoint(layout)?;
    let startup_deadline = Instant::now() + Duration::from_secs(options.startup_timeout_seconds);
    if let Some(response) = probe_existing_daemon_for_ensure(layout, startup_deadline)? {
        return Ok(json!({
            "started": false,
            "daemon": response["daemon"].clone(),
        }));
    }

    layout.ensure_run_dir()?;
    let _startup_lock = acquire_startup_lock(layout, remaining_budget(startup_deadline)?)?;
    if let Some(response) = probe_existing_daemon_for_ensure(layout, startup_deadline)? {
        return Ok(json!({
            "started": false,
            "daemon": response["daemon"].clone(),
        }));
    }

    loop {
        let mut command =
            Command::new(std::env::current_exe().context("locate current executable")?);
        command
            .arg("--home")
            .arg(layout.home_dir())
            .arg("daemon")
            .arg("serve")
            .arg("--idle-timeout-seconds")
            .arg(options.idle_timeout_seconds.to_string());
        if let Some(startup_sweep_now) = options.startup_sweep_now {
            command.arg("--now").arg(startup_sweep_now.to_string());
        } else {
            command.arg("--skip-startup-sweep");
        }
        command
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .process_group(0);
        let mut child = spawn_command_locked(&mut command).context("spawn cbth daemon")?;
        let child_pid = child.id();

        loop {
            let probe_budget = match remaining_budget(startup_deadline) {
                Ok(duration) => duration,
                Err(error) => {
                    let _ = child.kill();
                    let _ = child.wait();
                    cleanup_stale_socket_best_effort(layout);
                    bail!("daemon did not become ready: {error}");
                }
            };
            match daemon_request_with_timeout(layout, "ping", probe_budget) {
                Ok(response) if daemon_response_is_compatible(&response) => {
                    return Ok(json!({
                        "started": true,
                        "spawned_pid": child_pid,
                        "daemon": response["daemon"].clone(),
                    }));
                }
                Ok(_) => {
                    let _ = child.kill();
                    let _ = child.wait();
                    if let Some(response) = stop_incompatible_daemon(layout, startup_deadline)? {
                        return Ok(json!({
                            "started": false,
                            "daemon": response["daemon"].clone(),
                        }));
                    }
                    break;
                }
                Err(last_error) => {
                    if let Some(status) = child.try_wait().context("check daemon child status")? {
                        if let Some(response) =
                            probe_existing_daemon_for_ensure(layout, startup_deadline)?
                        {
                            return Ok(json!({
                                "started": false,
                                "daemon": response["daemon"].clone(),
                            }));
                        }
                        cleanup_stale_socket_best_effort(layout);
                        bail!("daemon exited before accepting connections: {status}");
                    }
                    if Instant::now() >= startup_deadline {
                        let _ = child.kill();
                        let _ = child.wait();
                        cleanup_stale_socket_best_effort(layout);
                        bail!("daemon did not become ready: {last_error}");
                    }
                    thread::sleep(STARTUP_POLL_INTERVAL);
                }
            }
        }
    }
}

fn acquire_startup_lock(layout: &FsLayout, timeout: Duration) -> Result<StartupLock> {
    let lock_path = layout.daemon_startup_lock_path();
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(&lock_path)
        .with_context(|| format!("open startup lock {}", lock_path.display()))?;
    fs::set_permissions(&lock_path, fs::Permissions::from_mode(0o600))
        .with_context(|| format!("chmod 0600 {}", lock_path.display()))?;
    if let Some(parent) = lock_path.parent() {
        sync_dir(parent)?;
    }

    let started = Instant::now();
    loop {
        let rc = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
        if rc == 0 {
            file.set_len(0)
                .with_context(|| format!("truncate {}", lock_path.display()))?;
            file.write_all(format!("pid={}\n", std::process::id()).as_bytes())
                .with_context(|| format!("write {}", lock_path.display()))?;
            file.sync_all()
                .with_context(|| format!("sync {}", lock_path.display()))?;
            return Ok(StartupLock { file });
        }

        let error = io::Error::last_os_error();
        let raw = error.raw_os_error();
        if raw != Some(libc::EWOULDBLOCK) && raw != Some(libc::EAGAIN) {
            return Err(error).context("lock daemon startup");
        }
        if started.elapsed() >= timeout {
            bail!("daemon startup is already in progress");
        }
        thread::sleep(STARTUP_POLL_INTERVAL);
    }
}

pub fn daemon_request(layout: &FsLayout, command: &str) -> Result<Value> {
    daemon_request_payload_with_timeout(layout, command, Value::Null, CLIENT_READ_TIMEOUT)
}

pub fn daemon_request_payload(layout: &FsLayout, command: &str, payload: Value) -> Result<Value> {
    daemon_request_payload_with_timeout(layout, command, payload, DOMAIN_REQUEST_TIMEOUT)
}

pub fn daemon_request_payload_timeout(
    layout: &FsLayout,
    command: &str,
    payload: Value,
    timeout: Duration,
) -> Result<Value> {
    daemon_request_payload_with_timeout(layout, command, payload, timeout)
}

fn daemon_request_with_timeout(
    layout: &FsLayout,
    command: &str,
    timeout: Duration,
) -> Result<Value> {
    daemon_request_payload_with_timeout(layout, command, Value::Null, timeout)
}

fn daemon_request_payload_with_timeout(
    layout: &FsLayout,
    command: &str,
    payload: Value,
    timeout: Duration,
) -> Result<Value> {
    if timeout.is_zero() {
        bail!("daemon request timeout is exhausted");
    }
    daemon_request_until(layout, command, payload, Instant::now() + timeout)
}

fn daemon_request_until(
    layout: &FsLayout,
    command: &str,
    payload: Value,
    deadline: Instant,
) -> Result<Value> {
    let request = daemon_request_bytes(command, payload)?;
    validate_socket_endpoint(layout)?;
    let socket_path = layout.daemon_socket_path();
    let mut stream = connect_unix_stream_until(&socket_path, deadline)
        .with_context(|| format!("connect daemon socket {}", socket_path.display()))?;
    stream
        .set_nonblocking(true)
        .context("set daemon client nonblocking")?;

    write_all_until(&mut stream, &request, deadline).context("write daemon request")?;
    write_all_until(&mut stream, b"\n", deadline).context("write daemon request")?;
    stream.shutdown(std::net::Shutdown::Write).ok();

    let response = read_limited_until(&mut stream, MAX_RESPONSE_BYTES, deadline)
        .context("read daemon response")?;
    let parsed: Value = serde_json::from_slice(&response).context("parse daemon response")?;
    if parsed["ok"].as_bool() == Some(true) {
        Ok(parsed["response"].clone())
    } else {
        let message = parsed["error"]
            .as_str()
            .unwrap_or("daemon returned an unknown error");
        bail!("{message}")
    }
}

fn daemon_request_bytes(command: &str, payload: Value) -> Result<Vec<u8>> {
    let request = serde_json::to_vec(&json!({
        "command": command,
        "payload": payload,
    }))?;
    if request.len().saturating_add(1) > MAX_REQUEST_BYTES {
        bail!(
            "daemon request is too large after JSON encoding: {} bytes plus newline exceeds {} bytes",
            request.len(),
            MAX_REQUEST_BYTES
        );
    }
    Ok(request)
}

pub(crate) fn validate_daemon_request_budget(command: &str, payload: &Value) -> Result<()> {
    daemon_request_bytes(command, payload.clone()).map(|_| ())
}

fn connect_unix_stream_until(path: &Path, deadline: Instant) -> Result<UnixStream> {
    let fd = unsafe { libc::socket(libc::AF_UNIX, libc::SOCK_STREAM, 0) };
    if fd < 0 {
        return Err(io::Error::last_os_error()).context("create unix socket");
    }
    let guard = FdGuard::new(fd);
    set_fd_nonblocking(guard.fd, true)?;

    let address = socket_address(path)?;
    let rc = unsafe {
        libc::connect(
            guard.fd,
            (&address as *const libc::sockaddr_un).cast(),
            std::mem::size_of::<libc::sockaddr_un>() as libc::socklen_t,
        )
    };
    if rc == 0 {
        return Ok(guard.into_unix_stream());
    }

    let error = io::Error::last_os_error();
    if !is_connect_in_progress(&error) {
        return Err(error).context("connect unix socket");
    }

    poll_fd_until(guard.fd, libc::POLLOUT, deadline).context("wait for unix socket connect")?;
    let mut socket_error: libc::c_int = 0;
    let mut len = std::mem::size_of::<libc::c_int>() as libc::socklen_t;
    let rc = unsafe {
        libc::getsockopt(
            guard.fd,
            libc::SOL_SOCKET,
            libc::SO_ERROR,
            (&mut socket_error as *mut libc::c_int).cast(),
            &mut len,
        )
    };
    if rc != 0 {
        return Err(io::Error::last_os_error()).context("read unix socket connect status");
    }
    if socket_error != 0 {
        return Err(io::Error::from_raw_os_error(socket_error)).context("connect unix socket");
    }
    Ok(guard.into_unix_stream())
}

fn socket_address(path: &Path) -> Result<libc::sockaddr_un> {
    let path_bytes = path.as_os_str().as_bytes();
    let mut address: libc::sockaddr_un = unsafe { std::mem::zeroed() };
    if path_bytes.len() >= address.sun_path.len() {
        bail!("daemon socket path is too long: {}", path.display());
    }
    address.sun_family = libc::AF_UNIX as libc::sa_family_t;
    #[cfg(any(
        target_os = "macos",
        target_os = "ios",
        target_os = "freebsd",
        target_os = "netbsd",
        target_os = "openbsd"
    ))]
    {
        address.sun_len = std::mem::size_of::<libc::sockaddr_un>() as u8;
    }
    for (slot, byte) in address.sun_path.iter_mut().zip(path_bytes) {
        *slot = *byte as libc::c_char;
    }
    Ok(address)
}

fn set_fd_nonblocking(fd: RawFd, enabled: bool) -> Result<()> {
    let flags = unsafe { libc::fcntl(fd, libc::F_GETFL) };
    if flags < 0 {
        return Err(io::Error::last_os_error()).context("read socket flags");
    }
    let updated = if enabled {
        flags | libc::O_NONBLOCK
    } else {
        flags & !libc::O_NONBLOCK
    };
    let rc = unsafe { libc::fcntl(fd, libc::F_SETFL, updated) };
    if rc == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error()).context("set socket nonblocking")
    }
}

fn set_fd_cloexec(fd: RawFd) -> Result<()> {
    let flags = unsafe { libc::fcntl(fd, libc::F_GETFD) };
    if flags < 0 {
        return Err(io::Error::last_os_error()).context("read fd flags");
    }
    let rc = unsafe { libc::fcntl(fd, libc::F_SETFD, flags | libc::FD_CLOEXEC) };
    if rc == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error()).context("set fd close-on-exec")
    }
}

fn is_connect_in_progress(error: &io::Error) -> bool {
    matches!(
        error.raw_os_error(),
        Some(code)
            if code == libc::EINPROGRESS
                || code == libc::EALREADY
                || code == libc::EWOULDBLOCK
                || code == libc::EAGAIN
    )
}

fn write_all_until(stream: &mut UnixStream, mut bytes: &[u8], deadline: Instant) -> Result<()> {
    while !bytes.is_empty() {
        match stream.write(bytes) {
            Ok(0) => bail!("daemon socket write returned zero bytes"),
            Ok(count) => bytes = &bytes[count..],
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                poll_fd_until(stream.as_raw_fd(), libc::POLLOUT, deadline)
                    .context("wait for daemon socket write")?;
            }
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(error) => return Err(error).context("write unix stream"),
        }
    }
    Ok(())
}

fn poll_fd_until(fd: RawFd, events: libc::c_short, deadline: Instant) -> Result<()> {
    loop {
        let mut pollfd = libc::pollfd {
            fd,
            events,
            revents: 0,
        };
        let rc = unsafe {
            libc::poll(
                &mut pollfd,
                1,
                duration_to_poll_timeout_ms(remaining_budget(deadline)?),
            )
        };
        if rc > 0 {
            if pollfd.revents & libc::POLLNVAL != 0 {
                bail!("daemon socket fd is invalid");
            }
            return Ok(());
        }
        if rc == 0 {
            bail!("daemon socket operation deadline exceeded");
        }
        let error = io::Error::last_os_error();
        if error.kind() != io::ErrorKind::Interrupted {
            return Err(error).context("poll unix socket");
        }
    }
}

fn duration_to_poll_timeout_ms(duration: Duration) -> libc::c_int {
    let millis = duration.as_millis().clamp(1, libc::c_int::MAX as u128);
    millis as libc::c_int
}

fn remaining_budget(deadline: Instant) -> Result<Duration> {
    deadline
        .checked_duration_since(Instant::now())
        .filter(|duration| !duration.is_zero())
        .ok_or_else(|| anyhow::anyhow!("startup timeout is exhausted"))
}

fn probe_existing_daemon_for_ensure(
    layout: &FsLayout,
    startup_deadline: Instant,
) -> Result<Option<Value>> {
    loop {
        match daemon_request_with_timeout(layout, "ping", remaining_budget(startup_deadline)?) {
            Ok(response) if daemon_response_is_compatible(&response) => return Ok(Some(response)),
            Ok(_) => {
                return stop_incompatible_daemon(layout, startup_deadline);
            }
            Err(error) if error_is_daemon_busy(&error) => {
                thread::sleep(STARTUP_POLL_INTERVAL);
            }
            Err(error) => {
                if Instant::now() >= startup_deadline {
                    bail!("daemon did not become ready: {error}");
                }
                return Ok(None);
            }
        }
    }
}

fn error_is_daemon_busy(error: &anyhow::Error) -> bool {
    let message = error.to_string();
    matches!(
        message.as_str(),
        DAEMON_BUSY_ERROR | DAEMON_CONNECTION_LIMIT_ERROR
    )
}

fn try_acquire_dispatch_slot(state: &DaemonState) -> Result<DispatchGuard<'_>> {
    loop {
        let current = state.active_dispatches.load(Ordering::Acquire);
        if current >= MAX_DISPATCH_WORKERS {
            bail!(DAEMON_BUSY_ERROR);
        }
        if state
            .active_dispatches
            .compare_exchange(current, current + 1, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            return Ok(DispatchGuard {
                active_dispatches: &state.active_dispatches,
            });
        }
    }
}

fn daemon_response_is_compatible(response: &Value) -> bool {
    let has_protocol = response["protocol_version"].as_u64() == Some(DAEMON_PROTOCOL_VERSION);
    let has_capabilities =
        response["capabilities"]
            .as_array()
            .is_some_and(|reported_capabilities| {
                DAEMON_CAPABILITIES.iter().all(|required_capability| {
                    reported_capabilities
                        .iter()
                        .any(|capability| capability.as_str() == Some(required_capability))
                })
            });
    has_protocol && has_capabilities
}

fn stop_incompatible_daemon(layout: &FsLayout, startup_deadline: Instant) -> Result<Option<Value>> {
    daemon_request_with_timeout(layout, "stop", remaining_budget(startup_deadline)?)
        .context("stop incompatible daemon")?;
    wait_for_incompatible_daemon_replaced_or_removed_until(layout, startup_deadline)
}

fn wait_for_incompatible_daemon_replaced_or_removed_until(
    layout: &FsLayout,
    deadline: Instant,
) -> Result<Option<Value>> {
    let socket_path = layout.daemon_socket_path();
    let mut saw_socket_absent = false;
    loop {
        if !socket_path.exists() {
            if saw_socket_absent {
                return Ok(None);
            }
            saw_socket_absent = true;
            if Instant::now() >= deadline {
                return Ok(None);
            }
            thread::sleep(
                remaining_budget(deadline)
                    .unwrap_or(STARTUP_POLL_INTERVAL)
                    .min(STARTUP_POLL_INTERVAL),
            );
            continue;
        }
        saw_socket_absent = false;
        let probe_budget = remaining_budget(deadline).unwrap_or(STARTUP_POLL_INTERVAL);
        match daemon_request_with_timeout(layout, "ping", probe_budget.min(STARTUP_POLL_INTERVAL)) {
            Ok(response) if daemon_response_is_compatible(&response) => return Ok(Some(response)),
            Ok(_) => {}
            Err(error) if error_is_daemon_busy(&error) => return Ok(None),
            Err(_) => {}
        }
        if Instant::now() >= deadline {
            bail!("incompatible daemon did not stop before startup timeout");
        }
        thread::sleep(STARTUP_POLL_INTERVAL);
    }
}

fn reap_finished_workers(workers: &mut Vec<thread::JoinHandle<()>>) {
    let mut index = 0;
    while index < workers.len() {
        if workers[index].is_finished() {
            let worker = workers.swap_remove(index);
            let _ = worker.join();
        } else {
            index += 1;
        }
    }
}

fn drain_workers_until(workers: &mut Vec<thread::JoinHandle<()>>, deadline: Instant) {
    loop {
        reap_finished_workers(workers);
        if workers.is_empty() || Instant::now() >= deadline {
            return;
        }
        thread::sleep(READ_POLL_INTERVAL);
    }
}

fn reap_finished_lifecycle_maintenance(worker: &mut Option<thread::JoinHandle<()>>) {
    if worker.as_ref().is_some_and(|worker| worker.is_finished()) {
        let worker = worker.take().expect("worker exists");
        let _ = worker.join();
    }
}

fn join_lifecycle_maintenance(worker: Option<thread::JoinHandle<()>>) {
    if let Some(worker) = worker {
        let _ = worker.join();
    }
}

fn drain_lifecycle_maintenance_until(
    worker: &mut Option<thread::JoinHandle<()>>,
    deadline: Instant,
) {
    loop {
        reap_finished_lifecycle_maintenance(worker);
        if worker.is_none() || Instant::now() >= deadline {
            return;
        }
        thread::sleep(READ_POLL_INTERVAL);
    }
}

fn stop_all_supervised_tasks(state: &DaemonState) {
    let term_deadline = Instant::now() + TASK_SHUTDOWN_DURABLE_CANCEL_GRACE;
    let unpersisted_cancel_signal_deadline = term_deadline + TASK_SHUTDOWN_UNPERSISTED_CANCEL_GRACE;
    let kill_deadline = term_deadline + TASK_TERM_GRACE;
    loop {
        let controls = match state.supervised_tasks.lock() {
            Ok(tasks) => tasks
                .iter()
                .map(|(task_id, control)| (task_id.clone(), control.clone()))
                .collect::<Vec<_>>(),
            Err(_) => return,
        };
        if controls.is_empty() {
            return;
        }
        let now = Instant::now();
        let signal = if now >= kill_deadline {
            libc::SIGKILL
        } else {
            libc::SIGTERM
        };
        let mut saw_cancel_target = false;
        let mut saw_unpersisted_cancel_target = false;
        let force_unpersisted_cancel = now >= unpersisted_cancel_signal_deadline;
        for (task_id, control) in &controls {
            if !supervised_task_should_request_stop_cancel(control).unwrap_or(true) {
                continue;
            }
            let _cancel_intent = SupervisedTaskCancelIntent::begin(control).ok();
            if !supervised_task_should_request_stop_cancel(control).unwrap_or(true) {
                continue;
            }
            saw_cancel_target = true;
            block_supervised_task_exec_release(control);
            let durable_cancelled = durably_request_supervised_task_cancel_for_shutdown(
                &state.layout,
                task_id,
                control,
            )
            .is_ok();
            if !durable_cancelled {
                if !force_unpersisted_cancel {
                    saw_unpersisted_cancel_target = true;
                    continue;
                }
                request_supervised_task_cancel(control);
            }
            let _ = signal_current_supervised_task_process_group(control, signal);
        }
        if !saw_cancel_target {
            wait_for_supervised_task_registry_empty(
                state,
                supervised_task_completion_drain_grace(&controls),
            );
            return;
        }
        if now >= kill_deadline && !saw_unpersisted_cancel_target {
            wait_for_supervised_task_process_groups_gone(&controls);
            wait_for_supervised_task_registry_empty(
                state,
                supervised_task_completion_drain_grace(&controls),
            );
            return;
        }
        thread::sleep(TASK_WAIT_POLL_INTERVAL);
    }
}

fn supervised_task_should_request_stop_cancel(control: &SupervisedTaskControl) -> Result<bool> {
    match supervised_task_process_state(control)? {
        SupervisedTaskProcessState::Pending => Ok(true),
        SupervisedTaskProcessState::Completing => Ok(false),
        SupervisedTaskProcessState::Running(_) => {
            let child_exited = current_supervised_task_child_has_exited(control)?;
            let has_live_members =
                current_supervised_task_process_group_has_live_members_after_leader_exit(control)?;
            Ok(!child_exited || has_live_members)
        }
    }
}

fn supervised_task_completion_drain_grace(
    controls: &[(String, Arc<SupervisedTaskControl>)],
) -> Duration {
    let has_natural_completion = controls.iter().any(|(_, control)| {
        control.child_exit_observed.load(Ordering::Acquire)
            && !control.cancel_requested.load(Ordering::Acquire)
    });
    if has_natural_completion {
        TASK_SHUTDOWN_COMPLETION_DRAIN_GRACE
    } else {
        TASK_SHUTDOWN_WORKER_DRAIN_GRACE
    }
}

fn wait_for_supervised_task_process_groups_gone(controls: &[(String, Arc<SupervisedTaskControl>)]) {
    let deadline = Instant::now() + TASK_STREAM_DRAIN_HARD_GRACE + Duration::from_secs(1);
    loop {
        let mut all_gone = true;
        for (_, control) in controls {
            if current_supervised_task_process_group_exists(control).unwrap_or(false) {
                all_gone = false;
                let _ = signal_current_supervised_task_process_group(control, libc::SIGKILL);
            }
        }
        if all_gone || Instant::now() >= deadline {
            return;
        }
        thread::sleep(TASK_WAIT_POLL_INTERVAL);
    }
}

fn wait_for_supervised_task_registry_empty(state: &DaemonState, timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if !has_active_supervised_tasks(state) {
            return true;
        }
        thread::sleep(TASK_WAIT_POLL_INTERVAL);
    }
    !has_active_supervised_tasks(state)
}

fn current_supervised_task_process_group_exists(control: &SupervisedTaskControl) -> Result<bool> {
    let Some(process) = supervised_task_process_snapshot(control)? else {
        return Ok(false);
    };
    if !supervised_task_process_identity_matches(process.pid, &process.pid_identity) {
        return Ok(false);
    }
    Ok(process_group_exists(process.pid))
}

fn current_supervised_task_process_group_has_live_members_after_leader_exit(
    control: &SupervisedTaskControl,
) -> Result<bool> {
    let Some(process) = supervised_task_process_snapshot(control)? else {
        return Ok(false);
    };
    match process_start_identity(process.pid) {
        Ok(Some(current_identity)) => {
            if current_identity != process.pid_identity {
                return Ok(false);
            }
        }
        // The worker still owns the child handle in this path; use the
        // lingering group only to request cooperative cancellation, not to
        // authorize direct shutdown/recovery signaling.
        Ok(None) => {}
        Err(_) => return Ok(false),
    }
    Ok(process_group_has_live_members_after_leader_exit(
        process.pid,
    ))
}

fn current_supervised_task_child_has_exited(control: &SupervisedTaskControl) -> Result<bool> {
    let Some(process) = supervised_task_process_snapshot(control)? else {
        return Ok(false);
    };
    if !supervised_task_process_identity_matches(process.pid, &process.pid_identity) {
        return Ok(true);
    }
    match child_status_without_reaping(process.pid).context("poll supervised task child")? {
        ChildStatusWithoutReaping::Running => Ok(false),
        ChildStatusWithoutReaping::Exited | ChildStatusWithoutReaping::NotWaitable => {
            mark_supervised_task_child_exit_observed(control);
            Ok(true)
        }
    }
}

fn supervised_task_process_state(
    control: &SupervisedTaskControl,
) -> Result<SupervisedTaskProcessState> {
    control
        .process
        .lock()
        .map_err(|_| anyhow::anyhow!("supervised task process lock poisoned"))
        .map(|process| process.clone())
}

fn supervised_task_process_snapshot(
    control: &SupervisedTaskControl,
) -> Result<Option<SupervisedTaskProcess>> {
    match supervised_task_process_state(control)? {
        SupervisedTaskProcessState::Running(process) => Ok(Some(process)),
        SupervisedTaskProcessState::Pending | SupervisedTaskProcessState::Completing => Ok(None),
    }
}

fn supervised_task_process_identity_matches(pid: u32, expected_identity: &str) -> bool {
    matches!(
        process_start_identity(pid),
        Ok(Some(current_identity)) if current_identity == expected_identity
    )
}

fn set_supervised_task_process(
    control: &SupervisedTaskControl,
    pid: u32,
    pid_identity: String,
) -> Result<()> {
    control.child_exit_observed.store(false, Ordering::Release);
    *control
        .process
        .lock()
        .map_err(|_| anyhow::anyhow!("supervised task process lock poisoned"))? =
        SupervisedTaskProcessState::Running(SupervisedTaskProcess { pid, pid_identity });
    Ok(())
}

#[cfg(test)]
fn clear_supervised_task_process(control: &SupervisedTaskControl) -> Result<()> {
    *control
        .process
        .lock()
        .map_err(|_| anyhow::anyhow!("supervised task process lock poisoned"))? =
        SupervisedTaskProcessState::Pending;
    Ok(())
}

fn mark_supervised_task_process_completing(control: &SupervisedTaskControl) -> Result<()> {
    mark_supervised_task_child_exit_observed(control);
    *control
        .process
        .lock()
        .map_err(|_| anyhow::anyhow!("supervised task process lock poisoned"))? =
        SupervisedTaskProcessState::Completing;
    Ok(())
}

fn mark_supervised_task_child_exit_observed(control: &SupervisedTaskControl) {
    control.child_exit_observed.store(true, Ordering::Release);
}

fn recover_lost_task_process_groups(layout: &FsLayout) -> Result<()> {
    let store = Store::open(layout)?;
    for process in store.lost_pending_task_processes()? {
        let Some(expected_identity) = process.pid_identity.as_deref() else {
            continue;
        };
        if !lost_task_process_leader_identity_matches(process.pid, expected_identity) {
            continue;
        }
        signal_process_group(process.pid, libc::SIGTERM);
        thread::sleep(TASK_WAIT_POLL_INTERVAL);
        if lost_task_process_group_needs_kill_after_verified_term(process.pid, expected_identity) {
            signal_process_group(process.pid, libc::SIGKILL);
        }
    }
    Ok(())
}

fn lost_task_process_leader_identity_matches(pid: u32, expected_identity: &str) -> bool {
    matches!(
        process_start_identity(pid),
        Ok(Some(current_identity)) if current_identity == expected_identity
    )
}

fn lost_task_process_group_needs_kill_after_verified_term(
    pid: u32,
    expected_identity: &str,
) -> bool {
    match process_start_identity(pid) {
        Ok(Some(current_identity)) => current_identity == expected_identity,
        // This check is only used after a SIGTERM whose leader identity was
        // verified, so same-PGID members are follow-up cleanup evidence.
        Ok(None) => process_group_has_enumerated_live_members_after_leader_exit(pid),
        Err(_) => false,
    }
}

fn block_supervised_task_exec_release(control: &SupervisedTaskControl) {
    control.exec_release_blocked.store(true, Ordering::Release);
}

fn request_supervised_task_cancel(control: &SupervisedTaskControl) {
    block_supervised_task_exec_release(control);
    control.cancel_requested.store(true, Ordering::Release);
}

struct SupervisedTaskCancelIntent<'a> {
    control: &'a SupervisedTaskControl,
}

impl<'a> SupervisedTaskCancelIntent<'a> {
    fn begin(control: &'a SupervisedTaskControl) -> Result<Self> {
        let _release_decision = control
            .exec_release_decision
            .lock()
            .map_err(|_| anyhow::anyhow!("supervised task exec release decision lock poisoned"))?;
        control.cancel_intents.fetch_add(1, Ordering::AcqRel);
        Ok(Self { control })
    }
}

impl Drop for SupervisedTaskCancelIntent<'_> {
    fn drop(&mut self) {
        self.control.cancel_intents.fetch_sub(1, Ordering::AcqRel);
    }
}

fn supervised_task_cancel_in_flight(control: &SupervisedTaskControl) -> bool {
    control.cancel_intents.load(Ordering::Acquire) > 0
}

fn supervised_task_exec_release_blocked(control: &SupervisedTaskControl) -> bool {
    control.exec_release_blocked.load(Ordering::Acquire)
}

fn acquire_supervised_task_exec_release(
    control: &SupervisedTaskControl,
) -> Result<Option<std::sync::MutexGuard<'_, ()>>> {
    loop {
        while supervised_task_cancel_in_flight(control)
            || supervised_task_exec_release_blocked(control)
        {
            if control.cancel_requested.load(Ordering::Acquire) {
                return Ok(None);
            }
            thread::sleep(TASK_WAIT_POLL_INTERVAL);
        }
        if control.cancel_requested.load(Ordering::Acquire) {
            return Ok(None);
        }
        let release_decision = control
            .exec_release_decision
            .lock()
            .map_err(|_| anyhow::anyhow!("supervised task exec release decision lock poisoned"))?;
        if supervised_task_cancel_in_flight(control) {
            drop(release_decision);
            continue;
        }
        if supervised_task_exec_release_blocked(control) {
            drop(release_decision);
            continue;
        }
        if control.cancel_requested.load(Ordering::Acquire) {
            return Ok(None);
        }
        return Ok(Some(release_decision));
    }
}

fn signal_current_supervised_task_process_group(
    control: &SupervisedTaskControl,
    signal: libc::c_int,
) -> Result<Option<u32>> {
    let Some(process) = supervised_task_process_snapshot(control)? else {
        return Ok(None);
    };
    if !supervised_task_process_identity_matches(process.pid, &process.pid_identity) {
        return Ok(None);
    }
    if signal_process_group(process.pid, signal) {
        if matches!(signal, libc::SIGTERM | libc::SIGKILL)
            && control.cancel_requested.load(Ordering::Acquire)
        {
            control.cancel_signal_sent.store(true, Ordering::Release);
        }
        return Ok(Some(process.pid));
    }
    Ok(None)
}

fn persist_task_cancel_request(layout: &FsLayout, task_id: &str) -> Result<TaskRecord> {
    persist_task_cancel_request_with_open(layout, task_id, Store::open)
}

fn persist_task_cancel_request_for_shutdown(
    layout: &FsLayout,
    task_id: &str,
) -> Result<TaskRecord> {
    persist_task_cancel_request_with_open(layout, task_id, Store::open_for_daemon_lifecycle)
}

fn persist_task_cancel_request_with_open(
    layout: &FsLayout,
    task_id: &str,
    open_store: fn(&FsLayout) -> Result<Store>,
) -> Result<TaskRecord> {
    let now = current_epoch_seconds()?;
    let mut store = open_store(layout)?;
    store.request_task_cancel(task_id, now)
}

fn durably_request_supervised_task_cancel(
    layout: &FsLayout,
    task_id: &str,
    control: &SupervisedTaskControl,
) -> Result<TaskRecord> {
    let task = persist_task_cancel_request(layout, task_id)?;
    request_supervised_task_cancel(control);
    Ok(task)
}

fn durably_request_supervised_task_cancel_for_shutdown(
    layout: &FsLayout,
    task_id: &str,
    control: &SupervisedTaskControl,
) -> Result<TaskRecord> {
    let task = persist_task_cancel_request_for_shutdown(layout, task_id)?;
    request_supervised_task_cancel(control);
    Ok(task)
}

fn task_spawn_exec_gate() -> Result<TaskSpawnExecGate> {
    let _spawn_lock = acquire_process_spawn_lock()?;
    let mut fds = [0 as RawFd; 2];
    if unsafe { libc::pipe(fds.as_mut_ptr()) } != 0 {
        return Err(io::Error::last_os_error()).context("create task spawn exec gate");
    }
    let mut read_fd = FdGuard::new(fds[0]);
    let mut write_fd = FdGuard::new(fds[1]);
    if write_fd.fd == TASK_SPAWN_EXEC_GATE_FD {
        write_fd = dup_fd_min_cloexec(write_fd.fd, TASK_SPAWN_EXEC_GATE_FD + 1)
            .context("move task spawn write fd away from exec gate fd")?;
    }
    if read_fd.fd == TASK_SPAWN_EXEC_GATE_FD {
        read_fd = dup_fd_min_cloexec(read_fd.fd, TASK_SPAWN_EXEC_GATE_FD + 1)
            .context("move task spawn read fd away from exec gate fd")?;
    }
    debug_assert_ne!(read_fd.fd, TASK_SPAWN_EXEC_GATE_FD);
    debug_assert_ne!(write_fd.fd, TASK_SPAWN_EXEC_GATE_FD);
    set_fd_cloexec(read_fd.fd).context("set task spawn read fd close-on-exec")?;
    set_fd_cloexec(write_fd.fd).context("set task spawn write fd close-on-exec")?;
    Ok(TaskSpawnExecGate { read_fd, write_fd })
}

fn dup_fd_min_cloexec(fd: RawFd, min_fd: RawFd) -> Result<FdGuard> {
    let duplicated = unsafe { libc::fcntl(fd, libc::F_DUPFD, min_fd) };
    if duplicated < 0 {
        return Err(io::Error::last_os_error()).context("duplicate fd");
    }
    let guard = FdGuard::new(duplicated);
    set_fd_cloexec(guard.fd)?;
    Ok(guard)
}

fn install_task_spawn_exec_gate(command: &mut Command, gate: &TaskSpawnExecGate) {
    let read_fd = gate.read_fd.fd;
    let write_fd = gate.write_fd.fd;
    unsafe {
        command.pre_exec(move || {
            if libc::dup2(read_fd, TASK_SPAWN_EXEC_GATE_FD) < 0 {
                return Err(io::Error::last_os_error());
            }
            let _ = libc::close(read_fd);
            let _ = libc::close(write_fd);
            Ok(())
        });
    }
}

fn release_task_spawn_exec_gate(gate: TaskSpawnExecGate) -> Result<()> {
    drop(gate.read_fd);
    let bytes = b"go\n";
    let mut written = 0;
    while written < bytes.len() {
        let rc = unsafe {
            libc::write(
                gate.write_fd.fd,
                bytes[written..].as_ptr().cast(),
                bytes.len() - written,
            )
        };
        if rc > 0 {
            written += usize::try_from(rc).unwrap_or(0);
            continue;
        }
        let error = io::Error::last_os_error();
        if error.kind() != io::ErrorKind::Interrupted {
            return Err(error).context("write task spawn exec gate");
        }
    }
    Ok(())
}

fn task_wait_poll_sleep_duration(
    start: Instant,
    timeout: Option<Duration>,
    now: Instant,
    termination_started: bool,
) -> Duration {
    if termination_started {
        return TASK_WAIT_POLL_INTERVAL;
    }
    let Some(timeout) = timeout else {
        return TASK_WAIT_POLL_INTERVAL;
    };
    timeout
        .checked_sub(now.duration_since(start))
        .map(|remaining| remaining.min(TASK_WAIT_POLL_INTERVAL))
        .unwrap_or(Duration::ZERO)
}

fn refresh_lifecycle_cache_if_due(
    state: &DaemonState,
    cache: &mut DaemonLifecycleCache,
    force: bool,
    idle_horizon_at: i64,
) {
    if !force
        && cache
            .refreshed_at
            .is_some_and(|refreshed_at| refreshed_at.elapsed() < DAEMON_LIFECYCLE_POLL_INTERVAL)
    {
        return;
    }

    cache.refreshed_at = Some(Instant::now());
    match refresh_lifecycle_status(state, idle_horizon_at) {
        Ok(status) => {
            cache.refresh_failed = false;
            cache.status = status;
        }
        Err(_error) => {
            // Lifecycle refresh is advisory for shutdown. Keep control RPCs responsive
            // and fail closed by blocking idle exit until a later refresh succeeds.
            cache.refresh_failed = true;
        }
    }
}

fn refresh_lifecycle_status(
    state: &DaemonState,
    idle_horizon_at: i64,
) -> Result<DaemonLifecycleStatus> {
    let now = current_epoch_seconds()?;
    let store = Store::open_for_daemon_lifecycle(&state.layout)?;
    store.daemon_lifecycle_status(now, idle_horizon_at)
}

fn maybe_spawn_lifecycle_maintenance(
    state: &Arc<DaemonState>,
    cache: &DaemonLifecycleCache,
    worker: &mut Option<thread::JoinHandle<()>>,
    next_attempt_at: &mut Instant,
) {
    let now = Instant::now();
    let maintenance_suppressed = state
        .lifecycle_maintenance_suppressed
        .load(Ordering::Acquire);
    let should_recover_lost_tasks = cache.status.nonterminal_tasks > 0;
    let should_sweep = !maintenance_suppressed && cache.status.has_due_maintenance();
    if worker.is_some()
        || cache.refresh_failed
        || (!should_recover_lost_tasks && !should_sweep)
        || now < *next_attempt_at
    {
        return;
    }

    let state = Arc::clone(state);
    *next_attempt_at = now + DAEMON_MAINTENANCE_RETRY_INTERVAL;
    *worker = Some(thread::spawn(move || {
        if state.stop_requested.load(Ordering::Acquire) {
            return;
        }
        let _ = recover_registryless_lost_tasks(&state);
        if state.stop_requested.load(Ordering::Acquire) {
            return;
        }
        if should_sweep {
            let Ok(now) = current_epoch_seconds() else {
                return;
            };
            let Ok(mut store) = Store::open_for_daemon_lifecycle(&state.layout) else {
                return;
            };
            if state.stop_requested.load(Ordering::Acquire) {
                return;
            };
            let _ = store.sweep(&state.layout, now);
        }
    }));
}

fn recover_registryless_lost_tasks(state: &DaemonState) -> Result<usize> {
    recover_registryless_lost_task_process_groups(state)?;
    let now = current_epoch_seconds()?;
    let mut store = Store::open_for_daemon_lifecycle(&state.layout)?;
    store.fail_lost_tasks_excluding_with(now, |task_id| supervised_task_is_active(state, task_id))
}

fn recover_registryless_lost_task_process_groups(state: &DaemonState) -> Result<()> {
    let store = Store::open_for_daemon_lifecycle(&state.layout)?;
    for process in store.lost_pending_task_processes()? {
        if supervised_task_is_active(state, &process.task_id)? {
            continue;
        }
        let Some(expected_identity) = process.pid_identity.as_deref() else {
            continue;
        };
        if !lost_task_process_leader_identity_matches(process.pid, expected_identity) {
            continue;
        }
        signal_process_group(process.pid, libc::SIGTERM);
        thread::sleep(TASK_WAIT_POLL_INTERVAL);
        if !supervised_task_is_active(state, &process.task_id)?
            && lost_task_process_group_needs_kill_after_verified_term(
                process.pid,
                expected_identity,
            )
        {
            signal_process_group(process.pid, libc::SIGKILL);
        }
    }
    Ok(())
}

fn supervised_task_is_active(state: &DaemonState, task_id: &str) -> Result<bool> {
    let tasks = state
        .supervised_tasks
        .lock()
        .map_err(|_| anyhow::anyhow!("supervised task map lock poisoned"))?;
    Ok(tasks.contains_key(task_id))
}

fn checked_epoch_add(now: i64, seconds: u64, name: &str) -> Result<i64> {
    let seconds = i64::try_from(seconds).with_context(|| format!("{name} exceeds i64"))?;
    now.checked_add(seconds)
        .with_context(|| format!("{name} overflows epoch seconds"))
}

fn join_workers(workers: Vec<thread::JoinHandle<()>>) {
    for worker in workers {
        let _ = worker.join();
    }
}

fn client_worker_capacity_reached(active_workers: usize) -> bool {
    active_workers >= MAX_CLIENT_WORKERS
}

fn handle_client(stream: &mut UnixStream, state: &DaemonState) -> Result<()> {
    ensure_peer_is_current_user(stream)?;
    stream
        .set_write_timeout(Some(CLIENT_READ_TIMEOUT))
        .context("set daemon client write timeout")?;
    let bytes = read_limited_until(
        stream,
        MAX_REQUEST_BYTES,
        Instant::now() + CLIENT_READ_TIMEOUT,
    )
    .context("read daemon request")?;
    let request: DaemonRequest = serde_json::from_slice(&bytes).context("parse daemon request")?;
    let response = match request.command.as_str() {
        "ping" => json!({
            "daemon": daemon_info(state),
            "protocol_version": DAEMON_PROTOCOL_VERSION,
            "capabilities": DAEMON_CAPABILITIES,
            "message": "pong",
        }),
        "status" => {
            reap_expired_cli_app_servers(state);
            reap_expired_cli_app_server_reservations(state);
            reap_expired_cli_thread_start_bootstraps(state);
            json!({
                "daemon": daemon_info(state),
                "protocol_version": DAEMON_PROTOCOL_VERSION,
                "capabilities": DAEMON_CAPABILITIES,
                "startup_sweep": &state.startup_sweep,
                "cli_app_servers": cli_app_server_infos(state),
            })
        }
        "cli_app_server_reserve" => {
            let payload: CliAppServerReservePayload = serde_json::from_value(request.payload)
                .context("parse cli app-server reservation payload")?;
            json!({
                "cli_app_server_reservation": reserve_cli_app_server(state, payload)?,
            })
        }
        "cli_app_server_ensure" => {
            let payload: CliAppServerEnsurePayload =
                serde_json::from_value(request.payload).context("parse cli app-server payload")?;
            json!({
                "cli_app_server": ensure_cli_app_server(state, payload)?,
            })
        }
        "cli_app_server_probe" => {
            let payload: CliAppServerProbePayload = serde_json::from_value(request.payload)
                .context("parse cli app-server probe payload")?;
            json!({
                "cli_app_server": probe_cli_app_server(state, payload)?,
            })
        }
        "cli_app_server_refresh" => {
            let payload: CliAppServerLeasePayload = serde_json::from_value(request.payload)
                .context("parse cli app-server lease payload")?;
            json!({
                "cli_app_server": refresh_cli_app_server_lease(state, payload)?,
            })
        }
        "cli_app_server_release" => {
            let payload: CliAppServerReleasePayload = serde_json::from_value(request.payload)
                .context("parse cli app-server reservation release payload")?;
            json!({
                "released": release_cli_app_server_reservation(state, payload)?,
            })
        }
        "cli_app_server_stop" => {
            let payload: CliAppServerStopPayload = serde_json::from_value(request.payload)
                .context("parse cli app-server stop payload")?;
            json!({
                "stopped": stop_cli_app_server(state, payload)?,
            })
        }
        "cli_thread_start" => {
            let payload: CliThreadStartPayload = serde_json::from_value(request.payload)
                .context("parse cli thread/start bootstrap payload")?;
            json!({
                "thread": start_cli_thread(state, payload)?,
            })
        }
        "cli_foreground_thread_start" => {
            let payload: CliForegroundThreadStartPayload = serde_json::from_value(request.payload)
                .context("parse cli foreground thread bootstrap payload")?;
            json!({
                "thread": start_cli_foreground_thread(state, payload)?,
            })
        }
        "cli_thread_start_promote" => {
            let payload: CliThreadStartPromotePayload = serde_json::from_value(request.payload)
                .context("parse cli thread/start promote payload")?;
            json!({
                "cli_app_server": promote_cli_thread_start_app_server(state, payload)?,
            })
        }
        "cli_thread_start_abort" => {
            let payload: CliThreadStartAbortPayload = serde_json::from_value(request.payload)
                .context("parse cli thread/start abort payload")?;
            json!({
                "aborted": abort_cli_thread_start_app_server(state, payload)?,
            })
        }
        "stop" => {
            state.stop_requested.store(true, Ordering::Release);
            json!({
                "daemon": daemon_info(state),
                "stopping": true,
            })
        }
        "dispatch" => {
            state
                .lifecycle_maintenance_suppressed
                .store(false, Ordering::Release);
            let _dispatch_slot = try_acquire_dispatch_slot(state)?;
            let payload: DispatchPayload =
                serde_json::from_value(request.payload).context("parse dispatch payload")?;
            crate::cli::dispatch_daemon_argv(&state.layout, payload.argv)?
        }
        "task_run" => {
            state
                .lifecycle_maintenance_suppressed
                .store(false, Ordering::Release);
            let _dispatch_slot = try_acquire_dispatch_slot(state)?;
            let payload: TaskRunPayload =
                serde_json::from_value(request.payload).context("parse task run payload")?;
            json!({
                "task": start_supervised_task(state, payload)?,
            })
        }
        "task_cancel" => {
            state
                .lifecycle_maintenance_suppressed
                .store(false, Ordering::Release);
            let _dispatch_slot = try_acquire_dispatch_slot(state)?;
            let payload: TaskCancelPayload =
                serde_json::from_value(request.payload).context("parse task cancel payload")?;
            json!({
                "task": cancel_supervised_task(state, payload)?,
            })
        }
        other => bail!("unknown daemon command: {other}"),
    };
    write_ok_response(stream, response)
}

fn daemon_info(state: &DaemonState) -> DaemonInfo {
    DaemonInfo {
        pid: std::process::id(),
        started_at: state.started_at,
        uptime_seconds: state.started_instant.elapsed().as_secs(),
        socket_path: state.layout.daemon_socket_path().display().to_string(),
        idle_timeout_seconds: state.idle_timeout.as_secs(),
        stop_requested: state.stop_requested.load(Ordering::Acquire),
    }
}

fn ensure_daemon_not_stopping(state: &DaemonState) -> Result<()> {
    if state.stop_requested.load(Ordering::Acquire) {
        bail!("daemon is stopping");
    }
    Ok(())
}

type SupervisedTaskWorker = Box<dyn FnOnce() + Send + 'static>;

fn start_supervised_task(state: &DaemonState, payload: TaskRunPayload) -> Result<TaskRecord> {
    start_supervised_task_with_spawner(state, payload, |name, worker| {
        thread::Builder::new().name(name).spawn(worker)
    })
}

fn start_supervised_task_with_spawner<F>(
    state: &DaemonState,
    payload: TaskRunPayload,
    spawn_worker: F,
) -> Result<TaskRecord>
where
    F: FnOnce(String, SupervisedTaskWorker) -> io::Result<thread::JoinHandle<()>>,
{
    validate_daemon_nonempty("source_thread_id", &payload.source_thread_id)?;
    validate_daemon_nonempty("summary", &payload.summary)?;
    validate_daemon_nonempty_bytes("cwd", &payload.cwd)?;
    validate_daemon_nonempty("cwd_display", &payload.cwd_display)?;
    validate_daemon_positive("max_delivery_attempts", payload.max_delivery_attempts)?;
    validate_daemon_positive(
        "redelivery_window_seconds",
        payload.redelivery_window_seconds,
    )?;
    validate_task_environment(&payload.environment)?;
    if let Some(timeout_seconds) = payload.timeout_seconds {
        validate_daemon_positive("timeout_seconds", timeout_seconds)?;
    }
    if payload.command.is_empty() {
        bail!("task command must not be empty");
    }
    validate_daemon_nonempty_bytes("task command argv[0]", &payload.command[0])?;
    ensure_daemon_not_stopping(state)?;

    let task_id = new_id();
    let job_id = new_id();
    let now = current_epoch_seconds()?;
    if now.checked_add(payload.redelivery_window_seconds).is_none() {
        bail!("redelivery_window_seconds overflows timestamp range");
    }
    let command_json = task_command_persistence_json(&payload.command)?;
    let control = Arc::new(SupervisedTaskControl::default());
    {
        let mut supervised_tasks = state
            .supervised_tasks
            .lock()
            .map_err(|_| anyhow::anyhow!("supervised task map lock poisoned"))?;
        ensure_daemon_not_stopping(state)?;
        if supervised_tasks.len() >= MAX_SUPERVISED_TASKS {
            bail!("maximum supervised task limit reached ({MAX_SUPERVISED_TASKS})");
        }
        supervised_tasks.insert(task_id.clone(), control.clone());
    }
    let job = NewJob {
        job_id: job_id.clone(),
        source_thread_id: payload.source_thread_id.clone(),
        summary: payload.summary.clone(),
        metadata_json: payload.metadata_json,
        policy: payload.policy,
        created_at: now,
    };
    let task = NewTask {
        task_id: task_id.clone(),
        job_id: job_id.clone(),
        source_thread_id: payload.source_thread_id,
        summary: payload.summary,
        command_json,
        cwd: payload.cwd_display,
        timeout_seconds: payload.timeout_seconds,
        max_delivery_attempts: payload.max_delivery_attempts,
        redelivery_window_seconds: payload.redelivery_window_seconds,
        created_at: now,
    };
    let layout = state.layout.clone();
    let command = payload.command;
    let environment = payload.environment;
    let cwd = PathBuf::from(OsString::from_vec(payload.cwd));
    let timeout_seconds = task.timeout_seconds;
    let max_delivery_attempts = payload.max_delivery_attempts;
    let redelivery_window_seconds = payload.redelivery_window_seconds;
    let task_registry = state.supervised_tasks.clone();
    let worker_task_id = task_id.clone();
    let worker_job_id = job_id.clone();
    let worker_control = control.clone();
    let worker_registry = task_registry.clone();
    let worker_name = format!("cbth-task-{task_id}");
    let (worker_start_tx, worker_start_rx) = mpsc::channel();
    let worker: SupervisedTaskWorker = Box::new(move || {
        if worker_start_rx.recv().is_err() {
            if let Ok(mut tasks) = worker_registry.lock() {
                tasks.remove(&worker_task_id);
            }
            return;
        }
        run_supervised_task_worker(
            layout,
            worker_task_id,
            worker_job_id,
            command,
            environment,
            cwd,
            timeout_seconds,
            max_delivery_attempts,
            redelivery_window_seconds,
            worker_control,
            worker_registry,
        );
    });
    if let Err(error) = spawn_worker(worker_name, worker) {
        if let Ok(mut tasks) = task_registry.lock() {
            tasks.remove(&task_id);
        }
        return Err(error).context("spawn task supervisor worker");
    }
    let mut store = match retry_task_store_setup("open task setup store", || {
        Store::open_for_task_supervisor_setup(&state.layout)
    }) {
        Ok(store) => store,
        Err(error) => {
            let _ = state
                .supervised_tasks
                .lock()
                .map(|mut tasks| tasks.remove(&task_id));
            return Err(error);
        }
    };
    if let Err(error) = ensure_daemon_not_stopping(state) {
        let _ = state
            .supervised_tasks
            .lock()
            .map(|mut tasks| tasks.remove(&task_id));
        return Err(error);
    }
    let (_job, task) = match retry_task_store_setup("create supervised task", || {
        store.create_task_with_job(job.clone(), task.clone())
    }) {
        Ok(created) => created,
        Err(error) => {
            let _ = state
                .supervised_tasks
                .lock()
                .map(|mut tasks| tasks.remove(&task_id));
            return Err(error);
        }
    };
    if let Err(error) = ensure_daemon_not_stopping(state) {
        drop(worker_start_tx);
        if let Ok(mut tasks) = task_registry.lock() {
            tasks.remove(&task_id);
        }
        store
            .delete_unstarted_task_with_job(&task_id, &job_id)
            .context("delete unstarted task after daemon stop")?;
        return Err(error);
    }
    drop(store);
    if worker_start_tx.send(()).is_err() {
        let reason = "task supervisor worker exited before start signal";
        cleanup_failed_supervised_task_worker_spawn(
            &state.layout,
            &task_id,
            &job_id,
            &task_registry,
            max_delivery_attempts,
            redelivery_window_seconds,
            reason,
        );
        bail!("task supervisor worker exited before start signal");
    }

    Ok(task)
}

fn cleanup_failed_supervised_task_worker_spawn(
    layout: &FsLayout,
    task_id: &str,
    job_id: &str,
    task_registry: &Arc<Mutex<HashMap<String, Arc<SupervisedTaskControl>>>>,
    max_delivery_attempts: i64,
    redelivery_window_seconds: i64,
    reason: &str,
) {
    if let Ok(mut tasks) = task_registry.lock() {
        tasks.remove(task_id);
    }
    terminalize_supervised_task_error(
        layout,
        task_id,
        job_id,
        "failed",
        reason,
        max_delivery_attempts,
        redelivery_window_seconds,
    );
}

fn cancel_supervised_task(state: &DaemonState, payload: TaskCancelPayload) -> Result<TaskRecord> {
    validate_daemon_nonempty("task_id", &payload.task_id)?;
    let control = state
        .supervised_tasks
        .lock()
        .map_err(|_| anyhow::anyhow!("supervised task map lock poisoned"))?
        .get(&payload.task_id)
        .cloned();
    if let Some(control) = control {
        let _cancel_intent = SupervisedTaskCancelIntent::begin(&control)?;
        match supervised_task_process_state(&control)? {
            SupervisedTaskProcessState::Pending => {
                let _spawn_guard = control
                    .spawn_gate
                    .lock()
                    .map_err(|_| anyhow::anyhow!("supervised task spawn gate lock poisoned"))?;
                let task = durably_request_supervised_task_cancel(
                    &state.layout,
                    &payload.task_id,
                    &control,
                )?;
                signal_current_supervised_task_process_group(&control, libc::SIGTERM)?;
                return Ok(task);
            }
            SupervisedTaskProcessState::Completing => {
                return Store::open(&state.layout)?.inspect_task(&payload.task_id);
            }
            SupervisedTaskProcessState::Running(_) => {}
        }
        if current_supervised_task_child_has_exited(&control)?
            && !current_supervised_task_process_group_has_live_members_after_leader_exit(&control)?
        {
            return Store::open(&state.layout)?.inspect_task(&payload.task_id);
        }
        let task =
            durably_request_supervised_task_cancel(&state.layout, &payload.task_id, &control)?;
        signal_current_supervised_task_process_group(&control, libc::SIGTERM)?;
        return Ok(task);
    }
    let task = persist_task_cancel_request(&state.layout, &payload.task_id)?;
    Ok(task)
}

#[allow(clippy::too_many_arguments)]
fn run_supervised_task_worker(
    layout: FsLayout,
    task_id: String,
    job_id: String,
    command: Vec<Vec<u8>>,
    environment: Vec<(Vec<u8>, Vec<u8>)>,
    cwd: PathBuf,
    timeout_seconds: Option<i64>,
    max_delivery_attempts: i64,
    redelivery_window_seconds: i64,
    control: Arc<SupervisedTaskControl>,
    task_registry: Arc<Mutex<HashMap<String, Arc<SupervisedTaskControl>>>>,
) {
    if let Err(error) = run_supervised_task_worker_inner(
        &layout,
        &task_id,
        &job_id,
        command,
        environment,
        &cwd,
        timeout_seconds,
        max_delivery_attempts,
        redelivery_window_seconds,
        &control,
    ) {
        let reason = format!("task supervisor error: {error:#}");
        let cancelled = control.cancel_requested.load(Ordering::Acquire);
        let task_status = if cancelled { "cancelled" } else { "failed" };
        let terminal_reason = if cancelled {
            "task cancelled"
        } else {
            reason.as_str()
        };
        terminalize_supervised_task_error(
            &layout,
            &task_id,
            &job_id,
            task_status,
            terminal_reason,
            max_delivery_attempts,
            redelivery_window_seconds,
        );
    }
    if let Ok(mut tasks) = task_registry.lock() {
        tasks.remove(&task_id);
    }
}

fn terminalize_supervised_task_error(
    layout: &FsLayout,
    task_id: &str,
    job_id: &str,
    task_status: &str,
    reason: &str,
    max_delivery_attempts: i64,
    redelivery_window_seconds: i64,
) {
    let _ = terminalize_supervised_task_error_with_timeout(
        layout,
        task_id,
        job_id,
        task_status,
        reason,
        max_delivery_attempts,
        redelivery_window_seconds,
        TASK_STORE_COMPLETION_RETRY_TIMEOUT,
    );
}

#[allow(clippy::too_many_arguments)]
fn terminalize_supervised_task_error_with_timeout(
    layout: &FsLayout,
    task_id: &str,
    job_id: &str,
    task_status: &str,
    reason: &str,
    max_delivery_attempts: i64,
    redelivery_window_seconds: i64,
    retry_timeout: Duration,
) -> Result<()> {
    let stdout_log_path = format!("tasks/{task_id}/stdout.log");
    let stderr_log_path = format!("tasks/{task_id}/stderr.log");
    retry_task_store_completion_with_timeout(
        "terminalize task supervisor error",
        retry_timeout,
        || {
            let now = current_epoch_seconds()?;
            let mut store = Store::open_for_task_supervisor_setup(layout)?;
            let update = TaskFinishUpdate {
                task_id,
                status: task_status,
                completed_at: now,
                exit_code: None,
                signal: None,
                failure_reason: Some(reason),
                stdout_log_path: Some(&stdout_log_path),
                stderr_log_path: Some(&stderr_log_path),
                stdout_bytes: 0,
                stderr_bytes: 0,
                stdout_truncated: false,
                stderr_truncated: false,
            };
            store
                .fail_supervised_task_with_job(
                    job_id,
                    reason,
                    update,
                    max_delivery_attempts,
                    redelivery_window_seconds,
                )
                .map(|_| ())
        },
    )
}

#[allow(clippy::too_many_arguments)]
fn run_supervised_task_worker_inner(
    layout: &FsLayout,
    task_id: &str,
    job_id: &str,
    command: Vec<Vec<u8>>,
    environment: Vec<(Vec<u8>, Vec<u8>)>,
    cwd: &Path,
    timeout_seconds: Option<i64>,
    max_delivery_attempts: i64,
    redelivery_window_seconds: i64,
    control: &Arc<SupervisedTaskControl>,
) -> Result<()> {
    let task_dir = layout.task_dir(task_id);
    ensure_private_dir(&task_dir)?;
    let stdout_path = task_dir.join("stdout.log");
    let stderr_path = task_dir.join("stderr.log");
    let result_path = task_dir.join("result.txt");
    let command_display = task_command_display(&command);

    let program = OsString::from_vec(command[0].clone());
    let args = command
        .iter()
        .skip(1)
        .map(|arg| OsString::from_vec(arg.clone()))
        .collect::<Vec<_>>();
    let mut command_builder = Command::new("/bin/sh");
    command_builder.env_clear();
    let mut saw_pwd = false;
    for (key, value) in environment {
        if key == b"PWD" {
            saw_pwd = true;
            command_builder.env(OsString::from_vec(key), cwd.as_os_str());
        } else {
            command_builder.env(OsString::from_vec(key), OsString::from_vec(value));
        }
    }
    if !saw_pwd {
        command_builder.env("PWD", cwd.as_os_str());
    }
    let spawn_guard = control
        .spawn_gate
        .lock()
        .map_err(|_| anyhow::anyhow!("supervised task spawn gate lock poisoned"))?;
    if control.cancel_requested.load(Ordering::Acquire) {
        finish_unspawned_cancelled_task(
            layout,
            task_id,
            job_id,
            max_delivery_attempts,
            redelivery_window_seconds,
        )?;
        return Ok(());
    }
    let exec_gate = task_spawn_exec_gate()?;
    install_task_spawn_exec_gate(&mut command_builder, &exec_gate);
    command_builder
        .arg("-c")
        .arg(TASK_SPAWN_EXEC_GATE_SCRIPT)
        .arg("cbth-task-gate")
        .arg(&program)
        .args(args)
        .current_dir(cwd)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .process_group(0);
    let mut child = spawn_command_locked(&mut command_builder)
        .with_context(|| format!("spawn task command {:?}", program))?;
    let pid = child.id();
    let pid_identity = match process_start_identity(pid)
        .and_then(|identity| identity.with_context(|| format!("task process {pid} disappeared")))
    {
        Ok(identity) => identity,
        Err(error) => {
            terminate_task_child_best_effort(&mut child, pid);
            mark_supervised_task_process_completing(control)?;
            return Err(error).with_context(|| format!("record task process identity for {pid}"));
        }
    };
    set_supervised_task_process(control, pid, pid_identity.clone())?;
    let started_at = match current_epoch_seconds() {
        Ok(started_at) => started_at,
        Err(error) => {
            terminate_task_child_best_effort(&mut child, pid);
            mark_supervised_task_process_completing(control)?;
            return Err(error);
        }
    };
    if let Err(error) = retry_task_store_setup("mark task started", || {
        let mut store = Store::open_for_task_supervisor_setup(layout)?;
        store.mark_task_started(task_id, i64::from(pid), Some(&pid_identity), started_at)
    }) {
        terminate_task_child_best_effort(&mut child, pid);
        mark_supervised_task_process_completing(control)?;
        return Err(error);
    }
    drop(spawn_guard);
    let exec_release_decision = match acquire_supervised_task_exec_release(control)? {
        Some(exec_release_decision) => exec_release_decision,
        None => {
            finish_cancelled_before_exec_gate_release(
                layout,
                task_id,
                job_id,
                max_delivery_attempts,
                redelivery_window_seconds,
                control,
                &mut child,
                pid,
            )?;
            return Ok(());
        }
    };
    if control.cancel_requested.load(Ordering::Acquire) {
        finish_cancelled_before_exec_gate_release(
            layout,
            task_id,
            job_id,
            max_delivery_attempts,
            redelivery_window_seconds,
            control,
            &mut child,
            pid,
        )?;
        return Ok(());
    }
    if let Err(error) = release_task_spawn_exec_gate(exec_gate) {
        terminate_task_child_best_effort(&mut child, pid);
        mark_supervised_task_process_completing(control)?;
        return Err(error).with_context(|| format!("release task {task_id}"));
    }
    drop(exec_release_decision);
    let start = Instant::now();
    let result = run_supervised_spawned_task(
        layout,
        task_id,
        job_id,
        &command_display,
        cwd,
        timeout_seconds,
        max_delivery_attempts,
        redelivery_window_seconds,
        control,
        &mut child,
        pid,
        start,
        stdout_path,
        stderr_path,
        result_path,
    );
    if result.is_err() {
        let should_terminate =
            supervised_task_process_snapshot(control)?.is_some_and(|process| process.pid == pid);
        if should_terminate {
            terminate_task_child_best_effort(&mut child, pid);
            mark_supervised_task_process_completing(control)?;
        }
    }
    result
}

#[allow(clippy::too_many_arguments)]
fn finish_cancelled_before_exec_gate_release(
    layout: &FsLayout,
    task_id: &str,
    job_id: &str,
    max_delivery_attempts: i64,
    redelivery_window_seconds: i64,
    control: &SupervisedTaskControl,
    child: &mut Child,
    pid: u32,
) -> Result<()> {
    terminate_task_child_best_effort(child, pid);
    mark_supervised_task_process_completing(control)?;
    finish_unspawned_cancelled_task(
        layout,
        task_id,
        job_id,
        max_delivery_attempts,
        redelivery_window_seconds,
    )
}

fn finish_unspawned_cancelled_task(
    layout: &FsLayout,
    task_id: &str,
    job_id: &str,
    max_delivery_attempts: i64,
    redelivery_window_seconds: i64,
) -> Result<()> {
    let now = current_epoch_seconds()?;
    let mut store = Store::open(layout)?;
    store.fail_job(
        job_id,
        "task cancelled",
        now,
        max_delivery_attempts,
        redelivery_window_seconds,
    )?;
    store.finish_task(TaskFinishUpdate {
        task_id,
        status: "cancelled",
        completed_at: now,
        exit_code: None,
        signal: None,
        failure_reason: Some("task cancelled"),
        stdout_log_path: Some(&format!("tasks/{task_id}/stdout.log")),
        stderr_log_path: Some(&format!("tasks/{task_id}/stderr.log")),
        stdout_bytes: 0,
        stderr_bytes: 0,
        stdout_truncated: false,
        stderr_truncated: false,
    })?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn run_supervised_spawned_task(
    layout: &FsLayout,
    task_id: &str,
    job_id: &str,
    command_display: &str,
    cwd: &Path,
    timeout_seconds: Option<i64>,
    max_delivery_attempts: i64,
    redelivery_window_seconds: i64,
    control: &Arc<SupervisedTaskControl>,
    child: &mut Child,
    pid: u32,
    start: Instant,
    stdout_path: PathBuf,
    stderr_path: PathBuf,
    result_path: PathBuf,
) -> Result<()> {
    let stdout = child
        .stdout
        .take()
        .context("task child stdout was not piped")?;
    let stderr = child
        .stderr
        .take()
        .context("task child stderr was not piped")?;
    set_fd_nonblocking(stdout.as_raw_fd(), true).context("set task stdout nonblocking")?;
    set_fd_nonblocking(stderr.as_raw_fd(), true).context("set task stderr nonblocking")?;
    let stdout_worker = spawn_task_stream_spool(stdout, stdout_path);
    let stderr_worker = spawn_task_stream_spool(stderr, stderr_path);
    let timeout = timeout_seconds
        .and_then(|seconds| u64::try_from(seconds).ok())
        .map(Duration::from_secs);
    let mut stream_state = TaskStreamJoinState::new(stdout_worker, stderr_worker);
    let mut timed_out = false;
    let mut cancelled = false;
    let mut termination_started_at: Option<Instant> = None;
    let mut sent_kill = false;
    let child_exited_at = loop {
        stream_state.poll_finished();
        if stream_state.stream_error.is_some() {
            return fail_spawned_task_after_stream_spool_error(
                stream_state,
                child,
                pid,
                control,
                start,
                timeout,
                &mut cancelled,
                &mut timed_out,
            );
        }
        match child_status_without_reaping(pid).context("poll task child without reaping")? {
            ChildStatusWithoutReaping::Exited => {
                if !timed_out && control.cancel_signal_sent.load(Ordering::Acquire) {
                    cancelled = true;
                }
                let exited_at = Instant::now();
                mark_supervised_task_child_exit_observed(control);
                break exited_at;
            }
            ChildStatusWithoutReaping::Running => {}
            ChildStatusWithoutReaping::NotWaitable => {
                bail!("task child is no longer waitable before completion");
            }
        }
        if termination_started_at.is_none()
            && !timed_out
            && control.cancel_requested.load(Ordering::Acquire)
            && signal_process_group(pid, libc::SIGTERM)
        {
            cancelled = true;
            control.cancel_signal_sent.store(true, Ordering::Release);
            termination_started_at = Some(Instant::now());
        }
        if termination_started_at.is_none()
            && timeout.is_some_and(|timeout| start.elapsed() >= timeout)
        {
            match child_status_without_reaping(pid)
                .context("re-check task child before timeout termination")?
            {
                ChildStatusWithoutReaping::Exited => {
                    let exited_at = Instant::now();
                    mark_supervised_task_child_exit_observed(control);
                    break exited_at;
                }
                ChildStatusWithoutReaping::Running => {
                    if signal_process_group(pid, libc::SIGTERM) {
                        timed_out = true;
                        termination_started_at = Some(Instant::now());
                    }
                }
                ChildStatusWithoutReaping::NotWaitable => {
                    bail!("task child is no longer waitable before timeout termination");
                }
            }
        }
        if let Some(started_at) = termination_started_at
            && !sent_kill
            && started_at.elapsed() >= TASK_TERM_GRACE
        {
            signal_process_group(pid, libc::SIGKILL);
            sent_kill = true;
        }
        thread::sleep(task_wait_poll_sleep_duration(
            start,
            timeout,
            Instant::now(),
            termination_started_at.is_some(),
        ));
    };
    let (stdout_capture, stderr_capture) = join_task_stream_spools_with_control_state(
        stream_state,
        pid,
        control,
        start,
        child_exited_at,
        timeout,
        &mut cancelled,
        &mut timed_out,
    )?;
    wait_process_group_exit_with_control(
        pid,
        control,
        start,
        child_exited_at,
        timeout,
        &mut cancelled,
        &mut timed_out,
    )?;
    let status = match child.wait().context("wait task child") {
        Ok(status) => status,
        Err(error) => {
            mark_supervised_task_process_completing(control)?;
            return Err(error);
        }
    };
    mark_supervised_task_process_completing(control)?;
    if !timed_out && control.cancel_signal_sent.load(Ordering::Acquire) {
        cancelled = true;
    }
    let completed_at = current_epoch_seconds()?;
    let exit_code = status.code().map(i64::from);
    let signal = status.signal().map(i64::from);
    let task_status = task_status_for_exit(status, cancelled, timed_out);
    let failure_reason = task_failure_reason(task_status, exit_code, signal);
    let delivery_summary = build_task_delivery_summary(TaskDeliverySummaryInput {
        task_status,
        command: command_display,
        cwd,
        exit_code,
        signal,
        stdout: &stdout_capture,
        stderr: &stderr_capture,
    });
    write_task_result_file(
        &result_path,
        task_id,
        job_id,
        command_display,
        cwd,
        task_status,
        exit_code,
        signal,
        &stdout_capture,
        &stderr_capture,
    )?;
    retry_task_store_completion("close task job with result", || {
        close_task_job_with_result(
            layout,
            job_id,
            &result_path,
            task_status,
            &delivery_summary,
            completed_at,
            max_delivery_attempts,
            redelivery_window_seconds,
        )
    })?;
    let stdout_log_path = format!("tasks/{task_id}/stdout.log");
    let stderr_log_path = format!("tasks/{task_id}/stderr.log");
    retry_task_store_completion("finish task", || {
        Store::open(layout)?
            .finish_task(TaskFinishUpdate {
                task_id,
                status: task_status,
                completed_at,
                exit_code,
                signal,
                failure_reason: failure_reason.as_deref(),
                stdout_log_path: Some(&stdout_log_path),
                stderr_log_path: Some(&stderr_log_path),
                stdout_bytes: i64::try_from(stdout_capture.bytes_seen).unwrap_or(i64::MAX),
                stderr_bytes: i64::try_from(stderr_capture.bytes_seen).unwrap_or(i64::MAX),
                stdout_truncated: stdout_capture.truncated,
                stderr_truncated: stderr_capture.truncated,
            })
            .map(|_| ())
    })?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn fail_spawned_task_after_stream_spool_error(
    stream_state: TaskStreamJoinState,
    child: &mut Child,
    pid: u32,
    control: &SupervisedTaskControl,
    start: Instant,
    timeout: Option<Duration>,
    cancelled: &mut bool,
    timed_out: &mut bool,
) -> Result<()> {
    stream_state.abort_workers();
    let terminate_result = terminate_task_child(child, pid)
        .with_context(|| format!("terminate task {pid} after stream spool failure"));
    mark_supervised_task_child_exit_observed(control);
    let _ = child.wait();
    mark_supervised_task_process_completing(control)?;
    let join_result = join_task_stream_spools_with_control_state(
        stream_state,
        pid,
        control,
        start,
        Instant::now(),
        timeout,
        cancelled,
        timed_out,
    )
    .map(|_| ());
    match (join_result, terminate_result) {
        (Err(error), _) => Err(error),
        (Ok(()), Err(error)) => Err(error),
        (Ok(()), Ok(())) => bail!("task stream failed without an error"),
    }
}

fn task_command_display(command: &[Vec<u8>]) -> String {
    let mut rendered = command
        .iter()
        .map(|arg| format!("{:?}", OsStr::from_bytes(arg)))
        .collect::<Vec<_>>()
        .join(" ");
    truncate_string_bytes(&mut rendered, TASK_PROMPT_COMMAND_PREVIEW_BYTES);
    rendered
}

fn task_command_persistence_json(command: &[Vec<u8>]) -> Result<String> {
    serde_json::to_string(&json!({
        "format": "argv-bytes-v1",
        "argv": command,
        "display": task_command_display(command),
    }))
    .context("serialize task command")
}

fn terminate_task_child(child: &mut Child, pid: u32) -> Result<()> {
    signal_process_group(pid, libc::SIGTERM);
    let deadline = Instant::now() + TASK_TERM_GRACE;
    while Instant::now() < deadline {
        if task_process_group_is_gone(child, pid)? {
            return Ok(());
        }
        thread::sleep(TASK_WAIT_POLL_INTERVAL);
    }
    signal_process_group(pid, libc::SIGKILL);
    let kill_deadline = Instant::now() + TASK_STREAM_DRAIN_HARD_GRACE + Duration::from_secs(1);
    while Instant::now() < kill_deadline {
        if task_process_group_is_gone(child, pid)? {
            return Ok(());
        }
        thread::sleep(TASK_WAIT_POLL_INTERVAL);
    }
    bail!("task process group did not stop after SIGKILL")
}

fn terminate_task_child_best_effort(child: &mut Child, pid: u32) {
    let _ = terminate_task_child(child, pid);
    let _ = child.wait();
}

fn task_process_group_is_gone(child: &mut Child, pid: u32) -> Result<bool> {
    let leader_exited = child
        .try_wait()
        .context("poll terminating task child")?
        .is_some();
    if !leader_exited {
        return Ok(false);
    }
    Ok(!process_group_has_live_members_after_leader_exit(pid))
}

#[derive(Debug)]
struct TaskStreamCapture {
    bytes_seen: u64,
    truncated: bool,
    tail: Vec<u8>,
}

struct TaskStreamWorker {
    handle: thread::JoinHandle<Result<TaskStreamCapture>>,
    abort: Arc<AtomicBool>,
}

struct TaskStreamJoinState {
    stdout_worker: Option<TaskStreamWorker>,
    stderr_worker: Option<TaskStreamWorker>,
    stdout_capture: Option<TaskStreamCapture>,
    stderr_capture: Option<TaskStreamCapture>,
    stream_error: Option<anyhow::Error>,
}

impl TaskStreamJoinState {
    fn new(stdout_worker: TaskStreamWorker, stderr_worker: TaskStreamWorker) -> Self {
        Self {
            stdout_worker: Some(stdout_worker),
            stderr_worker: Some(stderr_worker),
            stdout_capture: None,
            stderr_capture: None,
            stream_error: None,
        }
    }

    fn poll_finished(&mut self) {
        if let Some(result) = join_finished_task_stream_spool(&mut self.stdout_worker) {
            record_finished_task_stream_spool(
                "stdout",
                result,
                &mut self.stdout_capture,
                &mut self.stream_error,
            );
        }
        if let Some(result) = join_finished_task_stream_spool(&mut self.stderr_worker) {
            record_finished_task_stream_spool(
                "stderr",
                result,
                &mut self.stderr_capture,
                &mut self.stream_error,
            );
        }
    }

    fn abort_workers(&self) {
        abort_task_stream_worker(&self.stdout_worker);
        abort_task_stream_worker(&self.stderr_worker);
    }
}

fn spawn_task_stream_spool<R>(reader: R, path: PathBuf) -> TaskStreamWorker
where
    R: Read + Send + 'static,
{
    spawn_task_stream_spool_with_limits(
        reader,
        path,
        TASK_STREAM_SPOOL_MAX_BYTES,
        TASK_STREAM_TAIL_BYTES,
    )
}

fn spawn_task_stream_spool_with_limits<R>(
    mut reader: R,
    path: PathBuf,
    spool_max_bytes: u64,
    tail_bytes: usize,
) -> TaskStreamWorker
where
    R: Read + Send + 'static,
{
    let abort = Arc::new(AtomicBool::new(false));
    let worker_abort = abort.clone();
    let handle = thread::spawn(move || {
        let mut file = create_private_file(&path)?;
        let mut tail = VecDeque::new();
        let mut bytes_seen = 0_u64;
        let mut truncated = false;
        let mut buffer = [0_u8; 8192];
        loop {
            if worker_abort.load(Ordering::Acquire) {
                truncated = true;
                break;
            }
            let read = match reader.read(&mut buffer) {
                Ok(read) => read,
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                    thread::sleep(READ_POLL_INTERVAL);
                    continue;
                }
                Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                Err(error) => {
                    return Err(error)
                        .with_context(|| format!("read task stream {}", path.display()));
                }
            };
            if read == 0 {
                break;
            }
            bytes_seen = bytes_seen.saturating_add(u64::try_from(read).unwrap_or(u64::MAX));
            let remaining = spool_max_bytes.saturating_sub(file.metadata()?.len());
            if remaining > 0 {
                let write_len = read.min(usize::try_from(remaining).unwrap_or(usize::MAX));
                file.write_all(&buffer[..write_len])
                    .with_context(|| format!("write task stream {}", path.display()))?;
                if write_len < read {
                    truncated = true;
                }
            } else {
                truncated = true;
            }
            for byte in &buffer[..read] {
                if tail.len() == tail_bytes {
                    tail.pop_front();
                }
                if tail_bytes > 0 {
                    tail.push_back(*byte);
                }
            }
        }
        if !worker_abort.load(Ordering::Acquire) {
            file.sync_all()
                .with_context(|| format!("sync task stream {}", path.display()))?;
            if let Some(parent) = path.parent() {
                sync_dir(parent)?;
            }
        }
        Ok(TaskStreamCapture {
            bytes_seen,
            truncated,
            tail: tail.into_iter().collect(),
        })
    });
    TaskStreamWorker { handle, abort }
}

fn join_task_stream_spool(worker: TaskStreamWorker) -> Result<TaskStreamCapture> {
    worker
        .handle
        .join()
        .map_err(|_| anyhow::anyhow!("task stream worker panicked"))?
}

fn join_finished_task_stream_spool(
    worker: &mut Option<TaskStreamWorker>,
) -> Option<Result<TaskStreamCapture>> {
    if worker
        .as_ref()
        .is_some_and(|worker| worker.handle.is_finished())
    {
        return Some(join_task_stream_spool(
            worker.take().expect("stream worker is present"),
        ));
    }
    None
}

fn abort_task_stream_worker(worker: &Option<TaskStreamWorker>) {
    if let Some(worker) = worker {
        worker.abort.store(true, Ordering::Release);
    }
}

fn record_finished_task_stream_spool(
    label: &str,
    result: Result<TaskStreamCapture>,
    capture: &mut Option<TaskStreamCapture>,
    stream_error: &mut Option<anyhow::Error>,
) {
    match result {
        Ok(value) => *capture = Some(value),
        Err(error) if stream_error.is_none() => {
            *stream_error = Some(error.context(format!("{label} task stream failed")));
        }
        Err(_) => {}
    }
}

fn wait_process_group_exit_with_control(
    pid: u32,
    control: &SupervisedTaskControl,
    start: Instant,
    child_exited_at: Instant,
    timeout: Option<Duration>,
    cancelled: &mut bool,
    timed_out: &mut bool,
) -> Result<()> {
    let mut termination_started_at = None;
    let mut sent_kill = false;
    loop {
        if !process_group_has_live_members_after_leader_exit(pid) {
            return Ok(());
        }
        if termination_started_at.is_none()
            && !*timed_out
            && control.cancel_requested.load(Ordering::Acquire)
            && signal_process_group(pid, libc::SIGTERM)
        {
            *cancelled = true;
            control.cancel_signal_sent.store(true, Ordering::Release);
            termination_started_at = Some(Instant::now());
        }
        if termination_started_at.is_none()
            && timeout.is_some_and(|timeout| start.elapsed() >= timeout)
        {
            *timed_out = true;
            signal_process_group(pid, libc::SIGTERM);
            termination_started_at = Some(Instant::now());
        }
        if termination_started_at.is_none()
            && child_exited_at.elapsed() >= TASK_LEADER_EXITED_PROCESS_GROUP_GRACE
        {
            signal_process_group(pid, libc::SIGTERM);
            termination_started_at = Some(Instant::now());
        }
        if let Some(started_at) = termination_started_at
            && !sent_kill
            && started_at.elapsed() >= TASK_TERM_GRACE
        {
            signal_process_group(pid, libc::SIGKILL);
            sent_kill = true;
        }
        if sent_kill
            && termination_started_at.is_some_and(|started_at| {
                started_at.elapsed() >= TASK_TERM_GRACE + TASK_STREAM_DRAIN_HARD_GRACE
            })
            && process_group_has_live_members_after_leader_exit(pid)
        {
            bail!("task process group did not stop after SIGKILL");
        }
        thread::sleep(TASK_WAIT_POLL_INTERVAL);
    }
}

#[cfg(test)]
#[allow(clippy::too_many_arguments)]
fn join_task_stream_spools_with_control(
    stdout_worker: TaskStreamWorker,
    stderr_worker: TaskStreamWorker,
    pid: u32,
    control: &SupervisedTaskControl,
    start: Instant,
    child_exited_at: Instant,
    timeout: Option<Duration>,
    cancelled: &mut bool,
    timed_out: &mut bool,
) -> Result<(TaskStreamCapture, TaskStreamCapture)> {
    join_task_stream_spools_with_control_state(
        TaskStreamJoinState::new(stdout_worker, stderr_worker),
        pid,
        control,
        start,
        child_exited_at,
        timeout,
        cancelled,
        timed_out,
    )
}

#[allow(clippy::too_many_arguments)]
fn join_task_stream_spools_with_control_state(
    mut stream_state: TaskStreamJoinState,
    pid: u32,
    control: &SupervisedTaskControl,
    start: Instant,
    child_exited_at: Instant,
    timeout: Option<Duration>,
    cancelled: &mut bool,
    timed_out: &mut bool,
) -> Result<(TaskStreamCapture, TaskStreamCapture)> {
    let mut termination_started_at = None;
    let mut sent_kill = false;
    let mut stream_abort_started_at = None;
    let mut process_group_gone_at = None;
    loop {
        stream_state.poll_finished();
        if stream_state.stdout_worker.is_none() && stream_state.stderr_worker.is_none() {
            break;
        }
        let process_group_alive = process_group_has_live_members_after_leader_exit(pid);
        if process_group_alive {
            process_group_gone_at = None;
        } else if process_group_gone_at.is_none() {
            process_group_gone_at = Some(Instant::now());
        }
        if stream_state.stream_error.is_some() {
            stream_state.abort_workers();
            if stream_abort_started_at.is_none() {
                stream_abort_started_at = Some(Instant::now());
            }
            if termination_started_at.is_none()
                && process_group_alive
                && signal_process_group(pid, libc::SIGTERM)
            {
                termination_started_at = Some(Instant::now());
            }
        }
        if termination_started_at.is_none()
            && process_group_alive
            && !*timed_out
            && control.cancel_requested.load(Ordering::Acquire)
            && signal_process_group(pid, libc::SIGTERM)
        {
            *cancelled = true;
            control.cancel_signal_sent.store(true, Ordering::Release);
            termination_started_at = Some(Instant::now());
        }
        if termination_started_at.is_none()
            && process_group_alive
            && timeout.is_some_and(|timeout| start.elapsed() >= timeout)
        {
            *timed_out = true;
            signal_process_group(pid, libc::SIGTERM);
            termination_started_at = Some(Instant::now());
        }
        if termination_started_at.is_none()
            && process_group_alive
            && child_exited_at.elapsed() >= TASK_LEADER_EXITED_PROCESS_GROUP_GRACE
        {
            signal_process_group(pid, libc::SIGTERM);
            termination_started_at = Some(Instant::now());
        }
        if termination_started_at.is_none()
            && !process_group_alive
            && stream_abort_started_at.is_none()
            && (control.cancel_requested.load(Ordering::Acquire)
                || timeout.is_some_and(|timeout| start.elapsed() >= timeout)
                || process_group_gone_at
                    .is_some_and(|gone_at| gone_at.elapsed() >= TASK_STREAM_DRAIN_HARD_GRACE))
        {
            stream_state.abort_workers();
            stream_abort_started_at = Some(Instant::now());
        }
        if let Some(started_at) = termination_started_at
            && !sent_kill
            && started_at.elapsed() >= TASK_TERM_GRACE
        {
            signal_process_group(pid, libc::SIGKILL);
            sent_kill = true;
        }
        if sent_kill
            && stream_abort_started_at.is_none()
            && termination_started_at.is_some_and(|started_at| {
                started_at.elapsed() >= TASK_TERM_GRACE + TASK_STREAM_DRAIN_HARD_GRACE
            })
        {
            stream_state.abort_workers();
            stream_abort_started_at = Some(Instant::now());
        }
        if stream_abort_started_at
            .is_some_and(|started_at| started_at.elapsed() >= TASK_STREAM_DRAIN_HARD_GRACE)
            && (stream_state.stdout_worker.is_some() || stream_state.stderr_worker.is_some())
        {
            return match stream_state.stream_error.take() {
                Some(error) => {
                    Err(error.context("task stream workers did not stop after abort deadline"))
                }
                None => bail!("task stream workers did not stop after abort deadline"),
            };
        }
        thread::sleep(TASK_WAIT_POLL_INTERVAL);
    }

    if let Some(error) = stream_state.stream_error {
        return Err(error);
    }
    Ok((
        stream_state
            .stdout_capture
            .context("stdout task stream did not produce a capture")?,
        stream_state
            .stderr_capture
            .context("stderr task stream did not produce a capture")?,
    ))
}

fn task_status_for_exit(status: ExitStatus, cancelled: bool, timed_out: bool) -> &'static str {
    if timed_out {
        "timed_out"
    } else if cancelled {
        "cancelled"
    } else if status.success() {
        "succeeded"
    } else {
        "failed"
    }
}

fn task_failure_reason(
    task_status: &str,
    exit_code: Option<i64>,
    signal: Option<i64>,
) -> Option<String> {
    match task_status {
        "succeeded" => None,
        "cancelled" => Some("task cancelled".to_owned()),
        "timed_out" => Some("task timed out".to_owned()),
        "failed" => Some(match (exit_code, signal) {
            (Some(code), _) => format!("task exited with status {code}"),
            (_, Some(signal)) => format!("task terminated by signal {signal}"),
            _ => "task failed".to_owned(),
        }),
        other => Some(format!("task ended with status {other}")),
    }
}

struct TaskDeliverySummaryInput<'a> {
    task_status: &'a str,
    command: &'a str,
    cwd: &'a Path,
    exit_code: Option<i64>,
    signal: Option<i64>,
    stdout: &'a TaskStreamCapture,
    stderr: &'a TaskStreamCapture,
}

fn build_task_delivery_summary(input: TaskDeliverySummaryInput<'_>) -> String {
    format!(
        "Background task {status}.\n\
         Command: {command}\n\
         Cwd: {cwd}\n\
         Exit code: {exit_code:?}\n\
         Signal: {signal:?}\n\
         stdout bytes: {stdout_bytes} (truncated: {stdout_truncated})\n\
         stderr bytes: {stderr_bytes} (truncated: {stderr_truncated})\n\
         stdout/stderr tails are omitted from this automatic-delivery prompt because task output is untrusted. Read the result artifact if the logs are needed.",
        status = input.task_status,
        command = input.command,
        cwd = input.cwd.display(),
        exit_code = input.exit_code,
        signal = input.signal,
        stdout_bytes = input.stdout.bytes_seen,
        stdout_truncated = input.stdout.truncated,
        stderr_bytes = input.stderr.bytes_seen,
        stderr_truncated = input.stderr.truncated,
    )
}

fn truncate_string_bytes(value: &mut String, max_bytes: usize) {
    if value.len() <= max_bytes {
        return;
    }
    let mut end = max_bytes;
    while !value.is_char_boundary(end) {
        end = end.saturating_sub(1);
    }
    value.truncate(end);
    value.push_str("...");
}

#[allow(clippy::too_many_arguments)]
fn write_task_result_file(
    path: &Path,
    task_id: &str,
    job_id: &str,
    command: &str,
    cwd: &Path,
    task_status: &str,
    exit_code: Option<i64>,
    signal: Option<i64>,
    stdout: &TaskStreamCapture,
    stderr: &TaskStreamCapture,
) -> Result<()> {
    let mut file = create_private_file(path)?;
    writeln!(file, "task_id: {task_id}")?;
    writeln!(file, "job_id: {job_id}")?;
    writeln!(file, "command: {command}")?;
    writeln!(file, "cwd: {}", cwd.display())?;
    writeln!(file, "status: {task_status}")?;
    writeln!(file, "exit_code: {:?}", exit_code)?;
    writeln!(file, "signal: {:?}", signal)?;
    writeln!(file, "stdout_bytes: {}", stdout.bytes_seen)?;
    writeln!(file, "stdout_truncated: {}", stdout.truncated)?;
    writeln!(file, "stderr_bytes: {}", stderr.bytes_seen)?;
    writeln!(file, "stderr_truncated: {}", stderr.truncated)?;
    writeln!(file, "\n--- stdout tail ---")?;
    file.write_all(&stdout.tail)?;
    writeln!(file, "\n--- stderr tail ---")?;
    file.write_all(&stderr.tail)?;
    file.sync_all()?;
    if let Some(parent) = path.parent() {
        sync_dir(parent)?;
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn close_task_job_with_result(
    layout: &FsLayout,
    job_id: &str,
    result_path: &Path,
    task_status: &str,
    delivery_summary: &str,
    now: i64,
    max_delivery_attempts: i64,
    redelivery_window_seconds: i64,
) -> Result<()> {
    let artifact_id = new_id();
    let mut store = Store::open(layout)?;
    store.begin_artifact_ingest(job_id, &artifact_id, now)?;
    let ingested = ingest_result_file(layout, &artifact_id, job_id, result_path, now)?;
    let redelivery_window_seconds =
        clamp_redelivery_window_seconds_for_timestamp(now, redelivery_window_seconds);
    let result = if task_status == "succeeded" {
        store.complete_job(
            ingested.record,
            Some(delivery_summary.to_owned()),
            now,
            max_delivery_attempts,
            redelivery_window_seconds,
        )
    } else {
        store.fail_job_with_artifact(
            ingested.record,
            delivery_summary,
            now,
            max_delivery_attempts,
            redelivery_window_seconds,
        )
    };
    match result {
        Ok(_) => {
            remove_ingest_marker_best_effort(&ingested.artifact_dir);
            Ok(())
        }
        Err(error) => {
            if crate::fs_layout::remove_dir_all_durable(&ingested.artifact_dir).is_ok() {
                let _ = store.abandon_artifact_ingest(&artifact_id);
            }
            Err(error)
        }
    }
}

fn retry_task_store_completion<T>(
    operation: &str,
    mut action: impl FnMut() -> Result<T>,
) -> Result<T> {
    retry_task_store_completion_with_timeout(
        operation,
        TASK_STORE_COMPLETION_RETRY_TIMEOUT,
        &mut action,
    )
}

fn retry_task_store_setup<T>(operation: &str, mut action: impl FnMut() -> Result<T>) -> Result<T> {
    retry_task_store_completion_with_timeout(operation, TASK_STORE_SETUP_RETRY_TIMEOUT, &mut action)
}

fn retry_task_store_completion_with_timeout<T>(
    operation: &str,
    retry_timeout: Duration,
    mut action: impl FnMut() -> Result<T>,
) -> Result<T> {
    let started_at = Instant::now();
    let mut retry_delay = TASK_STORE_COMPLETION_RETRY_INITIAL_DELAY;
    loop {
        match action() {
            Ok(value) => return Ok(value),
            Err(error) if task_store_error_is_busy_or_locked(&error) => {
                let elapsed = started_at.elapsed();
                if elapsed >= retry_timeout {
                    return Err(error).with_context(|| format!("{operation} after retry timeout"));
                }
                let remaining = retry_timeout.saturating_sub(elapsed);
                thread::sleep(retry_delay.min(remaining));
                retry_delay = retry_delay
                    .saturating_mul(2)
                    .min(TASK_STORE_COMPLETION_RETRY_MAX_DELAY);
            }
            Err(error) => return Err(error).with_context(|| operation.to_owned()),
        }
    }
}

fn task_store_error_is_busy_or_locked(error: &anyhow::Error) -> bool {
    error.chain().any(|cause| {
        cause
            .downcast_ref::<rusqlite::Error>()
            .is_some_and(|sqlite_error| {
                matches!(
                    sqlite_error.sqlite_error_code(),
                    Some(ErrorCode::DatabaseBusy | ErrorCode::DatabaseLocked)
                )
            })
    })
}

fn clamp_redelivery_window_seconds_for_timestamp(now: i64, redelivery_window_seconds: i64) -> i64 {
    redelivery_window_seconds.min(i64::MAX.saturating_sub(now))
}

fn reserve_cli_app_server(
    state: &DaemonState,
    payload: CliAppServerReservePayload,
) -> Result<CliAppServerReservationInfo> {
    validate_daemon_nonempty("bound_thread_id", &payload.bound_thread_id)?;
    validate_daemon_nonempty("lease_id", &payload.lease_id)?;
    let lease_ttl = cli_app_server_lease_ttl(payload.lease_ttl_seconds)?;

    let mut dead_server_proofs = Vec::new();
    {
        let mut servers = state
            .cli_app_servers
            .lock()
            .map_err(|_| anyhow::anyhow!("CLI app-server registry lock is poisoned"))?;
        let mut dead_server_ids = Vec::new();
        for (managed_session_id, server) in servers.iter_mut() {
            if server.bound_thread_id != payload.bound_thread_id {
                continue;
            }
            if cli_app_server_child_is_running(server)? {
                bail!(
                    "thread {} already has an active CLI app-server",
                    payload.bound_thread_id
                );
            }
            dead_server_ids.push(managed_session_id.clone());
        }
        dead_server_proofs.extend(
            dead_server_ids
                .into_iter()
                .filter_map(|managed_session_id| servers.get(&managed_session_id))
                .map(cli_app_server_proof),
        );
    }
    for proof in dead_server_proofs {
        invalidate_and_stop_registered_cli_app_server(state, &proof)?;
    }

    let mut reservations = state
        .cli_app_server_reservations
        .lock()
        .map_err(|_| anyhow::anyhow!("CLI app-server reservation lock is poisoned"))?;
    let now = Instant::now();
    if reservations
        .get(&payload.bound_thread_id)
        .is_some_and(|reservation| reservation.lease_expires_at <= now)
    {
        reservations.remove(&payload.bound_thread_id);
    }
    if let Some(existing) = reservations.get_mut(&payload.bound_thread_id) {
        if existing.lease_id != payload.lease_id {
            bail!(
                "thread {} already has an active CLI app-server reservation",
                payload.bound_thread_id
            );
        }
        existing.lease_expires_at = now + lease_ttl;
        return Ok(cli_app_server_reservation_info(existing));
    }
    let reservation = CliAppServerReservation {
        bound_thread_id: payload.bound_thread_id.clone(),
        lease_id: payload.lease_id,
        lease_expires_at: now + lease_ttl,
    };
    let info = cli_app_server_reservation_info(&reservation);
    reservations.insert(payload.bound_thread_id, reservation);
    Ok(info)
}

fn ensure_cli_app_server(
    state: &DaemonState,
    payload: CliAppServerEnsurePayload,
) -> Result<CliAppServerInfo> {
    validate_daemon_nonempty("managed_session_id", &payload.managed_session_id)?;
    validate_daemon_nonempty("bound_thread_id", &payload.bound_thread_id)?;
    validate_daemon_positive("session_epoch", payload.session_epoch)?;
    validate_daemon_nonempty_bytes("codex_binary", &payload.codex_binary)?;
    validate_daemon_nonempty("lease_id", &payload.lease_id)?;
    let lease_ttl = cli_app_server_lease_ttl(payload.lease_ttl_seconds)?;
    ensure_daemon_not_stopping(state)?;
    let existing_dead_proof = {
        let mut servers = state
            .cli_app_servers
            .lock()
            .map_err(|_| anyhow::anyhow!("CLI app-server registry lock is poisoned"))?;
        ensure_daemon_not_stopping(state)?;
        if let Some(existing) = servers.get_mut(&payload.managed_session_id) {
            if existing.bound_thread_id != payload.bound_thread_id {
                bail!(
                    "managed session {} is already attached to app-server for thread {}",
                    payload.managed_session_id,
                    existing.bound_thread_id
                );
            }
            if cli_app_server_child_is_running(existing)? {
                if existing.session_epoch != payload.session_epoch {
                    bail!(
                        "managed session {} app-server is at epoch {}, not {}",
                        payload.managed_session_id,
                        existing.session_epoch,
                        payload.session_epoch
                    );
                }
                ensure_cli_app_server_lease_is_reentrant(existing, &payload.lease_id)?;
                existing.lease_id = payload.lease_id.clone();
                existing.lease_expires_at = Instant::now() + lease_ttl;
                return Ok(cli_app_server_info(existing));
            }
            Some(cli_app_server_proof(existing))
        } else {
            None
        }
    };
    if let Some(proof) = existing_dead_proof {
        invalidate_and_stop_registered_cli_app_server(state, &proof)?;
    }

    ensure_cli_app_server_reservation_matches(state, &payload.bound_thread_id, &payload.lease_id)?;
    ensure_daemon_not_stopping(state)?;

    let server = spawn_cli_app_server(SpawnCliAppServerOptions {
        managed_session_id: &payload.managed_session_id,
        bound_thread_id: &payload.bound_thread_id,
        session_epoch: payload.session_epoch,
        codex_binary: &payload.codex_binary,
        cwd: None,
        lease_id: &payload.lease_id,
        lease_ttl,
        stop_requested: &state.stop_requested,
    })?;
    let mut server = Some(server);
    loop {
        let mut servers = state
            .cli_app_servers
            .lock()
            .map_err(|_| anyhow::anyhow!("CLI app-server registry lock is poisoned"))?;
        if let Err(error) = ensure_daemon_not_stopping(state) {
            drop(servers);
            if let Some(server) = server.take() {
                stop_managed_cli_app_server_process(server);
            }
            return Err(error);
        }
        if let Some(existing) = servers.get_mut(&payload.managed_session_id) {
            if existing.bound_thread_id != payload.bound_thread_id {
                let attached_thread = existing.bound_thread_id.clone();
                drop(servers);
                if let Some(server) = server.take() {
                    stop_managed_cli_app_server_process(server);
                }
                bail!(
                    "managed session {} is already attached to app-server for thread {}",
                    payload.managed_session_id,
                    attached_thread
                );
            }
            match cli_app_server_child_is_running(existing) {
                Ok(true) => {
                    if existing.session_epoch != payload.session_epoch {
                        let existing_epoch = existing.session_epoch;
                        drop(servers);
                        if let Some(server) = server.take() {
                            stop_managed_cli_app_server_process(server);
                        }
                        bail!(
                            "managed session {} app-server is at epoch {}, not {}",
                            payload.managed_session_id,
                            existing_epoch,
                            payload.session_epoch
                        );
                    }
                    if let Err(error) =
                        ensure_cli_app_server_lease_is_reentrant(existing, &payload.lease_id)
                    {
                        drop(servers);
                        if let Some(server) = server.take() {
                            stop_managed_cli_app_server_process(server);
                        }
                        return Err(error);
                    }
                    existing.lease_id = payload.lease_id.clone();
                    existing.lease_expires_at = Instant::now() + lease_ttl;
                    let info = cli_app_server_info(existing);
                    drop(servers);
                    if let Some(server) = server.take() {
                        stop_managed_cli_app_server_process(server);
                    }
                    return Ok(info);
                }
                Ok(false) => {
                    let proof = cli_app_server_proof(existing);
                    drop(servers);
                    if let Err(error) = invalidate_and_stop_registered_cli_app_server(state, &proof)
                    {
                        if let Some(server) = server.take() {
                            stop_managed_cli_app_server_process(server);
                        }
                        return Err(error);
                    }
                    continue;
                }
                Err(error) => {
                    drop(servers);
                    if let Some(server) = server.take() {
                        stop_managed_cli_app_server_process(server);
                    }
                    return Err(error);
                }
            }
        }
        let server_to_insert = server.take().expect("candidate server is still available");
        let info = cli_app_server_info(&server_to_insert);
        servers.insert(payload.managed_session_id.clone(), server_to_insert);
        drop(servers);
        let _ = release_cli_app_server_reservation(
            state,
            CliAppServerReleasePayload {
                bound_thread_id: info.bound_thread_id.clone(),
                lease_id: payload.lease_id,
            },
        );
        return Ok(info);
    }
}

fn probe_cli_app_server(
    state: &DaemonState,
    payload: CliAppServerProbePayload,
) -> Result<CliAppServerInfo> {
    validate_daemon_nonempty_bytes("codex_binary", &payload.codex_binary)?;
    ensure_daemon_not_stopping(state)?;
    let probe_id = new_id();
    let managed_session_id = format!("doctor-probe-session-{probe_id}");
    let bound_thread_id = format!("doctor-probe-thread-{probe_id}");
    let lease_id = format!("doctor-probe-lease-{probe_id}");
    let server = spawn_cli_app_server(SpawnCliAppServerOptions {
        managed_session_id: &managed_session_id,
        bound_thread_id: &bound_thread_id,
        session_epoch: 1,
        codex_binary: &payload.codex_binary,
        cwd: None,
        lease_id: &lease_id,
        lease_ttl: Duration::from_secs(DEFAULT_CLI_APP_SERVER_LEASE_TTL_SECONDS),
        stop_requested: &state.stop_requested,
    })?;
    let info = cli_app_server_info(&server);
    stop_managed_cli_app_server_process(server);
    Ok(info)
}

fn refresh_cli_app_server_lease(
    state: &DaemonState,
    payload: CliAppServerLeasePayload,
) -> Result<CliAppServerInfo> {
    validate_daemon_nonempty("managed_session_id", &payload.managed_session_id)?;
    validate_daemon_nonempty("lease_id", &payload.lease_id)?;
    let lease_ttl = cli_app_server_lease_ttl(payload.lease_ttl_seconds)?;
    let mut servers = state
        .cli_app_servers
        .lock()
        .map_err(|_| anyhow::anyhow!("CLI app-server registry lock is poisoned"))?;
    let Some(server) = servers.get_mut(&payload.managed_session_id) else {
        bail!(
            "CLI app-server for managed session {} is not running",
            payload.managed_session_id
        );
    };
    ensure_cli_app_server_lease_matches(server, &payload.lease_id)?;
    if !cli_app_server_child_is_running(server)? {
        let proof = cli_app_server_proof(server);
        drop(servers);
        invalidate_and_stop_registered_cli_app_server(state, &proof)?;
        bail!(
            "CLI app-server for managed session {} has exited",
            payload.managed_session_id
        );
    }
    server.lease_expires_at = Instant::now() + lease_ttl;
    Ok(cli_app_server_info(server))
}

fn stop_cli_app_server(state: &DaemonState, payload: CliAppServerStopPayload) -> Result<bool> {
    validate_daemon_nonempty("managed_session_id", &payload.managed_session_id)?;
    validate_daemon_nonempty("lease_id", &payload.lease_id)?;
    let servers = state
        .cli_app_servers
        .lock()
        .map_err(|_| anyhow::anyhow!("CLI app-server registry lock is poisoned"))?;
    let Some(server) = servers.get(&payload.managed_session_id) else {
        return Ok(false);
    };
    ensure_cli_app_server_lease_matches(server, &payload.lease_id)?;
    let proof = cli_app_server_proof(server);
    drop(servers);
    invalidate_and_stop_registered_cli_app_server(state, &proof)
}

fn start_cli_thread(
    state: &DaemonState,
    payload: CliThreadStartPayload,
) -> Result<CliThreadStartInfo> {
    validate_daemon_nonempty_bytes("codex_binary", &payload.codex_binary)?;
    validate_daemon_nonempty("cwd", &payload.cwd)?;
    validate_daemon_nonempty("lease_id", &payload.lease_id)?;
    let lease_ttl = cli_app_server_lease_ttl(payload.lease_ttl_seconds)?;
    ensure_daemon_not_stopping(state)?;
    let bootstrap_id = new_id();
    let server = spawn_cli_app_server(SpawnCliAppServerOptions {
        managed_session_id: &bootstrap_id,
        bound_thread_id: CLI_THREAD_START_BOOTSTRAP_BOUND_THREAD_ID,
        session_epoch: 1,
        codex_binary: &payload.codex_binary,
        cwd: Some(&payload.cwd),
        lease_id: &payload.lease_id,
        lease_ttl,
        stop_requested: &state.stop_requested,
    })?;
    let mut server = Some(server);
    let result = (|| {
        {
            let mut bootstraps = state.cli_thread_start_bootstraps.lock().map_err(|_| {
                anyhow::anyhow!("CLI thread/start bootstrap registry lock is poisoned")
            })?;
            ensure_daemon_not_stopping(state)?;
            bootstraps.insert(
                bootstrap_id.clone(),
                server
                    .take()
                    .expect("bootstrap app-server is still available"),
            );
        }
        ensure_daemon_not_stopping(state)?;
        let url = {
            let bootstraps = state.cli_thread_start_bootstraps.lock().map_err(|_| {
                anyhow::anyhow!("CLI thread/start bootstrap registry lock is poisoned")
            })?;
            let server = bootstraps
                .get(&bootstrap_id)
                .ok_or_else(|| anyhow::anyhow!("CLI thread/start bootstrap stopped"))?;
            server.url.clone()
        };
        let mut client = AppServerJsonRpcClient::connect(&url, CLI_APP_SERVER_STARTUP_TIMEOUT)
            .context("connect bootstrap cli app-server")?;
        client
            .initialize(
                env!("CARGO_PKG_VERSION"),
                CLI_THREAD_START_BOOTSTRAP_REQUEST_TIMEOUT,
            )
            .context("initialize bootstrap cli app-server")?;
        client
            .notify_initialized()
            .context("notify bootstrap cli app-server initialized")?;
        let thread_start_params = cli_thread_start_params(&mut client, &payload)?;
        let started = client
            .request(
                "thread/start",
                thread_start_params,
                CLI_THREAD_START_BOOTSTRAP_REQUEST_TIMEOUT,
            )
            .context("bootstrap cli thread/start")?;
        let thread_id = parse_cli_thread_start_id(&started)?;
        let mut bootstraps = state
            .cli_thread_start_bootstraps
            .lock()
            .map_err(|_| anyhow::anyhow!("CLI thread/start bootstrap registry lock is poisoned"))?;
        ensure_daemon_not_stopping(state)?;
        let server = bootstraps
            .get_mut(&bootstrap_id)
            .ok_or_else(|| anyhow::anyhow!("CLI thread/start bootstrap stopped"))?;
        server.bound_thread_id = thread_id.clone();
        Ok(CliThreadStartInfo {
            bootstrap_id: bootstrap_id.clone(),
            thread_id,
        })
    })();
    if result.is_err() {
        if let Some(server) = server.take() {
            stop_managed_cli_app_server_process(server);
        } else {
            let server = state
                .cli_thread_start_bootstraps
                .lock()
                .ok()
                .and_then(|mut bootstraps| bootstraps.remove(&bootstrap_id));
            if let Some(server) = server {
                stop_managed_cli_app_server_process(server);
            }
        }
    }
    result
}

fn start_cli_foreground_thread(
    state: &DaemonState,
    payload: CliForegroundThreadStartPayload,
) -> Result<CliForegroundThreadStartInfo> {
    validate_daemon_nonempty_bytes("codex_binary", &payload.codex_binary)?;
    validate_daemon_nonempty("cwd", &payload.cwd)?;
    validate_daemon_nonempty("lease_id", &payload.lease_id)?;
    let lease_ttl = cli_app_server_lease_ttl(payload.lease_ttl_seconds)?;
    ensure_daemon_not_stopping(state)?;
    let bootstrap_id = new_id();
    let server = spawn_cli_app_server(SpawnCliAppServerOptions {
        managed_session_id: &bootstrap_id,
        bound_thread_id: CLI_FOREGROUND_THREAD_BOOTSTRAP_BOUND_THREAD_ID,
        session_epoch: 1,
        codex_binary: &payload.codex_binary,
        cwd: Some(&payload.cwd),
        lease_id: &payload.lease_id,
        lease_ttl,
        stop_requested: &state.stop_requested,
    })?;
    let mut server = Some(server);
    let result = (|| {
        let url = server
            .as_ref()
            .expect("bootstrap app-server is still available")
            .url
            .clone();
        {
            let mut bootstraps = state.cli_thread_start_bootstraps.lock().map_err(|_| {
                anyhow::anyhow!("CLI thread/start bootstrap registry lock is poisoned")
            })?;
            ensure_daemon_not_stopping(state)?;
            bootstraps.insert(
                bootstrap_id.clone(),
                server
                    .take()
                    .expect("bootstrap app-server is still available"),
            );
        }
        Ok(CliForegroundThreadStartInfo { bootstrap_id, url })
    })();
    if result.is_err()
        && let Some(server) = server.take()
    {
        stop_managed_cli_app_server_process(server);
    }
    result
}

fn cli_thread_start_params(
    client: &mut AppServerJsonRpcClient,
    payload: &CliThreadStartPayload,
) -> Result<Value> {
    let mut params = match payload
        .thread_start_params
        .clone()
        .unwrap_or_else(|| json!({ "cwd": payload.cwd.clone() }))
    {
        Value::Object(params) => params,
        _ => bail!("thread_start_params must be an object"),
    };
    let cwd = match params.get("cwd").and_then(Value::as_str) {
        Some(cwd) if !cwd.is_empty() => cwd.to_owned(),
        _ => {
            params.insert("cwd".to_owned(), Value::String(payload.cwd.clone()));
            payload.cwd.clone()
        }
    };

    // The app-server's fresh thread defaults can lag behind the foreground CLI
    // defaults. Echo only the stable effective config values that affect the
    // model picker display, unless the caller supplied an explicit override.
    // config/read exposes the raw config shape, not the profile-resolved final
    // Config, so leave active-profile cases to thread/start's normal config
    // resolution path. The fresh bootstrap app-server is launched in this cwd
    // so config/read still matches the target project if that method ignores
    // the request cwd field.
    if !thread_start_has_profile_override(&params)?
        && let Some(config) = read_effective_codex_config(client, &cwd)?
        && !effective_config_has_active_profile(&config)
    {
        merge_effective_config_into_thread_start_params(&mut params, &config)?;
    }

    Ok(Value::Object(params))
}

fn read_effective_codex_config(
    client: &mut AppServerJsonRpcClient,
    cwd: &str,
) -> Result<Option<Map<String, Value>>> {
    let response = match client.request(
        "config/read",
        json!({
            "cwd": cwd,
            "includeLayers": false,
        }),
        CLI_THREAD_START_BOOTSTRAP_REQUEST_TIMEOUT,
    ) {
        Ok(response) => response,
        Err(_) => return Ok(None),
    };
    // Current ConfigReadResponse exposes effective values under top-level
    // `config`; accept a flat result as a protocol-drift fallback.
    Ok(response
        .get("config")
        .and_then(Value::as_object)
        .or_else(|| response.as_object())
        .cloned())
}

fn merge_effective_config_into_thread_start_params(
    params: &mut Map<String, Value>,
    config: &Map<String, Value>,
) -> Result<()> {
    let provider_matches_config = thread_start_provider_matches_config(params, config);
    if provider_matches_config
        && !params.contains_key("model")
        && let Some(model) = config.get("model").filter(|value| !value.is_null())
    {
        params.insert("model".to_owned(), model.clone());
    }
    if !params.contains_key("modelProvider")
        && let Some(provider) = config
            .get("model_provider")
            .filter(|value| !value.is_null())
    {
        params.insert("modelProvider".to_owned(), provider.clone());
    }

    let mut config_overrides = Map::new();
    for key in ["model_reasoning_effort", "model_reasoning_summary"] {
        if provider_matches_config
            && !thread_start_config_contains(params, key)?
            && let Some(value) = config.get(key).filter(|value| !value.is_null())
        {
            config_overrides.insert(key.to_owned(), value.clone());
        }
    }
    if !config_overrides.is_empty() {
        let thread_config = params
            .entry("config".to_owned())
            .or_insert_with(|| Value::Object(Map::new()));
        let Some(thread_config) = thread_config.as_object_mut() else {
            bail!("thread_start_params config must be an object");
        };
        thread_config.extend(config_overrides);
    }

    Ok(())
}

fn thread_start_provider_matches_config(
    params: &Map<String, Value>,
    config: &Map<String, Value>,
) -> bool {
    let Some(provider) = params.get("modelProvider").filter(|value| !value.is_null()) else {
        return true;
    };
    config
        .get("model_provider")
        .filter(|value| !value.is_null())
        .is_some_and(|config_provider| config_provider == provider)
}

fn thread_start_config_contains(params: &Map<String, Value>, key: &str) -> Result<bool> {
    let Some(config) = params.get("config") else {
        return Ok(false);
    };
    let Some(config) = config.as_object() else {
        bail!("thread_start_params config must be an object");
    };
    Ok(config.contains_key(key))
}

fn thread_start_has_profile_override(params: &Map<String, Value>) -> Result<bool> {
    let Some(config) = params.get("config") else {
        return Ok(false);
    };
    let Some(config) = config.as_object() else {
        bail!("thread_start_params config must be an object");
    };
    Ok(config
        .get("profile")
        .or_else(|| config.get("config_profile"))
        .is_some_and(|value| !value.is_null()))
}

fn effective_config_has_active_profile(config: &Map<String, Value>) -> bool {
    config.get("profile").is_some_and(|value| !value.is_null())
}

fn promote_cli_thread_start_app_server(
    state: &DaemonState,
    payload: CliThreadStartPromotePayload,
) -> Result<CliAppServerInfo> {
    validate_daemon_nonempty("bootstrap_id", &payload.bootstrap_id)?;
    validate_daemon_nonempty("managed_session_id", &payload.managed_session_id)?;
    validate_daemon_nonempty("bound_thread_id", &payload.bound_thread_id)?;
    validate_daemon_positive("session_epoch", payload.session_epoch)?;
    validate_daemon_nonempty("lease_id", &payload.lease_id)?;
    let lease_ttl = cli_app_server_lease_ttl(payload.lease_ttl_seconds)?;
    ensure_daemon_not_stopping(state)?;
    ensure_cli_app_server_reservation_matches(state, &payload.bound_thread_id, &payload.lease_id)?;

    let server = {
        let mut bootstraps = state
            .cli_thread_start_bootstraps
            .lock()
            .map_err(|_| anyhow::anyhow!("CLI thread/start bootstrap registry lock is poisoned"))?;
        bootstraps
            .remove(&payload.bootstrap_id)
            .ok_or_else(|| anyhow::anyhow!("CLI thread/start bootstrap is not running"))?
    };
    let mut server = Some(server);
    let result = (|| {
        let server_ref = server
            .as_mut()
            .expect("bootstrap app-server is still available");
        ensure_daemon_not_stopping(state)?;
        if server_ref.bound_thread_id == CLI_FOREGROUND_THREAD_BOOTSTRAP_BOUND_THREAD_ID {
            server_ref.bound_thread_id = payload.bound_thread_id.clone();
        } else if server_ref.bound_thread_id != payload.bound_thread_id {
            let started_thread = server_ref.bound_thread_id.clone();
            bail!(
                "CLI thread/start bootstrap created thread {}, not {}",
                started_thread,
                payload.bound_thread_id
            );
        }
        if !cli_app_server_child_is_running(server_ref)? {
            bail!("CLI thread/start bootstrap app-server has exited");
        }
        server_ref.managed_session_id = payload.managed_session_id.clone();
        server_ref.session_epoch = payload.session_epoch;
        server_ref.lease_id = payload.lease_id.clone();
        server_ref.lease_expires_at = Instant::now() + lease_ttl;

        loop {
            let mut servers = state
                .cli_app_servers
                .lock()
                .map_err(|_| anyhow::anyhow!("CLI app-server registry lock is poisoned"))?;
            ensure_daemon_not_stopping(state)?;
            if let Some(existing) = servers.get_mut(&payload.managed_session_id) {
                if existing.bound_thread_id != payload.bound_thread_id {
                    let attached_thread = existing.bound_thread_id.clone();
                    drop(servers);
                    if let Some(server) = server.take() {
                        stop_managed_cli_app_server_process(server);
                    }
                    bail!(
                        "managed session {} is already attached to app-server for thread {}",
                        payload.managed_session_id,
                        attached_thread
                    );
                }
                match cli_app_server_child_is_running(existing) {
                    Ok(true) => {
                        drop(servers);
                        if let Some(server) = server.take() {
                            stop_managed_cli_app_server_process(server);
                        }
                        bail!(
                            "managed session {} already has a registered CLI app-server",
                            payload.managed_session_id
                        );
                    }
                    Ok(false) => {
                        let proof = cli_app_server_proof(existing);
                        drop(servers);
                        invalidate_and_stop_registered_cli_app_server(state, &proof)?;
                        continue;
                    }
                    Err(error) => {
                        drop(servers);
                        if let Some(server) = server.take() {
                            stop_managed_cli_app_server_process(server);
                        }
                        return Err(error);
                    }
                }
            }
            let server_to_insert = server
                .take()
                .expect("bootstrap app-server is still available");
            let info = cli_app_server_info(&server_to_insert);
            servers.insert(payload.managed_session_id.clone(), server_to_insert);
            drop(servers);
            let _ = release_cli_app_server_reservation(
                state,
                CliAppServerReleasePayload {
                    bound_thread_id: payload.bound_thread_id,
                    lease_id: payload.lease_id,
                },
            );
            return Ok(info);
        }
    })();
    if let Some(server) = server.take() {
        stop_managed_cli_app_server_process(server);
    }
    result
}

fn abort_cli_thread_start_app_server(
    state: &DaemonState,
    payload: CliThreadStartAbortPayload,
) -> Result<bool> {
    validate_daemon_nonempty("bootstrap_id", &payload.bootstrap_id)?;
    validate_daemon_nonempty("lease_id", &payload.lease_id)?;
    let server = {
        let mut bootstraps = state
            .cli_thread_start_bootstraps
            .lock()
            .map_err(|_| anyhow::anyhow!("CLI thread/start bootstrap registry lock is poisoned"))?;
        let Some(server) = bootstraps.get(&payload.bootstrap_id) else {
            return Ok(false);
        };
        ensure_cli_app_server_lease_matches(server, &payload.lease_id)?;
        bootstraps
            .remove(&payload.bootstrap_id)
            .expect("bootstrap exists")
    };
    stop_managed_cli_app_server_process(server);
    Ok(true)
}

fn parse_cli_thread_start_id(value: &Value) -> Result<String> {
    let thread = value
        .get("thread")
        .ok_or_else(|| anyhow::anyhow!("thread/start response missing thread"))?;
    let thread_id = thread
        .get("id")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow::anyhow!("thread/start response missing thread.id"))?;
    validate_daemon_nonempty("thread.id", thread_id)?;
    Ok(thread_id.to_owned())
}

struct SpawnCliAppServerOptions<'a> {
    managed_session_id: &'a str,
    bound_thread_id: &'a str,
    session_epoch: i64,
    codex_binary: &'a [u8],
    cwd: Option<&'a str>,
    lease_id: &'a str,
    lease_ttl: Duration,
    stop_requested: &'a AtomicBool,
}

fn spawn_cli_app_server(options: SpawnCliAppServerOptions<'_>) -> Result<ManagedCliAppServer> {
    let codex_binary = OsString::from_vec(options.codex_binary.to_vec());
    let mut command = Command::new(&codex_binary);
    command
        .arg("app-server")
        .arg("--listen")
        .arg("ws://127.0.0.1:0")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .process_group(0);
    if let Some(cwd) = options.cwd {
        command.current_dir(cwd);
    }
    let mut child = spawn_command_locked(&mut command)
        .with_context(|| format!("spawn codex app-server via {:?}", codex_binary))?;
    let stdout = child.stdout.take().context("capture app-server stdout")?;
    let stderr = child.stderr.take().context("capture app-server stderr")?;
    if let Err(error) = set_fd_nonblocking(stdout.as_raw_fd(), true) {
        stop_cli_app_server_process(&mut child);
        return Err(error).context("set app-server stdout nonblocking");
    }
    if let Err(error) = set_fd_nonblocking(stderr.as_raw_fd(), true) {
        stop_cli_app_server_process(&mut child);
        return Err(error).context("set app-server stderr nonblocking");
    }
    let (url_sender, url_receiver) = mpsc::channel();
    let drain_running = Arc::new(AtomicBool::new(true));
    let stdout_drain_running = Arc::clone(&drain_running);
    let stderr_drain_running = Arc::clone(&drain_running);
    let stderr_url_sender = url_sender.clone();
    let stdout_worker = thread::spawn(move || {
        drain_app_server_listener_stream(stdout, url_sender, stdout_drain_running)
    });
    let stderr_worker = thread::spawn(move || {
        drain_app_server_listener_stream(stderr, stderr_url_sender, stderr_drain_running)
    });
    let startup_deadline = Instant::now() + CLI_APP_SERVER_STARTUP_TIMEOUT;
    let url = loop {
        if options.stop_requested.load(Ordering::Acquire) {
            drain_running.store(false, Ordering::Release);
            stop_cli_app_server_process(&mut child);
            join_worker(stdout_worker);
            join_worker(stderr_worker);
            bail!("daemon is stopping");
        }
        let now = Instant::now();
        if now >= startup_deadline {
            drain_running.store(false, Ordering::Release);
            stop_cli_app_server_process(&mut child);
            join_worker(stdout_worker);
            join_worker(stderr_worker);
            bail!("codex app-server did not report a websocket listener: timed out");
        }
        let wait = READ_POLL_INTERVAL.min(startup_deadline.saturating_duration_since(now));
        match url_receiver.recv_timeout(wait) {
            Ok(url) => break url,
            Err(mpsc::RecvTimeoutError::Timeout) => {}
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                drain_running.store(false, Ordering::Release);
                stop_cli_app_server_process(&mut child);
                join_worker(stdout_worker);
                join_worker(stderr_worker);
                bail!("codex app-server did not report a websocket listener: output closed");
            }
        }
    };
    if !app_server_listener_url_is_loopback(&url) {
        drain_running.store(false, Ordering::Release);
        stop_cli_app_server_process(&mut child);
        join_worker(stdout_worker);
        join_worker(stderr_worker);
        bail!("codex app-server reported non-loopback listener {url}");
    }
    let started_at = current_epoch_seconds()?;
    Ok(ManagedCliAppServer {
        managed_session_id: options.managed_session_id.to_owned(),
        bound_thread_id: options.bound_thread_id.to_owned(),
        session_epoch: options.session_epoch,
        url,
        child,
        started_at,
        lease_id: options.lease_id.to_owned(),
        lease_expires_at: Instant::now() + options.lease_ttl,
        drain_running,
        stdout_worker: Some(stdout_worker),
        stderr_worker: Some(stderr_worker),
    })
}

fn drain_app_server_listener_stream<R: Read>(
    mut reader: R,
    url_sender: mpsc::Sender<String>,
    running: Arc<AtomicBool>,
) {
    let mut sent = false;
    let mut scan_buffer = Vec::with_capacity(CLI_APP_SERVER_LISTENER_SCAN_BYTES);
    let mut buffer = [0_u8; CLI_APP_SERVER_DRAIN_CHUNK_BYTES];
    loop {
        if !running.load(Ordering::Acquire) {
            break;
        }
        match reader.read(&mut buffer) {
            Ok(0) => break,
            Ok(read) => {
                if !sent {
                    append_bounded(
                        &mut scan_buffer,
                        &buffer[..read],
                        CLI_APP_SERVER_LISTENER_SCAN_BYTES,
                    );
                    if let Some(url) = parse_app_server_listener_url(&scan_buffer) {
                        let _ = url_sender.send(url);
                        sent = true;
                        scan_buffer.clear();
                    }
                }
            }
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                if !running.load(Ordering::Acquire) {
                    break;
                }
                thread::sleep(CLI_APP_SERVER_DRAIN_POLL_INTERVAL);
            }
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(_) => break,
        }
    }
}

#[cfg(test)]
fn drain_reader<R: Read>(mut reader: R, running: Arc<AtomicBool>) {
    let mut buffer = [0_u8; CLI_APP_SERVER_DRAIN_CHUNK_BYTES];
    loop {
        if !running.load(Ordering::Acquire) {
            break;
        }
        match reader.read(&mut buffer) {
            Ok(0) => break,
            Ok(_) => {}
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                if !running.load(Ordering::Acquire) {
                    break;
                }
                thread::sleep(CLI_APP_SERVER_DRAIN_POLL_INTERVAL);
            }
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(_) => break,
        }
    }
}

fn append_bounded(buffer: &mut Vec<u8>, bytes: &[u8], limit: usize) {
    if bytes.len() >= limit {
        buffer.clear();
        buffer.extend_from_slice(&bytes[bytes.len() - limit..]);
        return;
    }
    let overflow = buffer
        .len()
        .saturating_add(bytes.len())
        .saturating_sub(limit);
    if overflow > 0 {
        buffer.drain(..overflow);
    }
    buffer.extend_from_slice(bytes);
}

fn parse_app_server_listener_url(buffer: &[u8]) -> Option<String> {
    let text = String::from_utf8_lossy(buffer);
    text.split_inclusive('\n')
        .filter(|line| line.ends_with('\n'))
        .filter_map(|line| {
            let value = line.trim_start().strip_prefix("listening on:")?;
            let url = value.split_whitespace().next()?;
            url.starts_with("ws://").then(|| url.to_owned())
        })
        .next_back()
}

fn app_server_listener_url_is_loopback(url: &str) -> bool {
    let Some(rest) = url.strip_prefix("ws://") else {
        return false;
    };
    let authority = rest
        .split(['/', '?', '#'])
        .next()
        .expect("split always yields first segment");
    let Some((host, port)) = split_url_authority_host_port(authority) else {
        return false;
    };
    port.parse::<u16>().is_ok_and(|port| port > 0) && host_is_loopback(host)
}

fn split_url_authority_host_port(authority: &str) -> Option<(&str, &str)> {
    if authority.is_empty() || authority.contains('@') {
        return None;
    }
    if let Some(rest) = authority.strip_prefix('[') {
        let (host, rest) = rest.split_once(']')?;
        let port = rest.strip_prefix(':')?;
        return Some((host, port));
    }
    let (host, port) = authority.rsplit_once(':')?;
    if host.is_empty() || host.contains(':') {
        return None;
    }
    Some((host, port))
}

fn host_is_loopback(host: &str) -> bool {
    host.eq_ignore_ascii_case("localhost")
        || host
            .parse::<IpAddr>()
            .is_ok_and(|address| address.is_loopback())
}

fn cli_app_server_lease_ttl(value: Option<u64>) -> Result<Duration> {
    let seconds = value.unwrap_or(DEFAULT_CLI_APP_SERVER_LEASE_TTL_SECONDS);
    if seconds == 0 {
        bail!("lease_ttl_seconds must be positive");
    }
    Ok(Duration::from_secs(seconds))
}

fn ensure_cli_app_server_reservation_matches(
    state: &DaemonState,
    bound_thread_id: &str,
    lease_id: &str,
) -> Result<()> {
    let mut reservations = state
        .cli_app_server_reservations
        .lock()
        .map_err(|_| anyhow::anyhow!("CLI app-server reservation lock is poisoned"))?;
    if reservations
        .get(bound_thread_id)
        .is_some_and(|reservation| reservation.lease_expires_at <= Instant::now())
    {
        reservations.remove(bound_thread_id);
    }
    let Some(reservation) = reservations.get(bound_thread_id) else {
        bail!("thread {bound_thread_id} does not have an active CLI app-server reservation");
    };
    if reservation.lease_id == lease_id {
        Ok(())
    } else {
        bail!("thread {bound_thread_id} has a different active CLI app-server reservation")
    }
}

fn release_cli_app_server_reservation(
    state: &DaemonState,
    payload: CliAppServerReleasePayload,
) -> Result<bool> {
    validate_daemon_nonempty("bound_thread_id", &payload.bound_thread_id)?;
    validate_daemon_nonempty("lease_id", &payload.lease_id)?;
    let mut reservations = state
        .cli_app_server_reservations
        .lock()
        .map_err(|_| anyhow::anyhow!("CLI app-server reservation lock is poisoned"))?;
    let Some(reservation) = reservations.get(&payload.bound_thread_id) else {
        return Ok(false);
    };
    if reservation.lease_id != payload.lease_id {
        bail!(
            "thread {} has a different active CLI app-server reservation",
            payload.bound_thread_id
        );
    }
    reservations.remove(&payload.bound_thread_id);
    Ok(true)
}

fn cli_app_server_reservation_info(
    reservation: &CliAppServerReservation,
) -> CliAppServerReservationInfo {
    CliAppServerReservationInfo {
        bound_thread_id: reservation.bound_thread_id.clone(),
        lease_seconds_remaining: reservation
            .lease_expires_at
            .saturating_duration_since(Instant::now())
            .as_secs(),
    }
}

fn ensure_cli_app_server_lease_matches(server: &ManagedCliAppServer, lease_id: &str) -> Result<()> {
    if server.lease_id == lease_id {
        Ok(())
    } else {
        bail!(
            "CLI app-server for managed session {} is owned by a different lease",
            server.managed_session_id
        )
    }
}

fn ensure_cli_app_server_lease_is_reentrant(
    server: &ManagedCliAppServer,
    lease_id: &str,
) -> Result<()> {
    if server.lease_id == lease_id {
        Ok(())
    } else {
        bail!(
            "managed session {} already has an active CLI app-server lease",
            server.managed_session_id
        )
    }
}

fn cli_app_server_child_is_running(server: &mut ManagedCliAppServer) -> Result<bool> {
    match child_status_without_reaping(server.child.id()).context("check codex app-server")? {
        ChildStatusWithoutReaping::Running => Ok(true),
        ChildStatusWithoutReaping::Exited | ChildStatusWithoutReaping::NotWaitable => Ok(false),
    }
}

fn cli_app_server_info(server: &ManagedCliAppServer) -> CliAppServerInfo {
    CliAppServerInfo {
        managed_session_id: server.managed_session_id.clone(),
        bound_thread_id: server.bound_thread_id.clone(),
        url: server.url.clone(),
        pid: server.child.id(),
        started_at: server.started_at,
        lease_seconds_remaining: server
            .lease_expires_at
            .saturating_duration_since(Instant::now())
            .as_secs(),
    }
}

fn cli_app_server_proof(server: &ManagedCliAppServer) -> ManagedCliAppServerProof {
    ManagedCliAppServerProof {
        managed_session_id: server.managed_session_id.clone(),
        bound_thread_id: server.bound_thread_id.clone(),
        session_epoch: server.session_epoch,
        lease_id: server.lease_id.clone(),
        child_pid: server.child.id(),
    }
}

fn cli_app_server_matches_proof(
    server: &ManagedCliAppServer,
    proof: &ManagedCliAppServerProof,
) -> bool {
    server.managed_session_id == proof.managed_session_id
        && server.bound_thread_id == proof.bound_thread_id
        && server.session_epoch == proof.session_epoch
        && server.lease_id == proof.lease_id
        && server.child.id() == proof.child_pid
}

fn cli_app_server_infos(state: &DaemonState) -> Vec<CliAppServerInfo> {
    let Ok(servers) = state.cli_app_servers.lock() else {
        return Vec::new();
    };
    servers.values().map(cli_app_server_info).collect()
}

fn has_active_cli_app_servers(state: &DaemonState) -> bool {
    state
        .cli_app_servers
        .lock()
        .is_ok_and(|servers| !servers.is_empty())
}

fn has_active_cli_app_server_reservations(state: &DaemonState) -> bool {
    state
        .cli_app_server_reservations
        .lock()
        .is_ok_and(|reservations| !reservations.is_empty())
}

fn has_active_cli_thread_start_bootstraps(state: &DaemonState) -> bool {
    state
        .cli_thread_start_bootstraps
        .lock()
        .is_ok_and(|bootstraps| !bootstraps.is_empty())
}

fn has_active_supervised_tasks(state: &DaemonState) -> bool {
    state
        .supervised_tasks
        .lock()
        .is_ok_and(|tasks| !tasks.is_empty())
}

fn reap_expired_cli_app_servers(state: &DaemonState) {
    let Ok(servers) = state.cli_app_servers.lock() else {
        return;
    };
    let now = Instant::now();
    let expired_proofs = servers
        .values()
        .filter_map(|server| {
            if server.lease_expires_at <= now {
                Some(cli_app_server_proof(server))
            } else {
                None
            }
        })
        .collect::<Vec<_>>();
    drop(servers);
    for proof in expired_proofs {
        let _ = invalidate_and_stop_registered_cli_app_server(state, &proof);
    }
}

fn reap_expired_cli_app_server_reservations(state: &DaemonState) {
    let Ok(mut reservations) = state.cli_app_server_reservations.lock() else {
        return;
    };
    let now = Instant::now();
    reservations.retain(|_, reservation| reservation.lease_expires_at > now);
}

fn reap_expired_cli_thread_start_bootstraps(state: &DaemonState) {
    let Ok(mut bootstraps) = state.cli_thread_start_bootstraps.lock() else {
        return;
    };
    let now = Instant::now();
    let expired = bootstraps
        .iter()
        .filter_map(|(id, server)| {
            if server.lease_expires_at <= now {
                Some(id.clone())
            } else {
                None
            }
        })
        .collect::<Vec<_>>();
    let expired_servers = expired
        .into_iter()
        .filter_map(|id| bootstraps.remove(&id))
        .collect::<Vec<_>>();
    drop(bootstraps);
    for server in expired_servers {
        stop_managed_cli_app_server_process(server);
    }
}

fn stop_all_cli_app_servers(state: &DaemonState) {
    let Ok(mut servers) = state.cli_app_servers.lock() else {
        return;
    };
    let drained_servers = servers
        .drain()
        .map(|(_, server)| server)
        .collect::<Vec<_>>();
    drop(servers);
    for server in drained_servers {
        let _ = invalidate_cli_app_server_proof(&state.layout, &cli_app_server_proof(&server));
        stop_managed_cli_app_server_process(server);
    }
}

fn stop_all_cli_thread_start_bootstraps(state: &DaemonState) {
    let Ok(mut bootstraps) = state.cli_thread_start_bootstraps.lock() else {
        return;
    };
    let drained_servers = bootstraps
        .drain()
        .map(|(_, server)| server)
        .collect::<Vec<_>>();
    drop(bootstraps);
    for server in drained_servers {
        stop_managed_cli_app_server_process(server);
    }
}

fn clear_cli_app_server_reservations(state: &DaemonState) {
    if let Ok(mut reservations) = state.cli_app_server_reservations.lock() {
        reservations.clear();
    }
}

fn stop_managed_cli_app_server_process(mut server: ManagedCliAppServer) {
    server.drain_running.store(false, Ordering::Release);
    stop_cli_app_server_process(&mut server.child);
    if let Some(worker) = server.stdout_worker.take() {
        join_worker(worker);
    }
    if let Some(worker) = server.stderr_worker.take() {
        join_worker(worker);
    }
}

fn invalidate_and_stop_registered_cli_app_server(
    state: &DaemonState,
    proof: &ManagedCliAppServerProof,
) -> Result<bool> {
    let mut servers = state
        .cli_app_servers
        .lock()
        .map_err(|_| anyhow::anyhow!("CLI app-server registry lock is poisoned"))?;
    let Some(server) = servers.get(&proof.managed_session_id) else {
        return Ok(false);
    };
    if !cli_app_server_matches_proof(server, proof) {
        return Ok(false);
    }
    invalidate_cli_app_server_proof(&state.layout, proof)?;
    let server = servers
        .remove(&proof.managed_session_id)
        .expect("server exists");
    drop(servers);
    stop_managed_cli_app_server_process(server);
    Ok(true)
}

fn invalidate_cli_app_server_proof(
    layout: &FsLayout,
    proof: &ManagedCliAppServerProof,
) -> Result<()> {
    let mut store = Store::open_for_daemon_lifecycle(layout)?;
    let invalidation = store.invalidate_cli_managed_session_current_proof(
        &proof.managed_session_id,
        &proof.bound_thread_id,
        current_epoch_seconds()?,
    )?;
    if invalidation.session.bound_thread_id != proof.bound_thread_id {
        bail!("CLI app-server proof invalidation returned a different bound thread");
    }
    Ok(())
}

fn stop_cli_app_server_process(child: &mut Child) {
    let pid = child.id();
    if child_status_without_reaping(pid)
        .is_ok_and(|status| status == ChildStatusWithoutReaping::NotWaitable)
    {
        let _ = wait_child_until(child, Instant::now() + CLI_APP_SERVER_KILL_GRACE);
        return;
    }
    signal_process_group(pid, libc::SIGTERM);
    let status_after_term =
        wait_child_observed_until(pid, Instant::now() + CLI_APP_SERVER_TERM_GRACE);
    if status_after_term != ChildStatusWithoutReaping::NotWaitable {
        signal_process_group(pid, libc::SIGKILL);
        let _ = child.kill();
    }
    let child_exited = wait_child_until(child, Instant::now() + CLI_APP_SERVER_KILL_GRACE);
    if !child_exited {
        let _ = child.kill();
        let _ = wait_child_until(child, Instant::now() + CLI_APP_SERVER_KILL_GRACE);
    }
}

fn wait_child_observed_until(pid: u32, deadline: Instant) -> ChildStatusWithoutReaping {
    loop {
        match child_status_without_reaping(pid) {
            Ok(ChildStatusWithoutReaping::Running) => {}
            Ok(status) => return status,
            Err(_) => return ChildStatusWithoutReaping::NotWaitable,
        }
        if Instant::now() >= deadline {
            return ChildStatusWithoutReaping::Running;
        }
        thread::sleep(READ_POLL_INTERVAL);
    }
}

fn child_status_without_reaping(pid: u32) -> io::Result<ChildStatusWithoutReaping> {
    let mut info = mem::MaybeUninit::<libc::siginfo_t>::zeroed();
    let result = unsafe {
        libc::waitid(
            libc::P_PID,
            pid as libc::id_t,
            info.as_mut_ptr(),
            libc::WEXITED | libc::WNOHANG | libc::WNOWAIT,
        )
    };
    if result != 0 {
        let error = io::Error::last_os_error();
        if error.raw_os_error() == Some(libc::ECHILD) {
            return Ok(ChildStatusWithoutReaping::NotWaitable);
        }
        return Err(error);
    }
    let info = unsafe { info.assume_init() };
    if unsafe { info.si_pid() } == 0 {
        Ok(ChildStatusWithoutReaping::Running)
    } else {
        Ok(ChildStatusWithoutReaping::Exited)
    }
}

fn signal_process_group(pid: u32, signal: libc::c_int) -> bool {
    let pgid = pid as libc::pid_t;
    (unsafe { libc::killpg(pgid, signal) }) == 0
}

fn process_group_exists(pid: u32) -> bool {
    let pgid = pid as libc::pid_t;
    if unsafe { libc::killpg(pgid, 0) } == 0 {
        return true;
    }
    !matches!(io::Error::last_os_error().raw_os_error(), Some(libc::ESRCH))
}

fn process_group_has_live_members_after_leader_exit(leader_pid: u32) -> bool {
    match process_group_has_live_members_except_leader(leader_pid) {
        Ok(has_live_members) => has_live_members,
        Err(_) => process_group_exists(leader_pid),
    }
}

fn process_group_has_enumerated_live_members_after_leader_exit(leader_pid: u32) -> bool {
    process_group_has_live_members_except_leader(leader_pid).unwrap_or(false)
}

#[cfg(target_os = "linux")]
fn process_group_has_live_members_except_leader(leader_pid: u32) -> Result<bool> {
    let pgid = leader_pid;
    for entry in fs::read_dir("/proc").context("read /proc")? {
        let entry = match entry {
            Ok(entry) => entry,
            Err(_) => continue,
        };
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        let Ok(member_pid) = name.parse::<u32>() else {
            continue;
        };
        if member_pid == leader_pid {
            continue;
        }
        let stat = match fs::read_to_string(entry.path().join("stat")) {
            Ok(stat) => stat,
            Err(_) => continue,
        };
        let Some((state, member_pgid)) = linux_stat_state_and_pgrp(&stat) else {
            continue;
        };
        if member_pgid == pgid && state != 'Z' {
            return Ok(true);
        }
    }
    Ok(false)
}

#[cfg(target_os = "linux")]
fn linux_stat_state_and_pgrp(stat: &str) -> Option<(char, u32)> {
    let (_, suffix) = stat.rsplit_once(") ")?;
    let mut fields = suffix.split_whitespace();
    let state = fields.next()?.chars().next()?;
    let _ppid = fields.next()?;
    let pgrp = fields.next()?.parse::<u32>().ok()?;
    Some((state, pgrp))
}

#[cfg(target_os = "macos")]
fn process_group_has_live_members_except_leader(leader_pid: u32) -> Result<bool> {
    let mut pids = vec![0 as libc::pid_t; 4096];
    let buffer_bytes = pids
        .len()
        .checked_mul(mem::size_of::<libc::pid_t>())
        .context("process group pid buffer size overflow")?;
    let result = unsafe {
        libc::proc_listpgrppids(
            leader_pid as libc::pid_t,
            pids.as_mut_ptr().cast(),
            buffer_bytes as libc::c_int,
        )
    };
    if result <= 0 {
        return Ok(false);
    }
    let result = usize::try_from(result).unwrap_or(0);
    let count = if result <= pids.len() {
        result
    } else {
        result / mem::size_of::<libc::pid_t>()
    }
    .min(pids.len());
    for member_pid in pids.into_iter().take(count) {
        if member_pid <= 0 || member_pid as u32 == leader_pid {
            continue;
        }
        let Some(info) = macos_process_bsd_info(member_pid as u32)? else {
            continue;
        };
        if info.pbi_pgid == leader_pid && info.pbi_status != libc::SZOMB {
            return Ok(true);
        }
    }
    Ok(false)
}

#[cfg(target_os = "macos")]
fn macos_process_bsd_info(pid: u32) -> Result<Option<libc::proc_bsdinfo>> {
    let mut info = mem::MaybeUninit::<libc::proc_bsdinfo>::zeroed();
    let result = unsafe {
        libc::proc_pidinfo(
            pid as libc::c_int,
            libc::PROC_PIDTBSDINFO,
            0,
            info.as_mut_ptr().cast(),
            mem::size_of::<libc::proc_bsdinfo>() as libc::c_int,
        )
    };
    if result <= 0 {
        return Ok(None);
    }
    if usize::try_from(result).unwrap_or(0) < mem::size_of::<libc::proc_bsdinfo>() {
        bail!("short proc_pidinfo response for pid {pid}");
    }
    let info = unsafe { info.assume_init() };
    if info.pbi_pid != pid {
        return Ok(None);
    }
    Ok(Some(info))
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn process_group_has_live_members_except_leader(_leader_pid: u32) -> Result<bool> {
    Ok(false)
}

#[cfg(target_os = "linux")]
fn process_start_identity(pid: u32) -> Result<Option<String>> {
    let boot_id = linux_boot_id()?;
    let stat_path = PathBuf::from(format!("/proc/{pid}/stat"));
    let stat = match fs::read_to_string(&stat_path) {
        Ok(stat) => stat,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error).with_context(|| format!("read {}", stat_path.display())),
    };
    let (_, suffix) = stat
        .rsplit_once(") ")
        .with_context(|| format!("parse {}", stat_path.display()))?;
    let start_ticks = suffix
        .split_whitespace()
        .nth(19)
        .with_context(|| format!("read process start ticks from {}", stat_path.display()))?;
    Ok(Some(format!("linux:{boot_id}:{start_ticks}")))
}

#[cfg(target_os = "linux")]
fn linux_boot_id() -> Result<String> {
    Ok(fs::read_to_string("/proc/sys/kernel/random/boot_id")
        .context("read linux boot id")?
        .trim()
        .to_owned())
}

#[cfg(target_os = "macos")]
fn process_start_identity(pid: u32) -> Result<Option<String>> {
    let Some(info) = macos_process_bsd_info(pid)? else {
        return Ok(None);
    };
    Ok(Some(format!(
        "macos:{}:{}",
        info.pbi_start_tvsec, info.pbi_start_tvusec
    )))
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn process_start_identity(_pid: u32) -> Result<Option<String>> {
    Ok(None)
}

fn wait_child_until(child: &mut Child, deadline: Instant) -> bool {
    loop {
        match child.try_wait() {
            Ok(Some(_status)) => return true,
            Ok(None) => {}
            Err(_) => return true,
        }
        if Instant::now() >= deadline {
            return false;
        }
        thread::sleep(READ_POLL_INTERVAL);
    }
}

fn join_worker(worker: thread::JoinHandle<()>) {
    let _ = worker.join();
}

fn validate_daemon_nonempty(name: &str, value: &str) -> Result<()> {
    if value.is_empty() {
        bail!("{name} must not be empty");
    }
    Ok(())
}

fn validate_daemon_nonempty_bytes(name: &str, value: &[u8]) -> Result<()> {
    if value.is_empty() {
        bail!("{name} must not be empty");
    }
    Ok(())
}

fn validate_daemon_positive(name: &str, value: i64) -> Result<()> {
    if value <= 0 {
        bail!("{name} must be positive");
    }
    Ok(())
}

fn validate_task_environment(environment: &[(Vec<u8>, Vec<u8>)]) -> Result<()> {
    if environment.len() > MAX_TASK_ENV_VARS {
        bail!("task environment exceeds {MAX_TASK_ENV_VARS} variables");
    }
    let mut total_bytes = 0usize;
    for (key, value) in environment {
        if key.is_empty() {
            bail!("task environment contains an empty key");
        }
        if key.contains(&b'=') || key.contains(&0) || value.contains(&0) {
            bail!("task environment contains an invalid key or value");
        }
        total_bytes = total_bytes
            .checked_add(key.len())
            .and_then(|total| total.checked_add(value.len()))
            .ok_or_else(|| anyhow::anyhow!("task environment byte count overflow"))?;
        if total_bytes > MAX_TASK_ENV_BYTES {
            bail!("task environment exceeds {MAX_TASK_ENV_BYTES} bytes");
        }
    }
    Ok(())
}

fn write_ok_response(stream: &mut UnixStream, response: Value) -> Result<()> {
    serde_json::to_writer(
        &mut *stream,
        &json!({
            "ok": true,
            "response": response,
        }),
    )?;
    stream.write_all(b"\n")?;
    Ok(())
}

fn write_error_response(stream: &mut UnixStream, error: &str) -> Result<()> {
    serde_json::to_writer(
        &mut *stream,
        &json!({
            "ok": false,
            "error": error,
        }),
    )?;
    stream.write_all(b"\n")?;
    Ok(())
}

fn read_limited_until(
    stream: &mut UnixStream,
    max_bytes: usize,
    deadline: Instant,
) -> Result<Vec<u8>> {
    stream
        .set_nonblocking(true)
        .context("set daemon stream nonblocking")?;
    let result = read_limited_nonblocking(stream, max_bytes, deadline);
    let reset_result = stream
        .set_nonblocking(false)
        .context("restore daemon stream blocking mode");
    match (result, reset_result) {
        (Ok(bytes), Ok(())) => Ok(bytes),
        (Err(error), Ok(())) => Err(error),
        (Ok(_), Err(error)) => Err(error),
        (Err(error), Err(_reset_error)) => Err(error),
    }
}

fn read_limited_nonblocking(
    stream: &mut UnixStream,
    max_bytes: usize,
    deadline: Instant,
) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    let mut buffer = [0_u8; 4096];
    loop {
        match stream.read(&mut buffer) {
            Ok(0) => break,
            Ok(count) => {
                if bytes.len().saturating_add(count) > max_bytes {
                    bail!("daemon message exceeds {max_bytes} bytes");
                }
                bytes.extend_from_slice(&buffer[..count]);
            }
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                let remaining = deadline
                    .checked_duration_since(Instant::now())
                    .filter(|duration| !duration.is_zero())
                    .ok_or_else(|| anyhow::anyhow!("daemon read deadline exceeded"))?;
                thread::sleep(remaining.min(READ_POLL_INTERVAL));
            }
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(error) => return Err(error).context("read unix stream"),
        }
    }
    Ok(bytes)
}

fn prepare_socket_path(path: &Path) -> Result<()> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error).with_context(|| format!("stat {}", path.display())),
    };
    if !metadata.file_type().is_socket() {
        bail!("refusing to replace non-socket path {}", path.display());
    }
    if metadata.uid() != effective_uid() {
        bail!("refusing to replace socket not owned by current user");
    }
    match connect_unix_stream_until(path, Instant::now() + SOCKET_LIVENESS_TIMEOUT) {
        Ok(_) => bail!("daemon socket is already active: {}", path.display()),
        Err(error) if error_chain_has_io_kind(&error, io::ErrorKind::ConnectionRefused) => {}
        Err(error) => bail!(
            "refusing to replace socket with inconclusive liveness at {}: {error:#}",
            path.display()
        ),
    }
    fs::remove_file(path).with_context(|| format!("remove stale socket {}", path.display()))?;
    if let Some(parent) = path.parent() {
        sync_dir(parent)?;
    }
    Ok(())
}

fn error_chain_has_io_kind(error: &anyhow::Error, kind: io::ErrorKind) -> bool {
    error
        .chain()
        .filter_map(|cause| cause.downcast_ref::<io::Error>())
        .any(|io_error| io_error.kind() == kind)
}

fn cleanup_stale_socket_best_effort(layout: &FsLayout) {
    let _ = prepare_socket_path(&layout.daemon_socket_path());
}

pub fn validate_daemon_autostart_endpoint(layout: &FsLayout) -> Result<()> {
    validate_existing_private_dir(layout.home_dir(), "cbth home")?;
    validate_existing_private_dir(&layout.run_dir(), "cbth run directory")?;
    let socket_path = layout.daemon_socket_path();
    let metadata = match fs::symlink_metadata(&socket_path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error).with_context(|| format!("stat {}", socket_path.display())),
    };
    validate_socket_metadata(&socket_path, &metadata)
}

fn validate_socket_endpoint(layout: &FsLayout) -> Result<()> {
    validate_private_dir(layout.home_dir(), "cbth home")?;
    validate_private_dir(&layout.run_dir(), "cbth run directory")?;
    let socket_path = layout.daemon_socket_path();
    let metadata = fs::symlink_metadata(&socket_path)
        .with_context(|| format!("stat {}", socket_path.display()))?;
    validate_socket_metadata(&socket_path, &metadata)
}

fn validate_socket_metadata(socket_path: &Path, metadata: &fs::Metadata) -> Result<()> {
    if !metadata.file_type().is_socket() {
        bail!("daemon endpoint is not a socket: {}", socket_path.display());
    }
    if metadata.uid() != effective_uid() {
        bail!("daemon socket is not owned by current user");
    }
    if metadata.mode() & 0o177 != 0 {
        bail!("daemon socket permissions are wider than 0600");
    }
    Ok(())
}

fn validate_existing_private_dir(path: &Path, name: &str) -> Result<()> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error).with_context(|| format!("stat {}", path.display())),
    };
    validate_private_dir_metadata(path, name, &metadata)
}

fn validate_private_dir(path: &Path, name: &str) -> Result<()> {
    let metadata =
        fs::symlink_metadata(path).with_context(|| format!("stat {}", path.display()))?;
    validate_private_dir_metadata(path, name, &metadata)
}

fn validate_private_dir_metadata(path: &Path, name: &str, metadata: &fs::Metadata) -> Result<()> {
    if !metadata.is_dir() {
        bail!("{name} is not a directory: {}", path.display());
    }
    if metadata.uid() != effective_uid() {
        bail!("{name} is not owned by current user");
    }
    if metadata.mode() & 0o077 != 0 {
        bail!("{name} permissions are wider than 0700");
    }
    Ok(())
}

fn set_socket_permissions(path: &Path) -> Result<()> {
    fs::set_permissions(path, fs::Permissions::from_mode(0o600))
        .with_context(|| format!("chmod 0600 {}", path.display()))
}

fn ensure_peer_is_current_user(stream: &UnixStream) -> Result<()> {
    let peer_uid = peer_uid(stream)?;
    let current_uid = effective_uid();
    if peer_uid == current_uid {
        Ok(())
    } else {
        bail!("daemon peer uid {peer_uid} does not match current uid {current_uid}")
    }
}

#[cfg(target_os = "linux")]
fn peer_uid(stream: &UnixStream) -> Result<u32> {
    let mut cred = libc::ucred {
        pid: 0,
        uid: 0,
        gid: 0,
    };
    let mut len = std::mem::size_of::<libc::ucred>() as libc::socklen_t;
    let rc = unsafe {
        libc::getsockopt(
            stream.as_raw_fd(),
            libc::SOL_SOCKET,
            libc::SO_PEERCRED,
            (&mut cred as *mut libc::ucred).cast(),
            &mut len,
        )
    };
    if rc == 0 {
        Ok(cred.uid)
    } else {
        Err(io::Error::last_os_error()).context("read unix peer credentials")
    }
}

#[cfg(any(
    target_os = "macos",
    target_os = "ios",
    target_os = "freebsd",
    target_os = "netbsd",
    target_os = "openbsd"
))]
fn peer_uid(stream: &UnixStream) -> Result<u32> {
    let mut uid: libc::uid_t = 0;
    let mut gid: libc::gid_t = 0;
    let rc = unsafe { libc::getpeereid(stream.as_raw_fd(), &mut uid, &mut gid) };
    if rc == 0 {
        Ok(uid)
    } else {
        Err(io::Error::last_os_error()).context("read unix peer credentials")
    }
}

#[cfg(not(any(
    target_os = "linux",
    target_os = "macos",
    target_os = "ios",
    target_os = "freebsd",
    target_os = "netbsd",
    target_os = "openbsd"
)))]
fn peer_uid(_stream: &UnixStream) -> Result<u32> {
    bail!("same-user daemon IPC is unsupported on this platform")
}

fn effective_uid() -> u32 {
    unsafe { libc::geteuid() }
}

fn current_epoch_seconds() -> Result<i64> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock is before unix epoch")?;
    i64::try_from(duration.as_secs()).context("epoch seconds overflow")
}

#[cfg(test)]
mod tests {
    use std::io::{Read, Write};
    use std::net::Shutdown;

    use rusqlite::Connection;
    use tempfile::TempDir;

    use super::*;

    struct ContinuouslyReadable;

    impl Read for ContinuouslyReadable {
        fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
            buffer.fill(b'x');
            Ok(buffer.len())
        }
    }

    fn test_state() -> (TempDir, DaemonState) {
        let home = tempfile::tempdir().expect("temp home");
        fs::set_permissions(home.path(), fs::Permissions::from_mode(0o700))
            .expect("chmod temp home");
        let layout = FsLayout::resolve(Some(home.path().to_path_buf())).expect("layout");
        (
            home,
            DaemonState {
                layout,
                started_instant: Instant::now(),
                started_at: 0,
                idle_timeout: Duration::from_secs(60),
                startup_sweep: SweepReport::default(),
                stop_requested: AtomicBool::new(false),
                lifecycle_maintenance_suppressed: AtomicBool::new(false),
                activity_generation: AtomicU64::new(0),
                active_clients: AtomicUsize::new(0),
                active_dispatches: AtomicUsize::new(0),
                cli_app_servers: Mutex::new(HashMap::new()),
                cli_app_server_reservations: Mutex::new(HashMap::new()),
                cli_thread_start_bootstraps: Mutex::new(HashMap::new()),
                supervised_tasks: Arc::new(Mutex::new(HashMap::new())),
            },
        )
    }

    fn handle_test_request(state: &DaemonState, command: &str, payload: Value) -> Value {
        let (mut client, mut server) = UnixStream::pair().expect("socket pair");
        let request = serde_json::to_vec(&json!({
            "command": command,
            "payload": payload,
        }))
        .expect("request json");
        client.write_all(&request).expect("write request");
        client.write_all(b"\n").expect("write request newline");
        client.shutdown(Shutdown::Write).ok();

        if let Err(error) = handle_client(&mut server, state) {
            write_error_response(&mut server, &error.to_string()).expect("write error response");
        }
        drop(server);

        let mut response = Vec::new();
        client.read_to_end(&mut response).expect("read response");
        serde_json::from_slice(&response).expect("response json")
    }

    fn create_proven_cli_session(state: &DaemonState, bound_thread_id: &str) -> String {
        let mut store = Store::open(&state.layout).expect("open store");
        let attached = store
            .attach_or_create_cli_managed_session(
                bound_thread_id,
                crate::models::CliManagedSessionProfile {
                    session_allows_approval: false,
                    session_allows_network: false,
                    session_allows_write_access: false,
                },
                crate::models::CliManagedSessionProfileRequirement {
                    session_allows_approval: Some(false),
                    session_allows_network: Some(false),
                    session_allows_write_access: Some(false),
                },
                100,
            )
            .expect("attach CLI managed session");
        let managed_session_id = attached.session.managed_session_id;
        store
            .note_cli_managed_session_capabilities(
                &managed_session_id,
                1,
                1,
                crate::models::CliManagedSessionCapabilities {
                    capability_thread_resume: true,
                    capability_turn_start: true,
                    capability_current_state_sync: true,
                    capability_turn_completed_event: true,
                    capability_negative_terminal_events: true,
                    capability_thread_start: false,
                    capability_turn_steer: false,
                },
                101,
            )
            .expect("note capabilities");
        store
            .note_cli_managed_session_activity(&managed_session_id, 1, "idle", 1, 102)
            .expect("note activity");
        managed_session_id
    }

    #[test]
    fn task_store_completion_retry_retries_sqlite_lock_errors() {
        let mut attempts = 0;
        let result = retry_task_store_completion("test retry", || {
            attempts += 1;
            if attempts == 1 {
                return Err(rusqlite::Error::SqliteFailure(
                    rusqlite::ffi::Error {
                        code: ErrorCode::DatabaseLocked,
                        extended_code: 0,
                    },
                    None,
                )
                .into());
            }
            Ok("retried")
        })
        .expect("retry succeeds");

        assert_eq!(result, "retried");
        assert_eq!(attempts, 2);
    }

    #[test]
    fn terminalize_supervised_task_error_is_bounded_when_store_stays_locked() {
        let (home, state) = test_state();
        {
            let mut store = Store::open(&state.layout).expect("store");
            store
                .create_task_with_job(
                    NewJob {
                        job_id: "job-terminalize-lock".to_owned(),
                        source_thread_id: "thread-terminalize-lock".to_owned(),
                        summary: "terminalize lock".to_owned(),
                        metadata_json: "{}".to_owned(),
                        policy: DeliveryPolicy::fail_closed(),
                        created_at: 10,
                    },
                    NewTask {
                        task_id: "task-terminalize-lock".to_owned(),
                        job_id: "job-terminalize-lock".to_owned(),
                        source_thread_id: "thread-terminalize-lock".to_owned(),
                        summary: "terminalize lock".to_owned(),
                        command_json: "[]".to_owned(),
                        cwd: home.path().display().to_string(),
                        timeout_seconds: None,
                        max_delivery_attempts: 3,
                        redelivery_window_seconds: 60,
                        created_at: 10,
                    },
                )
                .expect("create task");
        }
        let lock = Connection::open(state.layout.db_path()).expect("lock connection");
        lock.execute_batch("PRAGMA locking_mode=EXCLUSIVE; BEGIN EXCLUSIVE;")
            .expect("hold exclusive lock");

        let started = Instant::now();
        let result = terminalize_supervised_task_error_with_timeout(
            &state.layout,
            "task-terminalize-lock",
            "job-terminalize-lock",
            "failed",
            "task supervisor error",
            3,
            60,
            Duration::from_millis(150),
        );

        assert!(result.is_err());
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "terminalization fallback must not retain the task slot forever"
        );
        drop(lock);
    }

    fn test_managed_cli_app_server(
        managed_session_id: &str,
        bound_thread_id: &str,
        lease_expires_at: Instant,
    ) -> ManagedCliAppServer {
        let mut command = Command::new("sh");
        command
            .arg("-c")
            .arg("while :; do sleep 1; done")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .process_group(0);
        let child = spawn_command_locked(&mut command).expect("spawn test app-server child");
        ManagedCliAppServer {
            managed_session_id: managed_session_id.to_owned(),
            bound_thread_id: bound_thread_id.to_owned(),
            session_epoch: 1,
            url: "ws://127.0.0.1:1".to_owned(),
            child,
            started_at: 100,
            lease_id: "lease-test".to_owned(),
            lease_expires_at,
            drain_running: Arc::new(AtomicBool::new(true)),
            stdout_worker: None,
            stderr_worker: None,
        }
    }

    fn assert_cli_session_proof_invalidated_at_epoch(
        state: &DaemonState,
        managed_session_id: &str,
        expected_epoch: i64,
    ) {
        let store = Store::open(&state.layout).expect("open store");
        let session = store
            .inspect_cli_managed_session(managed_session_id)
            .expect("inspect CLI managed session");
        assert_eq!(session.session_epoch, expected_epoch);
        assert_eq!(session.activity_state, "unknown");
        assert_eq!(session.activity_revision, 0);
        assert_eq!(session.capability_revision, 0);
        assert!(!session.capability_thread_resume);
        assert!(!session.capability_turn_start);
        assert!(!session.capability_current_state_sync);
    }

    fn assert_cli_session_proof_invalidated(state: &DaemonState, managed_session_id: &str) {
        assert_cli_session_proof_invalidated_at_epoch(state, managed_session_id, 2);
    }

    #[test]
    fn saturated_dispatch_slots_still_allow_control_requests() {
        let (_home, state) = test_state();
        let mut dispatch_slots = Vec::new();
        for _ in 0..MAX_DISPATCH_WORKERS {
            dispatch_slots.push(try_acquire_dispatch_slot(&state).expect("dispatch slot"));
        }

        let ping = handle_test_request(&state, "ping", Value::Null);
        assert_eq!(ping["ok"], true);
        assert_eq!(ping["response"]["message"], "pong");

        let dispatch = handle_test_request(&state, "dispatch", Value::Null);
        assert_eq!(dispatch["ok"], false);
        assert_eq!(dispatch["error"], "daemon is busy");

        let task_run = handle_test_request(&state, "task_run", Value::Null);
        assert_eq!(task_run["ok"], false);
        assert_eq!(task_run["error"], "daemon is busy");

        let task_cancel = handle_test_request(&state, "task_cancel", Value::Null);
        assert_eq!(task_cancel["ok"], false);
        assert_eq!(task_cancel["error"], "daemon is busy");

        drop(dispatch_slots);
        let _slot = try_acquire_dispatch_slot(&state).expect("released dispatch slot");
    }

    #[test]
    fn lifecycle_stop_drain_is_bounded_for_busy_worker() {
        let release_worker = Arc::new(AtomicBool::new(false));
        let release_worker_for_thread = Arc::clone(&release_worker);
        let mut worker = Some(thread::spawn(move || {
            while !release_worker_for_thread.load(Ordering::Acquire) {
                thread::sleep(READ_POLL_INTERVAL);
            }
        }));

        let started = Instant::now();
        drain_lifecycle_maintenance_until(&mut worker, started + Duration::from_millis(100));

        assert!(
            started.elapsed() < Duration::from_secs(1),
            "stop path lifecycle drain should be bounded"
        );
        assert!(
            worker.is_some(),
            "busy lifecycle worker should be left detached for stop-path shutdown"
        );
        release_worker.store(true, Ordering::Release);
        join_lifecycle_maintenance(worker);
    }

    #[test]
    fn daemon_shutdown_invalidates_registered_cli_app_server_proof() {
        let (_home, state) = test_state();
        let managed_session_id = create_proven_cli_session(&state, "thread-daemon-shutdown");
        let server = test_managed_cli_app_server(
            &managed_session_id,
            "thread-daemon-shutdown",
            Instant::now() + Duration::from_secs(60),
        );
        state
            .cli_app_servers
            .lock()
            .expect("servers lock")
            .insert(managed_session_id.clone(), server);

        stop_all_cli_app_servers(&state);

        assert!(
            state
                .cli_app_servers
                .lock()
                .expect("servers lock")
                .is_empty()
        );
        assert_cli_session_proof_invalidated(&state, &managed_session_id);
    }

    #[test]
    fn expired_cli_app_server_lease_invalidates_registered_proof() {
        let (_home, state) = test_state();
        let managed_session_id = create_proven_cli_session(&state, "thread-daemon-expired");
        let server = test_managed_cli_app_server(
            &managed_session_id,
            "thread-daemon-expired",
            Instant::now() - Duration::from_secs(1),
        );
        state
            .cli_app_servers
            .lock()
            .expect("servers lock")
            .insert(managed_session_id.clone(), server);

        reap_expired_cli_app_servers(&state);

        assert!(
            state
                .cli_app_servers
                .lock()
                .expect("servers lock")
                .is_empty()
        );
        assert_cli_session_proof_invalidated(&state, &managed_session_id);
    }

    #[test]
    fn expired_cli_app_server_reaper_cleans_new_epoch_proof() {
        let (_home, state) = test_state();
        let managed_session_id = create_proven_cli_session(&state, "thread-daemon-new-epoch");
        let server = test_managed_cli_app_server(
            &managed_session_id,
            "thread-daemon-new-epoch",
            Instant::now() - Duration::from_secs(1),
        );
        state
            .cli_app_servers
            .lock()
            .expect("servers lock")
            .insert(managed_session_id.clone(), server);
        let mut store = Store::open(&state.layout).expect("open store");
        store
            .invalidate_cli_managed_session_proof(&managed_session_id, 1, 200)
            .expect("invalidate epoch 1 proof");
        store
            .note_cli_managed_session_capabilities(
                &managed_session_id,
                2,
                1,
                crate::models::CliManagedSessionCapabilities {
                    capability_thread_resume: true,
                    capability_turn_start: false,
                    capability_current_state_sync: true,
                    capability_turn_completed_event: false,
                    capability_negative_terminal_events: false,
                    capability_thread_start: false,
                    capability_turn_steer: false,
                },
                201,
            )
            .expect("record epoch 2 capabilities");
        store
            .note_cli_managed_session_activity(&managed_session_id, 2, "idle", 1, 202)
            .expect("record epoch 2 activity");
        drop(store);

        reap_expired_cli_app_servers(&state);

        assert!(
            state
                .cli_app_servers
                .lock()
                .expect("servers lock")
                .is_empty()
        );
        assert_cli_session_proof_invalidated_at_epoch(&state, &managed_session_id, 3);
    }

    #[test]
    fn expired_cli_app_server_reaper_fences_already_clear_current_proof() {
        let (_home, state) = test_state();
        let managed_session_id = create_proven_cli_session(&state, "thread-daemon-clear-epoch");
        let server = test_managed_cli_app_server(
            &managed_session_id,
            "thread-daemon-clear-epoch",
            Instant::now() - Duration::from_secs(1),
        );
        state
            .cli_app_servers
            .lock()
            .expect("servers lock")
            .insert(managed_session_id.clone(), server);
        let mut store = Store::open(&state.layout).expect("open store");
        store
            .invalidate_cli_managed_session_proof(&managed_session_id, 1, 200)
            .expect("clear epoch 1 proof");
        drop(store);

        reap_expired_cli_app_servers(&state);

        assert!(
            state
                .cli_app_servers
                .lock()
                .expect("servers lock")
                .is_empty()
        );
        assert_cli_session_proof_invalidated_at_epoch(&state, &managed_session_id, 3);
    }

    #[test]
    fn stale_cli_app_server_proof_does_not_invalidate_replacement() {
        let (_home, state) = test_state();
        let managed_session_id = create_proven_cli_session(&state, "thread-daemon-replaced");
        let stale_server = test_managed_cli_app_server(
            &managed_session_id,
            "thread-daemon-replaced",
            Instant::now() - Duration::from_secs(1),
        );
        let stale_proof = cli_app_server_proof(&stale_server);
        stop_managed_cli_app_server_process(stale_server);

        let replacement = test_managed_cli_app_server(
            &managed_session_id,
            "thread-daemon-replaced",
            Instant::now() + Duration::from_secs(60),
        );
        let replacement_pid = replacement.child.id();
        state
            .cli_app_servers
            .lock()
            .expect("servers lock")
            .insert(managed_session_id.clone(), replacement);

        assert!(
            !invalidate_and_stop_registered_cli_app_server(&state, &stale_proof)
                .expect("stale proof cleanup")
        );

        let servers = state.cli_app_servers.lock().expect("servers lock");
        let server = servers
            .get(&managed_session_id)
            .expect("replacement remains registered");
        assert_eq!(server.child.id(), replacement_pid);
        drop(servers);
        let store = Store::open(&state.layout).expect("open store");
        let session = store
            .inspect_cli_managed_session(&managed_session_id)
            .expect("inspect CLI managed session");
        assert_eq!(session.session_epoch, 1);
        assert_eq!(session.activity_state, "idle");

        stop_all_cli_app_servers(&state);
    }

    #[test]
    fn expired_cli_app_server_reaper_uses_short_store_timeout() {
        let (_home, state) = test_state();
        let managed_session_id = create_proven_cli_session(&state, "thread-daemon-locked");
        let server = test_managed_cli_app_server(
            &managed_session_id,
            "thread-daemon-locked",
            Instant::now() - Duration::from_secs(1),
        );
        state
            .cli_app_servers
            .lock()
            .expect("servers lock")
            .insert(managed_session_id.clone(), server);
        let conn = Connection::open(state.layout.db_path()).expect("open db lock connection");
        conn.execute_batch("PRAGMA locking_mode=EXCLUSIVE; BEGIN EXCLUSIVE;")
            .expect("hold exclusive db lock");

        let started = Instant::now();
        reap_expired_cli_app_servers(&state);

        assert!(
            started.elapsed() < Duration::from_secs(1),
            "expired app-server reaper blocked too long on locked store"
        );
        assert!(
            state
                .cli_app_servers
                .lock()
                .expect("servers lock")
                .contains_key(&managed_session_id),
            "reaper should keep registry entry when proof invalidation fails"
        );

        drop(conn);
        stop_all_cli_app_servers(&state);
        assert!(
            state
                .cli_app_servers
                .lock()
                .expect("servers lock")
                .is_empty()
        );
        assert_cli_session_proof_invalidated(&state, &managed_session_id);
    }

    #[test]
    fn app_server_spawn_commands_reject_after_stop_request() {
        let (_home, state) = test_state();
        state.stop_requested.store(true, Ordering::Release);

        let ensure_error = ensure_cli_app_server(
            &state,
            CliAppServerEnsurePayload {
                managed_session_id: "session-stopped-ensure".to_owned(),
                bound_thread_id: "thread-stopped-ensure".to_owned(),
                session_epoch: 1,
                codex_binary: b"/definitely/missing/codex".to_vec(),
                lease_id: "lease-stopped-ensure".to_owned(),
                lease_ttl_seconds: None,
            },
        )
        .expect_err("stopped daemon should reject app-server ensure");
        assert!(
            ensure_error.to_string().contains("daemon is stopping"),
            "{ensure_error:#}"
        );

        let start_error = start_cli_thread(
            &state,
            CliThreadStartPayload {
                codex_binary: b"/definitely/missing/codex".to_vec(),
                cwd: "/tmp".to_owned(),
                lease_id: "lease-stopped-thread-start".to_owned(),
                lease_ttl_seconds: None,
                thread_start_params: None,
            },
        )
        .expect_err("stopped daemon should reject thread/start bootstrap");
        assert!(
            start_error.to_string().contains("daemon is stopping"),
            "{start_error:#}"
        );
    }

    #[test]
    fn thread_start_stop_before_bootstrap_registration_kills_candidate_server() {
        let (home, state) = test_state();
        let state = Arc::new(state);
        let fake_codex = home.path().join("fake-codex");
        let pid_file = home.path().join("fake-codex.pid");
        let pid_file_quoted = pid_file.display().to_string().replace('\'', "'\\''");
        fs::write(
            &fake_codex,
            format!(
                "#!/bin/sh\nprintf '%s\\n' \"$$\" > '{pid_file_quoted}'\nprintf 'listening on: ws://127.0.0.1:1234\\n'\nwhile :; do sleep 1; done\n"
            ),
        )
        .expect("write fake codex");
        fs::set_permissions(&fake_codex, fs::Permissions::from_mode(0o700))
            .expect("chmod fake codex");
        let bootstrap_guard = state
            .cli_thread_start_bootstraps
            .lock()
            .expect("bootstrap lock");
        let task_state = Arc::clone(&state);
        let task = thread::spawn(move || {
            start_cli_thread(
                &task_state,
                CliThreadStartPayload {
                    codex_binary: fake_codex.into_os_string().into_vec(),
                    cwd: home.path().display().to_string(),
                    lease_id: "lease-stopped-thread-start-registration".to_owned(),
                    lease_ttl_seconds: None,
                    thread_start_params: None,
                },
            )
        });

        let deadline = Instant::now() + Duration::from_secs(5);
        let pid = loop {
            if let Ok(pid) = fs::read_to_string(&pid_file)
                .map(|pid| pid.trim().to_owned())
                .and_then(|pid| {
                    pid.parse::<u32>().map_err(|error| {
                        io::Error::new(io::ErrorKind::InvalidData, error.to_string())
                    })
                })
            {
                break pid;
            }
            assert!(Instant::now() < deadline, "fake app-server did not start");
            thread::sleep(Duration::from_millis(20));
        };
        state.stop_requested.store(true, Ordering::Release);
        drop(bootstrap_guard);
        let error = task
            .join()
            .expect("thread/start worker joins")
            .expect_err("stopped daemon should reject bootstrap registration");

        assert!(
            error.to_string().contains("daemon is stopping"),
            "{error:#}"
        );
        assert!(
            state
                .cli_thread_start_bootstraps
                .lock()
                .expect("bootstrap lock")
                .is_empty()
        );
        assert!(
            !process_group_exists(pid),
            "candidate bootstrap app-server survived stopped registration"
        );
    }

    #[test]
    fn stopped_daemon_does_not_promote_removed_bootstrap_after_cleanup() {
        let (_home, state) = test_state();
        let state = Arc::new(state);
        let bootstrap_id = "bootstrap-stop-promote-race".to_owned();
        let managed_session_id = "session-stop-promote-race".to_owned();
        let bound_thread_id = "thread-stop-promote-race".to_owned();
        reserve_cli_app_server(
            &state,
            CliAppServerReservePayload {
                bound_thread_id: bound_thread_id.clone(),
                lease_id: "lease-test".to_owned(),
                lease_ttl_seconds: None,
            },
        )
        .expect("reserve app-server");
        let server = test_managed_cli_app_server(
            &bootstrap_id,
            &bound_thread_id,
            Instant::now() + Duration::from_secs(60),
        );
        let pid = server.child.id();
        state
            .cli_thread_start_bootstraps
            .lock()
            .expect("bootstrap lock")
            .insert(bootstrap_id.clone(), server);
        let app_server_guard = state.cli_app_servers.lock().expect("app-server lock");
        let promote_state = Arc::clone(&state);
        let promote = thread::spawn(move || {
            promote_cli_thread_start_app_server(
                &promote_state,
                CliThreadStartPromotePayload {
                    bootstrap_id,
                    managed_session_id,
                    bound_thread_id,
                    session_epoch: 1,
                    lease_id: "lease-test".to_owned(),
                    lease_ttl_seconds: None,
                },
            )
        });

        thread::sleep(Duration::from_millis(100));
        assert!(
            state
                .cli_thread_start_bootstraps
                .lock()
                .expect("bootstrap lock")
                .is_empty(),
            "promote should remove bootstrap before blocking on app-server registry"
        );
        state.stop_requested.store(true, Ordering::Release);
        drop(app_server_guard);
        let error = promote
            .join()
            .expect("promote worker joins")
            .expect_err("stopped daemon should reject promotion");

        assert!(
            error.to_string().contains("daemon is stopping"),
            "{error:#}"
        );
        assert!(
            state
                .cli_thread_start_bootstraps
                .lock()
                .expect("bootstrap lock")
                .is_empty()
        );
        assert!(
            state
                .cli_app_servers
                .lock()
                .expect("app-server lock")
                .is_empty()
        );
        assert_ne!(
            child_status_without_reaping(pid).unwrap_or(ChildStatusWithoutReaping::NotWaitable),
            ChildStatusWithoutReaping::Running
        );
    }

    #[test]
    fn promote_conflict_preserves_running_registered_app_server() {
        let (_home, state) = test_state();
        let managed_session_id = create_proven_cli_session(&state, "thread-promote-conflict");
        reserve_cli_app_server(
            &state,
            CliAppServerReservePayload {
                bound_thread_id: "thread-promote-conflict".to_owned(),
                lease_id: "lease-test".to_owned(),
                lease_ttl_seconds: None,
            },
        )
        .expect("reserve app-server");
        let existing = test_managed_cli_app_server(
            &managed_session_id,
            "thread-promote-conflict",
            Instant::now() + Duration::from_secs(60),
        );
        let existing_pid = existing.child.id();
        state
            .cli_app_servers
            .lock()
            .expect("servers lock")
            .insert(managed_session_id.clone(), existing);
        let bootstrap_id = "bootstrap-promote-conflict".to_owned();
        let candidate = test_managed_cli_app_server(
            &bootstrap_id,
            "thread-promote-conflict",
            Instant::now() + Duration::from_secs(60),
        );
        let candidate_pid = candidate.child.id();
        state
            .cli_thread_start_bootstraps
            .lock()
            .expect("bootstrap lock")
            .insert(bootstrap_id.clone(), candidate);

        let error = promote_cli_thread_start_app_server(
            &state,
            CliThreadStartPromotePayload {
                bootstrap_id,
                managed_session_id: managed_session_id.clone(),
                bound_thread_id: "thread-promote-conflict".to_owned(),
                session_epoch: 1,
                lease_id: "lease-test".to_owned(),
                lease_ttl_seconds: None,
            },
        )
        .expect_err("promote should reject a live registered app-server");

        assert!(
            error
                .to_string()
                .contains("already has a registered CLI app-server"),
            "{error:#}"
        );
        assert!(
            state
                .cli_thread_start_bootstraps
                .lock()
                .expect("bootstrap lock")
                .is_empty(),
            "conflicting promote should consume and stop only the candidate bootstrap"
        );
        let servers = state.cli_app_servers.lock().expect("servers lock");
        let existing = servers
            .get(&managed_session_id)
            .expect("existing app-server remains registered");
        assert_eq!(existing.child.id(), existing_pid);
        assert!(
            process_group_exists(existing_pid),
            "running registered app-server should not be stopped by a duplicate promote"
        );
        drop(servers);
        assert!(
            !process_group_exists(candidate_pid),
            "duplicate promote candidate should be stopped"
        );
        stop_all_cli_app_servers(&state);
    }

    #[test]
    fn app_server_listener_url_requires_loopback_host() {
        assert!(app_server_listener_url_is_loopback("ws://127.0.0.1:1234"));
        assert!(app_server_listener_url_is_loopback("ws://localhost:1234"));
        assert!(app_server_listener_url_is_loopback("ws://[::1]:1234"));
        assert!(!app_server_listener_url_is_loopback(
            "ws://127.0.0.1:1234@remote.example"
        ));
        assert!(!app_server_listener_url_is_loopback(
            "ws://localhost:1234@remote.example"
        ));
        assert!(!app_server_listener_url_is_loopback(
            "ws://localhost.example:1234"
        ));
        assert!(!app_server_listener_url_is_loopback("wss://127.0.0.1:1234"));
        assert!(!app_server_listener_url_is_loopback("ws://127.0.0.1:0"));
    }

    #[test]
    fn daemon_request_bytes_rejects_encoded_oversized_payload() {
        let payload = json!({
            "environment": [(vec![b'x'; MAX_REQUEST_BYTES / 2], Vec::<u8>::new())],
        });
        let error = daemon_request_bytes("task_run", payload)
            .expect_err("oversized request should fail")
            .to_string();
        assert!(
            error.contains("daemon request is too large after JSON encoding"),
            "{error}"
        );
    }

    #[test]
    fn app_server_listener_parser_waits_for_complete_line() {
        assert_eq!(
            parse_app_server_listener_url(b"  listening on: ws://127.0.0.1:"),
            None
        );
        assert_eq!(
            parse_app_server_listener_url(b"  listening on: ws://127.0.0.1:1234\n"),
            Some("ws://127.0.0.1:1234".to_owned())
        );
        assert_eq!(
            parse_app_server_listener_url(
                b"debug: listening on: ws://127.0.0.1:9\n  listening on: ws://127.0.0.1:1234\n"
            ),
            Some("ws://127.0.0.1:1234".to_owned())
        );
    }

    #[test]
    fn drain_reader_stops_even_when_pipe_is_continuously_readable() {
        let running = Arc::new(AtomicBool::new(true));
        let worker_running = Arc::clone(&running);
        let worker = thread::spawn(move || drain_reader(ContinuouslyReadable, worker_running));

        running.store(false, Ordering::Release);
        let deadline = Instant::now() + Duration::from_secs(1);
        while !worker.is_finished() && Instant::now() < deadline {
            thread::sleep(READ_POLL_INTERVAL);
        }

        assert!(
            worker.is_finished(),
            "drain worker did not observe stop flag"
        );
        worker.join().expect("join drain worker");
    }

    #[test]
    fn task_wait_poll_sleep_caps_at_timeout_deadline() {
        let start = Instant::now();
        assert_eq!(
            task_wait_poll_sleep_duration(
                start,
                Some(Duration::from_secs(1)),
                start + Duration::from_millis(950),
                false,
            ),
            Duration::from_millis(50)
        );
        assert_eq!(
            task_wait_poll_sleep_duration(
                start,
                Some(Duration::from_secs(1)),
                start + Duration::from_millis(1001),
                false,
            ),
            Duration::ZERO
        );
        assert_eq!(
            task_wait_poll_sleep_duration(
                start,
                Some(Duration::from_secs(1)),
                start + Duration::from_millis(1001),
                true,
            ),
            TASK_WAIT_POLL_INTERVAL
        );
        assert_eq!(
            task_wait_poll_sleep_duration(start, None, start + Duration::from_secs(2), false),
            TASK_WAIT_POLL_INTERVAL
        );
    }

    #[test]
    fn task_stream_spool_enforces_file_cap_and_tail_bound() {
        let home = tempfile::tempdir().expect("temp home");
        let path = home.path().join("stdout.log");
        let worker = spawn_task_stream_spool_with_limits(
            io::Cursor::new(b"abcdef".to_vec()),
            path.clone(),
            3,
            4,
        );

        let capture = join_task_stream_spool(worker).expect("join spool worker");
        assert_eq!(capture.bytes_seen, 6);
        assert!(capture.truncated);
        assert_eq!(capture.tail, b"cdef");
        assert_eq!(fs::read(&path).expect("spool file"), b"abc");
    }

    #[test]
    fn task_stream_spool_abort_stops_held_nonblocking_pipe() {
        let home = tempfile::tempdir().expect("temp home");
        let path = home.path().join("stdout.log");
        let (reader, mut writer) = UnixStream::pair().expect("stream pair");
        set_fd_nonblocking(reader.as_raw_fd(), true).expect("set nonblocking");
        writer.write_all(b"ready").expect("write stream");

        let worker = spawn_task_stream_spool_with_limits(reader, path.clone(), 1024, 16);
        let deadline = Instant::now() + Duration::from_secs(1);
        while !worker.handle.is_finished() && Instant::now() < deadline {
            thread::sleep(READ_POLL_INTERVAL);
        }
        assert!(
            !worker.handle.is_finished(),
            "worker should stay alive while writer holds the stream open"
        );

        worker.abort.store(true, Ordering::Release);
        let capture = join_task_stream_spool(worker).expect("join aborted spool");
        assert!(capture.truncated);
        assert_eq!(fs::read(&path).expect("spool file"), b"ready");
        drop(writer);
    }

    #[test]
    fn cancelled_before_spawn_does_not_execute_command() {
        let home = tempfile::tempdir().expect("temp home");
        fs::set_permissions(home.path(), fs::Permissions::from_mode(0o700))
            .expect("chmod temp home");
        let layout = FsLayout::resolve(Some(home.path().to_path_buf())).expect("layout");
        let marker = home.path().join("pre-spawn-cancel-marker");
        {
            let mut store = Store::open(&layout).expect("store");
            store
                .create_task_with_job(
                    NewJob {
                        job_id: "job-pre-spawn-cancel".to_owned(),
                        source_thread_id: "thread-pre-spawn-cancel".to_owned(),
                        summary: "pre spawn cancel".to_owned(),
                        metadata_json: "{}".to_owned(),
                        policy: DeliveryPolicy::fail_closed(),
                        created_at: 10,
                    },
                    NewTask {
                        task_id: "task-pre-spawn-cancel".to_owned(),
                        job_id: "job-pre-spawn-cancel".to_owned(),
                        source_thread_id: "thread-pre-spawn-cancel".to_owned(),
                        summary: "pre spawn cancel".to_owned(),
                        command_json: "[]".to_owned(),
                        cwd: home.path().display().to_string(),
                        timeout_seconds: None,
                        max_delivery_attempts: 3,
                        redelivery_window_seconds: 60,
                        created_at: 10,
                    },
                )
                .expect("create task");
            store
                .request_task_cancel("task-pre-spawn-cancel", 11)
                .expect("request cancel");
        }
        let control = Arc::new(SupervisedTaskControl::default());
        request_supervised_task_cancel(&control);
        let command = vec![
            b"/bin/sh".to_vec(),
            b"-c".to_vec(),
            format!("printf ran > '{}'", marker.display()).into_bytes(),
        ];

        run_supervised_task_worker_inner(
            &layout,
            "task-pre-spawn-cancel",
            "job-pre-spawn-cancel",
            command,
            Vec::new(),
            home.path(),
            None,
            3,
            60,
            &control,
        )
        .expect("cancel before spawn");

        assert!(!marker.exists(), "cancelled task command was executed");
        let store = Store::open(&layout).expect("store");
        let task = store
            .inspect_task("task-pre-spawn-cancel")
            .expect("inspect task");
        assert_eq!(task.status, "cancelled");
        assert_eq!(task.pid, None);
        let job = store.inspect_job("job-pre-spawn-cancel").expect("job");
        assert_eq!(job.status, "failed");
    }

    #[test]
    fn cancelled_after_started_before_exec_gate_release_does_not_execute_command() {
        let home = tempfile::tempdir().expect("temp home");
        fs::set_permissions(home.path(), fs::Permissions::from_mode(0o700))
            .expect("chmod temp home");
        let layout = FsLayout::resolve(Some(home.path().to_path_buf())).expect("layout");
        let marker = home.path().join("pre-exec-cancel-marker");
        {
            let mut store = Store::open(&layout).expect("store");
            store
                .create_task_with_job(
                    NewJob {
                        job_id: "job-pre-exec-cancel".to_owned(),
                        source_thread_id: "thread-pre-exec-cancel".to_owned(),
                        summary: "pre exec cancel".to_owned(),
                        metadata_json: "{}".to_owned(),
                        policy: DeliveryPolicy::fail_closed(),
                        created_at: 10,
                    },
                    NewTask {
                        task_id: "task-pre-exec-cancel".to_owned(),
                        job_id: "job-pre-exec-cancel".to_owned(),
                        source_thread_id: "thread-pre-exec-cancel".to_owned(),
                        summary: "pre exec cancel".to_owned(),
                        command_json: "[]".to_owned(),
                        cwd: home.path().display().to_string(),
                        timeout_seconds: None,
                        max_delivery_attempts: 3,
                        redelivery_window_seconds: 60,
                        created_at: 10,
                    },
                )
                .expect("create task");
        }
        let control = SupervisedTaskControl::default();
        let exec_gate = task_spawn_exec_gate().expect("exec gate");
        let mut command = Command::new("/bin/sh");
        install_task_spawn_exec_gate(&mut command, &exec_gate);
        command
            .arg("-c")
            .arg(TASK_SPAWN_EXEC_GATE_SCRIPT)
            .arg("cbth-task-gate")
            .arg("/bin/sh")
            .arg("-c")
            .arg("printf ran > \"$1\"")
            .arg("cbth-task")
            .arg(&marker)
            .current_dir(home.path())
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .process_group(0);
        let mut child = spawn_command_locked(&mut command).expect("spawn gated child");
        let pid = child.id();
        let pid_identity = process_start_identity(pid)
            .expect("process identity")
            .expect("process identity available");
        set_supervised_task_process(&control, pid, pid_identity).expect("set process");
        {
            let mut store = Store::open(&layout).expect("store");
            store
                .mark_task_started("task-pre-exec-cancel", i64::from(pid), Some("test-pid"), 11)
                .expect("mark started");
        }
        request_supervised_task_cancel(&control);

        finish_cancelled_before_exec_gate_release(
            &layout,
            "task-pre-exec-cancel",
            "job-pre-exec-cancel",
            3,
            60,
            &control,
            &mut child,
            pid,
        )
        .expect("finish pre-exec cancel");
        drop(exec_gate);

        assert!(!marker.exists(), "cancelled task command was executed");
        assert_eq!(
            supervised_task_process_state(&control).expect("process state"),
            SupervisedTaskProcessState::Completing
        );
        let store = Store::open(&layout).expect("store");
        let task = store.inspect_task("task-pre-exec-cancel").expect("task");
        assert_eq!(task.status, "cancelled");
        let job = store.inspect_job("job-pre-exec-cancel").expect("job");
        assert_eq!(job.status, "failed");
    }

    #[test]
    fn startup_recovery_skips_process_group_when_pid_identity_mismatches() {
        let home = tempfile::tempdir().expect("temp home");
        fs::set_permissions(home.path(), fs::Permissions::from_mode(0o700))
            .expect("chmod temp home");
        let layout = FsLayout::resolve(Some(home.path().to_path_buf())).expect("layout");
        let mut command = Command::new("/bin/sh");
        command.arg("-c").arg("sleep 30").process_group(0);
        let mut child = spawn_command_locked(&mut command).expect("spawn child");
        let pid = child.id();
        {
            let mut store = Store::open(&layout).expect("store");
            store
                .create_task_with_job(
                    NewJob {
                        job_id: "job-pid-identity-mismatch".to_owned(),
                        source_thread_id: "thread-pid-identity-mismatch".to_owned(),
                        summary: "pid identity mismatch".to_owned(),
                        metadata_json: "{}".to_owned(),
                        policy: DeliveryPolicy::fail_closed(),
                        created_at: 10,
                    },
                    NewTask {
                        task_id: "task-pid-identity-mismatch".to_owned(),
                        job_id: "job-pid-identity-mismatch".to_owned(),
                        source_thread_id: "thread-pid-identity-mismatch".to_owned(),
                        summary: "pid identity mismatch".to_owned(),
                        command_json: "[]".to_owned(),
                        cwd: home.path().display().to_string(),
                        timeout_seconds: None,
                        max_delivery_attempts: 3,
                        redelivery_window_seconds: 60,
                        created_at: 10,
                    },
                )
                .expect("create task");
            store
                .mark_task_started(
                    "task-pid-identity-mismatch",
                    i64::from(pid),
                    Some("not-the-current-process"),
                    11,
                )
                .expect("mark task started");
        }

        recover_lost_task_process_groups(&layout).expect("recover lost tasks");

        assert!(
            process_group_exists(pid),
            "identity mismatch should not kill this process group"
        );
        terminate_task_child_best_effort(&mut child, pid);
        let _ = child.wait();
    }

    #[test]
    fn startup_recovery_kills_group_after_leader_exits_on_term() {
        let home = tempfile::tempdir().expect("temp home");
        fs::set_permissions(home.path(), fs::Permissions::from_mode(0o700))
            .expect("chmod temp home");
        let layout = FsLayout::resolve(Some(home.path().to_path_buf())).expect("layout");
        let mut command = Command::new("/bin/sh");
        command
            .arg("-c")
            .arg(
                "trap 'exit 0' TERM; /bin/sh -c 'trap \"\" TERM; while true; do sleep 1; done' & wait",
            )
            .process_group(0);
        let mut child = spawn_command_locked(&mut command).expect("spawn child");
        let pid = child.id();
        let pid_identity = process_start_identity(pid)
            .expect("process identity")
            .expect("process identity available");
        {
            let mut store = Store::open(&layout).expect("store");
            store
                .create_task_with_job(
                    NewJob {
                        job_id: "job-leader-exits-on-term".to_owned(),
                        source_thread_id: "thread-leader-exits-on-term".to_owned(),
                        summary: "leader exits on term".to_owned(),
                        metadata_json: "{}".to_owned(),
                        policy: DeliveryPolicy::fail_closed(),
                        created_at: 10,
                    },
                    NewTask {
                        task_id: "task-leader-exits-on-term".to_owned(),
                        job_id: "job-leader-exits-on-term".to_owned(),
                        source_thread_id: "thread-leader-exits-on-term".to_owned(),
                        summary: "leader exits on term".to_owned(),
                        command_json: "[]".to_owned(),
                        cwd: home.path().display().to_string(),
                        timeout_seconds: None,
                        max_delivery_attempts: 3,
                        redelivery_window_seconds: 60,
                        created_at: 10,
                    },
                )
                .expect("create task");
            store
                .mark_task_started(
                    "task-leader-exits-on-term",
                    i64::from(pid),
                    Some(&pid_identity),
                    11,
                )
                .expect("mark task started");
        }

        recover_lost_task_process_groups(&layout).expect("recover lost tasks");

        let _ = child.wait();
        let deadline = Instant::now() + Duration::from_secs(2);
        while process_group_exists(pid) && Instant::now() < deadline {
            thread::sleep(TASK_WAIT_POLL_INTERVAL);
        }
        assert!(
            !process_group_exists(pid),
            "startup recovery should kill surviving same-group descendants"
        );
    }

    #[test]
    fn startup_recovery_skips_group_when_leader_identity_is_unavailable_but_members_remain() {
        let home = tempfile::tempdir().expect("temp home");
        fs::set_permissions(home.path(), fs::Permissions::from_mode(0o700))
            .expect("chmod temp home");
        let layout = FsLayout::resolve(Some(home.path().to_path_buf())).expect("layout");
        let release_path = home.path().join("release-leader");
        let mut command = Command::new("/bin/sh");
        command
            .arg("-c")
            .arg(format!(
                "while [ ! -f '{}' ]; do sleep 0.01; done; /bin/sh -c 'trap \"\" TERM; while true; do sleep 1; done' & exit 0",
                release_path.display()
            ))
            .process_group(0);
        let mut child = spawn_command_locked(&mut command).expect("spawn child");
        let pid = child.id();
        let pid_identity = process_start_identity(pid)
            .expect("process identity")
            .expect("process identity available");
        {
            let mut store = Store::open(&layout).expect("store");
            store
                .create_task_with_job(
                    NewJob {
                        job_id: "job-leader-already-exited".to_owned(),
                        source_thread_id: "thread-leader-already-exited".to_owned(),
                        summary: "leader already exited".to_owned(),
                        metadata_json: "{}".to_owned(),
                        policy: DeliveryPolicy::fail_closed(),
                        created_at: 10,
                    },
                    NewTask {
                        task_id: "task-leader-already-exited".to_owned(),
                        job_id: "job-leader-already-exited".to_owned(),
                        source_thread_id: "thread-leader-already-exited".to_owned(),
                        summary: "leader already exited".to_owned(),
                        command_json: "[]".to_owned(),
                        cwd: home.path().display().to_string(),
                        timeout_seconds: None,
                        max_delivery_attempts: 3,
                        redelivery_window_seconds: 60,
                        created_at: 10,
                    },
                )
                .expect("create task");
            store
                .mark_task_started(
                    "task-leader-already-exited",
                    i64::from(pid),
                    Some(&pid_identity),
                    11,
                )
                .expect("mark task started");
        }

        fs::write(&release_path, b"go").expect("release leader");
        let _ = child.wait();
        let live_deadline = Instant::now() + Duration::from_secs(2);
        while !process_group_has_live_members_after_leader_exit(pid)
            && Instant::now() < live_deadline
        {
            thread::sleep(TASK_WAIT_POLL_INTERVAL);
        }
        assert!(
            process_group_has_live_members_after_leader_exit(pid),
            "background member should keep the original process group alive"
        );

        recover_lost_task_process_groups(&layout).expect("recover lost tasks");

        assert!(
            process_group_exists(pid),
            "startup recovery must not signal a group after leader identity becomes unavailable"
        );
        signal_process_group(pid, libc::SIGKILL);
        let cleanup_deadline = Instant::now() + Duration::from_secs(2);
        while process_group_exists(pid) && Instant::now() < cleanup_deadline {
            thread::sleep(TASK_WAIT_POLL_INTERVAL);
        }
        assert!(!process_group_exists(pid), "test cleanup should kill group");
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn linux_process_identity_includes_boot_id() {
        let boot_id = linux_boot_id().expect("boot id");
        let identity = process_start_identity(std::process::id())
            .expect("process identity")
            .expect("current process identity");

        assert!(
            identity.starts_with(&format!("linux:{boot_id}:")),
            "linux process identity should be scoped to the current boot"
        );
    }

    #[test]
    fn task_setup_store_lock_retries_before_exec_release() {
        let home = tempfile::tempdir().expect("temp home");
        fs::set_permissions(home.path(), fs::Permissions::from_mode(0o700))
            .expect("chmod temp home");
        let layout = FsLayout::resolve(Some(home.path().to_path_buf())).expect("layout");
        {
            let mut store = Store::open(&layout).expect("store");
            store
                .create_task_with_job(
                    NewJob {
                        job_id: "job-setup-lock".to_owned(),
                        source_thread_id: "thread-setup-lock".to_owned(),
                        summary: "setup lock".to_owned(),
                        metadata_json: "{}".to_owned(),
                        policy: DeliveryPolicy::fail_closed(),
                        created_at: 10,
                    },
                    NewTask {
                        task_id: "task-setup-lock".to_owned(),
                        job_id: "job-setup-lock".to_owned(),
                        source_thread_id: "thread-setup-lock".to_owned(),
                        summary: "setup lock".to_owned(),
                        command_json: "[]".to_owned(),
                        cwd: home.path().display().to_string(),
                        timeout_seconds: Some(5),
                        max_delivery_attempts: 3,
                        redelivery_window_seconds: 60,
                        created_at: 10,
                    },
                )
                .expect("create task");
        }
        let lock = Connection::open(layout.db_path()).expect("lock connection");
        lock.execute_batch("PRAGMA locking_mode=EXCLUSIVE; BEGIN EXCLUSIVE;")
            .expect("hold exclusive lock");
        let marker = home.path().join("setup-lock-escaped");
        let marker_arg = marker.to_string_lossy().into_owned();
        let control = Arc::new(SupervisedTaskControl::default());
        let task_registry = Arc::new(Mutex::new(HashMap::from([(
            "task-setup-lock".to_owned(),
            Arc::clone(&control),
        )])));
        let worker_registry = Arc::clone(&task_registry);
        let worker_layout = layout.clone();
        let worker_cwd = home.path().to_path_buf();
        let worker_control = Arc::clone(&control);
        let started = Instant::now();

        let worker = thread::spawn(move || {
            run_supervised_task_worker(
                worker_layout,
                "task-setup-lock".to_owned(),
                "job-setup-lock".to_owned(),
                vec![
                    b"/bin/sh".to_vec(),
                    b"-c".to_vec(),
                    b"printf escaped > \"$1\"".to_vec(),
                    b"cbth-task".to_vec(),
                    marker_arg.into_bytes(),
                ],
                Vec::new(),
                worker_cwd,
                Some(1),
                3,
                60,
                worker_control,
                worker_registry,
            );
        });
        let spawn_observed_deadline = Instant::now() + Duration::from_secs(2);
        while supervised_task_process_snapshot(&control)
            .expect("process snapshot")
            .is_none()
            && !control.child_exit_observed.load(Ordering::Acquire)
            && Instant::now() < spawn_observed_deadline
        {
            thread::sleep(READ_POLL_INTERVAL);
        }
        assert!(
            supervised_task_process_snapshot(&control)
                .expect("process snapshot")
                .is_some()
                || control.child_exit_observed.load(Ordering::Acquire),
            "worker should reach the pre-exec started update while the store is locked"
        );
        thread::sleep(Duration::from_millis(500));
        assert!(
            task_registry
                .lock()
                .expect("task registry")
                .contains_key("task-setup-lock"),
            "worker must retain ownership until it writes a terminal task state"
        );
        assert!(
            !marker.exists(),
            "task command must not execute before the started update is durable"
        );
        assert!(
            started.elapsed() < Duration::from_secs(3),
            "started update should not leave the task unmonitored for the default SQLite busy timeout"
        );
        drop(lock);
        worker.join().expect("worker joins");
        assert!(
            !task_registry
                .lock()
                .expect("task registry")
                .contains_key("task-setup-lock"),
            "worker should release ownership after terminalizing the task"
        );
        let store = Store::open(&layout).expect("store");
        let task = store.inspect_task("task-setup-lock").expect("task");
        assert_eq!(task.status, "succeeded");
        let job = store.inspect_job("job-setup-lock").expect("job");
        assert_eq!(job.status, "completed");
        assert!(
            marker.exists(),
            "task process should execute after the started update succeeds"
        );
    }

    #[test]
    fn task_cancel_records_durable_cancel_before_spawn() {
        let (home, state) = test_state();
        {
            let mut store = Store::open(&state.layout).expect("store");
            store
                .create_task_with_job(
                    NewJob {
                        job_id: "job-cancel-before-spawn-race".to_owned(),
                        source_thread_id: "thread-cancel-before-spawn-race".to_owned(),
                        summary: "cancel before spawn race".to_owned(),
                        metadata_json: "{}".to_owned(),
                        policy: DeliveryPolicy::fail_closed(),
                        created_at: 10,
                    },
                    NewTask {
                        task_id: "task-cancel-before-spawn-race".to_owned(),
                        job_id: "job-cancel-before-spawn-race".to_owned(),
                        source_thread_id: "thread-cancel-before-spawn-race".to_owned(),
                        summary: "cancel before spawn race".to_owned(),
                        command_json: "[]".to_owned(),
                        cwd: home.path().display().to_string(),
                        timeout_seconds: None,
                        max_delivery_attempts: 3,
                        redelivery_window_seconds: 60,
                        created_at: 10,
                    },
                )
                .expect("create task");
        }
        let control = Arc::new(SupervisedTaskControl::default());
        state
            .supervised_tasks
            .lock()
            .expect("task registry")
            .insert(
                "task-cancel-before-spawn-race".to_owned(),
                Arc::clone(&control),
            );
        let state = Arc::new(state);
        let cancelled = cancel_supervised_task(
            &state,
            TaskCancelPayload {
                task_id: "task-cancel-before-spawn-race".to_owned(),
            },
        )
        .expect("cancel task");
        assert!(
            cancelled.cancel_requested_at.is_some(),
            "cancel should be durable before it is visible to the worker"
        );
        assert!(
            control.cancel_requested.load(Ordering::Acquire),
            "cancel flag should be visible after durable cancel is recorded"
        );
        let marker = home.path().join("cancel-before-spawn-race-escaped");
        let marker_arg = marker.to_string_lossy().into_owned();
        let worker_layout = state.layout.clone();
        let worker_registry = Arc::clone(&state.supervised_tasks);
        let worker_cwd = home.path().to_path_buf();
        let worker_control = Arc::clone(&control);
        let worker = thread::spawn(move || {
            run_supervised_task_worker(
                worker_layout,
                "task-cancel-before-spawn-race".to_owned(),
                "job-cancel-before-spawn-race".to_owned(),
                vec![
                    b"/bin/sh".to_vec(),
                    b"-c".to_vec(),
                    b"printf escaped > \"$1\"".to_vec(),
                    b"cbth-task".to_vec(),
                    marker_arg.into_bytes(),
                ],
                Vec::new(),
                worker_cwd,
                None,
                3,
                60,
                worker_control,
                worker_registry,
            );
        });

        worker.join().expect("worker joins");
        let store = Store::open(&state.layout).expect("store");
        let task = store
            .inspect_task("task-cancel-before-spawn-race")
            .expect("task");
        assert_eq!(task.status, "cancelled");
        let job = store
            .inspect_job("job-cancel-before-spawn-race")
            .expect("job");
        assert_eq!(job.status, "failed");
        assert!(
            !marker.exists(),
            "pre-spawn cancelled task command should never run"
        );
    }

    #[test]
    fn task_cancel_marks_intent_before_waiting_for_spawn_gate() {
        let (home, state) = test_state();
        {
            let mut store = Store::open(&state.layout).expect("store");
            store
                .create_task_with_job(
                    NewJob {
                        job_id: "job-cancel-spawn-gate".to_owned(),
                        source_thread_id: "thread-cancel-spawn-gate".to_owned(),
                        summary: "cancel spawn gate".to_owned(),
                        metadata_json: "{}".to_owned(),
                        policy: DeliveryPolicy::fail_closed(),
                        created_at: 10,
                    },
                    NewTask {
                        task_id: "task-cancel-spawn-gate".to_owned(),
                        job_id: "job-cancel-spawn-gate".to_owned(),
                        source_thread_id: "thread-cancel-spawn-gate".to_owned(),
                        summary: "cancel spawn gate".to_owned(),
                        command_json: "[]".to_owned(),
                        cwd: home.path().display().to_string(),
                        timeout_seconds: None,
                        max_delivery_attempts: 3,
                        redelivery_window_seconds: 60,
                        created_at: 10,
                    },
                )
                .expect("create task");
        }
        let control = Arc::new(SupervisedTaskControl::default());
        let spawn_guard = control.spawn_gate.lock().expect("spawn gate");
        state
            .supervised_tasks
            .lock()
            .expect("task registry")
            .insert("task-cancel-spawn-gate".to_owned(), Arc::clone(&control));
        let state = Arc::new(state);
        let cancel_state = Arc::clone(&state);
        let cancel = thread::spawn(move || {
            cancel_supervised_task(
                &cancel_state,
                TaskCancelPayload {
                    task_id: "task-cancel-spawn-gate".to_owned(),
                },
            )
        });

        let deadline = Instant::now() + Duration::from_secs(2);
        while !supervised_task_cancel_in_flight(&control) && Instant::now() < deadline {
            thread::sleep(Duration::from_millis(10));
        }
        assert!(
            supervised_task_cancel_in_flight(&control),
            "cancel should publish intent before blocking on spawn gate"
        );
        assert!(
            !control.cancel_requested.load(Ordering::Acquire),
            "durable cancel should still be pending while spawn gate is held"
        );
        drop(spawn_guard);

        let cancelled = cancel.join().expect("cancel thread").expect("cancel task");
        assert!(cancelled.cancel_requested_at.is_some());
        assert!(control.cancel_requested.load(Ordering::Acquire));
        assert!(!supervised_task_cancel_in_flight(&control));
    }

    #[test]
    fn task_cancel_after_child_exit_does_not_mark_successful_task_cancelled() {
        let (home, state) = test_state();
        let mut command = Command::new("/bin/sh");
        command.arg("-c").arg("true").process_group(0);
        let mut child = spawn_command_locked(&mut command).expect("spawn child");
        let pid = child.id();
        child.wait().expect("wait child");
        let control = Arc::new(SupervisedTaskControl::default());
        mark_supervised_task_process_completing(&control).expect("mark completing");
        state
            .supervised_tasks
            .lock()
            .expect("task registry")
            .insert("task-late-cancel".to_owned(), Arc::clone(&control));
        {
            let mut store = Store::open(&state.layout).expect("store");
            store
                .create_task_with_job(
                    NewJob {
                        job_id: "job-late-cancel".to_owned(),
                        source_thread_id: "thread-late-cancel".to_owned(),
                        summary: "late cancel".to_owned(),
                        metadata_json: "{}".to_owned(),
                        policy: DeliveryPolicy::fail_closed(),
                        created_at: 10,
                    },
                    NewTask {
                        task_id: "task-late-cancel".to_owned(),
                        job_id: "job-late-cancel".to_owned(),
                        source_thread_id: "thread-late-cancel".to_owned(),
                        summary: "late cancel".to_owned(),
                        command_json: "[]".to_owned(),
                        cwd: home.path().display().to_string(),
                        timeout_seconds: None,
                        max_delivery_attempts: 3,
                        redelivery_window_seconds: 60,
                        created_at: 10,
                    },
                )
                .expect("create task");
            store
                .mark_task_started("task-late-cancel", i64::from(pid), None, 11)
                .expect("mark started");
        }

        let task = cancel_supervised_task(
            &state,
            TaskCancelPayload {
                task_id: "task-late-cancel".to_owned(),
            },
        )
        .expect("cancel after exit");

        assert_eq!(task.status, "running");
        assert_eq!(task.cancel_requested_at, None);
        assert!(!control.cancel_requested.load(Ordering::Acquire));
        assert!(!control.cancel_signal_sent.load(Ordering::Acquire));
    }

    #[test]
    fn daemon_stop_does_not_persist_cancel_for_completing_task() {
        let (home, state) = test_state();
        let control = Arc::new(SupervisedTaskControl::default());
        mark_supervised_task_process_completing(&control).expect("mark completing");
        state
            .supervised_tasks
            .lock()
            .expect("task registry")
            .insert("task-stop-completing".to_owned(), control);
        {
            let mut store = Store::open(&state.layout).expect("store");
            store
                .create_task_with_job(
                    NewJob {
                        job_id: "job-stop-completing".to_owned(),
                        source_thread_id: "thread-stop-completing".to_owned(),
                        summary: "stop completing".to_owned(),
                        metadata_json: "{}".to_owned(),
                        policy: DeliveryPolicy::fail_closed(),
                        created_at: 10,
                    },
                    NewTask {
                        task_id: "task-stop-completing".to_owned(),
                        job_id: "job-stop-completing".to_owned(),
                        source_thread_id: "thread-stop-completing".to_owned(),
                        summary: "stop completing".to_owned(),
                        command_json: "[]".to_owned(),
                        cwd: home.path().display().to_string(),
                        timeout_seconds: None,
                        max_delivery_attempts: 3,
                        redelivery_window_seconds: 60,
                        created_at: 10,
                    },
                )
                .expect("create task");
            store
                .mark_task_started("task-stop-completing", 12345, Some("test-pid"), 11)
                .expect("mark started");
        }
        let registry = state.supervised_tasks.clone();
        let worker = thread::spawn(move || {
            thread::sleep(Duration::from_millis(100));
            registry
                .lock()
                .expect("task registry")
                .remove("task-stop-completing");
        });

        stop_all_supervised_tasks(&state);
        worker.join().expect("join completing worker");

        let store = Store::open(&state.layout).expect("store");
        let task = store.inspect_task("task-stop-completing").expect("task");
        assert_eq!(task.cancel_requested_at, None);
        assert_eq!(task.status, "running");
    }

    #[test]
    fn daemon_stop_blocks_exec_release_before_durable_shutdown_cancel() {
        let (home, state) = test_state();
        let state = Arc::new(state);
        let control = Arc::new(SupervisedTaskControl::default());
        {
            let mut store = Store::open(&state.layout).expect("store");
            store
                .create_task_with_job(
                    NewJob {
                        job_id: "job-stop-memory-cancel".to_owned(),
                        source_thread_id: "thread-stop-memory-cancel".to_owned(),
                        summary: "stop memory cancel".to_owned(),
                        metadata_json: "{}".to_owned(),
                        policy: DeliveryPolicy::fail_closed(),
                        created_at: 10,
                    },
                    NewTask {
                        task_id: "task-stop-memory-cancel".to_owned(),
                        job_id: "job-stop-memory-cancel".to_owned(),
                        source_thread_id: "thread-stop-memory-cancel".to_owned(),
                        summary: "stop memory cancel".to_owned(),
                        command_json: "[]".to_owned(),
                        cwd: home.path().display().to_string(),
                        timeout_seconds: None,
                        max_delivery_attempts: 3,
                        redelivery_window_seconds: 60,
                        created_at: 10,
                    },
                )
                .expect("create task");
        }
        state
            .supervised_tasks
            .lock()
            .expect("task registry")
            .insert("task-stop-memory-cancel".to_owned(), Arc::clone(&control));
        let lock = Connection::open(state.layout.db_path()).expect("lock connection");
        lock.execute_batch("PRAGMA locking_mode=EXCLUSIVE; BEGIN EXCLUSIVE;")
            .expect("hold exclusive lock");
        let stop_state = Arc::clone(&state);
        let stopper = thread::spawn(move || stop_all_supervised_tasks(&stop_state));

        let deadline = Instant::now() + Duration::from_millis(750);
        while !control.exec_release_blocked.load(Ordering::Acquire) && Instant::now() < deadline {
            thread::sleep(Duration::from_millis(20));
        }

        assert!(
            control.exec_release_blocked.load(Ordering::Acquire),
            "daemon stop must block exec release in memory while durable shutdown cancel is pending"
        );
        assert!(
            !control.cancel_requested.load(Ordering::Acquire),
            "daemon stop must not publish signal-capable cancel state before durable shutdown cancel succeeds"
        );
        state
            .supervised_tasks
            .lock()
            .expect("task registry")
            .remove("task-stop-memory-cancel");
        drop(lock);
        stopper.join().expect("stop joins");
    }

    #[test]
    fn daemon_stop_does_not_signal_before_durable_shutdown_cancel() {
        let (home, state) = test_state();
        let state = Arc::new(state);
        let marker = home.path().join("shutdown-cancel-before-durable-signal");
        let marker_arg = marker.to_string_lossy().into_owned();
        let mut command = Command::new("/bin/sh");
        command
            .arg("-c")
            .arg("trap 'printf term > \"$1\"; exit 0' TERM; while :; do sleep 1; done")
            .arg("cbth-task")
            .arg(&marker_arg)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .process_group(0);
        let mut child = spawn_command_locked(&mut command).expect("spawn child");
        let pid = child.id();
        let pid_identity = process_start_identity(pid)
            .expect("process identity")
            .expect("process identity available");
        let control = Arc::new(SupervisedTaskControl::default());
        set_supervised_task_process(&control, pid, pid_identity.clone()).expect("set process");
        {
            let mut store = Store::open(&state.layout).expect("store");
            store
                .create_task_with_job(
                    NewJob {
                        job_id: "job-stop-durable-before-signal".to_owned(),
                        source_thread_id: "thread-stop-durable-before-signal".to_owned(),
                        summary: "stop durable before signal".to_owned(),
                        metadata_json: "{}".to_owned(),
                        policy: DeliveryPolicy::fail_closed(),
                        created_at: 10,
                    },
                    NewTask {
                        task_id: "task-stop-durable-before-signal".to_owned(),
                        job_id: "job-stop-durable-before-signal".to_owned(),
                        source_thread_id: "thread-stop-durable-before-signal".to_owned(),
                        summary: "stop durable before signal".to_owned(),
                        command_json: "[]".to_owned(),
                        cwd: home.path().display().to_string(),
                        timeout_seconds: None,
                        max_delivery_attempts: 3,
                        redelivery_window_seconds: 60,
                        created_at: 10,
                    },
                )
                .expect("create task");
            store
                .mark_task_started(
                    "task-stop-durable-before-signal",
                    i64::from(pid),
                    Some(&pid_identity),
                    11,
                )
                .expect("mark started");
        }
        state
            .supervised_tasks
            .lock()
            .expect("task registry")
            .insert(
                "task-stop-durable-before-signal".to_owned(),
                Arc::clone(&control),
            );
        let lock = Connection::open(state.layout.db_path()).expect("lock connection");
        lock.execute_batch("PRAGMA locking_mode=EXCLUSIVE; BEGIN EXCLUSIVE;")
            .expect("hold exclusive lock");
        let stop_state = Arc::clone(&state);
        let stopper = thread::spawn(move || stop_all_supervised_tasks(&stop_state));

        let deadline = Instant::now() + Duration::from_millis(750);
        while !control.exec_release_blocked.load(Ordering::Acquire) && Instant::now() < deadline {
            thread::sleep(Duration::from_millis(20));
        }
        thread::sleep(Duration::from_millis(200));

        assert!(control.exec_release_blocked.load(Ordering::Acquire));
        assert!(!control.cancel_requested.load(Ordering::Acquire));
        assert!(!control.cancel_signal_sent.load(Ordering::Acquire));
        assert!(process_group_exists(pid));
        assert!(
            !marker.exists(),
            "daemon stop must not signal the task before cancel is durable"
        );
        drop(lock);
        stopper.join().expect("stop joins");
        let _ = child.wait();
    }

    #[test]
    fn daemon_stop_forces_shutdown_after_unpersisted_cancel_deadline() {
        let (home, state) = test_state();
        let state = Arc::new(state);
        let mut command = Command::new("/bin/sh");
        command
            .arg("-c")
            .arg("trap '' TERM; while :; do sleep 1; done")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .process_group(0);
        let mut child = spawn_command_locked(&mut command).expect("spawn child");
        let pid = child.id();
        let pid_identity = process_start_identity(pid)
            .expect("process identity")
            .expect("process identity available");
        let control = Arc::new(SupervisedTaskControl::default());
        set_supervised_task_process(&control, pid, pid_identity.clone()).expect("set process");
        {
            let mut store = Store::open(&state.layout).expect("store");
            store
                .create_task_with_job(
                    NewJob {
                        job_id: "job-stop-unpersisted-cancel".to_owned(),
                        source_thread_id: "thread-stop-unpersisted-cancel".to_owned(),
                        summary: "stop unpersisted cancel".to_owned(),
                        metadata_json: "{}".to_owned(),
                        policy: DeliveryPolicy::fail_closed(),
                        created_at: 10,
                    },
                    NewTask {
                        task_id: "task-stop-unpersisted-cancel".to_owned(),
                        job_id: "job-stop-unpersisted-cancel".to_owned(),
                        source_thread_id: "thread-stop-unpersisted-cancel".to_owned(),
                        summary: "stop unpersisted cancel".to_owned(),
                        command_json: "[]".to_owned(),
                        cwd: home.path().display().to_string(),
                        timeout_seconds: None,
                        max_delivery_attempts: 3,
                        redelivery_window_seconds: 60,
                        created_at: 10,
                    },
                )
                .expect("create task");
            store
                .mark_task_started(
                    "task-stop-unpersisted-cancel",
                    i64::from(pid),
                    Some(&pid_identity),
                    11,
                )
                .expect("mark started");
        }
        state
            .supervised_tasks
            .lock()
            .expect("task registry")
            .insert(
                "task-stop-unpersisted-cancel".to_owned(),
                Arc::clone(&control),
            );
        let lock = Connection::open(state.layout.db_path()).expect("lock connection");
        lock.execute_batch("PRAGMA locking_mode=EXCLUSIVE; BEGIN EXCLUSIVE;")
            .expect("hold exclusive lock");

        let started = Instant::now();
        stop_all_supervised_tasks(&state);
        let elapsed = started.elapsed();
        drop(lock);
        state
            .supervised_tasks
            .lock()
            .expect("task registry")
            .remove("task-stop-unpersisted-cancel");
        let status = child.wait().expect("wait child");

        assert!(
            control.cancel_requested.load(Ordering::Acquire),
            "shutdown must eventually force an in-memory cancel after durable cancel remains blocked"
        );
        assert_eq!(status.signal(), Some(libc::SIGKILL));
        assert!(
            elapsed
                < TASK_SHUTDOWN_DURABLE_CANCEL_GRACE
                    + TASK_SHUTDOWN_UNPERSISTED_CANCEL_GRACE
                    + TASK_TERM_GRACE
                    + TASK_STREAM_DRAIN_HARD_GRACE
                    + TASK_SHUTDOWN_WORKER_DRAIN_GRACE
                    + Duration::from_secs(3),
            "shutdown loop did not use the bounded unpersisted-cancel fallback: {elapsed:?}"
        );
    }

    #[test]
    fn task_cancel_does_not_signal_pid_identity_mismatch() {
        let (home, state) = test_state();
        let mut command = Command::new("/bin/sh");
        command
            .arg("-c")
            .arg("while :; do sleep 1; done")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .process_group(0);
        let mut child = spawn_command_locked(&mut command).expect("spawn child");
        let pid = child.id();
        let control = Arc::new(SupervisedTaskControl::default());
        set_supervised_task_process(&control, pid, "different-process-identity".to_owned())
            .expect("set mismatched process");
        state
            .supervised_tasks
            .lock()
            .expect("task registry")
            .insert(
                "task-mismatched-pid-cancel".to_owned(),
                Arc::clone(&control),
            );
        {
            let mut store = Store::open(&state.layout).expect("store");
            store
                .create_task_with_job(
                    NewJob {
                        job_id: "job-mismatched-pid-cancel".to_owned(),
                        source_thread_id: "thread-mismatched-pid-cancel".to_owned(),
                        summary: "mismatched pid cancel".to_owned(),
                        metadata_json: "{}".to_owned(),
                        policy: DeliveryPolicy::fail_closed(),
                        created_at: 10,
                    },
                    NewTask {
                        task_id: "task-mismatched-pid-cancel".to_owned(),
                        job_id: "job-mismatched-pid-cancel".to_owned(),
                        source_thread_id: "thread-mismatched-pid-cancel".to_owned(),
                        summary: "mismatched pid cancel".to_owned(),
                        command_json: "[]".to_owned(),
                        cwd: home.path().display().to_string(),
                        timeout_seconds: None,
                        max_delivery_attempts: 3,
                        redelivery_window_seconds: 60,
                        created_at: 10,
                    },
                )
                .expect("create task");
            store
                .mark_task_started(
                    "task-mismatched-pid-cancel",
                    i64::from(pid),
                    Some("different-process-identity"),
                    11,
                )
                .expect("mark started");
        }

        let task = cancel_supervised_task(
            &state,
            TaskCancelPayload {
                task_id: "task-mismatched-pid-cancel".to_owned(),
            },
        )
        .expect("cancel mismatched pid");

        assert_eq!(task.status, "running");
        assert!(!control.cancel_signal_sent.load(Ordering::Acquire));
        assert!(
            child.try_wait().expect("check child").is_none(),
            "mismatched pid process should not be signaled"
        );
        let _ = signal_process_group(pid, libc::SIGKILL);
        let _ = child.wait();
    }

    #[test]
    fn lifecycle_recovery_fails_only_registryless_lost_tasks() {
        let (home, state) = test_state();
        {
            let mut store = Store::open(&state.layout).expect("store");
            for (task_id, job_id, thread_id) in [
                (
                    "task-active-registry",
                    "job-active-registry",
                    "thread-active-registry",
                ),
                (
                    "task-registryless",
                    "job-registryless",
                    "thread-registryless",
                ),
            ] {
                store
                    .create_task_with_job(
                        NewJob {
                            job_id: job_id.to_owned(),
                            source_thread_id: thread_id.to_owned(),
                            summary: task_id.to_owned(),
                            metadata_json: "{}".to_owned(),
                            policy: DeliveryPolicy::fail_closed(),
                            created_at: 10,
                        },
                        NewTask {
                            task_id: task_id.to_owned(),
                            job_id: job_id.to_owned(),
                            source_thread_id: thread_id.to_owned(),
                            summary: task_id.to_owned(),
                            command_json: "[]".to_owned(),
                            cwd: home.path().display().to_string(),
                            timeout_seconds: None,
                            max_delivery_attempts: 3,
                            redelivery_window_seconds: 60,
                            created_at: 10,
                        },
                    )
                    .expect("create task");
                store
                    .mark_task_started(task_id, 12345, Some("test-pid"), 11)
                    .expect("mark started");
            }
        }
        state
            .supervised_tasks
            .lock()
            .expect("task registry")
            .insert(
                "task-active-registry".to_owned(),
                Arc::new(SupervisedTaskControl::default()),
            );

        let recovered = recover_registryless_lost_tasks(&state).expect("recover lost tasks");

        assert_eq!(recovered, 1);
        let store = Store::open(&state.layout).expect("store");
        let active = store.inspect_task("task-active-registry").expect("active");
        assert_eq!(active.status, "running");
        let registryless = store.inspect_task("task-registryless").expect("lost");
        assert_eq!(registryless.status, "lost");
        let active_job = store
            .inspect_job("job-active-registry")
            .expect("active job");
        assert_eq!(active_job.status, "pending");
        let registryless_job = store.inspect_job("job-registryless").expect("lost job");
        assert_eq!(registryless_job.status, "failed");
    }

    #[test]
    fn lifecycle_recovery_terminates_registryless_lost_task_process_group() {
        let (home, state) = test_state();
        let mut command = Command::new("/bin/sh");
        command.arg("-c").arg("sleep 30").process_group(0);
        let mut child = spawn_command_locked(&mut command).expect("spawn child");
        let pid = child.id();
        let pid_identity = process_start_identity(pid)
            .expect("process identity")
            .expect("process identity available");
        {
            let mut store = Store::open(&state.layout).expect("store");
            store
                .create_task_with_job(
                    NewJob {
                        job_id: "job-registryless-process".to_owned(),
                        source_thread_id: "thread-registryless-process".to_owned(),
                        summary: "registryless process".to_owned(),
                        metadata_json: "{}".to_owned(),
                        policy: DeliveryPolicy::fail_closed(),
                        created_at: 10,
                    },
                    NewTask {
                        task_id: "task-registryless-process".to_owned(),
                        job_id: "job-registryless-process".to_owned(),
                        source_thread_id: "thread-registryless-process".to_owned(),
                        summary: "registryless process".to_owned(),
                        command_json: "[]".to_owned(),
                        cwd: home.path().display().to_string(),
                        timeout_seconds: None,
                        max_delivery_attempts: 3,
                        redelivery_window_seconds: 60,
                        created_at: 10,
                    },
                )
                .expect("create task");
            store
                .mark_task_started(
                    "task-registryless-process",
                    i64::from(pid),
                    Some(&pid_identity),
                    11,
                )
                .expect("mark task started");
        }

        let recovered = recover_registryless_lost_tasks(&state).expect("recover lost task");

        assert_eq!(recovered, 1);
        let deadline = Instant::now() + Duration::from_secs(2);
        let mut exited = false;
        while Instant::now() < deadline {
            if child.try_wait().expect("poll child").is_some() {
                exited = true;
                break;
            }
            thread::sleep(TASK_WAIT_POLL_INTERVAL);
        }
        if !exited {
            terminate_task_child_best_effort(&mut child, pid);
        }
        let _ = child.wait();
        assert!(exited, "registryless recovery should terminate task child");
        assert!(
            !process_group_exists(pid),
            "registryless recovery should clean up the task process group"
        );
        let store = Store::open(&state.layout).expect("store");
        let task = store
            .inspect_task("task-registryless-process")
            .expect("inspect task");
        assert_eq!(task.status, "lost");
    }

    #[test]
    fn lifecycle_maintenance_recovers_nonterminal_task_after_job_closed() {
        let (home, state) = test_state();
        let state = Arc::new(state);
        {
            let mut store = Store::open(&state.layout).expect("store");
            store
                .create_task_with_job(
                    NewJob {
                        job_id: "job-closed-before-task".to_owned(),
                        source_thread_id: "thread-closed-before-task".to_owned(),
                        summary: "closed before task".to_owned(),
                        metadata_json: "{}".to_owned(),
                        policy: DeliveryPolicy::fail_closed(),
                        created_at: 10,
                    },
                    NewTask {
                        task_id: "task-closed-before-job".to_owned(),
                        job_id: "job-closed-before-task".to_owned(),
                        source_thread_id: "thread-closed-before-task".to_owned(),
                        summary: "closed before task".to_owned(),
                        command_json: "[]".to_owned(),
                        cwd: home.path().display().to_string(),
                        timeout_seconds: None,
                        max_delivery_attempts: 3,
                        redelivery_window_seconds: 60,
                        created_at: 10,
                    },
                )
                .expect("create task");
            store
                .mark_task_started("task-closed-before-job", 12345, Some("test-pid"), 11)
                .expect("mark task started");
            store
                .fail_job("job-closed-before-task", "task cancelled", 12, 3, 60)
                .expect("fail job");
        }

        let status = refresh_lifecycle_status(&state, 100).expect("refresh lifecycle");
        assert_eq!(status.active_jobs, 0);
        assert_eq!(status.nonterminal_tasks, 1);
        let cache = DaemonLifecycleCache {
            refreshed_at: Some(Instant::now()),
            refresh_failed: false,
            status,
        };
        let mut worker = None;
        let mut next_attempt_at = Instant::now();

        maybe_spawn_lifecycle_maintenance(&state, &cache, &mut worker, &mut next_attempt_at);
        join_lifecycle_maintenance(worker);

        let store = Store::open(&state.layout).expect("store");
        let task = store.inspect_task("task-closed-before-job").expect("task");
        assert_eq!(task.status, "cancelled");
        assert_eq!(task.failure_reason.as_deref(), Some("task cancelled"));
    }

    #[test]
    fn task_worker_spawn_error_fails_closed_before_db_create() {
        let (home, state) = test_state();
        let payload = TaskRunPayload {
            source_thread_id: "thread-worker-spawn-fail".to_owned(),
            summary: "worker spawn failure".to_owned(),
            metadata_json: "{}".to_owned(),
            policy: DeliveryPolicy::fail_closed(),
            cwd: home.path().as_os_str().as_bytes().to_vec(),
            cwd_display: home.path().display().to_string(),
            timeout_seconds: None,
            max_delivery_attempts: 3,
            redelivery_window_seconds: 60,
            command: vec![b"/bin/true".to_vec()],
            environment: Vec::new(),
        };

        let error = start_supervised_task_with_spawner(&state, payload, |_name, _worker| {
            Err(io::Error::from_raw_os_error(libc::EAGAIN))
        })
        .expect_err("worker spawn should fail");

        assert!(
            error.to_string().contains("spawn task supervisor worker"),
            "{error:#}"
        );
        assert!(
            state
                .supervised_tasks
                .lock()
                .expect("task registry")
                .is_empty(),
            "failed worker spawn should release registry slot"
        );
        let store = Store::open(&state.layout).expect("store");
        let tasks = store
            .list_tasks(Some("thread-worker-spawn-fail"), None, 10)
            .expect("list tasks");
        assert!(tasks.is_empty(), "spawn failure should not create a task");
    }

    #[test]
    fn task_run_persists_raw_command_bytes() {
        let (home, state) = test_state();
        let command = vec![
            b"/bin/sh".to_vec(),
            b"-c".to_vec(),
            b"true".to_vec(),
            vec![b'a', 0xFF, b'z'],
        ];
        let payload = TaskRunPayload {
            source_thread_id: "thread-raw-command-bytes".to_owned(),
            summary: "raw command bytes".to_owned(),
            metadata_json: "{}".to_owned(),
            policy: DeliveryPolicy::fail_closed(),
            cwd: home.path().as_os_str().as_bytes().to_vec(),
            cwd_display: home.path().display().to_string(),
            timeout_seconds: None,
            max_delivery_attempts: 3,
            redelivery_window_seconds: 60,
            command: command.clone(),
            environment: Vec::new(),
        };

        let task = start_supervised_task_with_spawner(&state, payload, |name, worker| {
            thread::Builder::new().name(name).spawn(worker)
        })
        .expect("start task");

        assert_eq!(task.command["format"], json!("argv-bytes-v1"));
        assert_eq!(task.command["argv"], json!(command));
        assert!(task.command["display"].is_string());
        wait_for_supervised_task_registry_empty(&state, Duration::from_secs(2));
    }

    #[test]
    fn task_run_initial_store_lock_fails_after_bounded_setup_retry_and_releases_worker() {
        let (home, state) = test_state();
        Store::open(&state.layout).expect("initialize store");
        let lock = Connection::open(state.layout.db_path()).expect("lock connection");
        lock.execute_batch("PRAGMA locking_mode=EXCLUSIVE; BEGIN EXCLUSIVE;")
            .expect("hold exclusive db lock");
        let payload = TaskRunPayload {
            source_thread_id: "thread-task-run-initial-store-lock".to_owned(),
            summary: "initial store lock".to_owned(),
            metadata_json: "{}".to_owned(),
            policy: DeliveryPolicy::fail_closed(),
            cwd: home.path().as_os_str().as_bytes().to_vec(),
            cwd_display: home.path().display().to_string(),
            timeout_seconds: None,
            max_delivery_attempts: 3,
            redelivery_window_seconds: 60,
            command: vec![b"/bin/true".to_vec()],
            environment: Vec::new(),
        };
        let worker_done = Arc::new(AtomicBool::new(false));
        let worker_done_for_spawner = Arc::clone(&worker_done);

        let started = Instant::now();
        let error = start_supervised_task_with_spawner(&state, payload, |_name, worker| {
            Ok(thread::spawn(move || {
                worker();
                worker_done_for_spawner.store(true, Ordering::Release);
            }))
        })
        .expect_err("locked initial task store should fail");

        assert!(
            started.elapsed() < Duration::from_secs(8),
            "initial task store open should use the bounded setup retry, not the default SQLite busy timeout"
        );
        let error_details = format!("{error:#}");
        assert!(
            error_details.contains("database is locked")
                || error_details.contains("database file is locked")
                || error_details.contains("database table is locked"),
            "{error_details}"
        );
        let worker_deadline = Instant::now() + Duration::from_secs(2);
        while !worker_done.load(Ordering::Acquire) {
            assert!(
                Instant::now() < worker_deadline,
                "worker did not exit after start signal was dropped"
            );
            thread::sleep(Duration::from_millis(20));
        }
        assert!(
            state
                .supervised_tasks
                .lock()
                .expect("task registry")
                .is_empty()
        );
        drop(lock);
        let store = Store::open(&state.layout).expect("store");
        let tasks = store
            .list_tasks(Some("thread-task-run-initial-store-lock"), None, 10)
            .expect("list tasks");
        assert!(
            tasks.is_empty(),
            "failed initial store create should not create a task"
        );
    }

    #[test]
    fn unstarted_task_cleanup_removes_job_before_worker_start() {
        let (home, state) = test_state();
        let mut store = Store::open(&state.layout).expect("store");
        store
            .create_task_with_job(
                NewJob {
                    job_id: "job-unstarted-cleanup".to_owned(),
                    source_thread_id: "thread-unstarted-cleanup".to_owned(),
                    summary: "unstarted cleanup".to_owned(),
                    metadata_json: "{}".to_owned(),
                    policy: DeliveryPolicy::fail_closed(),
                    created_at: 10,
                },
                NewTask {
                    task_id: "task-unstarted-cleanup".to_owned(),
                    job_id: "job-unstarted-cleanup".to_owned(),
                    source_thread_id: "thread-unstarted-cleanup".to_owned(),
                    summary: "unstarted cleanup".to_owned(),
                    command_json: "[]".to_owned(),
                    cwd: home.path().display().to_string(),
                    timeout_seconds: None,
                    max_delivery_attempts: 3,
                    redelivery_window_seconds: 60,
                    created_at: 10,
                },
            )
            .expect("create task");
        store
            .request_task_cancel("task-unstarted-cleanup", 11)
            .expect("mark unstarted task cancelled during shutdown");

        store
            .delete_unstarted_task_with_job("task-unstarted-cleanup", "job-unstarted-cleanup")
            .expect("delete unstarted task");

        assert!(
            store.inspect_task("task-unstarted-cleanup").is_err(),
            "unstarted task should be removed instead of becoming a hidden failed task"
        );
        assert!(
            store.inspect_job("job-unstarted-cleanup").is_err(),
            "unstarted job should be removed with its task"
        );
    }

    #[test]
    fn task_run_blocked_on_registry_lock_fails_after_stop_request() {
        let (home, state) = test_state();
        let state = Arc::new(state);
        let registry_guard = state.supervised_tasks.lock().expect("task registry");
        let spawner_called = Arc::new(AtomicBool::new(false));
        let spawner_called_for_task = Arc::clone(&spawner_called);
        let task_state = Arc::clone(&state);
        let payload = TaskRunPayload {
            source_thread_id: "thread-stop-fenced-task-run".to_owned(),
            summary: "stop fenced task run".to_owned(),
            metadata_json: "{}".to_owned(),
            policy: DeliveryPolicy::fail_closed(),
            cwd: home.path().as_os_str().as_bytes().to_vec(),
            cwd_display: home.path().display().to_string(),
            timeout_seconds: None,
            max_delivery_attempts: 3,
            redelivery_window_seconds: 60,
            command: vec![b"/bin/true".to_vec()],
            environment: Vec::new(),
        };
        let task = thread::spawn(move || {
            start_supervised_task_with_spawner(&task_state, payload, |_name, _worker| {
                spawner_called_for_task.store(true, Ordering::Release);
                Err(io::Error::from_raw_os_error(libc::EAGAIN))
            })
        });

        thread::sleep(Duration::from_millis(100));
        state.stop_requested.store(true, Ordering::Release);
        drop(registry_guard);
        let error = task
            .join()
            .expect("task runner joins")
            .expect_err("task run");

        assert!(
            error.to_string().contains("daemon is stopping"),
            "{error:#}"
        );
        assert!(!spawner_called.load(Ordering::Acquire));
        assert!(
            state
                .supervised_tasks
                .lock()
                .expect("task registry")
                .is_empty()
        );
        let store = Store::open(&state.layout).expect("store");
        let tasks = store
            .list_tasks(Some("thread-stop-fenced-task-run"), None, 10)
            .expect("list tasks");
        assert!(tasks.is_empty(), "stopped daemon should not create task");
    }

    #[test]
    fn daemon_stop_waits_for_completing_task_worker() {
        let (_home, state) = test_state();
        let control = Arc::new(SupervisedTaskControl::default());
        mark_supervised_task_process_completing(&control).expect("mark completing");
        state
            .supervised_tasks
            .lock()
            .expect("task registry")
            .insert("task-completing-drain".to_owned(), control);
        let registry = state.supervised_tasks.clone();
        let worker = thread::spawn(move || {
            thread::sleep(TASK_SHUTDOWN_WORKER_DRAIN_GRACE + Duration::from_millis(500));
            registry
                .lock()
                .expect("task registry")
                .remove("task-completing-drain");
        });

        let started = Instant::now();
        stop_all_supervised_tasks(&state);
        let elapsed = started.elapsed();
        worker.join().expect("join completing worker");

        assert!(
            elapsed >= TASK_SHUTDOWN_WORKER_DRAIN_GRACE,
            "stop returned before the completing worker had enough time to finish"
        );
        assert!(
            state
                .supervised_tasks
                .lock()
                .expect("task registry")
                .is_empty()
        );
    }

    #[test]
    fn daemon_stop_waits_for_observed_child_exit_before_completing_state() {
        let (_home, state) = test_state();
        let mut command = Command::new("/bin/sh");
        command
            .arg("-c")
            .arg("true")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .process_group(0);
        let mut child = spawn_command_locked(&mut command).expect("spawn child");
        let pid = child.id();
        let pid_identity = process_start_identity(pid)
            .expect("process identity")
            .expect("process identity available");
        let control = Arc::new(SupervisedTaskControl::default());
        set_supervised_task_process(&control, pid, pid_identity).expect("set process");
        assert_eq!(
            wait_child_observed_until(pid, Instant::now() + Duration::from_secs(5)),
            ChildStatusWithoutReaping::Exited
        );
        mark_supervised_task_child_exit_observed(&control);
        assert!(matches!(
            supervised_task_process_state(&control).expect("process state"),
            SupervisedTaskProcessState::Running(_)
        ));
        state
            .supervised_tasks
            .lock()
            .expect("task registry")
            .insert(
                "task-observed-child-exit-drain".to_owned(),
                Arc::clone(&control),
            );
        let registry = state.supervised_tasks.clone();
        let removed = Arc::new(AtomicBool::new(false));
        let removed_for_worker = Arc::clone(&removed);
        let worker = thread::spawn(move || {
            thread::sleep(TASK_SHUTDOWN_WORKER_DRAIN_GRACE + Duration::from_millis(500));
            registry
                .lock()
                .expect("task registry")
                .remove("task-observed-child-exit-drain");
            removed_for_worker.store(true, Ordering::Release);
        });

        stop_all_supervised_tasks(&state);
        assert!(
            removed.load(Ordering::Acquire),
            "stop returned before the child-exited task worker finished terminalization"
        );
        worker.join().expect("join terminalization worker");
        child.wait().expect("wait child");
    }

    #[test]
    fn daemon_stop_waits_for_child_exited_task_store_completion() {
        let (home, state) = test_state();
        let state = Arc::new(state);
        let task_id = "task-child-exited-store-completion".to_owned();
        let job_id = "job-child-exited-store-completion".to_owned();
        {
            let mut store = Store::open(&state.layout).expect("store");
            store
                .create_task_with_job(
                    NewJob {
                        job_id: job_id.clone(),
                        source_thread_id: "thread-child-exited-store-completion".to_owned(),
                        summary: "child exited store completion".to_owned(),
                        metadata_json: "{}".to_owned(),
                        policy: DeliveryPolicy::fail_closed(),
                        created_at: 10,
                    },
                    NewTask {
                        task_id: task_id.clone(),
                        job_id: job_id.clone(),
                        source_thread_id: "thread-child-exited-store-completion".to_owned(),
                        summary: "child exited store completion".to_owned(),
                        command_json: "[\"/bin/sh\",\"-c\",\"true\"]".to_owned(),
                        cwd: home.path().display().to_string(),
                        timeout_seconds: None,
                        max_delivery_attempts: 3,
                        redelivery_window_seconds: 60,
                        created_at: 10,
                    },
                )
                .expect("create task");
        }
        let task_dir = state.layout.task_dir(&task_id);
        ensure_private_dir(&task_dir).expect("task dir");
        let stdout_path = task_dir.join("stdout.log");
        let stderr_path = task_dir.join("stderr.log");
        let result_path = task_dir.join("result.txt");
        let mut command = Command::new("/bin/sh");
        command
            .arg("-c")
            .arg("true")
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .process_group(0);
        let mut child = spawn_command_locked(&mut command).expect("spawn child");
        let pid = child.id();
        let pid_identity = process_start_identity(pid)
            .expect("process identity")
            .expect("process identity available");
        let control = Arc::new(SupervisedTaskControl::default());
        set_supervised_task_process(&control, pid, pid_identity.clone()).expect("set process");
        {
            let mut store = Store::open(&state.layout).expect("store");
            store
                .mark_task_started(&task_id, i64::from(pid), Some(&pid_identity), 11)
                .expect("mark started");
        }
        state
            .supervised_tasks
            .lock()
            .expect("task registry")
            .insert(task_id.clone(), Arc::clone(&control));
        let lock = Connection::open(state.layout.db_path()).expect("lock connection");
        lock.execute_batch("PRAGMA locking_mode=EXCLUSIVE; BEGIN EXCLUSIVE;")
            .expect("hold exclusive lock");
        let worker_layout = state.layout.clone();
        let worker_registry = state.supervised_tasks.clone();
        let worker_control = Arc::clone(&control);
        let worker_task_id = task_id.clone();
        let worker_job_id = job_id.clone();
        let worker_cwd = home.path().to_path_buf();
        let worker = thread::spawn(move || {
            let result = run_supervised_spawned_task(
                &worker_layout,
                &worker_task_id,
                &worker_job_id,
                "/bin/sh -c true",
                &worker_cwd,
                None,
                3,
                60,
                &worker_control,
                &mut child,
                pid,
                Instant::now(),
                stdout_path,
                stderr_path,
                result_path,
            );
            worker_registry
                .lock()
                .expect("task registry")
                .remove(&worker_task_id);
            result
        });
        let observed_deadline = Instant::now() + Duration::from_secs(5);
        while !control.child_exit_observed.load(Ordering::Acquire) {
            assert!(
                Instant::now() < observed_deadline,
                "worker did not observe child exit before stop"
            );
            thread::sleep(Duration::from_millis(20));
        }

        let stop_state = Arc::clone(&state);
        let stop_done = Arc::new(AtomicBool::new(false));
        let stop_done_for_thread = Arc::clone(&stop_done);
        let stopper = thread::spawn(move || {
            stop_all_supervised_tasks(&stop_state);
            stop_done_for_thread.store(true, Ordering::Release);
        });
        thread::sleep(TASK_SHUTDOWN_WORKER_DRAIN_GRACE + Duration::from_millis(500));
        assert!(
            !stop_done.load(Ordering::Acquire),
            "daemon stop returned before child-exited terminalization could persist under store lock"
        );

        drop(lock);
        worker
            .join()
            .expect("join terminalization worker")
            .expect("terminalize task");
        stopper.join().expect("join stopper");
        let store = Store::open(&state.layout).expect("store");
        let task = store.inspect_task(&task_id).expect("task");
        assert_eq!(task.status, "succeeded");
    }

    #[test]
    fn daemon_stop_preserves_completion_drain_in_mixed_cancel_batch() {
        let (_home, state) = test_state();
        let state = Arc::new(state);

        let completing_control = Arc::new(SupervisedTaskControl::default());
        mark_supervised_task_process_completing(&completing_control).expect("mark completing");
        state
            .supervised_tasks
            .lock()
            .expect("task registry")
            .insert("task-mixed-completing".to_owned(), completing_control);

        let mut command = Command::new("/bin/sh");
        command
            .arg("-c")
            .arg("trap '' TERM; while :; do sleep 1; done")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .process_group(0);
        let mut child = spawn_command_locked(&mut command).expect("spawn child");
        let pid = child.id();
        let pid_identity = process_start_identity(pid)
            .expect("process identity")
            .expect("process identity available");
        let cancel_control = Arc::new(SupervisedTaskControl::default());
        set_supervised_task_process(&cancel_control, pid, pid_identity.clone())
            .expect("set process");
        {
            let mut store = Store::open(&state.layout).expect("store");
            store
                .create_task_with_job(
                    NewJob {
                        job_id: "job-mixed-cancel".to_owned(),
                        source_thread_id: "thread-mixed-cancel".to_owned(),
                        summary: "mixed cancel".to_owned(),
                        metadata_json: "{}".to_owned(),
                        policy: DeliveryPolicy::fail_closed(),
                        created_at: 10,
                    },
                    NewTask {
                        task_id: "task-mixed-cancel".to_owned(),
                        job_id: "job-mixed-cancel".to_owned(),
                        source_thread_id: "thread-mixed-cancel".to_owned(),
                        summary: "mixed cancel".to_owned(),
                        command_json: "[]".to_owned(),
                        cwd: "/tmp".to_owned(),
                        timeout_seconds: None,
                        max_delivery_attempts: 3,
                        redelivery_window_seconds: 60,
                        created_at: 10,
                    },
                )
                .expect("create task");
            store
                .mark_task_started("task-mixed-cancel", i64::from(pid), Some(&pid_identity), 11)
                .expect("mark started");
        }
        state
            .supervised_tasks
            .lock()
            .expect("task registry")
            .insert("task-mixed-cancel".to_owned(), cancel_control);

        let cancel_removed = Arc::new(AtomicBool::new(false));
        let cancel_removed_for_worker = Arc::clone(&cancel_removed);
        let cancel_registry = state.supervised_tasks.clone();
        let cancel_worker = thread::spawn(move || {
            let _ = child.wait();
            cancel_registry
                .lock()
                .expect("task registry")
                .remove("task-mixed-cancel");
            cancel_removed_for_worker.store(true, Ordering::Release);
        });

        let completion_registry = state.supervised_tasks.clone();
        let completion_removed = Arc::new(AtomicBool::new(false));
        let completion_removed_for_worker = Arc::clone(&completion_removed);
        let completion_worker = thread::spawn({
            let cancel_removed = Arc::clone(&cancel_removed);
            move || {
                while !cancel_removed.load(Ordering::Acquire) {
                    thread::sleep(READ_POLL_INTERVAL);
                }
                thread::sleep(TASK_SHUTDOWN_WORKER_DRAIN_GRACE + Duration::from_secs(1));
                completion_registry
                    .lock()
                    .expect("task registry")
                    .remove("task-mixed-completing");
                completion_removed_for_worker.store(true, Ordering::Release);
            }
        });

        let stop_state = Arc::clone(&state);
        let stop_done = Arc::new(AtomicBool::new(false));
        let stop_done_for_thread = Arc::clone(&stop_done);
        let stopper = thread::spawn(move || {
            stop_all_supervised_tasks(&stop_state);
            stop_done_for_thread.store(true, Ordering::Release);
        });

        let cancel_deadline = Instant::now()
            + TASK_SHUTDOWN_DURABLE_CANCEL_GRACE
            + TASK_SHUTDOWN_UNPERSISTED_CANCEL_GRACE
            + TASK_TERM_GRACE
            + Duration::from_secs(3);
        while !cancel_removed.load(Ordering::Acquire) && Instant::now() < cancel_deadline {
            thread::sleep(TASK_WAIT_POLL_INTERVAL);
        }
        assert!(
            cancel_removed.load(Ordering::Acquire),
            "stop should SIGKILL and reap the cancel target"
        );
        thread::sleep(TASK_SHUTDOWN_WORKER_DRAIN_GRACE + Duration::from_millis(300));
        assert!(
            !stop_done.load(Ordering::Acquire),
            "mixed shutdown must not shorten the natural-completion drain to worker-drain grace"
        );

        completion_worker.join().expect("join completion worker");
        stopper.join().expect("join stopper");
        cancel_worker.join().expect("join cancel worker");
        assert!(completion_removed.load(Ordering::Acquire));
        assert!(stop_done.load(Ordering::Acquire));
        assert!(
            state
                .supervised_tasks
                .lock()
                .expect("task registry")
                .is_empty()
        );
    }

    #[test]
    fn task_pid_clears_after_child_wait_before_store_retry() {
        let (home, state) = test_state();
        let task_id = "task-clear-pid-after-wait".to_owned();
        let job_id = "job-clear-pid-after-wait".to_owned();
        {
            let mut store = Store::open(&state.layout).expect("store");
            store
                .create_task_with_job(
                    NewJob {
                        job_id: job_id.clone(),
                        source_thread_id: "thread-clear-pid-after-wait".to_owned(),
                        summary: "clear pid after wait".to_owned(),
                        metadata_json: "{}".to_owned(),
                        policy: DeliveryPolicy::fail_closed(),
                        created_at: 10,
                    },
                    NewTask {
                        task_id: task_id.clone(),
                        job_id: job_id.clone(),
                        source_thread_id: "thread-clear-pid-after-wait".to_owned(),
                        summary: "clear pid after wait".to_owned(),
                        command_json: "[\"/bin/sh\",\"-c\",\"true\"]".to_owned(),
                        cwd: home.path().display().to_string(),
                        timeout_seconds: None,
                        max_delivery_attempts: 3,
                        redelivery_window_seconds: 60,
                        created_at: 10,
                    },
                )
                .expect("create task");
        }
        let task_dir = state.layout.task_dir(&task_id);
        ensure_private_dir(&task_dir).expect("task dir");
        let stdout_path = task_dir.join("stdout.log");
        let stderr_path = task_dir.join("stderr.log");
        let result_path = task_dir.join("result.txt");
        let mut command = Command::new("/bin/sh");
        command
            .arg("-c")
            .arg("true")
            .current_dir(home.path())
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .process_group(0);
        let mut child = spawn_command_locked(&mut command).expect("spawn child");
        let pid = child.id();
        let pid_identity = process_start_identity(pid)
            .expect("process identity")
            .expect("process identity available");
        let control = Arc::new(SupervisedTaskControl::default());
        set_supervised_task_process(&control, pid, pid_identity).expect("set process");
        let lock = Connection::open(state.layout.db_path()).expect("lock connection");
        lock.execute_batch("PRAGMA locking_mode=EXCLUSIVE; BEGIN EXCLUSIVE;")
            .expect("hold exclusive lock");
        let layout = state.layout.clone();
        let cwd = home.path().to_path_buf();
        let control_for_worker = Arc::clone(&control);
        let worker = thread::spawn(move || {
            run_supervised_spawned_task(
                &layout,
                &task_id,
                &job_id,
                "/bin/sh -c true",
                &cwd,
                None,
                3,
                60,
                &control_for_worker,
                &mut child,
                pid,
                Instant::now(),
                stdout_path,
                stderr_path,
                result_path,
            )
        });

        let deadline = Instant::now() + Duration::from_secs(5);
        while matches!(
            supervised_task_process_state(&control).expect("process state"),
            SupervisedTaskProcessState::Running(_)
        ) {
            assert!(
                Instant::now() < deadline,
                "task process was not moved to completing while completion store write was blocked"
            );
            thread::sleep(Duration::from_millis(20));
        }
        assert!(
            !worker.is_finished(),
            "worker should still be blocked on the held store lock after clearing pid"
        );
        drop(lock);
        worker
            .join()
            .expect("task worker joins")
            .expect("task completes after lock release");
    }

    #[test]
    fn task_cancel_signal_uses_current_pid_snapshot() {
        let home = tempfile::tempdir().expect("temp home");
        fs::set_permissions(home.path(), fs::Permissions::from_mode(0o700))
            .expect("chmod temp home");
        let marker = home.path().join("stale-pid-term-marker");
        let marker_arg = marker.to_string_lossy().into_owned();
        let mut command = Command::new("/bin/sh");
        command
            .arg("-c")
            .arg("trap 'printf term > \"$1\"; exit 0' TERM; while :; do sleep 1; done")
            .arg("cbth-task")
            .arg(&marker_arg)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .process_group(0);
        let mut child = spawn_command_locked(&mut command).expect("spawn stale pid guard");
        let pid = child.id();
        let control = Arc::new(SupervisedTaskControl::default());
        let pid_identity = process_start_identity(pid)
            .expect("process identity")
            .expect("process identity available");
        set_supervised_task_process(&control, pid, pid_identity).expect("set process");
        clear_supervised_task_process(&control).expect("clear process");

        let signaled = signal_current_supervised_task_process_group(&control, libc::SIGTERM)
            .expect("signal current process group");
        assert_eq!(signaled, None);
        let signal_deadline = Instant::now() + Duration::from_secs(1);
        while !marker.exists() && Instant::now() < signal_deadline {
            thread::sleep(Duration::from_millis(20));
        }
        assert!(
            !marker.exists(),
            "cancel signaling must use the current pid snapshot, not an earlier cached pid"
        );
        terminate_task_child_best_effort(&mut child, pid);
    }

    #[test]
    fn terminate_task_child_kills_group_after_leader_exits_on_term() {
        let home = tempfile::tempdir().expect("temp home");
        let marker = home.path().join("terminate-child-escaped");
        let marker_arg = marker.to_string_lossy().into_owned();
        let mut command = Command::new("/bin/sh");
        command
            .arg("-c")
            .arg(
                "trap 'exit 0' TERM; /bin/sh -c 'trap \"\" TERM; sleep 30; printf escaped > \"$1\"' child \"$1\" & wait",
            )
            .arg("cbth-task")
            .arg(&marker_arg)
            .process_group(0);
        let mut child = spawn_command_locked(&mut command).expect("spawn process group");
        let pid = child.id();
        thread::sleep(Duration::from_millis(100));

        terminate_task_child_best_effort(&mut child, pid);

        assert!(
            !process_group_has_live_members_after_leader_exit(pid),
            "terminating task child should clean surviving same-group descendants"
        );
        assert!(
            !marker.exists(),
            "surviving same-group descendant escaped task cleanup"
        );
    }

    #[test]
    fn late_cancel_during_stream_drain_does_not_override_gone_process_group() {
        let mut command = Command::new("/bin/sh");
        command.arg("-c").arg("true").process_group(0);
        let mut child = spawn_command_locked(&mut command).expect("spawn child");
        let pid = child.id();
        child.wait().expect("wait child");
        let deadline = Instant::now() + Duration::from_secs(1);
        while process_group_exists(pid) && Instant::now() < deadline {
            thread::sleep(TASK_WAIT_POLL_INTERVAL);
        }
        assert!(!process_group_exists(pid), "process group should be gone");

        let delayed_worker = || {
            let abort = Arc::new(AtomicBool::new(false));
            let handle = thread::spawn(|| {
                thread::sleep(Duration::from_millis(50));
                Ok(TaskStreamCapture {
                    bytes_seen: 0,
                    truncated: false,
                    tail: Vec::new(),
                })
            });
            TaskStreamWorker { handle, abort }
        };
        let control = SupervisedTaskControl::default();
        request_supervised_task_cancel(&control);
        let mut cancelled = false;
        let mut timed_out = false;

        let (stdout_capture, stderr_capture) = join_task_stream_spools_with_control(
            delayed_worker(),
            delayed_worker(),
            pid,
            &control,
            Instant::now(),
            Instant::now(),
            Some(Duration::ZERO),
            &mut cancelled,
            &mut timed_out,
        )
        .expect("join stream workers");

        assert!(!cancelled);
        assert!(!timed_out);
        assert!(!stdout_capture.truncated);
        assert!(!stderr_capture.truncated);
    }

    #[test]
    fn stream_drain_aborts_after_process_group_is_gone_without_cancel_or_timeout() {
        let mut command = Command::new("/bin/sh");
        command.arg("-c").arg("true").process_group(0);
        let mut child = spawn_command_locked(&mut command).expect("spawn child");
        let pid = child.id();
        child.wait().expect("wait child");
        let deadline = Instant::now() + Duration::from_secs(1);
        while process_group_exists(pid) && Instant::now() < deadline {
            thread::sleep(TASK_WAIT_POLL_INTERVAL);
        }
        assert!(!process_group_exists(pid), "process group should be gone");

        let blocking_worker = || {
            let abort = Arc::new(AtomicBool::new(false));
            let worker_abort = Arc::clone(&abort);
            let handle = thread::spawn(move || {
                while !worker_abort.load(Ordering::Acquire) {
                    thread::sleep(READ_POLL_INTERVAL);
                }
                Ok(TaskStreamCapture {
                    bytes_seen: 0,
                    truncated: true,
                    tail: Vec::new(),
                })
            });
            TaskStreamWorker { handle, abort }
        };
        let control = SupervisedTaskControl::default();
        let mut cancelled = false;
        let mut timed_out = false;

        let (stdout_capture, stderr_capture) = join_task_stream_spools_with_control(
            blocking_worker(),
            blocking_worker(),
            pid,
            &control,
            Instant::now(),
            Instant::now(),
            None,
            &mut cancelled,
            &mut timed_out,
        )
        .expect("join stream workers");

        assert!(!cancelled);
        assert!(!timed_out);
        assert!(stdout_capture.truncated);
        assert!(stderr_capture.truncated);
    }

    #[test]
    fn stream_drain_fails_when_worker_ignores_abort_deadline() {
        let mut command = Command::new("/bin/sh");
        command.arg("-c").arg("true").process_group(0);
        let mut child = spawn_command_locked(&mut command).expect("spawn child");
        let pid = child.id();
        child.wait().expect("wait child");
        let deadline = Instant::now() + Duration::from_secs(1);
        while process_group_exists(pid) && Instant::now() < deadline {
            thread::sleep(TASK_WAIT_POLL_INTERVAL);
        }
        assert!(!process_group_exists(pid), "process group should be gone");

        let worker_stop = Arc::new(AtomicBool::new(false));
        let (worker_done_tx, worker_done_rx) = std::sync::mpsc::channel();
        let stuck_worker = || {
            let abort = Arc::new(AtomicBool::new(false));
            let worker_stop = Arc::clone(&worker_stop);
            let worker_done_tx = worker_done_tx.clone();
            let handle = thread::spawn(move || {
                while !worker_stop.load(Ordering::Acquire) {
                    thread::sleep(READ_POLL_INTERVAL);
                }
                let _ = worker_done_tx.send(());
                Ok(TaskStreamCapture {
                    bytes_seen: 0,
                    truncated: true,
                    tail: Vec::new(),
                })
            });
            TaskStreamWorker { handle, abort }
        };
        let control = SupervisedTaskControl::default();
        let mut cancelled = false;
        let mut timed_out = false;

        let started = Instant::now();
        let error = join_task_stream_spools_with_control(
            stuck_worker(),
            stuck_worker(),
            pid,
            &control,
            Instant::now(),
            Instant::now(),
            None,
            &mut cancelled,
            &mut timed_out,
        )
        .expect_err("unresponsive stream workers should fail closed");

        assert!(
            started.elapsed() < TASK_TERM_GRACE,
            "stream abort deadline should bound task worker shutdown"
        );
        assert!(
            format!("{error:#}").contains("task stream workers did not stop after abort deadline"),
            "{error:#}"
        );
        assert!(!cancelled);
        assert!(!timed_out);

        worker_stop.store(true, Ordering::Release);
        worker_done_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("first detached worker exits");
        worker_done_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("second detached worker exits");
    }

    #[test]
    fn stream_drain_fails_fast_when_one_stream_worker_errors() {
        let mut command = Command::new("/bin/sh");
        command
            .arg("-c")
            .arg("/bin/sh -c 'trap \"exit 0\" TERM; while true; do sleep 1; done' >/dev/null 2>&1 & exit 0")
            .process_group(0);
        let mut child = spawn_command_locked(&mut command).expect("spawn process group");
        let pid = child.id();
        child.wait().expect("wait leader");
        let live_deadline = Instant::now() + Duration::from_secs(2);
        while !process_group_has_live_members_after_leader_exit(pid)
            && Instant::now() < live_deadline
        {
            thread::sleep(TASK_WAIT_POLL_INTERVAL);
        }
        assert!(
            process_group_exists(pid),
            "descendant should keep group alive"
        );

        let failing_worker = || {
            let abort = Arc::new(AtomicBool::new(false));
            let handle = thread::spawn(|| -> Result<TaskStreamCapture> {
                thread::sleep(Duration::from_millis(50));
                Err(anyhow::anyhow!("simulated stream spool failure"))
            });
            TaskStreamWorker { handle, abort }
        };
        let blocking_worker = || {
            let abort = Arc::new(AtomicBool::new(false));
            let worker_abort = Arc::clone(&abort);
            let handle = thread::spawn(move || {
                while !worker_abort.load(Ordering::Acquire) {
                    thread::sleep(READ_POLL_INTERVAL);
                }
                Ok(TaskStreamCapture {
                    bytes_seen: 0,
                    truncated: true,
                    tail: Vec::new(),
                })
            });
            TaskStreamWorker { handle, abort }
        };
        let control = SupervisedTaskControl::default();
        let mut cancelled = false;
        let mut timed_out = false;

        let started = Instant::now();
        let error = join_task_stream_spools_with_control(
            failing_worker(),
            blocking_worker(),
            pid,
            &control,
            Instant::now(),
            Instant::now(),
            None,
            &mut cancelled,
            &mut timed_out,
        )
        .expect_err("stream worker failure should fail the task");

        assert!(
            started.elapsed() < TASK_TERM_GRACE,
            "stream worker failure should not wait for the other stream forever"
        );
        assert!(
            format!("{error:#}").contains("stdout task stream failed"),
            "{error:#}"
        );
        assert!(!cancelled);
        assert!(!timed_out);
        let gone_deadline = Instant::now() + Duration::from_secs(2);
        while process_group_exists(pid) && Instant::now() < gone_deadline {
            thread::sleep(TASK_WAIT_POLL_INTERVAL);
        }
        assert!(
            !process_group_exists(pid),
            "stream worker failure should terminate the live process group"
        );
    }

    #[test]
    fn stream_spool_failure_terminates_running_task_before_child_exit() {
        let home = tempfile::tempdir().expect("temp home");
        fs::set_permissions(home.path(), fs::Permissions::from_mode(0o700))
            .expect("chmod temp home");
        let layout = FsLayout::resolve(Some(home.path().to_path_buf())).expect("layout");
        let missing_dir = home.path().join("missing-log-dir");
        fs::write(&missing_dir, b"not a directory").expect("create non-directory log parent");
        let mut command = Command::new("/bin/sh");
        command
            .arg("-c")
            .arg("while true; do sleep 1; done")
            .process_group(0)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        let mut child = spawn_command_locked(&mut command).expect("spawn child");
        let pid = child.id();
        let pid_identity = process_start_identity(pid)
            .expect("process identity")
            .expect("process identity available");
        let control = Arc::new(SupervisedTaskControl::default());
        set_supervised_task_process(&control, pid, pid_identity).expect("set process");

        let started = Instant::now();
        let error = run_supervised_spawned_task(
            &layout,
            "task-stream-spool-start-failure",
            "job-stream-spool-start-failure",
            "long-running command",
            home.path(),
            None,
            3,
            60,
            &control,
            &mut child,
            pid,
            Instant::now(),
            missing_dir.join("stdout.log"),
            missing_dir.join("stderr.log"),
            home.path().join("result.txt"),
        )
        .expect_err("stream spool failure should fail the task");

        assert!(
            started.elapsed() < TASK_TERM_GRACE,
            "stream spool failure should not wait for natural child exit"
        );
        assert!(
            format!("{error:#}").contains("task stream failed"),
            "{error:#}"
        );
        assert_eq!(
            supervised_task_process_state(&control).expect("process state"),
            SupervisedTaskProcessState::Completing
        );
        let gone_deadline = Instant::now() + Duration::from_secs(2);
        while process_group_exists(pid) && Instant::now() < gone_deadline {
            thread::sleep(TASK_WAIT_POLL_INTERVAL);
        }
        assert!(
            !process_group_exists(pid),
            "stream spool failure should terminate the running task group"
        );
    }

    #[test]
    fn cancel_during_stream_drain_terminates_live_process_group() {
        let leader_pid = unsafe { libc::fork() };
        assert!(leader_pid >= 0, "fork leader failed");
        if leader_pid == 0 {
            unsafe {
                if libc::setpgid(0, 0) != 0 {
                    libc::_exit(2);
                }
                let member_pid = libc::fork();
                if member_pid < 0 {
                    libc::_exit(3);
                }
                if member_pid == 0 {
                    loop {
                        libc::sleep(30);
                    }
                }
                libc::_exit(0);
            }
        }

        let mut status = 0;
        let waited = unsafe { libc::waitpid(leader_pid, &mut status, 0) };
        assert_eq!(waited, leader_pid, "wait leader");
        assert!(
            libc::WIFEXITED(status) && libc::WEXITSTATUS(status) == 0,
            "leader exited unsuccessfully"
        );
        let pid = u32::try_from(leader_pid).expect("leader pid");
        assert!(process_group_exists(pid), "member should keep group alive");

        let delayed_worker = || {
            let abort = Arc::new(AtomicBool::new(false));
            let handle = thread::spawn(|| {
                thread::sleep(Duration::from_millis(50));
                Ok(TaskStreamCapture {
                    bytes_seen: 0,
                    truncated: false,
                    tail: Vec::new(),
                })
            });
            TaskStreamWorker { handle, abort }
        };
        let control = SupervisedTaskControl::default();
        request_supervised_task_cancel(&control);
        let mut cancelled = false;
        let mut timed_out = false;

        let _captures = join_task_stream_spools_with_control(
            delayed_worker(),
            delayed_worker(),
            pid,
            &control,
            Instant::now(),
            Instant::now(),
            None,
            &mut cancelled,
            &mut timed_out,
        )
        .expect("join stream workers");
        wait_process_group_exit_with_control(
            pid,
            &control,
            Instant::now(),
            Instant::now(),
            None,
            &mut cancelled,
            &mut timed_out,
        )
        .expect("wait process group");

        assert!(cancelled);
        assert!(!timed_out);
        assert!(
            !process_group_exists(pid),
            "cancel should terminate live process group"
        );
    }

    #[test]
    fn leader_exit_cleanup_terminates_live_process_group_without_cancel_or_timeout() {
        let leader_pid = unsafe { libc::fork() };
        assert!(leader_pid >= 0, "fork leader failed");
        if leader_pid == 0 {
            unsafe {
                if libc::setpgid(0, 0) != 0 {
                    libc::_exit(2);
                }
                let member_pid = libc::fork();
                if member_pid < 0 {
                    libc::_exit(3);
                }
                if member_pid == 0 {
                    loop {
                        libc::sleep(30);
                    }
                }
                libc::_exit(0);
            }
        }

        let mut status = 0;
        let waited = unsafe { libc::waitpid(leader_pid, &mut status, 0) };
        assert_eq!(waited, leader_pid, "wait leader");
        assert!(
            libc::WIFEXITED(status) && libc::WEXITSTATUS(status) == 0,
            "leader exited unsuccessfully"
        );
        let pid = u32::try_from(leader_pid).expect("leader pid");
        assert!(process_group_exists(pid), "member should keep group alive");

        let control = SupervisedTaskControl::default();
        let mut cancelled = false;
        let mut timed_out = false;
        let leader_exited_at = Instant::now() - TASK_LEADER_EXITED_PROCESS_GROUP_GRACE;

        wait_process_group_exit_with_control(
            pid,
            &control,
            Instant::now(),
            leader_exited_at,
            None,
            &mut cancelled,
            &mut timed_out,
        )
        .expect("wait process group");

        assert!(!cancelled);
        assert!(!timed_out);
        assert!(
            !process_group_exists(pid),
            "leader-exit cleanup should terminate live descendants"
        );
    }
}
