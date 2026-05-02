use std::collections::HashMap;
use std::ffi::OsString;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::mem;
use std::net::IpAddr;
use std::os::fd::{AsRawFd, FromRawFd, RawFd};
use std::os::unix::ffi::{OsStrExt, OsStringExt};
use std::os::unix::fs::{FileTypeExt, MetadataExt, PermissionsExt};
use std::os::unix::net::{UnixListener, UnixStream};
use std::os::unix::process::CommandExt;
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::cli_app_server_client::AppServerJsonRpcClient;
use crate::fs_layout::{FsLayout, sync_dir};
use crate::models::{DaemonLifecycleStatus, SweepReport};
use crate::store::{Store, new_id};

const MAX_REQUEST_BYTES: usize = 2 * 1024 * 1024;
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
const DEFAULT_CLI_APP_SERVER_LEASE_TTL_SECONDS: u64 = 60;
const DAEMON_PROTOCOL_VERSION: u64 = 1;
const DAEMON_CAPABILITIES: &[&str] = &[
    "dispatch",
    "attempt-dispatch",
    "cli-app-server-lifecycle",
    "cli-thread-start-bootstrap",
    "cli-session-dispatch",
    "cli-session-capability-dispatch",
    "cli-session-proof-invalidation-dispatch",
    "cli-turn-observation-dispatch",
    "cli-auto-delivery-dispatch",
];
const MAX_DISPATCH_WORKERS: usize = 32;
const RESERVED_CONTROL_WORKERS: usize = 8;
const MAX_CLIENT_WORKERS: usize = MAX_DISPATCH_WORKERS + RESERVED_CONTROL_WORKERS;
const DAEMON_BUSY_ERROR: &str = "daemon is busy";
const DAEMON_CONNECTION_LIMIT_ERROR: &str = "daemon connection limit reached";

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
            || self.status.active_cli_acceptances > 0
            || self.status.active_cli_observations > 0
            || (!maintenance_suppressed && self.status.cli_acceptances_stale_now > 0)
            || (!maintenance_suppressed && self.status.cli_observations_due_now > 0)
            || (!maintenance_suppressed && self.status.open_batches_due_within_idle > 0)
    }
}

pub fn daemon_serve(layout: &FsLayout, options: DaemonServeOptions) -> Result<Value> {
    layout.ensure_run_dir()?;
    let startup_sweep = if let Some(now) = options.startup_sweep_now {
        let mut store = Store::open(layout)?;
        store.sweep(layout, now)?
    } else {
        SweepReport::default()
    };

    let socket_path = layout.daemon_socket_path();
    prepare_socket_path(&socket_path)?;

    let listener = UnixListener::bind(&socket_path)
        .with_context(|| format!("bind daemon socket {}", socket_path.display()))?;
    let _cleanup = SocketCleanup { path: &socket_path };
    set_socket_permissions(&socket_path)?;
    listener
        .set_nonblocking(true)
        .with_context(|| format!("set nonblocking {}", socket_path.display()))?;

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
    join_workers(workers);
    join_lifecycle_maintenance(lifecycle_maintenance_worker);
    stop_all_cli_app_servers(&state);
    stop_all_cli_thread_start_bootstraps(&state);
    clear_cli_app_server_reservations(&state);

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
        let mut child = command
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .context("spawn cbth daemon")?;
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
                    stop_incompatible_daemon(layout, startup_deadline)?;
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
    validate_socket_endpoint(layout)?;
    let socket_path = layout.daemon_socket_path();
    let mut stream = connect_unix_stream_until(&socket_path, deadline)
        .with_context(|| format!("connect daemon socket {}", socket_path.display()))?;
    stream
        .set_nonblocking(true)
        .context("set daemon client nonblocking")?;

    let request = serde_json::to_vec(&json!({
        "command": command,
        "payload": payload,
    }))?;
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
                stop_incompatible_daemon(layout, startup_deadline)?;
                return Ok(None);
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

fn stop_incompatible_daemon(layout: &FsLayout, startup_deadline: Instant) -> Result<()> {
    daemon_request_with_timeout(layout, "stop", remaining_budget(startup_deadline)?)
        .context("stop incompatible daemon")?;
    wait_for_incompatible_daemon_replaced_or_removed_until(layout, startup_deadline)
}

fn wait_for_incompatible_daemon_replaced_or_removed_until(
    layout: &FsLayout,
    deadline: Instant,
) -> Result<()> {
    let socket_path = layout.daemon_socket_path();
    let mut saw_socket_absent = false;
    loop {
        if !socket_path.exists() {
            if saw_socket_absent {
                return Ok(());
            }
            saw_socket_absent = true;
            if Instant::now() >= deadline {
                return Ok(());
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
            Ok(response) if daemon_response_is_compatible(&response) => return Ok(()),
            Ok(_) => {}
            Err(error) if error_is_daemon_busy(&error) => return Ok(()),
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
    if worker.is_some()
        || state
            .lifecycle_maintenance_suppressed
            .load(Ordering::Acquire)
        || cache.refresh_failed
        || !cache.status.has_due_maintenance()
        || now < *next_attempt_at
    {
        return;
    }

    let layout = state.layout.clone();
    *next_attempt_at = now + DAEMON_MAINTENANCE_RETRY_INTERVAL;
    *worker = Some(thread::spawn(move || {
        let Ok(now) = current_epoch_seconds() else {
            return;
        };
        let Ok(mut store) = Store::open(&layout) else {
            return;
        };
        let _ = store.sweep(&layout, now);
    }));
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
    let existing_dead_proof = {
        let mut servers = state
            .cli_app_servers
            .lock()
            .map_err(|_| anyhow::anyhow!("CLI app-server registry lock is poisoned"))?;
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

    let server = spawn_cli_app_server(
        &payload.managed_session_id,
        &payload.bound_thread_id,
        payload.session_epoch,
        &payload.codex_binary,
        &payload.lease_id,
        lease_ttl,
    )?;
    let mut server = Some(server);
    loop {
        let mut servers = state
            .cli_app_servers
            .lock()
            .map_err(|_| anyhow::anyhow!("CLI app-server registry lock is poisoned"))?;
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
    let bootstrap_id = new_id();
    let server = spawn_cli_app_server(
        &bootstrap_id,
        "__cbth_thread_start_bootstrap__",
        1,
        &payload.codex_binary,
        &payload.lease_id,
        lease_ttl,
    )?;
    let mut server = Some(server);
    let result = (|| {
        let url = server
            .as_ref()
            .expect("bootstrap app-server is available")
            .url
            .clone();
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
        let started = client
            .request(
                "thread/start",
                json!({ "cwd": payload.cwd }),
                CLI_THREAD_START_BOOTSTRAP_REQUEST_TIMEOUT,
            )
            .context("bootstrap cli thread/start")?;
        let thread_id = parse_cli_thread_start_id(&started)?;
        if let Some(server) = server.as_mut() {
            server.bound_thread_id = thread_id.clone();
        }
        let mut bootstraps = state
            .cli_thread_start_bootstraps
            .lock()
            .map_err(|_| anyhow::anyhow!("CLI thread/start bootstrap registry lock is poisoned"))?;
        bootstraps.insert(
            bootstrap_id.clone(),
            server
                .take()
                .expect("bootstrap app-server is still available"),
        );
        Ok(CliThreadStartInfo {
            bootstrap_id,
            thread_id,
        })
    })();
    if let Some(server) = server.take() {
        stop_managed_cli_app_server_process(server);
    }
    result
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
    ensure_cli_app_server_reservation_matches(state, &payload.bound_thread_id, &payload.lease_id)?;

    let mut server = {
        let mut bootstraps = state
            .cli_thread_start_bootstraps
            .lock()
            .map_err(|_| anyhow::anyhow!("CLI thread/start bootstrap registry lock is poisoned"))?;
        bootstraps
            .remove(&payload.bootstrap_id)
            .ok_or_else(|| anyhow::anyhow!("CLI thread/start bootstrap is not running"))?
    };
    if server.bound_thread_id != payload.bound_thread_id {
        let started_thread = server.bound_thread_id.clone();
        stop_managed_cli_app_server_process(server);
        bail!(
            "CLI thread/start bootstrap created thread {}, not {}",
            started_thread,
            payload.bound_thread_id
        );
    }
    if !cli_app_server_child_is_running(&mut server)? {
        stop_managed_cli_app_server_process(server);
        bail!("CLI thread/start bootstrap app-server has exited");
    }
    server.managed_session_id = payload.managed_session_id.clone();
    server.session_epoch = payload.session_epoch;
    server.lease_id = payload.lease_id.clone();
    server.lease_expires_at = Instant::now() + lease_ttl;

    let mut servers = state
        .cli_app_servers
        .lock()
        .map_err(|_| anyhow::anyhow!("CLI app-server registry lock is poisoned"))?;
    if let Some(existing) = servers.get_mut(&payload.managed_session_id) {
        let proof = cli_app_server_proof(existing);
        drop(servers);
        stop_managed_cli_app_server_process(server);
        invalidate_and_stop_registered_cli_app_server(state, &proof)?;
        bail!(
            "managed session {} already has a registered CLI app-server",
            payload.managed_session_id
        );
    }
    let info = cli_app_server_info(&server);
    servers.insert(payload.managed_session_id.clone(), server);
    drop(servers);
    let _ = release_cli_app_server_reservation(
        state,
        CliAppServerReleasePayload {
            bound_thread_id: payload.bound_thread_id,
            lease_id: payload.lease_id,
        },
    );
    Ok(info)
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

fn spawn_cli_app_server(
    managed_session_id: &str,
    bound_thread_id: &str,
    session_epoch: i64,
    codex_binary: &[u8],
    lease_id: &str,
    lease_ttl: Duration,
) -> Result<ManagedCliAppServer> {
    let codex_binary = OsString::from_vec(codex_binary.to_vec());
    let mut child = Command::new(&codex_binary)
        .arg("app-server")
        .arg("--listen")
        .arg("ws://127.0.0.1:0")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .process_group(0)
        .spawn()
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
    let url = match url_receiver.recv_timeout(CLI_APP_SERVER_STARTUP_TIMEOUT) {
        Ok(url) => url,
        Err(error) => {
            drain_running.store(false, Ordering::Release);
            stop_cli_app_server_process(&mut child);
            join_worker(stdout_worker);
            join_worker(stderr_worker);
            bail!("codex app-server did not report a websocket listener: {error}");
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
        managed_session_id: managed_session_id.to_owned(),
        bound_thread_id: bound_thread_id.to_owned(),
        session_epoch,
        url,
        child,
        started_at,
        lease_id: lease_id.to_owned(),
        lease_expires_at: Instant::now() + lease_ttl,
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

fn signal_process_group(pid: u32, signal: libc::c_int) {
    let pgid = pid as libc::pid_t;
    let _ = unsafe { libc::killpg(pgid, signal) };
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

    fn test_managed_cli_app_server(
        managed_session_id: &str,
        bound_thread_id: &str,
        lease_expires_at: Instant,
    ) -> ManagedCliAppServer {
        let child = Command::new("sh")
            .arg("-c")
            .arg("while :; do sleep 1; done")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .process_group(0)
            .spawn()
            .expect("spawn test app-server child");
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

        drop(dispatch_slots);
        let _slot = try_acquire_dispatch_slot(&state).expect("released dispatch slot");
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
}
