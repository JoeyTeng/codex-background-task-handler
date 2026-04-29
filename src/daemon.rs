use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::mem;
use std::os::fd::{AsRawFd, FromRawFd, RawFd};
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::{FileTypeExt, MetadataExt, PermissionsExt};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::Path;
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::fs_layout::{FsLayout, sync_dir};
use crate::models::{DaemonLifecycleStatus, SweepReport};
use crate::store::Store;

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
const DAEMON_PROTOCOL_VERSION: u64 = 1;
const DAEMON_CAPABILITIES: &[&str] = &[
    "dispatch",
    "attempt-dispatch",
    "cli-session-dispatch",
    "cli-turn-observation-dispatch",
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

#[derive(Debug, Serialize)]
struct DaemonInfo {
    pid: u32,
    started_at: i64,
    uptime_seconds: u64,
    socket_path: String,
    idle_timeout_seconds: u64,
    stop_requested: bool,
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
        "status" => json!({
            "daemon": daemon_info(state),
            "protocol_version": DAEMON_PROTOCOL_VERSION,
            "capabilities": DAEMON_CAPABILITIES,
            "startup_sweep": &state.startup_sweep,
        }),
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

    use tempfile::TempDir;

    use super::*;

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
}
