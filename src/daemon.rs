use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::mem;
use std::os::fd::{AsRawFd, FromRawFd, RawFd};
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::{FileTypeExt, MetadataExt, PermissionsExt};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::Path;
use std::process::{Command, Stdio};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::fs_layout::{FsLayout, sync_dir};
use crate::models::SweepReport;
use crate::store::Store;

const MAX_REQUEST_BYTES: usize = 16 * 1024;
const MAX_RESPONSE_BYTES: usize = 256 * 1024;
const CLIENT_READ_TIMEOUT: Duration = Duration::from_secs(5);
const IDLE_POLL_INTERVAL: Duration = Duration::from_millis(250);
const READ_POLL_INTERVAL: Duration = Duration::from_millis(10);
const STARTUP_POLL_INTERVAL: Duration = Duration::from_millis(50);

#[derive(Clone, Copy, Debug)]
pub struct DaemonServeOptions {
    pub idle_timeout_seconds: u64,
    pub startup_sweep_now: i64,
}

#[derive(Clone, Copy, Debug)]
pub struct DaemonEnsureOptions {
    pub idle_timeout_seconds: u64,
    pub startup_timeout_seconds: u64,
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

struct DaemonState<'a> {
    layout: &'a FsLayout,
    started_instant: Instant,
    started_at: i64,
    idle_timeout: Duration,
    startup_sweep: SweepReport,
    stop_requested: bool,
}

pub fn daemon_serve(layout: &FsLayout, options: DaemonServeOptions) -> Result<Value> {
    layout.ensure_run_dir()?;
    let startup_sweep = {
        let mut store = Store::open(layout)?;
        store.sweep(layout, options.startup_sweep_now)?
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

    let mut state = DaemonState {
        layout,
        started_instant: Instant::now(),
        started_at: current_epoch_seconds()?,
        idle_timeout: Duration::from_secs(options.idle_timeout_seconds),
        startup_sweep,
        stop_requested: false,
    };
    let mut last_activity = Instant::now();
    let shutdown_reason;

    loop {
        match listener.accept() {
            Ok((mut stream, _addr)) => {
                last_activity = Instant::now();
                if let Err(error) = handle_client(&mut stream, &mut state) {
                    let _ = write_error_response(&mut stream, &error.to_string());
                }
                if state.stop_requested {
                    shutdown_reason = "stop_requested";
                    break;
                }
            }
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                if last_activity.elapsed() >= state.idle_timeout {
                    shutdown_reason = "idle_timeout";
                    break;
                }
                thread::sleep(IDLE_POLL_INTERVAL);
            }
            Err(error) => return Err(error).context("accept daemon connection"),
        }
    }

    Ok(json!({
        "daemon": daemon_info(&state),
        "shutdown_reason": shutdown_reason,
        "startup_sweep": state.startup_sweep,
    }))
}

pub fn daemon_ensure(layout: &FsLayout, options: DaemonEnsureOptions) -> Result<Value> {
    let startup_deadline = Instant::now() + Duration::from_secs(options.startup_timeout_seconds);
    match daemon_request_with_timeout(layout, "ping", remaining_budget(startup_deadline)?) {
        Ok(response) => {
            return Ok(json!({
                "started": false,
                "daemon": response["daemon"].clone(),
            }));
        }
        Err(error) => {
            if Instant::now() >= startup_deadline {
                bail!("daemon did not become ready: {error}");
            }
        }
    }

    layout.ensure_run_dir()?;
    let _startup_lock = acquire_startup_lock(layout, remaining_budget(startup_deadline)?)?;
    match daemon_request_with_timeout(layout, "ping", remaining_budget(startup_deadline)?) {
        Ok(response) => {
            return Ok(json!({
                "started": false,
                "daemon": response["daemon"].clone(),
            }));
        }
        Err(error) => {
            if Instant::now() >= startup_deadline {
                bail!("daemon did not become ready: {error}");
            }
        }
    }

    let mut child = Command::new(std::env::current_exe().context("locate current executable")?)
        .arg("--home")
        .arg(layout.home_dir())
        .arg("daemon")
        .arg("serve")
        .arg("--idle-timeout-seconds")
        .arg(options.idle_timeout_seconds.to_string())
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
            Ok(response) => {
                return Ok(json!({
                    "started": true,
                    "spawned_pid": child_pid,
                    "daemon": response["daemon"].clone(),
                }));
            }
            Err(last_error) => {
                if let Some(status) = child.try_wait().context("check daemon child status")? {
                    if let Ok(response) = daemon_request_with_timeout(
                        layout,
                        "ping",
                        remaining_budget(startup_deadline).unwrap_or(STARTUP_POLL_INTERVAL),
                    ) {
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
    daemon_request_with_timeout(layout, command, CLIENT_READ_TIMEOUT)
}

fn daemon_request_with_timeout(
    layout: &FsLayout,
    command: &str,
    timeout: Duration,
) -> Result<Value> {
    if timeout.is_zero() {
        bail!("daemon request timeout is exhausted");
    }
    daemon_request_until(layout, command, Instant::now() + timeout)
}

fn daemon_request_until(layout: &FsLayout, command: &str, deadline: Instant) -> Result<Value> {
    validate_socket_endpoint(layout)?;
    let socket_path = layout.daemon_socket_path();
    let mut stream = connect_unix_stream_until(&socket_path, deadline)
        .with_context(|| format!("connect daemon socket {}", socket_path.display()))?;
    stream
        .set_nonblocking(true)
        .context("set daemon client nonblocking")?;

    let request = serde_json::to_vec(&json!({ "command": command }))?;
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

fn handle_client(stream: &mut UnixStream, state: &mut DaemonState<'_>) -> Result<()> {
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
            "message": "pong",
        }),
        "status" => json!({
            "daemon": daemon_info(state),
            "startup_sweep": state.startup_sweep,
        }),
        "stop" => {
            state.stop_requested = true;
            json!({
                "daemon": daemon_info(state),
                "stopping": true,
            })
        }
        other => bail!("unknown daemon command: {other}"),
    };
    write_ok_response(stream, response)
}

fn daemon_info(state: &DaemonState<'_>) -> DaemonInfo {
    DaemonInfo {
        pid: std::process::id(),
        started_at: state.started_at,
        uptime_seconds: state.started_instant.elapsed().as_secs(),
        socket_path: state.layout.daemon_socket_path().display().to_string(),
        idle_timeout_seconds: state.idle_timeout.as_secs(),
        stop_requested: state.stop_requested,
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
    if UnixStream::connect(path).is_ok() {
        bail!("daemon socket is already active: {}", path.display());
    }

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
    fs::remove_file(path).with_context(|| format!("remove stale socket {}", path.display()))?;
    if let Some(parent) = path.parent() {
        sync_dir(parent)?;
    }
    Ok(())
}

fn cleanup_stale_socket_best_effort(layout: &FsLayout) {
    let _ = prepare_socket_path(&layout.daemon_socket_path());
}

fn validate_socket_endpoint(layout: &FsLayout) -> Result<()> {
    validate_private_dir(layout.home_dir(), "cbth home")?;
    validate_private_dir(&layout.run_dir(), "cbth run directory")?;
    let socket_path = layout.daemon_socket_path();
    let metadata = fs::symlink_metadata(&socket_path)
        .with_context(|| format!("stat {}", socket_path.display()))?;
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

fn validate_private_dir(path: &Path, name: &str) -> Result<()> {
    let metadata =
        fs::symlink_metadata(path).with_context(|| format!("stat {}", path.display()))?;
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
