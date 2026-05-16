use std::collections::HashMap;
use std::fs::{self, OpenOptions};
use std::io::{self, Read, Write};
#[cfg(target_os = "macos")]
use std::os::fd::AsRawFd;
use std::os::unix::fs::{FileTypeExt, MetadataExt};
use std::os::unix::net::{UnixListener, UnixStream};
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::fs_layout::{
    FsLayout, atomic_write_private, ensure_private_dir, set_private_file_permissions_if_exists,
    sync_dir, validate_id_path_component,
};
use crate::plugin_rpc::{
    DaemonEndpointHint, PLUGIN_RPC_MAX_FRAME_BYTES, PluginHandshakePolicy, PluginHelloRequest,
    PluginRpcError, PluginRpcErrorKind, PluginRpcPolicy, PluginRpcRequestFrame,
    PluginRpcResponseFrame, ServiceCapability, handle_plugin_hello_frame, read_plugin_rpc_frame,
    write_plugin_rpc_frame,
};

const SERVICE_IDLE_POLL_INTERVAL: Duration = Duration::from_millis(200);
const DEFAULT_RESTART_INITIAL_DELAY_MS: u64 = 500;
const DEFAULT_RESTART_MAX_DELAY_MS: u64 = 30_000;
const DEFAULT_RESTART_MAX_CRASHES: u32 = 32;
const DEFAULT_LOG_MAX_BYTES: u64 = 1024 * 1024;
const DEFAULT_HANDSHAKE_TIMEOUT_MS: u64 = 5_000;
const PLUGIN_TERM_GRACE: Duration = Duration::from_millis(500);
const PLUGIN_KILL_GRACE: Duration = Duration::from_secs(2);
const PLUGIN_HEALTH_UPDATE_METHOD: &str = "plugin.health.update";
static SERVICE_TERMINATION_REQUESTED: AtomicBool = AtomicBool::new(false);

#[derive(Clone, Copy, Debug)]
pub struct ServiceRunOptions {
    pub once: bool,
    pub now: Option<i64>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct PluginRegistry {
    #[serde(default = "registry_schema_version")]
    pub schema_version: u32,
    #[serde(default)]
    pub plugins: Vec<PluginManifest>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct PluginManifest {
    pub name: String,
    pub executable_path: PathBuf,
    #[serde(default)]
    pub args: Vec<String>,
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub release_id: Option<String>,
    #[serde(default)]
    pub capabilities: Vec<String>,
    #[serde(default)]
    pub restart: PluginRestartPolicy,
    #[serde(default)]
    pub environment: HashMap<String, String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct PluginRestartPolicy {
    #[serde(default = "default_restart_initial_delay_ms")]
    pub initial_delay_ms: u64,
    #[serde(default = "default_restart_max_delay_ms")]
    pub max_delay_ms: u64,
    #[serde(default = "default_restart_max_crashes")]
    pub max_crashes: u32,
}

impl Default for PluginRestartPolicy {
    fn default() -> Self {
        Self {
            initial_delay_ms: DEFAULT_RESTART_INITIAL_DELAY_MS,
            max_delay_ms: DEFAULT_RESTART_MAX_DELAY_MS,
            max_crashes: DEFAULT_RESTART_MAX_CRASHES,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct PluginRuntimeStatus {
    pub name: String,
    pub enabled: bool,
    pub configured: bool,
    pub state: PluginRuntimeState,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pid: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub release_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub instance_id: Option<String>,
    pub crash_count: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub restart_after_epoch: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_started_at: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_healthy_at: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_exit: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
    pub plugin_home: String,
    pub stdout_log: String,
    pub stderr_log: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PluginRuntimeState {
    Disabled,
    Starting,
    Running,
    BackingOff,
    Failed,
    Exited,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct PluginStatusReport {
    pub registry_path: String,
    pub plugins: Vec<PluginRuntimeStatus>,
}

struct SupervisedPlugin {
    manifest: PluginManifest,
    process: Option<Child>,
    status: PluginRuntimeStatus,
    next_restart: Option<Instant>,
    current_backoff: Duration,
}

impl SupervisedPlugin {
    fn new(layout: &FsLayout, manifest: PluginManifest, now: i64) -> Self {
        let name = manifest.name.clone();
        let enabled = manifest.enabled;
        let plugin_home = layout.plugin_home_dir(&name).display().to_string();
        let stdout_log = layout
            .plugin_logs_dir(&name)
            .join("stdout.log")
            .display()
            .to_string();
        let stderr_log = layout
            .plugin_logs_dir(&name)
            .join("stderr.log")
            .display()
            .to_string();
        let state = if enabled {
            PluginRuntimeState::Starting
        } else {
            PluginRuntimeState::Disabled
        };
        let status = PluginRuntimeStatus {
            name,
            enabled,
            configured: true,
            state,
            pid: None,
            release_id: manifest.release_id.clone(),
            instance_id: None,
            crash_count: 0,
            restart_after_epoch: if enabled { Some(now) } else { None },
            last_started_at: None,
            last_healthy_at: None,
            last_exit: None,
            last_error: None,
            plugin_home,
            stdout_log,
            stderr_log,
        };
        let initial_backoff = Duration::from_millis(manifest.restart.initial_delay_ms);

        Self {
            manifest,
            process: None,
            status,
            next_restart: if enabled { Some(Instant::now()) } else { None },
            current_backoff: initial_backoff,
        }
    }

    fn can_restart(&self) -> bool {
        self.manifest.enabled && self.status.crash_count < self.manifest.restart.max_crashes
    }
}

pub fn service_run(layout: &FsLayout, options: ServiceRunOptions) -> Result<Value> {
    let _signal_guard = install_service_signal_handlers()?;
    layout.ensure_run_dir()?;
    let registry = load_plugin_registry(layout)?;
    validate_plugin_registry(layout, &registry)?;
    let now = options.now.unwrap_or(current_epoch_seconds()?);
    let shared = Arc::new(Mutex::new(ServiceSharedState::new(layout, &registry, now)));

    let socket_path = layout.service_socket_path();
    prepare_service_socket(&socket_path)?;
    let listener = UnixListener::bind(&socket_path)
        .with_context(|| format!("bind service socket {}", socket_path.display()))?;
    let _cleanup = ServiceSocketCleanup { path: socket_path };
    listener
        .set_nonblocking(true)
        .with_context(|| format!("set nonblocking service socket {}", _cleanup.path.display()))?;
    persist_all_status_if_dirty(layout, &shared)?;
    let mut process_cleanup = ServiceProcessCleanup::new(layout.clone(), Arc::clone(&shared));

    loop {
        let now = current_epoch_seconds()?;
        supervise_tick(layout, &shared, now)?;
        accept_plugin_connections(layout, &listener, &shared)?;
        persist_all_status_if_dirty(layout, &shared)?;
        if options.once {
            break;
        }
        if service_termination_requested() {
            break;
        }
        thread::sleep(SERVICE_IDLE_POLL_INTERVAL);
    }

    process_cleanup.shutdown()?;
    persist_all_status_if_dirty(layout, &shared)?;
    let report = status_report(layout, None)?;
    Ok(
        json!({ "service": { "socket_path": _cleanup.path.display().to_string(), "plugins": report.plugins } }),
    )
}

pub fn status_report(layout: &FsLayout, name: Option<&str>) -> Result<PluginStatusReport> {
    let registry = load_plugin_registry(layout)?;
    validate_plugin_registry(layout, &registry)?;
    let mut plugins = Vec::new();
    for manifest in registry.plugins {
        if let Some(name) = name
            && manifest.name != name
        {
            continue;
        }
        let status = match read_plugin_status(layout, &manifest.name)? {
            Some(mut status) => {
                status.enabled = manifest.enabled;
                status.configured = true;
                status.release_id = status.release_id.or(manifest.release_id.clone());
                status
            }
            None => status_from_manifest(layout, &manifest),
        };
        plugins.push(status);
    }
    if let Some(name) = name
        && plugins.is_empty()
    {
        bail!("plugin is not configured: {name}");
    }
    Ok(PluginStatusReport {
        registry_path: layout.plugin_registry_path().display().to_string(),
        plugins,
    })
}

pub fn write_status_human(report: &PluginStatusReport) -> Result<()> {
    let stdout = io::stdout();
    let mut output = stdout.lock();
    if report.plugins.is_empty() {
        writeln!(output, "No plugins configured.")?;
        return Ok(());
    }
    for plugin in &report.plugins {
        writeln!(
            output,
            "{}\t{:?}\tenabled={}\tpid={}\tcrashes={}\thealth={}",
            plugin.name,
            plugin.state,
            plugin.enabled,
            plugin
                .pid
                .map(|pid| pid.to_string())
                .unwrap_or_else(|| "-".to_owned()),
            plugin.crash_count,
            plugin
                .last_healthy_at
                .map(|value| value.to_string())
                .unwrap_or_else(|| "-".to_owned())
        )?;
        if let Some(error) = &plugin.last_error {
            writeln!(output, "  error: {error}")?;
        }
        if let Some(exit) = &plugin.last_exit {
            writeln!(output, "  last_exit: {exit}")?;
        }
        writeln!(output, "  home: {}", plugin.plugin_home)?;
        writeln!(
            output,
            "  logs: {}, {}",
            plugin.stdout_log, plugin.stderr_log
        )?;
    }
    Ok(())
}

struct ServiceSharedState {
    plugins: HashMap<String, SupervisedPlugin>,
    dirty: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct PluginConnectionIdentity {
    plugin_name: String,
    pid: u32,
    instance_id: String,
}

impl ServiceSharedState {
    fn new(layout: &FsLayout, registry: &PluginRegistry, now: i64) -> Self {
        let plugins = registry
            .plugins
            .iter()
            .cloned()
            .map(|manifest| {
                (
                    manifest.name.clone(),
                    SupervisedPlugin::new(layout, manifest, now),
                )
            })
            .collect();
        Self {
            plugins,
            dirty: true,
        }
    }

    fn mark_dirty(&mut self) {
        self.dirty = true;
    }
}

fn supervise_tick(
    layout: &FsLayout,
    shared: &Arc<Mutex<ServiceSharedState>>,
    now: i64,
) -> Result<()> {
    let mut state = shared
        .lock()
        .map_err(|_| anyhow::anyhow!("service state lock poisoned"))?;
    let mut dirty = false;
    for plugin in state.plugins.values_mut() {
        if !plugin.manifest.enabled {
            plugin.status.state = PluginRuntimeState::Disabled;
            continue;
        }

        if let Some(child) = plugin.process.as_mut() {
            match child.try_wait() {
                Ok(Some(status)) => {
                    plugin.status.pid = None;
                    plugin.process = None;
                    record_plugin_exit(plugin, status, now);
                    dirty = true;
                }
                Ok(None) => {}
                Err(error) => {
                    plugin.status.last_error = Some(format!("wait plugin process: {error}"));
                    plugin.status.pid = None;
                    plugin.process = None;
                    record_plugin_crash(plugin, now);
                    dirty = true;
                }
            }
        }

        if plugin.process.is_none()
            && plugin.can_restart()
            && plugin
                .next_restart
                .is_some_and(|deadline| Instant::now() >= deadline)
        {
            match spawn_plugin(layout, plugin, now) {
                Ok(child) => {
                    plugin.status.pid = Some(child.id());
                    plugin.status.state = PluginRuntimeState::Starting;
                    plugin.status.last_started_at = Some(now);
                    plugin.status.restart_after_epoch = None;
                    plugin.status.instance_id = None;
                    plugin.status.last_healthy_at = None;
                    plugin.status.last_error = None;
                    plugin.process = Some(child);
                    plugin.next_restart = None;
                    dirty = true;
                }
                Err(error) => {
                    plugin.status.last_error = Some(format!("{error:#}"));
                    record_plugin_start_failure(plugin, now);
                    dirty = true;
                }
            }
        }
    }
    if dirty {
        state.mark_dirty();
    }
    Ok(())
}

fn spawn_plugin(layout: &FsLayout, plugin: &mut SupervisedPlugin, now: i64) -> Result<Child> {
    layout.ensure_plugin_home(&plugin.manifest.name)?;
    let executable = &plugin.manifest.executable_path;
    if !executable.is_absolute() {
        bail!(
            "plugin executable path must be absolute: {}",
            executable.display()
        );
    }
    let metadata = fs::metadata(executable)
        .with_context(|| format!("stat plugin executable {}", executable.display()))?;
    if !metadata.is_file() {
        bail!(
            "plugin executable path is not a regular file: {}",
            executable.display()
        );
    }

    let mut command = Command::new(executable);
    command.args(&plugin.manifest.args);
    command.env("CBTH_PLUGIN_NAME", &plugin.manifest.name);
    command.env(
        "CBTH_PLUGIN_HOME",
        layout.plugin_home_dir(&plugin.manifest.name),
    );
    command.env("CBTH_PLUGIN_RPC_SOCKET", layout.service_socket_path());
    command.env("CBTH_PLUGIN_STARTED_AT", now.to_string());
    if let Some(release_id) = &plugin.manifest.release_id {
        command.env("CBTH_PLUGIN_RELEASE_ID", release_id);
    }
    for (key, value) in &plugin.manifest.environment {
        command.env(key, value);
    }
    command.stdin(Stdio::null());
    command.stdout(Stdio::piped());
    command.stderr(Stdio::piped());
    command.process_group(0);
    let mut child = command.spawn().context("spawn plugin process")?;
    if let Some(stdout) = child.stdout.take() {
        drain_bounded_log(
            stdout,
            layout
                .plugin_logs_dir(&plugin.manifest.name)
                .join("stdout.log"),
            DEFAULT_LOG_MAX_BYTES,
        );
    }
    if let Some(stderr) = child.stderr.take() {
        drain_bounded_log(
            stderr,
            layout
                .plugin_logs_dir(&plugin.manifest.name)
                .join("stderr.log"),
            DEFAULT_LOG_MAX_BYTES,
        );
    }
    Ok(child)
}

fn drain_bounded_log<R>(mut reader: R, path: PathBuf, max_bytes: u64)
where
    R: Read + Send + 'static,
{
    thread::spawn(move || {
        let _ = drain_bounded_log_inner(&mut reader, &path, max_bytes);
    });
}

fn drain_bounded_log_inner<R>(reader: &mut R, path: &Path, max_bytes: u64) -> Result<()>
where
    R: Read,
{
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(path)
        .with_context(|| format!("open plugin log {}", path.display()))?;
    set_private_file_permissions_if_exists(path)?;
    let mut written = 0_u64;
    let mut buffer = [0_u8; 8192];
    loop {
        let count = reader
            .read(&mut buffer)
            .with_context(|| format!("read stream for {}", path.display()))?;
        if count == 0 {
            break;
        }
        if written < max_bytes {
            let remaining = usize::try_from(max_bytes - written).unwrap_or(usize::MAX);
            let to_write = count.min(remaining);
            file.write_all(&buffer[..to_write])
                .with_context(|| format!("write plugin log {}", path.display()))?;
            written += u64::try_from(to_write).context("log byte count overflow")?;
        }
    }
    file.sync_all()
        .with_context(|| format!("sync plugin log {}", path.display()))?;
    Ok(())
}

fn record_plugin_start_failure(plugin: &mut SupervisedPlugin, now: i64) {
    plugin.status.pid = None;
    plugin.status.crash_count = plugin.status.crash_count.saturating_add(1);
    schedule_restart(plugin, now);
}

fn record_plugin_crash(plugin: &mut SupervisedPlugin, now: i64) {
    plugin.status.crash_count = plugin.status.crash_count.saturating_add(1);
    schedule_restart(plugin, now);
}

fn record_plugin_exit(plugin: &mut SupervisedPlugin, status: ExitStatus, now: i64) {
    plugin.status.last_exit = Some(status.to_string());
    if status.success() {
        plugin.status.state = PluginRuntimeState::Exited;
        plugin.status.restart_after_epoch = None;
        plugin.next_restart = None;
        plugin.current_backoff = Duration::from_millis(plugin.manifest.restart.initial_delay_ms);
    } else {
        record_plugin_crash(plugin, now);
    }
}

fn schedule_restart(plugin: &mut SupervisedPlugin, now: i64) {
    if !plugin.can_restart() {
        plugin.status.state = PluginRuntimeState::Failed;
        plugin.status.restart_after_epoch = None;
        plugin.next_restart = None;
        return;
    }
    let delay = plugin.current_backoff;
    plugin.status.state = PluginRuntimeState::BackingOff;
    plugin.status.restart_after_epoch = now
        .checked_add(i64::try_from(delay.as_secs()).unwrap_or(i64::MAX))
        .or(Some(i64::MAX));
    plugin.next_restart = Some(Instant::now() + delay);
    let doubled = delay.saturating_mul(2);
    let max = Duration::from_millis(plugin.manifest.restart.max_delay_ms);
    plugin.current_backoff = doubled.min(max);
}

fn accept_plugin_connections(
    layout: &FsLayout,
    listener: &UnixListener,
    shared: &Arc<Mutex<ServiceSharedState>>,
) -> Result<()> {
    loop {
        match listener.accept() {
            Ok((stream, _addr)) => {
                let layout = layout.clone();
                let shared = Arc::clone(shared);
                thread::spawn(move || {
                    let _ = handle_plugin_connection(&layout, stream, &shared);
                });
            }
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => return Ok(()),
            Err(error) => return Err(error).context("accept plugin RPC connection"),
        }
    }
}

fn handle_plugin_connection(
    layout: &FsLayout,
    mut stream: UnixStream,
    shared: &Arc<Mutex<ServiceSharedState>>,
) -> Result<()> {
    stream
        .set_read_timeout(Some(Duration::from_millis(DEFAULT_HANDSHAKE_TIMEOUT_MS)))
        .context("set plugin handshake read timeout")?;
    let peer_pid = unix_stream_peer_pid(&stream).context("read plugin RPC peer pid")?;
    let frame: PluginRpcRequestFrame =
        read_plugin_rpc_frame(&mut stream, PLUGIN_RPC_MAX_FRAME_BYTES)
            .map_err(|error| anyhow::anyhow!("read plugin hello: {error}"))?;
    let policy = plugin_handshake_policy(layout);
    let (response, identity) = validate_and_handle_hello(layout, shared, &frame, &policy, peer_pid);
    write_plugin_rpc_frame(&mut stream, &response, PLUGIN_RPC_MAX_FRAME_BYTES)
        .map_err(|error| anyhow::anyhow!("write plugin hello response: {error}"))?;
    if response.error.is_some() {
        return Ok(());
    }
    let identity = identity.context("successful plugin hello missing connection identity")?;
    stream
        .set_read_timeout(None)
        .context("clear plugin RPC read timeout")?;

    while let Ok(frame) =
        read_plugin_rpc_frame::<_, PluginRpcRequestFrame>(&mut stream, PLUGIN_RPC_MAX_FRAME_BYTES)
    {
        let response = handle_plugin_runtime_frame(shared, &identity, &frame);
        write_plugin_rpc_frame(&mut stream, &response, PLUGIN_RPC_MAX_FRAME_BYTES)
            .map_err(|error| anyhow::anyhow!("write plugin RPC response: {error}"))?;
    }
    Ok(())
}

fn validate_and_handle_hello(
    layout: &FsLayout,
    shared: &Arc<Mutex<ServiceSharedState>>,
    frame: &PluginRpcRequestFrame,
    policy: &PluginHandshakePolicy,
    peer_pid: Option<u32>,
) -> (PluginRpcResponseFrame, Option<PluginConnectionIdentity>) {
    let response = handle_plugin_hello_frame(frame, policy);
    if response.error.is_some() {
        return (response, None);
    }

    let validation = (|| -> Result<PluginConnectionIdentity> {
        let request: PluginHelloRequest = serde_json::from_value(frame.params.clone())
            .context("parse plugin hello request for supervisor validation")?;
        validate_id_path_component(&request.plugin_name, "plugin name")?;
        let mut state = shared
            .lock()
            .map_err(|_| anyhow::anyhow!("service state lock poisoned"))?;
        let plugin = state
            .plugins
            .get_mut(&request.plugin_name)
            .with_context(|| format!("plugin is not configured: {}", request.plugin_name))?;
        let expected_home = layout.plugin_home_dir(&request.plugin_name);
        if Path::new(&request.plugin_home) != expected_home {
            bail!(
                "plugin_home mismatch for {}: expected {}, got {}",
                request.plugin_name,
                expected_home.display(),
                request.plugin_home
            );
        }
        if !plugin.manifest.enabled {
            bail!("plugin is disabled: {}", request.plugin_name);
        }
        let Some(child) = plugin.process.as_ref() else {
            bail!(
                "plugin is not managed by this service: {}",
                request.plugin_name
            );
        };
        let expected_pid = child.id();
        if request.pid != expected_pid {
            bail!(
                "plugin pid mismatch for {}: expected {}, got {}",
                request.plugin_name,
                expected_pid,
                request.pid
            );
        }
        if let Some(peer_pid) = peer_pid
            && peer_pid != expected_pid
        {
            bail!(
                "plugin peer pid mismatch for {}: expected {}, got {}",
                request.plugin_name,
                expected_pid,
                peer_pid
            );
        }
        plugin.status.state = PluginRuntimeState::Running;
        plugin.status.pid = Some(expected_pid);
        plugin.status.release_id = Some(request.plugin_release_id.clone());
        plugin.status.instance_id = Some(request.plugin_instance_id.clone());
        plugin.status.last_healthy_at = Some(current_epoch_seconds()?);
        plugin.status.last_error = None;
        state.mark_dirty();
        Ok(PluginConnectionIdentity {
            plugin_name: request.plugin_name,
            pid: expected_pid,
            instance_id: request.plugin_instance_id,
        })
    })();

    match validation {
        Ok(identity) => (response, Some(identity)),
        Err(error) => (
            PluginRpcResponseFrame::failure(
                frame.id.clone(),
                PluginRpcError::new(PluginRpcErrorKind::PolicyBlocked, format!("{error:#}")),
            ),
            None,
        ),
    }
}

fn unix_stream_peer_pid(stream: &UnixStream) -> Result<Option<u32>> {
    unix_stream_peer_pid_impl(stream)
}

#[cfg(target_os = "macos")]
fn unix_stream_peer_pid_impl(stream: &UnixStream) -> Result<Option<u32>> {
    const SOL_LOCAL: libc::c_int = 0;
    const LOCAL_PEERPID: libc::c_int = 2;
    let mut pid: libc::pid_t = 0;
    let mut len = std::mem::size_of::<libc::pid_t>() as libc::socklen_t;
    let rc = unsafe {
        libc::getsockopt(
            stream.as_raw_fd(),
            SOL_LOCAL,
            LOCAL_PEERPID,
            (&mut pid as *mut libc::pid_t).cast(),
            &mut len,
        )
    };
    if rc != 0 {
        return Err(io::Error::last_os_error()).context("getsockopt LOCAL_PEERPID");
    }
    u32::try_from(pid)
        .map(Some)
        .context("plugin peer pid is out of range")
}

#[cfg(not(target_os = "macos"))]
fn unix_stream_peer_pid_impl(_stream: &UnixStream) -> Result<Option<u32>> {
    Ok(None)
}

fn handle_plugin_runtime_frame(
    shared: &Arc<Mutex<ServiceSharedState>>,
    identity: &PluginConnectionIdentity,
    frame: &PluginRpcRequestFrame,
) -> PluginRpcResponseFrame {
    if frame.method != PLUGIN_HEALTH_UPDATE_METHOD {
        return PluginRpcResponseFrame::failure(
            frame.id.clone(),
            PluginRpcError::new(
                PluginRpcErrorKind::MethodNotFound,
                format!("unsupported service method {}", frame.method),
            ),
        );
    }
    let now = match current_epoch_seconds() {
        Ok(now) => now,
        Err(error) => {
            return PluginRpcResponseFrame::failure(
                frame.id.clone(),
                PluginRpcError::internal(error),
            );
        }
    };
    match shared.lock() {
        Ok(mut state) => {
            if let Some(plugin) = state.plugins.get_mut(&identity.plugin_name) {
                if let Err(error) = validate_health_update_identity(plugin, identity, now) {
                    state.mark_dirty();
                    return PluginRpcResponseFrame::failure(
                        frame.id.clone(),
                        PluginRpcError::new(
                            PluginRpcErrorKind::PolicyBlocked,
                            format!("{error:#}"),
                        ),
                    );
                }
                plugin.status.last_healthy_at = Some(now);
                plugin.status.state = PluginRuntimeState::Running;
                state.mark_dirty();
                PluginRpcResponseFrame::success(frame.id.clone(), json!({ "accepted": true }))
            } else {
                PluginRpcResponseFrame::failure(
                    frame.id.clone(),
                    PluginRpcError::new(
                        PluginRpcErrorKind::PolicyBlocked,
                        format!("plugin is not configured: {}", identity.plugin_name),
                    ),
                )
            }
        }
        Err(_) => PluginRpcResponseFrame::failure(
            frame.id.clone(),
            PluginRpcError::internal("service state lock poisoned"),
        ),
    }
}

fn validate_health_update_identity(
    plugin: &mut SupervisedPlugin,
    identity: &PluginConnectionIdentity,
    now: i64,
) -> Result<()> {
    if plugin.status.instance_id.as_deref() != Some(identity.instance_id.as_str()) {
        bail!(
            "plugin instance mismatch for {}: expected {:?}, got {}",
            identity.plugin_name,
            plugin.status.instance_id,
            identity.instance_id
        );
    }
    let Some(child) = plugin.process.as_mut() else {
        bail!("plugin process is not active: {}", identity.plugin_name);
    };
    let current_pid = child.id();
    if current_pid != identity.pid {
        bail!(
            "plugin process mismatch for {}: expected {}, got {}",
            identity.plugin_name,
            current_pid,
            identity.pid
        );
    }
    match child.try_wait() {
        Ok(Some(status)) => {
            plugin.status.pid = None;
            plugin.process = None;
            record_plugin_exit(plugin, status, now);
            bail!("plugin process already exited: {}", identity.plugin_name);
        }
        Ok(None) => Ok(()),
        Err(error) => {
            plugin.status.last_error = Some(format!("wait plugin process: {error}"));
            plugin.status.pid = None;
            plugin.process = None;
            record_plugin_crash(plugin, now);
            bail!("failed to confirm plugin process: {error}");
        }
    }
}

fn plugin_handshake_policy(layout: &FsLayout) -> PluginHandshakePolicy {
    PluginHandshakePolicy {
        service_capabilities: vec![
            ServiceCapability::new("plugin-rpc-v1"),
            ServiceCapability::new("plugin-hello"),
            ServiceCapability::new("plugin-health-update"),
            ServiceCapability::new("plugin-supervisor-c2"),
        ],
        policy: PluginRpcPolicy::default(),
        daemon_endpoint: Some(DaemonEndpointHint::uds(
            layout.daemon_socket_path().display().to_string(),
        )),
        ..PluginHandshakePolicy::default()
    }
}

fn load_plugin_registry(layout: &FsLayout) -> Result<PluginRegistry> {
    let path = layout.plugin_registry_path();
    match fs::read(&path) {
        Ok(bytes) => {
            let registry = serde_json::from_slice(&bytes)
                .with_context(|| format!("parse {}", path.display()))?;
            Ok(registry)
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(PluginRegistry {
            schema_version: registry_schema_version(),
            plugins: Vec::new(),
        }),
        Err(error) => Err(error).with_context(|| format!("read {}", path.display())),
    }
}

pub fn validate_plugin_registry(layout: &FsLayout, registry: &PluginRegistry) -> Result<()> {
    if registry.schema_version != registry_schema_version() {
        bail!(
            "unsupported plugin registry schema_version {}; expected {}",
            registry.schema_version,
            registry_schema_version()
        );
    }
    let mut names = HashMap::<&str, ()>::new();
    for plugin in &registry.plugins {
        validate_id_path_component(&plugin.name, "plugin name")?;
        if names.insert(&plugin.name, ()).is_some() {
            bail!("duplicate plugin name: {}", plugin.name);
        }
        if !plugin.executable_path.is_absolute() {
            bail!(
                "plugin executable path must be absolute for {}",
                plugin.name
            );
        }
        if plugin.restart.initial_delay_ms == 0 {
            bail!(
                "restart.initial_delay_ms must be positive for {}",
                plugin.name
            );
        }
        if plugin.restart.max_delay_ms < plugin.restart.initial_delay_ms {
            bail!(
                "restart.max_delay_ms must be >= restart.initial_delay_ms for {}",
                plugin.name
            );
        }
        if plugin.restart.max_crashes == 0 {
            bail!("restart.max_crashes must be positive for {}", plugin.name);
        }
        for capability in &plugin.capabilities {
            validate_nonempty_ascii_token(capability, "plugin capability")?;
        }
        for key in plugin.environment.keys() {
            validate_environment_key(key)?;
        }
        let expected_home = layout.plugin_home_dir(&plugin.name);
        if expected_home
            .strip_prefix(layout.plugins_dir())
            .ok()
            .and_then(|relative| relative.components().next())
            .is_none()
        {
            bail!("plugin home path is not under plugin registry root");
        }
    }
    Ok(())
}

fn status_from_manifest(layout: &FsLayout, manifest: &PluginManifest) -> PluginRuntimeStatus {
    PluginRuntimeStatus {
        name: manifest.name.clone(),
        enabled: manifest.enabled,
        configured: true,
        state: if manifest.enabled {
            PluginRuntimeState::Exited
        } else {
            PluginRuntimeState::Disabled
        },
        pid: None,
        release_id: manifest.release_id.clone(),
        instance_id: None,
        crash_count: 0,
        restart_after_epoch: None,
        last_started_at: None,
        last_healthy_at: None,
        last_exit: None,
        last_error: None,
        plugin_home: layout.plugin_home_dir(&manifest.name).display().to_string(),
        stdout_log: layout
            .plugin_logs_dir(&manifest.name)
            .join("stdout.log")
            .display()
            .to_string(),
        stderr_log: layout
            .plugin_logs_dir(&manifest.name)
            .join("stderr.log")
            .display()
            .to_string(),
    }
}

fn read_plugin_status(layout: &FsLayout, plugin_name: &str) -> Result<Option<PluginRuntimeStatus>> {
    let path = layout.plugin_status_path(plugin_name);
    match fs::read(&path) {
        Ok(bytes) => serde_json::from_slice(&bytes)
            .with_context(|| format!("parse {}", path.display()))
            .map(Some),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error).with_context(|| format!("read {}", path.display())),
    }
}

fn persist_all_status_if_dirty(
    layout: &FsLayout,
    shared: &Arc<Mutex<ServiceSharedState>>,
) -> Result<()> {
    let statuses = {
        let mut state = shared
            .lock()
            .map_err(|_| anyhow::anyhow!("service state lock poisoned"))?;
        if !state.dirty {
            return Ok(());
        }
        state.dirty = false;
        state
            .plugins
            .values()
            .map(|plugin| (plugin.manifest.name.clone(), plugin.status.clone()))
            .collect::<Vec<_>>()
    };

    for (plugin_name, status) in statuses {
        layout.ensure_plugin_home(&plugin_name)?;
        let bytes = serde_json::to_vec_pretty(&status)?;
        atomic_write_private(&layout.plugin_status_path(&plugin_name), &bytes)?;
    }
    Ok(())
}

fn prepare_service_socket(path: &Path) -> Result<()> {
    if let Some(parent) = path.parent() {
        ensure_private_dir(parent)?;
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
        bail!("refusing to replace service socket not owned by current user");
    }
    match connect_unix_stream_until(path, Instant::now() + Duration::from_millis(250)) {
        Ok(_) => bail!("service socket is already active: {}", path.display()),
        Err(error) if error.kind() == io::ErrorKind::ConnectionRefused => {}
        Err(error) => bail!(
            "refusing to replace service socket with inconclusive liveness at {}: {error}",
            path.display()
        ),
    }
    fs::remove_file(path).with_context(|| format!("remove stale socket {}", path.display()))?;
    if let Some(parent) = path.parent() {
        sync_dir(parent)?;
    }
    Ok(())
}

fn connect_unix_stream_until(path: &Path, deadline: Instant) -> io::Result<UnixStream> {
    loop {
        match UnixStream::connect(path) {
            Ok(stream) => return Ok(stream),
            Err(error)
                if matches!(
                    error.kind(),
                    io::ErrorKind::ConnectionRefused | io::ErrorKind::NotFound
                ) =>
            {
                if Instant::now() >= deadline {
                    return Err(error);
                }
                thread::sleep(Duration::from_millis(10));
            }
            Err(error) => return Err(error),
        }
    }
}

fn effective_uid() -> u32 {
    unsafe { libc::geteuid() }
}

extern "C" fn handle_service_signal(_signal: libc::c_int) {
    SERVICE_TERMINATION_REQUESTED.store(true, Ordering::SeqCst);
}

fn install_service_signal_handlers() -> Result<ServiceSignalGuard> {
    SERVICE_TERMINATION_REQUESTED.store(false, Ordering::SeqCst);
    let mut guard = ServiceSignalGuard {
        previous_handlers: Vec::new(),
    };
    guard.install(libc::SIGINT)?;
    guard.install(libc::SIGTERM)?;
    Ok(guard)
}

struct ServiceSignalGuard {
    previous_handlers: Vec<(libc::c_int, libc::sighandler_t)>,
}

impl ServiceSignalGuard {
    fn install(&mut self, signal: libc::c_int) -> Result<()> {
        let previous = unsafe {
            libc::signal(
                signal,
                handle_service_signal as *const () as libc::sighandler_t,
            )
        };
        if previous == libc::SIG_ERR {
            return Err(io::Error::last_os_error())
                .with_context(|| format!("install signal handler for {signal}"));
        }
        self.previous_handlers.push((signal, previous));
        Ok(())
    }
}

impl Drop for ServiceSignalGuard {
    fn drop(&mut self) {
        for (signal, previous) in self.previous_handlers.iter().rev() {
            unsafe {
                libc::signal(*signal, *previous);
            }
        }
    }
}

fn service_termination_requested() -> bool {
    SERVICE_TERMINATION_REQUESTED.load(Ordering::SeqCst)
}

struct ServiceSocketCleanup {
    path: PathBuf,
}

impl Drop for ServiceSocketCleanup {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
        if let Some(parent) = self.path.parent() {
            let _ = sync_dir(parent);
        }
    }
}

struct ServiceProcessCleanup {
    layout: FsLayout,
    shared: Arc<Mutex<ServiceSharedState>>,
    done: bool,
}

impl ServiceProcessCleanup {
    fn new(layout: FsLayout, shared: Arc<Mutex<ServiceSharedState>>) -> Self {
        Self {
            layout,
            shared,
            done: false,
        }
    }

    fn shutdown(&mut self) -> Result<()> {
        if self.done {
            return Ok(());
        }
        shutdown_managed_plugins(&self.layout, &self.shared);
        self.done = true;
        Ok(())
    }
}

impl Drop for ServiceProcessCleanup {
    fn drop(&mut self) {
        if !self.done {
            shutdown_managed_plugins(&self.layout, &self.shared);
        }
    }
}

fn shutdown_managed_plugins(layout: &FsLayout, shared: &Arc<Mutex<ServiceSharedState>>) {
    let children = match shared.lock() {
        Ok(mut state) => {
            let mut children = Vec::new();
            for plugin in state.plugins.values_mut() {
                if let Some(child) = plugin.process.take() {
                    let pid = child.id();
                    plugin.status.pid = None;
                    plugin.status.state = PluginRuntimeState::Exited;
                    plugin.status.restart_after_epoch = None;
                    plugin.status.last_exit = Some("terminated by service shutdown".to_owned());
                    children.push((plugin.manifest.name.clone(), pid, child));
                }
            }
            if !children.is_empty() {
                state.mark_dirty();
            }
            children
        }
        Err(_) => return,
    };
    for (name, pid, mut child) in children {
        terminate_plugin_child_best_effort(&mut child, pid);
        let _ = layout.ensure_plugin_home(&name);
    }
}

fn terminate_plugin_child_best_effort(child: &mut Child, pid: u32) {
    signal_process_group(pid, libc::SIGTERM);
    let deadline = Instant::now() + PLUGIN_TERM_GRACE;
    while Instant::now() < deadline {
        if plugin_process_group_is_gone(child, pid) {
            let _ = child.wait();
            return;
        }
        thread::sleep(Duration::from_millis(20));
    }
    signal_process_group(pid, libc::SIGKILL);
    let kill_deadline = Instant::now() + PLUGIN_KILL_GRACE;
    while Instant::now() < kill_deadline {
        if plugin_process_group_is_gone(child, pid) {
            let _ = child.wait();
            return;
        }
        thread::sleep(Duration::from_millis(20));
    }
    let _ = child.kill();
    let _ = child.wait();
}

fn plugin_process_group_is_gone(child: &mut Child, pid: u32) -> bool {
    match child.try_wait() {
        Ok(Some(_)) => !process_group_exists(pid),
        Ok(None) => false,
        Err(_) => true,
    }
}

fn signal_process_group(pid: u32, signal: libc::c_int) -> bool {
    let pgid = pid as libc::pid_t;
    unsafe { libc::killpg(pgid, signal) == 0 }
}

fn process_group_exists(pid: u32) -> bool {
    let pgid = pid as libc::pid_t;
    if unsafe { libc::killpg(pgid, 0) } == 0 {
        return true;
    }
    !matches!(io::Error::last_os_error().raw_os_error(), Some(libc::ESRCH))
}

fn validate_nonempty_ascii_token(value: &str, name: &str) -> Result<()> {
    let valid = !value.is_empty()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'));
    if valid {
        Ok(())
    } else {
        bail!("{name} contains unsupported characters")
    }
}

fn validate_environment_key(value: &str) -> Result<()> {
    let valid = !value.is_empty()
        && !value.contains('=')
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_');
    if valid && !value.starts_with("CBTH_PLUGIN_") {
        Ok(())
    } else {
        bail!("environment key contains unsupported characters: {value}");
    }
}

fn current_epoch_seconds() -> Result<i64> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock is before unix epoch")?;
    i64::try_from(duration.as_secs()).context("epoch seconds overflow")
}

fn registry_schema_version() -> u32 {
    1
}

fn default_restart_initial_delay_ms() -> u64 {
    DEFAULT_RESTART_INITIAL_DELAY_MS
}

fn default_restart_max_delay_ms() -> u64 {
    DEFAULT_RESTART_MAX_DELAY_MS
}

fn default_restart_max_crashes() -> u32 {
    DEFAULT_RESTART_MAX_CRASHES
}

#[cfg(test)]
mod tests {
    use std::os::unix::net::UnixStream;

    use crate::plugin_rpc::{
        PLUGIN_RPC_PROTOCOL_VERSION_V1, PluginHelloRequest, PluginRpcRequestFrame,
        PluginRpcResponseFrame,
    };

    use super::*;

    fn layout_for_tempdir(dir: &tempfile::TempDir) -> FsLayout {
        FsLayout::resolve(Some(dir.path().join("home"))).expect("layout")
    }

    fn manifest(name: &str) -> PluginManifest {
        PluginManifest {
            name: name.to_owned(),
            executable_path: PathBuf::from("/bin/echo"),
            args: Vec::new(),
            enabled: true,
            release_id: Some("release-1".to_owned()),
            capabilities: vec!["health".to_owned()],
            restart: PluginRestartPolicy::default(),
            environment: HashMap::new(),
        }
    }

    #[test]
    fn registry_validation_rejects_unsafe_plugin_name() {
        let temp = tempfile::tempdir().expect("tempdir");
        let layout = layout_for_tempdir(&temp);
        let registry = PluginRegistry {
            schema_version: 1,
            plugins: vec![PluginManifest {
                name: "../bad".to_owned(),
                ..manifest("webex")
            }],
        };

        let error = validate_plugin_registry(&layout, &registry).expect_err("invalid registry");

        assert!(error.to_string().contains("plugin name"));
    }

    #[test]
    fn registry_validation_rejects_relative_executable() {
        let temp = tempfile::tempdir().expect("tempdir");
        let layout = layout_for_tempdir(&temp);
        let registry = PluginRegistry {
            schema_version: 1,
            plugins: vec![PluginManifest {
                executable_path: PathBuf::from("worker"),
                ..manifest("webex")
            }],
        };

        let error = validate_plugin_registry(&layout, &registry).expect_err("invalid registry");

        assert!(error.to_string().contains("absolute"));
    }

    #[test]
    fn registry_validation_rejects_reserved_plugin_environment() {
        let temp = tempfile::tempdir().expect("tempdir");
        let layout = layout_for_tempdir(&temp);
        let registry = PluginRegistry {
            schema_version: 1,
            plugins: vec![PluginManifest {
                environment: HashMap::from([(
                    "CBTH_PLUGIN_RPC_SOCKET".to_owned(),
                    "/tmp/other.sock".to_owned(),
                )]),
                ..manifest("webex")
            }],
        };

        let error = validate_plugin_registry(&layout, &registry).expect_err("invalid registry");

        assert!(error.to_string().contains("environment key"));
    }

    #[test]
    fn plugin_home_layout_is_private_and_under_plugins_root() {
        let temp = tempfile::tempdir().expect("tempdir");
        let layout = layout_for_tempdir(&temp);

        layout.ensure_plugin_home("webex").expect("plugin home");

        assert!(
            layout
                .plugin_home_dir("webex")
                .starts_with(layout.plugins_dir())
        );
        assert!(layout.plugin_state_dir("webex").is_dir());
        assert!(layout.plugin_logs_dir("webex").is_dir());
    }

    #[test]
    fn restart_backoff_doubles_until_cap() {
        let temp = tempfile::tempdir().expect("tempdir");
        let layout = layout_for_tempdir(&temp);
        let mut plugin = SupervisedPlugin::new(
            &layout,
            PluginManifest {
                restart: PluginRestartPolicy {
                    initial_delay_ms: 100,
                    max_delay_ms: 250,
                    max_crashes: 4,
                },
                ..manifest("webex")
            },
            1_000,
        );

        schedule_restart(&mut plugin, 1_000);
        assert_eq!(plugin.status.state, PluginRuntimeState::BackingOff);
        assert_eq!(plugin.current_backoff, Duration::from_millis(200));
        schedule_restart(&mut plugin, 1_001);
        assert_eq!(plugin.current_backoff, Duration::from_millis(250));
        schedule_restart(&mut plugin, 1_002);
        assert_eq!(plugin.current_backoff, Duration::from_millis(250));
    }

    #[test]
    fn startup_failures_increment_crash_count_until_failed() {
        let temp = tempfile::tempdir().expect("tempdir");
        let layout = layout_for_tempdir(&temp);
        let registry = PluginRegistry {
            schema_version: 1,
            plugins: vec![PluginManifest {
                executable_path: PathBuf::from("/definitely/missing/cbth-plugin"),
                restart: PluginRestartPolicy {
                    initial_delay_ms: 1,
                    max_delay_ms: 1,
                    max_crashes: 2,
                },
                ..manifest("webex")
            }],
        };
        let shared = Arc::new(Mutex::new(ServiceSharedState::new(
            &layout, &registry, 1_000,
        )));

        supervise_tick(&layout, &shared, 1_000).expect("first tick");
        {
            let mut state = shared.lock().expect("state");
            let plugin = state.plugins.get_mut("webex").expect("plugin");
            assert_eq!(plugin.status.crash_count, 1);
            assert_eq!(plugin.status.state, PluginRuntimeState::BackingOff);
            plugin.next_restart = Some(Instant::now());
        }

        supervise_tick(&layout, &shared, 1_001).expect("second tick");
        let state = shared.lock().expect("state");
        let plugin = state.plugins.get("webex").expect("plugin");
        assert_eq!(plugin.status.crash_count, 2);
        assert_eq!(plugin.status.state, PluginRuntimeState::Failed);
        assert_eq!(plugin.next_restart, None);
    }

    #[test]
    fn successful_restart_clears_restart_after_epoch() {
        let temp = tempfile::tempdir().expect("tempdir");
        let layout = layout_for_tempdir(&temp);
        let registry = PluginRegistry {
            schema_version: 1,
            plugins: vec![manifest("webex")],
        };
        let shared = Arc::new(Mutex::new(ServiceSharedState::new(
            &layout, &registry, 1_000,
        )));

        supervise_tick(&layout, &shared, 1_000).expect("tick");
        let state = shared.lock().expect("state");
        let plugin = state.plugins.get("webex").expect("plugin");
        assert_eq!(plugin.status.state, PluginRuntimeState::Starting);
        assert_eq!(plugin.status.restart_after_epoch, None);
        assert!(plugin.status.pid.is_some());
        drop(state);
        shutdown_managed_plugins(&layout, &shared);
    }

    #[test]
    fn status_serialization_roundtrips() {
        let temp = tempfile::tempdir().expect("tempdir");
        let layout = layout_for_tempdir(&temp);
        let status = status_from_manifest(&layout, &manifest("webex"));

        let encoded = serde_json::to_string(&status).expect("serialize");
        let decoded: PluginRuntimeStatus = serde_json::from_str(&encoded).expect("deserialize");

        assert_eq!(decoded, status);
    }

    #[test]
    fn prepare_service_socket_refuses_active_socket() {
        let temp = tempfile::tempdir().expect("tempdir");
        let layout = layout_for_tempdir(&temp);
        layout.ensure_run_dir().expect("run dir");
        let socket_path = layout.service_socket_path();
        let _listener = UnixListener::bind(&socket_path).expect("bind active socket");

        let error = prepare_service_socket(&socket_path).expect_err("active socket");

        assert!(error.to_string().contains("already active"));
        assert!(socket_path.exists());
    }

    #[test]
    fn service_run_does_not_overwrite_status_before_socket_bind() {
        let temp = tempfile::tempdir().expect("tempdir");
        let layout = layout_for_tempdir(&temp);
        layout.ensure_run_dir().expect("run dir");
        layout.ensure_plugin_home("webex").expect("plugin home");
        let registry = PluginRegistry {
            schema_version: 1,
            plugins: vec![manifest("webex")],
        };
        atomic_write_private(
            &layout.plugin_registry_path(),
            &serde_json::to_vec_pretty(&registry).expect("registry json"),
        )
        .expect("write registry");
        let mut existing = status_from_manifest(&layout, &manifest("webex"));
        existing.state = PluginRuntimeState::Running;
        existing.pid = Some(999);
        existing.instance_id = Some("existing".to_owned());
        atomic_write_private(
            &layout.plugin_status_path("webex"),
            &serde_json::to_vec_pretty(&existing).expect("status json"),
        )
        .expect("write status");
        let _listener = UnixListener::bind(layout.service_socket_path()).expect("bind active");

        let error = service_run(
            &layout,
            ServiceRunOptions {
                once: true,
                now: Some(2_000),
            },
        )
        .expect_err("active service socket");

        assert!(error.to_string().contains("already active"));
        let persisted = read_plugin_status(&layout, "webex")
            .expect("read status")
            .expect("status");
        assert_eq!(persisted.state, PluginRuntimeState::Running);
        assert_eq!(persisted.pid, Some(999));
        assert_eq!(persisted.instance_id.as_deref(), Some("existing"));
    }

    #[test]
    fn fake_plugin_hello_updates_status_and_returns_policy() {
        let temp = tempfile::tempdir().expect("tempdir");
        let layout = layout_for_tempdir(&temp);
        let registry = PluginRegistry {
            schema_version: 1,
            plugins: vec![manifest("webex")],
        };
        let shared = Arc::new(Mutex::new(ServiceSharedState::new(
            &layout, &registry, 1_000,
        )));
        layout.ensure_plugin_home("webex").expect("plugin home");
        let child = Command::new("/bin/sleep")
            .arg("30")
            .process_group(0)
            .spawn()
            .expect("spawn child");
        let child_pid = child.id();
        {
            let mut state = shared.lock().expect("state");
            let plugin = state.plugins.get_mut("webex").expect("plugin");
            plugin.status.pid = Some(child_pid);
            plugin.process = Some(child);
        }
        let hello = PluginHelloRequest {
            plugin_name: "webex".to_owned(),
            plugin_instance_id: "instance-1".to_owned(),
            plugin_release_id: "release-1".to_owned(),
            protocol_versions: vec![PLUGIN_RPC_PROTOCOL_VERSION_V1],
            capabilities: Vec::new(),
            plugin_home: layout.plugin_home_dir("webex").display().to_string(),
            pid: child_pid,
        };
        let frame = PluginRpcRequestFrame::plugin_hello("1", hello).expect("hello frame");

        let (response, identity) = validate_and_handle_hello(
            &layout,
            &shared,
            &frame,
            &plugin_handshake_policy(&layout),
            Some(child_pid),
        );

        assert!(response.error.is_none());
        assert_eq!(
            identity,
            Some(PluginConnectionIdentity {
                plugin_name: "webex".to_owned(),
                pid: child_pid,
                instance_id: "instance-1".to_owned(),
            })
        );
        let state = shared.lock().expect("state");
        let plugin = state.plugins.get("webex").expect("plugin");
        assert_eq!(plugin.status.state, PluginRuntimeState::Running);
        assert_eq!(plugin.status.pid, Some(child_pid));
        assert_eq!(plugin.status.instance_id.as_deref(), Some("instance-1"));
        drop(state);
        shutdown_managed_plugins(&layout, &shared);
    }

    #[test]
    fn health_update_accepts_current_plugin_identity() {
        let temp = tempfile::tempdir().expect("tempdir");
        let layout = layout_for_tempdir(&temp);
        let registry = PluginRegistry {
            schema_version: 1,
            plugins: vec![manifest("webex")],
        };
        let shared = Arc::new(Mutex::new(ServiceSharedState::new(
            &layout, &registry, 1_000,
        )));
        let child = Command::new("/bin/sleep")
            .arg("30")
            .process_group(0)
            .spawn()
            .expect("spawn child");
        let child_pid = child.id();
        {
            let mut state = shared.lock().expect("state");
            let plugin = state.plugins.get_mut("webex").expect("plugin");
            plugin.status.state = PluginRuntimeState::Starting;
            plugin.status.pid = Some(child_pid);
            plugin.status.instance_id = Some("instance-1".to_owned());
            plugin.process = Some(child);
        }
        let identity = PluginConnectionIdentity {
            plugin_name: "webex".to_owned(),
            pid: child_pid,
            instance_id: "instance-1".to_owned(),
        };
        let frame = PluginRpcRequestFrame::new("health-1", PLUGIN_HEALTH_UPDATE_METHOD, json!({}));

        let response = handle_plugin_runtime_frame(&shared, &identity, &frame);

        assert!(response.error.is_none());
        let state = shared.lock().expect("state");
        let plugin = state.plugins.get("webex").expect("plugin");
        assert_eq!(plugin.status.state, PluginRuntimeState::Running);
        assert!(plugin.status.last_healthy_at.is_some());
        drop(state);
        shutdown_managed_plugins(&layout, &shared);
    }

    #[test]
    fn health_update_rejects_stale_connection_without_current_process() {
        let temp = tempfile::tempdir().expect("tempdir");
        let layout = layout_for_tempdir(&temp);
        let registry = PluginRegistry {
            schema_version: 1,
            plugins: vec![manifest("webex")],
        };
        let shared = Arc::new(Mutex::new(ServiceSharedState::new(
            &layout, &registry, 1_000,
        )));
        {
            let mut state = shared.lock().expect("state");
            let plugin = state.plugins.get_mut("webex").expect("plugin");
            plugin.status.state = PluginRuntimeState::Running;
            plugin.status.pid = Some(4242);
            plugin.status.instance_id = Some("instance-1".to_owned());
        }
        let identity = PluginConnectionIdentity {
            plugin_name: "webex".to_owned(),
            pid: 4242,
            instance_id: "instance-1".to_owned(),
        };
        let frame = PluginRpcRequestFrame::new("health-1", PLUGIN_HEALTH_UPDATE_METHOD, json!({}));

        let response = handle_plugin_runtime_frame(&shared, &identity, &frame);

        assert!(response.error.is_some());
        let state = shared.lock().expect("state");
        let plugin = state.plugins.get("webex").expect("plugin");
        assert_eq!(plugin.status.state, PluginRuntimeState::Running);
        assert_eq!(plugin.status.last_healthy_at, None);
    }

    #[test]
    fn health_update_rejects_stale_plugin_instance() {
        let temp = tempfile::tempdir().expect("tempdir");
        let layout = layout_for_tempdir(&temp);
        let registry = PluginRegistry {
            schema_version: 1,
            plugins: vec![manifest("webex")],
        };
        let shared = Arc::new(Mutex::new(ServiceSharedState::new(
            &layout, &registry, 1_000,
        )));
        let child = Command::new("/bin/sleep")
            .arg("30")
            .process_group(0)
            .spawn()
            .expect("spawn child");
        let child_pid = child.id();
        {
            let mut state = shared.lock().expect("state");
            let plugin = state.plugins.get_mut("webex").expect("plugin");
            plugin.status.state = PluginRuntimeState::Running;
            plugin.status.pid = Some(child_pid);
            plugin.status.instance_id = Some("current-instance".to_owned());
            plugin.process = Some(child);
        }
        let stale_identity = PluginConnectionIdentity {
            plugin_name: "webex".to_owned(),
            pid: child_pid,
            instance_id: "old-instance".to_owned(),
        };
        let frame = PluginRpcRequestFrame::new("health-1", PLUGIN_HEALTH_UPDATE_METHOD, json!({}));

        let response = handle_plugin_runtime_frame(&shared, &stale_identity, &frame);

        assert!(response.error.is_some());
        assert!(
            response
                .error
                .as_ref()
                .expect("error")
                .message
                .contains("instance mismatch")
        );
        let state = shared.lock().expect("state");
        let plugin = state.plugins.get("webex").expect("plugin");
        assert_eq!(plugin.status.last_healthy_at, None);
        drop(state);
        shutdown_managed_plugins(&layout, &shared);
    }

    #[test]
    fn plugin_hello_rejects_non_current_child_pid() {
        let temp = tempfile::tempdir().expect("tempdir");
        let layout = layout_for_tempdir(&temp);
        let registry = PluginRegistry {
            schema_version: 1,
            plugins: vec![manifest("webex")],
        };
        let shared = Arc::new(Mutex::new(ServiceSharedState::new(
            &layout, &registry, 1_000,
        )));
        layout.ensure_plugin_home("webex").expect("plugin home");
        let child = Command::new("/bin/sleep")
            .arg("30")
            .process_group(0)
            .spawn()
            .expect("spawn child");
        let child_pid = child.id();
        {
            let mut state = shared.lock().expect("state");
            let plugin = state.plugins.get_mut("webex").expect("plugin");
            plugin.status.pid = Some(child_pid);
            plugin.process = Some(child);
        }
        let stale_pid = child_pid.saturating_add(1);
        let hello = PluginHelloRequest {
            plugin_name: "webex".to_owned(),
            plugin_instance_id: "instance-1".to_owned(),
            plugin_release_id: "release-1".to_owned(),
            protocol_versions: vec![PLUGIN_RPC_PROTOCOL_VERSION_V1],
            capabilities: Vec::new(),
            plugin_home: layout.plugin_home_dir("webex").display().to_string(),
            pid: stale_pid,
        };
        let frame = PluginRpcRequestFrame::plugin_hello("1", hello).expect("hello frame");

        let (response, identity) = validate_and_handle_hello(
            &layout,
            &shared,
            &frame,
            &plugin_handshake_policy(&layout),
            Some(stale_pid),
        );

        assert!(response.error.is_some());
        assert_eq!(identity, None);
        assert!(
            response
                .error
                .as_ref()
                .expect("error")
                .message
                .contains("pid mismatch")
        );
        let state = shared.lock().expect("state");
        let plugin = state.plugins.get("webex").expect("plugin");
        assert_eq!(plugin.status.state, PluginRuntimeState::Starting);
        assert_eq!(plugin.status.instance_id, None);
        drop(state);
        shutdown_managed_plugins(&layout, &shared);
    }

    #[test]
    fn service_shutdown_reaps_managed_plugin_child() {
        let temp = tempfile::tempdir().expect("tempdir");
        let layout = layout_for_tempdir(&temp);
        let registry = PluginRegistry {
            schema_version: 1,
            plugins: vec![manifest("webex")],
        };
        let shared = Arc::new(Mutex::new(ServiceSharedState::new(
            &layout, &registry, 1_000,
        )));
        let child = Command::new("/bin/sleep")
            .arg("30")
            .process_group(0)
            .spawn()
            .expect("spawn child");
        let pid = child.id();
        {
            let mut state = shared.lock().expect("state");
            let plugin = state.plugins.get_mut("webex").expect("plugin");
            plugin.status.pid = Some(pid);
            plugin.process = Some(child);
        }

        shutdown_managed_plugins(&layout, &shared);

        assert!(!process_group_exists(pid));
        let state = shared.lock().expect("state");
        let plugin = state.plugins.get("webex").expect("plugin");
        assert!(plugin.process.is_none());
        assert_eq!(plugin.status.pid, None);
        assert_eq!(plugin.status.last_healthy_at, None);
    }

    #[test]
    fn failed_plugin_hello_does_not_mark_plugin_running() {
        let temp = tempfile::tempdir().expect("tempdir");
        let layout = layout_for_tempdir(&temp);
        let registry = PluginRegistry {
            schema_version: 1,
            plugins: vec![manifest("webex")],
        };
        let shared = Arc::new(Mutex::new(ServiceSharedState::new(
            &layout, &registry, 1_000,
        )));
        layout.ensure_plugin_home("webex").expect("plugin home");
        let (mut client, server) = UnixStream::pair().expect("socket pair");
        let server_layout = layout.clone();
        let server_shared = Arc::clone(&shared);
        let worker = thread::spawn(move || {
            handle_plugin_connection(&server_layout, server, &server_shared).expect("connection");
        });
        let hello = PluginHelloRequest {
            plugin_name: "webex".to_owned(),
            plugin_instance_id: "instance-1".to_owned(),
            plugin_release_id: "release-1".to_owned(),
            protocol_versions: vec![999],
            capabilities: Vec::new(),
            plugin_home: layout.plugin_home_dir("webex").display().to_string(),
            pid: 123,
        };
        let frame = PluginRpcRequestFrame::plugin_hello("1", hello).expect("hello frame");

        write_plugin_rpc_frame(&mut client, &frame, PLUGIN_RPC_MAX_FRAME_BYTES)
            .expect("write hello");
        let response: PluginRpcResponseFrame =
            read_plugin_rpc_frame(&mut client, PLUGIN_RPC_MAX_FRAME_BYTES).expect("response");
        drop(client);
        worker.join().expect("join worker");

        assert!(response.error.is_some());
        let state = shared.lock().expect("state");
        let plugin = state.plugins.get("webex").expect("plugin");
        assert_eq!(plugin.status.state, PluginRuntimeState::Starting);
        assert_eq!(plugin.status.pid, None);
        assert_eq!(plugin.status.instance_id, None);
    }
}
