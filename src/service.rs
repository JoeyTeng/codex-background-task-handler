use std::collections::{HashMap, HashSet};
use std::fs::{self, OpenOptions};
use std::io::{self, Read, Write};
#[cfg(any(target_os = "linux", target_os = "macos"))]
use std::os::fd::AsRawFd;
use std::os::unix::fs::{FileTypeExt, MetadataExt};
use std::os::unix::net::{UnixListener, UnixStream};
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, AtomicU64, Ordering},
};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

use crate::daemon::{
    DaemonEndpoint, DaemonEnsureOptions, daemon_endpoint_from_response, daemon_ensure,
    daemon_request_payload_timeout_at_endpoint, error_is_daemon_endpoint_gone,
};
use crate::fs_layout::{
    FsLayout, atomic_write_private, ensure_private_dir, set_private_file_permissions_if_exists,
    sync_dir, validate_id_path_component,
};
use crate::plugin_rpc::{
    DaemonEndpointHint, PLUGIN_RPC_APP_SERVER_ENSURE_METHOD, PLUGIN_RPC_APP_SERVER_REFRESH_METHOD,
    PLUGIN_RPC_APP_SERVER_STOP_METHOD, PLUGIN_RPC_MAX_FRAME_BYTES, PluginAppServerEnsureRequest,
    PluginAppServerRefreshRequest, PluginAppServerStopRequest, PluginHandshakePolicy,
    PluginHelloRequest, PluginRpcError, PluginRpcErrorKind, PluginRpcPolicy, PluginRpcRequestFrame,
    PluginRpcResponseFrame, ServiceCapability, handle_plugin_hello_frame, read_plugin_rpc_frame,
    write_plugin_rpc_frame,
};

const SERVICE_IDLE_POLL_INTERVAL: Duration = Duration::from_millis(200);
const DEFAULT_RESTART_INITIAL_DELAY_MS: u64 = 500;
const DEFAULT_RESTART_MAX_DELAY_MS: u64 = 30_000;
const DEFAULT_RESTART_MAX_CRASHES: u32 = 32;
const DEFAULT_LOG_MAX_BYTES: u64 = 1024 * 1024;
const DEFAULT_HANDSHAKE_TIMEOUT_MS: u64 = 5_000;
const SERVICE_DAEMON_IDLE_TIMEOUT_SECONDS: u64 = 300;
const SERVICE_DAEMON_STARTUP_TIMEOUT_SECONDS: u64 = 15;
const PLUGIN_APP_SERVER_ENSURE_TIMEOUT: Duration = Duration::from_secs(15);
const PLUGIN_APP_SERVER_CONTROL_TIMEOUT: Duration = Duration::from_secs(5);
const DEFAULT_PLUGIN_APP_SERVER_LEASE_TTL_SECONDS: u64 = 60;
const MAX_PLUGIN_APP_SERVER_LEASE_TTL_SECONDS: u64 = 300;
const PLUGIN_TERM_GRACE: Duration = Duration::from_millis(500);
const PLUGIN_KILL_GRACE: Duration = Duration::from_secs(2);
const PLUGIN_HEALTH_UPDATE_METHOD: &str = "plugin.health.update";
static SERVICE_TERMINATION_REQUESTED: AtomicBool = AtomicBool::new(false);
static PLUGIN_APP_SERVER_CONNECTION_COUNTER: AtomicU64 = AtomicU64::new(1);

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

    fn new_reconciled(layout: &FsLayout, manifest: PluginManifest, now: i64) -> Result<Self> {
        let mut plugin = Self::new(layout, manifest, now);
        reconcile_persisted_plugin_process(layout, &mut plugin)?;
        Ok(plugin)
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

    let socket_path = layout.service_socket_path();
    prepare_service_socket(&socket_path)?;
    let listener = UnixListener::bind(&socket_path)
        .with_context(|| format!("bind service socket {}", socket_path.display()))?;
    let _cleanup = ServiceSocketCleanup { path: socket_path };
    listener
        .set_nonblocking(true)
        .with_context(|| format!("set nonblocking service socket {}", _cleanup.path.display()))?;
    let shared = Arc::new(Mutex::new(ServiceSharedState::new(layout, &registry, now)?));
    let app_server_leases = Arc::new(Mutex::new(PluginAppServerLeaseRegistry::default()));
    persist_all_status_if_dirty(layout, &shared)?;
    let mut process_cleanup = ServiceProcessCleanup::new(layout.clone(), Arc::clone(&shared));

    loop {
        let now = current_epoch_seconds()?;
        supervise_tick(layout, &shared, now)?;
        accept_plugin_connections(layout, &listener, &shared, &app_server_leases)?;
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
    cleanup_all_plugin_app_server_leases(layout, &app_server_leases);
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
                apply_manifest_status_overlay(&mut status, &manifest);
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

#[derive(Default)]
struct PluginAppServerLeaseRegistry {
    leases: HashMap<PluginAppServerLeaseKey, PluginAppServerSharedLeaseRecord>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct PluginConnectionIdentity {
    plugin_name: String,
    pid: u32,
    instance_id: String,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct PluginAppServerLeaseKey {
    plugin_name: String,
    instance_id: String,
    lease_id: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct PluginAppServerLeaseTarget {
    managed_session_id: String,
    bound_thread_id: String,
    session_epoch: i64,
}

#[derive(Clone, Debug)]
struct PluginAppServerLeaseRecord {
    target: PluginAppServerLeaseTarget,
    endpoint: DaemonEndpoint,
    scoped_lease_id: String,
    endpoint_confirmed: bool,
}

#[derive(Clone, Debug)]
struct PluginAppServerSharedLeaseRecord {
    target: PluginAppServerLeaseTarget,
    endpoint: DaemonEndpoint,
    scoped_lease_id: String,
    endpoint_confirmed: bool,
    holders: HashSet<u64>,
}

impl PluginAppServerSharedLeaseRecord {
    fn from_record(record: PluginAppServerLeaseRecord, holder: u64) -> Self {
        Self {
            target: record.target,
            endpoint: record.endpoint,
            scoped_lease_id: record.scoped_lease_id,
            endpoint_confirmed: record.endpoint_confirmed,
            holders: HashSet::from([holder]),
        }
    }

    fn to_record(&self) -> PluginAppServerLeaseRecord {
        PluginAppServerLeaseRecord {
            target: self.target.clone(),
            endpoint: self.endpoint.clone(),
            scoped_lease_id: self.scoped_lease_id.clone(),
            endpoint_confirmed: self.endpoint_confirmed,
        }
    }
}

trait PluginAppServerLeaseBroker {
    fn ensure(
        &mut self,
        identity: &PluginConnectionIdentity,
        request: PluginAppServerEnsureRequest,
    ) -> Result<Value, PluginRpcError>;

    fn refresh(
        &mut self,
        identity: &PluginConnectionIdentity,
        request: PluginAppServerRefreshRequest,
    ) -> Result<Value, PluginRpcError>;

    fn stop(
        &mut self,
        identity: &PluginConnectionIdentity,
        request: PluginAppServerStopRequest,
    ) -> Result<Value, PluginRpcError>;

    fn cleanup_connection_leases(&mut self);
}

struct DaemonPluginAppServerLeaseBroker<'a> {
    layout: &'a FsLayout,
    connection_id: u64,
    registry: Arc<Mutex<PluginAppServerLeaseRegistry>>,
    leases: HashMap<PluginAppServerLeaseKey, PluginAppServerLeaseRecord>,
    seen_targets: HashMap<PluginAppServerLeaseKey, PluginAppServerLeaseTarget>,
}

impl<'a> DaemonPluginAppServerLeaseBroker<'a> {
    fn new(layout: &'a FsLayout, registry: Arc<Mutex<PluginAppServerLeaseRegistry>>) -> Self {
        Self {
            layout,
            connection_id: next_plugin_app_server_connection_id(),
            registry,
            leases: HashMap::new(),
            seen_targets: HashMap::new(),
        }
    }

    fn ensure_daemon_endpoint(&self) -> Result<(DaemonEndpoint, Value), PluginRpcError> {
        let ensured = daemon_ensure(
            self.layout,
            DaemonEnsureOptions {
                idle_timeout_seconds: SERVICE_DAEMON_IDLE_TIMEOUT_SECONDS,
                startup_timeout_seconds: SERVICE_DAEMON_STARTUP_TIMEOUT_SECONDS,
                startup_sweep_now: Some(current_epoch_seconds().map_err(PluginRpcError::internal)?),
                replace_incompatible: false,
            },
        )
        .map_err(daemon_dispatch_error)?;
        let endpoint = daemon_endpoint_from_response(self.layout, &ensured)
            .map_err(|error| PluginRpcError::internal(format!("{error:#}")))?;
        Ok((endpoint, ensured))
    }

    fn ensure_app_server_at_endpoint(
        &self,
        endpoint: &DaemonEndpoint,
        request: &PluginAppServerEnsureRequest,
        scoped_lease_id: &str,
    ) -> Result<Value, anyhow::Error> {
        let payload = plugin_app_server_ensure_payload(request, scoped_lease_id);
        match daemon_request_payload_timeout_at_endpoint(
            self.layout,
            endpoint,
            "cli_app_server_ensure",
            payload.clone(),
            PLUGIN_APP_SERVER_ENSURE_TIMEOUT,
        ) {
            Ok(response) => Ok(response),
            Err(error) if daemon_error_needs_app_server_reservation(&error) => {
                daemon_request_payload_timeout_at_endpoint(
                    self.layout,
                    endpoint,
                    "cli_app_server_reserve",
                    plugin_app_server_reserve_payload(request, scoped_lease_id),
                    PLUGIN_APP_SERVER_CONTROL_TIMEOUT,
                )
                .context("reserve plugin app-server lease")?;
                match daemon_request_payload_timeout_at_endpoint(
                    self.layout,
                    endpoint,
                    "cli_app_server_ensure",
                    payload,
                    PLUGIN_APP_SERVER_ENSURE_TIMEOUT,
                ) {
                    Ok(response) => Ok(response),
                    Err(ensure_error) => {
                        release_plugin_app_server_reservation_best_effort(
                            self.layout,
                            endpoint,
                            &request.bound_thread_id,
                            scoped_lease_id,
                        );
                        Err(ensure_error)
                    }
                }
            }
            Err(error) => Err(error),
        }
    }

    fn refresh_app_server_at_endpoint(
        &self,
        endpoint: &DaemonEndpoint,
        record: &PluginAppServerLeaseRecord,
        lease_ttl_seconds: Option<u64>,
    ) -> Result<Value, anyhow::Error> {
        daemon_request_payload_timeout_at_endpoint(
            self.layout,
            endpoint,
            "cli_app_server_refresh",
            json!({
                "managed_session_id": record.target.managed_session_id.as_str(),
                "lease_id": record.scoped_lease_id.as_str(),
                "lease_ttl_seconds": lease_ttl_seconds
                    .or(Some(DEFAULT_PLUGIN_APP_SERVER_LEASE_TTL_SECONDS)),
            }),
            PLUGIN_APP_SERVER_CONTROL_TIMEOUT,
        )
    }

    fn follow_app_server_handoff(
        &self,
        endpoint: &DaemonEndpoint,
        command: &str,
        payload: Value,
        response: Value,
        timeout: Duration,
    ) -> Result<(DaemonEndpoint, Value), PluginRpcError> {
        let Some(handoff_socket_path) = app_server_handoff_socket_path(&response) else {
            return Ok((endpoint.clone(), response));
        };
        let handoff_endpoint = DaemonEndpoint::from_socket_path(PathBuf::from(handoff_socket_path));
        let handoff_response = daemon_request_payload_timeout_at_endpoint(
            self.layout,
            &handoff_endpoint,
            command,
            payload,
            timeout,
        )
        .map_err(daemon_dispatch_error)?;
        Ok((handoff_endpoint, handoff_response))
    }

    fn shared_lease_record(
        &self,
        key: &PluginAppServerLeaseKey,
        target: &PluginAppServerLeaseTarget,
    ) -> Result<Option<PluginAppServerLeaseRecord>, PluginRpcError> {
        let registry = self
            .registry
            .lock()
            .map_err(|_| PluginRpcError::internal("app-server lease registry lock poisoned"))?;
        let Some(record) = registry.leases.get(key) else {
            return Ok(None);
        };
        if record.target != *target {
            return Err(replayed_plugin_app_server_lease_target_error());
        }
        Ok(Some(record.to_record()))
    }

    fn register_connection_lease(
        &mut self,
        key: PluginAppServerLeaseKey,
        record: PluginAppServerLeaseRecord,
    ) -> Result<(), PluginRpcError> {
        {
            let mut registry = self
                .registry
                .lock()
                .map_err(|_| PluginRpcError::internal("app-server lease registry lock poisoned"))?;
            match registry.leases.get_mut(&key) {
                Some(shared) => {
                    if shared.target != record.target {
                        return Err(replayed_plugin_app_server_lease_target_error());
                    }
                    shared.endpoint = record.endpoint.clone();
                    shared.scoped_lease_id = record.scoped_lease_id.clone();
                    shared.endpoint_confirmed = record.endpoint_confirmed;
                    shared.holders.insert(self.connection_id);
                }
                None => {
                    registry.leases.insert(
                        key.clone(),
                        PluginAppServerSharedLeaseRecord::from_record(
                            record.clone(),
                            self.connection_id,
                        ),
                    );
                }
            }
        }
        self.seen_targets.insert(key.clone(), record.target.clone());
        self.leases.insert(key, record);
        Ok(())
    }

    fn update_connection_lease_endpoint(
        &mut self,
        key: &PluginAppServerLeaseKey,
        endpoint: DaemonEndpoint,
    ) -> Result<(), PluginRpcError> {
        if let Some(record) = self.leases.get_mut(key) {
            record.endpoint = endpoint.clone();
            record.endpoint_confirmed = true;
        }
        let mut registry = self
            .registry
            .lock()
            .map_err(|_| PluginRpcError::internal("app-server lease registry lock poisoned"))?;
        if let Some(shared) = registry.leases.get_mut(key)
            && shared.holders.contains(&self.connection_id)
        {
            shared.endpoint = endpoint;
            shared.endpoint_confirmed = true;
        }
        Ok(())
    }

    fn latest_connection_lease_record(
        &mut self,
        key: &PluginAppServerLeaseKey,
        fallback: PluginAppServerLeaseRecord,
    ) -> Result<PluginAppServerLeaseRecord, PluginRpcError> {
        let latest = {
            let registry = self
                .registry
                .lock()
                .map_err(|_| PluginRpcError::internal("app-server lease registry lock poisoned"))?;
            registry
                .leases
                .get(key)
                .filter(|shared| shared.holders.contains(&self.connection_id))
                .map(PluginAppServerSharedLeaseRecord::to_record)
        };
        if let Some(record) = latest {
            self.leases.insert(key.clone(), record.clone());
            Ok(record)
        } else {
            Ok(fallback)
        }
    }

    fn stop_app_server_record_best_effort(&self, record: &PluginAppServerLeaseRecord) {
        stop_plugin_app_server_record_best_effort(self.layout, record);
    }

    fn rollback_preregistered_lease_after_failed_ensure(
        &mut self,
        key: &PluginAppServerLeaseKey,
        fallback: PluginAppServerLeaseRecord,
        stop_last_holder: bool,
    ) {
        self.leases.remove(key);
        let Ok(mut registry) = self.registry.lock() else {
            return;
        };
        let stop_record = if let Some(shared) = registry.leases.get_mut(key) {
            shared.holders.remove(&self.connection_id);
            if !shared.holders.is_empty() {
                return;
            }
            shared.to_record()
        } else {
            fallback
        };
        if stop_last_holder {
            self.stop_app_server_record_best_effort(&stop_record);
        }
        registry.leases.remove(key);
    }
}

impl PluginAppServerLeaseBroker for DaemonPluginAppServerLeaseBroker<'_> {
    fn ensure(
        &mut self,
        identity: &PluginConnectionIdentity,
        request: PluginAppServerEnsureRequest,
    ) -> Result<Value, PluginRpcError> {
        validate_plugin_app_server_ensure_request(&request)?;
        let key = plugin_app_server_lease_key(identity, &request.lease_id);
        let target = plugin_app_server_lease_target(&request);
        let scoped_lease_id = scoped_plugin_app_server_lease_id(identity, &request.lease_id);
        if let Some(seen_target) = self.seen_targets.get(&key)
            && *seen_target != target
        {
            return Err(replayed_plugin_app_server_lease_target_error());
        }
        if let Some(record) = self.leases.get(&key)
            && record.target != target
        {
            return Err(replayed_plugin_app_server_lease_target_error());
        }

        let had_local_holder = self.leases.contains_key(&key);
        let existing_record = self
            .shared_lease_record(&key, &target)?
            .or_else(|| self.leases.get(&key).cloned());
        let pre_registered_holder = if had_local_holder {
            false
        } else {
            self.register_connection_lease(
                key.clone(),
                existing_record
                    .clone()
                    .unwrap_or_else(|| PluginAppServerLeaseRecord {
                        target: target.clone(),
                        endpoint: DaemonEndpoint::default(self.layout),
                        scoped_lease_id: scoped_lease_id.clone(),
                        endpoint_confirmed: false,
                    }),
            )?;
            true
        };
        let mut daemon_lease_accepted = false;
        let result = (|| {
            let (mut initial_endpoint, mut daemon) = match existing_record {
                Some(record) if record.endpoint_confirmed => (record.endpoint, Value::Null),
                _ => {
                    let (endpoint, daemon) = self.ensure_daemon_endpoint()?;
                    self.update_connection_lease_endpoint(&key, endpoint.clone())?;
                    (endpoint, daemon)
                }
            };
            let payload = plugin_app_server_ensure_payload(&request, &scoped_lease_id);
            let response = match self.ensure_app_server_at_endpoint(
                &initial_endpoint,
                &request,
                &scoped_lease_id,
            ) {
                Ok(response) => response,
                Err(error) if daemon_error_needs_endpoint_refresh(&error) => {
                    let (endpoint, ensured) = self.ensure_daemon_endpoint()?;
                    self.update_connection_lease_endpoint(&key, endpoint.clone())?;
                    initial_endpoint = endpoint;
                    daemon = ensured;
                    self.ensure_app_server_at_endpoint(
                        &initial_endpoint,
                        &request,
                        &scoped_lease_id,
                    )
                    .map_err(daemon_dispatch_error)?
                }
                Err(error) => return Err(daemon_dispatch_error(error)),
            };
            daemon_lease_accepted = true;
            let (endpoint, response) = self.follow_app_server_handoff(
                &initial_endpoint,
                "cli_app_server_ensure",
                payload,
                response,
                PLUGIN_APP_SERVER_ENSURE_TIMEOUT,
            )?;
            self.register_connection_lease(
                key.clone(),
                PluginAppServerLeaseRecord {
                    target,
                    endpoint: endpoint.clone(),
                    scoped_lease_id,
                    endpoint_confirmed: true,
                },
            )?;
            Ok(plugin_app_server_response(
                &request.lease_id,
                &endpoint,
                if daemon.is_null() { None } else { Some(daemon) },
                response,
            ))
        })();
        if result.is_err()
            && pre_registered_holder
            && let Some(record) = self.leases.get(&key).cloned()
        {
            self.rollback_preregistered_lease_after_failed_ensure(
                &key,
                record,
                daemon_lease_accepted,
            );
        }
        result
    }

    fn refresh(
        &mut self,
        identity: &PluginConnectionIdentity,
        request: PluginAppServerRefreshRequest,
    ) -> Result<Value, PluginRpcError> {
        validate_plugin_app_server_refresh_request(&request)?;
        let key = plugin_app_server_lease_key(identity, &request.lease_id);
        let record = self
            .leases
            .get(&key)
            .cloned()
            .ok_or_else(|| stale_plugin_app_server_lease(&request.lease_id))?;
        let record = self.latest_connection_lease_record(&key, record)?;
        if record.target.managed_session_id != request.managed_session_id {
            return Err(PluginRpcError::new(
                PluginRpcErrorKind::PolicyBlocked,
                "app-server lease refresh targets a different managed session",
            ));
        }
        let payload = json!({
            "managed_session_id": record.target.managed_session_id.as_str(),
            "lease_id": record.scoped_lease_id.as_str(),
            "lease_ttl_seconds": request
                .lease_ttl_seconds
                .or(Some(DEFAULT_PLUGIN_APP_SERVER_LEASE_TTL_SECONDS)),
        });
        let mut request_endpoint = record.endpoint.clone();
        let response = match self.refresh_app_server_at_endpoint(
            &request_endpoint,
            &record,
            request.lease_ttl_seconds,
        ) {
            Ok(response) => response,
            Err(error) if daemon_error_needs_endpoint_refresh(&error) => {
                let (endpoint, _) = self.ensure_daemon_endpoint()?;
                self.update_connection_lease_endpoint(&key, endpoint.clone())?;
                request_endpoint = endpoint;
                self.refresh_app_server_at_endpoint(
                    &request_endpoint,
                    &record,
                    request.lease_ttl_seconds,
                )
                .map_err(daemon_dispatch_error)?
            }
            Err(error) => return Err(daemon_dispatch_error(error)),
        };
        let (endpoint, response) = self.follow_app_server_handoff(
            &request_endpoint,
            "cli_app_server_refresh",
            payload,
            response,
            PLUGIN_APP_SERVER_CONTROL_TIMEOUT,
        )?;
        self.update_connection_lease_endpoint(&key, endpoint.clone())?;
        Ok(plugin_app_server_response(
            &request.lease_id,
            &endpoint,
            None,
            response,
        ))
    }

    fn stop(
        &mut self,
        identity: &PluginConnectionIdentity,
        request: PluginAppServerStopRequest,
    ) -> Result<Value, PluginRpcError> {
        validate_plugin_app_server_stop_request(&request)?;
        let key = plugin_app_server_lease_key(identity, &request.lease_id);
        let record = self
            .leases
            .get(&key)
            .cloned()
            .ok_or_else(|| stale_plugin_app_server_lease(&request.lease_id))?;
        let record = self.latest_connection_lease_record(&key, record)?;
        if record.target.managed_session_id != request.managed_session_id {
            return Err(PluginRpcError::new(
                PluginRpcErrorKind::PolicyBlocked,
                "app-server lease stop targets a different managed session",
            ));
        }
        let payload = json!({
            "managed_session_id": record.target.managed_session_id.as_str(),
            "lease_id": record.scoped_lease_id.as_str(),
        });
        let (endpoint, response) = {
            let mut registry = self
                .registry
                .lock()
                .map_err(|_| PluginRpcError::internal("app-server lease registry lock poisoned"))?;
            if let Some(shared) = registry.leases.get_mut(&key)
                && shared
                    .holders
                    .iter()
                    .any(|holder| *holder != self.connection_id)
            {
                shared.holders.remove(&self.connection_id);
                let retained = shared.to_record();
                drop(registry);
                self.leases.remove(&key);
                return Ok(plugin_app_server_retained_stop_response(
                    &request.lease_id,
                    &retained.endpoint,
                ));
            }
            let mut request_endpoint = record.endpoint.clone();
            let response = match daemon_request_payload_timeout_at_endpoint(
                self.layout,
                &request_endpoint,
                "cli_app_server_stop",
                payload.clone(),
                PLUGIN_APP_SERVER_CONTROL_TIMEOUT,
            ) {
                Ok(response) => response,
                Err(error) if daemon_error_needs_endpoint_refresh(&error) => {
                    let (endpoint, _) = self.ensure_daemon_endpoint()?;
                    request_endpoint = endpoint.clone();
                    if let Some(local) = self.leases.get_mut(&key) {
                        local.endpoint = endpoint.clone();
                        local.endpoint_confirmed = true;
                    }
                    if let Some(shared) = registry.leases.get_mut(&key) {
                        shared.endpoint = endpoint;
                        shared.endpoint_confirmed = true;
                    }
                    daemon_request_payload_timeout_at_endpoint(
                        self.layout,
                        &request_endpoint,
                        "cli_app_server_stop",
                        payload.clone(),
                        PLUGIN_APP_SERVER_CONTROL_TIMEOUT,
                    )
                    .map_err(daemon_dispatch_error)?
                }
                Err(error) => return Err(daemon_dispatch_error(error)),
            };
            let (endpoint, response) =
                if let Some(handoff_socket_path) = app_server_handoff_socket_path(&response) {
                    let handoff_endpoint =
                        DaemonEndpoint::from_socket_path(PathBuf::from(handoff_socket_path));
                    let handoff_response = daemon_request_payload_timeout_at_endpoint(
                        self.layout,
                        &handoff_endpoint,
                        "cli_app_server_stop",
                        payload,
                        PLUGIN_APP_SERVER_CONTROL_TIMEOUT,
                    )
                    .map_err(daemon_dispatch_error)?;
                    (handoff_endpoint, handoff_response)
                } else {
                    (request_endpoint, response)
                };
            registry.leases.remove(&key);
            (endpoint, response)
        };
        self.leases.remove(&key);
        Ok(plugin_app_server_stop_response(
            &request.lease_id,
            &endpoint,
            response,
        ))
    }

    fn cleanup_connection_leases(&mut self) {
        let records = self.leases.drain().collect::<Vec<_>>();
        for (key, record) in records {
            let Ok(mut registry) = self.registry.lock() else {
                continue;
            };
            let stop_record = if let Some(shared) = registry.leases.get_mut(&key) {
                shared.holders.remove(&self.connection_id);
                if !shared.holders.is_empty() {
                    continue;
                }
                shared.to_record()
            } else {
                record
            };
            self.stop_app_server_record_best_effort(&stop_record);
            registry.leases.remove(&key);
        }
    }
}

fn stop_plugin_app_server_record_best_effort(
    layout: &FsLayout,
    record: &PluginAppServerLeaseRecord,
) {
    let payload = json!({
        "managed_session_id": record.target.managed_session_id.as_str(),
        "lease_id": record.scoped_lease_id.as_str(),
    });
    if let Ok(response) = daemon_request_payload_timeout_at_endpoint(
        layout,
        &record.endpoint,
        "cli_app_server_stop",
        payload.clone(),
        PLUGIN_APP_SERVER_CONTROL_TIMEOUT,
    ) && let Some(handoff_socket_path) = app_server_handoff_socket_path(&response)
    {
        let handoff_endpoint = DaemonEndpoint::from_socket_path(PathBuf::from(handoff_socket_path));
        let _ = daemon_request_payload_timeout_at_endpoint(
            layout,
            &handoff_endpoint,
            "cli_app_server_stop",
            payload,
            PLUGIN_APP_SERVER_CONTROL_TIMEOUT,
        );
    }
}

fn cleanup_all_plugin_app_server_leases(
    layout: &FsLayout,
    registry: &Arc<Mutex<PluginAppServerLeaseRegistry>>,
) {
    let records = match registry.lock() {
        Ok(mut registry) => registry
            .leases
            .drain()
            .map(|(_, record)| record.to_record())
            .collect::<Vec<_>>(),
        Err(_) => return,
    };
    for record in records {
        stop_plugin_app_server_record_best_effort(layout, &record);
    }
}

fn next_plugin_app_server_connection_id() -> u64 {
    PLUGIN_APP_SERVER_CONNECTION_COUNTER.fetch_add(1, Ordering::Relaxed)
}

fn replayed_plugin_app_server_lease_target_error() -> PluginRpcError {
    PluginRpcError::new(
        PluginRpcErrorKind::PolicyBlocked,
        "app-server lease replay targets a different managed session, thread, or epoch",
    )
}

fn validate_plugin_app_server_ensure_request(
    request: &PluginAppServerEnsureRequest,
) -> Result<(), PluginRpcError> {
    validate_nonempty_rpc_field("managed_session_id", &request.managed_session_id)?;
    validate_nonempty_rpc_field("bound_thread_id", &request.bound_thread_id)?;
    validate_positive_rpc_i64("session_epoch", request.session_epoch)?;
    validate_nonempty_rpc_field("codex_binary", &request.codex_binary)?;
    validate_lease_id(&request.lease_id)?;
    validate_optional_lease_ttl(request.lease_ttl_seconds)
}

fn validate_plugin_app_server_refresh_request(
    request: &PluginAppServerRefreshRequest,
) -> Result<(), PluginRpcError> {
    validate_nonempty_rpc_field("managed_session_id", &request.managed_session_id)?;
    validate_lease_id(&request.lease_id)?;
    validate_optional_lease_ttl(request.lease_ttl_seconds)
}

fn validate_plugin_app_server_stop_request(
    request: &PluginAppServerStopRequest,
) -> Result<(), PluginRpcError> {
    validate_nonempty_rpc_field("managed_session_id", &request.managed_session_id)?;
    validate_lease_id(&request.lease_id)
}

fn validate_nonempty_rpc_field(name: &str, value: &str) -> Result<(), PluginRpcError> {
    if value.is_empty() {
        Err(PluginRpcError::new(
            PluginRpcErrorKind::InvalidRequest,
            format!("{name} must not be empty"),
        ))
    } else {
        Ok(())
    }
}

fn validate_positive_rpc_i64(name: &str, value: i64) -> Result<(), PluginRpcError> {
    if value > 0 {
        Ok(())
    } else {
        Err(PluginRpcError::new(
            PluginRpcErrorKind::InvalidRequest,
            format!("{name} must be positive"),
        ))
    }
}

fn validate_lease_id(value: &str) -> Result<(), PluginRpcError> {
    validate_nonempty_ascii_token(value, "lease_id").map_err(|error| {
        PluginRpcError::new(PluginRpcErrorKind::InvalidRequest, format!("{error:#}"))
    })
}

fn validate_optional_lease_ttl(value: Option<u64>) -> Result<(), PluginRpcError> {
    match value {
        Some(0) => Err(PluginRpcError::new(
            PluginRpcErrorKind::InvalidRequest,
            "lease_ttl_seconds must be positive",
        )),
        Some(seconds) if seconds > MAX_PLUGIN_APP_SERVER_LEASE_TTL_SECONDS => {
            Err(PluginRpcError::new(
                PluginRpcErrorKind::InvalidRequest,
                format!(
                    "lease_ttl_seconds must not exceed {MAX_PLUGIN_APP_SERVER_LEASE_TTL_SECONDS}"
                ),
            ))
        }
        Some(_) | None => Ok(()),
    }
}

fn plugin_app_server_lease_key(
    identity: &PluginConnectionIdentity,
    lease_id: &str,
) -> PluginAppServerLeaseKey {
    PluginAppServerLeaseKey {
        plugin_name: identity.plugin_name.clone(),
        instance_id: identity.instance_id.clone(),
        lease_id: lease_id.to_owned(),
    }
}

fn plugin_app_server_lease_target(
    request: &PluginAppServerEnsureRequest,
) -> PluginAppServerLeaseTarget {
    PluginAppServerLeaseTarget {
        managed_session_id: request.managed_session_id.clone(),
        bound_thread_id: request.bound_thread_id.clone(),
        session_epoch: request.session_epoch,
    }
}

fn scoped_plugin_app_server_lease_id(
    identity: &PluginConnectionIdentity,
    plugin_lease_id: &str,
) -> String {
    let mut hasher = Sha256::new();
    update_scoped_hash_field(&mut hasher, &identity.plugin_name);
    update_scoped_hash_field(&mut hasher, &identity.instance_id);
    update_scoped_hash_field(&mut hasher, plugin_lease_id);
    let digest = hasher.finalize();
    format!("plugin-{}", lowercase_hex(&digest[..16]))
}

fn update_scoped_hash_field(hasher: &mut Sha256, value: &str) {
    hasher.update(value.len().to_le_bytes());
    hasher.update(value.as_bytes());
}

fn lowercase_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(HEX[(byte >> 4) as usize] as char);
        output.push(HEX[(byte & 0x0f) as usize] as char);
    }
    output
}

fn plugin_app_server_ensure_payload(
    request: &PluginAppServerEnsureRequest,
    scoped_lease_id: &str,
) -> Value {
    json!({
        "managed_session_id": request.managed_session_id.as_str(),
        "bound_thread_id": request.bound_thread_id.as_str(),
        "session_epoch": request.session_epoch,
        "codex_binary": request.codex_binary.as_bytes(),
        "lease_id": scoped_lease_id,
        "lease_ttl_seconds": request
            .lease_ttl_seconds
            .or(Some(DEFAULT_PLUGIN_APP_SERVER_LEASE_TTL_SECONDS)),
    })
}

fn plugin_app_server_reserve_payload(
    request: &PluginAppServerEnsureRequest,
    scoped_lease_id: &str,
) -> Value {
    json!({
        "bound_thread_id": request.bound_thread_id.as_str(),
        "lease_id": scoped_lease_id,
        "lease_ttl_seconds": request
            .lease_ttl_seconds
            .or(Some(DEFAULT_PLUGIN_APP_SERVER_LEASE_TTL_SECONDS)),
    })
}

fn release_plugin_app_server_reservation_best_effort(
    layout: &FsLayout,
    endpoint: &DaemonEndpoint,
    bound_thread_id: &str,
    scoped_lease_id: &str,
) {
    let _ = daemon_request_payload_timeout_at_endpoint(
        layout,
        endpoint,
        "cli_app_server_release",
        json!({
            "bound_thread_id": bound_thread_id,
            "lease_id": scoped_lease_id,
        }),
        PLUGIN_APP_SERVER_CONTROL_TIMEOUT,
    );
}

fn app_server_handoff_socket_path(response: &Value) -> Option<String> {
    response
        .get("cli_app_server")
        .and_then(|server| server.get("handoff_daemon_socket_path"))
        .or_else(|| response.get("handoff_daemon_socket_path"))
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
}

fn plugin_app_server_response(
    plugin_lease_id: &str,
    endpoint: &DaemonEndpoint,
    daemon_ensure: Option<Value>,
    daemon_response: Value,
) -> Value {
    let mut response = json!({
        "lease_id": plugin_lease_id,
        "daemon": {
            "socket_path": endpoint.socket_path().display().to_string(),
        },
        "app_server": daemon_response
            .get("cli_app_server")
            .cloned()
            .unwrap_or(Value::Null),
    });
    if let Some(daemon_ensure) = daemon_ensure {
        response["daemon_ensure"] = daemon_ensure;
    }
    response
}

fn plugin_app_server_stop_response(
    plugin_lease_id: &str,
    endpoint: &DaemonEndpoint,
    daemon_response: Value,
) -> Value {
    json!({
        "lease_id": plugin_lease_id,
        "daemon": {
            "socket_path": endpoint.socket_path().display().to_string(),
        },
        "stopped": daemon_response
            .get("stopped")
            .and_then(Value::as_bool)
            .unwrap_or(false),
        "handoff_daemon_socket_path": daemon_response
            .get("handoff_daemon_socket_path")
            .cloned()
            .unwrap_or(Value::Null),
    })
}

fn plugin_app_server_retained_stop_response(
    plugin_lease_id: &str,
    endpoint: &DaemonEndpoint,
) -> Value {
    json!({
        "lease_id": plugin_lease_id,
        "daemon": {
            "socket_path": endpoint.socket_path().display().to_string(),
        },
        "stopped": false,
        "handoff_daemon_socket_path": Value::Null,
    })
}

fn stale_plugin_app_server_lease(lease_id: &str) -> PluginRpcError {
    PluginRpcError::new(
        PluginRpcErrorKind::StaleLease,
        format!("app-server lease is not active for this plugin connection: {lease_id}"),
    )
}

fn daemon_error_needs_app_server_reservation(error: &anyhow::Error) -> bool {
    error
        .to_string()
        .contains("does not have an active CLI app-server reservation")
}

fn daemon_error_needs_endpoint_refresh(error: &anyhow::Error) -> bool {
    if error_is_daemon_endpoint_gone(error) {
        return true;
    }
    let message = format!("{error:#}");
    message.contains("daemon is quiescing for handoff") || message.contains("daemon is stopping")
}

fn daemon_dispatch_error(error: anyhow::Error) -> PluginRpcError {
    let message = format!("{error:#}");
    let kind = if error_is_daemon_endpoint_gone(&error)
        || message.contains("daemon is quiescing for handoff")
        || message.contains("daemon is stopping")
        || message.contains("daemon is busy")
        || message.contains("daemon connection limit reached")
        || message.contains("daemon startup is already in progress")
    {
        PluginRpcErrorKind::TransientDaemonUnavailable
    } else if message.contains("owned by a different lease")
        || message.contains("already has an active CLI app-server")
        || message.contains("already has an active CLI app-server lease")
        || message.contains("already has an active CLI app-server reservation")
        || message.contains("already has a registered CLI app-server")
        || message.contains("is already attached to app-server")
        || message.contains("app-server is at epoch")
        || message.contains("different active CLI app-server reservation")
        || message.contains("targets a different")
    {
        PluginRpcErrorKind::PolicyBlocked
    } else if message.contains("is not running")
        || message.contains("has exited")
        || message.contains("does not have an active CLI app-server reservation")
    {
        PluginRpcErrorKind::StaleLease
    } else if message.contains("spawn") || message.contains("codex app-server") {
        PluginRpcErrorKind::TargetUnavailable
    } else {
        PluginRpcErrorKind::Internal
    };
    PluginRpcError::new(kind, message)
}

impl ServiceSharedState {
    fn new(layout: &FsLayout, registry: &PluginRegistry, now: i64) -> Result<Self> {
        let plugins = registry
            .plugins
            .iter()
            .cloned()
            .map(|manifest| {
                let plugin = SupervisedPlugin::new_reconciled(layout, manifest, now)?;
                Ok((plugin.manifest.name.clone(), plugin))
            })
            .collect::<Result<HashMap<_, _>>>()?;
        Ok(Self {
            plugins,
            dirty: true,
        })
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

fn reconcile_persisted_plugin_process(
    layout: &FsLayout,
    plugin: &mut SupervisedPlugin,
) -> Result<()> {
    if !plugin.manifest.enabled {
        return Ok(());
    }
    let Some(persisted) = read_plugin_status(layout, &plugin.manifest.name)? else {
        return Ok(());
    };
    let Some(pid) = persisted.pid else {
        return Ok(());
    };
    if !process_group_exists(pid) {
        return Ok(());
    }

    plugin.status.state = PluginRuntimeState::Failed;
    plugin.status.pid = Some(pid);
    plugin.status.instance_id = persisted.instance_id;
    plugin.status.crash_count = persisted.crash_count;
    plugin.status.last_started_at = persisted.last_started_at;
    plugin.status.last_healthy_at = persisted.last_healthy_at;
    plugin.status.last_exit = persisted.last_exit;
    plugin.status.last_error = Some(format!(
        "persisted plugin process group {pid} is still running; refusing to launch duplicate without a process identity fence"
    ));
    plugin.status.restart_after_epoch = None;
    plugin.next_restart = None;
    Ok(())
}

fn accept_plugin_connections(
    layout: &FsLayout,
    listener: &UnixListener,
    shared: &Arc<Mutex<ServiceSharedState>>,
    app_server_leases: &Arc<Mutex<PluginAppServerLeaseRegistry>>,
) -> Result<()> {
    loop {
        match listener.accept() {
            Ok((stream, _addr)) => {
                let layout = layout.clone();
                let shared = Arc::clone(shared);
                let app_server_leases = Arc::clone(app_server_leases);
                thread::spawn(move || {
                    let _ = handle_plugin_connection(&layout, stream, &shared, &app_server_leases);
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
    app_server_leases: &Arc<Mutex<PluginAppServerLeaseRegistry>>,
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
    let mut app_server_broker =
        DaemonPluginAppServerLeaseBroker::new(layout, Arc::clone(app_server_leases));

    while let Ok(frame) =
        read_plugin_rpc_frame::<_, PluginRpcRequestFrame>(&mut stream, PLUGIN_RPC_MAX_FRAME_BYTES)
    {
        let response =
            handle_plugin_runtime_frame(shared, &identity, &frame, &mut app_server_broker);
        if let Err(error) =
            write_plugin_rpc_frame(&mut stream, &response, PLUGIN_RPC_MAX_FRAME_BYTES)
        {
            app_server_broker.cleanup_connection_leases();
            return Err(anyhow::anyhow!("write plugin RPC response: {error}"));
        }
    }
    app_server_broker.cleanup_connection_leases();
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

#[cfg(target_os = "linux")]
fn unix_stream_peer_pid_impl(stream: &UnixStream) -> Result<Option<u32>> {
    let mut credentials: libc::ucred = unsafe { std::mem::zeroed() };
    let mut len = std::mem::size_of::<libc::ucred>() as libc::socklen_t;
    let rc = unsafe {
        libc::getsockopt(
            stream.as_raw_fd(),
            libc::SOL_SOCKET,
            libc::SO_PEERCRED,
            (&mut credentials as *mut libc::ucred).cast(),
            &mut len,
        )
    };
    if rc != 0 {
        return Err(io::Error::last_os_error()).context("getsockopt SO_PEERCRED");
    }
    u32::try_from(credentials.pid)
        .map(Some)
        .context("plugin peer pid is out of range")
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn unix_stream_peer_pid_impl(_stream: &UnixStream) -> Result<Option<u32>> {
    Ok(None)
}

fn handle_plugin_runtime_frame<B: PluginAppServerLeaseBroker>(
    shared: &Arc<Mutex<ServiceSharedState>>,
    identity: &PluginConnectionIdentity,
    frame: &PluginRpcRequestFrame,
    app_server_broker: &mut B,
) -> PluginRpcResponseFrame {
    match frame.method.as_str() {
        PLUGIN_HEALTH_UPDATE_METHOD => handle_plugin_health_update_frame(shared, identity, frame),
        PLUGIN_RPC_APP_SERVER_ENSURE_METHOD => {
            handle_plugin_app_server_ensure_frame(shared, identity, frame, app_server_broker)
        }
        PLUGIN_RPC_APP_SERVER_REFRESH_METHOD => {
            handle_plugin_app_server_refresh_frame(shared, identity, frame, app_server_broker)
        }
        PLUGIN_RPC_APP_SERVER_STOP_METHOD => {
            handle_plugin_app_server_stop_frame(shared, identity, frame, app_server_broker)
        }
        _ => PluginRpcResponseFrame::failure(
            frame.id.clone(),
            PluginRpcError::new(
                PluginRpcErrorKind::MethodNotFound,
                format!("unsupported service method {}", frame.method),
            ),
        ),
    }
}

fn handle_plugin_health_update_frame(
    shared: &Arc<Mutex<ServiceSharedState>>,
    identity: &PluginConnectionIdentity,
    frame: &PluginRpcRequestFrame,
) -> PluginRpcResponseFrame {
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
                if let Err(error) = validate_plugin_connection_identity(plugin, identity, now) {
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

fn handle_plugin_app_server_ensure_frame<B: PluginAppServerLeaseBroker>(
    shared: &Arc<Mutex<ServiceSharedState>>,
    identity: &PluginConnectionIdentity,
    frame: &PluginRpcRequestFrame,
    app_server_broker: &mut B,
) -> PluginRpcResponseFrame {
    if let Err(error) = validate_current_plugin_connection(shared, identity) {
        return PluginRpcResponseFrame::failure(frame.id.clone(), error);
    }
    let request = match serde_json::from_value::<PluginAppServerEnsureRequest>(frame.params.clone())
    {
        Ok(request) => request,
        Err(error) => {
            return PluginRpcResponseFrame::failure(
                frame.id.clone(),
                PluginRpcError::new(
                    PluginRpcErrorKind::InvalidRequest,
                    format!("invalid app_server.ensure request: {error}"),
                ),
            );
        }
    };
    match app_server_broker.ensure(identity, request) {
        Ok(result) => PluginRpcResponseFrame::success(frame.id.clone(), result),
        Err(error) => PluginRpcResponseFrame::failure(frame.id.clone(), error),
    }
}

fn handle_plugin_app_server_refresh_frame<B: PluginAppServerLeaseBroker>(
    shared: &Arc<Mutex<ServiceSharedState>>,
    identity: &PluginConnectionIdentity,
    frame: &PluginRpcRequestFrame,
    app_server_broker: &mut B,
) -> PluginRpcResponseFrame {
    if let Err(error) = validate_current_plugin_connection(shared, identity) {
        return PluginRpcResponseFrame::failure(frame.id.clone(), error);
    }
    let request =
        match serde_json::from_value::<PluginAppServerRefreshRequest>(frame.params.clone()) {
            Ok(request) => request,
            Err(error) => {
                return PluginRpcResponseFrame::failure(
                    frame.id.clone(),
                    PluginRpcError::new(
                        PluginRpcErrorKind::InvalidRequest,
                        format!("invalid app_server.refresh request: {error}"),
                    ),
                );
            }
        };
    match app_server_broker.refresh(identity, request) {
        Ok(result) => PluginRpcResponseFrame::success(frame.id.clone(), result),
        Err(error) => PluginRpcResponseFrame::failure(frame.id.clone(), error),
    }
}

fn handle_plugin_app_server_stop_frame<B: PluginAppServerLeaseBroker>(
    shared: &Arc<Mutex<ServiceSharedState>>,
    identity: &PluginConnectionIdentity,
    frame: &PluginRpcRequestFrame,
    app_server_broker: &mut B,
) -> PluginRpcResponseFrame {
    if let Err(error) = validate_current_plugin_connection(shared, identity) {
        return PluginRpcResponseFrame::failure(frame.id.clone(), error);
    }
    let request = match serde_json::from_value::<PluginAppServerStopRequest>(frame.params.clone()) {
        Ok(request) => request,
        Err(error) => {
            return PluginRpcResponseFrame::failure(
                frame.id.clone(),
                PluginRpcError::new(
                    PluginRpcErrorKind::InvalidRequest,
                    format!("invalid app_server.stop request: {error}"),
                ),
            );
        }
    };
    match app_server_broker.stop(identity, request) {
        Ok(result) => PluginRpcResponseFrame::success(frame.id.clone(), result),
        Err(error) => PluginRpcResponseFrame::failure(frame.id.clone(), error),
    }
}

fn validate_current_plugin_connection(
    shared: &Arc<Mutex<ServiceSharedState>>,
    identity: &PluginConnectionIdentity,
) -> Result<(), PluginRpcError> {
    let now = current_epoch_seconds().map_err(PluginRpcError::internal)?;
    match shared.lock() {
        Ok(mut state) => {
            let Some(plugin) = state.plugins.get_mut(&identity.plugin_name) else {
                return Err(PluginRpcError::new(
                    PluginRpcErrorKind::PolicyBlocked,
                    format!("plugin is not configured: {}", identity.plugin_name),
                ));
            };
            validate_plugin_connection_identity(plugin, identity, now).map_err(|error| {
                state.mark_dirty();
                PluginRpcError::new(PluginRpcErrorKind::PolicyBlocked, format!("{error:#}"))
            })
        }
        Err(_) => Err(PluginRpcError::internal("service state lock poisoned")),
    }
}

fn validate_plugin_connection_identity(
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
            ServiceCapability::new("app-server-lease-rpc-v1"),
            ServiceCapability::new(PLUGIN_RPC_APP_SERVER_ENSURE_METHOD),
            ServiceCapability::new(PLUGIN_RPC_APP_SERVER_REFRESH_METHOD),
            ServiceCapability::new(PLUGIN_RPC_APP_SERVER_STOP_METHOD),
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

fn apply_manifest_status_overlay(status: &mut PluginRuntimeStatus, manifest: &PluginManifest) {
    status.enabled = manifest.enabled;
    status.configured = true;
    status.release_id = status.release_id.take().or(manifest.release_id.clone());
    if !manifest.enabled {
        status.state = PluginRuntimeState::Disabled;
        status.pid = None;
        status.instance_id = None;
        status.restart_after_epoch = None;
        status.last_healthy_at = None;
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
    let Some(pgid) = process_group_pid(pid) else {
        return false;
    };
    unsafe { libc::killpg(pgid, signal) == 0 }
}

fn process_group_exists(pid: u32) -> bool {
    let Some(pgid) = process_group_pid(pid) else {
        return false;
    };
    if unsafe { libc::killpg(pgid, 0) } == 0 {
        return true;
    }
    !matches!(io::Error::last_os_error().raw_os_error(), Some(libc::ESRCH))
}

fn process_group_pid(pid: u32) -> Option<libc::pid_t> {
    let pid = i32::try_from(pid).ok()?;
    if pid > 0 { Some(pid) } else { None }
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

    #[derive(Default)]
    struct FakePluginAppServerLeaseBroker {
        ensure_requests: Vec<(PluginConnectionIdentity, PluginAppServerEnsureRequest)>,
        refresh_requests: Vec<(PluginConnectionIdentity, PluginAppServerRefreshRequest)>,
        stop_requests: Vec<(PluginConnectionIdentity, PluginAppServerStopRequest)>,
        cleanup_count: usize,
        next_error: Option<PluginRpcError>,
    }

    impl PluginAppServerLeaseBroker for FakePluginAppServerLeaseBroker {
        fn ensure(
            &mut self,
            identity: &PluginConnectionIdentity,
            request: PluginAppServerEnsureRequest,
        ) -> Result<Value, PluginRpcError> {
            self.ensure_requests
                .push((identity.clone(), request.clone()));
            if let Some(error) = self.next_error.take() {
                return Err(error);
            }
            Ok(json!({
                "lease_id": request.lease_id,
                "app_server": {
                    "managed_session_id": request.managed_session_id,
                    "bound_thread_id": request.bound_thread_id,
                    "session_epoch": request.session_epoch,
                    "url": "ws://127.0.0.1:1234",
                },
            }))
        }

        fn refresh(
            &mut self,
            identity: &PluginConnectionIdentity,
            request: PluginAppServerRefreshRequest,
        ) -> Result<Value, PluginRpcError> {
            self.refresh_requests
                .push((identity.clone(), request.clone()));
            if let Some(error) = self.next_error.take() {
                return Err(error);
            }
            Ok(json!({
                "lease_id": request.lease_id,
                "app_server": {
                    "managed_session_id": request.managed_session_id,
                    "lease_seconds_remaining": 60,
                },
            }))
        }

        fn stop(
            &mut self,
            identity: &PluginConnectionIdentity,
            request: PluginAppServerStopRequest,
        ) -> Result<Value, PluginRpcError> {
            self.stop_requests.push((identity.clone(), request.clone()));
            if let Some(error) = self.next_error.take() {
                return Err(error);
            }
            Ok(json!({
                "lease_id": request.lease_id,
                "stopped": true,
            }))
        }

        fn cleanup_connection_leases(&mut self) {
            self.cleanup_count += 1;
        }
    }

    fn shared_with_running_plugin(
        layout: &FsLayout,
        instance_id: &str,
    ) -> (Arc<Mutex<ServiceSharedState>>, u32) {
        let registry = PluginRegistry {
            schema_version: 1,
            plugins: vec![manifest("webex")],
        };
        let shared = Arc::new(Mutex::new(
            ServiceSharedState::new(layout, &registry, 1_000).expect("shared state"),
        ));
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
            plugin.status.instance_id = Some(instance_id.to_owned());
            plugin.process = Some(child);
        }
        (shared, child_pid)
    }

    fn app_server_lease_registry() -> Arc<Mutex<PluginAppServerLeaseRegistry>> {
        Arc::new(Mutex::new(PluginAppServerLeaseRegistry::default()))
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
        let shared = Arc::new(Mutex::new(
            ServiceSharedState::new(&layout, &registry, 1_000).expect("shared state"),
        ));

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
        let shared = Arc::new(Mutex::new(
            ServiceSharedState::new(&layout, &registry, 1_000).expect("shared state"),
        ));

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
    fn status_report_forces_disabled_manifest_state_over_persisted_runtime() {
        let temp = tempfile::tempdir().expect("tempdir");
        let layout = layout_for_tempdir(&temp);
        layout.ensure_plugin_home("webex").expect("plugin home");
        let registry = PluginRegistry {
            schema_version: 1,
            plugins: vec![PluginManifest {
                enabled: false,
                ..manifest("webex")
            }],
        };
        atomic_write_private(
            &layout.plugin_registry_path(),
            &serde_json::to_vec_pretty(&registry).expect("registry json"),
        )
        .expect("write registry");
        let mut persisted = status_from_manifest(&layout, &manifest("webex"));
        persisted.enabled = true;
        persisted.state = PluginRuntimeState::Running;
        persisted.pid = Some(1234);
        persisted.instance_id = Some("stale-instance".to_owned());
        persisted.restart_after_epoch = Some(2_000);
        persisted.last_healthy_at = Some(1_999);
        atomic_write_private(
            &layout.plugin_status_path("webex"),
            &serde_json::to_vec_pretty(&persisted).expect("status json"),
        )
        .expect("write status");

        let report = status_report(&layout, None).expect("status report");

        let status = report.plugins.first().expect("plugin status");
        assert!(!status.enabled);
        assert_eq!(status.state, PluginRuntimeState::Disabled);
        assert_eq!(status.pid, None);
        assert_eq!(status.instance_id, None);
        assert_eq!(status.restart_after_epoch, None);
        assert_eq!(status.last_healthy_at, None);
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[test]
    fn unix_stream_peer_pid_reads_current_process() {
        let temp = tempfile::tempdir().expect("tempdir");
        let socket_path = temp.path().join("peer.sock");
        let listener = UnixListener::bind(&socket_path).expect("bind peer socket");
        let _client = UnixStream::connect(&socket_path).expect("connect peer socket");
        let (server, _) = listener.accept().expect("accept peer socket");

        let peer_pid = unix_stream_peer_pid(&server).expect("peer pid");

        assert_eq!(peer_pid, Some(std::process::id()));
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
    fn service_start_blocks_relaunch_when_persisted_process_group_is_live() {
        let temp = tempfile::tempdir().expect("tempdir");
        let layout = layout_for_tempdir(&temp);
        layout.ensure_plugin_home("webex").expect("plugin home");
        let registry = PluginRegistry {
            schema_version: 1,
            plugins: vec![PluginManifest {
                executable_path: PathBuf::from("/bin/sleep"),
                args: vec!["30".to_owned()],
                ..manifest("webex")
            }],
        };
        let mut stale_child = Command::new("/bin/sleep")
            .arg("30")
            .process_group(0)
            .spawn()
            .expect("spawn stale child");
        let stale_pid = stale_child.id();
        let mut persisted = status_from_manifest(&layout, &manifest("webex"));
        persisted.state = PluginRuntimeState::Running;
        persisted.pid = Some(stale_pid);
        persisted.instance_id = Some("old-instance".to_owned());
        persisted.last_started_at = Some(1_998);
        persisted.last_healthy_at = Some(1_999);
        atomic_write_private(
            &layout.plugin_status_path("webex"),
            &serde_json::to_vec_pretty(&persisted).expect("status json"),
        )
        .expect("write status");

        let shared = Arc::new(Mutex::new(
            ServiceSharedState::new(&layout, &registry, 2_000).expect("shared state"),
        ));

        assert!(process_group_exists(stale_pid));
        {
            let state = shared.lock().expect("state");
            let plugin = state.plugins.get("webex").expect("plugin");
            assert!(plugin.process.is_none());
            assert_eq!(plugin.status.state, PluginRuntimeState::Failed);
            assert_eq!(plugin.status.pid, Some(stale_pid));
            assert_eq!(plugin.status.instance_id.as_deref(), Some("old-instance"));
            assert_eq!(plugin.status.last_started_at, Some(1_998));
            assert_eq!(plugin.status.last_healthy_at, Some(1_999));
            assert_eq!(plugin.status.restart_after_epoch, None);
            assert_eq!(plugin.next_restart, None);
            assert!(
                plugin
                    .status
                    .last_error
                    .as_deref()
                    .expect("last error")
                    .contains("refusing to launch duplicate")
            );
        }

        supervise_tick(&layout, &shared, 2_000).expect("tick");
        let state = shared.lock().expect("state");
        let plugin = state.plugins.get("webex").expect("plugin");
        assert!(plugin.process.is_none());
        assert_eq!(plugin.status.pid, Some(stale_pid));
        drop(state);
        terminate_plugin_child_best_effort(&mut stale_child, stale_pid);
    }

    #[test]
    fn fake_plugin_hello_updates_status_and_returns_policy() {
        let temp = tempfile::tempdir().expect("tempdir");
        let layout = layout_for_tempdir(&temp);
        let registry = PluginRegistry {
            schema_version: 1,
            plugins: vec![manifest("webex")],
        };
        let shared = Arc::new(Mutex::new(
            ServiceSharedState::new(&layout, &registry, 1_000).expect("shared state"),
        ));
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
        let shared = Arc::new(Mutex::new(
            ServiceSharedState::new(&layout, &registry, 1_000).expect("shared state"),
        ));
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
        let mut broker = FakePluginAppServerLeaseBroker::default();

        let response = handle_plugin_runtime_frame(&shared, &identity, &frame, &mut broker);

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
        let shared = Arc::new(Mutex::new(
            ServiceSharedState::new(&layout, &registry, 1_000).expect("shared state"),
        ));
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
        let mut broker = FakePluginAppServerLeaseBroker::default();

        let response = handle_plugin_runtime_frame(&shared, &identity, &frame, &mut broker);

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
        let shared = Arc::new(Mutex::new(
            ServiceSharedState::new(&layout, &registry, 1_000).expect("shared state"),
        ));
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
        let mut broker = FakePluginAppServerLeaseBroker::default();

        let response = handle_plugin_runtime_frame(&shared, &stale_identity, &frame, &mut broker);

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
    fn handshake_policy_advertises_app_server_lease_rpc() {
        let temp = tempfile::tempdir().expect("tempdir");
        let layout = layout_for_tempdir(&temp);

        let policy = plugin_handshake_policy(&layout);
        let capabilities = policy
            .service_capabilities
            .iter()
            .map(|capability| capability.name.as_str())
            .collect::<Vec<_>>();

        assert!(capabilities.contains(&"app-server-lease-rpc-v1"));
        assert!(capabilities.contains(&PLUGIN_RPC_APP_SERVER_ENSURE_METHOD));
        assert!(capabilities.contains(&PLUGIN_RPC_APP_SERVER_REFRESH_METHOD));
        assert!(capabilities.contains(&PLUGIN_RPC_APP_SERVER_STOP_METHOD));
    }

    #[test]
    fn app_server_ensure_brokers_current_plugin_identity() {
        let temp = tempfile::tempdir().expect("tempdir");
        let layout = layout_for_tempdir(&temp);
        let (shared, child_pid) = shared_with_running_plugin(&layout, "instance-1");
        let identity = PluginConnectionIdentity {
            plugin_name: "webex".to_owned(),
            pid: child_pid,
            instance_id: "instance-1".to_owned(),
        };
        let frame = PluginRpcRequestFrame::new(
            "ensure-1",
            PLUGIN_RPC_APP_SERVER_ENSURE_METHOD,
            json!({
                "managed_session_id": "managed-1",
                "bound_thread_id": "thread-1",
                "session_epoch": 1,
                "codex_binary": "/usr/bin/codex",
                "lease_id": "lease-1",
                "lease_ttl_seconds": 90,
            }),
        );
        let mut broker = FakePluginAppServerLeaseBroker::default();

        let response = handle_plugin_runtime_frame(&shared, &identity, &frame, &mut broker);

        assert!(response.error.is_none());
        assert_eq!(broker.ensure_requests.len(), 1);
        let (broker_identity, request) = &broker.ensure_requests[0];
        assert_eq!(broker_identity, &identity);
        assert_eq!(request.managed_session_id, "managed-1");
        assert_eq!(request.bound_thread_id, "thread-1");
        assert_eq!(request.session_epoch, 1);
        assert_eq!(request.lease_id, "lease-1");
        assert_eq!(request.lease_ttl_seconds, Some(90));
        assert_eq!(
            response
                .result
                .as_ref()
                .expect("result")
                .get("lease_id")
                .and_then(Value::as_str),
            Some("lease-1")
        );
        shutdown_managed_plugins(&layout, &shared);
    }

    #[test]
    fn app_server_ensure_rejects_oversized_plugin_lease_ttl() {
        let error = validate_plugin_app_server_ensure_request(&PluginAppServerEnsureRequest {
            managed_session_id: "managed-1".to_owned(),
            bound_thread_id: "thread-1".to_owned(),
            session_epoch: 1,
            codex_binary: "/usr/bin/codex".to_owned(),
            lease_id: "lease-1".to_owned(),
            lease_ttl_seconds: Some(MAX_PLUGIN_APP_SERVER_LEASE_TTL_SECONDS + 1),
        })
        .expect_err("oversized TTL should be rejected");

        assert_eq!(error.kind, PluginRpcErrorKind::InvalidRequest);
        assert!(error.message.contains("lease_ttl_seconds must not exceed"));
    }

    #[test]
    fn app_server_request_rejects_stale_plugin_instance_before_broker() {
        let temp = tempfile::tempdir().expect("tempdir");
        let layout = layout_for_tempdir(&temp);
        let (shared, child_pid) = shared_with_running_plugin(&layout, "current-instance");
        let stale_identity = PluginConnectionIdentity {
            plugin_name: "webex".to_owned(),
            pid: child_pid,
            instance_id: "old-instance".to_owned(),
        };
        let frame = PluginRpcRequestFrame::new(
            "ensure-1",
            PLUGIN_RPC_APP_SERVER_ENSURE_METHOD,
            json!({
                "managed_session_id": "managed-1",
                "bound_thread_id": "thread-1",
                "session_epoch": 1,
                "codex_binary": "/usr/bin/codex",
                "lease_id": "lease-1",
            }),
        );
        let mut broker = FakePluginAppServerLeaseBroker::default();

        let response = handle_plugin_runtime_frame(&shared, &stale_identity, &frame, &mut broker);

        assert_eq!(
            response.error.as_ref().expect("error").kind,
            PluginRpcErrorKind::PolicyBlocked
        );
        assert!(broker.ensure_requests.is_empty());
        shutdown_managed_plugins(&layout, &shared);
    }

    #[test]
    fn app_server_refresh_propagates_broker_stale_lease() {
        let temp = tempfile::tempdir().expect("tempdir");
        let layout = layout_for_tempdir(&temp);
        let (shared, child_pid) = shared_with_running_plugin(&layout, "instance-1");
        let identity = PluginConnectionIdentity {
            plugin_name: "webex".to_owned(),
            pid: child_pid,
            instance_id: "instance-1".to_owned(),
        };
        let frame = PluginRpcRequestFrame::new(
            "refresh-1",
            PLUGIN_RPC_APP_SERVER_REFRESH_METHOD,
            json!({
                "managed_session_id": "managed-1",
                "lease_id": "lease-1",
            }),
        );
        let mut broker = FakePluginAppServerLeaseBroker {
            next_error: Some(stale_plugin_app_server_lease("lease-1")),
            ..FakePluginAppServerLeaseBroker::default()
        };

        let response = handle_plugin_runtime_frame(&shared, &identity, &frame, &mut broker);

        assert_eq!(
            response.error.as_ref().expect("error").kind,
            PluginRpcErrorKind::StaleLease
        );
        assert_eq!(broker.refresh_requests.len(), 1);
        shutdown_managed_plugins(&layout, &shared);
    }

    #[test]
    fn scoped_app_server_lease_id_is_plugin_and_instance_scoped() {
        let identity = PluginConnectionIdentity {
            plugin_name: "webex".to_owned(),
            pid: 42,
            instance_id: "instance-1".to_owned(),
        };
        let same = scoped_plugin_app_server_lease_id(&identity, "lease-1");
        let same_again = scoped_plugin_app_server_lease_id(&identity, "lease-1");
        let other_instance = scoped_plugin_app_server_lease_id(
            &PluginConnectionIdentity {
                instance_id: "instance-2".to_owned(),
                ..identity.clone()
            },
            "lease-1",
        );
        let other_plugin = scoped_plugin_app_server_lease_id(
            &PluginConnectionIdentity {
                plugin_name: "slack".to_owned(),
                ..identity
            },
            "lease-1",
        );

        assert_eq!(same, same_again);
        assert_ne!(same, other_instance);
        assert_ne!(same, other_plugin);
        assert!(same.starts_with("plugin-"));
        assert_eq!(same.len(), "plugin-".len() + 32);
    }

    #[test]
    fn daemon_broker_cleanup_retains_shared_lease_for_other_connection() {
        let temp = tempfile::tempdir().expect("tempdir");
        let layout = layout_for_tempdir(&temp);
        let registry = app_server_lease_registry();
        let mut broker_a = DaemonPluginAppServerLeaseBroker::new(&layout, Arc::clone(&registry));
        let mut broker_b = DaemonPluginAppServerLeaseBroker::new(&layout, Arc::clone(&registry));
        let identity = PluginConnectionIdentity {
            plugin_name: "webex".to_owned(),
            pid: 42,
            instance_id: "instance-1".to_owned(),
        };
        let key = plugin_app_server_lease_key(&identity, "lease-1");
        let record = PluginAppServerLeaseRecord {
            target: PluginAppServerLeaseTarget {
                managed_session_id: "managed-1".to_owned(),
                bound_thread_id: "thread-1".to_owned(),
                session_epoch: 1,
            },
            endpoint: DaemonEndpoint::default(&layout),
            scoped_lease_id: scoped_plugin_app_server_lease_id(&identity, "lease-1"),
            endpoint_confirmed: true,
        };
        let broker_a_id = broker_a.connection_id;
        let broker_b_id = broker_b.connection_id;
        broker_a
            .register_connection_lease(key.clone(), record.clone())
            .expect("register broker a");
        broker_b
            .register_connection_lease(key.clone(), record)
            .expect("register broker b");

        broker_a.cleanup_connection_leases();

        assert!(broker_a.leases.is_empty());
        let registry = registry.lock().expect("registry");
        let shared = registry.leases.get(&key).expect("shared lease");
        assert!(!shared.holders.contains(&broker_a_id));
        assert!(shared.holders.contains(&broker_b_id));
    }

    #[test]
    fn daemon_broker_stop_retains_shared_lease_for_other_connection() {
        let temp = tempfile::tempdir().expect("tempdir");
        let layout = layout_for_tempdir(&temp);
        let registry = app_server_lease_registry();
        let mut broker_a = DaemonPluginAppServerLeaseBroker::new(&layout, Arc::clone(&registry));
        let mut broker_b = DaemonPluginAppServerLeaseBroker::new(&layout, Arc::clone(&registry));
        let identity = PluginConnectionIdentity {
            plugin_name: "webex".to_owned(),
            pid: 42,
            instance_id: "instance-1".to_owned(),
        };
        let key = plugin_app_server_lease_key(&identity, "lease-1");
        let record = PluginAppServerLeaseRecord {
            target: PluginAppServerLeaseTarget {
                managed_session_id: "managed-1".to_owned(),
                bound_thread_id: "thread-1".to_owned(),
                session_epoch: 1,
            },
            endpoint: DaemonEndpoint::default(&layout),
            scoped_lease_id: scoped_plugin_app_server_lease_id(&identity, "lease-1"),
            endpoint_confirmed: true,
        };
        let broker_a_id = broker_a.connection_id;
        let broker_b_id = broker_b.connection_id;
        broker_a
            .register_connection_lease(key.clone(), record.clone())
            .expect("register broker a");
        broker_b
            .register_connection_lease(key.clone(), record)
            .expect("register broker b");

        let response = broker_a
            .stop(
                &identity,
                PluginAppServerStopRequest {
                    managed_session_id: "managed-1".to_owned(),
                    lease_id: "lease-1".to_owned(),
                },
            )
            .expect("shared stop");

        assert_eq!(
            response.get("stopped").and_then(Value::as_bool),
            Some(false)
        );
        assert!(broker_a.leases.is_empty());
        let registry = registry.lock().expect("registry");
        let shared = registry.leases.get(&key).expect("shared lease");
        assert!(!shared.holders.contains(&broker_a_id));
        assert!(shared.holders.contains(&broker_b_id));
    }

    #[test]
    fn daemon_broker_stop_failure_preserves_final_holder_for_retry() {
        let temp = tempfile::tempdir().expect("tempdir");
        let layout = layout_for_tempdir(&temp);
        let registry = app_server_lease_registry();
        let mut broker = DaemonPluginAppServerLeaseBroker::new(&layout, Arc::clone(&registry));
        let identity = PluginConnectionIdentity {
            plugin_name: "webex".to_owned(),
            pid: 42,
            instance_id: "instance-1".to_owned(),
        };
        let key = plugin_app_server_lease_key(&identity, "lease-1");
        let connection_id = broker.connection_id;
        broker
            .register_connection_lease(
                key.clone(),
                PluginAppServerLeaseRecord {
                    target: PluginAppServerLeaseTarget {
                        managed_session_id: "managed-1".to_owned(),
                        bound_thread_id: "thread-1".to_owned(),
                        session_epoch: 1,
                    },
                    endpoint: DaemonEndpoint::default(&layout),
                    scoped_lease_id: scoped_plugin_app_server_lease_id(&identity, "lease-1"),
                    endpoint_confirmed: true,
                },
            )
            .expect("register lease");

        let error = broker
            .stop(
                &identity,
                PluginAppServerStopRequest {
                    managed_session_id: "managed-1".to_owned(),
                    lease_id: "lease-1".to_owned(),
                },
            )
            .expect_err("missing daemon socket should fail stop");

        assert_ne!(error.kind, PluginRpcErrorKind::PolicyBlocked);
        assert!(broker.leases.contains_key(&key));
        let registry = registry.lock().expect("registry");
        let shared = registry.leases.get(&key).expect("shared lease");
        assert!(shared.holders.contains(&connection_id));
    }

    #[test]
    fn daemon_broker_refresh_endpoint_update_reaches_shared_lease_record() {
        let temp = tempfile::tempdir().expect("tempdir");
        let layout = layout_for_tempdir(&temp);
        let registry = app_server_lease_registry();
        let mut broker_a = DaemonPluginAppServerLeaseBroker::new(&layout, Arc::clone(&registry));
        let mut broker_b = DaemonPluginAppServerLeaseBroker::new(&layout, Arc::clone(&registry));
        let identity = PluginConnectionIdentity {
            plugin_name: "webex".to_owned(),
            pid: 42,
            instance_id: "instance-1".to_owned(),
        };
        let key = plugin_app_server_lease_key(&identity, "lease-1");
        let old_endpoint = DaemonEndpoint::from_socket_path(layout.run_dir().join("old.sock"));
        let new_endpoint = DaemonEndpoint::from_socket_path(layout.run_dir().join("new.sock"));
        let record = PluginAppServerLeaseRecord {
            target: PluginAppServerLeaseTarget {
                managed_session_id: "managed-1".to_owned(),
                bound_thread_id: "thread-1".to_owned(),
                session_epoch: 1,
            },
            endpoint: old_endpoint,
            scoped_lease_id: scoped_plugin_app_server_lease_id(&identity, "lease-1"),
            endpoint_confirmed: false,
        };
        broker_a
            .register_connection_lease(key.clone(), record.clone())
            .expect("register broker a");
        broker_b
            .register_connection_lease(key.clone(), record)
            .expect("register broker b");

        broker_b
            .update_connection_lease_endpoint(&key, new_endpoint.clone())
            .expect("update endpoint");
        broker_a.cleanup_connection_leases();
        let registry = registry.lock().expect("registry");
        let shared = registry.leases.get(&key).expect("shared lease");
        assert_eq!(shared.endpoint.socket_path(), new_endpoint.socket_path());
        assert!(shared.endpoint_confirmed);
    }

    #[test]
    fn daemon_broker_ensure_rolls_back_preregistered_shared_holder_on_daemon_failure() {
        let temp = tempfile::tempdir().expect("tempdir");
        let layout = layout_for_tempdir(&temp);
        let registry = app_server_lease_registry();
        let mut broker_a = DaemonPluginAppServerLeaseBroker::new(&layout, Arc::clone(&registry));
        let mut broker_b = DaemonPluginAppServerLeaseBroker::new(&layout, Arc::clone(&registry));
        let identity = PluginConnectionIdentity {
            plugin_name: "webex".to_owned(),
            pid: 42,
            instance_id: "instance-1".to_owned(),
        };
        let key = plugin_app_server_lease_key(&identity, "lease-1");
        let record = PluginAppServerLeaseRecord {
            target: PluginAppServerLeaseTarget {
                managed_session_id: "managed-1".to_owned(),
                bound_thread_id: "thread-1".to_owned(),
                session_epoch: 1,
            },
            endpoint: DaemonEndpoint::default(&layout),
            scoped_lease_id: scoped_plugin_app_server_lease_id(&identity, "lease-1"),
            endpoint_confirmed: false,
        };
        let broker_a_id = broker_a.connection_id;
        let broker_b_id = broker_b.connection_id;
        broker_a
            .register_connection_lease(key.clone(), record)
            .expect("register broker a");

        let error = broker_b
            .ensure(
                &identity,
                PluginAppServerEnsureRequest {
                    managed_session_id: "managed-1".to_owned(),
                    bound_thread_id: "thread-1".to_owned(),
                    session_epoch: 1,
                    codex_binary: "/usr/bin/codex".to_owned(),
                    lease_id: "lease-1".to_owned(),
                    lease_ttl_seconds: None,
                },
            )
            .expect_err("daemon ensure should fail without a runnable daemon");

        assert_ne!(error.kind, PluginRpcErrorKind::PolicyBlocked);
        assert!(broker_b.leases.is_empty());
        let registry = registry.lock().expect("registry");
        let shared = registry.leases.get(&key).expect("shared lease");
        assert!(shared.holders.contains(&broker_a_id));
        assert!(!shared.holders.contains(&broker_b_id));
    }

    #[test]
    fn daemon_broker_registered_target_blocks_concurrent_different_target() {
        let temp = tempfile::tempdir().expect("tempdir");
        let layout = layout_for_tempdir(&temp);
        let registry = app_server_lease_registry();
        let mut broker_a = DaemonPluginAppServerLeaseBroker::new(&layout, Arc::clone(&registry));
        let broker_b = DaemonPluginAppServerLeaseBroker::new(&layout, Arc::clone(&registry));
        let identity = PluginConnectionIdentity {
            plugin_name: "webex".to_owned(),
            pid: 42,
            instance_id: "instance-1".to_owned(),
        };
        let key = plugin_app_server_lease_key(&identity, "lease-1");
        broker_a
            .register_connection_lease(
                key.clone(),
                PluginAppServerLeaseRecord {
                    target: PluginAppServerLeaseTarget {
                        managed_session_id: "managed-1".to_owned(),
                        bound_thread_id: "thread-1".to_owned(),
                        session_epoch: 1,
                    },
                    endpoint: DaemonEndpoint::default(&layout),
                    scoped_lease_id: scoped_plugin_app_server_lease_id(&identity, "lease-1"),
                    endpoint_confirmed: false,
                },
            )
            .expect("register pending target");

        let error = broker_b
            .shared_lease_record(
                &key,
                &PluginAppServerLeaseTarget {
                    managed_session_id: "managed-2".to_owned(),
                    bound_thread_id: "thread-2".to_owned(),
                    session_epoch: 1,
                },
            )
            .expect_err("different target must fail before daemon dispatch");

        assert_eq!(error.kind, PluginRpcErrorKind::PolicyBlocked);
    }

    #[test]
    fn daemon_broker_remembers_released_lease_target_for_replay_fence() {
        let temp = tempfile::tempdir().expect("tempdir");
        let layout = layout_for_tempdir(&temp);
        let identity = PluginConnectionIdentity {
            plugin_name: "webex".to_owned(),
            pid: 42,
            instance_id: "instance-1".to_owned(),
        };
        let mut broker =
            DaemonPluginAppServerLeaseBroker::new(&layout, app_server_lease_registry());
        let key = plugin_app_server_lease_key(&identity, "lease-1");
        let record = PluginAppServerLeaseRecord {
            target: PluginAppServerLeaseTarget {
                managed_session_id: "managed-1".to_owned(),
                bound_thread_id: "thread-1".to_owned(),
                session_epoch: 1,
            },
            endpoint: DaemonEndpoint::default(&layout),
            scoped_lease_id: scoped_plugin_app_server_lease_id(&identity, "lease-1"),
            endpoint_confirmed: true,
        };
        broker
            .register_connection_lease(key.clone(), record.clone())
            .expect("register lease");
        broker.rollback_preregistered_lease_after_failed_ensure(&key, record, false);

        let error = broker
            .ensure(
                &identity,
                PluginAppServerEnsureRequest {
                    managed_session_id: "managed-2".to_owned(),
                    bound_thread_id: "thread-2".to_owned(),
                    session_epoch: 1,
                    codex_binary: "/usr/bin/codex".to_owned(),
                    lease_id: "lease-1".to_owned(),
                    lease_ttl_seconds: None,
                },
            )
            .expect_err("released lease id cannot drift to another target");

        assert_eq!(error.kind, PluginRpcErrorKind::PolicyBlocked);
        assert!(broker.leases.is_empty());
    }

    #[test]
    fn daemon_broker_failed_ensure_rollback_cleans_last_holder() {
        let temp = tempfile::tempdir().expect("tempdir");
        let layout = layout_for_tempdir(&temp);
        let registry = app_server_lease_registry();
        let mut broker = DaemonPluginAppServerLeaseBroker::new(&layout, Arc::clone(&registry));
        let identity = PluginConnectionIdentity {
            plugin_name: "webex".to_owned(),
            pid: 42,
            instance_id: "instance-1".to_owned(),
        };
        let key = plugin_app_server_lease_key(&identity, "lease-1");
        let record = PluginAppServerLeaseRecord {
            target: PluginAppServerLeaseTarget {
                managed_session_id: "managed-1".to_owned(),
                bound_thread_id: "thread-1".to_owned(),
                session_epoch: 1,
            },
            endpoint: DaemonEndpoint::default(&layout),
            scoped_lease_id: scoped_plugin_app_server_lease_id(&identity, "lease-1"),
            endpoint_confirmed: true,
        };
        broker
            .register_connection_lease(key.clone(), record.clone())
            .expect("register lease");

        broker.rollback_preregistered_lease_after_failed_ensure(&key, record, true);

        assert!(broker.leases.is_empty());
        assert!(!registry.lock().expect("registry").leases.contains_key(&key));
    }

    #[test]
    fn service_shutdown_cleanup_drains_shared_app_server_leases() {
        let temp = tempfile::tempdir().expect("tempdir");
        let layout = layout_for_tempdir(&temp);
        let registry = app_server_lease_registry();
        let identity = PluginConnectionIdentity {
            plugin_name: "webex".to_owned(),
            pid: 42,
            instance_id: "instance-1".to_owned(),
        };
        let key = plugin_app_server_lease_key(&identity, "lease-1");
        let record = PluginAppServerLeaseRecord {
            target: PluginAppServerLeaseTarget {
                managed_session_id: "managed-1".to_owned(),
                bound_thread_id: "thread-1".to_owned(),
                session_epoch: 1,
            },
            endpoint: DaemonEndpoint::default(&layout),
            scoped_lease_id: scoped_plugin_app_server_lease_id(&identity, "lease-1"),
            endpoint_confirmed: true,
        };
        registry.lock().expect("registry").leases.insert(
            key.clone(),
            PluginAppServerSharedLeaseRecord::from_record(record, 1),
        );

        cleanup_all_plugin_app_server_leases(&layout, &registry);

        assert!(!registry.lock().expect("registry").leases.contains_key(&key));
    }

    #[test]
    fn daemon_broker_rejects_same_lease_with_different_replay_target_before_dispatch() {
        let temp = tempfile::tempdir().expect("tempdir");
        let layout = layout_for_tempdir(&temp);
        let identity = PluginConnectionIdentity {
            plugin_name: "webex".to_owned(),
            pid: 42,
            instance_id: "instance-1".to_owned(),
        };
        let mut broker =
            DaemonPluginAppServerLeaseBroker::new(&layout, app_server_lease_registry());
        let key = plugin_app_server_lease_key(&identity, "lease-1");
        broker.leases.insert(
            key,
            PluginAppServerLeaseRecord {
                target: PluginAppServerLeaseTarget {
                    managed_session_id: "managed-1".to_owned(),
                    bound_thread_id: "thread-1".to_owned(),
                    session_epoch: 1,
                },
                endpoint: DaemonEndpoint::default(&layout),
                scoped_lease_id: scoped_plugin_app_server_lease_id(&identity, "lease-1"),
                endpoint_confirmed: true,
            },
        );

        let error = broker
            .ensure(
                &identity,
                PluginAppServerEnsureRequest {
                    managed_session_id: "managed-2".to_owned(),
                    bound_thread_id: "thread-1".to_owned(),
                    session_epoch: 1,
                    codex_binary: "/usr/bin/codex".to_owned(),
                    lease_id: "lease-1".to_owned(),
                    lease_ttl_seconds: None,
                },
            )
            .expect_err("target mismatch should fail before daemon dispatch");

        assert_eq!(error.kind, PluginRpcErrorKind::PolicyBlocked);
    }

    #[test]
    fn daemon_dispatch_errors_map_to_rpc_error_kinds() {
        let transient = daemon_dispatch_error(anyhow::Error::new(io::Error::new(
            io::ErrorKind::NotFound,
            "missing daemon socket",
        )));
        assert_eq!(
            transient.kind,
            PluginRpcErrorKind::TransientDaemonUnavailable
        );
        assert!(transient.retryable);

        let stale = daemon_dispatch_error(anyhow::anyhow!(
            "CLI app-server for managed session managed-1 is not running"
        ));
        assert_eq!(stale.kind, PluginRpcErrorKind::StaleLease);

        let blocked = daemon_dispatch_error(anyhow::anyhow!(
            "managed session managed-1 already has an active CLI app-server lease"
        ));
        assert_eq!(blocked.kind, PluginRpcErrorKind::PolicyBlocked);

        let reservation_blocked = daemon_dispatch_error(anyhow::anyhow!(
            "thread thread-1 already has an active CLI app-server reservation"
        ));
        assert_eq!(reservation_blocked.kind, PluginRpcErrorKind::PolicyBlocked);

        let different_reservation = daemon_dispatch_error(anyhow::anyhow!(
            "thread thread-1 has a different active CLI app-server reservation"
        ));
        assert_eq!(
            different_reservation.kind,
            PluginRpcErrorKind::PolicyBlocked
        );

        let active_thread = daemon_dispatch_error(anyhow::anyhow!(
            "thread thread-1 already has an active CLI app-server"
        ));
        assert_eq!(active_thread.kind, PluginRpcErrorKind::PolicyBlocked);

        let handoff = anyhow::anyhow!("daemon is quiescing for handoff");
        assert!(daemon_error_needs_endpoint_refresh(&handoff));
        let handoff = daemon_dispatch_error(handoff);
        assert_eq!(handoff.kind, PluginRpcErrorKind::TransientDaemonUnavailable);

        let attached_session = daemon_dispatch_error(anyhow::anyhow!(
            "managed session managed-1 is already attached to app-server for thread thread-1"
        ));
        assert_eq!(attached_session.kind, PluginRpcErrorKind::PolicyBlocked);
    }

    #[test]
    fn plugin_hello_rejects_non_current_child_pid() {
        let temp = tempfile::tempdir().expect("tempdir");
        let layout = layout_for_tempdir(&temp);
        let registry = PluginRegistry {
            schema_version: 1,
            plugins: vec![manifest("webex")],
        };
        let shared = Arc::new(Mutex::new(
            ServiceSharedState::new(&layout, &registry, 1_000).expect("shared state"),
        ));
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
        let shared = Arc::new(Mutex::new(
            ServiceSharedState::new(&layout, &registry, 1_000).expect("shared state"),
        ));
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
        let shared = Arc::new(Mutex::new(
            ServiceSharedState::new(&layout, &registry, 1_000).expect("shared state"),
        ));
        layout.ensure_plugin_home("webex").expect("plugin home");
        let (mut client, server) = UnixStream::pair().expect("socket pair");
        let server_layout = layout.clone();
        let server_shared = Arc::clone(&shared);
        let server_app_server_leases = app_server_lease_registry();
        let worker = thread::spawn(move || {
            handle_plugin_connection(
                &server_layout,
                server,
                &server_shared,
                &server_app_server_leases,
            )
            .expect("connection");
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
