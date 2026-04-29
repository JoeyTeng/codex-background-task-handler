use std::env;
use std::ffi::{OsStr, OsString};
use std::fs;
use std::io::{self, Read, Write};
use std::os::unix::ffi::{OsStrExt, OsStringExt};
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail};
use clap::{Args, Parser, Subcommand, ValueEnum};
use serde::Serialize;
use serde_json::{Value, json};

use crate::artifact::{ingest_result_file, remove_ingest_marker_best_effort};
use crate::cli_app_server_client::{
    AppServerJsonRpcClient, AppServerNotification, AppServerReceive, AppServerRequestError,
    ThreadActivitySnapshot, decode_notification, thread_result_activity_snapshot,
};
use crate::daemon::{
    DaemonEnsureOptions, DaemonServeOptions, daemon_ensure, daemon_request, daemon_request_payload,
    daemon_request_payload_timeout, daemon_serve, validate_daemon_autostart_endpoint,
};
use crate::fs_layout::{FsLayout, remove_dir_all_durable};
use crate::models::{
    CliManagedSessionCapabilities, CliManagedSessionProfile, DEFAULT_MAX_DELIVERY_ATTEMPTS,
    DEFAULT_REDELIVERY_WINDOW_SECONDS, DeliveryPolicy, NewCliAcceptPendingAttempt, NewJob,
    PartialDeliveryPolicy, SubmitMetadata,
};
use crate::store::{Store, new_id};

const MAX_METADATA_BYTES: u64 = 1024 * 1024;
const DIRECT_STORE_ENV: &str = "CBTH_ALLOW_DIRECT_STORE";
const DEFAULT_DAEMON_IDLE_TIMEOUT_SECONDS: u64 = 300;
const DEFAULT_DAEMON_STARTUP_TIMEOUT_SECONDS: u64 = 5;
const MAX_CLI_OBSERVATION_WINDOW_SECONDS: i64 = 6 * 60 * 60;
const CLI_APP_SERVER_LEASE_TTL_SECONDS: u64 = 60;
const CLI_APP_SERVER_LEASE_REFRESH_SECONDS: u64 = 20;
const CLI_APP_SERVER_ENSURE_TIMEOUT_SECONDS: u64 = 15;
const CLI_APP_SERVER_CONTROL_TIMEOUT_SECONDS: u64 = 5;
const CLI_APP_SERVER_PASSIVE_CONNECT_TIMEOUT_MS: u64 = 250;
const CLI_APP_SERVER_PASSIVE_REQUEST_TIMEOUT_SECONDS: u64 = 3;
const CLI_APP_SERVER_PASSIVE_RECV_TIMEOUT_MS: u64 = 500;
const CLI_APP_SERVER_PASSIVE_RETRY_MAX_MS: u64 = 500;
const CLI_APP_SERVER_PASSIVE_STORE_TIMEOUT_SECONDS: u64 = 1;

#[derive(Debug, Parser)]
#[command(name = "cbth")]
#[command(about = "Codex background task handler")]
pub struct Cli {
    #[arg(long, global = true)]
    home: Option<PathBuf>,

    #[arg(long, global = true, hide = true)]
    direct_store: bool,

    #[arg(long, global = true, default_value_t = DEFAULT_DAEMON_STARTUP_TIMEOUT_SECONDS)]
    auto_daemon_startup_timeout_seconds: u64,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    Job {
        #[command(subcommand)]
        command: JobCommand,
    },
    Batch {
        #[command(subcommand)]
        command: BatchCommand,
    },
    #[command(hide = true)]
    Attempt {
        #[command(subcommand)]
        command: AttemptCommand,
    },
    Cli {
        #[command(subcommand)]
        command: CliCommand,
    },
    Maintenance {
        #[command(subcommand)]
        command: MaintenanceCommand,
    },
    Daemon {
        #[command(subcommand)]
        command: DaemonCommand,
    },
}

#[derive(Debug, Subcommand)]
enum JobCommand {
    Submit(JobSubmitArgs),
    Complete(JobCompleteArgs),
    Fail(JobFailArgs),
    Inspect(JobInspectArgs),
    List(JobListArgs),
}

#[derive(Debug, Args)]
struct JobSubmitArgs {
    #[arg(long)]
    source_thread_id: String,

    #[arg(long)]
    summary: String,

    #[arg(long)]
    metadata_file: Option<PathBuf>,

    #[arg(long, value_parser = clap::value_parser!(bool))]
    delivery_read_only: Option<bool>,

    #[arg(long, value_parser = clap::value_parser!(bool))]
    delivery_requires_approval: Option<bool>,

    #[arg(long, value_parser = clap::value_parser!(bool))]
    delivery_requires_network: Option<bool>,

    #[arg(long, value_parser = clap::value_parser!(bool))]
    delivery_requires_write_access: Option<bool>,
}

#[derive(Debug, Args)]
struct JobCompleteArgs {
    #[arg(long)]
    job_id: String,

    #[arg(long)]
    result_file: PathBuf,

    #[arg(long)]
    summary: Option<String>,

    #[arg(long, default_value_t = DEFAULT_MAX_DELIVERY_ATTEMPTS)]
    max_delivery_attempts: i64,

    #[arg(long, default_value_t = DEFAULT_REDELIVERY_WINDOW_SECONDS)]
    redelivery_window_seconds: i64,
}

#[derive(Debug, Args)]
struct JobFailArgs {
    #[arg(long)]
    job_id: String,

    #[arg(long)]
    reason: String,

    #[arg(long, default_value_t = DEFAULT_MAX_DELIVERY_ATTEMPTS)]
    max_delivery_attempts: i64,

    #[arg(long, default_value_t = DEFAULT_REDELIVERY_WINDOW_SECONDS)]
    redelivery_window_seconds: i64,
}

#[derive(Debug, Args)]
struct JobInspectArgs {
    #[arg(long)]
    job_id: String,
}

#[derive(Debug, Args)]
struct JobListArgs {
    #[arg(long)]
    source_thread_id: Option<String>,

    #[arg(long)]
    status: Option<String>,

    #[arg(long, default_value_t = 100)]
    limit: i64,
}

#[derive(Debug, Subcommand)]
enum BatchCommand {
    InspectHead(BatchInspectHeadArgs),
    Inspect(BatchInspectArgs),
    CloseHead(BatchCloseHeadArgs),
}

#[derive(Debug, Subcommand)]
enum AttemptCommand {
    BeginCliAccept(AttemptBeginCliAcceptArgs),
    AcceptCli(AttemptAcceptCliArgs),
    ObserveCliTurn(AttemptObserveCliTurnArgs),
    Inspect(AttemptInspectArgs),
}

#[derive(Debug, Subcommand)]
enum CliCommand {
    Run(CliRunArgs),
    #[command(hide = true)]
    Session {
        #[command(subcommand)]
        command: CliSessionCommand,
    },
}

#[derive(Debug, Subcommand)]
enum CliSessionCommand {
    Bind(CliSessionBindArgs),
    NoteActivity(CliSessionNoteActivityArgs),
    NoteCapabilities(CliSessionNoteCapabilitiesArgs),
    InvalidateProof(CliSessionInvalidateProofArgs),
    Inspect(CliSessionInspectArgs),
}

#[derive(Debug, Args)]
struct CliRunArgs {
    #[arg(long)]
    bind_thread_id: String,

    #[arg(long, required = true, value_parser = clap::value_parser!(bool), action = clap::ArgAction::Set)]
    session_allows_approval: bool,

    #[arg(long, required = true, value_parser = clap::value_parser!(bool), action = clap::ArgAction::Set)]
    session_allows_network: bool,

    #[arg(long, required = true, value_parser = clap::value_parser!(bool), action = clap::ArgAction::Set)]
    session_allows_write_access: bool,

    #[arg(long, default_value = "codex")]
    codex_bin: OsString,

    #[arg(last = true)]
    codex_args: Vec<OsString>,
}

#[derive(Debug, Args)]
struct BatchInspectHeadArgs {
    #[arg(long)]
    source_thread_id: String,
}

#[derive(Debug, Args)]
struct BatchInspectArgs {
    #[arg(long)]
    batch_id: String,
}

#[derive(Debug, Args)]
struct BatchCloseHeadArgs {
    #[arg(long)]
    source_thread_id: String,

    #[arg(long, value_enum)]
    reason: CloseReason,

    #[arg(long)]
    note: Option<String>,
}

#[derive(Clone, Debug, ValueEnum)]
enum CloseReason {
    OperatorClosedUnconfirmed,
    OperatorConfirmedDelivery,
}

impl CloseReason {
    fn as_str(&self) -> &'static str {
        match self {
            Self::OperatorClosedUnconfirmed => "operator_closed_unconfirmed",
            Self::OperatorConfirmedDelivery => "operator_confirmed_delivery",
        }
    }

    fn cli_value(&self) -> &'static str {
        match self {
            Self::OperatorClosedUnconfirmed => "operator-closed-unconfirmed",
            Self::OperatorConfirmedDelivery => "operator-confirmed-delivery",
        }
    }
}

#[derive(Clone, Debug, ValueEnum)]
enum AttemptRpcKind {
    TurnStart,
    TurnSteer,
}

impl AttemptRpcKind {
    fn as_str(&self) -> &'static str {
        match self {
            Self::TurnStart => "turn_start",
            Self::TurnSteer => "turn_steer",
        }
    }

    fn cli_value(&self) -> &'static str {
        match self {
            Self::TurnStart => "turn-start",
            Self::TurnSteer => "turn-steer",
        }
    }
}

#[derive(Debug, Args)]
struct AttemptBeginCliAcceptArgs {
    #[arg(long)]
    batch_id: String,

    #[arg(long)]
    managed_session_id: String,

    #[arg(long)]
    session_epoch: i64,

    #[arg(long, value_enum)]
    rpc_kind: AttemptRpcKind,

    #[arg(long)]
    rpc_request_id: String,

    #[arg(long)]
    rpc_correlation_marker: Option<String>,

    #[arg(long, hide = true)]
    now: Option<i64>,
}

#[derive(Debug, Args)]
struct AttemptAcceptCliArgs {
    #[arg(long)]
    attempt_id: String,

    #[arg(long)]
    delivery_turn_id: String,

    #[arg(long)]
    observation_window_seconds: i64,

    #[arg(long, hide = true)]
    now: Option<i64>,
}

#[derive(Clone, Debug, ValueEnum)]
enum CliTurnEvent {
    #[value(name = "turn-started")]
    Started,
    #[value(name = "turn-completed")]
    Completed,
    #[value(name = "turn-failed")]
    Failed,
    #[value(name = "turn-interrupted")]
    Interrupted,
    #[value(name = "turn-replaced")]
    Replaced,
}

impl CliTurnEvent {
    fn as_str(&self) -> &'static str {
        match self {
            Self::Started => "turn_started",
            Self::Completed => "turn_completed",
            Self::Failed => "turn_failed",
            Self::Interrupted => "turn_interrupted",
            Self::Replaced => "turn_replaced",
        }
    }

    fn cli_value(&self) -> &'static str {
        match self {
            Self::Started => "turn-started",
            Self::Completed => "turn-completed",
            Self::Failed => "turn-failed",
            Self::Interrupted => "turn-interrupted",
            Self::Replaced => "turn-replaced",
        }
    }
}

#[derive(Debug, Args)]
struct AttemptObserveCliTurnArgs {
    #[arg(long)]
    attempt_id: String,

    #[arg(long)]
    delivery_turn_id: String,

    #[arg(long, value_enum)]
    turn_event: CliTurnEvent,

    #[arg(long, hide = true)]
    now: Option<i64>,
}

#[derive(Debug, Args)]
struct AttemptInspectArgs {
    #[arg(long)]
    attempt_id: String,
}

#[derive(Debug, Args)]
struct CliSessionBindArgs {
    #[arg(long)]
    bound_thread_id: String,

    #[arg(long, required = true, value_parser = clap::value_parser!(bool), action = clap::ArgAction::Set)]
    session_allows_approval: bool,

    #[arg(long, required = true, value_parser = clap::value_parser!(bool), action = clap::ArgAction::Set)]
    session_allows_network: bool,

    #[arg(long, required = true, value_parser = clap::value_parser!(bool), action = clap::ArgAction::Set)]
    session_allows_write_access: bool,

    #[arg(long, hide = true)]
    now: Option<i64>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
enum CliSessionActivityState {
    Active,
    Idle,
}

impl CliSessionActivityState {
    fn as_str(&self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::Idle => "idle",
        }
    }

    fn cli_value(&self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::Idle => "idle",
        }
    }
}

#[derive(Debug, Args)]
struct CliSessionNoteActivityArgs {
    #[arg(long)]
    managed_session_id: String,

    #[arg(long)]
    session_epoch: i64,

    #[arg(long, value_enum)]
    activity_state: CliSessionActivityState,

    #[arg(long)]
    activity_revision: i64,

    #[arg(long, hide = true)]
    now: Option<i64>,
}

#[derive(Debug, Args)]
struct CliSessionNoteCapabilitiesArgs {
    #[arg(long)]
    managed_session_id: String,

    #[arg(long)]
    session_epoch: i64,

    #[arg(long)]
    capability_revision: i64,

    #[arg(long, value_parser = clap::value_parser!(bool), action = clap::ArgAction::Set)]
    thread_resume: bool,

    #[arg(long, value_parser = clap::value_parser!(bool), action = clap::ArgAction::Set)]
    turn_start: bool,

    #[arg(long, value_parser = clap::value_parser!(bool), action = clap::ArgAction::Set)]
    current_state_sync: bool,

    #[arg(long, value_parser = clap::value_parser!(bool), action = clap::ArgAction::Set)]
    turn_completed_event: bool,

    #[arg(long, value_parser = clap::value_parser!(bool), action = clap::ArgAction::Set)]
    negative_terminal_events: bool,

    #[arg(long, value_parser = clap::value_parser!(bool), default_value_t = false, action = clap::ArgAction::Set)]
    thread_start: bool,

    #[arg(long, value_parser = clap::value_parser!(bool), default_value_t = false, action = clap::ArgAction::Set)]
    turn_steer: bool,

    #[arg(long, hide = true)]
    now: Option<i64>,
}

#[derive(Debug, Args)]
struct CliSessionInvalidateProofArgs {
    #[arg(long)]
    managed_session_id: String,

    #[arg(long)]
    session_epoch: i64,

    #[arg(long, hide = true)]
    now: Option<i64>,
}

#[derive(Debug, Args)]
struct CliSessionInspectArgs {
    #[arg(long)]
    managed_session_id: String,
}

#[derive(Debug, Subcommand)]
enum MaintenanceCommand {
    Sweep(MaintenanceSweepArgs),
}

#[derive(Debug, Args)]
struct MaintenanceSweepArgs {
    #[arg(long)]
    now: Option<i64>,
}

#[derive(Debug, Subcommand)]
enum DaemonCommand {
    Serve(DaemonServeArgs),
    Ensure(DaemonEnsureArgs),
    Ping,
    Status,
    Stop,
}

#[derive(Debug, Args)]
struct DaemonServeArgs {
    #[arg(long, default_value_t = 300)]
    idle_timeout_seconds: u64,

    #[arg(long)]
    now: Option<i64>,

    #[arg(long, hide = true)]
    skip_startup_sweep: bool,
}

#[derive(Debug, Args)]
struct DaemonEnsureArgs {
    #[arg(long, default_value_t = 300)]
    idle_timeout_seconds: u64,

    #[arg(long, default_value_t = 5)]
    startup_timeout_seconds: u64,
}

pub fn run() -> Result<()> {
    let cli = Cli::parse();
    if cli.direct_store {
        require_direct_store_env()?;
    }
    validate_nonzero_u64(
        "auto_daemon_startup_timeout_seconds",
        cli.auto_daemon_startup_timeout_seconds,
    )?;
    let layout = FsLayout::resolve(cli.home)?;
    let output = match cli.command {
        Commands::Cli {
            command: CliCommand::Run(args),
        } => {
            if cli.direct_store {
                bail!("cli run does not support --direct-store");
            }
            let exit_code =
                run_cli_session(args, &layout, cli.auto_daemon_startup_timeout_seconds)?;
            std::process::exit(exit_code);
        }
        command => dispatch(
            command,
            &layout,
            DispatchMode::Client {
                direct_store: cli.direct_store,
                startup_timeout_seconds: cli.auto_daemon_startup_timeout_seconds,
            },
        )?,
    };
    write_json(&output)
}

enum DispatchMode {
    Client {
        direct_store: bool,
        startup_timeout_seconds: u64,
    },
    Direct,
}

fn dispatch(command: Commands, layout: &FsLayout, mode: DispatchMode) -> Result<Value> {
    if let DispatchMode::Client {
        direct_store: false,
        startup_timeout_seconds,
    } = mode
        && let Some(argv) = daemon_argv_for_mutating_command(&command)?
    {
        let startup_sweep_now = daemon_startup_sweep_for_command(&command)?;
        validate_daemon_autostart_endpoint(layout)?;
        let ensure_options = DaemonEnsureOptions {
            idle_timeout_seconds: DEFAULT_DAEMON_IDLE_TIMEOUT_SECONDS,
            startup_timeout_seconds,
            startup_sweep_now,
        };
        daemon_ensure(layout, ensure_options)?;
        return daemon_request_payload(layout, "dispatch", json!({ "argv": argv_payload(argv) }));
    }

    dispatch_direct(command, layout)
}

pub(crate) fn dispatch_daemon_argv(layout: &FsLayout, argv: Vec<Vec<u8>>) -> Result<Value> {
    let mut parse_argv = Vec::with_capacity(argv.len() + 1);
    parse_argv.push(OsString::from("cbth"));
    parse_argv.extend(argv.into_iter().map(OsString::from_vec));
    let cli = Cli::try_parse_from(parse_argv)?;
    if cli.home.is_some() {
        bail!("daemon dispatch must not include --home");
    }
    if cli.direct_store {
        bail!("daemon dispatch must not include --direct-store");
    }
    match cli.command {
        Commands::Daemon { .. } => bail!("daemon dispatch cannot execute daemon commands"),
        command => dispatch(command, layout, DispatchMode::Direct),
    }
}

fn dispatch_direct(command: Commands, layout: &FsLayout) -> Result<Value> {
    match command {
        Commands::Job { command } => dispatch_job(command, layout),
        Commands::Batch { command } => dispatch_batch(command, layout),
        Commands::Attempt { command } => dispatch_attempt(command, layout),
        Commands::Cli { command } => dispatch_cli(command, layout),
        Commands::Maintenance { command } => dispatch_maintenance(command, layout),
        Commands::Daemon { command } => dispatch_daemon(command, layout),
    }
}

fn run_cli_session(
    args: CliRunArgs,
    layout: &FsLayout,
    startup_timeout_seconds: u64,
) -> Result<i32> {
    validate_nonempty("bind_thread_id", &args.bind_thread_id)?;
    let codex_binary = resolve_executable(&args.codex_bin)?;
    let lease_id = new_id();

    validate_daemon_autostart_endpoint(layout)?;
    daemon_ensure(
        layout,
        DaemonEnsureOptions {
            idle_timeout_seconds: DEFAULT_DAEMON_IDLE_TIMEOUT_SECONDS,
            startup_timeout_seconds,
            startup_sweep_now: Some(now_epoch_seconds()?),
        },
    )?;
    reserve_cli_app_server_for_thread(layout, &args.bind_thread_id, &lease_id)?;

    let bind = match dispatch(
        Commands::Cli {
            command: CliCommand::Session {
                command: CliSessionCommand::Bind(CliSessionBindArgs {
                    bound_thread_id: args.bind_thread_id.clone(),
                    session_allows_approval: args.session_allows_approval,
                    session_allows_network: args.session_allows_network,
                    session_allows_write_access: args.session_allows_write_access,
                    now: None,
                }),
            },
        },
        layout,
        DispatchMode::Client {
            direct_store: false,
            startup_timeout_seconds,
        },
    ) {
        Ok(bind) => bind,
        Err(error) => {
            release_cli_app_server_reservation_best_effort(layout, &args.bind_thread_id, &lease_id);
            return Err(error);
        }
    };
    let session = &bind["cli_session"]["session"];
    let managed_session_id = json_string(session, "managed_session_id")?;
    let bound_thread_id = json_string(session, "bound_thread_id")?;
    let session_epoch = json_i64(session, "session_epoch")?;
    let activity_revision = json_i64(session, "activity_revision")?;
    let capability_revision = json_i64(session, "capability_revision")?;
    let app_server = match daemon_request_payload_timeout(
        layout,
        "cli_app_server_ensure",
        json!({
            "managed_session_id": managed_session_id,
            "bound_thread_id": bound_thread_id,
            "session_epoch": session_epoch,
            "codex_binary": codex_binary.as_bytes(),
            "lease_id": lease_id,
            "lease_ttl_seconds": CLI_APP_SERVER_LEASE_TTL_SECONDS,
        }),
        Duration::from_secs(CLI_APP_SERVER_ENSURE_TIMEOUT_SECONDS),
    ) {
        Ok(app_server) => app_server,
        Err(error) => {
            release_cli_app_server_reservation_best_effort(layout, &bound_thread_id, &lease_id);
            return Err(error);
        }
    };
    let url = json_string(&app_server["cli_app_server"], "url")?;
    let refresh_running = Arc::new(AtomicBool::new(true));
    spawn_cli_app_server_lease_refresher(
        layout.clone(),
        managed_session_id.clone(),
        lease_id.clone(),
        Arc::clone(&refresh_running),
    );
    let mut passive_adapter =
        spawn_cli_app_server_passive_adapter(CliAppServerPassiveAdapterConfig {
            layout: layout.clone(),
            url: url.clone(),
            managed_session_id: managed_session_id.clone(),
            bound_thread_id: bound_thread_id.clone(),
            session_epoch,
            activity_revision,
            capability_revision,
        });

    let foreground_status = Command::new(&codex_binary)
        .arg("--remote")
        .arg(&url)
        .arg("--cd")
        .arg(env::current_dir().context("read current directory")?)
        .args(args.codex_args)
        .status()
        .with_context(|| format!("spawn foreground codex via {:?}", codex_binary));
    let passive_stop_result = passive_adapter.stop();
    refresh_running.store(false, Ordering::Release);
    let stop_result = daemon_request_payload_timeout(
        layout,
        "cli_app_server_stop",
        json!({
            "managed_session_id": managed_session_id,
            "lease_id": lease_id,
        }),
        Duration::from_secs(CLI_APP_SERVER_CONTROL_TIMEOUT_SECONDS),
    );
    if let Err(error) = stop_result {
        eprintln!("warning: failed to stop CLI app-server: {error:#}");
    }
    passive_stop_result?;
    let status = foreground_status?;
    Ok(status.code().unwrap_or(1))
}

fn reserve_cli_app_server_for_thread(
    layout: &FsLayout,
    bound_thread_id: &str,
    lease_id: &str,
) -> Result<()> {
    daemon_request_payload_timeout(
        layout,
        "cli_app_server_reserve",
        json!({
            "bound_thread_id": bound_thread_id,
            "lease_id": lease_id,
            "lease_ttl_seconds": CLI_APP_SERVER_LEASE_TTL_SECONDS,
        }),
        Duration::from_secs(CLI_APP_SERVER_CONTROL_TIMEOUT_SECONDS),
    )?;
    Ok(())
}

fn release_cli_app_server_reservation_best_effort(
    layout: &FsLayout,
    bound_thread_id: &str,
    lease_id: &str,
) {
    let _ = daemon_request_payload_timeout(
        layout,
        "cli_app_server_release",
        json!({
            "bound_thread_id": bound_thread_id,
            "lease_id": lease_id,
        }),
        Duration::from_secs(CLI_APP_SERVER_CONTROL_TIMEOUT_SECONDS),
    );
}

fn spawn_cli_app_server_lease_refresher(
    layout: FsLayout,
    managed_session_id: String,
    lease_id: String,
    running: Arc<AtomicBool>,
) {
    thread::spawn(move || {
        while running.load(Ordering::Acquire) {
            thread::sleep(Duration::from_secs(CLI_APP_SERVER_LEASE_REFRESH_SECONDS));
            if !running.load(Ordering::Acquire) {
                break;
            }
            let _ = daemon_request_payload_timeout(
                &layout,
                "cli_app_server_refresh",
                json!({
                    "managed_session_id": managed_session_id,
                    "lease_id": lease_id,
                    "lease_ttl_seconds": CLI_APP_SERVER_LEASE_TTL_SECONDS,
                }),
                Duration::from_secs(CLI_APP_SERVER_CONTROL_TIMEOUT_SECONDS),
            );
        }
    });
}

#[derive(Clone)]
struct CliAppServerPassiveAdapterConfig {
    layout: FsLayout,
    url: String,
    managed_session_id: String,
    bound_thread_id: String,
    session_epoch: i64,
    activity_revision: i64,
    capability_revision: i64,
}

struct CliAppServerPassiveAdapterHandle {
    running: Arc<AtomicBool>,
    join: Option<thread::JoinHandle<()>>,
    config: CliAppServerPassiveAdapterConfig,
    state: Arc<Mutex<CliAppServerPassiveAdapterState>>,
}

impl CliAppServerPassiveAdapterHandle {
    fn stop(&mut self) -> Result<()> {
        self.running.store(false, Ordering::Release);
        if let Some(join) = self.join.take() {
            let _ = join.join();
        }
        let mut state = self
            .state
            .lock()
            .map_err(|_| anyhow::anyhow!("CLI passive adapter state lock poisoned"))?;
        invalidate_passive_adapter_proof(&self.config, &mut state)?;
        Ok(())
    }
}

impl Drop for CliAppServerPassiveAdapterHandle {
    fn drop(&mut self) {
        if let Err(error) = self.stop()
            && env::var_os("CBTH_DEBUG_PASSIVE_ADAPTER").is_some()
        {
            eprintln!("debug: CLI passive adapter final invalidation failed: {error:#}");
        }
    }
}

struct CliAppServerPassiveAdapterState {
    session_epoch: i64,
    activity_revision: i64,
    capability_revision: i64,
    last_activity_state: Option<CliSessionActivityState>,
    passive_capabilities_recorded: bool,
    durable_proof_may_exist: bool,
}

fn spawn_cli_app_server_passive_adapter(
    config: CliAppServerPassiveAdapterConfig,
) -> CliAppServerPassiveAdapterHandle {
    let running = Arc::new(AtomicBool::new(true));
    let thread_running = Arc::clone(&running);
    let state = Arc::new(Mutex::new(CliAppServerPassiveAdapterState {
        session_epoch: config.session_epoch,
        activity_revision: config.activity_revision,
        capability_revision: config.capability_revision,
        last_activity_state: None,
        passive_capabilities_recorded: false,
        durable_proof_may_exist: config.activity_revision != 0 || config.capability_revision != 0,
    }));
    let thread_state = Arc::clone(&state);
    let thread_config = config.clone();
    let join = thread::spawn(move || {
        let mut retry_delay = Duration::from_millis(CLI_APP_SERVER_PASSIVE_CONNECT_TIMEOUT_MS);
        while thread_running.load(Ordering::Acquire) {
            let Ok(mut state) = thread_state.lock() else {
                break;
            };
            match run_cli_app_server_passive_adapter_once(
                &thread_config,
                &mut state,
                &thread_running,
            ) {
                Ok(()) => {
                    retry_delay = Duration::from_millis(CLI_APP_SERVER_PASSIVE_CONNECT_TIMEOUT_MS)
                }
                Err(error) => {
                    if let Err(invalidation_error) =
                        invalidate_passive_adapter_proof(&thread_config, &mut state)
                        && env::var_os("CBTH_DEBUG_PASSIVE_ADAPTER").is_some()
                    {
                        eprintln!(
                            "debug: CLI passive adapter failed to invalidate proof after iteration error: {invalidation_error:#}"
                        );
                    }
                    if env::var_os("CBTH_DEBUG_PASSIVE_ADAPTER").is_some() {
                        eprintln!("debug: CLI passive adapter iteration failed: {error:#}");
                    }
                }
            }
            wait_for_passive_adapter_retry(&thread_running, retry_delay);
            retry_delay =
                (retry_delay * 2).min(Duration::from_millis(CLI_APP_SERVER_PASSIVE_RETRY_MAX_MS));
        }
    });

    CliAppServerPassiveAdapterHandle {
        running,
        join: Some(join),
        config,
        state,
    }
}

fn run_cli_app_server_passive_adapter_once(
    config: &CliAppServerPassiveAdapterConfig,
    state: &mut CliAppServerPassiveAdapterState,
    running: &AtomicBool,
) -> Result<()> {
    state.last_activity_state = None;
    let mut client = AppServerJsonRpcClient::connect(
        &config.url,
        Duration::from_millis(CLI_APP_SERVER_PASSIVE_CONNECT_TIMEOUT_MS),
    )?;
    client.initialize(
        env!("CARGO_PKG_VERSION"),
        Duration::from_secs(CLI_APP_SERVER_PASSIVE_REQUEST_TIMEOUT_SECONDS),
    )?;
    client.notify_initialized()?;

    let (resume_result, resume_messages) = passive_adapter_request(
        &mut client,
        "thread/resume",
        json!({ "threadId": config.bound_thread_id }),
        Duration::from_secs(CLI_APP_SERVER_PASSIVE_REQUEST_TIMEOUT_SECONDS),
    );
    let resume = resume_result?;
    let mut current_state_sync = false;
    let mut active = false;
    match thread_result_activity_snapshot(&resume, &config.bound_thread_id) {
        ThreadActivitySnapshot::Active => {
            current_state_sync = true;
            active = true;
        }
        ThreadActivitySnapshot::Idle => {
            current_state_sync = true;
        }
        ThreadActivitySnapshot::Missing => {}
        ThreadActivitySnapshot::Untrusted => {
            invalidate_passive_adapter_proof(config, state)?;
            bail!("app-server thread/resume returned untrusted current-state snapshot");
        }
    }
    let mut messages_to_replay = Vec::new();

    let (read_result, read_messages) = passive_adapter_request(
        &mut client,
        "thread/read",
        json!({
            "threadId": config.bound_thread_id,
            "includeTurns": true,
        }),
        Duration::from_secs(CLI_APP_SERVER_PASSIVE_REQUEST_TIMEOUT_SECONDS),
    );
    match read_result {
        Ok(read) => match thread_result_activity_snapshot(&read, &config.bound_thread_id) {
            ThreadActivitySnapshot::Active => {
                active = true;
                current_state_sync = true;
            }
            ThreadActivitySnapshot::Idle => {
                active = false;
                current_state_sync = true;
            }
            ThreadActivitySnapshot::Missing if current_state_sync => {
                messages_to_replay = resume_messages;
                messages_to_replay.extend(read_messages);
            }
            ThreadActivitySnapshot::Missing => {}
            ThreadActivitySnapshot::Untrusted => {
                invalidate_passive_adapter_proof(config, state)?;
                bail!("app-server thread/read returned untrusted current-state snapshot");
            }
        },
        Err(error) => {
            invalidate_passive_adapter_proof(config, state)?;
            return Err(error.into());
        }
    }

    if current_state_sync {
        record_passive_adapter_capabilities(config, state)?;
        record_passive_adapter_activity(
            config,
            state,
            if active {
                CliSessionActivityState::Active
            } else {
                CliSessionActivityState::Idle
            },
        )?;
    } else {
        invalidate_passive_adapter_proof(config, state)?;
    }
    for message in messages_to_replay {
        if let Some(notification) = decode_notification(&message) {
            record_passive_adapter_notification(config, state, notification)?;
        }
    }

    while running.load(Ordering::Acquire) {
        match client.recv(Duration::from_millis(
            CLI_APP_SERVER_PASSIVE_RECV_TIMEOUT_MS,
        ))? {
            AppServerReceive::Message(message) => {
                if let Some(notification) = decode_notification(&message) {
                    record_passive_adapter_notification(config, state, notification)?;
                }
            }
            AppServerReceive::Timeout => {}
            AppServerReceive::Closed => {
                if running.load(Ordering::Acquire) {
                    invalidate_passive_adapter_proof(config, state)?;
                }
                break;
            }
        }
    }
    Ok(())
}

fn passive_adapter_request(
    client: &mut AppServerJsonRpcClient,
    method: &str,
    params: Value,
    timeout: Duration,
) -> (
    std::result::Result<Value, AppServerRequestError>,
    Vec<Value>,
) {
    let result = client.request(method, params, timeout);
    let messages = client.drain_pending_messages();
    (result, messages)
}

fn record_passive_adapter_notification(
    config: &CliAppServerPassiveAdapterConfig,
    state: &mut CliAppServerPassiveAdapterState,
    notification: AppServerNotification,
) -> Result<()> {
    let notification_thread_id = match &notification {
        AppServerNotification::TurnStarted { thread_id }
        | AppServerNotification::TurnTerminal { thread_id }
        | AppServerNotification::ThreadProofInvalidated { thread_id }
        | AppServerNotification::ThreadActivityChanged { thread_id, .. } => thread_id,
    };
    if notification_thread_id.is_none() {
        invalidate_passive_adapter_proof(config, state)?;
        return Ok(());
    }

    match notification {
        AppServerNotification::TurnStarted { thread_id }
            if passive_adapter_thread_matches(&thread_id, &config.bound_thread_id) =>
        {
            record_passive_adapter_activity(config, state, CliSessionActivityState::Active)?;
        }
        AppServerNotification::TurnTerminal { thread_id }
            if passive_adapter_thread_matches(&thread_id, &config.bound_thread_id)
                && state.last_activity_state == Some(CliSessionActivityState::Active) =>
        {
            record_passive_adapter_activity(config, state, CliSessionActivityState::Idle)?;
        }
        AppServerNotification::ThreadActivityChanged { thread_id, active }
            if passive_adapter_thread_matches(&thread_id, &config.bound_thread_id) =>
        {
            if !active && state.last_activity_state != Some(CliSessionActivityState::Active) {
                return Ok(());
            }
            record_passive_adapter_activity(
                config,
                state,
                if active {
                    CliSessionActivityState::Active
                } else {
                    CliSessionActivityState::Idle
                },
            )?;
        }
        AppServerNotification::ThreadProofInvalidated { thread_id }
            if passive_adapter_thread_matches(&thread_id, &config.bound_thread_id) =>
        {
            invalidate_passive_adapter_proof(config, state)?;
        }
        _ => {}
    }
    Ok(())
}

fn record_passive_adapter_activity(
    config: &CliAppServerPassiveAdapterConfig,
    state: &mut CliAppServerPassiveAdapterState,
    activity_state: CliSessionActivityState,
) -> Result<()> {
    if state.last_activity_state == Some(activity_state) {
        return Ok(());
    }
    let next_revision = state
        .activity_revision
        .checked_add(1)
        .context("CLI passive adapter activity revision overflow")?;
    let result = dispatch_passive_adapter_command(
        config,
        Commands::Cli {
            command: CliCommand::Session {
                command: CliSessionCommand::NoteActivity(CliSessionNoteActivityArgs {
                    managed_session_id: config.managed_session_id.clone(),
                    session_epoch: state.session_epoch,
                    activity_state,
                    activity_revision: next_revision,
                    now: None,
                }),
            },
        },
        false,
    );
    state.durable_proof_may_exist = true;
    match result {
        Ok(_) => {
            state.activity_revision = next_revision;
            state.last_activity_state = Some(activity_state);
            Ok(())
        }
        Err(error) => {
            invalidate_passive_adapter_proof(config, state).with_context(|| {
                format!("invalidate passive adapter proof after activity write failed: {error:#}")
            })?;
            Err(error)
        }
    }
}

fn record_passive_adapter_capabilities(
    config: &CliAppServerPassiveAdapterConfig,
    state: &mut CliAppServerPassiveAdapterState,
) -> Result<()> {
    if state.passive_capabilities_recorded {
        return Ok(());
    }
    let next_revision = state
        .capability_revision
        .checked_add(1)
        .context("CLI passive adapter capability revision overflow")?;
    let result = dispatch_passive_adapter_command(
        config,
        Commands::Cli {
            command: CliCommand::Session {
                command: CliSessionCommand::NoteCapabilities(CliSessionNoteCapabilitiesArgs {
                    managed_session_id: config.managed_session_id.clone(),
                    session_epoch: state.session_epoch,
                    capability_revision: next_revision,
                    thread_resume: true,
                    turn_start: false,
                    current_state_sync: true,
                    turn_completed_event: false,
                    negative_terminal_events: false,
                    thread_start: false,
                    turn_steer: false,
                    now: None,
                }),
            },
        },
        false,
    );
    state.durable_proof_may_exist = true;
    match result {
        Ok(_) => {
            state.capability_revision = next_revision;
            state.passive_capabilities_recorded = true;
            Ok(())
        }
        Err(error) => {
            invalidate_passive_adapter_proof(config, state).with_context(|| {
                format!("invalidate passive adapter proof after capability write failed: {error:#}")
            })?;
            Err(error)
        }
    }
}

fn invalidate_passive_adapter_proof(
    config: &CliAppServerPassiveAdapterConfig,
    state: &mut CliAppServerPassiveAdapterState,
) -> Result<()> {
    if !state.durable_proof_may_exist
        && state.activity_revision == 0
        && state.capability_revision == 0
    {
        state.last_activity_state = None;
        state.passive_capabilities_recorded = false;
        return Ok(());
    }
    let value = dispatch_passive_adapter_command(
        config,
        Commands::Cli {
            command: CliCommand::Session {
                command: CliSessionCommand::InvalidateProof(CliSessionInvalidateProofArgs {
                    managed_session_id: config.managed_session_id.clone(),
                    session_epoch: state.session_epoch,
                    now: None,
                }),
            },
        },
        true,
    )?;
    let session = value.get("cli_session").ok_or_else(|| {
        anyhow::anyhow!("passive adapter invalidation response missing cli_session")
    })?;
    apply_passive_adapter_invalidated_session(config, state, session)?;
    Ok(())
}

fn apply_passive_adapter_invalidated_session(
    config: &CliAppServerPassiveAdapterConfig,
    state: &mut CliAppServerPassiveAdapterState,
    session: &Value,
) -> Result<()> {
    let managed_session_id = json_string(session, "managed_session_id")?;
    if managed_session_id != config.managed_session_id {
        bail!("passive adapter invalidation returned a different managed session");
    }
    let bound_thread_id = json_string(session, "bound_thread_id")?;
    if bound_thread_id != config.bound_thread_id {
        bail!("passive adapter invalidation returned a different bound thread");
    }
    let activity_state = json_string(session, "activity_state")?;
    let session_epoch = json_i64(session, "session_epoch")?;
    let activity_revision = json_i64(session, "activity_revision")?;
    let capability_revision = json_i64(session, "capability_revision")?;
    if activity_state != "unknown" || activity_revision != 0 || capability_revision != 0 {
        bail!("passive adapter invalidation response did not clear proof");
    }
    state.session_epoch = session_epoch;
    state.activity_revision = activity_revision;
    state.capability_revision = capability_revision;
    state.last_activity_state = None;
    state.passive_capabilities_recorded = false;
    state.durable_proof_may_exist = false;
    Ok(())
}

fn dispatch_passive_adapter_command(
    config: &CliAppServerPassiveAdapterConfig,
    command: Commands,
    allow_direct_fallback: bool,
) -> Result<Value> {
    let Some(argv) = daemon_argv_for_mutating_command(&command)? else {
        bail!("passive adapter command is not daemon-routable");
    };
    let result = daemon_request_payload_timeout(
        &config.layout,
        "dispatch",
        json!({ "argv": argv_payload(argv) }),
        Duration::from_secs(CLI_APP_SERVER_PASSIVE_STORE_TIMEOUT_SECONDS),
    );
    match result {
        Ok(value) => Ok(value),
        Err(error) if allow_direct_fallback => {
            dispatch(command, &config.layout, DispatchMode::Direct).with_context(|| {
                format!("fallback direct passive adapter command after daemon error: {error:#}")
            })
        }
        Err(error) => Err(error),
    }
}

fn passive_adapter_thread_matches(thread_id: &Option<String>, bound_thread_id: &str) -> bool {
    thread_id.as_deref() == Some(bound_thread_id)
}

fn wait_for_passive_adapter_retry(running: &AtomicBool, delay: Duration) {
    let deadline = Instant::now() + delay;
    while running.load(Ordering::Acquire) {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            break;
        }
        thread::sleep(remaining.min(Duration::from_millis(50)));
    }
}

fn daemon_argv_for_mutating_command(command: &Commands) -> Result<Option<Vec<OsString>>> {
    let argv = match command {
        Commands::Job {
            command: JobCommand::Submit(args),
        } => {
            let mut argv = vec![OsString::from("job"), OsString::from("submit")];
            push_string_arg(&mut argv, "--source-thread-id", &args.source_thread_id);
            push_string_arg(&mut argv, "--summary", &args.summary);
            if let Some(path) = &args.metadata_file {
                push_path_arg(&mut argv, "--metadata-file", &absolute_cli_path(path)?);
            }
            push_optional_bool_arg(&mut argv, "--delivery-read-only", args.delivery_read_only);
            push_optional_bool_arg(
                &mut argv,
                "--delivery-requires-approval",
                args.delivery_requires_approval,
            );
            push_optional_bool_arg(
                &mut argv,
                "--delivery-requires-network",
                args.delivery_requires_network,
            );
            push_optional_bool_arg(
                &mut argv,
                "--delivery-requires-write-access",
                args.delivery_requires_write_access,
            );
            argv
        }
        Commands::Job {
            command: JobCommand::Complete(args),
        } => {
            let mut argv = vec![OsString::from("job"), OsString::from("complete")];
            push_string_arg(&mut argv, "--job-id", &args.job_id);
            push_path_arg(
                &mut argv,
                "--result-file",
                &absolute_cli_path(&args.result_file)?,
            );
            push_optional_string_arg(&mut argv, "--summary", args.summary.as_deref());
            push_i64_arg(
                &mut argv,
                "--max-delivery-attempts",
                args.max_delivery_attempts,
            );
            push_i64_arg(
                &mut argv,
                "--redelivery-window-seconds",
                args.redelivery_window_seconds,
            );
            argv
        }
        Commands::Job {
            command: JobCommand::Fail(args),
        } => {
            let mut argv = vec![OsString::from("job"), OsString::from("fail")];
            push_string_arg(&mut argv, "--job-id", &args.job_id);
            push_string_arg(&mut argv, "--reason", &args.reason);
            push_i64_arg(
                &mut argv,
                "--max-delivery-attempts",
                args.max_delivery_attempts,
            );
            push_i64_arg(
                &mut argv,
                "--redelivery-window-seconds",
                args.redelivery_window_seconds,
            );
            argv
        }
        Commands::Batch {
            command: BatchCommand::CloseHead(args),
        } => {
            let mut argv = vec![OsString::from("batch"), OsString::from("close-head")];
            push_string_arg(&mut argv, "--source-thread-id", &args.source_thread_id);
            push_string_arg(&mut argv, "--reason", args.reason.cli_value());
            push_optional_string_arg(&mut argv, "--note", args.note.as_deref());
            argv
        }
        Commands::Attempt {
            command: AttemptCommand::BeginCliAccept(args),
        } => {
            let mut argv = vec![
                OsString::from("attempt"),
                OsString::from("begin-cli-accept"),
            ];
            push_string_arg(&mut argv, "--batch-id", &args.batch_id);
            push_string_arg(&mut argv, "--managed-session-id", &args.managed_session_id);
            push_i64_arg(&mut argv, "--session-epoch", args.session_epoch);
            push_string_arg(&mut argv, "--rpc-kind", args.rpc_kind.cli_value());
            push_string_arg(&mut argv, "--rpc-request-id", &args.rpc_request_id);
            push_optional_string_arg(
                &mut argv,
                "--rpc-correlation-marker",
                args.rpc_correlation_marker.as_deref(),
            );
            if let Some(now) = args.now {
                push_i64_arg(&mut argv, "--now", now);
            }
            argv
        }
        Commands::Attempt {
            command: AttemptCommand::AcceptCli(args),
        } => {
            let mut argv = vec![OsString::from("attempt"), OsString::from("accept-cli")];
            push_string_arg(&mut argv, "--attempt-id", &args.attempt_id);
            push_string_arg(&mut argv, "--delivery-turn-id", &args.delivery_turn_id);
            push_i64_arg(
                &mut argv,
                "--observation-window-seconds",
                args.observation_window_seconds,
            );
            if let Some(now) = args.now {
                push_i64_arg(&mut argv, "--now", now);
            }
            argv
        }
        Commands::Attempt {
            command: AttemptCommand::ObserveCliTurn(args),
        } => {
            let mut argv = vec![
                OsString::from("attempt"),
                OsString::from("observe-cli-turn"),
            ];
            push_string_arg(&mut argv, "--attempt-id", &args.attempt_id);
            push_string_arg(&mut argv, "--delivery-turn-id", &args.delivery_turn_id);
            push_string_arg(&mut argv, "--turn-event", args.turn_event.cli_value());
            if let Some(now) = args.now {
                push_i64_arg(&mut argv, "--now", now);
            }
            argv
        }
        Commands::Cli {
            command:
                CliCommand::Session {
                    command: CliSessionCommand::Bind(args),
                },
        } => {
            let mut argv = vec![
                OsString::from("cli"),
                OsString::from("session"),
                OsString::from("bind"),
            ];
            push_string_arg(&mut argv, "--bound-thread-id", &args.bound_thread_id);
            push_bool_arg(
                &mut argv,
                "--session-allows-approval",
                args.session_allows_approval,
            );
            push_bool_arg(
                &mut argv,
                "--session-allows-network",
                args.session_allows_network,
            );
            push_bool_arg(
                &mut argv,
                "--session-allows-write-access",
                args.session_allows_write_access,
            );
            if let Some(now) = args.now {
                push_i64_arg(&mut argv, "--now", now);
            }
            argv
        }
        Commands::Cli {
            command:
                CliCommand::Session {
                    command: CliSessionCommand::NoteActivity(args),
                },
        } => {
            let mut argv = vec![
                OsString::from("cli"),
                OsString::from("session"),
                OsString::from("note-activity"),
            ];
            push_string_arg(&mut argv, "--managed-session-id", &args.managed_session_id);
            push_i64_arg(&mut argv, "--session-epoch", args.session_epoch);
            push_string_arg(
                &mut argv,
                "--activity-state",
                args.activity_state.cli_value(),
            );
            push_i64_arg(&mut argv, "--activity-revision", args.activity_revision);
            if let Some(now) = args.now {
                push_i64_arg(&mut argv, "--now", now);
            }
            argv
        }
        Commands::Cli {
            command:
                CliCommand::Session {
                    command: CliSessionCommand::NoteCapabilities(args),
                },
        } => {
            let mut argv = vec![
                OsString::from("cli"),
                OsString::from("session"),
                OsString::from("note-capabilities"),
            ];
            push_string_arg(&mut argv, "--managed-session-id", &args.managed_session_id);
            push_i64_arg(&mut argv, "--session-epoch", args.session_epoch);
            push_i64_arg(&mut argv, "--capability-revision", args.capability_revision);
            push_bool_arg(&mut argv, "--thread-resume", args.thread_resume);
            push_bool_arg(&mut argv, "--turn-start", args.turn_start);
            push_bool_arg(&mut argv, "--current-state-sync", args.current_state_sync);
            push_bool_arg(
                &mut argv,
                "--turn-completed-event",
                args.turn_completed_event,
            );
            push_bool_arg(
                &mut argv,
                "--negative-terminal-events",
                args.negative_terminal_events,
            );
            push_bool_arg(&mut argv, "--thread-start", args.thread_start);
            push_bool_arg(&mut argv, "--turn-steer", args.turn_steer);
            if let Some(now) = args.now {
                push_i64_arg(&mut argv, "--now", now);
            }
            argv
        }
        Commands::Cli {
            command:
                CliCommand::Session {
                    command: CliSessionCommand::InvalidateProof(args),
                },
        } => {
            let mut argv = vec![
                OsString::from("cli"),
                OsString::from("session"),
                OsString::from("invalidate-proof"),
            ];
            push_string_arg(&mut argv, "--managed-session-id", &args.managed_session_id);
            push_i64_arg(&mut argv, "--session-epoch", args.session_epoch);
            if let Some(now) = args.now {
                push_i64_arg(&mut argv, "--now", now);
            }
            argv
        }
        Commands::Maintenance {
            command: MaintenanceCommand::Sweep(args),
        } => {
            let mut argv = vec![OsString::from("maintenance"), OsString::from("sweep")];
            if let Some(now) = args.now {
                push_i64_arg(&mut argv, "--now", now);
            }
            argv
        }
        Commands::Job {
            command: JobCommand::Inspect(_) | JobCommand::List(_),
        }
        | Commands::Batch {
            command: BatchCommand::InspectHead(_) | BatchCommand::Inspect(_),
        }
        | Commands::Attempt {
            command: AttemptCommand::Inspect(_),
        }
        | Commands::Cli {
            command: CliCommand::Run(_),
        }
        | Commands::Cli {
            command:
                CliCommand::Session {
                    command: CliSessionCommand::Inspect(_),
                },
        }
        | Commands::Daemon { .. } => return Ok(None),
    };
    Ok(Some(argv))
}

fn daemon_startup_sweep_for_command(command: &Commands) -> Result<Option<i64>> {
    match command {
        Commands::Maintenance {
            command: MaintenanceCommand::Sweep(_),
        } => Ok(None),
        _ => Ok(Some(now_epoch_seconds()?)),
    }
}

fn require_direct_store_env() -> Result<()> {
    match env::var(DIRECT_STORE_ENV) {
        Ok(value) if value == "1" => Ok(()),
        _ => bail!("--direct-store requires {DIRECT_STORE_ENV}=1"),
    }
}

fn argv_payload(argv: Vec<OsString>) -> Vec<Vec<u8>> {
    argv.into_iter().map(OsString::into_vec).collect()
}

fn push_optional_bool_arg(argv: &mut Vec<OsString>, flag: &str, value: Option<bool>) {
    if let Some(value) = value {
        push_bool_arg(argv, flag, value);
    }
}

fn push_bool_arg(argv: &mut Vec<OsString>, flag: &str, value: bool) {
    push_string_arg(argv, flag, &value.to_string());
}

fn push_optional_string_arg(argv: &mut Vec<OsString>, flag: &str, value: Option<&str>) {
    if let Some(value) = value {
        push_string_arg(argv, flag, value);
    }
}

fn push_i64_arg(argv: &mut Vec<OsString>, flag: &str, value: i64) {
    push_string_arg(argv, flag, &value.to_string());
}

fn push_string_arg(argv: &mut Vec<OsString>, flag: &str, value: &str) {
    argv.push(OsString::from(format!("{flag}={value}")));
}

fn push_path_arg(argv: &mut Vec<OsString>, flag: &str, value: &Path) {
    let mut bytes = Vec::with_capacity(flag.len() + 1 + value.as_os_str().as_bytes().len());
    bytes.extend_from_slice(flag.as_bytes());
    bytes.push(b'=');
    bytes.extend_from_slice(value.as_os_str().as_bytes());
    argv.push(OsString::from_vec(bytes));
}

fn absolute_cli_path(path: &Path) -> Result<PathBuf> {
    if path.is_absolute() {
        Ok(path.to_path_buf())
    } else {
        Ok(env::current_dir()?.join(path))
    }
}

fn resolve_executable(binary: &OsStr) -> Result<OsString> {
    if binary.as_bytes().contains(&b'/') {
        return Ok(absolute_cli_path(Path::new(binary))?.into_os_string());
    }
    let path = env::var_os("PATH").context("PATH is unset; pass --codex-bin <path>")?;
    for directory in env::split_paths(&path) {
        let candidate = directory.join(binary);
        if executable_file_exists(&candidate) {
            return Ok(absolute_cli_path(&candidate)?.into_os_string());
        }
    }
    bail!(
        "could not find executable {:?} on PATH; pass --codex-bin <path>",
        binary
    )
}

fn executable_file_exists(path: &Path) -> bool {
    fs::metadata(path)
        .is_ok_and(|metadata| metadata.is_file() && metadata.permissions().mode() & 0o111 != 0)
}

fn dispatch_job(command: JobCommand, layout: &FsLayout) -> Result<Value> {
    let mut store = Store::open(layout)?;
    match command {
        JobCommand::Submit(args) => {
            let (metadata_json, mut policy) = load_submit_metadata(args.metadata_file.as_deref())?;
            apply_cli_policy_overrides(
                &mut policy,
                PartialDeliveryPolicy {
                    delivery_read_only: args.delivery_read_only,
                    delivery_requires_approval: args.delivery_requires_approval,
                    delivery_requires_network: args.delivery_requires_network,
                    delivery_requires_write_access: args.delivery_requires_write_access,
                },
            );
            let now = now_epoch_seconds()?;
            let job = store.submit_job(NewJob {
                job_id: new_id(),
                source_thread_id: args.source_thread_id,
                summary: args.summary,
                metadata_json,
                policy,
                created_at: now,
            })?;
            Ok(json!({ "job": job }))
        }
        JobCommand::Complete(args) => {
            validate_positive("max_delivery_attempts", args.max_delivery_attempts)?;
            validate_positive("redelivery_window_seconds", args.redelivery_window_seconds)?;
            let job = store.inspect_job(&args.job_id)?;
            if job.status != "pending" {
                bail!("job {} is already {}", job.job_id, job.status);
            }
            let now = now_epoch_seconds()?;
            validate_timestamp_add(
                now,
                args.redelivery_window_seconds,
                "redelivery_window_seconds",
            )?;
            let artifact_id = new_id();
            store.begin_artifact_ingest(&args.job_id, &artifact_id, now)?;
            let ingested = match ingest_result_file(
                layout,
                &artifact_id,
                &args.job_id,
                &args.result_file,
                now,
            ) {
                Ok(ingested) => ingested,
                Err(error) => {
                    // Keep the ingest row so maintenance can retry cleanup if a partial
                    // artifact directory was created before the failure.
                    return Err(error);
                }
            };
            match store.complete_job(
                ingested.record,
                args.summary,
                now,
                args.max_delivery_attempts,
                args.redelivery_window_seconds,
            ) {
                Ok(batch) => {
                    remove_ingest_marker_best_effort(&ingested.artifact_dir);
                    Ok(json!({ "batch": batch }))
                }
                Err(error) => {
                    if remove_dir_all_durable(&ingested.artifact_dir).is_ok() {
                        let _ = store.abandon_artifact_ingest(&artifact_id);
                    }
                    Err(error)
                }
            }
        }
        JobCommand::Fail(args) => {
            validate_positive("max_delivery_attempts", args.max_delivery_attempts)?;
            validate_positive("redelivery_window_seconds", args.redelivery_window_seconds)?;
            let now = now_epoch_seconds()?;
            validate_timestamp_add(
                now,
                args.redelivery_window_seconds,
                "redelivery_window_seconds",
            )?;
            let batch = store.fail_job(
                &args.job_id,
                &args.reason,
                now,
                args.max_delivery_attempts,
                args.redelivery_window_seconds,
            )?;
            Ok(json!({ "batch": batch }))
        }
        JobCommand::Inspect(args) => {
            let job = store.inspect_job(&args.job_id)?;
            Ok(json!({ "job": job }))
        }
        JobCommand::List(args) => {
            let jobs = store.list_jobs(
                args.source_thread_id.as_deref(),
                args.status.as_deref(),
                args.limit,
            )?;
            Ok(json!({ "jobs": jobs }))
        }
    }
}

fn dispatch_batch(command: BatchCommand, layout: &FsLayout) -> Result<Value> {
    let mut store = Store::open(layout)?;
    match command {
        BatchCommand::InspectHead(args) => {
            let batch = store.inspect_head(&args.source_thread_id)?;
            Ok(json!({ "batch": batch }))
        }
        BatchCommand::Inspect(args) => {
            let batch = store.inspect_batch(&args.batch_id)?;
            Ok(json!({ "batch": batch }))
        }
        BatchCommand::CloseHead(args) => {
            let now = now_epoch_seconds()?;
            let batch = store.close_head(
                layout,
                &args.source_thread_id,
                args.reason.as_str(),
                args.note.as_deref(),
                now,
            )?;
            Ok(json!({ "batch": batch }))
        }
    }
}

fn dispatch_attempt(command: AttemptCommand, layout: &FsLayout) -> Result<Value> {
    let mut store = Store::open(layout)?;
    match command {
        AttemptCommand::BeginCliAccept(args) => {
            validate_positive("session_epoch", args.session_epoch)?;
            validate_nonempty("rpc_request_id", &args.rpc_request_id)?;
            let now = match args.now {
                Some(value) => value,
                None => now_epoch_seconds()?,
            };
            let marker = args
                .rpc_correlation_marker
                .unwrap_or_else(|| format!("cbth:{}", new_id()));
            let attempt = store.begin_cli_accept_pending_attempt(NewCliAcceptPendingAttempt {
                attempt_id: new_id(),
                batch_id: args.batch_id,
                managed_session_id: args.managed_session_id,
                session_epoch: args.session_epoch,
                delivery_rpc_request_id: args.rpc_request_id,
                delivery_rpc_kind: args.rpc_kind.as_str().to_owned(),
                delivery_rpc_correlation_marker: marker,
                delivery_rpc_started_at: now,
            })?;
            Ok(json!({ "attempt": attempt }))
        }
        AttemptCommand::AcceptCli(args) => {
            validate_nonempty("delivery_turn_id", &args.delivery_turn_id)?;
            validate_positive_max(
                "observation_window_seconds",
                args.observation_window_seconds,
                MAX_CLI_OBSERVATION_WINDOW_SECONDS,
            )?;
            let now = match args.now {
                Some(value) => value,
                None => now_epoch_seconds()?,
            };
            let deadline = validate_timestamp_add(
                now,
                args.observation_window_seconds,
                "observation_window_seconds",
            )?;
            let attempt = store.accept_cli_attempt(
                &args.attempt_id,
                &args.delivery_turn_id,
                now,
                deadline,
            )?;
            Ok(json!({ "attempt": attempt }))
        }
        AttemptCommand::ObserveCliTurn(args) => {
            validate_nonempty("delivery_turn_id", &args.delivery_turn_id)?;
            let now = match args.now {
                Some(value) => value,
                None => now_epoch_seconds()?,
            };
            let attempt = store.observe_cli_turn_event(
                layout,
                &args.attempt_id,
                &args.delivery_turn_id,
                args.turn_event.as_str(),
                now,
            )?;
            Ok(json!({ "attempt": attempt }))
        }
        AttemptCommand::Inspect(args) => {
            let attempt = store.inspect_attempt(&args.attempt_id)?;
            Ok(json!({ "attempt": attempt }))
        }
    }
}

fn dispatch_cli(command: CliCommand, layout: &FsLayout) -> Result<Value> {
    let mut store = if cli_command_uses_lifecycle_store_timeout(&command) {
        Store::open_for_daemon_lifecycle(layout)?
    } else {
        Store::open(layout)?
    };
    match command {
        CliCommand::Run(_) => bail!("cli run must execute from the foreground client"),
        CliCommand::Session { command } => match command {
            CliSessionCommand::Bind(args) => {
                validate_nonempty("bound_thread_id", &args.bound_thread_id)?;
                let now = match args.now {
                    Some(value) => value,
                    None => now_epoch_seconds()?,
                };
                let session = store.attach_or_create_cli_managed_session(
                    &args.bound_thread_id,
                    CliManagedSessionProfile {
                        session_allows_approval: args.session_allows_approval,
                        session_allows_network: args.session_allows_network,
                        session_allows_write_access: args.session_allows_write_access,
                    },
                    now,
                )?;
                Ok(json!({ "cli_session": session }))
            }
            CliSessionCommand::NoteActivity(args) => {
                validate_positive("session_epoch", args.session_epoch)?;
                validate_positive("activity_revision", args.activity_revision)?;
                let now = match args.now {
                    Some(value) => value,
                    None => now_epoch_seconds()?,
                };
                let session = store.note_cli_managed_session_activity(
                    &args.managed_session_id,
                    args.session_epoch,
                    args.activity_state.as_str(),
                    args.activity_revision,
                    now,
                )?;
                Ok(json!({ "cli_session": session }))
            }
            CliSessionCommand::NoteCapabilities(args) => {
                validate_positive("session_epoch", args.session_epoch)?;
                validate_positive("capability_revision", args.capability_revision)?;
                let now = match args.now {
                    Some(value) => value,
                    None => now_epoch_seconds()?,
                };
                let session = store.note_cli_managed_session_capabilities(
                    &args.managed_session_id,
                    args.session_epoch,
                    args.capability_revision,
                    CliManagedSessionCapabilities {
                        capability_thread_resume: args.thread_resume,
                        capability_turn_start: args.turn_start,
                        capability_current_state_sync: args.current_state_sync,
                        capability_turn_completed_event: args.turn_completed_event,
                        capability_negative_terminal_events: args.negative_terminal_events,
                        capability_thread_start: args.thread_start,
                        capability_turn_steer: args.turn_steer,
                    },
                    now,
                )?;
                Ok(json!({ "cli_session": session }))
            }
            CliSessionCommand::InvalidateProof(args) => {
                validate_positive("session_epoch", args.session_epoch)?;
                let now = match args.now {
                    Some(value) => value,
                    None => now_epoch_seconds()?,
                };
                let invalidation = store.invalidate_cli_managed_session_proof(
                    &args.managed_session_id,
                    args.session_epoch,
                    now,
                )?;
                Ok(json!({ "cli_session": invalidation.session }))
            }
            CliSessionCommand::Inspect(args) => {
                let session = store.inspect_cli_managed_session(&args.managed_session_id)?;
                Ok(json!({ "cli_session": session }))
            }
        },
    }
}

fn cli_command_uses_lifecycle_store_timeout(command: &CliCommand) -> bool {
    matches!(
        command,
        CliCommand::Session {
            command: CliSessionCommand::NoteActivity(_)
                | CliSessionCommand::NoteCapabilities(_)
                | CliSessionCommand::InvalidateProof(_),
        }
    )
}

fn dispatch_maintenance(command: MaintenanceCommand, layout: &FsLayout) -> Result<Value> {
    let mut store = Store::open(layout)?;
    match command {
        MaintenanceCommand::Sweep(args) => {
            let now = match args.now {
                Some(value) => value,
                None => now_epoch_seconds()?,
            };
            let report = store.sweep(layout, now)?;
            Ok(json!({ "sweep": report }))
        }
    }
}

fn dispatch_daemon(command: DaemonCommand, layout: &FsLayout) -> Result<Value> {
    match command {
        DaemonCommand::Serve(args) => {
            validate_nonzero_u64("idle_timeout_seconds", args.idle_timeout_seconds)?;
            if args.skip_startup_sweep && args.now.is_some() {
                bail!("--skip-startup-sweep cannot be combined with --now");
            }
            let startup_sweep_now = if args.skip_startup_sweep {
                None
            } else {
                Some(match args.now {
                    Some(value) => value,
                    None => now_epoch_seconds()?,
                })
            };
            daemon_serve(
                layout,
                DaemonServeOptions {
                    idle_timeout_seconds: args.idle_timeout_seconds,
                    startup_sweep_now,
                },
            )
        }
        DaemonCommand::Ensure(args) => {
            validate_nonzero_u64("idle_timeout_seconds", args.idle_timeout_seconds)?;
            validate_nonzero_u64("startup_timeout_seconds", args.startup_timeout_seconds)?;
            daemon_ensure(
                layout,
                DaemonEnsureOptions {
                    idle_timeout_seconds: args.idle_timeout_seconds,
                    startup_timeout_seconds: args.startup_timeout_seconds,
                    startup_sweep_now: Some(now_epoch_seconds()?),
                },
            )
        }
        DaemonCommand::Ping => daemon_request(layout, "ping"),
        DaemonCommand::Status => daemon_request(layout, "status"),
        DaemonCommand::Stop => daemon_request(layout, "stop"),
    }
}

fn load_submit_metadata(path: Option<&Path>) -> Result<(String, DeliveryPolicy)> {
    let Some(path) = path else {
        return Ok(("{}".to_owned(), DeliveryPolicy::fail_closed()));
    };

    let metadata = fs::metadata(path).with_context(|| format!("stat {}", path.display()))?;
    if !metadata.is_file() {
        bail!("metadata file must be a regular file: {}", path.display());
    }
    if metadata.len() > MAX_METADATA_BYTES {
        bail!("metadata file is too large; max {MAX_METADATA_BYTES} bytes");
    }

    let file = fs::File::open(path).with_context(|| format!("open {}", path.display()))?;
    let mut bytes = Vec::with_capacity(
        metadata
            .len()
            .min(MAX_METADATA_BYTES.saturating_add(1))
            .try_into()
            .expect("metadata capacity fits usize"),
    );
    let mut limited = file.take(MAX_METADATA_BYTES.saturating_add(1));
    limited
        .read_to_end(&mut bytes)
        .with_context(|| format!("read {}", path.display()))?;
    if bytes.len() as u64 > MAX_METADATA_BYTES {
        bail!("metadata file is too large; max {MAX_METADATA_BYTES} bytes");
    }
    let content = String::from_utf8(bytes)
        .with_context(|| format!("read UTF-8 metadata {}", path.display()))?;
    let parsed: SubmitMetadata =
        serde_json::from_str(&content).with_context(|| format!("parse {}", path.display()))?;
    let mut policy = DeliveryPolicy::fail_closed();
    if let Some(partial) = parsed.delivery_policy {
        partial.apply_to(&mut policy);
    }
    Ok((serde_json::to_string(&parsed.extra)?, policy))
}

fn apply_cli_policy_overrides(policy: &mut DeliveryPolicy, overrides: PartialDeliveryPolicy) {
    overrides.apply_to(policy);
}

fn validate_positive(name: &str, value: i64) -> Result<()> {
    if value > 0 {
        Ok(())
    } else {
        bail!("{name} must be positive")
    }
}

fn validate_positive_max(name: &str, value: i64, max: i64) -> Result<()> {
    validate_positive(name, value)?;
    if value <= max {
        Ok(())
    } else {
        bail!("{name} must be <= {max}")
    }
}

fn validate_nonzero_u64(name: &str, value: u64) -> Result<()> {
    if value > 0 {
        Ok(())
    } else {
        bail!("{name} must be positive")
    }
}

fn validate_nonempty(name: &str, value: &str) -> Result<()> {
    if value.is_empty() {
        bail!("{name} must not be empty")
    }
    Ok(())
}

fn json_string(value: &Value, field: &str) -> Result<String> {
    value[field]
        .as_str()
        .map(str::to_owned)
        .ok_or_else(|| anyhow::anyhow!("missing string field {field}"))
}

fn json_i64(value: &Value, field: &str) -> Result<i64> {
    value[field]
        .as_i64()
        .ok_or_else(|| anyhow::anyhow!("missing integer field {field}"))
}

fn validate_timestamp_add(now: i64, delta: i64, name: &str) -> Result<i64> {
    now.checked_add(delta)
        .ok_or_else(|| anyhow::anyhow!("{name} overflows timestamp range"))
}

fn now_epoch_seconds() -> Result<i64> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock is before unix epoch")?;
    i64::try_from(duration.as_secs()).context("epoch seconds overflow")
}

fn write_json<T: Serialize>(value: &T) -> Result<()> {
    let stdout = io::stdout();
    let mut lock = stdout.lock();
    serde_json::to_writer_pretty(&mut lock, value)?;
    lock.write_all(b"\n")?;
    Ok(())
}
