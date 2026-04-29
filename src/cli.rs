use std::env;
use std::ffi::OsString;
use std::fs;
use std::io::{self, Read, Write};
use std::os::unix::ffi::{OsStrExt, OsStringExt};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail};
use clap::{Args, Parser, Subcommand, ValueEnum};
use serde::Serialize;
use serde_json::{Value, json};

use crate::artifact::{ingest_result_file, remove_ingest_marker_best_effort};
use crate::daemon::{
    DaemonEnsureOptions, DaemonServeOptions, daemon_ensure, daemon_request, daemon_request_payload,
    daemon_serve, validate_daemon_autostart_endpoint,
};
use crate::fs_layout::{FsLayout, remove_dir_all_durable};
use crate::models::{
    CliManagedSessionProfile, DEFAULT_MAX_DELIVERY_ATTEMPTS, DEFAULT_REDELIVERY_WINDOW_SECONDS,
    DeliveryPolicy, NewCliAcceptPendingAttempt, NewJob, PartialDeliveryPolicy, SubmitMetadata,
};
use crate::store::{Store, new_id};

const MAX_METADATA_BYTES: u64 = 1024 * 1024;
const DIRECT_STORE_ENV: &str = "CBTH_ALLOW_DIRECT_STORE";
const DEFAULT_DAEMON_IDLE_TIMEOUT_SECONDS: u64 = 300;
const DEFAULT_DAEMON_STARTUP_TIMEOUT_SECONDS: u64 = 5;
const MAX_CLI_OBSERVATION_WINDOW_SECONDS: i64 = 6 * 60 * 60;

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
    #[command(hide = true)]
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
    Session {
        #[command(subcommand)]
        command: CliSessionCommand,
    },
}

#[derive(Debug, Subcommand)]
enum CliSessionCommand {
    Bind(CliSessionBindArgs),
    NoteActivity(CliSessionNoteActivityArgs),
    Inspect(CliSessionInspectArgs),
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

#[derive(Clone, Debug, ValueEnum)]
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
    let output = dispatch(
        cli.command,
        &layout,
        DispatchMode::Client {
            direct_store: cli.direct_store,
            startup_timeout_seconds: cli.auto_daemon_startup_timeout_seconds,
        },
    )?;
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
    let mut store = Store::open(layout)?;
    match command {
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
            CliSessionCommand::Inspect(args) => {
                let session = store.inspect_cli_managed_session(&args.managed_session_id)?;
                Ok(json!({ "cli_session": session }))
            }
        },
    }
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
