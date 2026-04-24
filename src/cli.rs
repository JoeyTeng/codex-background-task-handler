use std::fs;
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail};
use clap::{Args, Parser, Subcommand, ValueEnum};
use serde::Serialize;
use serde_json::{Value, json};

use crate::artifact::{ingest_result_file, remove_ingest_marker_best_effort};
use crate::fs_layout::{FsLayout, remove_dir_all_durable};
use crate::models::{
    DEFAULT_MAX_DELIVERY_ATTEMPTS, DEFAULT_REDELIVERY_WINDOW_SECONDS, DeliveryPolicy, NewJob,
    PartialDeliveryPolicy, SubmitMetadata,
};
use crate::store::{Store, new_id};

const MAX_METADATA_BYTES: u64 = 1024 * 1024;

#[derive(Debug, Parser)]
#[command(name = "cbth")]
#[command(about = "Codex background task handler")]
pub struct Cli {
    #[arg(long, global = true)]
    home: Option<PathBuf>,

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
    Maintenance {
        #[command(subcommand)]
        command: MaintenanceCommand,
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

pub fn run() -> Result<()> {
    let cli = Cli::parse();
    let layout = FsLayout::resolve(cli.home)?;
    let output = dispatch(cli.command, &layout)?;
    write_json(&output)
}

fn dispatch(command: Commands, layout: &FsLayout) -> Result<Value> {
    match command {
        Commands::Job { command } => dispatch_job(command, layout),
        Commands::Batch { command } => dispatch_batch(command, layout),
        Commands::Maintenance { command } => dispatch_maintenance(command, layout),
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

fn validate_timestamp_add(now: i64, delta: i64, name: &str) -> Result<()> {
    now.checked_add(delta)
        .map(|_| ())
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
