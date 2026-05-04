use serde::{Deserialize, Serialize};
use serde_json::Value;

pub const DEFAULT_MAX_DELIVERY_ATTEMPTS: i64 = 3;
pub const DEFAULT_REDELIVERY_WINDOW_SECONDS: i64 = 24 * 60 * 60;
pub const MIN_ARTIFACT_TTL_SECONDS: i64 = 24 * 60 * 60;
pub const POST_CLOSE_ARTIFACT_TTL_SECONDS: i64 = 72 * 60 * 60;
pub const ORPHAN_ARTIFACT_GRACE_SECONDS: i64 = 60 * 60;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct DeliveryPolicy {
    pub delivery_read_only: bool,
    pub delivery_requires_approval: bool,
    pub delivery_requires_network: bool,
    pub delivery_requires_write_access: bool,
}

impl DeliveryPolicy {
    pub fn fail_closed() -> Self {
        Self {
            delivery_read_only: false,
            delivery_requires_approval: true,
            delivery_requires_network: true,
            delivery_requires_write_access: true,
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
pub struct SubmitMetadata {
    #[serde(default)]
    pub delivery_policy: Option<PartialDeliveryPolicy>,
    #[serde(flatten)]
    pub extra: Value,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PartialDeliveryPolicy {
    #[serde(alias = "read_only")]
    pub delivery_read_only: Option<bool>,
    #[serde(alias = "requires_approval")]
    pub delivery_requires_approval: Option<bool>,
    #[serde(alias = "requires_network")]
    pub delivery_requires_network: Option<bool>,
    #[serde(alias = "requires_write_access")]
    pub delivery_requires_write_access: Option<bool>,
}

impl PartialDeliveryPolicy {
    pub fn apply_to(self, policy: &mut DeliveryPolicy) {
        if let Some(value) = self.delivery_read_only {
            policy.delivery_read_only = value;
        }
        if let Some(value) = self.delivery_requires_approval {
            policy.delivery_requires_approval = value;
        }
        if let Some(value) = self.delivery_requires_network {
            policy.delivery_requires_network = value;
        }
        if let Some(value) = self.delivery_requires_write_access {
            policy.delivery_requires_write_access = value;
        }
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct JobRecord {
    pub job_id: String,
    pub source_thread_id: String,
    pub status: String,
    pub summary: String,
    pub metadata: Value,
    pub created_at: i64,
    pub updated_at: i64,
    pub completed_at: Option<i64>,
    pub failed_at: Option<i64>,
    pub result_artifact_id: Option<String>,
    pub failure_reason: Option<String>,
    pub delivery_policy: DeliveryPolicy,
}

#[derive(Clone, Debug)]
pub struct NewJob {
    pub job_id: String,
    pub source_thread_id: String,
    pub summary: String,
    pub metadata_json: String,
    pub policy: DeliveryPolicy,
    pub created_at: i64,
}

#[derive(Clone, Debug, Serialize)]
pub struct TaskRecord {
    pub task_id: String,
    pub job_id: String,
    pub source_thread_id: String,
    pub status: String,
    pub summary: String,
    pub command: Value,
    pub cwd: String,
    pub timeout_seconds: Option<i64>,
    pub max_delivery_attempts: i64,
    pub redelivery_window_seconds: i64,
    pub pid: Option<i64>,
    pub pid_identity: Option<String>,
    pub created_at: i64,
    pub started_at: Option<i64>,
    pub completed_at: Option<i64>,
    pub exit_code: Option<i64>,
    pub signal: Option<i64>,
    pub failure_reason: Option<String>,
    pub stdout_log_path: Option<String>,
    pub stderr_log_path: Option<String>,
    pub stdout_bytes: i64,
    pub stderr_bytes: i64,
    pub stdout_truncated: bool,
    pub stderr_truncated: bool,
    pub cancel_requested_at: Option<i64>,
}

#[derive(Clone, Debug)]
pub struct NewTask {
    pub task_id: String,
    pub job_id: String,
    pub source_thread_id: String,
    pub summary: String,
    pub command_json: String,
    pub cwd: String,
    pub timeout_seconds: Option<i64>,
    pub max_delivery_attempts: i64,
    pub redelivery_window_seconds: i64,
    pub created_at: i64,
}

#[derive(Clone, Debug)]
pub struct LostPendingTaskProcess {
    pub task_id: String,
    pub pid: u32,
    pub pid_identity: Option<String>,
}

#[derive(Clone, Debug, Serialize)]
pub struct ArtifactRecord {
    pub artifact_id: String,
    pub job_id: String,
    pub relative_path: String,
    pub original_filename: Option<String>,
    pub size_bytes: i64,
    pub sha256: String,
    pub created_at: i64,
    pub retention_until: i64,
    pub manifest_synced_retention_until: i64,
    pub manifest_sync_attempted_at: i64,
    pub gc_attempted_at: i64,
}

#[derive(Clone, Debug)]
pub struct NewArtifact {
    pub artifact_id: String,
    pub job_id: String,
    pub relative_path: String,
    pub original_filename: Option<String>,
    pub size_bytes: i64,
    pub sha256: String,
    pub created_at: i64,
    pub retention_until: i64,
}

#[derive(Clone, Debug, Serialize)]
pub struct BatchRecord {
    pub batch_id: String,
    pub source_thread_id: String,
    pub state: String,
    pub replay_policy: String,
    pub close_reason: Option<String>,
    pub close_note: Option<String>,
    pub summary: String,
    pub created_at: i64,
    pub updated_at: i64,
    pub closed_at: Option<i64>,
    pub redelivery_window_ends_at: i64,
    pub max_delivery_attempts: i64,
    pub delivery_attempt_count: i64,
    pub delivery_policy: DeliveryPolicy,
    pub inline_payload_bytes: i64,
    pub requires_artifact_read: bool,
}

#[derive(Clone, Debug)]
pub struct NewBatch {
    pub batch_id: String,
    pub source_thread_id: String,
    pub summary: String,
    pub created_at: i64,
    pub redelivery_window_ends_at: i64,
    pub max_delivery_attempts: i64,
    pub policy: DeliveryPolicy,
    pub inline_payload_bytes: i64,
    pub requires_artifact_read: bool,
}

#[derive(Clone, Debug, Serialize)]
pub struct BatchJobRecord {
    pub job: JobRecord,
    pub artifact: Option<ArtifactRecord>,
}

#[derive(Clone, Debug, Serialize)]
pub struct BatchInspect {
    pub batch: BatchRecord,
    pub jobs: Vec<BatchJobRecord>,
}

#[derive(Clone, Debug, Serialize)]
pub struct DeliveryAttemptRecord {
    pub attempt_id: String,
    pub batch_id: String,
    pub source_thread_id: String,
    pub adapter_kind: String,
    pub authorization_mode: String,
    pub state: String,
    pub generation: i64,
    pub delivery_rpc_request_id: Option<String>,
    pub delivery_rpc_kind: Option<String>,
    pub delivery_rpc_state: Option<String>,
    pub delivery_rpc_correlation_marker: Option<String>,
    pub delivery_rpc_started_at: Option<i64>,
    pub managed_session_id: Option<String>,
    pub session_epoch: Option<i64>,
    pub session_activity_revision: i64,
    pub session_capability_revision: i64,
    pub delivery_turn_id: Option<String>,
    pub delivery_accepted_at: Option<i64>,
    pub delivery_observation_state: Option<String>,
    pub delivery_observation_deadline: Option<i64>,
    pub last_observed_turn_event: Option<String>,
    pub last_observed_turn_event_at: Option<i64>,
    pub created_at: i64,
    pub updated_at: i64,
    pub abandoned_at: Option<i64>,
    pub closed_at: Option<i64>,
}

#[derive(Clone, Debug, Serialize)]
pub struct CliManagedSessionRecord {
    pub managed_session_id: String,
    pub bound_thread_id: String,
    pub session_epoch: i64,
    pub session_state: String,
    pub activity_state: String,
    pub activity_revision: i64,
    pub capability_revision: i64,
    pub capability_thread_resume: bool,
    pub capability_turn_start: bool,
    pub capability_current_state_sync: bool,
    pub capability_turn_completed_event: bool,
    pub capability_negative_terminal_events: bool,
    pub capability_thread_start: bool,
    pub capability_turn_steer: bool,
    pub session_allows_approval: bool,
    pub session_allows_network: bool,
    pub session_allows_write_access: bool,
    pub created_at: i64,
    pub updated_at: i64,
    pub retired_at: Option<i64>,
}

#[derive(Clone, Debug, Serialize)]
pub struct CliManagedSessionAttach {
    pub outcome: String,
    pub session: CliManagedSessionRecord,
}

#[derive(Clone, Debug, Serialize)]
pub struct CliManagedSessionRetirement {
    pub session: CliManagedSessionRecord,
}

#[derive(Clone, Debug)]
pub struct CliManagedSessionProfile {
    pub session_allows_approval: bool,
    pub session_allows_network: bool,
    pub session_allows_write_access: bool,
}

#[derive(Clone, Debug, Serialize)]
pub struct CliManagedSessionActivityUpdate {
    pub session: CliManagedSessionRecord,
}

#[derive(Clone, Debug)]
pub struct CliManagedSessionCapabilities {
    pub capability_thread_resume: bool,
    pub capability_turn_start: bool,
    pub capability_current_state_sync: bool,
    pub capability_turn_completed_event: bool,
    pub capability_negative_terminal_events: bool,
    pub capability_thread_start: bool,
    pub capability_turn_steer: bool,
}

#[derive(Clone, Debug, Serialize)]
pub struct CliManagedSessionCapabilityUpdate {
    pub session: CliManagedSessionRecord,
}

#[derive(Clone, Debug, Serialize)]
pub struct CliManagedSessionProofInvalidation {
    pub session: CliManagedSessionRecord,
}

#[derive(Clone, Debug)]
pub struct NewCliAcceptPendingAttempt {
    pub attempt_id: String,
    pub batch_id: String,
    pub managed_session_id: String,
    pub session_epoch: i64,
    pub authorization_mode: String,
    pub delivery_rpc_request_id: String,
    pub delivery_rpc_kind: String,
    pub delivery_rpc_correlation_marker: String,
    pub delivery_rpc_started_at: i64,
}

#[derive(Clone, Debug)]
pub struct NewAuditDecision {
    pub audit_id: String,
    pub recorded_at: i64,
    pub source_thread_id: Option<String>,
    pub batch_id: Option<String>,
    pub attempt_id: Option<String>,
    pub managed_session_id: Option<String>,
    pub session_epoch: Option<i64>,
    pub policy_kind: String,
    pub decision: String,
    pub reason: String,
    pub adapter_kind: String,
    pub details: Value,
}

#[derive(Clone, Debug, Serialize)]
pub struct AuditDecisionRecord {
    pub audit_id: String,
    pub recorded_at: i64,
    pub source_thread_id: Option<String>,
    pub batch_id: Option<String>,
    pub attempt_id: Option<String>,
    pub managed_session_id: Option<String>,
    pub session_epoch: Option<i64>,
    pub policy_kind: String,
    pub decision: String,
    pub reason: String,
    pub adapter_kind: String,
    pub details: Value,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct SweepReport {
    pub stale_cli_acceptances_abandoned: usize,
    pub expired_cli_observations_abandoned: usize,
    pub expired_manual_batches_closed: usize,
    pub expired_automatic_batches_closed: usize,
    pub artifacts_deleted: usize,
    pub artifact_delete_failures: usize,
    pub orphan_artifacts_deleted: usize,
    pub orphan_artifact_delete_failures: usize,
    pub artifact_manifests_synced: usize,
    pub artifact_manifest_sync_failures: usize,
    pub task_log_dirs_deleted: usize,
    pub task_log_delete_failures: usize,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct DaemonLifecycleStatus {
    pub active_jobs: i64,
    pub nonterminal_tasks: i64,
    pub active_cli_acceptances: i64,
    pub cli_acceptances_stale_now: i64,
    pub active_cli_observations: i64,
    pub cli_observations_due_now: i64,
    pub open_batches_due_now: i64,
    pub open_batches_due_within_idle: i64,
}

impl DaemonLifecycleStatus {
    pub fn has_due_maintenance(&self) -> bool {
        self.cli_acceptances_stale_now > 0
            || self.cli_observations_due_now > 0
            || self.open_batches_due_now > 0
    }
}
