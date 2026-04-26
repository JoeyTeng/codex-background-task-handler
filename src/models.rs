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

#[derive(Clone, Debug, Default, Serialize)]
pub struct SweepReport {
    pub expired_manual_batches_closed: usize,
    pub expired_automatic_batches_closed: usize,
    pub artifacts_deleted: usize,
    pub artifact_delete_failures: usize,
    pub orphan_artifacts_deleted: usize,
    pub orphan_artifact_delete_failures: usize,
    pub artifact_manifests_synced: usize,
    pub artifact_manifest_sync_failures: usize,
}
