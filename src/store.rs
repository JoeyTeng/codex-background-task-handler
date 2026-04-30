use std::collections::HashSet;
use std::fs;
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, anyhow, bail};
use rusqlite::{
    Connection, ErrorCode, OptionalExtension, Transaction, TransactionBehavior, params,
};

use crate::artifact::{ingest_marker_path, rewrite_artifact_manifest};
use crate::fs_layout::{
    FsLayout, ensure_private_file_exists, relative_artifact_payload_path, remove_dir_all_durable,
    set_private_file_permissions_if_exists, validate_id_path_component,
};
use crate::models::{
    ArtifactRecord, AuditDecisionRecord, BatchInspect, BatchJobRecord, BatchRecord,
    CliManagedSessionActivityUpdate, CliManagedSessionAttach, CliManagedSessionCapabilities,
    CliManagedSessionCapabilityUpdate, CliManagedSessionProfile,
    CliManagedSessionProofInvalidation, CliManagedSessionRecord, DEFAULT_REDELIVERY_WINDOW_SECONDS,
    DaemonLifecycleStatus, DeliveryAttemptRecord, DeliveryPolicy, JobRecord, NewArtifact,
    NewAuditDecision, NewBatch, NewCliAcceptPendingAttempt, NewJob, ORPHAN_ARTIFACT_GRACE_SECONDS,
    POST_CLOSE_ARTIFACT_TTL_SECONDS, SweepReport,
};

const MAX_STALE_ARTIFACT_INGESTS_PER_SWEEP: i64 = 100;
const MAX_EXPIRED_BATCHES_PER_SWEEP: i64 = 100;
const MAX_DELETABLE_ARTIFACTS_PER_SWEEP: i64 = 100;
const MAX_MANIFEST_SYNCS_PER_SWEEP: i64 = 100;
const CLI_ACCEPT_PENDING_TIMEOUT_SECONDS: i64 = 5 * 60;
const SQLITE_BUSY_TIMEOUT: Duration = Duration::from_secs(30);
const SQLITE_DAEMON_LIFECYCLE_TIMEOUT: Duration = Duration::from_millis(100);
const SQLITE_OPEN_RETRY_INITIAL_DELAY: Duration = Duration::from_millis(25);
const SQLITE_OPEN_RETRY_MAX_DELAY: Duration = Duration::from_millis(500);

pub struct Store {
    conn: Connection,
}

struct ArtifactIngestRecord {
    artifact_id: String,
    relative_path: String,
}

#[derive(Default)]
struct ManifestSyncReport {
    synced: usize,
    failed: usize,
}

impl Store {
    pub fn open(layout: &FsLayout) -> Result<Self> {
        Self::open_with_timeout(layout, SQLITE_BUSY_TIMEOUT)
    }

    pub fn open_for_daemon_lifecycle(layout: &FsLayout) -> Result<Self> {
        Self::open_with_timeout(layout, SQLITE_DAEMON_LIFECYCLE_TIMEOUT)
    }

    fn open_with_timeout(layout: &FsLayout, busy_timeout: Duration) -> Result<Self> {
        let retry_started = SystemTime::now();
        let mut retry_delay = SQLITE_OPEN_RETRY_INITIAL_DELAY;
        loop {
            match Self::open_once(layout, busy_timeout) {
                Ok(store) => return Ok(store),
                Err(error) if is_sqlite_busy_or_locked(&error) => {
                    let elapsed = retry_started.elapsed().unwrap_or_default();
                    if elapsed >= busy_timeout {
                        return Err(error);
                    }
                    let remaining = busy_timeout.saturating_sub(elapsed);
                    thread::sleep(retry_delay.min(remaining));
                    retry_delay = retry_delay
                        .saturating_mul(2)
                        .min(SQLITE_OPEN_RETRY_MAX_DELAY);
                }
                Err(error) => return Err(error),
            }
        }
    }

    fn open_once(layout: &FsLayout, busy_timeout: Duration) -> Result<Self> {
        layout.ensure()?;
        let db_path = layout.db_path();
        ensure_private_file_exists(&db_path)?;
        let conn = Connection::open(&db_path)
            .with_context(|| format!("open sqlite database {}", db_path.display()))?;
        conn.busy_timeout(busy_timeout)?;
        conn.pragma_update(None, "foreign_keys", "ON")
            .context("enable sqlite foreign_keys")?;
        conn.pragma_update(None, "journal_mode", "WAL")
            .context("enable sqlite WAL journal mode")?;
        conn.pragma_update(None, "synchronous", "FULL")
            .context("enable sqlite FULL synchronous mode")?;
        migrate(&conn).context("migrate sqlite schema")?;
        set_private_file_permissions_if_exists(&db_path)?;
        set_private_file_permissions_if_exists(&db_path.with_extension("sqlite3-wal"))?;
        set_private_file_permissions_if_exists(&db_path.with_extension("sqlite3-shm"))?;
        Ok(Self { conn })
    }

    pub fn submit_job(&mut self, job: NewJob) -> Result<JobRecord> {
        let tx = self.conn.transaction()?;
        tx.execute(
            "INSERT INTO jobs (
                job_id, source_thread_id, status, summary, metadata_json,
                created_at, updated_at, delivery_read_only,
                delivery_requires_approval, delivery_requires_network,
                delivery_requires_write_access
            ) VALUES (?, ?, 'pending', ?, ?, ?, ?, ?, ?, ?, ?)",
            params![
                job.job_id,
                job.source_thread_id,
                job.summary,
                job.metadata_json,
                job.created_at,
                job.created_at,
                bool_to_i64(job.policy.delivery_read_only),
                bool_to_i64(job.policy.delivery_requires_approval),
                bool_to_i64(job.policy.delivery_requires_network),
                bool_to_i64(job.policy.delivery_requires_write_access),
            ],
        )?;
        let record = query_job_tx(&tx, &job.job_id)?;
        tx.commit()?;
        Ok(record)
    }

    pub fn complete_job(
        &mut self,
        artifact: NewArtifact,
        summary: Option<String>,
        now: i64,
        max_delivery_attempts: i64,
        redelivery_window_seconds: i64,
    ) -> Result<BatchInspect> {
        let tx = self.conn.transaction()?;
        let job = query_job_tx(&tx, &artifact.job_id)?;
        ensure_job_pending(&job)?;

        tx.execute(
            "INSERT INTO artifacts (
                artifact_id, job_id, relative_path, original_filename,
                size_bytes, sha256, created_at, retention_until,
                manifest_synced_retention_until, manifest_sync_attempted_at,
                gc_attempted_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            params![
                artifact.artifact_id,
                artifact.job_id,
                artifact.relative_path,
                artifact.original_filename,
                artifact.size_bytes,
                artifact.sha256,
                artifact.created_at,
                artifact.retention_until,
                artifact.retention_until,
                0_i64,
                0_i64,
            ],
        )?;

        tx.execute(
            "UPDATE jobs
             SET status = 'completed',
                 updated_at = ?,
                 completed_at = ?,
                 result_artifact_id = ?
             WHERE job_id = ?",
            params![now, now, artifact.artifact_id, artifact.job_id],
        )?;

        let completed_job = query_job_tx(&tx, &artifact.job_id)?;
        let batch_summary = summary.unwrap_or_else(|| completed_job.summary.clone());
        let redelivery_window_ends_at =
            checked_timestamp_add(now, redelivery_window_seconds, "redelivery_window_seconds")?;
        let batch = NewBatch {
            batch_id: new_id(),
            source_thread_id: completed_job.source_thread_id.clone(),
            summary: batch_summary,
            created_at: now,
            redelivery_window_ends_at,
            max_delivery_attempts,
            policy: completed_job.delivery_policy.clone(),
            inline_payload_bytes: 0,
            requires_artifact_read: true,
        };
        insert_batch_tx(&tx, &batch, std::slice::from_ref(&completed_job.job_id))?;
        tx.execute(
            "DELETE FROM artifact_ingests WHERE artifact_id = ?",
            params![artifact.artifact_id],
        )?;
        let inspect = query_batch_inspect_tx(&tx, &batch.batch_id)?;
        tx.commit()?;
        Ok(inspect)
    }

    pub fn begin_artifact_ingest(
        &mut self,
        job_id: &str,
        artifact_id: &str,
        now: i64,
    ) -> Result<()> {
        validate_id_path_component(artifact_id, "artifact_id")?;
        let tx = self.conn.transaction()?;
        let job = query_job_tx(&tx, job_id)?;
        ensure_job_pending(&job)?;
        tx.execute(
            "INSERT INTO artifact_ingests (
                artifact_id, job_id, relative_path, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?)",
            params![
                artifact_id,
                job_id,
                relative_artifact_payload_path(artifact_id),
                now,
                now,
            ],
        )?;
        tx.commit()?;
        Ok(())
    }

    pub fn abandon_artifact_ingest(&mut self, artifact_id: &str) -> Result<()> {
        self.conn.execute(
            "DELETE FROM artifact_ingests WHERE artifact_id = ?",
            params![artifact_id],
        )?;
        Ok(())
    }

    pub fn fail_job(
        &mut self,
        job_id: &str,
        reason: &str,
        now: i64,
        max_delivery_attempts: i64,
        redelivery_window_seconds: i64,
    ) -> Result<BatchInspect> {
        let tx = self.conn.transaction()?;
        let job = query_job_tx(&tx, job_id)?;
        ensure_job_pending(&job)?;

        tx.execute(
            "UPDATE jobs
             SET status = 'failed',
                 updated_at = ?,
                 failed_at = ?,
                 failure_reason = ?
             WHERE job_id = ?",
            params![now, now, reason, job_id],
        )?;
        let failed_job = query_job_tx(&tx, job_id)?;
        let redelivery_window_ends_at =
            checked_timestamp_add(now, redelivery_window_seconds, "redelivery_window_seconds")?;
        let batch = NewBatch {
            batch_id: new_id(),
            source_thread_id: failed_job.source_thread_id.clone(),
            summary: format!("Background job failed: {reason}"),
            created_at: now,
            redelivery_window_ends_at,
            max_delivery_attempts,
            policy: failed_job.delivery_policy.clone(),
            inline_payload_bytes: 0,
            requires_artifact_read: false,
        };
        insert_batch_tx(&tx, &batch, std::slice::from_ref(&failed_job.job_id))?;
        let inspect = query_batch_inspect_tx(&tx, &batch.batch_id)?;
        tx.commit()?;
        Ok(inspect)
    }

    pub fn inspect_job(&self, job_id: &str) -> Result<JobRecord> {
        query_job(&self.conn, job_id)
    }

    pub fn list_jobs(
        &self,
        source_thread_id: Option<&str>,
        status: Option<&str>,
        limit: i64,
    ) -> Result<Vec<JobRecord>> {
        let limit = limit.clamp(1, 500);
        match (source_thread_id, status) {
            (Some(thread_id), Some(status)) => {
                let mut stmt = self.conn.prepare(
                    "SELECT * FROM jobs
                     WHERE source_thread_id = ? AND status = ?
                     ORDER BY created_at DESC, job_id DESC
                     LIMIT ?",
                )?;
                rows_to_jobs(stmt.query(params![thread_id, status, limit])?)
            }
            (Some(thread_id), None) => {
                let mut stmt = self.conn.prepare(
                    "SELECT * FROM jobs
                     WHERE source_thread_id = ?
                     ORDER BY created_at DESC, job_id DESC
                     LIMIT ?",
                )?;
                rows_to_jobs(stmt.query(params![thread_id, limit])?)
            }
            (None, Some(status)) => {
                let mut stmt = self.conn.prepare(
                    "SELECT * FROM jobs
                     WHERE status = ?
                     ORDER BY created_at DESC, job_id DESC
                     LIMIT ?",
                )?;
                rows_to_jobs(stmt.query(params![status, limit])?)
            }
            (None, None) => {
                let mut stmt = self.conn.prepare(
                    "SELECT * FROM jobs
                     ORDER BY created_at DESC, job_id DESC
                     LIMIT ?",
                )?;
                rows_to_jobs(stmt.query(params![limit])?)
            }
        }
    }

    pub fn inspect_batch(&self, batch_id: &str) -> Result<BatchInspect> {
        query_batch_inspect(&self.conn, batch_id)
    }

    pub fn inspect_head(&self, source_thread_id: &str) -> Result<Option<BatchInspect>> {
        let batch_id: Option<String> = self
            .conn
            .query_row(
                "SELECT batch_id FROM batches
                 WHERE source_thread_id = ? AND state = 'open'
                 ORDER BY created_at ASC, batch_id ASC
                 LIMIT 1",
                params![source_thread_id],
                |row| row.get(0),
            )
            .optional()?;
        batch_id
            .map(|id| query_batch_inspect(&self.conn, &id))
            .transpose()
    }

    pub fn attach_or_create_cli_managed_session(
        &mut self,
        bound_thread_id: &str,
        profile: CliManagedSessionProfile,
        now: i64,
    ) -> Result<CliManagedSessionAttach> {
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        if let Some(existing) =
            query_non_retired_cli_managed_session_by_thread_tx(&tx, bound_thread_id)?
        {
            ensure_cli_session_profile_matches(&existing, &profile)?;
            ensure_cli_session_attachable(&existing)?;
            abandon_cli_observations_for_session_epoch_loss_tx(
                &tx,
                &existing.managed_session_id,
                now,
            )?;
            tx.execute(
                "UPDATE cli_managed_sessions
                 SET session_state = 'live',
                     session_epoch = session_epoch + 1,
                     activity_state = 'unknown',
                     activity_revision = 0,
                     capability_revision = 0,
                     capability_thread_resume = 0,
                     capability_turn_start = 0,
                     capability_current_state_sync = 0,
                     capability_turn_completed_event = 0,
                     capability_negative_terminal_events = 0,
                     capability_thread_start = 0,
                     capability_turn_steer = 0,
                     updated_at = ?
                 WHERE managed_session_id = ?",
                params![now, existing.managed_session_id],
            )?;
            let session = query_cli_managed_session_tx(&tx, &existing.managed_session_id)?;
            tx.commit()?;
            return Ok(CliManagedSessionAttach {
                outcome: "attached".to_owned(),
                session,
            });
        }

        let managed_session_id = new_id();
        tx.execute(
            "INSERT INTO cli_managed_sessions (
                managed_session_id, bound_thread_id, session_epoch, session_state,
                activity_state, activity_revision, capability_revision,
                capability_thread_resume, capability_turn_start,
                capability_current_state_sync, capability_turn_completed_event,
                capability_negative_terminal_events, capability_thread_start,
                capability_turn_steer, session_allows_approval, session_allows_network,
                session_allows_write_access, created_at, updated_at
            ) VALUES (?, ?, 1, 'live', 'unknown', 0, 0, 0, 0, 0, 0, 0, 0, 0, ?, ?, ?, ?, ?)",
            params![
                managed_session_id,
                bound_thread_id,
                bool_to_i64(profile.session_allows_approval),
                bool_to_i64(profile.session_allows_network),
                bool_to_i64(profile.session_allows_write_access),
                now,
                now,
            ],
        )?;
        let session = query_cli_managed_session_tx(&tx, &managed_session_id)?;
        tx.commit()?;
        Ok(CliManagedSessionAttach {
            outcome: "created".to_owned(),
            session,
        })
    }

    pub fn inspect_cli_managed_session(
        &self,
        managed_session_id: &str,
    ) -> Result<CliManagedSessionRecord> {
        query_cli_managed_session(&self.conn, managed_session_id)
    }

    pub fn note_cli_managed_session_activity(
        &mut self,
        managed_session_id: &str,
        session_epoch: i64,
        activity_state: &str,
        activity_revision: i64,
        now: i64,
    ) -> Result<CliManagedSessionActivityUpdate> {
        ensure_cli_session_activity_value(activity_state)?;
        ensure_positive_value("activity_revision", activity_revision)?;
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let session = query_cli_managed_session_tx(&tx, managed_session_id)?;
        ensure_cli_session_epoch_matches(&session, session_epoch)?;
        ensure_cli_session_attachable(&session)?;
        ensure_cli_session_activity_revision_can_advance(
            &session,
            activity_state,
            activity_revision,
        )?;
        if session.activity_revision == activity_revision {
            tx.commit()?;
            return Ok(CliManagedSessionActivityUpdate { session });
        }
        tx.execute(
            "UPDATE cli_managed_sessions
             SET activity_state = ?,
                 activity_revision = ?,
                 updated_at = ?
             WHERE managed_session_id = ?",
            params![activity_state, activity_revision, now, managed_session_id],
        )?;
        let session = query_cli_managed_session_tx(&tx, managed_session_id)?;
        tx.commit()?;
        Ok(CliManagedSessionActivityUpdate { session })
    }

    pub fn note_cli_managed_session_capabilities(
        &mut self,
        managed_session_id: &str,
        session_epoch: i64,
        capability_revision: i64,
        capabilities: CliManagedSessionCapabilities,
        now: i64,
    ) -> Result<CliManagedSessionCapabilityUpdate> {
        ensure_positive_value("capability_revision", capability_revision)?;
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let session = query_cli_managed_session_tx(&tx, managed_session_id)?;
        ensure_cli_session_epoch_matches(&session, session_epoch)?;
        ensure_cli_session_attachable(&session)?;
        ensure_cli_session_capability_revision_can_advance(
            &session,
            &capabilities,
            capability_revision,
        )?;
        if session.capability_revision == capability_revision {
            tx.commit()?;
            return Ok(CliManagedSessionCapabilityUpdate { session });
        }
        tx.execute(
            "UPDATE cli_managed_sessions
             SET capability_revision = ?,
                 capability_thread_resume = ?,
                 capability_turn_start = ?,
                 capability_current_state_sync = ?,
                 capability_turn_completed_event = ?,
                 capability_negative_terminal_events = ?,
                 capability_thread_start = ?,
                 capability_turn_steer = ?,
                 updated_at = ?
             WHERE managed_session_id = ?",
            params![
                capability_revision,
                bool_to_i64(capabilities.capability_thread_resume),
                bool_to_i64(capabilities.capability_turn_start),
                bool_to_i64(capabilities.capability_current_state_sync),
                bool_to_i64(capabilities.capability_turn_completed_event),
                bool_to_i64(capabilities.capability_negative_terminal_events),
                bool_to_i64(capabilities.capability_thread_start),
                bool_to_i64(capabilities.capability_turn_steer),
                now,
                managed_session_id,
            ],
        )?;
        let session = query_cli_managed_session_tx(&tx, managed_session_id)?;
        tx.commit()?;
        Ok(CliManagedSessionCapabilityUpdate { session })
    }

    pub fn invalidate_cli_managed_session_proof(
        &mut self,
        managed_session_id: &str,
        session_epoch: i64,
        now: i64,
    ) -> Result<CliManagedSessionProofInvalidation> {
        ensure_positive_value("session_epoch", session_epoch)?;
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let session = query_cli_managed_session_tx(&tx, managed_session_id)?;
        ensure_cli_session_attachable(&session)?;
        if session.session_epoch != session_epoch {
            if session.session_epoch > session_epoch && cli_session_proof_is_clear(&session) {
                tx.commit()?;
                return Ok(CliManagedSessionProofInvalidation { session });
            }
            ensure_cli_session_epoch_matches(&session, session_epoch)?;
        }
        abandon_cli_observations_for_session_epoch_loss_tx(&tx, managed_session_id, now)?;
        invalidate_cli_managed_session_activity_tx(
            &tx,
            Some(managed_session_id),
            Some(session_epoch),
            now,
        )?;
        let session = query_cli_managed_session_tx(&tx, managed_session_id)?;
        tx.commit()?;
        Ok(CliManagedSessionProofInvalidation { session })
    }

    pub fn invalidate_cli_managed_session_current_proof(
        &mut self,
        managed_session_id: &str,
        bound_thread_id: &str,
        now: i64,
    ) -> Result<CliManagedSessionProofInvalidation> {
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let session = query_cli_managed_session_tx(&tx, managed_session_id)?;
        ensure_cli_session_attachable(&session)?;
        if session.bound_thread_id != bound_thread_id {
            bail!(
                "CLI managed session {} is bound to {}, not {}",
                managed_session_id,
                session.bound_thread_id,
                bound_thread_id
            );
        }
        let session_epoch = session.session_epoch;
        abandon_cli_observations_for_session_epoch_loss_tx(&tx, managed_session_id, now)?;
        invalidate_cli_managed_session_activity_tx(
            &tx,
            Some(managed_session_id),
            Some(session_epoch),
            now,
        )?;
        let session = query_cli_managed_session_tx(&tx, managed_session_id)?;
        tx.commit()?;
        Ok(CliManagedSessionProofInvalidation { session })
    }

    pub fn begin_cli_accept_pending_attempt(
        &mut self,
        attempt: NewCliAcceptPendingAttempt,
    ) -> Result<DeliveryAttemptRecord> {
        ensure_cli_attempt_authorization_mode_value(&attempt.authorization_mode)?;
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        if let Some(existing) =
            query_delivery_attempt_by_rpc_request_id_tx(&tx, &attempt.delivery_rpc_request_id)?
        {
            ensure_existing_cli_accept_matches_request(&existing, &attempt)?;
            ensure_cli_attempt_has_current_managed_session_tx(&tx, &existing)?;
            tx.commit()?;
            return Ok(existing);
        }
        let batch = query_batch_tx(&tx, &attempt.batch_id)?;
        ensure_batch_open(&batch)?;
        ensure_batch_is_thread_head_tx(&tx, &batch)?;
        ensure_batch_allows_automatic_delivery(&batch)?;
        ensure_batch_allows_cli_delivery_for_authorization(&batch, &attempt.authorization_mode)?;
        let session = query_cli_managed_session_tx(&tx, &attempt.managed_session_id)?;
        ensure_cli_session_allows_delivery(
            &session,
            &batch.source_thread_id,
            attempt.session_epoch,
            &attempt.delivery_rpc_kind,
            &attempt.authorization_mode,
        )?;
        ensure_attempt_budget_remaining(&batch)?;
        ensure_no_active_attempt_for_thread_tx(&tx, &batch.source_thread_id)?;
        let generation = next_attempt_generation_tx(&tx, &attempt.batch_id)?;

        tx.execute(
            "INSERT INTO delivery_attempts (
                attempt_id, batch_id, source_thread_id, adapter_kind,
                authorization_mode, state,
                generation, delivery_rpc_request_id, delivery_rpc_kind,
                delivery_rpc_state, delivery_rpc_correlation_marker,
                delivery_rpc_started_at, managed_session_id, session_epoch,
                session_activity_revision, session_capability_revision, created_at, updated_at
            ) VALUES (?, ?, ?, 'cli', ?, 'accept_pending', ?, ?, ?, 'pending_acceptance', ?, ?, ?, ?, ?, ?, ?, ?)",
            params![
                attempt.attempt_id,
                attempt.batch_id,
                batch.source_thread_id,
                attempt.authorization_mode,
                generation,
                attempt.delivery_rpc_request_id,
                attempt.delivery_rpc_kind,
                attempt.delivery_rpc_correlation_marker,
                attempt.delivery_rpc_started_at,
                attempt.managed_session_id,
                attempt.session_epoch,
                session.activity_revision,
                session.capability_revision,
                attempt.delivery_rpc_started_at,
                attempt.delivery_rpc_started_at,
            ],
        )?;
        let record = query_delivery_attempt_tx(&tx, &attempt.attempt_id)?;
        tx.commit()?;
        Ok(record)
    }

    pub fn accept_cli_attempt(
        &mut self,
        attempt_id: &str,
        delivery_turn_id: &str,
        delivery_accepted_at: i64,
        delivery_observation_deadline: i64,
    ) -> Result<DeliveryAttemptRecord> {
        ensure_nonempty_value("delivery_turn_id", delivery_turn_id)?;
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let attempt = query_delivery_attempt_tx(&tx, attempt_id)?;
        if attempt.state == "cooldown"
            && attempt.delivery_rpc_state.as_deref() == Some("accepted")
            && attempt.delivery_turn_id.as_deref() == Some(delivery_turn_id)
        {
            ensure_cli_attempt_has_current_managed_session_tx(&tx, &attempt)?;
            tx.commit()?;
            return Ok(attempt);
        }
        ensure_attempt_accept_pending(&attempt)?;
        let batch = query_batch_tx(&tx, &attempt.batch_id)?;
        ensure_batch_open(&batch)?;
        ensure_batch_is_thread_head_tx(&tx, &batch)?;
        ensure_batch_allows_automatic_delivery(&batch)?;
        ensure_batch_allows_cli_delivery_for_authorization(&batch, &attempt.authorization_mode)?;
        ensure_cli_attempt_has_current_managed_session_for_batch_tx(&tx, &attempt, &batch)?;
        ensure_attempt_budget_remaining(&batch)?;
        ensure_attempt_is_current_generation_tx(&tx, &attempt)?;

        tx.execute(
            "UPDATE delivery_attempts
             SET state = 'cooldown',
                 delivery_rpc_state = 'accepted',
                 delivery_turn_id = ?,
                 delivery_accepted_at = ?,
                 delivery_observation_state = 'tracking',
                 delivery_observation_deadline = ?,
                 updated_at = ?
             WHERE attempt_id = ?
               AND state = 'accept_pending'
               AND delivery_rpc_state = 'pending_acceptance'",
            params![
                delivery_turn_id,
                delivery_accepted_at,
                delivery_observation_deadline,
                delivery_accepted_at,
                attempt_id,
            ],
        )?;
        let changed = tx.execute(
            "UPDATE batches
             SET delivery_attempt_count = delivery_attempt_count + 1,
                 updated_at = ?
             WHERE batch_id = ?
               AND state = 'open'
               AND delivery_attempt_count < max_delivery_attempts",
            params![delivery_accepted_at, attempt.batch_id],
        )?;
        if changed != 1 {
            bail!(
                "batch {} has no remaining delivery attempts",
                attempt.batch_id
            );
        }
        let record = query_delivery_attempt_tx(&tx, attempt_id)?;
        tx.commit()?;
        Ok(record)
    }

    pub fn reject_cli_attempt_before_accept(
        &mut self,
        attempt_id: &str,
        rejected_at: i64,
    ) -> Result<DeliveryAttemptRecord> {
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let attempt = query_delivery_attempt_tx(&tx, attempt_id)?;
        if attempt.state == "abandoned"
            && attempt.delivery_rpc_state.as_deref() == Some("rejected_before_accept")
        {
            tx.commit()?;
            return Ok(attempt);
        }
        ensure_attempt_accept_pending(&attempt)?;
        let batch = query_batch_tx(&tx, &attempt.batch_id)?;
        ensure_batch_open(&batch)?;
        ensure_batch_is_thread_head_tx(&tx, &batch)?;
        ensure_attempt_is_current_generation_tx(&tx, &attempt)?;
        ensure_cli_attempt_has_current_managed_session_for_batch_tx(&tx, &attempt, &batch)?;
        tx.execute(
            "UPDATE delivery_attempts
             SET state = 'abandoned',
                 delivery_rpc_state = 'rejected_before_accept',
                 delivery_observation_state = 'abandoned',
                 updated_at = ?,
                 abandoned_at = ?
             WHERE attempt_id = ?
               AND state = 'accept_pending'
               AND delivery_rpc_state = 'pending_acceptance'",
            params![rejected_at, rejected_at, attempt_id],
        )?;
        invalidate_cli_managed_session_activity_tx(
            &tx,
            attempt.managed_session_id.as_deref(),
            attempt.session_epoch,
            rejected_at,
        )?;
        let record = query_delivery_attempt_tx(&tx, attempt_id)?;
        tx.commit()?;
        Ok(record)
    }

    pub fn observe_cli_turn_event(
        &mut self,
        layout: &FsLayout,
        attempt_id: &str,
        delivery_turn_id: &str,
        turn_event: &str,
        observed_at: i64,
    ) -> Result<DeliveryAttemptRecord> {
        ensure_nonempty_value("delivery_turn_id", delivery_turn_id)?;
        ensure_cli_turn_event_value(turn_event)?;
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let attempt = query_delivery_attempt_tx(&tx, attempt_id)?;
        if attempt.adapter_kind != "cli" {
            bail!(
                "delivery attempt {} is not a CLI attempt",
                attempt.attempt_id
            );
        }
        ensure_cli_turn_observation_matches_attempt(&attempt, delivery_turn_id)?;
        if is_idempotent_terminal_cli_turn_observation(&attempt, turn_event) {
            tx.commit()?;
            return Ok(attempt);
        }
        if can_complete_delayed_cli_turn_after_expiry(&attempt, turn_event, observed_at)? {
            if ensure_cli_attempt_has_recorded_detached_delivery_proof_snapshot(&attempt).is_err() {
                record_late_cli_turn_evidence_tx(&tx, &attempt, turn_event, observed_at)?;
                let record = query_delivery_attempt_tx(&tx, attempt_id)?;
                tx.commit()?;
                return Ok(record);
            }
            let artifacts_to_sync =
                complete_delayed_cli_turn_after_expiry_tx(&tx, &attempt, observed_at)?;
            let record = query_delivery_attempt_tx(&tx, attempt_id)?;
            tx.commit()?;
            let _ =
                sync_artifact_manifests(&mut self.conn, layout, &artifacts_to_sync, observed_at);
            return Ok(record);
        }
        if can_record_late_cli_turn_evidence(&attempt, turn_event) {
            record_late_cli_turn_evidence_tx(&tx, &attempt, turn_event, observed_at)?;
            let record = query_delivery_attempt_tx(&tx, attempt_id)?;
            tx.commit()?;
            return Ok(record);
        }
        ensure_attempt_tracking_cli_turn_observation(&attempt)?;
        let batch = query_batch_tx(&tx, &attempt.batch_id)?;
        ensure_batch_open(&batch)?;
        ensure_batch_is_thread_head_tx(&tx, &batch)?;
        ensure_attempt_is_current_generation_tx(&tx, &attempt)?;
        if ensure_cli_attempt_has_current_managed_session_for_batch_tx(&tx, &attempt, &batch)
            .is_err()
        {
            abandon_cli_turn_observation_tx(&tx, &attempt, turn_event, "abandoned", observed_at)?;
            manualize_cli_batch_after_observation_loss_tx(&tx, &attempt, observed_at)?;
            let record = query_delivery_attempt_tx(&tx, attempt_id)?;
            tx.commit()?;
            return Ok(record);
        }

        let mut artifacts_to_sync = Vec::new();
        if cli_turn_observation_is_after_deadline(&attempt, observed_at)? {
            abandon_cli_turn_observation_tx(&tx, &attempt, turn_event, "expired", observed_at)?;
            manualize_cli_batch_after_observation_loss_tx(&tx, &attempt, observed_at)?;
        } else {
            match turn_event {
                "turn_started" => {
                    tx.execute(
                        "UPDATE delivery_attempts
                         SET last_observed_turn_event = ?,
                             last_observed_turn_event_at = ?,
                             updated_at = ?
                         WHERE attempt_id = ?
                           AND state = 'cooldown'
                           AND delivery_observation_state = 'tracking'",
                        params![turn_event, observed_at, observed_at, attempt_id],
                    )?;
                }
                "turn_completed" => {
                    ensure_batch_allows_automatic_delivery(&batch)?;
                    tx.execute(
                        "UPDATE delivery_attempts
                         SET delivery_observation_state = 'completed',
                             last_observed_turn_event = ?,
                             last_observed_turn_event_at = ?,
                             updated_at = ?
                         WHERE attempt_id = ?
                           AND state = 'cooldown'
                           AND delivery_observation_state = 'tracking'",
                        params![turn_event, observed_at, observed_at, attempt_id],
                    )?;
                    close_batch_tx(
                        &tx,
                        &attempt.batch_id,
                        "delivered",
                        Some("observed CLI turn completion"),
                        observed_at,
                    )?;
                    artifacts_to_sync = extend_closed_batch_artifact_retention_tx(
                        &tx,
                        &attempt.batch_id,
                        observed_at,
                    )?;
                }
                "turn_failed" | "turn_interrupted" | "turn_replaced" => {
                    abandon_cli_turn_observation_tx(
                        &tx,
                        &attempt,
                        turn_event,
                        "abandoned",
                        observed_at,
                    )?;
                    manualize_cli_batch_after_observation_loss_tx(&tx, &attempt, observed_at)?;
                }
                other => bail!("unsupported CLI turn event {other}"),
            }
        }

        let record = query_delivery_attempt_tx(&tx, attempt_id)?;
        tx.commit()?;
        let _ = sync_artifact_manifests(&mut self.conn, layout, &artifacts_to_sync, observed_at);
        Ok(record)
    }

    pub fn inspect_attempt(&self, attempt_id: &str) -> Result<DeliveryAttemptRecord> {
        query_delivery_attempt(&self.conn, attempt_id)
    }

    pub fn record_audit_decision(
        &mut self,
        decision: NewAuditDecision,
    ) -> Result<AuditDecisionRecord> {
        let details_json = serde_json::to_string(&decision.details)?;
        let tx = self.conn.transaction()?;
        tx.execute(
            "INSERT INTO audit_decisions (
                audit_id, recorded_at, source_thread_id, batch_id, attempt_id,
                managed_session_id, session_epoch, policy_kind, decision,
                reason, adapter_kind, details_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            params![
                decision.audit_id,
                decision.recorded_at,
                decision.source_thread_id,
                decision.batch_id,
                decision.attempt_id,
                decision.managed_session_id,
                decision.session_epoch,
                decision.policy_kind,
                decision.decision,
                decision.reason,
                decision.adapter_kind,
                details_json,
            ],
        )?;
        let record = query_audit_decision_tx(&tx, &decision.audit_id)?;
        tx.commit()?;
        Ok(record)
    }

    pub fn list_audit_decisions(
        &self,
        source_thread_id: Option<&str>,
        limit: i64,
    ) -> Result<Vec<AuditDecisionRecord>> {
        let limit = limit.clamp(1, 500);
        match source_thread_id {
            Some(thread_id) => {
                let mut stmt = self.conn.prepare(
                    "SELECT * FROM audit_decisions
                     WHERE source_thread_id = ?
                     ORDER BY recorded_at DESC, audit_id DESC
                     LIMIT ?",
                )?;
                rows_to_audit_decisions(stmt.query(params![thread_id, limit])?)
            }
            None => {
                let mut stmt = self.conn.prepare(
                    "SELECT * FROM audit_decisions
                     ORDER BY recorded_at DESC, audit_id DESC
                     LIMIT ?",
                )?;
                rows_to_audit_decisions(stmt.query(params![limit])?)
            }
        }
    }

    pub fn daemon_lifecycle_status(
        &self,
        now: i64,
        idle_horizon_at: i64,
    ) -> Result<DaemonLifecycleStatus> {
        let active_jobs = self.conn.query_row(
            "SELECT COUNT(*) FROM jobs WHERE status = 'pending'",
            [],
            |row| row.get::<_, i64>(0),
        )?;
        let active_cli_acceptances = count_active_cli_acceptances(&self.conn, now)?;
        let cli_acceptances_stale_now = count_stale_cli_acceptances(&self.conn, now)?;
        let active_cli_observations = count_active_cli_observations(&self.conn, now)?;
        let cli_observations_due_now = count_cli_observations_due_now(&self.conn, now)?;
        let open_batches_due_now = count_open_batches_due_at(&self.conn, now)?;
        let open_batches_due_within_idle = count_open_batches_due_at(&self.conn, idle_horizon_at)?;
        Ok(DaemonLifecycleStatus {
            active_jobs,
            active_cli_acceptances,
            cli_acceptances_stale_now,
            active_cli_observations,
            cli_observations_due_now,
            open_batches_due_now,
            open_batches_due_within_idle,
        })
    }

    pub fn close_head(
        &mut self,
        layout: &FsLayout,
        source_thread_id: &str,
        reason: &str,
        note: Option<&str>,
        now: i64,
    ) -> Result<BatchInspect> {
        let tx = self.conn.transaction()?;
        let batch_id: String = tx
            .query_row(
                "SELECT batch_id FROM batches
                 WHERE source_thread_id = ? AND state = 'open'
                 ORDER BY created_at ASC, batch_id ASC
                 LIMIT 1",
                params![source_thread_id],
                |row| row.get(0),
            )
            .optional()?
            .ok_or_else(|| anyhow!("no open batch for source_thread_id {source_thread_id}"))?;

        close_batch_tx(&tx, &batch_id, reason, note, now)?;
        let artifacts_to_sync = extend_closed_batch_artifact_retention_tx(&tx, &batch_id, now)?;
        let inspect = query_batch_inspect_tx(&tx, &batch_id)?;
        tx.commit()?;
        let _ = sync_artifact_manifests(&mut self.conn, layout, &artifacts_to_sync, now);
        Ok(inspect)
    }

    pub fn sweep(&mut self, layout: &FsLayout, now: i64) -> Result<SweepReport> {
        let tx = self.conn.transaction()?;
        let stale_cli_acceptances_abandoned = expire_stale_cli_acceptances_tx(&tx, now)?;
        let expired_cli_observations_abandoned = expire_due_cli_observations_tx(&tx, now)?;
        let (expired_manual_batches_closed, artifacts_to_sync) =
            close_expired_manual_batches_tx(&tx, now)?;
        let (expired_automatic_batches_closed, automatic_artifacts_to_sync) =
            close_expired_automatic_batches_tx(&tx, now)?;
        let expired_artifacts = query_deletable_artifacts_tx(&tx, now)?;
        let manifest_sync_artifacts = query_artifacts_for_manifest_sync_tx(&tx)?;
        tx.commit()?;
        let mut manifest_sync_candidates = artifacts_to_sync;
        manifest_sync_candidates.extend(automatic_artifacts_to_sync);
        manifest_sync_candidates.extend(manifest_sync_artifacts);
        let manifest_report = sync_artifact_manifests(
            &mut self.conn,
            layout,
            &dedupe_artifacts_by_id(manifest_sync_candidates),
            now,
        );

        let mut deleted_artifact_ids = Vec::new();
        let mut artifact_delete_failures = 0_usize;
        for artifact in &expired_artifacts {
            match validate_artifact_record(artifact)
                .and_then(|()| remove_artifact_dir(&layout.artifact_dir(&artifact.artifact_id)))
            {
                Ok(true) => deleted_artifact_ids.push(artifact.artifact_id.clone()),
                Ok(false) => {}
                Err(_) => {
                    mark_artifact_gc_attempted(&self.conn, &artifact.artifact_id, now)?;
                    artifact_delete_failures += 1;
                }
            }
        }
        let orphan_report = self.cleanup_stale_artifact_ingests(layout)?;

        if !deleted_artifact_ids.is_empty() {
            let tx = self.conn.transaction()?;
            for artifact_id in &deleted_artifact_ids {
                tx.execute(
                    "DELETE FROM artifacts WHERE artifact_id = ?",
                    params![artifact_id],
                )?;
            }
            tx.commit()?;
        }

        Ok(SweepReport {
            stale_cli_acceptances_abandoned,
            expired_cli_observations_abandoned,
            expired_manual_batches_closed,
            expired_automatic_batches_closed,
            artifacts_deleted: deleted_artifact_ids.len(),
            artifact_delete_failures,
            orphan_artifacts_deleted: orphan_report.deleted,
            orphan_artifact_delete_failures: orphan_report.failed,
            artifact_manifests_synced: manifest_report.synced,
            artifact_manifest_sync_failures: manifest_report.failed,
        })
    }

    fn cleanup_stale_artifact_ingests(&mut self, layout: &FsLayout) -> Result<CleanupReport> {
        let stale_before =
            wall_clock_epoch_seconds()?.saturating_sub(ORPHAN_ARTIFACT_GRACE_SECONDS);
        let stale_ingests = query_stale_artifact_ingests(&self.conn, stale_before)?;
        let mut report = CleanupReport::default();
        for ingest in stale_ingests {
            if validate_id_path_component(&ingest.artifact_id, "artifact_id").is_err() {
                self.abandon_artifact_ingest(&ingest.artifact_id)?;
                report.failed += 1;
                continue;
            }
            if ingest.relative_path != relative_artifact_payload_path(&ingest.artifact_id) {
                refresh_artifact_ingest_at_wall_clock(&self.conn, &ingest.artifact_id)?;
                report.failed += 1;
                continue;
            }

            if artifact_exists(&self.conn, &ingest.artifact_id)? {
                self.abandon_artifact_ingest(&ingest.artifact_id)?;
                continue;
            }

            let artifact_dir = layout.artifact_dir(&ingest.artifact_id);
            match artifact_ingest_is_active_or_too_recent(&artifact_dir) {
                Ok(true) => {
                    refresh_artifact_ingest_at_wall_clock(&self.conn, &ingest.artifact_id)?;
                    continue;
                }
                Ok(false) => {}
                Err(_) => {
                    refresh_artifact_ingest_at_wall_clock(&self.conn, &ingest.artifact_id)?;
                    report.failed += 1;
                    continue;
                }
            }

            match remove_artifact_dir(&artifact_dir) {
                Ok(true) => {
                    self.abandon_artifact_ingest(&ingest.artifact_id)?;
                    report.deleted += 1;
                }
                Ok(false) => {}
                Err(_) => {
                    refresh_artifact_ingest_at_wall_clock(&self.conn, &ingest.artifact_id)?;
                    report.failed += 1;
                }
            }
        }
        Ok(report)
    }
}

#[derive(Default)]
struct CleanupReport {
    deleted: usize,
    failed: usize,
}

fn sync_artifact_manifests(
    conn: &mut Connection,
    layout: &FsLayout,
    artifacts: &[ArtifactRecord],
    now: i64,
) -> ManifestSyncReport {
    let mut report = ManifestSyncReport::default();
    for artifact in artifacts {
        match sync_artifact_manifest(conn, layout, &artifact.artifact_id, now) {
            Ok(true) => report.synced += 1,
            Ok(false) => {}
            Err(_) => {
                let _ = mark_artifact_manifest_sync_attempted(conn, &artifact.artifact_id, now);
                report.failed += 1;
            }
        }
    }
    report
}

fn sync_artifact_manifest(
    conn: &mut Connection,
    layout: &FsLayout,
    artifact_id: &str,
    now: i64,
) -> Result<bool> {
    let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let Some(artifact) = query_artifact_by_id_tx(&tx, artifact_id)? else {
        tx.commit()?;
        return Ok(false);
    };
    if artifact.manifest_synced_retention_until >= artifact.retention_until {
        tx.commit()?;
        return Ok(false);
    }

    rewrite_artifact_manifest(layout, &artifact)?;
    let changed = tx.execute(
        "UPDATE artifacts
         SET manifest_synced_retention_until = retention_until,
             manifest_sync_attempted_at = ?
         WHERE artifact_id = ?
           AND retention_until = ?
           AND manifest_synced_retention_until < retention_until",
        params![now, artifact.artifact_id, artifact.retention_until],
    )?;
    if changed != 1 {
        bail!("artifact {} manifest sync CAS failed", artifact.artifact_id);
    }
    tx.commit()?;
    Ok(true)
}

fn mark_artifact_manifest_sync_attempted(
    conn: &Connection,
    artifact_id: &str,
    now: i64,
) -> Result<()> {
    conn.execute(
        "UPDATE artifacts
         SET manifest_sync_attempted_at = ?
         WHERE artifact_id = ?",
        params![now, artifact_id],
    )?;
    Ok(())
}

fn dedupe_artifacts_by_id(artifacts: Vec<ArtifactRecord>) -> Vec<ArtifactRecord> {
    let mut seen = HashSet::with_capacity(artifacts.len());
    artifacts
        .into_iter()
        .filter(|artifact| seen.insert(artifact.artifact_id.clone()))
        .collect()
}

fn validate_artifact_record(artifact: &ArtifactRecord) -> Result<()> {
    validate_id_path_component(&artifact.artifact_id, "artifact_id")?;
    let expected_relative_path = relative_artifact_payload_path(&artifact.artifact_id);
    if artifact.relative_path != expected_relative_path {
        bail!(
            "artifact {} has non-canonical relative_path {}",
            artifact.artifact_id,
            artifact.relative_path
        );
    }
    Ok(())
}

fn artifact_exists(conn: &Connection, artifact_id: &str) -> Result<bool> {
    let exists: Option<i64> = conn
        .query_row(
            "SELECT 1 FROM artifacts WHERE artifact_id = ?",
            params![artifact_id],
            |row| row.get(0),
        )
        .optional()?;
    Ok(exists.is_some())
}

fn mark_artifact_gc_attempted(conn: &Connection, artifact_id: &str, now: i64) -> Result<()> {
    conn.execute(
        "UPDATE artifacts SET gc_attempted_at = ? WHERE artifact_id = ?",
        params![now, artifact_id],
    )?;
    Ok(())
}

fn is_sqlite_busy_or_locked(error: &anyhow::Error) -> bool {
    let Some(sqlite_error) = error.downcast_ref::<rusqlite::Error>() else {
        return false;
    };
    matches!(
        sqlite_error.sqlite_error_code(),
        Some(ErrorCode::DatabaseBusy | ErrorCode::DatabaseLocked)
    )
}

fn artifact_ingest_is_active_or_too_recent(artifact_dir: &std::path::Path) -> Result<bool> {
    let marker = ingest_marker_path(artifact_dir);
    let metadata = match fs::metadata(&marker) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(error).with_context(|| format!("stat {}", marker.display())),
    };
    let now = system_time_epoch_seconds(SystemTime::now())?;
    let marker_modified = system_time_epoch_seconds(
        metadata
            .modified()
            .with_context(|| format!("read modified time {}", marker.display()))?,
    )?;
    Ok(now.saturating_sub(marker_modified) < ORPHAN_ARTIFACT_GRACE_SECONDS)
}

fn query_stale_artifact_ingests(
    conn: &Connection,
    stale_before: i64,
) -> Result<Vec<ArtifactIngestRecord>> {
    let mut stmt = conn.prepare(
        "SELECT artifact_id
                , relative_path
         FROM artifact_ingests
         WHERE updated_at <= ?
         ORDER BY updated_at ASC, artifact_id ASC
         LIMIT ?",
    )?;
    let records = stmt
        .query_map(
            params![stale_before, MAX_STALE_ARTIFACT_INGESTS_PER_SWEEP],
            |row| {
                Ok(ArtifactIngestRecord {
                    artifact_id: row.get("artifact_id")?,
                    relative_path: row.get("relative_path")?,
                })
            },
        )?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(records)
}

fn refresh_artifact_ingest(conn: &Connection, artifact_id: &str, now: i64) -> Result<()> {
    conn.execute(
        "UPDATE artifact_ingests SET updated_at = ? WHERE artifact_id = ?",
        params![now, artifact_id],
    )?;
    Ok(())
}

fn refresh_artifact_ingest_at_wall_clock(conn: &Connection, artifact_id: &str) -> Result<()> {
    refresh_artifact_ingest(conn, artifact_id, wall_clock_epoch_seconds()?)
}

fn system_time_epoch_seconds(time: SystemTime) -> Result<i64> {
    let duration = time
        .duration_since(UNIX_EPOCH)
        .context("file timestamp is before unix epoch")?;
    i64::try_from(duration.as_secs()).context("epoch seconds overflow")
}

fn wall_clock_epoch_seconds() -> Result<i64> {
    system_time_epoch_seconds(SystemTime::now())
}

fn remove_artifact_dir(path: &std::path::Path) -> Result<bool> {
    remove_dir_all_durable(path)
}

fn migrate(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        "
        CREATE TABLE IF NOT EXISTS jobs (
            job_id TEXT PRIMARY KEY,
            source_thread_id TEXT NOT NULL,
            status TEXT NOT NULL CHECK (status IN ('pending', 'completed', 'failed')),
            summary TEXT NOT NULL,
            metadata_json TEXT NOT NULL,
            created_at INTEGER NOT NULL,
            updated_at INTEGER NOT NULL,
            completed_at INTEGER,
            failed_at INTEGER,
            result_artifact_id TEXT,
            failure_reason TEXT,
            delivery_read_only INTEGER NOT NULL,
            delivery_requires_approval INTEGER NOT NULL,
            delivery_requires_network INTEGER NOT NULL,
            delivery_requires_write_access INTEGER NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_jobs_thread_created
            ON jobs(source_thread_id, created_at);
        CREATE INDEX IF NOT EXISTS idx_jobs_status_created
            ON jobs(status, created_at);

        CREATE TABLE IF NOT EXISTS artifacts (
            artifact_id TEXT PRIMARY KEY,
            job_id TEXT NOT NULL UNIQUE REFERENCES jobs(job_id) ON DELETE CASCADE,
            relative_path TEXT NOT NULL,
            original_filename TEXT,
            size_bytes INTEGER NOT NULL,
            sha256 TEXT NOT NULL,
            created_at INTEGER NOT NULL,
            retention_until INTEGER NOT NULL,
            manifest_synced_retention_until INTEGER NOT NULL DEFAULT 0,
            manifest_sync_attempted_at INTEGER NOT NULL DEFAULT 0,
            gc_attempted_at INTEGER NOT NULL DEFAULT 0
        );

        CREATE INDEX IF NOT EXISTS idx_artifacts_retention
            ON artifacts(retention_until);

        CREATE TABLE IF NOT EXISTS artifact_ingests (
            artifact_id TEXT PRIMARY KEY,
            job_id TEXT NOT NULL REFERENCES jobs(job_id) ON DELETE CASCADE,
            relative_path TEXT NOT NULL,
            created_at INTEGER NOT NULL,
            updated_at INTEGER NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_artifact_ingests_updated
            ON artifact_ingests(updated_at, artifact_id);

        CREATE TABLE IF NOT EXISTS batches (
            batch_id TEXT PRIMARY KEY,
            source_thread_id TEXT NOT NULL,
            state TEXT NOT NULL CHECK (state IN ('open', 'closed')),
            replay_policy TEXT NOT NULL CHECK (replay_policy IN ('automatic', 'manual_resolution_only')),
            close_reason TEXT,
            close_note TEXT,
            summary TEXT NOT NULL,
            created_at INTEGER NOT NULL,
            updated_at INTEGER NOT NULL,
            closed_at INTEGER,
            redelivery_window_ends_at INTEGER NOT NULL,
            max_delivery_attempts INTEGER NOT NULL,
            delivery_attempt_count INTEGER NOT NULL,
            delivery_read_only INTEGER NOT NULL,
            delivery_requires_approval INTEGER NOT NULL,
            delivery_requires_network INTEGER NOT NULL,
            delivery_requires_write_access INTEGER NOT NULL,
            inline_payload_bytes INTEGER NOT NULL,
            requires_artifact_read INTEGER NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_batches_head
            ON batches(source_thread_id, state, created_at, batch_id);
        CREATE INDEX IF NOT EXISTS idx_batches_manual_expiry
            ON batches(state, replay_policy, redelivery_window_ends_at);

        CREATE TABLE IF NOT EXISTS cli_managed_sessions (
            managed_session_id TEXT PRIMARY KEY,
            bound_thread_id TEXT NOT NULL,
            session_epoch INTEGER NOT NULL,
            session_state TEXT NOT NULL CHECK (session_state IN ('live', 'detached', 'parked', 'stale', 'retired')),
            activity_state TEXT NOT NULL CHECK (activity_state IN ('unknown', 'active', 'idle')),
            activity_revision INTEGER NOT NULL DEFAULT 0,
            capability_revision INTEGER NOT NULL DEFAULT 0,
            capability_thread_resume INTEGER NOT NULL DEFAULT 0,
            capability_turn_start INTEGER NOT NULL DEFAULT 0,
            capability_current_state_sync INTEGER NOT NULL DEFAULT 0,
            capability_turn_completed_event INTEGER NOT NULL DEFAULT 0,
            capability_negative_terminal_events INTEGER NOT NULL DEFAULT 0,
            capability_thread_start INTEGER NOT NULL DEFAULT 0,
            capability_turn_steer INTEGER NOT NULL DEFAULT 0,
            session_allows_approval INTEGER NOT NULL,
            session_allows_network INTEGER NOT NULL,
            session_allows_write_access INTEGER NOT NULL,
            created_at INTEGER NOT NULL,
            updated_at INTEGER NOT NULL,
            retired_at INTEGER
        );

        CREATE UNIQUE INDEX IF NOT EXISTS idx_cli_managed_sessions_bound_live
            ON cli_managed_sessions(bound_thread_id)
            WHERE session_state != 'retired';
        CREATE INDEX IF NOT EXISTS idx_cli_managed_sessions_state
            ON cli_managed_sessions(session_state, updated_at);

        CREATE TABLE IF NOT EXISTS delivery_attempts (
            attempt_id TEXT PRIMARY KEY,
            batch_id TEXT NOT NULL REFERENCES batches(batch_id) ON DELETE CASCADE,
            source_thread_id TEXT NOT NULL,
            adapter_kind TEXT NOT NULL CHECK (adapter_kind IN ('cli', 'desktop')),
            authorization_mode TEXT NOT NULL DEFAULT 'strict_safe' CHECK (authorization_mode IN ('strict_safe', 'trusted_all')),
            state TEXT NOT NULL CHECK (state IN ('prepared', 'accept_pending', 'arm_pending', 'cooldown', 'abandoned', 'superseded', 'closed')),
            generation INTEGER NOT NULL,
            delivery_rpc_request_id TEXT UNIQUE,
            delivery_rpc_kind TEXT CHECK (delivery_rpc_kind IS NULL OR delivery_rpc_kind IN ('turn_start', 'turn_steer')),
            delivery_rpc_state TEXT CHECK (delivery_rpc_state IS NULL OR delivery_rpc_state IN ('pending_acceptance', 'accepted', 'rejected_before_accept', 'unknown')),
            delivery_rpc_correlation_marker TEXT,
            delivery_rpc_started_at INTEGER,
            managed_session_id TEXT,
            session_epoch INTEGER,
            session_activity_revision INTEGER NOT NULL DEFAULT 0,
            session_capability_revision INTEGER NOT NULL DEFAULT 0,
            delivery_turn_id TEXT,
            delivery_accepted_at INTEGER,
            delivery_observation_state TEXT CHECK (delivery_observation_state IS NULL OR delivery_observation_state IN ('tracking', 'completed', 'expired', 'abandoned')),
            delivery_observation_deadline INTEGER,
            last_observed_turn_event TEXT CHECK (last_observed_turn_event IS NULL OR last_observed_turn_event IN ('turn_started', 'turn_completed', 'turn_failed', 'turn_interrupted', 'turn_replaced')),
            last_observed_turn_event_at INTEGER,
            created_at INTEGER NOT NULL,
            updated_at INTEGER NOT NULL,
            abandoned_at INTEGER,
            closed_at INTEGER,
            UNIQUE(batch_id, generation)
        );

        CREATE INDEX IF NOT EXISTS idx_delivery_attempts_batch_state
            ON delivery_attempts(batch_id, state, generation);
        CREATE INDEX IF NOT EXISTS idx_delivery_attempts_cli_observation
            ON delivery_attempts(adapter_kind, delivery_observation_state, delivery_observation_deadline);

        CREATE TABLE IF NOT EXISTS audit_decisions (
            audit_id TEXT PRIMARY KEY,
            recorded_at INTEGER NOT NULL,
            source_thread_id TEXT,
            batch_id TEXT,
            attempt_id TEXT,
            managed_session_id TEXT,
            session_epoch INTEGER,
            policy_kind TEXT NOT NULL,
            decision TEXT NOT NULL,
            reason TEXT NOT NULL,
            adapter_kind TEXT NOT NULL,
            details_json TEXT NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_audit_decisions_thread_time
            ON audit_decisions(source_thread_id, recorded_at DESC, audit_id DESC);

        CREATE TABLE IF NOT EXISTS batch_jobs (
            batch_id TEXT NOT NULL REFERENCES batches(batch_id) ON DELETE CASCADE,
            job_id TEXT NOT NULL REFERENCES jobs(job_id) ON DELETE CASCADE,
            position INTEGER NOT NULL,
            PRIMARY KEY (batch_id, job_id)
        );
        ",
    )?;
    ensure_column(
        conn,
        "artifacts",
        "manifest_synced_retention_until",
        "ALTER TABLE artifacts ADD COLUMN manifest_synced_retention_until INTEGER NOT NULL DEFAULT 0",
    )?;
    ensure_column(
        conn,
        "artifacts",
        "manifest_sync_attempted_at",
        "ALTER TABLE artifacts ADD COLUMN manifest_sync_attempted_at INTEGER NOT NULL DEFAULT 0",
    )?;
    ensure_column(
        conn,
        "artifacts",
        "gc_attempted_at",
        "ALTER TABLE artifacts ADD COLUMN gc_attempted_at INTEGER NOT NULL DEFAULT 0",
    )?;
    ensure_column(
        conn,
        "cli_managed_sessions",
        "activity_revision",
        "ALTER TABLE cli_managed_sessions ADD COLUMN activity_revision INTEGER NOT NULL DEFAULT 0",
    )?;
    ensure_column(
        conn,
        "cli_managed_sessions",
        "capability_revision",
        "ALTER TABLE cli_managed_sessions ADD COLUMN capability_revision INTEGER NOT NULL DEFAULT 0",
    )?;
    ensure_column(
        conn,
        "cli_managed_sessions",
        "capability_thread_resume",
        "ALTER TABLE cli_managed_sessions ADD COLUMN capability_thread_resume INTEGER NOT NULL DEFAULT 0",
    )?;
    ensure_column(
        conn,
        "cli_managed_sessions",
        "capability_turn_start",
        "ALTER TABLE cli_managed_sessions ADD COLUMN capability_turn_start INTEGER NOT NULL DEFAULT 0",
    )?;
    ensure_column(
        conn,
        "cli_managed_sessions",
        "capability_current_state_sync",
        "ALTER TABLE cli_managed_sessions ADD COLUMN capability_current_state_sync INTEGER NOT NULL DEFAULT 0",
    )?;
    ensure_column(
        conn,
        "cli_managed_sessions",
        "capability_turn_completed_event",
        "ALTER TABLE cli_managed_sessions ADD COLUMN capability_turn_completed_event INTEGER NOT NULL DEFAULT 0",
    )?;
    ensure_column(
        conn,
        "cli_managed_sessions",
        "capability_negative_terminal_events",
        "ALTER TABLE cli_managed_sessions ADD COLUMN capability_negative_terminal_events INTEGER NOT NULL DEFAULT 0",
    )?;
    ensure_column(
        conn,
        "cli_managed_sessions",
        "capability_thread_start",
        "ALTER TABLE cli_managed_sessions ADD COLUMN capability_thread_start INTEGER NOT NULL DEFAULT 0",
    )?;
    ensure_column(
        conn,
        "cli_managed_sessions",
        "capability_turn_steer",
        "ALTER TABLE cli_managed_sessions ADD COLUMN capability_turn_steer INTEGER NOT NULL DEFAULT 0",
    )?;
    ensure_column(
        conn,
        "delivery_attempts",
        "authorization_mode",
        "ALTER TABLE delivery_attempts ADD COLUMN authorization_mode TEXT NOT NULL DEFAULT 'strict_safe'",
    )?;
    ensure_column(
        conn,
        "delivery_attempts",
        "session_activity_revision",
        "ALTER TABLE delivery_attempts ADD COLUMN session_activity_revision INTEGER NOT NULL DEFAULT 0",
    )?;
    ensure_column(
        conn,
        "delivery_attempts",
        "session_capability_revision",
        "ALTER TABLE delivery_attempts ADD COLUMN session_capability_revision INTEGER NOT NULL DEFAULT 0",
    )?;
    Ok(())
}

fn ensure_column(
    conn: &Connection,
    table_name: &str,
    column_name: &str,
    alter_sql: &str,
) -> Result<()> {
    let pragma = format!("PRAGMA table_info({table_name})");
    let mut stmt = conn.prepare(&pragma)?;
    let exists = stmt
        .query_map([], |row| row.get::<_, String>(1))?
        .collect::<rusqlite::Result<Vec<_>>>()?
        .into_iter()
        .any(|name| name == column_name);
    drop(stmt);

    if !exists {
        conn.execute(alter_sql, [])?;
    }
    Ok(())
}

fn insert_batch_tx(tx: &Transaction<'_>, batch: &NewBatch, job_ids: &[String]) -> Result<()> {
    tx.execute(
        "INSERT INTO batches (
            batch_id, source_thread_id, state, replay_policy, summary,
            created_at, updated_at, redelivery_window_ends_at,
            max_delivery_attempts, delivery_attempt_count,
            delivery_read_only, delivery_requires_approval,
            delivery_requires_network, delivery_requires_write_access,
            inline_payload_bytes, requires_artifact_read
        ) VALUES (?, ?, 'open', 'automatic', ?, ?, ?, ?, ?, 0, ?, ?, ?, ?, ?, ?)",
        params![
            batch.batch_id,
            batch.source_thread_id,
            batch.summary,
            batch.created_at,
            batch.created_at,
            batch.redelivery_window_ends_at,
            batch.max_delivery_attempts,
            bool_to_i64(batch.policy.delivery_read_only),
            bool_to_i64(batch.policy.delivery_requires_approval),
            bool_to_i64(batch.policy.delivery_requires_network),
            bool_to_i64(batch.policy.delivery_requires_write_access),
            batch.inline_payload_bytes,
            bool_to_i64(batch.requires_artifact_read),
        ],
    )?;

    for (position, job_id) in job_ids.iter().enumerate() {
        tx.execute(
            "INSERT INTO batch_jobs (batch_id, job_id, position) VALUES (?, ?, ?)",
            params![batch.batch_id, job_id, position as i64],
        )?;
    }
    Ok(())
}

fn close_batch_tx(
    tx: &Transaction<'_>,
    batch_id: &str,
    reason: &str,
    note: Option<&str>,
    now: i64,
) -> Result<()> {
    let cli_session_fences = query_active_cli_attempt_session_fences_for_batch_tx(tx, batch_id)?;
    let changed = tx.execute(
        "UPDATE batches
         SET state = 'closed',
             close_reason = ?,
             close_note = ?,
             updated_at = ?,
             closed_at = ?
         WHERE batch_id = ? AND state = 'open'",
        params![reason, note, now, now, batch_id],
    )?;
    if changed == 0 {
        bail!("batch {batch_id} is not open");
    }
    tx.execute(
        "UPDATE delivery_attempts
         SET state = 'closed',
             updated_at = ?,
             closed_at = ?
         WHERE batch_id = ?
           AND state IN ('prepared', 'accept_pending', 'arm_pending', 'cooldown')",
        params![now, now, batch_id],
    )?;
    for (managed_session_id, session_epoch) in cli_session_fences {
        invalidate_cli_managed_session_activity_tx(
            tx,
            managed_session_id.as_deref(),
            session_epoch,
            now,
        )?;
    }
    Ok(())
}

fn query_active_cli_attempt_session_fences_for_batch_tx(
    tx: &Transaction<'_>,
    batch_id: &str,
) -> Result<Vec<(Option<String>, Option<i64>)>> {
    let mut stmt = tx.prepare(
        "SELECT managed_session_id, session_epoch
         FROM delivery_attempts
         WHERE batch_id = ?
           AND adapter_kind = 'cli'
           AND state IN ('prepared', 'accept_pending', 'arm_pending', 'cooldown')",
    )?;
    stmt.query_map(params![batch_id], |row| {
        Ok((
            row.get::<_, Option<String>>(0)?,
            row.get::<_, Option<i64>>(1)?,
        ))
    })?
    .collect::<rusqlite::Result<Vec<_>>>()
    .map_err(Into::into)
}

fn invalidate_cli_managed_session_activity_tx(
    tx: &Transaction<'_>,
    managed_session_id: Option<&str>,
    session_epoch: Option<i64>,
    now: i64,
) -> Result<()> {
    let (Some(managed_session_id), Some(session_epoch)) = (managed_session_id, session_epoch)
    else {
        return Ok(());
    };
    tx.execute(
        "UPDATE cli_managed_sessions
         SET session_epoch = session_epoch + 1,
             activity_state = 'unknown',
             activity_revision = 0,
             capability_revision = 0,
             capability_thread_resume = 0,
             capability_turn_start = 0,
             capability_current_state_sync = 0,
             capability_turn_completed_event = 0,
             capability_negative_terminal_events = 0,
             capability_thread_start = 0,
             capability_turn_steer = 0,
             updated_at = ?
         WHERE managed_session_id = ?
           AND session_epoch = ?
           AND session_state IN ('live', 'detached')",
        params![now, managed_session_id, session_epoch],
    )?;
    Ok(())
}

fn manualize_cli_batch_after_observation_loss_tx(
    tx: &Transaction<'_>,
    attempt: &DeliveryAttemptRecord,
    now: i64,
) -> Result<()> {
    let manual_resolution_window_ends_at = checked_timestamp_add(
        now,
        DEFAULT_REDELIVERY_WINDOW_SECONDS,
        "manual_resolution_window_seconds",
    )?;
    tx.execute(
        "UPDATE batches
         SET replay_policy = 'manual_resolution_only',
             redelivery_window_ends_at = max(redelivery_window_ends_at, ?),
             updated_at = ?
         WHERE batch_id = ?
           AND state = 'open'",
        params![manual_resolution_window_ends_at, now, attempt.batch_id],
    )?;
    invalidate_cli_managed_session_activity_tx(
        tx,
        attempt.managed_session_id.as_deref(),
        attempt.session_epoch,
        now,
    )
}

fn abandon_cli_observations_for_session_epoch_loss_tx(
    tx: &Transaction<'_>,
    managed_session_id: &str,
    now: i64,
) -> Result<()> {
    let manual_resolution_window_ends_at = checked_timestamp_add(
        now,
        DEFAULT_REDELIVERY_WINDOW_SECONDS,
        "manual_resolution_window_seconds",
    )?;
    let mut stmt = tx.prepare(
        "SELECT delivery_attempts.attempt_id, delivery_attempts.batch_id
         FROM delivery_attempts
         JOIN batches ON batches.batch_id = delivery_attempts.batch_id
         WHERE delivery_attempts.adapter_kind = 'cli'
           AND delivery_attempts.managed_session_id = ?
           AND batches.state = 'open'
           AND (
             (
               delivery_attempts.state = 'cooldown'
               AND delivery_attempts.delivery_observation_state = 'tracking'
             )
             OR (
               delivery_attempts.state = 'abandoned'
               AND delivery_attempts.delivery_observation_state = 'expired'
             )
           )",
    )?;
    let attempts = stmt
        .query_map(params![managed_session_id], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    drop(stmt);

    for (attempt_id, batch_id) in attempts {
        tx.execute(
            "UPDATE delivery_attempts
             SET state = 'abandoned',
                 delivery_observation_state = 'abandoned',
                 updated_at = ?,
                 abandoned_at = ?
             WHERE attempt_id = ?
               AND (
                 (
                   state = 'cooldown'
                   AND delivery_observation_state = 'tracking'
                 )
                 OR (
                   state = 'abandoned'
                   AND delivery_observation_state = 'expired'
                 )
               )",
            params![now, now, attempt_id],
        )?;
        tx.execute(
            "UPDATE batches
             SET replay_policy = 'manual_resolution_only',
                 redelivery_window_ends_at = max(redelivery_window_ends_at, ?),
                 updated_at = ?
             WHERE batch_id = ?
               AND state = 'open'",
            params![manual_resolution_window_ends_at, now, batch_id],
        )?;
    }
    Ok(())
}

fn abandon_cli_turn_observation_tx(
    tx: &Transaction<'_>,
    attempt: &DeliveryAttemptRecord,
    turn_event: &str,
    delivery_observation_state: &str,
    now: i64,
) -> Result<()> {
    tx.execute(
        "UPDATE delivery_attempts
         SET state = 'abandoned',
             delivery_observation_state = ?,
             last_observed_turn_event = ?,
             last_observed_turn_event_at = ?,
             updated_at = ?,
             abandoned_at = ?
         WHERE attempt_id = ?
           AND state = 'cooldown'
           AND delivery_observation_state = 'tracking'",
        params![
            delivery_observation_state,
            turn_event,
            now,
            now,
            now,
            attempt.attempt_id,
        ],
    )?;
    Ok(())
}

fn record_late_cli_turn_evidence_tx(
    tx: &Transaction<'_>,
    attempt: &DeliveryAttemptRecord,
    turn_event: &str,
    observed_at: i64,
) -> Result<()> {
    tx.execute(
        "UPDATE delivery_attempts
         SET last_observed_turn_event = ?,
             last_observed_turn_event_at = ?,
             updated_at = ?
         WHERE attempt_id = ?
           AND state = 'abandoned'
           AND (
             last_observed_turn_event IS NULL
             OR last_observed_turn_event = 'turn_started'
           )",
        params![turn_event, observed_at, observed_at, attempt.attempt_id],
    )?;
    Ok(())
}

fn complete_delayed_cli_turn_after_expiry_tx(
    tx: &Transaction<'_>,
    attempt: &DeliveryAttemptRecord,
    observed_at: i64,
) -> Result<Vec<ArtifactRecord>> {
    let batch = query_batch_tx(tx, &attempt.batch_id)?;
    ensure_batch_open(&batch)?;
    ensure_batch_is_thread_head_tx(tx, &batch)?;
    ensure_attempt_is_current_generation_tx(tx, attempt)?;
    let changed = tx.execute(
        "UPDATE delivery_attempts
         SET state = 'cooldown',
             delivery_observation_state = 'completed',
             last_observed_turn_event = 'turn_completed',
             last_observed_turn_event_at = ?,
             updated_at = ?,
             abandoned_at = NULL
         WHERE attempt_id = ?
           AND state = 'abandoned'
           AND delivery_observation_state IN ('expired', 'abandoned')",
        params![observed_at, observed_at, attempt.attempt_id],
    )?;
    if changed != 1 {
        bail!(
            "delivery attempt {} is no longer eligible for delayed completion",
            attempt.attempt_id
        );
    }
    close_batch_tx(
        tx,
        &attempt.batch_id,
        "delivered",
        Some("observed CLI turn completion"),
        observed_at,
    )?;
    extend_closed_batch_artifact_retention_tx(tx, &attempt.batch_id, observed_at)
}

fn extend_closed_batch_artifact_retention_tx(
    tx: &Transaction<'_>,
    batch_id: &str,
    now: i64,
) -> Result<Vec<ArtifactRecord>> {
    let retention_until = checked_timestamp_add(
        now,
        POST_CLOSE_ARTIFACT_TTL_SECONDS,
        "post_close_artifact_ttl",
    )?;
    tx.execute(
        "UPDATE artifacts
         SET retention_until = max(retention_until, ?)
         WHERE job_id IN (
            SELECT job_id FROM batch_jobs WHERE batch_id = ?
         )",
        params![retention_until, batch_id],
    )?;
    query_artifacts_for_batch_tx(tx, batch_id)
}

fn close_expired_manual_batches_tx(
    tx: &Transaction<'_>,
    now: i64,
) -> Result<(usize, Vec<ArtifactRecord>)> {
    let mut stmt = tx.prepare(
        "SELECT batch_id FROM batches
         WHERE state = 'open'
           AND replay_policy = 'manual_resolution_only'
           AND redelivery_window_ends_at <= ?
           AND NOT EXISTS (
             SELECT 1 FROM delivery_attempts
             WHERE delivery_attempts.batch_id = batches.batch_id
               AND delivery_attempts.state IN ('prepared', 'accept_pending', 'arm_pending', 'cooldown')
           )
         ORDER BY redelivery_window_ends_at ASC, batch_id ASC
         LIMIT ?",
    )?;
    let batch_ids = stmt
        .query_map(params![now, MAX_EXPIRED_BATCHES_PER_SWEEP], |row| {
            row.get::<_, String>(0)
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    drop(stmt);

    let mut artifacts_to_sync = Vec::new();
    for batch_id in &batch_ids {
        close_batch_tx(
            tx,
            batch_id,
            "manual_resolution_expired",
            Some("closed by maintenance sweep"),
            now,
        )?;
        artifacts_to_sync.extend(extend_closed_batch_artifact_retention_tx(
            tx, batch_id, now,
        )?);
    }
    Ok((batch_ids.len(), artifacts_to_sync))
}

fn count_open_batches_due_at(conn: &Connection, due_at: i64) -> Result<i64> {
    conn.query_row(
        "SELECT COUNT(*) FROM batches
         WHERE state = 'open'
           AND redelivery_window_ends_at <= ?
           AND NOT EXISTS (
             SELECT 1 FROM delivery_attempts
             WHERE delivery_attempts.batch_id = batches.batch_id
               AND delivery_attempts.state IN ('prepared', 'accept_pending', 'arm_pending', 'cooldown')
           )",
        params![due_at],
        |row| row.get::<_, i64>(0),
    )
    .context("count open batches due for daemon lifecycle")
}

fn count_active_cli_acceptances(conn: &Connection, now: i64) -> Result<i64> {
    let stale_started_at = now.saturating_sub(CLI_ACCEPT_PENDING_TIMEOUT_SECONDS);
    conn.query_row(
        "SELECT COUNT(*) FROM delivery_attempts
         JOIN batches ON batches.batch_id = delivery_attempts.batch_id
         WHERE delivery_attempts.adapter_kind = 'cli'
           AND delivery_attempts.state = 'accept_pending'
           AND delivery_attempts.delivery_rpc_state = 'pending_acceptance'
           AND delivery_attempts.delivery_rpc_started_at > ?
           AND batches.state = 'open'",
        params![stale_started_at],
        |row| row.get::<_, i64>(0),
    )
    .context("count active CLI accept-pending attempts for daemon lifecycle")
}

fn count_stale_cli_acceptances(conn: &Connection, now: i64) -> Result<i64> {
    let stale_started_at = now.saturating_sub(CLI_ACCEPT_PENDING_TIMEOUT_SECONDS);
    conn.query_row(
        "SELECT COUNT(*) FROM delivery_attempts
         JOIN batches ON batches.batch_id = delivery_attempts.batch_id
         WHERE delivery_attempts.adapter_kind = 'cli'
           AND delivery_attempts.state = 'accept_pending'
           AND delivery_attempts.delivery_rpc_state = 'pending_acceptance'
           AND delivery_attempts.delivery_rpc_started_at <= ?
           AND batches.state = 'open'",
        params![stale_started_at],
        |row| row.get::<_, i64>(0),
    )
    .context("count stale CLI accept-pending attempts for daemon lifecycle")
}

fn count_active_cli_observations(conn: &Connection, now: i64) -> Result<i64> {
    conn.query_row(
        "SELECT COUNT(*) FROM delivery_attempts
         JOIN batches ON batches.batch_id = delivery_attempts.batch_id
         WHERE delivery_attempts.adapter_kind = 'cli'
           AND delivery_attempts.state = 'cooldown'
           AND delivery_attempts.delivery_observation_state = 'tracking'
           AND delivery_attempts.delivery_observation_deadline > ?
           AND batches.state = 'open'",
        params![now],
        |row| row.get::<_, i64>(0),
    )
    .context("count active CLI delivery observations for daemon lifecycle")
}

fn count_cli_observations_due_now(conn: &Connection, now: i64) -> Result<i64> {
    conn.query_row(
        "SELECT COUNT(*) FROM delivery_attempts
         JOIN batches ON batches.batch_id = delivery_attempts.batch_id
         WHERE delivery_attempts.adapter_kind = 'cli'
           AND delivery_attempts.state = 'cooldown'
           AND delivery_attempts.delivery_observation_state = 'tracking'
           AND delivery_attempts.delivery_observation_deadline <= ?
           AND batches.state = 'open'",
        params![now],
        |row| row.get::<_, i64>(0),
    )
    .context("count due CLI delivery observations for daemon lifecycle")
}

fn expire_stale_cli_acceptances_tx(tx: &Transaction<'_>, now: i64) -> Result<usize> {
    let stale_started_at = now.saturating_sub(CLI_ACCEPT_PENDING_TIMEOUT_SECONDS);
    let manual_resolution_window_ends_at = checked_timestamp_add(
        now,
        DEFAULT_REDELIVERY_WINDOW_SECONDS,
        "manual_resolution_window_seconds",
    )?;
    let mut stmt = tx.prepare(
        "SELECT delivery_attempts.attempt_id, delivery_attempts.batch_id,
                delivery_attempts.managed_session_id, delivery_attempts.session_epoch
         FROM delivery_attempts
         JOIN batches ON batches.batch_id = delivery_attempts.batch_id
         WHERE delivery_attempts.adapter_kind = 'cli'
           AND delivery_attempts.state = 'accept_pending'
           AND delivery_attempts.delivery_rpc_state = 'pending_acceptance'
           AND delivery_attempts.delivery_rpc_started_at <= ?
           AND batches.state = 'open'
         ORDER BY delivery_attempts.delivery_rpc_started_at ASC,
                  delivery_attempts.attempt_id ASC
         LIMIT ?",
    )?;
    let attempts = stmt
        .query_map(
            params![stale_started_at, MAX_EXPIRED_BATCHES_PER_SWEEP],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, Option<String>>(2)?,
                    row.get::<_, Option<i64>>(3)?,
                ))
            },
        )?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    drop(stmt);

    for (attempt_id, batch_id, managed_session_id, session_epoch) in &attempts {
        tx.execute(
            "UPDATE delivery_attempts
             SET state = 'abandoned',
                 delivery_rpc_state = 'unknown',
                 delivery_observation_state = 'abandoned',
                 updated_at = ?,
                 abandoned_at = ?
             WHERE attempt_id = ?
               AND state = 'accept_pending'
               AND delivery_rpc_state = 'pending_acceptance'",
            params![now, now, attempt_id],
        )?;
        tx.execute(
            "UPDATE batches
             SET replay_policy = 'manual_resolution_only',
                 redelivery_window_ends_at = max(redelivery_window_ends_at, ?),
                 updated_at = ?
             WHERE batch_id = ?
               AND state = 'open'",
            params![manual_resolution_window_ends_at, now, batch_id],
        )?;
        invalidate_cli_managed_session_activity_tx(
            tx,
            managed_session_id.as_deref(),
            *session_epoch,
            now,
        )?;
    }
    Ok(attempts.len())
}

fn expire_due_cli_observations_tx(tx: &Transaction<'_>, now: i64) -> Result<usize> {
    let manual_resolution_window_ends_at = checked_timestamp_add(
        now,
        DEFAULT_REDELIVERY_WINDOW_SECONDS,
        "manual_resolution_window_seconds",
    )?;
    let mut stmt = tx.prepare(
        "SELECT delivery_attempts.attempt_id, delivery_attempts.batch_id,
                delivery_attempts.managed_session_id, delivery_attempts.session_epoch
         FROM delivery_attempts
         JOIN batches ON batches.batch_id = delivery_attempts.batch_id
         WHERE delivery_attempts.adapter_kind = 'cli'
           AND delivery_attempts.state = 'cooldown'
           AND delivery_attempts.delivery_observation_state = 'tracking'
           AND delivery_attempts.delivery_observation_deadline <= ?
           AND batches.state = 'open'
         ORDER BY delivery_attempts.delivery_observation_deadline ASC,
                  delivery_attempts.attempt_id ASC
         LIMIT ?",
    )?;
    let attempts = stmt
        .query_map(params![now, MAX_EXPIRED_BATCHES_PER_SWEEP], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, Option<String>>(2)?,
                row.get::<_, Option<i64>>(3)?,
            ))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    drop(stmt);

    for (attempt_id, batch_id, managed_session_id, session_epoch) in &attempts {
        let attempt = query_delivery_attempt_tx(tx, attempt_id)?;
        let batch = query_batch_tx(tx, batch_id)?;
        let expired_state =
            if ensure_cli_attempt_has_current_managed_session_for_batch_tx(tx, &attempt, &batch)
                .is_ok()
            {
                "expired"
            } else {
                "abandoned"
            };
        tx.execute(
            "UPDATE delivery_attempts
             SET state = 'abandoned',
                 delivery_observation_state = ?,
                 updated_at = ?,
                 abandoned_at = ?
             WHERE attempt_id = ?
               AND state = 'cooldown'
               AND delivery_observation_state = 'tracking'",
            params![expired_state, now, now, attempt_id],
        )?;
        tx.execute(
            "UPDATE batches
             SET replay_policy = 'manual_resolution_only',
                 redelivery_window_ends_at = max(redelivery_window_ends_at, ?),
                 updated_at = ?
             WHERE batch_id = ?
               AND state = 'open'",
            params![manual_resolution_window_ends_at, now, batch_id],
        )?;
        invalidate_cli_managed_session_activity_tx(
            tx,
            managed_session_id.as_deref(),
            *session_epoch,
            now,
        )?;
    }
    Ok(attempts.len())
}

fn close_expired_automatic_batches_tx(
    tx: &Transaction<'_>,
    now: i64,
) -> Result<(usize, Vec<ArtifactRecord>)> {
    let mut stmt = tx.prepare(
        "SELECT batch_id FROM batches
         WHERE state = 'open'
           AND replay_policy = 'automatic'
           AND redelivery_window_ends_at <= ?
           AND NOT EXISTS (
             SELECT 1 FROM delivery_attempts
             WHERE delivery_attempts.batch_id = batches.batch_id
               AND delivery_attempts.state IN ('prepared', 'accept_pending', 'arm_pending', 'cooldown')
           )
         ORDER BY redelivery_window_ends_at ASC, batch_id ASC
         LIMIT ?",
    )?;
    let batch_ids = stmt
        .query_map(params![now, MAX_EXPIRED_BATCHES_PER_SWEEP], |row| {
            row.get::<_, String>(0)
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    drop(stmt);

    let mut artifacts_to_sync = Vec::new();
    for batch_id in &batch_ids {
        close_batch_tx(
            tx,
            batch_id,
            "redelivery_window_exhausted",
            Some("closed by maintenance sweep"),
            now,
        )?;
        artifacts_to_sync.extend(extend_closed_batch_artifact_retention_tx(
            tx, batch_id, now,
        )?);
    }
    Ok((batch_ids.len(), artifacts_to_sync))
}

fn query_deletable_artifacts_tx(tx: &Transaction<'_>, now: i64) -> Result<Vec<ArtifactRecord>> {
    let mut stmt = tx.prepare(
        "SELECT artifacts.*
         FROM artifacts
         WHERE artifacts.retention_until <= ?
           AND NOT EXISTS (
             SELECT 1
             FROM batch_jobs
             JOIN batches ON batches.batch_id = batch_jobs.batch_id
             WHERE batch_jobs.job_id = artifacts.job_id
               AND batches.state = 'open'
         )
         ORDER BY artifacts.gc_attempted_at ASC, artifacts.retention_until ASC, artifacts.artifact_id ASC
         LIMIT ?",
    )?;
    let records = stmt
        .query_map(
            params![now, MAX_DELETABLE_ARTIFACTS_PER_SWEEP],
            artifact_from_row,
        )?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(records)
}

fn query_artifacts_for_manifest_sync_tx(tx: &Transaction<'_>) -> Result<Vec<ArtifactRecord>> {
    let mut stmt = tx.prepare(
        "SELECT *
         FROM artifacts
         WHERE manifest_synced_retention_until < retention_until
         ORDER BY manifest_sync_attempted_at ASC, retention_until ASC, artifact_id ASC
         LIMIT ?",
    )?;
    let records = stmt
        .query_map(params![MAX_MANIFEST_SYNCS_PER_SWEEP], artifact_from_row)?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(records)
}

fn query_job(conn: &Connection, job_id: &str) -> Result<JobRecord> {
    conn.query_row(
        "SELECT * FROM jobs WHERE job_id = ?",
        params![job_id],
        job_from_row,
    )
    .optional()?
    .ok_or_else(|| anyhow!("job not found: {job_id}"))
}

fn query_job_tx(tx: &Transaction<'_>, job_id: &str) -> Result<JobRecord> {
    tx.query_row(
        "SELECT * FROM jobs WHERE job_id = ?",
        params![job_id],
        job_from_row,
    )
    .optional()?
    .ok_or_else(|| anyhow!("job not found: {job_id}"))
}

fn query_batch_inspect(conn: &Connection, batch_id: &str) -> Result<BatchInspect> {
    let batch = query_batch(conn, batch_id)?;
    let jobs = query_batch_jobs(conn, batch_id)?;
    Ok(BatchInspect { batch, jobs })
}

fn query_batch_inspect_tx(tx: &Transaction<'_>, batch_id: &str) -> Result<BatchInspect> {
    let batch = query_batch_tx(tx, batch_id)?;
    let jobs = query_batch_jobs_tx(tx, batch_id)?;
    Ok(BatchInspect { batch, jobs })
}

fn query_batch(conn: &Connection, batch_id: &str) -> Result<BatchRecord> {
    conn.query_row(
        "SELECT * FROM batches WHERE batch_id = ?",
        params![batch_id],
        batch_from_row,
    )
    .optional()?
    .ok_or_else(|| anyhow!("batch not found: {batch_id}"))
}

fn query_batch_tx(tx: &Transaction<'_>, batch_id: &str) -> Result<BatchRecord> {
    tx.query_row(
        "SELECT * FROM batches WHERE batch_id = ?",
        params![batch_id],
        batch_from_row,
    )
    .optional()?
    .ok_or_else(|| anyhow!("batch not found: {batch_id}"))
}

fn query_cli_managed_session(
    conn: &Connection,
    managed_session_id: &str,
) -> Result<CliManagedSessionRecord> {
    conn.query_row(
        "SELECT * FROM cli_managed_sessions WHERE managed_session_id = ?",
        params![managed_session_id],
        cli_managed_session_from_row,
    )
    .optional()?
    .ok_or_else(|| anyhow!("CLI managed session not found: {managed_session_id}"))
}

fn query_cli_managed_session_tx(
    tx: &Transaction<'_>,
    managed_session_id: &str,
) -> Result<CliManagedSessionRecord> {
    tx.query_row(
        "SELECT * FROM cli_managed_sessions WHERE managed_session_id = ?",
        params![managed_session_id],
        cli_managed_session_from_row,
    )
    .optional()?
    .ok_or_else(|| anyhow!("CLI managed session not found: {managed_session_id}"))
}

fn query_non_retired_cli_managed_session_by_thread_tx(
    tx: &Transaction<'_>,
    bound_thread_id: &str,
) -> Result<Option<CliManagedSessionRecord>> {
    tx.query_row(
        "SELECT * FROM cli_managed_sessions
         WHERE bound_thread_id = ?
           AND session_state != 'retired'
         ORDER BY created_at DESC, managed_session_id DESC
         LIMIT 1",
        params![bound_thread_id],
        cli_managed_session_from_row,
    )
    .optional()
    .map_err(Into::into)
}

fn query_delivery_attempt(conn: &Connection, attempt_id: &str) -> Result<DeliveryAttemptRecord> {
    conn.query_row(
        "SELECT * FROM delivery_attempts WHERE attempt_id = ?",
        params![attempt_id],
        delivery_attempt_from_row,
    )
    .optional()?
    .ok_or_else(|| anyhow!("delivery attempt not found: {attempt_id}"))
}

fn query_delivery_attempt_tx(
    tx: &Transaction<'_>,
    attempt_id: &str,
) -> Result<DeliveryAttemptRecord> {
    tx.query_row(
        "SELECT * FROM delivery_attempts WHERE attempt_id = ?",
        params![attempt_id],
        delivery_attempt_from_row,
    )
    .optional()?
    .ok_or_else(|| anyhow!("delivery attempt not found: {attempt_id}"))
}

fn query_delivery_attempt_by_rpc_request_id_tx(
    tx: &Transaction<'_>,
    delivery_rpc_request_id: &str,
) -> Result<Option<DeliveryAttemptRecord>> {
    tx.query_row(
        "SELECT * FROM delivery_attempts WHERE delivery_rpc_request_id = ?",
        params![delivery_rpc_request_id],
        delivery_attempt_from_row,
    )
    .optional()
    .map_err(Into::into)
}

fn query_audit_decision_tx(tx: &Transaction<'_>, audit_id: &str) -> Result<AuditDecisionRecord> {
    tx.query_row(
        "SELECT * FROM audit_decisions WHERE audit_id = ?",
        params![audit_id],
        audit_decision_from_row,
    )
    .optional()?
    .ok_or_else(|| anyhow!("audit decision not found: {audit_id}"))
}

fn ensure_no_active_attempt_for_thread_tx(
    tx: &Transaction<'_>,
    source_thread_id: &str,
) -> Result<()> {
    let active: Option<String> = tx
        .query_row(
            "SELECT delivery_attempts.attempt_id
             FROM delivery_attempts
             JOIN batches ON batches.batch_id = delivery_attempts.batch_id
             WHERE delivery_attempts.source_thread_id = ?
               AND delivery_attempts.state IN ('prepared', 'accept_pending', 'arm_pending', 'cooldown')
               AND batches.state = 'open'
             ORDER BY delivery_attempts.generation DESC, delivery_attempts.attempt_id DESC
             LIMIT 1",
            params![source_thread_id],
            |row| row.get(0),
        )
        .optional()?;
    if let Some(attempt_id) = active {
        bail!("thread {source_thread_id} already has active delivery attempt {attempt_id}");
    }
    Ok(())
}

fn ensure_batch_is_thread_head_tx(tx: &Transaction<'_>, batch: &BatchRecord) -> Result<()> {
    let head_batch_id: Option<String> = tx
        .query_row(
            "SELECT batch_id FROM batches
             WHERE source_thread_id = ?
               AND state = 'open'
             ORDER BY created_at ASC, batch_id ASC
             LIMIT 1",
            params![batch.source_thread_id],
            |row| row.get(0),
        )
        .optional()?;
    if head_batch_id.as_deref() == Some(batch.batch_id.as_str()) {
        Ok(())
    } else {
        bail!(
            "batch {} is not the head batch for thread {}",
            batch.batch_id,
            batch.source_thread_id
        )
    }
}

fn next_attempt_generation_tx(tx: &Transaction<'_>, batch_id: &str) -> Result<i64> {
    let generation: Option<i64> = tx.query_row(
        "SELECT MAX(generation) FROM delivery_attempts WHERE batch_id = ?",
        params![batch_id],
        |row| row.get(0),
    )?;
    generation
        .unwrap_or(0)
        .checked_add(1)
        .ok_or_else(|| anyhow!("delivery attempt generation overflow for batch {batch_id}"))
}

fn ensure_attempt_is_current_generation_tx(
    tx: &Transaction<'_>,
    attempt: &DeliveryAttemptRecord,
) -> Result<()> {
    let current = tx.query_row(
        "SELECT MAX(generation) FROM delivery_attempts WHERE batch_id = ?",
        params![attempt.batch_id],
        |row| row.get::<_, Option<i64>>(0),
    )?;
    if current == Some(attempt.generation) {
        Ok(())
    } else {
        bail!(
            "delivery attempt {} is not current for batch {}",
            attempt.attempt_id,
            attempt.batch_id
        )
    }
}

fn query_batch_jobs(conn: &Connection, batch_id: &str) -> Result<Vec<BatchJobRecord>> {
    let mut stmt = conn.prepare(
        "SELECT jobs.*
         FROM batch_jobs
         JOIN jobs ON jobs.job_id = batch_jobs.job_id
         WHERE batch_jobs.batch_id = ?
         ORDER BY batch_jobs.position ASC",
    )?;
    let jobs = stmt
        .query_map(params![batch_id], job_from_row)?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    jobs.into_iter()
        .map(|job| {
            let artifact = query_artifact_for_job(conn, &job.job_id)?;
            Ok(BatchJobRecord { job, artifact })
        })
        .collect()
}

fn query_batch_jobs_tx(tx: &Transaction<'_>, batch_id: &str) -> Result<Vec<BatchJobRecord>> {
    let mut stmt = tx.prepare(
        "SELECT jobs.*
         FROM batch_jobs
         JOIN jobs ON jobs.job_id = batch_jobs.job_id
         WHERE batch_jobs.batch_id = ?
         ORDER BY batch_jobs.position ASC",
    )?;
    let jobs = stmt
        .query_map(params![batch_id], job_from_row)?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    drop(stmt);

    jobs.into_iter()
        .map(|job| {
            let artifact = query_artifact_for_job_tx(tx, &job.job_id)?;
            Ok(BatchJobRecord { job, artifact })
        })
        .collect()
}

fn query_artifacts_for_batch_tx(
    tx: &Transaction<'_>,
    batch_id: &str,
) -> Result<Vec<ArtifactRecord>> {
    let mut stmt = tx.prepare(
        "SELECT artifacts.*
         FROM batch_jobs
         JOIN artifacts ON artifacts.job_id = batch_jobs.job_id
         WHERE batch_jobs.batch_id = ?
         ORDER BY batch_jobs.position ASC",
    )?;
    let artifacts = stmt
        .query_map(params![batch_id], artifact_from_row)?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(artifacts)
}

fn query_artifact_for_job(conn: &Connection, job_id: &str) -> Result<Option<ArtifactRecord>> {
    conn.query_row(
        "SELECT * FROM artifacts WHERE job_id = ?",
        params![job_id],
        artifact_from_row,
    )
    .optional()
    .map_err(Into::into)
}

fn query_artifact_for_job_tx(tx: &Transaction<'_>, job_id: &str) -> Result<Option<ArtifactRecord>> {
    tx.query_row(
        "SELECT * FROM artifacts WHERE job_id = ?",
        params![job_id],
        artifact_from_row,
    )
    .optional()
    .map_err(Into::into)
}

fn query_artifact_by_id_tx(
    tx: &Transaction<'_>,
    artifact_id: &str,
) -> Result<Option<ArtifactRecord>> {
    tx.query_row(
        "SELECT * FROM artifacts WHERE artifact_id = ?",
        params![artifact_id],
        artifact_from_row,
    )
    .optional()
    .map_err(Into::into)
}

fn rows_to_jobs(mut rows: rusqlite::Rows<'_>) -> Result<Vec<JobRecord>> {
    let mut jobs = Vec::new();
    while let Some(row) = rows.next()? {
        jobs.push(job_from_row(row)?);
    }
    Ok(jobs)
}

fn rows_to_audit_decisions(mut rows: rusqlite::Rows<'_>) -> Result<Vec<AuditDecisionRecord>> {
    let mut decisions = Vec::new();
    while let Some(row) = rows.next()? {
        decisions.push(audit_decision_from_row(row)?);
    }
    Ok(decisions)
}

fn job_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<JobRecord> {
    let metadata_json: String = row.get("metadata_json")?;
    let metadata = serde_json::from_str(&metadata_json).unwrap_or(serde_json::Value::Null);
    Ok(JobRecord {
        job_id: row.get("job_id")?,
        source_thread_id: row.get("source_thread_id")?,
        status: row.get("status")?,
        summary: row.get("summary")?,
        metadata,
        created_at: row.get("created_at")?,
        updated_at: row.get("updated_at")?,
        completed_at: row.get("completed_at")?,
        failed_at: row.get("failed_at")?,
        result_artifact_id: row.get("result_artifact_id")?,
        failure_reason: row.get("failure_reason")?,
        delivery_policy: DeliveryPolicy {
            delivery_read_only: row.get::<_, i64>("delivery_read_only")? != 0,
            delivery_requires_approval: row.get::<_, i64>("delivery_requires_approval")? != 0,
            delivery_requires_network: row.get::<_, i64>("delivery_requires_network")? != 0,
            delivery_requires_write_access: row.get::<_, i64>("delivery_requires_write_access")?
                != 0,
        },
    })
}

fn artifact_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<ArtifactRecord> {
    Ok(ArtifactRecord {
        artifact_id: row.get("artifact_id")?,
        job_id: row.get("job_id")?,
        relative_path: row.get("relative_path")?,
        original_filename: row.get("original_filename")?,
        size_bytes: row.get("size_bytes")?,
        sha256: row.get("sha256")?,
        created_at: row.get("created_at")?,
        retention_until: row.get("retention_until")?,
        manifest_synced_retention_until: row.get("manifest_synced_retention_until")?,
        manifest_sync_attempted_at: row.get("manifest_sync_attempted_at")?,
        gc_attempted_at: row.get("gc_attempted_at")?,
    })
}

fn batch_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<BatchRecord> {
    Ok(BatchRecord {
        batch_id: row.get("batch_id")?,
        source_thread_id: row.get("source_thread_id")?,
        state: row.get("state")?,
        replay_policy: row.get("replay_policy")?,
        close_reason: row.get("close_reason")?,
        close_note: row.get("close_note")?,
        summary: row.get("summary")?,
        created_at: row.get("created_at")?,
        updated_at: row.get("updated_at")?,
        closed_at: row.get("closed_at")?,
        redelivery_window_ends_at: row.get("redelivery_window_ends_at")?,
        max_delivery_attempts: row.get("max_delivery_attempts")?,
        delivery_attempt_count: row.get("delivery_attempt_count")?,
        delivery_policy: DeliveryPolicy {
            delivery_read_only: row.get::<_, i64>("delivery_read_only")? != 0,
            delivery_requires_approval: row.get::<_, i64>("delivery_requires_approval")? != 0,
            delivery_requires_network: row.get::<_, i64>("delivery_requires_network")? != 0,
            delivery_requires_write_access: row.get::<_, i64>("delivery_requires_write_access")?
                != 0,
        },
        inline_payload_bytes: row.get("inline_payload_bytes")?,
        requires_artifact_read: row.get::<_, i64>("requires_artifact_read")? != 0,
    })
}

fn cli_managed_session_from_row(
    row: &rusqlite::Row<'_>,
) -> rusqlite::Result<CliManagedSessionRecord> {
    Ok(CliManagedSessionRecord {
        managed_session_id: row.get("managed_session_id")?,
        bound_thread_id: row.get("bound_thread_id")?,
        session_epoch: row.get("session_epoch")?,
        session_state: row.get("session_state")?,
        activity_state: row.get("activity_state")?,
        activity_revision: row.get("activity_revision")?,
        capability_revision: row.get("capability_revision")?,
        capability_thread_resume: row.get::<_, i64>("capability_thread_resume")? != 0,
        capability_turn_start: row.get::<_, i64>("capability_turn_start")? != 0,
        capability_current_state_sync: row.get::<_, i64>("capability_current_state_sync")? != 0,
        capability_turn_completed_event: row.get::<_, i64>("capability_turn_completed_event")? != 0,
        capability_negative_terminal_events: row
            .get::<_, i64>("capability_negative_terminal_events")?
            != 0,
        capability_thread_start: row.get::<_, i64>("capability_thread_start")? != 0,
        capability_turn_steer: row.get::<_, i64>("capability_turn_steer")? != 0,
        session_allows_approval: row.get::<_, i64>("session_allows_approval")? != 0,
        session_allows_network: row.get::<_, i64>("session_allows_network")? != 0,
        session_allows_write_access: row.get::<_, i64>("session_allows_write_access")? != 0,
        created_at: row.get("created_at")?,
        updated_at: row.get("updated_at")?,
        retired_at: row.get("retired_at")?,
    })
}

fn delivery_attempt_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<DeliveryAttemptRecord> {
    Ok(DeliveryAttemptRecord {
        attempt_id: row.get("attempt_id")?,
        batch_id: row.get("batch_id")?,
        source_thread_id: row.get("source_thread_id")?,
        adapter_kind: row.get("adapter_kind")?,
        authorization_mode: row.get("authorization_mode")?,
        state: row.get("state")?,
        generation: row.get("generation")?,
        delivery_rpc_request_id: row.get("delivery_rpc_request_id")?,
        delivery_rpc_kind: row.get("delivery_rpc_kind")?,
        delivery_rpc_state: row.get("delivery_rpc_state")?,
        delivery_rpc_correlation_marker: row.get("delivery_rpc_correlation_marker")?,
        delivery_rpc_started_at: row.get("delivery_rpc_started_at")?,
        managed_session_id: row.get("managed_session_id")?,
        session_epoch: row.get("session_epoch")?,
        session_activity_revision: row.get("session_activity_revision")?,
        session_capability_revision: row.get("session_capability_revision")?,
        delivery_turn_id: row.get("delivery_turn_id")?,
        delivery_accepted_at: row.get("delivery_accepted_at")?,
        delivery_observation_state: row.get("delivery_observation_state")?,
        delivery_observation_deadline: row.get("delivery_observation_deadline")?,
        last_observed_turn_event: row.get("last_observed_turn_event")?,
        last_observed_turn_event_at: row.get("last_observed_turn_event_at")?,
        created_at: row.get("created_at")?,
        updated_at: row.get("updated_at")?,
        abandoned_at: row.get("abandoned_at")?,
        closed_at: row.get("closed_at")?,
    })
}

fn audit_decision_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<AuditDecisionRecord> {
    let details_json: String = row.get("details_json")?;
    let details = serde_json::from_str(&details_json).unwrap_or(serde_json::Value::Null);
    Ok(AuditDecisionRecord {
        audit_id: row.get("audit_id")?,
        recorded_at: row.get("recorded_at")?,
        source_thread_id: row.get("source_thread_id")?,
        batch_id: row.get("batch_id")?,
        attempt_id: row.get("attempt_id")?,
        managed_session_id: row.get("managed_session_id")?,
        session_epoch: row.get("session_epoch")?,
        policy_kind: row.get("policy_kind")?,
        decision: row.get("decision")?,
        reason: row.get("reason")?,
        adapter_kind: row.get("adapter_kind")?,
        details,
    })
}

fn ensure_job_pending(job: &JobRecord) -> Result<()> {
    if job.status == "pending" {
        Ok(())
    } else {
        bail!("job {} is already {}", job.job_id, job.status)
    }
}

fn ensure_batch_open(batch: &BatchRecord) -> Result<()> {
    if batch.state == "open" {
        Ok(())
    } else {
        bail!("batch {} is already {}", batch.batch_id, batch.state)
    }
}

fn ensure_batch_allows_automatic_delivery(batch: &BatchRecord) -> Result<()> {
    if batch.replay_policy == "automatic" {
        Ok(())
    } else {
        bail!(
            "batch {} replay policy is {}",
            batch.batch_id,
            batch.replay_policy
        )
    }
}

fn ensure_batch_allows_detached_cli_delivery(batch: &BatchRecord) -> Result<()> {
    let policy = &batch.delivery_policy;
    if policy.delivery_read_only
        && !policy.delivery_requires_approval
        && !policy.delivery_requires_network
        && !policy.delivery_requires_write_access
        && !batch.requires_artifact_read
    {
        Ok(())
    } else {
        bail!(
            "batch {} is not eligible for detached CLI delivery",
            batch.batch_id
        )
    }
}

fn ensure_batch_allows_cli_delivery_for_authorization(
    batch: &BatchRecord,
    authorization_mode: &str,
) -> Result<()> {
    match authorization_mode {
        "strict_safe" => ensure_batch_allows_detached_cli_delivery(batch),
        "trusted_all" => Ok(()),
        other => bail!("unsupported CLI attempt authorization mode {other}"),
    }
}

fn ensure_cli_session_profile_matches(
    session: &CliManagedSessionRecord,
    profile: &CliManagedSessionProfile,
) -> Result<()> {
    if session.session_allows_approval == profile.session_allows_approval
        && session.session_allows_network == profile.session_allows_network
        && session.session_allows_write_access == profile.session_allows_write_access
    {
        Ok(())
    } else {
        bail!(
            "CLI managed session {} profile does not match requested profile",
            session.managed_session_id
        )
    }
}

fn ensure_cli_session_attachable(session: &CliManagedSessionRecord) -> Result<()> {
    match session.session_state.as_str() {
        "live" | "detached" => Ok(()),
        "stale" => bail!(
            "CLI managed session {} is stale",
            session.managed_session_id
        ),
        "parked" => bail!(
            "CLI managed session {} is pending manual resolution",
            session.managed_session_id
        ),
        "retired" => bail!(
            "CLI managed session {} is retired",
            session.managed_session_id
        ),
        other => bail!(
            "CLI managed session {} has unsupported state {other}",
            session.managed_session_id
        ),
    }
}

fn ensure_cli_session_activity_value(activity_state: &str) -> Result<()> {
    match activity_state {
        "active" | "idle" => Ok(()),
        other => bail!("unsupported CLI managed session activity state {other}"),
    }
}

fn cli_session_proof_is_clear(session: &CliManagedSessionRecord) -> bool {
    session.activity_state == "unknown"
        && session.activity_revision == 0
        && session.capability_revision == 0
        && !session.capability_thread_resume
        && !session.capability_turn_start
        && !session.capability_current_state_sync
        && !session.capability_turn_completed_event
        && !session.capability_negative_terminal_events
        && !session.capability_thread_start
        && !session.capability_turn_steer
}

fn ensure_cli_session_activity_revision_can_advance(
    session: &CliManagedSessionRecord,
    activity_state: &str,
    activity_revision: i64,
) -> Result<()> {
    if activity_revision == session.activity_revision && activity_state == session.activity_state {
        return Ok(());
    }
    if session.activity_revision.checked_add(1) == Some(activity_revision) {
        return Ok(());
    }
    bail!(
        "CLI managed session {} activity revision {} is not the next revision after {}",
        session.managed_session_id,
        activity_revision,
        session.activity_revision
    )
}

fn ensure_cli_session_capability_revision_can_advance(
    session: &CliManagedSessionRecord,
    capabilities: &CliManagedSessionCapabilities,
    capability_revision: i64,
) -> Result<()> {
    if capability_revision == session.capability_revision
        && cli_session_capabilities_match(session, capabilities)
    {
        return Ok(());
    }
    if session
        .capability_revision
        .checked_add(1)
        .is_some_and(|next_revision| next_revision == capability_revision)
    {
        return Ok(());
    }
    bail!(
        "CLI managed session {} capability revision {} is not the next revision after {}",
        session.managed_session_id,
        capability_revision,
        session.capability_revision
    )
}

fn cli_session_capabilities_match(
    session: &CliManagedSessionRecord,
    capabilities: &CliManagedSessionCapabilities,
) -> bool {
    session.capability_thread_resume == capabilities.capability_thread_resume
        && session.capability_turn_start == capabilities.capability_turn_start
        && session.capability_current_state_sync == capabilities.capability_current_state_sync
        && session.capability_turn_completed_event == capabilities.capability_turn_completed_event
        && session.capability_negative_terminal_events
            == capabilities.capability_negative_terminal_events
        && session.capability_thread_start == capabilities.capability_thread_start
        && session.capability_turn_steer == capabilities.capability_turn_steer
}

fn ensure_cli_session_has_minimum_turn_start_capabilities(
    session: &CliManagedSessionRecord,
) -> Result<()> {
    if session.capability_thread_resume
        && session.capability_turn_start
        && session.capability_current_state_sync
        && session.capability_turn_completed_event
        && session.capability_negative_terminal_events
    {
        Ok(())
    } else {
        bail!(
            "CLI managed session {} has not passed the minimum turn_start capability probe",
            session.managed_session_id
        )
    }
}

fn ensure_positive_value(name: &str, value: i64) -> Result<()> {
    if value > 0 {
        Ok(())
    } else {
        bail!("{name} must be positive")
    }
}

fn ensure_nonempty_value(name: &str, value: &str) -> Result<()> {
    if value.is_empty() {
        bail!("{name} must not be empty")
    } else {
        Ok(())
    }
}

fn ensure_cli_session_epoch_matches(
    session: &CliManagedSessionRecord,
    session_epoch: i64,
) -> Result<()> {
    if session.session_epoch == session_epoch {
        Ok(())
    } else {
        bail!(
            "CLI managed session {} is at epoch {}, not {}",
            session.managed_session_id,
            session.session_epoch,
            session_epoch
        )
    }
}

fn ensure_cli_session_allows_delivery(
    session: &CliManagedSessionRecord,
    source_thread_id: &str,
    session_epoch: i64,
    delivery_rpc_kind: &str,
    authorization_mode: &str,
) -> Result<()> {
    ensure_cli_session_identity_allows_delivery(
        session,
        source_thread_id,
        session_epoch,
        authorization_mode,
    )?;
    match delivery_rpc_kind {
        "turn_start" => {
            ensure_cli_session_has_minimum_turn_start_capabilities(session)?;
            if session.activity_state == "idle" {
                Ok(())
            } else {
                bail!(
                    "CLI managed session {} activity state is {}, not idle",
                    session.managed_session_id,
                    session.activity_state
                )
            }
        }
        "turn_steer" => bail!(
            "CLI turn_steer delivery requires active-turn risk proof, which is not implemented"
        ),
        other => bail!("unsupported CLI delivery RPC kind {other}"),
    }
}

fn ensure_cli_session_identity_allows_delivery(
    session: &CliManagedSessionRecord,
    source_thread_id: &str,
    session_epoch: i64,
    authorization_mode: &str,
) -> Result<()> {
    ensure_cli_attempt_authorization_mode_value(authorization_mode)?;
    if session.bound_thread_id != source_thread_id {
        bail!(
            "CLI managed session {} is bound to thread {}, not {}",
            session.managed_session_id,
            session.bound_thread_id,
            source_thread_id
        )
    }
    ensure_cli_session_epoch_matches(session, session_epoch)?;
    match session.session_state.as_str() {
        "live" | "detached" => {}
        other => bail!(
            "CLI managed session {} is not live or detached; state is {other}",
            session.managed_session_id
        ),
    }
    if authorization_mode == "strict_safe" {
        if !session.session_allows_approval
            && !session.session_allows_network
            && !session.session_allows_write_access
        {
            // Continue to the rpc-kind-specific activity proof below.
        } else {
            bail!(
                "CLI managed session {} is not eligible for detached delivery",
                session.managed_session_id
            )
        }
    }
    Ok(())
}

fn ensure_cli_attempt_has_current_managed_session_tx(
    tx: &Transaction<'_>,
    attempt: &DeliveryAttemptRecord,
) -> Result<()> {
    let batch = query_batch_tx(tx, &attempt.batch_id)?;
    ensure_cli_attempt_has_current_managed_session_for_batch_tx(tx, attempt, &batch)
}

fn ensure_cli_attempt_has_current_managed_session_for_batch_tx(
    tx: &Transaction<'_>,
    attempt: &DeliveryAttemptRecord,
    batch: &BatchRecord,
) -> Result<()> {
    let managed_session_id = attempt.managed_session_id.as_deref().ok_or_else(|| {
        anyhow!(
            "delivery attempt {} is missing a CLI managed session",
            attempt.attempt_id
        )
    })?;
    let session_epoch = attempt.session_epoch.ok_or_else(|| {
        anyhow!(
            "delivery attempt {} is missing a CLI session epoch",
            attempt.attempt_id
        )
    })?;
    let session = query_cli_managed_session_tx(tx, managed_session_id)?;
    ensure_cli_session_identity_allows_delivery(
        &session,
        &batch.source_thread_id,
        session_epoch,
        &attempt.authorization_mode,
    )?;
    ensure_cli_attempt_has_recorded_detached_delivery_proof(attempt, &session)
}

fn ensure_cli_attempt_has_recorded_detached_delivery_proof(
    attempt: &DeliveryAttemptRecord,
    session: &CliManagedSessionRecord,
) -> Result<()> {
    match ensure_cli_attempt_has_recorded_detached_delivery_proof_snapshot(attempt)? {
        "turn_start" => {
            if attempt.session_capability_revision > session.capability_revision {
                bail!(
                    "delivery attempt {} references CLI capability revision {}, but session {} is at revision {}",
                    attempt.attempt_id,
                    attempt.session_capability_revision,
                    session.managed_session_id,
                    session.capability_revision
                );
            }
            ensure_cli_session_has_minimum_turn_start_capabilities(session)
        }
        "turn_steer" => bail!(
            "CLI turn_steer delivery requires active-turn risk proof, which is not implemented"
        ),
        other => bail!("unsupported CLI delivery RPC kind {other}"),
    }
}

fn ensure_cli_attempt_has_recorded_detached_delivery_proof_snapshot(
    attempt: &DeliveryAttemptRecord,
) -> Result<&str> {
    let delivery_rpc_kind = attempt.delivery_rpc_kind.as_deref().ok_or_else(|| {
        anyhow!(
            "delivery attempt {} is missing a CLI delivery RPC kind",
            attempt.attempt_id
        )
    })?;
    match delivery_rpc_kind {
        "turn_start" => {
            if attempt.session_activity_revision <= 0 || attempt.session_capability_revision <= 0 {
                bail!(
                    "delivery attempt {} was not created with a CLI detached delivery proof",
                    attempt.attempt_id
                );
            }
            Ok(delivery_rpc_kind)
        }
        "turn_steer" => bail!(
            "CLI turn_steer delivery requires active-turn risk proof, which is not implemented"
        ),
        other => bail!("unsupported CLI delivery RPC kind {other}"),
    }
}

fn ensure_existing_cli_accept_matches_request(
    existing: &DeliveryAttemptRecord,
    attempt: &NewCliAcceptPendingAttempt,
) -> Result<()> {
    if existing.batch_id != attempt.batch_id {
        bail!(
            "delivery RPC request {} already belongs to batch {}",
            attempt.delivery_rpc_request_id,
            existing.batch_id
        );
    }
    if existing.managed_session_id.as_deref() != Some(attempt.managed_session_id.as_str()) {
        bail!(
            "delivery RPC request {} already belongs to a different managed session",
            attempt.delivery_rpc_request_id
        );
    }
    if existing.session_epoch != Some(attempt.session_epoch) {
        bail!(
            "delivery RPC request {} already belongs to a different session epoch",
            attempt.delivery_rpc_request_id
        );
    }
    if existing.delivery_rpc_kind.as_deref() != Some(attempt.delivery_rpc_kind.as_str()) {
        bail!(
            "delivery RPC request {} already belongs to a different RPC kind",
            attempt.delivery_rpc_request_id
        );
    }
    if existing.authorization_mode != attempt.authorization_mode {
        bail!(
            "delivery RPC request {} already belongs to a different authorization mode",
            attempt.delivery_rpc_request_id
        );
    }
    Ok(())
}

fn ensure_attempt_budget_remaining(batch: &BatchRecord) -> Result<()> {
    if batch.delivery_attempt_count < batch.max_delivery_attempts {
        Ok(())
    } else {
        bail!("batch {} has exhausted delivery attempts", batch.batch_id)
    }
}

fn ensure_attempt_accept_pending(attempt: &DeliveryAttemptRecord) -> Result<()> {
    if attempt.adapter_kind == "cli"
        && attempt.state == "accept_pending"
        && attempt.delivery_rpc_state.as_deref() == Some("pending_acceptance")
    {
        Ok(())
    } else {
        bail!(
            "delivery attempt {} is not a pending CLI acceptance",
            attempt.attempt_id
        )
    }
}

fn ensure_cli_attempt_authorization_mode_value(authorization_mode: &str) -> Result<()> {
    match authorization_mode {
        "strict_safe" | "trusted_all" => Ok(()),
        other => bail!("unsupported CLI attempt authorization mode {other}"),
    }
}

fn ensure_cli_turn_event_value(turn_event: &str) -> Result<()> {
    match turn_event {
        "turn_started" | "turn_completed" | "turn_failed" | "turn_interrupted"
        | "turn_replaced" => Ok(()),
        other => bail!("unsupported CLI turn event {other}"),
    }
}

fn ensure_cli_turn_observation_matches_attempt(
    attempt: &DeliveryAttemptRecord,
    delivery_turn_id: &str,
) -> Result<()> {
    if attempt.delivery_turn_id.as_deref() == Some(delivery_turn_id) {
        Ok(())
    } else {
        bail!(
            "delivery attempt {} is bound to a different delivery turn",
            attempt.attempt_id
        )
    }
}

fn ensure_attempt_tracking_cli_turn_observation(attempt: &DeliveryAttemptRecord) -> Result<()> {
    if attempt.adapter_kind == "cli"
        && attempt.state == "cooldown"
        && attempt.delivery_rpc_state.as_deref() == Some("accepted")
        && attempt.delivery_observation_state.as_deref() == Some("tracking")
        && attempt.delivery_turn_id.is_some()
    {
        Ok(())
    } else {
        bail!(
            "delivery attempt {} is not tracking an accepted CLI turn",
            attempt.attempt_id
        )
    }
}

fn is_idempotent_terminal_cli_turn_observation(
    attempt: &DeliveryAttemptRecord,
    turn_event: &str,
) -> bool {
    matches!(attempt.state.as_str(), "closed" | "abandoned")
        && attempt.last_observed_turn_event.as_deref() == Some(turn_event)
}

fn can_record_late_cli_turn_evidence(attempt: &DeliveryAttemptRecord, turn_event: &str) -> bool {
    attempt.state == "abandoned"
        && matches!(
            attempt.delivery_observation_state.as_deref(),
            Some("expired") | Some("abandoned")
        )
        && matches!(
            attempt.last_observed_turn_event.as_deref(),
            None | Some("turn_started")
        )
        && matches!(
            turn_event,
            "turn_completed" | "turn_failed" | "turn_interrupted" | "turn_replaced"
        )
}

fn can_complete_delayed_cli_turn_after_expiry(
    attempt: &DeliveryAttemptRecord,
    turn_event: &str,
    observed_at: i64,
) -> Result<bool> {
    if turn_event != "turn_completed"
        || attempt.state != "abandoned"
        || attempt.delivery_rpc_state.as_deref() != Some("accepted")
        || attempt.delivery_observation_state.as_deref() != Some("expired")
        || !matches!(
            attempt.last_observed_turn_event.as_deref(),
            None | Some("turn_started")
        )
    {
        return Ok(false);
    }
    Ok(!cli_turn_observation_is_after_deadline(
        attempt,
        observed_at,
    )?)
}

fn cli_turn_observation_is_after_deadline(
    attempt: &DeliveryAttemptRecord,
    observed_at: i64,
) -> Result<bool> {
    let deadline = attempt.delivery_observation_deadline.ok_or_else(|| {
        anyhow!(
            "delivery attempt {} is missing an observation deadline",
            attempt.attempt_id
        )
    })?;
    Ok(observed_at >= deadline)
}

fn bool_to_i64(value: bool) -> i64 {
    i64::from(value)
}

fn checked_timestamp_add(now: i64, delta: i64, field_name: &str) -> Result<i64> {
    now.checked_add(delta)
        .ok_or_else(|| anyhow!("{field_name} overflows timestamp range"))
}

pub fn new_id() -> String {
    uuid::Uuid::now_v7().to_string()
}

#[cfg(test)]
mod tests {
    use std::fs;

    use rusqlite::params;

    use super::*;

    #[test]
    fn manifest_sync_progresses_beyond_one_sweep_limit() {
        let home = tempfile::tempdir().expect("temp home");
        let layout = FsLayout::resolve(Some(home.path().to_path_buf())).expect("layout");
        let mut store = Store::open(&layout).expect("store");
        let total = (MAX_MANIFEST_SYNCS_PER_SWEEP + 1) as usize;

        for index in 0..total {
            let job_id = format!("job-{index}");
            let artifact_id = format!("artifact-{index:03}");
            let artifact_dir = layout.artifact_dir(&artifact_id);
            fs::create_dir_all(&artifact_dir).expect("artifact dir");
            fs::write(artifact_dir.join("payload"), b"payload").expect("payload");
            fs::write(
                artifact_dir.join("manifest.json"),
                br#"{"retention_until":10}"#,
            )
            .expect("manifest");

            store
                .conn
                .execute(
                    "INSERT INTO jobs (
                        job_id, source_thread_id, status, summary, metadata_json,
                        created_at, updated_at, delivery_read_only,
                        delivery_requires_approval, delivery_requires_network,
                        delivery_requires_write_access
                    ) VALUES (?, 'thread-sync', 'completed', 'summary', '{}', 1, 1, 0, 1, 1, 1)",
                    params![job_id],
                )
                .expect("insert job");
            store
                .conn
                .execute(
                    "INSERT INTO artifacts (
                        artifact_id, job_id, relative_path, original_filename,
                        size_bytes, sha256, created_at, retention_until,
                        manifest_synced_retention_until, manifest_sync_attempted_at,
                        gc_attempted_at
                    ) VALUES (?, ?, ?, NULL, 7, 'sha', 1, 100, 10, 0, 0)",
                    params![
                        artifact_id,
                        job_id,
                        format!("artifacts/{artifact_id}/payload")
                    ],
                )
                .expect("insert artifact");
        }

        let first = store.sweep(&layout, 0).expect("first sweep");
        assert_eq!(
            first.artifact_manifests_synced,
            MAX_MANIFEST_SYNCS_PER_SWEEP as usize
        );
        assert_eq!(pending_manifest_sync_count(&store.conn), 1);

        let second = store.sweep(&layout, 0).expect("second sweep");
        assert_eq!(second.artifact_manifests_synced, 1);
        assert_eq!(pending_manifest_sync_count(&store.conn), 0);
    }

    #[test]
    fn daemon_lifecycle_reports_active_and_stale_cli_acceptances() {
        let home = tempfile::tempdir().expect("temp home");
        let layout = FsLayout::resolve(Some(home.path().to_path_buf())).expect("layout");
        let mut store = Store::open(&layout).expect("store");
        let policy = DeliveryPolicy {
            delivery_read_only: true,
            delivery_requires_approval: false,
            delivery_requires_network: false,
            delivery_requires_write_access: false,
        };
        store
            .submit_job(NewJob {
                job_id: "job-cli-accept-lifecycle".to_owned(),
                source_thread_id: "thread-cli-accept-lifecycle".to_owned(),
                summary: "lifecycle".to_owned(),
                metadata_json: "{}".to_owned(),
                policy,
                created_at: 0,
            })
            .expect("submit job");
        let batch = store
            .fail_job("job-cli-accept-lifecycle", "ready", 0, 3, 10)
            .expect("fail job");
        let batch_id = batch.batch.batch_id;
        let session = store
            .attach_or_create_cli_managed_session(
                "thread-cli-accept-lifecycle",
                CliManagedSessionProfile {
                    session_allows_approval: false,
                    session_allows_network: false,
                    session_allows_write_access: false,
                },
                0,
            )
            .expect("bind CLI session");
        store
            .note_cli_managed_session_capabilities(
                &session.session.managed_session_id,
                session.session.session_epoch,
                1,
                CliManagedSessionCapabilities {
                    capability_thread_resume: true,
                    capability_turn_start: true,
                    capability_current_state_sync: true,
                    capability_turn_completed_event: true,
                    capability_negative_terminal_events: true,
                    capability_thread_start: false,
                    capability_turn_steer: false,
                },
                0,
            )
            .expect("note CLI session capabilities");
        store
            .note_cli_managed_session_activity(
                &session.session.managed_session_id,
                session.session.session_epoch,
                "idle",
                1,
                0,
            )
            .expect("note CLI session idle");
        store
            .begin_cli_accept_pending_attempt(NewCliAcceptPendingAttempt {
                attempt_id: "attempt-cli-accept-lifecycle".to_owned(),
                batch_id,
                managed_session_id: session.session.managed_session_id,
                session_epoch: 1,
                authorization_mode: "strict_safe".to_owned(),
                delivery_rpc_request_id: "rpc-cli-accept-lifecycle".to_owned(),
                delivery_rpc_kind: "turn_start".to_owned(),
                delivery_rpc_correlation_marker: "cbth:lifecycle".to_owned(),
                delivery_rpc_started_at: 100,
            })
            .expect("begin CLI accept");

        let active = store.daemon_lifecycle_status(399, 400).expect("active");
        assert_eq!(active.active_cli_acceptances, 1);
        assert_eq!(active.cli_acceptances_stale_now, 0);
        assert_eq!(active.open_batches_due_now, 0);
        assert!(!active.has_due_maintenance());

        let stale = store.daemon_lifecycle_status(400, 401).expect("stale");
        assert_eq!(stale.active_cli_acceptances, 0);
        assert_eq!(stale.cli_acceptances_stale_now, 1);
        assert_eq!(stale.open_batches_due_now, 0);
        assert!(stale.has_due_maintenance());
    }

    #[test]
    fn manifest_sync_failures_do_not_starve_later_artifacts() {
        let home = tempfile::tempdir().expect("temp home");
        let layout = FsLayout::resolve(Some(home.path().to_path_buf())).expect("layout");
        let mut store = Store::open(&layout).expect("store");

        for index in 0..MAX_MANIFEST_SYNCS_PER_SWEEP {
            let job_id = format!("bad-job-{index}");
            let artifact_id = format!("bad-artifact-{index:03}");
            insert_completed_job(&store.conn, &job_id);
            store
                .conn
                .execute(
                    "INSERT INTO artifacts (
                        artifact_id, job_id, relative_path, original_filename,
                        size_bytes, sha256, created_at, retention_until,
                        manifest_synced_retention_until, manifest_sync_attempted_at,
                        gc_attempted_at
                    ) VALUES (?, ?, 'artifacts/wrong-id/payload', NULL, 7, 'sha', 1, 10000, 10, 0, 0)",
                    params![artifact_id, job_id],
                )
                .expect("insert bad artifact");
        }

        let good_job_id = "good-job";
        let good_artifact_id = "good-artifact";
        let good_artifact_dir = layout.artifact_dir(good_artifact_id);
        fs::create_dir_all(&good_artifact_dir).expect("artifact dir");
        fs::write(good_artifact_dir.join("payload"), b"payload").expect("payload");
        fs::write(
            good_artifact_dir.join("manifest.json"),
            br#"{"retention_until":10}"#,
        )
        .expect("manifest");
        insert_completed_job(&store.conn, good_job_id);
        store
            .conn
            .execute(
                "INSERT INTO artifacts (
                    artifact_id, job_id, relative_path, original_filename,
                    size_bytes, sha256, created_at, retention_until,
                    manifest_synced_retention_until, manifest_sync_attempted_at,
                    gc_attempted_at
                ) VALUES (?, ?, ?, NULL, 7, 'sha', 1, 20000, 10, 0, 0)",
                params![
                    good_artifact_id,
                    good_job_id,
                    format!("artifacts/{good_artifact_id}/payload")
                ],
            )
            .expect("insert good artifact");

        let first = store.sweep(&layout, 1000).expect("first sweep");
        assert_eq!(
            first.artifact_manifest_sync_failures,
            MAX_MANIFEST_SYNCS_PER_SWEEP as usize
        );
        assert_eq!(first.artifact_manifests_synced, 0);
        assert_eq!(good_manifest_synced_retention(&store.conn), 10);

        let second = store.sweep(&layout, 1001).expect("second sweep");
        assert_eq!(second.artifact_manifests_synced, 1);
        assert_eq!(good_manifest_synced_retention(&store.conn), 20000);
    }

    #[test]
    fn manifest_sync_reloads_current_artifact_before_rewrite() {
        let home = tempfile::tempdir().expect("temp home");
        let layout = FsLayout::resolve(Some(home.path().to_path_buf())).expect("layout");
        let mut store = Store::open(&layout).expect("store");
        let job_id = "stale-candidate-job";
        let artifact_id = "stale-candidate-artifact";
        let artifact_dir = layout.artifact_dir(artifact_id);
        fs::create_dir_all(&artifact_dir).expect("artifact dir");
        fs::write(artifact_dir.join("payload"), b"payload").expect("payload");
        fs::write(
            artifact_dir.join("manifest.json"),
            br#"{"retention_until":100}"#,
        )
        .expect("manifest");
        insert_completed_job(&store.conn, job_id);
        store
            .conn
            .execute(
                "INSERT INTO artifacts (
                    artifact_id, job_id, relative_path, original_filename,
                    size_bytes, sha256, created_at, retention_until,
                    manifest_synced_retention_until, manifest_sync_attempted_at,
                    gc_attempted_at
                ) VALUES (?, ?, ?, NULL, 7, 'sha', 1, 200, 100, 0, 0)",
                params![
                    artifact_id,
                    job_id,
                    format!("artifacts/{artifact_id}/payload")
                ],
            )
            .expect("insert artifact");

        let stale_candidate = ArtifactRecord {
            artifact_id: artifact_id.to_owned(),
            job_id: job_id.to_owned(),
            relative_path: format!("artifacts/{artifact_id}/payload"),
            original_filename: None,
            size_bytes: 7,
            sha256: "sha".to_owned(),
            created_at: 1,
            retention_until: 100,
            manifest_synced_retention_until: 100,
            manifest_sync_attempted_at: 0,
            gc_attempted_at: 0,
        };
        let report = sync_artifact_manifests(&mut store.conn, &layout, &[stale_candidate], 50);
        assert_eq!(report.synced, 1);
        assert_eq!(report.failed, 0);
        assert_eq!(artifact_manifest_retention_until(&artifact_dir), 200);
        assert_eq!(
            store
                .conn
                .query_row(
                    "SELECT manifest_synced_retention_until
                     FROM artifacts
                     WHERE artifact_id = ?",
                    params![artifact_id],
                    |row| row.get::<_, i64>(0),
                )
                .expect("synced retention"),
            200
        );
    }

    #[test]
    fn artifact_gc_failures_do_not_starve_later_artifacts() {
        let home = tempfile::tempdir().expect("temp home");
        let layout = FsLayout::resolve(Some(home.path().to_path_buf())).expect("layout");
        let mut store = Store::open(&layout).expect("store");

        for index in 0..MAX_DELETABLE_ARTIFACTS_PER_SWEEP {
            let job_id = format!("bad-gc-job-{index}");
            let artifact_id = format!("bad-gc-artifact-{index:03}");
            insert_completed_job(&store.conn, &job_id);
            store
                .conn
                .execute(
                    "INSERT INTO artifacts (
                        artifact_id, job_id, relative_path, original_filename,
                        size_bytes, sha256, created_at, retention_until,
                        manifest_synced_retention_until, manifest_sync_attempted_at,
                        gc_attempted_at
                    ) VALUES (?, ?, 'artifacts/wrong-id/payload', NULL, 7, 'sha', 1, 10, 10, 0, 0)",
                    params![artifact_id, job_id],
                )
                .expect("insert bad gc artifact");
        }

        let good_job_id = "good-gc-job";
        let good_artifact_id = "good-gc-artifact";
        let good_artifact_dir = layout.artifact_dir(good_artifact_id);
        fs::create_dir_all(&good_artifact_dir).expect("artifact dir");
        fs::write(good_artifact_dir.join("payload"), b"payload").expect("payload");
        fs::write(good_artifact_dir.join("manifest.json"), b"{}").expect("manifest");
        insert_completed_job(&store.conn, good_job_id);
        store
            .conn
            .execute(
                "INSERT INTO artifacts (
                    artifact_id, job_id, relative_path, original_filename,
                    size_bytes, sha256, created_at, retention_until,
                    manifest_synced_retention_until, manifest_sync_attempted_at,
                    gc_attempted_at
                ) VALUES (?, ?, ?, NULL, 7, 'sha', 1, 10, 10, 0, 0)",
                params![
                    good_artifact_id,
                    good_job_id,
                    format!("artifacts/{good_artifact_id}/payload")
                ],
            )
            .expect("insert good gc artifact");

        let first = store.sweep(&layout, 1000).expect("first sweep");
        assert_eq!(
            first.artifact_delete_failures,
            MAX_DELETABLE_ARTIFACTS_PER_SWEEP as usize
        );
        assert_eq!(first.artifacts_deleted, 0);
        assert!(good_artifact_dir.exists());

        let second = store.sweep(&layout, 1001).expect("second sweep");
        assert_eq!(second.artifacts_deleted, 1);
        assert!(!good_artifact_dir.exists());
    }

    fn insert_completed_job(conn: &Connection, job_id: &str) {
        conn.execute(
            "INSERT INTO jobs (
                job_id, source_thread_id, status, summary, metadata_json,
                created_at, updated_at, delivery_read_only,
                delivery_requires_approval, delivery_requires_network,
                delivery_requires_write_access
            ) VALUES (?, 'thread-sync', 'completed', 'summary', '{}', 1, 1, 0, 1, 1, 1)",
            params![job_id],
        )
        .expect("insert job");
    }

    fn good_manifest_synced_retention(conn: &Connection) -> i64 {
        conn.query_row(
            "SELECT manifest_synced_retention_until
             FROM artifacts
             WHERE artifact_id = 'good-artifact'",
            [],
            |row| row.get(0),
        )
        .expect("good artifact sync state")
    }

    fn pending_manifest_sync_count(conn: &Connection) -> i64 {
        conn.query_row(
            "SELECT count(*) FROM artifacts
             WHERE manifest_synced_retention_until < retention_until",
            [],
            |row| row.get(0),
        )
        .expect("count")
    }

    fn artifact_manifest_retention_until(artifact_dir: &std::path::Path) -> i64 {
        let bytes = fs::read(artifact_dir.join("manifest.json")).expect("manifest");
        let manifest: serde_json::Value = serde_json::from_slice(&bytes).expect("json");
        manifest["retention_until"].as_i64().expect("retention")
    }
}
