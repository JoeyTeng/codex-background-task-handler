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
    ArtifactRecord, BatchInspect, BatchJobRecord, BatchRecord, DeliveryPolicy, JobRecord,
    NewArtifact, NewBatch, NewJob, ORPHAN_ARTIFACT_GRACE_SECONDS, POST_CLOSE_ARTIFACT_TTL_SECONDS,
    SweepReport,
};

const MAX_STALE_ARTIFACT_INGESTS_PER_SWEEP: i64 = 100;
const MAX_EXPIRED_BATCHES_PER_SWEEP: i64 = 100;
const MAX_DELETABLE_ARTIFACTS_PER_SWEEP: i64 = 100;
const MAX_MANIFEST_SYNCS_PER_SWEEP: i64 = 100;
const SQLITE_BUSY_TIMEOUT: Duration = Duration::from_secs(30);
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
        let retry_started = SystemTime::now();
        let mut retry_delay = SQLITE_OPEN_RETRY_INITIAL_DELAY;
        loop {
            match Self::open_once(layout) {
                Ok(store) => return Ok(store),
                Err(error) if is_sqlite_busy_or_locked(&error) => {
                    if retry_started.elapsed().unwrap_or_default() >= SQLITE_BUSY_TIMEOUT {
                        return Err(error);
                    }
                    thread::sleep(retry_delay);
                    retry_delay = retry_delay
                        .saturating_mul(2)
                        .min(SQLITE_OPEN_RETRY_MAX_DELAY);
                }
                Err(error) => return Err(error),
            }
        }
    }

    fn open_once(layout: &FsLayout) -> Result<Self> {
        layout.ensure()?;
        let db_path = layout.db_path();
        ensure_private_file_exists(&db_path)?;
        let conn = Connection::open(&db_path)
            .with_context(|| format!("open sqlite database {}", db_path.display()))?;
        conn.busy_timeout(SQLITE_BUSY_TIMEOUT)?;
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
    Ok(())
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

fn close_expired_automatic_batches_tx(
    tx: &Transaction<'_>,
    now: i64,
) -> Result<(usize, Vec<ArtifactRecord>)> {
    let mut stmt = tx.prepare(
        "SELECT batch_id FROM batches
         WHERE state = 'open'
           AND replay_policy = 'automatic'
           AND redelivery_window_ends_at <= ?
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

fn ensure_job_pending(job: &JobRecord) -> Result<()> {
    if job.status == "pending" {
        Ok(())
    } else {
        bail!("job {} is already {}", job.job_id, job.status)
    }
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
