use std::fs::{self, File};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use serde::Serialize;
use sha2::{Digest, Sha256};

use crate::fs_layout::{
    FsLayout, atomic_write_private, create_private_file, ensure_private_dir,
    relative_artifact_payload_path, remove_dir_all_best_effort, sync_dir,
    validate_id_path_component,
};
use crate::models::{ArtifactRecord, MIN_ARTIFACT_TTL_SECONDS, NewArtifact};

const COPY_BUFFER_BYTES: usize = 64 * 1024;
const INGEST_MARKER: &str = ".ingest-active";
const INGEST_MARKER_REFRESH_INTERVAL: Duration = Duration::from_secs(5);

#[derive(Debug)]
pub struct IngestedArtifact {
    pub record: NewArtifact,
    pub artifact_dir: PathBuf,
}

#[derive(Serialize)]
struct ArtifactManifest<'a> {
    artifact_id: &'a str,
    job_id: &'a str,
    relative_path: &'a str,
    original_filename: Option<&'a str>,
    size_bytes: i64,
    sha256: &'a str,
    created_at: i64,
    retention_until: i64,
}

pub fn ingest_result_file(
    layout: &FsLayout,
    artifact_id: &str,
    job_id: &str,
    source_path: &Path,
    now: i64,
) -> Result<IngestedArtifact> {
    validate_id_path_component(artifact_id, "artifact_id")?;
    validate_id_path_component(job_id, "job_id")?;

    let metadata = fs::metadata(source_path)
        .with_context(|| format!("stat result file {}", source_path.display()))?;
    if !metadata.is_file() {
        bail!(
            "result file must be a regular file: {}",
            source_path.display()
        );
    }

    let artifact_dir = layout.artifact_dir(artifact_id);
    ensure_private_dir(&artifact_dir)?;
    sync_dir(&layout.artifacts_dir())?;
    let tmp_payload_path = artifact_dir.join("payload.tmp");
    let payload_path = artifact_dir.join("payload");
    let manifest_path = artifact_dir.join("manifest.json");
    let ingest_marker_path = artifact_dir.join(INGEST_MARKER);
    write_ingest_marker(&ingest_marker_path)?;

    let copy_result = copy_payload(source_path, &tmp_payload_path, &ingest_marker_path).and_then(
        |(size_bytes, sha256)| {
            fs::rename(&tmp_payload_path, &payload_path).with_context(|| {
                format!(
                    "rename {} to {}",
                    tmp_payload_path.display(),
                    payload_path.display()
                )
            })?;
            sync_dir(&artifact_dir)?;

            let original_filename = source_path
                .file_name()
                .and_then(|value| value.to_str())
                .map(str::to_owned);
            let relative_path = relative_artifact_payload_path(artifact_id);
            let retention_until = now
                .checked_add(MIN_ARTIFACT_TTL_SECONDS)
                .context("min artifact retention overflows timestamp range")?;
            let record = NewArtifact {
                artifact_id: artifact_id.to_owned(),
                job_id: job_id.to_owned(),
                relative_path,
                original_filename,
                size_bytes,
                sha256,
                created_at: now,
                retention_until,
            };

            write_new_artifact_manifest(&manifest_path, &record)?;
            Ok(record)
        },
    );

    match copy_result {
        Ok(record) => Ok(IngestedArtifact {
            record,
            artifact_dir,
        }),
        Err(error) => {
            remove_dir_all_best_effort(&artifact_dir);
            Err(error)
        }
    }
}

pub fn remove_ingest_marker_best_effort(artifact_dir: &Path) {
    let marker = artifact_dir.join(INGEST_MARKER);
    match fs::remove_file(marker) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(_) => {}
    }
}

pub fn ingest_marker_path(artifact_dir: &Path) -> PathBuf {
    artifact_dir.join(INGEST_MARKER)
}

fn copy_payload(
    source_path: &Path,
    tmp_payload_path: &Path,
    ingest_marker_path: &Path,
) -> Result<(i64, String)> {
    let input =
        File::open(source_path).with_context(|| format!("open {}", source_path.display()))?;
    let mut reader = BufReader::with_capacity(COPY_BUFFER_BYTES, input);
    let output = create_private_file(tmp_payload_path)?;
    let mut writer = BufWriter::with_capacity(COPY_BUFFER_BYTES, output);
    let mut hasher = Sha256::new();
    let mut size_bytes = 0_i64;
    let mut buffer = [0_u8; COPY_BUFFER_BYTES];
    let mut last_marker_refresh = Instant::now();

    loop {
        let read = reader
            .read(&mut buffer)
            .with_context(|| format!("read {}", source_path.display()))?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
        writer
            .write_all(&buffer[..read])
            .with_context(|| format!("write {}", tmp_payload_path.display()))?;
        size_bytes += i64::try_from(read).context("payload size overflow")?;
        if last_marker_refresh.elapsed() >= INGEST_MARKER_REFRESH_INTERVAL {
            write_ingest_marker(ingest_marker_path)?;
            last_marker_refresh = Instant::now();
        }
    }

    writer
        .flush()
        .with_context(|| format!("flush {}", tmp_payload_path.display()))?;
    writer
        .get_ref()
        .sync_all()
        .with_context(|| format!("sync {}", tmp_payload_path.display()))?;

    Ok((size_bytes, format!("{:x}", hasher.finalize())))
}

fn write_ingest_marker(path: &Path) -> Result<()> {
    atomic_write_private(path, b"active\n")
}

pub fn rewrite_artifact_manifest(layout: &FsLayout, record: &ArtifactRecord) -> Result<()> {
    validate_id_path_component(&record.artifact_id, "artifact_id")?;
    let expected_relative_path = relative_artifact_payload_path(&record.artifact_id);
    if record.relative_path != expected_relative_path {
        bail!(
            "artifact {} has non-canonical relative_path {}",
            record.artifact_id,
            record.relative_path
        );
    }
    let manifest_path = layout
        .artifact_dir(&record.artifact_id)
        .join("manifest.json");
    let manifest = ArtifactManifest {
        artifact_id: &record.artifact_id,
        job_id: &record.job_id,
        relative_path: &record.relative_path,
        original_filename: record.original_filename.as_deref(),
        size_bytes: record.size_bytes,
        sha256: &record.sha256,
        created_at: record.created_at,
        retention_until: record.retention_until,
    };
    write_manifest(&manifest_path, &manifest)
}

fn write_new_artifact_manifest(path: &Path, record: &NewArtifact) -> Result<()> {
    let manifest = ArtifactManifest {
        artifact_id: &record.artifact_id,
        job_id: &record.job_id,
        relative_path: &record.relative_path,
        original_filename: record.original_filename.as_deref(),
        size_bytes: record.size_bytes,
        sha256: &record.sha256,
        created_at: record.created_at,
        retention_until: record.retention_until,
    };
    write_manifest(path, &manifest)
}

fn write_manifest(path: &Path, manifest: &ArtifactManifest<'_>) -> Result<()> {
    let bytes = serde_json::to_vec_pretty(&manifest).context("serialize artifact manifest")?;
    atomic_write_private(path, &bytes)
}
