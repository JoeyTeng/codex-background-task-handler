use std::env;
use std::fs::{self, File, OpenOptions};
use std::io;
#[cfg(unix)]
use std::os::fd::AsRawFd;
#[cfg(unix)]
use std::os::unix::ffi::OsStrExt;
#[cfg(unix)]
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};

#[derive(Clone, Debug)]
pub struct FsLayout {
    home: PathBuf,
}

impl FsLayout {
    pub fn resolve(home_arg: Option<PathBuf>) -> Result<Self> {
        let home = match home_arg {
            Some(path) => path,
            None => match env::var_os("CBTH_HOME") {
                Some(value) => PathBuf::from(value),
                None => {
                    let user_home = env::var_os("HOME")
                        .map(PathBuf::from)
                        .context("CBTH_HOME is unset and HOME is unavailable")?;
                    user_home.join(".cbth")
                }
            },
        };
        Ok(Self { home })
    }

    pub fn db_path(&self) -> PathBuf {
        self.home.join("cbth.sqlite3")
    }

    pub fn home_dir(&self) -> &Path {
        &self.home
    }

    pub fn run_dir(&self) -> PathBuf {
        self.home.join("run")
    }

    pub fn daemon_socket_path(&self) -> PathBuf {
        self.run_dir().join("cbth.sock")
    }

    pub fn daemon_startup_lock_path(&self) -> PathBuf {
        self.run_dir().join("startup.lock")
    }

    pub fn daemon_generations_dir(&self) -> PathBuf {
        self.run_dir().join("daemons")
    }

    pub fn daemon_generation_dir(&self, generation_id: &str) -> PathBuf {
        self.daemon_generations_dir().join(generation_id)
    }

    pub fn daemon_generation_socket_path(&self, generation_id: &str) -> PathBuf {
        self.daemon_generation_dir(generation_id).join("cbth.sock")
    }

    pub fn daemon_generation_startup_lock_path(&self, generation_id: &str) -> PathBuf {
        self.daemon_generation_dir(generation_id)
            .join("startup.lock")
    }

    pub fn artifacts_dir(&self) -> PathBuf {
        self.home.join("artifacts")
    }

    pub fn artifact_dir(&self, artifact_id: &str) -> PathBuf {
        self.artifacts_dir().join(artifact_id)
    }

    pub fn tasks_dir(&self) -> PathBuf {
        self.home.join("tasks")
    }

    pub fn task_dir(&self, task_id: &str) -> PathBuf {
        self.tasks_dir().join(task_id)
    }

    pub fn inbox_dir(&self) -> PathBuf {
        self.home.join("inbox")
    }

    pub fn desktop_current_snapshot_path(&self) -> PathBuf {
        self.inbox_dir().join("current-snapshot.json")
    }

    pub fn desktop_installation_state_path(&self) -> PathBuf {
        self.inbox_dir().join("desktop-installation-state.json")
    }

    pub fn desktop_snapshots_dir(&self) -> PathBuf {
        self.inbox_dir().join("snapshots")
    }

    pub fn desktop_snapshot_revision_dir(&self, snapshot_revision: &str) -> PathBuf {
        self.desktop_snapshots_dir().join(snapshot_revision)
    }

    pub fn desktop_ready_threads_path(&self, snapshot_revision: &str) -> PathBuf {
        self.desktop_snapshot_revision_dir(snapshot_revision)
            .join("ready-threads.json")
    }

    pub fn desktop_arm_pending_bindings_path(&self, snapshot_revision: &str) -> PathBuf {
        self.desktop_snapshot_revision_dir(snapshot_revision)
            .join("arm-pending-bindings.json")
    }

    pub fn desktop_pause_due_bindings_path(&self, snapshot_revision: &str) -> PathBuf {
        self.desktop_snapshot_revision_dir(snapshot_revision)
            .join("pause-due-bindings.json")
    }

    pub fn desktop_snapshot_installation_state_path(&self, snapshot_revision: &str) -> PathBuf {
        self.desktop_snapshot_revision_dir(snapshot_revision)
            .join("desktop-installation-state.json")
    }

    pub fn desktop_writeback_dropbox_dir(&self) -> PathBuf {
        self.inbox_dir().join("writeback-dropbox")
    }

    pub fn desktop_writeback_dropbox_probes_dir(&self) -> PathBuf {
        self.desktop_writeback_dropbox_dir().join("probes")
    }

    pub fn desktop_writeback_dropbox_probe_path(&self, probe_id: &str) -> PathBuf {
        self.desktop_writeback_dropbox_probes_dir()
            .join(format!("{probe_id}.json"))
    }

    pub fn ensure(&self) -> Result<()> {
        ensure_private_dir(&self.home)?;
        ensure_private_dir(&self.artifacts_dir())?;
        ensure_private_dir(&self.tasks_dir())?;
        Ok(())
    }

    pub fn ensure_run_dir(&self) -> Result<()> {
        ensure_private_dir(&self.home)?;
        ensure_private_dir(&self.run_dir())
    }

    pub fn ensure_daemon_generation_dir(&self, generation_id: &str) -> Result<()> {
        ensure_private_dir(&self.home)?;
        ensure_private_dir(&self.run_dir())?;
        ensure_private_dir(&self.daemon_generations_dir())?;
        ensure_private_dir(&self.daemon_generation_dir(generation_id))
    }
}

pub fn ensure_private_dir(path: &Path) -> Result<()> {
    let missing_dirs = missing_dirs(path)?;
    if missing_dirs.is_empty() {
        set_private_dir_permissions(path)?;
    } else {
        for dir in missing_dirs {
            match create_private_dir_single(&dir) {
                Ok(()) => {}
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
                Err(error) => {
                    return Err(error)
                        .with_context(|| format!("create directory {}", dir.display()));
                }
            }
            set_private_dir_permissions(&dir)?;
            if let Some(parent) = dir.parent() {
                sync_dir(parent)?;
            }
        }
    }
    Ok(())
}

#[cfg(unix)]
fn create_private_dir_single(path: &Path) -> io::Result<()> {
    use std::os::unix::fs::DirBuilderExt;

    let mut builder = fs::DirBuilder::new();
    builder.mode(0o700);
    builder.create(path)
}

#[cfg(not(unix))]
fn create_private_dir_single(path: &Path) -> io::Result<()> {
    fs::create_dir(path)
}

pub fn create_private_file(path: &Path) -> Result<File> {
    if let Some(parent) = path.parent() {
        ensure_private_dir(parent)?;
    }

    let file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .with_context(|| format!("create file {}", path.display()))?;
    set_private_file_permissions(path)?;
    Ok(file)
}

pub fn ensure_private_file_exists(path: &Path) -> Result<()> {
    if path.exists() {
        let metadata = fs::metadata(path).with_context(|| format!("stat {}", path.display()))?;
        if !metadata.is_file() {
            bail!("path exists but is not a regular file: {}", path.display());
        }
        set_private_file_permissions(path)?;
        return Ok(());
    }

    if let Some(parent) = path.parent() {
        ensure_private_dir(parent)?;
    }

    match OpenOptions::new().write(true).create_new(true).open(path) {
        Ok(file) => drop(file),
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
        Err(error) => return Err(error).with_context(|| format!("create file {}", path.display())),
    }
    if let Some(parent) = path.parent() {
        sync_dir(parent)?;
    }
    let metadata = fs::metadata(path).with_context(|| format!("stat {}", path.display()))?;
    if !metadata.is_file() {
        bail!("path exists but is not a regular file: {}", path.display());
    }
    set_private_file_permissions(path)?;
    Ok(())
}

pub fn set_private_file_permissions_if_exists(path: &Path) -> Result<()> {
    match fs::metadata(path) {
        Ok(metadata) if metadata.is_file() => set_private_file_permissions(path),
        Ok(_) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error).with_context(|| format!("stat {}", path.display())),
    }
}

pub fn atomic_write_private(path: &Path, bytes: &[u8]) -> Result<()> {
    let parent = path
        .parent()
        .with_context(|| format!("path {} has no parent", path.display()))?;
    ensure_private_dir(parent)?;

    let file_name = path
        .file_name()
        .and_then(|value| value.to_str())
        .with_context(|| format!("path {} has no valid file name", path.display()))?;
    let tmp = parent.join(format!(".{file_name}.{}.tmp", uuid::Uuid::now_v7()));

    let write_result = (|| -> Result<()> {
        let mut file = create_private_file(&tmp)?;
        use std::io::Write;
        file.write_all(bytes)
            .with_context(|| format!("write {}", tmp.display()))?;
        file.sync_all()
            .with_context(|| format!("sync {}", tmp.display()))?;
        Ok(())
    })();

    if let Err(error) = write_result {
        let _ = fs::remove_file(&tmp);
        return Err(error);
    }

    if let Err(error) = fs::rename(&tmp, path) {
        let _ = fs::remove_file(&tmp);
        return Err(error)
            .with_context(|| format!("rename {} to {}", tmp.display(), path.display()));
    }
    sync_dir(parent)?;
    Ok(())
}

pub fn sync_dir(path: &Path) -> Result<()> {
    let dir = File::open(path).with_context(|| format!("open directory {}", path.display()))?;
    dir.sync_all()
        .with_context(|| format!("sync directory {}", path.display()))?;
    Ok(())
}

pub fn remove_dir_all_best_effort(path: &Path) {
    match fs::remove_dir_all(path) {
        Ok(()) => {}
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(_) => {}
    }
}

pub fn remove_dir_all_durable(path: &Path) -> Result<bool> {
    let parent = path
        .parent()
        .with_context(|| format!("path {} has no parent", path.display()))?;
    match fs::remove_dir_all(path) {
        Ok(()) => {
            sync_dir(parent)?;
            Ok(true)
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            sync_dir(parent)?;
            Ok(true)
        }
        Err(error) => Err(error).with_context(|| format!("remove {}", path.display())),
    }
}

fn missing_dirs(path: &Path) -> Result<Vec<PathBuf>> {
    let mut dirs = Vec::new();
    let mut current = Some(path);
    while let Some(dir) = current {
        match fs::symlink_metadata(dir) {
            Ok(metadata) if metadata.is_dir() => break,
            Ok(_) => bail!("path exists but is not a directory: {}", dir.display()),
            Err(error) if error.kind() == io::ErrorKind::NotFound => dirs.push(dir.to_path_buf()),
            Err(error) => return Err(error).with_context(|| format!("stat {}", dir.display())),
        }
        current = dir.parent();
    }
    dirs.reverse();
    Ok(dirs)
}

pub fn relative_artifact_payload_path(artifact_id: &str) -> String {
    format!("artifacts/{artifact_id}/payload")
}

pub fn validate_id_path_component(value: &str, name: &str) -> Result<()> {
    let valid = !value.is_empty()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_');
    if valid {
        Ok(())
    } else {
        bail!("{name} contains unsupported path characters")
    }
}

#[cfg(unix)]
fn set_private_dir_permissions(path: &Path) -> Result<()> {
    let metadata = fs::symlink_metadata(path)
        .with_context(|| format!("stat private directory {}", path.display()))?;
    if !metadata.is_dir() {
        bail!("path exists but is not a directory: {}", path.display());
    }
    ensure_current_user_owned(path, &metadata, "private directory")?;
    if private_permissions_are_satisfied(&metadata, 0o700) {
        return Ok(());
    }

    let parent = path
        .parent()
        .with_context(|| format!("path {} has no parent", path.display()))?;
    let file_name = path
        .file_name()
        .with_context(|| format!("path {} has no file name", path.display()))?;
    let file_name = std::ffi::CString::new(file_name.as_bytes())
        .with_context(|| format!("path {} contains an interior NUL", path.display()))?;
    let parent = File::open(parent)
        .with_context(|| format!("open parent directory for {}", path.display()))?;
    let rc = unsafe {
        libc::fchmodat(
            parent.as_raw_fd(),
            file_name.as_ptr(),
            0o700,
            libc::AT_SYMLINK_NOFOLLOW,
        )
    };
    if rc == 0 {
        let metadata = fs::symlink_metadata(path)
            .with_context(|| format!("stat private directory {}", path.display()))?;
        ensure_current_user_owned(path, &metadata, "private directory")?;
        if private_permissions_are_satisfied(&metadata, 0o700) {
            Ok(())
        } else {
            bail!(
                "private directory permissions are {:o}, expected 700: {}",
                metadata.mode() & 0o7777,
                path.display()
            )
        }
    } else {
        Err(io::Error::last_os_error()).with_context(|| format!("chmod 0700 {}", path.display()))
    }
}

#[cfg(not(unix))]
fn set_private_dir_permissions(_path: &Path) -> Result<()> {
    Ok(())
}

#[cfg(unix)]
fn set_private_file_permissions(path: &Path) -> Result<()> {
    let metadata = fs::symlink_metadata(path)
        .with_context(|| format!("stat private file {}", path.display()))?;
    if !metadata.is_file() {
        bail!("path exists but is not a regular file: {}", path.display());
    }
    ensure_current_user_owned(path, &metadata, "private file")?;
    if private_permissions_are_satisfied(&metadata, 0o600) {
        return Ok(());
    }
    fs::set_permissions(path, fs::Permissions::from_mode(0o600))
        .with_context(|| format!("chmod 0600 {}", path.display()))?;
    let metadata = fs::symlink_metadata(path)
        .with_context(|| format!("stat private file {}", path.display()))?;
    ensure_current_user_owned(path, &metadata, "private file")?;
    if private_permissions_are_satisfied(&metadata, 0o600) {
        Ok(())
    } else {
        bail!(
            "private file permissions are {:o}, expected 600: {}",
            metadata.mode() & 0o7777,
            path.display()
        )
    }
}

#[cfg(unix)]
fn ensure_current_user_owned(path: &Path, metadata: &fs::Metadata, name: &str) -> Result<()> {
    let current_uid = unsafe { libc::geteuid() };
    if metadata.uid() == current_uid {
        Ok(())
    } else {
        bail!("{name} is not owned by current user: {}", path.display())
    }
}

#[cfg(unix)]
fn private_permissions_are_satisfied(metadata: &fs::Metadata, expected_mode: u32) -> bool {
    metadata.mode() & 0o7777 == expected_mode
}

#[cfg(not(unix))]
fn set_private_file_permissions(_path: &Path) -> Result<()> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::fs;
    #[cfg(unix)]
    use std::os::unix::fs::PermissionsExt;
    #[cfg(unix)]
    use std::path::PathBuf;

    use super::*;

    #[test]
    fn atomic_write_removes_temp_file_when_rename_fails() {
        let dir = tempfile::tempdir().expect("tempdir");
        let target = dir.path().join("manifest.json");
        fs::create_dir(&target).expect("target dir");

        let error = atomic_write_private(&target, b"payload").expect_err("rename should fail");
        assert!(error.to_string().contains("rename"));

        let leftovers = fs::read_dir(dir.path())
            .expect("read tempdir")
            .filter_map(Result::ok)
            .map(|entry| entry.file_name().to_string_lossy().into_owned())
            .collect::<Vec<_>>();
        assert_eq!(leftovers, vec!["manifest.json"]);
    }

    #[cfg(unix)]
    #[test]
    fn ensure_private_dir_repairs_directory_without_read_permission() {
        let dir = tempfile::tempdir().expect("tempdir");
        let target = dir.path().join("private");
        fs::create_dir(&target).expect("create private dir");
        fs::set_permissions(&target, fs::Permissions::from_mode(0o300))
            .expect("restrict private dir");

        ensure_private_dir(&target).expect("repair private dir");

        let mode = fs::symlink_metadata(&target)
            .expect("private dir metadata")
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(mode, 0o700);
    }

    #[cfg(unix)]
    #[test]
    fn ensure_private_dir_skips_chmod_when_already_private() {
        let dir = tempfile::tempdir().expect("tempdir");
        let parent = dir.path().join("parent");
        let target = parent.join("private");
        fs::create_dir(&parent).expect("create parent");
        fs::create_dir(&target).expect("create private dir");
        fs::set_permissions(&target, fs::Permissions::from_mode(0o700))
            .expect("set private dir mode");
        fs::set_permissions(&parent, fs::Permissions::from_mode(0o100)).expect("restrict parent");
        let _restore_parent = RestoreMode {
            path: parent.clone(),
            mode: 0o700,
        };

        ensure_private_dir(&target).expect("already-private dir should not need chmod");

        let mode = fs::symlink_metadata(&target)
            .expect("private dir metadata")
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(mode, 0o700);
    }

    #[cfg(unix)]
    #[test]
    fn ensure_private_dir_repairs_wide_directory_permissions() {
        let dir = tempfile::tempdir().expect("tempdir");
        let target = dir.path().join("private");
        fs::create_dir(&target).expect("create private dir");
        fs::set_permissions(&target, fs::Permissions::from_mode(0o755)).expect("widen private dir");

        ensure_private_dir(&target).expect("repair private dir");

        let mode = fs::symlink_metadata(&target)
            .expect("private dir metadata")
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(mode, 0o700);
    }

    #[cfg(unix)]
    #[test]
    fn ensure_private_file_exists_accepts_already_private_file() {
        let dir = tempfile::tempdir().expect("tempdir");
        let target = dir.path().join("private-file");
        fs::write(&target, b"payload").expect("write private file");
        fs::set_permissions(&target, fs::Permissions::from_mode(0o600))
            .expect("set private file mode");

        ensure_private_file_exists(&target).expect("already-private file");

        let mode = fs::symlink_metadata(&target)
            .expect("private file metadata")
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(mode, 0o600);
    }

    #[cfg(unix)]
    #[test]
    fn ensure_private_file_exists_repairs_wide_file_permissions() {
        let dir = tempfile::tempdir().expect("tempdir");
        let target = dir.path().join("private-file");
        fs::write(&target, b"payload").expect("write private file");
        fs::set_permissions(&target, fs::Permissions::from_mode(0o644))
            .expect("widen private file");

        ensure_private_file_exists(&target).expect("repair private file");

        let mode = fs::symlink_metadata(&target)
            .expect("private file metadata")
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(mode, 0o600);
    }

    #[cfg(unix)]
    struct RestoreMode {
        path: PathBuf,
        mode: u32,
    }

    #[cfg(unix)]
    impl Drop for RestoreMode {
        fn drop(&mut self) {
            let _ = fs::set_permissions(&self.path, fs::Permissions::from_mode(self.mode));
        }
    }
}
