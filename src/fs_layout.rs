use std::env;
use std::fs::{self, File, OpenOptions};
use std::io;
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

    pub fn artifacts_dir(&self) -> PathBuf {
        self.home.join("artifacts")
    }

    pub fn artifact_dir(&self, artifact_id: &str) -> PathBuf {
        self.artifacts_dir().join(artifact_id)
    }

    pub fn ensure(&self) -> Result<()> {
        ensure_private_dir(&self.home)?;
        ensure_private_dir(&self.artifacts_dir())?;
        Ok(())
    }
}

pub fn ensure_private_dir(path: &Path) -> Result<()> {
    let missing_dirs = missing_dirs(path)?;
    fs::create_dir_all(path).with_context(|| format!("create directory {}", path.display()))?;
    if missing_dirs.is_empty() {
        set_private_dir_permissions(path)?;
    } else {
        for dir in missing_dirs {
            set_private_dir_permissions(&dir)?;
            if let Some(parent) = dir.parent() {
                sync_dir(parent)?;
            }
        }
    }
    Ok(())
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
        match fs::metadata(dir) {
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
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(path, fs::Permissions::from_mode(0o700))
        .with_context(|| format!("chmod 0700 {}", path.display()))
}

#[cfg(not(unix))]
fn set_private_dir_permissions(_path: &Path) -> Result<()> {
    Ok(())
}

#[cfg(unix)]
fn set_private_file_permissions(path: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(path, fs::Permissions::from_mode(0o600))
        .with_context(|| format!("chmod 0600 {}", path.display()))
}

#[cfg(not(unix))]
fn set_private_file_permissions(_path: &Path) -> Result<()> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::fs;

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
}
