use std::env;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
#[cfg(target_os = "macos")]
use std::process::Command;

use anyhow::{Context, Result, anyhow, bail};
use semver::Version;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::fs_layout::sync_dir;

const GITHUB_OWNER: &str = "JoeyTeng";
const GITHUB_REPO: &str = "codex-background-task-handler";
const GITHUB_API_BASE: &str = "https://api.github.com";
const MAX_RELEASE_JSON_BYTES: u64 = 1024 * 1024;
const MAX_CHECKSUM_BYTES: u64 = 64 * 1024;
const MAX_BINARY_BYTES: u64 = 128 * 1024 * 1024;

#[derive(Debug)]
pub struct SelfUpdateOptions {
    pub version: Option<String>,
    pub check: bool,
    pub yes: bool,
}

#[derive(Debug, Serialize)]
struct SelfUpdateReport {
    current_version: String,
    target_version: String,
    target_tag: String,
    target_triple: String,
    release_url: Option<String>,
    asset_name: String,
    checksum_asset_name: String,
    update_available: bool,
    downgrade: bool,
    check: bool,
    yes: bool,
    updated: bool,
    install_path: Option<PathBuf>,
    message: String,
}

#[derive(Debug, Deserialize)]
struct GitHubRelease {
    tag_name: String,
    html_url: Option<String>,
    assets: Vec<GitHubAsset>,
}

#[derive(Debug, Deserialize)]
struct GitHubAsset {
    name: String,
    browser_download_url: String,
}

#[derive(Clone, Debug)]
struct SelectedReleaseAsset {
    name: String,
    download_url: String,
    checksum_name: String,
    checksum_download_url: String,
}

pub fn run_self_update(options: SelfUpdateOptions) -> Result<Value> {
    let target_triple = current_target_triple()
        .ok_or_else(|| anyhow!("self update is not supported on this platform"))?;
    let requested_tag = options
        .version
        .as_deref()
        .map(normalize_release_tag)
        .transpose()?;
    let release = fetch_release(requested_tag.as_deref())?;
    let target_tag = normalize_release_tag(&release.tag_name)?;
    if let Some(requested_tag) = &requested_tag
        && requested_tag != &target_tag
    {
        bail!(
            "GitHub release tag mismatch: requested {requested_tag}, got {}",
            release.tag_name
        );
    }

    let current_version = parse_version(env!("CARGO_PKG_VERSION"))?;
    let target_version = parse_release_version(&target_tag)?;
    let selected = select_release_asset(&release, &target_tag, target_triple)?;
    let update_available = target_version > current_version;
    let downgrade = target_version < current_version;
    let same_version = target_version == current_version;

    if options.check || same_version || (!update_available && requested_tag.is_none()) {
        let message = if same_version {
            "cbth is already at the requested version".to_owned()
        } else if downgrade && requested_tag.is_none() {
            "latest release is older than the current binary".to_owned()
        } else if update_available {
            "update available".to_owned()
        } else {
            "requested release is older than the current binary".to_owned()
        };
        return Ok(json!({
            "self_update": SelfUpdateReport {
                current_version: current_version.to_string(),
                target_version: target_version.to_string(),
                target_tag,
                target_triple: target_triple.to_owned(),
                release_url: release.html_url,
                asset_name: selected.name,
                checksum_asset_name: selected.checksum_name,
                update_available,
                downgrade,
                check: options.check,
                yes: options.yes,
                updated: false,
                install_path: None,
                message,
            }
        }));
    }
    if !options.yes {
        bail!("self update modifies the current executable; rerun with --yes or use --check");
    }

    let checksum_text = http_get_text(&selected.checksum_download_url, MAX_CHECKSUM_BYTES)
        .with_context(|| format!("download checksum asset {}", selected.checksum_name))?;
    let expected_sha256 = parse_sha256_checksum(&checksum_text)?;
    let binary = http_get_bytes(&selected.download_url, MAX_BINARY_BYTES)
        .with_context(|| format!("download release asset {}", selected.name))?;
    verify_sha256(&binary, &expected_sha256)?;

    let install_path = env::current_exe().context("resolve current cbth executable")?;
    install_binary_atomically(&install_path, &binary)?;

    Ok(json!({
        "self_update": SelfUpdateReport {
            current_version: current_version.to_string(),
            target_version: target_version.to_string(),
            target_tag,
            target_triple: target_triple.to_owned(),
            release_url: release.html_url,
            asset_name: selected.name,
            checksum_asset_name: selected.checksum_name,
            update_available,
            downgrade,
            check: options.check,
            yes: options.yes,
            updated: true,
            install_path: Some(install_path),
            message: "cbth updated from GitHub Releases".to_owned(),
        }
    }))
}

fn fetch_release(requested_tag: Option<&str>) -> Result<GitHubRelease> {
    let url = match requested_tag {
        Some(tag) => {
            format!("{GITHUB_API_BASE}/repos/{GITHUB_OWNER}/{GITHUB_REPO}/releases/tags/{tag}")
        }
        None => format!("{GITHUB_API_BASE}/repos/{GITHUB_OWNER}/{GITHUB_REPO}/releases/latest"),
    };
    let text = http_get_text(&url, MAX_RELEASE_JSON_BYTES)
        .with_context(|| format!("fetch GitHub release metadata from {url}"))?;
    serde_json::from_str(&text).context("parse GitHub release metadata")
}

fn http_get_text(url: &str, limit: u64) -> Result<String> {
    let mut response = github_get(url)?
        .call()
        .map_err(|error| anyhow!("{error}"))?;
    response
        .body_mut()
        .with_config()
        .limit(limit)
        .read_to_string()
        .map_err(|error| anyhow!("{error}"))
}

fn http_get_bytes(url: &str, limit: u64) -> Result<Vec<u8>> {
    let mut response = github_get(url)?
        .call()
        .map_err(|error| anyhow!("{error}"))?;
    response
        .body_mut()
        .with_config()
        .limit(limit)
        .read_to_vec()
        .map_err(|error| anyhow!("{error}"))
}

fn github_get(url: &str) -> Result<ureq::RequestBuilder<ureq::typestate::WithoutBody>> {
    let user_agent = format!("cbth/{}", env!("CARGO_PKG_VERSION"));
    let mut request = ureq::get(url)
        .header("User-Agent", user_agent)
        .header("Accept", "application/vnd.github+json")
        .header("X-GitHub-Api-Version", "2022-11-28");
    if let Some(token) = env::var_os("GITHUB_TOKEN")
        .and_then(|value| value.into_string().ok())
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
    {
        request = request.header("Authorization", format!("Bearer {token}"));
    }
    Ok(request)
}

fn normalize_release_tag(value: &str) -> Result<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        bail!("release version must not be empty");
    }
    let tag = if trimmed.starts_with('v') {
        trimmed.to_owned()
    } else {
        format!("v{trimmed}")
    };
    parse_release_version(&tag)?;
    Ok(tag)
}

fn parse_release_version(tag: &str) -> Result<Version> {
    let version = tag
        .strip_prefix('v')
        .ok_or_else(|| anyhow!("release tag {tag} must start with v"))?;
    parse_version(version)
}

fn parse_version(version: &str) -> Result<Version> {
    Version::parse(version).with_context(|| format!("parse semver version {version}"))
}

fn select_release_asset(
    release: &GitHubRelease,
    tag: &str,
    target_triple: &str,
) -> Result<SelectedReleaseAsset> {
    let name = release_asset_name(tag, target_triple);
    let checksum_name = format!("{name}.sha256");
    let download_url = find_asset_download_url(release, &name)?;
    let checksum_download_url = find_asset_download_url(release, &checksum_name)?;
    Ok(SelectedReleaseAsset {
        name,
        download_url,
        checksum_name,
        checksum_download_url,
    })
}

fn find_asset_download_url(release: &GitHubRelease, name: &str) -> Result<String> {
    release
        .assets
        .iter()
        .find(|asset| asset.name == name)
        .map(|asset| asset.browser_download_url.clone())
        .ok_or_else(|| anyhow!("release {} is missing asset {name}", release.tag_name))
}

fn release_asset_name(tag: &str, target_triple: &str) -> String {
    format!("cbth-{tag}-{target_triple}")
}

fn parse_sha256_checksum(text: &str) -> Result<String> {
    let checksum = text
        .split_whitespace()
        .next()
        .ok_or_else(|| anyhow!("sha256 file was empty"))?
        .to_ascii_lowercase();
    if checksum.len() != 64 || !checksum.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        bail!("sha256 file did not start with a 64-character hex digest");
    }
    Ok(checksum)
}

fn verify_sha256(bytes: &[u8], expected_hex: &str) -> Result<()> {
    let actual = format!("{:x}", Sha256::digest(bytes));
    if actual != expected_hex {
        bail!("sha256 mismatch: expected {expected_hex}, got {actual}");
    }
    Ok(())
}

fn install_binary_atomically(path: &Path, bytes: &[u8]) -> Result<()> {
    let metadata = fs::metadata(path).with_context(|| format!("stat {}", path.display()))?;
    if !metadata.is_file() {
        bail!(
            "current executable is not a regular file: {}",
            path.display()
        );
    }
    let parent = path
        .parent()
        .with_context(|| format!("executable path {} has no parent", path.display()))?;
    let file_name = path
        .file_name()
        .and_then(|value| value.to_str())
        .with_context(|| format!("executable path {} has no valid file name", path.display()))?;
    let tmp = parent.join(format!(".{file_name}.{}.tmp", Uuid::now_v7()));

    let write_result = (|| -> Result<()> {
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&tmp)
            .with_context(|| format!("create {}", tmp.display()))?;
        file.write_all(bytes)
            .with_context(|| format!("write {}", tmp.display()))?;
        fs::set_permissions(&tmp, fs::Permissions::from_mode(0o755))
            .with_context(|| format!("chmod {}", tmp.display()))?;
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
        return Err(error).with_context(|| format!("replace executable {}", path.display()));
    }
    sync_dir(parent)?;
    Ok(())
}

fn current_target_triple() -> Option<&'static str> {
    target_triple_for_platform(
        env::consts::OS,
        env::consts::ARCH,
        current_target_env(),
        macos_host_supports_arm64(),
    )
}

fn current_target_env() -> &'static str {
    if cfg!(target_env = "gnu") {
        "gnu"
    } else if cfg!(target_env = "musl") {
        "musl"
    } else {
        ""
    }
}

fn macos_host_supports_arm64() -> bool {
    macos_host_supports_arm64_impl()
}

#[cfg(target_os = "macos")]
fn macos_host_supports_arm64_impl() -> bool {
    env::consts::ARCH == "aarch64"
        || Command::new("/usr/sbin/sysctl")
            .args(["-n", "hw.optional.arm64"])
            .output()
            .map(|output| {
                output.status.success() && String::from_utf8_lossy(&output.stdout).trim() == "1"
            })
            .unwrap_or(false)
}

#[cfg(not(target_os = "macos"))]
fn macos_host_supports_arm64_impl() -> bool {
    false
}

fn target_triple_for_platform(
    os: &str,
    arch: &str,
    target_env: &str,
    macos_arm64_host: bool,
) -> Option<&'static str> {
    match (os, arch, target_env, macos_arm64_host) {
        ("linux", "x86_64", "gnu", _) => Some("x86_64-unknown-linux-gnu"),
        ("macos", "aarch64", _, _) => Some("aarch64-apple-darwin"),
        ("macos", "x86_64", _, true) => Some("aarch64-apple-darwin"),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn release_tag_normalization_accepts_prefixed_and_bare_semver() {
        assert_eq!(normalize_release_tag("v0.1.0").unwrap(), "v0.1.0");
        assert_eq!(normalize_release_tag("0.1.0").unwrap(), "v0.1.0");
        assert!(normalize_release_tag("latest").is_err());
        assert!(normalize_release_tag("").is_err());
    }

    #[test]
    fn target_triple_detection_is_limited_to_v1_targets() {
        assert_eq!(
            target_triple_for_platform("linux", "x86_64", "gnu", false),
            Some("x86_64-unknown-linux-gnu")
        );
        assert_eq!(
            target_triple_for_platform("macos", "aarch64", "", true),
            Some("aarch64-apple-darwin")
        );
        assert_eq!(
            target_triple_for_platform("macos", "x86_64", "", true),
            Some("aarch64-apple-darwin")
        );
        assert_eq!(
            target_triple_for_platform("linux", "x86_64", "musl", false),
            None
        );
        assert_eq!(
            target_triple_for_platform("macos", "x86_64", "", false),
            None
        );
        assert_eq!(
            target_triple_for_platform("windows", "x86_64", "", false),
            None
        );
    }

    #[test]
    fn release_asset_selection_requires_binary_and_checksum_assets() {
        let release = GitHubRelease {
            tag_name: "v0.1.0".to_owned(),
            html_url: Some("https://example.test/release".to_owned()),
            assets: vec![
                GitHubAsset {
                    name: "cbth-v0.1.0-x86_64-unknown-linux-gnu".to_owned(),
                    browser_download_url: "https://example.test/cbth".to_owned(),
                },
                GitHubAsset {
                    name: "cbth-v0.1.0-x86_64-unknown-linux-gnu.sha256".to_owned(),
                    browser_download_url: "https://example.test/cbth.sha256".to_owned(),
                },
            ],
        };
        let selected = select_release_asset(&release, "v0.1.0", "x86_64-unknown-linux-gnu")
            .expect("select release asset");
        assert_eq!(selected.name, "cbth-v0.1.0-x86_64-unknown-linux-gnu");
        assert_eq!(
            selected.checksum_name,
            "cbth-v0.1.0-x86_64-unknown-linux-gnu.sha256"
        );

        let error = select_release_asset(&release, "v0.1.0", "aarch64-apple-darwin")
            .expect_err("missing asset should fail");
        assert!(format!("{error:#}").contains("missing asset"));
    }

    #[test]
    fn checksum_parser_and_verifier_reject_mismatch() {
        let bytes = b"cbth-test-binary";
        let checksum = format!("{:x}", Sha256::digest(bytes));
        let parsed = parse_sha256_checksum(&format!("{checksum}  cbth\n")).unwrap();
        assert_eq!(parsed, checksum);
        verify_sha256(bytes, &parsed).expect("checksum matches");
        assert!(verify_sha256(b"different", &parsed).is_err());
        assert!(parse_sha256_checksum("not-a-checksum").is_err());
    }

    #[test]
    fn install_binary_atomically_replaces_existing_executable() {
        let temp = tempfile::tempdir().expect("tempdir");
        let path = temp.path().join("cbth");
        fs::write(&path, b"old").expect("write old binary");
        fs::set_permissions(&path, fs::Permissions::from_mode(0o755)).expect("chmod old binary");

        install_binary_atomically(&path, b"new").expect("install new binary");

        assert_eq!(fs::read(&path).expect("read binary"), b"new");
        let mode = fs::metadata(&path)
            .expect("stat binary")
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(mode, 0o755);
    }
}
