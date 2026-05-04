#!/bin/sh
set -eu

repo="${CBTH_REPO:-JoeyTeng/codex-background-task-handler}"
api_base="${CBTH_GITHUB_API:-https://api.github.com}"
github_base="${CBTH_GITHUB_BASE:-https://github.com}"
install_dir="${CBTH_INSTALL_DIR:-$HOME/.local/bin}"
version="${CBTH_VERSION:-}"

log() {
  printf '%s\n' "$*" >&2
}

fail() {
  log "install-cbth: $*"
  exit 1
}

need_cmd() {
  command -v "$1" >/dev/null 2>&1 || fail "missing required command: $1"
}

detect_target() {
  os="$(uname -s)"
  arch="$(uname -m)"
  case "${os}:${arch}" in
    Linux:x86_64)
      if command -v getconf >/dev/null 2>&1 && getconf GNU_LIBC_VERSION >/dev/null 2>&1; then
        printf '%s\n' "x86_64-unknown-linux-gnu"
      else
        fail "unsupported Linux libc: only glibc x86_64 release assets are available"
      fi
      ;;
    Darwin:arm64) printf '%s\n' "aarch64-apple-darwin" ;;
    *) fail "unsupported platform: ${os} ${arch}; supported: Linux x86_64 glibc, macOS arm64" ;;
  esac
}

normalize_tag() {
  case "$1" in
    v*) tag="$1" ;;
    *) tag="v$1" ;;
  esac
  printf '%s\n' "$tag" | grep -E '^v[0-9]+\.[0-9]+\.[0-9]+$' >/dev/null 2>&1 \
    || fail "invalid release version: $1; expected vX.Y.Z"
  printf '%s\n' "$tag"
}

curl_to_file() {
  url="$1"
  dest="$2"
  if [ -n "${GITHUB_TOKEN:-}" ]; then
    curl -fsSL -H "Authorization: Bearer ${GITHUB_TOKEN}" "$url" -o "$dest"
  else
    curl -fsSL "$url" -o "$dest"
  fi
}

latest_tag() {
  tmp_json="$1"
  url="${api_base}/repos/${repo}/releases/latest"
  if [ -n "${GITHUB_TOKEN:-}" ]; then
    curl -fsSL \
      -H "Authorization: Bearer ${GITHUB_TOKEN}" \
      -H "Accept: application/vnd.github+json" \
      "$url" -o "$tmp_json"
  else
    curl -fsSL -H "Accept: application/vnd.github+json" "$url" -o "$tmp_json"
  fi
  tag="$(sed -n 's/.*"tag_name"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p' "$tmp_json" | head -n 1)"
  [ -n "$tag" ] || fail "could not read latest release tag from GitHub API"
  normalize_tag "$tag"
}

sha256_file() {
  file="$1"
  if command -v shasum >/dev/null 2>&1; then
    shasum -a 256 "$file" | awk '{print $1}'
  elif command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$file" | awk '{print $1}'
  else
    fail "missing checksum command: shasum or sha256sum"
  fi
}

need_cmd uname
need_cmd curl
need_cmd grep
need_cmd sed
need_cmd awk
need_cmd mktemp

target="$(detect_target)"
tmp_dir="$(mktemp -d "${TMPDIR:-/tmp}/cbth-install.XXXXXX")"
cleanup() {
  rm -rf "$tmp_dir"
}
trap cleanup EXIT INT TERM

if [ -n "$version" ]; then
  tag="$(normalize_tag "$version")"
else
  tag="$(latest_tag "$tmp_dir/latest.json")"
fi

asset="cbth-${tag}-${target}"
release_base="${github_base}/${repo}/releases/download/${tag}"
binary_url="${release_base}/${asset}"
checksum_url="${binary_url}.sha256"
binary_path="${tmp_dir}/${asset}"
checksum_path="${tmp_dir}/${asset}.sha256"

log "install-cbth: downloading ${asset}"
curl_to_file "$binary_url" "$binary_path"
curl_to_file "$checksum_url" "$checksum_path"

expected="$(awk '{print $1; exit}' "$checksum_path" | tr 'A-F' 'a-f')"
case "$expected" in
  *[!0123456789abcdef]* | "") fail "invalid checksum file for ${asset}" ;;
  *) ;;
esac
[ "${#expected}" -eq 64 ] || fail "invalid checksum length for ${asset}"
actual="$(sha256_file "$binary_path" | tr 'A-F' 'a-f')"
[ "$actual" = "$expected" ] || fail "checksum mismatch for ${asset}"

mkdir -p "$install_dir"
tmp_install="${install_dir}/.cbth.$$"
cp "$binary_path" "$tmp_install"
chmod 0755 "$tmp_install"
mv "$tmp_install" "${install_dir}/cbth"

log "install-cbth: installed ${install_dir}/cbth (${tag}, ${target})"
log "install-cbth: run 'cbth doctor cli' to verify local readiness"
