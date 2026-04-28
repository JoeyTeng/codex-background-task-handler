#!/usr/bin/env bash
set -euo pipefail

repo_root=$(git rev-parse --show-toplevel 2>/dev/null) || {
  printf 'install-git-hooks: not inside a Git worktree\n' >&2
  exit 1
}

cd "$repo_root"

hook_path=".githooks/pre-commit"

if [ ! -f "$hook_path" ]; then
  printf 'install-git-hooks: missing %s\n' "$hook_path" >&2
  exit 1
fi

chmod +x "$hook_path"

if [ ! -x "$hook_path" ]; then
  printf 'install-git-hooks: %s is not executable\n' "$hook_path" >&2
  exit 1
fi

git config core.hooksPath .githooks

printf 'install-git-hooks: configured core.hooksPath=.githooks\n'
