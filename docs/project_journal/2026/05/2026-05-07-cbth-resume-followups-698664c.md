---
id: 20260507-698664c-resume-followups
title: cbth Resume Follow-ups
status: completed
created: 2026-05-07
updated: 2026-05-07
branch: wip/cbth-resume-followups
pr:
supersedes: []
superseded_by:
---

# cbth Resume Follow-ups

## Summary

- Complete the three follow-ups deferred from PR #43 in order: native resume cwd UX parity, canonical `permissionProfile` read-side parsing, and soft Codex CLI version compatibility warnings.
- Keep `cbth resume <thread-id> [-- <codex_args>]` on the managed app-server / sidecar path while avoiding silent caller-cwd override when the operator did not provide `--cd`.
- Treat `codex-cli 0.128.x` as the currently validated range and warn, rather than fail, when the local Codex CLI reports a different or unparsable version.

## Current State

- `cbth resume` now forwards a single explicit cwd only when `--cd` / `-C` is supplied or an interactive cwd selection has been made. Non-interactive no-override resumes omit `--cd`, so the native resume path can preserve the previous thread cwd instead of forcing the caller cwd.
- In interactive terminals, `cbth resume` reads the previous thread cwd with `thread/read(includeTurns=false)` before startup materialization and prompts between the prior thread cwd and the current cwd when they differ.
- Auto permission snapshots prefer `thread/resume.permissionProfile` when available. Legacy `approvalPolicy` / `sandbox` remains the fallback, and canonical/legacy disagreement on derived network or write permissions fails closed.
- Startup, current, effective, and audit snapshot JSON now records the permission snapshot source and canonical profile body when present. Request-side `turn/start` pinning still emits legacy `approvalPolicy` / `sandboxPolicy` because Codex 0.128 exposes canonical profile state on the read side while accepting legacy override fields.
- Managed startup and `cbth doctor cli` run `codex --version`, report compatibility details, and warn when the local CLI is outside `0.128.x`; protocol parsing remains the fail-closed safety gate.

## Next Steps

- Continue the broader CLI / daemon recovery backlog from [current follow-ups](2026-05-05-current-follow-ups-bbe4003.md).
- Revisit request-side exact permission-profile pinning only after Codex exposes a `turn/start` override field for canonical profiles.

## Evidence

- Base: `698664c` from PR #43; branch also merges `bcc86b7` / `v0.1.1` before review.
- Branch: `wip/cbth-resume-followups`.
- Validation:
  - `cargo fmt --all -- --check`
  - `cargo check --locked`
  - `cargo clippy --locked --all-targets -- -D warnings`
  - `cargo test --locked`
  - `uv run python /Users/hoteng/.codex/skills/project-journal/scripts/project_journal.py validate --repo /Users/hoteng/.codex/worktrees/aef0/codex-background-task-handler`
