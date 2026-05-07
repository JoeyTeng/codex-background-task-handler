---
id: 20260507-698664c-resume-followups
title: cbth Resume Follow-ups
status: completed
created: 2026-05-07
updated: 2026-05-07
branch: wip/cbth-resume-followups
pr: https://github.com/JoeyTeng/codex-background-task-handler/pull/45
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
- The managed app-server lease refresher starts before any interactive cwd prompt, so an operator pause cannot let the app-server reservation expire before the foreground `codex resume` process launches.
- Auto permission snapshots prefer `thread/resume.permissionProfile` when available. Legacy `approvalPolicy` / `sandbox` remains the fallback, and canonical/legacy disagreement on derived network or write permissions fails closed.
- Startup, current, effective, and audit snapshot JSON now records the permission snapshot source and canonical profile body when present. Request-side `turn/start` pinning still emits legacy `approvalPolicy` / `sandboxPolicy` because Codex 0.128 exposes canonical profile state on the read side while accepting legacy override fields.
- Managed startup and `cbth doctor cli` run `codex --version`, report compatibility details, and warn when the local CLI is outside `0.128.x`; protocol parsing remains the fail-closed safety gate.

## Next Steps

- Continue the broader CLI / daemon recovery backlog from [current follow-ups](2026-05-05-current-follow-ups-bbe4003.md).
- Revisit request-side exact permission-profile pinning only after Codex exposes a `turn/start` override field for canonical profiles.

## Evidence

- Base: `698664c` from PR #43; branch also merges `bcc86b7` / `v0.1.1` before review.
- Branch: `wip/cbth-resume-followups`.
- Review:
  - Internal `codex-review` on the initial branch range reported no blocking findings.
  - External bounded review found that the interactive cwd prompt could outlive the pre-refresher app-server lease; the follow-up fix starts lease refresh before cwd resolution and extends the no-`--cd` resume test to assert the initial sidecar `thread/resume` omits `cwd`.
  - Internal review then found a permission-profile/legacy write-scope mismatch hole; the fix now rejects canonical profiles whose write entries do not cover legacy writable roots, tmp access, or external sandbox shape.
  - Internal `codex-readonly` fallback found that `thread/read` cwd parsing only handled nested `thread.cwd`, and that permission drift did not report `permissionProfile` body changes. The final fix accepts both nested and top-level cwd response shapes and records `permission_profile` drift when the canonical body changes.
  - Final internal `codex-readonly` review on `origin/master..HEAD` returned `LGTM`.
  - Clean-context reviewer on PR #45 found that interactive resume cwd probing accepted `thread/read` cwd without proving the response belonged to the requested thread. The fix now requires nested `thread.id`, top-level `id`, or top-level `threadId` to explicitly match before using cwd, and treats missing/foreign ids as no history cwd so native no-`--cd` fallback remains intact.
  - Codex PR review gate found that canonical `permissionProfile` workspace writes can use `special.kind: "project_roots"` in normal Codex 0.128 responses. The fix treats unsuffixed `project_roots` as covering legacy workspace writable roots while still rejecting narrower `project_roots` subpaths as unrepresentable for legacy sandbox pinning.
  - A later clean-context review found four follow-up gaps: mixed-shape `thread/read` cwd identity, permission-affecting `--config` forwarding, malformed canonical `permissionProfile` shapes, and nested-root drift direction. The next fix requires the cwd-bearing nested thread to carry a matching id, rejects known sandbox-scope config overrides, validates canonical profile object/network/fileSystem shapes strictly, and computes root drift by normalized containment instead of exact string-set subset checks.
- Validation:
  - `cargo fmt --all -- --check`
  - `cargo clippy --locked --all-targets -- -D warnings`
  - `cargo test --locked`
  - `cargo test --locked thread_read_cwd_reads_nested_and_top_level_shapes --lib`
  - `cargo test --locked thread_read_cwd --lib`
  - `cargo test --locked permission_snapshot_rejects_malformed_permission_profiles --lib`
  - `cargo test --locked permission_drift_tracks_nested_workspace_roots_by_containment --lib`
  - `cargo test --locked permission_snapshot_accepts_project_roots_permission_profile_for_legacy_workspace_write --lib`
  - `cargo test --locked permission_snapshot_rejects_project_roots_subpath_for_legacy_workspace_write --lib`
  - `cargo test --locked cbth_resume_rejects_permission_affecting_config_overrides --test cli_run`
  - `cargo test --locked permission_drift_tracks_permission_profile_body_changes --lib`
  - `CARGO_TARGET_DIR=.codex-tmp/cargo-target-precommit git commit -S ...` pre-commit hook passed `cargo fmt --all`, `cargo clippy --locked --all-targets -- -D warnings`, and `cargo test --locked` in an isolated target directory after the shared target produced a cross-worktree binary mismatch.
  - `uv run python /Users/hoteng/.codex/skills/project-journal/scripts/project_journal.py validate --repo /Users/hoteng/.codex/worktrees/aef0/codex-background-task-handler`
