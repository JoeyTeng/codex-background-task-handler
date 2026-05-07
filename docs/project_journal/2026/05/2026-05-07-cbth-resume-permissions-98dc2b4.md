---
id: 20260507-98dc2b4-resume-permissions
title: cbth Resume Permission Drift Plan
status: completed
created: 2026-05-07
updated: 2026-05-07
branch: wip/cbth-resume-permissions
pr:
supersedes: []
superseded_by:
---

# cbth Resume Permission Drift Plan

## Summary

- Add `cbth resume <thread-id> [-- <codex_args>]` as the operator-facing wrapper for resuming an existing Codex thread through the managed app-server / sidecar path.
- Replace mandatory `--session-allows-approval`, `--session-allows-network`, and `--session-allows-write-access` inputs with an `auto` default while preserving explicit `true` / `false` overrides.
- Treat the startup permission snapshot as the maximum allowed risk. If later permissions tighten, continue with the tighter current permissions. If later permissions loosen, keep using the tighter startup permissions. Record every drift as a warning and audit event.

## Current State

- Existing `cbth cli run --bind-thread-id <thread-id>` already manages a daemon-owned app-server, a foreground Codex TUI, a passive sidecar, activity proof, capability proof, and `turn/start` automatic delivery.
- Current session risk profile fields are immutable booleans persisted at bind time; auto derivation from `thread/resume` and per-delivery permission pinning are not implemented yet.
- Codex app-server 0.128 exposes `approvalPolicy` and legacy `sandbox` in `thread/resume`; exact permission-profile support should be preferred later if it becomes available in the response.

## Implementation Plan

- Add `cbth resume <thread-id> [-- <codex_args>]` and run the foreground as `codex resume <thread-id> --remote <url> --cd <cwd> ...`, reusing the existing fixed-thread managed-session flow.
- Introduce a tri-state permission CLI value for the three `session_allows_*` flags: explicit `true`, explicit `false`, or default `auto`. Auto must fail closed if the app-server snapshot cannot be parsed.
- Parse permission snapshots from `thread/resume`: derive approval from `approvalPolicy`, network and write access from `sandbox`, and treat unknown or missing risk-critical fields as untrusted for automatic strict-safe delivery.
- Persist the first trusted auto snapshot as the managed session startup permission snapshot, then compare every later pre-delivery snapshot against it.
- Before automatic `turn/start`, compute effective permissions per dimension with `effective_allows = startup_allows && current_allows`, then pass matching pinned `approvalPolicy` and `sandboxPolicy` in the `turn/start` request.
- When startup and current snapshots differ, emit a stderr warning and write an audit record containing startup, current, effective, drift direction, and changed dimensions.

## Test Plan

- Verify `cbth resume thread-1 -- --model gpt-5.5` launches foreground Codex as `codex resume thread-1 --remote <url> --cd <cwd> --model gpt-5.5`.
- Cover permission derivation: `approvalPolicy=never` plus `sandbox=readOnly` and `networkAccess=false` derives all false; approval-enabled, network-enabled, write-enabled, unknown, and missing shapes fail closed or derive the higher-risk dimension.
- Cover permission drift: startup read-only plus current workspace-write pins read-only; startup workspace-write plus current read-only pins read-only; mixed network/write changes choose the tighter value per dimension.
- Assert drift writes both stderr warning and audit details.
- Preserve existing regression coverage for explicit false flags, profile mismatch, manual batch blockers, and active attempt blockers.

## Next Steps

- Implement the CLI surface and permission snapshot model.
- Add store/schema support for startup permission snapshots and drift audit evidence.
- Update fake app-server tests and run the relevant Rust test suites before review.

## Evidence

- Starting point: `98dc2b4 Add Desktop direct-helper preflight`
- Branch: `wip/cbth-resume-permissions`
- Implemented `cbth resume <thread-id> [-- <codex_args>]`, auto permission snapshots, effective permission pinning, drift warning/audit records, schema support, and documentation updates.
- Pinned `turn/start` legacy `sandboxPolicy` now omits Codex 0.128-rejected restricted-read fields (`access` / `readOnlyAccess`) while still using those fields for parsing, tightening, and drift/audit.
- Drift warning/audit now also compares raw `approvalPolicy` / `sandbox` details, so root or read-only access changes are visible even when the derived boolean permissions are unchanged.
- Proof invalidation and post-turn resync preserve the original startup permission cap for the same foreground managed session; only epoch-local current proof is cleared, and strict-safe delivery requires a refreshed current permission snapshot before reusing recorded risk booleans.
- Workspace-write pinning preserves safe nested writable-root intersections, such as a current root narrowed to a startup root's subdirectory.
- Explicit no-write effective permissions now downgrade `workspaceWrite` snapshots to a protocol-valid legacy `readOnly` sandboxPolicy instead of emitting rejected read-access fields.
- Validation:
  - `cargo fmt --check`
  - `cargo clippy --locked --all-targets -- -D warnings`
  - `cargo test --locked`
  - `cargo test --test cli_run`
  - `uv run python /Users/hoteng/.codex/skills/project-journal/scripts/project_journal.py validate --repo /Users/hoteng/.codex/worktrees/aef0/codex-background-task-handler`
