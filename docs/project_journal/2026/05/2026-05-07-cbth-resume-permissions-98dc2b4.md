---
id: 20260507-98dc2b4-resume-permissions
title: cbth Resume Permission Drift Plan
status: completed
created: 2026-05-07
updated: 2026-05-07
branch: wip/cbth-resume-permissions
pr: https://github.com/JoeyTeng/codex-background-task-handler/pull/43
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

These follow-ups are intentionally deferred out of PR #43 and should happen in this order:

1. **Native resume cwd parity**: make `cbth resume` match Codex's default resume UX instead of silently treating the caller cwd as the only working-directory choice. Explicit forwarded `--cd` / `-C` should keep bypassing the prompt, but the no-override path should let the operator choose between the current directory and the thread's prior directory before the sidecar materializes startup state.
2. **Exact permission profile migration**: prefer `thread/resume.permissionProfile` as the canonical read-side permission snapshot when Codex provides it, keep legacy `approvalPolicy` / `sandbox` fallback for older or unrepresentable app-server responses, and continue emitting legacy pinned `approvalPolicy` / `sandboxPolicy` on `turn/start` until Codex adds a request-side permission-profile override.
3. **Codex CLI compatibility pinning**: add a soft validated-version range and capability-shape warning around managed startup and `cbth doctor cli`. The warning should tell users when the local Codex CLI is outside the cbth-tested range and should write diagnostics/audit evidence, but actual execution should fail only when required protocol fields are missing or cannot be parsed safely.

## Evidence

- Starting point: `98dc2b4 Add Desktop direct-helper preflight`
- Branch: `wip/cbth-resume-permissions`
- Implemented `cbth resume <thread-id> [-- <codex_args>]`, auto permission snapshots, effective permission pinning, drift warning/audit records, schema support, and documentation updates.
- Pinned `turn/start` legacy `sandboxPolicy` now omits Codex 0.128-rejected restricted-read fields (`access` / `readOnlyAccess`) while still using those fields for parsing, tightening, and drift/audit.
- Drift warning/audit now also compares raw `approvalPolicy` / `sandbox` details, so root or read-only access changes are visible even when the derived boolean permissions are unchanged.
- Proof invalidation and post-turn resync preserve the original startup permission cap for the same foreground managed session; only epoch-local current proof is cleared, and strict-safe delivery requires a refreshed current permission snapshot before reusing recorded risk booleans.
- Workspace-write pinning preserves safe nested writable-root intersections, such as a current root narrowed to a startup root's subdirectory, and rejects parent-directory components before containment checks.
- Review follow-up keeps forwarded native resume option scanning aligned with Codex single-value and variadic options, so `--add-dir` or `--image <FILE>...` cannot hide later `--cd` / sandbox overrides from the initial sidecar `thread/resume`.
- Single-source workspace-write pins now normalize and validate writable roots before emission, matching the intersection path and failing closed on relative or parent-directory roots.
- Native resume consumes forwarded `--cd` / `-C` into the single pinned foreground cwd instead of passing duplicate cwd flags to Codex.
- Native resume rejects forwarded `--remote` / `--remote-auth-token-env` overrides so the foreground Codex process cannot bypass the daemon-owned managed app-server.
- Native resume rejects forwarded `--add-dir` because Codex `thread/resume` cannot faithfully carry additional writable roots; failing closed avoids a startup permission snapshot that omits foreground writable-root intent.
- Startup permission idempotency now compares the raw startup snapshot JSON as well as derived booleans, so a lost response cannot repin a different raw sandbox under the same risk booleans.
- Fresh unmaterialized `--new-thread` keeps default passive proof with auto permissions even before a startup permission snapshot exists; automatic delivery still requires a trusted snapshot.
- Default `auto` reattach no longer treats the fail-closed initial false profile as fixed, avoiding profile-drift replacement after a prior auto-derived effective profile was wider.
- Passive auto sessions continue current-state activity/capability sync when `thread/resume` lacks permission fields; automatic delivery remains disabled until a trusted snapshot appears.
- Mixed explicit/auto binds enforce explicit dimensions during profile drift checks while allowing only auto dimensions to float.
- Explicit no-write effective permissions now downgrade `workspaceWrite` snapshots to a protocol-valid legacy `readOnly` sandboxPolicy instead of emitting rejected read-access fields.
- Validation:
  - `cargo fmt --check`
  - `cargo clippy --locked --all-targets -- -D warnings`
  - `cargo test --locked`
  - `cargo test --locked cbth_resume_initial_sidecar_resume_carries_foreground_overrides`
  - `cargo test --locked pinned_turn_start_overrides_reject`
  - `cargo test --locked cli_session_note_permissions_rejects_startup_raw_snapshot_drift`
  - `cargo test --locked cli_session_invalidate_proof_preserves_startup_permission_cap`
  - `cargo test --locked cli_run_new_thread_bootstraps_thread_then_preserves_foreground_model`
  - `cargo test --locked cli_run_trusted_all_auto_delivery_resyncs_after_terminal_for_next_head`
  - `cargo test --locked cbth_resume_rejects_forwarded_remote_override`
  - `cargo test --locked cbth_resume_rejects_forwarded_add_dir`
  - `cargo test --locked cbth_resume_invalid_forwarded_args_do_not_start_app_server`
  - `cargo test --locked cli_run_passive_auto_missing_permission_snapshot_keeps_current_state_sync`
  - `cargo test --locked cli_session_bind_auto_profile_enforces_explicit_dimensions`
  - `cargo test --test cli_run`
  - `uv run python /Users/hoteng/.codex/skills/project-journal/scripts/project_journal.py validate --repo /Users/hoteng/.codex/worktrees/aef0/codex-background-task-handler`
