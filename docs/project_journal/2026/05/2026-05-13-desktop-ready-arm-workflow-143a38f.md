---
id: 20260513-143a38f-desktop-ready-arm-workflow
title: Desktop Ready Materialization And Arm Workflow
status: active
created: 2026-05-13
updated: 2026-05-13
branch: codex/desktop-ready-arm-workflow
pr:
supersedes:
  - 20260513-0c6d795-desktop-relay-scanner-live-validation
superseded_by:
---

# Desktop Ready Materialization And Arm Workflow

## Summary

- Desktop transcript relay scanner is live-validated against a real Desktop heartbeat rollout.
- This work connects that validated scanner path to production ready materialization: eligible Desktop head batches should appear in `ready-threads.json` with issued arm-pending markers.
- The bridge arm flow remains two-phase: heartbeat emits pending first, scanner advances to `arm_pending`, then a later preflight exports an arm-accepted marker for heartbeat emission and scanner consumption into `cooldown`.
- Desktop automatic caller continuation remains disabled in this workstream. Caller wake, `automation_update`, `note-boundary-crossed`, artifact payload reads, and end-to-end automatic delivery are future work.

## Planned Contract

- `bridge-preflight` may mutate cbth state because it already performs sweep and inbox publication; no-DB readers remain pure read-only.
- Each preflight may materialize at most one eligible Desktop ready entry.
- Eligible ready entries require a `bound` Desktop binding, validated read/writeback capabilities, matching binding fingerprint and transport generation, an open automatic safe head batch, no artifact-read requirement, remaining attempt budget, no active same-thread attempt, and no unquiesced armed generation or overdue pause.
- A ready entry must include caller/bridge tokens plus an issued `arm_pending_requested` marker: `source_thread_id`, `caller_automation_id`, `batch_id`, `attempt_id`, `generation`, `bridge_request_id`, `arm_pending_marker`, `marker_expires_at`, `snapshot_revision`, and `requires_artifact_read=false`.
- `arm_pending_bindings` should carry an issued `arm_accepted` marker only after the attempt is durably `arm_pending`; heartbeat output must not expose `bridge_arm_lease_id`.
- `claim-next-ready` remains a no-DB peek helper and must not create attempts, markers, leases, reservations, or cursor movement.

## Validation Plan

- Fake tests cover eligible ready materialization, exclusion of unsafe or blocked candidates, repeated preflight idempotency, pending scanner consumption, accepted marker export, accepted scanner consumption to `cooldown`, and unchanged `claim-next-ready` read-only semantics.
- Local gate: `cargo fmt --all -- --check`, `cargo clippy --locked --all-targets -- -D warnings`, `cargo test --test desktop_foundation --locked`, `cargo test --locked`, `cargo test`, project journal validation, `git diff --check`, and helper-backed Codex review.
- Live validation should use a synthetic Desktop binding and clean heartbeat rollout to prove preflight -> heartbeat emit pending -> scanner -> preflight -> heartbeat emit accepted -> scanner reaches `cooldown` with `delivery_attempt_count=1`.

## Remaining After This Work

- Production caller wake / `automation_update` prompt contract.
- Pause reconcile and binding quiescence lifecycle.
- `note-boundary-crossed` continuation boundary and recovery envelope.
- Artifact-read capability and policy for any future large-payload automatic path.
