---
id: 20260513-143a38f-desktop-ready-arm-workflow
title: Desktop Ready Materialization And Arm Workflow
status: completed
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

## Implemented Contract

- `bridge-preflight` may mutate cbth state because it already performs sweep and inbox publication; no-DB readers remain pure read-only.
- Each preflight materializes at most one eligible Desktop ready entry.
- Eligible ready entries require a `bound` Desktop binding, validated read/writeback capabilities, matching binding fingerprint and transport generation, an open automatic safe head batch, no artifact-read requirement, remaining attempt budget, no active same-thread attempt, and no unquiesced armed generation or overdue pause.
- A ready entry must include caller/bridge tokens plus an issued `arm_pending_requested` marker: `source_thread_id`, `caller_automation_id`, `batch_id`, `attempt_id`, `generation`, `bridge_request_id`, `arm_pending_marker`, `marker_expires_at`, `snapshot_revision`, and `requires_artifact_read=false`.
- `arm_pending_bindings` carries an issued `arm_accepted` marker only after the attempt is durably `arm_pending`; heartbeat output does not expose `bridge_arm_lease_id`.
- `claim-next-ready` remains a no-DB peek helper and must not create attempts, markers, leases, reservations, or cursor movement.

## Implementation Result

- Added ready materialization in `bridge-preflight`: eligible safe Desktop head batches are converted to a `prepared` Desktop attempt and a ready entry with an issued `arm_pending_requested` marker.
- Repeated preflight reuses the same current prepared attempt and unexpired issued pending marker instead of creating duplicate attempts or markers.
- Added arm-pending export marker issuance: eligible durable `arm_pending` attempts receive issued `arm_accepted` markers in `arm-pending-bindings.json`; the marker is not issued unless read/writeback capabilities remain validated and the binding still matches installation state.
- `claim-next-ready` remains a no-DB read helper over the published ready snapshot.
- Full Desktop automatic delivery remains disabled; this work stops at durable `cooldown`.

## Validation Evidence

- `cargo check` passed during implementation.
- `cargo fmt --all -- --check` passed after formatting.
- `cargo test --test desktop_foundation --locked` passed with new coverage for ready materialization, ineligible candidate filtering, repeated preflight idempotency, pending scanner consumption, accepted marker export, accepted scanner consumption to `cooldown`, and unchanged `claim-next-ready` read-only semantics.
- `cargo clippy --locked --all-targets -- -D warnings` passed.
- `cargo test --locked` passed.
- `cargo test` passed.
- Project journal validation passed.
- `git diff --check` passed.
- Helper-backed review and optional live validation remain PR validation steps.

## Remaining After This Work

- Production caller wake / `automation_update` prompt contract.
- Pause reconcile and binding quiescence lifecycle.
- `note-boundary-crossed` continuation boundary and recovery envelope.
- Artifact-read capability and policy for any future large-payload automatic path.
