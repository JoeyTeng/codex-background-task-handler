---
id: 20260507-d740ea1-desktop-writeback-helper
title: Desktop Writeback Helper Foundation
status: active
created: 2026-05-07
updated: 2026-05-07
branch: codex/desktop-writeback-helper-foundation
pr:
supersedes: []
superseded_by:
---

# Desktop Writeback Helper Foundation

## Summary

- The next Desktop bridge slice is the writeback helper foundation for arm lifecycle state.
- Real Desktop heartbeat has already validated no-DB inbox reads, but writeback capability is still `unknown`.
- This work adds durable helper primitives for future heartbeat agents without enabling automatic Desktop delivery.

## Planned Changes

- Add `cbth desktop note-arm-pending --source-thread-id <thread-id> --attempt-id <attempt-id> --generation <generation> --bridge-request-id <request-id> --json`.
- Add `cbth desktop note-arm --source-thread-id <thread-id> --attempt-id <attempt-id> --generation <generation> --bridge-request-id <request-id> --bridge-arm-lease-id <lease-id> --json`.
- Keep `claim-next-ready` read-only; durable state advancement begins at `note-arm-pending`.
- Implement CAS and idempotency so stale, mismatched, or duplicate helper calls fail closed or return a stable no-op result without creating duplicate delivery attempts.
- Export real `arm_pending_bindings` and `pause_due_bindings` data from `bridge-preflight` for later heartbeat reconciliation.
- Add a daemon capability gate for the writeback helper foundation so new clients cannot silently use an old daemon.

## Out Of Scope

- Desktop automatic delivery, caller heartbeat wake, `automation_update`, artifact payload reads, and `note-boundary-crossed`.
- `writeback_capability=validated`; that remains a later live Desktop heartbeat validation step.
- CLI delivery behavior or the native `codex --remote` foreground interaction model.

## Validation Plan

- Cover `note-arm-pending` success, idempotent retry, mismatched generation, mismatched attempt, missing binding, non-head batch, and non-prepared attempt behavior.
- Cover `note-arm` success, idempotent retry, no duplicate attempt-count increment, lease or request mismatch, and stale-generation behavior.
- Cover `bridge-preflight` snapshot export for `arm_pending_bindings` and `pause_due_bindings`.
- Run `cargo fmt --all -- --check`, `cargo clippy --locked --all-targets -- -D warnings`, `cargo test --locked`, `cargo test`, project journal validation, `git diff --check`, and helper-backed Codex review before PR.

## Evidence

- Base merge commit: `d740ea195101c7ee927e4fda477a5fc18b2db428`
- Desktop no-DB reader journal: [2026-05-07-desktop-no-db-inbox-reader-98dc2b4.md](2026-05-07-desktop-no-db-inbox-reader-98dc2b4.md)
- Active backlog: [2026-05-05-current-follow-ups-bbe4003.md](2026-05-05-current-follow-ups-bbe4003.md)
