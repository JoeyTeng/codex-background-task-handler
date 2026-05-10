---
id: 20260507-922c89c-desktop-writeback-live-validation
title: Desktop Writeback Helper Live Validation
status: blocked
created: 2026-05-07
updated: 2026-05-07
branch: codex/desktop-writeback-live-validation
pr:
supersedes: []
superseded_by:
---

# Desktop Writeback Helper Live Validation

## Summary

- This work validates whether real Codex Desktop heartbeat can execute the narrow Desktop writeback helpers without approval.
- The validation targets `cbth desktop note-arm-pending` and `cbth desktop note-arm` only.
- Desktop automatic delivery, caller wake, `automation_update`, `note-boundary-crossed`, artifact reads, and ready materialization remain out of scope.

## Planned Changes

- Add a hidden validation-only fixture command:
  `cbth desktop validation prepare-writeback-fixture --source-thread-id <thread-id> --caller-automation-id <automation-id> [--bridge-request-id <id>] --json`.
- The fixture should repair the Desktop binding, create an open safe Desktop batch, create a current-generation `adapter_kind=desktop` prepared attempt, and return the CAS tokens needed by the heartbeat validation prompt.
- Add `docs/DESKTOP_WRITEBACK_HELPER_LIVE_VALIDATION.md` with the operator setup, heartbeat prompt, success criteria, capability repair step, failure handling, and cleanup guidance.
- If live validation succeeds, record evidence and allow operator repair to set `writeback_capability=validated` while keeping `artifact_read_capability=unknown`.
- If live validation fails, leave `writeback_capability=unknown` and record the blocker instead of weakening Desktop bridge safety boundaries.

## Validation Plan

- Fake/default tests must prove the fixture creates a safe prepared Desktop attempt and bound binding.
- Tests must prove empty ids and incompatible active bindings fail closed.
- Tests must cover `note-arm-pending` and `note-arm` on the fixture path, including idempotent retries and no duplicate delivery-attempt count increments.
- Local gate before PR: `cargo fmt --all -- --check`, `cargo clippy --locked --all-targets -- -D warnings`, `cargo test --test desktop_foundation --locked`, `cargo test --locked`, `cargo test`, project journal validate, `git diff --check`, and helper-backed Codex review.

## Current State

- `read_transport_capability=validated` covers the no-DB Desktop inbox read helper path.
- The validation harness adds hidden `cbth desktop validation prepare-writeback-fixture ... --json` so operator shell can create a safe prepared Desktop attempt without manual SQLite edits.
- Fake coverage proves the fixture creates a safe batch / prepared attempt / bound binding, rejects empty or incompatible inputs, and drives `note-arm-pending` plus `note-arm` through idempotent retries without duplicate delivery-attempt count increments.
- `docs/DESKTOP_WRITEBACK_HELPER_LIVE_VALIDATION.md` records the operator setup, heartbeat prompt, post-run verification, capability repair, and cleanup flow.
- Real Desktop heartbeat attempted `note-arm-pending` against fixture `attempt_id=019e0400-eee1-7c73-8792-361330f0b674` and failed before state mutation: `open startup lock /Users/hoteng/.cbth/run/startup.lock: Operation not permitted`.
- Operator inspection confirmed the attempt stayed `prepared`, `delivery_attempt_count=0`, and `writeback_capability=unknown`; the synthetic fixture batch was closed with `operator_closed_unconfirmed`.
- The current writeback helper path is blocked by daemon-routed startup-lock access from Desktop heartbeat. The next design must avoid heartbeat-owned daemon autostart / startup-lock access, and should also account for prior SQLite WAL denial in Desktop heartbeat.
- Base PR #42 merge commit: `922c89cfbdfe6a92b4bf42f748ed0b71018a8239`.
- Implementation branch started from latest `master` at `bcc86b7a5d5d` after the `v0.1.1` release follow-up.

## Evidence

- Live writeback evidence: [Desktop live preflight evidence](../../../DESKTOP_LIVE_PREFLIGHT_EVIDENCE.md#2026-05-07-attempt-writeback-helpers-blocked-by-startup-lock)
- Desktop bridge foundation: [Desktop bridge foundation](../../../DESKTOP_BRIDGE_FOUNDATION.md)
- Desktop no-DB reader journal: [2026-05-07-desktop-no-db-inbox-reader-98dc2b4.md](2026-05-07-desktop-no-db-inbox-reader-98dc2b4.md)
- Writeback helper foundation journal: [2026-05-07-desktop-writeback-helper-foundation-d740ea1.md](2026-05-07-desktop-writeback-helper-foundation-d740ea1.md)
- Active backlog: [2026-05-05-current-follow-ups-bbe4003.md](2026-05-05-current-follow-ups-bbe4003.md)
