---
id: 20260512-f4b2e32-desktop-transcript-relay-production-scanner
title: Desktop Transcript Relay Production Scanner
status: active
created: 2026-05-12
updated: 2026-05-12
branch: codex/desktop-transcript-relay-production-scanner
pr:
supersedes:
  - 20260511-0796bf3-desktop-transcript-relay-consumer-live-validation
superseded_by:
---

# Desktop Transcript Relay Production Scanner

## Summary

- Desktop transcript relay writeback is live-validated: heartbeat stdout envelopes land in trusted `function_call_output` carriers, and a non-Desktop consumer can apply the existing arm CAS transitions.
- This work moves the operator-driven `consume-transcript` flow toward production by adding daemon-owned rollout scanning, durable cursors, marker issuance, and retention cleanup.
- Desktop automatic delivery remains disabled in this workstream. Ready materialization, caller wake, `automation_update`, `note-boundary-crossed`, and artifact reads remain future work.

## Design Decisions

- Scanner runtime: daemon-owned worker, not a separate long-running process. It should only keep the daemon alive while active issued markers exist.
- Rollout source: explicit operator binding from `bridge_thread_id` to a resolved rollout path. Forks, archives, side chats, or new heartbeat threads require an explicit rebind; the scanner must not silently switch paths.
- Marker model: scanner only consumes issued, unexpired markers whose envelope fields match the issuance record. Unissued marker text in rollout is diagnostic evidence only.
- Arm lease: production `arm-accepted` envelopes do not contain `bridge_arm_lease_id`. The scanner resolves the durable lease from the current `arm_pending` attempt before calling `note-arm`.
- Existing manual `consume-transcript` remains as an operator/debug surface, but production scanner applies stricter allowlist and cursor rules.

## Implementation Plan

- Add durable scanner binding and marker tables.
- Add JSON CLI surfaces for scanner bind/status/scan-once and marker issue.
- Add heartbeat-safe relay emit aliases for `arm-pending` and `arm-accepted`.
- Integrate a bounded daemon scan worker with 2 second cadence, 256 line / 1 MiB per-tick limits, fail-closed degraded binding behavior, and sweep cleanup for expired marker/replay records.
- Update Desktop relay and bridge docs with the production scanner contract and remaining non-goals.

## Validation Plan

- Fake tests cover rollout binding cursor initialization, issued marker consumption, scanner-resolved lease arm acceptance, replay idempotency, rejected/unissued markers, partial trailing lines, truncate/drift degradation, daemon idle interaction, and retention cleanup.
- Local gate: Rust fmt, clippy, `cargo test --test desktop_foundation --locked`, locked/full test runs, project journal validation, `git diff --check`, and helper-backed Codex review.
- Opt-in live validation should bind the existing heartbeat rollout path, issue pending and arm-accepted markers, emit from heartbeat, and let the daemon scanner advance the fixture to `cooldown`.
