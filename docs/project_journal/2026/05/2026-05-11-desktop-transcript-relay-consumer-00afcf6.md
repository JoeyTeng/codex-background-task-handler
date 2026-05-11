---
id: 20260511-00afcf6-desktop-transcript-relay-consumer
title: Desktop Transcript Relay Writeback Consumer Foundation
status: active
created: 2026-05-11
updated: 2026-05-11
branch: codex/desktop-transcript-relay-consumer-foundation
pr:
supersedes:
  - 20260511-eeef583-desktop-transcript-relay-validation
superseded_by: 20260511-0796bf3-desktop-transcript-relay-consumer-live-validation
---

# Desktop Transcript Relay Writeback Consumer Foundation

## Summary

- The Desktop transcript relay transport has already been live-validated: helper stdout emitted by a Desktop heartbeat automation appears in Codex rollout as `response_item.payload.type=function_call_output`.
- This work adds the first durable non-Desktop consumer for that carrier: an external operator / sidecar can scan a rollout marker and advance the existing Desktop `note-arm-pending` / `note-arm` CAS state outside the Desktop sandbox.
- Desktop automatic delivery remains disabled. This foundation does not create ready attempts, call `automation_update`, wake caller heartbeats, implement `note-boundary-crossed`, read artifacts, or repair `writeback_capability` automatically.

## Implemented Contract

- Heartbeat-side emit helpers remain stdout-only and do not open SQLite, connect daemon IPC, touch `startup.lock`, or write `~/.cbth`:
  `cbth desktop validation emit-transcript-arm-pending ... --marker <marker> --json`
  `cbth desktop validation emit-transcript-arm ... --bridge-arm-lease-id <lease-id> --marker <marker> --json`
- The non-Desktop consumer is:
  `cbth desktop relay consume-transcript --rollout-path <rollout-jsonl> --marker <marker> --json`
- The consumer accepts exactly one trusted `function_call_output` envelope from the scanner. Prompt copies, assistant final text, duplicate trusted envelopes, malformed trusted envelopes, unsupported envelope kinds, and wrong markers fail closed without mutation.
- Durable replay protection is marker-scoped: the first accepted marker stores a canonical envelope hash and CAS outcome; replaying the same marker / hash returns the stored outcome, while the same marker with a different hash fails closed.

## Validation Plan

- Fake/default tests cover stdout-only emitters, trusted relay consumption for `arm_pending_requested`, trusted relay consumption for `arm_requested`, same marker/hash replay idempotency, same marker/different hash rejection, prompt-only rejection, duplicate trusted rejection, malformed trusted rejection, and wrong-marker rejection.
- Local gate: `cargo fmt --all -- --check`, `cargo clippy --locked --all-targets -- -D warnings`, `cargo test --test desktop_foundation --locked`, `cargo test --locked`, `cargo test`, project journal validate, and `git diff --check`.
- Future live validation should use a real Desktop heartbeat to emit arm-pending and arm envelopes, then run `consume-transcript` from a normal shell and verify the attempt reaches `arm_pending` then `cooldown` with `delivery_attempt_count=1`.

## Evidence

- Base branch: `master`
- Base commit: `00afcf6 Validate Desktop transcript relay side channel`
- Transport evidence: [Desktop transcript relay validation](2026-05-11-desktop-transcript-relay-validation-eeef583.md)
- Design docs: [Desktop transcript relay validation](../../../DESKTOP_TRANSCRIPT_RELAY_VALIDATION.md), [Desktop bridge foundation](../../../DESKTOP_BRIDGE_FOUNDATION.md)

## Current State

- Transcript relay can now carry writeback requests to a durable consumer in fake/default tests.
- Follow-up live validation succeeded in [Desktop transcript relay consumer live validation](2026-05-11-desktop-transcript-relay-consumer-live-validation-0796bf3.md), and `writeback_capability` was explicitly repaired to `validated`.
- Production sidecar tailing, rollout auto-discovery, durable scan cursors, nonce generation / issuance, ready materialization, caller wake, pause reconcile, and boundary crossing remain future work.

## Next Steps

- Add a production sidecar scanner with durable scan cursors and marker issuance after live validation proves the manual consumer path.
- Implement ready materialization and caller wake only after writeback consumption, pause reconcile, and continuation-boundary contracts are validated.
