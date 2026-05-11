---
id: 20260511-0796bf3-desktop-transcript-relay-consumer-live-validation
title: Desktop Transcript Relay Consumer Live Validation
status: completed
created: 2026-05-11
updated: 2026-05-11
branch: codex/desktop-transcript-relay-consumer-live-validation
pr: https://github.com/JoeyTeng/codex-background-task-handler/pull/65
supersedes:
  - 20260511-00afcf6-desktop-transcript-relay-consumer
superseded_by:
---

# Desktop Transcript Relay Consumer Live Validation

## Summary

- Desktop transcript relay transport is live-validated, and the relay consumer foundation can drive `note-arm-pending` / `note-arm` CAS transitions from trusted `function_call_output` envelopes in fake tests.
- This work validates the real end-to-end side channel: a Desktop heartbeat emits arm-pending and arm envelopes, then a normal shell consumes those envelopes from the rollout and advances durable cbth state.
- Desktop automatic delivery remains disabled. This work does not add production tailing, ready materialization, `automation_update`, caller wake, `note-boundary-crossed`, or artifact reads.

## Validation Plan

- Create a synthetic Desktop writeback fixture from a normal shell with `cbth desktop validation prepare-writeback-fixture`, using a validation-only source thread id and automation id.
- Run the Desktop heartbeat thread `019db5e6-ba6a-7b80-95d2-a6867163281a` to emit one `arm_pending_requested` transcript envelope and one `arm_requested` transcript envelope with unique high-entropy markers.
- From a normal shell, run `cbth desktop relay consume-transcript --rollout-path <rollout-jsonl> --marker <marker> --json` for each marker.
- Success requires the consumer to accept exactly one trusted `function_call_output` envelope for each marker, move the attempt to `arm_pending` then `cooldown`, and keep `delivery_attempt_count=1` across replay checks.
- Only after complete success should an operator repair installation state with `writeback_capability=validated`; `artifact_read_capability` remains `unknown`.

## Evidence To Record

- Fixture ids: `source_thread_id`, `caller_automation_id`, `bridge_request_id`, `batch_id`, `attempt_id`, and generation.
- Transcript evidence: pending marker, arm marker, rollout path, trusted carrier line or scanner summary, and consumer JSON outputs.
- Durable state: `attempt inspect`, `batch inspect`, repeated consume replay output, and pause-due readback where applicable.
- Capability repair output if and only if the full heartbeat plus consumer path succeeds.

## Current State

- Live validation succeeded from `master` commit `0796bf3ec04c`.
- A real Desktop heartbeat emitted both arm-pending and arm transcript envelopes into trusted `function_call_output` carriers.
- A normal shell consumed those envelopes with `cbth desktop relay consume-transcript`, advancing the fixture attempt from `prepared` to `arm_pending` to `cooldown`.
- `writeback_capability` has been explicitly repaired to `validated`; `artifact_read_capability` remains `unknown`.
- The synthetic fixture batch was closed after evidence collection.

## Live Evidence

- Local binary: `/Users/hoteng/.cache/cargo-target/release/cbth`
- Local binary version: `cbth 0.1.5`
- Heartbeat thread id: `019db5e6-ba6a-7b80-95d2-a6867163281a`
- Rollout file: `/Users/hoteng/.codex/sessions/2026/04/22/rollout-2026-04-22T16-54-50-019db5e6-ba6a-7b80-95d2-a6867163281a.jsonl`
- Source thread id: `cbth-desktop-transcript-relay-consumer-live-20260511T211238Z`
- Caller automation id: `cbth-desktop-transcript-relay-consumer-live`
- Bridge request id: `CBTH_DESKTOP_RELAY_CONSUMER_LIVE_20260511T211238Z`
- Batch id: `019e18e2-baea-7a20-9975-8f1296acd76a`
- Attempt id: `019e18e2-baea-7a20-9975-8f2262980523`
- Generation: `1`
- Pending marker: `CBTH_DESKTOP_RELAY_PENDING_20260511T211238Z`
- Pending trusted carrier line: `457`
- Pending consumer outcome: `arm_pending`, replay state `fresh`; repeated consume replay state `replayed`
- Bridge arm lease id: `019e18e5-83a0-7a03-b125-c4213ab926f5`
- Arm marker: `CBTH_DESKTOP_RELAY_ARM_20260511T211238Z`
- Arm trusted carrier line: `476`
- Arm consumer outcome: `armed`, replay state `fresh`; repeated consume replay state `replayed`
- Final attempt state: `cooldown`
- Final batch `delivery_attempt_count`: `1`
- Pause-due snapshot revision: `019e18eb-2c6c-7921-9209-80af1cafe2da`
- Pause-due readback: `count=1`, `overdue=true`
- Capability repair result: `writeback_capability=validated`, `artifact_read_capability=unknown`, `read_transport_generation=2`
- Capability repair note: `degraded_bindings=2` because the validation fingerprint moved to the current `0.1.5` binary path.
- Cleanup: fixture batch closed with `operator_confirmed_delivery`.

## Next Steps

- Add a production sidecar scanner with rollout discovery, durable scan cursors, marker issuance, and replay retention cleanup.
- Implement ready materialization and caller wake only after production relay consumption, pause reconcile, and continuation-boundary contracts are complete.
- Validate artifact-read capability separately before allowing `requires_artifact_read=true` batches on the automatic Desktop path.
