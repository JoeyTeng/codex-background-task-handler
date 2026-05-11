---
id: 20260511-0796bf3-desktop-transcript-relay-consumer-live-validation
title: Desktop Transcript Relay Consumer Live Validation
status: active
created: 2026-05-11
updated: 2026-05-11
branch: codex/desktop-transcript-relay-consumer-live-validation
pr:
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

- Planned from `master` commit `0796bf3ec04c`.
- `writeback_capability` remains `unknown` until this live validation succeeds.

## Next Steps

- Run the live validation flow and update this journal with the observed result.
- If successful, record evidence and repair `writeback_capability=validated`.
- If blocked, leave capability state unchanged and record the exact blocker before designing production tailing.
