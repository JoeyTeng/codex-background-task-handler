---
id: 20260513-0c6d795-desktop-relay-scanner-live-validation
title: Desktop Relay Scanner Live Validation
status: completed
created: 2026-05-13
updated: 2026-05-13
branch: codex/desktop-relay-scanner-live-validation
pr:
supersedes:
  - 20260512-f4b2e32-desktop-transcript-relay-production-scanner
superseded_by:
---

# Desktop Relay Scanner Live Validation

## Summary

- Production scanner foundation is implemented and merged, and this work live-validates it against a real Codex Desktop heartbeat rollout.
- The validation covers the live path only: issued markers, heartbeat-emitted `function_call_output` envelopes, daemon-owned scanner consumption, and durable CAS transitions.
- Desktop automatic delivery remains disabled. Ready materialization, caller wake, `automation_update` production workflow, `note-boundary-crossed`, and artifact reads remain out of scope.

## Validation Target

- Use the existing Desktop bridge heartbeat thread `019db5e6-ba6a-7b80-95d2-a6867163281a`.
- Bind the current heartbeat rollout path with `cbth desktop relay scanner bind`.
- Create a validation-only Desktop writeback fixture from an operator shell.
- Issue an `arm-pending` marker, trigger heartbeat to run `cbth desktop relay emit-arm-pending`, and let the daemon scanner advance the fixture from `prepared` to `arm_pending`.
- Issue an `arm-accepted` marker, trigger heartbeat to run `cbth desktop relay emit-arm-accepted`, and let the daemon scanner advance the fixture from `arm_pending` to `cooldown`.
- Verify `delivery_attempt_count=1`, marker replay is idempotent, scanner cursor/status are sane, and the prior Codex review finding remains fixed by `binding_revision`.

## Safety Boundaries

- Use synthetic validation source thread and automation ids; do not reuse a production caller workflow.
- The heartbeat prompt must only run emit helpers. It must not run cleanup, capability repair, direct store writes, daemon commands, or caller wake.
- The daemon scanner is allowed to mutate only through issued marker CAS paths already covered by fake tests.
- If the rollout path changes, truncates, or identity drifts, bind must be refreshed explicitly; the scanner must not auto-discover another rollout.

## Planned Evidence

- `cbth --version` and binary path used by Desktop heartbeat.
- Heartbeat thread id and rollout path.
- Fixture output: `source_thread_id`, `caller_automation_id`, `batch_id`, `attempt_id`, `generation`, and `bridge_request_id`.
- Pending and accepted marker ids plus scanner status after each phase.
- Attempt and batch inspections proving `arm_pending`, then `cooldown`, with `delivery_attempt_count=1`.
- Any blocker output if Desktop heartbeat cannot execute emit helpers or the scanner cannot consume the trusted carrier.

## Validation Commands

```sh
cbth desktop validation prepare-writeback-fixture \
  --source-thread-id <synthetic-source-thread-id> \
  --caller-automation-id <synthetic-automation-id> \
  --bridge-request-id <request-id> \
  --json

cbth desktop relay scanner bind \
  --bridge-thread-id 019db5e6-ba6a-7b80-95d2-a6867163281a \
  --rollout-path <heartbeat-rollout-path> \
  --json

cbth desktop relay marker issue \
  --bridge-thread-id 019db5e6-ba6a-7b80-95d2-a6867163281a \
  --kind arm-pending \
  --source-thread-id <source-thread-id> \
  --attempt-id <attempt-id> \
  --generation <generation> \
  --bridge-request-id <bridge-request-id> \
  --json

cbth desktop relay marker issue \
  --bridge-thread-id 019db5e6-ba6a-7b80-95d2-a6867163281a \
  --kind arm-accepted \
  --source-thread-id <source-thread-id> \
  --attempt-id <attempt-id> \
  --generation <generation> \
  --bridge-request-id <bridge-request-id> \
  --json
```

## Next Steps

- Implement ready attempt materialization and the bridge heartbeat arm workflow on top of the validated scanner path.
- Implement caller wake / `automation_update` production workflow only after materialization and pause reconcile are durable.
- Implement `note-boundary-crossed` and continuation-boundary recovery before enabling Desktop automatic delivery end to end.
- Validate artifact-read capability separately before allowing `requires_artifact_read=true` batches on the automatic Desktop path.

## Current State

- Live validation succeeded from branch `codex/desktop-relay-scanner-live-validation`.
- A first attempt against the historical bridge heartbeat thread `019db5e6-ba6a-7b80-95d2-a6867163281a` proved one pending scanner consumption, but that old thread had grown to the model context limit and became unsuitable for repeated live validation.
- The completed validation used the cleaner Desktop test thread `019db49a-de4e-7d61-93ab-5d70a8905cc3`.
- A real Desktop heartbeat automation emitted both pending and accepted envelopes into trusted `response_item.payload.type=function_call_output` carriers.
- The daemon-owned production scanner consumed the issued markers from the bound rollout cursor and advanced the validation attempt from `prepared` to `arm_pending` to `cooldown`.
- Repeated heartbeat emissions after marker consumption did not increment the batch again; final `delivery_attempt_count` stayed `1`.
- Installation state already had `read_transport_capability=validated` and `writeback_capability=validated`, so no additional capability repair was needed. `artifact_read_capability` remains `unknown`.

## Live Evidence

- Desktop-visible binary: `/Users/hoteng/.local/bin/cbth`
- Desktop-visible binary version: `cbth 0.2.0`
- Operator binary used for store inspection: `/Users/hoteng/.cache/cargo-target/debug/cbth`
- Clean heartbeat thread id: `019db49a-de4e-7d61-93ab-5d70a8905cc3`
- Clean heartbeat rollout file: `/Users/hoteng/.codex/sessions/2026/04/22/rollout-2026-04-22T10-52-21-019db49a-de4e-7d61-93ab-5d70a8905cc3.jsonl`
- Scanner binding: active, Unix identity `unix:16777233:697017980`, initial cursor `byte=255043`, `line=78`
- Source thread id: `cbth-desktop-relay-scanner-live-20260513-c`
- Caller automation id: `cbth-desktop-relay-scanner-live-c`
- Bridge request id: `CBTH_DESKTOP_RELAY_SCANNER_LIVE_20260513_C`
- Batch id: `019e2056-850c-79a1-8d65-296e9a5dcd07`
- Attempt id: `019e2056-850c-79a1-8d65-297730f80e58`
- Generation: `1`
- Pending marker: `CBTH_DESKTOP_RELAY_ARM_PENDING_019e2056-cf32-7183-a341-74f34e82923d`
- Pending trusted carrier line: `91`
- Pending scanner result: `active_marker_counts=0`, `last_consumed_at=1778659175`, cursor advanced to line `91`
- Pending durable result: attempt state `arm_pending`, lease id `019e2059-2bd7-7bb3-81df-728c7e97824e`
- Arm marker: `CBTH_DESKTOP_RELAY_ARM_ACCEPTED_019e205a-aae5-74b1-9459-aba5a49c687f`
- Arm trusted carrier line: `122`
- Arm scanner result: `active_marker_counts=0`, `last_consumed_at=1778659451`, cursor advanced to line `122`
- Final durable result: attempt state `cooldown`, `desktop_armed_at=1778659451`
- Final batch result: `delivery_attempt_count=1`
- Replay / duplicate evidence: the heartbeat emitted duplicate pending and accepted envelopes later in the same thread, but the marker was already consumed; a follow-up `scan-once` had no active markers and `delivery_attempt_count` remained `1`.
- Cleanup: the successful fixture batch was closed with `operator_confirmed_delivery`; abandoned validation fixtures `20260513-a` and `20260513-b` were closed with `operator_closed_unconfirmed`.

## Validation Notes

- The historical bridge heartbeat thread remains useful as evidence that a long-running bridge thread can emit a trusted carrier, but its large context makes it a poor ongoing validation target.
- `arm-accepted` markers can only be issued after durable `arm_pending`; production flows need either prompt scheduling that fits the arm lease or a bridge workflow that emits accepted promptly after pending is observed.
- Old issued markers from abandoned validation paths are left to TTL / retention cleanup because the marker surface intentionally does not expose a destructive delete command.
