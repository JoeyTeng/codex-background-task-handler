---
id: 20260513-0c6d795-desktop-relay-scanner-live-validation
title: Desktop Relay Scanner Live Validation
status: active
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

- Production scanner foundation is implemented and merged, but the daemon-owned scanner has not yet been validated against a real Codex Desktop heartbeat rollout.
- This work validates the live path only: issued markers, heartbeat-emitted `function_call_output` envelopes, daemon-owned scanner consumption, and durable CAS transitions.
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

- Commit this plan first.
- Locate the current rollout file for the heartbeat thread from local Codex session metadata.
- Build/install a current `cbth` binary visible to the Desktop heartbeat.
- Run the pending and accepted marker validation flow.
- Record live evidence or a precise blocker in this journal before opening the PR.
