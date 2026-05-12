# Desktop Transcript Relay Validation

This document records the validation-only Desktop transcript relay path. It is a candidate writeback side channel for Desktop heartbeat contexts that can execute `cbth` but cannot open cbth SQLite, daemon sockets, `startup.lock`, or local writeback files.

## Contract

The heartbeat side only emits stdout. It must not mutate cbth durable state.
The original transport probe remains available:

```sh
cbth desktop validation emit-transcript-writeback-probe \
  --bridge-thread-id <bridge-thread-id> \
  --probe-id <probe-id> \
  --marker <unique-marker> \
  --json
```

The command prints exactly one prefixed envelope line:

```text
CBTH_TRANSCRIPT_WRITEBACK_V1 {"schema_version":1,...}
```

The operator / sidecar side scans a Codex rollout JSONL file:

```sh
cbth desktop validation scan-transcript-writeback \
  --rollout-path <rollout-jsonl> \
  --marker <unique-marker> \
  --json
```

The scanner classifies carriers:

- `trusted_auto`: exact prefixed envelope found in `response_item.payload.type=function_call_output`.
- `diagnostic_only`: marker or envelope found in assistant text such as `event_msg.agent_message` or `task_complete.last_agent_message`.
- `ignored_prompt`: marker or sample envelope found in user / heartbeat prompt text.

Only `trusted_auto` is a future automatic writeback input. `diagnostic_only` and `ignored_prompt` must never mutate cbth store automatically.

## Writeback Consumer Foundation

The relay now also supports writeback request envelopes for the existing Desktop arm CAS primitives. These emit helpers are still heartbeat-safe stdout-only commands:

```sh
cbth desktop validation emit-transcript-arm-pending \
  --source-thread-id <source-thread-id> \
  --attempt-id <attempt-id> \
  --generation <generation> \
  --bridge-request-id <request-id> \
  --marker <unique-marker> \
  --json

cbth desktop validation emit-transcript-arm \
  --source-thread-id <source-thread-id> \
  --attempt-id <attempt-id> \
  --generation <generation> \
  --bridge-request-id <request-id> \
  --bridge-arm-lease-id <lease-id> \
  --marker <unique-marker> \
  --json
```

The non-Desktop consumer scans the rollout and, only for exactly one trusted `function_call_output` envelope, applies the matching durable CAS transition:

```sh
cbth desktop relay consume-transcript \
  --rollout-path <rollout-jsonl> \
  --marker <unique-marker> \
  --json
```

Envelope kinds:

- `arm_pending_requested`: calls the same store CAS as `cbth desktop note-arm-pending`.
- `arm_requested`: calls the same store CAS as `cbth desktop note-arm` and must include `bridge_arm_lease_id`.

Replay protection is durable and marker-scoped. The first accepted marker stores the canonical envelope hash and CAS outcome; replaying the same marker/hash returns the stored outcome without another CAS call, while the same marker with a different hash fails closed. Duplicate trusted envelopes, malformed trusted envelopes, prompt copies, final assistant text, unsupported envelope kinds, and wrong markers do not mutate state.

## Live Validation Flow

1. Build or install the current `cbth` binary so the Desktop heartbeat can execute it.
2. Pick a unique marker, for example `CBTH_TRANSCRIPT_RELAY_LIVE_<timestamp>`.
3. In the Desktop heartbeat thread, run the emit command once and ask the agent not to run cleanup or capability repair.
4. From an operator shell, scan the known rollout path for that marker.
5. Success requires `auto_decision.trusted=true`, `reason=single_trusted_auto_envelope`, and `counts.trusted_auto=1`.
6. Record marker, rollout path, carrier, scanner JSON, and thread id in `DESKTOP_LIVE_PREFLIGHT_EVIDENCE.md`.

An interactive Desktop tool-output probe is useful as a lower-level transport sanity check when heartbeat scheduling cannot be triggered immediately. It proves that a real Desktop thread stores helper stdout in a `response_item.payload.type=function_call_output` carrier and that the scanner accepts that carrier.

The 2026-05-11 heartbeat automation validation used the same helper and proved that automation-delivered helper stdout also appears as a `response_item.payload.type=function_call_output` carrier. The scanner accepted exactly one `trusted_auto` envelope, treated prompt copies as `ignored_prompt`, and treated assistant / task-complete text as `diagnostic_only`.

The initial transport validation did not set `writeback_capability=validated`. A later live validation completed the missing consumer step: a real Desktop heartbeat emitted arm-pending and arm envelopes, a non-Desktop consumer applied them through durable CAS, and an operator recorded the result with `installation-state repair`. The local installation now has `writeback_capability=validated`; `artifact_read_capability` remains `unknown`.

The remaining production work is durable rollout discovery / scan cursors, marker issuance, background tailing, ready materialization, caller wake, pause reconcile, and continuation-boundary handling.

## Failure Interpretation

- If only `diagnostic_only` appears, the Desktop agent produced final text but not a trustworthy tool-output carrier.
- If only `ignored_prompt` appears, the scanner correctly avoided self-triggering from the heartbeat prompt.
- If duplicate `trusted_auto` envelopes appear for the same marker, the scanner must fail closed and report `duplicate_trusted_auto_envelopes`.
- If malformed trusted envelopes appear, the scanner must fail closed and report rejected trusted carrier evidence.
