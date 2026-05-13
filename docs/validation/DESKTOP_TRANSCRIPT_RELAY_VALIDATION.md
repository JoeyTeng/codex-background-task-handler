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

## Production Scanner Foundation

Manual `consume-transcript` remains available for operator debugging, but production relay writeback now has a daemon-owned scanner foundation.

The operator binds a bridge thread to the exact rollout JSONL path to scan:

```sh
cbth desktop relay scanner bind \
  --bridge-thread-id <bridge-thread-id> \
  --rollout-path <rollout-jsonl> \
  [--from-start] \
  --json
```

The binding stores the resolved path, a Unix device/inode identity, a durable byte cursor, a durable line cursor, and a monotonic binding revision used as the scanner CAS token. The scanner never silently discovers or switches rollout files. If the Desktop heartbeat thread forks, archives, side-chats, or starts writing to a different rollout, the operator must bind the new path explicitly. Truncation or identity drift marks the binding `degraded` and stops automatic consumption.

Before asking heartbeat to emit a writeback envelope, the operator / future bridge flow issues a high-entropy marker:

```sh
cbth desktop relay marker issue \
  --bridge-thread-id <bridge-thread-id> \
  --kind arm-pending|arm-accepted \
  --source-thread-id <source-thread-id> \
  --attempt-id <attempt-id> \
  --generation <generation> \
  --bridge-request-id <request-id> \
  --json
```

Issued markers expire after 6 hours. Consumed, expired, and rejected marker records are retained for 7 days, together with the existing marker/hash replay fence.

Heartbeat-safe production emit aliases are stdout-only and do not open SQLite, connect a daemon, or write local files:

```sh
cbth desktop relay emit-arm-pending ... --marker <issued-marker> --json
cbth desktop relay emit-arm-accepted ... --marker <issued-marker> --json
```

`arm-accepted` deliberately omits `bridge_arm_lease_id`, and any trusted `arm_accepted` envelope that includes that field is rejected. The scanner resolves the durable lease from the current `arm_pending` attempt immediately before calling the existing `note-arm` CAS path. This avoids putting the lease into Desktop heartbeat prompt/output text while still requiring the durable attempt to be in the expected state.

The daemon scans only while issued markers with active scanner bindings, issued markers with existing consumption fences, expired issued markers, or due relay retention cleanup exist. Each scan tick first `symlink_metadata`s the bound rollout path and rejects non-regular files / identity drift before opening it; the actual open uses nonblocking flags, then the opened handle's metadata / identity freezes tick-start EOF before the active marker set is read. This prevents path replacement races, avoids hanging on FIFO replacement, and prevents a marker issued during the scan from having its envelope skipped by a cursor advance past earlier rollout bytes. The tick is bounded to that captured EOF, 256 complete newline-delimited rollout records, or 1 MiB; a first record that cannot fit in that budget degrades the binding instead of letting the worker parse unbounded output. Partial trailing lines and lines appended after the tick-start EOF are left for the next tick. If a bounded tick observes any trusted marker evidence before reaching the captured EOF, the scanner degrades the binding instead of consuming or rejecting that marker; this avoids accepting a first envelope when a duplicate or contradicting envelope may exist later in the same tick-start snapshot. Cursor publication is conditional on the binding still being active at the expected prior byte/line cursor and monotonic `binding_revision`, so overlapping scans cannot move the cursor backward or clear a degraded state. Degrade and marker-reject writes use the same expected binding guard; an older tick cannot degrade or reject after a rebind, cursor advance, or prior degradation. `updated_at` is observational only and is not used as a scanner CAS token because same-second rebind/reset is possible. Only a single exact prefixed envelope in a `function_call_output` carrier for an issued, unexpired marker can mutate state. The scanner atomically re-checks marker state, expiry, expected fields, marker/hash fence, and the scanner binding path / identity / cursor / `binding_revision` in the same immediate SQLite transaction that applies `note-arm-pending` or `note-arm`; if another worker has already rejected or expired the marker, or if another scan has rebound/degraded/advanced the binding, CAS is not attempted. `arm-accepted` marker issuance is allowed only after the durable attempt is already `arm_pending`, so production does not create dependent accepted markers before pending is committed. Successful replay fences are checked before `arm_accepted` performs a pending-only lease lookup, and scan maintenance reconciles any existing consumption fence before expiring issued markers. That keeps crash recovery idempotent if the process dies after CAS but before the marker record is marked consumed. Unissued markers, prompt/user text, assistant final text, malformed trusted envelopes, duplicate trusted envelopes, wrong fields, expired markers without replay fences, failed-CAS replay fences, and marker/hash conflicts fail closed without advancing Desktop delivery state.

## Live Validation Flow

1. Build or install the current `cbth` binary so the Desktop heartbeat can execute it.
2. Pick a unique marker, for example `CBTH_TRANSCRIPT_RELAY_LIVE_<timestamp>`.
3. In the Desktop heartbeat thread, run the emit command once and ask the agent not to run cleanup or capability repair.
4. From an operator shell, scan the known rollout path for that marker.
5. Success requires `auto_decision.trusted=true`, `reason=single_trusted_auto_envelope`, and `counts.trusted_auto=1`.
6. Record marker, rollout path, carrier, scanner JSON, and thread id in the focused Desktop relay scanner live-validation journal.

An interactive Desktop tool-output probe is useful as a lower-level transport sanity check when heartbeat scheduling cannot be triggered immediately. It proves that a real Desktop thread stores helper stdout in a `response_item.payload.type=function_call_output` carrier and that the scanner accepts that carrier.

The 2026-05-11 heartbeat automation validation used the same helper and proved that automation-delivered helper stdout also appears as a `response_item.payload.type=function_call_output` carrier. The scanner accepted exactly one `trusted_auto` envelope, treated prompt copies as `ignored_prompt`, and treated assistant / task-complete text as `diagnostic_only`.

The initial transport validation did not set `writeback_capability=validated`. A later live validation completed the missing consumer step: a real Desktop heartbeat emitted arm-pending and arm envelopes, a non-Desktop consumer applied them through durable CAS, and an operator recorded the result with `installation-state repair`. The local installation now has `writeback_capability=validated`; `artifact_read_capability` remains `unknown`.

The 2026-05-13 production scanner live validation completed the next step: a real Desktop heartbeat emitted production `arm_pending_requested` and `arm_accepted` envelopes into trusted `function_call_output` carriers, and the daemon-owned scanner consumed issued markers from a bound rollout cursor to advance durable state from `prepared` to `arm_pending` to `cooldown`. The remaining production work is ready materialization, bridge heartbeat arm workflow, caller wake, pause reconcile, continuation-boundary handling, and artifact-read policy.

## Failure Interpretation

- If only `diagnostic_only` appears, the Desktop agent produced final text but not a trustworthy tool-output carrier.
- If only `ignored_prompt` appears, the scanner correctly avoided self-triggering from the heartbeat prompt.
- If duplicate `trusted_auto` envelopes appear for the same marker, the scanner must fail closed and report `duplicate_trusted_auto_envelopes`.
- If malformed trusted envelopes appear, the scanner must fail closed and report rejected trusted carrier evidence.
