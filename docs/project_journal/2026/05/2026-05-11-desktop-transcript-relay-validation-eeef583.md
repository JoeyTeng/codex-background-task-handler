---
id: 20260511-eeef583-desktop-transcript-relay-validation
title: Desktop Transcript Relay Validation
status: active
created: 2026-05-11
updated: 2026-05-11
branch: codex/desktop-transcript-relay-validation
pr:
supersedes:
  - 20260511-2b4ea02-desktop-writeback-dropbox-probe
superseded_by:
---

# Desktop Transcript Relay Validation

## Summary

- Real Desktop heartbeat can execute `cbth` and consume no-DB inbox reads, but it cannot mutate cbth store, daemon IPC, startup-lock, or local writeback files.
- The next side-channel candidate is transcript / tool-output relay: heartbeat emits a prefixed stdout envelope and an external operator / sidecar reads that exact stdout from Codex rollout records.
- This work validates the transport only. It does not enable Desktop automatic delivery, does not execute durable `note-arm-pending` / `note-arm`, and does not set `writeback_capability=validated`.

## Planned Changes

- Add hidden validation-only emitter:
  `cbth desktop validation emit-transcript-writeback-probe --bridge-thread-id <id> --probe-id <id> --marker <marker> --json`.
- The emitter prints one `CBTH_TRANSCRIPT_WRITEBACK_V1` prefixed JSON envelope to stdout and does not open SQLite, connect daemon IPC, touch `startup.lock`, or write `~/.cbth`.
- Add hidden validation-only scanner:
  `cbth desktop validation scan-transcript-writeback --rollout-path <path> --marker <marker> --json`.
- The scanner classifies rollout carriers as `trusted_auto`, `diagnostic_only`, or `ignored_prompt`; only exact `function_call_output` envelopes can be `trusted_auto`.
- Add fake tests for trusted carrier extraction, prompt self-trigger rejection, diagnostic-only final text, duplicate trusted envelopes, malformed trusted envelopes, and wrong-marker behavior.

## Validation Plan

- Local gate: `cargo fmt --all -- --check`, `cargo clippy --locked --all-targets -- -D warnings`, `cargo test --test desktop_foundation --locked`, `cargo test --locked`, `cargo test`, project journal validate, and `git diff --check`.
- Live probe: run the emitter from the real Desktop heartbeat thread `019db5e6-ba6a-7b80-95d2-a6867163281a`, then scan the known rollout JSONL for the marker.
- Success requires exactly one `trusted_auto` envelope from `function_call_output`; assistant final text remains diagnostic-only.

## Evidence

- Base branch: `master`
- Base commit: `eeef583099d22af196e9525aa80de2d4a4cd5397`
- Prior filesystem writeback blocker: [Desktop writeback dropbox probe](2026-05-11-desktop-writeback-dropbox-probe-2b4ea02.md)
- Validation instructions: [Desktop transcript relay validation](../../../DESKTOP_TRANSCRIPT_RELAY_VALIDATION.md)
- Interactive Desktop tool-output validation:
  - marker: `CBTH_TRANSCRIPT_RELAY_INTERACTIVE_20260511T132728Z`
  - rollout: `/Users/hoteng/.codex/sessions/2026/04/22/rollout-2026-04-22T10-14-58-019db478-a40c-7a62-b8d0-70ef2c3249d1.jsonl`
  - carrier: `response_item.payload.type=function_call_output`
  - decision: `single_trusted_auto_envelope`
  - record line: `90911`

## Current State

- Hidden emitter and scanner are implemented for this PR.
- Fake tests cover trusted carrier extraction, prompt self-trigger rejection, diagnostic-only final text, duplicate trusted envelopes, malformed trusted envelopes, wrong-marker behavior, and malformed diagnostic text.
- A real interactive Desktop tool-output probe succeeded and proved the scanner accepts exact prefixed stdout from `function_call_output`.
- Heartbeat-specific transcript carrier validation remains a follow-up because the temporary heartbeat automation did not provide an immediate run during this branch.
- The production sidecar consumer, durable scan cursor, replay protection, and CAS mutation path remain future work.

## Next Steps

- Run a heartbeat-specific transcript relay probe and record whether automation-delivered helper stdout uses the same `function_call_output` carrier or another structured carrier.
- Design a production sidecar consumer only after heartbeat carrier shape is proven; it must include durable scan cursors, replay protection, high-entropy nonce / lease / generation validation, and CAS mutation.
