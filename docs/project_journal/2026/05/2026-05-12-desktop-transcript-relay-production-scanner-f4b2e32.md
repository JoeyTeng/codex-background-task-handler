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

- Scanner runtime: daemon-owned worker, not a separate long-running process. It should only keep the daemon alive while issued markers can be consumed, consumed markers need reconciliation, expired markers need finalization, or relay retention cleanup is due.
- Rollout source: explicit operator binding from `bridge_thread_id` to a resolved rollout path. Forks, archives, side chats, or new heartbeat threads require an explicit rebind; the scanner must not silently switch paths.
- Marker model: scanner only consumes issued, unexpired markers whose envelope fields match the issuance record. Unissued marker text in rollout is diagnostic evidence only.
- Scan ordering: each tick first validates the bound rollout path with `symlink_metadata`, rejecting non-regular files and identity drift before open. It then uses a nonblocking open and freezes metadata / EOF from the opened handle before reading the issued marker set, so path replacement races fail closed, FIFO replacement cannot hang lifecycle maintenance, and markers issued while a scan is running cannot have their rollout bytes skipped by cursor advancement.
- Cursor safety: cursor publication, binding degradation, and marker rejection use the expected prior path / identity / byte cursor / line cursor / monotonic `binding_revision` and active binding state as a CAS guard. `updated_at` is observational only because same-second rebind/reset can otherwise collide. Older overlapping scanner ticks cannot move the cursor backward, clear a degraded binding, degrade a replacement binding, or reject a marker after rebind.
- Bounded evidence safety: if a bounded tick observes marker evidence before reaching tick-start EOF, it must degrade the scanner binding instead of consuming or rejecting the marker. This keeps duplicate/conflict detection sound without allowing unbounded rollout reads.
- Mutation atomicity: fresh scanner consumption must re-check marker state, expiry, tokens, hash, and scanner binding path / identity / cursor in the same immediate SQLite transaction that applies the arm CAS and marks the marker consumed or rejected. If another worker already changed the marker state or scanner binding, no CAS is attempted.
- Arm lease: production `arm-accepted` envelopes do not contain `bridge_arm_lease_id`, and the scanner rejects trusted `arm_accepted` envelopes that include it. Marker issuance requires durable `arm_pending` with matching request/generation/lease state, and the scanner resolves the durable lease from that attempt before calling `note-arm`.
- Existing manual `consume-transcript` remains as an operator/debug surface, but production scanner applies stricter allowlist and cursor rules.

## Implementation Plan

- Add durable scanner binding and marker tables.
- Add JSON CLI surfaces for scanner bind/status/scan-once and marker issue.
- Add heartbeat-safe relay emit aliases for `arm-pending` and `arm-accepted`.
- Integrate a bounded daemon scan worker with 2 second cadence, strict 256 line / 1 MiB per-tick limits, fail-closed degraded binding behavior, and sweep cleanup for expired marker/replay records.
- Update Desktop relay and bridge docs with the production scanner contract and remaining non-goals.

## Implementation Result

- Added durable `desktop_relay_scanner_bindings` with explicit `bridge_thread_id -> rollout_path` binding, Unix device/inode identity, byte cursor, line cursor, state, and last-error metadata.
- Added durable `desktop_transcript_relay_markers` allowlist with issued / consumed / expired / rejected states, 6 hour default TTL, 7 day retention, expected envelope kind, and expected source / attempt / generation / request tokens.
- Added JSON operator surfaces:
  - `cbth desktop relay scanner bind --bridge-thread-id <id> --rollout-path <path> [--from-start] --json`
  - `cbth desktop relay scanner status [--bridge-thread-id <id>] --json`
  - `cbth desktop relay scanner scan-once [--bridge-thread-id <id>] --json`
  - `cbth desktop relay marker issue --bridge-thread-id <id> --kind arm-pending|arm-accepted ... --json`
- Added heartbeat-safe production emit aliases:
  - `cbth desktop relay emit-arm-pending ... --marker <issued-marker> --json`
  - `cbth desktop relay emit-arm-accepted ... --marker <issued-marker> --json`
- `arm-accepted` envelopes intentionally omit `bridge_arm_lease_id`; marker issuance requires the attempt to already be durable `arm_pending` with matching generation / request / lease state, and the scanner resolves that durable lease before applying the existing `note-arm` CAS path. Trusted `arm_accepted` envelopes that include `bridge_arm_lease_id` are rejected as wrong-field envelopes.
- Added daemon capability `desktop-transcript-relay-scanner` and relay maintenance lifecycle accounting. Issued markers with active scanner bindings, issued markers with existing consumption fences, expired issued markers, and due marker/replay retention cleanup keep the daemon alive; no relay maintenance means the scanner worker does not block normal idle exit.
- The daemon scanner worker runs at a 2 second cadence only while relay marker/retention maintenance is due. Each tick validates the bound rollout path as the expected regular file before open, uses nonblocking open, and is bounded to the opened handle's EOF captured at tick start, 256 complete JSONL records, or 1 MiB; partial trailing lines and lines appended after tick-start EOF stay for the next tick. The scanner degrades bindings on special-file replacement, rollout truncate, oversized first-tick records, or device/inode drift.
- Cursor publication, binding degradation, and marker rejection now check the expected prior path / identity / byte cursor / line cursor / `binding_revision` and active binding state; stale scan ticks fail closed instead of moving the durable cursor backward, clearing `degraded`, degrading a replacement binding, or rejecting a marker after rebind.
- If a tick sees marker evidence before it reaches tick-start EOF, the binding is degraded before any marker or attempt mutation. This fail-closed rule prevents a bounded first tick from accepting an envelope while a duplicate or conflicting envelope remains beyond the 256-line / 1 MiB window in the same EOF snapshot.
- Fresh scanner consumption now atomically validates the issued marker, verifies the scanner binding has not been rebound/degraded/advanced by checking `binding_revision`, and applies the CAS in one immediate SQLite transaction; a marker rejected or expired by another worker, or a scanner binding changed by another tick, fails closed without touching the attempt or writing a new consumption fence.
- Failed durable CAS replay fences are fail-closed: repeated consumption of a stored `cas_failed` outcome returns an error and allows scanner-owned markers to be rejected instead of being mislabeled as consumed.
- `arm-accepted` markers cannot be issued before the attempt reaches durable `arm_pending`; this removes the need to rely on same-tick pending/accepted ordering for correctness. Successful same marker/hash replay is still checked before `arm_accepted` performs the pending-only lease lookup, and scan maintenance reconciles existing consumption fences before expiring issued markers, so crash recovery after CAS commit can mark the marker consumed instead of rejecting it.
- Trusted `function_call_output` carriers that mention an issued marker and relay prefix but contain no matching envelope are rejected immediately, rather than advancing the cursor and leaving the marker issued until expiry.

## Validation Plan

- Fake tests cover rollout binding cursor initialization, stale cursor tick rejection, same-second identical rebind rejection, rebound-same-cursor rejection, stale degrade/reject guard, issued marker consumption, scanner-resolved lease arm acceptance, rejected premature `arm-accepted` marker issuance, rejected `arm_accepted` envelopes carrying embedded leases, replay idempotency, rejected/unissued markers, atomic non-issued marker rejection before CAS, scanner binding guard before CAS, trusted carrier marker-mention rejection, marker evidence before tick-start EOF degradation, partial trailing lines, tick-start EOF read limiting, oversized tick records, FIFO/special-file replacement degradation before blocking open, truncate/drift degradation, daemon idle interaction, failed-CAS replay handling, consumption-fence reconciliation, and retention cleanup.
- Local gate: Rust fmt, clippy, `cargo test --test desktop_foundation --locked`, locked/full test runs, project journal validation, `git diff --check`, and helper-backed Codex review.
- Opt-in live validation should bind the existing heartbeat rollout path, issue a pending marker, emit from heartbeat, let the daemon scanner advance the fixture to `arm_pending`, then issue an arm-accepted marker and verify the scanner advances the fixture to `cooldown`.

## Current Validation Evidence

- `cargo check` passed during implementation.
- `cargo fmt --all -- --check` passed after formatting.
- `cargo clippy --locked --all-targets -- -D warnings` passed.
- `cargo test --test desktop_foundation --locked` passed with scanner coverage for issued marker consumption, scanner-resolved arm acceptance, rejected premature `arm-accepted` marker issuance, expired arm-accepted abandonment, partial trailing line deferral, tick-start EOF read limiting, marker evidence before tick-start EOF degradation, trusted carrier marker-mention rejection, oversized tick record degradation, conflicting duplicate trusted envelope rejection, same-hash duplicate trusted evidence deduplication, and no duplicate `delivery_attempt_count`.
- Store unit coverage includes atomic non-issued marker rejection before CAS and scanner binding guard before CAS, proving a rejected marker or stale scanner tick cannot still advance the prepared attempt or write a consumption fence.
- `cargo test --test daemon_phase2 --locked` passed with the new daemon capability included in compatibility expectations.
- `cargo test --locked` passed when rerun as a standalone full gate.
- `cargo test` passed; ignored live tests remained opt-in.
- Project journal validation and `git diff --check` passed.

## Remaining Next Steps

- Run helper-backed Codex review on the final diff before PR.
- Add opt-in live validation after the PR lands or as a follow-up: bind the real heartbeat rollout path, issue markers, run heartbeat emit helpers, and verify the daemon scanner advances a fixture to `cooldown`.
- Continue Desktop automatic delivery work after scanner live validation: ready materialization, bridge heartbeat arm workflow, caller wake / `automation_update`, pause reconcile, `note-boundary-crossed`, and artifact-read policy.
