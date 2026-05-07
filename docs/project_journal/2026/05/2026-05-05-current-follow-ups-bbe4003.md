---
id: 20260505-bbe4003-active
title: Current Follow-ups
status: active
created: 2026-05-05
updated: 2026-05-07
branch: master
pr: https://github.com/JoeyTeng/codex-background-task-handler/pull/37
supersedes: []
superseded_by:
---

# Current Follow-ups

## Summary

- The active backlog is now grouped here instead of living in the top-level trackers.
- Completed detail and older evidence remain available in [completed work archive](2026-05-05-completed-work-archive-bbe4003.md) and [legacy tracker snapshot](2026-05-05-legacy-tracker-snapshot-bbe4003.md). The legacy snapshot is an exact historical copy; use this entry for the active backlog and live links.

## External Integrations

- Design and implement external code review delegation, then notify results back to the original caller thread.
- Design and implement an app-server output bridge that can forward output to Webex / GitHub Issue and route channel replies back to app-server.
- Design and implement a PR / GitHub Actions status poller that reminds the right thread about remote review comments or merge blockers.

## Desktop Evidence And Validation

- If still needed, run a scheduled Desktop heartbeat test that distinguishes app-open behavior from fully-quit app behavior.
- Build a real heartbeat automation sample and confirm target-thread fields in `automation.toml` / `codex-dev.db`.
- Validate whether external process edits to Desktop automation schedule state, especially `next_run_at` and status transitions, are hot-observed by the caller thread heartbeat.
- Desktop heartbeat preflight attempts are recorded in [Desktop live preflight evidence](../../../DESKTOP_LIVE_PREFLIGHT_EVIDENCE.md): direct heartbeat access to redundant chmod, `startup.lock`, Unix socket, and SQLite WAL paths failed under Desktop sandboxing even when POSIX ownership / mode looked correct.
- The Desktop boundary is now split: a normal shell / daemon / future sidecar publishes the inbox snapshot, while the heartbeat consumes it through no-DB read helpers. Real heartbeat validation succeeded for `read-snapshot`, `list-arm-pending`, `list-pause-due`, and read/peek `claim-next-ready` without approval.
- `read_transport_capability=validated` now covers the no-DB direct-file-read helper path against an already-published revision-consistent snapshot, including the manifest-referenced installation-state export. It does not validate heartbeat-owned `bridge-preflight`, SQLite access, artifact payload reads, or writeback helpers.
- `note-arm-pending` and `note-arm` now exist as local writeback primitives with fake coverage. A hidden validation-only fixture and live validation doc are being added so real Desktop heartbeat can validate `note-arm-pending` / `note-arm` without direct SQLite edits; do not write `writeback_capability=validated` until that heartbeat run succeeds.
- If large artifacts ever enter the automatic caller path, separately validate `cbth desktop read-artifact ...` in heartbeat / caller contexts and write the result back to `artifact_read_capability`.

## Desktop Bridge Implementation

- Extend daemon lifecycle so Desktop binding / attempt schema can keep daemon alive for `arm_pending_deadline` and `pause_deadline`.
- Decide whether future daemon-owned supervised child processes need to be separated from externally reported pending jobs for idle-exit decisions.
- Complete Desktop binding lifecycle behavior beyond the current writeback fields: `armed_generation_quiesced_at` updates, paused-state readback validation, unbind/rebind, and no mixed `read_transport` bindings.
- Finish installation-wide `desktop_installation_state` authority and keep capability writes constrained to `installation-state repair`.
- Finish the read-only inbox shape beyond the no-DB helper baseline, including optional diagnostic `by-thread/<thread_id>.json`, artifact manifest export, and operator-only payload export.
- Implement bridge fairness and budget limits: independent reconcile and fresh-arm lanes, bounded item count / wall time, and `max_new_arms_per_wake=1`.
- Implement real ready / arm / pause materialization behind the existing no-DB read helpers.
- Extend `bridge_arm_lease` beyond the current `note-arm-pending` acquire / carry-forward primitive with overdue reconcile and cleanup semantics.
- Validate writeback helpers `note-arm-pending` and `note-arm` in real Desktop heartbeat before marking installation `writeback_capability=validated`.
- Implement `note-boundary-crossed` with prompt-token validation, binding / installation-state checks, `cooldown` and `armed_generation` preconditions, `handoff_recorded` close, and durable `boundary_recovery_envelope`.
- Verify the continuation-boundary contract: only fresh `note-boundary-crossed` success allows inline handoff; post-boundary automatic replay and ordinary tool continuation remain out of v1.
- Keep the v1 automatic path limited to read-only, no approval, no network, no write access batches, plus validated Desktop read and writeback capability; `requires_artifact_read=true` remains manual/operator follow-up.
- Define post-continuation operator recovery so lost response recovery uses `cbth batch inspect --batch-id ...`, not `inspect-head`, and pre-boundary manual batches require operator close or expiry.
- Implement Desktop ghost-wake reconciliation before declaring ambiguity: prove `cooldown`, `handoff_recorded`, or paused generation where possible; otherwise manualize and degrade.
- Add durable `arm_pending` barrier and a dedicated reconcile input surface through `arm-pending-bindings.json` or `list-arm-pending`.
- Add overdue-binding cleanup inputs through `pause-due-bindings.json` or `list-pause-due`.
- Design caller heartbeat cleanup and implement one-shot cleanup: `pause_not_before`, `pause_deadline`, quiesced generation tracking, bridge-first cleanup, and degraded state on repeated pause failures.
- Implement `note-boundary-crossed` compare-and-swap / idempotency, including `already-crossed` or stale-no-op outcomes and operator-only `boundary_recovery_envelope` inspection.
- Define Desktop operator/manual artifact recovery leases, lease deadlines, revocation / rotation / GC behavior, and `batch inspect --batch-id ...` re-lease surface.
- If post-output acknowledgement becomes necessary later, design a separate `note-delivered` contract with post-output / post-side-effect observation.
- Finish Desktop operator cleanup with `cbth desktop binding unbind ...`.
- Finish Desktop rebind / binding repair invalidation: prove old automation quiesced or deleted before reuse, otherwise force a fresh attempt / generation.

## CLI And Daemon Follow-ups

- Follow up PR #43 `cbth resume` hardening in order: native resume cwd UX parity, exact `permissionProfile` snapshot parsing with legacy fallback, then soft Codex CLI validated-version warnings.
- Finish CLI attach / recovery `activity_state=unknown -> current-state sync -> active/idle`; no auto-idle before authoritative sync and fail closed on continuity loss.
- Finish daemon overdue sweep / next-start reconcile so every entrypoint first closes, reconciles, or GC's due work, while `delivery_observation_deadline` remains the live-observation exception.
- Fix caller heartbeat long-term lifecycle: pre-bound `caller_automation_id`, normal `pause` / `update` / `reuse`, no normal-path delete, and operator-only unbind / destroy.
- Add machine-checkable active-turn steer risk fields: `active_turn_kind`, approval / network / write requirements, and `active_turn_risk_class`.
- Complete durable delivery-attempt contract, including CLI request fields, `delivery_turn_id`, optional `automation_id`, generation, and stale-wake no-op rules.
- Implement Desktop v1 `close_reason` and redelivery-window behavior so `closed` means automatic redelivery stopped, not caller-consumed.
- Implement `max_attempts_exhausted` durable auto-close based on `delivery_attempt_count >= max_delivery_attempts`.
- Define `binding_state=degraded` recovery: no automatic arm, abandon current attempt, and operator validation before returning to `bound`.
- Add remaining batch schema fields: redelivery window, max attempts, attempt count, replay policy, boundary snapshot revision, recovery envelope refs / bytes, retention, and operator pin timestamps.
- Implement `boundary_recovery_envelope` retention / GC, including inline limits, managed artifact fallback, retention windows, and stable `batch inspect --batch-id ...` recovery.
- Implement the minimal Rust sidecar supervisor skeleton for long-task status and result handoff.
- Finish CLI managed session remaining work: keep daemon alive after foreground exit while active jobs remain, define reconnect / resume, and add loopback auth validation only if upstream supports it.
- Finish fixed-thread contract details: exactly one target thread per managed session, explicit bootstraps only, no late-bind stable surface, no automatic thread-switch retarget, continuous observation for accepted attempts, fail-closed lost-response proof, and `turn_steer` only after active-turn risk proof.

## Evidence

- Migration PR: https://github.com/JoeyTeng/codex-background-task-handler/pull/37
- Desktop bridge foundation: [DESKTOP_BRIDGE_FOUNDATION.md](../../../DESKTOP_BRIDGE_FOUNDATION.md)
- Desktop live validation: [DESKTOP_LIVE_PREFLIGHT_VALIDATION.md](../../../DESKTOP_LIVE_PREFLIGHT_VALIDATION.md)
- Desktop bridge design: [DESKTOP_BACKGROUND_TASK_BRIDGE_DESIGN.md](../../../DESKTOP_BACKGROUND_TASK_BRIDGE_DESIGN.md)
- Desktop live preflight evidence: [DESKTOP_LIVE_PREFLIGHT_EVIDENCE.md](../../../DESKTOP_LIVE_PREFLIGHT_EVIDENCE.md)
- Shared architecture: [SHARED_CORE_ARCHITECTURE.md](../../../SHARED_CORE_ARCHITECTURE.md)
- CLI active steer design: [CLI_ACTIVE_TURN_STEER_DESIGN.md](../../../CLI_ACTIVE_TURN_STEER_DESIGN.md)
- Live e2e commands and prior successes: [LIVE_E2E.md](../../../LIVE_E2E.md)
- Full pre-migration checklist and state: [legacy tracker snapshot](2026-05-05-legacy-tracker-snapshot-bbe4003.md)
