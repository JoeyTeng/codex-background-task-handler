---
id: 20260517-8241b68-c4-generic-delivery-core
title: C4 Generic Delivery Core
status: completed
created: 2026-05-17
updated: 2026-05-17
branch: codex/c4-generic-delivery-core
pr:
supersedes:
superseded_by:
---

# C4 Generic Delivery Core

## Summary

- C4 adds plugin RPC entrypoints for `delivery.enqueue`, `delivery.inspect`, and `delivery.manualize`.
- The v1 delivery driver allowlist is deliberately limited to `codex_app_server`, using the C3 plugin-scoped app-server lease contract.
- Raw Codex CLI and Desktop delivered-success semantics remain unsupported by generic delivery core v1.

## Current State

- `src/plugin_rpc.rs` defines typed delivery request payloads and method constants.
- `src/service.rs` brokers current plugin connections into scoped delivery operations, validates the requested app-server lease target against the source thread / managed session / epoch, starts delivery through the daemon-owned app-server, and reconciles accepted turns through app-server turn observation.
- `src/store.rs` persists plugin delivery replay fences in the existing SQLite store, creates completed jobs plus batches through existing job/batch machinery, stores optional canonical artifact references in the existing artifact table, and records delivery attempts / manual-resolution outcomes through existing attempt and audit machinery.
- Delivery idempotency is scoped by plugin name plus idempotency key. Inspect/manualize by job or batch id also rechecks the plugin-scoped replay fence before returning or mutating state.
- `codex_app_server` delivery attempts reuse the existing accepted-turn proof fields; the implementation does not add Desktop success, raw CLI success, or a parallel delivery store.
- New plugin delivery batches are committed with their `codex_app_server` accept-pending attempt in the same SQLite transaction when they are already the thread head.
- The `plugin_delivery_requests` replay fence reserves queued plugin batches from generic CLI/Desktop delivery drivers until a plugin replay can start the supported `codex_app_server` driver.
- Idempotent replay resumes a persisted delivery batch only when no delivery attempt was created yet. Unknown in-flight `turn/start` outcomes remain `accept_pending`; stale pre-start accept-pending attempts age into manual resolution with `delivery_rpc_state = 'unknown'` instead of being misrecorded as pre-accept rejection.
- Stored plugin delivery metadata records payload and plugin-metadata digests/sizes rather than echoing full caller payloads into accepted responses.
- `codex_app_server` attempts are recorded as their own adapter kind so CLI/Desktop maintenance does not claim their delivered-success semantics.
- Store lifecycle maintenance now counts and sweeps stale `codex_app_server` accept-pending attempts and expired `codex_app_server` observations, so plugin delivery cannot permanently block a thread if the caller never retries; the accept-pending stale window is longer than the app-server `turn/start` acceptance timeout.
- When daemon lifecycle maintenance is suppressed, stale/due `codex_app_server` delivery cleanup uses a codex-only sweep instead of running the full store maintenance sweep.
- Plugin connection cleanup defers best-effort app-server stop while an active `codex_app_server` delivery attempt still targets the same managed session and epoch; the daemon-owned lease TTL remains the eventual cleanup boundary.
- Busy pre-accept `turn/start` rejections remain automatic retry candidates instead of forcing manual resolution; each retry gets its own attempt, RPC request fence, and audit record.
- Retryable pre-accept `turn/start` rejections consume the plugin batch delivery attempt budget; when repeated busy rejections exhaust the explicit max attempts, the batch moves to manual resolution instead of creating unbounded attempts/audit records.
- `delivery.inspect` reports retryable abandoned attempts as queued while the plugin batch remains open and automatic, and reports no-attempt terminal batches as closed.
- Service shutdown cleanup uses the same active-delivery retention guard as per-connection lease cleanup, preventing shutdown from stopping an app-server still needed for delivery observation.
- Replay attempt creation reports whether an attempt was actually created, so concurrent idempotent replays cannot start duplicate app-server turns for the same accept-pending attempt.
- Plugin deliveries can queue behind an older open head batch and start on idempotent replay after the head advances; same-second enqueue ordering is preserved by advancing new plugin batch timestamps after existing open batches for the same thread.
- Explicit `app_server.stop` retains the lease while an active `codex_app_server` delivery still depends on the same managed session and epoch.
- Oversized generated app-server `turn/start` frames are rejected before local send and manualized deterministically instead of lingering as unknown accept-pending attempts.
- `codex_app_server` `turn/start` delivery uses a 60-second acceptance timeout matching the existing app-server turn-start window, refreshes the delivery lease again after accepted `turn/start`, and uses a longer stale window for pre-start recovery / maintenance sweep.
- Post-accept app-server lease refresh is best-effort: once `turn/start` is accepted and persisted, a refresh failure is recorded in audit details but does not turn the already-started delivery enqueue into an RPC error.
- Manualizing a `codex_app_server` acceptance-pending attempt preserves `delivery_rpc_state = 'unknown'`, so same-epoch observation is not misclassified as a pre-accept rejection.
- Stale `codex_app_server` acceptance and due observation cleanup escapes daemon lifecycle maintenance suppression, so plugin delivery cleanup can run even before a later CLI/task command unsuppresses maintenance.
- Artifact deliveries populate the completed job result artifact id, include the absolute payload path in the app-server prompt, and remain queued for normal artifact manifest sync.
- Artifact-backed delivery validates artifact ids with the same path-component rules as the store before target lease lookup or store writes.
- No-attempt plugin batches that are manualized while still queued now inspect as `manual_resolution_only` instead of `queued`.

## Next Steps

- Build Webex W4 background / async notification routing on `delivery.enqueue` while keeping normal user-message forwarding on the direct C3 app-server lease path.
- Keep Desktop and raw CLI generic-delivery support out of v1 until their observation, boundary-crossing, and artifact policies are implemented.

## Evidence

- Base: C3 merge commit `8241b68d58663045fd23d045d95d38a6921d45ec`
- Branch: `codex/c4-generic-delivery-core`
- Local validation so far:
  - `cargo check --lib`
  - `cargo fmt --check`
  - `cargo test --lib delivery -- --test-threads=1`
  - `cargo test --lib service::tests -- --test-threads=1`
  - `cargo test --lib store::tests -- --test-threads=1`
  - `cargo test --lib plugin_rpc::tests -- --test-threads=1`
  - Final repeat `cargo test --locked -- --test-threads=1`
  - `cargo test --locked --test daemon_phase2 task_cancel_reused_generation_daemon_recovers_stale_previous_generation_task -- --test-threads=1`
  - `cargo test --locked --test daemon_phase2 -- --test-threads=1`
  - `cargo test --locked --test desktop_foundation -- --test-threads=1`
  - `cargo test --locked --test doctor_cli -- --test-threads=1`
  - `cargo test --locked live_ -- --test-threads=1`
  - `cargo clippy --locked --all-targets -- -D warnings`
  - `uv run /Users/hoteng/.codex/skills/project-journal/scripts/project_journal.py validate --repo .`
  - `git diff --check`
- Fresh-context local review: `codex-review` stateful lane reported four findings; fixes landed for replay resume, unknown `turn/start` outcomes, manualize attempt abandonment, and canonical plugin artifact paths.
- Second fresh-context local review: `codex-review` stateful lane reported three findings; fixes landed for atomic plugin batch/attempt creation, lean enqueue metadata, and paginated `thread/turns/list` reconciliation.
- Third fresh-context local review: `codex-review` stateful lane reported two findings; fixes landed for full-window delivery app-server lease TTL and artifact prompt absolute payload paths.
- Fourth fresh-context local review: `codex-review` stateful lane reported three findings; fixes landed for pre-turn/start manualization, unsupported `thread/turns/list` fallback variants, and observation/lease headroom.
- Fifth fresh-context local review: `codex-review` stateful lane reported three findings; fixes landed for `codex_app_server` adapter isolation, app-server stop deferral while active deliveries exist, and artifact-backed job result ids.
- Sixth fresh-context local review: `codex-review` stateful lane reported one finding; fix landed for stale pre-start accept-pending replay recovery through automatic manual resolution.
- Seventh fresh-context local review: `codex-review` stateful lane reported one finding; fix landed for `codex_app_server` lifecycle timeout counting and sweep cleanup.
- Eighth fresh-context local review: `codex-review` stateful lane reported three findings; fixes landed for retryable busy `turn/start` rejection handling, plugin artifact manifest sync queueing, and pre-send oversized app-server frame rejection.
- Ninth fresh-context local review: `codex-review` stateful lane reported three findings; fixes landed for replay begin-created detection, queueing behind existing head batches, and active-delivery lease retention on explicit stop.
- Tenth fresh-context local review: `codex-review` stateful lane reported three findings; fixes landed for reserving queued plugin batches from generic drivers, manualizing stale unknown `turn/start` attempts as unknown, and including attempt-specific audit fences.
- Eleventh fresh-context local review: `codex-review` stateful lane reported two findings; fixes landed for inspect state on retryable abandoned attempts and service shutdown lease retention during active delivery.
- Twelfth fresh-context local review: `codex-review` stateful lane reported one finding; fix landed for same-second plugin batch FIFO ordering despite deterministic idempotency-derived batch ids.
- Thirteenth fresh-context local review: `codex-review` stateful lane reported one finding; fix landed for using the longer `turn/start` acceptance timeout on side-effectful delivery starts.
- Fourteenth fresh-context local review: `codex-review` stateful lane reported two findings; fixes landed for stale accept-pending headroom beyond the `turn/start` request deadline and closed no-attempt inspect state.
- Fifteenth fresh-context local review: `codex-review` stateful lane reported one finding; fix landed for service/store artifact id validation consistency.
- Sixteenth fresh-context local review: `codex-review` stateful lane reported one finding; fix landed for refreshing the delivery app-server lease after accepted `turn/start`.
- Seventeenth fresh-context local review: `codex-review` stateful lane reported two findings; fixes landed for suppression-escape lifecycle cleanup and no-attempt manualized inspect state.
- Eighteenth fresh-context local review: helper `codex-review` stalled without a final artifact; deterministic `codex-readonly` fallback reported one finding, and the fix landed for limiting suppressed lifecycle cleanup to a codex-only sweep.
- Nineteenth fresh-context local review: deterministic `codex-readonly` reported one finding; fix landed for treating post-accept delivery lease refresh as best-effort.
- Twentieth fresh-context local review: deterministic `codex-readonly` reported one finding; fix landed for preserving unknown `codex_app_server` acceptance state when manualized.
- Twenty-first fresh-context local review: deterministic `codex-readonly` reported one finding; fix landed for bounding repeated busy pre-accept `turn/start` retries by the batch attempt budget.
- Final fresh-context local review: deterministic `codex-readonly` completed with `LGTM`.
- During broad validation, two `daemon_phase2` runs hit different startup-timing failures while orphaned temporary test daemons were still present from earlier runs; both failing tests passed targeted reruns, the orphaned temporary daemons were terminated, and the final full repeat passed.
- Latest targeted validation:
  - `cargo fmt --check`
  - `cargo test --lib service::tests::delivery_enqueue_busy_turn_start_rejections_exhaust_attempt_budget -- --test-threads=1`
  - `cargo test --lib service::tests::delivery_manualize_preserves_unknown_codex_app_server_acceptance -- --test-threads=1`
  - `cargo test --lib daemon::tests::lifecycle_sweep_due_escapes_suppression_for_codex_app_server_delivery_cleanup -- --test-threads=1`
  - `cargo test --lib store::tests::codex_app_server_cleanup_sweep_does_not_run_suppressed_cli_cleanup -- --test-threads=1`
  - `cargo test --lib service::tests::delivery_enqueue_treats_post_accept_lease_refresh_failure_as_best_effort -- --test-threads=1`
  - `cargo test --lib service::tests::delivery_inspect_reports_manualized_queued_batch_without_attempt_as_manual -- --test-threads=1`
  - `cargo test --lib store::tests::plugin_delivery_batches_are_reserved_from_generic_delivery_drivers -- --test-threads=1`
  - `cargo test --lib service::tests::delivery_enqueue_replay_manualizes_stale_pre_start_accept_pending -- --test-threads=1`
  - `cargo test --lib service::tests::delivery_enqueue_records_audit_for_each_retry_attempt -- --test-threads=1`
  - `cargo test --lib service::tests::delivery_enqueue_retries_busy_turn_start_rejection_without_manualizing -- --test-threads=1`
  - `cargo test --lib service::tests::service_shutdown_cleanup_preserves_active_delivery_app_server -- --test-threads=1`
  - `cargo test --lib service::tests::delivery_enqueue_manualizes_oversized_turn_start_frame_before_send -- --test-threads=1`
  - `cargo test --lib service::tests::delivery_enqueue_rejects_invalid_artifact_id_before_store_write -- --test-threads=1`
  - `cargo test --lib store::tests::plugin_delivery_artifact_sets_completed_job_result_artifact_id -- --test-threads=1`
  - `cargo test --lib service::tests::delivery_enqueue_queues_behind_existing_head_batch_and_replays_later -- --test-threads=1`
  - `cargo test --lib service::tests::active_codex_app_server_delivery_retains_explicit_stop_lease -- --test-threads=1`
  - `cargo test --lib service::tests::delivery_app_server_pre_start_recovery_has_acceptance_headroom -- --test-threads=1`
  - `cargo test --lib service::tests::delivery_inspect_reports_closed_batch_without_attempt_as_closed -- --test-threads=1`
  - `cargo test --lib store::tests::codex_app_server_begin_reports_existing_attempt_without_creating -- --test-threads=1`
  - `cargo test --lib store::tests::plugin_delivery_batches_preserve_fifo_order_with_same_second_enqueue -- --test-threads=1`
  - `cargo test --lib store::tests -- --test-threads=1`
  - `cargo test --lib service::tests -- --test-threads=1`
  - `cargo test --lib daemon::tests -- --test-threads=1`
  - `cargo clippy --locked --all-targets -- -D warnings`
  - `cargo test --locked -- --test-threads=1`
  - `uv run /Users/hoteng/.codex/skills/project-journal/scripts/project_journal.py validate --repo .`
  - `git diff --check`
