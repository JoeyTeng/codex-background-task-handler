---
id: 20260511-5721
title: Daemon Upgrade Safety
status: active
created: 2026-05-11
updated: 2026-05-12
branch: codex/daemon-jobs-drain
pr: https://github.com/JoeyTeng/codex-background-task-handler/pull/67
supersedes: []
superseded_by:
---

# Daemon Upgrade Safety

## Summary
- The upgrade stack is split into PR1 through PR5, with the `0.2.0` release PR held until the safety work lands.
- PR1 changed incompatible daemon replacement to fail closed by default.
- PR2 added generation daemon coexistence and scoped recovery ownership so a new daemon does not stop or recover work owned by an active old daemon.
- PR3 adds the `daemon-handoff-v1` protocol skeleton, `binary_version` gate, and quiesce state without app-server or job resource takeover.
- PR4 adds app-server handoff: legacy daemon exports owned app-server metadata, generation daemon adopts the same pid/url with pid identity fencing, and legacy refresh redirects wrappers to the new daemon.
- PR5 adds live jobs drain: quiescing daemons reject new task admission, keep already admitted tasks/jobs alive until terminal, and exit with `handoff_drain_complete` after their owned drain scope clears.

## Current State
- `docs/DAEMON_UPGRADE_SAFETY.md` is the design entrypoint for the upgrade sequence, PR3 gate/quiesce contract, PR4 app-server handoff contract, and PR5 jobs drain contract.
- Handoff minimum is fixed at `0.2.0`; lower versions can coexist but are not sent `handoff_quiesce`.
- A handoff-eligible incompatible default daemon is quiesced before the new binary starts or reuses a generation daemon.
- Quiescing daemons reject new work while keeping control, lease refresh/release/stop, thread abort, and task cancel paths available.
- Adopted app-servers keep the same websocket URL and pid; old daemon `handed_off` entries no longer stop or invalidate the app-server and return `handoff_daemon_socket_path` on matching refresh.
- Quiescing default daemons are no longer selected as new-work endpoints by `daemon ensure`; new tasks and mutating dispatches route to the generation daemon while the old daemon drains.
- Task cancellation is owner-routed by `supervisor_daemon_generation` before `daemon ensure`, so live unowned or generation-owned tasks continue to cancel through the quiescing daemon that owns their process controls; stale default owner sockets fall back to normal ensure, while stale generation owner sockets start/reuse the current generation daemon so generation-scoped startup recovery can kill and terminalize lost task process groups.
- Quiescing daemons force lifecycle refreshes, wait for owner-scoped pending jobs/tasks, ignore only already `handed_off` app-server shims, and auto-exit with `shutdown_reason=handoff_drain_complete` once drain is complete.
- PR4 quiesce export failure rolls back the new quiescing state, and adopted cleanup handles leader-exited process groups so app-server descendants are not orphaned.
- PR4 adopt/release registry changes are all-or-nothing across the handoff payload, and legacy handed-off entries remain child reapers until the child has exited.
- PR4 release failure now confirms legacy release status before rolling back generation adopted entries; old wrapper redirect immediately refreshes/stops against the new daemon; active bootstrap app-servers fail closed instead of being missed by export.
- PR4 active bootstrap handoff export failure is treated as a coexistence fallback for `daemon ensure`, so a concurrent foreground/thread-start bootstrap does not block the new generation daemon from starting.
- PR4 adopted entries now get a lease floor during handoff so a near-expired legacy lease cannot be reaped before the old wrapper follows the redirect.
- PR4 legacy exports now also extend owned leases through the release window, and already handed-off entries cannot be retargeted to a different generation socket.
- PR4 release-status recovery treats legacy entries that disappeared after export as `missing` and rolls back the generation adoption; adopted liveness also treats a waitable exited leader with no live process-group members as stopped.
- PR4 adopt preflight now rejects exited/zombie app-server leaders, and stale-export adopt failure unadopts any generation residue, fenced-unquiesces the legacy daemon, then degrades to generation coexistence.
- PR4 release rollback now fenced-unquiesces legacy after confirmed generation unadopt, and passive adapter daemon-routed writes proactively refresh/follow the handoff redirect instead of waiting for the periodic lease refresher.
- PR4 handoff quiesce fencing no longer covers long app-server spawn or `thread/start` RPC work; CLI app-server/bootstrap candidates that lose to quiesce before registry registration are rejected and stopped, while registered bootstraps still force coexistence fallback.

## Next Steps
- Release PR: bump `0.2.0`, update changelog/docs/install examples, and rerun release/version parsing checks after PR5 lands.

## Evidence
- Design: [DAEMON_UPGRADE_SAFETY.md](../../../DAEMON_UPGRADE_SAFETY.md)
- PR3: https://github.com/JoeyTeng/codex-background-task-handler/pull/64
- PR4: https://github.com/JoeyTeng/codex-background-task-handler/pull/66
- PR5: https://github.com/JoeyTeng/codex-background-task-handler/pull/67
- Local PR5 validation: `cargo test --locked task_run_rechecks_quiesce_before_registry_admission --target-dir /Users/hoteng/.cache/cargo-target/cbth-pr5-isolated`
- Local PR5 validation: `cargo test --locked --test daemon_phase2 task_run_uses_generation_daemon_when_incompatible_default_daemon_is_quiescing --target-dir /Users/hoteng/.cache/cargo-target/cbth-pr5-isolated`
- Local PR5 validation: `cargo test --locked --test daemon_phase2 task_cancel_falls_back_to_generation_recovery_when_owner_socket_is_stale --target-dir /Users/hoteng/.cache/cargo-target/cbth-pr5-isolated`
- Local PR5 validation: `cargo test --locked --test daemon_phase2 quiescing_daemon_waits_for_pending_job_then_exits_without_idle_timeout --target-dir /Users/hoteng/.cache/cargo-target/cbth-pr5-isolated`
- Local PR5 validation: `cargo test --locked --test daemon_phase2 quiescing_daemon_supervises_existing_task_to_terminal_before_exit --target-dir /Users/hoteng/.cache/cargo-target/cbth-pr5-isolated`
- Local PR5 validation: `cargo test --locked --test daemon_phase2 task_cancel_routes_to_quiescing_daemon_that_owns_live_task --target-dir /Users/hoteng/.cache/cargo-target/cbth-pr5-isolated`
- Local PR5 validation: `cargo test --locked --test daemon_phase2 task_cancel_routes_to_quiescing_generation_daemon_that_owns_live_task --target-dir /Users/hoteng/.cache/cargo-target/cbth-pr5-isolated`
- Local PR5 validation: `cargo fmt --all -- --check`
- Local PR5 validation: `git diff --check`
- Local PR5 validation: `uv run python /Users/hoteng/.codex/skills/project-journal/scripts/project_journal.py validate --repo /private/tmp/cbth-daemon-upgrade-stack`
- Local PR5 validation: `cargo test --locked --test daemon_phase2 --target-dir /Users/hoteng/.cache/cargo-target/cbth-pr5-isolated`
- Local PR5 validation: `cargo clippy --locked --all-targets --target-dir /Users/hoteng/.cache/cargo-target/cbth-pr5-isolated -- -D warnings`
- Local PR5 validation: `cargo test --locked --target-dir /Users/hoteng/.cache/cargo-target/cbth-pr5-isolated`
- Internal PR5 review: helper-managed `codex-readonly` found that `task_cancel` could route to generation instead of the quiescing daemon that owns the live task, and that a second quiesce request could fail if an already-quiescing default exited between ping and re-quiesce. Fixed with task owner-routed cancel and an endpoint-gone tolerant already-quiescing path; added regression coverage for both cases.
- Internal PR5 review follow-up: helper-managed `codex-readonly` found owner-routed cancel still ran `daemon ensure` before owner lookup, which could block quiescing generation-owned task cancel, and that skipping re-quiesce for already-quiescing default could strand app-server exports after a previous client crash. Fixed by resolving the task owner endpoint before ensure and by reattempting fenced export/adopt for already-quiescing default while tolerating normal endpoint-gone drain exit.
- Internal PR5 review follow-up: helper-managed `codex-readonly` found incompatible but already-quiescing default daemons still used the non-tolerant incompatible coexistence path, and stale owner sockets made `task cancel` fail before recovery. Fixed by deriving legacy coexistence from the quiescing flag and by retrying cancel through `daemon ensure` when the owner endpoint is gone.
- Internal PR5 review follow-up: helper-managed `codex-readonly` found generation-owned stale owner fallback started a default daemon, whose recovery scope intentionally skips current-generation tasks. Fixed by adding a generation-specific ensure path for generation-owned cancel fallback and by covering a killed generation daemon with a still-running task process group.
- Final internal PR5 review: helper-managed `codex-readonly` reviewed the fixed diff and returned `LGTM`.
- Local PR4 validation: `cargo check --locked --target-dir /Users/hoteng/.cache/cargo-target/cbth-pr4-isolated`
- Local PR4 validation: `cargo fmt --all -- --check`
- Local PR4 validation: `git diff --check`
- Local PR4 validation: `cargo clippy --locked --all-targets --target-dir /Users/hoteng/.cache/cargo-target/cbth-pr4-isolated -- -D warnings`
- Local PR4 validation: `cargo test --locked handoff_ --target-dir /Users/hoteng/.cache/cargo-target/cbth-pr4-isolated`
- Local PR4 validation: `cargo test --locked adopted_app_server_stop_terminates_group_after_leader_exit --target-dir /Users/hoteng/.cache/cargo-target/cbth-pr4-isolated`
- Local PR4 validation: `cargo test --locked handoff_release_status_reports_missing_for_removed_legacy_server --target-dir /Users/hoteng/.cache/cargo-target/cbth-pr4-isolated`
- Local PR4 validation: `cargo test --locked adopted_app_server_zombie_leader_without_members_is_stopped --target-dir /Users/hoteng/.cache/cargo-target/cbth-pr4-isolated`
- Local PR4 validation: `cargo test --locked handoff_unquiesce_clears_quiescing_with_fence --target-dir /Users/hoteng/.cache/cargo-target/cbth-pr4-isolated`
- Local PR4 validation: `cargo test --locked daemon_ensure_unquiesces_legacy_when_app_server_adopt_fails --target-dir /Users/hoteng/.cache/cargo-target/cbth-pr4-isolated`
- Local PR4 validation: `cargo test --locked handoff_release_rollback_unquiesces_legacy_daemon --target-dir /Users/hoteng/.cache/cargo-target/cbth-pr4-isolated`
- Local PR4 validation: `cargo test --locked passive_adapter_dispatch_follows_handoff_refresh_redirect --target-dir /Users/hoteng/.cache/cargo-target/cbth-pr4-isolated`
- Local PR4 validation: `cargo test --locked cli_app_server_ensure_quiesce_during_spawn_kills_candidate_server --target-dir /Users/hoteng/.cache/cargo-target/cbth-pr4-isolated`
- Local PR4 validation: `cargo test --locked thread_start_quiesce_before_bootstrap_registration_kills_candidate_server --target-dir /Users/hoteng/.cache/cargo-target/cbth-pr4-isolated`
- Local PR4 validation: `cargo test --locked --test daemon_phase2 --target-dir /Users/hoteng/.cache/cargo-target/cbth-pr4-isolated`
- Local PR4 validation: `cargo test --locked --target-dir /Users/hoteng/.cache/cargo-target/cbth-pr4-isolated`
- Internal PR4 review: helper-backed `codex-review` found quiesce export rollback and adopted leader-exit cleanup gaps; both fixed with regression tests.
- Internal PR4 review follow-up: second helper-backed `codex-review` found adopt/release partial mutation and handed-off child reaper gaps; fixed with all-or-nothing preflight and handed-off reap-owner retention tests.
- Internal PR4 review follow-up: third helper-backed `codex-review` found release-failure rollback, redirect refresh, and in-flight bootstrap/mutation fencing gaps; fixed with unadopt rollback, immediate refresh-on-redirect, and bootstrap fail-closed tests.
- Internal PR4 review follow-up: fourth helper-backed `codex-review` found uncertain release rollback and handed-off stop redirect gaps; fixed with release-status confirmation and stop-follow redirect tests.
- Internal PR4 review follow-up: fifth helper-backed `codex-review` found active-bootstrap quiesce errors still failed `daemon ensure`; fixed with coexistence fallback and daemon ensure coverage.
- Internal PR4 review follow-up: sixth helper-backed `codex-review` found near-expired adopted leases could be reaped before redirect discovery; fixed with an adopted lease floor and regression coverage.
- Internal PR4 review follow-up: seventh helper-backed `codex-review` found legacy owned leases could expire between export and release, and concurrent generation releases could retarget `handed_off` entries; fixed with legacy lease protection, same-socket idempotency, losing-adoption rollback, and regression coverage.
- Internal PR4 review follow-up: eighth helper-backed `codex-review` found release-status missing-entry rollback and adopted zombie-liveness gaps; fixed with `missing` release status, rollback decision coverage, and waitable-exited leader liveness coverage.
- Internal PR4 review follow-up: ninth helper-backed `codex-review` found stale-export adopt failures could leave legacy quiescing and zombie leaders could pass adopt preflight; fixed with fenced `handoff_unquiesce`, generation unadopt rollback, coexistence fallback, and adopt-time live-leader validation.
- Internal PR4 review follow-up: tenth helper-backed `codex-review` found confirmed release rollback could leave legacy quiescing and passive dispatch could wait for periodic refresh before following redirect; fixed with release-rollback unquiesce and dispatch-time refresh redirect coverage.
- Internal PR4 review follow-up: eleventh helper-backed `codex-review` found long bootstrap start/RPC work could hold the handoff lock and a retarget regression test leaked a handed-off test app-server; fixed by narrowing the lock to registry transitions, covering quiesced pre-registration cleanup, and making the retarget test adopt/cleanup through a generation owner.
- Internal PR4 review follow-up: twelfth helper-backed `codex-review` found `cli_app_server_ensure` still held the handoff lock across slow app-server spawn; fixed by narrowing ensure locking to registry transitions and routing post-spawn quiesce errors through candidate cleanup.
- Internal PR4 final review attempt: helper-backed `codex-review` and `codex-readonly` fallback both remained inconclusive after bounded waits and produced no final artifact; proceeded with the fixed review findings above plus current full test/clippy gates.
- PR4 GitHub review-gate finding: Codex found adopt could still skip rollback/unquiesce when startup budget was exhausted before the adopt RPC; fixed by using the post-adopt timeout floor for adopt and adding expired-deadline rollback coverage.
- Local PR3 validation: `cargo fmt --all -- --check`
- Local PR3 validation: `git diff --check`
- Local PR3 validation: `uv run python /Users/hoteng/.codex/skills/project-journal/scripts/project_journal.py validate --repo /private/tmp/cbth-daemon-upgrade-stack`
- Local PR3 validation: `cargo check --locked --target-dir /Users/hoteng/.cache/cargo-target/cbth-pr3-isolated`
- Local PR3 validation: `cargo test --locked --target-dir /Users/hoteng/.cache/cargo-target/cbth-pr3-isolated`
- Local PR3 validation: `cargo clippy --locked --all-targets --target-dir /Users/hoteng/.cache/cargo-target/cbth-pr3-isolated -- -D warnings`
- Internal PR3 review: helper-backed `codex-review` found a stale-ping quiesce race; fixed with expected pid/version fencing, then reran clean.
