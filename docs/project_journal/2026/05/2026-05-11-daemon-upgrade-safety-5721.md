---
id: 20260511-5721
title: Daemon Upgrade Safety
status: active
created: 2026-05-11
updated: 2026-05-11
branch: codex/daemon-handoff-skeleton
pr:
supersedes: []
superseded_by:
---

# Daemon Upgrade Safety

## Summary
- The upgrade stack is split into PR1 through PR5, with the `0.2.0` release PR held until the safety work lands.
- PR1 changed incompatible daemon replacement to fail closed by default.
- PR2 added generation daemon coexistence and scoped recovery ownership so a new daemon does not stop or recover work owned by an active old daemon.
- PR3 adds the `daemon-handoff-v1` protocol skeleton, `binary_version` gate, and quiesce state without app-server or job resource takeover.

## Current State
- `docs/DAEMON_UPGRADE_SAFETY.md` is the design entrypoint for the upgrade sequence and the PR3 gate/quiesce contract.
- Handoff minimum is fixed at `0.2.0`; lower versions can coexist but are not sent `handoff_quiesce`.
- A handoff-eligible incompatible default daemon is quiesced before the new binary starts or reuses a generation daemon.
- Quiescing daemons reject new work while keeping control, lease refresh/release/stop, thread abort, and task cancel paths available.

## Next Steps
- PR4: implement app-server handoff by exporting owned app-server state and adopting it without changing the websocket URL or pid.
- PR5: implement live jobs drain so old daemons reject new task work, supervise existing tasks to terminal, and exit after active jobs clear.
- Release PR: bump `0.2.0`, update changelog/docs/install examples, and rerun release/version parsing checks.

## Evidence
- Design: [DAEMON_UPGRADE_SAFETY.md](../../../DAEMON_UPGRADE_SAFETY.md)
- Local PR3 validation: `cargo fmt --all -- --check`
- Local PR3 validation: `git diff --check`
- Local PR3 validation: `uv run python /Users/hoteng/.codex/skills/project-journal/scripts/project_journal.py validate --repo /private/tmp/cbth-daemon-upgrade-stack`
- Local PR3 validation: `cargo check --locked --target-dir /Users/hoteng/.cache/cargo-target/cbth-pr3-isolated`
- Local PR3 validation: `cargo test --locked --target-dir /Users/hoteng/.cache/cargo-target/cbth-pr3-isolated`
- Local PR3 validation: `cargo clippy --locked --all-targets --target-dir /Users/hoteng/.cache/cargo-target/cbth-pr3-isolated -- -D warnings`
- Internal PR3 review: helper-backed `codex-review` found a stale-ping quiesce race; fixed with expected pid/version fencing, then reran clean.
