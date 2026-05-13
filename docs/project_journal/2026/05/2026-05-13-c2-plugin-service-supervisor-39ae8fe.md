---
id: 20260513-39ae8fe-c2-plugin-service-supervisor
title: C2 Plugin Service Supervisor
status: active
created: 2026-05-13
updated: 2026-05-13
branch: codex/c2-plugin-service-supervisor
pr:
supersedes:
superseded_by:
---

# C2 Plugin Service Supervisor

## Summary

- C2 adds the host plugin service foundation on top of the C1 plugin RPC skeleton.
- Scope is limited to `cbth service run`, JSON host plugin registry loading, plugin home/state/log layout, process supervision, hello/health handling, restart backoff, bounded stdout/stderr logs, and `cbth plugin status`.
- C3 app-server lease RPC, C4 delivery RPC, C5 LaunchAgent management, and C6 release/quiesce/drain/rollback remain out of scope.

## Current State

- `cbth service run` reads `~/.cbth/plugins/registry.json`, prepares `~/.cbth/plugins/<plugin_name>/`, launches enabled plugin processes, exposes a service UDS for C1 `plugin.hello`, and persists per-plugin supervisor status under plugin state.
- `cbth plugin status [name]` reads registry/status files and does not autostart the service or any plugin.
- The C2 branch is stacked on C1 head `39ae8fe49ba25615385b292cdd6ed1e6628ba460`; real integration still depends on C1 PR #78 landing first.

## Next Steps

- Keep this PR based on `codex/c1-plugin-rpc-skeleton` until C1 lands or is rebased.
- Follow-up C3 should add plugin-scoped `app_server.ensure/refresh/stop` RPC rather than extending this C2 supervisor slice.
- Follow-up C4/C5/C6 remain responsible for delivery, service install/manage, and release lifecycle respectively.

## Evidence

- Base: `39ae8fe49ba25615385b292cdd6ed1e6628ba460`
- Branch: `codex/c2-plugin-service-supervisor`
- Dependency: C1 draft PR #78
- Local review: helper-backed `codex-review` found active-socket replacement, idle status persistence, failed-hello status, reserved environment, foreground signal shutdown, and stale runtime health identity issues; all were fixed before commit.
- Validation:
  - `cargo fmt --check`
  - `cargo test --lib service::tests -- --test-threads=1`
  - `cargo test --lib plugin_rpc::tests -- --test-threads=1`
  - `cargo test --test cli_help -- --test-threads=1`
  - `cargo test --test desktop_foundation -- --test-threads=1`
  - `cargo test --test daemon_phase2 -- --test-threads=1`
  - `uv run /Users/hoteng/.codex/skills/project-journal/scripts/project_journal.py validate --repo /Users/hoteng/Program/GitHub/codex-background-task-handler`
  - `git diff --check`
- Full-suite note: `cargo test -- --test-threads=1` reached one non-C2 intermittent failure in `cli_run_trusted_all_task_run_auto_delivery_closes_delivered`; the exact test then passed, and `cargo test --test cli_run -- --test-threads=1` passed 63/63.
