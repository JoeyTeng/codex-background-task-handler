---
id: 20260513-39ae8fe-c2-plugin-service-supervisor
title: C2 Plugin Service Supervisor
status: completed
created: 2026-05-13
updated: 2026-05-16
branch: codex/c2-plugin-service-supervisor
pr: https://github.com/JoeyTeng/codex-background-task-handler/pull/81
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
- C2 is based on the landed C1 plugin RPC skeleton on `master`.

## Next Steps

- Follow-up C3 should add plugin-scoped `app_server.ensure/refresh/stop` RPC rather than extending this C2 supervisor slice.
- Follow-up C4/C5/C6 remain responsible for delivery, service install/manage, and release lifecycle respectively.

## Evidence

- Base branch head: `326748a`
- Branch: `codex/c2-plugin-service-supervisor`
- Dependency: C1 PR #78 merged before PR #81 was retargeted to `master`.
- CI follow-up: PR #81 clippy failures from `clippy::bool-assert-comparison` in `service_shutdown_reaps_managed_plugin_child` and Linux-only `AsRawFd` import drift after the C1 base update were fixed on 2026-05-16.
- Codex review follow-up: Linux now validates plugin UDS peers with `SO_PEERCRED`, `cbth plugin status` forces disabled manifests to report `Disabled` without stale process identity, and service startup fails closed instead of relaunching or killing when only a persisted PID identifies a live process group.
- Local review: helper-backed `codex-review` found active-socket replacement, idle status persistence, failed-hello status, reserved environment, foreground signal shutdown, and stale runtime health identity issues; all were fixed before commit.
- Validation:
  - `cargo fmt --check`
  - `cargo fmt --all -- --check`
  - `cargo test --lib service::tests -- --test-threads=1`
  - `cargo test --lib plugin_rpc::tests -- --test-threads=1`
  - `cargo test --test cli_help -- --test-threads=1`
  - `cargo test --test desktop_foundation -- --test-threads=1`
  - `cargo test --test daemon_phase2 -- --test-threads=1`
  - `env CARGO_TARGET_DIR=/private/tmp/cbth-pr81/target cargo clippy --locked --all-targets -- -D warnings`
  - `env CARGO_TARGET_DIR=/private/tmp/cbth-pr81/target cargo test --locked service::tests -- --test-threads=1`
  - `env CARGO_TARGET_DIR=/private/tmp/cbth-pr81/target cargo test --locked`
  - `uv run /Users/hoteng/.codex/skills/project-journal/scripts/project_journal.py validate --repo .`
  - `git diff --check`
- Full-suite note: `cargo test -- --test-threads=1` reached one non-C2 intermittent failure in `cli_run_trusted_all_task_run_auto_delivery_closes_delivered`; the exact test then passed, and `cargo test --test cli_run -- --test-threads=1` passed 63/63.
- Local sandbox note: full-suite socket tests require running outside the Codex filesystem sandbox; the sandboxed retry failed with `Operation not permitted` on Unix socket bind, and the same `cargo test --locked` command passed after escalation.
