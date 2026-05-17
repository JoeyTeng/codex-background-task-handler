---
id: 20260516-8a9a75f-c3-plugin-app-server-rpc
title: C3 Plugin App-Server Lease RPC
status: completed
created: 2026-05-16
updated: 2026-05-17
branch: codex/c3-plugin-app-server-rpc
pr: https://github.com/JoeyTeng/codex-background-task-handler/pull/88
supersedes:
superseded_by:
---

# C3 Plugin App-Server Lease RPC

## Summary

- C3 adds plugin-scoped `app_server.ensure`, `app_server.refresh`, and `app_server.stop` RPC handling to `cbth service`.
- The service brokers authenticated/current plugin connections into daemon-owned app-server lease machinery.
- Scope excludes generic delivery, service install/manage, plugin release management, and Webex-specific behavior.

## Current State

- `src/plugin_rpc.rs` defines typed `app_server.*` request payloads and method constants.
- `src/service.rs` validates current plugin identity on every app-server RPC, scopes plugin-visible lease ids by plugin name and instance id, fences same-lease replay against target drift, follows daemon handoff endpoints, caps plugin-supplied lease TTLs, and best-effort stops connection-owned or service-shutdown leftover app-server leases.
- The service reuses daemon `cli_app_server_*` commands and existing generation-aware daemon ensure behavior instead of spawning app-servers directly.
- `docs/design/HOST_PLUGIN_RUNTIME_AND_DELIVERY.md` now records the C3 method contract.

## Next Steps

- Continue with W3 planning on top of the C3 plugin app-server lease RPC contract.

## Evidence

- Base: `origin/master` at `8a9a75f`
- Branch: `codex/c3-plugin-app-server-rpc`
- PR: https://github.com/JoeyTeng/codex-background-task-handler/pull/88
- Local validation so far:
  - `cargo fmt --check`
  - `cargo test --lib service::tests -- --test-threads=1`
  - `cargo test --lib plugin_rpc::tests -- --test-threads=1`
  - `cargo clippy --locked --all-targets -- -D warnings`
  - `uv run /Users/hoteng/.codex/skills/project-journal/scripts/project_journal.py validate --repo .`
  - `git diff --check`
  - `cargo test --locked -- --test-threads=1`
- Fresh-context review:
  - `isolated_review stateful ... --entrypoint codex-review`
  - Final clean state dir: `.codex-tmp/isolated-review-607h0_30`
