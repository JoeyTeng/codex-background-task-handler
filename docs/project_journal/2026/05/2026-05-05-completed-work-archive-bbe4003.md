---
id: 20260505-bbe4003-completed
title: Completed Work Archive
status: completed
created: 2026-05-05
updated: 2026-05-06
branch: master
pr: https://github.com/JoeyTeng/codex-background-task-handler/pull/37
supersedes: []
superseded_by:
---

# Completed Work Archive

## Summary

- This entry archives the completed tracker themes that were previously mixed into `docs/PROJECT_STATE.md` and `docs/PROJECT_TODO.md`.
- The complete verbatim source records remain in [legacy tracker snapshot](2026-05-05-legacy-tracker-snapshot-bbe4003.md). Links in that snapshot's copied tracker blocks are intentionally historical; this archive provides current navigation links for completed themes.

## Completed Work

- Repo review gate was extracted to [tools/codex-review-gate](../../../../tools/codex-review-gate/README.md), wired as `codex/review-gate`, and live-validated for reaction-driven gating, review-body findings, resolved/outdated inline threads, and branch protection.
- Rust deterministic gate absorbed fake e2e coverage for job / batch / attempt delivery and passive `cbth cli run` no-delivery-RPC behavior.
- CLI explicit opt-in delivery landed: `cbth cli run --auto-delivery-policy trusted-all`, accepted-turn observation, pre-accept rejection handling, stale sweep to manual resolution, audit records, and `strict_safe` vs `trusted_all` authorization differences.
- CLI fresh-thread bootstrap landed through `cbth cli run --new-thread`, including fresh unmaterialized thread proof handling and opt-in live validation.
- Daemon-owned task supervisor landed with `cbth task run/list/inspect/cancel`, bounded task logs, result artifacts, fake e2e, and opt-in live task-supervisor e2e.
- `cbth doctor cli`, CLI operator recovery docs, local binary dogfood docs, and release install/update flow landed.
- `v0.1.0` was released with Linux x86_64 glibc and macOS arm64 assets, release workflow run `25336039804`, tag object `84ca65b57ada2dd696c960dd554c55364dec67ea`, and target commit `e6d013adbae633eb43efc5b7f9a3d680a4bb82a5`.
- Desktop experiments established that external app-server / `codex exec resume` can append persisted turns, but cannot reliably push into the live Desktop in-memory session.
- Desktop bridge architecture was documented and narrowed to bridge heartbeat plus pre-bound caller automation, direct-file-read snapshots, and gated `note-boundary-crossed` continuation.
- Desktop bridge foundation landed: installation state, binding repair, `bridge-preflight`, revision-consistent snapshot skeletons, and direct-file-read export documented in [DESKTOP_BRIDGE_FOUNDATION.md](../../../DESKTOP_BRIDGE_FOUNDATION.md).
- Shared core and early Rust phases landed: local store, artifact ingest / retention / GC, daemon IPC, domain RPC routing, lifecycle guard, accepted attempt state, managed sessions, app-server lifecycle, passive adapter, trusted-all delivery loop, audit, and capability gates.

## Evidence

- Migration PR: https://github.com/JoeyTeng/codex-background-task-handler/pull/37
- Release: [v0.1.0](https://github.com/JoeyTeng/codex-background-task-handler/releases/tag/v0.1.0)
- Live validation guide: [LIVE_E2E.md](../../../LIVE_E2E.md)
- CLI dogfood plan: [CLI_DOGFOOD_V1_COMPLETION_PLAN.md](../../../CLI_DOGFOOD_V1_COMPLETION_PLAN.md)
- CLI recovery guide: [CLI_OPERATOR_RECOVERY.md](../../../CLI_OPERATOR_RECOVERY.md)
- Shared architecture: [SHARED_CORE_ARCHITECTURE.md](../../../SHARED_CORE_ARCHITECTURE.md)
- Desktop validation plan: [DESKTOP_LIVE_PREFLIGHT_VALIDATION.md](../../../DESKTOP_LIVE_PREFLIGHT_VALIDATION.md)
- Historical PR / commit evidence is preserved verbatim in [legacy tracker snapshot](2026-05-05-legacy-tracker-snapshot-bbe4003.md).
