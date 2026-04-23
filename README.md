# codex-background-task-handler

Reference experiments and companion tooling for making long-running background work practical around Codex without modifying the upstream `codex` repository.

## Status

Foundational experiments are validated, but end-to-end v1 continuation is still gated by unfinished contract tightening and capability validation:

- Desktop enabling experiments validated: automation-based bridge/retargeting experiments work. The current first-version delivery contract is built around a bound caller-heartbeat plus a delivery-envelope abstraction: bridge-side reads prefer `direct_file_read`, but that automatic pre-boundary file path is intentionally limited to ready/reconcile metadata and prompt tokens rather than payload/artifact bytes. Caller-side automatic continuation must cross the continuation boundary through a single-shot `note-boundary-crossed` before receiving any payload or artifact access. The full automatic path still depends on installation-local validation of the chosen installation-wide read transport, the narrow `note-*` writeback helpers, and `read-artifact` whenever a batch materializes large artifacts. Installation-wide capability conclusions now belong to `desktop_installation_state`, are scoped by transport generation, and are further bound to a validation fingerprint so Codex/Desktop or helper-environment drift invalidates older `validated` conclusions. Crossing the continuation boundary remains the last automatic durable point in v1: after that, batches stay `manual_resolution_only` until an operator closes them or their redelivery window expires, and lost post-boundary responses are recovered through operator tooling rather than automatic replay. These are still design-level contracts plus enabling PoCs, not end-to-end validated delivery.
- CLI protocol/TUI continuation foundations validated: the shared `app-server` route supports multi-client continuation on the same thread, and foreground TUI visibility is proven while the user manually keeps the foreground on the same caller thread used in the PTY validation. The current first-version contract uses explicit fixed-thread binding for daemon-owned managed sessions, with durable `managed_session_id` / `bound_thread_id` bookkeeping, durable session-scoped risk-profile fields (`session_allows_approval`, `session_allows_network`, `session_allows_write_access`), and continuity-loss fail-closed behavior still remaining design contracts rather than end-to-end validated implementation. That explicit bind controls only the delivery target; v1 does not prove or enforce current foreground focus, and live TUI visibility outside the validated same-thread case is not guaranteed. Any attach/recovery path must first re-enter `activity_state=unknown` and regain a current-state proof before auto-delivery may treat the caller as idle, so a current-state sync surface is now part of the minimum CLI capability set. Detached auto-delivery is only allowed for managed sessions whose durable risk profile is explicitly no-approval / no-network / no-write; sessions with a broader risk profile fall back to manual/operator handling. The route depends on capability probing around the experimental RPC surface and currently uses an unauthenticated loopback-only daemon-owned control plane, so v1 should be treated as supported only on dedicated single-user workstations until stronger local auth exists. `turn/steer` remains a gated optimization instead of a default path, and automatic thread rebind/switch routing is intentionally out of scope for v1.

The shared architecture is converging on one Rust binary with thin integration entrypoints:

- a CLI entrypoint that launches the shared `app-server` path
- a Desktop-facing helper/bridge entrypoint
- a shared local daemon, store, artifact manager, and job-control CLI surface underneath

Design docs:

- [docs/SHARED_CORE_ARCHITECTURE.md](docs/SHARED_CORE_ARCHITECTURE.md)
- [docs/DESKTOP_BACKGROUND_TASK_BRIDGE_DESIGN.md](docs/DESKTOP_BACKGROUND_TASK_BRIDGE_DESIGN.md)
- [docs/CLI_SHARED_APP_SERVER_SIDECAR_DESIGN.md](docs/CLI_SHARED_APP_SERVER_SIDECAR_DESIGN.md)

## Repository Layout

- `docs/`
  - architecture notes
  - experiment logs
  - next-step tracking
- `scripts/`
  - lightweight Python probes and reference PoCs
- future Rust crates
  - sidecar supervisor
  - job-control CLI / daemon entrypoints
  - production background-task bridge components

## Python Usage

Python is kept for small probes and reference scripts only. Run scripts in this repo with `uv`.

Example:

```bash
uv run python scripts/desktop_thread_inject_poc.py --thread-id <thread-id> --mode inject
```

The current PoC script is [scripts/desktop_thread_inject_poc.py](scripts/desktop_thread_inject_poc.py). It starts a standalone `codex app-server`, targets an existing thread, and records whether external injection or `turn/start` operations persist into the rollout.

The current CLI shared-server PoC is [scripts/cli_shared_app_server_poc.mjs](scripts/cli_shared_app_server_poc.mjs). It connects two websocket clients to the same shared `codex app-server`, seeds a frontend thread, then validates that a sidecar client can resume the same thread and that the frontend client receives the resulting live turn notifications.

In addition to the protocol-level PoC, the shared-server CLI route has also been validated against a real foreground TUI session running through `codex --remote`, confirming that the user-facing TUI output reflects the sidecar-triggered turn while the foreground stays on the same caller thread used in the PTY validation. That PTY validation matches the current loopback-only upstream surface available in `codex-cli 0.123.0`.

The current CLI active-turn steering PoC is [scripts/cli_turn_steer_poc.mjs](scripts/cli_turn_steer_poc.mjs). It starts a long-running turn, submits `turn/steer` from a second client while that turn is still active, and validates that the same turn completes normally instead of ending early.

## Planned Implementation Direction

This repo is expected to grow around one shared Rust core with thin per-surface entrypoints:

1. A Rust daemon/store/job-control core with a stable CLI surface.
2. A managed artifact store plus thread-scoped inbox / delivery-batch scheduler inside that core.
3. Thin CLI and Desktop integration entrypoints on top of that core.
4. Small reference probes for one-off Desktop or protocol experiments.

Rust is the preferred implementation language for the real sidecar because it keeps resource usage low and is a better fit for cross-platform deployment anywhere Codex itself can run.

## License

This project is licensed under Apache License 2.0. See [LICENSE](LICENSE).
