# codex-background-task-handler

Reference experiments and companion tooling for making long-running background work practical around Codex without modifying the upstream `codex` repository.

## Status

Validated foundations plus the current first-version direction:

- Desktop foundation validated: automation-based bridge/retargeting works. The current first-version delivery contract uses read-only inbox snapshots and managed artifacts, but the “heartbeat can read those files without approval” assumption is still pending validation.
- CLI foundation validated: the shared `app-server` route works and foreground TUI visibility is proven. The current first-version contract depends on capability probing around the experimental RPC surface, defaults to loopback plus per-session bearer-token auth, and keeps `turn/steer` as a gated optimization instead of a default path.

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

In addition to the protocol-level PoC, the shared-server CLI route has also been validated against a real foreground TUI session running through `codex --remote`, confirming that the user-facing TUI output reflects the sidecar-triggered turn.

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
