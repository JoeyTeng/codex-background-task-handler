# codex-background-task-handler

Reference experiments and companion tooling for making long-running background work practical around Codex without modifying the upstream `codex` repository.

## Status

Two directions are now validated:

- Desktop: keep the long-running work in an external sidecar, expose state through a small local interface, use a dedicated heartbeat bridge thread, and arm a caller-thread heartbeat only when a job is ready.
- CLI: use a wrapper that starts a shared `codex app-server`, run the foreground TUI with `codex --remote`, and attach one or more sidecar clients to the same live thread.

Design docs:

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
  - shared-state helper CLI
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

## Planned Implementation Direction

This repo is expected to grow in two layers:

1. Rust sidecar and helper CLI for low-overhead, portable background-task orchestration.
2. Small Python reference probes for one-off Desktop or protocol experiments.

Rust is the preferred implementation language for the real sidecar because it keeps resource usage low and is a better fit for cross-platform deployment anywhere Codex itself can run.

## License

This project is licensed under Apache License 2.0. See [LICENSE](LICENSE).
