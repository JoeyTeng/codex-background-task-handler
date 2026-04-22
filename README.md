# codex-background-task-handler

Reference experiments and companion tooling for making long-running background work practical around Codex without modifying the upstream `codex` repository.

## Status

The current validated Desktop direction is:

- keep the real long-running work in an external sidecar
- expose sidecar state through a small local interface
- use a dedicated Desktop heartbeat bridge thread to watch for ready jobs
- arm a caller-thread heartbeat only when a job is actually ready
- let the original caller thread resume the task inside Codex Desktop

The design is captured in [docs/DESKTOP_BACKGROUND_TASK_BRIDGE_DESIGN.md](docs/DESKTOP_BACKGROUND_TASK_BRIDGE_DESIGN.md).

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

## Planned Implementation Direction

This repo is expected to grow in two layers:

1. Rust sidecar and helper CLI for low-overhead, portable background-task orchestration.
2. Small Python reference probes for one-off Desktop or protocol experiments.

Rust is the preferred implementation language for the real sidecar because it keeps resource usage low and is a better fit for cross-platform deployment anywhere Codex itself can run.

## License

This project is licensed under Apache License 2.0. See [LICENSE](LICENSE).
