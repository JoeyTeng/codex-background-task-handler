# codex-background-task-handler

Reference experiments and companion tooling for making long-running background work practical around Codex without modifying the upstream `codex` repository.

## Status

Foundational experiments are validated, but end-to-end v1 continuation is still gated by unfinished contract tightening and capability validation:

- Desktop enabling experiments validated: automation-based bridge/retargeting experiments work. The current first-version delivery contract is built around a bound caller-heartbeat plus a delivery-envelope abstraction: bridge-side reads prefer `direct_file_read`, but every bridge wake must first run the narrow `cbth desktop bridge-preflight ...` helper to start the daemon if needed, sweep overdue work, and atomically publish a ready/reconcile snapshot manifest/revision, with each referenced file checked against that revision. That automatic pre-boundary file path is intentionally limited to ready/reconcile metadata and prompt tokens rather than payload/artifact bytes. Caller-side automatic continuation must cross the continuation boundary through a single-shot `note-boundary-crossed` before receiving any continuation content. Under the tightened v1 contract, a fresh `note-boundary-crossed` success authorizes only a bounded inline text handoff and immediately closes the batch as `close_reason=handoff_recorded`, which releases FIFO but does not prove the caller assistant response became visible. Large-artifact continuation and post-boundary ordinary tool use are left to operator/manual follow-up rather than the automatic path. Installation-wide capability conclusions now belong to `desktop_installation_state`, are scoped by transport generation, and are further bound to a validation fingerprint so Codex/Desktop or helper-environment drift invalidates older `validated` conclusions. Lost post-boundary responses are recovered by batch id through operator tooling rather than automatic replay. These are still design-level contracts plus enabling PoCs, not end-to-end validated delivery.
- CLI protocol/TUI continuation foundations validated: the shared `app-server` route supports multi-client continuation on the same thread, and foreground TUI visibility is proven while the user manually keeps the foreground on the same caller thread used in the PTY validation. The current implementation has durable CLI delivery-attempt state for the accept-pending and accepted-observation slice, durable daemon-owned CLI managed-session records keyed by `managed_session_id` / `bound_thread_id`, immutable session-scoped risk-profile fields, and fail-closed `begin-cli-accept` validation. `cbth cli run --bind-thread-id <thread_id>` now binds a managed session, asks the daemon to own a loopback `codex app-server`, launches foreground `codex --remote`, and starts a sidecar client that keeps durable `active` / `idle` state current from initialize / `thread/resume` / `thread/read` plus lifecycle notifications. By default it remains passive and sends no delivery RPC. With explicit `--auto-delivery-policy trusted-all`, the sidecar records full automatic-delivery capability for the current epoch, waits for durable idle proof, writes an accept-pending barrier and audit records, sends one marked `turn/start`, accepts the returned `turn.id`, and closes delivered from matching completed notification or same-epoch `thread/read` reconcile. Clear pre-accept rejection leaves the batch retryable without charging an attempt; timeout/closed/protocol ambiguity is not retried and is left for stale sweep to mark `unknown + manual_resolution_only`. Bootstrap remains designed as two explicit modes: existing-thread mode via `cbth cli run --bind-thread-id <thread_id>` and conditional fresh-thread mode via `cbth cli run --new-thread` when `thread/start` is available. That bind controls only the delivery target; v1 does not prove or enforce current foreground focus, and live TUI visibility outside the validated same-thread case is not guaranteed. `trusted-all` is an explicit broad escape hatch for dedicated single-user workstations and bypasses batch policy, artifact-read, and managed-session risk-profile gates while still requiring an open head batch, budget, matching thread/session/epoch, and fresh idle proof. The route depends on capability probing around the experimental RPC surface; the upstream shared `app-server` side is currently unauthenticated loopback-only, while `cbth`'s own daemon IPC is scoped to a same-user Unix socket. v1 should still be treated as supported only on dedicated single-user workstations until stronger upstream local auth exists. `turn/steer` remains out of scope for automatic delivery, and automatic thread rebind/switch routing is intentionally out of scope for v1.

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
- `src/`
  - Rust `cbth` binary and shared core modules
- `tests/`
  - Rust integration tests for the core CLI and daemon control surface
- `scripts/`
  - lightweight Python probes and reference PoCs

## Local Git Hooks

本仓库提供 tracked pre-commit hook，用来在提交前运行本地确定性 Rust gate：`cargo fmt --all`、`cargo clippy --locked --all-targets -- -D warnings`、`cargo test --locked`。

```bash
bash scripts/install-git-hooks.sh
```

详细设计、工作树安全策略和后续跟踪见 [docs/GIT_HOOKS.md](docs/GIT_HOOKS.md)。

## Testing

默认 CI gate 保持单一 Rust matrix：Ubuntu 与 macOS 都运行 `cargo fmt --all -- --check`、`cargo clippy --locked --all-targets -- -D warnings`、`cargo test --locked`。Deterministic fake e2e 覆盖在 Rust integration tests 中，因此会随 `cargo test --locked` 自动进入双平台 gate。

真实 Codex shared `app-server` smoke 是 opt-in，不在默认 CI 执行；它只保持编译和 lint-clean。需要本机真实 `codex`、Node、登录态和模型/网络访问时再显式运行：

```bash
CBTH_RUN_LIVE_CODEX_E2E=1 cargo test --test live_smoke -- --ignored
```

## Rust CLI Usage

The Rust CLI currently provides read-only store inspection commands, daemon-routed mutating job/batch/maintenance commands, `cbth cli run`, adapter-internal attempt and CLI-session commands, an audit log, and a daemon control surface backed by a same-user Unix socket.

`cbth cli run --bind-thread-id <thread-id>` attach-or-creates a durable fixed-thread managed session, asks the daemon to launch a loopback-only `codex app-server --listen ws://127.0.0.1:0`, refreshes a short app-server lease while foreground Codex runs, launches native `codex --remote <url> --cd <cwd> ...`, and starts a sidecar websocket client that performs initialize / `thread/resume` / `thread/read` and maps `turn/started`, `turn/completed`, and `thread/status/changed` into durable activity updates. Successful `thread.status.type` snapshots for the bound thread dominate earlier request-window notifications, and capability/activity write failures invalidate proof before retry. On foreground exit it stops and joins the sidecar, clears activity/capability proof with the latest sidecar epoch, and stops the daemon-owned app-server.

Default `cbth cli run` remains passive and records only `thread_resume` / `current_state_sync` capability proof. With explicit `--auto-delivery-policy trusted-all`, the sidecar records full automatic-delivery capability for the current epoch, polls durable idle state every 2 seconds, writes an `accept_pending` barrier, sends a unique-marker `turn/start`, calls `accept-cli` with the returned `turn.id`, and observes the accepted turn. Matching completed notifications close the batch as `delivered`; missed notifications can be reconciled with same-epoch `thread/read(includeTurns=true)`; failed/interrupted/replaced turns move the batch to `manual_resolution_only`. Clear pre-accept rejection uses hidden `attempt reject-cli-before-accept` and keeps the batch automatic without incrementing `delivery_attempt_count`. Timeout, websocket close, or protocol ambiguity is not retried; stale sweep later marks the attempt `unknown` and fail-closes the batch.

`trusted-all` is deliberately broad: it bypasses batch policy, artifact-read, and managed-session risk-profile gates, but it still requires an open head batch, remaining budget, matching `source_thread_id`, bound `managed_session_id`, matching `session_epoch`, and fresh idle proof. `cbth audit list` exposes the append-only decision trail for allow/deny/attempt-start/accepted/rejected/reconciled/observed/manualized records. `turn/steer`, active-turn injection, and `--new-thread` are still outside the implemented automatic delivery path.

State lives under `~/.cbth` by default. Use `--home <path>` or `CBTH_HOME` for tests and isolated runs.
The local-store and daemon IPC semantics are supported on macOS and Linux; pure Windows support is out of scope until the IPC, atomic-replace, and directory-sync contracts are designed separately.
By default, `job submit`, `job complete`, `job fail`, `batch close-head`, and `maintenance sweep` first ensure the local daemon is running, then execute through its same-user Unix socket. Read-only commands such as `job inspect`, `job list`, `batch inspect-head`, and `batch inspect` read the local store directly.
Use `--auto-daemon-startup-timeout-seconds <seconds>` on routed mutating commands when a large startup sweep or slow disk needs more than the default 5 seconds.

```bash
cargo run --bin cbth -- \
  job submit \
  --source-thread-id <thread-id> \
  --summary "wait for CI" \
  --delivery-read-only true \
  --delivery-requires-approval false \
  --delivery-requires-network false \
  --delivery-requires-write-access false

cargo run --bin cbth -- \
  job complete \
  --job-id <job-id> \
  --result-file ./result.txt \
  --summary "CI passed"

cargo run --bin cbth -- \
  batch inspect-head \
  --source-thread-id <thread-id>

cargo run --bin cbth -- \
  batch close-head \
  --source-thread-id <thread-id> \
  --reason operator-closed-unconfirmed

cargo run --bin cbth -- \
  cli run \
  --bind-thread-id <thread-id> \
  --session-allows-approval false \
  --session-allows-network false \
  --session-allows-write-access false \
  -- --model gpt-5.5

cargo run --bin cbth -- \
  cli run \
  --bind-thread-id <thread-id> \
  --session-allows-approval false \
  --session-allows-network false \
  --session-allows-write-access false \
  --auto-delivery-policy trusted-all \
  -- --model gpt-5.5

cargo run --bin cbth -- \
  audit list \
  --source-thread-id <thread-id>
```

If delivery policy is omitted, the core records a fail-closed policy: not read-only, requires approval, requires network, and requires write access. Completed result files are streamed into the managed artifact store; the original `--result-file` path is only an input. The current core does not persist inline handoff payloads yet, so completed job batches conservatively keep `requires_artifact_read=true` even for small artifacts.

Daemon control commands:

```bash
cargo run --bin cbth -- daemon ensure
cargo run --bin cbth -- daemon status
cargo run --bin cbth -- daemon ping
cargo run --bin cbth -- daemon stop
```

`daemon ensure` starts `cbth daemon serve` on demand when no active daemon is reachable. The daemon listens on `~/.cbth/run/cbth.sock`, requires private `~/.cbth` / `run` directories, validates peer uid before serving requests, runs a startup maintenance sweep, and exits after an idle timeout. Mutating CLI commands use this IPC path by default and fail closed if the same-user socket proof cannot be established.

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

1. Add thread-scoped inbox snapshots and delivery adapter helpers on top of the existing daemon-routed batch/artifact model.
2. Add thin CLI and Desktop integration entrypoints.
3. Keep small reference probes for one-off Desktop or protocol experiments.

Rust is the preferred implementation language for the real sidecar because it keeps resource usage low and is a better fit for cross-platform deployment anywhere Codex itself can run.

## License

This project is licensed under Apache License 2.0. See [LICENSE](LICENSE).
