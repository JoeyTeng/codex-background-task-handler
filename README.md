# codex-background-task-handler

Reference experiments and companion tooling for making long-running background work practical around Codex without modifying the upstream `codex` repository.

## Status

Foundational experiments are validated, but end-to-end v1 continuation is still gated by unfinished contract tightening and capability validation:

- Desktop enabling experiments validated: automation-based bridge/retargeting experiments work. The current first-version delivery contract is built around a bound caller-heartbeat plus a delivery-envelope abstraction: bridge-side reads prefer `direct_file_read`, but every bridge wake must first run the narrow `cbth desktop bridge-preflight ...` helper to start the daemon if needed, sweep overdue work, and atomically publish a ready/reconcile snapshot manifest/revision, with each referenced file checked against that revision. That automatic pre-boundary file path is intentionally limited to ready/reconcile metadata and prompt tokens rather than payload/artifact bytes. Caller-side automatic continuation must cross the continuation boundary through a single-shot `note-boundary-crossed` before receiving any continuation content. Under the tightened v1 contract, a fresh `note-boundary-crossed` success authorizes only a bounded inline text handoff and immediately closes the batch as `close_reason=handoff_recorded`, which releases FIFO but does not prove the caller assistant response became visible. Large-artifact continuation and post-boundary ordinary tool use are left to operator/manual follow-up rather than the automatic path. Installation-wide capability conclusions now belong to `desktop_installation_state`, are scoped by transport generation, and are further bound to a validation fingerprint so Codex/Desktop or helper-environment drift invalidates older `validated` conclusions. Lost post-boundary responses are recovered by batch id through operator tooling rather than automatic replay. These are still design-level contracts plus enabling PoCs, not end-to-end validated delivery.
- CLI protocol/TUI continuation foundations validated: the shared `app-server` route supports multi-client continuation on the same thread, and foreground TUI visibility is proven while the user manually keeps the foreground on the same caller thread used in the PTY validation. The current implementation has durable CLI delivery-attempt state for the accept-pending and accepted-observation slice, durable daemon-owned CLI managed-session records keyed by `managed_session_id` / `bound_thread_id`, immutable session-scoped risk-profile fields (`session_allows_approval`, `session_allows_network`, `session_allows_write_access`), and fail-closed `begin-cli-accept` validation that only allows matching live/detached no-approval / no-network / no-write sessions with an epoch-local minimum capability proof and idle current-state proof. It also expires missed observations into `manual_resolution_only`, keeps the daemon alive for active accept-pending / accepted-observation windows, reopens a bounded manual-resolution window when automatic CLI delivery fail-closes, advances the managed-session epoch whenever attach/recovery or unresolved active attempts invalidate prior activity and capability proof, and provides an adapter-internal CLI turn-observation surface that records terminal events, closes batches only on timely matching `turn_completed`, and fail-closes failed/interrupted/replaced or late observations to manual resolution. `cbth cli run --bind-thread-id <thread_id>` now binds a managed session, asks the daemon to own a loopback `codex app-server`, launches foreground `codex --remote`, and starts a passive sidecar client that performs initialize / `thread/resume` / `thread/read` plus lifecycle notification processing to keep durable `active` / `idle` state current. The broader first-version contract still needs full `turn_start` capability proof, automatic delivery, and accepted-turn observation wiring. Bootstrap remains designed as two explicit modes: existing-thread mode via `cbth cli run --bind-thread-id <thread_id>` and conditional fresh-thread mode via `cbth cli run --new-thread` when `thread/start` is available. That bind controls only the delivery target; v1 does not prove or enforce current foreground focus, and live TUI visibility outside the validated same-thread case is not guaranteed. Any attach/recovery path currently bumps `session_epoch` and resets `activity_state` to `unknown`; auto-delivery still must regain a current-state proof before treating the caller as idle. Detached auto-delivery is only allowed for managed sessions whose durable risk profile is explicitly no-approval / no-network / no-write, and accepted turn observation is only required to keep the daemon alive until a durable `delivery_observation_deadline` expires; sessions with a broader risk profile or expired observation windows fall back to manual/operator handling. The route depends on capability probing around the experimental RPC surface; the upstream shared `app-server` side is currently unauthenticated loopback-only, while `cbth`'s own daemon IPC is scoped to a same-user Unix socket. v1 should still be treated as supported only on dedicated single-user workstations until stronger upstream local auth exists. `turn/steer` remains a gated optimization instead of a default path, and automatic thread rebind/switch routing is intentionally out of scope for v1.

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

## Rust CLI Usage

The Rust CLI currently provides read-only store inspection commands, daemon-routed mutating job/batch/maintenance commands, a minimal `cbth cli run` foreground wrapper, hidden adapter-internal attempt and CLI-session commands, and a daemon control surface backed by a same-user Unix socket. `cbth cli run --bind-thread-id <thread-id>` attach-or-creates a durable fixed-thread managed session, asks the daemon to launch a loopback-only `codex app-server --listen ws://127.0.0.1:0`, refreshes a short app-server lease while foreground Codex runs, launches native `codex --remote <url> --cd <cwd> ...`, and starts a passive sidecar websocket client that performs initialize / `thread/resume` / `thread/read` and maps `turn/started`, `turn/completed`, and `thread/status/changed` into durable activity updates; successful `thread.status.type` snapshots for the bound thread dominate earlier request-window notifications, and capability/activity write failures invalidate proof before retry. On foreground exit it stops and joins the sidecar, clears activity/capability proof with the latest sidecar epoch, and stops the daemon-owned app-server. The daemon also fences the current durable proof for a registered `managed_session_id + bound_thread_id` before removing a managed app-server on explicit stop, dead-child refresh, or lease expiry; cleanup advances an epoch fence even when proof is already clear, and the registry's startup epoch is only an identity check, so sidecar epoch advancement cannot strand cleanup on an old epoch or rewrite proof after cleanup starts. Shutdown best-effort fences proof but still kills children to avoid process leaks. The hidden `cli session bind` / `cli session note-capabilities` / `cli session note-activity` / `cli session invalidate-proof` / `cli session inspect` commands remain adapter-internal scaffolding: `bind` attach-or-creates the durable fixed-thread managed session with explicit risk-profile flags, re-attach advances `session_epoch` and clears prior activity and capability proof, `note-capabilities` records epoch-local RPC/current-state/terminal-event capability proof, `note-activity` records the next sequential current-state proof such as `idle`, `invalidate-proof` fences continuity loss by advancing `session_epoch` and clearing old proof, and `inspect` reads that record back. `invalidate-proof` is idempotent when a previous attempt already advanced the epoch and cleared proof, so daemon timeout/direct-fallback races do not strand the adapter on an old epoch. The passive sidecar currently records only partial capability proof (`thread_resume` and `current_state_sync`), so it does not open automatic delivery by itself. The hidden `attempt begin-cli-accept` command requires the adapter to provide a stable `--rpc-request-id` and a previously bound idle managed session that has passed the minimum capability probe; idempotent begin/accept paths still require that the stored attempt references a current valid managed session and was created with recorded activity/capability proof. The hidden `attempt observe-cli-turn` command is the store-facing terminal-event surface for a future app-server adapter: it records matching turn events, closes the batch only for an on-time `turn_completed`, and fail-closes late or failed/interrupted/replaced observations to `manual_resolution_only`. Daemon compatibility also requires the `attempt-dispatch`, `cli-app-server-lifecycle`, `cli-session-dispatch`, `cli-session-capability-dispatch`, `cli-session-proof-invalidation-dispatch`, and `cli-turn-observation-dispatch` capabilities so upgraded CLIs do not hand new attempt/session/app-server lifecycle commands to an older daemon.

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
```

If delivery policy is omitted, the core records a fail-closed policy: not read-only, requires approval, requires network, and requires write access. Completed result files are streamed into the managed artifact store; the original `--result-file` path is only an input. The current core does not persist inline handoff payloads yet, so completed job batches conservatively keep `requires_artifact_read=true` even for small artifacts.

`cbth cli run` is currently a process-model bridge plus passive lifecycle observer, not a full automatic delivery adapter. It proves the daemon-owned app-server lifecycle, foreground `codex --remote` launch path, and passive current-state/event sync; full `turn/start` capability proof, accepted-turn observation wiring, and automatic `turn/start` / `turn/steer` delivery are still future phases.

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
