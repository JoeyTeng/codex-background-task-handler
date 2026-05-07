# codex-background-task-handler

Reference experiments and companion tooling for making long-running background work practical around Codex without modifying the upstream `codex` repository.

## Status

Foundational experiments are validated, but end-to-end v1 continuation is still gated by unfinished contract tightening and capability validation:

- Desktop enabling experiments validated: automation-based bridge/retargeting experiments work. The current first-version delivery contract is built around a bound caller-heartbeat plus a delivery-envelope abstraction: bridge-side reads prefer `direct_file_read`, but every bridge wake must first run the narrow `cbth desktop bridge-preflight ...` helper to start the daemon if needed, sweep overdue work, and atomically publish a ready/reconcile snapshot manifest/revision, with each referenced file checked against that revision. That automatic pre-boundary file path is intentionally limited to ready/reconcile metadata and prompt tokens rather than payload/artifact bytes. Caller-side automatic continuation must cross the continuation boundary through a single-shot `note-boundary-crossed` before receiving any continuation content. Under the tightened v1 contract, a fresh `note-boundary-crossed` success authorizes only a bounded inline text handoff and immediately closes the batch as `close_reason=handoff_recorded`, which releases FIFO but does not prove the caller assistant response became visible. Large-artifact continuation and post-boundary ordinary tool use are left to operator/manual follow-up rather than the automatic path. Installation-wide capability conclusions now belong to `desktop_installation_state`, are scoped by transport generation, and are further bound to a validation fingerprint so Codex/Desktop or helper-environment drift invalidates older `validated` conclusions. Lost post-boundary responses are recovered by batch id through operator tooling rather than automatic replay. These are still design-level contracts plus enabling PoCs, not end-to-end validated delivery.
- CLI protocol/TUI continuation foundations validated: the shared `app-server` route supports multi-client continuation on the same thread, and foreground TUI visibility is proven while the user manually keeps the foreground on the same caller thread used in the PTY validation. The current implementation has durable CLI delivery-attempt state for the accept-pending and accepted-observation slice, durable daemon-owned CLI managed-session records keyed by `managed_session_id` / `bound_thread_id`, startup permission snapshots with current effective risk-profile fields, and fail-closed `begin-cli-accept` validation. `cbth cli run --bind-thread-id <thread_id>` now binds a managed session, asks the daemon to own a loopback `codex app-server`, launches foreground `codex --remote`, and starts a sidecar client that keeps durable `active` / `idle` state current from initialize / `thread/resume` / `thread/read` plus lifecycle notifications. `cbth resume <thread_id>` reuses that managed path while launching foreground `codex resume <thread_id> --remote <url> ...`; it forwards an explicit `--cd` when provided and otherwise preserves native resume working-directory choice instead of silently forcing the caller cwd. By default both entrypoints remain passive and send no delivery RPC. With explicit `--auto-delivery-policy trusted-all`, the sidecar records full automatic-delivery capability for the current epoch, waits for durable idle proof, writes an accept-pending barrier and audit records, sends one marked `turn/start` with pinned effective permissions when auto permission derivation is active, accepts the returned `turn.id`, and closes delivered from matching completed notification or same-epoch `thread/read` reconcile. Clear pre-accept rejection leaves the batch retryable without charging an attempt; timeout/closed/protocol ambiguity is not retried and is left for stale sweep to mark `unknown + manual_resolution_only`. Bootstrap remains designed as two explicit modes: existing-thread mode via `cbth cli run --bind-thread-id <thread_id>` or `cbth resume <thread_id>`, and conditional fresh-thread mode via `cbth cli run --new-thread` when `thread/start` is available. That bind controls only the delivery target; v1 does not prove or enforce current foreground focus, and live TUI visibility outside the validated same-thread case is not guaranteed. `trusted-all` is an explicit broad escape hatch for dedicated single-user workstations and bypasses batch policy, artifact-read, and managed-session risk-profile gates while still requiring an open head batch, budget, matching thread/session/epoch, and fresh idle proof. The route depends on capability probing around the experimental RPC surface; the upstream shared `app-server` side is currently unauthenticated loopback-only, while `cbth`'s own daemon IPC is scoped to a same-user Unix socket. v1 should still be treated as supported only on dedicated single-user workstations until stronger upstream local auth exists. `turn/steer` remains out of scope for automatic delivery, and automatic thread rebind/switch routing is intentionally out of scope for v1.

The shared architecture is converging on one Rust binary with thin integration entrypoints:

- a CLI entrypoint that launches the shared `app-server` path
- a Desktop-facing helper/bridge entrypoint
- a shared local daemon, store, artifact manager, and job/task-control CLI surface underneath

Design docs:

- [docs/SHARED_CORE_ARCHITECTURE.md](docs/SHARED_CORE_ARCHITECTURE.md)
- [docs/DESKTOP_BACKGROUND_TASK_BRIDGE_DESIGN.md](docs/DESKTOP_BACKGROUND_TASK_BRIDGE_DESIGN.md)
- [docs/CLI_SHARED_APP_SERVER_SIDECAR_DESIGN.md](docs/CLI_SHARED_APP_SERVER_SIDECAR_DESIGN.md)
- [docs/CLI_ACTIVE_TURN_STEER_DESIGN.md](docs/CLI_ACTIVE_TURN_STEER_DESIGN.md)
- [docs/CLI_OPERATOR_RECOVERY.md](docs/CLI_OPERATOR_RECOVERY.md)

## Project Journal

Current repo state and cross-task backlog stay in the short entrypoints [docs/PROJECT_STATE.md](docs/PROJECT_STATE.md) and [docs/PROJECT_TODO.md](docs/PROJECT_TODO.md). Detailed project journal records live under [docs/project_journal/](docs/project_journal/).

The migration journal includes:

- [current follow-ups](docs/project_journal/2026/05/2026-05-05-current-follow-ups-bbe4003.md) for the active implementation backlog
- [completed work archive](docs/project_journal/2026/05/2026-05-05-completed-work-archive-bbe4003.md) for completed tracker themes and current navigation links
- [legacy tracker snapshot](docs/project_journal/2026/05/2026-05-05-legacy-tracker-snapshot-bbe4003.md) for the exact pre-migration `PROJECT_STATE` and `PROJECT_TODO` text

The legacy snapshot is intentionally a verbatim archive. Relative links inside its fenced copied tracker text remain in their original historical form and are not expected to resolve from the journal directory; use the entrypoint docs and archive summaries for live navigation.
For merge-time project-journal bookkeeping rules, see [docs/README.md](docs/README.md).

## Repository Layout

- `docs/`
  - architecture notes
  - experiment logs
  - project journal entrypoints and per-workstream notes
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

## Release Install

CLI dogfood v1 is intended for macOS/Linux dedicated single-user workstations.
The supported release assets are currently Linux x86_64 glibc and macOS arm64, including Apple Silicon hosts launched from a Rosetta shell.

Install the latest GitHub Release:

```bash
curl -fsSL https://raw.githubusercontent.com/JoeyTeng/codex-background-task-handler/HEAD/scripts/install.sh | sh
command -v cbth
cbth doctor cli
```

The install script downloads the Rust release-profile binary from GitHub Releases, verifies the matching `.sha256`, and installs that binary; it does not build from source.

Install a specific release or custom directory:

```bash
CBTH_VERSION=v0.1.2 CBTH_INSTALL_DIR="$HOME/.local/bin" \
  sh scripts/install.sh
```

Upgrade an installed binary:

```bash
cbth self update --check
cbth self update --yes
```

`cbth self update --yes` downloads the matching GitHub Release binary and `.sha256`, verifies the checksum, writes a temporary file next to the current executable, and atomically replaces it. It does not use `sudo`; if the current executable is not writable, reinstall into a user-writable directory.

## Local Development Install

Install the local binary from a checkout:

```bash
cargo install --path .
command -v cbth
cbth --help
cbth doctor cli
```

`cbth doctor cli` is a readiness check, not a read-only inspection command. It may create or repair private `~/.cbth` state directories, open the SQLite store, start the same-user daemon, and briefly start a loopback `codex app-server` to verify listener parsing. It does not send a model request, create a Codex turn, or change the foreground Codex interaction model. Use `--codex-bin <path>` when testing a non-default Codex CLI binary:

```bash
cbth doctor cli --codex-bin /path/to/codex
```

For isolated testing, set `CBTH_HOME` or pass `--home <path>`:

```bash
CBTH_HOME=/tmp/cbth-dogfood cbth doctor cli
```

## Testing

默认 CI gate 保持单一 Rust matrix：Ubuntu 与 macOS 都运行 `cargo fmt --all -- --check`、`cargo clippy --locked --all-targets -- -D warnings`、`cargo test --locked`。Deterministic fake e2e 覆盖在 Rust integration tests 中，因此会随 `cargo test --locked` 自动进入双平台 gate。

真实 Codex shared `app-server` smoke 是 opt-in，不在默认 CI 执行；它只保持编译和 lint-clean。需要本机真实 `codex`、Node、登录态和模型/网络访问时再显式运行：

```bash
CBTH_RUN_LIVE_CODEX_E2E=1 cargo test --test live_smoke -- --ignored
```

完整 live `trusted-all` 自动投递 e2e 也是 opt-in；它会创建真实 Codex thread、运行 seed turn，并验证 `cbth cli run --auto-delivery-policy trusted-all` 能把 head batch 自动投递后关闭为 `delivered`：

```bash
CBTH_RUN_LIVE_TRUSTED_ALL_E2E=1 cargo test --test live_trusted_all -- --ignored
```

完整 live `--new-thread` e2e 同样是 opt-in；它用真实 `codex app-server` 在启动前创建 caller thread，通过 stderr 暴露新 thread id，然后验证 trusted-all 自动投递闭环：

```bash
CBTH_RUN_LIVE_NEW_THREAD_E2E=1 cargo test --test live_new_thread -- --ignored
```

完整 live task-supervisor e2e 也是 opt-in；它用真实 `cbth cli run --new-thread --auto-delivery-policy trusted-all` 和真实 daemon-owned `cbth task run` 验证后台命令完成后自动投递为 `delivered`：

```bash
CBTH_RUN_LIVE_TASK_SUPERVISOR_E2E=1 cargo test --test live_task_supervisor -- --ignored --nocapture
```

复测步骤、环境变量和失败排查见 [docs/LIVE_E2E.md](docs/LIVE_E2E.md)。

## Rust CLI Usage

The Rust CLI currently provides read-only store inspection commands, daemon-routed mutating job/batch/task/maintenance commands, `cbth cli run`, operator-facing CLI session recovery commands, adapter-internal attempt commands, an audit log, explicit `cbth self update`, and a daemon control surface backed by a same-user Unix socket.

Run the deployment readiness check before dogfooding a fresh install:

```bash
cargo run --bin cbth -- doctor cli
```

`cbth cli run --bind-thread-id <thread-id>` attach-or-creates a durable fixed-thread managed session, asks the daemon to launch a loopback-only `codex app-server --listen ws://127.0.0.1:0`, refreshes a short app-server lease while foreground Codex runs, launches native `codex --remote <url> --cd <cwd> ...`, and starts a sidecar websocket client that performs initialize / `thread/resume` / `thread/read` and maps `turn/started`, `turn/completed`, and `thread/status/changed` into durable activity updates. `cbth resume <thread-id> [-- <codex_args>]` is the existing-thread convenience entrypoint for native resume; it uses the same managed app-server and sidecar flow, but launches foreground Codex as `codex resume <thread-id> --remote <url> ...`. Explicit forwarded `--cd` / `-C` values are normalized against the caller cwd and are passed once to foreground Codex plus the sidecar's initial `thread/resume`; when no cwd override is provided, non-interactive runs omit `--cd` and preserve the thread's native resume cwd behavior, while interactive terminals first read the prior thread cwd and prompt between that directory and the current directory before the sidecar materializes startup state. For that native resume path, the sidecar's first `thread/resume` carries the foreground overrides it can derive from argv (`cwd` when explicitly chosen, `model`, `profile`, `approvalsReviewer`, and `persistExtendedHistory`) so a cold thread cannot be materialized by the sidecar before foreground resume settings are applied; forwarded Codex options are rejected after the resume prompt because the sidecar cannot safely mirror post-positional option parsing; forwarded `--remote` / `--remote-auth-token-env` overrides are rejected because `cbth` owns the managed app-server transport, forwarded `--add-dir` is rejected because current Codex `thread/resume` cannot faithfully carry additional writable roots, forwarded `--sandbox` / `--ask-for-approval` / `--full-auto` / danger-bypass permission overrides are rejected so managed resume permissions come only from the startup snapshot, forwarded `--search` / web-search feature enablement is rejected because the initial resume snapshot cannot otherwise reflect the foreground network-capable tool, and forwarded `--config` / `-c` sandbox/permission-scope overrides such as `approval_policy`, `sandbox_mode`, `sandbox_workspace_write.*`, `sandbox_read_only.*`, `sandbox_permissions.*`, `permissions.*`, `default_permissions`, `profiles.*`, `projects.*`, `trust_level`, `features` / `features.web_search*`, `web_search*`, `tools` / `tools.web_search*`, `writable_roots`, `readable_roots`, and `network_access` are rejected until they can be carried exactly into the initial `thread/resume`. Later permission refreshes use plain current-state `thread/resume`. `cbth cli run --new-thread` asks the daemon to start a pending app-server, calls `thread/start` on that same process, prints `cbth: bound thread id: <thread-id>` to stderr, then promotes the pending process into the fixed-thread managed session before foreground Codex starts. This avoids losing Codex's fresh unmaterialized thread state while still not sending a foreground prompt, injecting user input, or changing the native `codex --remote` interaction model. Successful `thread.status.type` snapshots for the bound thread dominate earlier request-window notifications, and capability/activity/permission write failures invalidate proof before retry. On foreground exit it stops and joins the sidecar, clears activity/capability/permission proof with the latest sidecar epoch, stops the daemon-owned app-server, and marks the managed session `detached` rather than leaving a misleading `live` record.

Default `cbth cli run` remains passive and records only current-state proof. Existing-thread sessions use `thread_resume` / `current_state_sync`; fresh unmaterialized `--new-thread` sessions can use the same-process `thread_start` + `thread/read(includeTurns=false)` proof until the first turn materializes rollout storage. With explicit `--auto-delivery-policy trusted-all`, the sidecar records full automatic-delivery capability for the current epoch, polls durable idle state every 2 seconds, writes an `accept_pending` barrier, sends a unique-marker `turn/start`, waits up to 60 seconds for acceptance, calls `accept-cli` with the returned `turn.id`, and observes the accepted turn. Matching completed notifications close the batch as `delivered`; missed notifications can be reconciled with same-epoch `thread/read(includeTurns=true)` after the accepted turn materializes the rollout; failed/interrupted/replaced turns move the batch to `manual_resolution_only`. Clear pre-accept rejection uses hidden `attempt reject-cli-before-accept` and keeps the batch automatic without incrementing `delivery_attempt_count`. Timeout, websocket close, or protocol ambiguity is not retried; stale sweep later marks the attempt `unknown` and fail-closes the batch.

`--session-allows-approval`, `--session-allows-network`, and `--session-allows-write-access` accept `auto`, `true`, or `false`; all three default to `auto`. In auto mode, the sidecar derives permissions from `thread/resume`, preferring canonical `permissionProfile` when present and falling back to `approvalPolicy` plus legacy `sandbox` when the profile is absent. If both canonical and legacy shapes are present but disagree on derived network/write permissions, the snapshot is treated as untrusted and automatic delivery fails closed. The first trusted snapshot is pinned as `startup_permission_snapshot`. Before each automatic `turn/start`, the sidecar refreshes the current snapshot and computes `effective_allows = startup_allows && current_allows` per dimension. Current tightening is honored, current loosening is capped by the startup snapshot, and mixed changes are handled per dimension. The pinned legacy `sandboxPolicy` sent to `turn/start` carries only protocol-accepted fields (`type`, `networkAccess`, `writableRoots`, and workspace exclude flags); restricted read shapes from `access` / `readOnlyAccess` and canonical `permissionProfile` details are parsed and compared for drift/audit, but exact profile pinning is not emitted yet because Codex 0.128 exposes canonical profiles as read-side state while `turn/start` still accepts the legacy override fields. Canonical deny scopes, canonical read scopes that cannot be represented by the legacy pin, and canonical read scopes narrower than legacy readable roots fail closed because the legacy pin cannot represent those nested rules exactly. Workspace writable roots are normalized before containment checks, and roots containing parent-directory components fail closed. Drift detection covers derived booleans, raw `approvalPolicy` / `sandbox`, and canonical `permissionProfile` details when available; any drift writes a stderr warning and an audit record with startup/current/effective snapshots, direction, and changed dimensions. Missing or unknown permission shapes fail closed for automatic delivery, but passive current-state proof can still continue without a permission snapshot so older app-server responses do not break attach visibility. Proof invalidation and post-turn resync clear epoch-local current proof but preserve the startup cap for the same foreground managed session; auto-pinned strict-safe delivery also requires a refreshed current permission snapshot before it can use the recorded risk booleans again. Fresh unmaterialized `--new-thread` sessions may record passive activity/capability proof before the first permission snapshot, but automatic delivery remains disabled until a trusted snapshot is available. Default `auto` bind is not treated as a fixed `false` profile when reattaching to an existing managed session, so a previous auto-derived effective profile can be reused without causing profile-drift replacement; explicit `true` / `false` values still override auto derivation for that dimension and are still enforced during profile-drift checks when the other dimensions remain `auto`.

Managed CLI startup and `cbth doctor cli` also perform a soft Codex CLI compatibility check. `cbth` is currently validated against `codex-cli 0.128.x`; newer or unparsable versions produce a warning with diagnostic details, but execution continues and still relies on fail-closed protocol parsing for required fields.

`trusted-all` is deliberately broad: it bypasses batch policy, artifact-read, and managed-session risk-profile gates, but it still requires an open head batch, remaining budget, matching `source_thread_id`, bound `managed_session_id`, matching `session_epoch`, and fresh idle proof. `cbth audit list` exposes the append-only decision trail for allow/deny/attempt-start/accepted/rejected/reconciled/observed/manualized records. `turn/steer`, active-turn injection, rollout-only delivery proof, and foreground thread retargeting are still outside the implemented automatic delivery path; the future steer contract is documented in [docs/CLI_ACTIVE_TURN_STEER_DESIGN.md](docs/CLI_ACTIVE_TURN_STEER_DESIGN.md).

Use `cbth cli session list`, `cbth cli session inspect`, and `cbth cli session retire` for managed-session recovery. Operator retirement refuses `live` sessions, sessions that still own active delivery attempts, and sessions whose bound thread still has an open `manual_resolution_only` head batch. `detached`, `parked`, or `stale` sessions with no blockers can be retired manually; `cli run` can also auto-retire and replace retire-eligible `parked` / `stale` / profile-drift records. Same-profile reattach refuses the same active-attempt/manual-head blockers. A fail-closed accepted or pre-accept delivery path parks the session until the manual head batch is closed or swept.

Desktop bridge foundation commands are implemented as operator/helper surfaces, not as an enabled Desktop automatic delivery path. `cbth desktop installation-state` owns installation-wide Desktop read/write capability state, `cbth desktop binding repair` durably binds a Desktop source thread to a caller heartbeat automation id, and `cbth desktop bridge-preflight` publishes a stable `~/.cbth/inbox/current-snapshot.json` manifest that points at revision-specific inbox snapshot files under `~/.cbth/inbox/snapshots/<revision>/`, including the installation-state export for that revision. It also exports latest-only `~/.cbth/inbox/desktop-installation-state.json` for convenience, but no-DB readers use the revision-specific export referenced by the manifest. Default preflight remains daemon-routed; `--require-existing-daemon` avoids daemon autostart but still uses the same-user Unix socket, while `--helper-direct-store` bypasses daemon autostart, `startup.lock`, and socket IPC but still opens SQLite to publish a fresh snapshot. Desktop heartbeat validation now prefers no-DB read helpers such as `read-snapshot`, `list-arm-pending`, `list-pause-due`, and `claim-next-ready`; those helpers only read already-published inbox JSON and do not open SQLite, connect the daemon, or write files. `note-arm-pending` and `note-arm` now provide durable Desktop writeback primitives for existing prepared attempts, and preflight can export real arm-pending / pause-due entries, but ready attempt materialization, caller wake, `note-boundary-crossed`, artifact reads, and automatic Desktop delivery remain future work. See [docs/DESKTOP_BRIDGE_FOUNDATION.md](docs/DESKTOP_BRIDGE_FOUNDATION.md), [docs/DESKTOP_LIVE_PREFLIGHT_VALIDATION.md](docs/DESKTOP_LIVE_PREFLIGHT_VALIDATION.md), and [docs/DESKTOP_WRITEBACK_HELPER_LIVE_VALIDATION.md](docs/DESKTOP_WRITEBACK_HELPER_LIVE_VALIDATION.md).

```bash
cargo run --bin cbth -- \
  desktop installation-state --json

cargo run --bin cbth -- \
  desktop installation-state repair \
  --read-transport direct-file-read \
  --read-transport-capability unknown \
  --artifact-read-capability unknown \
  --writeback-capability unknown \
  --json

cargo run --bin cbth -- \
  desktop binding repair \
  --source-thread-id <thread-id> \
  --caller-automation-id <automation-id> \
  --json

cargo run --bin cbth -- \
  desktop bridge-preflight \
  --bridge-thread-id <thread-id> \
  --json

cargo run --bin cbth -- \
  desktop bridge-preflight \
  --bridge-thread-id <thread-id> \
  --helper-direct-store \
  --json

cargo run --bin cbth -- \
  desktop read-snapshot \
  --bridge-thread-id <thread-id> \
  --json

cargo run --bin cbth -- \
  desktop list-arm-pending \
  --bridge-thread-id <thread-id> \
  --json

cargo run --bin cbth -- \
  desktop list-pause-due \
  --bridge-thread-id <thread-id> \
  --json

cargo run --bin cbth -- \
  desktop claim-next-ready \
  --bridge-thread-id <thread-id> \
  --json

cargo run --bin cbth -- \
  desktop note-arm-pending \
  --source-thread-id <thread-id> \
  --attempt-id <attempt-id> \
  --generation <generation> \
  --bridge-request-id <request-id> \
  --json

cargo run --bin cbth -- \
  desktop note-arm \
  --source-thread-id <thread-id> \
  --attempt-id <attempt-id> \
  --generation <generation> \
  --bridge-request-id <request-id> \
  --bridge-arm-lease-id <lease-id> \
  --json
```

State lives under `~/.cbth` by default. Use `--home <path>` or `CBTH_HOME` for tests and isolated runs.
The local-store and daemon IPC semantics are supported on macOS and Linux; pure Windows support is out of scope until the IPC, atomic-replace, and directory-sync contracts are designed separately.
By default, `job submit`, `job complete`, `job fail`, `batch close-head`, and `maintenance sweep` first ensure the local daemon is running, then execute through its same-user Unix socket. Read-only commands such as `job inspect`, `job list`, `batch inspect-head`, and `batch inspect` read the local store directly.
Use `--auto-daemon-startup-timeout-seconds <seconds>` on routed mutating commands when a large startup sweep or slow disk needs more than the default 5 seconds.

`cbth task run` is daemon-owned background supervision for local commands. It creates a durable task plus associated job, spawns the command in its own process group using the caller's environment, returns immediately with `task_id` / `job_id`, and lets the daemon complete or fail the job when the command exits. stdout and stderr are spooled to task log files under `~/.cbth/tasks/<task-id>/`, with bounded in-memory tails, 64 MiB per-stream spool caps, a 16 active-task daemon cap, and cleanup only after linked delivery batches have closed and their retention window has elapsed. The delivery prompt includes command metadata, exit status, byte/truncation flags, small tail previews, and managed artifact refs; large logs stay in the managed files/artifacts. `task cancel` records the cancel request and asks the daemon to terminate the task process group with SIGTERM, then SIGKILL after a grace window; daemon startup recovery validates a persisted process identity before killing a lost task process group.

```bash
cargo run --bin cbth -- \
  doctor cli

cargo run --bin cbth -- \
  self update --check

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
  -- --model gpt-5.5

cargo run --bin cbth -- \
  resume <thread-id> \
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
  cli run \
  --new-thread \
  --session-allows-approval false \
  --session-allows-network false \
  --session-allows-write-access false \
  --auto-delivery-policy trusted-all \
  -- --model gpt-5.5

cargo run --bin cbth -- \
  cli session list \
  --bound-thread-id <thread-id>

cargo run --bin cbth -- \
  cli session inspect \
  --managed-session-id <managed-session-id>

cargo run --bin cbth -- \
  cli session retire \
  --managed-session-id <managed-session-id> \
  --reason "operator cleanup after manual recovery"

cargo run --bin cbth -- \
  audit list \
  --source-thread-id <thread-id>

cargo run --bin cbth -- \
  task run \
  --source-thread-id <thread-id> \
  --summary "run slow test suite" \
  --delivery-read-only true \
  --delivery-requires-approval false \
  --delivery-requires-network false \
  --delivery-requires-write-access false \
  --cwd "$PWD" \
  --timeout-seconds 3600 \
  -- cargo test

cargo run --bin cbth -- \
  task inspect \
  --task-id <task-id>

cargo run --bin cbth -- \
  task list \
  --source-thread-id <thread-id>

cargo run --bin cbth -- \
  task cancel \
  --task-id <task-id>
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

Operator recovery procedures for `manual_resolution_only`, head batch inspection, task logs, audit records, and manual close are documented in [docs/CLI_OPERATOR_RECOVERY.md](docs/CLI_OPERATOR_RECOVERY.md).

## Python Usage

Python is kept for small probes and reference scripts only. Run scripts in this repo with `uv`.

Example:

```bash
uv run python scripts/desktop_thread_inject_poc.py --thread-id <thread-id> --mode inject
```

The current PoC script is [scripts/desktop_thread_inject_poc.py](scripts/desktop_thread_inject_poc.py). It starts a standalone `codex app-server`, targets an existing thread, and records whether external injection or `turn/start` operations persist into the rollout.

The current CLI shared-server PoC is [scripts/cli_shared_app_server_poc.mjs](scripts/cli_shared_app_server_poc.mjs). It connects two websocket clients to the same shared `codex app-server`, seeds a frontend thread, then validates that a sidecar client can resume the same thread and that the frontend client receives the resulting live turn notifications.

In addition to the protocol-level PoC, the shared-server CLI route has also been validated against a real foreground TUI session running through `codex --remote`, confirming that the user-facing TUI output reflects the sidecar-triggered turn while the foreground stays on the same caller thread used in the PTY validation. That PTY validation matches the current loopback-only upstream surface available in `codex-cli 0.123.0`.

The current CLI active-turn steering PoC is [scripts/cli_turn_steer_poc.mjs](scripts/cli_turn_steer_poc.mjs). It starts a long-running turn, submits `turn/steer` from a second client while that turn is still active, and validates that the same turn completes normally instead of ending early. The PoC is narrow evidence for the future gated contract in [docs/CLI_ACTIVE_TURN_STEER_DESIGN.md](docs/CLI_ACTIVE_TURN_STEER_DESIGN.md); automatic steer remains disabled.

## Planned Implementation Direction

This repo is expected to grow around one shared Rust core with thin per-surface entrypoints:

1. Add thread-scoped inbox snapshots and delivery adapter helpers on top of the existing daemon-routed batch/artifact model.
2. Add thin CLI and Desktop integration entrypoints.
3. Keep small reference probes for one-off Desktop or protocol experiments.

Rust is the preferred implementation language for the real sidecar because it keeps resource usage low and is a better fit for cross-platform deployment anywhere Codex itself can run.

## License

This project is licensed under Apache License 2.0. See [LICENSE](LICENSE).
