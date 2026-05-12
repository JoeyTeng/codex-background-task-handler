# CLI Dogfood V1 Completion Plan

## Summary

CLI Dogfood V1 的目标是把当前 CLI 端收敛到本机可稳定自用，而不是做成多用户服务器产品。第一版继续保持原生 `codex --remote` 前台交互模型：`cbth` 只负责固定 thread bootstrap、daemon-owned shared app-server、idle-time `turn/start` 自动投递、后台任务监督和 operator recovery。

Desktop bridge、Socket/API 插件协议、Homebrew/Tap 发布、规则/allowlist/model policy engine、以及 active-turn `turn/steer` 自动投递都不进入 Dogfood V1。

## Delivery Boundary

- **Phase 12 landing**：先合入 `cbth cli run --new-thread`，保留 `--bind-thread-id` / `--new-thread` 二选一，成功后只向 stderr 打印 `cbth: bound thread id: <thread-id>`，不注入 foreground 输入。
- **Task supervisor**：新增 daemon-owned `cbth task run`，让用户可以提交 shell command 作为 background task；daemon 创建 job、监督 child/process group，并在退出后自动 `job complete` 或 `job fail`。
- **Diagnostics and recovery**：`cbth doctor cli` 是本机 readiness check；它覆盖 codex binary、app-server listener parsing、same-user daemon IPC、store permissions、SQLite open、platform support 和 live smoke prerequisites，并配套 operator recovery 文档。
- **Local binary deploy**：支持 `cargo install --path .` 或 release binary 的本机安装说明、PATH 检查、`cbth doctor cli` 和最小 dogfood walkthrough；不做包管理器。
- **Active steer design only**：`CLI_ACTIVE_TURN_STEER_DESIGN.md` 文档化 `turn/steer` 未来需要的 risk/capability proof；当前自动投递仍只允许 durable idle proof 后的 `turn/start`。

## Task Supervisor Contract

The detailed supervisor design is tracked in [CLI_TASK_SUPERVISOR_DESIGN.md](../design/CLI_TASK_SUPERVISOR_DESIGN.md). This section records the stable Dogfood V1 contract for the PR sequence.

用户入口：

```text
cbth task run \
  --source-thread-id <thread-id> \
  --summary <text> \
  [--delivery-read-only <bool>] \
  [--delivery-requires-approval <bool>] \
  [--delivery-requires-network <bool>] \
  [--delivery-requires-write-access <bool>] \
  [--cwd <dir>] \
  [--timeout-seconds <n>] \
  -- <cmd> [args...]
```

Minimum operator surface:

```text
cbth task inspect --task-id <task-id>
cbth task list [--source-thread-id <thread-id>]
cbth task cancel --task-id <task-id>
```

Daemon behavior:

- `task run` routes through same-user daemon IPC, creates a durable supervised task and an associated job, spawns the command, then returns `task_id`, `job_id`, and `source_thread_id` immediately.
- The daemon owns the child process group. Active supervised tasks block daemon idle exit.
- stdout and stderr are streamed to managed task log artifacts. The daemon keeps bounded per-stream tails for summaries and must not hold full output in memory.
- On exit code 0, the daemon writes a concise result summary and marks the job complete. On non-zero exit, signal, timeout, spawn failure, output spool failure, or cancel, it marks the job failed.
- The prompt delivered to Codex must include command metadata, exit status, tail summary, truncation flags, and artifact refs. It must not inline large logs.
- Cancel first sends SIGTERM to the process group, waits 5 seconds, then sends SIGKILL if needed.

Resource defaults:

- Per-stream summary tail: 64 KiB.
- Per-stream spool limit: 64 MiB, with `truncated=true` when exceeded.
- Default timeout: none unless user provides `--timeout-seconds`.
- Completed task logs follow the existing managed artifact retention / GC policy.

## PR Sequence

1. **Phase 12 PR**: push `codex/phase-12-cli-new-thread`, create PR, wait for Rust CI / fake e2e / `codex/review-gate`, fix review comments if any, squash merge with the required Codex co-author footer.
2. **Supervisor PR**: implement daemon-owned `cbth task run/list/inspect/cancel`, task state, bounded log spool/tails, child cleanup, daemon idle integration, fake e2e, and opt-in live task-supervisor e2e.
3. **Diagnostics/deploy PR**: implement `cbth doctor cli`, recovery docs for tasks/batches/audit/manual-resolution states, local binary install, PATH verification, dogfood walkthrough, and live retest commands.
4. **Optional packaging PR**: only after dogfood stability, decide whether release binaries or package-manager docs are worth adding.
5. **Steer design PR**: record the future active-turn `turn/steer` risk/capability contract in [CLI_ACTIVE_TURN_STEER_DESIGN.md](../design/CLI_ACTIVE_TURN_STEER_DESIGN.md) without enabling automatic steer.

## Test Plan

Default gate:

```text
cargo fmt --all -- --check
cargo clippy --locked --all-targets -- -D warnings
cargo test --locked
cargo test
```

Fake e2e coverage to add with the supervisor PR:

- Successful `task run` completes the associated job, then existing-thread and new-thread `trusted-all` paths close the head batch as `delivered`.
- Non-zero exit, signal kill, timeout, cancel, and spawn failure fail the job and produce auditable task artifacts.
- Large stdout/stderr are spooled with bounded memory and truncation metadata.
- Active supervised tasks prevent daemon idle exit; completed tasks allow normal idle exit.
- Daemon restart/recovery fails closed for orphaned running tasks that can no longer be supervised.

Opt-in live coverage:

```text
CBTH_RUN_LIVE_TASK_SUPERVISOR_E2E=1 cargo test --test live_task_supervisor -- --ignored --nocapture
```

The live test should start real `cbth cli run --new-thread --auto-delivery-policy trusted-all`, submit a real `cbth task run`, then verify batch `delivered`, audit records, and task log artifacts.

## Assumptions

- CLI Dogfood V1 is supported for macOS/Linux dedicated single-user workstations.
- `trusted-all` remains an explicit broad escape hatch; default `cbth cli run` remains passive.
- `strict_safe` policy automation, rule/allowlist/model engines, public Socket/API/plugin protocols, Desktop bridge, Homebrew/Tap packaging, pure Windows support, and active-turn automatic `turn/steer` remain out of scope.
- Production Rust code must prioritize correctness, long-running reliability, and low idle CPU/memory. Supervisor implementation must use durable checkpoints, bounded buffers, explicit process cleanup, and fail-closed recovery.
