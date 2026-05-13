# 真实 E2E 指南

语言：[British English (en-GB)](LIVE_E2E.en-GB.md) | [简体中文 (zh-CN)](LIVE_E2E.zh-CN.md)

本文说明需要真实 Codex CLI 登录态、模型访问和网络访问的 opt-in live checks。默认 CI 不运行这些测试；它们用于本地验证和 dogfood。

## 环境

必要条件：

- 已安装并登录 `codex` CLI。
- 已安装 Node.js，用于 `scripts/cli_shared_app_server_poc.mjs`。
- 能访问网络和模型，用于真实 Codex turns。
- 本地 `cbth` checkout 和 Rust toolchain。

推荐 preflight：

```bash
codex --version
node --version
cargo --version
cargo run --bin cbth -- doctor cli
```

可选环境变量：

- `CBTH_LIVE_CODEX_BIN`: 覆盖真实 `codex` binary，默认 `codex`。
- `CBTH_LIVE_NODE_BIN`: 覆盖 Node binary，默认 `node`。
- `CBTH_LIVE_CODEX_E2E_TIMEOUT_MS`: shared app-server smoke timeout，默认 `180000`。
- `CBTH_LIVE_TRUSTED_ALL_E2E_TIMEOUT_SECONDS`: trusted-all full e2e timeout，默认 `360`。
- `CBTH_LIVE_NEW_THREAD_E2E_TIMEOUT_SECONDS`: `--new-thread` full e2e timeout，默认 `360`。
- `CBTH_LIVE_TASK_SUPERVISOR_E2E_TIMEOUT_SECONDS`: task-supervisor full e2e timeout，默认 `360`。

## Shared App-Server Smoke

这个 smoke 会启动真实 `codex app-server`，用 Node PoC 创建 frontend thread，再用 sidecar client 对同一 thread 发送 `turn/start`。它确认 frontend 能看到 started/completed notifications，并且 `thread/read` 能看到 marker。

```bash
CBTH_RUN_LIVE_CODEX_E2E=1 cargo test --test live_smoke -- --ignored
```

## Trusted-All Full Live E2E

这个测试验证 `cbth cli run --auto-delivery-policy trusted-all` 的自动投递：

- 通过 `codex app-server` 创建真实 caller thread。
- 启动 `cbth cli run --bind-thread-id <thread-id> --auto-delivery-policy trusted-all`。
- 等待 sidecar idle proof 和 automatic-delivery capability。
- 提交或 fail 一个 job，创建 head batch。
- 等待 sidecar `turn/start`、accepted turn observation 或 reconcile，以及 `close_reason=delivered`。
- 检查 audit records 中是否有 allow、attempt-start、accepted，以及 observed 或 reconciled events。

```bash
CBTH_RUN_LIVE_TRUSTED_ALL_E2E=1 cargo test --test live_trusted_all -- --ignored
```

## New-Thread Trusted-All Full Live E2E

这个测试验证 `cbth cli run --new-thread --auto-delivery-policy trusted-all` 的 fresh-thread bootstrap：

- `cbth` 启动 daemon-owned pending `codex app-server`。
- Foreground Codex 创建 thread。
- `cbth` 绑定发现到的 `thread/started` id，并打印到 stderr。
- 测试提交 marker job，并等待与 existing-thread trusted-all e2e 相同的 delivered close path。

```bash
CBTH_RUN_LIVE_NEW_THREAD_E2E=1 cargo test --test live_new_thread -- --ignored
```

## Task Supervisor Full Live E2E

这个测试验证真实 daemon-owned `cbth task run` 走完整 trusted-all 自动投递闭环：

- 启动 `cbth cli run --new-thread --auto-delivery-policy trusted-all`。
- 从 stderr 读取 bound thread id。
- 等待 idle proof 和 automatic-delivery capability。
- 通过 `cbth task run` 运行 daemon-supervised shell command。
- Daemon 完成 task、创建 result artifact 并创建 head batch。
- 等待自动投递，并验证 task logs、artifact payload 和 audit records。

```bash
CBTH_RUN_LIVE_TASK_SUPERVISOR_E2E=1 cargo test --test live_task_supervisor -- --ignored --nocapture
```

## Manual Dogfood Walkthrough

用 `cargo install --path .` 安装本地 binary 后，运行：

```bash
cbth doctor cli
```

启动一个受管理的 fresh thread：

```bash
cbth new \
  --session-allows-approval false \
  --session-allows-network false \
  --session-allows-write-access false \
  --auto-delivery-policy trusted-all \
  --model gpt-5.5
```

从 stderr 复制 bound thread id，然后在另一个 shell 提交 supervised task：

```bash
cbth task run \
  --source-thread-id <thread-id> \
  --summary "run a slow local check" \
  --delivery-read-only true \
  --delivery-requires-approval false \
  --delivery-requires-network false \
  --delivery-requires-write-access false \
  --cwd "$PWD" \
  --timeout-seconds 3600 \
  -- cargo test
```

如果 batch 没有自动关闭，使用 recovery surface：

```bash
cbth task list --source-thread-id <thread-id>
cbth batch inspect-head --source-thread-id <thread-id>
cbth audit list --source-thread-id <thread-id> --limit 100
```

## Failure Notes

- 如果 listener bootstrap 卡住，先运行 shared app-server smoke。它能快速暴露 `codex app-server` 输出流、登录态或 Node PoC 问题。
- 如果 `--new-thread` 一直没有打印 `cbth: bound thread id: ...`，检查当前 `codex app-server` 是否支持 startup path。
- 如果 session readiness 卡住，查看 test output 中最后观察到的 session proof fields。
- 如果 task-supervisor e2e 卡在 task completion，运行 `cbth task inspect --task-id <task-id>` 并检查 `status`、`pid`、log paths 和 truncation flags。
- 如果 assistant marker turn 看起来已经完成，但 batch 进入 `manual_resolution_only`，查看 `cbth audit list --source-thread-id <thread-id>` 中的 acceptance 和 observation/reconcile evidence。
