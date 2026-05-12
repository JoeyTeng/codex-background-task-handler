# Live E2E Reproduction Guide

本页记录需要真实 Codex 登录态、模型访问和网络的 opt-in live checks。默认 CI 不运行这些测试；它们只需要保持编译和 lint-clean。

## Environment

- 已安装并登录 `codex` CLI。
- 已安装 Node.js，用于 `scripts/cli_shared_app_server_poc.mjs`。
- 本机有模型/网络访问；这些测试会创建真实 Codex thread，并至少运行一次真实 model turn。
- 推荐先确认版本：

```bash
codex --version
node --version
cargo --version
cargo run --bin cbth -- doctor cli
```

可选 env：

- `CBTH_LIVE_CODEX_BIN`: 覆盖真实 `codex` binary，默认 `codex`。
- `CBTH_LIVE_NODE_BIN`: 覆盖 Node binary，默认 `node`。
- `CBTH_LIVE_CODEX_E2E_TIMEOUT_MS`: shared app-server smoke 超时，默认 `180000`。
- `CBTH_LIVE_TRUSTED_ALL_E2E_TIMEOUT_SECONDS`: trusted-all full e2e 超时，默认 `360`。
- `CBTH_LIVE_NEW_THREAD_E2E_TIMEOUT_SECONDS`: `--new-thread` full e2e 超时，默认 `360`。
- `CBTH_LIVE_TASK_SUPERVISOR_E2E_TIMEOUT_SECONDS`: task-supervisor full e2e 超时，默认 `360`。

## Shared App-Server Smoke

该 smoke 启动真实 `codex app-server`，用 Node PoC 创建 frontend thread，再用 sidecar client 对同一 thread 发送 `turn/start`，确认 frontend 能看到 started/completed notification，且 `thread/read` 可看到 marker。

```bash
CBTH_RUN_LIVE_CODEX_E2E=1 cargo test --test live_smoke -- --ignored
```

当前实现同时从 `stdout` 和 `stderr` 解析 `listening on: ws://...`，兼容 `codex-cli 0.125.0` 在非 TTY 下把 app-server banner 输出到 `stderr` 的行为。

## Trusted-All Full Live E2E

该 e2e 验证真实 `cbth cli run --auto-delivery-policy trusted-all` 自动投递闭环：

- 先用真实 `codex app-server` 创建 caller thread，并跑一个极短 seed turn，保证该 thread 可被后续 app-server 实例 resume。
- 启动 `cbth cli run --bind-thread-id <thread-id> --auto-delivery-policy trusted-all`。
- 测试专用 wrapper 只拦截 foreground `codex --remote` 并保持进程存活；`app-server` 分支仍执行真实 `codex app-server`。
- 等待 sidecar 记录 idle proof 和自动投递能力。
- 通过 `cbth job submit` / `cbth job fail` 创建 head batch。
- 等待 sidecar 发送带唯一 marker 的 `turn/start`，接受返回的 `turn.id`，并通过 notification 或 same-epoch `thread/read` reconcile 关闭 batch 为 `close_reason=delivered`。
- 校验 `cbth audit list` 至少包含 `allow`、`attempt-start`、`accepted`，以及 `observed` 或 `reconciled`。

```bash
CBTH_RUN_LIVE_TRUSTED_ALL_E2E=1 cargo test --test live_trusted_all -- --ignored
```

本机已验证一次成功运行：

```text
test live_codex_trusted_all_auto_delivery_is_opt_in ... ok
test result: ok. 1 passed; finished in 52.59s
```

## New-Thread Trusted-All Full Live E2E

该 e2e 验证真实 `cbth cli run --new-thread --auto-delivery-policy trusted-all` 的 fresh bootstrap：

- `cbth` 先启动 daemon-owned pending `codex app-server`，在同一个进程上调用 `thread/start`，再把该进程提升为 managed app-server；这是因为 Codex 0.128 在 first user message 前不会把 fresh thread rollout materialize，关闭 bootstrap app-server 会让后续 `thread/resume` 失去该 thread。
- bootstrap 不向 foreground TUI 注入任何输入。
- `cbth` 在启动 foreground 前向 stderr 打印 `cbth: bound thread id: <thread-id>`；测试只用这行提取 thread id。
- 测试专用 wrapper 只拦截 foreground `codex --remote` 并保持进程存活；`app-server` 分支仍执行真实 `codex app-server`。
- 后续流程与 existing-thread trusted-all e2e 相同：等待 session idle/capability proof，提交 exact-reply marker failed job，等待自动 `turn/start` 并关闭 batch 为 `delivered`。fresh unmaterialized thread 的初始 proof 使用 `thread_start + thread/read(includeTurns=false)`，accepted turn 后再回到 materialized rollout 的 observation/reconcile 路径。live marker 明确要求 assistant 只回复固定字符串且不运行工具，避免 fresh thread 把 e2e prompt 当作真实开发任务递归执行。

```bash
CBTH_RUN_LIVE_NEW_THREAD_E2E=1 cargo test --test live_new_thread -- --ignored
```

本机已验证一次成功运行：

```text
test live_codex_new_thread_trusted_all_auto_delivery_is_opt_in ... ok
test result: ok. 1 passed; finished in 15.54s
```

## Task Supervisor Full Live E2E

该 e2e 验证真实 daemon-owned `cbth task run` 可以触发完整 trusted-all 自动投递闭环：

- 启动真实 `cbth cli run --new-thread --auto-delivery-policy trusted-all`，并从 stderr 的 `cbth: bound thread id: <thread-id>` 读取 caller thread id。
- 等待 sidecar 记录 idle proof 和自动投递能力。
- 通过真实 `cbth task run --source-thread-id <thread-id> ... -- /bin/sh -c ...` 创建 daemon-supervised task。
- daemon 负责 child/process-group lifecycle，把 stdout/stderr 写到 `~/.cbth/tasks/<task-id>/` 下的 bounded log files。
- task 成功退出后，daemon 写 result artifact、自动 complete 关联 job、创建 head batch。
- sidecar 在 idle 时发送带 marker 的 `turn/start`，观察 notification 或 reconcile 后把 batch 关闭为 `delivered`。
- 测试同时校验 task stdout log、result artifact payload 和 audit records。

```bash
CBTH_RUN_LIVE_TASK_SUPERVISOR_E2E=1 cargo test --test live_task_supervisor -- --ignored --nocapture
```

该测试仍不进入默认 CI；它需要真实 `codex` 登录态、模型/网络访问和本机 shell execution。默认 CI 只编译该 ignored test，并通过 deterministic fake e2e 覆盖生产状态机。

本机已验证一次成功运行：

```text
test live_codex_task_supervisor_e2e_is_opt_in ... ok
test result: ok. 1 passed; finished in 23.47s
```

## Manual Dogfood Walkthrough

After installing the local binary with `cargo install --path .`, run the readiness check first:

```bash
cbth doctor cli
```

Then start a native Codex foreground session through `cbth`:

```bash
cbth new \
  --session-allows-approval false \
  --session-allows-network false \
  --session-allows-write-access false \
  --auto-delivery-policy trusted-all \
  --model gpt-5.5
```

`cbth new` is equivalent to the fresh-thread `cbth cli run --new-thread` path, but it accepts Codex foreground args directly. The bootstrap carries parsed model/config overrides into `thread/start` and fills missing `model_reasoning_effort` from the app-server's effective config so direct `codex` and managed fresh-thread starts use the same default thinking level.

For an already materialized thread, `cbth resume <thread-id> [-- <codex_args>]` is the preferred native-resume wrapper. The three `--session-allows-*` flags default to `auto` there: the sidecar reads `approvalPolicy` / `sandbox` from `thread/resume`, pins the startup permission snapshot, refreshes current permissions before automatic `turn/start`, and warns/audits if permissions drift. This fresh-thread smoke keeps explicit `false` flags because the unmaterialized `--new-thread` bootstrap may not have a trusted `thread/resume` permission snapshot before the first user message.

Copy the thread id from the stderr line:

```text
cbth: bound thread id: <thread-id>
```

From another shell, submit a supervised background command:

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

Use the recovery surface if the batch does not close automatically:

```bash
cbth task list --source-thread-id <thread-id>
cbth batch inspect-head --source-thread-id <thread-id>
cbth audit list --source-thread-id <thread-id> --limit 100
```

## Failure Notes

- 如果卡在 listener bootstrap，先单独运行 shared app-server smoke；它能快速暴露 `codex app-server` 输出流、登录态或 Node PoC 问题。
- 如果 `--new-thread` 没打印 `cbth: bound thread id: ...`，优先看 bootstrap `thread/start` 是否被当前 `codex app-server` 支持；该失败会发生在 foreground 启动前。
- 如果卡在 session readiness，通常说明 pending app-server 没有被成功 promote，或 passive adapter 没拿到 fresh `thread_start` / current-state / capability proof。测试失败信息会打印最后观察到的 session proof 字段。
- 如果 task-supervisor e2e 卡在 task completion，先用 `cbth task inspect --task-id <task-id>` 看 `status`、`pid`、`stdout_log_path`、`stderr_log_path` 和 truncation flags；如果 daemon 被异常终止，下一次 daemon startup 会把 lost queued/running task fail-closed。
- 如果 rollout 显示 assistant 已完成 marker turn，但 batch 进入 `manual_resolution_only`，检查 `turn/start` acceptance 是否超过 60 秒；也要检查 accepted observation 是否遇到 proof-only `thread/status/changed` noise 或 fresh first-turn `thread/read(includeTurns=true)` materialization error。
- 如果卡在 batch delivered，优先看 `cbth audit list --source-thread-id <thread-id>`：`accepted` 后长时间没有 `observed/reconciled` 说明真实 model turn 还没有 terminal evidence，或 websocket continuity 已丢失。terminal audit 是 batch close 后的 best-effort 记录，live harness 会短暂轮询，避免把 close/audit 的事务边界当成同步点。
- 测试使用临时 `CBTH_HOME`，结束时会 stop daemon 并清理临时目录；异常中断后可用 `pgrep -af "codex app-server"` 和 `pgrep -af cbth` 检查是否有遗留进程。

## Skill Candidate

这条流程适合作为后续个人 skill，但建议先保持为 repo 文档和 ignored test。原因是它依赖本机 Codex 登录态、模型权限、真实 app-server 行为和 runner 网络条件；等流程在多次本机复测后，再把常用命令、失败诊断和 cleanup 收敛进 `~/.codex/skills/cbth-live-e2e`。
