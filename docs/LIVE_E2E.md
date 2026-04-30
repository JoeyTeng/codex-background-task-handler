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
```

可选 env：

- `CBTH_LIVE_CODEX_BIN`: 覆盖真实 `codex` binary，默认 `codex`。
- `CBTH_LIVE_NODE_BIN`: 覆盖 Node binary，默认 `node`。
- `CBTH_LIVE_CODEX_E2E_TIMEOUT_MS`: shared app-server smoke 超时，默认 `180000`。
- `CBTH_LIVE_TRUSTED_ALL_E2E_TIMEOUT_SECONDS`: trusted-all full e2e 超时，默认 `360`。

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
test result: ok. 1 passed; finished in 139.65s
```

## Failure Notes

- 如果卡在 listener bootstrap，先单独运行 shared app-server smoke；它能快速暴露 `codex app-server` 输出流、登录态或 Node PoC 问题。
- 如果卡在 session readiness，通常说明 bootstrap thread 不能被新的 app-server resume，或 passive adapter 没拿到 idle/current-state/capability proof。测试失败信息会打印最后观察到的 session proof 字段。
- 如果卡在 batch delivered，优先看 `cbth audit list --source-thread-id <thread-id>`：`accepted` 后没有 `observed/reconciled` 说明真实 model turn 还没有 terminal evidence，或 websocket continuity 已丢失。
- 测试使用临时 `CBTH_HOME`，结束时会 stop daemon 并清理临时目录；异常中断后可用 `pgrep -af "codex app-server"` 和 `pgrep -af cbth` 检查是否有遗留进程。

## Skill Candidate

这条流程适合作为后续个人 skill，但建议先保持为 repo 文档和 ignored test。原因是它依赖本机 Codex 登录态、模型权限、真实 app-server 行为和 runner 网络条件；等流程在多次本机复测后，再把常用命令、失败诊断和 cleanup 收敛进 `~/.codex/skills/cbth-live-e2e`。
