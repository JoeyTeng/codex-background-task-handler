# 操作恢复

语言：[British English (en-GB)](OPERATOR_RECOVERY.en-GB.md) | [简体中文 (zh-CN)](OPERATOR_RECOVERY.zh-CN.md)

本文说明 CLI dogfood v1 的手动恢复流程。它假设你使用 macOS 或 Linux 单用户工作站，并且本地 `CBTH_HOME` 或 `~/.cbth` 可信。

## 查看当前状态

先检查 daemon 和 readiness：

```bash
cbth doctor cli
cbth daemon status
```

对于 caller thread，查看当前 head batch 和 audit trail：

```bash
cbth batch inspect-head --source-thread-id <thread-id>
cbth audit list --source-thread-id <thread-id> --limit 50
```

查看绑定到 caller thread 的 managed CLI session：

```bash
cbth cli session list --bound-thread-id <thread-id>
cbth cli session inspect --managed-session-id <managed-session-id>
cbth cli app-servers -H
cbth cli app-servers --latest-generation -H
```

`cbth cli app-servers -H` 默认检查所有已知 daemon generation，最新 generation 排在最前；`--all-daemons` 是这个默认行为的兼容别名。只想检查最新 generation 时使用 `--latest-generation -H`；如果没有 generation socket，它会 fallback 到默认 daemon。当同一个 Codex app-server 报告了除 `cbth` 绑定 session 以外的 loaded thread id 时，输出可能打印 `loaded non-bound codex sessions`。这只是 best-effort 诊断：loaded 不代表 foreground/current，也不会 retarget delivery。如果 operator 想让另一个 loaded thread 接收投递，需要用 `cbth resume <new-thread-id>` 或 `cbth cli run --bind-thread-id <new-thread-id>` 显式启动对应 managed session。

如果 head batch 已经关闭，`inspect-head` 找不到它，可以从 audit 或 task/job output 中取 `batch_id`：

```bash
cbth batch inspect --batch-id <batch-id>
```

## Task Logs

Daemon-supervised tasks 在关联 job 完成或失败后仍可查看：

```bash
cbth task list --source-thread-id <thread-id>
cbth task inspect --task-id <task-id>
```

`task inspect` 返回 `stdout_log_path`、`stderr_log_path`、byte counts 和 truncation flags。路径相对 `CBTH_HOME`；默认 home 下可以这样查看：

```bash
less ~/.cbth/<stdout_log_path>
less ~/.cbth/<stderr_log_path>
```

Completed task log directories 会在 linked batches 保持 open 时保留，并在 close 后继续保留一个 retention window。Maintenance cleanup 后，durable task row 仍保留，但 log path fields 可能被清空。

## 手动 Resolution

`manual_resolution_only` 表示 `cbth` 无法证明自动投递安全完成。常见原因包括 `turn/start` acceptance 模糊、acceptance 后 websocket/app-server continuity 丢失、terminal failure/interruption evidence，或 batch policy 不在当前自动路径内。

使用 audit trail 判断 assistant-visible result 是否已经落到 caller thread：

```bash
cbth audit list --source-thread-id <thread-id> --limit 100
cbth batch inspect-head --source-thread-id <thread-id>
```

如果你确认 caller thread 已经收到并使用了结果，把 head batch 关闭为 confirmed：

```bash
cbth batch close-head \
  --source-thread-id <thread-id> \
  --reason operator-confirmed-delivery \
  --note "verified in caller thread"
```

如果无法证明 delivery，重试或记录 follow-up 前先 unconfirmed close：

```bash
cbth batch close-head \
  --source-thread-id <thread-id> \
  --reason operator-closed-unconfirmed \
  --note "manual recovery: delivery could not be proven"
```

不要手动编辑 SQLite database 或 task log files。使用 CLI 才能保持 audit records、artifact retention 和 daemon lifecycle state 一致。

## Session Retirement

Managed CLI sessions 可能比前台 `codex --remote` 进程活得更久。Foreground teardown 后，`cbth` 会把 session 标成 `detached` 并清除 proof，避免 stale idle/capability evidence 打开新的自动投递。如果 delivery fail-closed 到 `manual_resolution_only`，session 会变为 `parked`，直到 manual head batch 被关闭或 sweep。

手动恢复完成后，如果要用不同 risk profile 复用 thread，或替换 stale record，先 retire 旧的 non-live session：

```bash
cbth cli session retire \
  --managed-session-id <managed-session-id> \
  --reason "operator cleanup after manual recovery"
```

Retirement 是 fail-closed。它会拒绝 `live` sessions、仍拥有 active delivery attempts 的 sessions，以及 bound thread 仍有 open `manual_resolution_only` head batch 的 sessions。

## 维护和清理

需要立即 reconcile stale attempts、expired artifacts 或 task-log cleanup 时运行 sweep：

```bash
cbth maintenance sweep
```

只有在 active tasks 已完成或已被有意 cancel 后才停止 daemon：

```bash
cbth task list --status running
cbth daemon stop
```

Daemon crash 或 restart 后，无法继续证明受托管的 queued/running tasks 会在 startup recovery 中 fail closed。重新提交 work 前，先检查 failed tasks 和关联 batches。
