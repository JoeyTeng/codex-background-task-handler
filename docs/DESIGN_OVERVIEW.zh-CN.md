# 设计概览

语言：[English (en-GB)](DESIGN_OVERVIEW.en-GB.md) | [简体中文 (zh-CN)](DESIGN_OVERVIEW.zh-CN.md)

`cbth` 是围绕 Codex 长时间本地工作的 companion layer。它不修改上游 `codex`，而是自己管理本地 daemon state、supervised tasks，以及与 Codex CLI 或 Desktop 实验面的窄集成点。

## 这个项目解决什么问题

Codex 可能启动超过单次前台交互寿命的工作：测试套件、build jobs、polling loops，或其他本地命令。`cbth` 在这些工作周围提供一个持久本地控制面：

- 启动或 resume 绑定到特定 Codex thread 的 managed Codex CLI session。
- 在 daemon supervision 下运行本地命令，并把 task output 保存在磁盘。
- 把 task completion 转成绑定 thread 的 durable delivery batch。
- 只有在本地 proof 和 policy checks 允许时才尝试自动投递。
- 无法证明自动投递时留下清晰的 manual recovery state。

## Shared Core

实现正在收敛为一个 Rust binary 加薄集成入口：

- CLI entrypoints：managed Codex sessions、task supervision、job/batch operations 和 operator recovery。
- Desktop-facing helper surfaces：bridge preflight、inbox snapshots、installation state 和 writeback validation。
- Same-user local daemon：负责 mutating operations、task process lifecycle、app-server leases 和 maintenance sweeps。
- `CBTH_HOME` 或 `~/.cbth` 下的 local SQLite store 与 managed artifact/log files。

详细内部架构记录见 [design/SHARED_CORE_ARCHITECTURE.md](design/SHARED_CORE_ARCHITECTURE.md)。

## CLI 投递路径

当前主要 dogfood 路径是 CLI managed-session delivery：

1. `cbth new`、`cbth resume` 或 `cbth cli run` 通过 daemon-owned loopback `codex app-server` 启动 foreground Codex。
2. Sidecar client 观察 thread/session state，并记录 durable activity、capability 和 permission proof。
3. `cbth task run` 或 job commands 创建 durable task/job/batch state。
4. 显式传入 `--auto-delivery-policy trusted-all` 后，sidecar 等待 idle proof，发送一个带 marker 的 `turn/start`，接受返回的 `turn.id`，并且只在 terminal observation 或 same-epoch reconcile 后关闭 batch。
5. 如果 acceptance 或 observation 模糊，batch 会进入 manual recovery，而不是盲目重试。

详细设计记录：

- [design/CLI_SHARED_APP_SERVER_SIDECAR_DESIGN.md](design/CLI_SHARED_APP_SERVER_SIDECAR_DESIGN.md)
- [design/CLI_TASK_SUPERVISOR_DESIGN.md](design/CLI_TASK_SUPERVISOR_DESIGN.md)
- [design/CLI_ACTIVE_TURN_STEER_DESIGN.md](design/CLI_ACTIVE_TURN_STEER_DESIGN.md)

## Desktop Bridge 边界

Desktop bridge work 还不是已启用的自动投递路径。当前 foundation 聚焦：

- Installation-wide Desktop capability state。
- Caller heartbeat binding records。
- `~/.cbth/inbox/` 下的 preflight snapshots。
- 针对已发布 inbox JSON 的 no-DB read helpers。
- 用于 writeback-like signals 的 transcript/tool-output relay validation。

Desktop 自动投递仍需要 production rollout tailing、boundary crossing、artifact-read policy 和更强的 lifecycle cleanup，之后才能被视为 supported path。

详细记录：

- [design/DESKTOP_BACKGROUND_TASK_BRIDGE_DESIGN.md](design/DESKTOP_BACKGROUND_TASK_BRIDGE_DESIGN.md)
- [design/DESKTOP_BRIDGE_FOUNDATION.md](design/DESKTOP_BRIDGE_FOUNDATION.md)
- [validation/](validation/)

## Safety Model

项目刻意保守：

- Local IPC 是 same-user 和 loopback-only。
- Mutating operations 默认通过 daemon。
- Automatic delivery 是 opt-in 且 fail-closed。
- CLI permissions 从 startup/current proof pin 住并做 cap，不做 widen。
- 模糊的 delivery evidence 会进入 `manual_resolution_only`。
- Desktop helper paths 在完整 delivery boundary 被证明之前，优先使用 read-only snapshots 和显式 operator validation。

`trusted-all` 权限很宽，应只在专用单用户工作站上使用。

## 演进方向

近期方向是在继续加固 CLI dogfood 路径的同时，把 Desktop 从 validation surfaces 推进到 production bridge：

- 收紧 daemon recovery、handoff 和 stale-state cleanup。
- 随着上游 protocol 演进，保持 Codex app-server compatibility probes 更新。
- 通过改进 observation 和 reconcile paths，继续减少 manual recovery cases。
- 只把稳定的 user-facing workflows 提升进双语 guide set。
- 详细 design、plan 和 validation records 继续保持内部，除非它们变成 operator-facing。
