# CLI Shared App-Server Sidecar Design

## 目标

- 不修改上游 `codex` 仓库。
- 保持前台用户使用原生 Codex TUI 的体验。
- 让后台 sidecar 能在长任务完成后，对同一个 CLI thread 继续执行，而不是依赖当前 turn 长时间挂起。
- 让前台 TUI 能感知 sidecar 触发的新 turn。

共通核心部分见：

- `docs/SHARED_CORE_ARCHITECTURE.md`

## 已敲定的约束

- 当前 CLI TUI 不能直接复用 Desktop 的 heartbeat / automation bridge 方案。
- 原因不是产品层面猜测，而是 TUI 目前直接拒绝 dynamic tool call，报错文案就是 `Dynamic tool calls are not available in TUI yet.`。
- 因此，CLI 方向的可靠方案不是 `automation_update`，而是共享 `app-server`。

## 核心设计

### 组件

1. `shared app-server`
   - 由 CLI 入口启动；在第一版共享架构里，这个入口是主 binary 的一个子命令，而不是独立产品。
   - 例如可表现为：

```text
codex app-server --listen ws://127.0.0.1:<port>
```

2. `foreground TUI`
   - 使用原生 Codex TUI，通过 `--remote` 连接共享 `app-server`：

```text
codex --remote ws://127.0.0.1:<port>
```

3. `sidecar client`
   - 与前台 TUI 并列，作为第二个 websocket client 连接同一个 `app-server`。
   - 负责等待 CI、等待 reviewer、等待外部系统结果、以及在结果 ready 后恢复 caller thread。

4. `optional shared job state`
   - 如果 sidecar 需要脱离当前进程继续工作，可以直接复用共享核心里的 store 与 `cbth job ...` CLI 子命令。
   - 但就“第二个 client 能否续跑同一 live thread”这一核心能力而言，不需要 heartbeat 或 Desktop 那套 bridge 结构。

## 已实证支持的关键能力

### 1. 第二个 client 可以续跑同一个 thread

最小 PoC 位于：

```text
scripts/cli_shared_app_server_poc.mjs
```

PoC 流程：

1. frontend client 连接共享 `app-server`
2. frontend 创建 thread
3. frontend 在该 thread 上先完成一个 seed turn
4. sidecar client 连接同一个 `app-server`
5. sidecar 对同一 thread 执行 `thread/resume`
6. sidecar 在该 thread 上执行新的 `turn/start`

已验证结果：

- `thread_id = 019db60d-97d8-7e73-b992-afe8073d7fe6`
- `frontend_seed_turn_id = 019db60d-97f2-7742-90ec-dfdf2c6e9436`
- `sidecar_turn_id = 019db60d-a9bd-7ad1-a372-931865554a89`
- `frontend_saw_turn_started = true`
- `frontend_saw_turn_completed = true`
- `sidecar_turn_status = completed`
- `marker_visible_in_thread_read = true`

这说明：

- sidecar client 可以续跑同一个 live thread
- 另一个已订阅该 thread 的前台 client 会收到 `turn/started` 与 `turn/completed`
- sidecar 结果会被正常写入 thread 历史

### 2. 真实前台 TUI 也能感知

除了协议层 PoC 以外，还额外做了真实 PTY 级别的 TUI 实测：

1. 启动共享 `app-server`
2. 用真实前台 TUI 连接：

```text
codex --remote ws://127.0.0.1:4312 --no-alt-screen -C /Users/hoteng/Program/GitHub/codex-background-task-handler
```

3. 让前台 TUI 先完成 seed turn，marker 为：

```text
TUI_SEED_MARKER_20260422
```

4. 让外部 sidecar client 对同一 thread 续跑，marker 为：

```text
TUI_SIDECAR_MARKER_20260422
```

5. 在 PTY 终端输出中，前台 TUI 实际显示出了 sidecar 触发的第二轮用户输入与 assistant 输出。

对应 thread 为：

- `thread_id = 019db614-1fb7-70a3-956f-7a96c48f0226`

这一步把结论从“协议层前台 client 能收到通知”进一步推进到了“真实前台 TUI 会把 sidecar 触发的新 turn 展示给用户”。

### 3. Active turn 可以被 `turn/steer`，且不会提前结束

针对 active-turn 边界，新增了最小 PoC：

```text
scripts/cli_turn_steer_poc.mjs
```

PoC 流程：

1. frontend client 连接共享 `app-server`
2. frontend 创建 thread
3. frontend 启动一个会调用 shell `sleep 10` 的 regular turn
4. 在该 turn 已经 `turn/started` 且仍处于 active 状态时
5. sidecar client 对同一 thread 调用 `turn/steer`
6. 等待原 turn 正常完成，并检查：
   - steer 返回的是否还是同一个 `turn_id`
   - steer 后是否没有额外新 turn 被启动
   - 原 turn 是否继续完成而不是被提前截断
   - 最终 assistant 文本是否吸收 steer 指令

已验证结果：

- `thread_id = 019db65d-2df2-7941-8871-b8ed1fe0b73b`
- `turn_id = 019db65d-2e9c-7ff2-9690-f84b725a9a12`
- `same_turn_id_after_steer = true`
- `turn_completed_same_turn = true`
- `turn_status = completed`
- `turn_started_notification_count = 1`
- `no_additional_turn_started = true`
- `notifications_have_command_execution = true`
- `final_agent_message_from_notifications = CLI_TURN_STEER_APPLIED_MARKER_20260422`
- `final_message_matches_steer_via_notifications = true`
- `no_premature_completion_signal = true`

观测到的关键时序：

- 原 turn 总耗时约 `24.8s`
- steer 发生在完成前约 `23.3s`
- steer 之后没有出现新的 `turn/started`
- 原 turn 最终以同一个 `turn_id` 正常完成

这说明：

- `turn/steer` 在 CLI shared `app-server` 路线下可以安全落在同一个 active regular turn 上
- steer 不会把当前 turn 直接截断成一个新的 turn
- steer 也不会导致当前 turn 提前 `turn/completed`
- 最终结果会吸收 steer 的后续指令

## 架构结论

CLI 的可行产品化路线是：

```text
cbth cli run
  -> spawn shared codex app-server
  -> spawn foreground codex --remote
  -> spawn sidecar client(s)
```

关键点：

- 前台仍然是原生 Codex TUI
- sidecar 不需要 hack TUI 本身
- sidecar 只需要作为第二个 `app-server` client 操作同一个 thread
- 这条路不依赖 dynamic tools / automations

## 不适用的方案

- Desktop heartbeat bridge
  - CLI TUI 当前没有可用的 dynamic tool call 面，因此不能照搬。

- 默认 embedded `codex`
  - 默认 `codex` / `codex resume` 走的是 embedded `app-server`，没有可被外部 sidecar attach 的公共 transport。
  - 因此必须由 CLI 入口接管启动方式。

## 第一版实现建议

1. `cbth cli run` 启动共享 `codex app-server`
2. `cbth cli run` 启动前台 `codex --remote`
3. CLI 入口为当前会话保存 `thread_id`
4. sidecar 监听外部任务状态
5. 任务 ready 后：
   - 如果 caller thread idle：`thread/resume + turn/start`
   - 如果 caller thread active：优先使用 `turn/steer`
6. 前台 TUI 通过已有线程订阅自然感知新 turn

## 仍待补的边界

- CLI 入口的进程生命周期和清理策略
- sidecar 长时间运行时的状态持久化与 resume 策略
- 多个 background jobs 同时命中同一 caller thread 时的串行化策略
