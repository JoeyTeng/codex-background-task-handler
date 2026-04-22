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
- 当前这条路线依赖实验 RPC，因此第一版必须把 capability probe 与 fail-closed 当成正式设计的一部分。

## 核心设计

### 组件

1. `shared app-server`
   - 由 daemon 持有，作为 managed CLI session process。
   - `cbth cli run` 负责创建或附着到这个 managed session，而不是长期直接持有该进程。
   - 例如可表现为：

```text
codex app-server --listen ws://127.0.0.1:<port> --ws-auth capability-token --ws-token-file <token-file>
```

第一版安全边界必须再加两条：

- 只绑定 loopback 地址，不允许监听非本机可达地址。
- 默认启用 websocket bearer-token auth，而不是把本地 loopback 当成唯一控制面。

2. `foreground TUI`
   - 使用原生 Codex TUI，通过 `--remote` 连接共享 `app-server`：

```text
codex --remote-auth-token-env CBTH_REMOTE_AUTH_TOKEN --remote ws://127.0.0.1:<port>
```

3. `sidecar client`
   - 与前台 TUI 并列，作为第二个 websocket client 连接同一个 `app-server`。
   - 负责等待 CI、等待 reviewer、等待外部系统结果、以及在结果 ready 后恢复 caller thread。

4. `optional shared job state`
   - sidecar 不直接从外部任务脚本拿结果，而是消费共享核心里的 thread-scoped queue / delivery batch。
   - 但就“第二个 client 能否续跑同一 live thread”这一核心能力而言，不需要 heartbeat 或 Desktop 那套 bridge 结构。

## 本地信任边界

- 上游 `app-server` 现在已经支持 websocket auth：
  - `--ws-auth capability-token --ws-token-file <path>`
  - `--ws-auth capability-token --ws-token-sha256 <hex>`
  - `--ws-auth signed-bearer-token --ws-shared-secret-file <path>`
- 上游 `codex --remote` 也支持：
  - `--remote-auth-token-env <ENV_VAR>`
- 因此，第一版不应把“本机 loopback 默认可信”当成唯一安全前提。
- 第一版推荐的最小安全边界是：
  - shared `app-server` 只监听 `127.0.0.1` / `localhost`
  - daemon 为每次 managed session 生成一枚新的 bearer token
  - token 必须落在 session-scoped token file，权限至少 `0600`
  - app-server 以 websocket auth 模式启动
  - 前台 `codex --remote` 通过 `--remote-auth-token-env` 读取同一 token
  - sidecar client 也使用同一 token
- token 生命周期合同：
  - 每个 managed CLI session 一枚独立 token
  - session 结束后立即作废
  - wrapper 自己的环境不长期保留 token
  - 不把 token 写入持久日志或 thread 历史
- 信任边界合同：
  - bearer token 主要防护“无关的本地进程/旧会话”误连 shared `app-server`
  - 由当前前台 Codex session 自己启动的命令，如果继承了同一进程环境，则视为同一 trust domain，而不是额外隔离边界
- 如果当前运行环境无法同时满足：
  - loopback-only 监听
  - bearer token 注入
  则第一版实现应 fail-closed，而不是退化成长期开放的本地 websocket 控制面。

## Shared App-Server 所有权

- shared `app-server` 不应只是前台 `cbth cli run` 的短生命周期子进程。
- 第一版应把它建模成：
  - daemon-owned managed session process
  - 前台 `codex --remote` 和 sidecar 都只是这个 session process 的 client
- 这样当：
  - 前台 TUI 退出
  - 但 active jobs 仍存在
  时，daemon 仍可保留：
  - shared `app-server`
  - sidecar session metadata
  - 当前 thread 的 live continuation 能力
- 当同时满足以下条件时，daemon 才允许清理该 managed session：
  - 没有 active jobs
  - 没有连接中的 foreground client
  - 没有需要继续投递的 sidecar client

## 实验 RPC 依赖面

### 当前 PoC 实际依赖

当前 PoC 在 `initialize` 时显式声明：

```text
capabilities.experimentalApi = true
```

并使用了以下 RPC：

- 关键能力：
  - `thread/resume`
  - `turn/start`
  - `turn/started`
  - `turn/completed`
- 可选优化：
  - `turn/steer`
- 仅用于 PoC 验证，不应作为第一版正式依赖：
  - `thread/start`
  - `item/completed`

### 第一版要求

- 启动时必须做 capability probe。
- 如果缺少 `thread/resume` 或 `turn/start`，CLI adapter 必须 fail-closed。
- 如果缺少 `turn/steer`，CLI adapter 仍可工作，但只能在 caller idle 时投递。
- 不能把 PoC 中碰巧可用的实验 RPC 直接当成长期稳定契约。
- 第一版 shipping 配置默认关闭 `turn/steer`，直到 active-turn 分类与安全门槛被实证支持。

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

但要注意：

- 这个 PoC 只覆盖了一个很窄的场景：
  - regular turn
  - `sleep 10`
  - `approvalPolicy: never`
  - 无网络 I/O
- 因此它只能支持“`turn/steer` 可作为受限优化”。
- 它不足以支持“任何 active turn 都默认优先 steer”的更强结论。

## 投递策略

CLI adapter 不直接按单 job 投递，而是消费共享核心为每个 thread 生成的 `delivery batch`。

### 默认策略

- 同一 `source_thread_id` 只允许一个 in-flight delivery attempt。
- ready jobs 先进入 thread-scoped FIFO 队列。
- daemon 对同一 thread 上的相邻 jobs 做 batch 合并。
- 默认只在 caller thread idle 时使用：

```text
thread/resume + turn/start
```

### Idle 判定与 race contract

- CLI adapter 的 idle 判定源必须来自 shared `app-server` 的 live event stream，而不是 TUI 屏幕解析。
- 第一版至少使用以下信号维护本地 thread liveness 视图：
  - `turn/started`
  - `turn/completed`
  - `thread/status/changed`（如果可用）
- 一个 thread 只有在以下条件同时满足时，才允许被本地视图判为 idle：
  - 最近一次已观察到的 regular turn 已完成/中断
  - 且在那之后没有新的 `turn/started`
- `turn/start` 本身必须被视为最后一道 compare-and-swap：
  - 如果 sidecar 观察到 idle 后发起 `thread/resume + turn/start`
  - 但在请求到达前用户又启动了新 turn
  - adapter 必须把这种失败当成 benign race，而不是成功送达
- benign race 的处理规则：
  - 不得关闭 batch
  - 不得创建第二个并发 attempt
  - 当前 attempt 保持在 `prepared`，不得推进到 `armed`
  - `last_delivery_attempt_at` 不得因为这次 race 被当成成功投递而更新
  - `delivery_attempt_count` 不得递增
  - 必须清除本地 idle 视图
  - 必须等待下一次 idle 观测后再重试
- 换句话说，CLI 第一版的串行化依赖：
  - daemon 的 per-thread attempt lease
  - app-server 的 live event stream
  - `turn/start` 失败时的 retry-on-next-idle 约束

### `turn/steer` 的策略

- `turn/steer` 是可选优化，不是默认主路径。
- 第一版默认 shipping 配置中应当关闭。
- 只有在以下条件同时满足时才允许使用：
  - capability probe 明确支持
  - 当前 caller turn 仍处于 active regular turn
  - 当前 batch 的 `delivery_read_only=true`
  - 当前 batch 的 `delivery_requires_approval=false`
  - 当前 batch 的 `delivery_requires_network=false`
  - 当前 batch 的 `delivery_requires_write_access=false`
  - 当前 batch 的 `steer_candidate=true`
  - 当前 batch 的 `inline_payload_bytes` 没超过 CLI adapter 的 steer 上限
  - 当前 thread 未触发最小连续发送间隔限制
- 不满足上述任一条件时，batch 保持排队，等 caller idle 后再 `turn/start`。

## 架构结论

CLI 的可行产品化路线是：

```text
cbth cli run
  -> ask daemon for a managed CLI session
  -> spawn foreground codex --remote
  -> spawn sidecar client(s)
```

关键点：

- 前台仍然是原生 Codex TUI
- sidecar 不需要 hack TUI 本身
- sidecar 只需要作为第二个 `app-server` client 操作同一个 thread
- 这条路不依赖 dynamic tools / automations
- 但它依赖实验 RPC，因此必须有 capability probe 与 fail-closed 策略

## 不适用的方案

- Desktop heartbeat bridge
  - CLI TUI 当前没有可用的 dynamic tool call 面，因此不能照搬。

- 默认 embedded `codex`
  - 默认 `codex` / `codex resume` 走的是 embedded `app-server`，没有可被外部 sidecar attach 的公共 transport。
  - 因此必须由 CLI 入口接管启动方式。

## 第一版实现建议

1. daemon 创建或恢复一个 managed CLI session
2. daemon 为该 session 启动共享 `codex app-server`
   - 监听 loopback
   - 启用 websocket bearer-token auth
3. 启动时对实验 RPC 做 capability probe
4. `cbth cli run` 连接该 managed session，并启动前台 `codex --remote --remote-auth-token-env ...`
5. CLI 入口为当前会话保存 `thread_id`
6. sidecar 从共享核心读取 per-thread `delivery batch`
7. 任务 ready 后：
   - 默认只在 caller thread idle 时：`thread/resume + turn/start`
   - 只有显式打开 steer feature flag，且 `turn/steer` 能力存在并满足只读/低风险策略时：允许 steer
8. 如果能力不足或协议形状漂移：fail-closed，不做自动续跑
9. 前台 TUI 通过已有线程订阅自然感知新 turn

## 仍待补的边界

- CLI 入口的进程生命周期和清理策略
- sidecar 长时间运行时的状态持久化与 resume 策略
- capability probe 的具体实现与版本策略
- 多个 background jobs 同时命中同一 caller thread 时的 batch 合并参数
