# Project State

## 当前目标

验证一套纯外围方案，分别覆盖 Codex Desktop 与 CLI 两条交互路径，让长时间后台任务可以在不修改上游 `codex` 仓库的前提下恢复或继续 caller thread。

## 当前架构收敛

- 双端方案现在共享一套更清晰的核心抽象：
  - 一个共享的本地 daemon
  - 一个共享的 job store / 状态机
  - 一个共享的 CLI 控制面
  - 两个薄的 Codex integration adapters：CLI 与 Desktop
- 第一版不做系统级常驻服务；改为按需启动的本地 daemon。
- 该 daemon 生命周期独立于单个 Codex 前台实例，但在没有 active jobs 且没有活跃接入端时会自动退出。
- 第一版稳定外部接口只做 CLI，不承诺公开 socket / Web / plugin 协议。
- 因此，之前设计里提到的 `background-taskctl` helper，应收敛成主 binary 的 `cbth job ...` 子命令，而不是第二个长期维护的独立工具。
- 经过 reviewer 复核后，Desktop 关键路径又做了一个更保守的收口：
  - 不再把“heartbeat turn 稳定执行本地 `cbth ...` CLI”当作既定前提
  - 改为优先使用只读 inbox snapshot / artifact 文件
- 同时，CLI 关键路径也收口为：
  - 明确依赖实验 RPC
  - 启动时 capability probe
  - 默认仅在 idle 时 `turn/start`
  - `turn/steer` 只作为只读、低风险场景下的受限优化
- 共享核心也补上了 reviewer 指出的 thread 级缺口：
  - 引入 thread-scoped FIFO 队列
  - 引入 `delivery batch`
  - 引入最小连续发送间隔
  - 同一 thread 同时最多一个 in-flight delivery attempt
- 结果保留责任也已收敛：
  - `cbth job complete --result-file <path>` 的语义改为 ingest/copy 到 `cbth` 自己管理的 artifact store
  - 原始外部文件不再承担长期保留责任
- 这套共通核心设计已单独沉淀在：
  - `docs/SHARED_CORE_ARCHITECTURE.md`

## 已确认事实

- 普通 `codex` / `codex resume` 使用嵌入式 `app-server`，默认没有对外 attach surface。
- `codex --remote` 可以连接共享 `app-server`，适合 wrapper + sidecar 方案。
- Codex Desktop 会启动自己的私有 `codex app-server` 子进程。
- 测试 thread `019db49a-de4e-7d61-93ab-5d70a8905cc3` 的 rollout 文件当前被桌面端私有 `app-server` 打开，说明它处于已加载状态。

## 当前实验

在本仓库实现一个最小 PoC 脚本：

```text
external process
  -> spawn standalone `codex app-server --listen stdio://`
  -> initialize
  -> thread/read
  -> thread/resume
  -> thread/inject_items
  -> verify rollout persistence
```

当前脚本位于：

```text
scripts/desktop_thread_inject_poc.py
```

## 当前结果

- PoC 已在测试 thread `019db49a-de4e-7d61-93ab-5d70a8905cc3` 上跑通两次。
- 两次运行都能成功 `thread/read` + `thread/resume` + `thread/inject_items`。
- rollout 行数从 `13 -> 14 -> 15`，每次只新增一条外部注入的 `response_item`。
- 注入时，桌面端私有 `app-server` 仍然持有该 rollout 文件的打开句柄。
- `thread/read(includeTurns=true)` 不会把这种“未附着到某个 turn 的原始 injected item”直接回显出来，因此它不能作为注入成功与否的判断标准；落盘结果才是关键证据。
- 用户随后在 Codex Desktop 的该 thread 窗口内直接询问历史，可见 assistant 消息仍只有最初那一条“你好，有什么需要我处理的？”，并明确回答看不到两条外部插入 marker。
- 第二个 PoC 已通过外部独立 `app-server` 对同一 thread 发起一次完整 `turn/start`。
- 该 turn 在 `2026-04-22T10:05:45Z` 完成，assistant 最终消息为 `EXTERNAL_TURN_START_MARKER_20260422`。
- 这次不只是 rollout 落盘；`thread/read(includeTurns=true)` 也能在返回 JSON 中看到该 marker，说明完整新 turn 被 thread 结构化吸收了。
- 随后用户在同一 desktop thread 中再次发起本地 turn，请求“逐字列出当前可见的最近 2 条 assistant 消息”。
- 该本地 turn 的 assistant 回复仍然只列出早先的两条本地 assistant 文本，完全没有把 `EXTERNAL_TURN_START_MARKER_20260422` 作为可见历史的一部分。
- 这说明问题不只是“UI 不热刷新”，而是 desktop 当前 loaded thread 的后续推理上下文也没有并入外部完整 turn。
- 之后又测试了 `codex exec resume 019db49a-de4e-7d61-93ab-5d70a8905cc3 ...`。
- `exec resume` 明确复用了同一个 thread id，并成功在 `2026-04-22T10:12:10Z` 追加了一次正常 turn，assistant 最终消息为 `EXEC_RESUME_MARKER_20260422`。
- rollout 行数从 `46 -> 56`，说明 `exec resume` 对 desktop-originated thread 的效果，本质上仍是“从另一个进程往同一 persisted thread 追加 turn”。
- 又跑了一次独立 `codex exec --ephemeral --json` 的 agent 通信 PoC。该会话成功调用 `spawn_agent`，新建了 worker agent `019db4ae-49df-75e2-b41f-51f23b562858`。
- 但当前实际暴露给该 `exec` 会话的工具面中，没有模型可调用的 `followup_task` / `send_message` 投递工具，因此无法在运行时直接复现你记忆中的 mailbox 风格跨 agent 发消息。
- 代码层面仍能看到 `send_message` / `followup_task` 与 `InterAgentCommunication` 的实现，但它们通过当前进程内的 `agent_control.send_inter_agent_communication(...)` 路由，目标 thread 必须是同一个 live thread manager 能处理的 agent。

## 关键判断标准

- 如果外部进程可以 `resume` 并 `inject_items`，说明 desktop-originated thread 的持久化历史可被外围流程改写。
- 如果消息能稳定落盘，但桌面端不即时反映，则说明“改历史”与“推送进当前交互 session”仍是两回事。

## 当前判断

- “外部独立进程改写 desktop-originated thread 的持久化历史”已经被实证支持。
- “外部独立进程直接向 desktop 当前 live in-process session 推送一条会被 UI/交互环立即消费的消息”目前有了反证迹象。
- 目前更像是：外部流程可以追加 thread history，但没有证据表明 desktop 私有 `app-server` 暴露了一个可被外部直接 attach 的公共 transport。
- 更具体地说，当前最符合证据的模型是：desktop 已加载 thread 的内存态不会热合并外部对 rollout 的追加写入，至少不会把这类 `thread/inject_items` 写入即时映射到当前窗口可见历史。
- 但完整 `turn/start` 与裸 `thread/inject_items` 并不等价。前者已经被 thread/read 吸收为正常 turn，因此下一步需要用户直接观察 desktop 窗口，判断 UI 是否会对“外部完整新 turn”做热更新或至少在下一次刷新时显示。
- 现在这一步已经有结论：即使是外部完整 `turn/start`，desktop 当前已加载 session 也不会在后续本地 turn 中把它并入自己的活动上下文。
- 因此，对于 desktop，外围进程最多只能改写同一个持久化 thread 文件；它不能可靠地“给当前 live session 发消息”。
- `codex exec resume` 不是例外路径。它能成功向同一个 thread 追加 turn，但没有证据表明它绕过了 desktop 当前 loaded session 的内存隔离。
- 你记忆中的 agent mailbox 能力在代码里确实存在，但更像“同一 live process 内父子/协作 agent 线程间通信”，不是一个稳定的跨进程、跨现有 desktop session 的公开注入面。

## Heartbeat / Automation 补充判断

- Desktop 的 heartbeat / automation 与“当前 thread 里的 agent 正在长时间等待”不是一回事。
- 本地状态里存在独立的 `automations` / `automation_runs` / `inbox_items` 持久化表，并且 `automations` 直接记录 `next_run_at` 与 `last_run_at`。
- 这说明 heartbeat 至少在建模上是独立调度的后台任务，而不是依赖当前 thread 在一个活跃 turn 内持续轮询。
- 官方公开材料也把 Automations 描述为“按计划在后台运行”并在完成后把结果投递到 review queue。
- 因此，如果目标只是避免“agent 在当前 turn 里短间隔轮询、轮几次后自己放弃”，heartbeat 路线比把等待留在当前 turn 内更稳。
- 但目前还没有直接实证证明：当 Desktop app 被完全退出后，heartbeat 仍会准时触发；现有证据只能支持“它不依赖当前主 thread 持续活着轮询”，不能支持“它等价于一个系统级常驻 daemon”。
- 进一步检查发现：`~/.codex/sqlite/codex-dev.db` 中的 `automations` 表由 Desktop 主进程持有打开句柄，而私有 `codex app-server` 并未直接打开该 DB。
- 同时，App bundle 字符串中存在 heartbeat 线程选择、目标 thread、next run、pause/resume、以及 `run now` 等 UI 文案，说明 Desktop 内部确实存在面向 thread heartbeat 的调度与立即运行语义。
- 这使得一个新的外围思路变得可疑似可行：预先为 caller thread 建一个 heartbeat automation，并在外部 sidecar 完成时通过外部写入 automation 持久化状态，把该 heartbeat 重新武装到“马上运行”，从而尽量避免固定频率 wake 痕迹。
- 但这条路径目前仍是推断，不是已实证能力；尚未验证 Desktop 是否会对运行中 app 外部改写的 automation 调度状态做及时热感知。
- 进一步从 App bundle 的前端代码字符串里看到了更强的信号：heartbeat automation 在内部对象上直接带有 `targetThreadId`，并且 UI 逻辑会按 `targetThreadId === conversationId` 关联 heartbeat 与具体 thread。
- 同一处还可以看到 heartbeat automation 的目标 chat 选择、`run now`、以及“直接向所选 chat 发送消息而不是在项目/worktree 中运行”的文案，说明“thread-targeted heartbeat automation”是 Desktop 的一等概念，而不是文档层面的抽象。
- 基于这些证据，一个更干净的 Desktop 外围架构浮现出来：不让外部 sidecar 直接改 Codex 的 automation DB；改由一个固定的 bridge automation thread 周期性检查外部长任务状态，再用 `automation_update` 为 caller thread 创建、更新、暂停或删除 heartbeat automation。
- 这条 bridge 架构的关键优点是：caller thread 不需要固定频率 wake，只在 bridge 判定任务 ready 后才被重新武装；周期性检查痕迹被集中在 bridge thread，而不是污染所有 caller thread。
- 仍需注意：即使不外改 Codex DB，bridge thread 与外部 sidecar 之间依然需要一个共享状态面，例如本地文件、socket、CLI helper，或 sidecar 自己的 store；这里只是避免去碰 Codex 自己的 automation 持久化层。

## Bridge-thread PoC 结论

- 通过 `automation_update` 可以直接在一个 thread 中创建 heartbeat automation，并把 `target_thread_id` 指向另一个现有 thread。
- 实测创建 `poc-foreign-heartbeat` 时，即使请求 `status = "PAUSED"`，实际落盘仍为 `ACTIVE`；这说明 tool / app 在 heartbeat 创建时可能会强制激活，后续设计需要把这一点当成实现细节风险。
- 用户提供的 thread `019db5e6-ba6a-7b80-95d2-a6867163281a` 确认使用 `gpt-5.3-codex-spark-preview` + `low`，并成功作为 bridge heartbeat thread 运行。
- 第一轮 bridge PoC `poc-bridge-armer` 证明：heartbeat turn 内部确实可以调用 `automation_update` 和 `automation_delete`。该 turn 把当前 automation 自身重写成指向 caller thread 的 heartbeat，再把自己删除，最终回复 `BRIDGE_ARMED`。
- 第二轮 bridge PoC `poc-bridge-retarget` 证明了完整闭环：
  - bridge thread 在 heartbeat turn 中调用 `automation_update`
  - 将当前 automation 的 `target_thread_id` 从 bridge thread 改写为 caller thread `019db49a-de4e-7d61-93ab-5d70a8905cc3`
  - 同时更新名称和 prompt
  - 下一分钟 caller thread 的 rollout 中出现新的 heartbeat turn，assistant 最终回复 `HEARTBEAT_POC_RETARGETED`
- 这说明“固定 bridge automation thread 监控状态，再用内建 `automation_update` 去 schedule caller thread heartbeat”在 Desktop 上是可行架构，不需要外部直接改 Codex automation DB。

## 当前方案收敛

- Desktop 方向的技术方案已经收敛为：
  - 外部 `sidecar supervisor` 跑长任务
  - 共享 `job state` 作为 bridge / caller 读取面
  - 固定 `bridge heartbeat thread` 负责轮询可投递 thread / batch
  - bridge 通过内建 `automation_update` 为 caller thread 创建、激活、更新或重定向 heartbeat
  - caller thread 被唤醒后读取只读 inbox snapshot、消费结果、继续原任务
- 该方案不依赖：
  - 外部 live push 当前 Desktop thread
  - 外部直接改 Codex automation DB
  - 后台 heartbeat 稳定执行本地 CLI
  - notification thread
- 独立设计文档已记录在：
  - `docs/DESKTOP_BACKGROUND_TASK_BRIDGE_DESIGN.md`

## CLI 补充结论

- 当前 CLI TUI 不能直接复用 Desktop 的 `automation_update` / heartbeat bridge 方案。
- 这不是推断，而是 TUI 自身对 `ServerRequest::DynamicToolCall` 直接返回 unsupported；对应单测名字就是 `rejects_dynamic_tool_calls_as_unsupported`，文案为 `Dynamic tool calls are not available in TUI yet.`。
- 因此，CLI 方向仍应以 `wrapper + shared app-server + sidecar` 作为主方案。
- 但 reviewer 指出了一个重要约束：这条路线实际建立在实验 RPC 上，因此文档已进一步收口为“必须 capability probe + fail-closed”，不能把当前 PoC 可用的所有 RPC 都当成长期稳定契约。
- 已新增一个最小 CLI 正向 PoC：
  - `scripts/cli_shared_app_server_poc.mjs`
  - 使用本机安装的 `codex app-server --listen ws://127.0.0.1:4311`
  - 由脚本同时模拟 frontend client 与 sidecar client
  - frontend 先创建 thread 并完成一个 seed turn
  - sidecar 随后对同一 thread 执行 `thread/resume + turn/start`
  - frontend 成功收到 sidecar turn 的 `turn/started` 与 `turn/completed` 通知
  - `thread/read` 中也能看到 sidecar marker
- 本次实测结果：
  - `thread_id = 019db60d-97d8-7e73-b992-afe8073d7fe6`
  - `frontend_seed_turn_id = 019db60d-97f2-7742-90ec-dfdf2c6e9436`
  - `sidecar_turn_id = 019db60d-a9bd-7ad1-a372-931865554a89`
  - `frontend_saw_turn_started = true`
  - `frontend_saw_turn_completed = true`
  - `sidecar_turn_status = completed`
  - `marker_visible_in_thread_read = true`
- 这说明 CLI wrapper 路线的关键前提已经被正向验证：在共享 `app-server` 模式下，第二个 sidecar client 可以续跑同一个 live thread，而且前台 client 会感知到对应 thread 事件。
- 又补做了一次真实 PTY 级别的前台 TUI 实测，而不只是协议层脚本模拟。
- 实测方式：
  - 启动共享 `codex app-server --listen ws://127.0.0.1:4312`
  - 前台启动真实 `codex --remote ws://127.0.0.1:4312 --no-alt-screen -C /Users/hoteng/Program/GitHub/codex-background-task-handler`
  - 先在前台 TUI 中完成 seed turn，marker 为 `TUI_SEED_MARKER_20260422`
  - 再由外部 sidecar client 对同一 thread 发起第二轮 turn，marker 为 `TUI_SIDECAR_MARKER_20260422`
- 对应 thread 为 `019db614-1fb7-70a3-956f-7a96c48f0226`。
- PTY 输出中确实出现了 sidecar 触发的第二轮用户输入与 assistant 最终结果 `TUI_SIDECAR_MARKER_20260422`。
- 因此，CLI 方向的结论已经不只是“协议层上的前台 client 会收到通知”，而是“真实前台 TUI 会把 sidecar 触发的新 turn 展示给用户”。
- 独立设计文档已记录在：
  - `docs/CLI_SHARED_APP_SERVER_SIDECAR_DESIGN.md`
- 又补做了 active-turn `turn/steer` 的定点 PoC，用来验证“caller thread 还在 running 时，sidecar 能否把同一个 turn steer 下去，而且不会让 turn 提前结束”。
- 新脚本位于：
  - `scripts/cli_turn_steer_poc.mjs`
- PoC 通过共享 `codex app-server` 启动一个 regular turn，并明确让模型使用 shell 执行 `sleep 10`。
- 在该 turn 仍处于 active 状态时，第二个 sidecar client 对同一 thread 调用 `turn/steer`。
- 第二轮实测结果：
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
- 这说明：
  - sidecar 对 active caller turn 使用 `turn/steer` 时，server 接受的是同一个 `turn_id`
  - steer 不会额外开启一个新 turn
  - steer 之后原 turn 继续运行并正常 `turn/completed`
  - steer 内容会被最终 assistant 结果吸收
- 但 reviewer 也指出：当前 steer PoC 只覆盖了无审批、无网络、固定 `sleep 10` 的窄场景，因此设计已进一步收口为“`turn/steer` 只作为只读、低风险投递场景下的可选优化”，而不是一般性的默认主路径。
- 第一轮同类实测还从 rollout 侧拿到了更底层的补充证据：
  - 模型实际发起了 `exec_command: sleep 10`
  - steer 文本作为新的 user message 被写入同一个 rollout
  - 最终 `agent_message` 与 `task_complete.last_agent_message` 都是 `CLI_TURN_STEER_APPLIED_MARKER_20260422`
