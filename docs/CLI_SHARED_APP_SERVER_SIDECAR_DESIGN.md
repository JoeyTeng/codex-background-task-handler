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
codex app-server --listen ws://127.0.0.1:<port>
```

第一版安全边界必须再加两条：

- 只绑定 loopback 地址，不允许监听非本机可达地址。
- shared `app-server` 进程必须由 daemon 持有，且只暴露一个随机本地端口。

2. `foreground TUI`
   - 使用原生 Codex TUI，通过 `--remote` 连接共享 `app-server`：

```text
codex --remote ws://127.0.0.1:<port>
```

3. `sidecar client`
   - 与前台 TUI 并列，作为第二个 websocket client 连接同一个 `app-server`。
   - 负责等待 CI、等待 reviewer、等待外部系统结果、以及在结果 ready 后恢复 caller thread。

4. `optional shared job state`
   - sidecar 不直接从外部任务脚本拿结果，而是消费共享核心里的 thread-scoped queue / delivery batch。
   - 但就“第二个 client 能否续跑同一 live thread”这一核心能力而言，不需要 heartbeat 或 Desktop 那套 bridge 结构。

5. `managed session binding`
   - 第一版一个 managed CLI session 只承诺一个固定的 caller thread。
   - daemon 必须 durable 维护：
     - `managed_session_id`
     - `bound_thread_id`
     - `session_allows_approval`
     - `session_allows_network`
     - `session_allows_write_access`
     - `startup_session_allows_approval`
     - `startup_session_allows_network`
     - `startup_session_allows_write_access`
     - `startup_permission_snapshot_json`
     - `last_permission_snapshot_json`
     - `permission_snapshot_revision`
     - `session_state`
       - `live`
       - `detached`
       - `parked`
       - `stale`
       - `retired`
   - 这三个 `session_allows_*` 字段表示当前可用于 gate 的 effective session risk profile：
     - explicit `true` / `false` 在 bootstrap / attach-or-create 时直接写入
     - default `auto` 先以 fail-closed profile 启动，随后由 sidecar 从可信 `thread/resume` permission snapshot 推导并 durable 更新
     - `startup_session_allows_*` 与 `startup_permission_snapshot_json` 记录启动后第一次可信 auto snapshot，是后续自动投递的风险上限
     - activity/capability/current permission proof invalidation 不得清空 startup permission cap；同一前台 managed session 后续 resync 仍受原 startup snapshot 限制
     - detached auto-delivery 的 strict-safe path 只能在当前 effective 三者都为 `false` 时开启；`trusted-all` 可以绕过该 gate，但仍必须携带或记录 auto permission snapshot
     - 任一 auto 维度无法可信解析时都必须 fail-closed 到 manual/operator path
   - attach/reuse 时如果调用方请求的 session profile 与 durable profile 不一致：
     - 不得原地改写现有 profile
     - 如果旧 session 仍有 active foreground client、未收口 accepted attempt、或其他未解决 delivery work，则必须 fail-closed 为 `session_profile_mismatch`
     - 只有在旧 session 已满足 retirement 条件后，daemon 才允许 retire 旧 session，并创建新的 `managed_session_id`
   - 第一版不再尝试从 shared `app-server` 的事件流里自动归因“哪个 turn 来自前台 TUI”：
     - 当前上游 surface 没有 per-client identity / source attribution
     - 因此 daemon 不能可靠地靠被动观察事件流来推断 `bound_thread_id`
   - 因此第一版的 managed-session bootstrap 也必须收口为两个显式入口：
     - existing-thread mode：
       - 启动时由 `cbth cli run --bind-thread-id <thread_id>` 显式建立 `bound_thread_id`
     - fresh-thread mode：
       - 仅当 capability probe 证明 `thread/start` 可用时，允许 `cbth cli run --new-thread`
       - daemon 先创建一个新 thread，再把返回的 `thread_id` durable 绑定成新的 `bound_thread_id`
   - 不提供 late-bind stable surface
   - 不提供依赖 `managed_session_id` 的外部发现/回填合同
   - 如果调用方既拿不到 caller `thread_id`，也没有 `thread/start` 能力：
     - 该前台会话仍可作为探索性 remote TUI 使用
     - 但它不进入 v1 的 managed-session auto-continuation 合同
   - 这个启动时显式 bootstrap 在 v1 只决定 delivery target：
     - 它不证明前台当前正在看的 thread 一定等于 `bound_thread_id`
     - 它也不要求 `cbth` 能从 app-server 侧可靠读出“当前 foreground thread id”
   - 因此，第一版的 fixed-thread 合同是“投递目标固定”，不是“前台焦点已校验”。
   - 同一个 `bound_thread_id` 在任意时刻最多只允许一个 non-retired managed session：
     - `cbth cli run --bind-thread-id <thread_id>` 必须是 attach-or-create，而不是 blind create
     - 如果 daemon 已经找到同一个 `bound_thread_id` 的 `live` / `detached` session，就必须复用它
     - 如果找到的是 `stale` session：
       - 且它仍有 active foreground client、未收口 accepted attempt、或未清空的 delivery work，则必须 fail-closed 为 `session_conflict` / `stale_session_pending_resolution`
       - 只有在它已经满足 retirement 条件后，daemon 才允许先把旧 session 标为 `retired`，再创建新的 managed session
   - 因此，`managed_session_id` 只在“同一个仍被复用的逻辑 session”内稳定：
     - attach/recover 到同一 logical session 时不变
     - 旧 session 一旦被 `retired`，后续再为同一个 `bound_thread_id` 新建 managed session 时，必须分配新的 `managed_session_id`
   - 一旦 durable 建立，`bound_thread_id` 就代表这条 managed session 的自动续跑目标 thread。
   - 第一版不承诺在同一 managed session 里自动追踪前台 TUI 的 thread 切换，也不承诺自动把 delivery retarget 到别的 thread。
   - 如果用户想把自动续跑目标换到另一个 thread，必须：
     - 显式启动一个新的 managed session
     - 或等待未来版本引入明确的 rebind contract
   - 这意味着第一版也不承诺一个 managed session 同时自动续跑多个 foreground-active threads。
   - 如果用户在同一个前台 TUI 里临时切到别的 thread：
     - daemon 仍只会把 ready batch 投递回 `bound_thread_id`
     - 只有当用户自己把前台停留在 `bound_thread_id` 上时，才复用已经验证过的 live-visibility 行为
     - 第一版不验证、也不强制前台当前正在看的 thread 一定等于 `bound_thread_id`
     - 是否恰好在另一个 thread 里看到 sidecar delivery，不属于第一版合同
   - 一旦某个 `bound_thread_id` 上的 attempt 已经 accepted，并 durable 记录了 `delivery_turn_id`：
     - 它仍允许等待自己匹配的 `turn/completed` 并正常 close
     - 不需要因为前台 UI 临时切到别的 thread 就强行中止或重开这次已 accepted 的投递

## 本地信任边界

- 上游 `app-server` 现在已经支持 websocket auth：
  - `--ws-auth capability-token --ws-token-file <path>`
  - `--ws-auth capability-token --ws-token-sha256 <hex>`
  - `--ws-auth signed-bearer-token --ws-shared-secret-file <path>`
- 上游 `codex --remote` 也支持：
  - `--remote-auth-token-env <ENV_VAR>`
- 但截至本机 `codex-cli 0.123.0`，`codex app-server --help` 仍把 `--ws-auth` 明确标成“for non-loopback listeners”。
- 这意味着第一版在 `127.0.0.1` / `localhost` 上的 shared `app-server` 目前不能把 bearer-token auth 当成既有能力来依赖。
- 因此，第一版不能把 loopback listener 描述成真正的本地 auth 边界；当前更准确的说法是：
  - shared `app-server` 只监听 `127.0.0.1` / `localhost`
  - daemon 为每个 managed session 选择随机高位本地端口
  - shared `app-server` 只在该 managed session 生命周期内存在
  - 上述措施只是在降低意外暴露面，不是授权机制
- 第一版明确不承诺防御“本机上其他本地进程附着 loopback app-server”。
- 因此 v1 的支持部署前提必须收口为：
  - 专用单用户工作站、单用户开发 VM，或等价的本机隔离环境
  - 不支持多用户共享主机、共享 shell 机、或把“本机上其他进程也不可信”纳入威胁模型的环境
- 更强的 auth 边界有两条未来路径：
  - 上游将 websocket auth 扩展到 loopback listeners
  - 或本项目改成不同的本地 transport / wrapper 形状
- 在那之前，第一版只能把 CLI shared `app-server` 视为：
  - loopback-only
  - unauthenticated local control plane
  - daemon-owned ephemeral process
  - 仅在 dedicated single-user deployment assumption 下支持

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
- `managed_session_id` 与 `session_epoch` 的合同必须是：
  - `managed_session_id`：
    - daemon 为一条逻辑 managed session 创建的稳定 durable id
    - 前台 / sidecar 重新附着到同一个仍存活的 shared `app-server` 时不变
  - `session_epoch`：
    - daemon 为该 managed session 当前可证明连续的 app-server event stream 分配的单调递增序号
    - 当 daemon 首次拉起该 shared `app-server` 时初始化为 `1`
    - 只要 daemon 还能证明自己仍连接到同一个未重建的 shared `app-server` 实例，短暂 websocket 重连不递增
    - 只要发生以下任一情况，就必须递增：
      - shared `app-server` 进程被重启或重建
      - daemon 自己重启后无法证明已恢复到同一个连续 event stream
      - 任何 accepted `delivery_turn_id` 的后续观察连续性无法再证明
- existing-thread mode（`cbth cli run --bind-thread-id <thread_id>` 或 `cbth resume <thread_id>`）的 v1 合同必须是：
  - 先按 `bound_thread_id` 查询是否已经存在 non-retired managed session
  - 如果存在 `live` / `detached` session：
    - 先比较 requested bootstrap profile 与 durable `session_allows_*` effective profile
    - 只有 profile 完全一致时，才允许 attach/reuse
    - 只要存在 profile drift，且旧 session 尚未满足 retirement 条件，就必须 fail-closed 为 `session_profile_mismatch`
    - 只有旧 session 已满足 retirement 条件时，daemon 才允许 retire 它，并创建一个新 profile 的 replacement session
  - 如果存在 `parked` session：
    - `parked` 的统一语义是：
      - 当前 managed session 的 live part 已结束
      - 不再要求 live observation 或自动 delivery
      - 但仍有 unresolved manual batch 等待 operator close / `manual_resolution_expired` auto-close
      - 这个 manual batch 可以来自 accepted attempt fail-closed，也可以来自 pre-accept manual/operator path
    - 且其 unresolved manual batch 仍未终态时：必须 fail-closed 为 `session_pending_manual_resolution`
    - 只有在这些 manual batch 已终态后，daemon 才允许先 retire 这个 `parked` session，再创建新的 managed session
  - 如果存在 `stale` session：只有在它已满足 retirement 条件后才允许替换
  - 如果存在不可安全替换的 `stale` / conflicting session：直接 fail-closed，不得并发创建第二个 session
- fresh-thread mode（`cbth cli run --new-thread`）的 v1 合同必须是：
  - 只在 capability probe 已证明 `thread/start` 可用时允许
  - daemon 必须先创建一个 brand-new thread，并把返回的 `thread_id` durable 绑定为新的 `bound_thread_id`
  - 这个新 `bound_thread_id` 之后同样进入既定的单 session / fixed-thread / attach-or-create 合同
- 当同时满足以下条件时，daemon 才允许清理该 managed session：
  - 没有 active jobs
  - 没有连接中的 foreground client
  - 没有需要继续投递的 sidecar client
  - 没有任何仍绑定到这个 `bound_thread_id` 的 unresolved delivery work
    - 包括 ready / materialized / cooldown batch
    - 包括 `replay_policy=manual_resolution_only`、且尚未被 operator close 或 `manual_resolution_expired` auto-close 的 head batch
  - 没有仍在 `delivery_observation_deadline` 之内等待匹配 `turn/completed` 的 `delivery_turn_id`
- 当 accepted attempt 因 deadline expiry / continuity-loss / untrusted terminal result 而 fail-closed 到 `manual_resolution_only` 后：
  - daemon 可以结束该 managed session 的 live 部分
    - 关闭 daemon-owned shared `app-server`
    - 断开对应 sidecar / foreground attachment
  - 但 durable session record 不得立刻 `retired`
  - 它必须先转入 `session_state=parked`
    - 继续保留 `bound_thread_id`
    - 保留 manual/operator 收口所需的 durable 证据
    - 不再允许自动 delivery 或 live observation
  - 只要仍有 unresolved manual batch，`parked` session 就不得被 attach/reuse 成 live session
    - 必须 fail-closed 为 `session_pending_manual_resolution`
  - 只有当这些 manual batch 已被 operator close 或 `manual_resolution_expired` auto-close 后，`parked` session 才允许 retirement / replace

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
- 如果缺少权威 current-state sync 面（例如 `thread/read` 或等价 surface），CLI adapter 也必须 fail-closed：
  - 没有这个能力时，v1 不支持 detached managed-session auto-continuation
  - 最多只允许用户手工跑一个 foreground `codex --remote` 会话，而不进入 daemon-owned managed-session 合同
  - 这个 current-state sync 至少必须能对 `bound_thread_id` 返回：
    - 是否存在 active regular turn
    - 若存在则返回 `active_turn_id`
    - 足以把 `activity_state=unknown` 收敛到 `active` 或 `idle` 的权威状态
- 如果缺少 accepted-turn 观察面，CLI adapter 也必须 fail-closed：
  - 最小能力集不只包括 `turn/completed`
  - 还必须能对当前 `delivery_turn_id` 观察并 durable 区分以下 canonical 事件：
    - `turn_started`
    - `turn_completed`
    - `turn_failed`
    - `turn_interrupted`
    - `turn_replaced`
  - 缺少这些负终态观察面时，v1 不得宣称自己能安全收口 accepted delivery
- `thread/start` 仍然不是 existing-thread resume 模式的最小必需能力。
- 但如果要支持 `cbth cli run --new-thread` 这种 fresh-thread bootstrap，capability probe 还必须额外证明 `thread/start` 可用。
- 如果缺少 `turn/steer`，CLI adapter 仍可工作，但只能在 caller idle 时投递。
- 不能把 PoC 中碰巧可用的实验 RPC 直接当成长期稳定契约。
- 第一版 shipping 配置默认关闭 `turn/steer`，直到 active-turn 分类与安全门槛被实证支持；未来合同收敛在 [CLI_ACTIVE_TURN_STEER_DESIGN.md](CLI_ACTIVE_TURN_STEER_DESIGN.md)。

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
- 但这次 PTY 级别实测走的是无认证 loopback 路径；而这也正是当前本机 `codex-cli 0.123.0` 在 loopback listener 上真实可用的上游能力边界。

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

### 默认策略与 explicit trusted-all escape hatch

- 同一 `source_thread_id` 只允许一个 in-flight delivery attempt。
- ready jobs 先进入 thread-scoped FIFO 队列。
- daemon 对同一 thread 上的相邻 jobs 做 batch 合并。
- 默认 `strict_safe` 自动投递的前提要先过共享核心的安全门槛：
  - `delivery_read_only=true`
  - `delivery_requires_approval=false`
  - `delivery_requires_network=false`
  - `delivery_requires_write_access=false`
- 默认 `strict_safe` 还要求 managed session 自身满足当前 effective detached auto-continuation profile：
  - `session_allows_approval=false`
  - `session_allows_network=false`
  - `session_allows_write_access=false`
- 如果这些字段来自 default `auto`，sidecar 必须已经从 `thread/resume` 记录启动 permission snapshot；自动 `turn/start` 前还必须刷新当前 snapshot，并逐维使用 `startup && current` 得到 pinned effective 权限。
- 当前权限收紧时使用当前更紧权限继续投递；当前权限放宽时仍以启动 snapshot 为上限；混合变化逐维取更紧值。任何 startup/current drift 都要写 stderr warning 和 audit record。
- `trusted_all` 是显式 opt-in broad escape hatch：
  - 由 `cbth cli run --auto-delivery-policy trusted-all` 或 `cbth resume --auto-delivery-policy trusted-all` 开启
  - 会绕过 batch policy gates、artifact-read gate 与 session risk-profile gates
  - 仍要求 current head batch、remaining budget、matching `source_thread_id`、bound `managed_session_id`、matching `session_epoch`、fresh idle proof
  - 如果 permission 参数为 `auto`，仍要记录 startup/current/effective snapshot，并在 `turn/start` 上携带 pinned `approvalPolicy` / `sandboxPolicy`
  - 仍不开放 `turn/steer` 或 active-turn injection
- 不满足 `strict_safe` 条件且未显式选择 `trusted_all` 的 batch 不得自动 `turn/start`；它们保留为 operator/manual 路径。
- 默认只在 caller thread idle 时使用：

```text
thread/resume + turn/start
```

- 无论 `turn/start` 还是 `turn/steer`，第一版都必须把“准备发送 side-effectful RPC”、“RPC 被 server 接受”与“batch 已成功送达”分开建模。
- 在调用 `turn/start` / `turn/steer` 之前，CLI adapter 必须先 durable 写入一个 accept-pending barrier：
  - `delivery_rpc_request_id`
  - `delivery_rpc_kind=turn_start|turn_steer`
  - `delivery_rpc_started_at`
  - `delivery_rpc_state=pending_acceptance`
  - `delivery_rpc_correlation_marker`
- `delivery_rpc_correlation_marker` 必须随 RPC 一起进入 app-server 可观察输入：
  - 如果上游 RPC 支持 opaque idempotency / metadata，就用协议字段承载
  - 否则把一个短的 CBTH marker 放进投递给 caller 的 continuation prompt
  - marker 只用于本地相关性判定，不得携带 artifact payload 或敏感内容
- 只要 attempt 已经进入 `accept_pending` / `delivery_rpc_state=pending_acceptance`，它就不再是普通 pre-accept retry：
  - adapter 不得因为 daemon 崩溃、websocket 断开、或 response 丢失而直接重新发送同一个 batch
  - 下一次 sweep 必须先做 accepted/unknown reconciliation
- 如果 RPC 在同一进程、同一 `managed_session_id + session_epoch` 内明确返回“未被接受”的 benign reject，例如 idle race 或 non-steerable active turn，才允许把该 attempt 恢复为 retry-on-idle 或重新排队。
  - 当前实现状态迁移是 `accept_pending -> abandoned`
  - 必须先 durable 写入 `delivery_rpc_state=rejected_before_accept`
  - `delivery_turn_id` 必须保持为空
  - `delivery_attempt_count` 不得递增
  - 下一次重试必须使用新的 `delivery_rpc_request_id + delivery_rpc_correlation_marker`
- 如果 RPC response 丢失，但 daemon 能在同一连续 event/current-state 面里正向证明同一个 `delivery_rpc_correlation_marker` 已经被接入且只接入一个具体 caller turn，则必须补写 `delivery_turn_id` 并按 accepted attempt 继续观察。
- 如果无法正向证明 accepted，也无法正向证明未 accepted，当前 attempt 必须收敛到 `abandoned`，当前 head batch 必须进入 `replay_policy=manual_resolution_only`，不得 automatic retry。
- 对 CLI 来说，accepted delivery 的第一层语义是：
  - batch 已被接入某个具体 caller turn 的 pending input / input queue
  - 但 batch 还不能因此立刻 `closed`
- 每次 accepted delivery attempt 都必须 durable 记录一个：
  - `delivery_turn_id`
  - idle 路径下来自 `turn/start` 返回的新 turn id
  - steer 路径下来自当前 active regular turn id
- accepted 之后，attempt 进入 `cooldown`；是否真正关闭 batch，要看后续同一个 `delivery_turn_id` 的 turn 结果。
- 因此，`delivery_turn_id` 也必须落进共享核心的 durable attempt schema，而不是只存在于 CLI adapter 的内存里。
- accepted attempt 还必须 durable 绑定：
  - `managed_session_id`
  - `session_epoch`
  - `delivery_accepted_at`
  - `delivery_observation_state=tracking`
  - `delivery_observation_deadline`
  - `last_observed_turn_event=null`
  - `last_observed_turn_event_at=null`
- 其中：
  - `managed_session_id` 是 daemon 为该逻辑 CLI 会话分配的稳定 id
  - `session_epoch` 是这个 managed session 当前“可证明连续的 shared app-server event stream”的单调递增代号
  - app-server 首次启动时从 `1` 开始
  - 前台 / sidecar 重连到同一个仍存活的 app-server 实例时不变
  - app-server 进程被重建，或 daemon 恢复后无法证明 continuity 时必须递增
- 只有在后续仍能证明自己附着在同一个 `managed_session_id + session_epoch` 的事件流上时，CLI adapter 才允许继续等待那个 `delivery_turn_id` 的 `turn/completed`。
- `delivery_observation_deadline` 是 accepted attempt 的硬边界：
  - 由 `delivery_accepted_at + max_turn_observation_window` 推导
  - `max_turn_observation_window` 必须显式大于 daemon 的 `idle timeout`
  - deadline 未到时，这个 attempt 属于 daemon 必须继续保活的近端 observation work
  - deadline 到期仍未看到可信 `turn/completed` 时：
    - 当前 attempt 必须收敛到 `abandoned`
    - `delivery_observation_state=expired`
    - 当前 head batch 必须进入 `replay_policy=manual_resolution_only`
    - 之后 daemon 才允许退出，而不是继续无限常驻或静默退出
- `last_observed_turn_event` / `last_observed_turn_event_at` 的 durable 合同必须是：
  - 只记录当前 `delivery_turn_id` 上真实观察到的事件
  - accepted 时初始化为 `null`
  - 后续只允许由同一 `delivery_turn_id` 的观察更新
  - canonical enum 在 v1 固定为：
    - `turn_started`
    - `turn_completed`
    - `turn_failed`
    - `turn_interrupted`
    - `turn_replaced`
  - capability probe 必须证明这些终局观察面可用；否则 detached auto-continuation 必须 fail-closed
- 对每个 managed session，CLI adapter 还必须维护一个独立的 thread activity state：
  - `unknown`
  - `active`
  - `idle`
- 在以下任一时刻，`activity_state` 都必须强制回到 `unknown`，并暂停自动 delivery：
  - daemon / sidecar 首次 attach 到该 managed session
  - `session_epoch` 递增
  - websocket continuity 丢失后重新附着
  - daemon 无法证明自己仍看到同一条连续 event stream
- `unknown` 只能通过权威 current-state sync 或新的本地观测收敛：
  - 如果 adapter 能从 app-server 当前状态同步出“存在 active regular turn”，则转为 `active`
  - 如果 adapter 能从权威状态同步出“当前没有 active regular turn”，则转为 `idle`
  - 这里的 current-state sync 必须是 `bound_thread_id` 级证明，而不是模糊的 session 级“最近没活动”
  - 如果当前实现拿不到这种权威 current-state sync，v1 必须 fail-closed 保持 `unknown`，直到重新观察到一轮本地 regular turn 生命周期并确认其完成
- 只要 `activity_state != idle`，就不得自动发 `thread/resume + turn/start`

### Idle 判定与 race contract

- CLI adapter 的 idle 判定源必须来自 shared `app-server` 的 live event stream，而不是 TUI 屏幕解析。
- 第一版至少使用以下信号维护本地 thread liveness 视图：
  - `turn/started`
  - `turn/completed`
  - `thread/status/changed`（如果可用）
- 一个 thread 只有在以下条件同时满足时，才允许被本地视图判为 idle：
  - 当前 `activity_state` 已经不是 `unknown`
  - 最近一次已观察到的 regular turn 已完成/中断
  - 且在那之后没有新的 `turn/started`
- `turn/start` 本身必须被视为最后一道 compare-and-swap：
  - 如果 sidecar 观察到 idle 后发起 `thread/resume + turn/start`
  - 但在请求到达前用户又启动了新 turn
  - adapter 必须把这种失败当成 benign race，而不是成功送达
- benign race 的处理规则：
  - 不得关闭 batch
  - 不得创建第二个并发 attempt
  - 当前 attempt 如果已经进入 `accept_pending`，只能在证明 RPC 未被接受后回到 `prepared`，不得推进到 `cooldown`
  - `last_delivery_attempt_at` 不得因为这次 race 被当成成功投递而更新
  - `delivery_attempt_count` 不得递增
  - 必须清除本地 idle 视图
  - 必须等待下一次 idle 观测后再重试
- 换句话说，CLI 第一版的串行化依赖：
  - daemon 的 per-thread attempt lease
  - app-server 的 live event stream
  - `turn/start` 失败时的 retry-on-next-idle 约束
- idle 路径的 batch close 语义必须是：
  - `turn/start` 被接受时：只记录 `delivery_turn_id`，attempt 进入 `cooldown`
  - 只有当同一个 `delivery_turn_id` 的 `turn/completed` 被观察到，且以下条件同时满足时，batch 才允许关闭为 `close_reason=delivered`
    - 该 attempt 仍然是当前 generation/head delivery
    - 事件自身的 `observed_at < delivery_observation_deadline`
    - 正常路径要求 `delivery_observation_state=tracking` 且 `replay_policy=automatic`
    - 如果 daemon startup sweep 先在 session continuity 仍有效时把同一 attempt 过期，但随后收到的 `turn/completed` 事件证明实际完成时间仍早于原 deadline，允许按 delayed on-time evidence 修正为 `delivered`
    - 如果后续 `cli session bind` / re-attach 已经把同一 managed session 的仍 open attempt 收敛到 `abandoned`，不得再按 delayed on-time evidence 修正为 `delivered`
  - 一旦事件自身 `observed_at >= delivery_observation_deadline`：
    - `turn/completed` 只能记录为 operator/debug 证据
    - 不得再自动关闭为 `delivered`
  - 如果该 turn 在被接受之后又失败、中断、被替换，或 batch 已被 supersede，则不得因为早先的 `turn/start` 接受而关闭 batch
  - 对这类“accepted 后又变得不可信”的 turn，第一版不得自动 replay；当前 attempt 必须收敛到 `abandoned`，当前 head batch 必须进入 `replay_policy=manual_resolution_only`
- 如果 websocket / daemon / app-server continuity 在 accepted 之后丢失：
  - 且无法再证明自己回到了同一个 `managed_session_id + session_epoch`
  - 当前 attempt 必须切到 `delivery_observation_state=abandoned`
  - 当前 head batch 必须进入 `replay_policy=manual_resolution_only`
  - 第一版不得靠“重投一次看看”来猜原 turn 是否已经产生副作用
- continuity-loss 之后的最小 operator-resolution flow 必须是：
  - 先运行：

```text
cbth batch inspect-head --source-thread-id <thread_id> --json
```

  - 读取 durable 证据：
    - `delivery_turn_id`
    - `managed_session_id`
    - `session_epoch`
    - `delivery_observation_state`
    - `delivery_observation_deadline`
    - `delivery_accepted_at`
    - `last_observed_turn_event`
    - `last_observed_turn_event_at`
  - 再结合外部可见证据（例如 thread history / rollout /人工确认）做二选一收口：

```text
cbth batch close-head --source-thread-id <thread_id> --reason operator_confirmed_delivery --json
cbth batch close-head --source-thread-id <thread_id> --reason operator_closed_unconfirmed --json
```

  - 第一版不提供 continuity-loss 后的自动 replay。

### `turn/steer` 的策略

- `turn/steer` 是可选优化，不是默认主路径。
- 第一版默认 shipping 配置中应当关闭。
- 只有在以下条件同时满足时才允许使用：
  - capability probe 明确支持
  - 当前 caller turn 仍处于 active regular turn
  - 当前 active turn 的风险视图明确是 `read_only_low_risk`
  - 当前 active turn 的：
    - `active_turn_requires_approval=false`
    - `active_turn_requires_network=false`
    - `active_turn_requires_write_access=false`
  - 当前 batch 的 `delivery_read_only=true`
  - 当前 batch 的 `delivery_requires_approval=false`
  - 当前 batch 的 `delivery_requires_network=false`
  - 当前 batch 的 `delivery_requires_write_access=false`
  - 当前 delivery 在 CLI adapter 的本地 steer policy 下被判定为 steer-eligible
  - 当前 batch 的 `inline_payload_bytes` 没超过 CLI adapter 的 steer 上限
  - 当前 thread 未触发最小连续发送间隔限制
- 不满足上述任一条件时，batch 保持排队，等 caller idle 后再 `turn/start`。
- 由于第一版自动续跑整体都只允许只读 batch，上面的 `turn/steer` 安全门槛其实是对默认总门槛的进一步细化，而不是独立例外。
- steer 路径的 delivery contract 要额外参考现有 TUI 语义：
  - accepted steer 表示新输入被并入当前 active regular turn 的 pending input
  - non-steerable turn 不算成功送达，而是必须像 TUI 一样回落到 queued-follow-up 语义
  - race / expected-turn mismatch 只能重试或回退，不能直接 close batch
- 如果当前 active turn 自己的风险分类无法确定，或其 delivery profile 不是 `read_only_low_risk`，则即使 batch 本身是只读，也不得 steer。
- 因此，steer 路径的 batch close 语义必须是：
  - `turn/steer` 被接受时：只记录当前 `delivery_turn_id = active_turn_id`，attempt 进入 `cooldown`
  - 只有当同一个 `delivery_turn_id` 之后出现 `turn/completed`，且以下条件同时满足时，batch 才允许关闭
    - 该 attempt 仍是当前 head delivery
    - 事件自身的 `observed_at < delivery_observation_deadline`
    - 正常路径要求 `delivery_observation_state=tracking` 且 `replay_policy=automatic`
    - 如果 daemon startup sweep 先在 session continuity 仍有效时把同一 attempt 过期，但随后收到的 `turn/completed` 事件证明实际完成时间仍早于原 deadline，允许按 delayed on-time evidence 修正为 `delivered`
    - 如果后续 `cli session bind` / re-attach 已经把同一 managed session 的仍 open attempt 收敛到 `abandoned`，不得再按 delayed on-time evidence 修正为 `delivered`
  - 一旦事件自身 `observed_at >= delivery_observation_deadline`：
    - `turn/completed` 只能记录为 operator/debug 证据
    - 不得再自动关闭为 `delivered`
  - 如果 steer 在被接受之前就被拒绝为 non-steerable，batch 不得关闭，必须继续走 queued / retry-on-idle 流程
  - 如果 steer 已被接受，但当前 turn 之后失败、中断、被替换，或观察连续性丢失，则 batch 同样不得关闭，且必须 fail-closed 到 `replay_policy=manual_resolution_only`

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
   - 不依赖 loopback websocket auth
3. 启动时对实验 RPC 做 capability probe
4. CLI bootstrap 必须先选定显式模式：
   - existing-thread mode：`cbth cli run --bind-thread-id <thread_id>`
   - native resume mode：`cbth resume <thread_id> [-- <codex_args>]`，前台进程为 `codex resume <thread_id> --remote <url> --cd <cwd> ...`
   - fresh-thread mode：`cbth cli run --new-thread`（仅在 `thread/start` capability 已通过 probe 时允许）
5. attach-or-create / create-new 必须遵守：
   - existing-thread mode：
     - 同一 `bound_thread_id` 有 `live` / `detached` session 时，先比较 requested bootstrap profile 与 durable effective profile；只有完全一致才允许 attach/reuse
     - 如果 profile drift 且旧 session 仍未满足 retirement 条件：fail-closed 为 `session_profile_mismatch`
     - 同一 `bound_thread_id` 有 `parked` session 且 unresolved manual batch 尚未终态时：fail-closed 为 `session_pending_manual_resolution`
     - 同一 `bound_thread_id` 有不可安全替换的 stale/conflicting session 时 fail-closed
     - 只有在没有 non-retired session，或旧 `parked/stale` session 已满足 retirement 条件时，才创建新的 managed session
   - fresh-thread mode：
     - daemon 必须先调用 `thread/start` 创建 brand-new thread
     - 再把返回的 `thread_id` durable 绑定为新的 `bound_thread_id`
     - 同时仍适用同一 `bound_thread_id` 最多一个 non-retired managed session 的唯一性合同
6. attach/create 成功后再启动前台 `codex --remote ...`
7. sidecar 从共享核心读取 per-thread `delivery batch`
8. 任务 ready 后：
   - 默认只在 caller thread idle 时：`thread/resume + turn/start`
   - 只有显式打开 steer feature flag，且 `turn/steer` 能力存在并满足只读/低风险策略时：允许 steer
9. 如果能力不足或协议形状漂移：fail-closed，不做自动续跑
10. 如果用户手动让前台停留在 `bound_thread_id`，TUI 就会通过该 thread 的已有订阅自然感知新 turn
   - 但 v1 不负责证明或强制这件事始终成立

v1 范围外：

- 通过后续 `cbth cli bind ...` 对已有 session 做 late-bind
- 通过 `managed_session_id` 外部发现/回填一个尚未 fixed-thread bootstrap 的 session
- 在同一 managed session 里自动重绑定到新的 caller thread

## 当前实现状态

- public `cbth cli run --bind-thread-id <thread_id>` 已落地 existing-thread 最小进程模型：
  - foreground wrapper 会先向 daemon 获取 `bound_thread_id` 级短 reservation，再执行 session bind，避免重复启动在失败前 bump 现有 managed session epoch。
  - 先复用 hidden `cli session bind` 建立 / attach durable managed session。
  - 再通过 daemon-owned `cli_app_server_ensure` 启动 `codex app-server --listen ws://127.0.0.1:0`，并只接受 `127.0.0.1` / `localhost` listener。
  - foreground wrapper 使用 `codex --remote <url> --cd <current_dir> ...` 暴露原生 Codex CLI/TUI 体验。
  - foreground wrapper 持有短 app-server lease，并在运行期间定期 refresh；如果 wrapper crash，daemon 会在 lease TTL 到期后 kill/wait app-server 并 join stdout/stderr drain worker。
  - foreground 退出时会显式 stop 当前 lease 下的 daemon-owned app-server。
  - daemon 对已注册 managed app-server 的 cleanup 是 fail-closed：explicit stop、refresh 发现 child 已退出、以及 lease expiry reaper 都必须先按注册的 `managed_session_id + bound_thread_id` 推进一个 current-proof epoch fence，再从 registry 移除并停止进程；即使 proof 已经 clear，也要推进 epoch，避免 cleanup 与 sidecar 重新写 proof 之间的竞态。如果 proof invalidation 失败，registry entry 保留以便后续重试。registry 里的启动时 `session_epoch`、lease id 和 child pid 只用于确认 registry entry 身份，不能作为 proof invalidation 的目标 epoch，否则 sidecar 断连后推进 epoch 并重新写入 proof 时会让 reaper 永远卡在旧 epoch。
  - 尚未注册进 registry 的候选 app-server 不得触碰 session proof，只能停止候选进程，避免并发 ensure 误清真实 session。
  - daemon shutdown 例外采用 best-effort proof invalidation，然后仍然 kill/wait 子进程并 join drain worker，优先避免 daemon 退出时泄漏 app-server。
- public `cbth cli run --new-thread` 已落地 fresh-thread bootstrap：
  - wrapper 先请求 daemon-owned pending loopback app-server 执行 `thread/start`。
  - bootstrap 成功后，返回的 `thread_id` 被用作后续 fixed-thread `bound_thread_id`，同一个 pending app-server 进程会被提升为 managed app-server，再复用 existing-thread sidecar / foreground 流程。
  - wrapper 只在启动 foreground 前向 stderr 打印 `cbth: bound thread id: <thread_id>`；不发送 foreground prompt，不注入用户消息，也不改变原生 `codex --remote` 交互模型。
  - 不能使用“short-lived bootstrap app-server 后关闭”的实现：Codex 0.128 的 fresh thread 在 first user message 前不会 materialize rollout，关闭 bootstrap app-server 后新的 app-server 会对该 thread 返回 `no rollout found` / `includeTurns is unavailable before first user message`。
  - bootstrap app-server 在 promote 前只存在于 pending registry，不触碰 managed session proof；bootstrap 失败、promote 失败、lease 过期或 daemon shutdown 时必须停止子进程并 join drain worker。
  - bootstrap 的 remote error / timeout / closed / malformed response 都在 foreground 启动前 fail closed，不创建 managed session。
- daemon status 现在会列出 active CLI app-server，并且 daemon capability 列表包含 `cli-app-server-lifecycle` / `cli-app-server-probe`；新 CLI 不会把 lifecycle 或 doctor probe request 投递给不支持该命令的旧 daemon。
- public `cbth resume <thread_id> [-- <codex_args>]` 已作为 native resume wrapper 落地；它复用 fixed-thread managed session / daemon-owned app-server / passive sidecar 流程，前台命令为 `codex resume <thread_id> --remote <url> --cd <cwd> ...`。
- 为避免 cold existing thread 被 sidecar 抢先 materialize，`cbth resume` 的 sidecar 首次 `thread/resume` 会携带从前台 argv 可推导出的 native resume overrides：`cwd`、`model`、`profile`、`approvalPolicy`、`sandbox`、`approvalsReviewer` 和 `persistExtendedHistory`；前台转发参数中的相对 `--cd` 会先按 caller cwd 规范成绝对路径，`--remote` / `--remote-auth-token-env` transport overrides 会被拒绝，避免前台 Codex 连接到非托管 app-server。后续自动投递前的 permission refresh 仍使用只含 `threadId` 的 current-state `thread/resume`。
- durable `cli_managed_sessions` schema 已落地，记录 `managed_session_id`、`bound_thread_id`、`session_epoch`、`session_state`、`activity_state`、`activity_revision`、current effective session risk profile、startup permission snapshot、last permission snapshot 和 timestamps。
- hidden adapter-internal `cbth cli session bind` 已作为 existing-thread attach-or-create building block 落地；它接收 bootstrap risk profile（explicit flags 直接给出，default `auto` 先以 fail-closed false profile 绑定），会复用同一 `bound_thread_id` 上无阻塞工作的 `live` / `detached` session，并在 attach 时递增 `session_epoch`、把 `activity_state` 重置为 `unknown`、把 epoch-local `activity_revision` 重置为 0，同时清空旧 activity/capability/permission proof；profile drift、`parked` 或 `stale` 旧 session 只有在满足 retirement 条件后才会先 retire 再 replacement。
- operator-facing `cbth cli session list` / `inspect` / `retire` 已落地；retire 拒绝 `live` session、active delivery attempt、以及仍有 open `manual_resolution_only` head batch 的 bound thread。
- hidden adapter-internal `cbth cli session note-activity` 已作为 current-state sync 的临时 durable 写入面落地；当前只允许同 epoch 的 `live` / `detached` session 通过严格顺序递增的 `activity_revision` 被标成 `active` 或 `idle`，同 revision 只允许完全相同状态的幂等重放。
- hidden adapter-internal `cbth cli session note-capabilities` 已作为 epoch-local capability probe 写入面落地；每次 bind / re-attach / continuity-loss fence 都会清空旧 capability proof。
- hidden adapter-internal `cbth cli session note-permissions` 已作为 epoch-local permission snapshot 写入面落地；auto 模式从 `thread/resume.approvalPolicy` 与 `thread/resume.sandbox` 派生 current permissions，首次可信 snapshot pin 为 startup，上游缺字段、未知 approval policy、未知 sandbox type 或不可信字段形状都会 fail-closed。
- default passive auto session 在 `thread/resume` 缺少权限字段时仍可继续记录 activity/capability current-state proof；缺失 snapshot 只阻止自动投递，直到后续 refresh 获得可信权限快照。混合 explicit/auto bind 只放宽 auto 维度，显式 `true` / `false` 维度仍参与 profile drift 检查。
- `begin-cli-accept` 已经要求引用一个匹配当前 batch `source_thread_id`、`session_epoch`、state、no-approval / no-network / no-write profile，且 `turn_start` 时同时具备最小 capability proof 和 `activity_state=idle` 的 managed session；不再接受任意字符串形式的 `managed_session_id`。
- 当前最小 capability proof 要求 adapter 已证明 `turn_start`、`current_state_sync`、`turn_completed_event`、负终态 observation surface，以及 `thread_resume` 或 fresh `thread_start` 中至少一种 caller-thread attachment proof；缺任一项都会 fail-closed。
- `cbth cli run` 已启动 wrapper-owned sidecar client：
  - sidecar 连接 daemon-owned loopback app-server，执行 initialize / `thread/resume` / `thread/read(includeTurns=true)`；fresh unmaterialized `--new-thread` 初始连接允许 `thread/resume` 返回 `no rollout found`，随后用同一 app-server 的 `thread/read(includeTurns=false)` 建立 `thread_start + current_state_sync` idle proof
  - foreground Codex 退出时，wrapper 会先停止 / join passive sidecar，并用 sidecar 的最新 epoch/revision 清空 activity / capability proof；之后再停止 daemon-owned app-server，避免 event stream 已结束后旧 `idle` proof 继续打开自动投递
  - 初始 `thread/resume` / `thread/read` 结果会同步成当前 epoch 的 `idle` / `active` activity proof；成功返回且 `thread.id` 匹配 `bound_thread_id` 的 `thread.status.type` authoritative snapshot 会支配该 request response 前已消费的 stale notification，缺少 status 时才 fallback 到 turns tail
  - `turn/started`、`turn/completed` 与 `thread/status/changed` notification 会继续推进 monotonic `activity_revision`；`completed` / `failed` / `interrupted` / `replaced` 都视为 terminal turn status；activity 写入失败会先 invalidate 当前 proof，再把错误交回 adapter retry loop
  - request 等待窗口内收到的 lifecycle notification 不会覆盖后续 authoritative snapshot；如果 `thread/read` 缺少 authoritative snapshot 但 `thread/resume` 已给出可信 current-state snapshot，则会按顺序 replay `thread/resume` / `thread/read` 两个 request window 的 notification，避免丢失 resume 响应前到达的 active 信号；如果 `thread/read` 超时、protocol / decode / closed / remote-error 等失败，则关闭本轮连接并 fail-closed retry，避免复用可能存在 outstanding response 或已失同步的 websocket；如果 snapshot 缺失导致 epoch invalidation，则旧 epoch 的 buffered notification 不会写回新 epoch
  - default `auto` permission path 要求 existing-thread `thread/resume` 返回可信 permission snapshot；fresh unmaterialized `--new-thread` 在 first user message 前无法从 `thread/resume` 取到 snapshot 时不会记录 auto proof，但默认 passive activity/capability proof 仍可建立，自动投递必须等可信 permission snapshot 后才允许
  - 自动 `turn/start` 前会重新执行 `thread/resume`，以 startup snapshot 为上限计算 effective permissions，并在 request params 中携带对应的 pinned `approvalPolicy` / legacy `sandboxPolicy`；pinned legacy sandbox 只发送 Codex 0.128 接受的 `type`、`networkAccess`、`writableRoots` 与 workspace exclude flags，`access` / `readOnlyAccess` restricted-read shape 只用于解析、收紧计算和 drift/audit，不写入 `turn/start`；workspace writable root containment 先做安全规范化，含 `..` 的 root fail-closed；权限 drift 会同时比较派生布尔值与 raw `approvalPolicy` / `sandbox` 细节，并输出 warning / 写 audit record；auto-pinned session 的 proof invalidation 会保留 startup cap，但 strict-safe 投递必须等 current permission snapshot 重新刷新后才能使用记录的风险布尔值；默认 `auto` bind reattach 不把 fail-closed 初始 false 当作固定 profile，因此不会因为上一轮 auto-derived effective profile 为 true 而触发 profile-drift replacement
  - 缺失 / foreign `threadId` 的 notification 不会污染当前 bound session；snapshot path 也要求 returned `thread.id` 匹配当前 bound thread；unknown turn status、`notLoaded` 与 `systemError` 不会被折叠成 `idle`
  - websocket continuity loss、activity write failure 或 authoritative current-state snapshot 缺失时，sidecar 会通过 hidden `cli session invalidate-proof` 推进 `session_epoch` 并清空旧 activity / capability proof；daemon 短超时失败时该 invalidation 命令允许 direct-store fallback；store 对“daemon 已经推进 epoch 并清空 proof”的旧 epoch 重放返回幂等成功，避免旧 proof 只被 best-effort 清理
  - passive sidecar 的 proof 写入路径必须使用短 SQLite busy timeout，并允许 lifecycle-aware bounded retry；不能把 1s client timeout 的 passive write 交给 daemon worker 后再让 worker 按普通 30s store timeout 阻塞，否则 DB lock 下的 retry loop 会耗尽 dispatch worker；foreground 退出 / sidecar shutdown 后也不能继续重试并阻塞 cleanup。
  - websocket receive 必须用 absolute deadline 贯穿 frame header / length / mask / payload 读取、control-frame loop 和 pong 写回；连续 ping/pong 或 trickled payload 不能无限延长一次 `recv`，control frame payload 也必须限制在 125 bytes，否则 foreground 退出时 sidecar join 会延迟 app-server cleanup。
  - 默认 `--auto-delivery-policy off` 时，sidecar 只记录 partial passive capability proof：existing-thread path 记录 `thread_resume=true`、`current_state_sync=true`；fresh unmaterialized `--new-thread` path 记录 `thread_start=true`、`current_state_sync=true`。两者都不会把 `turn_start` 或 terminal-event proof 标成 true，因此不会绕过当前最小 delivery gate
  - 显式 `--auto-delivery-policy trusted-all` 时，sidecar 会记录当前 epoch 的完整 automatic delivery capability，按 2 秒 poll interval 等待 durable idle proof，写入 accept-pending barrier 与 audit records，然后发送带唯一 marker 的 `turn/start`；acceptance response 最多等待 60 秒，用于容忍真实 app-server / model cold-start 或短时排队延迟
  - accepted `turn.id` 会立即通过 `accept-cli` 进入 6 小时 v1 observation window；matching completed notification 关闭 batch 为 `delivered`，missed notification 可通过同一 websocket/session epoch 下的 `thread/read(includeTurns=true)` reconcile 收口
  - accepted-turn observation window 由 accepted `turn.id` 主导，非终态 proof-only notification 不得废弃已 accepted attempt；fresh first-turn materialization 前 `thread/read(includeTurns=true)` 返回 `not materialized` / `includeTurns is unavailable before first user message` / `no rollout found` 时只表示本次 reconcile 暂无证据，不能自动 manualize
  - matching failed/interrupted/replaced terminal evidence 会 fail-close 到 `manual_resolution_only`
  - clear pre-accept rejection 会写入 `delivery_rpc_state=rejected_before_accept`，不递增 attempt count；timeout / closed / protocol ambiguity 不重发，保留 `accept_pending` 给 stale sweep 标成 `unknown + manual_resolution_only`
- 新建 CLI attempt 时会把通过 gate 当时的 `activity_revision` / `capability_revision` 快照写入 `delivery_attempts`；同一 `delivery_rpc_request_id` 的幂等重试会先恢复已有 attempt，但仍要求该 stored attempt 绑定当前有效 managed session 且携带非零 proof snapshot。它不再受后续 activity 漂移影响，但 proofless legacy attempt、session epoch 失效、缺失 session、profile drift、thread mismatch 或当前 capability 不再满足最小集都会 fail-closed。
- stale `accept_pending`、expired `cooldown` observation、以及 operator close 仍未终态的 CLI attempt 时，当前实现都会推进对应 managed session 的 `session_epoch` 并清空 activity proof，避免旧 idle 证明继续打开下一次自动投递。
- hidden adapter-internal `cbth attempt observe-cli-turn` 已作为 terminal-event 写入面落地；它只接受 stored `delivery_turn_id` 的事件，`observed_at < delivery_observation_deadline` 的 `turn_completed` 才关闭 batch 为 `delivered`，负终态和 late observation 都会 fail-closed 到 `manual_resolution_only`。
- `begin-cli-accept` 使用 deterministic `attempt_id` / `delivery_rpc_request_id`，daemon IPC 失败后会走幂等 direct-store fallback；最终仍无法证明 begin 是否落库时，会 best-effort reject 该 deterministic attempt 并清空本地 proof，避免未发送 `turn/start` 的 pending attempt 长时间阻塞。
- `turn/start` 返回 `turn.id` 后，adapter 会在有界窗口内重试幂等 `accept-cli` 持久化，并允许 daemon IPC 失败后的 direct-store fallback；只有在无法持久化 accepted turn id 后才 fail-closed。
- `turn/start` 已被接受后，accepted-turn observation 是高优先级路径：accepted / started / terminal audit 记录为 best-effort；matching terminal evidence 会先写入 `attempt observe-cli-turn`，再做 passive activity bookkeeping / resync，避免审计或 activity 写入失败覆盖真实完成证据。
- matching terminal evidence 的 `observe-cli-turn` 持久化同样使用有界重试与 direct-store fallback，避免已观察到的 accepted-turn completion 因 1 秒 daemon IPC/store timeout 被丢弃。
- `thread/read(includeTurns=true)` reconcile 同时兼容 nested `thread.turns` 与真实 app-server 可能返回的 top-level `turns`，前提是 `thread.id` / top-level `id` 仍匹配 bound thread。
- accepted-turn observation loop 也受 accepted attempt 的 `delivery_observation_deadline` 约束；本地 deadline 到期会触发一次 best-effort sweep / proof refresh 后退出观察，避免前台进程长期存活时旧 attempt 阻塞后续 head batch。
- sidecar shutdown 是 `turn/start` 前的硬门禁：passive loop 在每次 auto-delivery poll 前重新检查 stop flag；如果 `begin-cli-accept` 后、真正发送 `turn/start` 前进入 shutdown，会写入 pre-accept rejection 并保留 batch 可重试，而不是在关闭窗口里继续发 side-effectful RPC。
- 同一 pre-start cleanup 也覆盖 `begin-cli-accept` 后、`turn/start` 前的 prompt 构造 / attempt-start audit / response parsing 失败：这些失败都必须先 best-effort `reject-cli-before-accept`，避免从未发出 RPC 的 attempt 之后被 stale sweep 当作 unknown manualize。
- daemon capability 列表已包含 `cli-app-server-lifecycle`、`cli-app-server-probe`、`cli-session-capability-dispatch`、`cli-session-permission-dispatch`、`cli-session-proof-invalidation-dispatch`、`cli-turn-observation-dispatch`、`cli-turn-observation-expiry-dispatch` 与 `cli-auto-delivery-dispatch`，避免新 CLI 把 app-server lifecycle / doctor probe / capability / permission / proof invalidation / terminal-event / observation-expiry / auto-delivery audit or rejection 写入路由给不支持对应 subcommand 的旧 daemon。
- `turn_steer` 当前仍 fail-closed，直到后续 phase 落地 active-turn risk proof。
- 这些实现仍不等价于完整 CLI 自动续跑：`trusted-all` idle `turn/start` 路径与 `--new-thread` fresh bootstrap 已落地，但 `turn/steer`、active-turn injection、rollout-only automatic delivered proof、以及更细粒度 policy engine 仍待后续 phase 实现。

## 仍待实现的边界

- sidecar 长时间运行时的状态持久化与 resume 策略
- 如果未来上游允许 loopback websocket auth，则补一轮更强本地安全边界设计与实证
- 更细的 shared `app-server` capability collection 版本策略，尤其是未来如何在不产生用户可见 turn 的前提下证明 `turn/start` / terminal-event surface
- 多个 background jobs 同时命中同一 caller thread 时的 batch 合并参数
- accepted `delivery_turn_id` 在 daemon / websocket / app-server continuity 丢失后的 operator-resolution 体验细节；当前自动路径已 fail-closed 到 manual resolution
