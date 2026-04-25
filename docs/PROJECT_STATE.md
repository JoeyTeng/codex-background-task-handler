# Project State

## 当前目标

验证一套纯外围方案，分别覆盖 Codex Desktop 与 CLI 两条交互路径，让长时间后台任务可以在不修改上游 `codex` 仓库的前提下恢复或继续 caller thread。

## Repo CI / review gate

- 已新增一个外围 `codex/review-gate` commit status：
  - workflow 使用 `pull_request_target` 运行默认分支上的可信脚本，不执行 PR 代码
  - 脚本把 status 写到 PR 当前 head SHA
  - 当前 head 上 Codex inline review comments 会使 gate fail
  - 每个 workflow run 都创建新的 marker comment，用于触发 Codex 并建立等待起点
  - pass signal 只接受 marker 后的 Codex top-level comment，且必须原样带回本次 marker 的 `codex-review-gate-token`
  - 通过前会再次确认当前 head 没有 Codex inline review comments，不依赖 Codex clean summary 的自然语言文案
  - 创建 marker 前和通过前会重新确认 PR head 没变；PR body reaction 不作为通过信号，因为它不能绑定到当前 head
- workflow 落到默认分支后，还需要把 `codex/review-gate` 加进远端 ruleset 的 required status checks。
- 2026-04-25 用临时非默认 base branch 测试过：PR 只触发普通 `pull_request` CI，没有触发 `Codex Review Gate`；真实 GitHub Actions bot 路径要等 workflow 进入 repository default branch 后再测。

## 当前架构方向

- 双端方案现在共享一套更清晰的核心抽象：
  - 一个共享的本地 daemon
  - 一个共享的 job store / 状态机
  - 一个共享的 CLI 控制面
  - 两个薄的 Codex integration adapters：CLI 与 Desktop
- 第一版不做系统级常驻服务；改为按需启动的本地 daemon。
- 该 daemon 生命周期独立于单个 Codex 前台实例，但第一版不要求它为长窗口持续常驻：
  - 近端 delivery work / timers 可以阻止当前实例退出
  - 更长的 deadline 则改为 durable 落盘，并在下次启动时先做 overdue sweep / auto-close / reconcile
  - 唯一例外是 CLI accepted attempt 的 `delivery_observation_deadline`：在 deadline 到期前它仍属于必须常驻观察的 live-observation window
- 第一版稳定外部接口只做 CLI，不承诺公开 socket / Web / plugin 协议。
- 因此，之前设计里提到的 `background-taskctl` helper，应收敛成主 binary 的 `cbth job ...` 子命令，而不是第二个长期维护的独立工具。
- 经过 reviewer 复核后，Desktop 关键路径又做了一个更保守的收口：
  - 不再把“heartbeat turn 稳定执行通用 `cbth job ...` CLI”当作既定前提
  - 改为定义统一 delivery envelope schema，并给出两条候选读取传输：
    - `direct_file_read`
    - `helper_cli_read`
  - 其中 `direct_file_read` 仍是候选优先路径，`helper_cli_read` 只是条件性 fallback
  - 但 `direct_file_read` 不是 daemon liveness 机制：每轮 bridge wake 仍必须先执行窄 helper `cbth desktop bridge-preflight ...`，按需拉起 daemon、补做 overdue sweep / GC / auto-close / reconcile，并原子发布同一 `snapshot_revision` 的 ready/reconcile manifest
  - 同时又补了一层 explicit desktop binding：bridge 运行期只更新已知 caller automation，不做 blind create/discovery
  - Desktop 的 `read_transport` 也已收口为 installation-wide 选择：
    - binding 里只 durable 镜像当前安装选定 transport 与其 generation
    - 权威来源是 daemon-managed `desktop_installation_state`
    - v1 不支持 mixed Desktop `read_transport` bindings
    - `~/.cbth` 文件权限与稳定 helper CLI 只是在降低意外暴露面；Desktop helper / snapshot 路线同样只支持 dedicated single-user deployment assumption
    - installation-wide capability 结论也已收回到 `desktop_installation_state`：
      - transport generation 变化时，capability 必须原子重置为 `unknown`
      - capability 还必须绑定 installation-wide `validation_fingerprint`
      - binding repair 不得单独覆盖 installation-wide capability
  - Desktop 顶部文案也已改成更保守的口径：`bridge-preflight` / `note-arm-pending` / `note-arm` / `note-boundary-crossed` 是 v1 规划中的窄 helper 依赖；`note-delivered` 已降级为未来 post-output ack 扩展点，但后台 heartbeat 能否无审批执行前者仍待实证
  - 而且 Desktop 自动续跑现在被明确成双门槛：
    - batch 本身必须是只读 / 低风险
    - 目标安装上的读路径也必须已验证可无审批执行
    - 目标安装上的 `note-*` 写 helper 也必须已验证可无审批执行
  - 同时又进一步收紧成：
    - v1 automatic caller path 只支持 `note-boundary-crossed` fresh success 后的一次性 inline text handoff
    - 大 artifact continuation 与 post-boundary 普通工具步骤不再纳入 v1 automatic path
    - 这两类场景直接留给 operator/manual follow-up
- 同时，CLI 关键路径也收口为：
  - 明确依赖实验 RPC
  - 启动时 capability probe
  - 基于本机 `codex-cli 0.123.0` 的当前上游能力，第一版实际合同收口为 loopback-only shared `app-server`
  - `--ws-auth` 目前仍只适用于 non-loopback listeners，因此不再把 per-session bearer-token auth 当成 v1 既有能力
  - 因而 v1 只能在“专用单用户工作站 / 等价隔离环境”这个部署前提下成立；更强本地 auth 边界留待上游支持 loopback auth 后再补
  - shared `app-server` 由 daemon 持有，而不是前台 wrapper 临时持有
  - 一个 managed CLI session 在 v1 里只绑定一个 `bound_thread_id`
  - 这个 `bound_thread_id` 在 v1 现在有两个显式 bootstrap：
    - 已知 thread 的 `cbth cli run --bind-thread-id <thread_id>`
    - 以及 `thread/start` 可用时的 `cbth cli run --new-thread`
  - v1 不再承诺 late-bind，也不把 `managed_session_id` 暴露成外部回填 thread id 的 stable bootstrap surface
  - 同一个 `bound_thread_id` 在 v1 最多只允许一个 non-retired managed session；`cbth cli run --bind-thread-id` 必须是 attach-or-create，而不是 blind create
  - 前台 thread-switch 的自动观测/自动 retarget 不属于 v1 合同
  - 默认仅在 idle 时 `turn/start`
  - detached 自动投递还要求 managed session 自身是 no-approval / no-network / no-write profile
  - `turn/steer` 只作为只读、低风险场景下的受限优化
- 共享核心也补上了 reviewer 指出的 thread 级缺口：
  - 引入 thread-scoped FIFO 队列
  - 引入 `delivery batch`
  - 引入最小连续发送间隔
  - 同一 thread 同时最多一个 in-flight delivery attempt
  - 并进一步补上了 durable `attempt_id + generation` 合约，以及 optional `automation_id` 协调字段
  - 其中 `generation` 现在明确只作用在 batch 内 attempt 级 redelivery：
    - 新 generation 只会 supersede 旧 attempt
    - 不会自动把整个 batch 关闭为 `close_reason=superseded`
- Desktop 第一版的送达语义也已收口：
  - 目标是 `at-least-once wakeup scheduling`
  - `closed` 只表示 `cbth` 停止自动重投，不表示 caller 一定已消费
- 为了让 redelivery 语义真正可实现，batch schema 也进一步要求 durable 记录：
  - `redelivery_window_ends_at`
  - `max_delivery_attempts`
  - `delivery_attempt_count`
- Desktop 运行期还新增了两条窄控制面：
  - `cbth desktop note-arm-pending ...`
  - 用于在 bridge 真正调用 `automation_update` 前，把当前 head attempt durable 推到 `arm_pending`
  - `cbth desktop note-arm ...`
  - 用于在 bridge 成功 `automation_update` 后，把 attempt durable 推进到 `cooldown`
- Desktop helper/control 链路也进一步按职责分层：
  - mandatory preflight：`cbth desktop bridge-preflight ...`
  - optional bridge-side read fallback：`list-arm-pending` / `list-pause-due` / `claim-next-ready`
  - writeback / gated continuation：`note-arm-pending` / `note-arm` / `note-boundary-crossed`
  - operator/manual recovery 或 future-expansion：`read-artifact`
- 但除了 mandatory `bridge-preflight` 外，额外 helper read fallback 仍被重新降级成“条件性 fallback”：
  - 它仍要求 heartbeat turn 无审批执行窄 `cbth desktop ...` 命令
  - 在这个前提被实证前，不能把额外 helper read fallback 当作已验证主路径
  - 当前真正优先候选仍是 `bridge-preflight + direct_file_read`
- Desktop 的 delivery state machine 也继续收紧：
  - attempt 不再保留单独的 durable `armed`
  - bridge 在真正调用 `automation_update` 前，先把 attempt durable 推到 `arm_pending`
  - 只有 bridge arm 成功并 `note-arm` durable 记录后，attempt 才进入 `cooldown`
  - `cooldown` 期满后若仍可重投，也必须重新进入 eligible ready / fresh-arm gate；Desktop 同一 binding 必须等上一代 `armed_generation` quiesced 后才允许新 attempt
  - 若 `delivery_attempt_count >= max_delivery_attempts`，batch 必须自动关闭到 `close_reason=max_attempts_exhausted`
- Desktop 的送达语义也进一步收紧：
  - `claim-next-ready` 虽然名字里带 `claim`，但第一版必须是纯 read/peek helper，不能 reservation 或隐藏 head batch
  - `arm_pending` attempt 不再是新的 ready head；bridge 必须先 reconcile 它，不能重复 arm 同一 generation
  - `note-boundary-crossed` 现在不只是断点写回，而是 gated continuation helper：
    - caller 必须先拿到它的 fresh success 返回，才允许看到 inline continuation payload / summary
    - helper mutation 前必须校验完整 caller prompt token：`source_thread_id + batch_id + attempt_id + generation + expected_snapshot_revision`
    - 这一步在 v1 是单次 crossing，不再提供自动 replay-safe continuation；response 丢失后改走 operator recovery
    - redelivery 只能发生在 durable reconciliation 正向证明 crossing mutation 没有提交时；helper 已提交但 response 丢失仍按 `handoff_recorded` 处理
  - 如果 `note-boundary-crossed` 尚未 success，caller 不得真正跨过 continuation boundary
  - `note-boundary-crossed` 需要 compare-and-swap / stale-no-op 语义，避免重复 wake 或 supersede 后重复记账
  - 一旦 `note-boundary-crossed` 成功，当前 batch 就必须进入 `closed + close_reason=handoff_recorded + replay_policy=manual_resolution_only`
  - post-boundary 只允许进入一次性 inline text handoff phase，不把普通 Codex 工具纳入 supported automatic path
  - 第一版不再尝试把纯文本回复或后续工具动作自动收口成 “已送达”；`handoff_recorded` 只表示 inline handoff payload / recovery envelope 已 durable 记录，释放 FIFO，但不证明 caller assistant 文本可见
  - post-boundary lost response 只能通过 `cbth batch inspect --batch-id ...` 读取 `boundary_recovery_envelope` 做 operator recovery
  - `note-arm` 也新增了 CAS/幂等合同，避免 bridge 重试导致重复计数
  - `note-boundary-crossed` 的非 success outcome 已拆成 transient / stale / already-crossed / capability-invalid / unknown，避免把已 `handoff_recorded` 的 batch 误重投
  - pre-boundary 歧义 batch 的 durable 表达应落成 `replay_policy=manual_resolution_only`
  - 默认只允许 operator close；若长期无人处理，则在 `redelivery_window_ends_at` 到期时自动 close 释放 FIFO/GC
- 第一版自动续跑总门槛也已统一：
  - 只处理 `delivery_read_only=true`
  - 且不需要 approval/network/write access 的 batch
  - Desktop 还必须满足安装级 `read_transport_capability=validated`
  - Desktop 还必须满足 `writeback_capability=validated`
  - `requires_artifact_read=true` 的 batch 不再进入 Desktop automatic caller path
  - 非只读 batch 一律不自动续跑，留给 operator/manual follow-up
  - 这些字段的输入合同也已收口为：
    - submitter 显式提供 delivery policy
    - 若缺失则 core fail-closed 写入保守默认值
    - `inline_payload_bytes` / `requires_artifact_read` 由 core 统一派生，不由 submitter 直填
    - CLI adapter 再在共享核心字段之上本地判定是否 steer-eligible
- caller heartbeat lifecycle 也已收口：
  - `caller_automation_id` 是预绑定、长期复用的 heartbeat automation
  - `armed_generation` 作为这个长期复用 heartbeat 的 generation 栅栏
  - `armed_generation_quiesced_at` 记录上一代 one-shot wake 已经被 bridge 证明 `PAUSED` / deleted / otherwise quiesced
  - 同一 binding 在 `armed_generation_quiesced_at` 为空时不得 fresh-arm 下一批；`handoff_recorded` 释放 FIFO 不等于 heartbeat 已 quiesced
  - `arm_pending_deadline` 是当前 head attempt 的 reconcile 截止点，不属于 binding 自身的 durable 字段
  - `pause_not_before` / `pause_deadline` 才是 binding / armed generation 级的一次性 wake cleanup 窗口
  - 其中 `pause_not_before` 明确保证 bridge 至少给 caller 一次完整 heartbeat 触发机会，再允许回收这次 wake
  - bridge 的 ready entry 也已收口：
    - 只要求返回 prompt token
    - caller prompt token 必须包含 `source_thread_id + batch_id + attempt_id + generation + snapshot_revision`
    - `snapshot_path` 只保留在 bridge-side locator，不进入 caller prompt
    - `caller_automation_id` 一律由 bridge 通过 `source_thread_id -> binding` lookup 解析
  - daemon 自动退出条件也必须覆盖这两个 deadline
  - `read_transport_capability=validated` 现在明确包括 mandatory `bridge-preflight` 的无审批执行、daemon sweep/refresh 成功，以及刷新后 snapshot 的无审批读取
  - bridge 还需要一个专门的 overdue-binding 输入面：
    - `~/.cbth/inbox/arm-pending-bindings.json`
    - 或 `cbth desktop list-arm-pending ...`
    - `~/.cbth/inbox/pause-due-bindings.json`
    - 或 `cbth desktop list-pause-due ...`
  - 正常路径只由 bridge / operator `pause` / `update` / `reuse`
  - caller prompt 自己不直接 pause 这个长期复用 automation
  - stale wake、snapshot 不可读、boundary 已记录后的后续 wake、degraded 都先 no-op / helper writeback，再由 bridge 后续切回 `PAUSED`
  - 正常投递路径不做 `delete`
  - 只有明确 operator `binding unbind` 才允许删除
- CLI 侧 reviewer 指出的 idle/race 缺口也已收口：
  - idle 必须来自 app-server live event stream
  - attach/recovery / continuity-loss 之后必须先回到 `activity_state=unknown`
  - 只有重新拿到 current-state sync 或新的本地 turn lifecycle 证据后，才允许重新判定 `idle`
  - `turn/start` 失败要被当成 benign race，回到等待下一个 idle，而不是视为成功送达
- CLI managed session contract 也已补硬：
  - shared `app-server` 由 daemon 持有
  - 一个 managed session 在 v1 里只承诺一个固定的 `bound_thread_id`
  - `bound_thread_id` 通过两种显式 bootstrap 建立：
    - `cbth cli run --bind-thread-id <thread_id>`
    - 或 `thread/start` 可用时的 `cbth cli run --new-thread`
  - 当前上游 surface 仍不提供可依赖的前台来源归因
  - daemon 必须 durable 跟踪 `managed_session_id + bound_thread_id`
  - daemon 还必须 durable 跟踪 session-scoped risk profile：
    - `session_allows_approval`
    - `session_allows_network`
    - `session_allows_write_access`
  - session state 现在必须允许 `parked`：
    - live app-server 已结束
    - 但 unresolved manual batch 仍在 durable 等待 operator 收口
    - 这个 manual batch 可以来自 accepted attempt fail-closed，也可以来自 pre-accept manual/operator path
  - 这组 profile 对 non-retired session 是 immutable 的；attach-or-create 必须先比较 requested profile 与 durable profile，drift 时只能 fail-closed 或 retire-and-recreate
  - 同一个 `bound_thread_id` 最多只允许一个 non-retired managed session；不可安全复用时必须 fail-closed，而不是并发创建第二个 session
  - 启动时显式 bootstrap 只决定 delivery target，不证明前台焦点
  - v1 不提供 late-bind / external discovery surface；如需换目标 thread，必须新开 session
  - 如果用户想把自动续跑目标换到另一个 thread，必须显式开新 session 或等待未来的 rebind contract
- CLI 的 delivery completion contract 也继续收口：
  - 在真正调用 `turn/start` / `turn/steer` 前，必须先 durable 写入 `accept_pending` barrier：
    - `delivery_rpc_request_id`
    - `delivery_rpc_kind`
    - `delivery_rpc_started_at`
    - `delivery_rpc_state=pending_acceptance`
    - `delivery_rpc_correlation_marker`
  - response 丢失时，只有同一 `managed_session_id + session_epoch` 的连续 event/current-state 面能正向证明 marker 已接入 exactly one caller turn，才允许补写 `delivery_turn_id`
  - 如果既不能证明 accepted，也不能证明未 accepted，当前 head batch 必须 fail-closed 到 `manual_resolution_only`，不得重新发送同一 batch
  - 如果能在同一连续会话里正向证明 RPC rejected before accept，才允许 `accept_pending -> prepared`，写入 `delivery_rpc_state=rejected_before_accept`，且不递增 `delivery_attempt_count`
  - `turn/start` / `turn/steer` 被接受，只表示 batch 已接入某个 caller turn 的 pending input
  - 每次 accepted attempt 都必须 durable 记录 `delivery_turn_id`
  - accepted attempt 还必须 durable 绑定 `managed_session_id + session_epoch`
  - `managed_session_id` 是逻辑会话 id；`session_epoch` 是该会话当前可证明连续的 app-server 事件流代号
  - accepted attempt 还必须 durable 记录：
    - `delivery_accepted_at`
    - `last_observed_turn_event`
    - `last_observed_turn_event_at`
  - `last_observed_turn_event` 的 v1 canonical enum 已固定为：
    - `turn_started`
    - `turn_completed`
    - `turn_failed`
    - `turn_interrupted`
    - `turn_replaced`
  - accepted attempt 还必须带 `delivery_observation_deadline`
  - 只有在这个 deadline 之内，未收口 `delivery_turn_id` 才阻止 daemon 退出
  - deadline 到期仍未观察到可信 `turn/completed` 时：
    - 当前 attempt 收敛到 `abandoned`
    - `delivery_observation_state=expired`
    - 当前 head batch 必须 fail-closed 到 `manual_resolution_only`
  - 这之后任何迟到的 `turn/completed` 都只能保留为 operator/debug 证据，不得再自动 close 为 `delivered`
  - 只有当同一个 `delivery_turn_id` 的 `turn/completed` 被观察到，且以下条件同时满足时，batch 才允许关闭：
    - 该 attempt 仍是当前 head delivery
    - `delivery_observation_state=tracking`
    - `replay_policy=automatic`
    - `now <= delivery_observation_deadline`
  - 如果某次 accepted attempt 已经带有 `delivery_turn_id`，即使前台 UI 临时切到别的 thread，它也仍可等待匹配的 `turn/completed` 正常收口
  - 因此 daemon 退出条件只需要在 `delivery_observation_deadline` 窗口内覆盖这些未收口的 `delivery_turn_id` 观察
  - 但只要 `managed_session_id + session_epoch` 的观察连续性丢失，就不得自动 replay；当前 head batch 必须进入 `manual_resolution_only`
  - 同样地，只要 accepted 的 `delivery_turn_id` 后续出现失败/中断/替换等不可信终局，也必须 fail-closed 到 `manual_resolution_only`
  - CLI 的最小 capability set 现在不只包括 `thread/resume` / `turn/start` / current-state sync；还必须能观察 accepted turn 的负终态事件，否则 detached auto-continuation 必须 fail-closed
  - accepted attempt fail-closed 到 `manual_resolution_only` 后，managed session 的 live 部分可以结束，但 durable session 要先转成 `parked`；同样的 `parked` 语义也覆盖 pre-accept manual/operator path，等 manual batch 终态后再允许 replace
  - continuity-loss 后的最小人工收口路径也已收口为：`batch inspect-head` 看 durable 证据，再用 `batch close-head` 带明确 reason 收口
- 同时又补了一个和 TUI 当前实现一致的判断：
  - active-turn steer 语义更接近现有 TUI 的 `pending_steers` / queued-follow-up 行为
  - non-steerable turn 必须回落到排队，而不是被算作成功送达
  - steer 的 gating 不能只看 batch 自己；还必须证明当前 active turn 本身也是 `read_only_low_risk`
- 结果保留责任也已收敛：
  - `cbth job complete --result-file <path>` 的语义改为 ingest/copy 到 `cbth` 自己管理的 artifact store
  - 原始外部文件不再承担长期保留责任
  - artifact GC 也被绑定到 batch 终态与最小保留窗口，而不是外部临时文件生命周期
- 但 reviewer 第二轮指出，设计还没有完全闭环；当前剩余的是 contract 细化和实证，不再是路线选择问题。
- 进一步用本机 `codex-cli 0.123.0` 复核后，`codex app-server --help` 仍把 `--ws-auth` 标成仅适用于 non-loopback listeners。
- 因此 CLI 第一版现在改成更现实的安全边界：
  - loopback-only listener
  - daemon-owned ephemeral port
  - unauthenticated local control plane
  - 仅在 dedicated single-user deployment assumption 下支持
- 这里的 unauthenticated loopback 只指上游 Codex shared `app-server` 的当前可用 surface；`cbth` 自己的 daemon IPC v1 已收紧为 same-user-only Unix domain socket：
  - socket 位于 `~/.cbth/run` 这类 `0700` 用户私有目录
  - socket / parent ownership 与权限必须校验
  - daemon 接入后必须校验 peer uid
  - 无法提供 same-user proof 时，mutating / recovery CLI 命令 fail closed，不退回 unauthenticated TCP
- `--remote-auth-token-env` 仍是上游已存在的 surface，但在当前 loopback 合同下不再被当成第一版既有依赖。
- `~/.cbth` 的 Desktop 侧文件路径也新增了权限合同：
  - directory `0700`
  - file `0600`
  - automatic Desktop path 在 pre-boundary 阶段只暴露 ready/reconcile metadata
  - `by-thread/<thread_id>.json` 与 artifact 文件保留为 operator/debug export，默认不属于 automatic caller path
- phase 1 Rust local-store 实现边界已明确为 macOS / Linux：
  - 当前依赖 Unix 风格的私有权限、atomic replace 与 directory sync 语义
  - 纯 Windows 支持需要单独设计 IPC、ACL、replace/sync 合同后再纳入
- phase 1 CLI 的人工关闭 reason 已收口到 operator 专用 canonical reason：
  - `operator_closed_unconfirmed`
  - `operator_confirmed_delivery`
  - `manual_resolution_expired` / `max_attempts_exhausted` / `redelivery_window_exhausted` 继续作为系统状态机 reason，而不是 `batch close-head` 的用户输入 reason
- phase 1 orphan ingest cleanup 已修正一个 future-clock 风险：
  - `maintenance sweep --now <future>` 可以用合成时间做 overdue 判定
  - 但 pending ingest stale selection、`.ingest-active` marker observation 与 retry refresh 都必须按真实 wall clock 处理
  - 否则一次未来 sweep 可能抢删刚创建但 marker 尚未落下的 ingest ownership，或把已崩溃 ingest 的 `updated_at` 固定到未来
- Desktop bootstrap 合约也已收紧：
  - 不能只相信一次 `PAUSED` 创建请求
  - 必须 create/update 后读回验证 paused 状态
  - 否则 binding 不能进入 `bound`
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
- 基于这些证据，一个更干净的 Desktop 外围架构浮现出来：不让外部 sidecar 直接改 Codex 的 automation DB；改由一个固定的 bridge automation thread 周期性检查外部长任务状态，再用 `automation_update` 管理 caller thread 的 heartbeat。
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
- 但这些 PoC 主要证明的是 Desktop 内置 automation 能力与 thread-targeted heartbeat 可行；当前第一版运行期合同已经进一步收窄为“预绑定 caller heartbeat + bridge 只更新已知 automation”，不再把 runtime retarget/create 当成默认路径。

## 当前方案收敛

- Desktop 方向的技术方案已经收敛为：
  - 外部 `sidecar supervisor` 跑长任务
  - 共享 `job state` 作为 bridge / caller 读取面
  - 固定 `bridge heartbeat thread` 负责轮询可投递 thread / batch
  - bootstrap 预绑定 `caller_automation_id`
  - bridge 运行期只更新这个已知 heartbeat，不做 blind create / retarget
  - bridge 侧优先读取只读 inbox snapshot；caller thread 被唤醒后必须先通过 `note-boundary-crossed` 成功跨过 gated continuation boundary，拿到 inline continuation payload / summary 后才能进入一次性 handoff phase
- 该方案不依赖：
  - 外部 live push 当前 Desktop thread
  - 外部直接改 Codex automation DB
  - 后台 heartbeat 稳定执行通用本地 CLI
  - notification thread
- 独立设计文档已记录在：
  - `docs/DESKTOP_BACKGROUND_TASK_BRIDGE_DESIGN.md`

## CLI 补充结论

- 当前 CLI TUI 不能直接复用 Desktop 的 `automation_update` / heartbeat bridge 方案。
- 这不是推断，而是 TUI 自身对 `ServerRequest::DynamicToolCall` 直接返回 unsupported；对应单测名字就是 `rejects_dynamic_tool_calls_as_unsupported`，文案为 `Dynamic tool calls are not available in TUI yet.`。
- 因此，CLI 方向仍应以 `wrapper + shared app-server + sidecar` 作为主方案。
- 但 reviewer 指出了一个重要约束：这条路线实际建立在实验 RPC 上，因此文档已进一步收口为“必须 capability probe + fail-closed”，不能把当前 PoC 可用的所有 RPC 都当成长期稳定契约。
- 目前文档已经进一步规定：第一版 shipping 配置默认关闭 `turn/steer`，只有在 feature flag 打开且满足只读/低风险门槛时，才允许作为可选优化启用。
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
- 因此，CLI 方向的结论已经不只是“协议层上的前台 client 会收到通知”，而是“当用户手动把前台停留在同一个 `bound_thread_id` 时，真实前台 TUI 也会把 sidecar 触发的新 turn 展示给用户”。
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

## Phase 1 Implementation Priority

- 第一个实现 PR 先做共享核心最小闭环，而不是直接实现 Desktop heartbeat 或 CLI shared app-server adapter。
- 工程优先级固定为：
  - correctness first：状态机、artifact ingest、FIFO、operator close 等语义必须可恢复、可审计、fail-closed
  - long-running reliability second：所有后台或未来 daemon-facing 组件都必须避免无界内存增长、泄漏文件句柄/子进程、孤儿任务和不可停止轮询
  - resource efficiency third：默认 idle 路径应保持低内存、低 CPU，轮询和 sweep 必须有明确 budget / batch limit
- Phase 1 因此优先落地本地 store、artifact ownership、batch lifecycle、operator CLI 和测试，再把 daemon auto-start、Desktop bridge、CLI live continuation 放到后续 PR。

## Phase 1 Implementation Checkpoint

- 当前 `codex/phase-1-core-cli` 分支已经落地第一版 Rust `cbth` binary。
- 已实现的共享核心范围：
  - 本地 `~/.cbth` / `CBTH_HOME` 状态目录与 `0700` / `0600` 权限约束；新建目录和 DB 文件后同步父目录，避免 fresh home 成功返回后丢失 directory entry
  - SQLite store 与 schema 初始化，WAL 模式下使用 `synchronous=FULL`，避免命令成功返回后 DB ownership commit 比 artifact fsync 更弱；SQLite busy timeout 设为 30 秒，并且 store open / schema 初始化阶段对 `BUSY` / `LOCKED` 做有界 retry，用于承受短时多进程 CLI 启动风暴
  - `job submit` / `job complete` / `job fail` / `job inspect` / `job list`
  - `job complete --result-file` 的 streaming artifact ingest、SHA-256、manifest 与 retention metadata
  - DB-backed pending artifact ingest 记录与 result artifact ingest marker，用于避免 cleanup 删除仍在复制中的大 artifact，并让 crash orphan cleanup 有有界 DB 输入面；异常 ingest id 不参与文件系统删除
  - thread-scoped open batch、head batch 查询、operator `batch close-head`
  - fail-closed delivery policy 默认值、metadata / CLI override、metadata policy 短字段 alias 与 unknown-field 拒绝
  - `redelivery_window_seconds` 等 timestamp 派生使用 checked arithmetic，避免 CLI 超大输入造成 panic / wrap
  - Phase 1 尚未持久化 inline handoff payload，因此 completed job batch 统一保持 `requires_artifact_read=true`
  - `maintenance sweep` 的基础 manual-expiry、automatic redelivery-window expiry、artifact-GC、manifest reconcile、orphan artifact cleanup 入口
  - sweep lane 现在都有有界 work budget：expired batch close、artifact delete、manifest sync、orphan scan/delete 都不会一次性无界展开
  - artifact manifest reconcile 通过 DB 中的 `manifest_synced_retention_until` / `manifest_sync_attempted_at` 记录 durable progress，per-artifact 失败不会阻塞后续 GC / cleanup lane，也不会让长期失败集合饿死后续 artifact
  - artifact GC 通过 `gc_attempted_at` 记录 durable attempt progress，per-artifact 删除失败不会让后续可删 artifact 长期饥饿
  - artifact ingest 创建 per-artifact directory 后会同步父 `artifacts/` 目录，避免 DB commit 指向未 durable 的目录 entry
  - artifact maintenance 写入/删除路径按 `artifact_id` 计算 canonical store path，并校验 DB 中的 `relative_path` 未漂移
  - artifact 目录删除必须在 remove 后同步父目录，只有删除成功或确认 NotFound 后才丢弃 DB ownership
  - pending ingest cleanup 同样校验 `artifact_id` 与 `relative_path`，且 stale selection / active ingest marker 新鲜度均按真实 wall clock 判断，不受 `maintenance sweep --now` 影响
  - failed result ingest 会保留 `artifact_ingests` cleanup 输入面，避免 partial artifact cleanup 未确认时形成不可重试泄漏
  - metadata file 读取要求 regular file，并使用 bounded read 重新确认 `MAX_METADATA_BYTES` 上限
  - artifact / marker manifest 写入使用唯一临时文件再 rename，避免并发 sweep / close 之间抢同一个 `.tmp`
- 当前仍刻意不包含：
  - daemon auto-start / idle lifecycle
  - Unix socket same-user IPC
  - Desktop heartbeat / bridge helpers
  - CLI shared app-server managed session
  - `turn/start` / `turn/steer` delivery adapter
- 已覆盖的自动化检查：
  - `cargo fmt --check`
  - `cargo clippy --all-targets -- -D warnings`
  - `cargo test`
