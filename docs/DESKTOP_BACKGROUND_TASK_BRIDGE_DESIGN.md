# Desktop Background Task Bridge Design

## 目标

- 不修改上游 `codex` 仓库。
- 保持现有 Codex Desktop 使用方式。
- 让长时间外部任务在完成后，由原 caller thread 自动继续，而不是依赖用户手动回到 thread。
- 允许大约 1 分钟级别的延迟。
- 不要求在 Desktop app 退出后继续工作。

共通核心部分见：

- `docs/SHARED_CORE_ARCHITECTURE.md`

## 已敲定的约束

- 外部进程不能可靠地把消息直接推入 Desktop 当前已加载的 live thread。
- 外部进程可以改写 persisted thread history，但 Desktop 当前 session 不会把这些外部写入热并入自己的可见历史和后续上下文。
- `automation_update` 是 Codex thread 内置能力。
- `automation_update` 可以创建或更新指向其他 thread 的 heartbeat automation。
- heartbeat 触发出来的 turn 本身也可以继续调用 `automation_update`。
- 第一版不把“heartbeat turn 稳定执行通用 `cbth job ...` CLI”当作既定前提。
- 但 Desktop adapter 当前只把三类能力当作规划中的关键路径：
  - 优先的只读文件路径：`direct_file_read`
  - 窄写回 helper：
    - `cbth desktop note-arm-pending ...`
    - `cbth desktop note-arm ...`
    - `cbth desktop note-boundary-crossed ...`
  - caller 侧大 artifact 的后续 chunked helper：
    - `cbth desktop read-artifact ...`
- 其中窄写回 helper，以及大 artifact 场景下的 `read-artifact` capability，是否能在后台 heartbeat 中无审批执行，当前仍待实证；在这一步没验证前，不应把 Desktop 自动续跑表述成已实现能力。
- bridge 侧的 `claim-next-ready` 目前只是条件性 fallback，不应被表述成第一版必需面。
- `read-artifact` 不再算 bridge-side fallback：
  - 它是 caller 侧在大 artifact 场景下的必需 chunked access surface
  - 但仍然要求 heartbeat turn 能无审批执行对应窄 helper
- bridge-side fallback 当前只能算条件性方案：
  - 它仍要求 bridge heartbeat turn 能无审批执行窄本地命令
  - 在这个前提被实证前，不应把它表述成已验证主路径
- 因此，Desktop 上最可靠的方案不是“外部 sidecar 直接推送 caller thread”，而是“由 app 内部 automation scheduler 去唤醒 caller thread”。
- 运行期对 bound caller heartbeat 的 automation mutation 也必须收口：
  - bridge / operator 是唯一允许 `pause` / `update` / `reuse` 它的一方
  - caller prompt 本身不直接读取 per-thread envelope；它只通过 gated helper 请求 continuation access，并且不直接修改这个长期复用的 automation

## 核心设计

### 组件

1. `sidecar supervisor`
   - 负责真正执行长时间任务，例如等待 CI、等待 reviewer、等待外部系统结果。
   - 不尝试直接修改 Codex Desktop 的 live session。

2. `shared job state`
   - 由 sidecar 暴露给 Codex thread 读取的共享状态面。
   - 第一版定义统一的 delivery envelope schema，但允许两种读取传输：
     - `direct_file_read`
     - `helper_cli_read`
   - `direct_file_read` 建议至少包含：
     - `~/.cbth/inbox/ready-threads.json`
     - `~/.cbth/inbox/arm-pending-bindings.json`
     - `~/.cbth/inbox/pause-due-bindings.json`
   - `~/.cbth/inbox/by-thread/<thread_id>.json` 与 artifact 路径只允许作为 operator/debug export：
     - 默认禁用
     - 不属于 automatic caller path
     - 不得在 `note-boundary-crossed` 之前向 caller 暴露 payload / artifact 内容
   - `helper_cli_read` 建议提供一组窄 helper：

```text
cbth desktop note-arm-pending --source-thread-id <thread_id> --attempt-id <attempt_id> --generation <generation> --bridge-request-id <request_id> --json
cbth desktop list-arm-pending --bridge-thread-id <thread_id> --json
cbth desktop list-pause-due --bridge-thread-id <thread_id> --json
cbth desktop claim-next-ready --bridge-thread-id <thread_id> --json
cbth desktop note-boundary-crossed --source-thread-id <thread_id> --attempt-id <attempt_id> --generation <generation> --expected-snapshot-revision <revision> --json
cbth desktop read-artifact --artifact-id <artifact_id> --artifact-read-lease-id <lease_id> --offset <offset> --max-bytes <n> --json
cbth desktop note-arm --source-thread-id <thread_id> --attempt-id <attempt_id> --generation <generation> --bridge-request-id <request_id> --bridge-arm-lease-id <lease_id> --json
```

   - 底层仍可用 SQLite / 普通文件 / `mmap`，但这属于内部实现细节。
   - 明确不依赖直接改 Codex 自己的 automation DB。
   - `direct_file_read` 目前仍是 bridge 侧候选主路径，待“heartbeat 无审批读取”实证后再升级为已验证主路径。
   - `helper_cli_read` 目前只是条件性 fallback，待“heartbeat 无审批执行窄 helper”实证后才能升级为正式路径。
   - 如果 `direct_file_read` 在目标安装上不可行，Desktop 第一版只能退回“已单独验证过的 `helper_cli_read`”，而不是继续把任意本地 helper 执行当作既定前提。

3. `desktop thread binding`
   - 每个要支持自动续跑的 caller thread，都必须先完成一次 binding。
   - binding 至少要 durable 记录：
     - `source_thread_id`
     - `caller_automation_id`
     - `armed_generation` (optional)
     - `pause_not_before` (optional)
     - `pause_deadline` (optional)
     - `read_transport`
     - `read_transport_generation`
     - `read_transport_capability`
     - `artifact_read_capability`
     - `writeback_capability`
   - bridge 在运行期只允许更新这个已知 `caller_automation_id`，不做 blind create / discovery。
   - Desktop v1 不支持同一安装里 mixed `read_transport` bindings：
     - 同一安装只允许一个 installation-wide `read_transport`
     - binding 上的 `read_transport + read_transport_generation` 只是这个安装当前选定 transport 的 durable 镜像
     - 如果 binding 镜像与安装当前 installation state 不一致，该 binding 必须进入 `degraded` 或重新 bootstrap
   - Desktop 安装级还必须有一个 daemon-managed `desktop_installation_state` 作为权威来源：
     - `read_transport`
     - `read_transport_generation`
     - `read_transport_capability`
     - `artifact_read_capability`
     - `writeback_capability`
     - `validation_fingerprint`
     - `validated_at`
   - 推荐暴露面：
     - preferred: `~/.cbth/inbox/desktop-installation-state.json`
     - fallback: `cbth desktop installation-state --json`
   - bootstrap / repair 是唯一允许更新 installation state 的路径；bridge 运行期必须先读 installation state，再核对 binding 镜像
   - installation-wide capability 结论必须永远跟随当前 `read_transport_generation`：
     - transport generation 一旦变化
     - `read_transport_capability`
     - `artifact_read_capability`
     - `writeback_capability`
     - 都必须被原子重置为 `unknown`
     - 直到 installation-state repair 明确再次写入新的 validated 结论
   - installation state 的 capability 结论还必须绑定一个 installation-wide `validation_fingerprint`：
     - 至少覆盖当前 Codex Desktop build / helper binary revision / 与无审批执行相关的本地环境形状
     - 只要 fingerprint 变化，bridge 就必须把这套 capability 视为失效，直到 installation-state repair 重新确认
   - 只有当以下条件同时满足时，这个 binding 才允许进入真正可自动续跑的 `bound` 状态：
     - `read_transport_capability=validated`
     - `writeback_capability=validated`
   - `artifact_read_capability=validated` 不是 `bound` 自身的定义；它只在某个具体 batch 的 `requires_artifact_read=true` 时，作为额外的 per-batch delivery gate 生效
   - Desktop v1 的本地信任边界也必须收口为：
     - `~/.cbth` 的 `0700/0600` 权限与稳定 `cbth desktop ...` CLI 面，只是在降低意外暴露面
     - 它们不是 per-invocation 授权机制
     - prompt token / helper 参数在 v1 里只承担 correctness fencing，不承担抗同用户恶意进程的身份认证
     - 因此整个 Desktop helper / snapshot 路线同样只支持 dedicated single-user deployment assumption
   - 未完成 binding 的 thread 可以提交 job，但不会被 bridge 自动续跑。

4. `bridge heartbeat thread`
   - 一个固定存在的专用 thread。
   - 低成本、快模型即可。
   - 当前验证使用的 bridge thread：
     - `thread_id = 019db5e6-ba6a-7b80-95d2-a6867163281a`
     - `model = gpt-5.3-codex-spark-preview`
     - `reasoning_effort = low`
   - 职责只有一件事：轮询共享 job state，并在发现某个已绑定 caller thread 的 head batch 可投递时，用 `automation_update` 更新那个已知 caller heartbeat。

5. `caller thread heartbeat`
   - 不是常驻轮询器。
   - 第一版按“预绑定、运行期只更新”来建模。
   - 也就是：bootstrap 时创建并绑定；运行期只做激活、暂停和 prompt 更新，不再重定向到别的 thread。
   - 被唤醒后，在原 caller thread 中先通过 gated helper 开启 continuation，再继续原任务。
   - 第一版不要求它在关键路径上执行通用 `cbth job ...` CLI。
   - caller 真正开始 continuation 之前，必须先调用一个 gated helper：
     - `cbth desktop note-boundary-crossed ...`
   - `note-boundary-crossed` 的 success 返回必须同时代表：
     - boundary crossing 已 durable 记录
     - 当前 batch 已切到 `crossed_unacknowledged + replay_policy=manual_resolution_only`
     - caller 已获得继续消费当前 batch 所需的 payload / artifact access
     - 同时 durable 保存一份 operator-only `boundary_recovery_envelope`
   - 它必须发生在真正跨过 continuation boundary 之前：
     - 例如真正开始产出后续 assistant 文本
     - 或真正开始发起下一个基于该 batch 的工具 / 行动步骤
   - 只有 `note-boundary-crossed` 成功返回后，caller 才允许继续执行后续 continuation。
   - 但这不等于外围系统替 caller 后续动作兜底成“低风险”：
     - 如果 caller 之后决定发起 approval / network / write 工具
     - 仍然完全受 Codex 自己的审批与沙箱规则约束
   - 如果 caller 在 `note-boundary-crossed` 成功返回前崩溃，batch 仍可 redelivery。
   - 如果 caller 已经成功写入 `note-boundary-crossed` 后再崩溃，batch 必须保持 `manual_resolution_only`，不得自动 redelivery。
   - 因此，`note-boundary-crossed` 就是 v1 的最后一个自动 durable 断点：
     - crossing 之后不再尝试自动把 batch 收口成 “已送达”
     - 后续只允许 operator close，或等待 `redelivery_window_ends_at` 超时关闭
   - 这个已绑定 automation 的生命周期合同是：
     - 正常路径下只由 bridge / operator `pause` / `update` / `reuse`
     - caller 自己不直接 `pause` / `delete`
     - stale wake、snapshot 不可读、boundary 已记录后的后续 wake、degraded 都先 no-op / helper writeback，再由 bridge 后续 reconciliation 切回 `PAUSED`
     - 不在正常投递路径里 `delete`
     - 只有明确的 operator unbind / destroy 才允许删除它
   - `armed_generation` 是这个长期复用 caller heartbeat 的 generation 栅栏：
     - bridge arm 成功并 `note-arm` durable 后，才允许把 `armed_generation` 推进到当前 generation
     - bridge 后续做 pause/reconcile 时，也必须比较自己正在清理的 generation 是否仍等于 `armed_generation`
     - 只要 binding 上的 `armed_generation` 已经变成更新 generation，旧 generation 的 cleanup/pause 就必须 no-op
   - 每次成功 arm 还必须同时设置 `pause_not_before` 与 `pause_deadline`：
     - 这次 caller wake 只能被视为 one-shot wake window，而不是长期保持 `ACTIVE`
     - `pause_not_before` 是 bridge 最早允许尝试 pause 当前 generation 的时间
     - `pause_deadline` 是 bridge 最迟必须完成 pause/reconcile 的时间
     - `pause_not_before` 必须至少覆盖“一次完整 caller heartbeat 周期 + scheduler jitter budget”
     - 在 Desktop v1 固定 `FREQ=MINUTELY;INTERVAL=1` 的合同下，推荐：
       - `pause_not_before >= last_delivery_attempt_at + 90s`
       - `pause_deadline >= pause_not_before + 90s`
     - 在 `pause_not_before` 之前，bridge 不得因为普通 cleanup 直接把当前 generation 切回 `PAUSED`
     - bridge 必须在最迟到达 `pause_deadline` 的 reconciliation 中把该 generation 切回 `PAUSED`
     - 如果 pause 在限定重试窗口内仍无法验证成功，则 binding 必须进入 `degraded`

## 设计原则

- 长等待放在 sidecar，不放在 Codex 当前 turn。
- 周期性检查集中在 bridge thread，不污染所有 caller thread。
- caller thread 只在“确实有结果可消费”时被唤醒。
- 不要求 bridge thread 与 caller thread 之间直接 live push；两者都只依赖 automation scheduler 和共享 job state。
- 关键读取路径优先只读，但 Desktop 第一版允许一个窄的 `helper_cli_read` fallback。
- 第一版自动续跑只处理只读 batch：
  - `delivery_read_only=true`
  - `delivery_requires_approval=false`
  - `delivery_requires_network=false`
  - `delivery_requires_write_access=false`
- 但这还不是充分条件；Desktop 自动续跑还必须额外满足：
  - 当前安装选定 `read_transport` 已验证可无审批执行
  - 当前 binding 的 `writeback_capability=validated`
  - 对 `requires_artifact_read=true` 的 batch，当前 binding 的 `artifact_read_capability=validated`
  - 也就是 `note-arm-pending` / `note-arm` / `note-boundary-crossed` 这组窄 helper 已经被证明可在 heartbeat 中无审批执行
- 不满足这些条件的 batch 不得由 bridge 自动 arm caller heartbeat；它们保留为 manual/operator follow-up。
- 这里的“只读 / 低风险”只描述 bridge 自动投递与断点写回这条外围机制本身。
- caller 被唤醒之后如果决定发起 approval / network / write 工具，那仍然受 Codex 自己的审批与沙箱约束；这不属于本项目 v1 自动续跑门槛已经替你兜底的范围。
- 运行期 bridge 不得 blind create caller heartbeat；它只能更新已绑定 automation。
- 旧 heartbeat prompt 必须能够通过 attempt token / generation 检测自己已经过期，并立即 no-op。
- 旧 heartbeat prompt 即使被延迟唤醒，也不得直接 `pause` 当前这个长期复用的 caller heartbeat；否则会把新 generation 的合法 wake 一起关掉。

## 时序

### 1. 用户发起后台任务

在 caller thread 中：

1. Codex 把任务交给 sidecar。
2. sidecar 创建一条 job 记录。
3. job 至少包含：
   - `job_id`
   - `source_thread_id`
   - `status`
   - `task_summary`
   - `updated_at`
4. caller thread 不长时间等待，当前 turn 可以结束。
5. 如果该 thread 还没有 desktop binding，job 仍可继续运行，但 bridge 不会对它做自动续跑。
6. daemon 把 ready jobs 聚合成该 thread 的 `delivery batch`，并物化 delivery envelope。

### 2. bridge thread 轮询

bridge heartbeat 每分钟醒一次：

1. 先做上一轮已 arm generation 的 pause / reconcile：
   - 每轮 wake 必须遵守 bounded work budget：
     - `max_reconcile_items_per_wake`
     - `max_reconcile_wall_time_ms`
     - `max_new_arms_per_wake = 1`
   - bridge 必须把 reconcile lane 与 fresh-arm lane 分开建模：
     - reconcile lane 先处理 overdue / safety-critical item
     - fresh-arm lane 最多为一个新的 ready caller 安排 wake
   - 如果 reconcile backlog 超出本轮预算：
     - 剩余 item 必须保持 durable 可见，留给下轮 wake
     - bridge 不得在单轮里无界循环
   - 单个 degraded / overdue binding 不得独占整个 bridge：
     - 只要存在不依赖同一 binding 安全收口的 ready thread
     - bridge 就必须尽量保留 fresh-arm lane 给其中一个 ready thread
   - ready 来源本身必须是 daemon-owned fair-ready order，而不是 bridge 本地临时挑选：
     - `ready-threads.json` 的 entry 顺序必须已经是 canonical fair order
     - `claim-next-ready` pure peek 返回的也必须是这个 fair order 的当前第一项
     - fair order 只包含当前 eligible ready thread：
       - 同 thread unresolved reconcile item 必须先把该 thread 排除在 eligible 集合外
       - degraded / capability-invalid binding 也不得继续占据 ready 首位
     - daemon 必须用 durable `ready_cursor` / `eligible_after` 等等价机制避免同一个 pre-accept 失败 candidate 在每轮里永久霸占第一项
   - 所有仍处于 `arm_pending` 的 attempt 都必须比新 arm 更优先被处理
   - 只要某个 thread 的 head attempt 仍是 `arm_pending`，bridge 就不得对同一 `attempt_id + generation` 重新 arm
   - arm-pending 的读取面必须是：

```text
preferred: ~/.cbth/inbox/arm-pending-bindings.json
fallback:  cbth desktop list-arm-pending --bridge-thread-id <thread_id> --json
```

   - 所有到达 `pause_deadline` 的 binding 都必须优先处理
   - 只有在 bridge 确认这些 one-shot wake 已被 pause 或进入 `degraded` 后，才允许继续 arm 新 batch
   - overdue binding 的读取面必须是：

```text
preferred: ~/.cbth/inbox/pause-due-bindings.json
fallback:  cbth desktop list-pause-due --bridge-thread-id <thread_id> --json
```

2. 按当前安装选定的 `read_transport` 读取 bridge 侧的 ready 来源：

```text
preferred: ~/.cbth/inbox/ready-threads.json
fallback:  cbth desktop claim-next-ready --bridge-thread-id <thread_id> --json
```

   - `claim-next-ready` 虽然名字里带 `claim`，但第一版必须是纯 read/peek helper：
     - 不得 reservation
     - 不得移动 head batch
     - 不得递增 attempt / batch 计数
     - 真正的 durable 推进只能从后续 `note-arm-pending` 开始；`note-arm` 只负责 `arm_pending -> cooldown`

3. 如果没有可投递 thread，本次 turn 直接结束。
4. 如果有可投递 thread：
   - 无论来自 `ready-threads.json` 还是 `claim-next-ready` helper，都必须直接拿到一个 ready entry：
     - `source_thread_id`
     - `batch_id`
     - `attempt_id`
     - `generation`
     - `snapshot_revision`
     - `snapshot_path`
     - `requires_artifact_read`
   - 该 thread 必须已经存在 `binding_state=bound` 的 desktop binding
   - 且该 binding 必须同时满足：
     - `read_transport_capability=validated`
     - `writeback_capability=validated`
     - 如果当前 batch 的 `requires_artifact_read=true`：`artifact_read_capability=validated`
   - bridge 必须再根据 `source_thread_id` 查询 binding，解析当前唯一允许更新的 `caller_automation_id`
   - 用 `automation_update` 更新这个已知 caller heartbeat
   - heartbeat prompt 中带上：
     - `batch_id`
     - `attempt_id`
     - `generation`
     - `snapshot_revision`
   - 其中 `snapshot_path` 只属于 bridge 侧 internal ready-source locator：
     - 可用于 bridge 自己读取/核对 envelope
     - 不得写入 caller heartbeat prompt
   - 在真正调用 `automation_update` 前，bridge 必须先调用一个窄 helper，把当前 attempt durable 推到 `arm_pending`，同时 acquire 当前 generation 的 `bridge_arm_lease`：

```text
cbth desktop note-arm-pending --source-thread-id <thread_id> --attempt-id <attempt_id> --generation <generation> --bridge-request-id <request_id> --json
```

   - `note-arm-pending` 的实现合同必须是：
     - compare-and-swap：只允许当前 `prepared` 的 head attempt 唯一一次推进到 `arm_pending`
     - 这一步必须写下：
       - `bridge_request_id`
       - `bridge_arm_lease_id`
       - `bridge_arm_lease_deadline`
       - `arm_pending_since`
       - `arm_pending_deadline`
     - 如果同一 attempt 已经是 `arm_pending`：
       - 只有当前 durable `bridge_request_id` 与调用方相同，才允许返回 already-pending / idempotent success
       - 且必须返回同一个 `bridge_arm_lease_id`
       - 如果 durable `bridge_request_id` 不同，则必须返回 `lease-held` / `busy`，不得暴露现有 lease
     - stale/no-op：如果 `attempt_id` / `generation` 已经过期，就必须拒绝推进，也不能写新 deadline
   - 只有 `note-arm-pending` 成功并返回 `bridge_arm_lease_id` 后，bridge 才允许真正调用 `automation_update`
   - `automation_update` 成功后，bridge 调用一个窄 helper：

```text
cbth desktop note-arm --source-thread-id <thread_id> --attempt-id <attempt_id> --generation <generation> --bridge-request-id <request_id> --bridge-arm-lease-id <lease_id> --json
```

   - 这一步负责把 attempt durable 从 `arm_pending` 推进到 `cooldown`，并更新：
     - `head_attempt_id`
     - `last_delivery_attempt_at`
     - `delivery_attempt_count`
     - `armed_generation`
     - `pause_not_before`
     - `pause_deadline`
   - `note-arm` 的实现合同必须是：
     - compare-and-swap：只允许当前 `arm_pending` 的 head attempt 成功推进一次
     - 且调用方回传的：
       - `bridge_request_id`
       - `bridge_arm_lease_id`
       - 都必须与当前 durable lease owner 一致
     - idempotent retry：如果同一 attempt 已经是 `cooldown`，重复调用只能返回 already-armed / idempotent success，不能重复计数
     - stale/no-op：如果 `attempt_id` / `generation` 已经过期，就必须拒绝推进，也不能递增计数
   - 如果 `automation_update` 已被接受，但 `note-arm` 不可用或返回 unknown：
     - bridge 不能立刻把这次 wake 视为歧义失败
     - 它必须先做一次 durable reconciliation：
       - 如果 attempt 已进入 `cooldown` 且 `armed_generation` 已等于当前 generation，则按成功处理
       - 如果同一 attempt 已经成功 `note-boundary-crossed`，则当前 head batch 必须保持 `manual_resolution_only`
       - 如果当前 generation 对应的 heartbeat 已能被证明重新 `PAUSED`，则当前 attempt 收敛到 `abandoned`，head batch 保持 `replay_policy=automatic`
     - 只有在仍无法证明“已成功 arm”或“已成功 pause”时，才允许把当前 head batch 打到 `manual_resolution_only` 并把 binding 置为 `degraded`
   - `arm_pending_deadline` 到期时，这个 reconcile 必须强制完成收敛，禁止无限停留在 `arm_pending`：
     - 能证明 attempt 已进入 `cooldown`：按成功 arm 处理
     - 能证明这次 arm 从未真正生效、且当前 generation 对应 heartbeat 仍保持 `PAUSED`：attempt -> `abandoned`，head batch 保持 `replay_policy=automatic`
     - 两者都无法证明：attempt -> `abandoned`，head batch -> `manual_resolution_only`，binding -> `degraded`

### 3. caller thread 被唤醒

caller thread heartbeat 在下一次调度中醒来：

1. caller 不得先直接读取 per-thread envelope / artifact payload。
2. caller 必须先调用一个 gated helper：

```text
cbth desktop note-boundary-crossed --source-thread-id <thread_id> --attempt-id <attempt_id> --generation <generation> --expected-snapshot-revision <revision> --json
```

3. 这个 helper 必须一次性完成：
   - fresh compare-and-swap 校验
   - attempt 已经 durable 处于 `cooldown`
   - binding 上的 `armed_generation` 仍等于当前 `generation`
   - `continuation_boundary_state=not_crossed -> crossed_unacknowledged`
   - `replay_policy=automatic -> manual_resolution_only`
   - durable 保存 operator-only `boundary_recovery_envelope`
   - 返回 caller 继续消费当前 batch 所需的 payload / artifact access
4. 只有 `note-boundary-crossed` fresh success 后，caller 才允许继续后续工作。
5. `note-boundary-crossed` 的调用时机必须是：
   - caller 还没有看到当前 batch 的 payload / artifact 内容
   - 即将跨过 continuation boundary
   - 但还没有真正开始产生后续输出 / 工具副作用
   - 如果 caller 醒来时 `note-arm` 尚未 durable 成功，`note-boundary-crossed` 必须返回 not-armed-yet / stale-no-op，caller 直接退出
   - helper 在同一次 success 返回中完成 boundary crossing durable write，并返回 payload / artifact access
   - 如果同一 `(attempt_id, generation, snapshot_revision)` 之前已经成功 crossed，但 caller 丢失了 response，自动 caller path 不得继续；只能转入 operator recovery
   - 只有返回 fresh success 时 caller 才允许继续；返回 `already-crossed` / stale-no-op / error 时都必须立即停止并退出，不得继续
6. 对大结果，caller 只允许在 `note-boundary-crossed` success 之后，再调用：

```text
cbth desktop read-artifact --artifact-id <artifact_id> --artifact-read-lease-id <lease_id> --offset <offset> --max-bytes <n> --json
```

7. 第一版不再提供自动 `note-delivered` 收口：
   - 无论后续是纯文本回复，还是工具 / 行动步骤
   - 只要已经跨过 continuation boundary，batch 都保持 `manual_resolution_only`
   - 后续只能由 operator close，或等待 `redelivery_window_ends_at` 超时关闭
8. 如果 caller 在被唤醒后决定继续走 approval / network / write 工具：
   - 这一步仍按 Codex 自己的审批与沙箱规则执行
   - 不被本文档前面的“只读 / 低风险 batch”门槛自动豁免

## 最小状态机

### Job 状态

- `running`
- `ready`
- `failed`
- `cancelled`

### Delivery batch 状态

- `queued`
- `materialized`
- `cooldown`
- `closed`

### Delivery attempt 状态

- `prepared`
- `arm_pending`
- `cooldown`
- `closed`
- `superseded`
- `abandoned`

### Thread 级约束

- 同一个 `source_thread_id` 只允许一个 in-flight delivery attempt。
- ready jobs 先进入 thread-scoped FIFO 队列，再聚合成 batch。
- bridge 只读取“当前可投递 head batch”的快照，不直接操作单个 job。

### Attempt 合约

- 每个当前可投递 head batch 都必须绑定一条 durable attempt 记录。
- attempt 至少包含：
  - `batch_id`
  - `attempt_id`
  - `generation`
  - `snapshot_revision`
  - `automation_id` (optional)
  - `snapshot_path`
  - `delivery_deadline`
  - `arm_pending_deadline`
  - `cooldown_until`
- bridge arm caller heartbeat 前，必须先原子创建/更新 attempt，并在真正调用 `automation_update` 前先把它 durable 推到 `arm_pending`。
- caller prompt 中必须显式携带 `batch_id + attempt_id + generation + snapshot_revision`。
- `snapshot_path` 只用于 bridge 侧直接读取 / 核对 envelope，不属于 caller prompt token。
- `note-boundary-crossed` 的 success 返回也必须回显这四者；caller 必须先比较 helper 返回值与 prompt 期望值是否一致，只要 mismatch 就立即 no-op。
- 同一 thread 上出现新的 generation 后，所有旧 heartbeat prompt 都只能看到 mismatch，不得重复消费当前 head batch。
- 第一版不要求 `cbth` 在关键路径上同步拿到 `automation_id`。
- 对第一版来说：
  - `attempt_id + generation + snapshot_revision + envelope header` 才是防止 stale wake 的硬约束
  - `caller_automation_id` 来自 binding，而不是运行期 discovery
  - `automation_id` 只是 bridge 侧可选的协调/诊断信息

## 共享状态面的推荐接口

优先建议对 Desktop heartbeat 暴露统一 delivery envelope，而不是让 prompt 直接理解内部 SQLite。

### Bridge 侧

```text
~/.cbth/inbox/ready-threads.json
```

### Operator / diagnostic exports

```text
~/.cbth/inbox/by-thread/<thread_id>.json   # optional diagnostic export, disabled by default
~/.cbth/artifacts/<artifact_id>/manifest.json   # diagnostic / operator path only
~/.cbth/artifacts/<artifact_id>/payload   # diagnostic / operator path
```

- 它们不属于 automatic caller path。
- automatic caller continuation 在 `note-boundary-crossed` 之前没有稳定的 file-read 接口可用。
- 即使这些导出存在，也只能用于 operator / debug，不得被 caller prompt 当作 pre-boundary payload source。

### Helper fallback

```text
cbth desktop list-arm-pending --bridge-thread-id <thread_id> --json
cbth desktop list-pause-due --bridge-thread-id <thread_id> --json
cbth desktop note-arm-pending --source-thread-id <thread_id> --attempt-id <attempt_id> --generation <generation> --bridge-request-id <request_id> --json
cbth desktop claim-next-ready --bridge-thread-id <thread_id> --json
cbth desktop note-boundary-crossed --source-thread-id <thread_id> --attempt-id <attempt_id> --generation <generation> --expected-snapshot-revision <revision> --json
cbth desktop read-artifact --artifact-id <artifact_id> --artifact-read-lease-id <lease_id> --offset <offset> --max-bytes <n> --json
cbth desktop note-arm --source-thread-id <thread_id> --attempt-id <attempt_id> --generation <generation> --bridge-request-id <request_id> --bridge-arm-lease-id <lease_id> --json
```

`read-artifact` 的第一版返回合同至少包括：

- `artifact_id`
- `artifact_read_lease_id`
- `artifact_read_lease_deadline`
- `content_type`
- `size_bytes`
- `offset`
- `bytes_returned`
- `data_base64`
- `next_offset`
- `eof`

也就是说，大 artifact 的后续读取不能只靠 `artifact_id`；必须同时带上 `note-boundary-crossed` success 返回的 `artifact_read_lease_id`。
这个 lease 还必须是短寿命 continuation lease，而不是长期 artifact bearer token：

- 它只对当前 `crossed_unacknowledged` 的同一 `(attempt_id, generation, snapshot_revision)` 有效
- 一旦 batch 被 close、attempt 被 supersede / abandon、operator repair / unbind 收口，旧 lease 必须立即失效
- 如果 caller 没拿到第一次 `note-boundary-crossed` success 的返回值，自动 caller path 不得重新申请新的 artifact lease；只能通过 operator recovery 读取 durable `boundary_recovery_envelope` / manifest 做人工收口
- 对大 artifact，这个 operator recovery 还必须闭环成：
  - `cbth batch inspect-head ...` 返回 operator-only `artifact_recovery_lease_id`（或等价 re-lease surface）
  - 这样人工/operator 才能继续调用 `cbth desktop read-artifact ...` 完成收口

也就是说，`helper_cli_read` 对大 artifact 的 fallback 不是返回一个路径，而是返回一个显式 chunked payload 协议。

这样 bridge prompt 和 caller prompt 都可以很短，而且不需要知道底层 store 是 SQLite、普通文件还是 `mmap`。

## Automation 策略

### Bridge heartbeat

- 常驻、低成本。
- 固定 1 分钟 cadence 即可。
- 挂在专用 bridge thread 上。
- 只负责读取 ready index，并为有可投递 batch 的已绑定 caller thread arm heartbeat。

### Caller heartbeat

- 不做固定轮询。
- 只在 bridge 发现当前 thread 有可投递 batch 时更新已绑定 heartbeat。
- 目标是“一次唤醒、一次读取、一次继续”。

### 对 `run now` 的态度

- UI 和 app bundle 中可以看到 `run now` 语义。
- 但当前还没有把它当成模型可稳定调用的独立 tool 来依赖。
- 第一版实现应只依赖 `FREQ=MINUTELY;INTERVAL=1` 这一条已实证可行的 heartbeat 机制。

## Prompt 合约

### Bridge prompt 要求

- 每次醒来只做一次状态检查。
- 每次醒来先做 pause / reconcile，再决定是否 arm 新 batch。
- 每次醒来必须遵守 bounded work budget：
  - reconcile work 超预算时只 durable defer，不得在本轮无界循环
  - fresh-arm lane 最多处理一个新的 ready caller
  - 只要存在不依赖同一 binding reconcile 的 ready thread，就不得让 unrelated ready work 被单个坏 binding 永久饿死
- ready 选择必须尊重 daemon 提供的 canonical fair order；bridge 不得自行按本地启发式重排 ready thread。
- 只读取 ready index，不依赖通用 `cbth job ...` CLI。
- 没有 ready thread 就立即结束。
- 有 ready thread 时，只更新对应 caller thread 的已绑定 heartbeat，不直接展开主任务。
- 运行期不得 blind create 新 caller heartbeat automation。
- arm 完成后如果能直接拿到 `automation_id`，可以把它写进 prompt / automation metadata 作为协调信息；拿不到时也不能阻塞关键路径。
- `claim-next-ready` 如果被使用，必须保持纯 read/peek 语义；不得把 batch 隐藏到 bridge 私有 reservation 里。
- 运行期对这个长期复用 caller heartbeat 的 `pause` / `update` / `reuse` 只能由 bridge 或 operator 发起；caller prompt 本身不得直接 pause 它。
- bridge arm 的 durable 完成条件是：
  - attempt 已存在
  - snapshot 已物化
  - 当前 generation 的 caller heartbeat arm 请求已被 Codex 接受
  - `cbth desktop note-arm ...` 已成功执行
  - 并写下当前 generation 的 `pause_deadline`

### Caller prompt 要求

- 先通过 `cbth desktop note-boundary-crossed ...` 请求开启当前 batch 的 continuation。
- 只处理当前 helper success 返回所指向的 head batch。
- 对小结果可以直接消费 helper 返回的 inline payload；对大结果只允许在 helper success 之后继续读取 artifact 内容。
- 即使 prompt token 全匹配，也只有当 `replay_policy=automatic`、`continuation_boundary_state=not_crossed`、binding 仍是 `bound` 时才允许继续。
- 在 `note-boundary-crossed` success 之前，caller 不得直接读取 per-thread envelope / artifact payload，也不得开始输出或起工具。
- 一旦成功记录 `note-boundary-crossed`，当前 batch 就进入 `manual_resolution_only`：
  - 不再自动 redelivery
  - 不再尝试自动关闭为 “已送达”
  - 后续只能等待 operator close 或超时关闭
- caller 不直接 pause 当前 heartbeat；任务完成后的 pause/reconcile 由 bridge 在后续 heartbeat 中处理。
- 旧 generation 的 prompt 只允许 no-op，不允许“顺手处理当前 head batch”。
- 读取传输由 binding 预先决定：
  - v1 中它实际是 installation-wide 选择，再被 durable 镜像到 binding：
    - `direct_file_read`
    - 或 `helper_cli_read`

## Delivery 关闭语义

- Desktop 第一版的目标不是“精确证明 caller 已消费”，而是“在 batch 仍为 head 且允许重投时，至少安排一次 wakeup，并在必要时重投”。
- 因此：
  - `cooldown` 表示 bridge 已成功为 caller 安排了一次 heartbeat wakeup，且 `cbth` 正在等待这次 wakeup 的最短观察窗口
  - `closed` 表示 `cbth` 不会再自动为这个 batch 重新 arm heartbeat
- `closed` 不是以下任一命题的证明：
  - caller 一定读取了 snapshot
  - caller 一定读取了 artifact
  - caller 一定完成了后续工作
- 第一版推荐的 `close_reason` 至少包括：
  - `delivered`
  - `superseded`
  - `operator_confirmed_delivery`
  - `operator_closed_unconfirmed`
  - `cancelled`
  - `redelivery_window_exhausted`
  - `manual_resolution_expired`
  - `max_attempts_exhausted`
- 其中：
  - `delivered` 是共享核心的 canonical 枚举值，供 CLI 等可信自动送达路径使用
  - Desktop v1 自动路径默认只会产出其余几类 `close_reason`，不会在 post-boundary 阶段自动写出 `delivered`
  - `superseded` 只用于整个 batch 被新的 `batch_id` / compaction result / operator decision 取代
  - 同一 batch 内创建新 redelivery attempt 不属于 batch 级 `superseded`
- 也就是说，Desktop 第一版的自动续跑语义是：
  - `at-least-once wakeup scheduling`
  - not `exactly-once consumption`
- 如果 `delivery_attempt_count >= max_delivery_attempts`，该 head batch 也必须自动进入：
  - `close_reason=max_attempts_exhausted`
  - `closed`

## 失败与重试

### Sidecar 还没完成

- bridge thread 下次 heartbeat 再检查。

### Bridge arm 失败

- 如果失败发生在 `automation_update` 被接受之前：
  - 如果失败发生在 `note-arm-pending` 之前：
    - batch 保持可投递
    - 当前 attempt 标为 `abandoned` 或保持 `prepared`，由调度器决定是否重建新 attempt
  - 如果失败发生在 `note-arm-pending` 之后、`automation_update` 之前：
    - 当前 attempt 先保持 `arm_pending`
    - bridge 下一轮必须先 reconcile 这个 `arm_pending` attempt，而不是直接重 arm
    - 一旦 `arm_pending_deadline` 到期，reconcile 必须把它收敛到：
      - `cooldown`
      - 或 `abandoned + replay_policy=automatic`
      - 或 `abandoned + replay_policy=manual_resolution_only + binding_state=degraded`
- 下一次 bridge heartbeat 只能基于新的 head attempt 再试
- 如果 `automation_update` 已被接受，但 `note-arm` 没能 durable 成功：
  - bridge 不得立刻把它视为歧义失败
  - 它必须先做 reconciliation：
    - 如果 attempt 已进入 `cooldown` 且 `armed_generation` 匹配，则按 arm 成功处理
    - 如果当前 generation 对应的 heartbeat 已能被证明重新 `PAUSED`，则当前 attempt 收敛到 `abandoned`，head batch 继续保持 `replay_policy=automatic`
  - 只有在既无法证明 arm 成功、也无法证明 pause 成功时：
    - 当前 head batch 才进入 `replay_policy=manual_resolution_only`
    - 当前 binding 才进入 `degraded`
    - 后续任何 repair / re-arm 前都必须先验证 bound heartbeat 已被 `PAUSED`

### Caller heartbeat 醒来但 snapshot 不可读

- 说明快照暂时不可用、路径变化，或当前 batch 已被撤回。
- 当前 turn 直接退出并 no-op。
- 后续 pause/reconcile 由 bridge 完成；caller 不直接 pause 这个长期复用 heartbeat。

### Caller heartbeat 醒来但 attempt mismatch

- 说明这是旧 generation 的 stale wake，或者该 batch 已被 supersede。
- 当前 turn 必须 no-op 并退出，不得尝试消费当前 head batch。
- 后续 pause/reconcile 同样由 bridge 完成。

### Caller 处理中途失败

- 第一版不依赖 caller 回写失败状态。
- `cbth` 通过保留 batch、cooldown 与 redelivery timeout 决定是否再次 arm。
- 如果 `cooldown_until` 到期后，该 batch 仍然是当前 head batch，且 `replay_policy=automatic`、`close_reason` 仍为空、`now < redelivery_window_ends_at`、并且 `delivery_attempt_count < max_delivery_attempts`，就应该创建新 attempt 并再次 arm，而不是把旧 attempt 直接视为成功送达。

### Caller 请求开启 continuation，但 `note-boundary-crossed` 失败

- 如果 caller 还没成功得到 `note-boundary-crossed` 的 success 返回，就不得真正跨过 continuation boundary。
- 因此，如果 caller 在请求开启 continuation 时，`cbth desktop note-boundary-crossed ...` 失败：
  - caller 必须立即停止，不得继续产出后续输出或工具副作用
  - 当前 head batch 保持可自动 redelivery
  - 如果失败原因表明 helper / binding 已失效，则 binding 可以进入 `degraded`

### Binding degraded

- `binding_state=degraded` 表示该 thread 暂时失去自动续跑能力，但 job / artifact 仍可继续累积。
- degraded 之后：
  - bridge 不得再为该 thread 自动 arm caller heartbeat
  - 当前 in-flight attempt 必须收敛到 `abandoned`
  - 当前 head batch 保持未关闭
  - 调度器只保留结果与元数据，不继续自动 redelivery
- operator 必须通过显式 CLI 路径来解开这个状态，至少支持两类动作：

```text
cbth desktop binding repair --source-thread-id <thread_id> --caller-automation-id <automation_id> --json
cbth desktop installation-state repair --read-transport <transport> [--read-transport-capability <state>] [--artifact-read-capability <state>] [--writeback-capability <state>] --json
cbth batch close-head --source-thread-id <thread_id> --reason operator_closed_unconfirmed --json
cbth batch close-head --source-thread-id <thread_id> --reason operator_confirmed_delivery --json
cbth batch inspect-head --source-thread-id <thread_id> --json
cbth desktop binding unbind --source-thread-id <thread_id> --delete-automation <true|false> --json
```

  - 推荐语义：
  - `binding repair`：
    - 重新验证 binding-scoped 条件：
      - paused status
      - `caller_automation_id` 是否仍指向当前 `source_thread_id`
      - 当前 binding 镜像是否匹配 installation state
    - 不得直接切换 installation-wide `read_transport`
    - 不得直接写入 installation-wide capability 结论
    - 如果 operator 提供新的 `caller_automation_id`：
      - 必须证明该 automation 仍然 `target_thread_id == source_thread_id`
      - 必须证明它当前没有被别的 binding 占用
      - 必须优先证明旧 `caller_automation_id` 已 quiesced / deleted；如果做不到，也只能在强制轮换新的 fresh attempt / generation 之后恢复自动续跑
    - 成功返回的 binding snapshot 必须回显 `artifact_read_capability`
    - 只有 installation state 当前已经满足所需 capability 时，才允许把 binding 从 `degraded` 恢复到 `bound`
    - 只对“尚未成功写入 `note-boundary-crossed`”的失败允许把当前 head batch 重新放回可投递状态
    - 但凡 repair 过程中更换了 `caller_automation_id`，或旧 automation 的 quiesce 无法被证明：
      - 不得复用当前 head batch 的旧 attempt / generation
      - 必须先强制切换到新的 fresh attempt / generation，再允许后续自动 arm
    - 如果 degraded 的来源是 `note-arm` outcome unknown 这类 post-boundary / post-arm 歧义场景，`binding repair` 不得自动重投当前 head batch
    - 它恢复的是当前 caller heartbeat 与后续调度能力；但在当前 head batch 被显式关闭或超时自动关闭前，FIFO 仍会被这个 head batch 挡住
  - `installation-state repair`：
    - 是唯一允许切换 installation-wide `read_transport` 的 operator 路径
    - 也是唯一允许写 installation-wide capability 结论的路径
    - 成功时必须原子更新 installation state，并递增 `read_transport_generation`
    - capability 结论必须和当前 `validation_fingerprint` 一起写入；fingerprint 变化会让旧 validated 结论失效
    - 如果 `read_transport` 发生变化而 capability 参数未显式提供：
      - 必须把 `read_transport_capability`
      - `artifact_read_capability`
      - `writeback_capability`
      - 全部原子重置为 `unknown`
      - 清空 `validated_at`
    - 同时把所有镜像不再匹配的 bindings 推到 `degraded`
  - `batch close-head`：
    - 显式关闭当前 head batch
    - 让后续 FIFO 队列继续前进
- 第一版安全默认值是：
  - post-continuation-boundary 的歧义 batch 只能人工 close
  - 不提供自动 replay
  - 但如果 operator 长时间不处理，仍在 `redelivery_window_ends_at` 到期时自动关闭为 `manual_resolution_expired`，以释放 FIFO/GC
  - 未来如果要支持人工 replay，必须单独引入明确的 operator override contract，而不是复用普通 `binding repair`

## Bootstrap 约束

- Desktop 第一版不是零配置 attach。
- 一个 caller thread 要支持自动续跑，必须先完成 bootstrap：
  - 创建或接管一个 caller heartbeat automation
  - 把该 `caller_automation_id` durable 绑定到 `source_thread_id`
  - 为该 thread 选择 `read_transport`
- bootstrap 不能只相信一次 `status=PAUSED` 请求。
- 由于已观察到“创建时请求 `PAUSED` 但实际落成 `ACTIVE`”的 quirk，bootstrap 必须：
  - create/update 后立刻读回 automation 状态
  - 必要时再次 pause/update
  - 只有在最终状态被验证为 paused 后，binding 才允许进入 `bound`
- 如果 pause 状态无法被验证，binding 必须保持 `degraded` 或 `unbound`，而不是继续接受自动续跑。

## Caller Heartbeat 生命周期

- 预绑定的 `caller_automation_id` 是一个长期复用的 heartbeat automation，不是一次性 disposable automation。
- 第一版规则：
  - ready 时：bridge 更新 prompt 并切到 `ACTIVE`
  - 正常送达后：bridge 在后续 reconciliation 中、且只在目标 generation 仍等于 `armed_generation` 时，把它切回 `PAUSED`
  - stale wake / snapshot 不可读：caller turn 只 no-op；后续由 bridge 在 generation 仍匹配时切回 `PAUSED`
  - degraded：bridge 或 operator 在 generation 仍匹配时切回 `PAUSED`，等待 repair
  - 只有 operator 明确执行 `cbth desktop binding unbind ...`，才允许删除
- 换句话说，第一版 caller heartbeat 必须被当成“长期复用的 one-shot wake carrier”：
  - 每次 arm 只授权一个有限的 wake window
  - 不能无限期停留在 `ACTIVE`
- 未完成 bootstrap 的 thread 仍可提交 job，但只允许：
  - sidecar 继续跑任务
  - `cbth` 保留结果
  - 不允许 bridge 自动 arm caller heartbeat

## Artifact 生命周期

- artifact 的持有责任完全归 `cbth`，不归外部任务脚本。
- 第一版默认规则：
  - `min_artifact_ttl = 24h`
  - `post_close_ttl = 72h`
- 只要仍有非终态 batch 引用某个 artifact，就绝不能 GC。
- 当最后一个引用 batch 进入终态后，artifact 仍至少保留 `post_close_ttl` 作为排障窗口。
- caller thread 不负责删除 artifact，也不负责决定何时可清理。

## 已实证支持的关键能力

- 当前 thread 可创建指向其他 thread 的 heartbeat automation。
- heartbeat turn 内部可以调用 `automation_update`。
- bridge thread 可在自己的 heartbeat turn 中，把 automation retarget 到 caller thread。
- retarget 完成后，caller thread 会在下一分钟被成功唤醒。

## 当前不打算做的事

- 不尝试外部进程直接向 Desktop 当前 live thread push 消息。
- 不依赖外部直接改 Codex automation DB。
- 不把后台 heartbeat 对通用 `cbth job ...` CLI 的执行能力当成关键前提。
- 不以“不留下任何 caller thread 唤醒痕迹”为目标；当前目标是把痕迹压缩到“任务 ready 时的一次唤醒”。
- 不覆盖 Desktop app 退出后的常驻通知场景。
- 不把“单次读取后立即删除 artifact”当成第一版语义；artifact 生命周期由 `cbth` 统一管理。

## 第一版实现建议

1. 固定一个 bridge heartbeat thread。
2. 让 sidecar 只负责写 job 状态与完成结果。
3. 由 `cbth` 自己把结果 ingest 到 managed artifact store。
4. 由 `cbth` 物化 `ready-threads.json` 与 bridge-side inbox snapshot。
   - 这些自动路径 snapshot 只包含 ready/reconcile metadata，不包含 pre-boundary payload / artifact 内容。
5. 让 bridge thread 每分钟读取 `ready-threads.json`。
6. bridge 发现可投递 batch 后，为对应 caller thread arm 一次 heartbeat。
7. caller thread 醒来后先调用 `note-boundary-crossed`，拿到 gated payload / artifact access 后继续任务。

这套方案的关键优点是：

- 不改 Codex Desktop 内部实现。
- 不依赖外部 live push。
- 不依赖后台 heartbeat 执行通用 `cbth job ...` CLI。
- 如果 `direct_file_read` 不成立，窄 helper 执行能力仍需单独验证。
- 只把只读快照路径当作候选主路径，直到 heartbeat 无审批读取得到实证。
- 不需要用户手动切回某个 notification thread。
- caller thread 可以在原上下文内继续任务。
