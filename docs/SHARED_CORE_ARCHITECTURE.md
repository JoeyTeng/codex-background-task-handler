# Shared Core Architecture

## 目标

- 不修改上游 `codex` 仓库。
- 用一套共享核心同时支撑 CLI 与 Desktop 两条集成路径。
- 尽量保持部署简单：单个 Rust binary、本地状态、本地 IPC。
- 长时间任务的生命周期独立于单次 Codex 前台交互。
- 第一版只暴露稳定的 CLI 外部接口，不提前冻结 socket / Web / plugin API。

## 术语

- 下文用 `cbth` 作为示例 binary 名称，占位表示未来的主 Rust binary。
- `daemon` 指本项目自己的本地后台进程，不是系统级 `launchd` / `systemd` 服务。
- `integration adapter` 指 CLI 或 Desktop 这类接到 Codex 的薄接入层。
- `job` 指一个被后台系统追踪的长时间任务。
- `artifact` 指由 `cbth` 管理和保留的任务产物，不是外部脚本临时路径本身。
- `thread inbox` 指 `cbth` 为某个 `source_thread_id` 物化出的只读投递视图。
- `delivery batch` 指对同一 caller thread 的一组有序任务结果投递单元。

## 已敲定的取舍

### 1. 单 binary，多入口

- 第一版实现目标是一个主 Rust binary。
- CLI 集成只是这个 binary 的一个入口。
- Desktop 集成也是这个 binary 的另一个入口。
- 共享的 store、状态机、daemon、artifact 管理和 sidecar runtime 都归在同一个核心里。

### 2. 不做系统级常驻服务

- 第一版不要求用户安装 `launchd`、`systemd` 或 Windows Service。
- 核心进程采用按需启动的本地 daemon 模式：
  - 有命令调用时，如 daemon 不存在则自动拉起。
  - 有 active jobs 时，即使前台 CLI / Desktop 实例退出，daemon 也可继续活着。
  - 当且仅当以下条件同时满足时，daemon 才允许在 idle timeout 后自动退出：
    - 没有 active jobs
    - 没有活跃接入端
    - 没有未收口的 delivery batches / attempts
    - 没有等待中的 `arm_pending_deadline` / `pause_deadline` / `cooldown_until` / `redelivery_window_ends_at` / `delivery_turn_id` 完成观察

### 3. 第一版公共接口只做 CLI

- 第一版不承诺稳定的 socket API。
- 第一版不承诺稳定的 Web API。
- 第一版不承诺动态插件加载协议。
- 第一版唯一稳定的外部接入面是 CLI 命令。
- 任何外部脚本、future plugin、future Web bridge，都先通过 CLI 命令与核心系统交互。

### 4. Desktop 关键路径优先只读

- 第一版不把“Desktop heartbeat turn 能稳定执行通用 `cbth job ...` CLI”当作既定前提。
- Desktop adapter 的稳定关键路径应优先依赖：
  - bridge 侧只读 inbox snapshot 文件
  - caller 侧由窄 helper 原子开启 continuation，并在同一次 helper 成功返回后才暴露 payload / artifact 读取入口
- 只有在 bridge 侧 `direct_file_read` 不成立、且 helper 执行能力已被单独验证后，才条件性依赖读 helper：
  - `cbth desktop list-arm-pending ...`
  - `cbth desktop list-pause-due ...`
  - `cbth desktop claim-next-ready ...`
- 当前无论读路径怎么选，写回 helper 仍是既定窄依赖：
  - `cbth desktop note-arm-pending ...`
  - `cbth desktop note-arm ...`
  - `cbth desktop note-boundary-crossed ...`
- 在 v1 自动 caller path 里，`cbth desktop note-boundary-crossed ...` 还是一个 gated access helper：
  - 成功时必须原子地完成 boundary crossing durable write
  - 并把当前 batch 的可继续消费 payload / artifact access 一并返回给 caller
- `cbth desktop read-artifact ...` 只允许在 `note-boundary-crossed` 成功之后，作为大 artifact 的后续 chunked 读取面存在。
- `cbth desktop note-delivered ...` 目前不属于第一版自动成功路径；它保留为未来可能的 post-output / post-side-effect ack 扩展点。
- 因此，Desktop 第一版的自动续跑门槛不是“batch 只读”单条件，而是两层同时成立：
  - batch 自身满足只读 / 低风险 delivery policy
  - 当前安装上的 Desktop 读路径已被验证可在 heartbeat 中无审批执行
  - 当前安装上的 Desktop writeback helpers 已被验证可在 heartbeat 中无审批执行
  - 对 `requires_artifact_read=true` 的 batch，当前 binding 上的 `artifact_read_capability=validated`
- 这里的“只读 / 低风险”只约束自动投递与断点写回这条外围机制本身。
- caller 被唤醒后的后续推理与工具选择仍受 Codex 自身的 sandbox / approval policy 约束；本项目不把这些后续动作一并宣称成“已被外围系统降成低风险”。
- Desktop 的关键投递路径优先依赖只读状态面：
  - bridge 侧只读 inbox snapshot 文件
  - caller 侧不在 boundary crossing 前直接读取 per-thread envelope / artifact 文件
- 后续内部实现可以用普通文件、`mmap` 或 shared memory 优化，但外部语义先固定为“读一个稳定路径下的只读快照”。
- 这条只读文件路径当前仍是第一版候选主路径，必须在 Desktop heartbeat 无审批读取实证通过后，才升级成“已验证主路径”。

### 5. CLI 依赖实验 RPC，但必须收口

- 目前没有公开稳定接口可替代“同一个 live CLI thread 自动继续”。
- 因此 CLI 集成需要使用共享 `app-server` 的实验 RPC。
- 但第一版必须：
  - 明确最小能力集
  - 启动时做 capability probe
  - 缺能力时 fail-closed
  - 把 `turn/steer` 仅当作受限优化，而不是主路径
  - 默认 shipping 配置下先关闭 `turn/steer`

## 共享组件

### 1. `daemon runtime`

职责：

- 持有 SQLite store。
- 维护 job 状态机。
- 管理 thread inbox、delivery batch、artifact retention。
- 作为 CLI / Desktop 接入层与长任务 runtime 的共同协调者。

### 2. `store`

第一版推荐：

- 本地 SQLite。

原因：

- 单机、单用户模型足够。
- 便于原子 lease、compare-and-swap 和 FIFO 队列管理。
- 不要求额外安装 Redis / Postgres。
- CLI 和 Desktop 都能复用同一套状态。

### 3. `artifact store`

职责：

- 接管 `cbth job complete --result-file <path>` 提交过来的外部产物。
- 将结果复制或 ingest 到 `cbth` 自己管理的 durable 路径。
- 为 Codex 侧提供稳定的内部 `artifact_id`、manifest 和读取路径。
- 统一负责 retention 与 GC。

关键语义：

- `--result-file` 只是提交输入，不是长期保留路径。
- 一旦 `cbth job complete` 成功，后续生命周期就归 `cbth`，不再依赖外部脚本原始文件是否仍存在。

### 4. `Desktop delivery envelope transports`

职责：

- 为 Desktop 和调试工具暴露统一的只读投递视图。
- 把“投递 envelope 的语义”与“具体读取传输”拆开。

第一版定义两种传输：

1. `direct_file_read`

```text
~/.cbth/inbox/ready-threads.json
~/.cbth/inbox/arm-pending-bindings.json
~/.cbth/inbox/pause-due-bindings.json
~/.cbth/inbox/by-thread/<thread_id>.json   # diagnostic / future caller path
~/.cbth/artifacts/<artifact_id>/manifest.json   # diagnostic / future caller path
~/.cbth/artifacts/<artifact_id>/payload   # diagnostic / future caller path
```

2. `helper_cli_read`

```text
cbth desktop list-arm-pending --bridge-thread-id <thread_id> --json
cbth desktop list-pause-due --bridge-thread-id <thread_id> --json
cbth desktop claim-next-ready --bridge-thread-id <thread_id> --json
cbth desktop note-boundary-crossed --source-thread-id <thread_id> --attempt-id <attempt_id> --generation <generation> --expected-snapshot-revision <revision> --json
cbth desktop read-artifact --artifact-id <artifact_id> --offset <offset> --max-bytes <n> --json
```

bridge 侧两种传输必须返回同一个 ready-entry schema。
caller 侧 automatic continuation 则必须通过 `note-boundary-crossed` success 返回来获得 payload / artifact access。

`helper_cli_read` 的合同要额外收紧：

- 它不是“完全摆脱本地 CLI 执行依赖”的路径。
- 它只是 Desktop 在 `direct_file_read` 失败时可考虑的窄 helper fallback。
- 在把它升级成正式受支持路径之前，必须单独验证：
  - heartbeat turn 无审批执行这些窄 `cbth desktop ...` helper
  - helper 返回的 envelope / artifact 内容与 `direct_file_read` 完全等价
- 因此，第一版当前真正的优先候选仍然是 `direct_file_read`；`helper_cli_read` 只是条件性 fallback，不应在文档里被表述成已验证主路径。

`direct_file_read` 的第一版候选路径：

```text
~/.cbth/inbox/ready-threads.json
~/.cbth/inbox/arm-pending-bindings.json
~/.cbth/inbox/pause-due-bindings.json
~/.cbth/inbox/by-thread/<thread_id>.json   # diagnostic / future caller path
~/.cbth/artifacts/<artifact_id>/manifest.json   # diagnostic / future caller path
~/.cbth/artifacts/<artifact_id>/payload   # diagnostic / future caller path
```

更新方式：

- 用 `write temp + rename` 原子替换。
- 外部语义固定为“读快照文件”。
- 后续如果内部改成 `mmap` / shared memory，只能在不改变这一语义的前提下做。
- `direct_file_read` 在 Desktop 无审批读取能力得到实证前，仍视为候选内部 contract，而不是已冻结接口。
- 如果 `direct_file_read` 无法满足无审批读取约束，Desktop 第一版只能切到“已单独验证过的 `helper_cli_read`”，否则就继续保留为候选方案；不能直接把未验证 helper 执行前提当主链路。
- 无论哪种传输，`~/.cbth` 根目录默认要求：
  - directory mode `0700`
  - regular file mode `0600`
  - 临时写入文件在 rename 前也必须保持同等权限

### 5. `local IPC`

职责：

- 让 CLI 子命令与 daemon 通信。
- 在 daemon 未启动时，支持 auto-start / reconnect。

第一版定位：

- 这是内部实现细节，不是对外承诺的稳定公共 API。
- 对外只承诺 CLI 子命令行为，不承诺 socket 协议长期兼容。

### 6. `job orchestrator`

职责：

- 创建 job。
- 迁移 job 状态。
- 记录 artifact、摘要、任务元数据。
- 把 ready jobs 排入 thread-scoped FIFO 队列。
- 处理重试与超时。

### 7. `thread delivery scheduler`

职责：

- 以 `source_thread_id` 为单位做仲裁。
- 把多个 ready jobs 合并成 `delivery batch`。
- 控制每个 thread 的最小连续发送间隔。
- 确保同一 thread 同时最多只有一个 in-flight delivery attempt。

### 8. `Codex integration adapters`

这是端侧专属层，但依赖同一套共享核心：

- CLI adapter：
  - daemon-owned shared `app-server`
  - `codex --remote`
  - capability probe
  - idle 时 `thread/resume + turn/start`
  - active 时只有在受限条件下才允许 `turn/steer`
- Desktop adapter：
  - desktop thread binding
  - bridge heartbeat thread
  - caller heartbeat
  - `automation_update` update/pause on a bound caller automation
  - bridge-side delivery envelope 读取（`direct_file_read` 或 `helper_cli_read`）
  - caller-side gated continuation access (`cbth desktop note-boundary-crossed ...`)
  - narrow helper writeback (`cbth desktop note-arm-pending ...`, `cbth desktop note-arm ...`, `cbth desktop note-boundary-crossed ...`)

### 9. `desktop thread bindings`

职责：

- 把某个 Desktop source thread 绑定到一个稳定的 caller heartbeat automation。
- 让 bridge 在运行期只做“更新已知 automation”，而不是 blind create / discover。
- 记录当前 Desktop 安装已选定的 delivery-envelope 读取传输快照。

关键字段：

- `binding_id`
- `source_thread_id`
- `binding_state`
  - `unbound`
  - `bound`
  - `degraded`
- `caller_automation_id`
- `armed_generation` (optional)
- `pause_not_before` (optional)
- `pause_deadline` (optional)
- `bridge_thread_id`
- `read_transport`
  - `direct_file_read`
  - `helper_cli_read`
- `read_transport_capability`
  - `unknown`
  - `validated`
  - `unavailable`
- `artifact_read_capability`
  - `unknown`
  - `validated`
  - `unavailable`
- `writeback_capability`
  - `unknown`
  - `validated`
  - `unavailable`
- `created_at`
- `updated_at`
- `last_verified_at`

约束：

- Desktop 自动续跑只对同时满足以下条件的 thread 生效：
  - `binding_state=bound`
  - `read_transport_capability=validated`
  - 对 `requires_artifact_read=true` 的 batch，`artifact_read_capability=validated`
  - `writeback_capability=validated`
- Desktop v1 不支持同一安装里 mixed `read_transport` bindings：
  - 同一 Desktop 安装只允许一个 installation-wide `read_transport`
  - binding 上的 `read_transport` 只是这个安装当前选定 transport 的 durable 镜像，用于 bootstrap 校验、诊断和 stale-binding 检测
  - 如果 binding 记录的 `read_transport` 与安装当前 transport 不一致，该 binding 必须进入 `degraded` 或重新 bootstrap
- `unbound` thread 可以继续提交 job，但 bridge 不得尝试自动 arm caller heartbeat。
- 运行期 bridge 不负责发现新的 caller automation id；第一版要求这个 id 通过 bootstrap 预先 durable 绑定。
- `degraded` 表示该 thread 暂时失去自动续跑能力：
  - bridge 不再自动 arm
  - 当前 attempt 必须收敛到 `abandoned`
  - 当前 head batch 保持未关闭，等待 operator 恢复或人工处理
- `armed_generation` 是这个长期复用 caller heartbeat 的 generation CAS 栅栏：
  - bridge arm 成功并 `note-arm` durable 后，才允许把它更新为当前 generation
  - 后续 bridge 想把该 heartbeat 切回 `PAUSED` 时，也必须带着期望 generation 比较 `armed_generation`
  - 只要 binding 上的 `armed_generation` 已经变成更新 generation，任何旧 generation 的 cleanup/pause 都必须 no-op
- 每次成功 arm 还必须同时设置 `pause_not_before` 与 `pause_deadline`：
  - `pause_not_before` 表示 bridge 最早允许尝试把这次 one-shot wake 对应的 caller heartbeat 切回 `PAUSED` 的时间
  - `pause_deadline` 表示 bridge 最迟必须完成这次 pause/reconcile 的时间
  - `pause_not_before` 必须至少覆盖“一次完整 caller heartbeat 周期 + scheduler jitter budget”
  - 在 Desktop v1 固定 `FREQ=MINUTELY;INTERVAL=1` 的合同下，推荐：
    - `pause_not_before >= last_delivery_attempt_at + 90s`
    - `pause_deadline >= pause_not_before + 90s`
  - 在 `pause_not_before` 之前，bridge 不得因为普通 cleanup 直接把当前 generation 切回 `PAUSED`
  - bridge 的下一轮 reconciliation 必须优先处理所有已到 `pause_deadline` 的 binding
  - 如果 pause 在限定的 bridge 重试窗口内仍无法被验证，binding 必须进入 `degraded`
- 如果某次 `automation_update` 已被 Codex 接受，但后续 `note-arm` 没能 durable 成功：
  - bridge 必须先做 durable reconciliation
  - 如果能够证明同一 attempt 已进入 `cooldown` 且 `armed_generation` 与当前 generation 一致，则按“arm 成功但响应丢失”处理
  - 如果能够证明当前 generation 对应的 caller heartbeat 已经重新 `PAUSED`，则当前 attempt 进入 `abandoned`，head batch 保持 `replay_policy=automatic`
  - 只有在既无法证明 arm 成功、也无法证明 heartbeat 已重新 pause 时，当前 head batch 才切到 `replay_policy=manual_resolution_only`，binding 才进入 `degraded`
- 一旦 caller 已成功写入 `cbth desktop note-boundary-crossed ...`，当前 batch 就必须保持 `manual_resolution_only`；第一版不再提供 post-boundary 自动成功收口。
- 为了避免 FIFO 队列永久卡死，第一版必须给 operator 至少两条显式恢复路径：
  - `cbth desktop binding repair --source-thread-id ... --caller-automation-id ... --read-transport ... --json`
  - `cbth batch close-head --source-thread-id ... --reason operator_closed_unconfirmed --json`
  - `cbth batch close-head --source-thread-id ... --reason operator_confirmed_delivery --json`

### 10. `long-run task runners`

共享核心不关心任务到底是：

- 等 CI
- 等 reviewer
- 等某个外部命令
- 等某个本地/远端系统

第一版先把这些都视为“外部脚本通过 CLI 汇报状态”的来源，不在核心里先做复杂插件框架。

## 生命周期模型

- daemon 是按需启动的本地后台进程。
- daemon 生命周期独立于单个 CLI / Desktop 前台实例。
- daemon 不是系统级常驻服务。

## 推荐行为

1. 任意入口调用 `cbth ...` 时，先检查 daemon 是否存在。
2. 如果不存在，则自动拉起 daemon。
3. daemon 记录当前活跃接入端与当前 active jobs。
4. 只要还有 active jobs，daemon 就继续运行。
5. 当同时满足以下条件时，daemon 才允许退出：
   - 没有 active jobs
   - 没有活跃 integration clients
   - 没有未收口的 delivery batches / attempts
   - 没有等待中的 `arm_pending_deadline` / `pause_deadline` / `cooldown_until` / `redelivery_window_ends_at` / `delivery_turn_id` 完成观察
6. 再加一层 idle timeout，避免短时间内频繁启停。

## 第一版建议

- idle timeout 先做成配置项，但保守默认值可以设在 `5-15` 分钟区间。

## 共享投递模型

### Thread-scoped FIFO

- 每个 `source_thread_id` 都有自己的 FIFO 队列。
- ready job 不直接投递给 Codex，而是先进入该 thread 的队列。
- 队列顺序以 `ready_at` / `created_at` 为主。

### Delivery batch

- 真正的投递单位不是单 job，而是 `delivery batch`。
- daemon 会把同一 thread 上相邻的 ready jobs 合并成 batch。
- batch 合并要受限于：
  - `max_jobs_per_batch`
  - `max_total_bytes`
  - `max_wait_window`

### 单 thread 单 in-flight delivery

- 同一 `source_thread_id` 同时最多只能有一个 in-flight delivery attempt。
- 这条约束同时适用于：
  - Desktop caller heartbeat
  - CLI `turn/start`
  - CLI `turn/steer`

### Delivery attempt contract

每个 `source_thread_id` 都必须有一条 durable 的当前 attempt 记录。

#### Attempt 关键字段

- `attempt_id`
- `source_thread_id`
- `batch_id`
- `generation`
- `state`
- `bridge_arm_lease_id` (Desktop optional)
- `bridge_arm_lease_deadline` (Desktop optional)
- `arm_pending_since` (Desktop optional)
- `arm_pending_deadline` (Desktop optional)
- `delivery_turn_id` (optional)
- `managed_session_id` (CLI optional)
- `session_epoch` (CLI optional)
- `delivery_observation_state` (CLI optional)
  - `tracking`
  - `lost`
- `binding_id` (optional)
- `automation_id` (optional)
- `automation_binding_state`
  - `unknown`
  - `observed`
  - `reconciled`
- `snapshot_path`
- `snapshot_revision`
- `delivery_deadline`
- `cooldown_until`
- `created_at`
- `updated_at`

#### Attempt 状态

- `prepared`
- `arm_pending`
- `cooldown`
- `closed`
- `superseded`
- `abandoned`

#### Attempt 规则

- 新 attempt 创建时必须原子地：
  - 绑定一个 `batch_id`
  - 递增该 thread 的 `generation`
  - 物化新 snapshot
  - 把当前 head attempt 指向新的 `attempt_id`
- 这里的 `generation` 是 batch 内 attempt 级序号：
  - 同一 `batch_id` 上创建新的 redelivery attempt 时可以递增
  - 这只会 supersede 旧 attempt，不会自动把当前 batch 关闭为 `close_reason=superseded`
  - `close_reason=superseded` 只保留给整个 batch 被新的 `batch_id` / compaction result / operator decision 取代的 batch 级终态
- 对 Desktop target 来说，新 attempt 只有在存在 `binding_state=bound` 的 desktop binding 时才允许进入可投递状态。
- 同一 `source_thread_id` 任何时刻最多只能有一个非终态 attempt：
  - `prepared`
  - `arm_pending`
  - `cooldown`
- bridge 为 caller arm heartbeat 时，必须把以下值同时写入 caller prompt：
  - `batch_id`
  - `attempt_id`
  - `generation`
  - `snapshot_revision`
  - `snapshot_path`
- bridge 获取这组 token 的来源合同必须是二选一：
  - `direct_file_read` 路径：`ready-threads.json` 的每个 ready entry 必须直接携带这整组 prompt token
  - `helper_cli_read` 路径：`cbth desktop claim-next-ready ...` 必须一次性返回这整组 prompt token
- `caller_automation_id` 不要求由 ready entry 直接携带：
  - bridge 必须始终根据 `source_thread_id` 查询 desktop binding 来解析它
  - 如果 binding 缺失、不是 `bound`、或其 `read_transport` 与当前安装已选定 transport 不一致，则 bridge 不得继续 arm
- `cbth desktop claim-next-ready ...` 虽然名字里带 `claim`，但第一版语义必须是：
  - 纯读取 / peek helper
  - 不得创建 reservation
  - 不得移动 head batch
  - 不得递增 `delivery_attempt_count`
  - 不得改变当前 attempt / batch 的 durable 状态
- `claim-next-ready` 保持纯 peek 的同时，daemon 仍必须在 bridge 运行期内部提供一条不对外暴露的短租约：
  - `bridge_arm_lease`
  - 以 `(source_thread_id, attempt_id, generation)` 为 key
  - 只用于串行化 bridge 自己的 arm 流程
  - 它的 acquire/carry-forward 入口就是 `cbth desktop note-arm-pending ...`
  - `note-arm-pending` 必须返回：
    - `bridge_arm_lease_id`
    - `bridge_arm_lease_deadline`
  - bridge 之后调用 `note-arm` 时，必须回传这个 `bridge_arm_lease_id`
  - 不得改变 head batch 的外部可见性
- Desktop 第一版里，bridge 侧真正允许推进 durable 状态的动作有两步：
  - `cbth desktop note-arm-pending ...` 先把当前 head attempt durable 推到 `arm_pending`
  - 之后 `automation_update` 被 Codex 接受
  - 随后的 `cbth desktop note-arm ...` 再把 attempt 推到 `cooldown`
- `note-arm` 在把 attempt 推到 `cooldown` 时，还必须同时写下：
  - `pause_not_before`
  - `pause_deadline`
- 因此，即使 bridge 在 `claim-next-ready` 返回后崩溃，head batch 也必须仍然保持可见、可重读。
- 但只要某个 attempt 已经进入 `arm_pending`，bridge 就不得再对同一 `attempt_id + generation` 重新 arm，直到该 attempt 被明确收口为：
  - `cooldown`
  - `abandoned`
  - `superseded`
- `arm_pending_deadline` 到期时，reconcile 必须强制把当前 attempt 收敛到以下三者之一，禁止无限停留：
  - 能证明这次 arm 已 durable 成功：
    - 当前 attempt 进入 `cooldown`
  - 能证明这次 arm 从未真正生效，且当前 generation 对应 heartbeat 仍保持 `PAUSED` / 未被 caller 获得 wake 机会：
    - 当前 attempt 进入 `abandoned`
    - 当前 head batch 保持 `replay_policy=automatic`
  - 既无法证明 arm 成功，也无法证明这次 wake 从未生效：
    - 当前 attempt 进入 `abandoned`
    - 当前 head batch 进入 `replay_policy=manual_resolution_only`
    - 对应 binding 进入 `degraded`
- Desktop 第一版里，运行期对 bound caller heartbeat 的 automation mutation 必须只允许 bridge / operator 发起：
  - caller prompt 自己不得直接 `pause` / `update` / `delete` 这个长期复用的 automation
  - stale wake、不可读、caller 成功或 degraded 之后的 pause/reconcile 都必须由 bridge 在后续 heartbeat 中完成
- `note-boundary-crossed` 的 success 返回也必须回显：
  - `batch_id`
  - `attempt_id`
  - `generation`
  - `snapshot_revision`
- caller 必须先比较 helper 返回值与 prompt 中的期望值是否完全一致；任一不一致都视为 stale wake，立即退出。
- 即使 token 全部匹配，caller 也只有在以下条件同时满足时才允许继续：
  - 当前 head batch 仍是 `replay_policy=automatic`
  - `continuation_boundary_state=not_crossed`
  - 对应 binding 仍是 `bound`
- 否则当前 wake 也必须只做 no-op / 诊断退出，不能继续消费这个 batch。
- 第一版 Desktop 路线的 head-batch 安全性不建立在 `automation_id` 必定可同步回填这一前提上。
- 第一版真正的安全锚点是：
  - `batch_id`
  - `attempt_id`
  - `generation`
  - `snapshot_revision`
- 对于 Desktop target，bridge 运行期必须直接使用 binding 中已知的 `caller_automation_id`；运行期不允许 blind create 新 caller heartbeat automation。
- `automation_id` 在第一版里只是可选的协调/观测字段：
  - bridge 如果能直接观察到 `automation_update` 返回值，就写入 attempt
  - 如果关键路径上拿不到，就允许保持 `null + automation_binding_state=unknown`
  - 后续如果能通过 automation metadata、operator helper 或诊断流程补齐，再把状态提升为 `observed` 或 `reconciled`
- 因此，Desktop 第一版的重 arm / supersede / stale-wake 安全性不得依赖 `automation_id` 是否已知。
- 任何旧 generation 的 heartbeat，即使被延迟触发，也只能看到 mismatch 并 no-op，不得再次消费 head batch。
- 旧 generation 的 heartbeat 即使醒来，也不得直接去 pause 这个共享 caller heartbeat；否则会把新 generation 的合法 wake 一起关掉。

#### Attempt 迁移

```text
prepared -> arm_pending -> cooldown -> closed
prepared -> abandoned
prepared -> superseded
arm_pending -> abandoned
arm_pending -> superseded
cooldown -> abandoned
cooldown -> superseded
```

说明：

- `closed` 表示 `cbth` 不会再自动重投该 attempt 绑定的 batch。
- `abandoned` 表示本次投递尝试失败，需要调度器决定是否生成新 attempt。
- `superseded` 表示同一 batch 上出现了更新 generation 的 attempt，旧 attempt 必须彻底失效。
- 这不等于 batch 自己进入 `close_reason=superseded`；batch 级 supersede 必须来自新的 `batch_id` / compaction result / operator decision。
- 第一版 durable 状态里不再保留单独的 `armed`。
- 一次 wakeup arm 一旦被 delivery channel 接受并被 `note-arm` durable 记录，attempt 就直接进入 `cooldown`。
- `cooldown` 表示 `cbth` 正在等待这次 wakeup 的最短观察窗口结束；窗口结束后，如果 batch 仍是 head 且仍允许自动重投，就会生成新 attempt，而不是直接把旧 attempt 视为成功关闭。
- `arm_pending` 表示 bridge 已经 durable 记录“准备为该 attempt arm caller heartbeat”，但这次 arm 还没有被 `note-arm` 最终确认。
- 只要 attempt 仍处于 `arm_pending`：
  - 它就不再是新的 ready head
  - bridge 必须先做 reconcile，而不是再对同一 `attempt_id + generation` 重新 arm
- 如果某次 wakeup arm 已被 delivery channel 接受，但 `note-arm` 结果无法 durable 确认，则必须先走 reconcile：
  - 能证明 arm 成功 -> 按成功 arm 处理
  - 能证明 caller heartbeat 已重新 `PAUSED` -> 当前 attempt 收敛到 `abandoned`，head batch 仍可保持 `replay_policy=automatic`
  - 只有两者都无法证明时，当前 head batch 才进入 `manual_resolution_only`

### 最小连续发送间隔

- 每个 thread 都有最小连续发送间隔。
- 避免多个 batch 在短时间内连续命中同一 caller thread。
- 也避免 CLI active turn 上连续 steer。

### `turn/steer` 的定位

- `turn/steer` 不是共享核心的默认投递手段。
- 它只是 CLI adapter 的受限优化。
- 默认行为仍然应当是“等 caller idle 后再投递 batch”。
- 第一版默认 shipping 配置中，`turn/steer` 应视为关闭；只有在 capability probe 与 active-turn 分类能力都成熟后，才作为 feature flag 打开。
- 这里的 active-turn 分类不能只看 batch 自己的 delivery policy，还必须同时有一份可机判的当前 turn 风险视图，至少包括：
  - `active_turn_kind`
  - `active_turn_requires_approval`
  - `active_turn_requires_network`
  - `active_turn_requires_write_access`
  - `active_turn_risk_class`
- 只有当当前 active turn 本身也被分类为 `read_only_low_risk` 时，CLI adapter 才允许把只读 batch steer 进去；否则一律回退到 idle-only delivery。

### CLI `delivery_turn_id` 观察连续性

- 一旦某个 CLI attempt 已经被 `turn/start` 或 `turn/steer` 接受，并 durable 记录了 `delivery_turn_id`，后续安全收口就建立在“持续观察同一个 managed session / app-server 实例的 turn 事件流”之上。
- 因此，accepted CLI attempt 还必须 durable 绑定：
  - `managed_session_id`
  - `session_epoch`
  - `delivery_turn_id`
- 其中：
  - `managed_session_id` 是 daemon 为一条逻辑 managed CLI session 分配的稳定 durable id
  - `session_epoch` 是该 managed session 当前“可证明连续的 shared app-server event stream”的单调递增序号
  - `session_epoch` 在 daemon 首次拉起该 shared `app-server` 时初始化为 `1`
  - 只要 daemon 还能证明自己仍附着在同一个未重建的 shared `app-server` 实例上，短暂 websocket 重连不递增
  - 只要 app-server 进程重启、managed session 被重建，或 daemon 恢复后无法证明事件流连续性，就必须递增
- 如果 daemon 只是 websocket 短暂断开、但能够重新附着到同一个 `managed_session_id + session_epoch`，则允许继续等待对应的 `turn/completed`。
- 只要 `managed_session_id` 或 `session_epoch` 的连续性无法再证明，当前 head batch 就不得自动 replay：
  - 当前 attempt 收敛到 `abandoned`
  - 当前 head batch durable 进入 `replay_policy=manual_resolution_only`
  - 之后只允许 operator close，或等待 `redelivery_window_ends_at` 到期自动关闭
- 一旦某个 accepted CLI attempt 对应的 `delivery_turn_id` 在之后出现失败、中断、替换，或其他无法被证明为“同一 turn 正常完成”的终局结果：
  - 当前 attempt 同样必须收敛到 `abandoned`
  - 当前 head batch durable 进入 `replay_policy=manual_resolution_only`
  - 只有 pre-accept 的 benign race / non-steerable reject 才允许自动重试
- 第一版不允许在“accepted turn 的观察连续性已经丢失”后，靠重新投递来猜测原 turn 是否已经产生副作用。

## 共享数据模型

### Job 关键字段

- `job_id`
- `target_kind`
  - `cli`
  - `desktop`
- `source_thread_id`
- `status`
- `task_kind`
- `task_summary`
- `metadata_ref`
- `artifact_id`
- `result_summary`
- `dedupe_key`
- `created_at`
- `ready_at`
- `updated_at`
- `completed_at`

### Job 状态

- `running`
- `ready`
- `failed`
- `cancelled`

说明：

- `consumed` 不再作为第一版关键路径上的强语义。
- 对第一版来说，delivery 与 artifact retention 由 `delivery batch` 和 `artifact` 自己管理。

### Delivery batch 关键字段

- `batch_id`
- `source_thread_id`
- `job_ids`
- `state`
- `artifact_ids`
- `summary`
- `first_ready_at`
- `last_ready_at`
- `materialized_at`
- `last_delivery_attempt_at`
- `next_delivery_not_before`
- `redelivery_window_ends_at`
- `max_delivery_attempts`
- `delivery_attempt_count`
- `head_attempt_id`
- `generation`
- `continuation_boundary_state`
  - `not_crossed`
  - `crossed_unacknowledged`
  - `acknowledged` (reserved for a future post-output ack contract; not used in v1)
- `continuation_boundary_crossed_at`
- `boundary_attempt_id`
- `boundary_generation`
- `replay_policy`
  - `automatic`
  - `manual_resolution_only`
- `closed_at`
- `close_reason`
- `delivery_mode`
  - `desktop_heartbeat`
  - `cli_turn_start`
  - `cli_turn_steer`
- `delivery_read_only`
- `delivery_requires_approval`
- `delivery_requires_network`
- `delivery_requires_write_access`
- `inline_payload_bytes`
- `artifact_count`
- `requires_artifact_read`
- `steer_candidate`

### Delivery batch 状态

- `queued`
- `materialized`
- `cooldown`
- `closed`

### Canonical `close_reason`

- `delivered`
  - 可信 delivery channel 已被观察到完成并允许自动关闭
  - 例如 CLI 在同一 `managed_session_id + session_epoch` 上观察到可信的 `turn/completed`
- `superseded`
  - 当前 batch 被新的 `batch_id` / compaction result / operator decision 取代
  - 同一 batch 内生成新 redelivery attempt 不属于 batch 级 `superseded`
- `operator_confirmed_delivery`
  - operator 基于 durable 证据与外部可见证据确认该 batch 已经送达/生效后人工关闭
- `operator_closed_unconfirmed`
  - operator 明确决定停止继续跟踪该 batch，但不宣称它已被确认送达
- `cancelled`
  - 上游任务或用户显式取消
- `redelivery_window_exhausted`
  - batch 仍处于 `replay_policy=automatic`，但重投窗口已到期
- `manual_resolution_expired`
  - batch 已处于 `replay_policy=manual_resolution_only`，且在人工处理窗口到期后被系统关闭
- `max_attempts_exhausted`
  - 已达到 `max_delivery_attempts`

### Attempt 计数语义

- `delivery_attempt_count` 统计的是“被投递通道接受的尝试次数”，不是“生成过多少 prepared attempt”。
- 第一版统一规则：
  - Desktop：
    - 只有 `cbth desktop note-arm ...` 成功并把 attempt durable 推进到 `cooldown` 后，才递增 `delivery_attempt_count`
  - CLI idle path：
    - 只有 `turn/start` 被 server 接受后，才递增 `delivery_attempt_count`
  - CLI steer path：
    - 只有 `turn/steer` 被 server 接受后，才递增 `delivery_attempt_count`
- 因此：
  - `prepared` attempt 本身不消耗 attempt budget
  - CLI benign race 不得递增 `delivery_attempt_count`
  - 只有真正进入 delivery channel 的尝试才会逼近 `max_delivery_attempts`
- `cbth desktop note-arm ...` 的合同必须再补两条：
  - `cbth desktop note-arm-pending ...` 先提供一个 compare-and-swap durable barrier：
    - 只有当 `(source_thread_id, attempt_id, generation)` 仍指向当前 head attempt
    - 且当前 durable 状态仍是 `prepared`
    - 才允许执行唯一一次 `prepared -> arm_pending`
    - 并记录：
      - `bridge_arm_lease_id`
      - `bridge_arm_lease_deadline`
      - `arm_pending_since`
      - `arm_pending_deadline`
    - 如果同一 attempt 已经是 `arm_pending`，重复调用只能返回 already-pending / idempotent success
    - 如果 attempt 已过期、已 superseded 或已离开 head，则必须 stale/no-op
    - 重复调用如果当前 lease 仍有效，必须返回同一个 `bridge_arm_lease_id`，而不是重新发新 lease
  - compare-and-swap：
    - 只有当 `(source_thread_id, attempt_id, generation)` 仍指向当前 head attempt
    - 且当前 durable 状态仍是 `arm_pending`
    - 才允许执行唯一一次 `arm_pending -> cooldown`
    - 且调用方显式回传的 `bridge_arm_lease_id` 与 durable 记录一致
  - idempotent retry：
    - 如果同一 attempt 之前已经成功进入 `cooldown`
    - 重复 `note-arm` 必须返回 idempotent success / already-armed
    - 但不得再次递增 `delivery_attempt_count`
    - 也不得再次推进任何状态
- 如果 `attempt_id` / `generation` 已失配，或该 attempt 已经 `superseded/abandoned/closed`，`note-arm` 必须返回 stale/no-op，而不是重复记账。
- 如果 `automation_update` 已被接受，但 `note-arm` 返回 unknown / failed，则：
  - bridge 不得立刻把这次 wake 视为歧义失败
  - 它必须先做一次 durable reconciliation：
    - 如果当前 attempt 已经是 `cooldown`
    - 且 binding 的 `armed_generation` 已等于该 generation
    - 则把这次 unknown 当作“已成功 arm，但响应丢失”
    - 如果当前 generation 的 caller heartbeat 已能被证明重新 `PAUSED`
    - 则当前 attempt 收敛到 `abandoned`，而 head batch 继续保留 `replay_policy=automatic`
  - 只有在既无法证明 arm 成功、也无法证明 bound heartbeat 已经被重新 pause 时，才允许把当前 head batch durable 推到 `replay_policy=manual_resolution_only` 并把 binding 推到 `degraded`

### Artifact 关键字段

- `artifact_id`
- `source_job_id`
- `managed_path`
- `manifest_path`
- `content_type`
- `size_bytes`
- `created_at`
- `min_retention_until`
- `last_batch_closed_at`
- `operator_pin_until`
- `gc_eligible_at`
- `retention_until`

## Artifact retention / GC contract

第一版必须把 artifact 生命周期绑定到 batch 生命周期，而不是外部临时文件生命周期。

### 默认约束

- `min_artifact_ttl = 24h`
- `post_close_ttl = 72h`

### 规则

- `cbth job complete --result-file <path>` 成功后，artifact 必须先被 ingest 到 managed store。
- 只要仍有非终态 batch 引用该 artifact，就绝不能 GC。
- 当最后一个引用该 artifact 的 batch 进入终态时，记录 `last_batch_closed_at`。
- `gc_eligible_at` 计算为以下三者的最大值：
  - `created_at + min_artifact_ttl`
  - `last_batch_closed_at + post_close_ttl`
  - `operator_pin_until`
- 只有在：
  - 没有非终态 batch 再引用该 artifact
  - 且 `now >= gc_eligible_at`
  时，artifact 才允许进入 GC。

### Batch 终态语义

第一版把以下情况都视为 batch 终态：

- 可信 delivery channel 报告该 batch 已送达，对应 `close_reason=delivered`
- operator / user 显式关闭或取消该 batch，对应 `operator_confirmed_delivery`、`operator_closed_unconfirmed` 或 `cancelled`
- redelivery window 结束且不再继续自动重投，对应 `redelivery_window_exhausted` 或 `manual_resolution_expired`
- batch 被显式 supersede 或达到尝试上限，对应 `superseded` 或 `max_attempts_exhausted`

这意味着：

- `closed` 不是“用户一定已经消费”的证明
- 它只是“`cbth` 不再自动重投该 batch”的 durable 决策点
- `redelivery_window_ends_at` 与 `max_delivery_attempts` 必须 durable 落在 batch 上，而不是只存在于单次 attempt 的临时计算里。
- 这条 batch deadline / redelivery window 合同同样适用于：
  - CLI 中暂时没有 attached managed session、或仍等待为同一 caller thread 建立新 managed session 的 backlog
  - Desktop 中进入 `degraded` 但尚未被 operator 明确关闭的 head batch
- 换句话说：
- “保留在原 caller thread 上等待人工处理或后续重新附着”不等于无限期阻塞
- 如果 batch 在自己的 `redelivery_window_ends_at` 之前仍未恢复到可安全投递/关闭状态，就必须自动进入终态，并释放后续 FIFO/GC 压力
- `replay_policy` 是 durable 的 batch 级合同：
  - `automatic`：
    - 允许按正常合同继续自动 redelivery
  - `manual_resolution_only`：
    - 不允许自动 replay
    - 只允许 operator 显式 close
    - 或在 `redelivery_window_ends_at` 到期时自动 close，以释放 FIFO/GC

### Desktop 第一版送达语义

- Desktop 第一版的自动续跑保证应表述为：
  - `at-least-once wakeup scheduling while the batch remains head and redelivery is still allowed`
- 对 Desktop 来说，一次 attempt 的“成功”只表示：
  - bridge 已为 caller thread 成功 arm 了一次 heartbeat wakeup
- 这还不等于：
  - caller 一定读取了 snapshot
  - caller 一定消费了 batch
  - caller 一定完成了后续工作
- 因此，Desktop batch 不应在第一次 arm 成功后直接 `closed`。
- 推荐行为是：
  - arm 成功 -> attempt 进入 `cooldown`
  - `cooldown_until` 到期后，如果该 batch 仍是 head、`replay_policy=automatic`、`now < redelivery_window_ends_at`、且 `delivery_attempt_count < max_delivery_attempts` -> 创建新 attempt 并再次 arm
  - 如果 `delivery_attempt_count >= max_delivery_attempts`，该 batch 必须自动进入：
    - `close_reason=max_attempts_exhausted`
    - `closed`
- 只有在 operator 关闭、batch 被 superseded、可信 delivery channel 明确成功、redelivery window 结束、或 `max_attempts_exhausted` 时，batch 才进入 `closed`
- caller 的“明确 crossing 已发生”在第一版里应实现为一个窄 helper：

```text
cbth desktop note-boundary-crossed --source-thread-id <thread_id> --attempt-id <attempt_id> --generation <generation> --json
```

- `note-boundary-crossed` 是 Desktop 第一版必需的 gated continuation helper：
  - caller 在真正看到 batch payload / artifact 内容之前，必须先调用它
  - 它的成功返回同时代表：
    - boundary crossing 已 durable 记录
    - 当前 batch 已切到 `crossed_unacknowledged + replay_policy=manual_resolution_only`
    - caller 已获得继续消费该 batch 所需的 payload / artifact access
  - 这个 helper 必须发生在任何后续 assistant 输出、工具调用或其他副作用之前
  - 只有它成功后，caller 才允许继续执行后续 continuation
  - 它也必须具备 compare-and-swap / stale-no-op 语义：
    - 只有当当前 head batch 仍匹配 `(source_thread_id, attempt_id, generation)`
    - 且 `continuation_boundary_state=not_crossed`
    - 且 batch 仍然 open
    - 才允许唯一一次把状态推进到 `crossed_unacknowledged`
    - 如果同一 attempt/generation 已经进入 `crossed_unacknowledged` 或 `acknowledged`
    - 重复调用必须返回 already-crossed / stale-no-op，而不是再次授权 caller 继续
  - 它一旦成功，当前 head batch 必须 durable 进入：
    - `continuation_boundary_state=crossed_unacknowledged`
    - `replay_policy=manual_resolution_only`
  - 成功返回时至少要携带其一：
    - inline payload / summary
    - artifact manifest
    - 后续 `read-artifact` 所需的 chunked access token / parameters
  - 这样即使 caller 在之后崩溃，`cbth` 也不会再自动 redelivery 这个可能已产生副作用的 batch
- 第一版不再尝试在 continuation boundary 之后自动把 batch 收口到 “已送达”：
  - 无论后续是纯文本回复，还是工具 / 行动步骤
  - 只要已经成功执行 `note-boundary-crossed`
  - 当前 head batch 就保持 `crossed_unacknowledged + replay_policy=manual_resolution_only`
  - 后续只能等待 operator close，或在 `redelivery_window_ends_at` 到期时自动关闭
- 在 v1 自动 caller path 里，caller 不得在 `note-boundary-crossed` 之前直接读取 per-thread envelope / artifact payload。
- 如果 `note-boundary-crossed` 返回 error / stale-no-op / already-crossed：
  - caller 必须立即停止，不得继续输出或产生工具副作用
- 如果 caller 已经越过 continuation boundary 但还没成功得到 `note-boundary-crossed` 的 success 返回：
  - 这属于违背第一版安全合同的实现错误
  - 正确实现必须保证“先 `note-boundary-crossed` 成功返回，再继续”
- 一旦 `note-boundary-crossed` 成功，当前 head batch 的 v1 默认值就是：
  - 保持 open
  - `replay_policy=manual_resolution_only`
  - 等待 operator 显式 `batch close-head ...`
  - 或在 `redelivery_window_ends_at` 到期时自动关闭为 `close_reason=manual_resolution_expired`
  - 未来版本如果要支持自动 close，必须单独引入 post-output / post-side-effect observation contract

## 第一版稳定外部接口

第一版只保证 CLI 命令是稳定外部接口。

### 守则

- 外部系统不要直接改 SQLite。
- 外部系统不要直接连接内部 socket。
- 外部系统不要直接读写 daemon 内部队列表。
- 外部脚本只调用 `cbth ...` CLI 子命令。

### 推荐命令面

```text
cbth daemon run
cbth cli run
cbth cli bind
cbth desktop ...
cbth job submit
cbth job complete
cbth job fail
cbth job cancel
cbth job query
cbth batch close-head
cbth batch inspect-head
cbth desktop binding repair
cbth desktop binding unbind
```

说明：

- `cbth cli run` 是 CLI 集成入口。
- `cbth cli bind` 是 CLI fixed-thread bootstrap 的稳定入口：
  - 只允许把 `awaiting_thread` session 推进到 `bound`
  - 如果 session 已经 `bound`，必须返回 `already_bound`
- `cbth desktop ...` 预留给 Desktop bootstrap / helper。
- `cbth job ...` 是第一版对外稳定的任务提交与状态回报面。
- `cbth batch close-head` / `inspect-head` 与 `cbth desktop binding repair` / `unbind` 也必须作为第一版稳定的 operator recovery 面存在。
- `cbth desktop binding repair ...` 的成功输出必须至少回显：
  - `read_transport_capability`
  - `artifact_read_capability`
  - `writeback_capability`
- 其他更细的 queue / batch / inbox 控制面先视为内部实现，不在第一版对外冻结。
- Desktop 使用的 snapshot / artifact 路径目前只算候选内部 contract，不算第一版对外稳定接口。

## 第一版脚本协议

第一版不先做动态插件协议，直接把“外部接入”压缩成 CLI 脚本调用。

### 提交任务

提交任务的脚本只需要调用：

```text
cbth job submit --target <cli|desktop> --thread-id <thread_id> --task-kind <kind> --summary <text> --json
```

推荐补充参数：

- `--metadata-file <path>`
- `--dedupe-key <string>`
- `--delivery-read-only <true|false>`
- `--delivery-requires-approval <true|false>`
- `--delivery-requires-network <true|false>`
- `--delivery-requires-write-access <true|false>`

`--metadata-file` 的第一版最小 schema 必须允许承载一个 `delivery_policy` 对象，例如：

```json
{
  "delivery_policy": {
    "read_only": true,
    "requires_approval": false,
    "requires_network": false,
    "requires_write_access": false
  }
}
```

归一化规则：

- submitter 可以通过显式 CLI flags 或 `metadata-file.delivery_policy` 提供 delivery policy。
- 两者同时出现时，以显式 CLI flags 为准。
- 如果 submitter 没提供这些字段，core 必须 fail-closed 地写入保守默认值：
  - `delivery_read_only=false`
  - `delivery_requires_approval=true`
  - `delivery_requires_network=true`
  - `delivery_requires_write_access=true`
- `inline_payload_bytes` 不是 submitter 直接声明的输入；它必须由 `cbth` 在 materialization / artifact ingest 阶段根据实际 inline payload 大小计算。
- `requires_artifact_read` 也不是 submitter 直接声明的输入；它必须由 `cbth` 在 materialization / artifact ingest 阶段统一派生：
  - 当 continuation 只需要 inline payload / summary 时为 `false`
  - 只有当 continuation 需要在 `note-boundary-crossed` 之后继续调用 `cbth desktop read-artifact ...` 时才为 `true`
- `steer_candidate` 也不是 submitter 直接声明的输入；它必须由 `cbth` / CLI adapter 根据归一化后的 delivery policy、target kind 与 inline payload 大小计算。

返回 JSON 至少包含：

- `job_id`
- `status`
- `accepted_at`

### 回报完成

任务完成时，外部脚本调用：

```text
cbth job complete --job-id <job_id> --summary <text> --result-file <path> --json
```

语义：

- `cbth` 会 ingest/copy 该文件到自己管理的 artifact store。
- 成功后返回或记录内部 `artifact_id`。
- 之后原始 `result-file` 可以被外部脚本清理，不影响 `cbth` 后续投递。

### 回报失败

失败时，外部脚本调用：

```text
cbth job fail --job-id <job_id> --reason <text> --json
```

### 查询

给外部脚本或人工排障使用：

```text
cbth job query <job_id> --json
```

### Operator recovery

给人工排障和恢复 Desktop degraded thread 使用：

```text
cbth desktop binding repair --source-thread-id <thread_id> --caller-automation-id <automation_id> --read-transport <transport> --json
cbth batch close-head --source-thread-id <thread_id> --reason operator_closed_unconfirmed --json
cbth batch close-head --source-thread-id <thread_id> --reason operator_confirmed_delivery --json
cbth batch inspect-head --source-thread-id <thread_id> --json
cbth desktop binding unbind --source-thread-id <thread_id> --delete-automation <true|false> --json
```

## Desktop 只读快照约束

- 第一版不要求 Desktop heartbeat turn 在关键路径上执行通用 `cbth job ...` CLI。
- 但 Desktop adapter 可以依赖三类窄接口：
  - bridge 侧 `helper_cli_read`：只读 ready/reconcile fallback helper
  - narrow helper writeback：
    - `cbth desktop note-arm-pending ...`
    - `cbth desktop note-arm ...`
    - `cbth desktop note-boundary-crossed ...`
  - caller 侧 post-boundary artifact helper：
    - `cbth desktop read-artifact ...`
- 第一版如果 bridge 侧不用 `direct_file_read`，则 bridge-side fallback helper 链路必须是完整可用的：
  - `cbth desktop note-arm-pending ...`
  - `cbth desktop list-arm-pending ...`
  - `cbth desktop list-pause-due ...`
  - `cbth desktop claim-next-ready ...`
  - `cbth desktop note-arm ...`
  - `cbth desktop note-boundary-crossed ...`
- caller 侧 automatic continuation 不通过 `direct_file_read` 直接拿 payload：
  - 必须先通过 `cbth desktop note-boundary-crossed ...` 成功返回 gated payload / artifact access
  - 只有在这个 helper success 之后，才允许继续调用 `cbth desktop read-artifact ...`
- 但 bridge-side helper fallback 目前只能算条件性方案：
  - 它仍然要求 heartbeat turn 能无审批执行窄 `cbth desktop ...` 命令
  - 在这个前提被实证前，不应把它表述成已验证的默认主路径
- 而 `cbth desktop read-artifact ...` 不是 bridge-side fallback：
  - 它是大 artifact continuation 的 caller-side 必需接口
  - 因此只要 v1 支持大 artifact，就必须把它纳入 Desktop 自动路径的能力验证范围
- 其中 `cbth desktop read-artifact ...` 必须提供 chunked payload 协议，而不是返回一个需要再次 file-read 的路径。
- 其中 bridge 的 overdue-binding cleanup 也必须有对应只读输入面：
  - `~/.cbth/inbox/pause-due-bindings.json`
  - 或 `cbth desktop list-pause-due --bridge-thread-id <thread_id> --json`
- 其中 `cbth desktop claim-next-ready ...` 必须一次性返回 bridge 写 prompt 所需的整组 token：
  - `source_thread_id`
  - `batch_id`
  - `attempt_id`
  - `generation`
  - `snapshot_revision`
  - `snapshot_path`
- bridge 必须再根据 `source_thread_id` 查询 binding，解析当前唯一允许更新的 `caller_automation_id`
- 当前首选路径是：
  - bridge heartbeat 只读 `ready-threads.json`
  - caller heartbeat 先调用 `note-boundary-crossed`，并只在 success 返回后消费 payload / artifact
- 如果 `direct_file_read` 不能满足无审批读取约束，则 Desktop 设计要么切回经过单独验证的 `helper_cli_read`，要么继续保留为候选方案；不能直接把未验证的 helper 执行前提当成已经成立。
- Desktop 自动续跑只对已完成 binding 的 thread 生效；未绑定 thread 不得被 bridge 自动 arm。

## 为什么第一版只做 CLI 脚本

- 最简单稳定。
- 对 shell、Python、GitHub Actions、本地守护脚本都足够友好。
- 不会过早冻结 socket / Web / plugin 协议。
- 便于保持核心系统独立，不把任务适配方式绑死在单一语言或运行时里。

## 与端侧文档的关系

- CLI 侧如何接 Codex TUI，见：
  - `docs/CLI_SHARED_APP_SERVER_SIDECAR_DESIGN.md`
- Desktop 侧如何接 heartbeat，见：
  - `docs/DESKTOP_BACKGROUND_TASK_BRIDGE_DESIGN.md`

这两份文档描述的是“如何唤醒 caller thread”。
本文件描述的是两端共用的：

- daemon 生命周期
- store / artifact store
- thread inbox / delivery batch
- CLI 公共接口
- 外部长任务接入边界

## 第一版不做的事

- 不做系统级服务安装。
- 不做公开 Web API。
- 不做公开 socket API。
- 不做动态插件加载框架。
- 不把 `turn/steer` 当成必需能力。
- 第一版默认不打开 `turn/steer`。
- 不把 Desktop heartbeat 对通用 `cbth job ...` CLI 的执行能力当成关键前提。
