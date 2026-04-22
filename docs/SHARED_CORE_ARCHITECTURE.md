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
  - 当没有 active jobs 且没有活跃接入端时，daemon 在 idle timeout 后自动退出。

### 3. 第一版公共接口只做 CLI

- 第一版不承诺稳定的 socket API。
- 第一版不承诺稳定的 Web API。
- 第一版不承诺动态插件加载协议。
- 第一版唯一稳定的外部接入面是 CLI 命令。
- 任何外部脚本、future plugin、future Web bridge，都先通过 CLI 命令与核心系统交互。

### 4. Desktop 关键路径优先只读

- 第一版不把“Desktop heartbeat turn 能稳定执行通用 `cbth job ...` CLI”当作既定前提。
- 但 Desktop adapter 明确依赖一组窄 helper：
  - `cbth desktop read-envelope ...`
  - `cbth desktop read-artifact ...`
  - `cbth desktop note-arm ...`
  - `cbth desktop note-delivered ...`
- Desktop 的关键投递路径优先依赖只读状态面：
  - 只读 inbox snapshot 文件
  - 只读 artifact manifest / artifact 文件
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
~/.cbth/inbox/by-thread/<thread_id>.json
~/.cbth/artifacts/<artifact_id>/manifest.json
~/.cbth/artifacts/<artifact_id>/payload
```

2. `helper_cli_read`

```text
cbth desktop read-envelope --source-thread-id <thread_id> --expected-attempt-id <attempt_id> --expected-generation <generation> --expected-snapshot-revision <revision> --json
cbth desktop read-artifact --artifact-id <artifact_id> --offset <offset> --max-bytes <n> --json
```

两种传输必须返回同一个 envelope schema。

`direct_file_read` 的第一版候选路径：

```text
~/.cbth/inbox/ready-threads.json
~/.cbth/inbox/by-thread/<thread_id>.json
~/.cbth/artifacts/<artifact_id>/manifest.json
~/.cbth/artifacts/<artifact_id>/payload
```

更新方式：

- 用 `write temp + rename` 原子替换。
- 外部语义固定为“读快照文件”。
- 后续如果内部改成 `mmap` / shared memory，只能在不改变这一语义的前提下做。
- `direct_file_read` 在 Desktop 无审批读取能力得到实证前，仍视为候选内部 contract，而不是已冻结接口。
- 如果 `direct_file_read` 无法满足无审批读取约束，Desktop 第一版必须退回 `helper_cli_read`，而不是继续把未验证前提当主链路。
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
  - delivery envelope 读取（`direct_file_read` 或 `helper_cli_read`）
  - narrow helper writeback (`cbth desktop note-arm ...`)

### 9. `desktop thread bindings`

职责：

- 把某个 Desktop source thread 绑定到一个稳定的 caller heartbeat automation。
- 让 bridge 在运行期只做“更新已知 automation”，而不是 blind create / discover。
- 为每个 source thread 选择允许的 delivery-envelope 读取传输。

关键字段：

- `binding_id`
- `source_thread_id`
- `binding_state`
  - `unbound`
  - `bound`
  - `degraded`
- `caller_automation_id`
- `bridge_thread_id`
- `read_transport`
  - `direct_file_read`
  - `helper_cli_read`
- `created_at`
- `updated_at`
- `last_verified_at`

约束：

- Desktop 自动续跑只对 `binding_state=bound` 的 thread 生效。
- `unbound` thread 可以继续提交 job，但 bridge 不得尝试自动 arm caller heartbeat。
- 运行期 bridge 不负责发现新的 caller automation id；第一版要求这个 id 通过 bootstrap 预先 durable 绑定。
- `degraded` 表示该 thread 暂时失去自动续跑能力：
  - bridge 不再自动 arm
  - 当前 attempt 必须收敛到 `abandoned`
  - 当前 head batch 保持未关闭，等待 operator 恢复或人工处理
- 如果 caller 已成功读取 envelope，但 `cbth desktop note-delivered ...` 失败，也必须走同一条 `degraded` 收敛路径，而不是继续自动 redelivery。
- 为了避免 FIFO 队列永久卡死，第一版必须给 operator 至少两条显式恢复路径：
  - `cbth desktop binding repair --source-thread-id ... --caller-automation-id ... --read-transport ... --json`
  - `cbth batch close-head --source-thread-id ... --reason operator_closed --json`

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
- `armed`
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
- 对 Desktop target 来说，新 attempt 只有在存在 `binding_state=bound` 的 desktop binding 时才允许进入可投递状态。
- 同一 `source_thread_id` 任何时刻最多只能有一个非终态 attempt：
  - `prepared`
  - `armed`
  - `cooldown`
- bridge 为 caller arm heartbeat 时，必须把以下值同时写入 caller prompt：
  - `batch_id`
  - `attempt_id`
  - `generation`
  - `snapshot_revision`
  - `snapshot_path`
- caller 读取 envelope 后，必须先比较：
  - `snapshot.batch_id`
  - `snapshot.attempt_id`
  - `snapshot.generation`
  - `snapshot.snapshot_revision`
  与 prompt 中的期望值是否完全一致；任一不一致都视为 stale wake，立即退出。
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

#### Attempt 迁移

```text
prepared -> armed -> cooldown -> closed
prepared -> abandoned
armed -> abandoned
prepared -> superseded
armed -> superseded
cooldown -> superseded
```

说明：

- `closed` 表示 `cbth` 不会再自动重投该 attempt 绑定的 batch。
- `abandoned` 表示本次投递尝试失败，需要调度器决定是否生成新 attempt。
- `superseded` 表示同一 thread 上出现了更新 generation 的 attempt，旧 attempt 必须彻底失效。
- `armed` 只表示一次 wakeup arm 已被 Codex 侧接受，不代表 caller 已实际消费结果。
- `cooldown` 表示 `cbth` 正在等待这次 wakeup 的最短观察窗口结束；窗口结束后，如果 batch 仍是 head 且仍允许自动重投，就会生成新 attempt，而不是直接把旧 attempt 视为成功关闭。

### 最小连续发送间隔

- 每个 thread 都有最小连续发送间隔。
- 避免多个 batch 在短时间内连续命中同一 caller thread。
- 也避免 CLI active turn 上连续 steer。

### `turn/steer` 的定位

- `turn/steer` 不是共享核心的默认投递手段。
- 它只是 CLI adapter 的受限优化。
- 默认行为仍然应当是“等 caller idle 后再投递 batch”。
- 第一版默认 shipping 配置中，`turn/steer` 应视为关闭；只有在 capability probe 与 active-turn 分类能力都成熟后，才作为 feature flag 打开。

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
- `steer_candidate`

### Delivery batch 状态

- `queued`
- `materialized`
- `armed`
- `cooldown`
- `closed`

### Attempt 计数语义

- `delivery_attempt_count` 统计的是“被投递通道接受的尝试次数”，不是“生成过多少 prepared attempt”。
- 第一版统一规则：
  - Desktop：
    - 只有 `cbth desktop note-arm ...` 成功后，才递增 `delivery_attempt_count`
  - CLI idle path：
    - 只有 `turn/start` 被 server 接受后，才递增 `delivery_attempt_count`
  - CLI steer path：
    - 只有 `turn/steer` 被 server 接受后，才递增 `delivery_attempt_count`
- 因此：
  - `prepared` attempt 本身不消耗 attempt budget
  - CLI benign race 不得递增 `delivery_attempt_count`
  - 只有真正进入 delivery channel 的尝试才会逼近 `max_delivery_attempts`

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

- CLI adapter 报告该 batch 已成功送达
- operator / user 显式关闭或取消该 batch
- redelivery window 结束且不再继续自动重投

这意味着：

- `closed` 不是“用户一定已经消费”的证明
- 它只是“`cbth` 不再自动重投该 batch”的 durable 决策点
- `redelivery_window_ends_at` 与 `max_delivery_attempts` 必须 durable 落在 batch 上，而不是只存在于单次 attempt 的临时计算里。

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
  - `cooldown_until` 到期后，如果该 batch 仍是 head、`now < redelivery_window_ends_at`、且 `delivery_attempt_count < max_delivery_attempts` -> 创建新 attempt 并再次 arm
  - 只有在 operator 关闭、batch 被 supersede、caller 明确回写成功、或 redelivery window 结束时，batch 才进入 `closed`
- caller 的“明确回写成功”在第一版里应实现为一个窄 helper：

```text
cbth desktop note-delivered --source-thread-id <thread_id> --attempt-id <attempt_id> --generation <generation> --json
```

- 一旦该 helper 成功，当前 head batch 必须自动进入：
  - `close_reason=caller_acknowledged`
  - 不再继续 redelivery

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
cbth desktop ...
cbth job submit
cbth job complete
cbth job fail
cbth job cancel
cbth job query
```

说明：

- `cbth cli run` 是 CLI 集成入口。
- `cbth desktop ...` 预留给 Desktop bootstrap / helper。
- `cbth job ...` 是第一版对外稳定的任务提交与状态回报面。
- 更细的 queue / batch / inbox 控制面先视为内部实现，不在第一版对外冻结。
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
cbth batch close-head --source-thread-id <thread_id> --reason operator_closed --json
```

## Desktop 只读快照约束

- 第一版不要求 Desktop heartbeat turn 在关键路径上执行通用 `cbth job ...` CLI。
- 但 Desktop adapter 可以依赖两类窄接口：
  - `helper_cli_read`：只读 envelope/helper
  - `cbth desktop note-arm ...`：bridge 成功 arm 后的窄写回
- 第一版如果不用 `direct_file_read`，则 helper 链路必须是完整可用的：
  - `cbth desktop read-envelope ...`
  - `cbth desktop read-artifact ...`
  - `cbth desktop note-arm ...`
  - `cbth desktop note-delivered ...`
- 其中 `cbth desktop read-artifact ...` 必须提供 chunked payload 协议，而不是返回一个需要再次 file-read 的路径。
- 当前首选路径是：
  - bridge heartbeat 只读 `ready-threads.json`
  - caller heartbeat 通过 `direct_file_read` 读取自己的 per-thread envelope 与 artifact
- 如果 `direct_file_read` 不能满足无审批读取约束，则第一版 Desktop 必须切回 `helper_cli_read`，而不是继续假设文件读取成立。
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
