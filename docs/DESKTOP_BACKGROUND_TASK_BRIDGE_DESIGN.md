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
- 但 Desktop adapter 明确依赖一组窄 helper：
  - `cbth desktop read-envelope ...`
  - `cbth desktop read-artifact ...`
  - `cbth desktop note-arm ...`
  - `cbth desktop note-delivered ...`
- 因此，Desktop 上最可靠的方案不是“外部 sidecar 直接推送 caller thread”，而是“由 app 内部 automation scheduler 去唤醒 caller thread”。

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
     - `~/.cbth/inbox/by-thread/<thread_id>.json`
     - `~/.cbth/artifacts/<artifact_id>/manifest.json`
   - `helper_cli_read` 建议提供一组窄 helper：

```text
cbth desktop read-envelope --source-thread-id <thread_id> --expected-attempt-id <attempt_id> --expected-generation <generation> --expected-snapshot-revision <revision> --json
cbth desktop read-artifact --artifact-id <artifact_id> --offset <offset> --max-bytes <n> --json
```

   - 底层仍可用 SQLite / 普通文件 / `mmap`，但这属于内部实现细节。
   - 明确不依赖直接改 Codex 自己的 automation DB。
   - `direct_file_read` 目前仍是候选主路径，待“heartbeat 无审批读取”实证后再升级为已验证主路径。
   - 如果 `direct_file_read` 在目标安装上不可行，Desktop 第一版必须退回 `helper_cli_read`，而不是继续把文件读取当作既定前提。

3. `desktop thread binding`
   - 每个要支持自动续跑的 caller thread，都必须先完成一次 binding。
   - binding 至少要 durable 记录：
     - `source_thread_id`
     - `caller_automation_id`
     - `read_transport`
   - bridge 在运行期只允许更新这个已知 `caller_automation_id`，不做 blind create / discovery。
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
   - 被唤醒后，在原 caller thread 中读取自己的 delivery envelope，并继续原任务。
   - 第一版不要求它在关键路径上执行通用 `cbth job ...` CLI。
   - 但它必须在成功读取当前 envelope 后调用 `cbth desktop note-delivered ...`，把当前 head batch durable 关闭。
   - 这个已绑定 automation 的生命周期合同是：
     - 正常路径下只 `pause` / `update` / `reuse`
     - stale wake、snapshot 不可读、成功送达、degraded 都优先回到 `PAUSED`
     - 不在正常投递路径里 `delete`
     - 只有明确的 operator unbind / destroy 才允许删除它

## 设计原则

- 长等待放在 sidecar，不放在 Codex 当前 turn。
- 周期性检查集中在 bridge thread，不污染所有 caller thread。
- caller thread 只在“确实有结果可消费”时被唤醒。
- 不要求 bridge thread 与 caller thread 之间直接 live push；两者都只依赖 automation scheduler 和共享 job state。
- 关键读取路径优先只读，但 Desktop 第一版允许一个窄的 `helper_cli_read` fallback。
- 运行期 bridge 不得 blind create caller heartbeat；它只能更新已绑定 automation。
- 旧 heartbeat prompt 必须能够通过 attempt token / generation 检测自己已经过期，并立即 no-op。

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

1. 读取 bridge 侧的 ready 来源：

```text
preferred: ~/.cbth/inbox/ready-threads.json
fallback:  cbth desktop claim-next-ready --bridge-thread-id <thread_id> --json
```

2. 如果没有可投递 thread，本次 turn 直接结束。
3. 如果有可投递 thread：
   - 无论来自 `ready-threads.json` 还是 `claim-next-ready` helper，都必须直接拿到一个 ready entry：
     - `source_thread_id`
     - `batch_id`
     - `attempt_id`
     - `generation`
     - `snapshot_revision`
     - `snapshot_path`
     - `caller_automation_id`
   - 该 thread 必须已经存在 `binding_state=bound` 的 desktop binding
   - 用 `automation_update` 更新这个已知 caller heartbeat
   - heartbeat prompt 中带上：
     - `batch_id`
     - `attempt_id`
     - `generation`
     - `snapshot_revision`
     - `snapshot_path`
   - `automation_update` 成功后，bridge 调用一个窄 helper：

```text
cbth desktop note-arm --source-thread-id <thread_id> --attempt-id <attempt_id> --generation <generation> --json
```

   - 这一步负责把 attempt durable 推进到 `cooldown`，并更新：
     - `head_attempt_id`
     - `last_delivery_attempt_at`
     - `delivery_attempt_count`
   - 如果 `note-arm` 不可用，则该 desktop binding 必须视为 `degraded`，而不是继续假设状态已经推进成功。

### 3. caller thread 被唤醒

caller thread heartbeat 在下一次调度中醒来：

1. 根据 prompt 读取自己的 delivery envelope：

```text
preferred: ~/.cbth/inbox/by-thread/<thread_id>.json
fallback:  cbth desktop read-envelope --source-thread-id <thread_id> --expected-attempt-id <attempt_id> --expected-generation <generation> --expected-snapshot-revision <revision> --json
```

2. 如果 envelope 不存在、当前 batch 已被撤回，或只包含旧内容，本次 turn 直接结束。
3. 如果 envelope 存在并指向一个可消费 batch：
   - 先比较 envelope header 中的：
     - `batch_id`
     - `attempt_id`
     - `generation`
     - `snapshot_revision`
     与 prompt 中的期望值是否一致
   - 任一不一致都视为 stale wake，立即退出
   - 读取 batch 摘要
   - 对小结果可直接读取 inline payload
   - 对大结果：
     - `direct_file_read` 路径下读取 `cbth` 管理的 artifact 路径
     - `helper_cli_read` 路径下调用：

```text
cbth desktop read-artifact --artifact-id <artifact_id> --offset <offset> --max-bytes <n> --json
```

   - 在原 caller thread 中继续后续工作
4. caller 成功读取当前 envelope 后，必须调用一个窄 helper：

```text
cbth desktop note-delivered --source-thread-id <thread_id> --attempt-id <attempt_id> --generation <generation> --json
```

5. 这一步负责把当前 batch durable 关闭到：
   - `close_reason=caller_acknowledged`
   - 并停止该 head batch 的自动重投

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
  - `cooldown_until`
- bridge arm caller heartbeat 前，必须先原子创建/更新 attempt。
- caller prompt 中必须显式携带 `batch_id + attempt_id + generation + snapshot_revision`。
- caller 读取 envelope 后，必须先比较这四者；只要 mismatch 就立即 no-op。
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

### Caller 侧

```text
~/.cbth/inbox/by-thread/<thread_id>.json
~/.cbth/artifacts/<artifact_id>/manifest.json
~/.cbth/artifacts/<artifact_id>/payload
```

### Helper fallback

```text
cbth desktop claim-next-ready --bridge-thread-id <thread_id> --json
cbth desktop read-envelope --source-thread-id <thread_id> --expected-attempt-id <attempt_id> --expected-generation <generation> --expected-snapshot-revision <revision> --json
cbth desktop read-artifact --artifact-id <artifact_id> --offset <offset> --max-bytes <n> --json
cbth desktop note-arm --source-thread-id <thread_id> --attempt-id <attempt_id> --generation <generation> --json
cbth desktop note-delivered --source-thread-id <thread_id> --attempt-id <attempt_id> --generation <generation> --json
```

`read-artifact` 的第一版返回合同至少包括：

- `artifact_id`
- `content_type`
- `size_bytes`
- `offset`
- `bytes_returned`
- `data_base64`
- `next_offset`
- `eof`

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
- 只读取 ready index，不依赖通用 `cbth job ...` CLI。
- 没有 ready thread 就立即结束。
- 有 ready thread 时，只更新对应 caller thread 的已绑定 heartbeat，不直接展开主任务。
- 运行期不得 blind create 新 caller heartbeat automation。
- arm 完成后如果能直接拿到 `automation_id`，可以把它写进 prompt / automation metadata 作为协调信息；拿不到时也不能阻塞关键路径。
- bridge arm 的 durable 完成条件是：
  - attempt 已存在
  - snapshot 已物化
  - 当前 generation 的 caller heartbeat arm 请求已被 Codex 接受
  - `cbth desktop note-arm ...` 已成功执行

### Caller prompt 要求

- 先读取自己的 per-thread delivery envelope。
- 只处理当前 envelope 指向的 head batch。
- 对小结果可以直接读取 inline payload；对大结果通过当前 transport 读取 artifact 内容。
- 成功读取当前 envelope 后，必须调用 `cbth desktop note-delivered ...`，把当前 batch durable 关闭为 `caller_acknowledged`，从而停止自动 redelivery。
- 任务处理完成后可以清理或暂停当前 heartbeat，但不要求做更细粒度的 per-job `consumed` 记账。
- 旧 generation 的 prompt 只允许 no-op，不允许“顺手处理当前 head batch”。
- 读取传输由 binding 预先决定：
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
  - `superseded`
  - `operator_closed`
  - `cancelled`
  - `redelivery_window_exhausted`
  - `max_attempts_exhausted`
  - `caller_acknowledged`
- 也就是说，Desktop 第一版的自动续跑语义是：
  - `at-least-once wakeup scheduling`
  - not `exactly-once consumption`
- 但一旦 caller 已成功执行 `cbth desktop note-delivered ...`，该 head batch 就必须自动进入：
  - `close_reason=caller_acknowledged`
  - 不再继续 redelivery
- 如果 `delivery_attempt_count >= max_delivery_attempts`，该 head batch 也必须自动进入：
  - `close_reason=max_attempts_exhausted`
  - `closed`

## 失败与重试

### Sidecar 还没完成

- bridge thread 下次 heartbeat 再检查。

### Bridge arm 失败

- batch 保持可投递。
- 当前 attempt 标为 `abandoned` 或保持 `prepared`，由调度器决定是否重建新 attempt。
- 下一次 bridge heartbeat 只能基于新的 head attempt 再试。

### Caller heartbeat 醒来但 snapshot 不可读

- 说明快照暂时不可用、路径变化，或当前 batch 已被撤回。
- 当前 turn 直接退出并清理 heartbeat。

### Caller heartbeat 醒来但 attempt mismatch

- 说明这是旧 generation 的 stale wake，或者该 batch 已被 supersede。
- 当前 turn 必须 no-op 并退出，不得尝试消费当前 head batch。

### Caller 处理中途失败

- 第一版不依赖 caller 回写失败状态。
- `cbth` 通过保留 batch、cooldown 与 redelivery timeout 决定是否再次 arm。
- 如果 `cooldown_until` 到期后，该 batch 仍然是当前 head batch，且 `close_reason` 仍为空、`now < redelivery_window_ends_at`、并且 `delivery_attempt_count < max_delivery_attempts`，就应该创建新 attempt 并再次 arm，而不是把旧 attempt 直接视为成功送达。

### Caller 已读到 envelope，但 `note-delivered` 失败

- 这条路径不能回退成“继续自动 redelivery”，否则会把重复续跑重新变成默认行为。
- 因此，如果 caller 已成功读取 envelope，但 `cbth desktop note-delivered ...` 失败：
  - 当前 binding 必须进入 `degraded`
  - 当前 attempt 必须收敛到 `abandoned`
  - 当前 head batch 保持未关闭
  - bridge 不再继续自动 redelivery，等待 operator 恢复

### Binding degraded

- `binding_state=degraded` 表示该 thread 暂时失去自动续跑能力，但 job / artifact 仍可继续累积。
- degraded 之后：
  - bridge 不得再为该 thread 自动 arm caller heartbeat
  - 当前 in-flight attempt 必须收敛到 `abandoned`
  - 当前 head batch 保持未关闭
  - 调度器只保留结果与元数据，不继续自动 redelivery
- operator 必须通过显式 CLI 路径来解开这个状态，至少支持两类动作：

```text
cbth desktop binding repair --source-thread-id <thread_id> --caller-automation-id <automation_id> --read-transport <transport> --json
cbth batch close-head --source-thread-id <thread_id> --reason operator_closed --json
```

- 推荐语义：
  - `binding repair`：
    - 重新验证 paused status / read transport / narrow helpers
    - 成功后把 binding 从 `degraded` 恢复到 `bound`
    - 并把当前 head batch 重新放回可投递状态
  - `batch close-head`：
    - 显式关闭当前 head batch
    - 让后续 FIFO 队列继续前进

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
  - 正常送达后：caller 或 bridge 把它切回 `PAUSED`
  - stale wake / snapshot 不可读：当前 turn no-op 后切回 `PAUSED`
  - degraded：切回 `PAUSED`，等待 operator repair
  - 只有 operator 明确执行 unbind/destroy，才允许删除
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
4. 由 `cbth` 物化 `ready-threads.json` 与 per-thread inbox snapshot。
5. 让 bridge thread 每分钟读取 `ready-threads.json`。
6. bridge 发现可投递 batch 后，为对应 caller thread arm 一次 heartbeat。
7. caller thread 醒来后读取自己的 inbox snapshot，继续任务。

这套方案的关键优点是：

- 不改 Codex Desktop 内部实现。
- 不依赖外部 live push。
- 不依赖后台 heartbeat 的本地 CLI 执行能力。
- 只把只读快照路径当作候选主路径，直到 heartbeat 无审批读取得到实证。
- 不需要用户手动切回某个 notification thread。
- caller thread 可以在原上下文内继续任务。
