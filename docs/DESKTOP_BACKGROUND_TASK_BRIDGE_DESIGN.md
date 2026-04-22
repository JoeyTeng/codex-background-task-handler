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
- 第一版不把“heartbeat turn 稳定执行本地 `cbth ...` CLI”当作既定前提。
- 因此，Desktop 上最可靠的方案不是“外部 sidecar 直接推送 caller thread”，而是“由 app 内部 automation scheduler 去唤醒 caller thread”。

## 核心设计

### 组件

1. `sidecar supervisor`
   - 负责真正执行长时间任务，例如等待 CI、等待 reviewer、等待外部系统结果。
   - 不尝试直接修改 Codex Desktop 的 live session。

2. `shared job state`
   - 由 sidecar 暴露给 Codex thread 读取的共享状态面。
   - 第一版关键路径优先暴露成只读快照文件，而不是本地 CLI 调用。
   - 建议至少包含：
     - `~/.cbth/inbox/ready-threads.json`
     - `~/.cbth/inbox/by-thread/<thread_id>.json`
     - `~/.cbth/artifacts/<artifact_id>/manifest.json`
   - 底层仍可用 SQLite / 普通文件 / `mmap`，但这属于内部实现细节。
   - 明确不依赖直接改 Codex 自己的 automation DB。
   - 这条只读文件路径目前仍是候选主路径，待“heartbeat 无审批读取”实证后再升级为已验证主路径。

3. `bridge heartbeat thread`
   - 一个固定存在的专用 thread。
   - 低成本、快模型即可。
   - 当前验证使用的 bridge thread：
     - `thread_id = 019db5e6-ba6a-7b80-95d2-a6867163281a`
     - `model = gpt-5.3-codex-spark-preview`
     - `reasoning_effort = low`
   - 职责只有一件事：轮询共享 job state，并在发现某个 caller thread 对应 head batch 可投递时，用 `automation_update` 为该 caller thread 武装 heartbeat。

4. `caller thread heartbeat`
   - 不是常驻轮询器。
   - 只在 bridge 判定当前 thread 有可投递 batch 时，才被创建、激活、重定向或更新。
   - 被唤醒后，在原 caller thread 中读取自己的 inbox snapshot，并继续原任务。
   - 第一版不要求它在关键路径上写回 ack；artifact retention 与清理由 `cbth` 自己负责。

## 设计原则

- 长等待放在 sidecar，不放在 Codex 当前 turn。
- 周期性检查集中在 bridge thread，不污染所有 caller thread。
- caller thread 只在“确实有结果可消费”时被唤醒。
- 不要求 bridge thread 与 caller thread 之间直接 live push；两者都只依赖 automation scheduler 和共享 job state。
- 关键读取路径优先只读，不把后台 heartbeat 的本地 CLI 执行能力当成前提。
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
5. daemon 把 ready jobs 聚合成该 thread 的 `delivery batch`，并物化为只读 inbox snapshot。

### 2. bridge thread 轮询

bridge heartbeat 每分钟醒一次：

1. 读取只读 ready index，例如：

```text
~/.cbth/inbox/ready-threads.json
```

2. 如果没有可投递 thread，本次 turn 直接结束。
3. 如果有可投递 thread：
   - 选择最早到期且未处于 cooldown 的 `source_thread_id`
   - `cbth` 先为当前 head batch 原子创建新的 attempt，并递增 `generation`
   - 用 `automation_update` 为目标 caller thread 创建或更新 heartbeat
   - heartbeat prompt 中带上：
     - `batch_id`
     - `attempt_id`
     - `generation`
     - `snapshot_path`
   - `cbth` durable 记录：
     - `head_attempt_id`
     - `automation_id` (if observable)
     - `last_delivery_attempt_at`

### 3. caller thread 被唤醒

caller thread heartbeat 在下一次调度中醒来：

1. 根据 prompt 读取自己的只读 inbox snapshot，例如：

```text
~/.cbth/inbox/by-thread/<thread_id>.json
```

2. 如果 snapshot 不存在、当前 batch 已被撤回，或只包含旧内容，本次 turn 直接结束。
3. 如果 snapshot 存在并指向一个可消费 batch：
   - 先比较 snapshot header 中的：
     - `batch_id`
     - `attempt_id`
     - `generation`
     与 prompt 中的期望值是否一致
   - 任一不一致都视为 stale wake，立即退出
   - 读取 batch 摘要
   - 对小结果可直接读取 inline payload
   - 对大结果读取 `cbth` 管理的 artifact 路径
   - 在原 caller thread 中继续后续工作
4. 第一版不要求 caller 在关键路径上写回 `consumed`；批次的 retention、redelivery 与 GC 由 `cbth` 自己管理。

## 最小状态机

### Job 状态

- `running`
- `ready`
- `failed`
- `cancelled`

### Delivery batch 状态

- `queued`
- `materialized`
- `armed`
- `cooldown`
- `closed`

### Delivery attempt 状态

- `prepared`
- `armed`
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
  - `automation_id` (optional)
  - `snapshot_path`
  - `delivery_deadline`
  - `cooldown_until`
- bridge arm caller heartbeat 前，必须先原子创建/更新 attempt。
- caller prompt 中必须显式携带 `batch_id + attempt_id + generation`。
- caller 读取 snapshot 后，必须先比较这三者；只要 mismatch 就立即 no-op。
- 同一 thread 上出现新的 generation 后，所有旧 heartbeat prompt 都只能看到 mismatch，不得重复消费当前 head batch。
- 第一版不要求 `cbth` 在关键路径上同步拿到 `automation_id`。
- 对第一版来说：
  - `attempt_id + generation + snapshot header` 才是防止 stale wake 的硬约束
  - `automation_id` 只是 bridge 侧可选的协调/诊断信息
  - 如果 bridge 在 turn 内能直接拿到 `automation_update` 返回的 id，可以 best-effort 记录
  - 如果拿不到，也不影响 stale-wake no-op 与 supersede 规则成立

## 共享状态面的推荐接口

优先建议对 Desktop heartbeat 暴露只读快照文件，而不是要求它在关键路径上执行 CLI。

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

这样 bridge prompt 和 caller prompt 都可以很短，而且不需要知道底层 store 是 SQLite、普通文件还是 `mmap`。

## Automation 策略

### Bridge heartbeat

- 常驻、低成本。
- 固定 1 分钟 cadence 即可。
- 挂在专用 bridge thread 上。
- 只负责读取 ready index，并为有可投递 batch 的 caller thread arm heartbeat。

### Caller heartbeat

- 不做固定轮询。
- 只在 bridge 发现当前 thread 有可投递 batch 时创建或更新。
- 目标是“一次唤醒、一次读取、一次继续”。

### 对 `run now` 的态度

- UI 和 app bundle 中可以看到 `run now` 语义。
- 但当前还没有把它当成模型可稳定调用的独立 tool 来依赖。
- 第一版实现应只依赖 `FREQ=MINUTELY;INTERVAL=1` 这一条已实证可行的 heartbeat 机制。

## Prompt 合约

### Bridge prompt 要求

- 每次醒来只做一次状态检查。
- 只读取 ready index，不依赖本地 CLI 调用。
- 没有 ready thread 就立即结束。
- 有 ready thread 时，只为对应 caller thread 武装 heartbeat，不直接展开主任务。
- 避免创建重复 automation。
- arm 完成后如果能直接拿到 `automation_id`，可以把它写进 prompt / automation metadata 作为协调信息；拿不到时也不能阻塞关键路径。
- bridge arm 的 durable 完成条件是：
  - attempt 已存在
  - snapshot 已物化
  - 当前 generation 的 caller heartbeat arm 请求已被 Codex 接受

### Caller prompt 要求

- 先读取自己的 per-thread inbox snapshot。
- 只处理当前 snapshot 指向的 head batch。
- 对小结果可以直接读取 inline payload；对大结果读取 `cbth` 管理的 artifact。
- 任务处理完成后可以清理或暂停当前 heartbeat，但不要求在关键路径上回写 `consumed`。
- 旧 generation 的 prompt 只允许 no-op，不允许“顺手处理当前 head batch”。

## Delivery 关闭语义

- Desktop 第一版的目标不是“精确证明 caller 已消费”，而是“在 batch 仍为 head 且允许重投时，至少安排一次 wakeup，并在必要时重投”。
- 因此：
  - `armed` 表示 bridge 已成功为 caller 安排了一次 heartbeat wakeup
  - `cooldown` 表示 `cbth` 正在等待这次 wakeup 的最短观察窗口
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
  - `caller_acknowledged` (future optional)
- 也就是说，Desktop 第一版的自动续跑语义是：
  - `at-least-once wakeup scheduling`
  - not `exactly-once consumption`

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
- 如果 `cooldown_until` 到期后，该 batch 仍然是当前 head batch，且 `close_reason` 仍为空、redelivery window 也未结束，就应该创建新 attempt 并再次 arm，而不是把旧 attempt 直接视为成功送达。

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
- 不把后台 heartbeat 的本地 CLI 执行能力当成关键前提。
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
