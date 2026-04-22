# Desktop Background Task Bridge Design

## 目标

- 不修改上游 `codex` 仓库。
- 保持现有 Codex Desktop 使用方式。
- 让长时间外部任务在完成后，由原 caller thread 自动继续，而不是依赖用户手动回到 thread。
- 允许大约 1 分钟级别的延迟。
- 不要求在 Desktop app 退出后继续工作。

## 已敲定的约束

- 外部进程不能可靠地把消息直接推入 Desktop 当前已加载的 live thread。
- 外部进程可以改写 persisted thread history，但 Desktop 当前 session 不会把这些外部写入热并入自己的可见历史和后续上下文。
- `automation_update` 是 Codex thread 内置能力。
- `automation_update` 可以创建或更新指向其他 thread 的 heartbeat automation。
- heartbeat 触发出来的 turn 本身也可以继续调用 `automation_update`。
- 因此，Desktop 上最可靠的方案不是“外部 sidecar 直接推送 caller thread”，而是“由 app 内部 automation scheduler 去唤醒 caller thread”。

## 核心设计

### 组件

1. `sidecar supervisor`
   - 负责真正执行长时间任务，例如等待 CI、等待 reviewer、等待外部系统结果。
   - 不尝试直接修改 Codex Desktop 的 live session。

2. `shared job state`
   - 由 sidecar 暴露给 Codex thread 读取的共享状态面。
   - 推荐优先级：
     1. 本地 helper CLI
     2. 本地 JSON / SQLite store
     3. 本地 socket
   - 明确不依赖直接改 Codex 自己的 automation DB。

3. `bridge heartbeat thread`
   - 一个固定存在的专用 thread。
   - 低成本、快模型即可。
   - 当前验证使用的 bridge thread：
     - `thread_id = 019db5e6-ba6a-7b80-95d2-a6867163281a`
     - `model = gpt-5.3-codex-spark-preview`
     - `reasoning_effort = low`
   - 职责只有一件事：轮询共享 job state，并在发现某个 caller thread 对应任务 ready 时，用 `automation_update` 为该 caller thread 武装 heartbeat。

4. `caller thread heartbeat`
   - 不是常驻轮询器。
   - 只在 bridge 判定任务 ready 后，才被创建、激活、重定向或更新。
   - 被唤醒后，在原 caller thread 中读取结果并继续原任务。
   - 处理完成后删除或暂停自己，避免 caller thread 出现持续定时 wake 痕迹。

## 设计原则

- 长等待放在 sidecar，不放在 Codex 当前 turn。
- 周期性检查集中在 bridge thread，不污染所有 caller thread。
- caller thread 只在“确实有结果可消费”时被唤醒。
- 不要求 bridge thread 与 caller thread 之间直接 live push；两者都只依赖 automation scheduler 和共享 job state。

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
   - `result_ref`
   - `updated_at`
4. caller thread 不长时间等待，当前 turn 可以结束。

### 2. bridge thread 轮询

bridge heartbeat 每分钟醒一次：

1. 调用本地 helper，例如：

```text
background-taskctl list-ready --json
```

2. 如果没有 ready job，本次 turn 直接结束。
3. 如果有 ready job：
   - 选择目标 `source_thread_id`
   - 用 `automation_update` 为目标 caller thread 创建或更新 heartbeat
   - heartbeat prompt 中带上 `job_id`
   - 可选地把 job 标记为 `armed`

### 3. caller thread 被唤醒

caller thread heartbeat 在下一次调度中醒来：

1. 根据 prompt 中的 `job_id` 调用 helper，例如：

```text
background-taskctl claim-ready <job_id> --json
```

2. 如果 claim 失败，说明结果已被消费或重复武装，本次 turn 直接结束并清理 heartbeat。
3. 如果 claim 成功：
   - 读取任务结果
   - 在原 caller thread 中继续后续工作
   - 完成后调用 helper 标记 `consumed`
   - 删除或暂停自己的 heartbeat automation

## 最小状态机

### Job 状态

- `running`
- `ready`
- `armed`
- `claimed`
- `consumed`
- `failed`
- `cancelled`

### 推荐迁移

```text
running -> ready -> armed -> claimed -> consumed
running -> failed
running -> cancelled
armed -> ready
claimed -> ready
```

说明：

- `armed -> ready` 用于 caller heartbeat 没有按预期消费时的重试。
- `claimed -> ready` 用于 caller thread 醒来后中途失败，需要重新投递。

## 共享状态面的推荐接口

优先建议做一个很小的本地 CLI，避免让 Codex thread 自己解析复杂文件格式。

### Bridge 侧

```text
background-taskctl list-ready --json
background-taskctl mark-armed <job_id> <automation_id>
background-taskctl requeue <job_id>
```

### Caller 侧

```text
background-taskctl claim-ready <job_id> --json
background-taskctl mark-consumed <job_id>
background-taskctl mark-failed <job_id> --reason <text>
```

这样 bridge prompt 和 caller prompt 都可以很短，而且不需要知道底层 store 是文件、SQLite 还是 socket。

## Automation 策略

### Bridge heartbeat

- 常驻、低成本。
- 固定 1 分钟 cadence 即可。
- 挂在专用 bridge thread 上。
- 只负责检查 ready job 和 arm caller heartbeat。

### Caller heartbeat

- 不做固定轮询。
- 只在 bridge 发现 ready job 时创建或更新。
- 目标是“一次唤醒、一次消费、一次清理”。

### 对 `run now` 的态度

- UI 和 app bundle 中可以看到 `run now` 语义。
- 但当前还没有把它当成模型可稳定调用的独立 tool 来依赖。
- 第一版实现应只依赖 `FREQ=MINUTELY;INTERVAL=1` 这一条已实证可行的 heartbeat 机制。

## Prompt 合约

### Bridge prompt 要求

- 每次醒来只做一次状态检查。
- 没有 ready job 就立即结束。
- 有 ready job 时，只为对应 caller thread 武装 heartbeat，不直接展开主任务。
- 避免创建重复 automation。
- arm 完成后记录或回写 `automation_id`。

### Caller prompt 要求

- 先 claim 指定 `job_id`。
- claim 成功才继续任务。
- 任务处理完成后立即清理或暂停当前 heartbeat。
- 失败时把 job 退回 `ready` 或写回错误状态，避免静默丢失。

## 失败与重试

### Sidecar 还没完成

- bridge thread 下次 heartbeat 再检查。

### Bridge arm 失败

- job 保持 `ready`。
- 下一次 bridge heartbeat 重试。

### Caller heartbeat 醒来但 claim 失败

- 说明已被其他 turn 消费或状态已变化。
- 当前 turn 直接退出并清理 heartbeat。

### Caller 处理中途失败

- 把 job 放回 `ready` 或显式标为 `failed`。
- 由 bridge 决定是否再次 arm。

## 已实证支持的关键能力

- 当前 thread 可创建指向其他 thread 的 heartbeat automation。
- heartbeat turn 内部可以调用 `automation_update`。
- bridge thread 可在自己的 heartbeat turn 中，把 automation retarget 到 caller thread。
- retarget 完成后，caller thread 会在下一分钟被成功唤醒。

## 当前不打算做的事

- 不尝试外部进程直接向 Desktop 当前 live thread push 消息。
- 不依赖外部直接改 Codex automation DB。
- 不以“不留下任何 caller thread 唤醒痕迹”为目标；当前目标是把痕迹压缩到“任务 ready 时的一次唤醒”。
- 不覆盖 Desktop app 退出后的常驻通知场景。

## 第一版实现建议

1. 固定一个 bridge heartbeat thread。
2. 实现 `background-taskctl` 最小 CLI。
3. 让 sidecar 只负责写 job 状态。
4. 让 bridge thread 每分钟查询 `list-ready --json`。
5. bridge 发现 ready job 后，为对应 caller thread arm 一次 heartbeat。
6. caller thread 醒来后 `claim-ready`，继续任务，并在结束后清理 heartbeat。

这套方案的关键优点是：

- 不改 Codex Desktop 内部实现。
- 不依赖外部 live push。
- 不需要用户手动切回某个 notification thread。
- caller thread 可以在原上下文内继续任务。
