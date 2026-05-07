# Desktop Bridge Foundation

本文记录 Desktop bridge foundation 的第一版实现边界。它已经落地安装级状态、thread binding、revision-consistent inbox snapshot、no-DB read helpers，以及 `note-arm-pending` / `note-arm` writeback primitives；它不表示 Desktop automatic delivery 已可用。

## Current Scope

本阶段新增的稳定 helper / operator 面如下：

```bash
cbth desktop installation-state --json
cbth desktop installation-state repair \
  --read-transport direct-file-read \
  [--read-transport-capability unknown|validated] \
  [--artifact-read-capability unknown|validated] \
  [--writeback-capability unknown|validated] \
  [--validation-fingerprint <fingerprint>] \
  --json
cbth desktop binding repair \
  --source-thread-id <thread-id> \
  --caller-automation-id <automation-id> \
  --json
cbth desktop bridge-preflight --bridge-thread-id <thread-id> --json
cbth desktop bridge-preflight \
  --bridge-thread-id <thread-id> \
  --helper-direct-store \
  --json
cbth desktop read-snapshot --bridge-thread-id <thread-id> --json
cbth desktop list-arm-pending --bridge-thread-id <thread-id> --json
cbth desktop list-pause-due --bridge-thread-id <thread-id> --json
cbth desktop claim-next-ready --bridge-thread-id <thread-id> --json
cbth desktop note-arm-pending \
  --source-thread-id <thread-id> \
  --attempt-id <attempt-id> \
  --generation <generation> \
  --bridge-request-id <request-id> \
  --json
cbth desktop note-arm \
  --source-thread-id <thread-id> \
  --attempt-id <attempt-id> \
  --generation <generation> \
  --bridge-request-id <request-id> \
  --bridge-arm-lease-id <lease-id> \
  --json
```

所有输出都是 JSON。mutating / preflight 命令通过 same-user daemon IPC 路由；旧 daemon 缺少 `desktop-bridge-foundation-dispatch`、`desktop-inbox-revisioned-installation-state`、`desktop-writeback-helper-foundation` 或 validation-only `desktop-writeback-live-validation-fixture` capability 时会按现有 capability gate fail closed 或重启。`read-snapshot` / `list-*` / `claim-next-ready` 是 no-DB read helpers：它们只读取已经发布的 inbox JSON，不打开 SQLite、不连接 daemon、不写文件。

## Installation State

`desktop_installation_state` 是 Desktop 安装级 capability authority。它负责回答“当前这台机器上的 Desktop bridge 可以用哪一种读取路径，以及这些 capability 结论是否还可信”。

它不是 per-thread 状态，也不是 per-batch 授权。所有 Desktop binding 只能镜像并消费它，不能单独覆盖 capability 结论。

当前 singleton 字段：

- `read_transport`: 当前唯一实现值是 canonical `direct_file_read`；CLI flag 形式是 `direct-file-read`。
- `read_transport_generation`: transport、fingerprint 或 capability 发生实际变化时单调递增；no-op repair 不递增。
- `read_transport_capability`: `unknown` 或 `validated`。
- `artifact_read_capability`: `unknown` 或 `validated`。
- `writeback_capability`: `unknown` 或 `validated`。
- `validation_fingerprint`: 绑定本地 helper 环境的 deterministic fingerprint，默认覆盖 `cbth` version、platform、current executable path、inbox schema version 和 read transport。
- `validated_at`: 任一 capability 被写成 `validated` 且 repair 实际改变 state 时写入；no-op repair 保留原值。
- `created_at` / `updated_at`: durable 操作时间。

默认 `installation-state --json` 不写库；没有记录时返回 generation `0`、capability 全部 `unknown`、fingerprint 为当前 deterministic fingerprint 的 synthetic default。

`installation-state repair` 是当前唯一写入口。它的规则是：

- 未提供 `--validation-fingerprint` 时使用 deterministic local fingerprint。
- transport / fingerprint / capability 有实际变化时写入新记录并递增 generation。
- 同一参数重复执行是 no-op，不递增 generation，也不刷新 `validated_at`。
- repair 后所有镜像 generation / fingerprint / transport 不再匹配的 bound bindings 会被标为 `degraded`。

## Desktop Binding

`desktop_bindings` 负责把一个 Desktop source thread 绑定到一个 caller heartbeat automation：

- `source_thread_id`
- `caller_automation_id`
- `binding_state`: 当前 foundation 只创建 / 修复为 `bound`，并在 installation drift 时标成 `degraded`。
- `armed_generation`
- `armed_generation_quiesced_at`
- `pause_not_before`
- `pause_deadline`
- `read_transport`
- `read_transport_generation`
- `validation_fingerprint`
- `created_at` / `updated_at`
- `degraded_at`

`cbth desktop binding repair ...` 会读取当前 installation state，把 transport generation 与 fingerprint 镜像到 binding。后续 bridge 运行时必须确认 binding 仍是 `bound`，且镜像字段仍与 installation state 一致；不一致时不得自动 delivery。

同一个 active caller automation 只能被一个 source thread 占用。`binding repair` 会拒绝把已经属于其他 `bound` / `degraded` binding 的 `caller_automation_id` 绑定给新的 `source_thread_id`。

本阶段没有实现 `binding unbind`、caller automation cleanup、quiesced generation writeback 或 ready attempt materialization。

## Bridge Preflight Snapshots

`cbth desktop bridge-preflight --bridge-thread-id ... --json` 是 snapshot publisher helper。即使 bridge 最终使用 `direct_file_read`，也必须先有一个非 Desktop-sandbox publisher 调用 preflight，以便：

- 按需启动 same-user daemon。
- 执行现有 maintenance sweep / GC。
- 原子发布同一 `snapshot_revision` 的 inbox snapshot set。
- 避免 bridge 读取旧 snapshot 后继续推进 delivery。

默认 preflight 仍通过 same-user daemon 路由。`--require-existing-daemon` 只连接已经存在且兼容的 daemon，不 autostart，也不触碰 `startup.lock`。`--helper-direct-store` 不使用 daemon autostart、`startup.lock` 或 Unix socket，而是在当前 `cbth` 进程内打开 store、执行 sweep 并发布同样的 snapshot set。`--helper-direct-store` 与 `--require-existing-daemon` 互斥；direct-helper 失败时必须 fail closed，不 fallback 到 daemon 或旧 snapshot。

当前 preflight 发布一个稳定 manifest、一个 latest-only installation-state export，以及四份 revision-specific snapshot 文件：

- `~/.cbth/inbox/current-snapshot.json`
- `~/.cbth/inbox/snapshots/<snapshot_revision>/ready-threads.json`
- `~/.cbth/inbox/snapshots/<snapshot_revision>/arm-pending-bindings.json`
- `~/.cbth/inbox/snapshots/<snapshot_revision>/pause-due-bindings.json`
- `~/.cbth/inbox/snapshots/<snapshot_revision>/desktop-installation-state.json`

`current-snapshot.json` 必须最后写入，并且只引用 immutable revision-specific snapshot files。这样即使新的 preflight 正在发布或中途失败，旧 manifest 引用的旧 files 仍保持一致，不会因为固定文件名被覆盖而变成 revision mismatch。

`~/.cbth/inbox/desktop-installation-state.json` 仍作为 latest-only convenience export 保留，用于 operator inspection。它由 `bridge-preflight` 原子刷新，但不是 revision-consistent snapshot 的一部分；no-DB readers 必须使用 manifest 指向的 `snapshots/<snapshot_revision>/desktop-installation-state.json`。它也不是 capability 写入口，capability 仍只能通过 `installation-state repair` 写入。

每个 snapshot 文件都包含：

- `schema_version = 1`
- 相同的 `snapshot_revision`
- 相同的 `created_at`
- 相同的 `bridge_thread_id`

`ready_threads.entries` 仍为空，`count = 0`，因为真实 ready selection 和 attempt creation 还未实现。`arm_pending_bindings.entries` 会导出当前 open head Desktop attempts 中处于 `arm_pending` 的 reconcile metadata；`pause_due_bindings.entries` 会导出已 armed、未 quiesced、且 `pause_deadline <= now` 的 bound bindings。当前 PR 只发布这些 reconcile entries，不执行 pause cleanup 或 automatic delivery。

## Desktop Writeback Helpers

`note-arm-pending` 和 `note-arm` 是 Desktop bridge 后续执行 caller wake 前后的 durable writeback primitives。它们只对已经存在的 `adapter_kind=desktop`、当前 head、当前 generation 的 prepared attempt 生效；本阶段仍不创建 ready attempt，也不调用 `automation_update`。

- `note-arm-pending` 以 `(source_thread_id, attempt_id, generation, bridge_request_id)` 做 CAS，只允许 `prepared -> arm_pending`。成功时写入 `bridge_request_id`、`bridge_arm_lease_id`、`bridge_arm_lease_deadline`、`arm_pending_since` 和 `arm_pending_deadline`。
- 同一 request 重试 `note-arm-pending` 会返回同一个 lease；不同 request 遇到 existing `arm_pending` 会 fail closed，不泄露 lease。
- `note-arm` 必须带回同一个 `bridge_request_id` 和 `bridge_arm_lease_id`，只允许 `arm_pending -> cooldown`。成功后才递增 `delivery_attempt_count`，并写入 binding 的 `armed_generation`、清空 `armed_generation_quiesced_at`、设置 `pause_not_before` 与 `pause_deadline`。
- 重复 `note-arm` 对同一已 armed attempt 返回 idempotent success，不重复递增 `delivery_attempt_count`。
- helper 会拒绝 missing / degraded binding、非 head batch、unsafe delivery policy、requires-artifact-read batch、mismatched source thread、mismatched generation、mismatched request / lease、以及非 eligible attempt state。

这些 helper 是 capability validation 的候选面，但 `writeback_capability` 仍只能由 operator 在真实 Desktop heartbeat 验证后通过 `installation-state repair` 写为 `validated`。

## No-DB Inbox Read Helpers

真实 Desktop heartbeat 可能无法打开 SQLite、daemon socket 或 `startup.lock`。因此 heartbeat v1 可以改用 no-DB read helpers 消费已经发布的 inbox snapshot：

- `read-snapshot` 读取并校验 current manifest、三份 revision data snapshot 和 revision-specific `desktop-installation-state.json`。
- `list-arm-pending` 读取 `arm-pending-bindings.json` 的 entries。
- `list-pause-due` 读取 `pause-due-bindings.json` 的 entries。
- `claim-next-ready` 读取 `ready-threads.json` 并返回第一条 ready entry 或 `null`。

这些 helper 都是 pure read。`claim-next-ready` 当前只是 read/peek，不 reservation、不隐藏 head、不改变 durable state。它们会校验 `schema_version`、`snapshot_revision`、`bridge_thread_id`、manifest 引用路径和文件大小；任何不一致都 fail closed。

权限合同沿用私有文件约束：

- `~/.cbth/inbox` directory: `0700`
- `~/.cbth/inbox/snapshots/<snapshot_revision>` directory: `0700`
- snapshot / installation-state regular files: `0600`

## Fail-Closed Boundaries

当前 Desktop path 仍 fail closed：

- 未 validated 的 installation state 不允许 automatic Desktop delivery。
- `degraded` binding 不允许 automatic Desktop delivery。
- 默认 daemon-routed preflight / writeback 缺少 daemon capability `desktop-bridge-foundation-dispatch`、`desktop-inbox-revisioned-installation-state`、`desktop-writeback-helper-foundation` 或 validation-only `desktop-writeback-live-validation-fixture` 时不执行 preflight / repair / writeback / fixture setup。
- preflight 失败时 bridge 不得读取旧 snapshot 继续 arm。
- no-DB read helper 发现 manifest / snapshot 不一致时不得继续 delivery。
- writeback helper 发现 CAS token、binding、batch、attempt 或 policy 不匹配时不得推进 durable state。
- `ready_threads.entries` 为空不是“没有任何未来工作”的最终语义；它只是本阶段尚未实现 ready materialization。

## Out Of Scope

本阶段不实现：

- caller heartbeat wake / `automation_update` 调用。
- ready attempt materialization。
- `note-boundary-crossed`。
- writeback helper live Desktop heartbeat validation。
- Desktop automatic delivery live validation；preflight/read validation workflow is documented separately in [DESKTOP_LIVE_PREFLIGHT_VALIDATION.md](DESKTOP_LIVE_PREFLIGHT_VALIDATION.md).
- 大 artifact automatic continuation。
- 外部 Webex / GitHub / PR polling integrations。

这些能力仍由 [DESKTOP_BACKGROUND_TASK_BRIDGE_DESIGN.md](DESKTOP_BACKGROUND_TASK_BRIDGE_DESIGN.md) 和 [SHARED_CORE_ARCHITECTURE.md](SHARED_CORE_ARCHITECTURE.md) 定义未来合同。

Writeback helper live validation is tracked separately in [DESKTOP_WRITEBACK_HELPER_LIVE_VALIDATION.md](DESKTOP_WRITEBACK_HELPER_LIVE_VALIDATION.md).
