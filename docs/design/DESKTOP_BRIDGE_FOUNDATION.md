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
cbth desktop relay consume-transcript \
  --rollout-path <rollout-jsonl> \
  --marker <marker> \
  --json
cbth desktop relay scanner bind \
  --bridge-thread-id <bridge-thread-id> \
  --rollout-path <rollout-jsonl> \
  [--from-start] \
  --json
cbth desktop relay scanner status [--bridge-thread-id <bridge-thread-id>] --json
cbth desktop relay scanner scan-once [--bridge-thread-id <bridge-thread-id>] --json
cbth desktop relay marker issue \
  --bridge-thread-id <bridge-thread-id> \
  --kind arm-pending|arm-accepted \
  --source-thread-id <source-thread-id> \
  --attempt-id <attempt-id> \
  --generation <generation> \
  --bridge-request-id <request-id> \
  --json
cbth desktop relay emit-arm-pending ... --marker <issued-marker> --json
cbth desktop relay emit-arm-accepted ... --marker <issued-marker> --json
```

所有输出都是 JSON。mutating / preflight 命令通过 same-user daemon IPC 路由；旧 daemon 缺少 `desktop-bridge-foundation-dispatch`、`desktop-inbox-revisioned-installation-state`、`desktop-writeback-helper-foundation`、validation-only `desktop-writeback-live-validation-fixture`、`desktop-transcript-relay-consumer`、`desktop-transcript-relay-scanner` 或 `desktop-ready-arm-workflow` capability 时会按现有 capability gate fail closed 或重启。`read-snapshot` / `list-*` / `claim-next-ready` 是 no-DB read helpers：它们只读取已经发布的 inbox JSON，不打开 SQLite、不连接 daemon、不写文件。

另有一个 hidden validation-only probe：`cbth desktop validation writeback-dropbox-probe ...`。它不属于稳定 operator surface，只用于验证真实 Desktop heartbeat 能否在不打开 SQLite、不连接 daemon、不触碰 `startup.lock` 的情况下创建或 append `~/.cbth/inbox/writeback-dropbox/probes/<probe_id>.json`。

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

本阶段没有实现 `binding unbind`、caller automation cleanup 或 quiesced generation writeback。

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

Ready/arm workflow work changes this foundation boundary: `ready_threads.entries` is no longer permanently empty. `bridge-preflight` may materialize at most one eligible Desktop head batch into a ready entry with an issued `arm_pending_requested` marker, while `arm_pending_bindings.entries` may carry issued `arm_accepted` markers for attempts already durably in `arm_pending`. `pause_due_bindings.entries` continue to export armed, unquiesced bindings with `pause_deadline <= now`.

This still does not enable full Desktop automatic delivery. Caller wake, production `automation_update`, pause cleanup, `note-boundary-crossed`, and artifact payload reads remain separate work.

## Desktop Writeback Helpers

`note-arm-pending` 和 `note-arm` 是 Desktop bridge 后续执行 caller wake 前后的 durable writeback primitives。它们只对已经存在的 `adapter_kind=desktop`、当前 head、当前 generation 的 prepared attempt 生效；helper 本身不创建 ready attempt，也不调用 `automation_update`。Ready attempt creation belongs to `bridge-preflight` materialization and is gated by the same Desktop binding, capability, policy, budget, and redelivery checks.

- `note-arm-pending` 以 `(source_thread_id, attempt_id, generation, bridge_request_id)` 做 CAS，只允许 `prepared -> arm_pending`。成功时写入 `bridge_request_id`、`bridge_arm_lease_id`、`bridge_arm_lease_deadline`、`arm_pending_since` 和 `arm_pending_deadline`。
- 同一 request 重试 `note-arm-pending` 会返回同一个 lease；不同 request 遇到 existing `arm_pending` 会 fail closed，不泄露 lease。
- `note-arm` 必须带回同一个 `bridge_request_id` 和 `bridge_arm_lease_id`，只允许 `arm_pending -> cooldown`。成功后才递增 `delivery_attempt_count`，并写入 binding 的 `armed_generation`、清空 `armed_generation_quiesced_at`、设置 `pause_not_before` 与 `pause_deadline`。
- 重复 `note-arm` 对同一已 armed attempt 返回 idempotent success，不重复递增 `delivery_attempt_count`。
- helper 会拒绝 missing / degraded binding、非 head batch、unsafe delivery policy、requires-artifact-read batch、mismatched source thread、mismatched generation、mismatched request / lease、以及非 eligible attempt state。

这些 helper 是 capability validation 的候选面，但 `writeback_capability` 仍只能由 operator 在真实 Desktop heartbeat 验证后通过 `installation-state repair` 写为 `validated`。

真实 Desktop heartbeat 已证明 daemon-routed writeback helper 会在 `note-arm-pending` 前被 `startup.lock` sandbox `EPERM` 阻断。后续 hidden writeback dropbox probe 又证明 heartbeat 不能在 `~/.cbth/inbox/writeback-dropbox` 下创建目录、创建 probe file，或打开预创建的 probe file 追加写入。因此 Desktop v1 不应继续依赖 heartbeat-authored local filesystem writeback。

当前首选的 side channel 是 transcript / tool-output relay：heartbeat 只运行 pure stdout helper，输出带 `CBTH_TRANSCRIPT_WRITEBACK_V1` 前缀的结构化 envelope；外部 operator / sidecar 从 Codex rollout 的 `function_call_output` carrier 中读取精确 stdout，再在 Desktop sandbox 外执行真实 durable CAS writeback。scanner 必须把 `function_call_output` 视为唯一当前 `trusted_auto` carrier，把 assistant final text 归类为 `diagnostic_only`，把 heartbeat prompt 归类为 `ignored_prompt`，避免 prompt 自触发。`cbth desktop relay consume-transcript` 可以手动消费单个可信 `arm_pending_requested` / `arm_requested` envelope，但 marker 必须已经通过 `cbth desktop relay marker issue ...` 签发，且 durable binding / attempt snapshot 仍匹配；通过 marker + canonical envelope hash 做 durable replay fence 后才调用既有 `note-arm-pending` / `note-arm` CAS。

Production scanner foundation 在手动 consumer 之上增加了三层约束：

- `desktop_relay_scanner_bindings` 显式绑定 `bridge_thread_id -> rollout_path`，保存 resolved path、Unix device/inode identity、byte cursor、line cursor、monotonic binding revision、状态和 last error。scanner 不自动发现或切换 rollout；truncate / inode drift 会把 binding 标为 `degraded`。
- `desktop_transcript_relay_markers` 是 issued marker allowlist，保存 expected envelope kind、source / attempt / generation / request tokens、expiry 和 retention。daemon worker 在存在 issued marker + active scanner binding、issued marker + existing consumption fence、expired issued marker，或 due relay retention cleanup 时每 2 秒扫描一次；每 tick 先用 `symlink_metadata` 验证 bound rollout path 是同一 regular file，再用 nonblocking open 打开并用 opened handle 的 metadata / identity 冻结 tick-start EOF，最后读取 active marker set。这样 FIFO / special-file replacement 会在打开前或 nonblocking open 后 fail closed degrade，不会卡住 lifecycle maintenance。每 tick 最多处理 tick-start EOF 之前的 256 个完整 JSONL records 或 1 MiB，首条 record 超过预算时 fail-closed degrade binding。
- Cursor publication, binding degradation, and marker rejection are conditional on the binding still being active at the expected prior path / identity / byte cursor / line cursor / monotonic `binding_revision`. `updated_at` remains observational only and is not a CAS token because it has seconds-level resolution. Older overlapping scanner ticks cannot move the durable cursor backward, clear a degraded binding, degrade a replacement binding, or reject a marker after rebind.
- If a bounded tick sees marker evidence before it reaches tick-start EOF, the scanner degrades the binding instead of consuming or rejecting the marker. This preserves duplicate detection without allowing unbounded rollout reads.
- Fresh scanner consumption re-checks marker state, expiry, expected fields, hash fence, and the scanner binding path / identity / cursor in the same immediate SQLite transaction that applies the arm CAS and marks the marker consumed. This prevents a concurrent reject/expire or rebind/cursor advance from racing with `note-arm-pending` or `note-arm`.
- Production `emit-arm-accepted` envelope 不带 `bridge_arm_lease_id`，scanner 也会拒绝任何带该字段的 trusted `arm_accepted` envelope。`arm-accepted` marker 只能在 attempt 已 durable 进入 `arm_pending`、request/generation/lease 均匹配且 lease 尚未过期时签发；scanner 在 Desktop sandbox 外从该 durable attempt 解析 lease，再调用已有 `note-arm` CAS。这避免把 lease 暴露给 heartbeat prompt/output text，同时保持 CAS token 校验。
- `arm_accepted` 在 pending-only lease lookup 前会先检查同 marker/hash 的成功 replay fence，且 scan maintenance 会先 reconcile 已有 consumption fence 再过期 issued marker，避免 CAS 已提交但 marker 尚未标 consumed 的 crash 窗口把成功写回误判为 expired/rejected。production 不允许先签发 accepted marker 再依赖同 tick pending envelope 排序来补救状态。

2026-05-11 live validation 已证明真实 heartbeat arm envelopes + non-Desktop consumer 可以推进 `prepared -> arm_pending -> cooldown`，并由 operator 将 `writeback_capability` repair 为 `validated`；`artifact_read_capability` 仍为 `unknown`。Production scanner 仍需要后续 opt-in live validation。该 validation surface 记录在 [Desktop transcript relay validation](../validation/DESKTOP_TRANSCRIPT_RELAY_VALIDATION.md) 和 [Desktop live preflight evidence](../validation/DESKTOP_LIVE_PREFLIGHT_EVIDENCE.md)。

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
- 默认 daemon-routed preflight / writeback 缺少 daemon capability `desktop-bridge-foundation-dispatch`、`desktop-inbox-revisioned-installation-state`、`desktop-writeback-helper-foundation`、validation-only `desktop-writeback-live-validation-fixture`、`desktop-transcript-relay-consumer`、`desktop-transcript-relay-scanner` 或 `desktop-ready-arm-workflow` 时不执行 preflight / repair / writeback / fixture setup / relay consume / ready materialization。
- preflight 失败时 bridge 不得读取旧 snapshot 继续 arm。
- no-DB read helper 发现 manifest / snapshot 不一致时不得继续 delivery。
- writeback helper 发现 CAS token、binding、batch、attempt 或 policy 不匹配时不得推进 durable state。
- transcript relay consumer 只接受单个 trusted `function_call_output` envelope；prompt、assistant text、duplicate trusted、malformed trusted、wrong marker 或 replay hash mismatch 都不得推进 durable state。
- production transcript relay scanner 只消费已签发、未过期、字段完全匹配的 marker；scan tick 先对 path 做 pre-open regular-file / identity gate，再用 nonblocking open 和 opened file handle metadata / EOF 冻结，且 fresh CAS 与 marker consumed/rejected 写回同事务完成并复核 scanner binding path / identity / cursor / `binding_revision`；cursor 发布带 expected prior cursor 和 monotonic revision 条件；`arm-accepted` marker 签发要求 attempt 已 durable `arm_pending`，successful replay fences 优先于 pending-only lease lookup 和 marker expiry；partial trailing rollout lines 和 tick-start EOF 之后追加的 lines 会延后处理，special-file replacement / truncate / inode drift / oversized first tick record / marker evidence before tick-start EOF 都会 degrade binding，unissued / expired-without-replay / duplicate / malformed / wrong-field envelopes 和 failed-CAS replay fences 都不得推进 durable state。
- writeback dropbox probe 只允许 validation-only file creation；它不得绕过 `note-arm-pending` / `note-arm` 的 durable CAS，也不得被解释为 automatic Desktop delivery 已启用。
- `ready_threads.entries` 为空不是“没有任何未来工作”的最终语义；它只表示当前 preflight 没有找到可安全 materialize 或可重发的 eligible ready entry。

## Out Of Scope

本阶段不实现：

- caller heartbeat wake / `automation_update` 调用。
- `note-boundary-crossed`。
- Desktop automatic delivery live validation；preflight/read validation workflow is documented separately in [DESKTOP_LIVE_PREFLIGHT_VALIDATION.md](../validation/DESKTOP_LIVE_PREFLIGHT_VALIDATION.md).
- 大 artifact automatic continuation。
- 外部 Webex / GitHub / PR polling integrations。

这些能力仍由 [DESKTOP_BACKGROUND_TASK_BRIDGE_DESIGN.md](DESKTOP_BACKGROUND_TASK_BRIDGE_DESIGN.md) 和 [SHARED_CORE_ARCHITECTURE.md](SHARED_CORE_ARCHITECTURE.md) 定义未来合同。

Writeback helper live validation is tracked separately in [DESKTOP_WRITEBACK_HELPER_LIVE_VALIDATION.md](../validation/DESKTOP_WRITEBACK_HELPER_LIVE_VALIDATION.md).
