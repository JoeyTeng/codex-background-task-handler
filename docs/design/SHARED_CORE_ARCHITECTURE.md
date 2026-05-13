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
- CLI 路径里 `CBTH_HOME`、daemon、daemon-owned app-server、Codex thread、managed session、foreground wrapper process 和 background task 的拓扑关系见 `docs/design/CLI_SHARED_APP_SERVER_SIDECAR_DESIGN.md` 的“运行拓扑与对应关系”。

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
  - v1 不要求 daemon 为大多数长时间窗口持续驻留；它只需要把 deadline durable 落盘，并在下一次启动时先做 overdue sweep。
  - 唯一例外是 CLI accepted attempt 的 `delivery_observation_deadline`：
    - 这是 live-observation window，而不是“允许下次启动再 sweep”的普通长窗口 deadline
    - 只要它还没到期，daemon 就必须持续保活并观察同一条 event stream
  - 当且仅当以下条件同时满足时，daemon 才允许在 idle timeout 后自动退出：
    - 没有 active jobs
    - 没有活跃接入端
    - 没有“需要在 idle timeout 内继续本地观察”的近端 delivery work
  - 对超出当前 idle timeout 的 `arm_pending_deadline` / `pause_deadline` / `redelivery_window_ends_at` / artifact GC deadline：
    - daemon 可以退出
    - 但下次任何入口拉起 daemon 时，必须先执行一次 deterministic overdue sweep
    - 把已到期的 deadline / GC / auto-close / reconcile 全部补做完，再处理新请求
  - Desktop bridge heartbeat 是这个规则的显式入口之一：
    - 即使 bridge 采用 `direct_file_read` 读取 snapshots，每轮 wake 也必须先调用窄 helper `cbth desktop bridge-preflight ...`
    - 这个 preflight 负责按需拉起 daemon、执行 overdue sweep、刷新 snapshots
    - `direct_file_read` 只是 refreshed snapshot 的读取传输，不是 daemon liveness / sweep 机制
  - 上面这条“允许退出并 sweep”的规则不适用于 `delivery_observation_deadline`。
  - 换句话说，Desktop v1 的 `manual_resolution_only` batch 不能因为等待 operator close 就强迫 daemon 无限常驻；可靠性来自 durable deadline + next-start sweep，而不是常驻进程。

### 3. 第一版公共接口只做 CLI

- 第一版不承诺稳定的 socket API。
- 第一版不承诺稳定的 Web API。
- 第一版不承诺动态插件加载协议。
- 第一版唯一稳定的外部接入面是 CLI 命令。
- 任何外部脚本、future plugin、future Web bridge，都先通过 CLI 命令与核心系统交互。

### 4. Desktop 关键路径优先只读

- 第一版不把“Desktop heartbeat turn 能稳定执行通用 `cbth job ...` CLI”当作既定前提。
- Desktop adapter 的稳定关键路径应优先依赖：
  - bridge 每轮 wake 先调用 mandatory `cbth desktop bridge-preflight ...`
  - bridge 侧只读 ready/reconcile metadata snapshot 文件
  - caller 侧由窄 helper 原子开启 continuation，并且在 `note-boundary-crossed` success 之前，不向 automatic caller path 物化 payload / artifact 内容
- 只有在 bridge 侧 `direct_file_read` 不成立、且 helper 执行能力已被单独验证后，才条件性依赖额外读 helper：
  - `cbth desktop list-arm-pending ...`
  - `cbth desktop list-pause-due ...`
  - `cbth desktop claim-next-ready ...`
- 当前无论读路径怎么选，写回仍落到既定窄 CAS primitive；真实 Desktop heartbeat 不一定直接执行这些 mutating helper，也可以通过 transcript relay 输出请求，由 Desktop sandbox 外的 consumer 执行 CAS：
  - `cbth desktop note-arm-pending ...`
  - `cbth desktop note-arm ...`
  - `cbth desktop note-boundary-crossed ...`
- 在 v1 自动 caller path 里，`cbth desktop note-boundary-crossed ...` 还是一个 gated access helper：
  - 成功时必须原子地完成 boundary crossing durable write
  - 并把当前 v1 supported handoff 所需的 inline continuation payload / summary 返回给 caller
- `cbth desktop read-artifact ...` 不再属于 v1 automatic caller path：
  - 它保留给 operator/manual recovery
  - 或 future-expansion 的大 artifact continuation
- `cbth desktop note-delivered ...` 目前不属于第一版自动成功路径；它保留为未来可能的 post-output / post-side-effect ack 扩展点。
- 因此，Desktop 第一版的自动续跑门槛不是“batch 只读”单条件，而是两层同时成立：
  - batch 自身满足只读 / 低风险 delivery policy
  - 当前安装上的 Desktop 读路径已被验证可在 heartbeat 中无审批执行
  - 当前安装上的 Desktop writeback path 已被验证：heartbeat 能产生可信写回请求，且 Desktop sandbox 外的 consumer 能按 replay / CAS 合同写回
- `requires_artifact_read=true` 的 batch 不再进入 v1 automatic caller path：
  - 它们直接留在 manual/operator follow-up
- 这里的“只读 / 低风险”只约束自动投递与断点写回这条外围机制本身。
- caller 被唤醒后的后续推理与工具选择仍受 Codex 自身的 sandbox / approval policy 约束；本项目不把这些后续动作一并宣称成“已被外围系统降成低风险”。
- Desktop 的关键投递路径优先依赖只读状态面：
  - bridge 侧只读 inbox snapshot 文件
  - caller 侧不在 boundary crossing 前直接读取 per-thread envelope / artifact 文件
- 后续内部实现可以用普通文件、`mmap` 或 shared memory 优化，但外部语义先固定为“读一个稳定路径下的只读快照”。
- 这条只读文件路径当前仍是第一版候选主路径，必须在 Desktop heartbeat 无审批读取实证通过后，才升级成“已验证主路径”。
- `direct_file_read` 成立还必须搭配已验证的 `bridge-preflight`：
  - preflight 成功前不得信任磁盘上的 ready / reconcile snapshot 是 fresh 的
  - preflight 失败时，本轮 bridge wake 只能退出，不能根据旧 snapshot 继续 arm

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
~/.cbth/inbox/current-snapshot.json
~/.cbth/inbox/snapshots/<snapshot_revision>/ready-threads.json
~/.cbth/inbox/snapshots/<snapshot_revision>/arm-pending-bindings.json
~/.cbth/inbox/snapshots/<snapshot_revision>/pause-due-bindings.json
```

`current-snapshot.json` 是 `bridge-preflight` 原子发布的 stable manifest；它必须指向 immutable revision-specific data files。bridge 必须按 manifest revision 校验所有读取文件。

附加的 `by-thread/<thread_id>.json` / artifact 文件只允许作为 operator/debug export：

```text
~/.cbth/inbox/by-thread/<thread_id>.json   # optional diagnostic export, disabled by default
~/.cbth/artifacts/<artifact_id>/manifest.json   # diagnostic / operator path only
~/.cbth/artifacts/<artifact_id>/payload   # diagnostic / operator path
```

2. mandatory preflight

```text
cbth desktop bridge-preflight --bridge-thread-id <thread_id> --json
```

3. optional bridge-side `helper_cli_read` fallback

```text
cbth desktop list-arm-pending --bridge-thread-id <thread_id> --json
cbth desktop list-pause-due --bridge-thread-id <thread_id> --json
cbth desktop claim-next-ready --bridge-thread-id <thread_id> --json
```

4. writeback / gated continuation helpers

```text
cbth desktop note-arm-pending --source-thread-id <thread_id> --attempt-id <attempt_id> --generation <generation> --bridge-request-id <request_id> --json
cbth desktop note-arm --source-thread-id <thread_id> --attempt-id <attempt_id> --generation <generation> --bridge-request-id <request_id> --bridge-arm-lease-id <lease_id> --json
cbth desktop note-boundary-crossed --source-thread-id <thread_id> --batch-id <batch_id> --attempt-id <attempt_id> --generation <generation> --expected-snapshot-revision <revision> --json
```

5. operator / future-expansion artifact helper

```text
cbth desktop read-artifact --artifact-id <artifact_id> --artifact-read-lease-id <lease_id> --offset <offset> --max-bytes <n> --json
```

bridge 侧 `direct_file_read` 与 `helper_cli_read` 必须返回同一个 ready-entry schema。
caller 侧 automatic continuation 则必须通过 `note-boundary-crossed` success 返回来获得 inline continuation payload / summary。

`bridge-preflight` 是每轮 bridge wake 的 mandatory helper：它按需拉起 daemon，执行 overdue sweep / auto-close / artifact GC / binding reconcile，并原子发布本轮 snapshot manifest。`helper_cli_read` 的合同要额外收紧：

- 它不是“完全摆脱本地 CLI 执行依赖”的路径。
- 它只是 Desktop 在 `direct_file_read` 失败时可考虑的窄 helper fallback。
- 在把它升级成正式受支持路径之前，必须单独验证：
  - heartbeat turn 无审批执行 `bridge-preflight`
  - heartbeat turn 无审批执行 bridge-side read fallback helpers
  - read fallback helper 返回的 bridge-side metadata / locator 合同与 `direct_file_read` 等价
  - automatic caller path 需要的 continuation 内容只允许通过 `note-boundary-crossed` success 暴露
- 因此，第一版当前真正的优先候选仍然是 `bridge-preflight + direct_file_read`；额外 `helper_cli_read` 只是条件性 fallback，不应在文档里被表述成已验证主路径。

`direct_file_read` 的第一版自动路径只暴露 bridge-ready / reconcile metadata：

```text
~/.cbth/inbox/current-snapshot.json
~/.cbth/inbox/snapshots/<snapshot_revision>/ready-threads.json
~/.cbth/inbox/snapshots/<snapshot_revision>/arm-pending-bindings.json
~/.cbth/inbox/snapshots/<snapshot_revision>/pause-due-bindings.json
```

`by-thread/<thread_id>.json` 与 artifact 文件只允许作为 operator/debug export，默认不属于自动 caller path，也不应用来绕过 continuation boundary。
- 也就是说，pre-boundary automatic path 在磁盘上只看得到：
  - ready / reconcile metadata
  - prompt token
  - bridge-side internal locator
- 真正的 automatic caller continuation 内容只能在 `note-boundary-crossed` 成功返回中首次 materialize 给 caller。

更新方式：

- daemon 先写入 revision-specific snapshot files，再发布一个包含 `snapshot_revision` 与各文件 locator 的 `current-snapshot.json` manifest。
- manifest 用 `write temp + rename` 原子替换；单个数据文件也必须用 temp + rename，但多文件一致性只由 manifest revision 合同保证。
- bridge 必须先读取 manifest，再读取 manifest 指向的文件，并确认每个文件内嵌 `snapshot_revision` 都等于 manifest revision；任何 mismatch 都必须 fail closed。
- 外部语义固定为“读同一 generation 的快照 manifest + 文件”。
- 后续如果内部改成 `mmap` / shared memory，只能在不改变这一语义的前提下做。
- `direct_file_read` 在 Desktop 无审批读取能力得到实证前，仍视为候选内部 contract，而不是已冻结接口。
- 如果 `direct_file_read` 无法满足无审批读取约束，Desktop 第一版只能切到“已单独验证过的 `helper_cli_read`”，否则就继续保留为候选方案；不能直接把未验证 helper 执行前提当主链路。
- 无论哪种传输，`~/.cbth` 根目录默认要求：
  - directory mode `0700`
  - regular file mode `0600`
  - 临时写入文件在 rename 前也必须保持同等权限
- 但这些文件权限和稳定 `cbth desktop ...` CLI 面都不是 per-invocation 授权机制：
  - 它们只能降低意外暴露面
  - 不能防御“同一本机用户下的其他本地进程调用 helper / 恢复 prompt token”
  - 因此 `source_thread_id + batch_id + attempt_id + generation + snapshot_revision` 在 v1 里只是 correctness fencing，不是对抗同用户本地进程的身份认证
  - 因此 Desktop helper / snapshot 路线同样只支持 dedicated single-user deployment assumption

### 5. `local IPC`

职责：

- 让 CLI 子命令与 daemon 通信。
- 在 daemon 未启动时，支持 auto-start / reconnect。

第一版定位：

- 这是内部实现细节，不是对外承诺的稳定公共 API。
- 对外只承诺 CLI 子命令行为，不承诺 socket 协议长期兼容。
- 但它仍是安全边界的一部分：稳定 CLI 子命令可以提交 job、关闭 batch、repair binding、读取 recovery envelope，因此 daemon IPC v1 必须是 same-user-only。
- macOS / Linux 第一版只支持 Unix domain socket：
  - socket path 位于 `~/.cbth/run/cbth.sock` 或等价 `0700` 用户私有目录
  - parent directories 必须由当前 uid 拥有且 mode 不宽于 `0700`
  - socket 文件必须由当前 uid 拥有且 mode 不宽于 `0600`
  - daemon 接受连接后必须校验 peer uid 等于 daemon owner uid
  - macOS 使用 `getpeereid`，Linux 使用 `SO_PEERCRED` 或等价机制
- 如果平台或运行环境无法提供 same-user peer proof，相关 mutating / recovery CLI 命令必须 fail closed，不得退回 unauthenticated loopback TCP。
- 纯 Windows IPC 不属于 v1 支持范围；未来若支持，必须先定义 named-pipe owner / ACL 等价合同。

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
  - mandatory `cbth desktop bridge-preflight ...`
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
- `armed_generation_quiesced_at` (optional)
- `pause_not_before` (optional)
- `pause_deadline` (optional)
- `bridge_thread_id`
- `read_transport`
  - `direct_file_read`
  - `helper_cli_read`
- `read_transport_generation`
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
- `validation_fingerprint`
- `created_at`
- `updated_at`
- `last_verified_at`

- Desktop 安装级还必须有一个单独的 singleton `desktop_installation_state` 作为权威来源：
  - `read_transport`
  - `read_transport_generation`
  - `read_transport_capability`
  - `artifact_read_capability`
  - `writeback_capability`
  - `validation_fingerprint`
  - `validated_at`
  - `updated_by_bootstrap_or_repair`
- 这个 installation state 的 source of truth 必须由 `cbth` durable 持有：
  - bootstrap / repair 是唯一允许更新它的路径
  - bridge 运行期必须优先读取它，再检查 binding 上的镜像字段是否一致
  - installation-wide capability 结论只允许由 installation state 自己写入；binding repair 只能消费它，不能覆盖它
  - `validation_fingerprint` 至少必须覆盖：
    - 当前 Codex Desktop / helper binary 版本或 build identity
    - 当前 `cbth` helper surface / compatibility revision
    - 与无审批读取 / 写回能力直接相关的本地权限与执行环境形状
  - 只要当前观测到的 fingerprint 与 installation state 里 durable 的 `validation_fingerprint` 不一致：
    - installation-wide capability 结论就必须被视为失效
    - bridge 不得继续把该安装当成 `validated`
  - 这些 capability 结论始终绑定在当前 `read_transport_generation` 上：
    - 只要 `read_transport_generation` 递增，旧 generation 上的 `validated` 结论就不能继续被 bridge 使用
    - `installation-state repair` 可以在同一次 operator-validated repair 中写入新的 capability 结论
    - 未显式提供 capability flags 时，CLI 默认写入 `unknown`
    - 同一参数重复 repair 必须是 no-op，不递增 generation，也不刷新 `validated_at`
  - 推荐暴露面：
    - preferred: `~/.cbth/inbox/desktop-installation-state.json`
    - fallback: `cbth desktop installation-state --json`

约束：

- Desktop 自动续跑只对同时满足以下条件的 thread 生效：
  - `binding_state=bound`
  - `read_transport_capability=validated`
  - `writeback_capability=validated`
  - binding 镜像的 `validation_fingerprint` 等于当前 `desktop_installation_state.validation_fingerprint`
- Desktop v1 中，`read_transport_capability=validated` 不是单纯“能读文件”的结论；它必须同时证明：
  - bridge heartbeat 可以无审批执行 mandatory `cbth desktop bridge-preflight ...`
  - preflight 能按需拉起 daemon、完成 overdue sweep / refresh snapshot
  - 当前 installation-wide `read_transport` 能无审批读取 preflight 刷新的 ready/reconcile snapshots
- `requires_artifact_read=true` 的 batch 不进入 v1 automatic caller path
- Desktop v1 不支持同一安装里 mixed `read_transport` bindings：
  - 同一 Desktop 安装只允许一个 installation-wide `read_transport`
  - binding 上的 `read_transport + read_transport_generation` 只是这个安装当前选定 transport 的 durable 镜像，用于 bootstrap 校验、诊断和 stale-binding 检测
  - 如果 binding 镜像与 installation state 不一致，该 binding 必须进入 `degraded` 或重新 bootstrap
- `unbound` thread 可以继续提交 job，但 bridge 不得尝试自动 arm caller heartbeat。
- 运行期 bridge 不负责发现新的 caller automation id；第一版要求这个 id 通过 bootstrap 预先 durable 绑定。
- `degraded` 表示该 thread 暂时失去自动续跑能力：
  - bridge 不再自动 arm
  - 当前 attempt 必须收敛到 `abandoned`
  - 如果尚未 `handoff_recorded`，当前 head batch 保持未关闭，等待 operator 恢复或人工处理
  - 如果已经 `handoff_recorded`，batch 已关闭且不再阻塞 FIFO；operator recovery 必须按 `batch_id` 查看 `boundary_recovery_envelope`
- `armed_generation` 是这个长期复用 caller heartbeat 的 generation CAS 栅栏：
  - bridge arm 成功并 `note-arm` durable 后，才允许把它更新为当前 generation
  - 后续 bridge 想把该 heartbeat 切回 `PAUSED` 时，也必须带着期望 generation 比较 `armed_generation`
  - 只要 binding 上的 `armed_generation` 已经变成更新 generation，任何旧 generation 的 cleanup/pause 都必须 no-op
  - `note-arm` 更新 `armed_generation` 时必须清空 `armed_generation_quiesced_at`
  - 只有 bridge 已验证该 generation 对应的 caller heartbeat 已经 `PAUSED` / deleted / otherwise quiesced，才允许设置 `armed_generation_quiesced_at`
  - 同一 binding 在 `armed_generation_quiesced_at` 为空时不得 fresh-arm 下一批；`handoff_recorded` 释放 FIFO 不等于 caller heartbeat 已 quiesced
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
  - 如果能够证明同一 attempt 已经成功 `note-boundary-crossed`，则该 batch 必须保持 `closed + close_reason=handoff_recorded`
  - 如果能够证明当前 generation 对应的 caller heartbeat 已经重新 `PAUSED`，则当前 attempt 进入 `abandoned`，head batch 保持 `replay_policy=automatic`
  - 只有在既无法证明 arm 成功、也无法证明 heartbeat 已重新 pause 时，当前 head batch 才切到 `replay_policy=manual_resolution_only`，binding 才进入 `degraded`
- 一旦 caller 已成功写入 `cbth desktop note-boundary-crossed ...`，当前 batch 就必须关闭为 `close_reason=handoff_recorded`；第一版不再提供 post-boundary “已展示/已消费”自动收口。
- 为了避免 FIFO 队列永久卡死，第一版必须给 operator 至少两条显式恢复路径：
  - `cbth desktop binding repair --source-thread-id ... --caller-automation-id ... --json`
  - `cbth desktop installation-state repair --read-transport ... [--read-transport-capability ...] [--artifact-read-capability ...] [--writeback-capability ...] --json`
  - `cbth batch close-head --source-thread-id ... --reason operator_closed_unconfirmed --json`
  - `cbth batch close-head --source-thread-id ... --reason operator_confirmed_delivery --json`
  - `cbth batch inspect --batch-id ... --json`

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
4. 只要还存在以下任一条件，daemon 就继续运行：
  - active jobs
  - 活跃 integration clients
  - 需要在当前 idle timeout 内继续观察的近端 delivery work
    - 例如等待匹配的 `delivery_turn_id -> turn/completed`
      - 这类 CLI observation 必须同时受 `delivery_observation_deadline` 约束
    - 例如 `arm_pending_deadline` / `pause_deadline` / `cooldown_until` 会在当前 idle timeout 内到期
    - 例如 artifact GC / auto-close deadline 已经 overdue，或会在当前 idle timeout 内到期
5. 只有当同时满足以下条件时，daemon 才允许退出：
   - 没有 active jobs
   - 没有活跃 integration clients
   - 没有需要在当前 idle timeout 内继续本地观察的近端 delivery work
6. 以下 durable 状态本身不阻止 daemon 退出：
   - open 但长窗口的 batch / attempt
   - `manual_resolution_only` head batch
   - 超出当前 idle timeout 的 `redelivery_window_ends_at`
   - 超出当前 idle timeout 的 `arm_pending_deadline` / `pause_deadline` / artifact GC deadline
7. 对这些“允许跨进程休眠”的长窗口状态：
   - 必须 durable 落盘
   - 下次任意入口拉起 daemon 时，必须先做 deterministic overdue sweep
   - sweep 完成前不得处理新的 submit / delivery 请求
8. 再加一层 idle timeout，避免短时间内频繁启停。

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
- `delivery_rpc_request_id` (CLI optional)
- `delivery_rpc_kind` (CLI optional)
  - `turn_start`
  - `turn_steer`
- `delivery_rpc_started_at` (CLI optional)
- `delivery_rpc_state` (CLI optional)
  - `pending_acceptance`
  - `accepted`
  - `rejected_before_accept`
  - `acceptance_unknown`
- `delivery_rpc_correlation_marker` (CLI optional)
- `delivery_turn_id` (optional)
- `managed_session_id` (CLI optional)
- `session_epoch` (CLI optional)
- `delivery_accepted_at` (CLI optional)
- `delivery_observation_state` (CLI optional)
  - `tracking`
  - `lost`
  - `expired`
- `delivery_observation_deadline` (CLI optional)
- `last_observed_turn_event` (CLI optional)
- `last_observed_turn_event_at` (CLI optional)
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
- `accept_pending`
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
- bridge 为 caller arm heartbeat 时，必须把以下 caller prompt token 写入 prompt：
  - `source_thread_id`
  - `batch_id`
  - `attempt_id`
  - `generation`
  - `snapshot_revision`
- `snapshot_path` 只属于 bridge-side internal locator，不得在 v1 caller prompt 中暴露，也不应被当成稳定的自动 caller 文件接口。
- `requires_artifact_read` 是 bridge-side gating metadata，不是 caller stale-wake token 的一部分。
- bridge 获取 ready entry 的来源合同必须是二选一：
  - `direct_file_read` 路径：`ready-threads.json` 的每个 ready entry 必须携带：
    - caller prompt token：`source_thread_id + batch_id + attempt_id + generation + snapshot_revision`
    - bridge-side internal locator：`snapshot_path`
    - gating metadata：`requires_artifact_read`
  - `helper_cli_read` 路径：`cbth desktop claim-next-ready ...` 必须一次性返回同样三类信息
- `caller_automation_id` 不要求由 ready entry 直接携带：
  - bridge 必须始终根据 `source_thread_id` 查询 desktop binding 来解析它
  - 如果 binding 缺失、不是 `bound`、或其 `read_transport + read_transport_generation` 与当前 installation state 不一致，则 bridge 不得继续 arm
- `cbth desktop claim-next-ready ...` 虽然名字里带 `claim`，但第一版语义必须是：
  - 纯读取 / peek helper
  - 不得创建 reservation
  - 不得移动 head batch
  - 不得递增 `delivery_attempt_count`
  - 不得改变当前 attempt / batch 的 durable 状态
- Desktop 的 ready 选择器不能依赖任意 SQL 顺序或 bridge 本地启发式：
  - daemon 必须维护一个 durable 的 canonical fair-ready order
  - `ready-threads.json` 与 `claim-next-ready` 都只是这个 fair-ready order 的只读视图
  - eligible ready thread 至少要求：
    - 当前 head batch 仍 open 且 `replay_policy=automatic`
    - 当前 thread 没有 unresolved 的同 thread safety item
      - 例如 `arm_pending`
      - 例如 binding 上仍有未 quiesced 的 `armed_generation`
      - 例如 overdue `pause_deadline`
      - 例如 binding `degraded`
      - 例如 `eligible_after > now`
    - 对 Desktop binding 来说，fresh arm 还要求上一代 `armed_generation` 已被证明 quiesced：
      - 没有 active `armed_generation`
      - 或当前 `armed_generation` 已设置 `armed_generation_quiesced_at`
      - 或 binding 已转入 `degraded` / `unbound` 并因此不再属于 eligible ready set
  - 在 eligible 集合内，daemon 必须按 durable `ready_cursor` 做 round-robin：
    - `ready_cursor` 至少按 target-kind / bridge 作用域维护
    - tie-break 至少稳定到 `ready_at` / `source_thread_id`
  - `claim-next-ready` 是 pure peek，因此不直接推进 `ready_cursor`
  - `ready_cursor` 只在以下事件发生后推进：
    - fresh delivery 被通道接受
      - Desktop: `note-arm` 成功
      - CLI: `turn/start` 或 `turn/steer` 被 server 接受
    - operator / daemon 显式 close 或 skip 当前 head batch
  - 如果某个 ready candidate 在 fresh delivery 被接受之前就失败：
    - daemon 必须 durable 地把它移出当前 immediate-eligible 集合
      - 例如写 `eligible_after`
      - 或让当前 attempt 进入 `abandoned` / `arm_pending`
    - 不得让它在下一轮继续无界占据 fair-ready order 的首位
- `claim-next-ready` 保持纯 peek 的同时，daemon 仍必须在 bridge 运行期内部提供一条不对外暴露的短租约：
  - `bridge_arm_lease`
  - 以 `(source_thread_id, attempt_id, generation)` 为 key
  - 只用于串行化 bridge 自己的 arm 流程
  - 它的 acquire/carry-forward 入口就是 `cbth desktop note-arm-pending ... --bridge-request-id <request_id>`
  - `note-arm-pending` 必须返回：
    - `bridge_arm_lease_id`
    - `bridge_arm_lease_deadline`
  - `bridge_request_id` 是每次 bridge wake / reconcile 流水线自己的唯一 owner token：
    - 同一个 `bridge_request_id` 的重试才允许 carry-forward 同一 lease
    - 不同 `bridge_request_id` 在 lease 仍有效时必须收到 `lease-held` / `busy`，不得拿到现有 lease
  - bridge 之后调用 `note-arm` 时，必须同时回传：
    - `bridge_request_id`
    - `bridge_arm_lease_id`
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
- `note-boundary-crossed` 的 mutation-side CAS 必须先校验调用方传入的完整 token：
  - `source_thread_id`
  - `batch_id`
  - `attempt_id`
  - `generation`
  - `expected_snapshot_revision`
- 任一 token 与当前 head batch / head attempt / materialized snapshot 不一致时，helper 必须在任何 mutation 前返回 stale/no-op。
- `note-boundary-crossed` 的 success 返回也必须回显：
  - `source_thread_id`
  - `batch_id`
  - `attempt_id`
  - `generation`
  - `snapshot_revision`
- caller 必须先比较 helper 返回值与 prompt 中的期望值是否完全一致；任一不一致都视为 stale wake，立即退出。
- 即使 token 全部匹配，caller 也只有在自己刚刚拿到一次 fresh `note-boundary-crossed` success 时才允许继续。
- fresh success 之前，`replay_policy=automatic` / `continuation_boundary_state=not_crossed` / binding `bound` 都属于 helper 的前置校验，而不是 post-success caller 再次判断的条件。
- 如果没有 fresh success，当前 wake 也必须只做 no-op / 诊断退出，不能继续消费这个 batch。
- 第一版 Desktop 路线的 head-batch 安全性不建立在 `automation_id` 必定可同步回填这一前提上。
- 第一版真正的安全锚点是：
  - `source_thread_id`
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
- 如果 binding repair / rebind 替换了 `caller_automation_id`，或无法证明旧 automation 已经 quiesced：
  - 后续自动续跑绝不能复用当前 attempt / generation
  - 必须先把当前 head batch 的自动 delivery 恢复路径切换到新的 fresh attempt / generation
  - 这样旧 automation 即使迟到，也只会命中旧 generation 并 stale-no-op

#### Attempt 迁移

```text
prepared -> arm_pending -> cooldown -> closed
prepared -> accept_pending -> cooldown -> closed
accept_pending -> prepared
prepared -> abandoned
prepared -> superseded
accept_pending -> abandoned
accept_pending -> superseded
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
- `accept_pending` 表示 CLI adapter 已经 durable 记录“准备调用 `turn/start` / `turn/steer`”，但还没有 durable 证明这次 side-effectful RPC 被接受或未被接受。
  - 进入 `accept_pending` 时必须写入 `delivery_rpc_request_id + delivery_rpc_kind + delivery_rpc_started_at + delivery_rpc_state=pending_acceptance + delivery_rpc_correlation_marker`
  - 只要 attempt 仍处于 `accept_pending`，该 batch 不得 automatic redelivery
  - 如果同一连续 event/current-state 面能证明 marker 被接入 exactly one caller turn，则补写 `delivery_turn_id` 并进入 `cooldown`
  - 如果同一连续 event/current-state 面能证明 RPC 未被接受，则设置 `delivery_rpc_state=rejected_before_accept`，清空 active acceptance 观察，回到 `prepared`
  - `accept_pending -> prepared` 只允许用于这类 proven-before-accept benign reject；它不得递增 `delivery_attempt_count`，也不得更新 `last_delivery_attempt_at` 为成功投递时间
  - 下一次 retry 必须生成新的 `delivery_rpc_request_id + delivery_rpc_correlation_marker`；旧 rejected request 只能作为 audit evidence 保留
  - 如果 acceptance 结果无法证明，attempt 必须进入 `abandoned`，head batch 必须进入 `manual_resolution_only`
- `arm_pending` 表示 bridge 已经 durable 记录“准备为该 attempt arm caller heartbeat”，但这次 arm 还没有被 `note-arm` 最终确认。
  - 只要 attempt 仍处于 `arm_pending`，它就不再是新的 ready head
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
- 第一版默认 shipping 配置中，`turn/steer` 应视为关闭；只有在 capability probe 与 active-turn 分类能力都成熟后，才作为 feature flag 打开。CLI 侧未来合同见 [CLI_ACTIVE_TURN_STEER_DESIGN.md](CLI_ACTIVE_TURN_STEER_DESIGN.md)。
- 这里的 active-turn 分类不能只看 batch 自己的 delivery policy，还必须同时有一份可机判的当前 turn 风险视图，至少包括：
  - `active_turn_kind`
  - `active_turn_requires_approval`
  - `active_turn_requires_network`
  - `active_turn_requires_write_access`
  - `active_turn_risk_class`
- 只有当当前 active turn 本身也被分类为 `read_only_low_risk` 时，CLI adapter 才允许把只读 batch steer 进去；否则一律回退到 idle-only delivery。

### CLI `delivery_turn_id` 观察连续性

- 在调用 `turn/start` / `turn/steer` 前，CLI adapter 必须先 durable 写入 `accept_pending` barrier：
  - `delivery_rpc_request_id`
  - `delivery_rpc_kind`
  - `delivery_rpc_started_at`
  - `delivery_rpc_state=pending_acceptance`
  - `delivery_rpc_correlation_marker`
- `delivery_rpc_correlation_marker` 必须随 RPC 一起进入 app-server 可观察输入；协议如果没有 opaque idempotency key，就把短 marker 放进 continuation prompt。
- 如果 RPC response 丢失，只有在同一 `managed_session_id + session_epoch` 的连续 event/current-state 面能正向证明 marker 被接入 exactly one caller turn 时，adapter 才允许补写 `delivery_turn_id`。
- 如果无法证明 accepted，也无法证明未 accepted，当前 attempt 必须 fail-closed 到 `abandoned + manual_resolution_only`，不得自动重发。
- 一旦某个 CLI attempt 已经被 `turn/start` 或 `turn/steer` 接受，并 durable 记录了 `delivery_turn_id`，后续安全收口就建立在“持续观察同一个 managed session / app-server 实例的 turn 事件流”之上。
- 因此，accepted CLI attempt 还必须 durable 绑定：
  - `managed_session_id`
  - `session_epoch`
  - `delivery_rpc_request_id`
  - `delivery_turn_id`
  - `delivery_observation_deadline`
- 其中：
  - `managed_session_id` 是 daemon 为一条逻辑 managed CLI session 分配的稳定 durable id
  - `session_epoch` 是该 managed session 当前“可证明连续的 shared app-server event stream”的单调递增序号
  - `session_epoch` 在 daemon 首次拉起该 shared `app-server` 时初始化为 `1`
  - 只要 daemon 还能证明自己仍附着在同一个未重建的 shared `app-server` 实例上，短暂 websocket 重连不递增
  - 只要 app-server 进程重启、managed session 被重建，或 daemon 恢复后无法证明事件流连续性，就必须递增
- 如果 daemon 只是 websocket 短暂断开、但能够重新附着到同一个 `managed_session_id + session_epoch`，则允许继续等待对应的 `turn/completed`。
- `delivery_observation_deadline` 是 accepted CLI attempt 的硬边界：
  - 在 `turn/start` / `turn/steer` 被接受时写入
  - 由 `delivery_accepted_at + max_turn_observation_window` 推导
  - `max_turn_observation_window` 必须显式大于当前 daemon `idle timeout`
  - 只要 deadline 未到，未收口的 `delivery_turn_id` 就属于“近端 observation work”，会阻止 daemon 退出
- 如果在 `delivery_observation_deadline` 到期前仍未观察到可信的 `turn/completed`，则不得静默退出：
  - 当前 attempt 必须收敛到 `abandoned`
  - `delivery_observation_state=expired`
  - 当前 head batch durable 进入 `replay_policy=manual_resolution_only`
  - 之后 daemon 才允许按正常 idle 规则退出
- 因此，CLI 自动 close `close_reason=delivered` 还必须额外满足：
  - 当前 attempt 仍是 head delivery
  - `delivery_observation_state=tracking`
  - `replay_policy=automatic`
  - `now <= delivery_observation_deadline`
- 一旦 attempt 已 `abandoned`、`delivery_observation_state != tracking`、或 batch 已进入 `replay_policy=manual_resolution_only`：
  - 迟到的 `turn/completed` 只能作为 operator/debug 证据保留
  - 不得再自动把 batch 关闭成 `close_reason=delivered`
- `last_observed_turn_event` / `last_observed_turn_event_at` 的 canonical 合同必须是：
  - 只记录当前 `delivery_turn_id` 上真实观察到的事件
  - accepted 时初始化为 `null`
  - 后续只能由同一 `delivery_turn_id` 的观察更新
  - v1 fixed canonical enum 为：
    - `turn_started`
    - `turn_completed`
    - `turn_failed`
    - `turn_interrupted`
    - `turn_replaced`
  - CLI minimum capability probe 还必须证明：
    - 能观察 `turn_started`
    - 能观察 `turn_completed`
    - 能观察 accepted-turn 的负终态：`turn_failed` / `turn_interrupted` / `turn_replaced`
  - 缺少这组观察面时，CLI detached auto-continuation 必须 fail-closed
- 只要 `managed_session_id` 或 `session_epoch` 的连续性无法再证明，当前 head batch 就不得自动 replay：
  - 当前 attempt 收敛到 `abandoned`
  - 当前 head batch durable 进入 `replay_policy=manual_resolution_only`
  - 之后只允许 operator close，或等待 `redelivery_window_ends_at` 到期自动关闭
- 一旦某个 accepted CLI attempt 对应的 `delivery_turn_id` 在之后出现失败、中断、替换，或其他无法被证明为“同一 turn 正常完成”的终局结果：
  - 当前 attempt 同样必须收敛到 `abandoned`
  - 当前 head batch durable 进入 `replay_policy=manual_resolution_only`
  - 只有 pre-accept 的 benign race / non-steerable reject 才允许自动重试
- 第一版不允许在“accepted turn 的观察连续性已经丢失”后，靠重新投递来猜测原 turn 是否已经产生副作用。

### CLI managed session profile

- detached auto-delivery 不只依赖 batch 自身的 delivery policy，还依赖 managed session 自身的 durable effective risk profile 和 startup permission snapshot。
- 每条 managed session 都必须 durable 记录：
  - `session_allows_approval`
  - `session_allows_network`
  - `session_allows_write_access`
  - `startup_session_allows_approval`
  - `startup_session_allows_network`
  - `startup_session_allows_write_access`
  - `startup_permission_snapshot_json`
  - `last_permission_snapshot_json`
  - `permission_snapshot_revision`
- `--session-allows-approval` / `--session-allows-network` / `--session-allows-write-access` 接受 `auto`、`true`、`false`，默认 `auto`：
  - explicit `true` / `false` 是调用方给出的 bootstrap profile
  - `auto` 必须从 `thread/resume.approvalPolicy` 与 `thread/resume.sandbox` 取可信 snapshot，无法解析时 fail-closed
  - 第一次可信 auto snapshot pin 为 startup upper bound
  - 当前 proof invalidation / resync 只清理 epoch-local current proof，不清理同一前台 managed session 的 startup upper bound
  - 每次自动 `turn/start` 前重新读取 current snapshot，并逐维计算 `effective_allows = startup_allows && current_allows`
  - 当前收紧时按当前更紧权限投递；当前放宽时仍受 startup 限制；混合变化逐维取更紧值
- drift 必须写 stderr warning 与 audit record，包含 startup/current/effective、方向和 changed dimensions
- `turn/start` request 必须显式携带 effective 权限对应的 pinned `approvalPolicy`，避免 durable 记录和真实 turn 权限不一致。Codex 0.129 形态下，permission snapshot 优先解析 tagged `managed` / `disabled` / `external` `permissionProfile`，legacy `sandbox` 缺少 `access` / `readOnlyAccess` 时按 full legacy read 兼容校验；如果 stable built-in current `activePermissionProfile` 能精确表示 effective sandbox cap，active selection 的 network/write 布尔值与 effective cap 一致，且 active selection 与 current canonical `permissionProfile` body 双向等价，则优先发送 `permissions: { type: "profile", id, modifications }` 且不发送 `sandboxPolicy`，并允许 canonical profile 保留 legacy sandbox 无法表达的 deny carve-outs；如果 effective cap 是 startup-tighter、mixed synthetic、来自 mutable user-defined profile id、current active selection 与 canonical body 不一致、或当前 active profile 无法无损表达，则 fallback 到 pinned legacy `sandboxPolicy`。legacy fallback 只发送可表示的 `type`、`networkAccess`、`writableRoots` 与 workspace exclude flags，且只有 canonical profile 可安全降级为 legacy sandbox、effective read access 为 full read 时才允许；restricted-read `access` / `readOnlyAccess` shape 只进入解析、收紧计算和 drift/audit，如果需要 legacy fallback 保留 restricted read scope 则 fail-closed。
- workspace writable root 收紧必须先做安全规范化；含 parent-directory component 的 root 直接 fail-closed，避免路径解析后落到 startup cap 之外。
- auto-pinned session 的 proof invalidation 只清掉 epoch-local current proof 并保留 startup cap；strict-safe 投递在 current permission snapshot 重新刷新前不得把旧 `session_allows_*` 风险布尔值当作可信证明。
- 默认 `auto` reattach 不应把 fail-closed 初始 false 作为固定 profile 来匹配 durable `session_allows_*`；显式 `true` / `false` 仍按固定 profile 处理，profile drift、manual batch blocker、active attempt blocker 继续拒绝 reattach。
- 只有当前 effective 三者都为 `false` 时，CLI strict-safe detached auto-delivery 才允许开启；`trusted-all` 可以绕过该 gate，但 auto snapshot / drift 记录仍然适用。
- `attach-or-create` 发现 requested bootstrap profile 与 durable effective profile 不一致时，不得原地改写：
  - 如果旧 session 仍有 active foreground client、未收口 accepted attempt、或其他未解决 delivery work，则必须 fail-closed 为 `session_profile_mismatch`
  - 只有在旧 session 已满足 retirement 条件后，daemon 才允许把它标为 `retired`，并创建一个带新 profile 的新 `managed_session_id`
- `parked` 是 managed session 的统一非 live 停放态，不是 accepted-path 专属状态：
  - live part 已结束
  - 不再要求 automatic delivery 或 accepted-turn live observation
  - 但仍有 unresolved manual batch 等待 operator close / `manual_resolution_expired` auto-close
  - 这个 manual batch 可以来自 accepted attempt fail-closed，也可以来自 pre-accept manual/operator path
- 只要 session 仍处于 `parked` 且 unresolved manual batch 未终态：
  - attach/reuse 必须 fail-closed 为 `session_pending_manual_resolution`
  - daemon 不得创建第二个指向同一 `bound_thread_id` 的 non-retired replacement session
- 任一字段为 `true` 或 `unknown` 时：
  - batch 即使本身是 `delivery_read_only=true`
  - 也必须回落到 manual/operator path

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

### Managed session 关键字段 (CLI)

- `managed_session_id`
- `bound_thread_id`
- `session_epoch`
- `session_state`
  - `live`
  - `detached`
  - `parked`
  - `stale`
  - `retired`
- `session_allows_approval`
- `session_allows_network`
- `session_allows_write_access`
- `startup_session_allows_approval`
- `startup_session_allows_network`
- `startup_session_allows_write_access`
- `startup_permission_snapshot_json`
- `last_permission_snapshot_json`
- `permission_snapshot_revision`

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
- `boundary_snapshot_revision`
- `boundary_recovery_envelope_ref`
- `boundary_recovery_envelope_bytes`
- `boundary_recovery_retention_until`
- `boundary_recovery_operator_pin_until`
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
- `handoff_recorded`
  - Desktop `note-boundary-crossed` fresh success 已把 inline handoff payload / recovery envelope durable 记录下来
  - 这个 reason 释放 FIFO，但不证明 caller assistant 文本已经对用户可见

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
  - `cbth desktop note-arm-pending ... --bridge-request-id <request_id>` 先提供一个 compare-and-swap durable barrier：
    - 只有当 `(source_thread_id, attempt_id, generation)` 仍指向当前 head attempt
    - 且当前 durable 状态仍是 `prepared`
    - 才允许执行唯一一次 `prepared -> arm_pending`
    - 并记录：
      - `bridge_request_id`
      - `bridge_arm_lease_id`
      - `bridge_arm_lease_deadline`
      - `arm_pending_since`
      - `arm_pending_deadline`
    - 如果同一 attempt 已经是 `arm_pending`：
      - 只有当前 durable `bridge_request_id` 与调用方相同，才允许返回 already-pending / idempotent success
      - 且必须返回同一个 `bridge_arm_lease_id`
      - 如果 durable `bridge_request_id` 不同，则必须返回 `lease-held` / `busy`，不得泄露现有 lease
    - 如果 attempt 已过期、已 superseded 或已离开 head，则必须 stale/no-op
  - compare-and-swap：
    - 只有当 `(source_thread_id, attempt_id, generation)` 仍指向当前 head attempt
    - 且当前 durable 状态仍是 `arm_pending`
    - 才允许执行唯一一次 `arm_pending -> cooldown`
    - 且调用方显式回传的：
      - `bridge_request_id`
      - `bridge_arm_lease_id`
      - 都与 durable 记录一致
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

## Boundary recovery envelope retention

`boundary_recovery_envelope` 是 Desktop `handoff_recorded` 的恢复证据，不是 caller prompt 的普通 payload 文件。

最小 schema：

- `batch_id`
- `source_thread_id`
- `attempt_id`
- `generation`
- `snapshot_revision`
- `closed_at`
- `close_reason=handoff_recorded`
- `inline_payload_summary` 或 `inline_payload_ref`
- `artifact_manifest_refs`
- `artifact_ids`
- `created_at`
- `retention_until`

约束：

- `note-boundary-crossed` fresh success 必须在关闭 batch 前原子写入 `boundary_recovery_envelope_ref`。
- 小 handoff 可以 inline 保存，但必须受 `max_boundary_recovery_inline_bytes` 限制。
- 超过 inline 上限时，payload 必须进入 managed artifact / recovery object，envelope 只保存受控引用。
- `boundary_recovery_retention_until` 至少为以下三者最大值：
  - `closed_at + post_close_ttl`
  - 所有关联 artifact 的 `retention_until`
  - `boundary_recovery_operator_pin_until`
- 在 `now < boundary_recovery_retention_until` 前，不得 GC envelope 或其引用的 recovery object。
- `cbth batch inspect --batch-id ...` 是读取该 envelope 的稳定 operator surface。

### Batch 终态语义

第一版把以下情况都视为 batch 终态：

- 可信 delivery channel 报告该 batch 已送达，对应 `close_reason=delivered`
- operator / user 显式关闭或取消该 batch，对应 `operator_confirmed_delivery`、`operator_closed_unconfirmed` 或 `cancelled`
- redelivery window 结束且不再继续自动重投，对应 `redelivery_window_exhausted` 或 `manual_resolution_expired`
- batch 被显式 supersede 或达到尝试上限，对应 `superseded` 或 `max_attempts_exhausted`
- Desktop v1 `note-boundary-crossed` fresh success 已 durable 记录 handoff，对应 `handoff_recorded`

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
  - `cooldown_until` 到期后，如果该 batch 仍是 head、`replay_policy=automatic`、`now < redelivery_window_ends_at`、且 `delivery_attempt_count < max_delivery_attempts` -> 重新进入 eligible ready / fresh-arm gate
  - 对 Desktop 来说，重新进入 fresh-arm gate 仍必须满足同一 binding 的上一代 `armed_generation` 已 quiesced；否则只能等待 pause/reconcile，不能直接创建新 attempt 并再次 arm
  - 如果 `delivery_attempt_count >= max_delivery_attempts`，该 batch 必须自动进入：
    - `close_reason=max_attempts_exhausted`
    - `closed`
- 只有在 operator 关闭、batch 被 superseded、可信 delivery channel 明确成功、Desktop handoff 已 durable 记录、redelivery window 结束、或 `max_attempts_exhausted` 时，batch 才进入 `closed`
- caller 的“明确 crossing 已发生”在第一版里应实现为一个窄 helper：

```text
cbth desktop note-boundary-crossed --source-thread-id <thread_id> --batch-id <batch_id> --attempt-id <attempt_id> --generation <generation> --expected-snapshot-revision <revision> --json
```

- `note-boundary-crossed` 是 Desktop 第一版必需的 gated continuation helper：
  - caller 在真正看到 batch payload / artifact 内容之前，必须先调用它
  - 它的成功返回同时代表：
    - boundary crossing 已 durable 记录
    - 当前 batch 已切到 `crossed_unacknowledged + replay_policy=manual_resolution_only`
    - caller 已获得当前 v1 supported handoff 所需的 inline continuation payload / summary
  - 这个 helper 必须发生在任何后续 assistant 输出之前
  - 只有它成功后，caller 才允许进入 post-boundary handoff phase
  - v1 不再把“系统层面阻止 post-boundary 普通工具”当成架构保证：
    - supported automatic path 只覆盖这次 handoff phase 的 inline text continuation
    - 任何偏离这条路径的后续动作都属于 unsupported implementation drift，而不是 core delivery safety contract 的一部分
  - 它也必须具备 compare-and-swap / stale-no-op 语义：
    - 只有当当前 head batch 仍匹配 `(source_thread_id, batch_id, attempt_id, generation)`
    - 且当前 materialized snapshot revision 仍等于 `expected_snapshot_revision`
    - 且当前 attempt 已经 durable 进入 `cooldown`
    - 且 binding 上的 `armed_generation` 仍等于当前 `generation`
    - 且 binding 仍处于 `bound`
    - 且 binding 镜像的 `read_transport_generation` 仍等于 installation state 当前 generation
    - 且 binding 镜像的 `validation_fingerprint` 仍与 installation state 一致
    - 且 installation state 当前仍满足：
      - `read_transport_capability=validated`
      - `writeback_capability=validated`
    - 且 batch 仍然 open
    - 且 `continuation_boundary_state=not_crossed`
    - 才允许唯一一次把状态推进到 `crossed_unacknowledged`
    - 一旦已经 `crossed_unacknowledged`，自动 caller path 的重复调用必须返回 `already-crossed` / stale-no-op，而不是再次授权 continuation
  - 它一旦成功，当前 head batch 必须 durable 进入：
    - `continuation_boundary_state=crossed_unacknowledged`
    - `replay_policy=manual_resolution_only`
    - `closed`
    - `close_reason=handoff_recorded`
    - 并同时 durable 保存一份 operator-only `boundary_recovery_envelope`
  - `handoff_recorded` 的语义是：
    - inline handoff payload / recovery envelope 已由 `cbth` durable 记录
    - FIFO 可以立即前进到该 thread 的下一个 batch
    - 但它不证明 caller assistant 文本已经成功展示给用户
  - v1 automatic caller path 的成功返回必须携带：
    - inline continuation payload / summary
  - `requires_artifact_read=true` 的 batch 不允许走这条 automatic caller path：
    - 它们在 bridge 侧就必须被留给 manual/operator follow-up
  - `boundary_recovery_envelope` 也必须足以支持 operator recovery：
    - 小 payload：直接 durable 保存可恢复的 inline payload / summary
    - 大 artifact：至少 durable 保存 manifest，并允许 operator recovery 按需签发短寿命 `artifact_recovery_lease_id + artifact_recovery_lease_deadline`
    - `cbth desktop read-artifact --artifact-read-lease-id ...` 的参数名是通用读取 lease 槽位；Desktop v1 recovery 传入的值就是 `artifact_recovery_lease_id`
    - `note-boundary-crossed` fresh success 会关闭 batch，但不得删除 `boundary_recovery_envelope`
    - `boundary_recovery_envelope` 必须至少保留到 batch/artifact retention contract 允许 GC
    - 短寿命 artifact recovery lease 只允许在 deadline 到期、lease rotation、artifact GC、或 operator 明确 revoke 后失效
    - stale wake 或其他本地调用方即使拿到旧 `artifact_id`，也不得绕过 continuation boundary 继续读大 artifact
  - 这样即使 caller 在之后崩溃，`cbth` 也不会再自动 redelivery 这个可能已产生副作用的 batch
- 第一版不再尝试在 continuation boundary 之后自动把 batch 收口到 “已送达”：
  - 无论后续是纯文本回复，还是工具 / 行动步骤
  - 只要已经成功执行 `note-boundary-crossed`
  - 当前 batch 就以 `close_reason=handoff_recorded` 关闭并释放 FIFO
  - lost post-boundary response 只能通过 operator recovery 查看 `boundary_recovery_envelope`
- 在 v1 自动 caller path 里，caller 不得在 `note-boundary-crossed` 之前直接读取 per-thread envelope / artifact payload。
- 如果 `note-boundary-crossed` 返回 error / stale-no-op：
  - caller 必须立即停止，不得继续输出或产生工具副作用
  - helper 必须把非 success outcome 至少区分为：
    - `transient_not_ready`
    - `stale_or_superseded`
    - `already_crossed_or_handoff_recorded`
    - `binding_or_capability_invalid`
    - `unknown_after_helper_failure`
  - 只有 `transient_not_ready` 且 batch 仍 open、`replay_policy=automatic`、未过 redelivery window 时，bridge 后续才允许 automatic redelivery
  - `already_crossed_or_handoff_recorded` 必须导向 `cbth batch inspect --batch-id ...` operator recovery，不得自动重放 continuation
  - `binding_or_capability_invalid` 必须 fail closed 到 degraded/manual operator path，直到 repair 产生 fresh attempt / generation
  - `unknown_after_helper_failure` 必须先做 durable reconciliation；只有正向证明没有发生 crossing 后才允许重新分类成 `transient_not_ready`，否则必须 fail closed 到 manual/operator path
- 如果 `note-boundary-crossed` 已经成功过一次，而 caller 没拿到那次 response：
  - 自动 caller path 不得再次 continuation
  - 后续只能走 operator recovery：
    - `cbth batch inspect --batch-id ...` 必须暴露 `boundary_recovery_envelope`
    - 以及必要的 artifact manifest / diagnostic refs
    - 对大 artifact 还必须返回 operator-only `artifact_recovery_lease_id + artifact_recovery_lease_deadline`（或等价 re-lease surface）
  - v1 选择 safety over liveness：不允许靠下一次 heartbeat 自动重放同一 delivery
- 如果 caller 已经越过 continuation boundary 但还没成功得到 `note-boundary-crossed` 的 success 返回：
  - 这属于违背第一版安全合同的实现错误
  - 正确实现必须保证“先 `note-boundary-crossed` 成功返回，再继续”
- 一旦 `note-boundary-crossed` 成功，当前 batch 的 v1 默认值就是：
  - `closed`
  - `close_reason=handoff_recorded`
  - `replay_policy=manual_resolution_only`
  - 保留 `boundary_recovery_envelope` 供 operator 按 batch id 检索
  - 未来版本如果要支持“真正已展示/已消费”的自动 close reason，必须单独引入 post-output / post-side-effect observation contract

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
cbth desktop installation-state repair
cbth job submit
cbth job complete
cbth job fail
cbth job cancel
cbth job query
cbth batch close-head
cbth batch inspect-head
cbth batch inspect --batch-id <batch_id>
cbth desktop binding repair
cbth desktop binding unbind
```

说明：

- `cbth cli run` 是 CLI 集成入口。
- CLI v1 的 bootstrap 有两个显式入口：
  - existing-thread mode：`cbth cli run --bind-thread-id <thread_id>`
    - 启动时显式建立 `bound_thread_id`
  - fresh-thread mode：`cbth cli run --new-thread`
    - daemon 先启动 pending shared app-server
    - foreground Codex 在该 app-server 上创建 brand-new thread
    - `cbth` 监听 `thread/started`，把 foreground 返回的真实 thread id durable 绑定为新的 `bound_thread_id`
- v1 不提供 late-bind stable surface。
- 也不把 `managed_session_id` 暴露成需要外部回填 thread id 的 bootstrap 契约。
- 如果调用方既拿不到 caller `thread_id`，也不能通过 foreground-created `thread/started` 建立 fresh thread 绑定：
  - 该前台会话只能视为探索性 remote TUI
  - 不进入 v1 的 managed-session auto-continuation 合同
- `cbth desktop ...` 预留给 Desktop bootstrap / helper。
- `cbth desktop bridge-preflight ...` 是 Desktop bridge 每轮 wake 的必经窄 helper：
  - 它按需拉起 daemon
  - 执行 deterministic overdue sweep / GC / auto-close / binding reconcile
  - 原子发布本轮 snapshot manifest，并让 manifest 指向的 revision-specific `ready-threads.json` / `arm-pending-bindings.json` / `pause-due-bindings.json` 全部绑定同一个 `snapshot_revision`
  - 返回本轮 `snapshot_manifest_path + snapshot_revision / generation`
  - 如果它失败，本轮 bridge 不得读取旧 snapshot 继续 arm
- `cbth desktop installation-state repair ...` 是 installation-wide Desktop transport / capability state 的稳定 operator 面：
  - 它才允许切换 `read_transport`
  - 也是唯一允许写 installation-wide capability 结论的路径
  - 发生实际 state 变化时必须原子更新 installation state，并递增 `read_transport_generation`
  - 同一参数重复执行必须是 no-op
  - 成功输出还必须至少回显：
    - `validation_fingerprint`
    - `validated_at`
  - 如果 `read_transport` 发生变化而 capability 状态没有由同一次 repair 显式提供：
    - 必须把 `read_transport_capability`
    - `artifact_read_capability`
    - `writeback_capability`
    - 全部原子重置为 `unknown`
    - 并清空 `validated_at`
  - 同时把所有镜像不再匹配的 bindings 推到 `degraded`
- `cbth job ...` 是第一版对外稳定的任务提交与状态回报面。
- `cbth batch close-head` / `inspect-head`、`cbth batch inspect --batch-id ...` 与 `cbth desktop binding repair` / `unbind` 也必须作为第一版稳定的 operator recovery 面存在。
- 对 CLI fail-closed 路径，`cbth batch inspect-head ...` 的最小可观测面还必须至少包含：
  - `delivery_turn_id`
  - `managed_session_id`
  - `session_epoch`
  - `delivery_observation_state`
  - `delivery_observation_deadline`
  - `delivery_accepted_at`
  - `last_observed_turn_event`
  - `last_observed_turn_event_at`
- `cbth batch inspect-head ...` 只用于查看当前仍为 head 的 open/manual batch 或 CLI fail-closed 证据：
  - 它不得被 Desktop lost post-boundary response recovery 依赖
  - 因为 `handoff_recorded` 会立即关闭 batch 并释放 FIFO，当前 head 可能已经是后续 batch
- `cbth batch inspect --batch-id ...` 必须能对已 `handoff_recorded` 的历史 batch 回显 recovery surface：
  - `boundary_recovery_envelope`
  - 对大 artifact 则再回显 operator-only `artifact_recovery_lease_id + artifact_recovery_lease_deadline`（或等价 re-lease surface）
  - 这保证 `note-boundary-crossed` fresh success 释放 FIFO 后，lost post-boundary response 仍可人工恢复
- `cbth desktop binding repair ...` 的成功输出必须至少回显：
  - `read_transport_capability`
  - `artifact_read_capability`
  - `writeback_capability`
  - 这些字段只是 installation state 的当前镜像，不得由 binding repair 单独写入
  - 如果 repair 替换了 `caller_automation_id`，还必须明确回显：
    - 旧 automation 是否已证明 quiesced / deleted
    - 是否已强制当前自动 delivery 切换到新的 fresh attempt / generation
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
  - 当 continuation 需要额外 operator/manual artifact recovery 时为 `true`
- CLI adapter 可以在共享核心 canonical batch 字段之上，本地派生一个临时的 `steer_candidate` / `steer_eligible` 判定：
  - 但它不是 shared-core durable schema 的一部分
  - 也不作为跨端 canonical delivery policy 字段冻结

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
cbth desktop binding repair --source-thread-id <thread_id> --caller-automation-id <automation_id> --json
cbth desktop installation-state repair --read-transport <transport> [--read-transport-capability <state>] [--artifact-read-capability <state>] [--writeback-capability <state>] --json
cbth batch close-head --source-thread-id <thread_id> --reason operator_closed_unconfirmed --json
cbth batch close-head --source-thread-id <thread_id> --reason operator_confirmed_delivery --json
cbth batch inspect-head --source-thread-id <thread_id> --json
cbth batch inspect --batch-id <batch_id> --json
cbth desktop relay marker issue --bridge-thread-id <bridge_thread_id> --kind arm-pending|arm-accepted --source-thread-id <thread_id> --attempt-id <attempt_id> --generation <generation> --bridge-request-id <request_id> --json
cbth desktop relay consume-transcript --rollout-path <rollout_jsonl> --marker <marker> --json
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
    - 或 transcript relay consumer 对等执行 `note-arm-pending` / `note-arm` CAS
  - operator / future-expansion artifact helper：
    - `cbth desktop read-artifact ...`
- 第一版无论 bridge 读取传输怎么选，都必须先能运行：
  - `cbth desktop bridge-preflight ...`
- 第一版如果 bridge 侧不用 `direct_file_read`，则 bridge-side fallback helper 链路还必须是完整可用的：
  - `cbth desktop note-arm-pending ...`
  - `cbth desktop list-arm-pending ...`
  - `cbth desktop list-pause-due ...`
  - `cbth desktop claim-next-ready ...`
  - `cbth desktop note-arm ...`
  - `cbth desktop note-boundary-crossed ...`
- caller 侧 automatic continuation 不通过 `direct_file_read` 直接拿 payload：
  - 必须先通过 `cbth desktop note-boundary-crossed ...` 成功返回 gated inline continuation payload / summary
  - v1 supported automatic path 不再包含 post-boundary `read-artifact` 或普通 Codex tools
- 但 bridge-side helper fallback 目前只能算条件性方案：
  - 它仍然要求 heartbeat turn 能无审批执行窄 `cbth desktop ...` 命令
  - 在这个前提被实证前，不应把它表述成已验证的默认主路径
- 而 `cbth desktop read-artifact ...` 不是 bridge-side fallback：
  - 它保留给 operator/manual recovery，或 future-expansion 的大 artifact continuation
  - 当前 v1 automatic caller path 不依赖它
- 如果未来重新启用它，`cbth desktop read-artifact ...` 仍必须提供 chunked payload 协议，而不是返回一个需要再次 file-read 的路径。
- 其中 bridge 的 overdue-binding cleanup 也必须有对应只读输入面：
  - `~/.cbth/inbox/snapshots/<snapshot_revision>/pause-due-bindings.json`
  - 或 `cbth desktop list-pause-due --bridge-thread-id <thread_id> --json`
- 其中 `cbth desktop claim-next-ready ...` 必须一次性返回 bridge 写 prompt 所需的整组 token：
  - `source_thread_id`
  - `batch_id`
  - `attempt_id`
  - `generation`
  - `snapshot_revision`
  - `snapshot_path`
  - `requires_artifact_read`
- bridge 必须再根据 `source_thread_id` 查询 binding，解析当前唯一允许更新的 `caller_automation_id`
- 当前首选路径是：
  - bridge heartbeat 先读取 `current-snapshot.json`，再读取其中 locator 指向的 revision-specific `ready-threads.json`
- caller heartbeat 先调用 `note-boundary-crossed`，并只在 success 返回后消费 inline continuation payload / summary
- 如果 `direct_file_read` 不能满足无审批读取约束，则 Desktop 设计要么切回经过单独验证的 `helper_cli_read`，要么继续保留为候选方案；不能直接把未验证的 helper 执行前提当成已经成立。
- Desktop 自动续跑只对已完成 binding 的 thread 生效；未绑定 thread 不得被 bridge 自动 arm。

## 为什么第一版只做 CLI 脚本

- 最简单稳定。
- 对 shell、Python、GitHub Actions、本地守护脚本都足够友好。
- 不会过早冻结 socket / Web / plugin 协议。
- 便于保持核心系统独立，不把任务适配方式绑死在单一语言或运行时里。

## 与端侧文档的关系

- CLI 侧如何接 Codex TUI，见：
  - `docs/design/CLI_SHARED_APP_SERVER_SIDECAR_DESIGN.md`
- Desktop 侧如何接 heartbeat，见：
  - `docs/design/DESKTOP_BACKGROUND_TASK_BRIDGE_DESIGN.md`

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
