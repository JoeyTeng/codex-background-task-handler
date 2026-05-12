# Daemon Upgrade Safety

本文记录 `0.2.0` daemon upgrade 的安全边界。目标是让新版 `cbth` 在遇到旧 daemon、旧 app-server、旧 foreground sessions 和 live jobs 时默认不破坏现有工作。

## Goals

- `cbth new`、`resume` 和 mutating commands 遇到 incompatible legacy daemon 时默认 fail closed 或并存，不再隐式 `stop`。
- 新旧 daemon 可以同时存在：legacy `run/cbth.sock` 继续服务旧资源，新版 binary 可在 `run/daemons/<generation>/cbth.sock` 上启动 generation daemon。
- 每个 daemon 只监督自己安全拥有的 jobs/tasks，避免新版 startup recovery 标记或 signal 正被旧 daemon 管理的 work。
- `0.2.0` 才启用 handoff/quiesce 协议。低版本 daemon 只允许并存，不迁移资源。

## PR Slices

1. PR1: `daemon ensure` 默认遇到 incompatible daemon 返回结构化错误；只有 `--replace-incompatible` 才 stop-and-replace。
2. PR2: 引入 generation daemon socket，并让 mutating commands 选择 compatible daemon。旧 daemon 继续服务旧 app-server 和 foreground sessions。
3. PR3: 增加 `daemon-handoff-v1` capability、`binary_version` gate 和 quiesce 状态机骨架，但不真实接管资源。
4. PR4: app-server handoff。旧 daemon 导出 owned resource，新 daemon adopt 后继续 refresh lease/proof，foreground websocket port 不变。
5. PR5: live jobs drain。旧 daemon quiescing 后拒绝新 task，但继续监督已有 task 到 terminal；active jobs 清零后自动退出。

Release PR 单独 bump `0.2.0`，同步 changelog、README install examples 和 self-update/version parsing 验证。

## PR2 Coexistence Contract

- Default daemon 使用 legacy socket `run/cbth.sock`，只拥有 `supervisor_daemon_generation IS NULL` 的 task/job recovery scope。
- Generation daemon 使用 `run/daemons/<generation>/cbth.sock`，基础 owner 是当前 binary version，也就是 `CARGO_PKG_VERSION`。
- `daemon ensure` 在 legacy default daemon incompatible 且未显式 `--replace-incompatible` 时启动或复用 generation daemon；legacy daemon 不会被 stop。
- `--replace-incompatible` 仍表示替换 legacy default daemon，但 default daemon startup recovery 不会扫描 generation-owned tasks。
- `daemon status --all` 和 `cli app-servers --all-daemons` 枚举 default socket 与 generation socket，供 operator 观察并存状态。

## Recovery Ownership

Recovery owner 集合由 daemon 类型决定：

- Default daemon: unowned jobs/tasks。
- Generation daemon: current generation jobs/tasks。
- Both: 额外包含 stale generation tasks，但前提是该 generation 的 socket endpoint 不存在或没有 listener。

这避免两个风险：

- Active generation daemon 仍在运行时，新 daemon 不会 signal 或 terminalize 它正在监督的 process group。
- 旧 generation daemon 已死且 socket endpoint 不存在时，新 daemon 会回收它留下的 queued/running rows 和 stored process groups，避免 orphaned process group 或永久 nonterminal task。

Pending jobs without tasks are not migrated in PR2。它们仍属于后续 jobs drain / handoff work；PR2 只保证 supervised task recovery 不误伤 active daemon，也不永久跳过 dead generation 的 task process group。

## Handoff Minimum

`0.2.0` 是第一个支持 daemon handoff 的 minimum version。PR3 之后只有同时满足以下条件才允许 handoff：

- peer daemon exposes `daemon-handoff-v1`
- peer daemon reports `binary_version >= 0.2.0`
- peer daemon enters quiesce mode before exporting owned resources

低版本 daemon 不参与 handoff，只按 PR2 规则并存和 drain。

## PR3 Handoff Protocol Skeleton

PR3 只增加协议骨架和状态机，不迁移真实 app-server 或 live jobs。

Daemon ping/status 新增以下协议面：

- `daemon.binary_version`: 当前 daemon binary 的 package version。
- `daemon.quiescing`: daemon 是否已经进入 handoff quiesce mode。
- `daemon.handoff_minimum_binary_version`: 固定为 `0.2.0`。
- `daemon.handoff_eligible`: 当前 daemon binary 是否达到 handoff minimum。
- `capabilities` 包含 `daemon-handoff-v1`，用于声明 daemon 理解 quiesce/handoff 协议。

Handoff initiator 的 gate 是 fail-closed 的：

- protocol 必须是当前 daemon protocol。
- peer 必须暴露 `daemon-handoff-v1`。
- peer 必须报告可解析的 `daemon.binary_version`。
- peer `binary_version` 必须 `>=0.2.0`。

只有 gate 通过时，新 binary 才会向 incompatible legacy default daemon 发送 hidden `handoff_quiesce` request。低版本 daemon、缺 capability daemon、缺 version daemon、protocol 不匹配 daemon 都不会收到 quiesce request；它们继续按 PR2 并存路径处理。

`handoff_quiesce` request carries the expected peer `pid` and `binary_version` from the gated ping response. The receiver validates both before setting quiescing state, so a default socket replacement race cannot accidentally quiesce a different daemon.

Quiesce mode 的 PR3 语义：

- `handoff_quiesce` idempotently sets in-memory quiescing state and returns daemon info with `quiescing=true`。
- Quiescing daemon refuses new work: `dispatch`、`task_run`、CLI app-server reserve/ensure/probe、thread-start/bootstrap/promote。
- Quiescing daemon keeps control/maintenance paths available: `ping`、`status`、`stop`、app-server lease refresh/release/stop、thread-start abort、`task_cancel`。
- Handoff quiesce fencing must not cover long app-server spawn or `thread/start` RPC work. It only protects registry transitions; if quiesce wins before a CLI app-server or bootstrap candidate is registered, the candidate is rejected and stopped, and if quiesce observes an already registered bootstrap, `daemon ensure` falls back to coexistence.
- PR3 不导出 app-server registry，不 adopt app-server，不迁移或 drain live jobs。PR4/PR5 才实现这些资源面的行为。

当 incompatible legacy default daemon 通过 handoff gate 时，`daemon ensure` 会先 quiesce legacy daemon，再启动或复用 generation daemon，并在 ensure response 中标注 `legacy_daemon_quiesced` 和 `legacy_handoff_quiesce`。如果 eligible daemon 未确认 quiesce，ensure fail closed，避免后续 PR 在未 quiesce 的 peer 上做资源接管。

## PR4 App-Server Handoff

PR4 接管 daemon-owned CLI app-server，但不改变 foreground Codex 已连接的 websocket listener。

Registry ownership 扩展为三类：

- `owned`: 当前 daemon 直接 spawn 的 app-server child，继续拥有 child handle、stdout/stderr drain worker、lease expiry 和 proof cleanup。
- `adopted`: 新 daemon 从 quiesced legacy daemon 接管的 app-server。它没有 child handle，但记录 `pid`、`pid_identity`、原 daemon socket、thread、epoch、lease、ws url，并用 pid identity fencing 做 refresh/stop/reap。
- `handed_off`: legacy daemon 已把 app-server 交给新 daemon。legacy daemon 保留短期 redirect/drain 记录，不再 stop 或 invalidate 该 app-server；旧 foreground wrapper 下次 lease refresh 会收到 `handoff_daemon_socket_path` 并切到新 daemon。

Handoff flow:

1. 新 CLI 对 handoff-eligible legacy default daemon 发送 fenced `handoff_quiesce`。
2. Legacy daemon 进入 quiesce，先 fence 已经进入的 app-server mutation，再导出仍存活的 `owned` app-server：`managed_session_id`、`bound_thread_id`、`session_epoch`、`lease_id`、remaining lease、`ws url`、`pid`、`pid_identity`、`started_at`。如果 export 失败，daemon 必须回滚本次 quiesce，避免 legacy daemon 留在拒绝新 work 的半切换状态；active foreground/thread-start bootstrap 会让本次 handoff fail closed，`daemon ensure` 继续启动或复用 generation daemon，只并存不迁移。
3. 新 CLI 启动或复用 generation daemon，并发送 hidden `handoff_cli_app_servers_adopt`。
4. 新 daemon 先验证整批 export 的 loopback url、active lease、pid identity、leader 进程仍存活且非 zombie、process group 和 registry conflict，再 all-or-nothing 注册 `adopted` app-server；refresh 返回同一个 `pid` / `url`。
5. 新 CLI 对 legacy daemon 发送 hidden `handoff_cli_app_servers_release`；legacy daemon 先验证整批 export 仍匹配当前 registry，再 all-or-nothing 将对应 registry entry 转为 `handed_off`，后续 matching refresh 只返回 redirect，不延长 legacy ownership。
6. 如果 release 失败，新 CLI 会先向 legacy daemon 查询 release status：确认 legacy 仍是 `owned` 或 export 后已 `missing` 时才请求 generation daemon 撤销刚 adopt 的 entries，然后 fenced `handoff_unquiesce` legacy daemon 并降级为 generation coexistence；如果 legacy 已经是同一 generation socket 的 `handed_off`，则把丢失的 release response 视为已提交；如果 legacy 已经 `handed_off` 给别的 generation，则撤销本 generation 的 losing adoption；状态不明时 fail closed 而不盲目撤销。撤销只移除 generation registry，不 signal app-server，因为 legacy daemon 仍是 owner 或已经负责了 pre-release stop/proof cleanup。Adopt 成功后，release/status/rollback/unquiesce 都使用独立的最小超时预算，不再因为 startup deadline 边缘耗尽而跳过 rollback。
7. 如果 generation adopt 因 stale export 或 transport error 失败，新 CLI 先请求 generation daemon 撤销可能已注册的 adopted entries，再对 legacy daemon 发送 fenced `handoff_unquiesce`；两步都成功时，`daemon ensure` 降级为 generation coexistence，不迁移 app-server。任一步状态不明时 fail closed，避免 legacy quiescing 与 generation adopted split-brain。

Fail-closed rules:

- 缺 pid identity、pid identity mismatch、process group 不存在、url 非 loopback、lease 已过期或 registry entry 已变化时，handoff 不继续。
- Adopt preflight 必须拒绝已经退出或 zombie 的 app-server leader；即使 process group 仍可 signal，也不能把没有 live websocket listener 的 export 注册为 `adopted`。
- Legacy export must also protect the owned registry entry until release by extending or freezing its lease for the handoff floor; otherwise the legacy reaper could kill a near-expired app-server after export but before release.
- 一旦 generation daemon 已经 adopt，后续 release/rollback 不能再直接继承可能接近 0 的 startup budget；否则失败路径会留下 generation `adopted`、legacy `owned` 的 split-brain registry。
- New `adopted` entries must get a lease floor long enough for old foreground wrappers to discover the legacy redirect and refresh against the generation daemon; short remaining legacy leases are not inherited verbatim.
- 旧 wrapper 收到 `handoff_daemon_socket_path` redirect 后必须立即向新 daemon 刷新同一 lease，不能等下一次周期性 refresh，否则短剩余 lease 可能在切换间隙过期。
- 旧 wrapper 的 passive adapter daemon-routed writes 如果先打到 quiesced legacy daemon，必须主动用 app-server lease refresh 发现 redirect 并立即重试到 generation daemon，不能等周期性 lease refresher 更新共享 endpoint。
- 旧 wrapper 对 legacy `handed_off` entry 发送 stop 时，也必须收到可跟随的 `handoff_daemon_socket_path`，并立即把 stop 转发到新 daemon，避免用户退出后只能等待 adopted lease 过期。
- Legacy `handed_off` release is idempotent only for the same generation socket. A different generation socket must fail and roll back its losing `adopted` entries instead of retargeting the redirect.
- Legacy `handed_off` entry 的 stop/shutdown 不 kill app-server，也不 invalidate proof；new `adopted` daemon 才负责后续 lease refresh、proof cleanup 和 stop。`adopted` cleanup 需要同时处理 leader 仍匹配 pid identity 和 leader 已退出但 process group 仍有 live members 两种情况；leader 已退出且没有 live group member 时必须视为 stopped，不能继续 refresh adopted lease。
- Legacy `handed_off` entry 即使旧 lease 到期，也必须保留 child handle / drain owner，直到 child 已可 reap 后才从 registry 移除；这样 legacy daemon 不会在仍作为 parent 时丢掉 reaping responsibility。
- Old daemon 仍可能作为短期 stdout/stderr drain 和 redirect shim 存活到旧 lease 窗口结束；PR4 不承诺迁移 stdout/stderr pipe ownership。

Non-goals:

- PR4 不迁移 live jobs，也不迁移 worker 内存状态。
- PR4 不承诺 app-server stdout/stderr drain 无损迁移；只承诺 foreground websocket url/pid 不变且不会因为 legacy daemon handoff 被 reset。
