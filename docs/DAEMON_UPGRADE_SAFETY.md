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
