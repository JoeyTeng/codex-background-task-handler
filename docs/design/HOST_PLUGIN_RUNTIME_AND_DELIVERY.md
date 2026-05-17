# Host Plugin Runtime And Generic Delivery

## 目标

`cbth` 需要支持 host-level integration plugins：它们是本机常驻的外部集成进程，不是 Codex runtime plugins，也不是上游 `codex` 内部模块。

本设计的第一目标是把 Webex connector 整合为一个由 `cbth` 托管的外部插件，同时把可复用能力沉到 `cbth`：

- 插件生命周期、登录自启动、崩溃恢复和升级编排。
- 插件与 `cbth` core 的 versioned persistent RPC。
- daemon-owned Codex app-server lease、task supervision、job/batch/artifact 和 delivery proof。
- generic delivery core 与明确的 delivery driver capability。

Webex connector 只保留 Webex 专属语义：Webex websocket / REST、Control Space / Session Space / Data Space、message forwarding、approval card、远端 event replay 和本地 session reconcile。

## 非目标

- 不把插件实现成 Rust dynamic library，也不承诺稳定 Rust crate ABI。
- 不把 host-level plugins 与 Codex 自己的 plugins / MCP / app connector 混为一类。
- v1 不用 CLI 作为插件常驻通信路径；CLI 只保留 operator surface。
- v1 不直接采用 gRPC。协议设计保持 transport-agnostic，后续可以增加 gRPC transport。
- v1 generic delivery 不承诺 raw Codex CLI 或 Codex Desktop 已可投递成功。

## 术语

- `cbth service`: 登录自启动的用户级 supervisor，负责插件 registry、进程监督、release pointer、升级编排和 plugin RPC broker。
- `cbth daemon`: 现有 same-user daemon，继续拥有 SQLite store、task supervision、daemon-owned app-servers、job/batch/artifact、maintenance sweep 和 handoff。
- `host-level plugin`: 由 `cbth service` 托管的外部进程，例如 Webex connector。
- `plugin home`: `~/.cbth/plugins/<plugin_name>/` 下的插件私有 config/state/logs 目录。
- `delivery core`: cbth 统一的 job/batch/attempt/audit 状态机。
- `delivery driver`: 把 delivery core 的 batch 交给某个 Codex surface 的具体机制。

## 架构边界

`cbth service` 与 `cbth daemon` 分开：

- `service` 应尽量长驻，处理 login autostart、plugin process lifecycle、release management 和 RPC routing。
- `daemon` 继续按需启动或由 service 保持可用，负责所有核心本地资源和 mutating state。
- plugin 不直接写 cbth SQLite，也不直接 spawn production `codex app-server`。
- plugin 通过 RPC 请求 cbth 分配 app-server lease、提交 delivery、运行 task 或检查 target capability。

这个拆分避免插件崩溃/升级污染核心 daemon ownership，也避免 daemon 被插件 runtime policy 迫使承担太多 host supervisor 职责。

## Plugin RPC v1

第一版使用 Unix domain socket 上的 persistent JSON-RPC-like frame。理由：

- 本机 same-user 通信不需要 HTTP/2 才能达到足够性能。
- 避免为 v1 引入 tonic / protobuf codegen / async transport 的复杂度。
- 长连接可以消除 CLI fork / parse overhead。
- 方法、版本、capability 和错误模型可以先稳定，未来再增加 gRPC transport。

### Connection Handshake

插件启动后主动连接 `cbth service` 并发送：

```text
plugin.hello
```

请求携带：

- `plugin_name`
- `plugin_instance_id`
- `plugin_release_id`
- `protocol_versions`
- `capabilities`
- `plugin_home`
- `pid`

`cbth service` 返回选定 protocol version、service capability、daemon endpoint hint 和 policy。

### Plugin -> cbth Methods

v1 推荐方法族：

- `app_server.ensure`
- `app_server.refresh`
- `app_server.stop`
- `delivery.enqueue`
- `delivery.inspect`
- `delivery.manualize`
- `task.run`
- `task.inspect`
- `task.cancel`
- `target.capabilities.inspect`
- `plugin.health.update`

所有 mutating 方法必须有 idempotency key 或可机判 replay fence。RPC 错误必须区分：

- unsupported protocol
- missing capability
- stale lease
- policy blocked
- target unavailable
- transient daemon unavailable
- internal error

#### `app_server.*` C3 Contract

C3 只实现 plugin-scoped app-server lease RPC，不实现 generic delivery、service install/manage、release manager 或 Webex 专属行为。

`app_server.ensure` 请求：

- `managed_session_id`
- `bound_thread_id`
- `session_epoch`
- `codex_binary`
- `lease_id`
- optional `lease_ttl_seconds`（1..=300；省略时 service 使用默认短租约）

`lease_id` 是 plugin-visible replay fence。`cbth service` 必须把它与 authenticated plugin connection identity（`plugin_name` + `plugin_instance_id`）组合成 daemon-visible scoped lease id，避免不同插件或不同 plugin instance 互相刷新/停止 lease。同一连接上重复 `app_server.ensure` 只有在 `managed_session_id`、`bound_thread_id`、`session_epoch` 与首次请求一致时才是 idempotent replay；目标不同的重复 lease 必须 fail closed。若同一当前 plugin instance 通过多个 RPC socket 重放同一 lease，service 可以共享同一个 daemon lease，但必须按连接记录 holder，避免某个 socket 的 cleanup/stop 误停仍被其他 socket 持有的 app-server。

`app_server.refresh` 请求：

- `managed_session_id`
- `lease_id`
- optional `lease_ttl_seconds`（1..=300；省略时 service 使用默认短租约）

`app_server.stop` 请求：

- `managed_session_id`
- `lease_id`

service 只接受已通过 `plugin.hello` 且仍匹配当前 supervisor process identity 的连接调用这些方法。service 负责：

- 通过 daemon ensure 获取 compatible daemon endpoint，并保留 generation handoff 语义。
- 复用 daemon-owned `cli_app_server_*` lease machinery，而不是让 plugin 直接 spawn production app-server。
- 在 daemon 返回 handoff endpoint 时跟随到新 daemon，并更新本连接的 lease endpoint。
- 在 plugin connection 结束时 release 本连接仍持有的 app-server lease；只有最后一个 connection holder 释放后才 best-effort stop daemon app-server，daemon TTL reaper 仍是 cleanup fallback。

### cbth -> Plugin Methods

`cbth service` 可以对 active plugin instance 发 lifecycle RPC：

- `plugin.health_check`
- `plugin.quiesce`
- `plugin.drain`
- `plugin.shutdown`
- `plugin.handoff_export` (optional)
- `plugin.handoff_import` (optional)
- `plugin.unquiesce`

插件未声明 handoff capability 时，service 仍可通过 quiesce + drain + replay/reconcile 做保守升级。

## Plugin Lifecycle

### Install And Autostart

`cbth service` 后续应提供：

```text
cbth service install
cbth service uninstall
cbth service status
cbth plugin enable <name>
cbth plugin disable <name>
cbth plugin status <name>
```

macOS v1 用 `LaunchAgent` 登录自启动。Linux 后续可加 user `systemd`。`LaunchAgent` 应启动 `cbth service run`，而不是直接启动 Webex worker。

### Crash Recovery

service 持久化 plugin instance 状态：

- release id
- current executable path
- plugin home
- last healthy timestamp
- crash count
- restart backoff
- last error

插件必须将业务 cursor / mirror 写入 plugin home，不能依赖进程内存。Webex plugin 重启后必须先 replay/reconcile，再恢复事件处理。

### Upgrade

保守升级流程：

1. 下载并校验新 release，写入 release dir。
2. 启动 shadow plugin instance，只做 handshake / warmup，不处理外部事件。
3. 对旧 instance 发送 `plugin.quiesce`，停止接收新外部 work 或只 ack 不处理。
4. 旧 instance `plugin.drain` 当前 handler，并持久化 cursor / mirror。
5. 如果双方支持 handoff，执行 `handoff_export` / `handoff_import`。
6. 新 instance 在 fenced pre-active mode 做只读 reconcile / health check，不推进外部 cursor，不提交 delivery，不发送 Webex REST side effect。
7. 健康检查通过后，service 原子切换 active lease、promote release pointer，并允许新 instance 处理 Webex listener。
8. 任一步失败则 rollback：停止 shadow，旧 instance `unquiesce`。因为 promote 前 shadow 不能拥有 cursor 或 side effect，rollback 不需要 handoff-back。

这里的“不断线”定义为业务不中断。Webex websocket 允许短暂重连，但必须通过 event id、cursor 和 replay 避免消息丢失或重复处理。若后续要允许新旧 instance 在 promote 前做有限事件预取，必须先增加 cursor fencing、side-effect fence、handoff-back 和 rollback reconciliation 合同。

## Generic Delivery Core

Generic delivery 应拆成 core + drivers，而不是把所有 Codex surface 混成一个成功语义。

### Core Contract

`delivery.enqueue` 输入：

- `source_thread_id`
- `summary`
- inline payload or artifact reference
- delivery policy
- idempotency key
- optional plugin metadata

core 负责：

- 创建/复用 job。
- 创建/复用 head batch。
- 管理 delivery attempts、audit、manual resolution、artifact retention。
- 根据 target capability 选择 driver。

### Driver Capability

v1 supported driver：

- `codex_app_server`: daemon-owned loopback app-server，使用 `turn/start`，并做 accepted-turn observation / same-epoch reconcile。

v1 staged / experimental driver：

- `desktop_heartbeat_relay`: 当前只支持 ready materialization、arm marker emission、scanner writeback 到 `cooldown` 等阶段性状态。

不作为 v1 driver：

- raw Codex CLI。普通 CLI/TUI 没有稳定 target、idle proof、accepted turn observation 合同；只有 `cbth cli run` / managed shared app-server 路径间接使用 `codex_app_server` driver。
- Codex Desktop delivered success。近期 Desktop PR 只验证到 transcript relay scanner 和 ready/arm workstream；caller wake、`note-boundary-crossed`、artifact-read policy 尚未闭环。

### Desktop Boundary

Desktop driver 需要独立 capability gates：

- `desktop.ready_materialization`
- `desktop.arm_writeback`
- `desktop.caller_wake`
- `desktop.boundary_crossed`
- `desktop.artifact_read`

在 `caller_wake` 和 `boundary_crossed` 未 validated 前，Desktop driver 不得把 batch 标为 delivered。它只能报告 staged states，例如 `ready_materialized`、`arm_pending`、`cooldown` 或 `manual_resolution_only`。

## Webex Plugin Responsibilities

Webex plugin 使用 cbth shared substrate，但保留自己的产品语义：

- Webex websocket sidecar / REST client。
- Webex Control Space、Session Space、Data Space。
- Webex message -> Codex `turn/start` forwarding。
- Codex approval request -> Webex Adaptive Card。
- Webex Data Space replay / snapshot。
- 本地 Codex thread reconcile。
- Webex room membership、archive、purge、diagnostics。

普通用户消息 forwarding 暂时由 Webex plugin 直接连 cbth-managed app-server 实现。后台任务结果、外部异步事件和长任务完成通知应走 `delivery.enqueue`，复用 cbth delivery proof。

## State Ownership

Webex Data Space 是远端索引和审计，不是唯一业务真相。

本机可执行权威来自：

- Codex local thread 是否 readable / resumable。
- cbth managed app-server / delivery target capability。
- cbth local job/batch/task/artifact state。
- plugin local mirror 与 cursor。

Webex session 必须记录 installation identity。远端存在但本机 thread 缺失或不可读时，应进入 degraded / missing-local-thread 状态，并从默认 active list 过滤，只在 diagnose / cleanup 视图展示。

## PR Dependency Graph

建议分三波推进。

### Wave 1: Boundary

| PR | Repo | Content | Depends On |
| --- | --- | --- | --- |
| C1 | cbth | Plugin RPC protocol skeleton: UDS persistent connection, `plugin.hello`, version/capability negotiation, error model | none |
| C2 | cbth | `cbth service run`, plugin registry/supervisor, manifest, plugin home, health, restart backoff, logs, `cbth plugin status` | C1 |
| W1 | webex-connector | State authority split: installation id, local mirror, remote session vs local thread reconcile, missing/unreadable filtering | none |
| W2 | webex-connector | Plugin packaging: manifest, RPC client, `doctor`, legacy config compatibility, standalone compatibility | C1, W1 |

### Wave 2: cbth Substrate Reuse

| PR | Repo | Content | Depends On |
| --- | --- | --- | --- |
| C3 | cbth | Plugin-scoped app-server lease RPC: `app_server.ensure/refresh/stop`, daemon-owned app-server reuse, generation handoff and lease cleanup | C1 |
| W3 | webex-connector | Replace direct `codex app-server stdio://` spawn with cbth-managed loopback app-server; keep Webex forwarding/approval in plugin | C3, W2 |
| C4 | cbth | Generic delivery core v1: `delivery.enqueue/inspect/manualize`, job/batch idempotency, supported `codex_app_server` driver only | C3 |
| W4 | webex-connector | Route background results / async notifications through `delivery.enqueue`; keep normal Webex user messages on direct app-server forwarding | C4, W3 |

### Wave 3: Production Lifecycle

| PR | Repo | Content | Depends On |
| --- | --- | --- | --- |
| C5 | cbth | Service install/manage: macOS LaunchAgent install/uninstall/status, future Linux user systemd shape | C2 |
| C6 | cbth | Plugin release manager: prepare shadow, quiesce old, drain, promote, rollback, optional handoff hooks | C2 |
| W5 | webex-connector | Lifecycle hooks: quiesce, drain, shutdown; durable cursor/session mirror and replay gap recovery | C6, W2 |
| W6 | webex-connector | Optional handoff: export/import Webex cursor, in-flight handler state, sidecar restart metadata | W5 |
| C7/W7 | both | Opt-in live E2E: real Webex token, cbth service, plugin upgrade smoke, delivery smoke | all previous |

并行规则：

- C1/W1 可以并行。
- C2 可以基于 C1 draft / fake protocol 草拟 service 与 supervisor 结构，但真实集成和 merge 必须等 C1 协议合同落地。
- W2 可以基于 C1 draft + fake cbth RPC server 开始，但真实协议接入必须等 C1。
- W3 必须等 C3。
- W4 必须等 C4。
- W5/W6 不能在 C6 前宣称 production upgrade / handoff 可用。

## Review And Delivery Gates

每个 PR 一个 code-owning agent。该 agent 可以开自己的 review/test subagents，但默认只读；需要 code-edit worker 时必须限定 disjoint write set。

每个实现 PR 必须：

- 固定 `base_sha..head_sha` review range。
- 运行 repo-relevant tests。
- 运行 clear-context GPT-5.5 fast-mode comprehensive local review。
- 处理本地 review findings 后再推送更新。
- 处理 GitHub remote review comments。
- 对 RPC / daemon / delivery / upgrade PR，最终再跑 whole-range review。

设计 PR 本身以文档校验、diff check 和 focused review 为主要 gate。
