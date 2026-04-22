# Shared Core Architecture

## 目标

- 不修改上游 `codex` 仓库。
- 用一套共享核心同时支撑 CLI 与 Desktop 两条集成路径。
- 尽量保持部署简单：单个 Rust binary、本地状态、本地 IPC。
- 长时间任务的生命周期独立于单次 Codex 前台交互。
- 第一版只暴露稳定的 CLI 接口，不提前冻结 socket / Web / plugin API。

## 术语

- 下文用 `cbth` 作为示例 binary 名称，占位表示未来的主 Rust binary。
- `daemon` 指本项目自己的本地后台进程，不是系统级 `launchd` / `systemd` 服务。
- `integration adapter` 指 CLI 或 Desktop 这类接到 Codex 的薄接入层。
- `job` 指一个被后台系统追踪的长时间任务。

## 已敲定的取舍

### 1. 单 binary，多入口

- 第一版实现目标是一个主 Rust binary。
- CLI 集成只是这个 binary 的一个入口。
- Desktop 集成也是这个 binary 的另一个入口。
- 共享的 job store、状态机、daemon、sidecar runtime 都归在同一个核心里。

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

## 共享组件

### 1. `daemon runtime`

职责：

- 持有 SQLite store。
- 维护 job 状态机。
- 管理 leases、claim、重试、去重。
- 作为 CLI / Desktop 接入层与长任务 runtime 的共同协调者。

### 2. `store`

第一版推荐：

- 本地 SQLite。

原因：

- 单机、单用户模型足够。
- 便于原子 claim / compare-and-swap。
- 不要求额外安装 Redis / Postgres。
- CLI 和 Desktop 都能复用同一套状态。

### 3. `local IPC`

职责：

- 让 CLI 子命令与 daemon 通信。
- 在 daemon 未启动时，支持 auto-start / reconnect。

第一版定位：

- 这是内部实现细节，不是对外承诺的稳定公共 API。
- 对外只承诺 CLI 子命令的行为，不承诺 socket 协议长期兼容。

### 4. `job orchestrator`

职责：

- 创建 job。
- 迁移状态。
- 保存结果引用与摘要。
- 对 ready / claimed / consumed 做原子操作。
- 处理超时 lease 与重试。

### 5. `Codex integration adapters`

这是端侧专属层，但依赖同一套共享核心：

- CLI adapter：
  - shared `app-server`
  - `codex --remote`
  - idle 时 `thread/resume + turn/start`
  - active 时 `turn/steer`
- Desktop adapter：
  - bridge heartbeat thread
  - caller heartbeat
  - `automation_update` arm / cleanup

### 6. `long-run task runners`

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
- `result_summary`
- `result_ref`
- `dedupe_key`
- `lease_owner`
- `lease_expires_at`
- `created_at`
- `updated_at`
- `completed_at`

### Job 状态

- `running`
- `ready`
- `armed`
- `claimed`
- `consumed`
- `failed`
- `cancelled`

说明：

- `armed` 主要给 Desktop heartbeat 路线用。
- CLI 路线不一定需要显式 `armed`，但共享状态机保留该状态不会有坏处。

## 第一版稳定外部接口

第一版只保证 CLI 命令是稳定外部接口。

### 守则

- 外部系统不要直接改 SQLite。
- 外部系统不要直接连接内部 socket。
- 外部系统不要依赖未冻结的 daemon 内部协议。
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
cbth job list-ready
cbth job claim-ready
cbth job mark-armed
cbth job requeue
cbth job mark-consumed
```

说明：

- `cbth cli run` 是 CLI 集成入口。
- `cbth desktop ...` 预留给 Desktop bootstrap / helper。
- `cbth job ...` 是共享 job 控制面。

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

说明：

- 第一版优先用 `--result-file` 传结果引用，而不是内联巨大 JSON。
- `result-file` 可以是 JSON、纯文本、日志摘录或其它产物文件。
- 核心系统第一版只需要稳定保存引用与摘要，不强制解析所有业务字段。

### 回报失败

失败时，外部脚本调用：

```text
cbth job fail --job-id <job_id> --reason <text> --json
```

### 查询与消费

给 Codex bridge / caller 或人工排障使用：

```text
cbth job list-ready --json
cbth job claim-ready <job_id> --json
cbth job mark-armed <job_id> <token>
cbth job requeue <job_id>
cbth job mark-consumed <job_id>
cbth job query <job_id> --json
```

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
- job store
- CLI 公共接口
- 外部长任务接入边界

## 第一版不做的事

- 不做系统级服务安装。
- 不做公开 Web API。
- 不做公开 socket API。
- 不做动态插件加载框架。
- 不要求外部任务直接嵌入核心进程执行。
