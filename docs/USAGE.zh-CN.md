# 使用指南

语言：[British English (en-GB)](USAGE.en-GB.md) | [简体中文 (zh-CN)](USAGE.zh-CN.md)

本文说明 `cbth` CLI 的常规本地 dogfood 流程。它假设你使用专用 macOS 或 Linux 单用户工作站，并且本机 `codex` CLI 已登录。

## 安装或升级

当前 release assets 支持 Linux x86_64 glibc 和 macOS arm64。项目不发布 Intel macOS release asset；从 Rosetta shell 启动的 Apple Silicon host 会由 installer 映射到 macOS arm64 asset。

安装最新 GitHub Release：

```bash
curl -fsSL https://raw.githubusercontent.com/JoeyTeng/codex-background-task-handler/HEAD/scripts/install.sh | sh
command -v cbth
cbth doctor cli
```

安装指定版本或目录：

```bash
CBTH_VERSION=v0.2.1 CBTH_INSTALL_DIR="$HOME/.local/bin" \
  sh scripts/install.sh
```

检查并升级：

```bash
cbth self update --check
cbth self update -i
cbth self update --yes
```

`cbth self update --yes` 会下载匹配的 release binary 和 `.sha256`，校验 checksum，在当前 executable 旁边写入临时文件，然后原子替换。它不使用 `sudo`；如果当前 executable 不可写，请重新安装到用户可写目录。

## 就绪检查

fresh install 后先运行部署就绪检查：

```bash
cbth doctor cli
```

`cbth doctor cli` 可能创建或修复私有 `~/.cbth` 状态目录、打开 SQLite store、启动同用户 daemon，并短暂启动 loopback `codex app-server` 来验证 listener 解析。它不会发送 model request，也不会创建 Codex turn。

测试非默认 Codex CLI binary 时使用 `--codex-bin <path>`：

```bash
cbth doctor cli --codex-bin /path/to/codex
```

隔离测试可以设置 `CBTH_HOME` 或传入 `--home <path>`：

```bash
CBTH_HOME=/tmp/cbth-dogfood cbth doctor cli
```

## 受管理 Codex Sessions

启动一个受管理的新 Codex thread：

```bash
cbth new \
  --session-allows-approval false \
  --session-allows-network false \
  --session-allows-write-access false \
  --auto-delivery-policy trusted-all \
  --model gpt-5.5
```

`cbth new` 会通过受管理的 shared app-server 路径启动前台 Codex。它会把绑定的 thread id 打印到 stderr：

```text
cbth: bound thread id: <thread-id>
```

通过相同受管理路径 resume 已有 thread：

```bash
cbth resume <thread-id> -- --model gpt-5.5
```

需要更低层控制时使用：

```bash
cbth cli run --bind-thread-id <thread-id> -- --model gpt-5.5
cbth cli run --new-thread -- --model gpt-5.5
```

默认情况下，managed session 是被动模式，只记录 current-state proof。自动投递需要显式传入 `--auto-delivery-policy trusted-all`。

## 运行后台 Task

为绑定 thread 运行受托管本地命令：

```bash
cbth task run \
  --source-thread-id <thread-id> \
  --summary "run a slow local check" \
  --delivery-read-only true \
  --delivery-requires-approval false \
  --delivery-requires-network false \
  --delivery-requires-write-access false \
  --cwd "$PWD" \
  --timeout-seconds 3600 \
  -- cargo test
```

`cbth task run` 会创建持久 task 和关联 job，在独立 process group 中启动命令，立即返回 task/job identifiers，并由 daemon 在命令退出时完成或失败该 job。

查看和管理 task：

```bash
cbth task list --source-thread-id <thread-id>
cbth task inspect --task-id <task-id>
cbth task cancel --task-id <task-id>
```

stdout 和 stderr 会 spool 到 `~/.cbth/tasks/<task-id>/`，并带有 bounded tails 和 spool caps。如果无法证明自动投递，请看[操作恢复](OPERATOR_RECOVERY.zh-CN.md)。

## 投递策略

默认投递策略是 fail-closed。如果省略 delivery flags，`cbth` 会把 batch 视为非 read-only，且需要 approval、network 和 write access。

`trusted-all` 是一个明确的宽权限 dogfood escape hatch。它会绕过 batch policy、artifact-read 和 managed-session risk-profile gates，但仍要求 open head batch、剩余 budget、匹配的 thread/session/epoch，以及 fresh idle proof。

当前 CLI 自动路径在 durable idle proof 后使用 `turn/start`。`turn/steer`、active-turn injection、rollout-only delivery proof 和 foreground thread retargeting 都不在自动投递路径内。

## 查看状态

查看当前 head batch 和 audit trail：

```bash
cbth batch inspect-head --source-thread-id <thread-id>
cbth audit list --source-thread-id <thread-id> --limit 50
```

查看 managed sessions：

```bash
cbth cli session list --bound-thread-id <thread-id>
cbth cli session inspect --managed-session-id <managed-session-id>
```

查看 daemon-owned Codex app-servers：

```bash
cbth cli app-servers
cbth cli app-servers --human
cbth cli app-servers --latest-generation --human
```

`cbth cli app-servers` 默认检查所有已知 daemon generation，最新 generation 排在最前；`--all-daemons` 是这个默认行为的兼容别名。只想看最新 generation 并保留单 endpoint 输出形态时，使用 `--latest-generation`；如果没有 generation socket，`--latest-generation` 会 fallback 到默认 daemon。只有 legacy default daemon 的部署会保留既有 single-endpoint JSON shape。当 app-server 支持 `thread/loaded/list` 时，JSON output 可能包含可选字段 `loaded_non_bound_codex_sessions`，`--human` / `-H` 也可能打印 `loaded non-bound codex sessions`。这只是 best-effort 的 loaded-thread 诊断信息；它不表示 foreground/current session，也不会改变投递路由。API 不支持、返回错误或列表为空时，该字段会被省略。

Daemon control commands：

```bash
cbth daemon ensure
cbth daemon status
cbth daemon ping
cbth daemon stop
```

Mutating job、batch、task 和 maintenance commands 默认通过同用户 daemon 执行。Read-only inspection commands 直接读取本地 store。

## Desktop Bridge Commands

Desktop bridge commands 当前只是 operator/helper surfaces，不是已启用的 Desktop 自动投递路径。

```bash
cbth desktop installation-state --json
cbth desktop binding repair --source-thread-id <thread-id> --caller-automation-id <automation-id> --json
cbth desktop bridge-preflight --bridge-thread-id <thread-id> --json
cbth desktop read-snapshot --bridge-thread-id <thread-id> --json
cbth desktop list-arm-pending --bridge-thread-id <thread-id> --json
cbth desktop list-pause-due --bridge-thread-id <thread-id> --json
cbth desktop claim-next-ready --bridge-thread-id <thread-id> --json
```

当前 Desktop 边界见[设计概览](DESIGN_OVERVIEW.zh-CN.md)。
