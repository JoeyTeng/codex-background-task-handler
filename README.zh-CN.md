# codex-background-task-handler

语言：[British English (en-GB)](README.md) | [简体中文 (zh-CN)](README.zh-CN.md)

`codex-background-task-handler` 提供 `cbth` companion CLI，用于在不修改上游 `codex` 仓库的前提下，让 Codex 周边的长时间后台工作更可控。它是一个本地、同用户的辅助工具，可以启动受管理的 Codex CLI session、托管长时间运行的 shell task，并在本地安全检查允许时把 task 结果投递回绑定的 Codex thread。

## 当前状态

当前项目适合在专用 macOS 或 Linux 单用户工作站上本地 dogfood。

- CLI dogfood v1 是主要支持路径。它可以启动受管理的 `codex` 前台 session、托管本地 task、维护持久 daemon 状态，并在显式选择 `trusted-all` 时尝试自动投递。
- Desktop bridge 仍处于基础能力和验证阶段，尚不是已启用的 Desktop 自动投递路径。
- 实现有意放在上游 `codex` 仓库之外。本仓库包含 companion Rust binary、集成实验，以及外部控制面的文档。

## 安装

当前 release assets 支持 Linux x86_64 glibc 和 macOS arm64。项目不发布 Intel macOS release asset；从 Rosetta shell 启动的 Apple Silicon host 会由 installer 映射到 macOS arm64 asset。

安装最新 GitHub Release：

```bash
curl -fsSL https://raw.githubusercontent.com/JoeyTeng/codex-background-task-handler/HEAD/scripts/install.sh | sh
command -v cbth
cbth doctor cli
```

安装指定版本或目录：

```bash
CBTH_VERSION=v0.2.2 CBTH_INSTALL_DIR="$HOME/.local/bin" \
  sh scripts/install.sh
```

升级已安装 binary：

```bash
cbth self update --check
cbth self update -i
cbth self update --yes
```

从 checkout 做本地开发安装：

```bash
cargo install --path .
cbth doctor cli
```

## 快速开始

启动一个受管理的新 Codex thread：

```bash
cbth new \
  --session-allows-approval false \
  --session-allows-network false \
  --session-allows-write-access false \
  --auto-delivery-policy trusted-all \
  --model gpt-5.5
```

从 stderr 复制 thread id：

```text
cbth: bound thread id: <thread-id>
```

在另一个 shell 里为该 thread 运行受托管 task：

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

查看 task 和投递状态：

```bash
cbth task list --source-thread-id <thread-id>
cbth batch inspect-head --source-thread-id <thread-id>
cbth audit list --source-thread-id <thread-id> --limit 50
```

## 文档

- [使用指南](docs/USAGE.zh-CN.md)：安装、受管理 session、task supervision、daemon 状态和 operator 命令。
- [设计概览](docs/DESIGN_OVERVIEW.zh-CN.md)：高层设计、信任边界和演进方向。
- [操作恢复](docs/OPERATOR_RECOVERY.zh-CN.md)：`manual_resolution_only`、task logs、sessions 和维护命令的手动恢复。
- [开发指南](docs/DEVELOPMENT.zh-CN.md)：本地开发安装、确定性测试、hooks 和仓库约定。
- [真实 E2E 指南](docs/LIVE_E2E.zh-CN.md)：需要真实 Codex 登录态、网络和模型访问的 opt-in 检查。
- [Codex Review Gate](https://github.com/JoeyTeng/codex-review-gate)：可复用的 Codex review 完成状态检查。
- [文档索引](docs/README.zh-CN.md)：user-facing 文档、内部设计记录、计划、验证记录和项目 tracking 入口。

## License

本项目使用 Apache License 2.0。见 [LICENSE](LICENSE)。
