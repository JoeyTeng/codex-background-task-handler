# 开发指南

语言：[English (en-GB)](DEVELOPMENT.en-GB.md) | [简体中文 (zh-CN)](DEVELOPMENT.zh-CN.md)

本文说明本仓库的本地开发和确定性检查。

## 本地安装

从 checkout 安装本地 binary：

```bash
cargo install --path .
command -v cbth
cbth --help
cbth doctor cli
```

如果不安装，直接测试 checkout：

```bash
cargo run --bin cbth -- doctor cli
cargo run --bin cbth -- daemon status
```

## 测试 Gate

默认确定性 Rust gate 是：

```bash
cargo fmt --all -- --check
cargo clippy --locked --all-targets -- -D warnings
cargo test --locked
```

Ubuntu 和 macOS CI 都运行这套 Rust matrix。Deterministic fake e2e 覆盖在 Rust integration tests 中，会随 `cargo test --locked` 运行。

Live Codex checks 是 opt-in，因为它们需要真实 Codex 登录态、网络/模型访问，有时还需要前台进程协同。见[真实 E2E 指南](LIVE_E2E.zh-CN.md)。

## Python 和 Probes

Python 只用于轻量 tooling、probes 和 prototypes。运行 Python 必须通过 `uv`：

```bash
uv run python scripts/desktop_thread_inject_poc.py --thread-id <thread-id> --mode inject
```

Production-facing components，例如 sidecars、supervisors 和 reusable CLIs，除非任务明确要求，否则应使用 Rust 实现。

## 本地 Git Hooks

安装 tracked pre-commit hook：

```bash
bash scripts/install-git-hooks.sh
```

脚本会给当前 clone 或 worktree 配置：

```bash
git config core.hooksPath .githooks
```

当前 hook 只在 staged diff 包含 Rust/Cargo 相关文件时运行：

- `*.rs`
- `Cargo.toml` 或 nested `Cargo.toml`
- `Cargo.lock` 或 nested `Cargo.lock`
- Rust toolchain、rustfmt 或 `.cargo` 配置

Hook 会运行：

```bash
cargo fmt --all
cargo clippy --locked --all-targets -- -D warnings
cargo test --locked
```

`cargo fmt --all` 会改写文件。如果 formatting 产生改动，hook 会失败并列出被改写文件；review 并 stage 这些文件后再重新 commit。

## Working Tree Safety

Hook 检查当前 working tree，而不是纯 staged snapshot。为了避免验证未提交的 Rust/Cargo 输入，它会在存在 tracked unstaged Rust/Cargo 文件或 untracked Rust/Cargo 文件时拒绝继续。

提交前请 stage 相关文件、把无关工作拆到其他 commit，或移动/删除未跟踪 Rust/Cargo 输入。

紧急 bypass：

```bash
git commit --no-verify
CBTH_SKIP_PRECOMMIT=1 git commit
```

## 文档布局

- User-facing guides 位于 `docs/` 顶层，并使用 `en-GB` / `zh-CN` 语言后缀；默认 `README.md` 入口例外。
- 内部设计记录位于 `docs/design/`。
- 实现计划位于 `docs/plans/`。
- 验证 harnesses 和 evidence 位于 `docs/validation/`。
- 项目 tracking 入口保持在 `docs/PROJECT_STATE.md` 和 `docs/PROJECT_TODO.md`。
- Durable project journal entries 保持在 `docs/project_journal/YYYY/MM/*.md`。
