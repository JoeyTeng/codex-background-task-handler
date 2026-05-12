# Git Hooks

本仓库使用 repo-tracked Git hooks 来提前拦截会让确定性 CI 变红的问题。hook 文件放在 `.githooks/`，安装脚本只设置本地 Git 配置，不引入额外依赖。

## Installation

```bash
bash scripts/install-git-hooks.sh
```

安装脚本会执行：

```bash
git config core.hooksPath .githooks
```

这个配置是 clone-local 的，不会随 commit 自动传播。新 clone 或新 worktree 需要各自运行一次安装脚本。

## Current Hook

当前只有 `.githooks/pre-commit`。它只在 staged diff 里出现 Rust/Cargo 相关文件时运行：

- `*.rs`
- `Cargo.toml` / `*/Cargo.toml`
- `Cargo.lock` / `*/Cargo.lock`
- Rust toolchain / rustfmt / `.cargo` 配置文件

当前检查顺序：

```bash
cargo fmt --all
cargo clippy --locked --all-targets -- -D warnings
cargo test --locked
```

`cargo fmt --all` 会自动改写格式。如果格式化产生改动，hook 会退出失败并列出被改写文件；调用者需要 review、stage，再重新 commit。

## Working Tree Safety

`cargo fmt`、`cargo clippy` 和 `cargo test` 都基于当前 working tree，而不是纯 staged snapshot。为了避免 hook 静默改写或验证未 staged / 未提交的 Rust/Cargo 输入，pre-commit 在检测到 tracked unstaged Rust/Cargo 文件或 untracked Rust/Cargo 文件时会拒绝继续。

处理方式：

```bash
git add <files>
git commit
```

或先把不属于当前 commit 的 Rust/Cargo 改动 stash / 拆到另一个 commit。未跟踪的 Rust/Cargo 文件也需要先 `git add`、移走或删除，否则本地 `cargo` 可能依赖它们通过检查，但 commit / CI 不包含这些输入。

紧急情况下可以用标准 Git bypass：

```bash
git commit --no-verify
```

也可以只跳过本仓库 hook：

```bash
CBTH_SKIP_PRECOMMIT=1 git commit
```

## Tracking

- 当前覆盖范围：Rust formatter、Rust lint、Rust unit/integration tests，与 `.github/workflows/ci.yml` 的 deterministic Rust gate 对齐。
- 当前不覆盖：`tools/codex-review-gate` 的 Node syntax/test checks。该子项目目前不是主 CI 的 formatter/linter/UT 卡点；如果未来纳入 required CI，再把对应命令追加到本文和 hook。
- 当前不覆盖：Markdown、YAML、Python、shell formatting。除非它们进入 required CI 或开始反复卡 PR，否则保持 hook 轻量。
