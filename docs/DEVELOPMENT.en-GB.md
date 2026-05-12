# Development Guide

This guide covers local development and deterministic checks for this repository.

## Local Install

Install the local binary from a checkout:

```bash
cargo install --path .
command -v cbth
cbth --help
cbth doctor cli
```

Use `cargo run` when testing the checkout without installing:

```bash
cargo run --bin cbth -- doctor cli
cargo run --bin cbth -- daemon status
```

## Test Gate

The default deterministic Rust gate is:

```bash
cargo fmt --all -- --check
cargo clippy --locked --all-targets -- -D warnings
cargo test --locked
```

Ubuntu and macOS CI both run that Rust matrix. Deterministic fake e2e coverage lives in Rust integration tests and runs with `cargo test --locked`.

Live Codex checks are opt-in because they require a real Codex login, network/model access, and sometimes foreground process coordination. See [Live E2E Guide](LIVE_E2E.en-GB.md).

## Python And Probes

Python is limited to lightweight tooling, probes, and prototypes. Run Python through `uv` only:

```bash
uv run python scripts/desktop_thread_inject_poc.py --thread-id <thread-id> --mode inject
```

Production-facing components such as sidecars, supervisors, and reusable CLIs should be implemented in Rust unless a task explicitly says otherwise.

## Local Git Hooks

Install the tracked pre-commit hook:

```bash
bash scripts/install-git-hooks.sh
```

The script configures this clone or worktree with:

```bash
git config core.hooksPath .githooks
```

The current hook runs only when the staged diff contains Rust/Cargo-related files:

- `*.rs`
- `Cargo.toml` or nested `Cargo.toml`
- `Cargo.lock` or nested `Cargo.lock`
- Rust toolchain, rustfmt, or `.cargo` configuration

The hook runs:

```bash
cargo fmt --all
cargo clippy --locked --all-targets -- -D warnings
cargo test --locked
```

`cargo fmt --all` rewrites files. If formatting changes anything, the hook exits failed and lists the rewritten files; review and stage those files before committing again.

## Working Tree Safety

The hook checks the current working tree, not a pure staged snapshot. To avoid validating Rust/Cargo inputs that will not be committed, it refuses to continue when tracked unstaged Rust/Cargo files or untracked Rust/Cargo files are present.

Stage the relevant files, split unrelated work into another commit, or move/remove untracked Rust/Cargo inputs before committing.

Emergency bypasses:

```bash
git commit --no-verify
CBTH_SKIP_PRECOMMIT=1 git commit
```

## Documentation Layout

- User-facing guides live at the top of `docs/` and use `en-GB` / `zh-CN` language suffixes, except default `README.md` entrypoints.
- Internal design records live under `docs/design/`.
- Implementation plans live under `docs/plans/`.
- Validation harnesses and evidence live under `docs/validation/`.
- Project tracking entrypoints remain at `docs/PROJECT_STATE.md` and `docs/PROJECT_TODO.md`.
- Durable project journal entries remain under `docs/project_journal/YYYY/MM/*.md`.
