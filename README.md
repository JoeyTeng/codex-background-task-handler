# codex-background-task-handler

Languages: [English (en-GB)](README.md) | [简体中文 (zh-CN)](README.zh-CN.md)

`codex-background-task-handler` provides the `cbth` companion CLI for supervised background work around Codex without modifying the upstream `codex` repository. It is a local, same-user helper for launching managed Codex CLI sessions, supervising long-running shell tasks, and delivering task results back to the bound Codex thread when the local safety checks allow it.

## Status

The project is currently suitable for local dogfooding on dedicated macOS or Linux single-user workstations.

- CLI dogfood v1 is the main supported path. It can launch managed `codex` foreground sessions, supervise local tasks, keep durable daemon state, and attempt automatic delivery with explicit `trusted-all` opt-in.
- Desktop bridge work is still a foundation and validation surface. It is not an enabled automatic Desktop delivery path yet.
- The implementation is intentionally outside the upstream `codex` repository. This repo contains the companion Rust binary, integration experiments, and documentation for the external control surfaces.

## Install

Release assets are currently available for Linux x86_64 glibc and macOS arm64. Intel macOS release assets are not published; Apple Silicon hosts launched from a Rosetta shell are mapped to the macOS arm64 asset by the installer.

Install the latest GitHub Release:

```bash
curl -fsSL https://raw.githubusercontent.com/JoeyTeng/codex-background-task-handler/HEAD/scripts/install.sh | sh
command -v cbth
cbth doctor cli
```

Install a specific release or directory:

```bash
CBTH_VERSION=v0.2.2 CBTH_INSTALL_DIR="$HOME/.local/bin" \
  sh scripts/install.sh
```

Upgrade an installed binary:

```bash
cbth self update --check
cbth self update -i
cbth self update --yes
```

For local development from a checkout:

```bash
cargo install --path .
cbth doctor cli
```

## Quick Start

Start a managed fresh Codex thread:

```bash
cbth new \
  --session-allows-approval false \
  --session-allows-network false \
  --session-allows-write-access false \
  --auto-delivery-policy trusted-all \
  --model gpt-5.5
```

Copy the thread id from:

```text
cbth: bound thread id: <thread-id>
```

From another shell, run a supervised task for that thread:

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

Inspect task and delivery state:

```bash
cbth task list --source-thread-id <thread-id>
cbth batch inspect-head --source-thread-id <thread-id>
cbth audit list --source-thread-id <thread-id> --limit 50
```

## Documentation

- [Usage guide](docs/USAGE.en-GB.md): installation, managed sessions, task supervision, daemon state, and operator commands.
- [Design overview](docs/DESIGN_OVERVIEW.en-GB.md): the high-level design, trust boundaries, and evolution path.
- [Operator recovery](docs/OPERATOR_RECOVERY.en-GB.md): manual recovery for `manual_resolution_only`, task logs, sessions, and maintenance.
- [Development guide](docs/DEVELOPMENT.en-GB.md): local development install, deterministic tests, hooks, and repo conventions.
- [Live E2E guide](docs/LIVE_E2E.en-GB.md): opt-in checks that use a real Codex login, network, and model access.
- [Codex Review Gate](https://github.com/JoeyTeng/codex-review-gate): reusable GitHub status check for Codex review completion.
- [Documentation index](docs/README.md): user-facing docs, internal design notes, plans, validation records, and project tracking entrypoints.

## License

This project is licensed under the Apache License 2.0. See [LICENSE](LICENSE).
