# Usage Guide

Languages: [English (en-GB)](USAGE.en-GB.md) | [Simplified Chinese (zh-CN)](USAGE.zh-CN.md)

This guide covers the normal `cbth` CLI workflow for local dogfooding. It assumes a dedicated macOS or Linux single-user workstation with a working `codex` CLI login.

## Install Or Upgrade

Install the latest GitHub Release:

```bash
curl -fsSL https://raw.githubusercontent.com/JoeyTeng/codex-background-task-handler/HEAD/scripts/install.sh | sh
command -v cbth
cbth doctor cli
```

Install a specific version or directory:

```bash
CBTH_VERSION=v0.2.0 CBTH_INSTALL_DIR="$HOME/.local/bin" \
  sh scripts/install.sh
```

Check for updates and upgrade:

```bash
cbth self update --check
cbth self update -i
cbth self update --yes
```

`cbth self update --yes` downloads the matching release binary and `.sha256`, verifies the checksum, writes a temporary file next to the current executable, and atomically replaces it. It does not use `sudo`; reinstall into a user-writable directory if the current executable is not writable.

## Readiness Check

Run the deployment readiness check before dogfooding a fresh install:

```bash
cbth doctor cli
```

`cbth doctor cli` may create or repair private `~/.cbth` state directories, open the SQLite store, start the same-user daemon, and briefly start a loopback `codex app-server` to verify listener parsing. It does not send a model request or create a Codex turn.

Use `--codex-bin <path>` when testing a non-default Codex CLI binary:

```bash
cbth doctor cli --codex-bin /path/to/codex
```

For isolated testing, set `CBTH_HOME` or pass `--home <path>`:

```bash
CBTH_HOME=/tmp/cbth-dogfood cbth doctor cli
```

## Managed Codex Sessions

Start a new managed Codex thread:

```bash
cbth new \
  --session-allows-approval false \
  --session-allows-network false \
  --session-allows-write-access false \
  --auto-delivery-policy trusted-all \
  --model gpt-5.5
```

`cbth new` launches foreground Codex through the managed shared app-server path. It prints the bound thread id to stderr:

```text
cbth: bound thread id: <thread-id>
```

Resume an existing thread through the same managed path:

```bash
cbth resume <thread-id> -- --model gpt-5.5
```

For lower-level control, use:

```bash
cbth cli run --bind-thread-id <thread-id> -- --model gpt-5.5
cbth cli run --new-thread -- --model gpt-5.5
```

By default, managed sessions are passive and record only current-state proof. Automatic delivery requires explicit `--auto-delivery-policy trusted-all`.

## Running Background Tasks

Run a supervised local command for the bound thread:

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

`cbth task run` creates a durable task and associated job, starts the command in its own process group, returns immediately with task/job identifiers, and lets the daemon complete or fail the job when the command exits.

Inspect and manage tasks:

```bash
cbth task list --source-thread-id <thread-id>
cbth task inspect --task-id <task-id>
cbth task cancel --task-id <task-id>
```

stdout and stderr are spooled under `~/.cbth/tasks/<task-id>/` with bounded tails and spool caps. Use [Operator Recovery](OPERATOR_RECOVERY.en-GB.md) if automatic delivery cannot be proven.

## Delivery Policy

Delivery policy is fail-closed by default. If the delivery flags are omitted, `cbth` treats the batch as not read-only and as requiring approval, network, and write access.

`trusted-all` is a broad explicit dogfood escape hatch. It bypasses batch policy, artifact-read, and managed-session risk-profile gates, but it still requires an open head batch, remaining budget, matching thread/session/epoch, and fresh idle proof.

The current automatic CLI path uses `turn/start` after durable idle proof. `turn/steer`, active-turn injection, rollout-only delivery proof, and foreground thread retargeting are outside the automatic delivery path.

## Inspecting State

Inspect the current head batch and audit trail:

```bash
cbth batch inspect-head --source-thread-id <thread-id>
cbth audit list --source-thread-id <thread-id> --limit 50
```

Inspect managed sessions:

```bash
cbth cli session list --bound-thread-id <thread-id>
cbth cli session inspect --managed-session-id <managed-session-id>
```

Inspect daemon-owned Codex app-servers:

```bash
cbth cli app-servers
cbth cli app-servers --human
```

Daemon control commands:

```bash
cbth daemon ensure
cbth daemon status
cbth daemon ping
cbth daemon stop
```

Mutating job, batch, task, and maintenance commands route through the same-user daemon by default. Read-only inspection commands read the local store directly.

## Desktop Bridge Commands

Desktop bridge commands currently expose operator/helper surfaces, not an enabled Desktop automatic delivery path.

```bash
cbth desktop installation-state --json
cbth desktop binding repair --source-thread-id <thread-id> --caller-automation-id <automation-id> --json
cbth desktop bridge-preflight --bridge-thread-id <thread-id> --json
cbth desktop read-snapshot --bridge-thread-id <thread-id> --json
cbth desktop list-arm-pending --bridge-thread-id <thread-id> --json
cbth desktop list-pause-due --bridge-thread-id <thread-id> --json
cbth desktop claim-next-ready --bridge-thread-id <thread-id> --json
```

See [Design Overview](DESIGN_OVERVIEW.en-GB.md) for the current Desktop boundary.
