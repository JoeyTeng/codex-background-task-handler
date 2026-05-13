# Live E2E Guide

Languages: [English (en-GB)](LIVE_E2E.en-GB.md) | [简体中文 (zh-CN)](LIVE_E2E.zh-CN.md)

This guide covers opt-in live checks that use a real Codex CLI login, model access, and network access. Default CI does not run these tests; they are intended for local validation and dogfooding.

## Environment

Required:

- Installed and logged-in `codex` CLI.
- Node.js for `scripts/cli_shared_app_server_poc.mjs`.
- Network/model access for real Codex turns.
- Local `cbth` checkout and Rust toolchain.

Recommended preflight:

```bash
codex --version
node --version
cargo --version
cargo run --bin cbth -- doctor cli
```

Optional environment variables:

- `CBTH_LIVE_CODEX_BIN`: override the real `codex` binary, default `codex`.
- `CBTH_LIVE_NODE_BIN`: override the Node binary, default `node`.
- `CBTH_LIVE_CODEX_E2E_TIMEOUT_MS`: shared app-server smoke timeout, default `180000`.
- `CBTH_LIVE_TRUSTED_ALL_E2E_TIMEOUT_SECONDS`: trusted-all full e2e timeout, default `360`.
- `CBTH_LIVE_NEW_THREAD_E2E_TIMEOUT_SECONDS`: `--new-thread` full e2e timeout, default `360`.
- `CBTH_LIVE_TASK_SUPERVISOR_E2E_TIMEOUT_SECONDS`: task-supervisor full e2e timeout, default `360`.

## Shared App-Server Smoke

This smoke starts a real `codex app-server`, uses the Node PoC to create a frontend thread, then uses a sidecar client to send `turn/start` to the same thread. It confirms that the frontend sees started/completed notifications and that `thread/read` can see the marker.

```bash
CBTH_RUN_LIVE_CODEX_E2E=1 cargo test --test live_smoke -- --ignored
```

## Trusted-All Full Live E2E

This test validates automatic delivery through `cbth cli run --auto-delivery-policy trusted-all`:

- Create a real caller thread through `codex app-server`.
- Start `cbth cli run --bind-thread-id <thread-id> --auto-delivery-policy trusted-all`.
- Wait for sidecar idle proof and automatic-delivery capability.
- Submit or fail a job to create a head batch.
- Wait for sidecar `turn/start`, accepted turn observation or reconcile, and `close_reason=delivered`.
- Check audit records for allow, attempt-start, accepted, and observed or reconciled events.

```bash
CBTH_RUN_LIVE_TRUSTED_ALL_E2E=1 cargo test --test live_trusted_all -- --ignored
```

## New-Thread Trusted-All Full Live E2E

This test validates fresh-thread bootstrap through `cbth cli run --new-thread --auto-delivery-policy trusted-all`:

- `cbth` starts a daemon-owned pending `codex app-server`.
- Foreground Codex creates the thread.
- `cbth` binds the discovered `thread/started` id and prints it to stderr.
- The test submits a marker job and waits for the same delivered close path as existing-thread trusted-all e2e.

```bash
CBTH_RUN_LIVE_NEW_THREAD_E2E=1 cargo test --test live_new_thread -- --ignored
```

## Task Supervisor Full Live E2E

This test validates real daemon-owned `cbth task run` through the full trusted-all automatic-delivery loop:

- Start `cbth cli run --new-thread --auto-delivery-policy trusted-all`.
- Read the bound thread id from stderr.
- Wait for idle proof and automatic-delivery capability.
- Run a daemon-supervised shell command through `cbth task run`.
- Let the daemon complete the task, create a result artifact, and create a head batch.
- Wait for automatic delivery and verify task logs, artifact payload, and audit records.

```bash
CBTH_RUN_LIVE_TASK_SUPERVISOR_E2E=1 cargo test --test live_task_supervisor -- --ignored --nocapture
```

## Manual Dogfood Walkthrough

After installing the local binary with `cargo install --path .`, run:

```bash
cbth doctor cli
```

Start a managed fresh thread:

```bash
cbth new \
  --session-allows-approval false \
  --session-allows-network false \
  --session-allows-write-access false \
  --auto-delivery-policy trusted-all \
  --model gpt-5.5
```

Copy the bound thread id from stderr, then submit a supervised task from another shell:

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

Use the recovery surface if the batch does not close automatically:

```bash
cbth task list --source-thread-id <thread-id>
cbth batch inspect-head --source-thread-id <thread-id>
cbth audit list --source-thread-id <thread-id> --limit 100
```

## Failure Notes

- If listener bootstrap hangs, run the shared app-server smoke first. It quickly exposes `codex app-server` output-stream, login, or Node PoC issues.
- If `--new-thread` never prints `cbth: bound thread id: ...`, check whether the current `codex app-server` supports the startup path.
- If session readiness stalls, inspect the last observed session proof fields in the test output.
- If task-supervisor e2e stalls on task completion, run `cbth task inspect --task-id <task-id>` and inspect `status`, `pid`, log paths, and truncation flags.
- If the assistant marker turn appears complete but the batch enters `manual_resolution_only`, inspect `cbth audit list --source-thread-id <thread-id>` for acceptance and observation/reconcile evidence.
