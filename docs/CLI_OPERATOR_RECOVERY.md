# CLI Operator Recovery

This guide covers the manual recovery surface for CLI dogfood v1. It assumes a macOS/Linux single-user workstation and a trusted local `CBTH_HOME` / `~/.cbth`.

## Inspect Current State

Start with the daemon and readiness checks:

```bash
cbth doctor cli
cbth daemon status
```

For a caller thread, inspect the current head batch and audit trail:

```bash
cbth batch inspect-head --source-thread-id <thread-id>
cbth audit list --source-thread-id <thread-id> --limit 50
```

Inspect the managed CLI session bound to the caller thread:

```bash
cbth cli session list --bound-thread-id <thread-id>
cbth cli session inspect --managed-session-id <managed-session-id>
```

If the head batch has already closed and `inspect-head` no longer finds it, use the `batch_id` from audit or task/job output:

```bash
cbth batch inspect --batch-id <batch-id>
```

## Task Logs

Daemon-supervised tasks are inspectable even after their associated job has completed or failed:

```bash
cbth task list --source-thread-id <thread-id>
cbth task inspect --task-id <task-id>
```

`task inspect` returns `stdout_log_path`, `stderr_log_path`, byte counts, and truncation flags. Paths are relative to `CBTH_HOME`; for the default home, inspect logs with:

```bash
less ~/.cbth/<stdout_log_path>
less ~/.cbth/<stderr_log_path>
```

Completed task log directories are retained while linked batches remain open and for the post-close retention window. After maintenance cleanup, the durable task row remains, but log path fields may be cleared.

## Manual Resolution

`manual_resolution_only` means `cbth` could not prove safe automatic delivery. Typical causes are ambiguous `turn/start` acceptance, websocket/app-server continuity loss after acceptance, terminal failure/interruption evidence, or a batch policy outside the current automatic path.

Use the audit trail to decide whether the assistant-visible result already landed:

```bash
cbth audit list --source-thread-id <thread-id> --limit 100
cbth batch inspect-head --source-thread-id <thread-id>
```

If you verified the caller thread already received and used the result, close the head batch as confirmed:

```bash
cbth batch close-head \
  --source-thread-id <thread-id> \
  --reason operator-confirmed-delivery \
  --note "verified in caller thread"
```

If you cannot prove delivery, close it unconfirmed before retrying or filing a follow-up:

```bash
cbth batch close-head \
  --source-thread-id <thread-id> \
  --reason operator-closed-unconfirmed \
  --note "manual recovery: delivery could not be proven"
```

Do not manually edit the SQLite database or task log files. Use the CLI so audit records, artifact retention, and daemon lifecycle state stay consistent.

## Session Retirement

Managed CLI sessions can outlive a foreground `codex --remote` process. After foreground teardown, `cbth` marks the session `detached` and clears proof so stale idle/capability evidence cannot open a new automatic delivery. If delivery fails closed into `manual_resolution_only`, the session becomes `parked` until the manual head batch is closed or swept.

After manual recovery is complete, retire an old non-live session before reusing the thread with a different risk profile or replacing a stale record:

```bash
cbth cli session retire \
  --managed-session-id <managed-session-id> \
  --reason "operator cleanup after manual recovery"
```

Retirement is fail-closed. It refuses `live` sessions, sessions that still own active delivery attempts, and sessions whose bound thread still has an open `manual_resolution_only` head batch. In those cases, inspect the head batch or attempt first, close the manual head batch when you have made a decision, then retry `cli session retire` or `cli run`.

`cbth cli run --bind-thread-id ...` can automatically retire and replace `detached`, `parked`, or `stale` records only after the same safety checks pass. It never creates a second non-retired session for the same bound thread.

## Maintenance And Cleanup

Run a sweep when stale attempts, expired artifacts, or task-log cleanup need to be reconciled immediately:

```bash
cbth maintenance sweep
```

Stop the daemon only after active tasks are complete or intentionally cancelled:

```bash
cbth task list --status running
cbth daemon stop
```

On daemon crash/restart, queued or running tasks that can no longer be proven supervised are failed closed during startup recovery. Inspect failed tasks and associated batches before resubmitting work.
