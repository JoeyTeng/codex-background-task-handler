# CLI Task Supervisor Design

## Summary

The CLI task supervisor is the Dogfood V1 bridge between local long-running commands and the existing fixed-thread delivery loop. It lets a user submit a local command as a background task, lets the `cbth` daemon own the child process lifecycle, and converts the terminal task result into a normal background job/batch that can be delivered by the existing idle `turn/start` path.

This design intentionally does not change the foreground Codex interaction model. Users still run native `codex --remote` through `cbth cli run`; the supervisor only produces durable background work that the already-bound caller thread can consume when idle.

## Goals

- Provide a stable first local-dogfood task entrypoint: `cbth task run`.
- Keep the daemon responsible for long-running child/process-group lifecycle, not the short-lived CLI process.
- Bound memory, file, child-process, and idle CPU usage for tasks that run for minutes or hours.
- Reuse existing job, batch, audit, and trusted-all idle auto-delivery state instead of adding a separate delivery channel.
- Preserve fail-closed behavior when supervision, output capture, or delivery evidence is incomplete.

## Non-Goals

- No public socket/API/plugin protocol in this PR.
- No Desktop bridge changes.
- No active-turn `turn/steer` automatic injection.
- No rule/allowlist/model policy engine; `trusted-all` remains the explicit escape hatch.
- No multi-user/server deployment model and no pure Windows support.

## User Interface

Task submission:

```text
cbth task run \
  --source-thread-id <thread-id> \
  --summary <text> \
  [--delivery-read-only <bool>] \
  [--delivery-requires-approval <bool>] \
  [--delivery-requires-network <bool>] \
  [--delivery-requires-write-access <bool>] \
  [--cwd <dir>] \
  [--timeout-seconds <n>] \
  -- <cmd> [args...]
```

Operator surface:

```text
cbth task inspect --task-id <task-id>
cbth task list [--source-thread-id <thread-id>] [--status <status>] [--limit <n>]
cbth task cancel --task-id <task-id>
```

`task run` returns immediately after durable task/job creation and successful daemon spawn ownership. `task inspect` and `task list` are read-only local-store operations. `task cancel` routes to the same-user daemon so cancellation targets the live supervised process group when it still exists.

The client resolves the executable before submitting to the daemon. Bare command names are resolved against the caller's `PATH`; relative paths containing `/` are resolved against the requested task cwd. This prevents an already-running daemon with a different environment from changing command lookup semantics.

## Data Model

Each task has a durable `TaskRecord` linked to exactly one `JobRecord`:

- `task_id`: stable task identifier returned to the user.
- `job_id`: associated background job that participates in the existing batch pipeline.
- `source_thread_id`: caller thread that should receive the final task result.
- `summary`: user-provided short purpose of the task.
- `command_json`: command and argv for audit/inspection.
- `cwd`: working directory used for the child process.
- `status`: queued, running, succeeded, failed, cancelled, timed_out, or lost.
- `pid`: best-effort child pid observed while the daemon owns the task.
- stdout/stderr log paths, byte counts, and truncation flags.
- timestamps and terminal metadata such as exit code, signal, and failure reason.

The job remains the delivery unit. A successful task completes its associated job; a failed, cancelled, timed-out, or spawn-failed task fails its associated job. The existing head-batch logic then decides whether and when to deliver that job result.

## Daemon Supervision

`task run` is daemon-owned:

1. The CLI ensures or connects to the same-user daemon.
2. The daemon creates a durable task and associated job in one store transaction.
3. The daemon spawns the command in its own process group.
4. The daemon records running state and returns `task_id`, `job_id`, and `source_thread_id` to the CLI.
5. A daemon worker owns process wait, output spool, timeout, cancellation, and terminal job close.

The submitted command runs with the `cbth task run` caller's environment, not the daemon's stale startup environment. The environment is transmitted over the same-user daemon IPC for spawn only and is not persisted in the task row, avoiding accidental long-term storage of secrets while keeping command behavior stable when the daemon was already running.

Active supervised tasks prevent normal daemon idle exit. On intentional daemon shutdown, the daemon best-effort terminates all supervised process groups before releasing app-server resources.

If the daemon crashes or restarts, queued/running tasks that can no longer be proven supervised are failed closed on startup. Startup recovery only runs after the daemon has proven socket exclusivity, so a duplicate daemon process cannot kill tasks still owned by the active daemon. Before marking pending jobs lost, startup recovery sends the first termination signal only when the current PID still matches the process identity captured at spawn time; if identity is missing or mismatched, recovery skips the kill to avoid PID/PGID reuse damage. After that verified SIGTERM, recovery sends SIGKILL if the process group still exists, which covers leaders that exit on TERM while same-group descendants keep running. V1 does not attempt orphan process adoption.

## Output And Artifact Handling

stdout and stderr are streamed to managed task log files under the local `CBTH_HOME` task directory. The daemon keeps bounded in-memory tails only for prompt summaries.

Resource defaults:

- Per-stream in-memory tail: 64 KiB.
- Per-stream spool cap: 64 MiB.
- Concurrent supervised task cap: 16 active tasks per daemon.
- Cancel grace before kill: 5 seconds.
- Task timeout: none unless `--timeout-seconds` is provided.

When a spool cap is reached, the daemon marks that stream as truncated and stops appending more bytes for that stream. It must not keep full command output in memory.

The final job result includes command metadata, terminal status, exit code or signal, byte counts, truncation flags, small stdout/stderr tail previews, and managed artifact refs. Large logs stay in files/artifacts and are not inlined into the delivery prompt.

The task row persists its delivery retry budget and redelivery window so restart recovery can create the same fail-closed batch the live worker would have created.

`redelivery_window_seconds` is rejected before durable task/job creation if adding it to the current timestamp would overflow.

Completed task log directories are removed by maintenance sweep only after all associated delivery batches are closed and the post-close artifact retention window has elapsed. The durable task row remains inspectable, but log path fields are cleared once the directory has been deleted.

## Cancellation And Timeout

Cancellation is durable and best-effort:

- `task cancel` records the cancel request first.
- If cancellation is already requested before the worker wins the spawn gate, the daemon closes the task as `cancelled` without executing the command.
- The daemon sends SIGTERM to the task process group when available.
- After the grace window, the daemon sends SIGKILL if the process group is still alive.
- The final task/job state records `cancelled` when the cancel request was recorded before the worker observed process exit, including SIGTERM trap handlers that exit with status 0. If the worker had already observed the process-group exit before a late cancel/timeout arrives during stream drain, the original child result is preserved.

Timeout uses the same process-group termination path and records `timed_out`. If the direct child exits while same-group descendants keep running, the daemon keeps the task in a controllable running state until the process group exits, is cancelled, or times out. If the process group is already gone and a late cancel/timeout arrives while stdout/stderr workers are only draining or syncing logs, the daemon preserves the already observed child result and may only abort/truncate the stream drain. If descendants still hold stdout/stderr pipes open after process-group termination, the daemon keeps the process group cancellable while bounded spool joins finish.

## Delivery Integration

The supervisor does not create a new delivery mechanism. It feeds the existing pipeline:

1. Terminal task result updates the associated job.
2. Existing batch creation places that job in the caller thread head batch.
3. `cbth cli run --auto-delivery-policy trusted-all` can deliver the head batch only when the managed session has fresh idle proof and full app-server capability proof.
4. Delivery uses existing unique marker, accepted-turn tracking, notification/reconcile handling, and fail-closed stale sweep behavior.

Default `cbth cli run` remains passive. `trusted-all` is still required for automatic delivery.

## Reliability Boundaries

- No unbounded stdout/stderr memory accumulation.
- No unbounded task registry growth: new submissions are rejected once the daemon reaches the active supervised task cap, and in-memory task controls are removed after terminal handling.
- Child/process-group handles are explicitly cleaned up on spawn, post-spawn setup, cancellation, timeout, daemon shutdown, and worker failure paths.
- Direct child exit does not by itself prove task completion; the daemon also waits for the supervised process group to disappear so backgrounded same-group work remains cancellable.
- Store state is updated before externally visible actions whenever possible, so restart recovery can fail closed.
- Startup recovery validates persisted process identity before first signalling a lost process group, then escalates to SIGKILL if that verified process group remains alive.
- Restart recovery terminalizes queued/running task rows whose associated jobs already reached `completed` or `failed`, rather than leaving stale live-looking task records.
- Stream drain has a hard cutoff after termination/kill and after the supervised process group disappears. If an escaped descendant keeps stdout/stderr open, spool workers are asked to abort and the task is terminalized instead of occupying a supervisor slot indefinitely.
- Task log files are not retained forever; maintenance cleanup removes expired task directories and clears log path fields only after linked batches are no longer open and their post-close retention has elapsed.
- Ambiguous delivery evidence is not retried blindly; existing delivery state owns manual resolution.

## Test Strategy

Default deterministic tests should cover:

- `task run` success completes the job and trusted-all fake e2e closes the batch as delivered.
- Non-zero exit, spawn failure, signal, timeout, and cancel fail the job.
- Large stdout/stderr respect spool caps and bounded tails.
- Active tasks block daemon idle exit; terminal tasks allow normal idle behavior.
- Daemon stop terminates supervised tasks.
- Restart with queued/running tasks fails closed when ownership was lost.

Opt-in live coverage should cover:

```text
CBTH_RUN_LIVE_TASK_SUPERVISOR_E2E=1 cargo test --test live_task_supervisor -- --ignored --nocapture
```

The live test uses real `cbth cli run --new-thread --auto-delivery-policy trusted-all`, real daemon-owned `cbth task run`, and real Codex app-server delivery, but remains outside default CI.
