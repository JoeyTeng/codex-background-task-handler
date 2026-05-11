# Desktop Writeback Helper Live Validation

本文记录 Desktop automatic delivery 前置的 writeback helper 验证流程。目标是证明真实 Codex Desktop heartbeat 能无审批执行窄写回命令：`cbth desktop note-arm-pending` 和 `cbth desktop note-arm`。

本流程不启用 Desktop automatic delivery，不调用 `automation_update`，不唤醒 caller heartbeat，不实现 `note-boundary-crossed`，也不读取 artifact payload。

Current status: this direct helper path is blocked in real Desktop heartbeat because it reaches daemon autostart / `startup.lock` before `note-arm-pending` can mutate state. Before retrying this flow, first validate or implement a non-startup-lock writeback path. The current narrow probe is documented in [DESKTOP_WRITEBACK_DROPBOX_PROBE_VALIDATION.md](DESKTOP_WRITEBACK_DROPBOX_PROBE_VALIDATION.md).

## Preconditions

- `cbth` 已通过 GitHub Release 或本地 release build 安装到 Desktop heartbeat 可见的 `PATH`。
- 当前机器是 macOS / Linux dedicated single-user workstation。
- Desktop app 保持打开，并有一个用于验证的 heartbeat thread。
- `CBTH_HOME` 使用默认 `~/.cbth`，或 operator shell 与 heartbeat prompt 都显式传入同一个 `--home <path>`。
- `read_transport_capability` 可以已经是 `validated`；`writeback_capability` 在本验证成功前必须保持 `unknown`。

## Operator Setup

先在普通 shell 中准备 validation fixture。这个步骤可以打开 SQLite、使用 daemon、写入 store；它不在 Desktop heartbeat sandbox 内运行。

```bash
cbth desktop validation prepare-writeback-fixture \
  --source-thread-id <source-thread-id> \
  --caller-automation-id <caller-automation-id> \
  --bridge-request-id <bridge-request-id> \
  --json
```

记录返回 JSON 中的：

- `.desktop_writeback_fixture.source_thread_id`
- `.desktop_writeback_fixture.bridge_request_id`
- `.desktop_writeback_fixture.attempt.attempt_id`
- `.desktop_writeback_fixture.attempt.generation`
- `.desktop_writeback_fixture.batch.batch_id`

如果 fixture 创建失败，不要继续 heartbeat 验证。常见失败包括同一 source thread 已有 open batch、binding 已绑定到不同 automation、或 binding 仍有未 quiesced `armed_generation`。

## Heartbeat Prompt

在真实 Codex Desktop heartbeat 中使用类似下面的 prompt。把占位符替换为 fixture 返回值；如果使用非默认 home，也同时替换 `<cbth-home-arg>`。

```text
Run this Desktop writeback helper validation. Do not modify repository files.

1. Run:
   cbth <cbth-home-arg> desktop note-arm-pending \
     --source-thread-id <source-thread-id> \
     --attempt-id <attempt-id> \
     --generation <generation> \
     --bridge-request-id <bridge-request-id> \
     --json
2. Parse .desktop_arm_pending.outcome and .desktop_arm_pending.bridge_arm_lease_id.
3. Run the same note-arm-pending command again.
4. Confirm the second .desktop_arm_pending.outcome is already_pending and the lease id matches.
5. Run:
   cbth <cbth-home-arg> desktop note-arm \
     --source-thread-id <source-thread-id> \
     --attempt-id <attempt-id> \
     --generation <generation> \
     --bridge-request-id <bridge-request-id> \
     --bridge-arm-lease-id <bridge-arm-lease-id> \
     --json
6. Run the same note-arm command again.
7. Confirm:
   - first note-arm-pending outcome is arm_pending
   - repeated note-arm-pending outcome is already_pending
   - first note-arm outcome is armed
   - repeated note-arm outcome is already_armed
   - repeated note-arm .desktop_arm.delivery_attempt_count is still 1
8. Reply with VALIDATION_OK plus the attempt id, lease id, and command summaries if all checks passed. If any command requires approval, blocks, or fails, reply with VALIDATION_FAILED and the exact failed step.
```

Use an empty `<cbth-home-arg>` for the default home. For an isolated home, use `--home /absolute/path/to/cbth-home`.

## Operator Verification

After a heartbeat `VALIDATION_OK`, verify state from a normal shell:

```bash
cbth attempt inspect --attempt-id <attempt-id>

cbth batch inspect --batch-id <batch-id>

cbth desktop bridge-preflight \
  --bridge-thread-id <bridge-thread-id> \
  --helper-direct-store \
  --json

cbth desktop list-pause-due \
  --bridge-thread-id <bridge-thread-id> \
  --json
```

Success evidence requires:

- attempt state is `cooldown`.
- `desktop_armed_at` is set.
- batch `delivery_attempt_count` is `1`.
- binding has `armed_generation`, `pause_not_before`, and `pause_deadline`.
- `list-pause-due` can read the published pause-due skeleton when the current time is at or after `pause_deadline`.

Only after this evidence is recorded should the operator mark writeback helpers as validated:

```bash
cbth desktop installation-state repair \
  --read-transport direct-file-read \
  --read-transport-capability validated \
  --artifact-read-capability unknown \
  --writeback-capability validated \
  --json
```

`artifact_read_capability` remains `unknown`; it requires separate artifact-read helper validation.

## Failure Handling

- If heartbeat cannot execute either writeback command without approval, leave `writeback_capability=unknown`.
- If the command fails while connecting to daemon IPC, opening SQLite, or touching Desktop-sandbox-restricted files, record the exact error as the next blocker.
- If note-arm-pending succeeds but note-arm fails, leave the fixture in its fail-closed durable state and inspect it from operator shell; do not manually edit SQLite.
- If repeated calls are not idempotent, treat that as a helper bug and do not repair capability state.
- If fixture setup accidentally targets a real caller automation, stop and close the fixture batch manually before further testing.

## Cleanup

The fixture creates a real open Desktop batch for the chosen source thread. After validation, close it from operator shell:

```bash
cbth batch close-head \
  --source-thread-id <source-thread-id> \
  --reason operator-confirmed-delivery \
  --note "Desktop writeback helper live validation cleanup"
```

The cleanup is operator-owned. The heartbeat validation itself must not invoke `batch close-head`, `installation-state repair`, or any Desktop automatic delivery path.
