# Desktop Live Preflight Validation

本文记录 Desktop automatic delivery 前置验证流程。目标只是不经过审批地运行 no-DB Desktop inbox read helpers，并通过 direct file read 消费同一 revision 的 inbox snapshot；它不启用 caller heartbeat wake、`automation_update` 运行期调用、ready attempt materialization、writeback helper 或 continuation handoff。

## Preconditions

- `cbth` 已通过 GitHub Release 或本地 release build 安装到 Desktop heartbeat 可见的 `PATH`。
- 当前机器是 macOS / Linux dedicated single-user workstation。
- Desktop app 保持打开，并有一个用于验证的 bridge heartbeat thread。
- `CBTH_HOME` 使用默认 `~/.cbth`，或 heartbeat prompt 中显式传入同一个 `--home <path>`。

## Operator Setup

先在普通 shell 中创建 foundation state，并发布一个用于验证的 snapshot。这个 publisher 路径可以打开 SQLite；它不在 Desktop heartbeat sandbox 内运行：

```bash
cbth desktop installation-state --json

cbth desktop bridge-preflight \
  --bridge-thread-id <bridge-thread-id> \
  --helper-direct-store \
  --json
```

如果要记录一个真实验证成功后的 capability 结论，只能在 heartbeat 成功完成本文验证后执行：

```bash
cbth desktop installation-state repair \
  --read-transport direct-file-read \
  --read-transport-capability validated \
  --artifact-read-capability unknown \
  --writeback-capability unknown \
  --json
```

`artifact_read_capability` 和 `writeback_capability` 在本验证中必须保持 `unknown`。它们分别需要后续 artifact read 和 writeback helper 的独立无审批验证。

## Heartbeat Prompt

在真实 Codex Desktop heartbeat 中使用类似下面的 prompt。把 `<bridge-thread-id>` 替换成当前 bridge thread id；如果使用非默认 home，也同时替换 `<cbth-home-arg>`。

```text
Run this Desktop no-DB inbox read validation. Do not modify repository files.

1. Run:
   cbth <cbth-home-arg> desktop read-snapshot --bridge-thread-id <bridge-thread-id> --json
2. Run:
   cbth <cbth-home-arg> desktop list-arm-pending --bridge-thread-id <bridge-thread-id> --json
3. Run:
   cbth <cbth-home-arg> desktop list-pause-due --bridge-thread-id <bridge-thread-id> --json
4. Run:
   cbth <cbth-home-arg> desktop claim-next-ready --bridge-thread-id <bridge-thread-id> --json
5. Parse the JSON using these exact paths:
   - read snapshot object: .desktop_snapshot
   - installation read transport: .desktop_snapshot.installation_state.desktop_installation_state.read_transport
   - ready section: .desktop_snapshot.snapshots.ready_threads
   - arm section: .desktop_snapshot.snapshots.arm_pending_bindings
   - pause section: .desktop_snapshot.snapshots.pause_due_bindings
   - arm list object: .desktop_arm_pending_bindings
   - pause list object: .desktop_pause_due_bindings
   - ready claim object: .desktop_ready_claim
   - ready claim entry: .desktop_ready_claim.entry
6. Confirm .desktop_snapshot.schema_version, .desktop_arm_pending_bindings.schema_version, .desktop_pause_due_bindings.schema_version, and .desktop_ready_claim.schema_version are all 1.
7. Confirm all four nested objects have the same snapshot_revision and bridge_thread_id.
8. Confirm installation read_transport is direct_file_read, arm/pause entries are arrays, and ready claim entry is null or an object.
9. Reply with VALIDATION_OK plus the snapshot_revision and command summaries if all checks passed. If any command or read requires approval, blocks, or fails, reply VALIDATION_FAILED with the exact failed step.
```

Use an empty `<cbth-home-arg>` for the default home. For an isolated home, use `--home /absolute/path/to/cbth-home`.

## Expected Success Evidence

A valid run proves only these capabilities for the current Desktop / `cbth` / local environment fingerprint:

- Heartbeat can execute no-DB `cbth desktop ...` read helpers without approval.
- Heartbeat can direct-read `~/.cbth/inbox/current-snapshot.json` through `read-snapshot`.
- Heartbeat can direct-read the three revision-specific snapshot files referenced by the manifest.
- Heartbeat can direct-read `~/.cbth/inbox/desktop-installation-state.json`.
- Helper outputs agree on `schema_version`, `snapshot_revision`, and `bridge_thread_id`.

After recording this evidence, the operator may mark `read_transport_capability=validated`. This does not validate writeback helpers or artifact payload reads.

## Failure Handling

- If any read helper asks for approval, leave `read_transport_capability=unknown`.
- If any read helper attempts to publish snapshots, open SQLite, connect a daemon socket, or write files, treat that as a bug and leave `read_transport_capability=unknown`.
- If any file read asks for approval or fails, leave `read_transport_capability=unknown`.
- If helper outputs disagree on snapshot revision or bridge thread, treat it as a snapshot/export bug and do not repair capability state.
- If `desktop-installation-state.json` is missing, rerun with a `cbth` build that includes this validation foundation.
- If the Desktop app is closed, this validation is inconclusive; v1 only targets the app-alive heartbeat case.

## Cleanup

The heartbeat validation is read-only. It only reads cbth-owned state under `~/.cbth/inbox` and must not autostart a daemon, connect the daemon socket, open SQLite, or write files. There is no Codex thread mutation beyond the heartbeat response itself.
