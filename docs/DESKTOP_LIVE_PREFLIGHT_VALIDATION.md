# Desktop Live Preflight Validation

本文记录 Desktop automatic delivery 前置验证流程。目标只是不经过审批地运行 mandatory preflight helper，并通过 direct file read 读取同一 revision 的 inbox snapshot；它不启用 caller heartbeat wake、`automation_update` 运行期调用、ready attempt materialization 或 continuation handoff。

## Preconditions

- `cbth` 已通过 GitHub Release 或本地 release build 安装到 Desktop heartbeat 可见的 `PATH`。
- 当前机器是 macOS / Linux dedicated single-user workstation。
- Desktop app 保持打开，并有一个用于验证的 bridge heartbeat thread。
- `CBTH_HOME` 使用默认 `~/.cbth`，或 heartbeat prompt 中显式传入同一个 `--home <path>`。

## Operator Setup

先在普通 shell 中创建 foundation state，并用 direct-helper 路径创建一个用于验证的 snapshot：

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
Run this Desktop bridge preflight validation. Do not modify repository files.

1. Run:
   cbth <cbth-home-arg> desktop bridge-preflight --bridge-thread-id <bridge-thread-id> --helper-direct-store --json
2. Read the JSON field desktop_bridge_preflight.snapshot_manifest_path.
3. Read that current-snapshot.json file as plain text and parse it.
4. From the manifest, read:
   - snapshots.ready_threads.path
   - snapshots.arm_pending_bindings.path
   - snapshots.pause_due_bindings.path
   - installation_state_path
5. Confirm every snapshot file has schema_version = 1, the same snapshot_revision as the manifest, and the same bridge_thread_id.
6. Confirm desktop-installation-state.json has schema_version = 1 and a desktop_installation_state object.
7. Reply with VALIDATION_OK plus the snapshot_revision and the four file paths if all checks passed. If any command or read requires approval, blocks, or fails, reply VALIDATION_FAILED with the exact failed step.
```

Use an empty `<cbth-home-arg>` for the default home. For an isolated home, use `--home /absolute/path/to/cbth-home`.

## Expected Success Evidence

A valid run proves only these capabilities for the current Desktop / `cbth` / local environment fingerprint:

- Heartbeat can execute `cbth desktop bridge-preflight ... --json` without approval.
- Heartbeat can open the cbth store and publish inbox snapshots without touching daemon autostart, `startup.lock`, or the daemon Unix socket.
- Heartbeat can direct-read `~/.cbth/inbox/current-snapshot.json` without approval.
- Heartbeat can direct-read the three revision-specific snapshot files referenced by the manifest.
- Heartbeat can direct-read `~/.cbth/inbox/desktop-installation-state.json`.
- Manifest and data snapshots agree on `schema_version`, `snapshot_revision`, and `bridge_thread_id`.

After recording this evidence, the operator may mark `read_transport_capability=validated`. This does not validate writeback helpers or artifact payload reads.

## Failure Handling

- If helper execution asks for approval, leave `read_transport_capability=unknown`.
- If `--helper-direct-store` cannot open SQLite or write the inbox files, leave `read_transport_capability=unknown`; do not fall back to daemon-routed preflight for this validation.
- If any file read asks for approval or fails, leave `read_transport_capability=unknown`.
- If snapshot revisions disagree, treat it as a preflight/export bug and do not repair capability state.
- If `desktop-installation-state.json` is missing, rerun with a `cbth` build that includes this validation foundation.
- If the Desktop app is closed, this validation is inconclusive; v1 only targets the app-alive heartbeat case.

## Cleanup

This validation only writes cbth-owned state under `~/.cbth/inbox` and may run normal store sweep / GC as part of preflight. The heartbeat preflight itself must use `--helper-direct-store` and must not autostart a daemon or connect the daemon socket. There is no Codex thread mutation beyond the heartbeat response itself.
