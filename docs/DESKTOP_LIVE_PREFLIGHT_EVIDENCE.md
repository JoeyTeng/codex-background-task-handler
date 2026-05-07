# Desktop Live Preflight Evidence

本文记录真实 Codex Desktop heartbeat 对 Desktop bridge preflight 的实测证据。它只用于判断 installation-wide capability 是否可以写为 `validated`，不表示 Desktop automatic delivery 已启用。

## 2026-05-05 Attempt: Failed Before Preflight Snapshot Read

Result: `VALIDATION_FAILED`

Evidence:

- PR #35 merge commit: `bbe400391dd3a44dcf7bbc82ef59e04bac120e6f`
- Local branch: `codex/desktop-live-preflight-evidence`
- Local binary: `/Users/hoteng/.cache/cargo-target/release/cbth`
- Local binary version: `cbth 0.1.0`
- Bridge thread id: `019db5e6-ba6a-7b80-95d2-a6867163281a`
- Temporary heartbeat automation id: `cbth-desktop-live-preflight-validation`
- Validation marker: `CBTH_DESKTOP_PREFLIGHT_20260505_BBE4003`
- Target rollout file: `/Users/hoteng/.codex/sessions/2026/04/22/rollout-2026-04-22T16-54-50-019db5e6-ba6a-7b80-95d2-a6867163281a.jsonl`
- Heartbeat run timestamp: `2026-05-05T10:47:01.839Z`

The Desktop heartbeat successfully started a turn and executed the local validation script without asking the user for approval. It ran `/Users/hoteng/.cache/cargo-target/release/cbth --version` and received `cbth 0.1.0`.

The mandatory preflight helper failed at:

```text
CBTH_DESKTOP_PREFLIGHT_20260505_BBE4003 VALIDATION_FAILED step2_desktop_bridge_preflight_failed: chmod 0700 /Users/hoteng/.cbth: Operation not permitted (os error 1)
```

Local inspection immediately after the failure showed `/Users/hoteng/.cbth` already had private permissions:

```text
drwx------ 7 hoteng staff 224 May  5 11:45 /Users/hoteng/.cbth
```

Therefore this attempt does not validate `read_transport_capability`. The durable installation state was left unchanged:

```text
read_transport_capability=unknown
artifact_read_capability=unknown
writeback_capability=unknown
read_transport_generation=0
validated_at=null
```

## Retest Condition From First Attempt

Before rerunning the same Desktop heartbeat validation, `cbth` should avoid a redundant `chmod` when an existing private directory or private file already satisfies the required mode. That change must preserve fail-closed repair behavior for too-permissive or otherwise unsafe paths; it should only skip permission mutation when local metadata proves the existing object is already private enough.

## 2026-05-06 Attempt: Failed At Startup Lock

Result: `VALIDATION_FAILED`

Evidence:

- Code branch: `codex/desktop-noop-private-permissions`
- Local binary: `/Users/hoteng/.cache/cargo-target/release/cbth`
- Local binary version: `cbth 0.1.0`
- Bridge thread id: `019db5e6-ba6a-7b80-95d2-a6867163281a`
- Temporary heartbeat automation id: `cbth-desktop-live-preflight-validation-retry`
- Validation marker: `CBTH_DESKTOP_PREFLIGHT_20260506_NOOP_CHMOD`
- Target rollout file: `/Users/hoteng/.codex/sessions/2026/04/22/rollout-2026-04-22T16-54-50-019db5e6-ba6a-7b80-95d2-a6867163281a.jsonl`
- Heartbeat run timestamp: `2026-05-06T10:11:29.115Z`

The local shell preflight with the same binary succeeded after the no-op chmod fix and published snapshot revision `019dfcc3-ed26-7b93-84f2-836054bc630f`.

The Desktop heartbeat got past the previous `chmod 0700 /Users/hoteng/.cbth` blocker, but mandatory preflight still failed before snapshot reads:

```text
CBTH_DESKTOP_PREFLIGHT_20260506_NOOP_CHMOD VALIDATION_FAILED step2_bridge_preflight_failed:open startup lock /Users/hoteng/.cbth/run/startup.lock: Operation not permitted (os error 1)
```

Local inspection after the failure showed the daemon run directory and startup lock were already private:

```text
drwx------ 7 hoteng staff 224 May  6 11:09 /Users/hoteng/.cbth
drwx------ 4 hoteng staff 128 May  6 11:09 /Users/hoteng/.cbth/run
srw------- 1 hoteng staff   0 May  6 11:09 /Users/hoteng/.cbth/run/cbth.sock
-rw------- 1 hoteng staff  10 May  6 11:09 /Users/hoteng/.cbth/run/startup.lock
```

Therefore this attempt still does not validate `read_transport_capability`. The durable installation state was left unchanged:

```text
read_transport_capability=unknown
artifact_read_capability=unknown
writeback_capability=unknown
read_transport_generation=0
validated_at=null
```

## 2026-05-06 Attempt: Existing-Daemon Socket Blocked

Result: `VALIDATION_FAILED`

Evidence:

- Code branch: `codex/desktop-existing-daemon-preflight`
- Local binary: `/Users/hoteng/.cache/cargo-target/release/cbth`
- Local binary version: `cbth 0.1.0`
- Bridge thread id: `019db5e6-ba6a-7b80-95d2-a6867163281a`
- Temporary heartbeat automation id: `cbth-desktop-live-preflight-existing-daemon-validation-final`
- Validation marker: `CBTH_DESKTOP_PREFLIGHT_20260506_EXISTING_DAEMON_V3`
- Target rollout file: `/Users/hoteng/.codex/sessions/2026/04/22/rollout-2026-04-22T16-54-50-019db5e6-ba6a-7b80-95d2-a6867163281a.jsonl`
- Heartbeat run timestamp: `2026-05-06T13:11:01.592Z`

The local foreground daemon path succeeded with the same final binary and `--require-existing-daemon`, publishing snapshot revision `019dfd68-7b03-7bc3-b35f-6742386cb40c`.

The Desktop heartbeat executed the corrected helper prompt and failed before publishing a snapshot:

```text
CBTH_DESKTOP_PREFLIGHT_20260506_EXISTING_DAEMON_V3 VALIDATION_FAILED step1_command_failed:probe existing daemon: connect daemon socket /Users/hoteng/.cbth/run/cbth.sock: connect unix socket: Operation not permitted (os error 1)
```

Local POSIX permission inspection after the failure showed the cbth home, run directory, daemon socket, and startup lock were already private and owned by the current user (`uid=501`, `hoteng`):

```text
/Users/hoteng/.cbth type=Directory mode=drwx------ uid=501 gid=20 flags=0
/Users/hoteng/.cbth/run type=Directory mode=drwx------ uid=501 gid=20 flags=0
/Users/hoteng/.cbth/run/cbth.sock type=Socket mode=srw------- uid=501 gid=20 flags=0
/Users/hoteng/.cbth/run/startup.lock type=Regular File mode=-rw------- uid=501 gid=20 flags=0
```

This means the live blocker is not a `chmod` / ownership misconfiguration. The evidence points to the Codex Desktop heartbeat tool sandbox or macOS app permission boundary denying `connect()` to the Unix domain socket with `EPERM`.

The heartbeat thread reported that the temporary automation was deleted after the failed run.

Therefore this attempt still does not validate `read_transport_capability`. The durable installation state was left unchanged:

```text
read_transport_capability=unknown
artifact_read_capability=unknown
writeback_capability=unknown
read_transport_generation=0
validated_at=null
```

## Next Retest Condition

PR #38 (`08d196e76429836938e5bb6ba560c2f21c22cee7`) fixed the redundant private-permission mutation that caused the first failure. PR #39 (`22309e4d9c13e311b1d5e496281cd752b54dd3ed`) proved the existing-daemon path avoids `startup.lock` but still fails when Desktop heartbeat tries to connect the same-user Unix socket.

The existing-daemon path now fails at Desktop heartbeat socket connection, not at chmod, ownership, or startup-lock mutation. The next validation should use the narrower direct-helper path so heartbeat can publish/read inbox snapshots without daemon autostart, `startup.lock`, or Unix socket access.

For local shell sanity checks, the direct-helper command is:

```bash
cbth desktop bridge-preflight --bridge-thread-id 019db5e6-ba6a-7b80-95d2-a6867163281a --helper-direct-store --json
```

Do not mark `read_transport_capability=validated` until a real heartbeat completes both helper execution and direct snapshot reads.

## 2026-05-06 Attempt: Direct Helper Blocked At SQLite WAL Setup

Result: `VALIDATION_FAILED`

Evidence:

- Code branch: `codex/desktop-direct-helper-preflight`
- Local binary: `/Users/hoteng/.cache/cargo-target/release/cbth`
- Local binary version: `cbth 0.1.0`
- Bridge thread id: `019db5e6-ba6a-7b80-95d2-a6867163281a`
- Temporary heartbeat automation id: `cbth-desktop-direct-helper-preflight-validation`
- Validation marker: `CBTH_DESKTOP_PREFLIGHT_20260506_DIRECT_HELPER_V1`
- Target rollout file: `/Users/hoteng/.codex/sessions/2026/04/22/rollout-2026-04-22T16-54-50-019db5e6-ba6a-7b80-95d2-a6867163281a.jsonl`
- Heartbeat run timestamp: `2026-05-06T18:17:37.188Z`

The local shell direct-helper path succeeded with the same final binary and published snapshot revision `019dfe81-3d0d-7de1-983f-0dca71faa6a5`.

The Desktop heartbeat executed the direct-helper prompt and got past daemon autostart, `startup.lock`, and Unix socket access. It failed while opening the store and enabling SQLite WAL mode:

```text
CBTH_DESKTOP_PREFLIGHT_20260506_DIRECT_HELPER_V1 VALIDATION_FAILED step2_bridge_preflight_failed:enable sqlite WAL journal mode: unable to open database file: Error code 14: Unable to open the database file
```

Local POSIX permission inspection after the failure showed the cbth home, database, inbox directory, and current snapshot were private and owned by the current user:

```text
/Users/hoteng/.cbth type=directory mode=drwx------ uid=501 gid=20
/Users/hoteng/.cbth/cbth.sqlite3 type=regular file mode=-rw------- uid=501 gid=20
/Users/hoteng/.cbth/inbox type=directory mode=drwx------ uid=501 gid=20
/Users/hoteng/.cbth/inbox/current-snapshot.json type=regular file mode=-rw------- uid=501 gid=20
```

`cbth.sqlite3-wal` and `cbth.sqlite3-shm` were not present after the failed heartbeat run. The failure is therefore still not explained by ordinary POSIX mode / ownership drift. The next blocker is Desktop heartbeat access to SQLite WAL setup under `~/.cbth`; `read_transport_capability` remains unvalidated.

The heartbeat thread reported that the temporary automation was deleted after the failed run.

The durable installation state must remain unchanged:

```text
read_transport_capability=unknown
artifact_read_capability=unknown
writeback_capability=unknown
```

## Next Retest Condition From SQLite WAL Blocker

The direct-helper publisher path proved Desktop heartbeat can execute `cbth` without daemon autostart, `startup.lock`, or Unix socket access, but it still failed when the heartbeat process tried to open SQLite and enable WAL mode.

The next validation should stop asking Desktop heartbeat to publish snapshots. A normal shell, daemon, or future non-Desktop sidecar should publish the inbox snapshot first; the heartbeat should only run no-DB read helpers:

```bash
cbth desktop read-snapshot --bridge-thread-id 019db5e6-ba6a-7b80-95d2-a6867163281a --json
cbth desktop list-arm-pending --bridge-thread-id 019db5e6-ba6a-7b80-95d2-a6867163281a --json
cbth desktop list-pause-due --bridge-thread-id 019db5e6-ba6a-7b80-95d2-a6867163281a --json
cbth desktop claim-next-ready --bridge-thread-id 019db5e6-ba6a-7b80-95d2-a6867163281a --json
```

Do not mark `read_transport_capability=validated` until a real heartbeat completes those read-only helpers against a freshly published snapshot without approval. This validation still does not cover `artifact_read_capability` or `writeback_capability`.

## 2026-05-07 Attempt: No-DB Inbox Read Helpers Succeeded

Result: `VALIDATION_OK`

Evidence:

- Code branch: `codex/desktop-no-db-inbox-reader`
- Base merge commit: `98dc2b42f8edd595d1ede6dd846634ab09a86779`
- Local binary: `/Users/hoteng/.cache/cargo-target/release/cbth`
- Local binary version: `cbth 0.1.0`
- Bridge thread id: `019db5e6-ba6a-7b80-95d2-a6867163281a`
- Successful heartbeat automation id: `cbth-desktop-no-db-inbox-read-validation-v4`
- Successful validation marker: `CBTH_DESKTOP_NO_DB_INBOX_READ_V4`
- Target rollout file: `/Users/hoteng/.codex/sessions/2026/04/22/rollout-2026-04-22T16-54-50-019db5e6-ba6a-7b80-95d2-a6867163281a.jsonl`
- Heartbeat run timestamp: `2026-05-07T08:30:07.707Z`

The local shell first published a fresh snapshot through the direct-helper publisher path:

```text
snapshot_revision=019e0188-51de-77a2-87e9-af4a6cd15379
snapshot_manifest_path=/Users/hoteng/.cbth/inbox/current-snapshot.json
installation_state_path=/Users/hoteng/.cbth/inbox/desktop-installation-state.json
```

The first three no-DB heartbeat retries proved the commands executed but the validation prompt used the wrong JSON paths. Those runs failed at parser checks only:

```text
CBTH_DESKTOP_PREFLIGHT_20260507_NO_DB_INBOX_READ VALIDATION_FAILED step5_inconsistent_json
CBTH_DESKTOP_NO_DB_INBOX_READ_V2 VALIDATION_FAILED step9_read_snapshot_missing_required_sections
CBTH_DESKTOP_NO_DB_INBOX_READ_V3 VALIDATION_FAILED step9_read_transport_not_direct_file_read:
```

The fourth retry used the exact wrapper / nested JSON paths and succeeded:

```text
CBTH_DESKTOP_NO_DB_INBOX_READ_V4 VALIDATION_OK snapshot_revision=019e0188-51de-77a2-87e9-af4a6cd15379 ready_path=/Users/hoteng/.cbth/inbox/snapshots/019e0188-51de-77a2-87e9-af4a6cd15379/ready-threads.json arm_path=/Users/hoteng/.cbth/inbox/snapshots/019e0188-51de-77a2-87e9-af4a6cd15379/arm-pending-bindings.json pause_path=/Users/hoteng/.cbth/inbox/snapshots/019e0188-51de-77a2-87e9-af4a6cd15379/pause-due-bindings.json installation_path=/Users/hoteng/.cbth/inbox/desktop-installation-state.json arm_entries=0 pause_entries=0 claim_entry=null
```

The follow-up V5 retry used a build that publishes a revision-specific installation-state export and requires `read-snapshot` to consume that manifest-referenced file instead of the latest-only singleton:

```text
CBTH_DESKTOP_NO_DB_INBOX_READ_V5 VALIDATION_OK snapshot_revision=019e01a8-82e2-7701-be48-cf1a35933253 ready_path=/Users/hoteng/.cbth/inbox/snapshots/019e01a8-82e2-7701-be48-cf1a35933253/ready-threads.json arm_path=/Users/hoteng/.cbth/inbox/snapshots/019e01a8-82e2-7701-be48-cf1a35933253/arm-pending-bindings.json pause_path=/Users/hoteng/.cbth/inbox/snapshots/019e01a8-82e2-7701-be48-cf1a35933253/pause-due-bindings.json installation_path=/Users/hoteng/.cbth/inbox/snapshots/019e01a8-82e2-7701-be48-cf1a35933253/desktop-installation-state.json arm_entries=0 pause_entries=0 claim_entry=null
```

This validates that a real Desktop heartbeat can execute the no-DB read helpers without approval and consume a revision-consistent, already-published inbox snapshot through direct file read, including the revision-specific installation-state export. It does not validate Desktop heartbeat access to SQLite, daemon sockets, `startup.lock`, artifact payloads, or writeback helpers.

After the successful heartbeat run, the local installation state was repaired:

```text
read_transport_capability=validated
artifact_read_capability=unknown
writeback_capability=unknown
read_transport_generation=1
validated_at=1778142693
validation_fingerprint=cbth_version=0.1.0;os=macos;arch=aarch64;exe=/Users/hoteng/.cache/cargo-target/release/cbth;inbox_schema=1;read_transport=direct_file_read
```

After the V5 retry, the same repair command was a no-op (`changed=false`, `degraded_bindings=0`), confirming the local installation still records `read_transport_capability=validated` with artifact and writeback capabilities unchanged at `unknown`.

A follow-up local preflight refreshed the inbox export after the repair:

```text
snapshot_revision=019e0190-6ceb-7cd3-91bd-ae17c82383ed
read_transport_capability=validated
artifact_read_capability=unknown
writeback_capability=unknown
```
