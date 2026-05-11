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

## 2026-05-07 Attempt: Writeback Helpers Blocked By Startup Lock

Result: `VALIDATION_FAILED`

Evidence:

- Code branch: `codex/desktop-writeback-live-evidence`
- Base merge commit: `2fd0e0d95a69f14d593fdbee58d6de59b2b0990c`
- Local binary: `/Users/hoteng/.cache/cargo-target/release/cbth`
- Local binary version: `cbth 0.1.1` built from current `master`
- Bridge thread id: `019db5e6-ba6a-7b80-95d2-a6867163281a`
- Heartbeat automation id: `cbth-desktop-writeback-live-validation-20260507t195236z`
- Validation marker: `CBTH_DESKTOP_WRITEBACK_HELPER_LIVE_20260507T195236Z`
- Target rollout file: `/Users/hoteng/.codex/sessions/2026/04/22/rollout-2026-04-22T16-54-50-019db5e6-ba6a-7b80-95d2-a6867163281a.jsonl`
- Heartbeat run timestamp: `2026-05-07T19:55:11.222Z`

The operator shell created a validation-only fixture:

```text
source_thread_id=cbth-desktop-writeback-live-validation-20260507T195236Z
caller_automation_id=cbth-desktop-writeback-live-validation-20260507T195236Z
bridge_request_id=CBTH_DESKTOP_WRITEBACK_HELPER_LIVE_20260507T195236Z
job_id=019e0400-eee1-7c73-8792-35f8e015a572
batch_id=019e0400-eee1-7c73-8792-360120454fc5
attempt_id=019e0400-eee1-7c73-8792-361330f0b674
attempt_state=prepared
attempt_generation=1
requires_artifact_read=false
created_at=2026-05-07T19:53:50Z
```

The first heartbeat writeback command failed before any durable writeback state changed:

```text
CBTH_DESKTOP_WRITEBACK_HELPER_LIVE_20260507T195236Z VALIDATION_FAILED step1_note_arm_pending_first_failed:open startup lock /Users/hoteng/.cbth/run/startup.lock: Operation not permitted (os error 1)
```

The heartbeat deleted the temporary automation after reporting the failure. Operator inspection afterwards showed:

```text
attempt_state=prepared
desktop_armed_at=null
bridge_request_id=null
bridge_arm_lease_id=null
batch_state=open
delivery_attempt_count=0
```

The operator then closed the synthetic fixture batch:

```text
close_reason=operator_closed_unconfirmed
closed_at=2026-05-07T19:56:09Z
close_note=Desktop writeback helper live validation failed before note-arm-pending due Desktop startup.lock EPERM
```

The installation state remains:

```text
read_transport_capability=validated
artifact_read_capability=unknown
writeback_capability=unknown
read_transport_generation=1
```

Conclusion: real Desktop heartbeat can execute `cbth`, but the current daemon-routed writeback helpers still hit the same `startup.lock` sandbox boundary before reaching `note-arm-pending`. Do not mark `writeback_capability=validated`. The next design step must avoid heartbeat-owned daemon autostart / startup-lock access for writeback, and should also account for the earlier evidence that heartbeat-owned SQLite WAL setup failed.

## 2026-05-11 Attempt: Writeback Dropbox Local FS Probe Blocked

Result: `VALIDATION_FAILED`

Evidence:

- Code branch: `codex/desktop-writeback-dropbox-probe`
- Base merge commit: `2b4ea029aa2a665b142947d644ebee53ce5a56e6`
- Local binary: `/Users/hoteng/.cache/cargo-target/release/cbth`
- Local binary version: `cbth 0.1.5` built from current branch
- Bridge thread id: `019db5e6-ba6a-7b80-95d2-a6867163281a`
- Target rollout file: `/Users/hoteng/.codex/sessions/2026/04/22/rollout-2026-04-22T16-54-50-019db5e6-ba6a-7b80-95d2-a6867163281a.jsonl`

The validation-only hidden command was:

```text
cbth desktop validation writeback-dropbox-probe --bridge-thread-id <bridge-thread-id> --probe-id <probe-id> --marker <marker> --json
```

It does not open SQLite, connect to daemon IPC, autostart the daemon, or touch `startup.lock`.

First heartbeat attempt tested create-new mode with no pre-existing dropbox directory:

```text
automation_id=cbth-desktop-writeback-dropbox-probe-20260511t094529z
marker=CBTH_DESKTOP_WRITEBACK_DROPBOX_PROBE_20260511T094529Z
result=VALIDATION_FAILED step1_command_failed:create directory /Users/hoteng/.cbth/inbox/writeback-dropbox: Operation not permitted (os error 1)
heartbeat_timestamp=2026-05-11T09:46:59.345Z
```

Second heartbeat attempt tested create-new mode after the operator pre-created the dropbox directory:

```text
automation_id=cbth-desktop-writeback-dropbox-probe-existing-dir-20260511t094809z
marker=CBTH_DESKTOP_WRITEBACK_DROPBOX_EXISTING_DIR_20260511T094809Z
result=VALIDATION_FAILED step1_command_failed:create file /Users/hoteng/.cbth/inbox/writeback-dropbox/probes/cbth_desktop_writeback_dropbox_existing_dir_20260511T094809Z.json: Operation not permitted (os error 1)
heartbeat_timestamp=2026-05-11T09:49:27.850Z
```

Third heartbeat attempt tested `--append-existing` after the operator pre-created an empty private probe file:

```text
automation_id=cbth-desktop-writeback-dropbox-append-existing-20260511t094809z
marker=CBTH_DESKTOP_WRITEBACK_DROPBOX_APPEND_EXISTING_20260511T094809Z
result=VALIDATION_FAILED step1_command_failed:open existing probe file /Users/hoteng/.cbth/inbox/writeback-dropbox/probes/cbth_desktop_writeback_dropbox_append_existing_20260511T094809Z.json: Operation not permitted (os error 1)
heartbeat_timestamp=2026-05-11T09:54:28.433Z
```

Operator inspection after the append-existing attempt showed the path was present, private, and still empty:

```text
drwx------ /Users/hoteng/.cbth/inbox
drwx------ /Users/hoteng/.cbth/inbox/writeback-dropbox
drwx------ /Users/hoteng/.cbth/inbox/writeback-dropbox/probes
-rw------- 0 bytes /Users/hoteng/.cbth/inbox/writeback-dropbox/probes/cbth_desktop_writeback_dropbox_append_existing_20260511T094809Z.json
```

All temporary heartbeat automations were deleted after the attempts. The installation state remains:

```text
read_transport_capability=validated
artifact_read_capability=unknown
writeback_capability=unknown
```

Conclusion: real Desktop heartbeat can execute no-DB read helpers, but cannot create directories, create files, or open pre-created files for append under `~/.cbth/inbox/writeback-dropbox`. Local filesystem writeback from Desktop heartbeat is not viable for v1. The next writeback design should use a non-filesystem side channel or a Desktop-exposed tool result / automation mechanism instead of heartbeat-authored local files.

## 2026-05-11 Attempt: Interactive Desktop Transcript Relay Succeeded

Result: `VALIDATION_OK` for the Desktop tool-output carrier. This validates the transcript relay scanner and the `function_call_output` carrier in a real Desktop thread.

Evidence:

- Code branch: `codex/desktop-transcript-relay-validation`
- Base merge commit: `eeef583099d22af196e9525aa80de2d4a4cd5397`
- Local binary: `/Users/hoteng/.cache/cargo-target/release/cbth`
- Local binary version: `cbth 0.1.5`
- Interactive Desktop thread id: `019db478-a40c-7a62-b8d0-70ef2c3249d1`
- Validation marker: `CBTH_TRANSCRIPT_RELAY_INTERACTIVE_20260511T132728Z`
- Probe id: `cbth_transcript_relay_interactive_20260511T132728Z`
- Rollout file: `/Users/hoteng/.codex/sessions/2026/04/22/rollout-2026-04-22T10-14-58-019db478-a40c-7a62-b8d0-70ef2c3249d1.jsonl`
- Trusted carrier line: `90911`

The validation-only emitter wrote one prefixed stdout envelope:

```text
CBTH_TRANSCRIPT_WRITEBACK_V1 {"bridge_thread_id":"019db478-a40c-7a62-b8d0-70ef2c3249d1","cbth_version":"0.1.5","channel":"desktop_transcript_writeback","created_at":1778506053,"kind":"validation_probe","marker":"CBTH_TRANSCRIPT_RELAY_INTERACTIVE_20260511T132728Z","probe_id":"cbth_transcript_relay_interactive_20260511T132728Z","schema_version":1}
```

The scanner accepted the rollout carrier as trusted automatic input:

```text
auto_decision.trusted=true
auto_decision.reason=single_trusted_auto_envelope
counts.trusted_auto=1
counts.diagnostic_only=0
counts.ignored_prompt=0
counts.rejected=0
trusted_auto[0].payload_type=function_call_output
```

The first live scan also found a scanner defect: copied prefix text in non-trusted carrier contexts could make the command fail while parsing diagnostic text. The branch fixes that behavior so malformed prefix text in `diagnostic_only` remains non-automatic diagnostic evidence, while malformed prefixed envelopes in `function_call_output` still fail closed through `rejected_trusted_auto_envelopes`.

Conclusion: transcript/tool-output relay is a viable side-channel candidate for Desktop writeback, provided a future sidecar only consumes exact prefixed envelopes from `function_call_output` and adds durable scan cursors, replay protection, high-entropy nonce / lease / generation validation, and CAS mutation. Later work added a manual non-Desktop consumer foundation with marker/hash replay protection and CAS mutation for arm envelopes, but this evidence still validates transport shape only. `writeback_capability` remains `unknown`.

## 2026-05-11 Attempt: Heartbeat Automation Transcript Relay Succeeded

Result: `VALIDATION_OK` for the Desktop heartbeat automation carrier. This validates that automation-delivered helper stdout uses the same trusted `response_item.payload.type=function_call_output` carrier as the interactive probe.

Evidence:

- Code branch: `codex/desktop-transcript-relay-validation`
- Base merge commit: `eeef583099d22af196e9525aa80de2d4a4cd5397`
- Local binary: `/Users/hoteng/.cache/cargo-target/release/cbth`
- Local binary version: `cbth 0.1.5`
- Heartbeat thread id: `019db5e6-ba6a-7b80-95d2-a6867163281a`
- Temporary heartbeat automation id: `cbth-transcript-relay-heartbeat-validation-20260511t141427z`
- Validation marker: `CBTH_TRANSCRIPT_RELAY_HEARTBEAT_20260511T141427Z`
- Probe id: `cbth_transcript_relay_heartbeat_20260511T141427Z`
- Rollout file: `/Users/hoteng/.codex/sessions/2026/04/22/rollout-2026-04-22T16-54-50-019db5e6-ba6a-7b80-95d2-a6867163281a.jsonl`
- Trusted carrier line: `444`
- Temporary automation cleanup: deleted after validation

The scanner accepted the heartbeat rollout carrier as trusted automatic input:

```text
auto_decision.trusted=true
auto_decision.reason=single_trusted_auto_envelope
counts.trusted_auto=1
counts.diagnostic_only=6
counts.ignored_prompt=4
counts.rejected=0
trusted_auto[0].payload_type=function_call_output
trusted_auto[0].envelope.marker=CBTH_TRANSCRIPT_RELAY_HEARTBEAT_20260511T141427Z
trusted_auto[0].envelope.bridge_thread_id=019db5e6-ba6a-7b80-95d2-a6867163281a
```

The prompt copies of the marker appeared as `ignored_prompt`, and assistant / task-complete text appeared as `diagnostic_only`. This is the expected shape: only the exact helper stdout envelope inside `function_call_output` is eligible for future automatic writeback consumption.

Conclusion: the transcript/tool-output side channel is validated for both interactive Desktop tool calls and heartbeat automation runs. `writeback_capability` still remains `unknown`; later consumer-foundation work covers manual marker/hash replay protection and CAS mutation, but production sidecar tailing, replay cursor management, nonce issuance, and live arm-envelope validation remain incomplete.
