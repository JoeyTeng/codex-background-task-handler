# Desktop Writeback Dropbox Probe Validation

This document records the narrow Desktop writeback file-write probe. The goal is only to decide whether a real Codex Desktop heartbeat can create a validation-only file under the cbth inbox without touching daemon autostart, `startup.lock`, Unix sockets, or SQLite.

This probe does not enable Desktop automatic delivery, does not mark `writeback_capability=validated`, and does not replace the durable `note-arm-pending` / `note-arm` CAS contract.

Current live result: on 2026-05-11, real Desktop heartbeat failed create-new mode without a directory, create-new mode with a pre-created directory, and append-existing mode with a pre-created empty file. Keep this document as a reproducible harness, but do not treat local filesystem writeback from Desktop heartbeat as viable unless a future Desktop sandbox change invalidates that evidence. See [DESKTOP_LIVE_PREFLIGHT_EVIDENCE.md](DESKTOP_LIVE_PREFLIGHT_EVIDENCE.md#2026-05-11-attempt-writeback-dropbox-local-fs-probe-blocked).

## Probe Surface

The hidden validation command is:

```bash
cbth desktop validation writeback-dropbox-probe \
  --bridge-thread-id <bridge-thread-id> \
  --probe-id <probe-id> \
  --marker <marker> \
  [--append-existing] \
  --json
```

It writes exactly one JSON file:

```text
~/.cbth/inbox/writeback-dropbox/probes/<probe-id>.json
```

Safety boundaries:

- It does not open SQLite.
- It does not connect to the daemon.
- It does not call daemon autostart.
- It does not open `~/.cbth/run/startup.lock`.
- Default mode uses `create_new` semantics and fails if the probe file already exists.
- `--append-existing` opens an already-created probe file and appends one JSON object; it fails if the file is missing.
- `probe_id` is a single safe path component containing only ASCII letters, digits, `-`, and `_`.
- The marker is bounded to 4 KiB and is written as JSON string data.

The command output returns:

- `.desktop_writeback_dropbox_probe.probe_id`
- `.desktop_writeback_dropbox_probe.bridge_thread_id`
- `.desktop_writeback_dropbox_probe.marker`
- `.desktop_writeback_dropbox_probe.path`
- `.desktop_writeback_dropbox_probe.created_at`
- `.desktop_writeback_dropbox_probe.bytes`
- `.desktop_writeback_dropbox_probe.write_mode`

## Operator Setup

Use a unique probe id and marker. The probe is intentionally independent from real caller threads and real Desktop delivery attempts.

Example values:

```text
bridge_thread_id=019db5e6-ba6a-7b80-95d2-a6867163281a
probe_id=cbth_desktop_writeback_dropbox_probe_20260511
marker=CBTH_DESKTOP_WRITEBACK_DROPBOX_PROBE_20260511
```

If testing a local unreleased binary, use an absolute binary path in the heartbeat prompt instead of relying on `PATH`.

## Heartbeat Prompt: Create-New Mode

Run this prompt in the real Codex Desktop heartbeat thread:

```text
Run this Desktop writeback dropbox probe. Do not modify repository files.

1. Run:
   <cbth-bin> desktop validation writeback-dropbox-probe \
     --bridge-thread-id <bridge-thread-id> \
     --probe-id <probe-id> \
     --marker <marker> \
     --json
2. Parse .desktop_writeback_dropbox_probe.path and .desktop_writeback_dropbox_probe.marker.
3. Read the returned path and confirm the JSON marker equals <marker>.
4. Reply with VALIDATION_OK plus the returned path and created_at if all checks passed.
5. If the command requires approval, fails, or the file cannot be read back, reply with VALIDATION_FAILED and the exact failed step and error.
```

Use the default home unless the operator intentionally passes `--home <path>` to both the command and the verification flow.

## Operator Verification

After heartbeat replies `VALIDATION_OK`, verify from a normal shell:

```bash
cbth desktop validation writeback-dropbox-probe \
  --bridge-thread-id <bridge-thread-id> \
  --probe-id <same-probe-id> \
  --marker duplicate \
  --json
```

The duplicate command should fail because the probe file already exists. This confirms the probe path is append-only for a given id.

Then inspect the returned probe file:

```bash
cat ~/.cbth/inbox/writeback-dropbox/probes/<probe-id>.json
```

Expected evidence:

- `schema_version` is `1`.
- `bridge_thread_id` matches the heartbeat bridge thread.
- `probe_id` matches the requested id.
- `marker` matches the requested marker.
- The file is private (`0600`) and parent directories are private (`0700`) on Unix platforms.

## Interpreting Results

If this probe succeeds in a future environment, Desktop heartbeat can write a narrow cbth-owned inbox file. The next design can use a heartbeat-authored request file and have a non-Desktop daemon / sidecar consume the request and execute the real store-backed CAS.

If this probe fails with permission, sandbox, atomic write, or readback errors, the Desktop writeback path should not depend on local filesystem writes from heartbeat. Keep `writeback_capability=unknown` and evaluate a non-filesystem side channel instead.

Even on success, do not set `writeback_capability=validated` from this probe alone. The validated writeback capability still requires an end-to-end request-consumption path that advances `note-arm-pending` / `note-arm` semantics without violating the Desktop sandbox boundary.

## Heartbeat Prompt: Existing-File Append Mode

If create-new mode fails because Desktop cannot create files, pre-create an empty probe file from operator shell:

```bash
mkdir -p ~/.cbth/inbox/writeback-dropbox/probes
chmod 700 ~/.cbth ~/.cbth/inbox ~/.cbth/inbox/writeback-dropbox ~/.cbth/inbox/writeback-dropbox/probes
touch ~/.cbth/inbox/writeback-dropbox/probes/<probe-id>.json
chmod 600 ~/.cbth/inbox/writeback-dropbox/probes/<probe-id>.json
```

Then run the same heartbeat prompt with `--append-existing` added to the command. This tests the more constrained design where a non-Desktop publisher predeclares request slots and heartbeat only writes into an existing file.

Interpretation:

- If append-existing succeeds, future Desktop writeback can use predeclared request files plus a daemon / sidecar consumer.
- If append-existing also fails, local filesystem writeback from Desktop heartbeat is not viable for v1.
