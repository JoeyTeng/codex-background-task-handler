---
id: 20260511-2b4ea02-desktop-writeback-dropbox-probe
title: Desktop Writeback Dropbox Probe
status: blocked
created: 2026-05-11
updated: 2026-05-11
branch: codex/desktop-writeback-dropbox-probe
pr:
supersedes:
  - 20260507-922c89c-desktop-writeback-live-validation
superseded_by:
---

# Desktop Writeback Dropbox Probe

## Summary

- Real Desktop heartbeat writeback validation failed before `note-arm-pending` because the daemon-routed helper attempted to open `~/.cbth/run/startup.lock` and hit Desktop sandbox `EPERM`.
- The next narrow experiment is a no-DB / no-daemon / no-socket / no-startup-lock file-write probe.
- The probe tests whether heartbeat can create a validation-only request file under `~/.cbth/inbox/writeback-dropbox/probes/`, and if create-new is blocked, whether heartbeat can append to an operator / sidecar pre-created probe file.
- This work does not enable Desktop automatic delivery and does not set `writeback_capability=validated`.

## Planned Changes

- Add hidden command:
  `cbth desktop validation writeback-dropbox-probe --bridge-thread-id <id> --probe-id <id> --marker <marker> --json`.
- The command writes one JSON probe file with `create_new` semantics and fails on duplicate `probe_id`.
- `--append-existing` appends the same JSON object to a pre-created probe file and fails if the file is missing.
- The command must not open SQLite, connect to daemon IPC, autostart daemon, or touch `startup.lock`.
- Add fake/default tests proving create-new success, append-existing success, duplicate rejection, missing append target rejection, invalid path rejection, private file permissions, and no store / startup-lock side effects.
- Add validation documentation for a real Desktop heartbeat run.

## Validation Plan

- Local gate: `cargo fmt --all -- --check`, `cargo clippy --locked --all-targets -- -D warnings`, `cargo test --test desktop_foundation --locked`, `cargo test --locked`, project journal validate, and `git diff --check`.
- Live probe: run the new hidden command from real Desktop heartbeat using a unique marker, then read back the returned path.
- If live probe succeeds, record evidence and design a request-consumer path where heartbeat writes a file and a non-Desktop daemon / sidecar performs the real CAS.
- If live probe fails, record the sandbox blocker and keep `writeback_capability=unknown`.

## Evidence

- Base branch: `master`
- Base commit: `2b4ea029aa2a665b142947d644ebee53ce5a56e6`
- Writeback blocker evidence: [Desktop live preflight evidence](../../../validation/DESKTOP_LIVE_PREFLIGHT_EVIDENCE.md#2026-05-07-attempt-writeback-helpers-blocked-by-startup-lock)
- Dropbox probe evidence: [Desktop live preflight evidence](../../../validation/DESKTOP_LIVE_PREFLIGHT_EVIDENCE.md#2026-05-11-attempt-writeback-dropbox-local-fs-probe-blocked)
- Validation instructions: [Desktop writeback dropbox probe validation](../../../validation/DESKTOP_WRITEBACK_DROPBOX_PROBE_VALIDATION.md)

## Current State

- Fake/default tests prove create-new and append-existing probe behavior without daemon autostart or SQLite.
- Real Desktop heartbeat failed create-new mode before writing because it could not create `~/.cbth/inbox/writeback-dropbox`.
- After the operator pre-created the directory, real Desktop heartbeat still failed to create a probe file under `~/.cbth/inbox/writeback-dropbox/probes`.
- After the operator pre-created an empty private probe file, real Desktop heartbeat still failed to open the existing file for append.
- `writeback_capability` remains `unknown`.

## Next Steps

- Do not pursue local filesystem writeback from Desktop heartbeat for v1.
- Evaluate a non-filesystem Desktop writeback side channel, such as a Desktop-exposed tool result / automation mechanism that can return structured output to a non-Desktop consumer.
- Keep the hidden dropbox probe as a reproducible harness in case a future Desktop sandbox change alters local write behavior.
