---
id: 20260507-98dc2b4-desktop-no-db-reader
title: Desktop No-DB Inbox Reader
status: completed
created: 2026-05-07
updated: 2026-05-07
branch: codex/desktop-no-db-inbox-reader
pr:
supersedes: []
superseded_by:
---

# Desktop No-DB Inbox Reader

## Summary

- The next Desktop bridge slice is a no-DB, no-daemon, no-write inbox reader for Desktop heartbeat agents.
- Live Desktop evidence showed heartbeat can execute `cbth`, but cannot reliably touch daemon sockets, `startup.lock`, or SQLite WAL under `~/.cbth`.
- This work keeps snapshot publishing outside the Desktop heartbeat sandbox and lets heartbeat consume already-published inbox JSON through stable read-only `cbth desktop ...` helpers.

## Planned Changes

- Add read-only Desktop helpers:
  - `cbth desktop read-snapshot --bridge-thread-id <thread-id> --json`
  - `cbth desktop list-arm-pending --bridge-thread-id <thread-id> --json`
  - `cbth desktop list-pause-due --bridge-thread-id <thread-id> --json`
  - `cbth desktop claim-next-ready --bridge-thread-id <thread-id> --json`
- The helpers must only read `~/.cbth/inbox/current-snapshot.json`, the referenced revision snapshots, and `desktop-installation-state.json`.
- The helpers must not open SQLite, connect the daemon, autostart, create `startup.lock`, write files, or depend on hidden `--direct-store`.
- Shared reader validation must fail closed on schema mismatch, revision mismatch, bridge thread mismatch, missing files, malformed JSON, oversized files, or referenced paths outside the cbth inbox.
- `claim-next-ready` is v1 read/peek only: return the first ready entry or `null`; do not reserve, hide, or mutate durable state.

## Validation Plan

- Unit/integration coverage must prove the helpers can consume snapshots published by existing `bridge-preflight`.
- Tests must prove the helpers still work when SQLite cannot be opened but valid inbox files already exist, and that no `run/startup.lock` is created.
- Failure tests must cover missing manifest, malformed JSON, oversized files, revision mismatch, bridge-thread mismatch, and path escape.
- Local gate before PR: `cargo fmt --all -- --check`, `cargo clippy --locked --all-targets -- -D warnings`, `cargo test --test desktop_foundation --locked`, `cargo test --locked`, `cargo test`, project journal validate, `git diff --check`, and helper-backed `codex-review`.
- Live validation should publish a snapshot from a normal shell, then have real Desktop heartbeat run the read-only helpers. Only a successful heartbeat read should justify manually repairing `read_transport_capability=validated`; artifact and writeback capabilities remain `unknown`.

## Current State

- `cbth desktop read-snapshot`, `list-arm-pending`, `list-pause-due`, and read/peek `claim-next-ready` are implemented as no-DB, no-daemon, no-write inbox readers.
- The shared reader validates the current manifest, revision-specific snapshots, installation-state export, schema version, bridge thread id, snapshot revision, manifest paths, regular-file type, JSON size limit, and entry counts before returning data.
- Fake integration coverage proves the helpers can consume published snapshots even when SQLite is no longer openable and that they fail closed on missing, malformed, oversized, mismatched, or path-escaped snapshot inputs.
- Real Desktop heartbeat validation succeeded with the no-DB helper path on 2026-05-07. The local installation was repaired to `read_transport_capability=validated`; `artifact_read_capability` and `writeback_capability` remain `unknown`.

## Evidence

- Base merge commit: `98dc2b42f8edd595d1ede6dd846634ab09a86779`
- Prior live evidence: [Desktop live preflight evidence](../../../DESKTOP_LIVE_PREFLIGHT_EVIDENCE.md)
- Foundation design: [Desktop bridge foundation](../../../DESKTOP_BRIDGE_FOUNDATION.md)
- Live validation marker: `CBTH_DESKTOP_NO_DB_INBOX_READ_V4 VALIDATION_OK`
- Live validation snapshot revision: `019e0188-51de-77a2-87e9-af4a6cd15379`
- Post-repair exported snapshot revision: `019e0190-6ceb-7cd3-91bd-ae17c82383ed`
- Local validation: `cargo fmt --all -- --check`; `cargo clippy --locked --all-targets -- -D warnings`; `cargo test --test desktop_foundation --locked`; `cargo test --locked`; `cargo test`; project journal validate; `git diff --check`
