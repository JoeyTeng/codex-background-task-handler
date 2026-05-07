---
id: 20260507-98dc2b4-desktop-no-db-reader
title: Desktop No-DB Inbox Reader
status: active
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

- This journal entry records the implementation plan as the first commit for the PR.
- Implementation, docs updates, tests, live validation, and final review remain to be completed.

## Evidence

- Base merge commit: `98dc2b42f8edd595d1ede6dd846634ab09a86779`
- Prior live evidence: [Desktop live preflight evidence](../../../DESKTOP_LIVE_PREFLIGHT_EVIDENCE.md)
- Foundation design: [Desktop bridge foundation](../../../DESKTOP_BRIDGE_FOUNDATION.md)
