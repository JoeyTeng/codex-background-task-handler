---
id: 20260510-41fb384
title: cbth CLI Operator UX
status: completed
created: 2026-05-10
updated: 2026-05-10
branch: wip/cbth-help-app-servers-update
pr:
supersedes: []
superseded_by:
---

# cbth CLI Operator UX

## Summary
- `cbth` now has clearer Clap help for the public command groups and visible operator parameters.
- `cbth cli app-servers` lists currently running daemon-owned Codex app-servers without daemon autostart; JSON is the default, and `--format human` prints a compact summary.
- `cbth self update --interactive` checks GitHub Releases and prompts with y/N before installing.

## Current State
- App-server JSON entries include Codex session id, managed session id, session epoch, websocket URL, pid, lease, start time in local time, cwd, title, and best-effort thread-info errors.
- The daemon status payload now exposes `session_epoch` for each managed CLI app-server.
- Interactive self-update reuses the same release resolution and checksum-verified install path as `--yes`; non-TTY use fails before network access and directs callers to `--yes` or `--check`.
- Internal review found that session-index title fallback should use the latest matching entry; the final implementation scans the full index and keeps the last matching title, with unit coverage.

## Next Steps
- None for this workstream; future commands can reuse the shared `--format json|human` enum if they need operator output.

## Evidence
- `cargo fmt --all -- --check`
- `cargo clippy --locked --all-targets -- -D warnings`
- `cargo test --locked`
- Internal review: helper-backed `codex-review` found one P3 fallback-title issue, fixed; follow-up `codex-readonly` baseline returned LGTM.
- External review: helper `bounded-semantic` / `opencode` returned LGTM.
