---
id: 20260509-41fb384-codex-130-pagination
title: Codex 0.130 Pagination
status: completed
created: 2026-05-09
updated: 2026-05-09
branch: wip/codex-130-pagination
pr:
supersedes: []
superseded_by:
---

# Codex 0.130 Pagination

## Summary

- Adapt managed CLI accepted-turn reconcile to prefer Codex 0.130 experimental `thread/turns/list` with `itemsView=notLoaded`.
- Keep the existing `thread/read(includeTurns=true)` reconcile path as a fail-safe fallback when the paginated method is unsupported or cannot locate the accepted turn.
- Treat `codex-cli 0.130.x` as the current soft validated range while leaving protocol parsing and capability gates fail-closed.
- Record that `codex remote-control` and non-loopback websocket auth are upstream 0.130 surfaces, but not part of the local v1 `cbth` safety model yet.

## Current State

- Accepted CLI auto-delivery observation first asks `thread/turns/list` for recent turns without loading full item payloads.
- A `-32601` / not-supported response marks the paginated method unsupported for the foreground managed session and falls back to `thread/read(includeTurns=true)`.
- Fresh first-turn materialization errors remain transient observation gaps rather than manual-resolution evidence.
- `cbth doctor cli` and managed startup now warn outside `codex-cli 0.130.x`.

## Validation

- `cargo fmt --all`
- `cargo test --locked thread_turns_list -- --nocapture`
- `cargo test --locked doctor_cli_warns_when_codex_version_is_outside_validated_range -- --nocapture`
- `cargo test --locked cli_run_trusted_all_auto_delivery_turns_list_reconcile_closes_delivered -- --nocapture`
- `cargo test --locked cli_run_trusted_all_auto_delivery_falls_back_when_turns_list_is_unsupported -- --nocapture`
- `cargo test --locked cli_run_trusted_all_auto_delivery_falls_back_when_turns_list_times_out -- --nocapture`
- `git diff --check`
- `cargo clippy --locked --all-targets -- -D warnings`
- `cargo test --locked`
- `uv run python /Users/hoteng/.codex/skills/project-journal/scripts/project_journal.py validate --repo /Users/hoteng/.codex/worktrees/eedb/codex-background-task-handler`

## Next Steps

- After this PR lands, create a separate small release PR to bump the 0.1.x patch version and publish the next release.
