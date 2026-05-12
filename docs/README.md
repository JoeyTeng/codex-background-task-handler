# Documentation

This directory contains user-facing guides, internal design records, implementation plans, validation notes, and project tracking entrypoints.

## User-Facing Guides

- [Usage guide](USAGE.en-GB.md) / [使用指南](USAGE.zh-CN.md)
- [Design overview](DESIGN_OVERVIEW.en-GB.md) / [设计概览](DESIGN_OVERVIEW.zh-CN.md)
- [Operator recovery](OPERATOR_RECOVERY.en-GB.md) / [操作恢复](OPERATOR_RECOVERY.zh-CN.md)
- [Development guide](DEVELOPMENT.en-GB.md) / [开发指南](DEVELOPMENT.zh-CN.md)
- [Live E2E guide](LIVE_E2E.en-GB.md) / [真实 E2E 指南](LIVE_E2E.zh-CN.md)

The repository root [README.md](../README.md) is the default `en-GB` entrypoint. Its `zh-CN` counterpart is [README.zh-CN.md](../README.zh-CN.md).

## Internal Records

- [design/](design/) contains architecture and implementation design records.
- [plans/](plans/) contains phased implementation and delivery plans.
- [validation/](validation/) contains live validation harnesses, probes, and evidence records.

These internal records are not required to be bilingual unless they are later promoted into the user-facing guide set.

## Project Tracking

- [PROJECT_STATE.md](PROJECT_STATE.md) is the concise current-state and handoff entrypoint.
- [PROJECT_TODO.md](PROJECT_TODO.md) is the concise cross-task backlog entrypoint.
- [project_journal/](project_journal/) contains durable per-workstream records under dated `YYYY/MM/*.md` entries.
- Generated `project_journal/INDEX.md` is local ignored output and must not be committed.

## Merge-Time Bookkeeping

This repo is squash-merge-only. Tracked journal docs should describe the target-branch state after the PR lands, not the temporary review state of the PR itself.

- If a PR fully completes a workstream, mark the relevant journal entry `status: completed` before merge and use the PR link as evidence.
- Do not leave tracked docs with transient states like "waiting for merge", "not merged yet", or "ready for review"; keep those in the PR body, checklist, or comments.
- If a PR only completes part of a larger workstream, keep the journal `active` or `blocked` and record the real remaining next steps.
