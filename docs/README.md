# Documentation

This directory holds design notes, live validation records, project tracking entrypoints, and durable project journal records.

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

## Design And Validation

Use the root [README.md](../README.md) for the main status summary and links to the core design documents.
