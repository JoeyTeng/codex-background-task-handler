---
name: cbth-change-checklist
description: Use when changing the cbth repository in ways that affect public CLI behavior, JSON or human operator output, CLI help, user-facing docs, design or architecture records, validation evidence, or PR delivery checks.
---

# CBTH Change Checklist

## Overview

Use this repo-local checklist before editing and again before finalizing a cbth change. It complements the broader `$change-delivery-workflow` by identifying cbth-specific docs, help, tests, and project-tracking expectations for the change type.

## Classify The Change

Start by naming every affected surface:

- `user-facing-cli`: public commands, flags, help text, stderr/stdout copy, install/update behavior, or operator workflows.
- `public-output`: JSON schemas, human `-H` output, audit/store-visible fields, or documented machine-readable contracts.
- `design-contract`: architecture, safety model, delivery routing, daemon/session lifecycle, or protocol behavior.
- `validation-evidence`: live validation procedure, evidence records, opt-in e2e, or diagnostic tooling.
- `repo-delivery`: PR body, review gate, branch/merge handling, or release-facing documentation.

## Apply The Checklist

- For `user-facing-cli`, update help text when behavior changes, add or adjust help/integration tests, and update matching `en-GB` and `zh-CN` user-facing guides when the behavior is documented.
- For `public-output`, test the normal shape plus empty/omitted and unsupported/error fallback cases. Cover human output separately when the field or wording is visible to operators.
- For `design-contract`, update the relevant `docs/design/` or `docs/plans/` record when the contract or architecture changes. Do not update design docs for cosmetic or purely diagnostic changes that leave the contract unchanged.
- For `validation-evidence`, update `docs/validation/` only when the validation procedure or durable evidence changes. Do not use validation docs as a generic changelog.
- For `repo-delivery`, confirm the diff against the intended base, list validations in the PR body, wait for Rust CI and `codex/review-gate`, and use this repo's squash-merge flow.

## Project Tracking

Do not let this checklist conflict with the project journal convention. `docs/PROJECT_STATE.md`, `docs/PROJECT_TODO.md`, and `docs/project_journal/**` are for durable workstream state, blockers, and post-merge target-branch truth. Keep transient PR states, checklist notes, "ready for review", "waiting for merge", and CI progress in the PR body, comments, or final response instead.

Only update project tracking when the current change genuinely changes durable project state or completes/blocks a tracked workstream. When in doubt, leave project journal files untouched and make the durable status explicit in the PR body instead.
