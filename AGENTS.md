# Repository Guidelines

- Keep repo-specific policy here. Cross-repo behavior belongs in the personal `AGENTS.md`.
- Run Python in this repo via `uv` only. Use forms like `uv run python ...` or `uv run <script>`. Do not use bare `python`, `pip`, or ad-hoc virtualenv management.
- Keep Python limited to lightweight tooling, probes, and prototypes. Implement long-running or production-facing components such as sidecars, supervisors, and reusable CLIs in Rust unless the user asks otherwise.
- For production Rust components, prioritize correctness first, then long-running reliability, then resource efficiency. Avoid designs that accumulate unbounded memory, tasks, file handles, child processes, or polling work; prefer bounded queues, durable checkpoints, explicit cleanup, and low idle CPU/memory overhead.
- Do not modify the upstream `codex` repository from this repo. This project is for external integrations, reference PoCs, and companion tooling only.
- Keep `docs/PROJECT_STATE.md` and `docs/PROJECT_TODO.md` as concise entrypoints, not giant status files; durable per-workstream state belongs in `docs/project_journal/YYYY/MM/*.md`.
- Treat generated `docs/project_journal/INDEX.md` as local ignored convenience output; do not commit it.
- Keep user-facing documentation bilingual once a document is in the public guide set. Use `en-GB` and `zh-CN` language variants with matching structure and bidirectional language links.
- Write language-switcher labels for the reader who would use that link: in `en-GB` docs, link to Chinese as `简体中文 (zh-CN)`; in `zh-CN` docs, link to English as `British English (en-GB)`.
- During an explicitly stacked documentation migration, an intermediate PR may introduce or rewrite the `en-GB` guide set first if the follow-up PR adds the matching `zh-CN` files before the migration is considered complete. Do not add bidirectional language links until both files in a pair exist.
- Keep unsuffixed `README.md` files as the default `en-GB` entrypoints when a platform expects that name; add `README.zh-CN.md` next to them instead of replacing the default README with a symlink.
- Use language suffixes for other user-facing Markdown guides, for example `docs/USAGE.en-GB.md` and `docs/USAGE.zh-CN.md`.
- Keep internal documentation under `docs/design/`, `docs/plans/`, and `docs/validation/` unless it is promoted into the user-facing guide set. Internal design records, plans, validation notes, `docs/PROJECT_STATE.md`, `docs/PROJECT_TODO.md`, and `docs/project_journal/**` are not required to be bilingual.
- Use `$cbth-change-checklist` for repo changes that affect user-facing CLI/docs, public output/schema, architecture/design docs, validation docs, or PR delivery checks.
- This repo is squash-merge-only. Tracked journal docs should describe target-branch state after the PR lands: if the PR completes the workstream, mark the journal `status: completed` before merge and use the PR link as evidence.
- Keep transient PR states such as "waiting for merge", "not merged yet", or "ready for review" in the PR body/checklist/comments, not tracked docs. If a PR only completes part of a workstream, keep the journal `active` or `blocked` and record the real remaining next steps.
- If a journal entry is marked as a legacy or verbatim snapshot, preserve copied historical content exactly; add navigational summaries beside it instead of rewriting archived relative links.
- Commits created by Codex in this repo must include the footer `Co-authored-by: Codex (model=GPT-5) <codex@openai.com>`.
