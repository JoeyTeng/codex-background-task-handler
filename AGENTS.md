# Repository Guidelines

- Keep repo-specific policy here. Cross-repo behavior belongs in the personal `AGENTS.md`.
- Run Python in this repo via `uv` only. Use forms like `uv run python ...` or `uv run <script>`. Do not use bare `python`, `pip`, or ad-hoc virtualenv management.
- Keep Python limited to lightweight tooling, probes, and prototypes. Implement long-running or production-facing components such as sidecars, supervisors, and reusable CLIs in Rust unless the user asks otherwise.
- For production Rust components, prioritize correctness first, then long-running reliability, then resource efficiency. Avoid designs that accumulate unbounded memory, tasks, file handles, child processes, or polling work; prefer bounded queues, durable checkpoints, explicit cleanup, and low idle CPU/memory overhead.
- Do not modify the upstream `codex` repository from this repo. This project is for external integrations, reference PoCs, and companion tooling only.
- Keep `docs/PROJECT_STATE.md` and `docs/PROJECT_TODO.md` aligned with meaningful architectural or experimental changes.
- Commits created by Codex in this repo must include the footer `Co-authored-by: Codex (model=GPT-5) <codex@openai.com>`.
