# Repository Guidelines

- Keep repo-specific policy here. Cross-repo behavior belongs in the personal `AGENTS.md`.
- Run Python in this repo via `uv` only. Use forms like `uv run python ...` or `uv run <script>`. Do not use bare `python`, `pip`, or ad-hoc virtualenv management.
- Keep Python limited to lightweight tooling, probes, and prototypes. Implement long-running or production-facing components such as sidecars, supervisors, and reusable CLIs in Rust unless the user asks otherwise.
- Do not modify the upstream `codex` repository from this repo. This project is for external integrations, reference PoCs, and companion tooling only.
- Keep `docs/PROJECT_STATE.md` and `docs/PROJECT_TODO.md` aligned with meaningful architectural or experimental changes.
