# Project State

## Current State

- 本仓用于在不修改上游 `codex` 的前提下验证和实现 `cbth` background task handler；当前主线已经完成 CLI dogfood v1 的主要能力、`v0.1.2` release，以及 Desktop bridge foundation。
- 顶层历史 tracker 已迁移到 project journal；完整旧记录保存在 [legacy snapshot](project_journal/2026/05/2026-05-05-legacy-tracker-snapshot-bbe4003.md)，完成历史摘要见 [completed work archive](project_journal/2026/05/2026-05-05-completed-work-archive-bbe4003.md)。Legacy snapshot 是逐字归档，内部复制的旧相对链接保留历史原貌；实时导航以本文件、TODO 和 archive summary 为准。
- 当前活跃工作集中在 Desktop bridge 自动投递闭环、CLI/daemon recovery contract 的剩余收口，以及外部 review / output / PR polling integrations；详见 [current follow-ups](project_journal/2026/05/2026-05-05-current-follow-ups-bbe4003.md)。
- Desktop heartbeat 已通过 no-DB helper path 验证 direct-file-read inbox 消费能力；Desktop writeback helper foundation 已有本地 fake coverage，writeback live validation harness 正在补齐，但尚未记录真实 heartbeat writeback success；`read_transport_capability=validated`，`artifact_read_capability` / `writeback_capability` 仍为 `unknown`。

## Active Handoff

- Phase: project-journal migration complete; implementation follow-ups remain active.
- Summary: `docs/PROJECT_STATE.md` and `docs/PROJECT_TODO.md` are now short entrypoints. Durable detail lives under `docs/project_journal/2026/05/`.
- Next Steps:
  - Continue from the active backlog in [current follow-ups](project_journal/2026/05/2026-05-05-current-follow-ups-bbe4003.md).
  - Desktop writeback live validation is tracked in [Desktop writeback helper live validation](project_journal/2026/05/2026-05-07-desktop-writeback-live-validation-922c89c.md).
  - Keep new workstream detail in focused journal entries instead of expanding these top-level files.
- Blockers:
  - Desktop automatic delivery is not yet live-validated end to end.
  - Desktop writeback helper live validation, boundary crossing, binding lifecycle cleanup, artifact reads, and several daemon deadline/recovery contracts remain incomplete.
- Evidence:
  - Release: `v0.1.2`
  - Design docs: [SHARED_CORE_ARCHITECTURE.md](SHARED_CORE_ARCHITECTURE.md), [DESKTOP_BRIDGE_FOUNDATION.md](DESKTOP_BRIDGE_FOUNDATION.md), [DESKTOP_LIVE_PREFLIGHT_VALIDATION.md](DESKTOP_LIVE_PREFLIGHT_VALIDATION.md), [DESKTOP_WRITEBACK_HELPER_LIVE_VALIDATION.md](DESKTOP_WRITEBACK_HELPER_LIVE_VALIDATION.md), [LIVE_E2E.md](LIVE_E2E.md)

## Risks Or Open Questions

- Desktop no-DB inbox reads are validated, but writeback capability and artifact-read capability are still `unknown`.
- Future `turn/steer`, artifact continuation, and post-output acknowledgement paths remain explicitly out of the supported automatic v1 path until their risk and observation contracts are implemented.
