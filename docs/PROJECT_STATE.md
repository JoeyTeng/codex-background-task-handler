# Project State

## Current State

- 本仓用于在不修改上游 `codex` 的前提下验证和实现 `cbth` background task handler；当前主线已经完成 CLI dogfood v1 的主要能力、`v0.1.4` release，以及 Desktop bridge foundation。
- 顶层历史 tracker 已迁移到 project journal；完整旧记录保存在 [legacy snapshot](project_journal/2026/05/2026-05-05-legacy-tracker-snapshot-bbe4003.md)，完成历史摘要见 [completed work archive](project_journal/2026/05/2026-05-05-completed-work-archive-bbe4003.md)。Legacy snapshot 是逐字归档，内部复制的旧相对链接保留历史原貌；实时导航以本文件、TODO 和 archive summary 为准。
- 当前活跃工作集中在 Desktop bridge 自动投递闭环、CLI/daemon recovery contract 的剩余收口，以及外部 review / output / PR polling integrations；`cbth resume` 的 cwd、canonical permission profile、Codex CLI version warning follow-up 已收口，Codex 0.129 request-side permission profile selection 已补齐 stable built-in exact-match 优先与 legacy fallback，Codex 0.130 accepted-turn reconcile 已优先使用 `thread/turns/list` 并保留 `thread/read` fallback，CLI operator UX 已补齐 help、app-server listing 和 interactive self-update，详见 [resume follow-ups](project_journal/2026/05/2026-05-07-cbth-resume-followups-698664c.md)、[Codex 0.129 permission selection](project_journal/2026/05/2026-05-08-codex-129-permission-selection-6431a08.md)、[Codex 0.130 pagination](project_journal/2026/05/2026-05-09-codex-130-pagination-41fb384.md)、[cbth CLI operator UX](project_journal/2026/05/2026-05-10-cbth-cli-operator-ux-41fb384.md) 和 [current follow-ups](project_journal/2026/05/2026-05-05-current-follow-ups-bbe4003.md)。
- Desktop heartbeat 已通过 no-DB helper path 验证 direct-file-read inbox 消费能力；Desktop writeback helper foundation 已有本地 fake coverage，writeback live validation harness 正在补齐，但尚未记录真实 heartbeat writeback success；`read_transport_capability=validated`，`artifact_read_capability` / `writeback_capability` 仍为 `unknown`。

## Active Handoff

- Phase: CLI operator UX and `cbth resume` follow-up implementation complete; broader CLI/Desktop follow-ups remain active.
- Summary: The managed resume path now preserves native cwd behavior unless an explicit/interactive cwd is selected, parses Codex 0.129 tagged canonical permission profiles on read, pins Codex 0.129 `turn/start.permissions` when a stable built-in current active profile exactly matches the effective cap, falls back to legacy `sandboxPolicy` otherwise, treats `codex-cli 0.130.x` as the validated CLI range, uses Codex 0.130 paginated turn reads for accepted-turn reconcile when available, and `cbth` now has clearer help, `cli app-servers --format json|human` with `-H`, and `self update --interactive` with `-i`.
- Next Steps:
  - Continue from the active backlog in [current follow-ups](project_journal/2026/05/2026-05-05-current-follow-ups-bbe4003.md).
  - Desktop writeback live validation is tracked in [Desktop writeback helper live validation](project_journal/2026/05/2026-05-07-desktop-writeback-live-validation-922c89c.md).
  - Keep new workstream detail in focused journal entries instead of expanding these top-level files.
- Blockers:
  - Desktop automatic delivery is not yet live-validated end to end.
  - Desktop writeback helper live validation, boundary crossing, binding lifecycle cleanup, artifact reads, and several daemon deadline/recovery contracts remain incomplete.
- Evidence:
  - Release: `v0.1.4`
  - Latest completed Codex 0.130 note: [2026-05-09-codex-130-pagination-41fb384.md](project_journal/2026/05/2026-05-09-codex-130-pagination-41fb384.md)
  - Latest completed CLI operator UX note: [2026-05-10-cbth-cli-operator-ux-41fb384.md](project_journal/2026/05/2026-05-10-cbth-cli-operator-ux-41fb384.md)
  - Design docs: [SHARED_CORE_ARCHITECTURE.md](SHARED_CORE_ARCHITECTURE.md), [DESKTOP_BRIDGE_FOUNDATION.md](DESKTOP_BRIDGE_FOUNDATION.md), [DESKTOP_LIVE_PREFLIGHT_VALIDATION.md](DESKTOP_LIVE_PREFLIGHT_VALIDATION.md), [DESKTOP_WRITEBACK_HELPER_LIVE_VALIDATION.md](DESKTOP_WRITEBACK_HELPER_LIVE_VALIDATION.md), [LIVE_E2E.md](LIVE_E2E.md)

## Risks Or Open Questions

- Desktop no-DB inbox reads are validated, but writeback capability and artifact-read capability are still `unknown`.
- Future `turn/steer`, artifact continuation, and post-output acknowledgement paths remain explicitly out of the supported automatic v1 path until their risk and observation contracts are implemented.
