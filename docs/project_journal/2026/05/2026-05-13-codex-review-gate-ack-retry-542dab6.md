---
id: 20260513-542dab6-codex-review-gate-ack-retry
title: Codex Review Gate Ack Retry
status: completed
created: 2026-05-13
updated: 2026-05-13
branch: codex/review-gate-ack-retry
pr: https://github.com/JoeyTeng/codex-background-task-handler/pull/79
supersedes: []
superseded_by:
---

# Codex Review Gate Ack Retry

## Summary

- `codex-review-gate` 现在会区分 marker 未被 Codex ack 和 Codex 已 ack 但仍在 review 的状态。
- 未被 ack 的 marker 默认 300 秒后关闭为 `missed_ack` 并重发；连续 missed ack 在同一 head 上按 300、600、1200、1800 秒退避，之后封顶。
- Codex 一旦发出 `eyes`，marker 进入 `waiting_result`，继续使用原有 1 小时 result timeout。

## Current State

- Composite action 暴露 `marker-ack-timeout-seconds` 和 `marker-ack-timeout-max-seconds` 输入，默认分别为 300 和 1800。
- Marker hidden metadata 会持久化本次 `ackTimeoutSeconds`，旧 marker metadata 缺少该字段时仍按配置 fallback。
- 设计文档和双语 README 已同步默认 timeout 和 missed-ack 行为。

## Next Steps

- 合入后观察下一次 `codex/review-gate` live run，确认未 ack marker 会在 5 分钟左右补发而不是等 1 小时。

## Evidence

- https://github.com/JoeyTeng/codex-background-task-handler/pull/79
- `npm --prefix tools/codex-review-gate run check`
- `npm --prefix tools/codex-review-gate test`
