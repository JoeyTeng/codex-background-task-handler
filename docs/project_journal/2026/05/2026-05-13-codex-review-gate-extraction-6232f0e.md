---
id: 20260513-6232f0e-codex-review-gate-extraction
title: Codex Review Gate Extraction
status: completed
created: 2026-05-13
updated: 2026-05-13
branch: codex/extract-codex-review-gate
pr: https://github.com/JoeyTeng/codex-background-task-handler/pull/82
supersedes: []
superseded_by:
---

# Codex Review Gate Extraction

## Summary

- `codex-review-gate` 已拆到独立公开仓库：https://github.com/JoeyTeng/codex-review-gate。
- 当前仓库保留 thin workflow，并用完整 commit SHA 引用外部 action，避免 `pull_request_target` privileged workflow 依赖可移动 tag。
- 本地 `tools/codex-review-gate` 副本已移除，顶层 README 链接改为独立仓库。

## Current State

- 独立 action 初始提交为 `b6a2fc7d011ba60d9220e6f881c8d5e7c733fc2d`，并已发布 `v1.0.0` 和 `v1` tags。
- 目标仓库 workflow 使用 pinned external action，status context 仍保持 `codex/review-gate`。
- 迁移不改变 branch protection 的 required status 名称。

## Evidence

- https://github.com/JoeyTeng/codex-review-gate
- `npm run check` in the standalone action repo
- `npm test` in the standalone action repo
