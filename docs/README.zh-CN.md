# 文档

语言：[British English (en-GB)](README.md) | [简体中文 (zh-CN)](README.zh-CN.md)

本目录包含 user-facing guides、内部设计记录、实现计划、验证记录和项目 tracking 入口。

## User-Facing Guides

- [使用指南](USAGE.zh-CN.md) / [Usage guide](USAGE.en-GB.md)
- [设计概览](DESIGN_OVERVIEW.zh-CN.md) / [Design overview](DESIGN_OVERVIEW.en-GB.md)
- [操作恢复](OPERATOR_RECOVERY.zh-CN.md) / [Operator recovery](OPERATOR_RECOVERY.en-GB.md)
- [开发指南](DEVELOPMENT.zh-CN.md) / [Development guide](DEVELOPMENT.en-GB.md)
- [真实 E2E 指南](LIVE_E2E.zh-CN.md) / [Live E2E guide](LIVE_E2E.en-GB.md)

仓库根目录 [README.md](../README.md) 是默认 `en-GB` 入口；对应中文版本是 [README.zh-CN.md](../README.zh-CN.md)。

## 内部记录

- [design/](design/) 包含 architecture 和 implementation design records，包括 [Host Plugin Runtime And Generic Delivery](design/HOST_PLUGIN_RUNTIME_AND_DELIVERY.md)。
- [plans/](plans/) 包含 phased implementation 和 delivery plans。
- [validation/](validation/) 包含 live validation harnesses、probes 和 evidence records。

这些内部记录不要求双语，除非后续被提升到 user-facing guide set。

## Project Tracking

- [PROJECT_STATE.md](PROJECT_STATE.md) 是简洁的 current-state 和 handoff 入口。
- [PROJECT_TODO.md](PROJECT_TODO.md) 是简洁的 cross-task backlog 入口。
- [project_journal/](project_journal/) 包含按 `YYYY/MM/*.md` 日期组织的 durable per-workstream records。
- 生成的 `project_journal/INDEX.md` 是本地 ignored convenience output，不要提交。

## Merge-Time Bookkeeping

本仓库只做 squash merge。Tracked journal docs 应描述 PR 落到 target branch 后的状态，而不是 PR review 期间的临时状态。

- 如果一个 PR 完整完成某个 workstream，merge 前把相关 journal entry 标成 `status: completed`，并用 PR link 作为 evidence。
- 不要在 tracked docs 中留下 "waiting for merge"、"not merged yet" 或 "ready for review" 这类临时状态；这些内容放在 PR body、checklist 或 comments。
- 如果一个 PR 只完成较大 workstream 的一部分，保持 journal 为 `active` 或 `blocked`，并记录真实剩余 next steps。
