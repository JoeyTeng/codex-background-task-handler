# Codex Review Gate

本仓库可以用确定性的 `codex/review-gate` commit status 作为 PR 合入门禁。

## 工作方式

- `.github/workflows/codex-review-gate.yml` 使用 `pull_request_target`，因此 gate 由默认分支上的可信 workflow 控制，不执行 PR 代码。
- workflow 会把 `codex/review-gate` commit status 写到 PR head SHA。
- 如果当前 head SHA 已经有 Codex inline review comments，status 直接失败。
- 否则 workflow 会为当前 run 创建新的 marker comment：

  ```text
  @codex review

  <!-- codex-review-gate
  head=<head-sha>
  run=<workflow-run-url>
  -->
  ```

- marker comment 用来触发 Codex 并建立当前 head 的等待起点；它本身不代表通过。每次 run 都创建新的 marker comment，避免同一 head 的重跑复用旧 trigger 后继续超时。
- workflow 只接受 marker comment 之后 `chatgpt-codex-connector` 发出的 clean top-level PR comment，例如 “didn't find any major issues”。
- PR body `+1` reaction 不能作为当前 head 的通过信号，因为它没有 commit 绑定，也可能来自别的 run/head 的延迟结果。
- 通过前 workflow 会重新确认 PR head 仍然等于本次 run 的 head SHA。
- gate 超时时间是 30 分钟。

## 仓库配置

workflow 合入默认分支并至少运行一次后，把 `codex/review-gate` 加到仓库 ruleset 的 required status checks。这个 context 建议选择 "any source"，因为 status 可能由 `GITHUB_TOKEN` 写入，也可能由 `CODEX_REVIEW_GATE_TOKEN` 写入。

首次引入这个 workflow 的 PR 不能完整自测 gate，因为 `pull_request_target` 只会运行 repository default branch 上已经存在的 workflow。这个 PR 也不会因为新 commit 自动创建 gate comment 或写入 `codex/review-gate` status。

同理，临时创建一个非默认 base branch 并把 workflow 放到那个 base branch 上，也不能验证 GitHub Actions bot 的真实触发路径；当前 GitHub Actions 会从 repository default branch 取 `pull_request_target` 的 workflow source/reference。

推荐启用顺序：

1. 先把 workflow 合入 repository default branch。
2. 再开一个后续测试 PR，确认 workflow 会在 `opened` / `synchronize` 时创建当前 head marker comment，并在 Codex clean top-level comment 出现后写入 `codex/review-gate` status。
3. 确认测试 PR 的 gate 能通过或失败后，再把 `codex/review-gate` 加到 ruleset required status checks。

不要在 workflow 还没进入受保护分支前提前要求 `codex/review-gate`，否则当前引入 PR 会被一个没有 runner 能创建的 required status 卡住。

workflow 默认使用 `GITHUB_TOKEN`。如果本仓库里 GitHub Actions comment 不能触发 Codex，则配置 `CODEX_REVIEW_GATE_TOKEN` secret。建议使用 fine-grained token，并授予：

- Commit statuses: read/write
- Issues: read/write
- Pull requests: read

为了让信号最干净，建议关闭 Codex automatic review-on-push，只让这个 gate comment 触发当前 head review。
