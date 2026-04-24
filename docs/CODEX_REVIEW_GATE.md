# Codex Review Gate

本仓库可以用确定性的 `codex/review-gate` commit status 作为 PR 合入门禁。

## 工作方式

- `.github/workflows/codex-review-gate.yml` 使用 `pull_request_target`，因此 gate 由默认分支上的可信 workflow 控制，不执行 PR 代码。
- workflow 会把 `codex/review-gate` commit status 写到 PR head SHA。
- 如果当前 head SHA 已经有 Codex inline review comments，status 直接失败。
- 否则 workflow 会创建或复用当前 head SHA 对应的 marker comment：

  ```text
  @codex review

  <!-- codex-review-gate
  head=<head-sha>
  run=<workflow-run-url>
  -->
  ```

- workflow 只接受当前 head marker comment 上 `chatgpt-codex-connector` 的 `+1` reaction。
- marker comment 本身不代表通过；即使有人手工创建了同样的 marker，也仍然必须等到 Codex bot 对这条当前 head comment 给出 `+1` reaction。
- `eyes` 表示 Codex 已经接收请求，gate 会继续 poll。
- PR body reactions 以及旧 head SHA 的 comments/reactions 全部忽略。
- gate 超时时间是 30 分钟。

## 仓库配置

workflow 合入默认分支并至少运行一次后，把 `codex/review-gate` 加到仓库 ruleset 的 required status checks。这个 context 建议选择 "any source"，因为 status 可能由 `GITHUB_TOKEN` 写入，也可能由 `CODEX_REVIEW_GATE_TOKEN` 写入。

首次引入这个 workflow 的 PR 不能完整自测 gate，因为 `pull_request_target` 只会运行默认分支上已经存在的 workflow。需要先让 workflow 落到默认分支，再用后续测试 PR 验证 `codex/review-gate` 的真实行为。

workflow 默认使用 `GITHUB_TOKEN`。如果本仓库里 GitHub Actions comment 不能触发 Codex，则配置 `CODEX_REVIEW_GATE_TOKEN` secret。建议使用 fine-grained token，并授予：

- Commit statuses: read/write
- Issues: read/write
- Pull requests: read

为了让信号最干净，建议关闭 Codex automatic review-on-push，只让这个 gate comment 触发当前 head review。
