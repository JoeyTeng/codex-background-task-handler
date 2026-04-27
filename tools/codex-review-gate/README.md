# Codex Review Gate

This internal subproject owns the reusable `codex/review-gate` GitHub status check.

The repository workflow stays in `.github/workflows/codex-review-gate.yml` because GitHub Actions requires workflows there. That workflow is intentionally a thin wrapper around `tools/codex-review-gate/src/gate.mjs`.

## Files

- `action.yml`: composite action wrapper for the runner.
- `src/gate.mjs`: the GitHub Actions runner script.
- `src/core.mjs`: testable state and signal helpers.
- `DESIGN.md`: target signal model and state machine.
- `TODO.md`: implementation and validation backlog for this subproject.

## Current Status

The checked-in runner implements the reaction-driven serialized marker design:

- It runs under `pull_request_target` from the repository default branch.
- It writes the `codex/review-gate` commit status to the PR head SHA.
- It fails when unresolved, non-outdated Codex inline review threads or Codex review-body findings exist on the current head.
- It keeps a trusted sticky PR state comment with hidden metadata.
- It treats PR-open automatic review output as first-round baseline only.
- It serializes controlled `@codex review` marker comments.
- It treats Codex `eyes` reactions as liveness only.
- It passes only when a new Codex PR-body `+1` reaction identity or Codex top-level completion comment appears after the active marker baseline and the current head has no Codex findings.
- It marks unchanged old `+1` reactions as pending/stalled instead of reusing them.

## 工作方式

- `.github/workflows/codex-review-gate.yml` 使用 `pull_request_target`，因此 gate 由默认分支上的可信 workflow 控制，不执行 PR 代码。
- workflow 会把 `codex/review-gate` commit status 写到 PR head SHA。
- 如果当前 head SHA 已经有未 resolved、未 outdated 的 Codex inline review threads 或 review body findings，status 直接失败。
- 第一次运行会先记录 PR-open automatic review 已经产生的 Codex `eyes` / `+1` / inline comments / review body findings，作为 bootstrap baseline；只等待一个短 bootstrap grace window，然后就进入 controlled marker。
- 之后 workflow 同一时间只维护一条 controlled marker comment：

  ```text
  @codex review

  <!-- codex-review-gate-marker
  {
    "headSha": "<head-sha>",
    "baseline": {
      "plusOne": "<current Codex +1 reaction identity or null>",
      "eyes": "<current Codex eyes reaction identity or null>",
      "completionComment": "<current Codex top-level completion comment identity or null>"
    }
  }
  -->
  ```

- marker comment 用来触发 Codex 并建立当前 head 的等待起点；它本身不代表通过。
- workflow 不解析 Codex clean comment 文案，也不要求 Codex echo token。
- `eyes` 只说明 Codex ongoing。
- pass candidate 只来自 marker baseline 之后新出现或更新的 Codex PR-body `+1` reaction identity，或 marker 之后新出现的 Codex top-level completion comment；completion comment 的文案不参与判定，通过前仍只看当前 head 有没有 Codex findings。
- 这里的 “之后” 是严格晚于 marker comment timestamp；如果 reaction 和 marker 落在同一秒，runner 会按不可归因于当前 marker 处理。实际 Codex completion signal 预期会明显晚于 marker，通常不需要为同秒 timestamp 放宽通过条件。
- 如果 push 发生时旧 marker 还没完成，runner 会立刻把旧 marker 标为 `obsolete_head`，然后为最新 head 重新 baseline / 发 marker；不会等旧 marker 的一小时 timeout。
- 如果 sticky state comment 丢失，runner 不会把旧 marker comment 重新激活为可通过的 active marker；它会把该 marker 记为 `state_lost`，重新 baseline 后发新 marker。
- 实测后触发的 Codex review 可能 supersede 早先 onflight review，且旧 marker comment 上的小眼睛不会被移除；这些旧 `eyes` 只作为历史 liveness，不作为必须等待 completion 的代数关系。
- bootstrap 同样不等待旧 `eyes` 闭合；grace 结束后即使 auto review 看起来还在 ongoing，也会发 controlled marker 让当前 head review 接管。
- 通过前 workflow 会再次确认当前 head 没有未 resolved、未 outdated 的 Codex inline review threads 或 review body findings。
- inline review comments 来自 REST `/pulls/{number}/comments`，但 runner 会额外用 GraphQL `reviewThreads` 确认 thread 是否 `isResolved` / `isOutdated`，并分页补齐每个 thread 的 comment ID 映射。这样做是必要的，因为 GitHub REST 可能把一个已解决旧 thread 的 `commit_id` 映射到后续 head；没有 GraphQL thread metadata 的 current-head Codex inline comment 会保守地继续算作 finding。
- review-body findings 没有可 resolve 的 review thread；runner 只能按 `PullRequestReview.commit_id` 和 body 中的 current-head blob link 判断它是否属于当前 head。
- 如果旧 `+1` 已存在且不变化，gate 保持 pending；marker 一小时级 timeout 后标为 stalled 并重新 baseline / 重发。
- 当前默认 overall timeout 是 2 小时，marker timeout 是 1 小时，bootstrap grace 是 60 秒。

## Composite Action Usage

```yaml
- uses: ./tools/codex-review-gate
  with:
    github-token: ${{ github.token }}
    pull-request: ${{ github.event.pull_request.number }}
    head-sha: ${{ github.event.pull_request.head.sha }}
```

## Repository Setup

workflow 合入默认分支并至少运行一次后，把 `codex/review-gate` 加到仓库 ruleset 的 required status checks。这个 context 建议选择 GitHub Actions 作为 source，因为 status 由 workflow 的 `GITHUB_TOKEN` 写入。

首次引入这个 workflow 的 PR 不能完整自测 gate，因为 `pull_request_target` 只会运行 repository default branch 上已经存在的 workflow。这个 PR 也不会因为新 commit 自动创建 gate comment 或写入 `codex/review-gate` status。

同理，临时创建一个非默认 base branch 并把 workflow 放到那个 base branch 上，也不能验证 GitHub Actions bot 的真实触发路径；当前 GitHub Actions 会从 repository default branch 取 `pull_request_target` 的 workflow source/reference。

推荐启用顺序：

1. 先把 workflow 合入 repository default branch。
2. 再开一个后续测试 PR，确认 workflow 会在 `opened` / `synchronize` 时创建当前 head marker comment，并按当前 runner 实现写入 `codex/review-gate` status。
3. 确认测试 PR 的 gate 能通过或失败后，再把 `codex/review-gate` 加到 ruleset required status checks。

不要在 workflow 还没进入受保护分支前提前要求 `codex/review-gate`，否则当前引入 PR 会被一个没有 runner 能创建的 required status 卡住。

workflow 使用 `GITHUB_TOKEN`，这样 marker comment 的作者会是 `github-actions[bot]`。实测创建 PR conversation comment 需要 workflow token 同时具备 `issues: write` 与 `pull-requests: write`。为了让信号最干净，建议关闭 Codex automatic review-on-push，只让这个 gate comment 触发当前 head review。
