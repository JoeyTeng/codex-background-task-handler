# Codex Review Gate

Languages: [English (en-GB)](README.md) | [Simplified Chinese (zh-CN)](README.zh-CN.md)

`codex-review-gate` is an internal subproject that owns the reusable `codex/review-gate` GitHub status check. It is designed for repositories that want a required status to stay pending or failing until Codex review output for the current PR head is clean.

The repository workflow remains at `.github/workflows/codex-review-gate.yml` because GitHub Actions requires workflows there. That workflow is a thin wrapper around `tools/codex-review-gate/src/gate.mjs`.

## What It Checks

The checked-in runner implements a reaction-driven serialized marker flow:

- Runs under `pull_request_target` from the repository default branch.
- Writes the `codex/review-gate` commit status to the PR head SHA.
- Fails when current-head Codex inline review threads or review-body findings are unresolved and not outdated.
- Keeps a trusted sticky PR state comment with hidden metadata.
- Treats PR-open automatic review output as first-round baseline only.
- Serializes controlled `@codex review` marker comments.
- Treats Codex `eyes` reactions as liveness only.
- Passes only after a new Codex PR-body `+1` reaction identity or Codex top-level completion comment appears after the active marker baseline and the current head has no Codex findings.
- Keeps unchanged old `+1` reactions pending or stalled instead of reusing them.

## Files

- `action.yml`: composite action wrapper for the runner.
- `src/gate.mjs`: GitHub Actions runner script.
- `src/core.mjs`: testable state and signal helpers.
- `DESIGN.md`: target signal model and state machine.
- `TODO.md`: implementation and validation backlog for this subproject.

## Composite Action Usage

```yaml
- uses: ./tools/codex-review-gate
  with:
    github-token: ${{ github.token }}
    pull-request: ${{ github.event.pull_request.number }}
    head-sha: ${{ github.event.pull_request.head.sha }}
```

## Repository Setup

After the workflow is merged into the default branch and has run at least once, add `codex/review-gate` to the repository ruleset as a required status check. Use GitHub Actions as the source because the workflow writes the status with `GITHUB_TOKEN`.

Recommended rollout:

1. Merge the workflow into the repository default branch.
2. Open a follow-up test PR.
3. Confirm the workflow creates a current-head marker comment on `opened` and `synchronize`.
4. Confirm the gate can pass or fail with the current runner implementation.
5. Add `codex/review-gate` to the ruleset required status checks.

Do not require `codex/review-gate` before the workflow exists on the protected default branch. The first PR that introduces the workflow cannot fully self-test the `pull_request_target` path because GitHub Actions reads that workflow from the repository default branch.

## Operational Notes

- The workflow does not execute PR code.
- The workflow should have both `issues: write` and `pull-requests: write` so it can create PR conversation comments.
- For the cleanest signal, disable Codex automatic review-on-push and let the gate marker comment trigger the current-head review.
- The runner uses REST pull request comments plus GraphQL `reviewThreads` metadata to avoid treating resolved or outdated Codex inline threads as current findings.
- Review-body findings do not have resolvable review threads, so the runner matches them by `PullRequestReview.commit_id` and current-head blob links.
- Default timeouts are currently 2 hours overall, 1 hour per marker, and 60 seconds bootstrap grace.
