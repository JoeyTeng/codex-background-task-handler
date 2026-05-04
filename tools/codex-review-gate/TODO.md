# Codex Review Gate TODO

- [x] Replace the legacy token-echo gate with the reaction-driven serialized marker design in `DESIGN.md`.
- [x] Package the runner behind a local composite action wrapper.
- [x] Store a sticky PR state comment with bootstrap baseline, active marker, reaction identity, and status head.
- [x] Treat PR-open auto review as bootstrap baseline only.
- [x] Allow at most one controlled `@codex review` marker per PR at a time.
- [x] Treat `eyes` as ongoing/liveness only.
- [x] Pass only on a Codex `+1` reaction transition after the marker baseline and no current-head Codex findings.
- [x] Keep an unchanged old `+1` pending/stalled; never reuse it for pass.
- [x] Add a one-hour-scale stalled retry that re-baselines and creates a new controlled marker.
- [x] Reconstruct state from trusted marker comments if the sticky state comment is missing; fail closed when parsed trusted metadata is invalid.
- [x] Detect Codex review-body findings in `PullRequestReview.body` when they are scoped to the current head.
- [x] Filter inline Codex findings through GraphQL review-thread state so resolved or outdated threads do not block a later head.
- [x] Validate on a live PR before adding `codex/review-gate` to required status checks.
- [x] Configure branch protection to require both `codex/review-gate` and all conversations resolved.
- [x] After this branch lands, rerun #12 or a successor PR to confirm a resolved outdated Codex thread no longer blocks marker creation.

## Live validation evidence

- 2026-05-04: reran `codex-review-gate.yml` via `workflow_dispatch` on PR #14.
- Evidence target: PR #14 head `6a2d9e57e6dff257f1ea4a81f4a0cca96768a383`, GraphQL thread `PRRT_kwDOSJZ-as594_hR`, Codex comment `3148673469`.
- REST remap shape: comment `3148673469` reports `commit_id=6a2d9e57e6dff257f1ea4a81f4a0cca96768a383` while `original_commit_id=07172b85d5404b65a9a3400bf63831a4ef8c4fa8`.
- GraphQL thread state: `isResolved=true`, `isOutdated=false`.
- Gate result: run `25316273973` passed with status `Codex completion observed and current head has no Codex findings`, proving the resolved remapped inline thread did not block marker creation or final success.
