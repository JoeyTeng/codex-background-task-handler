# Codex Review Gate TODO

- [x] Replace the legacy token-echo gate with the reaction-driven serialized marker design in `DESIGN.md`.
- [x] Package the runner behind a local composite action wrapper.
- [x] Store a sticky PR state comment with bootstrap baseline, active marker, reaction identity, and status head.
- [x] Treat PR-open auto review as bootstrap baseline only.
- [x] Allow at most one controlled `@codex review` marker per PR at a time.
- [x] Treat `eyes` as ongoing/liveness only.
- [x] Pass only on a Codex `+1` reaction transition after the marker baseline and no current-head Codex inline findings.
- [x] Keep an unchanged old `+1` pending/stalled; never reuse it for pass.
- [x] Add a one-hour-scale stalled retry that re-baselines and creates a new controlled marker.
- [x] Reconstruct state from trusted marker comments if the sticky state comment is missing; fail closed when parsed trusted metadata is invalid.
- [ ] Validate on a live PR before adding `codex/review-gate` to required status checks.
- [ ] Configure branch protection to require both `codex/review-gate` and all conversations resolved.
