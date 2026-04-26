# Codex Review Gate TODO

- [ ] Replace the legacy token-echo gate with the reaction-driven serialized marker design in `DESIGN.md`.
- [ ] Store a sticky PR state comment with bootstrap baseline, active marker, reaction identity, and status head.
- [ ] Treat PR-open auto review as bootstrap baseline only.
- [ ] Allow at most one controlled `@codex review` marker per PR at a time.
- [ ] Treat `eyes` as ongoing/liveness only.
- [ ] Pass only on a Codex `+1` reaction transition after the marker baseline and no current-head Codex inline findings.
- [ ] Keep an unchanged old `+1` pending/stalled; never reuse it for pass.
- [ ] Add a one-hour-scale stalled retry that re-baselines and creates a new controlled marker.
- [ ] Reconstruct state from PR comments, reactions, and review comments if the sticky state comment is missing; fail closed when reconstruction is ambiguous.
- [ ] Validate on a live PR before adding `codex/review-gate` to required status checks.
- [ ] Configure branch protection to require both `codex/review-gate` and all conversations resolved.
