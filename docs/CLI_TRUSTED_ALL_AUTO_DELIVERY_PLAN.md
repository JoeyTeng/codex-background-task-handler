# CLI Trusted-All Idle Auto Delivery Plan

## Summary

Phase 11b implements the first automatic CLI delivery loop for `cbth cli run`.
The default behavior remains passive. Automatic delivery only starts when the
user explicitly opts in with `--auto-delivery-policy trusted-all`.

In `trusted-all` mode, the sidecar waits for the bound caller thread to become
idle, then delivers the current head background batch through `turn/start`.
Each delivery must include a unique marker and must prefer app-server evidence
over inference:

- `turn/start` response proves acceptance and returns the accepted `turn.id`.
- `turn/completed` notification proves terminal outcome for that `turn.id`.
- If the notification is missed but the same app-server session epoch remains
  continuous, `thread/read(includeTurns=true)` may reconcile the accepted turn.
- If accepted vs rejected cannot be proven, the sidecar must not retry. The
  existing stale sweep closes the ambiguity fail-closed.

Local rollout polling is useful diagnostic evidence, but v1 must not close a
batch as delivered from rollout-only evidence.

## Interfaces And Durable State

Add `cbth cli run --auto-delivery-policy <off|trusted-all>`.

- `off` is the default and preserves the current passive adapter behavior.
- `trusted-all` explicitly allows any submitted background task to be delivered
  automatically.

Add CLI attempt authorization modes:

- `strict_safe` preserves the current safety gate.
- `trusted_all` bypasses batch delivery policy gates, artifact-read gates, and
  managed-session risk-profile gates.
- `trusted_all` still requires an open head batch, remaining budget, matching
  `source_thread_id`, valid `managed_session_id`, matching `session_epoch`, and
  fresh idle proof.

Add hidden adapter-internal rejection handling:

- `cbth attempt reject-cli-before-accept --attempt-id ... [--now ...]`
- It is valid only for `accept_pending + delivery_rpc_state=pending_acceptance`.
- It writes `delivery_rpc_state=rejected_before_accept`.
- It does not increment `delivery_attempt_count`.
- It leaves the batch retryable after fresh idle proof.

Add append-only audit decisions:

- Stable user-facing command: `cbth audit list [--source-thread-id ...] [--limit ...]`
- Record at least: timestamp, source thread id, batch id, attempt id, managed
  session id, session epoch, policy kind, decision, reason, adapter kind, and
  details JSON.
- Decisions should cover allow, deny, attempt-start, accepted, rejected,
  reconciled, observed, and manualized outcomes.
- Before sending any `turn/start`, the audit allow / attempt-start records must
  be durable; audit write failure blocks delivery.

## Delivery Flow

The sidecar polls only when all of these are true:

- `--auto-delivery-policy trusted-all` is enabled.
- The durable managed session proof says the bound thread is idle.
- No other delivery is in flight for the same managed session.

Default poll interval is 2 seconds.

For each delivery:

1. Select the current open head batch for the bound source thread.
2. Write audit allow / attempt-start.
3. Enter `accept_pending` with a unique `delivery_rpc_request_id` and
   `delivery_rpc_correlation_marker`.
4. Send `turn/start` with `threadId` and text input.
5. Include the marker, source thread id, batch id, attempt id, job summaries,
   failure reasons, and artifact references in the prompt. Do not inline large
   artifact payloads.
6. On `turn/start` success, call `accept-cli` with the returned `turn.id` and a
   v1 observation window of 21600 seconds.
7. Observe terminal evidence for that accepted `turn.id`.

Terminal outcome mapping:

- `Completed` closes the batch as `close_reason=delivered`.
- `Failed` and `Interrupted` fail-close to `manual_resolution_only`.
- Replaced-equivalent evidence also fail-closes to `manual_resolution_only`.

If terminal notification is missed but the websocket connection and
`managed_session_id + session_epoch` remain continuous, run bounded
`thread/read(includeTurns=true)` reconcile for the accepted `turn.id`.

If `turn/start` returns a clear pre-accept rejection, call
`reject-cli-before-accept`, keep the batch automatic, and wait for fresh idle
proof before retrying.

If timeout, websocket close, or protocol error makes acceptance unknowable:

- Do not retry.
- Leave the attempt in `accept_pending`.
- Let stale sweep mark it as `delivery_rpc_state=unknown` and move the batch to
  `manual_resolution_only`.

## Capability And Safety Rules

`trusted-all` is a broad explicit escape hatch. It is not the final permission
model. The audit interface is the compatibility surface for future policy
engines:

- rule-based allow
- allowlist
- model-based allow

The sidecar must not record full automatic delivery capability until the current
session epoch has proven:

- `thread_resume`
- `turn_start`
- `current_state_sync`
- `turn_completed_event`
- negative terminal observation or reconcile support

Any app-server reconnect, session reattach, unresolved active attempt, or proof
invalidation clears idle and capability proof before another delivery.

Out of scope for this phase:

- `turn/steer`
- active-turn injection
- `--new-thread`
- Desktop bridge changes
- automatic delivered closure from rollout-only evidence

## Test Plan

Add deterministic fake e2e coverage under Rust integration tests:

- Default passive `cbth cli run` still sends no `turn/start`.
- `trusted-all` sends exactly one `turn/start` with a unique marker and closes
  delivered on completed notification.
- Missed terminal notification path uses `thread/read(includeTurns=true)` to
  reconcile a matching completed turn and close delivered.
- Failed / interrupted terminal fake paths manualize the batch.
- Pre-accept rejection writes `rejected_before_accept`, leaves the batch
  automatic, and does not increment attempt count.
- Timeout / websocket closed / protocol error leaves `accept_pending`; sweep
  later marks `unknown + manual_resolution_only`.
- Unsafe, requires-artifact-read, or risky-session batches are denied by
  `strict_safe` but allowed by `trusted-all`.
- `audit list` returns allow, deny, attempt-start, accepted, rejected,
  reconciled, observed, and manualized records, with filtering by
  `source_thread_id`.

Local validation before review and commit:

- `cargo fmt --all -- --check`
- `cargo clippy --locked --all-targets -- -D warnings`
- `cargo test --locked`
- `cargo test`
- helper-backed Codex review over the frozen review range
