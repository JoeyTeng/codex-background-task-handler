# CLI Active-Turn `turn/steer` Design

## Summary

`turn/steer` is a future CLI-only optimization for delivering a background batch into an already active caller turn. It is not part of the current automatic delivery path. CLI Dogfood V1 remains idle-only: `cbth cli run --auto-delivery-policy trusted-all` waits for durable idle proof, then uses `turn/start`.

Current implementation state:

- `turn/start` auto-delivery is implemented for idle caller threads.
- `turn/steer` has a narrow PoC proving same-turn continuation without early completion in one low-risk scenario.
- `begin-cli-accept --rpc-kind turn-steer` still fails closed until active-turn risk proof is implemented.
- No CLI flag, daemon capability, schema migration, or delivery loop change enables automatic steer today.

## Evidence And Limits

The reference PoC is `scripts/cli_turn_steer_poc.mjs`. It starts a long-running regular turn, sends `turn/steer` from a second shared app-server client while the turn is active, and observes that the same `turn_id` completes normally and absorbs the steered input.

That evidence is intentionally narrow. It only covers a regular turn with no approval, no network, and no write access. It proves `turn/steer` can be safe in a constrained case; it does not justify steering into arbitrary active turns.

## Future Eligibility Contract

Future automatic steer may only be considered when all of the following are true:

- The feature is explicitly enabled behind a dedicated gate; default shipping behavior remains idle-only.
- The batch is machine-classified as read-only and low-risk: no approval, no network, no write access, no large artifact-read dependency for the automatic path.
- The current active caller turn is also machine-classified as read-only and low-risk.
- The adapter has a fresh same-epoch active-turn proof for the bound `source_thread_id`, `managed_session_id`, and `session_epoch`.
- The adapter can identify exactly one active regular `turn_id` and can prove that this turn is steerable.
- The thread has no unresolved delivery attempt and satisfies the same FIFO, budget, and minimum consecutive-send interval rules as idle `turn/start`.

If any condition is missing or stale, the adapter must fall back to idle-only delivery. It must not attempt active-turn injection, foreground retargeting, rollout-only delivery proof, or best-effort steer.

## Required Proof Fields

The future current-state surface must provide a durable active-turn risk view, not just `activity_state=active`. At minimum it must include:

- `active_turn_id`
- `active_turn_kind`
- `active_turn_requires_approval`
- `active_turn_requires_network`
- `active_turn_requires_write_access`
- `active_turn_risk_class`
- `active_turn_steerable`

The only eligible risk class for automatic steer is `read_only_low_risk`. Unknown, missing, mixed, stale, or non-regular active turns are not steerable.

## Required Capability Proof

Before enabling automatic steer, the CLI adapter must prove the app-server surface can support the full delivery lifecycle for the current epoch:

- `turn/steer` returns an accepted response that identifies the same active `turn_id`.
- The event stream or same-epoch `thread/read(includeTurns=true)` can reconcile the steered marker to exactly that turn.
- The observer can distinguish completed, failed, interrupted, and replaced terminal outcomes for that `turn_id`.
- Ordering is deterministic enough to prevent concurrent steer attempts and to preserve FIFO batch order.
- A fresh PoC or live smoke proves steer does not start an extra turn or cause early completion for the supported active-turn class.

Without all of these proofs, `capability_turn_steer` must remain false and `turn_steer` attempts must remain fail-closed.

## Delivery Semantics

Accepted steer uses the same durable attempt model as idle `turn/start`:

- Write `accept_pending` before sending the side-effectful RPC.
- Include a unique `delivery_rpc_request_id` and `delivery_rpc_correlation_marker`.
- Use `delivery_rpc_kind=turn_steer`.
- Record the accepted active `delivery_turn_id` only after acceptance is proven.
- Close the batch as delivered only after a matching completed terminal event for the same `delivery_turn_id`.
- Failed, interrupted, replaced, continuity-loss, expired observation, or ambiguous acceptance all fail closed to manual resolution.

Pre-accept benign rejection, such as an active turn becoming non-steerable, may leave the batch retryable after fresh proof. It must not consume an attempt count. Accepted steer must never be treated as immediate delivery success.

## Out Of Scope

- Enabling automatic `turn/steer`.
- Adding public user-facing `turn/steer` flags.
- Changing `cbth cli run` foreground behavior.
- Retargeting when the user switches foreground threads.
- Desktop bridge active-turn injection.
- Treating rollout polling alone as automatic delivery proof.
