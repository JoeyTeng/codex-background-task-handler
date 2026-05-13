---
id: 20260513-542dab6-c1-plugin-rpc-skeleton
title: C1 Plugin RPC Skeleton
status: completed
created: 2026-05-13
updated: 2026-05-13
branch: codex/c1-plugin-rpc-skeleton
pr:
supersedes:
superseded_by:
---

# C1 Plugin RPC Skeleton

## Summary

- C1 adds the host-level plugin RPC protocol skeleton for `cbth`.
- The scope is limited to persistent UDS-compatible JSON-RPC-like framing, `plugin.hello`, protocol version negotiation, capability negotiation, service policy hints, daemon endpoint hints, and a typed error model.
- Follow-up PRs still own `cbth service run`, plugin supervision, registry, release management, app-server lease RPC, and delivery RPC methods.

## Current State

- `src/plugin_rpc.rs` defines the C1 protocol types and a bounded length-prefixed JSON frame codec for persistent streams.
- The handshake helper selects the highest mutually supported protocol version and rejects unsupported protocol versions or missing required plugin capabilities with typed errors.
- Unit tests cover successful hello negotiation, unsupported protocol, missing required capability, malformed JSON, truncated and oversized frames, persistent multi-frame reads, response construction, and error serialization roundtrip.

## Next Steps

- C2 can wire this module into `cbth service run` and plugin process supervision.
- C3/C4 should add app-server lease and generic delivery RPC methods on top of this skeleton instead of expanding C1.

## Evidence

- Base: `542dab68a8d374af75d9d1210b5f9708e0d9f48e`
- Branch: `codex/c1-plugin-rpc-skeleton`
- PR:
