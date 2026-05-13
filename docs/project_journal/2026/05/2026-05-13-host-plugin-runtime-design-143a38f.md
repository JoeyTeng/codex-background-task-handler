---
id: 20260513-143a38f-host-plugin-runtime-design
title: Host Plugin Runtime And Generic Delivery Design
status: active
created: 2026-05-13
updated: 2026-05-13
branch: codex/host-plugin-runtime-design
pr:
supersedes:
superseded_by:
---

# Host Plugin Runtime And Generic Delivery Design

## Summary

- Canonical host-level plugin design is now recorded in [Host Plugin Runtime And Generic Delivery](../../../design/HOST_PLUGIN_RUNTIME_AND_DELIVERY.md).
- The design separates `cbth service` from `cbth daemon`, keeps Webex connector as an external integration plugin, and distinguishes host-level plugins from Codex runtime plugins.
- Plugin communication is planned as versioned persistent UDS RPC in v1, not CLI and not gRPC yet.
- Generic delivery is modelled as delivery core plus drivers. The only v1 supported driver is `codex_app_server`; Desktop remains staged until caller wake and boundary crossing are validated.

## PR Graph

- Wave 1:
  - C1: cbth plugin RPC skeleton.
  - C2: cbth service and plugin supervisor.
  - W1: Webex state authority split.
  - W2: Webex plugin packaging and RPC client.
- Wave 2:
  - C3: plugin-scoped app-server lease RPC.
  - W3: Webex plugin uses cbth-managed app-server.
  - C4: generic delivery core with supported `codex_app_server` driver.
  - W4: Webex async/background results use `delivery.enqueue`.
- Wave 3:
  - C5: service install/manage.
  - C6: plugin release manager and rollback.
  - W5: Webex lifecycle hooks.
  - W6: optional Webex handoff.
  - C7/W7: opt-in live E2E.

## Execution Gates

- One PR has one code-owning agent.
- Review/test subagents should be read-only unless the PR owner assigns a disjoint write set.
- Each implementation PR needs a fixed `base_sha..head_sha` review range, relevant tests, and clear-context GPT-5.5 fast-mode comprehensive local review before final push/review resolution.
- RPC, daemon, delivery, and upgrade PRs need a final whole-range review after local findings and remote review comments are resolved.

## Next Steps

- Land this design baseline.
- Start C1/W1 in parallel. C2 and W2 may draft against the C1 protocol draft or fake cbth RPC server, but merge/integration must wait for C1.
- Keep Desktop delivery out of v1 supported generic delivery until caller wake, boundary crossing, and artifact-read policy are implemented and validated.
