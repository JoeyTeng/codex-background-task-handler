# Codex 0.129 Permission Selection

- status: completed
- branch: `wip/codex-129-permission-selection`
- related: [cbth resume follow-ups](2026-05-07-cbth-resume-followups-698664c.md)

## Goal

Adapt managed `cbth` CLI auto-delivery permission pinning to Codex 0.129 while preserving fail-closed behavior for older, partial, or unrepresentable protocol shapes.

## Current State

- `cbth` treats `codex-cli 0.129.x` as the soft validated range for managed startup and `cbth doctor cli`.
- `thread/resume` snapshots still prefer canonical `permissionProfile` for read-side permission derivation, including Codex 0.129 tagged `managed` / `disabled` / `external` profiles, and retain legacy `approvalPolicy` / `sandbox` fallback.
- Codex 0.129 legacy `sandbox` responses without `access` / `readOnlyAccess` are accepted and treated as full legacy read for canonical-vs-legacy compatibility checks.
- `thread/resume.activePermissionProfile` is now recorded for audit and parsed as the Codex 0.129 request-side profile-selection shape: `{ id, modifications }`.
- Automatic `turn/start` always pins the effective `approvalPolicy`.
- When a stable built-in current active profile exactly represents the computed effective sandbox cap, the active selection's network/write booleans match that cap, and the active selection is proven bidirectionally equivalent to the current canonical profile body, `turn/start` sends `permissions: { type: "profile", id, modifications }` and omits `sandboxPolicy`.
- When the effective cap is synthetic, startup-tighter than current, based on a mutable user-defined profile id, or cannot be represented by the current active profile selection, `turn/start` falls back to pinned legacy `sandboxPolicy`.
- Canonical profiles with deny carve-outs are allowed on the exact stable built-in `permissions` path, but legacy fallback rejects them because `sandboxPolicy` cannot represent those nested rules.
- Legacy fallback also requires effective full read access; restricted read intersections fail closed because Codex 0.129 request-side legacy `sandboxPolicy` cannot carry `access` / `readOnlyAccess` restrictions.
- Drift audit now includes `activePermissionProfile` alongside derived booleans, legacy policy fields, and canonical `permissionProfile`.

## Rationale

Codex 0.129 adds the request-side `permissions` field, but the available request shape selects a named active profile plus supported modifications rather than carrying an arbitrary fully canonical permission profile body. `cbth` therefore prefers the newest request shape only when it can prove a stable built-in selected active profile is exactly the effective cap, has matching network/write booleans, and is neither wider nor narrower than the canonical `permissionProfile` body returned by `thread/resume`. Mutable user-defined profile ids still use the older pinned `sandboxPolicy` path because Codex resolves profile ids again when processing `turn/start.permissions`.

## Validation

- Unit coverage checks that exact current active profiles use `turn/start.permissions`.
- Unit coverage checks that startup-tighter effective permissions fall back to legacy `sandboxPolicy`.
- Unit coverage checks that mutable user-defined active profile ids fall back to legacy `sandboxPolicy`.
- Unit coverage checks real 0.129 tagged read-only, disabled, and external profile parsing.
- Unit coverage checks exact stable built-in profile selection still works when canonical deny carve-outs make legacy fallback impossible, that fallback rejects those profiles when exact selection is not applicable, that restricted-read legacy fallback fails closed, and that malformed, wider, narrower, or boolean-mismatched `activePermissionProfile` data cannot make exact `turn/start.permissions` diverge from the current canonical body/effective cap.
- Generated Codex 0.129 app-server JSON Schema confirms request/response modification tags use `additionalWritableRoot`; the parser also accepts core-model `additional_writable_root` metadata defensively but emits the app-server wire tag.
- Doctor and CLI integration fixtures now default to `codex-cli 0.129.0`; out-of-range warning coverage uses `0.130.0`.
