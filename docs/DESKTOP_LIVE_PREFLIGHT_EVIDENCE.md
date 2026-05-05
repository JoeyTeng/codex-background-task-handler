# Desktop Live Preflight Evidence

本文记录真实 Codex Desktop heartbeat 对 Desktop bridge preflight 的实测证据。它只用于判断 installation-wide capability 是否可以写为 `validated`，不表示 Desktop automatic delivery 已启用。

## 2026-05-05 Attempt: Failed Before Preflight Snapshot Read

Result: `VALIDATION_FAILED`

Evidence:

- PR #35 merge commit: `bbe400391dd3a44dcf7bbc82ef59e04bac120e6f`
- Local branch: `codex/desktop-live-preflight-evidence`
- Local binary: `/Users/hoteng/.cache/cargo-target/release/cbth`
- Local binary version: `cbth 0.1.0`
- Bridge thread id: `019db5e6-ba6a-7b80-95d2-a6867163281a`
- Temporary heartbeat automation id: `cbth-desktop-live-preflight-validation`
- Validation marker: `CBTH_DESKTOP_PREFLIGHT_20260505_BBE4003`
- Target rollout file: `/Users/hoteng/.codex/sessions/2026/04/22/rollout-2026-04-22T16-54-50-019db5e6-ba6a-7b80-95d2-a6867163281a.jsonl`
- Heartbeat run timestamp: `2026-05-05T10:47:01.839Z`

The Desktop heartbeat successfully started a turn and executed the local validation script without asking the user for approval. It ran `/Users/hoteng/.cache/cargo-target/release/cbth --version` and received `cbth 0.1.0`.

The mandatory preflight helper failed at:

```text
CBTH_DESKTOP_PREFLIGHT_20260505_BBE4003 VALIDATION_FAILED step2_desktop_bridge_preflight_failed: chmod 0700 /Users/hoteng/.cbth: Operation not permitted (os error 1)
```

Local inspection immediately after the failure showed `/Users/hoteng/.cbth` already had private permissions:

```text
drwx------ 7 hoteng staff 224 May  5 11:45 /Users/hoteng/.cbth
```

Therefore this attempt does not validate `read_transport_capability`. The durable installation state was left unchanged:

```text
read_transport_capability=unknown
artifact_read_capability=unknown
writeback_capability=unknown
read_transport_generation=0
validated_at=null
```

## Next Retest Condition

Before rerunning the same Desktop heartbeat validation, `cbth` should avoid a redundant `chmod` when an existing private directory or private file already satisfies the required mode. That change must preserve fail-closed repair behavior for too-permissive or otherwise unsafe paths; it should only skip permission mutation when local metadata proves the existing object is already private enough.
