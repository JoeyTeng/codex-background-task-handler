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

## Retest Condition From First Attempt

Before rerunning the same Desktop heartbeat validation, `cbth` should avoid a redundant `chmod` when an existing private directory or private file already satisfies the required mode. That change must preserve fail-closed repair behavior for too-permissive or otherwise unsafe paths; it should only skip permission mutation when local metadata proves the existing object is already private enough.

## 2026-05-06 Attempt: Failed At Startup Lock

Result: `VALIDATION_FAILED`

Evidence:

- Code branch: `codex/desktop-noop-private-permissions`
- Local binary: `/Users/hoteng/.cache/cargo-target/release/cbth`
- Local binary version: `cbth 0.1.0`
- Bridge thread id: `019db5e6-ba6a-7b80-95d2-a6867163281a`
- Temporary heartbeat automation id: `cbth-desktop-live-preflight-validation-retry`
- Validation marker: `CBTH_DESKTOP_PREFLIGHT_20260506_NOOP_CHMOD`
- Target rollout file: `/Users/hoteng/.codex/sessions/2026/04/22/rollout-2026-04-22T16-54-50-019db5e6-ba6a-7b80-95d2-a6867163281a.jsonl`
- Heartbeat run timestamp: `2026-05-06T10:11:29.115Z`

The local shell preflight with the same binary succeeded after the no-op chmod fix and published snapshot revision `019dfcc3-ed26-7b93-84f2-836054bc630f`.

The Desktop heartbeat got past the previous `chmod 0700 /Users/hoteng/.cbth` blocker, but mandatory preflight still failed before snapshot reads:

```text
CBTH_DESKTOP_PREFLIGHT_20260506_NOOP_CHMOD VALIDATION_FAILED step2_bridge_preflight_failed:open startup lock /Users/hoteng/.cbth/run/startup.lock: Operation not permitted (os error 1)
```

Local inspection after the failure showed the daemon run directory and startup lock were already private:

```text
drwx------ 7 hoteng staff 224 May  6 11:09 /Users/hoteng/.cbth
drwx------ 4 hoteng staff 128 May  6 11:09 /Users/hoteng/.cbth/run
srw------- 1 hoteng staff   0 May  6 11:09 /Users/hoteng/.cbth/run/cbth.sock
-rw------- 1 hoteng staff  10 May  6 11:09 /Users/hoteng/.cbth/run/startup.lock
```

Therefore this attempt still does not validate `read_transport_capability`. The durable installation state was left unchanged:

```text
read_transport_capability=unknown
artifact_read_capability=unknown
writeback_capability=unknown
read_transport_generation=0
validated_at=null
```

## Next Retest Condition

Before rerunning the Desktop heartbeat validation again, decide whether mandatory Desktop `bridge-preflight` should avoid daemon autostart / startup-lock writes when an already-running same-user daemon is available, or whether Desktop heartbeat needs a narrower helper path that can publish/read inbox snapshots without opening `~/.cbth/run/startup.lock`. Any change must keep same-user daemon IPC fail-closed for mutating helper paths.
