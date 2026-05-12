# Design Overview

`cbth` is a companion layer around Codex for long-running local work. It does not modify upstream `codex`; instead, it owns local daemon state, supervised tasks, and narrow integration points with Codex CLI or Desktop experiments.

## What The Project Is For

Codex can start work that outlives a single foreground interaction: test suites, build jobs, polling loops, or other local commands. `cbth` provides a durable local control plane around that work:

- Start or resume a managed Codex CLI session bound to a specific Codex thread.
- Run a local command under daemon supervision and keep task output on disk.
- Turn task completion into a durable delivery batch for the bound thread.
- Attempt automatic delivery only when local proof and policy checks allow it.
- Leave clear manual recovery state when automatic delivery cannot be proven.

## Shared Core

The implementation is converging on one Rust binary with thin integration entrypoints:

- CLI entrypoints for managed Codex sessions, task supervision, job/batch operations, and operator recovery.
- Desktop-facing helper surfaces for bridge preflight, inbox snapshots, installation state, and writeback validation.
- A same-user local daemon that owns mutating operations, task process lifecycle, app-server leases, and maintenance sweeps.
- A local SQLite store and managed artifact/log files under `CBTH_HOME` or `~/.cbth`.

The detailed internal architecture record lives in [design/SHARED_CORE_ARCHITECTURE.md](design/SHARED_CORE_ARCHITECTURE.md).

## CLI Delivery Path

The current primary dogfood path is CLI managed-session delivery:

1. `cbth new`, `cbth resume`, or `cbth cli run` launches foreground Codex through a daemon-owned loopback `codex app-server`.
2. A sidecar client observes thread/session state and records durable activity, capability, and permission proof.
3. `cbth task run` or job commands create durable task/job/batch state.
4. With explicit `--auto-delivery-policy trusted-all`, the sidecar waits for idle proof, sends one marked `turn/start`, accepts the returned `turn.id`, and closes the batch only after terminal observation or same-epoch reconcile.
5. If acceptance or observation is ambiguous, the batch moves to manual recovery instead of retrying blindly.

Detailed design records:

- [design/CLI_SHARED_APP_SERVER_SIDECAR_DESIGN.md](design/CLI_SHARED_APP_SERVER_SIDECAR_DESIGN.md)
- [design/CLI_TASK_SUPERVISOR_DESIGN.md](design/CLI_TASK_SUPERVISOR_DESIGN.md)
- [design/CLI_ACTIVE_TURN_STEER_DESIGN.md](design/CLI_ACTIVE_TURN_STEER_DESIGN.md)

## Desktop Bridge Boundary

Desktop bridge work is not yet an enabled automatic delivery path. The current foundation focuses on:

- Installation-wide Desktop capability state.
- Caller heartbeat binding records.
- Preflight snapshots under `~/.cbth/inbox/`.
- No-DB read helpers for already-published inbox JSON.
- Transcript/tool-output relay validation for writeback-like signals.

Desktop automatic delivery still needs production rollout tailing, boundary crossing, artifact-read policy, and stronger lifecycle cleanup before it can be treated as a supported path.

Detailed records:

- [design/DESKTOP_BACKGROUND_TASK_BRIDGE_DESIGN.md](design/DESKTOP_BACKGROUND_TASK_BRIDGE_DESIGN.md)
- [design/DESKTOP_BRIDGE_FOUNDATION.md](design/DESKTOP_BRIDGE_FOUNDATION.md)
- [validation/](validation/)

## Safety Model

The project is deliberately conservative:

- Local IPC is same-user and loopback-only.
- Mutating operations route through the daemon by default.
- Automatic delivery is opt-in and fail-closed.
- CLI permissions are pinned from startup/current proof and capped rather than widened.
- Ambiguous delivery evidence becomes `manual_resolution_only`.
- Desktop helper paths prefer read-only snapshots and explicit operator validation until the full delivery boundary is proven.

`trusted-all` is intentionally broad and should be reserved for dedicated single-user workstations.

## Evolution Direction

The near-term direction is to keep hardening the CLI dogfood path while moving Desktop from validation surfaces toward a production bridge:

- Tighten daemon recovery, handoff, and stale-state cleanup.
- Keep Codex app-server compatibility probes current as the upstream protocol evolves.
- Continue reducing manual recovery cases by improving observation and reconcile paths.
- Promote only stable, user-facing workflows into the bilingual guide set.
- Keep detailed design, plan, and validation records internal unless they become operator-facing.
