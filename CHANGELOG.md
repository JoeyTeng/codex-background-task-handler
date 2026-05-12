# Changelog

All notable release changes for `cbth` are documented here.

## v0.2.0 - 2026-05-12

### Added

- Added daemon upgrade safety for `cbth new`, `resume`, and mutating commands: incompatible legacy daemons are no longer stopped by default, and new clients can start or reuse generation-specific daemons alongside old sessions.
- Added `daemon-handoff-v1` quiesce/handoff support for handoff-capable daemons, including app-server adoption that preserves foreground websocket pid/url and live jobs drain for already admitted tasks.
- Added operator visibility for coexistence with `daemon status --all` and `cli app-servers --all-daemons`.
- Added Desktop transcript relay writeback consumption from trusted rollout `function_call_output` carriers, plus live validation evidence for the relay-to-CAS path.

### Changed

- Scoped daemon startup recovery by owner generation so new daemons avoid signaling work still owned by an active legacy or generation daemon.
- Quiescing daemons now reject new work, keep control paths and task cancellation available, and auto-exit with `handoff_drain_complete` after their owned drain scope clears.
- Updated daemon upgrade design documentation and project tracking for the full PR1-PR5 safety sequence.

### Fixed

- Fixed stale generation-owned task cancellation so fallback recovery runs before returning the cancel response, preventing orphaned process groups after daemon replacement or reuse.
- Fixed app-server handoff rollback and redirect edge cases around stale exports, near-expired leases, release-status ambiguity, active bootstrap races, and adopted process cleanup.

## v0.1.5 - 2026-05-10

### Added

- Added clearer `cbth` help text for the top-level CLI, public subcommands, and visible arguments.
- Added `cbth cli app-servers --format json|human` plus `-H/--human` for concise managed app-server summaries.
- Added `cbth self update -i/--interactive` for prompted self-update installs.

## v0.1.4 - 2026-05-09

### Changed

- Validated managed CLI startup and diagnostics against `codex-cli 0.130.x`.
- Preferred Codex 0.130 `thread/turns/list` for accepted-turn reconciliation, with the existing `thread/read(includeTurns=true)` path kept as the compatibility fallback.
- Documented Codex 0.130 remote-control and non-loopback authentication surfaces as upstream capabilities that remain outside cbth's local v1 safety model.

## v0.1.3 - 2026-05-08

### Added

- Added `cbth new thread` defaults so managed new-thread startup can use the same checked defaults as existing thread flows.
- Added runtime topology documentation for the managed CLI, daemon, app-server sidecar, and Desktop bridge boundaries.

### Changed

- Hardened `cbth resume` so managed resume now preserves native cwd behavior unless an explicit or interactive cwd is selected.
- Parsed Codex 0.129 canonical permission profile records on read, including tagged profile shapes.
- Prefer Codex 0.129 stable built-in `turn/start.permissions` selection when the current active profile exactly matches the effective cap, with the legacy `sandboxPolicy` request fallback kept for older shapes.
- Added a soft Codex CLI compatibility warning around managed startup and `cbth doctor cli`; protocol field parsing remains the fail-closed safety gate.

## v0.1.2 - 2026-05-07

### Fixed

- Fixed daemon autostart process-group handling so daemon startup is less likely to stay coupled to the launching foreground process.

### Added

- Added Desktop writeback live-validation fixture coverage and evidence tracking.
- Updated Desktop bridge state tracking around writeback validation readiness.

## v0.1.1 - 2026-05-07

### Added

- Added the Desktop bridge foundation, including direct-helper preflight and existing-daemon Desktop preflight support.
- Added the no-DB Desktop inbox reader path for direct-file-read consumption.
- Added managed `cbth resume` permission handling for Desktop-bound flows.
- Added release dogfood diagnostics for install and self-update readiness.

### Changed

- Migrated long-form project trackers into focused project journal entries while preserving the legacy tracker snapshot.

## v0.1.0 - 2026-05-04

### Added

- Initial GitHub Release install and self-update support.
- Published Linux x86_64 glibc and macOS arm64 release assets, each with a matching `.sha256` checksum.
