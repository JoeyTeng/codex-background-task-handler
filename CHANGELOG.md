# Changelog

All notable release changes for `cbth` are documented here.

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
