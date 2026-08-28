# Task 4 Report

## Status

Complete after review fix round 1. The Pi 0.84.3 harness now launches only
inside its provisioned agent container through argument-separated `docker exec`,
using `/workspace` and `/session` container paths and one compact materialized
`TaskPacket`. The authoritative live JSON event stream is captured durably on
the host and tailed independently of Pi's conversation JSONL. Model policy stays
in durable owner metadata and is never translated into Pi launch selection.

Empty-session resume reloads the materialized packet without resetting the
delivered-event cursor; complete malformed conversation records are rejected and
only a torn final record is truncated. Stop, duplicate-start, and harness-drop
paths manage the in-container Pi process group with TERM then KILL while retaining
registry ownership until reap is confirmed. Session state remains after the
disposable agent container is removed. Native non-empty conversation resume/fork,
normalized sequence-zero events, unknown-event accounting, and inactivity,
wall-clock, and CPU-saturation health classification remain implemented.

## Commits

- `279b7fb` — implementation and tests.
- `3076652` — review fix round 1 implementation and real-Docker tests.
- Report commit — this report.

## Tests

- `cargo test -p harness-pi --test pi_harness` — 8 passed against a real Docker
  daemon and isolated labelled Debian containers with a mounted stub Pi. Coverage
  includes container-only paths, absent model-selection flags, distinct live and
  conversation JSONL, incremental partial-line delivery, persistence after
  container removal, empty/malformed/torn resume behavior, TERM→KILL process-group
  cleanup, duplicate rejection, drop cleanup, missing Docker, and missing Pi.
- `cargo test -p orchestrator-worker --test health` — 3 passed; half-window
  inactivity warning/reset, inactivity and wall-clock failures, and sustained
  CPU saturation.
- Current toolchain: `cargo build --workspace`, `cargo test --workspace`,
  `cargo clippy --workspace --all-targets -- -D warnings`, and
  `cargo fmt --all -- --check` — passed, including real Docker, PostgreSQL, and
  Git suites.
- Rust 1.85: the same complete build/test/clippy/fmt gate — passed, including
  the eight real-Docker Pi harness tests.

## Concerns

- Live InferWeave inference was not exercised; the installed Pi 0.84.3 CLI/help
  and package source were used as the authoritative process/session interface.
- Production runtime mount provisioning is intentionally unchanged in this fix
  round; the harness requires the caller-provisioned agent container to bind the
  host worktree at `/workspace` and durable session directory at `/session`.
