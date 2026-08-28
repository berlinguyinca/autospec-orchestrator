# Task 4 Report

## Status

Complete after review fix round 2. The Pi 0.84.3 harness now launches only
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

The live protocol normalizer now follows the installed Pi 0.84.3 declaration:
the session header and turn start are recognized but ignored, `agent_start`
emits the sole `AgentStarted`, assistant `message_end` backend failures emit
`ModelFailed`, and `agent_settled` emits `ReviewReady`. Other declared protocol
records are deliberately ignored without inflating the unknown-event counter.

Process-group authority no longer crosses into the Pi-writable session mount.
An immutable inline supervisor runs through argument-separated `docker exec`,
creates the process group, and emits one trusted control record before executing
Pi. The host removes that record from the event stream and retains the PGID only
in memory. Root `docker exec` control commands signal that trusted PGID, while
bounded drop cleanup uses `try_wait`, TERM, and KILL without an indefinite wait.

## Commits

- `279b7fb` — implementation and tests.
- `3076652` — review fix round 1 implementation and real-Docker tests.
- `a274938` — review fix round 2 protocol normalization and trusted supervisor.
- Report commit — this report.

## Tests

- `cargo test -p harness-pi --test pi_harness` — 10 passed against a real Docker
  daemon and isolated labelled Debian containers with a mounted stub Pi. Coverage
  includes container-only paths, absent model-selection flags, distinct live and
  conversation JSONL, incremental partial-line delivery, persistence after
  container removal, empty/malformed/torn resume behavior, TERM→KILL process-group
  cleanup, duplicate rejection, drop cleanup, missing Docker, and missing Pi.
  Round-2 coverage adds an ordered Pi 0.84.3 JSON fixture, both assistant backend
  failure shapes, deliberate known-record ignores, trusted-header stripping,
  forged session control files/PGIDs, and bounded cleanup of a TERM-ignoring
  descendant.
- `cargo test -p orchestrator-worker --test health` — 3 passed; half-window
  inactivity warning/reset, inactivity and wall-clock failures, and sustained
  CPU saturation.
- Current toolchain: `cargo build --workspace`, `cargo test --workspace`,
  `cargo clippy --workspace --all-targets -- -D warnings`, and
  `cargo fmt --all -- --check` — passed, including real Docker, PostgreSQL, and
  Git suites.
- Rust 1.85: the same complete build/test/clippy/fmt gate — passed, including
  the ten real-Docker Pi harness tests.

## Concerns

- Live InferWeave inference was not exercised; the installed Pi 0.84.3 CLI/help
  and package source were used as the authoritative process/session interface.
- Production runtime mount provisioning is intentionally unchanged in this fix
  round; the harness requires the caller-provisioned agent container to bind the
  host worktree at `/workspace` and durable session directory at `/session`.
