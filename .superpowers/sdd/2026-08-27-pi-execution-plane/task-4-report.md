# Task 4 Report

## Status

Complete after review fix round 5. The Pi 0.84.3 harness now launches only
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

The live protocol normalizer now follows the installed Pi 0.84.3 implementation:
the session header and turn start are recognized but ignored, `agent_start`
emits the sole `AgentStarted`, and terminal state is reduced across incremental
polls and harness restarts. Assistant errors remain pending through
`agent_end.willRetry` and `auto_retry_start`; a successful `auto_retry_end`
allows `agent_settled` to emit `ReviewReady`, while an exhausted retry emits one
`ModelFailed` and suppresses the finally-block `agent_settled`. Other declared
protocol records are deliberately ignored without inflating the unknown-event
counter. The undeclared top-level `message` shape is no longer treated as known
and increments unknown-event accounting.

Process-group authority no longer crosses into the Pi-writable session mount.
An immutable inline supervisor runs through argument-separated `docker exec`,
creates the process group, and emits one trusted control record before executing
Pi. The host removes that record from the event stream and retains the PGID only
in memory. Root `docker exec` control commands signal that trusted PGID, while
bounded drop cleanup uses `try_wait`, TERM, and KILL without an indefinite wait.
The supervisor now emits a token-bound PGID header and blocks before Pi exec on
an interactive stdin acknowledgment. The host sends that ACK only after header
validation and event-pump registration; invalid, unreadable, timed-out, or
thread-spawn startup failures close the channel, so no accepted future launch can
enter the Pi body. The removed token-free stability heuristic is no longer part
of correctness. Group reap uses Linux `/proc` PGID and non-zombie membership
rather than `kill -0`; zombie-only groups no longer block stop, drop, or startup
cleanup, and the harness does not depend on a PID 1 reaper. The durable live event
file, reducer/cursor, owner, and resume count all stay in the host-private session
root; only `session_root/conversation/` is mounted at `/session`.

## Commits

- `279b7fb` — implementation and tests.
- `3076652` — review fix round 1 implementation and real-Docker tests.
- `a274938` — review fix round 2 protocol normalization and trusted supervisor.
- `54d9164` — review fix round 3 retry reducer, private state boundary, and
  startup-failure cleanup.
- `fb59162` — review fix round 4 zombie-aware reap, delayed-start cleanup, and
  exact message normalization.
- `96d3a68` — widen delayed-start stability under parallel Docker load.
- `65dc4ae` — replace startup heuristics with the token-bound ACK gate and remove
  public test injection controls.
- Report commit — this report.

## Tests

- `cargo test -p harness-pi --test pi_harness` — 17 passed against a real Docker
  daemon and isolated labelled Debian containers with a mounted stub Pi. Coverage
  includes container-only paths, absent model-selection flags, distinct live and
  conversation JSONL, incremental partial-line delivery, persistence after
  container removal, empty/malformed/torn resume behavior, TERM→KILL process-group
  cleanup, duplicate rejection, drop cleanup, missing Docker, and missing Pi.
  Round-2 coverage adds an ordered Pi 0.84.3 JSON fixture, both assistant backend
  failure shapes, deliberate known-record ignores, trusted-header stripping,
  forged session control files/PGIDs, and bounded cleanup of a TERM-ignoring
  descendant. Round-3 coverage adds retry-success and final-failure sequences
  in authoritative Pi order across incremental polls and harness restart,
  malicious conversation writes against all host-private metadata, and cleanup
  after invalid JSON, invalid UTF-8 reader failure, and a silent supervisor
  header timeout. Round-4 coverage removes `--init`, proves zombie-only groups
  are accepted only after every runnable descendant is gone across stop, drop,
  and startup failure, deterministically delays `/proc` visibility after an
  event-thread spawn failure, and verifies undeclared `message` records increase
  the unknown counter. Round-5 coverage delays Docker acceptance, injects a
  reader failure, and proves through a body marker that Pi never executes after
  the host returns a startup error.
- `cargo test -p orchestrator-worker --test health` — 3 passed; half-window
  inactivity warning/reset, inactivity and wall-clock failures, and sustained
  CPU saturation.
- Current toolchain: `cargo build --workspace`, `cargo test --workspace`,
  `cargo clippy --workspace --all-targets -- -D warnings`, and
  `cargo fmt --all -- --check` — passed, including real Docker, PostgreSQL, and
  Git suites.
- Rust 1.85: the same complete build/test/clippy/fmt gate — passed, including
  the seventeen real-Docker Pi harness tests.

## Concerns

- Live InferWeave inference was not exercised; the installed Pi 0.84.3 CLI/help
  and package source were used as the authoritative process/session interface.
- Production runtime mount provisioning is intentionally unchanged in this fix
  round; the upcoming runtime contract must bind the host worktree at
  `/workspace` and only `session_root/conversation/` at `/session`, leaving the
  session root host-private.
