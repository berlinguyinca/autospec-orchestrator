# Task 4 Report

## Status

Complete after review fix round 6. The Pi 0.84.3 harness now launches only
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
validation and event-pump registration. The supervisor then emits a distinct
READY record carrying the same unpredictable token before it can exec Pi, and
the host does not register or report startup success until its trusted stdout
reader validates that exact confirmation. A dropped ACK, malformed READY,
wrong-token replay, timeout, or EOF therefore retains the trusted PGID and Docker
client for bounded cleanup; those control records are consumed without losing
the first byte of subsequent Pi JSON output. Invalid, unreadable, timed-out, or
thread-spawn startup failures close the launch gate, so no accepted future launch
can enter the Pi body. The removed token-free stability heuristic is no longer
part of correctness. Group reap uses Linux `/proc` PGID and non-zombie membership
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
- `3e0acab` — require an exact token-bound READY proof before startup succeeds.
- Report commit — this report.

## Tests

- `cargo test -p harness-pi --test pi_harness` — 20 passed against a real Docker
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
  the host returns a startup error. Round-6 coverage adds a Docker proxy that
  forwards the trusted header but consumes the ACK, plus malformed and replayed
  wrong-token READY records; all three startup paths fail, reap their supervisor,
  and leave the Pi body marker absent. The normal live-event test also proves
  READY consumption preserves the first Pi JSON record without leaking either
  control record into the durable event stream.
- `cargo test -p orchestrator-worker --test health` — 3 passed; half-window
  inactivity warning/reset, inactivity and wall-clock failures, and sustained
  CPU saturation.
- Current toolchain: `cargo build --workspace`, `cargo test --workspace`,
  `cargo clippy --workspace --all-targets -- -D warnings`, and
  `cargo fmt --all -- --check` — passed, including real Docker, PostgreSQL, and
  Git suites.
- Rust 1.85: the same complete build/test/clippy/fmt gate — passed, including
  the twenty real-Docker Pi harness tests.

## Concerns

- Live InferWeave inference was not exercised; the installed Pi 0.84.3 CLI/help
  and package source were used as the authoritative process/session interface.
- Production runtime mount provisioning is intentionally unchanged in this fix
  round; the upcoming runtime contract must bind the host worktree at
  `/workspace` and only `session_root/conversation/` at `/session`, leaving the
  session root host-private.

## Task 5 fix round 6

Cleanup finalization is now deliberately split across the database and external
metadata boundaries. The atomic PostgreSQL operation commits execution, attempt,
result, capacity release, and any required state-change event while advancing the
durable cleanup authority only to `RESERVATION_RELEASED`. That authority remains
listable until the worker durably acknowledges both authenticated Git and storage
tombstones; only then does an exact phase CAS advance it to `RESOLVED`. Normal,
adopted, startup-recovery, and periodic-reconciliation paths all use this order.
An acknowledgment error therefore preserves enough authority for a fresh worker
to retry without reopening deleted allocation storage.

Cleanup finalization allocates an event only when it changes the logical execution
state. Existing `ReviewReady`, `Failed`, `Completed`, and `Cancelled` outcomes keep
their already-published event, so retained explicit cleanup and recovery cannot
duplicate terminal events. A reservation-released replay authenticates the exact
attempt/worker authority before accepting idempotent completion, but does not
require an execution-to-reservation join after capacity has already been released.
Startup fencing is restricted to `Active`/`CleanupPending` authorities with the
exact live reservation; later physical-cleanup phases proceed directly to
finalization or tombstone acknowledgment.

### Round 6 verification

- `cargo test -p orchestrator-worker --test run` — 12 passed, including injected
  storage-ACK failure followed by successful fresh-worker authority recovery.
- `cargo test -p orchestrator-persistence --test postgres` with the real task
  PostgreSQL instance — 22 passed. The transaction fault regression proves
  capacity and `STORAGE_RELEASED` remain unchanged on failure; the replay
  regression rejects a forged authority worker; finalization remains idempotent
  and emits no duplicate `ReviewReady`.
- `cargo test -p orchestrator-worker --test real_e2e -- --nocapture` with real
  PostgreSQL, Git, Docker, and stub Pi — 6 test functions passed (3 substantive,
  3 guarded child helpers). The failure matrix covers 11 named crash/fault stages,
  including ephemeral runtime loss and a separate-process exit after DB finalize
  but before external ACK. Fresh recovery removes the exact tombstones and
  resources, resolves the authority, and observes exactly one `ReviewReady` or
  `ExecutionFailed` event as applicable. Restart adoption plus later explicit
  retained cleanup also preserves exactly one `ReviewReady`.
- Current toolchain: `cargo build --workspace`, serialized
  `cargo test --workspace`, `cargo clippy --workspace --all-targets -- -D warnings`,
  and `cargo fmt --all -- --check` — passed, including all real Docker,
  PostgreSQL, Git, Pi, runtime, and worker E2E suites.
- Rust 1.85.0: the same complete build, serialized test, clippy, and formatting
  gates passed.

### Round 6 concerns

- The production storage backends still require their configured APFS/LVM pools;
  the worker E2E intentionally uses the existing fixed-capacity filesystem test
  backend while retaining real PostgreSQL, Git, Docker, and Pi process boundaries.
