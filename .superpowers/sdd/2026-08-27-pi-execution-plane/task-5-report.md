# Task 5 — Worker scheduling, lifecycle, and recovery

## Status

Implemented the worker execution plane for issues #19–#22:

- capability-backed worker registration and bounded authenticated heartbeats;
- authenticated `/api/v1/workers` registration/list/heartbeat routes with server-owned liveness and a 90-second `UNREACHABLE` reaper;
- durable PostgreSQL workers, reservations, attempts, and atomic execution/event progress;
- transactional `FOR UPDATE` / `SKIP LOCKED` reservation without slot, CPU, or memory oversubscription;
- ordered storage → Git → runtime → compact Pi packet → incremental event polling → evidence → reverse cleanup lifecycle;
- per-execution cancellation and panic containment with aggregate cleanup errors;
- durable cleanup authority that survives task/process failure and prevents lower-layer release after uncertain Pi/runtime cleanup;
- controller recovery that atomically fences unreachable attempts, requeues resumable executions, marks ephemeral executions `WorkerLost`, and releases slot reservations;
- production lifecycle composition over execution-storage, git-worktree, runtime, harness, and evidence traits;
- same-worker restart adoption that reconstructs the exact storage receipt, Git owner, Docker container capability, Pi hold, session, attempt, and reservation before resuming incremental polling;
- startup adoption before the reservation loop, with fail-closed handling for incomplete or mismatched durable authority and no duplicate allocation, repository, runtime, Pi session, or attempt;
- a separate-process crash/restart integration that exercises PostgreSQL, real Git, real Docker, a stub Pi executable, durable evidence, and reverse cleanup end to end.

Round 2 hardening additionally provides:

- one advisory-lock-backed PostgreSQL event sequence allocator shared by direct append, progress, and unreachable-worker recovery transactions;
- phase-by-phase cleanup authority handles for storage, Git, runtime, Pi, running, and retained review state, with exact recovery fallback through the storage journal, Git owner, runtime labels, and Pi lifecycle holds;
- attempt-fenced reservation release so cleanup from an old attempt cannot free reassigned capacity;
- resumable `ReviewReady` retention until explicit cleanup, while ephemeral terminal executions still tear down in strict reverse order;
- exact restart inventory of the agent, every manifest service container, the execution network, and volumes, including exact ownership-label validation before adoption;
- configured Docker endpoint propagation through capability proof, runtime stats, harness inspect/control, adoption, and cleanup;
- reusable constant-time API token validation in `orchestrator-api::auth`, with worker routes included in the public versioned router;
- daemon per-record recovery isolation, reservation-error backoff without dropping active tasks, and heartbeat-404 re-registration;
- removal of the scheduler's pre-capability `LIMIT 64`, which could starve compatible work behind older incompatible queued rows.

Round 3 closes the remaining disposition and crash-recovery gaps:

- one typed, transactional cleanup state machine: `ACTIVE:<stage>` → `RETAIN_REQUESTED` / `CLEANUP_PENDING` → physical cleanup checkpoints → `RESERVATION_RELEASED` → `RESOLVED`;
- atomic `ReviewReady` progress, event allocation, and retention request, followed by unconditional capacity release for retained resumable executions;
- authenticated explicit cleanup requests plus daemon startup and periodic reconciliation of retained/pending authorities;
- startup fencing and persistence-mode classification before capacity release, including adoption of valid resumable post-Pi authority while the execution row is still `Provisioning`;
- physical cleanup checkpointing after Pi stop, runtime destroy, interrupted Git-create recovery, storage release, and reservation release, so later database failures resume below already-confirmed boundaries;
- journal-aware recovery of partial Git creation even when no worktree owner record was committed;
- a real seven-stage child-process crash matrix and a concurrent peer-isolation scenario over PostgreSQL, Git, Docker, execution storage, and stub Pi.

Round 4 makes capacity release and physical cleanup crash-safe:

- one PostgreSQL transaction now commits `Retained` and releases the exact attempt reservation/capacity under execution, authority, attempt, and worker locks; injected transaction failure proves all-or-nothing behavior and idempotent replay;
- the cleanup-disposition migration derives legacy `ReviewReady` handling from the manifest persistence mode, retaining only explicit resumable executions and conservatively scheduling ephemeral or ambiguous rows for cleanup;
- unreachable-worker reaping fences the old attempt and capacity behind `CleanupPending`; the scheduler excludes every execution with unresolved cleanup authority, and only post-cleanup finalization requeues resumable work or fails ephemeral work as `WorkerLost`;
- Git destruction and storage release leave authenticated, fsynced tombstones in pinned metadata outside the deleted resource; worker recovery acknowledges them only after the matching PostgreSQL disposition checkpoint commits;
- explicit cleanup accepts only exact `Retained` authority, rejecting active executions with HTTP 409 rather than creating an unsafe active-to-cleanup transition;
- the real crash matrix now covers nine boundaries, including Git physical deletion and storage physical deletion before their PostgreSQL checkpoints.

Round 5 closes the remaining fencing, event-truthfulness, and cleanup-finalization gaps:

- controller fencing is idempotent for the exact already-finished attempt and every cleanup subphase, so a fresh worker process can restart after the authenticated controller reaper without aborting startup or duplicating execution;
- fencing no longer publishes `ExecutionFailed` while the execution row is still assigned/running; the atomic finalizer alone publishes a state-matching `ExecutionRequeued`, `ExecutionFailed`, `ReviewReady`, `ExecutionCompleted`, or `ExecutionCancelled` event through the shared sequence allocator;
- one `finalize_cleanup` PostgreSQL transaction releases exact capacity when present, updates execution and attempt disposition, resolves cleanup authority, and appends the final event; exact fenced/terminal evidence permits idempotent completion when capacity was already released;
- ephemeral `ReviewReady` cleanup preserves the trusted result and `ReviewReady` state while removing disposable resources and capacity; resumable `ReviewReady` still retains its allocation until explicit cleanup;
- storage release authenticates the receipt, labels, Ready/Releasing journal, backend identity, and Docker bind before creating and fsyncing the external tombstone immediately before the first physical mutation;
- Git and storage acknowledge absence only for exact `NotFound`; dangling symlinks, permission failures, and other I/O errors fail closed without removing the external tombstone;
- the real nine-stage matrix now performs authenticated worker registration, controller heartbeat reaping, and a fresh OS-process worker cleanup/finalization at the post-runtime crash boundary.

## Commits

- `8ac7143` — Prevent worker oversubscription with durable capability-backed reservations
- `4de91d9` — Keep execution failures isolated while preserving recovery authority
- `194680f8e170c1c02e73b937e69dad3cd44809ee` — Fence worker control and cleanup authority at durable boundaries
- `4567709` — Resume exact Pi authority after worker process loss
- `1c2e8f9` — Keep worker recovery authoritative across every durable boundary
- `c8345c5` — Make worker cleanup disposition durable across crashes
- `12f42cfb6e54974c707a011bfa6a7e86fdc100e3` — Keep capacity fenced until cleanup is durably acknowledged
- This report's commit — Publish cleanup outcomes only with atomic final disposition

## Verification

- `cargo fmt --all -- --check` and `git diff --check` — passed.
- `AUTOSPEC_DATABASE_URL=… cargo test --workspace -- --test-threads=1` — passed on current stable, including 38 real Docker/Pi, 18 real Docker runtime, 60 real Git, 22 real PostgreSQL, 11 worker lifecycle, and 6 worker real-E2E target tests.
- `cargo clippy --workspace --all-targets -- -D warnings` — passed on current stable.
- `AUTOSPEC_DATABASE_URL=… cargo +1.85.0 test --workspace -- --test-threads=1` — passed in full with the same real boundary suites.
- `cargo +1.85.0 clippy --workspace --all-targets -- -D warnings` — passed.
- Focused Task 5 suite — passed: API route integration 3/3, autospec-worker 3/3, scheduler 4/4, worker lifecycle 11/11, health 3/3, recovery/evidence 3/3, PostgreSQL 22/22, execution storage 31/31, and Git worktree 60/60.
- Worker real-E2E target — passed 6/6. Three are substantive tests: `crashed_worker_is_adopted_across_postgres_git_docker_pi_evidence_and_cleanup`, `real_failure_stage_matrix_reconciles_without_resource_leaks`, and `real_cleanup_uncertainty_does_not_destabilize_a_concurrent_peer`; three are child-process helpers and are not counted as scenarios.
- The nine-stage matrix crashes after reservation, storage, interrupted Git create, runtime, Git physical delete before disposition, storage physical delete before disposition, Pi-before-event, ReviewReady-before-retention, and retention; each fresh manager reconciliation proves the required retained/resolved disposition, released reservation, and absence of exact Docker/Git/storage/Pi leaks after explicit cleanup.
- The peer-isolation test runs two real executions concurrently, crashes one after runtime creation, proves the other reaches `ReviewReady` with durable evidence and intact retained resources, then reconciles and cleans each exact authority independently.
- The post-runtime matrix boundary additionally re-registers through the authenticated HTTP API, lets the controller mark the worker `UNREACHABLE` and fence the attempt, then launches a fresh test process that idempotently accepts the controller fence and performs exact physical cleanup plus atomic finalization.
- PostgreSQL concurrency test launched 16 simultaneous reservations against four slots and assigned exactly four unique executions.

## Concerns

- The monolithic E2E uses a test-only fixed-capacity filesystem backend so it can run safely where an APFS/LVM pool is unavailable. Production storage remains configured through APFS/LVM and fails closed when that capability is absent.
- PostgreSQL advisory locking serializes the three production-shaped real worker scenarios inside one test binary; their subprocess helpers do not acquire the lock and are excluded from the scenario count.
- A first parallel full-workspace run hit the pre-existing Pi `crash_before_pgid_binding_leaves_token_recoverable_hold` timing race under concurrent Docker load. The exact test passed immediately in isolation, and both current and Rust 1.85 full workspaces passed with `--test-threads=1`; no Task 5 code touches the Pi handshake.
- Startup adoption accepts only exact, live authority: a Ready storage receipt, pinned worktree/session layout, matching Git owner, immutable Docker container proof and mounts, the complete manifest resource set, and one matching Pi hold. One invalid record is retained or cleaned independently and does not abort the daemon or create a duplicate workload.
- Recovery policy remains authority classification only. Retry and workflow decisions remain outside this repository as required.

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
