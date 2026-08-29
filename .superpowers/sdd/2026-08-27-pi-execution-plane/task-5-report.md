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

## Commits

- `8ac7143` — Prevent worker oversubscription with durable capability-backed reservations
- `4de91d9` — Keep execution failures isolated while preserving recovery authority
- `194680f8e170c1c02e73b937e69dad3cd44809ee` — Fence worker control and cleanup authority at durable boundaries
- `4567709` — Resume exact Pi authority after worker process loss
- `1c2e8f9` — Keep worker recovery authoritative across every durable boundary
- This report's commit — Make worker cleanup disposition durable across crashes

## Verification

- `cargo fmt --all -- --check` and `git diff --check` — passed.
- `AUTOSPEC_DATABASE_URL=… cargo test --workspace` — passed on current stable, including 38 real Docker/Pi, 18 real Docker runtime, 59 real Git, 18 real PostgreSQL, 11 worker lifecycle, and 5 worker real-E2E tests.
- `cargo clippy --workspace --all-targets -- -D warnings` — passed on current stable.
- `AUTOSPEC_DATABASE_URL=… cargo +1.85.0 test --workspace` — passed in full with the same real boundary suites.
- `cargo +1.85.0 clippy --workspace --all-targets -- -D warnings` — passed.
- Focused Task 5 suite — passed: API route integration 3/3, autospec-worker 3/3, scheduler 4/4, worker lifecycle 11/11, health 3/3, recovery/evidence 3/3, PostgreSQL 18/18, and Git worktree 59/59.
- Worker real-E2E target — passed 5/5. Three are substantive tests: `crashed_worker_is_adopted_across_postgres_git_docker_pi_evidence_and_cleanup`, `real_failure_stage_matrix_reconciles_without_resource_leaks`, and `real_cleanup_uncertainty_does_not_destabilize_a_concurrent_peer`; two are child-process helpers and are not counted as scenarios.
- The seven-stage matrix crashes after reservation, storage, interrupted Git create, runtime, Pi-before-event, ReviewReady-before-retention, and retention; each fresh manager reconciliation proves the required retained/resolved disposition, released reservation, and absence of exact Docker/Git/storage/Pi leaks after explicit cleanup.
- The peer-isolation test runs two real executions concurrently, crashes one after runtime creation, proves the other reaches `ReviewReady` with durable evidence and intact retained resources, then reconciles and cleans each exact authority independently.
- PostgreSQL concurrency test launched 16 simultaneous reservations against four slots and assigned exactly four unique executions.

## Concerns

- The monolithic E2E uses a test-only fixed-capacity filesystem backend so it can run safely where an APFS/LVM pool is unavailable. Production storage remains configured through APFS/LVM and fails closed when that capability is absent.
- PostgreSQL advisory locking serializes the three production-shaped real worker scenarios inside one test binary; their subprocess helpers do not acquire the lock and are excluded from the scenario count.
- Startup adoption accepts only exact, live authority: a Ready storage receipt, pinned worktree/session layout, matching Git owner, immutable Docker container proof and mounts, the complete manifest resource set, and one matching Pi hold. One invalid record is retained or cleaned independently and does not abort the daemon or create a duplicate workload.
- Recovery policy remains authority classification only. Retry and workflow decisions remain outside this repository as required.
