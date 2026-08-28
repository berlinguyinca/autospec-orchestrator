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

## Commits

- `8ac7143` — Prevent worker oversubscription with durable capability-backed reservations
- `4de91d9` — Keep execution failures isolated while preserving recovery authority
- `194680f8e170c1c02e73b937e69dad3cd44809ee` — Fence worker control and cleanup authority at durable boundaries
- This report's commit — Resume exact Pi authority after worker process loss

## Verification

- `cargo fmt --all` and `git diff --check` — passed.
- Current stable exact-source verification passed as an equivalent segmented workspace gate: `cargo test --workspace --exclude harness-pi --exclude runtime-docker -- --test-threads=1`, `cargo test -p harness-pi --test pi_harness`, and the 18-test runtime-docker integration suite.
- `cargo clippy --workspace --all-targets -- -D warnings` — passed on current stable.
- `cargo +1.85.0 clippy --workspace --all-targets -- -D warnings` — passed.
- `AUTOSPEC_DATABASE_URL=… cargo +1.85.0 test --workspace -- --test-threads=1` — passed in full, including 38 real Docker/Pi, 18 real Docker runtime, 58 real Git, and 15 real PostgreSQL tests.
- Focused Task 5 suite — passed: API route integration 2/2, autospec-worker 3/3, scheduler 4/4, worker lifecycle 10/10, health 3/3, recovery/evidence 3/3, and PostgreSQL 15/15.
- Separate-process crash/restart E2E — passed 2/2: a helper process reserves and starts Pi, exits without `Drop`, and a fresh lifecycle/storage/worker instance adopts the exact session and attempt, observes two total Pi invocations (initial plus resume), persists diff/evidence, and removes runtime resources.
- PostgreSQL concurrency test launched 16 simultaneous reservations against four slots and assigned exactly four unique executions.
- Concurrent workspace-wide Docker gates from another process caused one anonymous-volume observation and one 90-second E2E startup timeout. Both failed cases passed immediately in isolation; the exact-source segmented current gate and complete Rust 1.85 gate passed after removing only aborted, exactly identified test resources.

## Concerns

- The monolithic E2E uses a test-only fixed-capacity filesystem backend so it can run safely where an APFS/LVM pool is unavailable. Production storage remains configured through APFS/LVM and fails closed when that capability is absent.
- Startup adoption accepts only exact, live authority: a Ready storage receipt, pinned worktree/session layout, matching Git owner, immutable Docker container proof and mounts, the complete manifest resource set, and one matching Pi hold. One invalid record is retained or cleaned independently and does not abort the daemon or create a duplicate workload.
- Recovery policy remains authority classification only. Retry and workflow decisions remain outside this repository as required.
