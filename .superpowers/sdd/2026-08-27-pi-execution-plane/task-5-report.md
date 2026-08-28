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

## Commits

- `8ac7143` — Prevent worker oversubscription with durable capability-backed reservations
- `4de91d9` — Keep execution failures isolated while preserving recovery authority
- `194680f8e170c1c02e73b937e69dad3cd44809ee` — Fence worker control and cleanup authority at durable boundaries
- This report's commit — Resume exact Pi authority after worker process loss

## Verification

- `cargo fmt --all` and `git diff --check` — passed.
- `AUTOSPEC_DATABASE_URL=… cargo test --workspace -- --test-threads=1` — passed in full on current stable, including real Docker/Pi/Git/PostgreSQL suites.
- `cargo clippy --workspace --all-targets -- -D warnings` — passed on current stable.
- `cargo +1.85.0 clippy --workspace --all-targets -- -D warnings` — passed.
- `AUTOSPEC_DATABASE_URL=… cargo +1.85 test --workspace -- --test-threads=1` — passed in full, including 37 real Docker/Pi, 18 real Docker runtime, 58 real Git, and 12 real PostgreSQL tests.
- Focused Task 5 suite — passed: API route integration 2/2, autospec-worker 2/2, scheduler 4/4, worker lifecycle/health/recovery/evidence 13/13, PostgreSQL 12/12.
- Separate-process crash/restart E2E — passed 2/2: a helper process reserves and starts Pi, exits without `Drop`, and a fresh lifecycle/storage/worker instance adopts the exact session and attempt, observes two total Pi invocations (initial plus resume), persists diff/evidence, and removes runtime resources.
- PostgreSQL concurrency test launched 16 simultaneous reservations against four slots and assigned exactly four unique executions.
- The first current-stable full run encountered one Pi crash-helper supervisor timeout (`crashed_pi_resume_recovers_before_any_session_mutation`); its isolated rerun and the subsequent complete workspace rerun both passed.

## Concerns

- The monolithic E2E uses a test-only fixed-capacity filesystem backend so it can run safely where an APFS/LVM pool is unavailable. Production storage remains configured through APFS/LVM and fails closed when that capability is absent.
- Startup adoption accepts only exact, live authority: a Ready storage receipt, pinned worktree/session layout, matching Git owner, immutable Docker container proof and mounts, and one matching Pi hold. Incomplete or mismatched authority prevents daemon startup rather than creating a duplicate workload.
- Recovery policy remains authority classification only. Retry and workflow decisions remain outside this repository as required.
