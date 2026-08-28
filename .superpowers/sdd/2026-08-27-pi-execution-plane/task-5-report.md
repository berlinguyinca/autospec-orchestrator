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
- production lifecycle composition over execution-storage, git-worktree, runtime, harness, and evidence traits.

## Commits

- `8ac7143` — Prevent worker oversubscription with durable capability-backed reservations
- `4de91d9` — Keep execution failures isolated while preserving recovery authority
- This report's commit — Fence worker control and cleanup authority at durable boundaries

## Verification

- `cargo fmt --all` and `git diff --check` — passed.
- `cargo check --workspace` — passed on current stable.
- `cargo test --workspace` — passed in full on current stable, including real Docker/Pi/Git/PostgreSQL suites.
- `cargo clippy --workspace --all-targets -- -D warnings` — passed on current stable.
- `cargo +1.85.0 check --workspace` — passed.
- `cargo +1.85.0 clippy --workspace --all-targets -- -D warnings` — passed.
- `cargo +1.85.0 test --workspace` — passed in full, including 37 real Docker/Pi, 18 real Docker runtime, 58 real Git, and 12 real PostgreSQL tests.
- Focused Task 5 suite — passed: API route integration 2/2, autospec-worker 2/2, scheduler 4/4, worker lifecycle/health/recovery/evidence 12/12, PostgreSQL 12/12.
- PostgreSQL concurrency test launched 16 simultaneous reservations against four slots and assigned exactly four unique executions.
- The first current-stable full run encountered one Pi crash-helper supervisor timeout (`crashed_pi_resume_recovers_before_any_session_mutation`); its isolated rerun and the subsequent complete workspace rerun both passed.

## Concerns

- The production boundaries are individually covered by their owning real-service suites and compiled together by `SystemExecutionLifecycle`; this change does not add a second monolithic real-service worker E2E fixture.
- Same-worker process-restart adoption is not yet wired into the production daemon. The recovery abstractions fail closed on uncertain authority, and controller-side `UNREACHABLE` recovery prevents duplicate attempts, but a fresh daemon does not yet reconstruct and resume an already-running Pi session.
- Recovery policy remains authority classification only. Retry and workflow decisions remain outside this repository as required.
