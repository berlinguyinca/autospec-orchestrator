# Task 5 — Worker scheduling, lifecycle, and recovery

## Status

Implemented the worker execution plane for issues #19–#22:

- capability-backed worker registration and bounded authenticated heartbeats;
- durable PostgreSQL workers, reservations, attempts, and atomic execution/event progress;
- transactional `FOR UPDATE` / `SKIP LOCKED` reservation without slot, CPU, or memory oversubscription;
- ordered storage → Git → runtime → compact Pi packet → incremental event polling → evidence → reverse cleanup lifecycle;
- per-execution cancellation and panic containment with aggregate cleanup errors;
- startup recovery that adopts only exact valid authority, otherwise records `WorkerLost`, and fails closed on uncertain inspection;
- production lifecycle composition over execution-storage, git-worktree, runtime, harness, and evidence traits.

## Commits

- `8ac7143` — Prevent worker oversubscription with durable capability-backed reservations
- `4de91d9` — Keep execution failures isolated while preserving recovery authority

## Verification

- `cargo fmt --all -- --check` — passed.
- `cargo build --workspace` — passed on current stable.
- `AUTOSPEC_DATABASE_URL=… cargo test --workspace -- --test-threads=1` — passed in full on current stable.
- `cargo clippy --workspace --all-targets -- -D warnings` — passed on current stable.
- `cargo +1.85.0 check --workspace` — passed.
- `cargo +1.85.0 clippy --workspace --all-targets -- -D warnings` — passed.
- `AUTOSPEC_DATABASE_URL=… cargo +1.85.0 test --workspace -- --test-threads=1` — passed in full, including 37 real Docker/Pi, 18 real Docker runtime, 58 real Git, and 9 real PostgreSQL tests.
- Focused Task 5 suite — passed: autospec-worker 2/2, scheduler 4/4, worker lifecycle/health/recovery 9/9, PostgreSQL 9/9.
- PostgreSQL concurrency test launched 16 simultaneous reservations against four slots and assigned exactly four unique executions.
- The first current-stable full run encountered one Pi crash-helper supervisor timeout (`crashed_pi_resume_recovers_before_any_session_mutation`); its isolated rerun and the subsequent complete workspace rerun both passed.

## Concerns

- The worker HTTP route/auth implementation is owned by the orchestrator-api lane and is not changed here; autospec-worker sends the required bearer token to the frozen `/workers` and heartbeat routes.
- The production boundaries are individually covered by their owning real-service suites and compiled together by `SystemExecutionLifecycle`; this change does not add a second monolithic real-service worker E2E fixture.
- Recovery policy remains authority classification only. Retry and workflow decisions remain outside this repository as required.
