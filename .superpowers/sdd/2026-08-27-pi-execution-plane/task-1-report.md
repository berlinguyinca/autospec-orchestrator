# Task 1 Report: Durable Execution Foundation

## Outcome

Implemented the Task 1 foundation for execution persistence, append-only event replay,
strict manifest/environment validation, and correlation-aware JSON telemetry.

## Changes

- Added `orchestrator-persistence` as a workspace crate with the object-safe
  `ExecutionStore` and `EventLog` contracts.
- Added PostgreSQL-backed `PgExecutionStore` with durable inserts, reads, live-record
  filtering, row-locked legal transitions, version increments, and duplicate conflict
  mapping.
- Added PostgreSQL-backed `PgEventLog` with transaction-scoped per-execution advisory
  locking, gapless sequence allocation, one conflict retry, ordered replay, and a
  500-event batch cap.
- Added migrations `0001_executions.sql` and `0002_execution_events.sql` using the
  shared TEXT/JSONB/TIMESTAMPTZ conventions and fixed migration numbering.
- Added the shared append-only `StoreError` variants required by downstream store
  owners.
- Added `ExecutionManifest::validate` for the exact API version, repository shape,
  nonzero/minimum resources, tagged or digested images, and lowercase capability and
  service identifiers.
- Added `EnvironmentFile` parsing and requested-name-only service resolution from the
  map-shaped repository environment file into manifest service lists.
- Added JSON telemetry initialization, flattened execution correlation fields, and
  case-insensitive field-name redaction for token, secret, password, and API-key data.
- Added workspace dependencies `sqlx` and `regex`, crate dependencies, and lockfile
  updates.

## TDD Evidence

The first focused test run failed at compilation for the deliberately missing
`ExecutionManifest::validate`, `EnvironmentFile`, telemetry functions, persistence
traits, and PostgreSQL store types. Implementation followed only after those failures
were observed.

After implementation:

- `cargo test -p orchestrator-core` — 11 passed.
- A labelled disposable PostgreSQL 17 container ran
  `AUTOSPEC_DATABASE_URL=postgres://postgres:autospec@127.0.0.1:50485/autospec cargo test -p orchestrator-persistence -- --nocapture`
  — 4 passed, including eight concurrent gapless appenders and concurrent transition
  serialization. The exact container was removed afterward.
- `cargo test --workspace` — all workspace unit, integration, and doc tests passed.
- `cargo clippy --workspace --all-targets -- -D warnings` — passed.
- `cargo fmt --all -- --check` — passed.
- `cargo build --workspace` — passed.
- `git diff --check` — passed.

## Notes and Remaining Risks

- PostgreSQL integration tests print an explicit skip reason when
  `AUTOSPEC_DATABASE_URL` is absent or unreachable, per the repository test contract.
- `init_tracing` is exposed for the controller and worker binaries, but binary call-site
  changes were intentionally excluded because Task 1 ownership was limited to the
  persistence crate, core additions, and workspace manifests.
- Event sequence allocation uses a transaction-scoped PostgreSQL advisory lock keyed by
  `execution_id`; this provides the required gapless behavior without relying on an
  aggregate `SELECT ... FOR UPDATE`, which PostgreSQL does not permit.
