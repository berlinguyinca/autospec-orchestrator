# Task 6 Report: Versioned execution API and durable evidence

## Outcome

Task 6 is complete. The daemon now serves the authenticated `/api/v1`
execution, event, artifact, and worker contracts from one operational
`AppState`. Execution state and events are committed durably before SSE
publication, retries remain explicit client requests rather than orchestrator
policy, and evidence is stored out of band as content-addressed blobs with
metadata-only API responses.

## Scope delivered

- Added authenticated create, read, cancel, and retry execution endpoints.
  Create and retry require idempotency keys; replay is stable and reuse with a
  different request is rejected.
- Reused `orchestrator-core` execution, manifest, and event DTOs throughout.
- Added uniform versioned API errors and the exact 1,048,576-byte request-body
  limit to all JSON route groups.
- Added resumable SSE using `Last-Event-ID` or `cursor`, durable sequence order,
  replay-before-live behavior, lag recovery, and PostgreSQL polling for events
  written directly by workers.
- Added SHA-256 content-addressed artifact storage, deduplication, per-execution
  associations, cross-execution checks, and metadata-only listing.
- Wired production worker evidence capture through `ArtifactStore`; artifact
  bytes and metadata are never added to the Pi task packet or manifest.
- Replaced the test-only worker router composition with the operational daemon
  composition over PostgreSQL execution, event, worker, reservation, cleanup,
  and artifact stores.
- Kept retry selection and scheduling policy outside this repository boundary:
  the API performs only the specific supplied create/cancel/retry operation.

## Persistence and migration numbering

- `0005_artifacts.sql` creates the content-addressed blob metadata and
  execution-artifact association tables. Although this task landed after
  migrations `0006` through `0009` already existed, `0005` was intentionally
  vacant and is the authoritative plan number. The migration runner sorts by
  migration version, so existing databases apply this missing version safely.
- `0010_execution_requests.sql` is the next collision-free migration and stores
  execution request idempotency mappings.
- Artifact files use `$AUTOSPEC_STATE_ROOT/artifacts/<first-two-sha256-bytes>/<sha256>`.
  Existing content is revalidated before reuse; symlinks and irregular path
  components fail closed.
- No new external dependency was introduced. SHA-256 is implemented locally and
  checked against standard test vectors.

## TDD evidence

RED was observed before implementation for each major contract:

1. Persistence artifact tests initially failed to compile because
   `ArtifactStore` and `PgArtifactStore` did not exist.
2. The live SSE test timed out when an event was appended directly by a worker,
   proving that broadcast-only delivery could miss durable external writers.
3. The reservation-label regression test showed an assigned reservation still
   carried the `unassigned` worker label.
4. The corruption test accepted an existing content-addressed blob without
   checking its digest.

The implementations were added only after those failures. All four regressions
are now covered by passing tests, alongside auth, uniform errors, body limits,
idempotency conflicts, legal and illegal lifecycle transitions, cursor replay,
gapless concurrent events, artifact dedupe/listing, and execution association.

## Verification

Final verification used a newly created, labelled PostgreSQL 17 container and a
serialized workspace test run:

- `AUTOSPEC_DATABASE_URL=... cargo +1.85.0 test --workspace -- --test-threads=1`
  — passed, including 23 PostgreSQL persistence tests, 3 execution API contract
  tests, the production evidence-adapter test, and 6 real worker E2E tests.
- `cargo build --workspace` — passed.
- `cargo clippy --workspace --all-targets -- -D warnings` — passed.
- `cargo fmt --all -- --check` — passed.
- `git diff --check` — passed.

## Commits

- `bc38191` — production implementation, migrations, and test coverage.
- The report itself is recorded in the following documentation-only commit.

## Concerns and follow-up

- Each active SSE subscriber performs a 500 ms durable-event poll so worker
  writes from other processes cannot be missed. This is correct and resumable,
  but a future high-volume deployment may prefer PostgreSQL notification fanout
  while retaining the database as the source of truth.
- Artifact path validation and atomic creation fail closed on the supported
  safe-Rust filesystem surface. A future platform-hardening task could adopt
  directory-handle-relative operations when the repository's portability and
  dependency policy provides that primitive.
