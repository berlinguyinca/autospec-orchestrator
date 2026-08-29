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
- Cancellation is a durable request rather than a controller-side terminal
  transition. The exact worker task observes it, stops Pi, performs reverse
  cleanup, releases its reservation, and only then atomically records one
  terminal `Cancelled` event.
- Reused `orchestrator-core` execution, manifest, and event DTOs throughout.
- Added uniform versioned API errors and the exact 1,048,576-byte request-body
  limit to all JSON route groups.
- Added resumable SSE using `Last-Event-ID` or `cursor`, bounded durable batches,
  replay-before-live behavior, lag recovery, and PostgreSQL polling for events
  written directly by workers. Broadcasts are shared wakeups only; payloads
  never advance a subscriber cursor ahead of the durable log.
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
- `0011_cancellation_requests.sql` stores independently durable cancellation
  requests so a controller restart cannot lose intent or publish completion
  before the worker has terminated and cleaned the workload.
- Artifact files use `$AUTOSPEC_STATE_ROOT/artifacts/<first-two-sha256-bytes>/<sha256>`.
  Existing content is revalidated before reuse; symlinks and irregular path
  components fail closed.
- No new external dependency was introduced. SHA-256 is implemented locally and
  checked against standard test vectors.

## TDD evidence

Initial Task 6 RED was observed before implementation for each major contract:

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

The reviewer fix round also followed RED to GREEN:

1. Empty-token startup validation initially failed to compile because
   `ServerConfig::validate` did not exist; missing and malformed bearer inputs
   also reached the validator. Startup now rejects both API and worker empty
   tokens, and malformed headers fail before constant-time validation.
2. A malformed SSE cursor produced Axum's non-uniform rejection, a durable
   sequence could be overtaken by a later broadcast payload, and an unrelated
   broadcast flood starved direct durable events until timeout. Uniform cursor
   validation and shared-wakeup bounded durable draining now pass all three
   regressions.
3. Cancellation tests initially failed to compile without durable request
   methods. The real controller-worker hung-Pi regression then remained
   `Running`, exposing a cleanup-fence conflict. The corrected flow terminates
   the exact task within the test bound, cleans all resources, releases its
   reservation, and emits exactly one terminal `Cancelled` event.
4. Artifact barrier tests initially failed to compile without the no-clobber
   installer. They now prove corrupt and symlink winners are rejected without
   overwriting their targets and concurrent same-digest writers converge only
   on a re-opened, type/identity/size/digest-verified final file.
5. PostgreSQL regressions cover concurrent same-key create/retry replay,
   different-key execution-ID collision handling, bounded event batches, and a
   pre-Task-6 migration fixture containing `0001`-`0004` and `0006`-`0009` that
   preserves data while the current migrator applies missing `0005` and `0010`.

## Verification

Final verification used newly created, labelled PostgreSQL 17 containers and
serialized workspace test runs on both toolchains:

- `AUTOSPEC_DATABASE_URL=... cargo test --workspace -- --test-threads=1`
  — passed, including 26 PostgreSQL persistence tests, 4 execution API contract
  tests, the production evidence-adapter test, and 7 real worker E2E tests.
- `AUTOSPEC_DATABASE_URL=... cargo +1.85.0 test --workspace -- --test-threads=1`
  — passed with the same full workspace coverage.
- `cargo build --workspace` — passed.
- `cargo clippy --workspace --all-targets -- -D warnings` — passed.
- `cargo +1.85.0 clippy --workspace --all-targets -- -D warnings` — passed.
- `cargo fmt --all -- --check` — passed.
- `git diff --check` — passed.

## Commits

- `bc38191` — production implementation, migrations, and test coverage.
- `f6aae6a` — initial Task 6 SDD report.
- `3bb8fd6` — reviewer hardening for cancellation ownership, ordered SSE,
  authentication, uniform errors, artifact installation, and race coverage.
- This updated report is recorded in the following documentation-only commit.

## Concerns and follow-up

- Each active SSE subscriber performs a 500 ms durable-event poll and drains at
  most 128 ordered records per query. This is correct, bounded, and resumable,
  but a future high-volume deployment may prefer PostgreSQL notification fanout
  while retaining the database as the source of truth.
- Artifact installation atomically hard-links a same-directory pending file to
  a previously absent final name, then re-opens and verifies the winner's type,
  identity, size, and digest before committing metadata. Under the safe-Rust,
  no-new-dependency constraint there is no pinned directory-handle primitive;
  therefore read-only `.pending-*` hardlinks are deliberately retained instead
  of risking deletion through a swapped path. This is fail-closed but can leave
  bounded-by-upload orphan files for later trusted-root garbage collection.
- A process already privileged to mutate the trusted state root could still
  race after final verification. Directory-fd-relative installation and pinned
  file-handle metadata would be the appropriate future hardening when the
  portability/dependency policy admits those primitives.
