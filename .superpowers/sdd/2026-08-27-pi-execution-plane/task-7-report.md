# Task 7 Report: Isolation, scoped credentials, and interactive Pi

## Outcome

Task 7 is complete. Implementation and independent review executions now have
real concurrent boundary proof across Docker, Git, Pi, credentials, mutable
binds, and cleanup authority. The production worker mints one short-lived
credential beneath the verified execution root, mounts it read-only into only
that execution's agent, and revokes it on normal, cancellation, failure,
recovery, and repeated cleanup paths. Authenticated pause, resume, attachment,
and Pi conversation fork operations are durable worker-owned controls rather
than controller-only state changes.

## Scope delivered

- Added the exact `CredentialBroker::mint(&Execution)` and
  `CredentialBroker::revoke(&ExecutionId)` boundary and the reserved
  `EnvironmentHandle.credentials_path` field.
- Added a local execution-scoped broker because no InferWeave issuance endpoint
  is specified. It creates opaque 256-bit material from the operating system,
  stores it as a private `0600` file beneath a `0700` execution directory,
  records an expiry, reuses only an unexpired exact file, and revokes
  idempotently even after execution storage has already been released.
- Mounted the credential at `/autospec-credential` read-only into the exact
  agent container only. Services receive no credential bind. The nested
  `/run` target was rejected because it would overlap the agent's writable
  runtime bind.
- Wired credential minting before Docker/Pi startup and revocation through the
  operational runtime factory and every reverse-cleanup/recovery path. Missing
  mint, mount verification, or revocation proof fails closed.
- Added authenticated `/api/v1/executions/{id}/pause`, `/resume`, and
  GET/POST `/attach` routes with the existing bearer-auth contract, exact 1 MiB
  JSON limit, bounded idempotency keys, uniform errors, lifecycle conflicts,
  and `202 Accepted` for newly durable intent.
- Added cursor-only attachment metadata: execution, state, session, worktree,
  and latest durable event sequence. It never returns conversation history,
  artifact bodies, credentials, or prompt material.
- Added the authoritative `0012_execution_control_requests.sql` migration. The
  briefly considered `0013` split was folded into `0012` because neither
  migration had been committed or released and there was no compatibility
  reason to preserve two schema steps.
- Added worker reconciliation of ordered durable pause, resume, and
  conversation-fork requests through the same production daemon tick used for
  cancellation and cleanup recovery.
- Pause stops the owned Pi process before committing `PausedForHuman`; resume
  invokes Pi's native resume before committing `Running`. A conversation fork
  invokes Pi's native `--fork` primitive, keeps the exact source worktree,
  records a distinct session, and quiesces the new Pi process before completing
  the request when the execution remains paused.
- Restart adoption now accepts `PausedForHuman`, reconstructs the exact runtime
  and credential mount, restores the harness without falsely resuming Pi, and
  processes pending controls on later daemon ticks.
- Kept inference serving, model placement, token accounting, separation-of-
  duties policy, and AutoSpec work-selection policy outside this repository.

## Isolation proof

The concurrent production E2E test starts an implementation execution and an
independent review execution against real PostgreSQL, Git, Docker, verified
storage, and the Pi harness. While both are live it proves they do not share:

- Docker network, agent container, service container, or anonymous/named
  volume authority;
- execution root, worktree, Git common directory, or object database;
- Pi session directory, native conversation JSONL, live-event file, or session
  identifier;
- credential file or opaque token;
- any writable runtime bind; or
- durable cleanup authority.

It also verifies every mount is bounded beneath only its own execution root,
the credential bind is read-only, and neither credential token appears in
execution rows, attempts, events, cleanup records, artifacts, evidence, or task
packets. Cleanup of the failed implementation execution then proves the review
credential, session, worktree, container, network, evidence, and retained
cleanup authority remain intact before the peer is independently cleaned.

## TDD and debugging evidence

Task 7 was implemented RED to GREEN:

1. Credential tests initially failed to compile because the broker trait and
   result did not exist. Runtime tests then rejected the credential file because
   bind verification assumed every source was a directory, and real Docker
   rejected the first target because it nested inside the writable `/run` bind.
   The verified file-bind path and top-level read-only target now pass unit and
   real Docker coverage.
2. Broker tests prove per-execution unpredictability, expiry, private modes,
   peer-safe idempotent revoke, invalid-ID rejection, and repeated revocation
   after the execution root is absent. The last regression reproduced a full
   cleanup-order failure before the broker treated an absent exact path as
   already revoked.
3. Persistence tests initially failed without durable controls. They now prove
   ordered projected lifecycle validation, restart visibility from a newly
   opened store, request replay, idempotency conflicts, attempt/worktree
   fencing, and one atomic event/completion even when completion is repeated.
4. API tests initially failed without interactive routes. They now prove bearer
   auth, missing/invalid idempotency keys, legal and conflicting state order,
   body limits, replay status, fork mode validation, and metadata-only attach.
5. Worker tests initially could not interrupt a pending Pi poll. The control
   channel and daemon observer now interrupt the exact active execution, remain
   cancellation-responsive while paused, preserve worktree identity across a
   fork, and do not manually replay a task packet.
6. The production fork E2E first reached Pi and failed with `source session
   JSONL is missing`. Boundary tracing showed the API row, daemon selection, and
   worker delivery were correct; the test stub emitted live stdout records but,
   unlike real Pi and the harness fixture, did not create native conversation
   JSONL. Correcting that fixture moved the test through fork persistence.
7. The next deterministic failure was `unresolved Pi lifecycle hold` on resume.
   Pi's fork primitive starts a process while the durable execution remained
   paused. Quiescing the fork before completing the paused request fixed the
   ownership transition. The real test now proves pause, fork, and resume make
   exactly three Pi invocations, preserve one worktree, create a new session,
   reach `ReviewReady`, and emit exactly one paused, forked, and resumed event.

## Verification

- `AUTOSPEC_DATABASE_URL=... cargo test --workspace -- --test-threads=1`
  — passed, including 30 PostgreSQL tests, 5 execution API tests, 38 Pi harness
  tests, all 8 real worker E2Es, 3 credential tests, and 18 Docker runtime tests.
- `AUTOSPEC_DATABASE_URL=... cargo +1.85.0 test --workspace -- --test-threads=1`
  — passed with the same full serialized workspace coverage.
- `cargo build --workspace` — passed.
- `cargo +1.85.0 build --workspace` — passed.
- `cargo clippy --workspace --all-targets -- -D warnings` — passed.
- `cargo +1.85.0 clippy --workspace --all-targets -- -D warnings` — passed.
- `cargo fmt --all -- --check` — passed.
- `git diff --check` — passed.

## Commits

- `9ed494d` — exact credential broker contract, local scoped broker, worker
  wiring, cleanup revocation, and initial tests.
- `aa6e7ce` — durable interactive API/worker controls, real isolation proof,
  conversation fork semantics, and production reconciliation coverage.
- `3986942` — idempotent revocation after already-released storage.
- `2a73e64` — allocation-free token encoding accepted by current and Rust 1.85
  clippy.
- This report is recorded in the following documentation-only commit.

## Concerns and follow-up

- The production broker is deliberately local and scoped because the contract
  specifies no live InferWeave issuance or revocation API. Replacing it later
  should preserve the same injected broker boundary, path containment, expiry,
  agent-only read-only mount, and cleanup proof; it must not introduce model or
  inference policy here.
- Durable controls guarantee restart-visible intent and exactly-once durable
  completion/events. As with other process side effects, a host crash after Pi
  starts but before PostgreSQL completion can leave a lifecycle hold; existing
  harness recovery terminates that exact process before retry. Any uncommitted
  native fork JSONL remains confined to the execution-private session directory
  and is removed with that execution's storage.
- Attachment intentionally provides resumable metadata and an event cursor,
  not a websocket terminal or conversation dump. A richer Workbench transport
  can build on these references without copying prior context into prompts.
