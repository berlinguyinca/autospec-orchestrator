# Task 7 Report: Isolation, scoped credentials, and interactive Pi

## Outcome

Task 7 is complete. Implementation and independent review executions now have
real concurrent boundary proof across Docker, Git, Pi, credentials, mutable
binds, and cleanup authority. The production worker fails closed without an
injected credential issuer; an explicit local-development opt-in mints one
short-lived credential beneath the verified execution root, mounts it read-only
into only that execution's agent, and revokes it on normal, cancellation,
failure, recovery, and repeated cleanup paths. Authenticated pause, resume, attachment,
and Pi conversation fork operations are durable worker-owned controls rather
than controller-only state changes.

## Scope delivered

- Added the exact `CredentialBroker::mint(&Execution)` and
  `CredentialBroker::revoke(&ExecutionId)` boundary and the reserved
  `EnvironmentHandle.credentials_path` field.
- Added a development/test-only local execution-scoped broker because no
  InferWeave issuance endpoint is specified. Production startup rejects it
  unless `--allow-local-development-credentials` (or its explicit environment
  equivalent) is supplied. It creates opaque 256-bit material from the operating system,
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
- Added cursor-only attachment metadata: execution, state, session, opaque workspace reference,
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
8. Review-fix RED tests showed controls needed immutable acceptance fences and
   crash phases. `ACCEPTED`, `APPLYING`, `SIDE_EFFECT_APPLIED`, `COMPLETED`, and
   `STALE` now preserve the exact worker, attempt, source session, worktree,
   execution version/state, deterministic fork target, and stale disposition.
   The unreleased schema remains one authoritative `0012`; frozen fence columns
   deliberately do not cascade from mutable attempt rows.
9. Separate child processes now exit immediately after Pi stop, resume launch,
   native fork launch, and `SIDE_EFFECT_APPLIED` before completion. Fresh worker
   processes adopt the exact authority and reconcile through the production
   daemon tick. The tests prove one durable action event. Debugging found two
   concrete recovery defects: Running adoption resumed before pending controls,
   and hold validation compared an immutable Docker ID with a container name.
   Adoption now quiesces first, checks durable controls before resuming, and
   fences holds against the resolved exact container ID.
10. The adversarial Pi fixture reads its own credential, copies it into the
    worktree, and echoes it. The worktree scan rejects evidence persistence and
    cleanup removes the execution. A real Pi test separately proves recognized
    terminal polling while both stdout JSONL and stderr logs contain only the
    replacement marker. This RED test also found that stderr had previously
    bypassed scrubbing; both streams now pass through known-secret redaction
    before durable writes.
11. The concurrent isolation E2E now provisions a real Redis service for each
    execution and inspects exact agent/service container IDs, one distinct
    network membership, execution-bounded binds, and absence of the credential
    mount from services. After cleanup of one execution it reinspects the peer
    service, runtime, session, credential, evidence, and cleanup authority.
12. Fix-round-two RED tests proved that a control left at
    `SIDE_EFFECT_APPLIED` needs post-side-effect liveness reconciliation, not
    another native side effect. Four separate-process cuts now cover pause,
    resume, paused fork, and running fork. Resume and running fork relaunch the
    exact already-persisted target session; pause and paused fork remain
    quiescent. Each reaches one eventual action event without replaying the
    native fork or task packet.
13. Cancellation/control races now resolve inside the same locked transaction:
    pending cancellation rejects a new control, cancellation and terminal
    progress stale every nonterminal control phase, and `begin_control`
    revalidates authority even after a row reached `APPLYING` or
    `SIDE_EFFECT_APPLIED`. The daemon gives cancellation precedence and cannot
    enqueue a control in the same tick.
14. Adopted paused executions initialize their health monitor in the paused
    epoch. Fake-clock tests exceed both wall and inactivity limits over long and
    repeated pauses, then resume without consuming automatic crash-resume
    budget.
15. Provisioning now persists label-scoped cleanup authority before Docker
    provision starts. A real Docker fixture creates a partial resource, injects
    both provision and first rollback failure, restarts through the production
    worker entrypoint, removes the failed execution by its exact selector, and
    proves the peer remains live. This test exposed an anonymous image-volume
    leak: exact container removal used Docker's `v=false`. Changing only that
    exact removal to `v=true` makes the regression pass without broad cleanup.
16. Broker mint/revoke operations are serialized per execution within the
    local process. Concurrent tests prove one mint winner and deterministic
    mint/revoke ordering; only `NotFound` means absent. Adoption requires the
    same unexpired path and authority and rejects rotation or expiry rather than
    replacing a bind-mounted inode.
17. Attachment now locks the execution before reading authority and cursor in
    one transaction. Secret containment scans the materialized task-packet file,
    Pi event and stderr logs, artifacts, evidence, cleanup/control rows,
    executions, and `execution_attempts`; the adversarial credential is absent
    from every durable surface.
18. The first post-fix MSRV rerun intentionally failed the freshness gate: it
    reused the current run's database, so a fixed-ID sequence test observed old
    events. No code change was made. Both full toolchain gates were restarted
    against distinct empty databases and passed.
19. Round-three PostgreSQL barriers reproduced a real `40P01` deadlock between
    cancellation and `begin_control`: cancellation locked the execution first
    while the control path had locked the control row first. Every control,
    cancellation, terminal-progress, and cleanup transition now acquires the
    execution row first and then its exact control/cleanup row. Barriers at
    `APPLYING`, `SIDE_EFFECT_APPLIED`, and completion prove cancellation wins
    without a stranded control row or durable action event. These persistence
    barriers do not claim that an already-started native side effect is absent.
20. An attachment race initially returned metadata while retained cleanup had
    already locked its authority row for transition. Attachment now locks the
    execution and matching cleanup authority, in that order, before reading
    state, retention, and event cursor. The deterministic race proves the
    resulting snapshot is coherent or conflicts after cleanup begins.
21. Credential publication now owns each temporary candidate with an RAII
    guard from creation until successful publication. Mint and revoke scavenge
    only exact `.inferweave.credential.<64-lowercase-hex>.tmp` regular files;
    symlinks and other non-regular candidates fail closed and unrelated names
    are never deleted. Error, crash-residue, revoke, and concurrent tests prove
    bounded cleanup. If the filesystem refuses both guard cleanup and later
    scavenging, the bounded candidate is deliberately retained rather than
    risking an unsafe path deletion.
22. The strengthened side-effect recovery test initially observed the exact
    resumed Pi session but not the recovery boundary because `RunningRestored`
    was emitted only by one resume branch. The checkpoint now follows both
    branches. Each child asserts its shared observer saw the exact recovered
    execution/session, no `ReviewReady` existed before the cut, and the later
    terminal event has a post-recovery sequence.
23. Startup authority reconciliation is now one public worker routine used by
    the `autospec-worker` binary and the separate-process recovery fixtures.
    Both control crash replacement and partial-Docker rollback replacement call
    that exact production routine rather than reconstructing startup behavior
    in the test. Secret containment directly scans the materialized task packet,
    Pi stdout/stderr, remaining execution files, and these durable tables:
    `artifact_blobs`, `execution_requests`, `execution_control_requests`,
    `execution_cancellation_requests`, `reservations`, `workers`, `executions`,
    `execution_attempts`, `execution_events`, `cleanup_authorities`, and
    `artifacts`.
24. Round four corrected an overstatement in the prior report: the
    partial-Docker rollback fixture had used same-process
    `reconcile_daemon_tick` even though the report described a fresh process
    using production startup reconciliation. The original child now reserves
    the execution, proves `ACTIVE:RUNTIME` authority exists before its first
    labelled Docker side effect, fails provision and its first exact rollback,
    and exits. A fresh replacement child invokes the exact public
    `Worker::reconcile_startup`, resolves only that authority, removes its
    labelled network, and leaves a labelled peer network intact.
25. Cleanup finalization no longer relies on PostgreSQL's plan for a joined
    multi-table row lock. It explicitly locks the execution row first and then
    the exact cleanup-authority row. A real PostgreSQL barrier observes the
    blocked finalizer query, races attachment against it, and proves both
    finish without `40P01` while attachment sees the coherent post-finalization
    conflict.
26. Credential candidates are now armed for RAII removal immediately after
    `create_new`, before either content write or `fsync`. Injected initial-write
    and post-write fsync-boundary failures both prove zero candidate residue;
    the existing exact-name, regular-file-only scavenging rules remain intact.
27. Startup cancellation lookup failure is isolated per durable authority.
    Structured logs carry `worker_id`, `execution_id`, and `attempt_id`; the
    uncertain authority remains durable while later authorities continue
    through recovery. The regression injects failure for the first record and
    proves only the later record resolves.
28. Credential race tests now cross actual operating-system threads. Barriers
    release simultaneous mint/mint and mint/revoke calls. Immediately after
    both threads join, before any remint or scavenging, the mint/revoke test
    requires zero exact candidates and permits only an absent final credential
    or one complete exact final credential. A subsequent remint separately
    proves live authority. Injecting an exact validated candidate made this
    pre-remint assertion fail before the injection was removed for GREEN.
29. The live `resume_side_effect` resource identified after round four was the
    residue of the expected RED recovery run at 05:14. Its complete labels
    resolved container
    `b2ec22de91030d4a00a9124dbfb8fae98c437e66df0838ec1db489c7a91880d8`
    and network
    `2d4959b051d0d6e27e8672e6837ac3a4e1811ade382b6c41b44d68526af8dc03`
    to execution `execution-cc-rse-93541-1788005657794257000`, attempt
    `attempt-b5d4ac775e0b44539b6b87809dc3e434`, and its exact verified storage
    receipt, Git owner, Pi session, and private credential. The original
    `autospec_red` PostgreSQL database belonged to the temporary
    `autospec-task7-round3-pg` gate and had been explicitly removed after the
    run, so no current PostgreSQL row falsely claimed authority. The archived
    command result proves the replacement child had restored the exact session
    but failed the newly added observer assertion; the parent therefore exited
    before its cleanup block. The same scenario passed after the observer fix.
30. Exact production reconciliation of that old authority exposed a separate
    recovery defect: runtime cleanup used the provisioning constructor, so the
    now-expired bound credential blocked Docker destruction. A RED real-Docker
    regression reproduced the failure. `RuntimeFactory::build_for_cleanup`
    now constructs only receipt- and label-scoped teardown authority and never
    mints workload credentials. The GREEN path destroyed the exact runtime,
    revoked the expired credential, destroyed and acknowledged the exact Git
    worktree, released and acknowledged storage, and removed the test root.
    The four-cut control recovery scenario then passed on a fresh PostgreSQL
    database, with no matching container, network, volume, execution, control,
    cleanup authority, credential, storage root, or current temporary root in
    the post-run audit.
31. Final breaker review found that cleanup still inherited current provisioning
    verifier authority in two ways: the factory trait supplied a credentialful
    default, and Docker cleanup construction compared the persisted receipt's
    proof method with the worker's current verifier image and command. The trait
    now requires every implementation to provide `build_for_cleanup` explicitly.
    The production implementation constructs only receipt-, daemon-, path-, and
    full-label-bound Docker cleanup authority. It receives no credential broker,
    live Ready verifier, or current trusted-verifier configuration. Provisioning
    retains its immutable verifier proof comparison unchanged.
32. The existing real partial-provision restart scenario now creates its receipt
    under the original verifier configuration, writes an expired credential,
    and restarts `Worker::reconcile_startup` with a different verifier image and
    command. RED left the durable authority at `RuntimeStopped`; GREEN removed
    only the target runtime, revoked the expired credential, resolved the exact
    authority, and preserved the peer network. Constructor-level tests also
    reject mismatched worker labels, irregular allocation paths, and foreign
    Docker daemon receipts before removing the target.

## Verification

- The strengthened OS-thread credential test was run RED with an injected exact
  validated candidate immediately after the joins; it failed at the new
  pre-remint assertion. With the injection removed, the complete 9-test
  credential suite passed.
- `expired_bound_credential_does_not_block_exact_runtime_cleanup` was run RED
  against real Docker and failed because the provisioning constructor rejected
  the expired bound credential. It passed after cleanup gained its credential-
  independent constructor.
- `partial_docker_provision_and_rollback_failure_recovers_by_exact_selector_after_restart`
  was strengthened and run RED on
  `autospec_task7_fix6_red_20260829`: the provision child passed, while the
  restart child using a different verifier image and command left cleanup at
  `RuntimeStopped`. It passed after the cleanup-only constructor stopped
  accepting current verifier configuration. The provisioning constructor's
  old-proof rejection, expired credential revocation, exact target removal, and
  peer survival are assertions in the same regression.
- `cargo test -p runtime-docker cleanup_constructor_ -- --nocapture --test-threads=1`
  — passed both focused cleanup authority tests, including a real Docker daemon
  mismatch that preserved the exact target until trusted test cleanup.
- The exact four-cut `control_side_effect_crash_cuts_reconcile_in_a_fresh_process_exactly_once`
  scenario passed to completion on the fresh
  `autospec_task7_fix5_focus_20260829` database.
- `AUTOSPEC_DATABASE_URL=.../autospec_task7_fix5_current_final_20260829 cargo test --workspace -- --test-threads=1`
  — passed on its own empty database, including 35 PostgreSQL tests, 5 execution
  API tests, 40 Pi harness tests, all 13 real worker E2Es, 17 Docker runtime
  unit tests, 9 credential tests, and 18 real Docker runtime tests.
- `AUTOSPEC_DATABASE_URL=.../autospec_task7_fix5_msrv_final_20260829 rustup run 1.85.0 cargo test --workspace -- --test-threads=1`
  — passed on a separate empty database with the same full serialized workspace
  coverage.
- `AUTOSPEC_DATABASE_URL=.../autospec_task7_fix6_current_final_20260829 cargo test --workspace -- --test-threads=1`
  — passed on its own fresh database, including 35 PostgreSQL tests, 5 execution
  API tests, 40 Pi harness tests, all 13 real worker E2Es, 17 Docker runtime
  unit tests, 9 credential tests, and 20 real Docker runtime tests.
- `AUTOSPEC_DATABASE_URL=.../autospec_task7_fix6_msrv_final_20260829 rustup run 1.85.0 cargo test --workspace -- --test-threads=1`
  — passed on a distinct fresh database with the same complete serialized
  workspace coverage.
- `cargo build --workspace` — passed.
- `rustup run 1.85.0 cargo build --workspace` — passed.
- `cargo clippy --workspace --all-targets -- -D warnings` — passed.
- `rustup run 1.85.0 cargo clippy --workspace --all-targets -- -D warnings` — passed.
- `cargo fmt --all -- --check` — passed.
- `git diff --check` — passed.
- The intentional fix-six RED authority was recovered through production
  `Worker::reconcile_startup`; its exact runtime and expired credential were
  removed, its cleanup authority reached `Resolved`, its peer network was
  removed by exact name, and its remaining test root was moved to the user's
  Trash. Reinspection found no matching Docker resource or live temporary root.
- The round-four post-gate audit found and removed one empty `peer-task7`
  network but missed the live Task 7 resume RED residue described above. Round
  five resolved it through production cleanup and found six additional
  filesystem-only control RED roots whose PostgreSQL and Docker authorities
  were already absent. Those six roots and the deliberately failed expired-
  credential regression root were moved by exact path to the user's Trash, so
  they remain recoverable. Reinspection after both final workspace gates found
  zero current Task 7 control-recovery or expired-cleanup Docker resources,
  PostgreSQL execution/control/cleanup rows, credentials, storage, or temporary
  roots; older Task 5/runtime resources owned by other work remain untouched.

## Commits

- `9ed494d` — exact credential broker contract, local scoped broker, worker
  wiring, cleanup revocation, and initial tests.
- `aa6e7ce` — durable interactive API/worker controls, real isolation proof,
  conversation fork semantics, and production reconciliation coverage.
- `3986942` — idempotent revocation after already-released storage.
- `2a73e64` — allocation-free token encoding accepted by current and Rust 1.85
  clippy.
- `aee776b` — reviewer fix round: fenced control phases, crash reconciliation,
  fail-closed credential startup, secret containment, timer suspension, opaque
  attach snapshot, service-backed isolation, and 0011→0012 upgrade proof.
- `13b84ad` — fix round two: cancellation precedence, paused-health adoption,
  exact post-side-effect liveness, broker serialization, atomic attachment, and
  restart-safe partial-provision cleanup.
- `c4ab8b4` — fix round three: post-create candidate ownership, explicit
  recovery-boundary proof, shared startup reconciliation, and execution-first
  cleanup attachment locking.
- `7993a95` — fix round four: explicit cleanup locking, immediate credential
  candidate ownership, fresh-process partial rollback, and isolated startup
  cancellation recovery.
- `b3b6035` — fix round five: keep durable runtime cleanup independent of
  expired workload authority.
- Fix round six is recorded with this updated report in the following commit.

## Concerns and follow-up

- The local broker is deliberately development/test-only because the contract
  specifies no live InferWeave issuance or revocation API. A production issuer
  must be injected through the existing broker boundary. Replacing it later
  should preserve the same injected broker boundary, path containment, expiry,
  agent-only read-only mount, and cleanup proof; it must not introduce model or
  inference policy here.
- The local broker's safe-Rust per-execution lock is process-local. There is no
  dependency-free portable `dirfd`/cross-process locking primitive in this
  implementation, so the development broker must not be shared by concurrent
  worker processes. Production remains fail-closed until an injected issuer
  provides cross-process mint/revoke authority.
- Durable controls guarantee restart-visible intent and exactly-once durable
  completion/events. A host crash after Pi starts but before PostgreSQL
  completion can leave a lifecycle hold; harness recovery terminates that exact
  process before idempotent retry. Any uncommitted
  native fork JSONL remains confined to the execution-private session directory
  and is removed with that execution's storage.
- Attachment intentionally provides resumable metadata and an event cursor,
  not a websocket terminal or conversation dump. A richer Workbench transport
  can build on these references without copying prior context into prompts.
