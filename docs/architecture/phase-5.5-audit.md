# Phase 5.5 completion audit

Audit date: 2026-08-29
Evidence refreshed: 2026-08-29T19:36:39-07:00
Implementation baseline: `3ff8b5e` on `feat/pi-execution-plane`
Database-isolation repair baseline: `8e44ca9`
Scanner baseline: `06c0c5e`
Scope: the execution plane only; no AutoSpec planning policy or InferWeave
model-serving behavior was added.

## Verdict

The Docker execution path is supported by a real, persisted end-to-end test and
the repository's normal workspace gates on both the current compiler and Rust
1.85. The test begins at the authenticated v1 HTTP manifest endpoint and ends
after label-scoped cleanup, while retaining content-addressed evidence and the
gapless durable event stream.

The audit is deliberately conditional for physical aggregate storage
exhaustion. The configured APFS/LVM tests ran and reported an explicit skip
because this host has no operator-provisioned execution-storage pool. The audit
did not create, repartition, or delete host storage to manufacture that proof.
Podman and Apptainer are also unavailable and remain ineligible future adapters.

## Environment and reproducibility

| Boundary | Audited value | Evidence |
| --- | --- | --- |
| Current Rust | `rustc 1.98.0 (88d9e12ae 2026-08-18)` | `rustc --version` |
| MSRV | `rustc 1.85.0 (4d91de4e4 2025-02-17)` | `rustup run 1.85.0 rustc --version` |
| Docker | client/server `29.2.1` | `docker version --format ...` |
| PostgreSQL | `postgres:17.6-bookworm` | disposable container inspection |
| Database | `autospec_test_task9fix3_20260829` | destructive-test guard accepted it |
| Pi CLI | `0.84.3` at `/opt/homebrew/bin/pi` | `pi --version` |

PostgreSQL ran in the disposable container
`autospec-task9fix3-pg-20260829`, bound only to `127.0.0.1:49850`, with the labels
`autospec.managed=true` and
`autospec.execution_id=task9-fix3-20260829`. Commands below used
`AUTOSPEC_DATABASE_URL` pointing at that database; credentials are intentionally
omitted from this durable record.

The current-toolchain gate was rerun at `3ff8b5e`:

```text
cargo fmt --all -- --check
cargo build --workspace
AUTOSPEC_DATABASE_URL="$DISPOSABLE_TEST_DATABASE" cargo test --workspace -- --nocapture
cargo clippy --workspace --all-targets -- -D warnings
```

Result: exit 0 on 2026-08-29. The real worker suite passed 14/14 in
296.54s, including the Task 9 chain, crash adoption, independent peer
isolation, credential containment, failure-stage recovery, cancellation, and
exact cleanup. This rerun also exposed the original process-local PostgreSQL
test-contamination guard. That guard was subsequently superseded at `8e44ca9`
by the cross-process evidence below.

The database-isolation repair was tested against a freshly recreated
`autospec_test_task9fix4_20260829` database. Every database-mutating integration
binary now proves the disposable database identity and holds the same
PostgreSQL session advisory lock for its complete mutation scope. Persistence
migrates before its guarded whole-database reset. The direct two-session lock
regression passed, followed by the adversarial cross-binary reproduction:

```text
# launched first and kept live while the second binary attempted its reset
cargo test -p orchestrator-worker --test real_e2e \
  task9_manifest_runs_through_real_worker_and_exact_cleanup_with_durable_evidence \
  -- --exact --nocapture

# launched concurrently
cargo test -p orchestrator-persistence --test postgres \
  new_controller_rows_remain_replayable_by_prior_controllers_during_rollout \
  -- --exact --nocapture
```

Result: both commands exited 0. Task 9 passed in 10.86s; the prior-controller
test passed in 5.32s after waiting for the shared lock instead of truncating
Task 9's live rows. The full persistence suite then passed 41/41, API database
integration passed 10/10, and the worker evidence integration test passed.
Current-toolchain clippy with warnings denied passed for the three affected
crates, and Rust 1.85 `cargo check --all-targets` passed for those crates. This
focused repair evidence supplements rather than replaces the complete
workspace gates recorded below.

The Rust 1.85 gate used the rustup binary explicitly because Homebrew's `cargo`
preceded the rustup proxy on this host. The latest complete rerun covered the
final staged-directory and killed-production-probe closure, using a distinct
`target-rust185-dirfdfix3` target directory:

```text
CARGO_TARGET_DIR=target-rust185-dirfdfix3 rustup run 1.85.0 cargo fmt --all -- --check
CARGO_TARGET_DIR=target-rust185-dirfdfix3 rustup run 1.85.0 cargo build --workspace
CARGO_TARGET_DIR=target-rust185-dirfdfix3 AUTOSPEC_DATABASE_URL="$DISPOSABLE_TEST_DATABASE" \
  rustup run 1.85.0 cargo test --workspace -- --nocapture
CARGO_TARGET_DIR=target-rust185-dirfdfix3 rustup run 1.85.0 cargo clippy \
  --workspace --all-targets -- -D warnings
```

Result: exit 0 on 2026-08-29. The default-parallel real worker suite passed
14/14 in 383.59s, and clippy completed with warnings denied. The matching
current-toolchain fmt/build/workspace-test/clippy chain also exited 0; its real
worker suite passed 14/14 in 950.64s. The first current-toolchain test attempt
had one unrelated five-second Pi drop-bound miss under shared-host parallel
load; that exact case passed in isolation in 9.94s, and the complete unmodified
workspace gate then passed on rerun. This is recorded as a superseded failed
attempt, not folded into the successful result.

The Unix-specific closure also ran in labeled, disposable Linux/aarch64 Rust
1.85 containers. `cargo test -p execution-storage --all-targets --locked`
passed all 34 unit, 7 backend, and 35 storage tests. The base image omitted
clippy, so the first clippy invocation was an environmental failure; after a
fresh labeled container installed the Rust 1.85 clippy component,
`cargo clippy -p execution-storage --all-targets --locked -- -D warnings`
exited 0. No test container or disposable database was retained.

The immediately preceding Rust 1.85 attempt at `a3aeb33` did **not** establish
a green full workspace gate: one real parallel case timed out under host load,
although that exact case passed when rerun in isolation, and fmt, build, and
clippy exited 0. That qualified evidence is retained here rather than being
reported as a full pass; the complete `3ff8b5e` rerun above supersedes it.

This Mac was not a dedicated unloaded runner: unrelated Docker services and the
Docker virtualization process remained active. No competing Cargo, rustc, or
autospec test process was present before the latest gate. The recorded exit 0
therefore proves the full gate on this shared host, but it is not represented as
dedicated-host performance evidence.

## Manifest-to-cleanup proof

`task9_manifest_runs_through_real_worker_and_exact_cleanup_with_durable_evidence`
in `crates/orchestrator-worker/tests/real_e2e.rs` proves this chain without an
in-memory persistence substitute:

1. It sends an authenticated `POST /api/v1/executions` request containing an
   `autospec.dev/v1alpha1` manifest.
2. PostgreSQL stores the canonical manifest and the initial `Created` event.
3. A registered, capability-proven worker reserves the persisted execution.
4. The worker creates an execution-owned Git worktree and durable owner record.
5. Docker creates the isolated agent, network, and requested real
   `redis:7-alpine` service using the exact ownership labels.
6. A deterministic Pi JSON-mode fixture consumes the serialized task packet
   exactly once inside that boundary and writes the execution result.
7. Before cleanup PostgreSQL contains exactly four gapless lifecycle events:
   `Created`, `EnvironmentReady`, `AgentStarted`, and `ReviewReady`.
8. The result is stored once as a SHA-256 content-addressed blob plus scoped
   artifact metadata.
9. Cancellation drives exact reverse cleanup of the execution's Pi, Docker,
   worktree, and storage authorities.
10. The test captures the exact IDs, names, and complete five-label ownership
    maps for both containers and the network (and any owned volumes), then proves
    every captured resource and the label-scoped resource set are absent.
11. The execution record, complete five-event gapless stream including the
    single cancellation event, artifact metadata, and SHA-256-verified artifact
    bytes remain durable after disposable resources are absent.

The installed Pi CLI was inspected and versioned, but the deterministic E2E
uses the repository's Pi-compatible fixture rather than issuing a paid or
credentialed external inference request. This preserves a repeatable execution
boundary test while recording live inference as an explicit gap below.

## Invariant evidence map

| AGENTS invariant | Executed evidence | Result |
| --- | --- | --- |
| 1. Every source mutation uses an isolated worktree | Task 9 E2E verifies the owned worktree before the Pi fixture writes; `git-worktree` tests reject owner/receipt drift and prove mirror refs and objects remain unchanged | Pass |
| 2. Conflicting executions get isolated runtime environments | Task 9 verifies execution-scoped agent, service, network, credentials, session, repository, and storage paths; Docker conformance checks distinct names and labels | Pass for Docker |
| 3. Implementation and independent review share no mutable runtime state | `real_cleanup_uncertainty_does_not_destabilize_a_concurrent_peer` runs disjoint implementation/review executions and proves peer resources and progress survive the other's cleanup uncertainty | Pass |
| 4. Harness sessions persist independently of containers | `durable_session_survives_agent_container_removal` and `paused_worker_is_adopted_across_a_separate_process_with_zero_live_pi_holds` prove session/adoption state survives container and worker-process loss | Pass |
| 5. Resources are disposable; records and evidence are durable | Task 9 re-checks every captured Docker ID/name and the label-scoped set after cleanup, then re-reads the complete event stream and SHA-256-verifies the retained artifact bytes | Pass |
| 6. Resources carry both labels; cleanup selects only those labels | Task 9 compares each resource's complete ownership map, including worker/repository/issue metadata, and cleanup/conformance uses the exact managed+execution selector | Pass |
| 7. Global Docker prune and `git branch | grep ... | xargs` are forbidden | `repository_invariants::executable_sources_never_gain_global_cleanup_or_inference_placement` scans crate production sources, build scripts, root Dockerfiles/executable scripts, deployment configuration, and CI workflows; committed fixtures exercise positive and negative cases | Pass |
| 8. No unrestricted host Docker by default | `autospec-worker` operability tests render the constrained proxy topology and prove the worker has no host socket mount; startup fails closed without explicit proxy opt-in | Pass |
| 9. One failure cannot destabilize a peer or worker | Independent peer cleanup test, full real failure-stage matrix, and cancellation test | Pass for exercised Docker/Git paths; aggregate physical-pool proof blocked |
| 10. Runtime, not prompts, enforces limits | Frozen Docker conformance proves CPU/memory/read-only-root constraints; Git ENOSPC tests prove exact rollback; configured storage tests require real quota/reserve proof or fail/skip explicitly | Pass except unavailable physical-pool exercise |

## Storage-exhaustion containment

The normal suite executes real Git ENOSPC injection at clone, checkout, owner
record, and owner-commit phases. These tests prove that the exact partial
execution repository rolls back, its execution filesystem is recoverable, and
the shared bare mirror is not mutated.

`configured_storage_enforces_one_aggregate_quota_and_preserves_another_execution`
additionally records a sentinel in the shared mirror outside both execution
allocations. When an operator-provisioned quota pool is present, the test fills
one execution to real aggregate ENOSPC and requires all of the following before
passing: the failing allocation is contained, the peer allocation stays
writable, and the shared mirror sentinel is byte-for-byte unchanged.

On this host that test reported:

```text
SKIP configured Docker aggregate quota: operator storage pool is absent
SKIP real storage quota lifecycle: operator pool configuration is absent
```

This is not counted as physical ENOSPC proof. Providing
`AUTOSPEC_APFS_PROBE_PATH` (or the Linux LVM equivalent) for a deliberately
provisioned operator pool remains a deployment-readiness prerequisite.

## Task-context duplication audit

The E2E stores a distinctive task goal and searches every durable table. During
the mixed-controller rollout window, the goal exists in the canonical
`executions.manifest` and in the temporary compatibility copy at
`execution_requests.manifest`; it is absent from worker, reservation, attempt,
event, cleanup, control, cancellation, artifact-metadata, and artifact-blob
records. The Pi fixture records exactly one task-packet invocation.

The audit originally caught a duplicate full manifest in
`execution_requests`. Migration
`0013_prepare_request_manifest_contract.sql` is the safe expand phase: it
makes the legacy column nullable, preserves populated pre-upgrade rows, and
allows prior-version controllers to keep inserting during a mixed rollout. New
controllers temporarily dual-write the legacy column so a prior controller can
replay a new row using its non-optional manifest decoder. New-controller replay
still compares against the canonical `executions.manifest` in the same
transaction, preserving same-request and conflicting-request concurrency
semantics. This compatibility duplication is the sole deliberate exception to
the steady-state task-context efficiency claim.
Artifact bytes remain stored once by content hash, with metadata referring to
the blob rather than duplicating it.

After the minimum supported controller contract excludes the prior reader, a
separate contract release must first stop the legacy dual-write and verify no
old controller can serve traffic; only a later migration may drop the column.
For rollback from that future release, stop every newer controller and backfill
only the legacy compatibility column from the canonical execution row:

```sql
UPDATE execution_requests AS request
SET manifest = execution.manifest
FROM executions AS execution
WHERE execution.id = request.execution_id
  AND request.manifest IS NULL;
```

Only after that backfill may the older controller be started. Dropping the
legacy column is a separate future contract migration after the rollback window
closes; migration 0013 must not be changed into that contraction.

## Boundary scan

`crates/orchestrator-core/tests/repository_invariants.rs` scans production Rust,
crate build scripts, root Dockerfiles and executable scripts, deployment
configuration, and CI workflows. It rejects:

- inference/model placement calls such as `selectGpu`, `loadModel`, `gpuQueue`,
  and `modelPlacement`;
- global Docker `system`, `container`, `image`, `network`, or `volume` prune;
- the forbidden `git branch` plus `grep` plus `xargs` cleanup pipeline.

Committed positive fixtures cover shell continuations, URL-prefixed shell/YAML/
Dockerfile commands, attached `&&` and pipe operators, configurable or wrapped
Docker binaries, constructed Rust commands, Git pipelines, and whitespace-
separated model-placement calls. Negative fixtures cover syntax-specific
comments, Rust validation literals, URL arguments, `git worktree prune`, and
other non-Docker prune commands. Docker cleanup is rejected only when a Docker
command invokes the `system`, `container`, `image`, `network`, or `volume`
prune family. Every violation retains file and line evidence. Neutral manifest
model policy remains data transported to the harness boundary; this repository
does not select hardware, load models, serve inference, or decide model
placement.

## Explicit gaps and unavailable adapters

- No operator APFS/LVM execution pool was configured, so physical aggregate
  quota exhaustion was explicitly skipped. No destructive host provisioning was
  attempted.
- Podman was unavailable/ineligible and its conformance entry explicitly
  skipped. Its adapter intentionally exposes detection only.
- Apptainer was unavailable/ineligible and its conformance entry explicitly
  skipped. Its adapter intentionally exposes detection only.
- The real Pi CLI is installed, but no credentialed external model request was
  made; deterministic JSON-mode Pi boundary behavior is the tested contract.
- Production InferWeave credential issuance is an external integration boundary;
  real E2E uses the execution-scoped, short-lived local credential broker.
- The macOS directory-FD race is closed with the approved direct `rustix`
  filesystem boundary. The originally supplied directory is opened with
  `O_DIRECTORY|O_NOFOLLOW|O_CLOEXEC` and compared with its initial `lstat`
  before canonical-path diagnostics are computed; production child directories
  are captured with `openat` from the retained parent descriptor. New metadata
  subdirectories are first created under an unpredictable, exclusively claimed
  operation name. The retained parent descriptor is used to record the staged
  inode, immediately open and authenticate it, and commit it to the requested
  final name with `RENAME_NOREPLACE`; both the now-absent staged source and the
  final inode are authenticated after the rename. Any staged or final identity
  mismatch preserves every object for inspection and never deletes an
  unauthenticated entry. Journal and
  secure-metadata mutations hold a shared process-local mutex together with an
  advisory lock on that descriptor. Independently captured stores coordinate
  through the advisory lock, while calls through the same instance or a clone
  cannot bypass it through process-local `flock` re-entrancy. No-replace commits
  and atomic exchanges carry both source and target inode identities through
  the operation and authenticate the final canonical target plus the displaced
  exchange side before any cleanup. An unauthenticated post-operation side is
  preserved; it is never mutated by an attempted rollback or deletion.

  Operation temporaries and ownership probes use process, nanosecond-epoch, and
  atomic-counter components with bounded exclusive-create collision retries.
  Unauthenticated crash temporaries and stale probes are ignored and preserved
  rather than deleted, and reconciliation fails closed after 1,024 such entries
  instead of accepting unbounded accumulation. Ownership probes are unlinked
  while their descriptor remains open, with unwind cleanup covering the
  create-to-unlink interval. Deterministic public-flow race tests cover root and
  child capture, source and final-target swaps during create/replace, remove,
  staged-subdirectory creation before open, before commit, and after commit,
  subdirectory cleanup, journal create/write/removal, lock exclusion, collision
  retry, both ownership-probe unwind cuts, and a genuinely killed subprocess
  blocked inside the production `verify_current_owner` probe path while proving
  attacker sentinels and displaced trusted inodes remain byte-for-byte intact.

  Unix does not provide an inode-conditional `unlinkat`: a continuously active
  process with the same uid can ignore the advisory lock and replace a verified
  tombstone in the final instruction window before unlink. The implementation
  therefore revalidates immediately before unlink and never treats the lock as
  a security boundary. Runtime deployment must preserve the existing invariant
  that untrusted agents do not share the worker host uid or writable metadata
  directory; the tests prove fail-closed behavior for a replacement at every
  exposed verify-to-mutate barrier, not containment of a fully privileged
  same-uid host adversary.

These gaps are explicit rather than inferred as passes. The supported release
claim is the Docker execution plane with the tested storage-proof fail-closed
gate; it is not a claim that unavailable adapters or an unconfigured physical
quota pool were exercised.
