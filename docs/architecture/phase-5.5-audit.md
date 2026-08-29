# Phase 5.5 completion audit

Audit date: 2026-08-29  
Baseline: `003ead7` on `feat/pi-execution-plane`  
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
| Database | `autospec_test_task9audit20260829` | destructive-test guard accepted it |
| Pi CLI | `0.84.3` at `/opt/homebrew/bin/pi` | `pi --version` |

PostgreSQL ran in the disposable container
`autospec-task9-pg-20260829`, bound only to `127.0.0.1:61901`, with the labels
`autospec.managed=true` and
`autospec.execution_id=task9-audit-20260829`. Commands below used
`AUTOSPEC_DATABASE_URL` pointing at that database; credentials are intentionally
omitted from this durable record.

The current-toolchain gate was:

```text
cargo fmt --all -- --check
cargo build --workspace
AUTOSPEC_DATABASE_URL="$DISPOSABLE_TEST_DATABASE" cargo test --workspace -- --nocapture
cargo clippy --workspace --all-targets -- -D warnings
```

Result: exit 0. The real worker suite passed 14/14, including the Task 9 chain,
crash adoption, independent peer isolation, credential containment, failure
stage recovery, cancellation, and exact cleanup.

The Rust 1.85 gate used the rustup binary explicitly because Homebrew's `cargo`
preceded the rustup proxy on this host:

```text
CARGO_TARGET_DIR=target-rust185 rustup run 1.85.0 cargo fmt --all -- --check
CARGO_TARGET_DIR=target-rust185 rustup run 1.85.0 cargo build --workspace
CARGO_TARGET_DIR=target-rust185 AUTOSPEC_DATABASE_URL="$DISPOSABLE_TEST_DATABASE" \
  rustup run 1.85.0 cargo test --workspace -- --nocapture
CARGO_TARGET_DIR=target-rust185 rustup run 1.85.0 cargo clippy \
  --workspace --all-targets -- -D warnings
```

Result: exit 0. The default-parallel real worker suite passed 14/14 in 882.78s,
and clippy completed with warnings denied. The real-environment polling windows
are 60s so concurrent Docker tests measure behavior rather than a 15s host-load
assumption. The failing 15s cases were first reproduced in the parallel gate,
then passed in isolation, and the full parallel gate passed after this test-only
correction.

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
7. PostgreSQL contains exactly four gapless lifecycle events: `Created`,
   `EnvironmentReady`, `AgentStarted`, and `ReviewReady`.
8. The result is stored once as a SHA-256 content-addressed blob plus scoped
   artifact metadata.
9. Cancellation drives exact reverse cleanup of the execution's Pi, Docker,
   worktree, and storage authorities.
10. The execution record, event stream, artifact metadata, and artifact blob
    remain durable after disposable resources are absent.

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
| 5. Resources are disposable; records and evidence are durable | Task 9 asserts all execution resources absent after cleanup while the execution, event stream, artifact metadata, and verified blob remain | Pass |
| 6. Resources carry both labels; cleanup selects only those labels | Task 9 inspects agent/service/network labels; Docker cleanup/conformance and partial-provision recovery use the exact two-label selector | Pass |
| 7. Global Docker prune and `git branch | grep ... | xargs` are forbidden | `repository_invariants::executable_sources_never_gain_global_cleanup_or_inference_placement` scans executable crate and deployment sources | Pass |
| 8. No unrestricted host Docker by default | `autospec-worker` operability tests render the constrained proxy topology and prove the worker has no host socket mount; startup fails closed without explicit proxy opt-in | Pass |
| 9. One failure cannot destabilize a peer or worker | Independent peer cleanup test, full real failure-stage matrix, cancellation test, and configured aggregate-quota test's peer assertion | Pass for exercised Docker/Git paths |
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

The E2E stores a distinctive task goal and searches every durable table. The
goal exists only in the canonical `executions.manifest`; it is absent from
request, worker, reservation, attempt, event, cleanup, control, cancellation,
artifact-metadata, and artifact-blob records. The Pi fixture records exactly one
task-packet invocation.

The audit originally caught a duplicate full manifest in
`execution_requests`. Migration
`0013_remove_request_manifest_duplication.sql` removes that column. Idempotency
replay now compares against the canonical `executions.manifest` in the same
transaction, preserving conflict semantics without a second payload copy.
Artifact bytes remain stored once by content hash, with metadata referring to
the blob rather than duplicating it.

## Boundary scan

`crates/orchestrator-core/tests/repository_invariants.rs` scans executable Rust
and deployment sources and rejects:

- inference/model placement calls such as `selectGpu`, `loadModel`, `gpuQueue`,
  and `modelPlacement`;
- global Docker `system`, `container`, `image`, `network`, or `volume` prune;
- the forbidden `git branch` plus `grep` plus `xargs` cleanup pipeline.

The scanner was proven red with a temporary forbidden-source probe before the
probe was removed and the repository test passed. Neutral manifest model policy
remains data transported to the harness boundary; this repository does not
select hardware, load models, serve inference, or decide model placement.

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
- The previously identified macOS directory-FD race hardening requires a direct
  low-level filesystem dependency decision. That dependency/scope decision was
  not expanded during this no-new-dependencies completion audit.

These gaps are explicit rather than inferred as passes. The supported release
claim is the Docker execution plane with the tested storage-proof fail-closed
gate; it is not a claim that unavailable adapters or an unconfigured physical
quota pool were exercised.
