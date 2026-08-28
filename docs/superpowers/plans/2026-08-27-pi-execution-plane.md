# Pi Execution Plane Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Deliver a usable, durable, isolated AutoSpec execution plane that runs coding workloads through Pi and returns compact, replayable evidence.

**Architecture:** AutoSpec submits a versioned neutral manifest to the controller. The controller persists it, selects a capable worker, and the worker creates an isolated worktree and labelled runtime before launching Pi with a compact task packet. Pi sessions and append-only events survive container restarts; artifacts are content-addressed, and cleanup touches only execution-owned resources.

**Tech Stack:** Rust 2021, Tokio, Axum, PostgreSQL 17/sqlx, Docker/bollard, Git worktrees, Pi JSONL sessions.

**Spec:** `docs/specs/three-plane-execution-architecture.md`

## Global Constraints

- Source-mutating executions always use isolated Git worktrees and isolated runtimes.
- Implementation and independent review never share mutable runtime state.
- Every runtime resource carries `autospec.managed=true` and `autospec.execution_id=...`; cleanup uses labels only.
- No global Docker or Git pruning and no unrestricted host Docker access by default.
- The orchestrator carries model policy verbatim; InferWeave owns model and GPU decisions.
- Pi is the primary harness. Persist its JSONL session outside disposable containers.
- Token efficiency comes from compact task packets, cursor-based incremental JSONL reads, event normalization, content-addressed artifacts, and resume rather than prompt replay.
- Public HTTP APIs use `/api/v1`; manifests use `autospec.dev/v1alpha1`.
- Follow `docs/architecture/shared-contracts.md` for exact file ownership and interfaces.
- New behavior is developed test-first; real PostgreSQL, Docker, Git, and a stub Pi executable are used at integration boundaries.

---

### Task 1: Durable execution foundation (#2–#5)

**Files:**
- Create: `crates/orchestrator-persistence/`, migrations `0001_executions.sql` and `0002_execution_events.sql`
- Create: `crates/orchestrator-core/src/environment.rs`, `telemetry.rs`
- Modify: `crates/orchestrator-core/src/manifest.rs`, `lib.rs`, workspace manifests

**Interfaces:**
- Produces `ExecutionStore`, `EventLog`, manifest validation/environment resolution, and correlated tracing.
- Consumed by controller API, worker registration, recovery, SSE, and artifacts.

- [x] Add failing tests for persisted transitions, gapless event sequences, manifest validation, environment resolution, and log-field redaction.
- [x] Implement migrations and object-safe PostgreSQL stores.
- [x] Implement strict manifest/environment validation without interpreting task intent.
- [x] Implement structured correlation spans and secret redaction.
- [x] Run format, focused tests, workspace tests, and clippy.

### Task 2: Docker runtime MVP and safe cleanup (#6–#10)

**Files:**
- Create: `crates/runtime-docker/src/{provision,limits,services,cleanup}.rs`
- Modify: `crates/runtime-docker/src/lib.rs`, runtime test suite

**Interfaces:**
- Produces a real `DockerRuntime` implementing the frozen `Runtime` trait.
- Consumes ownership labels and runtime/service requirements.

- [x] Add daemon-probe and real-resource tests with explicit dependency skips.
- [x] Connect through bollard with a minimum API version check.
- [x] Provision one labelled network, limited agent container, and isolated service containers without host ports.
- [x] Enforce CPU, memory, PID, and disk constraints on every container.
- [x] Destroy only selector-matched resources and report—not delete—orphans during reconciliation.
- [x] Prove unrelated Docker resources survive cleanup.

### Task 3: Physical Git isolation and evidence (#11–#14)

**Files:**
- Create: `crates/git-worktree/src/{manager,lock,diff,cleanup}.rs`
- Modify: `crates/git-worktree/src/lib.rs`

**Interfaces:**
- Produces `GitWorktreeManager` and `DiffCapture` through the existing synchronous trait.
- Consumed by the worker run loop through `spawn_blocking`.

- [x] Add tests using real temporary Git repositories.
- [x] Implement locked bare mirrors and safe repository-name normalization.
- [x] Create execution-scoped worktrees with `.autospec-owner.json`.
- [x] Capture patch plus changed-file evidence.
- [x] Destroy only verified owned worktrees and identify stale owner records.

### Task 4: Token-efficient Pi harness (#15–#18)

**Files:**
- Create: `crates/harness-pi/src/{session,events,resume}.rs`
- Modify: `crates/harness-pi/src/lib.rs`, `crates/harness-traits/src/lib.rs`

**Interfaces:**
- Produces durable Pi start/stop/resume/fork and normalized incremental events.
- Consumes a compact `TaskPacket`; never rebuilds prompts from repository-wide context.

- [x] Add tests around a stub `pi` executable and representative JSONL records.
- [x] Serialize the task packet once and launch Pi against mounted worktree/session paths.
- [x] Persist `owner.json`, `.cursor`, and `resume-count` outside containers.
- [x] Poll only JSONL bytes after `.cursor`; normalize known events and skip unknown records with counters.
- [x] Resume the same session after container loss and fork conversations without copying worktrees.
- [x] Add inactivity, wall-clock, and CPU-saturation health classification.

### Task 5: Worker scheduling, lifecycle, and recovery (#19–#22)

**Files:**
- Create worker API/store modules, `crates/orchestrator-worker/src/{run,cleanup_guard,health,recovery}.rs`
- Modify: `crates/autospec-worker/src/main.rs`, scheduler and persistence modules

**Interfaces:**
- Produces registration/heartbeat, transactional capacity reservation, terminal run results, and stranded-execution recovery.

- [ ] Add concurrency and failure-injection tests proving one execution cannot destabilize another.
- [ ] Implement authenticated worker registration and heartbeats.
- [ ] Reserve worker slots transactionally before assignment.
- [ ] Execute worktree → runtime → Pi → incremental events → diff/artifacts with cleanup guards.
- [ ] Persist state before publishing events and recover executions from lost workers.

### Task 6: Versioned execution API and durable evidence (#23–#25)

**Files:**
- Create: API state/error/auth/execution/event/artifact modules
- Create: persistence artifact store and `0003_workers.sql` through `0005_artifacts.sql`

**Interfaces:**
- Produces authenticated `/api/v1` execution, worker, SSE, and artifact surfaces.

- [ ] Add HTTP contract tests for status codes, auth, idempotency, transitions, and body limits.
- [ ] Implement create/read/cancel/retry without duplicating core DTOs.
- [ ] Implement resumable SSE using append-only sequence cursors.
- [ ] Store artifacts by SHA-256 and list them by execution without injecting blobs into prompts.

### Task 7: Isolation, scoped credentials, and interactive Pi (#26–#28)

**Files:**
- Create: worker isolation tests, Docker credential broker, interactive API routes

- [ ] Prove implementation and review executions share no network, volume, worktree, session, or credential state.
- [ ] Mint short-lived execution-scoped credentials and revoke them during cleanup.
- [ ] Implement pause/resume/attach and conversation fork while preserving the original worktree boundary.

### Task 8: Operability and runtime conformance (#29–#31)

**Files:**
- Create: `deploy/docker-compose.yml`, `Dockerfile`, Podman/Apptainer crates, controller CLI module

- [ ] Add a single-host deployment with constrained Docker access.
- [ ] Run the frozen runtime conformance suite against Docker, Podman, and Apptainer where available.
- [ ] Implement operator commands for workers, executions, queue state, and cleanup health.

### Task 9: Completion and invariant audit (#33)

**Files:**
- Create: `docs/architecture/phase-5.5-audit.md`, repository-wide invariant tests, end-to-end fixtures

- [ ] Demonstrate manifest → persisted execution → worker → worktree → Docker/services → Pi → events/artifacts → cleanup.
- [ ] Demonstrate crash resume and independent review isolation.
- [ ] Assert forbidden prune/xargs/model-placement patterns are absent.
- [ ] Run `cargo fmt --all -- --check`, `cargo build --workspace`, `cargo test --workspace`, and `cargo clippy --workspace --all-targets -- -D warnings`.
- [ ] Record evidence for every invariant and all unavailable external-runtime tests.
