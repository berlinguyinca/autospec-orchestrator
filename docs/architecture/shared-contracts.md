# Shared contracts for cross-issue interfaces

Issues #2–#31 of the execution-plane build are implemented independently, often
by different models in different sessions. Without a shared contract they invent
incompatible type signatures, file layouts, and naming schemes — mismatches that
only surface at integration time.

**This document is the authoritative tie-breaker.** When it and an issue body
disagree on a name, path, signature, or key, this document wins. It is also
appended verbatim to every child issue between
`<!-- autospec-shared-contracts:begin -->` / `:end` markers, so an implementer
never has to leave the issue to find it.

It records only what two or more issues share. Anything an issue owns alone is
left to that issue. Governing spec:
[`docs/specs/three-plane-execution-architecture.md`](../specs/three-plane-execution-architecture.md);
boundaries and invariants in [`AGENTS.md`](../../AGENTS.md); the correlation
vocabulary and source-of-truth matrix in
[`source-of-truth.md`](source-of-truth.md).

---

Authoritative tie-breaker for cross-issue interfaces across #2–#31. Where this document and any issue body disagree on a name, path, signature, or key, **this document wins**.

### 1. File and module ownership

One file has exactly one owning issue. Other issues may append (`mod` lines, new functions, call-site hooks) but never rename, reorder, or restructure what the owner declared.

| File | Owner | Appenders |
| --- | --- | --- |
| `crates/orchestrator-core/src/environment.rs` | #4 | — |
| `crates/orchestrator-core/src/telemetry.rs` | #5 | — |
| `crates/orchestrator-persistence/` (crate, `error.rs`, `migrations/`) | #2 | #3 #19 #21 #25 |
| `crates/orchestrator-persistence/src/event_log.rs` | #3 | #24 (read-only use) |
| `crates/orchestrator-persistence/src/artifacts.rs` | #25 | — |
| `crates/runtime-docker/src/lib.rs` (`DockerRuntime` struct) | #6 | #7 #8 #9 #10 #27 |
| `crates/runtime-docker/src/provision.rs` | #7 | #8 #9 #27 |
| `crates/runtime-docker/src/limits.rs` / `services.rs` / `cleanup.rs` / `credentials.rs` | #8 / #9 / #10 / #27 | — |
| `crates/runtime-traits/src/lib.rs` | scaffold (frozen, see §4) | #7 #10 #27 #30 |
| `crates/git-worktree/src/manager.rs` (`GitWorktreeManager`) | #11 | #12 #13 #14 |
| `crates/git-worktree/src/lock.rs` / `diff.rs` / `cleanup.rs` | #12 / #13 / #14 | — |
| `crates/harness-pi/src/session.rs` | #15 | #17 |
| `crates/harness-pi/src/events.rs` / `resume.rs` | #16 / #17 | — |
| `crates/orchestrator-worker/src/run.rs` + `cleanup_guard.rs` | #20 | #26 #27 |
| `crates/orchestrator-worker/src/health.rs` / `recovery.rs` / `isolation.rs` | #18 / #22 / #26 | — |
| `crates/orchestrator-api/src/state.rs`, `error.rs`, `auth.rs`, `executions.rs` | #23 | #19 #24 #25 #28 #31 |
| `crates/orchestrator-api/src/workers.rs` / `events.rs` / `artifacts.rs` / `interactive.rs` | #19 / #24 / #25 / #28 | #22 (workers.rs) |
| `crates/autospec-orchestrator/src/cli.rs` | #31 | — |
| `deploy/docker-compose.yml`, `Dockerfile` | #29 | — |
| `crates/runtime-podman/`, `crates/runtime-apptainer/` | #30 | — |

**auth.rs race (#19 vs #23):** #19 lands worker-token auth and #23 lands client-token auth, possibly in parallel. Whichever lands first creates `crates/orchestrator-api/src/auth.rs` containing **both** `pub async fn require_worker_token` and `pub async fn require_api_token` (stubbing the one it does not need). #23 is the declared owner of the file's final shape. Do not put an auth check inline in a handler module.

### 2. Root `Cargo.toml`

Touched by #2, #6, #30. Append to `members` at the end and to `[workspace.dependencies]` alphabetically; never reorder existing entries. Agreed new workspace deps and their owning issue:

`sqlx = { version = "0.8", features = ["runtime-tokio", "tls-rustls", "postgres", "chrono", "json", "uuid", "migrate"] }` (#2) · `bollard = "0.18"` (#6) · `fs2 = "0.4"` (#11) · `rand = "0.8"` (#9, #27) · `sha2 = "0.10"` (#25) · `reqwest = { version = "0.12", features = ["json", "rustls-tls"], default-features = false }` (#19 worker client, #31 CLI) · `regex = "1"` (#5) · `tempfile = "3"` (dev-dependency, any crate). Use `dep.workspace = true` in crate manifests — never a version literal in a crate `Cargo.toml`.

### 3. Persistence contracts (`orchestrator-persistence`, owner #2)

All store traits are object-safe, `#[async_trait]`, `Send + Sync`, held as `Arc<dyn …>`.

```rust
#[async_trait] pub trait ExecutionStore: Send + Sync {          // #2
    async fn insert(&self, e: &Execution) -> Result<(), StoreError>;
    async fn get(&self, id: &ExecutionId) -> Result<Execution, StoreError>;
    async fn list_live(&self) -> Result<Vec<Execution>, StoreError>;
    async fn transition(&self, id: &ExecutionId, next: ExecutionState) -> Result<Execution, StoreError>;
}
#[async_trait] pub trait EventLog: Send + Sync {                 // #3
    async fn append(&self, event: &ExecutionEvent) -> Result<u64, StoreError>;
    async fn since(&self, id: &ExecutionId, after: u64) -> Result<Vec<ExecutionEvent>, StoreError>;
}
#[async_trait] pub trait WorkerStore: Send + Sync { … }          // #19
#[async_trait] pub trait ReservationStore: Send + Sync { … }     // #21
#[async_trait] pub trait ArtifactStore: Send + Sync { … }        // #25
```

**`StoreError` is one enum in `crates/orchestrator-persistence/src/error.rs`, owned by #2.** Variants are append-only: `Db(#[from] sqlx::Error)`, `NotFound(String)`, `IllegalTransition { from, to }`, `Conflict(String)` (#2); `SequenceConflict` (#3); `CapacityExhausted(WorkerId)` (#21); `InvalidArtifactName(String)` (#25). Nobody defines a second store error type.

**Event `sequence` is allocated only by `EventLog::append`.** `ExecutionEvent.sequence` is `u64` / SQL `BIGINT`. #16's `poll_events` returns events with `sequence: 0` as a placeholder; the run loop (#20) obtains the real value from `append`. No other code assigns a sequence.

**Migration numbering is fixed and never renumbered.** All migrations live in `crates/orchestrator-persistence/migrations/`: `0001_executions.sql` (#2), `0002_execution_events.sql` (#3), `0003_workers.sql` (#19), `0004_reservations.sql` (#21), `0005_artifacts.sql` (#25). Anything new starts at `0006_`.

**DB naming:** tables plural `snake_case`; columns `snake_case`; ids and enums stored as `TEXT` whose value is exactly the serde rendering from `orchestrator-core` (`ExecutionState`/`FailureClass`/`WorkerState` are `SCREAMING_SNAKE_CASE`, `Role` is `kebab-case`, `HarnessKind`/`RuntimeKind`/`PersistenceMode` are `lowercase`); whole core structs stored as `JSONB`; times as `TIMESTAMPTZ`. No PostgreSQL `ENUM` types. No foreign keys to, or tables shared with, AutoSpec — §74 forbids a shared database.

### 4. Runtime contracts

The `Runtime` trait method set in `crates/runtime-traits/src/lib.rs` is **frozen** at `name`/`available`/`provision`/`destroy`/`reconcile`. #30 implements it as-is and adds no methods. `EnvironmentHandle` gains fields append-only; #7 owns those additions (including `credentials_path: Option<PathBuf>` reserved for #27). #27 owns adding to that file:

```rust
#[async_trait] pub trait CredentialBroker: Send + Sync {
    async fn mint(&self, e: &Execution) -> Result<ExecutionCredentials, RuntimeError>;
    async fn revoke(&self, id: &ExecutionId) -> Result<(), RuntimeError>;
}
pub struct ExecutionCredentials { pub path: PathBuf, pub expires_at: DateTime<Utc> }
```

`DockerRuntime` shape is owned by #6: `pub struct DockerRuntime { client: bollard::Docker, min_api_version: String }`, constructed via `DockerRuntime::connect(socket: Option<&str>) -> Result<Self, RuntimeError>`; `new()` is a thin wrapper over `connect(None)`. Later issues append fields; nobody reorders or reintroduces a unit struct. `host_limits(req: &RuntimeRequirement) -> HostConfigLimits` (#8) is applied to the agent container **and** every service container.

**Docker object naming — all derived from `ExecutionId`, relied upon verbatim by #26's `assert_isolated`:**

- network `autospec-{execution_id}` (already `DockerRuntime::network_name`)
- agent container `autospec-{execution_id}-agent`
- service container `autospec-{execution_id}-{service_name}`, network alias `{service_name}`, **no published host ports**
- volume `autospec-{execution_id}-{purpose}`

### 5. Ownership labels (§42, §83, invariant 6)

The only label keys are the five constants in `orchestrator_core::labels`: `autospec.managed`, `autospec.execution_id`, `autospec.worker_id`, `autospec.repository`, `autospec.issue`. No issue invents a new `autospec.*` key without editing `labels.rs`. Every created network, container, and volume carries `OwnershipLabels::to_map()`. Targeted deletion filters on `OwnershipLabels::selector()`; `reconcile` filters on `autospec.managed=true` only and deletes nothing. Global `prune` in any form is forbidden.

Non-Docker resources carry an on-disk owner record instead: `{worktree}/.autospec-owner.json` (#12, containing `labels.to_map()` plus `base_sha`; read by #13 and #14) and `{session}/owner.json` (#15/#17). These two filenames differ deliberately — do not unify them.

### 6. `AUTOSPEC_STATE_ROOT` and the on-disk layout

There is **one** root, `AUTOSPEC_STATE_ROOT` (default `/var/lib/autospec`). `GitWorktreeManager`'s `cache_root`, the harness `state_root`, and the artifact blob root are all this same directory. Do not introduce a second root env var.

```
$AUTOSPEC_STATE_ROOT/
  mirrors/{owner}__{name}.git            #11   (+ sibling {…}.git.lock, fs2 exclusive)
  worktrees/{execution_id}/              #12   (+ .autospec-owner.json)
  sessions/{execution_id}/               #15   (bind-mounted at /session)
    owner.json  #17     .cursor  #16     resume-count  #17
  artifacts/{sha256[0..2]}/{sha256}      #25
```

### 7. Environment variables (`AUTOSPEC_*`, SCREAMING_SNAKE)

Existing: `AUTOSPEC_ORCHESTRATOR_ADDR`, `AUTOSPEC_ORCHESTRATOR_URL`, `AUTOSPEC_WORKER_ID`, `AUTOSPEC_WORKER_CONCURRENCY`. Added: `AUTOSPEC_DATABASE_URL` (#2 — the controller DSN; **not** `DATABASE_URL`, which is reserved for the value #9 injects into the agent container), `AUTOSPEC_WORKER_TOKEN` (#19, worker→controller bearer), `AUTOSPEC_API_TOKEN` (#23/#31, client→controller bearer), `AUTOSPEC_STATE_ROOT` (#11/#15/#25), `AUTOSPEC_DOCKER_SOCKET` (#6), `AUTOSPEC_WORKER_HOST_DOCKER` (#29). Every one is also a `clap` `#[arg(long, env = …)]` where a binary consumes it.

### 8. HTTP surface (#19, #23, #24, #25, #28, #31)

- Prefix comes from `orchestrator_api::api_root()` / `orchestrator_core::API_VERSION`. Never hardcode the literal `/api/v1`. `/healthz` stays unauthenticated and outside the versioned prefix.
- Each module exposes `pub fn routes() -> axum::Router<AppState>`; `lib.rs::router()` merges them. No module edits another module's routes.
- `AppState` (owner #23, `crates/orchestrator-api/src/state.rs`) is `#[derive(Clone)]` and holds `Arc<dyn ExecutionStore>`, `Arc<dyn EventLog>`, `Arc<dyn WorkerStore>`, and `tokio::sync::broadcast::Sender<ExecutionEvent>`. Fields are added append-only.
- Wire JSON is produced by serializing `orchestrator-core` types directly. Do not declare a parallel DTO that duplicates a core type; field names and casings are whatever the core `serde` attributes already emit.
- Error body is uniform: `{"error":{"code":"SNAKE_CASE","message":"…"}}`, `code` mirroring the Rust variant. Owner #23 defines `ApiError` with `IntoResponse` in `crates/orchestrator-api/src/error.rs`.
- Status codes: `201` create · `200` idempotent replay / read · `400` validation · `401` missing or bad bearer · `404` unknown id · `409` illegal transition or terminal-state conflict · `413` body over `1048576` bytes.
- Auth is `Authorization: Bearer <token>` on every `/api/v1` route.
- Route shapes: `/executions`, `/executions/{id}`, `/executions/{id}/{cancel,retry,events,artifacts,pause,resume,attach}`, `/workers`, `/workers/{id}/heartbeat`. Path segments are lowercase; ids are path parameters, never query parameters.

### 9. `orchestrator-core` is frozen except by declared additions

Edited by #4 (`manifest.rs` `validate` + new `environment.rs`), #5 (`telemetry.rs`), #16 (`event.rs`), #18 (`error.rs`), #26 and #28 (`execution.rs`).

- Existing enum variants, struct fields, and `serde` renames are **frozen**. Additions go at the end.
- `ExecutionState::can_transition_to` is frozen; only #28 may add the `PausedForHuman` edges it needs.
- **#16 adds no `ExecutionEventKind` variants** — the enumerated set covers the Pi mapping, and unrecognised Pi records are counted and logged, not modelled.
- **#18 adds no `FailureClass` variants** — `Inactivity`, `Timeout`, and `ResourceViolation` already exist.
- #5 adds `tracing`, `tracing-subscriber`, and `regex` to `orchestrator-core` and re-exports `telemetry::{init_tracing, execution_span}` from `lib.rs`.
- **#4:** `examples/environment.yaml` declares `services` as a **map** keyed by name; `ExecutionManifest::services` is a **list** of `ServiceRequirement { name, image, env }`. `EnvironmentFile::resolve` converts map → list. Neither file's shape changes.
- **#13 changes one existing signature** (the only sanctioned breaking change): `WorktreeManager::capture_diff` becomes `-> Result<DiffCapture, WorktreeError>`. `ExecutionResult::diff_artifact` stays `Option<String>` and holds the artifact id once #25 lands.

### 10. `ExecutionId` format

`{repo-slug}-{issue_id}-{role}-{NN}`, e.g. `node-417-impl-01`, `node-417-review-01`. Role abbreviations: `impl`, `review`, `doc`, `ui`, `itest`, `inter`. Allocated by #23 only. Must match `^[a-z0-9][a-z0-9-]{0,62}$` so it is safe as both a Docker object name and a single path component; #12 rejects any id containing `..` or `/`.

### 11. Rust conventions

- One `thiserror::Error` enum per crate, named for its crate: `CoreError`, `StoreError`, `RuntimeError`, `HarnessError`, `WorktreeError`, `ScheduleError`, `ApiError`. Variants carry `String` context or `#[from]`. Binaries use `anyhow`; **libraries never put `anyhow` in a public signature**.
- Object-safe async traits use `#[async_trait::async_trait]` and are held as `Arc<dyn Trait>`.
- **`WorktreeManager` stays synchronous** (as scaffolded). #20's async run loop calls it inside `tokio::task::spawn_blocking`. Do not make the trait async.
- Every git and container invocation uses `std::process::Command` (or `bollard`) with separate arguments — never a shell string.
- `unsafe_code = "forbid"` is workspace-wide; no crate adds `#[allow(unsafe_code)]`.

### 12. Invariants no issue re-derives

Ownership labels on every runtime resource, owner records on every worktree and session (§42, §83, invariants 6–7). No global `docker … prune`, no `git branch | grep | xargs`; deletion is always selector- or owner-record-driven (§43, §82). No GPU, VRAM, model-placement, or token-accounting concept anywhere in this repo — that is InferWeave's (§12, §13). Retry policy belongs to AutoSpec; the orchestrator only classifies with `FailureClass` and reports evidence (§9, §40). The correlation vocabulary is exactly the nine ids in `docs/architecture/source-of-truth.md`, never abbreviated in a log field name (§72). Manifests are `autospec.dev/v1alpha1` via `MANIFEST_API_VERSION`; the HTTP surface is versioned from day one (§75). One failing execution never destabilises another or its worker (invariant 13).

### 13. Test conventions

Real services only — PostgreSQL 17, a real Docker daemon, real local git repositories, a stub `pi` script baked into a test image. No mocks at any of those boundaries. A test whose dependency is absent **skips with an explicit printed message**; it never fails and never silently passes. Test-created runtime resources carry the same ownership labels so leak detection works in CI. Target 80%+ line coverage on new code.
