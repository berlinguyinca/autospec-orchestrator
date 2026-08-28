# Task 2 Report: Docker runtime MVP and safe cleanup

## Outcome

Implemented a real Bollard-backed `DockerRuntime` that provisions one isolated,
ownership-labelled Docker network per execution, starts a limited agent container
and isolated service containers, performs selector-scoped destruction, and reports
orphans without deleting them.

## Changes

- Added Bollard connection handling through `DockerRuntime::connect`, including
  `AUTOSPEC_DOCKER_SOCKET` support, Docker API 1.41 compatibility validation,
  daemon-minimum validation, and Bollard client-version negotiation.
- Added deterministic network, agent-container, and service-container naming from
  the shared contracts.
- Added CPU, memory, swap, PID, and writable-layer disk limits to every agent and
  service container; explicitly disabled privilege and automatic port publication.
- Added labelled network/container provisioning, service network aliases, image
  pulls, and rollback through execution-scoped cleanup on provisioning failure.
- Image inspection now pulls only after an actual Docker 404. Permission, daemon,
  API, timeout, and transport failures are propagated without registry fallback.
- Image-declared `VOLUME` paths are overridden with deterministic execution-owned
  named volumes, including Redis `/data` as
  `autospec-{execution_id}-cache-data`; handles report every created volume.
- Added cleanup for containers, volumes, and networks selected only by
  `OwnershipLabels::selector()`. Cleanup attempts every resource class and
  aggregates removal failures instead of returning after the first failure.
- Provisioning rollback preserves the original provisioning error, aggregated
  rollback failure, and `execution_id` in one diagnostic.
- Added read-only reconciliation across managed containers, networks, and volumes;
  live execution IDs are excluded and orphan IDs are deduplicated.
- Appended `credentials_path: Option<PathBuf>` to `EnvironmentHandle` as reserved by
  the shared runtime contract.
- Added real-Docker tests with explicit printed skips only when the daemon is
  unavailable. Tests inspect daemon-created resources and prove labels, limits,
  running state, network aliases, no host ports, read-only reconciliation,
  selector cleanup, and survival of unrelated containers, networks, and volumes.
- Pinned compatible transitive lockfile releases so the workspace still checks
  with its declared Rust 1.85 toolchain after adding Bollard.

## TDD Evidence

The first focused compile failed because `host_limits`, `DEFAULT_PIDS_LIMIT`, the
Bollard-backed constructor, and `EnvironmentHandle::credentials_path` did not yet
exist. After implementation, the first real-Docker lifecycle run found Docker's
empty-map representation for absent port bindings; the assertion was corrected to
accept both `null` and an empty map, without weakening the no-published-port check.

Fix-round tests first failed on the missing image-error classifier, negotiated
client-version surface, deterministic volume naming, populated handle volumes,
and aggregate rollback behavior. Real Docker failure injection then proved two
in-use volume failures are both retained while network cleanup still runs, and
that a failed Redis provisioning attempt leaves no anonymous volume.

## Verification

- `cargo fmt --all -- --check` — passed.
- `cargo build --workspace` — passed.
- `cargo test --workspace -- --nocapture --test-threads=1` — passed; real Docker
  tests: 5 passed, 0 skipped. PostgreSQL tests printed their existing explicit
  skips because `AUTOSPEC_DATABASE_URL` was unset.
- `cargo clippy --workspace --all-targets -- -D warnings` — passed.
- `cargo +1.85.0 check --workspace` — passed.
- `git diff --check` — passed.
- Post-test Docker inventory for Task 2 ownership labels — empty.

## Remaining Risks

- Docker writable-layer `StorageOpt.size` support depends on the daemon storage
  driver. The tested Docker Desktop daemon accepted and reported the quota; an
  unsupported production daemon will reject provisioning instead of silently
  running without the disk limit.
- Docker has no portable quota for ordinary local named volumes. Image-declared
  data paths are therefore made ownership-labelled and lifecycle-scoped to prevent
  anonymous leaks, while `StorageOpt.size` remains the enforced per-container
  writable-layer quota.
- The agent container currently uses the runtime image's `sleep infinity`
  capability. Task 4/5 integration may replace that command when the Pi harness
  launch contract is wired, without changing the runtime trait.
