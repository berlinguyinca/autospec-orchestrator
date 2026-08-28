# Task 2 Report: Docker runtime MVP and safe cleanup

## Outcome

Implemented a real Bollard-backed `DockerRuntime` that provisions one isolated,
ownership-labelled Docker network per execution, starts a limited agent container
and isolated service containers, performs selector-scoped destruction, and reports
orphans without deleting them. The agent now receives only its execution worktree
at `/workspace` and durable conversation directory at `/session`; host-private
session metadata and the Docker socket remain outside the container boundary.

## Changes

- Added Bollard connection handling through `DockerRuntime::connect`, including
  `AUTOSPEC_DOCKER_SOCKET` support, Docker API 1.41 compatibility validation,
  daemon-minimum validation, and Bollard client-version negotiation.
- Added `DockerRuntime::connect_with_state_root` while preserving `connect` and
  `new`. The default constructor reads the one shared `AUTOSPEC_STATE_ROOT`, and
  the explicit constructor supports deterministic worker/test configuration
  without changing the frozen `Runtime` trait.
- Agent provisioning derives the owned worktree and conversation paths from the
  validated execution ID. It requires the harness-owned state/worktree/session
  hierarchy, safely creates only `conversation/`, canonicalizes both sources, and
  bind-mounts them read-write at `/workspace` and `/session` respectively.
- Service containers receive neither host bind. The host-private session root,
  `owner.json`, `.cursor`, `resume-count`, live-event files, and the Docker socket
  are never mounted.
- Added deterministic network, agent-container, and service-container naming from
  the shared contracts.
- Added CPU, memory, swap, PID, and writable-layer disk limits to every agent and
  service container; explicitly disabled privilege and automatic port publication.
- Added labelled network/container provisioning, service network aliases, image
  pulls, and rollback through execution-scoped cleanup on provisioning failure.
- Image inspection now pulls only after an actual Docker 404. Permission, daemon,
  API, timeout, and transport failures are propagated without registry fallback.
- Image-declared `VOLUME` paths are overridden with bounded tmpfs mounts. Every
  agent/service writable layer and declared mount conservatively shares one
  execution-wide `disk_gib` budget, so aggregate configured capacity cannot
  exceed the manifest even when services are added.
- Bounded mounts are ephemeral container state rather than standalone Docker
  resources, preventing anonymous or unlabelled volume creation. Environment
  handles therefore report no volumes for these mounts.
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
- Hardened real-Docker test isolation for concurrent workspace sessions. Test
  execution IDs combine process ID, wall-clock nanoseconds, and a process-local
  atomic sequence; an unwind-safe scope removes managed resources only through
  valid `OwnershipLabels` selectors. Control resources use a distinct control
  `execution_id` and exactly the five shared `autospec.*` ownership keys.
- Normal test cleanup attempts both the control and execution selectors,
  aggregates every error, fails the test on any cleanup error, and marks its
  guard clean only after complete success. Unwind cleanup is non-panicking and
  emits the same aggregate diagnostics.
- The unwind path uses fallible `std::thread::Builder::spawn`, handles thread
  creation, Tokio runtime creation, selector cleanup, and join failures, and
  writes diagnostics through ignored-result `std::io::Write` calls. It contains
  no `panic!`, `expect`, `unwrap`, direct `thread::spawn`, or `eprintln!` path.
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

The second fix round first failed because Redis provisioning still added the
standalone `autospec-{execution_id}-cache-data` volume. The next red test exposed
that allocating one budget per container multiplied capacity when services were
added. The final allocation divides the one execution budget across every agent
and service writable layer plus every image-declared path before Docker objects
are created. With a 1 GiB execution, one agent, one Redis service, and Redis
`/data`, each of the three storage slots receives 357,913,941 bytes; a real write
past the `/data` bound fails at the daemon-enforced boundary. A multi-container,
multi-mount unit case proves aggregate capacity cannot exceed the requested
budget. Daemon rejection of a bounded mount is classified as
`RuntimeError::ResourceLimit`.

The post-review regression test first proved that wall-clock-only IDs were not
process-scoped, then created an owned network and intentionally panicked. Before
the guard existed, the network survived the unwind. Concurrent runtime-docker
tests then exposed a second issue: global volume-set equality observed another
test's labelled volumes. Assertions now inspect execution-specific resources and
only treat newly-created 64-hex Docker volume names as anonymous leaks. Two
runtime-docker test binaries subsequently passed concurrently without collisions.
The follow-up label test then failed on the invented
`autospec.test_execution_id` key. After replacing it with distinct contract
ownership, a real-Docker failure-injection test held one execution volume and one
control volume under independently owned containers. The first cleanup returned
both selector failures and kept the guard armed; after cleaning the blocker
selectors, the retry succeeded.
The final static regression first failed on direct `std::thread::spawn` and
`eprintln!` in `Drop`. The replacement retains the concrete error from thread
creation, runtime construction, thread panic payloads, and the aggregate returned
by both selector destroys, while stderr write failures are deliberately ignored
so cleanup cannot cause a second panic during unwinding.

The Pi integration regression first failed because the agent had no bind mounts.
After wiring the configured shared state root, the real-Docker inspection test
found the expected sources and destinations; Docker normalized an explicit
read-write flag to its omitted/default form, so writability is additionally
proved by container writes observed on the host. The same test proves only the
previously absent conversation directory is created, private session siblings and
the Docker socket are inaccessible, and conversation data survives container
destruction.

## Verification

- `cargo fmt --all -- --check` — passed.
- `cargo build --workspace` — passed.
- `cargo test --workspace -- --nocapture` — passed with normal test concurrency;
  real Docker tests: 12 passed, 0 skipped. PostgreSQL tests printed their existing explicit
  skips because `AUTOSPEC_DATABASE_URL` was unset.
- Two simultaneous `cargo test -p runtime-docker --test docker_runtime --
  --nocapture` processes — both passed 12/12 with no name or state-root collisions.
- `cargo clippy --workspace --all-targets -- -D warnings` — passed.
- `cargo +1.85.0 fmt --all -- --check`, `build --workspace`, `test
  --workspace -- --nocapture`, and `clippy --workspace --all-targets -- -D
  warnings` — passed; the Rust 1.85 real-Docker run also passed 12/12.
- `git diff --check` — passed.
- Post-test Docker inventory for the new process/time/sequence execution IDs and
  their valid control execution IDs — empty. Old wall-clock-only
  resources from a foreign failed session remained untouched, as required by the
  no-foreign-cleanup rule.

## Remaining Risks

- Docker writable-layer `StorageOpt.size` support depends on the daemon storage
  driver. The tested Docker Desktop daemon accepted and reported the quota; an
  unsupported production daemon will reject provisioning instead of silently
  running without the disk limit.
- Bounded image paths are intentionally ephemeral tmpfs state. Services that need
  persistence beyond an execution require a future explicitly budgeted storage
  contract rather than falling back to unbounded Docker volumes.
- The configured state root, worktree, and harness-owned session root must exist
  as real directories before provisioning. The runtime deliberately refuses
  symlinked/missing roots and creates only the conversation child.
- The agent container currently uses the runtime image's `sleep infinity`
  capability. Task 4/5 integration may replace that command when the Pi harness
  launch contract is wired, without changing the runtime trait.
