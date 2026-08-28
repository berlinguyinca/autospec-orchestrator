# Task 2 Report: storage-backed Docker runtime

## Outcome

`DockerRuntime` now provisions only from an exact live Ready
`execution-storage` allocation. Agent and service containers use immutable root
filesystems, disabled Docker logging, and deterministic writable bind directories
inside the allocation's aggregate hard-quota filesystem. Legacy constructors
remain available for daemon probes, reconciliation, and cleanup, but provisioning
through them fails closed.

## Runtime integration

- Added the production `connect_with_verified_execution_storage` path, consuming
  an exact `AllocationReceipt`, a live `ReadyAllocationVerifier`, and a pinned
  immutable verifier image/absolute stat command without changing the frozen
  `Runtime` trait. The older storage constructor now also fails closed for
  provisioning because it cannot supply that trusted image contract.
- Provisioning exact-matches allocation ownership labels and `disk_gib`, verifies
  the durable Ready journal/backend/mount/Docker proof through the capability,
  pins that capability through provisioning, pins every bind directory's
  filesystem identity, and obtains a fresh full Ready proof immediately before
  each Docker mutation and container start.
- The runtime compares the current Docker daemon's immutable daemon ID with the
  allocation's daemon-derived bind proof before creating a network or container.
  A foreign-daemon receipt fails as `RuntimeError::ResourceLimit`.
- The agent receives the verified `layout.repository` at `/workspace` and only
  `layout.session/conversation` at `/session`. Host-private session siblings and
  the Docker socket are not mounted.
- Agent and service containers set `ReadonlyRootfs=true` and log driver `none`.
  No `StorageOpt`, tmpfs, anonymous volume, named volume, or published host port
  is used as quota authority.
- Every intentional writable location is a read-write bind beneath the one
  verified execution filesystem: deterministic per-container home, `/tmp`,
  `/var/tmp`, `/run`, and remapped image-declared `VOLUME` paths. Bind sources
  are canonicalized and created as direct descendants of `layout.runtime`.
- Image `VOLUME` declarations equal to, nested beneath, or shadowing
  `/workspace` or `/session` remain rejected before Docker resource creation.
- After container creation and before workload start, daemon inspection must
  exact-match the requested bind mounts (type, absent volume name, canonical
  source, target, and access mode), readonly rootfs, log driver `none`, disabled
  IPC, bounded 64 MiB memory-only `/dev/shm`, and no unrequested/anonymous
  mount. Docker was observed to normalize `shm_size=0` to its 64 MiB default,
  so the runtime requests and verifies that bound explicitly.
- Before any workload starts, a short-lived ownership-labelled verifier
  container uses only the configured immutable image ID and absolute command.
  It mounts every expected source read-only, returns exactly one device/inode
  identity per source, and must match the receipt's daemon-derived execution-root
  identity and device. The verifier is removed before services or the agent are
  started. Workload entrypoints are never used as a trust probe.
- Any gate failure rolls back through the execution-label selector. The verifier
  image ID and command are included in the versioned Docker proof method, so
  verifier or daemon drift invalidates the allocation.
- Existing API 1.41 negotiation, 404-only image pulls, execution-labelled object
  creation, aggregated rollback/cleanup diagnostics, selector-only destruction,
  and read-only reconciliation remain intact.

## TDD and real-Docker evidence

- A new red regression first showed that a foreign-daemon receipt could provision
  successfully. The runtime now rejects it before a network exists; the same test
  proves legacy provisioning fails closed.
- A malicious workload-image regression first showed its entrypoint writing a
  marker during the old mount probe. It now proves a Ready-to-invalid transition
  after container creation prevents every workload start and leaves the marker
  absent while selector-scoped rollback removes the created resources.
- Unit regressions cover immutable verifier configuration, exact requested versus
  daemon-reported mount equality (including anonymous/unexpected rejection),
  bounded memory-only namespace configuration, and replacement of a pinned
  runtime bind directory.
- Real Docker inspection proves exact `/workspace` and `/session` sources,
  deterministic runtime bind sources, `ReadonlyRootfs=true`, log driver `none`,
  no tmpfs/storage options/volumes/socket/ports, and one daemon-observed device.
- Container writes prove `/workspace`, `/session`, home, temp, run, and Redis
  `/data` are writable and durable after container destruction. Writes to `/etc`
  fail for both agent and service roots. Private session metadata remains
  inaccessible.
- Real images declaring `/workspace`, `/session/history`, and `/` are rejected
  before execution resources appear while another execution remains running.
- The operator-configured lifecycle allocates two independent APFS/LVM execution
  filesystems, alternates 64 MiB writes across one execution's agent and service,
  accepts only ENOSPC after substantial successful writes, proves the other
  execution remains writable, then selector-cleans Docker resources and releases
  both exact storage receipts. It runs only when `AUTOSPEC_APFS_PROBE_PATH` or
  `AUTOSPEC_LVM_VOLUME_GROUP` is configured.

## Verification

- Two simultaneous `cargo test -p runtime-docker --test docker_runtime --
  --test-threads=8` runs — all 16 integration tests passed in both processes
  against the real Docker daemon. The destructive aggregate quota test
  printed an explicit skip because no operator APFS/LVM pool is configured.
- `cargo test -p runtime-docker --lib` — all 12 unit tests passed.
- `cargo fmt --all -- --check`, `cargo build --workspace`, `cargo test
  --workspace`, and `cargo clippy --workspace --all-targets -- -D warnings` —
  passed on the current toolchain.
- `cargo +1.85.0 fmt --all -- --check`, `build --workspace`, and `clippy
  --workspace --all-targets -- -D warnings` — passed. All 12 runtime unit, 16
  real-Docker integration, and 40 execution-storage tests passed under Rust
  1.85. The full workspace test run is recorded under Remaining concern.
- `git diff --check` — passed.

## Remaining concern

- This workstation has no configured destructive APFS/LVM worker pool, so the
  end-to-end aggregate ENOSPC test compiled and explicitly skipped rather than
  performing allocation. The execution-storage backend's own configured
  lifecycle test has the same operator gate. Production provisioning fails
  closed when the live Ready allocation or its daemon bind proof is unavailable.
- The Rust 1.85 full workspace test was attempted twice. An unrelated
  `harness-pi` process-cleanup timing test exceeded its five-second bound in both
  full concurrent runs (the second overloaded run also timed out three sibling
  process-control tests); the exact failing test passed alone in 4.17 seconds.
  The current-toolchain full workspace test passed, and the complete owned
  runtime/storage Rust 1.85 suites passed. No `harness-pi` files were changed.
- The safe storage pin API rechecks captured path/device/inode identity at every
  gate, but it cannot eliminate the final macOS pathname-to-Docker bind race
  without the separately deferred descriptor-relative filesystem work. The
  runtime fails closed on every identity change it can observe and does not
  claim that deferred race is solved.
