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
  acquires an execution-scoped shared Ready lease for the complete async
  provisioning/rollback lifetime, pins every bind directory's filesystem
  identity, and obtains a fresh full Ready proof immediately before each Docker
  mutation and container start. Release requires the corresponding exclusive
  lease and fails without transitioning the journal while a consumer is active.
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
  The image config is inspected immediately before creation and any nonempty
  declared `VOLUME` set is rejected.
  It mounts every expected source read-only, returns exactly one device/inode
  identity per source, and must match the receipt's daemon-derived execution-root
  identity and device. The verifier is removed before services or the agent are
  started. Its returned immutable container ID is captured; teardown lists only
  the exact execution selector, requires that ID and every Autospec ownership
  label, then removes by ID with volume deletion disabled. A same-name foreign
  replacement is never selected. Workload entrypoints are never used as a trust
  probe.
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
- Storage regressions prove an exact Ready lease validates the receipt/backend/
  mount/daemon proof, prevents a concurrent release, and permits release after
  drop. Invalid release input cannot create lease metadata. A real-Docker
  observation proves the runtime retains the lease after agent creation through
  the final workload-start gate, then drops it on successful return.
- Verifier regressions prove declared verifier-image volumes fail before Docker
  resource creation without an anonymous-volume leak, and teardown rejects a
  missing/replaced captured ID or incomplete ownership labels. The configured
  lifecycle proof executes the recorded absolute `/bin/stat`, never PATH lookup.
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

- `cargo test --workspace` — passed on the current toolchain, including all 14
  runtime unit tests, 18 real-Docker integration tests, and 41
  execution-storage unit/integration tests. The destructive aggregate quota test
  printed an explicit skip because no operator APFS/LVM pool is configured.
- `cargo fmt --all -- --check`, `cargo build --workspace`, `cargo test
  --workspace`, and `cargo clippy --workspace --all-targets -- -D warnings` —
  passed on the current toolchain.
- `cargo +1.85.0 fmt --all -- --check`, `build --workspace`, `test --workspace`,
  and `clippy --workspace --all-targets -- -D warnings` — passed, including all
  14 runtime unit, 18 real-Docker integration, and 41 execution-storage tests.
- `git diff --check` — passed.

## Remaining concern

- This workstation has no configured destructive APFS/LVM worker pool, so the
  end-to-end aggregate ENOSPC test compiled and explicitly skipped rather than
  performing allocation. The execution-storage backend's own configured
  lifecycle test has the same operator gate. Production provisioning fails
  closed when the live Ready allocation or its daemon bind proof is unavailable.
- The safe storage pin API rechecks captured path/device/inode identity at every
  gate, but it cannot eliminate the final macOS pathname-to-Docker bind race
  without the separately deferred descriptor-relative filesystem work. The
  runtime fails closed on every identity change it can observe and does not
  claim that deferred race is solved.
